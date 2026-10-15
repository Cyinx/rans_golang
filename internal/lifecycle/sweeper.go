package lifecycle

// 周期任务族（规格 §15 P0 周期 checkpoint；
// 扫描经 *activation.Table 协作）：
//   - CheckpointSweeper：周期驱动 checkpoint 复合体（枚举本地 Active 且有
//     快照能力的实体，按脏纪元门控——活动纪元 > 已 checkpoint 纪元，闲置
//     实体永不重写；捕获经 pawnite loop 任务执行——串行即静默（捕获与
//     Handle 无并发）；Store.Put 在捕获结果收妥后进行、in-flight CAS 门保证
//     同实体 IO 永不重叠。间隔默认 1min、单轮上限 1000，规格附录 A）。
//
// 与 Watchdog/Drainer 同族：编排器持有注入面，表驱动的复合体由本包装配
// 函数接线（NewCheckpointSweeper），宿主负责 Run 的生命
// 周期。

import (
	"context"
	"errors"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// staleDestroyBudget 陈旧实例止损销毁的单实体预算（实体已围栏，超时无害
// ——与 rpc 侧 ownership-lost 销毁预算同源量级）。
const staleDestroyBudget = 30 * time.Second

// 缺省参数同源（规格附录 A）：checkpoint 间隔 1min、单轮上限 1000。
const (
	DefaultCheckpointInterval = time.Minute
	DefaultCheckpointRound    = 1000
)

// CheckpointSweeperConfig 周期参数：零/负值一律回落缺省。
type CheckpointSweeperConfig struct {
	Interval    time.Duration
	MaxPerRound int
}

// CheckpointSweeper 周期 checkpoint 编排器。Sweep 是 checkpoint 复合体
// （NewCheckpointSweeper 装配为 CheckpointSweep）：起始一轮捕获，返回起始
// 数。nil Sweep → Scan 空转。
type CheckpointSweeper struct {
	Sweep  func(ctx context.Context, maxCount int) int
	Config CheckpointSweeperConfig
}

// NewCheckpointSweeper 装配周期 checkpoint 编排器（规格 §15）：Sweep 面接
// 线 CheckpointSweep（下），参数由 cfg 提供（零值字段回落缺省 1min/1000）。
// 宿主负责 Run 的生命周期（ctx 随进程/停机编排）。
func NewCheckpointSweeper(tbl *activation.Table, cfg CheckpointSweeperConfig) *CheckpointSweeper {
	return &CheckpointSweeper{
		Sweep:  func(ctx context.Context, maxCount int) int { return CheckpointSweep(ctx, tbl, maxCount) },
		Config: cfg,
	}
}

// effectiveInterval 解析生效间隔（≤0 回落缺省）。
func (s *CheckpointSweeper) effectiveInterval() time.Duration {
	if s.Config.Interval <= 0 {
		return DefaultCheckpointInterval
	}
	return s.Config.Interval
}

// effectiveRound 解析生效单轮上限（≤0 回落缺省）。
func (s *CheckpointSweeper) effectiveRound() int {
	if s.Config.MaxPerRound <= 0 {
		return DefaultCheckpointRound
	}
	return s.Config.MaxPerRound
}

// Scan 单轮：以间隔为上限的子 ctx 驱动 Sweep 面（与 CheckpointSweeper 同
// 纪律：慢 Store 不得挂死循环，到点未完即弃——
// checkpoint 幂等，下轮重扫）并透传起始数。可独立调用（确定性测试面）。
func (s *CheckpointSweeper) Scan(ctx context.Context) int {
	if s.Sweep == nil {
		return 0
	}
	sctx, cancel := context.WithTimeout(ctx, s.effectiveInterval())
	defer cancel()
	return s.Sweep(sctx, s.effectiveRound())
}

// Run 周期扫描直至 ctx 取消。首轮在 Interval 到点后（checkpoint 是
// 新鲜度优化而非拦截面，与 Watchdog 同约定：无需立即扫）。
func (s *CheckpointSweeper) Run(ctx context.Context) {
	runPeriodic(ctx, s.effectiveInterval(), func(c context.Context) { s.Scan(c) })
}

// CheckpointSweep 起始一轮周期 checkpoint：对每个脏实体（Active + 实现
// rans.Snapshotter + 活动纪元 > 已 checkpoint 纪元 + 无在飞）经
// CaptureOnLoop 入队捕获；每候选一个收尾 goroutine 等结果、落 Store、
// 记纪元、清在飞门。返回本轮起始的捕获数（≤ maxCount；≤0 视为缺省
// 上限由调用方给定，这里直接 0）。Put 失败仅留痕不记纪元（下轮重试）；
// 空快照不落库但记纪元（该纪元无内容可写）。
func CheckpointSweep(ctx context.Context, tbl *activation.Table, maxCount int) int {
	if tbl.SnapshotStore() == nil || maxCount <= 0 {
		return 0
	}
	// 锁内快照、锁外读状态（idleScan/看门狗视图同款锁序纪律：readMu 内
	// 不取 p.mu——两锁序并存是环隐患温床；
	// p.mu/readMu 无反序持有点，快照后锁外读状态无竞态）。
	type snap struct {
		id  rans.ID
		p   *pawnite.Pawnite
		ent rans.Entity
	}
	var snaps []snap
	for _, loc := range tbl.Locals() {
		ent, ok := tbl.EntityOf(loc.ID)
		if !ok {
			continue
		}
		snaps = append(snaps, snap{id: loc.ID, p: loc.Pawn, ent: ent})
	}

	cands := make([]snap, 0, len(snaps))
	for _, s := range snaps {
		if len(cands) == maxCount {
			break
		}
		if s.p.State() != pawnite.StateActive {
			continue
		}
		if _, ok := s.ent.(rans.Snapshotter); !ok {
			continue // 无快照能力面：裸激活语义
		}
		if !tbl.TryBeginCheckpoint(s.p) {
			continue // 干净（纪元未越过）或在飞/并发扫轮竞态：让先手
		}
		cands = append(cands, s)
	}

	for _, c := range cands {
		captureCheckpoint(ctx, tbl, c.p, c.ent.(rans.Snapshotter), c.id) // 上面已断言
	}
	return len(cands)
}

// captureCheckpoint 单实体一轮 checkpoint：loop 捕获（静默点）→ 收妥后
// Store.Put（锁/IO 都不在 loop 上——Handle 不因 checkpoint 阻塞）→ 记
// 纪元清在飞门。done 必达（pawnite 终局排空保证），收尾 goroutine 不悬挂。
func captureCheckpoint(ctx context.Context, tbl *activation.Table, p *pawnite.Pawnite, carrier rans.Snapshotter, id rans.ID) {
	done := make(chan pawnite.CaptureOutcome, 1)
	if !p.CaptureOnLoop(carrier.Snapshot, done) {
		tbl.AbortCheckpoint(p) // gate 拒（非 Active/已排队）：归还门
		return
	}
	go func() {
		out := <-done
		switch {
		case out.Skipped:
			tbl.AbortCheckpoint(p)
			return // 让渡给销毁/迁移：那些路径自有快照语义
		case out.Err != nil:
			tbl.AbortCheckpoint(p)
			log.Warn("rans/activation: checkpoint capture failed (retry next sweep)",
				"uuid", id.String(), "err", out.Err)
			return
		}
		if len(out.Snapshot) == 0 {
			tbl.EndCheckpoint(p, out.Epoch) // 该纪元无内容可写：视为已 checkpoint
			return
		}
		// 落库 ctx 脱离扫描轮取消（Scan 返回即 cancel，早于本收尾 Put），
		// 重挂本轮 deadline：到点未完即弃。
		pctx := context.WithoutCancel(ctx)
		if dl, ok := ctx.Deadline(); ok {
			var cancel context.CancelFunc
			pctx, cancel = context.WithDeadline(pctx, dl)
			defer cancel()
		}
		// 围栏：Put 携带本 incarnation 纪元——收尾 goroutine 与实例生命周期
		// 解耦（ckptInFlight 是实例内的门，跨 incarnation 不设防）。stale
		// 拒写=更新的化身已落库（本实例确定失主）：围栏该实例并按实例身份
		// 异步销毁（stop 形态不落终快照），其余失败留痕下轮重扫。
		if err := tbl.SnapshotStore().Put(pctx, id, p.Incarnation(), out.Snapshot); err != nil {
			tbl.AbortCheckpoint(p) // 先归还在飞门：retireCore 的 AwaitCheckpointSettled 不空转
			if errors.Is(err, checkpoint.ErrStaleIncarnation) {
				p.Fence()
				tbl.MetricsRegistry().Counter(metrics.NameOwnershipStaleFenced,
					"stale-incarnation writes that fenced or dropped the writing instance").Add(1)
				log.Info("rans/activation: checkpoint rejected as stale; instance fenced for identity-guarded destroy",
					"uuid", id.String(), "incarnation", p.Incarnation())
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), staleDestroyBudget)
					defer cancel()
					if err := DestroyPawn(ctx, tbl, id, p); err != nil {
						log.Warn("rans/activation: stale-instance destroy failed (entity stays fenced)",
							"uuid", id.String(), "err", err)
					}
				}()
				return
			}
			log.Warn("rans/activation: checkpoint put failed (retry next sweep)",
				"uuid", id.String(), "err", err)
			return
		}
		tbl.EndCheckpoint(p, out.Epoch)
	}()
}
