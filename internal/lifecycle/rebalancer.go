package lifecycle

// 自动再平衡与闲置卸载（规格 §14.3 E2 近似档 + G1 闲置回收，卸载面经
// *activation.Table 协作；offloadIdle 只服务本包编排器，外部经
// NewRebalancer/RebalancerService 消费）：
//   - RebalancerService：本地实体数超阈 → 注入的卸载面按单轮上限卸载
//     （表复合体 = offloadIdle：migrate 模式闲置候选最冷优先经 Migrate
//     迁给放置远端）。近似档——本地超阈卸载，全局均衡需负载视图（后续）；
//   - offloadIdle：卸载一轮闲置实体（RebalancerService 的表驱动卸载面，
//     包内直调）。
//
// 语义唯一出处 docs/porting/ARCHITECTURE.zh-CN.md §14.3/§14.1 备注。

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cyinx/rans_golang/internal/activation"
)

// offloadIdle 卸载一轮闲置实体（规格 §14.3 E2：包内编排面——Rebalancer
// 的表驱动卸载实现）：migrate 模式候选最冷优先逐个经 Migrate 迁给放置
// 远端，单轮至多 maxCount 个。返回 (成功迁移数, errors.Join 汇总失败)。
//
// 语义要点：
//   - 候选快照于调用入口：快照后转热的实体仍会被迁移（近似档——迁移
//     本身语义无损，在箱消息整箱随迁）；
//   - ErrNoEligibleTarget 是配置性全局事实：首个候选即命中则短路返回，
//     不再对其余候选空转；
//   - ctx 耗尽同理短路（后续 Migrate 只会以同一输因失败）；
//   - 单实体迁移输（ErrMigrationLost 等）不阻断其余候选——输者保持
//     Active，下轮自然重试。
func offloadIdle(ctx context.Context, tbl *activation.Table, maxCount int) (int, error) {
	if maxCount <= 0 {
		return 0, nil
	}
	ids := idleScan(tbl, activation.CollectMigrate, 0, maxCount)
	if len(ids) == 0 {
		return 0, nil
	}
	var (
		n    int
		errs []error
	)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return n, errors.Join(append(errs, err)...)
		}
		err := Migrate(ctx, tbl, id)
		switch {
		case err == nil:
			n++
		case errors.Is(err, ErrNoEligibleTarget):
			// 全局配置事实：剩余候选无点可选，短路。
			return n, errors.Join(append(errs, err)...)
		default:
			errs = append(errs, fmt.Errorf("offload %s: %w", id.String(), err))
		}
	}
	return n, errors.Join(errs...)
}

// 再平衡缺省参数（规格 §14.3：60s 周期、单轮上限 10、阈值缺省 0=关闭）。
const (
	DefaultRebalancePeriod      = 60 * time.Second
	DefaultRebalanceMaxPerRound = 10
)

// RebalancerService 本地超阈卸载编排器（规格 §14.3 近似档）。全字段可
// 注入：nil 计数/卸载面 → Scan 空转；Threshold ≤0（零值缺省）→ 关闭。
// 表驱动形态经 NewRebalancer 构造（卸载面 offloadIdle 收内部）。
type RebalancerService struct {
	// LocalCount 本地实体计数面（表侧 = len(local) 快照）。nil →
	// Scan 空转。
	LocalCount func() int
	// OffloadIdle 卸载面（表驱动形态 = offloadIdle：migrate 模式
	// 闲置候选最冷优先迁给放置远端）。返回值与错误在本编排器视角都是
	// 尽力而为：输的实体保持 Active，下轮自然重试。nil → Scan 空转。
	OffloadIdle func(ctx context.Context, maxCount int) (int, error)
	// Threshold 本地实体数阈值：超过（严格 >）才卸载；≤0（缺省 0）=
	// 关闭（再平衡不启用）。
	Threshold int
	// Period 扫描周期；≤0 → DefaultRebalancePeriod（60s）。
	Period time.Duration
	// MaxPerRound 单轮卸载上限；≤0 → DefaultRebalanceMaxPerRound（10）。
	MaxPerRound int
}

// NewRebalancer 构造表驱动的再平衡编排器（卸载面 offloadIdle
// 收内部，宿主经本构造消费——计数/卸载面预接 activation.Table）。
// threshold/MaxPerRound ≤0 的缺省回落同字段约定。
func NewRebalancer(tbl *activation.Table, threshold, maxPerRound int) *RebalancerService {
	return &RebalancerService{
		LocalCount: func() int {
			entities, _ := tbl.MetricsView()
			return entities
		},
		OffloadIdle: func(ctx context.Context, max int) (int, error) {
			return offloadIdle(ctx, tbl, max)
		},
		Threshold:   threshold,
		MaxPerRound: maxPerRound,
	}
}

// Scan 单轮再平衡：本地计数严格超过 Threshold（且 Threshold > 0）→ 经
// OffloadIdle 卸载至多 MaxPerRound 个。可独立调用（确定性测试面），Run
// 即周期循环本方法。首轮在 Period 到点后（纠正面而非拦截面，同 Watchdog
// 的次序约定——无需立即扫）。
func (r *RebalancerService) Scan(ctx context.Context) {
	if r.Threshold <= 0 || r.LocalCount == nil || r.OffloadIdle == nil {
		return
	}
	if r.LocalCount() <= r.Threshold {
		return
	}
	max := r.MaxPerRound
	if max <= 0 {
		max = DefaultRebalanceMaxPerRound
	}
	_, _ = r.OffloadIdle(ctx, max) // 尽力而为：输者保持 Active，下轮重试
}

// Run 周期再平衡直至 ctx 取消。Period ≤0 回落 DefaultRebalancePeriod。
func (r *RebalancerService) Run(ctx context.Context) {
	period := r.Period
	if period <= 0 {
		period = DefaultRebalancePeriod
	}
	runPeriodic(ctx, period, r.Scan)
}
