package lifecycle

// 迁移编排（规格 §13.1 源端生命周期 + §13.2 不变量 + 《加固 spec》§5.1 第 3 点
// core 矩阵的 Migrate 行）与交接窗公共粘合（编排经 *activation.Table 协作）：
// 选点（placement 注入，排除本地）→ 预算 ctx
//（默认 30s，可配）→ core 闭包注入 pawnite.Migrate（首步等在途周期
// checkpoint 落定 → 有状态实体：快照 → Store.Put（失败即判输）→ 随迁包直发
// 尽力而为 → 目录实例级 TryTransferOwnership CAS 换锁；无状态实体：直接换
// 锁；有状态未配置 Store：拒绝迁移）。CAS 是线性化点：赢 ⟺ 目录已指向目标
//（不存在"源已让渡、目标未接管"窗口），pawnite 随即整箱转发并跳过目录注销
//（本端 cleanup 经 SkipDirUnregister 查询面跳过 UnregisterIfOwner——接线见
// activation 的 cleanup），OnEvict 照常逐出本地条目；输/取消 → pawnite 回退
// Active 继续服役，返回携带输因的 ErrMigrationLost。

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// DefaultMigrationBudget 迁移总预算默认值（规格 §13.1：linked CTS 30s 的
// Go 形态——context.WithTimeout）。
const DefaultMigrationBudget = 30 * time.Second

var (
	// ErrMigrationUnknown：迁移目标不在本地注册表（未激活/已逐出），零副作用
	// （NotFound+NotLocal——管理操作目标面，分类唯一表不重试）。
	ErrMigrationUnknown = rans.NewError(rans.CodeNotFound, rans.ReasonNotLocal,
		"rans: migrate: target not locally active")
	// ErrNoEligibleTarget：placement 无候选（未注入选点函数、返回 false 或
	// 返回本地自身——单节点集群、全员下线等）。实体保持 Active，零副作用。
	ErrNoEligibleTarget = rans.NewError(rans.CodeUnavailable, rans.ReasonNoEligibleNode,
		"rans: migrate: no eligible target")
	// ErrMigrationLost：迁移输/被抢占（目录换锁 CAS 落败、预算取消、销毁
	// 介入）。pawnite 已回退 Active；错误链携带输因（directory.ErrNotOwner /
	// context.DeadlineExceeded 等，errors.Is 可判）。
	ErrMigrationLost = rans.NewError(rans.CodeError, rans.ReasonMigrationLost,
		"rans: migrate: lost")
	// ErrMigrationNoStore 有状态实体未配置 Store 时迁移被拒（《加固 spec》
	// §5.1 第 3 点表：只靠随迁包时包丢失即状态丢失——有状态 kind 的
	// 迁移以 Store 落库为前提）。经 Migrate 返回时与 ErrMigrationLost
	// 同链（fmt.Errorf 双 %w），errors.Is 两者皆命中；实体保持 Active 零副作用。
	ErrMigrationNoStore = errors.New("rans: migrate: refusing stateful entity without checkpoint store")
)

// Migrate 把本地实体迁移到 placement 选出的目标节点（规格 §13.1
// PawniteManager.Migrate 同构；阻塞至裁决）：
//
//  1. GetLocal miss → ErrMigrationUnknown；placement 缺席/无候选 →
//     ErrNoEligibleTarget（两者均零副作用，实体保持 Active）；
//  2. 预算 ctx = 调用方 ctx + WithTimeout(预算)；
//  3. core 闭包（契约见 migCore）注入 pawnite.Migrate——阻塞至
//     裁决返回；
//  4. Won：目录已指向目标、在箱消息已整箱转发、本地已逐出（cleanup 已
//     跳过目录注销），随后在锁外触发 onMigrated（LocationCache
//     PutPositive(新位置) 接线面；次序在 onEvicted 之后，赢家条目不被
//     失效清理掉——同 arbLost 次序契约），返回 nil；
//  5. 输/取消：pawnite 已回退 Active；返回 ErrMigrationLost（链上携带
//     输因）或确定性前置错误（pawnite.ErrNotActive——并发销毁/二次迁移
//     抢窗，原样透传）。
func Migrate(ctx context.Context, tbl *activation.Table, id rans.ID) error {
	p, ok := tbl.GetLocal(id)
	if !ok {
		return ErrMigrationUnknown
	}
	placement := tbl.Placement()
	if placement == nil {
		return ErrNoEligibleTarget
	}
	self := tbl.SelfNode()
	// kind 感知迁移选点：选点与首激活共用
	// kind 维——node 侧 placementBridge.PickForKind 按声明承载 id.Kind 的
	// 节点过滤候选（纯接入节点永不成为迁移目标）。
	target, ok := placement(id.Kind, self)
	if !ok || target == self {
		return ErrNoEligibleTarget
	}
	budget := tbl.MigrationBudget()
	if budget <= 0 {
		budget = DefaultMigrationBudget
	}
	migCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	// 实体在 core 内做快照——core 运行于 pawnite loop goroutine（静默点），
	// 故先于 pawnite.Migrate 取实体引用（与逐出同临界区写入，GetLocal
	// 命中即有效；极小竞态窗内被逐出则按未激活处理）。
	ent, ok := tbl.EntityOf(id)
	if !ok {
		return ErrMigrationUnknown
	}

	// cause 由 core（loop goroutine）经容量 1 缓冲通道回传——通道操作自身
	// 无数据竞态。正常裁决路径下发送先于 pawnite 的 result 回写（happens-before
	// 链保证此处必读到）；core 卡死、调用方按 ctx 脱离等待时读不到输因
	//（此刻主错误已是 ctx.Err()，语义不缺——迁移输于预算耗尽）。
	causeCh := make(chan error, 1)
	core := func(cctx context.Context) pawnite.MigrationOutcome {
		outcome, cerr := migCore(cctx, tbl, id, ent, target, p)
		if cerr != nil {
			causeCh <- cerr // 至多一次发送，容量 1 永不阻塞
		}
		return outcome
	}

	err := p.Migrate(migCtx, target, core)
	var cause error
	select {
	case cause = <-causeCh:
	default:
	}
	switch {
	case err == nil:
		// Won：pawnite 终局收尾（整箱转发 + finish + OnEvict cleanup 跳注销）
		// 已在裁决回传前完成；锁外触发赢家钩子。
		tbl.NotifyMigrated(id, target)
		return nil
	case errors.Is(err, pawnite.ErrNotActive):
		// 前置失败（并发销毁/二次迁移抢窗）：pawnite 语义原样透传，零副作用。
		return err
	default:
		return migrationLossError(migCtx, err, cause)
	}
}

// migCore 迁移核心（规格 §13.1 B/B2/C + 《加固 spec》§5.1 第 3 点 core 矩阵
// Migrate 行）：
// 运行在 pawnite loop goroutine 上（串行即静默）。core 契约：
//   - 首步 AwaitCheckpointSettled（《加固 spec》§5.1 第 2 点）：等
//     在途周期 checkpoint 落定再拍终快照——终快照不被更旧的同纪元快照后到
//     覆盖；预算耗尽于此即判输；
//   - 快照捕获在 core 内进行（静默点：此刻无在途执行，快照一致；快照先于
//     换锁后的源端 Deactivate——Deactivate 对实体状态的修改不进快照）；
//   - 「有状态」（实现 rans.Snapshotter）且配置 Store 是迁移前提：快照 →
//     Store.Put（失败即判输——Store 是正确性依赖）→ 随迁包
//     直发（尽力而为快路径，失败仅记录）→ TryTransferOwnership；有状态但
//     未配置 Store → 拒绝迁移（ErrMigrationNoStore——只靠随迁包时包丢失即
//     状态丢失）；无状态 → 直接换锁；
//   - 预算由形参 ctx 强制——core 必须尊重 ctx 取消（所有 IO 调用都携带
//     ctx；实体 Snapshot 无 ctx 参数，属实体自身契约）；
//   - 返回 Won=true 当且仅当目录换锁成功（CAS 是线性化点）。
//
// 步骤（随迁包为唯一尽力而为项——失败仅记录，at-most-once 快路径，规格
// §13.3；正确性依赖是刚落库的 Store）：
//  1. 有状态且配置 Store：捕获快照（失败按 Put 失败处理——判输）；snap 非空
//     → store.Put（围栏携带源端 incarnation；失败判输）→ 随迁包
//     直发（可缺席/失败）；
//  2. TryTransferOwnership(self,inc → target)（实例级：期望归属
//     是本实例的 Owner{node, inc}——同 node 新实例不被本实例的迟到换锁
//     误伤；换锁成功即目录升纪元 K）。换锁能力经类型断言发现（实例级换锁
//     是可选能力面 directory.TransferDirectory；仓库自带
//     实现 inmem/redis 均具备，自定义目录缺该面时迁移输——错误链响亮
//     留痕）。返回的 newInc 供观测
//     留痕（目标端不消费：目标端激活走 Register 自指向收养升纪元，得自己
//     的新纪元——跳号合法）。(true,nil)=赢；(false,nil)=条目已消失（输，
//     无特定输因）；(false,ErrNotOwner)=归属不符（输）；其余错误=输。
//     返回的 cause 供 Migrate 错误链使用。
func migCore(ctx context.Context, tbl *activation.Table, id rans.ID, ent rans.Entity, target rans.NodeID, p *pawnite.Pawnite) (pawnite.MigrationOutcome, error) {
	if err := awaitSettled(ctx, tbl, p, "migrate"); err != nil {
		return pawnite.MigrationOutcome{}, err
	}
	carrier, stateful := ent.(rans.Snapshotter)
	if stateful {
		store := tbl.SnapshotStore()
		if store == nil {
			// 有状态但未配置 Store：拒绝迁移（spec §5.1 第 3 点表——只靠
			// 随迁包时包丢失即状态丢失）。零副作用：窗回退 Active。
			return pawnite.MigrationOutcome{}, ErrMigrationNoStore
		}
		b, err := carrier.Snapshot()
		if err != nil {
			// 快照捕获失败按 Put 失败处理：判输（状态未落地不得让渡所有权
			// ——换锁成功而快照未落 = 新家裸激活，已确认写丢失）。
			return pawnite.MigrationOutcome{}, fmt.Errorf("rans: migrate: snapshot capture failed: %w", err)
		}
		if len(b) > 0 {
			// Put 失败即判输（Store 是正确性依赖）：围栏
			// 携带源端 incarnation——强弃换代后晚落的迁移快照按
			// ErrStaleIncarnation 被拒（判输，窗回退）。
			if err := store.Put(ctx, id, p.Incarnation(), b); err != nil {
				return pawnite.MigrationOutcome{}, fmt.Errorf("rans: migrate: checkpoint put failed: %w", err)
			}
			// 随迁包直发（规格 §13.3 E1 快路径）：at-most-once，失败仅记录
			// ——目标端未命中包时从刚落库的 Store 恢复，语义无损。
			if send := tbl.PackageSender(); send != nil {
				if err := send(ctx, target, id, b); err != nil {
					log.Warn("migration package send failed (best effort)",
						"uuid", id.String(), "target", target, "err", err)
				}
			}
		}
	}
	// 换锁（Directory 合并端口本面方法，无能力断言形态）：期望归属三元组
	//（本实例 node+epoch+inc），目标归属 node+epoch——目标代际从成员注册
	// 面取（目录实现 NodeRegistry 即查；未实现/未注册回落 0 形态，条目
	// 有效性判定按「未接代际」处理）。
	targetEpoch := uint64(0)
	if reg, ok := tbl.Directory().(directory.NodeRegistry); ok {
		if ep, alive, err := reg.NodeEpoch(ctx, target); err == nil && alive {
			targetEpoch = ep
		}
	}
	won, newInc, _, err := tbl.Directory().Transfer(ctx, id,
		directory.Owner{Node: tbl.SelfNode(), Epoch: tbl.SelfEpoch(), Incarnation: p.Incarnation()},
		directory.Owner{Node: target, Epoch: targetEpoch})
	if err != nil {
		// 含 ErrNotOwner（归属不符）：输因随 core 错误链上行——调用方
		// 据此分辨 CAS 输与预算取消（migrationLossError 的裁决输入）。
		return pawnite.MigrationOutcome{}, fmt.Errorf("rans: transfer: ownership CAS failed: %w", err)
	}
	if !won {
		// (false, nil)：条目已不存在（竞态消失）——输但无特定输因。
		return pawnite.MigrationOutcome{}, nil
	}
	log.Info("rans/activation: migration ownership transferred to target",
		"uuid", id.String(), "from", tbl.SelfNode(), "to", target, "new_incarnation", newInc)
	return pawnite.MigrationOutcome{Won: true}, nil
}

// wrapsCancellation 与 pawnite.wrapsCancellation 同判据（cause
// 通道镜像；两包各自持有是依赖方向使然）：err 携带取消签名（context.
// Canceled/DeadlineExceeded）即可能是预算取消并集坍缩的产物，预算优先
// 语义对其保留裁决权；不含签名者是确定性拒绝，优先于预算错误回传。
func wrapsCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// migrationLossError 迁移输/取消的错误链装配（Migrate 默认分支的纯函数，
// 决策可直测）。输入三源：pawnite 返回的裁决错误（ErrNotActive
// 已在上游截住）、cause 通道的 core 自报错误（pawnite.Migrate 包装丢弃
// MigrationOutcome 的 Err，这里是 core 错误到达调用方的唯一通道）、migCtx
// 的预算状态。注意端到端的固有边界：预算先于 core 完成耗尽时，pawnite 的
// 等待 select 即刻以 ctx.Err() 返回（caller-detached），cause 往往尚未发送
// ——本函数拿不到 cause，预算错误是唯一诚实表述（迁移输于预算耗尽）；
// 「确定性拒绝 + 预算耗尽」可观测形态由 cause 在场时的优先级钉死（
// TestMigrationLossErrorDeterministicCauseBeforeBudget）。
//
// 优先级：cause 的确定性拒绝（不含取消签名——wrapsCancellation 判据）→
// 预算错误 → cause（取消签名，预算仍活跃）→ pawnite 原始错误。预算优先的
// 依据与 pawnite.handoffLossError 同源：core ctx 是预算的取消并集派生
// （WithCancel 派生遮蔽父级 Deadline），含取消签名的 cause 只是
// 并集坍缩的产物——错误链保留 DeadlineExceeded 调用方才能分辨「预算耗尽」
// 与「被取消」。预算仍活跃时按原序保留 ErrMigrationLost 哨兵与具体输因
// （CAS 的 directory.ErrNotOwner / core 自报错误等）。
func migrationLossError(migCtx context.Context, err, cause error) error {
	// 确定性拒绝优先于预算错误（预算检查殿后——若预算检查在前，
	// 确定性拒绝会被 DeadlineExceeded 整体吞掉）：cause 不含取消
	// 签名时它是诚实输因（ErrMigrationNoStore、Put 实败等——重试仍将
	// 复现），预算同时耗尽也不遮蔽。
	if cause != nil && !wrapsCancellation(cause) {
		return fmt.Errorf("%w: %w", ErrMigrationLost, cause)
	}
	// 预算耗尽时以预算错误为输因权威。
	if berr := migCtx.Err(); berr != nil {
		return fmt.Errorf("%w: %w", ErrMigrationLost, berr)
	}
	if cause != nil {
		return fmt.Errorf("%w: %w", ErrMigrationLost, cause)
	}
	return fmt.Errorf("%w: %w", ErrMigrationLost, err)
}

// errHandoffNotLocal 交接驱动面（passivateInstance/retireInstance）的
// 前置失败：目标不在本地注册表（未激活/已终局逐出）——零副作用。
var errHandoffNotLocal = errors.New("rans: handoff: target not locally active")

// handoffTarget 交接驱动面的前置取对：实例与实体引用先于开窗取得（与
// Migrate 同理——core 运行在 loop 上，引用取点与逐出同临界区写入，
// 命中即有效；极小竞态窗内被逐出按不在册处理）。
func handoffTarget(tbl *activation.Table, id rans.ID) (*pawnite.Pawnite, rans.Entity, error) {
	p, ok := tbl.GetLocal(id)
	if !ok {
		return nil, nil, errHandoffNotLocal
	}
	ent, ok := tbl.EntityOf(id)
	if !ok {
		return nil, nil, errHandoffNotLocal
	}
	return p, ent, nil
}

// awaitSettled 三 core 统一首步（《加固 spec》§5.1 第 2 点）：等
// 在途周期 checkpoint 落定再拍终快照——否则更旧的同纪元快照可能后到覆盖终
// 快照（Store 对同纪元允许覆盖，围栏拦不住同纪元写）。预算 ctx
// 耗尽返回错误，core 据此判败/中止（不带着在飞 IO 强行转换）。what 是错误
// 文案的交接类别。
func awaitSettled(ctx context.Context, tbl *activation.Table, p *pawnite.Pawnite, what string) error {
	if err := tbl.AwaitCheckpointSettled(ctx, p); err != nil {
		return fmt.Errorf("rans: %s: aborted waiting for in-flight checkpoint: %w", what, err)
	}
	return nil
}
