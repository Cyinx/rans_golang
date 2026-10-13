package pawnite

// 交接窗状态机（《加固 spec》§5.1；迁移窗正本规格 §13.1/§13.2 + §19 陷阱 3）：
// pending 先于 Destroying、在箱消息留箱等待、交接核心在 loop goroutine 上
// 串行执行（串行即静默）；终局处置按 req.kind 与 outcome——Migrate 赢家
// 整箱转发且跳过目录注销（finish 默认序）、输回退 Active；Passivate/Retire
// 成功走 finishRetiredInOrder 显式次序（Deactivate → 实例级注销 → 在箱
// 回绝 → Destroyed，§5.1 第 4 点）、失败回退 Active。目录 CAS、随迁包发送
// 等迁移实质动作由 manager 注入的 core 闭包承担；pawnite 只负责时序骨架与
// 收件箱语义。

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/log"
)

// MigrationOutcome 迁移核心的裁决结果（规格 §13.1 C 步）——Migrate
// 专用形态（Won=目录换锁成功，目录已指向目标，源端随即整箱转发并跳过
// 目录注销；零值=输/取消，迁移窗回退、单元恢复服役）。泛化形态见
// HandoffOutcome。
type MigrationOutcome struct {
	Won bool
}

var (
	// ErrNotActive：Handoff/Migrate 前置校验失败（当前非 Active——
	// 交接只能从活跃态发起，规格 §13.1；Created/Activating/交接窗内
	//（Migrate/Passivate/Retire 互斥窗）/终态均拒绝）。
	ErrNotActive = errors.New("pawnite: handoff requires StateActive")
	// ErrMigrationLost：迁移输/被抢占（目录换锁失败、预算取消或销毁介入）。
	ErrMigrationLost = errors.New("pawnite: migration lost")
)

// errHandoffCorePanic 交接 core panic 折输的包内哨兵：Passivate/Retire 以
// Err==nil 为成功，recover 若折零值恰好落进成功编码（panic 被判成完成）
// ——故 panic 必须显式落 Err。三 kind 统一生效（recover 在 pawnite 侧，
// wrapper 拦不住）；Migrate 调用方收本哨兵而非 ErrMigrationLost（manager
// 错误链在默认分支包一层 ErrMigrationLost，消费方判据不变）。
var errHandoffCorePanic = errors.New("pawnite: handoff core panicked")

// handoffReq 交接信号：由 Handoff 入队、loop goroutine 消费。
type handoffReq struct {
	ctx        context.Context // 调用方预算 ctx（core 取消并集的一源，见 handoffCoreCtx）
	kind       HandoffKind
	targetNode rans.NodeID // Migrate：Won 后整箱转发地址；其余 kind 零值未用
	core       func(ctx context.Context) HandoffOutcome
	result     chan error // 裁决回传（容量 1：至多一次发送）
}

// Migrate 发起迁移并阻塞至裁决（规格 §13.1 源端生命周期）——
// Handoff(HandoffMigrate) 的薄包装：迁移 core
// 的「Won 后整箱转发 + 跳过目录注销」收口语义由 Handoff 按 kind 承担。
// 开窗时序（陷阱 3 同临界区）、入队纪律、等待段预算绑定、组合规则与
// 取消面，契约正本见 Handoff。
func (p *Pawnite) Migrate(ctx context.Context, targetNode rans.NodeID, core func(ctx context.Context) MigrationOutcome) error {
	if core == nil {
		return errors.New("pawnite: nil migration core")
	}
	return p.Handoff(ctx, HandoffMigrate, targetNode, func(c context.Context) HandoffOutcome {
		return HandoffOutcome{Won: core(c).Won}
	})
}

// runHandoff 在 loop goroutine 上执行交接核心（串行即静默，《加固 spec》
// §5.1 + 规格 §13.1 A）。返回 true = 已达终局（run 应退出循环）。
//
// Go 形态与规格 FIFO 的偏差：select 对 inbox 与
// handoffCh 的就绪分支随机选择，交接信号入队前的在箱消息既可能在 core
// 之前本地执行（窗开前的合法工作），也可能在终局处置中被整箱转发/回绝
// ——Migrate 的 CAS 才是线性化点，两种交错均保持恰好一次语义。规格"在途
// >0 借道队列重排（FIFO 保证挂起续体先落地）"在单 loop 模型下由"core 即
// loop 任务"自然满足：正在执行的 invoke 必先于 core 完成。
func (p *Pawnite) runHandoff(hookCtx context.Context, req handoffReq) bool {
	// core 尚未开跑前销毁已介入：交接作废（输），终局交还 stop 分支。
	// 此预检兜底 select 随机性（stop 与 handoffCh 同时就绪时 handoffCh
	// 可能先被选中）；core 一旦开跑则照常处置结果（销毁竞态由成功/输
	// 路径各自收束）。
	if p.stopping() {
		p.loseHandoff(false, nil)
		req.result <- ErrMigrationLost
		return false
	}
	// core ctx 取消并集：hookCtx 派生（值链照旧携带）
	// + 调用方预算 ctx 的取消——看门狗强弃经 cancelHooksForAbandon 取消
	// hookCtx 时协作 core 得以尽早返回；调用方预算耗尽同样可达 core。
	coreCtx, cancelCore := p.handoffCoreCtx(hookCtx, req.ctx)
	// 排队捕获避让：core 即 loop 任务，若捕获已入队未拾取，
	// 其收尾方（manager 侧等捕获结果才落在飞门）要等 loop 让出 core 才能被
	// 驱动——core 内等门（AwaitCheckpointSettled）即自旋至预算耗尽。core 前
	// 排空一次即结构性完备：CaptureOnLoop 的 Active 校验与入队同持 mu，窗开
	//（pending+Destroying）同在 mu 临界区——凡窗开前合法入队者必经「锁内
	// 入队 → handoffCh 锁内入队 → loop 拾取」的 happens-before 链先于本
	// drain 可见，此处必然排空；窗开后再无入队（非 Active 天然拒绝，在飞门
	// 由 manager 收尾 goroutine 归还）。排空者以 Skipped 收场（零副作用，
	// manager 下轮扫轮重试）。
	p.drainPendingCaptures()
	outcome := p.safeHandoffCore(req, coreCtx)
	cancelCore() // 释放并集看护（AfterFunc stop + cancel；幂等）
	if handoffSucceeded(req.kind, outcome) {
		p.settleHandoffSuccess(hookCtx, req)
		req.result <- nil
		return true
	}
	// 输/失败/取消：清窗 + 结算跟随者 + 回退 Active（销毁未介入时）。
	// 销毁/强弃已介入（窗未回退、单位走向终局）时回输输哨兵而非 core 自报
	// 错——调用方预算并未耗尽时 core 观察到的 context.Canceled（来自强弃
	// 取消面）对调用方有误导性，ErrMigrationLost 才是「窗已作废」的诚实
	// 裁决。
	loss := handoffLossError(req, outcome)
	if p.loseHandoff(true, loss) {
		req.result <- loss
	} else {
		req.result <- ErrMigrationLost
	}
	return false
}

// settleHandoffSuccess 交接成功的终局处置（loop goroutine 上，裁决回传前
// 完成——Handoff 返回 nil 即终局已落），按 req.kind（在飞窗类别）分流：
//   - Migrate：先落终局原因 Migrated（写一次，先于 finish 的 Retired）——
//     供 manager 清理钩子经 SkipDirUnregister 查询跳过 UnregisterIfOwner，
//     并供终局复扫/准入面迟到者经 disposeLate 矩阵走转发；随后整箱转发
//     目标（注册已指向目标，§13.2）、finish 收尾（源端 Deactivate 在换锁后
//     落地，与目标端 Activate 的重叠是规格允许的持久写令牌裁决面）；
//   - Passivate/Retire：走 finishRetiredInOrder 的显式终局次序（规格 §5.1
//     第 4 点）——用户 Deactivate → 实例级条件注销 → 在箱以 StatusUnavailable
//     （确定未执行）回绝 → Destroyed，不复用 finish 默认序。
//
// 共用收尾：组合规则跟随者结算（nil=已完成）→ 按 kind 的终局处置 → 兜底
// 结算一次——窗在终局复扫前保持放行（handoffPending 不清），钩子窗内迟到
// 的 Retire 跟随者等到终局落地才结算（语义即「等在飞交接走完」，有界于
// 终局收尾）。迟到者统一经 disposeLate 矩阵分流：Migrated 转投目标，其余
// 回绝。
func (p *Pawnite) settleHandoffSuccess(ctx context.Context, req handoffReq) {
	p.settleFollowersNow(nil)
	switch req.kind {
	case HandoffMigrate:
		p.mu.Lock()
		p.setTerminalLocked(TerminalMigrated, req.targetNode)
		p.mu.Unlock()
		p.drainForwardTo(req.targetNode, rans.ReasonStaleLocation)
		p.finish(ctx)
	default: // HandoffPassivate / HandoffRetire
		p.finishRetiredInOrder(ctx)
	}
	p.settleFollowersNow(nil) // 兜底：终局收尾钩子窗内迟登记的跟随者
	p.drainPendingCaptures()  // 终局排空：loop 不再拾取，未决捕获以 Skipped 收场
}

// finishRetiredInOrder Passivate/Retire 成功的终局处置次序（规格 §5.1 第 4
// 点；finish 的交接窗显式次序变体——本路径不经 finish）：
//
//	快照（core 内已拍，先于本函数）→ 用户 Deactivate → 实例级条件注销
//	（OnEvict 单胜钩子承载 manager cleanup：releaseLease → UnregisterIfOwner
//	→ 本地逐出 → onEvicted）→ 在箱以 StatusUnavailable（确定未执行）回绝 →
//	Destroyed。
//
// 次序不变量：
//   - 快照先于用户 Deactivate：Deactivate 只做资源收尾，它对实体状态的修改
//     不进快照（godoc 契约，manager 三 core 的 Snapshot 在 core
//     内先拍，规格 §5.1 第 5 点）；
//   - 注销在 Deactivate 之后：同 id 新实例的 Activate 不与本实例 Deactivate
//     重叠；
//   - 注销在在箱回绝之前：被回绝的调用方重试时目录已空、本地已逐出——重试
//     即刻落新激活，从 Store 恢复（包含终快照）。
//
// 与 finish 默认序（Deactivate → Destroyed → 复扫 → OnEvict，注销最后）的
// 差异即次序本身：注销殿后会让「回绝早于注销」——调用方立刻重试会命中尚未
// 让位的目录条目与未逐出的本地实例，落空一轮。幂等性与 finish 同源：状态
// 条件转移、setTerminalLocked 写一次、OnEvict evictOnce 单胜、复扫防御性
// 兜底——看门狗强弃抢先终局后 zombie 续体经过本函数无二次副作用。
func (p *Pawnite) finishRetiredInOrder(ctx context.Context) {
	if p.opts.Deactivate != nil {
		p.safeDeactivate(ctx)
	}
	// 实例级条件注销 + 本地逐出（OnEvict 恰一次；本路径 terminal 尚未写
	// Migrated，SkipDirUnregister 为假——注销照常执行）。
	p.runOnEvictOnce()
	// 在箱回绝（注销之后）：确定未执行，可重试——此刻目录已空、本地已逐出，
	// 重试落新激活（从 Store 恢复终快照）。
	p.mu.Lock()
	pending := p.takeInboxLocked()
	p.mu.Unlock()
	for _, d := range pending {
		// 交接已完成、投递未执行（确定未执行）：按销毁族回绝（Deactivating）
		// ——重试落新激活，从 Store 恢复终快照。
		d.Reject(rans.CodeUnavailable, rans.ReasonDeactivating)
	}
	// Destroyed 翻转 + 终局原因落位（同一临界区含防御性复扫——回绝回调执行
	// 期间准入窗仍放行留箱，翻转前入箱者在此收口，经 disposeLate 的 Retired
	// 行回绝）。
	p.mu.Lock()
	late := p.takeInboxLocked()
	if p.state != StateDestroyed {
		p.state = StateDestroyed
		p.stateSince = time.Time{}
	}
	p.setTerminalLocked(TerminalRetired, 0)
	p.mu.Unlock()
	p.disposeLate(late)
}

// settleFollowersNow 取走在册跟随者并按 settleErr 结算（nil=已完成）。
func (p *Pawnite) settleFollowersNow(settleErr error) {
	p.mu.Lock()
	followers := p.takeHandoffFollowersLocked()
	p.mu.Unlock()
	p.settleFollowers(followers, settleErr)
}

// loseHandoff 交接输/失败的窗内清理：清 pending（窗关闭，准入恢复销毁
// 语义）+ 结算跟随者；restore 时若仍是交接所致 Destroying（销毁未介入
// ——stopClosed 为假）则回退 Active 并返回 true。销毁已介入时不回退
// （返回 false）：终局由 stop 分支完成（状态机单调性），此时跟随者按
// 「单位走向终局」结算为已完成（nil）而非接棒。loss 是跟随者的结算值
// （仅窗回退时送达——Retire 等待者接棒自开窗）。
func (p *Pawnite) loseHandoff(restore bool, loss error) bool {
	p.mu.Lock()
	p.handoffPending = false
	followers := p.takeHandoffFollowersLocked()
	settle := error(nil)
	restored := false
	if restore && p.state == StateDestroying && !p.stopClosed {
		p.state = StateActive
		p.stateSince = time.Now() // 回退 Active：状态进驻时钟重盖（非清零）
		settle = loss             // 窗回退 Active：跟随者接棒重试
		restored = true
	}
	p.mu.Unlock()
	p.settleFollowers(followers, settle)
	return restored
}

// abortPendingHandoff 销毁/强弃对在飞交接的抢占（loop 的 stop 分支与
// abandon 调用）：清窗 + 跟随者按「单位走向终局」结算（已完成——Retire
// 无须接棒）+ 给尚未执行的交接请求回输（至多一个——状态机门控；含挂起等
// 在途 work 的让位形态，见 heldHandoff），core 绝不执行（已执行的卡死 core
// 由取消面通知 + zombie 续体让位收束）。收集持 mu、回传/结算在锁外
// （陷阱 2 纪律）。
func (p *Pawnite) abortPendingHandoff() {
	p.mu.Lock()
	p.handoffPending = false
	followers := p.takeHandoffFollowersLocked()
	var aborted []handoffReq
	select {
	case req := <-p.handoffCh:
		aborted = append(aborted, req)
	default:
	}
	var held *heldHandoff
	if p.heldHandoff != nil {
		held = p.heldHandoff
		p.heldHandoff = nil
	}
	p.mu.Unlock()
	if held != nil && held.stopBudget != nil {
		held.stopBudget() // 预算看护解除（已触发则空转；避免看护悬挂到 ctx 释放）
	}
	p.settleFollowers(followers, nil)
	for _, req := range aborted {
		req.result <- ErrMigrationLost
	}
	if held != nil {
		held.req.result <- ErrMigrationLost
	}
}

// takeHandoffFollowersLocked 取走全部组合规则等待者（调用方必须已持 mu）。
func (p *Pawnite) takeHandoffFollowersLocked() []chan error {
	fs := p.handoffFollowers
	p.handoffFollowers = nil
	return fs
}

// settleFollowers 结算组合规则等待者：settleErr==nil = 在飞交接裁决既定
// 成功——Retire 视为已完成（时序注记：成功路径的首结算早于终局翻转，
// nil 后即刻查 State()/SkipDirUnregister() 可能读到未翻转值，需观测终态
// 以 Done 关闭为准）；非 nil = 窗回退 Active——等待者接棒自开窗。
// 每跟随者恰一送（Handoff 登记、跟随者退出等待后由缓冲通道吞掉迟送），
// 发送不阻塞。
func (p *Pawnite) settleFollowers(followers []chan error, settleErr error) {
	for _, f := range followers {
		select {
		case f <- settleErr:
		default: // 容量 1：跟随者已脱等（ctx 耗尽）时落缓冲即弃
		}
	}
}

// handoffSucceeded 裁决成败判据（HandoffOutcome 契约）：Migrate 看 Won
// （目录换锁成败）；Passivate/Retire 以 Err==nil 为成功（Won 恒忽略）。
func handoffSucceeded(kind HandoffKind, o HandoffOutcome) bool {
	if kind == HandoffMigrate {
		return o.Won
	}
	return o.Err == nil
}

// handoffLossError 输因回传值（优先级：core 自报的确定性拒绝 → 调用方
// 预算 ctx 已耗尽时以预算错误为准 → core 自报 Err——含 panic 折输哨兵 →
// 迁移输哨兵）。
//
// 确定性拒绝优先：core 自报
// 错误若不含取消签名（context.Canceled/DeadlineExceeded，判据见
// wrapsCancellation），它不可能是取消并集坍缩的产物——是诚实的终局裁决
// （Store 未配置、Put 实败、panic 折输等），即便预算同时耗尽也原样回传：
// 预算耗尽只是巧合，重试仍将复现同一拒绝。
//
// 预算优先的依据（仅对含取消签名的 core 错误成立）：core ctx 是预算的取消
// 并集派生（handoffCoreCtx），预算到点时 core 观察到的 context.Canceled 只
// 是并集坍缩的产物——WithCancel 派生遮蔽了父级 Deadline，原样回传会把
// 「预算耗尽」伪装成「被取消」（manager 侧
// TestMigrate_BudgetExpiryLoses 钉死该链形态）；调用方自己的预算 ctx 才是
// 「窗预算耗尽」的权威表述。预算仍活跃时 core 自报 Err 是诚实输因
// （Store/CAS 失败、panic 折输等）。Passivate/Retire 的失败必有 Err（core
// 自报或 panic 折输），哨兵兜底仅防非契约 core。注意：本值只在窗回退时
// 送达调用方——销毁/强弃已介入（窗未回退）时 runHandoff 改回输
// ErrMigrationLost（裁决优先级，见其注释）。
func handoffLossError(req handoffReq, o HandoffOutcome) error {
	if o.Err != nil && !wrapsCancellation(o.Err) {
		return o.Err // 确定性拒绝：预算不得遮蔽
	}
	if err := req.ctx.Err(); err != nil {
		return err
	}
	if o.Err != nil {
		return o.Err
	}
	return ErrMigrationLost
}

// wrapsCancellation 报告 err 是否携带取消签名（context.Canceled /
// DeadlineExceeded）。core ctx 是预算的取消并集派生——凡含此签名者都
// 可能是并集坍缩产物，预算优先语义对其保留裁决权；不含签名者的裁决
// 与取消面无关，是确定性拒绝。
func wrapsCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// safeHandoffCore 包裹交接核心：enterHook 留痕（强弃时「钩子在飞」警告
// 可见）+ panic 折为输（宿主快照/
// 发送 panic 不得杀死 loop；交接是可放弃/可重试的转换路径）。折输必须
// 携带 Err（errHandoffCorePanic 哨兵）：Passivate/Retire 以 Err==nil 为
// 成功，零值会被判成成功。
func (p *Pawnite) safeHandoffCore(req handoffReq, ctx context.Context) (outcome HandoffOutcome) {
	p.enterHook(handoffHookName(req.kind))
	defer p.exitHook()
	defer func() {
		if r := recover(); r != nil {
			log.Warn("pawnite handoff core panicked, treating as lost",
				"uuid", p.id.String(), "kind", int(req.kind), "panic", r)
			outcome = HandoffOutcome{Err: fmt.Errorf("%w: %v", errHandoffCorePanic, r)}
		}
	}()
	return req.core(ctx)
}

// handoffHookName 交接核心的在飞留痕名（hookInFlight 簿记标签）。
func handoffHookName(kind HandoffKind) string {
	switch kind {
	case HandoffPassivate:
		return "passivate"
	case HandoffRetire:
		return "retire"
	default:
		return "migrate"
	}
}

// handoffCoreCtx 构造 core ctx（取消并集派生）：以 hookCtx 为
// 父（values 值链照旧携带——fencing 纪元等），任一 hookCtx（强弃取消面）
// 或调用方预算 ctx 完成即取消。返回的 cancel 释放 AfterFunc 看护（core
// 返回后由 runHandoff 调用；与并集触发的自动取消幂等）。Won 分支的
// finish 仍用 hookCtx（非本派生）：强弃后晚到 Deactivate 收已取消 ctx
// 是钉死行为（消费端 select 依赖的唯一真实落点）。
func (p *Pawnite) handoffCoreCtx(hookCtx, budget context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(hookCtx)
	stop := context.AfterFunc(budget, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// SkipDirUnregister 迁移赢家查询面：true = 目录所有权已随 CAS 让渡给目标
// 节点，本端清理（Deactivate/OnEvict 钩子内的 UnregisterIfOwner）必须跳过
// ——注销会误删新家的注册（§13.2 换锁不变量）。判据是终局原因 Migrated
// （core 赢裁决时置位，写一次）。已知交错：看门狗强弃先落（Abandoned
// 权威）后 zombie core 才迟回 Won 时本面为 false——但终局清理已在强弃内
// 同步完成，且 Won 的 CAS 已把条目指向目标，UnregisterIfOwner 的实例级
// 所有权匹配（Owner{node, inc} 同时相等才删——归属判据是实例
// 粒度，inc 不符即新实例已接管同 node 条目，不误删）兜住不误删新家。
func (p *Pawnite) SkipDirUnregister() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.term.reason == TerminalMigrated
}

// drainForwardTo 整箱排空转发（规格 §12 输家整箱转发与 §13.1 迁移赢家
// 搭车的共用实现——两路径仅 reason 文案不同）：
// 投递实现 Forwardable 则转发给目标，否则按 Unavailable 回结对端（reason
// 上报文案）。收集持 mu、Forward/Reject 在锁外（同 drainReject 纪律，
// 陷阱 2）；保持 FIFO 顺序。
func (p *Pawnite) drainForwardTo(target rans.NodeID, reason rans.Reason) {
	p.mu.Lock()
	pending := p.takeInboxLocked()
	p.mu.Unlock()
	disposition(pending, target, reason)
}

// disposition 对已收集的在箱投递做整箱处置：Forwardable → Forward(目标)，
// 非 Forwardable → Reject(Unavailable, reason)。FIFO 保序。
func disposition(pending []InboundDelivery, target rans.NodeID, reason rans.Reason) {
	for _, d := range pending {
		if f, ok := d.(Forwardable); ok {
			f.Forward(target)
		} else {
			d.Reject(rans.CodeUnavailable, reason)
		}
	}
}
