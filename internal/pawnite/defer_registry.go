// 延迟应答终局记账（票 11）：实例终局（finish——钝化/迁移/销毁/强弃/停机
// 的统一终局点）对未 Reply 的 Responder 立即收场——调用方收到
// Timeout+Deactivated 而不是干等到请求 deadline；此后迟到的 Reply 丢弃。
// 记账经信封注入的包装器在 ArmDefer/DeliverDeferred 两端进行（rans 不能
// 导入 pawnite，包装器是运行时侧的记账载体）。
package pawnite

import (
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
)

// DeferredSettler 实例终局收场面（票 11，可选接口）：投递实现以**不折叠**
// 形态送达终局错误（终局是运行时状态，非 Handle 错误——折叠边界见
// activation.FoldHandlerError 的分界说明）。
type DeferredSettler interface {
	// SettleDeferred 终局收场：e 为 Timeout+Deactivated 的 *rans.Error，
	// 非阻塞送达（与 DeliverDeferred 同纪律）。
	SettleDeferred(e *rans.Error)
}

// deferEntry 一笔未收场的延迟应答。
type deferEntry struct {
	arm  rans.DeferredArm
	dl   time.Time // Responder 到期判据（DeferredDeadline 快照）
	done atomic.Bool
}

// take 单胜收场权（真 Reply 与终局收场竞争的第一落点）。
func (e *deferEntry) take() bool { return e.done.CompareAndSwap(false, true) }

// deferPointArm rans 登记点惯例形态（rans.Defer 经 ctx 取 ArmDefer 落点；
// 请求投递实现满足）。
type deferPointArm interface {
	ArmDefer() rans.DeferredArm
}

// deferAccounting 信封注入的登记点包装器（满足 rans 的 deferPoint 惯例）：
// ArmDefer 时记账、经 deferReply 在 DeliverDeferred 端销账。
type deferAccounting struct {
	p     *Pawnite
	inner deferPointArm
}

// ArmDefer 登记并取回销账包装的执行面；内层 nil（重复 Defer）原样透传。
func (w *deferAccounting) ArmDefer() rans.DeferredArm {
	inner := w.inner.ArmDefer()
	if inner == nil {
		return nil
	}
	e := &deferEntry{arm: inner, dl: inner.DeferredDeadline()}
	w.p.mu.Lock()
	if w.p.deferred == nil {
		w.p.deferred = map[*deferEntry]struct{}{}
	}
	w.p.deferred[e] = struct{}{}
	w.p.mu.Unlock()
	return &deferReply{p: w.p, e: e}
}

// deferReply Responder 侧执行面：Reply 落地即销账；已收场（终局/到期清扫/
// 先到 Reply）的迟到调用丢弃。
type deferReply struct {
	p *Pawnite
	e *deferEntry
}

func (r *deferReply) DeliverDeferred(v any, err error) {
	if !r.e.take() {
		return // 终局已收场或已 Reply：迟到收场丢弃
	}
	r.p.mu.Lock()
	delete(r.p.deferred, r.e)
	r.p.mu.Unlock()
	r.e.arm.DeliverDeferred(v, err)
}

func (r *deferReply) DeferredDeadline() time.Time { return r.e.arm.DeferredDeadline() }

// DeferredCanceled 在箱取消标记读数转发（rans.Responder 的 H12 决策 17
// 丢弃判据——包装不改变取消语义）。
func (r *deferReply) DeferredCanceled() bool {
	if ca, ok := r.e.arm.(interface{ DeferredCanceled() bool }); ok {
		return ca.DeferredCanceled()
	}
	return false
}

// settleDeferred 终局收场（finish 调用，锁外）：未收场且未过期的 Responder
// 以 Timeout+Deactivated 立即收场；已过期者静默丢弃（调用方已按自身预算
// 超时，迟到收场无意义）。
func (p *Pawnite) settleDeferred() {
	p.mu.Lock()
	entries := make([]*deferEntry, 0, len(p.deferred))
	for e := range p.deferred {
		entries = append(entries, e)
	}
	p.deferred = nil
	p.mu.Unlock()
	now := time.Now()
	for _, e := range entries {
		if !e.take() {
			continue // 已收场
		}
		if !e.dl.IsZero() && now.After(e.dl) {
			continue // 调用方已超时：迟到收场无意义
		}
		settled := rans.NewError(rans.CodeTimeout, rans.ReasonDeactivated,
			"rans: entity reached terminal state before deferred reply (passivated/migrated/destroyed)")
		if s, ok := e.arm.(DeferredSettler); ok {
			s.SettleDeferred(settled)
			continue
		}
		e.arm.DeliverDeferred(nil, settled) // 无终局面的实现：常规送达兜底
	}
}
