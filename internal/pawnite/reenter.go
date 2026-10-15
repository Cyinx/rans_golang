// 让出重入（H12 决策 16/17 后半）：ReenterAfter 的 loop 外 work 执行面、续
// 体任务通道与在途 work 登记表（契约正本在 rans.ReenterAfter 的 godoc）。
package pawnite

import (
	"context"
	"fmt"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/ctxkey"
	"github.com/cyinx/rans_golang/internal/log"
)

// 编译期断言：ReenterArm 满足 rans 让出重入执行面契约（ReenterAfter 的落点）。
var _ rans.ReenterArm = (*ReenterArm)(nil)

// ReenterArm 让出重入执行面（投递边界在 Deliver 构造 Handle ctx 时注入，实体
// 经 rans.ReenterAfter 取用）。detached 标记远端腿：Handle ctx 随投递返回的
// defer cancel 收场，wctx 须脱钩派生；本地腿 wctx 直挂 Handle ctx 链（调用
// 方取消/请求 deadline 自然穿透）。
type ReenterArm struct {
	p        *Pawnite
	hctx     context.Context // Handle ctx（deadline/因果路径/传播键的继承源）
	detached bool
	// works 本投递登记的在飞 work（mu 保护）：MsgCancel 定位命中的取消面
	// ——只波及本请求的 work，不误伤同实例他请求。
	works map[*workUnit]struct{}
}

func (p *Pawnite) NewReenterArm(hctx context.Context, detached bool) *ReenterArm {
	return &ReenterArm{p: p, hctx: hctx, detached: detached, works: map[*workUnit]struct{}{}}
}

// Reenter 实现 rans.ReenterArm：派生 wctx、登记在飞 work 并起专属 goroutine。
func (a *ReenterArm) Reenter(work func(ctx context.Context) (any, error), then func(ctx context.Context, v any, err error)) {
	wctx, cancel := a.deriveWctx()
	u := &workUnit{cancel: cancel}
	a.p.mu.Lock()
	a.p.works[u] = struct{}{}
	a.works[u] = struct{}{}
	a.p.mu.Unlock()
	go a.p.runWork(u, a, wctx, work, then)
}

// deriveWctx 派生 work ctx（继承规则正本见 rans.ReenterAfter godoc）：脱钩腿
// 保留值链、重挂请求 deadline；两腿统一遮蔽自身身份并挂自有取消面。
func (a *ReenterArm) deriveWctx() (context.Context, context.CancelFunc) {
	base := a.hctx
	var deadlineCancel context.CancelFunc
	if a.detached {
		base = context.WithoutCancel(a.hctx)
		if dl, ok := a.hctx.Deadline(); ok {
			base, deadlineCancel = context.WithDeadline(base, dl)
		}
	}
	wctx, cancel := context.WithCancel(ctxkey.WithoutSelf(base))
	return wctx, func() {
		cancel()
		if deadlineCancel != nil {
			deadlineCancel()
		}
	}
}

// thenCtx 派生续体 ctx（票 09；契约正本见 rans.ReenterAfter godoc 的 tctx
// 规则节）：以 Handle ctx 值链为底脱钩其收尾 cancel、重挂原请求 deadline；
// 无类型遮蔽延迟应答登记点（then 内 Defer 必 panic）；让出面经值链保持
// （嵌套让出允许）。
func (a *ReenterArm) thenCtx() (context.Context, context.CancelFunc) {
	ctx := context.WithoutCancel(a.hctx)
	var dcancel context.CancelFunc
	if dl, ok := a.hctx.Deadline(); ok {
		ctx, dcancel = context.WithDeadline(ctx, dl)
	}
	ctx = ctxkey.WithDeferPoint(ctx, nil)
	ctx, cancel := context.WithCancel(ctx)
	return ctx, func() {
		cancel()
		if dcancel != nil {
			dcancel()
		}
	}
}

// CancelWorks 取消本投递登记的全部在飞 work（MsgCancel 定位命中面，cancel 锁外）。
func (a *ReenterArm) CancelWorks() {
	a.p.mu.Lock()
	units := a.takeArmWorksLocked()
	a.p.mu.Unlock()
	for _, u := range units {
		u.cancel()
	}
}

func (a *ReenterArm) takeArmWorksLocked() []*workUnit {
	units := make([]*workUnit, 0, len(a.works))
	for u := range a.works {
		units = append(units, u)
	}
	a.works = map[*workUnit]struct{}{}
	return units
}

type workUnit struct {
	cancel context.CancelFunc
}

// runWork work goroutine 本体：执行 work（panic 折错不杀进程），完成后单临
// 界区摘除登记 + 续体入队，再释放 wctx 取消面。
func (p *Pawnite) runWork(u *workUnit, arm *ReenterArm, wctx context.Context,
	work func(ctx context.Context) (any, error), then func(context.Context, any, error)) {
	v, err := p.safeWork(wctx, work)
	task := func() {
		if then != nil {
			tctx, tcancel := arm.thenCtx()
			defer tcancel()
			then(tctx, v, err)
		}
	}
	p.completeWork(u, arm, task)
	u.cancel()
}

// safeWork work 执行的 panic 监管：折为错误交给 then（loop 外 panic 不杀进程）。
func (p *Pawnite) safeWork(wctx context.Context, work func(ctx context.Context) (any, error)) (v any, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Warn("pawnite reenter work panicked", "uuid", p.id.String(), "panic", r)
			v, err = nil, fmt.Errorf("rans: reenter work panicked: %v", r)
		}
	}()
	return work(wctx)
}

// completeWork work 收场的单临界区（work goroutine 上调用）：登记摘除与续
// 体入队/信号同一 mu 临界区——交接窗静默判据（works 空且续体队空）与「末
// 个 work 完成必唤醒 loop」原子成立。then 为 nil 也入队空任务：续体任务是
// 交接窗等待的唯一唤醒源。
func (p *Pawnite) completeWork(u *workUnit, arm *ReenterArm, task func()) {
	p.mu.Lock()
	delete(p.works, u)
	delete(arm.works, u)
	p.reenterQ = append(p.reenterQ, task)
	p.mu.Unlock()
	select {
	case p.reenterCh <- struct{}{}:
	default: // 信号已在：无需重复
	}
}

// cancelWorks 取消实例全部在飞 work 的 wctx（窗预算超时/强弃/收尾统一入口）。
func (p *Pawnite) cancelWorks() {
	p.mu.Lock()
	units := make([]*workUnit, 0, len(p.works))
	for u := range p.works {
		units = append(units, u)
	}
	p.mu.Unlock()
	for _, u := range units {
		u.cancel()
	}
}

// popReenter 取出队首续体任务（mu 内）；队列有余量时重武装信号（与 popMail 同纪律）。
func (p *Pawnite) popReenter() (func(), bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.reenterQ) == 0 {
		return nil, false
	}
	fn := p.reenterQ[0]
	copy(p.reenterQ, p.reenterQ[1:])
	p.reenterQ[len(p.reenterQ)-1] = nil
	p.reenterQ = p.reenterQ[:len(p.reenterQ)-1]
	if len(p.reenterQ) > 0 {
		select {
		case p.reenterCh <- struct{}{}:
		default:
		}
	}
	return fn, true
}

// continuationUnit 续体执行的在途执行槽标记（executing 槽的续体形态）：
// 续体无投递载体，登记只为统一「执行时钟非零 ⟹ 在途槽有绑」的不变量——
// 看门狗强弃的解绑面（unbindExecuting）对标记的回绝无悬挂调用方可解，
// no-op 即正确语义。
type continuationUnit struct{ p *Pawnite }

func (u *continuationUnit) UUID() rans.ID                 { return u.p.id }
func (u *continuationUnit) Deliver(*Pawnite)              {}
func (u *continuationUnit) Reject(rans.Code, rans.Reason) {}

// runContinuation 续体任务的 loop 执行段：计入活动纪元（脏门控由此覆盖续
// 体写——捕获放行判据同源双空，见 activityQuiescedLocked）与执行停驻时钟
// + 在途执行槽（then 回 loop 后重新计入执行看门狗；强弃解绑面统一）；
// panic 监管与 invoke 同口径（Default 服役；Isolate 销毁）。
func (p *Pawnite) runContinuation(fn func()) {
	marker := InboundDelivery(&continuationUnit{p: p})
	p.executing.Store(&marker)
	p.executingSinceNano.Store(time.Now().UnixNano())
	p.activityEpoch.Add(1)
	defer p.executing.Store(nil)
	defer p.executingSinceNano.Store(0)
	defer func() {
		if r := recover(); r != nil {
			log.Warn("pawnite reenter continuation panicked",
				"uuid", p.id.String(), "panic", r, "policy", p.opts.PanicPolicy)
			if p.opts.PanicPolicy == PanicPolicyIsolate {
				p.isolateAfterPanic()
			}
		}
	}()
	fn()
}

// activityQuiescedLocked 静默判据（调用方必须已持 mu）：在飞 work 与续体
// 队列双空——交接 core 与周期捕获放行的唯一依据。works 空而 reenterQ 非
// 空是「work 已完成、续体未执行」的一致态窗口：此刻拍终快照/捕获即静默
// 丢已确认写（completeWork 在单临界区内清登记并入队，两表同源观测）。
func (p *Pawnite) activityQuiescedLocked() bool {
	return len(p.works) == 0 && len(p.reenterQ) == 0
}

// heldHandoff 挂起等待在途 work 的交接请求（mu 保护）；stopBudget 解除预算看护。
type heldHandoff struct {
	req        handoffReq
	stopBudget func()
}

// holdHandoffForWorks 交接信号到达时未达静默（在飞 work 或续体排队——判据
// 见 activityQuiescedLocked）：请求挂起，loop 只服务续体任务，静默判据满足
// 后续体分支驱动 core（H12 决策 17）。预算看护先起后挂：req.ctx 完成
// （deadline 到点或调用方取消）即取消全部在飞 wctx，work 协作返回后续体
// 排尽、core 照常驱动。返回 false = 已静默：core 立即执行。
func (p *Pawnite) holdHandoffForWorks(req handoffReq) bool {
	watch := context.AfterFunc(req.ctx, p.cancelWorks)
	stop := func() { watch() }
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activityQuiescedLocked() {
		stop()
		return false
	}
	p.heldHandoff = &heldHandoff{req: req, stopBudget: stop}
	return true
}

// releaseHeldHandoffIfQuiesced 静默判据满足时取回挂起的交接请求（续体分支
// 调用）：判据与挂起判据同源（activityQuiescedLocked）——work 完成且续体
// 已落地（状态一致点）。挂起/取回在 loop、清场（abort）在任意 goroutine，
// 互斥由 mu 保证，result 恰送一次。
func (p *Pawnite) releaseHeldHandoffIfQuiesced() (handoffReq, func(), bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.heldHandoff
	if h == nil || !p.activityQuiescedLocked() {
		return handoffReq{}, nil, false
	}
	p.heldHandoff = nil
	return h.req, h.stopBudget, true
}

// holdingHandoff 挂起等待在途 work 的交接窗在位判定（mailCh 分支的留箱闸）。
func (p *Pawnite) holdingHandoff() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.heldHandoff != nil
}
