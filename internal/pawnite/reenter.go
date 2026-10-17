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

// newLoopArm 构造 loop 内部任务的让出执行面，返回挂好 loop 点的 ctx。ctx 与
// arm 互相引用（arm.hctx 是续体 ctx 的派生底），唯一正确构造序是：先建 arm →
// 把带登记点的 ctx 记回 arm.hctx。两步走（先记裸 ctx 再挂点）会让续体 ctx 链
// 上没有登记点、then 内再让出必 panic——把次序收进构造器即该缺陷的修复。
func (p *Pawnite) newLoopArm(base context.Context, detached bool) (context.Context, *ReenterArm) {
	arm := &ReenterArm{p: p, detached: detached, works: map[*workUnit]struct{}{}}
	// 挂点而不遮挡延迟应答面：Handle ctx 的 DeferPoint 是本请求的应答面
	//（遮挡它就是杀掉 rans.Defer）；任务侧的遮挡在 loopTaskCtx。
	ctx := ctxkey.WithTimerPoint(ctxkey.WithReenterPoint(base, arm), p)
	arm.hctx = ctx
	return ctx, arm
}

// loopTaskCtx loop 内部任务 ctx 的统一派生点（续体与定时器回调共用）：遮蔽
// 延迟应答面（任务没有可应答的请求——Defer 必 panic）、挂让出登记点与定时器点。
func (p *Pawnite) loopTaskCtx(base context.Context, arm *ReenterArm) context.Context {
	ctx := ctxkey.WithDeferPoint(base, nil)
	ctx = ctxkey.WithReenterPoint(ctx, arm)
	return ctxkey.WithTimerPoint(ctx, p)
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
// 保留值链、重挂请求 deadline；两腿统一遮蔽自身身份并挂自有取消面。loop 点
// （让出与定时器）同样剥掉：work 不在 loop 上，武装定时器会丢唤醒，嵌套让出
// 也越过 godoc 的「work 不得再让出」纪律。
func (a *ReenterArm) deriveWctx() (context.Context, context.CancelFunc) {
	base := a.hctx
	var deadlineCancel context.CancelFunc
	if a.detached {
		base = context.WithoutCancel(a.hctx)
		if dl, ok := a.hctx.Deadline(); ok {
			base, deadlineCancel = context.WithDeadline(base, dl)
		}
	}
	masked := ctxkey.WithReenterPoint(ctxkey.WithTimerPoint(ctxkey.WithoutSelf(base), nil), nil)
	wctx, cancel := context.WithCancel(masked)
	return wctx, func() {
		cancel()
		if deadlineCancel != nil {
			deadlineCancel()
		}
	}
}

// thenCtx 派生续体 ctx（契约正本见 rans.ReenterAfter godoc 的 tctx 规则节）：
// 以 Handle ctx 值链为底脱钩其收尾 cancel、重挂原请求 deadline；经 loop 任务
// 派生点重挂让出登记点（then 内可再让出）与定时器点（then 内可再定时），并
// 遮蔽延迟应答登记点（then 内 Defer 必 panic）。
func (a *ReenterArm) thenCtx() (context.Context, context.CancelFunc) {
	ctx := context.WithoutCancel(a.hctx)
	var dcancel context.CancelFunc
	if dl, ok := a.hctx.Deadline(); ok {
		ctx, dcancel = context.WithDeadline(ctx, dl)
	}
	ctx, cancel := context.WithCancel(a.p.loopTaskCtx(ctx, a))
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

// loopTaskUnit loop 内部任务（续体、定时器回调）的在途执行槽标记（executing
// 槽的循环任务形态）：任务无投递载体，登记只为统一「执行时钟非零 ⟹ 在途槽
// 有绑」的不变量——看门狗强弃的解绑面（unbindExecuting）对标记的回绝无悬挂
// 调用方可解，no-op 即正确语义。
type loopTaskUnit struct{ p *Pawnite }

func (u *loopTaskUnit) UUID() rans.ID                 { return u.p.id }
func (u *loopTaskUnit) Deliver(*Pawnite)              {}
func (u *loopTaskUnit) Reject(rans.Code, rans.Reason) {}

// runLoopTask loop 内部任务的统一执行段：计入活动纪元（脏门控由此覆盖任务
// 写——捕获放行判据同源双空，见 activityQuiescedLocked）与执行停驻时钟 +
// 在途执行槽（任务执行段计入执行看门狗；强弃解绑面统一）；panic 监管与
// invoke 同口径（Default 服役；Isolate 销毁）。label 只进日志。
func (p *Pawnite) runLoopTask(label string, fn func()) {
	marker := InboundDelivery(&loopTaskUnit{p: p})
	p.executing.Store(&marker)
	p.executingSinceNano.Store(time.Now().UnixNano())
	p.activityEpoch.Add(1)
	defer p.executing.Store(nil)
	defer p.executingSinceNano.Store(0)
	defer func() {
		if r := recover(); r != nil {
			log.Warn("pawnite "+label+" panicked",
				"uuid", p.id.String(), "panic", r, "policy", p.opts.PanicPolicy)
			if p.opts.PanicPolicy == PanicPolicyIsolate {
				p.isolateAfterPanic()
			}
		}
	}()
	fn()
}

// runContinuation 续体任务的 loop 执行段（语义见 runLoopTask）。
func (p *Pawnite) runContinuation(fn func()) { p.runLoopTask("reenter continuation", fn) }

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
