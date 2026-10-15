package activation

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/ctxkey"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// 本包哨兵族（统一错误模型）：全部为 *rans.Error 形态——
// 调用方判错一律经 rans.CodeOf/ReasonOf/errors.Is(rans.* 哨兵)。
// rans.ErrTimeout/ErrNotFound/ErrUnavailable/ErrCanceled 直接复用 rans
// 哨兵（同值同义，本包不再定义重复哨兵）；Reason 细分的回绝类在下方
// 以包内哨兵定义（不构成消费方入口——消费方面是 rans 的 Code/Reason）。
var (
	// ErrBusy 收件箱满/激活竞态逐出窗的回绝（Unavailable+Busy）：过载
	// 保护——确定未执行但分类唯一表不自动重试（约定 spec §5.3）。
	// 仅包内接线与同模块测试引用；消费方判错面是 rans.CodeOf/ReasonOf。
	ErrBusy = rans.NewError(rans.CodeUnavailable, rans.ReasonBusy,
		"rans: deliver: target busy or unavailable")
)

type callDelivery struct {
	id      rans.ID
	handler func(ctx context.Context, req any) (any, error)
	// handler 的执行 ctx：InvokeLocal 构造时刻已按传播键同语义重建
	//（登记键以 decode 产物形态在场；deadline/因果/仅本地键/取消面经
	// 预算派生与继承保持）。
	ctx   context.Context
	req   any
	reply chan callResult
	// deferArmed 延迟应答登记标记（H12 决策 16 前半，跨 goroutine 原子读写）。
	deferArmed atomic.Bool
}

type callResult struct {
	val any
	err error
}

// FoldHandlerError 把 Handle 返回的错误折叠为统一形态 *rans.Error{Code:
// CodeError, Reason: ReasonHandler}（约定 spec §5.3 Handle 错误边界；
// 决策出处：docs/decisions/2026-10-09-hr2-unavailable-retry.md）：*rans.Error 不实现
// Unwrap——调用方与重试分类（routing.Classify）无法从错误链摸到实体嵌套
// 调用携带的任何运行时形态（Busy/NotFound/Timeout 的 *rans.Error 等），
// 「实体做完副作用后返回包装了回绝形态的业务错误被误判为确定未执行而
// 重复执行」的结构性风险在此消失；错误文本保留在 Message，实现了
// rans.AppError/rans.AppDataError 的业务错误再带上 AppCode/AppData
// （随结构化 Error 帧过线，本地与远端同构）。Reject 产生的错误不经此——
// 回绝是运行时 (Code, Reason) 映射（见 Reject）。
// 例外：调用方 ctx 的到期/取消经 Handle 传播（文档化惯例，slowEntity
// 形态）不折叠——ctxErr 非 nil 且属于该形态时原样返回；本地腿传调用方
// ctx.Err()（InvokeLocal 的错误面归一依赖 errors.Is(err, ctx.Err()) 识别
// 该形态），远端腿传 nil（调用方 ctx 不跨节点，无该形态）。context 误差
// 经各腿的 ctxOutcome 归一为 Canceled/Timeout 的 *rans.Error，分类安全。
// 导出面：本地（callDelivery.Deliver）与远端（remoteDelivery.Deliver）两
// 条投递边界共用本唯一定义，保证折叠边界不漂移。
func FoldHandlerError(err error, ctxErr error) error {
	if ctxErr != nil && errors.Is(err, ctxErr) {
		return err
	}
	folded := rans.NewError(rans.CodeError, rans.ReasonHandler, err.Error())
	var app rans.AppError
	if errors.As(err, &app) {
		folded.AppCode = app.AppCode()
	}
	var appData rans.AppDataError
	if errors.As(err, &appData) {
		folded.AppData = appData.AppData()
	}
	return folded
}

func (c *callDelivery) UUID() rans.ID { return c.id }

// 编译期断言：callDelivery 是延迟应答回包执行面（登记点即本体——Handle ctx 注入）。
var _ rans.DeferredArm = (*callDelivery)(nil)

// ArmDefer 延迟应答登记（rans.Defer 的落点）：首次生效取回执行面；重复调用返回 nil。
func (c *callDelivery) ArmDefer() rans.DeferredArm {
	if !c.deferArmed.CompareAndSwap(false, true) {
		return nil
	}
	return c
}

// DeliverDeferred 延迟回包送达（Responder.Reply 的落点）：与 Deliver 同一
// 非阻塞应答通道（缓冲 1 满即丢）；err 按 Handle 错误边界折叠（无 ctx 透传）。
func (c *callDelivery) DeliverDeferred(v any, err error) {
	if err != nil {
		err = FoldHandlerError(err, nil)
	}
	c.replyResult(v, err)
}

// DeferredDeadline 本请求的执行 deadline（Responder 到期丢弃判据）。
func (c *callDelivery) DeferredDeadline() time.Time {
	dl, _ := c.ctx.Deadline()
	return dl
}

// SettleDeferred 实例终局收场（票 11，pawnite.DeferredSettler）：未 Reply
// 的延迟应答以 Timeout+Deactivated 立即送达调用方——不折叠（终局是运行
// 时状态，非 Handle 错误）；非阻塞应答通道与 DeliverDeferred 同纪律。
func (c *callDelivery) SettleDeferred(e *rans.Error) {
	c.replyResult(nil, e)
}

// DeferredCanceled 调用方收场让渡读数（rans.Responder 的可选丢弃判据，本地
// 腿）：调用方 ctx 已取消/到期 = 收场权威已让渡——迟到延迟应答丢弃（与远端
// 腿的 MsgCancel 标记收口同一 Responder 丢弃路径）。
func (c *callDelivery) DeferredCanceled() bool { return c.ctx.Err() != nil }

func (c *callDelivery) replyResult(v any, err error) { // 非阻塞应答投递（两收场共用）
	select {
	case c.reply <- callResult{val: v, err: err}:
	default:
	}
}

// DeliveryCausalPath 入站因果路径（pawnite.CycleAware，H12 决策 15）：ctx
// 携带的哈希序列原样上报供准入判环；未携带返回 nil。
func (c *callDelivery) DeliveryCausalPath() []uint64 {
	if cp := ctxkey.CausalFrom(c.ctx); cp != nil {
		return cp.Path
	}
	return nil
}

// DeliveryDeadline 执行预算自描述（pawnite.Expirable）：本地腿 deadline =
// 调用方合成 ctx 的 deadline（票 03 callBudget 产物），出队据此复查过期。
func (c *callDelivery) DeliveryDeadline() (time.Time, bool) { return c.ctx.Deadline() }

// DeliveryCanceled 在箱取消标记读数（pawnite.CancelAware，本地请求投递）：
// 调用方在出队前主动取消 → 出队即弃并计 rans.call.canceled——与远端 MsgCancel
// 同构。deadline 到期不在此列（出队过期检查先行，Timeout+Expired 收场）。
func (c *callDelivery) DeliveryCanceled() bool {
	return errors.Is(c.ctx.Err(), context.Canceled)
}

// Deliver 在 Pawnite loop goroutine 上执行（串行语义的落点）。Handle ctx 经
// 唯一信封构造（票 08）：请求形态调用方取消穿透、携带执行实例的 incarnation
// 纪元（fencing 面——zombie 迟回的 Handle 续体直写消费方存储时可凭 ctx 令牌
// 被拒；值取自实际执行投递的 pawnite）与延迟应答登记点（本地请求投递恒携
// 带）；让出面挂链——调用方取消/请求 deadline 穿透到 work，而发送腿不提前
// 释放预算 ctx（rpc callOnce 本地腿），work 存活到请求 deadline。
//
// Handle 错误边界折叠：本方法是本地投递的 Handle 错误唯一
// 产生点，返回的错误一律经 FoldHandlerError 折叠为 CodeError+ReasonHandler
// 的 *rans.Error——重试环内路由翻转到本地时（rpc invokeRouted），折叠后
// 的形态对分类唯一表必判 NoRetry，嵌套运行时形态不再诱发重复执行。
// 延迟应答判读与 remoteDelivery 同一矩阵（契约正本见 rans.Defer）。
func (c *callDelivery) Deliver(p *pawnite.Pawnite) {
	dl, _ := c.ctx.Deadline()
	hctx, cancel, _ := p.HandleCtx(c.ctx, c.id, pawnite.EnvelopeCall, dl, c, false)
	defer cancel()
	val, err := c.handler(hctx, c.req)
	if err != nil {
		c.replyResult(nil, FoldHandlerError(err, c.ctx.Err()))
		return
	}
	if val == rans.Deferred {
		if c.deferArmed.Load() {
			return // 已 Defer：不回包，等待 Responder.Reply
		}
		c.replyResult(nil, rans.NewError(rans.CodeError, rans.ReasonHandler,
			"rans: handler returned rans.Deferred without calling rans.Defer"))
		return
	}
	if c.deferArmed.Load() {
		c.replyResult(nil, rans.NewError(rans.CodeError, rans.ReasonHandler,
			"rans: handler called rans.Defer but returned a value; must return rans.Deferred"))
		return
	}
	c.replyResult(val, nil)
}

// Reject 按 pawnite 回绝的 (Code, Reason) 直接构造调用方错误
// *rans.Error{Code, Reason}（统一错误模型：回绝是运行时状态码
// 映射，**不折叠**——与 Deliver 折叠的 Handle 错误形成「回绝可判
// Code/Reason、执行错误不透明」的分界；reason 细分由 pawnite 按约定
// spec §5.3 映射表给出：满箱 Busy、销毁中 Deactivating、失租 LeaseLost、
// 激活失败排空 ActivationFailed…）。非阻塞投递（缓冲 1，Deliver/Reject
// 至多一者胜出）。分类唯一表判据：Unavailable+Busy 不重试（过载保护）、
// Unavailable+ActivationFailed 无条件重试（注册后失败属执行前
// 确定性失败，同一事件的异步窗（在箱投递被 drain 回绝）与 GetOrActivate
// 复检的同步窗产出同 (Code, Reason)，接线层据此本地重试一次）；
// StatusOK 不构成回绝，防御性丢弃（不投递 nil 错误结果——会伪造成功
// 应答、顶掉真实收场）。
func (c *callDelivery) Reject(status rans.Code, reason rans.Reason) {
	if status == rans.CodeOK {
		return
	}
	msg := rejectMessage(status, reason)
	if reason == rans.ReasonCycle { // 环回绝文本带完整环（H12 决策 15）
		msg = pawnite.CycleRejectText(c.DeliveryCausalPath(), c.id)
	}
	err := rans.NewError(status, reason, msg)
	select {
	case c.reply <- callResult{nil, err}:
	default:
	}
}

// rejectMessage 回绝错误的诊断文本（Code/Reason 才是判定面，文本供日志
// 与排障）。
func rejectMessage(status rans.Code, reason rans.Reason) string {
	if reason == rans.ReasonNone {
		return fmt.Sprintf("rans: deliver: rejected (status=%d)", uint8(status))
	}
	return fmt.Sprintf("rans: deliver: rejected (status=%d reason=%s)", uint8(status), reason)
}

// BindExpiredHook 出队过期即弃事件钩子的装配期接线（H10 决策 7）：唯一
// 生产调用点是 rpc.New（计数器挂在节点指标注册表，rpc 内部构件进不了
// Options）；Start 前单写一次后不可变（与 BindStateLoader 同族纪律）。
func (m *Table) BindExpiredHook(fn func()) { m.onExpired = fn }

// BindCanceledHook 出队取消弃置事件钩子的装配期接线（H10 决策 9；唯一生
// 产调用点 rpc.New，Start 前单写——与 BindExpiredHook 同族纪律）。
func (m *Table) BindCanceledHook(fn func()) { m.onCanceled = fn }

// InvokeLocal 本地激活表直调（调用面收口的内部执行腿）：GetOrActivate
// → 收件箱投递 → 等回复/超时。唯一调用方是 internal/rpc 的本地快路径
// （callLocal）——统一调用入口 rans.Client（node.Client()）路由命中本地时
// 经此执行。它不是消费方调用入口：仓内一切实体调用必须走 rans.Client，
// 直调本面会绕过路由梯子得到与远端不一致的错误/重试语义。投递 ctx 在
// 构造时刻经传播键同语义重建（RebuildPropagation——Handle 可见值与远端
// 解码注入同形态，INV6；仅本地键与取消面经继承保持）。
func (m *Table) InvokeLocal(ctx context.Context, id rans.ID, req any) (any, error) {
	p, err := m.GetOrActivate(ctx, id)
	if err != nil {
		return nil, err // ErrUnknownKind / ErrActivationFailed（*rans.Error 形态）透传
	}
	ent, ok := m.EntityOf(id)
	if !ok {
		return nil, ErrBusy // 激活竞态极小窗：实体刚被逐出，按 busy 让调用方重试
	}
	d := &callDelivery{
		id:      id,
		handler: ent.Handle,
		ctx:     ctxkey.RebuildPropagation(ctx),
		req:     req,
		reply:   make(chan callResult, 1),
	}
	if !p.ReceiveRequest(d) {
		select {
		case r := <-d.reply:
			return r.val, r.err
		default:
			return nil, ErrBusy
		}
	}
	// 应答优先探询 + 双就绪复检：reply 与 ctx.Done
	// 同时就绪时 select 随机选——已落地的成功结果可能被取消信号丢弃。
	// 先非阻塞探一次应答、取消分支落地前再复检一次，把随机窗口压到探询
	// 与返回之间的固有极窄段（channel 双相 select 无原生偏好，此为 Go
	// 惯用近似）。取消与超时的错误面分离：Deadline → rans.ErrTimeout（既有
	// 契约，TestCall_Timeout 钉死），主动取消 → rans.ErrCanceled（包装
	// ctx.Err，errors.Is(err, context.Canceled) 仍成立）——「我放弃了」
	// 与「执行超时」对调用方是不同的处置依据。
	v, err := d.awaitReply(ctx)
	if err != nil && ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		// 实体把调用方 ctx 的到期/取消原样作为错误回执（Handle 传播调用
		// 方取消是文档化惯例，slowEntity 形态）——错误面归一到调用侧语义，
		// 与等待段同一收场。
		return nil, ctxOutcome(ctx)
	}
	return v, err
}

// awaitReply 等待应答，取消/超时按 ctxOutcome 收场（见 InvokeLocal 尾段注释）。
func (d *callDelivery) awaitReply(ctx context.Context) (any, error) {
	select {
	case r := <-d.reply:
		return r.val, r.err
	default:
	}
	select {
	case r := <-d.reply:
		return r.val, r.err
	case <-ctx.Done():
		select { // 终检：取消裁决前应答若已落地，优先交付
		case r := <-d.reply:
			return r.val, r.err
		default:
		}
		return nil, ctxOutcome(ctx)
	}
}

// ctxOutcome 调用方 ctx 的收场错误（统一错误模型形态，约定 spec §5.3
// 映射表「调用方 deadline 到期 → Timeout、主动取消 → Canceled」）：
// Deadline → rans.ErrTimeout（既有契约）；主动取消 → rans.ErrCanceled
// 包装 ctx.Err()（「我放弃了」与「执行超时」对调用方是不同的处置依据，
// 也不再伪装超时）。
func ctxOutcome(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return rans.ErrTimeout
	}
	return fmt.Errorf("%w: %w", rans.ErrCanceled, ctx.Err())
}
