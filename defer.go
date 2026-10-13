package rans

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang/internal/ctxkey"
)

type deferredToken struct{}

// Deferred Handle 的延迟应答返回值哨兵：调用过 Defer 后必须返回 (rans.Deferred, nil)
// ——投递边界据此不回包（契约正本见 Defer 的 godoc）。
var Deferred deferredToken

// DeferredArm 延迟回包执行面（运行时投递边界实现——本地与远端请求投递各
// 自实现，Responder.Reply 的落点经此送达；导出仅为跨包实现签名的可写性）。
type DeferredArm interface {
	// DeliverDeferred 回包送达（非阻塞：本地缓冲 1、远端发件箱满即丢）。
	DeliverDeferred(v any, err error)
	// DeferredDeadline 本请求的执行 deadline（零值 = 无预算约束）。
	DeferredDeadline() time.Time
}

// deferPoint 延迟应答登记点（运行时投递边界实现，仅请求投递的 Handle ctx 携带）。
type deferPoint interface {
	// ArmDefer 登记延迟应答并取回执行面；返回 nil = 本请求已登记过。
	ArmDefer() DeferredArm
}

// deferredCancelAware 在箱取消标记的应答面感知（H12 决策 17 取消面扩展，
// 可选接口）：实现者声明自己可被 MsgCancel 标记取消——已标记请求的迟到
// Reply 丢弃（收场权威已随取消让渡，投递只会落进已放弃的等待面）。
type deferredCancelAware interface {
	DeferredCanceled() bool
}

// Responder 延迟应答的应答器：Defer(ctx) 的产物，与 ctx 解耦——可存入实
// 体状态，由后续任一消息的 Handle（聚合应答形态）甚至其他 goroutine 调用
// Reply 收场本请求。并发安全；nil 与零值的 Reply 空转（不 panic）。收场契
// 约：Reply 幂等（仅首次生效）；deadline 已过后的 Reply 丢弃且无效（权威
// 在调用方）；err 非 nil 按 Handle 错误边界折叠后送达、v 被忽略。
type Responder struct {
	arm      DeferredArm
	deadline time.Time
	done     atomic.Bool
}

// Reply 送达本请求的应答（幂等：仅首次生效；deadline 已过后的调用丢弃且
// 无效；投递面已标记取消的调用丢弃且无效——MsgCancel 定位到已 Deferred 未
// Reply 的请求时置位，H12 决策 17 取消面扩展）。
func (r *Responder) Reply(v any, err error) {
	if r == nil || r.arm == nil {
		return
	}
	if !r.done.CompareAndSwap(false, true) {
		return // 幂等：第二次及以后调用无效
	}
	if !r.deadline.IsZero() && time.Now().After(r.deadline) {
		return // 到期丢弃：deadline 权威在调用方
	}
	if ca, ok := r.arm.(deferredCancelAware); ok && ca.DeferredCanceled() {
		return // 已标记取消：迟到收场不再投递
	}
	r.arm.DeliverDeferred(v, err)
}

// Defer 在 Handle 内把本请求标记为延迟应答（不立即回包）并返回 Responder
// 供后续收场；ctx 必须是 Handle 收到的 ctx（投递关联经 ctx 惯例取用，换造
// 或重建的 ctx 即失去登记点）。
//
// 两件套纪律（H12 决策 16；与 ReenterAfter 互补，本原语可单独使用）：
//
//	r := rans.Defer(ctx)         // 本请求改延迟应答，拿 Responder
//	return rans.Deferred, nil    // 投递边界识别的哨兵返回值
//
// Defer 之后的合法收场（godoc 钉死）：返回 (rans.Deferred, nil) = 不回包，
// 等待 Responder.Reply（可由后续任一消息的 Handle 调用——聚合应答形态，
// Responder 存实体状态即可）；返回 (v, err 非 nil) = 错误提前收场，错误照
// 常送达、迟到 Reply 被调用方侧自然丢弃；返回 (v 非 Deferred, nil) = 编程
// 错误，投递边界回 CodeError+ReasonHandler 错误应答（不回业务值）；只
// Defer 不回复的请求由调用方 deadline 超时收场（收端不自建到期定时，
// deadline 过后 Reply 丢弃且无效）。Defer 返回后本请求不再计入执行看门狗
// ——延迟应答不算卡死。
//
// 误用形态（编程错误）：非 Handle ctx（含 Tell 单向投递的 Handle——单向
// 无应答面）调用 Defer、同一请求上重复 Defer——均 panic 于调用点；返回
// Deferred 却未 Defer——非 panic，边界回 CodeError+ReasonHandler 错误应答
// （绝不把哨兵值当成功回包）。
func Defer(ctx context.Context) *Responder {
	v, ok := ctxkey.DeferPointFrom(ctx)
	if !ok {
		panic("rans: Defer called outside a request Handle ctx (only Call deliveries carry the defer point; Tell and non-runtime ctx do not)")
	}
	arm := v.(deferPoint).ArmDefer()
	if arm == nil {
		panic("rans: Defer called twice on the same request")
	}
	return &Responder{arm: arm, deadline: arm.DeferredDeadline()}
}
