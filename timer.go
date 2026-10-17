package rans

import (
	"context"
	"time"

	"github.com/cyinx/rans_golang/internal/ctxkey"
)

// TimerArm 定时器执行面（运行时在 loop 任务 ctx 注入——Handle、续体、定时器
// 回调三类；work ctx 不携带）。导出仅为跨包实现签名的可写性。
type TimerArm interface {
	// AddTimer 预约一次到点回调，返回句柄 ID（0 = 实例已终局未武装）。
	AddTimer(d time.Duration, fn func(ctx context.Context)) int64
	// RemoveTimer 取消尚未触发的定时器；true = 确实取消。
	RemoveTimer(timerID int64) bool
}

// AddTimer 在实体 loop 上预约一次到点任务（每 Pawnite 定时器）：d 从本次
// 调用起算，到期后 fn 在实体 loop 上串行执行一次——与投递 Handle、续体同一
// 串行面，不经收件箱（无满箱回绝、不会为一条定时消息激活新实例）。
//
// 一次性语义（契约正本）：回调执行前该 ID 已摘表，触发只发生一次；周期任务
// 由回调内再次 AddTimer 完成（每次拿新 ID）。d <= 0 表示本次 loop 任务返回
// 后的下一轮执行（替代「立即自投」写法）。
//
// ctx 必须是 loop 任务 ctx（Handle、续体 tctx、定时器回调 ctx），否则 panic
// ——与 ReenterAfter 同源校验。回调 ctx 携带 Self/Incarnation（触发时读取）/
// Client 与让出面（回调内可 ReenterAfter 让出做 IO）；不携带请求 deadline、
// 延迟应答面（回调内 Defer 必 panic）与因果路径（定时器触发是新的因果链
// 起源——回调内发起调用按顶层链起根）。
//
// ID 契约：进程内单调递增、从 1 起、永不复用；只在进程内有效（不跨重启、
// 不得写入快照——恢复后必须视为失效）；触发或移除后立即失效。实例已终局
// 时返回 0。单实例未触发上限缺省 65536（约 9 MB 上界），可按节点经
// `node.Config.Tuning.MaxTimers` 抬高（万级/十万级用法必须抬高），超限 panic。
//
// 生命周期：定时器纯内存，不进快照、不随迁移；触发不计入闲置判定（周期 tick
// 不会让实体永不钝化），因此钝化/迁移/销毁会丢弃未触发的定时器——需要跨
// 实例存活的提醒语义另立（reminder）。交接窗内不触发（窗回退后恢复），终局
// 后彻底静默。
func AddTimer(ctx context.Context, d time.Duration, fn func(context.Context)) int64 {
	p, ok := ctxkey.TimerPointFrom(ctx)
	if !ok {
		panic("rans: AddTimer called outside a runtime loop ctx (only Handle, continuation and timer callback ctx carry the timer point; rebuilt or plain ctx do not)")
	}
	arm, ok := p.(TimerArm)
	if !ok {
		panic("rans: AddTimer timer point payload is not a TimerArm (runtime contract broken)")
	}
	return arm.AddTimer(d, fn)
}

// RemoveTimer 取消一个尚未触发的定时器：true = 确实取消（此后不会再触发）；
// false = 该 ID 不存在、已触发、已被移除，或属于别的实体（幂等，清理路径
// 可无条件调用）。ctx 校验同 AddTimer。
func RemoveTimer(ctx context.Context, timerID int64) bool {
	p, ok := ctxkey.TimerPointFrom(ctx)
	if !ok {
		panic("rans: RemoveTimer called outside a runtime loop ctx (only Handle, continuation and timer callback ctx carry the timer point; rebuilt or plain ctx do not)")
	}
	arm, ok := p.(TimerArm)
	if !ok {
		panic("rans: RemoveTimer timer point payload is not a TimerArm (runtime contract broken)")
	}
	return arm.RemoveTimer(timerID)
}
