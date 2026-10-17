// 唯一投递信封（票 08）：本地 Call / 本地 Tell / 远端三条入站路径共用的
// Handle ctx 构造——Self/Incarnation 注入、DeferPoint 显式给定、deadline
// 重挂与收尾 cancel、让出面注入在同一定义收口，位置透明不再靠三处逐项
// 对齐维持（此前已连续三轮分叉）。
package pawnite

import (
	"context"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/ctxkey"
)

// EnvelopeForm 投递形态（Handle ctx 的取消面语义）：请求形态调用方取消
// 穿透（慢实体可观测取消、在箱取消经 CancelAware 弃置）；单向形态脱钩
// 发送腿取消面（预算 cancel 随发送返回触发，Handle 只承袭 deadline）。
type EnvelopeForm int

const (
	// EnvelopeCall 请求形态：Handle ctx 挂调用方取消链。
	EnvelopeCall EnvelopeForm = iota
	// EnvelopeOneWay 单向形态：Handle ctx 脱钩发送腿（WithoutCancel）。
	EnvelopeOneWay
)

// noopCancel 无挂载层的收尾空操作（链上已带同源 deadline 时返回）。
func noopCancel() {}

// HandleCtx 构造 Handle ctx 与让出面（三条入站路径的唯一信封）：形态决定
// 取消面；deadline 只在链上无同源 deadline 时重挂（双值叠层有微秒级翻转
// 竞态——内层到期而父链尚未翻转 Err 时，取消传播惯例的折叠判据会被误判，
// 票 08 实证）；DeferPoint 显式给定（nil = 无类型遮蔽——单向无应答面，
// rans.Defer 必 panic）。armDetached 决定让出面与 Handle ctx 链的关系：
// 远端/单向 detached（work 跑到请求 deadline，在飞取消经 arm.CancelWorks
// ——MsgCancel 定位面）；本地请求腿挂链（调用方取消/请求 deadline 自然
// 穿透到 work——发送腿不提前释放预算 ctx，见 rpc callOnce 本地腿）。
// 返回的 cancel 由调用方 defer；arm 供取消面登记。
func (p *Pawnite) HandleCtx(base context.Context, id rans.ID, form EnvelopeForm,
	deadline time.Time, deferPoint any,
	armDetached bool) (context.Context, context.CancelFunc, *ReenterArm) {
	if form == EnvelopeOneWay {
		base = context.WithoutCancel(base)
	}
	ctx := ctxkey.WithSelf(ctxkey.WithIncarnation(base, p.Incarnation()), id.Kind, id.Key)
	if deferPoint != nil {
		deferPoint = &deferAccounting{p: p, inner: deferPoint.(deferPointArm)} // 终局记账（票 11）
	}
	ctx = ctxkey.WithDeferPoint(ctx, deferPoint)
	cancel := context.CancelFunc(noopCancel)
	if _, has := ctx.Deadline(); !has && !deadline.IsZero() {
		ctx, cancel = context.WithDeadline(ctx, deadline)
	}
	ctx, arm := p.newLoopArm(ctx, armDetached)
	return ctx, cancel, arm
}
