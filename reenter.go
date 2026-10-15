package rans

import (
	"context"

	"github.com/cyinx/rans_golang/internal/ctxkey"
)

// ReenterArm 让出重入执行面（运行时投递边界实现——本地与远端投递在 Deliver
// 构造 Handle ctx 时注入，ReenterAfter 经 ctx 惯例取用；导出仅为跨包实现
// 签名的可写性）。
type ReenterArm interface {
	// Reenter 让出登记：派生 wctx（继承规则见 ReenterAfter）、在实体 loop
	// 外执行 work，then 作为续体任务回实体 loop 串行执行。非阻塞。
	Reenter(work func(ctx context.Context) (any, error), then func(ctx context.Context, v any, err error))
}

// ReenterAfter 在 Handle 内让出实体 loop（H12 决策 16/17 的让出重入原语）：
// work 在实体 loop 之外的专属 goroutine 上执行（每调用一个——等待外部 IO 的
// 独立生命周期，不做池化，生命周期由 wctx 的 deadline 与取消面收敛），完成
// 后 then 作为续体任务经 loop 信号通道回实体 loop 串行执行。让出期间实体邮
// 箱照常服务（可重入），让出点前后实体状态
// 的一致性由实体自行保证。
//
// ctx 必须是运行时投递边界的 Handle ctx（与 Defer 同源校验：请求与单向投递
// 均携带登记点；换造、重建或自派的 ctx 即无登记点）——否则 panic 于调用点。
// 同一 Handle 内多次调用各自生效（每次一个独立 work 生命周期）。
//
// wctx（传给 work 的 ctx）派生规则：
//   - 继承请求的 deadline/trace/因果路径与传播键值；work 必须尊重 wctx 取消
//     （协作式契约——取消来源见下，deadline 是不协作 work 的兜底）；
//   - 遮蔽自身身份（rans.Self 在 wctx 不可见）——让出期间 loop 空闲不构成
//     等待边，work 的出站调用不追加自身哈希：「A 的 work 调 B，B 再调 A」
//     正常完成；work 调回仍在同步等待的上游时上游哈希已在路径中，照样立即
//     判环（Cycle）。需在 work 中知晓自身身份时在让出点前自行捕获。
//
// tctx（传给 then 的续体 ctx）派生规则：
//   - 以 Handle ctx 的值链为底（Self/Incarnation/Client/因果路径/传播键
//     可用），脱钩其收尾 cancel 并重挂原请求 deadline——续体不随投递返回
//     收场，跑不到 deadline 之外的取消面；
//   - 无延迟应答登记点（无类型遮蔽）：then 内 rans.Defer 必 panic，不得
//     武装原请求的迟到应答；
//   - 让出面保持（then 内可嵌套 ReenterAfter）；then 返回后收尾 cancel。
//
// 典型两件套（契约正本）：
//
//	r := rans.Defer(ctx)                  // 本请求改延迟应答，拿 Responder
//	rans.ReenterAfter(ctx, work, then)    // then 里通常 r.Reply(v, err)
//	return rans.Deferred, nil             // 投递边界识别的哨兵返回值
//
// 只 ReenterAfter 不 Defer（Tell 单向形态）也合法——then 无 Reply 面，结果
// 经实体状态或后续消息自担。work 不得触碰实体状态（让出期间 loop 并发处理
// 他消息，无锁保护）——状态变更一律放 then（回 loop 后串行安全）。
//
// 在途 work 处置（H12 决策 17）：交接窗（Retire/Passivate/Migrate）等待在途
// work 完成（受交接窗预算约束，超时 cancel wctx；等待期 loop 只处理续体任
// 务，新邮件按 H6 留箱）；看门狗强弃立即 cancel wctx；MsgCancel 定位到
// work 在飞 → cancel wctx、已 Deferred 未 Reply → 标记取消（后续 Reply 丢
// 弃）；终局未完成的 Responder 以调用方 Timeout 收场（模糊结果）。执行看门
// 狗只统计 loop 同步执行段：work 期间不计入，then 回 loop 后重新计入。
func ReenterAfter(ctx context.Context, work func(ctx context.Context) (any, error), then func(ctx context.Context, v any, err error)) {
	p, ok := ctxkey.ReenterPointFrom(ctx)
	if !ok {
		panic("rans: ReenterAfter called outside a runtime Handle ctx (only deliveries injected by the runtime carry the reenter point; rebuilt or plain ctx do not)")
	}
	arm, ok := p.(ReenterArm)
	if !ok {
		panic("rans: ReenterAfter reenter point payload is not a ReenterArm (runtime contract broken)")
	}
	arm.Reenter(work, then)
}
