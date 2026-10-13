package routing

import "time"

// InvokeOptions 调用级选项：幂等标注与调用预算的运行时半边（幂等双轨
// 机制论证见 docs/decisions/2026-09-30-
// phase4-idempotency.md）。调用方在 Call/OneWay 逐次传入；重试环对模糊
// 投递类失败（RetryAmbiguous）仅在 Idempotent=true 时重试。
type InvokeOptions struct {
	// Idempotent 标注本次调用可安全重放（重复执行不产生额外副作用）。
	// 零值 false——未标注按非幂等处理（保守侧）：模糊投递失败上抛调用方，
	// 重试副作用风险由标注者显式承担。
	Idempotent bool
	// Timeout 调用预算选项值（rans.Timeout(d) 的运行时半边；0 = 未传）。
	// 参与逐请求预算合成 min(调用方 ctx 剩余, Timeout, MaxCallTimeout)，
	// 合成落点在 rpc 接线层。
	Timeout time.Duration
}

// DefaultOptions 返回默认调用选项：零值（非幂等、无预算选项）。
func DefaultOptions() InvokeOptions { return InvokeOptions{} }
