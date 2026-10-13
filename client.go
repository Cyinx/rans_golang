package rans

import (
	"context"
	"time"
)

// Client 是唯一调用入口：一切调用经路由——本地命中走本地快路径，错误
// 形态与远端一致。经 Node.Client() 或 ClientFrom(ctx) 获得。
type Client interface {
	// Call 请求-响应调用：阻塞到应答、错误或 ctx 结束；并发安全；可在
	// 实体 Handle 内调用（嵌套调用，注意 H12 调用环约定——落地前卡到
	// 本调用的 ctx 超时）。
	Call(ctx context.Context, id ID, msg any, opts ...CallOption) (any, error)
	// Tell 单向发送：入箱即返回（不阻塞于实体执行），无应答无回执；
	// 并发安全；可在实体 Handle 内调用。
	Tell(ctx context.Context, id ID, msg any, opts ...CallOption) error
}

// CallOption 配置一次 Call/Tell；只能由 rans 构造。
type CallOption interface {
	idempotent() bool
	timeout() time.Duration
}

type idempotentCall struct{}

func (idempotentCall) idempotent() bool       { return true }
func (idempotentCall) timeout() time.Duration { return 0 }

// Idempotent 声明本次调用幂等：仅幂等调用对「可能已执行」的错误
// （Timeout / ConnectionLost）自动重试（重试分类表见约定 spec §5.3）。
func Idempotent() CallOption { return idempotentCall{} }

// timeoutCall Timeout(d) 选项的载体（d ≤0 构造面已视为未传）。
type timeoutCall struct{ d time.Duration }

func (timeoutCall) idempotent() bool         { return false }
func (t timeoutCall) timeout() time.Duration { return t.d }

// Timeout 声明本次调用的预算上界（调用预算合成三源之一）：本次预算 =
// min(调用方 ctx 剩余, Timeout(d), 节点 Calls.Max)；调用方 ctx 无 deadline
// 且未传本选项时取 Calls.Default。调用方不能经 ctx 或本选项延长超出
// Calls.Max 的预算；本地与远端同一规则。d ≤0 视为未传；多个 Timeout
// 选项取最小者（保守侧）。
func Timeout(d time.Duration) CallOption { return timeoutCall{d: d} }

// IsIdempotent 报告选项集中是否含幂等标注（调用面实现读取，非消费方 API）。
func IsIdempotent(opts ...CallOption) bool {
	for _, o := range opts {
		if o != nil && o.idempotent() {
			return true
		}
	}
	return false
}

// TimeoutOf 报告选项集中的预算值（调用面实现读取，非消费方 API）：无有效
// 选项 → 0；多个有效选项取最小。
func TimeoutOf(opts ...CallOption) time.Duration {
	d := time.Duration(0)
	for _, o := range opts {
		if o == nil {
			continue
		}
		if v := o.timeout(); v > 0 && (d == 0 || v < d) {
			d = v
		}
	}
	return d
}
