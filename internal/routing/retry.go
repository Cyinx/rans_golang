// retry.go 实现分类重试环（规格 §9.2）：上限 2 次重试、
// (n+1)*20ms 线性退避、载荷工厂契约；本地命中路径零重试由接线层绕过。
package routing

import (
	"context"
	"time"
)

// maxAttempts 总尝试上限 = 初次 + 2 次重试（规格 §9.2「上限 2 次」）。
const maxAttempts = 3

// DefaultBackoff 默认线性退避：(n+1)×20ms，n 为刚失败尝试的 0 基序号
// （首败后 20ms、次败后 40ms）。
func DefaultBackoff(attempt int) time.Duration {
	return time.Duration(attempt+1) * 20 * time.Millisecond
}

// Invoke 是 InvokeWithBackoff 的默认退避封装（DefaultBackoff）。
func Invoke(ctx context.Context, opts InvokeOptions, attempt func(ctx context.Context) error) error {
	return InvokeWithBackoff(ctx, opts, DefaultBackoff, attempt)
}

// InvokeWithBackoff 分类重试环：传输无关的 attempt 编排器。
//
// attempt 载荷工厂契约（规格 §9.2）：每次调用必须重建本次载荷（envelope
// 重新编码）——池化缓冲随失败归还，同载荷重试会读到已归还/复用的内存。
// 该契约由调用方闭包保证（本函数只承诺每次重试重新调用 attempt），
// 接线层在闭包内做「序列化 + 取池」。
//
// 环行为：
//  1. attempt 失败 → Classify：
//     RetryUnconditional / RetryNotFound → 重试；
//     RetryAmbiguous → 仅 opts.Idempotent 时重试；
//     NoRetry → 立即返回该错误；
//  2. 重试前按 backoff(刚失败尝试的 0 基序号) 退避，ctx 取消则中止
//     返回末次错误；
//  3. 重试耗尽（共 maxAttempts=3 次 attempt）→ 返回末次错误；
//  4. attempt panic 原样穿透，不吞不改。
//
// 错误形态（统一错误模型）：Classify 只看链上首个 *rans.Error 的
// Code/Reason（分类唯一表见 classify.go）——本地回绝与远端 Error 帧在产生
// 点已归一为同一形态，环传输无关；本地命中路径零重试仍由接线层绕过环
// （调用结构不变），环内翻转腿的本地错误经同一分类表裁决。本包不反向
// import activation（依赖只允许向下：activation → routing）。
func InvokeWithBackoff(ctx context.Context, opts InvokeOptions, backoff func(attempt int) time.Duration, attempt func(ctx context.Context) error) error {
	var lastErr error
	for attemptNo := 0; attemptNo < maxAttempts; attemptNo++ {
		if attemptNo > 0 {
			// 先显式检查再进 select：ctx 已 done 时确定性中止，不与
			// 零/极短退避的立即到通计时器发生 select 随机二选一。
			if ctx.Err() != nil {
				return lastErr
			}
			select {
			case <-ctx.Done():
				return lastErr
			case <-time.After(backoff(attemptNo - 1)):
			}
		}
		err := attempt(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		switch Classify(err) {
		case RetryUnconditional, RetryNotFound:
			// 无条件重试：消息未投递/位置陈旧，重建载荷再试。
		case RetryAmbiguous:
			if !opts.Idempotent {
				return err
			}
		default: // NoRetry：确定性失败直抛。
			return err
		}
	}
	return lastErr
}

// ShouldEnterRing 报告错误是否应进入重试环：可重试类（含幂等判定的
// RetryAmbiguous——进环后由 InvokeOptions 再裁）为 true；NoRetry（含
// nil、ctx 取消、Busy/Overloaded 过载保护）为 false。接线层用它决定
// 失败后是否失效位置缓存（invalidate 钩子）。
func ShouldEnterRing(err error) bool {
	switch Classify(err) {
	case RetryUnconditional, RetryNotFound, RetryAmbiguous:
		return true
	default:
		return false
	}
}
