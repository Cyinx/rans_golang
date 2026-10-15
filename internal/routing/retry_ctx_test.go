package routing

// 票 13 红测：重试环在两次尝试之间 ctx 结束时返回 *rans.Error（超时→
// Timeout、取消→Canceled）——不再泄漏上一次尝试的原始错误（CodeOf 曾为
// 上次的 Unavailable/SendFailed）。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

func TestRetry_CtxEndBetweenAttemptsReturnsRansError(t *testing.T) {
	sendErr := rans.NewError(rans.CodeUnavailable, rans.ReasonSendFailed, "boom") // RetryUnconditional
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := InvokeWithBackoff(ctx, DefaultOptions(),
		func(int) time.Duration { return time.Second },
		func(context.Context) error { return sendErr })
	if rans.CodeOf(err) != rans.CodeTimeout {
		t.Fatalf("deadline between attempts: err = %v (code=%v), want CodeTimeout", err, rans.CodeOf(err))
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel2()
	}()
	err = InvokeWithBackoff(ctx2, DefaultOptions(),
		func(int) time.Duration { return time.Second },
		func(context.Context) error { return sendErr })
	if rans.CodeOf(err) != rans.CodeCanceled {
		t.Fatalf("cancel between attempts: err = %v (code=%v), want CodeCanceled", err, rans.CodeOf(err))
	}
}
