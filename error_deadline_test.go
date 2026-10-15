package rans_test

// 票 13 红测：超时类 *rans.Error 满足标准库判错 errors.Is(err,
// context.DeadlineExceeded)——Code==Timeout 即匹配（Reason 无关）；取消类
// 与其他 Code 不误匹配。

import (
	"context"
	"errors"
	"testing"

	"github.com/cyinx/rans_golang"
)

func TestError_TimeoutMatchesDeadlineExceeded(t *testing.T) {
	if !errors.Is(rans.ErrTimeout, context.DeadlineExceeded) {
		t.Fatal("ErrTimeout must satisfy errors.Is(err, context.DeadlineExceeded)")
	}
	if !errors.Is(rans.NewError(rans.CodeTimeout, rans.ReasonExpired, "x"), context.DeadlineExceeded) {
		t.Fatal("Code==Timeout must match DeadlineExceeded regardless of Reason")
	}
	if errors.Is(rans.ErrCanceled, context.DeadlineExceeded) {
		t.Fatal("canceled must not match DeadlineExceeded")
	}
	if errors.Is(rans.NewError(rans.CodeUnavailable, rans.ReasonSendFailed, "x"), context.DeadlineExceeded) {
		t.Fatal("non-timeout code must not match DeadlineExceeded")
	}
}
