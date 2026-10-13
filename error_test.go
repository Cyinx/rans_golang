package rans_test

import (
	"errors"
	"fmt"
	"testing"

	rans "github.com/cyinx/rans_golang"
)

func TestErrorIsCodeDimension(t *testing.T) {
	if !errors.Is(rans.NewError(rans.CodeTimeout, rans.ReasonNone, "x"), rans.ErrTimeout) {
		t.Fatal("CodeTimeout must match ErrTimeout")
	}
	if errors.Is(rans.NewError(rans.CodeNotFound, rans.ReasonNone, "x"), rans.ErrTimeout) {
		t.Fatal("CodeNotFound must not match ErrTimeout")
	}
}

func TestErrorIsReasonDimension(t *testing.T) {
	e := rans.NewError(rans.CodeUnavailable, rans.ReasonBusy, "x")
	// target 指定 Reason：Code+Reason 同时相等才命中。
	if !errors.Is(e, rans.NewError(rans.CodeUnavailable, rans.ReasonBusy, "other")) {
		t.Fatal("same Code+Reason must match")
	}
	if errors.Is(e, rans.NewError(rans.CodeUnavailable, rans.ReasonOverloaded, "")) {
		t.Fatal("different Reason must not match")
	}
	// target 零 Reason：只比 Code。
	if !errors.Is(e, rans.NewError(rans.CodeUnavailable, rans.ReasonNone, "")) {
		t.Fatal("zero-Reason target must match on Code alone")
	}
}

func TestErrorIsAppCodeDimension(t *testing.T) {
	e := rans.NewError(rans.CodeError, rans.ReasonHandler, "x")
	e.AppCode = 42
	if !errors.Is(e, &rans.Error{Code: rans.CodeError, Reason: rans.ReasonHandler, AppCode: 42}) {
		t.Fatal("same AppCode must match")
	}
	if errors.Is(e, &rans.Error{Code: rans.CodeError, Reason: rans.ReasonHandler, AppCode: 43}) {
		t.Fatal("different AppCode must not match")
	}
}

func TestCodeOfReasonOf(t *testing.T) {
	e := rans.NewError(rans.CodeTimeout, rans.ReasonNone, "x")
	if got := rans.CodeOf(e); got != rans.CodeTimeout {
		t.Fatalf("CodeOf = %d, want CodeTimeout", got)
	}
	if got := rans.ReasonOf(e); got != rans.ReasonNone {
		t.Fatalf("ReasonOf = %d, want ReasonNone", got)
	}
	// 包装链穿透。
	w := fmt.Errorf("outer: %w", e)
	if got := rans.CodeOf(w); got != rans.CodeTimeout {
		t.Fatalf("CodeOf(wrapped) = %d, want CodeTimeout", got)
	}
	// nil → 零值。
	if got := rans.CodeOf(nil); got != rans.CodeOK {
		t.Fatalf("CodeOf(nil) = %d, want CodeOK", got)
	}
	// 非 rans 错误 → 通用码。
	if got := rans.CodeOf(errors.New("io")); got != rans.CodeError {
		t.Fatalf("CodeOf(plain) = %d, want CodeError", got)
	}
}

func TestErrorSentinelDistinctCodes(t *testing.T) {
	pairs := [][2]error{
		{rans.ErrTimeout, rans.ErrNotFound},
		{rans.ErrNotFound, rans.ErrUnavailable},
		{rans.ErrUnavailable, rans.ErrCanceled},
		{rans.ErrCanceled, rans.ErrConnectionLost},
		{rans.ErrConnectionLost, rans.ErrSerialization},
		{rans.ErrSerialization, rans.ErrDecode},
	}
	for _, p := range pairs {
		if errors.Is(p[0], p[1]) {
			t.Fatalf("%v must not match %v", p[0], p[1])
		}
	}
}
