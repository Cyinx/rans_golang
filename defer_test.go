package rans_test

// Defer 延迟应答的契约单测（H12 决策 16 前半，HR4 票 11）：误用形态的
// panic 面（非请求 ctx、重复 Defer）与 Responder 的收场契约（Reply 幂等、
// deadline 过后丢弃且无效、nil/零值空转）。投递边界的行为锚（本地+远端
// 两形态、编程错误的明确错误收场）在 testkit 验收套件。

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rans "github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/ctxkey"
)

// fakeDelivery 一次 Reply 的观测记录。
type fakeDelivery struct {
	v   any
	err error
}

// fakeArm 回包执行面道具：记录 DeliverDeferred 的全部调用（幂等/到期/取消
// 判定的观测锚）。
type fakeArm struct {
	mu        sync.Mutex
	delivered []fakeDelivery
	deadline  time.Time
	canceled  bool // DeferredCanceled 读数（取消丢弃判据的观测锚）
}

func (a *fakeArm) DeliverDeferred(v any, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.delivered = append(a.delivered, fakeDelivery{v: v, err: err})
}

func (a *fakeArm) DeferredDeadline() time.Time { return a.deadline }

// DeferredCanceled 取消标记读数（rans.Responder 丢弃判据的道具实现）。
func (a *fakeArm) DeferredCanceled() bool { return a.canceled }

func (a *fakeArm) calls() []fakeDelivery {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]fakeDelivery(nil), a.delivered...)
}

// fakePoint 登记点道具：实现 rans 的登记接口（导出方法签名——跨包结构
// 匹配），首次 ArmDefer 交出 arm，重复调用返回 nil（模拟投递边界语义）。
type fakePoint struct {
	arm   *fakeArm
	armed atomic.Bool
}

func (p *fakePoint) ArmDefer() rans.DeferredArm {
	if !p.armed.CompareAndSwap(false, true) {
		return nil
	}
	return p.arm
}

func withFakePoint(arm *fakeArm) context.Context {
	return ctxkey.WithDeferPoint(context.Background(), &fakePoint{arm: arm})
}

// mustPanic 断言 fn panic 且消息含 want 子串。
func mustPanic(t *testing.T, what, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("%s must panic, got none", what)
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, want) {
			t.Fatalf("%s panic = %v, want string containing %q", what, r, want)
		}
	}()
	fn()
}

func TestDefer_PanicsOutsideRequestCtx(t *testing.T) {
	// 裸 ctx（无投递关联）：编程错误，panic 于调用点。
	mustPanic(t, "Defer on plain ctx", "outside a request Handle ctx", func() {
		rans.Defer(context.Background())
	})
	// Tell（单向投递）的 Handle ctx 不携带登记点：同面 panic（单向无
	// 应答面，延迟应答无意义）。
	tellCtx := ctxkey.WithSelf(context.Background(), "defr", "t1")
	mustPanic(t, "Defer on Tell ctx", "outside a request Handle ctx", func() {
		rans.Defer(tellCtx)
	})
}

func TestDefer_SecondCallPanics(t *testing.T) {
	// 同一请求上重复 Defer：编程错误，panic 于调用点（登记点返回 nil）。
	ctx := withFakePoint(&fakeArm{})
	if r := rans.Defer(ctx); r == nil {
		t.Fatal("first Defer must return a responder")
	}
	mustPanic(t, "second Defer on the same request", "twice", func() {
		rans.Defer(ctx)
	})
}

func TestResponder_ReplyIdempotent(t *testing.T) {
	// Reply 幂等：仅首次生效，第二次及以后无效（值不同同样无效）。
	arm := &fakeArm{}
	r := rans.Defer(withFakePoint(arm))
	r.Reply(42, nil)
	r.Reply(43, errors.New("late"))
	got := arm.calls()
	if len(got) != 1 {
		t.Fatalf("delivered calls = %d, want 1 (second Reply must be a no-op)", len(got))
	}
	if got[0].v != 42 || got[0].err != nil {
		t.Fatalf("delivered = %+v, want {42, nil}", got[0])
	}
}

func TestResponder_ReplyAfterDeadlineDropped(t *testing.T) {
	// deadline 已过后的 Reply 丢弃且无效：后续 Reply 同样空转。
	arm := &fakeArm{deadline: time.Now().Add(-time.Second)}
	r := rans.Defer(withFakePoint(arm))
	r.Reply(42, nil)
	r.Reply(42, nil)
	if got := arm.calls(); len(got) != 0 {
		t.Fatalf("post-deadline Reply must be dropped, got %+v", got)
	}
	// deadline 未到：正常送达。
	arm2 := &fakeArm{deadline: time.Now().Add(time.Hour)}
	r2 := rans.Defer(withFakePoint(arm2))
	r2.Reply(7, nil)
	if got := arm2.calls(); len(got) != 1 || got[0].v != 7 {
		t.Fatalf("pre-deadline Reply must deliver, got %+v", got)
	}
}

func TestResponder_NilAndZeroNoOp(t *testing.T) {
	// nil 与零值 Responder 的 Reply 空转（不 panic——延迟应答的收场路径
	// 永不因应答器形态炸断实体 loop）。
	var nilResp *rans.Responder
	nilResp.Reply(42, nil)
	zero := &rans.Responder{}
	zero.Reply(42, nil)
}
