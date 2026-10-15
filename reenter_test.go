package rans_test

// ReenterAfter 让出重入的契约单测（H12 决策 16 后半，HR4 票 12）：非 Handle
// ctx 的 panic 面、Tell（单向）形态的合法性（登记点在位即委派）与 Responder
// 的取消丢弃扩展（已标记取消的 Reply 丢弃且无效）。行为锚（两件套、判环、
// 交接窗等待、MsgCancel 联动）在 testkit 验收套件。

import (
	"context"
	"testing"

	rans "github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/ctxkey"
)

// fakeReenterArm 让出执行面道具：记录 ReenterAfter 的同步委派。
type fakeReenterArm struct {
	works int
}

func (a *fakeReenterArm) Reenter(work func(context.Context) (any, error), then func(context.Context, any, error)) {
	a.works++
}

func TestReenterAfter_PanicsOutsideHandleCtx(t *testing.T) {
	// 裸 ctx（无投递关联）：编程错误，panic 于调用点（与 Defer 同源校验——
	// 换造、重建或自派的 ctx 即无登记点）。
	mustPanic(t, "ReenterAfter on plain ctx", "outside a runtime Handle ctx", func() {
		rans.ReenterAfter(context.Background(),
			func(context.Context) (any, error) { return nil, nil }, nil)
	})
}

func TestReenterAfter_TellFormDelegates(t *testing.T) {
	// 只 ReenterAfter 不 Defer（Tell 单向形态）合法：登记点在位即委派执行面
	// ——无 Defer 前置要求（then 无 Reply 面，结果经实体状态或后续消息自担）。
	arm := &fakeReenterArm{}
	ctx := ctxkey.WithReenterPoint(context.Background(), arm)
	rans.ReenterAfter(ctx,
		func(context.Context) (any, error) { return "v", nil }, func(context.Context, any, error) {})
	if arm.works != 1 {
		t.Fatalf("arm Reenter calls = %d, want 1", arm.works)
	}
}

func TestResponder_ReplyAfterCancelDropped(t *testing.T) {
	// 已标记取消的请求：Reply 丢弃且无效（MsgCancel 定位已 Deferred 未 Reply
	// 的请求 → 标记 → 迟到收场不再投递）。
	arm := &fakeArm{canceled: true}
	r := rans.Defer(withFakePoint(arm))
	r.Reply(42, nil)
	if got := arm.calls(); len(got) != 0 {
		t.Fatalf("canceled Reply must be dropped, got %+v", got)
	}
	// 未标记的请求照常送达（丢弃只针对已标记时刻的收场；同一 Responder 的
	// 幂等 CAS 已在首次 Reply 落定，对照面用独立请求）。
	arm2 := &fakeArm{}
	r2 := rans.Defer(withFakePoint(arm2))
	r2.Reply(7, nil)
	if got := arm2.calls(); len(got) != 1 || got[0].v != 7 {
		t.Fatalf("uncanceled Reply must deliver, got %+v", got)
	}
}
