package ctxkey_test

import (
	"context"
	"testing"

	"github.com/cyinx/rans_golang/internal/ctxkey"
)

func TestSelfRoundTrip(t *testing.T) {
	ctx := ctxkey.WithSelf(context.Background(), "chat", "room-1")
	kind, key, ok := ctxkey.SelfFrom(ctx)
	if !ok || kind != "chat" || key != "room-1" {
		t.Fatalf("SelfFrom = (%q, %q, %v), want (chat, room-1, true)", kind, key, ok)
	}
	if _, _, ok := ctxkey.SelfFrom(context.Background()); ok {
		t.Fatal("absent self must miss")
	}
}

func TestIncarnationRoundTrip(t *testing.T) {
	ctx := ctxkey.WithIncarnation(context.Background(), 7)
	if inc, ok := ctxkey.IncarnationFrom(ctx); !ok || inc != 7 {
		t.Fatalf("IncarnationFrom = (%d, %v), want (7, true)", inc, ok)
	}
	// 0 值挂载与未挂载同义（0 值不可观测）。
	if _, ok := ctxkey.IncarnationFrom(ctxkey.WithIncarnation(context.Background(), 0)); ok {
		t.Fatal("zero incarnation must not be observable")
	}
	if _, ok := ctxkey.IncarnationFrom(context.Background()); ok {
		t.Fatal("absent incarnation must miss")
	}
}

func TestClientRoundTrip(t *testing.T) {
	type marker struct{}
	ctx := ctxkey.WithClient(context.Background(), &marker{})
	v, ok := ctxkey.ClientFrom(ctx)
	if !ok || v == nil {
		t.Fatalf("ClientFrom = (%v, %v), want non-nil true", v, ok)
	}
	if _, ok := ctxkey.ClientFrom(context.Background()); ok {
		t.Fatal("absent client must miss")
	}
}

func TestCausalRoundTrip(t *testing.T) {
	ctx := context.Background()
	if ctxkey.CausalFrom(ctx) != nil {
		t.Fatal("absent causal must be nil")
	}
	p := &ctxkey.CausalPath{Root: "r", Path: []uint64{1, 2}}
	if got := ctxkey.CausalFrom(ctxkey.WithCausal(ctx, p)); got != p {
		t.Fatal("causal must round-trip via ctx")
	}
}

func TestDeferPointRoundTrip(t *testing.T) {
	type marker struct{}
	if v, ok := ctxkey.DeferPointFrom(context.Background()); ok || v != nil {
		t.Fatalf("absent defer point = (%v, %v), want (nil, false)", v, ok)
	}
	p := &marker{}
	ctx := ctxkey.WithDeferPoint(context.Background(), p)
	if v, ok := ctxkey.DeferPointFrom(ctx); !ok || v != any(p) {
		t.Fatalf("DeferPointFrom = (%v, %v), want the injected point", v, ok)
	}
}

func TestCausalWithoutShadowsValue(t *testing.T) {
	// WithoutCausal（typed-nil 遮蔽——ctx 值无法移除的唯一实现）：遮蔽后
	// CausalFrom 返回 nil，调用方的路径不再对投递 ctx 可见（Tell 边界）。
	p := &ctxkey.CausalPath{Root: "r", Path: []uint64{9}}
	ctx := ctxkey.WithoutCausal(ctxkey.WithCausal(context.Background(), p))
	if got := ctxkey.CausalFrom(ctx); got != nil {
		t.Fatalf("shadowed causal must read as nil, got %+v", got)
	}
	// 遮蔽点之后的 WithCausal 正常生效（遮蔽只是同键覆写）。
	p2 := &ctxkey.CausalPath{Root: "r2"}
	if got := ctxkey.CausalFrom(ctxkey.WithCausal(ctx, p2)); got != p2 {
		t.Fatal("WithCausal after WithoutCausal must take effect")
	}
}
