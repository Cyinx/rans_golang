package rans_test

import (
	"context"
	"testing"

	rans "github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/ctxkey"
)

func TestSelfFromWrittenContext(t *testing.T) {
	ctx := ctxkey.WithSelf(context.Background(), "acct", "a-1")
	id, ok := rans.Self(ctx)
	if !ok || id.Kind != "acct" || id.Key != "a-1" {
		t.Fatalf("Self = (%v, %v), want (acct/a-1, true)", id, ok)
	}
	if _, ok := rans.Self(context.Background()); ok {
		t.Fatal("absent self must miss")
	}
}

func TestIncarnationAccessor(t *testing.T) {
	ctx := ctxkey.WithIncarnation(context.Background(), 9)
	if inc, ok := rans.Incarnation(ctx); !ok || inc != 9 {
		t.Fatalf("Incarnation = (%d, %v), want (9, true)", inc, ok)
	}
	if _, ok := rans.Incarnation(context.Background()); ok {
		t.Fatal("absent incarnation must miss")
	}
}

func TestClientFromNilWhenAbsent(t *testing.T) {
	if c := rans.ClientFrom(context.Background()); c != nil {
		t.Fatalf("ClientFrom(absent) = %v, want nil", c)
	}
	// 注入值类型不符 Client 也返回 nil（不 panic）。
	if c := rans.ClientFrom(ctxkey.WithClient(context.Background(), 42)); c != nil {
		t.Fatalf("ClientFrom(wrong type) = %v, want nil", c)
	}
}

type stubClient struct{ calls int }

func (s *stubClient) Call(ctx context.Context, id rans.ID, msg any, opts ...rans.CallOption) (any, error) {
	s.calls++
	return nil, nil
}

func (s *stubClient) Tell(ctx context.Context, id rans.ID, msg any, opts ...rans.CallOption) error {
	s.calls++
	return nil
}

func TestClientFromInjected(t *testing.T) {
	sc := &stubClient{}
	ctx := ctxkey.WithClient(context.Background(), sc)
	c := rans.ClientFrom(ctx)
	if c == nil {
		t.Fatal("ClientFrom(injected) must be non-nil")
	}
	if _, err := c.Call(ctx, rans.ID{Kind: "k", Key: "v"}, nil); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if sc.calls != 1 {
		t.Fatalf("stub calls = %d, want 1", sc.calls)
	}
}

func TestIsIdempotent(t *testing.T) {
	if rans.IsIdempotent() {
		t.Fatal("no options must not be idempotent")
	}
	if !rans.IsIdempotent(rans.Idempotent()) {
		t.Fatal("Idempotent() must be observed")
	}
}
