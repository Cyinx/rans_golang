package checkpoint

// InMemory 围栏语义单测：回退纪元拒绝（ErrStaleIncarnation）、
// 同纪元重试允许、0 直通（未围栏形态）、Delete 不撤销纪元高水位。

import (
	"context"
	"errors"
	"testing"

	"github.com/cyinx/rans_golang"
)

func fenceID(t *testing.T, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID("fence", inst)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestInMemoryFenceRejectsStaleIncarnation(t *testing.T) {
	s := New()
	id := fenceID(t, "f1")
	ctx := context.Background()

	if err := s.Put(ctx, id, 5, []byte("v5")); err != nil {
		t.Fatalf("put inc=5: %v", err)
	}
	err := s.Put(ctx, id, 3, []byte("v3"))
	if !errors.Is(err, ErrStaleIncarnation) {
		t.Fatalf("put inc=3 err = %v, want ErrStaleIncarnation", err)
	}
	got, ok, err := s.Get(ctx, id)
	if err != nil || !ok || string(got) != "v5" {
		t.Fatalf("get after rejected stale put = %q %v %v, want v5 true nil", got, ok, err)
	}

	// 同纪元重试：允许覆盖（checkpoint 重试语义）。
	if err := s.Put(ctx, id, 5, []byte("v5b")); err != nil {
		t.Fatalf("same-incarnation retry must be accepted: %v", err)
	}
	// 更高纪元：接受。
	if err := s.Put(ctx, id, 6, []byte("v6")); err != nil {
		t.Fatalf("higher incarnation must be accepted: %v", err)
	}
}

func TestInMemoryFenceZeroBypasses(t *testing.T) {
	s := New()
	id := fenceID(t, "f2")
	ctx := context.Background()

	if err := s.Put(ctx, id, 5, []byte("v5")); err != nil {
		t.Fatal(err)
	}
	// 0 = 未围栏直通（目录无纪元源的裸形态）。
	if err := s.Put(ctx, id, 0, []byte("unfenced")); err != nil {
		t.Fatalf("inc=0 must bypass fencing: %v", err)
	}
	got, _, _ := s.Get(ctx, id)
	if string(got) != "unfenced" {
		t.Fatalf("get = %q, want unfenced overwrite", got)
	}
}

func TestInMemoryFenceDeleteKeepsHighWater(t *testing.T) {
	s := New()
	id := fenceID(t, "f3")
	ctx := context.Background()

	if err := s.Put(ctx, id, 7, []byte("v7")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	// 删除不是重置：旧纪元写入仍被拒（高水位存续——zombie 围栏前提）。
	if err := s.Put(ctx, id, 6, []byte("v6")); !errors.Is(err, ErrStaleIncarnation) {
		t.Fatalf("put inc=6 after delete err = %v, want ErrStaleIncarnation", err)
	}
	if err := s.Put(ctx, id, 8, []byte("v8")); err != nil {
		t.Fatalf("put inc=8 after delete must be accepted: %v", err)
	}
}
