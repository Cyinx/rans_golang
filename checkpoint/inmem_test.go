package checkpoint

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/cyinx/rans_golang"
)

// 编译期断言：InMemory 满足 Store（契约面钉死）。
var _ Store = (*InMemory)(nil)

func mustID(t *testing.T, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID("agent", inst)
	if err != nil {
		t.Fatalf("rans.NewID: %v", err)
	}
	return id
}

func TestInMemoryRoundTrip(t *testing.T) {
	s := New()
	id := mustID(t, "rt-1")
	snap := []byte("snapshot-payload-v1")

	if err := s.Put(context.Background(), id, 1, snap); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok, err := s.Get(context.Background(), id)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v, want ok=true err=nil", ok, err)
	}
	if !bytes.Equal(got, snap) {
		t.Fatalf("Get = %q, want %q", got, snap)
	}
}

func TestInMemoryPutOverwrites(t *testing.T) {
	s := New()
	id := mustID(t, "rt-2")
	_ = s.Put(context.Background(), id, 0, []byte("v1"))
	if err := s.Put(context.Background(), id, 0, []byte("v2")); err != nil {
		t.Fatalf("second Put: %v", err)
	}
	got, ok, _ := s.Get(context.Background(), id)
	if !ok || !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("Get after overwrite = %q ok=%v, want v2 ok=true", got, ok)
	}
}

// Put 拷贝入：调用方在 Put 之后改写原切片不得影响已存内容。
func TestInMemoryPutCopiesIn(t *testing.T) {
	s := New()
	id := mustID(t, "copy-in")
	buf := []byte("original")
	if err := s.Put(context.Background(), id, 0, buf); err != nil {
		t.Fatalf("Put: %v", err)
	}
	buf[0] = 'X'
	got, ok, _ := s.Get(context.Background(), id)
	if !ok || !bytes.Equal(got, []byte("original")) {
		t.Fatalf("stored = %q, want %q (Put must copy in)", got, "original")
	}
}

// Get 拷贝出：调用方改写 Get 返回的切片不得影响已存内容。
func TestInMemoryGetCopiesOut(t *testing.T) {
	s := New()
	id := mustID(t, "copy-out")
	if err := s.Put(context.Background(), id, 0, []byte("original")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	first, _, _ := s.Get(context.Background(), id)
	first[0] = 'X'
	second, ok, _ := s.Get(context.Background(), id)
	if !ok || !bytes.Equal(second, []byte("original")) {
		t.Fatalf("stored = %q, want %q (Get must copy out)", second, "original")
	}
}

func TestInMemoryGetMissing(t *testing.T) {
	s := New()
	got, ok, err := s.Get(context.Background(), mustID(t, "missing"))
	if got != nil || ok || err != nil {
		t.Fatalf("Get missing = (%q, %v, %v), want (nil, false, nil)", got, ok, err)
	}
}

func TestInMemoryDeleteIdempotent(t *testing.T) {
	s := New()
	id := mustID(t, "del")
	// 未命中删除：no-op 不报错。
	if err := s.Delete(context.Background(), id); err != nil {
		t.Fatalf("Delete on missing: %v, want nil", err)
	}
	if err := s.Put(context.Background(), id, 0, []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete(context.Background(), id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, _ := s.Get(context.Background(), id); ok {
		t.Fatal("Get after Delete must miss")
	}
	// 重复删除仍 no-op。
	if err := s.Delete(context.Background(), id); err != nil {
		t.Fatalf("repeat Delete: %v, want nil", err)
	}
}

// 零值可用：&InMemory{} 与 New() 等价（懒初始化内部 map）。
func TestInMemoryZeroValueUsable(t *testing.T) {
	s := &InMemory{}
	id := mustID(t, "zero")
	if _, ok, _ := s.Get(context.Background(), id); ok {
		t.Fatal("zero-value Get must miss")
	}
	if err := s.Delete(context.Background(), id); err != nil {
		t.Fatalf("zero-value Delete: %v", err)
	}
	if err := s.Put(context.Background(), id, 0, []byte("v")); err != nil {
		t.Fatalf("zero-value Put: %v", err)
	}
	if got, ok, _ := s.Get(context.Background(), id); !ok || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("zero-value Get after Put = %q ok=%v", got, ok)
	}
}

// 空/nil 快照 round-trip：字节内容按 bytes.Equal 语义无损。
func TestInMemoryEmptyAndNilSnapshot(t *testing.T) {
	s := New()
	emptyID, nilID := mustID(t, "empty"), mustID(t, "nil")
	if err := s.Put(context.Background(), emptyID, 0, []byte{}); err != nil {
		t.Fatalf("Put empty: %v", err)
	}
	if err := s.Put(context.Background(), nilID, 0, nil); err != nil {
		t.Fatalf("Put nil: %v", err)
	}
	for _, id := range []rans.ID{emptyID, nilID} {
		got, ok, err := s.Get(context.Background(), id)
		if err != nil || !ok || len(got) != 0 {
			t.Fatalf("Get(%s) = (%q, %v, %v), want empty hit", id.String(), got, ok, err)
		}
	}
}

// 并发读写删混合（-race 下验证互斥保护）。
func TestInMemoryConcurrent(t *testing.T) {
	s := New()
	const (
		workers = 8
		iters   = 200
	)
	ids := make([]rans.ID, 4)
	for i := range ids {
		ids[i] = mustID(t, "conc-"+string(rune('a'+i)))
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < iters; i++ {
				id := ids[(w+i)%len(ids)]
				switch i % 3 {
				case 0:
					if err := s.Put(ctx, id, 0, []byte{byte(w), byte(i)}); err != nil {
						t.Errorf("Put: %v", err)
						return
					}
				case 1:
					if _, _, err := s.Get(ctx, id); err != nil {
						t.Errorf("Get: %v", err)
						return
					}
				default:
					if err := s.Delete(ctx, id); err != nil {
						t.Errorf("Delete: %v", err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
}
