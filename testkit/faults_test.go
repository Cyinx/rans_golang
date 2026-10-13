package testkit

// 故障注入面工具自测（faults.go）——Store 读写故障包装器与续租丢键目录
// 包装器的正反两面。集群链路的行为验证归场景矩阵
//（invariant_history_test.go），本文件只锁注入原语自身的语义。

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/checkpoint"
)

// TestFaultStoreTransientFailures 瞬时失败注入：前 N 次 Get/Put 失败，
// 额度耗尽后放行；失败计入统计。
func TestFaultStoreTransientFailures(t *testing.T) {
	ctx := context.Background()
	base := checkpoint.New()
	s := NewFaultStore(base)
	id := idOf("k", "transient")

	s.FailGets(2)
	for i := 0; i < 2; i++ {
		if _, _, err := s.Get(ctx, id); err == nil || !errors.Is(err, ErrInjectedFault) {
			t.Fatalf("第 %d 次 Get 必须以注入错误失败, got %v", i+1, err)
		}
	}
	if _, _, err := s.Get(ctx, id); err != nil {
		t.Fatalf("额度耗尽后 Get 必须放行, got %v", err)
	}

	s.FailPuts(1)
	if err := s.Put(ctx, id, 1, []byte{1}); err == nil || !errors.Is(err, ErrInjectedFault) {
		t.Fatalf("首次 Put 必须以注入错误失败, got %v", err)
	}
	if err := s.Put(ctx, id, 1, []byte{1, 2}); err != nil {
		t.Fatalf("额度耗尽后 Put 必须放行: %v", err)
	}
	attempts, failures := s.PutStats()
	if attempts != 2 || failures != 1 {
		t.Fatalf("PutStats = (%d,%d), want (2,1)", attempts, failures)
	}
	// 放行的写入真实落底座。
	if b, found, _ := base.Get(ctx, id); !found || len(b) != 2 {
		t.Fatalf("放行的 Put 必须落底座: (%v bytes, found=%v)", len(b), found)
	}
}

// TestFaultStorePersistentFailures 持续失败注入（额度 -1）：反复失败直至
// Heal，Heal 后放行（Passivate 回退/Retire 终局的编排抓手）。
func TestFaultStorePersistentFailures(t *testing.T) {
	ctx := context.Background()
	s := NewFaultStore(checkpoint.New())
	id := idOf("k", "persistent")

	s.FailPuts(-1)
	for i := 0; i < 3; i++ {
		if err := s.Put(ctx, id, 1, []byte{1}); err == nil {
			t.Fatal("持续失败下 Put 必须失败")
		}
	}
	s.HealPuts()
	if err := s.Put(ctx, id, 1, []byte{1}); err != nil {
		t.Fatalf("Heal 后 Put 必须放行: %v", err)
	}
	_, failures := s.PutStats()
	if failures != 3 {
		t.Fatalf("失败统计 = %d, want 3", failures)
	}
}

// TestFaultStoreSlowPuts 慢 Put 注入：Put 耗时不低于注入值（慢周期 Put
// 的确定性抓手），恢复 0 后不再延迟。
func TestFaultStoreSlowPuts(t *testing.T) {
	ctx := context.Background()
	s := NewFaultStore(checkpoint.New())
	id := idOf("k", "slow")

	s.SlowPuts(60 * time.Millisecond)
	start := time.Now()
	if err := s.Put(ctx, id, 1, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 60*time.Millisecond {
		t.Fatalf("慢 Put 必须 ≥ 注入延迟, took %v", d)
	}
	s.SlowPuts(0)
	start = time.Now()
	if err := s.Put(ctx, id, 1, []byte{2}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 30*time.Millisecond {
		t.Fatalf("恢复后 Put 不应再延迟, took %v", d)
	}
}
