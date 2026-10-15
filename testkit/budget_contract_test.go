package testkit

// 票 05 合同锚（绿侧）：排队/分区耗时不放大调用方预算——分区恢复后的
// 调用不迟到执行（对端零执行超预算请求）、Handle ctx 预算 ≤ 调用方剩余。
// 排队驻留窗口的确定性红证在 internal/rpc 单测（outbox_budget_test.go）。

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

type budgetProbeEntity struct {
	handled atomic.Int64
}

func (e *budgetProbeEntity) Activate(context.Context) error   { return nil }
func (e *budgetProbeEntity) Deactivate(context.Context) error { return nil }

func (e *budgetProbeEntity) Handle(ctx context.Context, msg any) (any, error) {
	e.handled.Add(1)
	return "ok", nil
}

// TestCall_QueuedPastBudgetNeverExecutes 调用预算 300ms、目标分区 800ms 后
// 恢复：调用按预算失败（Timeout 类），恢复后对端零执行该请求。
func TestCall_QueuedPastBudgetNeverExecutes(t *testing.T) {
	e := &budgetProbeEntity{}
	c := NewCluster(t, 2, map[string]rans.Factory{
		"budgetprobe": func(rans.ID) rans.Entity { return e },
	})
	id, _ := rans.NewID("budgetprobe", "p1")
	if _, err := c.Node(1).Call(context.Background(), id, "warm"); err != nil {
		t.Fatalf("pre-activate: %v", err)
	}
	c.Partition(0, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := c.Node(0).Call(ctx, id, "go"); err == nil {
		t.Fatal("call across partition must fail at budget")
	}
	c.Heal(0, 1)
	time.Sleep(500 * time.Millisecond) // 恢复后的观察窗（迟到帧若有必达）
	if n := e.handled.Load(); n != 1 {
		t.Fatalf("peer executed %d requests, want exactly 1 (the warm-up; expired frames must never execute late)", n)
	}
}
