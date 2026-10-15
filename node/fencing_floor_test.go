package node

// FencingFloor 宿主钩子（A2 运行时面）：宿主自持存储经钩子报告的纪元
// 下限参与激活取号——Handle 携带的纪元必须高于钩子值（宿主围栏不被
// 跳过）。

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// floorSeenEntity Handle 记录投递层携带的纪元。
type floorSeenEntity struct{ inc atomic.Uint64 }

func (e *floorSeenEntity) Activate(context.Context) error   { return nil }
func (e *floorSeenEntity) Deactivate(context.Context) error { return nil }
func (e *floorSeenEntity) Handle(ctx context.Context, _ any) (any, error) {
	if v, ok := rans.Incarnation(ctx); ok {
		e.inc.Store(v)
	}
	return "ok", nil
}

// TestNodeFencingFloor_IncarnationAboveHostFloor 钩子对目标 id 返回 41：
// 首次激活的 Handle 纪元 == 42（下限 41 + 取号 +1），未登记 id 不受影响。
func TestNodeFencingFloor_IncarnationAboveHostFloor(t *testing.T) {
	seen := &floorSeenEntity{}
	plain := &floorSeenEntity{}
	n := mustStartNode(t, Config{
		NodeID:     1,
		Standalone: true,
		Kinds: map[string]rans.Factory{
			"floored": func(_ rans.ID) rans.Entity { return seen },
			"plain":   func(_ rans.ID) rans.Entity { return plain },
		},
		FencingFloor: func(_ context.Context, id rans.ID) (uint64, error) {
			if id.Key == "host-floored" {
				return 41, nil
			}
			return 0, nil
		},
	})
	defer shutdownNode(n)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	flooredID := mustID(t, "floored", "host-floored")
	if _, err := n.Client().Call(ctx, flooredID, "x"); err != nil {
		t.Fatalf("floored call: %v", err)
	}
	if got := seen.inc.Load(); got != 42 {
		t.Fatalf("incarnation seen by Handle = %d, want 42 (host floor 41 + 1)", got)
	}
	plainID := mustID(t, "plain", "no-floor")
	if _, err := n.Client().Call(ctx, plainID, "x"); err != nil {
		t.Fatalf("plain call: %v", err)
	}
	if got := plain.inc.Load(); got != 1 {
		t.Fatalf("plain incarnation = %d, want 1 (no floor)", got)
	}
}
