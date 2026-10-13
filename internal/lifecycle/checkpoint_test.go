package lifecycle

// 周期 checkpoint 编排器（规格 §15：间隔默认 1min、单轮上限 1000）——
// 与 Watchdog 同族的全回调注入形态。

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestCheckpointSweeperDefaultsAndScan：零值配置回落缺省（1min/1000）；
// Scan 以缺省/覆盖上限驱动 Sweep 面并汇总起始数。
func TestCheckpointSweeperDefaultsAndScan(t *testing.T) {
	var mu sync.Mutex
	var gotMax int
	s := &CheckpointSweeper{
		Sweep: func(ctx context.Context, maxCount int) int {
			mu.Lock()
			gotMax = maxCount
			mu.Unlock()
			return 3
		},
	}
	if s.effectiveInterval() != DefaultCheckpointInterval {
		t.Fatalf("default interval = %v, want %v", s.effectiveInterval(), DefaultCheckpointInterval)
	}
	if n := s.Scan(context.Background()); n != 3 {
		t.Fatalf("Scan returned %d, want 3", n)
	}
	mu.Lock()
	max := gotMax
	mu.Unlock()
	if max != DefaultCheckpointRound {
		t.Fatalf("scan maxCount = %d, want default %d", max, DefaultCheckpointRound)
	}

	s.Config.MaxPerRound = 7
	s.Scan(context.Background())
	mu.Lock()
	max = gotMax
	mu.Unlock()
	if max != 7 {
		t.Fatalf("scan maxCount = %d, want override 7", max)
	}
}

// TestCheckpointSweeperRunTicks：Run 周期驱动 Sweep 直至 ctx 取消。
func TestCheckpointSweeperRunTicks(t *testing.T) {
	ticks := make(chan struct{}, 4)
	s := &CheckpointSweeper{
		Sweep: func(ctx context.Context, maxCount int) int {
			ticks <- struct{}{}
			return 0
		},
	}
	s.Config.Interval = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	<-ticks
	<-ticks // 至少两轮：周期性确证
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run must exit after ctx cancel")
	}
}
