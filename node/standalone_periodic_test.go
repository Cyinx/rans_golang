package node

// Standalone 周期任务族（看门狗/周期 checkpoint）：与传输形态无关的生命
// 周期任务两种形态都要起跑——单节点缺看门狗=执行卡死无收敛黑洞，缺周期
// checkpoint=有状态实体只靠停机终快照。

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
)

// snapEntity 计数实体（Snapshotter）：快照承载执行计数值。
type snapEntity struct{ n atomic.Int32 }

func (e *snapEntity) Activate(context.Context) error   { return nil }
func (e *snapEntity) Deactivate(context.Context) error { return nil }
func (e *snapEntity) Handle(_ context.Context, _ any) (any, error) {
	return e.n.Add(1), nil
}
func (e *snapEntity) Snapshot() ([]byte, error) { return []byte(strconv.Itoa(int(e.n.Load()))), nil }
func (e *snapEntity) Restore(b []byte) error    { return nil }

// TestNodeStandalone_PeriodicCheckpointPersists Standalone + Store + 秒级
// checkpoint 间隔：一次调用后，有界时间内 Store 出现该实体的快照——周期
// sweeper 在单节点形态照常起跑。
func TestNodeStandalone_PeriodicCheckpointPersists(t *testing.T) {
	store := checkpoint.New()
	n := mustStartNode(t, Config{
		NodeID:     1,
		Standalone: true,
		Kinds: map[string]rans.Factory{
			"cell": func(rans.ID) rans.Entity { return &snapEntity{} },
		},
		Store:  store,
		Tuning: TuningConfig{CheckpointInterval: 100 * time.Millisecond},
	})
	defer shutdownNode(n)
	id := mustID(t, "cell", "ck-1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := n.Client().Call(ctx, id, "req"); err != nil {
		t.Fatalf("call: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, found, _ := store.Get(ctx, id); found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for periodic checkpoint to land in store")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// stuckHandleEntity Handle 永久阻塞（测试收尾放行，避免 goroutine 泄漏面）。
type stuckHandleEntity struct{ release chan struct{} }

func (e *stuckHandleEntity) Activate(context.Context) error   { return nil }
func (e *stuckHandleEntity) Deactivate(context.Context) error { return nil }
func (e *stuckHandleEntity) Handle(context.Context, any) (any, error) {
	<-e.release
	return nil, nil
}

// TestNodeStandalone_WatchdogAbandonsStuckHandle Standalone + 短执行阈值：
// Handle 卡死 → 在途调用方以 Timeout 收场、实体被强弃逐出（粘性黑洞解除）。
func TestNodeStandalone_WatchdogAbandonsStuckHandle(t *testing.T) {
	ent := &stuckHandleEntity{release: make(chan struct{})}
	defer close(ent.release)
	n := mustStartNode(t, Config{
		NodeID:     1,
		Standalone: true,
		Kinds: map[string]rans.Factory{
			"cell": func(rans.ID) rans.Entity { return ent },
		},
		Watchdog: WatchdogConfig{
			Interval:          20 * time.Millisecond,
			ExecTimeout:       60 * time.Millisecond,
			UnloadTimeout:     time.Hour,
			ActivationTimeout: time.Hour,
		},
	})
	defer shutdownNode(n)
	id := mustID(t, "cell", "wd-1")
	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := n.Client().Call(ctx, id, "req")
		errCh <- err
	}()
	var err error
	select {
	case err = <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("call never returned")
	}
	if rans.CodeOf(err) != rans.CodeTimeout {
		t.Fatalf("stuck-handle call err = %v, want CodeTimeout", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := n.mgr.GetLocal(id); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("stuck instance must be force-abandoned out of the local table")
		}
		time.Sleep(2 * time.Millisecond)
	}
}
