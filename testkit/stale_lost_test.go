package testkit

// 陈旧即失主故障面（R11/B9）：周期 checkpoint 落库被拒为 stale → 围栏并
// 止损销毁本地实例；Retire 遇 stale 不重试；所有权计数器落在节点注册表。

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
)

// snapCounter 有状态计数实体（checkpoint 候选面：快照承载计数值）。
type snapCounter struct{ n int }

func (e *snapCounter) Activate(context.Context) error   { return nil }
func (e *snapCounter) Deactivate(context.Context) error { return nil }
func (e *snapCounter) Handle(_ context.Context, _ any) (any, error) {
	e.n++
	return e.n, nil
}
func (e *snapCounter) Snapshot() ([]byte, error) { return []byte(strconv.Itoa(e.n)), nil }
func (e *snapCounter) Restore(b []byte) error {
	n, err := strconv.Atoi(string(b))
	if err != nil {
		return fmt.Errorf("snapCounter restore: %w", err)
	}
	e.n = n
	return nil
}

func snapKinds() map[string]rans.Factory {
	return map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &snapCounter{} },
	}
}

// nodeMetricVal 节点注册表单指标读数（未注册的懒建计数器计 0——首次使用
// 才落表是注入面的既定形态）。
func nodeMetricVal(t *testing.T, n *Node, name string) float64 {
	t.Helper()
	for _, m := range n.Node.Metrics() {
		if m.Name == name {
			return m.Value
		}
	}
	return 0
}

// TestFault_StaleCheckpointFencesLocalInstance 周期 checkpoint Put 被拒为
// stale（测试直接抬 Store 水位模拟新化身落库）→ 本地实例秒级围栏销毁 +
// rans.ownership.stale_fenced ≥1。修复前实例永驻（retry next sweep）。
func TestFault_StaleCheckpointFencesLocalInstance(t *testing.T) {
	store := checkpoint.New()
	c := NewCluster(t, 1, snapKinds(),
		WithCheckpointStore(store), WithCheckpointInterval(100*time.Millisecond))
	ctx := context.Background()
	id := mustID(t, "counter", "stale-ckpt")
	if v, err := c.Node(0).Call(ctx, id, 1); err != nil || v != 1 {
		t.Fatalf("activation call: v=%v err=%v", v, err)
	}
	if !c.Node(0).Holds(id) {
		t.Fatal("precondition: node must hold the entity")
	}
	if err := store.Put(ctx, id, 1000, []byte("newer-incarnation")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for c.Node(0).Holds(id) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if c.Node(0).Holds(id) {
		t.Fatal("stale checkpoint must fence and drop the local instance within seconds")
	}
	if got := nodeMetricVal(t, c.Node(0), "rans.ownership.stale_fenced"); got < 1 {
		t.Fatalf("rans.ownership.stale_fenced = %v, want >= 1", got)
	}
}

// TestFault_RetireStaleIncarnationNoRetry 抬高水位后 Destroy：Retire 终
// 快照 Put 尝试恰 1 次（stale 即失主，重试必败——修复前 3 次）。
func TestFault_RetireStaleIncarnationNoRetry(t *testing.T) {
	base := checkpoint.New()
	fs := NewFaultStore(base)
	c := NewCluster(t, 1, snapKinds(), WithCheckpointStore(fs))
	ctx := context.Background()
	id := mustID(t, "counter", "stale-retire")
	if v, err := c.Node(0).Call(ctx, id, 1); err != nil || v != 1 {
		t.Fatalf("activation call: v=%v err=%v", v, err)
	}
	if err := base.Put(ctx, id, 1000, []byte("newer-incarnation")); err != nil {
		t.Fatal(err)
	}
	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()
	if err := c.Node(0).Destroy(dctx, id); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	attempts, _ := fs.PutStats()
	if attempts != 1 {
		t.Fatalf("retire Put attempts = %d, want exactly 1 (stale means lost, no retry)", attempts)
	}
}
