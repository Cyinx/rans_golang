package testkit

// Cluster.Shutdown 优雅关停的行为锁——逆序 Drain（末节点先排空）、全节点
// 本地实体清空、返回聚合错误面。时序断言走 channel 握手。

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// seqGateDeactEntity 激活序号实体：Deactivate 上报自己的激活序号并停在独立
// gate 上（Shutdown 逆序排空的行为锁：首个上报必须是末节点的实体——工厂
// 无 id 参数，激活序即「第 i 个被创建」，测试按节点序逐个本地激活使两者
// 对齐）。
type seqGateDeactEntity struct {
	seq     int
	entered chan<- int
	gate    chan struct{}
}

func (e *seqGateDeactEntity) Activate(context.Context) error { return nil }

func (e *seqGateDeactEntity) Handle(_ context.Context, req any) (any, error) {
	return req, nil
}

func (e *seqGateDeactEntity) Deactivate(context.Context) error {
	e.entered <- e.seq
	<-e.gate
	return nil
}

func TestClusterShutdownDrainsReverseOrder(t *testing.T) {
	entered := make(chan int, 4)
	var seq atomic.Int32
	gates := make([]chan struct{}, 2)
	kinds := map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity {
			i := int(seq.Add(1)) - 1
			g := make(chan struct{})
			gates[i] = g // 工厂在 Call 的同步激活路径上执行，无并发写
			return &seqGateDeactEntity{seq: i, entered: entered, gate: g}
		},
	}
	c := NewCluster(t, 2, kinds)
	ctx := context.Background()

	// 每节点本地激活一个实体（目录 miss、无放置 → 本地）：激活序 0=节点1、
	// 1=节点2
	for i := 0; i < 2; i++ {
		id, err := rans.NewID("cell", fmt.Sprintf("sd-%d", i+1))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Node(i).Call(ctx, id, 7); err != nil {
			t.Fatalf("node %d local call: %v", i+1, err)
		}
		if got := len(c.Node(i).LocalIDs()); got != 1 {
			t.Fatalf("node %d must hold 1 local entity, got %d", i+1, got)
		}
	}

	done := make(chan error, 1)
	go func() { done <- c.Shutdown(ctx) }()

	// 首个销毁必须是末节点（节点 2）的实体：Shutdown 串行 Drain，同一时刻
	// 只有一个节点在排空，首个 Deactivate 信号即首个被排空的节点。
	select {
	case first := <-entered:
		if first != 1 {
			t.Fatalf("shutdown must drain the last node first, first destroyed entity seq=%d (node %d), want seq=1 (node 2)",
				first, first+1)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown destroyed no entity")
	}
	close(gates[1]) // 放行节点 2 的销毁 → 节点 2 Drain 收尾 → 轮到节点 1
	select {
	case second := <-entered:
		if second != 0 {
			t.Fatalf("second destroyed entity seq=%d, want 0 (node 1)", second)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("node 1 was not drained after node 2")
	}
	close(gates[0])

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not return")
	}
	for i := 0; i < 2; i++ {
		if got := len(c.Node(i).LocalIDs()); got != 0 {
			t.Fatalf("node %d still holds %d entities after shutdown", i+1, got)
		}
	}
	// Shutdown 后 NewCluster 的 t.Cleanup 再走 Stop：幂等无害（不 panic 即过）
}
