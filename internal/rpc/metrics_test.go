package rpc

// Remote 指标接线测试（构造点注入）：Config.Metrics 注册表
// 上的五项仪表——pending 反映在途请求、queue_depth 反映本地实体数、
// batch_size 反映最近写批；收场后归零。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/metrics"
)

// pollGauge 有界轮询某注册表上的 gauge 读数（异步收敛点——在途请求登记、
// 远端激活都是异步面，channel 无法直接桥接注册表读数）。
func pollGauge(t *testing.T, d time.Duration, reg *metrics.Registry, name string, want float64) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if got := reg.Snapshot()[name]; got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gauge %s = %v, want %v (polled %s)", name, reg.Snapshot()[name], want, d)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestRemoteMetricsGauges 双节点回环：node1 先激活实体并阻塞执行（占住
// owner 位）→ node2 在途 Call（pending=1 on node2 / queue_depth=1 on node1）
// → 放行收场（双 Call 收口、pending=0）；请求帧写出使 node2 的
// batch_size ≥ 1。各节点独立注册表，读数互不串味。
func TestRemoteMetricsGauges(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	reg1 := metrics.New("test")
	reg2 := metrics.New("test")

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id: 1,
		handle: func(ctx context.Context, req any) (any, error) {
			entered <- struct{}{}
			<-release
			return 1, nil
		},
		metrics: reg1,
	})
	node2 := startRemNode(t, dir, rt, nodeSpec{
		id:      2,
		handle:  func(ctx context.Context, req any) (any, error) { return -1, nil },
		metrics: reg2,
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})

	id := mustUUID(t, "metrics-1")
	ctx := context.Background()
	type callResult struct {
		v   any
		err error
	}
	// node1 首调：目录 miss → 本地激活（owner=1），实体阻塞在执行位。
	done1 := make(chan callResult, 1)
	go func() {
		v, err := node1.rem.Call(ctx, id, 0)
		done1 <- callResult{v, err}
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("node1 entity handle not entered within 2s")
	}

	// node2 调同 id：目录命中 node1 → 远程路径，pending 表出现 1 条在途。
	done2 := make(chan callResult, 1)
	go func() {
		v, err := node2.rem.Call(ctx, id, 0)
		done2 <- callResult{v, err}
	}()
	pollGauge(t, 2*time.Second, reg2, metrics.NameGatewayPending, 1)
	if got := reg1.Snapshot()[metrics.NameQueueDepth]; got != 1 {
		t.Fatalf("node1 queue_depth gauge = %v, want 1 (activated entity)", got)
	}
	if got := reg2.Snapshot()[metrics.NameQueueDepth]; got != 0 {
		t.Fatalf("node2 queue_depth gauge = %v, want 0 (entity lives on node1)", got)
	}

	// 放行：两个 Call 都从 node1 的同一串行实体上依次收口。
	close(release)
	for i, done := range []chan callResult{done1, done2} {
		select {
		case r := <-done:
			if r.err != nil || r.v != 1 {
				t.Fatalf("call %d result = (%v, %v), want (1, nil)", i+1, r.v, r.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("call %d did not return after release", i+1)
		}
	}
	pollGauge(t, 2*time.Second, reg2, metrics.NameGatewayPending, 0)
	if got := reg2.Snapshot()[metrics.NameTransportBatchSize]; got < 1 {
		t.Fatalf("node2 batch_size gauge = %v, want >= 1 after request frames flushed", got)
	}
}
