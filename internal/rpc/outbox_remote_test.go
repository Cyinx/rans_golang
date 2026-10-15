package rpc

// 发件箱装配层行为锁（H11 决策 11/12 Tell 腿 + 停序③出站 flush 接线）：
// 复用 remote_test.go 的回环双节点基座（startRemNode/resolveTable），时序
// 断言走 channel。发件箱本体（目标 FIFO/满箱阻塞/熔断/指标）见 outbox_test.go。

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// 1. Tell 腿走发件箱（入箱即返回，worker 异步送达）：显式单向发送经路由
// 梯子入箱，对端实体照常执行——装配 rewiring 不破坏端到端投递。
func TestRemoteTellFlowsThroughOutbox(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	handled := make(chan int, 4)
	nodeA := startRemNode(t, dir, rt, nodeSpec{id: 1})
	nodeB := startRemNode(t, dir, rt, nodeSpec{
		id: 2,
		handle: func(_ context.Context, req any) (any, error) {
			handled <- req.(int)
			return nil, nil
		},
	})
	rt.set(map[rans.NodeID]string{1: nodeA.rem.Addr(), 2: nodeB.rem.Addr()})
	ctx := context.Background()

	id := mustUUID(t, "tell-ob")
	if _, _, err := dir.Register(ctx, id, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatal(err)
	}
	if err := nodeA.rem.SendOneWay(ctx, id, 42); err != nil {
		t.Fatalf("tell: %v", err)
	}
	select {
	case v := <-handled:
		if v != 42 {
			t.Fatalf("tell delivered %v, want 42", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("tell enqueued to outbox was never delivered to the peer entity")
	}
}

// 2. 停序③出站 flush 等发件箱排空：对端拨号恒挂起（net.Pipe 半端——握手
// 永不完成，首帧卡在发送段、余帧留箱），Shutdown 的 flush 必须等箱空或
// 窗口耗尽——断言停机不即时返回（flush 在非空箱上等待至有界窗口边界），
// 且最终有界收场（阶段④照常关门）。
func TestRemoteDrainFlushWaitsOutbox(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	hangDial := func(context.Context, string) (net.Conn, error) {
		c, _ := net.Pipe() // 两端无人读：握手写阻塞，连接永不就绪
		return c, nil
	}
	nodeA := startRemNode(t, dir, rt, nodeSpec{id: 1, dial: hangDial})
	rt.set(map[rans.NodeID]string{1: nodeA.rem.Addr(), 7: "127.0.0.1:1"})
	ctx := context.Background()

	id := mustUUID(t, "flush-ob")
	if _, _, err := dir.Register(ctx, id, directory.Owner{Node: 7}, nil); err != nil {
		t.Fatal(err)
	}
	// 两帧入箱：首帧被 worker 取走卡在就绪等待，次帧留箱（深度 ≥ 1）。
	for i := 0; i < 2; i++ {
		if err := nodeA.rem.SendOneWay(ctx, id, i); err != nil {
			t.Fatalf("tell: %v", err)
		}
	}

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- nodeA.rem.Shutdown(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
		if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
			t.Fatalf("shutdown returned in %v; flush must wait for the outbox (window 500ms) before closing", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown never returned: flush must stay bounded by the flush window")
	}
}

// 3. 跨节点 Tell→Call 保序（C1 锚）：同一调用方先 Tell 后 Call 同一实体，
// Call 应答必须看见此前全部 Tell 的写入（read-your-writes）。修复前 Call
// 请求帧同步直发、常超车仍在发件箱里的 Tell（帧序即执行序），应答读到
// 空或部分历史——本断言按全量可见裁决。
func TestRemoteTellThenCallOrdering_CrossNode(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	var hist []string
	nodeA := startRemNode(t, dir, rt, nodeSpec{id: 1})
	nodeB := startRemNode(t, dir, rt, nodeSpec{
		id: 2,
		handle: func(_ context.Context, req any) (any, error) {
			s := req.(string)
			if s == "history" {
				return strings.Join(hist, ","), nil
			}
			hist = append(hist, s) // 实体 loop 串行执行，无锁安全
			return nil, nil
		},
	})
	rt.set(map[rans.NodeID]string{1: nodeA.rem.Addr(), 2: nodeB.rem.Addr()})
	ctx := context.Background()

	id := mustUUID(t, "tell-call-order")
	if _, _, err := dir.Register(ctx, id, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatal(err)
	}
	if err := nodeA.rem.SendOneWay(ctx, id, "hello"); err != nil {
		t.Fatalf("tell hello: %v", err)
	}
	if err := nodeA.rem.SendOneWay(ctx, id, "world"); err != nil {
		t.Fatalf("tell world: %v", err)
	}
	v, err := nodeA.rem.Call(ctx, id, "history")
	if err != nil {
		t.Fatalf("call history: %v", err)
	}
	if got := v.(string); got != "hello,world" {
		t.Fatalf("history = %q, want %q (earlier tells must be visible to the follow-up call)", got, "hello,world")
	}
}

// 4. 跨目标并行不退化（守护锚，票 04 按目标分队列）：一个目标的发送腿
// 阻塞（resolve 恒失败，帧卡在发送重试段）不得拖慢其他目标的 Tell→Call
// ——Call 请求帧按目标隔离入队。
func TestRemoteTellThenCallOrdering_MultiTarget(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	var hist []string
	nodeA := startRemNode(t, dir, rt, nodeSpec{id: 1})
	nodeB := startRemNode(t, dir, rt, nodeSpec{
		id: 2,
		handle: func(_ context.Context, req any) (any, error) {
			s := req.(string)
			if s == "history" {
				return strings.Join(hist, ","), nil
			}
			hist = append(hist, s)
			return nil, nil
		},
	})
	// jam 目标不进地址表：resolve 恒失败，其专属 worker 在发送重试段空转
	//（票 04 后按目标隔离——与节点 2 天然互不阻塞）。
	jam := rans.NodeID(3)
	rt.set(map[rans.NodeID]string{1: nodeA.rem.Addr(), 2: nodeB.rem.Addr()})
	ctx := context.Background()

	jamID := mustUUID(t, "tell-call-jam")
	if _, _, err := dir.Register(ctx, jamID, directory.Owner{Node: jam}, nil); err != nil {
		t.Fatal(err)
	}
	if err := nodeA.rem.SendOneWay(ctx, jamID, "stuck"); err != nil {
		t.Fatalf("tell to jam target: %v", err)
	}

	id := mustUUID(t, "tell-call-multi")
	if _, _, err := dir.Register(ctx, id, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatal(err)
	}
	if err := nodeA.rem.SendOneWay(ctx, id, "ping"); err != nil {
		t.Fatalf("tell ping: %v", err)
	}
	v, err := nodeA.rem.Call(ctx, id, "history")
	if err != nil {
		t.Fatalf("call history with a jammed foreign shard: %v", err)
	}
	if got := v.(string); got != "ping" {
		t.Fatalf("history = %q, want %q", got, "ping")
	}
}

// 5.（票 04 删除）原「同分片队头阻塞下排队过预算的 Call 按预算收场」的
// 分片前提随按目标分队列消亡——同目标排队形态由包内单测
// TestOutbox_CallFrameExpiredBudgetAbortsAtSend 钉住，端到端「排队过预算
// 零执行」由票 05 的 testkit 红测接管。
