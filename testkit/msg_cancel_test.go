package testkit

// MsgCancel 尽力取消的端到端验收（H10 决策 9，缝合点 rans.Client.Call +
// Node.Metrics()）：调用方 ctx 主动取消（非预算到期）→ 本端网关向 pending
// 目标节点发 MsgCancel → 收端定位请求：执行中 cancel 其 Handle ctx、在箱
// 标记后出队即弃（计 rans.call.canceled）。链式传播不逐跳发消息——被调方
// Handle ctx 取消使其嵌套出站调用随之取消，对端同理自然收敛。红判据：无
// 取消消息时远端 Handle 睡满过线预算（8s），取消后须 <2s 收场。

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// cancelProbe 取消观测实体：req 非 nil 的调用阻塞于 ctx.Done 与 5s 定时器
// 的竞速（取消语义的观测前提——ctx 不取消则睡满 5s），nil 请求快路径应答
// （预热放置用）。收场记录 ctx.Err() 文本供断言（实体可观测到自己被取消）。
type cancelProbe struct {
	startOnce sync.Once
	doneOnce  sync.Once
	started   chan struct{}
	done      chan struct{}
	mu        sync.Mutex
	observed  string // Handle 收场时的 ctx.Err() 文本；"" = 定时器先到
}

func (p *cancelProbe) Handle(ctx context.Context, req any) (any, error) {
	if req == nil {
		return "ok", nil
	}
	p.startOnce.Do(func() { close(p.started) })
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		p.mu.Lock()
		p.observed = ctx.Err().Error()
		p.mu.Unlock()
		p.doneOnce.Do(func() { close(p.done) })
		return nil, ctx.Err()
	case <-timer.C:
		p.doneOnce.Do(func() { close(p.done) })
		return "ok", nil
	}
}

func (p *cancelProbe) Activate(context.Context) error   { return nil }
func (p *cancelProbe) Deactivate(context.Context) error { return nil }

func (p *cancelProbe) observedErr() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.observed
}

// chainedBranch 链式中游实体：req 非 nil 时经 ctx 调用面对下游 target 发起
// 嵌套调用（链式取消的传播媒介——中游 Handle ctx 取消即嵌套调用 ctx 取消），
// 收场记录自身 ctx 的取消观测。
type chainedBranch struct {
	startOnce sync.Once
	doneOnce  sync.Once
	started   chan struct{}
	done      chan struct{}
	target    rans.ID
	mu        sync.Mutex
	observed  string
}

func (b *chainedBranch) Handle(ctx context.Context, req any) (any, error) {
	if req == nil {
		return "ok", nil
	}
	b.startOnce.Do(func() { close(b.started) })
	_, err := rans.ClientFrom(ctx).Call(ctx, b.target, "go")
	b.mu.Lock()
	if ctx.Err() != nil {
		b.observed = ctx.Err().Error()
	}
	b.mu.Unlock()
	b.doneOnce.Do(func() { close(b.done) })
	if err != nil {
		return nil, err
	}
	return "ok", nil
}

func (b *chainedBranch) Activate(context.Context) error   { return nil }
func (b *chainedBranch) Deactivate(context.Context) error { return nil }

func (b *chainedBranch) observedErr() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.observed
}

// waitCallerCanceled 等取消后的调用方收场：限时断言 Canceled 收场与耗时。
func waitCallerCanceled(t *testing.T, ch chan error, cancelAt time.Time) {
	t.Helper()
	select {
	case err := <-ch:
		if elapsed := time.Since(cancelAt); elapsed > time.Second {
			t.Fatalf("caller returned %s after cancel, want immediate", elapsed)
		}
		if !errors.Is(err, rans.ErrCanceled) {
			t.Fatalf("err = %v, want rans.ErrCanceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("caller did not return after cancel")
	}
}

// TestMsgCancel_RemoteHandleCancelled 远端执行中取消（H10 决策 9）：双节点，
// 实体 Handle 睡 5s（select ctx.Done 可中断形态），调用方 300ms 后主动
// cancel → 远端 Handle 在 <2s 内收场（而非等到过线预算耗尽）且观测到自身
// ctx 被取消；调用方即时按 Canceled 收场。
func TestMsgCancel_RemoteHandleCancelled(t *testing.T) {
	p := &cancelProbe{started: make(chan struct{}), done: make(chan struct{})}
	c := NewCluster(t, 2, map[string]rans.Factory{
		"probe": func(_ rans.ID) rans.Entity { return p },
	})
	id := mustID(t, "probe", "msg-cancel-remote")
	// 预热：节点 1 本地激活（probe 落节点 2），节点 0 的调用经目录走远端腿。
	if _, err := c.Node(1).Call(context.Background(), id, nil); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outcome := make(chan error, 1)
	go func() {
		_, err := c.Node(0).CallOpts(ctx, id, "slow", rans.Timeout(8*time.Second))
		outcome <- err
	}()
	<-p.started // 远端 Handle 已在途执行
	time.Sleep(300 * time.Millisecond)
	cancelAt := time.Now()
	cancel()
	waitCallerCanceled(t, outcome, cancelAt)

	select {
	case <-p.done:
	case <-time.After(2 * time.Second):
		t.Fatal("remote handle did not return after caller cancel (cancel not propagated)")
	}
	if obs := p.observedErr(); obs == "" {
		t.Fatal("remote handle finished without observing ctx cancellation")
	}
}

// TestMsgCancel_Chained 链式收敛（H10 决策 9）：A（节点 1）调 B（节点 2），
// B 的 Handle 内再调 C（节点 3）；A 取消 → B、C 的 Handle 都快速取消收敛
// ——不逐跳发消息，中游 Handle ctx 取消使其嵌套出站调用随之取消，下游
// 经同一 MsgCancel 机制收场。
func TestMsgCancel_Chained(t *testing.T) {
	cid := mustID(t, "leaf", "chain-c")
	leaf := &cancelProbe{started: make(chan struct{}), done: make(chan struct{})}
	mid := &chainedBranch{started: make(chan struct{}), done: make(chan struct{}), target: cid}
	c := NewCluster(t, 3, map[string]rans.Factory{
		"branch": func(_ rans.ID) rans.Entity { return mid },
		"leaf":   func(_ rans.ID) rans.Entity { return leaf },
	})
	bid := mustID(t, "branch", "chain-b")
	// 预热：branch 落节点 1（NodeID 2）、leaf 落节点 2（NodeID 3）。
	if _, err := c.Node(1).Call(context.Background(), bid, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Node(2).Call(context.Background(), cid, nil); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outcome := make(chan error, 1)
	go func() {
		_, err := c.Node(0).CallOpts(ctx, bid, "go", rans.Timeout(8*time.Second))
		outcome <- err
	}()
	<-mid.started // 中游已在途：嵌套调用 leaf 中
	<-leaf.started
	time.Sleep(300 * time.Millisecond)
	cancelAt := time.Now()
	cancel()
	waitCallerCanceled(t, outcome, cancelAt)

	for _, w := range []struct {
		name string
		done chan struct{}
	}{{"branch", mid.done}, {"leaf", leaf.done}} {
		select {
		case <-w.done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s handle did not converge after cancel", w.name)
		}
	}
	if mo, lo := mid.observedErr(), leaf.observedErr(); mo == "" || lo == "" {
		t.Fatalf("cancellation not observed: branch=%q leaf=%q, want both", mo, lo)
	}
}

// TestMsgCancel_InBoxDroppedCounts 在箱弃置计数（H10 决策 9）：长 Handle
// 占住 loop 期间远端请求入箱，调用方取消 → 在箱标记 → 出队即弃——实体
// 零副作用、调用方按 Canceled 收场、rans.call.canceled 计 ≥1（S6 指标面，
// 与 expired 分列）。
func TestMsgCancel_InBoxDroppedCounts(t *testing.T) {
	p := &expiryProbe{started: make(chan struct{}), release: make(chan struct{})}
	c := NewCluster(t, 2, expiryProbeKinds(p))
	id := mustID(t, "probe", "msg-cancel-inbox")

	// 占住 loop：长 Handle 阻塞期间后续请求入箱（首个调用本地激活在节点 1）。
	callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()
	firstErr := make(chan error, 1)
	go func() {
		_, err := c.Node(1).Call(callCtx, id, nil)
		firstErr <- err
	}()
	<-p.started

	// 远端调用入箱（排在长 Handle 之后）：等在箱深度 ≥1 再取消——MsgCancel
	// 到达时请求必已入箱（同连接 TCP FIFO 保证取消晚于请求帧）。
	cancelCtx, cancelCall2 := context.WithCancel(context.Background())
	defer cancelCall2()
	secondErr := make(chan error, 1)
	go func() {
		_, err := c.Node(0).Call(cancelCtx, id, nil)
		secondErr <- err
	}()
	pollNodeMetric(t, c, 1, "rans.pawnite.inbox_depth_total", 1, 2*time.Second)
	cancelAt := time.Now()
	cancelCall2()
	waitCallerCanceled(t, secondErr, cancelAt)
	// 留出取消通知落地窗：MsgCancel 异步尽力送达（同连接 FIFO，回环亚毫秒
	// 级），释放点必须晚于在箱标记——否则出队竞速取消通知，请求照常执行
	//（尽力语义的正确形态，非缺陷）。500ms ≈ 回环送达的数百倍裕度。
	time.Sleep(500 * time.Millisecond)

	// 释放长 Handle：被取消的在箱请求出队即弃（不执行）并计数。
	close(p.release)
	if err := <-firstErr; err != nil {
		t.Fatalf("occupying call: %v", err)
	}
	if n := p.executed(); n != 1 {
		t.Fatalf("entity executed %d times, want 1 (canceled request must not execute)", n)
	}
	pollNodeMetric(t, c, 1, "rans.call.canceled", 1, 2*time.Second)
}

// pollNodeMetric 轮询节点指标值到达下界（S6 指标面的观测口；出队弃置发生
// 在实体 loop 上，计数落地相对释放点有调度延迟）。
func pollNodeMetric(t *testing.T, c *Cluster, node int, name string, wantMin float64, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		var v float64
		for _, m := range c.Node(node).Node.Metrics() {
			if m.Name == name {
				v = m.Value
			}
		}
		if v >= wantMin {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("metric %s on node %d = %v, want >= %v", name, node, v, wantMin)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
