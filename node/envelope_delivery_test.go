package node

// 本地 Call 的两处位置透明缺口（票 08 红测）：在箱取消（出队即弃并计数，
// 与远端 MsgCancel 同构）与 ReenterAfter 让出 work 的存活（跑到请求
// deadline，不随发送腿预算 cancel 收场）。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// envProbe 票 08 测试实体的共享观测面（一测一份）。
type envProbe struct {
	blocked chan struct{} // "block" 已占住 loop
	release chan struct{} // 放行 "block"
	gone    chan struct{} // "go" 已执行（在箱取消测试：不应发生）
	outcome chan string   // 让出 work 的收场读数
}

func newEnvProbe() *envProbe {
	return &envProbe{
		blocked: make(chan struct{}, 1),
		release: make(chan struct{}),
		gone:    make(chan struct{}, 1),
		outcome: make(chan string, 1),
	}
}

func (p *envProbe) kinds() map[string]rans.Factory {
	return map[string]rans.Factory{
		"envprobe": func(rans.ID) rans.Entity { return &envEntity{p: p} },
	}
}

type envEntity struct{ p *envProbe }

func (e *envEntity) Activate(context.Context) error   { return nil }
func (e *envEntity) Deactivate(context.Context) error { return nil }

func (e *envEntity) Handle(ctx context.Context, msg any) (any, error) {
	switch msg {
	case "block": // 占住 loop：其后入箱的本地 Call 必在放行后才出队
		e.p.blocked <- struct{}{}
		select {
		case <-e.p.release:
		case <-time.After(5 * time.Second):
		}
		return nil, nil
	case "go": // 在箱取消测试的探针投递：执行即记过
		e.p.gone <- struct{}{}
		return nil, nil
	case "reenter": // 让出重入后立即返回：work 不应随发送腿收场
		rans.ReenterAfter(ctx, func(wctx context.Context) (any, error) {
			select {
			case <-wctx.Done():
				e.p.outcome <- "work-ctx: " + wctx.Err().Error()
			case <-time.After(2 * time.Second):
				e.p.outcome <- "alive-after-2s"
			}
			return nil, nil
		}, nil)
		return "returned", nil
	}
	return nil, nil
}

// metricVal 节点注册表单指标读数（未注册的懒建计数器计 0）。
func metricVal(t *testing.T, n *Node, name string) float64 {
	t.Helper()
	for _, m := range n.Metrics() {
		if m.Name == name {
			return m.Value
		}
	}
	return 0
}

// TestNodeCall_LocalCanceledInBoxNotExecuted 本地 Call 在箱取消：调用方在
// 投递出队前取消 → 出队即弃（零执行）并计 rans.call.canceled——与远端
// MsgCancel 的 CancelAware 出队检查同构。修复前红：本地投递无 CancelAware，
// 取消后照常执行。
func TestNodeCall_LocalCanceledInBoxNotExecuted(t *testing.T) {
	p := newEnvProbe()
	n := mustStartNode(t, Config{
		NodeID: 1, Standalone: true, Kinds: p.kinds(),
		Calls: CallConfig{Default: 5 * time.Second},
	})
	defer shutdownNode(n)
	id := mustID(t, "envprobe", "cancel-in-box")
	// 占住 loop（block 的 Handle 等待放行）。
	go func() { _, _ = n.Client().Call(context.Background(), id, "block") }()
	select {
	case <-p.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("block never started")
	}
	// 调用方 ctx 无 deadline：预算由缺省合成（派生 ctx——发送腿返回即触发
	// 预算 cancel）。入箱后、出队前取消。
	cctx, cancel := context.WithCancel(context.Background())
	callDone := make(chan struct{})
	go func() {
		_, _ = n.Client().Call(cctx, id, "go")
		close(callDone)
	}()
	time.Sleep(200 * time.Millisecond) // 等激活与投递入箱（loop 被 block 占住，必在箱）
	cancel()
	close(p.release)
	select {
	case <-callDone:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled call never returned")
	}
	select {
	case <-p.gone:
		t.Fatal("canceled-in-box call was executed (must be discarded at dequeue)")
	default:
	}
	if got := metricVal(t, n, "rans.call.canceled"); got < 1 {
		t.Fatalf("rans.call.canceled = %v, want >= 1", got)
	}
}

// TestNodeCall_LocalFireAndForgetReenterWorkSurvivesReturn 本地 Call 内
// ReenterAfter（不 Defer）的 work：调用返回（发送腿预算 cancel 触发）后
// work ctx 仍活到请求 deadline——与远端/本地 Tell 同构。修复前红：wctx 挂
// 发送腿链，调用返回即取消。
func TestNodeCall_LocalFireAndForgetReenterWorkSurvivesReturn(t *testing.T) {
	p := newEnvProbe()
	n := mustStartNode(t, Config{
		NodeID: 1, Standalone: true, Kinds: p.kinds(),
		Calls: CallConfig{Default: 5 * time.Second},
	})
	defer shutdownNode(n)
	id := mustID(t, "envprobe", "reenter-survive")
	v, err := n.Client().Call(context.Background(), id, "reenter")
	if err != nil || v != "returned" {
		t.Fatalf("call reenter: v=%v err=%v", v, err)
	}
	select {
	case out := <-p.outcome:
		if out != "alive-after-2s" {
			t.Fatalf("reenter work ctx ended early: %s (want alive until request deadline)", out)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("reenter work never reported")
	}
}
