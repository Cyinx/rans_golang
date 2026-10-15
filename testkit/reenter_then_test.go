package testkit

// 票 09 红测：ReenterAfter 续体 then 的 ctx——远端投递下可用（携带调用面/
// 传播值/请求 deadline，修复前续体只能复用已取消的捕获 Handle ctx）；且无
// 延迟应答登记点（then 内 rans.Defer 必 panic，不得武装原请求的迟到应答）。

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// thenProbeEntity 让出续体探针：yield-then-call 让出后由 then 用续体 ctx 对
// 远端对端发起调用；yield-then-defer 在 then 内试 Defer（预期 panic）。
type thenProbeEntity struct {
	peer   rans.ID
	report chan string
}

func (e *thenProbeEntity) Activate(context.Context) error   { return nil }
func (e *thenProbeEntity) Deactivate(context.Context) error { return nil }

func (e *thenProbeEntity) Handle(ctx context.Context, req any) (any, error) {
	switch cmd, _ := req.(string); cmd {
	case "yield-then-call":
		rans.ReenterAfter(ctx, func(wctx context.Context) (any, error) {
			return nil, nil
		}, func(tctx context.Context, v any, err error) {
			v2, err2 := rans.ClientFrom(tctx).Call(tctx, e.peer, "ping")
			e.report <- fmt.Sprintf("then-call: v=%v err=%v", v2, err2)
		})
		return "yielded", nil
	case "yield-then-defer":
		rans.ReenterAfter(ctx, func(wctx context.Context) (any, error) {
			return nil, nil
		}, func(tctx context.Context, v any, err error) {
			defer func() {
				if rec := recover(); rec != nil {
					e.report <- "defer-panicked"
				}
			}()
			rans.Defer(tctx)
			e.report <- "defer-no-panic"
		})
		return "yielded", nil
	case "ping":
		return "pong", nil
	}
	return nil, nil
}

func thenKinds(report chan string) map[string]rans.Factory {
	peer, _ := rans.NewID("thenprobe", "peer")
	return map[string]rans.Factory{
		"thenprobe": func(rans.ID) rans.Entity {
			return &thenProbeEntity{peer: peer, report: report}
		},
	}
}

// TestReenterAfter_ThenCtxUsableForRemoteCall 远端投递（实体在 node2、调用方
// node1）下 then 用其 ctx 对另一远端实体发起 Call 成功——续体 ctx 不得随
// Handle 投递返回而取消（修复前：捕获 ctx 已取消 → context canceled）。
func TestReenterAfter_ThenCtxUsableForRemoteCall(t *testing.T) {
	report := make(chan string, 4)
	c := NewCluster(t, 2, thenKinds(report))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	peer, _ := rans.NewID("thenprobe", "peer")
	yielder, _ := rans.NewID("thenprobe", "yielder")
	// 布局：yielder 落 node2（由 node2 首调激活）、peer 落 node1——then 的
	// 调用是 node2 → node1 的远端调用。
	if _, err := c.Node(1).Call(ctx, yielder, "ping"); err != nil {
		t.Fatalf("pre-activate yielder: %v", err)
	}
	if _, err := c.Node(0).Call(ctx, peer, "ping"); err != nil {
		t.Fatalf("pre-activate peer: %v", err)
	}
	v, err := c.Node(0).Call(ctx, yielder, "yield-then-call")
	if err != nil || v != "yielded" {
		t.Fatalf("yield call: v=%v err=%v", v, err)
	}
	select {
	case s := <-report:
		if s != "then-call: v=pong err=<nil>" {
			t.Fatalf("then remote call outcome = %q, want v=pong err=<nil>", s)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("then never reported")
	}
}

// TestReenterAfter_ThenCtxHasNoDeferPoint 续体 ctx 无延迟应答登记点：then 内
// rans.Defer panic（不得武装原请求的迟到应答）。
func TestReenterAfter_ThenCtxHasNoDeferPoint(t *testing.T) {
	report := make(chan string, 4)
	c := NewCluster(t, 1, thenKinds(report))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, _ := rans.NewID("thenprobe", "yielder")
	if _, err := c.Node(0).Call(ctx, id, "yield-then-defer"); err != nil {
		t.Fatalf("yield call: %v", err)
	}
	select {
	case s := <-report:
		if s != "defer-panicked" {
			t.Fatalf("Defer in then = %q, want defer-panicked", s)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("then never reported")
	}
}
