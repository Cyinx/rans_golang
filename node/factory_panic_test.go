package node

// 用户工厂 panic 收敛（位置透明）：远端读循环上的激活与本地顶层调用一律回
// CodeError+ActivationFailed——不可重试（工厂恰调一次）、不崩进程。

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/testhooks"
)

// whereEntity 回显所在节点号（判读调用确实落在预期节点上执行）。
type whereEntity struct{ node rans.NodeID }

func (e *whereEntity) Activate(context.Context) error   { return nil }
func (e *whereEntity) Deactivate(context.Context) error { return nil }
func (e *whereEntity) Handle(context.Context, any) (any, error) {
	return fmt.Sprintf("ok@%d", e.node), nil
}

// boomKinds 工厂对 key "bad" panic（bad 计调用次数，跨节点共享），其余 key
// 构造 whereEntity。
func boomKinds(node rans.NodeID, bad *atomic.Int32) map[string]rans.Factory {
	return map[string]rans.Factory{
		"boom": func(id rans.ID) rans.Entity {
			if id.Key == "bad" {
				bad.Add(1)
				panic("factory panics for key bad")
			}
			return &whereEntity{node: node}
		},
	}
}

// assertFactoryPanicErr 工厂 panic 的调用方错误形态：*rans.Error 且
// CodeError+ActivationFailed（不可重试），产生节点为 from（0 = 本地）。
func assertFactoryPanicErr(t *testing.T, err error, from rans.NodeID) {
	t.Helper()
	var re *rans.Error
	if !errors.As(err, &re) || re.Code != rans.CodeError ||
		re.Reason != rans.ReasonActivationFailed || re.Node != from {
		t.Fatalf("err = %v, want *rans.Error CodeError+ActivationFailed from node %d", err, from)
	}
}

// TestNodeFactoryPanic_RemoteCallRejectedOnce 目录预置 boom/bad、boom/good →
// node2：node1 的调用远发、node2 在传输读循环上激活。工厂 panic 收敛为 node2
// 产生的不可重试回绝（分发器激活重试环与调用方重试都不命中——工厂恰调一次）；
// node2 存活：其后对正常 key 的调用在 node2 上执行成功。
func TestNodeFactoryPanic_RemoteCallRejectedOnce(t *testing.T) {
	dir := inmem.New()
	var addrs sync.Map // rans.NodeID → 监听地址（进程内两节点共享解析表）
	resolve := func(_ context.Context, n rans.NodeID) (string, error) {
		if a, ok := addrs.Load(n); ok {
			return a.(string), nil
		}
		return "", fmt.Errorf("no address for node %d", n)
	}
	var bad atomic.Int32
	start := func(id rans.NodeID) *Node {
		n := mustStartNode(t, Config{
			NodeID: id, Kinds: boomKinds(id, &bad),
			Security:  SecurityConfig{Insecure: true},
			Directory: DirectoryConfig{Injected: dir},
			TestHooks: &testhooks.Hooks{Resolve: resolve},
		})
		addrs.Store(id, n.Addr())
		t.Cleanup(func() { shutdownNode(n) })
		return n
	}
	n1 := start(1)
	start(2)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	badID, goodID := mustID(t, "boom", "bad"), mustID(t, "boom", "good")
	for _, id := range []rans.ID{badID, goodID} {
		if _, _, err := dir.Register(ctx, id, directory.Owner{Node: 2}, nil); err != nil {
			t.Fatal(err)
		}
	}

	_, err := n1.Client().Call(ctx, badID, "x")
	assertFactoryPanicErr(t, err, 2)
	if v, err := n1.Client().Call(ctx, goodID, "x"); err != nil || v != "ok@2" {
		t.Fatalf("follow-up call via node2 = (%v, %v), want (ok@2, nil): node2 must survive", v, err)
	}
	if got := bad.Load(); got != 1 {
		t.Fatalf("factory ran %d times for the bad key, want exactly 1 (non-retryable)", got)
	}
}

// TestNodeFactoryPanic_LocalCallReturnsError Standalone 本地顶层 Call：工厂
// panic 不得直达调用方 goroutine——返回同一不可重试错误（本地「激活失败重试
// 一次」不命中，工厂恰调一次），其后正常 key 照常激活。
func TestNodeFactoryPanic_LocalCallReturnsError(t *testing.T) {
	var bad atomic.Int32
	n := mustStartNode(t, Config{NodeID: 1, Standalone: true, Kinds: boomKinds(1, &bad)})
	defer shutdownNode(n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("local Call panicked: %v", r)
			}
		}()
		_, err = n.Client().Call(ctx, mustID(t, "boom", "bad"), "x")
		return err
	}()
	assertFactoryPanicErr(t, err, 0)
	if v, err := n.Client().Call(ctx, mustID(t, "boom", "good"), "x"); err != nil || v != "ok@1" {
		t.Fatalf("follow-up local call = (%v, %v), want (ok@1, nil)", v, err)
	}
	if got := bad.Load(); got != 1 {
		t.Fatalf("factory ran %d times for the bad key, want exactly 1 (non-retryable)", got)
	}
}
