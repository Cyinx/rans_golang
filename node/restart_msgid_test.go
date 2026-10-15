package node

// MessageID 跨同号重启不复用：网关只按 MessageID 关联应答，同 NodeID 重启
// 后 ID 若从头编号，旧化身在途请求的迟到应答/回绝会命中新化身的无关
// pending 项——错值静默交付与非幂等重放都是零容忍面（移植 eval-rpc 夹具）。

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

// restartAddrs 进程内多节点共享的地址解析表；同号重启覆写旧地址——对端的
// 迟到应答正是经此被路由到新化身（回程按 SourceNodeID 重新解析）。
type restartAddrs struct{ addrs sync.Map }

func (a *restartAddrs) resolve(_ context.Context, n rans.NodeID) (string, error) {
	if v, ok := a.addrs.Load(n); ok {
		return v.(string), nil
	}
	return "", fmt.Errorf("no addr for node %d", n)
}

// slowEntity 预热消息（warm）即回，其余消息延迟后回固定值（延迟期即请求
// 在执行节点在途的窗口）。
type slowEntity struct {
	delay time.Duration
	ret   any
}

func (e *slowEntity) Activate(context.Context) error   { return nil }
func (e *slowEntity) Deactivate(context.Context) error { return nil }
func (e *slowEntity) Handle(_ context.Context, msg any) (any, error) {
	if msg == "warm" {
		return "ok", nil
	}
	time.Sleep(e.delay)
	return e.ret, nil
}

// flakyEntity 激活延迟后失败（让在箱请求按激活失败回绝——确定未执行的
// 形态，回执按 SourceNodeID 寻址回已重启的新化身）。
type flakyEntity struct{ failAfter time.Duration }

func (e *flakyEntity) Activate(context.Context) error {
	time.Sleep(e.failAfter)
	return errors.New("activation failed on purpose")
}
func (e *flakyEntity) Deactivate(context.Context) error { return nil }
func (e *flakyEntity) Handle(context.Context, any) (any, error) {
	return "never", nil
}

// countEntity 非幂等副作用体：每执行一次计数 +1，延迟后回执行序号。
type countEntity struct {
	n     *atomic.Int64
	delay time.Duration
}

func (e *countEntity) Activate(context.Context) error   { return nil }
func (e *countEntity) Deactivate(context.Context) error { return nil }
func (e *countEntity) Handle(_ context.Context, msg any) (any, error) {
	if msg == "warm" {
		return "ok", nil
	}
	v := e.n.Add(1)
	time.Sleep(e.delay)
	return v, nil
}

// startRestartNode 起节点并把监听地址登记进共享解析表（同号重启时覆写）。
func startRestartNode(t *testing.T, dir directory.Directory, addrs *restartAddrs, id rans.NodeID, kinds map[string]rans.Factory) *Node {
	t.Helper()
	n := mustStartNode(t, Config{
		NodeID:     id,
		ListenAddr: "127.0.0.1:0",
		Kinds:      kinds,
		Security:   SecurityConfig{Insecure: true},
		Directory:  DirectoryConfig{Injected: dir},
		Calls:      CallConfig{Default: 15 * time.Second, Max: time.Minute},
		TestHooks:  &testhooks.Hooks{Resolve: addrs.resolve},
	})
	addrs.addrs.Store(id, n.Addr())
	return n
}

// warmActivate 在节点 n 上本地激活 id（miss 即本地激活，目录归属落 n）。
func warmActivate(t *testing.T, n *Node, id rans.ID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := n.Client().Call(ctx, id, "warm"); err != nil {
		t.Fatalf("warm activate %s: %v", id, err)
	}
}

// TestNodeRestart_LateResponseNotMisdelivered 旧化身发往 node2 的慢请求在途
// 时节点崩溃；同号新化身的首个远程请求（目标 node3）不得拿到 node2 的迟到
// 应答——MessageID 起点跨化身不复用。
func TestNodeRestart_LateResponseNotMisdelivered(t *testing.T) {
	dir := inmem.New()
	addrs := &restartAddrs{}
	kinds := map[string]rans.Factory{
		"slow":  func(rans.ID) rans.Entity { return &slowEntity{delay: 1500 * time.Millisecond, ret: "STALE-from-node2"} },
		"fresh": func(rans.ID) rans.Entity { return &slowEntity{delay: 3 * time.Second, ret: "FRESH-from-node3"} },
	}
	n2 := startRestartNode(t, dir, addrs, 2, kinds)
	n3 := startRestartNode(t, dir, addrs, 3, kinds)
	t.Cleanup(func() { shutdownNode(n2); shutdownNode(n3) })
	S, F := mustID(t, "slow", "s1"), mustID(t, "fresh", "f1")
	warmActivate(t, n2, S)
	warmActivate(t, n3, F)

	n1a := startRestartNode(t, dir, addrs, 1, kinds)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = n1a.Client().Call(ctx, S, "go") // 旧化身首个远程请求
	}()
	time.Sleep(300 * time.Millisecond) // 请求已在 node2 执行中
	_ = n1a.Close()                    // 旧化身崩溃（立即路径）

	n1b := startRestartNode(t, dir, addrs, 1, kinds) // 同号重启
	t.Cleanup(func() { shutdownNode(n1b) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	v, err := n1b.Client().Call(ctx, F, "go") // 新化身首个远程请求，目标 node3
	if err != nil || v != "FRESH-from-node3" {
		t.Fatalf("new incarnation call = (%v, %v), want (FRESH-from-node3, nil): late response from the dead incarnation must not be delivered", v, err)
	}
}

// TestNodeRestart_LateUnavailableDoesNotReplayCall 旧化身发往 node2 的请求在
// 激活窗入箱后节点崩溃；node2 激活失败回绝时，回执命中同号新化身在途的
// 非幂等调用并触发无条件重放（实体执行两次）——不得发生。
func TestNodeRestart_LateUnavailableDoesNotReplayCall(t *testing.T) {
	dir := inmem.New()
	addrs := &restartAddrs{}
	var incr atomic.Int64
	kinds := map[string]rans.Factory{
		"acct": func(rans.ID) rans.Entity { return &countEntity{n: &incr, delay: 2500 * time.Millisecond} },
		"flaky": func(rans.ID) rans.Entity {
			return &flakyEntity{failAfter: 1500 * time.Millisecond}
		},
	}
	n2 := startRestartNode(t, dir, addrs, 2, kinds)
	n3 := startRestartNode(t, dir, addrs, 3, kinds)
	t.Cleanup(func() { shutdownNode(n2); shutdownNode(n3) })
	A, X := mustID(t, "acct", "a1"), mustID(t, "flaky", "x1")
	warmActivate(t, n3, A)
	incr.Store(0)
	// 目录预置 X → node2：旧化身的请求远发 node2 并在其激活窗入箱。
	regCtx, regCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if _, _, err := dir.Register(regCtx, X, directory.Owner{Node: 2}, nil); err != nil {
		regCancel()
		t.Fatal(err)
	}
	regCancel()

	n1a := startRestartNode(t, dir, addrs, 1, kinds)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = n1a.Client().Call(ctx, X, "go") // 入 node2 激活窗收件箱
	}()
	time.Sleep(200 * time.Millisecond)
	_ = n1a.Close()

	n1b := startRestartNode(t, dir, addrs, 1, kinds)
	t.Cleanup(func() { shutdownNode(n1b) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	v, err := n1b.Client().Call(ctx, A, "incr") // 非幂等（无 Idempotent 选项）
	time.Sleep(200 * time.Millisecond)
	if err != nil || incr.Load() != 1 {
		t.Fatalf("non-idempotent call: v=%v err=%v executions=%d, want executions=1 (no replay on late unavailability)", v, err, incr.Load())
	}
}
