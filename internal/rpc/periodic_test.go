package rpc

// 传输形态的目录清理装配证据。周期任务族（看门狗/checkpoint sweeper）
// 的起跑面已上移 node.Start（单节点等价形态测试在 node 包
// standalone_periodic_test.go）。

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
)

// periodEntity：可卡死激活的实体（stuck 模式），Handle 计数并实现
// rans.Snapshotter（checkpoint sweeper 的候选面）。
type periodEntity struct {
	stuck   chan struct{} // 非 nil：Activate 阻塞其上（测试收尾关闭）
	release chan struct{}
	entered atomic.Bool
	n       atomic.Int32
}

func (e *periodEntity) Activate(ctx context.Context) error {
	e.entered.Store(true)
	if e.stuck != nil {
		<-e.stuck
	}
	return nil
}
func (e *periodEntity) Handle(ctx context.Context, req any) (any, error) {
	return e.n.Add(1), nil
}
func (e *periodEntity) Deactivate(ctx context.Context) error { return nil }
func (e *periodEntity) Snapshot() ([]byte, error) {
	return []byte(strconv.Itoa(int(e.n.Load()))), nil
}
func (e *periodEntity) Restore(b []byte) error { return nil }

// startPeriodicNode 构造带周期任务配置的单节点（不经 startRemNode——其
// nodeSpec 无周期任务字段，本文件自含构造避免扩散改动共享助手）。
func startPeriodicNode(t *testing.T, dir directory.Directory, mgr *activation.Table, cfg Config) *Remote {
	t.Helper()
	cfg.Dir = dir
	cfg.Manager = mgr
	cfg.NodeID = 1
	cfg.ClusterName = "periodic-cluster"
	cfg.Addr = "127.0.0.1:0"
	cfg.Secret = []byte("periodic-secret")
	rt := &resolveTable{}
	cfg.Resolve = rt.resolve
	if cfg.DefaultCallTimeout == 0 {
		cfg.DefaultCallTimeout = 3 * time.Second
	}
	rem, err := New(cfg)
	if err != nil {
		t.Fatalf("new remote: %v", err)
	}
	if err := rem.Start(context.Background()); err != nil {
		t.Fatalf("start remote: %v", err)
	}
	t.Cleanup(func() { _ = rem.Close() })
	return rem
}

// waitUntil 有界轮询直至 cond 为真。
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestRemoteStart_PlainDirectorySkipsCleanup(t *testing.T) {
	dir := inmem.New()
	mgr := activation.New(dir, 1, map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity { return &periodEntity{} },
	})
	startPeriodicNode(t, dir, mgr, Config{})
	// Start 成功返回本身就是断言面（无 panic/无错误）。
}
