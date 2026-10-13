package rpc

// 周期任务族装配测试：Remote.Start 后看门狗/目录清理/checkpoint
// sweeper 真跑（部署级纠正面，不只是库存编排器）；Close/Shutdown 收口
// 周期任务。看门狗部署级证据同时是激活卡死的收敛兜底闭环（无需进程重启）。

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/lifecycle"
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

// TestRemoteStart_RunsWatchdog 部署级证据：Start 后看门狗
// 真跑——卡死激活的实体被自动强弃，粘性黑洞（GetLocal 命中）解除。
func TestRemoteStart_RunsWatchdog(t *testing.T) {
	dir := inmem.New()
	ent := &periodEntity{stuck: make(chan struct{})}
	defer close(ent.stuck) // 收尾放行 zombie
	mgr := activation.New(dir, 1, map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity { return ent },
	})
	rem := startPeriodicNode(t, dir, mgr, Config{
		Watchdog: &lifecycle.WatchdogConfig{
			Interval:          20 * time.Millisecond,
			ActivationTimeout: 60 * time.Millisecond,
			UnloadTimeout:     time.Hour,
			ExecTimeout:       time.Hour,
		},
	})
	id, _ := rans.NewID("cell", "wd-1")
	if _, err := mgr.GetOrActivate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "Activate entered", ent.entered.Load)
	if _, ok := mgr.GetLocal(id); !ok {
		t.Fatal("precondition: stuck instance must be sticky in local")
	}
	waitUntil(t, "deployed watchdog relieves the black hole", func() bool {
		_, ok := mgr.GetLocal(id)
		return !ok
	})
	_ = rem
}

func TestRemoteStart_PlainDirectorySkipsCleanup(t *testing.T) {
	dir := inmem.New()
	mgr := activation.New(dir, 1, map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity { return &periodEntity{} },
	})
	startPeriodicNode(t, dir, mgr, Config{})
	// Start 成功返回本身就是断言面（无 panic/无错误）。
}

func TestRemoteStart_RunsCheckpointSweeper(t *testing.T) {
	dir := inmem.New()
	ent := &periodEntity{}
	store := checkpoint.New()
	// Store 经表构造期注入（快照落库承载归 activation.Options——
	// rpc.Config.CheckpointStore 只驱动周期 sweeper，不顶替写表）。
	mgr := activation.New(dir, 1, map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity { return ent },
	}, activation.Options{Store: store})
	rem := startPeriodicNode(t, dir, mgr, Config{
		CheckpointStore:    store,
		CheckpointInterval: 40 * time.Millisecond,
	})
	id, _ := rans.NewID("cell", "ck-1")
	if _, err := rem.Call(context.Background(), id, "req"); err != nil {
		t.Fatalf("call: %v", err)
	}
	waitUntil(t, "periodic checkpoint lands in store", func() bool {
		_, found, _ := store.Get(context.Background(), id)
		return found
	})
}
