package node

// 装配与生命周期全族回归：构造校验、inmem 形态起停、Start 恰一次/
// 二次 Start 报错、Shutdown 幂等、Close 语义、Run ctx 收口。

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/redis"
)

var errUnsupported = errors.New("unsupported request")

// contains 断言助手（错误信息包含子串）。
func contains(s, sub string) bool { return strings.Contains(s, sub) }

// testCounter 最小实体：int 请求计数、string 请求返回报告（arbCounter 同模式，
// 绕过 codegen——int/string 载荷走内置线型，见 examples/arbitration）。
type testCounter struct{ calls int }

func (c *testCounter) Activate(context.Context) error   { return nil }
func (c *testCounter) Deactivate(context.Context) error { return nil }
func (c *testCounter) Handle(_ context.Context, req any) (any, error) {
	switch req.(type) {
	case int:
		c.calls++
		return c.calls, nil
	case string:
		return "ok", nil
	}
	return nil, errUnsupported
}

// TestNewValidation 构造期全量校验（fail fast，不留半开组合体）。
func TestNewValidation(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{"no node id", Config{Kinds: testKinds(), Security: SecurityConfig{Insecure: true}}, "NodeID"},
		{"zero node id", Config{NodeID: 0, Kinds: testKinds(), Security: SecurityConfig{Insecure: true}}, "NodeID"},
		{"no kinds", Config{NodeID: 1, Security: SecurityConfig{Insecure: true}}, "Kinds"},
		{"redis and injected", Config{NodeID: 1, Kinds: testKinds(), Security: SecurityConfig{Insecure: true},
			Directory: DirectoryConfig{Redis: &redis.Options{Addr: "unused"}, Injected: mustInjected()}}, "mutually exclusive"},
		{"standalone with listen addr", Config{NodeID: 1, Standalone: true, ListenAddr: "127.0.0.1:0",
			Kinds: testKinds()}, "ListenAddr"},
		{"nil factory", Config{NodeID: 1, Security: SecurityConfig{Insecure: true},
			Kinds: map[string]rans.Factory{"counter": nil}}, `Kinds["counter"] factory is nil`},
		{"invalid kind key", Config{NodeID: 1, Security: SecurityConfig{Insecure: true},
			Kinds: map[string]rans.Factory{"bad!": func(rans.ID) rans.Entity { return &testCounter{} }}},
			`Kinds["bad!"] is not a valid kind name`},
		{"calls max below default", Config{NodeID: 1, Security: SecurityConfig{Insecure: true},
			Kinds: testKinds(), Calls: CallConfig{Default: time.Minute, Max: time.Second}},
			"must not be below"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.cfg)
			if err == nil || !contains(err.Error(), tc.wantErr) {
				t.Fatalf("New() error = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

// TestNewInmemMinimal 最小装配：构造即完整（调用入口就绪、未起跑无地址）。
func TestNewInmemMinimal(t *testing.T) {
	n, err := New(Config{NodeID: 1, Kinds: testKinds(), Security: SecurityConfig{Insecure: true}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if n.Client() == nil {
		t.Fatal("New must assemble the unified client entry")
	}
	if got := n.Addr(); got != "" {
		t.Fatalf("Addr before Start = %q, want empty", got)
	}
}

// TestStartTwice_Error Start 恰一次：二次 Start 返回错误（终态不可复用）。
func TestStartTwice_Error(t *testing.T) {
	n := mustStartNode(t, Config{NodeID: 1, Kinds: testKinds(), Security: SecurityConfig{Insecure: true}})
	defer shutdownNode(n)
	if err := n.Start(context.Background()); err == nil {
		t.Fatal("second Start must return an error (single-use)")
	}
}

// TestLifecycleInmem inmem 形态起停：Addr 绑定、本地 Call 往返、Shutdown
// 幂等、排空后本地实体清零。
func TestLifecycleInmem(t *testing.T) {
	n, err := New(Config{NodeID: 1, Kinds: testKinds(), Security: SecurityConfig{Insecure: true}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := n.Addr(); got != "" {
		t.Fatalf("Addr before Start = %q, want empty", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if n.Addr() == "" {
		t.Fatal("Addr after Start must be bound")
	}

	// 本地 Call 往返（决策文档约束：ctx 必须带 deadline）。
	id, err := rans.NewID("counter", "life-1")
	if err != nil {
		t.Fatal(err)
	}
	v, err := n.Client().Call(ctx, id, 1)
	if err != nil || v != 1 {
		t.Fatalf("local call: v=%v err=%v (want 1)", v, err)
	}

	// 优雅停机幂等。
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	if err := n.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := n.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown twice (idempotent): %v", err)
	}
	// 停机后排空已生效：本地实体清零。注：排空门闸只辖入站 wire 帧
	//（dispatcher），本地直调路径无停态、排空后仍可按需再激活，故锁
	// 「排空效果」这一可观测等价物（host 测试同口径迁入）。
	if got := len(n.LocalIDs()); got != 0 {
		t.Fatalf("local entities after shutdown = %d, want 0 (drain took effect)", got)
	}
}

// TestCloseInmem Close 语义：立即停止、幂等、目录不关
// （inmem 注入形态本就归调用方）、不排空。
func TestCloseInmem(t *testing.T) {
	n, err := New(Config{NodeID: 1, Kinds: testKinds(), Security: SecurityConfig{Insecure: true}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := n.Close(); err != nil { // 不 panic；立即停止
		t.Fatalf("Close: %v", err)
	}
	if err := n.Close(); err != nil { // 幂等
		t.Fatalf("Close twice (idempotent): %v", err)
	}
}

// TestShutdownBudgetExhaustedReturnsCtxErr Shutdown 预算语义（spec §5.1 第
// 1 条——阻塞导出方法尊重 ctx，耗尽返回包装 ctx.Err() 的错误）：
// 排空是终态动作，预算耗尽不回滚进度、停序照常收口，但调用方必须得到
// 「预算已尽」的错误面——空表 + 预取消 ctx 是确定性最小窗口（逐实体等待
// 段的耗尽断言在 internal/lifecycle 与 SIGTERM 演练各有一级锚）。
func TestShutdownBudgetExhaustedReturnsCtxErr(t *testing.T) {
	// 活预算基线：无实体排空即返回 nil。
	alive := mustStartNode(t, Config{NodeID: 1, Kinds: testKinds(), Security: SecurityConfig{Insecure: true}})
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	if err := alive.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown with live budget: %v, want nil", err)
	}

	// 预算已耗尽（预取消，不依赖时序）：返回包装 ctx.Err() 的错误；节点
	// 照常进入终态（Shutdown 幂等面缓存同一错误）。
	dead := mustStartNode(t, Config{NodeID: 2, Kinds: testKinds(), Security: SecurityConfig{Insecure: true}})
	dctx, dcancel := context.WithCancel(context.Background())
	dcancel()
	err := dead.Shutdown(dctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown with exhausted budget err = %v, want wrapping context.Canceled", err)
	}
	if err := dead.Shutdown(dctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown twice (idempotent) err = %v, want same wrapped context.Canceled", err)
	}
}

// TestStandaloneForm 单节点无传输形态：不监听（Addr 恒空）、本地调用直连
// 本地激活表、管理操作本地语义可用、优雅停机可走。
func TestStandaloneForm(t *testing.T) {
	n, err := New(Config{NodeID: 1, Standalone: true, Kinds: testKinds()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer shutdownNode(n)
	if got := n.Addr(); got != "" {
		t.Fatalf("standalone Addr = %q, want empty (no transport)", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := rans.NewID("counter", "standalone-1")
	if err != nil {
		t.Fatal(err)
	}
	if v, err := n.Client().Call(ctx, id, 1); err != nil || v != 1 {
		t.Fatalf("standalone local call: v=%v err=%v", v, err)
	}
	if err := n.Destroy(ctx, id); err != nil {
		t.Fatalf("Destroy(local): %v", err)
	}
	err = n.Destroy(ctx, mustID(t, "counter", "not-local"))
	if rans.CodeOf(err) != rans.CodeNotFound || rans.ReasonOf(err) != rans.ReasonNotLocal {
		t.Fatalf("Destroy(not local) = %v, want NotLocal", err)
	}
}

// TestRunCtxCancel Run 的 ctx 分支：取消后 Run 返回（nil）。
func TestRunCtxCancel(t *testing.T) {
	n, err := New(Config{NodeID: 1, Kinds: testKinds(), Security: SecurityConfig{Insecure: true}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()
	// 等 Addr 绑定（Start 已过）再取消——避免 Start 未完成即收口。
	deadline := time.Now().Add(3 * time.Second)
	for n.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n.Addr() == "" {
		t.Fatal("Run did not start node in time")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// TestClientFromInEntityHooks rans.ClientFrom 写入侧接线（统一调用入口配套）：
// Activate 与 Handle 的 ctx 都携带统一调用入口——实体钩子内自省可用并发起
// 嵌套调用（本地快路径）。
func TestClientFromInEntityHooks(t *testing.T) {
	calleeID := rans.ID{Kind: "callee", Key: "cf-1"}
	kinds := map[string]rans.Factory{
		"callee": func(_ rans.ID) rans.Entity { return &testCounter{} },
		"caller": func(_ rans.ID) rans.Entity { return &cfCaller{callee: calleeID} },
	}
	n := mustStartNode(t, Config{NodeID: 1, Standalone: true, Kinds: kinds})
	defer shutdownNode(n)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	caller := mustID(t, "caller", "cf-caller")
	v, err := n.Client().Call(ctx, caller, nil)
	if err != nil {
		t.Fatalf("caller call: %v", err)
	}
	if got, ok := v.(int); !ok || got != 1 {
		t.Fatalf("nested call via ClientFrom = %v (%T), want callee count 1", v, v)
	}
}

// cfCaller 实体内嵌套调用实体：Activate/Handle 自省 ClientFrom（nil 即错
// ——接线缺失测试即红），Handle 经调用面嵌套调 callee 计数。
type cfCaller struct {
	callee    rans.ID
	activated bool
	hasInAct  bool
}

func (c *cfCaller) Activate(ctx context.Context) error {
	c.hasInAct = rans.ClientFrom(ctx) != nil
	return nil
}

func (c *cfCaller) Deactivate(context.Context) error { return nil }

func (c *cfCaller) Handle(ctx context.Context, _ any) (any, error) {
	cl := rans.ClientFrom(ctx)
	if cl == nil {
		return nil, errors.New("Handle ctx carries no rans.Client (wiring broken)")
	}
	if !c.hasInAct {
		return nil, errors.New("Activate ctx carries no rans.Client (wiring broken)")
	}
	v, err := cl.Call(ctx, c.callee, 1)
	if err != nil {
		return nil, err
	}
	return v, nil
}
