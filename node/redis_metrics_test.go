package node

// redis 模式指标可见性验收（指标接线随组装根内置：gateway pending /
// transport reconnects 归 rpc 装配，仪表落节点自有注册表、经 Node.Metrics()
// 读；读数面 internal/metrics 不在 testkit 依赖面内（约定 spec §3.2
// testkit 行），node 包测试是合法观察点）。
// 锁两项指标经组合后的可观测行为：
//   - gateway pending：在途请求驻留期 ≥1、Kill（Close 急停）快速失败后归零；
//   - transport reconnects：关断既有连接（WrapConn 捕获把手——SeverNode
//     的注入点等价物）→ 再调用按需重拨 → 计数 +1。
// 时序断言一律有界轮询（pollUntil），无承重 sleep。

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/redis"
	"github.com/cyinx/rans_golang/internal/testhooks"
)

// nodeMetric 本节点快照的单指标读数（节点仪表落自有注册表，读数经
// Node.Metrics()——进程级 Default 不承载节点装配的仪表）。
func nodeMetric(n *Node, name string) float64 {
	for _, m := range n.Metrics() {
		if m.Name == name {
			return m.Value
		}
	}
	return 0
}

// metricsBlockEntity 阻塞实体（pending 在途窗口的制造者）：每次 Handle
// 上报到达后挂起，直至 release 关闭（10s 安全上限防泄漏）。
type metricsBlockEntity struct {
	arrivals chan string
	release  chan struct{}
}

func (e *metricsBlockEntity) Activate(context.Context) error   { return nil }
func (e *metricsBlockEntity) Deactivate(context.Context) error { return nil }
func (e *metricsBlockEntity) Handle(_ context.Context, req any) (any, error) {
	if s, ok := req.(string); ok {
		e.arrivals <- s
	}
	select {
	case <-e.release:
	case <-time.After(10 * time.Second):
	}
	return "late", nil
}

// metricsCluster 双节点装配（注入目录由调用方构建并持有；b 侧可带连接包装
// 钩子）。
func metricsCluster(t *testing.T, dir directory.Directory, kinds map[string]rans.Factory,
	bWrap func(net.Conn) net.Conn) (a, b *Node) {
	return metricsClusterDialCapture(t, dir, kinds, nil, bWrap)
}

// metricsClusterDialCapture 同上，另可在 a 侧挂连接包装钩子（reconnects
// 腿的拨号侧关断把手）。
func metricsClusterDialCapture(t *testing.T, dir directory.Directory, kinds map[string]rans.Factory,
	aWrap func(net.Conn) net.Conn, bWrap func(net.Conn) net.Conn) (a, b *Node) {
	t.Helper()
	mk := func(id rans.NodeID, mut func(*Config)) *Node {
		t.Helper()
		cfg := Config{
			NodeID: id, ClusterName: "node-metrics", Security: SecurityConfig{Insecure: true},
			Directory: DirectoryConfig{Injected: dir},
			Kinds:     kinds,
			Lease:     LeaseConfig{Interval: 300 * time.Millisecond},
			Calls:     CallConfig{Default: 5 * time.Second},
		}
		if mut != nil {
			mut(&cfg)
		}
		n, err := New(cfg)
		if err != nil {
			t.Fatalf("New node %d: %v", id, err)
		}
		if err := n.Start(context.Background()); err != nil {
			t.Fatalf("Start node %d: %v", id, err)
		}
		t.Cleanup(func() { _ = n.Close() })
		return n
	}
	b = mk(2, func(c *Config) {
		c.TestHooks = &testhooks.Hooks{WrapConn: bWrap}
	})
	a = mk(1, func(c *Config) {
		c.TestHooks = &testhooks.Hooks{WrapConn: aWrap}
	})
	return a, b
}

// TestRedisMetricsPendingFailFast pending 在途窗腿：n1 对 node2 上的阻塞
// 实体持在途请求 → 读数 ≥1；Kill(node2)（Close 急停）→ 快速失败 → 归零。
func TestRedisMetricsPendingFailFast(t *testing.T) {
	opts := nodeRedisOptions(t)
	opts.HeartbeatTTL = 3 * time.Second
	dir := redis.New(opts)
	defer dir.Close()

	arrivals := make(chan string, 16)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	kinds := testKinds()
	kinds["blocker"] = func(_ rans.ID) rans.Entity {
		return &metricsBlockEntity{arrivals: arrivals, release: release}
	}
	n1, n2 := metricsCluster(t, dir, kinds, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	idBlock := mustID(t, "blocker", "metrics-pending")
	if _, _, err := dir.Register(ctx, idBlock, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// ① pending 在途窗。
	type outcome struct{ err error }
	done := make(chan outcome, 1)
	go func() {
		_, err := n1.Client().Call(ctx, idBlock, "metrics-marker") // 非幂等：快速失败即上抛
		done <- outcome{err: err}
	}()
	arrivalDeadline := time.After(4 * time.Second)
	for arrived := false; !arrived; {
		select {
		case s := <-arrivals:
			if s == "metrics-marker" {
				arrived = true
			}
		case <-arrivalDeadline:
			t.Fatal("阻塞实体未收到请求（pending 在途窗无法建立）")
		}
	}
	if !pollUntil(t, ctx, 2*time.Second, func() bool {
		return nodeMetric(n1, "rans.gateway.pending") >= 1
	}) {
		t.Fatal("gateway pending gauge must be >= 1 while in-flight")
	}
	n2.Close() // Kill 语义：急停（不排空）
	select {
	case r := <-done:
		if r.err == nil {
			t.Fatal("in-flight call must fail fast with node-unavailable, got nil")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("in-flight call did not settle after Kill")
	}
	if !pollUntil(t, ctx, 2*time.Second, func() bool {
		return nodeMetric(n1, "rans.gateway.pending") == 0
	}) {
		t.Fatal("gateway pending gauge must return to 0 after Kill")
	}

}

// TestRedisMetricsReconnects reconnects 腿（独立小集群，计数下界断言）：
// 首拨建立连接（不计数）→ 在**拨号侧**（n1）关断既有连接（WrapConn 捕获
// 把手——SeverNode 的注入点等价物；本地 Close 同步走 shutdown → connLost
// 清表上报，无对端 FIN 传播的异步窗）→ 再调用按需重拨 → reconnects +1
// （default 注册表跨测试共享，计数单调取下界）。
func TestRedisMetricsReconnects(t *testing.T) {
	opts := nodeRedisOptions(t)
	opts.KeyPrefix += "reconnects:"
	dir := redis.New(opts)
	defer dir.Close()

	// 连接捕获把手（mu 保护——WrapConn 可在拨号/接入两侧的装配路径上
	// 执行，测试 goroutine 读取需同步）。挂在 n1：其出站连接由 n1 的调用
	// 建立，捕获到的即拨号侧连接。
	var mu sync.Mutex
	var captured net.Conn
	n1, _ := metricsClusterDialCapture(t, dir, testKinds(), func(nc net.Conn) net.Conn {
		mu.Lock()
		captured = nc
		mu.Unlock()
		return nc
	}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	idDial := mustID(t, "counter", "metrics-reconnect")
	// 首拨：归属放 node2，node1 调用建立连接（首拨不计数）。
	if _, _, err := dir.Register(ctx, idDial, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if v, err := n1.Client().Call(ctx, idDial, 0); err != nil || v != 1 {
		t.Fatalf("first call (first dial) must succeed: v=%v err=%v", v, err)
	}
	if captured == nil {
		t.Fatal("precondition: n1 must have captured its dialed connection")
	}

	mu.Lock()
	severed := captured
	mu.Unlock()
	if severed == nil {
		t.Fatal("precondition: n1 must have captured its dialed connection")
	}
	before := nodeMetric(n1, "rans.transport.reconnects_total")
	if err := severed.Close(); err != nil { // 拨号侧本地关断（同步 shutdown → connLost）
		t.Fatalf("sever captured connection: %v", err)
	}
	// 断连后的重拨收敛：每次调用尝试自带短预算（撞上断连摊派窗的首呼按
	// node-unavailable 快速失败；回复路径与关断传播的交错可能让个别尝试
	// 挂到预算界）——逐次尝试独立拨号新连接，直到回复在重拨连接上往返。
	// 幂等标注 + string 请求（回显不计数）：模糊类失败的环内重试不引入
	// 重复执行歧义（生产调用方对 ConnectionLost 类的既定契约，spec §5.3）。
	var lastErr error
	if !pollUntil(t, ctx, 10*time.Second, func() bool {
		cctx, ccancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		defer ccancel()
		v, err := n1.Client().Call(cctx, idDial, "ping", rans.Idempotent())
		if err != nil {
			lastErr = err
			return false
		}
		return v == "ok"
	}) {
		t.Fatalf("redialed call must succeed on same entity after sever, last err: %v", lastErr)
	}
	if !pollUntil(t, ctx, 2*time.Second, func() bool {
		return nodeMetric(n1, "rans.transport.reconnects_total") >= before+1
	}) {
		t.Fatal("reconnects counter must increment after sever+redial")
	}
}
