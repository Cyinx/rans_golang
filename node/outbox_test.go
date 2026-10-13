package node

// 节点发件箱观测面（H11 决策 11）：三仪表恒装配进本节点注册表——
// Node.Metrics() 快照可见；深度读数经真实路径驱动（对端握手永不完成的
// 死管道拨号——首帧卡在发送段，余帧留箱）。计数行为（丢弃/发送失败的
// 增量语义）在 internal/rpc 的发件箱测试锁（替身发送面），本面只锁注册
// 与读数通路。

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/testhooks"
)

// TestNodeOutboxMetrics 发件箱三仪表经 Node.Metrics() 可见（恒装配无开
// 关）：深度在 Tell 出站入箱后非零（真实路由→发件箱路径驱动），丢弃与
// 发送失败以零读数在场（计数增量语义归 internal/rpc 发件箱测试）。
func TestNodeOutboxMetrics(t *testing.T) {
	dir := inmem.New()
	n := mustStartNode(t, Config{
		NodeID:    1,
		Kinds:     testKinds(),
		Security:  SecurityConfig{Insecure: true},
		Directory: DirectoryConfig{Injected: dir},
		TestHooks: &testhooks.Hooks{
			Resolve: func(context.Context, rans.NodeID) (string, error) {
				return "127.0.0.1:1", nil // 地址任意：拨号被死管道钩子接管
			},
			Dial: func(context.Context, string) (net.Conn, error) {
				c, _ := net.Pipe() // 两端无人读：握手写阻塞，连接永不就绪
				return c, nil
			},
		},
	})
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	defer shutdownNode(n)

	id := mustID(t, "counter", "outbox-metrics-1")
	if _, _, err := dir.Register(sctx, id, directory.Owner{Node: 7}, nil); err != nil {
		t.Fatal(err)
	}
	// 三帧 Tell 出站：首帧被 worker 取走卡在发送段，余帧留箱（深度 ≥ 1）。
	for i := 0; i < 3; i++ {
		tctx, tcancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := n.Client().Tell(tctx, id, i); err != nil {
			t.Fatalf("tell %d: %v", i, err)
		}
		tcancel()
	}

	// 深度非零（真实入箱路径），三仪表全部在场。
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := map[string]float64{}
		for _, m := range n.Metrics() {
			got[m.Name] = m.Value
		}
		for _, name := range []string{
			"rans.outbox.depth",
			"rans.outbox.dropped",
			"rans.outbox.send_failed",
		} {
			if _, ok := got[name]; !ok {
				t.Fatalf("metric %q missing from node snapshot; got %v", name, got)
			}
		}
		if got["rans.outbox.depth"] >= 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("outbox depth never reached 1 in node snapshot (tell must route through the outbox)")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
