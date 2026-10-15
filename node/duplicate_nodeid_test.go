package node

// NodeID 重号让位（inmem 目录 + inmem 注册表组合，免 Redis）：同 NodeID
// 两进程共享成员面——新进程注册更大代际后，旧进程下一续期 tick 判定
// Superseded：围栏全部、封表、自关；新进程照常服务。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// regDir inmem 目录 + inmem 注册表的组合目录（两同号进程共享同一实例）。
type regDir struct {
	directory.Directory
	directory.NodeRegistry
}

func startDupNode(t *testing.T, dir directory.Directory) *Node {
	t.Helper()
	return mustStartNode(t, Config{
		NodeID:     1,
		ListenAddr: "127.0.0.1:0",
		Kinds:      testKinds(),
		Security:   SecurityConfig{Insecure: true},
		Directory:  DirectoryConfig{Injected: dir},
		Liveness:   LivenessConfig{Interval: 40 * time.Millisecond},
	})
}

func nodeMetricOf(t *testing.T, n *Node, name string) float64 {
	t.Helper()
	for _, m := range n.Metrics() {
		if m.Name == name {
			return m.Value
		}
	}
	return 0
}

// TestNodeDuplicateNodeID_OlderProcessSuperseded 旧进程存活期取号在先
// （代际更小）；新进程注册后，旧进程在下一 tick 让位：superseded ≥1、
// 调用面封表拒绝、进程自关；新进程继续可服务。
func TestNodeDuplicateNodeID_OlderProcessSuperseded(t *testing.T) {
	dir := regDir{Directory: inmem.New(), NodeRegistry: inmem.NewNodeRegistry(time.Minute)}
	oldN := startDupNode(t, dir)
	defer shutdownNode(oldN)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := mustID(t, "counter", "dup-1")
	if _, err := oldN.Client().Call(ctx, id, 1); err != nil {
		t.Fatalf("old process warmup: %v", err)
	}

	newN := startDupNode(t, dir) // 同号新进程：注册占据成员键（代际更大）
	defer shutdownNode(newN)

	deadline := time.Now().Add(3 * time.Second)
	for nodeMetricOf(t, oldN, "rans.membership.superseded") < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := nodeMetricOf(t, oldN, "rans.membership.superseded"); got < 1 {
		t.Fatal("older process must observe superseded within a few ticks")
	}
	// 封表：旧进程新激活不可重试拒绝（CodeError 形态）。
	other := mustID(t, "counter", "dup-2")
	if _, err := oldN.Client().Call(ctx, other, 1); err == nil {
		t.Fatal("sealed old process must reject new activations")
	}
	// 新进程照常服务。
	if v, err := newN.Client().Call(ctx, mustID(t, "counter", "dup-3"), 1); err != nil || v != 1 {
		t.Fatalf("new process must keep serving: v=%v err=%v", v, err)
	}
}
