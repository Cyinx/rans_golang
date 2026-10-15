package node

// 启动代际检查（A5）：成员键承载协议区间，node.Start 在监听前拒绝与
// 区间不相交的存活节点（缺字段 0/0 = 断点前版本，同样拒绝）；读取失败
// fail-open。

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

func startGenNode(t *testing.T, dir directory.Directory) *Node {
	t.Helper()
	n, err := New(Config{
		NodeID:     1,
		ListenAddr: "127.0.0.1:0",
		Kinds:      testKinds(),
		Security:   SecurityConfig{Insecure: true},
		Directory:  DirectoryConfig{Injected: dir},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { shutdownNode(n) })
	return n
}

// TestNodeStart_RefusesIncompatibleLiveNode 注册表预置协议区间不相交
// （含缺字段 0/0）的存活节点 → Start 报错且未监听；区间相交或仅有自身
// 旧键 → 正常启动。
func TestNodeStart_RefusesIncompatibleLiveNode(t *testing.T) {
	newDirWith := func(pre func(reg directory.NodeRegistry)) directory.Directory {
		reg := inmem.NewNodeRegistry(time.Minute)
		pre(reg)
		return regDir{Directory: inmem.New(), NodeRegistry: reg}
	}
	nodeCfg := func(dir directory.Directory) Config {
		return Config{
			NodeID: 1, ListenAddr: "127.0.0.1:0", Kinds: testKinds(),
			Security:  SecurityConfig{Insecure: true},
			Directory: DirectoryConfig{Injected: dir},
		}
	}

	// 不相交（对端只讲 v3）：拒绝且未监听。
	n, err := New(nodeCfg(newDirWith(func(reg directory.NodeRegistry) {
		if err := reg.RegisterNode(context.Background(), 2, nodeInfoOf("10.0.0.2:1", 3, 3)); err != nil {
			t.Fatal(err)
		}
	})))
	if err != nil {
		t.Fatal(err)
	}
	err = n.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "mixed-version") {
		t.Fatalf("Start must refuse an incompatible live node, got %v", err)
	}
	if addr := n.Addr(); addr != "" {
		t.Fatalf("refused node must not be listening, got addr %q", addr)
	}
	_ = n.Close()

	// 缺字段（0/0 = 断点前版本）：同样拒绝。
	n2, err := New(nodeCfg(newDirWith(func(reg directory.NodeRegistry) {
		if err := reg.RegisterNode(context.Background(), 2, nodeInfoOf("10.0.0.2:1", 0, 0)); err != nil {
			t.Fatal(err)
		}
	})))
	if err != nil {
		t.Fatal(err)
	}
	if err := n2.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "mixed-version") {
		t.Fatalf("Start must refuse a pre-breakpoint node without proto fields, got %v", err)
	}
	_ = n2.Close()

	// 区间相交（对端 [4,9]）：正常启动。
	if n3 := startGenNode(t, newDirWith(func(reg directory.NodeRegistry) {
		if err := reg.RegisterNode(context.Background(), 2, nodeInfoOf("10.0.0.2:1", 4, 9)); err != nil {
			t.Fatal(err)
		}
	})); n3.Addr() == "" {
		t.Fatal("compatible live node must not block start")
	}

	// 仅有自身旧键：正常启动（自身残留不阻断）。
	if n4 := startGenNode(t, newDirWith(func(reg directory.NodeRegistry) {
		if err := reg.RegisterNode(context.Background(), 1, nodeInfoOf("10.0.0.1:OLD", 3, 3)); err != nil {
			t.Fatal(err)
		}
	})); n4.Addr() == "" {
		t.Fatal("own stale key must not block start")
	}
}

// nodeInfoOf 成员键信息构造（区间 0/0 = 缺字段形态）。
func nodeInfoOf(addr string, pmin, pmax uint16) directory.NodeInfo {
	return directory.NodeInfo{Addr: addr, ProtoMin: pmin, ProtoMax: pmax}
}
