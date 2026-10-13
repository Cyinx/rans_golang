package contract

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

// RunNodeMembershipSuite 对提供节点级成员面的目录实现跑成员键契约断言
// （节点成员键与逐实体租约分离：键在 ⟺ 节点可见、值=监听地址、TTL 续期、
// 节点注销顺带清键；值=JSON{addr,kinds}，NodesByKind 按声明过滤——
// 决策出处：docs/decisions/2026-10-02-node-membership-key.md、2026-10-05-node-key-kinds.md）。
// newReg 以套件注入的短过期阈值构造全新成员面；节点注销子测试在实现
// 同时提供 Browser 视图时执行（类型断言探测）。阻塞到全部子测试完成；
// 测试编排面——仅测试进程内使用，非运行时路径。
func RunNodeMembershipSuite(t *testing.T, newReg func(t *testing.T, hbTTL time.Duration) directory.NodeRegistry) {
	ctx := t.Context() // 契约套件：ctx 随测试生命期
	hbTTL := 300 * time.Millisecond

	t.Run("注册即进成员视图且地址可读，未注册不可见", func(t *testing.T) {
		d := newReg(t, hbTTL)
		if err := d.RegisterNode(ctx, 3, directory.NodeInfo{Addr: "10.0.0.3:7001", Kinds: []string{"workspace"}}); err != nil {
			t.Fatal(err)
		}
		if nodes, err := d.NodeIDs(ctx); err != nil {
			t.Fatal(err)
		} else if len(nodes) != 1 || nodes[0] != 3 {
			t.Fatalf("NodeIDs() = %v, want [3]", nodes)
		}
		// NodeAddr 解析 JSON 值格式：addr 字段完整往返
		if addr, ok, err := d.NodeAddr(ctx, 3); err != nil || !ok || addr != "10.0.0.3:7001" {
			t.Fatalf("NodeAddr(3) = (%q,%v,%v), want (10.0.0.3:7001,true,nil)", addr, ok, err)
		}
		// 未注册节点：视图不含、地址 miss（非错误）
		if nodes, err := d.NodeIDs(ctx); err != nil {
			t.Fatal(err)
		} else {
			for _, n := range nodes {
				if n == 4 {
					t.Fatalf("unregistered node 4 must not be visible: %v", nodes)
				}
			}
		}
		if addr, ok, err := d.NodeAddr(ctx, 4); err != nil || ok || addr != "" {
			t.Fatalf("NodeAddr(4) = (%q,%v,%v), want miss", addr, ok, err)
		}
	})

	t.Run("零实体的节点照常可见（空节点不死锁）", func(t *testing.T) {
		d := newReg(t, hbTTL)
		// 只写成员键、不注册任何实体：放置候选集必须含它
		if err := d.RegisterNode(ctx, 7, directory.NodeInfo{Addr: "10.0.0.7:7007"}); err != nil {
			t.Fatal(err)
		}
		if nodes, err := d.NodeIDs(ctx); err != nil {
			t.Fatal(err)
		} else if len(nodes) != 1 || nodes[0] != 7 {
			t.Fatalf("empty node must be visible: %v", nodes)
		}
	})

	t.Run("成员键过期即退出视图", func(t *testing.T) {
		d := newReg(t, hbTTL)
		if err := d.RegisterNode(ctx, 5, directory.NodeInfo{Addr: "10.0.0.5:7005"}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(hbTTL + 2*time.Second)
		for {
			nodes, err := d.NodeIDs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(nodes) == 0 {
				if _, ok, err := d.NodeAddr(ctx, 5); err != nil || ok {
					t.Fatalf("expired node addr: ok=%v err=%v, want miss", ok, err)
				}
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("node still visible %v after membership ttl", hbTTL+2*time.Second)
			}
			time.Sleep(10 * time.Millisecond)
		}
	})

	t.Run("周期重注册保活跨多 TTL 周期", func(t *testing.T) {
		d := newReg(t, hbTTL)
		deadline := time.Now().Add(3 * hbTTL)
		for time.Now().Before(deadline) {
			if err := d.RegisterNode(ctx, 6, directory.NodeInfo{Addr: "10.0.0.6:7006"}); err != nil {
				t.Fatal(err)
			}
			if nodes, err := d.NodeIDs(ctx); err != nil {
				t.Fatal(err)
			} else if len(nodes) != 1 || nodes[0] != 6 {
				t.Fatalf("node lost while re-registering across TTL periods: %v", nodes)
			}
			time.Sleep(hbTTL / 4)
		}
	})

	t.Run("重注册覆盖地址（换监听地址后值更新）", func(t *testing.T) {
		d := newReg(t, hbTTL)
		if err := d.RegisterNode(ctx, 8, directory.NodeInfo{Addr: "10.0.0.8:1"}); err != nil {
			t.Fatal(err)
		}
		if err := d.RegisterNode(ctx, 8, directory.NodeInfo{Addr: "10.0.0.8:2"}); err != nil {
			t.Fatal(err)
		}
		if addr, ok, err := d.NodeAddr(ctx, 8); err != nil || !ok || addr != "10.0.0.8:2" {
			t.Fatalf("NodeAddr(8) = (%q,%v,%v), want latest address", addr, ok, err)
		}
	})

	t.Run("NodesByKind 按声明过滤，空 kinds 节点不进任何 kind 视图", func(t *testing.T) {
		d := newReg(t, hbTTL)
		// 21 声明两个 kind；22 声明其一；23 空 kinds（nil）；24 空数组。
		if err := d.RegisterNode(ctx, 21, directory.NodeInfo{Addr: "10.0.0.21:1", Kinds: []string{"workspace", "greet"}}); err != nil {
			t.Fatal(err)
		}
		if err := d.RegisterNode(ctx, 22, directory.NodeInfo{Addr: "10.0.0.22:1", Kinds: []string{"workspace"}}); err != nil {
			t.Fatal(err)
		}
		if err := d.RegisterNode(ctx, 23, directory.NodeInfo{Addr: "10.0.0.23:1"}); err != nil {
			t.Fatal(err)
		}
		if err := d.RegisterNode(ctx, 24, directory.NodeInfo{Addr: "10.0.0.24:1", Kinds: []string{}}); err != nil {
			t.Fatal(err)
		}
		// workspace：21+22（空 kinds 的 23/24 不进）
		if nodes, err := d.NodesByKind(ctx, "workspace"); err != nil {
			t.Fatal(err)
		} else if !equalNodes(nodes, []rans.NodeID{21, 22}) {
			t.Fatalf("NodesByKind(workspace) = %v, want [21 22]", nodes)
		}
		// greet：只有 21
		if nodes, err := d.NodesByKind(ctx, "greet"); err != nil {
			t.Fatal(err)
		} else if !equalNodes(nodes, []rans.NodeID{21}) {
			t.Fatalf("NodesByKind(greet) = %v, want [21]", nodes)
		}
		// 无人声明的 kind：空集（ErrNoEligibleTarget 面，非错误）
		if nodes, err := d.NodesByKind(ctx, "nobody"); err != nil || len(nodes) != 0 {
			t.Fatalf("NodesByKind(nobody) = %v (%v), want empty", nodes, err)
		}
		// 全集视图不受过滤影响：四个节点都在（空 kinds 节点仍是集群成员）
		if nodes, err := d.NodeIDs(ctx); err != nil {
			t.Fatal(err)
		} else if !equalNodes(nodes, []rans.NodeID{21, 22, 23, 24}) {
			t.Fatalf("NodeIDs = %v, want all four nodes", nodes)
		}
	})

	t.Run("重注册覆盖 kinds（形态变更后视图跟随）", func(t *testing.T) {
		d := newReg(t, hbTTL)
		if err := d.RegisterNode(ctx, 25, directory.NodeInfo{Addr: "10.0.0.25:1", Kinds: []string{"workspace"}}); err != nil {
			t.Fatal(err)
		}
		// 形态变更：业务节点转纯接入（kinds 收缩）
		if err := d.RegisterNode(ctx, 25, directory.NodeInfo{Addr: "10.0.0.25:1", Kinds: []string{"greet"}}); err != nil {
			t.Fatal(err)
		}
		if nodes, err := d.NodesByKind(ctx, "workspace"); err != nil {
			t.Fatal(err)
		} else if len(nodes) != 0 {
			t.Fatalf("NodesByKind(workspace) after kind change = %v, want empty", nodes)
		}
		if nodes, err := d.NodesByKind(ctx, "greet"); err != nil {
			t.Fatal(err)
		} else if !equalNodes(nodes, []rans.NodeID{25}) {
			t.Fatalf("NodesByKind(greet) after kind change = %v, want [25]", nodes)
		}
	})

	t.Run("NodeAddrs 全量快照与探活一致", func(t *testing.T) {
		d := newReg(t, hbTTL)
		if err := d.RegisterNode(ctx, 11, directory.NodeInfo{Addr: "10.1.1.11:1"}); err != nil {
			t.Fatal(err)
		}
		if err := d.RegisterNode(ctx, 12, directory.NodeInfo{Addr: "10.1.1.12:2"}); err != nil {
			t.Fatal(err)
		}
		m, err := d.NodeAddrs(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(m) != 2 || m[11] != "10.1.1.11:1" || m[12] != "10.1.1.12:2" {
			t.Fatalf("NodeAddrs = %v, want {11:10.1.1.11:1, 12:10.1.1.12:2}", m)
		}
		// 与 NodeIDs 同一探活口径：视图一致
		nodes, err := d.NodeIDs(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !equalNodeSet(nodes, m) {
			t.Fatalf("NodeIDs %v and NodeAddrs %v disagree", nodes, m)
		}
	})

	t.Run("UnregisterNode 即时退出成员视图", func(t *testing.T) {
		d := newReg(t, hbTTL)
		if err := d.RegisterNode(ctx, 9, directory.NodeInfo{Addr: "10.0.0.9:9"}); err != nil {
			t.Fatal(err)
		}
		if err := d.UnregisterNode(ctx, 9); err != nil {
			t.Fatal(err)
		}
		if nodes, err := d.NodeIDs(ctx); err != nil {
			t.Fatal(err)
		} else if len(nodes) != 0 {
			t.Fatalf("node must leave the view after UnregisterNode: %v", nodes)
		}
	})

	// 排空标记契约：draining 节点退出三个聚合视图（NodeIDs/
	// NodesByKind/NodeAddrs 一致过滤——放置不再选中排空节点），但单点解析
	// NodeAddr 不过滤（在途请求的应答与转发照常寻址回程）；重注册解除
	// draining 后节点回归视图（覆盖写幂等语义不变）。
	t.Run("draining 节点退出视图但 NodeAddr 仍可解析", func(t *testing.T) {
		d := newReg(t, hbTTL)
		if err := d.RegisterNode(ctx, 31, directory.NodeInfo{Addr: "10.0.0.31:1", Kinds: []string{"workspace"}}); err != nil {
			t.Fatal(err)
		}
		if err := d.RegisterNode(ctx, 32, directory.NodeInfo{Addr: "10.0.0.32:1", Kinds: []string{"workspace"}, Draining: true}); err != nil {
			t.Fatal(err)
		}
		if nodes, err := d.NodeIDs(ctx); err != nil {
			t.Fatal(err)
		} else if !equalNodes(nodes, []rans.NodeID{31}) {
			t.Fatalf("NodeIDs = %v, want [31] (draining node excluded)", nodes)
		}
		if nodes, err := d.NodesByKind(ctx, "workspace"); err != nil {
			t.Fatal(err)
		} else if !equalNodes(nodes, []rans.NodeID{31}) {
			t.Fatalf("NodesByKind(workspace) = %v, want [31] (draining node excluded)", nodes)
		}
		if m, err := d.NodeAddrs(ctx); err != nil {
			t.Fatal(err)
		} else if len(m) != 1 || m[31] != "10.0.0.31:1" {
			t.Fatalf("NodeAddrs = %v, want only node 31 (draining node excluded)", m)
		}
		// 单点解析不过滤：在途应答照常回程
		if addr, ok, err := d.NodeAddr(ctx, 32); err != nil || !ok || addr != "10.0.0.32:1" {
			t.Fatalf("NodeAddr(32) = (%q,%v,%v), want resolvable while draining", addr, ok, err)
		}
		// 解除 draining（重注册覆盖）：回归视图
		if err := d.RegisterNode(ctx, 32, directory.NodeInfo{Addr: "10.0.0.32:1", Kinds: []string{"workspace"}}); err != nil {
			t.Fatal(err)
		}
		if nodes, err := d.NodeIDs(ctx); err != nil {
			t.Fatal(err)
		} else if !equalNodes(nodes, []rans.NodeID{31, 32}) {
			t.Fatalf("NodeIDs after un-drain = %v, want [31 32]", nodes)
		}
	})
}

// equalNodeSet 校验节点集与节点→地址映射的键集一致。
func equalNodeSet(nodes []rans.NodeID, m map[rans.NodeID]string) bool {
	if len(nodes) != len(m) {
		return false
	}
	for _, n := range nodes {
		if _, ok := m[n]; !ok {
			return false
		}
	}
	return true
}

// equalNodes 校验节点集与期望集相等（顺序敏感——接口契约承诺升序）。
func equalNodes(got, want []rans.NodeID) bool {
	if len(got) != len(want) {
		return false
	}
	for i, n := range got {
		if n != want[i] {
			return false
		}
	}
	return true
}

// nodeEpochSuiteTTL 代际套件注入的成员键过期阈值：短值让「判活键过期」
// 路径秒级收敛（死亡节点的查不到断言不等真 TTL）。
const nodeEpochSuiteTTL = 300 * time.Millisecond

// RunNodeEpochMonotonicSuite 节点代际单调契约（H13 决策 20 前半）：取号全
// 局单调；同节点号「注册 → 停（注销/判活键过期）→ 再注册」新代际严格更
// 大；重注册全程携带同一代际（注册面不二次取号）；优雅停机注销路径不触
// 碰计数器（提供 Browser 视图的实现加验）。newReg 以注入的短过期阈值构造
// 全新成员面。阻塞到全部子测试完成；测试编排面——仅测试进程内使用，非
// 运行时路径。
func RunNodeEpochMonotonicSuite(t *testing.T, newReg func(t *testing.T, hbTTL time.Duration) directory.NodeRegistry) {
	ctx := t.Context()
	hbTTL := nodeEpochSuiteTTL

	t.Run("取号单调且同节点号停起重启后新代际严格更大", func(t *testing.T) {
		d := newReg(t, hbTTL)
		e1, err := d.NextNodeEpoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if e1 == 0 {
			t.Fatal("first epoch must be positive")
		}
		if err := d.RegisterNode(ctx, 61, directory.NodeInfo{Addr: "10.0.0.61:1", Epoch: e1}); err != nil {
			t.Fatal(err)
		}
		if e, ok, err := d.NodeEpoch(ctx, 61); err != nil || !ok || e != e1 {
			t.Fatalf("NodeEpoch(61) = (%d,%v,%v), want (%d,true,nil)", e, ok, err, e1)
		}
		// 重注册（地址更新形态）全程携带同一代际：取号是启动期一次动作，
		// 注册面不二次取号
		if err := d.RegisterNode(ctx, 61, directory.NodeInfo{Addr: "10.0.0.61:2", Epoch: e1}); err != nil {
			t.Fatal(err)
		}
		if e, ok, err := d.NodeEpoch(ctx, 61); err != nil || !ok || e != e1 {
			t.Fatalf("NodeEpoch(61) after re-register = (%d,%v,%v), want carried %d", e, ok, err, e1)
		}
		// 停：注销优先（优雅停机路径），无 Browser 视图的实现等判活键过期
		nodeDown(ctx, t, d, 61, hbTTL)
		// 再注册：同节点号，新代际严格更大（全局计数器跨节点死亡存续）
		e2, err := d.NextNodeEpoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if e2 <= e1 {
			t.Fatalf("epoch after node down = %d, want > %d (monotonic across restart)", e2, e1)
		}
		if err := d.RegisterNode(ctx, 61, directory.NodeInfo{Addr: "10.0.0.61:1", Epoch: e2}); err != nil {
			t.Fatal(err)
		}
		if e, ok, err := d.NodeEpoch(ctx, 61); err != nil || !ok || e != e2 {
			t.Fatalf("NodeEpoch(61) after restart = (%d,%v,%v), want (%d,true,nil)", e, ok, err, e2)
		}
	})

	t.Run("优雅停机注销不触碰计数器", func(t *testing.T) {
		d := newReg(t, hbTTL)
		e1, err := d.NextNodeEpoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.RegisterNode(ctx, 62, directory.NodeInfo{Addr: "10.0.0.62:1", Epoch: e1}); err != nil {
			t.Fatal(err)
		}
		if err := d.UnregisterNode(ctx, 62); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := d.NodeEpoch(ctx, 62); err != nil || ok {
			t.Fatalf("NodeEpoch(62) after unregister = ok=%v err=%v, want miss", ok, err)
		}
		// 注销只摘成员键：计数器原样，下一号严格更大
		e2, err := d.NextNodeEpoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if e2 <= e1 {
			t.Fatalf("epoch after graceful unregister = %d, want > %d (counter untouched)", e2, e1)
		}
	})
}

// RunNodeEpochQuerySuite 成员视图代际查询契约（node→epoch 查询面）：多节
// 点注册后各存活节点的代际可查且各归各；未注册节点查不到（非错误）；判
// 活键过期（死亡）后代际与聚合视图一致查不到。newReg 以注入的短过期阈值
// 构造全新成员面。阻塞到全部子测试完成；测试编排面——仅测试进程内使用，
// 非运行时路径。
func RunNodeEpochQuerySuite(t *testing.T, newReg func(t *testing.T, hbTTL time.Duration) directory.NodeRegistry) {
	ctx := t.Context()
	hbTTL := nodeEpochSuiteTTL
	nodes := []rans.NodeID{51, 52, 53}

	t.Run("存活节点的代际可查且各归各", func(t *testing.T) {
		d := newReg(t, hbTTL)
		epochs := make([]uint64, 0, len(nodes))
		for _, node := range nodes {
			e, err := d.NextNodeEpoch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := d.RegisterNode(ctx, node, directory.NodeInfo{
				Addr:  fmt.Sprintf("10.0.0.%d:1", node),
				Epoch: e,
			}); err != nil {
				t.Fatal(err)
			}
			epochs = append(epochs, e)
		}
		for i, node := range nodes {
			if e, ok, err := d.NodeEpoch(ctx, node); err != nil || !ok || e != epochs[i] {
				t.Fatalf("NodeEpoch(%d) = (%d,%v,%v), want (%d,true,nil)", node, e, ok, err, epochs[i])
			}
		}
		// 未注册节点查不到（非错误）
		if e, ok, err := d.NodeEpoch(ctx, 99); err != nil || ok || e != 0 {
			t.Fatalf("NodeEpoch(99) = (%d,%v,%v), want miss", e, ok, err)
		}
	})

	t.Run("判活键过期后代际查不到", func(t *testing.T) {
		d := newReg(t, hbTTL)
		e, err := d.NextNodeEpoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.RegisterNode(ctx, 54, directory.NodeInfo{Addr: "10.0.0.54:1", Epoch: e}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(hbTTL + 2*time.Second)
		for {
			_, ok, err := d.NodeEpoch(ctx, 54)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				// 与聚合视图同一死亡口径：代际查不到 ⟺ 节点退出视图
				gone, err := d.NodeIDs(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if len(gone) != 0 {
					t.Fatalf("NodeIDs after expiry = %v, want empty (same liveness ruling)", gone)
				}
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("NodeEpoch still alive after membership ttl")
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

// nodeDown 节点即时退出成员视图（成员注销面——优雅停机路径）。
func nodeDown(ctx context.Context, t *testing.T, d directory.NodeRegistry, node rans.NodeID, _ time.Duration) {
	t.Helper()
	if err := d.UnregisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := d.NodeEpoch(ctx, node); err != nil || ok {
		t.Fatalf("node %d still visible after unregister: ok=%v err=%v", node, ok, err)
	}
}
