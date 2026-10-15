package testkit

// 误判接管三形态统一验收套件（审查报告 §5 补测建议一的收口）：三种「成员
// 视图与节点真实存活脱节」的触发形态各有端到端验收，断言面统一为四项——
// 接管收敛（目录归属翻转）、旧 owner 止损（围栏/销毁或死方不可服务）、窗
// 口期无双服务（双侧调用收敛新 owner、计数连续）、路由收敛（调用命中新
// owner）。形态与注入原语的对应：
//
//	形态① 成员键外部删除 —— FakeDeath（活体假死：TCP/实体键/服务照常，
//	       仅成员键判死且不复活）；旧 owner 的止损走接管通知围栏（H13 决
//	       策 22 发送半边的 e2e 承载）。
//	形态② KillMembership 注入 —— 既有成员死亡原语（优雅 Close + 双删）：
//	       旧 owner 进程级已死，接管通知与 SelfFence 对它均不可达，「止损」
//	       诚实地退化为「死方不可服务 + 接管收敛」（不虚标活 owner 围栏）。
//	形态③ 取号失败注入 —— WithNodeEpochFailure（成员键停写、节点对成员
//	       视图不可见）：接管通知经成员键派生寻址解析不到该节点（best-
//	       effort 丢失），止损由该节点自身 SelfFence 兜底——「通知丢失由
//	       SelfFence 兜底」承诺的 e2e 承载。
//
// redis 真跑门禁沿用仓库既有模式（mustRedis 探活，make test 的
// test-redis-maybe 拉起 docker 容器；bare go test 无 Redis 时 skip 仍绿）。

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

// TestFault_TakeoverForm1MemberKeyDeleted：双节点 redis 集群，实体激活于
// 节点 A 并经 B 的调用建立 B→A 连接（通知的承载连接——成员键删除后成员
// 键派生寻址不再可用，既有连接即「A 的 TCP 正常」的可达面）→ FakeDeath
// 注入假死 → B 调用触发 CAS 接管 → A 收通知围栏销毁（发送半边缺线时 A
// 的实例永驻，本测试必红）→ 后续调用双侧收敛新 owner 无双服务。
func TestFault_TakeoverForm1MemberKeyDeleted(t *testing.T) {
	leaseTTL, hbInterval := 2*time.Second, 300*time.Millisecond
	mustRedis(t)
	c := NewCluster(t, 2, counterKinds(nil),
		WithRedis(redisAddr(), leaseTTL, hbInterval), WithFakeDeath())
	ctx := context.Background()
	dir := c.Directory()
	reg, ok := dir.(directory.NodeRegistry)
	if !ok {
		t.Fatal("testkit: redis directory must implement NodeRegistry")
	}
	id := mustID(t, "counter", "fakedeath-takeover")

	// 实体激活于节点 A（缺省固定本地放置），再经 B 的调用建立 B→A 连接。
	if v, err := c.Node(0).Call(ctx, id, 0); err != nil || v != 1 {
		t.Fatalf("activation call: v=%v err=%v", v, err)
	}
	if v, err := c.Node(1).Call(ctx, id, 0); err != nil || v != 2 {
		t.Fatalf("warmup remote call: v=%v err=%v", v, err)
	}
	if !c.Node(0).Holds(id) {
		t.Fatal("precondition: node A must hold the entity")
	}

	// 假死注入：成员视图判死、实体条目保持死 owner 常驻、TCP 照常、
	// liveness 周期不复活成员键（两个心跳周期后复验）。
	c.FakeDeath(0)
	time.Sleep(2 * hbInterval)
	ns, err := reg.NodeIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range ns {
		if n == 1 {
			t.Fatal("member view still contains fake-dead node 1 after two heartbeat periods")
		}
	}
	if loc, found, _ := dir.Lookup(ctx, id); !found || loc.Node != 1 {
		t.Fatalf("entity entry must persist with fake-dead owner: loc=%v found=%v", loc, found)
	}
	conn, err := net.DialTimeout("tcp", c.Node(0).Addr(), time.Second)
	if err != nil {
		t.Fatalf("fake-dead node TCP must stay alive: %v", err)
	}
	_ = conn.Close()

	// 节点 B 触发激活：路由有效性判死按 miss 收口 → Register CAS 接管赢
	//（新鲜身份语义：无 Store 实体计数从 1 起）。
	if v, err := c.Node(1).Call(ctx, id, 0); err != nil || v != 1 {
		t.Fatalf("takeover call: v=%v err=%v", v, err)
	}
	if !c.Node(1).Holds(id) {
		t.Fatal("takeover must own on node B")
	}
	if loc, found, _ := dir.Lookup(ctx, id); !found || loc.Node != 2 {
		t.Fatalf("ownership must move to node 2: loc=%v found=%v", loc, found)
	}

	// 旧 owner 秒级围栏/销毁（接管通知直达；发送半边缺线时此处必红）。
	fenceDeadline := time.Now().Add(5 * time.Second)
	for c.Node(0).Holds(id) && time.Now().Before(fenceDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if c.Node(0).Holds(id) {
		t.Fatal("old owner must fence and destroy the taken entity within seconds (takeover notification)")
	}

	// B9 观测：接管赢方 takeover_won ≥1、旧 owner lost_received ≥1
	//（节点注册表读数，懒建计数器未注册计 0）。
	if got := nodeMetricVal(t, c.Node(1), "rans.ownership.takeover_won"); got < 1 {
		t.Fatalf("rans.ownership.takeover_won = %v on winner, want >= 1", got)
	}
	if got := nodeMetricVal(t, c.Node(0), "rans.ownership.lost_received"); got < 1 {
		t.Fatalf("rans.ownership.lost_received = %v on old owner, want >= 1", got)
	}

	// 窗口期无双服务：新旧 owner 双侧发起的调用全部收敛新 owner，计数连续。
	for i, want := range []int{2, 3, 4, 5} {
		node := i % 2
		if v, err := c.Node(node).Call(ctx, id, 0); err != nil || v != want {
			t.Fatalf("post-takeover call from node %d: v=%v want=%d err=%v", node+1, v, want, err)
		}
	}
}

// TestFault_TakeoverForm2KillMembership：三节点 redis 集群，实体激活于节
// 点 A 并经 B 的调用建立远程服务面（B 的位置缓存落 A 正条目——死亡后的
// 陈旧缓存收口梯子也在断言面内）→ KillMembership 注入（成员视图即刻判
// 死、死 owner 条目常驻）→ B 经陈旧缓存/目录有效性判死按 miss 收口，CAS
// 接管翻转归属 → 双幸存者（B、C）调用全部收敛新 owner，计数连续——死方
// 陈旧孤儿随进程死亡不可服务（吸收原 TestFault_NodeDeathTakeoverReactiv
// ation 的接管重激活验收，断言面并入本套件）。
//
// 止损断言面的诚实口径（原语真实语义）：KillMembership 先 Close 节点——
// 旧 owner 已死，接管通知与 SelfFence 对它都不可达，「旧 owner 止损」退化
// 为「死方不可服务 + 接管收敛」（TCP 拒连锚定前提；活 owner 的通知围栏与
// 自围栏止损分别归形态①③，不在此虚标）。
func TestFault_TakeoverForm2KillMembership(t *testing.T) {
	leaseTTL, hbInterval := 2*time.Second, 300*time.Millisecond
	mustRedis(t)
	c := NewCluster(t, 3, counterKinds(nil), WithRedis(redisAddr(), leaseTTL, hbInterval))
	ctx := context.Background()
	dir := c.Directory()
	id := mustID(t, "counter", "takeover-form2")

	// 实体激活于节点 A（缺省固定本地放置），经 B 的调用建立远程服务面。
	if v, err := c.Node(0).Call(ctx, id, 0); err != nil || v != 1 {
		t.Fatalf("activation call: v=%v err=%v", v, err)
	}
	if v, err := c.Node(1).Call(ctx, id, 0); err != nil || v != 2 {
		t.Fatalf("warmup remote call: v=%v err=%v", v, err)
	}
	if !c.Node(0).Holds(id) {
		t.Fatal("precondition: node A must hold the entity")
	}

	// 成员死亡注入（确定性快路径）：节点停机 + 判活键/总表 field 双删。
	c.KillMembership(0)
	// 前提锚定：旧 owner 已死（TCP 拒连）——止损退化口径的事实基础。
	if conn, err := net.DialTimeout("tcp", c.Node(0).Addr(), time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("killed node must not accept connections (premise: old owner is dead)")
	}
	reg, ok := dir.(directory.NodeRegistry)
	if !ok {
		t.Fatal("testkit: redis directory must implement NodeRegistry")
	}
	pollUntil(t, 2*time.Second, "member view eviction of killed node", func() bool {
		ns, err := reg.NodeIDs(ctx)
		return err == nil && !slices.Contains(ns, rans.NodeID(1))
	})
	if loc, found, _ := dir.Lookup(ctx, id); !found || loc.Node != 1 {
		t.Fatalf("dead-owner entry must persist (v3 crash residue): owner=%v found=%v", loc.Node, found)
	}

	// 接管收敛：B 的调用经陈旧缓存/目录有效性判死按 miss 收口 → CAS 接管
	//（新鲜身份语义：无 Store 实体计数从 1 起）。
	if v, err := c.Node(1).Call(ctx, id, 0); err != nil || v != 1 {
		t.Fatalf("takeover call: v=%v err=%v", v, err)
	}
	if !c.Node(1).Holds(id) {
		t.Fatal("takeover must own on node B")
	}
	if loc, found, _ := dir.Lookup(ctx, id); !found || loc.Node != 2 {
		t.Fatalf("ownership must move to node 2: loc=%v found=%v", loc.Node, found)
	}

	// 窗口期无双服务/路由收敛：双幸存者（B、C）发起的调用全部收敛新
	// owner，计数连续——若死方陈旧实例仍在服务，返回值将破坏连续性。
	for i, want := range []int{2, 3, 4, 5} {
		node := 1 + i%2
		if v, err := c.Node(node).Call(ctx, id, 0); err != nil || v != want {
			t.Fatalf("post-takeover call from node %d: v=%v want=%d err=%v", node+1, v, want, err)
		}
	}
}

// TestFault_TakeoverForm3StartFailsOnEpochAllocation（票 14 语义重塑）：
// 同步取号使「取号失败的不可见节点仍在服务」形态消亡——取号失败即
// Start 终态报错：无监听残留、成员视图不可见、其余节点不受影响（原形
// 态的本地服务/接管/止损链随 epoch-0 窗口一并退役；运行期成员面故障由
func TestFault_TakeoverForm3StartFailsOnEpochAllocation(t *testing.T) {
	const (
		leaseTTL   = 2 * time.Second
		hbInterval = 100 * time.Millisecond
	)
	mustRedis(t)
	c := NewCluster(t, 2, counterKinds(nil), WithRedis(redisAddr(), leaseTTL, hbInterval),
		WithNodeEpochFailure(0))
	ctx := context.Background()
	reg, ok := c.Directory().(directory.NodeRegistry)
	if !ok {
		t.Fatal("testkit: redis directory must implement NodeRegistry")
	}
	id := mustID(t, "counter", "start-fail-form3")

	// 注入生效断言：节点 A 成员键永不可见（Start 失败于同步取号）。
	pollUntil(t, 2*time.Second, "injected node stays invisible (start failed)", func() bool {
		ns, err := reg.NodeIDs(ctx)
		return err == nil && !slices.Contains(ns, rans.NodeID(1))
	})
	// 其余节点照常服务。
	if v, err := c.Node(1).Call(ctx, id, 0); err != nil || v != 1 {
		t.Fatalf("healthy node service: v=%v err=%v", v, err)
	}
	if a := c.Node(0).Addr(); a != "" {
		t.Fatalf("start-failed node must leave no listener, got %q", a)
	}
}
