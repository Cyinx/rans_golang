package testkit

// 成员死亡注入（KillMembership）的接管重激活验收（H13 决策 21/22 的
// testkit 闭环）：节点成员死亡（确定性双删，免等 heartbeatTTL）→ 死
// owner 目录条目常驻（v3 崩溃残留形态）→ 幸存者调用经路由有效性判定按
// miss 收口 → 本地激活 Register 的 CAS 接管完成归属翻转 → 陈旧孤儿
//（被杀节点的本地实例）不再服务（调用连续性锚定）。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

// TestFault_NodeDeathTakeoverReactivation：三节点 redis 集群，实体激活于
// 任一宿主 → KillMembership 杀宿主（成员死亡，条目保持死 owner 常驻）→
// 成员视图立即淘汰（无轮询等待）→ 幸存者调用触发接管重激活（新鲜身份
// 语义：无 checkpoint 计数从 1 起）→ 目录归属翻转至幸存者 → 另一幸存者
// 的后续调用路由到新 owner（计数连续，被杀节点的陈旧实例不服务）。
func TestFault_NodeDeathTakeoverReactivation(t *testing.T) {
	leaseTTL, hbInterval := 2*time.Second, 300*time.Millisecond
	c := NewCluster(t, 3, counterKinds(nil), WithRedis(redisAddr(), leaseTTL, hbInterval))
	ctx := context.Background()
	dir := c.Directory()
	id := mustID(t, "counter", "noddeath-takeover")

	v, err := c.Node(0).Call(ctx, id, 0)
	if err != nil || v != 1 {
		t.Fatalf("activation call: v=%v err=%v", v, err)
	}
	loc, found, err := dir.Lookup(ctx, id)
	if err != nil || !found {
		t.Fatalf("lookup after activation: found=%v err=%v", found, err)
	}
	dead := loc.Node // 实体宿主（放置随机）；幸存者 = 其余两节点
	if dead < 1 || dead > 3 {
		t.Fatalf("unexpected owner node %d", dead)
	}
	var s1, s2 int // 幸存者下标（0 基；s1 后到 0 值即首个幸存者）
	for i := 1; i <= 3; i++ {
		if rans.NodeID(i) == dead {
			continue
		}
		if s1 == 0 {
			s1 = i - 1
		} else {
			s2 = i - 1
		}
	}

	// 成员死亡（确定性快路径）：节点停机 + 判活键/总表 field 双删——
	// 成员视图即刻淘汰，死 owner 条目常驻（v3 崩溃残留形态）。
	c.KillMembership(int(dead) - 1)
	reg, ok := dir.(directory.NodeRegistry)
	if !ok {
		t.Fatal("testkit: redis directory must implement NodeRegistry")
	}
	ns, err := reg.NodeIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range ns {
		if n == dead {
			t.Fatalf("member view still contains killed node %d right after KillMembership", dead)
		}
	}
	if loc, found, _ := dir.Lookup(ctx, id); !found || loc.Node != dead {
		t.Fatalf("dead-owner entry must persist (v3 crash residue): owner=%v found=%v", loc.Node, found)
	}

	// 幸存者重激活：路由有效性判定按 miss 收口 → 本地激活 → CAS 接管
	//（新鲜身份语义：无 Store 实体计数从 1 重新开始）。
	v, err = c.Node(s1).Call(ctx, id, 0)
	if err != nil || v != 1 {
		t.Fatalf("survivor takeover reactivation (survivor=%d): v=%v err=%v", s1+1, v, err)
	}
	loc, found, err = dir.Lookup(ctx, id)
	if err != nil || !found {
		t.Fatalf("lookup after takeover: found=%v err=%v", found, err)
	}
	if loc.Node == dead {
		t.Fatalf("directory still points at dead node %d after takeover", dead)
	}
	if loc.Node != rans.NodeID(s1+1) {
		t.Fatalf("takeover must own on the activating survivor node %d, got node %d", s1+1, loc.Node)
	}

	// 陈旧孤儿不服务：另一幸存者的调用经目录路由到新 owner，计数连续
	// 到 2——若被杀节点的陈旧实例仍在服务，返回值将破坏连续性。
	v, err = c.Node(s2).Call(ctx, id, 0)
	if err != nil || v != 2 {
		t.Fatalf("post-takeover continuity call (survivor=%d): v=%v err=%v", s2+1, v, err)
	}
}
