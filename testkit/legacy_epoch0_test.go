package testkit

// 票 15 红测（testkit redis 门禁）：遗留 epoch-0 目录条目在 registry 形态
// 被接管（路由真值面判死 → miss → 激活 CAS 自愈）；放置提示缓存条目
// （epoch=0「未知」语义）不因严格判定被误判失效（防回归：严格化后每次
// 命中都 Invalidate+重选）。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

// TestLegacyEpoch0EntryTakenOver 遗留 epoch-0 条目（外部注入形态）：路由
// 真值面判死 → miss 收口 → 本地激活 CAS 接管自愈（分支 3）。
func TestLegacyEpoch0EntryTakenOver(t *testing.T) {
	mustRedis(t)
	c := NewCluster(t, 2, counterKinds(nil), WithRedis(redisAddr(), 1500*time.Millisecond, 300*time.Millisecond))
	ctx := context.Background()
	dir := c.Directory()
	id := mustID(t, "counter", "legacy-epoch0")

	// 注入遗留 epoch-0 条目：归属 node1（其本地无实例）。
	if won, _, err := dir.Register(ctx, id, directory.Owner{Node: 1, Epoch: 0}, nil); err != nil || !won {
		t.Fatalf("inject legacy epoch-0 entry: won=%v err=%v", won, err)
	}
	// node2 调用：真值面判死 → miss → 放置/激活接管（计数从 1 起）。
	if v, err := c.Node(1).Call(ctx, id, 0); err != nil || v != 1 {
		t.Fatalf("call over legacy epoch-0 entry: v=%v err=%v (want takeover + v=1)", v, err)
	}
	loc, found, err := dir.Lookup(ctx, id)
	if err != nil || !found {
		t.Fatalf("lookup after takeover: found=%v err=%v", found, err)
	}
	if loc.Epoch == 0 {
		t.Fatalf("entry must be re-registered with a real epoch, got %+v", loc)
	}
}

// TestPlacementHint_StaysValidWithoutEpoch 放置提示缓存条目（epoch=0）不
// 被严格判定误杀：放置回填的 hint 经路由缓存命中不失效（每次命中都失效
// 会退化为逐调用放置——放置计数爆炸的回归形态）。
func TestPlacementHint_StaysValidWithoutEpoch(t *testing.T) {
	mustRedis(t)
	c := NewCluster(t, 3, counterKinds(nil), WithRedis(redisAddr(), 1500*time.Millisecond, 300*time.Millisecond), WithPlacement())
	ctx := context.Background()
	dir := c.Directory()

	// 预热：等待成员视图就绪（放置候选非空的前提）。
	reg, ok := dir.(directory.NodeRegistry)
	if !ok {
		t.Fatal("redis directory must implement NodeRegistry")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		ns, err := reg.NodesByKind(ctx, "counter")
		if err == nil && len(ns) == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("member view not ready")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 同一 id 连续调用 N 次：放置 hint 条目（epoch=0）在缓存命中期照常
	// 路由到目标——若严格判定误杀 hint，每次命中都 Invalidate+重选，N
	// 次调用的落点将随机漂移（同一实体多节点激活竞态）——以「目录归属
	// 稳定 + 调用全成功」为锚。
	id := mustID(t, "counter", "hint-stable")
	last := rans.NodeID(0)
	for i := 0; i < 10; i++ {
		if _, err := c.Node(0).Call(ctx, id, 0); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		loc, found, _ := dir.Lookup(ctx, id)
		if !found {
			t.Fatalf("call %d: no directory entry", i)
		}
		if last != 0 && loc.Node != last {
			t.Fatalf("ownership drifted across calls: %d → %d (placement hint must pin the first choice)", last, loc.Node)
		}
		last = loc.Node
	}
}
