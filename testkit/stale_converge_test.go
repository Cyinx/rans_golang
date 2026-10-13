package testkit

// 无 watcher 收敛锚（H13：DirectoryWatcher 已删）：陈旧正缓存（死 owner）
// 经路由有效性判定即时按 miss 收口——不依赖 30s TTL（那是提示丢失的兜底
// 上界）。编排：n1 激活实体 → n2 调用（缓存正条目 n1）→ KillMembership
// 杀 n1 → n2 再调用：缓存条目指死 owner → 有效性判定失效 → miss 收口 →
// 幸存者接管成功（不等 TTL）。

import (
	"context"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"testing"
	"time"
)

func TestStaleRouting_ConvergesWithoutWatcher(t *testing.T) {
	mustRedis(t)
	c := NewCluster(t, 3, counterKinds(nil), WithRedis(redisAddr(), 2*time.Second, 300*time.Millisecond), WithPlacement())
	n2 := c.Node(1)
	ctx := context.Background()
	dir := c.Directory()

	// 成员视图就绪（三节点心跳键落位——放置候选依赖 kinds 视图；未就绪
	// 时 NodesByKind 为空，放置回落本地激活）。
	regFace := dir.(directory.NodeRegistry)
	deadline0 := time.Now().Add(5 * time.Second)
	for {
		ns, err := regFace.NodeIDs(ctx)
		if err == nil && len(ns) == 3 {
			break
		}
		if time.Now().After(deadline0) {
			t.Fatalf("member view not ready: %v", ns)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 放置随机：逐个新身份调用直到首个落在非 n2 节点的实体（重试新
	// 身份而非同 id——同 id 一旦落在 n2 即本地命中不再改判）。
	var id rans.ID
	var owner int
	for i := 0; i < 10; i++ {
		cand := mustID(t, "counter", "stale-conv")
		if _, err := n2.Call(ctx, cand, 0); err != nil {
			t.Fatalf("seed call: %v", err)
		}
		loc, found, _ := dir.Lookup(ctx, cand)
		if found && loc.Node != 2 {
			id, owner = cand, int(loc.Node)
			break
		}
	}
	if owner == 0 {
		t.Fatal("placement kept landing on caller; cannot arrange stale cache shape")
	}

	c.KillMembership(owner - 1)

	// 死 owner 正缓存：调用即刻经有效性判定收口（无 TTL 等待——全程
	// 预算 5s 内必须成功）。
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := n2.Call(cctx, id, 0); err != nil {
		t.Fatalf("stale-cache call must converge via validity check: %v", err)
	}
	loc, found, _ := dir.Lookup(ctx, id)
	if !found || loc.Node == rans.NodeID(owner) {
		t.Fatalf("owner must flip off killed node: %+v found=%v", loc, found)
	}
}
