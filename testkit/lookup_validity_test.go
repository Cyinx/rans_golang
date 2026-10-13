package testkit

// Lookup 有效性判定锚（H13 决策 20，票 18）：同节点号重启（换代）后的
// 旧 epoch 条目按陈旧归属处理——调用经路由有效性判定按 miss 收口走
// 本地激活，激活 Register 分支 2 不收养（epoch 不匹配）→ 死 owner 接管
// 翻转归属（inc 递增锚定）。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/directory"
)

func TestLookupValidity_EpochMismatch(t *testing.T) {
	mustRedis(t)
	c := NewCluster(t, 3, counterKinds(nil), WithRedis(redisAddr(), 2*time.Second, 300*time.Millisecond))
	n1 := c.Node(0)
	ctx := context.Background()
	dir := c.Directory()

	id := mustID(t, "counter", "epoch-mismatch")
	if v, err := n1.Call(ctx, id, 0); err != nil || v != 1 {
		t.Fatalf("activation: v=%v err=%v", v, err)
	}
	loc, found, err := dir.Lookup(ctx, id)
	if err != nil || !found {
		t.Fatalf("lookup: found=%v err=%v", found, err)
	}
	node1 := loc.Node

	// 模拟同节点号重启换代：条目改写为同 node、旧 epoch+1（不匹配该节点
	// 当前世代的全部活值——本测试直接写一个与真实代际不同的值）。
	reg := dir.(directory.NodeRegistry)
	ep, alive, err := reg.NodeEpoch(ctx, node1)
	if err != nil || !alive {
		t.Fatalf("node epoch: ep=%v alive=%v err=%v", ep, alive, err)
	}
	// 种入换代陈旧条目：先墓碑现条目，再以「同节点号、过期代际」注册
	//（分支 1 收养——epoch 是写入方自称的，目录不校验代际真伪）。
	if ok, err := dir.UnregisterIfOwner(ctx, id, loc); err != nil || !ok {
		t.Fatalf("tombstone current entry: ok=%v err=%v", ok, err)
	}
	staleSelf := directory.Owner{Node: node1, Epoch: ep + 1000}
	won, seeded, err := dir.Register(ctx, id, staleSelf, nil)
	if err != nil || !won {
		t.Fatalf("seed stale-epoch entry: won=%v err=%v", won, err)
	}

	// 从 n2 调用（无本地实例——重启后真实形态）：路由 Lookup 见死 owner
	//（同节点号但代际过期）→ 有效性判定按 miss 收口 → 放置/本地激活 →
	// Register 分支 4+死 owner 接管（inc 严格递增，归属翻转出过期代际）。
	n2 := c.Node(1)
	if _, err := n2.Call(ctx, id, 0); err != nil {
		t.Fatalf("call after epoch mismatch: %v", err)
	}
	loc2, found, err := dir.Lookup(ctx, id)
	if err != nil || !found {
		t.Fatalf("lookup after takeover: found=%v err=%v", found, err)
	}
	if loc2.Epoch == seeded.Epoch {
		t.Fatalf("stale-epoch entry must not survive routing: owner=%+v", loc2)
	}
	if loc2.Incarnation <= seeded.Incarnation {
		t.Fatalf("takeover must bump incarnation: %d ≤ %d", loc2.Incarnation, seeded.Incarnation)
	}
}
