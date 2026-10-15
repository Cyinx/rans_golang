package testkit

// 票 03 红测：首激活放置候选包含本节点（对齐 README——修复前缺省随机放
// 置在有其他候选时永不选本节点，首激活必多一跳、分布偏斜本节点占比 0）；
// 迁移选点仍排除本节点。

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/checkpoint"
)

// TestPlacement_FirstActivationMayLandLocally 3 节点缺省随机放置，从
// node1 发起 30 个新实体首调：落在 node1 的数量显著 > 0（修复前恒 0）。
// 概率口径：均匀随机下 30 次全不落本节点的概率 (2/3)^30 ≈ 5e-6——统计
// 断言的绿侧抖动可忽略；红侧确定性（候选不含 self 时恒 0）。
func TestPlacement_FirstActivationMayLandLocally(t *testing.T) {
	mustRedis(t)
	// 短心跳（300ms）：成员键尽快可见——视图候选 = 3 节点（统计断言的
	// 前提；30s 心跳下视图长期只有自己，首激活会全部退化为本地）。
	c := NewCluster(t, 3, counterKinds(nil), WithRedis(redisAddr(), time.Minute, 300*time.Millisecond), WithPlacement())
	ctx := context.Background()
	time.Sleep(700 * time.Millisecond) // 视图首刷：成员键就位（心跳首写窗）
	local := 0
	for i := 0; i < 30; i++ {
		id := mustID(t, "counter", fmt.Sprintf("land-%d", i))
		if _, err := c.Node(0).Call(ctx, id, 0); err != nil {
			t.Fatalf("first call %d: %v", i, err)
		}
		if c.Node(0).Holds(id) {
			local++
		}
	}
	if local == 0 {
		t.Fatal("first-activation placement never landed on the calling node (self must be a candidate)")
	}
	t.Logf("landed locally: %d/30", local)
}

// TestPlacement_MigrationExcludesSelf 迁移选点候选不含本节点（回归锚，
// 手动 Migrate 面）：node1 的实体经缺省随机放置迁移，目标必是远端
// node2——本节点即便在候选集中也不被选中（修复前后均应成立——锚定
// 迁移面的排除语义不随首激活含 self 而漂移）。
func TestPlacement_MigrationExcludesSelf(t *testing.T) {
	mustRedis(t)
	c := NewCluster(t, 2, counterKinds(nil), WithRedis(redisAddr(), time.Minute, 300*time.Millisecond),
		WithCheckpointStore(checkpoint.New()), WithPlacement())
	ctx := context.Background()
	id := mustID(t, "counter", "mig-self")
	if _, err := c.Node(0).Call(ctx, id, 0); err != nil {
		t.Fatalf("activate: %v", err)
	}
	time.Sleep(700 * time.Millisecond) // 视图含 node2（心跳首写窗）
	// 首激活候选含 self（票 03）：激活可能落在 node1 或 node2（各半）。
	// 已在 node2 = 排除语义平凡成立；在 node1 才走迁移断言。
	if !c.Node(0).Holds(id) {
		if !c.Node(1).Holds(id) {
			t.Fatal("entity must live on one of the two nodes")
		}
		return
	}
	if err := c.Node(0).Migrate(ctx, id); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if c.Node(0).Holds(id) {
		t.Fatal("migration must leave the source node (self excluded from migration candidates)")
	}
	if v, err := c.Node(1).Call(ctx, id, 0); err != nil || v != 1 {
		t.Fatalf("post-migration call on node2: v=%v err=%v", v, err)
	}
	if !c.Node(1).Holds(id) {
		t.Fatal("migration target must be the remote node")
	}
}
