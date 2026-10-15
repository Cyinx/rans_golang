package testkit

// 成员面一死永死 e2e（R1 移植）：存活期成员键丢失 + 分区（接管通知不可
// 达）下，同代际复活禁止——旧代际实例围栏止损、重取号、接管收敛。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/checkpoint"

	"github.com/cyinx/rans_golang/directory"
)

// TestFault_TakeoverForm4MemberKeyResurrection A 持有实体、A↔B 分区、未
// 布防删除 A 成员键、B CAS 接管：A 须在约一个心跳周期内不再持有该实体
// （换代围栏，非通知——通知被分区挡住）、A 新代际 > 旧代际、
// resurrected ≥1；分区解除后双侧调用收敛 B。
func TestFault_TakeoverForm4MemberKeyResurrection(t *testing.T) {
	leaseTTL, hb := 2*time.Second, 300*time.Millisecond
	mustRedis(t)
	c := NewCluster(t, 2, counterKinds(nil), WithRedis(redisAddr(), leaseTTL, hb))
	ctx := context.Background()
	reg, ok := c.Directory().(directory.NodeRegistry)
	if !ok {
		t.Fatal("testkit: redis directory must implement NodeRegistry")
	}
	id := mustID(t, "counter", "resurrect-form4")

	if v, err := c.Node(0).Call(ctx, id, 0); err != nil || v != 1 {
		t.Fatalf("activation call: v=%v err=%v", v, err)
	}
	if !c.Node(0).Holds(id) {
		t.Fatal("precondition: node A must hold the entity")
	}
	oldEpoch, alive, err := reg.NodeEpoch(ctx, 1)
	if err != nil || !alive || oldEpoch == 0 {
		t.Fatalf("precondition: node 1 epoch=%d alive=%v err=%v", oldEpoch, alive, err)
	}

	// 分区：B 接管赢方直发的所有权丢失通知对 A 不可达。
	c.Partition(0, 1)

	// 未布防删除 A 成员键（布防门控会吞掉续期探测——本形态要真探测）；
	// A 的续期可能抢先重写键，按取大窗口重试直至 B 接管成立。
	took := false
	var vB any
	for attempt := 0; attempt < 8 && !took; attempt++ {
		_ = reg.UnregisterNode(ctx, 1, 0)
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		v, err := c.Node(1).Call(cctx, id, 0)
		cancel()
		if err == nil {
			vB = v
			took = true
		}
	}
	if !took {
		t.Fatalf("could not inject takeover (last reply %v)", vB)
	}

	// A 在数个心跳周期内不再持有该实体（换代围栏止损，不靠通知）。
	deadline := time.Now().Add(5*hb + 2*time.Second)
	for c.Node(0).Holds(id) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if c.Node(0).Holds(id) {
		t.Fatal("node A must drop the taken entity via re-epoch fencing (notification is partitioned away)")
	}
	// A 换代：新代际严格大于旧值；resurrected 观测落表。
	d2 := time.Now().Add(5*hb + 2*time.Second)
	for {
		if ep, alive, _ := reg.NodeEpoch(ctx, 1); alive && ep > oldEpoch {
			break
		}
		if time.Now().After(d2) {
			t.Fatal("node A must re-epoch after membership key loss (same-epoch resurrection forbidden)")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := nodeMetricVal(t, c.Node(0), "rans.membership.resurrected"); got < 1 {
		t.Fatalf("rans.membership.resurrected = %v on node A, want >= 1", got)
	}

	// 分区解除：双侧调用收敛 B，计数连续（接管后的新实例续走）。
	c.Heal(0, 1)
	base := vB.(int)
	for i, want := range []int{base + 1, base + 2} {
		node := i % 2
		if v, err := c.Node(node).Call(ctx, id, 0); err != nil || v != want {
			t.Fatalf("post-heal call from node %d: v=%v want=%d err=%v", node+1, v, want, err)
		}
	}
}

// TestFault_MemberKeyLossWithoutTakeoverReEpochs 删键后无人接管：A 换代、
// 旧实例经 Retire 终快照落库（高水位抬升）、后续调用在 A 新代际上恢复且
// 计数连续。
func TestFault_MemberKeyLossWithoutTakeoverReEpochs(t *testing.T) {
	leaseTTL, hb := 2*time.Second, 300*time.Millisecond
	mustRedis(t)
	store := checkpoint.New()
	c := NewCluster(t, 1, snapKinds(),
		WithRedis(redisAddr(), leaseTTL, hb), WithCheckpointStore(store))
	ctx := context.Background()
	reg := c.Directory().(directory.NodeRegistry)
	id := mustID(t, "counter", "reepoch-alone")

	if v, err := c.Node(0).Call(ctx, id, 1); err != nil || v != 1 {
		t.Fatalf("activation call: v=%v err=%v", v, err)
	}
	oldEpoch, alive, err := reg.NodeEpoch(ctx, 1)
	if err != nil || !alive || oldEpoch == 0 {
		t.Fatalf("precondition: epoch=%d alive=%v err=%v", oldEpoch, alive, err)
	}

	if err := reg.UnregisterNode(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}

	// 换代（数个心跳周期内）+ resurrected 观测。
	deadline := time.Now().Add(5*hb + 2*time.Second)
	for {
		if ep, alive, _ := reg.NodeEpoch(ctx, 1); alive && ep > oldEpoch {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node must re-epoch after membership key loss")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := nodeMetricVal(t, c.Node(0), "rans.membership.resurrected"); got < 1 {
		t.Fatalf("rans.membership.resurrected = %v, want >= 1", got)
	}
	// 旧实例终快照落库：高水位 ≥1（Retire 形态落盘，非周期 sweep）。
	d2 := time.Now().Add(3 * time.Second)
	for {
		if hw, _, _ := store.HighWater(ctx, id); hw >= 1 {
			break
		}
		if time.Now().After(d2) {
			t.Fatal("retired old instance must land its final snapshot")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 后续调用恢复且计数连续（Store 恢复 → 2）。
	if v, err := c.Node(0).Call(ctx, id, 1); err != nil || v != 2 {
		t.Fatalf("post-re-epoch call: v=%v err=%v, want 2 (state restored from Store)", v, err)
	}
}
