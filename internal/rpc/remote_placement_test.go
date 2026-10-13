package rpc

// Placement 接线测试——目录 miss 路径的放置决策（规格 §12 放置）。
// 两个回环 Remote + 共享 inmem 目录 + 可编程假 Placement：
//   - Pick 命中 → miss 请求远发到选中节点（对端 GetOrActivate 落地）；
//   - Pick 未命中 (0,false) → 回退本地激活；
//   - Placement 为 nil → 缺省行为回归钉（miss 一律本地）。

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// fakePlacement 可编程假放置：pick 即 Pick 委托；calls 记录每次 exclude 实参。
type fakePlacement struct {
	pick func(exclude rans.NodeID) (rans.NodeID, bool)

	mu    sync.Mutex
	calls []rans.NodeID
}

func (f *fakePlacement) Pick(exclude rans.NodeID) (rans.NodeID, bool) {
	f.mu.Lock()
	f.calls = append(f.calls, exclude)
	f.mu.Unlock()
	return f.pick(exclude)
}

// excludes 返回已收到的 exclude 实参快照（断言用）。
func (f *fakePlacement) excludes() []rans.NodeID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]rans.NodeID(nil), f.calls...)
}

// 1. miss + Pick→node2：实体在 node2 激活（对端 GetOrActivate 落地），调用方
// 拿到远端执行结果；node1 从未激活该身份。Pick 的 exclude 实参必须是调用方
// 自身节点号（规格 §13「放置目标(排除本地)」）。
func TestRemotePlacementMissRoutesToPickedNode(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	fp := &fakePlacement{pick: func(rans.NodeID) (rans.NodeID, bool) { return 2, true }}
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id:        1,
		placement: fp,
		handle:    func(context.Context, any) (any, error) { return -1, nil }, // 若被本地执行立即暴露
	})
	var n2 atomic.Int32
	node2 := startRemNode(t, dir, rt, nodeSpec{
		id: 2,
		handle: func(context.Context, any) (any, error) {
			return int(n2.Add(1)), nil
		},
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	id := mustUUID(t, "place-1")

	v, err := node1.rem.Call(context.Background(), id, 0)
	if err != nil {
		t.Fatalf("placed call: %v", err)
	}
	if v != 1 {
		t.Fatalf("placed call returned %v, want 1 (executed on picked node 2)", v)
	}
	if n := node1.led.count(); n != 0 {
		t.Fatalf("caller node1 must not activate the entity, %d activations", n)
	}
	if n := node2.led.count(); n != 1 {
		t.Fatalf("picked node2 must activate the entity exactly once, got %d", n)
	}
	if e := fp.excludes(); len(e) != 1 || e[0] != 1 {
		t.Fatalf("Pick exclude args = %v, want [1] (self excluded, one routing decision)", e)
	}
}

// 2. miss + Pick→(0,false)（无候选）：回退本地激活——NoEligibleTarget 保持
// Active 的调用方语义（规格 §13），不因放置缺席而失败。
func TestRemotePlacementPickMissFallsBackLocal(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	fp := &fakePlacement{pick: func(rans.NodeID) (rans.NodeID, bool) { return 0, false }}
	var n1 atomic.Int32
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id:        1,
		placement: fp,
		handle: func(context.Context, any) (any, error) {
			return int(n1.Add(1)), nil
		},
	})
	node2 := startRemNode(t, dir, rt, nodeSpec{
		id:     2,
		handle: func(context.Context, any) (any, error) { return -1, nil },
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	id := mustUUID(t, "place-2")

	v, err := node1.rem.Call(context.Background(), id, 0)
	if err != nil {
		t.Fatalf("no-candidate call: %v", err)
	}
	if v != 1 {
		t.Fatalf("no-candidate call returned %v, want 1 (local fallback)", v)
	}
	if n := node1.led.count(); n != 1 {
		t.Fatalf("fallback must activate locally on node1, got %d activations", n)
	}
	if n := node2.led.count(); n != 0 {
		t.Fatalf("node2 must never see the entity, %d activations", n)
	}
}

// 3. nil Placement（缺省）：miss 一律本地激活；
// Lookup 命中路由不受影响（node2 对 node1 的条目仍走远程）。
func TestRemotePlacementNilDefaultsToLocalActivation(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	var n1, n2 atomic.Int32
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id: 1,
		handle: func(context.Context, any) (any, error) {
			return int(n1.Add(1)), nil
		},
	})
	node2 := startRemNode(t, dir, rt, nodeSpec{
		id: 2,
		handle: func(context.Context, any) (any, error) {
			return int(n2.Add(1)), nil
		},
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	ctx := context.Background()

	// 两个新鲜身份各自 miss → 各自本地激活（缺省语义）
	idA := mustUUID(t, "place-nil-a")
	v, err := node1.rem.Call(ctx, idA, 0)
	if err != nil || v != 1 {
		t.Fatalf("node1 miss call: v=%v err=%v", v, err)
	}
	idB := mustUUID(t, "place-nil-b")
	v, err = node2.rem.Call(ctx, idB, 0)
	if err != nil || v != 1 {
		t.Fatalf("node2 miss call: v=%v err=%v", v, err)
	}
	if n := node1.led.count(); n != 1 {
		t.Fatalf("node1 activations = %d, want 1", n)
	}
	if n := node2.led.count(); n != 1 {
		t.Fatalf("node2 activations = %d, want 1", n)
	}

	// Lookup 命中路由不变：node2 对 idA 走远程，node1 计数连续到 2
	v, err = node2.rem.Call(ctx, idA, 0)
	if err != nil || v != 2 {
		t.Fatalf("lookup-hit remote call: v=%v err=%v (want 2)", v, err)
	}
	if n := n2.Load(); n != 1 {
		t.Fatalf("node2 counter = %d, want 1 (idA must not execute locally)", n)
	}
}

// ---- kind 感知放置探测 ----

// fakeKindPlacement 可编程 kind 感知假放置：PickForKind 即委托；pickFor/
// pick 分别记录两面的实参（kind 与 exclude）——路由梯子「实现 KindPlacement
// 即按 kind 询问、否则沿用 Pick」的探测行为锁。
type fakeKindPlacement struct {
	pickFor func(kind string, exclude rans.NodeID) (rans.NodeID, bool)

	mu     sync.Mutex
	kinds  []string
	exFor  []rans.NodeID
	picked int // Pick 面被问过的次数（探测路径必须为 0）
}

func (f *fakeKindPlacement) PickForKind(kind string, exclude rans.NodeID) (rans.NodeID, bool) {
	f.mu.Lock()
	f.kinds = append(f.kinds, kind)
	f.exFor = append(f.exFor, exclude)
	f.mu.Unlock()
	return f.pickFor(kind, exclude)
}

func (f *fakeKindPlacement) Pick(rans.NodeID) (rans.NodeID, bool) {
	f.mu.Lock()
	f.picked++
	f.mu.Unlock()
	return 0, false
}

// 4. Placement 实现 KindPlacement → 路由梯子按 kind 询问（kind=目标身份的
// kind、exclude=调用方自身节点号），Pick 面不被触碰；PickForKind 无候选 →
// 回退本地激活（与 Pick-miss 同收口）。
func TestRemotePlacementKindAwareLadderPrefersPickForKind(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	var n2 atomic.Int32
	fp := &fakeKindPlacement{pickFor: func(kind string, exclude rans.NodeID) (rans.NodeID, bool) {
		if kind != "cell" || exclude != 1 {
			t.Errorf("PickForKind(kind=%q, exclude=%d); want (cell, 1)", kind, exclude)
		}
		return 2, true
	}}
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id:        1,
		placement: fp,
		handle:    func(context.Context, any) (any, error) { return -1, nil },
	})
	node2 := startRemNode(t, dir, rt, nodeSpec{
		id: 2,
		handle: func(context.Context, any) (any, error) {
			return int(n2.Add(1)), nil
		},
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	id := mustUUID(t, "kind-place-1")

	v, err := node1.rem.Call(context.Background(), id, 0)
	if err != nil {
		t.Fatalf("kind-aware placed call: %v", err)
	}
	if v != 1 {
		t.Fatalf("kind-aware call returned %v, want 1 (executed on picked node 2)", v)
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if len(fp.kinds) != 1 || fp.kinds[0] != "cell" {
		t.Fatalf("PickForKind kind args = %v, want [cell]", fp.kinds)
	}
	if len(fp.exFor) != 1 || fp.exFor[0] != 1 {
		t.Fatalf("PickForKind exclude args = %v, want [1] (self)", fp.exFor)
	}
	if fp.picked != 0 {
		t.Fatalf("Pick must not be consulted when KindPlacement is implemented, got %d calls", fp.picked)
	}
}

// 5. kind 感知无候选（kind 无人承载）→ 回退本地激活：调用方保持 Active 的
// 既有收口语义不因 kind 维引入而改变。
func TestRemotePlacementKindAwareNoCandidateFallsBackLocal(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	var n1 atomic.Int32
	fp := &fakeKindPlacement{pickFor: func(string, rans.NodeID) (rans.NodeID, bool) { return 0, false }}
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id:        1,
		placement: fp,
		handle: func(context.Context, any) (any, error) {
			return int(n1.Add(1)), nil
		},
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr()})
	id := mustUUID(t, "kind-place-2")

	v, err := node1.rem.Call(context.Background(), id, 0)
	if err != nil {
		t.Fatalf("no-candidate kind-aware call: %v", err)
	}
	if v != 1 {
		t.Fatalf("no-candidate call returned %v, want 1 (local fallback)", v)
	}
}
