package lifecycle

// 闲置视图与闲置回收不变量测试（规格 §14.3 E2/G1）：
//   - IdleEntities：仅 Active、kind 已登记策略、闲置时长同时超过策略阈值与
//     minIdle 全局下限；最冷优先（LastDeliveryTime 升序，从未投递=最冷）；
//   - offloadIdle：仅 migrate 模式候选、最冷优先、单轮 maxCount 上限、
//     迁移落位（目录指向远端 + 本地逐出）、无候选零副作用；
//   - CollectIdle（钝化复合体）：快照落 Store → 本地销毁 → 下次 GetOrActivate
//     从 Store 恢复（计数连续）；快照失败/未配 Store 不得销毁。

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// idleCountEntity：计数实体 + carrier 快照面——钝化→重激活的计数连续性断言用。
type idleCountEntity struct {
	n int
}

func (e *idleCountEntity) Activate(context.Context) error   { return nil }
func (e *idleCountEntity) Deactivate(context.Context) error { return nil }
func (e *idleCountEntity) Handle(_ context.Context, _ any) (any, error) {
	e.n++
	return e.n, nil
}
func (e *idleCountEntity) Snapshot() ([]byte, error) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(e.n))
	return b, nil
}
func (e *idleCountEntity) Restore(b []byte) error {
	if len(b) != 8 {
		return fmt.Errorf("idleCountEntity: bad snapshot shape %d", len(b))
	}
	e.n = int(binary.BigEndian.Uint64(b))
	return nil
}

var (
	_ rans.Entity      = (*idleCountEntity)(nil)
	_ rans.Snapshotter = (*idleCountEntity)(nil)
)

// brokenCaptureEntity：快照捕获恒失败的 carrier——快照失败不销毁的断言用。
type brokenCaptureEntity struct {
	idleCountEntity
}

func (e *brokenCaptureEntity) Snapshot() ([]byte, error) {
	return nil, errors.New("capture unavailable (injected)")
}

// idleID 构造测试身份（失败即 Fatal）。
func idleID(t *testing.T, kind, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID(kind, inst)
	if err != nil {
		t.Fatalf("rans.NewID: %v", err)
	}
	return id
}

// newIdleManager 构造带 counter kind（idleCountEntity）的测试 manager，
// checkpoint store 已接线并返回。
func newIdleManager(opts ...activation.Options) (*activation.Table, *checkpoint.InMemory) {
	store := checkpoint.New()
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &idleCountEntity{} },
	}, append([]activation.Options{{Store: store}}, opts...)...)

	return m, store
}

// ---- IdleEntities 视图 ----

// TestIdleEntities_PolicyFloorAndActiveOnly：候选判据四要素——
//  1. kind 未登记策略 → 永不闲置（无策略=不参与回收）；
//  2. 闲置时长须超过策略阈值（新鲜投递者排除）；
//  3. minIdle 是全局下限（抬高门槛）；
//  4. 仅 Active 参与（Destroying 停驻者排除）。
func TestIdleEntities_PolicyFloorAndActiveOnly(t *testing.T) {
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"a": func(_ rans.ID) rans.Entity { return &idleCountEntity{} },
		"b": func(_ rans.ID) rans.Entity { return &idleCountEntity{} },
	}, activation.Options{IdlePolicies: map[string]activation.IdlePolicy{

		// 候选扫描只取 Active

		// policyFresh 刚投递：策略阈值 40ms 内不闲置；policyNever 从未投递
		// （最冷）；noPolicy 的 kind 无策略——永不参与。

		"a": activation.IdlePolicy{IdleAfter: 40 * time.Millisecond, Mode: activation.CollectPassivate}}})
	policyFresh := idleID(t, "a", "idle-fresh")
	policyNever := idleID(t, "a", "idle-never")
	noPolicy := idleID(t, "b", "idle-nopolicy")
	for _, id := range []rans.ID{policyFresh, policyNever, noPolicy} {
		p, err := m.GetOrActivate(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		waitActive(t, p, "pre-scan pawnite")
	}

	if _, err := m.InvokeLocal(context.Background(), policyFresh, nil); err != nil {
		t.Fatal(err)
	}

	if got := IdleEntities(m, 0); len(got) != 1 || got[0] != policyNever {
		t.Fatalf("fresh delivery + no-policy kinds must not idle; want [%v], got %v", policyNever, got)
	}

	time.Sleep(60 * time.Millisecond) // policyFresh 越过 40ms 策略阈值
	if got := IdleEntities(m, 0); len(got) != 2 {
		t.Fatalf("both kind-a entities must idle after threshold, got %v", got)
	}

	// minIdle 全局下限：抬高到 10min 后，60ms 闲置者出局；从未投递者是
	// 最冷（零值时钟=极大闲置，规格 §4.1 tick 语义）不受下限排除。
	if got := IdleEntities(m, 10*time.Minute); len(got) != 1 || got[0] != policyNever {
		t.Fatalf("minIdle floor must exclude sub-floor entities (never-delivered stays coldest), want [%v], got %v", policyNever, got)
	}

	// Active-only：Destroying 停驻（阻塞 Deactivate 维持瞬态）者排除。
	release := make(chan struct{})
	m2 := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"gate": func(_ rans.ID) rans.Entity { return &blockingDeactEntity{release: release} },
	}, activation.Options{IdlePolicies: map[string]activation.IdlePolicy{

		// 前置：激活整体成功后才 Destroy——否则 Destroy 抢先赢激活竞态时用户
		// Deactivate 按配对约束被跳过，实体瞬灭逐出，前置永不成立
		"gate": activation.IdlePolicy{IdleAfter: time.Nanosecond, Mode: activation.CollectPassivate}}})
	stuck := idleID(t, "gate", "idle-stuck")
	pStuck, err := m2.GetOrActivate(context.Background(), stuck)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, pStuck, "idle-stuck pawnite")

	go DestroyLocal(context.Background(), m2, stuck)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p, ok := m2.GetLocal(stuck); ok && p.State() == pawnite.StateDestroying {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if p, ok := m2.GetLocal(stuck); !ok || p.State() != pawnite.StateDestroying {
		t.Fatal("precondition: entity must be held in Destroying by blocked Deactivate")
	}
	if got := IdleEntities(m2, 0); len(got) != 0 {
		t.Fatalf("Destroying entity must not be an idle candidate, got %v", got)
	}
	close(release)
}

// TestIdleEntities_ColdestFirst：排序=LastDeliveryTime 升序（从未投递最冷）。
func TestIdleEntities_ColdestFirst(t *testing.T) {
	m, _ := newIdleManager(activation.Options{IdlePolicies: map[string]activation.IdlePolicy{
		"counter": {IdleAfter: 5 * time.Millisecond, Mode: activation.CollectMigrate},
	}})
	warm := idleID(t, "counter", "cold-warm")
	colder := idleID(t, "counter", "cold-colder")
	coldest := idleID(t, "counter", "cold-never")
	for _, id := range []rans.ID{warm, colder, coldest} {
		p, err := m.GetOrActivate(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		waitActive(t, p, "pre-scan pawnite") // 候选扫描只取 Active
	}
	if _, err := m.InvokeLocal(context.Background(), colder, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := m.InvokeLocal(context.Background(), warm, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // 两者都已越过 5ms 阈值

	got := IdleEntities(m, 0)
	want := []rans.ID{coldest, colder, warm}
	if len(got) != len(want) {
		t.Fatalf("idle entities = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("idle order[%d] = %v, want %v (coldest first)", i, got[i], want[i])
		}
	}
}

// ---- offloadIdle（migrate 模式卸载，包内面）----

// TestOffloadIdle_MigratesColdestFirstWithCap：最冷优先逐个迁移、
// 单轮 maxCount 上限、迁移落位（目录指向远端 + 本地逐出 + Done 关闭）、
// 未卸载者保持本地 Active。
func TestOffloadIdle_MigratesColdestFirstWithCap(t *testing.T) {
	dir := newFakeTransferDir()
	m := activation.New(dir, 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &idleCountEntity{} },
	}, activation.Options{Store: checkpoint.New(), Placement:

	// 制造冷热差：c 最热（最后投递）、a 最冷、b 次之。

	// a/b 越过 5ms 阈值；c 也越过（全部闲置）

	func(string, rans.NodeID) (rans.NodeID, bool) { return 7, true }, IdlePolicies: map[string]activation.IdlePolicy{"counter": activation.IdlePolicy{IdleAfter: 5 * time.Millisecond, Mode: activation.CollectMigrate}}})

	a := idleID(t, "counter", "off-a")
	b := idleID(t, "counter", "off-b")
	c := idleID(t, "counter", "off-c")
	ps := map[rans.ID]*pawnite.Pawnite{}
	for _, id := range []rans.ID{a, b, c} {
		p, err := m.GetOrActivate(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		waitActive(t, p, "pre-offload pawnite")
		ps[id] = p
	}

	if _, err := m.InvokeLocal(context.Background(), a, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	if _, err := m.InvokeLocal(context.Background(), b, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	if _, err := m.InvokeLocal(context.Background(), c, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)

	n, err := offloadIdle(context.Background(), m, 2)
	if err != nil {
		t.Fatalf("offloadIdle error = %v", err)
	}
	if n != 2 {
		t.Fatalf("offloaded = %d, want 2 (maxCount cap)", n)
	}
	// 最冷的 a、b 迁走：目录指向 7、本地逐出、终态落位。
	for _, id := range []rans.ID{a, b} {
		if loc, found, _ := dir.Lookup(context.Background(), id); !found || loc.Node != 7 {
			t.Fatalf("offloaded %v directory = (%v, %v), want node 7", id, loc, found)
		}
		if _, ok := m.GetLocal(id); ok {
			t.Fatalf("offloaded %v must be evicted from local registry", id)
		}
		select {
		case <-ps[id].Done():
		case <-time.After(2 * time.Second):
			t.Fatalf("offloaded %v must reach terminal state", id)
		}
	}
	// 未卸载的 c（最热、超出单轮上限）保持本地 Active 且目录仍指本节点。
	if p, ok := m.GetLocal(c); !ok || p.State() != pawnite.StateActive {
		t.Fatalf("capped-out entity must stay locally Active, got (%v, %v)", ok, p)
	}
	if loc, found, _ := dir.Lookup(context.Background(), c); !found || loc.Node != 1 {
		t.Fatalf("capped-out entity directory = (%v, %v), want node 1", loc, found)
	}
}

// TestOffloadIdle_ModeFilterAndNoCandidates：passivate 模式策略不参与
// 卸载；无候选（无策略/未闲置）→ (0, nil) 零副作用（目录零 CAS）。
func TestOffloadIdle_ModeFilterAndNoCandidates(t *testing.T) {
	dir := newFakeTransferDir()
	m := activation.New(dir, 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &idleCountEntity{} },
	}, activation.Options{Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 7, true }, IdlePolicies:
	// passivate 模式：即使闲置也不是卸载候选。
	map[string]activation.IdlePolicy{"counter": activation.IdlePolicy{IdleAfter: time.Nanosecond, Mode: activation.CollectPassivate}}})

	passive := idleID(t, "counter", "off-passive")
	if _, err := m.GetOrActivate(context.Background(), passive); err != nil {
		t.Fatal(err)
	}

	if n, err := offloadIdle(context.Background(), m, 10); n != 0 || err != nil {
		t.Fatalf("passivate-mode policy must not offload, got (%d, %v)", n, err)
	}
	// 无任何策略：零候选零副作用。
	plain := idleID(t, "counter", "off-plain")
	if _, err := m.GetOrActivate(context.Background(), plain); err != nil {
		t.Fatal(err)
	}
	if n, err := offloadIdle(context.Background(), m, 10); n != 0 || err != nil {
		t.Fatalf("no-policy kinds must never offload, got (%d, %v)", n, err)
	}
	if got := len(dir.transfersSnapshot()); got != 0 {
		t.Fatalf("transfer attempts = %d, want 0", got)
	}
	if _, ok := m.GetLocal(passive); !ok {
		t.Fatal("passivate entity must stay local (offload is migrate-mode only)")
	}
}

// TestOffloadIdle_NoEligibleTargetShortCircuits：placement 无候选 →
// 首个 Migrate 即 ErrNoEligibleTarget（配置性事实）→ 短路返回，不再
// 对其余候选发起迁移；实体全部保持 Active。
func TestOffloadIdle_NoEligibleTargetShortCircuits(t *testing.T) {
	dir := newFakeTransferDir()
	m := activation.New(dir, 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &idleCountEntity{} },
	}, activation.Options{Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 0, false }, IdlePolicies: map[string]activation.

		// 候选扫描只取 Active：先等激活落位
		IdlePolicy{"counter": activation.IdlePolicy{IdleAfter: time.Nanosecond, Mode: activation.CollectMigrate}}})
	ids := []rans.ID{idleID(t, "counter", "off-nt1"), idleID(t, "counter", "off-nt2")}
	for _, id := range ids {
		if _, err := m.GetOrActivate(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}

	for _, id := range ids {
		if p, ok := m.GetLocal(id); !ok {
			t.Fatalf("entity %v not local", id)
		} else {
			waitActive(t, p, "pre-offload pawnite")
		}
	}
	n, err := offloadIdle(context.Background(), m, 10)
	if n != 0 {
		t.Fatalf("offloaded = %d, want 0", n)
	}
	if !errors.Is(err, ErrNoEligibleTarget) {
		t.Fatalf("err = %v, want ErrNoEligibleTarget", err)
	}
	if got := len(dir.transfersSnapshot()); got != 0 {
		t.Fatalf("transfer attempts = %d, want 0", got)
	}
	for _, id := range ids {
		if p, ok := m.GetLocal(id); !ok || p.State() != pawnite.StateActive {
			t.Fatalf("entity %v must stay Active after short-circuit", id)
		}
	}
}

// ---- CollectIdle（钝化复合体）----

// TestCollectIdle_PassivatesAndRestoresState（G1 灵魂）：闲置钝化全闭环——
// 快照落 Store（计数 n=3）→ 本地销毁（逐出 + 目录注销 + Done 关闭）→
// 下次 GetOrActivate 重激活从 Store 恢复（Call 返回 4，计数连续）。
func TestCollectIdle_PassivatesAndRestoresState(t *testing.T) {
	m, store := newIdleManager(activation.Options{IdlePolicies: map[string]activation.IdlePolicy{
		"counter": {IdleAfter: 20 * time.Millisecond, Mode: activation.CollectPassivate},
	}})
	id := idleID(t, "counter", "passive-1")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-passivation pawnite")
	for i := 0; i < 3; i++ {
		if _, err := m.InvokeLocal(context.Background(), id, nil); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(40 * time.Millisecond) // 越过阈值

	n, err := NewIdleCollector(m).CollectIdle(context.Background(), 10)
	if err != nil {
		t.Fatalf("CollectIdle error = %v", err)
	}
	if n != 1 {
		t.Fatalf("passivated = %d, want 1", n)
	}
	// 快照已落 Store，内容是钝化前计数。
	snap, found, err := store.Get(context.Background(), id)
	if err != nil || !found {
		t.Fatalf("store snapshot = (%v, %v, %v), want found", len(snap), found, err)
	}
	if got := int(binary.BigEndian.Uint64(snap)); got != 3 {
		t.Fatalf("snapshot count = %d, want 3", got)
	}
	// 本地已销毁：逐出 + Done 关闭 + 目录条目注销。
	if _, ok := m.GetLocal(id); ok {
		t.Fatal("passivated entity must be evicted from local registry")
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("passivated entity must reach terminal state")
	}
	if _, found, _ := m.Directory().Lookup(context.Background(), id); found {
		t.Fatal("passivation must unregister the directory entry")
	}
	// 重激活从 Store 恢复：计数连续（4）。
	p2, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatalf("re-activation after passivation: %v", err)
	}
	waitActive(t, p2, "re-activated pawnite")
	got, err := m.InvokeLocal(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("call after re-activation: %v", err)
	}
	if got != 4 {
		t.Fatalf("post-restore call = %v, want 4 (counter continuity)", got)
	}
	DestroyLocal(context.Background(), m, id)
}

// TestCollectIdle_SnapshotFailureNeverDestroys：快照捕获失败 → 该实体跳过
// 钝化（销毁不得发生——状态未落地），错误上抛汇总；实体保持 Active。
func TestCollectIdle_SnapshotFailureNeverDestroys(t *testing.T) {
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"broken": func(_ rans.ID) rans.Entity { return &brokenCaptureEntity{} },
	}, activation.Options{Store: checkpoint.New(), IdlePolicies: map[string]activation.IdlePolicy{"broken": activation.IdlePolicy{IdleAfter: time.Nanosecond, Mode: activation.CollectPassivate}}})

	id := idleID(t, "broken", "passive-broken")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-passivation pawnite")

	n, err := NewIdleCollector(m).CollectIdle(context.Background(), 10)
	if n != 0 {
		t.Fatalf("passivated = %d, want 0 (snapshot failure must skip)", n)
	}
	if err == nil {
		t.Fatal("snapshot failure must surface an error")
	}
	p, ok := m.GetLocal(id)
	if !ok || p.State() != pawnite.StateActive {
		t.Fatal("entity must stay locally Active when snapshot fails")
	}
	select {
	case <-p.Done():
		t.Fatal("entity must NOT be destroyed when snapshot fails")
	default:
	}
	DestroyLocal(context.Background(), m, id)
}

// TestCollectIdle_NoStoreRefusesStatefulPassivation：实体有状态（carrier）
// 但未注入 checkpoint Store → 拒绝钝化（错误上抛、不销毁）——无落点的状态
// 不得被丢弃。
func TestCollectIdle_NoStoreRefusesStatefulPassivation(t *testing.T) {
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &idleCountEntity{} },
	}, activation.Options{IdlePolicies: map[string]activation.IdlePolicy{"counter": activation.IdlePolicy{IdleAfter: time.Nanosecond, Mode: activation.CollectPassivate}}})
	id := idleID(t, "counter", "passive-nostore")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-passivation pawnite")

	n, err := NewIdleCollector(m).CollectIdle(context.Background(), 10)
	if n != 0 || err == nil {
		t.Fatalf("storeless passivation of stateful entity must refuse, got (%d, %v)", n, err)
	}
	if p, ok := m.GetLocal(id); !ok || p.State() != pawnite.StateActive {
		t.Fatal("entity must stay Active when store is not configured")
	}
	DestroyLocal(context.Background(), m, id)
}

// TestCollectIdle_MaxCountCap：单轮钝化数量上限——三个闲置实体、上限 2 →
// 最冷两个钝化、最热者（最后投递）留守本地。
func TestCollectIdle_MaxCountCap(t *testing.T) {
	m, _ := newIdleManager(activation.Options{IdlePolicies: map[string]activation.IdlePolicy{
		"counter": {IdleAfter: time.Nanosecond, Mode: activation.CollectPassivate},
	}})
	ids := []rans.ID{
		idleID(t, "counter", "passive-cap1"),
		idleID(t, "counter", "passive-cap2"),
		idleID(t, "counter", "passive-cap3"),
	}
	for _, id := range ids {
		if _, err := m.GetOrActivate(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	// 冷热差：cap1 最冷 → cap3 最热。
	for _, id := range ids {
		if _, err := m.InvokeLocal(context.Background(), id, nil); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond) // 全部越过纳秒级阈值

	n, err := NewIdleCollector(m).CollectIdle(context.Background(), 2)
	if err != nil {
		t.Fatalf("CollectIdle error = %v", err)
	}
	if n != 2 {
		t.Fatalf("passivated = %d, want 2 (maxCount cap)", n)
	}
	local := m.LocalIDs()
	if len(local) != 1 || local[0] != ids[2] {
		t.Fatalf("remaining local = %v, want [%v] (warmest survives the cap)", local, ids[2])
	}
	DestroyLocal(context.Background(), m, local[0])
}

// gateCaptureEntity：Handle 阻塞至放行；捕获时若 Handle 仍在途则记违规
// （捕获串行不变量的可观测化——捕获必须与 Handle 无并发）。
type gateCaptureEntity struct {
	inHandle          atomic.Bool
	release           chan struct{}
	captureConcurrent atomic.Bool
}

func (e *gateCaptureEntity) Activate(ctx context.Context) error   { return nil }
func (e *gateCaptureEntity) Deactivate(ctx context.Context) error { return nil }
func (e *gateCaptureEntity) Handle(ctx context.Context, req any) (any, error) {
	e.inHandle.Store(true)
	<-e.release
	e.inHandle.Store(false)
	return nil, nil
}
func (e *gateCaptureEntity) Snapshot() ([]byte, error) {
	if e.inHandle.Load() {
		e.captureConcurrent.Store(true)
	}
	return []byte("s"), nil
}
func (e *gateCaptureEntity) Restore(b []byte) error { return nil }

// TestCollectIdle_CaptureWaitsForInFlightHandle：钝化捕获经
// pawnite loop 任务执行——Handle 在途时捕获不得被调用（收集者 goroutine
// 若直接捕获，与在途 Handle 构成并发调用，单线程实体模型下即数据竞态）。
func TestCollectIdle_CaptureWaitsForInFlightHandle(t *testing.T) {
	store := checkpoint.New()
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &gateCaptureEntity{} },
	}, activation.Options{Store: store, IdlePolicies: map[string]activation.IdlePolicy{"counter": activation.IdlePolicy{IdleAfter: 20 * time.Millisecond, Mode: activation.CollectPassivate}}})

	id := idleID(t, "counter", "gate-cap")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-collect pawnite")
	entRaw, ok := m.EntityOf(id)
	if !ok {
		t.Fatal("entity not found after activation")
	}
	ent := entRaw.(*gateCaptureEntity)
	ent.release = make(chan struct{})

	// 占住 loop：Call 阻塞在 Handle 内
	callDone := make(chan error, 1)
	go func() {
		_, err := m.InvokeLocal(context.Background(), id, nil)
		callDone <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for !ent.inHandle.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !ent.inHandle.Load() {
		t.Fatal("handle not entered within 2s")
	}
	time.Sleep(40 * time.Millisecond) // 越过闲置阈值（时钟在 invoke 入口已记）

	collectDone := make(chan int, 1)
	go func() {
		n, err := NewIdleCollector(m).CollectIdle(context.Background(), 10)
		if err != nil {
			t.Errorf("CollectIdle error: %v", err)
		}
		collectDone <- n
	}()
	// Handle 仍阻塞：捕获不得发生
	time.Sleep(200 * time.Millisecond)
	if ent.captureConcurrent.Load() {
		t.Fatal("capture ran concurrently with an in-flight Handle (race)")
	}
	if _, found, _ := store.Get(context.Background(), id); found {
		t.Fatal("snapshot must not land while Handle is in flight")
	}
	close(ent.release)
	if err := <-callDone; err != nil {
		t.Fatalf("call: %v", err)
	}
	select {
	case n := <-collectDone:
		if n != 1 {
			t.Fatalf("passivated = %d, want 1", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CollectIdle not completed after Handle release")
	}
	if ent.captureConcurrent.Load() {
		t.Fatal("capture ran concurrently with an in-flight Handle (race)")
	}
	if snap, found, _ := store.Get(context.Background(), id); !found || string(snap) != "s" {
		t.Fatalf("store snapshot = (%q, %v), want s/true", snap, found)
	}
}
