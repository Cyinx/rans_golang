package lifecycle

// 陈旧清理异步化 + 实例级销毁面 + 续租接线收口四锚点：
//
//  1. TestStaleCleanupAsync_RetireWindowTerminalOrder——优雅销毁的终局次序
//     （spec §5.1 第 4 点：注销先于 Destroyed 翻转）+ 实例级条件注销的
//     owner 匹配收场 + 终局后新纪元激活落位（「新激活不被旧注销阻塞」
//     由交接窗次序承担——同 id 重投在窗内被留箱/回绝，异步化的
//     存续面见测试头注）。
//  2. TestDestroyCtxInstance_EpochGuarded——实例级销毁面：纪元匹配才销毁，
//     不匹配/不在册 no-op 返回 nil（心跳 OnLost 的消费侧契约）。
//  3. TestRegisterYieldGuard_GhostUnregisteredAfterConcurrentDestroy——
//     注册后让位守卫的测试锚：门控 Register
//     + 强弃并发 + 开 gate → 幽灵条目就地注销恰一次（Lookup miss、无二次
//     注销）；不看门狗时钟。

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// ---- 锚点 1：陈旧清理异步化 ----

// TestStaleCleanupAsync_RetireWindowTerminalOrder：优雅
// 销毁走 Retire 交接窗，终局次序按 spec §5.1 第 4 点执行——用户
// Deactivate → 实例级条件注销 → 在箱回绝 → Destroyed。注销在 Destroyed
// 翻转**之前**（旧 stop 面的 finish 是翻转在前）：注销在飞期间实例停驻
// Destroying——「新实例的 Activate 不与本实例的 Deactivate 重叠」正是该
// 次序的规格意图，同 id 重投由交接窗的留箱/回绝语义承担（回绝可重试，
// 重试时目录已空落新激活）。本测试锁定：
//   - 注销在飞时终态未翻转（次序不变量）；
//   - 放行后条件注销按 owner 匹配收场、终态翻转、本地逐出；
//   - 终局后的全新激活取得新纪元实例，条目正常落位（条件注销已随旧实例
//     单胜完成，不存在迟到注销误删新条目的窗口）。
//
// H7 异步化语义在新次序下的存续面：读路径零 IO 不等注销（GetLocal 对
// Destroyed 即逐即走）+ 强弃路径先翻转终态后清理（abandon 单点次序）。
func TestStaleCleanupAsync_RetireWindowTerminalOrder(t *testing.T) {
	inner := inmem.New()
	gdir := &gatedUnregisterDir{
		Directory: inner,
		entered:   make(chan struct{}, 8),
		gate:      make(chan struct{}),
	}
	m, id := newArbManager(1, gdir, "stale-async")
	p1, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p1, "first pawnite")
	inc1 := p1.Incarnation()
	if loc, found, _ := inner.Lookup(context.Background(), id); !found || loc.Node != 1 {
		t.Fatalf("precondition: first activation must register at self, got (%v, %v)", loc, found)
	}

	destroyDone := make(chan struct{})
	go func() {
		defer close(destroyDone)
		DestroyLocal(context.Background(), m, id) // Retire 窗：终局收口停在 gate 上的注销
	}()
	<-gdir.entered // P1 的注销已进入且被阻塞（终局次序：注销先于 Destroyed 翻转）
	if st := p1.State(); st != pawnite.StateDestroying {
		t.Fatalf("state while unregister in flight = %v, want Destroying (spec §5.1 #4 disposal order)", st)
	}

	close(gdir.gate) // 放行注销：条目仍属本实例 → 实例级条件注销删除
	<-destroyDone
	if st := p1.State(); st != pawnite.StateDestroyed {
		t.Fatalf("state after unregister = %v, want Destroyed", st)
	}
	if _, found, _ := inner.Lookup(context.Background(), id); found {
		t.Fatal("directory entry must be unregistered after the retire completes (owner matched)")
	}

	// 终局后的激活：全新实例、新纪元，条目重新落位。
	p2, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p2, "second pawnite")
	if p2 == p1 {
		t.Fatal("post-terminal activation must build a fresh pawnite")
	}
	if inc2 := p2.Incarnation(); inc2 <= inc1 {
		t.Fatalf("incarnation across generations = %d then %d, want strictly increasing", inc1, inc2)
	}
	if loc, found, _ := inner.Lookup(context.Background(), id); !found || loc.Node != 1 {
		t.Fatalf("new instance's directory entry = (%v, %v), want node 1", loc, found)
	}
}

// ---- 锚点 2：实例级销毁面 ----

// TestDestroyCtxInstance_EpochGuarded 纪元匹配才销毁；不匹配与不在册 no-op
// 返回 nil；匹配路径走完整销毁闭环（终态 + 目录注销 + 本地逐出）。
func TestDestroyCtxInstance_EpochGuarded(t *testing.T) {
	dir := inmem.New()
	m, id := newArbManager(1, dir, "destroy-instance")
	p1, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p1, "pawnite")
	inc := p1.Incarnation()
	if inc == 0 {
		t.Fatal("incarnation must be set after register-first activation")
	}

	// 纪元不匹配（陈旧 OnLost / 迟到报告）：no-op 成功，活实例与目录条目不受扰。
	if err := DestroyInstance(context.Background(), m, id, inc+1); err != nil {
		t.Fatalf("mismatched epoch must be a no-op success, got %v", err)
	}
	if st := p1.State(); st != pawnite.StateActive {
		t.Fatalf("state = %v after mismatched-epoch destroy, want Active (untouched)", st)
	}
	if _, found, _ := dir.Lookup(context.Background(), id); !found {
		t.Fatal("directory entry must survive mismatched-epoch destroy")
	}

	// 不在册 id：no-op 返回 nil（与 DestroyLocal 同口径）。
	other, _ := rans.NewID("k", "destroy-instance-other")
	if err := DestroyInstance(context.Background(), m, other, 1); err != nil {
		t.Fatalf("unknown id must be a no-op success, got %v", err)
	}

	// 纪元匹配：完整销毁闭环。
	if err := DestroyInstance(context.Background(), m, id, inc); err != nil {
		t.Fatalf("matched-epoch destroy: %v", err)
	}
	<-p1.Done()
	if loc, found, _ := dir.Lookup(context.Background(), id); found {
		t.Fatalf("directory entry must be unregistered after matched-epoch destroy, still %v", loc)
	}
	if _, ok := m.GetLocal(id); ok {
		t.Fatal("local entry must be evicted after matched-epoch destroy")
	}
}

// ---- 锚点 3：注册后让位守卫 ----

// yieldGuardDir 门控 Register 的让位守卫观测目录：Register 进入即上报并停在
// gate 上（放行后委托 inmem 真注册）；实例级注销计数，首次调用经通道上报
// （守卫注销完成的确定性握手）。注销面即实例级 UnregisterIfOwner
// 单形态。
type yieldGuardDir struct {
	directory.Directory
	regEntered chan struct{}
	regGate    chan struct{}
	unregDone  chan struct{}

	unregCalls atomic.Int32
}

func (d *yieldGuardDir) Register(ctx context.Context, id rans.ID, self directory.Owner, expected *directory.Owner) (bool, directory.Owner, error) {
	close(d.regEntered)
	<-d.regGate
	return d.Directory.Register(ctx, id, self, expected)
}

func (d *yieldGuardDir) UnregisterIfOwner(ctx context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	d.unregCalls.Add(1)
	ok, err := d.Directory.UnregisterIfOwner(ctx, id, owner)
	d.unregDone <- struct{}{}
	return ok, err
}

// TestRegisterYieldGuard_GhostUnregisteredAfterConcurrentDestroy 注册 IO 在飞
// 期间实例被强弃：cleanup 以 owner=nil 跑完——**零目录调用**（无目录主张
// 的实例不做节点粒度回落清扫，残留由下次激活收养 + 租约 TTL 自愈；本断言
// 即该行为锚）；僵尸 Register 迟回赢了 →
// 注册后让位守卫就地把幽灵条目以实例级标记注销恰一次——Lookup miss、
// 无幽灵注册、无二次注销。
func TestRegisterYieldGuard_GhostUnregisteredAfterConcurrentDestroy(t *testing.T) {
	inner := inmem.New()
	d := &yieldGuardDir{
		Directory:  inner,
		regEntered: make(chan struct{}),
		regGate:    make(chan struct{}),
		unregDone:  make(chan struct{}, 4),
	}
	m, id := newArbManager(1, d, "yield-guard")

	pCh := make(chan *pawnite.Pawnite, 1)
	go func() {
		p, err := m.GetOrActivate(context.Background(), id)
		if err != nil {
			t.Error(err)
			pCh <- nil
			return
		}
		pCh <- p
	}()
	<-d.regEntered // 注册原子域停在 gate 上（实例 Activating、注册未回）
	waitFor(t, "instance seated in local registry", func() bool {
		_, ok := m.GetLocal(id)
		return ok
	})

	// 强弃并发（不经看门狗时钟，直调复合体）：终态翻转 + OnEvict→cleanup
	// 同步收口——此刻 owner 未捕获，cleanup 零目录调用（回落面已删除）。
	if !AbandonStuckActivation(m, id) {
		t.Fatal("abandon must take the gate (instance was Activating)")
	}
	if n := d.unregCalls.Load(); n != 0 {
		t.Fatalf("cleanup directory calls with owner uncaptured = %d, want 0 (fallback face removed; residue self-heals via next activation + lease TTL)", n)
	}

	close(d.regGate) // 僵尸注册迟回：赢 → 让位守卫就地把幽灵条目注销
	<-d.unregDone
	if loc, found, _ := inner.Lookup(context.Background(), id); found {
		t.Fatalf("ghost registration must not survive the yield guard, got %v", loc)
	}
	if n := d.unregCalls.Load(); n != 1 {
		t.Fatalf("instance-level unregister calls = %d, want 1（守卫恰一次，无二次注销）", n)
	}

	p := <-pCh
	if p == nil {
		t.Fatal("in-flight form must return the pawnite")
	}
	<-p.Done() // 强弃终局（让位返回后 zombie loop 自然退出，无二次效应）
	if _, ok := m.GetLocal(id); ok {
		t.Fatal("abandoned instance must be evicted")
	}
}

// leaseProbeEntity 最小 carrier 实体（带快照面：restoreState 走 Store.Get
// 路径的装载编排面）。
type leaseProbeEntity struct{}

func (e *leaseProbeEntity) Activate(context.Context) error           { return nil }
func (e *leaseProbeEntity) Handle(context.Context, any) (any, error) { return nil, nil }
func (e *leaseProbeEntity) Deactivate(context.Context) error         { return nil }
func (e *leaseProbeEntity) Snapshot() ([]byte, error)                { return nil, nil }
func (e *leaseProbeEntity) Restore([]byte) error                     { return nil }
