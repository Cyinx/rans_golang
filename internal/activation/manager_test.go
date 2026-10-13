package activation

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

type counterEntity struct {
	n int
}

func (c *counterEntity) Activate(ctx context.Context) error { return nil }
func (c *counterEntity) Handle(ctx context.Context, req any) (any, error) {
	c.n++
	return c.n, nil
}
func (c *counterEntity) Deactivate(ctx context.Context) error { return nil }

func newTestManager(t *testing.T, opts ...Options) *Table {
	t.Helper()
	return New(inmem.New(), 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &counterEntity{} },
	}, opts...)
}

func TestGetOrActivate_UnknownKind(t *testing.T) {
	m := newTestManager(t)
	id, _ := rans.NewID("nope", "x")
	if _, err := m.GetOrActivate(context.Background(), id); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("err = %v, want ErrUnknownKind", err)
	}
}

func TestGetOrActivate_SameInstanceReused(t *testing.T) {
	m := newTestManager(t)
	id, _ := rans.NewID("counter", "c1")
	p1, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if p1 != p2 {
		t.Fatal("same uuid must reuse local activation")
	}
}

func TestGetOrActivate_LocalVisibleAfterActivation(t *testing.T) {
	m := newTestManager(t)
	id, _ := rans.NewID("counter", "c2")
	if _, err := m.GetOrActivate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.GetLocal(id); !ok {
		t.Fatal("activated pawnite must be locally visible")
	}
}

func TestConcurrentGetOrActivate_SingleActivation(t *testing.T) {
	m := newTestManager(t)
	id, _ := rans.NewID("counter", "c3")
	var mu sync.Mutex
	seen := map[*pawnite.Pawnite]int{}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := m.GetOrActivate(context.Background(), id)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			seen[p]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(seen) != 1 {
		t.Fatalf("distinct activations = %d, want 1", len(seen))
	}
}

func TestEvictionOnDestroyed(t *testing.T) {
	m := newTestManager(t)
	id, _ := rans.NewID("counter", "c4")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	// 销毁入口已迁 internal/lifecycle（DestroyLocal）——本表内测试以 pawnite
	// stop 面直驱同一终局闭环（finish→OnEvict→cleanup 同步，等终态语义同）。
	_ = p.Destroy(context.Background())
	<-p.Done()
	if _, ok := m.GetLocal(id); ok {
		t.Fatal("destroyed pawnite must be evicted from local registry")
	}
}

// ---- 激活失败不得毒化注册表 ----

// flakyState 跨实例共享：第一个实体的 Activate 失败，其后成功（fail-once）。
type flakyState struct {
	mu    sync.Mutex
	calls int
}

type flakyEntity struct {
	st *flakyState
}

func (e *flakyEntity) Activate(ctx context.Context) error {
	e.st.mu.Lock()
	defer e.st.mu.Unlock()
	e.st.calls++
	if e.st.calls == 1 {
		return errors.New("flaky: first activation fails")
	}
	return nil
}
func (e *flakyEntity) Handle(ctx context.Context, req any) (any, error) { return nil, nil }
func (e *flakyEntity) Deactivate(ctx context.Context) error             { return nil }

// blockingEntity 的 Activate 阻塞到 release 关闭后失败：钉死"在途返回 (p,nil) +
// 入表后 OnEvict 逐出"分支——身份守卫不得误跳过正主逐出。
type blockingEntity struct {
	release chan struct{}
}

func (e *blockingEntity) Activate(ctx context.Context) error {
	<-e.release
	return errors.New("blocked activation fails")
}
func (e *blockingEntity) Handle(ctx context.Context, req any) (any, error) { return nil, nil }
func (e *blockingEntity) Deactivate(ctx context.Context) error             { return nil }

func TestGetOrActivate_ActivationFailureDoesNotPoisonRegistry(t *testing.T) {
	st := &flakyState{}
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"flaky": func(_ rans.ID) rans.Entity { return &flakyEntity{st: st} },
	})
	id, _ := rans.NewID("flaky", "f1")

	p1, err1 := m.GetOrActivate(context.Background(), id)
	if err1 != nil {
		// 复检分支（Activate 同步快失败时命中）：错误身份钉死，且不得返回死实例。
		if !errors.Is(err1, ErrActivationFailed) {
			t.Fatalf("err = %v, want ErrActivationFailed", err1)
		}
		if p1 != nil {
			t.Fatal("ErrActivationFailed must not return a pawnite")
		}
	} else if p1 != nil {
		// 异步分支：在途实例返回后等终态。
		<-p1.Done()
	} else {
		t.Fatal("err1 == nil must come with a pawnite")
	}

	// 轮询兜底非确定性：重试必须成功并拿到全新可用实例（毒化不再发生）。
	var p2 *pawnite.Pawnite
	deadline := time.Now().Add(2 * time.Second)
	for {
		var err error
		p2, err = m.GetOrActivate(context.Background(), id)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrActivationFailed) {
			t.Fatalf("retry err = %v, want ErrActivationFailed", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("retry did not succeed before deadline")
		}
		time.Sleep(time.Millisecond)
	}
	if p1 != nil && p2 == p1 {
		t.Fatal("retry must return a fresh pawnite, not the poisoned dead one")
	}
	for p2.State() != pawnite.StateActive {
		if p2.State() == pawnite.StateDestroyed || time.Now().After(deadline) {
			t.Fatalf("fresh pawnite state = %v, want Active", p2.State())
		}
		time.Sleep(time.Millisecond)
	}
	if got, ok := m.GetLocal(id); !ok || got != p2 {
		t.Fatal("local registry must hold the fresh pawnite")
	}
}

// TestEvict_IdentityGuardRejectsStaleEviction 钉死身份守卫的两个方向：
// 陈旧实例的逐出不得误删正主条目；正主逐出必须生效。
func TestEvict_IdentityGuardRejectsStaleEviction(t *testing.T) {
	m := newTestManager(t)
	id, _ := rans.NewID("counter", "g1")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Destroy(context.Background())

	stale := pawnite.New(id, pawnite.Options{}) // 同 UUID 的陈旧实例
	m.evict(id, stale)                          // 陈旧逐出不得误删正主
	if _, ok := m.GetLocal(id); !ok {
		t.Fatal("stale evict must not remove the live entry")
	}
	m.evict(id, p) // 正主逐出生效
	if _, ok := m.GetLocal(id); ok {
		t.Fatal("owner evict must remove the entry")
	}
	if _, ok := m.EntityOf(id); ok {
		t.Fatal("owner evict must remove the entity entry too")
	}
}

// signalEntity：Activate 立即失败。终局握手信号在目录包装
// unregisterSignalDir 上（finish 置 Destroyed 后 cleanup 的第一步）——
// 配对约束下激活失败路径不再调用 Deactivate，旧「Deactivate 通道」
// 握手面已随其退役。
type signalEntity struct {
}

func (e *signalEntity) Activate(ctx context.Context) error {
	return errors.New("signal: activation fails")
}
func (e *signalEntity) Handle(ctx context.Context, req any) (any, error) { return nil, nil }
func (e *signalEntity) Deactivate(ctx context.Context) error             { return nil }

// TestGetOrActivate_ActivationFailureRecheckRemovesDeadEntry 确定性钉死复检分支
// （复检移除死条目的核心机制）：白盒编排"OnEvict 在入表前跑完"的交错。
// 握手设计（每步通道同步，无时序竞态）：
//  1. factory 在锁外（工厂出锁后的停点）放 seated 信号并停在 proceed 上
//     ——此刻 g1 不持任何 manager 锁；
//  2. 测试持 readMu 读锁后放行 proceed：g1 先过重锁双检（读锁与双检的
//     RLock 并存），New→Start 后停在入表的写锁上；run 侧 finish 推进到
//     evict 停在同一把锁前（StateDestroyed 已置位）；
//  3. 测试等 unregister 信号（cleanup 已进入，Destroyed 已置位）后放锁：
//     g1 入表即复检命中 StateDestroyed → 身份守卫移除 → 返回 ErrActivationFailed。
func TestGetOrActivate_ActivationFailureRecheckRemovesDeadEntry(t *testing.T) {
	ent := &signalEntity{}
	seated := make(chan struct{})
	proceed := make(chan struct{})
	dir := newUnregisterSignalDir()
	m := New(dir, 1, map[string]rans.Factory{
		"signal": func(_ rans.ID) rans.Entity {
			close(seated)
			<-proceed // g1 停在工厂（锁外），双检在其后
			return ent
		},
	})
	id, _ := rans.NewID("signal", "s1")

	g1done := make(chan struct{})
	var p1 *pawnite.Pawnite
	var err1 error
	go func() {
		defer close(g1done)
		p1, err1 = m.GetOrActivate(context.Background(), id)
	}()

	<-seated         // g1 已停在工厂（锁外），manager 锁上无人
	m.readMu.RLock() // 读锁占位：挡住入表/evict 的写锁，不挡双检的读锁
	close(proceed)
	<-dir.entered      // finish 已置 Destroyed，cleanup 进入（evict 停在锁前）
	m.readMu.RUnlock() // 放行：g1 入表 → 复检命中
	<-g1done

	if !errors.Is(err1, ErrActivationFailed) {
		t.Fatalf("err1 = %v, want ErrActivationFailed", err1)
	}
	if p1 != nil {
		t.Fatal("ErrActivationFailed must not return a pawnite")
	}
	if _, ok := m.GetLocal(id); ok {
		t.Fatal("recheck must remove the just-inserted dead entry")
	}
}

func TestGetOrActivate_ActivationFailureAsyncEvictsEntry(t *testing.T) {
	release := make(chan struct{})
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"blocker": func(_ rans.ID) rans.Entity { return &blockingEntity{release: release} },
	})
	id, _ := rans.NewID("blocker", "b1")

	p1, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatalf("in-flight activation must return the pawnite, got err = %v", err)
	}
	if _, ok := m.GetLocal(id); !ok {
		t.Fatal("in-flight pawnite must be locally visible")
	}
	close(release)
	<-p1.Done()
	if _, ok := m.GetLocal(id); ok {
		t.Fatal("post-insert OnEvict must remove the entry (identity guard must not skip the owner)")
	}
}

// TestFactoryPanicDoesNotStrandCreationLock：用户工厂（rans.Factory）
// panic 沿调用链传播（编程错误不吞），但不得滞留 m.mu——创建段若手工
// 解锁，panic 直接跳过解锁点，后续任何激活永久死锁（爆炸半径从
// 一个实体扩大到全表）。
func TestFactoryPanicDoesNotStrandCreationLock(t *testing.T) {
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"boom": func(_ rans.ID) rans.Entity { panic("factory bug") },
		"ok":   func(_ rans.ID) rans.Entity { return &counterEntity{} },
	})
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("factory panic must propagate to the caller")
			}
		}()
		boomID, _ := rans.NewID("boom", "x")
		_, _ = m.GetOrActivate(context.Background(), boomID)
	}()
	done := make(chan error, 1)
	go func() {
		okID, _ := rans.NewID("ok", "y")
		_, err := m.GetOrActivate(context.Background(), okID)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("subsequent activation err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("creation lock stranded by factory panic — manager-wide deadlock")
	}
}
