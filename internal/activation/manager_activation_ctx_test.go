package activation

// 激活 ctx 解耦与生命周期钩子测试：激活预算不继承首调用者 ctx；
// onActivated/onEvicted 钩子接线点契约。

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// ctxGatedEntity：Activate 停在 release 上；放行后尊重激活 ctx——ctx 已被
// 取消则像真实 IO 实体一样中止激活。解耦正确时激活 ctx 不继承调用方取消，
// 放行后必然成功。
type ctxGatedEntity struct {
	release chan struct{}
}

func (e *ctxGatedEntity) Activate(ctx context.Context) error {
	<-e.release
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("activation aborted by ctx: %w", err)
	}
	return nil
}
func (e *ctxGatedEntity) Handle(ctx context.Context, req any) (any, error) {
	return "pong", nil
}
func (e *ctxGatedEntity) Deactivate(ctx context.Context) error { return nil }

// TestCallerCtxCancelDoesNotAbortActivation：激活中途调用方取消不得中止
// 激活——激活是节点级行为（实体加载+目录注册）非请求级行为，激活预算
// 不继承首调用者 ctx：pawnite 仍达 Active、目录已注册、
// 后续 Call 成功。
func TestCallerCtxCancelDoesNotAbortActivation(t *testing.T) {
	release := make(chan struct{})
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"gated": func(_ rans.ID) rans.Entity { return &ctxGatedEntity{release: release} },
	})
	id, _ := rans.NewID("gated", "ctx-1")

	callerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := m.GetOrActivate(callerCtx, id) // 在途激活：立即返回 (p, nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()       // 激活中途取消（Activate 仍阻塞在 release 上）
	close(release) // 放行激活

	waitActive(t, p, "caller-canceled activation")
	loc, found, err := m.dir.Lookup(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !found || loc.Node != 1 {
		t.Fatalf("directory = (%v, %v), want registered at node 1", loc, found)
	}
	// 后续调用（全新 ctx）在幸存的激活上成功。
	if _, err := m.InvokeLocal(context.Background(), id, nil); err != nil {
		t.Fatalf("post-cancel Call must succeed on the survived activation: %v", err)
	}
}

// ---- 生命周期钩子（Track/Untrack 接线点）----

// TestOnActivatedFiredAfterRegister：onActivated 在目录注册成功之后触发——
// 钩子内经共享目录 Lookup 已可观察到本节点注册（顺序钉死），且每次激活
// 恰触发一次。
func TestOnActivatedFiredAfterRegister(t *testing.T) {
	dir := inmem.New()
	activated := make(chan rans.ID, 4)
	m := New(dir, 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &counterEntity{} },
	}, Options{OnActivated: func(got rans.ID) {
		loc, found, err := dir.Lookup(context.Background(), got)
		if err != nil {
			t.Errorf("onActivated lookup: %v", err)
		} else if !found || loc.Node != 1 {
			t.Errorf("onActivated must observe completed registration, got (%v, %v)", loc, found)
		}
		activated <- got
	}})
	id, _ := rans.NewID("counter", "hook-act")

	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-activated:
		if got != id {
			t.Fatalf("onActivated id = %v, want %v", got, id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("onActivated did not fire")
	}
	waitActive(t, p, "hooked pawnite") // activate 闭包已返回，此后不可能再触发
	if len(activated) != 0 {
		t.Fatal("onActivated must fire exactly once per activation")
	}
}

// TestOnEvictedFiredAfterCleanup：onEvicted 在销毁完成、目录注销可观察
// （Lookup miss）、本地条目已逐出（GetLocal miss）之后触发——顺序钉死。
func TestOnEvictedFiredAfterCleanup(t *testing.T) {
	dir := inmem.New()
	evicted := make(chan rans.ID, 4)
	var m *Table // 闭包经变量捕获引用自身表（构造期声明，触发点在 Start 后）
	m = New(dir, 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &counterEntity{} },
	}, Options{OnEvicted: func(got rans.ID) {
		if _, found, err := dir.Lookup(context.Background(), got); err != nil {
			t.Errorf("onEvicted lookup: %v", err)
		} else if found {
			t.Error("onEvicted must fire after directory unregister (Lookup must miss)")
		}
		if _, ok := m.GetLocal(got); ok {
			t.Error("onEvicted must fire after local eviction (GetLocal must miss)")
		}
		evicted <- got
	}})
	id, _ := rans.NewID("counter", "hook-evict")

	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "hooked pawnite")
	_ = p.Destroy(context.Background()) // 同步等终态，返回 ⟹ OnEvict 已跑完（DestroyLocal 已迁 lifecycle）
	select {
	case got := <-evicted:
		if got != id {
			t.Fatalf("onEvicted id = %v, want %v", got, id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("onEvicted did not fire")
	}
}

// TestOnActivatedHook_ReentrantSafe：钩子重入 GetOrActivate（他 ID 全新
// 激活 + 自 ID 复用）不得死锁——钩子在不持 manager 锁的 run goroutine 上
// 被调，全部动作须在有界期限内完成（卡死则 waitActive 超时失败）。
func TestOnActivatedHook_ReentrantSafe(t *testing.T) {
	idA, _ := rans.NewID("counter", "re-act-a")
	idB, _ := rans.NewID("counter", "re-act-b")
	var m *Table // 闭包经变量捕获引用自身表（构造期声明，触发点在 Start 后）
	m = newTestManager(t, Options{OnActivated: func(id rans.ID) {
		if _, err := m.GetOrActivate(context.Background(), idB); err != nil {
			t.Errorf("reentrant GetOrActivate(other): %v", err)
		}
		if _, err := m.GetOrActivate(context.Background(), id); err != nil {
			t.Errorf("reentrant GetOrActivate(self): %v", err)
		}
	}})

	p, err := m.GetOrActivate(context.Background(), idA)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "activating pawnite") // Active ⟹ 钩子（含其内重入调用）已返回
	pb, ok := m.GetLocal(idB)
	if !ok {
		t.Fatal("re-entrant GetOrActivate(other) must have activated idB")
	}
	waitActive(t, pb, "re-entrantly activated pawnite")
}

// TestOnEvictedHook_ReentrantSafe：钩子重入 Destroy（自 ID——条目已逐出，
// 必须无阻塞返回）与 GetOrActivate（自 ID 重激活，走完整创建+注册闭环）
// 不得死锁（有界期限内完成）。
func TestOnEvictedHook_ReentrantSafe(t *testing.T) {
	dir := inmem.New()
	var m *Table // 闭包经变量捕获引用自身表（构造期声明，触发点在 Start 后）
	m = New(dir, 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &counterEntity{} },
	}, Options{OnEvicted: func(got rans.ID) {
		if dp, ok := m.GetLocal(got); ok {
			_ = dp.Destroy(context.Background()) // 已逐出时本分支不可达：no-op 返回
		}
		if _, err := m.GetOrActivate(context.Background(), got); err != nil {
			t.Errorf("reentrant GetOrActivate after evict: %v", err)
		}
	}})
	id, _ := rans.NewID("counter", "re-evict")

	p1, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p1, "first pawnite")

	destroyDone := make(chan struct{})
	go func() {
		defer close(destroyDone)
		_ = p1.Destroy(context.Background()) // OnEvict → cleanup → 钩子（重激活在钩子内完成）
	}()
	select {
	case <-destroyDone:
	case <-time.After(3 * time.Second):
		t.Fatal("re-entrant onEvicted hook deadlocked (Destroy did not return)")
	}

	p2, ok := m.GetLocal(id)
	if !ok {
		t.Fatal("re-entrant GetOrActivate must have re-activated the id")
	}
	if p2 == p1 {
		t.Fatal("re-activation must produce a fresh pawnite")
	}
	waitActive(t, p2, "re-activated pawnite")
}

// TestOnEvictedHook_ReentrantAfterActivationFailure：激活失败路径（复检
// 分支与 OnEvict 竞争 cleanupOnce）上的钩子重入也不得死锁——复检分支的
// cleanup 在释放创建锁之后执行（钩子锁外不变量，见 GetOrActivate 注释），
// 钩子内 GetOrActivate 全程有界完成（本测试以 deadline 断言钉死）。
func TestOnEvictedHook_ReentrantAfterActivationFailure(t *testing.T) {
	st := &flakyState{}
	evicted := make(chan rans.ID, 4)
	var m *Table // 闭包经变量捕获引用自身表（构造期声明，触发点在 Start 后）
	m = New(inmem.New(), 1, map[string]rans.Factory{
		"flaky": func(_ rans.ID) rans.Entity { return &flakyEntity{st: st} },
	}, Options{OnEvicted: func(got rans.ID) {
		if _, err := m.GetOrActivate(context.Background(), got); err != nil {
			t.Errorf("reentrant GetOrActivate after failed activation: %v", err)
		}
		evicted <- got
	}})
	id, _ := rans.NewID("flaky", "re-fail")

	type result struct {
		p   *pawnite.Pawnite
		err error
	}
	resc := make(chan result, 1)
	go func() {
		p, err := m.GetOrActivate(context.Background(), id)
		resc <- result{p, err}
	}()
	select {
	case r := <-resc:
		if r.err != nil {
			if !errors.Is(r.err, ErrActivationFailed) {
				t.Fatalf("err = %v, want ErrActivationFailed", r.err)
			}
		} else {
			<-r.p.Done() // 在途返回形态：等终态
		}
	case <-time.After(3 * time.Second):
		t.Fatal("activation-failure path deadlocked with re-entrant onEvicted hook")
	}
	select {
	case <-evicted:
	case <-time.After(2 * time.Second):
		t.Fatal("onEvicted did not fire after failed activation")
	}
	// 钩子内重激活（fail-once 的第二次激活成功）最终可见且 Active。
	deadline := time.Now().Add(2 * time.Second)
	for {
		p2, ok := m.GetLocal(id)
		if ok {
			if p2.State() == pawnite.StateActive {
				break
			}
			if p2.State() == pawnite.StateDestroyed {
				t.Fatal("re-activated pawnite reached Destroyed")
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("re-activated instance did not reach Active before deadline")
		}
		time.Sleep(time.Millisecond)
	}
}
