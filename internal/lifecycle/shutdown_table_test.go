package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// stuckEntity 的 Deactivate 阻塞至 release 关闭：Shutdown 等待预算与并行
// 不阻断语义的测试道具。
type stuckEntity struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *stuckEntity) Activate(ctx context.Context) error               { return nil }
func (e *stuckEntity) Handle(ctx context.Context, req any) (any, error) { return nil, nil }
func (e *stuckEntity) Deactivate(ctx context.Context) error {
	e.once.Do(func() { close(e.entered) })
	<-e.release
	return nil
}

// activateAndWait 激活并轮询到 Active（Shutdown 前置确定性）。
func activateAndWait(t *testing.T, m *activation.Table, kind, inst string) *pawnite.Pawnite {
	t.Helper()
	id, err := rans.NewID(kind, inst)
	if err != nil {
		t.Fatal(err)
	}
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for p.State() != pawnite.StateActive {
		if p.State() == pawnite.StateDestroyed || time.Now().After(deadline) {
			t.Fatalf("pawnite %s state = %v, want Active", id.String(), p.State())
		}
		time.Sleep(time.Millisecond)
	}
	return p
}

// localSize 表大小断言面（原 readMu 白盒的协作面等价形态）：local =
// Locals() 快照长度；entity = 逐一探测已知在册身份的 EntityOf 计数
// ——local/entity 两表成对写（evict 同临界区删除），身份探测与白盒
// 计数对「entity 表残留」的覆盖等价。
func localSize(m *activation.Table, ids ...rans.ID) (int, int) {
	l := len(m.Locals())
	e := 0
	for _, id := range ids {
		if _, ok := m.EntityOf(id); ok {
			e++
		}
	}
	return l, e
}

// TestShutdown_AllDestroyed_MapsEmptyDoneClosed 收尾契约的绿色路径：N 实体
// （其一 Deactivate 卡死后放行）+ 足够长的 ctx → Shutdown 返回 nil，全部
// 实体 Destroyed、两张本地表清空、Done 通道全关（goroutine 泄漏测试可断言）。
func TestShutdown_AllDestroyed_MapsEmptyDoneClosed(t *testing.T) {
	stuck := &stuckEntity{entered: make(chan struct{}), release: make(chan struct{})}
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &counterEntity{} },
		"stuck":   func(_ rans.ID) rans.Entity { return stuck },
	})

	const n = 8
	ps := make([]*pawnite.Pawnite, 0, n)
	for i := 0; i < n-1; i++ {
		ps = append(ps, activateAndWait(t, m, "counter", "s1"))
	}
	ps = append(ps, activateAndWait(t, m, "stuck", "s2"))
	idS1, _ := rans.NewID("counter", "s1")
	idS2, _ := rans.NewID("stuck", "s2")

	done := make(chan error, 1)
	go func() { done <- Shutdown(context.Background(), m) }()

	select {
	case <-stuck.entered: // 屏障：卡死实体的 Deactivate 确已进入并占住
	case <-time.After(2 * time.Second):
		t.Fatal("stuck Deactivate never ran")
	case err := <-done:
		t.Fatalf("Shutdown returned early (err=%v) while Deactivate stuck", err)
	}
	close(stuck.release)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown err = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown never returned")
	}

	if l, e := localSize(m, idS1, idS2); l != 0 || e != 0 {
		t.Fatalf("local/entity size = %d/%d, want 0/0", l, e)
	}
	for _, p := range ps {
		if p.State() != pawnite.StateDestroyed {
			t.Fatalf("pawnite state = %v, want StateDestroyed", p.State())
		}
		select {
		case <-p.Done():
		default:
			t.Fatal("Done must be closed after Shutdown")
		}
	}
}

// TestShutdown_TightCtx_StuckEntityDoesNotBlockOthers 紧预算路径：卡死实体让
// Shutdown 返回 ctx.Err()（Joined 错误可 errors.Is），其余实体照常并行销毁、
// 表项清理；卡死实体的残留行为可断言（条目滞留至放行后由 loop 自主闭环）。
func TestShutdown_TightCtx_StuckEntityDoesNotBlockOthers(t *testing.T) {
	stuck := &stuckEntity{entered: make(chan struct{}), release: make(chan struct{})}
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &counterEntity{} },
		"stuck":   func(_ rans.ID) rans.Entity { return stuck },
	})

	others := make([]*pawnite.Pawnite, 0, 4)
	for i := 0; i < 4; i++ {
		others = append(others, activateAndWait(t, m, "counter", "t1"))
	}
	stuckP := activateAndWait(t, m, "stuck", "t2")
	idT1, _ := rans.NewID("counter", "t1")
	idT2, _ := rans.NewID("stuck", "t2")

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	err := Shutdown(ctx, m)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown err = %v, want joined context.DeadlineExceeded", err)
	}

	// 其余实体不受卡死者阻断：全部终态、Done 关闭、表项已清。
	for _, p := range others {
		if p.State() != pawnite.StateDestroyed {
			t.Fatalf("other pawnite state = %v, want StateDestroyed (stuck must not block others)", p.State())
		}
		select {
		case <-p.Done():
		default:
			t.Fatal("other Done must be closed")
		}
	}
	// 残留行为（文档化）：卡死实体条目滞留（Destroying 未终态），错误面带
	// 滞留计数；放行后 loop 自主走完 OnEvict→逐出，表最终清空。
	if l, _ := localSize(m); l != 1 {
		t.Fatalf("local size = %d, want 1 (stuck residual)", l)
	}
	if stuckP.State() != pawnite.StateDestroying {
		t.Fatalf("stuck pawnite state = %v, want StateDestroying", stuckP.State())
	}

	close(stuck.release)
	select {
	case <-stuckP.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("stuck pawnite never reached terminal after release")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		l, e := localSize(m, idT1, idT2)
		if l == 0 && e == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("local/entity size = %d/%d after release, want 0/0", l, e)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestShutdown_EmptyRegistryNil 空注册表收尾：nil 错误、零副作用。
func TestShutdown_EmptyRegistryNil(t *testing.T) {
	m := newTestTable(t)
	if err := Shutdown(context.Background(), m); err != nil {
		t.Fatalf("Shutdown on empty registry err = %v, want nil", err)
	}
}

// TestDestroyLocal_HonorsBudget 单实体 ctx-honoring 销毁的 manager 面（最终
// 审查 I1）：Deactivate 卡死时 DestroyLocal 按预算返回 DeadlineExceeded——
// Drainer 阶段②（Remote.Drain）的逐实体注入依赖此语义，单实体
// 卡死不得挂死整个优雅停机；不在册身份 no-op 返回 nil）。
func TestDestroyLocal_HonorsBudget(t *testing.T) {
	stuck := &stuckEntity{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(stuck.release) // 泄漏兜底：放行卡死的 Deactivate
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"stuck": func(_ rans.ID) rans.Entity { return stuck },
	})
	p := activateAndWait(t, m, "stuck", "d1")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := DestroyLocal(ctx, m, p.UUID())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DestroyLocal err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("DestroyLocal blocked %v, far beyond its budget", elapsed)
	}

	ghost, gerr := rans.NewID("stuck", "ghost")
	if gerr != nil {
		t.Fatal(gerr)
	}
	if err := DestroyLocal(context.Background(), m, ghost); err != nil {
		t.Fatalf("DestroyLocal on unknown id err = %v, want nil (no-op, same as Destroy)", err)
	}
}

// TestShutdown_UnknownIDIsNoOp 快照后已不在册的身份是 no-op（LocalIDs 语义
// 对齐）：Shutdown 对 miss 不报错。
func TestShutdown_UnknownIDIsNoOp(t *testing.T) {
	m := newTestTable(t)
	p := activateAndWait(t, m, "counter", "u1")
	p.Destroy(context.Background()) // 先单独销毁：Shutdown 快照前表已清
	if err := Shutdown(context.Background(), m); err != nil {
		t.Fatalf("Shutdown err = %v, want nil", err)
	}
}
