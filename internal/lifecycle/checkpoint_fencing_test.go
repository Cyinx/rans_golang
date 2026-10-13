package lifecycle

// X10 围栏测试：强弃换
// incarnation 后，旧实例在飞的 checkpoint Put 晚落时不得覆写新 incarnation
// 的快照。交错：旧实例首轮 sweep 的 Put 被门控 Store 扣住 → 复合强弃
// （执行卡死形态：Handle 卡死驱动 Abandon(StateActive)）→ 新
// incarnation 激活并完成二轮 Put → 放行旧 Put → 断言 Store 终态 = 新快照
// 且旧 Put 按 checkpoint.ErrStaleIncarnation 被拒。
// 本文件同载 fencing 面测试：incarnation 经 actCtx（Activate/Deactivate
// 钩子）与 Handle ctx 可读、跨 incarnation 单调。

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// fenceSnapEntity 共享实体（工厂复用同一实例）：Activate 计世代并记录
// 钩子侧读到的 fencing 纪元，快照内容按世代区分（gen<N>），Handle 记录
// ctx 侧纪元、可被卡死（驱动执行强弃）。
type fenceSnapEntity struct {
	gen       atomic.Int64
	handled   atomic.Bool // Handle 已进入卡死（一次性）
	block     chan struct{}
	blockUsed atomic.Bool

	actInc   atomic.Int64 // Activate 读到的 fencing 纪元
	deactInc atomic.Int64 // Deactivate 读到的 fencing 纪元
	hookOK   atomic.Bool  // 钩子侧 Fencing 返回 true（任一钩子）
	handleIn atomic.Int64 // Handle 读到的 fencing 纪元
}

func (e *fenceSnapEntity) Activate(ctx context.Context) error {
	e.gen.Add(1)
	if inc, ok := rans.Incarnation(ctx); ok {
		e.actInc.Store(int64(inc))
		e.hookOK.Store(true)
	}
	return nil
}
func (e *fenceSnapEntity) Deactivate(ctx context.Context) error {
	if inc, ok := rans.Incarnation(ctx); ok {
		e.deactInc.Store(int64(inc))
	}
	return nil
}
func (e *fenceSnapEntity) Handle(ctx context.Context, req any) (any, error) {
	if inc, ok := rans.Incarnation(ctx); ok {
		e.handleIn.Store(int64(inc))
	}
	if e.block != nil && e.blockUsed.CompareAndSwap(false, true) {
		e.handled.Store(true)
		<-e.block // 首个世代卡死 Handle：制造执行停驻（仅围栏交错测试置 block）
	}
	return e.gen.Load(), nil
}
func (e *fenceSnapEntity) Snapshot() ([]byte, error) {
	return []byte(fmt.Sprintf("gen%d", e.gen.Load())), nil
}
func (e *fenceSnapEntity) Restore(b []byte) error { return nil }

// gatedFenceStore 门控 Store：第一个 Put 扣住直至放行（旧 incarnation 的
// 在飞 IO），其余直通；done 逐次通知 Put 收场；staleRejects 记围栏拒绝数。
type gatedFenceStore struct {
	inner       *checkpoint.InMemory
	first       atomic.Bool
	pending     chan struct{} // 首个 Put 已到达
	gate        chan struct{} // 放行首个 Put
	done        chan struct{} // 每次 Put 收场（含被拒）
	putCalls    atomic.Int32
	staleReject atomic.Int32
}

func newGatedFenceStore() *gatedFenceStore {
	return &gatedFenceStore{
		inner:   checkpoint.New(),
		pending: make(chan struct{}),
		gate:    make(chan struct{}),
		done:    make(chan struct{}, 4),
	}
}

func (s *gatedFenceStore) Get(ctx context.Context, id rans.ID) ([]byte, bool, error) {
	return s.inner.Get(ctx, id)
}
func (s *gatedFenceStore) Put(ctx context.Context, id rans.ID, incarnation uint64, snap []byte) error {
	s.putCalls.Add(1)
	if s.first.CompareAndSwap(false, true) {
		close(s.pending)
		<-s.gate
	}
	err := s.inner.Put(ctx, id, incarnation, snap)
	if errors.Is(err, checkpoint.ErrStaleIncarnation) {
		s.staleReject.Add(1)
	}
	s.done <- struct{}{}
	return err
}
func (s *gatedFenceStore) Delete(ctx context.Context, id rans.ID) error {
	return s.inner.Delete(ctx, id)
}

// TestCheckpointSweep_StalePutCannotOverwriteNewIncarnation 围栏主断言：
// Store 终态必须是新 incarnation 的快照，且旧 Put 按围栏哨兵被拒（无围栏
// 时本测试红灯：终态 "gen1"——同纪元覆盖窗口实锤）。
func TestCheckpointSweep_StalePutCannotOverwriteNewIncarnation(t *testing.T) {
	ent := &fenceSnapEntity{block: make(chan struct{})}
	defer close(ent.block) // 收尾放行 zombie Handle（不泄漏 loop goroutine）

	store := newGatedFenceStore()
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"fencesnap": func(_ rans.ID) rans.Entity { return ent },
	}, activation.Options{Store: store})

	id, _ := rans.NewID("fencesnap", "fs1")
	p1, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "first incarnation active", func() bool { return p1.State() == pawnite.StateActive })

	// 旧 incarnation 首轮 checkpoint：Put(gen1) 被门控扣住（在飞）。
	if n := CheckpointSweep(context.Background(), m, 10); n != 1 {
		t.Fatalf("first sweep candidates = %d, want 1", n)
	}
	<-store.pending

	// 执行卡死形态强弃：Handle 卡死 → 复合强弃 → 目录注销 + 本地逐出。
	go func() { _, _ = m.InvokeLocal(context.Background(), id, "stick") }()
	waitFor(t, "stuck Handle entered", func() bool { return ent.handled.Load() })
	if !AbandonStuckExecution(m, id) {
		t.Fatal("composite exec abandon must win")
	}
	waitFor(t, "old instance evicted", func() bool { _, ok := m.GetLocal(id); return !ok })

	// 新 incarnation 激活并完成二轮 checkpoint：Put(gen2) 直通落库。
	p2, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if p2 == p1 {
		t.Fatal("reactivation must build a fresh pawnite")
	}
	waitFor(t, "second incarnation active", func() bool { return p2.State() == pawnite.StateActive })
	if n := CheckpointSweep(context.Background(), m, 10); n != 1 {
		t.Fatalf("second sweep candidates = %d, want 1", n)
	}
	<-store.done // 新快照已落

	// 放行旧 Put：晚落于新快照之后——必须被围栏拒绝（X10）。
	close(store.gate)
	<-store.done // 旧 Put 收场（被拒）

	if got := store.staleReject.Load(); got != 1 {
		t.Fatalf("stale rejections = %d, want 1 (old incarnation's late Put must be fenced)", got)
	}
	got, found, err := store.Get(context.Background(), id)
	if err != nil || !found {
		t.Fatalf("store get = %v, %v", found, err)
	}
	if string(got) != "gen2" {
		t.Fatalf("store final snapshot = %q, want %q (stale put from abandoned incarnation must not overwrite)", string(got), "gen2")
	}
}

// TestFencingCtxVisibleToHooksAndHandle fencing 面：实体 Activate/Deactivate
// 钩子（经 actCtx 挂值）与 Handle（经投递层 Deliver 包装）都能读到本
// incarnation 的纪元，且跨 incarnation 单调递增。
func TestFencingCtxVisibleToHooksAndHandle(t *testing.T) {
	ent := &fenceSnapEntity{} // block=nil：Handle 不卡死（本测试无强弃交错）
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"fencesnap": func(_ rans.ID) rans.Entity { return ent },
	})
	id, _ := rans.NewID("fencesnap", "fs2")

	p1, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "first incarnation active", func() bool { return p1.State() == pawnite.StateActive })
	if _, err := m.InvokeLocal(context.Background(), id, "ping"); err != nil {
		t.Fatal(err)
	}
	if !ent.hookOK.Load() {
		t.Fatal("Activate hook must see the fencing incarnation in ctx")
	}
	inc1 := ent.actInc.Load()
	h1 := ent.handleIn.Load()
	if inc1 <= 0 || h1 != inc1 {
		t.Fatalf("incarnation visible to activate=%d handle=%d, want equal and > 0", inc1, h1)
	}

	// 换代：优雅销毁 → 再激活，纪元必须更大，钩子/Handle 读到新值。
	DestroyLocal(context.Background(), m, id)
	if got := ent.deactInc.Load(); got != inc1 {
		t.Fatalf("Deactivate ctx incarnation = %d, want %d (same incarnation as activate)", got, inc1)
	}
	p2, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "second incarnation active", func() bool { return p2.State() == pawnite.StateActive })
	if _, err := m.InvokeLocal(context.Background(), id, "ping"); err != nil {
		t.Fatal(err)
	}
	if inc2 := ent.actInc.Load(); inc2 <= inc1 {
		t.Fatalf("incarnation across generations = %d then %d, want strictly increasing", inc1, inc2)
	}
	if h2 := ent.handleIn.Load(); h2 <= h1 {
		t.Fatalf("Handle ctx incarnation across generations = %d then %d, want strictly increasing", h1, h2)
	}
}
