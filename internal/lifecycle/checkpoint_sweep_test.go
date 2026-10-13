package lifecycle

// 周期 checkpoint 的 manager 复合面（规格 §15：脏纪元门控——活动纪元 >
// 已 checkpoint 纪元，闲置实体永不重写；in-flight CAS——同实体 IO 永不
// 重叠；单轮上限）。

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// snapEntity 有状态实体：Handle 计数，快照回 "v<N>"（loop 静默点捕获——
// 契约收窄后无需自行加锁）。
type snapEntity struct {
	n        int
	restored string
}

func (e *snapEntity) Activate(ctx context.Context) error   { return nil }
func (e *snapEntity) Deactivate(ctx context.Context) error { return nil }
func (e *snapEntity) Handle(ctx context.Context, req any) (any, error) {
	e.n++
	return e.n, nil
}
func (e *snapEntity) Snapshot() ([]byte, error) {
	return []byte(fmt.Sprintf("v%d", e.n)), nil
}
func (e *snapEntity) Restore(b []byte) error {
	e.restored = string(b)
	return nil
}

// countingStore 计数封装：观测 Put 次数与最后内容。
type countingStore struct {
	mu    sync.Mutex
	inner *checkpoint.InMemory
	puts  int
	last  []byte
}

func (s *countingStore) Get(ctx context.Context, id rans.ID) ([]byte, bool, error) {
	return s.inner.Get(ctx, id)
}
func (s *countingStore) Put(ctx context.Context, id rans.ID, incarnation uint64, snap []byte) error {
	s.mu.Lock()
	s.puts++
	s.last = append([]byte(nil), snap...)
	s.mu.Unlock()
	return s.inner.Put(ctx, id, incarnation, snap)
}
func (s *countingStore) Delete(ctx context.Context, id rans.ID) error {
	return s.inner.Delete(ctx, id)
}

func newSnapManager(t *testing.T) (*activation.Table, *countingStore) {
	t.Helper()
	store := &countingStore{inner: checkpoint.New()}
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"snap": func(_ rans.ID) rans.Entity { return &snapEntity{} },
	}, activation.Options{Store: store})

	return m, store
}

// TestCheckpointSweepDirtyGating：激活后首轮捕获落库；无活动再扫不重写
// （脏纪元门控）；活动后再扫重写新值。
func TestCheckpointSweepDirtyGating(t *testing.T) {
	m, store := newSnapManager(t)
	id, _ := rans.NewID("snap", "c1")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	waitActive(t, p, "snap")
	if n := CheckpointSweep(context.Background(), m, 100); n != 1 {
		t.Fatalf("first sweep started = %d, want 1 (fresh activation is dirty)", n)
	}
	waitStorePuts(t, store, 1)
	if got := string(store.last); got != "v0" {
		t.Fatalf("first snapshot = %q, want v0", got)
	}

	// 无活动：脏门控拦截，不重写
	if n := CheckpointSweep(context.Background(), m, 100); n != 0 {
		t.Fatalf("idle sweep started = %d, want 0 (epoch unchanged)", n)
	}
	waitStorePutsSettle(t, store, 1)

	// 活动（Call 计数）后再扫：重写
	if _, err := m.InvokeLocal(context.Background(), id, 1); err != nil {
		t.Fatalf("call: %v", err)
	}
	if n := CheckpointSweep(context.Background(), m, 100); n != 1 {
		t.Fatalf("post-activity sweep started = %d, want 1", n)
	}
	waitStorePuts(t, store, 2)
	if got := string(store.last); got != "v1" {
		t.Fatalf("second snapshot = %q, want v1", got)
	}
}

// TestCheckpointSweepRoundCap：单轮上限截断（maxCount）。
func TestCheckpointSweepRoundCap(t *testing.T) {
	m, _ := newSnapManager(t)
	for i := 0; i < 3; i++ {
		id, _ := rans.NewID("snap", fmt.Sprintf("cap%d", i))
		p, err := m.GetOrActivate(context.Background(), id)
		if err != nil {
			t.Fatalf("activate %d: %v", i, err)
		}
		waitActive(t, p, "snap")
	}
	if n := CheckpointSweep(context.Background(), m, 2); n != 2 {
		t.Fatalf("capped sweep started = %d, want 2", n)
	}
}

func waitStorePuts(t *testing.T, s *countingStore, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		n := s.puts
		s.mu.Unlock()
		if n >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.mu.Lock()
	n := s.puts
	s.mu.Unlock()
	t.Fatalf("store puts = %d, want >= %d", n, want)
}

func waitStorePutsSettle(t *testing.T, s *countingStore, want int) {
	t.Helper()
	time.Sleep(150 * time.Millisecond) // 给潜在误写留窗口
	s.mu.Lock()
	n := s.puts
	s.mu.Unlock()
	if n != want {
		t.Fatalf("store puts settled = %d, want %d (idle entity rewritten)", n, want)
	}
}

// TestAwaitCheckpointSettled 交接窗 core 第一步（规格 §5.1 H6 第 2 点）：
// 在飞门清零前受预算约束（ctx 耗尽返回 DeadlineExceeded）、清零即返回 nil；
// 实例不在本地表视为已落定。在飞门经表的占门/还门协作面置位/清零
// （TryBeginCheckpoint/AbortCheckpoint——纯内存原子，无 IO）。
func TestAwaitCheckpointSettled(t *testing.T) {
	m, _ := newSnapManager(t)
	id, _ := rans.NewID("snap", "settle1")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	waitActive(t, p, "snap")

	// 无在飞：立即返回 nil。
	if err := m.AwaitCheckpointSettled(context.Background(), p); err != nil {
		t.Fatalf("no in-flight: err = %v, want nil", err)
	}

	// 人为置在飞门：先投递一次使实例变脏（TryBeginCheckpoint 的脏判可通过），
	// 占门后短预算等待 → 预算耗尽；AbortCheckpoint 清零后 → nil。
	if _, err := m.InvokeLocal(context.Background(), id, 1); err != nil {
		t.Fatalf("call (dirty the instance): %v", err)
	}
	if !m.TryBeginCheckpoint(p) {
		t.Fatal("instance must be dirty after a delivery (gate acquirable)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := m.AwaitCheckpointSettled(ctx, p); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("in-flight + exhausted budget: err = %v, want DeadlineExceeded", err)
	}
	m.AbortCheckpoint(p)
	if err := m.AwaitCheckpointSettled(context.Background(), p); err != nil {
		t.Fatalf("settled: err = %v, want nil", err)
	}

	// 实例不在本地表（未激活/已逐出）：无本 manager 名义在飞的 IO，视为已落定。
	foreign := pawnite.New(id, pawnite.Options{})
	if err := m.AwaitCheckpointSettled(context.Background(), foreign); err != nil {
		t.Fatalf("foreign instance: err = %v, want nil", err)
	}
}

// ---- 交接窗 × 在飞 checkpoint 的避让契约 ----
//
// 恶性链：CheckpointSweep CAS 置 ckptInFlight → CaptureOnLoop 入队（captureCh
// 容量 1）→ 同一瞬间交接窗开（pending+Destroying 同临界区）→ loop 的 select
// 对两就绪通道随机——先拾取 handoffCh 时 core（loop 任务）在 AwaitCheckpointSettled
// 轮询在飞门，而唯一能清门的排队捕获要等 core 让出 loop 才被拾取：自旋至预算
// 耗尽判输。契约由两半保证：runHandoff core 前排空排队捕获（Skipped 收场、收尾
// goroutine 归还门）+ CaptureOnLoop 的 Active 门拒绝窗内入队（startCheckpoint
// 就地归还门）。

// TestHandoffWindowDrainsQueuedCaptureBeforeCore（恶性半例：捕获已入队
// 未拾取 + 开窗）。loop 到达 select 时 captureCh 与 handoffCh 同时就绪、分支
// 随机（Go select 语义不可强制）——契约是「无论分支，迁移都在预算内赢」：
// 无避让时恶性分支烧预算判输；两分支恒毫秒级通过即证明避让生效。同时锚定
// 排空零副作用：被排空的周期捕获不落库
// （Skipped），唯一的 Put 来自交接 core 自身快照。
func TestHandoffWindowDrainsQueuedCaptureBeforeCore(t *testing.T) {
	ctx := context.Background()
	for i := 0; i < 16; i++ {
		var ent *stuckAcctEntity
		store := &migStore{}
		m := activation.New(inmem.New(), 1, map[string]rans.Factory{
			"acct": func(_ rans.ID) rans.Entity {
				ent = &stuckAcctEntity{entered: make(chan struct{}), release: make(chan struct{})}
				return ent
			},
		}, activation.Options{Store: store, Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 7, true }, MigrationBudget: 2 * time.Second})

		// 避让失效 = 烧预算判输：测试秒红
		X := mustTestID(t, "acct", fmt.Sprintf("ckpt-yield-%d", i))

		p, err := m.GetOrActivate(ctx, X)
		if err != nil {
			t.Fatalf("iter %d: activate: %v", i, err)
		}
		waitActive(t, p, fmt.Sprintf("iter %d pre-window", i))

		// 占住 loop（在途 Handle 期间 loop 无法拾取捕获），再走真实扫轮入口
		// 入队一轮周期捕获：CAS 置门 + captureCh 入队未拾取。
		callDone := make(chan error, 1)
		go func() {
			_, err := m.InvokeLocal(ctx, X, "add:1")
			callDone <- err
		}()
		<-ent.entered
		if n := CheckpointSweep(ctx, m, 10); n != 1 {
			t.Fatalf("iter %d: sweep started = %d, want 1", i, n)
		}
		// 在飞门已置的前置锚（等价白盒读：清零前 settle 等待必受预算约束）。
		settleCtx, settleCancel := context.WithTimeout(ctx, 20*time.Millisecond)
		if err := m.AwaitCheckpointSettled(settleCtx, p); !errors.Is(err, context.DeadlineExceeded) {
			settleCancel()
			t.Fatalf("iter %d: precondition ckptInFlight not held (settle err = %v, want DeadlineExceeded)", i, err)
		}
		settleCancel()
		if p.CaptureOnLoop(func() ([]byte, error) { return nil, nil }, make(chan pawnite.CaptureOutcome, 1)) {
			t.Fatalf("iter %d: capture must be queued unpicked (capacity-1 channel free?)", i)
		}

		// 窗开稳定（与 handoffCh 入队同临界区）后放行 Handle：loop 到达
		// select 时两通道同时就绪——分支随机，两分支都必须有界赢。
		res := make(chan error, 1)
		go func() { res <- Migrate(ctx, m, X) }()
		waitFor(t, fmt.Sprintf("iter %d window open", i), func() bool {
			return p.State() == pawnite.StateDestroying
		})
		close(ent.release)

		select {
		case err := <-res:
			if err != nil {
				t.Fatalf("iter %d: migration = %v, want nil (core must not self-lock on the queued capture)", i, err)
			}
		case <-time.After(4 * time.Second):
			t.Fatalf("iter %d: migration hung beyond budget", i)
		}
		waitTerminal(t, p, fmt.Sprintf("iter %d won migration", i))
		if _, ok := m.GetLocal(X); ok {
			t.Fatalf("iter %d: won migration must evict the local entry", i)
		}
		<-callDone
		// 排空零副作用：被排空的周期捕获不落库——唯一 Put 是交接 core 快照
		//（放行后的 add:1 已确认，bal=1）。
		puts := store.putSnapshot()
		if len(puts) != 1 || string(puts[0].snap) != "1" {
			t.Fatalf("iter %d: store puts = %+v, want single handoff snapshot \"1\" (drained capture must stay side-effect free)", i, puts)
		}
	}
}

// TestCheckpointGateReturnedWhenWindowRefusesCapture 结构补充锚（CAS 与入队
// 被抢占分隔的残余半例）：在飞门已置（CheckpointSweep 的 CAS 已落、
// startCheckpoint 的 CaptureOnLoop 未跑）时窗开——窗开后的迟到入队被
// Active 门拒绝，startCheckpoint 就地归还门，core 的 AwaitCheckpointSettled
// 不依赖 loop 即收敛。与 core 前 drain 构成结构性完备的两半；两处时序谁先
// 谁后裁决不变（门要么被排空半清除、要么被归还半清除）。
func TestCheckpointGateReturnedWhenWindowRefusesCapture(t *testing.T) {
	ctx := context.Background()
	var ent *stuckAcctEntity
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"acct": func(_ rans.ID) rans.Entity {
			ent = &stuckAcctEntity{entered: make(chan struct{}), release: make(chan struct{})}
			return ent
		},
	}, activation.Options{Store: &migStore{}, Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 7, true }, MigrationBudget: 2 * time.Second})

	X := mustTestID(t, "acct", "ckpt-gate-return")

	p, err := m.GetOrActivate(ctx, X)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	waitActive(t, p, "pre-window")

	// 占住 loop，经占门面置在飞门（模拟 CheckpointSweep 的 CAS 已落、收尾
	// CaptureOnLoop 未跑的被抢占切点——实体已因「add:1」排队而变脏，占门
	// 必成功；原白盒 inst.ckptInFlight.Store(true) 的协作面等价形态）。
	// captureCh 保持空——此后 loop 拾取 handoffCh 是唯一就绪分支（确定性）。
	go func() {
		_, _ = m.InvokeLocal(ctx, X, "add:1")
	}()
	<-ent.entered
	if !m.TryBeginCheckpoint(p) {
		t.Fatal("precondition: gate must be acquirable while the entity is dirty")
	}

	// 窗开稳定后「恢复」被抢占的收尾：CaptureOnLoop 窗内被 Active 门拒绝
	// → captureCheckpoint 归还门（不经 loop，core 的轮询随即收敛）。
	res := make(chan error, 1)
	go func() { res <- Migrate(ctx, m, X) }()
	waitFor(t, "window open", func() bool { return p.State() == pawnite.StateDestroying })
	close(ent.release)
	captureCheckpoint(ctx, m, p, ent, X)

	select {
	case err := <-res:
		if err != nil {
			t.Fatalf("migration = %v, want nil (refused capture must return the in-flight gate)", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("migration hung beyond budget")
	}
	waitTerminal(t, p, "won migration")
}
