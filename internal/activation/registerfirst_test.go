package activation

// register-first 激活闭包契约测试：注册先于装载、取号与
// 注册同一原子域（INV3：令牌只发给注册成功的 owner）。输家（陈旧路由投机
// 激活）不运行任何用户代码——Activate 与 Deactivate 均零调用；赢家纪元是
// 全集群可观测的最大纪元（输家不再取号，无更大令牌外流）；owner 后续
// checkpoint Put 不再被输家毒化的围栏高水位拒绝（断言反面即
// ErrStaleIncarnation）。激活失败回绝码：《加固 spec》§5.2 第 4 点。注册之后的
// 失败（restore 失败）发生在执行之前——在箱投递以 StatusUnavailable 回绝，
// 本地调用方错误链同时携带 ErrActivationFailed 与 rans.ErrUnavailable 双哨兵。

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// rfToken 钩子侧观测到的 fencing 令牌：节点 + 钩子名 + 纪元。
type rfToken struct {
	node rans.NodeID
	hook string
	inc  uint64
}

// rfEntity register-first 形态实体：Activate/Deactivate 计数并记录钩子侧读到的
// fencing 纪元；writeOnActivate 模拟宿主文档推荐的围栏条件写（Activate
// 即以 ctx 令牌落自有存储）。配对约束下输家不跑 Deactivate，Activate 是
// 仅存的落库向量——本测试把条件写挂到 Activate 上钉死「输家零用户代码」
// 的全部毒化面。
type rfEntity struct {
	node            rans.NodeID
	id              rans.ID
	store           checkpoint.Store // 宿主自有围栏存储（与 manager checkpoint 同库）
	writeOnActivate bool

	activated   atomic.Int32
	deactivated atomic.Int32

	mu   sync.Mutex
	toks []rfToken
}

func (e *rfEntity) Activate(ctx context.Context) error {
	e.activated.Add(1)
	inc, _ := rans.Incarnation(ctx)
	e.mu.Lock()
	e.toks = append(e.toks, rfToken{node: e.node, hook: "Activate", inc: inc})
	e.mu.Unlock()
	if e.writeOnActivate && e.store != nil && inc > 0 {
		_ = e.store.Put(ctx, e.id, inc, []byte("loser-write"))
	}
	return nil
}

func (e *rfEntity) Deactivate(ctx context.Context) error {
	e.deactivated.Add(1)
	inc, _ := rans.Incarnation(ctx)
	e.mu.Lock()
	e.toks = append(e.toks, rfToken{node: e.node, hook: "Deactivate", inc: inc})
	e.mu.Unlock()
	return nil
}

func (e *rfEntity) Handle(_ context.Context, _ any) (any, error) { return nil, nil }

// 快照面（rans.Snapshotter）：owner 的周期 checkpoint 落库据此发生
// （CheckpointSweep 只对 carrier 实体捕获）；恢复为 no-op——本测试的
// Store 只作围栏高水位与 Put 结果的观测面。
func (e *rfEntity) Snapshot() ([]byte, error) { return []byte("owner-state"), nil }
func (e *rfEntity) Restore(b []byte) error    { return nil }

func (e *rfEntity) tokens() []rfToken {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]rfToken(nil), e.toks...)
}

// putErrsSnapshot 暴露 recStore 记录的 Put 结果（recStore 定义在
// restore_test.go，同包复用；owner 围栏断言的数据面）。
func (s *recStore) putErrsSnapshot() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.putErrs...)
}

// TestRegisterFirst_LoserZeroUserHooksAndOwnerFenceIntact 主断言：
// 两 manager 共享目录，赢家（node1）激活服役；输家（node2，陈旧路由投机
// 激活）零用户代码；赢家纪元不小于一切可观测纪元；owner 的 checkpoint
// Put 以自身纪元落库成功（不再被输家更大令牌毒化为 ErrStaleIncarnation）。
func TestRegisterFirst_LoserZeroUserHooksAndOwnerFenceIntact(t *testing.T) {
	dir := inmem.New()
	shared := checkpoint.New()
	rec := &recStore{Store: shared}
	X := mustTestID(t, "acct", "ex3")

	e1 := &rfEntity{node: 1, id: X}
	e2 := &rfEntity{node: 2, id: X, store: shared, writeOnActivate: true}
	m1 := New(dir, 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return e1 }}, Options{Store: rec})
	m2 := New(dir, 2, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return e2 }})

	ctx := context.Background()
	// 1) node1 激活并服役：注册取号原子域产出赢家纪元（首个 ≥1）。
	if _, err := m1.InvokeLocal(ctx, X, "ping"); err != nil {
		t.Fatal(err)
	}
	if got := e1.activated.Load(); got != 1 {
		t.Fatalf("winner Activate = %d, want 1", got)
	}
	toks := e1.tokens()
	if len(toks) != 1 || toks[0].hook != "Activate" || toks[0].inc < 1 {
		t.Fatalf("winner tokens = %+v, want single Activate token with inc >= 1", toks)
	}
	winnerInc := toks[0].inc

	// 2) 陈旧路由投机激活：node2 必输仲裁（条目已归 node1）——输家零用户代码。
	p2, err := m2.GetOrActivate(ctx, X)
	if err == nil {
		<-p2.Done() // 在途形态：等输家终局（错误形态断言由错误窗覆盖）
	} else {
		var lost *ArbitrationLostError
		if !errors.As(err, &lost) || lost.Winner.Node != 1 {
			t.Fatalf("loser err = %v, want ArbitrationLostError carrying winner node 1", err)
		}
		if !errors.Is(err, ErrActivationFailed) {
			t.Fatalf("loser err = %v, want ErrActivationFailed sentinel", err)
		}
	}
	waitTerminalEviction(t, m2, X)
	if got := e2.activated.Load(); got != 0 {
		t.Fatalf("loser Activate = %d, want 0（register-first：输家不运行任何用户代码）", got)
	}
	if got := e2.deactivated.Load(); got != 0 {
		t.Fatalf("loser Deactivate = %d, want 0", got)
	}

	// 3) 纪元面：全集群钩子观测到的令牌只有赢家的——输家零观测、无更大
	// 令牌外流（旧形态输家先取号再输仲裁，会以 inc=winner+1 跑用户钩子）。
	all := append(e1.tokens(), e2.tokens()...)
	for _, tk := range all {
		if tk.node != 1 {
			t.Fatalf("token %+v observed on non-winner node — user code ran on the loser", tk)
		}
		if tk.inc > winnerInc {
			t.Fatalf("token %+v exceeds winner inc %d — fencing poisoned", tk, winnerInc)
		}
	}

	// 4) owner 的 checkpoint Put 不再被 fence：以赢家纪元落库成功。
	//（CheckpointSweep 复合体已迁 internal/lifecycle——此处以表协作面 +
	// 捕获原语直驱同一链路的扫轮/落库段，断言面不变：脏判可捕获 ≈ 候选数、
	// Put 返回 nil ≈ errs[0]==nil。）
	p1, ok := m1.GetLocal(X)
	if !ok {
		t.Fatal("winner pawnite missing from local table")
	}
	if !m1.TryBeginCheckpoint(p1) {
		t.Fatal("checkpoint candidates = 0, want 1 (winner is active and dirty)")
	}
	ent1, ok := m1.EntityOf(X)
	if !ok {
		t.Fatal("winner entity missing")
	}
	carrier := ent1.(rans.Snapshotter)
	capDone := make(chan pawnite.CaptureOutcome, 1)
	if !p1.CaptureOnLoop(carrier.Snapshot, capDone) {
		t.Fatal("capture must be accepted on the active winner")
	}
	out := <-capDone
	if out.Skipped || out.Err != nil {
		t.Fatalf("capture outcome = %+v, want a landed snapshot", out)
	}
	if err := rec.Put(ctx, X, p1.Incarnation(), out.Snapshot); err != nil {
		t.Fatalf("owner checkpoint put = %v, want nil（owner 的 Put 不再被 ErrStaleIncarnation 拒绝）", err)
	}
	m1.EndCheckpoint(p1, out.Epoch)

	// 5) 配对收尾：赢家销毁恰一次 Deactivate（激活/卸载配对的正面锚）。
	_ = p1.Destroy(ctx) // 销毁入口已迁 lifecycle（DestroyLocal）：stop 面直驱同一终局闭环
	if got := e1.activated.Load(); got != 1 {
		t.Fatalf("winner Activate total = %d, want 1", got)
	}
	if got := e1.deactivated.Load(); got != 1 {
		t.Fatalf("winner Deactivate = %d, want 1", got)
	}
}

// gatedGetStore 门控 Store：首个 Get 进入即上报并停在 gate 上（放行后返回
// 注入的 IO 错误）——把「投递先入箱、激活后失败」的异步窗钉成确定性次序
// （Call 在 GetOrActivate 返回在途实例后完成准入，随后才放行失败）。
type gatedGetStore struct {
	inner   *checkpoint.InMemory
	entered chan struct{}
	gate    chan struct{}
	getErr  error
}

func newGatedGetStore(err error) *gatedGetStore {
	return &gatedGetStore{
		inner:   checkpoint.New(),
		entered: make(chan struct{}),
		gate:    make(chan struct{}),
		getErr:  err,
	}
}

func (s *gatedGetStore) Get(ctx context.Context, id rans.ID) ([]byte, bool, error) {
	close(s.entered)
	<-s.gate
	return nil, false, s.getErr
}

func (s *gatedGetStore) Put(ctx context.Context, id rans.ID, incarnation uint64, snapshot []byte) error {
	return s.inner.Put(ctx, id, incarnation, snapshot)
}

func (s *gatedGetStore) Delete(ctx context.Context, id rans.ID) error {
	return s.inner.Delete(ctx, id)
}

// TestActivationFailure_RejectsWithUnavailableDualSentinel 回绝码：
// restore 失败（Store Get IO 错误）→ 在箱投递被 drainReject(
// StatusUnavailable, "activation failed") 回绝 → 本地 m.Call 的错误链同时
// 携带 ErrActivationFailed 与 rans.ErrUnavailable（ErrBusy 同判——call.go 为
// ReasonActivationFailed 同判映射，统一形态）。
func TestActivationFailure_RejectsWithUnavailableDualSentinel(t *testing.T) {
	gs := newGatedGetStore(errors.New("transient: store get failed"))
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} },
	}, Options{Store: gs})

	X := mustTestID(t, "acct", "rejcode")

	res := make(chan error, 1)
	go func() {
		_, err := m.InvokeLocal(context.Background(), X, "get")
		res <- err
	}()
	<-gs.entered // 激活停在 restore 的 Store Get 上（实例 Activating、在途）
	waitFor(t, "delivery admitted to inbox", func() bool {
		p, ok := m.GetLocal(X)
		return ok && p.InboxLen() == 1
	})
	close(gs.gate) // 放行失败：激活失败 → 在箱投递按 Unavailable 回绝

	var err error
	select {
	case err = <-res:
	case <-time.After(5 * time.Second):
		t.Fatal("Call did not settle after activation failure")
	}
	if !errors.Is(err, ErrActivationFailed) {
		t.Fatalf("err = %v, want ErrActivationFailed sentinel", err)
	}
	if !errors.Is(err, rans.ErrUnavailable) {
		t.Fatalf("err = %v, want rans.ErrUnavailable（注册后失败=执行前确定性失败，Unavailable 回绝）", err)
	}
	// Reason 契约：Reason=ActivationFailed 是比双哨兵链更精确的判定面
	//（同步窗同 Code+Reason）。
	if rans.ReasonOf(err) != rans.ReasonActivationFailed {
		t.Fatalf("ReasonOf(err) = %d, want ReasonActivationFailed", rans.ReasonOf(err))
	}
	waitTerminalEviction(t, m, X)
}
