package lifecycle

// 表驱动编排测试的共用道具与轮询助手：
// 与 internal/activation 的同名助手是两包各自的副本（测试文件随被测复合体
// 分居两包，助手不跨包共享）——语义一致、断言面不弱化。

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// mustTestID 构造测试身份（失败即 Fatal）。
func mustTestID(t testing.TB, kind, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID(kind, inst)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// waitFor 带界轮询直至 cond 为真。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// waitActive 有界轮询等待实例进入 Active；进入 Destroyed 或超时即失败。
func waitActive(t *testing.T, p *pawnite.Pawnite, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for p.State() != pawnite.StateActive {
		if p.State() == pawnite.StateDestroyed || time.Now().After(deadline) {
			t.Fatalf("%s state = %v, want Active", what, p.State())
		}
		time.Sleep(time.Millisecond)
	}
}

// waitTerminalEviction 等待失败激活的终局收尾完成：本地表逐出（OnEvict →
// cleanup 已跑完）⟹ finish 的 Deactivate 包装已被经过，计数稳定可断言。
func waitTerminalEviction(t *testing.T, tbl *activation.Table, id rans.ID) {
	t.Helper()
	waitFor(t, "terminal cleanup (local eviction)", func() bool {
		_, ok := tbl.GetLocal(id)
		return !ok
	})
}

// counterEntity 计数实体（最小无状态道具；与 activation 侧同名道具同语义）。
type counterEntity struct {
	n int
}

func (c *counterEntity) Activate(ctx context.Context) error { return nil }
func (c *counterEntity) Handle(ctx context.Context, req any) (any, error) {
	c.n++
	return c.n, nil
}
func (c *counterEntity) Deactivate(ctx context.Context) error { return nil }

// newTestTable 最小测试表：inmem 目录 + counter 工厂。
func newTestTable(t *testing.T) *activation.Table {
	t.Helper()
	return activation.New(inmem.New(), 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &counterEntity{} },
	})
}

// ctxT 带预算的测试 ctx。
func ctxT(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// acctEntity 有状态实体：快照 = 余额十进制文本；非数字文本 = 形状不匹配。
type acctEntity struct {
	bal int
}

func (a *acctEntity) Activate(ctx context.Context) error   { return nil }
func (a *acctEntity) Deactivate(ctx context.Context) error { return nil }
func (a *acctEntity) Handle(_ context.Context, req any) (any, error) {
	s, _ := req.(string)
	switch {
	case strings.HasPrefix(s, "add:"):
		n, _ := strconv.Atoi(strings.TrimPrefix(s, "add:"))
		a.bal += n
		return a.bal, nil
	case s == "get":
		return a.bal, nil
	}
	return nil, errors.New("unknown request")
}

func (a *acctEntity) Snapshot() ([]byte, error) {
	return []byte(strconv.Itoa(a.bal)), nil
}

func (a *acctEntity) Restore(b []byte) error {
	n, err := strconv.Atoi(string(b))
	if err != nil {
		return err
	}
	a.bal = n
	return nil
}

// stubStateLoader 恒命中的装载器道具（随迁包快路径模拟；srcInc=0：未携带
// 版本的包形态——恢复侧退回包优先历史行为）。
type stubStateLoader struct {
	b  []byte
	ok bool
}

func (s *stubStateLoader) LoadState(ctx context.Context, id rans.ID) ([]byte, uint64, bool) {
	return s.b, 0, s.ok
}

// fakeCarrierEntity 同时实现 rans.Entity 最小面与 rans.Snapshotter（与
// activation 侧同名道具同语义：能力面经类型断言发现，Restore 可注入形状
// 不匹配）。
type fakeCarrierEntity struct {
	state  []byte
	broken bool // Restore 拒绝的形状
}

func (f *fakeCarrierEntity) Activate(ctx context.Context) error { return nil }
func (f *fakeCarrierEntity) Handle(ctx context.Context, req any) (any, error) {
	return nil, nil
}
func (f *fakeCarrierEntity) Deactivate(ctx context.Context) error { return nil }

func (f *fakeCarrierEntity) Snapshot() ([]byte, error) {
	if f.broken {
		return nil, errors.New("capture unavailable")
	}
	out := make([]byte, len(f.state))
	copy(out, f.state)
	return out, nil
}

func (f *fakeCarrierEntity) Restore(b []byte) error {
	if f.broken {
		return errors.New("shape mismatch") // 形状不匹配 → error → 上层退化 Store
	}
	f.state = append(f.state[:0], b...)
	return nil
}

// recStorePutDelay putDelay 注入的睡眠时长（慢周期 Put 模拟——终快照次序
// 测试的注入点，交接窗零丢失测试族使用）。
const recStorePutDelay = 200 * time.Millisecond

// recStore 可注入一次性 Get 失败、Put 延迟、记录 Put 结果的 Store 包装。
type recStore struct {
	checkpoint.Store
	getFails atomic.Int32
	// putDelay >0 时前 N 次 Put 进入即睡眠 recStorePutDelay（慢 IO 注入，
	// 模拟在飞周期 checkpoint 与终快照的次序竞争）。
	putDelay atomic.Int32
	// putEntered 非 nil 时首次 Put 进入即关闭（「Put 已在飞」的控制信号）。
	putEntered   chan struct{}
	putEnterOnce sync.Once
	mu           sync.Mutex
	putErrs      []error
}

func (s *recStore) Get(ctx context.Context, id rans.ID) ([]byte, bool, error) {
	if s.getFails.Add(-1) >= 0 {
		return nil, false, errors.New("transient: connection reset by peer")
	}
	return s.Store.Get(ctx, id)
}

func (s *recStore) Put(ctx context.Context, id rans.ID, incarnation uint64, snapshot []byte) error {
	if s.putEntered != nil {
		s.putEnterOnce.Do(func() { close(s.putEntered) })
	}
	if s.putDelay.Add(-1) >= 0 {
		select {
		case <-ctx.Done():
		case <-time.After(recStorePutDelay):
		}
	}
	err := s.Store.Put(ctx, id, incarnation, snapshot)
	s.mu.Lock()
	s.putErrs = append(s.putErrs, err)
	s.mu.Unlock()
	return err
}

func (s *recStore) puts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.putErrs)
}

// seedDurableState 预置持久历史：消耗 n 个纪元并把 val 作为最终快照落
// Store（模拟「该实体此前活过 n 个 incarnation」）。取号经 Register 的
// 注册-注销循环消耗（取号并入注册原子域），
// 同 node 收养路径每轮产出一个严格递增纪元。
func seedDurableState(t *testing.T, dir directory.Directory, base checkpoint.Store, id rans.ID, n uint64, val string) {
	t.Helper()
	ctx := context.Background()
	var last uint64
	for i := uint64(0); i < n; i++ {
		won, cur, err := dir.Register(ctx, id, directory.Owner{Node: 99}, nil)
		if err != nil || !won {
			t.Fatalf("seed register %d: won=%v err=%v", i, won, err)
		}
		if _, err := dir.UnregisterIfOwner(ctx, id, cur); err != nil {
			t.Fatal(err)
		}
		last = cur.Incarnation
	}
	if err := base.Put(ctx, id, last, []byte(val)); err != nil {
		t.Fatal(err)
	}
}

// noopEntity 最小无状态实体（与 activation 侧同名道具同语义）。
type noopEntity struct{}

func (noopEntity) Activate(context.Context) error           { return nil }
func (noopEntity) Handle(context.Context, any) (any, error) { return nil, nil }
func (noopEntity) Deactivate(context.Context) error         { return nil }

// newArbManager 仲裁/迁移测试基座：指定节点号与目录，kind "k" 用 noop 实体，
// 返回该 kind 下的一个身份（与 activation 侧同名助手同语义）。
func newArbManager(node rans.NodeID, dir directory.Directory, key string, opts ...activation.Options) (*activation.Table, rans.ID) {
	m := activation.New(dir, node, map[string]rans.Factory{
		"k": func(_ rans.ID) rans.Entity { return noopEntity{} },
	}, opts...)
	id, _ := rans.NewID("k", key)
	return m, id
}

// gatedUnregisterDir 注销门控目录（与 activation 侧同名道具同语义）：实例
// 注销进入即上报 entered，其中第一个调用停在 gate 上直到放行，其余直通——
// 用于确定性编排「旧实例的注销被延迟」的交错（强弃/销毁终局的清理路径）。
// 嵌入透传（inmem 具体实现满足）。
type gatedUnregisterDir struct {
	directory.Directory
	entered chan struct{}
	gate    chan struct{}

	mu    sync.Mutex
	gated bool
}

func (g *gatedUnregisterDir) UnregisterIfOwner(ctx context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	g.enterUnregisterGate()
	return g.Directory.UnregisterIfOwner(ctx, id, owner)
}

func (g *gatedUnregisterDir) enterUnregisterGate() {
	g.mu.Lock()
	first := !g.gated
	g.gated = true
	g.mu.Unlock()
	g.entered <- struct{}{}
	if first {
		<-g.gate
	}
}

// gatedGetStore 门控 Store（与 activation 侧同名道具同语义）：首个 Get 进入
// 即上报并停在 gate 上（放行后返回注入的 IO 错误）——把「投递先入箱、激活
// 后失败」的异步窗钉成确定性次序。
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

// signalEntity Activate 即失败的道具（与 activation 侧同名道具同语义）。
type signalEntity struct{}

func (e *signalEntity) Activate(ctx context.Context) error {
	return errors.New("signal: activation fails")
}
func (e *signalEntity) Handle(ctx context.Context, req any) (any, error) { return nil, nil }
func (e *signalEntity) Deactivate(ctx context.Context) error             { return nil }
