package activation

// 激活期状态恢复（restoreState）契约测试：Store 读失败（Get IO 错误 /
// 快照解码错误）即激活失败，绝不以空状态对外服务、更不得把空状态写回
// 持久层；未命中 → 裸激活（合法）。

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
)

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

// TestRestore_IOErrorFailsActivationAndKeepsDurableState：激活读 Store 时
// 瞬时 IO 失败 → 激活以 ErrActivationFailed 失败，持久状态不被空状态
// 覆盖，首轮周期 checkpoint 无从落盘（实体从未 Active）。
// 断言经 GetOrActivate 直调双窗处理（快速失败下 Call 的准入
// 窗会以 ErrBusy 形态收场——与本断言无关的第三窗，直调面避免之，形态
// 参照 deactivate_pairing_test.go）。
func TestRestore_IOErrorFailsActivationAndKeepsDurableState(t *testing.T) {
	dir := inmem.New()
	X := mustTestID(t, "acct", "y")
	base := checkpoint.New()
	seedDurableState(t, dir, base, X, 5, "1000")

	rec := &recStore{Store: base}
	rec.getFails.Store(1) // 激活读快照时一次瞬时 IO 失败
	m := New(dir, 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, Options{Store: rec})

	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()
	before := activationFailedCounter().Value()
	p, err := m.GetOrActivate(ctx, X)
	if err == nil {
		<-p.Done() // 在途形态：等失败终局（错误形态断言由错误窗覆盖）
	} else if !errors.Is(err, ErrActivationFailed) {
		t.Fatalf("期望 ErrActivationFailed（读失败即激活失败），得到 %v", err)
	}
	waitTerminalEviction(t, m, X)
	// 根因（ErrRestoreFailed）不跨投递面：drain-reject 异步窗只携带状态码
	// 与 reason 字符串（契约见 ErrRestoreFailed 注释），根因断言在
	// TestRestoreState_Classification 的直调层面钉。
	if b, _, _ := base.Get(ctx, X); string(b) != "1000" {
		t.Fatalf("持久状态被改写：%q", string(b))
	}
	if l := len(m.Locals()); l != 0 {
		t.Fatalf("首轮 checkpoint 候选 = %d，want 0（实体没活过——本地表必空，扫轮无从捕获）", l)
	}
	time.Sleep(20 * time.Millisecond) // 即便有在飞捕获也已完成收尾
	if rec.puts() != 0 {
		t.Fatalf("首轮 checkpoint 落盘 %d 次，want 0", rec.puts())
	}
	if got := activationFailedCounter().Value(); got != before+1 {
		t.Fatalf("rans.activation.failed 计数 %v，want %v（恰好一次）", got, before+1)
	}
}

// TestRestore_DecodeErrorFailsActivation：解码失败同 IO 失败——激活失败、
// Store 不动、无 checkpoint 落盘。GetOrActivate 直调双窗处理（同上）。
func TestRestore_DecodeErrorFailsActivation(t *testing.T) {
	dir := inmem.New()
	X := mustTestID(t, "acct", "d")
	base := checkpoint.New()
	seedDurableState(t, dir, base, X, 5, "!!") // 持久层遗留损坏快照

	rec := &recStore{Store: base}
	m := New(dir, 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, Options{Store: rec})

	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()
	p, err := m.GetOrActivate(ctx, X)
	if err == nil {
		<-p.Done() // 在途形态：等失败终局
	} else if !errors.Is(err, ErrActivationFailed) {
		t.Fatalf("期望 ErrActivationFailed（解码失败即激活失败），得到 %v", err)
	}
	waitTerminalEviction(t, m, X)
	if b, _, _ := base.Get(ctx, X); string(b) != "!!" {
		t.Fatalf("持久状态被改写：%q", string(b))
	}
	if l := len(m.Locals()); l != 0 {
		t.Fatalf("首轮 checkpoint 候选 = %d，want 0（实体没活过——本地表必空，扫轮无从捕获）", l)
	}
	if rec.puts() != 0 {
		t.Fatalf("首轮 checkpoint 落盘 %d 次，want 0", rec.puts())
	}
}

// TestRestore_MissBareActivation：Store 未命中 → 裸激活（合法：实体对
// 空状态自身负责）。
func TestRestore_MissBareActivation(t *testing.T) {
	dir := inmem.New()
	X := mustTestID(t, "acct", "m")
	m := New(dir, 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, Options{Store: checkpoint.New()})
	// 空 Store：恒未命中

	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()
	v, err := m.InvokeLocal(ctx, X, "get")
	if err != nil {
		t.Fatalf("未命中应裸激活成功：%v", err)
	}
	if v != 0 {
		t.Fatalf("裸激活余额 = %v，want 0", v)
	}
}

// errStore 表驱动的 Store 桩：可注入 Get IO 错误 / 命中字节 / 未命中。
type errStore struct {
	getErr error
	snap   []byte
	hit    bool
}

func (s *errStore) Get(context.Context, rans.ID) ([]byte, bool, error) {
	if s.getErr != nil {
		return nil, false, s.getErr
	}
	return s.snap, s.hit, nil
}
func (s *errStore) Put(context.Context, rans.ID, uint64, []byte) error { return nil }
func (s *errStore) Delete(context.Context, rans.ID) error              { return nil }

// stubStateLoader 固定返回注入字节的 StateLoader 桩。
type stubStateLoader struct {
	b  []byte
	ok bool
}

func (l *stubStateLoader) LoadState(context.Context, rans.ID) ([]byte, bool) {
	return l.b, l.ok
}

// TestRestoreState_Classification：包内直调 restoreState 的单元分类——
// Store IO 错误 / 解码错误 → ErrRestoreFailed 包装；未命中 → nil；随迁包
// 恢复失败 → 退回 Store（fallback 发生且成功 → restoreStore）；未接线任何
// 状态面 → nil。同时钉恢复来源分类：只有「Store 命中且恢复成功」（含包
// 恢复失败退回 Store 后的成功）为 restoreStore；「随迁包命中且恢复成功」
// 为 restorePackage 且携带包字节原样；其余路径恒 restoreNone。
func TestRestoreState_Classification(t *testing.T) {
	ctx := context.Background()
	X := mustTestID(t, "acct", "cls")
	ioErr := errors.New("store io down")

	t.Run("StoreGetIOError", func(t *testing.T) {
		m := New(inmem.New(), 1, nil, Options{Store: &errStore{getErr: ioErr}})

		ent := &acctEntity{}
		src, _, err := m.restoreState(ctx, X, ent)
		if !errors.Is(err, ErrRestoreFailed) || !errors.Is(err, ioErr) {
			t.Fatalf("期望 ErrRestoreFailed 且链上有原错误，得到 %v", err)
		}
		if src != restoreNone {
			t.Fatalf("失败路径恢复来源应为 restoreNone，得到 %v", src)
		}
		if ent.bal != 0 {
			t.Fatalf("实体不应被恢复：bal = %d", ent.bal)
		}
	})

	t.Run("SnapshotDecodeError", func(t *testing.T) {
		m := New(inmem.New(), 1, nil, Options{Store: &errStore{snap: []byte("!!"), hit: true}})

		ent := &acctEntity{}
		src, _, err := m.restoreState(ctx, X, ent)
		if !errors.Is(err, ErrRestoreFailed) {
			t.Fatalf("期望 ErrRestoreFailed（解码失败），得到 %v", err)
		}
		if src != restoreNone {
			t.Fatalf("失败路径恢复来源应为 restoreNone，得到 %v", src)
		}
	})

	t.Run("StoreMiss", func(t *testing.T) {
		m := New(inmem.New(), 1, nil, Options{Store: &errStore{}})

		ent := &acctEntity{}
		src, b, err := m.restoreState(ctx, X, ent)
		if err != nil {
			t.Fatalf("未命中应 nil（裸激活），得到 %v", err)
		}
		if src != restoreNone {
			t.Fatalf("未命中裸激活恢复来源应为 restoreNone，得到 %v", src)
		}
		if b != nil {
			t.Fatalf("restoreNone 不携带来源字节，得到 %d bytes", len(b))
		}
	})

	t.Run("MigrationPackageFallsBackToStore", func(t *testing.T) {
		m := New(inmem.New(), 1, nil, Options{
			Store:       &errStore{snap: []byte("1000"), hit: true},
			StateLoader: &stubStateLoader{b: []byte("not-a-number"), ok: true},
		})

		ent := &acctEntity{}
		src, b, err := m.restoreState(ctx, X, ent)
		if err != nil {
			t.Fatalf("包恢复失败应退回 Store 且成功，得到 %v", err)
		}
		if ent.bal != 1000 {
			t.Fatalf("fallback 未发生：bal = %d, want 1000（Store 值）", ent.bal)
		}
		if src != restoreStore {
			t.Fatalf("fallback 落到 Store 且恢复成功：恢复来源应为 restoreStore，得到 %v", src)
		}
		if b != nil {
			t.Fatalf("restoreStore 不携带来源字节，得到 %d bytes", len(b))
		}
	})

	t.Run("MigrationPackageHit", func(t *testing.T) {
		// 包命中即不读 Store：注入失败证明之
		m := New(inmem.New(), 1, nil, Options{
			Store:       &errStore{getErr: ioErr},
			StateLoader: &stubStateLoader{b: []byte("500"), ok: true},
		})

		ent := &acctEntity{}
		src, b, err := m.restoreState(ctx, X, ent)
		if err != nil {
			t.Fatalf("包命中且恢复成功应 nil，得到 %v", err)
		}
		if src != restorePackage {
			t.Fatalf("包命中且恢复成功：恢复来源应为 restorePackage，得到 %v", src)
		}
		if string(b) != "500" {
			t.Fatalf("restorePackage 应携带包字节原样，得到 %q", string(b))
		}
		if ent.bal != 500 {
			t.Fatalf("实体未从包恢复：bal = %d, want 500", ent.bal)
		}
	})

	t.Run("NoStateFacesWired", func(t *testing.T) {
		m := New(inmem.New(), 1, nil)
		ent := &acctEntity{}
		src, _, err := m.restoreState(ctx, X, ent)
		if err != nil {
			t.Fatalf("未接线任何状态面应 nil（裸激活），得到 %v", err)
		}
		if src != restoreNone {
			t.Fatalf("未接线任何状态面恢复来源应为 restoreNone，得到 %v", src)
		}
	})
}
