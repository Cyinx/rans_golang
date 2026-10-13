package lifecycle

// 恢复即落库失败路径的门控测试：
// 「恢复即落库」（spec §5.2 第 6 点）的实现分居两面——落库动作在
// internal/activation 激活闭包的 restorePackage 分支（包可能比 Store 新，
// Activate 前以本实例新纪元落库一次），失败路径的收口在 lifecycle 的首轮
// CheckpointSweep（Put 失败仅 Warn 不标记 → ckpt 纪元不抬 → 脏门放行 →
// 首轮 sweep 的 Put 补落库，围栏窗就此收口）。本文件从 lifecycle 面断言
// 两面接线的终值：激活不被落库失败阻断、Store 终值=实体状态、纪元簿记
// 恰在补落库后判干净。

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
)

// pkgLoader 随迁包载荷（命中即返回包字节——restoreSource=restorePackage
// 的恢复梯；consume-on-read 语义由真实 rpc 缓存承载，测试面恒命中即可）。
type pkgLoader struct{ payload []byte }

func (l *pkgLoader) LoadState(context.Context, rans.ID) ([]byte, uint64, bool) {
	return l.payload, 0, true
}

// sweepStore 记账型 Store：注入 N 次 Put 失败 + 终值/纪元台账（断言面）。
type sweepStore struct {
	mu       sync.Mutex
	stored   map[rans.ID][]byte // id → 已落库快照（终值断言）
	inc      map[rans.ID]uint64 // id → 最近落库纪元（fencing 携带面）
	putFails atomic.Int32       // 剩余注入失败数（Put 命中即失败一次）
	puts     atomic.Int32       // Put 调用总数（失败含内）
}

func (s *sweepStore) Put(_ context.Context, id rans.ID, incarnation uint64, snap []byte) error {
	s.puts.Add(1)
	if s.putFails.Add(-1) >= 0 {
		return errors.New("injected put failure (restore-persist window)")
	}
	s.mu.Lock()
	s.stored[id] = append([]byte(nil), snap...)
	s.inc[id] = incarnation
	s.mu.Unlock()
	return nil
}

func (s *sweepStore) Get(_ context.Context, id rans.ID) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.stored[id]
	return append([]byte(nil), b...), ok, nil
}

func (s *sweepStore) Delete(_ context.Context, id rans.ID) error {
	s.mu.Lock()
	delete(s.stored, id)
	delete(s.inc, id)
	s.mu.Unlock()
	return nil
}

// TestRestorePackagePersistFailureDefersToFirstSweep：随迁包恢复
// 后的立即落库（恢复即落库）失败 → 仅 Warn：①激活不阻断（尽力而为档）；
// ②ckpt 纪元不标记（脏门保持放行）；③首轮 CheckpointSweep 补落库，Store
// 终值=实体状态（包字节）；④补落库后纪元判干净（第二轮 sweep 零候选——
// 不把同内容反复重写）。
func TestRestorePackagePersistFailureDefersToFirstSweep(t *testing.T) {
	dir := inmem.New()
	packageState := []byte("pkg-state-v7")
	store := &sweepStore{stored: map[rans.ID][]byte{}, inc: map[rans.ID]uint64{}}
	store.putFails.Store(1) // 恢复即落库的那一次 Put 注入失败
	m := activation.New(dir, 1, map[string]rans.Factory{
		"carrier": func(_ rans.ID) rans.Entity { return &fakeCarrierEntity{state: packageState} },
	}, activation.Options{
		Store:       store,
		StateLoader: &pkgLoader{payload: packageState},
	})
	X := mustTestID(t, "carrier", "restore-persist")

	p, err := m.GetOrActivate(context.Background(), X)
	if err != nil {
		t.Fatalf("restore-persist Put failure must not block activation: %v", err)
	}
	waitActive(t, p, "restored pawnite")
	if got := store.puts.Load(); got != 1 {
		t.Fatalf("put attempts after activation = %d, want 1 (restore-persist attempted once, failed, warned)", got)
	}
	if _, ok := store.stored[X]; ok {
		t.Fatal("store must not hold a value after the failed restore-persist Put")
	}

	// ckpt 纪元未标记 ⟹ 首轮 sweep 判脏：补落库收口围栏窗。capture 的
	// Put 在收尾 goroutine 异步落库——有界等待其落地后再断言终值。
	if n := CheckpointSweep(context.Background(), m, 100); n != 1 {
		t.Fatalf("first sweep candidates = %d, want 1 (ckpt epoch unmarked — dirty gate must admit the compensation put)", n)
	}
	waitFor(t, "compensation put landing", func() bool {
		_, ok, _ := store.Get(context.Background(), X) // 锁内读（收尾 goroutine 并发写台账）
		return ok
	})
	b, ok, _ := store.Get(context.Background(), X)
	if !ok {
		t.Fatal("compensation put vanished after waitFor observed it (store mutated concurrently)")
	}
	if string(b) != string(packageState) {
		t.Fatalf("store final value = %q, want entity state (package bytes) persisted by first sweep", b)
	}
	if got := store.inc[X]; got == 0 {
		t.Fatalf("compensation put incarnation = %d, want nonzero (fencing carried)", got)
	}

	// 补落库后判干净：第二轮 sweep 零候选（闲置实体永不重写）。
	if n := CheckpointSweep(context.Background(), m, 100); n != 0 {
		t.Fatalf("second sweep candidates = %d, want 0 (epoch marked clean by first sweep)", n)
	}
}
