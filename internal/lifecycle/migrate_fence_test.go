package lifecycle

// 换锁围栏时点测试（《加固 spec》§5.2 第 6 点）：Store 只按 per-id
// 已接受最大纪元判定、不读目录，所以「源端旧纪元的写被拒」从**目标端第一次
// 以新纪元落库**时才开始成立。两根锚：
//  1. 迁移换锁（目录升纪元）后、目标端落库前——源端旧纪元 Put 仍可通过
//     （== 高水位：Store「相等=同 incarnation 重试允许」语义）；
//  2. 目标端从随迁包恢复即以本实例新纪元落库（不等首轮 checkpoint）——
//     此后源端旧纪元 Put 被 ErrStaleIncarnation 拒。
// 双 manager 共享同一目录与同一 Store（checkpoint.Store 是集群级后端：
// 源端的迟到写入与目标端的落库天然落在同一 per-id 高水位上）。

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/internal/activation"
)

// fenceProbeStore 计数装饰 Store：Put/Get 全透传 InMemory（真围栏语义：
// per-id 高水位 + ErrStaleIncarnation），逐次记录 Put 的 (纪元, 快照拷贝)
// 供次数/内容断言。
type fenceProbeStore struct {
	inner *checkpoint.InMemory
	mu    sync.Mutex
	puts  []fencePutRecord
}

type fencePutRecord struct {
	inc  uint64
	snap []byte
}

func newFenceProbeStore() *fenceProbeStore {
	return &fenceProbeStore{inner: checkpoint.New()}
}

func (s *fenceProbeStore) Get(ctx context.Context, id rans.ID) ([]byte, bool, error) {
	return s.inner.Get(ctx, id)
}

func (s *fenceProbeStore) Put(ctx context.Context, id rans.ID, incarnation uint64, snap []byte) error {
	err := s.inner.Put(ctx, id, incarnation, snap)
	cp := append([]byte(nil), snap...)
	s.mu.Lock()
	s.puts = append(s.puts, fencePutRecord{inc: incarnation, snap: cp})
	s.mu.Unlock()
	return err
}

func (s *fenceProbeStore) Delete(ctx context.Context, id rans.ID) error {
	return s.inner.Delete(ctx, id)
}

func (s *fenceProbeStore) putRecords() []fencePutRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]fencePutRecord, len(s.puts))
	copy(out, s.puts)
	return out
}

// chanLoader 激活期装载桩：LoadState 返回注入的随迁包字节（模拟 remote
// PackageCache 的 Put→Take 链路——单次消费语义下等价直读）。
type chanLoader struct {
	mu sync.Mutex
	b  []byte
	ok bool
}

func (l *chanLoader) supply(b []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b, l.ok = b, true
}

func (l *chanLoader) LoadState(context.Context, rans.ID) ([]byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b, l.ok
}

// fenceCluster 双 manager 迁移夹具：源（node 1）/目标（node 2）共享目录与
// Store；sendPackage 回调把随迁包喂给目标端 loader（等价 dispatcher 落缓存）。
type fenceCluster struct {
	dir    *fakeTransferDir
	store  *fenceProbeStore
	source *activation.Table
	target *activation.Table
	loader *chanLoader
}

func newFenceCluster(t *testing.T, kind string, ent *fakeCarrierEntity) *fenceCluster {
	t.Helper()
	dir := newFakeTransferDir()
	store := newFenceProbeStore()
	loader := &chanLoader{}
	c := &fenceCluster{
		dir:    dir,
		store:  store,
		loader: loader,
		source: activation.New(dir, 1, map[string]rans.Factory{kind: func(_ rans.ID) rans.Entity { return ent }},
			activation.Options{
				Store:     store,
				Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 2, true },
				SendPackage: func(_ context.Context, _ rans.NodeID, _ rans.ID, snap []byte) error {
					loader.supply(append([]byte(nil), snap...))
					return nil
				},
			}),
		target: activation.New(dir, 2, map[string]rans.Factory{kind: func(_ rans.ID) rans.Entity {
			return &fakeCarrierEntity{}
		}}, activation.Options{Store: store, StateLoader: loader}),
	}
	return c
}

// waitTerminal 等待 pawnite 到达终态（有界；超时即失败）。
func waitTerminal(t *testing.T, p interface{ Done() <-chan struct{} }, what string) {
	t.Helper()
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("%s must reach terminal state", what)
	}
}

// TestMigrationFence_WindowClosesAtTargetFirstPersist（锚 1）：源端 inc=S
// 完成过 checkpoint（高水位 S）→ 迁移换锁 → 目标端以新纪元落库**前**，源端
// 旧纪元 Put 仍可通过（相等允许）；目标端落库**后**，旧纪元 Put 被
// ErrStaleIncarnation 拒——围栏从目标端第一次以新纪元落库时开始成立。
func TestMigrationFence_WindowClosesAtTargetFirstPersist(t *testing.T) {
	c := newFenceCluster(t, "carrier", &fakeCarrierEntity{state: []byte("state-v1")})
	id := mustTestID(t, "carrier", "fence-window")
	ctx := context.Background()

	p1, err := c.source.GetOrActivate(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p1, "source pawnite")
	srcInc := p1.Incarnation()
	if srcInc == 0 {
		t.Fatal("precondition: source incarnation must be fenced (> 0)")
	}

	// 源端完成过 checkpoint：Store 高水位 = srcInc。
	if n := CheckpointSweep(ctx, c.source, 10); n != 1 {
		t.Fatalf("source sweep candidates = %d, want 1", n)
	}
	waitFor(t, "source checkpoint landed", func() bool { return len(c.store.putRecords()) >= 1 })

	// 迁移换锁：目录升纪元指向 node 2；源端被逐出。
	if err := Migrate(ctx, c.source, id); err != nil {
		t.Fatalf("Migrate error = %v, want nil", err)
	}
	waitTerminal(t, p1, "won migration")
	if loc, found, _ := c.dir.Lookup(ctx, id); !found || loc.Node != 2 {
		t.Fatalf("post-CAS directory = (%v, %v), want target node 2", loc, found)
	}

	// 围栏窗开：目标端尚未落库——源端旧纪元 Put 通过（== 高水位，同
	// incarnation 重试允许语义；内容覆写为迟到的 straggler 快照）。
	if err := c.store.Put(ctx, id, srcInc, []byte("state-straggler")); err != nil {
		t.Fatalf("pre-persist stale-epoch put must pass (equal high-water), got %v", err)
	}

	// 目标端激活：自指向收养升纪元 → 随迁包恢复 → 恢复即落库（新纪元）。
	p2, err := c.target.GetOrActivate(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p2, "target pawnite")
	if p2.Incarnation() <= srcInc {
		t.Fatalf("target incarnation %d must exceed source %d", p2.Incarnation(), srcInc)
	}

	// 围栏窗关：目标端已以新纪元落库——源端旧纪元 Put 被拒（X10 哨兵）。
	err = c.store.Put(ctx, id, srcInc, []byte("state-straggler-late"))
	if !errors.Is(err, checkpoint.ErrStaleIncarnation) {
		t.Fatalf("post-persist stale-epoch put = %v, want ErrStaleIncarnation (fence starts at target's first persist)", err)
	}
	// Store 终态 = 目标端落库内容（不是源端迟到快照）。
	got, found, err := c.store.Get(ctx, id)
	if err != nil || !found {
		t.Fatalf("store get = %v, %v", found, err)
	}
	if string(got) != "state-v1" {
		t.Fatalf("store final snapshot = %q, want %q (target's persist must win)", string(got), "state-v1")
	}
}

// TestMigrationFence_RestorePersistsImmediately（锚 2）：目标端从随迁包恢复
// → 激活完成时 Store 已含新纪元快照（不等首轮 checkpoint）：恰一次落库、
// 纪元 = 目标实例纪元、内容 = 包字节原样；成功落库后首轮 sweep 判干净
// （ckptEpoch 已标记）不再重写。
func TestMigrationFence_RestorePersistsImmediately(t *testing.T) {
	c := newFenceCluster(t, "carrier", &fakeCarrierEntity{state: []byte("state-v7")})
	id := mustTestID(t, "carrier", "fence-restore")
	ctx := context.Background()

	p1, err := c.source.GetOrActivate(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p1, "source pawnite")

	// 迁移赢：源端 migCore 尽力而为落库一次（inc=源纪元，内容=快照）。
	if err := Migrate(ctx, c.source, id); err != nil {
		t.Fatalf("Migrate error = %v, want nil", err)
	}
	waitTerminal(t, p1, "won migration")
	if puts := c.store.putRecords(); len(puts) != 1 {
		t.Fatalf("puts after source migration = %d, want 1 (source best-effort put)", len(puts))
	}

	// 目标端激活完成（不跑任何 CheckpointSweep）：恢复即落库必须已发生。
	p2, err := c.target.GetOrActivate(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p2, "target pawnite")

	puts := c.store.putRecords()
	if len(puts) != 2 {
		t.Fatalf("puts after target activation = %d, want 2 (restore-persist must land without any checkpoint sweep)", len(puts))
	}
	if puts[1].inc != p2.Incarnation() {
		t.Fatalf("restore-persist incarnation = %d, want target incarnation %d", puts[1].inc, p2.Incarnation())
	}
	if string(puts[1].snap) != "state-v7" {
		t.Fatalf("restore-persist snapshot = %q, want package bytes %q verbatim", string(puts[1].snap), "state-v7")
	}
	if loc, found, _ := c.dir.Lookup(ctx, id); !found || loc.Incarnation != p2.Incarnation() {
		t.Fatalf("directory = (%v, %v), want incarnation matched to target pawnite %d", loc, found, p2.Incarnation())
	}

	// 成功落库即「当前状态已持久」：首轮 sweep 判干净，零捕获、零追加写。
	if n := CheckpointSweep(ctx, c.target, 10); n != 0 {
		t.Fatalf("first sweep candidates = %d, want 0 (restore-persist marked the epoch clean)", n)
	}
	if len(c.store.putRecords()) != 2 {
		t.Fatalf("puts after first sweep = %d, want 2 (clean epoch must not rewrite identical content)", len(c.store.putRecords()))
	}
}
