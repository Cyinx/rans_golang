package rpc

// 迁移随迁包测试（规格 §13.3 目标端恢复）：MigrationPackage 投递 → 包缓存
// （copy-in / consume-on-read / TTL 惰性过期）→ 激活期 loader（包优先、
// Store 兜底）→ 双节点回环迁移端到端（包路径 + 包丢失 Store 承载）。
// 时序敏感断言一律走 channel / 确定性握手（peek 轮询带 deadline），不裸
// sleep；快照形状 = 8 字节大端计数（migCounter 私有契约，形状不匹配即报错）。

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/lifecycle"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/routing"
	"github.com/cyinx/rans_golang/internal/wire"
)

// ---- 测试基座 ----

// fakeClock 可控时钟：PackageCache 惰性过期的确定性驱动（不 sleep）。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// migCounterLedger 计数实体激活台账（激活发生在哪个节点/第几次的行为锁）。
type migCounterLedger struct {
	mu   sync.Mutex
	ents []*migCounter
}

func (l *migCounterLedger) add(e *migCounter) {
	l.mu.Lock()
	l.ents = append(l.ents, e)
	l.mu.Unlock()
}

func (l *migCounterLedger) get(t *testing.T, i int) *migCounter {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if i >= len(l.ents) {
		t.Fatalf("counter entity %d not created (created=%d)", i, len(l.ents))
	}
	return l.ents[i]
}

func (l *migCounterLedger) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.ents)
}

// migCounter 计数迁移实体：Handle 任意请求 → 计数+1 返回新值；快照 = 8 字节
// 大端计数；restore 对长度≠8 报错（形状不匹配行为锁——上层据此弃包走 Store
// 兜底）。countAtActivate 记录 Activate 时刻观测到的计数：load-first（restore
// 先于 Activate，规格 §13.3「实体先就绪」）的行为锚点。
type migCounter struct {
	mu              sync.Mutex
	count           int64
	countAtActivate int64
	activated       bool
}

var (
	_ rans.Entity      = (*migCounter)(nil)
	_ rans.Snapshotter = (*migCounter)(nil)
)

func (e *migCounter) Activate(context.Context) error {
	e.mu.Lock()
	e.countAtActivate, e.activated = e.count, true
	e.mu.Unlock()
	return nil
}

func (e *migCounter) Handle(_ context.Context, _ any) (any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.count++
	return e.count, nil
}

func (e *migCounter) Deactivate(context.Context) error { return nil }

func (e *migCounter) Snapshot() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(e.count))
	return b, nil
}

func (e *migCounter) Restore(b []byte) error {
	if len(b) != 8 {
		return fmt.Errorf("migCounter: snapshot shape mismatch: %d bytes, want 8", len(b))
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.count = int64(binary.BigEndian.Uint64(b))
	return nil
}

// activateView 读取 Activate 时刻观测到的计数与激活标志。
func (e *migCounter) activateView() (int64, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.countAtActivate, e.activated
}

// migEnv 分发器级测试基座：真 manager（inmem 目录）+ fakeConnManager 网关 +
// 分发器 + PackageCache（可控时钟）+ InMemory Store；loader 已按 remote.New
// 的接线形态（包缓存 Take）注入。
type migEnv struct {
	disp  *Dispatcher
	mgr   *activation.Table
	dir   directory.Directory
	gw    *Gateway
	fake  *fakeConnManager
	pool  *wire.PagePool
	cache *PackageCache
	store *checkpoint.InMemory
	clock *fakeClock
	led   *migCounterLedger
}

// newMigEnv 计数实体形态（kind=counter）。
func newMigEnv(t *testing.T) *migEnv {
	t.Helper()
	led := &migCounterLedger{}
	return newMigEnvWithKinds(t, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity {
			e := &migCounter{}
			led.add(e)
			return e
		},
	}, led)
}

// newMigEnvWithKinds 自定义 kind 表形态（无 carrier 面、混合 kind 等）。
func newMigEnvWithKinds(t *testing.T, kinds map[string]rans.Factory, led *migCounterLedger) *migEnv {
	t.Helper()
	dir := inmem.New()
	clock := &fakeClock{now: time.Now()}
	cache := NewPackageCache(0)
	cache.now = clock.Now
	store := checkpoint.New()
	mgr := activation.New(dir, 2, kinds, activation.Options{StateLoader: &cacheStateLoader{cache: cache}, Store: store})
	pool := wire.NewPagePool()
	fake := newFakeConnManager()
	gw := NewGateway(fake, func(_ context.Context, _ rans.NodeID) (string, error) {
		return "", errors.New("no address in test")
	}, pool, time.Second, nil)
	disp := NewDispatcher(mgr, gw, 2, pool, cache, 0, newDispOutbox(t, gw), nil)

	return &migEnv{
		disp: disp, mgr: mgr, dir: dir, gw: gw, fake: fake,
		pool: pool, cache: cache, store: store, clock: clock, led: led,
	}
}

// feedPackage 以池化页驱动一帧 MigrationPackage（模拟传输层解码完成后的
// 回调；body = 原始快照字节，不套外封）。
func (e *migEnv) feedPackage(inst string, snap []byte) {
	hdr := wire.Header{
		Type:         protocol.MsgMigrationPackage,
		Flags:        protocol.FlagHasTargetUUID | protocol.FlagHasSourceNodeID,
		MessageID:    9001,
		SourceNodeID: 1,
		TargetKind:   "counter",
		TargetInst:   inst,
	}
	page := e.pool.Get(len(snap))
	copy(page, snap)
	e.disp.HandleMessage(hdr, page)
}

// migSnap 构造计数快照（8 字节大端）。
func migSnap(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// migID 构造测试身份（kind 固定 counter）。
func migID(t *testing.T, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID("counter", inst)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// waitPackageArrived 等随迁包落到目标缓存（确定性握手）：包在源端 CAS 前
// 发出，但到达目标 dispatcher 与 Migrate 返回之间无同步关系——测试先
// 等包就位再触发激活，端到端断言才确定。
func waitPackageArrived(t *testing.T, c *PackageCache, id rans.ID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := c.peek(id); ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("migration package never arrived at the target cache")
}

// ---- PackageCache 单元：copy-in / consume-once / TTL 惰性过期 ----

// 1. copy-in：Put 后调用方对入参切片的改写不影响缓存内容（dispatcher 的
// 入站页 Put 后即归还页池，缓存必须自持拷贝）。
func TestPackageCachePutCopiesIn(t *testing.T) {
	c := NewPackageCache(0)
	id := migID(t, "cache-copy")
	snap := migSnap(7)
	c.Put(id, snap)
	snap[7] ^= 0xFF // 调用方后续改写
	got, ok := c.Take(id)
	if !ok || int64(binary.BigEndian.Uint64(got)) != 7 {
		t.Fatalf("Take = (%v, %v), want copied snapshot of 7", got, ok)
	}
}

// 2. consume-on-read：包恢复一次——第二次 Take miss（后续激活落 Store 兜底）。
func TestPackageCacheTakeConsumesOnce(t *testing.T) {
	c := NewPackageCache(0)
	id := migID(t, "cache-once")
	c.Put(id, migSnap(1))
	if _, ok := c.Take(id); !ok {
		t.Fatal("first Take must hit")
	}
	if _, ok := c.Take(id); ok {
		t.Fatal("second Take must miss (consume-on-read)")
	}
}

// 3. TTL 惰性过期：缺省 5min（TTL-1s 存活、TTL+1s 过期），过期即 Take miss。
func TestPackageCacheTTLExpiryLazy(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	c := NewPackageCache(0) // 0 → DefaultPackageTTL
	c.now = clock.Now
	id := migID(t, "cache-ttl")
	c.Put(id, migSnap(2))
	clock.Advance(DefaultPackageTTL - time.Second)
	if _, ok := c.Take(id); !ok {
		t.Fatal("entry must survive TTL minus one second")
	}
	c.Put(id, migSnap(3))
	clock.Advance(DefaultPackageTTL + time.Second)
	if _, ok := c.Take(id); ok {
		t.Fatal("entry must be lazily expired after TTL")
	}
}

// ---- 分发器级：包投递 → 激活 restore（loader 包优先、Store 兜底）----

// 4. 包到达 → 缓存命中 → 激活 restore：计数从快照值继续（状态延续）；
// restore 先于 Activate（countAtActivate = 快照值）；系统面单向零回帧；
// 包随激活消费。
func TestMigrationPackageDispatchCacheHitRestore(t *testing.T) {
	env := newMigEnv(t)
	id := migID(t, "pkg-hit")
	env.feedPackage("pkg-hit", migSnap(5))

	v, err := env.mgr.InvokeLocal(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("call after package dispatch: %v", err)
	}
	if v != int64(6) {
		t.Fatalf("call returned %v, want 6 (snapshot 5 restored +1)", v)
	}
	if at, activated := env.led.get(t, 0).activateView(); !activated || at != 5 {
		t.Fatalf("Activate observed count %d (activated=%v), want 5 (restore-before-activate)", at, activated)
	}
	if n := sentCount(env.fake); n != 0 {
		t.Fatalf("migration package is system-plane one-way, %d frames were sent", n)
	}
	if _, ok := env.cache.peek(id); ok {
		t.Fatal("package must be consumed by the activation restore")
	}
}

// 5. 包缺失 → Store 兜底恢复。
func TestMigrationPackageMissingStoreFallback(t *testing.T) {
	env := newMigEnv(t)
	id := migID(t, "pkg-missing")
	if err := env.store.Put(context.Background(), id, 0, migSnap(5)); err != nil {
		t.Fatal(err)
	}
	v, err := env.mgr.InvokeLocal(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("call without package: %v", err)
	}
	if v != int64(6) {
		t.Fatalf("call returned %v, want 6 (store snapshot 5 restored +1)", v)
	}
	if at, _ := env.led.get(t, 0).activateView(); at != 5 {
		t.Fatalf("Activate observed count %d, want 5 (store fallback)", at)
	}
}

// 6. 包形状不匹配（restore 对垃圾字节报错）→ 弃包 → Store 兜底。
func TestMigrationPackageShapeMismatchStoreFallback(t *testing.T) {
	env := newMigEnv(t)
	id := migID(t, "pkg-shape")
	env.feedPackage("pkg-shape", []byte("garbage-not-a-counter-snapshot"))
	if err := env.store.Put(context.Background(), id, 0, migSnap(7)); err != nil {
		t.Fatal(err)
	}
	v, err := env.mgr.InvokeLocal(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("call after shape mismatch: %v", err)
	}
	if v != int64(8) {
		t.Fatalf("call returned %v, want 8 (store snapshot 7 restored +1)", v)
	}
	if at, _ := env.led.get(t, 0).activateView(); at != 7 {
		t.Fatalf("Activate observed count %d, want 7 (store fallback after package dropped)", at)
	}
}

// 7. TTL 过期 → Take miss → Store 兜底（过期包视同未到达）。
func TestMigrationPackageTTLExpiredStoreFallback(t *testing.T) {
	env := newMigEnv(t)
	id := migID(t, "pkg-ttl")
	env.feedPackage("pkg-ttl", migSnap(5)) // 包=5
	if err := env.store.Put(context.Background(), id, 0, migSnap(9)); err != nil {
		t.Fatal(err)
	}
	env.clock.Advance(DefaultPackageTTL + time.Second)

	v, err := env.mgr.InvokeLocal(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("call after TTL expiry: %v", err)
	}
	if v != int64(10) {
		t.Fatalf("call returned %v, want 10 (store snapshot 9 restored +1)", v)
	}
	if at, _ := env.led.get(t, 0).activateView(); at != 9 {
		t.Fatalf("Activate observed count %d, want 9 (store fallback after TTL expiry)", at)
	}
}

// 8. consume-once 行为面：首次激活用包（包优先于 Store——Store 预置不同值
// 以区分来源），且恢复即落库（spec §5.2 第 6 点）：激活完成
// 时 Store 已被包字节覆写（陈旧孤儿值 9 退出——Store 与活实体状态一致）；
// 实体销毁走 Retire 交接窗写终快照——二次
// 激活从终快照恢复（6 而非包内容 5，销毁前确认过的 +1 不再丢失）。
func TestMigrationPackageRestoredOnceThenStore(t *testing.T) {
	env := newMigEnv(t)
	id := migID(t, "pkg-once")
	env.feedPackage("pkg-once", migSnap(5)) // 包=5
	if err := env.store.Put(context.Background(), id, 0, migSnap(9)); err != nil {
		t.Fatal(err)
	}

	v1, err := env.mgr.InvokeLocal(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if v1 != int64(6) {
		t.Fatalf("first call returned %v, want 6 (package 5 wins over store 9)", v1)
	}
	if at, _ := env.led.get(t, 0).activateView(); at != 5 {
		t.Fatalf("first Activate observed count %d, want 5 (package takes precedence)", at)
	}
	// 恢复即落库：Store 已含包字节（陈旧孤儿值 9 被覆写——Store 从此与活
	// 实体状态一致，源端旧纪元写也被 per-id 高水位围栏）。
	if b, ok, err := env.store.Get(context.Background(), id); err != nil || !ok || string(b) != string(migSnap(5)) {
		t.Fatalf("store after first activation = (%q, %v, %v), want package bytes %q (restore-persist)", string(b), ok, err, string(migSnap(5)))
	}

	// 销毁写终快照（Retire 交接窗）：确认过的 6 落 Store，包已消费不再参与。
	lifecycle.DestroyLocal(context.Background(), env.mgr, id)
	if b, ok, err := env.store.Get(context.Background(), id); err != nil || !ok || string(b) != string(migSnap(6)) {
		t.Fatalf("store after destroy = (%q, %v, %v), want final snapshot %q (retire persists acked state)", string(b), ok, err, string(migSnap(6)))
	}
	v2, err := env.mgr.InvokeLocal(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if v2 != int64(7) {
		t.Fatalf("second call returned %v, want 7 (final snapshot 6 restored +1 — acked +1 survives destroy)", v2)
	}
	if at, _ := env.led.get(t, 1).activateView(); at != 6 {
		t.Fatalf("second Activate observed count %d, want 6 (restored from retire final snapshot)", at)
	}
}

// 9. 畸形 TargetUUID → warn 丢弃：不 panic、缓存不落条目、零回帧（UUID
// 寻址失败即无语义可执行，系统面单向也不回执）。
func TestMigrationPackageMalformedTargetDropped(t *testing.T) {
	env := newMigEnv(t)
	hdr := wire.Header{
		Type:         protocol.MsgMigrationPackage,
		Flags:        protocol.FlagHasTargetUUID | protocol.FlagHasSourceNodeID,
		MessageID:    9002,
		SourceNodeID: 1,
		TargetKind:   "counter",
		TargetInst:   "", // 空实例键 → rans.NewID 失败
	}
	page := env.pool.Get(8)
	copy(page, migSnap(1))
	env.disp.HandleMessage(hdr, page)

	if _, ok := env.cache.peek(rans.ID{Kind: "counter", Key: ""}); ok {
		t.Fatal("malformed package must not land in the cache")
	}
	if n := sentCount(env.fake); n != 0 {
		t.Fatalf("malformed package must produce no reply, %d frames sent", n)
	}
}

// 10. 无 carrier 面 + loader 命中 → 字节被忽略（warn 留痕）：激活照常、
// Handle 正常执行、不 panic。
func TestMigrationPackageNoCarrierIgnoresBytes(t *testing.T) {
	env := newMigEnvWithKinds(t, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity {
			return &testEntity{handle: func(context.Context, any) (any, error) {
				return "bare-ok", nil
			}}
		},
	}, nil)
	env.feedPackage("pkg-nocarrier", migSnap(5))

	v, err := env.mgr.InvokeLocal(context.Background(), migID(t, "pkg-nocarrier"), nil)
	if err != nil {
		t.Fatalf("call after package to non-carrier entity: %v", err)
	}
	if v != "bare-ok" {
		t.Fatalf("call returned %v, want bare-ok (bare activation, bytes ignored)", v)
	}
}

// ---- 端到端：双节点回环迁移 ----

// migTransferDir 具备换锁能力（directory.TransferDirectory）的进程内测试
// 目录（remote 包内联实现——manager 侧 fakeTransferDir 不跨包共享；自带实现
// 以钉 CAS 原子性不依赖具体目录）：map+单锁，查主+换主同一临界区（CAS
// 原子性）。注册/注销即 Directory 合并端口本面，换锁为可选能力
// 面（本实现具备）。
type migTransferDir struct {
	mu  sync.Mutex
	m   map[rans.ID]directory.Owner
	inc map[rans.ID]uint64 // per-id 取号序列（跨条目死亡存续）
}

func newMigTransferDir() *migTransferDir {
	return &migTransferDir{
		m:   make(map[rans.ID]directory.Owner),
		inc: make(map[rans.ID]uint64),
	}
}

var (
	_ directory.Directory = (*migTransferDir)(nil)
	_ directory.Directory = (*migTransferDir)(nil)
)

// nextIncLocked 取下一纪元（调用方必须已持 mu）：per-id 严格递增，首个 1。
func (d *migTransferDir) nextIncLocked(id rans.ID) uint64 {
	d.inc[id]++
	return d.inc[id]
}

// Register 注册取号原子域（inmem 同语义）：缺失或自指向（收养）→
// 赢并升纪元；他节点持有 → 输并返回当前 owner。
func (d *migTransferDir) Register(_ context.Context, id rans.ID, self directory.Owner, expected *directory.Owner) (bool, directory.Owner, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if cur, exists := d.m[id]; exists && cur.Node != 0 {
		switch {
		case cur.Node == self.Node && cur.Epoch == self.Epoch:
		case expected != nil && cur == *expected:
		default:
			return false, cur, nil
		}
	}
	owner := directory.Owner{Node: self.Node, Epoch: self.Epoch, Incarnation: d.nextIncLocked(id)}
	d.m[id] = owner
	return true, owner, nil
}

// UnregisterIfOwner 实例级条件注销：node+inc 同时匹配才删。
func (d *migTransferDir) UnregisterIfOwner(_ context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	loc, found := d.m[id]
	if !found || loc != owner {
		return false, nil
	}
	delete(d.m, id)
	return true, nil
}

// Transfer 实例级换锁（Directory 本面方法）：归属匹配即取新纪元并改写归属。
func (d *migTransferDir) Transfer(_ context.Context, id rans.ID, expected, target directory.Owner) (bool, uint64, directory.Owner, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	cur, found := d.m[id]
	if !found {
		return false, 0, directory.Owner{}, nil // 条目竞态消失：输但无特定输因
	}
	if cur != expected {
		return false, 0, cur, directory.ErrNotOwner
	}
	next := d.nextIncLocked(id)
	won := directory.Owner{Node: target.Node, Epoch: target.Epoch, Incarnation: next}
	d.m[id] = won
	return true, next, won, nil
}

func (d *migTransferDir) Lookup(_ context.Context, id rans.ID) (directory.Owner, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	loc, exists := d.m[id]
	return loc, exists, nil
}

// migRemNode 迁移测试节点：Remote + manager + 计数实体台账。
type migRemNode struct {
	rem *Remote
	mgr *activation.Table
	led *migCounterLedger
}

// migNodeSpec 迁移节点构造参数。
type migNodeSpec struct {
	id        rans.NodeID
	dir       directory.Directory
	rt        *resolveTable
	store     checkpoint.Store // nil → 不接 Store（包路径隔离形态）
	led       *migCounterLedger
	cache     *routing.LocationCache                                     // nil → 不接位置缓存
	placement func(kind string, exclude rans.NodeID) (rans.NodeID, bool) // 迁移选点（构造期注入）
}

// startMigNode 构造并启动一个计数实体节点（kind=counter）。
func startMigNode(t *testing.T, spec migNodeSpec) *migRemNode {
	t.Helper()
	opts := activation.Options{Placement: spec.placement, Store: spec.store}
	if spec.cache != nil {
		// 缓存失效/回填钩子（归表构造期，经 Options 注入）。
		opts.OnEvicted = func(id rans.ID) { spec.cache.Invalidate(id) }
		opts.OnArbitrationLost = func(id rans.ID, winner directory.Owner) { spec.cache.PutPositive(id, winner) }
		opts.OnMigrated = func(id rans.ID, to rans.NodeID) { spec.cache.PutPositive(id, directory.Owner{Node: to}) }
	}
	mgr := activation.New(spec.dir, spec.id, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity {
			e := &migCounter{}
			spec.led.add(e)
			return e
		},
	}, opts)
	rem, err := New(Config{
		NodeID:             spec.id,
		ClusterName:        "mig-cluster",
		Addr:               "127.0.0.1:0",
		Secret:             []byte("loop-secret"),
		Dir:                spec.dir,
		Manager:            mgr,
		Resolve:            spec.rt.resolve,
		DefaultCallTimeout: 5 * time.Second,
		LocationCache:      spec.cache,
		CheckpointStore:    spec.store,
	})
	if err != nil {
		t.Fatalf("new migration node %d: %v", spec.id, err)
	}
	if err := rem.Start(context.Background()); err != nil {
		t.Fatalf("start migration node %d: %v", spec.id, err)
	}
	t.Cleanup(func() { _ = rem.Close() })
	return &migRemNode{rem: rem, mgr: mgr, led: spec.led}
}

// 11. 端到端（包路径）：源计数至 3 → Migrate → 目录指向目标、源端
// 逐出、源缓存回填新位置 → 目标激活 restore（包承载状态——目标端未接
// Store）→ 计数连续 3→4；源端再调用经路由到目标续走 4→5（源端不再服役）。
func TestRemoteLoopbackMigrationPackagePath(t *testing.T) {
	dir := newMigTransferDir()
	rt := &resolveTable{}
	store := checkpoint.New() // 仅源端接入：目标端恢复只能经随迁包
	led1, led2 := &migCounterLedger{}, &migCounterLedger{}
	cache1 := routing.NewLocationCache(time.Minute, time.Minute, 0)
	node1 := startMigNode(t, migNodeSpec{id: 1, dir: dir, rt: rt, store: store, led: led1, cache: cache1,
		placement: func(_ string, exclude rans.NodeID) (rans.NodeID, bool) { return 2, true }})
	node2 := startMigNode(t, migNodeSpec{id: 2, dir: dir, rt: rt, led: led2})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	ctx := context.Background()
	id := migID(t, "e2e-pkg")

	// 源端计数至 3。
	for want := int64(1); want <= 3; want++ {
		v, err := node1.rem.Call(ctx, id, 0)
		if err != nil {
			t.Fatalf("source call %d: %v", want, err)
		}
		if v != want {
			t.Fatalf("source call %d returned %v", want, v)
		}
	}

	// 迁移到节点 2（placement 由宿主在装配期注入，remote 只转发编排）。
	if err := node1.rem.Migrate(ctx, id); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// 目录指向目标；源端本地条目已逐出。
	if loc, found, _ := dir.Lookup(ctx, id); !found || loc.Node != 2 {
		t.Fatalf("directory = (%v, %v), want target node 2", loc, found)
	}
	if _, ok := node1.mgr.GetLocal(id); ok {
		t.Fatal("source must no longer hold the entity locally")
	}
	// 源端位置缓存经 onMigrated 回填新位置（PutPositive 接线）。
	if loc, found, positive := cache1.Get(id); !found || !positive || loc.Node != 2 {
		t.Fatalf("source location cache = (%v, %v, %v), want positive node 2", loc, found, positive)
	}

	// 等随迁包到达目标缓存（确定性握手），再触发目标激活。
	waitPackageArrived(t, node2.rem.pkgs, id)

	v, err := node2.rem.Call(ctx, id, 0)
	if err != nil {
		t.Fatalf("target call: %v", err)
	}
	if v != int64(4) {
		t.Fatalf("target call returned %v, want 4 (package-restored 3 +1)", v)
	}
	if n := led2.count(); n != 1 {
		t.Fatalf("target activations = %d, want 1", n)
	}
	if at, _ := led2.get(t, 0).activateView(); at != 3 {
		t.Fatalf("target Activate observed count %d, want 3 (package restored before activate)", at)
	}

	// 源端不再服役：node1 的调用经目录/缓存路由到目标，计数在目标续走。
	v, err = node1.rem.Call(ctx, id, 0)
	if err != nil {
		t.Fatalf("post-migration call from source: %v", err)
	}
	if v != int64(5) {
		t.Fatalf("post-migration call from source returned %v, want 5 (served by target)", v)
	}
	if n := led1.count(); n != 1 {
		t.Fatalf("source activations = %d, want 1 (never re-activated)", n)
	}
}

// 12. 端到端（包丢失变体）：随迁包发送注入失败（at-most-once：包是快路径
// 而非正确性依赖）→ 迁移照常完成，Store 是持久记录，目标激活落 Store 兜底
// 承载状态，计数连续。
func TestRemoteLoopbackMigrationPackageLostStoreCarries(t *testing.T) {
	dir := newMigTransferDir()
	rt := &resolveTable{}
	store := checkpoint.New() // 双端共享：Store 路径承载状态
	led1, led2 := &migCounterLedger{}, &migCounterLedger{}
	node1 := startMigNode(t, migNodeSpec{id: 1, dir: dir, rt: rt, store: store, led: led1,
		placement: func(_ string, exclude rans.NodeID) (rans.NodeID, bool) { return 2, true }})
	node2 := startMigNode(t, migNodeSpec{id: 2, dir: dir, rt: rt, store: store, led: led2})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	ctx := context.Background()
	id := migID(t, "e2e-lost")

	// 源端计数至 2。
	for want := int64(1); want <= 2; want++ {
		v, err := node1.rem.Call(ctx, id, 0)
		if err != nil {
			t.Fatalf("source call %d: %v", want, err)
		}
		if v != want {
			t.Fatalf("source call %d returned %v", want, v)
		}
	}

	// 注入包发送失败（装配期覆写 rpc.New 的直发接线——BindPackageSender 是
	// 该槽唯一写入点，无运行期 Set 面）。
	node1.mgr.BindPackageSender(func(context.Context, rans.NodeID, rans.ID, []byte) error {
		return errors.New("migration package lost (injected)")
	})
	if err := node1.rem.Migrate(ctx, id); err != nil {
		t.Fatalf("Migrate must survive package loss (best effort): %v", err)
	}

	if loc, found, _ := dir.Lookup(ctx, id); !found || loc.Node != 2 {
		t.Fatalf("directory = (%v, %v), want target node 2", loc, found)
	}

	// 目标激活：无包（发送失败）→ Store 兜底恢复 2 → 计数 3。
	v, err := node2.rem.Call(ctx, id, 0)
	if err != nil {
		t.Fatalf("target call after package loss: %v", err)
	}
	if v != int64(3) {
		t.Fatalf("target call returned %v, want 3 (store-restored 2 +1)", v)
	}
	if at, _ := led2.get(t, 0).activateView(); at != 2 {
		t.Fatalf("target Activate observed count %d, want 2 (store fallback carried the state)", at)
	}
}
