package lifecycle

// 迁移编排不变量测试（规格 §13.1/§13.2 核心）：
//   - 换锁成功 ⟺ 目录指向目标——无中间窗（CAS 成功时刻目录值必为新节点、
//     迁移全程目录观察者要么见旧主要么见新主、绝不"消失"）；
//   - 在箱留箱——迁移窗内投递全部经 Forward 到达赢家，零回绝；
//   - 输回退——第三方抢注目录 → ErrMigrationLost（携带 ErrNotOwner）、实体
//     回 Active 继续服役、cleanup 注销不误删他人条目（恰一次语义复用）；
//   - 迁移换锁走实例级换锁面（裸 inmem 即可迁移）；
//   - 预算耗尽 → core ctx 生效 → 输路径；
//   - Store.Put 失败判输；随迁包发送失败（尽力而为）不阻断。

import (
	"context"
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
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// ---- 测试基座：实例级换锁的假目录 ----

// migTransferRecord 记录一次 TryTransferOwnership：入参（期望归属/目标）+
// 裁决 + 记录时刻的目录终值（postLoc——CAS 同临界区复读，钉死"CAS 返回真 ⟺
// 目录已指向目标"无中间窗）。
type migTransferRecord struct {
	id        rans.ID
	expected  directory.Owner
	to        rans.NodeID
	newInc    uint64
	won       bool
	err       error
	postLoc   directory.Owner
	postFound bool
}

// migUnregRecord 记录一次实例级 UnregisterIfOwner：调用时刻的目录状态与
// 是否删除（nodeID 取自 owner.Node）。
type migUnregRecord struct {
	nodeID  rans.NodeID
	found   bool
	owner   rans.NodeID
	deleted bool
}

// fakeTransferDir：inmem 同语义的内存目录 + 全量时序记录。默认行为是单锁内
// 原子 CAS（查主+换主+复读终值同一临界区——Lookup 观察者要么见旧主、要么见
// 新主，绝无中间态）。override 钩子替换 CAS 行为用于编排（阻塞/输方/预算
// 取消）；钩子可回调 cas 复用原子换主。注销面即实例级
// UnregisterIfOwner 单形态。
type fakeTransferDir struct {
	mu sync.Mutex
	m  map[rans.ID]directory.Owner
	// inc per-id 取号序列（与条目存亡解耦，Register/换锁共用，
	// 单调性跨条目死亡存续）。
	inc map[rans.ID]uint64

	recMu     sync.Mutex
	transfers []migTransferRecord
	unregs    []migUnregRecord
	deletes   int

	override func(ctx context.Context, id rans.ID, expected, target directory.Owner) (bool, uint64, directory.Owner, error)
}

var _ directory.Directory = (*fakeTransferDir)(nil)

func newFakeTransferDir() *fakeTransferDir {
	return &fakeTransferDir{
		m:   make(map[rans.ID]directory.Owner),
		inc: make(map[rans.ID]uint64),
	}
}

// nextIncLocked 取下一纪元（调用方必须已持 mu）：per-id 严格递增，首个 1。
func (d *fakeTransferDir) nextIncLocked(id rans.ID) uint64 {
	d.inc[id]++
	return d.inc[id]
}

// Register 注册原子域（v3 四分支，inmem 同语义）：无值/墓碑（inc 序列在
// 独立 map，条目删除即墓碑形态）→ 写自归属；现值 node+epoch 即本实例 →
// 重注册升纪元；expected 匹配 → 接管升纪元；否则输返回现归属。
func (d *fakeTransferDir) Register(_ context.Context, id rans.ID, self directory.Owner, expected *directory.Owner) (bool, directory.Owner, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	cur, exists := d.m[id]
	if exists && cur.Node != 0 {
		switch {
		case cur.Node == self.Node && cur.Epoch == self.Epoch:
			// 重注册/收养
		case expected != nil && cur == *expected:
			// CAS 接管
		default:
			return false, cur, nil
		}
	}
	owner := directory.Owner{Node: self.Node, Epoch: self.Epoch, Incarnation: d.nextIncLocked(id)}
	d.m[id] = owner
	return true, owner, nil
}

// UnregisterIfOwner 实例级条件注销：node+inc 同时匹配才删；
// 记录进 unregs 台账（「正常销毁恰一次注销」断言的面）。
func (d *fakeTransferDir) UnregisterIfOwner(_ context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	d.mu.Lock()
	loc, found := d.m[id]
	deleted := found && loc == owner
	if deleted {
		delete(d.m, id)
	}
	d.recMu.Lock()
	d.unregs = append(d.unregs, migUnregRecord{nodeID: owner.Node, found: found, owner: loc.Node, deleted: deleted})
	if deleted {
		d.deletes++
	}
	d.recMu.Unlock()
	d.mu.Unlock()
	return deleted, nil
}

// Transfer 实例级换锁（Directory 本面方法，迁移面的消费目标）：归属匹配即
// 取新纪元并改写归属。
func (d *fakeTransferDir) Transfer(ctx context.Context, id rans.ID, expected, target directory.Owner) (bool, uint64, directory.Owner, error) {
	if d.override != nil {
		won, newInc, cur, err := d.override(ctx, id, expected, target)
		d.mu.Lock()
		postLoc, postFound := d.m[id]
		d.mu.Unlock()
		d.recMu.Lock()
		d.transfers = append(d.transfers, migTransferRecord{id: id, expected: expected, to: target.Node, newInc: newInc, won: won, err: err, postLoc: postLoc, postFound: postFound})
		d.recMu.Unlock()
		return won, newInc, cur, err
	}
	// 默认路径：查主+换主+复读终值在同一临界区（原子 CAS 的时序证据）。
	d.mu.Lock()
	defer d.mu.Unlock()
	cur, found := d.m[id]
	var (
		won    bool
		newInc uint64
		ret    directory.Owner
		err    error
	)
	switch {
	case !found:
	// 条目不存在 → (false, 0, zero, nil)
	case cur != expected:
		ret = cur
		err = directory.ErrNotOwner
	default:
		newInc = d.nextIncLocked(id)
		ret = directory.Owner{Node: target.Node, Epoch: target.Epoch, Incarnation: newInc}
		d.m[id] = ret
		won = true
	}
	postLoc, postFound := d.m[id]
	d.recMu.Lock()
	d.transfers = append(d.transfers, migTransferRecord{id: id, expected: expected, to: target.Node, newInc: newInc, won: won, err: err, postLoc: postLoc, postFound: postFound})
	d.recMu.Unlock()
	return won, newInc, ret, err
}

func (d *fakeTransferDir) Lookup(_ context.Context, id rans.ID) (directory.Owner, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	loc, exists := d.m[id]
	return loc, exists, nil
}

// cas 原子换主（查主+换主同一临界区，实例级语义：匹配即升纪元写新归属），
// 供 override 钩子复用（不得直调 Transfer——override 自其内部调用会递归）。
func (d *fakeTransferDir) cas(id rans.ID, expected, target directory.Owner) (bool, uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	cur, found := d.m[id]
	if !found {
		return false, 0, nil
	}
	if cur != expected {
		return false, 0, directory.ErrNotOwner
	}
	next := d.nextIncLocked(id)
	d.m[id] = directory.Owner{Node: target.Node, Epoch: target.Epoch, Incarnation: next}
	return true, next, nil
}

// forceOwner 测试侧直接改主（第三方抢注编排；inc=0 的条目不被任何实例级
// 注销误删——注销按 {node, inc} 同时匹配）。
func (d *fakeTransferDir) forceOwner(id rans.ID, node rans.NodeID) {
	d.mu.Lock()
	d.m[id] = directory.Owner{Node: node}
	d.mu.Unlock()
}

// removeEntry 测试侧直接删条目（租约过期清理编排）。
func (d *fakeTransferDir) removeEntry(id rans.ID) {
	d.mu.Lock()
	delete(d.m, id)
	d.mu.Unlock()
}

func (d *fakeTransferDir) transfersSnapshot() []migTransferRecord {
	d.recMu.Lock()
	defer d.recMu.Unlock()
	out := make([]migTransferRecord, len(d.transfers))
	copy(out, d.transfers)
	return out
}

func (d *fakeTransferDir) unregsSnapshot() []migUnregRecord {
	d.recMu.Lock()
	defer d.recMu.Unlock()
	out := make([]migUnregRecord, len(d.unregs))
	copy(out, d.unregs)
	return out
}

func (d *fakeTransferDir) deleteCount() int {
	d.recMu.Lock()
	defer d.recMu.Unlock()
	return d.deletes
}

// ---- 测试基座：投递/Store/包发送假实现 ----

// migRejectInfo 记录一次回绝的状态码与理由。
type migRejectInfo struct {
	st     rans.Code
	reason rans.Reason
}

// migDelivery 通道式迁移投递记录：Deliver/Reject/Forward 三态回报（缓冲保证
// pawnite loop 上的处置方非阻塞）。
type migDelivery struct {
	id        rans.ID
	delivered chan struct{}
	forwarded chan rans.NodeID
	rejected  chan migRejectInfo
}

func newMigDelivery(id rans.ID) *migDelivery {
	return &migDelivery{
		id:        id,
		delivered: make(chan struct{}),
		forwarded: make(chan rans.NodeID, 4),
		rejected:  make(chan migRejectInfo, 4),
	}
}

func (d *migDelivery) UUID() rans.ID { return d.id }
func (d *migDelivery) Deliver(_ *pawnite.Pawnite) {
	close(d.delivered)
}
func (d *migDelivery) Reject(st rans.Code, reason rans.Reason) {
	d.rejected <- migRejectInfo{st: st, reason: reason}
}
func (d *migDelivery) Forward(w rans.NodeID) { d.forwarded <- w }

// migStore：checkpoint.Store 假实现——记录 Put 调用（快照拷贝），可注入失败。
type migStore struct {
	mu       sync.Mutex
	fail     bool
	putCalls []migPutCall
}

type migPutCall struct {
	id   rans.ID
	snap []byte
}

func (s *migStore) Get(context.Context, rans.ID) ([]byte, bool, error) { return nil, false, nil }
func (s *migStore) Put(_ context.Context, id rans.ID, incarnation uint64, snap []byte) error {
	cp := append([]byte(nil), snap...)
	s.mu.Lock()
	s.putCalls = append(s.putCalls, migPutCall{id: id, snap: cp})
	n := len(s.putCalls)
	fail := s.fail
	s.mu.Unlock()
	if fail {
		return fmt.Errorf("migStore: put #%d failed (injected)", n)
	}
	return nil
}
func (s *migStore) Delete(context.Context, rans.ID) error { return nil }

var _ checkpoint.Store = (*migStore)(nil)

func (s *migStore) putSnapshot() []migPutCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]migPutCall, len(s.putCalls))
	copy(out, s.putCalls)
	return out
}

// migPackage 随迁包发送记录。
type migPackage struct {
	target rans.NodeID
	id     rans.ID
	snap   []byte
}

// migMigrated onMigrated 钩子触发记录。
type migMigrated struct {
	id rans.ID
	to rans.NodeID
}

// ---- 前置门：未激活 / 无候选 ----

func TestMigrate_UnknownID(t *testing.T) {
	m := newTestTable(t)
	id, _ := rans.NewID("counter", "mig-unknown")
	if err := Migrate(context.Background(), m, id); !errors.Is(err, ErrMigrationUnknown) {
		t.Fatalf("err = %v, want ErrMigrationUnknown", err)
	}
}

// TestMigrate_NoEligibleTargetVariants：placement 未注入 / 返回 false /
// 返回本地自身，一律 ErrNoEligibleTarget——实体保持 Active、目录原状、零
// CAS 尝试；选点函数收到的 exclude 必须是本地节点号。
func TestMigrate_NoEligibleTargetVariants(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []activation.Options
	}{
		{"no placement injected", nil},
		{"placement returns false", []activation.Options{{Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 0, false }}}},
		{"placement returns self", []activation.Options{{Placement: func(_ string, exclude rans.NodeID) (rans.NodeID, bool) { return exclude, true }}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := newFakeTransferDir()
			m, id := newArbManager(1, dir, "mig-notarget", tc.opts...)
			p, err := m.GetOrActivate(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			waitActive(t, p, "pre-migration pawnite")
			if err := Migrate(context.Background(), m, id); !errors.Is(err, ErrNoEligibleTarget) {
				t.Fatalf("err = %v, want ErrNoEligibleTarget", err)
			}
			if p.State() != pawnite.StateActive {
				t.Fatalf("state = %v, want Active (zero side effects)", p.State())
			}
			if _, ok := m.GetLocal(id); !ok {
				t.Fatal("local registry must retain the untouched instance")
			}
			if loc, found, _ := dir.Lookup(context.Background(), id); !found || loc.Node != 1 {
				t.Fatalf("directory = (%v, %v), want intact self entry", loc, found)
			}
			if got := len(dir.transfersSnapshot()); got != 0 {
				t.Fatalf("transfer attempts = %d, want 0", got)
			}
		})
	}

	// exclude 语义：选点函数收到本地节点号。
	dir := newFakeTransferDir()
	var exclude rans.NodeID
	var seen bool
	m, id := newArbManager(1, dir, "mig-exclude", activation.Options{Placement: func(_ string, ex rans.NodeID) (rans.NodeID, bool) {
		exclude, seen = ex, true
		return 0, false
	}})
	if _, err := m.GetOrActivate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	_ = Migrate(context.Background(), m, id)
	if !seen || exclude != 1 {
		t.Fatalf("placement exclude = (%v, %v), want local node 1", exclude, seen)
	}
}

// ---- 赢家全流程：快照/Store/随迁包/钩子时序/逐出/换锁 ----

// TestMigrate_PlacementReceivesKind 迁移选点与首激活共用 kind 感知
// Placement：选点函数收到迁移实体
// 的 id.Kind——node 侧的 placementBridge 据此按 NodesByKind 过滤候选（纯
// 接入节点永不成为迁移目标）。选点返回无候选 → ErrNoEligibleTarget、实体
// 保持 Active（零副作用：本测试止步选点面，不进入交接窗）。
func TestMigrate_PlacementReceivesKind(t *testing.T) {
	dir := newFakeTransferDir()
	var seen bool
	var gotKind string
	m, id := newArbManager(1, dir, "mig-kind", activation.Options{Placement: func(kind string, self rans.NodeID) (rans.NodeID, bool) {
		seen, gotKind = true, kind
		return 0, false
	}}) // kind = "k"
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-migration pawnite")

	if err := Migrate(context.Background(), m, id); !errors.Is(err, ErrNoEligibleTarget) {
		t.Fatalf("err = %v, want ErrNoEligibleTarget", err)
	}
	if !seen {
		t.Fatal("placement must be consulted before bailing out")
	}
	if gotKind != "k" {
		t.Fatalf("placement kind = %q, want the migrating entity's kind %q", gotKind, "k")
	}
	if p.State() != pawnite.StateActive {
		t.Fatalf("state = %v, want Active (zero side effects)", p.State())
	}
}

// TestMigrate_WonFullPipeline：carrier 实体全流程——快照落 Store、随迁包
// 直发（target/id/快照内容逐字段）、目录换锁指向目标、本地逐出、onMigrated
// 在"目录已指新家 + 本地已逐出"之后触发恰一次、迁移赢家零目录注销。
func TestMigrate_WonFullPipeline(t *testing.T) {
	dir := newFakeTransferDir()
	store := &migStore{}
	pkgs := make(chan migPackage, 4)
	var m *activation.Table // OnMigrated 闭包经变量捕获引用自身表（触发点在迁移路径）
	migrated := make(chan migMigrated, 4)
	m = activation.New(dir, 1, map[string]rans.Factory{
		"carrier": func(_ rans.ID) rans.Entity { return &fakeCarrierEntity{state: []byte("state-v1")} },
	}, activation.Options{Store: store, SendPackage: func(_ context.Context, target rans.NodeID, pid rans.ID, snap []byte) error {
		pkgs <- migPackage{target: target, id: pid, snap: append([]byte(nil), snap...)}
		return nil
	}, Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 7, true }, OnMigrated: func(got rans.ID, to rans.NodeID) {
		// 钩子时序钉死：目录已指向目标、本地已逐出（cleanup 已完成）。
		loc, found, err := dir.Lookup(context.Background(), got)
		if err != nil {
			t.Errorf("onMigrated lookup: %v", err)
		} else if !found || loc.Node != 7 {
			t.Errorf("onMigrated must observe directory at target, got (%v, %v)", loc, found)
		}
		if _, ok := m.GetLocal(got); ok {
			t.Error("onMigrated must fire after local eviction")
		}
		migrated <- migMigrated{id: got, to: to}
	}})
	id, _ := rans.NewID("carrier", "mig-full")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-migration pawnite")

	if err := Migrate(context.Background(), m, id); err != nil {
		t.Fatalf("Migrate error = %v, want nil", err)
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("won migration must reach terminal state")
	}
	// 目录已指向目标，且迁移全程零注销（cleanup 的 SkipDirUnregister 接线）。
	if loc, found, _ := dir.Lookup(context.Background(), id); !found || loc.Node != 7 {
		t.Fatalf("directory = (%v, %v), want target node 7", loc, found)
	}
	if got := dir.deleteCount(); got != 0 {
		t.Fatalf("won migration directory deletes = %d, want 0", got)
	}
	if got := len(dir.unregsSnapshot()); got != 0 {
		t.Fatalf("won migration unregister attempts = %d, want 0 (skip-dir-unregister)", got)
	}
	// 本地注册表已逐出。
	if _, ok := m.GetLocal(id); ok {
		t.Fatal("won migration must evict the local registry entry")
	}
	// Store 落快照恰一次，内容为 carrier 快照。
	puts := store.putSnapshot()
	if len(puts) != 1 || puts[0].id != id || string(puts[0].snap) != "state-v1" {
		t.Fatalf("store puts = %+v, want one state-v1 snapshot", puts)
	}
	// 随迁包直发恰一次，target/id/快照逐字段。
	select {
	case pkg := <-pkgs:
		if pkg.target != 7 || pkg.id != id || string(pkg.snap) != "state-v1" {
			t.Fatalf("package = (%d, %v, %q), want (7, %v, state-v1)", pkg.target, pkg.id, pkg.snap, id)
		}
	default:
		t.Fatal("migration package must be sent before CAS")
	}
	if len(pkgs) != 0 {
		t.Fatal("package sender must be invoked exactly once")
	}
	// onMigrated 恰一次，携带 (id, target)。
	select {
	case got := <-migrated:
		if got.id != id || got.to != 7 {
			t.Fatalf("onMigrated = (%v, %d), want (%v, 7)", got.id, got.to, id)
		}
	default:
		t.Fatal("onMigrated must fire after Won")
	}
	if len(migrated) != 0 {
		t.Fatal("onMigrated must fire exactly once")
	}
	// CAS 记录：恰一次成功，成功时刻目录终值即目标（无中间窗）。
	trs := dir.transfersSnapshot()
	if len(trs) != 1 || !trs[0].won || trs[0].err != nil {
		t.Fatalf("transfers = %+v, want single successful CAS", trs)
	}
	if !trs[0].postFound || trs[0].postLoc.Node != 7 {
		t.Fatalf("post-CAS directory = (%v, %v), want target 7 atomically", trs[0].postLoc, trs[0].postFound)
	}
}

// ---- 不变量一：换锁成功 ⟺ 目录指向目标（无中间窗）----

// TestMigrate_NoWindowBetweenCASAndDirectory（§13.2 灵魂）：迁移全程
// （含 CAS 阻塞窗与 Won 后终局收尾）目录观察者要么见旧主、要么见新主，
// 绝不"消失"（注销穿插即违规）；每次 CAS 成功时刻目录终值必为目标。
func TestMigrate_NoWindowBetweenCASAndDirectory(t *testing.T) {
	dir := newFakeTransferDir()
	m, id := newArbManager(1, dir, "mig-window", migPlacement(7))
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-migration pawnite")

	entered := make(chan struct{})
	release := make(chan struct{})
	dir.override = func(_ context.Context, pid rans.ID, expected, target directory.Owner) (bool, uint64, directory.Owner, error) {
		close(entered)
		<-release // CAS 阻塞：迁移窗持续存在（Destroying+pending、loop 被 core 占住）
		won, newInc, err := dir.cas(pid, expected, target)
		return won, newInc, directory.Owner{}, err
	}
	// 观察者：迁移全程密集轮询目录，旧主/新主之外的任何观测（含消失）都是违规。
	stop := make(chan struct{})
	pollerDone := make(chan struct{})
	violations := make(chan string, 8)
	go func() {
		defer close(pollerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			loc, found, _ := dir.Lookup(context.Background(), id)
			switch {
			case !found:
				violations <- "directory entry gone during migration (unregister interposed)"
			case loc.Node != 1 && loc.Node != 7:
				violations <- fmt.Sprintf("directory owner %d observed (want 1 or 7)", loc.Node)
			}
		}
	}()

	res := make(chan error, 1)
	go func() { res <- Migrate(context.Background(), m, id) }()
	<-entered
	time.Sleep(50 * time.Millisecond) // 采样放大（非承重：窗由 gate 确定性持有）
	close(release)
	err = <-res
	if err != nil {
		t.Fatalf("Migrate error = %v, want nil", err)
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("won migration must reach terminal state")
	}
	close(stop)
	<-pollerDone

	select {
	case v := <-violations:
		t.Fatal(v)
	default:
	}
	for _, tr := range dir.transfersSnapshot() {
		if tr.won && (!tr.postFound || tr.postLoc.Node != 7) {
			t.Fatalf("successful CAS left directory at (%v, %v), want target 7 with no window", tr.postLoc, tr.postFound)
		}
	}
	if got := dir.deleteCount(); got != 0 {
		t.Fatalf("directory deletes during migration = %d, want 0", got)
	}
}

// ---- 不变量二：在箱留箱（迁移窗投递零回绝）----

// TestMigrate_HoldInBoxZeroReject（§13.2 灵魂）：CAS 阻塞窗（core 占住
// loop、Destroying+pending）内准入的投递全部整箱 Forward 到赢家节点，零
// 回绝、零本地执行（core 持有 loop 期间 inbox 不被消费，Won 后 drainForward
// 必然接管）。
func TestMigrate_HoldInBoxZeroReject(t *testing.T) {
	dir := newFakeTransferDir()
	m, id := newArbManager(1, dir, "mig-hold", migPlacement(9))
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-migration pawnite")

	entered := make(chan struct{})
	release := make(chan struct{})
	dir.override = func(_ context.Context, pid rans.ID, expected, target directory.Owner) (bool, uint64, directory.Owner, error) {
		close(entered)
		<-release
		won, newInc, err := dir.cas(pid, expected, target)
		return won, newInc, directory.Owner{}, err
	}
	res := make(chan error, 1)
	go func() { res <- Migrate(context.Background(), m, id) }()
	<-entered // core 已进入 CAS：迁移窗开且稳定（loop 被占住）

	const n = 4
	ds := make([]*migDelivery, n)
	for i := range ds {
		ds[i] = newMigDelivery(id)
		if !p.ReceiveRequest(ds[i]) {
			t.Fatalf("window admission %d must be held-in-box, not rejected", i)
		}
	}
	close(release)
	if err := <-res; err != nil {
		t.Fatalf("Migrate error = %v, want nil", err)
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("won migration must reach terminal state")
	}
	for i, d := range ds {
		select {
		case <-d.delivered:
			t.Fatalf("delivery %d executed locally while core held the loop (must be forwarded)", i)
		default:
		}
		select {
		case w := <-d.forwarded:
			if w != 9 {
				t.Fatalf("delivery %d forward target = %d, want 9", i, w)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("held-in-box delivery %d never reached the winner", i)
		}
		select {
		case r := <-d.rejected:
			t.Fatalf("zero-reject violated for delivery %d: %v %v", i, r.st, r.reason)
		default:
		}
	}
}

// ---- 输回退：第三方抢注 ----

// TestMigrate_LostRollbackThirdPartyWins：CAS 前第三方已换主（条目指向
// 42）→ Migrate 返回 ErrMigrationLost 且错误链携带 directory.ErrNotOwner；
// 实体回退 Active 继续服役（Call 成功、本地条目保留、onMigrated 不触发）；
// 之后正常销毁的 cleanup 注销不误删他人条目（恰一次语义复用：归属守卫下
// 零删除）。
func TestMigrate_LostRollbackThirdPartyWins(t *testing.T) {
	dir := newFakeTransferDir()
	migrated := make(chan migMigrated, 1)
	m, id := newArbManager(1, dir, "mig-lost",
		activation.Options{OnMigrated: func(got rans.ID, to rans.NodeID) { migrated <- migMigrated{id: got, to: to} }},
		migPlacement(7))
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-migration pawnite")

	dir.override = func(_ context.Context, pid rans.ID, _, _ directory.Owner) (bool, uint64, directory.Owner, error) {
		dir.forceOwner(pid, 42) // 第三方的转移先落地：条目易主 42
		return false, 0, directory.Owner{}, directory.ErrNotOwner
	}

	err = Migrate(context.Background(), m, id)
	if !errors.Is(err, ErrMigrationLost) {
		t.Fatalf("err = %v, want ErrMigrationLost", err)
	}
	if !errors.Is(err, directory.ErrNotOwner) {
		t.Fatalf("err = %v, must carry directory.ErrNotOwner (CAS-specific cause)", err)
	}
	// 回退 Active 继续服役。
	if p.State() != pawnite.StateActive {
		t.Fatalf("post-Lost state = %v, want StateActive", p.State())
	}
	if _, ok := m.GetLocal(id); !ok {
		t.Fatal("rolled-back instance must remain in the local registry")
	}
	if _, err := m.InvokeLocal(context.Background(), id, nil); err != nil {
		t.Fatalf("rolled-back instance must serve calls: %v", err)
	}
	select {
	case <-p.Done():
		t.Fatal("Done must NOT close after a lost migration")
	default:
	}
	select {
	case <-migrated:
		t.Fatal("onMigrated must not fire on a lost migration")
	default:
	}
	// 目录仍归属第三方。
	if loc, found, _ := dir.Lookup(context.Background(), id); !found || loc.Node != 42 {
		t.Fatalf("directory = (%v, %v), want third-party owner 42", loc, found)
	}

	// 之后正常销毁：cleanup 的 UnregisterIfOwner 不得误删他人条目。
	DestroyLocal(context.Background(), m, id)
	<-p.Done()
	if loc, found, _ := dir.Lookup(context.Background(), id); !found || loc.Node != 42 {
		t.Fatalf("foreign-owned entry must survive our cleanup unregister, got (%v, %v)", loc, found)
	}
	for _, u := range dir.unregsSnapshot() {
		if u.deleted {
			t.Fatalf("cleanup must never delete a foreign-owned entry: %+v", u)
		}
	}
}

// TestMigrate_LostEntryVanished：CAS 时条目已不存在（(false,nil) 分支
// ——租约过期清理/竞态消失）→ ErrMigrationLost（无特定输因）、实体回退
// Active 继续服役。
func TestMigrate_LostEntryVanished(t *testing.T) {
	dir := newFakeTransferDir()
	m, id := newArbManager(1, dir, "mig-vanished", migPlacement(7))
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-migration pawnite")

	dir.override = func(_ context.Context, pid rans.ID, _, _ directory.Owner) (bool, uint64, directory.Owner, error) {
		dir.removeEntry(pid) // 模拟租约过期清理：条目在 CAS 前消失
		return false, 0, directory.Owner{}, nil
	}
	if err := Migrate(context.Background(), m, id); !errors.Is(err, ErrMigrationLost) {
		t.Fatalf("err = %v, want ErrMigrationLost", err)
	}
	if p.State() != pawnite.StateActive {
		t.Fatalf("post-Lost state = %v, want StateActive", p.State())
	}
	if _, err := m.InvokeLocal(context.Background(), id, nil); err != nil {
		t.Fatalf("rolled-back instance must serve calls: %v", err)
	}
	// 输路径不注销不抢注：条目消失留给销毁后的重激活自愈。
	DestroyLocal(context.Background(), m, id)
	<-p.Done()
	p2, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatalf("re-activation must self-heal the vanished entry: %v", err)
	}
	waitActive(t, p2, "re-activated pawnite")
	if loc, found, _ := dir.Lookup(context.Background(), id); !found || loc.Node != 1 {
		t.Fatalf("directory = (%v, %v), want re-registered at self", loc, found)
	}
}

// blockingDeactEntity：Activate 成功；Deactivate 阻塞到 release——维持
// Destroying 瞬态供迁移前置校验测试编排。
type blockingDeactEntity struct {
	release chan struct{}
}

func (e *blockingDeactEntity) Activate(context.Context) error { return nil }
func (e *blockingDeactEntity) Handle(context.Context, any) (any, error) {
	return nil, nil
}
func (e *blockingDeactEntity) Deactivate(context.Context) error {
	<-e.release
	return nil
}

// TestMigrate_NotActivePassThrough：并发销毁已把 pawnite 置 Destroying
// （Deactivate 阻塞维持该瞬态）时发起迁移 → pawnite.ErrNotActive 原样透传
// （非 ErrMigrationLost 变体），零副作用、钩子不触发。
func TestMigrate_NotActivePassThrough(t *testing.T) {
	dir := newFakeTransferDir()
	release := make(chan struct{})
	migrated := make(chan migMigrated, 1)
	m := activation.New(dir, 1, map[string]rans.Factory{
		"gate": func(_ rans.ID) rans.Entity { return &blockingDeactEntity{release: release} },
	}, activation.Options{Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 7, true }, OnMigrated: func(rans.ID, rans.NodeID) { close(migrated) }})
	id, _ := rans.NewID("gate", "mig-notactive")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-destroy pawnite")

	destroyed := make(chan struct{})
	go func() {
		defer close(destroyed)
		DestroyLocal(context.Background(), m, id) // Destroying 瞬态由阻塞的 Deactivate 维持
	}()
	deadline := time.Now().Add(2 * time.Second)
	for p.State() != pawnite.StateDestroying && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.State() != pawnite.StateDestroying {
		t.Fatal("precondition: pawnite must be held in Destroying by the blocked Deactivate")
	}

	if err := Migrate(context.Background(), m, id); !errors.Is(err, pawnite.ErrNotActive) {
		t.Fatalf("err = %v, want pawnite.ErrNotActive pass-through", err)
	}
	select {
	case <-migrated:
		t.Fatal("onMigrated must not fire on a precondition failure")
	default:
	}
	close(release)
	<-destroyed
	<-p.Done()
	if got := dir.deleteCount(); got != 1 {
		t.Fatalf("normal destroy must still unregister exactly once, deletes = %d", got)
	}
}

// ---- 换锁能力结构性成立：裸 Directory（inmem）可迁移 ----

// TestMigrate_BareDirectorySupportsMigration：迁移换锁经 TransferDirectory
// 能力断言发现（目录缺 Transfer 能力的配置性不可用形态不存在——register-
// first 下产不出 Active 实体的目录谈不到迁移；inmem 自带换锁能力，单机
// 形态支持迁移）。快照/Store.Put/随迁包/换锁全链
// 在纯 inmem 目录上跑通。
func TestMigrate_BareDirectorySupportsMigration(t *testing.T) {
	store := &migStore{}
	pkgs := make(chan migPackage, 4)
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"carrier": func(_ rans.ID) rans.Entity { return &fakeCarrierEntity{state: []byte("state-v1")} },
	}, activation.Options{Store: store, SendPackage: func(_ context.Context, target rans.NodeID, pid rans.ID, snap []byte) error {
		pkgs <- migPackage{target: target, id: pid, snap: append([]byte(nil), snap...)}
		return nil
	}, Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 7, true }})
	id, _ := rans.NewID("carrier", "mig-inmem-e2e")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-migration pawnite")

	if err := Migrate(context.Background(), m, id); err != nil {
		t.Fatalf("migration on bare inmem directory must succeed: %v", err)
	}
	waitTerminal(t, p, "won migration")
	if loc, found, _ := m.Directory().Lookup(context.Background(), id); !found || loc.Node != 7 {
		t.Fatalf("directory = (%v, %v), want target 7 (instance-level transfer on inmem)", loc, found)
	}
	if got := len(store.putSnapshot()); got != 1 {
		t.Fatalf("store puts = %d, want 1 (best-effort put)", got)
	}
	select {
	case pkg := <-pkgs:
		if pkg.target != 7 || pkg.id != id || string(pkg.snap) != "state-v1" {
			t.Fatalf("package = (%d, %v, %q), want (7, %v, state-v1)", pkg.target, pkg.id, pkg.snap, id)
		}
	default:
		t.Fatal("migration package must be sent before CAS")
	}
	if _, ok := m.GetLocal(id); ok {
		t.Fatal("won migration must evict the local registry entry")
	}
}

// ---- 预算耗尽 ----

// TestMigrate_BudgetExpiryLoses：预算（SetMigrationBudget 短值）耗尽 →
// core 的 ctx 取消 → CAS 返回输 → ErrMigrationLost（链携带
// context.DeadlineExceeded）、实体回退 Active。
func TestMigrate_BudgetExpiryLoses(t *testing.T) {
	dir := newFakeTransferDir()
	m, id := newArbManager(1, dir, "mig-budget",
		activation.Options{MigrationBudget: 60 * time.Millisecond}, migPlacement(7))
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-migration pawnite")

	dir.override = func(ctx context.Context, _ rans.ID, _, _ directory.Owner) (bool, uint64, directory.Owner, error) {
		<-ctx.Done() // core 契约：尊重预算取消——IO 挂起至预算耗尽
		return false, 0, directory.Owner{}, ctx.Err()
	}
	start := time.Now()
	err = Migrate(context.Background(), m, id)
	elapsed := time.Since(start)
	if !errors.Is(err, ErrMigrationLost) {
		t.Fatalf("err = %v, want ErrMigrationLost", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, must carry context.DeadlineExceeded", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("budget expiry must bound the wait, took %v", elapsed)
	}
	// 等待段绑定预算后（fix round 1 修复 1）：调用方在预算耗尽即返回，
	// pawnite 的输回退（Active）由 loop 在 core 尊重 ctx 迟来返回后异步
	// 落位——有界等待，不再即时断言。
	waitActive(t, p, "post-budget-expiry rollback")
	if _, ok := m.GetLocal(id); !ok {
		t.Fatal("local registry must retain the rolled-back instance")
	}
}

// ---- Store.Put 失败判输；随迁包发送失败仍尽力而为 ----

// TestMigrate_PutFailureLoses：Store.Put 注入失败 → 迁移判输（《加固 spec》
// §5.1 第 3 点表 Migrate 行：Store 是正确性依赖，换锁成功而快照未落 = 新家
// 裸激活、已确认写丢失）。错误链 ErrMigrationLost 携带 Put
// 根因；实体回退 Active 继续服役、随迁包不发送（Put 未落库）。
func TestMigrate_PutFailureLoses(t *testing.T) {
	dir := newFakeTransferDir()
	store := &migStore{fail: true}
	sends := make(chan struct{}, 4)
	m := activation.New(dir, 1, map[string]rans.Factory{
		"carrier": func(_ rans.ID) rans.Entity { return &fakeCarrierEntity{state: []byte("state-v9")} },
	}, activation.Options{Store: store, SendPackage: func(context.Context, rans.NodeID, rans.ID, []byte) error {
		sends <- struct{}{}
		return errors.New("network partition (injected)")
	}, Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 7, true }})
	id, _ := rans.NewID("carrier", "mig-putfail")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-migration pawnite")

	err = Migrate(context.Background(), m, id)
	if !errors.Is(err, ErrMigrationLost) {
		t.Fatalf("put failure must lose the migration: %v", err)
	}
	if got := len(store.putSnapshot()); got != 1 {
		t.Fatalf("store put attempts = %d, want 1 (tried once, failed, judged lost)", got)
	}
	// Put 未落库 ⟹ 随迁包不发送（快路径只在正确性依赖落地后尝试）。
	if got := len(sends); got != 0 {
		t.Fatalf("package send attempts = %d, want 0 (put failed before send)", got)
	}
	// 输回退：实体回 Active 继续服役，目录未换锁。
	waitActive(t, p, "post-loss pawnite")
	if loc, found, _ := dir.Lookup(context.Background(), id); !found || loc.Node != 1 {
		t.Fatalf("directory = (%v, %v), want self node 1 (no transfer on loss)", loc, found)
	}
	if _, ok := m.GetLocal(id); !ok {
		t.Fatal("lost migration must retain the local registry entry")
	}
}

// TestMigrate_SendFailureStillWins：随迁包直发（at-most-once 快路径）
// 失败不阻断迁移——正确性依赖是刚落库的 Store（spec §5.1 第 3 点表）。
func TestMigrate_SendFailureStillWins(t *testing.T) {
	dir := newFakeTransferDir()
	store := &migStore{}
	m := activation.New(dir, 1, map[string]rans.Factory{
		"carrier": func(_ rans.ID) rans.Entity { return &fakeCarrierEntity{state: []byte("state-v8")} },
	}, activation.Options{Store: store, SendPackage: func(context.Context, rans.NodeID, rans.ID, []byte) error {
		return errors.New("network partition (injected)")
	}, Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 7, true }})
	id, _ := rans.NewID("carrier", "mig-sendfail")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-migration pawnite")

	if err := Migrate(context.Background(), m, id); err != nil {
		t.Fatalf("best-effort send failure must not block migration: %v", err)
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("won migration must reach terminal state")
	}
	if loc, found, _ := dir.Lookup(context.Background(), id); !found || loc.Node != 7 {
		t.Fatalf("directory = (%v, %v), want target 7", loc, found)
	}
	if got := len(store.putSnapshot()); got != 1 {
		t.Fatalf("store puts = %d, want 1 (correctness dependency landed)", got)
	}
	if _, ok := m.GetLocal(id); ok {
		t.Fatal("won migration must evict the local registry entry")
	}
}

// ---- 无 carrier 实体：nil 快照跳过 Store/包发送 ----

// TestMigrate_CarrierlessSkipsStoreAndPackage：实体未实现
// rans.Snapshotter → 快照为 nil → Store.Put 与随迁包发送均不调用（目标端
// 裸激活），迁移本身照常完成。
func TestMigrate_CarrierlessSkipsStoreAndPackage(t *testing.T) {
	dir := newFakeTransferDir()
	store := &migStore{}
	sends := make(chan struct{}, 4)
	m, id := newArbManager(1, dir, "mig-nocarrier",
		activation.Options{
			Store: store,
			SendPackage: func(context.Context, rans.NodeID, rans.ID, []byte) error {
				sends <- struct{}{}
				return nil
			},
		},
		migPlacement(7))
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-migration pawnite")

	if err := Migrate(context.Background(), m, id); err != nil {
		t.Fatalf("carrierless migration must succeed: %v", err)
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("won migration must reach terminal state")
	}
	if got := len(store.putSnapshot()); got != 0 {
		t.Fatalf("store puts = %d, want 0 (nil snapshot)", got)
	}
	if got := len(sends); got != 0 {
		t.Fatalf("package sends = %d, want 0 (nil snapshot)", got)
	}
	if loc, found, _ := dir.Lookup(context.Background(), id); !found || loc.Node != 7 {
		t.Fatalf("directory = (%v, %v), want target 7", loc, found)
	}
}

// migPlacement 固定目标选点（测试便利：所有迁移路径用例的固定 target）。
func migPlacement(target rans.NodeID) activation.Options {
	return activation.Options{Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return target, true }}
}
