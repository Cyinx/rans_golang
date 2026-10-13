package lifecycle

// 交接窗三种 core 的零丢失契约测试（《加固 spec》§5.1
// 第 3 点 core 矩阵 + 第 4 点终局次序）。包侧三 core（Migrate 在
// migrate.go、Passivate/Retire 在 drain.go）与 pawnite 的显式终局次序
//（settleHandoffSuccess 按交接类别 req.kind 分流的 finishRetiredInOrder）
// 共同承诺：
//   - 钝化窗口内并发灌入的非幂等 add 零丢失——确认过的写全部进终快照，
//     窗内到达者按确定未执行回绝（可重试，重试落新激活从 Store 恢复）；
//   - 停机（Retire）写终快照——checkpoint 之后确认的写不再因销毁丢失，
//     且快照先于用户 Deactivate（Deactivate 对状态的修改不进快照，godoc 契约）；
//   - 终快照不被更旧的同纪元周期快照覆盖（core 首步等在飞 checkpoint 落定）；
//   - Passivate 失败回退 Active 继续服役（本轮中止，下轮再试）；
//   - 有状态未配置 Store：迁移被拒 / 钝化被拒 / Retire 走
//     「不写快照 + 启动告警一次」档；
//   - 看门狗强弃与 PanicPolicyIsolate 隔离销毁不走交接窗、不写快照（豁免锚
//     ——前者实体已卡死，后者 panic 意味着状态不可信）。

import (
	"context"
	"errors"
	"fmt"
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

// gatedStore Put 门闸桩：交接 core 走到 store.Put 即停在 gate（钝化窗稳定
// 展开的控制点），entered 在首次 Put 进入时关闭。puts 记进入次数（交接窗
// 组合矩阵断言面：跟随者 Retire 的 core 不得重复落库）。
type gatedStore struct {
	checkpoint.Store
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
	puts    atomic.Int32
}

func (s *gatedStore) Put(ctx context.Context, id rans.ID, inc uint64, snap []byte) error {
	s.puts.Add(1)
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.Store.Put(ctx, id, inc, snap)
}

// putsSnapshot 已进入的 Put 次数（含被取消者——进入即计数）。
func (s *gatedStore) putsSnapshot() int { return int(s.puts.Load()) }

// putFailStore Put 恒败桩（Get 恒未命中——激活裸恢复），记录 Put 次数。
type putFailStore struct {
	puts atomic.Int32
}

func (s *putFailStore) Get(context.Context, rans.ID) ([]byte, bool, error) {
	return nil, false, nil
}
func (s *putFailStore) Put(context.Context, rans.ID, uint64, []byte) error {
	s.puts.Add(1)
	return errors.New("rans: put: store down")
}
func (s *putFailStore) Delete(context.Context, rans.ID) error { return nil }

// recDir 记录实例级注销调用的目录包装（终局注销断言面）。
type recDir struct {
	directory.Directory

	mu     sync.Mutex
	unregs []directory.Owner
}

func (d *recDir) UnregisterIfOwner(ctx context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	d.mu.Lock()
	d.unregs = append(d.unregs, owner)
	d.mu.Unlock()
	return d.Directory.UnregisterIfOwner(ctx, id, owner)
}

func (d *recDir) unregsSnapshot() []directory.Owner {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]directory.Owner(nil), d.unregs...)
}

// deactivateMutatingEntity Deactivate 时改写余额的记账实体：终快照先于用户
// Deactivate 的 godoc 契约锚——Deactivate 写入的 999 不得进快照。
type deactivateMutatingEntity struct {
	acctEntity
}

func (d *deactivateMutatingEntity) Deactivate(ctx context.Context) error {
	d.bal = 999
	return nil
}

// TestHandoffPassivateWindowZeroLoss：钝化窗口内并发
// 灌入的非幂等 add 全部以可重试错误回绝（确定未执行）；钝化完成+重激活后，
// 确认过的写全部可见（acked 3 笔 → 重激活余额恰为 3），被回绝的 8 笔未执行
// ——重试补一笔得 4。若钝化关准入前快照已拍（快照→Put→Destroy 三段式），
// Put 窗口内确认的写会丢失——交接窗留箱正是消除该窗口。
func TestHandoffPassivateWindowZeroLoss(t *testing.T) {
	base := checkpoint.New()
	st := &gatedStore{Store: base, gate: make(chan struct{}), entered: make(chan struct{})}
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, activation.Options{Store: st})

	X := mustTestID(t, "acct", "ex9")
	ctx := context.Background()

	// 窗开前确认三笔写（acked 基准，逐笔回值核对）。
	for i := 0; i < 3; i++ {
		v, err := m.InvokeLocal(ctx, X, "add:1")
		if err != nil {
			t.Fatalf("acked add #%d: %v", i+1, err)
		}
		if v != i+1 {
			t.Fatalf("acked add #%d balance = %v, want %d", i+1, v, i+1)
		}
	}

	// 开钝化窗：core 卡在 Put 门闸（静默点——捕获已定格 acked 后状态）。
	res := make(chan error, 1)
	go func() { res <- passivateInstance(ctx, m, X) }()
	select {
	case <-st.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("passivate core never reached store put")
	}

	// 窗内并发灌入 8 笔非幂等 add：留箱等待，终局按确定未执行回绝。
	const burst = 8
	errs := make(chan error, burst)
	var wg sync.WaitGroup
	for i := 0; i < burst; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.InvokeLocal(ctx, X, "add:1")
			errs <- err
		}()
	}
	p, ok := m.GetLocal(X)
	if !ok {
		t.Fatal("instance must stay in local registry during the window")
	}
	waitFor(t, "burst held in box", func() bool { return p.InboxLen() >= burst })

	close(st.gate) // 放行 Put → 钝化走向成功终局（Deactivate→注销→回绝→Destroyed）
	if err := <-res; err != nil {
		t.Fatalf("passivate = %v, want nil", err)
	}
	for i := 0; i < burst; i++ {
		select {
		case err := <-errs:
			if !errors.Is(err, rans.ErrUnavailable) {
				t.Fatalf("held-in-box add err = %v, want retryable rans.ErrUnavailable", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("held-in-box add never settled")
		}
	}

	// 零丢失断言：重激活后确认过的写全部可见（3），回绝者未执行——重试补
	// 一笔恰得 4（回绝可安全重放的正面证明）。
	if v, err := m.InvokeLocal(ctx, X, "get"); err != nil || v != 3 {
		t.Fatalf("reactivated balance = (%v, %v), want (3, nil) — acked writes must all be visible", v, err)
	}
	if v, err := m.InvokeLocal(ctx, X, "add:1"); err != nil || v != 4 {
		t.Fatalf("retry add after reactivation = (%v, %v), want (4, nil)", v, err)
	}
}

// TestHandoffRetireFinalSnapshot：checkpoint 持久基线
// 100 之上确认 add:50（150）→ Retire → 重激活余额 150。终快照生效——没有
// 它，销毁路径（finish 只跑用户 Deactivate）会丢掉确认过的 50。同时锚定
// 快照先于用户 Deactivate（Deactivate 写入的 999 不进快照）与终局注销
// （实例级 UnregisterIfOwner 恰一次）。
func TestHandoffRetireFinalSnapshot(t *testing.T) {
	dir := &recDir{Directory: inmem.New()}
	base := checkpoint.New()
	X := mustTestID(t, "acct", "ex10")
	seedDurableState(t, dir, base, X, 2, "100") // 上一轮 checkpoint 留下的持久基线

	m := activation.New(dir, 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity {
		return &deactivateMutatingEntity{}
	}}, activation.Options{Store: base})

	ctx := context.Background()

	// 激活恢复 100，确认一笔 add:50 → 150（仅确认、未 checkpoint）。
	if v, err := m.InvokeLocal(ctx, X, "add:50"); err != nil || v != 150 {
		t.Fatalf("acked add:50 = (%v, %v), want (150, nil)", v, err)
	}
	p, ok := m.GetLocal(X)
	if !ok {
		t.Fatal("instance must be local before retire")
	}
	inc := p.Incarnation()
	unregsBefore := len(dir.unregsSnapshot()) // 含 seedDurableState 的取号注销

	// Retire：终快照落库 + 终局次序收尾。
	if err := retireInstance(ctx, m, X); err != nil {
		t.Fatalf("retire = %v, want nil", err)
	}
	if _, ok := m.GetLocal(X); ok {
		t.Fatal("retire must evict the local registry entry")
	}
	// 终局注销恰一次（实例级：本实例 node+inc）。
	unregs := dir.unregsSnapshot()
	if got := len(unregs) - unregsBefore; got != 1 {
		t.Fatalf("retire unregister calls = %d, want 1 (terminal disposal order)", got)
	}
	if last := unregs[len(unregs)-1]; last != (directory.Owner{Node: 1, Incarnation: inc}) {
		t.Fatalf("retire unregister owner = %+v, want {1, %d} (instance-level)", last, inc)
	}
	// 终快照内容 = 确认过的 150（不是 Deactivate 写入的 999）。
	if b, _, _ := base.Get(ctx, X); string(b) != "150" {
		t.Fatalf("store final value = %q, want \"150\" (final snapshot, pre-Deactivate)", string(b))
	}

	// 重激活：从 Store 恢复终快照 → 余额 150。
	if v, err := m.InvokeLocal(ctx, X, "get"); err != nil || v != 150 {
		t.Fatalf("reactivated balance = (%v, %v), want (150, nil)", v, err)
	}
}

// TestHandoffRetireFinalSnapshotBeatsStaleCheckpoint：慢周期 Put 注入
// （recStore.putDelay）——终快照不被更旧的同纪元快照覆盖。core 首步等在飞
// 周期 checkpoint 落定后才拍终快照：Store 同纪元允许覆盖，旧快照后到会把
// 终值打回旧内容。断言 Store 终值为终快照内容（50 而非 10）。
func TestHandoffRetireFinalSnapshotBeatsStaleCheckpoint(t *testing.T) {
	base := checkpoint.New()
	rec := &recStore{Store: base}
	rec.putDelay.Store(1) // 首个 Put（周期 checkpoint）慢 200ms
	entered := make(chan struct{})
	rec.putEntered = entered
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, activation.Options{Store: rec})

	X := mustTestID(t, "acct", "ex-stale")
	ctx := context.Background()

	if v, err := m.InvokeLocal(ctx, X, "add:10"); err != nil || v != 10 {
		t.Fatalf("add:10 = (%v, %v), want (10, nil)", v, err)
	}
	// 起一轮周期 checkpoint：捕获 bal=10 → Put 慢 200ms（在飞门 held）。
	if n := CheckpointSweep(ctx, m, 10); n != 1 {
		t.Fatalf("checkpoint sweep started = %d, want 1", n)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("periodic checkpoint put never started")
	}
	// 慢 Put 在飞期间再确认 add:40（bal=50）——终快照必须含这笔。
	if v, err := m.InvokeLocal(ctx, X, "add:40"); err != nil || v != 50 {
		t.Fatalf("add:40 = (%v, %v), want (50, nil)", v, err)
	}

	// Retire：core 首步等在飞 Put 落定，再拍终快照 50。
	if err := retireInstance(ctx, m, X); err != nil {
		t.Fatalf("retire = %v, want nil", err)
	}
	if b, _, _ := base.Get(ctx, X); string(b) != "50" {
		t.Fatalf("store final value = %q, want \"50\" (final snapshot must not be overwritten by stale same-epoch checkpoint)", string(b))
	}
	if got := rec.puts(); got != 2 {
		t.Fatalf("store puts = %d, want 2 (periodic + final)", got)
	}
}

// TestHandoffPassivatePutFailureRollsBack：Put 持续失败 → 钝化中止
// （错误返回），窗回退 Active——实体继续服役，后续调用成功，状态无损。
func TestHandoffPassivatePutFailureRollsBack(t *testing.T) {
	st := &putFailStore{}
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, activation.Options{Store: st})

	X := mustTestID(t, "acct", "ex-rollback")
	ctx := context.Background()

	if v, err := m.InvokeLocal(ctx, X, "add:5"); err != nil || v != 5 {
		t.Fatalf("add:5 = (%v, %v), want (5, nil)", v, err)
	}
	if err := passivateInstance(ctx, m, X); err == nil {
		t.Fatal("passivate with failing store must be aborted with an error")
	}
	p, ok := m.GetLocal(X)
	if !ok {
		t.Fatal("failed passivate must keep the instance in the local registry")
	}
	waitFor(t, "rollback to Active", func() bool { return p.State() == pawnite.StateActive })
	// 回退后继续服役：状态无损（5），后续写成功（10）。
	if v, err := m.InvokeLocal(ctx, X, "add:5"); err != nil || v != 10 {
		t.Fatalf("post-rollback add:5 = (%v, %v), want (10, nil)", v, err)
	}
}

// TestHandoffStatefulNoStoreMatrix（《加固 spec》§5.1 第 3 点表「有状态但未配置
// Store」列）：迁移被拒（只靠随迁包时包丢失即状态丢失；错误链
// ErrMigrationNoStore + ErrMigrationLost，实体保持 Active）、钝化被拒、
// Retire 走「不写快照 + 启动告警一次」档照样终局。
func TestHandoffStatefulNoStoreMatrix(t *testing.T) {
	dir := newFakeTransferDir()
	m := activation.New(dir, 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, activation.
		// 不注入 Store。
		Options{Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 7, true }})

	X := mustTestID(t, "acct", "ex-nostore")
	ctx := context.Background()

	if _, err := m.InvokeLocal(ctx, X, "add:5"); err != nil {
		t.Fatalf("bare activation add:5: %v", err)
	}

	// 迁移被拒：ErrMigrationNoStore（经 ErrMigrationLost 链），实体回 Active。
	if err := Migrate(ctx, m, X); !errors.Is(err, ErrMigrationNoStore) || !errors.Is(err, ErrMigrationLost) {
		t.Fatalf("stateful migration without store = %v, want ErrMigrationLost wrapping ErrMigrationNoStore", err)
	}
	p, ok := m.GetLocal(X)
	if !ok {
		t.Fatal("refused migration must keep the instance")
	}
	waitFor(t, "active after refused migration", func() bool { return p.State() == pawnite.StateActive })

	// 钝化被拒（现状保留）：错误返回，实体保持 Active。
	if err := passivateInstance(ctx, m, X); err == nil {
		t.Fatal("stateful passivation without store must be refused")
	}
	if got := p.State(); got != pawnite.StateActive {
		t.Fatalf("state after refused passivation = %v, want StateActive", got)
	}

	// Retire 走「不写快照」档：照样终局（停机不能被配置缺口挂死）。
	if err := retireInstance(ctx, m, X); err != nil {
		t.Fatalf("retire without store = %v, want nil (terminal proceeds)", err)
	}
	if _, ok := m.GetLocal(X); ok {
		t.Fatal("retire without store must still evict the local registry entry")
	}
	// 重激活裸激活：没有终快照可恢复（「不写快照」档的如实代价）。
	if v, err := m.InvokeLocal(ctx, X, "get"); err != nil || v != 0 {
		t.Fatalf("reactivated balance = (%v, %v), want (0, nil) — bare activation without snapshot", v, err)
	}
}

// TestHandoffAbandonNoSnapshotAnchor（豁免锚）：看门狗强弃不走交接窗、
// 不写快照——实体已卡死，快照无从谈起；下次激活从上一份持久快照恢复。
func TestHandoffAbandonNoSnapshotAnchor(t *testing.T) {
	base := checkpoint.New()
	rec := &recStore{Store: base}
	ent := &stuckAcctEntity{entered: make(chan struct{}), release: make(chan struct{})}
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return ent }}, activation.Options{Store: rec})

	X := mustTestID(t, "acct", "ex-abandon")
	ctx := context.Background()

	p, err := m.GetOrActivate(ctx, X)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-abandon pawnite")

	callRes := make(chan error, 1)
	go func() {
		_, err := m.InvokeLocal(ctx, X, "add:7")
		callRes <- err
	}()
	<-ent.entered // Handle 已卡死（执行在途）
	if !p.Abandon(pawnite.StateActive, pawnite.TerminalAbandoned) {
		t.Fatal("watchdog abandon must win on the stuck execution")
	}
	select {
	case err := <-callRes:
		if !errors.Is(err, rans.ErrTimeout) {
			t.Fatalf("stuck call err = %v, want rans.ErrTimeout (unbound by abandon)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stuck call never unbound")
	}
	waitTerminalEviction(t, m, X)
	// 豁免锚：强弃全程零快照写入。
	if got := rec.puts(); got != 0 {
		t.Fatalf("store puts after abandon = %d, want 0 (abandon never snapshots)", got)
	}
	close(ent.release) // zombie Handle 收尾
}

// stuckAcctEntity Handle 阻塞的记账实体（执行卡死强弃锚的载体；快照面承自
// acctEntity——「本应可快照」的实体被强弃时同样零快照写入）。
type stuckAcctEntity struct {
	acctEntity
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *stuckAcctEntity) Handle(ctx context.Context, req any) (any, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.acctEntity.Handle(ctx, req)
}

// panicDelivery Deliver 即 panic 的投递（PanicPolicyIsolate 隔离锚的载体）。
type panicDelivery struct {
	id     rans.ID
	stOnce sync.Once
	st     rans.Code
	called chan struct{}
}

func (d *panicDelivery) UUID() rans.ID              { return d.id }
func (d *panicDelivery) Deliver(_ *pawnite.Pawnite) { panic("boom-isolate") }
func (d *panicDelivery) Reject(st rans.Code, _ rans.Reason) {
	d.stOnce.Do(func() {
		d.st = st
		close(d.called)
	})
}

// TestHandoffIsolateDestroyNoHandoffAnchor（豁免锚）：PanicPolicyIsolate
// 的隔离销毁不走交接窗、不写快照——快照面归 manager 交接 core，隔离路径
// （isolateAfterPanic → stop shutdown）不经过任何 core；panic 意味着状态
// 不可信，下次激活应从上一份持久快照恢复。pawnite 层锚定：隔离销毁直达
// 终局、无迁移/交接痕迹（SkipDirUnregister=false）。
func TestHandoffIsolateDestroyNoHandoffAnchor(t *testing.T) {
	X := mustTestID(t, "acct", "ex-isolate")
	p := pawnite.New(X, pawnite.Options{
		Activate:    func(context.Context, *pawnite.Pawnite) error { return nil },
		PanicPolicy: pawnite.PanicPolicyIsolate,
	})
	p.Start(context.Background())
	waitFor(t, "raw pawnite active", func() bool { return p.State() == pawnite.StateActive })

	d := &panicDelivery{id: X, called: make(chan struct{})}
	if !p.ReceiveRequest(d) {
		t.Fatal("admit")
	}
	select {
	case <-d.called:
		if d.st != rans.CodeError {
			t.Fatalf("panic reject status = %v, want StatusError", d.st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("panicking delivery never rejected")
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("isolate destroy must reach terminal state")
	}
	if p.State() != pawnite.StateDestroyed {
		t.Fatalf("post-isolate state = %v, want StateDestroyed", p.State())
	}
	if p.SkipDirUnregister() {
		t.Fatal("isolate destroy must not carry migration semantics (no handoff window)")
	}
}

// TestMigrationLossErrorDeterministicCauseBeforeBudget：确定性拒绝（cause 不含取消签名——如
// ErrMigrationNoStore）优先于预算错误。预算与确定性拒绝同时成立时错误链
// 回精确原因，不被 DeadlineExceeded/Canceled 遮蔽。
//
// 断言面选点说明：Migrate 端到端形态对此窗口不可确定性构造——预算先于
// core 完成耗尽时，pawnite 的等待 select 即刻以 ctx.Err() 返回
// （caller-detached，migrationLossError 注释），cause 通道的发送与调用方
// 的读取存在固有竞争，先到期预算下的端到端测试要么测到 caller-detached
// 语义要么靠睡眠对齐（竞态）。故决策点以纯函数直测锚定，
// 与 pawnite 侧 TestHandoffLossErrorDeterministicRejectionNotShadowedByBudget
// 互为镜像；端到端两侧既有断言（TestHandoffStatefulNoStoreMatrix 预算
// 活跃的 NoStore 链、TestMigrate_BudgetExpiryLoses 预算耗尽的 ctx 签名
// 权威）保持不变。
func TestMigrationLossErrorDeterministicCauseBeforeBudget(t *testing.T) {
	expired, cancel := context.WithCancel(context.Background())
	cancel() // 预算已耗尽（显式取消——确定性前提）
	defer cancel()

	// 主断言：确定性拒绝 + 预算耗尽 → 精确原因胜出。
	err := migrationLossError(expired, context.Canceled, ErrMigrationNoStore)
	if !errors.Is(err, ErrMigrationNoStore) {
		t.Fatalf("err = %v, want chain carrying ErrMigrationNoStore (deterministic rejection must not be shadowed by exhausted budget)", err)
	}
	if !errors.Is(err, ErrMigrationLost) {
		t.Fatalf("err = %v, want ErrMigrationLost in chain", err)
	}

	// 对照 1（现状保留）：取消签名的 cause + 预算耗尽 → 预算错误权威
	//（并集坍缩嫌疑，TestMigrate_BudgetExpiryLoses 同语义——该测试预算是
	// 真 Deadline 故断言 DeadlineExceeded；本测试预算经 WithCancel 耗尽，
	// 权威即 context.Canceled）。
	err = migrationLossError(expired, context.Canceled, fmt.Errorf("transfer CAS failed: %w", context.DeadlineExceeded))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want budget error authority (context.Canceled) for ctx-signed cause", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, cause's DeadlineExceeded must not leak past the budget authority", err)
	}

	// 对照 2（现状保留）：cause 缺席 + 预算耗尽 → 预算错误（caller-detached）。
	err = migrationLossError(expired, context.Canceled, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want budget error when cause absent", err)
	}

	// 对照 3（现状保留）：预算活跃 + cause 在场 → ErrMigrationLost 链携带
	// cause（TestHandoffStatefulNoStoreMatrix 同语义）。
	err = migrationLossError(context.Background(), errors.New("pawnite: migration lost"), ErrMigrationNoStore)
	if !errors.Is(err, ErrMigrationNoStore) || !errors.Is(err, ErrMigrationLost) {
		t.Fatalf("err = %v, want ErrMigrationLost wrapping cause while budget alive", err)
	}
}
