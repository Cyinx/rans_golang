package pawnite

// 迁移状态机测试（规格 §13.1/§13.2 + §19 陷阱 3）：
// pending 先于 Destroying、留箱等待、core 在 loop goroutine 串行执行、
// Won 整箱转发且跳过目录注销、Lost 回退 Active、销毁抢占。

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// waitState 轮询等待稳定状态（Active/Destroyed 等稳态专用；
// 瞬态请用通道屏障，轮询可能采不到）。
func waitState(t *testing.T, p *Pawnite, want State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for p.State() != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.State() != want {
		t.Fatalf("state = %v, want %v", p.State(), want)
	}
}

// waitDestroying 轮询等待迁移窗开（Destroying 置位）。瞬态 Destroying 轮询
// 有采空风险，调用方必须同时持有屏障（core 阻塞 / loop 被占住）保证窗口
// 持续存在。
func waitDestroying(t *testing.T, p *Pawnite) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		destroying := p.state == StateDestroying
		p.mu.Unlock()
		if destroying {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatal("migration window (Destroying) never opened")
		}
		time.Sleep(time.Millisecond)
	}
}

// waitStopClosed 轮询等待 stop 通道已关闭（白盒 stopClosed 标志）。
func waitStopClosed(t *testing.T, p *Pawnite) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		closed := p.stopClosed
		p.mu.Unlock()
		if closed {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatal("stop channel never closed")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestMigrateTrap3_PendingBeforeDestroying 钉死陷阱 3（规格 §19.3 + §12 语义）：
// migrationPending 必须先于 Destroying 且同临界区落位——任何观测到
// Destroying 的时刻 pending 必为真（收件箱准入依赖该组合判留箱等待）。
// 同时钉：core 在 loop goroutine 上串行执行（串行即静默）；窗内准入留箱
// 不回绝；core 执行期间准入的消息在 Won 后整箱转发到目标节点。
func TestMigrateTrap3_PendingBeforeDestroying(t *testing.T) {
	id := mustUUID(t, "player", "mig1")
	coreStarted := make(chan struct{})
	releaseCore := make(chan struct{})
	core := func(ctx context.Context) MigrationOutcome {
		close(coreStarted)
		<-releaseCore
		return MigrationOutcome{Won: true}
	}
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	res := make(chan error, 1)
	go func() { res <- p.Migrate(context.Background(), 7, core) }()
	<-coreStarted // core 已在 loop 上运行：Destroying+pending 窗已开且稳定（core 阻塞中）

	// 确定性白盒断言：窗口内状态组合。
	p.mu.Lock()
	s, pend := p.state, p.handoffPending
	p.mu.Unlock()
	if s != StateDestroying || !pend {
		t.Fatalf("during migration window: state=%v pending=%v, want Destroying+true", s, pend)
	}
	// 密集采样加固（-race/-count=5 下反复锤）：Destroying ⟹ pending 无违例。
	dl := time.Now().Add(30 * time.Millisecond)
	for time.Now().Before(dl) {
		p.mu.Lock()
		s, pend := p.state, p.handoffPending
		p.mu.Unlock()
		if s == StateDestroying && !pend {
			t.Fatal("trap 3 violated: Destroying observed without migrationPending")
		}
	}
	// 行为面：窗内准入放行（留箱等待，§13.2"不转发会转回自己"）。
	d := newRec(id)
	if !p.ReceiveRequest(d) {
		t.Fatal("admission during migration window must be held-in-box, not rejected")
	}

	close(releaseCore)
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Won migration must reach terminal state")
	}
	if err := <-res; err != nil {
		t.Fatalf("Migrate(Won) error = %v, want nil", err)
	}
	// core 执行期间入箱的消息 → 整箱转发到目标节点。
	select {
	case w := <-d.forwarded:
		if w != 7 {
			t.Fatalf("forward target = %d, want 7", w)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("mid-core admission must be forwarded after Won")
	}
	if !p.SkipDirUnregister() {
		t.Fatal("Won migration must set SkipDirUnregister (CAS already transferred ownership)")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("post-Won state = %v, want StateDestroyed", p.State())
	}
}

// TestMigrateWonForwardsWholeInbox 钉死赢家整箱转发：core 内（loop goroutine）
// 准入的一组投递在 Won 后按 FIFO 逐条 Forward(目标)；非 Forwardable 按
// Unavailable 回绝且理由提及迁移；Done 关闭、跳过目录注销。
func TestMigrateWonForwardsWholeInbox(t *testing.T) {
	id := mustUUID(t, "player", "mig2")
	log := &fwdLog{}
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		InboxCap: 8,
	})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	plain := &plainDelivery{id: id, rejected: make(chan rejectInfo, 1)}
	core := func(ctx context.Context) MigrationOutcome {
		for i := 0; i < 3; i++ {
			d := newRec(id)
			seq := i
			d.onForward = func(w rans.NodeID) { log.record(seq, w) }
			if !p.ReceiveRequest(d) {
				t.Error("in-core admission must be held-in-box")
				return MigrationOutcome{}
			}
		}
		if !p.ReceiveRequest(plain) {
			t.Error("plain in-core admission must be held-in-box")
		}
		return MigrationOutcome{Won: true}
	}
	if err := p.Migrate(context.Background(), 9, core); err != nil {
		t.Fatalf("Migrate error = %v, want nil", err)
	}
	got := log.snapshot()
	if len(got) != 3 {
		t.Fatalf("forwards = %d, want 3", len(got))
	}
	for i, r := range got {
		if r.seq != i {
			t.Fatalf("forward FIFO broken: position %d carried seq %d", i, r.seq)
		}
		if r.winner != 9 {
			t.Fatalf("forward target = %d, want 9", r.winner)
		}
	}
	select {
	case r := <-plain.rejected:
		if r.st != rans.CodeUnavailable {
			t.Fatalf("non-forwardable reject status = %v, want StatusUnavailable", r.st)
		}
		if r.reason != rans.ReasonStaleLocation {
			t.Fatalf("reject reason = %v, want ReasonStaleLocation (migration-won fallback)", r.reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("non-forwardable delivery must be rejected on Won drain")
	}
	// 有界等待而非 select-default：Won 路径 runHandoff 的
	// req.result <- nil（缓冲非阻塞）先于 run 的 defer p.closeDone()——
	// Migrate 返回后瞬时 default 断言可能落在裁决回传与 closeDone 之间
	// 的调度窗，偶发误报；Done 恒闭合，只是闭合点在返回之后。
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done must be closed after Won")
	}
	if !p.SkipDirUnregister() {
		t.Fatal("Won must set SkipDirUnregister")
	}
}

// TestMigrateLostRestoresActive 输路径：清 pending + 回退 Active；后续投递
// 正常执行；不跳过目录注销；Done 不关（单元继续服役）。
func TestMigrateLostRestoresActive(t *testing.T) {
	id := mustUUID(t, "player", "mig3")
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	err := p.Migrate(context.Background(), 9, func(ctx context.Context) MigrationOutcome {
		return MigrationOutcome{} // Lost
	})
	if !errors.Is(err, ErrMigrationLost) {
		t.Fatalf("Migrate error = %v, want ErrMigrationLost", err)
	}
	if p.State() != StateActive {
		t.Fatalf("post-Lost state = %v, want StateActive", p.State())
	}
	if p.SkipDirUnregister() {
		t.Fatal("lost migration must not skip directory unregister")
	}
	d := newRec(id)
	if !p.ReceiveRequest(d) {
		t.Fatal("post-rollback admission must succeed")
	}
	select {
	case <-d.delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("post-rollback delivery must execute normally")
	}
	select {
	case <-p.Done():
		t.Fatal("Done must NOT close after lost migration")
	default:
	}
	p.Destroy(context.Background())
}

// TestMigrateRequiresActiveAndCore 前置校验：非 Active（Created / 迁移窗内）
// 一律 ErrNotActive；nil core 直接报错。
func TestMigrateRequiresActiveAndCore(t *testing.T) {
	id := mustUUID(t, "player", "mig4")
	// Created（未 Start）→ ErrNotActive。
	p0 := New(id, Options{})
	if err := p0.Migrate(context.Background(), 9, func(context.Context) MigrationOutcome {
		return MigrationOutcome{Won: true}
	}); !errors.Is(err, ErrNotActive) {
		t.Fatalf("created-state error = %v, want ErrNotActive", err)
	}

	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	if err := p.Migrate(context.Background(), 9, nil); err == nil {
		t.Fatal("nil core must return an error")
	}

	coreStarted := make(chan struct{})
	release := make(chan struct{})
	res := make(chan error, 1)
	go func() {
		res <- p.Migrate(context.Background(), 9, func(ctx context.Context) MigrationOutcome {
			close(coreStarted)
			<-release
			return MigrationOutcome{Won: true}
		})
	}()
	<-coreStarted // 窗已开（Destroying+pending）
	if err := p.Migrate(context.Background(), 8, func(context.Context) MigrationOutcome {
		return MigrationOutcome{}
	}); !errors.Is(err, ErrNotActive) {
		t.Fatalf("in-window second migrate error = %v, want ErrNotActive", err)
	}
	close(release)
	if err := <-res; err != nil {
		t.Fatalf("first migration error = %v, want nil", err)
	}
	<-p.Done()
}

// TestMigrateHoldInBoxPreCoreRace 迁移信号入队前已在箱消息的归宿（Go 形态
// 与规格 FIFO 的偏差，见 runHandoff 实现注释）：select 对 inbox/migrateCh 的随机选择下，排队
// 消息既可能在 core 前本地执行（合法工作——CAS 才是线性化点），也可能在
// Won 后被整箱转发——恰好一次，绝不回绝。
func TestMigrateHoldInBoxPreCoreRace(t *testing.T) {
	id := mustUUID(t, "player", "mig5")
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		InboxCap: 8,
	})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	d1, d2 := newRec(id), newRec(id)
	p.ReceiveRequest(&enterBlockDelivery{id: id, entered: entered, release: block}) // 占住 loop
	<-entered
	p.ReceiveRequest(d1) // 迁移前已在箱

	res := make(chan error, 1)
	go func() {
		res <- p.Migrate(context.Background(), 9, func(context.Context) MigrationOutcome {
			return MigrationOutcome{Won: true}
		})
	}()
	waitDestroying(t, p) // 窗开；core 未跑（loop 仍被占）

	if !p.ReceiveRequest(d2) {
		t.Fatal("admission during migration window must be held-in-box, not rejected")
	}
	close(block) // 放行：loop 在 inbox 与 migrateCh 间随机选择
	<-p.Done()
	if err := <-res; err != nil {
		t.Fatalf("Migrate error = %v, want nil", err)
	}
	for i, d := range []*recDelivery{d1, d2} {
		executed, forwarded := false, false
		select {
		case <-d.delivered:
			executed = true
		default:
		}
		select {
		case w := <-d.forwarded:
			if w != 9 {
				t.Fatalf("delivery %d forward target = %d, want 9", i, w)
			}
			forwarded = true
		default:
		}
		if executed == forwarded {
			t.Fatalf("delivery %d must be exactly one of executed/forwarded (executed=%v forwarded=%v)",
				i, executed, forwarded)
		}
		select {
		case r := <-d.rejected:
			t.Fatalf("held-in-box delivery %d must never be rejected: %v %q", i, r.st, r.reason)
		default:
		}
	}
}

// TestMigrateBudgetCancelLoses 30s 预算外（ctx 取消）：core 观测取消返回
// Lost → 回退 Active；Migrate 回传 ctx.Err()。
func TestMigrateBudgetCancelLoses(t *testing.T) {
	id := mustUUID(t, "player", "mig6")
	ctx, cancel := context.WithCancel(context.Background())
	coreStarted := make(chan struct{})
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	res := make(chan error, 1)
	go func() {
		res <- p.Migrate(ctx, 9, func(c context.Context) MigrationOutcome {
			close(coreStarted)
			<-c.Done() // core 契约：尊重预算取消，取消即输
			return MigrationOutcome{}
		})
	}()
	<-coreStarted
	cancel()
	select {
	case err := <-res:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Migrate error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Migrate must return after budget cancel")
	}
	waitState(t, p, StateActive)
	p.Destroy(context.Background())
}

// TestDestroyDuringPendingMigration_AbortsCore 销毁抢占未及执行的迁移：
// stop 关闭 → loop 的 stop 分支回输在飞迁移请求（输），core 绝不执行；
// 窗内留箱消息按销毁语义回绝（迁移已输，无新家可转）；正常注销（不跳过）。
// 同时钉死：Destroy 在迁移窗内必须关 stop 抢占——只等 done 会因输路径回退
// Active 而永久挂死等待方。
func TestDestroyDuringPendingMigration_AbortsCore(t *testing.T) {
	id := mustUUID(t, "player", "mig7")
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	var coreRan atomic.Bool
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	p.ReceiveRequest(&enterBlockDelivery{id: id, entered: entered, release: block})
	<-entered

	res := make(chan error, 1)
	go func() {
		res <- p.Migrate(context.Background(), 9, func(context.Context) MigrationOutcome {
			coreRan.Store(true)
			return MigrationOutcome{Won: true}
		})
	}()
	waitDestroying(t, p) // 窗开；core 因 loop 被占未跑

	d2 := newRec(id)
	if !p.ReceiveRequest(d2) {
		t.Fatal("admission during migration window must be held-in-box")
	}

	destroyed := make(chan struct{})
	go func() {
		p.Destroy(context.Background())
		close(destroyed)
	}()
	waitStopClosed(t, p) // 销毁抢占落地后才放行，保 core 必不执行
	close(block)
	<-p.Done()
	<-destroyed

	if coreRan.Load() {
		t.Fatal("core must not run when Destroy preempts a pending migration")
	}
	if err := <-res; !errors.Is(err, ErrMigrationLost) {
		t.Fatalf("Migrate error = %v, want ErrMigrationLost", err)
	}
	if p.SkipDirUnregister() {
		t.Fatal("aborted migration must not skip directory unregister")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed", p.State())
	}
	select {
	case r := <-d2.rejected:
		if r.st != rans.CodeUnavailable {
			t.Fatalf("aborted-migration inbox reject status = %v, want StatusUnavailable", r.st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("held-in-box message must be rejected when migration aborted by destroy")
	}
}

// TestDestroyDuringCoreWonStillTerminal core 执行中介入的 Destroy：core 已
// 越过起点，结果照常处置——Won → 终局 + 跳过注销（销毁意图由终态满足）。
func TestDestroyDuringCoreWonStillTerminal(t *testing.T) {
	id := mustUUID(t, "player", "mig8")
	coreStarted := make(chan struct{})
	releaseCore := make(chan struct{})
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	res := make(chan error, 1)
	go func() {
		res <- p.Migrate(context.Background(), 9, func(ctx context.Context) MigrationOutcome {
			close(coreStarted)
			<-releaseCore
			return MigrationOutcome{Won: true}
		})
	}()
	<-coreStarted

	destroyed := make(chan struct{})
	go func() {
		p.Destroy(context.Background())
		close(destroyed)
	}()
	waitStopClosed(t, p)
	close(releaseCore)
	<-p.Done()
	<-destroyed

	if err := <-res; err != nil {
		t.Fatalf("Migrate error = %v, want nil (core already won)", err)
	}
	if !p.SkipDirUnregister() {
		t.Fatal("won migration must skip directory unregister even with destroy racing")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed", p.State())
	}
}

// TestDestroyDuringCoreLostTerminal core 执行中介入 Destroy 且 core 输：
// 不回退 Active（销毁已介入），走销毁终局，正常注销。
func TestDestroyDuringCoreLostTerminal(t *testing.T) {
	id := mustUUID(t, "player", "mig9")
	coreStarted := make(chan struct{})
	releaseCore := make(chan struct{})
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	res := make(chan error, 1)
	go func() {
		res <- p.Migrate(context.Background(), 9, func(ctx context.Context) MigrationOutcome {
			close(coreStarted)
			<-releaseCore
			return MigrationOutcome{} // Lost
		})
	}()
	<-coreStarted

	destroyed := make(chan struct{})
	go func() {
		p.Destroy(context.Background())
		close(destroyed)
	}()
	waitStopClosed(t, p)
	close(releaseCore)
	<-p.Done()
	<-destroyed

	if err := <-res; !errors.Is(err, ErrMigrationLost) {
		t.Fatalf("Migrate error = %v, want ErrMigrationLost", err)
	}
	if p.SkipDirUnregister() {
		t.Fatal("lost migration must not skip directory unregister")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed", p.State())
	}
}

// TestMigrateWonSweepForwardsLatecomers 终局复扫的迁移赢家路径：finish 内
// Deactivate 钩子运行时仍处迁移窗（Destroying+pending），此刻准入的迟到者
// 必须被终局复扫整箱转发而非回绝（§13.2 留箱等待的收尾）。
func TestMigrateWonSweepForwardsLatecomers(t *testing.T) {
	id := mustUUID(t, "player", "mig10")
	late := newRec(id)
	admitted := false
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		Deactivate: func(ctx context.Context, p *Pawnite) error {
			// finish 序：Deactivate 在终局复扫之前——迁移窗仍开，准入放行。
			admitted = p.ReceiveRequest(late)
			return nil
		},
	})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	if err := p.Migrate(context.Background(), 11, func(context.Context) MigrationOutcome {
		return MigrationOutcome{Won: true}
	}); err != nil {
		t.Fatalf("Migrate error = %v, want nil", err)
	}
	if !admitted {
		t.Fatal("late admission inside finish (Deactivate hook) must be held-in-box")
	}
	select {
	case w := <-late.forwarded:
		if w != 11 {
			t.Fatalf("late forward target = %d, want 11", w)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal sweep must forward latecomers on Won, not reject")
	}
	// 有界等待而非 select-default：同 TestMigrateWonForwardsWholeInbox——
	// Won 裁决回传先于 closeDone defer，瞬时断言有误报窗。
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done must be closed")
	}
}

// TestMigrateWonStragglerAfterTerminalForwards 迁移赢家的准入面迟到者
// （TestArbitrationLostStragglerAfterTerminalForwards 的对称钉）：调度交错把
// 调用方帧压到 Won 终局翻转之后才敲源端收件箱——调用方
// 位置是换锁前源端的合法观测，不得按销毁回绝：Forwardable 投递转发给
// 迁移目标节点；非 Forwardable 按 Unavailable 回绝且理由提及迁移（与
// 整箱转发的回绝分支同口径）。
func TestMigrateWonStragglerAfterTerminalForwards(t *testing.T) {
	id := mustUUID(t, "player", "won-straggler")
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	if err := p.Migrate(context.Background(), 7, func(context.Context) MigrationOutcome {
		return MigrationOutcome{Won: true}
	}); err != nil {
		t.Fatalf("Migrate error = %v, want nil", err)
	}
	// Done 关闭在 loop goroutine 的 defer（Won 裁决回传 result 之后）——
	// Migrate 返回与 closeDone 之间存在调度窗，
	// 有界等待终态可见，不做瞬时 select-default 检查。
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Won migration must close Done (terminal flipped)")
	}

	d := newRec(id)
	if p.ReceiveRequest(d) {
		t.Fatal("straggler after Won terminal state must not be admitted")
	}
	select {
	case w := <-d.forwarded:
		if w != 7 {
			t.Fatalf("forwardable straggler forwarded to node %d, want 7", w)
		}
	case r := <-d.rejected:
		t.Fatalf("forwardable straggler must forward to the migration target, got reject %v %q", r.st, r.reason)
	case <-time.After(2 * time.Second):
		t.Fatal("straggler neither forwarded nor rejected")
	}

	pd := &plainDelivery{id: id, rejected: make(chan rejectInfo, 1)}
	if p.ReceiveRequest(pd) {
		t.Fatal("non-forwardable straggler after Won terminal state must not be admitted")
	}
	select {
	case r := <-pd.rejected:
		if r.st != rans.CodeUnavailable {
			t.Fatalf("non-forwardable straggler reject status = %v, want StatusUnavailable", r.st)
		}
		if r.reason != rans.ReasonStaleLocation {
			t.Fatalf("reject reason = %v, want ReasonStaleLocation (migration-won fallback)", r.reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("non-forwardable straggler must be rejected")
	}
}

// TestMigrateDestroyRaceNoHang 并发竞速守卫：Migrate 与 Destroy 同时
// 发起时，两者都必须有界返回（无泄漏回复、无挂起）。裁决三种皆合法
// （Won-nil / ErrNotActive / ErrMigrationLost），断言仅"不挂起"。
// 注：入队位于锁内的正确性由 mu 串行化证明保证（见 migrate.go 注释），
// 本测试是对并发竞速语义的概率性守卫，不是对该顺序的确定性钉——
// 黑盒无法构造"入队晚于 stop 收尾"的交错（该窗口只在入队置于锁外时
// 存在于 Migrate 内部）。
func TestMigrateDestroyRaceNoHang(t *testing.T) {
	for i := 0; i < 100; i++ {
		id := mustUUID(t, "player", "migrace")
		p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
		p.Start(context.Background())
		waitState(t, p, StateActive)

		res := make(chan error, 1)
		go func() {
			res <- p.Migrate(context.Background(), 9, func(context.Context) MigrationOutcome {
				return MigrationOutcome{Won: true}
			})
		}()
		p.Destroy(context.Background())
		select {
		case err := <-res:
			if err != nil && !errors.Is(err, ErrNotActive) && !errors.Is(err, ErrMigrationLost) {
				t.Fatalf("iter %d: unexpected Migrate error = %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("iter %d: Migrate hung (destroy/migrate enqueue race leaked a reply)", i)
		}
		<-p.Done()
	}
}

// TestMigrateWaitHonorsCtx_WhenCoreStuck 回归（fix round 1 审查修复 1）：
// core 卡死（迁移信号已被 loop 消费，abortPendingMigration 只能回输仍在
// migrateCh 排队的信号）时，裸 <-req.result 会把调用方与看门狗收敛后的
// zombie 一起永久挂死——等待段必须绑定调用方预算。断言：短预算 ctx →
// 有界返回 DeadlineExceeded；看门狗强弃仍收敛终态（Destroy 等待方解绑）；
// zombie core 迟来回写孤儿 result（缓冲落空）无二次效应。
func TestMigrateWaitHonorsCtx_WhenCoreStuck(t *testing.T) {
	id := mustUUID(t, "player", "mig12")
	coreStarted := make(chan struct{})
	releaseCore := make(chan struct{})
	var evicts atomic.Int32
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		OnEvict:  func(p *Pawnite) { evicts.Add(1) },
	})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	res := make(chan error, 1)
	go func() {
		res <- p.Migrate(ctx, 9, func(c context.Context) MigrationOutcome {
			close(coreStarted)
			<-releaseCore // 卡死：不看预算 ctx（如快照捕获无 ctx 可观测）
			return MigrationOutcome{}
		})
	}()
	<-coreStarted // 信号已被 loop 消费、core 卡在 loop 上（Destroying+pending 稳定）

	select {
	case err := <-res:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Migrate error = %v, want DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Migrate hung: wait must honor caller ctx when the migration core is stuck")
	}

	// 看门狗强弃（卸载看门狗管辖：Destroying 停驻）仍收敛终态并解绑等待方。
	if !p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("watchdog abandon must own the stuck-core Destroying window")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed", p.State())
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done must be closed by abandon")
	}
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count = %d, want 1", got)
	}
	// Destroy 重试：done 已关，立即 nil。
	if err := p.Destroy(context.Background()); err != nil {
		t.Fatalf("retry Destroy err = %v, want nil", err)
	}

	// zombie core 迟来返回：runMigration 续体写孤儿 result（容量 1 缓冲落空，
	// 不阻塞 loop）、loop 让位退出，无二次终局效应。
	close(releaseCore)
	time.Sleep(100 * time.Millisecond)
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count after zombie core return = %d, want 1", got)
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state after zombie core return = %v, want StateDestroyed", p.State())
	}
}

// TestLastDeliveryTime 闲置时钟（规格 §4.1 _lastDeliveryTickCount）：未投递
// 为零值；投递后更新且不回退。
func TestLastDeliveryTime(t *testing.T) {
	id := mustUUID(t, "player", "mig11")
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	if !p.LastDeliveryTime().IsZero() {
		t.Fatal("LastDeliveryTime must be zero before any delivery")
	}
	d1 := newRec(id)
	p.ReceiveRequest(d1)
	select {
	case <-d1.delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery never ran")
	}
	first := p.LastDeliveryTime()
	if first.IsZero() {
		t.Fatal("LastDeliveryTime must be set after a delivery")
	}
	time.Sleep(5 * time.Millisecond)
	d2 := newRec(id)
	p.ReceiveRequest(d2)
	select {
	case <-d2.delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("second delivery never ran")
	}
	second := p.LastDeliveryTime()
	if second.Before(first) {
		t.Fatalf("LastDeliveryTime regressed: first=%v second=%v", first, second)
	}
	p.Destroy(context.Background())
}

// TestAbandonInWonWindowForwardsLatecomersToTarget：CAS 已赢
// （所有权已归目标节点）后卡死在 finish 的 Deactivate 段——卸载看门狗强弃
// 时，在箱迟到者的处置必须与 finish 复扫/准入面迟到者一致：转投目标节点
// （强弃点若一律 Reject(Timeout)，同一时刻到达的消息会因路径不同得到不同
// 处置——Timeout 可重试无丢失，但白付一轮失败往返）。
func TestAbandonInWonWindowForwardsLatecomersToTarget(t *testing.T) {
	id := mustUUID(t, "player", "abwon2")
	entered := make(chan struct{})
	release := make(chan struct{})
	p := New(id, Options{
		Activate:   func(ctx context.Context, p *Pawnite) error { return nil },
		Deactivate: func(ctx context.Context, p *Pawnite) error { close(entered); <-release; return nil },
		InboxCap:   8,
	})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	res := make(chan error, 1)
	go func() {
		res <- p.Migrate(context.Background(), 9, func(ctx context.Context) MigrationOutcome {
			return MigrationOutcome{Won: true}
		})
	}()
	<-entered // Won 已裁决、整箱转发已跑、finish 卡在 Deactivate（窗仍放行留箱）
	late := newRec(id)
	if !p.ReceiveRequest(late) {
		t.Fatal("won window must hold latecomers in box")
	}
	if !p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("abandon must win on stuck Destroying")
	}
	select {
	case w := <-late.forwarded:
		if w != 9 {
			t.Fatalf("forward target = %d, want 9", w)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("latecomer held in won box must be forwarded to migration target on abandon")
	}
	close(release)
	if err := <-res; err != nil { // zombie 续体收尾须无死锁/panic
		t.Fatalf("Migrate result = %v, want nil", err)
	}
}
