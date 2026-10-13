package pawnite

// 强弃面测试（规格 §14.2 双看门狗 + §19 陷阱 12 单写者纪律）：
//   - ExecutingSince/DestroyingSince 时钟：置位/清零窗口；
//   - Abandon(StateDestroying)：卡死销毁（Deactivate 永阻塞 / 销毁窗内
//     invoke 永阻塞）→ 置终态 + done 关闭解绑等待方 + 在箱/在途回绝
//     （Timeout 类）+ zombie 续体幂等（OnEvict 恰一次、无 panic）；
//   - Abandon(StateActive)：Active 执行卡死 → 同临界区直达终态
//     （无 Destroying 中间态可观察）+ 调用方解绑 + zombie 无害；
//   - 陷阱 12：并发 Destroy vs 强弃恰一赢家（gate 内让位）。

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// rejectRecorder 包裹任一投递并记录 Reject 调用（不透传——被包裹者的
// 回绝处置对断言无意义）。Deliver 原样委托。
type rejectRecorder struct {
	inner InboundDelivery
	mu    sync.Mutex
	n     int
	st    rans.Code
	rs    rans.Reason
}

func (r *rejectRecorder) UUID() rans.ID      { return r.inner.UUID() }
func (r *rejectRecorder) Deliver(p *Pawnite) { r.inner.Deliver(p) }
func (r *rejectRecorder) Reject(st rans.Code, reason rans.Reason) {
	r.mu.Lock()
	r.n++
	r.st, r.rs = st, reason
	r.mu.Unlock()
}
func (r *rejectRecorder) rejects() (int, rans.Code) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n, r.st
}

// TestExecutingSince_WindowAroundInvoke 执行时钟：空闲零值 → invoke 期间
// 非零 → 完成后归零。
func TestExecutingSince_WindowAroundInvoke(t *testing.T) {
	id := mustUUID(t, "player", "es1")
	entered := make(chan struct{})
	release := make(chan struct{})
	p := New(id, Options{})
	p.Start(context.Background())
	waitForState(t, p, StateActive)

	if !p.ExecutingSince().IsZero() {
		t.Fatal("ExecutingSince must be zero when idle")
	}
	if !p.ReceiveRequest(&enterBlockDelivery{id: id, entered: entered, release: release}) {
		t.Fatal("admission must succeed")
	}
	<-entered
	if p.ExecutingSince().IsZero() {
		t.Fatal("ExecutingSince must be set while a delivery executes")
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for !p.ExecutingSince().IsZero() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !p.ExecutingSince().IsZero() {
		t.Fatal("ExecutingSince must be cleared after the delivery completes")
	}
	p.Destroy(context.Background())
}

// TestStateSince_TracksStateEntry 统一状态进驻时钟（全状态转移盖戳的
// 单一时钟）：Created 零值 → run 起步 Activating 盖戳 → Active 重盖 →
// Destroying 重盖 → Destroyed 清零。握手 channel 钉各停驻窗，不用 sleep。
func TestStateSince_TracksStateEntry(t *testing.T) {
	id := mustUUID(t, "player", "es2")
	actEntered := make(chan struct{})
	actRelease := make(chan struct{})
	deactEntered := make(chan struct{})
	deactRelease := make(chan struct{})
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error {
			close(actEntered)
			<-actRelease
			return nil
		},
		Deactivate: func(ctx context.Context, p *Pawnite) error {
			close(deactEntered)
			<-deactRelease
			return nil
		},
	})
	if !p.StateSince().IsZero() {
		t.Fatal("StateSince must be zero before Start (Created)")
	}
	p.Start(context.Background())
	<-actEntered
	if p.State() != StateActivating {
		t.Fatalf("state = %v, want StateActivating", p.State())
	}
	activatingSince := p.StateSince()
	if activatingSince.IsZero() {
		t.Fatal("StateSince must be stamped while Activating")
	}

	close(actRelease)
	waitForState(t, p, StateActive)
	if !p.StateSince().After(activatingSince) {
		t.Fatal("StateSince must be restamped at Active entry")
	}

	go p.Destroy(context.Background())
	<-deactEntered
	if p.State() != StateDestroying {
		t.Fatalf("state = %v, want StateDestroying", p.State())
	}
	if p.StateSince().IsZero() {
		t.Fatal("StateSince must be stamped while stuck in Destroying")
	}
	close(deactRelease)
	<-p.Done()
	if !p.StateSince().IsZero() {
		t.Fatal("StateSince must be zero after terminal state")
	}
}

// TestAbandon_Destroying_UnblocksWaiters 卡死 Deactivate（销毁路径
// 停在 finish 的 safeDeactivate 内）：强弃 → 立即终态 + done 关闭（Destroy
// 等待方解绑）+ OnEvict 恰一次；zombie Deactivate 事后返回不得二次收尾。
func TestAbandon_Destroying_UnblocksWaiters(t *testing.T) {
	id := mustUUID(t, "player", "ab1")
	entered := make(chan struct{})
	release := make(chan struct{})
	var evicts atomic.Int32
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		Deactivate: func(ctx context.Context, p *Pawnite) error {
			close(entered)
			<-release
			return nil
		},
		OnEvict: func(p *Pawnite) { evicts.Add(1) },
	})
	p.Start(context.Background())
	waitForState(t, p, StateActive)

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		errCh <- p.Destroy(ctx)
	}()
	<-entered // Deactivate 已卡死（loop 停在 finish 内）
	if err := <-errCh; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Destroy err = %v, want DeadlineExceeded", err)
	}
	select {
	case <-p.Done():
		t.Fatal("Done must not be closed while Deactivate is stuck")
	default:
	}

	if !p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("Abandon must win on a stuck Destroying pawnite")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state after abandon = %v, want StateDestroyed", p.State())
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done must be closed by abandon (unblock Destroy waiters)")
	}
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count = %d, want 1", got)
	}
	// 重试 Destroy：等待段立即 nil（done 已关）。
	if err := p.Destroy(context.Background()); err != nil {
		t.Fatalf("retry Destroy err = %v, want nil", err)
	}
	// 二次强弃：终态已落，gate 让位（no-op）。
	if p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("second abandon must no-op on already Destroyed pawnite")
	}

	close(release) // zombie Deactivate 返回 → finish 续体必须幂等
	time.Sleep(100 * time.Millisecond)
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count after zombie continuation = %d, want 1", got)
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state after zombie continuation = %v, want StateDestroyed", p.State())
	}
}

// TestAbandon_Destroying_RejectsQueuedAndExecuting 销毁窗内 invoke
// 卡死（stop 已关、loop 停在 Deliver）：在箱滞留者与在途执行者都在强弃时
// 以 Timeout 类回绝解绑，在箱者绝不再被投递。
func TestAbandon_Destroying_RejectsQueuedAndExecuting(t *testing.T) {
	id := mustUUID(t, "player", "ab2")
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var evicts atomic.Int32
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		OnEvict:  func(p *Pawnite) { evicts.Add(1) },
	})
	p.Start(context.Background())
	waitForState(t, p, StateActive)

	exec := &rejectRecorder{inner: &enterBlockDelivery{id: id, entered: entered, release: release}}
	if !p.ReceiveRequest(exec) {
		t.Fatal("admission of executing delivery must succeed")
	}
	<-entered // loop 已在 exec.Deliver 内
	queued := &rejectRecorder{inner: &testDelivery{id: id}}
	if !p.ReceiveRequest(queued) {
		t.Fatal("admission of queued delivery must succeed")
	}
	go p.Destroy(context.Background())
	deadline := time.Now().Add(2 * time.Second)
	for p.State() != StateDestroying && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.State() != StateDestroying {
		t.Fatal("destroy never entered Destroying")
	}

	if !p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("abandon must win")
	}
	if n, st := exec.rejects(); n != 1 || st != rans.CodeTimeout {
		t.Fatalf("executing delivery rejects = (%d, %v), want (1, StatusTimeout)", n, st)
	}
	if n, st := queued.rejects(); n != 1 || st != rans.CodeTimeout {
		t.Fatalf("queued delivery rejects = (%d, %v), want (1, StatusTimeout)", n, st)
	}
	if td, ok := queued.inner.(*testDelivery); ok && td.delivered() != 0 {
		t.Fatal("queued delivery must never execute after abandon")
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done must be closed")
	}
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count = %d, want 1", got)
	}

	close(release) // zombie invoke 返回 → loop 让位于强弃（不再 stopShutdown 收尾）
	time.Sleep(100 * time.Millisecond)
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count after zombie continuation = %d, want 1", got)
	}
	if n, _ := exec.rejects(); n != 1 {
		t.Fatalf("executing delivery rejected %d times, want exactly 1", n)
	}
}

// TestAbandon_Destroying_RefusesNonDestroying gate 前置校验：
// Active / Destroyed 状态下强弃必须让位（返回 false，零副作用）。
func TestAbandon_Destroying_RefusesNonDestroying(t *testing.T) {
	id := mustUUID(t, "player", "ab3")
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	if p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("abandon must refuse before Start")
	}
	p.Start(context.Background())
	waitForState(t, p, StateActive)
	if p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("abandon must refuse an Active pawnite")
	}
	p.Destroy(context.Background())
	if p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("abandon must refuse a Destroyed pawnite")
	}
}

// TestAbandon_Active_TerminalWithoutDestroyingWindow 执行卡死
// 强弃：Active→Destroying→Destroyed 同临界区完成——密集采样器绝无
// Destroying 中间态观测；在途/在箱调用方解绑（Timeout 类）；OnEvict 恰一次。
func TestAbandon_Active_TerminalWithoutDestroyingWindow(t *testing.T) {
	id := mustUUID(t, "player", "fa1")
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var evicts atomic.Int32
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		OnEvict:  func(p *Pawnite) { evicts.Add(1) },
	})
	p.Start(context.Background())
	waitForState(t, p, StateActive)

	exec := &rejectRecorder{inner: &enterBlockDelivery{id: id, entered: entered, release: release}}
	if !p.ReceiveRequest(exec) {
		t.Fatal("admission must succeed")
	}
	<-entered

	// 密集采样器：全程不得观测 Destroying（终态由单次持锁多转移保证）。
	var sawDestroying atomic.Bool
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		for {
			switch p.State() {
			case StateDestroying:
				sawDestroying.Store(true)
			case StateDestroyed:
				return
			}
		}
	}()
	time.Sleep(20 * time.Millisecond) // 采样器先跑起来（观测到 Active）
	if !p.Abandon(StateActive, TerminalAbandoned) {
		t.Fatal("force abandon must win on Active pawnite")
	}
	<-samplerDone
	if sawDestroying.Load() {
		t.Fatal("Destroying intermediate state was observable; both transitions must share one critical section")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed", p.State())
	}
	if n, st := exec.rejects(); n != 1 || st != rans.CodeTimeout {
		t.Fatalf("executing delivery rejects = (%d, %v), want (1, StatusTimeout)", n, st)
	}
	after := &testDelivery{id: id}
	if p.ReceiveRequest(after) {
		t.Fatal("admission after force abandon must fail")
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done must be closed")
	}
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count = %d, want 1", got)
	}

	close(release) // zombie invoke 返回：loop 让位，无二次收尾
	time.Sleep(100 * time.Millisecond)
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count after zombie continuation = %d, want 1", got)
	}
}

// TestAbandon_Active_RefusesNonActive gate：Destroying 已置
// （销毁在途）时执行强弃让位常规路径。
func TestAbandon_Active_RefusesNonActive(t *testing.T) {
	id := mustUUID(t, "player", "fa2")
	entered := make(chan struct{})
	release := make(chan struct{})
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		Deactivate: func(ctx context.Context, p *Pawnite) error {
			close(entered)
			<-release
			return nil
		},
	})
	p.Start(context.Background())
	waitForState(t, p, StateActive)

	go p.Destroy(context.Background())
	<-entered // Destroying 已置（Deactivate 卡死）
	if p.Abandon(StateActive, TerminalAbandoned) {
		t.Fatal("force abandon must yield when Destroying is already set (trap 12)")
	}
	if p.State() != StateDestroying {
		t.Fatalf("state = %v, want StateDestroying (destroy owns the terminal)", p.State())
	}
	// 该场景归卸载看门狗管辖：Abandon(StateDestroying) 接手。
	if !p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("unload watchdog abandon must own the stuck Destroying case")
	}
	close(release)
}

// TestAbandon_Active_RefusesIdleActive 回归（fix round 1 审查
// 修复 2）：gate 只看 state==Active 会因看门狗快照陈旧误杀"恰在裁决时刻
// 完成执行"的健康实体——gate 内必须复检执行时钟：空闲（零值）即让位
// （false，零副作用）。真实卡死（时钟非零）时强弃照常胜出。
func TestAbandon_Active_RefusesIdleActive(t *testing.T) {
	id := mustUUID(t, "player", "fa3")
	var evicts atomic.Int32
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		OnEvict:  func(p *Pawnite) { evicts.Add(1) },
	})
	p.Start(context.Background())
	waitForState(t, p, StateActive)

	if !p.ExecutingSince().IsZero() {
		t.Fatal("precondition: idle Active pawnite must report zero executing clock")
	}
	if p.Abandon(StateActive, TerminalAbandoned) {
		t.Fatal("force abandon must refuse an idle Active pawnite (stale watchdog snapshot)")
	}
	if p.State() != StateActive {
		t.Fatalf("state = %v, want StateActive (refusal must be side-effect free)", p.State())
	}
	select {
	case <-p.Done():
		t.Fatal("Done must not close on refused force abandon")
	default:
	}
	if got := evicts.Load(); got != 0 {
		t.Fatalf("OnEvict count = %d, want 0", got)
	}

	// 空闲让位不削弱真实卡死场景：占住 loop 后（时钟非零）强弃照常胜出。
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	if !p.ReceiveRequest(&enterBlockDelivery{id: id, entered: entered, release: release}) {
		t.Fatal("admission must succeed")
	}
	<-entered
	if !p.Abandon(StateActive, TerminalAbandoned) {
		t.Fatal("force abandon must win on a genuinely stuck execution")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed", p.State())
	}
	close(release)
}

// TestTrap12_ConcurrentDestroyVsAbandon_ExactlyOneWinner 陷阱 12 钉死：
// 并发 Destroy/两种强弃混跑，终局效应恰一次（OnEvict 一次、done 单次关闭
// 不 panic），状态机无重复转移。多轮摇晃交错。
//
// 前提：loop 被卡死投递占住（执行时钟非零）——强杀 gate 复检执行时钟后，
// 空闲实体的 Abandon 让位，本测试的强杀分支须真实卡死才有效。
// Destroy 等待段有界 + wg 后确定性收敛兜底：窄交错（全部强弃在
// Destroying 落位前跑完、loop 又被占住推不动 finish）下等待方等不到
// done，由兜底补刀（真实看门狗下一轮扫描的语义），测试不挂死。
func TestTrap12_ConcurrentDestroyVsAbandon_ExactlyOneWinner(t *testing.T) {
	for round := 0; round < 8; round++ {
		id := mustUUID(t, "player", "t12")
		execEntered := make(chan struct{}, 1)
		release := make(chan struct{})
		var evicts atomic.Int32
		p := New(id, Options{
			Activate: func(ctx context.Context, p *Pawnite) error { return nil },
			Deactivate: func(ctx context.Context, p *Pawnite) error {
				<-release
				return nil
			},
			OnEvict: func(p *Pawnite) { evicts.Add(1) },
		})
		p.Start(context.Background())
		waitForState(t, p, StateActive)

		if !p.ReceiveRequest(&enterBlockDelivery{id: id, entered: execEntered, release: release}) {
			t.Fatalf("round %d: admission of stuck delivery must succeed", round)
		}
		<-execEntered

		const n = 24
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				switch i % 3 {
				case 0:
					ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
					defer cancel()
					p.Destroy(ctx)
				case 1:
					p.Abandon(StateDestroying, TerminalAbandoned)
				case 2:
					p.Abandon(StateActive, TerminalAbandoned)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		// 确定性收敛兜底：窄交错下 goroutine 混跑可能无人置终态（真实看门狗
		// 下一轮扫描会补刀），此处就地补上再断言。
		if p.State() != StateDestroyed {
			p.Abandon(StateDestroying, TerminalAbandoned)
		}

		if p.State() != StateDestroyed {
			t.Fatalf("round %d: state = %v, want StateDestroyed", round, p.State())
		}
		select {
		case <-p.Done():
		default:
			t.Fatalf("round %d: Done must be closed", round)
		}
		if got := evicts.Load(); got != 1 {
			t.Fatalf("round %d: OnEvict count = %d, want exactly 1", round, got)
		}
		close(release) // 放行卡死投递/任意卡死的 Deactivate（若常规路径赢）
		time.Sleep(20 * time.Millisecond)
		if got := evicts.Load(); got != 1 {
			t.Fatalf("round %d: OnEvict count after zombie = %d, want 1", round, got)
		}
	}
}

// TestAbandon_Activating_BlackholeRelief 激活卡死强弃（规格 §14.2 第三
// 形态——Go 移植补充）：Activate 永阻塞（loader IO 挂死形态）→
// 强弃单点置终态：在箱投递 Timeout 回绝解绑悬挂调用方 + OnEvict 恰一次 +
// done 关闭 + 准入关闭；zombie Activate 事后返回不得复活状态、不得二次
// OnEvict（条件转移不复位 + stop 分支让位）；二次强弃让位。
func TestAbandon_Activating_BlackholeRelief(t *testing.T) {
	id := mustUUID(t, "player", "aa1")
	actEntered := make(chan struct{})
	actRelease := make(chan struct{})
	var evicts atomic.Int32
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error {
			close(actEntered)
			<-actRelease // 永阻塞：卡死在 Activate 的形态
			return nil
		},
		OnEvict: func(p *Pawnite) { evicts.Add(1) },
	})
	p.Start(context.Background())
	<-actEntered
	if p.State() != StateActivating {
		t.Fatalf("state = %v, want StateActivating", p.State())
	}
	if p.StateSince().IsZero() {
		t.Fatal("precondition: state clock must be stamped while Activating")
	}
	queued := &rejectRecorder{inner: &testDelivery{id: id}}
	if !p.ReceiveRequest(queued) {
		t.Fatal("admission must succeed while Activating (messages queue until registration)")
	}

	if !p.Abandon(StateActivating, TerminalAbandoned) {
		t.Fatal("abandon must win on a stuck Activating pawnite")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state after abandon = %v, want StateDestroyed", p.State())
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done must be closed by abandon")
	}
	if n, st := queued.rejects(); n != 1 || st != rans.CodeTimeout {
		t.Fatalf("queued delivery rejects = (%d, %v), want (1, StatusTimeout)", n, st)
	}
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count = %d, want 1", got)
	}
	if p.ReceiveRequest(&testDelivery{id: id}) {
		t.Fatal("admission after abandon must fail")
	}
	if !p.StateSince().IsZero() {
		t.Fatal("state clock must be zero after abandon")
	}
	// stop 必须已关：zombie loop 出 Activate 后靠它让位退出（否则只剩
	// goroutine 泄漏——状态断言测不到的唯一面，同包直查钉死）。
	select {
	case <-p.stop:
	default:
		t.Fatal("stop must be closed by abandon (zombie loop exit hinge)")
	}
	if p.Abandon(StateActivating, TerminalAbandoned) {
		t.Fatal("second abandon must no-op on already Destroyed pawnite")
	}

	close(actRelease) // zombie Activate 返回：run 条件转移不复位、stop 分支让位
	time.Sleep(100 * time.Millisecond)
	if p.State() != StateDestroyed {
		t.Fatalf("state after zombie continuation = %v, want StateDestroyed", p.State())
	}
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count after zombie continuation = %d, want 1", got)
	}
	if n, _ := queued.rejects(); n != 1 {
		t.Fatalf("queued delivery rejected %d times, want exactly 1", n)
	}
}

// TestAbandon_Activating_RefusesNonActivating gate 前置校验：Created /
// Active 状态下强弃必须让位（返回 false，零副作用）。
func TestAbandon_Activating_RefusesNonActivating(t *testing.T) {
	id := mustUUID(t, "player", "aa2")
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	if p.Abandon(StateActivating, TerminalAbandoned) {
		t.Fatal("abandon must refuse before Start (Created)")
	}
	p.Start(context.Background())
	waitForState(t, p, StateActive)
	if p.Abandon(StateActivating, TerminalAbandoned) {
		t.Fatal("abandon must refuse an Active pawnite")
	}
	p.Destroy(context.Background())
	if p.Abandon(StateActivating, TerminalAbandoned) {
		t.Fatal("abandon must refuse a Destroyed pawnite")
	}
}

// TestAbandon_Activating_ZombieErrorSkipsFinish zombie 错误路径：强弃后卡死的
// Activate 以错误收场——失败分支不得再走 finish
// （OnEvict/done 已由强弃收口；宿主 Deactivate 不该在逐出之后再补跑）。
func TestAbandon_Activating_ZombieErrorSkipsFinish(t *testing.T) {
	id := mustUUID(t, "player", "aa3")
	actEntered := make(chan struct{})
	actRelease := make(chan struct{})
	var evicts, deacts atomic.Int32
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error {
			close(actEntered)
			<-actRelease // 先卡死，被强弃后放行并返回错误
			return errors.New("late activation failure")
		},
		Deactivate: func(ctx context.Context, p *Pawnite) error {
			deacts.Add(1)
			return nil
		},
		OnEvict: func(p *Pawnite) { evicts.Add(1) },
	})
	p.Start(context.Background())
	<-actEntered
	if !p.Abandon(StateActivating, TerminalAbandoned) {
		t.Fatal("abandon must win on stuck Activating")
	}
	close(actRelease)
	<-p.Done() // 强弃已关 done（zombie 收尾无外部可见信号——按既有惯例留窗口）

	// zombie 失败分支若未守卫将在此窗口内补跑 Deactivate（无守卫时微秒级
	// 发生，150ms 足以捕获）；守卫正确时永不为 1。
	time.Sleep(150 * time.Millisecond)
	if p.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed", p.State())
	}
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count = %d, want 1 (from abandon only)", got)
	}
	if got := deacts.Load(); got != 0 {
		t.Fatalf("Deactivate must not run after abandoned zombie error, got %d", got)
	}
}
