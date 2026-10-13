package pawnite

// 交接窗原语 Handoff 测试（《加固 spec》§5.1 第 6 点组合矩阵 + core 取消
// 面）：迁移窗泛化为 Migrate/Passivate/Retire 三 kind 共用的
// 「pending 先于 Destroying → 准入照收留箱 → core 即 loop 任务（静默点）→
// 终局处置按 kind 与 outcome」骨架。四组行为锁：
//   ① 窗内准入照收且 core 执行时无在途 Handle（静默点）；
//   ② 组合矩阵逐格（窗内再请求拒/等待/幂等）；
//   ③ 窗内看门狗强弃 → 在箱投递收 StatusUnavailable（对照 Timeout——
//      锚定 AbandonedInHandoff 终局原因：在箱消息从未执行过）；
//   ④ core 卡死 + 强弃 → core ctx 被取消（取消并集派生）。

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// blockCore 构造「进入即投票、阻塞至 release」的交接 core：结局由 won 控制
// （Migrate=Won；Passivate/Retire 恒 Err==nil 即成功）。静默点探针
// （无在途 Handle）在进入时刻采样（p 由用例在 New 之后回填）。
type blockCore struct {
	p       *Pawnite
	entered chan struct{}
	release chan struct{}
	won     bool
	silent  atomic.Bool // core 进入时刻 ExecutingSince 是否为零（无在途 Handle）
}

func (b *blockCore) core(ctx context.Context) HandoffOutcome {
	b.silent.Store(b.p.ExecutingSince().IsZero())
	close(b.entered)
	select {
	case <-ctx.Done():
		return HandoffOutcome{Err: ctx.Err()}
	case <-b.release:
	}
	if b.won {
		return HandoffOutcome{Won: true}
	}
	return HandoffOutcome{}
}

// TestHandoffWindowAdmissionHoldsAndCoreSilent 行为锁①：三种 kind 从 Active
// 开窗——窗内准入照收（留箱不回绝）；core 是 loop 任务，执行时刻必无在途
// Handle（串行即静默）。Won 迁移在箱消息转发目标；Passivate/Retire 成功
// 在箱消息按 Unavailable 回绝（确定未执行）；Handoff 返回 nil、终态 Destroyed。
func TestHandoffWindowAdmissionHoldsAndCoreSilent(t *testing.T) {
	cases := []struct {
		name        string
		kind        HandoffKind
		coreWon     bool // Migrate 用 Won 表达成功；Passivate/Retire 恒 Err==nil
		wantForward bool // 在箱消息的终局归宿：true=转发目标 / false=Unavailable 回绝
	}{
		{"migrate_won", HandoffMigrate, true, true},
		{"passivate_ok", HandoffPassivate, false, false},
		{"retire_ok", HandoffRetire, false, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			id := mustUUID(t, "player", "hd1")
			bc := &blockCore{entered: make(chan struct{}), release: make(chan struct{}), won: tc.coreWon}
			p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
			bc.p = p
			p.Start(context.Background())
			waitState(t, p, StateActive)

			res := make(chan error, 1)
			go func() {
				res <- p.Handoff(context.Background(), tc.kind, 7, bc.core)
			}()
			<-bc.entered // core 已在 loop 上：窗已开且稳定（core 阻塞中）

			// 白盒断言：窗口内状态组合（陷阱 3：pending 先于 Destroying）。
			p.mu.Lock()
			s, pend := p.state, p.handoffPending
			p.mu.Unlock()
			if s != StateDestroying || !pend {
				t.Fatalf("during handoff window: state=%v pending=%v, want Destroying+true", s, pend)
			}
			// 静默点：core 执行时刻必无在途 Handle（invoke 必先于 core 完成）。
			if !bc.silent.Load() {
				t.Fatal("handoff core must run with no in-flight Handle (serial loop)")
			}
			// 窗内准入照收：留箱等待，不回绝。
			d := newRec(id)
			if !p.ReceiveRequest(d) {
				t.Fatal("admission during handoff window must be held-in-box, not rejected")
			}

			close(bc.release)
			select {
			case err := <-res:
				if err != nil {
					t.Fatalf("Handoff(%v) error = %v, want nil", tc.kind, err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Handoff must return after core completes")
			}
			// 在箱消息的终局处置按 kind：Migrate 赢转发目标；Passivate/Retire
			// 按 Unavailable 回绝（确定未执行，调用方重试落新激活）。
			if tc.wantForward {
				select {
				case w := <-d.forwarded:
					if w != 7 {
						t.Fatalf("forward target = %d, want 7", w)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("migrate-won window must forward held-in-box delivery to target")
				}
			} else {
				select {
				case r := <-d.rejected:
					if r.st != rans.CodeUnavailable {
						t.Fatalf("held-in-box reject status = %v, want StatusUnavailable", r.st)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("passivate/retire success must reject held-in-box delivery as Unavailable")
				}
			}
			if p.State() != StateDestroyed {
				t.Fatalf("post-handoff state = %v, want StateDestroyed", p.State())
			}
			select {
			case <-p.Done():
			default:
				t.Fatal("Done must be closed after successful handoff")
			}
		})
	}
}

// TestHandoffCombinationMatrix 行为锁②：组合矩阵逐格（《加固 spec》§5.1 第 6 点，
// 在飞窗全取成功结局；强弃列归行为锁③④）：
//
//	在飞 Migrate/Passivate × 新 Migrate/Passivate → ErrNotActive 拒；
//	在飞 Migrate/Passivate × 新 Retire → 等待在飞结果：成功 → 视为已完成
//	（Retire core 不再执行）；在飞 Retire × 新 Migrate/Passivate → 拒；
//	在飞 Retire × 新 Retire → 幂等等待同一结果（恰一个 core）。
func TestHandoffCombinationMatrix(t *testing.T) {
	cases := []struct {
		name     string
		inflight HandoffKind
		newKind  HandoffKind
		behavior string // "reject" | "follow" | "idempotent"
	}{
		{"migrate_x_migrate", HandoffMigrate, HandoffMigrate, "reject"},
		{"migrate_x_passivate", HandoffMigrate, HandoffPassivate, "reject"},
		{"migrate_x_retire", HandoffMigrate, HandoffRetire, "follow"},
		{"passivate_x_migrate", HandoffPassivate, HandoffMigrate, "reject"},
		{"passivate_x_passivate", HandoffPassivate, HandoffPassivate, "reject"},
		{"passivate_x_retire", HandoffPassivate, HandoffRetire, "follow"},
		{"retire_x_migrate", HandoffRetire, HandoffMigrate, "reject"},
		{"retire_x_passivate", HandoffRetire, HandoffPassivate, "reject"},
		{"retire_x_retire", HandoffRetire, HandoffRetire, "idempotent"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			id := mustUUID(t, "player", "hd2")
			inflight := &blockCore{entered: make(chan struct{}), release: make(chan struct{}), won: true}
			p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
			inflight.p = p
			p.Start(context.Background())
			waitState(t, p, StateActive)

			inflightRes := make(chan error, 1)
			go func() {
				inflightRes <- p.Handoff(context.Background(), tc.inflight, 7, inflight.core)
			}()
			<-inflight.entered // 在飞窗已开（core 阻塞中）

			newCoreRan := make(chan struct{}, 1)
			newCore := func(ctx context.Context) HandoffOutcome {
				newCoreRan <- struct{}{}
				return HandoffOutcome{}
			}
			newRes := make(chan error, 1)
			go func() {
				newRes <- p.Handoff(context.Background(), tc.newKind, 8, newCore)
			}()

			newResConsumed := false
			switch tc.behavior {
			case "reject":
				// 窗内再请求同步被拒：ErrNotActive（非 Active 前置校验）。
				newResConsumed = true
				select {
				case err := <-newRes:
					if !errors.Is(err, ErrNotActive) {
						t.Fatalf("in-window %v request error = %v, want ErrNotActive", tc.newKind, err)
					}
				case <-time.After(2 * time.Second):
					t.Fatalf("in-window %v request must be rejected synchronously", tc.newKind)
				}
			case "follow":
				// 等待在飞结果：在飞成功 → Retire 视为已完成（新 core 不执行）。
				select {
				case <-newCoreRan:
					t.Fatal("retire follower must not run its own core when the in-flight handoff succeeds")
				default:
				}
			case "idempotent":
				// 幂等：第二个 Retire 等待同一结果，不排自己的 core。
				select {
				case <-newCoreRan:
					t.Fatal("idempotent retire must not run a second core")
				default:
				}
			}

			close(inflight.release) // 在飞窗走向成功终局
			select {
			case err := <-inflightRes:
				if err != nil {
					t.Fatalf("in-flight handoff result = %v, want nil", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("in-flight handoff caller never settled")
			}
			if !newResConsumed {
				select {
				case err := <-newRes:
					if err != nil {
						t.Fatalf("new handoff result = %v, want nil", err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("new handoff caller never settled")
				}
			}
			select {
			case <-newCoreRan:
				t.Fatal("retire follower/idempotent core must never run after in-flight success")
			default:
			}
			if p.State() != StateDestroyed {
				t.Fatalf("final state = %v, want StateDestroyed", p.State())
			}
		})
	}
}

// TestHandoffRetireFollowsFailure 矩阵等待格的失败分支：在飞迁移输 →
// 回退 Active → 等待中的 Retire 自行开窗执行（其 core 恰跑一次）→ 成功终局。
func TestHandoffRetireFollowsFailure(t *testing.T) {
	id := mustUUID(t, "player", "hd3")
	inflight := &blockCore{entered: make(chan struct{}), release: make(chan struct{})} // won=false → 迁移输
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	inflight.p = p
	p.Start(context.Background())
	waitState(t, p, StateActive)

	inflightRes := make(chan error, 1)
	go func() {
		inflightRes <- p.Handoff(context.Background(), HandoffMigrate, 7, inflight.core)
	}()
	<-inflight.entered

	retireEntered := make(chan struct{})
	releaseRetire := make(chan struct{})
	retireCore := func(ctx context.Context) HandoffOutcome {
		close(retireEntered)
		<-releaseRetire
		return HandoffOutcome{}
	}
	retireRes := make(chan error, 1)
	go func() {
		retireRes <- p.Handoff(context.Background(), HandoffRetire, 0, retireCore)
	}()

	close(inflight.release) // 迁移输：窗回退 Active，Retire 等待者接棒
	if err := <-inflightRes; !errors.Is(err, ErrMigrationLost) {
		t.Fatalf("in-flight migrate error = %v, want ErrMigrationLost", err)
	}
	select {
	case <-retireEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("retire follower must re-open its own window after the in-flight handoff fails")
	}
	// retireEntered 即接棒证明：跟随者收到回退结算后已重开自己的窗
	//（回退后的瞬态 Active 不做轮询断言——接棒开窗是无间隔的紧邻事件）。
	close(releaseRetire)
	select {
	case err := <-retireRes:
		if err != nil {
			t.Fatalf("retire error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retire follower never settled")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("final state = %v, want StateDestroyed", p.State())
	}
}

// TestAbandonInHandoffWindowUnavailable 行为锁③：交接窗内看门狗强弃 → 在箱
// 投递（从未执行过）收 StatusUnavailable——对照迁移窗行为的 Timeout。
// 锚定 TerminalAbandonedInHandoff：交接窗内强弃与普通销毁窗强弃（在途可能
// 已部分执行 → Timeout）分流两个终局原因。
func TestAbandonInHandoffWindowUnavailable(t *testing.T) {
	id := mustUUID(t, "player", "hd4")
	bc := &blockCore{entered: make(chan struct{}), release: make(chan struct{})}
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	bc.p = p
	p.Start(context.Background())
	waitState(t, p, StateActive)

	res := make(chan error, 1)
	go func() {
		res <- p.Handoff(context.Background(), HandoffMigrate, 7, bc.core)
	}()
	<-bc.entered

	d := newRec(id)
	if !p.ReceiveRequest(d) {
		t.Fatal("in-window admission must be held-in-box")
	}
	if !p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("watchdog abandon must win on the stuck handoff window")
	}
	select {
	case r := <-d.rejected:
		if r.st != rans.CodeUnavailable {
			t.Fatalf("held-in-box delivery after in-window abandon: status = %v (%q), want StatusUnavailable (never executed)", r.st, r.reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("held-in-box delivery must be rejected on in-window abandon")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state after abandon = %v, want StateDestroyed", p.State())
	}
	if p.SkipDirUnregister() {
		t.Fatal("abandoned handoff must not skip directory unregister (no new home)")
	}
	// 调用方解绑：输哨兵（zombie core 迟回前先回输——abort 回输路径）。
	select {
	case err := <-res:
		if !errors.Is(err, ErrMigrationLost) {
			t.Fatalf("Handoff error after abandon = %v, want ErrMigrationLost", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Handoff caller must be unbound by the abandon")
	}
}

// TestAbandonCancelsStuckHandoffCore 行为锁④：core 卡死
// + 看门狗强弃 → core 的 ctx 被取消（hookCtx 派生 + 调用方预算 ctx 的取消
// 并集）。协作 core 据此尽早返回（若 core ctx 只是调用方预算 ctx，强弃
// 够不到，卡死 core 无法收敛）。
func TestAbandonCancelsStuckHandoffCore(t *testing.T) {
	id := mustUUID(t, "player", "hd5")
	entered := make(chan struct{})
	release := make(chan struct{})
	canceled := make(chan struct{})
	core := func(ctx context.Context) HandoffOutcome {
		close(entered)
		select {
		case <-ctx.Done():
			close(canceled)
			return HandoffOutcome{Err: ctx.Err()}
		case <-release:
			return HandoffOutcome{}
		}
	}
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	res := make(chan error, 1)
	go func() {
		res <- p.Handoff(context.Background(), HandoffMigrate, 7, core)
	}()
	<-entered

	if !p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("watchdog abandon must win on the stuck handoff window")
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("stuck handoff core must observe ctx cancellation after abandon")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state after abandon = %v, want StateDestroyed", p.State())
	}
	close(release) // zombie core 收尾：无二次终局效应
	select {
	case err := <-res:
		if err != nil && !errors.Is(err, ErrMigrationLost) {
			t.Fatalf("Handoff error = %v, want ErrMigrationLost or nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Handoff caller never settled")
	}
	time.Sleep(50 * time.Millisecond)
	if p.State() != StateDestroyed {
		t.Fatalf("state after zombie core = %v, want StateDestroyed", p.State())
	}
}

// TestCaptureOnLoopRejectedInHandoffWindow 核实性锚定：交接窗内不接受新的
// 周期捕获（CaptureOnLoop 要求 Active——窗内非 Active 天然拒），终快照之后
// 不会再有更旧的同纪元快照落库（规格 §5.1 H6 第 2 步的前半）。
func TestCaptureOnLoopRejectedInHandoffWindow(t *testing.T) {
	id := mustUUID(t, "player", "hd6")
	bc := &blockCore{entered: make(chan struct{}), release: make(chan struct{}), won: true}
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	bc.p = p
	p.Start(context.Background())
	waitState(t, p, StateActive)

	res := make(chan error, 1)
	go func() {
		res <- p.Handoff(context.Background(), HandoffMigrate, 7, bc.core)
	}()
	<-bc.entered

	done := make(chan CaptureOutcome, 1)
	if p.CaptureOnLoop(func() ([]byte, error) { return []byte("late"), nil }, done) {
		t.Fatal("CaptureOnLoop must be rejected while a handoff window is open (non-Active)")
	}
	close(bc.release)
	<-res
}

// TestHandoffCorePanicLoses 行为锁：交接 core panic
// 必须折输而非零值成功——Passivate/Retire 的成败判据是 Err==nil，recover 折
// 零值恰好落进成功编码（中途 panic → 终局 Destroyed、在箱按确定未执行回绝、
// 返回 nil）。契约：三 kind 统一折输（Err 携带包内哨兵 errHandoffCorePanic）、
// 窗回退 Active 继续服役、不写终局（对照面见 TestMigratePanicStillLoses）。
func TestHandoffCorePanicLoses(t *testing.T) {
	cases := []struct {
		name string
		kind HandoffKind
	}{
		{"migrate_panic_loses", HandoffMigrate},
		{"passivate_panic_loses", HandoffPassivate},
		{"retire_panic_loses", HandoffRetire},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			id := mustUUID(t, "player", "hd7")
			p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
			p.Start(context.Background())
			waitState(t, p, StateActive)

			err := p.Handoff(context.Background(), tc.kind, 7, func(ctx context.Context) HandoffOutcome {
				panic("boom-" + tc.name)
			})
			if !errors.Is(err, errHandoffCorePanic) {
				t.Fatalf("Handoff(%v) with panicking core = %v, want errHandoffCorePanic (zero outcome must not encode success)", tc.kind, err)
			}
			// 窗回退 Active：单元继续服役，不写终局。
			if p.State() != StateActive {
				t.Fatalf("post-panic state = %v, want StateActive (window rolled back)", p.State())
			}
			select {
			case <-p.Done():
				t.Fatal("Done must NOT close on core panic (no terminal)")
			default:
			}
			if p.SkipDirUnregister() {
				t.Fatal("no terminal reason must be written on core panic")
			}
			d := newRec(id)
			if !p.ReceiveRequest(d) {
				t.Fatal("post-rollback admission must succeed")
			}
			select {
			case <-d.delivered:
			case <-time.After(2 * time.Second):
				t.Fatal("post-rollback delivery must execute normally (unit still serving)")
			}
			p.Destroy(context.Background())
		})
	}
}

// orderDelivery 把事件名写进共享次序账本的投递（终局次序断言面）。
type orderDelivery struct {
	id   rans.ID
	note func(what string)
}

func (d *orderDelivery) UUID() rans.ID                     { return d.id }
func (d *orderDelivery) Deliver(_ *Pawnite)                {}
func (d *orderDelivery) Reject(_ rans.Code, _ rans.Reason) { d.note("reject") }

// TestHandoffRetiredDisposalOrder 终局处置次序锁（《加固 spec》§5.1 第 4 点）：
// Passivate/Retire 成功的收尾依次为——用户 Deactivate → 实例级条件
// 注销（OnEvict 单胜钩子是 manager cleanup（releaseLease→UnregisterIfOwner
// →本地逐出）的承载点）→ 在箱 StatusUnavailable 回绝 → Destroyed。快照在
// core 内已拍（先于 Deactivate，manager core 测试锚定）。
//
// 次序不变量：注销在 Deactivate 之后（新实例 Activate 不与本实例 Deactivate
// 重叠）；注销在在箱回绝之前（调用方重试时目录已空，新激活从 Store 恢复
// 终快照）。对照旧序（在箱回绝先于 finish 的注销段）：回绝早于注销时调用方
// 立刻重试会命中未让位的目录条目，落空一轮。
func TestHandoffRetiredDisposalOrder(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind HandoffKind
	}{
		{"passivate", HandoffPassivate},
		{"retire", HandoffRetire},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			id := mustUUID(t, "player", "disp-"+tc.name)
			var (
				mu    sync.Mutex
				order []string
			)
			note := func(what string) {
				mu.Lock()
				order = append(order, what)
				mu.Unlock()
			}
			p := New(id, Options{
				Activate:   func(context.Context, *Pawnite) error { return nil },
				Deactivate: func(context.Context, *Pawnite) error { note("deactivate"); return nil },
				OnEvict:    func(*Pawnite) { note("unregister") },
			})
			p.Start(context.Background())
			waitState(t, p, StateActive)

			bc := &blockCore{entered: make(chan struct{}), release: make(chan struct{})}
			bc.p = p
			res := make(chan error, 1)
			go func() {
				res <- p.Handoff(context.Background(), tc.kind, 0, bc.core)
			}()
			<-bc.entered // 窗已开且 core 阻塞中

			d := &orderDelivery{id: id, note: note}
			if !p.ReceiveRequest(d) {
				t.Fatal("admission during handoff window must be held-in-box")
			}
			close(bc.release) // core 成功 → 走终局次序
			select {
			case err := <-res:
				if err != nil {
					t.Fatalf("Handoff(%v) error = %v, want nil", tc.kind, err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Handoff never settled")
			}
			mu.Lock()
			got := append([]string(nil), order...)
			mu.Unlock()
			want := []string{"deactivate", "unregister", "reject"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("disposal order = %v, want %v (Deactivate -> unregister -> in-box reject)", got, want)
			}
			if p.State() != StateDestroyed {
				t.Fatalf("post-disposal state = %v, want StateDestroyed", p.State())
			}
		})
	}
}

// TestMigratePanicStillLoses Migrate 面 panic 锚定（TestHandoffCorePanicLoses
// 的对照面）：迁移 core panic 同样折输（recover 在 pawnite 侧，
// wrapper 拦不住——Migrate 调用方收更精确的 panic 哨兵；
// manager 错误链默认分支仍包 ErrMigrationLost，消费方判据不变）、窗回退
// Active、不写终局。
func TestMigratePanicStillLoses(t *testing.T) {
	id := mustUUID(t, "player", "hd8")
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	waitState(t, p, StateActive)

	err := p.Migrate(context.Background(), 7, func(ctx context.Context) MigrationOutcome {
		panic("boom-migrate-wrapper")
	})
	if !errors.Is(err, errHandoffCorePanic) {
		t.Fatalf("Migrate panic error = %v, want errHandoffCorePanic (recover folds panic to loss on all kinds)", err)
	}
	if errors.Is(err, context.Canceled) || err == nil {
		t.Fatalf("Migrate panic must not encode success/cancellation: %v", err)
	}
	if p.State() != StateActive {
		t.Fatalf("post-panic state = %v, want StateActive", p.State())
	}
	select {
	case <-p.Done():
		t.Fatal("Done must NOT close")
	default:
	}
	p.Destroy(context.Background())
}

// TestHandoffLossErrorDeterministicRejectionNotShadowedByBudget：core 自报的
// 确定性拒绝（错误链不含取消签名——不可能是取消并集坍缩的产物）优先于
// 预算错误。预算与确定性拒绝同时成立时回精确原因，不被
// DeadlineExceeded/Canceled 遮蔽；含取消签名的 core 错误仍是并集坍缩嫌疑，
// 预算优先语义保留——预算优先的唯一依据是「并集派生遮蔽父级 Deadline」，
// 对无取消签名的
// 裁决不成立。manager 侧镜像规则见 lifecycle.Migrate 默认分支（cause 通道
// 路径——pawnite.Migrate 包装丢弃 MigrationOutcome 的 Err，Error 经
// HandoffOutcome.Err 只达泛化 Handoff 调用方）。
func TestHandoffLossErrorDeterministicRejectionNotShadowedByBudget(t *testing.T) {
	budgetCtx, cancel := context.WithCancel(context.Background())
	cancel() // 预算已耗尽（显式取消——确定性前提，非时序竞态）
	defer cancel()
	sentinel := errors.New("pawnite: deterministic rejection (test)")
	req := handoffReq{ctx: budgetCtx}

	// 确定性拒绝 + 预算耗尽：精确原因胜出（主断言）。
	if got := handoffLossError(req, HandoffOutcome{Err: sentinel}); !errors.Is(got, sentinel) {
		t.Fatalf("handoffLossError = %v, want the deterministic rejection (exhausted budget must not shadow it)", got)
	}
	// 对照 1：取消签名（并集坍缩嫌疑）+ 预算耗尽 → 预算错误权威。
	if got := handoffLossError(req, HandoffOutcome{Err: fmt.Errorf("store put: %w", context.DeadlineExceeded)}); !errors.Is(got, context.Canceled) {
		t.Fatalf("handoffLossError = %v, want budget error (context.Canceled) for ctx-signed core error", got)
	}
	// 对照 2（现状保留）：预算活跃 + core 自报 Err → core Err。
	alive := handoffReq{ctx: context.Background()}
	coreErr := errors.New("pawnite: transfer CAS failed")
	if got := handoffLossError(alive, HandoffOutcome{Err: coreErr}); !errors.Is(got, coreErr) {
		t.Fatalf("handoffLossError = %v, want core error while budget alive", got)
	}
}
