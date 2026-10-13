package pawnite

// 强弃后「晚到 Deactivate」形态区分钉死：强弃置终态后 zombie 续体对 Deactivate 钩子的关系按形态
// 分两条——
//   - 执行卡死强杀（Abandon(StateActive)）：zombie 经 stopShutdown 让位
//     检查直接退出，Deactivate **不启动**（比「收到已取消 ctx」更强）；
//   - 迁移卡死强弃 + core 迟回 Won：Won 分支无条件 finish(hookCtx)，Deactivate
//     在强弃**之后启动**且收到**已取消**的 ctx——消费端 Deactivate 三面合流
//     select（泵退出/宽限/ctx 取消）所依赖语义的唯一真实落点。同测试另锚
//     core 取消面：core ctx 是 hookCtx 派生 + 调用方预算 ctx 的取消并集，
//     强弃经取消面直达 core（观察到 context.Canceled——若 core ctx 只随
//     调用方预算取消，强弃够不到卡死 core）。
//   - 终局原因先落（ArbitrationLost）+ 迟到处置锁外段挂死 + 强弃胜出：续体
//     凭 abandoned 让位标志退出（与 write-once 的 terminal 分属两个维度），
//     Deactivate 不得在强弃之后新发起。

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// TestAbandonExecutionSkipsLateDeactivate 执行卡死强杀后无晚到 Deactivate：
// gate 已单点完成终局（OnEvict/done），zombie invoke 迟回后经 stopShutdown
// 的让位检查（stuckAbandoned+Destroyed）直接退出——Deactivate 钩子不得启动。
// 消费方语境：卡死的是 Handle（如 LLM 调用）时，被强弃的 incarnation 不会
// 再触发实体清理钩子；新 incarnation 的清理面从 checkpoint/Store 重建。
func TestAbandonExecutionSkipsLateDeactivate(t *testing.T) {
	id := mustUUID(t, "player", "al1")
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var deactivations atomic.Int32
	p := New(id, Options{
		Activate:   func(ctx context.Context, p *Pawnite) error { return nil },
		Deactivate: func(ctx context.Context, p *Pawnite) error { deactivations.Add(1); return nil },
	})
	p.Start(context.Background())
	waitForState(t, p, StateActive)

	exec := &enterBlockDelivery{id: id, entered: entered, release: release}
	if !p.ReceiveRequest(exec) {
		t.Fatal("admission must succeed")
	}
	<-entered
	if !p.Abandon(StateActive, TerminalAbandoned) {
		t.Fatal("force abandon must win")
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done must be closed by abandon")
	}

	close(release) // zombie invoke 迟回：续体走 stop 分支
	time.Sleep(100 * time.Millisecond)
	if got := deactivations.Load(); got != 0 {
		t.Fatalf("Deactivate after exec abandon = %d, want 0 (stopShutdown yields before finish)", got)
	}
}

// TestAbandonedMigrationWonLateDeactivateSeesCanceledCtx 迁移卡死强弃 +
// core 迟回 Won：Deactivate 在强弃之后启动，收到的 hookCtx 必须已是取消态
// （消费端 select 对已取消 ctx 立即放行的依赖锚点）。同窗内断言 core 自身
// 的 ctx 观察到取消——core ctx 是 hookCtx 派生 + 调用方预算 ctx 的取消
// 并集（强弃取消面直达迁移 core）。
func TestAbandonedMigrationWonLateDeactivateSeesCanceledCtx(t *testing.T) {
	id := mustUUID(t, "player", "al2")
	coreEntered := make(chan struct{})
	coreRelease := make(chan struct{})
	coreCtxErrAfterAbandon := make(chan error, 1) // core 迟回时刻其 ctx 的状态（取消并集下必为 Canceled）
	deactEntered := make(chan struct{}, 1)
	deactCtxErr := make(chan error, 1)
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		Deactivate: func(ctx context.Context, p *Pawnite) error {
			deactEntered <- struct{}{}
			deactCtxErr <- ctx.Err()
			return nil
		},
	})
	p.Start(context.Background())
	waitForState(t, p, StateActive)

	migDone := make(chan error, 1)
	go func() {
		migDone <- p.Migrate(context.Background(), 2, func(ctx context.Context) MigrationOutcome {
			close(coreEntered)
			<-coreRelease
			coreCtxErrAfterAbandon <- ctx.Err()
			return MigrationOutcome{Won: true}
		})
	}()
	<-coreEntered // core 在飞（迁移窗：Destroying + pending）

	if !p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("Abandon must win on a stuck migration window")
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done must be closed by abandon")
	}

	close(coreRelease) // zombie core 迟回 Won：续体走 finish → safeDeactivate
	select {
	case <-deactEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("migration-Won continuation must reach Deactivate (finish runs unconditionally)")
	}
	select {
	case err := <-deactCtxErr:
		if err != context.Canceled {
			t.Fatalf("late Deactivate ctx err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late Deactivate must report its ctx state")
	}
	if err := <-coreCtxErrAfterAbandon; !errors.Is(err, context.Canceled) {
		t.Fatalf("migration core ctx err after abandon = %v, want context.Canceled (cancel union reaches core)", err)
	}
	select {
	case err := <-migDone:
		if err != nil {
			t.Fatalf("Migrate result = %v, want nil (Won)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Migrate must return after zombie core completes")
	}
}

// blockFwdDelivery 进入 Forward 即投票、随后阻塞至 release 的 Forwardable
// 投递：构造「终局原因已落、finish 未调、loop 停在迟到处置的锁外回调段」
// 窗口（整箱排空的 Forward/Reject 是回调，可长时间不返回——看门狗适用
// 场景；收件箱上限 10_000 条，窗口非纳秒级）。
type blockFwdDelivery struct {
	id      rans.ID
	entered chan rans.NodeID
	release chan struct{}
}

func (b *blockFwdDelivery) UUID() rans.ID                 { return b.id }
func (b *blockFwdDelivery) Deliver(*Pawnite)              {}
func (b *blockFwdDelivery) Reject(rans.Code, rans.Reason) {}
func (b *blockFwdDelivery) Forward(w rans.NodeID) {
	b.entered <- w
	<-b.release
}

var _ Forwardable = (*blockFwdDelivery)(nil)

// TestAbandonDuringLateDisposalSuppressesFinish 让位判据的两个维度钉死
// （terminal 写一次 ≠ 强弃让位）：激活仲裁落败的整箱排空（drainForwardTo
// 锁外段）挂死在一条 Forward 回调上——终局原因 ArbitrationLost 已落、
// finish 未调、状态仍 Activating，激活看门狗强弃胜出。zombie 续体出
// Forward 后必须让位（不再 finish）：宿主 Deactivate 不得在强弃之后新发起
// ——OnEvict 已由强弃单点收口，补跑 Deactivate 会让宿主可见的
// Deactivate→OnEvict 次序倒置。同时钉死：迟到处置矩阵按先到的终局原因
// 分流，不受强弃让位影响（准入面迟到者仍转发赢家）。
func TestAbandonDuringLateDisposalSuppressesFinish(t *testing.T) {
	id := mustUUID(t, "player", "al3")
	actRelease := make(chan struct{})
	fwdEntered := make(chan rans.NodeID, 1)
	fwdRelease := make(chan struct{})
	var deacts, evicts atomic.Int32
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error {
			<-actRelease // 先占住 Activate，测试侧把探针入箱后再放行落败
			return &lostErr{winner: 5}
		},
		Deactivate: func(ctx context.Context, p *Pawnite) error {
			deacts.Add(1)
			return nil
		},
		OnEvict: func(p *Pawnite) { evicts.Add(1) },
	})
	p.Start(context.Background())
	blk := &blockFwdDelivery{id: id, entered: fwdEntered, release: fwdRelease}
	if !p.ReceiveRequest(blk) {
		t.Fatal("admission while Activating must succeed")
	}
	close(actRelease) // Activate 落败返回：失败分支置 ArbitrationLost 后整箱排空，Forward 挂死
	if w := <-fwdEntered; w != 5 {
		t.Fatalf("forward target = %d, want winner 5", w)
	}
	// 窗口达成：term=ArbitrationLost（write-once 先占）、finish 未调、
	// state 仍 Activating——激活看门狗的适用面。
	if !p.Abandon(StateActivating, TerminalAbandoned) {
		t.Fatal("abandon must win on the stuck late-disposal window (state Activating)")
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done must be closed by abandon")
	}
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count = %d, want 1 (from abandon)", got)
	}

	close(fwdRelease) // zombie 续体出 Forward：必须让位，不得再走 finish
	time.Sleep(150 * time.Millisecond)
	if got := deacts.Load(); got != 0 {
		t.Fatalf("Deactivate after abandon = %d, want 0 (zombie must yield before finish)", got)
	}
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count after zombie continuation = %d, want 1", got)
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state after zombie continuation = %v, want StateDestroyed", p.State())
	}

	// 迟到处置矩阵不受让位影响：准入面迟到者按先到的 ArbitrationLost
	// 转发赢家（abandoned 只管续体让位，不参与处置分流）。
	late := newRec(id)
	if p.ReceiveRequest(late) {
		t.Fatal("straggler after terminal must not be admitted")
	}
	select {
	case w := <-late.forwarded:
		if w != 5 {
			t.Fatalf("late straggler forwarded to %d, want winner 5", w)
		}
	case r := <-late.rejected:
		t.Fatalf("late straggler must forward to the winner, got reject %v %q", r.st, r.reason)
	case <-time.After(2 * time.Second):
		t.Fatal("late straggler must be forwarded to the arbitration winner")
	}
}
