package pawnite

// 周期 checkpoint 的 pawnite 机制面（规格 §15：快照捕获是 loop 任务——
// 串行即静默，与 Handle 无并发；活动纪元为脏门控依据）。

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// newQuietPawnite 无钩子实体：Activate 即 Active。
func newQuietPawnite(t *testing.T, inst string) *Pawnite {
	t.Helper()
	p := New(mustUUID(t, "player", inst), Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
	})
	p.Start(context.Background())
	waitState(t, p, StateActive)
	t.Cleanup(func() {
		// 有界预算：测试自身断言失败时不得在清理段挂死整个测试进程
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = p.Destroy(ctx)
	})
	return p
}

// TestActivityEpochAdvances：进入 Active 记一次活动，此后每次投递递增——
// 脏纪元门控（活动纪元 > 已 checkpoint 纪元）的依据；闲置实体纪元不变。
func TestActivityEpochAdvances(t *testing.T) {
	p := newQuietPawnite(t, "epoch")
	base := p.Epoch()
	if base == 0 {
		t.Fatal("entering Active must advance the epoch from zero")
	}
	d := newRec(p.UUID())
	if !p.ReceiveRequest(d) {
		t.Fatal("delivery must be admitted")
	}
	<-d.delivered // 投递完成信号（Deliver 内 close）：epoch 加点先于 Deliver，必可见
	if got := p.Epoch(); got != base+1 {
		t.Fatalf("epoch after one delivery = %d, want %d", got, base+1)
	}
}

// TestCaptureOnLoopSerializesWithInvoke：捕获与投递串行（loop 任务即
// 静默点——捕获机制的根基）——invoke 阻塞期间捕获不得运行，invoke
// 完成后捕获在无并发语境执行并带回捕获时刻纪元。
func TestCaptureOnLoopSerializesWithInvoke(t *testing.T) {
	p := newQuietPawnite(t, "serial")
	block := make(chan struct{})
	entered := make(chan struct{})
	// 直接占用 loop：经 inbox 投递一个阻塞投递；finished 在 Deliver 返回时
	// 关闭——捕获必须排在其后（串行即静默）。
	blk := newBlockingRec(p.UUID(), entered, block)
	if !p.ReceiveRequest(blk) {
		t.Fatal("blocking delivery must be admitted")
	}
	<-entered

	done := make(chan CaptureOutcome, 1)
	if !p.CaptureOnLoop(func() ([]byte, error) {
		// 捕获运行时 loop 已空闲：阻塞投递必然已完成
		select {
		case <-blk.finished:
			return []byte("snap"), nil
		default:
			return nil, errors.New("capture ran while a delivery was in flight")
		}
	}, done) {
		t.Fatal("capture must be admitted on Active pawnite")
	}
	select {
	case <-done:
		t.Fatal("capture must not run while the loop is busy with a delivery")
	case <-time.After(150 * time.Millisecond):
	}
	close(block)
	<-blk.finished // 阻塞投递收尾（Deliver 返回）
	out := recvOutcome(t, done)
	if out.Err != nil || string(out.Snapshot) != "snap" {
		t.Fatalf("capture outcome = %+v, want snapshot", out)
	}
	if out.Epoch != p.Epoch() {
		t.Fatalf("outcome epoch = %d, want current %d", out.Epoch, p.Epoch())
	}
}

// TestCaptureOnLoopGateAndSingleFlight：非 Active 拒绝；待处理捕获排队
// 不堆积（通道容量 1：loop 被占住时第二条入队必须被拒；捕获之间由 loop
// 单线程天然串行，IO 层不重叠门在 manager 侧）。
func TestCaptureOnLoopGateAndSingleFlight(t *testing.T) {
	p := newQuietPawnite(t, "gate")
	if p.CaptureOnLoop(func() ([]byte, error) { return nil, nil }, nil) {
		t.Fatal("nil done channel must be rejected")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	blk := newBlockingRec(p.UUID(), entered, release)
	if !p.ReceiveRequest(blk) {
		t.Fatal("blocking delivery must be admitted")
	}
	<-entered
	done := make(chan CaptureOutcome, 1)
	if !p.CaptureOnLoop(func() ([]byte, error) { return []byte("x"), nil }, done) {
		t.Fatal("pending capture must be admitted while the loop is busy")
	}
	if p.CaptureOnLoop(func() ([]byte, error) { return nil, nil }, make(chan CaptureOutcome, 1)) {
		t.Fatal("second capture must be rejected while one is pending")
	}
	close(release)
	<-blk.finished
	out := recvOutcome(t, done)
	if out.Err != nil || string(out.Snapshot) != "x" {
		t.Fatalf("capture outcome = %+v, want snapshot", out)
	}

	// 非 Active 拒绝（销毁终态是稳定态——Destroying 是瞬态，轮询易错过）
	go func() { _ = p.Destroy(context.Background()) }()
	waitState(t, p, StateDestroyed)
	if p.CaptureOnLoop(func() ([]byte, error) { return nil, nil }, make(chan CaptureOutcome, 1)) {
		t.Fatal("capture must be rejected on non-Active pawnite")
	}
}

// TestCaptureOnLoopDrainedOnStrongAbandon：入队后未拾取的捕获在终局
// （强弃）被排空为 Skipped——done 必达，等待方不悬挂。
func TestCaptureOnLoopDrainedOnStrongAbandon(t *testing.T) {
	p := newQuietPawnite(t, "drain")
	entered := make(chan struct{})
	release := make(chan struct{})
	blk := newBlockingRec(p.UUID(), entered, release)
	if !p.ReceiveRequest(blk) {
		t.Fatal("blocking delivery must be admitted")
	}
	<-entered
	done := make(chan CaptureOutcome, 1)
	if !p.CaptureOnLoop(func() ([]byte, error) { return []byte("late"), nil }, done) {
		t.Fatal("capture must be admitted while loop is busy (buffered)")
	}
	// loop 卡在投递里：执行看门狗强杀（Active + 在途非零）
	if !p.Abandon(StateActive, TerminalAbandoned) {
		t.Fatal("strong abandon must win")
	}
	out := recvOutcome(t, done)
	if !out.Skipped {
		t.Fatalf("pending capture must be drained as Skipped on terminal, got %+v", out)
	}
	close(release)
}

// ---- 测试夹具 ----

type blockingRec struct {
	id       rans.ID
	entered  chan struct{}
	release  chan struct{}
	finished chan struct{} // Deliver 返回时关闭（投递完成信号）
}

func newBlockingRec(id rans.ID, entered, release chan struct{}) *blockingRec {
	return &blockingRec{id: id, entered: entered, release: release, finished: make(chan struct{})}
}

func (b *blockingRec) UUID() rans.ID { return b.id }
func (b *blockingRec) Deliver(p *Pawnite) {
	close(b.entered)
	<-b.release
	close(b.finished)
}
func (b *blockingRec) Reject(status rans.Code, reason rans.Reason) {}

func recvOutcome(t *testing.T, done chan CaptureOutcome) CaptureOutcome {
	t.Helper()
	select {
	case out := <-done:
		return out
	case <-time.After(2 * time.Second):
		t.Fatal("capture outcome not delivered within 2s")
		return CaptureOutcome{}
	}
}
