package pawnite

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// lostErr 是 ArbitrationLost 的测试替身（正片由 manager 层错误实现）。
type lostErr struct{ winner rans.NodeID }

func (e *lostErr) Error() string           { return "activation lost arbitration" }
func (e *lostErr) WinnerNode() rans.NodeID { return e.winner }

var _ ArbitrationLost = (*lostErr)(nil)
var _ Forwardable = (*recDelivery)(nil)
var _ InboundDelivery = (*plainDelivery)(nil)

// fwdRecord 记一次转发（投递序号 + 赢家），供 FIFO 顺序断言。
type fwdRecord struct {
	seq    int
	winner rans.NodeID
}

// fwdLog 收集转发调用序（Forward 在 run goroutine 上顺序执行，记录即调用序）。
type fwdLog struct {
	mu sync.Mutex
	s  []fwdRecord
}

func (f *fwdLog) record(seq int, winner rans.NodeID) {
	f.mu.Lock()
	f.s = append(f.s, fwdRecord{seq: seq, winner: winner})
	f.mu.Unlock()
}

func (f *fwdLog) snapshot() []fwdRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fwdRecord(nil), f.s...)
}

// TestArbitrationLostDrainsForward 钉死输家整箱转发（规格 §12）：
// 激活失败错误实现 ArbitrationLost 时，在箱投递按 FIFO 逐条 Forward(赢家)，
// 不得有任何 Reject，随后照常终态。
func TestArbitrationLostDrainsForward(t *testing.T) {
	id := mustUUID(t, "player", "fwd1")
	log := &fwdLog{}
	ds := make([]*recDelivery, 3)
	for i := range ds {
		d := newRec(id)
		seq := i
		d.onForward = func(w rans.NodeID) { log.record(seq, w) }
		ds[i] = d
	}
	p := New(id, Options{
		InboxCap: 4,
		Activate: func(ctx context.Context, p *Pawnite) error {
			return &lostErr{winner: 9}
		},
	})
	p.Start(context.Background())
	for i, d := range ds {
		if !p.ReceiveRequest(d) {
			t.Fatalf("admit %d", i)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(log.snapshot()) < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	got := log.snapshot()
	if len(got) != 3 {
		t.Fatalf("inbox must drain via Forward on arbitration loss, got %d forwards", len(got))
	}
	for i, r := range got {
		if r.seq != i {
			t.Fatalf("forward order broken: position %d carried seq %d (FIFO violated)", i, r.seq)
		}
		if r.winner != 9 {
			t.Fatalf("forward winner = %d, want 9", r.winner)
		}
	}
	// forwardable 投递不得走 Reject。
	for i, d := range ds {
		select {
		case r := <-d.rejected:
			t.Fatalf("forwardable delivery %d must not be rejected: %v %q", i, r.st, r.reason)
		default:
		}
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("pawnite must reach terminal state after arbitration loss")
	}
}

// TestArbitrationLostLateAdmissionForwards 钉死仲裁输家终局竞窗的零丢失
// （testkit TestAcceptance_MigrationWindowZeroLoss 的 flaky 根因，本测试把
// 该窗确定性拉宽）：run 的激活失败分支先 drainForwardTo（整箱转发）
// 再 finish（Deactivate 钩子 → Destroyed 翻转 → 终局复扫）——两段之间准入
// 的迟到投递必须同样转发给赢家，而非按 "destroyed" 回绝（调用方位置是
// 落败前的合法观测，规格 §12 输家转发）。Deactivate 钩子被闸住即按需开窗：
// 钩子进入信号 ⟹ 整箱排空已发生，此时准入者只能经终局复扫处置。
func TestArbitrationLostLateAdmissionForwards(t *testing.T) {
	id := mustUUID(t, "player", "late-admit")
	deactEntered := make(chan struct{})
	gate := make(chan struct{})
	var openOnce sync.Once
	openGate := func() { openOnce.Do(func() { close(gate) }) }
	defer openGate() // 失败路径兜底放行
	p := New(id, Options{
		InboxCap: 4,
		Activate: func(ctx context.Context, p *Pawnite) error {
			return &lostErr{winner: 9}
		},
		Deactivate: func(ctx context.Context, p *Pawnite) error {
			close(deactEntered)
			<-gate
			return nil
		},
	})
	p.Start(context.Background())
	select {
	case <-deactEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("Deactivate never entered (activation failure teardown stuck)")
	}
	// 整箱排空已发生（drainForwardTo 先于 finish 的 Deactivate 钩子）；
	// 状态尚在 Activating（终局翻转在钩子之后）——迟到者合法准入。
	d := newRec(id)
	if !p.ReceiveRequest(d) {
		t.Fatal("late delivery must be admitted before the terminal flip (state Activating)")
	}
	openGate()
	select {
	case w := <-d.forwarded:
		if w != 9 {
			t.Fatalf("late-admitted delivery forwarded to node %d, want 9", w)
		}
	case r := <-d.rejected:
		t.Fatalf("late-admitted delivery must forward to the winner, got reject %v %q", r.st, r.reason)
	case <-time.After(2 * time.Second):
		t.Fatal("late-admitted delivery neither forwarded nor rejected")
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("pawnite must reach terminal state after arbitration loss")
	}
}

// TestArbitrationLostStragglerAfterTerminalForwards 竞争次序的另一腿：
// 迟到者排在 Destroyed 翻转之后才敲门（调度交错把 ReceiveRequest 压在
// GetOrActivate 已返回实例的终态之后）。仲裁输家实例不得按销毁回绝——
// Forwardable 投递转发给赢家；非 Forwardable 按 Unavailable 回绝并提及
// 仲裁（与整箱排空的回绝分支同口径）。
func TestArbitrationLostStragglerAfterTerminalForwards(t *testing.T) {
	id := mustUUID(t, "player", "straggler")
	p := New(id, Options{
		InboxCap: 4,
		Activate: func(ctx context.Context, p *Pawnite) error {
			return &lostErr{winner: 9}
		},
	})
	p.Start(context.Background())
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("pawnite must reach terminal state after arbitration loss")
	}
	d := newRec(id)
	if p.ReceiveRequest(d) {
		t.Fatal("straggler after terminal state must not be admitted")
	}
	select {
	case w := <-d.forwarded:
		if w != 9 {
			t.Fatalf("forwardable straggler forwarded to node %d, want 9", w)
		}
	case r := <-d.rejected:
		t.Fatalf("forwardable straggler must forward to the winner, got reject %v %q", r.st, r.reason)
	case <-time.After(2 * time.Second):
		t.Fatal("straggler neither forwarded nor rejected")
	}
	pd := &plainDelivery{id: id, rejected: make(chan rejectInfo, 1)}
	if p.ReceiveRequest(pd) {
		t.Fatal("non-forwardable straggler after terminal state must not be admitted")
	}
	select {
	case r := <-pd.rejected:
		if r.st != rans.CodeUnavailable {
			t.Fatalf("non-forwardable straggler reject status = %v, want StatusUnavailable", r.st)
		}
		if r.reason != rans.ReasonArbitrationLost {
			t.Fatalf("reject reason = %v, want ReasonArbitrationLost (arbitration-lost fallback)", r.reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("non-forwardable straggler must be rejected")
	}
}

// plainDelivery 只实现 InboundDelivery（无 Forward 能力，本地投递形态）：
// 仲裁落败时必须按 StatusUnavailable 回结对端，而非转发。
type plainDelivery struct {
	id       rans.ID
	rejected chan rejectInfo
}

func (d *plainDelivery) UUID() rans.ID      { return d.id }
func (d *plainDelivery) Deliver(_ *Pawnite) {}
func (d *plainDelivery) Reject(st rans.Code, reason rans.Reason) {
	d.rejected <- rejectInfo{st: st, reason: reason}
}

// TestArbitrationLostNonForwardableRejects 钉死非 Forwardable 投递的回绝分支：
// StatusUnavailable + 理由提及仲裁落败。
func TestArbitrationLostNonForwardableRejects(t *testing.T) {
	id := mustUUID(t, "player", "fwd2")
	d := &plainDelivery{id: id, rejected: make(chan rejectInfo, 1)}
	// 激活钩子先于败裁挂闸：准入（Activating 态入箱合法）确定先于败裁落地
	// ——否则 Start 的 run goroutine 可在主线程 ReceiveRequest 前跑完整个
	// 激活失败终局，迟到者走 disposeLate（返回 false），测试前提崩塌。
	entered := make(chan struct{})
	release := make(chan struct{})
	p := New(id, Options{
		InboxCap: 4,
		Activate: func(ctx context.Context, p *Pawnite) error {
			close(entered)
			<-release
			return &lostErr{winner: 9}
		},
	})
	p.Start(context.Background())
	<-entered
	if !p.ReceiveRequest(d) {
		t.Fatal("admit")
	}
	close(release)
	select {
	case r := <-d.rejected:
		if r.st != rans.CodeUnavailable {
			t.Fatalf("non-forwardable reject status = %v, want StatusUnavailable", r.st)
		}
		if r.reason != rans.ReasonArbitrationLost {
			t.Fatalf("reject reason = %v, want ReasonArbitrationLost (arbitration-lost fallback)", r.reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("non-forwardable delivery must be rejected on arbitration loss")
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("pawnite must reach terminal state")
	}
}
