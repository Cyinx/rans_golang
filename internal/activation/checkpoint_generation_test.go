package activation

// checkpoint 簿记的实例身份守卫测试（审查 M5）：簿记收尾（End/Abort/
// AwaitSettled）按 id 命中条目后必须校验 inst.p == p——慢 Put 收尾窗内
// 实例被强弃并同 id 重激活换代时，旧 goroutine 的收尾不得写进新实例
//（纪元不被抬高、在飞门不被误放），否则新 incarnation 的脏门控长期判净。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// TestCheckpointBookkeeping_StaleEndKeepsNewIncarnationDirty：慢 Put 收尾
// 窗内旧实例被销毁并同 id 重激活，旧 goroutine 的 EndCheckpoint 不得把
// 旧实例的活动纪元抬进新实例的 ckptEpoch——新实例投递后的脏门控必须
// 照常放行捕获。复刻 sweeper.captureCheckpoint 收尾 goroutine 的交错
// （捕获收妥 → 慢 Put → 收尾），换代点由 putHold 精确钉住。
func TestCheckpointBookkeeping_StaleEndKeepsNewIncarnationDirty(t *testing.T) {
	X := mustTestID(t, "acct", "gen-end")
	base := checkpoint.New()
	rec := &recStore{Store: base}
	rec.putEntered = make(chan struct{}) // 「Put 已在飞」信号（首次 Put 进入即关）
	rec.putHold = make(chan struct{})
	ent := &acctEntity{}
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"acct": func(_ rans.ID) rans.Entity { return ent },
	}, Options{Store: rec})

	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()

	p1, err := m.GetOrActivate(ctx, X)
	if err != nil {
		t.Fatalf("首代激活失败：%v", err)
	}
	for i := 0; i < 2; i++ { // 旧实例纪元推到 3（入 Active 记 1 + 两次投递）
		if _, err := m.InvokeLocal(ctx, X, "add:5"); err != nil {
			t.Fatalf("旧实例投递失败：%v", err)
		}
	}
	if !m.TryBeginCheckpoint(p1) {
		t.Fatal("旧实例首轮捕获占门应成功（脏：纪元 3 > 0）")
	}
	done := make(chan pawnite.CaptureOutcome, 1)
	if !p1.CaptureOnLoop(ent.Snapshot, done) {
		t.Fatal("旧实例捕获入队应成功")
	}
	settled := make(chan struct{})
	go func() { // sweeper.captureCheckpoint 收尾 goroutine 同构
		defer close(settled)
		out := <-done
		if out.Skipped || out.Err != nil {
			t.Error("捕获应在强弃前收妥，得到 Skipped/Err")
			return
		}
		if perr := m.SnapshotStore().Put(context.Background(), X, p1.Incarnation(), out.Snapshot); perr != nil {
			m.AbortCheckpoint(p1)
			return
		}
		m.EndCheckpoint(p1, out.Epoch)
	}()
	<-rec.putEntered // 旧 Put 已进入慢收尾窗（阻塞在 putHold 上）

	if derr := p1.Destroy(context.Background()); derr != nil {
		t.Fatalf("旧实例销毁失败：%v", derr)
	}
	waitTerminalEviction(t, m, X)
	p2, err := m.GetOrActivate(ctx, X) // 同 id 立即重激活换代
	if err != nil {
		t.Fatalf("换代激活失败：%v", err)
	}
	if p2 == p1 {
		t.Fatal("换代激活应产出新实例")
	}
	if _, err := m.InvokeLocal(ctx, X, "add:5"); err != nil { // 新实例纪元 2：已脏
		t.Fatalf("新实例投递失败：%v", err)
	}

	close(rec.putHold) // 放行旧 Put：收尾 goroutine 即将命中已换代的条目
	<-settled

	if !m.TryBeginCheckpoint(p2) {
		t.Fatalf("新实例脏门控被旧收尾污染：ckptEpoch=%d 抬过了活动纪元 %d"+
			"（旧 goroutine 的 EndCheckpoint 写进了换代后的条目）",
			m.peekLocal(X).ckptEpoch.Load(), p2.Epoch())
	}
}

// TestCheckpointBookkeeping_StaleAbortAndSettleLeaveNewIncarnationAlone：
// 旧实例的失败收尾（AbortCheckpoint）不得误放新实例的在飞门（同实体
// IO 永不重叠）；同族等待面（AwaitCheckpointSettled）对已换代条目视为
// 已落定（旧实例的在飞簿记已随逐出作废），不得阻塞在新实例的门上。
func TestCheckpointBookkeeping_StaleAbortAndSettleLeaveNewIncarnationAlone(t *testing.T) {
	X := mustTestID(t, "acct", "gen-abort")
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} },
	}, Options{Store: checkpoint.New()})

	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()

	p1, err := m.GetOrActivate(ctx, X)
	if err != nil {
		t.Fatalf("首代激活失败：%v", err)
	}
	if _, err := m.InvokeLocal(ctx, X, "add:5"); err != nil {
		t.Fatalf("旧实例投递失败：%v", err)
	}
	if !m.TryBeginCheckpoint(p1) {
		t.Fatal("旧实例捕获占门应成功")
	}

	if derr := p1.Destroy(context.Background()); derr != nil {
		t.Fatalf("旧实例销毁失败：%v", derr)
	}
	waitTerminalEviction(t, m, X)
	p2, err := m.GetOrActivate(ctx, X)
	if err != nil {
		t.Fatalf("换代激活失败：%v", err)
	}
	if _, err := m.InvokeLocal(ctx, X, "add:5"); err != nil {
		t.Fatalf("新实例投递失败：%v", err)
	}
	if !m.TryBeginCheckpoint(p2) {
		t.Fatal("新实例捕获占门应成功（在飞门由新实例持有）")
	}

	sctx, scancel := ctxT(100 * time.Millisecond)
	defer scancel()
	if err := m.AwaitCheckpointSettled(sctx, p1); err != nil {
		t.Fatalf("旧实例已换代：等待面应视为已落定返回 nil，得到 %v"+
			"（旧 goroutine 阻塞在新实例的在飞门上）", err)
	}
	m.AbortCheckpoint(p1) // 旧 goroutine 的失败收尾命中换代后的条目
	if m.TryBeginCheckpoint(p2) {
		t.Fatal("旧 AbortCheckpoint 误放新实例在飞门：同实体 IO 重叠窗被敞开")
	}
}
