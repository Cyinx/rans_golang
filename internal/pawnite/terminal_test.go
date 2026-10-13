package pawnite

// 终局归一矩阵测试：五条终局路径（优雅销毁、Start 前快路径、迁移赢、
// 仲裁输、看门狗强弃）收敛为 terminal 写一次 + disposeLate 单点后的处置
// 契约锚定——每种 TerminalReason × 三个到达面：
//   ① 准入面：终态敲门（ReceiveRequest 就地处置）；
//   ② 复扫面：终局翻转前入箱、经 finish 复扫 / Start 前快路径处置；
//   ③ 强弃面：看门狗强弃锁内快照的处置。
//
// 断言状态码/Reason 与转发目标（Reason 契约面；文案自由度留给 disposeLate 的
// lateReason 形参与原因前缀）。矩阵要点：Retired/Abandoned 行的
// Forwardable 投递不转发（只有 Migrated/ArbitrationLost 让渡了目录归属）；
// 非 Forwardable 探针在 Migrated/ArbitrationLost 行收到 Unavailable
//（disposition 既有语义）。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// newPlain 非 Forwardable 探针（仅 InboundDelivery——本地投递形态）。
func newPlain(id rans.ID) *plainDelivery {
	return &plainDelivery{id: id, rejected: make(chan rejectInfo, 1)}
}

// assertDisposed 矩阵断言：wantSt==0 → Forwardable 探针收到 Forward(target)、
// 普通探针收到 Unavailable（非 Forwardable 不转发）；否则两探针均以 wantSt
// 回绝且 Forwardable 探针不得被转发。
func assertDisposed(t *testing.T, fwd *recDelivery, plain *plainDelivery, target rans.NodeID, wantSt rans.Code) {
	t.Helper()
	if wantSt == 0 {
		select {
		case w := <-fwd.forwarded:
			if w != target {
				t.Fatalf("forwardable probe forwarded to %d, want %d", w, target)
			}
		case r := <-fwd.rejected:
			t.Fatalf("forwardable probe must forward to %d, got reject %v %q", target, r.st, r.reason)
		case <-time.After(2 * time.Second):
			t.Fatal("forwardable probe neither forwarded nor rejected")
		}
		select {
		case r := <-plain.rejected:
			if r.st != rans.CodeUnavailable {
				t.Fatalf("non-forwardable probe reject status = %v, want StatusUnavailable", r.st)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("non-forwardable probe must be rejected")
		}
		return
	}
	select {
	case r := <-fwd.rejected:
		if r.st != wantSt {
			t.Fatalf("forwardable probe reject status = %v, want %v (must not forward on this reason)", r.st, wantSt)
		}
	case w := <-fwd.forwarded:
		t.Fatalf("forwardable probe must not be forwarded on this terminal reason, forwarded to %d", w)
	case <-time.After(2 * time.Second):
		t.Fatal("forwardable probe neither forwarded nor rejected")
	}
	select {
	case r := <-plain.rejected:
		if r.st != wantSt {
			t.Fatalf("plain probe reject status = %v, want %v", r.st, wantSt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("plain probe must be rejected")
	}
}

// actOK 无副作用的激活钩子。
func actOK(context.Context, *Pawnite) error { return nil }

// wonCore 立即赢的迁移 core。
func wonCore(context.Context) MigrationOutcome { return MigrationOutcome{Won: true} }

// faceAdmission 准入面：经真实终局写点置入终态后投递探针——
// ReceiveRequest 必须拒绝准入并按矩阵就地处置。
func faceAdmission(t *testing.T, reason TerminalReason, target rans.NodeID, wantSt rans.Code) {
	t.Helper()
	id := mustUUID(t, "player", "mx-a")
	var p *Pawnite
	var release func()
	switch reason {
	case TerminalMigrated:
		p = New(id, Options{Activate: actOK})
		p.Start(context.Background())
		waitState(t, p, StateActive)
		if err := p.Migrate(context.Background(), target, wonCore); err != nil {
			t.Fatalf("Migrate err = %v", err)
		}
	case TerminalArbitrationLost:
		p = New(id, Options{Activate: func(context.Context, *Pawnite) error {
			return &lostErr{winner: target}
		}})
		p.Start(context.Background())
	case TerminalRetired:
		p = New(id, Options{Activate: actOK})
		p.Start(context.Background())
		waitState(t, p, StateActive)
		p.Destroy(context.Background())
	case TerminalAbandoned:
		rel := make(chan struct{})
		actEntered := make(chan struct{})
		p = New(id, Options{Activate: func(context.Context, *Pawnite) error {
			close(actEntered)
			<-rel
			return nil
		}})
		p.Start(context.Background())
		<-actEntered
		if !p.Abandon(StateActivating, TerminalAbandoned) {
			t.Fatal("abandon must win on stuck Activating")
		}
		release = func() { close(rel) }
	}
	defer func() {
		if release != nil {
			release()
		}
	}()
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("pawnite must reach terminal state")
	}
	fwd, plain := newRec(id), newPlain(id)
	if p.ReceiveRequest(fwd) {
		t.Fatal("straggler after terminal must not be admitted")
	}
	if p.ReceiveRequest(plain) {
		t.Fatal("plain straggler after terminal must not be admitted")
	}
	assertDisposed(t, fwd, plain, target, wantSt)
}

// faceSweep 复扫面：终局翻转前入箱的探针经 finish 复扫（Migrated/
// ArbitrationLost 在终局钩子窗内准入留箱）或 Start 前快路径（Retired——
// 正常销毁的在箱余量由 stopShutdown 的 drainReject 先行排空，复扫是
// 防御性兜底，公开面不可构造）处置。
func faceSweep(t *testing.T, reason TerminalReason, target rans.NodeID, wantSt rans.Code) {
	t.Helper()
	id := mustUUID(t, "player", "mx-s")
	fwd, plain := newRec(id), newPlain(id)
	switch reason {
	case TerminalMigrated:
		var admitted int
		var p *Pawnite
		p = New(id, Options{
			Activate: actOK,
			Deactivate: func(context.Context, *Pawnite) error {
				// finish 序：Deactivate 在终局复扫之前——迁移窗仍开，准入留箱。
				if p.ReceiveRequest(fwd) {
					admitted++
				}
				if p.ReceiveRequest(plain) {
					admitted++
				}
				return nil
			},
		})
		p.Start(context.Background())
		waitState(t, p, StateActive)
		if err := p.Migrate(context.Background(), target, wonCore); err != nil {
			t.Fatalf("Migrate err = %v", err)
		}
		if admitted != 2 {
			t.Fatal("probes must be held in box during the finish hook window (migration window open)")
		}
		select {
		case <-p.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("Won migration must reach terminal state")
		}
	case TerminalArbitrationLost:
		var admitted int
		var p *Pawnite
		p = New(id, Options{
			Activate: func(context.Context, *Pawnite) error {
				return &lostErr{winner: target}
			},
			Deactivate: func(context.Context, *Pawnite) error {
				// 落败排空已跑、终局翻转未落（状态仍 Activating）——准入留箱，
				// 归宿是 finish 复扫。
				if p.ReceiveRequest(fwd) {
					admitted++
				}
				if p.ReceiveRequest(plain) {
					admitted++
				}
				return nil
			},
		})
		p.Start(context.Background())
		select {
		case <-p.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("pawnite must reach terminal state")
		}
		if admitted != 2 {
			t.Fatal("probes must be admitted during the pre-flip Activating window")
		}
	case TerminalRetired:
		p := New(id, Options{})
		if !p.ReceiveRequest(fwd) || !p.ReceiveRequest(plain) {
			t.Fatal("admission in Created state must succeed (held for the pre-Start fast path)")
		}
		if err := p.Destroy(context.Background()); err != nil {
			t.Fatalf("Destroy err = %v, want nil", err)
		}
	case TerminalAbandoned:
		// 复扫面公开行为不可构造（强弃先排空在箱，终态后无新入箱路径）；
		// 白盒直接锚 disposeLate 对 Abandoned 的矩阵分流。
		p := New(id, Options{})
		p.mu.Lock()
		p.setTerminalLocked(TerminalAbandoned, 0)
		p.mu.Unlock()
		p.disposeLate([]InboundDelivery{fwd, plain})
		return
	}
	assertDisposed(t, fwd, plain, target, wantSt)
}

// faceAbandon 强弃面：看门狗强弃锁内快照的处置。Retired 行不适用——
// 强弃只写 TerminalAbandoned，且 Retired 只随终态翻转写入、强弃 gate 在
// 终态让位。
func faceAbandon(t *testing.T, reason TerminalReason, target rans.NodeID, wantSt rans.Code) {
	t.Helper()
	if reason == TerminalRetired {
		t.Skip("强弃不产生 Retired 终局：Retired 只随终态翻转写入，强弃 gate 在终态让位")
	}
	id := mustUUID(t, "player", "mx-b")
	fwd, plain := newRec(id), newPlain(id)
	switch reason {
	case TerminalMigrated:
		deactEntered := make(chan struct{})
		release := make(chan struct{})
		var p *Pawnite
		p = New(id, Options{
			Activate: actOK,
			Deactivate: func(context.Context, *Pawnite) error {
				close(deactEntered)
				<-release
				return nil
			},
		})
		p.Start(context.Background())
		waitState(t, p, StateActive)
		res := make(chan error, 1)
		go func() { res <- p.Migrate(context.Background(), target, wonCore) }()
		<-deactEntered // Won 已裁决、整箱转发已跑、finish 卡在 Deactivate（迁移窗仍放行留箱）
		if !p.ReceiveRequest(fwd) || !p.ReceiveRequest(plain) {
			t.Fatal("won window must hold probes in box")
		}
		if !p.Abandon(StateDestroying, TerminalAbandoned) {
			t.Fatal("abandon must win on stuck Destroying")
		}
		assertDisposed(t, fwd, plain, target, wantSt)
		close(release)
		if err := <-res; err != nil {
			t.Fatalf("Migrate result = %v, want nil", err)
		}
	case TerminalArbitrationLost:
		deactEntered := make(chan struct{})
		release := make(chan struct{})
		var p *Pawnite
		p = New(id, Options{
			Activate: func(context.Context, *Pawnite) error {
				return &lostErr{winner: target}
			},
			Deactivate: func(context.Context, *Pawnite) error {
				close(deactEntered)
				<-release
				return nil
			},
		})
		p.Start(context.Background())
		<-deactEntered // 落败排空已跑、finish 卡在 Deactivate（状态仍 Activating，准入留箱）
		if !p.ReceiveRequest(fwd) || !p.ReceiveRequest(plain) {
			t.Fatal("activating window must hold probes in box")
		}
		if !p.Abandon(StateActivating, TerminalAbandoned) {
			t.Fatal("abandon must win on stuck Activating (deactivate phase of failed activation)")
		}
		// 写一次锚点之一：ArbitrationLost 先落，强弃的 Abandoned 补写让位——
		// 迟到处置仍按先到原因转投赢家（激活已落败，赢家持有目录归属）。
		assertDisposed(t, fwd, plain, target, wantSt)
		close(release)
		select {
		case <-p.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("pawnite must stay terminal after zombie continuation")
		}
	case TerminalAbandoned:
		actEntered := make(chan struct{})
		release := make(chan struct{})
		p := New(id, Options{Activate: func(context.Context, *Pawnite) error {
			close(actEntered)
			<-release
			return nil
		}})
		p.Start(context.Background())
		<-actEntered
		waitForState(t, p, StateActivating)
		if !p.ReceiveRequest(fwd) || !p.ReceiveRequest(plain) {
			t.Fatal("activating admission must queue probes")
		}
		if !p.Abandon(StateActivating, TerminalAbandoned) {
			t.Fatal("abandon must win on stuck Activating")
		}
		assertDisposed(t, fwd, plain, target, wantSt)
		close(release)
	}
}

// TestDisposeLate_Matrix 迟到处置矩阵表驱动主测试。
func TestDisposeLate_Matrix(t *testing.T) {
	cases := []struct {
		name   string
		reason TerminalReason
		target rans.NodeID
		wantSt rans.Code // 0 = 期望 Forward(target)
	}{
		{"migrated", TerminalMigrated, 7, 0},
		{"arbitration_lost", TerminalArbitrationLost, 3, 0},
		{"retired", TerminalRetired, 0, rans.CodeUnavailable},
		{"abandoned", TerminalAbandoned, 0, rans.CodeTimeout},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Run("admission", func(t *testing.T) { faceAdmission(t, tc.reason, tc.target, tc.wantSt) })
			t.Run("sweep", func(t *testing.T) { faceSweep(t, tc.reason, tc.target, tc.wantSt) })
			t.Run("abandon", func(t *testing.T) { faceAbandon(t, tc.reason, tc.target, tc.wantSt) })
		})
	}
}

// TestSetTerminalLocked_WriteOnce 写一次锚定：先到的终局原因权威——
// Migrated(9) 落位后 Abandoned 补写被忽略，处置仍按 Migrated 分流
// （Forwardable 探针 Forward(9)，非 Forwardable 探针 Unavailable——
// 顺带锚定转发行对非 Forwardable 的既有语义）。
func TestSetTerminalLocked_WriteOnce(t *testing.T) {
	id := mustUUID(t, "player", "mx-once")
	p := New(id, Options{})
	p.mu.Lock()
	p.setTerminalLocked(TerminalMigrated, 9)
	p.setTerminalLocked(TerminalAbandoned, 0) // 二次写入必须被忽略
	p.mu.Unlock()

	fwd, plain := newRec(id), newPlain(id)
	p.disposeLate([]InboundDelivery{fwd, plain})
	assertDisposed(t, fwd, plain, 9, 0)
}
