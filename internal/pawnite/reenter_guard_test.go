package pawnite

// 交接窗静默判据函数族的直接测试（三态对称性：挂起等待/即时放行/续体排队
// ——判据漏查续体队列时终快照静默丢已确认写，判据正本是「在飞 work 与续
// 体队列双空」）。附带续体执行收口断言：计入活动纪元（脏门控覆盖续体写）
// 与在途执行槽登记（执行时钟非零 ⟹ 在途槽有绑）。

import (
	"context"
	"testing"
)

// newActiveGuardPawnite 起 loop 并等至 Active 的最小判据道具。
func newActiveGuardPawnite(t *testing.T) *Pawnite {
	t.Helper()
	p := New(mustUUID(t, "guard", "g1"), Options{Activate: func(context.Context, *Pawnite) error {
		return nil
	}})
	p.Start(context.Background())
	waitState(t, p, StateActive)
	t.Cleanup(func() { p.Destroy(context.Background()) })
	return p
}

// guardReq 判据面入参（core 永不执行——判据函数不驱动 core；result 带缓冲
// 供 abort 还原挂起态的回输落缓冲即弃）。
func guardReq() handoffReq {
	return handoffReq{
		ctx:    context.Background(),
		kind:   HandoffRetire,
		core:   func(context.Context) HandoffOutcome { return HandoffOutcome{} },
		result: make(chan error, 1),
	}
}

// seedWorkLocked 直插一个在飞 work 登记（判据态道具；调用方持 mu）。
func seedWorkLocked(p *Pawnite) {
	p.works[&workUnit{cancel: func() {}}] = struct{}{}
}

// seedContinuation 按 completeWork 同款纪律直插一个续体任务：入队与重武装
// 信号在 mu 内完成（「队列非空 ⟹ 信号在」的唤醒不变量）。
func seedContinuation(p *Pawnite) {
	p.mu.Lock()
	p.reenterQ = append(p.reenterQ, func() {})
	p.mu.Unlock()
	select {
	case p.reenterCh <- struct{}{}:
	default:
	}
}

// drainContinuations 排空续体队列（判据还原用）。
func drainContinuations(p *Pawnite) {
	for {
		if _, ok := p.popReenter(); !ok {
			return
		}
	}
}

// TestHoldHandoffQuiescenceThreeStates holdHandoffForWorks 三态判据对称性：
// 在飞 work → 挂起；全静默 → 即时放行且不留挂起态；works 空而续体排队 →
// 必须挂起（core 不得先于已排队的续体执行——否则终快照缺续体写）。
func TestHoldHandoffQuiescenceThreeStates(t *testing.T) {
	cases := []struct {
		name      string
		seedWorks bool
		seedQueue bool
		wantHold  bool
	}{
		{"works_pending_holds", true, false, true},
		{"fully_quiesced_releases", false, false, false},
		{"works_empty_queue_nonempty_holds", false, true, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			p := newActiveGuardPawnite(t)
			if tc.seedWorks {
				p.mu.Lock()
				seedWorkLocked(p)
				p.mu.Unlock()
			}
			if tc.seedQueue {
				seedContinuation(p)
			}
			held := p.holdHandoffForWorks(guardReq())
			if held != tc.wantHold {
				t.Fatalf("holdHandoffForWorks = %v, want %v", held, tc.wantHold)
			}
			if !tc.wantHold {
				if p.heldHandoff != nil {
					t.Fatal("immediate release must not leave a held handoff")
				}
				return
			}
			if p.heldHandoff == nil {
				t.Fatal("held handoff must be registered when held")
			}
			p.abortPendingHandoff() // 还原挂起态（判据面不驱动 core）
		})
	}
}

// TestReleaseHeldHandoffQuiescenceSymmetry 挂起释放判据与挂起判据同源双空：
// 续体队列未排空不放行（终快照等续体落地），排空后放行且请求原样取回。
func TestReleaseHeldHandoffQuiescenceSymmetry(t *testing.T) {
	p := newActiveGuardPawnite(t)
	p.mu.Lock()
	seedWorkLocked(p)
	p.mu.Unlock()
	req := guardReq()
	if !p.holdHandoffForWorks(req) {
		t.Fatal("precondition: in-flight work must hold the handoff")
	}
	seedContinuation(p)
	if _, _, ok := p.releaseHeldHandoffIfQuiesced(); ok {
		t.Fatal("must not release while continuation queue is non-empty")
	}
	drainContinuations(p)
	p.mu.Lock()
	for u := range p.works {
		delete(p.works, u)
	}
	p.mu.Unlock()
	got, stopBudget, ok := p.releaseHeldHandoffIfQuiesced()
	if !ok {
		t.Fatal("must release once quiesced")
	}
	if got.kind != req.kind {
		t.Fatalf("released req kind = %v, want %v", got.kind, req.kind)
	}
	if stopBudget == nil {
		t.Fatal("released req must carry its budget stop")
	}
	stopBudget()
}

// TestRunCaptureQuiescence 周期捕获判据与交接判据同源：在飞 work 或续体
// 队列未排空时捕获必须 Skipped（零副作用，扫轮下轮重试）——此刻放行会把
// 未含续体写的快照连同覆盖该写的纪元一起入账，脏门控自此判净。
func TestRunCaptureQuiescence(t *testing.T) {
	cases := []struct {
		name      string
		seedWorks bool
		seedQueue bool
		wantSnap  bool
	}{
		{"works_pending_skips", true, false, false},
		{"queue_pending_skips", false, true, false},
		{"quiesced_captures", false, false, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			p := newActiveGuardPawnite(t)
			if tc.seedWorks {
				p.mu.Lock()
				seedWorkLocked(p)
				p.mu.Unlock()
			}
			if tc.seedQueue {
				seedContinuation(p)
			}
			ran := false
			done := make(chan CaptureOutcome, 1)
			p.runCapture(&captureReq{
				capture: func() ([]byte, error) {
					ran = true
					return []byte("SNAP"), nil
				},
				done: done,
			})
			out := <-done
			if !tc.wantSnap {
				if ran {
					t.Fatal("capture closure must not run while work/continuation pending")
				}
				if !out.Skipped {
					t.Fatalf("outcome = %+v, want Skipped", out)
				}
				return
			}
			if !ran || out.Skipped || string(out.Snapshot) != "SNAP" {
				t.Fatalf("quiesced capture ran=%v outcome=%+v, want snapshot", ran, out)
			}
			if out.Epoch != p.Epoch() {
				t.Fatalf("captured epoch = %d, want %d", out.Epoch, p.Epoch())
			}
		})
	}
}

// TestRunContinuationCountsEpochAndExecSlot 续体执行收口：执行计入活动纪元
// （脏门控由此覆盖续体写）且登记在途执行槽；出口槽与时钟双归零。
func TestRunContinuationCountsEpochAndExecSlot(t *testing.T) {
	p := newActiveGuardPawnite(t)
	epochBefore := p.Epoch()
	var epochIn int64
	var slotIn *InboundDelivery
	p.runContinuation(func() {
		epochIn = p.Epoch()
		slotIn = p.executing.Load()
	})
	if epochIn != epochBefore+1 {
		t.Fatalf("epoch inside continuation = %d, want %d", epochIn, epochBefore+1)
	}
	if slotIn == nil {
		t.Fatal("continuation must register the in-flight execution slot")
	}
	if p.executing.Load() != nil || p.executingSinceNano.Load() != 0 {
		t.Fatal("execution slot and stall clock must clear after continuation")
	}
}

// TestRunContinuationPanicClearsExecSlot 续体 panic 的簿记出清：槽与时钟照
// 常归零（Default 策略 loop 存活、实体不隔离）。
func TestRunContinuationPanicClearsExecSlot(t *testing.T) {
	p := newActiveGuardPawnite(t)
	p.runContinuation(func() { panic("boom") })
	if p.executing.Load() != nil || p.executingSinceNano.Load() != 0 {
		t.Fatal("execution slot and stall clock must clear after panic")
	}
	if p.State() != StateActive {
		t.Fatalf("state after continuation panic = %v, want Active (Default policy)", p.State())
	}
}
