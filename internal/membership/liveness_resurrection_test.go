package membership

// 成员面一死永死状态机：Absent 失主重取号、待发布代际不误判 Superseded、
// Superseded 让位停写、排空中失主不再注册、计数器回退保护。

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// newResLiveness 带围栏计数的便捷构造。
func newResLiveness(t *testing.T, reg *fakeNodeRegistry, interval time.Duration) (*Liveness, *atomic.Int32) {
	t.Helper()
	var rounds atomic.Int32
	lv, err := NewLiveness(LivenessConfig{
		Reg: reg, Self: 1, Interval: interval, SelfFence: -1,
		Fence: func(uint64) int { rounds.Add(1); return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	return lv, &rounds
}

// TestLiveness_AbsentFencesThenReEpochs 成员键在存活期消失：第一遍围栏
// 全部、重取号写新键、发布新代际后第二遍围栏（keep=新代际）；新代际严格
// 大于旧值，resurrected 计数 ≥1。
func TestLiveness_AbsentFencesThenReEpochs(t *testing.T) {
	reg := newFakeNodeRegistry(nil)
	lv, rounds := newResLiveness(t, reg, 20*time.Millisecond)
	lv.PublishSelf("127.0.0.1:1", nil)
	startLiveness(t, lv)

	reg.waitRenew(t)
	old := lv.NodeEpoch()
	if old == 0 {
		t.Fatal("precondition: epoch must be published before dropping the key")
	}
	reg.dropKey(1)

	deadline := time.After(2 * time.Second)
	for lv.NodeEpoch() <= old {
		select {
		case <-deadline:
			t.Fatalf("liveness did not re-epoch after key loss: epoch=%d old=%d", lv.NodeEpoch(), old)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if n := rounds.Load(); n < 2 {
		t.Fatalf("re-epoching must fence twice (all + keep-new), got %d rounds", n)
	}
	deadline2 := time.After(2 * time.Second)
	for {
		renews := reg.renewals()
		if n := len(renews); n > 0 && renews[n-1].info.Epoch == lv.NodeEpoch() {
			break
		}
		select {
		case <-deadline2:
			t.Fatalf("post-re-epoch beats must renew the new epoch %d", lv.NodeEpoch())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// TestLiveness_PendingEpochNotMistakenForSupersede RegisterNode 服务端成功
// 而客户端报错（next 保持待发布）：下一 tick 重写同值（不误判 Superseded、
// 不退回旧代际续期），成功后发布。
func TestLiveness_PendingEpochNotMistakenForSupersede(t *testing.T) {
	reg := newFakeNodeRegistry(nil)
	lv, _ := newResLiveness(t, reg, 20*time.Millisecond)
	lv.PublishSelf("127.0.0.1:1", nil)
	startLiveness(t, lv)

	reg.waitCalled(t) // 首写成功（代际已发布）
	first := lv.NodeEpoch()
	reg.dropKey(1)
	// 注入「服务端成功、客户端报错」：fake 的 failWrites 让 RegisterNode
	// 报错但取号/视图照常推进——重写同值 next 直至成功。
	reg.failWrites(errors.New("torn write"))
	reg.healWrites()

	deadline := time.After(2 * time.Second)
	for lv.NodeEpoch() <= first {
		select {
		case <-deadline:
			t.Fatalf("pending epoch never published: epoch=%d first=%d", lv.NodeEpoch(), first)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if reg.supersededSeen() {
		t.Fatal("pending epoch must not be mistaken for supersede")
	}
}

// TestLiveness_SupersededStopsWriting 成员键被更大代际占据（NodeID 重号）：
// 让位回调触发、此后不再有任何注册/续期。
func TestLiveness_SupersededStopsWriting(t *testing.T) {
	reg := newFakeNodeRegistry(nil)
	var yielded atomic.Bool
	lv, rounds := newResLiveness(t, reg, 20*time.Millisecond)
	lv.onSuperseded = func() { yielded.Store(true) }
	lv.PublishSelf("127.0.0.1:1", nil)
	startLiveness(t, lv)

	reg.waitRenew(t)
	old := lv.NodeEpoch()
	reg.supersedeKey(1, old+5)

	deadline := time.After(2 * time.Second)
	for !yielded.Load() {
		select {
		case <-deadline:
			t.Fatal("liveness never yielded after supersede")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if n := rounds.Load(); n < 1 {
		t.Fatal("supersede must fence all local entities")
	}
	regs := len(reg.registrations())
	renews := len(reg.renewals())
	time.Sleep(80 * time.Millisecond)
	if len(reg.registrations()) != regs || len(reg.renewals()) != renews {
		t.Fatal("yielded liveness must stop writing the membership key")
	}
}

// TestLiveness_AbsentWhileDrainingNoReRegister 排空中失主：围栏全部后停写
// （不再注册——排空是终态动作，键交给 TTL/接管）。
func TestLiveness_AbsentWhileDrainingNoReRegister(t *testing.T) {
	reg := newFakeNodeRegistry(nil)
	lv, rounds := newResLiveness(t, reg, 20*time.Millisecond)
	lv.PublishSelf("127.0.0.1:1", nil)
	startLiveness(t, lv)

	reg.waitRenew(t)
	lv.BeginDraining()
	reg.dropKey(1)

	deadline := time.After(2 * time.Second)
	for rounds.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("absent while draining must still fence all")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	regs := len(reg.registrations())
	time.Sleep(80 * time.Millisecond)
	if len(reg.registrations()) != regs {
		t.Fatal("draining liveness must not re-register after key loss")
	}
}

// TestLiveness_EpochCounterRegressionStillAdvances 计数器随 Redis 丢数据
// 回退时取号不回退：新代际 = 旧值+1（存活期内代际单调）。
func TestLiveness_EpochCounterRegressionStillAdvances(t *testing.T) {
	reg := newFakeNodeRegistry(nil)
	lv, _ := newResLiveness(t, reg, 20*time.Millisecond)
	lv.PublishSelf("127.0.0.1:1", nil)
	startLiveness(t, lv)

	reg.waitRenew(t)
	old := lv.NodeEpoch()
	reg.mu.Lock()
	reg.nextEpoch = 1 // 模拟计数器回退（丢数据后重置）
	reg.mu.Unlock()
	reg.dropKey(1)

	deadline := time.After(2 * time.Second)
	for lv.NodeEpoch() <= old {
		select {
		case <-deadline:
			t.Fatalf("re-epoch did not advance past regression: epoch=%d old=%d", lv.NodeEpoch(), old)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if got := lv.NodeEpoch(); got != old+1 {
		t.Fatalf("re-epoch under regression = %d, want old+1 = %d", got, old+1)
	}
}

// supersededSeen fake 是否返回过 Superseded 判定。
func (f *fakeNodeRegistry) supersededSeen() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sawSuperseded
}
