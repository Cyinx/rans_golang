package membership

// Liveness 自围栏（H13：自 LeaseKeeper 迁入）单元验收：成员键写持续失败
// 超过 SelfFence → 对 LocalIDs 全集逐实体 Fence（同步关准入）+ OnSelfFenceLost
// 止损销毁（独立 goroutine，不阻塞存活循环——阻塞不冻结面由 EX5 族
// TestLiveness_NodeKeyNotFrozenByBlockingDestroy 锚定）；写恢复即复位
//（下一失败幕可再次触发）；<0 关闭。

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

func TestSelfFence_TriggersOnProlongedFailure(t *testing.T) {
	reg := newFakeNodeRegistry(nil)
	a, b := mustID(t, "k", "sf-a"), mustID(t, "k", "sf-b")

	var mu sync.Mutex
	fenced := make(map[rans.ID]int)
	lost := make(map[rans.ID]int)
	lv, err := NewLiveness(LivenessConfig{
		Reg: reg, Self: 1, Interval: 20 * time.Millisecond,
		SelfFence: 60 * time.Millisecond,
		Fence:     func(id rans.ID) { mu.Lock(); fenced[id]++; mu.Unlock() },
		LocalIDs:  func() []rans.ID { return []rans.ID{a, b} },
		OnSelfFenceLost: func(_ context.Context, lostID rans.ID) error {
			mu.Lock()
			lost[lostID]++
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	lv.PublishSelf("127.0.0.1:1", nil)
	startLiveness(t, lv)

	reg.waitCalled(t) // 首写成功
	reg.failWrites(errors.New("partition"))
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		done := len(fenced) == 2 && len(lost) == 2
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("self-fence did not cover all locals: fenced=%v lost=%v", fenced, lost)
			mu.Unlock()
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// 写恢复即复位：失败幕恰触发一次（恢复后无新失败不再围栏）。
	reg.healWrites()
	reg.waitCalled(t)
	time.Sleep(120 * time.Millisecond)
	mu.Lock()
	if fenced[a] != 1 || fenced[b] != 1 {
		t.Fatalf("fence must fire exactly once per failure episode: %v", fenced)
	}
	mu.Unlock()
}

func TestSelfFence_DisabledByNegative(t *testing.T) {
	reg := newFakeNodeRegistry(nil)
	reg.failWrites(errors.New("partition"))
	var fenced int
	lv, err := NewLiveness(LivenessConfig{
		Reg: reg, Self: 1, Interval: 20 * time.Millisecond, SelfFence: -1,
		Fence:    func(rans.ID) { fenced++ },
		LocalIDs: func() []rans.ID { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	lv.PublishSelf("127.0.0.1:1", nil)
	startLiveness(t, lv)
	reg.waitCalled(t)
	reg.waitCalled(t)
	time.Sleep(80 * time.Millisecond)
	if fenced != 0 {
		t.Fatalf("SelfFence<0 must disable fencing, got %d", fenced)
	}
}
