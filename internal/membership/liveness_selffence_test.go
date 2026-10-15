package membership

// Liveness 自围栏单元验收：存活节拍持续失败超过 SelfFence → Fence(0)
// 围栏全部本地实例（钩子自带异步止损销毁——阻塞不冻结面由
// TestLiveness_NodeKeyNotFrozenByBlockingDestroy 锚定）；写恢复即复位
//（下一失败幕可再次触发）；<0 关闭。

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestSelfFence_TriggersOnProlongedFailure(t *testing.T) {
	reg := newFakeNodeRegistry(nil)

	var rounds atomic.Int32
	lv, err := NewLiveness(LivenessConfig{
		Reg: reg, Self: 1, Interval: 20 * time.Millisecond,
		SelfFence: 60 * time.Millisecond,
		Fence:     func(uint64) int { rounds.Add(1); return 2 },
	})
	if err != nil {
		t.Fatal(err)
	}
	lv.PublishSelf("127.0.0.1:1", nil)
	startLiveness(t, lv)

	reg.waitCalled(t) // 首写成功
	reg.failWrites(errors.New("partition"))
	deadline := time.After(2 * time.Second)
	for rounds.Load() == 0 {
		select {
		case <-deadline:
			t.Fatalf("self-fence did not fire: rounds=%d", rounds.Load())
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// 写恢复即复位：失败幕恰触发一次（恢复后无新失败不再围栏）。
	reg.healWrites()
	reg.waitRenew(t)
	time.Sleep(120 * time.Millisecond)
	if n := rounds.Load(); n != 1 {
		t.Fatalf("fence must fire exactly once per failure episode: rounds=%d", n)
	}
}

func TestSelfFence_DisabledByNegative(t *testing.T) {
	reg := newFakeNodeRegistry(nil)
	reg.failWrites(errors.New("partition"))
	var fenced int
	lv, err := NewLiveness(LivenessConfig{
		Reg: reg, Self: 1, Interval: 20 * time.Millisecond, SelfFence: -1,
		Fence: func(uint64) int { fenced++; return 0 },
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
