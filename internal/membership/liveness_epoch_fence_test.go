package membership

// Liveness 取号失败形态的自围栏记账验收：节点代际取号失败与成员键写失败
// 同按真实时刻入账。零时刻入账令记账双向失效——首败以零时刻为基线（垃圾
// 负值），第二轮失败即提前触发全表自毁；成功基线之后的零时刻失败呈巨大
// 负差，直接豁免不入账（成员键停更却无围栏）。本文件锚两个方向：取号连
// 续失败不提前触发（按 SelfFence 时长）、零时刻失败不被豁免（仍入账）。

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestSelfFence_EpochFailureNotEarly 触发基准是首败真实时刻 + SelfFence，
// 而非第二轮取号失败——零时刻基线曾把 SelfFence 阈值缩水到一轮 interval
// （节点从未对外可见就自毁全部本地实体）。
func TestSelfFence_EpochFailureNotEarly(t *testing.T) {
	reg := newFakeNodeRegistry(nil)
	reg.failEpoch(errors.New("directory partition"))

	var fenced atomic.Int32
	lv, err := NewLiveness(LivenessConfig{
		Reg: reg, Self: 1, Interval: 10 * time.Millisecond,
		SelfFence: 500 * time.Millisecond,
		Fence:     func(uint64) int { fenced.Add(1); return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	lv.PublishSelf("127.0.0.1:1", nil)
	startLiveness(t, lv)

	// 连续两轮取号失败（取号失败形态下 RegisterNode 永不被调用，写计数
	// 不是节拍证据——以取号失败计数为准）：围栏不得提前触发。
	deadline := time.After(2 * time.Second)
	for reg.epochFailCount() < 2 {
		select {
		case <-deadline:
			t.Fatal("epoch allocation did not fail twice; test premise broken")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if n := fenced.Load(); n != 0 {
		t.Fatalf("self-fence fired after two epoch failures, before SelfFence=%s (premature): fired=%d", lv.selfFence, n)
	}
	// 记账基线必须是真实时刻（零时刻入账的 UnixNano 是巨大负值，真实时刻恒正）。
	if base := lv.lastWriteOk.Load(); base <= 0 {
		t.Fatalf("failure accounting baseline must be a real timestamp, got %d (zero-time accounting)", base)
	}

	// 持续失败到 SelfFence 时长：围栏必须触发（失败按真实时刻推进记账）。
	deadline = time.After(3 * time.Second)
	for fenced.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("self-fence did not trigger past SelfFence under sustained epoch failure")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// TestSelfFence_ZeroTimeFailureStillAccounted 成功基线后的失败不得因入参
// 时刻为零被豁免：曾成功一轮（基线非零）后持续失败必须在 SelfFence 时长
// 内触发围栏——巨大负差直接 return 曾令失败完全不入账。直驱记账缝（循环
// 不参与：存活节拍的失败记账契约与驱动方式无关）。
func TestSelfFence_ZeroTimeFailureStillAccounted(t *testing.T) {
	reg := newFakeNodeRegistry(nil)

	var fenced atomic.Int32
	lv, err := NewLiveness(LivenessConfig{
		Reg: reg, Self: 1, Interval: time.Hour,
		SelfFence: 60 * time.Millisecond,
		Fence:     func(uint64) int { fenced.Add(1); return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}

	lv.noteWriteOutcome(true, time.Now()) // 曾成功一轮：基线非零
	deadline := time.After(2 * time.Second)
	for fenced.Load() == 0 {
		lv.noteWriteOutcome(false, time.Time{}) // 取号失败的历史调用形态：零时刻
		select {
		case <-deadline:
			t.Fatal("self-fence never triggered: zero-time failure exempted from accounting")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
