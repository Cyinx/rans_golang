package testkit

// 每 Pawnite 定时器验收（票 01）：一次性语义、移除与 ID 契约、武装收敛点、
// 回调重排；时间由虚拟时钟推进驱动。

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// TestAddTimer_FiresOnLoopWithRunCtx 到点回调在实体 loop 上执行一次，回调 ctx
// 携带实体身份与调用面；时钟未推进前不得触发。
func TestAddTimer_FiresOnLoopWithRunCtx(t *testing.T) {
	report := make(chan string, 4)
	clock := NewVirtualClock()
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(clock))
	id, _ := rans.NewID("timerprobe", "t1")
	armTimer(t, c, id, "t1", 25*time.Millisecond)
	expectSilent(t, report, "时钟推进前")
	clock.Advance(25 * time.Millisecond)
	expectReport(t, report, "t1 self=timerprobe/t1 ok=true client=true")
}

// TestAddTimer_ZeroDelayRunsNextLoopRound d=0 表示下一轮 loop 执行：不推进
// 虚拟时钟也必须触发（「立即落库」用它替代自投消息）。
func TestAddTimer_ZeroDelayRunsNextLoopRound(t *testing.T) {
	report := make(chan string, 4)
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(NewVirtualClock()))
	id, _ := rans.NewID("timerprobe", "t0")
	armTimer(t, c, id, "now", 0)
	expectReport(t, report, "now self=timerprobe/t0 ok=true client=true")
}

// TestAddTimer_EarlierItemFiresOnTime 先武装 1 小时项，再经下一次投递加一个
// 20 毫秒项：被武装的底层定时器必须重排到更早项（漏重排会把 20 毫秒的触发
// 拖到 1 小时之后）。
func TestAddTimer_EarlierItemFiresOnTime(t *testing.T) {
	report := make(chan string, 4)
	clock := NewVirtualClock()
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(clock))
	id, _ := rans.NewID("timerprobe", "t2")
	armTimer(t, c, id, "late", time.Hour)
	armTimer(t, c, id, "early", 20*time.Millisecond)
	clock.Advance(20 * time.Millisecond)
	expectReport(t, report, "early self=timerprobe/t2 ok=true client=true")
	expectSilent(t, report, "只有早项到期时")
	clock.Advance(time.Hour)
	expectReport(t, report, "late self=timerprobe/t2 ok=true client=true")
}

// TestRemoveTimer_CancelsAndIsIdempotent 移除后不再触发；重复移除返回 false
// （清理路径可无条件调用）。
func TestRemoveTimer_CancelsAndIsIdempotent(t *testing.T) {
	report := make(chan string, 4)
	clock := NewVirtualClock()
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(clock))
	id, _ := rans.NewID("timerprobe", "t3")
	tid := armTimer(t, c, id, "gone", time.Hour)
	if ok := removeTimer(t, c, id, tid); !ok {
		t.Fatal("首次移除应返回 true")
	}
	if ok := removeTimer(t, c, id, tid); ok {
		t.Fatal("重复移除应返回 false")
	}
	clock.Advance(time.Hour)
	expectSilent(t, report, "移除后推进到原到期点")
}

// TestRemoveTimer_AfterFireReturnsFalse 触发即摘表：回调执行后的移除返回
// false（一次性语义，ID 已失效）。
func TestRemoveTimer_AfterFireReturnsFalse(t *testing.T) {
	report := make(chan string, 4)
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(NewVirtualClock()))
	id, _ := rans.NewID("timerprobe", "t4")
	tid := armTimer(t, c, id, "once", 0)
	expectReport(t, report, "once self=timerprobe/t4 ok=true client=true")
	if ok := removeTimer(t, c, id, tid); ok {
		t.Fatal("已触发定时器的移除应返回 false")
	}
}

// TestRemoveTimer_ForeignInstanceReturnsFalse 别的实体拿同一 ID 移除得到
// false（ID 表按实例隔离），且不影响原实体的定时器。
func TestRemoveTimer_ForeignInstanceReturnsFalse(t *testing.T) {
	report := make(chan string, 4)
	clock := NewVirtualClock()
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(clock))
	owner, _ := rans.NewID("timerprobe", "owner")
	other, _ := rans.NewID("timerprobe", "other")
	tid := armTimer(t, c, owner, "mine", 10*time.Millisecond)
	if _, err := c.Node(0).Call(context.Background(), other, "arm:probe:"+time.Hour.String()); err != nil {
		t.Fatalf("arm other: %v", err)
	}
	if ok := removeTimer(t, c, other, tid); ok {
		t.Fatal("跨实体移除应返回 false")
	}
	clock.Advance(10 * time.Millisecond)
	expectReport(t, report, "mine self=timerprobe/owner ok=true client=true")
}

// TestAddTimer_IDsAreMonotonic 同一实体上多次预约的句柄严格递增且非零。
func TestAddTimer_IDsAreMonotonic(t *testing.T) {
	report := make(chan string, 4)
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(NewVirtualClock()))
	id, _ := rans.NewID("timerprobe", "t5")
	first := armTimer(t, c, id, "a", time.Hour)
	second := armTimer(t, c, id, "b", time.Hour)
	third := armTimer(t, c, id, "c", time.Hour)
	if !(first < second && second < third) {
		t.Fatalf("句柄未单调递增：%d, %d, %d", first, second, third)
	}
}

// TestRemoveTimer_StaleIDDoesNotCancelNewTimer 陈旧 ID 只对旧定时器有效：移除
// 旧 ID 后再预约的新定时器不受影响（不复用 ID 的直接后果）。
func TestRemoveTimer_StaleIDDoesNotCancelNewTimer(t *testing.T) {
	report := make(chan string, 4)
	clock := NewVirtualClock()
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(clock))
	id, _ := rans.NewID("timerprobe", "t6")
	stale := armTimer(t, c, id, "old", time.Hour)
	if ok := removeTimer(t, c, id, stale); !ok {
		t.Fatal("移除旧定时器应返回 true")
	}
	fresh := armTimer(t, c, id, "new", time.Hour)
	if fresh == stale {
		t.Fatalf("句柄被复用：%d", stale)
	}
	if ok := removeTimer(t, c, id, stale); ok {
		t.Fatal("陈旧 ID 不得再次命中")
	}
	clock.Advance(time.Hour)
	expectReport(t, report, "new self=timerprobe/t6 ok=true client=true")
	expectSilent(t, report, "只剩新定时器")
}

// probeTimerCall 执行一次定时器调用并报告是否 panic。

// removeTimer 经一次投递移除句柄并取回结果。
// TestAddTimer_PeriodicByRearm 周期用法 = 回调内重排自己：按间隔连续触发，
// 每次拿新句柄；最后一次不再重排。
func TestAddTimer_PeriodicByRearm(t *testing.T) {
	report := make(chan string, 8)
	clock := NewVirtualClock()
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(clock))
	id, _ := rans.NewID("timerprobe", "p1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if v, err := c.Node(0).Call(ctx, id, "period:p1:10ms:3"); err != nil || v != "periodic" {
		t.Fatalf("period: v=%v err=%v", v, err)
	}
	for want := 2; want >= 0; want-- {
		clock.Advance(10 * time.Millisecond)
		expectReport(t, report, fmt.Sprintf("p1 tick left=%d", want))
	}
	clock.Advance(10 * time.Millisecond)
	expectSilent(t, report, "重排次数用尽后")
}

// TestAddTimer_WorkCtxPanics loop 外的 work ctx 不带定时器面：AddTimer 与
// RemoveTimer 都必须 panic（否则 loop 睡在 select 上会丢唤醒）。
func TestAddTimer_WorkCtxPanics(t *testing.T) {
	report := make(chan string, 4)
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(NewVirtualClock()))
	id, _ := rans.NewID("timerprobe", "w1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if v, err := c.Node(0).Call(ctx, id, "yield-armtimer"); err != nil || v != "yielded" {
		t.Fatalf("yield: v=%v err=%v", v, err)
	}
	expectReport(t, report, "work-panicked")
	expectReport(t, report, "work-panicked")
}

// TestAddTimer_SameDeadlineFiresInHandleOrder 同一到期时刻的多个定时器按句柄
// （创建）序触发：堆比较键 (deadline, id) 的可观测后果。
func TestAddTimer_SameDeadlineFiresInHandleOrder(t *testing.T) {
	report := make(chan string, 4)
	clock := NewVirtualClock()
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(clock))
	id, _ := rans.NewID("timerprobe", "o1")
	armTimer(t, c, id, "first", 10*time.Millisecond)
	armTimer(t, c, id, "second", 10*time.Millisecond)
	clock.Advance(10 * time.Millisecond)
	expectReport(t, report, "first self=timerprobe/o1 ok=true client=true")
	expectReport(t, report, "second self=timerprobe/o1 ok=true client=true")
}

// TestAddTimer_DebounceRearmFiresOnce 防抖 = 移除 + 新增：窗口内多次重设只触发
// 最后一次，且只触发一次。
func TestAddTimer_DebounceRearmFiresOnce(t *testing.T) {
	report := make(chan string, 4)
	clock := NewVirtualClock()
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(clock))
	id, _ := rans.NewID("timerprobe", "d2")
	var last int64
	for i := 0; i < 3; i++ {
		if last != 0 && !removeTimer(t, c, id, last) {
			t.Fatalf("第 %d 次重设前移除失败", i)
		}
		last = armTimer(t, c, id, "debounced", 10*time.Millisecond)
		clock.Advance(5 * time.Millisecond) // 每次只走半个窗口，重设把窗口推后
	}
	expectSilent(t, report, "最后一次窗口未到点")
	clock.Advance(10 * time.Millisecond)
	expectReport(t, report, "debounced self=timerprobe/d2 ok=true client=true")
	clock.Advance(time.Hour)
	expectSilent(t, report, "防抖只触发一次")
}

// TestAddTimer_SilentAfterPassivation 钝化随实例清理定时器：钝化之后越过原到期点
// 也不再触发（定时器不保活、纯内存）。
func TestAddTimer_SilentAfterPassivation(t *testing.T) {
	report := make(chan string, 8)
	c := NewCluster(t, 1, timerKindsIdle(report), WithIdlePassivation("timerprobe", 30*time.Millisecond))
	id, _ := rans.NewID("timerprobe", "pv1")
	armTimer(t, c, id, "dropped", 200*time.Millisecond)
	expectReport(t, report, "deactivated")
	select {
	case s := <-report:
		t.Fatalf("钝化后仍收到报告 %q", s)
	case <-time.After(300 * time.Millisecond): // 越过原到期点（200ms）
	}
}
