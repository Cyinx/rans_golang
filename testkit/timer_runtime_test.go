package testkit

// 每 Pawnite 定时器验收（票 04）：回调的 loop 执行段计入执行看门狗、panic 按
// 策略收场、触发不刷新闲置时钟（周期 tick 不保活实体）。

import (
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// TestAddTimer_CallbackPanicKeepsServing 回调 panic（缺省 Default 策略）：loop
// 存活、实体继续服役（修复前 panic 打死 loop goroutine，进程随之退出）。
func TestAddTimer_CallbackPanicKeepsServing(t *testing.T) {
	report := make(chan string, 8)
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(NewVirtualClock()))
	id, _ := rans.NewID("timerprobe", "pan1")
	callEntity(t, c, id, "timer-panic")
	expectReport(t, report, "timer-panicking")
	if v := callEntity(t, c, id, "ping"); v != "pong" {
		t.Fatalf("panic 后投递返回 %v, want pong", v)
	}
	if v := callEntity(t, c, id, "ping"); v != "pong" {
		t.Fatalf("panic 后第二次投递返回 %v, want pong", v)
	}
}

// TestAddTimer_CallbackCountsIntoExecutionWatchdog 卡死回调计入执行看门狗：
// 强弃后实体被逐出本地激活表，后续投递落到新激活的实例。
func TestAddTimer_CallbackCountsIntoExecutionWatchdog(t *testing.T) {
	release := make(chan struct{})
	report := make(chan string, 8)
	c := NewCluster(t, 1, timerKindsRelease(report, release),
		WithTimerClock(NewVirtualClock()),
		WithWatchdog(20*time.Millisecond, 50*time.Millisecond))
	t.Cleanup(func() { close(release) }) // 放行 zombie 回调，避免 goroutine 泄漏
	id, _ := rans.NewID("timerprobe", "stuck1")
	callEntity(t, c, id, "timer-stuck")
	deadline := time.Now().Add(5 * time.Second)
	for c.Node(0).Holds(id) {
		if time.Now().After(deadline) {
			t.Fatal("卡死回调未被执行看门狗强弃（实体仍在本地激活表）")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if v := callEntity(t, c, id, "ping"); v != "pong" {
		t.Fatalf("强弃后投递返回 %v, want pong", v)
	}
}

// TestAddTimer_DoesNotRefreshIdleClock 周期定时器不保活实体：闲置窗口到点后
// 仍被钝化（触发不刷新闲置时钟）。
func TestAddTimer_DoesNotRefreshIdleClock(t *testing.T) {
	report := make(chan string, 8)
	c := NewCluster(t, 1, timerKindsIdle(report), WithIdlePassivation("timerprobe", 30*time.Millisecond))
	id, _ := rans.NewID("timerprobe", "idle1")
	callEntity(t, c, id, "timer-forever:5ms")
	expectReport(t, report, "timer-fired")
	expectReport(t, report, "deactivated")
}
