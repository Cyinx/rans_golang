package testkit

import (
	"fmt"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// 每 Pawnite 定时器验收（票 02）：回调 ctx 与投递 Handle ctx 同族——身份/纪元/
// 调用面可用、可让出做 IO、无延迟应答面；让出期间收件箱照常服务。

func TestAddTimer_CallbackCanYield(t *testing.T) {
	report := make(chan string, 8)
	clock := NewVirtualClock()
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(clock))
	id, _ := rans.NewID("timerprobe", "y1")
	callEntity(t, c, id, "timer-yield")
	clock.Advance(10 * time.Millisecond)
	wantReportSet(t, collectReports(t, report, 3), "work-ran", "callback-returned", "then-ran")
}

func TestAddTimer_YieldKeepsMailboxServing(t *testing.T) {
	release := make(chan struct{})
	report := make(chan string, 8)
	clock := NewVirtualClock()
	c := NewCluster(t, 1, timerKindsRelease(report, release), WithTimerClock(clock))
	id, _ := rans.NewID("timerprobe", "y2")
	callEntity(t, c, id, "timer-yield-blocking")
	clock.Advance(10 * time.Millisecond)
	expectReport(t, report, "work-started")
	if v := callEntity(t, c, id, "ping"); v != "pong" {
		t.Fatalf("让出期间投递返回 %v, want pong", v)
	}
	close(release)
	expectReport(t, report, "then-ran")
}

func TestAddTimer_CallbackDeferPanics(t *testing.T) {
	report := make(chan string, 4)
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(NewVirtualClock()))
	id, _ := rans.NewID("timerprobe", "d1")
	callEntity(t, c, id, "timer-defer")
	expectReport(t, report, "defer-panicked")
}

func TestAddTimer_CallbackCarriesIncarnation(t *testing.T) {
	report := make(chan string, 4)
	clock := NewVirtualClock()
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(clock))
	id, _ := rans.NewID("timerprobe", "i1")
	callEntity(t, c, id, "arm-inc")
	handle := <-report
	clock.Advance(10 * time.Millisecond)
	callback := <-report
	var hi, ci uint64
	var hok, cok bool
	if _, err := fmt.Sscanf(handle, "handle inc=%d ok=%t", &hi, &hok); err != nil {
		t.Fatalf("解析 Handle 报告 %q: %v", handle, err)
	}
	if _, err := fmt.Sscanf(callback, "callback inc=%d ok=%t", &ci, &cok); err != nil {
		t.Fatalf("解析回调报告 %q: %v", callback, err)
	}
	if !hok || !cok || hi != ci {
		t.Fatalf("纪元不一致：handle=%d(%v) callback=%d(%v)", hi, hok, ci, cok)
	}
}

func TestAddTimer_TwoCallbacksYieldIndependently(t *testing.T) {
	release := make(chan struct{})
	report := make(chan string, 8)
	clock := NewVirtualClock()
	c := NewCluster(t, 1, timerKindsRelease(report, release), WithTimerClock(clock))
	id, _ := rans.NewID("timerprobe", "y3")
	callEntity(t, c, id, "timer-yield-pair")
	clock.Advance(10 * time.Millisecond)
	wantReportSet(t, collectReports(t, report, 2), "pair-a-started", "pair-b-started")
	close(release)
	wantReportSet(t, collectReports(t, report, 2), "pair-a-then", "pair-b-then")
}
