package metrics_test

// metrics 包手动读数器与并发安全测试（规格 §16）：
// Counter 累计/幂等注册、Gauge 观察回调、Default 包级注册表、
// 无 SDK（全局 noop provider）静默路径、-race 并发。

import (
	"sync"
	"testing"

	"github.com/cyinx/rans_golang/internal/metrics"
)

// TestCounterAddAccumulates 计数器累计与幂等注册：同名二次注册返回同一
// 实例（描述以首次为准），Snapshot 反映累计值。
func TestCounterAddAccumulates(t *testing.T) {
	reg := metrics.New("test-meter")
	c := reg.Counter("test.events", "test counter")
	c.Add(1)
	c.Add(2.5)
	if got := c.Value(); got != 3.5 {
		t.Fatalf("counter value = %v, want 3.5", got)
	}
	if again := reg.Counter("test.events", "different desc"); again != c {
		t.Fatal("same-name Counter registration must be idempotent (same instance)")
	}
	snap := reg.Snapshot()
	if got := snap["test.events"]; got != 3.5 {
		t.Fatalf("snapshot[test.events] = %v, want 3.5", got)
	}
}

// TestGaugeObserveInSnapshot Gauge 采样：Snapshot 逐次调用观察闭包（即时
// 读数）；同名多观察者求和（多 Remote 共享 default 注册表的聚合语义）。
func TestGaugeObserveInSnapshot(t *testing.T) {
	reg := metrics.New("test-meter")
	x := 42.0
	reg.Gauge("test.gauge", "test gauge", func() float64 { return x })
	x = 7
	if got := reg.Snapshot()["test.gauge"]; got != 7 {
		t.Fatalf("snapshot gauge = %v, want 7 (observe must be invoked at read time)", got)
	}
	reg.Gauge("test.gauge", "test gauge", func() float64 { return 3 })
	if got := reg.Snapshot()["test.gauge"]; got != 10 {
		t.Fatalf("snapshot gauge with two observers = %v, want 10 (7+3 sum)", got)
	}
}

// TestDefaultRegistryAndPackageSnapshot Default 注册表稳定（幂等）+ 包级
// Snapshot 直读；缺省 meter 名 "rans"（规格 §16）。
func TestDefaultRegistryAndPackageSnapshot(t *testing.T) {
	if metrics.Default() != metrics.Default() {
		t.Fatal("Default() must return a stable registry")
	}
	c := metrics.Default().Counter("test.default.counter", "d")
	before := metrics.Snapshot()["test.default.counter"]
	c.Add(1)
	after := metrics.Snapshot()["test.default.counter"]
	if after-before != 1 {
		t.Fatalf("package Snapshot delta = %v, want 1 (before=%v after=%v)", after-before, before, after)
	}
	if metrics.Default().MeterName() != "rans" {
		t.Fatalf("default meter name = %q, want \"rans\" (spec §16)", metrics.Default().MeterName())
	}
	// New 空名回落缺省 "rans"。
	if got := metrics.New("").MeterName(); got != "rans" {
		t.Fatalf("New(\"\") meter name = %q, want \"rans\"", got)
	}
}

// TestNoSDKPathSilent 无 SDK（全局 noop meter provider）：仪表注册、Add、
// Gauge 采样全部静默成功（不 panic 不报错），手动读数器照常工作。
func TestNoSDKPathSilent(t *testing.T) {
	reg := metrics.New("no-sdk-meter")
	c := reg.Counter("nosdk.events", "d")
	c.Add(5)
	reg.Gauge("nosdk.gauge", "d", func() float64 { return 1 })
	snap := reg.Snapshot()
	if snap["nosdk.events"] != 5 || snap["nosdk.gauge"] != 1 {
		t.Fatalf("no-SDK snapshot = %v, want {nosdk.events:5 nosdk.gauge:1}", snap)
	}
}

// TestCounterConcurrentAddSnapshot -race 并发：多 goroutine Add 与并发
// Snapshot 互不撕裂，终值恰为总加量。
func TestCounterConcurrentAddSnapshot(t *testing.T) {
	reg := metrics.New("race-meter")
	c := reg.Counter("race.events", "d")
	reg.Gauge("race.gauge", "d", func() float64 { return 1 })

	var addWG sync.WaitGroup
	for i := 0; i < 8; i++ {
		addWG.Add(1)
		go func() {
			defer addWG.Done()
			for j := 0; j < 1000; j++ {
				c.Add(1)
			}
		}()
	}
	// 并发快照读：加量全部完成后经 stop 收口（独立收尾，不与加量 Wait 互等）。
	stop := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stop:
				return
			default:
				_ = reg.Snapshot()
			}
		}
	}()
	addWG.Wait()
	close(stop)
	<-readerDone
	if got := c.Value(); got != 8000 {
		t.Fatalf("concurrent counter value = %v, want 8000", got)
	}
}
