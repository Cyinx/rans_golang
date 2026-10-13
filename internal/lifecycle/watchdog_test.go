package lifecycle

// 看门狗测试（规格 §14.2 三看门狗）：纯回调注入面，钉死：
//   - 扫描分派：Destroying 停驻超时 → AbandonDeactivation；Activating 停驻
//     超时 → AbandonActivation（第三形态）；Active 执行停驻超时
//     → AbandonExecution；新鲜/无关实体不动；
//   - KindOverrides per-kind 参数（覆盖生效、零值回落全局）；
//   - Run 周期（Interval 节流）与 ctx 退出。

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// wdFake: 计数型强弃回调面 + 可编排实体快照表。
type wdFake struct {
	mu         sync.Mutex
	entities   []WatchdogEntity
	deactCalls map[string]int // String() → 次数
	actCalls   map[string]int
	execCalls  map[string]int
	scans      atomic.Int64 // Entities 被调用次数（= 扫描轮次）
}

func newWdFake(entities ...WatchdogEntity) *wdFake {
	return &wdFake{
		entities:   entities,
		deactCalls: map[string]int{},
		actCalls:   map[string]int{},
		execCalls:  map[string]int{},
	}
}

func (f *wdFake) Entities() []WatchdogEntity {
	f.scans.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]WatchdogEntity, len(f.entities))
	copy(out, f.entities)
	return out
}

func (f *wdFake) AbandonDeactivation(id rans.ID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deactCalls[id.String()]++
	return true
}

func (f *wdFake) AbandonActivation(id rans.ID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.actCalls[id.String()]++
	return true
}

func (f *wdFake) AbandonExecution(id rans.ID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execCalls[id.String()]++
	return true
}

func mustWdID(t *testing.T, kind, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID(kind, inst)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestWatchdogScan_DispatchesByState 分派正确性：卡死 Destroying → 卸载强弃；
// Active 执行超时 → 执行强弃；两者都不超时/非目标状态 → 不动。
func TestWatchdogScan_DispatchesByState(t *testing.T) {
	staleDeact := mustWdID(t, "player", "wd1")
	staleAct := mustWdID(t, "player", "wd2")
	staleExec := mustWdID(t, "player", "wd5")
	fresh := mustWdID(t, "player", "wd3")
	freshAct := mustWdID(t, "player", "wd6")
	other := mustWdID(t, "player", "wd4")
	old := time.Now().Add(-2 * time.Hour)
	f := newWdFake(
		WatchdogEntity{ID: staleDeact, Kind: "player", Destroying: true, DestroyingSince: old},
		WatchdogEntity{ID: staleAct, Kind: "player", Activating: true, ActivatingSince: old},
		WatchdogEntity{ID: staleExec, Kind: "player", Active: true, ExecutingSince: old},
		WatchdogEntity{ID: fresh, Kind: "player", Active: true, ExecutingSince: time.Now()},
		WatchdogEntity{ID: freshAct, Kind: "player", Activating: true, ActivatingSince: time.Now()},
		WatchdogEntity{ID: other, Kind: "player"}, // Created 等无关状态
	)
	w := &Watchdog{
		Entities:            f.Entities,
		AbandonDeactivation: f.AbandonDeactivation,
		AbandonActivation:   f.AbandonActivation,
		AbandonExecution:    f.AbandonExecution,
	}
	w.Scan()
	if f.deactCalls[staleDeact.String()] != 1 {
		t.Fatalf("stale Destroying must trigger deactivation abandon once, got %d", f.deactCalls[staleDeact.String()])
	}
	if f.actCalls[staleAct.String()] != 1 {
		t.Fatalf("stale Activating must trigger activation abandon once, got %d", f.actCalls[staleAct.String()])
	}
	if f.execCalls[staleExec.String()] != 1 {
		t.Fatalf("stale executing must trigger execution abandon once, got %d", f.execCalls[staleExec.String()])
	}
	if f.deactCalls[fresh.String()] != 0 || f.execCalls[fresh.String()] != 0 {
		t.Fatal("fresh entity must not be abandoned")
	}
	if f.actCalls[freshAct.String()] != 0 {
		t.Fatal("fresh Activating entity must not be abandoned")
	}
	if f.deactCalls[other.String()] != 0 || f.execCalls[other.String()] != 0 || f.actCalls[other.String()] != 0 {
		t.Fatal("non-target-state entity must not be abandoned")
	}
}

// TestWatchdogScan_ZeroSinceFieldsSkipped 防御性跳过：时钟零值（未知停驻
// 起点）不构成强弃依据。
func TestWatchdogScan_ZeroSinceFieldsSkipped(t *testing.T) {
	id := mustWdID(t, "player", "wd5")
	f := newWdFake(
		WatchdogEntity{ID: id, Kind: "player", Destroying: true}, // DestroyingSince 零
		WatchdogEntity{ID: id, Kind: "player", Activating: true}, // ActivatingSince 零
		WatchdogEntity{ID: id, Kind: "player", Active: true},     // ExecutingSince 零
	)
	w := &Watchdog{Entities: f.Entities, AbandonDeactivation: f.AbandonDeactivation, AbandonActivation: f.AbandonActivation, AbandonExecution: f.AbandonExecution}
	w.Scan()
	if f.deactCalls[id.String()] != 0 || f.actCalls[id.String()] != 0 || f.execCalls[id.String()] != 0 {
		t.Fatal("zero since-clock must be skipped, not abandoned")
	}
}

// TestWatchdogScan_KindOverrides per-kind 参数：覆盖值放宽/收紧只影响对应
// kind；零值覆盖回落全局。
func TestWatchdogScan_KindOverrides(t *testing.T) {
	llm := mustWdID(t, "llm", "wd6")
	plain := mustWdID(t, "player", "wd7")
	old := time.Now().Add(-10 * time.Second)
	f := newWdFake(
		WatchdogEntity{ID: llm, Kind: "llm", Active: true, ExecutingSince: old},
		WatchdogEntity{ID: llm, Kind: "llm", Activating: true, ActivatingSince: old},
		WatchdogEntity{ID: plain, Kind: "player", Active: true, ExecutingSince: old},
		WatchdogEntity{ID: plain, Kind: "player", Activating: true, ActivatingSince: old},
	)
	w := &Watchdog{
		Entities:          f.Entities,
		AbandonExecution:  f.AbandonExecution,
		AbandonActivation: f.AbandonActivation,
		Config: WatchdogConfig{
			ExecTimeout:       time.Hour, // 全局极长：默认无人触发
			ActivationTimeout: time.Hour,
			KindOverrides: map[string]WatchdogParams{
				"llm":    {ExecTimeout: time.Second, ActivationTimeout: time.Second}, // llm 放宽到秒级即触发
				"player": {ExecTimeout: 0, ActivationTimeout: 0},                     // 零值覆盖必须回落全局（不触发）
			},
		},
	}
	w.Scan()
	if f.execCalls[llm.String()] != 1 {
		t.Fatalf("llm override must trigger, got %d", f.execCalls[llm.String()])
	}
	if f.actCalls[llm.String()] != 1 {
		t.Fatalf("llm activation override must trigger, got %d", f.actCalls[llm.String()])
	}
	if f.execCalls[plain.String()] != 0 {
		t.Fatalf("zero override must fall back to global, got %d", f.execCalls[plain.String()])
	}
	if f.actCalls[plain.String()] != 0 {
		t.Fatalf("zero activation override must fall back to global, got %d", f.actCalls[plain.String()])
	}
}

// TestWatchdogScan_ZeroGlobalFallsBackDefaults 全局零值回落附录 A 缺省
// （60s/120s）：1 小时停驻触发，10 秒停驻不触发。
func TestWatchdogScan_ZeroGlobalFallsBackDefaults(t *testing.T) {
	deact := mustWdID(t, "player", "wd8")
	act := mustWdID(t, "player", "wd11")
	exec := mustWdID(t, "player", "wd9")
	f := newWdFake(
		WatchdogEntity{ID: deact, Kind: "player", Destroying: true, DestroyingSince: time.Now().Add(-time.Hour)},
		WatchdogEntity{ID: act, Kind: "player", Activating: true, ActivatingSince: time.Now().Add(-10 * time.Second)},
		WatchdogEntity{ID: exec, Kind: "player", Active: true, ExecutingSince: time.Now().Add(-10 * time.Second)},
	)
	w := &Watchdog{Entities: f.Entities, AbandonDeactivation: f.AbandonDeactivation, AbandonActivation: f.AbandonActivation, AbandonExecution: f.AbandonExecution}
	w.Scan()
	if f.deactCalls[deact.String()] != 1 {
		t.Fatal("1h-stuck Destroying must trigger with default 60s threshold")
	}
	if f.actCalls[act.String()] != 0 {
		t.Fatal("10s-stuck activation must not trigger with default 60s threshold")
	}
	if f.execCalls[exec.String()] != 0 {
		t.Fatal("10s-stuck execution must not trigger with default 120s threshold")
	}
}

// TestWatchdogRun_IntervalRespected 周期节流：Interval 内至多一轮扫描，
// 长窗口内扫描轮数落在 [elapsed/interval - 1, elapsed/interval + 1]。
func TestWatchdogRun_IntervalRespected(t *testing.T) {
	id := mustWdID(t, "player", "wd10")
	f := newWdFake(WatchdogEntity{ID: id, Kind: "player"})
	w := &Watchdog{Entities: f.Entities, Config: WatchdogConfig{Interval: 80 * time.Millisecond}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	time.Sleep(400 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after ctx cancel")
	}
	scans := f.scans.Load()
	// 400ms / 80ms ≈ 5 轮；容差 [2, 8] 吸收调度抖动，同时钉死节流存在
	//（无节流将远超上界）。
	if scans < 2 || scans > 8 {
		t.Fatalf("scans in 400ms with 80ms interval = %d, want [2, 8]", scans)
	}
}

// TestWatchdogRun_CtxStopExitsGoroutine ctx 取消必须即时收掉扫描 goroutine。
func TestWatchdogRun_CtxStopExitsGoroutine(t *testing.T) {
	f := newWdFake()
	w := &Watchdog{Entities: f.Entities, Config: WatchdogConfig{Interval: time.Hour}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run goroutine leaked past ctx cancel")
	}
}
