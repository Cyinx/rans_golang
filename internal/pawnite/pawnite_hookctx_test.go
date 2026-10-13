package pawnite

// 强弃取消面测试：看门狗强弃置终态时，在飞的宿主生命周期钩子
// （Deactivate/Activate）拿到的 ctx 必须被取消——协作宿主得以尽早返回，
// 「强弃后新 incarnation 与 zombie 并发」的窗口从无界收敛到宿主配合度界。
// 反向契约（优雅路径）一并钉死：无强弃介入时钩子 ctx 全程不被取消。
// 观测面：强弃时钩子仍在飞 → log.Warn 留痕（SetLogger 捕获断言）；
// 钩子不在飞的强弃（执行卡死形态）不产生该留痕。

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/internal/log"
)

// TestAbandonCancelsStuckDeactivateHook 卸载卡死强弃的取消面：Deactivate
// 钩子卡死（销毁路径停在 finish 的 safeDeactivate 内）→ Abandon(StateDestroying)
// 置终态的同时，在飞钩子的 ctx 必须观察到取消。zombie 续体
// （取消触发的返回）不得二次收尾（OnEvict 恰一次）。
func TestAbandonCancelsStuckDeactivateHook(t *testing.T) {
	id := mustUUID(t, "player", "hc1")
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	var evicts atomic.Int32
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		Deactivate: func(ctx context.Context, p *Pawnite) error {
			close(entered)
			select {
			case <-ctx.Done():
				close(cancelled)
				return ctx.Err()
			case <-release:
				return nil
			}
		},
		OnEvict: func(p *Pawnite) { evicts.Add(1) },
	})
	p.Start(context.Background())
	waitForState(t, p, StateActive)

	go p.Destroy(context.Background())
	<-entered // Deactivate 已在飞（loop 停在 finish 内）

	if !p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("Abandon must win on a stuck Destroying pawnite")
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("stuck Deactivate hook must observe ctx cancellation after abandon")
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done must be closed by abandon")
	}
	if got := evicts.Load(); got != 1 {
		t.Fatalf("OnEvict count = %d, want 1 (zombie continuation must not re-run it)", got)
	}
}

// TestAbandonCancelsStuckActivateHook 激活卡死强弃的取消面：Activate 钩子
// 卡死 → Abandon(StateActivating) 置终态的同时，在飞钩子的 ctx 必须观察到取消。
func TestAbandonCancelsStuckActivateHook(t *testing.T) {
	id := mustUUID(t, "player", "hc2")
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error {
			close(entered)
			select {
			case <-ctx.Done():
				close(cancelled)
				return ctx.Err()
			case <-release:
				return nil
			}
		},
	})
	p.Start(context.Background())
	<-entered // Activate 已在飞

	if !p.Abandon(StateActivating, TerminalAbandoned) {
		t.Fatal("Abandon must win on a stuck Activating pawnite")
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("stuck Activate hook must observe ctx cancellation after abandon")
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done must be closed by abandon")
	}
}

// TestGracefulLifecycleNeverCancelsHookCtx 优雅路径契约（反向钉死）：无强弃
// 介入时（Start → 服役 → Destroy），Activate/Deactivate 钩子在整个执行期间
// 观测到的 ctx 必须未被取消（进/出各记一次，停驻窗放大取消竞态可观测面）。
func TestGracefulLifecycleNeverCancelsHookCtx(t *testing.T) {
	id := mustUUID(t, "player", "hc3")
	var mu sync.Mutex
	var obs []error
	hook := func(ctx context.Context, p *Pawnite) error {
		entry := ctx.Err()
		time.Sleep(10 * time.Millisecond) // 放大窗口：停驻期间任何取消都会被出口复检捕获
		mu.Lock()
		obs = append(obs, entry, ctx.Err())
		mu.Unlock()
		return nil
	}
	p := New(id, Options{Activate: hook, Deactivate: hook})
	p.Start(context.Background())
	waitForState(t, p, StateActive)
	p.Destroy(context.Background())
	<-p.Done()

	mu.Lock()
	defer mu.Unlock()
	if len(obs) != 4 {
		t.Fatalf("hook observations = %d, want 4 (activate/deactivate 进出各一)", len(obs))
	}
	for i, err := range obs {
		if err != nil {
			t.Fatalf("graceful lifecycle must never cancel hook ctx: observation %d = %v", i, err)
		}
	}
}

// captureHandler 收集全部日志记录（线程安全），供留痕断言。
type captureHandler struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.recs = append(h.recs, r.Clone())
	h.mu.Unlock()
	return nil
}
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func (h *captureHandler) countWithSubstring(sub string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.recs {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, sub) {
			n++
		}
	}
	return n
}

// TestAbandonLogsInFlightHook 观测面：强弃时生命周期钩子仍在飞 → Warn
// 留痕（消息含 "lifecycle hook in flight"，携带 uuid）。zombie 侧效应仅靠
// 取消收敛，运维需要窗口打开的可见信号。
func TestAbandonLogsInFlightHook(t *testing.T) {
	h := &captureHandler{}
	log.SetLogger(slog.New(h))
	defer log.SetLogger(nil)

	id := mustUUID(t, "player", "hc4")
	entered := make(chan struct{})
	release := make(chan struct{})
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		Deactivate: func(ctx context.Context, p *Pawnite) error {
			close(entered)
			<-release
			return nil
		},
	})
	p.Start(context.Background())
	waitForState(t, p, StateActive)
	go p.Destroy(context.Background())
	<-entered

	if !p.Abandon(StateDestroying, TerminalAbandoned) {
		t.Fatal("abandon must win")
	}
	if n := h.countWithSubstring("lifecycle hook in flight"); n != 1 {
		t.Fatalf("in-flight hook warn records = %d, want 1", n)
	}
	close(release) // 收尾 zombie
	time.Sleep(50 * time.Millisecond)
}

// TestAbandonQuietWhenNoHookInFlight 反向观测面：执行卡死强杀（无生命周期
// 钩子在飞——invoke 的 ctx 归调用方，manager.Call 已有逐调用 deadline）不
// 产生在飞留痕。
func TestAbandonQuietWhenNoHookInFlight(t *testing.T) {
	h := &captureHandler{}
	log.SetLogger(slog.New(h))
	defer log.SetLogger(nil)

	id := mustUUID(t, "player", "hc5")
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
	})
	p.Start(context.Background())
	waitForState(t, p, StateActive)

	exec := &enterBlockDelivery{id: id, entered: entered, release: release}
	if !p.ReceiveRequest(exec) {
		t.Fatal("admission must succeed")
	}
	<-entered
	if !p.Abandon(StateActive, TerminalAbandoned) {
		t.Fatal("force abandon must win")
	}
	if n := h.countWithSubstring("lifecycle hook in flight"); n != 0 {
		t.Fatalf("in-flight hook warn records = %d, want 0 (no lifecycle hook runs during exec abandon)", n)
	}
	close(release)
	time.Sleep(50 * time.Millisecond)
}
