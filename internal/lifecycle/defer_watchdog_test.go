package lifecycle

// 看门狗 × 延迟应答的交叉锚（H12 决策 16/17，HR4 票 11）：Defer 返回
//（Handle 返回 Deferred）后该请求不再计入执行看门狗的 executingSince——
// 挂起远超执行阈值的延迟应答不被强弃，后续 Reply 正常收场（延迟应答不算
// 卡死；收端不自建到期定时）。

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/activation"
)

// deferWatchEntity 延迟应答道具实体："defer" 登记 Responder 存实体状态并
// 投票；"reply" 由后续消息的 Handle 收场（聚合应答形态）。
type deferWatchEntity struct {
	mu      sync.Mutex
	resp    *rans.Responder
	stashed chan struct{}
}

func (e *deferWatchEntity) Activate(context.Context) error   { return nil }
func (e *deferWatchEntity) Deactivate(context.Context) error { return nil }

func (e *deferWatchEntity) Handle(ctx context.Context, req any) (any, error) {
	switch req {
	case "defer":
		e.mu.Lock()
		e.resp = rans.Defer(ctx)
		e.mu.Unlock()
		select {
		case e.stashed <- struct{}{}:
		default:
		}
		return rans.Deferred, nil
	case "reply":
		e.mu.Lock()
		r := e.resp
		e.mu.Unlock()
		r.Reply(42, nil)
		return "replied", nil
	}
	return "ok", nil
}

// TestDefer_WatchdogNotCounting Defer 后的挂起不计入执行看门狗：短执行阈
// 值下延迟应答挂起时长远超阈值——实体不被强弃（在册、非 Destroying），
// 后续消息的 Handle 里 Reply 正常收场调用 1。若 Defer 返回后仍计入
// executingSince，看门狗会先强弃逐出（调用 1 收 Timeout）——本测试红。
func TestDefer_WatchdogNotCounting(t *testing.T) {
	dir := newFakeTransferDir()
	ent := &deferWatchEntity{stashed: make(chan struct{}, 1)}
	m := activation.New(dir, 1, map[string]rans.Factory{
		"defw": func(_ rans.ID) rans.Entity { return ent },
	})
	id := mustTestID(t, "defw", "dw1")

	type outcome struct {
		v   any
		err error
	}
	out := make(chan outcome, 1)
	go func() {
		v, err := m.InvokeLocal(context.Background(), id, "defer")
		out <- outcome{v, err}
	}()
	select {
	case <-ent.stashed:
	case <-time.After(2 * time.Second):
		t.Fatal("message 1 Handle never deferred")
	}

	wd := NewWatchdog(m, WatchdogConfig{
		Interval:          10 * time.Millisecond,
		ExecTimeout:       50 * time.Millisecond,
		UnloadTimeout:     time.Hour,
		ActivationTimeout: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); wd.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(2 * time.Second):
			t.Error("watchdog Run leaked past ctx cancel")
		}
	}()

	// 挂起 300ms >> 50ms 阈值：多轮扫描后实体必须在册（未被强弃逐出）。
	time.Sleep(300 * time.Millisecond)
	if _, ok := m.GetLocal(id); !ok {
		t.Fatal("deferred request must not be watchdog-abandoned (not counted as stuck execution)")
	}

	// 消息 2 的 Handle 里 Reply：调用 1 正常收到 42（挂起未被强弃打断）。
	go func() {
		_, _ = m.InvokeLocal(context.Background(), id, "reply")
	}()
	select {
	case o := <-out:
		if o.err != nil {
			t.Fatalf("deferred call err = %v, want nil", o.err)
		}
		if o.v != 42 {
			t.Fatalf("deferred call value = %v, want 42", o.v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deferred caller never received the later reply")
	}
}
