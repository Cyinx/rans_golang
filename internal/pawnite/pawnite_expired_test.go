package pawnite

// 出队过期即弃（H10 决策 7）检查点单测：共用投递执行入口 invoke 对实现
// Expirable 的投递做预算复查——已过期 → 不执行（Deliver 不达）、按
// (CodeTimeout, ReasonExpired) 回绝、OnExpired 钩子触发（计数锚点，nil
// 安全）；未过期或无 deadline 照常执行。

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// expirableDelivery 可过期投递替身：deadline 可设，Deliver/Reject 收场即
// 关 settled（测试等待口，免轮询）。
type expirableDelivery struct {
	id        rans.ID
	dl        time.Time
	hasDL     bool
	delivered bool
	rejected  bool
	code      rans.Code
	reason    rans.Reason

	settled chan struct{}
	once    sync.Once
}

func newExpirableDelivery(id rans.ID, dl time.Time, hasDL bool) *expirableDelivery {
	return &expirableDelivery{id: id, dl: dl, hasDL: hasDL, settled: make(chan struct{})}
}

func (d *expirableDelivery) UUID() rans.ID                       { return d.id }
func (d *expirableDelivery) DeliveryDeadline() (time.Time, bool) { return d.dl, d.hasDL }
func (d *expirableDelivery) Deliver(*Pawnite) {
	d.delivered = true
	d.once.Do(func() { close(d.settled) })
}
func (d *expirableDelivery) Reject(st rans.Code, reason rans.Reason) {
	d.rejected, d.code, d.reason = true, st, reason
	d.once.Do(func() { close(d.settled) })
}

func TestInvoke_ExpiredOnDequeue(t *testing.T) {
	t.Run("expired_not_executed_rejected_timeout_expired", func(t *testing.T) {
		id := fenceID(t, "expired")
		hookFired := 0
		var mu sync.Mutex
		p := New(id, Options{
			Activate:  func(context.Context, *Pawnite) error { return nil },
			OnExpired: func() { mu.Lock(); hookFired++; mu.Unlock() },
		})
		p.Start(context.Background())
		defer func() { _ = p.Destroy(context.Background()) }()

		d := newExpirableDelivery(id, time.Now().Add(-time.Second), true)
		if !p.ReceiveRequest(d) {
			t.Fatal("delivery must be admitted")
		}
		select {
		case <-d.settled:
		case <-time.After(5 * time.Second):
			t.Fatal("delivery never settled")
		}
		if d.delivered {
			t.Fatal("expired delivery must not execute (Deliver unreachable)")
		}
		if !d.rejected || d.code != rans.CodeTimeout || d.reason != rans.ReasonExpired {
			t.Fatalf("reject = %v/%v/%v, want CodeTimeout/ReasonExpired", d.rejected, d.code, d.reason)
		}
		mu.Lock()
		defer mu.Unlock()
		if hookFired != 1 {
			t.Fatalf("OnExpired fired %d times, want 1", hookFired)
		}
	})

	t.Run("live_budget_executes", func(t *testing.T) {
		id := fenceID(t, "live")
		hookFired := 0
		p := New(id, Options{
			Activate:  func(context.Context, *Pawnite) error { return nil },
			OnExpired: func() { hookFired++ },
		})
		p.Start(context.Background())
		defer func() { _ = p.Destroy(context.Background()) }()

		d := newExpirableDelivery(id, time.Now().Add(time.Hour), true)
		if !p.ReceiveRequest(d) {
			t.Fatal("delivery must be admitted")
		}
		<-d.settled
		if !d.delivered || d.rejected {
			t.Fatalf("live delivery must execute: delivered=%v rejected=%v", d.delivered, d.rejected)
		}
		if hookFired != 0 {
			t.Fatalf("OnExpired fired %d times, want 0", hookFired)
		}
	})

	t.Run("no_deadline_executes", func(t *testing.T) {
		id := fenceID(t, "nodl")
		p := New(id, Options{Activate: func(context.Context, *Pawnite) error { return nil }})
		p.Start(context.Background())
		defer func() { _ = p.Destroy(context.Background()) }()

		d := newExpirableDelivery(id, time.Time{}, false)
		if !p.ReceiveRequest(d) {
			t.Fatal("delivery must be admitted")
		}
		<-d.settled
		if !d.delivered || d.rejected {
			t.Fatalf("deadline-free delivery must execute: delivered=%v rejected=%v", d.delivered, d.rejected)
		}
	})

	t.Run("non_expirable_executes", func(t *testing.T) {
		id := fenceID(t, "plain")
		p := New(id, Options{Activate: func(context.Context, *Pawnite) error { return nil }})
		p.Start(context.Background())
		defer func() { _ = p.Destroy(context.Background()) }()

		d := &fenceDelivery{id: id}
		if !p.ReceiveRequest(d) {
			t.Fatal("delivery must be admitted")
		}
		deadline := time.Now().Add(5 * time.Second)
		for p.activityEpoch.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if d.rejected {
			t.Fatal("non-expirable delivery must not be rejected")
		}
	})
}
