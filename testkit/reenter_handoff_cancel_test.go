package testkit

// HR4 验收落点（H12 决策 17 后半 + 取消面扩展，票 12）：交接窗与在途 work、
// MsgCancel 取消面。交接窗（Retire）等待在途 work 完成、续体落地后才收尾
// （延迟应答在终局前送达）；等待期新邮件留箱按 H6 处置；窗预算取消 wctx
// （work 协作返回、core 观测取消预算判输回退 Active）。MsgCancel 定位到
// work 在飞 → cancel wctx；已 Deferred 未 Reply → 标记取消（Reply 丢弃）。
// 道具实体 reenterEntity 共享自 reenter_acceptance_test.go。

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// TestReenterAfter_HandoffWaitsWork 交接窗（Retire）与在途 work：
//   - waits-then-retires：work 在飞时触发 Retire → 交接窗等 work 完成、续体
//     回 loop 落地后才收尾（延迟应答在终局前送达）；等待期新邮件留箱按 H6
//     处置（终局按确定未执行回绝，绝不执行）。
//   - budget-cancels-work：窗预算取消（调用方放弃）→ cancel wctx → work 快速
//     返回、续体落地（then 收 ctx.Canceled）→ core 观测取消预算输 → 窗回退
//     Active，实体继续服役。
func TestReenterAfter_HandoffWaitsWork(t *testing.T) {
	t.Run("waits-then-retires", func(t *testing.T) {
		e := &reenterEntity{started: make(chan struct{}, 1), release: make(chan struct{})}
		c := NewCluster(t, 1, map[string]rans.Factory{
			"reent": func(rans.ID) rans.Entity { return e },
		})
		id := mustID(t, "reent", "hw1")
		node := c.Node(0)
		ctx := context.Background()

		type outcome struct {
			v   any
			err error
		}
		out := make(chan outcome, 1)
		go func() {
			v, err := node.Call(ctx, id, "yield")
			out <- outcome{v, err}
		}()
		<-e.started // work 阻塞在 release：在飞

		destroyErr := make(chan error, 1)
		go func() { destroyErr <- node.Destroy(ctx, id) }()
		time.Sleep(200 * time.Millisecond) // 交接窗必已开（work 阻塞 → core 无法跑）

		// 等待期新邮件：H6 留箱——终局按确定未执行回绝（不得执行）。
		pingErr := make(chan error, 1)
		go func() {
			_, err := node.Call(ctx, id, "ping")
			pingErr <- err
		}()
		time.Sleep(100 * time.Millisecond) // ping 已入箱

		close(e.release) // work 返回：then 回 loop → Reply → 静默 → core → 终局
		select {
		case o := <-out:
			if o.err != nil {
				t.Fatalf("deferred call must complete before retire, got %v", o.err)
			}
			if o.v != "wv" {
				t.Fatalf("deferred call value = %v, want wv", o.v)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("deferred caller never received the reply (handoff did not wait for work)")
		}
		select {
		case err := <-destroyErr:
			if err != nil {
				t.Fatalf("retire must succeed after work completes, got %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("destroy never completed")
		}
		select {
		case err := <-pingErr:
			if rans.CodeOf(err) != rans.CodeUnavailable || rans.ReasonOf(err) != rans.ReasonDeactivating {
				t.Fatalf("boxed ping = %v, want Unavailable+Deactivating", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("boxed ping never settled")
		}
		if node.Holds(id) {
			t.Fatal("entity must be evicted after retire")
		}
	})

	t.Run("budget-cancels-work", func(t *testing.T) {
		e := &reenterEntity{started: make(chan struct{}, 1), release: make(chan struct{})} // release 永不关
		c := NewCluster(t, 1, map[string]rans.Factory{
			"reent": func(rans.ID) rans.Entity { return e },
		})
		id := mustID(t, "reent", "hw2")
		node := c.Node(0)
		ctx := context.Background()

		callErr := make(chan error, 1)
		go func() {
			_, err := node.Call(ctx, id, "yield")
			callErr <- err
		}()
		<-e.started

		destroyCtx, cancel := context.WithCancel(context.Background())
		destroyErr := make(chan error, 1)
		go func() { destroyErr <- node.Destroy(destroyCtx, id) }()
		time.Sleep(200 * time.Millisecond) // 窗开、挂起等待在途 work

		cancel() // 窗预算取消：cancel wctx → work 快速返回 → 续体落地
		select {
		case err := <-destroyErr:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("destroy = %v, want context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("destroy caller never unblocked after budget cancel")
		}
		select {
		case err := <-callErr:
			if err == nil || !strings.Contains(err.Error(), "context canceled") {
				t.Fatalf("deferred call = %v, want the canceled-work error", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("deferred caller never settled after work cancel")
		}
		// then 收到 wctx 的取消错误（ctx.Canceled）——收场观测。
		deadline := time.Now().Add(2 * time.Second)
		for {
			v, err := node.Call(ctx, id, "then-err")
			if err == nil && v.(string) != "" {
				if !strings.Contains(v.(string), "context canceled") {
					t.Fatalf("then err text = %q, want context canceled", v)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("then never observed the wctx cancellation")
			}
			time.Sleep(5 * time.Millisecond)
		}
		// 窗回退 Active：core 观测到取消预算判输——实体继续服役。
		deadline = time.Now().Add(2 * time.Second)
		for {
			if node.Holds(id) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("entity must return to Active after window rollback")
			}
			time.Sleep(5 * time.Millisecond)
		}
		if v, err := node.Call(ctx, id, "ping"); err != nil || v != "ok" {
			t.Fatalf("entity must serve again after rollback, got (%v, %v)", v, err)
		}
	})
}

// TestMsgCancel_CancelsWork 取消面扩展（票 06 联动）：work 在飞时调用方取消
// → wctx 取消 → work 快速返回 → then 收 err（ctx.Canceled）→ Reply 丢弃
// （已 Deferred 未 Reply 标记取消——丢弃语义在 rans 单测锚定，端到端观测为
// 调用方按自身取消收场后实体照常服务）。本地腿经调用方 ctx 穿透、远端腿经
// MsgCancel 控制消息，同一取消契约两腿一致。
func TestMsgCancel_CancelsWork(t *testing.T) {
	for _, leg := range reenterLegs {
		t.Run(leg.name, func(t *testing.T) {
			e := &reenterEntity{started: make(chan struct{}, 1)}
			c := NewCluster(t, leg.nodes, map[string]rans.Factory{
				"reent": func(rans.ID) rans.Entity { return e },
			})
			id := mustID(t, "reent", "mc-1")
			caller := c.Node(0)
			ctx := context.Background()
			if leg.nodes == 2 {
				if _, err := c.Node(1).Call(ctx, id, "warm"); err != nil {
					t.Fatalf("pre-activate entity on node 2: %v", err)
				}
			}

			callCtx, cancel := context.WithCancel(ctx)
			out := make(chan error, 1)
			go func() {
				_, err := caller.CallOpts(callCtx, id, "reenter-slow", rans.Timeout(10*time.Second))
				out <- err
			}()
			<-e.started                        // work 在飞（30s 定时器与 wctx 竞速）
			time.Sleep(300 * time.Millisecond) // 取消通知送达窗（尽力语义）

			cancelAt := time.Now()
			cancel()
			select {
			case err := <-out:
				if elapsed := time.Since(cancelAt); elapsed > time.Second {
					t.Fatalf("caller returned %v after cancel, want immediate", elapsed)
				}
				if !errors.Is(err, rans.ErrCanceled) {
					t.Fatalf("err = %v, want rans.ErrCanceled", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("caller did not return after cancel")
			}

			// wctx 取消 → work 快速返回 → then 收 ctx.Canceled（收场观测）。
			deadline := time.Now().Add(2 * time.Second)
			for {
				v, err := caller.Call(ctx, id, "then-err")
				if err == nil && v.(string) != "" {
					if !strings.Contains(v.(string), "context canceled") {
						t.Fatalf("then err text = %q, want context canceled", v)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("work did not converge after cancel (wctx not canceled)")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if v, err := caller.Call(ctx, id, "ping"); err != nil || v != "ok" {
				t.Fatalf("entity loop must stay healthy, got (%v, %v)", v, err)
			}
		})
	}
}
