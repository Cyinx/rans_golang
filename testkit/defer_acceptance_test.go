package testkit

// HR4 验收落点（H12 决策 16 前半，票 11）：Defer 延迟应答——Responder 与
// 投递边界 Deferred 哨兵。本地（callDelivery）与远端（remoteDelivery）两
// 形态同矩阵：后续消息的 Handle 里 Reply 收场（聚合应答形态）、只 Defer
// 不回复由调用方 deadline 超时收场（收端不自建到期定时）、Reply 幂等；
// Defer 后的编程错误收场形态（明确错误应答，哨兵值绝不当作成功回包）。

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// deferEntity 延迟应答道具实体："defer" 登记 Responder 存实体状态（聚合
// 应答形态——Responder 与 ctx 解耦），"reply"/"reply-twice" 由后续消息的
// Handle 收场。stashed 在 Defer 登记完成后投递（测试与后续消息间的确定
// 性时序锚；nil 通道 + default = 不观测形态）。
type deferEntity struct {
	mu      sync.Mutex
	resp    *rans.Responder
	stashed chan struct{}
}

func (e *deferEntity) Activate(context.Context) error   { return nil }
func (e *deferEntity) Deactivate(context.Context) error { return nil }

func (e *deferEntity) Handle(ctx context.Context, req any) (any, error) {
	switch cmd, _ := req.(string); cmd {
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
	case "reply-twice":
		e.mu.Lock()
		r := e.resp
		e.mu.Unlock()
		r.Reply(42, nil)
		r.Reply(43, nil)
		return "replied", nil
	case "defer-and-return":
		// 编程错误形态（Defer 后返回业务值）：错误应答收场。
		e.mu.Lock()
		e.resp = rans.Defer(ctx)
		e.mu.Unlock()
		return "oops", nil
	case "bogus-deferred":
		// 编程错误形态（未 Defer 返回哨兵）：错误应答收场。
		return rans.Deferred, nil
	}
	return "ok", nil
}

// deferLeg 单测参数：local = 单节点（本地 callDelivery 腿）；remote =
// 双节点（实体预激活节点 2，远端 remoteDelivery 腿）。
var deferLegs = []struct {
	name  string
	nodes int
}{
	{"local", 1},
	{"remote", 2},
}

// newDeferCluster 装配 defer 腿集群并返回（集群, 调用节点, 实体身份）。
// remote 形态先用中性消息把实体预激活在节点 2（目录路由随后指向节点 2，
// 节点 1 的调用走远端腿）。
func newDeferCluster(t *testing.T, nodes int, stashed chan struct{}) (*Cluster, *Node, rans.ID) {
	t.Helper()
	c := NewCluster(t, nodes, map[string]rans.Factory{
		"defr": func(rans.ID) rans.Entity { return &deferEntity{stashed: stashed} },
	})
	ctx := context.Background()
	id := mustID(t, "defr", "d1")
	node := c.Node(0)
	if nodes == 2 {
		if _, err := c.Node(1).Call(ctx, id, "warm"); err != nil {
			t.Fatalf("pre-activate entity on node 2: %v", err)
		}
	}
	return c, node, id
}

// waitStashed 有界等待 Defer 登记完成（时序锚）。
func waitStashed(t *testing.T, stashed chan struct{}) {
	t.Helper()
	select {
	case <-stashed:
	case <-time.After(5 * time.Second):
		t.Fatal("message 1 Handle never deferred")
	}
}

// TestDefer_ReplyFromLaterMessage 消息 1 Defer（不回包）、消息 2 的 Handle
// 里 Reply → 调用 1 成功收到值：本地与远端两形态一致。突变自证锚：投递
// 边界若不识别 Deferred（当作普通返回回包），调用 1 在消息 2 之前就提前
// 收到哨兵值/编码错误——本测试红。
func TestDefer_ReplyFromLaterMessage(t *testing.T) {
	for _, leg := range deferLegs {
		t.Run(leg.name, func(t *testing.T) {
			stashed := make(chan struct{}, 4)
			_, node, id := newDeferCluster(t, leg.nodes, stashed)
			ctx := context.Background()

			type outcome struct {
				v   any
				err error
			}
			out := make(chan outcome, 1)
			go func() {
				v, err := node.Call(ctx, id, "defer")
				out <- outcome{v, err}
			}()
			waitStashed(t, stashed)

			// 消息 2：由后续消息的 Handle 里 Reply（聚合应答形态——
			// Responder 存实体状态，与 ctx 解耦）。
			if _, err := node.Call(ctx, id, "reply"); err != nil {
				t.Fatalf("reply message: %v", err)
			}
			select {
			case o := <-out:
				if o.err != nil {
					t.Fatalf("deferred call must complete via later Reply, got %v", o.err)
				}
				if o.v != 42 {
					t.Fatalf("deferred call value = %v, want 42", o.v)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("caller 1 never received the deferred reply")
			}
			// 实体 loop 照常服务。
			if v, err := node.Call(ctx, id, "ping"); err != nil || v != "ok" {
				t.Fatalf("entity loop must stay healthy, got (%v, %v)", v, err)
			}
		})
	}
}

// TestDefer_TimeoutWhenNeverReplied 只 Defer 不回复：调用方在自身预算到期
// 得 Timeout（deadline 权威在调用方——本地腿经调用方 ctx、远端腿经网关
// pending，收端不自建到期定时）；deadline 过后实体的 Reply 丢弃且无效、
// 不 panic，实体 loop 照常服务。
func TestDefer_TimeoutWhenNeverReplied(t *testing.T) {
	for _, leg := range deferLegs {
		t.Run(leg.name, func(t *testing.T) {
			stashed := make(chan struct{}, 4)
			_, node, id := newDeferCluster(t, leg.nodes, stashed)
			ctx := context.Background()

			errCh := make(chan error, 1)
			go func() {
				callCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
				defer cancel()
				_, err := node.Call(callCtx, id, "defer")
				errCh <- err
			}()
			waitStashed(t, stashed)
			select {
			case err := <-errCh:
				if !errors.Is(err, rans.ErrTimeout) {
					t.Fatalf("never-replied deferred call = %v, want rans.ErrTimeout", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("caller did not time out at its own budget")
			}

			// deadline 过后的 Reply 丢弃且无效（不 panic）；实体照常服务。
			if _, err := node.Call(ctx, id, "reply"); err != nil {
				t.Fatalf("post-deadline Reply must be dropped without panic: %v", err)
			}
			if v, err := node.Call(ctx, id, "ping"); err != nil || v != "ok" {
				t.Fatalf("entity loop must stay healthy, got (%v, %v)", v, err)
			}
		})
	}
}

// TestDefer_IdempotentReply Reply 两次 → 第二次无效，调用方只收第一次的
// 值（幂等契约面在 Responder——本地腿锚定，两投递边界共用同一 Responder）。
func TestDefer_IdempotentReply(t *testing.T) {
	stashed := make(chan struct{}, 4)
	_, node, id := newDeferCluster(t, 1, stashed)
	ctx := context.Background()

	type outcome struct {
		v   any
		err error
	}
	out := make(chan outcome, 1)
	go func() {
		v, err := node.Call(ctx, id, "defer")
		out <- outcome{v, err}
	}()
	waitStashed(t, stashed)

	if _, err := node.Call(ctx, id, "reply-twice"); err != nil {
		t.Fatalf("reply-twice message: %v", err)
	}
	select {
	case o := <-out:
		if o.err != nil {
			t.Fatalf("deferred call err = %v, want nil", o.err)
		}
		if o.v != 42 {
			t.Fatalf("deferred call value = %v, want 42 (first Reply wins, second is a no-op)", o.v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("caller 1 never received the deferred reply")
	}
}

// TestDefer_ProgrammingErrorReplies 编程错误的明确错误收场（非 panic 形态
// ——投递边界确定性回包）：Defer 后返回业务值、未 Defer 返回 Deferred 哨
// 兵，两者都回 CodeError+ReasonHandler 的错误应答（不回业务值、不把哨兵
// 值当成功回包）；误用后实体的迟到 Reply 自然丢弃，loop 照常服务。
func TestDefer_ProgrammingErrorReplies(t *testing.T) {
	for _, leg := range deferLegs {
		t.Run(leg.name, func(t *testing.T) {
			_, node, id := newDeferCluster(t, leg.nodes, nil)
			ctx := context.Background()

			if _, err := node.Call(ctx, id, "defer-and-return"); err == nil {
				t.Fatal("Defer-then-return must be answered with an error")
			} else if rans.CodeOf(err) != rans.CodeError || rans.ReasonOf(err) != rans.ReasonHandler {
				t.Fatalf("err = %v, want CodeError+ReasonHandler", err)
			}
			// Responder 的迟到 Reply 被调用方侧自然丢弃（不 panic）。
			if _, err := node.Call(ctx, id, "reply"); err != nil {
				t.Fatalf("late Reply after misuse must be dropped: %v", err)
			}

			if v, err := node.Call(ctx, id, "bogus-deferred"); err == nil {
				t.Fatalf("returned-Deferred-without-Defer must be an error, got value %v", v)
			} else if rans.CodeOf(err) != rans.CodeError || rans.ReasonOf(err) != rans.ReasonHandler {
				t.Fatalf("err = %v, want CodeError+ReasonHandler", err)
			}
			if v, err := node.Call(ctx, id, "ping"); err != nil || v != "ok" {
				t.Fatalf("entity loop must stay healthy, got (%v, %v)", v, err)
			}
		})
	}
}
