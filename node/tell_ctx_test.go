package node

// 本地 Tell 的 Handle ctx 与远端同构（位置透明）：不随发送腿的预算 cancel
// 取消、保留发送预算的 deadline；不承袭发送方 Handle 的延迟应答登记点。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// tellProbe 测试实体的共享观测面（一测一份，同 kind 各 key 共用）。
type tellProbe struct {
	blocked  chan struct{} // "block" 已占住 loop
	release  chan struct{} // 放行 "block"
	seen     chan tellSeen // "probe" 的 Handle ctx 读数
	bDone    chan struct{} // "defer" 已尝试 rans.Defer
	panicMsg atomic.Value  // "defer" 的 rans.Defer panic 文本（string；未 panic 不写）
}

type tellSeen struct {
	err error
	dl  time.Time
	has bool
}

func newTellProbe() *tellProbe {
	return &tellProbe{
		blocked: make(chan struct{}, 1),
		release: make(chan struct{}),
		seen:    make(chan tellSeen, 1),
		bDone:   make(chan struct{}),
	}
}

func (p *tellProbe) kinds() map[string]rans.Factory {
	return map[string]rans.Factory{
		"tellprobe": func(rans.ID) rans.Entity { return &tellEntity{p: p} },
	}
}

type tellEntity struct{ p *tellProbe }

func (e *tellEntity) Activate(context.Context) error   { return nil }
func (e *tellEntity) Deactivate(context.Context) error { return nil }

func (e *tellEntity) Handle(ctx context.Context, msg any) (any, error) {
	switch msg {
	case "block": // 占住 loop：其后入箱的 Tell 必在发送方返回后才执行
		e.p.blocked <- struct{}{}
		select {
		case <-e.p.release:
		case <-time.After(5 * time.Second):
		}
	case "probe":
		dl, has := ctx.Deadline()
		e.p.seen <- tellSeen{err: ctx.Err(), dl: dl, has: has}
	case "tell-defer": // A：Call 投递内向本地 B 发 Tell，B 的 Defer 尝试落定后才收场
		b, err := rans.NewID("tellprobe", "B")
		if err != nil {
			return nil, err
		}
		if err := rans.ClientFrom(ctx).Tell(ctx, b, "defer"); err != nil {
			return nil, err
		}
		select {
		case <-e.p.bDone:
		case <-time.After(5 * time.Second):
			return nil, errors.New("tell target never ran")
		}
		return "A-reply", nil
	case "defer": // B：Tell 投递内调 rans.Defer；记录后原样重抛，交 pawnite 折叠留痕
		defer func() {
			r := recover()
			if r != nil {
				e.p.panicMsg.Store(fmt.Sprint(r))
			}
			close(e.p.bDone)
			if r != nil {
				panic(r)
			}
		}()
		rans.Defer(ctx)
	}
	return nil, nil
}

// TestNodeTell_LocalHandleCtxLive 本地 Tell（调用方 ctx 无 deadline）的 Handle
// ctx 执行时未取消、带发送预算 deadline（远端腿同构）。先以 "block" 占住 loop，
// 探针 Tell 入箱返回（发送腿预算 cancel 已触发）后才放行执行——判读确定。
func TestNodeTell_LocalHandleCtxLive(t *testing.T) {
	const budget = 3 * time.Second
	p := newTellProbe()
	n := mustStartNode(t, Config{
		NodeID: 1, Standalone: true, Kinds: p.kinds(),
		Calls: CallConfig{Default: budget},
	})
	defer shutdownNode(n)
	id := mustID(t, "tellprobe", "live")
	if err := n.Client().Tell(context.Background(), id, "block"); err != nil {
		t.Fatalf("tell block: %v", err)
	}
	select {
	case <-p.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("block never started")
	}
	if err := n.Client().Tell(context.Background(), id, "probe"); err != nil {
		t.Fatalf("tell probe: %v", err)
	}
	sent := time.Now()
	close(p.release)
	select {
	case s := <-p.seen:
		if s.err != nil {
			t.Fatalf("local Tell Handle ctx.Err() = %v, want nil (must not die with the send leg)", s.err)
		}
		if !s.has || s.dl.After(sent.Add(budget)) {
			t.Fatalf("local Tell Handle ctx deadline = %v (has=%v), want the send budget (≤ %v past send)",
				s.dl, s.has, budget)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("probe tell never delivered")
	}
}

// TestNodeTell_LocalHandleHasNoDeferPoint A 的 Call 投递内向本地 B 发 Tell：B 的
// Handle 调 rans.Defer 必须以「无登记点」panic（单向无应答面；有类型 nil 遮蔽
// 只会得到空指针 panic，文本判读可分辨），绝不武装 A 的请求——A 的调用方照常
// 拿到 A 的返回值。
func TestNodeTell_LocalHandleHasNoDeferPoint(t *testing.T) {
	p := newTellProbe()
	n := mustStartNode(t, Config{NodeID: 1, Standalone: true, Kinds: p.kinds()})
	defer shutdownNode(n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	v, err := n.Client().Call(ctx, mustID(t, "tellprobe", "A"), "tell-defer")
	if msg, _ := p.panicMsg.Load().(string); !strings.Contains(msg, "outside a request Handle ctx") {
		t.Errorf("rans.Defer in a local Tell Handle: panic = %q, want the no-defer-point panic", msg)
	}
	if err != nil || v != "A-reply" {
		t.Fatalf("A's caller got (%v, %v), want (A-reply, nil)", v, err)
	}
}
