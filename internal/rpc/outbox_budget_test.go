package rpc

// 票 05 红测：发送时刻预算语义——排队过预算的帧不送出（调用帧回投确
// 定未送出、单向帧计 rans.call.expired）；扩展块 budget_ms 在写出时刻按
// 剩余重写（排队耗时扣除，收端不放大调用方预算）。

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// TestOutbox_ExpiredFrameNotSent 排队过预算：同目标队头挂住、调用帧预算
// 在箱内耗尽后放行——worker 不得再调 SendCall（修复前以已过期 ctx 调用，
// 真实面的逃逸路径仍会送出并在对端迟到执行）；调用帧回投「确定未送出」、
// 单向帧丢弃计 rans.call.expired。
func TestOutbox_ExpiredFrameNotSent(t *testing.T) {
	tgt := rans.NodeID(2)
	gate := make(chan struct{})
	var gateOnce sync.Once
	open := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(open)
	var calls, ones atomicInt32
	send := func(context.Context, rans.NodeID, wire.Header, []byte) error {
		ones.add(1)
		<-gate
		return nil
	}
	sendCall := func(context.Context, rans.NodeID, wire.Header, []byte, time.Time) error {
		calls.add(1)
		<-gate
		return nil
	}
	reg := metrics.New("outbox-exp-test")
	o, err := NewOutbox(OutboxConfig{Capacity: 16, Send: send, SendCall: sendCall, Metrics: reg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Close)

	// 队头 Tell 挂住发送腿。
	if err := o.Put(context.Background(), tgt, wire.Header{MessageID: 1}, nil); err != nil {
		t.Fatalf("head tell: %v", err)
	}
	// 过期调用帧 + 过期单向帧（预算 50ms）。
	cctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	fails := make(chan error, 1)
	if err := o.PutCall(cctx, tgt, wire.Header{MessageID: 2}, nil, func(err error) { fails <- err }); err != nil {
		t.Fatalf("put call: %v", err)
	}
	if err := o.Put(cctx, tgt, wire.Header{MessageID: 3}, nil); err != nil {
		t.Fatalf("put tell: %v", err)
	}
	<-cctx.Done() // 预算在箱内耗尽（队头仍挂住）
	open()        // 放行队头，worker 轮到过期帧

	select {
	case err := <-fails:
		if rans.CodeOf(err) != rans.CodeUnavailable {
			t.Fatalf("expired call settled as %v (code=%v), want Unavailable (确定未送出)", err, rans.CodeOf(err))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expired call frame never settled")
	}
	if n := calls.load(); n != 0 {
		t.Fatalf("SendCall invoked %d times for expired frames, want 0", n)
	}
	if v := waitReading(t, reg, metrics.NameCallExpired, 1); v != 1 {
		t.Fatalf("expired one-way drops = %v, want 1", v)
	}
	// 队头自身（未过期，Background 无预算）照常发送。
	if n := ones.load(); n < 1 {
		t.Fatal("head tell must still be sent")
	}
}

// TestOutbox_BudgetRestampedAtSend 扩展块预算写出时刻重写：帧排队 150ms
// （队头挂住）、总预算 1s——送出帧的 budget_ms 必须 ≤ 剩余（修复前是盖戳
// 时的 1000，收端按「到达+预算」锚定会放大调用方预算）。
func TestOutbox_BudgetRestampedAtSend(t *testing.T) {
	tgt := rans.NodeID(3)
	gate := make(chan struct{})
	var gateOnce sync.Once
	open := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(open)
	var gotHdr wire.Header
	sent := make(chan struct{}, 1)
	send := func(context.Context, rans.NodeID, wire.Header, []byte) error { <-gate; return nil }
	sendCall := func(_ context.Context, _ rans.NodeID, hdr wire.Header, _ []byte, _ time.Time) error {
		gotHdr = hdr
		sent <- struct{}{}
		return nil
	}
	o, err := NewOutbox(OutboxConfig{Capacity: 16, Send: send, SendCall: sendCall,
		Metrics: metrics.New("outbox-restamp-test")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Close)

	// 队头挂住 + 携带预算扩展块的调用帧排队 150ms。
	if err := o.Put(context.Background(), tgt, wire.Header{MessageID: 1}, nil); err != nil {
		t.Fatalf("head tell: %v", err)
	}
	ext := wire.EncodeRequestExt(wire.RequestExt{BudgetMS: 1000, HasBudget: true})
	hdr := wire.Header{MessageID: 2, Flags: protocol.FlagHasExtensions, Ext: ext}
	cctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := o.PutCall(cctx, tgt, hdr, nil, func(error) {}); err != nil {
		t.Fatalf("put call: %v", err)
	}
	for time.Since(start) < 150*time.Millisecond { // 排队 150ms（预算未耗尽）
		time.Sleep(5 * time.Millisecond)
	}
	open()
	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("call frame never sent after head release")
	}
	e, err := wire.DecodeRequestExt(gotHdr.Ext)
	if err != nil || !e.HasBudget {
		t.Fatalf("sent frame ext = %+v err=%v, want budget field", e, err)
	}
	remaining := time.Until(time.Now().Add(time.Second)) // 调用方剩余（近似）
	if int64(e.BudgetMS) > remaining.Milliseconds()+50 {
		t.Fatalf("sent budget_ms = %d, want <= remaining ~%dms (queue time must be deducted)",
			e.BudgetMS, remaining.Milliseconds())
	}
}

// TestRestampBudget 重写助手：无扩展块/无预算字段/零 deadline 原样通过；
// 因果与传播项往返保真。
func TestRestampBudget(t *testing.T) {
	plain := wire.Header{MessageID: 1}
	restampBudget(&plain, time.Now().Add(time.Second))
	if plain.Ext != nil {
		t.Fatal("no-ext header must pass through untouched")
	}
	e := wire.RequestExt{
		BudgetMS: 5000, HasBudget: true,
		TraceID: "t1", HasTraceID: true,
		Ctx: []wire.CtxItem{{Name: "k", Value: []byte("v")}},
	}
	hdr := wire.Header{MessageID: 2, Flags: protocol.FlagHasExtensions, Ext: wire.EncodeRequestExt(e)}
	restampBudget(&hdr, time.Time{}) // 零 deadline：不动
	got, _ := wire.DecodeRequestExt(hdr.Ext)
	if got.BudgetMS != 5000 {
		t.Fatalf("zero deadline must not restamp, got %d", got.BudgetMS)
	}
	restampBudget(&hdr, time.Now().Add(300*time.Millisecond))
	got, _ = wire.DecodeRequestExt(hdr.Ext)
	if got.BudgetMS > 350 {
		t.Fatalf("restamped budget = %d, want <= 350ms", got.BudgetMS)
	}
	if got.TraceID != "t1" || len(got.Ctx) != 1 || string(got.Ctx[0].Value) != "v" {
		t.Fatalf("roundtrip lost fields: %+v", got)
	}
}

// atomicInt32 测试侧原子计数。
type atomicInt32 struct{ n int32 }

func (a *atomicInt32) add(d int32) int32 { return atomic.AddInt32(&a.n, d) }
func (a *atomicInt32) load() int32       { return atomic.LoadInt32(&a.n) }
