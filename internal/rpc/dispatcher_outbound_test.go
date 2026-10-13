package rpc

// 出站分野收口的行为锁（H11 决策 12/13，票 09）：
//   - 输家转发的两条调用腿（读循环输家分支 / pawnite 终局整箱排空）遇慢
//     目标（解析挂起形态）不阻塞调用方 goroutine——转发经节点发件箱满即
//     丢入箱即返回，发送归分片 worker；
//   - 错误回包满箱满即丢 + 计数：调用方（源端）按请求预算超时收场。
//
// 慢目标经端点发送面挂闸注入（conns 替身 SendToNode 阻塞至放行——生产
// 形态里 resolve/懒拨号/握手都在这一次调用内收敛，与 testkit 分区/慢拨
// 号同族的确定性形态）；时序断言走 channel/仪表轮询，不用 sleep 同步。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// gatedResolveEnv 输家转发测试基座：B 节点（self=2）真 activation.Table +
// 真 Gateway（conns 为挂闸发送替身——端点发送不收敛即旧同步重试链的
// 全部阻塞面，与 testkit 分区/慢拨号同族的确定性形态）+ 接箱 Dispatcher。
type gatedResolveEnv struct {
	disp *Dispatcher
	mgr  *activation.Table
	dir  directory.Directory
	gw   *Gateway
	fake *gatedSender
	pool *wire.PagePool
}

// newGatedResolveEnv 构造基座：端点发送在闸关闭时阻塞（慢目标形态）。
func newGatedResolveEnv(t *testing.T) *gatedResolveEnv {
	t.Helper()
	dir := inmem.New()
	led := &entityLedger{}
	kinds := map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity {
			e := &testEntity{handle: func(context.Context, any) (any, error) { return nil, nil }}
			led.add(e)
			return e
		},
	}
	mgr := activation.New(dir, 2, kinds)
	pool := wire.NewPagePool()
	fake := newGatedSender()
	gw := NewGateway(fake, nil, pool, time.Second, nil)
	ob, err := NewOutbox(OutboxConfig{Send: gw.SendOneWay, Metrics: metrics.New("gated-resolve")})
	if err != nil {
		t.Fatalf("new outbox: %v", err)
	}
	t.Cleanup(ob.Close)
	disp := NewDispatcher(mgr, gw, 2, pool, nil, 0, ob, nil)
	return &gatedResolveEnv{disp: disp, mgr: mgr, dir: dir, gw: gw, fake: fake, pool: pool}
}

// page 构造池化页载荷。
func (e *gatedResolveEnv) page(b []byte) []byte {
	p := e.pool.Get(len(b))
	copy(p, b)
	return p
}

// TestDispatcher_LoserForwardOffReadLoop 输家转发不阻塞调用腿（红测锚：
// 旧同步重试链在 resolve 闸上队头阻塞读循环/实体 loop，两腿双双变红）。
func TestDispatcher_LoserForwardOffReadLoop(t *testing.T) {
	env := newGatedResolveEnv(t)
	ctx := context.Background()
	id, err := rans.NewID("cell", "fw")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := env.dir.Register(ctx, id, directory.Owner{Node: 3}, nil); err != nil {
		t.Fatal(err)
	}
	const reqID = 701
	loserHdr := reqHdrFor(protocol.MsgRequest, reqID, "fw", nil)

	// 腿一（读循环）：handleRequest 捕获 ArbitrationLostError 后的逐字调用
	// 形状——同一 goroutine 紧接处理下一帧（同连接读循环语义）。转发遇
	// 发送闸：非阻塞入箱下下一帧照常被处理；旧同步重试链下读循环卡死
	// 在转发里，下一帧永不达。
	nextHandled := make(chan struct{})
	oneWayHdr := reqHdrFor(protocol.MsgOneWay, 702, "next", nil)
	go func() {
		env.disp.newDelivery(loserHdr, env.page(envBody(t, 7)), id).Forward(3)
		env.disp.HandleMessage(oneWayHdr, env.page(envBody(t, 8)))
		close(nextHandled)
	}()
	select {
	case <-nextHandled:
	case <-time.After(2 * time.Second):
		t.Fatal("read loop blocked in loser forward: next frame never processed")
	}

	// 腿二（pawnite 终局整箱排空）：输家请求入站 → 仲裁落败 → 整箱转发。
	// 终态收尾（含转发入箱）必须在闸仍关时完成——旧代码 Forward 卡在
	// 发送闸上，实例永不逐出。
	drainID, err := rans.NewID("cell", "drain")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := env.dir.Register(ctx, drainID, directory.Owner{Node: 3}, nil); err != nil {
		t.Fatal(err)
	}
	const drainIDReq = 703
	done := make(chan struct{})
	go func() {
		env.disp.HandleMessage(reqHdrFor(protocol.MsgRequest, drainIDReq, "drain", nil),
			env.page(envBody(t, 9)))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("inbound request handling blocked on the losing activation")
	}
	waitLoserTerminalDeadline(t, env.mgr, drainID, 2*time.Second)

	// 放闸：两腿的转发帧异步离箱，原样透传（FlagForwarded + 原始字节 +
	// MessageID），全部寻址赢家节点 3。
	close(env.fake.gate)
	first := gatedSentOf(t, env.fake)
	second := gatedSentOf(t, env.fake)
	for _, fr := range []gatedSent{first, second} {
		if fr.node != 3 {
			t.Fatalf("forward must address winner node 3, got node %d", fr.node)
		}
		if fr.hdr.Flags&protocol.FlagForwarded == 0 {
			t.Fatalf("forwarded frame must set FlagForwarded, got %+v", fr.hdr)
		}
	}
	if first.hdr.MessageID != reqID || second.hdr.MessageID != drainIDReq {
		t.Fatalf("forward MessageID passthrough broken: %d/%d", first.hdr.MessageID, second.hdr.MessageID)
	}
	env.pool.Put(first.body)
	env.pool.Put(second.body)
}

// gatedSentOf 从挂闸替身确定性收一帧（channel 断言，不 sleep）。
func gatedSentOf(t *testing.T, s *gatedSender) gatedSent {
	t.Helper()
	select {
	case fr := <-s.sent:
		return fr
	case <-time.After(5 * time.Second):
		t.Fatal("gated sender never delivered the frame")
		return gatedSent{} // 不可达
	}
}

// waitLoserTerminalDeadline 带显式超时的输家终态等待（旧阻塞形态在此变红
// ——共用 waitLoserTerminal 的 5s 预算会把红信号拖到测试超时）。
func waitLoserTerminalDeadline(t *testing.T, m *activation.Table, id rans.ID, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if _, ok := m.GetLocal(id); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("loser instance did not reach terminal eviction within %v (forward blocked the terminating loop?)", within)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestDispatcherReplyDropsAtOutboxCapacity 错误回包满即丢（H11 决策 12）：
// 箱满后回包就地丢弃 + rans.outbox.dropped 计数，reply 即刻返回（源端按
// 请求预算超时收场——best-effort 回执的固定满策略）；腾位后恢复入箱。
func TestDispatcherReplyDropsAtOutboxCapacity(t *testing.T) {
	blocked := newGatedSender()
	gw := NewGateway(blocked, nil, nil, time.Minute, nil)
	t.Cleanup(gw.Close)
	reg := metrics.New("reply-drop")
	ob, err := NewOutbox(OutboxConfig{Capacity: 1, Send: gw.SendOneWay, Metrics: reg})
	if err != nil {
		t.Fatalf("new outbox: %v", err)
	}
	t.Cleanup(ob.Close)
	d := NewDispatcher(nil, gw, 1, nil, nil, 0, ob, nil)

	in := wire.Header{Type: protocol.MsgRequest, MessageID: 1, SourceNodeID: 2}
	d.replyError(in, rans.NewError(rans.CodeError, rans.ReasonNone, "e1"))
	waitReading(t, reg, metrics.NameOutboxDepth, 0) // 首帧被 worker 取走（卡在发送段）
	d.replyError(wire.Header{Type: protocol.MsgRequest, MessageID: 2, SourceNodeID: 2},
		rans.NewError(rans.CodeError, rans.ReasonNone, "e2"))
	waitReading(t, reg, metrics.NameOutboxDepth, 1) // 容量 1 占满

	// 满箱回包：就地丢弃 + 计数，即刻返回（不阻塞调用腿）。
	start := time.Now()
	d.replyError(wire.Header{Type: protocol.MsgRequest, MessageID: 3, SourceNodeID: 2},
		rans.NewError(rans.CodeError, rans.ReasonNone, "e3"))
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("reply at capacity took %v, want immediate drop", elapsed)
	}
	if v := waitReading(t, reg, metrics.NameOutboxDropped, 1); v != 1 {
		t.Fatalf("outbox drops = %v, want 1", v)
	}

	// 放行：在箱两帧照常离箱（#1/#2），被丢的 #3 永不到达。
	close(blocked.gate)
	f1 := <-blocked.sent
	f2 := <-blocked.sent
	if f1.hdr.MessageID != 1 || f2.hdr.MessageID != 2 {
		t.Fatalf("queued replies broken: got %d/%d, want 1/2", f1.hdr.MessageID, f2.hdr.MessageID)
	}
	select {
	case fr := <-blocked.sent:
		t.Fatalf("dropped reply %d must never reach the wire", fr.hdr.MessageID)
	case <-time.After(100 * time.Millisecond):
	}
}
