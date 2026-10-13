package tcp

// 传输去阻塞面三组行为测试（HR4 票 07）：Send 尊重 ctx、TrySend 非阻塞
// ok 语义、控制帧独立优先通道。时序锚点全部用握手 channel、写闸 wrapper
// 与 net.Pipe 同步语义（读方不读即按住写循环），不用 sleep 同步。

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// blockWriteConn 写闸连接：置位后 Write 阻塞在闸上直至放行（确定性把
// 本端写循环按住在帧写出段——队列灌满不依赖调度时序）。Read 直通。
type blockWriteConn struct {
	net.Conn
	blocked atomic.Bool
	gate    chan struct{}
}

func newBlockWriteConn(nc net.Conn) *blockWriteConn {
	return &blockWriteConn{Conn: nc, gate: make(chan struct{})}
}

func (w *blockWriteConn) Write(p []byte) (int, error) {
	if w.blocked.Load() {
		<-w.gate
	}
	return w.Conn.Write(p)
}

// pinnedHandler 把读循环按住在 HandleMessage 内：入闸即发讯号——测试拿到
// 讯号即知对端（持有本 handler 的一端）不再读任何字节。body 页在闸放行
// 后归还（读循环消费下一帧前，所有权窗口覆盖闸内停留期）。
type pinnedHandler struct {
	handshake chan rans.NodeID
	lost      chan rans.NodeID
	entered   chan struct{}
	gate      chan struct{}
	pool      *wire.PagePool
}

func newPinnedHandler(pool *wire.PagePool) *pinnedHandler {
	return &pinnedHandler{
		handshake: make(chan rans.NodeID, 4),
		lost:      make(chan rans.NodeID, 4),
		entered:   make(chan struct{}, 1),
		gate:      make(chan struct{}),
		pool:      pool,
	}
}

func (h *pinnedHandler) HandleMessage(_ wire.Header, body []byte) {
	select {
	case h.entered <- struct{}{}:
	default:
	}
	<-h.gate
	h.pool.Put(body)
}

func (h *pinnedHandler) OnHandshakeComplete(n rans.NodeID) { h.handshake <- n }
func (h *pinnedHandler) OnConnectionLost(n rans.NodeID)    { h.lost <- n }

// waitEntered 等待对端读循环入闸（确定性背压锚点）。
func waitEntered(t *testing.T, h *pinnedHandler) {
	t.Helper()
	select {
	case <-h.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("peer read loop never pinned in HandleMessage")
	}
}

// TestConn_SendRespectsCtx 数据队列满载时，带 deadline 的 Send 必须在 ctx
// 到期返回包装 ctx.Err() 的错误（非永久阻塞）；腾出空位后连接照常可用。
func TestConn_SendRespectsCtx(t *testing.T) {
	cc, _ := newIdleConn(t, 2)
	cc.ready.Store(true) // 白盒：背压语义与握手无关，直入就绪态
	hdr := wire.Header{Type: protocol.MsgOneWay, Flags: protocol.FlagHasSourceNodeID, SourceNodeID: 1}
	if err := cc.Send(context.Background(), hdr, nil); err != nil {
		t.Fatalf("send 1: %v", err)
	}
	if err := cc.Send(context.Background(), hdr, nil); err != nil {
		t.Fatalf("send 2: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	blocked := make(chan error, 1)
	go func() { blocked <- cc.Send(ctx, hdr, nil) }()
	select {
	case err := <-blocked:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("send on full queue: err=%v want ctx deadline error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("send on full queue must return at ctx deadline, still blocked")
	}
	// 满队等待只是等待段：腾出空位后连接照常可用
	select {
	case f := <-cc.out:
		if f.body != nil {
			cc.pool.Put(f.body)
		}
	default:
		t.Fatal("expected a queued frame to consume")
	}
	if err := cc.Send(context.Background(), hdr, nil); err != nil {
		t.Fatalf("send after queue freed: %v", err)
	}
}

// TestConn_TrySendFullQueue 满队即 false 不阻塞（ok 语义）；空位即刻 true。
func TestConn_TrySendFullQueue(t *testing.T) {
	cc, _ := newIdleConn(t, 2)
	cc.ready.Store(true)
	hdr := wire.Header{Type: protocol.MsgOneWay, Flags: protocol.FlagHasSourceNodeID, SourceNodeID: 1}
	if !cc.TrySend(hdr, nil) {
		t.Fatal("try send on empty queue must succeed")
	}
	if err := cc.Send(context.Background(), hdr, nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	if cc.TrySend(hdr, nil) {
		t.Fatal("try send on full queue must report false")
	}
}

// TestConn_ControlFramePriority 数据队列满载（写循环被对端停读按住）后，
// 控制帧（Pong）经优先通道仍即刻发出，且先于其后所有仍在队的数据帧写出。
// 32KiB 帧体 ×2 即灌满 64KiB 写缓冲：写循环至多消费 2 帧即阻塞在冲刷段，
// 队列必有残留——优先通道的「先于队尾数据」才可观测。
func TestConn_ControlFramePriority(t *testing.T) {
	cfg := baseCfg()
	cfg.NodeID = 2
	cfg.SendQueueDepth = 4
	raw, cc, h, pool := rawListener(t, cfg)

	// 握手：raw 发 Handshake，读走 cc 回的 Ack——此后 raw 不再读，
	// cc 写循环在下一次冲刷即被 net.Pipe 同步语义按住。
	hs := wire.Handshake{NodeID: 1, ClusterName: "t", SchemaDigest: 7}
	if _, err := raw.Write(rawFrame(t, wire.Header{Type: protocol.MsgHandshake}, hs.Encode(nil))); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	dec := wire.NewFrameDecoder(raw, pool)
	rawHdr, rawBody, err := dec.Next()
	if err != nil {
		t.Fatalf("read handshake ack: %v", err)
	}
	ackHdr, err := wire.DecodeHeader(rawHdr)
	pool.Put(rawBody)
	if err != nil {
		t.Fatalf("decode ack header: %v", err)
	}
	if ackHdr.Type != protocol.MsgHandshakeAck {
		t.Fatalf("ack type=%v want HandshakeAck", ackHdr.Type)
	}
	waitHandshake(t, h, 1)

	// 灌数据队列（4×32KiB）：写循环至多吃掉 2 帧进写缓冲即阻塞，队列满
	data := wire.Header{Type: protocol.MsgOneWay, Flags: protocol.FlagHasSourceNodeID, SourceNodeID: 2}
	for i := 0; i < cfg.SendQueueDepth; i++ {
		if err := cc.Send(context.Background(), data, make([]byte, 32<<10)); err != nil {
			t.Fatalf("fill data queue #%d: %v", i, err)
		}
	}
	// 控制帧入优先通道：数据队满不影响控制面
	pong := wire.Header{Type: protocol.MsgPong, MessageID: 99}
	if !cc.TrySend(pong, nil) {
		t.Fatal("control frame must be accepted while data queue is full")
	}

	// raw 开始读：期望 [数据前缀…][Pong][至少一帧残留数据]——Pong 不得
	// 排在队尾（红态：控制帧同队排队尾，Pong 之后无任何数据帧）。
	_ = raw.SetReadDeadline(time.Now().Add(3 * time.Second))
	dataAfter, sawPong, dataBefore := 0, false, 0
	for {
		fhdr, fbody, err := dec.Next()
		if err != nil {
			t.Fatalf("read frame #%d: %v", dataBefore+dataAfter, err)
		}
		hdr, err := wire.DecodeHeader(fhdr)
		if err != nil {
			t.Fatalf("decode frame header: %v", err)
		}
		isPong := hdr.Type == protocol.MsgPong && hdr.MessageID == 99
		pool.Put(fbody)
		switch {
		case isPong:
			sawPong = true
		case sawPong:
			dataAfter++
		default:
			dataBefore++
		}
		if sawPong && dataAfter > 0 {
			break
		}
	}
	if !sawPong {
		t.Fatal("control frame never delivered")
	}
	if dataAfter == 0 {
		t.Fatalf("control frame starved behind data backlog (%d data frames before it)", dataBefore)
	}
}

// TestPeerStopsReading_LocalLoopAlive 对端停读时本端持续灌数据至队列满、
// 写循环阻塞在写闸——本端读循环必须仍处理对端发来的 Pong（存活时钟推进）
// 与应用帧（HandleMessage 投递）：读路径与控制面不被写背压拖死。
func TestPeerStopsReading_LocalLoopAlive(t *testing.T) {
	cfgA := Config{NodeID: 1, ClusterName: "t", NoHMAC: true, SendQueueDepth: 4, HandshakeTimeout: time.Second}
	cfgB := cfgA
	cfgB.NodeID = 2
	c1, c2 := net.Pipe()
	gate := newBlockWriteConn(c1) // 本端写闸：写循环按住在帧写出段
	pool := wire.NewPagePool()
	ha := newFakeHandler()
	hb := newPinnedHandler(pool)
	ca := NewConn(gate, cfgA, ha, pool, true)
	cb := NewConn(c2, cfgB, hb, pool, false)
	ca.Start()
	cb.Start()
	t.Cleanup(func() { _ = ca.Close(); _ = cb.Close() })
	t.Cleanup(func() { close(gate.gate) }) // 先放写闸再关连接（清理逆序）
	t.Cleanup(func() { close(hb.gate) })   // 先放读闸再放写闸（清理逆序）
	waitHandshake(t, ha, 2)
	waitGateHandshake(t, hb.handshake, 1)

	// 对端停读：一帧诱饵进 HandleMessage 闸，cb 从此不再读任何字节
	if err := ca.Send(context.Background(), wire.Header{Type: protocol.MsgOneWay, Flags: protocol.FlagHasSourceNodeID, SourceNodeID: 1}, nil); err != nil {
		t.Fatalf("send bait: %v", err)
	}
	waitEntered(t, hb)

	// 本端写循环确定性按住：置写闸，灌数据帧（>64KiB 写缓冲的帧体——
	// 首帧即被写循环取走卡在直写段，队列满载后不再有任何消费）
	gate.blocked.Store(true)
	stuck := wire.Header{Type: protocol.MsgOneWay, Flags: protocol.FlagHasSourceNodeID, SourceNodeID: 1}
	if err := ca.Send(context.Background(), stuck, make([]byte, 68<<10)); err != nil {
		t.Fatalf("send stall frame: %v", err)
	}
	takeDeadline := time.After(2 * time.Second)
	for {
		if len(ca.out) == 0 {
			break // 首帧已被写循环取走（卡在写闸），队列从此不再被消费
		}
		select {
		case <-takeDeadline:
			t.Fatal("writeLoop never picked up the stall frame")
		default:
		}
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < cfgA.SendQueueDepth; i++ {
		if err := ca.Send(context.Background(), stuck, make([]byte, 68<<10)); err != nil {
			t.Fatalf("fill data queue #%d: %v", i, err)
		}
	}

	// 对端写面照常健康：Ping / Pong / 应用帧依次发来——本端读循环必须
	// 全部照常处理（红态：Pong 应答阻塞在满数据队上，读循环拖死）。
	before := ca.LastPong()
	if err := cb.Send(context.Background(), wire.Header{Type: protocol.MsgPing}, nil); err != nil {
		t.Fatalf("peer ping: %v", err)
	}
	if err := cb.Send(context.Background(), wire.Header{Type: protocol.MsgPong}, nil); err != nil {
		t.Fatalf("peer pong: %v", err)
	}
	pongDeadline := time.After(2 * time.Second)
	for {
		if ca.LastPong().After(before) {
			break
		}
		select {
		case <-pongDeadline:
			t.Fatal("inbound Pong not processed: local read loop dead under write backpressure")
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if err := cb.Send(context.Background(), wire.Header{Type: protocol.MsgOneWay, Flags: protocol.FlagHasSourceNodeID, SourceNodeID: 2}, []byte("still-alive")); err != nil {
		t.Fatalf("peer app frame: %v", err)
	}
	select {
	case m := <-ha.got:
		if m.hdr.Type != protocol.MsgOneWay || string(m.body) != "still-alive" {
			t.Fatalf("unexpected delivery: type=%v body=%q", m.hdr.Type, m.body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inbound app frame not delivered: local read loop dead under write backpressure")
	}
}
