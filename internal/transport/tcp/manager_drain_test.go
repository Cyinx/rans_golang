package tcp

// 票 06 红测：连接收敛的发送侧保序缓解——胜出的新连接在旧连接退役排空
//（写出全部在队帧）之后才放行 ready；旧连接写侧被闸住期间新连接不可用
//（SendToNode 就绪等待按 ctx 收场），放行后照常发送。

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// gateAppWrites 包装连接：握手帧（Handshake/Ack）放行，应用帧闸到 release。
type gateAppWrites struct {
	net.Conn
	release chan struct{}
}

func (g *gateAppWrites) Write(b []byte) (int, error) {
	if len(b) > 0 && (b[0] == uint8(protocol.MsgHandshake) || b[0] == uint8(protocol.MsgHandshakeAck)) {
		return g.Conn.Write(b)
	}
	<-g.release
	return g.Conn.Write(b)
}

func TestManager_NewConnWaitsOldDrainBeforeReady(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	cfgA := baseCfg()
	cfgA.NodeID = 1
	cfgA.WrapConn = func(nc net.Conn) net.Conn { return &gateAppWrites{Conn: nc, release: release} }
	hA := newFakeHandler()
	mA := NewManager(cfgA, hA, nil)
	t.Cleanup(mA.Close)
	addrA, err := mA.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen A: %v", err)
	}

	mB, _ := newManager(t, 2)
	addrB, err := mB.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen B: %v", err)
	}
	resA := func(_ context.Context, _ rans.NodeID) (string, error) { return addrA, nil }

	// conn1：B → A 拨号（A 侧收到的连接写侧被闸）。
	if _, err := mB.GetOrConnect(t.Context(), addrA); err != nil {
		t.Fatalf("B dial: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for mA.ConnToNode(2) == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	conn1 := mA.ConnToNode(2)
	if conn1 == nil {
		t.Fatal("conn1 never handshaked")
	}

	// A 在 conn1 上入队一帧——写侧被闸，writeLoop 停在应用帧写出段。
	hdr := wire.Header{Type: protocol.MsgOneWay, MessageID: 7, Flags: protocol.FlagHasSourceNodeID, SourceNodeID: 1}
	if err := mA.SendToNode(context.Background(), 2, resA, hdr, []byte("old-conn frame")); err != nil {
		t.Fatalf("send on old conn: %v", err)
	}

	// conn2：A → B 拨号（握手帧放行、新连接胜出顶替 conn1）。
	if _, err := mA.GetOrConnect(t.Context(), addrB); err != nil {
		t.Fatalf("A dial: %v", err)
	}
	// 等收敛登记落地（byNode[2] 翻到新连接）——否则发送仍走旧连接，断言
	// 面失焦。
	for mA.ConnToNode(2) == conn1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if mA.ConnToNode(2) == conn1 {
		t.Fatal("convergence never replaced conn1")
	}

	// 旧连接排空被闸期间：新连接不得放行（SendToNode 就绪等待按 ctx 收
	// 场——修复前 ready 即时关闭、发送立即入队返回 nil）。
	sctx, scancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer scancel()
	err = mA.SendToNode(sctx, 2, func(context.Context, rans.NodeID) (string, error) { return addrB, nil }, hdr, []byte("new-conn frame"))
	if err == nil {
		t.Fatal("new conn must stay unready until the old conn drains (send returned before drain)")
	}

	// 放行：旧连接排空 → 新连接 ready → 发送照常。
	close(release)
	bctx, bcancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer bcancel()
	if err := mA.SendToNode(bctx, 2, func(context.Context, rans.NodeID) (string, error) { return addrB, nil }, hdr, []byte("after drain")); err != nil {
		t.Fatalf("send after drain: %v", err)
	}
}
