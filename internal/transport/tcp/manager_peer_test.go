package tcp

// 票 02 红测：SendToNode 的对端身份校验——解析函数把节点 2 解析到节点 3
// 的监听地址（k8s pod IP 复用形态）时，帧不得送给节点 3，返回可分类的发送
// 错误（确定未投递）。

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

func TestManager_SendToNodeRejectsWrongPeer(t *testing.T) {
	mA, hA := newManager(t, 3) // 地址实际属于节点 3
	addr, err := mA.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mB, _ := newManager(t, 1)

	hdr := wire.Header{
		Type:         protocol.MsgRequest,
		MessageID:    7,
		Flags:        protocol.FlagHasSourceNodeID,
		SourceNodeID: 1,
	}
	resolve := func(_ context.Context, n rans.NodeID) (string, error) {
		return addr, nil // 节点 2 的陈旧地址已属节点 3
	}
	err = mB.SendToNode(t.Context(), 2, resolve, hdr, []byte("wrong peer"))
	if err == nil {
		t.Fatal("SendToNode must reject when the resolved address belongs to another node")
	}
	if !errors.Is(err, ErrWrongPeer) {
		t.Fatalf("err = %v, want ErrWrongPeer in chain", err)
	}
	select {
	case m := <-hA.got:
		t.Fatalf("node 3 must not receive the frame, got %+v", m.hdr)
	default:
	}
	// 连接本身留给真实对端：经节点 3 的正确号发送复用同一条连接成功。
	if err := mB.SendToNode(t.Context(), 3, resolve, hdr, []byte("right peer")); err != nil {
		t.Fatalf("send to the actual peer via the same conn: %v", err)
	}
	select {
	case m := <-hA.got:
		if string(m.body) != "right peer" {
			t.Fatalf("body = %q, want right peer only", m.body)
		}
	case <-time.After(time.Second):
		t.Fatal("actual peer must receive via the same conn")
	}
	select {
	case m := <-hA.got:
		t.Fatalf("unexpected extra frame: %+v", m.hdr)
	default:
	}
}
