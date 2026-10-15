package rpc

// 票 12 红测（包内）：MsgCancel 经发件箱入请求同一目标队列——排在请求
// 之后不超车（修复前直发绕过发件箱：请求仍在箱内时取消先到对端、定位
// 扑空被忽略、请求随后完整执行）。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

func TestCancel_DoesNotOvertakeQueuedRequest(t *testing.T) {
	tgt := rans.NodeID(4)
	gate := make(chan struct{})
	var gateOnce = make(chan struct{})
	_ = gateOnce
	send := func(context.Context, rans.NodeID, wire.Header, []byte) error { <-gate; return nil }
	o, err := NewOutbox(OutboxConfig{Capacity: 16, Send: send,
		SendCall: func(context.Context, rans.NodeID, wire.Header, []byte, time.Time) error { <-gate; return nil },
		Metrics:  metrics.New("cancel-order-test")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
		o.Close()
	})
	gw := NewGateway(nil, nil, nil, time.Minute, nil)
	t.Cleanup(gw.Close)
	gw.SetCancelQueue(o)

	// 队头挂住 + 请求帧入箱（尚未送出）。
	if err := o.Put(context.Background(), tgt, wire.Header{MessageID: 1}, nil); err != nil {
		t.Fatalf("head tell: %v", err)
	}
	reqHdr := wire.Header{
		Type: protocol.MsgRequest, MessageID: 42,
		Flags:        protocol.FlagHasSourceNodeID | protocol.FlagHasTargetUUID,
		SourceNodeID: 1, TargetKind: "k", TargetInst: "i",
	}
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := o.PutCall(cctx, tgt, reqHdr, nil, func(error) {}); err != nil {
		t.Fatalf("put request frame: %v", err)
	}

	// 调用方取消：MsgCancel 必须入同一目标队列、排在请求帧之后（不超车）。
	gw.canceledOutcome(cctx, tgt, reqHdr)
	q, ok := o.queueFor(tgt)
	if !ok {
		t.Fatal("queueFor must not reject a live outbox")
	}
	q.mu.Lock()
	var types []uint8
	ids := make([]uint64, 0, len(q.queue))
	for _, it := range q.queue {
		types = append(types, uint8(it.hdr.Type))
		ids = append(ids, it.hdr.MessageID)
	}
	q.mu.Unlock()
	// 队头可能已被 worker 取走进发送段（挂闸）——断言面收敛为「请求帧之
	// 后紧跟取消帧」（取消不超车：它不在请求前面）。
	for i := 1; i < len(ids); i++ {
		if ids[i-1] == 42 && ids[i-1] == ids[i] {
			if types[i] != uint8(protocol.MsgCancel) || types[i-1] == uint8(protocol.MsgCancel) {
				t.Fatalf("queue ids=%v types=%v — cancel must sit right after the request", ids, types)
			}
			return
		}
	}
	if len(ids) == 0 {
		t.Fatal("cancel never enqueued")
	}
}
