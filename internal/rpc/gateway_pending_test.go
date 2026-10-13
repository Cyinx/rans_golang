package rpc

// pending 分片化验收（H15）：①到期精度——250ms 扫描周期不变，堆顶收割
// 只看过期项（精度与全表扫等价）；②FailPendingForNode 与 pending 总量
// 解耦（按节点索引——大 pending 面下只随该节点在途数线性）。

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/wire"
)

func TestGatewayPending_DeadlinePrecision(t *testing.T) {
	gw := NewGatewayWithSweep(fakeConnSender{}, nil, nil, time.Second, 20*time.Millisecond, nil)
	t.Cleanup(gw.Close)
	start := time.Now()
	_, _, err := gw.SendRequest(context.Background(), 2,
		wire.Header{MessageID: gw.NextMessageID()}, []byte("x"))
	if !errors.Is(err, rans.ErrTimeout) {
		t.Fatalf("deadline err = %v, want timeout", err)
	}
	// 精度上界 = 预算到期 + 扫描周期 + 完成传播（对照全表扫形态等价：
	// 1s 预算 + 20ms 周期 → 收场不得显著超出 1.1s；也不得早于预算）。
	if d := time.Since(start); d > 1100*time.Millisecond || d < time.Second {
		t.Fatalf("deadline precision degraded: %v (want ≈ 1s + sweep period)", d)
	}
}

func TestGatewayPending_FailForNodeIndexed(t *testing.T) {
	gw := NewGatewayWithSweep(fakeConnSender{}, nil, nil, time.Minute, time.Minute, nil)
	t.Cleanup(gw.Close)
	// 大 pending 面：另一节点的在途项填满（发送永不完成——fake 静默）。
	for i := 0; i < 5000; i++ {
		id := gw.NextMessageID()
		e := &pendingEntry{ch: make(chan rpcResult, 1), target: 7, deadline: time.Now().Add(time.Hour)}
		if err := gw.pt.register(id, 7, e); err != nil {
			t.Fatalf("fill %d: %v", i, err)
		}
	}
	victim := &pendingEntry{ch: make(chan rpcResult, 1), target: 3, deadline: time.Now().Add(time.Hour)}
	if err := gw.pt.register(gw.NextMessageID(), 3, victim); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if n := gw.FailPendingForNode(3, errors.New("severed")); n != 1 {
		t.Fatalf("FailPendingForNode(3) = %d, want 1", n)
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("FailForNode must not scale with total pending: %v", d)
	}
	select {
	case r := <-victim.ch:
		if r.err == nil {
			t.Fatal("victim must receive ConnectionLost error")
		}
	default:
		t.Fatal("victim must be completed promptly")
	}
	if gw.PendingCount() != 5000 {
		t.Fatalf("unrelated pending must survive: %d", gw.PendingCount())
	}
}
