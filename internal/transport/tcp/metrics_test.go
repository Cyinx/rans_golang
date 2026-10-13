package tcp

// 传输指标观测面测试：重拨计数钩子（SeverNode 后重拨 +1）
// 与最近写批大小（LastBatchSize ≥ 1）。

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// TestManagerReconnectCounter 重拨计数：首拨不计数；连接断开（SeverNode
// 走 Lost 清表）后同地址重拨恰 +1；既有连接复用（懒建缓存命中）不计数。
func TestManagerReconnectCounter(t *testing.T) {
	mA, hA := newManager(t, 1)
	addr, err := mA.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var reconnects atomic.Int64
	mB, hB := newManagerWithReconnect(t, 2, func() { reconnects.Add(1) })

	// 首拨：不计数（双端握手齐备——SeverNode 依赖本端 byNode 登记）。
	if _, err := mB.GetOrConnect(t.Context(), addr); err != nil {
		t.Fatalf("first dial: %v", err)
	}
	waitHandshake(t, hA, 2)
	waitHandshake(t, hB, 1)
	if n := reconnects.Load(); n != 0 {
		t.Fatalf("reconnects after first dial = %d, want 0", n)
	}

	// 缓存复用：不计数。
	if _, err := mB.GetOrConnect(t.Context(), addr); err != nil {
		t.Fatalf("cached dial: %v", err)
	}
	if n := reconnects.Load(); n != 0 {
		t.Fatalf("reconnects on cache hit = %d, want 0", n)
	}

	// 断连 + 重拨：恰 +1。
	mB.SeverNode(1)
	if c := mB.ConnToNode(1); c != nil {
		t.Fatal("severed node must be cleared from byNode")
	}
	if _, err := mB.GetOrConnect(t.Context(), addr); err != nil {
		t.Fatalf("re-dial after sever: %v", err)
	}
	waitHandshake(t, hA, 2) // SeverNode 只关旧连接，重拨后对端再登记
	if n := reconnects.Load(); n != 1 {
		t.Fatalf("reconnects after re-dial = %d, want 1", n)
	}
}

// newManagerWithReconnect 构造带重拨观察钩子的 Manager（钩子经
// tcp.Config.OnReconnect 构造期注入）。
func newManagerWithReconnect(t *testing.T, node rans.NodeID, onReconnect func()) (*Manager, *fakeHandler) {
	t.Helper()
	cfg := baseCfg()
	cfg.NodeID = node
	cfg.OnReconnect = onReconnect
	h := newFakeHandler()
	m := NewManager(cfg, h, nil)
	t.Cleanup(func() { m.Close() })
	return m, h
}

// TestManagerBatchSize 最近写批大小：握手 + 应用帧写出后，最近一次冲刷的
// 帧数 ≥ 1（frames-per-flush 的直读面，gauge 数据源）。
func TestManagerBatchSize(t *testing.T) {
	mA, hA := newManager(t, 1)
	addr, err := mA.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mB, hB := newManager(t, 2)

	c, err := mB.GetOrConnect(t.Context(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	waitHandshake(t, hA, 2)
	waitHandshake(t, hB, 1)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if mB.LastBatchSize() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("LastBatchSize still 0 after handshake frames flushed")
		}
		// 握手帧之后补发应用帧兜底（握手本身的冲刷已可满足断言，此处只
		// 保证至少一次写出被观测）。
		if err := c.Send(context.Background(), wire.Header{Type: protocol.MsgRequest}, []byte("x")); err != nil {
			t.Fatalf("send: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
