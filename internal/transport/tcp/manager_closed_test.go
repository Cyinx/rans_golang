package tcp

// Manager 关闭语义契约：Close 是终态——Close 与在途单飞拨号的竞态由
// 登记临界区收口（Close 快照 conns 之后才登记的连接由复验当场关闭，
// 不会成为本次 Close 的漏网者）；Close 后
// getOrConnect/Listen/StartHeartbeat 一律拒绝，不「复活」半残体（无心跳、
// 无收口）。closed 旗标在 closeOnce 内置位，登记临界区复验。

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// closeCountingConn 计数 Close 的包装连接（迟到登记连接被关闭的观测面）。
type closeCountingConn struct {
	net.Conn
	closes atomic.Int64
}

func (c *closeCountingConn) Close() error {
	c.closes.Add(1)
	return c.Conn.Close()
}

// 1. Close 后拨号被拒：不再新建连接。
func TestManagerCloseRejectsPostCloseDial(t *testing.T) {
	mA, _ := newManager(t, 1)
	addr, err := mA.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mA.Close()
	if _, err := mA.GetOrConnect(t.Context(), addr); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("post-close GetOrConnect err = %v, want ErrManagerClosed", err)
	}
}

// 2. Close 后不得重新 Listen（终态拒绝半残监听复活）。
func TestManagerCloseRejectsRelisten(t *testing.T) {
	m, _ := newManager(t, 1)
	if _, err := m.Listen(t.Context(), "127.0.0.1:0"); err != nil {
		t.Fatalf("listen: %v", err)
	}
	m.Close()
	if _, err := m.Listen(t.Context(), "127.0.0.1:0"); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("post-close Listen err = %v, want ErrManagerClosed", err)
	}
}

// 3. Close 与在途拨号竞态：Close 快照后才完成登记的连接必须被关闭并使
// 拨号失败——不得留一条「永不收口、无心跳」的孤儿连接。
func TestManagerCloseShutsDownLateRegisteredDial(t *testing.T) {
	mB, _ := newManager(t, 2) // 对端只提供握手地址；本测试不要求握手完成
	addr, err := mB.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	dialEntered := make(chan struct{})
	release := make(chan struct{})
	var dialed closeCountingConn
	cfgA := baseCfg()
	cfgA.NodeID = 1
	cfgA.Dial = func(ctx context.Context, a string) (net.Conn, error) {
		nc, peer := net.Pipe()
		_ = peer // 握手不会发生：登记前即被 closed 拒绝
		dialed = closeCountingConn{Conn: nc}
		close(dialEntered)
		<-release
		return &dialed, nil
	}
	mA := NewManager(cfgA, newFakeHandler(), nil)
	t.Cleanup(mA.Close)

	gorc := make(chan error, 1)
	go func() {
		_, err := mA.GetOrConnect(context.Background(), addr)
		gorc <- err
	}()
	<-dialEntered
	mA.Close() // 快照时连接尚未登记
	close(release)

	select {
	case err := <-gorc:
		if !errors.Is(err, ErrManagerClosed) {
			t.Fatalf("racing GetOrConnect err = %v, want ErrManagerClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("GetOrConnect did not return after Close")
	}
	if n := dialed.closes.Load(); n == 0 {
		t.Fatal("late-registered conn must be closed by the closed gate, not leaked")
	}
}

// 4. NewManager nil handler 装配期 fail-fast（装配错误在装配点暴露）。
func TestNewManagerNilHandlerPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewManager(nil handler) must panic at assembly time")
		}
	}()
	NewManager(baseCfg(), nil, nil)
}

// 5. 并发 Close 与拨号压测：-race 下无 panic，且任何交错要么连接被关闭
// 要么拨号被拒——不允许静默成功登记。
func TestManagerCloseVersusDialConcurrently(t *testing.T) {
	mA, _ := newManager(t, 1)
	mB, _ := newManager(t, 2)
	addr, err := mB.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = mA.GetOrConnect(context.Background(), addr) // 任意交错：成功（Close 前登记、被 Close 收编）或 ErrManagerClosed
		}()
	}
	mA.Close()
	wg.Wait()
}
