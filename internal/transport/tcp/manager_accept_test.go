package tcp

// acceptLoop 错误韧性契约：Accept 错误三分——监听器已关闭（Manager.Close
// 的常规收尾）静默终止；临时类（net.Error.Temporary 语义，EMFILE/ENFILE
// 资源耗尽形态）退避重试并 Warn 留痕；其余持久错误终止并 Warn 留痕。
// 一次 fd 耗尽不得让入站永久失聪。

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// fakeTempErr 模拟 EMFILE 类临时 Accept 错误：实现 net.Error 的 Temporary
// 语义（临时类判据）。
type fakeTempErr struct{}

func (fakeTempErr) Error() string   { return "rans: fake: temporary accept error" }
func (fakeTempErr) Timeout() bool   { return false }
func (fakeTempErr) Temporary() bool { return true }

var _ net.Error = fakeTempErr{}

// injectListener Accept 注入包装：按序弹出 pending 错误（每次 Accept 一条），
// 耗尽后直通内层监听；accepts 记成功接入数（真接入的观察面）。
type injectListener struct {
	net.Listener
	mu      sync.Mutex
	pending []error
	accepts int
}

func (l *injectListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if len(l.pending) > 0 {
		err := l.pending[0]
		l.pending = l.pending[1:]
		l.mu.Unlock()
		return nil, err
	}
	l.mu.Unlock()
	nc, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.accepts++
		l.mu.Unlock()
	}
	return nc, err
}

func (l *injectListener) acceptCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.accepts
}

// acceptLoopFixture 韧性测试底座：长握手超时（半开连接不被超时收尸，conns
// 登记数是稳定的断言面）+ 收集日志的 sink。
func acceptLoopFixture(t *testing.T) (*Manager, *logSink) {
	t.Helper()
	sink := captureLog(t)
	cfg := baseCfg()
	cfg.NodeID = 1
	cfg.HandshakeTimeout = 30 * time.Second
	m := NewManager(cfg, newFakeHandler(), nil)
	t.Cleanup(m.Close)
	return m, sink
}

// rawTestListener 裸回环监听（注入包装的内层），测试收尾统一关闭。
func rawTestListener(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

// 1. 注入一次临时 Accept 错误后监听仍接入后续连接（修复前循环静默退出
// ——后续连接永不被接入），重试留 Warn 痕，关闭后循环有界终止（无泄漏）。
func TestAcceptLoopRetriesTemporaryError(t *testing.T) {
	m, sink := acceptLoopFixture(t)
	l := rawTestListener(t)
	wl := &injectListener{Listener: l, pending: []error{fakeTempErr{}}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.acceptLoop(wl)
	}()

	for i := 0; i < 2; i++ {
		nc, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Fatalf("dial #%d: %v", i, err)
		}
		t.Cleanup(func() { nc.Close() })
	}
	pollUntil(t, "both dials must be accepted after a temporary error", func() bool {
		return wl.acceptCount() == 2
	})
	pollUntil(t, "accepted conns must reach startConn registration", func() bool {
		return connsLen(m) == 2
	})
	if sink.count("temporary accept error, retrying") == 0 {
		t.Fatal("temporary accept error must leave a Warn trace")
	}
	l.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("acceptLoop must terminate after the listener is closed")
	}
}

// 2. 监听器关闭（net.ErrClosed，Manager.Close 形态）：循环正常终止且
// 不留日志（常规收尾非事故），关闭后不得重试空转（空转即 goroutine 泄漏）。
func TestAcceptLoopStopsOnClosedListener(t *testing.T) {
	m, sink := acceptLoopFixture(t)
	l := rawTestListener(t)
	wl := &injectListener{Listener: l}
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.acceptLoop(wl)
	}()

	l.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("closed listener must terminate acceptLoop")
	}
	if n := sink.count("rans/tcp:"); n != 0 {
		t.Fatalf("closed listener must terminate silently, got %d log lines", n)
	}
}

// 3. 持久错误（非临时非关闭）：循环终止并 Warn 留痕——永久失聪必须可观测；
// 终止后循环不得重试空转。
func TestAcceptLoopStopsOnPersistentError(t *testing.T) {
	m, sink := acceptLoopFixture(t)
	l := rawTestListener(t)
	wl := &injectListener{Listener: l, pending: []error{errors.New("rans: fake: persistent accept failure")}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.acceptLoop(wl)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("persistent accept error must terminate the loop")
	}
	if sink.count("persistent accept failure") == 0 {
		t.Fatal("persistent accept error must leave a Warn trace")
	}
	l.Close()
}
