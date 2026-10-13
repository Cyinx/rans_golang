package tcp

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// newManager 测试侧构造：独立 Config/Handler，统一 Cleanup 关闭。
func newManager(t *testing.T, node rans.NodeID) (*Manager, *fakeHandler) {
	t.Helper()
	cfg := baseCfg()
	cfg.NodeID = node
	h := newFakeHandler()
	m := NewManager(cfg, h, nil)
	t.Cleanup(func() { m.Close() })
	return m, h
}

// 1. Listen + GetOrConnect：双方握手完成，双侧 byNode 登记。
func TestListenAndConnect(t *testing.T) {
	mA, hA := newManager(t, 1)
	addr, err := mA.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mB, hB := newManager(t, 2)
	if _, err := mB.GetOrConnect(t.Context(), addr); err != nil {
		t.Fatalf("get or connect: %v", err)
	}
	waitHandshake(t, hB, 1)
	waitHandshake(t, hA, 2)
	if c := mB.ConnToNode(1); c == nil {
		t.Fatal("B must have a conn to node 1")
	} else if !c.Ready() {
		t.Fatal("conn to node 1 must be ready")
	}
	if c := mA.ConnToNode(2); c == nil {
		t.Fatal("listener side must register the accepted conn by node")
	}
	// 懒建幂等：同地址再取，命中同一条连接
	c2, err := mB.GetOrConnect(t.Context(), addr)
	if err != nil {
		t.Fatalf("second get or connect: %v", err)
	}
	if c2 != mB.ConnToNode(1) {
		t.Fatal("second GetOrConnect must reuse the cached conn")
	}
}

// 2. 单飞拨号：慢拨号（200ms）×10 并发同地址 → 只拨一次。
func TestSingleFlightDial(t *testing.T) {
	// 真回环监听（net.Pipe 无 addr），fake dialer 只做计数 + 人为延迟
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("raw listen: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	// 受端只接不读：拨号成功即返回，握手交给超时兜底（本测试只验证单飞）
	go func() {
		for {
			nc, err := l.Accept()
			if err != nil {
				return
			}
			_ = nc // 持有到测试结束（listener Close 时统一断开）
		}
	}()

	var dials atomic.Int32
	cfg := baseCfg()
	cfg.NodeID = 1
	cfg.Dial = func(ctx context.Context, addr string) (net.Conn, error) {
		dials.Add(1)
		time.Sleep(200 * time.Millisecond) // 慢拨号：放大并发窗口
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
	m := NewManager(cfg, newFakeHandler(), nil)
	t.Cleanup(m.Close)

	const n = 10
	var wg sync.WaitGroup
	errs := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // 同拍起跑，全部落在同一拨号窗口内
			if _, err := m.GetOrConnect(context.Background(), l.Addr().String()); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent get or connect: %v", err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dial count=%d want 1 (single flight)", got)
	}
}

// 3. 心跳不误杀健康对端：双侧真实 Manager 短间隔心跳，无 Lost 事件。
func TestHeartbeatKeepsHealthyPeer(t *testing.T) {
	mA, hA := newManager(t, 1)
	mB, hB := newManager(t, 2)
	addr, err := mA.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if _, err := mB.GetOrConnect(t.Context(), addr); err != nil {
		t.Fatalf("connect: %v", err)
	}
	waitHandshake(t, hA, 2)
	waitHandshake(t, hB, 1)
	lostA, lostB := hA.lost, hB.lost
	mA.StartHeartbeat(t.Context(), 10*time.Millisecond, 50*time.Millisecond)
	mB.StartHeartbeat(t.Context(), 10*time.Millisecond, 50*time.Millisecond)
	// 6 个 pong 超时周期内互相 Ping/Pong 正常 → 双侧都不得踢除
	for _, ch := range []chan rans.NodeID{lostA, lostB} {
		select {
		case n := <-ch:
			t.Fatalf("healthy peer must not be kicked, lost=%d", n)
		case <-time.After(300 * time.Millisecond):
		}
	}
	if mA.ConnToNode(2) == nil || mB.ConnToNode(1) == nil {
		t.Fatal("both conns must survive heartbeat")
	}
}

// 4. 心跳踢除静默对端：raw 对端完成握手后吞 Ping 不回 Pong → OnLost + 移除。
func TestHeartbeatKicksSilentPeer(t *testing.T) {
	mA, hA := newManager(t, 1)
	addr, err := mA.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	lost := hA.lost

	// raw 对端：完成握手，之后只读丢弃（收到 Ping 不回 Pong）
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	t.Cleanup(func() { raw.Close() })
	hs := wire.Handshake{NodeID: 2, ClusterName: baseCfg().ClusterName, SchemaDigest: baseCfg().SchemaDigest}
	if _, err := raw.Write(rawFrame(t, wire.Header{Type: protocol.MsgHandshake}, hs.Encode(nil))); err != nil {
		t.Fatalf("raw handshake: %v", err)
	}
	discardDone := make(chan struct{})
	go func() {
		defer close(discardDone)
		dec := wire.NewFrameDecoder(raw, wire.NewPagePool())
		for {
			if _, _, err := dec.Next(); err != nil {
				return // 被踢除后读到关闭
			}
		}
	}()

	waitHandshake(t, hA, 2)
	mA.StartHeartbeat(t.Context(), 10*time.Millisecond, 50*time.Millisecond)
	select {
	case n := <-lost:
		if n != 2 {
			t.Fatalf("lost node=%d want 2", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("silent peer must be kicked by pong timeout")
	}
	if c := mA.ConnToNode(2); c != nil {
		t.Fatal("kicked conn must be removed from byNode")
	}
	select {
	case <-discardDone:
	case <-time.After(time.Second):
		t.Fatal("raw peer must observe the close")
	}
}

// 5. SeverNode：主动断开与某节点的连接，双方观察到 Done + OnLost。
func TestSeverNode(t *testing.T) {
	mA, hA := newManager(t, 1)
	addr, err := mA.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mB, hB := newManager(t, 2)
	c, err := mB.GetOrConnect(t.Context(), addr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	waitHandshake(t, hA, 2)
	waitHandshake(t, hB, 1)
	lostA, lostB := hA.lost, hB.lost

	mA.SeverNode(2)

	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("peer conn must be closed by SeverNode")
	}
	select {
	case n := <-lostB:
		if n != 1 {
			t.Fatalf("B lost node=%d want 1", n)
		}
	case <-time.After(time.Second):
		t.Fatal("peer must report node lost")
	}
	select {
	case n := <-lostA:
		if n != 2 {
			t.Fatalf("A lost node=%d want 2", n)
		}
	case <-time.After(time.Second):
		t.Fatal("severing side must report node lost")
	}
	if mA.ConnToNode(2) != nil {
		t.Fatal("severed conn must leave byNode on the severing side")
	}
}

// 6. SendToNode 未直连路径：resolve → 拨号 → 等握手 → 对端 Handler 收到。
func TestSendToNodeViaResolve(t *testing.T) {
	mA, hA := newManager(t, 1)
	addr, err := mA.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mB, _ := newManager(t, 2)

	hdr := wire.Header{
		Type:         protocol.MsgRequest,
		MessageID:    99,
		Flags:        protocol.FlagHasSourceNodeID,
		SourceNodeID: 2,
	}
	resolved := make(chan string, 1)
	resolve := func(_ context.Context, node rans.NodeID) (string, error) {
		resolved <- addr
		return addr, nil
	}
	if err := mB.SendToNode(t.Context(), 1, resolve, hdr, []byte("via resolve")); err != nil {
		t.Fatalf("send to node: %v", err)
	}
	select {
	case a := <-resolved:
		if a != addr {
			t.Fatalf("resolved addr=%q want %q", a, addr)
		}
	case <-time.After(time.Second):
		t.Fatal("resolver must be consulted for unknown node")
	}
	select {
	case m := <-hA.got:
		if m.hdr.Type != protocol.MsgRequest || m.hdr.MessageID != 99 || m.hdr.SourceNodeID != 2 {
			t.Fatalf("header mismatch: %+v", m.hdr)
		}
		if string(m.body) != "via resolve" {
			t.Fatalf("body=%q want %q", m.body, "via resolve")
		}
	case <-time.After(time.Second):
		t.Fatal("peer handler must receive the message")
	}
	if mB.ConnToNode(1) == nil {
		t.Fatal("SendToNode must leave the dialed conn registered by node")
	}
}

// 7. Close：listener 停 + 全部连接 Done + 端口可复用 + 幂等。
func TestCloseStopsListenerAndConns(t *testing.T) {
	mA, hA := newManager(t, 1)
	addr, err := mA.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mB, hB := newManager(t, 2)
	c, err := mB.GetOrConnect(t.Context(), addr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	waitHandshake(t, hA, 2)
	waitHandshake(t, hB, 1)
	aSide := mA.ConnToNode(2)
	if aSide == nil {
		t.Fatal("expected A-side conn")
	}

	mA.Close()
	mA.Close() // 幂等

	select {
	case <-aSide.Done():
	default:
		t.Fatal("A-side conn must be closed by Manager.Close")
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("B-side conn must observe the peer close")
	}
	// listener 已停：再拨被拒（连接失败即 listener 不在）
	var d net.Dialer
	dctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := d.DialContext(dctx, "tcp", addr); err == nil {
		t.Fatal("dial after Close must be refused (listener stopped)")
	}
	// 端口已释放：同端口可重新监听
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("port must be reusable after Close: %v", err)
	}
	l.Close()
}

// 8. registerNode 死分支清表：shutdown 在 ready 置位前赢得
// closeOnce（此刻 ready=false，OnConnectionLost 被跳过——connLost 不会
// 再来清表），finishHandshake 事后完成仍回调 OnHandshakeComplete →
// registerNode 见 Done 已关。该分支必须清 conns/byAddr：监听侧连接
// （addr=""）没有 getOrConnect 兜底，漏删即永久滞留表内（有界泄漏 +
// 心跳每拍对死连接空转 Close）。
func TestRegisterNodeDeadConnCleansMaps(t *testing.T) {
	// newDead 生产同构的「登记后死亡」连接：按 startConn 语义构造并登记
	// conns（addr 非 "" 时同步按 getOrConnect 语义登记 byAddr）；不 Start
	// ——无后台 goroutine，生产竞态改由 finishAfterDeath 的显式次序确定
	// 性复现。
	newDead := func(m *Manager, addr string) *managedConn {
		nc, peer := net.Pipe()
		t.Cleanup(func() { nc.Close(); peer.Close() })
		mc := &managedConn{addr: addr, ready: make(chan struct{})}
		mc.c = NewConn(nc, m.cfg, &connHandler{m: m, mc: mc, user: m.handler}, m.pool, addr != "")
		m.mu.Lock()
		m.conns[mc] = struct{}{}
		if addr != "" {
			m.byAddr[addr] = mc
		}
		m.mu.Unlock()
		return mc
	}
	// finishAfterDeath 复现生产行文：关闭先赢（ready=false 时刻的 shutdown，
	// Lost 被跳过），握手随后完成并走生产回调路径（finishHandshake 尾调用）。
	finishAfterDeath := func(mc *managedConn, peer rans.NodeID) {
		mc.c.Close()                  // shutdown 赢 closeOnce：此刻 ready=false，不报 Lost
		mc.c.peer.Store(uint32(peer)) // finishHandshake 事后完成：peer → ready
		mc.c.ready.Store(true)
		mc.c.handler.OnHandshakeComplete(peer)
	}

	t.Run("listener side conns cleaned", func(t *testing.T) {
		m, _ := newManager(t, 1)
		mc := newDead(m, "") // listener 接入侧：不进 byAddr，无自愈路径
		finishAfterDeath(mc, 42)
		m.mu.Lock()
		_, inConns := m.conns[mc]
		m.mu.Unlock()
		if inConns {
			t.Fatal("dead conn must be removed from conns (listener side has no self-heal path)")
		}
		select {
		case <-mc.ready:
		default:
			t.Fatal("ready channel must be closed to release SendToNode waiters")
		}
		if m.ConnToNode(42) != nil {
			t.Fatal("dead conn must not be registered in byNode")
		}
	})

	t.Run("dialer side conns and byAddr cleaned", func(t *testing.T) {
		m, _ := newManager(t, 1)
		mc := newDead(m, "10.0.0.9:7000")
		finishAfterDeath(mc, 42)
		m.mu.Lock()
		_, inConns := m.conns[mc]
		_, inByAddr := m.byAddr["10.0.0.9:7000"]
		m.mu.Unlock()
		if inConns || inByAddr {
			t.Fatalf("dead conn must be removed from conns and byAddr: inConns=%v inByAddr=%v", inConns, inByAddr)
		}
	})

	t.Run("byAddr identity guard keeps replacement", func(t *testing.T) {
		m, _ := newManager(t, 1)
		dead := newDead(m, "10.0.0.9:7000")
		dead.c.Close()
		// getOrConnect 死检查先行完成重建：byAddr 已指向替代连接
		repl := newDead(m, "10.0.0.9:7000")
		dead.c.peer.Store(42)
		dead.c.ready.Store(true)
		dead.c.handler.OnHandshakeComplete(42)
		m.mu.Lock()
		cur, ok := m.byAddr["10.0.0.9:7000"]
		_, deadInConns := m.conns[dead]
		_, replInConns := m.conns[repl]
		m.mu.Unlock()
		if !ok || cur != repl {
			t.Fatal("identity guard must keep the replacement conn in byAddr")
		}
		if deadInConns {
			t.Fatal("dead conn must be removed from conns")
		}
		if !replInConns {
			t.Fatal("replacement conn must stay in conns")
		}
	})
}

// failClosedCfg 测试构造：无密钥且未 opt-in（fail-closed 默认形态）。
func failClosedCfg(node rans.NodeID) Config {
	cfg := baseCfg()
	cfg.NodeID = node
	cfg.NoHMAC = false // baseCfg 为迁移后的显式 opt-in，此处还原默认拒
	return cfg
}

// 9. fail-closed 拨号侧：无密钥未 opt-in → GetOrConnect 即刻带因失败
// （"HMAC required"），死连接不进 byAddr 缓存。
func TestManagerFailClosedDialError(t *testing.T) {
	// 对端：显式 NoHMAC 的正常监听者（隔离被测面：拒因在本端配置）
	peer, _ := newManager(t, 1)
	addr, err := peer.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("peer listen: %v", err)
	}
	m := NewManager(failClosedCfg(2), newFakeHandler(), nil)
	t.Cleanup(m.Close)
	_, err = m.GetOrConnect(t.Context(), addr)
	if err == nil || !strings.Contains(err.Error(), "HMAC required") {
		t.Fatalf("dial err=%v want 'HMAC required'", err)
	}
	m.mu.Lock()
	cached := len(m.byAddr)
	m.mu.Unlock()
	if cached != 0 {
		t.Fatalf("refused conn must not be cached, byAddr=%d", cached)
	}
}

// 10. fail-closed 监听侧：无密钥未 opt-in → 接入连接即刻关闭（对端观察
// 断连），本端 Warn 留痕含 HMAC required（本地配置事故，非静默错位）。
func TestManagerFailClosedListenSideRefuses(t *testing.T) {
	sink := captureLog(t)
	h := newFakeHandler()
	m := NewManager(failClosedCfg(1), h, nil)
	t.Cleanup(m.Close)
	addr, err := m.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	t.Cleanup(func() { raw.Close() })
	hs := wire.Handshake{NodeID: 2, ClusterName: baseCfg().ClusterName, SchemaDigest: baseCfg().SchemaDigest}
	if _, err := raw.Write(rawFrame(t, wire.Header{Type: protocol.MsgHandshake}, hs.Encode(nil))); err != nil {
		t.Fatalf("raw handshake write: %v", err)
	}
	if _, err := raw.Read(make([]byte, 16)); err == nil {
		t.Fatal("listener must close the connection without any reply")
	}
	// 本端 Warn 留痕（异步：接入后 Start 内门闸触发）
	deadline := time.Now().Add(time.Second)
	for sink.count("HMAC required") == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if sink.count("HMAC required") == 0 {
		t.Fatal("listen side must warn with 'HMAC required'")
	}
	expectNoEvent(t, h.handshake)
}

// ---- conns 表生命周期收尸（Done 驱动）----
//
// 不变量：连接终态（Done 关闭）⟹ conns 表项移除，与死亡形态无关。
// 收尸覆盖三类无其他清表点的死亡形态——拨号侧到手即死（fail-closed）、
// 拨号侧握手超时僵尸（无人重拨则滞留）、接入侧半开（永久滞留，外部可
// 无界积累表项）。

// pollUntil 有界轮询（收尸异步于 Done，测试不得依赖即时性）。
func pollUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s not satisfied within 2s", what)
}

func connsLen(m *Manager) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.conns)
}

func byAddrLen(m *Manager) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.byAddr)
}

// 拨号侧到手即死（fail-closed 本地拒绝）：错误返回后 conns 不得滞留。
func TestFailClosedDialLeavesNoConnEntry(t *testing.T) {
	peer, _ := newManager(t, 1)
	addr, err := peer.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("peer listen: %v", err)
	}
	m := NewManager(failClosedCfg(2), newFakeHandler(), nil)
	t.Cleanup(m.Close)
	if _, err := m.GetOrConnect(t.Context(), addr); err == nil {
		t.Fatal("fail-closed config must reject the dial")
	}
	pollUntil(t, "dead-on-arrival dial conn must leave m.conns", func() bool {
		return connsLen(m) == 0
	})
}

// 接入侧半开：对端连接后不交握手、超时死亡（非就绪无 Lost 上报），
// conns 必须仍被清理——外部明文探测/慢握手不得无界积累表项。
func TestListenerHalfOpenConnReapedOnDone(t *testing.T) {
	cfg := baseCfg()
	cfg.NodeID = 1
	cfg.HandshakeTimeout = 100 * time.Millisecond
	m := NewManager(cfg, newFakeHandler(), nil)
	t.Cleanup(m.Close)
	addr, err := m.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	t.Cleanup(func() { raw.Close() })
	// 不发任何字节：hsTimer 到点 shutdown（未就绪 → 无 Lost、无 connLost）。
	// 先等注册出现（accept 异步于拨号），再等收尸——否则首查 0 项即空转通过。
	pollUntil(t, "half-open listener conn must be registered", func() bool {
		return connsLen(m) == 1
	})
	pollUntil(t, "half-open listener conn must be reaped from m.conns", func() bool {
		return connsLen(m) == 0
	})
}

// 拨号侧握手超时僵尸：拨号成功、握手无响应而亡，且此后无人重拨——
// conns 与 byAddr 都必须自清（无人重拨的地址同样不滞留）。
func TestDialHandshakeTimeoutZombieReapedWithoutRedial(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("raw listen: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	go func() { // 只接不读不握手
		for {
			nc, err := l.Accept()
			if err != nil {
				return
			}
			_ = nc
		}
	}()
	cfg := baseCfg()
	cfg.NodeID = 1
	cfg.HandshakeTimeout = 100 * time.Millisecond
	m := NewManager(cfg, newFakeHandler(), nil)
	t.Cleanup(m.Close)
	if _, err := m.GetOrConnect(t.Context(), l.Addr().String()); err != nil {
		t.Fatalf("get or connect: %v", err)
	}
	pollUntil(t, "handshake-timeout zombie must leave m.conns and m.byAddr", func() bool {
		return connsLen(m) == 0 && byAddrLen(m) == 0
	})
}

// ---- fail-closed 门闸前置于拨号/TLS 握手 ----
// 本地纯判定（未配密钥且未显式 NoHMAC），应在发出任何字节前拒绝：
// 拨号侧不得发起 TCP/TLS 握手，接入侧不得把连接按到握手超时。

// 拨号侧：门闸先于拨号——注入的拨号钩子不得被调用（零字节拒绝）。
func TestFailClosedGatePrecedesDial(t *testing.T) {
	cfg := failClosedCfg(1)
	var dialed atomic.Bool
	cfg.Dial = func(ctx context.Context, addr string) (net.Conn, error) {
		dialed.Store(true)
		c, s := net.Pipe()
		t.Cleanup(func() { s.Close() })
		return c, nil
	}
	m := NewManager(cfg, newFakeHandler(), nil)
	t.Cleanup(m.Close)
	_, err := m.GetOrConnect(t.Context(), "gate-test")
	if err == nil || !strings.Contains(err.Error(), "HMAC required") {
		t.Fatalf("dial err=%v want 'HMAC required'", err)
	}
	if dialed.Load() {
		t.Fatal("fail-closed gate must reject before dialing (zero bytes)")
	}
}

// 接入侧：门闸先于 TLS 握手——静默探测者即刻被关，不等
// HandshakeTimeout（baseCfg=1s）的握手超时再关。
func TestFailClosedGatePrecedesTLSHandshakeOnAccept(t *testing.T) {
	mat := newTLSMaterial(t)
	cfg := failClosedCfg(1)
	cfg.TLS = mat.server
	m := NewManager(cfg, newFakeHandler(), nil)
	t.Cleanup(m.Close)
	addr, err := m.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	t.Cleanup(func() { raw.Close() })
	if err := raw.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := raw.Read(make([]byte, 64)); !errors.Is(err, io.EOF) {
		t.Fatalf("listener must close the fail-closed conn before handshake timeout, read err=%v", err)
	}
}

// TestSimultaneousBidirectionalConnectConverges（双向首接触收敛回归）：
// 两节点同时互拨（高负载/并行全量下常见——调用方与承接方首次接触互为对端），
// 两侧 Manager 各自出现同节点双连接。收敛必须满足三条契约：
//  1. 握手完成后立即互发的帧全部到达——Send 已返回成功（入队）的帧不得
//     因连接被顶替而静默丢失；
//  2. 不误报 OnConnectionLost——对端仍有存活连接，误报会触发上层
//     FailPendingForNode 杀死在途非幂等请求；
//  3. 战况平息后双向发送仍可达（收敛到同一条存活连接）。
//
// 反例锚定（低号发起规则钉死的契约）：「新顶替旧 + 静默关旧」的非确定性
// 解冲突在坏序下双侧存活连接各落在对方已关 socket 上（相互断连），或被
// 顶替连接上已入队未冲刷的帧被丢弃。
func TestSimultaneousBidirectionalConnectConverges(t *testing.T) {
	const framesPerSide = 4

	mA, hA := newManager(t, 1)
	addrA, err := mA.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen A: %v", err)
	}
	mB, hB := newManager(t, 2)
	addrB, err := mB.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen B: %v", err)
	}
	resA := func(_ context.Context, _ rans.NodeID) (string, error) { return addrB, nil }
	resB := func(_ context.Context, _ rans.NodeID) (string, error) { return addrA, nil }
	ctx := t.Context()

	// 同时互拨（双向首接触）。
	start := make(chan struct{})
	var dialWG sync.WaitGroup
	dialWG.Add(2)
	go func() {
		defer dialWG.Done()
		<-start
		if _, err := mA.GetOrConnect(ctx, addrB); err != nil {
			t.Errorf("A dial: %v", err)
		}
	}()
	go func() {
		defer dialWG.Done()
		<-start
		if _, err := mB.GetOrConnect(ctx, addrA); err != nil {
			t.Errorf("B dial: %v", err)
		}
	}()
	close(start)
	dialWG.Wait()

	// 握手完成即互发。调用方契约语义（与网关 sendWithRetry 同口径）：收敛期
	// 的瞬时发送失败（退役连接 ErrRetiring / 败者连接就绪前被退役的终态错）
	// 有界重试，重试经 byNode 落到存活连接；断言面是「Send 成功返回的帧
	// 必达」——入队成功后被顶替静默丢弃是本契约的红线。
	send := func(m *Manager, peer rans.NodeID, res func(context.Context, rans.NodeID) (string, error), src uint16) {
		for i := 0; i < framesPerSide; i++ {
			hdr := wire.Header{Type: protocol.MsgOneWay, MessageID: uint64(i + 1), SourceNodeID: src}
			for attempt := 0; ; attempt++ {
				err := m.SendToNode(ctx, peer, res, hdr, nil)
				if err == nil {
					break
				}
				if attempt >= 10 {
					t.Errorf("node %d send #%d: %v", src, i, err)
					break
				}
				time.Sleep(5 * time.Millisecond) // 瞬时错误的重试退避（非承重等待）
			}
		}
	}
	var sendWG sync.WaitGroup
	sendWG.Add(2)
	go func() { defer sendWG.Done(); send(mA, 2, resA, 1) }()
	go func() { defer sendWG.Done(); send(mB, 1, resB, 2) }()
	sendWG.Wait()

	// 契约 1：2N 帧全部到达（有界等待）。
	waitFrames := func(h *fakeHandler, want int, who string) {
		deadline := time.After(2 * time.Second)
		for {
			select {
			case <-h.got:
				want--
				if want == 0 {
					return
				}
			case <-deadline:
				t.Fatalf("%s: frames lost on superseded conn — %d of %d undelivered", who, want, framesPerSide)
				return
			}
		}
	}
	waitFrames(hA, framesPerSide, "A")
	waitFrames(hB, framesPerSide, "B")

	// 契约 3：平息后双向仍可达（各自再发一帧探活，经收敛后的存活连接；
	// 瞬时 ErrRetiring 走调用方契约有界重试——与 send 助手同口径，收敛期
	// 撞上退役中连接是预期形态非失败）。
	probe := func(m *Manager, peer rans.NodeID, res func(context.Context, rans.NodeID) (string, error), src uint16) {
		hdr := wire.Header{Type: protocol.MsgOneWay, MessageID: 999, SourceNodeID: src}
		for attempt := 0; ; attempt++ {
			err := m.SendToNode(ctx, peer, res, hdr, nil)
			if err == nil {
				return
			}
			if attempt >= 10 {
				t.Errorf("node %d post-war probe: %v", src, err)
				return
			}
			time.Sleep(5 * time.Millisecond) // 瞬时错误的重试退避（非承重等待）
		}
	}
	probe(mA, 2, resA, 1)
	probe(mB, 1, resB, 2)
	// 5s 有界等待：-race/并行争用下对端读循环可能被调度饥饿数秒（帧在
	// 内核缓冲、协议未丢）——等待上界区分「慢」与「失联」，非承重 sleep。
	deadline := time.After(5 * time.Second)
	for want := 2; want > 0; {
		select {
		case <-hA.got:
			want--
		case <-hB.got:
			want--
		case <-deadline:
			t.Fatalf("post-war connectivity broken (no mutual live conn), %d probe frames undelivered", want)
			return
		}
	}

	// 契约 2：对端仍有存活连接期间不误报断连（非阻塞复核，承重断言在
	// 契约 1/3 的可达性上）。
	expectNoEvent(t, hA.lost)
	expectNoEvent(t, hB.lost)
}

// TestRetirePipeDrainsQueuedFrames（退役收尾×net.Pipe 形态回归）：net.Pipe
// 无 CloseWrite（探针实证：方法集只有 SetReadDeadline）——退役走「跳过半关
// + 读期限收口」路径。钉死：Send 已返回成功（入队）的帧在 Retire 后全部
// 必达（writeLoop 退役排空），连接在有界时间内收口（非永久滞留）。
func TestRetirePipeDrainsQueuedFrames(t *testing.T) {
	ca, cb, ha, hb, _ := pipePair(t, nil) // NoHMAC 形态（secret nil）
	defer ca.Close()
	defer cb.Close()
	waitHandshake(t, hb, 1) // cb 见到握手（Ack 已回）
	waitHandshake(t, ha, 2) // ca 收到 Ack（就绪，Send 门禁放行）

	const n = 16
	for i := 0; i < n; i++ {
		hdr := wire.Header{Type: protocol.MsgOneWay, MessageID: uint64(i + 1), SourceNodeID: 1}
		if err := ca.Send(context.Background(), hdr, []byte("frame")); err != nil {
			t.Fatalf("send #%d: %v", i, err)
		}
	}
	ca.Retire() // 立即退役：已入队帧必须经退役排空冲刷必达

	// cb 侧全部收到（pipe 同步：Send 返回即已写出/在队，Retire 后排空写出）。
	deadline := time.After(2 * time.Second)
	for want := n; want > 0; want-- {
		select {
		case <-hb.got:
		case <-deadline:
			t.Fatalf("retire drain lost frames: %d of %d undelivered", want, n)
		}
	}
	// 有界收口：Done 在 retireLinger（读静默上界）内关闭。cb 关闭 → ca 读侧
	// EOF → 正常 shutdown；不依赖 linger 兜底也能收口。
	cb.Close()
	select {
	case <-ca.Done():
	case <-time.After(retireLinger + time.Second):
		t.Fatal("retired pipe conn must close within the bounded linger")
	}
}

// gateHandler 握手/断连入 channel（同 fakeHandler）；HandleMessage 停在
// 放行闸上——把对端读循环按住在「已读一帧、分发中」的背压形态。
type gateHandler struct {
	handshake chan rans.NodeID
	lost      chan rans.NodeID
	gate      chan struct{}
	mu        sync.Mutex
	got       int
}

func newGateHandler() *gateHandler {
	return &gateHandler{handshake: make(chan rans.NodeID, 4), lost: make(chan rans.NodeID, 4), gate: make(chan struct{})}
}

func (h *gateHandler) HandleMessage(wire.Header, []byte) {
	h.mu.Lock()
	h.got++
	h.mu.Unlock()
	<-h.gate
}
func (h *gateHandler) OnHandshakeComplete(n rans.NodeID) { h.handshake <- n }
func (h *gateHandler) OnConnectionLost(n rans.NodeID)    { h.lost <- n }

func (h *gateHandler) received() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.got
}

// waitGateHandshake gateHandler 的握手确认（裸 channel 版 waitHandshake）。
func waitGateHandshake(t *testing.T, ch chan rans.NodeID, want rans.NodeID) {
	t.Helper()
	select {
	case n := <-ch:
		if n != want {
			t.Fatalf("handshake peer=%d want %d", n, want)
		}
	case <-time.After(time.Second):
		t.Fatal("handshake not completed")
	}
}

// TestRetireUnderBackpressureNoDeadlock（退役协议活性×满队背压，并发回归）：
// 出站队列满载（读侧被闸停）时并发 Retire——Send 的互斥入队路径持有
// retireMu 阻塞在满队上，Retire 不得死锁（writeLoop 排空不取该锁，放行
// 读侧后链式解阻塞）。发送方契约：返回 nil 的帧必达；Retire 封队后的
// Send 得 ErrRetiring（可重试），绝不悬挂。记账用原子计数 + WaitGroup：
// 背压编排只读计数不消费结果（消费即丢账——首版测试的教训）。
func TestRetireUnderBackpressureNoDeadlock(t *testing.T) {
	cfgA := Config{NodeID: 1, ClusterName: "t", NoHMAC: true, SendQueueDepth: 2, HandshakeTimeout: time.Second}
	cfgB := cfgA
	cfgB.NodeID = 2
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	pool := wire.NewPagePool()
	hb := newGateHandler()
	ha := newFakeHandler()
	ca := NewConn(c1, cfgA, ha, pool, true)
	cb := NewConn(c2, cfgB, hb, pool, false)
	ca.Start()
	cb.Start()
	t.Cleanup(func() { _ = ca.Close(); _ = cb.Close() })
	waitGateHandshake(t, hb.handshake, 1) // cb 见握手（Ack 已回）
	waitHandshake(t, ha, 2)               // ca 收到 Ack（就绪，Send 门禁放行）

	const k = 6
	var (
		wg       sync.WaitGroup
		errs     = make([]error, k)
		returned atomic.Int32
	)
	for i := 0; i < k; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hdr := wire.Header{Type: protocol.MsgOneWay, MessageID: uint64(i + 1), SourceNodeID: 1}
			// 32KB 帧体：6 帧超过写缓冲（64KiB）+ 管道在途——读侧闸停后
			// out 通道必然满载，后续发送方挂起在互斥入队路径上。
			errs[i] = ca.Send(context.Background(), hdr, make([]byte, 32<<10))
			returned.Add(1) // Send 收场后才计数（背压判据是「已返回数」停滞）
		}(i)
	}
	// 编排确认背压生效：返回数稳定低于 k（读侧闸停、队列满、部分发送方
	// 挂起）——只读计数，有界等待，非承重断言。
	backpressure := false
	for probe := 0; probe < 10 && !backpressure; probe++ {
		prev := returned.Load()
		time.Sleep(100 * time.Millisecond)
		if now := returned.Load(); now == prev && now < k {
			backpressure = true // 返回数停滞且低于 k：队列满、余量发送方挂起
		}
	}
	if !backpressure {
		t.Fatalf("orchestration: backpressure not observed (returned=%d of %d)", returned.Load(), k)
	}

	retireDone := make(chan struct{})
	go func() { ca.Retire(); close(retireDone) }()
	select {
	case <-retireDone:
		// Retire 抢先完成（封队先于发送方持锁）：活性行列同断言二覆盖
	case <-time.After(300 * time.Millisecond):
		// Retire 阻塞在 retireMu（发送方持锁阻塞满队）——放行后必须解阻塞
	}
	close(hb.gate) // 放行读侧：背压链式解除

	allDone := make(chan struct{})
	go func() { wg.Wait(); close(allDone) }()
	select {
	case <-allDone:
	case <-time.After(5 * time.Second):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("sender hung under retire+backpressure (deadlock)\n\n%s", buf[:n])
	}
	// 断言一：全部发送方收场且错误面仅 nil / ErrRetiring。
	var nils, retiring, others int
	for _, err := range errs {
		switch {
		case err == nil:
			nils++
		case errors.Is(err, ErrRetiring):
			retiring++
		default:
			others++
		}
	}
	if others > 0 {
		for i, err := range errs {
			if err != nil && !errors.Is(err, ErrRetiring) {
				t.Logf("sender %d: %v", i, err)
			}
		}
		t.Fatalf("senders must fail with ErrRetiring at most, got %d other errors", others)
	}
	// 断言二：Retire 有界返回（活性核心——满队发送方持 retireMu 阻塞时
	// Retire 不得死锁）。
	select {
	case <-retireDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Retire deadlocked against backpressured senders")
	}
	// 断言三：零丢失——返回 nil 的帧最终全部到达（排空冲刷必达）。B 侧对
	// 已缓冲帧的分发异步进行，有界轮询收敛而非瞬时采样。
	dl := time.After(5 * time.Second)
	for {
		if got := hb.received(); got >= nils {
			break
		}
		select {
		case <-dl:
			t.Fatalf("delivered %d frames, but %d sends returned nil (frame loss on retire drain)", hb.received(), nils)
		case <-time.After(10 * time.Millisecond):
		}
	}
	// 断言四：连接有界收口。
	cb.Close()
	select {
	case <-ca.Done():
	case <-time.After(retireLinger + 3*time.Second):
		t.Fatal("retired conn must settle within bounded linger")
	}
}

// TestManagerHandshakePoolAccounting：完整握手对的池收支
// 必须平衡——握手体经页池拷入，与 writeFrame 的无条件 Put 收支相抵，
// 不给管理器池白记 Put（Gets==Puts 的漏还检测锚点不受稀释）。
func TestManagerHandshakePoolAccounting(t *testing.T) {
	pool := wire.NewPagePool()
	cfg := baseCfg()
	cfg.NodeID = 1
	mA := NewManager(cfg, newFakeHandler(), pool)
	cfgB := cfg
	cfgB.NodeID = 2
	mB := NewManager(cfgB, newFakeHandler(), pool)
	t.Cleanup(func() { mA.Close(); mB.Close() })

	addr, err := mA.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if _, err := mB.GetOrConnect(t.Context(), addr); err != nil {
		t.Fatalf("connect: %v", err)
	}
	// 等握手双双完成（Handshake + HandshakeAck 两个方向都已写出）。
	deadline := time.Now().Add(3 * time.Second)
	for (mA.ConnToNode(2) == nil || mB.ConnToNode(1) == nil) && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if mA.ConnToNode(2) == nil || mB.ConnToNode(1) == nil {
		t.Fatal("handshake did not complete on both sides")
	}
	// ping/pong 一轮（空载 body，不占池）——排除其它流量干扰后断言收支。
	if err := mA.SendToNode(t.Context(), 2, nil, wire.Header{Type: protocol.MsgPing}, nil); err != nil {
		t.Fatalf("ping: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	mA.Close()
	mB.Close()
	time.Sleep(50 * time.Millisecond) // drainOut 收尾（关闭路径的页归还）

	st := pool.Stats()
	if st.Gets != st.Puts {
		t.Fatalf("pool accounting after handshake pair: gets=%d puts=%d (want equal)", st.Gets, st.Puts)
	}
}
