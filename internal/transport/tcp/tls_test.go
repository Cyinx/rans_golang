// TLS 传输测试（规格 §10.1 / §19 陷阱 13）：认证先于管道 IO——TLS 握手
// 在帧循环启动前完成，明文字节不进 FrameDecoder。证书全部测试内自签
// （crypto/x509 + crypto/ecdsa），不依赖外部文件。
package tcp

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// testCA 测试内自签 CA（ECDSA P-256）。
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

// newTestCA 生成一把自签 CA。serial 须每次相异（证书序列号唯一性）。
func newTestCA(t *testing.T, serial int64, cn string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca cert: %v", err)
	}
	return &testCA{cert: cert, key: key}
}

// pool 该 CA 的信任池（客户端 RootCAs 用）。
func (ca *testCA) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(ca.cert)
	return p
}

// issueServerCert 由该 CA 签发一张服务器证书（127.0.0.1/::1/localhost SAN）。
func (ca *testCA) issueServerCert(t *testing.T, serial int64) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "rans-test-node"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse server cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// tlsMaterial 一套测试 PKI：正 CA（签发服务器证书）+ 误 CA（客户端误信池，
// 令证书校验必败）。
type tlsMaterial struct {
	server    *tls.Config // 监听侧：证书由正 CA 签发
	client    *tls.Config // 拨号侧：信任正 CA
	badClient *tls.Config // 拨号侧：信任误 CA——服务器证书校验必败
}

var testTLSVersion uint16 = tls.VersionTLS13

func newTLSMaterial(t *testing.T) *tlsMaterial {
	t.Helper()
	ca := newTestCA(t, 101, "rans-test-ca")
	other := newTestCA(t, 102, "rans-test-other-ca")
	return &tlsMaterial{
		server: &tls.Config{
			Certificates: []tls.Certificate{ca.issueServerCert(t, 201)},
			MinVersion:   testTLSVersion,
		},
		client: &tls.Config{
			RootCAs:    ca.pool(),
			ServerName: "127.0.0.1",
			MinVersion: testTLSVersion,
		},
		badClient: &tls.Config{
			RootCAs:    other.pool(),
			ServerName: "127.0.0.1",
			MinVersion: testTLSVersion,
		},
	}
}

// tlsManager 带 TLS 配置的 Manager 测试构造（统一 Cleanup 关闭）。
func tlsManager(t *testing.T, node rans.NodeID, tlsCfg *tls.Config) (*Manager, *fakeHandler) {
	t.Helper()
	cfg := baseCfg()
	cfg.NodeID = node
	cfg.TLS = tlsCfg
	h := newFakeHandler()
	m := NewManager(cfg, h, nil)
	t.Cleanup(func() { m.Close() })
	return m, h
}

// 1. TLS 双端握手成功 + 应用消息往返 + ready/peer 事件（spec §10.1）。
func TestTLSPairHandshakeAndRoundTrip(t *testing.T) {
	mat := newTLSMaterial(t)
	ms, hs := tlsManager(t, 1, mat.server)
	addr, err := ms.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	mc, hc := tlsManager(t, 2, mat.client)
	if _, err := mc.GetOrConnect(t.Context(), addr); err != nil {
		t.Fatalf("tls get or connect: %v", err)
	}
	waitHandshake(t, hc, 1)
	waitHandshake(t, hs, 2)

	c := mc.ConnToNode(1)
	if c == nil || !c.Ready() {
		t.Fatal("client conn to node 1 must be ready")
	}
	if n, ok := c.PeerNodeID(); !ok || n != 1 {
		t.Fatalf("client peer=%d ok=%v want 1", n, ok)
	}
	if s := ms.ConnToNode(2); s == nil || !s.Ready() {
		t.Fatal("server conn to node 2 must be ready")
	}

	// 正向往返：client → server，header 条件字段与 body 完整到达
	if err := c.Send(context.Background(), wire.Header{
		Type:         protocol.MsgRequest,
		MessageID:    77,
		Flags:        protocol.FlagHasSourceNodeID,
		SourceNodeID: 2,
	}, []byte("tls round trip")); err != nil {
		t.Fatalf("send over tls: %v", err)
	}
	select {
	case m := <-hs.got:
		if m.hdr.Type != protocol.MsgRequest || m.hdr.MessageID != 77 || m.hdr.SourceNodeID != 2 {
			t.Fatalf("header round trip mismatch: %+v", m.hdr)
		}
		if string(m.body) != "tls round trip" {
			t.Fatalf("body=%q want %q", m.body, "tls round trip")
		}
	case <-time.After(time.Second):
		t.Fatal("app message not delivered over tls")
	}

	// 反向：server → client
	if err := ms.ConnToNode(2).Send(context.Background(), wire.Header{Type: protocol.MsgResponse, MessageID: 78}, []byte("pong data")); err != nil {
		t.Fatalf("send back over tls: %v", err)
	}
	select {
	case m := <-hc.got:
		if m.hdr.Type != protocol.MsgResponse || m.hdr.MessageID != 78 || string(m.body) != "pong data" {
			t.Fatalf("reverse round trip mismatch: %+v %q", m.hdr, m.body)
		}
	case <-time.After(time.Second):
		t.Fatal("reverse message not delivered over tls")
	}
}

// 2. 陷阱 13 锁（明文探测）：裸明文客户端直写帧形态字节（无 ClientHello）
// → TLS 握手失败、连接被关、服务端零事件、不回任何可解码帧。
func TestTLSPlaintextProbeRejected(t *testing.T) {
	mat := newTLSMaterial(t)
	ms, h := tlsManager(t, 1, mat.server)
	addr, err := ms.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("plaintext probe dial: %v", err)
	}
	t.Cleanup(func() { raw.Close() })

	// pre-auth 应用消息形态的明文帧：若帧层先于 TLS 认证启动，这条会
	// 命中 pre-auth 门禁——陷阱 13 要求它在 TLS 层即被终结
	frame := rawFrame(t, wire.Header{Type: protocol.MsgRequest, MessageID: 9}, []byte("plaintext probe"))
	if _, err := raw.Write(frame); err != nil {
		t.Fatalf("write plaintext probe: %v", err)
	}

	// 服务端必须关连接：读尽回向字节直至收尾错误
	if err := raw.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var echoed []byte
	buf := make([]byte, 4096)
	for {
		n, err := raw.Read(buf)
		echoed = append(echoed, buf[:n]...)
		if err != nil {
			break // EOF/RST：连接已被服务端关闭
		}
	}
	// 静默错位门闸：不回任何可解码帧（零字节或仅 TLS alert 残片）
	dec := wire.NewFrameDecoder(bytes.NewReader(echoed), wire.NewPagePool())
	if _, _, err := dec.Next(); err == nil {
		t.Fatalf("plaintext probe must not be answered with a decodable frame (%d bytes)", len(echoed))
	}
	// 服务端零事件：握手未完成、消息未入管道、不报 Lost
	expectNoMessage(t, h)
	expectNoEvent(t, h.handshake)
	expectNoEvent(t, h.lost)
}

// 3. 陷阱 13 锁（错证书）：客户端误信 CA 池 → 证书校验失败 → 拨号快速
// 失败（帧循环从未启动），服务端零事件。
func TestTLSClientCertVerificationFails(t *testing.T) {
	mat := newTLSMaterial(t)
	ms, hs := tlsManager(t, 1, mat.server)
	addr, err := ms.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	mc, _ := tlsManager(t, 2, mat.badClient)

	start := time.Now()
	if _, err := mc.GetOrConnect(t.Context(), addr); err == nil {
		t.Fatal("dial with untrusted server cert must fail")
	}
	// 快速失败：证书校验在往返内完成，远早于握手超时（baseCfg=1s）
	if elapsed := time.Since(start); elapsed > 800*time.Millisecond {
		t.Fatalf("cert verification must fail fast, took %v", elapsed)
	}
	expectNoMessage(t, hs)
	expectNoEvent(t, hs.handshake)
	expectNoEvent(t, hs.lost)
}

// 4. 本端配 TLS 而对端明文 → 拨号失败（ClientHello 对明文服务端是垃圾
// 帧，服务端关闭；客户端 TLS 握手拿不到 ServerHello 即失败）。零帧进入
// 任何一侧的帧循环。
func TestTLSDialToPlaintextServerFails(t *testing.T) {
	mat := newTLSMaterial(t)
	mp, hp := newManager(t, 1) // 明文服务端（TLS=nil，现状形态）
	addr, err := mp.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("plaintext listen: %v", err)
	}
	mc, _ := tlsManager(t, 2, mat.client)
	if _, err := mc.GetOrConnect(t.Context(), addr); err == nil {
		t.Fatal("tls dial to plaintext server must fail")
	}
	// 明文服务端零事件（垃圾帧在解码/pre-auth 层即终结，不进消息路径）
	expectNoMessage(t, hp)
	expectNoEvent(t, hp.handshake)
	expectNoEvent(t, hp.lost)
}

// 5. WrapConn × TLS 组合：握手先于包装。包装连接嵌 net.Conn 接口——
// HandshakeContext 不随接口
// 嵌入提升，Conn.Start 的握手探测对包装后连接失效；「握手先于帧循环」
// 的结构性保证由 Manager 落实：startConn 在应用包装之前对裸 *tls.Conn
// 显式握手。可观测断言：包装钩子触发时刻监听侧连接的握手必须已完成
// （*tls.Conn.HandshakeComplete），且包装后连接照常往返（身份包装只观测
// 不改变语义，包装层只见 TLS 记录流）。
func TestTLSWrapConnHandshakeBeforeWrap(t *testing.T) {
	mat := newTLSMaterial(t)

	var wraps atomic.Int32
	completeAtWrap := make(chan bool, 8) // 每次包装时刻的握手完成态
	cfg := baseCfg()
	cfg.NodeID = 1
	cfg.TLS = mat.server
	cfg.WrapConn = func(nc net.Conn) net.Conn {
		done := false
		if tc, ok := nc.(*tls.Conn); ok {
			done = tc.ConnectionState().HandshakeComplete
		}
		completeAtWrap <- done
		wraps.Add(1)
		return nc // 身份包装：仅观测次序
	}
	hs := newFakeHandler()
	ms := NewManager(cfg, hs, nil)
	t.Cleanup(ms.Close)

	addr, err := ms.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	mc, hc := tlsManager(t, 2, mat.client)
	if _, err := mc.GetOrConnect(t.Context(), addr); err != nil {
		t.Fatalf("tls get or connect: %v", err)
	}
	waitHandshake(t, hs, 2)
	waitHandshake(t, hc, 1)

	// 包装已被应用（每次接入恰一次），且每次包装时刻握手均已完成。
	n := wraps.Load()
	if n < 1 {
		t.Fatalf("wrap hook must be applied at least once, got %d", n)
	}
	for i := 0; i < int(n); i++ {
		select {
		case done := <-completeAtWrap:
			if !done {
				t.Fatal("wrap must observe a completed TLS handshake (handshake-before-wrap order)")
			}
		case <-time.After(time.Second):
			t.Fatal("wrap record missing")
		}
	}

	// 包装后的连接照常工作：应用消息往返（经包装层传输 TLS 记录）。
	c := mc.ConnToNode(1)
	if c == nil || !c.Ready() {
		t.Fatal("client conn to node 1 must be ready")
	}
	if err := c.Send(context.Background(), wire.Header{Type: protocol.MsgRequest, MessageID: 80}, []byte("wrapped")); err != nil {
		t.Fatalf("send over wrapped tls conn: %v", err)
	}
	select {
	case m := <-hs.got:
		if m.hdr.Type != protocol.MsgRequest || m.hdr.MessageID != 80 || string(m.body) != "wrapped" {
			t.Fatalf("wrapped round trip mismatch: %+v %q", m.hdr, m.body)
		}
	case <-time.After(time.Second):
		t.Fatal("app message not delivered over wrapped tls conn")
	}
}

// 6. accept 吞吐解耦（决策出处：
// docs/decisions/2026-09-30-phase6-security.md）：慢 TLS 握手（slowloris
// 形态——接入后不交 ClientHello）不得阻塞 accept 循环：后续合法 TLS 接入
// 必须即刻完成握手，不等首个僵死握手的 HandshakeTimeout（baseCfg=1s）。
func TestTLSSlowHandshakeDoesNotBlockAccept(t *testing.T) {
	mat := newTLSMaterial(t)
	ms, _ := tlsManager(t, 1, mat.server)
	addr, err := ms.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	// 僵死探测者：只完成 TCP 连接、不发送任何字节——服务端接入路径的
	// ensureHandshake 停驻至超时
	prober, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("prober dial: %v", err)
	}
	t.Cleanup(func() { prober.Close() })

	mb, hb := tlsManager(t, 2, mat.client)
	start := time.Now()
	if _, err := mb.GetOrConnect(t.Context(), addr); err != nil {
		t.Fatalf("legit tls dial: %v", err)
	}
	waitHandshake(t, hb, 1)
	if elapsed := time.Since(start); elapsed > 700*time.Millisecond {
		t.Fatalf("legit handshake blocked %v behind stalled prober", elapsed)
	}
}

// 7. 接入侧 TLS 握手失败留痕：明文探测 TLS 端口必须
// 可观测——Warn 记录远端与原因（静默错位门闸辖「不回错帧」，不辖本地日志）。
func TestTLSHandshakeFailureOnAcceptIsLogged(t *testing.T) {
	sink := captureLog(t)
	mat := newTLSMaterial(t)
	ms, _ := tlsManager(t, 1, mat.server)
	addr, err := ms.Listen(t.Context(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("plaintext probe dial: %v", err)
	}
	t.Cleanup(func() { raw.Close() })
	frame := rawFrame(t, wire.Header{Type: protocol.MsgRequest, MessageID: 9}, []byte("plaintext probe"))
	if _, err := raw.Write(frame); err != nil {
		t.Fatalf("write plaintext probe: %v", err)
	}
	if err := raw.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, _ = raw.Read(make([]byte, 64)) // 服务端关闭：TLS alert/EOF，内容不究
	deadline := time.Now().Add(time.Second)
	for sink.count("tls handshake") == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if sink.count("tls handshake") == 0 {
		t.Fatal("accept-side TLS handshake failure must be logged")
	}
}
