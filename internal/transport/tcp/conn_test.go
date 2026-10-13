package tcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/transport"
	"github.com/cyinx/rans_golang/internal/wire"
)

// recvMsg 测试侧捕获的一次入站投递（header + body 快照）。
type recvMsg struct {
	hdr  wire.Header
	body []byte
}

// fakeHandler transport.Handler 的测试替身：事件入 channel，测试用
// channel 收讯断言（不用 sleep 等握手）。
type fakeHandler struct {
	got       chan recvMsg
	handshake chan rans.NodeID
	lost      chan rans.NodeID
}

func newFakeHandler() *fakeHandler {
	return &fakeHandler{
		got:       make(chan recvMsg, 16),
		handshake: make(chan rans.NodeID, 4),
		lost:      make(chan rans.NodeID, 4),
	}
}

func (f *fakeHandler) HandleMessage(h wire.Header, b []byte) { f.got <- recvMsg{hdr: h, body: b} }
func (f *fakeHandler) OnHandshakeComplete(n rans.NodeID)     { f.handshake <- n }
func (f *fakeHandler) OnConnectionLost(n rans.NodeID)        { f.lost <- n }

// pipePair 构造内存双端连接对：ca 为 dialer、cb 为 listener，双方除
// NodeID 外一致。secret 为 nil 时显式 NoHMAC（不安全形态的合法测试形态）。
// 返回共享页池供收支断言。
func pipePair(t *testing.T, secret []byte) (*Conn, *Conn, *fakeHandler, *fakeHandler, *wire.PagePool) {
	t.Helper()
	cfgA := Config{NodeID: 1, ClusterName: "t", Secret: secret, NoHMAC: secret == nil, SchemaDigest: 7, SendQueueDepth: 4, HandshakeTimeout: time.Second}
	cfgB := cfgA
	cfgB.NodeID = 2
	return connPair(t, cfgA, cfgB)
}

// connPair 按显式双方配置构造连接对（密钥/旗标错位类测试的底座）。
func connPair(t *testing.T, cfgA, cfgB Config) (*Conn, *Conn, *fakeHandler, *fakeHandler, *wire.PagePool) {
	t.Helper()
	c1, c2 := net.Pipe()
	ha, hb := newFakeHandler(), newFakeHandler()
	pool := wire.NewPagePool()
	ca := NewConn(c1, cfgA, ha, pool, true)  // dialer
	cb := NewConn(c2, cfgB, hb, pool, false) // listener
	ca.Start()
	cb.Start()
	t.Cleanup(func() { ca.Close(); cb.Close() })
	return ca, cb, ha, hb, pool
}

// rawListener 构造「裸端 + listener Conn」：测试直接向裸端写原始帧，
// 用于握手前注入任意帧（pre-auth 应用消息无法经对端 Send 送达——
// 出站门禁会先拒绝它，这正是被测行为）。
func rawListener(t *testing.T, cfg Config) (net.Conn, *Conn, *fakeHandler, *wire.PagePool) {
	t.Helper()
	c1, c2 := net.Pipe()
	h := newFakeHandler()
	pool := wire.NewPagePool()
	cc := NewConn(c2, cfg, h, pool, false)
	cc.Start()
	t.Cleanup(func() { cc.Close() })
	return c1, cc, h, pool
}

// newIdleConn 构造未 Start 的连接：无后台 goroutine，出站队列状态
// 完全由测试控制（背压/关闭语义的确定性观察窗）。
func newIdleConn(t *testing.T, depth int) (*Conn, *wire.PagePool) {
	t.Helper()
	c1, c2 := net.Pipe()
	t.Cleanup(func() { c1.Close(); c2.Close() })
	pool := wire.NewPagePool()
	cfg := Config{NodeID: 1, ClusterName: "t", NoHMAC: true, SendQueueDepth: depth, HandshakeTimeout: time.Second}
	return NewConn(c1, cfg, newFakeHandler(), pool, true), pool
}

// rawFrame 编码一条供裸端写入的原始帧。
func rawFrame(t *testing.T, hdr wire.Header, body []byte) []byte {
	t.Helper()
	frame, err := wire.EncodeFrame(hdr.Encode(), body)
	if err != nil {
		t.Fatalf("encode raw frame: %v", err)
	}
	return frame
}

func waitHandshake(t *testing.T, h *fakeHandler, want rans.NodeID) {
	t.Helper()
	select {
	case n := <-h.handshake:
		if n != want {
			t.Fatalf("handshake peer=%d want %d", n, want)
		}
	case <-time.After(time.Second):
		t.Fatal("handshake not completed")
	}
}

func expectNoMessage(t *testing.T, h *fakeHandler) {
	t.Helper()
	select {
	case m := <-h.got:
		t.Fatalf("unexpected HandleMessage: %+v", m.hdr)
	default:
	}
}

func expectNoEvent(t *testing.T, ch chan rans.NodeID) {
	t.Helper()
	select {
	case n := <-ch:
		t.Fatalf("unexpected event: %d", n)
	default:
	}
}

func baseCfg() Config {
	// NoHMAC: 测试基座无密钥——显式 opt-in（fail-closed 默认下不安全形态
	// 必须声明，规格 §10.2）。
	return Config{ClusterName: "t", SchemaDigest: 7, NoHMAC: true, HandshakeTimeout: time.Second}
}

// 1. 双端握手：双方 Ready、PeerNodeID 正确、OnHandshakeComplete 各一次。
func TestHandshakeDialerListenerPair(t *testing.T) {
	ca, cb, ha, hb, _ := pipePair(t, []byte("s3cret"))
	waitHandshake(t, ha, 2)
	waitHandshake(t, hb, 1)
	if !ca.Ready() || !cb.Ready() {
		t.Fatal("both must be ready")
	}
	if n, ok := ca.PeerNodeID(); !ok || n != 2 {
		t.Fatalf("ca peer=%d ok=%v want 2", n, ok)
	}
	if n, ok := cb.PeerNodeID(); !ok || n != 1 {
		t.Fatalf("cb peer=%d ok=%v want 1", n, ok)
	}
	// 各触发一次：无多余事件
	expectNoEvent(t, ha.handshake)
	expectNoEvent(t, hb.handshake)
}

// 2. 集群名不匹配：静默关闭，不写任何错误帧（§10.2 静默错位门闸）。
func TestHandshakeClusterMismatchClosesSilently(t *testing.T) {
	secret := []byte("topsecret")
	cfg := baseCfg()
	cfg.NodeID = 2
	cfg.ClusterName = "other"
	cfg.Secret = secret
	raw, cc, h, _ := rawListener(t, cfg)

	// 收集 listener 写回的全部字节：静默关闭 = 0 字节 + EOF
	written := make(chan []byte, 1)
	go func() {
		var all []byte
		buf := make([]byte, 4096)
		for {
			n, err := raw.Read(buf)
			all = append(all, buf[:n]...)
			if err != nil {
				written <- all
				return
			}
		}
	}()

	hs := wire.Handshake{NodeID: 1, ClusterName: "t", SchemaDigest: 7}
	if _, err := raw.Write(rawFrame(t, wire.Header{Type: protocol.MsgHandshake}, hs.Encode(secret))); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	select {
	case <-cc.Done():
	case <-time.After(time.Second):
		t.Fatal("mismatched handshake must close the connection")
	}
	select {
	case b := <-written:
		if len(b) != 0 {
			t.Fatalf("silent close expected, got %d bytes written back", len(b))
		}
	case <-time.After(time.Second):
		t.Fatal("raw end not closed")
	}
	expectNoMessage(t, h)
	expectNoEvent(t, h.handshake)
	expectNoEvent(t, h.lost) // 未就绪：不报 Lost
}

// 3. pre-auth 门禁：握手前的应用消息 → 断连且不进消息处理路径（陷阱 10/13）。
func TestPreAuthGateDisconnectsAppMessage(t *testing.T) {
	cfg := baseCfg()
	cfg.NodeID = 2
	raw, cc, h, _ := rawListener(t, cfg)
	if _, err := raw.Write(rawFrame(t, wire.Header{Type: protocol.MsgRequest, MessageID: 9}, []byte("early"))); err != nil {
		t.Fatalf("write pre-auth request: %v", err)
	}
	select {
	case <-cc.Done():
	case <-time.After(time.Second):
		t.Fatal("pre-auth application message must disconnect")
	}
	expectNoMessage(t, h)
	expectNoEvent(t, h.handshake)
	expectNoEvent(t, h.lost) // 未就绪：不报 Lost
}

// 4. 握手前 Ping 可过、Pong 携同 MessageID 返回（心跳先于握手可用）。
func TestPreAuthAllowsPingPong(t *testing.T) {
	cfg := baseCfg()
	cfg.NodeID = 2
	raw, cc, h, pool := rawListener(t, cfg)
	if _, err := raw.Write(rawFrame(t, wire.Header{Type: protocol.MsgPing, MessageID: 42}, nil)); err != nil {
		t.Fatalf("write pre-auth ping: %v", err)
	}
	dec := wire.NewFrameDecoder(raw, pool)
	rawHdr, rawBody, err := dec.Next()
	if err != nil {
		t.Fatalf("read pong: %v", err)
	}
	hdr, err := wire.DecodeHeader(rawHdr)
	if err != nil {
		t.Fatalf("decode pong header: %v", err)
	}
	pool.Put(rawBody)
	if hdr.Type != protocol.MsgPong {
		t.Fatalf("reply type=%v want Pong", hdr.Type)
	}
	if hdr.MessageID != 42 {
		t.Fatalf("pong message id=%d want 42 (copied from ping)", hdr.MessageID)
	}
	// 连接不得因 pre-auth Ping 关闭
	select {
	case <-cc.Done():
		t.Fatal("pre-auth ping must not close the connection")
	case <-time.After(150 * time.Millisecond):
	}
	expectNoMessage(t, h)
	// 池化页收支平衡（陷阱 1 锚点）：入站 Get 与处理路径 Put 一一对应
	if s := pool.Stats(); s.Gets != s.Puts {
		t.Fatalf("pool gets=%d puts=%d (leak)", s.Gets, s.Puts)
	}
}

// 5. 背压第一级：SendQueueDepth=2，数据队列塞满后第三个 Send 阻塞，
// 消费后放行（控制帧走优先通道，数据背压承载面是数据出站队列）。
func TestSendBackpressure(t *testing.T) {
	cc, pool := newIdleConn(t, 2)
	cc.ready.Store(true) // 白盒：背压语义与握手无关，直入就绪态
	data := wire.Header{Type: protocol.MsgOneWay, Flags: protocol.FlagHasSourceNodeID, SourceNodeID: 1}
	for i := 0; i < 2; i++ {
		if err := cc.Send(context.Background(), data, nil); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	blocked := make(chan error, 1)
	go func() {
		blocked <- cc.Send(context.Background(), wire.Header{Type: protocol.MsgOneWay, MessageID: 3}, nil)
	}()
	select {
	case err := <-blocked:
		t.Fatalf("third send must block on full queue, got %v", err)
	case <-time.After(150 * time.Millisecond):
		// 队列已满：第三个 Send 仍在阻塞——出站 chan 满即背压
	}
	// 消费一帧放行（按实现的所有权纪律归还页）
	select {
	case f := <-cc.out:
		if f.body != nil {
			pool.Put(f.body)
		}
	default:
		t.Fatal("expected a queued frame to consume")
	}
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("send after queue freed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send still blocked after queue freed")
	}
}

// 6. 出站门禁：握手完成前发应用消息 → transport.ErrNotReady；控制面不受限。
func TestSendRejectsAppBeforeReady(t *testing.T) {
	cc, _ := newIdleConn(t, 4)
	if err := cc.Send(context.Background(), wire.Header{Type: protocol.MsgRequest}, nil); !errors.Is(err, transport.ErrNotReady) {
		t.Fatalf("app send before ready: err=%v want ErrNotReady", err)
	}
	if err := cc.Send(context.Background(), wire.Header{Type: protocol.MsgPing}, nil); err != nil {
		t.Fatalf("control-plane send before ready: %v", err)
	}
}

// 7. Close 解除阻塞中的 Send（返回错误）。
func TestCloseUnblocksSender(t *testing.T) {
	cc, _ := newIdleConn(t, 2)
	cc.ready.Store(true) // 白盒：数据面门禁放行（背压与握手无关）
	data := wire.Header{Type: protocol.MsgOneWay, Flags: protocol.FlagHasSourceNodeID, SourceNodeID: 1}
	for i := 0; i < 2; i++ {
		if err := cc.Send(context.Background(), data, nil); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	blocked := make(chan error, 1)
	go func() { blocked <- cc.Send(context.Background(), data, nil) }()
	select {
	case err := <-blocked:
		t.Fatalf("send should block on full queue, got %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := cc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("send after close must return error")
		}
	case <-time.After(time.Second):
		t.Fatal("close must unblock pending sender")
	}
}

// 8. 双方 Secret=nil 且显式 NoHMAC：握手成功（不安全形态的显式 opt-in，
// 规格 §10.2），且每端握手完成各告警一次（共 2 条）。
func TestHandshakeNoSecretOptInWarns(t *testing.T) {
	sink := captureLog(t)
	ca, cb, ha, hb, _ := pipePair(t, nil)
	waitHandshake(t, ha, 2)
	waitHandshake(t, hb, 1)
	if !ca.Ready() || !cb.Ready() {
		t.Fatal("both must be ready (explicit NoHMAC opt-in)")
	}
	if n := sink.count("insecure: HMAC disabled"); n != 2 {
		t.Fatalf("insecure warns=%d want 2 (one per handshake completion)", n)
	}
}

// 9. 就绪后重复 Handshake 到达 → 关闭；就绪连接的丢失须上报。
func TestDuplicateHandshakeAfterReadyCloses(t *testing.T) {
	ca, cb, ha, hb, _ := pipePair(t, []byte("s3cret"))
	waitHandshake(t, ha, 2)
	waitHandshake(t, hb, 1)
	hs := wire.Handshake{NodeID: 1, ClusterName: "t", SchemaDigest: 7}
	if err := ca.Send(context.Background(), wire.Header{Type: protocol.MsgHandshake}, hs.Encode([]byte("s3cret"))); err != nil {
		t.Fatalf("send duplicate handshake: %v", err)
	}
	select {
	case <-cb.Done():
	case <-time.After(time.Second):
		t.Fatal("duplicate handshake after ready must close")
	}
	expectNoMessage(t, hb)
	select {
	case n := <-hb.lost:
		if n != 1 {
			t.Fatalf("lost peer=%d want 1", n)
		}
	case <-time.After(time.Second):
		t.Fatal("ready connection loss must be reported")
	}
	select {
	case <-ca.Done():
	case <-time.After(time.Second):
		t.Fatal("dialer must observe the close")
	}
}

// 10. 应用消息经 pipe 往返：header 条件字段与 body 完整到达 HandleMessage。
func TestAppMessageRoundTrip(t *testing.T) {
	ca, _, ha, hb, _ := pipePair(t, []byte("s3cret"))
	waitHandshake(t, ha, 2)
	waitHandshake(t, hb, 1)
	hdr := wire.Header{
		Type:         protocol.MsgRequest,
		MessageID:    77,
		Flags:        protocol.FlagHasSourceNodeID,
		SourceNodeID: 1,
	}
	if err := ca.Send(context.Background(), hdr, []byte("hello round trip")); err != nil {
		t.Fatalf("send app message: %v", err)
	}
	select {
	case m := <-hb.got:
		if m.hdr.Type != protocol.MsgRequest || m.hdr.MessageID != 77 || m.hdr.SourceNodeID != 1 {
			t.Fatalf("header round trip mismatch: %+v", m.hdr)
		}
		if string(m.body) != "hello round trip" {
			t.Fatalf("body=%q want %q", m.body, "hello round trip")
		}
	case <-time.After(time.Second):
		t.Fatal("app message not delivered")
	}
}

// logSink 线程安全日志收集器（经 captureLog 注入 log 门面捕获）。
type logSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

// count 统计子串出现次数（按日志行级别粗粒度断言，够用即可）。
func (s *logSink) count(sub string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Count(s.buf.String(), sub)
}

// captureLog 把 log 门面 logger 换成收集 sink，测试收尾恢复
// （库包日志出口统一走 log 门面，断言注入点是 log.SetLogger）。
func captureLog(t *testing.T) *logSink {
	t.Helper()
	sink := &logSink{}
	log.SetLogger(slog.New(slog.NewTextHandler(sink, nil)))
	t.Cleanup(func() { log.SetLogger(nil) })
	return sink
}

// 11. fail-closed 默认：Secret=nil 且未显式 NoHMAC → Start 即拒连（不发
// 握手字节），双端 Done 关闭，终态错误含 "HMAC required" 与指引（默认
// 拒绝隐式不安全形态，规格 §10.2）。
func TestHandshakeFailClosedWithoutSecret(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() { c1.Close(); c2.Close() })
	cfgA := Config{NodeID: 1, ClusterName: "t", SchemaDigest: 7, HandshakeTimeout: time.Second}
	cfgB := cfgA
	cfgB.NodeID = 2
	ca := NewConn(c1, cfgA, newFakeHandler(), nil, true)
	cb := NewConn(c2, cfgB, newFakeHandler(), nil, false)
	ca.Start()
	cb.Start()
	for _, cc := range []*Conn{ca, cb} {
		select {
		case <-cc.Done():
		case <-time.After(time.Second):
			t.Fatal("nil secret without NoHMAC must close at Start (fail-closed)")
		}
		if cc.Ready() {
			t.Fatal("fail-closed conn must never become ready")
		}
		err := cc.terminalErr()
		if err == nil || !strings.Contains(err.Error(), "HMAC required") {
			t.Fatalf("terminal err=%v want 'HMAC required'", err)
		}
	}
}

// 12. 双端都配钥但不同 → MAC 不匹配拒连（listener 校验 dialer 的 Handshake
// 即败，双端断连且永不就绪）。
func TestHandshakeMACMismatchRefused(t *testing.T) {
	cfgA := Config{NodeID: 1, ClusterName: "t", Secret: []byte("k1"), SchemaDigest: 7, HandshakeTimeout: time.Second}
	cfgB := cfgA
	cfgB.NodeID = 2
	cfgB.Secret = []byte("k2")
	ca, cb, ha, hb, _ := connPair(t, cfgA, cfgB)
	select {
	case <-cb.Done():
	case <-time.After(time.Second):
		t.Fatal("listener must reject mismatched MAC")
	}
	select {
	case <-ca.Done():
	case <-time.After(time.Second):
		t.Fatal("dialer must observe the close")
	}
	if ca.Ready() || cb.Ready() {
		t.Fatal("mismatched secrets must never become ready")
	}
	expectNoEvent(t, ha.handshake)
	expectNoEvent(t, hb.handshake)
	expectNoEvent(t, hb.lost) // 未就绪：不报 Lost
}

// 13. 一端配钥、另一端无钥显式 NoHMAC → 拒：配钥端校验对端无 MAC 的
// Ack 失败断连（不安全端可能短暂就绪，但配钥端永不就绪）。
func TestHandshakeKeyVersusNoHMACRefused(t *testing.T) {
	cfgA := Config{NodeID: 1, ClusterName: "t", Secret: []byte("k1"), SchemaDigest: 7, HandshakeTimeout: time.Second}
	cfgB := Config{NodeID: 2, ClusterName: "t", NoHMAC: true, SchemaDigest: 7, HandshakeTimeout: time.Second}
	ca, cb, _, _, _ := connPair(t, cfgA, cfgB)
	select {
	case <-ca.Done():
	case <-time.After(time.Second):
		t.Fatal("keyed side must reject the MAC-less ack")
	}
	if ca.Ready() {
		t.Fatal("keyed side must never become ready against a MAC-less peer")
	}
	select {
	case <-cb.Done(): // 配钥端断连后，无钥端观察 EOF 收尾
	case <-time.After(time.Second):
		t.Fatal("opt-in side must observe the close")
	}
}

// 14. Secret 非 nil 时 NoHMAC 旗标无效：MAC 恒启用——同钥双端置旗标仍通，
// 异钥双端置旗标仍拒（fail-closed 只辖 nil 形态）。
func TestHandshakeNoHMACFlagIgnoredWhenSecretSet(t *testing.T) {
	mk := func(id rans.NodeID, secret []byte) Config {
		return Config{NodeID: id, ClusterName: "t", Secret: secret, NoHMAC: true, SchemaDigest: 7, HandshakeTimeout: time.Second}
	}
	// 同钥 + 旗标：握手成功（MAC 照常计算/校验）
	ca, cb, ha, hb, _ := connPair(t, mk(1, []byte("k")), mk(2, []byte("k")))
	waitHandshake(t, ha, 2)
	waitHandshake(t, hb, 1)
	if !ca.Ready() || !cb.Ready() {
		t.Fatal("matching secrets must handshake even with NoHMAC set")
	}
	// 异钥 + 旗标：拒连（MAC 强制，旗标不免除）
	cd, ce, _, _, _ := connPair(t, mk(1, []byte("k1")), mk(2, []byte("k2")))
	select {
	case <-ce.Done():
	case <-time.After(time.Second):
		t.Fatal("differing secrets must be refused even with NoHMAC set")
	}
	select {
	case <-cd.Done():
	case <-time.After(time.Second):
		t.Fatal("dialer must observe the close")
	}
}

// TestDrainDeadlineRetrySemantics（退役读期限让位判定单测，前提实证见
// decisions 复核补记）：Go net 语义——期限一旦过期，后续 Read 立即超时
// 返回、即使内核缓冲已有数据（探针 A/C2 各 200/200）。故首次超时必须
// 续期重读一次（数据可能恰在期限窗口尾到达而读循环尚在分发上一帧），
// 续期过仍超时（连续静默两个 linger 窗）才按对端僵死收口；非期限错误
// 与非收尾期一律不让位。
func TestDrainDeadlineRetrySemantics(t *testing.T) {
	// 让位路径会续期读期限（落到 nc 上）——接一对 net.Pipe 防 nil 接口调用。
	p1, p2 := net.Pipe()
	defer p1.Close()
	defer p2.Close()
	c := &Conn{nc: p1}
	if c.drainDeadlineRetry(os.ErrDeadlineExceeded) {
		t.Fatal("not draining must not retry")
	}
	c.draining.Store(true)
	if c.drainDeadlineRetry(io.EOF) {
		t.Fatal("non-deadline error must not retry")
	}
	if !c.drainDeadlineRetry(os.ErrDeadlineExceeded) {
		t.Fatal("first deadline expiry while draining must retry (buffered data may be masked)")
	}
	if c.drainDeadlineRetry(os.ErrDeadlineExceeded) {
		t.Fatal("second consecutive deadline expiry must not retry (peer quiesced two windows)")
	}
	// 包装过的期限错误同样命中（errors.Is 语义）。
	c2 := &Conn{nc: p2}
	c2.draining.Store(true)
	wrapped := fmt.Errorf("read tcp x: %w", os.ErrDeadlineExceeded)
	if !c2.drainDeadlineRetry(wrapped) {
		t.Fatal("wrapped ErrDeadlineExceeded must retry")
	}
}

// ---- SchemaDigest 观测与严格模式（规格 §10.2 v3：指纹降级为
// 观测——默认失配告警+计数、连接保持；StrictSchema=true 拒连）----

// 15. 摘要失配 + 默认（非严格）：握手照常完成（连接保持），双端各自回调
// OnSchemaMismatch 恰一次（计数接线的落点；告警断言走 log 文案）。
func TestHandshakeSchemaMismatchObservation(t *testing.T) {
	sink := captureLog(t)
	cfgA := baseCfg()
	cfgA.NodeID = 1
	cfgA.SchemaDigest = 1
	cfgB := cfgA
	cfgB.NodeID = 2
	cfgB.SchemaDigest = 2
	var missesA, missesB atomic.Int32
	cfgA.OnSchemaMismatch = func() { missesA.Add(1) }
	cfgB.OnSchemaMismatch = func() { missesB.Add(1) }
	ca, cb, ha, hb, _ := connPair(t, cfgA, cfgB)
	waitHandshake(t, ha, 2)
	waitHandshake(t, hb, 1)
	if !ca.Ready() || !cb.Ready() {
		t.Fatal("digest mismatch must keep the connection in observation mode")
	}
	if n := missesA.Load(); n != 1 {
		t.Fatalf("dialer-side mismatch callbacks=%d want 1", n)
	}
	if n := missesB.Load(); n != 1 {
		t.Fatalf("listener-side mismatch callbacks=%d want 1", n)
	}
	if n := sink.count("schema digest mismatch"); n < 2 {
		t.Fatalf("mismatch warns=%d want >=2 (one per side)", n)
	}
	expectNoEvent(t, ha.lost)
	expectNoEvent(t, hb.lost)
}

// 16. 摘要失配 + StrictSchema：严格端拒连（ErrHandshakeSchema 进终态错误），
// 双端永不就绪；同摘要 + 严格照常通过（严格只辖失配）。
func TestHandshakeSchemaMismatchStrictRefused(t *testing.T) {
	cfgA := baseCfg()
	cfgA.NodeID = 1
	cfgA.SchemaDigest = 1
	cfgB := cfgA
	cfgB.NodeID = 2
	cfgB.SchemaDigest = 2
	cfgB.StrictSchema = true
	ca, cb, ha, hb, _ := connPair(t, cfgA, cfgB)
	select {
	case <-cb.Done():
	case <-time.After(time.Second):
		t.Fatal("strict side must refuse digest mismatch")
	}
	select {
	case <-ca.Done():
	case <-time.After(time.Second):
		t.Fatal("dialer must observe the strict refusal")
	}
	if ca.Ready() || cb.Ready() {
		t.Fatal("strict mismatch must never become ready")
	}
	if err := cb.terminalErr(); err == nil || !errors.Is(err, wire.ErrHandshakeSchema) {
		t.Fatalf("strict refusal terminal err=%v want ErrHandshakeSchema", err)
	}
	expectNoEvent(t, ha.handshake)
	expectNoEvent(t, hb.handshake)

	// 对照：同摘要 + 严格 → 握手照常。
	cfgA.SchemaDigest = 9
	cfgB.SchemaDigest = 9
	ca2, cb2, ha2, hb2, _ := connPair(t, cfgA, cfgB)
	waitHandshake(t, ha2, 2)
	waitHandshake(t, hb2, 1)
	if !ca2.Ready() || !cb2.Ready() {
		t.Fatal("matching digests must handshake even under StrictSchema")
	}
}
