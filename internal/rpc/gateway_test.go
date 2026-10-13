package rpc

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/transport/tcp"
	"github.com/cyinx/rans_golang/internal/wire"
)

// recvMsg 测试侧捕获的一次入站投递（header + 池化 body；页所有权随读取
// 转移给测试，读方负责 Put）。
type recvMsg struct {
	hdr  wire.Header
	body []byte
}

// peerHandler transport.Handler 测试替身：Response/Error 帧路由给 Gateway
// （A 侧接线，分发器同款路由），其余消息入 channel 由测试读取。
type peerHandler struct {
	got       chan recvMsg
	handshake chan rans.NodeID
	lost      chan rans.NodeID
	gw        *Gateway // nil 表示不路由（B 侧/独立 manager）
}

func newPeerHandler() *peerHandler {
	return &peerHandler{
		got:       make(chan recvMsg, 16),
		handshake: make(chan rans.NodeID, 4),
		lost:      make(chan rans.NodeID, 4),
	}
}

func (h *peerHandler) HandleMessage(hdr wire.Header, body []byte) {
	if h.gw != nil && (hdr.Type == protocol.MsgResponse || hdr.Type == protocol.MsgError) {
		h.gw.HandleResponse(hdr, body)
		return
	}
	h.got <- recvMsg{hdr: hdr, body: body}
}

func (h *peerHandler) OnHandshakeComplete(n rans.NodeID) { h.handshake <- n }
func (h *peerHandler) OnConnectionLost(n rans.NodeID)    { h.lost <- n }

// gwPipe 真实回环测试驱动：A 侧为真 tcp.Manager（cfg.Dial 返回
// net.Pipe 一端，dialer 角色握手），B 侧为真 tcp.Conn 承接另一端
// （listener 角色）。Gateway 的 SendToNode/ConnToNode 走真实路径；
// resolve 把节点 2 指到管道地址。握手超时用默认 5s（被测超时只落在
// Gateway 的请求超时上）。
type gwPipe struct {
	gw   *Gateway
	peer *tcp.Conn
	hB   *peerHandler
	pool *wire.PagePool
}

// testSweepInterval 真管道测试的默认扫描周期（短注入）：让百毫秒级超时
// 用例在一个 tick 内收场，不必等默认 250ms。
const testSweepInterval = 20 * time.Millisecond

func newGwPipe(t *testing.T, timeout time.Duration) *gwPipe {
	return newGwPipeSweep(t, timeout, testSweepInterval)
}

func newGwPipeSweep(t *testing.T, timeout, sweep time.Duration) *gwPipe {
	t.Helper()
	p1, p2 := net.Pipe()
	t.Cleanup(func() { _ = p1.Close(); _ = p2.Close() })
	pool := wire.NewPagePool()
	cfg := tcp.Config{ClusterName: "t", SchemaDigest: 7, NoHMAC: true} // 测试管道无密钥：显式 opt-in

	hA := newPeerHandler()
	cfgA := cfg
	cfgA.NodeID = 1
	cfgA.Dial = func(ctx context.Context, addr string) (net.Conn, error) { return p1, nil }
	mA := tcp.NewManager(cfgA, hA, pool)
	t.Cleanup(mA.Close)

	hB := newPeerHandler()
	cfgB := cfg
	cfgB.NodeID = 2
	peer := tcp.NewConn(p2, cfgB, hB, pool, false)
	peer.Start()
	t.Cleanup(func() { _ = peer.Close() })

	gw := NewGatewayWithSweep(mA, func(_ context.Context, node rans.NodeID) (string, error) { return "pipe", nil }, pool, timeout, sweep, nil)
	t.Cleanup(gw.Close) // 扫描器 goroutine 随测试收尾（零泄漏纪律）
	hA.gw = gw          // 先接线后发送：首个 SendRequest 之前完成，无竞态
	return &gwPipe{gw: gw, peer: peer, hB: hB, pool: pool}
}

// fakeConnSender 立即成功的发送替身（零耗时）：等待路径测试（快速失败/
// ctx 取消/容量守卫）不依赖真实管道——SendToNode 恒 nil 使 SendRequest
// 直接进入等待段。
type fakeConnSender struct{}

func (fakeConnSender) SendToNode(ctx context.Context, node rans.NodeID, resolve func(ctx context.Context, n rans.NodeID) (string, error), hdr wire.Header, body []byte) error {
	return nil
}

// mustRecv 从 B 侧消息 channel 确定性收帧（channel 断言，不 sleep）。
func mustRecv(t *testing.T, ch chan recvMsg) recvMsg {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("peer did not receive the message")
		return recvMsg{} // 不可达
	}
}

// pendingLen 白盒读取 pending 表长度（分片表总量）。
func pendingLen(g *Gateway) int {
	return g.pt.count()
}

// waitPending 白盒轮询等 pending 表长度到达期望值（登记/摘除的确定性
// 同步点——SendRequest 登记先于发送与等待）。
func waitPending(t *testing.T, g *Gateway, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if n := pendingLen(g); n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending=%d want %d (timeout waiting for pending table state)", pendingLen(g), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// recvErrWithin 限时收一个等待方错误（channel 断言，不 sleep）。
func recvErrWithin(t *testing.T, ch chan error, within time.Duration) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(within):
		t.Fatal("waiter did not return within " + within.String())
		return nil // 不可达
	}
}

// reqHdr 构造 A→2 的请求头：SourceNodeID 由调用方写入（回程寻址依据，
// 陷阱 4），Target 携带条件字段验证编解码往返。
func reqHdr(id uint64) wire.Header {
	return wire.Header{
		Type:         protocol.MsgRequest,
		Flags:        protocol.FlagHasSourceNodeID | protocol.FlagHasTargetUUID,
		MessageID:    id,
		SourceNodeID: 1,
		TargetKind:   "cell",
		TargetInst:   "inst-1",
	}
}

// 1. Call 往返：SendRequest → B 侧断言 MessageID/Source/Target → 回写
// Response（拷 MessageID）→ SendRequest 返回匹配 header/body；pending 清空。
func TestGatewayCallRoundTrip(t *testing.T) {
	p := newGwPipe(t, 5*time.Second)
	id := p.gw.NextMessageID()

	type callResult struct {
		hdr  wire.Header
		body []byte
		err  error
	}
	done := make(chan callResult, 1)
	go func() {
		h, b, err := p.gw.SendRequest(context.Background(), 2, reqHdr(id), []byte("ping-body"))
		done <- callResult{hdr: h, body: b, err: err}
	}()

	m := mustRecv(t, p.hB.got)
	if m.hdr.Type != protocol.MsgRequest || m.hdr.MessageID != id {
		t.Fatalf("request header mismatch: %+v", m.hdr)
	}
	if m.hdr.SourceNodeID != 1 {
		t.Fatalf("request source=%d want 1 (回程寻址依据)", m.hdr.SourceNodeID)
	}
	if m.hdr.TargetKind != "cell" || m.hdr.TargetInst != "inst-1" {
		t.Fatalf("request target=%q/%q want cell/inst-1", m.hdr.TargetKind, m.hdr.TargetInst)
	}
	if string(m.body) != "ping-body" {
		t.Fatalf("request body=%q want ping-body", m.body)
	}
	p.pool.Put(m.body)

	respHdr := wire.Header{
		Type:         protocol.MsgResponse,
		Flags:        protocol.FlagHasSourceNodeID,
		MessageID:    m.hdr.MessageID, // 拷贝请求 ID：pending 关联键
		SourceNodeID: 2,
	}
	if err := p.peer.Send(context.Background(), respHdr, []byte("pong-body")); err != nil {
		t.Fatalf("peer send response: %v", err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("send request: %v", r.err)
		}
		if r.hdr.MessageID != id || r.hdr.SourceNodeID != 2 {
			t.Fatalf("response header mismatch: %+v", r.hdr)
		}
		if string(r.body) != "pong-body" {
			t.Fatalf("response body=%q want pong-body", r.body)
		}
		p.pool.Put(r.body) // 调用方解码后归还页（所有权契约）
	case <-time.After(5 * time.Second):
		t.Fatal("SendRequest did not return after response")
	}
	if n := pendingLen(p.gw); n != 0 {
		t.Fatalf("pending table must be empty after round trip, got %d", n)
	}
}

// 2. 固定超时：无应答 → *rans.Error{CodeTimeout}（文本含 MessageID/目标节点/
// 时长）；pending 表清空。
func TestGatewayTimeout(t *testing.T) {
	p := newGwPipe(t, 150*time.Millisecond)
	// 预烧两个 ID，使被测 ID=3 与目标 2、150ms 在文本中断言可区分
	p.gw.NextMessageID()
	p.gw.NextMessageID()
	id := p.gw.NextMessageID()

	_, _, err := p.gw.SendRequest(context.Background(), 2, reqHdr(id), []byte("unanswered"))
	// 统一错误模型：网关超时 = *rans.Error{CodeTimeout}（errors.Is
	// 与 rans.ErrTimeout 同判）。
	var te *rans.Error
	if !errors.As(err, &te) || te.Code != rans.CodeTimeout {
		t.Fatalf("err=%v want *rans.Error{CodeTimeout}", err)
	}
	for _, want := range []string{strconv.FormatUint(id, 10), "2", "150ms"} {
		if !strings.Contains(te.Error(), want) {
			t.Fatalf("timeout text=%q want substring %q", te.Error(), want)
		}
	}
	if n := pendingLen(p.gw); n != 0 {
		t.Fatalf("pending table must be empty after timeout, got %d", n)
	}
	// 排干 B 侧收到的请求页
	if m := mustRecv(t, p.hB.got); m.body != nil {
		p.pool.Put(m.body)
	}
}

// 3. 迟到响应丢弃：超时后响应到达 → HandleResponse 不 panic、页就地
// 归还（收支差量锚点）、pending 不复活；未知 MessageID 同样丢弃。
func TestGatewayLateResponseDropped(t *testing.T) {
	p := newGwPipe(t, 100*time.Millisecond)
	id := p.gw.NextMessageID()
	_, _, err := p.gw.SendRequest(context.Background(), 2, reqHdr(id), nil)
	var te *rans.Error
	if !errors.As(err, &te) || te.Code != rans.CodeTimeout {
		t.Fatalf("err=%v want *rans.Error{CodeTimeout}", err)
	}
	if m := mustRecv(t, p.hB.got); m.body != nil {
		p.pool.Put(m.body)
	}

	base := p.pool.Stats()
	page := p.pool.Get(8)
	copy(page, "late-data")
	p.gw.HandleResponse(wire.Header{Type: protocol.MsgResponse, MessageID: id}, page)
	s := p.pool.Stats()
	if s.Gets != base.Gets+1 || s.Puts != base.Puts+1 {
		t.Fatalf("late response page must be returned in place: gets %d→%d puts %d→%d",
			base.Gets, s.Gets, base.Puts, s.Puts)
	}
	if n := pendingLen(p.gw); n != 0 {
		t.Fatalf("late response must not resurrect pending, got %d", n)
	}
	// 未知 MessageID 的响应：同样丢弃不 panic
	p.gw.HandleResponse(wire.Header{Type: protocol.MsgResponse, MessageID: id + 999}, nil)
}

// 4. Error 帧：Status=CodeNotFound → *rans.Error{Code/Message/Node}。
func TestGatewayHandleErrorFrame(t *testing.T) {
	p := newGwPipe(t, 5*time.Second)
	id := p.gw.NextMessageID()
	done := make(chan error, 1)
	go func() {
		_, _, err := p.gw.SendRequest(context.Background(), 2, reqHdr(id), []byte("doomed"))
		done <- err
	}()

	m := mustRecv(t, p.hB.got)
	p.pool.Put(m.body)
	errHdr := wire.Header{
		Type:         protocol.MsgError,
		Flags:        protocol.FlagHasStatus | protocol.FlagHasSourceNodeID,
		MessageID:    m.hdr.MessageID,
		Status:       rans.CodeNotFound,
		SourceNodeID: 2,
	}
	body := wire.MarshalError(&rans.Error{Code: rans.CodeNotFound, Message: "actor not found"})
	if err := p.peer.Send(context.Background(), errHdr, body); err != nil {
		t.Fatalf("peer send error frame: %v", err)
	}

	select {
	case err := <-done:
		// 结构化 Error 帧：解析为 *rans.Error{Code, Reason, Message, Node}
		// ——reason 以帧为准：帧体未携带 reason 时保持 ReasonNone
		// （网关侧不做 reason 归一）。
		var re *rans.Error
		if !errors.As(err, &re) {
			t.Fatalf("err=%v want *rans.Error", err)
		}
		if re.Code != rans.CodeNotFound {
			t.Fatalf("code=%d want CodeNotFound", re.Code)
		}
		if re.Reason != rans.ReasonNone {
			t.Fatalf("reason=%d want ReasonNone (no frame-side reason, no gateway-side normalization)", re.Reason)
		}
		if re.Message != "actor not found" {
			t.Fatalf("message=%q want actor not found", re.Message)
		}
		if re.Node != 2 {
			t.Fatalf("node=%d want 2", re.Node)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SendRequest did not return after error frame")
	}
	if n := pendingLen(p.gw); n != 0 {
		t.Fatalf("pending table must be empty after error, got %d", n)
	}
}

// 5. MessageID 进程内单调（陷阱 7）：串行严格递增、并发全不重复；
// 顺带验证 NewGateway 的 defTimeout ≤0 → 30s 默认与扫描器/容量默认参数
// （附录 A：扫描周期 250ms、pending 上限 100_000）。
func TestGatewayMessageIDMonotonic(t *testing.T) {
	g := NewGateway(nil, nil, nil, 0, nil)
	t.Cleanup(g.Close)
	if g.defTimeout != 30*time.Second {
		t.Fatalf("default defTimeout=%s want 30s", g.defTimeout)
	}
	if g.sweepInterval != 250*time.Millisecond {
		t.Fatalf("default sweep interval=%s want 250ms", g.sweepInterval)
	}
	if g.pt.max != 100_000 {
		t.Fatalf("default max pending=%d want 100000", g.pt.max)
	}
	prev := g.NextMessageID()
	if prev == 0 {
		t.Fatal("message id must start from 1 (0 reserved)")
	}
	for i := 0; i < 100; i++ {
		next := g.NextMessageID()
		if next <= prev {
			t.Fatalf("ids must strictly increase: %d then %d", prev, next)
		}
		prev = next
	}
	const workers, per = 8, 200
	ids := make(chan uint64, workers*per)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				ids <- g.NextMessageID()
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[uint64]struct{}, workers*per)
	for id := range ids {
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate message id %d", id)
		}
		seen[id] = struct{}{}
	}
}

// 6. OneWay：不登记 pending、不等应答即返回，对端可收到帧。
func TestSendOneWayNoPending(t *testing.T) {
	p := newGwPipe(t, 5*time.Second)
	hdr := wire.Header{
		Type:         protocol.MsgOneWay,
		Flags:        protocol.FlagHasSourceNodeID,
		MessageID:    p.gw.NextMessageID(),
		SourceNodeID: 1,
	}
	start := time.Now()
	if err := p.gw.SendOneWay(context.Background(), 2, hdr, []byte("fire-forget")); err != nil {
		t.Fatalf("send one way: %v", err)
	}
	// 网关超时 5s：若 OneWay 误等应答将耗满超时，4s 上界区分之
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("one-way send must not wait for a response, took %s", elapsed)
	}
	if n := pendingLen(p.gw); n != 0 {
		t.Fatalf("one-way must not register pending, got %d", n)
	}
	m := mustRecv(t, p.hB.got)
	if m.hdr.Type != protocol.MsgOneWay || m.hdr.MessageID != hdr.MessageID {
		t.Fatalf("one-way header mismatch: %+v", m.hdr)
	}
	if string(m.body) != "fire-forget" {
		t.Fatalf("one-way body=%q want fire-forget", m.body)
	}
	p.pool.Put(m.body)
}

// 7. 发送重试与端点判停：resolve 永远失败 → 首次尝试 + 5 次重试后放弃 →
// OnEndpointTerminated 触发；ctx 取消即止（不再重试、不判停）；未设
// 回调时不 panic。
func TestEndpointRetryThenTerminated(t *testing.T) {
	cfg := tcp.Config{NodeID: 1, ClusterName: "t", SchemaDigest: 7, NoHMAC: true, HandshakeTimeout: time.Second}
	m := tcp.NewManager(cfg, newPeerHandler(), nil)
	t.Cleanup(m.Close)

	var resolves atomic.Int32
	resolve := func(_ context.Context, node rans.NodeID) (string, error) {
		resolves.Add(1)
		return "", errors.New("no address for node")
	}
	term := make(chan rans.NodeID, 1)
	g := NewGateway(m, resolve, nil, time.Second, func(n rans.NodeID) { term <- n })
	t.Cleanup(g.Close)
	g.retryInterval = time.Millisecond // 测试缩短重试间隔（默认 2s）

	hdr := wire.Header{
		Type:         protocol.MsgRequest,
		Flags:        protocol.FlagHasSourceNodeID,
		MessageID:    g.NextMessageID(),
		SourceNodeID: 1,
	}
	err := g.sendWithRetry(context.Background(), 2, hdr, []byte("x"), time.Time{}, true)
	if err == nil {
		t.Fatal("send with retry must fail when resolve always fails")
	}
	// 连接类形态（统一错误模型）：耗尽错误必须是 Unavailable+SendFailed——分类
	// 唯一表归 RetryUnconditional（确定未投递）。
	if !errors.Is(err, errSendFailed) {
		t.Fatalf("exhausted send error = %v, must wrap errSendFailed", err)
	}
	if rans.ReasonOf(err) != rans.ReasonSendFailed {
		t.Fatalf("ReasonOf = %d, want ReasonSendFailed", rans.ReasonOf(err))
	}
	if got := resolves.Load(); got != 1+maxSendRetries {
		t.Fatalf("resolve calls=%d want %d (1 initial + %d retries)", got, 1+maxSendRetries, maxSendRetries)
	}
	select {
	case n := <-term:
		if n != 2 {
			t.Fatalf("terminated node=%d want 2", n)
		}
	default:
		t.Fatal("OnEndpointTerminated must fire after retries exhausted")
	}

	// ctx 取消即止：首次尝试后即返回，不再重试、不触发判停
	resolves.Store(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = g.sendWithRetry(ctx, 2, hdr, []byte("x"), time.Time{}, true)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if got := resolves.Load(); got != 1 {
		t.Fatalf("resolve calls after cancel=%d want 1 (no retries)", got)
	}
	select {
	case n := <-term:
		t.Fatalf("cancelled send must not fire termination, got %d", n)
	default:
	}

	// 默认无回调：仅记日志并返回错误，不 panic
	g2 := NewGateway(m, resolve, nil, time.Second, nil)
	t.Cleanup(g2.Close)
	g2.retryInterval = time.Millisecond
	if err := g2.sendWithRetry(context.Background(), 2, hdr, []byte("x"), time.Time{}, true); err == nil {
		t.Fatal("expected error with default (nil) termination callback")
	}

	// 公开路径同判：SendOneWay 直通 sendWithRetry，耗尽
	// 错误同为 Unavailable+SendFailed。放在末尾：判停回调同步触发，term
	//（缓冲 1）须先被上方各断言清空才能再承载本次触发。
	if err := g.SendOneWay(context.Background(), 2, hdr, []byte("x")); !errors.Is(err, errSendFailed) {
		t.Fatalf("SendOneWay exhausted error = %v, must wrap errSendFailed", err)
	}
}

// 8. 重复 MessageID 拒绝：同 ID 的第二个在途请求被显式拒绝，首个请求
// 仍按超时收场（陷阱 7 纪律：关联键不得复用）。
func TestSendRequestDuplicateMessageIDRejected(t *testing.T) {
	p := newGwPipe(t, 500*time.Millisecond)
	id := p.gw.NextMessageID()
	done := make(chan error, 1)
	go func() {
		_, _, err := p.gw.SendRequest(context.Background(), 2, reqHdr(id), []byte("first"))
		done <- err
	}()
	m := mustRecv(t, p.hB.got) // 首个请求已送达且在等待中（确定性信号）
	p.pool.Put(m.body)

	_, _, err := p.gw.SendRequest(context.Background(), 2, reqHdr(id), []byte("second"))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate message id must be rejected, got %v", err)
	}
	if err := <-done; !errors.Is(err, rans.ErrTimeout) {
		t.Fatalf("first waiter must still time out, got %v", err)
	}
}

// 9. 陷阱 11 次序锁（宽限窗口侧）：deadline 已过但扫描器尚未 tick 的窗口
// 内响应到达——已超时者不得收到该响应（页就地归还），表项不被投递方抢摘，
// 留给扫描器以「先摘表（释放门）后完成 waiter」次序收场超时错误。
// 长扫描周期（400ms）制造确定性窗口，无竞态轮询。
func TestGatewaySweeperExpiredEntryRefusesResponse(t *testing.T) {
	p := newGwPipeSweep(t, 30*time.Millisecond, 400*time.Millisecond)
	id := p.gw.NextMessageID()
	done := make(chan error, 1)
	go func() {
		_, _, err := p.gw.SendRequest(context.Background(), 2, reqHdr(id), nil)
		done <- err
	}()
	m := mustRecv(t, p.hB.got)
	if m.body != nil {
		p.pool.Put(m.body)
	}

	// 宽限窗口：deadline(30ms) 已过、扫描器(400ms) 未 tick——表项仍在
	time.Sleep(80 * time.Millisecond)
	if n := pendingLen(p.gw); n != 1 {
		t.Fatalf("grace window must keep the expired entry for the sweeper, pending=%d", n)
	}

	base := p.pool.Stats()
	page := p.pool.Get(8)
	copy(page, "racing-resp")
	p.gw.HandleResponse(wire.Header{Type: protocol.MsgResponse, MessageID: id, SourceNodeID: 2}, page)
	s := p.pool.Stats()
	if s.Gets != base.Gets+1 || s.Puts != base.Puts+1 {
		t.Fatalf("expired-entry response page must be returned in place: gets %d→%d puts %d→%d",
			base.Gets, s.Gets, base.Puts, s.Puts)
	}
	if n := pendingLen(p.gw); n != 1 {
		t.Fatalf("deliver must not steal the reaping right from the sweeper, pending=%d", n)
	}

	err := recvErrWithin(t, done, 2*time.Second)
	var te *rans.Error
	if !errors.As(err, &te) || te.Code != rans.CodeTimeout {
		t.Fatalf("waiter must get *rans.Error{CodeTimeout} from the sweeper, got %v (racing response must not win)", err)
	}
	if !strings.Contains(te.Error(), strconv.FormatUint(id, 10)) {
		t.Fatalf("timeout text=%q want message id mentioned", te.Error())
	}
	if n := pendingLen(p.gw); n != 0 {
		t.Fatalf("pending must be empty after sweep, got %d", n)
	}
}

// 10. 陷阱 11 次序锁（扫描完成侧）：扫描器摘表之后响应到达已摘表项——
// 迟到丢弃（页归还）、waiter 只收超时错误，绝不双投递。见到表空即
// 「释放门」已开（先摘表后完成的次序保证此刻投递方扑空）。
func TestGatewaySweeperReapedEntryRefusesResponse(t *testing.T) {
	p := newGwPipeSweep(t, 40*time.Millisecond, 20*time.Millisecond)
	id := p.gw.NextMessageID()
	done := make(chan error, 1)
	go func() {
		_, _, err := p.gw.SendRequest(context.Background(), 2, reqHdr(id), nil)
		done <- err
	}()
	m := mustRecv(t, p.hB.got)
	if m.body != nil {
		p.pool.Put(m.body)
	}

	// 轮询等扫描器完成「摘表」——先摘表后完成，见到表空即门已释放
	waitPending(t, p.gw, 0)

	base := p.pool.Stats()
	page := p.pool.Get(8)
	copy(page, "post-reap-resp")
	p.gw.HandleResponse(wire.Header{Type: protocol.MsgResponse, MessageID: id, SourceNodeID: 2}, page)
	s := p.pool.Stats()
	if s.Gets != base.Gets+1 || s.Puts != base.Puts+1 {
		t.Fatalf("post-reap response page must be returned in place: gets %d→%d puts %d→%d",
			base.Gets, s.Gets, base.Puts, s.Puts)
	}

	err := recvErrWithin(t, done, 2*time.Second)
	var te *rans.Error
	if !errors.As(err, &te) || te.Code != rans.CodeTimeout {
		t.Fatalf("waiter must get *rans.Error{CodeTimeout}, got %v", err)
	}
	if n := pendingLen(p.gw); n != 0 {
		t.Fatalf("pending must stay empty, got %d", n)
	}
}

// 11. 快速失败（规格 §9.1）：FailPendingForNode 摘除目标节点全部 pending、
// 各 waiter 收 *rans.Error{ConnectionLost, Node}、返回摘除数；其他节点的
// 表项不受影响。满超时 10s，2s 限时收错即毫秒级快速失败的时序证据。
func TestFailPendingForNode(t *testing.T) {
	g := NewGatewayWithSweep(fakeConnSender{}, nil, nil, 10*time.Second, 10*time.Second, nil)
	t.Cleanup(g.Close)
	reason := errors.New("connection reset by peer")

	start := func(target rans.NodeID) chan error {
		id := g.NextMessageID()
		ch := make(chan error, 1)
		go func() {
			_, _, err := g.SendRequest(context.Background(), target, reqHdr(id), nil)
			ch <- err
		}()
		return ch
	}
	to2a, to2b := start(2), start(2)
	to3 := start(3)
	waitPending(t, g, 3)

	if n := g.FailPendingForNode(2, reason); n != 2 {
		t.Fatalf("FailPendingForNode(2)=%d want 2", n)
	}
	for i, ch := range []chan error{to2a, to2b} {
		err := recvErrWithin(t, ch, 2*time.Second) // 不等满 10s 超时
		var nue *rans.Error
		if !errors.As(err, &nue) {
			t.Fatalf("waiter %d: err=%v want *rans.Error", i, err)
		}
		if nue.Code != rans.CodeConnectionLost || nue.Node != 2 {
			t.Fatalf("fast-fail error code=%d node=%d, want ConnectionLost/2", nue.Code, nue.Node)
		}
		if !errors.Is(err, rans.ErrConnectionLost) {
			t.Fatalf("err=%v must match rans.ErrConnectionLost", err)
		}
		if !strings.Contains(err.Error(), "node 2") || !strings.Contains(err.Error(), reason.Error()) {
			t.Fatalf("error text=%q want node 2 and reason context mentioned", err.Error())
		}
	}
	if n := pendingLen(g); n != 1 {
		t.Fatalf("node 3 entry must survive, pending=%d", n)
	}
	if n := g.FailPendingForNode(9, reason); n != 0 {
		t.Fatalf("unknown node count=%d want 0", n)
	}
	if n := g.FailPendingForNode(3, nil); n != 1 {
		t.Fatalf("FailPendingForNode(3)=%d want 1", n)
	}
	if err := recvErrWithin(t, to3, 2*time.Second); !errors.Is(err, rans.ErrConnectionLost) {
		t.Fatalf("node 3 waiter: err=%v must match rans.ErrConnectionLost (nil reason tolerated)", err)
	}
	if n := pendingLen(g); n != 0 {
		t.Fatalf("pending must be empty, got %d", n)
	}
}

// 12. ctx 取消即时：SendRequest 只等 channel + ctx.Done，取消即刻返回，
// 不等扫描 tick（扫描周期 10s 远大于测试窗口）。
func TestSendRequestCtxCancelImmediate(t *testing.T) {
	g := NewGatewayWithSweep(fakeConnSender{}, nil, nil, 30*time.Second, 10*time.Second, nil)
	t.Cleanup(g.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	id := g.NextMessageID()
	go func() {
		_, _, err := g.SendRequest(ctx, 2, reqHdr(id), nil)
		done <- err
	}()
	waitPending(t, g, 1)

	canceledAt := time.Now()
	cancel()
	err := recvErrWithin(t, done, time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if elapsed := time.Since(canceledAt); elapsed > 500*time.Millisecond {
		t.Fatalf("ctx cancel must return immediately without waiting for a sweep tick, took %s", elapsed)
	}
	waitPending(t, g, 0)
}

// 13. 容量守卫（附录 A NET-8）：pending 满载时新请求立即拒绝（错误文本含
// 上限值），被拒请求不残留表项，存量在途请求不受影响。
func TestGatewayCapacityGuard(t *testing.T) {
	g := NewGatewayWithSweep(fakeConnSender{}, nil, nil, 10*time.Second, 10*time.Second, nil)
	t.Cleanup(g.Close)
	g.pt = newPendingTable(1)
	done := make(chan error, 1)
	id := g.NextMessageID()
	go func() {
		_, _, err := g.SendRequest(context.Background(), 2, reqHdr(id), nil)
		done <- err
	}()
	waitPending(t, g, 1)

	_, _, err := g.SendRequest(context.Background(), 2, reqHdr(g.NextMessageID()), nil)
	if !errors.Is(err, ErrPendingCapacity) {
		t.Fatalf("request beyond capacity must be rejected with ErrPendingCapacity, got %v", err)
	}
	waitPending(t, g, 1) // 被拒请求不得入表，存量在途仍为 1
}

// 14. Close 幂等且扫描器 goroutine 退出（零泄漏）。
func TestGatewayCloseIdempotentStopsSweeper(t *testing.T) {
	g := NewGateway(nil, nil, nil, 0, nil)
	g.Close()
	g.Close() // 幂等：二次 Close 不 panic
	select {
	case <-g.sweepDone:
	case <-time.After(2 * time.Second):
		t.Fatal("sweeper goroutine must exit after Close")
	}
}
