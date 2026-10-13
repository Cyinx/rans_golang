package rpc

// 分发器测试：fakeConnManager 替身承接 Gateway 的按节点发送
// （connSender 收窄的测试面），测试以池化页直接驱动 Dispatcher.
// HandleMessage（A 侧手写帧：header+body 构造后注入，模拟传输层解码完成
// 后的回调）；回程/转发帧从替身 channel 断言——陷阱 4（回程按 Source
// 寻址）与陷阱 5（原始字节转发、投递时刻解码）的行为锁都锚在替身记录的
// 参数上。

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/ctxkey"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/routing"
	"github.com/cyinx/rans_golang/internal/wire"
)

// fakeSent 替身捕获的一次按节点发送（目标节点 + 帧）。
type fakeSent struct {
	node rans.NodeID
	hdr  wire.Header
	body []byte
}

// fakeConnManager connSender 测试替身：记录按节点寻址的发送参数（陷阱 4
// 断言锚点——回程必须按 SourceNodeID 寻址），不写真连接。body 所有权随
// 捕获转移给测试，断言毕归还页池。
type fakeConnManager struct {
	sent chan fakeSent
}

func newFakeConnManager() *fakeConnManager {
	return &fakeConnManager{sent: make(chan fakeSent, 32)}
}

func (f *fakeConnManager) SendToNode(_ context.Context, node rans.NodeID, _ func(ctx context.Context, n rans.NodeID) (string, error), hdr wire.Header, body []byte) error {
	f.sent <- fakeSent{node: node, hdr: hdr, body: body}
	return nil
}

// testEntity 可配置测试实体：Activate 可停于 gate（输家场景的时序编排：
// 先入箱后放行激活），Handle 行为由闭包注入。输家终态信号见
// waitLoserTerminal（激活失败/仲裁落败路径不调用用户
// Deactivate，终态信号不经 Deactivate）。
type testEntity struct {
	gate   chan struct{}
	handle func(ctx context.Context, req any) (any, error)
}

func (e *testEntity) Activate(context.Context) error {
	if e.gate != nil {
		<-e.gate
	}
	return nil
}

func (e *testEntity) Deactivate(context.Context) error { return nil }

func (e *testEntity) Handle(ctx context.Context, req any) (any, error) {
	return e.handle(ctx, req)
}

// waitLoserTerminal 等待输家/激活失败实例的终局收尾完成：本地注册表逐出
// （OnEvict → cleanup 已跑完，Destroyed 必已置位）。整箱转发先于 finish
// 发生，故返回时转发帧已入替身队列。
func waitLoserTerminal(t *testing.T, m *activation.Table, id rans.ID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := m.GetLocal(id); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("loser instance did not reach terminal eviction")
		}
		time.Sleep(time.Millisecond)
	}
}

// entityLedger 工厂台账：factory 每被调用（=一次激活创建）登记一个实体。
// feed 同步返回后该次激活的实体必已在册——factory 在 GetOrActivate 的
// 创建区内同步执行。
type entityLedger struct {
	mu   sync.Mutex
	ents []*testEntity
}

func (l *entityLedger) add(e *testEntity) {
	l.mu.Lock()
	l.ents = append(l.ents, e)
	l.mu.Unlock()
}

func (l *entityLedger) get(t *testing.T, i int) *testEntity {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if i >= len(l.ents) {
		t.Fatalf("entity %d not created (created=%d)", i, len(l.ents))
	}
	return l.ents[i]
}

func (l *entityLedger) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.ents)
}

// dispEnv 分发器测试基座：B 节点（self=2）真 activation.Table（inmem 目录）
// + 真 Gateway（conns 为 fakeConnManager 替身）+ Dispatcher。
type dispEnv struct {
	disp *Dispatcher
	mgr  *activation.Table
	dir  directory.Directory
	gw   *Gateway
	fake *fakeConnManager
	pool *wire.PagePool
	led  *entityLedger
}

// newDispOutbox 测试发件箱（与生产装配同构：发送面接网关——回包/转发
// 经箱异步到达替身），Close 挂 Cleanup；独立注册表避免全局聚合串场。
func newDispOutbox(t *testing.T, gw *Gateway) *Outbox {
	t.Helper()
	ob, err := NewOutbox(OutboxConfig{Send: gw.SendOneWay, Metrics: metrics.New("disp-test")})
	if err != nil {
		t.Fatalf("new outbox: %v", err)
	}
	t.Cleanup(ob.Close)
	return ob
}

// newDispEnv 构造基座。handle 为实体 Handle 行为；gated=true 时实体
// Activate 停在 gate 上（测试关 gate 前消息已入箱——输家整箱转发的确定性
// 编排：GetOrActivate 返回在途激活 (p,nil)，准入先于激活结果落地）。
func newDispEnv(t *testing.T, handle func(ctx context.Context, req any) (any, error), gated bool) *dispEnv {
	t.Helper()
	dir := inmem.New()
	led := &entityLedger{}
	kinds := map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity {
			e := &testEntity{handle: handle}
			if gated {
				e.gate = make(chan struct{})
			}
			led.add(e)
			return e
		},
	}
	mgr := activation.New(dir, 2, kinds)
	pool := wire.NewPagePool()
	fake := newFakeConnManager()
	gw := NewGateway(fake, func(_ context.Context, _ rans.NodeID) (string, error) {
		return "", errors.New("no address in test")
	}, pool, time.Second, nil)
	disp := NewDispatcher(mgr, gw, 2, pool, nil, 0, newDispOutbox(t, gw), nil)
	return &dispEnv{disp: disp, mgr: mgr, dir: dir, gw: gw, fake: fake, pool: pool, led: led}
}

// feed 以池化页驱动一次入站投递，返回所投 body 的原始字节副本（供转发
// 原始字节比对）。
func (e *dispEnv) feed(hdr wire.Header, body []byte) []byte {
	page := e.pool.Get(len(body))
	copy(page, body)
	e.disp.HandleMessage(hdr, page)
	return append([]byte(nil), body...)
}

// mustSent 从替身 channel 确定性收一帧（channel 断言，不 sleep）。
func mustSent(t *testing.T, f *fakeConnManager) fakeSent {
	t.Helper()
	select {
	case s := <-f.sent:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("gateway did not send any frame")
		return fakeSent{} // 不可达
	}
}

// sentCount 替身 channel 中未消费帧数（负面断言前须先取得完成信号）。
func sentCount(f *fakeConnManager) int { return len(f.sent) }

// envBody 构造 Envelope(int) 请求体。
func envBody(t *testing.T, v int) []byte {
	t.Helper()
	name, payload, err := codec.Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	return wire.MarshalEnvelope(name, payload)
}

// reqHdrFor 构造 A(1)→B 的请求头（Target=cell/<inst>），可附带扩展块因果
// 路径（H12 决策 14：因果载荷只有扩展块通道）。
func reqHdrFor(typ protocol.MessageType, id uint64, inst string, causal *ctxkey.CausalPath) wire.Header {
	h := wire.Header{
		Type:         typ,
		Flags:        protocol.FlagHasSourceNodeID | protocol.FlagHasTargetUUID,
		MessageID:    id,
		SourceNodeID: 1,
		TargetKind:   "cell",
		TargetInst:   inst,
	}
	if causal != nil {
		h.Ext = wire.EncodeRequestExt(wire.RequestExt{Causal: causal})
		h.Flags |= protocol.FlagHasExtensions
	}
	return h
}

// 1. Request 激活并回程：A（节点 1）手写 Request → B 分发器激活实体执行 →
// 回程 Response 经替身按 Source=1 寻址发出（陷阱 4），MessageID 拷贝、
// body 解 Envelope 得实体返回值；入站因果段在派发入口注入 ctx（§17.2）。
func TestDispatchRequestActivatesAndReplies(t *testing.T) {
	got := make(chan any, 4)
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		if cp := ctxkey.CausalFrom(ctx); cp == nil || cp.Root != "trace-1" {
			t.Errorf("causal path must be injected at dispatch entry (§17.2), got %v", cp)
		}
		got <- req
		return req.(int) + 1, nil
	}, false)
	const reqID = 101
	env.feed(reqHdrFor(protocol.MsgRequest, reqID, "1",
		&ctxkey.CausalPath{Root: "trace-1", Path: []uint64{1}}), envBody(t, 41))

	select {
	case v := <-got:
		if v != 41 {
			t.Fatalf("entity received %v, want int 41", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("entity Handle was not invoked")
	}

	fr := mustSent(t, env.fake)
	if fr.node != 1 {
		t.Fatalf("reply must be addressed to source node 1 (trap 4), got node %d", fr.node)
	}
	if fr.hdr.Type != protocol.MsgResponse || fr.hdr.MessageID != reqID {
		t.Fatalf("response header mismatch: %+v", fr.hdr)
	}
	if fr.hdr.SourceNodeID != 2 {
		t.Fatalf("reply SourceNodeID=%d want 2 (B self)", fr.hdr.SourceNodeID)
	}
	if fr.hdr.Flags&protocol.FlagHasStatus == 0 || fr.hdr.Status != rans.CodeOK {
		t.Fatalf("response must carry StatusOK, got %+v", fr.hdr)
	}
	renv, err := wire.UnmarshalEnvelope(fr.body)
	if err != nil {
		t.Fatalf("response body not an envelope: %v", err)
	}
	out, err := codec.Decode(renv.TypeName, renv.Payload)
	if err != nil {
		t.Fatalf("decode response value: %v", err)
	}
	if out != 42 {
		t.Fatalf("response value=%v want 42", out)
	}
	env.pool.Put(fr.body)
}

// 2. 投递时刻解码（陷阱 5 行为锁）：非法 Envelope 的 body 在 Deliver 内
// 解码失败 → 实体 Handle 不被调用、A 收 Error(StatusDecodeError)。
func TestDispatchDecodeAtDeliveryTime(t *testing.T) {
	var calls atomic.Int32
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		calls.Add(1)
		return nil, nil
	}, false)
	const reqID = 102
	env.feed(reqHdrFor(protocol.MsgRequest, reqID, "dec", nil), []byte{0xFF}) // 畸形外封：varint 续位无后续字节

	fr := mustSent(t, env.fake)
	if fr.hdr.Type != protocol.MsgError || fr.hdr.Status != rans.CodeDecode {
		t.Fatalf("want Error(StatusDecodeError), got %+v", fr.hdr)
	}
	if fr.hdr.MessageID != reqID || fr.node != 1 {
		t.Fatalf("decode error frame mismatch: hdr=%+v node=%d", fr.hdr, fr.node)
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("entity Handle must not run on undecodable body, ran %d times", n)
	}
	env.pool.Put(fr.body)
}

// 3. 畸形身份：TargetKind 非法字符 → DecodeError 回错（不让调用方白等，
// 规格 §10.3），激活不发生（工厂未被调用）。
func TestDispatchMalformedUUIDRejected(t *testing.T) {
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}, false)
	const reqID = 103
	hdr := reqHdrFor(protocol.MsgRequest, reqID, "x", nil)
	hdr.TargetKind = "bad!" // 身份字符集外的非法字符
	env.feed(hdr, envBody(t, 1))

	fr := mustSent(t, env.fake)
	if fr.hdr.Type != protocol.MsgError || fr.hdr.Status != rans.CodeDecode {
		t.Fatalf("want Error(StatusDecodeError), got %+v", fr.hdr)
	}
	if fr.node != 1 || fr.hdr.MessageID != reqID {
		t.Fatalf("reject frame mismatch: hdr=%+v node=%d", fr.hdr, fr.node)
	}
	if n := env.led.count(); n != 0 {
		t.Fatalf("malformed uuid must not trigger activation, %d entities created", n)
	}
	env.pool.Put(fr.body)
}

// 4. 输家转发原始字节 + 单跳防环（规格 §12）：目录预注册赢家=节点 3，
// Request 到 B → B 激活落败 → 整箱转发：替身收到发往节点 3 的帧——body
// 与原始字节相同（不解码不重编，陷阱 5 的另一面）、Forwarded 置位、
// MessageID/Source/因果段原样透传；把该 Forwarded 帧再投给 B（仍落败）→
// 不再二次转发，回 Error(StatusNotFound) 给源端（该帧必然
// 从未被执行——环守卫先于一切投递，NotFound=位置陈旧 → 调用方重试环
// 无条件重解析路由重发，零丢失；非幂等调用同样获救）。
func TestForwardUsesRawBodySingleHop(t *testing.T) {
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		t.Error("loser must not run entity Handle")
		return nil, nil
	}, true)
	id, err := rans.NewID("cell", "fw")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := env.dir.Register(context.Background(), id, directory.Owner{Node: 3}, nil); err != nil {
		t.Fatal(err)
	}

	const reqID = 201
	fed := env.feed(reqHdrFor(protocol.MsgRequest, reqID, "fw",
		&ctxkey.CausalPath{Root: "trace-fw"}), envBody(t, 7))

	e0 := env.led.get(t, 0)
	close(e0.gate)                    // 消息已入箱（feed 同步返回），放行激活 → 必落败于节点 3
	waitLoserTerminal(t, env.mgr, id) // 终态：整箱转发已完成、实例已逐出

	fr := mustSent(t, env.fake)
	if fr.node != 3 {
		t.Fatalf("loser must forward to winner node 3, got node %d", fr.node)
	}
	if fr.hdr.Flags&protocol.FlagForwarded == 0 {
		t.Fatalf("forwarded frame must set FlagForwarded, got %+v", fr.hdr)
	}
	if fr.hdr.MessageID != reqID || fr.hdr.SourceNodeID != 1 {
		t.Fatalf("MessageID/Source must pass through unchanged: %+v", fr.hdr)
	}
	if fr.hdr.Type != protocol.MsgRequest {
		t.Fatalf("forwarded type=%v want Request", fr.hdr.Type)
	}
	ext, err := wire.DecodeRequestExt(fr.hdr.Ext)
	if err != nil || ext.Causal == nil || ext.Causal.Root != "trace-fw" {
		t.Fatalf("causal path must pass through unchanged (ext bytes opaque), got %v err %v", ext.Causal, err)
	}
	if !bytes.Equal(fr.body, fed) {
		t.Fatalf("forward body must be the exact raw bytes fed: got %x want %x", fr.body, fed)
	}
	fwdHdr, fwdBody := fr.hdr, append([]byte(nil), fr.body...) // 拷出再归还页
	env.pool.Put(fr.body)

	// 第二跳：Forwarded 帧再投（B 仍落败）→ 回 Error 给源节点，不再转发
	env.feed(fwdHdr, fwdBody)
	e1 := env.led.get(t, 1)
	close(e1.gate)
	waitLoserTerminal(t, env.mgr, id)

	fr2 := mustSent(t, env.fake)
	if fr2.node != 1 {
		t.Fatalf("forward-loop error must reply to source node 1, got node %d", fr2.node)
	}
	if fr2.hdr.Type != protocol.MsgError || fr2.hdr.Status != rans.CodeNotFound {
		t.Fatalf("want Error(StatusNotFound) on forward loop (stale location, unconditional re-route retry), got %+v", fr2.hdr)
	}
	if fr2.hdr.MessageID != reqID {
		t.Fatalf("loop error MessageID=%d want %d", fr2.hdr.MessageID, reqID)
	}
	// 结构化帧体：环守卫回 NotFound+StaleLocation（位置陈旧 → 调用方
	// 重解析重试），诊断文本保留 forward 语义。
	re2, err := wire.UnmarshalError(fr2.body)
	if err != nil {
		t.Fatalf("forward-loop frame body must decode: %v", err)
	}
	if re2.Code != rans.CodeNotFound || re2.Reason != rans.ReasonStaleLocation {
		t.Fatalf("loop frame code=%d reason=%d, want NotFound+StaleLocation", re2.Code, re2.Reason)
	}
	if !strings.Contains(re2.Message, "forward") {
		t.Fatalf("loop error message=%q must mention forward", re2.Message)
	}
	env.pool.Put(fr2.body)
	if n := sentCount(env.fake); n != 0 {
		t.Fatalf("exactly two frames expected in total, %d extra left", n)
	}
}

// 5. 匿名源（SourceNodeID=0）输家：规格 §12「匿名源不转发回 NotFound 促
// 客户端自愈」——无地址可回，drop+log（见 Dispatcher.reply 的匿名源分支）：
// 任何节点都收不到帧。
func TestAnonymousSourceForwardDropsReply(t *testing.T) {
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}, true)
	id, err := rans.NewID("cell", "anon")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := env.dir.Register(context.Background(), id, directory.Owner{Node: 3}, nil); err != nil {
		t.Fatal(err)
	}

	hdr := reqHdrFor(protocol.MsgRequest, 202, "anon", nil)
	hdr.SourceNodeID = 0 // 匿名源
	env.feed(hdr, envBody(t, 7))

	e0 := env.led.get(t, 0)
	close(e0.gate)
	waitLoserTerminal(t, env.mgr, id) // 终态：Forward 收场（匿名分支）已完成

	if n := sentCount(env.fake); n != 0 {
		t.Fatalf("anonymous source must get no frame (drop+log ruling), got %d", n)
	}
}

// 6. OneWay：实体执行、无任何应答帧。完成信号用 FIFO 哨兵：同一实体上
// 后续 Request 的 Response 到达 ⟹ OneWay 投递已整体收场（串行 loop）。
func TestOneWayNoReply(t *testing.T) {
	got := make(chan any, 4)
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		got <- req
		return req.(int) + 1, nil
	}, false)
	env.feed(reqHdrFor(protocol.MsgOneWay, 301, "ow", nil), envBody(t, 5))
	select {
	case v := <-got:
		if v != 5 {
			t.Fatalf("one-way entity received %v, want 5", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("one-way entity Handle was not invoked")
	}

	// 哨兵 Request（同一身份，FIFO 排在 OneWay 之后）：其 Response 到达即
	// 证明 OneWay 投递已完整收场且未发过任何帧。
	env.feed(reqHdrFor(protocol.MsgRequest, 302, "ow", nil), envBody(t, 10))
	fr := mustSent(t, env.fake)
	if fr.hdr.Type != protocol.MsgResponse || fr.hdr.MessageID != 302 {
		t.Fatalf("sentinel response mismatch: %+v", fr.hdr)
	}
	if v := <-got; v != 10 {
		t.Fatalf("sentinel entity received %v, want 10", v)
	}
	if n := sentCount(env.fake); n != 0 {
		t.Fatalf("one-way must produce no reply frame, %d extra left", n)
	}
	env.pool.Put(fr.body)
}

// 7. Deliver panic → reject-on-panic 远程闭环：对端收
// Error(StatusError)，reason 含 panic 语义；页恰好归还一次（once 守卫）。
func TestRejectOnPanicPropagatesError(t *testing.T) {
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		panic("boom")
	}, false)
	base := env.pool.Stats() // 池收支锚点（panic 双路径的恰一次归还）
	const reqID = 401
	env.feed(reqHdrFor(protocol.MsgRequest, reqID, "panic", nil), envBody(t, 1))

	fr := mustSent(t, env.fake)
	if fr.hdr.Type != protocol.MsgError || fr.hdr.Status != rans.CodeError {
		t.Fatalf("want Error(StatusError) on entity panic, got %+v", fr.hdr)
	}
	if fr.node != 1 || fr.hdr.MessageID != reqID {
		t.Fatalf("panic error frame mismatch: hdr=%+v node=%d", fr.hdr, fr.node)
	}
	// 结构化帧体：panic 回绝的 (Code, Reason) 过线——ReasonHandler
	// 进帧体字段（panic 细节在 invoke 的 warn 日志留痕，不进回绝理由）。
	re, err := wire.UnmarshalError(fr.body)
	if err != nil {
		t.Fatalf("panic error frame body must decode: %v", err)
	}
	if re.Code != rans.CodeError || re.Reason != rans.ReasonHandler {
		t.Fatalf("panic frame code=%d reason=%d, want CodeError+ReasonHandler", re.Code, re.Reason)
	}
	// Deliver panic + Reject 双路径后页恰一次归还：feed 的 Get 已被 release Put
	// （断言先于对 Error 帧 body 的归还——该 body 是 reply 侧新分配，非池 Get）
	s := env.pool.Stats()
	if s.Gets != base.Gets+1 || s.Puts != base.Puts+1 {
		t.Fatalf("panicked delivery must release its page exactly once: gets %d→%d puts %d→%d",
			base.Gets, s.Gets, base.Puts, s.Puts)
	}
	env.pool.Put(fr.body)
}

// 8. MigrationPackage 缓存未接线（NewDispatcher pkgs=nil 的防御形态）
// 与未知类型防御丢弃：页就地归还（池收支锚点）、不产生任
// 何帧。缓存已接线的投递行为见 migration_pkg_test.go。
func TestMigrationAndUnknownTypesDropped(t *testing.T) {
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}, false)
	base := env.pool.Stats()
	env.feed(wire.Header{Type: protocol.MsgMigrationPackage, MessageID: 501}, []byte("mig"))
	env.feed(wire.Header{Type: protocol.MsgPing, MessageID: 502}, []byte("ping"))
	s := env.pool.Stats()
	if s.Gets != base.Gets+2 || s.Puts != base.Puts+2 {
		t.Fatalf("dropped pages must be returned in place: gets %d→%d puts %d→%d",
			base.Gets, s.Gets, base.Puts, s.Puts)
	}
	if n := sentCount(env.fake); n != 0 {
		t.Fatalf("dropped messages must not produce frames, got %d", n)
	}
}

// 9. Response 路由进网关：分发器把 Response 帧交 Gateway.HandleResponse，
// 挂起中的 SendRequest 收到 header/body（页所有权经 pending 移交调用方）。
func TestDispatchRoutesResponsesToGateway(t *testing.T) {
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}, false)
	id := env.gw.NextMessageID()
	type result struct {
		hdr  wire.Header
		body []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		h, b, err := env.gw.SendRequest(context.Background(), 7,
			wire.Header{Type: protocol.MsgRequest, Flags: protocol.FlagHasSourceNodeID,
				MessageID: id, SourceNodeID: 2}, nil)
		done <- result{hdr: h, body: b, err: err}
	}()
	// 请求帧已过替身（发送成功，pending 挂起中——确定性信号）
	if fr := mustSent(t, env.fake); fr.node != 7 {
		t.Fatalf("outgoing request must target node 7, got %d", fr.node)
	}
	page := env.pool.Get(4)
	copy(page, "pong")
	env.disp.HandleMessage(wire.Header{
		Type:         protocol.MsgResponse,
		Flags:        protocol.FlagHasStatus | protocol.FlagHasSourceNodeID,
		MessageID:    id,
		Status:       rans.CodeOK,
		SourceNodeID: 7,
	}, page)

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("send request: %v", r.err)
		}
		if r.hdr.SourceNodeID != 7 || r.hdr.MessageID != id {
			t.Fatalf("routed response header mismatch: %+v", r.hdr)
		}
		if string(r.body) != "pong" {
			t.Fatalf("routed response body=%q want pong", r.body)
		}
		env.pool.Put(r.body) // 调用方解码后归还页（所有权契约）
	case <-time.After(5 * time.Second):
		t.Fatal("dispatcher must route responses into the gateway pending table")
	}
}

// 10. 激活失败回绝码（register-first 语义）：注册之后的失败（restore/Activate
// 失败）是执行前确定性失败——无论经在箱排空（异步窗：remoteDelivery 被
// drainReject(StatusUnavailable, "activation failed") 回绝）还是分发器
// 激活重试环耗尽后的收尾回执，源端一律收到 Error(StatusUnavailable)。
// routing.Classify 对 Unavailable 归 RetryUnconditional
// （执行前失败无条件重试），分类锚见本测试尾段。
func TestDispatcher_ActivationFailureRepliesUnavailable(t *testing.T) {
	dir := inmem.New()
	kinds := map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity {
			return &failActivateEntity{handle: fastHandle}
		},
	}
	mgr := activation.New(dir, 2, kinds)
	pool := wire.NewPagePool()
	fake := newFakeConnManager()
	gw := NewGateway(fake, func(_ context.Context, _ rans.NodeID) (string, error) {
		return "", errors.New("no address in test")
	}, pool, time.Second, nil)
	disp := NewDispatcher(mgr, gw, 2, pool, nil, 0, newDispOutbox(t, gw), nil)

	const reqID = 601
	disp.HandleMessage(reqHdrFor(protocol.MsgRequest, reqID, "actfail", nil),
		append([]byte(nil), envBody(t, 1)...))

	fr := mustSent(t, fake)
	if fr.hdr.Type != protocol.MsgError || fr.hdr.Status != rans.CodeUnavailable {
		t.Fatalf("activation failure must reply Error(StatusUnavailable) (pre-execution deterministic failure), got %+v", fr.hdr)
	}
	if fr.hdr.MessageID != reqID || fr.node != 1 {
		t.Fatalf("activation-failure frame mismatch: hdr=%+v node=%d", fr.hdr, fr.node)
	}
	pool.Put(fr.body)

	// 远端面分类锚（决策出处：docs/decisions/2026-10-09-hr2-unavailable-retry.md）：
	// Unavailable 由运行时在 admission 之前回绝产生——确定未执行，无条件
	// 重试。
	if got := routing.Classify(rans.NewError(rans.CodeUnavailable, rans.ReasonActivationFailed, "activation failed")); got != routing.RetryUnconditional {
		t.Fatalf("Classify(Unavailable+ActivationFailed) = %v, want RetryUnconditional (determined-not-executed)", got)
	}
}

// failActivateEntity Activate 恒失败的实体（激活失败回绝码编排）。
type failActivateEntity struct {
	handle func(ctx context.Context, req any) (any, error)
}

func (e *failActivateEntity) Activate(context.Context) error {
	return errors.New("rans: activate: activation failed")
}
func (e *failActivateEntity) Deactivate(context.Context) error { return nil }
func (e *failActivateEntity) Handle(ctx context.Context, req any) (any, error) {
	return e.handle(ctx, req)
}
