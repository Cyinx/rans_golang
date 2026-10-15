package rpc

// 网关错误面与发送预算契约：
//   - 满载拒绝（附录 A NET-8）与重复 MessageID 拒绝是哨兵错误——消费方可
//     errors.Is 区分「满载/编程错误」与「对端失联」；
//   - SendRequest 的发送阶段受请求 deadline 约束：重试链（首试+免退避
//     重试+4×2s 退避）在短超时配置下不得比超时长一个数量级，等待段的
//     deadline 权威贯通发送段；
//   - reply/Forward 出站非阻塞（H11 决策 12/13）：经节点发件箱满即丢
//     入箱即返回，读循环/实体串行 loop 零阻塞 IO。

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// failingSender 恒失败的发送替身（每次尝试 blockAfter 后失败——模拟不可
// 达目标的拨号耗时）。
type failingSender struct {
	mu          sync.Mutex
	blockFor    time.Duration
	attempts    int
	failInstant bool // true：零耗时立即失败
}

func (f *failingSender) SendToNode(ctx context.Context, node rans.NodeID, resolve func(ctx context.Context, n rans.NodeID) (string, error), hdr wire.Header, body []byte) error {
	f.mu.Lock()
	f.attempts++
	block := f.blockFor
	f.mu.Unlock()
	if f.failInstant {
		return errors.New("unreachable (injected)")
	}
	select {
	case <-time.After(block):
		return errors.New("dial timeout (injected)")
	case <-ctx.Done():
		return ctx.Err()
	}
}

//  1. 满载拒绝必须是哨兵：容量 1 占用后，第二条 SendRequest 以
//     ErrPendingCapacity 拒绝（可编程区分，非裸文本）。
func TestGatewayCapacityRejectionSentinel(t *testing.T) {
	gw := NewGatewayWithSweep(fakeConnSender{}, nil, nil, time.Minute, time.Minute, nil)
	gw.pt = newPendingTable(1) // 容量 1（全局精确语义）
	t.Cleanup(gw.Close)

	inflight := make(chan error, 1)
	go func() {
		_, _, err := gw.SendRequest(context.Background(), 2,
			wire.Header{MessageID: gw.NextMessageID()}, []byte("x"))
		inflight <- err
	}()
	// 等第一条真正入表（发送立即成功 → 进入等待段）。
	deadline := time.Now().Add(2 * time.Second)
	for gw.PendingCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	_, _, err := gw.SendRequest(context.Background(), 2,
		wire.Header{MessageID: gw.NextMessageID()}, []byte("y"))
	if !errors.Is(err, ErrPendingCapacity) {
		t.Fatalf("capacity rejection err = %v, want ErrPendingCapacity", err)
	}
	gw.FailPendingForNode(2, errors.New("test teardown"))
	if err := <-inflight; err == nil {
		t.Fatal("teardown must fail the in-flight request")
	}
}

//  2. 重复 MessageID 拒绝必须是哨兵：同一 ID 二次入表以
//     ErrDuplicateMessageID 拒绝（编程错误可检测）。
func TestGatewayDuplicateMessageIDSentinel(t *testing.T) {
	gw := NewGatewayWithSweep(fakeConnSender{}, nil, nil, time.Minute, time.Minute, nil)
	t.Cleanup(gw.Close)

	id := gw.NextMessageID()
	inflight := make(chan error, 1)
	go func() {
		_, _, err := gw.SendRequest(context.Background(), 2,
			wire.Header{MessageID: id}, []byte("x"))
		inflight <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for gw.PendingCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	_, _, err := gw.SendRequest(context.Background(), 2,
		wire.Header{MessageID: id}, []byte("dup"))
	if !errors.Is(err, ErrDuplicateMessageID) {
		t.Fatalf("duplicate rejection err = %v, want ErrDuplicateMessageID", err)
	}
	gw.FailPendingForNode(2, errors.New("test teardown"))
	<-inflight
}

//  3. 发送阶段受请求 deadline 约束：短超时（150ms）× 慢失败发送（每次
//     尝试 300ms）——deadline 权威使整体收场有界（不得吃满
//     ~8s 重试链）。
func TestGatewaySendPhaseBoundedByRequestDeadline(t *testing.T) {
	gw := NewGatewayWithSweep(&failingSender{blockFor: 300 * time.Millisecond}, nil, nil, 150*time.Millisecond, time.Minute, nil)
	t.Cleanup(gw.Close)

	start := time.Now()
	_, _, err := gw.SendRequest(context.Background(), 2,
		wire.Header{MessageID: gw.NextMessageID()}, []byte("x"))
	elapsed := time.Since(start)
	if !errors.Is(err, errSendFailed) {
		t.Fatalf("err = %v, want Unavailable+SendFailed wrap", err)
	}
	if rans.ReasonOf(err) != rans.ReasonSendFailed {
		t.Fatalf("ReasonOf = %d, want ReasonSendFailed", rans.ReasonOf(err))
	}
	if elapsed > time.Second {
		t.Fatalf("SendRequest took %v, want bounded by request deadline (~300ms)", elapsed)
	}
}

//  5. 逐请求 deadline（H10 决策 4）：pending deadline 在登记时按本次调用
//     ctx 的 deadline 计算（统一调用面的合成结果即权威）——构造期缺省
//     预算只兜底无 deadline 的直连形态，不得把带 deadline 的请求提前收割。
func TestGatewayPerRequestDeadline(t *testing.T) {
	gw := NewGatewayWithSweep(fakeConnSender{}, nil, nil, 50*time.Millisecond, 40*time.Millisecond, nil)
	t.Cleanup(gw.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := gw.SendRequest(ctx, 2, wire.Header{MessageID: gw.NextMessageID()}, []byte("x"))
	elapsed := time.Since(start)
	if rans.CodeOf(err) != rans.CodeTimeout {
		t.Fatalf("err = %v, want CodeTimeout", err)
	}
	if elapsed < 250*time.Millisecond {
		t.Fatalf("request reaped at %v, want ctx deadline (~300ms) to govern pending, not constructor fallback (50ms)", elapsed)
	}
}

//  4. reply/Forward 出站非阻塞（H11 决策 12/13）：恒挂起的发送替身（模拟
//     不可达目标的慢拨号）下，回包与转发即刻经发件箱入箱返回，不得在
//     读循环/实体串行 loop 上同步等网络（旧 5s 预算重试链已随非阻塞化
//     消亡）；放行后帧异步离箱按原寻址送达。
func TestReplyForwardEnqueueNonBlocking(t *testing.T) {
	blocked := newGatedSender()
	gw := NewGateway(blocked, nil, nil, time.Minute, nil)
	t.Cleanup(gw.Close)
	ob, err := NewOutbox(OutboxConfig{Send: gw.SendOneWay, Metrics: metrics.New("reply-nonblocking")})
	if err != nil {
		t.Fatalf("new outbox: %v", err)
	}
	t.Cleanup(ob.Close)
	d := NewDispatcher(nil, gw, 1, nil, nil, 0, ob, nil)

	hdr := wire.Header{
		Type:         protocol.MsgRequest,
		MessageID:    7,
		SourceNodeID: 2,
	}
	start := time.Now()
	d.replyError(hdr, rans.NewError(rans.CodeError, rans.ReasonNone, "nope"))
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("reply took %v, want non-blocking enqueue", elapsed)
	}

	id, _ := rans.NewID("cell", "budget")
	disp := &remoteDelivery{d: d, hdr: hdr, body: []byte("raw"), id: id}
	start = time.Now()
	disp.Forward(2)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("forward took %v, want non-blocking enqueue", elapsed)
	}

	// 放行后帧异步离箱：回包按 Source 寻址、转发置位原样透传。
	close(blocked.gate)
	var fr gatedSent
	for fr = range blocked.sent { // 票 12：跳过 Forward 漏斗随发的重定向提示
		if fr.hdr.Type != protocol.MsgRedirectHint {
			break
		}
	}
	if fr.node != 2 || fr.hdr.Type != protocol.MsgError {
		t.Fatalf("reply frame mismatch: node=%d type=%v", fr.node, fr.hdr.Type)
	}
	for fr = range blocked.sent { // 同上：跳过重定向提示
		if fr.hdr.Type != protocol.MsgRedirectHint {
			break
		}
	}
	if fr.node != 2 || fr.hdr.Flags&protocol.FlagForwarded == 0 || string(fr.body) != "raw" {
		t.Fatalf("forward frame mismatch: node=%d hdr=%+v body=%q", fr.node, fr.hdr, fr.body)
	}
}

// gatedSender 挂闸发送替身：gate 关闭时 SendToNode 阻塞（慢目标形态），
// 放行后记录到达帧。
type gatedSender struct {
	gate chan struct{}
	sent chan gatedSent
}

type gatedSent struct {
	node rans.NodeID
	hdr  wire.Header
	body []byte
}

func newGatedSender() *gatedSender {
	return &gatedSender{gate: make(chan struct{}), sent: make(chan gatedSent, 8)}
}

func (s *gatedSender) SendToNode(ctx context.Context, node rans.NodeID, _ func(ctx context.Context, n rans.NodeID) (string, error), hdr wire.Header, body []byte) error {
	select {
	case <-s.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.sent <- gatedSent{node: node, hdr: hdr, body: body}
	return nil
}
