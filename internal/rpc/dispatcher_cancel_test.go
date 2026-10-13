package rpc

// MsgCancel 收端单元测试（H10 决策 9，A 侧手写帧驱动 dispEnv）：取消定位
// 三态——执行中 cancel 其 Handle ctx、在箱标记后出队即弃（计数经
// BindCanceledHook）、迟到/重复/未知 ID 幂等忽略。真实过线的端到端形态
// （含 rans.call.canceled 指标面）见 testkit msg_cancel_test.go。

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// cancelHdrFor 构造 A(1)→B 的 MsgCancel 头（MessageID/SourceNodeID 即被取
// 消请求的定位键，与请求帧同值）。
func cancelHdrFor(id uint64) wire.Header {
	return wire.Header{
		Type:         protocol.MsgCancel,
		Flags:        protocol.FlagHasSourceNodeID | protocol.FlagHasTargetUUID,
		MessageID:    id,
		SourceNodeID: 1,
		TargetKind:   "cell",
		TargetInst:   "1",
	}
}

// TestMsgCancel_LateIdempotent 迟到/重复/未知 ID 的取消无害：请求正常完成
// （应答已回、定位表已摘）后再收同 ID 与未知 ID 的 MsgCancel——无新帧、
// 无重复执行、取消计数不误增（幂等收口的回归护栏）。
func TestMsgCancel_LateIdempotent(t *testing.T) {
	var exec atomic.Int32
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		exec.Add(1)
		return "ok", nil
	}, false)
	var canceled atomic.Int32
	env.mgr.BindCanceledHook(func() { canceled.Add(1) })

	const reqID = 901
	env.feed(reqHdrFor(protocol.MsgRequest, reqID, "late-1", nil), envBody(t, 1))
	fr := mustSent(t, env.fake)
	if fr.hdr.Type != protocol.MsgResponse {
		t.Fatalf("frame type = %d, want response (request completed normally)", fr.hdr.Type)
	}
	env.pool.Put(fr.body)

	env.feed(cancelHdrFor(reqID), nil) // 迟到：同 ID 重复取消 ×2
	env.feed(cancelHdrFor(reqID), nil)
	env.feed(cancelHdrFor(424242), nil) // 未知 ID

	if n := sentCount(env.fake); n != 0 {
		t.Fatalf("late cancel produced %d extra frames, want 0", n)
	}
	if n := exec.Load(); n != 1 {
		t.Fatalf("handle executed %d times, want 1", n)
	}
	if n := canceled.Load(); n != 0 {
		t.Fatalf("canceled counter = %d after late cancel, want 0 (no miscount)", n)
	}
}

// TestDispatcher_CancelExecuting 执行中取消定位：MsgCancel 命中正在执行的
// 请求 → Handle ctx 立即取消（Handle 竞速观测），不等 Handle 的固定 5s；
// 执行中取消不计 rans.call.canceled（计数专属于在箱弃置）。
func TestDispatcher_CancelExecuting(t *testing.T) {
	started := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once
	var observedCanceled atomic.Bool
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		once.Do(func() { close(started) })
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			observedCanceled.Store(true)
			close(done)
			return nil, ctx.Err()
		case <-timer.C:
			close(done)
			return "ok", nil
		}
	}, false)
	var canceled atomic.Int32
	env.mgr.BindCanceledHook(func() { canceled.Add(1) })

	const reqID = 902
	env.feed(reqHdrFor(protocol.MsgRequest, reqID, "exec-1", nil), envBody(t, 1))
	<-started
	cancelAt := time.Now()
	env.feed(cancelHdrFor(reqID), nil)
	select {
	case <-done:
		if elapsed := time.Since(cancelAt); elapsed > time.Second {
			t.Fatalf("handle returned %s after cancel, want prompt", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handle not cancelled by MsgCancel")
	}
	if !observedCanceled.Load() {
		t.Fatal("handle returned without observing ctx cancellation")
	}
	if n := canceled.Load(); n != 0 {
		t.Fatalf("executing-cancel counted %d, want 0 (only in-box drops count)", n)
	}
}

// TestDispatcher_CancelInboxDropped 在箱取消弃置：请求在箱（激活在途未出
// 队）时收到 MsgCancel → 标记后出队即弃——Handle 零执行、按 CodeCanceled
// 回绝、rans.call.canceled 计 1。
func TestDispatcher_CancelInboxDropped(t *testing.T) {
	var exec atomic.Int32
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		exec.Add(1)
		return "ok", nil
	}, true) // gated：Activate 停闸，请求在箱等待
	var canceled atomic.Int32
	env.mgr.BindCanceledHook(func() { canceled.Add(1) })

	const reqID = 903
	env.feed(reqHdrFor(protocol.MsgRequest, reqID, "inbox-1", nil), envBody(t, 1))
	env.feed(cancelHdrFor(reqID), nil)

	close(env.led.get(t, 0).gate) // 放行激活：loop 随后出队
	fr := mustSent(t, env.fake)
	if fr.hdr.Type != protocol.MsgError || fr.hdr.Status != rans.CodeCanceled {
		t.Fatalf("frame = type %d status %d, want Error/CodeCanceled", fr.hdr.Type, fr.hdr.Status)
	}
	env.pool.Put(fr.body)
	if n := exec.Load(); n != 0 {
		t.Fatalf("handle executed %d times, want 0 (in-box canceled request must drop)", n)
	}
	if n := canceled.Load(); n != 1 {
		t.Fatalf("canceled counter = %d, want 1", n)
	}
}
