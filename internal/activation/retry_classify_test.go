package activation

// 本地投递边界的错误面契约（决策出处：docs/decisions/2026-10-09-hr2-unavailable-retry.md）。
//   - Handle 返回的错误在投递边界（callDelivery.Deliver）折叠为不可 Unwrap
//     的专用形态：调用方与重试分类（routing.Classify）无法从错误链摸到实体
//     嵌套调用携带的任何运行时哨兵（ErrBusy/rans.ErrNotFound/…）或传输形态
//     （*RemoteError/ErrSendFailed/…）——「实体做完副作用后返回包装了回绝
//     哨兵的业务错误被误判为确定未执行而重复执行」的结构性风险在此消失；
//     错误文本仍可读；
//   - Reject（运行时回绝）错误不折叠：rans.ErrUnavailable/ErrBusy 双哨兵面语义
//     保留，且对重试环呈现 NoRetry——本地 Busy 回绝不触发环内重试。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/pawnite"
	"github.com/cyinx/rans_golang/internal/routing"
)

// foldEntity Handle 行为注入的最小实体（折叠面测试编排）。
type foldEntity struct {
	handle func(ctx context.Context, req any) (any, error)
}

func (e *foldEntity) Activate(context.Context) error                   { return nil }
func (e *foldEntity) Handle(ctx context.Context, req any) (any, error) { return e.handle(ctx, req) }
func (e *foldEntity) Deactivate(context.Context) error                 { return nil }

// assertFoldedOpaque 断言错误已折叠为统一形态：*rans.Error 且
// Code=CodeError、Reason=ReasonHandler；任何运行时形态（Busy/Unavailable/
// NotFound/Timeout/ActivationFailed 的 *rans.Error）都不可达，Classify 必判
// NoRetry（重试环对该错误零兴趣），文本保持可读。
func assertFoldedOpaque(t *testing.T, err error, wantText string) {
	t.Helper()
	var e *rans.Error
	if !errors.As(err, &e) {
		t.Fatalf("err = %v (%T) must be *rans.Error after folding", err, err)
	}
	if e.Code != rans.CodeError || e.Reason != rans.ReasonHandler {
		t.Fatalf("folded = code=%d reason=%d, want CodeError+ReasonHandler", e.Code, e.Reason)
	}
	for _, s := range []error{ErrBusy, rans.ErrUnavailable, rans.ErrNotFound, rans.ErrTimeout, ErrActivationFailed} {
		if errors.Is(err, s) {
			t.Fatalf("err = %v must not expose runtime shape %v after folding", err, s)
		}
	}
	if got := routing.Classify(err); got != routing.NoRetry {
		t.Fatalf("Classify(%v) = %v, want NoRetry (folded handler errors must never re-enter the ring)", err, got)
	}
	if !strings.Contains(err.Error(), wantText) {
		t.Fatalf("folded error must keep readable text %q, got %q", wantText, err.Error())
	}
}

// TestCall_HandlerErrorFoldedOpaque：Handle 返回的错误
// 哪怕嵌着可判重试的形态（ErrBusy 链、嵌套 NotFound 的 *rans.Error——实体做完
// 副作用后嵌套调用失败的最危险形态），到达调用方时已折叠为不透明错误。
func TestCall_HandlerErrorFoldedOpaque(t *testing.T) {
	cases := []struct {
		name    string
		handler func(context.Context, any) (any, error)
		text    string
	}{
		{"busy-wrapped business error", func(context.Context, any) (any, error) {
			return nil, fmt.Errorf("nested call failed: %w", ErrBusy)
		}, "nested call failed"},
		{"notfound-wrapped runtime shape", func(context.Context, any) (any, error) {
			return nil, fmt.Errorf("nested lookup failed: %w",
				rans.NewError(rans.CodeNotFound, rans.ReasonStaleLocation, "nested stale"))
		}, "nested lookup failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := New(inmem.New(), 1, map[string]rans.Factory{
				"cell": func(_ rans.ID) rans.Entity { return &foldEntity{handle: tc.handler} },
			})
			id, err := rans.NewID("cell", "fold-1")
			if err != nil {
				t.Fatal(err)
			}
			_, err = m.InvokeLocal(context.Background(), id, nil)
			if err == nil {
				t.Fatal("handler error must surface to the caller")
			}
			assertFoldedOpaque(t, err, tc.text)
		})
	}
}

// TestCall_CtxCancelConventionSurvivesFolding（折叠例外面的守护）：调用方
// ctx 到期/取消经 Handle 传播（文档化惯例，slowEntity 形态）不折叠——Call
// 的错误面归一（ctxOutcome）依赖 errors.Is(err, ctx.Err()) 识别该形态，
// 折叠不得切断它。
func TestCall_CtxCancelConventionSurvivesFolding(t *testing.T) {
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"slow": func(_ rans.ID) rans.Entity { return &slowEntity{} },
	})
	id, _ := rans.NewID("slow", "fold-cancel")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := m.InvokeLocal(ctx, id, nil); !errors.Is(err, rans.ErrTimeout) {
		t.Fatalf("err = %v, want rans.ErrTimeout (ctx deadline propagation must survive the fold)", err)
	}
}

// TestRejectBusyNotRetried（统一错误模型形态）：本地
// Reject 的 Busy 回绝面是 *rans.Error{Code: Unavailable, Reason: Busy}——
// Code/Reason 双维可判（过载保护），对重试环呈现 NoRetry/ShouldEnterRing=
// false：确定未执行，但按分类唯一表不自动重试。
func TestRejectBusyNotRetried(t *testing.T) {
	id, _ := rans.NewID("cell", "busy-pin")
	p := pawnite.New(id, pawnite.Options{InboxCap: 1})
	c1 := newTestDelivery(id)
	if !p.ReceiveRequest(c1) { // 未 Start：状态 Created，准入放行
		t.Fatal("first delivery must be admitted")
	}
	c2 := newTestDelivery(id)
	if p.ReceiveRequest(c2) {
		t.Fatal("second delivery must be rejected (inbox full)")
	}
	r, ok := drainReply(c2)
	if !ok {
		t.Fatal("inbox-full rejection must deliver a result")
	}
	if got := rans.CodeOf(r.err); got != rans.CodeUnavailable {
		t.Fatalf("CodeOf(reject busy) = %d, want CodeUnavailable", got)
	}
	if got := rans.ReasonOf(r.err); got != rans.ReasonBusy {
		t.Fatalf("ReasonOf(reject busy) = %d, want ReasonBusy", got)
	}
	if got := routing.Classify(r.err); got != routing.NoRetry {
		t.Fatalf("Classify(reject busy) = %v, want NoRetry (busy must not re-enter the ring)", got)
	}
	if routing.ShouldEnterRing(r.err) {
		t.Fatal("reject busy must not enter the retry ring")
	}
}
