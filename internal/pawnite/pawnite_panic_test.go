package pawnite

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/log"
)

// rejectInfo 记录一次回绝的状态码与理由（通道式断言用）。
type rejectInfo struct {
	st     rans.Code
	reason rans.Reason
}

// recDelivery 通道式投递记录：Deliver/Reject/Forward 皆经缓冲通道回报
// （缓冲保证回绝/转发方非阻塞——回绝调用发生在 loop goroutine 上，
// 测试侧未及时读取也不得卡死 pawnite）。Deliver 以 defer close 报完成，
// panic 场景下 close 仍随栈展开执行。
type recDelivery struct {
	id        rans.ID
	delivered chan struct{}
	rejected  chan rejectInfo
	forwarded chan rans.NodeID

	panicOnDeliver bool
	onForward      func(w rans.NodeID) // 可选：转发时附带记账（FIFO 断言用）
}

func (d *recDelivery) UUID() rans.ID { return d.id }
func (d *recDelivery) Deliver(_ *Pawnite) {
	defer close(d.delivered)
	if d.panicOnDeliver {
		panic("boom")
	}
}
func (d *recDelivery) Reject(st rans.Code, reason rans.Reason) {
	d.rejected <- rejectInfo{st: st, reason: reason}
}
func (d *recDelivery) Forward(w rans.NodeID) {
	d.forwarded <- w
	if d.onForward != nil {
		d.onForward(w)
	}
}

func newRec(id rans.ID) *recDelivery {
	return &recDelivery{
		id:        id,
		delivered: make(chan struct{}),
		rejected:  make(chan rejectInfo, 4),
		forwarded: make(chan rans.NodeID, 4),
	}
}

// TestDeliverPanicRejects 钉死 Reject-on-panic：
// Deliver panic 的投递按 StatusError 回结对端，且 loop 存活——
// 后续投递照常执行（panic 不杀进程、不杀循环）。
func TestDeliverPanicRejects(t *testing.T) {
	id := mustUUID(t, "player", "panic1")
	d := newRec(id)
	d.panicOnDeliver = true
	p := New(id, Options{InboxCap: 4})
	p.Start(context.Background())
	defer p.Destroy(context.Background())
	if !p.ReceiveRequest(d) {
		t.Fatal("admit")
	}
	select {
	case r := <-d.rejected:
		if r.st != rans.CodeError {
			t.Fatalf("panic reject status = %v, want StatusError", r.st)
		}
		// panic 回绝 = CodeError+ReasonHandler（panic 细节
		// 在 invoke 的 warn 日志留痕，不进回绝理由）。
		if r.reason != rans.ReasonHandler {
			t.Fatalf("panic reject reason = %v, want ReasonHandler", r.reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("panic must trigger Reject (reject-on-panic, roadmap 2.11)")
	}
	// loop 必须存活：后续投递正常。
	d2 := newRec(id)
	if !p.ReceiveRequest(d2) {
		t.Fatal("loop died after panic")
	}
	select {
	case <-d2.delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("second delivery never ran: loop died after panic")
	}
}

// TestActivatePanicDoesNotCrashProcess 钉死 Activate 钩子 panic 的最小 recover：
// panic 视作激活失败——在箱投递照旧回绝（StatusUnavailable——注册后失败
// 属执行前确定性失败），pawnite 照常抵达终态，进程不崩。
func TestActivatePanicDoesNotCrashProcess(t *testing.T) {
	id := mustUUID(t, "player", "panic2")
	d := newRec(id)
	p := New(id, Options{
		InboxCap: 4,
		Activate: func(ctx context.Context, p *Pawnite) error { panic("hook boom") },
	})
	p.Start(context.Background())
	if !p.ReceiveRequest(d) {
		t.Fatal("admit")
	}
	select {
	case r := <-d.rejected:
		if r.st != rans.CodeUnavailable {
			t.Fatalf("reject status after activate panic = %v, want StatusUnavailable", r.st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("activation panic must still reject inbox (hook recover, 2.11)")
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("pawnite must reach terminal state after activate panic")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed", p.State())
	}
}

// TestDeactivatePanicStillReachesDestroyed 钉死 Deactivate 钩子 panic 的最小
// recover（2.11）：吞掉并记日志，终态照达、Destroy 照常返回。
func TestDeactivatePanicStillReachesDestroyed(t *testing.T) {
	id := mustUUID(t, "player", "panic3")
	p := New(id, Options{
		Deactivate: func(ctx context.Context, p *Pawnite) error { panic("deact") },
	})
	p.Start(context.Background())
	destroyed := make(chan struct{})
	go func() {
		p.Destroy(context.Background())
		close(destroyed)
	}()
	select {
	case <-destroyed:
	case <-time.After(2 * time.Second):
		t.Fatal("Destroy blocked on panicking Deactivate")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed despite Deactivate panic", p.State())
	}
}

// TestOnEvictPanicStillReachesDestroyed 钉死 OnEvict 钩子 panic 的最小 recover
// （2.11）：仅记录不传播——Destroy 照常返回、状态已是 Destroyed。
func TestOnEvictPanicStillReachesDestroyed(t *testing.T) {
	id := mustUUID(t, "player", "panic4")
	p := New(id, Options{
		OnEvict: func(p *Pawnite) { panic("evict") },
	})
	p.Start(context.Background())
	destroyed := make(chan struct{})
	go func() {
		p.Destroy(context.Background())
		close(destroyed)
	}()
	select {
	case <-destroyed:
	case <-time.After(2 * time.Second):
		t.Fatal("Destroy blocked on panicking OnEvict")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed despite OnEvict panic", p.State())
	}
}

// ---- panic 监管策略（Isolate/Default 分叉面）----

// TestPanicPolicyZeroValueIsDefault：Options 零值的 PanicPolicy 必须是
// PanicPolicyDefault——零值兼容是消费方无感升级的前提。
func TestPanicPolicyZeroValueIsDefault(t *testing.T) {
	if PanicPolicyDefault != 0 {
		t.Fatalf("PanicPolicyDefault = %d, want 0 (zero value of Options.PanicPolicy)", PanicPolicyDefault)
	}
	var o Options
	if o.PanicPolicy != PanicPolicyDefault {
		t.Fatalf("zero Options.PanicPolicy = %v, want PanicPolicyDefault", o.PanicPolicy)
	}
}

// TestInvokePanicIsolate_TerminatesEntity：Isolate 策略下 Deliver panic →
// 对端仍先收到 StatusError 回绝（Reject-on-panic 不变），随后实体就地
// 销毁（隔离）：Done 关闭、State Destroyed、后续 ReceiveRequest 回绝。
func TestInvokePanicIsolate_TerminatesEntity(t *testing.T) {
	id := mustUUID(t, "player", "iso1")
	d := newRec(id)
	d.panicOnDeliver = true
	p := New(id, Options{InboxCap: 4, PanicPolicy: PanicPolicyIsolate})
	p.Start(context.Background())
	defer p.Destroy(context.Background())
	if !p.ReceiveRequest(d) {
		t.Fatal("admit")
	}
	// 回绝先行：隔离不吞掉对端通知。
	select {
	case r := <-d.rejected:
		if r.st != rans.CodeError {
			t.Fatalf("panic reject status = %v, want StatusError", r.st)
		}
		// panic 回绝 = CodeError+ReasonHandler（panic 细节
		// 在 invoke 的 warn 日志留痕，不进回绝理由）。
		if r.reason != rans.ReasonHandler {
			t.Fatalf("panic reject reason = %v, want ReasonHandler", r.reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("isolate policy must still reject the panicking delivery")
	}
	// 隔离：实体终局落位。
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("isolate policy: invoke panic must terminate the entity")
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed after isolation", p.State())
	}
	// 后续准入被回绝（Destroyed）。
	d2 := newRec(id)
	if p.ReceiveRequest(d2) {
		t.Fatal("post-isolation admission must be rejected")
	}
	select {
	case r := <-d2.rejected:
		if r.st != rans.CodeUnavailable {
			t.Fatalf("post-isolation reject status = %v, want StatusUnavailable", r.st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("rejected admission must invoke Reject")
	}
}

// TestInvokePanicIsolate_QueuedDeliveriesDrained：隔离销毁把在箱滞留投递
// 按销毁语义回绝（Unavailable）——与 Destroy 的余量处置一致。
func TestInvokePanicIsolate_QueuedDeliveriesDrained(t *testing.T) {
	id := mustUUID(t, "player", "iso2")
	boom := newRec(id)
	boom.panicOnDeliver = true
	queued := newRec(id)
	p := New(id, Options{InboxCap: 4, PanicPolicy: PanicPolicyIsolate})
	p.Start(context.Background())
	defer p.Destroy(context.Background())
	if !p.ReceiveRequest(boom) || !p.ReceiveRequest(queued) {
		t.Fatal("admit")
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("isolation must terminate the entity")
	}
	select {
	case r := <-queued.rejected:
		if r.st != rans.CodeUnavailable {
			t.Fatalf("queued delivery reject status = %v, want StatusUnavailable", r.st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued delivery must be drained on isolation")
	}
}

// TestInvokePanicIsolate_FreshInstanceServes：隔离是实体级而非身份级——
// 同 id 新实例（下次激活的形态）照常服役。
func TestInvokePanicIsolate_FreshInstanceServes(t *testing.T) {
	id := mustUUID(t, "player", "iso3")
	boom := newRec(id)
	boom.panicOnDeliver = true
	p1 := New(id, Options{InboxCap: 4, PanicPolicy: PanicPolicyIsolate})
	p1.Start(context.Background())
	if !p1.ReceiveRequest(boom) {
		t.Fatal("admit")
	}
	select {
	case <-p1.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("isolation must terminate the first instance")
	}

	p2 := New(id, Options{InboxCap: 4, PanicPolicy: PanicPolicyIsolate})
	p2.Start(context.Background())
	defer p2.Destroy(context.Background())
	d := newRec(id)
	if !p2.ReceiveRequest(d) {
		t.Fatal("fresh instance must admit")
	}
	select {
	case <-d.delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("fresh instance must serve deliveries (entity-level isolation)")
	}
}

// TestPanicPolicyIsolate_HookPanicsSameAsDefault：策略面只作用于 invoke
// panic——hook panic 两策略语义一致：Activate panic 仍按激活失败终态；
// Deactivate/OnEvict panic 仍吞掉记日志、终态照达。
func TestPanicPolicyIsolate_HookPanicsSameAsDefault(t *testing.T) {
	// Activate panic → 激活失败 → 终态（Default 既有语义）。
	id := mustUUID(t, "player", "iso4")
	d := newRec(id)
	pa := New(id, Options{
		InboxCap:    4,
		PanicPolicy: PanicPolicyIsolate,
		Activate:    func(ctx context.Context, p *Pawnite) error { panic("hook boom") },
	})
	pa.Start(context.Background())
	if !pa.ReceiveRequest(d) {
		t.Fatal("admit")
	}
	select {
	case r := <-d.rejected:
		if r.st != rans.CodeUnavailable {
			t.Fatalf("activate-panic reject status = %v, want StatusUnavailable (pre-execution failure)", r.st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("activate panic must still reject inbox")
	}
	select {
	case <-pa.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("activate panic must reach terminal state under Isolate")
	}
	if pa.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed", pa.State())
	}

	// Deactivate panic → 吞掉、Destroy 照常返回、终态照达。
	pd := New(id, Options{
		PanicPolicy: PanicPolicyIsolate,
		Deactivate:  func(ctx context.Context, p *Pawnite) error { panic("deact") },
	})
	pd.Start(context.Background())
	destroyed := make(chan struct{})
	go func() {
		pd.Destroy(context.Background())
		close(destroyed)
	}()
	select {
	case <-destroyed:
	case <-time.After(2 * time.Second):
		t.Fatal("Destroy blocked on panicking Deactivate under Isolate")
	}
	if pd.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed despite Deactivate panic", pd.State())
	}

	// OnEvict panic → 吞掉、终态照达。
	pe := New(id, Options{
		PanicPolicy: PanicPolicyIsolate,
		OnEvict:     func(p *Pawnite) { panic("evict") },
	})
	pe.Start(context.Background())
	destroyed2 := make(chan struct{})
	go func() {
		pe.Destroy(context.Background())
		close(destroyed2)
	}()
	select {
	case <-destroyed2:
	case <-time.After(2 * time.Second):
		t.Fatal("Destroy blocked on panicking OnEvict under Isolate")
	}
	if pe.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed despite OnEvict panic", pe.State())
	}
}

// TestPanicWarningGoesThroughFacade 门面可换证明：库包
// 日志出口统一走 log 门面——注入捕获 logger 后，panic 告警被捕获且带 uuid
// 定位字段；恢复后不再进捕获（RANS_LOG_FACADE 契约：库包不持 logger 实例）。
func TestPanicWarningGoesThroughFacade(t *testing.T) {
	var mu sync.Mutex
	var buf bytes.Buffer
	log.SetLogger(slog.New(slog.NewTextHandler(&muWriter{mu: &mu, buf: &buf}, nil)))
	t.Cleanup(func() { log.SetLogger(nil) })

	id := mustUUID(t, "player", "paniclog")
	d := newRec(id)
	d.panicOnDeliver = true
	p := New(id, Options{InboxCap: 4})
	p.Start(context.Background())
	defer p.Destroy(context.Background())
	if !p.ReceiveRequest(d) {
		t.Fatal("admit")
	}
	// Warn 先于 Reject（同一 defer 内顺序执行）：收到回绝即可安全断言缓冲。
	select {
	case r := <-d.rejected:
		if r.st != rans.CodeError {
			t.Fatalf("panic reject status = %v, want StatusError", r.st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("panic must trigger Reject")
	}
	mu.Lock()
	logged := buf.String()
	mu.Unlock()
	if !strings.Contains(logged, "pawnite delivery panicked") {
		t.Fatalf("facade must capture panic warning, got %q", logged)
	}
	if !strings.Contains(logged, id.String()) {
		t.Fatalf("panic warning must carry uuid, got %q", logged)
	}

	// 恢复默认：后续 panic 告警不再进捕获。
	log.SetLogger(nil)
	d2 := newRec(id)
	d2.panicOnDeliver = true
	if !p.ReceiveRequest(d2) {
		t.Fatal("loop died")
	}
	select {
	case <-d2.rejected:
	case <-time.After(2 * time.Second):
		t.Fatal("second panic must still reject")
	}
	mu.Lock()
	still := buf.String()
	mu.Unlock()
	if n := strings.Count(still, "pawnite delivery panicked"); n != 1 {
		t.Fatalf("restored facade must not capture, warnings=%d want 1", n)
	}
}

// muWriter 线程安全的缓冲（告警来自 loop goroutine，与断言并发）。
type muWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w *muWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
