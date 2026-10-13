package activation

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

func TestCall_RoundTrip(t *testing.T) {
	m := newTestManager(t)
	id, _ := rans.NewID("counter", "call1")
	got, err := m.InvokeLocal(context.Background(), id, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.(int) != 1 {
		t.Fatalf("got %v, want 1", got)
	}
	got, err = m.InvokeLocal(context.Background(), id, nil)
	if err != nil || got.(int) != 2 {
		t.Fatalf("got %v err %v, want 2", got, err)
	}
}

func TestCall_Timeout(t *testing.T) {
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"slow": func(_ rans.ID) rans.Entity { return &slowEntity{} },
	})
	id, _ := rans.NewID("slow", "s1")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := m.InvokeLocal(ctx, id, nil); !errors.Is(err, rans.ErrTimeout) {
		t.Fatalf("err = %v, want rans.ErrTimeout", err)
	}
}

type slowEntity struct{}

func (s *slowEntity) Activate(ctx context.Context) error { return nil }
func (s *slowEntity) Handle(ctx context.Context, req any) (any, error) {
	select {
	case <-ctx.Done(): // 传播调用方取消
		return nil, ctx.Err()
	case <-time.After(5 * time.Second):
		return "slow", nil
	}
}
func (s *slowEntity) Deactivate(ctx context.Context) error { return nil }

func TestCall_UnknownKind(t *testing.T) {
	m := newTestManager(t)
	id, _ := rans.NewID("nope", "x")
	if _, err := m.InvokeLocal(context.Background(), id, nil); err == nil {
		t.Fatal("expected error for unknown kind")
	}
}

func TestCall_SerializesOnEntity(t *testing.T) {
	// 同一实体上的并发 Call 必须串行执行（状态累加无竞争）。
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &counterEntity{} },
	})
	id, _ := rans.NewID("counter", "serial1")
	const n = 20
	done := make(chan int, n)
	for i := 0; i < n; i++ {
		go func() {
			v, err := m.InvokeLocal(context.Background(), id, nil)
			if err != nil {
				t.Error(err)
				done <- 0
				return
			}
			done <- v.(int)
		}()
	}
	max := 0
	for i := 0; i < n; i++ {
		if v := <-done; v > max {
			max = v
		}
	}
	if max != n {
		t.Fatalf("max counter = %d, want %d (serialization broken)", max, n)
	}
}

// ---- Reject 状态码映射 ----

// newTestDelivery 构造最小 callDelivery（handler 恒成功——本组用例只走
// Reject 路径，Deliver 不会被触发）。
func newTestDelivery(id rans.ID) *callDelivery {
	return &callDelivery{
		id:      id,
		handler: func(ctx context.Context, req any) (any, error) { return "ok", nil },
		ctx:     context.Background(),
		reply:   make(chan callResult, 1),
	}
}

// drainReply 非阻塞读一次回执：Reject 同步投递（缓冲 1），Reject 返回后
// 通道内容即定局，无内容返回 false。
func drainReply(c *callDelivery) (callResult, bool) {
	select {
	case r := <-c.reply:
		return r, true
	default:
		return callResult{}, false
	}
}

// assertNoSentinel 断言错误不命中任何重试相关哨兵（泛化/编解码错误不得
// 伪装成可判定重试语义）。
func assertNoSentinel(t *testing.T, err error) {
	t.Helper()
	for _, sentinel := range []error{rans.ErrTimeout, rans.ErrNotFound, rans.ErrUnavailable, ErrBusy} {
		if errors.Is(err, sentinel) {
			t.Fatalf("err = %v must not match sentinel %v", err, sentinel)
		}
	}
}

// TestCallDelivery_RejectStatusMapping Reject 状态码→错误面映射的契约表。
// NotFound/Timeout/序列化/解码四态的真实来源是远端 Error 帧（dispatcher
// reply → 源端 *rans.Error（统一形态，分类面）；本地投递按契约直驱。
// Error/Unavailable 两态另有经 pawnite 真实排空/准入路径的端到端用例
// （TestCallDelivery_*ViaPawnite 系列）。
func TestCallDelivery_RejectStatusMapping(t *testing.T) {
	id, _ := rans.NewID("counter", "reject-table")
	cases := []struct {
		name   string
		status rans.Code
		reason rans.Reason
		check  func(*testing.T, error)
	}{
		{"status error keeps reason text", rans.CodeError, rans.ReasonHandler, func(t *testing.T, err error) {
			if err == nil || !strings.Contains(err.Error(), "rejected") {
				t.Fatalf("err = %v, want rejected text", err)
			}
			assertNoSentinel(t, err)
		}},
		{"status not found maps sentinel", rans.CodeNotFound, rans.ReasonStaleLocation, func(t *testing.T, err error) {
			if !errors.Is(err, rans.ErrNotFound) {
				t.Fatalf("err = %v, want rans.ErrNotFound", err)
			}
		}},
		{"status timeout maps sentinel", rans.CodeTimeout, rans.ReasonNone, func(t *testing.T, err error) {
			if !errors.Is(err, rans.ErrTimeout) {
				t.Fatalf("err = %v, want rans.ErrTimeout", err)
			}
		}},
		{"status unavailable busy maps code+reason", rans.CodeUnavailable, rans.ReasonBusy, func(t *testing.T, err error) {
			if !errors.Is(err, rans.ErrUnavailable) {
				t.Fatalf("err = %v, want rans.ErrUnavailable (Code match)", err)
			}
			if rans.ReasonOf(err) != rans.ReasonBusy {
				t.Fatalf("ReasonOf = %d, want ReasonBusy", rans.ReasonOf(err))
			}
			// 同 Code 且回绝面哨兵：ErrBusy（Unavailable+Busy）判定命中。
			if !errors.Is(err, ErrBusy) {
				t.Fatalf("err = %v, ErrBusy (same Code+Reason) check must match", err)
			}
		}},
		{"serialization error maps code", rans.CodeSerialization, rans.ReasonNone, func(t *testing.T, err error) {
			if rans.CodeOf(err) != rans.CodeSerialization {
				t.Fatalf("CodeOf = %d, want CodeSerialization", rans.CodeOf(err))
			}
			assertNoSentinel(t, err)
		}},
		{"decode error maps code", rans.CodeDecode, rans.ReasonNone, func(t *testing.T, err error) {
			if rans.CodeOf(err) != rans.CodeDecode {
				t.Fatalf("CodeOf = %d, want CodeDecode", rans.CodeOf(err))
			}
			assertNoSentinel(t, err)
		}},
		{"unknown status maps generic", rans.Code(200), rans.ReasonNone, func(t *testing.T, err error) {
			if err == nil || !strings.Contains(err.Error(), "rejected") {
				t.Fatalf("err = %v, want rejected text", err)
			}
			assertNoSentinel(t, err)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestDelivery(id)
			c.Reject(tc.status, tc.reason)
			r, ok := drainReply(c)
			if !ok {
				t.Fatal("reject must deliver exactly one result")
			}
			if r.val != nil {
				t.Fatalf("val = %v, want nil", r.val)
			}
			tc.check(t, r.err)
		})
	}

	// StatusOK：no-op——成功状态不构成回绝，不得投递任何结果（伪造 nil
	// 错误应答会顶掉真实收场）。
	t.Run("status ok is a no-op", func(t *testing.T) {
		c := newTestDelivery(id)
		c.Reject(rans.CodeOK, rans.ReasonNone)
		if _, ok := drainReply(c); ok {
			t.Fatal("StatusOK reject must deliver nothing")
		}
	})
}

// waitState 有界轮询等待指定状态（同 manager_arbitration_test.go 手法）。
func waitState(t *testing.T, p *pawnite.Pawnite, want pawnite.State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for p.State() != want {
		if time.Now().After(deadline) {
			t.Fatalf("state = %v, want %v", p.State(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestCallDelivery_UnavailableViaPawniteAdmission 经 pawnite 准入回绝路径
// 端到端：Destroyed 单元上 ReceiveRequest 被拒（销毁中/已销毁语义，规格
// §4.2）→ Reject(Unavailable) 由 pawnite 终局矩阵真实发出 → 调用方收到
// Unavailable+Deactivating 的 *rans.Error（销毁族回绝）。
func TestCallDelivery_UnavailableViaPawniteAdmission(t *testing.T) {
	id, _ := rans.NewID("counter", "adm-rej")
	p := pawnite.New(id, pawnite.Options{})
	p.Destroy(context.Background()) // Start 前销毁：就地 Created→Destroying→Destroyed
	c := newTestDelivery(id)
	if p.ReceiveRequest(c) {
		t.Fatal("admission into destroyed pawnite must be rejected")
	}
	r, ok := drainReply(c)
	if !ok {
		t.Fatal("rejected admission must deliver a result")
	}
	if !errors.Is(r.err, rans.ErrUnavailable) || rans.ReasonOf(r.err) != rans.ReasonDeactivating {
		t.Fatalf("err = %v, want Unavailable+Deactivating (*rans.Error reject face)", r.err)
	}
}

// TestCallDelivery_UnavailableViaPawniteInboxFull 收件箱满回绝——Busy
// 的历史主场景：容量 1，第二条准入被拒 → Unavailable+ReasonBusy。
func TestCallDelivery_UnavailableViaPawniteInboxFull(t *testing.T) {
	id, _ := rans.NewID("counter", "inbox-full")
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
	if !errors.Is(r.err, rans.ErrUnavailable) || rans.ReasonOf(r.err) != rans.ReasonBusy {
		t.Fatalf("err = %v, want Unavailable+Busy (*rans.Error reject face)", r.err)
	}
}

// TestCallDelivery_ErrorViaPawniteActivationFailure 经 pawnite 激活失败
// 排空路径端到端：激活失败时在箱投递被 drainReject(StatusUnavailable,
// "activation failed") 回绝（规格 §12——注册后失败属执行前确定性
// 失败）→ 调用方得到 Unavailable+ActivationFailed 的 *rans.Error（异步窗
// 与同步窗同 (Code, Reason)——ErrActivationFailed 同判）。
func TestCallDelivery_ErrorViaPawniteActivationFailure(t *testing.T) {
	id, _ := rans.NewID("counter", "act-fail-drain")
	gate := make(chan struct{})
	p := pawnite.New(id, pawnite.Options{
		Activate: func(ctx context.Context, p *pawnite.Pawnite) error {
			<-gate
			return errors.New("store offline")
		},
	})
	p.Start(context.Background())
	waitState(t, p, pawnite.StateActivating) // 激活已入段且停在 gate（准入窗口确定性打开）
	c := newTestDelivery(id)
	if !p.ReceiveRequest(c) { // Activating 准入放行：消息留队待激活收场（规格 §12）
		t.Fatal("delivery during activation must be admitted and queued")
	}
	close(gate) // 激活失败 → 整箱排空回绝
	select {
	case r := <-c.reply:
		if !errors.Is(r.err, ErrActivationFailed) {
			t.Fatalf("err = %v, want ErrActivationFailed (Unavailable+ActivationFailed)", r.err)
		}
		if !errors.Is(r.err, rans.ErrUnavailable) {
			t.Fatalf("err = %v, want rans.ErrUnavailable (Code face)", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("activation-failure drain must reject the queued delivery")
	}
	<-p.Done() // 等 loop 终局收尾，测试不得先于 goroutine 退出
}

// failingCauseEntity Activate 失败于 cause。终局握手信号在目录包装
// unregisterSignalDir 上（复检握手用，配对约束下激活失败路径不再调用
// 用户 Deactivate）。
type failingCauseEntity struct {
	cause error
}

func (e *failingCauseEntity) Activate(context.Context) error { return e.cause }
func (e *failingCauseEntity) Handle(context.Context, any) (any, error) {
	return nil, nil
}
func (e *failingCauseEntity) Deactivate(context.Context) error { return nil }

// TestActivationFailedWrapsArbitraryCause 核验 ErrActivationFailed 的 %w
// 双链（fmt.Errorf("%w: %w", ...)）：哨兵
// 语义与任意根因同时可判（errors.Is 双向）。仲裁根因的 errors.As 变体由
// TestArbitrationLostErrorCarriesWinner 覆盖。创建区握手钉死入表后复检
// 分支（同 manager_arbitration_test.go 手法）使断言确定性成立。
func TestActivationFailedWrapsArbitraryCause(t *testing.T) {
	cause := errors.New("store: snapshot unreadable")
	dir := newUnregisterSignalDir()
	id, _ := rans.NewID("k", "w-cause")

	seated := make(chan struct{})
	proceed := make(chan struct{})
	ent := &failingCauseEntity{cause: cause}
	m := New(dir, 1, map[string]rans.Factory{
		"k": func(_ rans.ID) rans.Entity {
			close(seated)
			<-proceed // g1 停在工厂（锁外），重锁双检在其后
			return ent
		},
	})

	g1done := make(chan struct{})
	var err1 error
	go func() {
		defer close(g1done)
		_, err1 = m.GetOrActivate(context.Background(), id)
	}()

	<-seated         // g1 已停在工厂（锁外），manager 锁上无人
	m.readMu.RLock() // 读锁占位：挡住入表/evict 的写锁，不挡双检的读锁
	close(proceed)
	<-dir.entered // finish 已置 Destroyed，cleanup 进入（evict 停在锁前）
	m.readMu.RUnlock()
	<-g1done

	if err1 == nil {
		t.Fatal("activation failure must surface synchronously via post-insert recheck")
	}
	if !errors.Is(err1, ErrActivationFailed) {
		t.Fatalf("err1 = %v, activation-failure sentinel must survive wrapping", err1)
	}
	if !errors.Is(err1, cause) {
		t.Fatalf("err1 = %v, underlying cause must be reachable via the wrapping chain", err1)
	}
}

// TestCall_CancelSurfacesCtxErrNotTimeout：等待段不得把
// ctx.Done 一律折成 rans.ErrTimeout——那会让主动取消与超时混同，调用方无法
// 区分「我放弃了」与「执行超时」。契约：Deadline 仍 rans.ErrTimeout，取消
// 透传 ctx.Err。
func TestCall_CancelSurfacesCtxErrNotTimeout(t *testing.T) {
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity { return &gateCaptureEntity{} },
	})
	id := idleID(t, "cell", "call-cancel")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-call pawnite")
	entRaw, ok := m.EntityOf(id)
	if !ok {
		t.Fatal("entity not found")
	}
	ent := entRaw.(*gateCaptureEntity)
	ent.release = make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	callDone := make(chan error, 1)
	go func() {
		_, err := m.InvokeLocal(ctx, id, nil)
		callDone <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for !ent.inHandle.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !ent.inHandle.Load() {
		t.Fatal("handle not entered within 2s")
	}
	cancel()
	select {
	case err := <-callDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled call err = %v, want context.Canceled", err)
		}
		if errors.Is(err, rans.ErrTimeout) {
			t.Fatal("cancellation must not masquerade as rans.ErrTimeout")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Call did not return after cancel")
	}
	close(ent.release)
	if dp, ok := m.GetLocal(id); ok {
		_ = dp.Destroy(context.Background())
	}
}

// gateCaptureEntity：Handle 阻塞至放行；捕获时若 Handle 仍在途则记违规
// （与 internal/lifecycle 侧同名道具同语义——原定义随闲置复合体迁移，
// 本包 call 测试保留自身副本）。
type gateCaptureEntity struct {
	inHandle          atomic.Bool
	release           chan struct{}
	captureConcurrent atomic.Bool
}

func (e *gateCaptureEntity) Activate(ctx context.Context) error   { return nil }
func (e *gateCaptureEntity) Deactivate(ctx context.Context) error { return nil }
func (e *gateCaptureEntity) Handle(ctx context.Context, req any) (any, error) {
	e.inHandle.Store(true)
	<-e.release
	e.inHandle.Store(false)
	return nil, nil
}
func (e *gateCaptureEntity) Snapshot() ([]byte, error) {
	if e.inHandle.Load() {
		e.captureConcurrent.Store(true)
	}
	return []byte("s"), nil
}
func (e *gateCaptureEntity) Restore(b []byte) error { return nil }

// idleID 构造测试身份（与 internal/lifecycle 侧同名助手同语义）。
func idleID(t testing.TB, kind, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID(kind, inst)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
