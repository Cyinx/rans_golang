package routing

import (
	"context"
	"errors"
	"fmt"
	"github.com/cyinx/rans_golang"
	"sync"
	"testing"
	"time"
)

// errSeq 返回按序投放错误、末尾固定 sentinel 的 attempt 闭包，同时记录
// 每次调用次数与每次调用是否重建了载荷（载荷工厂契约：每次 attempt 必须
// 重建 envelope——由调用方闭包保证，此处用 buildCount 锚定「每次都调了
// 构造器」）。
type attemptRecorder struct {
	mu         sync.Mutex
	fails      []error // 前 len(fails) 次返回的失败；之后返回 successErr=nil
	attempts   int
	builds     int
	backoffGot []int
}

func (r *attemptRecorder) attempt(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.attempts
	r.attempts++
	if n < len(r.fails) {
		return r.fails[n]
	}
	return nil
}

func (r *attemptRecorder) build() { r.mu.Lock(); r.builds++; r.mu.Unlock() }

func (r *attemptRecorder) attemptWithBuild(ctx context.Context) error {
	r.build()
	return r.attempt(ctx)
}

func (r *attemptRecorder) count() int { r.mu.Lock(); defer r.mu.Unlock(); return r.attempts }

// zeroBackoff 测试注入：零等待并记录收到的 attempt 序号。
func zeroBackoff(r *attemptRecorder) func(int) time.Duration {
	return func(n int) time.Duration {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.backoffGot = append(r.backoffGot, n)
		return 0
	}
}

// 统一错误模型形态的错误构造器（真网关路径的 Unavailable+SendFailed 形态
// 见 rpc/gateway_test.go）。
var (
	sendFailed = func() error {
		return rans.NewError(rans.CodeUnavailable, rans.ReasonSendFailed, "send failed")
	}
	remoteNotFound = func() error {
		return rans.NewError(rans.CodeNotFound, rans.ReasonStaleLocation, "stale location")
	}
	remoteUnavailable = func() error {
		return rans.NewError(rans.CodeUnavailable, rans.ReasonNone, "inbox full")
	}
	remoteTimeout = func() error {
		return rans.NewError(rans.CodeTimeout, rans.ReasonNone, "request timeout")
	}
	remoteAppErr = func() error {
		return rans.NewError(rans.CodeError, rans.ReasonHandler, "handler boom")
	}
)

// TestRetryRingUnconditionalRetriesToSuccess 无条件类（确定未投递）重试到
// 成功：send-failed → send-failed → nil，环在 3 次尝试内成功返回 nil。
func TestRetryRingUnconditionalRetriesToSuccess(t *testing.T) {
	rec := &attemptRecorder{fails: []error{
		sendFailed(),
		sendFailed(),
	}}
	if err := InvokeWithBackoff(context.Background(), DefaultOptions(), zeroBackoff(rec), rec.attempt); err != nil {
		t.Fatalf("expected success after unconditional retries, got %v", err)
	}
	if got := rec.count(); got != 3 {
		t.Fatalf("attempts = %d, want 3 (initial + 2 retries)", got)
	}
}

// TestRetryRingNotFoundRetries 位置陈旧类（对端 NotFound）重试：第二次成功。
func TestRetryRingNotFoundRetries(t *testing.T) {
	rec := &attemptRecorder{fails: []error{remoteNotFound()}}
	if err := InvokeWithBackoff(context.Background(), DefaultOptions(), zeroBackoff(rec), rec.attempt); err != nil {
		t.Fatalf("expected success after NotFound retry, got %v", err)
	}
	if got := rec.count(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

// TestRetryRingAmbiguousOnlyWhenIdempotent 模糊投递类仅在 Idempotent 标注
// 时重试：标注 → 重试到成功；未标注 → 首败即上抛。
func TestRetryRingAmbiguousOnlyWhenIdempotent(t *testing.T) {
	t.Run("idempotent retries to success", func(t *testing.T) {
		rec := &attemptRecorder{fails: []error{remoteTimeout(), remoteUnavailable()}}
		opts := InvokeOptions{Idempotent: true}
		if err := InvokeWithBackoff(context.Background(), opts, zeroBackoff(rec), rec.attempt); err != nil {
			t.Fatalf("expected success after idempotent ambiguous retries, got %v", err)
		}
		if got := rec.count(); got != 3 {
			t.Fatalf("attempts = %d, want 3", got)
		}
	})
	t.Run("non-idempotent raises immediately", func(t *testing.T) {
		rec := &attemptRecorder{fails: []error{remoteTimeout()}}
		err := InvokeWithBackoff(context.Background(), DefaultOptions(), zeroBackoff(rec), rec.attempt)
		if err == nil {
			t.Fatal("non-idempotent ambiguous failure must raise immediately")
		}
		if got := rec.count(); got != 1 {
			t.Fatalf("attempts = %d, want 1 (no retry without Idempotent)", got)
		}
	})
}

// TestRetryRingCapExhaustedReturnsLastError 上限 2 次重试（共 3 次 attempt）：
// 之后放弃并返回末次错误（非首次错误）。
func TestRetryRingCapExhaustedReturnsLastError(t *testing.T) {
	first := fmt.Errorf("send#0: %w", sendFailed())
	mid := fmt.Errorf("send#1: %w", sendFailed())
	last := fmt.Errorf("send#2: %w", sendFailed())
	rec := &attemptRecorder{fails: []error{first, mid, last}}
	got := InvokeWithBackoff(context.Background(), DefaultOptions(), zeroBackoff(rec), rec.attempt)
	if !errors.Is(got, last) {
		t.Fatalf("cap-exhausted error = %v, want the LAST attempt error %v", got, last)
	}
	if errors.Is(got, first) {
		t.Fatal("must not return the first attempt error")
	}
	if n := rec.count(); n != 3 {
		t.Fatalf("attempts = %d, want exactly 3 (cap 2 retries)", n)
	}
}

// TestRetryRingNoRetryImmediate 确定性失败（对端回错/未知错误）直抛，零重试。
func TestRetryRingNoRetryImmediate(t *testing.T) {
	for name, mkErr := range map[string]func() error{
		"remote application error": remoteAppErr,
		"plain error":              func() error { return errors.New("plain") },
		"ctx canceled":             func() error { return context.Canceled },
	} {
		t.Run(name, func(t *testing.T) {
			rec := &attemptRecorder{fails: []error{mkErr()}}
			err := InvokeWithBackoff(context.Background(), DefaultOptions(), zeroBackoff(rec), rec.attempt)
			if err == nil {
				t.Fatal("NoRetry failure must be returned")
			}
			if !errors.Is(err, rec.fails[0]) {
				t.Fatalf("returned error = %v, want original %v", err, rec.fails[0])
			}
			if n := rec.count(); n != 1 {
				t.Fatalf("attempts = %d, want 1 (NoRetry → no retry)", n)
			}
		})
	}
}

// TestRetryRingBackoffSequence 退避序列：注入 backoff 记录调用序号——两次
// 重试分别收到 0、1（0 基失败尝试号），与默认 (n+1)*20ms 递增序列一致。
func TestRetryRingBackoffSequence(t *testing.T) {
	rec := &attemptRecorder{fails: []error{
		fmt.Errorf("a: %w", sendFailed()),
		fmt.Errorf("b: %w", sendFailed()),
		fmt.Errorf("c: %w", sendFailed()),
	}}
	_ = InvokeWithBackoff(context.Background(), DefaultOptions(), zeroBackoff(rec), rec.attempt)
	if len(rec.backoffGot) != 2 || rec.backoffGot[0] != 0 || rec.backoffGot[1] != 1 {
		t.Fatalf("backoff call sequence = %v, want [0 1]", rec.backoffGot)
	}
}

// TestDefaultBackoff 默认退避 = (n+1)*20ms（规格 §9.2）。
func TestDefaultBackoff(t *testing.T) {
	for n, want := range map[int]time.Duration{0: 20 * time.Millisecond, 1: 40 * time.Millisecond, 2: 60 * time.Millisecond} {
		if got := DefaultBackoff(n); got != want {
			t.Fatalf("DefaultBackoff(%d) = %v, want %v", n, got, want)
		}
	}
}

// TestRetryRingRealBackoffTimingBounds 真实短退避时序界：注入 25ms 固定
// 退避 × 2 次重试，断言环总耗时落在 [50ms, 2s) 内（下界证明确实等待）。
func TestRetryRingRealBackoffTimingBounds(t *testing.T) {
	rec := &attemptRecorder{fails: []error{
		fmt.Errorf("a: %w", sendFailed()),
		fmt.Errorf("b: %w", sendFailed()),
	}}
	start := time.Now()
	_ = InvokeWithBackoff(context.Background(), DefaultOptions(), func(int) time.Duration { return 25 * time.Millisecond }, rec.attempt)
	elapsed := time.Since(start)
	if elapsed < 50*time.Millisecond {
		t.Fatalf("elapsed %v < 50ms: backoff not honored", elapsed)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("elapsed %v >= 2s: backoff runaway", elapsed)
	}
}

// TestRetryRingCtxCancelDuringBackoffAborts ctx 在退避期间取消 → 中止且
// 不再调 attempt，收场为 CtxOutcome（票 13：取消→Canceled 的 *rans.Error，
// 不再泄漏上一次尝试的错误）。
func TestRetryRingCtxCancelDuringBackoffAborts(t *testing.T) {
	rec := &attemptRecorder{fails: []error{sendFailed()}}
	ctx, cancel := context.WithCancel(context.Background())
	cancellingBackoff := func(int) time.Duration {
		cancel() // 在退避期间取消
		return time.Hour
	}
	got := InvokeWithBackoff(ctx, DefaultOptions(), cancellingBackoff, rec.attempt)
	if rans.CodeOf(got) != rans.CodeCanceled {
		t.Fatalf("cancelled-during-backoff error = %v (code=%v), want Canceled", got, rans.CodeOf(got))
	}
	if n := rec.count(); n != 1 {
		t.Fatalf("attempts = %d, want 1 (cancel must abort before retry)", n)
	}
}

// TestRetryRingCtxCancelBeforeBackoffAborts attempt 失败的同时取消 ctx：
// 环进入退避时 ctx 已 done → 中止并收场为 CtxOutcome（票 13：取消→
// Canceled），不再调 attempt。
func TestRetryRingCtxCancelBeforeBackoffAborts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	stale := remoteNotFound()
	attempt := func(context.Context) error {
		calls++
		cancel() // 失败的同时 ctx 已取消
		return stale
	}
	got := InvokeWithBackoff(ctx, DefaultOptions(), func(int) time.Duration { return 0 }, attempt)
	if rans.CodeOf(got) != rans.CodeCanceled {
		t.Fatalf("ctx-done-at-backoff error = %v (code=%v), want Canceled", got, rans.CodeOf(got))
	}
	if calls != 1 {
		t.Fatalf("attempts = %d, want 1 (no retry when ctx done before backoff)", calls)
	}
}

// TestRetryRingAttemptPanicPropagates attempt panic 必须穿透（不被环吞掉
// 改写为错误），且 panic 后不再重试。
func TestRetryRingAttemptPanicPropagates(t *testing.T) {
	calls := 0
	attempt := func(ctx context.Context) error {
		calls++
		panic("boom in attempt")
	}
	defer func() {
		r := recover()
		if r != "boom in attempt" {
			t.Fatalf("panic must propagate unchanged, got %v", r)
		}
		if calls != 1 {
			t.Fatalf("attempts after panic = %d, want 1", calls)
		}
	}()
	_ = InvokeWithBackoff(context.Background(), DefaultOptions(), zeroBackoff(&attemptRecorder{}), attempt)
	t.Fatal("unreachable: panic must propagate")
}

// TestRetryRingPayloadFactoryContract 载荷工厂契约锚点：每次 attempt 前构
// 造器都被重新调用（builds == attempts），不复用首次载荷。
func TestRetryRingPayloadFactoryContract(t *testing.T) {
	rec := &attemptRecorder{fails: []error{
		fmt.Errorf("a: %w", sendFailed()),
		fmt.Errorf("b: %w", sendFailed()),
	}}
	if err := InvokeWithBackoff(context.Background(), DefaultOptions(), zeroBackoff(rec), rec.attemptWithBuild); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if rec.builds != rec.attempts {
		t.Fatalf("payload builds = %d, attempts = %d: factory must run per attempt", rec.builds, rec.attempts)
	}
}

// TestInvokeUsesDefaultBackoff Invoke = InvokeWithBackoff + 默认退避：等价
// 面仅断言行为（失败→重试到成功）；默认值精确断言在 TestDefaultBackoff。
func TestInvokeUsesDefaultBackoff(t *testing.T) {
	rec := &attemptRecorder{fails: []error{remoteNotFound()}}
	if err := Invoke(context.Background(), DefaultOptions(), rec.attempt); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if n := rec.count(); n != 2 {
		t.Fatalf("attempts = %d, want 2", n)
	}
}

// TestShouldEnterRingTruthTable ShouldEnterRing 真值表：可重试类为 true
// （进入环），NoRetry（含 nil、Busy 过载保护）为 false。
func TestShouldEnterRingTruthTable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"send-failed", sendFailed(), true},
		{"remote not-found", remoteNotFound(), true},
		{"remote unavailable", remoteUnavailable(), true},
		{"timeout", remoteTimeout(), true},
		{"wrapped ambiguous", fmt.Errorf("call: %w", remoteTimeout()), true},
		{"remote application error", remoteAppErr(), false},
		{"plain error", errors.New("plain"), false},
		{"ctx canceled", context.Canceled, false},
	}
	for _, tc := range cases {
		if got := ShouldEnterRing(tc.err); got != tc.want {
			t.Errorf("%s: ShouldEnterRing(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}
