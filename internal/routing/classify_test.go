package routing

import (
	"context"
	"errors"
	"fmt"
	"github.com/cyinx/rans_golang"
	"testing"
)

// TestClassifyTruthTable 分类真值表（规格 §9.2，以 *rans.Error
// 形态表达——内部形态 RemoteError/TimeoutError/ErrSendFailed 已随统一
// 错误模型退役）：覆盖裸值、单层/嵌套 %w 包装链与组合链。真网关路径的
// 连接类锚点在 rpc/gateway_test.go（SendOneWay 重试耗尽 → Unavailable+
// SendFailed），本表以 *rans.Error 构造验证识别面。
func TestClassifyTruthTable(t *testing.T) {
	notFound := rans.NewError(rans.CodeNotFound, rans.ReasonStaleLocation, "stale location")
	unavail := rans.NewError(rans.CodeUnavailable, rans.ReasonNone, "inbox full")
	busy := rans.NewError(rans.CodeUnavailable, rans.ReasonBusy, "inbox full")
	appErr := rans.NewError(rans.CodeError, rans.ReasonHandler, "handler failed")
	decodeErr := rans.NewError(rans.CodeDecode, rans.ReasonNone, "bad envelope")
	timeout := rans.NewError(rans.CodeTimeout, rans.ReasonNone, "request timeout")
	nodeDown := rans.NewError(rans.CodeConnectionLost, rans.ReasonNone, "node 7 lost")

	cases := []struct {
		name string
		err  error
		want Class
	}{
		{"nil error", nil, NoRetry},
		{"bare send-failed shape", rans.NewError(rans.CodeUnavailable, rans.ReasonSendFailed, "send failed"), RetryUnconditional},
		{"wrapped send-failed", fmt.Errorf("dial: %w", rans.NewError(rans.CodeUnavailable, rans.ReasonSendFailed, "send failed")), RetryUnconditional},
		{"nested wrapped send-failed",
			fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", rans.NewError(rans.CodeUnavailable, rans.ReasonSendFailed, "send failed"))), RetryUnconditional},
		{"send-failed first in mixed chain (first-wins)",
			fmt.Errorf("mixed: %w / %w", rans.NewError(rans.CodeUnavailable, rans.ReasonSendFailed, "send failed"), appErr), RetryUnconditional},
		{"bare node-unavailable", nodeDown, RetryAmbiguous},
		{"wrapped node-unavailable", fmt.Errorf("call: %w", nodeDown), RetryAmbiguous},
		{"bare remote not-found", notFound, RetryNotFound},
		{"wrapped remote not-found", fmt.Errorf("call: %w", notFound), RetryNotFound},
		// 分类只看链上首个 *rans.Error（产生方），没有跨形态
		// 优先级——真实产生点只发单一 *rans.Error，混合链仅存于显式 %w 组合
		// （first-wins 契约锚在 TestClassifyFirstErrorWins）。
		{"timeout first in mixed chain (first-wins)",
			fmt.Errorf("mixed: %w / %w", timeout, notFound), RetryAmbiguous},
		// Unavailable 重试契约（docs/decisions/2026-10-09-hr2-unavailable-retry.md）：
		// Unavailable 全部由运行时在 admission 之前回绝产生——确定未执行，
		// 无条件重试（Busy/Overloaded 过载保护例外见分类唯一表）。
		{"bare remote unavailable", unavail, RetryUnconditional},
		{"wrapped remote unavailable", fmt.Errorf("node: %w", unavail), RetryUnconditional},
		{"unavailable first in mixed chain (first-wins)",
			fmt.Errorf("mixed: %w / %w", unavail, timeout), RetryUnconditional},
		// 过载保护：Busy 不自动重试。
		{"busy overload guard", busy, NoRetry},
		{"bare timeout", timeout, RetryAmbiguous},
		{"nested wrapped timeout",
			fmt.Errorf("a: %w", fmt.Errorf("b: %w", timeout)), RetryAmbiguous},
		{"remote application error", appErr, NoRetry},
		{"remote decode error", decodeErr, NoRetry},
		{"plain error", errors.New("plain"), NoRetry},
		{"ctx canceled", context.Canceled, NoRetry},
	}
	for _, tc := range cases {
		if got := Classify(tc.err); got != tc.want {
			t.Errorf("%s: Classify(%v) = %d, want %d", tc.name, tc.err, got, tc.want)
		}
	}
}

// TestClassifyUnifiedTable 分类唯一表（约定 spec §5.3 重试分类表，
// Classify 的全部判据）：分类只看 *rans.Error 的 Code/Reason，逐行一例；
// 另含 %w 包装链与非 rans 错误的边界行。
func TestClassifyUnifiedTable(t *testing.T) {
	mk := func(c rans.Code, r rans.Reason) error { return rans.NewError(c, r, "table row") }
	cases := []struct {
		name string
		err  error
		want Class
	}{
		{"nil error", nil, NoRetry},
		// Unavailable（Busy/Overloaded 除外）：确定未执行 → 无条件重试。
		{"unavailable no reason", mk(rans.CodeUnavailable, rans.ReasonNone), RetryUnconditional},
		{"unavailable send-failed", mk(rans.CodeUnavailable, rans.ReasonSendFailed), RetryUnconditional},
		{"unavailable deactivating", mk(rans.CodeUnavailable, rans.ReasonDeactivating), RetryUnconditional},
		{"unavailable draining", mk(rans.CodeUnavailable, rans.ReasonDraining), RetryUnconditional},
		{"unavailable lease-lost", mk(rans.CodeUnavailable, rans.ReasonLeaseLost), RetryUnconditional},
		{"unavailable activation-failed", mk(rans.CodeUnavailable, rans.ReasonActivationFailed), RetryUnconditional},
		{"unavailable no-eligible-node", mk(rans.CodeUnavailable, rans.ReasonNoEligibleNode), RetryUnconditional},
		{"wrapped unavailable", fmt.Errorf("call: %w", mk(rans.CodeUnavailable, rans.ReasonNone)), RetryUnconditional},
		// Unavailable + Busy/Overloaded：确定未执行，但过载保护不自动重试。
		{"unavailable busy", mk(rans.CodeUnavailable, rans.ReasonBusy), NoRetry},
		{"unavailable overloaded", mk(rans.CodeUnavailable, rans.ReasonOverloaded), NoRetry},
		// NotFound + StaleLocation/UnknownKind：重解析位置后重试。
		{"not-found stale-location", mk(rans.CodeNotFound, rans.ReasonStaleLocation), RetryNotFound},
		{"not-found unknown-kind", mk(rans.CodeNotFound, rans.ReasonUnknownKind), RetryNotFound},
		{"wrapped not-found stale-location",
			fmt.Errorf("a: %w", fmt.Errorf("b: %w", mk(rans.CodeNotFound, rans.ReasonStaleLocation))), RetryNotFound},
		// NotFound + NotLocal：管理操作，不重试。
		{"not-found not-local", mk(rans.CodeNotFound, rans.ReasonNotLocal), NoRetry},
		// Timeout/ConnectionLost：可能已执行 → 仅 Idempotent（环内 RetryAmbiguous）。
		{"timeout", mk(rans.CodeTimeout, rans.ReasonNone), RetryAmbiguous},
		{"connection-lost", mk(rans.CodeConnectionLost, rans.ReasonNone), RetryAmbiguous},
		{"wrapped connection-lost", fmt.Errorf("node: %w", mk(rans.CodeConnectionLost, rans.ReasonNone)), RetryAmbiguous},
		// Canceled：调用方放弃，不重试。
		{"canceled", mk(rans.CodeCanceled, rans.ReasonNone), NoRetry},
		// 其余：确定性失败，不重试。
		{"error handler", mk(rans.CodeError, rans.ReasonHandler), NoRetry},
		{"error internal", mk(rans.CodeError, rans.ReasonInternal), NoRetry},
		{"error unsupported", mk(rans.CodeError, rans.ReasonUnsupported), NoRetry},
		{"error migration-lost", mk(rans.CodeError, rans.ReasonMigrationLost), NoRetry},
		// Cycle（H12 决策 15）：准入判环的确定性回绝——重试必然再成环，
		// 不可重试是语义要求而非缺省归类的巧合，独立成行锁死。
		{"error cycle", mk(rans.CodeError, rans.ReasonCycle), NoRetry},
		{"error no reason", mk(rans.CodeError, rans.ReasonNone), NoRetry},
		{"serialization", mk(rans.CodeSerialization, rans.ReasonNone), NoRetry},
		{"decode", mk(rans.CodeDecode, rans.ReasonNone), NoRetry},
		{"plain non-rans error", errors.New("plain"), NoRetry},
		{"ctx canceled", context.Canceled, NoRetry},
	}
	for _, tc := range cases {
		if got := Classify(tc.err); got != tc.want {
			t.Errorf("%s: Classify(%v) = %d, want %d", tc.name, tc.err, got, tc.want)
		}
	}
}

// TestClassifyFirstErrorWins 链上多个 *rans.Error 时取链上首个（errors.As
// 语义）——统一模型下链上多错误只出现在显式 %w 组合，取首即产生方。
func TestClassifyFirstErrorWins(t *testing.T) {
	head := rans.NewError(rans.CodeUnavailable, rans.ReasonNone, "head")
	tail := rans.NewError(rans.CodeTimeout, rans.ReasonNone, "tail")
	if got := Classify(fmt.Errorf("%w: %w", head, tail)); got != RetryUnconditional {
		t.Fatalf("Classify(chain head=unavailable tail=timeout) = %d, want RetryUnconditional (first wins)", got)
	}
}

// TestDefaultOptions 默认选项必须落在保守侧：零值非幂等——模糊投递类失败
// 对未标注调用上抛，重试副作用风险由标注者显式承担。
func TestDefaultOptions(t *testing.T) {
	if opt := DefaultOptions(); opt.Idempotent {
		t.Fatal("DefaultOptions must be non-idempotent (conservative side)")
	}
	var zero InvokeOptions
	if zero.Idempotent {
		t.Fatal("InvokeOptions zero value must be non-idempotent")
	}
}
