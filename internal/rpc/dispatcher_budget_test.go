package rpc

// 远端 Handle ctx 的预算派生（H10 决策 6，S7 内部缝）：入站扩展块带
// budget_ms → Handle ctx deadline ≈ 收到时刻 + budget（锚定 newDelivery
// 构造时刻，入箱等待不重置预算）；未带 budget_ms → 本节点防御缺省
// （Calls.Default 装配值）。

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// deadlineProbe Handle 入口的 ctx deadline 观测口（channel 同步，免轮询）。
type deadlineProbe struct {
	got  chan time.Duration
	mu   sync.Mutex
	rem  time.Duration
	ok   bool
	once sync.Once
}

func newDeadlineProbe() *deadlineProbe {
	return &deadlineProbe{got: make(chan time.Duration, 4)}
}

func (p *deadlineProbe) handle(ctx context.Context, _ any) (any, error) {
	dl, has := ctx.Deadline()
	p.mu.Lock()
	p.rem, p.ok = time.Until(dl), has
	p.mu.Unlock()
	p.once.Do(func() { close(p.got) })
	return "ok", nil
}

func (p *deadlineProbe) wait(t *testing.T) (time.Duration, bool) {
	t.Helper()
	select {
	case <-p.got:
	case <-time.After(5 * time.Second):
		t.Fatal("entity Handle was not invoked")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rem, p.ok
}

// assertHandleDeadline 断言观测剩余 ≈ budget：上界 +50ms（clock 粒度）、
// 下界 -250ms（调度暂停容忍），与 testkit assertBudgetRemaining 同口径。
func assertHandleDeadline(t *testing.T, rem time.Duration, ok bool, budget time.Duration) {
	t.Helper()
	if !ok {
		t.Fatalf("Handle ctx 无 deadline：预算派生未生效（budget %s）", budget)
	}
	if rem > budget+50*time.Millisecond || rem < budget-250*time.Millisecond {
		t.Fatalf("Handle ctx 剩余 %s，want ≈%s", rem, budget)
	}
}

// extHeaderFor 构造带/不带 budget_ms 扩展块的入站请求头（target = cell/x）。
func extHeaderFor(id uint64, budget time.Duration, hasBudget bool) wire.Header {
	h := reqHdrFor(protocol.MsgRequest, id, "budget", nil)
	if !hasBudget {
		return h
	}
	h.Flags |= protocol.FlagHasExtensions
	h.Ext = wire.EncodeRequestExt(wire.RequestExt{
		BudgetMS:  uint64(budget.Milliseconds()),
		HasBudget: true,
	})
	return h
}

func TestDispatcherBudget_HandleCtxDeadline(t *testing.T) {
	t.Run("budget_ms_applied", func(t *testing.T) {
		p := newDeadlineProbe()
		env := newDispEnv(t, p.handle, false)
		env.feed(extHeaderFor(701, 800*time.Millisecond, true), envBody(t, 1))
		rem, ok := p.wait(t)
		assertHandleDeadline(t, rem, ok, 800*time.Millisecond)
	})

	t.Run("absent_budget_falls_back_to_default", func(t *testing.T) {
		p := newDeadlineProbe()
		env := newDispEnv(t, p.handle, false)
		// 防御缺省注入：覆盖 newDispEnv 的零值装配（缺省形态下 dispatcher
		// 落 defaultCallTimeout 常量；本用例钉装配面把 Calls.Default 带进来）。
		env.disp.defaultCallTimeout = 1234 * time.Millisecond
		env.feed(extHeaderFor(702, 0, false), envBody(t, 1))
		rem, ok := p.wait(t)
		assertHandleDeadline(t, rem, ok, 1234*time.Millisecond)
	})
}
