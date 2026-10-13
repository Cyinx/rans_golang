package testkit

// 预算过线与出队过期（H10 决策 5/6/7，缝合点 rans.Client.Call +
// Node.Metrics()）：出站请求帧在扩展块写 budget_ms（发送时刻剩余预算，票 03
// 合成结果），远端 Handle ctx = 基 ctx 派生 + deadline（收到时刻 + budget，
// 对端未带 budget_ms 取本节点 Calls.Default——EX8 改写：不再被 Default 截断）；
// 出队执行前预算已耗尽 → 不执行、按 (CodeTimeout, ReasonExpired) 回绝、
// 计 rans.call.expired。

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// TestHR4_EX8_BudgetPropagates 预算过线（EX8 改写）：调用方 10s 预算 → 远端
// Handle ctx deadline≈10s——收端 Calls.Default 刻意设短于 10s，若远端未按
// 过线 budget_ms 派生（回退 Default 或无 deadline），断言即红；调用方短预算
// → 远端可见同量级短 deadline。
func TestHR4_EX8_BudgetPropagates(t *testing.T) {
	p := &budgetProbe{}
	c := NewCluster(t, 2, budgetProbeKinds(p), WithCallDefault(500*time.Millisecond))
	id := mustID(t, "probe", "ex8-wire")
	// 预热：节点 1 本地激活（testkit 缺省放置 miss → 本地收口），此后节点 0
	// 的同 kind 调用经目录命中节点 1 走远端腿。
	if _, err := c.Node(1).Call(context.Background(), id, nil); err != nil {
		t.Fatal(err)
	}

	t.Run("ten_second_budget_not_truncated_by_default", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := c.Node(0).Call(ctx, id, nil); err != nil {
			t.Fatal(err)
		}
		assertBudgetRemaining(t, p, 10*time.Second)
	})

	t.Run("short_budget_visible_remotely", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if _, err := c.Node(0).Call(ctx, id, nil); err != nil {
			t.Fatal(err)
		}
		assertBudgetRemaining(t, p, 300*time.Millisecond)
	})
}

// expiryProbe 副作用观测实体：首次 Handle 阻塞在 release 上占住 loop（长
// Handle 形态），收场即计数——过期请求若被执行，executed 将超出占位的一次。
type expiryProbe struct {
	started chan struct{} // 首次 Handle 进入即关闭
	release chan struct{} // 关闭后首次 Handle 返回
	mu      sync.Mutex
	count   int
}

func (p *expiryProbe) Handle(_ context.Context, _ any) (any, error) {
	p.mu.Lock()
	p.count++
	first := p.count == 1
	p.mu.Unlock()
	if first {
		close(p.started)
		<-p.release
	}
	return "ok", nil
}

func (p *expiryProbe) executed() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count
}

func (p *expiryProbe) Activate(context.Context) error   { return nil }
func (p *expiryProbe) Deactivate(context.Context) error { return nil }

func expiryProbeKinds(p rans.Entity) map[string]rans.Factory {
	return map[string]rans.Factory{"probe": func(_ rans.ID) rans.Entity { return p }}
}

// TestDelivery_ExpiredOnDequeue 出队已过期即弃（H10 决策 7）：长 Handle 占
// 住 loop 期间短预算请求入箱 → 出队执行前预算已耗尽 → 不执行（实体侧零副
// 作用）、调用方按预算收场 Timeout、rans.call.expired 计数 ≥1（S6 指标面）。
func TestDelivery_ExpiredOnDequeue(t *testing.T) {
	p := &expiryProbe{started: make(chan struct{}), release: make(chan struct{})}
	c := NewCluster(t, 1, expiryProbeKinds(p))
	id := mustID(t, "probe", "expired-dequeue")

	// 占住 loop：长 Handle 阻塞期间第二条请求入箱。
	callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()
	firstErr := make(chan error, 1)
	go func() {
		_, err := c.Node(0).Call(callCtx, id, nil)
		firstErr <- err
	}()
	<-p.started // 长 Handle 已占住 loop

	// 短预算请求入箱：预算 300ms，出队时刻（释放后）必已越过预算边界。
	expCtx, cancelExp := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelExp()
	if _, err := c.Node(0).Call(expCtx, id, nil); !errors.Is(err, rans.ErrTimeout) {
		t.Fatalf("err = %v, want rans.ErrTimeout (caller budget authority)", err)
	}

	// 出队时刻稳过预算边界（deadline ≈ 入箱时刻 + 300ms，释放点再让 200ms）。
	time.Sleep(200 * time.Millisecond)
	close(p.release)
	if err := <-firstErr; err != nil {
		t.Fatalf("occupying call: %v", err)
	}

	// 副作用断言：只有占位的那一次执行，过期请求从未执行。
	if n := p.executed(); n != 1 {
		t.Fatalf("entity executed %d times, want 1 (expired request must not execute)", n)
	}
	// 指标断言（S6）：出队过期即弃计数 ≥1（出队发生在释放后的 loop 上，
	// 轮询等待其落地）。
	deadline := time.Now().Add(2 * time.Second)
	for {
		v := 0.0
		for _, m := range c.Node(0).Node.Metrics() {
			if m.Name == "rans.call.expired" {
				v = m.Value
			}
		}
		if v >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rans.call.expired = %v, want >= 1 on node metrics", v)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
