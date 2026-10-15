package testkit

// 票 11 红测：实例终局（钝化/迁移/销毁）对未 Reply 的延迟应答立即收场——
// 调用方在终局后短时间内收到 CodeTimeout+ReasonDeactivated（修复前只能干等
// 到请求 deadline）。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/node"
)

// deferHoldEntity 聚合应答形态：Responder 存实体状态、刻意不回复。
type deferHoldEntity struct {
	pending *rans.Responder
}

func (a *deferHoldEntity) Activate(context.Context) error   { return nil }
func (a *deferHoldEntity) Deactivate(context.Context) error { return nil }
func (a *deferHoldEntity) Snapshot() ([]byte, error)        { return []byte("s"), nil }
func (a *deferHoldEntity) Restore([]byte) error             { return nil }

func (a *deferHoldEntity) Handle(ctx context.Context, msg any) (any, error) {
	if msg == "wait" {
		a.pending = rans.Defer(ctx)
		return rans.Deferred, nil
	}
	return nil, nil
}

func deferHoldKinds() map[string]rans.Factory {
	return map[string]rans.Factory{
		"deferhold": func(rans.ID) rans.Entity { return &deferHoldEntity{} },
	}
}

// waitDeferred 发起 3s 预算的 wait 调用，返回 (err, 收场耗时)。
func waitDeferred(t *testing.T, call func(ctx context.Context, id rans.ID, msg any) (any, error), id rans.ID) (error, time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	_, err := call(ctx, id, "wait")
	return err, time.Since(start)
}

func assertSettledEarly(t *testing.T, form string, err error, d time.Duration) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: caller not settled", form)
	}
	if rans.CodeOf(err) != rans.CodeTimeout || rans.ReasonOf(err) != rans.ReasonDeactivated {
		t.Fatalf("%s: err = %v (code=%v reason=%v), want Timeout+deactivated", form, err, rans.CodeOf(err), rans.ReasonOf(err))
	}
	if d > 1500*time.Millisecond {
		t.Fatalf("%s: settled after %v, want well before the 3s caller budget", form, d)
	}
}

// TestDefer_PassivationSettlesPendingResponderImmediately 钝化终局：idle 策略
// 200ms 后钝化，调用方随即收到 Timeout+Deactivated（修复前等满 3s 预算）。
func TestDefer_PassivationSettlesPendingResponderImmediately(t *testing.T) {
	n, err := node.New(node.Config{
		NodeID: 1, Standalone: true, Kinds: deferHoldKinds(),
		Store:       checkpoint.New(),
		KindOptions: map[string]node.KindOptions{"deferhold": {Idle: &node.IdlePolicy{After: 200 * time.Millisecond}}},
		Rebalance:   node.RebalanceConfig{Period: 100 * time.Millisecond},
		Calls:       node.CallConfig{Default: 3 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if err := n.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	id, _ := rans.NewID("deferhold", "p1")
	err, d := waitDeferred(t, func(ctx context.Context, id rans.ID, msg any) (any, error) {
		return n.Client().Call(ctx, id, msg)
	}, id)
	assertSettledEarly(t, "passivation", err, d)
}

// TestDefer_DestroySettlesPendingResponder 销毁终局：Destroy 后调用方随即收
// 到 Timeout+Deactivated。
func TestDefer_DestroySettlesPendingResponder(t *testing.T) {
	n, err := node.New(node.Config{
		NodeID: 1, Standalone: true, Kinds: deferHoldKinds(),
		Store: checkpoint.New(),
		Calls: node.CallConfig{Default: 3 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if err := n.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	id, _ := rans.NewID("deferhold", "d1")
	settled := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		start := time.Now()
		_, err := n.Client().Call(ctx, id, "wait")
		if d := time.Since(start); err != nil && d <= 1500*time.Millisecond {
			settled <- err
			return
		}
		settled <- nil // 未在终局附近收场（或成功——异常形态）
	}()
	deadline := time.Now().Add(2 * time.Second)
	for len(n.LocalIDs()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := n.Destroy(context.Background(), id); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	select {
	case err := <-settled:
		if err == nil {
			t.Fatal("destroy: caller not settled early")
		}
		if rans.CodeOf(err) != rans.CodeTimeout || rans.ReasonOf(err) != rans.ReasonDeactivated {
			t.Fatalf("destroy: err = %v (code=%v reason=%v), want Timeout+deactivated", err, rans.CodeOf(err), rans.ReasonOf(err))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("destroy: caller waited full budget (must settle at terminal)")
	}
}

// TestDefer_MigrationSettlesPendingResponder 迁移终局：手动 Migrate 换锁成
// 功后，调用方随即收到 Timeout+Deactivated。
func TestDefer_MigrationSettlesPendingResponder(t *testing.T) {
	migPlacement := &migrateArmedPlacement{target: 2}
	c := NewCluster(t, 2, deferHoldKinds(),
		WithCheckpointStore(checkpoint.New()), WithNodePlacement(0, migPlacement))
	id, _ := rans.NewID("deferhold", "m1")
	if _, err := c.Node(0).Call(context.Background(), id, "ping"); err != nil {
		t.Fatalf("pre-activate: %v", err)
	}
	settled := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := c.Node(0).Call(ctx, id, "wait")
		settled <- err
	}()
	time.Sleep(200 * time.Millisecond) // 等 wait 投递落地（Defer 已武装）
	migPlacement.arm()
	if err := c.Node(0).Migrate(context.Background(), id); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	select {
	case err := <-settled:
		if err == nil {
			t.Fatal("migration: caller not settled")
		}
		if rans.CodeOf(err) != rans.CodeTimeout || rans.ReasonOf(err) != rans.ReasonDeactivated {
			t.Fatalf("migration: err = %v (code=%v reason=%v), want Timeout+deactivated", err, rans.CodeOf(err), rans.ReasonOf(err))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("migration: caller waited full budget (must settle at terminal)")
	}
}
