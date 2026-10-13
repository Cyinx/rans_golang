package lifecycle

// 钝化与周期 checkpoint 的交错：
//   - 捕获槽与钝化的交错：钝化 core 即 loop 任务（自捕获、不经
//     captureCh）——两条快照路径的竞争消失；本测试锁「交错下状态零丢失」：
//     滞留的周期捕获与钝化终快照无论
//     何序收场，Store 必有快照、实体终局逐出。
//   - F2 原形：捕获排队后等待 done 无 ctx 预算——loop 卡死时收集者永久
//     挂起。交接窗形态下等待段绑定调用方预算（Handoff 契约）：预算耗尽
//     即解除调用方等待；窗的终态推进由 loop 自驱（Handle 放行后自然收场）。

import (
	"context"
	"errors"
	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/activation"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// TestCollectIdle_ContendedCaptureStillLandsSnapshot（F1 迁移）：周期
// checkpoint 的捕获滞留 captureCh（loop 被 Handle 占住）时发起钝化——
// 交接窗 core 自捕获落库，滞留捕获或先落库或被终局排空 Skipped：无论
// 何序，Store 必有快照（状态零丢失），实体一轮内终局逐出。
func TestCollectIdle_ContendedCaptureStillLandsSnapshot(t *testing.T) {
	store := checkpoint.New()
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &gateCaptureEntity{} },
	}, activation.Options{Store: store, IdlePolicies: map[string]activation.IdlePolicy{"counter": activation.IdlePolicy{IdleAfter: time.Nanosecond, Mode: activation.CollectPassivate}}})

	id := idleID(t, "counter", "contended-cap")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-collect pawnite")
	entRaw, ok := m.EntityOf(id)
	if !ok {
		t.Fatal("entity not found after activation")
	}
	ent := entRaw.(*gateCaptureEntity)
	ent.release = make(chan struct{})

	// 占住 loop：Call 阻塞在 Handle 内（捕获请求将滞留 captureCh）。
	callDone := make(chan error, 1)
	go func() {
		_, err := m.InvokeLocal(context.Background(), id, nil)
		callDone <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for !ent.inHandle.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !ent.inHandle.Load() {
		t.Fatal("handle not entered within 2s")
	}

	// 周期 checkpoint 起跑：捕获入队成功（滞留通道——loop 被 Handle 占住）。
	if started := CheckpointSweep(context.Background(), m, 10); started != 1 {
		t.Fatalf("checkpoint sweep started = %d, want 1 (capture must queue)", started)
	}

	// 释放 Handle：滞留捕获与钝化 core 在 loop 上按 select 随机序收场——
	// 两种交错都必须把状态带上 Store。
	close(ent.release)
	if err := <-callDone; err != nil {
		t.Fatalf("call: %v", err)
	}

	n, err := NewIdleCollector(m).CollectIdle(context.Background(), 10)
	if err != nil || n != 1 {
		t.Fatalf("CollectIdle = (%d, %v), want (1, nil) — window form resolves the contention in one round", n, err)
	}
	if _, ok := m.GetLocal(id); ok {
		t.Fatal("entity must be evicted after successful passivation")
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("passivated entity must reach terminal state")
	}
	if snap, found, _ := store.Get(context.Background(), id); !found || len(snap) == 0 {
		t.Fatalf("store snapshot = (%v, %v), want non-empty found — state must land despite capture contention", snap, found)
	}
}

// TestPassivate_BoundedWaitWhenLoopStuck（F2 迁移）：loop 卡在长 Handle 中
// 时，Passivate 的等待段受 ctx 预算约束——预算耗尽即返回（调用方不被
// loop 卡死拖住）；窗的终态推进由 loop 自驱，Handle 放行后自然收场
// （Handoff 契约：ctx 耗尽只解除调用方等待，不改 pawnite 状态）。
func TestPassivate_BoundedWaitWhenLoopStuck(t *testing.T) {
	store := checkpoint.New()
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &gateCaptureEntity{} },
	}, activation.Options{Store: store, IdlePolicies: map[string]activation.IdlePolicy{"counter": activation.IdlePolicy{IdleAfter: time.Nanosecond, Mode: activation.CollectPassivate}}})

	id := idleID(t, "counter", "bounded-cap")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-collect pawnite")
	entRaw, ok := m.EntityOf(id)
	if !ok {
		t.Fatal("entity not found after activation")
	}
	ent := entRaw.(*gateCaptureEntity)
	ent.release = make(chan struct{})

	callDone := make(chan error, 1)
	go func() {
		_, err := m.InvokeLocal(context.Background(), id, nil)
		callDone <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for !ent.inHandle.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !ent.inHandle.Load() {
		t.Fatal("handle not entered within 2s")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = Passivate(ctx, m, id)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Passivate = %v, want DeadlineExceeded (bounded by ctx budget)", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Passivate hung for %v, want bounded by ctx", elapsed)
	}
	if p.State() != pawnite.StateDestroying {
		t.Fatalf("entity state = %v, want Destroying (window open, terminal deferred to loop)", p.State())
	}

	// 放行 Handle：窗自然收场（终态推进由 loop 自驱）。
	close(ent.release)
	if err := <-callDone; err != nil {
		t.Fatalf("call: %v", err)
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("passivate window must complete after Handle release")
	}
	if _, ok := m.GetLocal(id); ok {
		t.Fatal("entity must be evicted after the deferred passivation")
	}
}
