package lifecycle

// Drainer 四段排空的行为锁（规格 §14.1）。时序断言一律走 channel
// 握手，无裸 sleep 承重断言；并发度上限用「阻塞屏障 + 计数」
// 的确定性观测（信号量在槽位全占时结构性放不进第 N+1 个）。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// ---- 测试基座 ----

// rec 回调序列记录器：四段时序断言的锚点（并发 Destroy 下仅段间次序确定，
// 段内 Destroy 次序不锁定）。
type rec struct {
	mu     sync.Mutex
	events []string
}

func (r *rec) add(ev string) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
}

func (r *rec) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

// nIDs 构造 n 个测试身份（inst 即序号，Destroy 事件的判别成分）。
func nIDs(t *testing.T, n int) []rans.ID {
	t.Helper()
	ids := make([]rans.ID, 0, n)
	for i := 0; i < n; i++ {
		id, err := rans.NewID("cell", fmt.Sprintf("e%d", i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

// drainDone 以有界等待收 Drain 的返回错误（channel 握手，不裸等）。
func drainDone(t *testing.T, d *Drainer, ctx context.Context) error {
	t.Helper()
	ch := make(chan error, 1)
	go func() { ch <- d.Drain(ctx) }()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("drain did not return")
		return nil // 不可达
	}
}

// 1. 四段时序钉死：begin 先于一切 Destroy；全部 Destroy 先于 flush；flush
// 先于 Teardown 序列（Teardown 按登记序）。
func TestDrainFourStageOrdering(t *testing.T) {
	var r rec
	ids := nIDs(t, 3)
	d := &Drainer{
		BeginDraining: func() { r.add("begin") },
		Entities:      func() []rans.ID { return ids },
		Destroy: func(_ context.Context, id rans.ID) error {
			r.add("destroy:" + id.Key)
			return nil
		},
		Flush:       func(context.Context) { r.add("flush") },
		FlushWindow: 50 * time.Millisecond,
		Teardown: []func() error{
			func() error { r.add("teardown:0"); return nil },
			func() error { r.add("teardown:1"); return nil },
			func() error { r.add("teardown:2"); return nil },
		},
	}
	if err := drainDone(t, d, context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}
	ev := r.snapshot()
	if len(ev) != 1+3+1+3 {
		t.Fatalf("want 1 begin + 3 destroy + 1 flush + 3 teardown events, got %v", ev)
	}
	if ev[0] != "begin" {
		t.Fatalf("stage ① (BeginDraining) must precede everything, first event %q", ev[0])
	}
	got := map[string]bool{}
	for _, e := range ev[1:4] {
		if !strings.HasPrefix(e, "destroy:") {
			t.Fatalf("destroy phase contaminated by %q (full %v)", e, ev)
		}
		got[e] = true
	}
	if len(got) != 3 {
		t.Fatalf("each entity destroyed exactly once, got %v", ev[1:4])
	}
	if ev[4] != "flush" {
		t.Fatalf("flush must follow all destroys, event[4]=%q (full %v)", ev[4], ev)
	}
	for i, want := range []string{"teardown:0", "teardown:1", "teardown:2"} {
		if ev[5+i] != want {
			t.Fatalf("teardown order broken at %d: %q (full %v)", i, ev[5+i], ev)
		}
	}
}

// 2. 并行度上限：cap 个 Destroy 阻塞在屏障上（全部槽位占满），此刻并发数
// 恰为 cap——第 cap+1 个被信号量结构性挡住；放行后全部实体收场。
func TestDrainParallelismCap(t *testing.T) {
	const n, cap = 20, 4
	ids := nIDs(t, n)
	entered := make(chan struct{}, n)
	release := make(chan struct{})
	var cur, maxSeen atomic.Int32
	d := &Drainer{
		Entities:    func() []rans.ID { return ids },
		Parallelism: cap,
		Destroy: func(_ context.Context, _ rans.ID) error {
			c := cur.Add(1)
			for {
				m := maxSeen.Load()
				if c <= m || maxSeen.CompareAndSwap(m, c) {
					break
				}
			}
			entered <- struct{}{}
			<-release
			cur.Add(-1)
			return nil
		},
	}
	drained := make(chan error, 1)
	go func() { drained <- d.Drain(context.Background()) }()
	for i := 0; i < cap; i++ {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatalf("destroy %d never started (parallelism gate stuck?)", i)
		}
	}
	// 全部 cap 个 Destroy 都在屏障上且未释放：观测到的并发峰值即真实并发，
	// 无界实现在此刻必 > cap。
	if ms := maxSeen.Load(); ms != cap {
		t.Fatalf("concurrent destroys while all slots blocked = %d, want exactly %d", ms, cap)
	}
	close(release)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("drain did not return after release")
	}
	if ms := maxSeen.Load(); ms > cap {
		t.Fatalf("observed concurrency %d exceeded cap %d", ms, cap)
	}
}

// 3. 零值 Parallelism 回落缺省 max(8, CPU×8)（上限性质与用例 2 同代码
// 路径）：8 个屏障位必须能同时占满（缺省下限 8 的存在性证明——缺省若 <8，
// 第 8 个 Destroy 永远不来，超时失败）。
func TestDrainDefaultParallelismAtLeast8(t *testing.T) {
	const n = 16
	ids := nIDs(t, n)
	entered := make(chan struct{}, n)
	release := make(chan struct{})
	d := &Drainer{
		Entities: func() []rans.ID { return ids },
		Destroy: func(_ context.Context, _ rans.ID) error {
			entered <- struct{}{}
			<-release
			return nil
		},
	}
	drained := make(chan error, 1)
	go func() { drained <- d.Drain(context.Background()) }()
	for i := 0; i < 8; i++ {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d destroys started; default parallelism must admit 8", i)
		}
	}
	close(release)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("drain did not return after release")
	}
}

// 4. 单实体失败不阻断：一个 Destroy 报错，其余全部收场、flush/teardown
// 照常执行，Drain 返回携带该错误的聚合错误。
func TestDrainDestroyFailureCollected(t *testing.T) {
	ids := nIDs(t, 5)
	bad := ids[2]
	sentinel := errors.New("stuck deactivate")
	var r rec
	d := &Drainer{
		Entities: func() []rans.ID { return ids },
		Destroy: func(_ context.Context, id rans.ID) error {
			r.add("destroy:" + id.Key)
			if id == bad {
				return sentinel
			}
			return nil
		},
		Flush:    func(context.Context) { r.add("flush") },
		Teardown: []func() error{func() error { r.add("teardown:0"); return nil }},
	}
	err := drainDone(t, d, context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("drain must surface per-entity destroy error, got %v", err)
	}
	ev := r.snapshot()
	destroys := 0
	sawFlush, sawTeardown := false, false
	for _, e := range ev {
		switch {
		case strings.HasPrefix(e, "destroy:"):
			destroys++
		case e == "flush":
			sawFlush = true
		case e == "teardown:0":
			sawTeardown = true
		}
	}
	if destroys != 5 {
		t.Fatalf("single-entity failure must not block the rest, %d/5 destroys ran", destroys)
	}
	if !sawFlush || !sawTeardown {
		t.Fatalf("flush/teardown must still run after a failed destroy, events %v", ev)
	}
}

// 5. flush 窗口有界：永不返回的 Flush 也只占住窗口——窗口一到编排器弃等
// 继续阶段④；flush 收到的 ctx 带 deadline（窗口即预算）。
func TestDrainFlushWindowBounded(t *testing.T) {
	releaseFlush := make(chan struct{})
	t.Cleanup(func() { close(releaseFlush) }) // 防 flush goroutine 挂满测试进程
	flushStarted := make(chan struct{})
	var startedOnce sync.Once
	sawDeadline := make(chan bool, 1)
	var r rec
	const window = 80 * time.Millisecond
	d := &Drainer{
		FlushWindow: window,
		Flush: func(ctx context.Context) {
			if _, ok := ctx.Deadline(); ok {
				sawDeadline <- true
			} else {
				sawDeadline <- false
			}
			startedOnce.Do(func() { close(flushStarted) })
			<-releaseFlush // 永不主动返回：只有窗口边界能解耦
		},
		Teardown: []func() error{func() error { r.add("teardown:0"); return nil }},
	}
	start := time.Now()
	err := drainDone(t, d, context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("blocked flush must not fail the drain (window bounded), got %v", err)
	}
	if elapsed < window {
		t.Fatalf("drain returned before flush window elapsed: %v < %v", elapsed, window)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("drain overran flush window: %v", elapsed)
	}
	select {
	case ok := <-sawDeadline:
		if !ok {
			t.Fatal("flush ctx must carry a deadline (the window)")
		}
	default:
		t.Fatal("flush never started")
	}
	if len(r.snapshot()) != 1 {
		t.Fatalf("teardown must run after the flush window, events %v", r.snapshot())
	}
}

// 6. 零值防御：全字段零值的 Drainer 可安全排空（各段空转），返回 nil。
func TestDrainZeroValueNoPanic(t *testing.T) {
	d := &Drainer{}
	if err := drainDone(t, d, context.Background()); err != nil {
		t.Fatalf("zero-value drainer: %v", err)
	}
}
