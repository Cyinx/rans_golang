package lifecycle

// 再平衡与闲置钝化测试（规格 §14.3 + G1；钝化面为单调用交接窗复合体）：
// 纯回调注入面。
//   - RebalancerService.Scan：阈值 0=关闭、≤阈值不卸载、>阈值按 MaxPerRound
//     卸载（≤0 回落缺省 10）、nil 注入面空转；
//   - Run：ctx 退出 + 超阈值周期触发；
//   - IdleCollector.CollectIdle：逐候选恰一次钝化、失败不计成功数且汇总
//     不阻断、maxCount 截断（快照+销毁次序不变量已并入
//     Passivate 的交接窗复合体，由 activation/lifecycle 侧测试承载）。

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// mustRbID 构造测试身份。
func mustRbID(t *testing.T, kind, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID(kind, inst)
	if err != nil {
		t.Fatalf("rans.NewID: %v", err)
	}
	return id
}

// ---- RebalancerService ----

// rbFake：计数型再平衡注入面。
type rbFake struct {
	mu       sync.Mutex
	count    int
	offloads []int // 每次 OffloadIdle 收到的 maxCount
}

func (f *rbFake) LocalCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count
}

func (f *rbFake) OffloadIdle(_ context.Context, maxCount int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.offloads = append(f.offloads, maxCount)
	return 0, nil
}

func (f *rbFake) offloadCalls() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int, len(f.offloads))
	copy(out, f.offloads)
	return out
}

// TestRebalancerScan_ThresholdGating：阈值门控三态——0=关闭（超载也不卸）、
// count≤阈值不卸、count>阈值卸载；MaxPerRound 原样传递、≤0 回落 10。
func TestRebalancerScan_ThresholdGating(t *testing.T) {
	// 阈值 0=关闭。
	f := &rbFake{count: 100}
	r := &RebalancerService{LocalCount: f.LocalCount, OffloadIdle: f.OffloadIdle}
	r.Scan(context.Background())
	if got := len(f.offloadCalls()); got != 0 {
		t.Fatalf("threshold 0 must disable rebalancing, offloads = %d", got)
	}

	// 低于/等于阈值：不卸载。
	for _, tc := range []struct {
		threshold int
		count     int
		want      bool
	}{
		{threshold: 5, count: 4, want: false}, // 低于
		{threshold: 5, count: 5, want: false}, // 等于（不构成"超阈"）
		{threshold: 5, count: 6, want: true},  // 超过
	} {
		f := &rbFake{count: tc.count}
		r := &RebalancerService{
			LocalCount:  f.LocalCount,
			OffloadIdle: f.OffloadIdle,
			Threshold:   tc.threshold,
			MaxPerRound: 3,
		}
		r.Scan(context.Background())
		got := len(f.offloadCalls()) == 1
		if got != tc.want {
			t.Fatalf("threshold=%d count=%d: offload fired = %v, want %v", tc.threshold, tc.count, got, tc.want)
		}
		if tc.want && f.offloadCalls()[0] != 3 {
			t.Fatalf("MaxPerRound must pass through, got %d", f.offloadCalls()[0])
		}
	}

	// MaxPerRound ≤0 → 回落缺省 10。
	f = &rbFake{count: 100}
	r = &RebalancerService{LocalCount: f.LocalCount, OffloadIdle: f.OffloadIdle, Threshold: 5}
	r.Scan(context.Background())
	if got := f.offloadCalls(); len(got) != 1 || got[0] != DefaultRebalanceMaxPerRound {
		t.Fatalf("default MaxPerRound = %v, want [%d]", got, DefaultRebalanceMaxPerRound)
	}
}

// TestRebalancerScan_NilFacesNoop：nil 注入面空转（防御性，不 panic）。
func TestRebalancerScan_NilFacesNoop(t *testing.T) {
	r := &RebalancerService{Threshold: 1} // LocalCount/OffloadIdle 均 nil
	r.Scan(context.Background())
	r2 := &RebalancerService{OffloadIdle: func(context.Context, int) (int, error) { return 0, nil }, Threshold: 1}
	r2.Scan(context.Background()) // LocalCount nil
}

// TestRebalancerRun_ExitsOnCtxCancel：Run 随 ctx 取消退出（阈值关闭与
// 开启两形态都不泄漏 goroutine）。
func TestRebalancerRun_ExitsOnCtxCancel(t *testing.T) {
	for _, threshold := range []int{0, 5} {
		f := &rbFake{count: 100}
		r := &RebalancerService{
			LocalCount: f.LocalCount,
			OffloadIdle: func(context.Context, int) (int, error) {
				time.Sleep(time.Hour) // 卡死卸载面也不得卡死 Run 的退出
				return 0, nil
			},
			Threshold:   threshold,
			Period:      time.Hour,
			MaxPerRound: 10,
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			r.Run(ctx)
		}()
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("Run(threshold=%d) did not exit on ctx cancel", threshold)
		}
	}
}

// TestRebalancerRun_TickerFiresOffload：Period 到点后超阈即触发卸载。
func TestRebalancerRun_TickerFiresOffload(t *testing.T) {
	f := &rbFake{count: 10}
	r := &RebalancerService{
		LocalCount:  f.LocalCount,
		OffloadIdle: f.OffloadIdle,
		Threshold:   2,
		Period:      10 * time.Millisecond,
		MaxPerRound: 10,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(f.offloadCalls()) >= 1 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("Run must fire OffloadIdle after the period when over threshold")
}

// ---- IdleCollector ----

// icFake：记录型钝化注入面——单一事件日志（IdleCollector
// 只有一个钝化面：快照+销毁已并入 Passivate 的交接窗复合体，编排器
// 只见单调用）+ 注入失败编排。
type icFake struct {
	mu        sync.Mutex
	events    []string // "passivate:<key>"，全局发生序
	passivErr map[string]error
}

func (f *icFake) Passivate(_ context.Context, id rans.ID) error {
	f.mu.Lock()
	err := f.passivErr[id.String()]
	f.events = append(f.events, "passivate:"+id.String())
	f.mu.Unlock()
	return err
}

func (f *icFake) log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.events...)
}

func (f *icFake) countOf(prefix string) int {
	n := 0
	for _, e := range f.log() {
		if len(e) >= len(prefix) && e[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

// TestIdleCollector_PassivatesCandidates 每个候选恰好一次钝化调用；计数=
// 成功数。
func TestIdleCollector_PassivatesCandidates(t *testing.T) {
	a, b := mustRbID(t, "player", "ic-a"), mustRbID(t, "player", "ic-b")
	f := &icFake{}
	c := IdleCollector{
		Candidates: func(int) []rans.ID { return []rans.ID{a, b} },
		Passivate:  f.Passivate,
	}
	n, err := c.CollectIdle(context.Background(), 10)
	if err != nil {
		t.Fatalf("CollectIdle error = %v", err)
	}
	if n != 2 {
		t.Fatalf("passivated = %d, want 2", n)
	}
	if got := f.countOf("passivate:"); got != 2 {
		t.Fatalf("passivate calls = %d, want 2 (one per candidate)", got)
	}
}

// TestIdleCollector_PassivateFailureSkipsCount：钝化失败的候选不计入成功
// 数（manager.Passivate 的交接窗失败 = 实体回退 Active 继续服役），错误
// 汇总上抛；其余候选照常钝化。
func TestIdleCollector_PassivateFailureSkipsCount(t *testing.T) {
	a, b := mustRbID(t, "player", "ic-snapfail-a"), mustRbID(t, "player", "ic-snapfail-b")
	f := &icFake{passivErr: map[string]error{a.String(): errors.New("passivate boom")}}
	c := IdleCollector{
		Candidates: func(int) []rans.ID { return []rans.ID{a, b} },
		Passivate:  f.Passivate,
	}
	n, err := c.CollectIdle(context.Background(), 10)
	if n != 1 {
		t.Fatalf("passivated = %d, want 1 (failed candidate skipped)", n)
	}
	if err == nil {
		t.Fatal("passivate failure must surface an error")
	}
	// 失败不阻断：其余候选照常收到钝化调用（2 个候选都被触达）。
	if f.countOf("passivate:") != 2 {
		t.Fatalf("passivate calls = %d, want 2 (failure must not block the remaining candidate)", f.countOf("passivate:"))
	}
}

// TestIdleCollector_MaxCountTruncation：Candidates 返回超量时 CollectIdle
// 防御性截断到 maxCount。
func TestIdleCollector_MaxCountTruncation(t *testing.T) {
	ids := []rans.ID{
		mustRbID(t, "player", "ic-trunc1"),
		mustRbID(t, "player", "ic-trunc2"),
		mustRbID(t, "player", "ic-trunc3"),
	}
	f := &icFake{}
	c := IdleCollector{
		Candidates: func(int) []rans.ID { return ids }, // 无视 maxCount 的失约实现
		Passivate:  f.Passivate,
	}
	n, err := c.CollectIdle(context.Background(), 2)
	if err != nil {
		t.Fatalf("CollectIdle error = %v", err)
	}
	if n != 2 {
		t.Fatalf("passivated = %d, want 2 (defensive truncation)", n)
	}
	if got := f.countOf("passivate:"); got != 2 {
		t.Fatalf("passivate events = %d, want 2", got)
	}
}

// TestIdleCollector_NilFacesNoop：nil 注入面空转返回 (0, nil)。
func TestIdleCollector_NilFacesNoop(t *testing.T) {
	c := IdleCollector{}
	if n, err := c.CollectIdle(context.Background(), 10); n != 0 || err != nil {
		t.Fatalf("nil faces must no-op, got (%d, %v)", n, err)
	}
	c2 := IdleCollector{Candidates: func(int) []rans.ID {
		return []rans.ID{mustRbID(t, "player", "ic-nilpass")}
	}}
	if n, err := c2.CollectIdle(context.Background(), 10); n != 0 || err != nil {
		t.Fatalf("nil passivate must no-op, got (%d, %v)", n, err)
	}
}
