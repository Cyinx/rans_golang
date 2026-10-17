package pawnite

// 定时器表的白盒判据（公开面观测不到的部分）：索引自洽、出堆顺序、
// 终局后不再武装、容量护栏。语义正本见 rans.AddTimer 的 godoc。

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/testhooks"
)

// TestTimerHeap_OrderAndIndex 随机投递后每个条目的 idx 自洽，且反复取堆顶
// 得到的顺序 = (deadline, id) 升序。
func TestTimerHeap_OrderAndIndex(t *testing.T) {
	h := timerHeap{byID: make(map[int64]*timerEntry)}
	base := time.Now()
	rng := rand.New(rand.NewSource(1))
	const n = 256
	want := make([]*timerEntry, 0, n)
	for i := 0; i < n; i++ {
		e := &timerEntry{id: int64(i + 1), deadline: base.Add(time.Duration(rng.Intn(4*n)) * time.Millisecond)}
		want = append(want, e)
		h.push(e)
	}
	for i, e := range h.entries {
		if e.idx != i || h.byID[e.id] != e {
			t.Fatalf("索引不自洽：堆位 %d 的条目 idx=%d，byID 命中=%v", i, e.idx, h.byID[e.id] == e)
		}
	}
	prev := h.min()
	for h.min() != nil {
		e := h.removeAt(0)
		if prev != nil && e.deadline.Before(prev.deadline) {
			t.Fatalf("出堆顺序违反 deadline 升序：%v < %v", e.deadline, prev.deadline)
		}
		if prev != nil && e.deadline.Equal(prev.deadline) && e.id < prev.id {
			t.Fatalf("同刻出堆顺序违反 id 升序：%d < %d", e.id, prev.id)
		}
		prev = e
	}
}

// TestAddTimer_TerminalReturnsZero 终局后不再武装：返回 0（未武装哨兵）。
func TestAddTimer_TerminalReturnsZero(t *testing.T) {
	p := New(mustUUID(t, "timer", "term"), Options{Activate: func(context.Context, *Pawnite) error {
		return nil
	}})
	p.Start(context.Background())
	waitState(t, p, StateActive)
	if err := p.Destroy(context.Background()); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	<-p.Done()
	if id := p.AddTimer(time.Hour, func(context.Context) {}); id != 0 {
		t.Fatalf("终局后 AddTimer 返回 %d, want 0", id)
	}
}

// TestAddTimer_CapacityPanics 单实例容量是「无界武装」缺陷的护栏：超限 panic。
func TestAddTimer_CapacityPanics(t *testing.T) {
	p := New(mustUUID(t, "timer", "cap"), Options{
		MaxTimers: 1,
		Activate:  func(context.Context, *Pawnite) error { return nil },
	})
	p.Start(context.Background())
	waitState(t, p, StateActive)
	t.Cleanup(func() { p.Destroy(context.Background()) })
	if id := p.AddTimer(time.Hour, func(context.Context) {}); id == 0 {
		t.Fatal("首个定时器应武装成功")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("超容量必须 panic")
		}
	}()
	p.AddTimer(time.Hour, func(context.Context) {})
}

// armDelivery 在 loop 上下文里登记一条定时器（AddTimer 的合法调用面）。
type armDelivery struct {
	id    rans.ID
	delay time.Duration
	fn    func(context.Context)
	armed chan int64
}

func (d *armDelivery) UUID() rans.ID                 { return d.id }
func (d *armDelivery) Deliver(p *Pawnite)            { d.armed <- p.AddTimer(d.delay, d.fn) }
func (d *armDelivery) Reject(rans.Code, rans.Reason) {}

// TestTimers_TerminalDisarmsClockTimer 终局清表：武装面停掉、表项释放（否则
// 每个销毁实例都留一个 runtime 定时器到原到期点，长延迟定时器会积压）。
func TestTimers_TerminalDisarmsClockTimer(t *testing.T) {
	clock := testhooks.NewVirtualClock()
	p := New(mustUUID(t, "timer", "disarm"), Options{
		Now: clock.Now, NewTimer: clock.NewTimer,
		Activate: func(context.Context, *Pawnite) error { return nil },
	})
	p.Start(context.Background())
	waitState(t, p, StateActive)
	armed := make(chan int64, 1)
	p.ReceiveRequest(&armDelivery{id: p.UUID(), delay: time.Hour, fn: func(context.Context) {}, armed: armed})
	<-armed
	deadline := time.Now().Add(2 * time.Second)
	for clock.LiveTimers() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("定时器未武装到底层时钟")
		}
		time.Sleep(time.Millisecond)
	}
	if err := p.Destroy(context.Background()); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	<-p.Done()
	if n := clock.LiveTimers(); n != 0 {
		t.Fatalf("终局后仍有 %d 个底层定时器武装", n)
	}
	p.mu.Lock()
	left := len(p.timers.byID) + len(p.timers.entries)
	p.mu.Unlock()
	if left != 0 {
		t.Fatalf("终局后表未清空：%d 项", left)
	}
}

// TestTimers_SilentWhileHandoffHeldOnWork 交接窗挂在在途 work 上时（窗口打开、
// loop 空闲），到点的定时器不得触发。
func TestTimers_SilentWhileHandoffHeldOnWork(t *testing.T) {
	clock := testhooks.NewVirtualClock()
	fired := make(chan struct{}, 4)
	p := New(mustUUID(t, "timer", "win"), Options{
		Now: clock.Now, NewTimer: clock.NewTimer,
		Activate: func(context.Context, *Pawnite) error { return nil },
	})
	p.Start(context.Background())
	waitState(t, p, StateActive)
	t.Cleanup(func() { p.Destroy(context.Background()) })

	p.mu.Lock()
	seedWorkLocked(p) // 在飞 work：交接窗挂起等待，loop 保持空闲
	p.mu.Unlock()
	armed := make(chan int64, 1)
	p.ReceiveRequest(&armDelivery{id: p.UUID(), delay: 10 * time.Millisecond, armed: armed,
		fn: func(context.Context) { fired <- struct{}{} }})
	<-armed

	done := make(chan error, 1)
	go func() {
		done <- p.Handoff(context.Background(), HandoffRetire, 0, func(context.Context) HandoffOutcome {
			return HandoffOutcome{}
		})
	}()
	deadline := time.Now().Add(2 * time.Second)
	for !p.holdingHandoff() {
		if time.Now().After(deadline) {
			t.Fatal("交接窗未进入等待在途 work 的挂起态")
		}
		time.Sleep(time.Millisecond)
	}
	clock.Advance(10 * time.Millisecond)
	select {
	case <-fired:
		t.Fatal("交接窗内定时器不得触发")
	case <-time.After(50 * time.Millisecond):
	}
	if err := p.Destroy(context.Background()); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	<-p.Done()
	<-done
	select {
	case <-fired:
		t.Fatal("终局后仍触发")
	case <-time.After(20 * time.Millisecond):
	}
}

// TestTimers_OverdueFiresAfterHandoffRollback 交接失败回退 Active 后，窗内到期
// 的定时器立即补触发（窗内不清表的意义）。
func TestTimers_OverdueFiresAfterHandoffRollback(t *testing.T) {
	clock := testhooks.NewVirtualClock()
	fired := make(chan struct{}, 4)
	p := New(mustUUID(t, "timer", "rollback"), Options{
		Now: clock.Now, NewTimer: clock.NewTimer,
		Activate: func(context.Context, *Pawnite) error { return nil },
	})
	p.Start(context.Background())
	waitState(t, p, StateActive)
	t.Cleanup(func() { p.Destroy(context.Background()) })

	armed := make(chan int64, 1)
	p.ReceiveRequest(&armDelivery{id: p.UUID(), delay: 10 * time.Millisecond, armed: armed,
		fn: func(context.Context) { fired <- struct{}{} }})
	<-armed

	coreEntered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- p.Handoff(context.Background(), HandoffRetire, 0, func(context.Context) HandoffOutcome {
			close(coreEntered)
			<-release
			return HandoffOutcome{Err: errors.New("core lost")}
		})
	}()
	<-coreEntered
	clock.Advance(10 * time.Millisecond)
	select {
	case <-fired:
		t.Fatal("交接窗内定时器不得触发")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("core 自报错误时应以失败收场")
	}
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("窗回退 Active 后超期定时器未恢复触发")
	}
}

// TestRunTimerCallback_IsolatePolicyDestroys 回调 panic + Isolate 策略：就地
// 销毁实例（与投递 Handle 的 panic 监管同口径）。
func TestRunTimerCallback_IsolatePolicyDestroys(t *testing.T) {
	p := New(mustUUID(t, "timer", "iso"), Options{
		PanicPolicy: PanicPolicyIsolate,
		Activate:    func(context.Context, *Pawnite) error { return nil },
	})
	p.Start(context.Background())
	waitState(t, p, StateActive)
	armed := make(chan int64, 1)
	p.ReceiveRequest(&armDelivery{id: p.UUID(), delay: 0, armed: armed,
		fn: func(context.Context) { panic("timer boom") }})
	<-armed
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Isolate 策略下回调 panic 应就地销毁实例")
	}
}
