// 每 Pawnite 定时器：一次性定时器表与 loop 侧调度。契约正本在
// rans.AddTimer / rans.RemoveTimer 的 godoc。
//
// 机制：每实例一张带 ID 索引的最小堆（比较键 (deadline, id)），只对堆顶武装
// 一个底层定时器；loop 的 select 直接等它的通道，因此定时器不产生 goroutine、
// 不引入周期唤醒。增删与触发 O(log n)，终局清表 O(1)。
package pawnite

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang/internal/ctxkey"
)

// timerIDs 进程级 ID 分配器：进程内单调、从 1 起、永不复用。跨实例唯一是
// 刻意的——实体状态与快照可能带着 ID，每实例计数器在迁移/重激活后从 1 重来
// 会让陈旧 ID 撞上新定时器。
var timerIDs atomic.Int64

// timerEntry 一个待触发定时器（最小堆元素）。
type timerEntry struct {
	id       int64
	deadline time.Time
	fn       func(context.Context)
	idx      int // 堆内下标（removeByID 的 O(1) 定位依据）
}

// timerHeap 每实例定时器表（调用方持 Pawnite.mu）：byID 是 ID → 条目的索引，
// entries 是二叉最小堆（顺序 = (deadline, id)，id 单调保证全序）。
type timerHeap struct {
	byID    map[int64]*timerEntry
	entries []*timerEntry
}

func (h *timerHeap) min() *timerEntry {
	if len(h.entries) == 0 {
		return nil
	}
	return h.entries[0]
}

func (h *timerHeap) less(i, j int) bool {
	if h.entries[i].deadline.Equal(h.entries[j].deadline) {
		return h.entries[i].id < h.entries[j].id
	}
	return h.entries[i].deadline.Before(h.entries[j].deadline)
}

func (h *timerHeap) swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
	h.entries[i].idx = i
	h.entries[j].idx = j
}

func (h *timerHeap) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.less(i, parent) {
			return
		}
		h.swap(i, parent)
		i = parent
	}
}

func (h *timerHeap) down(i int) {
	n := len(h.entries)
	for {
		left, right, smallest := 2*i+1, 2*i+2, i
		if left < n && h.less(left, smallest) {
			smallest = left
		}
		if right < n && h.less(right, smallest) {
			smallest = right
		}
		if smallest == i {
			return
		}
		h.swap(i, smallest)
		i = smallest
	}
}

func (h *timerHeap) push(e *timerEntry) {
	e.idx = len(h.entries)
	h.entries = append(h.entries, e)
	h.byID[e.id] = e
	h.up(e.idx)
}

// removeAt 摘除下标 i 的条目并返回它（O(log n)：末项换位后双向复核堆序）。
func (h *timerHeap) removeAt(i int) *timerEntry {
	e := h.entries[i]
	last := len(h.entries) - 1
	if i != last {
		h.swap(i, last)
	}
	h.entries[last] = nil
	h.entries = h.entries[:last]
	delete(h.byID, e.id)
	if i != last {
		h.down(i)
		h.up(i)
	}
	e.idx = -1
	return e
}

// removeByID 按 ID 摘除条目（O(log n)）；不存在返回 nil（取消幂等的落点）。
func (h *timerHeap) removeByID(id int64) *timerEntry {
	e := h.byID[id]
	if e == nil {
		return nil
	}
	return h.removeAt(e.idx)
}

// timerArmed 当前武装的底层定时器（mu）：只为堆顶那一项武装，重排走 Reset
// 复用同一底层定时器（新建的代价是它的数倍且带分配）。scheduled=false 表示
// 已停表但对象留着——表空/交接窗之后恢复时仍走 Reset 复用。
type timerArmed struct {
	id        int64
	deadline  time.Time
	c         <-chan time.Time
	stop      func() bool
	reset     func(time.Duration) bool
	scheduled bool
}

// AddTimer 预约一次到点回调（实现 rans.TimerArm；契约正本见 rans.AddTimer）。
func (p *Pawnite) AddTimer(d time.Duration, fn func(context.Context)) int64 {
	if fn == nil {
		panic("rans: AddTimer called with nil callback")
	}
	now := p.opts.Now() // 时钟在锁外取（叶式纯读，不进锁内回调面）
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == StateDestroyed {
		return 0 // 终局后不再武装（并发事实，不是编程错误）
	}
	if p.timers.byID == nil {
		return 0 // 终局清表与 Destroyed 翻转间不可达（见 clearTimersLocked），防御返回
	}
	if len(p.timers.byID) >= p.maxTimers {
		panic("rans: AddTimer exceeds per-instance timer capacity (Options.MaxTimers)")
	}
	id := timerIDs.Add(1)
	p.timers.push(&timerEntry{id: id, deadline: now.Add(d), fn: fn})
	p.timersEver.Store(true)
	return id
}

// RemoveTimer 取消尚未触发的定时器（实现 rans.TimerArm）。武装面无需即时
// 修正：loop 每轮顶部按堆顶与武装项的差异重排，而本方法只在 loop 上被调用
// （调用栈之上必有下一轮 timerWait）。
func (p *Pawnite) RemoveTimer(timerID int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.timers.removeByID(timerID) != nil
}

// timerWait 定时器分支的通道视图（loop 每轮顶部调用）：交接窗内停表返回
// nil（窗回退后自动恢复）；否则按堆顶与当前武装项的差异决定是否重排。返回
// nil 通道 = select 该分支永久禁用。
//
// 锁内调用注入时钟（Now/NewTimer/Reset/Stop）是本文件对「锁内禁回调」纪律的
// 唯一例外：时钟是叶式注入面（不回调运行时、锁序只有 pawnite.mu → 时钟锁），
// 且建表/重排必须与表状态同临界区，挪出锁会引入「按过期读数武装」的窗口。
func (p *Pawnite) timerWait() <-chan time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	top := p.timers.min()
	if p.handoffPending || top == nil {
		p.parkTimerLocked()
		return nil
	}
	if a := p.armed; a != nil && a.scheduled && a.id == top.id && a.deadline.Equal(top.deadline) {
		return a.c
	}
	delay := top.deadline.Sub(p.opts.Now())
	if delay < 0 {
		delay = 0
	}
	if a := p.armed; a != nil {
		a.reset(delay)
		a.id, a.deadline, a.scheduled = top.id, top.deadline, true
		return a.c
	}
	c, stop, reset := p.opts.NewTimer(delay)
	p.armed = &timerArmed{
		id: top.id, deadline: top.deadline, c: c, stop: stop, reset: reset, scheduled: true,
	}
	return c
}

// parkTimerLocked 停表但保留武装对象（表空/交接窗）：恢复时仍走 Reset 复用，
// 不必每次重新 NewTimer（后者带分配，一次性定时器的主用法每触发一次都会走）。
func (p *Pawnite) parkTimerLocked() {
	if a := p.armed; a != nil && a.scheduled {
		a.stop()
		a.scheduled = false
	}
}

// disarmTimerLocked 停表并丢弃武装面（终局清场；空闲/交接窗走 parkTimer 保留
// 复用对象）。
func (p *Pawnite) disarmTimerLocked() {
	if a := p.armed; a != nil {
		a.stop()
		p.armed = nil
	}
}

// fireTimer 定时器分支执行体：摘一个到期项（摘表先于执行）并在 loop 上执行
// 其回调。虚假唤醒（堆顶未到期，例如刚被移除或时钟未推进）直接返回。
func (p *Pawnite) fireTimer() {
	e, ok := p.takeDueTimer()
	if !ok {
		return
	}
	p.runTimerCallback(e)
}

// takeDueTimer 取走最早的一个到期定时器（mu）：交接窗内一律不取。
func (p *Pawnite) takeDueTimer() (*timerEntry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.handoffPending {
		return nil, false
	}
	top := p.timers.min()
	if top == nil || p.opts.Now().Before(top.deadline) {
		return nil, false
	}
	return p.timers.removeAt(0), true
}

// runTimerCallback 定时器回调的 loop 执行段：每次触发一个独立的让出执行面
// （回调内 ReenterAfter 的 work 登记在该面上，交接窗照常等待），回调 ctx 与
// 投递 Handle ctx 同族（身份/纪元/调用面 + 让出点 + 定时器点）。
func (p *Pawnite) runTimerCallback(e *timerEntry) {
	fn := e.fn
	e.fn = nil
	// 非 detached：hookCtx 的强弃取消面穿透到回调让出的 work。
	ctx, _ := p.newLoopArm(p.timerCallbackBase(), false)
	p.runLoopTask("timer callback", func() { fn(ctx) })
}

// timerCallbackBase 定时器回调 ctx 的基座（hookCtx 已含 Self/Client 且随强弃
// 取消；纪元在触发时现读；因果路径遮蔽——触发是新的因果链起源，不继承上游）。
func (p *Pawnite) timerCallbackBase() context.Context {
	ctx := ctxkey.WithoutCausal(p.hookCtx)
	ctx = ctxkey.WithIncarnation(ctx, p.Incarnation())
	return ctxkey.WithDeferPoint(ctx, nil)
}

// realNewTimer 真实时钟的定时器构造（Stop/Reset 语义同 time.Timer）。
func realNewTimer(d time.Duration) (<-chan time.Time, func() bool, func(time.Duration) bool) {
	t := time.NewTimer(d)
	return t.C, t.Stop, t.Reset
}
