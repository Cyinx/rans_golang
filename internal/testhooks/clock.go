package testhooks

import (
	"sort"
	"sync"
	"time"
)

// 虚拟时钟（定时器测试注入面）：定时器只在 Advance 时到期，时间推进与被测逻辑
// 解耦——用例不靠 sleep 等时间过去（等待与负向断言仍用有界超时）。
//
// 语义对齐真实时钟（time.Timer）：d <= 0 创建/重置即到期；Reset 丢弃尚未消费
// 的旧触发值；Advance 按 (deadline, 创建序) 触发全部到期项且不阻塞（通道缓冲
// 1、非阻塞投递）——一次推进可连锁触发多个到期定时器的重排。
//
// 本类型经 node.Config.TestHooks 注入装配链（Now/NewTimer 两个函数值直通），
// testkit 以别名导出；定时器实现的白盒测试也直接用它（测试文件导入不吃导入图
// 门禁）。
type VirtualClock struct {
	mu     sync.Mutex
	now    time.Time
	seq    int64
	timers map[int64]*virtualTimer
}

// virtualTimer 一个虚拟定时器：登记在表中即待触发，触发后自表摘除。
type virtualTimer struct {
	deadline time.Time
	seq      int64
	ch       chan time.Time
}

// NewVirtualClock 固定起点（非零值，便于断言时刻差）。
func NewVirtualClock() *VirtualClock {
	return &VirtualClock{now: time.Unix(1700000000, 0), timers: map[int64]*virtualTimer{}}
}

// Now 当前虚拟时刻。
func (c *VirtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimer 起一个 d 后到期的定时器，返回触发通道、停止面与重置面（Stop/Reset
// 语义同 time.Timer；Stop 对已触发项返回 false）。
func (c *VirtualClock) NewTimer(d time.Duration) (<-chan time.Time, func() bool, func(time.Duration) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	t := &virtualTimer{deadline: c.now.Add(d), seq: c.seq, ch: make(chan time.Time, 1)}
	c.timers[t.seq] = t
	if !t.deadline.After(c.now) {
		c.fireLocked(t)
	}
	return t.ch,
		func() bool { return c.stop(t) },
		func(d time.Duration) bool { return c.reset(t, d) }
}

func (c *VirtualClock) stop(t *virtualTimer) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.timers[t.seq]; !ok {
		return false
	}
	delete(c.timers, t.seq)
	return true
}

func (c *VirtualClock) reset(t *virtualTimer, d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-t.ch: // 丢弃尚未消费的旧触发值（对齐 time.Timer.Reset 语义）
	default:
	}
	t.deadline = c.now.Add(d)
	c.timers[t.seq] = t
	if !t.deadline.After(c.now) {
		c.fireLocked(t)
	}
	return true
}

// fireLocked 触发并摘表（调用方持 c.mu；非阻塞投递）。
func (c *VirtualClock) fireLocked(t *virtualTimer) {
	delete(c.timers, t.seq)
	select {
	case t.ch <- c.now:
	default: // 通道已有待消费值：本次投递无信息增量
	}
}

// Advance 推进虚拟时间并触发全部到期定时器（按 (deadline, 创建序)）。
func (c *VirtualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	due := make([]*virtualTimer, 0, len(c.timers))
	for _, t := range c.timers {
		if !t.deadline.After(c.now) {
			due = append(due, t)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].deadline.Equal(due[j].deadline) {
			return due[i].seq < due[j].seq
		}
		return due[i].deadline.Before(due[j].deadline)
	})
	for _, t := range due {
		c.fireLocked(t)
	}
}

// LiveTimers 仍在武装的底层定时器数（终局清场必须归零：销毁后若还留着武装面，
// 每个长延迟定时器都会驻留到原到期点）。
func (c *VirtualClock) LiveTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}
