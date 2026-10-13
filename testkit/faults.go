package testkit

// 故障注入面：面向集群集成的注入原语。故障 Store 包装器（FaultStore）经
// WithCheckpointStore 注入全部节点共享——一次性失败/慢 Put 的集群链路等
// 价物（membership 层的 fake 注入归其单元测试）。续租丢键注入面已随 H13 续租
// 拆除退役（所有权丢失场景改由接管通知 + SelfFence 承接）。

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
)

// ErrInjectedFault 注入故障的哨兵错误（Get/Put 失败统一包装，errors.Is
// 可判；底座自身的错误原样透传，不套哨兵）。
var ErrInjectedFault = errors.New("testkit: injected store fault")

// FaultStore 故障注入 Store 包装器（集群集成面）：Get/Put 可注入瞬时
// （前 N 次）或持续（额度 <0 直至 Heal）失败；Put 可注入固定延迟（慢
// 周期 Put——在飞 checkpoint 与交接窗终快照的次序编排抓手）。底座语义
// 原样透传（围栏/防御性拷贝归实现）。并发安全（原子额度 + 统计锁）。
// 阻塞语义随底座与注入（SlowPuts 即延迟源）；额度设定面（FailGets/
// FailPuts/SlowPuts/HealGets/HealPuts/PutStats）不阻塞；测试编排面。
type FaultStore struct {
	checkpoint.Store
	getFails atomic.Int64 // >0: 剩余瞬时失败次数；<0: 持续失败；0: 放行
	putFails atomic.Int64
	putDelay atomic.Int64 // ns；0 = 不延迟

	mu          sync.Mutex
	putAttempts int
	putFailures int
}

// NewFaultStore 包装底座 Store。
func NewFaultStore(base checkpoint.Store) *FaultStore {
	return &FaultStore{Store: base}
}

// consumeFault 原子判定并消耗一次注入额度：>0 瞬时（递减并命中），
// <0 持续命中，=0 放行。CAS 循环吸收并发消耗的竞态。
func consumeFault(c *atomic.Int64) bool {
	for {
		v := c.Load()
		switch {
		case v == 0:
			return false
		case v < 0:
			return true
		default:
			if c.CompareAndSwap(v, v-1) {
				return true
			}
		}
	}
}

// FailGets 注入 Get 失败：n>0 前 n 次失败；n<0 持续失败直至 HealGets。
func (s *FaultStore) FailGets(n int) { s.getFails.Store(int64(n)) }

// HealGets 清除 Get 故障注入。
func (s *FaultStore) HealGets() { s.getFails.Store(0) }

// FailPuts 注入 Put 失败：n>0 前 n 次失败；n<0 持续失败直至 HealPuts。
func (s *FaultStore) FailPuts(n int) { s.putFails.Store(int64(n)) }

// HealPuts 清除 Put 故障注入。
func (s *FaultStore) HealPuts() { s.putFails.Store(0) }

// SlowPuts 注入 Put 固定延迟（d ≤ 0 恢复直通）。延迟期间尊重 ctx 取消。
// 慢周期 Put 是在飞 checkpoint 与交接窗终快照的次序编排抓手（集群集成面
// 的预留工具，按需取用）。
func (s *FaultStore) SlowPuts(d time.Duration) { s.putDelay.Store(int64(d)) }

// PutStats 返回 Put 的 (尝试数, 注入失败数)（底座失败不计入 failures——
// 只统计注入命中，断言注入确实生效）。
func (s *FaultStore) PutStats() (attempts, failures int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putAttempts, s.putFailures
}

// Get 透传 + 注入判定（注入命中不计 Get 统计——Get 的注入消费即可观测）。
func (s *FaultStore) Get(ctx context.Context, id rans.ID) ([]byte, bool, error) {
	if consumeFault(&s.getFails) {
		return nil, false, fmt.Errorf("%w: Get %s", ErrInjectedFault, id.String())
	}
	return s.Store.Get(ctx, id)
}

// Put 透传 + 延迟 + 注入判定（注入命中计入统计）。
func (s *FaultStore) Put(ctx context.Context, id rans.ID, incarnation uint64, snapshot []byte) error {
	s.mu.Lock()
	s.putAttempts++
	s.mu.Unlock()
	if d := s.putDelay.Load(); d > 0 {
		t := time.NewTimer(time.Duration(d))
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	if consumeFault(&s.putFails) {
		s.mu.Lock()
		s.putFailures++
		s.mu.Unlock()
		return fmt.Errorf("%w: Put %s", ErrInjectedFault, id.String())
	}
	return s.Store.Put(ctx, id, incarnation, snapshot)
}
