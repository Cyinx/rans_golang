package activation

// 闲置策略登记面（规格 §14.3 E2/G1；闲置候选
// 扫描与钝化/卸载复合体已迁 internal/lifecycle（passivator.go/rebalancer.go），
// 本文件保留策略存储（kind → 闲置阈值+回收模式）及其查询面）。

import (
	"time"
)

// CollectMode 闲置回收模式（IdlePolicy.Mode 的取值正本）。
type CollectMode int

const (
	// CollectPassivate 钝化：快照落 Store + 本地销毁（G1；下次访问重激活
	// 从 Store 恢复）。消费面 = lifecycle.IdleCollector/CollectIdle。
	CollectPassivate CollectMode = iota
	// CollectMigrate 迁移：经 lifecycle.Migrate 迁给放置远端（E2；
	// 候选最冷优先）。消费面 = lifecycle.Rebalancer（表驱动卸载面 offloadIdle）。
	CollectMigrate
)

// IdlePolicy 单 kind 的闲置回收策略（经 Options.IdlePolicies
// 构造期一次给全，运行中不可变——「运行中并发 Set 与扫描属数据竞态」的旧
// 约定随 Set 面一并消失）。
type IdlePolicy struct {
	// IdleAfter 是该 kind 的闲置阈值（≤0 视为零——登记即随时可回收，
	// 调用方自负）。
	IdleAfter time.Duration
	// Mode 决定候选的消费面（CollectPassivate → lifecycle.CollectIdle；
	// CollectMigrate → lifecycle.Rebalancer）。
	Mode CollectMode
}

// IdlePolicyFor 查询 kind 的闲置策略（协作面，lifecycle 闲置扫描调用）：
// 返回阈值与模式；未登记 ok=false（该 kind 永不闲置）。构造期定死，读免锁。
func (m *Table) IdlePolicyFor(kind string) (idleAfter time.Duration, mode CollectMode, ok bool) {
	p, ok := m.idlePolicies[kind]
	if !ok {
		return 0, 0, false
	}
	return p.IdleAfter, p.Mode, true
}
