package node

import "time"

// 缺省值常量集中地（约定 spec §5.4 第 1 条：缺省值常量集中在各包的
// defaults.go，README 参数表引用）。
const (
	// DefaultClusterName 集群名缺省（握手域）。
	DefaultClusterName = "rans"
	// DefaultListenAddr 非 Standalone 形态的缺省监听地址（内核分配端口）。
	DefaultListenAddr = "127.0.0.1:0"
	// DefaultShutdownBudget Run 的停机预算缺省（Shutdown 的 ctx 窗口：
	// 心跳等待 + 排空四段的合计上限）。
	DefaultShutdownBudget = 15 * time.Second
	// DefaultRedisKeyPrefix 自建 redis 目录的生产缺省键前缀。
	DefaultRedisKeyPrefix = "rans:"
	// DefaultCallTimeout 单次调用预算缺省（Calls.Default ≤0 时的回落值）：
	// 调用方 ctx 无 deadline 且未传 rans.Timeout(d) 选项时的预算。
	DefaultCallTimeout = 30 * time.Second
	// DefaultMaxCallTimeout 单次调用预算上限缺省（Calls.Max ≤0 时的回落
	// 值）：合成的任何来源超过它即封顶，调用方不能延长超上限的预算。
	DefaultMaxCallTimeout = 10 * time.Minute
	// DefaultRebalancePeriod 闲置回收/再平衡循环周期缺省（Rebalance.Period
	// ≤0 时的回落值）。
	DefaultRebalancePeriod = 60 * time.Second
	// DefaultMaxPerRound 单轮回收上限缺省（Rebalance.MaxPerRound ≤0 时的
	// 回落值）。
	DefaultMaxPerRound = 10
	// DefaultOutboxCapacity 节点发件箱全箱共享容量缺省（Outbox.Capacity
	// ≤0 时的回落值，条）。
	DefaultOutboxCapacity = 65536
	// DefaultOutboxShards 节点发件箱按目标节点哈希的分片数缺省
	//（Outbox.Shards ≤0 时的回落值）。
	DefaultOutboxShards = 16
)
