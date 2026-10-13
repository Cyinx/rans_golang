package testkit

import (
	"time"

	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/node"
)

// Option 集群可选配置（函数选项）。
type Option func(*options)

type options struct {
	callDefault        time.Duration // Calls.Default（缺省 defaultCallTimeout，见 cluster.go）
	callMax            time.Duration // Calls.Max（缺省 node 缺省 10min）
	dir                directory.Directory
	redis              *redisOptions    // 非 nil 时启用 redis 模式（目录+续租由 node 内置接线）
	store              checkpoint.Store // 共享快照存储（nil = 无存储面）
	tls                bool             // TLS 模式（WithTLS；NewCluster 内自签 PKI）
	checkpointInterval time.Duration    // 周期 checkpoint 间隔（WithCheckpointInterval；0 = node 缺省 1min）
	nodePlacements     map[int]node.Placement
	randomPlacement    bool // WithPlacement：node 缺省随机放置（kind 感知候选）
	watchdog           *node.WatchdogConfig
	idleKind           string        // 闲置回收策略 kind（WithIdlePassivation）
	idleAfter          time.Duration // 闲置阈值
	idleLoop           time.Duration // 闲置收集循环周期（随 WithIdlePassivation 启用）
	outboxCapacity     int           // 发件箱容量（WithOutboxCapacity；0 = node 缺省）
}

// redisOptions redis 模式参数（WithRedis）。
type redisOptions struct {
	addr              string
	leaseTTL          time.Duration
	heartbeatInterval time.Duration
}

// WithCallDefault 覆盖全部节点的调用缺省预算（Calls.Default；缺省 3s）：
// 无 deadline 无 rans.Timeout(d) 选项的调用按此收场（短值让超时用例快速
// 收场，避免等默认重试间隔）。
func WithCallDefault(d time.Duration) Option {
	return func(o *options) { o.callDefault = d }
}

// WithCallMax 覆盖全部节点的调用预算上限（Calls.Max；缺省 node 缺省
// 10min）：合成的任何来源超过它即封顶。
func WithCallMax(d time.Duration) Option {
	return func(o *options) { o.callMax = d }
}

// WithDirectory 注入共享目录（缺省 inmem；确定性编排测试可包一层门控目录）。
// 与 WithRedis 互斥（redis 模式自带目录），同时给出即装配失败。
func WithDirectory(d directory.Directory) Option {
	return func(o *options) { o.dir = d }
}

// WithRedis 切换 redis 模式：共享目录换 *redis.Directory（独立 KeyPrefix，
// 集群实例间键隔离、-count=N 重跑间也隔离），经 Directory.Injected 注入全部
// 节点（测试持有——Kill 的「目录不关」语义天然成立，CrashKeepsDir 开关在
// 注入形态下无效果）。续租/变更轮询/地址解析由 node 内置接线（目录实现
// LeaseDirectory/BrowseAll/NodeAddrResolver 即自动启用）；leaseTTL/
// heartbeatInterval ≤0 由各自实现取缺省。redis 不可达时目录命令报错——
// 测试方自带探活门禁 skip。
func WithRedis(addr string, leaseTTL, heartbeatInterval time.Duration) Option {
	return func(o *options) {
		o.redis = &redisOptions{
			addr:              addr,
			leaseTTL:          leaseTTL,
			heartbeatInterval: heartbeatInterval,
		}
	}
}

// WithPlacement 启用放置（node 缺省随机策略：候选 = 目录成员视图声明承载
// 该 kind 的存活节点，排除自身——redis 形态满足成员视图，inmem 无视图即
// 恒本地）。缺省不启用：miss 一律本地激活（确定性编排；需要放置分布断言
// 的场景显式给出）。
func WithPlacement() Option {
	return func(o *options) { o.randomPlacement = true }
}

// WithCheckpointStore 给全部节点注入共享快照存储：迁移源端 CAS 前尽力而为
// 写入、目标端/重激活激活闭包的 Store 兜底读、闲置钝化快照落储都经此面。
// 缺省 nil = 无存储面。跨节点迁移/钝化恢复场景必须传同一实例（Store 是
// 正确性依赖）。注意 node 对已配置 Store 自动起周期 checkpoint 扫描器
// （间隔 = Tuning.CheckpointInterval，缺省 1min——秒级测试内不触发；需要
// 扫描器在测试窗口内落储的场景用 WithCheckpointInterval 调短）。
func WithCheckpointStore(store checkpoint.Store) Option {
	return func(o *options) { o.store = store }
}

// WithCheckpointInterval 调短全部节点的周期 checkpoint 间隔（断言前把在役
// 实体的效果量经扫描器收口落储的场景用；0 = node 缺省 1min）。
func WithCheckpointInterval(d time.Duration) Option {
	return func(o *options) { o.checkpointInterval = d }
}

// WithNodePlacement 覆盖第 i 个节点（0 基）的放置策略：迁移选点/激活 miss
// 选点的确定性编排抓手（如迁移固定目标）。未覆盖的节点用 node 缺省随机。
func WithNodePlacement(i int, p node.Placement) Option {
	return func(o *options) {
		if o.nodePlacements == nil {
			o.nodePlacements = make(map[int]node.Placement)
		}
		o.nodePlacements[i] = p
	}
}

// WithWatchdog 覆盖全局看门狗参数（周期/执行超时；强弃场景用短值）。
// unload/activation 阈值取 node 缺省。
func WithWatchdog(interval, execTimeout time.Duration) Option {
	return func(o *options) {
		o.watchdog = &node.WatchdogConfig{Interval: interval, ExecTimeout: execTimeout}
	}
}

// WithIdlePassivation 给全部节点登记指定 kind 的闲置钝化策略（闲置阈值
// after；快照落 Store 才有意义——搭配 WithCheckpointStore）并把闲置收集
// 循环调到测试节奏（20ms/轮）。
func WithIdlePassivation(kind string, after time.Duration) Option {
	return func(o *options) {
		o.idleKind, o.idleAfter, o.idleLoop = kind, after, 20*time.Millisecond
	}
}

// WithTLS 切换 TLS 模式：NewCluster 时自签一把集群 CA、逐节点签发服务器
// 证书（SAN 127.0.0.1/::1/localhost——节点监听/拨号都在回环）并装配双角色
// tls.Config（监听侧持本节点证书、拨号侧信任集群 CA）。TLS 握手先于帧循环
// 与 Slow 包装完成（tcp 层 startConn 次序）——延迟注入只作用于握手后的
// TLS 记录流。PKI 生成器自含于本包（测试代码不跨包共享）。
func WithTLS() Option {
	return func(o *options) { o.tls = true }
}

// WithOutboxCapacity 调小全部节点的发件箱容量（H11 决策 11；缺省 node 缺省
// 65536 条）：满箱语义（显式 Tell 阻塞 / 非阻塞生产者满即丢）的确定性
// 编排面——小容量使少数几帧即达满箱，阻塞/丢弃行为在测试窗口内可观测。
func WithOutboxCapacity(n int) Option {
	return func(o *options) { o.outboxCapacity = n }
}
