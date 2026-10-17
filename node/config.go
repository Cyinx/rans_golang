package node

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/redis"
	"github.com/cyinx/rans_golang/internal/membership"
	"github.com/cyinx/rans_golang/internal/testhooks"
)

// Config 节点装配的全部输入（普通结构体，便于从 env/YAML 映射）。构造时
// 完成全部校验（New），运行中不可变；测试注入走 TestHooks。
//
// 字段语义与内部装配的对应：Directory/Security/Calls/Liveness/Watchdog/
// Rebalance/Placement/Tuning 逐项见各子配置类型注释；逐字段映射表见
// 约定 spec §4.2。
type Config struct {
	// NodeID 本节点号（必填，≠ 0——0 是匿名源哨兵，以此号运行的节点发出的
	// 请求将因无回程地址而被对端丢弃）。
	NodeID rans.NodeID
	// ClusterName 集群名（握手域，全集群一致）；缺省 DefaultClusterName。
	ClusterName string
	// Standalone 单节点无传输形态：不监听、不握手，本地调用直连本地激活
	// 表。与 ListenAddr、Directory.Redis 互斥（New 校验）。Standalone 无
	// HMAC 要求（无传输即无握手可保护）。
	Standalone bool
	// ListenAddr 监听地址；非 Standalone 时缺省 DefaultListenAddr（端口 0
	// 由内核分配，Start 后经 Addr() 读实际地址）。
	ListenAddr string
	// Kinds 业务 kind 工厂表（生成物 Kinds 可直接填入；必填）。
	Kinds map[string]rans.Factory
	// KindOptions per-kind 闲置回收策略（未登记的 kind 永不闲置）；
	// per-kind 看门狗覆盖在 Watchdog.KindOverrides。
	KindOptions map[string]KindOptions
	// Directory 目录来源三分支：Redis 自建（node 持有并负责关闭）＞
	// Injected 注入（调用方持有）＞ 都为零 → inmem（单节点形态）。
	Directory DirectoryConfig
	// Store 快照存储（有状态实体必配——迁移/钝化/停机终快照的正确性依赖）。
	Store checkpoint.Store
	// Security 传输安全（握手 HMAC/TLS）；fail-closed 缺省见 SecurityConfig。
	Security SecurityConfig
	// Calls 调用面参数。
	Calls CallConfig
	// Outbox 出站发件箱参数（H11 决策 11，恒装配无开关）。
	Outbox OutboxConfig
	// Liveness 存活节拍参数（redis 等成员面目录形态生效——目录实现
	// NodeRegistry 才装配 Liveness；inmem 无成员面自动跳过）。
	Liveness LivenessConfig
	// Watchdog 看门狗全局阈值与扫描周期；per-kind 覆盖在 KindOverrides。
	Watchdog WatchdogConfig
	// Rebalance 自动再平衡参数（Threshold 0 = 关闭）；依赖 KindOptions 的
	// Migrate 闲置策略提供候选。
	Rebalance RebalanceConfig
	// Placement 放置策略；nil → 在候选节点中随机（RandomPlacement 语义）。
	Placement Placement
	// Tuning 周期任务与缓存微调。
	Tuning TuningConfig
	// ShutdownBudget Run 的停机预算（Shutdown 的 ctx 窗口上限）；≤0 取
	// DefaultShutdownBudget。直接调 Shutdown 时预算由调用方 ctx 承载。
	ShutdownBudget time.Duration
	// FencingFloor 宿主纪元下限钩子（A2 运行时围栏）：返回该 id 已被
	// 宿主自持存储接受的最大纪元（0 = 无）；每次激活前调用一次，阻塞受
	// 激活预算约束；报错 → 激活失败（可重试）。nil = 无钩子。目录键丢失
	// 后激活取号仍高于持久层高水位，复活实例的写不被 Store 围栏静默拒绝。
	FencingFloor func(ctx context.Context, id rans.ID) (uint64, error)
	// TestHooks 测试注入点（testkit 与进程内多节点测试专用；类型在
	// internal/testhooks——模块外无法构造。字段语义见其文档；node.New
	// 构造期一次性接线，运行中不可变）。
	TestHooks *testhooks.Hooks
}

// DirectoryConfig 目录来源（优先级与缺省见 Config.Directory）。
type DirectoryConfig struct {
	// Redis 自建 redis 目录的选项（node 持有：Shutdown 关闭、装配失败随
	// 收；KeyPrefix 空时补 DefaultRedisKeyPrefix）。与 Injected 互斥。
	Redis *redis.Options
	// Injected 注入目录（调用方持有，node 不负责关闭；进程内多节点测试
	// 共享目录即此形态）。与 Redis 互斥。
	Injected directory.Directory
}

// SecurityConfig 传输安全。fail-closed 缺省：非 Standalone 且 HMACKey 空且
// 未显式 Insecure → New 报错——未配密钥的明文集群必须是一次显式决定
// （决策出处：docs/decisions/2026-09-30-phase6-security.md）。
type SecurityConfig struct {
	// HMACKey 握手共享密钥；密钥装载助手见 LoadHMACKeyFromEnv/LoadHMACKeyFromFile。
	HMACKey []byte
	// Insecure 显式放弃 HMAC 的 opt-in（仅 HMACKey 为空时有意义）——不安全
	// 形态运行，每次握手完成告警。
	Insecure bool
	// StrictSchema 严格摘要门闸（规格 §6.1 H9「指纹降级为观测」）：true
	// 时握手 SchemaDigest 与对端不等即拒连（ErrHandshakeSchema）；false
	//（默认）观测模式——失配只告警+计数（rans.handshake.schema_mismatch），
	// 连接保持。摘要 = 全量已登记消息形状（生成物 init 自动登记），kind 集
	// 不参与握手（异构 kind 由 kind 感知放置承担）；滚动升级窗口内新旧二进
	// 制形状集不同属预期，观测模式不影响互通，严格模式由部署方按需开启。
	StrictSchema bool
	// TLS 可选传输加密：同一 *tls.Config 兼任监听侧（Certificates）与拨号
	// 侧（RootCAs/ServerName）双角色；nil = 明文。
	TLS *tls.Config
}

// CallConfig 调用面参数（逐请求预算合成的配置半边；合成规则见
// rans.Timeout——本次预算 = min(调用方 ctx 剩余, Timeout(d) 选项, Max)）。
// 封顶对全部来源生效：Max < Default 构造期报错（validate）——缺省预算
// 不允许越过上限，杜绝「无 deadline 调用反而拿超大预算」的静默破口。
type CallConfig struct {
	// Default 单次调用的缺省预算：调用方 ctx 无 deadline 且未传
	// rans.Timeout(d) 选项时取本值；≤0 → DefaultCallTimeout。受 Max 封顶
	//（Max < Default 构造期报错）。
	Default time.Duration
	// Max 单次调用预算上限：合成的任何来源超过它即封顶；≤0 →
	// DefaultMaxCallTimeout。
	Max time.Duration
	// StrictLocalCodec 本地严格编解码模式（票 10，缺省 false）：开启后本地
	// Call/Tell 对请求与应答（含延迟应答）各做一次 codec 编解码往返——
	// 未注册类型本地即失败（与远端同构错误）、引用别名被切断，部署拓扑
	// 变化（本地→远端）不再暴露行为差异。推荐测试/预发开启。成本：每跳
	// 两次编解码 + 一次内存拷贝。
	StrictLocalCodec bool
}

// OutboxConfig 出站发件箱参数（H11 决策 11）：单向发送入箱即返回、
// worker 按目标节点哈希分片承同目标 FIFO。恒装配无开关（零值回落缺省）。
type OutboxConfig struct {
	// Capacity 全箱共享容量（在箱条目上限，跨分片合计）；≤0 →
	// DefaultOutboxCapacity。满箱语义按生产者分野（决策 12）：显式 Tell
	// 阻塞至调用方 ctx 到期；事件扇出/回包/转发等非阻塞生产者满即丢计数。
	Capacity int
}

// LivenessConfig 存活节拍参数（成员面形态生效——目录实现 NodeRegistry
// 才装配 Liveness；逐字段语义见各字段注释）。票 23 更名自 LeaseConfig
// （逐实体租约 v0.23 已删——「租约」是化石名）。
type LivenessConfig struct {
	// Interval 成员键刷新周期（存活节拍——Liveness 每周期无条件续写本
	// 节点成员键，零实体照写）；≤0 → 30s（membership 缺省）。
	Interval time.Duration
	// SelfFence 本节点成员键写/代际取号连续失败的自检阈值（超时即本地
	// 全集关准入 + 止损销毁——接管误判的兜底）；0 → 按 MemberTTL 推导
	//（max(MemberTTL−2×Interval, Interval)），<0 关闭。
	SelfFence time.Duration
}

// WatchdogParams 看门狗阈值（per-kind 覆盖粒度）。零值字段回落全局/缺省。
type WatchdogParams struct {
	UnloadTimeout     time.Duration
	ExecTimeout       time.Duration
	ActivationTimeout time.Duration
}

// WatchdogConfig 看门狗全局参数（规格 §14.2 三形态）：无关闭开关——看门
// 狗是规格规定的纠正面，空闲节点零成本（扫描空表）。
type WatchdogConfig struct {
	Interval          time.Duration
	UnloadTimeout     time.Duration
	ExecTimeout       time.Duration
	ActivationTimeout time.Duration
	// KindOverrides per-kind 阈值覆盖（LLM 类慢实体按轮长放宽等）。
	KindOverrides map[string]WatchdogParams
}

// RebalanceConfig 自动再平衡参数（规格 §14.3 近似档）：本地实体数超阈 →
// 把 Migrate 闲置候选迁给放置远端。
type RebalanceConfig struct {
	// Threshold 本地实体数阈值：严格超过才卸载；0 = 关闭。
	Threshold int
	// Period 扫描周期；≤0 → 60s。
	Period time.Duration
	// MaxPerRound 单轮卸载上限；≤0 → 10。
	MaxPerRound int
}

// TuningConfig 周期任务与缓存微调。
type TuningConfig struct {
	// CheckpointInterval 周期 checkpoint 间隔（Store 已配置才接线）；
	// ≤0 → 1min。
	CheckpointInterval time.Duration
	// PackageTTL 随迁包缓存存活窗；≤0 → 5min。
	PackageTTL time.Duration
	// MembershipRefresh 本地成员视图周期刷新间隔（票 01）；≤0 → 1s；
	// 下限 100ms（小于该值构造期报错——低于此等于没有视图，退回每跳读
	// 注册表还多一层间接）。
	MembershipRefresh time.Duration
	// MaxTimers 单实例待触发定时器上限（≤0 → 65536，约 9 MB 上界）。它是
	// 「无界武装」缺陷的护栏而非容量规划面：万级/十万级用法按节点显式抬高。
	MaxTimers int
}

// KindOptions per-kind 设置（约定 spec §5.4 第 3 条）。per-kind 看门狗阈值
// 覆盖在 Watchdog.KindOverrides（本结构只承载闲置回收）。
type KindOptions struct {
	// Idle 闲置回收策略；nil = 永不闲置。配置后自动执行（Passivate 周期
	// 钝化 / Migrate 随 Rebalance 卸载），无手动入口。
	Idle *IdlePolicy
}

// IdlePolicy 单 kind 的闲置回收策略。
type IdlePolicy struct {
	// After 闲置阈值（最后一次投递起算）；≤0 视为零——登记即随时可回收。
	After time.Duration
	// Migrate 选择回收方式：true = 迁给放置远端（需 Rebalance.Threshold>0
	// 驱动，且实体可迁移）；false = 本地钝化（有 Store 才有意义——快照落
	// 储后销毁，下次访问重激活恢复）。
	Migrate bool
}

// kind 键字符集门闸经 rans.ValidKindName 复用正本判据（原本地复制已收口）。

// validate 构造期全量校验（New 单一入口，失败返回错误不留半开组合体）。
// 就地补默认值（Config 值语义——调用方副本不受影响）。
func (c *Config) validate() error {
	if c.NodeID == 0 {
		return errors.New("rans: new: NodeID is required (0 is the anonymous-source sentinel)")
	}
	if c.ClusterName == "" {
		c.ClusterName = DefaultClusterName
	}
	if c.Calls.Default <= 0 {
		c.Calls.Default = DefaultCallTimeout
	}
	if c.Calls.Max <= 0 {
		c.Calls.Max = DefaultMaxCallTimeout
	}
	if c.Calls.Max < c.Calls.Default {
		return fmt.Errorf("rans: new: Calls.Max (%s) must not be below Calls.Default (%s) (Max caps every budget source, the default included)", c.Calls.Max, c.Calls.Default)
	}
	if c.Outbox.Capacity <= 0 {
		c.Outbox.Capacity = DefaultOutboxCapacity
	}
	if c.Directory.Redis != nil && c.Directory.Redis.TombstoneTTL > 0 {
		// 墓碑 TTL 的纪元下限前提（票 17）：墓碑过期后重激活的纪元必须
		// 由下限来源兜住（Store 高水位或宿主钩子），否则回退到 A2 修复
		// 前的纪元回退面。
		hasHW := false
		if c.Store != nil {
			_, hasHW = c.Store.(checkpoint.HighWaterReader)
		}
		if !hasHW && c.FencingFloor == nil {
			return fmt.Errorf("rans: new: Directory.Redis.TombstoneTTL requires an epoch-floor source (Store implementing checkpoint.HighWaterReader, or FencingFloor) — without one, tombstone expiry regresses incarnation monotonicity")
		}
	}
	if c.Tuning.MembershipRefresh > 0 && c.Tuning.MembershipRefresh < membership.MinRefreshInterval {
		return fmt.Errorf("rans: new: Tuning.MembershipRefresh (%s) below the %s floor (a shorter interval is no view at all — every hop would still read the registry)", c.Tuning.MembershipRefresh, membership.MinRefreshInterval)
	}
	if len(c.Kinds) == 0 {
		return errors.New("rans: new: Kinds are required (Config.Kinds)")
	}
	for name, factory := range c.Kinds {
		if !rans.ValidKindName(name) {
			return fmt.Errorf("rans: new: Kinds[%q] is not a valid kind name (charset: ^[A-Za-z_][A-Za-z0-9_-]{0,63}$)", name)
		}
		if factory == nil {
			return fmt.Errorf("rans: new: Kinds[%q] factory is nil", name)
		}
	}
	if c.Directory.Redis != nil && c.Directory.Injected != nil {
		return errors.New("rans: new: Directory.Redis and Directory.Injected are mutually exclusive")
	}
	for kind := range c.KindOptions {
		if _, ok := c.Kinds[kind]; !ok {
			return fmt.Errorf("rans: new: KindOptions[%q] is not a registered kind (Config.Kinds)", kind)
		}
	}
	for kind := range c.Watchdog.KindOverrides {
		if _, ok := c.Kinds[kind]; !ok {
			return fmt.Errorf("rans: new: Watchdog.KindOverrides[%q] is not a registered kind (Config.Kinds)", kind)
		}
	}
	if c.Standalone {
		if c.ListenAddr != "" {
			return errors.New("rans: new: Standalone must not set ListenAddr (no transport)")
		}
		if c.Directory.Redis != nil {
			return errors.New("rans: new: Standalone must not build a redis directory (single node, no transport)")
		}
		return nil
	}
	if c.Security.HMACKey == nil && !c.Security.Insecure {
		return errors.New("rans: new: Security.HMACKey is required for networked nodes (set Insecure to opt out explicitly)")
	}
	return nil
}

// HMACKeyEnv 握手 HMAC 密钥的环境变量名（值为 hex 文本；传输侧门闸错误
// 文本引用同名常量）。
const HMACKeyEnv = "RANS_HMAC_KEY"

// LoadHMACKeyFromEnv 从环境变量装载握手密钥（hex 文本）。未设/空白 →
// (nil, nil)（未配置——调用方据此把 Security.HMACKey 留空，装配门闸
// fail-closed 拒绝构造：未配密钥且未显式 Insecure 的节点 New 即报错，
// 空钥绝不静默放行）；非 hex → error 点名环境变量（配置事故当场暴露，
// 不静默降级）。两通道（env/文件）同一 hex 约定，可互换。
// 阻塞于进程环境读取（微秒级）；并发安全（纯读环境）；构造期调用（非
// Handle 路径）。
func LoadHMACKeyFromEnv() ([]byte, error) {
	return loadHMACKeyHex("env "+HMACKeyEnv, os.Getenv(HMACKeyEnv))
}

// LoadHMACKeyFromFile 从文件装载握手密钥：内容为 hex 文本（与 env 同一
// 约定），trim 首尾空白（容忍编辑器换行收尾）。文件缺失/读取失败 →
// error（绝不空钥放行）；空白内容 → (nil, nil)（同上——末端由装配门闸
// fail-closed 收口）。密钥文件不入库（决策出处见 SecurityConfig）。
// 阻塞于文件读取（微秒级小文件）；并发安全（纯读）；构造期调用（非
// Handle 路径）。
func LoadHMACKeyFromFile(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("rans: load HMAC key: read file %s: %w", path, err)
	}
	return loadHMACKeyHex("file "+path, string(b))
}

// loadHMACKeyHex hex 文本 → 密钥字节；空白 → 未配置 (nil, nil)。
func loadHMACKeyHex(src, s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	key, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("rans: load HMAC key: key from %s is not valid hex: %w", src, err)
	}
	return key, nil
}
