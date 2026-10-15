package rpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/ctxkey"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/routing"
	"github.com/cyinx/rans_golang/internal/transport"
	"github.com/cyinx/rans_golang/internal/transport/tcp"
	"github.com/cyinx/rans_golang/internal/wire"
)

// Config Remote 组合体配置（内部装配面）：组合入口收敛于 node 组装根——
// node.New 是公开装配唯一定义点，Remote 是 node 的传输装配体，本面仅供
// 组装根消费。
//
// 一个节点进程的全部传输与路由声明。
//   - Dir/Manager 为该节点的目录与本地激活编排（New 校验非 nil）；
//   - Resolve 为节点号→地址解析（tcp.Manager 懒建连接的寻址依据）；
//   - Dial/WrapConn 可选：缺省标准拨号/直通（testkit 注入点：分区模拟、
//     连接层故障注入）；
//   - Placement 可选：目录 miss 时的放置决策器（Pick(self) 命中
//     → 对选中节点远发，对端 GetOrActivate 落地）；nil 或无候选 → miss 本地
//     激活（缺省行为）。接口注入，rpc 不依赖 placement 包；
//   - LocationCache 可选（规格 §8.1 ②/§9.2）：身份→位置缓存。
//     nil → 直查目录（零缓存零钩子的缺省形态）；非 nil →
//     Call/SendOneWay 的路由梯子改为「缓存 Get → miss 查目录 → 正/负回填」，
//     并在 New 时自动接线六类失效/更新钩子（本地销毁/CAS 冲突/迁移换家/
//     发送失败/端点判停/节点下线——陷阱 14：缺一即 stale 路由长存）。缓存
//     实例归调用方所有（ChangePoller Unregistered → Invalidate 的接线由
//     拥有 poller 的宿主经 LocationCache() 访问器自行完成，rpc 不拥有
//     poller）；
//   - CheckpointStore 可选（规格 §15）：只驱动本组合体的周期 checkpoint
//     sweeper——快照落库承载由 node 装配根直接经 activation.Options.Store
//     交给表。迁移源端把快照尽力而
//     为写入（durable 记录），目标端激活时作装载 miss 的兜底读。⚠️ nil 的
//     部署形态下，有状态实体（实现 rans.Snapshotter）的迁移状态只活在
//     随迁包 5min TTL 窗口内——窗口外激活（迁移取消/目标重启/包丢失）即
//     状态归零；钝化面同样拒绝无 Store 的有状态钝化。生产部署有状态实体
//     必须配置（at-most-once 以 Store 为正确性依赖）；
//   - PackageTTL 随迁包缓存存活窗（规格 §13.3）：≤0 →
//     DefaultPackageTTL（5min）。包缓存随 New 内置，无需外部注入；
//   - 重试环（规格 §9.2）为缺省常开：远程路径经 routing.Invoke 分类重试
//     （上限 2 次 + 线性退避），本地命中路径绕环（本地错误零重试，仅
//     仅 ErrActivationFailed 在接线层本地重试一次——执行前确定性失败
//     （register-first：注册后失败一律 Unavailable 回绝），重试
//     不破坏恰好一次语义）；
//   - DefaultCallTimeout/MaxCallTimeout 调用预算合成参数（H10 决策 4）：
//     统一调用面逐请求合成 min(调用方 ctx 剩余, rans.Timeout(d) 选项, Max)，
//     无 deadline 无选项取 Default；≤0 由 New 补内部缺省（30s/10min）。
//
// 握手 schema 摘要不设配置面：New 以 codec.SchemaDigest()（生成物 init
// 自动登记的全量形状）注入 tcp；kind 集不参与握手（异构 kind 由 kind
// 感知放置承担）。
// RoutingView 路由面的本地成员视图缝（票 01——node 的 membership.
// LocalView 满足；窄化为 rpc 所需的最小面，测试注替身）。
type RoutingView interface {
	// Alive 严格判定：节点存活且代际一致（epoch==0 视为代际未知不过滤）。
	// 视图不可信（未首刷/超龄）→ 保守存活。
	Alive(node rans.NodeID, epoch uint64) bool
	// Invalidate 拓扑信号（连接丢失/端点判停/重定向提示）→ 触发合并窗内
	// 的一次全量刷新。
	Invalidate()
}

type Config struct {
	NodeID      rans.NodeID
	ClusterName string
	Addr        string
	Secret      []byte
	// NoHMAC 显式不安全 opt-in，透传 tcp.Config.NoHMAC（语义见其文档）：
	// 仅 Secret nil 时有意义——未配密钥且未置本旗标 → 握手 fail-closed
	// 拒连（规格 §10.2）；置位则不安全形态运行，
	// 每次握手完成告警。密钥装载助手：node.LoadHMACKeyFromEnv/File。
	NoHMAC  bool
	Dir     directory.Directory
	Manager *activation.Table
	// StrictSchema 严格摘要门闸（规格 §6.1 H9）：透传
	// tcp.Config.StrictSchema——true 时握手 SchemaDigest 失配拒连
	// （ErrHandshakeSchema）；false（默认）观测模式，失配告警+计数、连接保持。
	StrictSchema bool
	// TLS 可选传输加密（规格 §10.1/陷阱 13）：透传 tcp.Config.TLS
	//（监听侧 tls.NewListener + 服务器证书，拨号侧 tls.Client + 显式握手，
	// 握手先于帧循环与 WrapConn 包装）。同一 *tls.Config 兼任双角色：服务器
	// 角色用 Certificates，客户端角色用 RootCAs/ServerName。nil = 明文。
	TLS *tls.Config
	// Resolve 节点号→地址解析（tcp.Manager 懒建连接的寻址依据；调用方的
	// ctx 贯穿——解析尊重调用预算，返回包装 ctx.Err() 的错误）。必填。
	Resolve func(ctx context.Context, n rans.NodeID) (string, error)
	// DefaultCallTimeout/MaxCallTimeout 调用预算合成参数（语义见 Config
	// 类型注释末条）；≤0 由 New 补内部缺省。
	DefaultCallTimeout time.Duration
	MaxCallTimeout     time.Duration
	// RoutingView 本地成员视图缝（票 01）：路由有效性与地址解析的快照
	// 优先来源；nil = 无视图形态（回落 directory.OwnerAlive/注册表实时读
	//——单机/测试构造）。Invalidate 在端点判停/连接丢失/重定向提示时由
	// 本包回调（拓扑信号触发视图即时刷新）。
	RoutingView RoutingView

	// StrictLocalCodec 本地严格编解码（票 10，node.Config.Calls 同名透传；
	// 语义与成本见其文档——钩子只能落 rpc：activation 不得导入 codec）。
	StrictLocalCodec bool
	// OutboxCapacity 节点发件箱全箱容量（H11 决策 11 + 票 04 按目标分队
	// 列）；≤0 由 New 补缺省，恒装配无开关。
	OutboxCapacity int
	Dial           func(ctx context.Context, addr string) (net.Conn, error)
	WrapConn       func(net.Conn) net.Conn
	Placement      Placement
	LocationCache  *routing.LocationCache
	PackageTTL     time.Duration
	// Metrics 可选指标注册表（规格 §16）：nil = metrics.Default()
	//（meter "rans"）。New 在此注册本节点的仪表（gateway pending、
	// queue/inbox 深度、transport 重拨计数与写批量、握手摘要失配计数）——
	// 构造点集中注入，业务包不散落 metrics 引用。同名仪表在共享注册表上
	// 聚合（观察值求和/计数器共用——多 Remote 单进程形态即 testkit 集群）。
	Metrics *metrics.Registry

	// NodeEpoch 本节点当前代际读取面（停序守卫注销的入参；nil → 停序
	// 跳过注销，成员键交给 TTL 过期）。装配根接线 Liveness.NodeEpoch。
	NodeEpoch func() uint64

	// ShutdownTeardown 停序头回调（构造期注入，无运行期选项面）：Remote.
	// Shutdown 阶段④最先执行，先于目录节点注销与关传输。宿主组件（续租/
	// 存活/轮询）的确定性收口在此登记——阶段②排空与阶段③ flush 期间它们
	// 照常运行（长排空不过租约 TTL），阶段④先确定性收口、再注销节点键
	// ——次序颠倒会被在途续租把注销过的成员键写活（幽灵节点滞留视图直到
	// TTL 过期）。回调自备等待预算（独立小超时——
	// 排空 ctx 可能已被阶段②③耗尽）；返回的错误并入 Shutdown 的结果。
	// 按切片序执行；nil 段跳过。
	ShutdownTeardown []func() error
}

// Remote 远端组合体（规格 §8 节点间消息路径的装配层）：目录/缓存路由
// （缓存 Get → Lookup → 远程 / 本地）+ 连接管理器（TCP 传输）+ 网关
// （pending 关联与发送韧性）+ 分发器（入站投递）+ 分类重试环（规格 §9.2，
// 远程路径每 attempt 重建载荷并重走路由梯子）的单一装配点，上层
// （membership / testkit）只面对本类型。
//
// 本类型同时实现 transport.Handler：入站应用消息移交分发器；连接事件半边
// （握手完成/连接丢失）——握手完成无需动作；连接丢失即快速失败
// 主干（OnConnectionLost 失效该节点全部缓存条目 + FailPendingForNode 摊派
// 取消在途请求，规格 §9.1）。
type Remote struct {
	cfg    Config
	pool   *wire.PagePool
	conns  *tcp.Manager
	gw     *Gateway
	outbox *Outbox
	disp   *Dispatcher
	pkgs   *PackageCache // 随迁包缓存（dispatcher Put / 激活 loader Take 双端共享）

	addr     atomic.Pointer[string] // Start 绑定成功的实际地址
	stopOnce sync.Once

	// runCtx 周期任务族（看门狗/目录清理/checkpoint sweeper）的生命周期：
	// New 创建，Shutdown 与 Close 的停序回调收口（幂等 cancel）。
	runCtx    context.Context
	runCancel context.CancelFunc

	// propValueDropped/propTotalDropped 传播键护栏丢弃计数（H10 决策 8，
	// New 注册）：远端发送面（buildRequest 打包）的丢弃归此处——丢弃不使
	// 调用失败，计数 + Warn 留痕（notePropDrops）。
	propValueDropped *metrics.Counter
	propTotalDropped *metrics.Counter
	// causalCapped 因果路径深度上限降级计数（H12 决策 14，New 注册）：出站
	// 挂点（outboundCausal）超限跳过追加的告警面。
	causalCapped *metrics.Counter
}

// 编译期断言：Remote 即传输端口的连接事件回调实现。
var _ transport.Handler = (*Remote)(nil)

// New 组装 Remote（内部装配面——公开组装入口是 node.New）：单页池贯通
// conns/gw/dispatcher（Get/Put 收支锚点闭合在本组合体），握手摘要 =
// codec.SchemaDigest()（生成物 init 自动登记的全量形状快照——形状维
// 降级为观测/严格门闸，不再决定默认握手成败），
// Dial/WrapConn 注入 tcp.Manager。构造期常驻面仅两处：网关 deadline 扫
// 描器与发件箱分片 worker（各自构造内启动、Close/Shutdown 收口）；Listen
// 与心跳在 Start。
func New(cfg Config) (*Remote, error) {
	if cfg.Dir == nil {
		return nil, errors.New("rans: new: Config.Dir is required")
	}
	if cfg.Manager == nil {
		return nil, errors.New("rans: new: Config.Manager is required")
	}
	if cfg.Resolve == nil {
		return nil, errors.New("rans: new: Config.Resolve is required")
	}
	if cfg.NodeID == 0 {
		// 0 是「匿名源」哨兵：以此号运行的节点发出的请求将因无回程地址
		// 而被对端丢弃（规格 §12），必须在装配期拒绝。
		return nil, errors.New("rans: new: Config.NodeID must not be 0 (anonymous-source sentinel)")
	}
	if cfg.DefaultCallTimeout <= 0 {
		cfg.DefaultCallTimeout = defaultCallTimeout
	}
	if cfg.MaxCallTimeout <= 0 {
		cfg.MaxCallTimeout = defaultMaxCallTimeout
	}
	pool := wire.NewPagePool()
	// 例外①（spec §5.1）：节点根 ctx（进程级生命周期）——周期任务族不挂
	// Start 的调用方 ctx（那只辖监听建立，取消不应静默停摆看门狗），由
	// Shutdown/Close 的停序 cancel 收口。
	runCtx, runCancel := context.WithCancel(context.Background()) // 例外①（见上注）
	// ---- 指标接线面（规格 §16 + 网关/传输扩展）----
	// 构造点集中注册：仪表的观察闭包/计数钩子只引用本组合体构件，
	// 采样由读数方驱动（OTel 收集回调 / metrics 手动 Snapshot），无后台
	// 任务。注册表缺省 Default()（宿主未显式配置时零配置可观测）。
	reg := cfg.Metrics
	if reg == nil {
		reg = metrics.Default()
	}
	schemaMismatch := reg.Counter(metrics.NameHandshakeSchemaMismatch,
		"handshake schema digest mismatches (observation mode)")
	reconnects := reg.Counter(metrics.NameTransportReconnects,
		"re-dials to addresses that previously had a connection")
	r := &Remote{cfg: cfg, pool: pool, runCtx: runCtx, runCancel: runCancel}
	// 摘要观测接线（规格 §6.1 H9：tcp 不 import metrics，计数
	// 闭包在此接）：失配默认连接保持，只告警+计数（StrictSchema 形态的
	// 拒连在 wire.Validate，不计入）。重拨计数同面（构造期
	// OnReconnect 注入）。
	r.conns = tcp.NewManager(tcp.Config{
		NodeID:           cfg.NodeID,
		ClusterName:      cfg.ClusterName,
		Secret:           cfg.Secret,
		NoHMAC:           cfg.NoHMAC,
		SchemaDigest:     codec.SchemaDigest(),
		StrictSchema:     cfg.StrictSchema,
		TLS:              cfg.TLS,
		Dial:             cfg.Dial,
		WrapConn:         cfg.WrapConn,
		OnSchemaMismatch: func() { schemaMismatch.Add(1) },
		OnReconnect:      func() { reconnects.Add(1) },
	}, r, pool)
	// 端点判停回调（构造参数注入）：闭包引用 r.gw——调用点在
	// 发送重试耗尽（运行期），构造完成后 r.gw 恒已就绪。
	r.gw = NewGateway(r.conns, cfg.Resolve, pool, cfg.DefaultCallTimeout,
		func(node rans.NodeID) {
			if cfg.LocationCache != nil {
				cfg.LocationCache.InvalidateNode(node)
			}
			if r.outbox != nil {
				r.outbox.NotifyTargetDown(node) // 票 04：端点判停即开断
			}
			if cfg.RoutingView != nil {
				cfg.RoutingView.Invalidate() // 票 01：端点判停=拓扑信号
			}
			r.gw.FailPendingForNode(node, fmt.Errorf("send retries exhausted for node %d", node))
		})
	reg.Gauge(metrics.NameGatewayPending, "in-flight request-response pairs in gateway",
		func() float64 { return float64(r.gw.PendingCount()) })
	reg.Gauge(metrics.NameQueueDepth, "locally activated pawnite entities",
		func() float64 {
			entities, _ := cfg.Manager.MetricsView()
			return float64(entities)
		})
	reg.Gauge(metrics.NameInboxDepth, "sum of pawnite inbox backlogs",
		func() float64 {
			_, inbox := cfg.Manager.MetricsView()
			return float64(inbox)
		})
	reg.Gauge(metrics.NameTransportBatchSize, "frames written in the most recent outbound flush",
		func() float64 { return float64(r.conns.LastBatchSize()) })
	// 节点发件箱（H11 决策 11）：恒装配——发送面两腿接网关（单向帧
	// SendOneWay / 调用帧 SendOneWayBounded），仪表在构造内注册到同一注
	// 册表（Node.Metrics() 面）。生产者三族接线：显式 Tell（Put 满箱阻塞
	// 至预算）、回包/转发（dispatcher 持箱 TryPut）与 Call 请求帧
	//（PutCall——同目标 FIFO 保序，见 callOnce）；非阻塞单向发送能力面
	//（TryTell，满即丢）经统一调用面能力探测取用。满策略分野见决策 12。
	ob, oerr := NewOutbox(OutboxConfig{
		Capacity: cfg.OutboxCapacity,
		Send:     r.gw.SendOneWay,
		SendCall: r.gw.SendOneWayBounded,
		Metrics:  reg,
	})
	r.gw.SetCancelQueue(ob) // 票 12：MsgCancel 经请求同一目标队列
	if oerr != nil {
		r.Close()
		return nil, fmt.Errorf("rans: new: outbox: %w", oerr)
	}
	r.outbox = ob
	r.pkgs = NewPackageCache(cfg.PackageTTL)
	r.disp = NewDispatcher(cfg.Manager, r.gw, r.cfg.NodeID, pool, r.pkgs, cfg.DefaultCallTimeout, r.outbox, cfg.LocationCache)
	// 出队过期即弃计数（H10 决策 7）：计数器落节点自有注册表
	//（Node.Metrics() 可见），事件钩子经表注入 pawnite 出队检查点。
	expiredCalls := reg.Counter(metrics.NameCallExpired,
		"requests dropped as expired at dequeue (budget exhausted before execution)")
	cfg.Manager.BindExpiredHook(func() { expiredCalls.Add(1) })
	// 出队取消弃置计数（H10 决策 9，与 expired 分列；装配面同族见 expiredCalls）。
	canceledCalls := reg.Counter(metrics.NameCallCanceled,
		"requests dropped as canceled at dequeue (caller canceled while pending)")
	cfg.Manager.BindCanceledHook(func() { canceledCalls.Add(1) })
	r.disp.BindCanceledCounter(canceledCalls) // 票 12：转发前取消收场同面聚合
	if cfg.RoutingView != nil {
		r.disp.BindViewInvalidate(cfg.RoutingView.Invalidate) // 票 01：重定向提示触发视图刷新
	}
	// 传播键护栏丢弃计数（H10 决策 8）：护栏守在过线打包面（buildRequest），
	// 丢弃 = 位置透明的降级而非失败——计数 + Warn 留痕见 notePropDrops。
	r.propValueDropped = reg.Counter(metrics.NamePropValueDropped,
		"propagation keys dropped by per-value cap or encode failure")
	r.propTotalDropped = reg.Counter(metrics.NamePropTotalDropped,
		"propagation keys dropped after per-request total cap hit")
	// 因果路径深度上限降级计数（H12 决策 14）：出站挂点超限跳过追加的告警面。
	r.causalCapped = reg.Counter(metrics.NameCallCausalCapped,
		"outbound causal path appends skipped at depth cap (degraded passthrough)")
	// ---- 迁移接线面（规格 §13.1 源端/§13.3 目标端）----
	// 目标端半边：激活 loader = 随迁包缓存（consume-on-read），miss 由
	// manager 内置的 Store 兜底接管（CheckpointStore 已配置时）——包优先
	// 免 Store 读，Store 是正确性依赖（at-most-once：包丢失不损语义）。
	//
	// 随迁包两端的装配期接线：BindStateLoader/BindPackageSender
	// 是这两槽的唯一写入点——包缓存与网关是本组合体内部构件，装配序晚于
	// 表构造，无法进 Options；Start 前单写一次，之后不可变。目标端恢复
	// 梯子（包缓存优先 + Store 兜底）锚定于此。
	cfg.Manager.BindStateLoader(&cacheStateLoader{cache: r.pkgs})
	// 源端半边：随迁包直发（body = 原始快照字节，不套外封；Header 条件
	// 字段：TargetUUID 寻址 + SourceNodeID 留源；扩展块携带源纪元——源端
	// 落库围栏 incarnation，收端恢复时与 Store 高水位比较、包旧弃包）。
	// 失败仅记录（migCore 尽力而为）；发送成功即快照缓冲所有权移交连接
	//（writeFrame 编码后归还页池），失败时缓冲归 migCore 局部。
	cfg.Manager.BindPackageSender(
		func(ctx context.Context, target rans.NodeID, id rans.ID, snap []byte, srcInc uint64) error {
			hdr := wire.Header{
				Type:         protocol.MsgMigrationPackage,
				Flags:        protocol.FlagHasTargetUUID | protocol.FlagHasSourceNodeID,
				MessageID:    r.gw.NextMessageID(),
				SourceNodeID: uint16(r.cfg.NodeID),
				TargetKind:   id.Kind,
				TargetInst:   id.Key,
			}
			if srcInc != 0 {
				hdr.Flags |= protocol.FlagHasExtensions
				hdr.Ext = wire.EncodeRequestExt(wire.RequestExt{
					HasSrcIncarnation: true,
					SrcIncarnation:    srcInc,
				})
			}
			return r.gw.SendOneWay(ctx, target, hdr, snap)
		})
	// 接管通知发送半边（H13 决策 22）：CAS 接管赢方经发件箱以 MsgOwnershipLost
	// 控制帧直发原归属节点（帧类型在控制面集合内，传输层入控道优先写出；
	// 收端处置见 dispatcher handleOwnershipLost）。best-effort：TryPut 满箱
	// 即丢、关箱拒绝——通知丢失由 SelfFence 兜底（既定语义，不重试）。装配
	// 位与随迁包两端同族：发件箱是本组合体内部构件，Start 前单写一次。
	cfg.Manager.BindTakeoverNotifier(
		func(target rans.NodeID, id rans.ID, prev directory.Owner) {
			hdr := wire.Header{
				Type:         protocol.MsgOwnershipLost,
				Flags:        protocol.FlagHasTargetUUID | protocol.FlagHasSourceNodeID,
				MessageID:    r.gw.NextMessageID(),
				SourceNodeID: uint16(r.cfg.NodeID),
				TargetKind:   id.Kind,
				TargetInst:   id.Key,
			}
			_ = r.outbox.TryPut(target, hdr, wire.EncodeOwnershipLost(
				wire.OwnershipLost{Epoch: prev.Epoch, Incarnation: prev.Incarnation}))
		})
	return r, nil
}

// notePropDrops 传播键护栏丢弃的计数告警（H10 决策 8）：超限丢弃该键，
// 不使调用失败——每键计数 + Warn 留痕（键名 + 护栏类）。
func (r *Remote) notePropDrops(drops []ctxkey.PropDrop) {
	for _, d := range drops {
		if d.TotalCap {
			r.propTotalDropped.Add(1)
		} else {
			r.propValueDropped.Add(1)
		}
		log.Warn("rans/rpc: propagation key dropped", "name", d.Name, "cap", propCapKind(d))
	}
}

// propCapKind 丢弃护栏类的日志标签（value = 单值/编码失败，total = 合计）。
func propCapKind(d ctxkey.PropDrop) string {
	if d.TotalCap {
		return "total"
	}
	return "value"
}
