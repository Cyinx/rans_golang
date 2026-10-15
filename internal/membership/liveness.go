// Liveness：节点存活信号（成员键写入）。每周期 RenewNode 比对续期——
// 键缺席即失主（一死永死：本代际作废、重取号、旧代际实例围栏止损，键不
// 复活）；被更大代际占据即让位（NodeID 重号：停写 + 让位回调）。独立
// goroutine 驱动，永不执行实体级工作、用户回调或目录批量 IO——实体侧
// 任何阻塞都不能冻结节点存活信号。
package membership

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/metrics"
)

// LivenessConfig 构造 Liveness 的全部输入。Interval ≤0 → 30s（存活节拍
// 共用缺省常量）。Reg 必填：无节点注册能力的目录（inmem 等基础面）不构造
// 本组件（node 装配按 directory.NodeRegistry 能力探测接线）。
type LivenessConfig struct {
	// Reg 是节点成员面（必填，nil 报错）。
	Reg directory.NodeRegistry

	// Self 是本节点号（成员键的节点身份）。
	Self rans.NodeID

	// Interval 是成员键刷新周期（≤0 → 30s）。
	Interval time.Duration

	// SelfFence 自围栏阈值：存活节拍（成员键写/续期/代际取号）持续失败
	// 超过该时长 → Fence(0) 围栏全部本地实例（钩子自带异步止损销毁——
	// 接管误判的兜底）。0 → max(MemberTTL−2×Interval, Interval)；<0 关
	// 闭；每失败幕恰一次（写恢复复位）。
	SelfFence time.Duration

	// MemberTTL 判活键存活期（SelfFence 缺省推导基准；≤0 → 2×Interval）。
	MemberTTL time.Duration

	// Fence 实例围栏钩子（非阻塞；nil = no-op）：keepEpoch=0 围栏全部；
	// 否则围栏 owner 已落位且 epoch≠keep 的实例。返回围栏数。SelfFence
	// 触发与换代/让位处置共用本钩子——止损销毁由实现侧按围栏时刻捕获的
	// 实例集异步执行（捕获发生在围栏时刻，杜绝销毁时才读归属的误杀窗）。
	Fence func(keepEpoch uint64) int

	// OnSuperseded 让位回调（Superseded 后恰一次，循环 goroutine 上同步
	// 调用——封表/停进程的编排面；必须非阻塞或自行异步）。
	OnSuperseded func()

	// Metrics 观测注册表（resurrected/superseded 计数落点；nil → Default）。
	Metrics *metrics.Registry

	// ProtoMin/ProtoMax 本端握手协议区间（成员键承载——node.Start 启动
	// 代际检查的数据面；组装根注入 wire.HandshakeVersion*）。
	ProtoMin uint16
	ProtoMax uint16
}

// Liveness 节点存活信号：成员键值 = JSON{addr, kinds, draining, epoch}
// （draining=false、epoch=0 缺省不写——见 directory.NodeRegistry 值格式）。
// 节点代际（epoch）在首写成员键前经 Reg.NextNodeEpoch 取号一次、全程携带
// （同一存活期的每次注册携带同一值；同节点号重启必得严格更大的代际——全
// 局计数器保证，优雅停机不动计数器）。地址与
// 承载 kind 集经 PublishSelf 一次性发布（node.Start 绑定实际地址后、Start
// 起跑前调用——成员键载体是运行期发布的成员视图数据，非构造配置，故不
// 进 LivenessConfig）；排空
// 标记经 BeginDraining 开启（运行期状态命令，一次性置位不复位——排空是
// 终态动作，kick 即时发布）。并发安全；生命周期动词 Start（恰一次）/
// Shutdown（优雅，等在途一轮写完）/ Close（立即、幂等）——约定 spec
// §4.3/§5.5。
type Liveness struct {
	reg      directory.NodeRegistry
	self     rans.NodeID
	interval time.Duration

	mu       sync.Mutex
	nodeAddr string
	// nodeKinds 本节点声明承载的实体 kind 集（判活键值 {addr,kinds}
	// ——kind 感知放置的过滤依据）。nil = 空 kinds（纯心跳节点：只在
	// NodeIDs 全集，不进任何 NodesByKind 视图）。经 PublishSelf 一次性
	// 发布（此后只读），与 nodeAddr 同读写锁。
	nodeKinds []string
	// draining 排空标记（spec §5.1 第 9 点）：随节点
	// 键发布——聚合视图（NodeIDs/NodeAddrs/NodesByKind）据此过滤，放置/
	// 迁移不再选中排空节点。经 BeginDraining 置位（运行期状态命令，一次
	// 性——排空是终态动作），锁内读写、kick 即时补一轮刷新。
	draining bool

	// epoch 节点代际（已发布：新键写成功才推进——新激活永不携带「键缺席
	// 的代际」）；next 待发布代际（取号成功、写键未确认：RegisterNode
	// 服务端成功而客户端报错时下轮重试同值，不得误判 Superseded 也不得
	// 退回旧代际续期）。两者仅存活信号循环触达，无竞争。0 = 无值。
	epoch atomic.Uint64
	next  atomic.Uint64
	// stopped 循环停写标记（Superseded 让位/排空中失主：不再注册——
	// 让位进程的成员键属于占据者）。
	stopped atomic.Bool

	// 自围栏半边（SelfFence>0 激活）：lastWriteOk 最近成功写时刻（0 = 首写
	// 前不触发）；fenced 本失败幕已触发标记（写恢复复位）。
	selfFence    time.Duration
	lastWriteOk  atomic.Int64
	fenced       atomic.Bool
	fence        func(keepEpoch uint64) int
	onSuperseded func()
	metrics      *metrics.Registry
	protoMin     uint16
	protoMax     uint16

	stop     chan struct{}
	stopOnce sync.Once

	// kick 立即刷新信号（容量 1，非阻塞发送——多次合并为一次）：BeginDraining
	// 置位时通知循环立刻补一轮成员键写入，排空标记不等下一周期就进入
	// 放置视图（注销前的可见窗口就是它的效用窗口）。
	kick chan struct{}

	// started 标记 Start 是否已进入、done 由循环退出后关闭——Shutdown
	// 据此提供确定性收口（见其 godoc）。
	started atomic.Bool
	done    chan struct{}
}

// defaultHeartbeatInterval 存活刷新缺省周期（由本文件承载）。
const defaultHeartbeatInterval = 30 * time.Second

// NewLiveness 构造节点存活信号。Reg 必填（nil 报错——无注册能力的目录不
// 应构造本组件，配置性缺失响亮失败优于静默不可见）。
func NewLiveness(cfg LivenessConfig) (*Liveness, error) {
	if cfg.Reg == nil {
		return nil, errors.New("rans: new liveness: LivenessConfig.Reg is required")
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}
	sf := cfg.SelfFence
	if sf == 0 {
		base := cfg.MemberTTL
		if base <= 0 {
			base = 2 * interval
		}
		sf = max(base-2*interval, interval)
	} else if sf < 0 {
		sf = 0
	}
	fence, sup := cfg.Fence, cfg.OnSuperseded
	if fence == nil {
		fence = func(uint64) int { return 0 }
	}
	if sup == nil {
		sup = func() {}
	}
	return &Liveness{
		reg:          cfg.Reg,
		self:         cfg.Self,
		interval:     interval,
		selfFence:    sf,
		fence:        fence,
		onSuperseded: sup,
		metrics:      cfg.Metrics,
		protoMin:     cfg.ProtoMin,
		protoMax:     cfg.ProtoMax,
		stop:         make(chan struct{}),
		kick:         make(chan struct{}, 1),
		done:         make(chan struct{}),
	}, nil
}

// metricsReg 观测注册表（注入优先、Default 兜底）。
func (l *Liveness) metricsReg() *metrics.Registry {
	if l.metrics != nil {
		return l.metrics
	}
	return metrics.Default()
}

// PublishSelf 一次性发布本节点成员键载体（键在=节点活、值=在哪：地址发现即读成员键）。
// addr 是成员键承载的监听地址，kinds 是声明承载的实体 kind 集（kind 感知
// 放置按它过滤候选；nil = 空 kinds——纯心跳节点形态）。标准装配序
// New → node.Start（绑定实际地址）→ PublishSelf(Addr(), kinds) → go Run；
// 未发布（空地址）时跳过节点键写入——空地址入键只会让地址发现拿到必然
// 拨号失败的 ""，宁可让节点暂不可见。切片被复制，调用方后续改动不影响
// 已发布值。
func (l *Liveness) PublishSelf(addr string, kinds []string) {
	l.mu.Lock()
	l.nodeAddr = addr
	l.nodeKinds = slices.Clone(kinds)
	l.mu.Unlock()
}

// BeginDraining 置位节点排空标记（spec §5.1 第 9 点；运行期状态命令，
// 一次性——排空是终态动作，进程随后即停）：成员键值携带 draining，聚合
// 视图过滤（放置/迁移不再选中本节点）。置位即 kick 立即补写一轮——标记
// 不等下一周期就进入放置视图（注销前的可见窗口就是它的效用窗口）。排空
// 中成员键照常续期（长排空不过成员 TTL）。停序消费示范见 node.Shutdown /
// testkit.Cluster.Shutdown。
func (l *Liveness) BeginDraining() {
	l.mu.Lock()
	l.draining = true
	l.mu.Unlock()
	select {
	case l.kick <- struct{}{}:
	default: // 已有待刷新：合并
	}
}

// Start 起跑存活信号循环（返回即可服务；恰一次，二次调用返回错误——单次
// 性，重启请构造新实例）：循环在本方法启动的 goroutine 上运行，每 Interval
// 写一次节点成员键（启动即写一次），kick（BeginDraining 触发）立即补写。
// ctx 是循环生命期（宿主传入节点根 ctx 派生物；Close/Shutdown 提前收口）。
// 不阻塞（返回后首轮写入在后台进行）；并发安全（单次性由 started 守卫）。
func (l *Liveness) Start(ctx context.Context) error {
	if !l.started.CompareAndSwap(false, true) {
		return errors.New("rans: start: liveness already started (single-use; rebuild instead of restarting)")
	}
	go func() {
		defer close(l.done)
		l.loop(ctx)
	}()
	return nil
}

// StartSync 同步首写 + 起循环（票 14 的 Start 次序面）：首写（含必要的
// 同步取号）在本方法内完成——返回即成员键在场、代际已发布，循环此后只
// 续期。首写/取号失败原样上抛（Start 即终态，不启动循环、不发布代际）。
// 与 Start（异步首写形态）互斥——同一 started 守卫，装配只走其一。
func (l *Liveness) StartSync(ctx context.Context) error {
	if !l.started.CompareAndSwap(false, true) {
		return errors.New("rans: start: liveness already started (single-use; rebuild instead of restarting)")
	}
	if err := l.firstWriteSync(ctx); err != nil {
		return err
	}
	go func() {
		defer close(l.done)
		l.loop(ctx)
	}()
	return nil
}

// EnsureEpoch 同步取号（票 14：Start 次序的第一步，绑定之前）：已发布或
// 已取号 → no-op；否则取号入 next（写键成功才发布——次序不变量不变，
// 代际发布仍在首写内）。失败原样返回。
func (l *Liveness) EnsureEpoch(ctx context.Context) error {
	if l.epoch.Load() != 0 || l.next.Load() != 0 {
		return nil
	}
	n, err := l.allocEpoch(ctx, l.epoch.Load())
	if err != nil {
		return err
	}
	l.next.Store(n)
	return nil
}

// firstWriteSync 同步首写一次：已发布（幂等重入）即返回；地址未发布（裸
// 形态误用）返回 nil——循环接管；否则确保已取号 → RegisterNode → 成功
// 才发布代际（发布语义与 publishNext 一致；失败上抛而非下轮重试——
// Start 终态语义）。
func (l *Liveness) firstWriteSync(ctx context.Context) error {
	if l.epoch.Load() != 0 {
		return nil
	}
	l.mu.Lock()
	addr := l.nodeAddr
	kinds := l.nodeKinds
	draining := l.draining
	l.mu.Unlock()
	if addr == "" {
		return errors.New("rans: membership: sync start before PublishSelf (address unknown; bind+publish first)")
	}
	if err := l.EnsureEpoch(ctx); err != nil {
		return fmt.Errorf("rans: membership: epoch allocation: %w", err)
	}
	nx := l.next.Load()
	now := time.Now()
	if err := l.publishKey(ctx, addr, kinds, draining, nx, now); err != nil {
		return fmt.Errorf("rans: membership: first key write: %w", err)
	}
	return nil
}

// loop 节点存活信号循环：独立 goroutine（node 经 go Run 驱动），永不执行
// 实体级工作、用户回调或目录批量 IO——实体侧任何阻塞都不能冻结节点存活
// 信号。
func (l *Liveness) loop(ctx context.Context) {
	l.tick(ctx)
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.stop:
			return
		case <-l.kick:
			l.tick(ctx)
		case <-ticker.C:
			l.tick(ctx)
		}
	}
}

// tick 存活节拍状态机（每周期一次 + Start 启动一次 + BeginDraining kick
// 一次）：未发布代际先取号写键（RegisterNode，成功才发布）；已发布走
// RenewNode 比对续期——Absent 即失主重取号（一死永死），Superseded 即
// 让位停写。地址未发布时跳过（空地址入键只会产出必然拨号失败的解析
// 候选）。写失败 Warn 下轮重试（SelfFence 记账）。
func (l *Liveness) tick(ctx context.Context) {
	if l.stopped.Load() {
		return
	}
	l.mu.Lock()
	addr := l.nodeAddr
	kinds := l.nodeKinds
	draining := l.draining
	l.mu.Unlock()
	if addr == "" {
		return
	}
	now := time.Now()
	if l.epoch.Load() == 0 && l.next.Load() == 0 {
		n, err := l.allocEpoch(ctx, 0)
		if err != nil {
			log.Warn("rans/membership: node epoch allocation failed, retry next tick (node stays invisible until it succeeds)",
				"self", l.self, "err", err)
			l.noteWriteOutcome(false, now)
			return
		}
		l.next.Store(n)
	}
	if l.next.Load() != 0 {
		l.publishNext(ctx, addr, kinds, draining, now)
		return
	}
	l.renew(ctx, addr, kinds, draining, now)
}
