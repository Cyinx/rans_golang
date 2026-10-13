// Liveness：节点存活信号（成员键写入）。每周期无条件
// RegisterNode(self, nodeAddr, nodeKinds, draining)：零实体节点照写（空
// 节点可见）、启动即写一次（冷启动窗口消除）、写失败仅 Warn 下轮重试。
// 独立 goroutine 驱动，永不执行实体级工作、用户回调或目录批量 IO——同包
// LeaseKeeper（逐实体续租）侧的任何阻塞都不能冻结节点存活信号。
package membership

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/log"
)

// LivenessConfig 构造 Liveness 的全部输入。Interval ≤0 → 30s（与续租面
// 共用缺省常量）。Reg 必填：无节点注册能力的目录（inmem 等基础面）不构造
// 本组件（node 装配按 directory.NodeRegistry 能力探测接线）。
type LivenessConfig struct {
	// Reg 是节点成员面（必填，nil 报错）。
	Reg directory.NodeRegistry

	// Self 是本节点号（成员键的节点身份）。
	Self rans.NodeID

	// Interval 是成员键刷新周期（≤0 → 30s）。
	Interval time.Duration

	// SelfFence 自围栏阈值（H13 自 LeaseKeeper 迁入）：成员键写持续失败
	// 超过该时长 → LocalIDs 全集关准入 + OnSelfFenceLost 止损销毁（接管
	// 误判的兜底）。0 → max(MemberTTL−2×Interval, Interval)；<0 关闭；
	// 每失败幕恰一次（写恢复复位）。
	SelfFence time.Duration

	// MemberTTL 判活键存活期（SelfFence 缺省推导基准；≤0 → 2×Interval）。
	MemberTTL time.Duration

	// Fence 逐实体同步关准入钩子（非阻塞；nil = 仅计数）。
	Fence func(id rans.ID)

	// LocalIDs 本地激活表身份快照（止损范围；nil = 未接线）。
	LocalIDs func() []rans.ID

	// OnSelfFenceLost 止损销毁钩子（每实体一次，自备预算；nil = 仅关准入）。
	OnSelfFenceLost func(ctx context.Context, id rans.ID) error
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

	// epoch 节点代际：首写成员键前经 Reg.NextNodeEpoch 取号一次（仅存活
	// 信号循环这一条 goroutine 触达，无竞争），此后全程携带——NodeInfo.
	// Epoch 恒为该值。0 = 未取号（Start 前或取号未成功——未取号不写成员
	// 键，取号失败与写失败同纪律：Warn 下轮重试）。
	epoch atomic.Uint64

	// 自围栏半边（SelfFence>0 激活）：lastWriteOk 最近成功写时刻（0 = 首写
	// 前不触发）；fenced 本失败幕已触发标记（写恢复复位）。
	selfFence   time.Duration
	lastWriteOk atomic.Int64
	fenced      atomic.Bool
	fence       func(id rans.ID)
	localIDs    func() []rans.ID
	onSFLost    func(ctx context.Context, id rans.ID) error

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

// defaultHeartbeatInterval 存活刷新缺省周期（续租面拆除后由本文件承载）。
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
	fence, ids, sfl := cfg.Fence, cfg.LocalIDs, cfg.OnSelfFenceLost
	if fence == nil {
		fence = func(rans.ID) {}
	}
	if ids == nil {
		ids = func() []rans.ID { return nil }
	}
	if sfl == nil {
		sfl = func(context.Context, rans.ID) error { return nil }
	}
	return &Liveness{
		reg:       cfg.Reg,
		self:      cfg.Self,
		interval:  interval,
		selfFence: sf,
		fence:     fence,
		localIDs:  ids,
		onSFLost:  sfl,
		stop:      make(chan struct{}),
		kick:      make(chan struct{}, 1),
		done:      make(chan struct{}),
	}, nil
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

// BeginDraining 置位节点排空标记（spec §5.1 第 9 点；运行期状态命令而非
// 配置注入，一次性置位不复位：排空是终态动作，进程随后即停）。置位后节点成员键值携带
// draining（聚合视图过滤——放置/迁移不再选中本节点），**租约续租不受
// 影响**（归同包 LeaseKeeper：排空阶段②的逐实体 Retire 期间续租照常——
// 长排空不过租约 TTL）。置位即 kick 立即补写一轮成员键——标记不等下一
// 周期就进入放置视图（注销前的可见窗口就是它的效用窗口）。排空停序的
// 消费示范见 node.Shutdown / testkit.Cluster.Shutdown（标记 → 排空 →
// flush → 停续租/存活 → 节点注销）。
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

// loop 节点存活信号循环：独立 goroutine（node 经 go Run 驱动），永不执行
// 实体级工作、用户回调或目录批量 IO——实体侧任何阻塞都不能冻结节点存活
// 信号。
func (l *Liveness) loop(ctx context.Context) {
	l.registerSelf(ctx)
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.stop:
			return
		case <-l.kick:
			l.registerSelf(ctx)
		case <-ticker.C:
			l.registerSelf(ctx)
		}
	}
}

// registerSelf 无条件写/续期本节点的成员键（每周期一次 + Run 启动一次 +
// BeginDraining kick 一次）。地址未发布时跳过；代际未取号先取号（取号失
// 败本轮不写——成员键值必携代际，宁可让节点晚一拍可见；同一不可达的目
// 录上写键也必然失败，无额外可见性代价）；写失败仅 Warn（下轮重试）
// ——成员键与租约是两个独立半边。
func (l *Liveness) registerSelf(ctx context.Context) {
	l.mu.Lock()
	addr := l.nodeAddr
	kinds := l.nodeKinds
	draining := l.draining
	l.mu.Unlock()
	if addr == "" {
		return
	}
	if l.epoch.Load() == 0 {
		e, err := l.reg.NextNodeEpoch(ctx)
		if err != nil {
			log.Warn("rans/membership: node epoch allocation failed, retry next tick (node stays invisible until it succeeds)",
				"self", l.self, "err", err)
			l.noteWriteOutcome(false, time.Time{})
			return
		}
		l.epoch.Store(e)
	}
	info := directory.NodeInfo{
		Addr:     addr,
		Kinds:    kinds,
		Draining: draining,
		Epoch:    l.epoch.Load(),
	}
	err := l.reg.RegisterNode(ctx, l.self, info)
	if err != nil {
		log.Warn("rans/membership: node membership key write failed, retry next tick (node may be invisible to placement until it succeeds)",
			"self", l.self, "err", err)
	}
	l.noteWriteOutcome(err == nil, time.Now())
}

// noteWriteOutcome 成员键写结果记账；持续失败超 SelfFence 触发自围栏
// （成功复位失败幕；首次失败起算基准；fence CAS 保证每幕恰一次；止损
// 销毁独立 goroutine——本循环永不等待实体级工作，INV5）。
func (l *Liveness) noteWriteOutcome(ok bool, now time.Time) {
	if l.selfFence <= 0 {
		return
	}
	if ok {
		l.fenced.Store(false)
		l.lastWriteOk.Store(now.UnixNano())
		return
	}
	last := l.lastWriteOk.Load()
	if last == 0 {
		l.lastWriteOk.Store(now.UnixNano()) // 以首失败时刻起算
		return
	}
	if now.Sub(time.Unix(0, last)) < l.selfFence {
		return
	}
	if !l.fenced.CompareAndSwap(false, true) {
		return
	}
	ids := l.localIDs()
	log.Error("rans/membership: self-fence triggered (membership key writes failing past threshold; fencing all local entities)",
		"self", l.self, "entities", len(ids))
	for _, id := range ids {
		l.fence(id)
	}
	go func(ids []rans.ID) {
		for _, id := range ids {
			// 例外②：止损销毁预算（selfFenceDestroyTimeout 集中定义——存活
			// 循环不等待本 goroutine，INV5）。
			ctx, cancel := context.WithTimeout(context.Background(), selfFenceDestroyTimeout)
			_ = l.onSFLost(ctx, id)
			cancel()
		}
	}(ids)
}

// selfFenceDestroyTimeout 自围栏止损销毁单实体预算（实体已 fence，超时
// 放弃等待无正确性影响）。
const selfFenceDestroyTimeout = 30 * time.Second

// NodeEpoch 返回本节点代际（启动取号的单调值，成员键全程携带）：目录条
// 目记录 {node, nodeEpoch, inc} 三元组的代际来源（激活/迁移调用点经本面
// 取值）。0 = 未取号（Start 前或取号未成功）。同节点号重启必得严格更大
// 的代际（{reg} 全局计数器保证）；优雅停机不影响计数器——重启后的新存活
// 期自然取到更大的值。
func (l *Liveness) NodeEpoch() uint64 { return l.epoch.Load() }

// Close 立即终止循环（幂等；不等待退出——在途一轮刷新，含慢目录 IO，仍
// 可能运行片刻；停序要确定性收口，如 Close 后立刻注销节点键，用 Shutdown）。
// Close 先于 Start 调用亦有效：循环首轮 select 即见关闭的 stop 通道。不
// 阻塞；并发安全；返回值恒 nil（本面无错误来源，统一动词签名）。
func (l *Liveness) Close() error {
	l.stopOnce.Do(func() { close(l.stop) })
	return nil
}

// Shutdown 优雅停机：停止并等待循环退出——返回 nil 即在途成员键写已结束
// （node 停序保证先停信号、再注销节点键）。ctx 耗尽返回 ctx.Err()。幂等；
// 阻塞到循环退出或 ctx 耗尽；不得在实体 Handle 内调用。
func (l *Liveness) Shutdown(ctx context.Context) error {
	l.Close()
	if !l.started.Load() {
		return nil // Start 未启动：无可等待者（stop 已关，迟到的 Start 首轮即退）
	}
	select {
	case <-l.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
