package rpc

import (
	"context"
	"errors"
	"github.com/cyinx/rans_golang/internal/metrics"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/ctxkey"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/routing"
	"github.com/cyinx/rans_golang/internal/wire"
)

// maxActivationAttempts GetOrActivate 重试环上限（瞬态激活失败重试；
// 仲裁落败不占重试名额——直接按输家语义转发给赢家）。
const maxActivationAttempts = 3

// cancelKey 取消定位键（H10 决策 9）：（源节点，MessageID）——MessageID 跨源可撞号。
type cancelKey struct {
	src uint16
	id  uint64
}

// Dispatcher 入站消息分发器（规格 §10.3 RemoteMessageDispatcher 的消息
// 半边；连接事件半边由 Remote 组合实现 transport.Handler）：
// 按消息类型路由——Response/Error 进网关 pending 表，Request/OneWay 解析
// 目标身份后经 manager 激活并以 remoteDelivery 入箱（解码推迟到投递时刻，
// 陷阱 5），MsgCancel 按（源节点，MessageID）定位在途/在箱请求取消之
// （H10 决策 9），MigrationPackage 落随迁包缓存（规格 §13.3 目标端）。
//
// 自有状态仅排空门闸（规格 §14.1 ①：排空
// 时本地表命中照常准入、miss 才回绝）：draining 原子置位后 Request/OneWay
// 的准入以 mgr.GetLocal 为闸；其余并发安全由依赖（manager/gateway/pool/
// pkgcache）与逐消息独立投递对象保证，可被多条连接的读循环并发调用。
type Dispatcher struct {
	mgr      *activation.Table
	gw       *Gateway
	self     rans.NodeID
	pool     *wire.PagePool
	pkgs     *PackageCache // MigrationPackage 落点；nil → 包丢弃留痕（防御）
	draining atomic.Bool   // 排空门闸（规格 §14.1 ①；BeginDraining 置位）
	// viewInvalidate 重定向提示触发的视图失效钩子（票 01）。
	viewInvalidate func()

	// canceledFrames 转发前取消收场计数（票 12；BindCanceledCounter 装
	// 配期接线，nil = 不计——独立测试构造形态）。
	canceledFrames *metrics.Counter

	// outbox 节点发件箱（NewDispatcher 装配期注入，恒非 nil）：回包/转发
	// 出站的非阻塞入箱面（H11 决策 12/13——满即丢计数，发送归分片 worker
	// 异步完成，读循环与实体 loop 零阻塞 IO）。
	outbox *Outbox

	// cache 位置缓存（重定向提示的落盘面，H13 决策 24；nil = 未接线，
	// 提示丢弃——加速面丢失由正 TTL 兜底）。
	cache *routing.LocationCache

	// defaultCallTimeout 入站请求的防御缺省预算（H10 决策 6）：对端未带
	// budget_ms 时 Handle ctx 以「收到时刻 + 本值」派生 deadline（装配面 =
	// Calls.Default；≤0 落 defaultCallTimeout 常量兜底）。
	defaultCallTimeout time.Duration

	// baseClient 投递基 ctx 的调用面（NewClient 装配期单写——Start 前的
	// 程序序建立 happens-before，投递读循环无锁读）：入站请求的激活与
	// Handle ctx 由基 ctx 派生，实体经 rans.ClientFrom 自省取调用面。
	// nil = 未接线（直连 Remote 形态）。
	baseClient rans.Client

	// inflightCancel 在途请求取消定位表（H10 决策 9）：newDelivery 构造点
	// 注册、release 摘除。临界区仅 map 增删，取消动作走投递原子标记面。
	inflightMu sync.Mutex
	inflight   map[cancelKey]*remoteDelivery
}

// setBaseClient 注入投递基 ctx 的调用面（NewClient 装配期调用，Start 前）。
func (d *Dispatcher) setBaseClient(c rans.Client) { d.baseClient = c }

// BindCanceledCounter 转发前取消收场的计数接线（票 12；remote 装配期把
// rans.call.canceled 计数器传入，与 activation 出队取消同面聚合）。
func (d *Dispatcher) BindCanceledCounter(c *metrics.Counter) { d.canceledFrames = c }

// BindViewInvalidate 视图失效钩子接线（票 01：重定向提示=拓扑信号——
// 对端换代/接管，候选与地址跟上；nil 静默）。
func (d *Dispatcher) BindViewInvalidate(fn func()) { d.viewInvalidate = fn }

// noteCanceledFrame 转发前取消收场计数（nil 安全）。
func (d *Dispatcher) noteCanceledFrame() {
	if d.canceledFrames != nil {
		d.canceledFrames.Add(1)
	}
}

// baseCtx 投递基 ctx：携带调用面（nil 未接线时退回 Background——行为零
// 变化）。入站请求的 GetOrActivate 与 Handle ctx 一律由本基 ctx 派生。
func (d *Dispatcher) baseCtx() context.Context {
	if d.baseClient != nil {
		return ctxkey.WithClient(context.Background(), d.baseClient) // 例外①：投递根 ctx——入站读循环无父 ctx 可派，Handle/激活 ctx 一律由它派生（携带统一调用面）
	}
	return context.Background() // 例外①：同上（未接线直连形态）
}

// NewDispatcher 构造分发器。mgr 提供激活编排（GetOrActivate/EntityOf）；
// gw 提供自源应答的本地投递路径；outbox 承接回包/转发出站的非阻塞入箱
// （装配期恒接线——Remote.New 传本节点发件箱）；self 为本端节点号（应答
// 头 SourceNodeID）；pool 为入站页归还池（nil 自建，与 NewGateway 同防御
// 口径）；pkgs 为随迁包缓存（nil → MigrationPackage 丢弃留痕——独立构造
// Dispatcher 的形态，rpc.New 总是接线缓存）；defCallTimeout 为入站请求的
// 防御缺省预算（≤0 落常量兜底，语义见 Dispatcher 字段注）。
func NewDispatcher(mgr *activation.Table, gw *Gateway, self rans.NodeID, pool *wire.PagePool, pkgs *PackageCache, defCallTimeout time.Duration, outbox *Outbox, cache *routing.LocationCache) *Dispatcher {
	if pool == nil {
		pool = wire.NewPagePool()
	}
	if defCallTimeout <= 0 {
		defCallTimeout = defaultCallTimeout
	}
	return &Dispatcher{mgr: mgr, gw: gw, self: self, pool: pool, pkgs: pkgs, outbox: outbox, cache: cache, defaultCallTimeout: defCallTimeout, inflight: make(map[cancelKey]*remoteDelivery)}
}

// newDelivery 构造远端投递并锚定请求 deadline（H10 决策 6）：deadline =
// 构造时刻（handleRequest 的入站接收时刻）+ 预算——入箱等待不重置预算，
// 出队执行入口据此复查「入箱后睡过预算」（H10 决策 7）。预算取扩展块
// budget_ms，对端未带或扩展块畸形（DecodeHeader walk 已挡大半）→ 本节点
// 防御缺省——预算缺位即防御缺省的存在意义，不因观测字段畸形升级为调用失败。
// 构造点同步注册取消定位表（H10 决策 9）：直到收场 MsgCancel 均可定位本投递。
func (d *Dispatcher) newDelivery(hdr wire.Header, body []byte, id rans.ID) *remoteDelivery {
	ext, _ := wire.DecodeRequestExt(hdr.Ext) // 畸形 ext → 零值：预算落防御缺省、无 ctx 项
	budget := d.defaultCallTimeout
	if ext.HasBudget {
		budget = extBudgetDuration(ext.BudgetMS)
	}
	r := &remoteDelivery{d: d, hdr: hdr, body: body, id: id, ext: ext, dl: time.Now().Add(budget)}
	d.registerCancel(r)
	return r
}

// registerCancel/unregisterCancel 取消定位表增删（构造点注册 / release 摘除）。
func (d *Dispatcher) registerCancel(r *remoteDelivery) {
	d.inflightMu.Lock()
	d.inflight[cancelKey{src: r.hdr.SourceNodeID, id: r.hdr.MessageID}] = r
	d.inflightMu.Unlock()
}

func (d *Dispatcher) unregisterCancel(r *remoteDelivery) {
	k := cancelKey{src: r.hdr.SourceNodeID, id: r.hdr.MessageID}
	d.inflightMu.Lock()
	if cur, ok := d.inflight[k]; ok && cur == r {
		delete(d.inflight, k)
	}
	d.inflightMu.Unlock()
}

// extBudgetDuration 扩展块预算毫秒 → 时长：uint64 毫秒可超出 int64 纳秒域，
// 畸形大值封顶到时长上限（只为不回绕出负 deadline，仍在一切现实预算之上）。
func extBudgetDuration(ms uint64) time.Duration {
	const maxMS = uint64(math.MaxInt64) / uint64(time.Millisecond)
	if ms > maxMS {
		ms = maxMS
	}
	return time.Duration(ms) * time.Millisecond
}

// BeginDraining 置位排空门闸（规格 §14.1 ① 的落点）：这是运行期状态
// 命令而非配置注入，一次性置位不复位——排空是终态动作，进程随后即停，
// Remote.Shutdown 是唯一生产置位点。置位后排空门闸按目标区分——本地表命中
// 的目标照常准入（不进激活路径——排空节点零新激活），本地 miss 回绝促重
// 路由（rejectWhileDraining 的口径）；Response/Error/Ping/Pong 心跳与
// MigrationPackage 不受影响。原子操作，可在任意 goroutine 调用。
func (d *Dispatcher) BeginDraining() { d.draining.Store(true) }

// HandleMessage 应用消息到达（transport.Handler 的消息半边）。body 为
// 池化页，所有权随本次调用移交：Request/OneWay 路径经 remoteDelivery 在
// 收场（Deliver/Reject/Forward）时恰好归还一次，其余路径就地归还。
// 排空门闸（规格 §14.1 ①/§10.3 同口径）：draining
// 置位时 Request/OneWay 的准入收敛到本地表——命中照常投递（既有实体
// 继续服务到排空收编为止），miss 在激活路径之前被拦下；既有投递（已在
// 收件箱/在途执行）不受门闸影响，由阶段②的统一销毁路径收编。
func (d *Dispatcher) HandleMessage(hdr wire.Header, body []byte) {
	// 入站即可达（票 04）：收到某节点帧即闭合该目标的发件箱熔断——分区
	// 恢复后首个入站帧立刻解除快速失败，不等冷却探针（无开断时零成本）。
	if src := rans.NodeID(hdr.SourceNodeID); src != 0 && src != d.self {
		d.outbox.NotifyNodeAlive(src)
	}
	switch hdr.Type {
	case protocol.MsgResponse, protocol.MsgError:
		d.gw.HandleResponse(hdr, body)
	case protocol.MsgRequest, protocol.MsgOneWay:
		d.handleRequest(hdr, body)
	case protocol.MsgCancel:
		d.handleCancel(hdr, body)
	case protocol.MsgOwnershipLost:
		d.handleOwnershipLost(hdr, body)
	case protocol.MsgRedirectHint:
		d.handleRedirectHint(hdr, body)
	case protocol.MsgMigrationPackage:
		d.handleMigrationPackage(hdr, body)
	default:
		if body != nil {
			d.pool.Put(body)
		}
		log.Debug("rans/rpc: unknown message type dropped",
			"type", uint8(hdr.Type), "message", hdr.MessageID)
	}
}

// rejectWhileDraining 排空门闸的拦截半边（规格 §14.1 ①；只承接本地 miss）：
//   - Request：回 Error(Unavailable+Draining)——Unavailable 属确定未执行
//     （分类唯一表 RetryUnconditional），调用方的环内重试自然
//     重路由到别处，这正是门闸回 Unavailable 的语义意图（促重路由）；匿名
//     源照旧丢弃（reply 的匿名分支，规格 §12）；
//   - OneWay：丢弃 + log.Warn 留痕（无回执面，规格「OneWay 丢弃留痕」）。
//
// 页就地归还（未进入任何投递路径）。Response/Error 与 MigrationPackage
// 不设闸：应答正是阶段③要 flush 的对象；随迁包是迁移在途的载荷——迁移
// 不因本端排空而中止（目标端激活消费或 TTL 5min 自行过期，快路径语义以
// 「包驻缓存」为准，Store 兜底不受影响）。
func (d *Dispatcher) rejectWhileDraining(hdr wire.Header, body []byte) {
	if body != nil {
		d.pool.Put(body)
	}
	if hdr.Type == protocol.MsgOneWay {
		log.Warn("rans/rpc: one-way message dropped while draining",
			"self", d.self, "message", hdr.MessageID, "source", hdr.SourceNodeID)
		return
	}
	d.replyError(hdr, rans.NewError(rans.CodeUnavailable, rans.ReasonDraining, "node draining"))
}

const ownershipLostDestroyBudget = 30 * time.Second // 止损销毁单实体预算（实体已 Fence，超时无害）

// handleCancel MsgCancel 定位与取消（H10 决策 9 收端半边）：按（源节点，
// MessageID）定位——执行中 → cancel 其 Handle ctx；在箱 → 置取消标记
// （出队弃置计数）；找不到 → 幂等忽略。body 无载荷就地归还。
func (d *Dispatcher) handleCancel(hdr wire.Header, body []byte) {
	if body != nil {
		d.pool.Put(body)
	}
	k := cancelKey{src: hdr.SourceNodeID, id: hdr.MessageID}
	d.inflightMu.Lock()
	r, ok := d.inflight[k]
	d.inflightMu.Unlock()
	if !ok {
		log.Debug("rans/rpc: cancel for unknown request ignored",
			"source", hdr.SourceNodeID, "message", hdr.MessageID)
		return
	}
	r.markCanceled()
}

// handleMigrationPackage 随迁包投递（规格 §13.3 E1 快路径的目标端半边）：
// 包体 = 原始快照字节（不套外封——快照形状是实体私有契约，
// Header 常规条件字段承载 TargetUUID 寻址）；扩展块 ExtFieldSrcIncarnation
// 携带源纪元（随包缓存，恢复侧与 Store 高水位比较的版本依据）。系统面单向：
// 不回执；也不激活——包驻缓存等激活闭包消费（load-first），激活时机归调用
// 方。畸形 TargetUUID → warn 丢弃（UUID 寻址失败即无语义可执行，且无地址
// 可回）。缓存未接线 → warn 丢弃（防御，见 NewDispatcher）。
func (d *Dispatcher) handleMigrationPackage(hdr wire.Header, body []byte) {
	defer func() {
		if body != nil {
			d.pool.Put(body)
		}
	}()
	if d.pkgs == nil {
		log.Warn("rans/rpc: migration package dropped (no package cache wired)",
			"message", hdr.MessageID)
		return
	}
	id, err := rans.NewID(hdr.TargetKind, hdr.TargetInst)
	if err != nil {
		log.Warn("rans/rpc: migration package dropped (malformed target uuid)",
			"message", hdr.MessageID, "err", err)
		return
	}
	// 包头扩展块携带源纪元（缺位 → 零值，按旧端发送方形态处理；畸形
	// 同落零值但留痕——与上方畸形身份的防御口径一致）。
	ext, err := wire.DecodeRequestExt(hdr.Ext)
	if err != nil {
		log.Warn("rans/rpc: migration package ext decode failed; treating as legacy sender form",
			"message", hdr.MessageID, "err", err)
	}
	var srcInc uint64
	if ext.HasSrcIncarnation {
		srcInc = ext.SrcIncarnation
	}
	d.pkgs.Put(id, body, srcInc) // copy-in：上方 defer 归还页后缓存内容不受影响
}

// handleRequest Request/OneWay 分发（规格 §10.3）：
//
//   - 目标身份畸形（rans.NewID 失败）→ Error(CodeDecode) 回错，不让调用
//     方白等；
//   - 排空门闸（draining 置位，规格 §14.1 ①）：
//     本地表（mgr.GetLocal）命中 → 照常准入投递（**不经 GetOrActivate**——
//     送达即终结，排空节点零新激活；排空与销毁之间 GetLocal/GetOrActivate
//     的竞态窗不会再造出注册竞态者；已进 Retire 交接窗的实体由 pawnite 留
//     箱/终局回绝，注销后调用方重试路由他节点）；miss → rejectWhileDraining
//     （Request 回 Unavailable+Draining 促重路由、OneWay 丢弃留痕）；
//   - GetOrActivate 重试环 ≤maxActivationAttempts：ErrUnknownKind →
//     Error(NotFound+UnknownKind)（kind 不在注册表的远端面与本地
//     激活面同构，NotFound 促源端重解析重试）；ErrActivationFailed
//     瞬态失败 → 重试（注册后的失败属执行前确定性失败——回执面一律
//     Unavailable+ActivationFailed；分类唯一表归 RetryUnconditional，源端
//     环内无条件重试）；其余错误 → Error(CodeError；CodeError 形态透传 Reason)；
//   - 仲裁落败（错误链携带 *activation.ArbitrationLostError）：GetOrActivate
//     入表后复检命中终态时错误同步返回——此刻消息尚未入任何收件箱，
//     pawnite 的整箱转发扑空，由本层直接按输家语义转发原始字节给赢家
//     （与整箱转发共用 Forward；见 manager 入表后复检分支）。
func (d *Dispatcher) handleRequest(hdr wire.Header, body []byte) {
	id, err := rans.NewID(hdr.TargetKind, hdr.TargetInst)
	if err != nil {
		if body != nil {
			d.pool.Put(body)
		}
		d.replyError(hdr, rans.NewError(rans.CodeDecode, rans.ReasonNone,
			"malformed target uuid: "+err.Error()))
		return
	}
	if d.draining.Load() {
		// 排空门闸：本地命中照常准入、miss 回绝促重路由。
		// 送达即终结（直接 ReceiveRequest）而非 GetOrActivate：门闸判定与
		// 激活之间实体被排空销毁的竞态窗内，GetOrActivate 会为 miss 重新
		// 走 register-first 激活（新注册落在排空节点、逃过阶段②快照成为
		// 僵尸）；直投把 miss 收口为回绝——重试路由他节点，语义闭合。
		if p, ok := d.mgr.GetLocal(id); ok {
			p.ReceiveRequest(d.newDelivery(hdr, body, id))
			return
		}
		d.rejectWhileDraining(hdr, body)
		return
	}
	for attempt := 0; attempt < maxActivationAttempts; attempt++ {
		p, err := d.mgr.GetOrActivate(d.baseCtx(), id)
		if err == nil {
			// 页所有权随投递移交 remoteDelivery（deadline 锚定于 newDelivery
			// 的接收时刻）；准入被拒时 Reject 已由 pawnite 发出（页随之释放、
			// Error 帧已回源）。
			p.ReceiveRequest(d.newDelivery(hdr, body, id))
			return
		}
		var lost *activation.ArbitrationLostError
		if errors.As(err, &lost) {
			// 输家转发（提示发送随 Forward 漏斗统一收口，见 delivery.go）。
			d.newDelivery(hdr, body, id).Forward(lost.WinnerNode())
			return
		}
		if errors.Is(err, activation.ErrUnknownKind) {
			if body != nil {
				d.pool.Put(body)
			}
			// 未知 kind 归一：远端面与本地激活面同构——
			// NotFound+UnknownKind（约定 spec §5.3 映射表），分类唯一表
			// 归 RetryNotFound：促源端重解析位置后重试（注册表可能刚在
			// 别节点扩了 kind）。
			d.replyError(hdr, rans.NewError(rans.CodeNotFound, rans.ReasonUnknownKind,
				"unknown kind: "+id.Kind))
			return
		}
		if !errors.Is(err, activation.ErrActivationFailed) {
			if body != nil {
				d.pool.Put(body)
			}
			e := rans.NewError(rans.CodeError, rans.ReasonNone, "activation error: "+err.Error())
			var re *rans.Error
			if errors.As(err, &re) && re.Code == rans.CodeError {
				e.Reason = re.Reason // 不可重试形态（如工厂 panic）的 Reason 原样过线
			}
			d.replyError(hdr, e)
			return
		}
		// ErrActivationFailed：瞬态失败，重试（页仍在本层手中）
	}
	if body != nil {
		d.pool.Put(body)
	}
	// 重试环耗尽仍失败：与逐次失败同口径回 Unavailable+ActivationFailed
	// （注册后失败是执行前确定性失败）。分类
	// 唯一表对 Unavailable+ActivationFailed 归 RetryUnconditional——
	// 源端环内无条件重试（仍受上限与退避约束）。
	d.replyError(hdr, rans.NewError(rans.CodeUnavailable, rans.ReasonActivationFailed,
		"activation failed after retries"))
}

// reply 构造应答帧并按入站 SourceNodeID 寻址发回（陷阱 4：不绑定到达
// 连接）。应答头拷贝入站 MessageID（关联键）、SourceNodeID=self、置
// HasStatus|HasSourceNodeID；Response 的 body 为外封对象、Error 的 body
// 为结构化错误对象（规格 §5.3「线上半边」；构造走 replyError）。
//
// 自源短路（陷阱 4 self case）：源即本进程（SourceNodeID==self，经输家
// 转发回弹的常见形态——调用方节点恰是仲裁赢家）时不走网络发送：回程按
// Source 寻址，而「源=本进程」的路由就是本进程的 pending 表，直接交网关
// HandleResponse 投递。绕开 SendToNode(self) 的回环双端连接——两条连接端
// 落在同一 tcp.Manager，registerNode 的同节点顶替策略会掐断承载连接，
// 应答永远到不了挂起中的表项（调用方固定超时收场）。MessageID 原样保留，
// 本地 pending 表项由此命中；页所有权语义与网络路径一致（HandleResponse
// 消费该页：投递给等待方，或迟到/未知 MessageID 时就地归还）。
//
// 两类不回：
//   - OneWay：无等待方（规格 §10.3 排空门闸同口径：丢弃留痕），回执只会
//     被源端网关按未知 MessageID 丢弃；
//   - 匿名源（SourceNodeID=0，规格 §12「匿名源不转发回 NotFound 促客户端
//     自愈」）：无地址可回，drop+log——不向节点 0 发空地址帧。
//
// 出站形态（H11 决策 12/13）：应答帧经节点发件箱非阻塞入箱（满即丢计数，
// 发送归分片 worker 异步完成）——本函数可在连接读循环与实体串行 loop 上
// 被调，箱满不再构成队头阻塞；被丢应答的源端按请求预算超时收场（可重
// 试，best-effort 回执语义）。
func (d *Dispatcher) reply(in wire.Header, typ protocol.MessageType, status rans.Code, body []byte) {
	if in.Type == protocol.MsgOneWay {
		log.Debug("rans/rpc: one-way message gets no reply", "message", in.MessageID)
		return
	}
	if in.SourceNodeID == 0 {
		log.Debug("rans/rpc: reply to anonymous source dropped", "message", in.MessageID)
		return
	}
	hdr := wire.Header{
		Type:         typ,
		Flags:        protocol.FlagHasStatus | protocol.FlagHasSourceNodeID,
		MessageID:    in.MessageID,
		Status:       status,
		SourceNodeID: uint16(d.self),
	}
	if rans.NodeID(in.SourceNodeID) == d.self {
		d.gw.HandleResponse(hdr, body)
		return
	}
	// 非阻塞入箱：满即丢计数（best-effort 回执的固定满策略，决策 12），
	// 调用方（源端）按请求预算超时收场。
	if err := d.outbox.TryPut(rans.NodeID(in.SourceNodeID), hdr, body); err != nil {
		log.Warn("rans/rpc: reply dropped without sending",
			"message", hdr.MessageID, "target", in.SourceNodeID, "err", err)
	}
}

// replyError 回结构化 Error 帧：统一错误对象经 wire.MarshalError
// 编码为帧体（Code/Reason/Message/AppCode/AppData 全维过线），header
// Status 与 body 的 Code 同值（头部快速判定面保留；线上判定以帧体为准）。
// body 为新分配缓冲（非池页），所有权语义与 reply 一致。OneWay/匿名源
// 的不回规则、自源短路均由 reply 承接。
func (d *Dispatcher) replyError(in wire.Header, e *rans.Error) {
	d.reply(in, protocol.MsgError, e.Code, wire.MarshalError(e))
}
