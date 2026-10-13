// Package rpc 实现远端消息网关（规格 §8/§9.1）：pending 关联表 +
// 逐请求预算 deadline + 按源寻址的端点发送韧性。Gateway 只搬 header+bytes
// ——信封（wire.Envelope）的编解码是上层 Remote 的职责，envBytes 对本包
// 不透明。
package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/transport/tcp"
	"github.com/cyinx/rans_golang/internal/wire"
)

// 发送韧性与调用预算默认参数（附录 A；重试 5 次 × 2s——「批量发送」由
// 写循环的 bufio.Writer 承接，channel 积压自然合帧）。
const (
	// defaultCallTimeout 无 deadline 调用的防御缺省预算（同值公开常量
	// node.DefaultCallTimeout）：统一调用面合成后 ctx 恒带 deadline，
	// 本值只兜底直连网关的内部形态。
	defaultCallTimeout = 30 * time.Second
	// defaultMaxCallTimeout 调用预算上限缺省（= node.DefaultMaxCallTimeout）。
	defaultMaxCallTimeout = 10 * time.Minute
	defaultRetryInterval  = 2 * time.Second
	maxSendRetries        = 5 // 首次尝试之外的追加重试次数（共 1+5 次尝试）

	// deadline 扫描器参数（规格 §9.1 / 附录 A NET-8）：默认 250ms 全表扫
	// 一次；pending 容量上限 100_000，满载即拒新请求。
	defaultSweepInterval = 250 * time.Millisecond
	defaultMaxPending    = 100_000
)

// rpcResult 一次请求的应答投递：err 非 nil 时为 *rans.Error（统一错误
// 模型形态——Error 帧 / 网关超时 / 快速失败三类收场的共同形态），此时
// body 为空。
type rpcResult struct {
	hdr  wire.Header
	body []byte // 池化页：所有权随投递转移给等待方，解码后由其归还
	err  error
}

// pendingEntry pending 表项：ch 缓冲 1（select-default 非阻塞投递的双发
// 守卫基础）；deadline 为登记时刻按本次调用预算算出的截止点（逐请求，
// H10 决策 4）——超时判定唯一权威（扫描器全表扫描）；budget = 预算快照。
type pendingEntry struct {
	ch       chan rpcResult
	target   rans.NodeID
	deadline time.Time
	budget   time.Duration
}

// connSender 包内发送端口：把 Gateway 对 *tcp.Manager 的依赖收窄为最小面
// （tcp.Manager 自然满足；测试以替身记录按节点寻址参数——陷阱 4 回程
// 寻址断言的锚点）。不导出：包内构件，Remote 组合时同样只经
// Gateway 间接使用。
type connSender interface {
	// SendToNode 按节点发送（resolve 为节点→地址解析；语义同 tcp.Manager，
	// body 所有权随成功返回移交连接）。
	SendToNode(ctx context.Context, node rans.NodeID, resolve func(ctx context.Context, n rans.NodeID) (string, error), hdr wire.Header, body []byte) error
}

// 编译期断言：tcp.Manager 满足 connSender。
var _ connSender = (*tcp.Manager)(nil)

// Gateway 远端消息网关：按 MessageID 关联请求与应答（pending 表 + mutex，
// 陷阱 4/7 纪律），Call/OneWay 两种发送语义，端点发送失败重试 + 判停。
//
// 超时模型（规格 §9.1）：零 per-request timer——构造即启动单
// goroutine deadline 扫描器，按 sweepInterval 全表扫 pending，期限已过即
// 「先摘表（释放门）后完成 waiter」（陷阱 11 次序）投递 *rans.Error{Timeout}；
// pending deadline 逐请求在登记时按调用 ctx 的合成预算计算（H10 决策 4），
// defTimeout 只兜底无 deadline 的直连形态。Close 停止扫描器。快速失败由
// FailPendingForNode 承担（摘目标节点全部在途请求并投递 NodeUnavailable
// 类错误）。
//
// 回程寻址（陷阱 4）：应答一律按请求头里的 SourceNodeID 经
// SendToNode(source) 寻址发回——绝不绑定请求到达的那条连接（对端可能已
// 重连换连接）。Gateway 自身不构造应答，写 SourceNodeID 的职责在调用方
// （分发器 / Remote）。
type Gateway struct {
	conns         connSender
	resolve       func(ctx context.Context, n rans.NodeID) (string, error)
	pool          *wire.PagePool
	defTimeout    time.Duration // 无 deadline 调用的防御缺省预算（默认 30s）
	retryInterval time.Duration // 发送重试间隔（默认 2s；测试可缩短）
	ids           wire.IDGen    // 进程内单调（陷阱 7）

	sweepInterval time.Duration // deadline 扫描周期（默认 250ms；测试可缩短）

	// pt 分片化 pending 表（H15：256 分片+到期堆+按节点索引；完成先于
	// 释放门的陷阱 11 次序在各分片内保持）。mu 仍护其余非热路径字段。
	pt           *pendingTable
	onTerminated func(node rans.NodeID)

	stopCh    chan struct{} // Close 关闭之，扫描器退出
	stopOnce  sync.Once     // Close 幂等
	sweepDone chan struct{} // 扫描器 goroutine 退出后关闭（泄漏断言锚点）
}

// NewGateway 构造消息网关（deadline 扫描器随构造启动，扫描周期取默认
// 250ms）。conns 提供按节点发送（SendToNode 的 resolve→拨号路径；收窄为
// 包内 connSender 接口，tcp.Manager 自然满足）；resolve 为节点→地址解析；
// pool 为应答页的归还池（nil 自建，与 tcp.Manager 同防御口径）；
// defTimeout 为无 deadline 调用的防御缺省预算（≤0 取默认 30s——统一调用
// 面合成后 ctx 恒带 deadline，本值只兜底直连形态）；onEndpointTerminated
// 为端点判停回调（某节点发送重试耗尽时以目标节点号调用——锁外执行，不
// 得长时间阻塞；构造参数注入；nil = 缺省 log.Warn）。
func NewGateway(conns connSender, resolve func(ctx context.Context, n rans.NodeID) (string, error), pool *wire.PagePool, defTimeout time.Duration, onEndpointTerminated func(rans.NodeID)) *Gateway {
	return NewGatewayWithSweep(conns, resolve, pool, defTimeout, defaultSweepInterval, onEndpointTerminated)
}

// NewGatewayWithSweep 同 NewGateway，但显式指定 deadline 扫描周期
// （sweepInterval ≤0 取默认 250ms）——测试注入短周期（毫秒级）使超时用例
// 在一个 tick 内收场；生产行为经 NewGateway 取附录 A 默认值。
func NewGatewayWithSweep(conns connSender, resolve func(ctx context.Context, n rans.NodeID) (string, error), pool *wire.PagePool, defTimeout, sweepInterval time.Duration, onEndpointTerminated func(rans.NodeID)) *Gateway {
	if defTimeout <= 0 {
		defTimeout = defaultCallTimeout
	}
	if sweepInterval <= 0 {
		sweepInterval = defaultSweepInterval
	}
	if pool == nil {
		pool = wire.NewPagePool()
	}
	g := &Gateway{
		conns:         conns,
		resolve:       resolve,
		pool:          pool,
		defTimeout:    defTimeout,
		retryInterval: defaultRetryInterval,
		sweepInterval: sweepInterval,
		pt:            newPendingTable(defaultMaxPending),
		stopCh:        make(chan struct{}),
		sweepDone:     make(chan struct{}),
		onTerminated:  onEndpointTerminated,
	}
	go g.sweepLoop()
	return g
}

// Close 停止 deadline 扫描器 goroutine（幂等，可重复调用）。调用方须先让
// 在途 SendRequest 全部收场（扫描器停止后不再有超时兜底）；Remote 的
// Close 与 Shutdown 停序调用本方法。
func (g *Gateway) Close() {
	g.stopOnce.Do(func() { close(g.stopCh) })
}

// sweepLoop deadline 扫描器主循环（规格 §9.1：单 PeriodicTimer 全表扫描
// 的直译）：按 sweepInterval 周期扫描 pending，Close 即退。
func (g *Gateway) sweepLoop() {
	defer close(g.sweepDone)
	ticker := time.NewTicker(g.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			g.sweepDeadlines()
		}
	}
}

// sweepDeadlines 一轮全表扫描：期限已过（now ≥ deadline）的表项在 mu 内
// 摘除、锁外投递超时错误（缓冲 1 且摘表保证唯一投递者，send 必不
// 阻塞）。陷阱 11 次序——**先摘表（释放门）后完成 waiter**：摘表即
// 释放投递门，此后 deliver 对该表项必然扑空（迟到丢弃），完成动作与投递
// 竞态不再可能双投递；锁内只做列表搬移（陷阱 2），channel 发送留锁外。
func (g *Gateway) sweepDeadlines() {
	expired := g.pt.reapExpired(time.Now())
	for _, r := range expired {
		// 统一错误模型：请求预算耗尽 → *rans.Error{Timeout}，Node 携带
		// 目标节点，文本保留 MessageID/本次预算快照供排障。
		r.e.ch <- rpcResult{err: rans.NewError(rans.CodeTimeout, rans.ReasonNone,
			fmt.Sprintf("rans: invoke: request timeout (message=%d target=%d budget=%s)",
				r.id, r.e.target, r.e.budget))}
	}
}

// requestDeadline 逐请求 deadline 与预算（登记时刻计算，H10 决策 4）：
// 调用 ctx 已带 deadline（统一调用面按 min(ctx 剩余, Timeout(d), Max) 合成
// 派生）即直接采用；无 deadline 的直连形态才落防御缺省（缺省只兜底）。
func (g *Gateway) requestDeadline(ctx context.Context) (time.Time, time.Duration) {
	if dl, ok := ctx.Deadline(); ok {
		return dl, time.Until(dl)
	}
	return time.Now().Add(g.defTimeout), g.defTimeout
}

// FailPendingForNode 快速失败（规格 §9.1）：
// 摘除目标节点全部在途请求并投递 *rans.Error{Code: ConnectionLost,
// Node: node}（在途请求可能已送达——分类唯一表归模糊类），返回摘除数。
// reason 为失联语境（仅进诊断文本）。次序同扫描器：mu 内先摘表、锁外完成
// waiter（陷阱 11）。其他节点的表项不受影响；reason 可为 nil。
func (g *Gateway) FailPendingForNode(node rans.NodeID, reason error) int {
	reaped := g.pt.reapForNode(node)
	msg := fmt.Sprintf("rans: gateway: node %d unavailable", node)
	if reason != nil {
		msg += ": " + reason.Error()
	}
	for _, e := range reaped {
		e.ch <- rpcResult{err: &rans.Error{Code: rans.CodeConnectionLost, Message: msg, Node: node}}
	}
	return len(reaped)
}

// NextMessageID 返回进程内单调递增的 MessageID（陷阱 7：请求/响应关联键
// 不得复用——回绕前复用会让 pending 表错配到旧请求）。并发安全，从 1 起
// （0 保留为"无 ID"哨兵）。
func (g *Gateway) NextMessageID() uint64 { return g.ids.Next() }

// PendingCount 返回 pending 表当前条目数（排空 flush 的等待面，
// 规格 §14.1 ③）：出站请求-应答在途数归零即最终响应已全部收口——
// Remote.Shutdown 的阶段③在 FlushWindow 有界窗口内轮询本计数到零。
func (g *Gateway) PendingCount() int {
	return g.pt.count()
}

// SendRequest 直发形态的请求-响应发送（Call 语义，规格 §9.1 逐请求预算）
// ——发送段同步于调用方（directSend 腿）。生产 Call 路径走 SendRequestQueued
// （请求帧入发件箱，同目标 FIFO 承载「先 Tell 后 Call」次序）；两形态的
// 登记/等待/超时/取消语义完全同源（共用实现）。
//
// 调用方须先以 NextMessageID 预填 hdr.MessageID（ID 即 pending 关联键，
// 在途期间不得复用）；应答（Response/Error 帧）经 HandleResponse 投递回
// 本调用。
//
// 返回语义（错误一律 *rans.Error 形态——统一错误模型）：
//   - Response 到达：返回对端 header 与 body；body 为池化页，所有权经
//     HandleResponse → pending channel → 本调用移交调用方——解码消费后
//     必须 pool.Put 归还（全程移交不拷贝）。
//   - Error 帧：结构化错误对象解码为 *rans.Error（Code/Reason/
//     Message/AppCode/AppData 全维过线，Node 填源节点），无页需归还。
//   - 预算耗尽：*rans.Error{Timeout}——pending deadline 在登记时按调用
//     ctx 的 deadline（统一调用面的合成结果）计算，由 deadline 扫描器
//     到期投递（零 per-request timer）；调用方 ctx 到期：*rans.Error{Timeout}、
//     主动取消：*rans.Error{Canceled}（即时，不等扫描；收场同时向目标节点
//     尽力发 MsgCancel，见 gateway_cancel.go）；发送失败（重试耗尽）：
//     *rans.Error{Unavailable+SendFailed}。后三种收场均摘除 pending 表项，
//     其后到达的响应按迟到丢弃（页由 HandleResponse 就地归还）。
//   - 容量满载（附录 A NET-8）：立即拒绝（Unavailable+Overloaded），不入表
//     不发送。
//
// envBytes 对本包不透明；其所有权随发送成功移交连接（写循环写出后归还
// 页池），发送失败时不移交、仍归调用方——与 transport.Conn.Send 同口径。
func (g *Gateway) SendRequest(ctx context.Context, target rans.NodeID, hdr wire.Header, envBytes []byte) (wire.Header, []byte, error) {
	return g.SendRequestQueued(ctx, target, hdr, envBytes, g.directSend)
}

// directSend 直发投递腿：sendWithRetry 同步发送（预算取派生 ctx 的
// deadline，即防御缺省或调用 ctx 的较早者），失败同步返回——与入箱
// 形态的 fail 回投殊途同归（本腿在调用方 goroutine 上内联完成，无需
// 回投面，fail 形参仅为对齐投递面签名）。
func (g *Gateway) directSend(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte, fail func(error)) error {
	dl, _ := ctx.Deadline()
	return g.sendWithRetry(ctx, target, hdr, body, dl, true)
}

// SendRequestQueued 登记请求、经投递面入箱后阻塞等待应答（Call 语义，
// 规格 §9.1 逐请求预算；返回语义与 SendRequest 同源，见其注释）。via 为
// 投递面：同步返回非 nil = 确定未投递（未入箱的容量/关箱拒绝，或直发
// 形态的发送段确定失败），原样上抛；入箱形态的异步发送失败经 fail 回投
// （由本方法登记的 pending 项收场，等待段据此返回——调用方无需区分直
// 发/入箱两形态）。投递 ctx（预算派生）随 via 传递：入箱阻塞与帧内发
// 送预算共用同一 deadline 权威。
func (g *Gateway) SendRequestQueued(ctx context.Context, target rans.NodeID, hdr wire.Header, envBytes []byte,
	via func(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte, fail func(error)) error) (wire.Header, []byte, error) {
	id := hdr.MessageID
	deadline, budget := g.requestDeadline(ctx)
	e := &pendingEntry{
		ch:       make(chan rpcResult, 1),
		target:   target,
		deadline: deadline,
		budget:   budget,
	}
	if err := g.pt.register(id, target, e); err != nil {
		if err == ErrDuplicateMessageID {
			return wire.Header{}, nil, fmt.Errorf("%w: message id %d (duplicate SendRequest)", err, id)
		}
		return wire.Header{}, nil, fmt.Errorf("%w: rejecting message id %d (appendix A NET-8)", err, id)
	}

	// 兜底摘除 + 竞速回收：正常路径表项已被投递方摘除（no-op）；ctx 取消/
	// 发送失败路径在此清表，并回收竞速窗口内恰好入 channel 的页。超时路径
	// 的清表在扫描器（本 defer 对已摘表项为 no-op，drain 亦无页可回收）。
	defer g.removePending(id, e)

	// 发送阶段与等待段共用同一 deadline 权威（e.deadline）：调用 ctx 已带
	// deadline 即原样贯穿（预算边界解除阻塞的拨号/写出/入箱等待）；无
	// deadline 的直连形态以防御缺省派生投递 ctx。等待段取消归调用方，
	// 到期归扫描器。
	sendCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		sendCtx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}
	if err := via(sendCtx, target, hdr, envBytes, func(err error) { g.deliver(id, rpcResult{err: err}) }); err != nil {
		return wire.Header{}, nil, err
	}

	select {
	case r := <-e.ch:
		if r.err != nil {
			return wire.Header{}, nil, r.err
		}
		return r.hdr, r.body, nil
	case <-ctx.Done():
		return wire.Header{}, nil, g.canceledOutcome(ctx, target, hdr)
	}
}

// removePending 摘除 pending 表项并回收竞速结果页（SendRequest 收场路径
// 专用）。与 deliver 的互斥纪律：map 摘除都在 mu 内，谁摘到表项谁负责
// 结果页的去向——本方摘到（投递方扑空、就地归还页）后 channel 必空，
// drain 为 no-op；投递方先摘到（已投递入 channel）则本方 drain 回收该页。
// 两条路径互斥且穷尽，结果页恰好归还一次。
func (g *Gateway) removePending(id uint64, e *pendingEntry) {
	g.pt.remove(id)
	select {
	case r := <-e.ch:
		if r.body != nil {
			g.pool.Put(r.body)
		}
	default:
	}
}

// HandleResponse 处理到达的 Response/Error 帧（分发器把应用消息按类型
// 路由到此；测试亦可直接注入）。
//
// 页所有权契约（与 SendRequest 的对偶）：
//   - Response 帧：body 页经 pending channel 原样移交等待方（SendRequest
//     调用方解码后 Put 归还），本方法不拷贝不归还；
//   - Error 帧：body 解码为 *rans.Error（结构化对象；Message/AppData
//     拷出）后页就地归还；
//   - 迟到/未知 MessageID（含重复响应）：页就地归还 + log.Debug，
//     绝不 panic（双发守卫：锁内摘表保证唯一投递者，channel 缓冲 1）。
//
// 回程寻址（陷阱 4）：本方法不回写任何帧；应答由调用方构造 header 写
// SourceNodeID 后经 SendToNode(source) 发回——不绑定到达连接。
func (g *Gateway) HandleResponse(hdr wire.Header, body []byte) {
	switch hdr.Type {
	case protocol.MsgResponse:
		g.deliver(hdr.MessageID, rpcResult{hdr: hdr, body: body})
	case protocol.MsgError:
		// 统一错误模型：Error 帧 body 为结构化错误对象
		// （wire.UnmarshalError）——Code/Reason/Message/AppCode/AppData 全维
		// 过线，线上 reason 以帧为准（NotFound 帧的 reason 由发射端显式
		// 携带）。
		// Node 按帧头源节点回填（本地/远端同构）。畸形 body 不做文本兼容
		// 读取：降级为 header Status + 解码失败文本，调用方不
		// 悬空等超时。body 已消费（AppData/Message 均拷出），页就地归还。
		e, derr := wire.UnmarshalError(body)
		if derr != nil {
			e = &rans.Error{Code: hdr.Status,
				Message: "rans: decode: malformed error frame body: " + derr.Error()}
		}
		e.Node = rans.NodeID(hdr.SourceNodeID)
		if body != nil {
			g.pool.Put(body)
		}
		g.deliver(hdr.MessageID, rpcResult{err: e})
	default:
		if body != nil {
			g.pool.Put(body)
		}
		log.Debug("rans/rpc: non-response frame dropped at gateway",
			"type", uint8(hdr.Type), "message", hdr.MessageID)
	}
}

// deliver 按 MessageID 投递结果：锁内摘表 + 非阻塞入 channel（缓冲 1 且
// 摘表保证唯一投递者，send 必不阻塞，持锁无等待风险）；无等待者（迟到/
// 未知/重复）时锁外归还页并留调试日志。
//
// 陷阱 11 配套口径：期限已过的表项视同已超时者——响应不得投递（页就地
// 归还），且**不抢摘表**：完成权留给扫描器（先摘表后完成次序收场
// 超时错误），本方法绝不制造「表项已删却无超时投递」的
// 悬空等待。
func (g *Gateway) deliver(id uint64, r rpcResult) {
	e, ok := g.pt.takeForDeliver(id, time.Now())
	if ok {
		e.ch <- r
	}
	if !ok {
		if r.body != nil {
			g.pool.Put(r.body)
		}
		log.Debug("rans/rpc: late or unknown response dropped", "message", id)
	}
}

// SendOneWay 单向发送（规格 §8 OneWay 语义）：不登记 pending、不等应答。
// at-most-once 交付：重试仅发生在上一次尝试确定失败（帧未送出：resolve
// 失败/拨号失败/连接未就绪/写出错误）之后，不构成补偿性重投；重试耗尽
// 即消息丢失，由 OnEndpointTerminated 的端点判停兜底。
func (g *Gateway) SendOneWay(ctx context.Context, target rans.NodeID, hdr wire.Header, envBytes []byte) error {
	return g.sendWithRetry(ctx, target, hdr, envBytes, time.Time{}, true)
}

// SendOneWayBounded 带请求 deadline 的定向发送（发件箱调用帧的发送面，
// 与 SendOneWay 同一端点韧性）：重试预算以 deadline 为界——排队过期的
// 帧在此中止（确定未送出，不产生迟到执行）。重试耗尽不叠加端点判停
// （notifyExhausted=false，见 sendWithRetry）：耗尽收场经帧内 fail 回投
// 等待方——确定未投递形态保留调用方的无条件重试权；节点失联的表项
// 摘除与缓存清扫仍由单向帧耗尽和连接丢失事件承载。
func (g *Gateway) SendOneWayBounded(ctx context.Context, target rans.NodeID, hdr wire.Header, envBytes []byte, deadline time.Time) error {
	return g.sendWithRetry(ctx, target, hdr, envBytes, deadline, false)
}

// sendWithRetry 端点发送韧性：瞬时
// 发送失败（resolve 失败/拨号失败/连接未就绪/写出错误）按 retryInterval
// 间隔重试至多 maxSendRetries 次，ctx 取消即止；耗尽后触发
// OnEndpointTerminated 并返回包装的末次错误。body 所有权仅在最终成功的
// 那次尝试随 Send 移交连接，失败尝试不移交（重试可安全复用同一 body）。
// 首次重试免退避（attempt 1 即时），自第二次起按 retryInterval 线性退避：
// 收敛期的瞬时失败（退役连接/陈旧条目/就绪前死亡）在首次即时重试后经 byNode
// 即刻落到存活连接，不该吃 2s 间隔；真端点失联仍有后续退避兜底。
//
// deadline（零值 = 无预算，SendOneWay 形态）：请求-响应调用的发送阶段与
// 等待段共用 pending deadline——预算耗尽即收场（此前尝试全部失败 ⟹ 消息
// 从未送出，按 Unavailable+SendFailed 确定未投递收场；不触发端点判停——节点未必
// 失联，是本请求没了预算）。退避启动前预检：退避全程已在预算外时不再白等。
//
// notifyExhausted 控制重试耗尽是否触发端点判停：true（单向/直发形态）
// ——FailPendingForNode 摘除该节点全部在途表项并投递 ConnectionLost；
// false（调用帧发送面）——判停会把本请求刚耗尽的表项摘走、等待段读到
// ConnectionLost（歧义类），覆盖掉 fail 回投的确定未投递形态，夺走非幂
// 等调用方的无条件重试权。
func (g *Gateway) sendWithRetry(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte, deadline time.Time, notifyExhausted bool) error {
	var lastErr error
	for attempt := 0; attempt <= maxSendRetries; attempt++ {
		if attempt > 1 && !errors.Is(lastErr, tcp.ErrRetiring) {
			if !deadline.IsZero() && !time.Now().Add(g.retryInterval).Before(deadline) {
				return g.sendBudgetExhausted(target, lastErr)
			}
			t := time.NewTimer(g.retryInterval)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return ctxOutcome(ctx)
			}
		}
		err := g.conns.SendToNode(ctx, target, g.resolve, hdr, body)
		if err == nil {
			return nil
		}
		lastErr = err
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return g.sendBudgetExhausted(target, err)
		}
		if errors.Is(err, tcp.ErrManagerClosed) {
			// 管理器已关停（Stop/Drain 后的残余发送）：终态，重试无意义。
			return fmt.Errorf("%w: %w", errSendFailed, err)
		}
		if ctx.Err() != nil {
			return ctxOutcome(ctx)
		}
	}
	if notifyExhausted {
		g.fireEndpointTerminated(target)
	}
	// Unavailable+SendFailed 形态先行（统一错误模型）：连接类失败＝
	// 确定未投递，分类唯一表归 RetryUnconditional。
	return fmt.Errorf("%w: send to node %d failed after %d retries: %w",
		errSendFailed, target, maxSendRetries, lastErr)
}

// sendBudgetExhausted 发送阶段的请求预算收场：所有尝试都失败＝消息从未
// 送出连接，按确定未投递哨兵包装（Classify 归 RetryUnconditional——环内
// 下一 attempt 以新预算重走路由）。
func (g *Gateway) sendBudgetExhausted(target rans.NodeID, lastErr error) error {
	if lastErr != nil {
		return fmt.Errorf("%w: send to node %d aborted at request deadline: %w",
			errSendFailed, target, lastErr)
	}
	return fmt.Errorf("%w: send to node %d aborted at request deadline",
		errSendFailed, target)
}

// fireEndpointTerminated 触发端点判停（锁外调用构造期注入的回调——
// NewGateway 的 onEndpointTerminated 参数，构造期定死）。
func (g *Gateway) fireEndpointTerminated(node rans.NodeID) {
	if g.onTerminated == nil {
		log.Warn("rans/rpc: endpoint terminated after send retries exhausted", "node", node)
		return
	}
	g.onTerminated(node)
}
