// Manager：连接管理器（规格 §10.1 ConnectionsManager 的 Go 形态）。
// 职责：按地址懒建连接 + 单飞拨号、Listener 接入、按节点索引（握手完成后
// 登记）、NodeConnectionLost 事件、心跳保活（仅 Ready 连接，§10.2）。
// 与 Conn 的分工：Conn 负责单条连接的编解码/握手/背压；Manager 负责
// 连接表生命周期与节点级事件翻译（per-conn 事件 → per-node 事件）。
package tcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang/internal/log"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/transport"
	"github.com/cyinx/rans_golang/internal/wire"
)

var errAlreadyListening = errors.New("rans: listen: already listening")

// ErrManagerClosed Manager 已 Close（哨兵，与连接级 ErrClosed 区分）：
// Close 后的拨号/监听/心跳启动一律以本错误拒绝——「Close 后复活半残体」
// （无心跳、无收口的连接或监听）不是合法状态。
var ErrManagerClosed = errors.New("rans: manager: closed")

// drainWaitBudget 连接收敛等待旧连接排空的有界预算（票 06；与
// retireLinger 同量级——对端正常收口毫秒级完成，僵死时到点放行）。
const drainWaitBudget = 2 * time.Second

// ErrWrongPeer SendToNode 的对端身份不符（票 02）：解析到的地址已被其他
// 节点复用（如 k8s pod IP 复用）——帧确定未投递（rpc 网关包装为
// Unavailable+SendFailed，调用方可无条件重试）；连接本身留给真实对端。
var ErrWrongPeer = errors.New("rans: send: connection peer mismatch")

// managedConn Manager 视角的连接登记项：三张表（byAddr/byNode/conns）
// 的值类型，携带连接的经理侧私有状态。
type managedConn struct {
	c     *Conn
	addr  string        // 拨号目标地址；listener 接入的连接为 ""（不进 byAddr）
	ready chan struct{} // 握手完成即 close（SendToNode 的就绪等待点）

	// peer 握手得到的对端节点号（票 02：SendToNode 的身份校验面）；mu 写
	//（registerNode，先于 close(ready)）、ready 关闭后免锁读。0 = 尚未握手。
	peer rans.NodeID
	// peerWarned 身份不符的 Warn 单次标记（同一连接只告警一次）。
	peerWarned atomic.Bool

	superseded atomic.Bool // 被同节点新连接替换：其 Lost 不再上报（节点仍有新连接）
}

// connHandler 用户 handler 的 per-conn 包裹层：把连接级回调翻译成
// Manager 的表操作 + 节点级事件。HandleMessage 原样透传（body 页所有权
// 纪律不变：移交用户 handler）。
type connHandler struct {
	m    *Manager
	mc   *managedConn
	user transport.Handler
}

func (h *connHandler) HandleMessage(hdr wire.Header, body []byte) {
	h.user.HandleMessage(hdr, body)
}

// OnHandshakeComplete 先登记 byNode（替换策略：同节点旧连接关旧，
// 新连接顶替），再放行就绪等待者，最后透传用户回调——用户收到事件时
// ConnToNode 已可查。
func (h *connHandler) OnHandshakeComplete(peer rans.NodeID) {
	h.m.registerNode(h.mc, peer)
	h.user.OnHandshakeComplete(peer)
}

// OnConnectionLost 先清表（身份守卫），锁外再上报（§19 陷阱 2）。
func (h *connHandler) OnConnectionLost(peer rans.NodeID) {
	h.m.connLost(h.mc, peer)
}

// Manager TCP 连接管理器。并发安全；所有回调（用户 handler 三方法与
// Config.OnReconnect 重拨观察钩子）均在 Manager 锁外调用。
//
// 关闭语义：Close 是终态——此后拨号（GetOrConnect/SendToNode 的懒建
// 路径）、Listen、StartHeartbeat 一律 ErrManagerClosed/拒绝；与在途单飞拨号的
// 竞态由登记临界区（startConn）的 closed 复验兜底：Close 快照 conns 之后
// 才登记的连接当场关闭并使拨号带因失败，不留孤儿连接。
type Manager struct {
	cfg     Config
	handler transport.Handler
	pool    *wire.PagePool

	mu          sync.Mutex
	byAddr      map[string]*managedConn      // 拨号目标地址 → 连接（懒建缓存）
	byNode      map[rans.NodeID]*managedConn // 对端节点号 → 连接（握手完成后登记）
	acceptStart sync.Once                    // accept 循环单发（ListenBind+StartAccept 的次序拆分面）
	conns       map[*managedConn]struct{}
	dialing     map[string]*dialFlight // 进行中的单飞拨号
	hadConn     map[string]struct{}    // 曾成功建连的地址集（重拨判据：断连后再拨即计数）
	listener    net.Listener
	hbCancel    context.CancelFunc
	closed      bool // Close 终态旗标（mu 保护）：post-close 拨号/监听/心跳全拒

	lastBatch atomic.Int64 // 最近一次写冲刷的帧数（跨连接，观测面）
	closeOnce sync.Once
}

// NewManager 构造连接管理器。cfg 为其下所有连接的共享配置（本端
// NodeID/集群名/密钥/指纹）；pool 为 nil 时自建共享页池（所有连接
// 复用同一池，收支锚点在管理器层闭合）。h 为 nil 时装配期 panic
// （连接事件无处投递的装配错误，fail-fast 而非首条消息才爆）。
func NewManager(cfg Config, h transport.Handler, pool *wire.PagePool) *Manager {
	if h == nil {
		panic("rans: new manager: nil handler")
	}
	if pool == nil {
		pool = wire.NewPagePool()
	}
	return &Manager{
		cfg:     cfg,
		handler: h,
		pool:    pool,
		byAddr:  make(map[string]*managedConn),
		byNode:  make(map[rans.NodeID]*managedConn),
		conns:   make(map[*managedConn]struct{}),
		dialing: make(map[string]*dialFlight),
		hadConn: make(map[string]struct{}),
	}
}

// LastBatchSize 返回最近一次写冲刷的帧数（跨本管理器全部连接，观测面
// ——rans.transport.batch_size Gauge 数据源；0 = 尚无写出）。
// 「最近」语义：任一连接的 writeLoop 清空队列冲刷即更新，读数是最近的
// 批量而非均值/累计。
func (m *Manager) LastBatchSize() int { return int(m.lastBatch.Load()) }

// recordBatch 记录一次写冲刷的帧数（Conn 写循环经 onFlush 回灌；n≤0 忽略
// ——空冲刷无观测价值）。
func (m *Manager) recordBatch(n int) {
	if n > 0 {
		m.lastBatch.Store(int64(n))
	}
}

// reapWhenDone 连接终态收尸：Done 关闭 ⟹ conns 表项移除，与死亡形态
// 无关。收尸是 conns 生命周期的
// 单一真相源：覆盖三类无其他清表点的死亡形态——拨号侧到手即死、拨号侧
// 握手超时僵尸（无人重拨即滞留）、接入侧半开（永久滞留，外部探测可无界
// 积累表项）。既有清表点保留（幂等，其中 getOrConnect 的 byAddr 死检查
// 必须同步于重拨，不等收尸）。byAddr 清理带身份守卫：仅当表项仍是本连接
// 时移除（替代连接不得误逐，同 connLost 纪律），使无人重拨的地址也自愈。
func (m *Manager) reapWhenDone(mc *managedConn) {
	<-mc.c.Done()
	m.mu.Lock()
	m.removeConnTablesLocked(mc)
	m.mu.Unlock()
}

// removeConnTablesLocked 从 conns/byAddr 清除连接表项（收尸/死分支/断连
// 三处同构清表的单一实现；byAddr 带身份守卫——替代连接
// 不得误逐）。byNode 归属调用点语义（connLost 带节点守卫清理；收尸与
// registerNode 死分支不涉——后者 close(ready) 语义在前者之外）。
// 调用方必须已持 mu。
func (m *Manager) removeConnTablesLocked(mc *managedConn) {
	delete(m.conns, mc)
	if mc.addr != "" {
		if cur, ok := m.byAddr[mc.addr]; ok && cur == mc {
			delete(m.byAddr, mc.addr)
		}
	}
}

// registerNode 握手完成登记：byNode 收敛裁决 + 替换策略（败者优雅退役，
// 其 Lost 因 superseded 标记不再上报、不误删新表项）+ 放行就绪等待者。
//
// 双向首接触收敛规则（决策出处：
// docs/decisions/2026-09-30-tcp-bidir-convergence.md）：两节点同时互拨时，
// 双侧 Manager 各出现
// 同节点双连接（己方出站 + 对方入站，各指一条 socket）。裁决必须双端独立
// 计算出**同一条存活 socket**，否则坏序下双侧 byNode 各指向对方已关的
// socket（相互断连 → 上层 FailPendingForNode 杀在途非幂等请求）。规则取
// 纯函数：**低号节点发起（dial）的连接存活**——self<peer 时存活者是本端
// 出站连接，self>peer 时存活者是本端入站连接，双端无需通信必然一致。
// 同向双连接（重拨交叠等）维持「新顶替旧」。败者一律 Retire（互斥封队 →
// 排空冲刷 → 半关）：顶替不得丢弃 Send 已返回成功（入队）的帧。
//
// 就绪与关闭有赛跑窗（Conn 先置 Ready 再回调本函数）：连接若在登记前
// 已死（shutdown 赢 closeOnce 时 ready 尚 false，Lost 被跳过、connLost
// 不会再来清表），不得登记——锁内复查 Done 并就地清 conns/byAddr
// （byAddr 身份守卫，同 connLost：替代连接不得误逐）。监听侧连接
// （addr=""）无 getOrConnect 死检查兜底，此处即该连接最后的清表点。
func (m *Manager) registerNode(mc *managedConn, peer rans.NodeID) {
	m.mu.Lock()
	mc.peer = peer // 票 02：先于 close(ready) 落位，SendToNode 就绪后可读
	select {
	case <-mc.c.Done():
		m.removeConnTablesLocked(mc)
		m.mu.Unlock()
		close(mc.ready) // 释放 SendToNode 等待者（其 Send 将得到终态错误）
		return
	default:
	}
	old, replacing := m.byNode[peer]
	keepNew := true
	if replacing && old.c.dialer != mc.c.dialer {
		// 异向双连接（双向首接触）：低号节点发起的连接存活——双端一致收敛。
		keepNew = mc.c.dialer == (m.cfg.NodeID < peer)
	}
	if keepNew {
		m.byNode[peer] = mc
	}
	m.mu.Unlock()
	if !keepNew {
		// 新连接是败者：不经 ready 放行（其等待者经 Done 解除后重试，经
		// byNode 落到存活连接）；先标 superseded（其终局 shutdown→connLost
		// 不误报节点断连）再优雅退役。
		mc.superseded.Store(true)
		mc.c.Retire()
		return
	}
	if replacing {
		old.superseded.Store(true) // 先标记后退役：其 Lost 上报被抑制
		old.c.Retire()
		// 发送侧保序缓解（票 06）：新连接的 ready 在旧连接写出全部在队帧
		// 之后才放行——新旧交接期的发送序不交错（收端两读循环的交错窗口
		// 由此显著缩小，FIFO 承诺收窄为「同一连接内」）。有界等待：超时
		// 照常放行（僵死连接不阻塞收敛）并告警留痕。
		select {
		case <-old.c.Drained():
		case <-time.After(drainWaitBudget):
			log.Warn("rans/tcp: new conn ready before old conn drained (bounded wait exhausted)",
				"peer", peer)
		}
	}
	close(mc.ready)
}

// connLost 连接断开终态：身份守卫清表（旧连接的 Lost 不得误删新连接的
// 表项——按 conn 指针比对），锁外上报节点级事件（handler.OnConnectionLost
// 是节点丢失的唯一观测面——Remote 据此失效缓存并快速失败在途请求）。
// superseded 连接（被替换关旧）不上报：节点仍有新连接在表。
func (m *Manager) connLost(mc *managedConn, peer rans.NodeID) {
	m.mu.Lock()
	m.removeConnTablesLocked(mc)
	if cur, ok := m.byNode[peer]; ok && cur == mc {
		delete(m.byNode, peer)
	}
	m.mu.Unlock()
	// 抑制两类非节点断连：本端收敛裁决的败者（superseded）+ 对端已通知退役
	//（peerRetired——对端必有替代连接，本副本死亡是收敛的正常收尾）。
	if mc.superseded.Load() || mc.c.peerRetired.Load() {
		return
	}
	m.handler.OnConnectionLost(peer)
}

// ConnToNode 按节点号查连接；未建立（或已断开被清表）返回 nil。
func (m *Manager) ConnToNode(node rans.NodeID) *Conn {
	m.mu.Lock()
	mc := m.byNode[node]
	m.mu.Unlock()
	if mc == nil {
		return nil
	}
	return mc.c
}

// SendToNode 面向节点的发送：byNode 命中直发（就绪等待点已关闭，即刻
// 通过）；未命中经 resolve 查址 → GetOrConnect → 等握手就绪（或连接
// 关闭/ctx 超时）→ 发送。最终 Send 尊重 ctx（出站满则阻塞至腾出空间或
// ctx 到期，连接关闭/退役即时失败）。body 所有权随成功返回移交连接。
func (m *Manager) SendToNode(ctx context.Context, node rans.NodeID, resolve func(ctx context.Context, n rans.NodeID) (string, error), hdr wire.Header, body []byte) error {
	m.mu.Lock()
	mc := m.byNode[node]
	if mc != nil {
		select {
		case <-mc.c.Done():
			// 陈旧条目：连接已死但 connLost 清表尚未调度到——就地剔除转拨号
			// 路径，不把死连接发放给发送方（必然失败的尝试 + 网关退避会白耗
			// 请求预算；收敛期的连接更替使该窗口常见）。
			delete(m.byNode, node)
			mc = nil
		default:
		}
	}
	m.mu.Unlock()
	if mc == nil {
		if resolve == nil {
			return fmt.Errorf("rans: connect: no connection to node %d and no resolver", node)
		}
		addr, err := resolve(ctx, node)
		if err != nil {
			return fmt.Errorf("rans: connect: resolve node %d: %w", node, err)
		}
		if mc, err = m.getOrConnect(ctx, addr); err != nil {
			return err
		}
	}
	// 就绪等待的 ctx 优先（票 05）：ctx 已结束时确定性返回 ctx 结果——
	// ready 与 ctx.Done 同时就绪的 select 随机选不再可能送出调用方已放弃
	// 的帧。
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-mc.ready:
	case <-mc.c.retired:
		// 败者连接（收敛裁决退役中、ready 永不关闭）：按退役语义即时失败，
		// 网关免退避重试经 byNode 落到存活连接——不等 Done（退役收尾最长
		// retireLinger，等它白耗请求预算）。
		return fmt.Errorf("rans: send: connection to node %d: %w", node, ErrRetiring)
	case <-mc.c.Done():
		if err := mc.c.terminalErr(); err != nil {
			return fmt.Errorf("rans: send: connection to node %d closed before ready: %w", node, err)
		}
		return fmt.Errorf("rans: send: connection to node %d closed before ready", node)
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err // 就绪后的复检：等待期间调用方已放弃
	}
	// 对端身份校验（票 02）：就绪连接的握手对端与目标不符 = 解析地址已被
	// 其他节点复用。帧不送出（确定未投递），连接留给真实对端复用；解析结
	// 果的失效由上层视图/缓存承接（票 01 事件失效）。
	if p := mc.peer; p != node {
		if mc.peerWarned.CompareAndSwap(false, true) {
			log.Warn("rans/tcp: resolved address belongs to a different node; frame not delivered",
				"want", node, "got", p, "addr", mc.addr)
		}
		return fmt.Errorf("rans: send: node %d resolved to node %d's address: %w", node, p, ErrWrongPeer)
	}
	return mc.c.Send(ctx, hdr, body)
}
