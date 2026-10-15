// Package tcp 实现传输端口（transport.Conn）的网络适配器：帧编解码、
// 握手编排（规格 §10.2）、pre-auth 门禁与有界出站队列背压（规格 §10.1）。
// 规格中的 IOQueue 调度隔离在 Go 侧简化为读写专用 goroutine（Go 调度器
// 即 PipeScheduler）。
package tcp

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/transport"
	"github.com/cyinx/rans_golang/internal/wire"
)

// 连接层默认参数：数据出站有界 chan 1024（规格 §10.1）；控制面出站优先
// 通道 64（HR4 决策 10：Ping/Pong/Retire 与未来的接管通知不被数据背压
// 饿死）；握手超时 5s。
const (
	defaultSendQueueDepth   = 1024
	controlQueueDepth       = 64
	defaultHandshakeTimeout = 5 * time.Second
	writeBufSize            = 64 << 10 // bufio 写缓冲 64KiB
)

// ErrClosed 连接已关闭（Send 的终态错误外壳，包装实际原因，errors.Is
// 可同时命中外壳与原因）。
var ErrClosed = errors.New("rans: conn: closed")

// ErrRetiring 连接退役中（被同节点新连接确定性顶替，见 Manager.registerNode
// 的收敛规则与 Conn.Retire 的退役协议）。Send 立即失败促调用方重试——重试
// 经 byNode 落到存活连接，非网络恢复语义，网关对其免退避即时重试。
var ErrRetiring = errors.New("rans: conn: retiring (superseded)")

// 内部关闭原因（不导出：静默错位门闸下对端只能观察断连，原因是本地日志事）。
var (
	errPreAuthApp         = errors.New("application message before handshake")
	errDuplicateHandshake = errors.New("duplicate handshake after ready")
	errHandshakeTimeout   = errors.New("handshake timeout")
	errRoleViolation      = errors.New("handshake message inconsistent with connection role")
)

// outFrame 出站帧：hdr 为值拷贝（Encode 在写循环做）；body 所有权已在
// 入队时移交连接，写出后由写循环归还页池。
type outFrame struct {
	hdr  wire.Header
	body []byte
}

// Conn 单条传输连接：读循环（帧解码 → pre-auth 门禁 → 分发）+ 写循环
// （有界出站 chan → bufio 批量写）。并发安全：状态全部为原子量与 channel，
// 无互斥锁——回调（handler 三方法）均在锁外调用（§19 陷阱 2）。
type Conn struct {
	nc      net.Conn
	cfg     Config
	handler transport.Handler
	pool    *wire.PagePool
	out     chan outFrame // 数据出站队列（背压第一级，规格 §10.1）
	ctl     chan outFrame // 控制面出站优先通道（容量 64，先于数据写出）
	dialer  bool

	// onFlush 每次写冲刷（队列清空）后以本批帧数调用（观测面：
	// rans.transport.batch_size 数据源；Manager 在 Start 前回接，写循环
	// 只读）。nil = 未接线（裸 NewConn 用户）。nil 判断在写循环热路径上，
	// 无锁无原子开销。
	onFlush func(n int)

	ready        atomic.Bool
	peer         atomic.Uint32
	lastPong     atomic.Int64 // unix nano：Start 时初始化、收到 Pong 更新
	peerRetired  atomic.Bool  // 对端已发 MsgRetire：本副本的死亡按抑制语义收场
	draining     atomic.Bool  // 退役收尾期（读期限按读静默续期，见 retireFinish）
	drainRetried atomic.Bool  // 退役读期限超时的续期重读已用（仅一次，防静默 socket 空转）

	closed    chan struct{}
	closeOnce sync.Once
	sendErr   error // 仅 closeOnce 获胜方在 close(closed) 前写入，读方经 closed 收讯后可见
	hsTimer   *time.Timer

	// 优雅退役面（同节点顶替的败者收尾，见 Retire）：
	// retiredFlag 在 retireMu 内先于 close(retired) 设置——Send 的互斥入队
	// 路径与 enqueue 的退役分支据此拒绝新帧，此后「出站队列空」即真正终态
	//（writeLoop 的退役排空不依赖任何计时宽限）。retireDrained 由 writeLoop
	// 在任何退出路径关闭（defer）：退役中的读循环收到对端 FIN/错误时先等它
	// 再关 socket——否则 closed 与 retired 两信号在 writeLoop 竞速，先选中
	// closed 分支退出会经 drainOut 丢弃已入队帧（对端 FIN 早于本端排空完成
	// 的交错即零丢失破口）。retireLinger 是
	// 退役半关后等对端 EOF 的兜底上限（对端僵死时全关，防读侧悬挂）。
	retiredFlag   bool // retireMu 保护
	retired       chan struct{}
	retireDrained chan struct{} // writeLoop defer 关闭：已入队帧处置完毕
	retireOnce    sync.Once
	retireMu      sync.Mutex
}

// 接口静态断言：*Conn 即传输端口。
var _ transport.Conn = (*Conn)(nil)

// Drained 返回退役排空信号（writeLoop 退出即关闭——已入队帧已全部写出
// 或处置完毕；票 06 连接收敛的发送侧保序等待面）。未退役的连接随其终态
// 关闭。
func (c *Conn) Drained() <-chan struct{} { return c.retireDrained }

// NewConn 构造连接。Start 前不启动任何 goroutine；dialer=true 时
// Start 先发 Handshake 并等待 HandshakeAck，false 时等待对端 Handshake
// 后回 HandshakeAck。pool 为 nil 时内部自建（防御：nil 池会让读路径
// 解码即 panic）。
func NewConn(nc net.Conn, cfg Config, h transport.Handler, pool *wire.PagePool, dialer bool) *Conn {
	cfg = cfg.normalize()
	if pool == nil {
		pool = wire.NewPagePool()
	}
	return &Conn{
		nc:            nc,
		cfg:           cfg,
		handler:       h,
		pool:          pool,
		out:           make(chan outFrame, cfg.SendQueueDepth),
		ctl:           make(chan outFrame, controlQueueDepth),
		dialer:        dialer,
		closed:        make(chan struct{}),
		retired:       make(chan struct{}),
		retireDrained: make(chan struct{}),
	}
}

// Start 启动读写双 goroutine；dialer 侧先发 Handshake。握手超时守护：
// HandshakeTimeout 内未就绪即关闭（半开连接不进上层连接表）。
// fail-closed 门闸（规格 §10.2）先行：未配密钥且未
// 显式 NoHMAC → 本地配置事故，即刻关闭不发任何握手字节——与静默错位门闸
// 不同（那是防探测面），此处错误必须留痕（shutdown 对非 EOF 记 Warn）。
// TLS 连接（陷阱 13：认证先于管道 IO）先显式完成 TLS 握手——失败静默
// 关闭（不回错帧，对端只观察断连），读写 goroutine 不启动，明文字节
// 不进 FrameDecoder。Manager 路径已在包装之前握手（见 startConn），
// 此处对裸 NewConn 用户保留、对已完成握手幂等；包装连接嵌 net.Conn 接口
// （HandshakeContext 不随接口嵌入提升），探测对其零操作——包装路径的
// 握手保证由 Manager 在包装之前落实。须在双 goroutine 就位后才发握手
// ——net.Pipe 等同步管道上，对端的读循环必须已在收帧，否则握手写会与
// 对端握手写互等成死锁。
func (c *Conn) Start() {
	if c.cfg.failClosedHMAC() {
		c.shutdown(errHMACRequired)
		return
	}
	if err := ensureHandshake(c.nc, c.cfg.HandshakeTimeout); err != nil {
		c.shutdown(err)
		return
	}
	c.lastPong.Store(time.Now().UnixNano())
	c.hsTimer = time.AfterFunc(c.cfg.HandshakeTimeout, func() {
		if !c.ready.Load() {
			c.shutdown(errHandshakeTimeout)
		}
	})
	go c.readLoop()
	go c.writeLoop()
	if c.dialer {
		c.sendHandshake(wire.Header{Type: protocol.MsgHandshake})
	}
}

// readLoop 入站主循环：帧解码 → 头解码 → 按就绪状态分发。
// 读失败（含对端关闭的 io.EOF，帧解码器契约中的干净收尾）统一走 shutdown。
// 退役收尾期（draining 置位后）：每读到一帧即刷新读期限——对端仍在冲刷
// 已入队帧时不因绝对定时器到点而带未读数据强关（RST 会静默销毁对端已
// 冲刷、本端未读的帧）；连续静默满 retireLinger 才收口。
func (c *Conn) readLoop() {
	dec := wire.NewFrameDecoder(c.nc, c.pool)
	for {
		rawHdr, body, err := dec.Next()
		if err != nil {
			if c.drainDeadlineRetry(err) {
				continue
			}
			// 退役中：先等 writeLoop 把已入队帧处置完（retireDrained）再关
			// socket——shutdown 会关 nc，抢在排空前即堵死写出面，closed 分支
			// 退出经 drainOut 丢帧。非退役路径直通（无等待必要）。
			select {
			case <-c.retired:
				<-c.retireDrained
			default:
			}
			c.shutdown(err)
			return
		}
		c.refreshRetireReadDeadline()
		hdr, err := wire.DecodeHeader(rawHdr)
		if err != nil {
			c.pool.Put(body) // 解码失败时页仍在手：先归还再关闭
			c.shutdown(err)
			return
		}
		if c.ready.Load() {
			c.dispatchPostAuth(hdr, body)
		} else {
			c.dispatchPreAuth(hdr, body)
		}
		select {
		case <-c.closed:
			return
		default:
		}
	}
}

// dispatchPreAuth 握手完成前的入站分发（规格 §10.2 pre-auth 门禁）：
// 仅 Handshake/HandshakeAck/Ping/Pong 可过；其余类型静默断连且**不调
// Handler**——应用消息不得进入消息处理路径（陷阱 10/13：不回错帧、
// 不给对端探测面）。body 页所有权在本函数内终结（归还）。
func (c *Conn) dispatchPreAuth(hdr wire.Header, body []byte) {
	switch hdr.Type {
	case protocol.MsgHandshake:
		if c.dialer {
			c.pool.Put(body)
			c.shutdown(errRoleViolation) // dialer 应等 Ack，收到 Handshake 即协议错位
			return
		}
		c.finishHandshake(body, true)
	case protocol.MsgHandshakeAck:
		if !c.dialer {
			c.pool.Put(body)
			c.shutdown(errRoleViolation) // listener 应先收 Handshake
			return
		}
		c.finishHandshake(body, false)
	case protocol.MsgPing:
		c.pool.Put(body)
		c.replyPong(hdr.MessageID)
	case protocol.MsgPong:
		c.pool.Put(body)
		c.lastPong.Store(time.Now().UnixNano())
	default:
		// pre-auth 应用消息：断连不回错，绝不进入消息处理路径
		c.pool.Put(body)
		c.shutdown(errPreAuthApp)
	}
}

// dispatchPostAuth 就绪后的入站分发：应用消息移交 handler（body 页所有权
// 随之转移，handler 用完归还）；Ping 自动回 Pong（不出包到 Handler）；
// Pong 刷新存活时钟；Handshake/Ack 重复到达视为协议错位，关闭（陷阱 13）。
func (c *Conn) dispatchPostAuth(hdr wire.Header, body []byte) {
	switch hdr.Type {
	case protocol.MsgPing:
		c.pool.Put(body)
		c.replyPong(hdr.MessageID)
	case protocol.MsgPong:
		c.pool.Put(body)
		c.lastPong.Store(time.Now().UnixNano())
	case protocol.MsgRetire:
		c.pool.Put(body)
		// 对端已收敛退役（必有替代连接）：标记死亡抑制 + 本端同 socket 副本
		// 一并退役——该 socket 双端都要关，本端先把手头已入队帧排空冲刷再收
		// 口（否则对端 FIN 早到时，readLoop 直通 shutdown 会经 nc.Close 杀死
		// writeLoop，临时存活者已入队帧经 drainOut 丢弃）。
		c.peerRetired.Store(true)
		c.Retire()
	case protocol.MsgHandshake, protocol.MsgHandshakeAck:
		c.pool.Put(body)
		c.shutdown(errDuplicateHandshake)
	default:
		c.handler.HandleMessage(hdr, body) // 页所有权移交 handler
	}
}

// finishHandshake 入站握手终态：解码（版本区间门禁在其中）→ 校验（集群名→
// MAC→摘要，规格 §10.2 v3 校验序）→ 摘要观测（失配告警+计数；StrictSchema
// 已在 Validate 拒）→ replyAck 时回 HandshakeAck → 置 Ready 并回调
// OnHandshakeComplete。任一步失败静默关闭：不回错帧，对端只观察到断连
// （§10.2 静默错位门闸）。
func (c *Conn) finishHandshake(body []byte, replyAck bool) {
	hs, err := wire.DecodeHandshake(body)
	c.pool.Put(body) // 解码产物自持拷贝（MAC/认证前缀），页可归还
	if err != nil {
		c.shutdown(err)
		return
	}
	if err := hs.Validate(c.cfg.ClusterName, c.cfg.SchemaDigest, c.cfg.Secret, c.cfg.StrictSchema); err != nil {
		c.shutdown(err)
		return
	}
	if hs.SchemaDigest != c.cfg.SchemaDigest {
		// 摘要失配观测面（H9：指纹降级为观测）：默认连接保持，告警+计数
		// 各一次；StrictSchema 形态到不了这里（Validate 已拒）。
		if c.cfg.OnSchemaMismatch != nil {
			c.cfg.OnSchemaMismatch()
		}
		log.Warn("rans/tcp: handshake schema digest mismatch (observation mode)",
			"node", c.cfg.NodeID, "peer", hs.NodeID, "remote", c.RemoteAddr(),
			"local", c.cfg.SchemaDigest, "peer", hs.SchemaDigest)
	}
	if c.cfg.Secret == nil {
		// NoHMAC 显式 opt-in 的不安全形态：MAC 未启用，逐握手
		// 完成告警（能走到这里且 Secret nil，必然已过 Start 的 fail-closed
		// 门闸，即 NoHMAC=true）。
		log.Warn("rans/tcp: insecure: HMAC disabled",
			"node", c.cfg.NodeID, "peer", hs.NodeID, "remote", c.RemoteAddr())
	}
	if replyAck {
		c.sendHandshake(wire.Header{Type: protocol.MsgHandshakeAck})
	}
	c.peer.Store(uint32(hs.NodeID))
	c.ready.Store(true) // 先 Ready 后停表：此后超时守护必见就绪态，不会误杀
	if c.hsTimer != nil {
		c.hsTimer.Stop()
	}
	c.handler.OnHandshakeComplete(rans.NodeID(hs.NodeID))
}

// sendHandshake 构造并发出本端握手体（Handshake 与 Ack 同构复用）。
// 握手体从页池取页拷入（writeFrame 对一切非 nil body 无条件 Put 归还——
// 裸 make 会给池白记一枚 Put，稀释 Gets==Puts 漏还检测锚点）。
func (c *Conn) sendHandshake(hdr wire.Header) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		c.shutdown(fmt.Errorf("generate handshake nonce: %w", err))
		return
	}
	hs := wire.Handshake{
		NodeID:       uint16(c.cfg.NodeID),
		ClusterName:  c.cfg.ClusterName,
		SchemaDigest: c.cfg.SchemaDigest,
		Nonce:        nonce,
	}
	raw := hs.Encode(c.cfg.Secret)
	page := c.pool.Get(len(raw))
	copy(page, raw)
	c.enqueue(outFrame{hdr: hdr, body: page})
}

// replyPong 心跳应答：拷贝 Ping 的 MessageID 便于关联；空载 body 不占池。
// pre-auth 阶段即生效（规格 §10.2：握手前 Ping/Pong 可用）。
func (c *Conn) replyPong(msgID uint64) {
	c.enqueue(outFrame{hdr: wire.Header{Type: protocol.MsgPong, MessageID: msgID}})
}

// Ready 握手是否已完成。
func (c *Conn) Ready() bool { return c.ready.Load() }

// PeerNodeID 对端节点号；ok 与 Ready 同义（peer 先于 ready 写入）。
func (c *Conn) PeerNodeID() (rans.NodeID, bool) {
	if !c.ready.Load() {
		return 0, false
	}
	return rans.NodeID(c.peer.Load()), true
}

// Close 关闭连接；幂等，重复调用返回 nil。
func (c *Conn) Close() error {
	c.shutdown(nil)
	return nil
}

// Done 连接关闭信号（close 即已关闭）。
func (c *Conn) Done() <-chan struct{} { return c.closed }

// terminalErr 连接终态错误（Done 关闭后可安全读取——sendErr 先于
// close(closed) 写入，channel close 的 happens-before 保证可见）。Manager
// 拨号路径借此把拒连原因（如 fail-closed 的 HMAC required）带给调用方。
func (c *Conn) terminalErr() error {
	select {
	case <-c.closed:
		return c.sendErr
	default:
		return nil
	}
}

// RemoteAddr 对端地址描述（日志与上层排障用）。
func (c *Conn) RemoteAddr() string { return c.nc.RemoteAddr().String() }

// LastPong 最近一次收到 Pong 的时刻（心跳判活读时钟，连接管理器消费）；
// 初始化为 Start 时刻，首拍心跳前不会误判超时。
func (c *Conn) LastPong() time.Time { return time.Unix(0, c.lastPong.Load()) }
