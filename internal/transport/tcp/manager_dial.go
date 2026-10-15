package tcp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/cyinx/rans_golang/internal/log"
)

// acceptRetryMin/Max 临时 Accept 错误的重试退避边界：毫秒级起步、指数
// 增长封顶——资源耗尽窗口通常短暂，长退避反伤入站恢复；封顶防持续耗尽
// 时的日志洪水。成功接入即复位。
const (
	acceptRetryMin = time.Millisecond
	acceptRetryMax = 20 * time.Millisecond
)

// dialFlight 单飞拨号结果槽：done 关闭后 mc/err 可读（channel close 的
// happens-before 保证字段可见）。
type dialFlight struct {
	done chan struct{}
	mc   *managedConn
	err  error
}

// Listen 在 addr 上监听（":0" 由内核分配），返回实际绑定地址；accept
// 循环在后台 goroutine 运行，接入的连接按 listener 侧握手（等对端
// Handshake 后回 Ack）。每个 Manager 只允许一个 listener。Config.TLS
// 非 nil 时以 tls.NewListener 包装（陷阱 13：TLS 握手在 startConn 内、
// 包装与帧循环之前对裸连接显式完成）。
func (m *Manager) Listen(ctx context.Context, addr string) (string, error) {
	lc := net.ListenConfig{}
	l, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return "", fmt.Errorf("rans: listen: %s: %w", addr, err)
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		l.Close()
		return "", ErrManagerClosed
	}
	if m.listener != nil {
		m.mu.Unlock()
		l.Close()
		return "", errAlreadyListening
	}
	if m.cfg.TLS != nil {
		l = tls.NewListener(l, m.cfg.TLS)
	}
	m.listener = l
	m.mu.Unlock()
	go m.acceptLoop(l)
	return l.Addr().String(), nil
}

// ListenBind 只绑定不接入（票 14 的 Start 次序面）：返回实际地址，accept
// 循环经 StartAccept 起——绑定与接入之间的窗口内入站连接停留内核积压，
// StartAccept 后照常握手接入。绑定即计入单 listener 限额（与 Listen 互
// 斥）；Manager.Close 照常收口未接入的监听器。
func (m *Manager) ListenBind(ctx context.Context, addr string) (string, error) {
	lc := net.ListenConfig{}
	l, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return "", fmt.Errorf("rans: listen: %s: %w", addr, err)
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		l.Close()
		return "", ErrManagerClosed
	}
	if m.listener != nil {
		m.mu.Unlock()
		l.Close()
		return "", errAlreadyListening
	}
	if m.cfg.TLS != nil {
		l = tls.NewListener(l, m.cfg.TLS)
	}
	m.listener = l
	m.mu.Unlock()
	return l.Addr().String(), nil
}

// StartAccept 起接入循环（ListenBind 的后半；票 14：同步首写成员键之后
// 才开始接入）。无监听器或已起接入为 no-op（幂等）。
func (m *Manager) StartAccept() {
	m.mu.Lock()
	l := m.listener
	m.mu.Unlock()
	if l == nil {
		return
	}
	m.acceptStart.Do(func() { go m.acceptLoop(l) })
}

// acceptLoop 后台接入循环：Accept 错误三分——监听器已关闭（Manager.Close
// 的常规收尾）静默终止；临时类（net.Error.Temporary 语义，EMFILE/ENFILE
// 等资源耗尽形态）退避重试 + Warn 留痕——一次 fd 耗尽不得让入站永久失聪；
// 其余持久错误终止 + Warn 留痕。接入失败不致命（单条连接失败不影响后续
// Accept）；TLS 握手失败（明文探测等）连接已关、静默丢弃——不回错帧（静默
// 错位门闸），继续接后续连接。逐连接 goroutine 处理（决策出处：
// docs/decisions/2026-09-30-phase6-security.md）：startConn 内联 TLS 握手（阻塞
// 至 HandshakeTimeout），同步处理会让慢握手/僵死探测串行限流 accept 吞吐
// （slowloris 形态）；处理 goroutine 生命周期有界——握手超时与 hsTimer
// 都以 HandshakeTimeout 封顶。
func (m *Manager) acceptLoop(l net.Listener) {
	backoff := acceptRetryMin
	for {
		nc, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if isTemporaryAcceptErr(err) {
				log.Warn("rans/tcp: temporary accept error, retrying",
					"node", m.cfg.NodeID, "reason", err)
				time.Sleep(backoff)
				backoff = min(backoff*2, acceptRetryMax)
				continue
			}
			log.Warn("rans/tcp: accept loop terminated",
				"node", m.cfg.NodeID, "reason", err)
			return
		}
		backoff = acceptRetryMin
		go func(nc net.Conn) {
			// 失败路径 startConn 内已关连接（握手失败/包装前拒绝），
			// 此处无须再处置；Close 兜底一切残余。
			_, _ = m.startConn(nc, "", false)
		}(nc)
	}
}

// isTemporaryAcceptErr 报告 Accept 错误是否临时类（net.Error.Temporary
// 语义——EMFILE/ENFILE 等资源耗尽可自愈形态）；关闭类错误已在上游分流。
func isTemporaryAcceptErr(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Temporary()
}

// GetOrConnect 按地址懒建连接（单飞：同地址并发只拨一次，后来者共享
// 同一结果；失败结果不缓存，下次重拨）。返回的连接可能尚未完成握手
// ——需要就绪保证的发送路径用 SendToNode。
func (m *Manager) GetOrConnect(ctx context.Context, addr string) (*Conn, error) {
	mc, err := m.getOrConnect(ctx, addr)
	if err != nil {
		return nil, err
	}
	return mc.c, nil
}

// getOrConnect 内部形态：返回 managedConn（SendToNode 需要就绪等待点）。
func (m *Manager) getOrConnect(ctx context.Context, addr string) (*managedConn, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrManagerClosed
	}
	if mc, ok := m.byAddr[addr]; ok {
		select {
		case <-mc.c.Done():
			// 缓存连接已死：剔除重建。Conn 只对就绪连接报 Lost，握手失败的
			// 半开连接死在这里只能由本检查兜底（失败/死亡结果不缓存）。
			delete(m.byAddr, addr)
			delete(m.conns, mc)
		case <-mc.c.retired:
			// 缓存连接退役中（收敛裁决败者）：同死处理，重拨建立新连接——
			// 发放退役连接只会得到 ErrRetiring 的重试空转。
			delete(m.byAddr, addr)
			delete(m.conns, mc)
		default:
			m.mu.Unlock()
			return mc, nil
		}
	}
	if f, ok := m.dialing[addr]; ok {
		m.mu.Unlock()
		select { // 后来者搭车：等单飞结果或调用方超时
		case <-f.done:
			return f.mc, f.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f := &dialFlight{done: make(chan struct{})}
	m.dialing[addr] = f
	// 重拨观察：该地址曾成功建连（hadConn）→ 本次为重拨，
	// 锁外触发观察钩子（rans.transport.reconnects_total 的 +1 点）。
	// 计数时点在拨号发起（attempt 口径）：失败的尝试同样算一次重拨尝试。
	if _, redial := m.hadConn[addr]; redial {
		hook := m.cfg.OnReconnect
		m.mu.Unlock()
		if hook != nil {
			hook()
		}
	} else {
		m.mu.Unlock()
	}

	mc, err := m.dialConn(ctx, addr)

	m.mu.Lock()
	delete(m.dialing, addr)
	if err != nil {
		m.mu.Unlock()
		f.err = err // 失败不进 byAddr：不缓存失败结果
		close(f.done)
		return nil, err
	}
	m.byAddr[addr] = mc
	m.hadConn[addr] = struct{}{} // 建连成功即入集：此后该地址任何再拨都是重拨
	m.mu.Unlock()
	f.mc = mc
	close(f.done)
	return mc, nil
}

// dialConn 拨号并启动连接（单飞leader 路径）；连接登记进 conns 表。
func (m *Manager) dialConn(ctx context.Context, addr string) (*managedConn, error) {
	// fail-closed 门闸前置于拨号与 TLS 握手：本地纯判定，
	// 误配形态零字节拒绝——TCP/TLS 握手字节都不发出，错误同步直达调用方
	// （Conn.Start 内的门闸为裸 NewConn 用户保留，Manager 路径在此前置）。
	if m.cfg.failClosedHMAC() {
		return nil, errHMACRequired
	}
	var nc net.Conn
	var err error
	if m.cfg.Dial != nil {
		nc, err = m.cfg.Dial(ctx, addr)
	} else {
		var d net.Dialer
		nc, err = d.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return nil, err
	}
	if m.cfg.TLS != nil {
		// 陷阱 13：拨号侧在 NewConn 前显式完成 TLS 握手——失败即关，
		// 拨号报错，帧循环从未启动
		nc, err = dialTLS(ctx, nc, m.cfg.TLS, m.cfg.HandshakeTimeout)
		if err != nil {
			return nil, err
		}
	}
	mc, err := m.startConn(nc, addr, true)
	if err != nil {
		return nil, err
	}
	// 到手即死的连接（fail-closed 门闸等本地拒绝在 Start 内同步触发）：
	// 拨号即刻带因失败，死连接不进 byAddr 缓存。
	if err := mc.c.terminalErr(); err != nil {
		return nil, err
	}
	return mc, nil
}

// startConn TLS 显式握手（裸连接上，先于包装——次序见函数体）→ 包装
// 裸连接（cfg.WrapConn 注入点）→ 构造 Conn → 登记 conns（挂终态收尸）→
// Start。addr 为 ""
// 表示 listener 接入侧。握手失败：连接关闭并返回错误（帧循环不启动，明文
// 字节不进 FrameDecoder）；拨号侧 dialTLS 已先行握手，此处幂等即刻返回。
func (m *Manager) startConn(nc net.Conn, addr string, dialer bool) (*managedConn, error) {
	// fail-closed 门闸前置于 TLS 握手（Conn.Start 内的门闸为
	// 裸 NewConn 用户保留，此处幂等前置）：本地纯判定零字节拒绝，不把
	// 连接按到握手超时。接入侧必须留痕——本地配置事故与静默错位门闸
	// 不同（同 Conn.Start 门闸的 shutdown Warn 纪律）。
	if m.cfg.failClosedHMAC() {
		log.Warn("rans/tcp: connection rejected",
			"node", m.cfg.NodeID, "remote", nc.RemoteAddr().String(), "reason", errHMACRequired)
		nc.Close()
		return nil, errHMACRequired
	}
	if m.cfg.TLS != nil {
		// 握手必须先于包装：包装连接嵌
		// net.Conn 接口，HandshakeContext 不随接口嵌入提升——Conn.Start 的
		// 握手探测对包装后连接失效（退化为惰性握手，认证先于帧解码虽仍由
		// tls.Conn 语义守住，但「Start 前显式完成握手」的结构性保证只能
		// 在裸连接上落实）。包装层（延迟注入等）从此只见 TLS 记录流，
		// 延迟注入不影响握手本身。
		if err := ensureHandshake(nc, m.cfg.HandshakeTimeout); err != nil {
			// 接入侧握手失败留痕：明文探测 TLS 端口/错证书
			// 必须可观测——远端与原因进日志（静默错位门闸
			// 辖「不回错帧」，不辖本地日志）。
			log.Warn("rans/tcp: tls handshake failed on accept",
				"node", m.cfg.NodeID, "remote", nc.RemoteAddr().String(), "reason", err)
			nc.Close()
			return nil, err
		}
	}
	if m.cfg.WrapConn != nil {
		nc = m.cfg.WrapConn(nc)
	}
	mc := &managedConn{addr: addr, ready: make(chan struct{})}
	mc.c = NewConn(nc, m.cfg, &connHandler{m: m, mc: mc, user: m.handler}, m.pool, dialer)
	// 写批观察钩子：在 Start 起写 goroutine 前回接 Manager 的
	// 批量记录面（happens-before 由 Start 建立 goroutine 保证，之后只读）。
	mc.c.onFlush = m.recordBatch
	// 登记临界区 closed 复验：Close 的 conns
	// 快照若先于本登记执行，此处是唯一拦截点——当场关闭连接并使拨号带因
	// 失败，不留「永不收口、无心跳」的孤儿连接。
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		nc.Close()
		return nil, ErrManagerClosed
	}
	m.conns[mc] = struct{}{}
	m.mu.Unlock()
	go m.reapWhenDone(mc) // 先于 Start：任何同步死亡（fail-closed 拒绝）即被收尸
	mc.c.Start()
	return mc, nil
}
