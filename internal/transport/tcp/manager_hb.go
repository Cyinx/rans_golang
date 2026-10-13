package tcp

import (
	"context"
	"time"

	"github.com/cyinx/rans_golang"
)

// 心跳默认参数（规格 §10.2/附录 A：10s ping、30s 无 pong 踢除）；
// 测试经 StartHeartbeat 注入短值覆盖。
const (
	defaultPingInterval = 10 * time.Second
	defaultPongTimeout  = 30 * time.Second
)

// StartHeartbeat 启动心跳环（幂等：重复调用忽略；已 Close 的管理器上
// 静默 no-op——Close 是终态）。每 interval 拍一次：对 Ready 连接检查
// now-LastPong，超 pongTimeout 未收到 Pong 即踢除（关闭连接 → 走 Lost
// 路径清表上报）；否则尽力发一拍 Ping。ctx 取消即停环（Manager.Close
// 亦停）。
func (m *Manager) StartHeartbeat(ctx context.Context, interval, pongTimeout time.Duration) {
	if interval <= 0 {
		interval = defaultPingInterval
	}
	if pongTimeout <= 0 {
		pongTimeout = defaultPongTimeout
	}
	m.mu.Lock()
	if m.hbCancel != nil || m.closed {
		m.mu.Unlock()
		return
	}
	hbCtx, cancel := context.WithCancel(ctx)
	m.hbCancel = cancel
	m.mu.Unlock()
	go m.heartbeatLoop(hbCtx, interval, pongTimeout)
}

// heartbeatLoop 心跳主环。快照在锁内、动作（关闭/发送）在锁外：Close →
// shutdown → connLost 需要重取 Manager 锁，持锁调用会死锁。
//
// 写路径对冲：Config 无写超时配置，写循环可能阻塞在
// TCP 发送窗口（对端不读）；pong 超时踢除（Close 掐断 net.Conn）即是
// 卡死写路径的对冲手段——对端停读时其 Pong 同时停更，心跳环据此关连接。
func (m *Manager) heartbeatLoop(ctx context.Context, interval, pongTimeout time.Duration) {
	tk := time.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
		now := time.Now()
		m.mu.Lock()
		var kick, ping []*managedConn
		for mc := range m.conns {
			if !mc.c.Ready() {
				continue // 规格 §10.2：心跳仅对 Ready 连接
			}
			if now.Sub(mc.c.LastPong()) > pongTimeout {
				kick = append(kick, mc)
			} else {
				ping = append(ping, mc)
			}
		}
		m.mu.Unlock()
		for _, mc := range kick {
			mc.c.Close() // 踢除：关连接 + 触发 Lost 清表上报
		}
		for _, mc := range ping {
			mc.c.pingBestEffort()
		}
	}
}

// SeverNode 主动断开与某节点的连接（testkit Partition 注入点）：
// 关闭该节点当前连接，走 Lost 路径清表并上报。未连接/未完成握手时
// 无操作。
func (m *Manager) SeverNode(peer rans.NodeID) {
	m.mu.Lock()
	mc := m.byNode[peer]
	m.mu.Unlock()
	if mc != nil {
		mc.c.Close()
	}
}

// Close 关闭 manager（终态，幂等）：先置 closed 旗标（此后拨号/监听/
// 心跳启动一律拒绝；在途拨号的登记临界区复验本旗标），再关 listener +
// 全部连接 + 心跳环。已就绪连接的关闭各自触发一次 NodeConnectionLost
// 上报（Conn.Close 同步执行 shutdown → connLost）。
func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		l := m.listener
		m.listener = nil
		conns := make([]*managedConn, 0, len(m.conns))
		for mc := range m.conns {
			conns = append(conns, mc)
		}
		cancel := m.hbCancel
		m.mu.Unlock()
		if l != nil {
			l.Close()
		}
		if cancel != nil {
			cancel()
		}
		for _, mc := range conns {
			mc.c.Close()
		}
	})
}
