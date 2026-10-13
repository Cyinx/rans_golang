package testkit

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
)

// Partition 双向分区节点 a、b：屏蔽对向拨号并关断两方向既有连接。后续两节点
// 间调用在发送侧重试耗尽或请求超时后失败；其他节点不受影响（被误伤的连接
// 会在下次调用时按需重拨——屏蔽表只拦 a↔b）。
func (c *Cluster) Partition(a, b int) {
	c.t.Helper()
	pa, pb := c.nodeID(a), c.nodeID(b)
	c.setBlocked(pa, pb, true)
	c.setBlocked(pb, pa, true)
	for _, pair := range [][2]rans.NodeID{{pa, pb}, {pb, pa}} {
		for _, nc := range c.takeTracked(pair[0], pair[1]) {
			nc.Close()
		}
	}
}

// Heal 解除节点 a、b 之间的双向分区（屏蔽表移除；连接按需重拨重建）。
func (c *Cluster) Heal(a, b int) {
	c.t.Helper()
	pa, pb := c.nodeID(a), c.nodeID(b)
	c.setBlocked(pa, pb, false)
	c.setBlocked(pb, pa, false)
}

// Slow 给节点 i 的全部连接注入每帧读写延迟 d（对已建立连接即时生效——
// 包装连接动态读当前值）。d ≤ 0 等效 HealSlow。
func (c *Cluster) Slow(i int, d time.Duration) {
	c.t.Helper()
	c.nodeAt(i).latency.Store(int64(d))
}

// HealSlow 移除节点 i 的延迟注入。
func (c *Cluster) HealSlow(i int) {
	c.t.Helper()
	c.nodeAt(i).latency.Store(0)
}

// ---- 内部：注入点装配 ----

// nodeAt 索引（0 基）→ clusterNode。
func (c *Cluster) nodeAt(i int) *clusterNode {
	if i < 0 || i >= len(c.nodes) {
		c.t.Fatalf("testkit: node index %d out of range (n=%d)", i, len(c.nodes))
	}
	return c.nodes[i]
}

// nodeID 索引（0 基）→ 节点号。
func (c *Cluster) nodeID(i int) rans.NodeID {
	return c.nodeAt(i).node.ID
}

// dialFor 节点拨号钩子：查本节点屏蔽表拦截分区对向拨号；成功拨出的原生连接
// 按对端登记（Partition 的关断把手——连接管理器不公开，经此注入点持有可从
// 外部 Close 的连接引用）。
func (c *Cluster) dialFor(self rans.NodeID) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		peer, known := c.peerByAddr(addr)
		if known && c.isBlocked(self, peer) {
			return nil, fmt.Errorf("testkit: node %d → node %d blocked by partition", self, peer)
		}
		var d net.Dialer
		nc, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		if known {
			c.trackConn(self, peer, nc)
		}
		return nc, nil
	}
}

// wrapForNode 节点连接包装钩子：一切 Accept/Dial 出的连接都包成 slowConn
// （延迟动态读节点当前值，0 时零开销透传）。捕获稳定实例指针而非集群切片
// 索引——acceptLoop 的包装可与 NewCluster 的 append 并发执行。
func wrapForNode(cn *clusterNode) func(net.Conn) net.Conn {
	return func(nc net.Conn) net.Conn {
		return &slowConn{Conn: nc, delay: &cn.latency}
	}
}

// peerByAddr 地址反查节点号（锁内读：拨号钩子可能运行在与建表写入无既定
// happens-before 的后台 goroutine 上）。
func (c *Cluster) peerByAddr(addr string) (rans.NodeID, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id, ok := c.byAddr[addr]
	return id, ok
}

// isBlocked 节点 self 是否分区屏蔽了对端 peer。
func (c *Cluster) isBlocked(self, peer rans.NodeID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.blocked[self][peer]
}

// setBlocked 增删屏蔽表项。
func (c *Cluster) setBlocked(self, peer rans.NodeID, blocked bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if blocked {
		if c.blocked[self] == nil {
			c.blocked[self] = make(map[rans.NodeID]bool)
		}
		c.blocked[self][peer] = true
	} else if m := c.blocked[self]; m != nil {
		delete(m, peer)
	}
}

// trackConn 登记拨号侧连接（供 Partition 关断）。
func (c *Cluster) trackConn(self, peer rans.NodeID, nc net.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tracked[self] == nil {
		c.tracked[self] = make(map[rans.NodeID][]net.Conn)
	}
	c.tracked[self][peer] = append(c.tracked[self][peer], nc)
}

// takeTracked 摘除并返回节点 self 拨向 peer 的全部连接（关断后不再复用；
// 若屏蔽解除后需要重建，按需重拨即可）。
func (c *Cluster) takeTracked(self, peer rans.NodeID) []net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	conns := c.tracked[self][peer]
	delete(c.tracked[self], peer)
	return conns
}

// slowConn 延迟注入连接：每次 Read/Write 前按当前延迟值睡（Slow/HealSlow
// 对已建立连接即时生效）。其余 net.Conn 语义透传。
type slowConn struct {
	net.Conn
	delay *atomic.Int64
}

// Unwrap 剥开包装（stdlib tls.Conn 同款约定）：传输层控制面操作（退役
// 半关/读期限）经它落在真实 socket 上——包装层只辖数据面延迟注入。
func (s *slowConn) Unwrap() net.Conn { return s.Conn }

func (s *slowConn) Read(p []byte) (int, error) {
	if d := s.delay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	return s.Conn.Read(p)
}

func (s *slowConn) Write(p []byte) (int, error) {
	if d := s.delay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	return s.Conn.Write(p)
}
