package tcp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/cyinx/rans_golang/internal/log"
)

// retireLinger 退役半关（CloseWrite）后等待对端 EOF 的兜底上限：对端同侧
// 退役会紧随其后 FIN，正常路径毫秒级收口；对端僵死（崩溃/不合作）时到点
// 全关，读侧滞留不悬挂。
const retireLinger = 2 * time.Second

// Retire 优雅退役（幂等）：Manager 的同节点收敛裁决对败者的收尾入口。
// 协议三段：
//  1. 互斥封队：retireMu 内置 retiredFlag 并 close(retired)——此后 Send
//     的互斥入队路径必见标志而拒绝、enqueue 的退役分支必丢弃，「出站队列
//     空」从此成为真正终态（无需计时宽限即无丢帧竞窗）；
//  2. writeLoop 退役排空：已入队帧全部写出并冲刷（Send 已返回成功的帧
//     必达——这是零丢失的承重点）；
//  3. retireFinish 半关收尾：CloseWrite 发 FIN 保出站数据在对端必达
//     （全关时己侧收缓冲未读数据会触发 RST，殃及对端收缓冲中己方刚冲刷
//     的数据）；读循环继续读到对端 EOF（对端同侧退役也 FIN）后正常
//     shutdown 全关，retireLinger 兜底对端僵死。
//
// 与 Close 的分工：Close 是立即终态（读侧不再消费）；Retire 是零丢失终态
// （出站必达、入站读到 EOF 为止）。Lost 抑制（superseded 标记）归 Manager。
func (c *Conn) Retire() {
	c.retireOnce.Do(func() {
		c.retireMu.Lock()
		c.retiredFlag = true
		c.retireMu.Unlock()
		close(c.retired)
	})
}

// retireFinish writeLoop 退役排空后的收尾（在其 goroutine 上调用）：
// 半关（CloseWrite→FIN，经 unwrapNetConn 落在真实 socket——包装层只辖
// 数据面，不转发可选接口）保出站数据必达；读侧转入「按读静默判定」的
// 收口（refreshRetireReadDeadline + draining 旗标）：每读到一帧续期，
// 连续静默满 retireLinger（对端僵死/不再有残余帧）才关——绝对定时器到点
// 强关会在收缓冲仍有对端已冲刷未读帧时发 RST，静默销毁它们（零丢失的
// 反面）。读期限无需能力探测：SetReadDeadline 是 net.Conn 接口本身的
// 方法，任何连接形态（含包装层）必然实现——实现为 no-op 的病态包装层
// 丢失的只是收口上界而非数据（读循环持续消费，对端 FIN/心跳踢除兜底），
// 定时器回退形态因此不可达（决策出处：
// docs/decisions/2026-09-30-tcp-bidir-convergence.md）。
func (c *Conn) retireFinish() {
	raw := unwrapNetConn(c.nc)
	if cw, ok := raw.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite() // 半关（FIN）保出站数据必达；net.Pipe 等无半关形态跳过
	}
	_ = raw.SetReadDeadline(time.Now().Add(retireLinger))
	c.draining.Store(true)
}

// setRetireReadDeadline 设置退役读期限（剥开包装层落在真实 socket 上）。
func (c *Conn) setRetireReadDeadline(t time.Time) {
	unwrapNetConn(c.nc).SetReadDeadline(t)
}

// unwrapNetConn 剥开包装连接（stdlib tls.Conn 的 Unwrap 约定，Go 1.20+）：
// 控制面操作（半关/读期限）必须落在真实 socket 上——包装层只辖数据面
// （延迟注入/观测），不必转发可选接口。逐层剥到无 Unwrap 为止。
func unwrapNetConn(nc net.Conn) net.Conn {
	for {
		u, ok := nc.(interface{ Unwrap() net.Conn })
		if !ok {
			return nc
		}
		next := u.Unwrap()
		if next == nil || next == nc {
			return nc
		}
		nc = next
	}
}

// refreshRetireReadDeadline 退役收尾期的读期限续期：非收尾期（draining
// 未置位）零开销直通。
func (c *Conn) refreshRetireReadDeadline() {
	if !c.draining.Load() {
		return
	}
	c.setRetireReadDeadline(time.Now().Add(retireLinger))
}

// drainDeadlineRetry 退役收尾期读期限超时的让位判定：期限到点但内核收
// 缓冲可能仍有对端已冲刷未交付的数据（Read 对已过期期限可能先报超时而不
// 返回缓冲数据）——首次超时续期重读一次；已续期过仍超时（连续静默两个
// linger 窗）才按对端僵死收口。
func (c *Conn) drainDeadlineRetry(err error) bool {
	if !c.draining.Load() || !errors.Is(err, os.ErrDeadlineExceeded) {
		return false
	}
	if c.drainRetried.Swap(true) {
		return false
	}
	c.setRetireReadDeadline(time.Now().Add(retireLinger))
	return true
}

// shutdown 统一关闭路径（读失败/写失败/校验失败/用户 Close/握手超时共用），
// closeOnce 保证恰好一次。sendErr 在 close(closed) 之前写入（channel
// close 的 happens-before 保证 Send 读方可见）。OnConnectionLost 在
// sync.Once 临界区之外执行——Once 持锁区内不调 handler（§19 陷阱 2），
// 且 Lost→Close 重入不会死锁；仅就绪连接上报（半开连接不进上层连接表）。
func (c *Conn) shutdown(reason error) {
	won := false
	c.closeOnce.Do(func() {
		won = true
		if reason == nil {
			c.sendErr = ErrClosed
		} else {
			if !errors.Is(reason, io.EOF) { // 对端干净收尾不告警
				log.Warn("rans/tcp: connection closing",
					"node", c.cfg.NodeID, "remote", c.RemoteAddr(), "reason", reason)
			}
			c.sendErr = fmt.Errorf("%w: %w", ErrClosed, reason)
		}
		close(c.closed)
		c.nc.Close()
	})
	if won && c.ready.Load() {
		if peer, ok := c.PeerNodeID(); ok {
			c.handler.OnConnectionLost(peer)
		}
	}
}
