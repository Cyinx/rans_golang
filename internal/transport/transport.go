// Package transport 定义传输端口（hexagonal port）：Conn 是真正的抽象半边
// ——上层经它收发帧、不触具体网络实现，transport/tcp 是其适配器；连接管理
// （Manager）属装配面而非端口——rpc 直接构造并驱动 *tcp.Manager（未抽象）。
package transport

import (
	"context"
	"errors"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/wire"
)

// ErrNotReady 握手完成前发送非控制面消息被拒（规格 §10.2 pre-auth 门禁
// 的出站侧）。控制面消息（握手/心跳）不受此限。
var ErrNotReady = errors.New("rans: send: connection not ready")

// Handler 连接事件回调接口，由上层（连接管理器/远端分发器）实现，
// 传输适配器在握手完成、应用消息到达、连接丢失时调用。
//
// 回调不宜长时间阻塞：入站读循环在 HandleMessage 返回前不会继续读帧
// （慢消费者 = 背压向对端传播）；OnConnectionLost 可能与进行中的
// HandleMessage 并发（写路径失败触发的关闭与读路径互不同步），上层
// 需自行串行化连接表操作。
type Handler interface {
	// HandleMessage 应用消息到达。仅握手完成后可达——pre-auth 门禁
	// 保证应用消息不会进入本路径（规格 §10.2，断连不回错）。
	// body 为池化页（wire.PagePool.Get 所出），所有权随本次调用移交
	// handler：用完必须 pool.Put 归还，归还后不得再引用——页会被复用，
	// 跨帧持有等于数据损坏。
	HandleMessage(hdr wire.Header, body []byte)
	// OnHandshakeComplete 对端握手校验通过、连接就绪时调用一次，
	// peerNode 为对端声明的节点号。此后本连接上开始投递应用消息。
	OnHandshakeComplete(peerNode rans.NodeID)
	// OnConnectionLost 连接断开时调用；仅在就绪后报告——未完成握手的
	// 半开连接不进上层连接表，无需清理。
	OnConnectionLost(peerNode rans.NodeID)
}

// Conn 传输连接端口（规格 §10.1 连接层次的最小面）。Send 的阻塞语义
// 即背压第一级：出站有界队列满则阻塞调用方，压力最终传导至 TCP 窗口。
type Conn interface {
	// Send 发送一帧，尊重 ctx：握手完成前仅允许控制面消息
	// （protocol.IsControlPlane），应用消息返回 ErrNotReady；出站队列满时
	// 阻塞直到腾出空间、连接关闭或 ctx 到期（到期返回包装 ctx.Err() 的
	// 错误，非永久阻塞），已关连接返回错误。body 所有权随成功返回移交
	// 连接（实现负责写出后归还页池），调用方不得再修改或复用该切片；
	// 失败返回时所有权不移交。
	Send(ctx context.Context, hdr wire.Header, body []byte) error
	// TrySend 非阻塞发送一帧（ok 语义）：队列满即刻 false 不阻塞；false
	// 时所有权不移交（调用方持有 body 自行处置）。true 与 Send 成功同义
	// ——帧已入队，必达或随连接终态处置。
	TrySend(hdr wire.Header, body []byte) bool
	// PeerNodeID 返回对端节点号；ok=false 表示握手尚未完成。
	PeerNodeID() (rans.NodeID, bool)
	// Ready 握手是否已完成。
	Ready() bool
	// Close 关闭连接；幂等，重复调用返回 nil。
	Close() error
	// Done 返回连接关闭信号 channel（close 即已关闭）。
	Done() <-chan struct{}
	// RemoteAddr 对端地址描述（日志与上层排障用）。
	RemoteAddr() string
}
