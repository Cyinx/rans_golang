// TLS 包装层（规格 §10.1 TlsTransportConnection / §19 陷阱 13）：
// TLS 认证先于管道 IO——握手在帧循环启动之前显式完成，明文字节不进
// FrameDecoder；帧/消息层零改动（tls.Conn 只是 net.Conn 的包装）。
package tcp

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

// tlsHandshaker 握手能力探测接口：仅裸 *tls.Conn 满足。包装连接
// （WrapConn 注入的故障注入层，如 testkit 的 slowConn）嵌 net.Conn 接口
// ——接口嵌入不提升 HandshakeContext，探测对其失效。因此 Manager 在应用
// 包装之前对裸连接显式握手（startConn 次序：握手 → 包装 → 帧循环），
// 「握手先于帧循环」的结构性保证不因包装丢失。
type tlsHandshaker interface {
	HandshakeContext(context.Context) error
}

// handshakeTLS 有界时限内完成 TLS 握手；timeout<=0 时取默认握手超时。
// ctx 同时受调用方语境约束（拨号侧传入拨号 ctx，两者取更早到期）。
func handshakeTLS(ctx context.Context, hs tlsHandshaker, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultHandshakeTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := hs.HandshakeContext(ctx); err != nil {
		return fmt.Errorf("rans: connect: tls handshake: %w", err)
	}
	return nil
}

// ensureHandshake 帧循环启动前的结构性握手保证（陷阱 13 核心）。主调用点
// 是 Manager.startConn：在应用 WrapConn 包装**之前**对裸连接执行（包装连接
// 嵌 net.Conn 接口，本探测对其失效——见 tlsHandshaker 注释），保证显式
// 握手先于帧循环；Conn.Start 内的调用保留给不经 Manager 的裸 NewConn 用户，
// 对已完成握手的连接幂等即刻返回（crypto/tls 契约）。失败即返回错误
// （调用方关闭连接，帧循环不启动）；明文连接零操作返回（明文路径零改动）。
func ensureHandshake(nc net.Conn, timeout time.Duration) error {
	hs, ok := nc.(tlsHandshaker)
	if !ok {
		return nil
	}
	return handshakeTLS(context.Background(), hs, timeout) // 例外②：拨号侧握手显式预算（timeout 常量集中定义；拨号面无调用方 ctx 可派）
}

// dialTLS 拨号侧包装 + 显式握手（陷阱 13）：dial func 返回的裸连接外再
// 包 tls.Client，并在 NewConn 之前完成握手——失败关闭连接并返回错误
// （拨号失败，帧循环永不启动，任何帧不曾被读）。成功后 startConn 内的
// ensureHandshake 对已完成握手的连接即刻返回（crypto/tls 幂等）。
func dialTLS(ctx context.Context, nc net.Conn, tlsCfg *tls.Config, timeout time.Duration) (net.Conn, error) {
	tc := tls.Client(nc, tlsCfg)
	if err := handshakeTLS(ctx, tc, timeout); err != nil {
		tc.Close()
		return nil, err
	}
	return tc, nil
}
