// 取消收场面（H10 决策 9）：ctx 收场映射与 MsgCancel 尽力通知发送半边；
// 独立成文件——gateway.go 恰在体量门禁线上。
package rpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// cancelSendBudget MsgCancel 发送尽力预算：单次尝试失败仅留痕（不占调用预算）。
const cancelSendBudget = 2 * time.Second

// ctxOutcome 调用方 ctx 收场错误（约定 spec §5.3 映射表，与 activation 同口径）。
func ctxOutcome(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return rans.ErrTimeout
	}
	return fmt.Errorf("%w: %w", rans.ErrCanceled, ctx.Err())
}

// canceledOutcome SendRequest 等待段 ctx.Done 收场：主动取消先尽力发 MsgCancel。
func (g *Gateway) canceledOutcome(ctx context.Context, target rans.NodeID, reqHdr wire.Header) error {
	g.sendCancelOnCanceled(ctx, target, reqHdr)
	return ctxOutcome(ctx)
}

// SetCancelQueue 取消通知的发件箱接线（票 12：MsgCancel 经请求同一目标
// 队列——排在请求之后不超车；目标熔断时取消随 TryPut 丢弃，请求必然也
// 未送出）。装配期注入（remote：网关构造后再建发件箱，随后接线）；nil
// 或未接线时退回直发（独立测试构造形态）。
func (g *Gateway) SetCancelQueue(o *Outbox) {
	g.cancelQueue = o
}

// sendCancelOnCanceled 调用方 ctx 以「主动取消」（非预算到期）收场时，向
// pending 目标节点异步发 MsgCancel（H10 决策 9）：头部携原请求 MessageID/
// SourceNodeID/TargetUUID——对端按（源节点，MessageID）定位在途/在箱请求。
// 消息不占 pending 表、无回包，失败仅留痕；deadline 到期不发。异步发送：
// 取消路径即刻收场，不被网络 IO 拖住。
func (g *Gateway) sendCancelOnCanceled(ctx context.Context, target rans.NodeID, reqHdr wire.Header) {
	if !errors.Is(ctx.Err(), context.Canceled) {
		return
	}
	hdr := wire.Header{
		Type:         protocol.MsgCancel,
		Flags:        protocol.FlagHasSourceNodeID | protocol.FlagHasTargetUUID,
		MessageID:    reqHdr.MessageID,
		SourceNodeID: reqHdr.SourceNodeID,
		TargetKind:   reqHdr.TargetKind,
		TargetInst:   reqHdr.TargetInst,
	}
	if g.cancelQueue != nil {
		// 同目标队列入队（票 12）：满即丢/熔断丢弃与取消的尽力语义一致
		//（请求本体在同一队列，二者同命运）。
		if err := g.cancelQueue.TryPut(target, hdr, nil); err != nil {
			log.Debug("rans/rpc: cancel notification dropped at outbox",
				"target", target, "message", hdr.MessageID, "err", err)
		}
		return
	}
	go func() {
		cctx, cancel := context.WithTimeout(context.Background(), cancelSendBudget) // 例外②：取消通知尽力预算（cancelSendBudget 集中定义——调用方 ctx 已收场，不能再作发送面）
		defer cancel()
		if err := g.conns.SendToNode(cctx, target, g.resolve, hdr, nil); err != nil {
			log.Warn("rans/rpc: cancel notification send failed",
				"target", target, "message", hdr.MessageID, "err", err)
		}
	}()
}
