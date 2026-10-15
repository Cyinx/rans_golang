package rpc

// 网关发送腿：逐尝试重试环与预算收场（票 05 拆出；网关本体见 gateway.go）。

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/transport/tcp"
	"github.com/cyinx/rans_golang/internal/wire"
)

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
		// 每次尝试（含第 0 次）前的 deadline 检查（票 05）：已过线的帧不再
		// 进 SendToNode——其就绪等待/快路径的 ctx 逃逸窗口不再有机会送出
		// 过期帧（迟到执行的封口）。
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return g.sendBudgetExhausted(target, lastErr)
		}
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
		// 失败后的 deadline 复查先于 ctx 收场（票 05 表单优先级）：deadline
		// 到期的失败以「确定未投递」形态收场，而非 ctx 的 Timeout/Canceled。
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
