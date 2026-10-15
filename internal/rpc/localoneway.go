package rpc

import (
	"context"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/ctxkey"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// sendLocalOneWay 本地 OneWay：激活编排 + 收件箱准入即返回，实体在 Pawnite
// loop goroutine 上串行执行（localOneWay 只留痕结果）。StrictLocalCodec
// （票 10）开启时请求入箱前做一次 codec 往返（未注册类型本地即败、实体持
// 有副本）。
func (r *Remote) sendLocalOneWay(ctx context.Context, id rans.ID, req any) error {
	if r.cfg.StrictLocalCodec {
		var err error
		req, err = strictRoundTrip(req, "request value")
		if err != nil {
			return err
		}
	}
	p, err := r.cfg.Manager.GetOrActivate(ctx, id)
	if err != nil {
		return err // ErrUnknownKind / ErrActivationFailed 透传
	}
	ent, ok := r.cfg.Manager.EntityOf(id)
	if !ok {
		return activation.ErrBusy // 激活竞态极小窗：实体刚被逐出，按 busy 让调用方重试
	}
	if !p.ReceiveRequest(&localOneWay{
		id:  id,
		ent: ent,
		// Tell 是因果链边界（H12 决策 15）：不携带调用方的路径——目标
		// Handle ctx 无因果值，其后续 Call 视为新的链起源（远端腿同款
		// 边界在 buildRequest 的 MsgOneWay 分野）。
		ctx: ctxkey.RebuildPropagation(ctxkey.WithoutCausal(ctx)),
		req: req,
	}) {
		return activation.ErrBusy
	}
	return nil
}

// localOneWay 本地单向投递：与远端投递同入箱接口（规格 §4.4 网络无关入站
// 抽象），Deliver 在串行 loop 上执行实体，结果与错误都只留痕——OneWay
// 无回执（规格 §8.2「一切失败记日志」）。ctx 已在 sendLocalOneWay 经传播
// 键同语义重建（与 callDelivery 同面）。
type localOneWay struct {
	id  rans.ID
	ent rans.Entity
	ctx context.Context
	req any
}

// 编译期断言：localOneWay 满足入站投递契约。
var _ pawnite.InboundDelivery = (*localOneWay)(nil)

// UUID 收件箱路由键。
func (d *localOneWay) UUID() rans.ID { return d.id }

// DeliveryDeadline 执行预算自描述（pawnite.Expirable）：OneWay 发送 ctx 经
// callBudget 派生（恒带 deadline），出队据此复查过期（Reject 仅留痕）。
func (d *localOneWay) DeliveryDeadline() (time.Time, bool) { return d.ctx.Deadline() }

// Deliver 在 Pawnite loop goroutine 上执行（串行语义落点）。Handle ctx 经
// 唯一信封构造（票 08）：单向形态脱钩发送腿取消面、重挂发送预算 deadline、
// 无类型遮蔽延迟应答登记点（单向无应答面，rans.Defer 必 panic）、携带执行
// 实例的 fencing 纪元与让出重入登记点（detached——work 跑到请求 deadline）。
func (d *localOneWay) Deliver(p *pawnite.Pawnite) {
	dl, _ := d.ctx.Deadline()
	ctx, cancel, _ := p.HandleCtx(d.ctx, d.id, pawnite.EnvelopeOneWay, dl, nil, true)
	defer cancel()
	if _, err := d.ent.Handle(ctx, d.req); err != nil {
		log.Warn("rans/rpc: local one-way handler error",
			"uuid", d.id.String(), "err", err)
	}
}

// Reject 准入回绝（收件箱满/销毁中）：无等待方可回，留痕即收场。
func (d *localOneWay) Reject(status rans.Code, reason rans.Reason) {
	log.Warn("rans/rpc: local one-way rejected",
		"uuid", d.id.String(), "status", uint8(status), "reason", reason.String())
}
