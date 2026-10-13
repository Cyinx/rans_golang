package rpc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/ctxkey"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/pawnite"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// 编译期断言：remoteDelivery 满足入站投递、输家转发、在箱取消标记、因果
// 判环携带与延迟应答登记契约。
var (
	_ pawnite.InboundDelivery = (*remoteDelivery)(nil)
	_ pawnite.Forwardable     = (*remoteDelivery)(nil)
	_ pawnite.CancelAware     = (*remoteDelivery)(nil)
	_ pawnite.CycleAware      = (*remoteDelivery)(nil)
	_ rans.DeferredArm        = (*remoteDelivery)(nil)
)

// remoteDelivery 远端请求投递（规格 §4.4 网络无关入站抽象的远端形态）：
// 持有入站 header 与池化 body 页（所有权随构造移交本对象），解码推迟到
// 投递时刻（陷阱 5：输家转发需要原始字节，入箱时不解码）。
type remoteDelivery struct {
	d    *Dispatcher
	hdr  wire.Header
	body []byte // 池化页：所有权归本对象，release 恰好归还一次
	id   rans.ID
	// ext 入站扩展块的类型化解码（newDelivery 构造时刻解码一次）：预算
	// 半边锚 deadline、ctx 半边（传播键值项）在 Deliver 注入 Handle ctx。
	ext wire.RequestExt
	// dl 请求执行 deadline（H10 决策 6）：newDelivery 构造时刻（= 入站接收
	// 时刻）+ 预算——入箱等待不重置预算；Deliver 据此派生 Handle ctx。
	dl   time.Time
	once sync.Once

	// canceled MsgCancel 标记面（H10 决策 9）：在箱置位由出队检查弃置
	//（计 rans.call.canceled），执行中随 execCancel 取消 Handle ctx。
	canceled atomic.Bool
	// deferCanceled 已 Deferred 未 Reply 的标记取消（H12 决策 17 取消面扩展）：
	// 置位后 Responder 的迟到 Reply 丢弃。
	deferCanceled atomic.Bool
	// execCancel Handle ctx 取消面（Deliver 出队登记；nil = 尚未出队）。
	execCancel atomic.Pointer[context.CancelFunc]
	// reenter 让出面（Deliver 注入 Handle ctx 的同一份；MsgCancel 命中时
	// 取消在飞 work 的 wctx——见 markCanceled）。
	reenter atomic.Pointer[pawnite.ReenterArm]
	// deferArmed 延迟应答登记标记（H12 决策 16 前半，仅 MsgRequest 生效）。
	deferArmed atomic.Bool
}

// UUID 目标身份（收件箱路由键）。
func (r *remoteDelivery) UUID() rans.ID { return r.id }

// DeliveryDeadline 执行预算自描述（pawnite.Expirable）：deadline 锚定在
// newDelivery 构造时刻（收到时刻 + budget_ms，对端未带 → 本节点 Default）。
func (r *remoteDelivery) DeliveryDeadline() (time.Time, bool) { return r.dl, true }

// release 恰好一次归还页。
//
// 幂等性契约：Deliver panic 后 pawnite.invoke 的
// recover 会再调 Reject——Deliver 的 defer 与 Reject 的 defer 两条路径都
// 落到这里，sync.Once 保证池页不双重 Put（双重 Put = 同一页双租，数据
// 损坏）。Forward 成功路径先把 body 解除引用（所有权随发送移交连接），
// 此处随之空转。
//
// 取消定位摘除（H12 决策 17 的延迟应答扩展）：未延迟的投递照旧就地摘除；
// 已 Defer 的投递对调用方仍在途——定位保留至请求 deadline（迟到 MsgCancel
// 仍可标记/取消 wctx），deadline 后看护定时清扫、正常收场即刻摘除。
func (r *remoteDelivery) release() {
	r.once.Do(func() {
		if r.deferArmed.Load() {
			d := r.d
			time.AfterFunc(time.Until(r.dl), func() { d.unregisterCancel(r) })
		} else {
			r.d.unregisterCancel(r)
		}
		if r.body != nil {
			r.d.pool.Put(r.body)
		}
	})
}

// markCanceled 取消定位命中（dispatcher.handleCancel 调用）：先置标记再调
// 取消面——执行中立即取消 Handle ctx，在箱由出队检查弃置，在飞 work 由让出
// 面取消 wctx、已 Deferred 未 Reply 标记取消（H12 决策 17）。与 Deliver 的
// 「先写后读」交叉配对（原子线性化）保证取消不丢单边；重复调用幂等。
func (r *remoteDelivery) markCanceled() {
	r.canceled.Store(true)
	r.deferCanceled.Store(true)
	if fn := r.execCancel.Load(); fn != nil {
		(*fn)()
	}
	if arm := r.reenter.Load(); arm != nil {
		arm.CancelWorks()
	}
}

// DeliveryCanceled 在箱取消标记读数（pawnite.CancelAware）：出队执行入口据此弃置。
func (r *remoteDelivery) DeliveryCanceled() bool { return r.canceled.Load() }

// DeferredCanceled 已 Deferred 未 Reply 的标记取消读数（H12 决策 17，可选
// 丢弃判据）：MsgCancel 命中后迟到 Reply 丢弃。
func (r *remoteDelivery) DeferredCanceled() bool { return r.deferCanceled.Load() }

// DeliveryCausalPath 入站因果路径（pawnite.CycleAware，H12 决策 15）：扩展块
// causal 解码产物原样上报（哈希序列，途经序）；未携带返回 nil（Tell 恒 nil）。
func (r *remoteDelivery) DeliveryCausalPath() []uint64 {
	if r.ext.Causal != nil {
		return r.ext.Causal.Path
	}
	return nil
}

// Deliver 在 Pawnite loop goroutine 上执行（串行语义落点）：投递时刻解码
// 外封（陷阱 5）→ 实体 Handle（ctx 注入入站因果段，§17.2 派发入口注入）→
// Request 成功回 Response（外封编码的实体返回值）/ OneWay 不回应答；
// 各失败态回结构化 Error 帧（OneWay 与匿名源在 Dispatcher.reply 内统一
// 丢弃留痕）。
func (r *remoteDelivery) Deliver(p *pawnite.Pawnite) {
	defer r.release()
	env, err := wire.UnmarshalEnvelope(r.body)
	if err != nil {
		r.replyError(rans.NewError(rans.CodeDecode, rans.ReasonNone,
			"decode envelope: "+err.Error()))
		return
	}
	v, err := codec.Decode(env.TypeName, env.Payload)
	if err != nil {
		r.replyError(rans.NewError(rans.CodeDecode, rans.ReasonNone,
			"decode payload: "+err.Error()))
		return
	}
	ent, ok := r.d.mgr.EntityOf(r.id)
	if !ok {
		r.replyError(rans.NewError(rans.CodeUnavailable, rans.ReasonNone,
			"entity evicted before delivery"))
		return
	}
	// ctx 携带执行实例的 incarnation 纪元（fencing 面，与本地
	// callDelivery.Deliver 同款）；基 ctx 携带节点调用面。入站因果路径注入
	//（H12 决策 14 v2：扩展块 causal 解码产物；Tell 不携带，恒 nil）。传播
	// 键值项解码注入（H10 决策 8）：扩展块 ctx 项按登记名查表解码回填，与
	// 本地同语义重建共守「可见值 = decode 产物」形态（INV6）；未登记名/
	// 解码失败项跳过留痕。Handle ctx 的 deadline 按请求预算派生（H10 决策
	// 6）：dl 已在 newDelivery 按「收到时刻 + budget」锚定；出队检查已挡掉
	// 耗尽者，竞态极小窗内得到已到期 ctx 按取消语义收场。调用方 ctx 不跨
	// 节点：远端折叠传 nil ctxErr。延迟应答登记点仅请求形态注入（H12 决策
	// 16 前半：单向无应答面，Handle 内 rans.Defer 经它登记）。
	ctx := r.d.baseCtx()
	if cp := r.ext.Causal; cp != nil {
		ctx = ctxkey.WithCausal(ctx, cp)
	}
	if n := len(r.ext.Ctx); n > 0 {
		items := make([]ctxkey.PropItem, 0, n)
		for _, it := range r.ext.Ctx {
			items = append(items, ctxkey.PropItem{Name: it.Name, Value: it.Value})
		}
		var skipped int
		ctx, skipped = ctxkey.InjectPropagation(ctx, items)
		if skipped > 0 {
			log.Warn("rans/rpc: inbound propagation items skipped",
				"count", skipped, "message", r.hdr.MessageID)
		}
	}
	ctx = ctxkey.WithSelf(ctxkey.WithIncarnation(ctx, p.Incarnation()), r.id.Kind, r.id.Key)
	if r.hdr.Type == protocol.MsgRequest {
		ctx = ctxkey.WithDeferPoint(ctx, r)
	}
	ctx, cancel := context.WithDeadline(ctx, r.dl)
	defer cancel()
	// 让出面注入（H12 决策 16 后半）：远端腿 detached——Handle ctx 随投递
	// 返回的 defer cancel 收场，wctx 脱钩派生（见 pawnite.ReenterArm）；
	// 执行面随投递留存供 MsgCancel 取消在飞 work（markCanceled）。
	arm := p.NewReenterArm(ctx, true)
	r.reenter.Store(arm)
	ctx = ctxkey.WithReenterPoint(ctx, arm)
	// 取消面登记 + 竞态复查（H10 决策 9）：先登记取消面再复查标记，兜住
	// markCanceled 读到 nil 面的窗口（配对次序见 markCanceled 注释）。
	r.execCancel.Store(&cancel)
	if r.canceled.Load() {
		p.NoteCanceled()
		r.Reject(rans.CodeCanceled, rans.ReasonNone)
		return
	}
	out, err := ent.Handle(ctx, v)
	if err != nil {
		// Handle 错误边界（远端半边）：与本地投递边界共用
		// activation.FoldHandlerError 唯一定义——折叠为 CodeError+ReasonHandler，
		// AppError/AppDataError 的业务维（AppCode/AppData）随结构化 Error
		// 帧过线，调用方得到与本地同构的 *rans.Error。ctxErr=nil
		// 时折叠无透传出口，类型断言必中（defensive 分支仅兜契约破坏）。
		r.replyError(foldHandlerErr(err))
		return
	}
	if r.hdr.Type == protocol.MsgOneWay {
		return // 单向消息：执行即可，不应答
	}
	// 延迟应答判读与本地 callDelivery.Deliver 同一矩阵（契约正本见 rans.Defer）。
	if out == rans.Deferred {
		if r.deferArmed.Load() {
			return // 已 Defer：不回包，等待 Responder.Reply
		}
		r.replyError(rans.NewError(rans.CodeError, rans.ReasonHandler,
			"rans: handler returned rans.Deferred without calling rans.Defer"))
		return
	}
	if r.deferArmed.Load() {
		r.replyError(rans.NewError(rans.CodeError, rans.ReasonHandler,
			"rans: handler called rans.Defer but returned a value; must return rans.Deferred"))
		return
	}
	r.replyValue(out)
}

// ArmDefer 延迟应答登记（rans.Defer 的落点）：首次生效取回执行面；重复调用返回 nil。
func (r *remoteDelivery) ArmDefer() rans.DeferredArm {
	if !r.deferArmed.CompareAndSwap(false, true) {
		return nil
	}
	return r
}

// DeliverDeferred 延迟回包送达（Responder.Reply 的落点）：与正常应答同一非
// 阻塞出站面（满即丢）；可在 release 之后调用（只用 hdr/dl 值，不触池化
// body）。延迟应答收场即摘除取消定位（幂等，看护清扫兜底）。
func (r *remoteDelivery) DeliverDeferred(v any, err error) {
	r.d.unregisterCancel(r)
	if err != nil {
		r.replyError(foldHandlerErr(err))
		return
	}
	r.replyValue(v)
}

// DeferredDeadline 本请求的执行 deadline（newDelivery 锚定的「收到时刻 +
// budget」权威，Responder 到期丢弃判据）。
func (r *remoteDelivery) DeferredDeadline() time.Time { return r.dl }

// foldHandlerErr Handle 错误折叠为线格式 *rans.Error（两收场共用；
// FoldHandlerError 唯一定义的远端半边适配，defensive 分支仅兜契约破坏）。
func foldHandlerErr(err error) *rans.Error {
	folded := activation.FoldHandlerError(err, nil)
	re, ok := folded.(*rans.Error)
	if !ok {
		re = rans.NewError(rans.CodeError, rans.ReasonNone, folded.Error())
	}
	return re
}

func (r *remoteDelivery) replyValue(v any) { // 编码返回值并回 Response 帧（两收场共用）
	typeName, payload, err := codec.Encode(v)
	if err != nil {
		r.replyError(rans.NewError(rans.CodeError, rans.ReasonNone,
			"encode reply: "+err.Error()))
		return
	}
	r.d.reply(r.hdr, protocol.MsgResponse, rans.CodeOK,
		wire.MarshalEnvelope(typeName, payload))
}

// Reject 回结对端：pawnite 准入拒绝（判环/收件箱满/销毁中/失租/激活失败
// 排空）与 Deliver panic 后的销账（reject-on-panic）都走这里。
// (Code, Reason) 原样入结构化 Error 帧体（reason 维过线，线上判定
// 以帧为准；文本仅保留 Reason 诊断名供排障，环回绝例外——文本带完整环，
// 见 CycleRejectText）——回绝不折叠（与 Deliver 折叠的 Handle 错误形成
// 「回绝可判 Code/Reason、执行错误不透明」的分界）。页归还幂等（见
// release）；Deliver panic 后再 Reject 是预期路径而非双重收场。
func (r *remoteDelivery) Reject(status rans.Code, reason rans.Reason) {
	defer r.release()
	msg := reason.String()
	if reason == rans.ReasonCycle {
		msg = pawnite.CycleRejectText(r.DeliveryCausalPath(), r.id)
	}
	r.replyError(rans.NewError(status, reason, msg))
}

// Forward 输家整箱转发（规格 §12）：仲裁落败时把原始 body 字节透传给
// 赢家节点——不解码不重编（陷阱 5 的另一面），MessageID/Source/因果段
// 原样保留，仅置 Forwarded 标记。
//
// 单跳防环：入站已带 Forwarded 标记（本消息已被转发过一次仍遭遇仲裁
// 落败）→ 不再二次转发，按 NotFound+StaleLocation 回源。状态依据（零丢失
// 收尾）：该帧必然从未被执行（环守卫先于一切投递路径——
// 本方法只在「未投递即被整箱收集」的面上调用），回 NotFound=位置陈旧
// → 调用方重试环重解析路由无条件重试（RetryNotFound 不看幂等标注），
// 良性竞态（归属又翻了一手）不被上抛为调用方可见的确定性失败。匿名源
// （SourceNodeID=0）：无地址可回，Error 帧在 Dispatcher.reply 内按匿名源
// 规则丢弃（规格 §12 促客户端自愈）。
//
// panic-safety 契约：本方法由 pawnite.
// drainForwardOrReject 在锁外调用且无 recover 包裹——实现不得 panic：
// 仅头部位运算与发件箱非阻塞入箱（满即丢，错误只留痕，不上抛）。
func (r *remoteDelivery) Forward(winner rans.NodeID) {
	defer r.release()
	if r.hdr.Flags&protocol.FlagForwarded != 0 {
		// NotFound+StaleLocation（结构化 reason）：该帧必然从未被执行
		// ——分类唯一表归 RetryNotFound，调用方重解析位置后重试。
		r.replyError(rans.NewError(rans.CodeNotFound, rans.ReasonStaleLocation,
			"forward loop prevented (single hop, spec §12); location stale, re-resolve and retry"))
		return
	}
	if r.hdr.SourceNodeID == 0 {
		r.replyError(rans.NewError(rans.CodeNotFound, rans.ReasonStaleLocation,
			"forward loser with anonymous source: no reply address"))
		return
	}
	fh := r.hdr
	fh.Flags |= protocol.FlagForwarded
	// 非阻塞入箱（H11 决策 12/13）：转发帧交节点发件箱——满即丢计数，
	// 发送（地址解析+懒拨号+端点韧性）归分片 worker 异步完成。调用点
	// （读循环输家分支与 pawnite 终局/迁移整箱排空）零阻塞 IO。
	if err := r.d.outbox.TryPut(winner, fh, r.body); err != nil {
		// 满箱丢弃/关箱拒绝：所有权未移交（页由 defer release 归还池），
		// 源端将按请求预算超时收场。
		log.Warn("rans/rpc: forward to winner dropped without sending",
			"winner", winner, "message", fh.MessageID, "err", err)
		return
	}
	r.body = nil // 所有权随入箱移交发件箱：解除引用，防 release 双重归还
}

// replyError 按入站 SourceNodeID 回结构化 Error 帧：统一错误对象
// 经 wire.MarshalError 编码为帧体，头部 Status 与 body 的 Code 同值
// （头部快速判定面保留，线上判定以帧体为准）。
func (r *remoteDelivery) replyError(e *rans.Error) {
	r.d.replyError(r.hdr, e)
}
