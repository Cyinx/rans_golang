package rpc

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/ctxkey"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/routing"
	"github.com/cyinx/rans_golang/internal/wire"
)

// Call 请求-响应调用（规格 §9.1）：默认（非幂等）选项的 CallOpts。出站 ctx
// 的因果路径在两腿分岔前统一挂接（H12 决策 14/15，见 outboundCausal）。
func (r *Remote) Call(ctx context.Context, id rans.ID, req any) (any, error) {
	return r.CallOpts(ctx, id, req, routing.DefaultOptions())
}

// CallOpts 请求-响应调用（规格 §9.1/§9.2）：路由梯子（缓存 Get → 目录
// Lookup → 远程 / 本地）+ 分类重试环。
//   - 本地命中（含负缓存 miss 的本地激活/放置未命中）→ 绕环直调（本地
//     错误零重试；仅 ErrActivationFailed 这类执行前确定性失败本地重试
//     一次）；
//   - 远程路径进环（上限 2 次重试 + (n+1)×20ms 线性退避）：每次 attempt
//     重走路由梯子 + 重建外封（载荷工厂契约——池化缓冲随失败归还，同载荷
//     重试会读到已归还内存）；RetryAmbiguous（超时/节点失联）仅在
//     opts.Idempotent 时重试；可重试类失败先失效缓存条目再退避（NotFound
//     的位置陈旧重解析、SendFailed 发送失败的重路由都以失效为前提——钩子④的
//     调用点收敛在此）；
//   - 重试途中重路由翻转为本地（目录/缓存更新）→ 环内直接本地执行。
//
// 错误透传（统一错误模型，一律 *rans.Error）：对端 Error 帧 →
// 结构化帧体解码的 {Code, Reason, Message, AppCode, AppData} + Node=源节点；
// 网关超时/调用方预算到期 → Timeout；
// 节点失联快速失败 → ConnectionLost；发送重试耗尽 → Unavailable+SendFailed；
// 调用方主动取消 → Canceled；编解码失败按原样返回（非 rans 形态，分类必
// NoRetry）。
func (r *Remote) CallOpts(ctx context.Context, id rans.ID, req any, opts routing.InvokeOptions) (any, error) {
	// 因果路径出站挂点（H12 决策 14/15）先于路由/重试执行一次：接线层激活
	// 重试与环内 attempt 共用同一次追加，不重复计跳（追加面见 outboundCausal）。
	ctx = r.outboundCausal(ctx)
	v, err := r.callOnce(ctx, id, req, opts)
	// 本地激活失败/仲裁落败重试：都发生在请求执行前（register-first：
	// 注册后失败一律 Unavailable 双哨兵回绝——同步窗 ErrActivationFailed
	// 单哨兵、异步窗（在箱投递被终局排空回绝）ArbitrationLost 双哨兵均可
	// 判），重试不破坏恰好一次语义。异步窗的陈旧 self 正缓存未被钩子③
	// 改写（同步窗才有）——重试前就地失效，路由梯子重新收敛到赢家（票
	// 01 时移暴露的存量缺口）。有界两试：陈旧缓存与视图空窗可能连环
	//（Form4 实证一次重试仍落同败路），可重试类零执行代价，多试收敛。
	for attempt := 0; attempt < 2 && isLocalActivationRetryable(err); attempt++ {
		r.invalidateOnFailure(id, err)
		v, err = r.callOnce(ctx, id, req, opts)
	}
	return v, err
}

// callOnce 单轮完整调用：路由 → 本地直调 / 远程环。本地/远程两腿的执行
// ctx 都经 callBudget 派生（逐请求预算合成，取消穿透保持）。
func (r *Remote) callOnce(ctx context.Context, id rans.ID, req any, opts routing.InvokeOptions) (any, error) {
	var out any
	err := r.invokeRouted(ctx, id, opts,
		func(c context.Context) error {
			// 本地腿不随返回释放预算 ctx（票 08）：ReenterAfter 的 work 挂
			// Handle ctx 链存活到请求 deadline/调用方取消——发送腿的 defer
			// cancel 会杀掉在飞 work（远端腿的 work 在对端 detached 让出面上，
			// 不受本腿释放影响）。预算 ctx 自带 deadline，释放由到期兜底。
			bctx, _ := r.callBudget(c, opts)
			v, lerr := r.callLocal(bctx, id, req)
			if lerr != nil {
				// 本地错误形态（折叠后的 CodeError+ReasonHandler 与
				// 回绝的 *rans.Error）对分类唯一表裁决——Busy 过载保护不重试、
				// Handler 错误确定性失败；与远端同构。
				return lerr
			}
			out = v
			return nil
		},
		func(c context.Context, tgt rans.NodeID) error {
			bctx, cancel := r.callBudget(c, opts)
			if cancel != nil {
				defer cancel()
			}
			hdr, body, berr := r.buildRequest(bctx, protocol.MsgRequest, id, req)
			if berr != nil {
				return berr
			}
			// 请求帧与单向帧同箱同分片（同目标 FIFO）：「先 Tell 后 Call」
			// 的跨节点次序由发件箱单 worker 串行承载；登记/等待/超时/取消
			// 语义与直发形态同源（见 SendRequestQueued）。
			_, respBody, serr := r.gw.SendRequestQueued(bctx, tgt, hdr, body, r.outbox.PutCall)
			if serr != nil {
				r.invalidateOnFailure(id, serr)
				return serr
			}
			v, derr := r.decodeResponse(respBody)
			if derr != nil {
				return derr
			}
			out = v
			return nil
		})
	return out, err
}

// callBudget 逐请求预算合成与派生（H10 决策 4）：本次预算 =
// min(调用方 ctx 剩余, opts.Timeout（rans.Timeout(d) 选项值）, MaxCallTimeout)，
// 无 deadline 无选项取 DefaultCallTimeout。返回调用方 ctx 的派生（取消
// 穿透保持）与释放函数；调用方 ctx 的 deadline 已不超合成结果时原样返回
// （零派生——调用方缩短/已到期的 ctx 语义原样生效，也不能经合成延长超
// 预算的 deadline）。重试环每 attempt 各调一次（逐请求各自登记预算）。
func (r *Remote) callBudget(ctx context.Context, opts routing.InvokeOptions) (context.Context, context.CancelFunc) {
	budget, has := time.Duration(0), false
	if dl, ok := ctx.Deadline(); ok {
		budget, has = time.Until(dl), true
	}
	if opts.Timeout > 0 && (!has || opts.Timeout < budget) {
		budget, has = opts.Timeout, true
	}
	if !has {
		budget = r.cfg.DefaultCallTimeout
	} else if budget > r.cfg.MaxCallTimeout {
		budget = r.cfg.MaxCallTimeout
	}
	if dl, ok := ctx.Deadline(); ok && budget >= time.Until(dl) {
		return ctx, nil
	}
	return context.WithTimeout(ctx, budget)
}

// invokeRouted 「路由 → 本地 / 远程环」的共用骨架（Call 与 OneWay 的路由/
// 重试语义单点实现）：前置路由一次，本地命中直调 local；远程路径进 routing.Invoke
// 重试环，每 attempt 重走路由（可翻转为本地），翻转后仍走 local。初次
// attempt 复用前置路由结果（缓存未配置时不做双重目录查询）。Call 与
// OneWay 的差异（本地执行函数、远程发送函数）经参数注入。
func (r *Remote) invokeRouted(ctx context.Context, id rans.ID, opts routing.InvokeOptions,
	local func(ctx context.Context) error,
	remote func(ctx context.Context, target rans.NodeID) error) error {
	target, isRemote, err := r.route(ctx, id)
	if err != nil {
		return err
	}
	if !isRemote {
		return local(ctx)
	}
	first := true
	return routing.Invoke(ctx, opts, func(attemptCtx context.Context) error {
		if first {
			first = false
			return remote(attemptCtx, target)
		}
		tgt, rem, rerr := r.route(attemptCtx, id)
		if rerr != nil {
			return rerr
		}
		if !rem {
			// 环内翻转本地：本地 Handle 错误已在
			// 投递边界（callDelivery.Deliver）折叠为 CodeError+ReasonHandler
			// 的 *rans.Error，进入分类唯一表必判 NoRetry——嵌套运行时形态
			// 原样透传会被误判为投递失败而重复执行（回归锚
			// TestCallFlipToLocalHandlerErrorNoDuplicateExecution）。
			return local(attemptCtx)
		}
		return remote(attemptCtx, tgt)
	})
}

// callLocal 本地路径：mgr.InvokeLocal 直调，零重试（激活失败重试一次在外层
// CallOpts/SendOneWayOpts 的接线层）。Handle 错误已在投递边界折叠为
// CodeError+ReasonHandler，回绝错误保留
// (Code, Reason) 面——两类经分类唯一表裁决。StrictLocalCodec（票 10）开启
// 时：请求先往返再投递（实体持有副本、未注册类型本地即败），应答在返回前
// 往返（调用方持有副本，含延迟应答——都经本方法返回）；错误形态与远端
// buildRequest/decodeResponse 同构。
func (r *Remote) callLocal(ctx context.Context, id rans.ID, req any) (any, error) {
	if r.cfg.StrictLocalCodec {
		var err error
		req, err = strictRoundTrip(req, "request value")
		if err != nil {
			return nil, err
		}
	}
	v, err := r.cfg.Manager.InvokeLocal(ctx, id, req)
	if err != nil || !r.cfg.StrictLocalCodec {
		return v, err
	}
	return strictRoundTrip(v, "response value")
}

// strictRoundTrip 本地严格编解码的一次往返（票 10）：错误文本与远端
// buildRequest/decodeResponse 逐字对齐（同构判据）。
func strictRoundTrip(v any, what string) (any, error) {
	name, payload, err := codec.Encode(v)
	if err != nil {
		return nil, fmt.Errorf("rans: encode: %s: %w", what, err)
	}
	nv, err := codec.Decode(name, payload)
	if err != nil {
		return nil, fmt.Errorf("rans: decode: %s (type %q): %w", what, name, err)
	}
	return nv, nil
}

// isLocalActivationRetryable 报告本地错误是否属于「执行前确定性失败」可
// 重试类：ErrActivationFailed（激活终态已落、注册表已清；register-first
// 下仲裁落败也以此哨兵包装——Register 原子域
// 落败即返回 *ArbitrationLostError，无「无赢家可转发」竞态）。其余本地错
// 误（handler 错误、ErrBusy/ErrTimeout 等）一律上抛。
func isLocalActivationRetryable(err error) bool {
	if errors.Is(err, activation.ErrActivationFailed) {
		return true
	}
	// 异步窗孪生形态（票 01 时移暴露的存量缺口）：在箱本地投递被 pawnite
	// 终局排空回绝的 Unavailable+ArbitrationLost——与同步窗的
	// ErrActivationFailed 同为「确定未执行且归属已在赢家」，重试经路由
	// 梯子自然送达。
	return rans.CodeOf(err) == rans.CodeUnavailable && rans.ReasonOf(err) == rans.ReasonArbitrationLost
}

// invalidateOnFailure 可重试类发送失败后的缓存失效（钩子④调用点 + NotFound
// 重查前提）：条目失效后，环的下一次 attempt 重走路由梯子才会真正重查目录
// （否则缓存仍命中旧位置，重试原地打转）。ctx 取消等 NoRetry 类不失效。
func (r *Remote) invalidateOnFailure(id rans.ID, err error) {
	if r.cfg.LocationCache == nil {
		return
	}
	if routing.ShouldEnterRing(err) {
		r.cfg.LocationCache.Invalidate(id)
	}
}

// SendOneWay 单向发送（规格 §8.2）：默认（非幂等）选项的 SendOneWayOpts。
func (r *Remote) SendOneWay(ctx context.Context, id rans.ID, req any) error {
	return r.SendOneWayOpts(ctx, id, req, routing.DefaultOptions())
}

// SendOneWayOpts 单向发送（规格 §8.2/§9.2 OneWay 环）：路由与重试语义同
// CallOpts——远程路径经环（at-most-once：重试只发生在上一次确定未投递
// [Unavailable+SendFailed 类] 或幂等标注的模糊类之后）；本地路径入箱即返回，激活
// 失败重试一次。实体执行结果与错误只留痕，Task 永不 fault 的等价语义。
func (r *Remote) SendOneWayOpts(ctx context.Context, id rans.ID, req any, opts routing.InvokeOptions) error {
	err := r.sendOneWayOnce(ctx, id, req, opts)
	if isLocalActivationRetryable(err) {
		r.invalidateOnFailure(id, err) // 同 CallOpts：异步窗陈旧 self 缓存就地失效
		err = r.sendOneWayOnce(ctx, id, req, opts)
	}
	return err
}

// sendOneWayOnce 单轮完整单向发送：路由 → 本地入箱 / 远程环（骨架见
// invokeRouted，差异仅本地/远程两个执行函数；两腿执行 ctx 同样经
// callBudget 派生——OneWay 的发送段与本地投递受同一逐请求预算约束）。
func (r *Remote) sendOneWayOnce(ctx context.Context, id rans.ID, req any, opts routing.InvokeOptions) error {
	return r.sendOneWayLeg(ctx, id, req, opts, false)
}

// sendOneWayLeg 单向发送腿。fullDrop 为满策略生产者分野开关（H11 决策 12
// ——固定策略不设公开开关）：false = 显式 Tell，发件箱满箱阻塞至预算到
// 期；true = 非阻塞生产者（事件扇出），满即丢计数不阻塞调用方。
func (r *Remote) sendOneWayLeg(ctx context.Context, id rans.ID, req any, opts routing.InvokeOptions, fullDrop bool) error {
	return r.invokeRouted(ctx, id, opts,
		func(c context.Context) error {
			bctx, cancel := r.callBudget(c, opts)
			if cancel != nil {
				defer cancel()
			}
			return r.sendLocalOneWay(bctx, id, req)
		},
		func(c context.Context, tgt rans.NodeID) error {
			bctx, cancel := r.callBudget(c, opts)
			if cancel != nil {
				defer cancel()
			}
			hdr, body, berr := r.buildRequest(bctx, protocol.MsgOneWay, id, req)
			if berr != nil {
				return berr
			}
			// H11 决策 11/12：单向出站入发件箱——满箱语义按生产者分野
			// （显式 Tell 阻塞至 bctx 到期；事件扇出满即丢计数）。Call
			// 请求帧同箱入队（PutCall，见 callOnce 远程腿）——同目标 FIFO
			// 承载「先 Tell 后 Call」的跨节点次序。
			var serr error
			if fullDrop {
				serr = r.outbox.TryPut(tgt, hdr, body)
			} else {
				serr = r.outbox.Put(bctx, tgt, hdr, body)
			}
			if serr != nil {
				r.invalidateOnFailure(id, serr)
			}
			return serr
		})
}

// ---- 内部构件 ----

// routeMiss 目录 miss 的收口：放置决策或本地激活。放置询问经 kind 感知
// 探测：Placement 实现 KindPlacement 即按目标 kind 询问
// （候选只含声明承载该 kind 的节点——纯接入节点不进业务放置），否则沿用
// Pick（全节点随机，既有行为）。
func (r *Remote) routeMiss(id rans.ID) (rans.NodeID, bool, error) {
	if r.cfg.Placement != nil {
		var target rans.NodeID
		var ok bool
		if kp, is := r.cfg.Placement.(KindPlacement); is {
			target, ok = kp.PickForKind(id.Kind, r.cfg.NodeID)
		} else {
			target, ok = r.cfg.Placement.Pick(r.cfg.NodeID)
		}
		if ok && target != 0 {
			// 放置正缓存（H14 决策 25）：选点结果回填短 TTL 正条目——
			// 覆盖「选点 → 目标端注册可见」的激活窗；错误命中（激活失败/
			// 重定向提示/死 owner 判定）即时 Invalidate。
			if r.cfg.LocationCache != nil {
				r.cfg.LocationCache.PutPositiveTTL(id,
					directory.Owner{Node: target, Epoch: placementHintEpoch},
					placementCacheTTL)
			}
			return target, true, nil
		}
	}
	return 0, false, nil
}

// placementHintEpoch 放置正条目的代际占位（0=未知——有效性判定对 epoch 0
// 只按存活校验；激活窗内条目很快被真实归属覆写）。
const placementHintEpoch = 0

// placementCacheTTL 放置正缓存窗口：只需覆盖「选点 → 目录注册可见」的
// 激活窗（首个请求到达目标端完成注册），5s 常量集中定义。
const placementCacheTTL = 5 * time.Second

// outboundCausal 因果路径出站挂点（H12 决策 14/15，本地与远端两腿统一在
// CallOpts 分岔前执行一次）：ctx 无因果路径 = 顶层链起源——生成根 trace 并
// 携空路径；ctx 携带实体身份（Handle 内发出的 Call）→ 追加自身哈希；深度
// 达上限 → 降级直通不再追加（接受性降级）+ causal_capped 计数。Tell 不经
// 此（非等待边，见 buildRequest）。
func (r *Remote) outboundCausal(ctx context.Context) context.Context {
	cp := ctxkey.CausalFrom(ctx)
	if cp == nil {
		cp = &ctxkey.CausalPath{Root: newTraceID()}
	}
	kind, key, hasSelf := ctxkey.SelfFrom(ctx)
	if !hasSelf || len(cp.Path) >= wire.MaxCausalDepth {
		if hasSelf {
			r.causalCapped.Add(1)
			log.Warn("rans/rpc: causal path depth cap hit, append skipped (passthrough)",
				"depth", len(cp.Path), "cap", wire.MaxCausalDepth)
		}
		return ctxkey.WithCausal(ctx, cp)
	}
	// 追加自身哈希（复制新序列——Path 只读共享，不得原地改写）。
	np := make([]uint64, len(cp.Path)+1)
	copy(np, cp.Path)
	np[len(cp.Path)] = rans.ID{Kind: kind, Key: key}.Hash()
	return ctxkey.WithCausal(ctx, &ctxkey.CausalPath{Root: cp.Root, Path: np})
}

// traceFallback 熵源失败的回退序（never-empty 契约——根 trace 空串无意义）。
var traceFallback atomic.Uint64

// newTraceID 生成根 trace 标识（H12 决策 14：随机形态，实现自洽）：8 随机
// 字节的 16 位 hex；生成频率 = 每条链的顶层调用一次（下游继承过线）。熵源
// 失败回退进程内单调序（可辨识、不回绕复用）。
func newTraceID() string {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return fmt.Sprintf("fallback-%016x", traceFallback.Add(1))
	}
	return fmt.Sprintf("%016x", binary.LittleEndian.Uint64(b[:]))
}

// buildRequest 构造出站 Request/OneWay 帧：值编码进外封（Envelope），header
// 置 TargetUUID+SourceNodeID（陷阱 4：回程按 SourceNodeID 寻址，此处即回程
// 地址的填写点）、MessageID 取网关单调序（陷阱 7）。扩展块按需写：两类帧
// 通写的 budget_ms（H10 决策 5）、仅 Request 书写的因果路径（ctx 携带值原样
// 过线——追加/生成已在出站挂点完成）+ trace_id 同源双载、传播键值项（H10
// 决策 8）。Tell 不写因果路径与 trace_id：非等待边不入链、不携带调用方路径
// （否则 Tell 目标的后续 Call 沾上调用方祖先哈希，诱发准入误判环），Tell
// 目标是新的链起源。body 为新分配缓冲，随发送成功移交连接。
func (r *Remote) buildRequest(ctx context.Context, typ protocol.MessageType, id rans.ID, req any) (wire.Header, []byte, error) {
	typeName, payload, err := codec.Encode(req)
	if err != nil {
		return wire.Header{}, nil, fmt.Errorf("rans: encode: request value: %w", err)
	}
	flags := protocol.FlagHasTargetUUID | protocol.FlagHasSourceNodeID
	hdr := wire.Header{
		Type:         typ,
		Flags:        flags,
		MessageID:    r.gw.NextMessageID(),
		SourceNodeID: uint16(r.cfg.NodeID),
		TargetKind:   id.Kind,
		TargetInst:   id.Key,
	}
	var ext wire.RequestExt
	if dl, ok := ctx.Deadline(); ok {
		rem := time.Until(dl)
		if rem < 0 {
			rem = 0 // 已过线的请求如实写 0：收端出队检查立即过期即弃
		}
		ext.BudgetMS, ext.HasBudget = uint64(rem.Milliseconds()), true
	}
	if typ == protocol.MsgRequest {
		if cp := ctxkey.CausalFrom(ctx); cp != nil {
			ext.Causal = cp
			ext.TraceID, ext.HasTraceID = cp.Root, true
		}
	}
	items, drops := ctxkey.PackPropagationForWire(ctx)
	r.notePropDrops(drops)
	for _, it := range items {
		ext.Ctx = append(ext.Ctx, wire.CtxItem{Name: it.Name, Value: it.Value})
	}
	if ext.HasBudget || ext.Causal != nil || len(ext.Ctx) > 0 {
		hdr.Ext = wire.EncodeRequestExt(ext)
		hdr.Flags |= protocol.FlagHasExtensions
	}
	return hdr, wire.MarshalEnvelope(typeName, payload), nil
}

// decodeResponse 解码 Response body 并归还池化页（陷阱 1：解码后 Put，
// 失败路径同样归还，勿泄漏）。DecodeValue 的产物不引用页内缓冲（注册表
// 值自带拷贝语义，[]byte 序列化器显式拷出），Put 后可安全持有返回值。
func (r *Remote) decodeResponse(page []byte) (any, error) {
	env, err := wire.UnmarshalEnvelope(page)
	if err != nil {
		r.pool.Put(page)
		return nil, fmt.Errorf("rans: decode: response envelope: %w", err)
	}
	v, err := codec.Decode(env.TypeName, env.Payload)
	r.pool.Put(page)
	if err != nil {
		return nil, fmt.Errorf("rans: decode: response value (type %q): %w", env.TypeName, err)
	}
	return v, nil
}
