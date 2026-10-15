package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/lifecycle"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/routing"
	"github.com/cyinx/rans_golang/internal/wire"
)

// Start 缺省监听地址与心跳参数（规格 §10.2/附录 A）。
const (
	defaultListenAddr     = "127.0.0.1:0"
	heartbeatPingInterval = 10 * time.Second
	heartbeatPongTimeout  = 30 * time.Second

	// Drain 阶段③/④参数（规格 §14.1/附录 A）。
	drainFlushPollInterval = 10 * time.Millisecond // pending 清空轮询周期
	drainUnregisterWindow  = 2 * time.Second       // 节点注销 best-effort 小超时
)

// Start 监听 cfg.Addr（缺省 127.0.0.1:0 由内核分配）并启动心跳环
// （10s ping / 30s pong 踢除）。返回前 accept 循环与心跳已在后台运行，
// Addr 即刻可查。
//
// ctx 只辖监听的建立；心跳与周期任务族挂 New 时创建的 runCtx（Shutdown/
// Close 收口）——调用方取消 Start 的 ctx 不会静默停摆节点（心跳若随调用方
// ctx 派生，取消后 pong 踢除失效，本端仅剩对端视角检测）。
//
// listen 失败即终态：内部走 Close 收口（New 已起的网关扫描器与 runCtx 一并
// 终止——不留每实例恒漏 1 goroutine 的半开组合体），失败后的 Remote 不可
// 复用，宿主重建重试。
func (r *Remote) Start(ctx context.Context) error {
	if err := r.Bind(ctx); err != nil {
		return err
	}
	r.Accept()
	return nil
}

// Bind 只绑定监听不接入（票 14 的 Start 次序拆分面）：返回实际地址并存
// Addr——供 node 在同步首写成员键（键值携带真实地址）之后才 Accept。
// 失败即终态（内部 Close 收口）。
func (r *Remote) Bind(ctx context.Context) error {
	addr := r.cfg.Addr
	if addr == "" {
		addr = defaultListenAddr
	}
	bound, err := r.conns.ListenBind(ctx, addr)
	if err != nil {
		_ = r.Close()
		return fmt.Errorf("rans: listen: node %d: %w", r.cfg.NodeID, err)
	}
	r.addr.Store(&bound)
	return nil
}

// Accept 开始接入（Bind 的后半）+ 起连接心跳（票 14：同步首写成员键之
// 后调用）。
func (r *Remote) Accept() {
	r.conns.StartAccept()
	r.conns.StartHeartbeat(r.runCtx, heartbeatPingInterval, heartbeatPongTimeout)
}

// Addr 返回绑定地址（Start 前为空串）；testkit 据此建立 resolver 表。
func (r *Remote) Addr() string {
	if p := r.addr.Load(); p != nil {
		return *p
	}
	return ""
}

// Migrate 把本地实体迁移到 placement 选出的目标节点（规格
// §13.1；转发 lifecycle.Migrate，阻塞至裁决）：placement 由组装根经
// activation.Options.Placement 构造期注入（rpc 不越权选点）；
// 快照落 Store / 随迁包直发目标节点 / 目录换锁 / 在箱整箱转发 / 输回退的
// 编排语义见 manager（迁移接线的装配点在 New，见迁移接线面注释）。
//
// 阻塞到迁移裁决或预算耗尽；并发安全；可在实体 Handle 内调用（对自身经
// 串行队列等待）。
func (r *Remote) Migrate(ctx context.Context, id rans.ID) error {
	return lifecycle.Migrate(ctx, r.cfg.Manager, id)
}

// Close 立即关停组合体（不排空；幂等）：listener + 全部连接 + 心跳环 +
// 网关 deadline 扫描器 + 周期任务族一并收口。连接关闭同步触发各节点的
// 快速失败（OnConnectionLost → FailPendingForNode），在途请求即刻以
// ConnectionLost 收场而非等满超时。本地实体不在关停范围（直接丢弃）。
// 优雅停机走 Shutdown。不阻塞在长操作上；并发安全；不得在实体 Handle 内
// 调用（终态动作）。返回值恒 nil（本面无错误来源，统一动词签名）。
func (r *Remote) Close() error {
	r.stopOnce.Do(func() {
		r.conns.Close()
		r.gw.Close()
		if r.outbox != nil { // 装配失败路径发件箱尚未就绪
			r.outbox.Close()
		}
		r.runCancel() // 收口周期任务族（看门狗/checkpoint）
	})
	return nil
}

// Shutdown 优雅停机（规格 §14.1 四段排空的装配点；停序头回调经 Config.
// ShutdownTeardown 构造期注入）：以 lifecycle.
// Drainer 组合四段——
//
//	① dispatcher 门闸（BeginDraining）：排空中本地表命中的目标照常
//	   准入（不进激活路径——排空节点零新激活；已进 Retire 交接窗的实体
//	   留箱、注销后按确定未执行回绝）；本地 miss 回 Unavailable 促调用方
//	   重路由（Unavailable 属确定未执行——分类唯一表 RetryUnconditional，
//	   调用方的环内重试自然换台）、OneWay 丢弃留痕；Response/Error/心跳
//	   不受影响，MigrationPackage 照常落缓存（迁移在途不丢包，§13.3）。
//	   不复位门闸：排空是终态动作，进程随后即停；
//	② 并行 Retire 全部本地实体（LocalIDs 枚举 + DestroyLocal——Retire
//	   交接窗：终快照生效，checkpoint 之后确认的写随排空落库）；排空预算
//	   贯通到逐实体等待段：单实体 Deactivate 卡死只在预算内挂住，预算
//	   返回后阶段③/④照常推进，卡死实体留给看门狗/其自身设备（§14.2）；
//	   条件注销由销毁闭环自带；并行度 max(8, CPU×8)，单实体失败（含预算
//	   耗尽）收集不阻断、计入返回错误面）；
//	③ 出站 flush：FlushWindow（500ms）有界窗口内轮询网关 pending 表与
//	   发件箱深度清空——在途请求的最终响应与在箱单向帧全部收口后才关门；
//	④ 停序：Config.ShutdownTeardown 登记的宿主组件收口（membership 续租/
//	   轮询——阶段②③期间照常续租）→ 目录节点注销（实现 NodeRegistry 才
//	   接线，best-effort——兜住②中注销失败的残条目）→ listener+连接+心跳
//	   环（conns.Close）→ 网关 deadline 扫描器（gw.Close）→ 周期任务族
//	   （runCancel）。
//
// 预算语义（spec §5.1 第 1 条）：ctx 是阶段②/③的预算而非总开关——排空
// 是终态动作，ctx 耗尽不回滚已排空进度、阶段④无条件执行（停机路径上的
// 停序回调不能因预算枯竭被跳过），但预算耗尽即返回包装 ctx.Err() 的错误
// （与逐实体销毁错误一并 Join），调用方据此得知排空未在预算内走完。返回
// errors.Join 的逐实体销毁错误与停序头回调错误。幂等性依赖宿主（node 以
// stopOnce 编排；重复调用以各组件 Close 的幂等性兜底）。
func (r *Remote) Shutdown(ctx context.Context) error {
	teardown := make([]func() error, 0, len(r.cfg.ShutdownTeardown)+4)
	teardown = append(teardown, r.cfg.ShutdownTeardown...)
	teardown = append(teardown, teardownErr(r.unregisterNode), teardownErr(r.conns.Close),
		teardownErr(r.gw.Close), teardownErr(r.outbox.Close),
		func() error { r.runCancel(); return nil })
	d := &lifecycle.Drainer{
		BeginDraining: r.disp.BeginDraining,
		Entities:      r.cfg.Manager.LocalIDs,
		Destroy: func(ctx context.Context, id rans.ID) error {
			// DestroyLocal 把排空预算贯通到逐实体等待段（ctx-honoring）：
			// 卡死实体按预算返回错误（计入 Join），不挂死整个停机。
			// 「单实体失败」面由 pawnite 终态语义与看门狗（§14.2）承接
			// （销毁闭环的注销失败在 cleanup 内留痕）。保持逐实体注入形态
			//（Drainer 持有 flush/停序编排）；lifecycle.Shutdown 是阶段②的
			// 复合形态（并行 DestroyLocal + Join + 空表校验），宿主不经四段
			// 编排直接收尾本地实体时可用。
			return lifecycle.DestroyLocal(ctx, r.cfg.Manager, id)
		},
		Flush: func(ctx context.Context) {
			t := time.NewTicker(drainFlushPollInterval)
			defer t.Stop()
			// 出站 flush 双面：网关 pending（请求-应答在途）+ 发件箱在箱
			//（H11 决策 11 的排空接线——显式 Tell 等单向出站入箱后由
			// worker 异步送出）。两者任一非空即继续轮询，窗口耗尽弃等。
			for r.gw.PendingCount() > 0 || r.outbox.Depth() > 0 {
				select {
				case <-ctx.Done():
					return // 窗口（或父预算）耗尽：弃等，阶段④关门兜底
				case <-t.C:
				}
			}
		},
		// 停序末位收口周期任务族（幂等 cancel，Close 再触发无害）。
		Teardown: teardown,
	}
	drainErr := d.Drain(ctx)
	return drainErr
}

// teardownErr 把无错误面的停序回调适配为统一签名的停序回调。
func teardownErr(fn func()) func() error {
	return func() error { fn(); return nil }
}

// unregisterNode 停序回调①：目录节点注销（规格 §14.1 ④ 兜底清扫）。
// 阶段②的逐实体销毁已各自条件注销（UnregisterIfOwner），此处整节点清扫
// 兜住注销失败/竞态残留的条目。best-effort：目录实现 NodeRegistry 能力
// （redis）时执行；无成员面的目录直接跳过（逐实体注销已覆盖）。独立小
// 超时——Drain ctx 可能已耗尽（注销不该因父预算枯竭被跳过），也不能因
// 目录僵死挂死停序。
func (r *Remote) unregisterNode() {
	reg, ok := r.cfg.Dir.(directory.NodeRegistry)
	if !ok {
		return
	}
	// 守卫注销需要本节点当前代际：未接线（NodeEpoch nil）或未取号（0）时
	// 跳过注销——键交给 TTL 过期（守卫档不得退化为无条件删除）。
	var epoch uint64
	if r.cfg.NodeEpoch != nil {
		epoch = r.cfg.NodeEpoch()
	}
	if epoch == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), drainUnregisterWindow) // 例外②：节点注销窗（drainUnregisterWindow 集中定义——排空 ctx 可能已耗尽，注销不该被跳过也不能因目录僵死挂死停序）
	defer cancel()
	if err := reg.UnregisterNode(ctx, r.cfg.NodeID, epoch); err != nil {
		log.Warn("rans/rpc: node unregister on drain failed",
			"self", r.cfg.NodeID, "err", err)
	}
}

// LocationCache 返回配置的位置缓存（未配置返回 nil）：宿主（testkit/
// membership）自有 ChangePoller 时经本访问器把 Unregistered 事件接到
// cache.Invalidate（钩子②的接线示范——rpc 不拥有 poller，无法代接）。
func (r *Remote) LocationCache() *routing.LocationCache {
	return r.cfg.LocationCache
}

// ---- transport.Handler（连接事件半边）----

// HandleMessage 入站应用消息移交分发器（页所有权随之转移，纪律不变）。
func (r *Remote) HandleMessage(hdr wire.Header, body []byte) {
	r.disp.HandleMessage(hdr, body)
}

// OnHandshakeComplete 对端就绪：byNode 登记/顶替已由 tcp.Manager 完成，
// 上层无需动作，留痕即可。
func (r *Remote) OnHandshakeComplete(peer rans.NodeID) {
	log.Debug("rans/rpc: peer connected", "self", r.cfg.NodeID, "peer", peer)
}

// OnConnectionLost 节点连接丢失（规格 §9.1 快速失败主干）：
// byNode 清表已由 tcp.Manager 的 connLost 完成；
// 本方法消费节点丢失事件——失效该节点的全部缓存正条目（钩子⑤）+
// FailPendingForNode 摊派取消发往该节点的在途请求（毫秒级 NodeUnavailable
// 收场，不等满请求固定超时）。superseded 连接（被替代关旧）不进本路径
// （tcp.Manager 过滤），节点仍有新连接在表。
func (r *Remote) OnConnectionLost(peer rans.NodeID) {
	log.Debug("rans/rpc: peer lost", "self", r.cfg.NodeID, "peer", peer)
	if r.cfg.LocationCache != nil {
		r.cfg.LocationCache.InvalidateNode(peer)
	}
	if r.cfg.RoutingView != nil {
		r.cfg.RoutingView.Invalidate() // 票 01：连接丢失=拓扑信号
	}
	r.gw.FailPendingForNode(peer, fmt.Errorf("rans: conn: node %d connection lost", peer))
}
