package membership

// 成员键写入面（publish/renew/取号）与 SelfFence 记账——主状态机见
// liveness.go（票 14 拆出：文件 ≤500 行门禁）。

import (
	"context"
	"time"

	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/metrics"
)

// publishNext 写入待发布代际的成员键：成功才发布 epoch（次序不变量——
// 新激活永不携带「键缺席的代际」）；失败保留 next 下轮重试（服务端成功
// 而客户端报错时重写同值即可，不得退回旧代际续期）。换代的第二遍围栏
// （keep=新代际）清掉取号到写键窗口内以旧代际落位的实例（第一遍在
// Absent 处置时已做）。
func (l *Liveness) publishNext(ctx context.Context, addr string, kinds []string, draining bool, now time.Time) {
	if err := l.publishKey(ctx, addr, kinds, draining, l.next.Load(), now); err != nil {
		log.Warn("rans/membership: node membership key write failed, retry next tick (node may be invisible to placement until it succeeds)",
			"self", l.self, "err", err)
	}
}

// publishKey 写键并发布（publishNext/firstWriteSync 共核）：失败保留
// next、记账失败幕并上抛——调用方按异步重试或同步终态分野处置；成功才
// 发布 epoch（次序不变量）。
func (l *Liveness) publishKey(ctx context.Context, addr string, kinds []string, draining bool, nx uint64, now time.Time) error {
	err := l.reg.RegisterNode(ctx, l.self, directory.NodeInfo{
		Addr: addr, Kinds: kinds, Draining: draining, Epoch: nx,
		ProtoMin: l.protoMin, ProtoMax: l.protoMax,
	})
	if err != nil {
		l.noteWriteOutcome(false, now)
		return err
	}
	prev := l.epoch.Swap(nx)
	l.next.Store(0)
	l.noteWriteOutcome(true, now)
	if prev != 0 {
		l.fence(nx)
	}
	return nil
}

// renew 比对续期已发布代际：Renewed 记成功；Absent 即失主（成员键在本
// 节点存活期间消失——进程暂停 ≥ TTL / Redis 丢键）：本代际作废，围栏
// 全部本地实例（第一遍）后重取号写新键；Superseded 即让位（NodeID 重
// 号：更大代际已占据成员键）：停写、围栏全部、让位回调。
func (l *Liveness) renew(ctx context.Context, addr string, kinds []string, draining bool, now time.Time) {
	ep := l.epoch.Load()
	res, err := l.reg.RenewNode(ctx, l.self, directory.NodeInfo{
		Addr: addr, Kinds: kinds, Draining: draining, Epoch: ep,
		ProtoMin: l.protoMin, ProtoMax: l.protoMax,
	})
	if err != nil {
		log.Warn("rans/membership: node membership renewal failed, retry next tick",
			"self", l.self, "epoch", ep, "err", err)
		l.noteWriteOutcome(false, now)
		return
	}
	l.noteWriteOutcome(true, now)
	switch res {
	case directory.RenewRenewed:
		return
	case directory.RenewSuperseded:
		l.stopped.Store(true)
		l.metricsReg().Counter(metrics.NameMembershipSuperseded,
			"memberships taken over by a newer epoch of the same NodeID").Add(1)
		log.Error("rans/membership: superseded by a newer epoch of the same node id (duplicate NodeID?); fencing all local entities and yielding",
			"self", l.self, "epoch", ep)
		l.fence(0)
		l.onSuperseded()
	default: // RenewAbsent：一死永死——同代际复活禁止，重取号
		l.metricsReg().Counter(metrics.NameMembershipResurrected,
			"membership keys lost while alive (re-epoching, old epoch invalidated)").Add(1)
		log.Error("rans/membership: membership key lost while alive; invalidating this epoch, fencing all local entities and re-epoching",
			"self", l.self, "epoch", ep)
		l.fence(0)
		if draining {
			l.stopped.Store(true)
			return
		}
		n, aerr := l.allocEpoch(ctx, ep)
		if aerr != nil {
			log.Warn("rans/membership: re-epoch allocation failed, retry next tick",
				"self", l.self, "err", aerr)
			return
		}
		l.next.Store(n)
		l.publishNext(ctx, addr, kinds, draining, now)
	}
}

// allocEpoch 取新代际（old = 本存活期上一次使用的值）：全局计数器取号；
// 取到的号不大于 old（计数器随 Redis 丢数据回退）时取 old+1——同节点号
// 存活期内代际不得回退。
func (l *Liveness) allocEpoch(ctx context.Context, old uint64) (uint64, error) {
	n, err := l.reg.NextNodeEpoch(ctx)
	if err != nil {
		return 0, err
	}
	if n <= old {
		log.Error("rans/membership: node epoch counter regressed below the published epoch; advancing from the published value",
			"self", l.self, "got", n, "published", old)
		n = old + 1
	}
	return n, nil
}

// noteWriteOutcome 存活节拍失败记账（成员键写失败与节点代际取号失败同一
// 入账——两形态经 Warn 日志可区分，记账统一按真实时刻推进同一 SelfFence
// 时钟）；持续失败超 SelfFence 触发自围栏（成功复位失败幕；首次失败起算
// 基准；零值入参时刻按当下真实时刻入账——垃圾时刻既不得生成垃圾基线也不
// 得豁免入账；fence CAS 保证每幕恰一次；围栏钩子自备异步销毁——本循环
// 永不等待实体级工作，INV5）。
func (l *Liveness) noteWriteOutcome(ok bool, now time.Time) {
	if l.selfFence <= 0 {
		return
	}
	if now.IsZero() {
		now = time.Now() // 零值时刻按当下入账：失败既不豁免也不产生垃圾基线
	}
	if ok {
		l.fenced.Store(false)
		l.lastWriteOk.Store(now.UnixNano())
		return
	}
	last := l.lastWriteOk.Load()
	if last == 0 {
		l.lastWriteOk.Store(now.UnixNano()) // 以首失败时刻起算
		return
	}
	if now.Sub(time.Unix(0, last)) < l.selfFence {
		return
	}
	if !l.fenced.CompareAndSwap(false, true) {
		return
	}
	n := l.fence(0) // 止损销毁由钩子实现按围栏时刻捕获的实例集异步执行（INV5）
	log.Error("rans/membership: self-fence triggered (membership writes failing past threshold; fencing all local entities)",
		"self", l.self, "entities", n)
}

// NodeEpoch 返回本节点代际（启动取号的单调值，成员键全程携带）：目录条
// 目记录 {node, nodeEpoch, inc} 三元组的代际来源（激活/迁移调用点经本面
// 取值）。0 = 未取号（Start 前或取号未成功）。同节点号重启必得严格更大
// 的代际（{reg} 全局计数器保证）；优雅停机不影响计数器——重启后的新存活
// 期自然取到更大的值。
func (l *Liveness) NodeEpoch() uint64 { return l.epoch.Load() }

// Close 立即终止循环（幂等；不等待退出——在途一轮刷新，含慢目录 IO，仍
// 可能运行片刻；停序要确定性收口，如 Close 后立刻注销节点键，用 Shutdown）。
// Close 先于 Start 调用亦有效：循环首轮 select 即见关闭的 stop 通道。不
// 阻塞；并发安全；返回值恒 nil（本面无错误来源，统一动词签名）。
func (l *Liveness) Close() error {
	l.stopOnce.Do(func() { close(l.stop) })
	return nil
}

// Shutdown 优雅停机：停止并等待循环退出——返回 nil 即在途成员键写已结束
// （node 停序保证先停信号、再注销节点键）。ctx 耗尽返回 ctx.Err()。幂等；
// 阻塞到循环退出或 ctx 耗尽；不得在实体 Handle 内调用。
func (l *Liveness) Shutdown(ctx context.Context) error {
	l.Close()
	if !l.started.Load() {
		return nil // Start 未启动：无可等待者（stop 已关，迟到的 Start 首轮即退）
	}
	select {
	case <-l.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
