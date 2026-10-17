// 终局归一：finish/setTerminalLocked 写一次、迟到处置矩阵与优雅销毁。
package pawnite

import (
	"context"
	"time"

	"github.com/cyinx/rans_golang"
)

// finish：Deactivate 钩子 → Destroyed → 逐出回调。Reject/OnEvict 全部在锁外调用。
// 钩子 panic 由 safeDeactivate/safeOnEvict 吞掉（记日志不崩进程，
// 终态照达）。ctx 自 run 线 threading 而来——即钩子取消面 hookCtx，
// 强弃时在飞钩子得以观察到取消。
//
// 强弃幂等（§19 陷阱 12）：看门狗强弃已置 Destroyed 时，卡死的 Deactivate
// 事后返回会让本方法继续走到此处——转移改为条件化（终态不重置）、终局效应
// （OnEvict）经 evictOnce 单胜，closeDone 幂等，全程无二次副作用。
func (p *Pawnite) finish(ctx context.Context) {
	if p.opts.Deactivate != nil {
		p.safeDeactivate(ctx)
	}
	p.mu.Lock()
	if p.state != StateDestroyed {
		p.state = StateDestroyed
		p.stateSince = time.Time{}
	}
	// 终局原因缺位（优雅销毁/非仲裁的激活失败——先到的 Migrated/
	// ArbitrationLost/Abandoned 权威，写一次不覆盖）落 Retired。
	p.setTerminalLocked(TerminalRetired, 0)
	// 终局复扫：吸收与准入竞态的漏网入箱者（准入持 mu 且查状态，此处已
	// Destroyed，理论上不会再有；复扫是防御性兜底）。收集在锁内、处置在
	// 锁外（锁内禁回调），经 disposeLate 终局矩阵分流——迁移赢家/仲裁输家
	// 的迟到者转发目标（§13.2 留箱等待的收尾、§12 零丢失），其余回绝。
	late := p.takeInboxLocked()
	p.mu.Unlock()
	p.disposeLate(late)
	// 延迟应答终局收场（票 11）：未 Reply 的 Responder 立即以
	// Timeout+Deactivated 收场，迟到 Reply 由记账包装器丢弃。
	p.settleDeferred()
	p.runOnEvictOnce()
}

// setTerminalLocked 记录终局原因（调用方必须已持 mu）：全生命周期只写
// 一次——零值（未写，与 Retired 同编码）才写，已写即忽略（与 evictOnce
// 单胜同哲学：先到的终局裁决权威，后到的竞争路径让位）。各写点的次序
// 保证 Retired 零值歧义无实效：Migrated/ArbitrationLost 先于 finish 的
// Retired 写入（同一 loop goroutine 程序序）；Retired 只随终态翻转的同一
// 临界区写入，强弃 gate 在终态让位（或先于翻转以 Abandoned 落位——非零
// 值，后续 Retired 补写被忽略）。
func (p *Pawnite) setTerminalLocked(reason TerminalReason, target rans.NodeID) {
	if p.term.reason == TerminalRetired { // 零值 = 未写
		p.term.reason = reason
		p.term.target = target
	}
	p.clearTimersLocked()
}

// clearTimersLocked 终局清表（调用方必须已持 mu）：置空引用 + 停武装面，
// 不遍历条目（十万级遍历会把终局临界区拖到毫秒级并卡住准入）。本方法挂在
// setTerminalLocked 上是刻意的——五条终局路径都经那里，清表因此只有一个入口。
// 置空后的表不会再被写：任意终局写点的 state 都已（或同一临界区内即将）为
// Destroyed，而 AddTimer 在 Destroyed 上返回 0——loop-only 调用的可见性由
// 同一把 mu 串行化。
func (p *Pawnite) clearTimersLocked() {
	p.timers.byID = nil
	p.timers.entries = nil
	p.disarmTimerLocked()
}

// disposeLate 迟到投递的唯一处置点（终局矩阵，按 term.reason 分流）：
//
//	Migrated / ArbitrationLost → disposition(ds, term.target)：
//	  Forwardable 转发给目标（调用方位置是目录归属让渡前的合法观测，
//	  规格 §12/§13.2 零丢失），非 Forwardable 按 Unavailable 回绝
//	  （StaleLocation / ArbitrationLost——回绝理由可辨识终局原因）；
//	Retired（含未写零值，同语义）/ AbandonedInHandoff
//	  → Reject(Unavailable, Deactivating)（优雅销毁/交接窗强弃——消息
//	  确定未执行，可重试落新激活）；
//	ActivationFailed → Reject(Unavailable, ActivationFailed)（注册后失败
//	  属执行前确定性失败）；
//	Abandoned → Reject(Timeout, ReasonNone)（在途可能已部分执行，规格只
//	  承诺不重复）。
//
// 锁内只读 term，处置全在锁外（陷阱 2：锁内禁回调）。矩阵断言锚定
// 状态码/Reason 与转发目标。
func (p *Pawnite) disposeLate(ds []InboundDelivery) {
	if len(ds) == 0 {
		return
	}
	p.mu.Lock()
	term := p.term
	p.mu.Unlock()
	switch term.reason {
	case TerminalMigrated:
		disposition(ds, term.target, rans.ReasonStaleLocation)
	case TerminalArbitrationLost:
		disposition(ds, term.target, rans.ReasonArbitrationLost)
	case TerminalAbandoned:
		for _, d := range ds {
			d.Reject(rans.CodeTimeout, rans.ReasonNone)
		}
	case TerminalActivationFailed:
		for _, d := range ds {
			d.Reject(rans.CodeUnavailable, rans.ReasonActivationFailed)
		}
	case TerminalAbandonedInHandoff:
		for _, d := range ds {
			d.Reject(rans.CodeUnavailable, rans.ReasonDeactivating)
		}
	default: // TerminalRetired（零值同语义）：优雅销毁
		for _, d := range ds {
			d.Reject(rans.CodeUnavailable, rans.ReasonDeactivating)
		}
	}
}

// runOnEvictOnce 终局效应（OnEvict）恰一次执行（evictOnce 单胜）：
// finish 与看门狗强弃两条终局路径竞争同一守卫——OnEvict（目录注销+逐出
// 的宿主接线点）全生命周期严格一次。
func (p *Pawnite) runOnEvictOnce() {
	if p.opts.OnEvict == nil {
		return
	}
	p.evictOnce.Do(func() { p.safeOnEvict() })
}

// closeDone 幂等关闭 done（mu 内单点裁决）：run 的 defer 与看门狗强弃
// 两条路径都可能到达，恰一次真正 close。维持既有次序不变量：
// done 关闭 ⟹ OnEvict 已跑（各路径先 runOnEvictOnce 再 closeDone）。
func (p *Pawnite) closeDone() {
	p.mu.Lock()
	if !p.doneClosed {
		p.doneClosed = true
		close(p.done)
	}
	p.mu.Unlock()
}

// Destroy 优雅销毁：Destroy(ctx) error 是唯一销毁面（spec §4.2 内部表
// 承诺）。语义：
// Destroying 转移与迁移抢占（含 Start 前快路径），等待段——
//
//	select { case <-p.done: return nil; case <-ctx.Done(): return ctx.Err() }
//
// ctx 耗尽返回后本方法不再改动状态：终态推进始终由 loop 自驱（Deactivate
// 不合作阻塞时可能滞后），调用方可凭新 ctx 重试（幂等：Destroying/
// Destroyed 分支直接进入等待段；Start 前快路径就地终态，必返回 nil）。
// 迁移窗内的 Destroy 是抢占而非等待：输路径会
// 回退 Active 继续服役，只等 done 将永久挂死——handoffPending 且 core 未跑
// 时，等待段前先关 stop 让 loop 走输分支，避免只等 done 永久挂死
// （§13.1 输/取消；stopClosed 标志防重复关闭）。
// （本方法不写终快照：优雅销毁的终快照由交接窗 Retire 承担，manager 侧
// 优雅路径一律走 Retire 交接窗。）
func (p *Pawnite) Destroy(ctx context.Context) error {
	p.mu.Lock()
	if p.state == StateDestroying || p.state == StateDestroyed {
		if p.state == StateDestroying && p.handoffPending && !p.stopClosed {
			p.stopClosed = true
			close(p.stop)
		}
		p.mu.Unlock()
		return p.waitDone(ctx)
	}
	if !p.started {
		// Start 前销毁：Activate 从未运行，故不跑 Deactivate；终局归一走
		// Retired——状态转移、终局原因与在箱快照在同一次持锁内完成，
		// 处置/OnEvict/done 在锁外（与主路径同构）。
		p.enterDestroyingLocked()
		p.setTerminalLocked(TerminalRetired, 0)
		late := p.takeInboxLocked()
		p.state = StateDestroyed
		p.stateSince = time.Time{}
		p.mu.Unlock()
		p.disposeLate(late)
		p.runOnEvictOnce()
		p.closeDone()
		return nil
	}
	p.enterDestroyingLocked()
	p.stopClosed = true
	close(p.stop)
	p.mu.Unlock()
	return p.waitDone(ctx)
}

// enterDestroyingLocked 进入 Destroying 并盖状态进驻时钟（调用方必须已持 mu；
// 卸载看门狗的停驻时钟来源）。写点收敛于此：Destroy、Handoff 与执行
// 强弃共用，保证"看到 Destroying ⟹ 停驻时钟非零"。
func (p *Pawnite) enterDestroyingLocked() {
	p.state = StateDestroying
	p.stateSince = time.Now()
}

// StateSince 当前状态的进驻时刻（统一状态进驻时钟——
// Activating/Destroying 停驻判据同源）。
// 零值 = Created（未起步）或 Destroyed（已终态）。
func (p *Pawnite) StateSince() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stateSince
}
