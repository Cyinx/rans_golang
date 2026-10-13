// 看门狗强弃单点：Abandon 三形态 gate 与在途投递解绑（终局效应单胜）。
package pawnite

import (
	"time"

	"github.com/cyinx/rans_golang"
)

// Abandon 看门狗强弃单点（spec §4.2 内部表——《加固 spec》H8
// 三导出形态收敛于此；规格 §14.2 + §19 陷阱 12 单写者纪律）。gate（mu）
// 内单临界区完成全部状态面：
//
//	expect 校验 →（Active 形态）执行时钟复检 + Active→Destroying→
//	Destroyed 双转移（同临界区，Destroying 中间态不可观察）→
//	setTerminalLocked(reason)（写一次：先到的终局原因权威——迁移赢/
//	仲裁输已写时强弃原因让位，迟到处置仍按先到原因分流；交接窗内
//	升级为 AbandonedInHandoff——在箱消息确定未执行）+
//	abandoned=true（无条件：强弃终局化事实，与 terminal 写一次分属
//	两个维度——见字段注释）→ handoffPending=false（恢复准入回绝
//	语义/防御性清零）→ state=Destroyed（立即，不等 zombie）→ 在箱
//	快照 → stop 关闭（未关时——zombie loop 下一轮即让位退出）
//
// 锁外次序：**取消在飞钩子（hookCtx cancel + 在飞留痕——先于
// 其余处置，zombie 尽早收信号；交接 core 经取消并集同样收到信号）** →
// 在箱滞留者处置（disposeLate 终局矩阵——先到的
// Migrated 转投目标节点；AbandonedInHandoff/Retired 权威时 Unavailable
// 回绝确定未执行者、Abandoned 权威时 Timeout 回绝解绑悬挂调用方）→
// 在飞捕获回输 → 在飞交接信号回输（Handoff/Migrate 调用方解绑）
// → 在途投递解绑（executing 槽换出回绝）→ 终局效应（OnEvict，evictOnce
// 单胜）→ done 关闭（解绑 Destroy 等待方）。
//
// zombie 续体无害化（限定口径）：卡死的 Deactivate/Activate/
// invoke/迟到处置回调 事后返回时，loop 侧续体（stop 分支/激活失败分支）凭
// abandoned 标志让位（terminal 写一次 ≠ 强弃让位：终局原因先落
// （Migrated/ArbitrationLost）+ 强弃后胜的交错下强弃照样终局化）；已越过
// 让位检查的续体（卡在 finish 内的 Deactivate 返回、Won 分支的无条件
// finish 续体）由 finish 的全程幂等兜底（条件转移 + evictOnce 单胜 +
// closeDone 幂等）——pawnite 内部状态的续体写入全部落在孤儿实例上，
// 效果丢弃。宿主侧副作用（对 Store/外部系统的写入）不在「孤儿」内：
// 强弃同时取消在飞钩子的 ctx（协作宿主尽早返回，并发窗口从无界收敛到
// 宿主配合度界），非协作宿主的残留写入不设防。
//
// expect 三形态（规格 §14.2）：
//   - StateDestroying（卸载卡死，双看门狗之一）：Destroying 停驻超卸载
//     超时（默认 60s）——典型为 finish 阻塞在 Deactivate（safeDeactivate
//     永不返回）、销毁窗内 invoke 永不返回、或交接 core 卡死（交接窗内
//     stop 尚未关，强弃负责关掉；窗内强弃落 TerminalAbandonedInHandoff
//     ——在箱消息确定未执行）。迁移赢家箱内停驻者的处置继承 disposeLate
//     终局矩阵——转投目标节点（目录已指向目标，转投省一轮失败往返）；
//     在途投递槽在 core 卡死形态下必空（core 是 loop 任务，invoke 必先
//     于 core 完成）。
//   - StateActivating（激活卡死，第三形态——Go 移植补充的规格
//     缺口）：Activating 停驻超激活超时（默认 60s，时钟经
//     StateSince 观测）——典型为 Activate 钩子（宿主 loader IO / 用户
//     激活逻辑）永不返回。该形态下本节点成为该 id 的粘性请求黑洞：卡死
//     实例已入本地表，GetLocal 永远命中，消息堆积至收件箱上限；actCtx
//     为 WithoutCancel（防首调用者取消中止在途激活——激活预算与调用方
//     ctx 解耦的设计不变量），Destroy
//     解不开——本形态是唯一直接收敛面。胜出含仲裁落败后卡在 finish 的
//     Deactivate 的窗内（状态仍 Activating；在箱迟到者按先到的
//     ArbitrationLost 原因经矩阵转投赢家）；在飞捕获/迁移信号/在途投递
//     的回输为防御性统一接线（Activating 态必空）。目录面（register-first）：
//     注册先于装载——装载期卡死时注册已落位，终局
//     清理的实例级条件注销在 lifecycle 复合体侧接线。
//   - StateActive（执行卡死，双看门狗之二）：Active 且顶层执行停驻超
//     执行超时（默认 120s，停驻时钟经 ExecutingSince 观测）。与卸载形态
//     的关键差别：实体仍在服役，必须由本形态主动发起终局——Active→
//     Destroying→Destroyed 两次转移在同一持锁临界区完成，外部观察者绝无
//     Destroying 中间态可观察（规格「强制终态同临界区（无中间态可观察）」）；
//     gate 内复检执行时钟——快照陈旧的空闲实体（时钟为零）让位，不误杀
//     健康实体。卡死的 invoke 继续作为 zombie 运行：其返回值写入孤儿
//     reply 通道（调用方已被解绑收走 Timeout 错误，非阻塞投递使迟写
//     无害）；loop 后续迭代在 stopShutdown 让位退出，下次激活从上轮
//     checkpoint/Store 恢复。Active ⟹ 无在飞交接（Handoff 同临界区置
//     pending+Destroying）、无生命周期钩子在飞（invoke 的 ctx 归调用方
//     所有）——回输接线为防御性统一。
//
// 单写者契约（陷阱 12）：仅供看门狗扫描线程调用（lifecycle 看门狗复合体
// 的接线需要）；与常规 Destroy 的竞态在 gate 内让位——谁先把状态
// 推到终态谁赢，输家零副作用返回 false。非 expect 状态一律拒绝。
func (p *Pawnite) Abandon(expect State, reason TerminalReason) bool {
	p.mu.Lock()
	if p.state != expect {
		p.mu.Unlock()
		return false
	}
	if reason == TerminalAbandoned && p.handoffPending {
		// 交接窗内的强弃（《加固 spec》§5.1）：终局原因分流 AbandonedInHandoff——
		// 在箱消息从未执行过（core 未跑到/被取消面通知），迟到处置按
		// Unavailable 而非 Timeout。写一次保证：Won 终局已先落时本写让位，
		// 迟到处置仍按 Migrated 转投目标（转发矩阵不受影响）。
		reason = TerminalAbandonedInHandoff
	}
	if expect == StateActive {
		// 执行时钟复检（与 ExecutingSince 同源原子读）：看门狗裁决基于扫描
		// 快照，实体可能恰在快照后、本次调用前完成执行回到空闲——此刻强杀
		// 会误杀健康实体。时钟为零（空闲）即让位：无卡死可杀。原子读与 loop
		// invoke 的置位/清零无锁协调，非零即证执行仍在途。
		if p.executingSinceNano.Load() == 0 {
			p.mu.Unlock()
			return false
		}
		// Active 形态的两次转移在同一临界区完成：外部观察者绝无 Destroying
		// 中间态可观察（规格「强制终态同临界区」）。
		p.enterDestroyingLocked()
	}
	p.setTerminalLocked(reason, 0)
	p.abandoned = true       // 强弃终局化事实：无条件置位（terminal 写一次 ≠ 强弃让位，两维度分离）
	p.handoffPending = false // Destroying 形态恢复准入回绝语义/防御性清零（跟随者由 abort 结算）
	p.state = StateDestroyed
	p.stateSince = time.Time{}
	late := p.takeInboxLocked()
	closeStop := false
	if !p.stopClosed {
		// stop 尚未关时必须关掉，否则 zombie loop 出 core/invoke/Activate 后
		// 会继续消费收件箱（run 循环不查状态、只查 stop）。Active/Activating
		// 形态 stopClosed 必为假（关 stop 的各路径都先置 Destroying），条件写
		// 统一三形态；Destroying 形态销毁可能已关（不重复关）。
		p.stopClosed = true
		closeStop = true
	}
	p.mu.Unlock()

	p.cancelHooksForAbandon() // 强弃取消面：在飞钩子尽早收信号
	// 强弃取消面（H12 决策 17）：在飞 work 立即收取消信号——协作 work 快速
	// 返回，续体随终局自然落空（未完成 Responder 以调用方 Timeout 收场）。
	p.cancelWorks()

	if closeStop {
		close(p.stop)
	}
	p.disposeLate(late)
	p.drainPendingCaptures()
	p.abortPendingHandoff() // 回输尚未执行的交接信号 + 结算跟随者（Handoff 调用方解绑）
	p.unbindExecuting()
	p.runOnEvictOnce()
	p.closeDone()
	return true
}

// unbindExecuting 换出在途投递槽并按 Timeout 回绝（解绑悬挂调用方；
// ReasonNone——在途可能已部分执行，规格只承诺不重复）。换出（Swap）与
// invoke 出口的清零互斥协调：恰一方持有投递引用——强弃先换到则 invoke
// 出口清 nil 空转；invoke 先完成则换到 nil（投递已正常收场，回绝被投递方
// 的单槽非阻塞语义丢弃，无害）。不持 mu 调用（Reject 是回调，陷阱 2：锁内
// 禁回调）。
func (p *Pawnite) unbindExecuting() {
	if d := p.executing.Swap(nil); d != nil {
		(*d).Reject(rans.CodeTimeout, rans.ReasonNone)
	}
}
