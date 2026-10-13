// 交接窗公共入口：Handoff/HandoffKind 与仲裁落败输家通知面。
package pawnite

import (
	"context"
	"errors"
	"fmt"

	"github.com/cyinx/rans_golang"
)

// NoteArbitrationLost 激活仲裁落败的输家通知面（register-first 语义）：
// manager 激活闭包在目录 Register 裁定落败时调用——置终局原因
// {ArbitrationLost, winner}（写一次）并立即把在箱投递整箱转发给赢家。
// 输家不运行任何用户代码（规格 INV3）：本调用点先于任何装载/Activate，钩子
// ctx 从不携带输家令牌。幂等：terminal 写一次、重复
// 调用的整箱排空扑空（空箱 no-op）——run 激活失败分支对 ArbitrationLost
// 错误的探测与此处构成防御性双入口。
func (p *Pawnite) NoteArbitrationLost(winner rans.NodeID) {
	p.mu.Lock()
	p.setTerminalLocked(TerminalArbitrationLost, winner)
	p.mu.Unlock()
	p.drainForwardTo(winner, rans.ReasonArbitrationLost)
}

// HandoffKind 交接窗类别（《加固 spec》§5.1：迁移窗泛化——三种优雅转换共用
// 「pending 先于 Destroying → 准入照收留箱 → core 即 loop 任务（静默点）
// → 终局处置按 kind 与 outcome」骨架）。
type HandoffKind int

const (
	// HandoffMigrate 迁移（规格 §13.1）：core 裁决 Won=目录换锁成功——
	// 终局整箱转发目标并跳过目录注销；输/取消回退 Active。
	HandoffMigrate HandoffKind = iota
	// HandoffPassivate 钝化：core Err==nil 即成功——终局 Destroyed（在箱
	// 按确定未执行回绝）；失败回退 Active（本轮中止，下轮再试）。
	HandoffPassivate
	// HandoffRetire 优雅退役：core Err==nil 即成功——终局 Destroyed。
	HandoffRetire
)

// HandoffOutcome 交接核心的裁决结果：Won 仅对 Migrate 有意义（目录换锁
// 成败）；Passivate/Retire 以 Err==nil 为成功（Won 恒忽略）。core panic
// 由 pawnite 折为输（Err 携带包内哨兵 errHandoffCorePanic——零值会是
// Passivate/Retire 的成功编码）。
type HandoffOutcome struct {
	Won bool
	Err error
}

// Handoff 进入交接窗并阻塞至裁决（《加固 spec》§5.1；迁移窗规格 §13.1/§13.2 的
// 泛化正本）：
//
//  1. mu 内同一临界区：state=Active 校验 → handoffPending=true →
//     state=Destroying（陷阱 3：pending 必须先于 Destroying——收件箱准入
//     依赖「看到 Destroying ⟹ 必能读到 pending」判留箱等待）；
//  2. 交接信号入队（handoffCh，同临界区——缓冲发送=队列搬移，与 Destroy
//     的 stop 关闭互斥串行化），loop 到达即执行 core——正在执行的投递必
//     先完成（core 即 loop 任务，静默点：core 执行期间无在途 Handle）；
//  3. 成功 → 终局处置按 kind（Migrate 整箱转发+跳过目录注销；
//     Passivate/Retire 在箱按确定未执行回绝），返回 nil；
//  4. 输/失败/取消 → 清窗 + 结算跟随者；销毁未介入则回退 Active，返回
//     输因——优先级链与 handoffLossError 一致（仅在窗回退路径生效）：
//     调用方预算 ctx 已耗尽 → ctx.Err()（core ctx 是预算的取消并集派生，
//     预算到点时 core 观察到的 Canceled 只是并集坍缩的产物，原样回传会把
//     「预算耗尽」伪装成「被取消」）→ core 自报 Err（有则——含 core panic
//     的折输哨兵）→ ErrMigrationLost。更外层的裁决覆盖：销毁/强弃已介入
//     （窗未回退、单位走向终局）时**无条件回 ErrMigrationLost**——覆盖
//     core 自报 Err 与预算取消（core 观察到的 Canceled 来自强弃取消面而
//     非调用方预算耗尽，原样回传有误导性）。
//
// 组合规则（规格 §5.1 第 6 点矩阵）：
//   - 窗内 Migrate/Passivate 再请求 → ErrNotActive 拒（同步）；
//   - 窗内 Retire → 登记为跟随者等待在飞结果：成功 → 返回 nil（Retire
//     视为已完成，自己的 core 不执行）。**时序如实注记**：nil 结算发生在
//     在飞交接「裁决既定」点，早于终局翻转（setTerminalLocked/finish/
//     Deactivate 随后在 loop 上完成）——拿到 nil 即查 State()/
//     SkipDirUnregister() 可能读到未翻转值；需观测终态的调用方以 Done
//     关闭为准。失败（窗回退 Active）→ 接棒自开窗执行；
//   - 任一窗内看门狗强弃 → 终局 TerminalAbandonedInHandoff（在箱按
//     Unavailable 回绝）+ core 取消面通知。
//
// ctx 预算取消由 core 观测并返回输（core 契约）；core ctx 是 hookCtx
// 派生 + 调用方预算 ctx 的取消并集。裁决单一真相源在 loop 侧
// ——等待段虽绑定同一 ctx，ctx 耗尽返回 ctx.Err() 时不改动 pawnite 状态：
// 终态推进仍由 loop 自驱（core 卡死时由看门狗强弃兜底），仅调用方解除
// 等待。
func (p *Pawnite) Handoff(ctx context.Context, kind HandoffKind, target rans.NodeID,
	core func(ctx context.Context) HandoffOutcome) error {
	if core == nil {
		return errors.New("pawnite: nil handoff core")
	}
	switch kind {
	case HandoffMigrate, HandoffPassivate, HandoffRetire:
	default:
		return fmt.Errorf("pawnite: unknown handoff kind %d", int(kind))
	}
	for {
		p.mu.Lock()
		if p.state != StateActive {
			// 组合规则的等待格：窗内 Retire 等待在飞结果（幂等跟随——
			// 同窗重复 Retire 也走此路，等的是同一个结果）。判据钉在
			// 「Destroying 且 handoffPending」的活窗组合上：终态翻转后
			//（Destroyed）到达的 Retire 无窗可等，单位已终局——直接视为
			// 已完成（幂等的终态面），不得登记为跟随者（成功路径的结算
			// 已收摊，迟到登记会永久悬挂）。
			if p.state == StateDestroying && p.handoffPending && kind == HandoffRetire {
				follow := make(chan error, 1)
				p.handoffFollowers = append(p.handoffFollowers, follow)
				p.mu.Unlock()
				select {
				case err := <-follow:
					if err == nil {
						return nil // 在飞交接裁决既定成功：Retire 视为已完成（终局翻转随后完成，见导出注记）
					}
					if cerr := ctx.Err(); cerr != nil {
						return cerr
					}
					continue // 窗回退 Active：重试开自己的窗（执行 Retire）
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if p.state == StateDestroyed && kind == HandoffRetire {
				p.mu.Unlock()
				return nil // 单位已终局（此前的交接/销毁已收口）：无事可做
			}
			p.mu.Unlock()
			return ErrNotActive
		}
		// 陷阱 3（规格 §19.3）+ §12 语义：handoffPending 必须先于
		// Destroying 落位且两者同一临界区——否则存在「已见 Destroying、
		// 尚未读到 pending」的窗口，准入会把交接窗消息误回绝，违反 §13.2
		// 留箱等待。Go 互斥锁内两写对外原子可见，书写顺序用于钉死意图。
		// 窗类别不落实例字段：随 req.kind 入队（全仓
		// 消费点都读 req.kind，实例面无读者）。
		p.handoffPending = true
		p.enterDestroyingLocked()
		req := handoffReq{
			ctx:        ctx,
			kind:       kind,
			targetNode: target,
			core:       core,
			result:     make(chan error, 1),
		}
		// 交接信号入队也在同一临界区内（缓冲发送=队列搬移，陷阱 2 允许
		// 类）：若在锁外发送，Destroy 可能在解锁与发送之间完成整个 stop
		// 收尾（abortPendingHandoff 先于入队执行）——请求滞留通道、调用方
		// 永久挂起。锁内发送则与 Destroy 的 stop 关闭互斥串行化，abort 必
		// 能取到。非阻塞证明：state==Active ⟹ 无交接在飞 ⟹ 通道必空
		//（上一请求要么已被 runHandoff 消费、要么已被 abortPendingHandoff
		// 排空；容量 1）。
		p.handoffCh <- req
		p.mu.Unlock()
		// 等待段绑定调用方预算（与 waitDone 同族）：core 卡死且交接信号
		// 已被 loop 消费时（abortPendingHandoff 只能回输仍在 handoffCh
		// 排队的信号），裁决永远不会到来——裸 <-req.result 会把调用方与
		// 看门狗收敛后的 zombie 一起永久挂死。result 容量 1 保证 zombie
		// loop 迟来的回写落缓冲即弃、不阻塞 loop；ctx 先耗尽只解除调用方
		// 等待，pawnite 终态推进仍由 loop 自驱（core 卡死时由看门狗强弃
		// 兜底）。
		select {
		case r := <-req.result:
			return r
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
