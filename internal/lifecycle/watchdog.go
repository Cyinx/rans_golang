package lifecycle

// 卡死看门狗（规格 §14.2 三看门狗）：
// 周期扫描本地实体注册表，钉死三类停驻——
//   - 卸载卡死：state==Destroying 停驻超时（默认 60s）→ AbandonStuckDeactivation
//   - 激活卡死（Go 移植补充的第三形态）：state==Activating 停驻超时
//     （默认 60s）→ AbandonStuckActivation——本节点粘性请求黑洞的唯一收敛面
//   - 执行卡死：state==Active 且顶层执行停驻超时（默认 120s）→ AbandonStuckExecution
//
// 编排器与表复合体（经 *activation.Table 协作）：
//   - Watchdog：全回调注入的扫描编排器（Entities 返回快照、Abandon* 是
//     表复合体强弃面）；快照数据可能陈旧——强弃方法在 pawnite gate 内
//     重验状态（陷阱 12 让位），陈旧裁决零副作用；
//   - WatchdogView：把本地注册表枚举为扫描快照；
//   - AbandonStuck*：pawnite 强弃（gate 内单点置终态 + 在箱/在途解绑 +
//     取消在飞钩子（hookCtx 取消面——宿主 Deactivate/Activate
//     应消费 ctx 尽早返回，缩小强弃后新 incarnation 与 zombie 的并发窗
//     口））+ 终局清理（条件目录注销 + 本地逐出，经 pawnite OnEvict 的
//     cleanup 闭环）+ 留痕；
//   - NewWatchdog：把枚举面/强弃面接线到指定表的便捷装配函数。
//
// 单写者纪律（陷阱 12）：复合体只由看门狗扫描线程调用（watchdog.Run →
// Scan → 本方法）；与常规 Destroy 的竞态由 pawnite gate 让位（返回 false
// 即零副作用）。zombie 宿主侧效应仅靠取消面收敛（协作宿主），非协作宿主
// 不设防（硬围栏面属 checkpoint/迁移的 Store 纪元围栏，不在本面）。

import (
	"context"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// 看门狗缺省参数（规格附录 A：卸载/执行/激活 60s/120s/60s——激活为 Go
// 移植补充，与卸载同值；周期 1s——测试/生产随节点规模调整）。
const (
	DefaultWatchdogInterval  = time.Second
	DefaultUnloadTimeout     = 60 * time.Second
	DefaultExecTimeout       = 120 * time.Second
	DefaultActivationTimeout = 60 * time.Second
)

// WatchdogParams 单一类别的停驻阈值（KindOverrides 表项）。零值 = 未覆盖
// （回落全局值）。
type WatchdogParams struct {
	UnloadTimeout     time.Duration
	ExecTimeout       time.Duration
	ActivationTimeout time.Duration
}

// WatchdogConfig 看门狗参数：全局阈值 + per-kind 覆盖（预留：agent 消费方
// 放宽 LLM 轮长等慢实体）。零/负值一律回落缺省。
type WatchdogConfig struct {
	Interval          time.Duration
	UnloadTimeout     time.Duration
	ExecTimeout       time.Duration
	ActivationTimeout time.Duration
	KindOverrides     map[string]WatchdogParams
}

// WatchdogEntity 扫描快照：枚举时刻的状态形状与时钟。状态只取看门狗
// 关心的四态（Activating / Active 执行中 / Destroying / 其余不管）；
// 时钟零值 = 未知停驻起点（防御性跳过，不构成强弃依据）。
type WatchdogEntity struct {
	ID rans.ID
	// Kind 是 KindOverrides 查找键（rans.ID.Kind）。
	Kind            string
	Active          bool
	Destroying      bool
	Activating      bool
	ExecutingSince  time.Time // 零 = 空闲
	DestroyingSince time.Time // 零 = 不在 Destroying
	ActivatingSince time.Time // 零 = 不在 Activating
}

// Watchdog 三看门狗编排器。全字段可注入：nil 枚举/强弃面 → 对应阶段空转。
type Watchdog struct {
	// Entities 枚举面：返回全部待扫描实体快照。nil → Scan 空转。
	Entities func() []WatchdogEntity
	// AbandonDeactivation 卸载卡死强弃（表复合体：pawnite
	// Abandon(StateDestroying) + 条件目录注销 + 本地逐出）。返回是否
	// 真正强弃（gate 让位时 false）。nil → 跳过。
	AbandonDeactivation func(id rans.ID) bool
	// AbandonActivation 激活卡死强弃（第三形态，表复合体：
	// pawnite Abandon(StateActivating) + cleanup 幂等兜底）。返回是否真正
	// 强弃（gate 让位时 false）。nil → 跳过。
	AbandonActivation func(id rans.ID) bool
	// AbandonExecution 执行卡死强杀（表复合体：pawnite
	// Abandon(StateActive) + 条件目录注销 + 本地逐出）。nil → 跳过。
	AbandonExecution func(id rans.ID) bool
	// Config 参数（零值字段回落缺省）。
	Config WatchdogConfig
}

// NewWatchdog 装配三看门狗（规格 §14.2）：枚举面/强弃面接线指定表，
// 参数由 cfg 提供（零值字段回落缺省）。宿主负责 Run 的生命周期
// （ctx 随进程/停机编排）。
func NewWatchdog(tbl *activation.Table, cfg WatchdogConfig) *Watchdog {
	return &Watchdog{
		Entities:            func() []WatchdogEntity { return WatchdogView(tbl) },
		AbandonDeactivation: func(id rans.ID) bool { return AbandonStuckDeactivation(tbl, id) },
		AbandonActivation:   func(id rans.ID) bool { return AbandonStuckActivation(tbl, id) },
		AbandonExecution:    func(id rans.ID) bool { return AbandonStuckExecution(tbl, id) },
		Config:              cfg,
	}
}

// effectiveParams 解析 kind 的生效阈值：全局（≤0 回落缺省）上叠加
// per-kind 覆盖（覆盖表内 ≤0 的分量视为未覆盖）。
func (w *Watchdog) effectiveParams(kind string) WatchdogParams {
	p := WatchdogParams{
		UnloadTimeout:     w.Config.UnloadTimeout,
		ExecTimeout:       w.Config.ExecTimeout,
		ActivationTimeout: w.Config.ActivationTimeout,
	}
	if p.UnloadTimeout <= 0 {
		p.UnloadTimeout = DefaultUnloadTimeout
	}
	if p.ExecTimeout <= 0 {
		p.ExecTimeout = DefaultExecTimeout
	}
	if p.ActivationTimeout <= 0 {
		p.ActivationTimeout = DefaultActivationTimeout
	}
	if ov, ok := w.Config.KindOverrides[kind]; ok {
		if ov.UnloadTimeout > 0 {
			p.UnloadTimeout = ov.UnloadTimeout
		}
		if ov.ExecTimeout > 0 {
			p.ExecTimeout = ov.ExecTimeout
		}
		if ov.ActivationTimeout > 0 {
			p.ActivationTimeout = ov.ActivationTimeout
		}
	}
	return p
}

// Scan 单轮扫描：逐实体按停驻形状分派强弃。次序 Destroying → Activating →
// Active+执行（状态互斥，防御性固定次序）；单个候选的分派异常不影响其余
// （强弃面自保证；本方法无 panic 面）。可独立调用（确定性测试面），Run 即
// 周期循环本方法。
func (w *Watchdog) Scan() {
	if w.Entities == nil {
		return
	}
	now := time.Now()
	for _, e := range w.Entities() {
		ps := w.effectiveParams(e.Kind)
		if e.Destroying {
			// 时钟零值（未知停驻起点）跳过——不构成强弃依据。
			if !e.DestroyingSince.IsZero() && now.Sub(e.DestroyingSince) > ps.UnloadTimeout {
				if w.AbandonDeactivation != nil {
					w.AbandonDeactivation(e.ID)
				}
			}
			continue
		}
		if e.Activating {
			if !e.ActivatingSince.IsZero() && now.Sub(e.ActivatingSince) > ps.ActivationTimeout {
				if w.AbandonActivation != nil {
					w.AbandonActivation(e.ID)
				}
			}
			continue
		}
		if e.Active && !e.ExecutingSince.IsZero() && now.Sub(e.ExecutingSince) > ps.ExecTimeout {
			if w.AbandonExecution != nil {
				w.AbandonExecution(e.ID)
			}
		}
	}
}

// Run 周期扫描直至 ctx 取消。Interval ≤0 回落 DefaultWatchdogInterval；
// 首轮在 Interval 到点后（排空/看门狗是纠正面而非拦截面，无需立即扫）。
func (w *Watchdog) Run(ctx context.Context) {
	interval := w.Config.Interval
	if interval <= 0 {
		interval = DefaultWatchdogInterval
	}
	runPeriodic(ctx, interval, func(context.Context) { w.Scan() })
}

// WatchdogView 把本地注册表枚举为扫描快照（规格 §14.2 扫描面）：先经
// Table.Locals 快照实例引用，锁外读状态与时钟（p.mu/readMu 无反序持有
// 点）。已 Destroyed 的陈旧条目一并报出（三态全 false——看门狗天然忽略；
// 其清理由 GetOrActivate/GetLocal 的既有路径闭环）。停驻时钟取自统一状态
// 进驻时钟 StateSince，按观测到的状态分支填入对应快照字段（状态与时钟两
// 次读之间的迁移最坏表现为「时钟陈旧/零值」，看门狗跳过 + 强弃 gate 重验
// 兜底，同快照陈旧语义）。
func WatchdogView(tbl *activation.Table) []WatchdogEntity {
	locals := tbl.Locals()
	ents := make([]WatchdogEntity, 0, len(locals))
	for _, loc := range locals {
		p := loc.Pawn
		st := p.State()
		since := p.StateSince()
		e := WatchdogEntity{
			ID:             loc.ID,
			Kind:           loc.ID.Kind,
			Active:         st == pawnite.StateActive,
			Destroying:     st == pawnite.StateDestroying,
			Activating:     st == pawnite.StateActivating,
			ExecutingSince: p.ExecutingSince(),
		}
		if e.Destroying {
			e.DestroyingSince = since
		}
		if e.Activating {
			e.ActivatingSince = since
		}
		ents = append(ents, e)
	}
	return ents
}

// AbandonStuckDeactivation 卸载卡死复合强弃（规格 §14.2）：Destroying
// 停驻超 unload 超时的实体（典型：Deactivate 永阻塞）。
// pawnite 强弃成功 ⟹ 终局清理已由其 OnEvict（→cleanup：条件目录注销——
// 迁移赢家经 SkipDirUnregister 跳过——+ 身份守卫逐出 + onEvicted 钩子）
// 同步完成；此处的 cleanup 重放是幂等兜底（Once + 身份守卫），覆盖
// OnEvict 未接线的裸 pawnite 形态。返回 false = gate 让位或实体不在册
// （零副作用）。
func AbandonStuckDeactivation(tbl *activation.Table, id rans.ID) bool {
	return abandonStuck(tbl, id, pawnite.StateDestroying,
		"watchdog abandoned stuck deactivation")
}

// AbandonStuckActivation 激活卡死复合强弃（规格 §14.2 第三形态）：
// Activating 停驻超激活超时的实体（典型：Activate 钩子/loader IO
// 永阻塞）。强弃把卡死实例逐出本地表——解除「GetLocal 粘性命中、消息
// 堆积至上限」的请求黑洞。目录面（register-first）：注册先于装载
// ——装载期卡死时注册已落位，强弃的终局清理（OnEvict→cleanup）以实例
// 标记 {node, inc} 条件注销；注册 IO 在飞期间被强弃的交错由激活闭包的
// 注册后让位守卫就地注销兜底。清理闭环同 AbandonStuckDeactivation。
func AbandonStuckActivation(tbl *activation.Table, id rans.ID) bool {
	return abandonStuck(tbl, id, pawnite.StateActivating,
		"watchdog abandoned stuck activation (sticky black hole relieved)")
}

// AbandonStuckExecution 执行卡死复合强杀（规格 §14.2）：Active 且顶层
// 执行停驻超 exec 超时的实体（典型：handler 永阻塞）。强弃后实体按孤儿
// 处置（zombie 续体效果丢弃），下次激活从 checkpoint/Store 恢复。
// 清理闭环同 AbandonStuckDeactivation。
func AbandonStuckExecution(tbl *activation.Table, id rans.ID) bool {
	return abandonStuck(tbl, id, pawnite.StateActive,
		"watchdog force-abandoned stuck execution")
}

// abandonStuck 三形态强弃复合体的共用骨架：
// PeekLocal → pawnite 强弃 gate（Abandon(expect, TerminalAbandoned) 单点
// ——spec §4.2 内部表；gate 让位即零副作用返回 false）→ 终局
// 清理（FinalizeEviction：幂等兜底，覆盖 OnEvict 未接线的裸 pawnite 形态）
// → 留痕。单写者纪律（陷阱 12）不变：仅看门狗
// 扫描线程调用。
func abandonStuck(tbl *activation.Table, id rans.ID, expect pawnite.State, event string) bool {
	p, ok := tbl.PeekLocal(id)
	if !ok {
		return false
	}
	if !p.Abandon(expect, pawnite.TerminalAbandoned) {
		return false
	}
	tbl.FinalizeEviction(id, p)
	log.Warn("rans/activation: "+event, "uuid", id.String())
	return true
}
