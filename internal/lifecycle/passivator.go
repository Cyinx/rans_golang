package lifecycle

// 闲置视图与闲置钝化（规格 §14.3 E2/G1；Idle→Passivate 路径经
// *activation.Table 协作）：
// 候选扫描与钝化（passivate 模式）复合体，以及 IdleCollector 的接线面。
//
// 冷热判据是 pawnite.LastDeliveryTime（规格 §4.1 闲置时钟）：
//   - 从未投递（零值时钟）视为最冷——激活后从未收到请求的实体就是无
//     流量实体，排在候选序首位；
//   - 候选 = Active 且 kind 已登记策略且闲置时长严格超过策略阈值；
//   - IdleEntities 的 minIdle 形参是本轮调用的全局下限（策略阈值之外
//     再抬一道门槛），不用于放宽任何策略。

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// errPassivateNoStore 有状态实体未配置 Store 时钝化被拒（《加固 spec》§5.1 第 3
// 点表：无落点的状态不得钝化——闲置收集编排据此跳过、下轮再试）。
var errPassivateNoStore = errors.New("rans: passivate: refusing stateful entity without checkpoint store")

// IdleEntities 返回当前闲置候选（最冷优先：LastDeliveryTime 升序，从未
// 投递最冷）。候选判据：本地 Active + kind 已登记策略 + 闲置时长严格
// 超过策略阈值，且同时超过 minIdle 全局下限（minIdle ≤0 不设限；从未
// 投递者零值时钟=极大闲置，任何下限都拦不住——规格 §4.1 tick 语义）。
// 快照语义：返回后注册表的演化不影响调用方。
func IdleEntities(tbl *activation.Table, minIdle time.Duration) []rans.ID {
	return idleScan(tbl, -1, minIdle, 0)
}

// idleScan 闲置候选扫描（各消费面的共同实现）：
//   - modeFilter 指定 CollectMode 时只取该模式候选，<0 取全部模式；
//   - minIdle 全局下限（≤0 不设限）；
//   - maxCount >0 时截断至该数量（已按最冷优先排序后截）。
//
// 实现次序：idleMu 快照策略表（Table.IdlePolicy 查询面）→ readMu 快照实例
// 引用（Table.Locals）→ 锁外读状态与时钟（p.mu/readMu/idleMu 无反序持有
// 点，统一锁序纪律）。已 Destroyed 的陈旧条目按非 Active 排
// 除（其清理由 GetOrActivate/GetLocal 既有路径闭环）。
func idleScan(tbl *activation.Table, modeFilter activation.CollectMode, minIdle time.Duration, maxCount int) []rans.ID {
	type policy struct {
		after time.Duration
		mode  activation.CollectMode
	}
	policies := make(map[string]policy)
	for _, id := range tbl.LocalIDs() {
		if _, seen := policies[id.Kind]; seen {
			continue
		}
		after, mode, ok := tbl.IdlePolicyFor(id.Kind)
		if ok {
			policies[id.Kind] = policy{after: after, mode: mode}
		}
	}
	if len(policies) == 0 {
		return nil
	}

	type snap struct {
		id rans.ID
		p  *pawnite.Pawnite
	}
	locals := tbl.Locals()
	snaps := make([]snap, 0, len(locals))
	for _, loc := range locals {
		snaps = append(snaps, snap{id: loc.ID, p: loc.Pawn})
	}

	now := time.Now()
	type candidate struct {
		id   rans.ID
		last time.Time
	}
	var cands []candidate
	for _, s := range snaps {
		pl, ok := policies[s.id.Kind]
		if !ok {
			continue // 无策略 kind：永不闲置
		}
		if modeFilter >= 0 && pl.mode != modeFilter {
			continue // 模式过滤：卸载面只取 migrate、钝化面只取 passivate
		}
		if s.p.State() != pawnite.StateActive {
			continue // 仅 Active（Destroying/Destroyed/激活中不参与）
		}
		last := s.p.LastDeliveryTime()
		idleFor := now.Sub(last) // 零值时钟（从未投递）= 巨大闲置 → 最冷
		if idleFor <= pl.after {
			continue // 未越过策略阈值
		}
		if minIdle > 0 && idleFor <= minIdle {
			continue // 未越过全局下限
		}
		cands = append(cands, candidate{id: s.id, last: last})
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].last.Before(cands[j].last) })

	ids := make([]rans.ID, len(cands))
	for i, c := range cands {
		ids[i] = c.id
	}
	if maxCount > 0 && len(ids) > maxCount {
		ids = ids[:maxCount]
	}
	return ids
}

// IdleCollector 闲置钝化编排器（G1）：对枚举出的闲置候选逐个钝化。全字段
// 注入：
//
//   - Candidates 枚举面：返回待钝化候选（表驱动装配 = passivate 模式闲置
//     扫描，最冷优先）。nil → 空转；
//   - Passivate 钝化面（表驱动装配 = Passivate——快照落储与本地销毁并入
//     交接窗复合体，编排器只见单调用）。错误 → 该
//     候选不计成功数、其余候选照常（单实体失败 ≠ 一轮失败；Passivate 失败
//     即窗回退 Active，实体继续服役下轮再试）。nil → 空转。
type IdleCollector struct {
	Candidates func(maxCount int) []rans.ID
	Passivate  func(ctx context.Context, id rans.ID) error
}

// CollectIdle 执行一轮钝化：逐候选钝化（失败收集不阻断）。返回 (钝化成功
// 数, errors.Join 汇总的失败)。maxCount ≤0 → 全量候选（防御性截断：
// Candidates 返回超量时仍在 maxCount 处截断，maxCount ≤0 时不截）。
func (c *IdleCollector) CollectIdle(ctx context.Context, maxCount int) (int, error) {
	if c.Candidates == nil || c.Passivate == nil {
		return 0, nil
	}
	ids := c.Candidates(maxCount)
	if maxCount > 0 && len(ids) > maxCount {
		ids = ids[:maxCount]
	}
	var (
		n    int
		errs []error
	)
	for _, id := range ids {
		if err := c.Passivate(ctx, id); err != nil {
			errs = append(errs, fmt.Errorf("idle collect: passivate %s: %w", id.String(), err))
			continue
		}
		n++
	}
	return n, errors.Join(errs...)
}

// NewIdleCollector 装配闲置钝化编排器（G1）：枚举面 = passivate 模式闲置
// 扫描（最冷优先、maxCount 截断），钝化面 = Passivate（快照+销毁两段式
// 并入交接窗复合体
// ——快照落库/失败中止/终局次序由 Passivate 的 HandoffPassivate 窗一体
// 承担，编排器只见单调用）。宿主/测试按需调用 CollectIdle（无自持
// goroutine）；周期搭载（如随心跳扫描）归宿主接线。
func NewIdleCollector(tbl *activation.Table) *IdleCollector {
	return &IdleCollector{
		Candidates: func(maxCount int) []rans.ID {
			return idleScan(tbl, activation.CollectPassivate, 0, maxCount)
		},
		Passivate: func(ctx context.Context, id rans.ID) error { return Passivate(ctx, tbl, id) },
	}
}

// CollectIdle 一轮闲置钝化的便捷直调形态（NewIdleCollector(tbl).CollectIdle
// 的等价捷径）。返回 (钝化成功数, 汇总失败)。
func CollectIdle(ctx context.Context, tbl *activation.Table, maxCount int) (int, error) {
	n, err := NewIdleCollector(tbl).CollectIdle(ctx, maxCount)
	if n > 0 {
		log.Info("rans/activation: idle collection passivated entities", "count", n)
	}
	return n, err
}

// Passivate 钝化本地实体（《加固 spec》§5.1 G1 的公共调用面）：
// 开 Handoff(HandoffPassivate) 交接窗并以 passivateCore 裁决——有状态实体
// 先等在飞周期 checkpoint 落定、再拍终快照落 Store（快照先于用户
// Deactivate），成功即终局 Destroyed（在箱按确定未执行回绝、目录注销、
// 本地逐出）；下次访问重激活从 Store 恢复。失败（快照捕获/Put 失败、无
// Store 的有状态实体、ctx 预算耗尽于等待段）返回错误——窗回退 Active，
// 实体继续服役，调用方可重试（闲置收集循环下轮自然再试）。
//
// 边界面：
//   - 不在册身份（未激活/已终局逐出）no-op 返回 nil——与 DestroyLocal 的
//     「不在册 no-op」同口径；闲置候选在扫描与钝化之间被他路销毁/迁移
//     收口是常态竞态，不以错误刷屏；
//   - 实体无快照能力面（未实现 rans.Snapshotter）→ 无状态直达终局，无须
//     Store；空快照同理（无内容可落）；
//   - ctx 是窗预算（等待段与 Store IO 共用），耗尽返回 ctx.Err()，窗由
//     pawnite 自驱收束。
//
// 组合规则（pawnite.Handoff）：窗内再请求 Passivate/Migrate → ErrNotActive；
// 窗内 Retire（DestroyLocal 族）等待本窗结果而非抢占。
func Passivate(ctx context.Context, tbl *activation.Table, id rans.ID) error {
	if err := passivateInstance(ctx, tbl, id); err != nil {
		if errors.Is(err, errHandoffNotLocal) {
			return nil // 不在册：no-op（DestroyLocal 口径；候选消失的常态竞态）
		}
		return err
	}
	return nil
}

// passivateInstance 钝化本地实体（《加固 spec》§5.1 G1 的交接窗形态；包内驱动面）：
// 开 HandoffPassivate 窗并以 passivateCore 裁决。成功返回 nil——终快照已落、
// 目录已注销、在箱已回绝、终局 Destroyed（ pawnite 侧次序收尾在裁决回传前
// 完成）；失败返回 core 自报错误——窗回退 Active，实体继续服役，下轮再试。
// ctx 是窗预算（等待段与 core 的 Store IO 共用）。
func passivateInstance(ctx context.Context, tbl *activation.Table, id rans.ID) error {
	p, ent, err := handoffTarget(tbl, id)
	if err != nil {
		return err
	}
	return p.Handoff(ctx, pawnite.HandoffPassivate, 0, func(cctx context.Context) pawnite.HandoffOutcome {
		return passivateCore(cctx, tbl, id, ent, p)
	})
}

// passivateCore 钝化核心（《加固 spec》§5.1 第 3 点表 Passivate 行）：运行在 pawnite
// loop goroutine 上（静默点——无在途 Handle，快照一致；Snapshot
// 的 loop 串行契约由 core 所在 goroutine 直接满足，无须 CaptureOnLoop）。
// 返回 Err != nil 即中止：窗回退 Active、实体继续服役，下轮再试。失败语义：
//   - 有状态 + 快照捕获/Put 失败（捕获失败按 Put 失败处理）→ 中止——状态
//     未落地不得钝化；
//   - 有状态（有内容可落）无 Store → 拒绝钝化；
//   - 空快照 → 无内容可落，直接成功（裸激活语义——下次访问重置，状态
//     丢弃是实体自身的既定契约）；
//   - 无状态 → 直接成功。
func passivateCore(ctx context.Context, tbl *activation.Table, id rans.ID, ent rans.Entity, p *pawnite.Pawnite) pawnite.HandoffOutcome {
	if err := awaitSettled(ctx, tbl, p, "passivate"); err != nil {
		return pawnite.HandoffOutcome{Err: err}
	}
	carrier, stateful := ent.(rans.Snapshotter)
	if !stateful {
		return pawnite.HandoffOutcome{} // 无状态：直接成功
	}
	snap, err := carrier.Snapshot()
	if err != nil {
		return pawnite.HandoffOutcome{Err: fmt.Errorf("rans: passivate: snapshot capture failed: %w", err)}
	}
	if len(snap) == 0 {
		return pawnite.HandoffOutcome{} // 空快照：无内容可落
	}
	store := tbl.SnapshotStore()
	if store == nil {
		return pawnite.HandoffOutcome{Err: errPassivateNoStore}
	}
	// 围栏：Put 携带本 incarnation 纪元（p 与快照同源实例）。
	if err := store.Put(ctx, id, p.Incarnation(), snap); err != nil {
		return pawnite.HandoffOutcome{Err: fmt.Errorf("rans: passivate: snapshot put failed: %w", err)}
	}
	return pawnite.HandoffOutcome{}
}
