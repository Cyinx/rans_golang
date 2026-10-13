package testkit

// 不变量历史检查器（INV1「确认即持久」）：记录每个被确认的操作（id、操
// 作、确认效果量、确认时刻），在故障与转换注入后，比对最终持久状态与确认
// 历史，断言 INV1。
//
// 语义分层（INV1 原文的两款）：
//   - CheckStrict / AssertDurable（优雅转换：停机、钝化、迁移）：确认 ⟹
//     持久可见，零丢失——持久效果量必须 ≥ 每实体的最高确认效果量；
//   - CheckCrashBound / AssertCrashLoss（崩溃与崩溃形态的强弃/Kill）：
//     损失上界 = 最近一次成功 checkpoint——调用方以「崩溃前最后一次成功
//     checkpoint 承载的效果量」为基线，检查器断言基线及之前的确认全部存活，
//     并把基线之后的确认显式列为允许损失（返回 LostOp 清单，不静默）。
//
// 记录点（唯一确认定义）：调用方在 Call 成功返回处 Record——Call 返回成功
// 即「被确认」（INV1 原文的确认口径）；Seq 取实体返回的效果量（计数实体 =
// 新计数值），同实体内单调。比对单向：确认 ⊆ 持久——持久量超过确认量
//（未确认执行的模糊效果落储）不属本检查裁断（at-most-once 语义的合法
// 形态）。
//
// 测量面：DurableView 由调用方给定（经 Store 读快照，或经重激活观测）。
// 在役实体的效果量须先经周期 checkpoint（Manager.CheckpointSweep 驱动）
// 或转换自身的终快照收口落储，再比对——INV1 的「对下一次激活可见」以
// Store 承载面为准。

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// ConfirmedOp 一条被确认的操作记录（确认时刻 At 供时序归档与失败诊断）。
type ConfirmedOp struct {
	ID  rans.ID   // 实体身份
	Op  string    // 操作描述（测试自定，违约报告与 LostOp 列报用）
	Seq int64     // 实体内效果量（计数实体 = 该次执行后的累计值）
	At  time.Time // 确认时刻（Call 返回成功处）
}

// LostOp 崩溃窗中被丢弃的确认操作（CheckCrashBound 的显式损失列报——
// 例外必须可见，不静默）。
type LostOp struct {
	ConfirmedOp
}

// DurableView 持久可见效果读取面：返回实体当前持久状态承载的效果量
// （计数实体 = 快照计数值）。found=false 表示持久面无该实体条目（效果量
// 视为 0）；读取失败经 err 透传（检查不吞 IO 错误）。
type DurableView func(ctx context.Context, id rans.ID) (effect int64, found bool, err error)

// InvHistory 确认历史记录器（并发安全）：单实体多调用方、多实体并发记录
// 均安全（mu 保护追加）；Records 返回副本，外部改不动内部状态。
type InvHistory struct {
	mu  sync.Mutex
	ops map[rans.ID][]ConfirmedOp
}

// NewInvHistory 构造空历史。
func NewInvHistory() *InvHistory {
	return &InvHistory{ops: make(map[rans.ID][]ConfirmedOp)}
}

// Record 记录一次被确认的操作（记录点 = Call 成功返回处）。同 id 追加，
// 顺序即确认顺序；MaxSeq 取最大值，乱序记录不破坏比对。
func (h *InvHistory) Record(id rans.ID, op string, seq int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ops[id] = append(h.ops[id], ConfirmedOp{ID: id, Op: op, Seq: seq, At: time.Now()})
}

// IDs 返回有确认记录的全部实体身份快照（按 String() 排序——违约报告的
// 确定性）。不阻塞；并发安全；测试观测面。
func (h *InvHistory) IDs() []rans.ID {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := make([]rans.ID, 0, len(h.ops))
	for id := range h.ops {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	return ids
}

// Records 返回该实体的确认记录副本（无记录返回 nil）。
func (h *InvHistory) Records(id rans.ID) []ConfirmedOp {
	h.mu.Lock()
	defer h.mu.Unlock()
	src := h.ops[id]
	out := make([]ConfirmedOp, len(src))
	copy(out, src)
	return out
}

// MaxSeq 返回该实体的最高确认效果量（无记录为 0——「确认到 0」即无义务）。
func (h *InvHistory) MaxSeq(id rans.ID) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	var max int64
	for _, op := range h.ops[id] {
		if op.Seq > max {
			max = op.Seq
		}
	}
	return max
}

// CheckStrict 优雅转换语义的比对核（零丢失，严格）：每实体的持久可见效果
// 量必须 ≥ 最高确认效果量。任一实体违约即返回点名错误（id、确认量、持久
// 量）；读取失败透传；空历史恒过（无确认即无义务）。返回 nil = INV1 的
// 优雅款成立。阻塞到核查完成；并发安全；测试断言面。
func (h *InvHistory) CheckStrict(ctx context.Context, view DurableView) error {
	for _, id := range h.IDs() {
		want := h.MaxSeq(id)
		got, found, err := view(ctx, id)
		if err != nil {
			return fmt.Errorf("INV1 检查读持久状态失败 (id=%s): %w", id.String(), err)
		}
		if !found || got < want {
			return fmt.Errorf("INV1 零丢失违约（优雅转换后确认必须持久可见）: id=%s 已确认效果量至 %d, 持久可见 %s",
				id.String(), want, durableDesc(found, got))
		}
	}
	return nil
}

// CheckCrashBound 崩溃窗例外语义的比对核（损失上界 = 最近一次成功
// checkpoint）：baseline 是「崩溃前最后一次成功 checkpoint 承载的效果量」
// per id（缺省 0 = 无承诺，全部确认可失——新鲜身份语义）。断言基线及
// 之前的确认全部存活（基线被击穿 = 违约：checkpoint 承诺的效果本身丢了）；
// 基线之后、持久量之前的确认差口显式列为允许损失返回（升序，不静默）。
// 空历史的实体不进比对（无确认即无义务）。阻塞到核查完成；并发安全；
// 测试断言面。
func (h *InvHistory) CheckCrashBound(ctx context.Context, view DurableView, baseline map[rans.ID]int64) ([]LostOp, error) {
	var lost []LostOp
	for _, id := range h.IDs() {
		base := baseline[id]
		got, found, err := view(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("INV1 检查读持久状态失败 (id=%s): %w", id.String(), err)
		}
		if !found {
			got = 0 // 持久面无条目 = 0 效果量（基线 0 时全部损失在界内）
		}
		if got < base {
			return nil, fmt.Errorf("INV1 崩溃窗违约（checkpoint 基线内的确认丢失）: id=%s 基线=%d 持久可见 %s",
				id.String(), base, durableDesc(found, got))
		}
		for _, op := range h.Records(id) {
			if op.Seq > got {
				lost = append(lost, LostOp{op})
			}
		}
	}
	return lost, nil
}

// AssertDurable 严格零丢失断言面（t 收口）：CheckStrict 违约/读失败即
// Fatal。适用于优雅转换（停机、钝化、迁移）后的断言点。阻塞到视图读取
// 完成；并发安全（经 InvHistory 内部锁）；测试断言面。
func AssertDurable(t *testing.T, ctx context.Context, h *InvHistory, view DurableView) {
	t.Helper()
	if err := h.CheckStrict(ctx, view); err != nil {
		t.Fatal(err)
	}
}

// AssertCrashLoss 崩溃窗断言面（t 收口）：基线存活断言违约即 Fatal；通过
// 时返回允许损失清单（显式列报——调用方可进一步断言损失集形状）。适用于
// 崩溃与崩溃形态（强弃、节点 Kill）后的断言点。阻塞到核查完成；并发安全
// （经 InvHistory 内部锁）；测试断言面。
func AssertCrashLoss(t *testing.T, ctx context.Context, h *InvHistory, view DurableView, baseline map[rans.ID]int64) []LostOp {
	t.Helper()
	lost, err := h.CheckCrashBound(ctx, view, baseline)
	if err != nil {
		t.Fatal(err)
	}
	return lost
}

// durableDesc 持久量的报告形态（无条目与 0 值分开表述）。
func durableDesc(found bool, v int64) string {
	if !found {
		return "无条目（未持久化）"
	}
	return fmt.Sprintf("%d", v)
}
