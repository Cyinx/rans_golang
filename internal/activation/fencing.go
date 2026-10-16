package activation

// 实例身份围栏原语：止损面（围栏后销毁）必须精确到具体实例——按 id 围栏
// 会误伤同 id 的新化身（owner 三元组的 Incarnation 是实例唯一身份）。

import (
	"context"
	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"time"
)

// FenceIfOwner 条件围栏：仅当在册实例的目录归属恰为 want 时围栏，返回
// 被围栏实例的引用（供销毁循环按同一实例身份收场）。判据是 id + owner
// 双维度全等——Incarnation 是 per-id 序列，owner 三元组全等不蕴含同一
// 实体，单实体止损只比 owner 会跨实体误杀（误围即永久损伤）。无实例/
// owner 未落位/已换代均 miss（幂等 no-op）。
func (m *Table) FenceIfOwner(id rans.ID, want directory.Owner) (PawnRef, bool) {
	m.readMu.RLock()
	inst := m.local[id]
	m.readMu.RUnlock()
	if inst == nil {
		return PawnRef{}, false
	}
	if o := inst.owner.Load(); o == nil || *o != want {
		return PawnRef{}, false
	}
	inst.p.Fence()
	return PawnRef{ID: id, Pawn: inst.p}, true
}

// FenceWhere 谓词围栏：表读锁内快照命中项、锁外逐个围栏，返回被围栏的
// 实例集（调用方按实例身份销毁——换代交错由销毁侧身份守卫裁定）。
func (m *Table) FenceWhere(pred func(*directory.Owner) bool) []PawnRef {
	m.readMu.RLock()
	refs := make([]PawnRef, 0, 8)
	for id, inst := range m.local {
		if o := inst.owner.Load(); o != nil && pred(o) {
			refs = append(refs, PawnRef{ID: id, Pawn: inst.p})
		}
	}
	m.readMu.RUnlock()
	for i := range refs {
		refs[i].Pawn.Fence()
	}
	return refs
}

// Seal 封表：此后新激活一律不可重试失败（成员面 Superseded 让位的封表
// 面——让位进程不再接受任何新实体）。
func (m *Table) Seal() { m.sealed.Store(true) }

// epochPublishWait 激活等待代际发布的预算（liveness 首写一两个目录往返
// 量级；取号持续失败的超时回落 epoch-0 形态）。
const epochPublishWait = 500 * time.Millisecond

// awaitEpochPublished 代际未发布时的有界等待（仅成员面形态——无代际源
// 的目录恒 0 不等待）。尊重激活 ctx 取消。
func (m *Table) awaitEpochPublished(ctx context.Context) {
	if m.selfEpoch == nil || m.SelfEpoch() != 0 {
		return
	}
	deadline := time.Now().Add(epochPublishWait)
	for time.Now().Before(deadline) && m.SelfEpoch() == 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Millisecond):
		}
	}
}
