package activation

import (
	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// ---- internal/lifecycle 协作面 ----
//
// 周期扫描与交接窗编排已迁 internal/lifecycle（迁移/钝化/停机 core、看门
// 狗/闲置/checkpoint 复合体）。编排器经 *activation.Table 协作，本节与
// checkpoint.go（纪元等待/在飞门）、idle.go（闲置策略查询）、migrate.go
// （迁移注入面读取）共同构成这条内部导出协作面——以编排所需的最小面为限
//（实例查询/纪元等待/终局清理重放/注入面读取），不构成消费方入口。

// PawnRef 本地在册条目的协作快照（id + pawnite 引用；锁内快照、锁外读
// 状态——统一快照纪律）。
type PawnRef struct {
	ID   rans.ID
	Pawn *pawnite.Pawnite
}

// Locals 枚举本地在册条目快照（lifecycle 扫描面：看门狗视图/闲置扫描/
// checkpoint 扫轮/整体停机快照共用）。陈旧 Destroyed 条目一并报出——
// 扫描方按各自的状态判据忽略或触发清理。
func (m *Table) Locals() []PawnRef {
	m.readMu.RLock()
	refs := make([]PawnRef, 0, len(m.local))
	for id, inst := range m.local {
		refs = append(refs, PawnRef{ID: id, Pawn: inst.p})
	}
	m.readMu.RUnlock()
	return refs
}

// MetricsRegistry 观测注册表协作面（lifecycle/rpc 侧 B9 计数器落点；
// 注入优先、Default 兜底——语义同 carrier.go 的 metricsReg）。
func (m *Table) MetricsRegistry() *metrics.Registry { return m.metricsReg() }

// InstanceIncarnation 返回在册实例的注册纪元（协作面）：ok=false 表示注册
// 从未完成（owner 未落位——输家/注册失败/未起步，本实例无目录主张）。
// 心跳 OnLost 的实例级止损销毁（lifecycle.DestroyInstance）据此把「纪元
// 匹配才销毁」的判定留在表外编排、纪元簿记留在表内。
func (m *Table) InstanceIncarnation(id rans.ID) (uint64, bool) {
	inst := m.peekLocal(id)
	if inst == nil {
		return 0, false
	}
	owner := inst.owner.Load()
	if owner == nil {
		return 0, false
	}
	return owner.Incarnation, true
}

// FinalizeEviction 终局清理的幂等重放（协作面）：看门狗强弃骨架的清理半步
// ——pawnite 强弃成功后其 OnEvict 已同步跑过 cleanup，此处重放是兜底（Once
// + 身份守卫），覆盖 OnEvict 未接线的裸 pawnite 形态。身份守卫：条目已换代
// /被逐出时零副作用（后来者不被误清）。
func (m *Table) FinalizeEviction(id rans.ID, p *pawnite.Pawnite) {
	if inst := m.peekLocal(id); inst != nil && inst.p == p {
		inst.cleanup(inst.p)
	}
}
