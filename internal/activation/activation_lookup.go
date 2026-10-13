package activation

import (
	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// GetLocal 本地快查。命中但 State()==Destroyed 视为 miss——只做本地逐出
// （身份守卫、纯内存操作），不等目录注销：终局由 pawnite 自驱（finish/强弃
// 单点跑 OnEvict→cleanup 同步承担注销），本路径读循环零 IO 亦不被在途注销
// 阻塞（《加固 spec》§5.2 第 7 点：读循环零 IO 前提）。
// 同 id 新激活因而不被旧实例的在途注销串行化；交错安全由实例级标记承载
// ——旧实例的迟到注销（UnregisterIfOwner {node, inc} 同时匹配才删）遇到
// 新实例条目为 no-op，不会留下活实例无目录条目。
func (m *Table) GetLocal(id rans.ID) (*pawnite.Pawnite, bool) {
	m.readMu.RLock()
	inst := m.local[id]
	m.readMu.RUnlock()
	if inst == nil {
		return nil, false
	}
	if inst.p.State() == pawnite.StateDestroyed {
		m.evict(id, inst.p)
		return nil, false
	}
	return inst.p, true
}

// peekLocal 本地快查的「只看不清理」变体：creationLock 临界区内的双检专用。
// 陈旧条目的逐出含写锁与（潜在）钩子触发，必须发生在 m.mu 之外（钩子重入
// GetOrActivate 时 m.mu 不可重入会死锁），故持锁路径只取快照，由调用方
// 放锁后再逐出。
func (m *Table) peekLocal(id rans.ID) *instance {
	m.readMu.RLock()
	defer m.readMu.RUnlock()
	return m.local[id]
}

// PeekLocal 本地快查的非清理形态（协作面）：
// 命中即返回实例引用（含陈旧 Destroyed 条目），不做逐出、不等终局——
// 看门狗强弃骨架（gate 先行、清理重放随后）需要这个原始视图。需要
// 「陈旧即 miss + 就地逐出」语义的读路径用 GetLocal。
func (m *Table) PeekLocal(id rans.ID) (*pawnite.Pawnite, bool) {
	inst := m.peekLocal(id)
	if inst == nil {
		return nil, false
	}
	return inst.p, true
}

// Fence 对本地实例关闭准入（租约失守止损）。非本地 id 是 no-op。只改内存
// 状态、毫秒级返回，不触发销毁——销毁编排归调用方（心跳服务 OnLost）。
func (m *Table) Fence(id rans.ID) {
	m.readMu.RLock()
	inst, ok := m.local[id]
	m.readMu.RUnlock()
	if ok {
		inst.p.Fence()
	}
}

// LocalIDs 枚举本地注册表全部实体身份（排空的枚举面，规格 §14.1 ②
// ——Drain 逐实体走 Destroy 统一销毁路径）。拷贝快照：返回后注册表的演化
// （销毁逐出/新激活）不影响调用方遍历；Destroy 对快照后已不在册的身份是
// no-op，快照后新激活者的收编见调用方（Remote.Shutdown：排空是终态动作，
// 门闸已拦新到请求，竞态新激活者随本节点停机一并消亡）。
func (m *Table) LocalIDs() []rans.ID {
	m.readMu.RLock()
	defer m.readMu.RUnlock()
	ids := make([]rans.ID, 0, len(m.local))
	for id := range m.local {
		ids = append(ids, id)
	}
	return ids
}

// evict 身份守卫逐出：仅当 local 中仍是本 pawnite 时才删，防止陈旧 OnEvict
// 误删后来者（身份守卫不变量）。entity 与 local 同临界区成对写入，
// 守卫命中时按同一对应关系一并删除；未命中则整体让位（后来者已接管）。
// 幂等：cleanup 的注销后重放与测试直调均可安全重复调用。
func (m *Table) evict(id rans.ID, p *pawnite.Pawnite) {
	m.readMu.Lock()
	if inst := m.local[id]; inst != nil && inst.p == p {
		delete(m.local, id)
		delete(m.entity, id)
	}
	m.readMu.Unlock()
}

// EntityOf 暴露激活实体给调用层（Call 绑定 handler 用）。
func (m *Table) EntityOf(id rans.ID) (rans.Entity, bool) {
	m.readMu.RLock()
	defer m.readMu.RUnlock()
	e, ok := m.entity[id]
	return e, ok
}

// MetricsView 观测面快照（规格 §16 queue_depth/inbox_depth
// Gauge 的数据源）：entities = 本地注册表条目数，inboxDepth = 各 pawnite
// 收件箱积压深度之和。先在 readMu 内快照 pawnite 指针、锁外逐个 InboxLen
// （不持表读锁进 pawnite 锁——避免与任何反向取锁路径构成环）。
// 读数是采样瞬间的近似（观测语义）；rpc.New 以观察闭包接入 metrics
// 注册表，采样由读数方驱动（OTel 收集 / 手动 Snapshot），无后台任务。
func (m *Table) MetricsView() (entities int, inboxDepth int) {
	m.readMu.RLock()
	ps := make([]*pawnite.Pawnite, 0, len(m.local))
	for _, inst := range m.local {
		ps = append(ps, inst.p)
	}
	m.readMu.RUnlock()
	for _, p := range ps {
		inboxDepth += p.InboxLen()
	}
	return len(ps), inboxDepth
}
