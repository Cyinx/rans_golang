package activation

// 迁移注入面（规格 §13.1/§13.3；迁移编排与
// migCore 已迁 internal/lifecycle/migrate.go，本文件保留 Table 侧的注入
// 字段（Options 构造期装配）与编排器的读取协作面）。

import (
	"context"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/log"
)

// BindStateLoader / BindPackageSender 随迁包两端的装配期接线：前者接目标端
// 激活装载器（随迁包缓存 consume-on-read 适配，nil =
// 跳过装载），后者接源端随迁包直发（nil = 跳过快路径）。rpc.New 是唯一
// 生产调用点——包缓存与网关是 rpc 内部构件，装配序晚于表构造，无法进
// Options；Start 前单写一次后与 Options 字段同族不可变，无运行期 Set 面
// （旧 SetStateLoader 的「顶替语义」随之消失——预置自有 loader 的装配形
// 态不再可表示，目标端恢复梯子按契约锚定 rpc 装配）。
func (m *Table) BindStateLoader(loader StateLoader) { m.stateLoader = loader }

func (m *Table) BindPackageSender(send func(ctx context.Context, target rans.NodeID, id rans.ID, snap []byte) error) {
	m.sendPackage = send
}

// WarnStatefulNoStore Retire 遇「有状态实体 + 未配置 Store」的部署缺口
// 告警（spec §5.1 第 3 点表：表级单胜一次，不逐实体刷屏——停机照样终局，
// 代价是上一份 checkpoint 之后的增量随终局丢弃；单胜粒度为表级而非进程：
// 多表宿主的各自缺口各自可见。协作面——由 lifecycle 的 retireCore 调用）。
func (m *Table) WarnStatefulNoStore(id rans.ID) {
	m.warnNoStoreOnce.Do(func() {
		log.Warn("rans/activation: retiring stateful entity without checkpoint store; final snapshot skipped "+
			"(state since last checkpoint is lost on this stop; configure checkpoint.Store for stateful kinds)",
			"uuid", id.String())
	})
}

// ---- 迁移注入面的读取协作面（internal/lifecycle 编排器专用）----

// Placement 返回迁移选点函数（nil = 未注入 → lifecycle.Migrate 报
// ErrNoEligibleTarget）。
func (m *Table) Placement() func(kind string, exclude rans.NodeID) (rans.NodeID, bool) {
	return m.placement
}

// PackageSender 返回随迁包直发函数（nil = 未接线 → 跳过快路径）。
func (m *Table) PackageSender() func(ctx context.Context, target rans.NodeID, id rans.ID, snap []byte) error {
	return m.sendPackage
}

// MigrationBudget 返回迁移总预算配置（零/负值由调用方回落缺省）。
func (m *Table) MigrationBudget() time.Duration { return m.migrationBudget }

// SnapshotStore 返回快照存储（nil = 未配置；迁移/钝化对有状态实体拒，
// 停机走「不写快照」档）。
func (m *Table) SnapshotStore() checkpoint.Store { return m.store }

// Directory 返回合并端口目录（迁移换锁 Transfer 的承载面——合并端口
// 本面方法，无能力断言形态）。
func (m *Table) Directory() directory.Directory { return m.dir }

// SelfNode 返回本表节点号（迁移归属 Owner 的 node 半边）。
func (m *Table) SelfNode() rans.NodeID { return m.self }

// InstanceOwner 本地实例的目录归属（无实例/未落位 → false）：所有权丢失
// 通知的收端匹配面。
func (m *Table) InstanceOwner(id rans.ID) (directory.Owner, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inst := m.local[id]
	if inst == nil {
		return directory.Owner{}, false
	}
	o := inst.owner.Load()
	if o == nil {
		return directory.Owner{}, false
	}
	return *o, true
}

// BindTakeoverNotifier 接管通知发送面（H13 决策 22 发送半边，Start 前
// 单写一次）：CAS 接管赢后把旧归属直发旧 owner 节点（best-effort）。
func (m *Table) BindTakeoverNotifier(fn func(target rans.NodeID, id rans.ID, prev directory.Owner)) {
	m.takeoverNotify = fn
}

// notifyTakeover 通知旧 owner（未绑定 no-op）：调用点在激活闭包上，发送
// 面自带异步与预算，本调用即刻返回。
func (m *Table) notifyTakeover(prev directory.Owner, id rans.ID) {
	if m.takeoverNotify != nil && prev.Node != 0 {
		m.takeoverNotify(prev.Node, id, prev)
	}
}

// BindSelfEpoch 节点代际读取面（Liveness.NodeEpoch 装配期接线；绑定晚于
// 表构造——Start 前单写一次，Bind 族不可变）。nil = epoch 0 形态。
func (m *Table) BindSelfEpoch(epoch func() uint64) { m.selfEpoch = epoch }

// SelfEpoch 返回节点代际（未绑定即 0——同节点重启代际严格递增的取号面
// 在 Liveness；0 值条目按「未接代际」判定）。
func (m *Table) SelfEpoch() uint64 {
	if m.selfEpoch == nil {
		return 0
	}
	return m.selfEpoch()
}
