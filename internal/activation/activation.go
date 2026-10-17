// Package activation 实现本地激活表与注册仲裁（规格 §12 GetOrActivatePawnite
// 同构）。Table 即本地激活表——注册/激活/
// 恢复/终局处置机制与实例查询在此；周期扫描、交接窗终局（迁移/钝化/停机）
// 编排归 internal/lifecycle，经本包的内部导出协作面驱动）。
package activation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// 哨兵族（统一错误模型）：*rans.Error 形态，判定面是
// Code/Reason（rans.CodeOf/ReasonOf 或 errors.Is）。仅包内接线与同模块
// 测试引用，不构成消费方入口。
var (
	// ErrUnknownKind：目标 kind 不在注册表（NotFound+UnknownKind——分类
	// 唯一表「重解析后重试」行；远端未知 kind 同映射，本地远端一致）。
	ErrUnknownKind = rans.NewError(rans.CodeNotFound, rans.ReasonUnknownKind,
		"rans: activate: unknown kind")
	// ErrActivationFailed：激活已失败且终态已落（Unavailable+
	// ActivationFailed）。GetOrActivate 入表后复检命中 StateDestroyed 时
	// 同步返回，注册表条目已同步移除，调用方可安全重试。
	// 异步窗（在箱投递被 drain 回绝）经 callDelivery.Reject 产出同
	// (Code, Reason) 的 *rans.Error——两窗判为同类（注册后失败属执行前
	// 确定性失败）。
	ErrActivationFailed = rans.NewError(rans.CodeUnavailable, rans.ReasonActivationFailed,
		"rans: activate: activation failed and pawnite reached terminal state")
	// ErrFactoryPanic：用户工厂 panic（CodeError+ActivationFailed）——确定性缺陷
	// 不可重试（Is 先比 Code：不命中 ErrActivationFailed 的重试判据）。
	ErrFactoryPanic = rans.NewError(rans.CodeError, rans.ReasonActivationFailed,
		"rans: activate: entity factory panicked")
	// ErrTableSealed：表已封（成员面让位——同 NodeID 更大代际占据成员键，
	// 本进程停止服务）：新激活一律不可重试失败。
	ErrTableSealed = rans.NewError(rans.CodeError, rans.ReasonNone,
		"rans: activate: table sealed (node superseded)")
	// ErrRestoreFailed：激活恢复失败（Store 读取 IO 错误或快照解码错误）。
	// 作为激活失败的根因出现在日志与错误链里（非调用状态码——端口契约
	// 错误保留普通 error 形态）；本地调用方看到的对外形态是
	// ErrActivationFailed（根因文本随错误链保留）。
	ErrRestoreFailed = errors.New("rans: activate: restore failed")
)

// 实体契约与工厂类型由 rans 包承载（rans.Entity / rans.Factory）。

// ArbitrationLostError 激活仲裁落败错误（规格 §12 输家）：携带赢家位置，
// 实现 pawnite.ArbitrationLost——pawnite 用 errors.As 探测后把在箱投递
// 整箱转发给赢家（Forwardable）或按 Unavailable 回绝。
type ArbitrationLostError struct {
	Winner directory.Owner
}

func (e *ArbitrationLostError) Error() string {
	return fmt.Sprintf("rans: register: lost directory arbitration, winner node %d", e.Winner.Node)
}

// WinnerNode 返回赢家节点号（pawnite.ArbitrationLost 契约）。
func (e *ArbitrationLostError) WinnerNode() rans.NodeID { return e.Winner.Node }

// 编译期断言：ArbitrationLostError 满足 pawnite.ArbitrationLost。
var _ pawnite.ArbitrationLost = (*ArbitrationLostError)(nil)

// instance 本地注册条目：pawnite 与其终局清理闭包成对存放——GetLocal 的
// 陈旧 Destroyed 分支经 cleanup 完成注销+逐出（不在持锁状态下做目录 IO）。
type instance struct {
	p       *pawnite.Pawnite
	cleanup func(*pawnite.Pawnite)

	// owner 本实例的目录归属标记（实例级）：激活闭包赢得 Register
	// 后首写（run goroutine，先于任何后续步骤），终局 cleanup 据此做实例级
	// 条件注销（node+inc 同时匹配才删——同节点新实例的条目不被陈旧清理
	// 误删）。nil = 注册从未完成（输家/注册失败/未起步）——本实例无目录
	// 主张。跨 goroutine 读写（run 写、cleanup 在看门狗/销毁 goroutine 读）
	// 经 atomic 协调。
	owner atomic.Pointer[directory.Owner]

	// 周期 checkpoint 簿记（规格 §15，见 checkpoint.go）：ckptEpoch 已
	// checkpoint 的活动纪元（脏门控下界）；ckptInFlight 同实体 IO 在飞门
	//（CAS，Put 收妥前不接受下一轮捕获）。
	ckptEpoch    atomic.Int64
	ckptInFlight atomic.Bool
}

type Table struct {
	dir     directory.Directory
	self    rans.NodeID
	kinds   map[string]rans.Factory
	metrics *metrics.Registry // 观测注册表（Options.Metrics；nil → Default）
	// fencingFloorHook 宿主纪元下限钩子（Options.FencingFloor；nil = 无）。
	fencingFloorHook func(ctx context.Context, id rans.ID) (uint64, error)
	// now/newTimer 定时器时钟注入面（Options.Now/NewTimer；nil = 真实时钟，
	// 语义见 pawnite.Options）。
	now      func() time.Time
	newTimer func(d time.Duration) (c <-chan time.Time, stop func() bool, reset func(d time.Duration) bool)
	// maxTimers 单实例定时器上限（Options.MaxTimers；≤0 → 运行时缺省）。
	maxTimers int
	// sealed 封表标记（Seal 置位后新激活一律 ErrTableSealed）。
	sealed atomic.Bool
	// selfEpoch 节点代际读取面（BindSelfEpoch 装配期单写一次，Start 前
	// 接线完成——Register/Transfer 写入 Owner.Epoch 的取值源；nil = 0
	// 形态，见 BindSelfEpoch 契约）。
	selfEpoch func() uint64
	// takeoverNotify 接管通知发送面（BindTakeoverNotifier 装配期单写一次；
	// nil = 不通知）。见 migrate.go 的 Bind 契约。
	takeoverNotify func(target rans.NodeID, id rans.ID, prev directory.Owner)
	// dir 即合并端口目录：Register（取号+注册原子域）
	// 是 Directory 本面的方法——注册在激活闭包首步（pawnite run goroutine
	// 上，不在创建锁内），不存在「目录缺注册能力」的配置形态。

	// ---- 生命周期协作钩子与迁移/闲置装配面 ----
	// 全部由 New 经 Options 构造期一次给全，运行中不可变（不存在运行期
	// Set 面——运行中并发 Set 与触发属数据竞态）。消费方：
	//   - 钩子族：node 装配根接线（缓存失效等内部协作；不公开——
	//     用户生命周期事件面不提供）；
	//     的 Track/Untrack；nil 对 = 无租约目录形态，本地激活不进续租域）；
	//   - placement/store/stateLoader/sendPackage/migrationBudget：迁移
	//     注入面（规格 §13.1/§13.3；零值 = 未注入 → 对应步骤跳过/拒绝，
	//     语义见 migrate.go）；stateLoader/sendPackage 由 rpc.New 在装配期
	//     经 BindMigrationPackages 补齐（随迁包缓存与网关是 rpc 内部构件
	//     ——装配序上晚于表构造，唯一写入点、Start 前完成）；
	//   - idlePolicies：per-kind 闲置策略（未登记的 kind 永不闲置）。
	//
	// 触发点纪律不变：钩子全部在表锁外同步执行（重入边界与阻塞禁忌见
	// fireOn* 注释）。
	onActivated       func(id rans.ID)
	onEvicted         func(id rans.ID)
	onArbitrationLost func(id rans.ID, winner directory.Owner)
	onMigrated        func(id rans.ID, to rans.NodeID)

	// ---- 迁移注入面（规格 §13.1/§13.3；语义细节见 migrate.go/carrier.go）----
	// 零值 = 未注入：placement nil → Migrate 直接 ErrNoEligibleTarget；
	// sender/stateLoader nil → core/激活闭包跳过对应尽力而为步骤；store nil
	// → 有状态实体的迁移/钝化被拒、停机走「不写快照」档（spec §5.1 第 3 点
	// core 矩阵——core 已迁 internal/lifecycle：drain.go（Retire）/
	// migrate.go（Migrate）/passivator.go（Passivate)）。预算零值回落
	// DefaultMigrationBudget。
	// 迁移选点带 kind 维：kind = 迁移实体的
	// id.Kind——选点与首激活共用 kind 感知 Placement（node 的
	// placementBridge.PickForKind：候选只含声明承载该 kind 的节点）。
	placement   func(kind string, exclude rans.NodeID) (rans.NodeID, bool)
	store       checkpoint.Store
	stateLoader StateLoader
	// registryForm 目录是否带成员面（票 14 的 epoch-0 闸门判据：registry
	// 形态下未取号（SelfEpoch==0）的激活直接拒绝——Start 前调用不写
	// epoch-0 条目；registry-less 形态代际恒 0，闸门不设）。
	registryForm    bool
	sendPackage     func(ctx context.Context, target rans.NodeID, id rans.ID, snap []byte, srcInc uint64) error
	migrationBudget time.Duration

	// warnNoStoreOnce Retire 遇「有状态实体 + 未配置 Store」的部署缺口
	// 告警单胜（Table 级——多 Table 形态各自告警一次；不用进程级 sync.Once
	// ——进程级单胜会让第二个 Table 起的同类缺口静默，且无法在进程内复验）。
	warnNoStoreOnce sync.Once

	// onExpired 出队过期即弃事件钩子（语义见 pawnite.Options.OnExpired）：
	// rpc.New 装配期唯一写入点（计数器是 rpc 内部构件，装配序晚于表构造）。
	onExpired func()

	// onCanceled 出队取消弃置事件钩子（语义同 pawnite.Options.OnCanceled）。
	onCanceled func()

	// ---- 闲置策略面（规格 §14.3 E2/G1；语义细节见 idle.go）----
	// kind → 闲置阈值+回收模式；未登记的 kind 永不闲置。构造期一次给全
	// 后只读，扫描快照免锁。
	idlePolicies map[string]IdlePolicy

	readMu sync.RWMutex // local/entities 读路径
	mu     sync.Mutex   // creationLock 双检（吞吐优化项：条纹化，规格 §12 注）
	local  map[rans.ID]*instance
	entity map[rans.ID]rans.Entity
}

// Options Table 的构造期装配面（配置一次给全）：内部
// 协作钩子、续租接线、迁移注入面与闲置策略全部在构造时定死，运行中不可
// 变。零值字段 = 对应面未接线。唯一例外是随迁包两端
// （StateLoader/SendPackage）：承载它们的包缓存与网关是 rpc 内部构件，装
// 配序晚于表构造，由 rpc.New 在装配期经 BindMigrationPackages 补齐——
// 该方法是这两个槽的唯一写入点，Start 前完成后同样不可变。
type Options struct {
	// OnActivated 激活完成钩子：目录注册成功之后、在 Pawnite run goroutine
	// 上同步触发（严格一次/次激活），不持任何表锁。其内对同一 id 调
	// Destroy 会自等待串行队列（死锁），禁止。
	OnActivated func(id rans.ID)
	// OnEvicted 逐出钩子：本实例终局清理（目录注销+本地逐出）完成后同步
	// 触发，严格一次（随 cleanup 的 sync.Once）。在 cleanup 调用 goroutine
	// 上执行（Destroy/Drain 编排或看门狗扫描）——必须非阻塞；重入
	// GetOrActivate/Destroy 安全。
	OnEvicted func(id rans.ID)
	// OnArbitrationLost 激活仲裁落败钩子：输家实例终局清理（cleanup 逐出）
	// 之后同步触发，携带赢家位置。不持任何表锁；必须非阻塞。
	OnArbitrationLost func(id rans.ID, winner directory.Owner)
	// OnMigrated 迁移赢家钩子：Migrate 裁决 Won 且终局收尾完成后、
	// 返回 nil 前同步触发（晚于 OnEvicted——新位置条目不被逐出失效清掉，
	// 次序契约见 NotifyMigrated）。不持任何表锁；必须非阻塞。
	OnMigrated func(id rans.ID, to rans.NodeID)

	// Placement 迁移目标选点（规格 §13.1）：kind 为迁移实体的 id.Kind
	//（与首激活共用 kind 感知 Placement），exclude 为本地节点号；返回
	// (target, true) 时 target 必须不等于 exclude，(0, false) = 无候选 →
	// lifecycle.Migrate 返回 ErrNoEligibleTarget、实体保持 Active。
	Placement func(kind string, exclude rans.NodeID) (rans.NodeID, bool)
	// Store 快照存储（规格 §15）：交接窗三 core 的快照落库承载（语义见
	// migrate.go 的 SnapshotStore）。nil → 有状态实体的迁移/钝化被拒。
	Store checkpoint.Store
	// StateLoader 激活期状态装载器（规格 §13.3 目标端恢复；语义见
	// carrier.go）。nil → 激活闭包跳过装载（Store 兜底仍独立生效）。
	// rpc 装配形态经 BindMigrationPackages 接线。
	StateLoader StateLoader
	// Metrics 观测注册表（B9 所有权计数器落点）：nil → 进程级
	// metrics.Default 共享。node 装配根注入本节点注册表。
	Metrics *metrics.Registry
	// FencingFloor 宿主纪元下限钩子（A2）：返回该 id 已被宿主自持存储
	// 接受的最大纪元（0 = 无；每次激活前调用一次，阻塞受激活预算约束）。
	// 报错 → 激活失败（可重试）。nil = 无钩子。
	FencingFloor func(ctx context.Context, id rans.ID) (uint64, error)
	// SendPackage 随迁包直发（规格 §13.3 E1 快路径；nil → 跳过发送）。rpc
	// 装配形态经 BindMigrationPackages 接线。srcInc 形参是源端落库围栏
	// incarnation（随迁包源纪元，收端恢复时与 Store 高水位比较）。
	SendPackage func(ctx context.Context, target rans.NodeID, id rans.ID, snap []byte, srcInc uint64) error
	// MigrationBudget 迁移总预算；零/负值回落 DefaultMigrationBudget。
	MigrationBudget time.Duration
	// Now/NewTimer 定时器时钟注入面（测试虚拟时钟；须同给，nil = 真实
	// 时钟）。函数形态语义见 pawnite.Options 同名字段。
	Now      func() time.Time
	NewTimer func(d time.Duration) (c <-chan time.Time, stop func() bool, reset func(d time.Duration) bool)
	// MaxTimers 单实例定时器上限（≤0 → 运行时缺省 65536；节点级护栏，
	// 语义见 node.Config.Tuning.MaxTimers）。
	MaxTimers int

	// IdlePolicies per-kind 闲置回收策略（未登记的 kind 永不闲置）。
	IdlePolicies map[string]IdlePolicy
}

func New(dir directory.Directory, selfNode rans.NodeID, kinds map[string]rans.Factory, opts ...Options) *Table {
	m := &Table{
		dir:   dir,
		self:  selfNode,
		kinds: kinds,
		registryForm: func() bool {
			_, ok := dir.(directory.NodeRegistry)
			return ok
		}(),
		local:  make(map[rans.ID]*instance),
		entity: make(map[rans.ID]rans.Entity),
	}
	// 多个 Options 按 non-zero 合并（后到者覆盖同名字段）——装配方可以
	// 分段给全（如基础面 + 测试追加策略），空字段不清掉已接线的部分。
	for _, o := range opts {
		if o.OnActivated != nil {
			m.onActivated = o.OnActivated
		}
		if o.OnEvicted != nil {
			m.onEvicted = o.OnEvicted
		}
		if o.OnArbitrationLost != nil {
			m.onArbitrationLost = o.OnArbitrationLost
		}
		if o.Metrics != nil {
			m.metrics = o.Metrics
		}
		if o.FencingFloor != nil {
			m.fencingFloorHook = o.FencingFloor
		}
		if o.OnMigrated != nil {
			m.onMigrated = o.OnMigrated
		}
		if o.Placement != nil {
			m.placement = o.Placement
		}
		if o.Store != nil {
			m.store = o.Store
		}
		if o.StateLoader != nil {
			m.stateLoader = o.StateLoader
		}
		if o.SendPackage != nil {
			m.sendPackage = o.SendPackage
		}
		if o.MigrationBudget != 0 {
			m.migrationBudget = o.MigrationBudget
		}
		if o.Now != nil && o.NewTimer != nil {
			m.now, m.newTimer = o.Now, o.NewTimer
		}
		if o.MaxTimers > 0 {
			m.maxTimers = o.MaxTimers
		}
		for kind, p := range o.IdlePolicies {
			if m.idlePolicies == nil {
				m.idlePolicies = make(map[string]IdlePolicy, len(o.IdlePolicies))
			}
			m.idlePolicies[kind] = p
		}
	}
	return m
}

// fireOnActivated 锁外触发激活完成钩子（Options 构造期定死，读免锁）。
func (m *Table) fireOnActivated(id rans.ID) {
	if m.onActivated != nil {
		m.onActivated(id)
	}
}

// fireOnEvicted 锁外触发逐出钩子。宿主责任注记：钩子在 cleanup 的调用 goroutine 上同步执行——
// Destroy/Drain 的编排 goroutine 或看门狗扫描 goroutine（AbandonStuck*
// 终局）——慢钩子同样会拖慢/冻结所在环，不异步化（保留 cleanup 幂等
// 兜底的次序语义）；注册的钩子必须非阻塞。
func (m *Table) fireOnEvicted(id rans.ID) {
	if m.onEvicted != nil {
		m.onEvicted(id)
	}
}

// fireOnArbitrationLost 锁外触发仲裁落败钩子。
func (m *Table) fireOnArbitrationLost(id rans.ID, winner directory.Owner) {
	if m.onArbitrationLost != nil {
		m.onArbitrationLost(id, winner)
	}
}

// NotifyMigrated 锁外触发迁移赢家钩子（internal/lifecycle 的迁移
// 编排器在 Migrate 赢家路径上调用——钩子触发点仍归表，见
// Options.OnMigrated；触发时点在 onEvicted 之后，新位置条目不会被逐出
// 失效清掉——次序契约同 arbLost 槽）。
func (m *Table) NotifyMigrated(id rans.ID, to rans.NodeID) {
	if m.onMigrated != nil {
		m.onMigrated(id, to)
	}
}
