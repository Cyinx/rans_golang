package activation

import (
	"bytes"
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/metrics"

	"go.opentelemetry.io/otel/attribute"
)

// 快照能力面由 rans 包承载（rans.Snapshotter）；本文件保留激活期状态装载与恢复逻辑。

// StateLoader 激活期状态装载契约（规格 §13.3 目标端恢复的运行时半边）：
// GetOrActivate 的激活闭包在 ent.Activate 之前查询（load-first：实体先
// 就绪，Activate 观测到的即恢复后的状态）。命中返回 (快照字节, 源纪元,
// true)；consume-on-read 语义由实现自带（如 rpc 的随迁包缓存：包恢复恰一次，
// 后续激活 miss 落 Store 兜底）。srcInc 是包的源端围栏 incarnation（发送方
// 落库快照时的 Store 纪元；0 = 未携带——旧端发送方或未围栏形态），恢复侧
// 据此与 Store per-id 高水位比较、包旧弃包（见 restoreState）。manager 不
// 假设字节形状——形状校验归 rans.Snapshotter.Restore。注入面：
// Options.StateLoader（构造期）与 BindMigrationPackages（rpc 装配期唯一
// 补写点）——运行中不可变。
type StateLoader interface {
	LoadState(ctx context.Context, id rans.ID) (snap []byte, srcInc uint64, ok bool)
}

// restoreSource restoreState 的恢复来源（恢复即落库，《加固 spec》§5.2
// 第 6 点）：激活闭包据此分叉后续动作——restoreStore 状态已持久（标记
// ckptEpoch 判干净）；restorePackage 状态可能比 Store 新（激活方立即以
// 本实例新纪元落库一次，围栏从这一次落库起对源端旧纪元写生效）；两者
// 互斥，枚举形态使不可能组合不可表示。
type restoreSource uint8

const (
	restoreNone    restoreSource = iota // 未命中任何状态面（或包弃包落 Store 前的各失败路径）：裸激活
	restorePackage                      // 随迁包命中且经版本比较判新：返回包字节原样，实体状态=包字节，可能比 Store 新
	restoreStore                        // Store 命中且恢复成功：实体状态与持久层一致
)

// restoreState 激活期状态恢复（load-first，规格 §13.3「实体先就绪」）：在
// ent.Activate 之前执行。随迁包是快路径，包恢复失败退回 Store；包经版本
// 比较判旧（packageStale）同样弃包退回 Store——「包优先」前提是包不旧于
// Store（该前提只对成功迁移成立，判输迁移留下的是过期包）；Store 是
// 正确性依赖——Get 的 IO 错误与快照解码错误都让激活失败（ErrRestoreFailed
// 包装原错误），绝不以空状态对外服务。未命中 → 裸激活。需要宽容不兼容
// 快照的实体在 Restore 里自行吞掉并返回 nil。运行在
// Pawnite run goroutine 上（串行即静默），对实体状态的读写无竞态。
//
// 返回恢复来源与来源字节（仅 restorePackage 携带，包字节原样移交）：
//   - restoreStore：实体状态与持久层一致，激活方据此把 ckptEpoch 抬到活动
//     纪元（首轮周期 checkpoint 判干净，不把刚读出的状态原样写回）；
//   - restorePackage：包可能比 Store 新——激活方必须立即以本实例新纪元把
//     包字节落库一次（「恢复即落库」，spec §5.2 第 6 点）：目标端第一次以
//     新纪元落库后，Store 的 per-id 高水位才越过源端旧纪元，源端迟到写入
//     开始被 ErrStaleIncarnation 拒。落库是尽力而为档：Store 暂不可写时
//     仅 Warn 不阻断激活，围栏窗延后到首轮周期 checkpoint 收口。nil-Store
//     例外（只接 loader 不接 Store 的装配形态）：restorePackage 命中但
//     store==nil 时静默跳过落库——围栏窗永不收口（没有任何后续 Put 能把
//     per-id 高水位抬过源端旧纪元），源端迟到写入持续被本实例新纪元接受，
//     覆盖刚恢复的状态；该形态下随迁包只当快路径用，宿主不得依赖围栏。
func (m *Table) restoreState(ctx context.Context, id rans.ID, ent rans.Entity) (restoreSource, []byte, error) {
	if m.stateLoader == nil && m.store == nil {
		return restoreNone, nil, nil // 未接线任何状态面：裸激活零开销
	}
	if m.stateLoader != nil {
		if b, srcInc, ok := m.stateLoader.LoadState(ctx, id); ok {
			carrier, isCarrier := ent.(rans.Snapshotter)
			if !isCarrier {
				log.Warn("rans/activation: activation state loaded but entity lacks migration carrier face; bytes ignored",
					"uuid", id.String(), "bytes", len(b))
				return restoreNone, nil, nil // 弃包落到 Store 梯级
			}
			// 先判旧再应用（票 19）：包旧/已删时不触碰实体——「先应用再
			// 判旧」会让实体带着旧包状态走 Store 二次 Restore（合并态污
			// 染）或在 Store 未命中时整包保留已删除状态（首轮周期
			// checkpoint 以新纪元写回=复活）。
			if m.packageStale(ctx, id, srcInc, b) {
				// 包旧于 Store / 同纪元内容已删（判输迁移的过期包、宿主
				// Delete 留高水位）：弃包落 Store 梯级——接管恢复必须读
				// 最新已确认状态，过期包不得以新纪元写回。
				log.Warn("rans/activation: stale migration package dropped in favor of checkpoint store",
					"uuid", id.String(), "src_incarnation", srcInc)
			} else if err := carrier.Restore(b); err != nil {
				// 随迁包是加速面而非正确性依赖：弃包落到 Store 梯级。
				log.Warn("rans/activation: activation state restore failed; falling back to checkpoint store",
					"uuid", id.String(), "err", err)
			} else {
				return restorePackage, b, nil // 包命中且判新：不读 Store 内容（包可能比 Store 新）
			}
		}
	}
	carrier, isCarrier := ent.(rans.Snapshotter)
	if !isCarrier || m.store == nil {
		return restoreNone, nil, nil
	}
	b, ok, err := m.store.Get(ctx, id)
	if err != nil {
		return restoreNone, nil, fmt.Errorf("%w: store get: %w", ErrRestoreFailed, err)
	}
	if !ok {
		return restoreNone, nil, nil // 未命中：裸激活
	}
	if err := carrier.Restore(b); err != nil {
		return restoreNone, nil, fmt.Errorf("%w: snapshot decode: %w", ErrRestoreFailed, err)
	}
	return restoreStore, nil, nil // Store 命中且恢复成功：实体状态与持久层一致
}

// highWaterStore Store 高水位只读能力面（checkpoint.HighWaterReader 的本包
// 视图，可选能力经类型断言发现）：未实现者恢复侧退回「包优先」历史行为。
type highWaterStore interface {
	HighWater(ctx context.Context, id rans.ID) (uint64, bool, error)
}

// fencingFloor 激活取号的纪元下限合成：max(Store 高水位, 宿主钩子)。
// 高水位仅当实体可快照且 Store 实现高水位面时可读（能力缺席跳过）；钩子
// nil 跳过。任一读取出错返回错误——激活失败（可重试，根因保留）。
func (m *Table) fencingFloor(ctx context.Context, id rans.ID, ent rans.Entity) (uint64, error) {
	var floor uint64
	if _, ok := ent.(rans.Snapshotter); ok && m.store != nil {
		if hwr, can := m.store.(highWaterStore); can {
			hw, _, err := hwr.HighWater(ctx, id)
			if err != nil {
				return 0, fmt.Errorf("rans: activate: store high-water read failed: %w", err)
			}
			if hw > floor {
				floor = hw
			}
		}
	}
	if m.fencingFloorHook != nil {
		hf, err := m.fencingFloorHook(ctx, id)
		if err != nil {
			return 0, fmt.Errorf("rans: activate: host fencing floor failed: %w", err)
		}
		if hf > floor {
			floor = hf
		}
	}
	return floor, nil
}

// packageStale 随迁包陈旧判定（restoreState 包命中时调用，与 Store per-id
// 高水位比较）：包旧于 Store 高水位即弃包——失败迁移（CAS 判输/预算耗尽）
// 留下的过期包不再让接管恢复静默回滚已确认变更。判定序：
//  1. srcInc == 0：发送方未携带版本（旧端形态）——不弃（无法判定，包优先）；
//  2. Store 未实现 HighWaterReader：能力缺席——不弃（同 1 的保守回退）；
//  3. 包纪元 < 高水位：Store 已接受更新的 incarnation——弃包；
//  4. 包纪元 > 高水位：Store 无更新写，包不旧——不弃；
//  5. 包纪元 == 高水位：同纪元分不出快照先后（成功迁移：源端先落库后发包，
//     同内容；判输迁移：源端同纪元续落了更新的快照）——内容比对定胜负，
//     Store 内容不同即源端已前进，弃包；Get 失败/未命中不弃（比较是加速
//     判定，不可用时不损快路径——Store IO 错误由恢复梯/落库面既有语义收口）。
func (m *Table) packageStale(ctx context.Context, id rans.ID, srcInc uint64, pkg []byte) bool {
	if srcInc == 0 || m.store == nil {
		return false
	}
	hwr, can := m.store.(highWaterStore)
	if !can {
		return false
	}
	hw, ok, err := hwr.HighWater(ctx, id)
	if err != nil || !ok || hw < srcInc {
		return false
	}
	if hw > srcInc {
		return true
	}
	cur, hit, err := m.store.Get(ctx, id)
	if err != nil {
		return false
	}
	if !hit {
		// 同纪元且内容不在（票 19）：高水位存在即宿主 Delete 撤内容留水
		// 位的形态——已删除实体不得经随迁包复活，判旧弃包（裸激活）。
		return true
	}
	return !bytes.Equal(cur, pkg)
}

// activationFailedCounter 懒建的激活失败计数器（metrics.Default 进程级共享
// 注册表，同名幂等注册——进程内多表实例聚合到同一计数器；未发生
// 过激活失败的进程不注册该仪表）。
var activationFailedCounter = sync.OnceValue(func() *metrics.Counter {
	return metrics.Default().Counter(metrics.NameActivationFailed,
		"entity activations that ended in failure (entity never reached Active)")
})

// metricsReg 观测注册表（构造期注入；未注入回落进程级 Default——直接
// 构造表的测试与 rpc 形态同 Default）。
func (m *Table) metricsReg() *metrics.Registry {
	if m.metrics != nil {
		return m.metrics
	}
	return metrics.Default()
}

// activationFailReason* 是激活失败计数器 reason 属性的取值（恢复失败 /
// 注册失败 / 工厂 panic）。reason 暂用字符串维度。
const (
	activationFailReasonRestore  = "restore"
	activationFailReasonRegister = "register"
	activationFailReasonFactory  = "factory"
)

// observeFactoryPanic 记录工厂 panic 导致的激活失败：Error 日志（panic 值 +
// 栈）+ rans.activation.failed{reason=factory} 计数器。
func (m *Table) observeFactoryPanic(id rans.ID, r any) {
	log.Error("rans/activation: entity factory panicked; activation rejected",
		"uuid", id.String(), "panic", r, "stack", string(debug.Stack()))
	activationFailedCounter().Add(1, attribute.String("reason", activationFailReasonFactory))
}

// observeRestoreFailure 记录恢复失败导致的激活失败：Error 日志 +
// rans.activation.failed{reason=restore} 计数器。
func (m *Table) observeRestoreFailure(id rans.ID, err error) {
	log.Error("rans/activation: activation restore failed; activation aborted",
		"uuid", id.String(), "err", err)
	activationFailedCounter().Add(1, attribute.String("reason", activationFailReasonRestore))
}

// observeRegisterFailure 记录注册失败（目录 Register IO 错误/能力面缺席，
// register-first——激活闭包首步）导致的激活失败：Error 日志 +
// rans.activation.failed{reason=register} 计数器。与恢复失败同面
// （激活流程以失败收场、实体从未进入 Active 即计数），标签区分失败面。
func (m *Table) observeRegisterFailure(id rans.ID, err error) {
	log.Error("rans/activation: directory register failed; activation aborted",
		"uuid", id.String(), "err", err)
	activationFailedCounter().Add(1, attribute.String("reason", activationFailReasonRegister))
}
