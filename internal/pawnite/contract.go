// 包级契约：终局原因、入站投递抽象、panic 监管策略与构造选项。
package pawnite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cyinx/rans_golang"
)

// TerminalReason 实例终局原因：全生命周期恰一次生效（setTerminalLocked
// 写一次，先到的终局裁决权威）；迟到投递按本原因经 disposeLate 矩阵分流。
type TerminalReason int

const (
	// TerminalRetired 优雅销毁（Destroy 主路径与 Start 前快路径）。
	// 零值编码：与「未写」同值——写点保证 Retired 只随终态翻转的同一临界区
	// 写入，此后各写点（强弃 gate）在终态让位，零值歧义无实效（见
	// setTerminalLocked 的写点次序说明）。
	TerminalRetired TerminalReason = iota
	// TerminalMigrated 迁移换锁成功：目录所有权已让渡给 target 节点。
	TerminalMigrated
	// TerminalArbitrationLost 激活仲裁落败：目录归属在 target（赢家）。
	TerminalArbitrationLost
	// TerminalAbandoned 看门狗强弃（执行/卸载/激活卡死；在途可能已部分执行）。
	TerminalAbandoned
	// TerminalAbandonedInHandoff 交接窗内看门狗强弃（《加固 spec》§5.1）：
	// 在箱消息从未执行过（core 未跑到或已被取消面通知）——迟到处置按
	// Unavailable 回绝（区别于 TerminalAbandoned 的 Timeout：后者在途可能
	// 已部分执行，规格只承诺不重复；此处是确定未执行，可重试落新激活）。
	TerminalAbandonedInHandoff
	// TerminalActivationFailed 激活失败终局（run 激活失败分支的非仲裁
	// 路径）：注册之后的失败都发生在执行之前——迟到处置按 Unavailable
	// 回绝（确定未执行），与激活失败排空行同口径。
	TerminalActivationFailed
)

// terminal 终局记录（mu 保护）：reason 恰一次写入；target 是 Migrated/
// ArbitrationLost 行迟到投递的转发目标，其余原因为零值。
type terminal struct {
	reason TerminalReason
	target rans.NodeID
}

// InboundDelivery 是入站投递的网络无关抽象（规格 §4.4）。
// 本地调用与远端请求同接口，收件箱逻辑可脱离网络单测。
// Reject 时序契约：Reject 可能晚于正常完成到达——投递已
// Deliver 成功后，panic 监管（invoke 的 recover）或看门狗强弃的解绑仍会
// 调 Reject——实现必须非阻塞且幂等（迟到的 Reject 不得 panic、不得二次
// 收尾；仓内实现经 sync.Once 守护资源释放，见 remote.remoteDelivery）。
// Reject 的 reason 面是 rans.Reason（统一错误模型）：回绝按
// 约定 spec §5.3 映射表携带 (Code, Reason)——activation 排空 →
// ActivationFailed、销毁中 → Deactivating、失租 → LeaseLost、满箱 →
// Busy；远端投递的实现把 (Code, Reason) 原样写进结构化 Error 帧体。
type InboundDelivery interface {
	UUID() rans.ID
	Deliver(p *Pawnite)
	Reject(status rans.Code, reason rans.Reason)
}

// Expirable 投递的执行预算自描述（H10 决策 7，可选接口）：实现者声明自己
// 的执行 deadline（本地腿 = 调用方合成 ctx 的 deadline，远端腿 = 收到时刻
// + 过线 budget_ms）；出队执行入口 invoke 据此复查「入箱后睡过预算」——
// 已耗尽不执行、按 (CodeTimeout, ReasonExpired) 回绝，本地/远端同检查点。
type Expirable interface {
	DeliveryDeadline() (time.Time, bool)
}

// CancelAware 在箱取消标记感知（H10 决策 9，可选接口）：实现者声明自己可
// 被 MsgCancel 标记取消（本地投递不实现——本地取消经 ctx 穿透无需消息）。
// 出队执行入口 invoke 据此弃置已标记的投递。
type CancelAware interface {
	DeliveryCanceled() bool
}

// CycleAware 因果路径环判定的投递携带面（H12 决策 15，可选接口）：实现者
// 声明自己的入站因果路径（实体身份 FNV-1a 64 哈希序列，途经序）。准入边界
// ReceiveRequest 据此判定「路径含目标实体自身」——命中即 (CodeError,
// ReasonCycle) 回绝：消息未执行、不可重试（重试必然再成环）、文本带完整环
// （渲染归投递实现的 Reject，见 CycleRejectText）。单向投递不实现（Tell 不
// 入链，路径恒空）。
type CycleAware interface {
	DeliveryCausalPath() []uint64
}

// CycleRejectText 环回绝的诊断文本（H12 决策 15「文本带完整环」）：按途经
// 序展示路径哈希并以目标自身收尾（收尾项与命中位闭合）。哈希为 FNV-1a 64
// 的 16 位 hex（rans.ID.Hash 同源，反查身份归持有方）；判定面是 (Code,
// Reason)，文本仅供排障。
func CycleRejectText(path []uint64, target rans.ID) string {
	var b strings.Builder
	b.WriteString("rans: call cycle detected:")
	for _, hop := range path {
		b.WriteString(" ")
		fmt.Fprintf(&b, "%016x", hop)
		b.WriteString(" ->")
	}
	fmt.Fprintf(&b, " %016x", target.Hash())
	return b.String()
}

// ArbitrationLost：激活因目录仲裁落败而失败的标志接口（由 manager 层错误
// 实现；pawnite 用 errors.As 探测。规格 §12；接口放本包避免 pawnite→manager
// 反向依赖）。
type ArbitrationLost interface {
	error
	WinnerNode() rans.NodeID
}

// Forwardable：输家整箱转发能力的可选投递接口（远程投递实现——持有原始
// body 字节，转发给赢家；本地投递不实现即按 Unavailable 回绝。规格 §12）。
type Forwardable interface {
	Forward(winnerNode rans.NodeID)
}

// PanicPolicy panic 监管策略。
// 适用面（两策略的共同底盘，差异只在 invoke 分支）：
//   - invoke（投递执行）panic：Default=回结对端后 loop 继续服役（实体
//     健康度由宿主自负）；Isolate=回结对端后就地销毁实体（隔离——
//     panic 视为实体状态已损的信号，下次激活从 checkpoint/Store 重建）；
//   - Activate panic：一律视为激活失败 → 直达终态（规格 §12）；
//   - Deactivate/OnEvict panic：一律吞掉记日志、终态照达；
//   - 迁移 core panic：一律转为迁移输（可放弃的优化路径，见 migrate.go）。
type PanicPolicy int

const (
	// PanicPolicyDefault 基础监管（零值）：invoke
	// panic → Reject(StatusError) 后 loop 存活继续服役。
	PanicPolicyDefault PanicPolicy = iota
	// PanicPolicyIsolate 隔离监管：invoke panic → Reject(StatusError) 后
	// 销毁实体（Done 关闭、后续准入回绝）——对"panic 意味着实体内部状态
	// 已损"的实体（宿主裁定）防止带病继续服役。hook panic 行为与
	// Default 完全一致（见 PanicPolicy 适用面注释）。
	PanicPolicyIsolate
)

type Options struct {
	Activate   func(ctx context.Context, p *Pawnite) error
	Deactivate func(ctx context.Context, p *Pawnite) error
	OnEvict    func(p *Pawnite)
	InboxCap   int
	// OnExpired 出队过期即弃事件钩子（H10 决策 7）：消息因预算耗尽被弃置
	// 时触发——rans.call.expired 计数的接线点（经表装配注入）。nil = 不
	// 观测；必须非阻塞（loop goroutine 上执行）。
	OnExpired func()
	// OnCanceled 出队取消弃置事件钩子（H10 决策 9）：rans.call.canceled 计数
	// 接线点。nil = 不观测；必须非阻塞（loop 上执行）。
	OnCanceled func()
	// PanicPolicy panic 监管策略（零值 = PanicPolicyDefault，既有行为
	// 不变）。投入使用前配置（与 hooks 同族约定）。
	PanicPolicy PanicPolicy
	// Incarnation 本 incarnation 的 fencing 纪元。两条落位路径：直接构造
	// 时经本字段注入（构造即生效）；manager 激活路径在激活闭包首步取号后
	// 经 AssignIncarnation 首写落位（取号 IO 移出创建锁的代价——落位点从
	// 构造期推迟到 run goroutine 起跑后）。两条路径下落位后都不可变。
	// 0 = 未围栏（目录无纪元源的裸形态）。宿主读取面是 ctx
	//（经 rans.Incarnation；写入面 internal/ctxkey——钩子 ctx 与 Handle ctx 都携带）；本访问面供
	// 投递层接线（manager/rpc 的 Deliver 包装）。
	Incarnation uint64
	// Now/NewTimer 定时器时钟注入面（测试虚拟时钟专用；生产留 nil = 真实
	// 时钟）。两者须同给：函数形态而非接口，是为跨包（测试注入面 → 装配根
	// → 本包）直通而无需在公开面新增类型。NewTimer 返回触发通道、停止面与
	// 重置面，Stop/Reset 语义同 time.Timer。
	Now      func() time.Time
	NewTimer func(d time.Duration) (c <-chan time.Time, stop func() bool, reset func(d time.Duration) bool)
	// MaxTimers 单实例待触发定时器上限（≤0 → DefaultMaxTimers）。超限
	// panic：它是「无界武装」缺陷的护栏，不是容量规划面。
	MaxTimers int
}

// DefaultMaxTimers 单实例定时器缺省上限（约 9 MB 上界；十万级用法依
// MaxTimers 显式抬高）。
const DefaultMaxTimers = 65536
