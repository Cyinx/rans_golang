package activation

// 周期 checkpoint 的表内簿记与协作面（规格 §15 P0；
// 扫轮与捕获复合体在 internal/lifecycle/sweeper.go，本文件保留实例级的
// 纪元/在飞门簿记——ckptEpoch 已 checkpoint 的活动纪元（脏门控下界）与
// ckptInFlight 同实体 IO 在飞门——及其纪元等待/占门协作面）。簿记写入方：
// 激活闭包的恢复路径（restoreStore/restorePackage 落库成功即标记干净，见
// activation.go）与 lifecycle 收尾 goroutine（EndCheckpoint/AbortCheckpoint）。

import (
	"context"
	"time"

	"github.com/cyinx/rans_golang/internal/pawnite"
)

// checkpointSettlePollInterval 在飞 checkpoint 等待面的轮询间隔（等待的是
// 单次 Put 收尾——毫秒级；轮询而非改造 ckptInFlight 为通知面，保持实例内
// 普通 CAS 门的最小形态）。
const checkpointSettlePollInterval = 2 * time.Millisecond

// AwaitCheckpointSettled 等待本实例的在飞周期 checkpoint 落定（《加固 spec》
// §5.1 第 2 点：交接窗 core 的第一步）。轮询实例的 ckptInFlight 门，受交接
// 窗预算 ctx 约束——预算耗尽返回 ctx.Err()（core 据此判输/中止，不带着
// 在飞 IO 强行转换）。本方法不启动任何新捕获；进入交接窗后周期捕获不再
// 被接受（CaptureOnLoop 要求 Active——窗内非 Active 天然拒），故返回 nil
// 即等价于「清零后不会再有更旧的同纪元快照落库」。
//
// 实例不在本地表（未激活/已逐出/他端持有）视为已落定并返回 nil——本表
// 没有以该实例名义在飞的周期 Put。返回非 nil 错误时实例状态未被改动
// （纯读面）。
func (m *Table) AwaitCheckpointSettled(ctx context.Context, p *pawnite.Pawnite) error {
	if p == nil {
		return nil
	}
	inst := m.peekLocal(p.UUID())
	if inst == nil {
		return nil
	}
	for inst.ckptInFlight.Load() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(checkpointSettlePollInterval):
		}
	}
	return nil
}

// TryBeginCheckpoint 周期捕获的占门面（协作面，lifecycle 扫轮调用）：脏判
// （活动纪元 > 已 checkpoint 纪元——闲置实体永不重写）与在飞 CAS 合一，
// 命中即占住在飞门并报告 true；调用方必须以 EndCheckpoint（落库收妥）或
// AbortCheckpoint（捕获被拒/失败）收尾，门才归还。
func (m *Table) TryBeginCheckpoint(p *pawnite.Pawnite) bool {
	inst := m.peekLocal(p.UUID())
	if inst == nil {
		return false
	}
	if inst.ckptInFlight.Load() {
		return false // IO 在飞：同实体永不重叠
	}
	if p.Epoch() <= inst.ckptEpoch.Load() {
		return false // 干净：活动纪元未越过已 checkpoint 纪元
	}
	return inst.ckptInFlight.CompareAndSwap(false, true) // 并发扫轮竞态：让先手
}

// EndCheckpoint 占门后的成功收尾：把已 checkpoint 纪元抬到捕获纪元并归还
// 在飞门。空快照同此路径（该纪元无内容可写：视为已 checkpoint）。
func (m *Table) EndCheckpoint(p *pawnite.Pawnite, epoch int64) {
	if inst := m.peekLocal(p.UUID()); inst != nil {
		inst.ckptEpoch.Store(epoch)
		inst.ckptInFlight.Store(false)
	}
}

// AbortCheckpoint 占门后的失败收尾：不记纪元（下轮重扫重试）只归还门。
func (m *Table) AbortCheckpoint(p *pawnite.Pawnite) {
	if inst := m.peekLocal(p.UUID()); inst != nil {
		inst.ckptInFlight.Store(false)
	}
}
