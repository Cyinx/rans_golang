package pawnite

// 快照捕获的 loop 任务机制（规格 §15：周期 checkpoint 与钝化捕获的共用
// 原语——竞态根除面：捕获与 Handle 无并发）。捕获作为 loop 任务执行（串行即静默，
// 与 Handle 无并发），活动纪元（每次进入 Active 与每次投递各 +1）供
// manager 侧脏门控（活动纪元 > 已 checkpoint 纪元，闲置实体永不重写）。

import (
	"fmt"

	"github.com/cyinx/rans_golang/internal/log"
)

// captureReq 一次捕获请求：由 CaptureOnLoop 入队、loop goroutine 消费。
type captureReq struct {
	capture func() ([]byte, error)
	done    chan<- CaptureOutcome
}

// CaptureOutcome loop 捕获结果（done 通道载荷，恰送达一次）。
type CaptureOutcome struct {
	Snapshot []byte
	Epoch    int64 // 捕获时刻的活动纪元（成功时有效）
	Skipped  bool  // 拾取时已离开 Active（销毁/迁移/强弃抢占）——零副作用
	Err      error // 捕获闭包失败（panic 同折为 Err，loop 存活）
}

// CaptureOnLoop 把快照捕获排入 loop 任务：gate（mu）内校验 Active →
// 锁内入队（与状态转移互斥串行化，同 migrateCh 纪律）；拾取时复验
// Active，离开则 Skipped。capture 在 loop goroutine 上执行（静默点——
// 捕获之间由 loop 单线程天然串行，同实体捕获永不并发执行）；通道容量 1，
// 已有**待处理**捕获排队时拒绝入队（不堆积——调用方下轮再试）。IO 层的
// 「同实体永不重叠」由 manager 侧 in-flight 门承担（Put 在捕获结果收妥
// 后才进行）。done（调用方供，容量 ≥1）必达——实体终局路径
// （stopShutdown/迁移赢家/双强弃）与交接窗 core 起跑前的排空
// （runHandoff）把未拾取请求收场为 Skipped。
// 返回 false = gate 拒绝（非 Active / 已有待处理捕获 / 参数缺失），零副作用。
func (p *Pawnite) CaptureOnLoop(capture func() ([]byte, error), done chan<- CaptureOutcome) bool {
	if capture == nil || done == nil {
		return false
	}
	p.mu.Lock()
	if p.state != StateActive {
		p.mu.Unlock()
		return false
	}
	select {
	case p.captureCh <- &captureReq{capture: capture, done: done}:
		p.mu.Unlock()
		return true
	default: // 已有待处理捕获（容量 1 占用）：不堆积，调用方下轮再试
		p.mu.Unlock()
		return false
	}
}

// runCapture loop 任务侧执行：复验 Active（入队与拾取之间可能已让渡给
// 销毁/迁移）→ 捕获闭包（静默语境）→ 回捕获时刻纪元。纪元在 loop 上
// 只由 invoke/转入 Active 写，本读取无并发写方。
func (p *Pawnite) runCapture(req *captureReq) {
	p.mu.Lock()
	active := p.state == StateActive
	p.mu.Unlock()
	if !active {
		req.done <- CaptureOutcome{Skipped: true}
		return
	}
	snap, err := p.safeCapture(req.capture)
	if err != nil {
		req.done <- CaptureOutcome{Err: err}
		return
	}
	req.done <- CaptureOutcome{Snapshot: snap, Epoch: p.activityEpoch.Load()}
}

// safeCapture 包裹捕获闭包：panic 折为错误（捕获是尽力而为的优化路径，
// 对齐迁移 core 的 recover 纪律——不杀 loop）。
func (p *Pawnite) safeCapture(capture func() ([]byte, error)) (snap []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Warn("pawnite snapshot capture panicked, treating as failed",
				"uuid", p.id.String(), "panic", r)
			err = fmt.Errorf("pawnite capture panic: %v", r)
		}
	}()
	return capture()
}

// drainPendingCaptures 排空未拾取的捕获请求，两个共用契约的调用面：
// 终局路径（stopShutdown/迁移赢家/双强弃——此后 loop 不再拾取）与交接窗
// core 起跑前（runHandoff——core 是 loop 任务，排队者的收尾要等 loop 让出
// core 才能被驱动，不排空则 core 内等在飞 checkpoint 的门自锁至预算耗尽）。
// Active 校验与入队同持 mu、窗开（pending+Destroying）同
// 临界区：排空点之后到窗闭/终局前不可能再有新请求入队。排空者以 Skipped
// 收场（零副作用，调用方下轮再试）；done 容量 ≥1 且每请求恰一送达，发送
// 不阻塞。已拾取且在跑的捕获不在通道内，其 done 由 runCapture 自行送达。
func (p *Pawnite) drainPendingCaptures() {
	for {
		select {
		case req := <-p.captureCh:
			req.done <- CaptureOutcome{Skipped: true}
		default:
			return
		}
	}
}

// Epoch 活动纪元（脏门控依据）：进入 Active 记 1，此后每次投递 +1。
// 原子读无锁。
func (p *Pawnite) Epoch() int64 { return p.activityEpoch.Load() }
