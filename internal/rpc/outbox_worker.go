package rpc

// 目标 worker 的发送腿与熔断状态机（票 04；核心队列/入箱面见 outbox.go）。

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cyinx/rans_golang/internal/wire"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/log"
)

// sendOutcome 发送结果落位：成功且熔断中 → 闭合；失败 → 开断（清扫箱
// 内帧——调用帧回投「确定未送出」、单向帧丢弃计数）。
func (o *Outbox) sendOutcome(q *targetQueue, target rans.NodeID, err error) {
	if err == nil {
		q.mu.Lock()
		if q.open {
			o.openCount.Add(-1)
		}
		q.open, q.probing, q.fails = false, false, 0
		q.mu.Unlock()
		return
	}
	o.sendFailed.Add(1)
	// ctx 类失败不开断（票 04）：调用方取消/预算耗尽是本帧自身的收场，
	// 不是目标死亡的信号（包丢失注入等慢路径场景会被误伤——把健康目标
	// 误判为死目标，后续流量被冷却窗快速失败）。仅端点重试耗尽的非 ctx
	// 错误（网络不可达/判停）才开断。
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	q.mu.Lock()
	q.fails++
	hit := q.fails >= outboxOpenThreshold
	q.mu.Unlock()
	if !hit {
		return // 单次瞬态失败：计数等待下一次佐证
	}
	log.Warn("rans/rpc: outbox send failed; opening circuit breaker",
		"node", target, "err", err)
	o.openBreaker(q, target)
}

// openBreaker 开断（幂等重入安全）：置熔断态并清扫本目标箱内帧。熔断未
// 闭合过的再开断（探针失败复开——probing 窗口内后续帧照常入队）不重复
// 计数，在箱帧照常清扫——开断瞬间在箱帧一律确定未送出。
func (o *Outbox) openBreaker(q *targetQueue, target rans.NodeID) {
	q.mu.Lock()
	already := q.open // probing 蕴含 open：探针失败复开属「未闭合的再开断」
	q.open, q.probing, q.fails = true, false, 0
	q.openAt = time.Now()
	pending := q.queue
	q.queue = nil
	q.mu.Unlock()
	if !already {
		o.openCount.Add(1)
		o.breakerOpened.Add(1)
	}
	if n := len(pending); n > 0 {
		o.inBox.Add(-int64(n))
		o.signalSpace()
		o.signalTargetSpace(q)
		for _, it := range pending {
			o.failFrame(it, errOutboxBreaker)
		}
	}
}

// failFrame 熔断/清扫收场：调用帧回投「确定未送出」（Unavailable+
// SendFailed——调用方保留无条件重试权），单向帧丢弃计数。
func (o *Outbox) failFrame(it outboxItem, cause error) {
	if it.fail != nil {
		it.fail(fmt.Errorf("rans: outbox: node %d unreachable: %w: %w",
			it.target, errSendFailed, cause))
		return
	}
	o.dropped.Add(1)
}

// sendOne 单向帧发送：预算 ctx（关箱取消贯穿）调注入面；扩展块预算按
// 剩余重写（票 05）。
func (o *Outbox) sendOne(q *targetQueue, it outboxItem) {
	ctx, cancel := context.WithTimeout(o.runCtx, o.cfg.SendBudget)
	defer cancel()
	restampBudget(&it.hdr, it.dl)
	o.sendOutcome(q, it.target, o.cfg.Send(ctx, it.target, it.hdr, it.body))
}

// sendCall 调用帧发送腿：发送 ctx 以防御预算封顶调用方 ctx（WithTimeout
// 派生——deadline 自动取两者较早者，到期 Err=DeadlineExceeded 经发送面
// 收场 Timeout/SendFailed 可重试类，而非主动取消 Canceled 丧失重试权）；
// 关箱取消面单独看护（runCtx 结束即收掉在途发送——runCtx 不是调用方 ctx
// 的父，取消不贯穿）。排队过期的帧在此中止——确定未送出；失败经 fail
// 回投（熔断清扫另有 failFrame 面不发 Send）。
func (o *Outbox) sendCall(q *targetQueue, it outboxItem) {
	ctx, cancel := context.WithTimeout(it.ctx, o.cfg.SendBudget)
	done := make(chan struct{})
	go func() { // 关箱取消面：runCtx 结束即收掉在途发送
		select {
		case <-o.runCtx.Done():
			cancel()
		case <-done:
		}
	}()
	defer func() {
		close(done)
		cancel()
	}()
	// WithTimeout 派生恒带 deadline（min(调用方, 预算)）——收端 budget_ms
	// 重写锚与发送面中止判据同源于此。
	deadline, _ := ctx.Deadline()
	restampBudget(&it.hdr, deadline)
	err := o.cfg.SendCall(ctx, it.target, it.hdr, it.body, deadline)
	o.sendOutcome(q, it.target, err)
	if err != nil {
		it.fail(err)
	}
}

// restampBudget 发送时刻重写扩展块预算（票 05）：budget_ms = 至 deadline
// 的剩余毫秒——排队耗时从预算中扣除，收端按「到达+剩余」锚定，不放大调
// 用方预算。无扩展块/无预算字段/零 deadline/解码失败一律原样通过（保守，
// 保持既有线形态）。
func restampBudget(hdr *wire.Header, dl time.Time) {
	if dl.IsZero() || hdr.Ext == nil {
		return
	}
	e, err := wire.DecodeRequestExt(hdr.Ext)
	if err != nil || !e.HasBudget {
		return
	}
	rem := time.Until(dl)
	if rem < 0 {
		rem = 0
	}
	e.BudgetMS = uint64(rem.Milliseconds())
	hdr.Ext = wire.EncodeRequestExt(e)
}

// signalTargetSpace 目标级空间广播（每目标上限的等待者唤醒）。
func (o *Outbox) signalTargetSpace(q *targetQueue) {
	q.mu.Lock()
	close(q.spaceCh)
	q.spaceCh = make(chan struct{})
	q.mu.Unlock()
}

// signalSpace 全箱空间广播。
func (o *Outbox) signalSpace() {
	o.spaceMu.Lock()
	close(o.spaceCh)
	o.spaceCh = make(chan struct{})
	o.spaceMu.Unlock()
}

// NotifyTargetDown 端点判停的外部通知面（Remote 的 onEndpointTerminated
// 接线）：目标立即开断（不等发送腿自然失败）。
func (o *Outbox) NotifyTargetDown(target rans.NodeID) {
	if q, ok := o.queueFor(target); ok {
		o.openBreaker(q, target)
	}
}

// NotifyNodeAlive 目标复活/可达的外部通知面（票 01 成员视图事件接线；
// 票 04 的即时接线是 dispatcher 的入站可达性通知——收到某节点帧即闭合
// 该目标熔断，分区恢复不等待冷却探针）：即时闭合熔断。无任何开断目标
// 时零成本返回（逐帧调用面）。
func (o *Outbox) NotifyNodeAlive(target rans.NodeID) {
	if o.openCount.Load() == 0 {
		return
	}
	q, ok := o.queueFor(target)
	if !ok {
		return
	}
	q.mu.Lock()
	if q.open {
		o.openCount.Add(-1)
	}
	q.open, q.probing, q.fails = false, false, 0 // 终审：复活收场清计数（残留瞬态失败会让下次单败即开断）
	q.mu.Unlock()
}

// Close 幂等收口：落关箱闸、逐目标清扫在箱帧（计数丢弃，与置位同临界
// 区）、取消在途发送、等 worker 全退。
func (o *Outbox) Close() {
	o.stopOnce.Do(func() {
		close(o.stopCh)
		o.tmu.Lock()
		o.closed = true
		queues := make([]*targetQueue, 0, len(o.targets))
		for _, q := range o.targets {
			queues = append(queues, q)
		}
		o.tmu.Unlock()
		dropped := 0
		for _, q := range queues {
			q.mu.Lock()
			q.stopped = true
			dropped += len(q.queue)
			q.queue = nil
			q.mu.Unlock()
			q.cond.Broadcast()
		}
		if dropped > 0 {
			o.inBox.Add(-int64(dropped))
		}
		o.runCancel()
		o.wg.Wait()
		if dropped > 0 {
			o.dropped.Add(float64(dropped))
			log.Warn("rans/rpc: outbox closed with pending items", "dropped", dropped)
		}
	})
}
