package rpc

// 节点发件箱（票 04 重构：按目标分队列）：每个目标节点一条 FIFO 队列 +
// 按需起停的专属 worker——跨目标互不阻塞（哈希分片的同分片队头阻塞随分
// 片模型消亡），同目标 FIFO 保持（C1「先 Tell 后 Call」承诺载体）。容量
// 两级：全箱共享（Outbox.Capacity）+ 每目标上限（防单目标独占全箱）；控
// 制面帧（MsgOwnershipLost/MsgRedirectHint 经 TryPut）走独立预留容量，数
// 据帧占满时仍可入箱。死目标熔断：发送重试耗尽或端点判停 → 该目标熔断
// ——箱内帧立即按「确定未送出」收场（调用帧回投 Unavailable+SendFailed、
// 单向帧丢弃计数），熔断期间新入箱快速失败；冷却期满放行一帧探测、成功
// 即闭合，成员视图换代/复活事件（票 01 接线 NotifyNodeAlive）即时闭合。
//
// 发送面由组装根注入（Send/SendCall 接网关端点；重试窗口在端点内部——
// 发件箱看到的是重试耗尽后的最终错误，即熔断触发判据）。body 所有权随
// 入箱移交发件箱、随发送成功移交端点或随弃置/熔断清扫释放——与
// SendOneWay 同口径，不得复用已归还缓冲。
//
// 生命周期：worker 按需起（首帧入队）停（队列排空），Close 幂等收口
// ——在箱帧计数丢弃（dropped）、在途发送经 runCtx 取消（sendCall 的发送
// ctx 同时受调用方预算与关箱取消约束）。优雅停机先经停序③ flush 等箱空
// 再 Close（Remote.Shutdown 装配）。

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// 发件箱缺省参数与 worker 发送预算。
const (
	// defaultOutboxCapacity 全箱共享容量缺省（条）。
	defaultOutboxCapacity = 65536
	// outboxPerTargetDiv/Floor 每目标上限 = max(容量/Div, Floor)——防单
	// 目标独占全箱（死目标熔断前的排队上界）。
	outboxPerTargetDiv   = 8
	outboxPerTargetFloor = 64
	// outboxControlReserve 控制面帧的独立预留容量（全箱容量之上的余量
	// 段位，仅控制帧可占；控制帧同时豁免每目标上限）。
	outboxControlReserve = 256
	// outboxOpenThreshold 连续发送失败开断阈值：单次失败可能是瞬态抖动
	//（连接更替/注入丢包），连续两次才判目标死亡。端点判停通知
	//（NotifyTargetDown）本身是重试耗尽的强信号，不受阈值约束。
	outboxOpenThreshold = 2
	// outboxBreakerCooldown 熔断冷却：期满后下一次入箱即半开探测（该帧
	// 经真实发送面，成败决定闭合/复开）。取短值（100ms）：分区恢复/目标
	// 重启后的首个调用即探针——快速恢复可达性；死目标高频流量下每冷却
	// 期只放行一帧进端点重试，代价有界。成员视图事件（票 01 接线
	// NotifyNodeAlive）是即时闭合的正解，冷却是无线索时的兜底。
	outboxBreakerCooldown = 100 * time.Millisecond
	// outboxSendBudget 单帧发送的防御预算（端点韧性重试窗口的上界；
	// 调用帧另受调用方预算封顶，取较早者）。
	outboxSendBudget = 30 * time.Second
)

// errOutboxClosed 关箱后的入箱拒绝（停机窗口竞态）：plain 错误——分类
// 唯一表 NoRetry（停机终态不重试）。
var errOutboxClosed = errors.New("rans: outbox: closed (node shutting down)")

// errOutboxFull 非阻塞入箱（TryPut）的满箱丢弃哨兵：满即丢是决策 12 的
// 固定策略而非故障，消费方据此留痕继续，计数落 rans.outbox.dropped。
var errOutboxFull = errors.New("rans: outbox: full (non-blocking producer drop)")

// errOutboxBreaker 目标熔断的入箱拒绝：该目标判停（发送重试耗尽/端点判
// 停），帧确定未入箱——快速失败而非排队干等。
var errOutboxBreaker = errors.New("rans: outbox: target circuit breaker open")

// OutboxConfig 发件箱构造参数（内部装配面——组装收敛于 Remote.New）。
type OutboxConfig struct {
	// Capacity 全箱共享容量（在箱条目上限，跨目标合计）；≤0 →
	// defaultOutboxCapacity。每目标上限由此派生（见 outboxPerTargetDiv）。
	Capacity int
	// Send 发送面（必填）：一帧一次端点发送（路由+发送的韧性在实现内
	// 部）；Remote 装配网关的 SendOneWay，测试注入替身。
	Send func(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte) error
	// SendCall 调用帧发送面（PutCall 的 worker 发送腿）：与 Send 同一端点
	// 韧性，携带请求 deadline——过期即中止（确定未送出），失败经帧内
	// fail 回投等待方。PutCall 使用时必填（nil → 入箱拒绝）。
	SendCall func(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte, deadline time.Time) error
	// Metrics 指标注册表；nil → metrics.Default()。
	Metrics *metrics.Registry
	// SendBudget 单帧发送的防御预算（0 → outboxSendBudget；生产装配
	// Remote.New 不设置取缺省，测试注入短预算使到期用例在一个预算窗内
	// 收场——与网关 sweepInterval 的周期注入同形态）。
	SendBudget time.Duration
}

// outboxItem 在箱帧：body 所有权已移交发件箱（见包注释契约）。ctx/fail
// 仅调用帧（PutCall）非零：ctx 携调用方取消与预算（worker 发送腿的上
// 界），fail 回投发送失败/熔断收场（响应不走此面，仍归网关 pending）。
type outboxItem struct {
	target rans.NodeID
	hdr    wire.Header
	body   []byte
	ctx    context.Context
	fail   func(error)
	// dl 帧预算 deadline（票 05）：写出时刻的过期判据与 budget_ms 重写锚
	//（排队耗时扣除——收端以「到达+剩余」锚定，不放大调用方预算）；
	// 零值 = 无预算（TryPut 生产者与无 deadline ctx）。
	dl time.Time
}

// targetQueue 单目标的 FIFO 队列 + 按需 worker 运行态 + 熔断状态机。
type targetQueue struct {
	mu    sync.Mutex
	cond  *sync.Cond // worker 的空队等待（入队 Signal、关箱 Broadcast）
	queue []outboxItem
	// worker worker 在跑（空队退出清零；首帧入队时按需起）。
	worker bool
	// stopped 关箱标志（Close 与清扫同临界区置位——此后入箱必被拒）。
	stopped bool
	// 熔断状态（mu 保护）：open=熔断中（新入箱快速失败、箱内帧被清
	// 扫）；probing=半开（冷却期满已放行一帧探测，成败决闭合/复开）；
	// openAt 复开计时锚。
	open    bool
	probing bool
	openAt  time.Time
	// fails 连续发送失败计数（mu 保护；成功清零）：达 outboxOpenThreshold
	// 才开断——单次瞬态失败（连接抖动/包丢失注入）不毒化后续流量。
	fails int
	// spaceCh 每目标上限的满等待者唤醒通道（关旧建新广播——与全箱
	// spaceCh 同机制的目标级副本；worker 出箱即触发）。
	spaceCh chan struct{}
}

// Outbox 节点级有界发件箱。Put 入箱即返回（满箱阻塞段见方法注释）；
// TryPut 非阻塞生产者面（满即丢计数）；Depth 为排空等待面；Close 幂等
// 收口。并发安全。
type Outbox struct {
	cfg       OutboxConfig
	cap       int
	perTarget int

	tmu     sync.Mutex
	targets map[rans.NodeID]*targetQueue
	closed  bool // tmu：关箱后 queueFor 不再新建（Close 清扫与入箱的竞态收口）

	// inBox 全箱共享计数（容量唯一闸门——跨目标合计）：入箱 CAS +1、
	// 出箱/弃置/熔断清扫 -1；深度 gauge 与停序 flush 读同一原子。
	inBox atomic.Int64

	// openCount 当前开断目标数（入站可达性通知的零成本快速路径：无熔断
	// 即直接返回——逐帧通知不进锁）。
	openCount atomic.Int64

	// 空间广播面（满箱等待者的唤醒通道）：任一帧出箱即关旧建新。
	spaceMu sync.Mutex
	spaceCh chan struct{}

	sendFailed    *metrics.Counter
	dropped       *metrics.Counter
	breakerOpened *metrics.Counter
	expired       *metrics.Counter

	runCtx    context.Context
	runCancel context.CancelFunc
	stopCh    chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

// NewOutbox 构造发件箱（worker 按需起停，无常驻集）。Send 必填（nil 拒
// 绝构造——无发送面的发件箱是配置事故）。
func NewOutbox(cfg OutboxConfig) (*Outbox, error) {
	if cfg.Send == nil {
		return nil, errors.New("rans: outbox: Send is required")
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = defaultOutboxCapacity
	}
	if cfg.SendBudget <= 0 {
		cfg.SendBudget = outboxSendBudget
	}
	reg := cfg.Metrics
	if reg == nil {
		reg = metrics.Default()
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	o := &Outbox{
		cfg:       cfg,
		cap:       cfg.Capacity,
		perTarget: max(cfg.Capacity/outboxPerTargetDiv, outboxPerTargetFloor),
		targets:   map[rans.NodeID]*targetQueue{},
		spaceCh:   make(chan struct{}),
		runCtx:    runCtx,
		runCancel: runCancel,
		stopCh:    make(chan struct{}),
	}
	o.sendFailed = reg.Counter(metrics.NameOutboxSendFailed,
		"outbox worker send attempts failed (retry window exhausted or close-canceled)")
	o.dropped = reg.Counter(metrics.NameOutboxDropped,
		"frames dropped after enqueue without sending (close abandonment; capacity/breaker drops land here too)")
	o.breakerOpened = reg.Counter(metrics.NameOutboxBreakerOpened,
		"per-target circuit breaker openings (send retries exhausted or endpoint terminated)")
	o.expired = reg.Counter(metrics.NameCallExpired,
		"frames dropped before execution on budget expiry (pawnite dequeue + outbox write-out)")
	reg.Gauge(metrics.NameOutboxDepth, "outbox entries waiting to be sent (all targets)",
		func() float64 { return float64(o.inBox.Load()) })
	return o, nil
}

// queueFor 目标队列 registry（无则建；队列随目标集常驻——节点号空间有
// 界，空队仅占微小结构）。关箱后不再新建（返回 false）：Close 的清扫快
// 照与入箱的建队竞态由此收口——关箱后入箱一律 errOutboxClosed，杜绝「清
// 扫后才建的队列漏清/漏计」与 wg.Add 与 Wait 的并发误用。
func (o *Outbox) queueFor(target rans.NodeID) (*targetQueue, bool) {
	o.tmu.Lock()
	defer o.tmu.Unlock()
	if o.closed {
		return nil, false
	}
	q := o.targets[target]
	if q == nil {
		q = &targetQueue{spaceCh: make(chan struct{})}
		q.cond = sync.NewCond(&q.mu)
		o.targets[target] = q
	}
	return q, true
}

// isControlFrame 控制面帧判定（预留容量的适用面）：归属通知与重定向提
// 示——正确性承载帧，满箱不可丢；数据帧（请求/应答/单向/迁移包）不在列。
func isControlFrame(hdr wire.Header) bool {
	return hdr.Type == protocol.MsgOwnershipLost || hdr.Type == protocol.MsgRedirectHint
}

// Put 入箱一帧：箱有位即返回（入箱即返回语义）；满箱（全箱或本目标上
// 限）阻塞至腾位、关箱或 ctx 结束；目标熔断即快速失败（确定未投递）。
// body 所有权随本调用移交（见包注释）。
func (o *Outbox) Put(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte) error {
	return o.putBlocking(ctx, outboxItem{target: target, hdr: hdr, body: body})
}

// PutCall 入箱一帧请求-响应调用帧：与 Put 同一满箱/熔断语义，差异仅在
// item 携调用方 ctx 与失败回投面（worker 发送腿见 sendCall）。
func (o *Outbox) PutCall(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte, fail func(error)) error {
	if o.cfg.SendCall == nil {
		return errors.New("rans: outbox: SendCall is required for call frames")
	}
	if fail == nil {
		return errors.New("rans: outbox: PutCall requires a fail callback")
	}
	return o.putBlocking(ctx, outboxItem{target: target, hdr: hdr, body: body, ctx: ctx, fail: fail})
}

// putBlocking 阻塞生产者（Put/PutCall）的满箱等待共核。
func (o *Outbox) putBlocking(ctx context.Context, it outboxItem) error {
	select {
	case <-o.stopCh:
		return errOutboxClosed
	default:
	}
	it.dl, _ = ctx.Deadline() // 票 05：写出时刻的过期/重写锚（无则零值）
	for {
		q, state, err := o.enqueueItem(it, false)
		if state == enqueued {
			return nil
		}
		if state == breakerOpen {
			// 阻塞生产者不吃快速失败（票 04 收口）：等冷却余量后重试——本
			// 帧成为半开探针（分区恢复的自愈面；TryPut 的非阻塞生产者仍
			// 即刻拒绝）。等待受关箱与调用方 ctx 收口。
			q.mu.Lock()
			wait := outboxBreakerCooldown - time.Since(q.openAt)
			q.mu.Unlock()
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-o.stopCh:
					timer.Stop()
					return errOutboxClosed
				case <-ctx.Done():
					timer.Stop()
					return o.putOutcome(it.target, ctx)
				}
			}
			continue
		}
		if err != nil {
			return err
		}
		if state == rejected {
			return errOutboxClosed
		}
		var space chan struct{}
		if state == globalFull {
			o.spaceMu.Lock()
			space = o.spaceCh
			o.spaceMu.Unlock()
		} else { // targetFull：本目标上限的腾位等待
			q.mu.Lock()
			space = q.spaceCh
			q.mu.Unlock()
		}
		select {
		case <-space:
		case <-o.stopCh:
			return errOutboxClosed
		case <-ctx.Done():
			return o.putOutcome(it.target, ctx)
		}
	}
}

// TryPut 非阻塞入箱一帧（事件扇出/错误回包/仲裁输家转发/正常应答帧）：
// 箱有位即入；满即丢——计 rans.outbox.dropped 后返回 errOutboxFull，绝不
// 等待；控制面帧走预留容量段位（数据帧占满仍可入）；关箱拒绝；目标熔
// 断拒绝（控制帧同拒——死目标不会处理它）。满箱/熔断丢弃时 body 所有
// 权不移交。
func (o *Outbox) TryPut(target rans.NodeID, hdr wire.Header, body []byte) error {
	select {
	case <-o.stopCh:
		return errOutboxClosed
	default:
	}
	_, state, err := o.enqueueItem(outboxItem{target: target, hdr: hdr, body: body}, true)
	if err != nil {
		if state == breakerOpen {
			o.dropped.Add(1)
		}
		return err
	}
	if state == enqueued {
		return nil
	}
	o.dropped.Add(1)
	return errOutboxFull
}

// enqueueState 入箱临界区的结论。
type enqueueState int

const (
	enqueued enqueueState = iota
	globalFull
	targetFull
	breakerOpen
	rejected
)

// enqueueItem 占位入队共核（全部生产者的临界区单点）。熔断目标快速失败
// （冷却期满转半开放行一帧探测——errOutboxBreaker 包装 errSendFailed，
// 调用方得到「确定未投递」分类；容量拒收则回滚半开标记，探测资格不残
// 留）；容量两级（全箱 CAS 占位、每目标上限在队列锁内复查）；关箱竞态
// 回退占位。
func (o *Outbox) enqueueItem(it outboxItem, try bool) (*targetQueue, enqueueState, error) {
	q, ok := o.queueFor(it.target)
	if !ok {
		return nil, rejected, errOutboxClosed
	}
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return q, rejected, errOutboxClosed
	}
	probe := false // 本帧是否被置为探测帧（容量拒收时须回滚半开标记）
	if q.open && !q.probing {
		if time.Since(q.openAt) >= outboxBreakerCooldown {
			q.probing = true // 半开：本帧即探测帧
			probe = true
		} else {
			q.mu.Unlock()
			return q, breakerOpen, fmt.Errorf("rans: outbox: node %d: %w: %w",
				it.target, errSendFailed, errOutboxBreaker)
		}
	}
	control := isControlFrame(it.hdr)
	limit := int64(o.cap)
	if control {
		limit += outboxControlReserve
	}
	if !o.claimInBox(limit) {
		if probe {
			// 探测帧未入箱：回滚半开标记——残留 true 让「open 且非
			// probing」恒不成立，新帧绕过熔断直达死目标。
			q.probing = false
		}
		q.mu.Unlock()
		return q, globalFull, nil
	}
	if !control && len(q.queue) >= o.perTarget {
		o.inBox.Add(-1)
		if probe {
			q.probing = false // 同上：目标上限拒收同样不放行残留
		}
		q.mu.Unlock()
		return q, targetFull, nil
	}
	q.queue = append(q.queue, it)
	startWorker := !q.worker
	if startWorker {
		q.worker = true
		// wg.Add 在 q.mu 内与 Close 的清扫/Wait 定序：本队列的 worker 计
		// 数先于（或被回绝于）Close 对本队列的临界区——杜绝 Add 与 Wait
		// 的并发误用窗口。
		o.wg.Add(1)
	}
	q.mu.Unlock()
	if startWorker {
		go o.run(q)
	} else {
		q.cond.Signal()
	}
	return q, enqueued, nil
}

// claimInBox 全箱占位（limit 段位内的 CAS 自旋）。
func (o *Outbox) claimInBox(limit int64) bool {
	for {
		cur := o.inBox.Load()
		if cur >= limit {
			return false
		}
		if o.inBox.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// putOutcome 满箱等待段的调用方收场：预算到期 → Timeout（消息确定未入
// 箱），主动取消 → Canceled。
func (o *Outbox) putOutcome(target rans.NodeID, ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("rans: outbox: tell to node %d blocked at capacity until caller budget: %w",
			target, rans.ErrTimeout)
	}
	return fmt.Errorf("%w: %w", rans.ErrCanceled, ctx.Err())
}

// Depth 返回在箱条目数（全箱共享计数；停序③ flush 的等待面）。
func (o *Outbox) Depth() int { return int(o.inBox.Load()) }

// run 目标 worker 主循环：串行消费队头（同目标 FIFO 的承载点）——出箱
// 即释放两级占位并广播空间，再同步走发送面；熔断中的遗留帧不空发（出
// 箱即按熔断收场）。空队退出（worker 清零，下一帧入队时按需重启）。
func (o *Outbox) run(q *targetQueue) {
	defer o.wg.Done()
	for {
		q.mu.Lock()
		for len(q.queue) == 0 && !q.stopped {
			q.cond.Wait()
		}
		if len(q.queue) == 0 {
			q.worker = false
			q.mu.Unlock()
			return
		}
		it := q.queue[0]
		q.queue[0] = outboxItem{}
		q.queue = q.queue[1:]
		blocked := q.open && !q.probing
		q.mu.Unlock()

		o.inBox.Add(-1)
		o.signalSpace()
		o.signalTargetSpace(q)
		if blocked {
			o.failFrame(it, errOutboxBreaker)
			continue
		}
		// 写出时刻过期检查（票 05）：排队过预算的帧不送出——调用帧回投
		//「确定未送出」、单向帧丢弃计 rans.call.expired（与 pawnite 出队
		// 过期同一指标面）。
		if !it.dl.IsZero() && !time.Now().Before(it.dl) {
			if it.fail != nil {
				it.fail(fmt.Errorf("%w: node %d expired in outbox queue",
					errSendFailed, it.target))
			} else {
				o.expired.Add(1)
			}
			continue
		}
		if it.fail != nil {
			o.sendCall(q, it)
		} else {
			o.sendOne(q, it)
		}
	}
}
