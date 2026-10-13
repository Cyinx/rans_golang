package rpc

// 节点发件箱（H11 决策 11）：单向发送入箱即返回，worker 按目标节点哈希
// 分片异步负责路由与发送。分片内单 worker 串行承同目标 FIFO，分片间并
// 行（同分片异目标互相排队是哈希分片的固有代价）。容量为全箱共享计数
// （跨分片合计），满箱语义按生产者分野（决策 12）：显式 Tell 阻塞至调
// 用方 ctx 到期，非阻塞生产者（事件扇出/回包/转发——票 09）满即丢计数。
//
// 请求-响应调用帧同箱入队（PutCall）：与单向帧同分片同 FIFO——同目标
// 「先 Tell 后 Call」的跨节点写出次序由单 worker 串行承载。调用帧携带
// 调用方 ctx 与失败回投面，worker 以 SendCall 发送：预算随帧入箱，排队
// 过期的帧在发送段自我中止（确定未送出，不产生迟到执行）。
//
// 发送面由组装根注入（Remote 接网关 SendOneWay——路由与发送韧性在端点
// 内部），worker 逐帧调用，失败即弃置计数（at-most-once：重试窗口在端
// 点内，分片头阻塞由箱容量吸收）。body 所有权随入箱移交发件箱、随发送
// 成功移交端点或随弃置释放引用——与 SendOneWay 同口径，不得复用已归还
// 缓冲。
//
// 生命周期：构造即启动全部分片 worker（同网关扫描器的构造期常驻面），
// Close 幂等收口——在途发送经内部 ctx 取消、在箱帧弃置计数（dropped）。
// 优雅停机须先经停序③ flush 等箱空再 Close（Remote.Shutdown 装配）。

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/wire"
)

// 发件箱缺省参数（H11 决策 11；node.Config.Outbox 零值回落同值——
// node.DefaultOutboxCapacity/DefaultOutboxShards）与 worker 发送预算。
const (
	// defaultOutboxCapacity 全箱共享容量缺省（条）。
	defaultOutboxCapacity = 65536
	// defaultOutboxShards 按目标节点哈希的分片数缺省。
	defaultOutboxShards = 16
	// outboxSendBudget 单帧发送的防御预算（异步发送无调用方预算可继承
	// ——满箱等待消耗的是入箱调用方的预算，出箱发送的预算归发件箱自
	// 有）：分片的单帧头阻塞上界，覆盖端点韧性面的整轮重试窗口。
	outboxSendBudget = 30 * time.Second
)

// errOutboxClosed 关箱后的入箱拒绝（停机窗口竞态）：plain 错误——分类
// 唯一表 NoRetry（停机终态不重试），与满箱预算收场（Timeout/Canceled）
// 分列。
var errOutboxClosed = errors.New("rans: outbox: closed (node shutting down)")

// errOutboxFull 非阻塞入箱（TryPut）的满箱丢弃哨兵：plain 错误——满即
// 丢是决策 12 的固定策略而非故障，消费方（事件扇出/回包/转发）据此留痕
// 继续即可，计数落 rans.outbox.dropped。
var errOutboxFull = errors.New("rans: outbox: full (non-blocking producer drop)")

// OutboxConfig 发件箱构造参数（内部装配面——组装收敛于 Remote.New）。
type OutboxConfig struct {
	// Capacity 全箱共享容量（在箱条目上限，跨分片合计）；≤0 →
	// defaultOutboxCapacity。
	Capacity int
	// Shards 按目标节点哈希的分片数（每分片单 worker 串行）；≤0 →
	// defaultOutboxShards。
	Shards int
	// Send 发送面（必填）：一帧一次端点发送（路由+发送的韧性在实现内
	// 部）；Remote 装配网关的 SendOneWay，测试注入替身。
	Send func(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte) error
	// SendCall 调用帧发送面（PutCall 的 worker 发送腿）：与 Send 同一端点
	// 韧性，携带请求 deadline——过期即中止（确定未送出），失败经帧内
	// fail 回投等待方。PutCall 使用时必填（nil → 入箱拒绝）。
	SendCall func(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte, deadline time.Time) error
	// Metrics 指标注册表；nil → metrics.Default()。深度 gauge 与丢弃/
	// 发送失败计数器在此注册（构造点集中注入）。
	Metrics *metrics.Registry
}

// outboxItem 在箱帧：body 所有权已移交发件箱（见包注释契约）。ctx/fail
// 仅调用帧（PutCall）非零：ctx 携调用方取消与预算（worker 发送腿的上
// 界），fail 回投发送失败（响应不走此面，仍归网关 pending）。
type outboxItem struct {
	target rans.NodeID
	hdr    wire.Header
	body   []byte
	ctx    context.Context
	fail   func(error)
}

// outboxShard 分片：单 worker 串行消费的 FIFO 队列（mutex + cond；
// 切片尾部入头部出、按峰值增长——无预分配大缓冲）。
type outboxShard struct {
	mu    sync.Mutex
	cond  *sync.Cond // worker 在空队列上等待；入箱 Signal、关箱 Broadcast
	queue []outboxItem
	// stopped 关箱标志（与 sweep 同临界区置位——此后入箱必被拒，杜绝
	// worker 退出后的孤儿帧）。
	stopped bool
}

// Outbox 节点级有界发件箱。Put 入箱即返回（满箱阻塞段见方法注释）；
// TryPut 非阻塞生产者面（满即丢计数）；Depth 为排空等待面；Close 幂等
// 收口。并发安全。
type Outbox struct {
	cfg    OutboxConfig
	cap    int64
	shards []*outboxShard

	// inBox 全箱共享计数（容量唯一闸门——跨分片合计）：入箱 CAS +1、
	// 出箱/弃置 -1；深度 gauge 与停序 flush 读同一原子。
	inBox atomic.Int64

	// 空间广播面（满箱等待者的唤醒通道）：任一帧出箱即关旧建新（广播
	// 唤醒全部等待者，胜者 CAS 占位、余者在新通道上重等）。
	spaceMu sync.Mutex
	spaceCh chan struct{}

	sendFailed *metrics.Counter
	dropped    *metrics.Counter

	runCtx    context.Context
	runCancel context.CancelFunc
	stopCh    chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

// NewOutbox 构造发件箱并启动全部分片 worker（构造期常驻面，Close 收口
// ——同网关扫描器形态）。Send 必填（nil 拒绝构造——无发送面的发件箱是
// 配置事故）。
func NewOutbox(cfg OutboxConfig) (*Outbox, error) {
	if cfg.Send == nil {
		return nil, errors.New("rans: outbox: Send is required")
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = defaultOutboxCapacity
	}
	if cfg.Shards <= 0 {
		cfg.Shards = defaultOutboxShards
	}
	reg := cfg.Metrics
	if reg == nil {
		reg = metrics.Default()
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	o := &Outbox{
		cfg:       cfg,
		cap:       int64(cfg.Capacity),
		spaceCh:   make(chan struct{}),
		runCtx:    runCtx,
		runCancel: runCancel,
		stopCh:    make(chan struct{}),
	}
	o.sendFailed = reg.Counter(metrics.NameOutboxSendFailed,
		"outbox worker send attempts failed (retry window exhausted or close-canceled)")
	o.dropped = reg.Counter(metrics.NameOutboxDropped,
		"frames dropped after enqueue without sending (close abandonment; capacity-policy drops land here too)")
	reg.Gauge(metrics.NameOutboxDepth, "outbox entries waiting to be sent (all shards)",
		func() float64 { return float64(o.inBox.Load()) })
	for i := 0; i < cfg.Shards; i++ {
		s := &outboxShard{}
		s.cond = sync.NewCond(&s.mu)
		o.shards = append(o.shards, s)
		o.wg.Add(1)
		go o.run(s)
	}
	return o, nil
}

// Put 入箱一帧：箱有位即返回（入箱即返回语义）；满箱阻塞至腾位、关箱
// 或 ctx 结束（显式 Tell 的满箱段——阻塞至调用方预算到期，错误形态与调
// 用面统一映射一致：预算到期 → Timeout、主动取消 → Canceled）。body 所
// 有权随本调用移交（见包注释）。
func (o *Outbox) Put(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte) error {
	return o.putBlocking(ctx, outboxItem{target: target, hdr: hdr, body: body})
}

// putBlocking 阻塞生产者（Put/PutCall）的满箱等待共核：占位失败（满箱）
// 即等空间/关箱/ctx 三路收口后重试。
func (o *Outbox) putBlocking(ctx context.Context, it outboxItem) error {
	select {
	case <-o.stopCh:
		return errOutboxClosed
	default:
	}
	for {
		full, err := o.enqueueItem(it)
		if err != nil || !full {
			return err
		}
		// 满箱：等空间/关箱/预算（广播通道 + 关箱闸 + ctx 三路收口）。
		o.spaceMu.Lock()
		ch := o.spaceCh
		o.spaceMu.Unlock()
		select {
		case <-ch:
		case <-o.stopCh:
			return errOutboxClosed
		case <-ctx.Done():
			return o.putOutcome(it.target, ctx)
		}
	}
}

// PutCall 入箱一帧请求-响应调用帧：与 Put 同一满箱阻塞语义，差异仅在
// item 携调用方 ctx 与失败回投面（worker 发送腿见 sendCall）。调用帧与
// 单向帧同分片同 FIFO——同目标「先 Tell 后 Call」的写出次序由此承载；
// 排队期间预算照常流逝（过期帧由发送段中止，见包注释）。
func (o *Outbox) PutCall(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte, fail func(error)) error {
	if o.cfg.SendCall == nil {
		return errors.New("rans: outbox: SendCall is required for call frames")
	}
	if fail == nil {
		return errors.New("rans: outbox: PutCall requires a fail callback")
	}
	return o.putBlocking(ctx, outboxItem{target: target, hdr: hdr, body: body, ctx: ctx, fail: fail})
}

// TryPut 非阻塞入箱一帧（H11 决策 12 的非阻塞生产者面——事件扇出/错误
// 回包/仲裁输家转发/正常应答帧）：箱有位即入（与 Put 同一占位与分片队
// 序，同目标 FIFO 语义不变）；满箱满即丢——计 rans.outbox.dropped 后返
// 回 errOutboxFull，绝不等待；关箱拒绝（errOutboxClosed，与 Put 的停机
// 窗口语义同型）。满箱丢弃时 body 所有权不移交（调用方继续持有并自行
// 收场——池页归池、临时缓冲归 GC）。CAS 竞争失败内部自旋重试（无等待
// 段，非阻塞面不因竞争升级为阻塞）。
func (o *Outbox) TryPut(target rans.NodeID, hdr wire.Header, body []byte) error {
	select {
	case <-o.stopCh:
		return errOutboxClosed
	default:
	}
	for {
		full, err := o.enqueue(target, hdr, body)
		if err != nil {
			return err
		}
		if !full {
			return nil
		}
		o.dropped.Add(1)
		return errOutboxFull
	}
}

// enqueue 占位入队（TryPut 的临界区单点，构造最小 item）：语义见
// enqueueItem。
func (o *Outbox) enqueue(target rans.NodeID, hdr wire.Header, body []byte) (bool, error) {
	return o.enqueueItem(outboxItem{target: target, hdr: hdr, body: body})
}

// enqueueItem 占位入队共核（全部生产者的临界区单点）：占位成功并入队完
// 成返回 (false, nil)；满箱返回 (true, nil)（占位未发生，等待/丢弃语义
// 归调用方分野）；关箱（停机闸窗口竞态）回退占位返回 errOutboxClosed。
func (o *Outbox) enqueueItem(it outboxItem) (bool, error) {
	for {
		cur := o.inBox.Load()
		if cur >= o.cap {
			return true, nil
		}
		if !o.inBox.CompareAndSwap(cur, cur+1) {
			continue // 竞争失败重占位
		}
		s := o.shards[o.shardOf(it.target)]
		s.mu.Lock()
		if s.stopped { // 关箱竞态：与 sweep 同临界区判定，回退占位
			s.mu.Unlock()
			o.inBox.Add(-1)
			return false, errOutboxClosed
		}
		s.queue = append(s.queue, it)
		empty := len(s.queue) == 1
		s.mu.Unlock()
		if empty {
			s.cond.Signal() // 单 worker：空队→忙等待切换只需 Signal
		}
		return false, nil
	}
}

// putOutcome 满箱等待段的调用方收场（与网关 ctxOutcome 同映射）：预算
// 到期 → Timeout（消息确定未入箱），主动取消 → Canceled。
func (o *Outbox) putOutcome(target rans.NodeID, ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("rans: outbox: tell to node %d blocked at capacity until caller budget: %w",
			target, rans.ErrTimeout)
	}
	return fmt.Errorf("%w: %w", rans.ErrCanceled, ctx.Err())
}

// Depth 返回在箱条目数（全箱共享计数；停序③ flush 的等待面——归零即
// 出站单向队列排空；在途发送不计数，其收场由端点韧性面与关箱取消承担）。
func (o *Outbox) Depth() int { return int(o.inBox.Load()) }

// shardOf 目标节点 → 分片（FNV-1a 32 过 2 字节节点号，确定性散布）。
func (o *Outbox) shardOf(n rans.NodeID) int {
	h := uint32(2166136261)
	h ^= uint32(n) & 0xff
	h *= 16777619
	h ^= (uint32(n) >> 8) & 0xff
	h *= 16777619
	return int(h % uint32(len(o.shards)))
}

// run 分片 worker 主循环：单 worker 串行消费队头（同目标 FIFO 的承载
// 点）——出箱即释放全箱占位并广播空间，再同步走发送面（韧性在端点内
// 部）。关箱即退（队列已由 Close 清扫，无余帧可弃）。
func (o *Outbox) run(s *outboxShard) {
	defer o.wg.Done()
	for {
		s.mu.Lock()
		for len(s.queue) == 0 && !s.stopped {
			s.cond.Wait()
		}
		if len(s.queue) == 0 {
			s.mu.Unlock()
			return
		}
		it := s.queue[0]
		s.queue[0] = outboxItem{}
		s.queue = s.queue[1:]
		s.mu.Unlock()

		o.inBox.Add(-1)
		o.signalSpace()
		if it.fail != nil {
			o.sendCall(it)
		} else {
			o.sendOne(it)
		}
	}
}

// sendOne 单向帧发送：预算 ctx（关箱取消贯穿）调注入面；失败计数留痕后
// 弃置（at-most-once——重试窗口在端点韧性面内）。
func (o *Outbox) sendOne(it outboxItem) {
	ctx, cancel := context.WithTimeout(o.runCtx, outboxSendBudget)
	defer cancel()
	if err := o.cfg.Send(ctx, it.target, it.hdr, it.body); err != nil {
		o.sendFailed.Add(1)
		log.Warn("rans/rpc: outbox send failed",
			"node", it.target, "type", uint8(it.hdr.Type), "err", err)
	}
}

// sendCall 调用帧发送腿：ctx 双亲封顶（帧内调用方 ctx 承载取消与请求
// 预算，runCtx+防御预算封顶关窗等待上界），deadline 取派生后的 ctx（即
// 两者的较早者）。排队过期的帧在此中止——确定未送出；失败计数留痕并
// 回投等待方（回投错误即确定未投递形态，等待方据此分类重试）。
func (o *Outbox) sendCall(it outboxItem) {
	ctx, cancel := context.WithTimeout(it.ctx, outboxSendBudget)
	defer cancel()
	deadline, _ := ctx.Deadline()
	if err := o.cfg.SendCall(ctx, it.target, it.hdr, it.body, deadline); err != nil {
		o.sendFailed.Add(1)
		log.Warn("rans/rpc: outbox call-frame send failed",
			"node", it.target, "type", uint8(it.hdr.Type), "err", err)
		it.fail(err)
	}
}

// signalSpace 空间广播：关旧通道唤醒全部满箱等待者、建新通道承接下一
// 轮等待（胜者 CAS 占位、余者重等——容量闸门在 CAS，广播只负责唤醒）。
func (o *Outbox) signalSpace() {
	o.spaceMu.Lock()
	close(o.spaceCh)
	o.spaceCh = make(chan struct{})
	o.spaceMu.Unlock()
}

// Close 幂等收口：落关箱闸、逐分片清扫在箱帧（计数丢弃——与置位同
// 临界区，杜绝 worker 退出后的孤儿帧）、取消在途发送、等 worker 全退。
// 优雅停机先经停序③ flush 等箱空（Remote.Shutdown 装配）再调用。
func (o *Outbox) Close() {
	o.stopOnce.Do(func() {
		close(o.stopCh)
		dropped := 0
		for _, s := range o.shards {
			s.mu.Lock()
			s.stopped = true
			dropped += len(s.queue)
			s.queue = nil
			s.mu.Unlock()
			s.cond.Broadcast()
		}
		if dropped > 0 {
			o.inBox.Add(-int64(dropped)) // 深度随弃置回落（计数与队列同源）
		}
		o.runCancel()
		o.wg.Wait()
		if dropped > 0 {
			o.dropped.Add(float64(dropped))
			log.Warn("rans/rpc: outbox closed with pending items", "dropped", dropped)
		}
	})
}
