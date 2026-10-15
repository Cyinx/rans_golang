package rpc

// 节点发件箱（H11 决策 11）的红测面：分片 FIFO（同目标保序 + 分片间并
// 行）、满箱 Tell 阻塞语义（预算到期收场）、指标计数（深度/丢弃/发送失
// 败）。替身发送面按目标门控——门未开即阻塞（模拟慢目标/慢拨号），门开
// 后记录到达序。时序断言一律走 channel/轮询，不用 sleep。
//
// 装配层（Tell 接线 / 停序③ flush）的行为锁见 outbox_remote_test.go。

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/wire"
)

// outboxFakeSend 发件箱替身发送面：按目标门控（gate 未开时阻塞至 ctx
// ——慢目标形态），门开后记录到达序（hdr.MessageID 序列）并按 failSet
// 决定成败。gate 通道惰性创建；block/release 可反复开关（多阶段测试）。
type outboxFakeSend struct {
	mu       sync.Mutex
	gates    map[rans.NodeID]chan struct{}
	arrivals map[rans.NodeID][]uint64
	fail     map[rans.NodeID]bool
}

func newOutboxFakeSend() *outboxFakeSend {
	return &outboxFakeSend{
		gates:    map[rans.NodeID]chan struct{}{},
		arrivals: map[rans.NodeID][]uint64{},
		fail:     map[rans.NodeID]bool{},
	}
}

// gate 取目标的门通道（惰性创建，默认关着——未显式 release 即阻塞）。
func (f *outboxFakeSend) gate(t rans.NodeID) chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch, ok := f.gates[t]
	if !ok {
		ch = make(chan struct{})
		f.gates[t] = ch
	}
	return ch
}

// release 打开目标门（在途与后续帧全部放行）。
func (f *outboxFakeSend) release(t rans.NodeID) { close(f.gate(t)) }

// block 重新关上目标门（多阶段测试的下一停点；新门通道，旧门已关）。
func (f *outboxFakeSend) block(t rans.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gates[t] = make(chan struct{})
}

// setFail 设置目标的发送成败（门开后生效）。
func (f *outboxFakeSend) setFail(t rans.NodeID, on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[t] = on
}

// send 实现发件箱发送面：门等待（尊重 ctx——关箱取消即返回）→ 记录到达
// 序 → 按 failSet 返回。
func (f *outboxFakeSend) send(ctx context.Context, t rans.NodeID, hdr wire.Header, _ []byte) error {
	select {
	case <-f.gate(t):
	case <-ctx.Done():
		return ctx.Err()
	}
	f.mu.Lock()
	f.arrivals[t] = append(f.arrivals[t], hdr.MessageID)
	fail := f.fail[t]
	f.mu.Unlock()
	if fail {
		return errors.New("outbox fake: send rejected")
	}
	return nil
}

// arrivals 返回目标到达序快照。
func (f *outboxFakeSend) arrivalSeqs(t rans.NodeID) []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.arrivals[t]...)
}

// waitArrivals 轮询等待目标到达帧数达 want（channel 语义替代 sleep）。
func (f *outboxFakeSend) waitArrivals(t *testing.T, target rans.NodeID, want int) []uint64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := f.arrivalSeqs(target)
		if len(got) >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("target %d arrivals %d, want >= %d", target, len(got), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// newTestOutbox 构造带独立注册表的发件箱（Close 挂 Cleanup）。
func newTestOutbox(t *testing.T, capacity int, send func(context.Context, rans.NodeID, wire.Header, []byte) error) (*Outbox, *metrics.Registry) {
	t.Helper()
	reg := metrics.New("outbox-test")
	o, err := NewOutbox(OutboxConfig{Capacity: capacity, Send: send, Metrics: reg})
	if err != nil {
		t.Fatalf("new outbox: %v", err)
	}
	t.Cleanup(o.Close)
	return o, reg
}

// putTell 入箱一帧（MessageID 携带序号作断言载荷），失败即 Fatal。
func putTell(t *testing.T, o *Outbox, ctx context.Context, target rans.NodeID, seq uint64) {
	t.Helper()
	if err := o.Put(ctx, target, wire.Header{MessageID: seq}, nil); err != nil {
		t.Fatalf("put seq %d to node %d: %v", seq, target, err)
	}
}

// reading 取注册表某仪表读数（缺名报错）。
func reading(t *testing.T, reg *metrics.Registry, name string) float64 {
	t.Helper()
	for _, r := range reg.Readings() {
		if r.Name == name {
			return r.Value
		}
	}
	t.Fatalf("metric %q missing from registry", name)
	return 0
}

// waitReading 轮询等待仪表读数达 want（异步计数面的确定性收口）。
func waitReading(t *testing.T, reg *metrics.Registry, name string, want float64) float64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if v := reading(t, reg, name); v == want {
			return v
		}
		if time.Now().After(deadline) {
			t.Fatalf("metric %q = %v, want %v", name, reading(t, reg, name), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// 1. 目标 FIFO：同目标并发乱序入箱保序送达（送达序 == 目标队列序）；
// 不同目标并行互不阻塞——慢目标卡死首帧时快目标照常清空（票 04 按目标
// 分队列；回归锚：退回全局单 worker 形态即红）。
func TestOutbox_PerTargetFIFO(t *testing.T) {
	fake := newOutboxFakeSend()
	o, _ := newTestOutbox(t, 1024, fake.send)
	tStuck, tFast := rans.NodeID(2), rans.NodeID(9)

	// 同目标 FIFO——阶段 A（确定性序）：worker 停在首帧发送段，随后并发
	// 入箱 20 帧；队列序在入箱完成后冻结，可整队快照（包内测试缝）。
	putTell(t, o, context.Background(), tStuck, 1000)
	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(seq uint64) {
			defer wg.Done()
			putTell(t, o, context.Background(), tStuck, seq)
		}(uint64(1 + i))
	}
	wg.Wait()
	q, ok := o.queueFor(tStuck)
	if !ok {
		t.Fatal("queueFor must not reject a live outbox")
	}
	q.mu.Lock()
	var queued []uint64
	for _, it := range q.queue {
		queued = append(queued, it.hdr.MessageID)
	}
	q.mu.Unlock()
	if len(queued) != n {
		t.Fatalf("queued %d frames after concurrent puts, want %d", len(queued), n)
	}
	fake.release(tStuck)
	want := append([]uint64{1000}, queued...)
	got := fake.waitArrivals(t, tStuck, n+1)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("same-target delivery broke FIFO: got %v want %v", got, want)
		}
	}

	// 阶段 B（跨目标并行）：tStuck 重新卡死首帧，tFast 快速放行——快目
	// 标的 n 帧全部送达时慢目标仍停在首帧。
	fake.block(tStuck)
	fake.release(tFast)
	putTell(t, o, context.Background(), tStuck, 2000)
	for i := 0; i < n; i++ {
		putTell(t, o, context.Background(), tFast, uint64(3000+i))
	}
	fake.waitArrivals(t, tFast, n)
	if c := len(fake.arrivalSeqs(tStuck)) - (n + 1); c != 0 {
		t.Fatalf("slow target advanced %d frames while gated, want 0 (cross-target parallelism broken)", c)
	}
}

// 2. 满箱 Tell 阻塞语义（H11 决策 12 显式 Tell 腿）：箱不满入箱即返回；
// 满时阻塞至调用方 ctx 到期返回错误（预算到期 → Timeout，主动取消 →
// Canceled），绝不永久阻塞；放行排空后恢复快速返回。
func TestOutbox_TellBlocksUntilCtx(t *testing.T) {
	fake := newOutboxFakeSend()
	o, reg := newTestOutbox(t, 2, fake.send)
	tgt := rans.NodeID(1)

	// 箱不满：入箱即返回。首帧入箱后等 worker 取走进入发送段（深度归零
	// ——出箱是异步面），再置满箱，消除出箱竞态。
	fastCtx, fastCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer fastCancel()
	putTell(t, o, fastCtx, tgt, 1)
	waitReading(t, reg, metrics.NameOutboxDepth, 0)
	// #2/#3 用长预算（票 05 写出时刻过期检查）：满箱段耗去的 200ms 不应
	// 使在箱帧放行后被丢弃——它们要在放行后真实送达（恢复快速返回断言）。
	queuedCtx, queuedCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer queuedCancel()
	putTell(t, o, queuedCtx, tgt, 2)
	putTell(t, o, queuedCtx, tgt, 3)
	waitReading(t, reg, metrics.NameOutboxDepth, 2) // 满箱：容量 2，#2/#3 在箱

	// 满箱 + 预算到期：阻塞至 ctx 到期返回 Timeout（阻塞时长尊重预算）。
	start := time.Now()
	err := o.Put(mustShortCtx(t, 150*time.Millisecond), tgt, wire.Header{MessageID: 4}, nil)
	elapsed := time.Since(start)
	if !errors.Is(err, rans.ErrTimeout) {
		t.Fatalf("full-box put at budget expiry = %v, want rans.ErrTimeout", err)
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("full-box put returned after %v, want blocked until budget", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("full-box put blocked %v, want bounded by ctx (not forever)", elapsed)
	}

	// 满箱 + 主动取消 → Canceled。
	cctx, ccancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		ccancel()
	}()
	err = o.Put(cctx, tgt, wire.Header{MessageID: 5}, nil)
	if !errors.Is(err, rans.ErrCanceled) {
		t.Fatalf("full-box put at caller cancel = %v, want rans.ErrCanceled", err)
	}

	// 放行排空后：箱不满恢复快速返回。
	fake.release(tgt)
	fake.waitArrivals(t, tgt, 3)
	putTell(t, o, queuedCtx, tgt, 6)
}

// mustShortCtx 带短预算的测试 ctx。
func mustShortCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// 3. 指标计数（H11 决策 11 + 票 04 熔断）：深度随在箱积压增长；发送失
// 败即开断（首帧失败计数 1，同目标在箱帧被清扫计丢弃）；关箱弃置在箱帧
// 计丢弃、在途帧取消计发送失败。全部经构造注入的注册表读数断言。
func TestOutbox_Metrics(t *testing.T) {
	fake := newOutboxFakeSend()
	o, reg := newTestOutbox(t, 64, fake.send)
	tStuck, tFail := rans.NodeID(2), rans.NodeID(9)
	fake.setFail(tFail, true)

	// 深度：worker 卡在 tStuck 首帧（出箱），随后 3 帧留箱。
	putTell(t, o, context.Background(), tStuck, 1)
	for i := 0; i < 3; i++ {
		putTell(t, o, context.Background(), tStuck, uint64(2+i))
	}
	if v := waitReading(t, reg, metrics.NameOutboxDepth, 3); v != 3 {
		t.Fatalf("outbox depth = %v, want 3", v)
	}

	// 发送失败即熔断（票 04，开断阈值 2）：tFail 的 3 帧入箱（门开着、
	// 逐帧失败）——前两帧真实发送各计一次失败，第二帧触发开断，第三帧
	// 被清扫计丢弃：send_failed=2、breaker_opened=1。
	fake.release(tFail)
	for i := 0; i < 3; i++ {
		putTell(t, o, context.Background(), tFail, uint64(10+i))
	}
	if v := waitReading(t, reg, metrics.NameOutboxSendFailed, 2); v != 2 {
		t.Fatalf("outbox send failures = %v, want 2 (threshold sends)", v)
	}
	if v := waitReading(t, reg, metrics.NameOutboxDropped, 1); v != 1 {
		t.Fatalf("outbox breaker sweeps = %v, want 1", v)
	}
	if v := waitReading(t, reg, metrics.NameOutboxBreakerOpened, 1); v != 1 {
		t.Fatalf("outbox breaker openings = %v, want 1", v)
	}

	// 关箱：弃置 tStuck 在箱 3 帧（静默丢失面——计数留痕）；卡在发送段
	// 的首帧随关箱取消，计入发送失败（并使 tStuck 也开断——断言见上式
	// 的姊妹读数，不再单独断言）。
	o.Close()
	if v := waitReading(t, reg, metrics.NameOutboxDropped, 4); v != 4 {
		t.Fatalf("outbox drops after close = %v, want 4", v)
	}
	if v := waitReading(t, reg, metrics.NameOutboxSendFailed, 3); v != 3 {
		t.Fatalf("outbox send failures after close = %v, want 3 (in-flight frame canceled)", v)
	}
	if v := reading(t, reg, metrics.NameOutboxDepth); v != 0 {
		t.Fatalf("outbox depth after close = %v, want 0", v)
	}
}

// 4. 非阻塞入箱面（H11 决策 12 非阻塞生产者腿）：箱有位即刻入（返回
// nil）；满箱满即丢——计数 rans.outbox.dropped 并返回 errOutboxFull 哨
// 兵，绝不阻塞；腾位后恢复即入。关箱拒绝与 Put 同型。
func TestOutbox_TryPutFullDropsAndCounts(t *testing.T) {
	fake := newOutboxFakeSend()
	o, reg := newTestOutbox(t, 2, fake.send)
	tgt := rans.NodeID(1)

	// 首帧入箱并等 worker 取走进入发送段（深度归零——出箱是异步面），
	// 消除出箱竞态后，#2/#3 占满容量 2。
	putTell(t, o, context.Background(), tgt, 1)
	waitReading(t, reg, metrics.NameOutboxDepth, 0)
	putTell(t, o, context.Background(), tgt, 2)
	putTell(t, o, context.Background(), tgt, 3)
	waitReading(t, reg, metrics.NameOutboxDepth, 2)

	// 满箱：满即丢返回哨兵，非阻塞（不等待 ctx——无 ctx 参数的语义锚），
	// 丢弃计数落 rans.outbox.dropped。
	if err := o.TryPut(tgt, wire.Header{MessageID: 4}, nil); !errors.Is(err, errOutboxFull) {
		t.Fatalf("try-put on full box = %v, want errOutboxFull", err)
	}
	if v := waitReading(t, reg, metrics.NameOutboxDropped, 1); v != 1 {
		t.Fatalf("outbox drops = %v, want 1", v)
	}

	// 腾位后恢复即入（同目标分片 FIFO 不受丢弃影响：先入者先出）。
	fake.release(tgt)
	fake.waitArrivals(t, tgt, 3)
	putTell(t, o, context.Background(), tgt, 5)
	fake.waitArrivals(t, tgt, 4)
	if got := fake.arrivalSeqs(tgt); len(got) != 4 || got[3] != 5 {
		t.Fatalf("arrivals = %v, want seq 5 delivered last after space freed", got)
	}
}

// 调用帧排队过预算（票 05 语义更新）：原断言「SendCall 收到已过期 ctx 由
// 发送面中止」——写出时刻过期检查落地后升级为「过期帧不进发送面、出箱
// 即回投确定未送出」，由 outbox_budget_test.go 的 TestOutbox_ExpiredFrame
// NotSent 与 TestOutbox_BudgetRestampedAtSend 接管钉住。
