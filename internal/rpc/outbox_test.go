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
func newTestOutbox(t *testing.T, capacity, shards int, send func(context.Context, rans.NodeID, wire.Header, []byte) error) (*Outbox, *metrics.Registry) {
	t.Helper()
	reg := metrics.New("outbox-test")
	o, err := NewOutbox(OutboxConfig{Capacity: capacity, Shards: shards, Send: send, Metrics: reg})
	if err != nil {
		t.Fatalf("new outbox: %v", err)
	}
	t.Cleanup(o.Close)
	return o, reg
}

// distinctShardTargets 返回 k 个落在 k 个不同分片的目标节点号（慢目标只
// 拖同分片——分片间并行断言的前提）。
func distinctShardTargets(t *testing.T, o *Outbox, k int) []rans.NodeID {
	t.Helper()
	seen := map[int]bool{}
	var out []rans.NodeID
	for id := rans.NodeID(1); id < 4096 && len(out) < k; id++ {
		s := o.shardOf(id)
		if !seen[s] {
			seen[s] = true
			out = append(out, id)
		}
	}
	if len(out) < k {
		t.Fatalf("cannot find %d distinct shards", k)
	}
	return out
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

// 1. 分片 FIFO：同目标并发乱序入箱保序送达（送达序 == 分片队列序）；
// 不同目标（异分片）并行互不阻塞——慢目标卡死首帧时快目标照常清空。
func TestOutbox_PerTargetFIFO(t *testing.T) {
	fake := newOutboxFakeSend()
	o, _ := newTestOutbox(t, 1024, 8, fake.send)
	ts := distinctShardTargets(t, o, 3)
	tStuck, tSame, tFast := ts[0], ts[0], ts[2] // tSame 与 tStuck 同分片同目标

	// 同目标 FIFO——阶段 A（确定性序）：worker 停在首帧发送段，随后并发
	// 入箱 20 帧；队列序在入箱完成后冻结，可整队快照（包内测试缝）。
	putTell(t, o, context.Background(), tStuck, 1000)
	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(seq uint64) {
			defer wg.Done()
			putTell(t, o, context.Background(), tSame, seq)
		}(uint64(1 + i))
	}
	wg.Wait()
	s := o.shards[o.shardOf(tStuck)]
	s.mu.Lock()
	var queued []uint64
	for _, it := range s.queue {
		queued = append(queued, it.hdr.MessageID)
	}
	s.mu.Unlock()
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

	// 阶段 B（分片间并行）：tStuck 重新卡死首帧，tFast（异分片）快速放行
	// ——快目标的 n 帧全部送达时慢目标仍停在首帧。单 worker 突变下快目
	// 标被队头阻塞，本断言变红。
	fake.block(tStuck)
	fake.release(tFast)
	putTell(t, o, context.Background(), tStuck, 2000)
	for i := 0; i < n; i++ {
		putTell(t, o, context.Background(), tFast, uint64(3000+i))
	}
	fake.waitArrivals(t, tFast, n)
	if c := len(fake.arrivalSeqs(tStuck)) - (n + 1); c != 0 {
		t.Fatalf("slow target advanced %d frames while gated, want 0 (cross-shard parallelism broken)", c)
	}
}

// 2. 满箱 Tell 阻塞语义（H11 决策 12 显式 Tell 腿）：箱不满入箱即返回；
// 满时阻塞至调用方 ctx 到期返回错误（预算到期 → Timeout，主动取消 →
// Canceled），绝不永久阻塞；放行排空后恢复快速返回。
func TestOutbox_TellBlocksUntilCtx(t *testing.T) {
	fake := newOutboxFakeSend()
	o, reg := newTestOutbox(t, 2, 1, fake.send)
	tgt := rans.NodeID(1)

	// 箱不满：入箱即返回。首帧入箱后等 worker 取走进入发送段（深度归零
	// ——出箱是异步面），再置满箱，消除出箱竞态。
	fastCtx, fastCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer fastCancel()
	putTell(t, o, fastCtx, tgt, 1)
	waitReading(t, reg, metrics.NameOutboxDepth, 0)
	putTell(t, o, fastCtx, tgt, 2)
	putTell(t, o, fastCtx, tgt, 3)
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
	putTell(t, o, fastCtx, tgt, 6)
}

// mustShortCtx 带短预算的测试 ctx。
func mustShortCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// 3. 指标计数（H11 决策 11）：深度随在箱积压增长；发送失败按帧计数；
// 关箱弃置在箱帧计丢弃。全部经构造注入的注册表读数断言。
func TestOutbox_Metrics(t *testing.T) {
	fake := newOutboxFakeSend()
	o, reg := newTestOutbox(t, 64, 4, fake.send)
	ts := distinctShardTargets(t, o, 2)
	tStuck, tFail := ts[0], ts[1]
	fake.setFail(tFail, true)

	// 深度：worker 卡在 tStuck 首帧（出箱），随后 3 帧留箱。
	putTell(t, o, context.Background(), tStuck, 1)
	for i := 0; i < 3; i++ {
		putTell(t, o, context.Background(), tStuck, uint64(2+i))
	}
	if v := waitReading(t, reg, metrics.NameOutboxDepth, 3); v != 3 {
		t.Fatalf("outbox depth = %v, want 3", v)
	}

	// 发送失败：tFail 的 3 帧门开即败，逐帧计数。
	fake.release(tFail)
	for i := 0; i < 3; i++ {
		putTell(t, o, context.Background(), tFail, uint64(10+i))
	}
	if v := waitReading(t, reg, metrics.NameOutboxSendFailed, 3); v != 3 {
		t.Fatalf("outbox send failures = %v, want 3", v)
	}

	// 丢弃：关箱弃置在箱 3 帧（静默丢失面——计数留痕）；卡在发送段的
	// 首帧随关箱取消，计入发送失败。
	o.Close()
	if v := waitReading(t, reg, metrics.NameOutboxDropped, 3); v != 3 {
		t.Fatalf("outbox drops = %v, want 3", v)
	}
	if v := waitReading(t, reg, metrics.NameOutboxSendFailed, 4); v != 4 {
		t.Fatalf("outbox send failures after close = %v, want 4 (in-flight frame canceled)", v)
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
	o, reg := newTestOutbox(t, 2, 1, fake.send)
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

// 调用帧排队过预算的发送段中止（健全性锁）：单分片队头被门挂住，调用帧
// 在箱内排队至调用方预算耗尽；放行队头后 worker 取到的是已过期帧——
// SendCall 必须收到已到期的 ctx 与 deadline（真实面据此确定不送出，不产
// 生迟到执行），发送失败经 fail 回投。fake 的写读经 fail 通道定序，无竞态。
func TestOutbox_CallFrameExpiredBudgetAbortsAtSend(t *testing.T) {
	fake := newOutboxFakeSend()
	var gotCtxErr error
	var gotDeadline time.Time
	failErrs := make(chan error, 1)
	o, err := NewOutbox(OutboxConfig{
		Capacity: 8,
		Shards:   1,
		Send:     fake.send,
		SendCall: func(ctx context.Context, _ rans.NodeID, _ wire.Header, _ []byte, deadline time.Time) error {
			gotCtxErr = ctx.Err()
			gotDeadline = deadline
			return errors.New("outbox fake: expired frame")
		},
		Metrics: metrics.New("outbox-call-test"),
	})
	if err != nil {
		t.Fatalf("new outbox: %v", err)
	}
	t.Cleanup(o.Close)

	if err := o.Put(context.Background(), rans.NodeID(2), wire.Header{MessageID: 1}, nil); err != nil {
		t.Fatalf("head tell: %v", err)
	}
	cctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := o.PutCall(cctx, rans.NodeID(3), wire.Header{MessageID: 2}, nil, func(err error) { failErrs <- err }); err != nil {
		t.Fatalf("put call frame: %v", err)
	}
	<-cctx.Done() // 调用帧预算在箱内耗尽（队头仍被门挂住，帧未出箱）

	fake.release(rans.NodeID(2))
	select {
	case ferr := <-failErrs:
		if ferr == nil {
			t.Fatal("fail must receive the send error of the expired frame")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call-frame send never completed after head release")
	}
	if gotCtxErr == nil {
		t.Fatal("SendCall must be invoked with an expired ctx (budget burned in queue)")
	}
	if gotDeadline.IsZero() || !time.Now().After(gotDeadline) {
		t.Fatalf("SendCall deadline = %v, want an already-passed request deadline", gotDeadline)
	}
}
