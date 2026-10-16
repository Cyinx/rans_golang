package rpc

// 票 04 行为锚：跨目标不阻塞（死目标专属 worker 不拖别人）、死目标熔断
// （首败开断清扫+新入箱快速失败+复活闭合）、控制面帧预留容量。

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// TestOutbox_DeadTargetDoesNotBlockOthers 死目标（发送腿挂死）不得拖慢其
// 他目标——1 与 17 在旧哈希分片模型下同落缺省分片（同分片队头阻塞的复现
// 形态）；按目标分队列下节点 17 的调用帧即刻发送成功。
func TestOutbox_DeadTargetDoesNotBlockOthers(t *testing.T) {
	stuck := rans.NodeID(1)
	release := make(chan struct{})
	entered := make(chan rans.NodeID, 8)
	send := func(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte) error {
		entered <- target
		if target == stuck {
			<-release
		}
		return nil
	}
	sendCall := func(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte, _ time.Time) error {
		entered <- target
		if target == stuck {
			<-release
		}
		return nil
	}
	o, err := NewOutbox(OutboxConfig{Capacity: 64, Send: send, SendCall: sendCall,
		Metrics: metrics.New("outbox-hol-test")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Close)

	defer close(release) // 失败路径也放行卡死的发送腿（t.Cleanup 的 Close 才能收口）
	if err := o.Put(context.Background(), stuck, wire.Header{MessageID: 1}, nil); err != nil {
		t.Fatalf("tell to stuck: %v", err)
	}
	select {
	case got := <-entered:
		if got != stuck {
			t.Fatalf("first send entered for node %d, want %d", got, stuck)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stuck send never started")
	}
	// 调用帧的发送成功不回投 fail（应答走网关 pending 面）——「未被打
	// 阻塞」的判据是 SendCall 被即刻调用（专属 worker 直达发送段）。
	if err := o.PutCall(context.Background(), rans.NodeID(17), wire.Header{MessageID: 2}, nil,
		func(error) {}); err != nil {
		t.Fatalf("put call: %v", err)
	}
	select {
	case got := <-entered:
		if got != rans.NodeID(17) {
			t.Fatalf("next send entered for node %d, want 17", got)
		}
	case <-time.After(time.Second):
		t.Fatal("node 17's call blocked behind node 1's stuck send (cross-target isolation broken)")
	}
}

// TestOutbox_BreakerFailsQueuedCallsFast 死目标熔断：首帧发送失败即开断
// ——同目标在箱帧立即收场（调用帧回投 Unavailable+SendFailed、单向帧丢
// 弃计数，不再逐帧发送）；熔断期间新入箱快速失败；NotifyNodeAlive 即时
// 闭合恢复入箱。
func TestOutbox_BreakerFailsQueuedCallsFast(t *testing.T) {
	dead := rans.NodeID(5)
	release := make(chan struct{})
	var openOnce sync.Once
	open := func() { openOnce.Do(func() { close(release) }) }
	defer open() // 失败路径放行（同上）
	var released atomic.Bool
	var sends atomic.Int32
	send := func(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte) error {
		sends.Add(1)
		if !released.Load() {
			<-release
			return errors.New("node down")
		}
		return nil
	}
	sendCall := func(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte, _ time.Time) error {
		return send(ctx, target, hdr, body)
	}
	reg := metrics.New("outbox-breaker-test")
	o, err := NewOutbox(OutboxConfig{Capacity: 64, Send: send, SendCall: sendCall, Metrics: reg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Close)

	fails := make(chan error, 3)
	for i := 0; i < 3; i++ {
		if err := o.PutCall(context.Background(), dead, wire.Header{MessageID: uint64(i)}, nil,
			func(err error) { fails <- err }); err != nil {
			t.Fatalf("put call %d: %v", i, err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := o.Put(context.Background(), dead, wire.Header{MessageID: uint64(10 + i)}, nil); err != nil {
			t.Fatalf("put tell %d: %v", i, err)
		}
	}
	// 等首帧进入发送段（其余 4 帧在箱排队）。
	deadline := time.Now().Add(2 * time.Second)
	for sends.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	open() // 首帧失败 → 开断清扫
	// 三帧全部收场：开断阈值 2——前两帧各自真实发送失败（回投原始错
	// 误），第二帧失败触发开断，第三帧被清扫回投（Unavailable+SendFailed
	// 确定未送出形态）。
	raw, classified := 0, 0
	for i := 0; i < 3; i++ {
		select {
		case err := <-fails:
			if err == nil {
				t.Fatalf("call %d settled with nil error", i)
			}
			if rans.CodeOf(err) == rans.CodeUnavailable && rans.ReasonOf(err) == rans.ReasonSendFailed {
				classified++
			} else {
				raw++
			}
		case <-time.After(2 * time.Second):
			t.Fatal("queued call frames must settle promptly on breaker open")
		}
	}
	if raw != 2 || classified != 1 {
		t.Fatalf("settles = %d raw + %d classified, want 2 sent-and-failed + 1 swept", raw, classified)
	}
	if n := sends.Load(); n != 2 {
		t.Fatalf("send attempts = %d, want 2 (threshold), swept frame must not be sent", n)
	}
	// 两条单向帧被清扫丢弃。
	if v := waitReading(t, reg, metrics.NameOutboxDropped, 2); v != 2 {
		t.Fatalf("breaker sweeps = %v, want 2", v)
	}
	if v := waitReading(t, reg, metrics.NameOutboxBreakerOpened, 1); v != 1 {
		t.Fatalf("breaker openings = %v, want 1", v)
	}
	// 熔断期间非阻塞生产者即刻拒绝（确定未投递）。
	if err := o.TryPut(dead, wire.Header{MessageID: 20}, nil); !errors.Is(err, errOutboxBreaker) {
		t.Fatalf("try-put to broken target = %v, want errOutboxBreaker", err)
	}
	// 阻塞生产者等冷却余量后自带探针——门仍关着时按自身预算收场（不无
	// 限等待）；门开后（复活）Put 即刻成功。
	pctx, pcancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer pcancel()
	if err := o.Put(pctx, dead, wire.Header{MessageID: 21}, nil); err == nil {
		t.Fatal("put with tiny budget during breaker-open+closed gate must not succeed")
	}
	released.Store(true)
	o.NotifyNodeAlive(dead)
	if err := o.Put(context.Background(), dead, wire.Header{MessageID: 22}, nil); err != nil {
		t.Fatalf("put after revive: %v", err)
	}
}

// review15 票 04 回归锚（两处熔断修复）：半开探针的容量拒收路径与探针失
// 败复开的计数路径此前均无覆盖。

// TestOutbox_BreakerProbeCapacityFailRollsBackHalfOpen 半开探针被容量拒收
// （全箱占满）后不得残留 probing 标记：残留让「open 且非 probing」恒不
// 成立，新帧绕过熔断快速失败直达死目标。probing 无外部读数（仅熔断状态
// 机内部消费），回归锚只能在包内断言状态回滚。
func TestOutbox_BreakerProbeCapacityFailRollsBackHalfOpen(t *testing.T) {
	dead := rans.NodeID(3)
	hog := rans.NodeID(4)
	gate := make(chan struct{})
	var deadSends atomic.Int32
	send := func(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte) error {
		if target == dead {
			deadSends.Add(1)
			return errors.New("node down")
		}
		<-gate // hog 首帧卡在发送段，后续帧滞留在箱
		return nil
	}
	reg := metrics.New("outbox-probe-rollback-test")
	o, err := NewOutbox(OutboxConfig{Capacity: 2, Send: send, Metrics: reg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { close(gate); o.Close() })

	// hog 占满全箱：首帧进发送段（卡死），两帧在箱 = 容量占满。
	for i := 0; i < 3; i++ {
		if err := o.Put(context.Background(), hog, wire.Header{}, nil); err != nil {
			t.Fatalf("hog put %d: %v", i, err)
		}
	}
	if v := waitReading(t, reg, metrics.NameOutboxDepth, 2); v != 2 {
		t.Fatalf("depth = %v, want 2 (box full)", v)
	}
	// dead 判停开断，冷却期满后试入探针——满箱拒收（确定未入箱）。
	o.NotifyTargetDown(dead)
	time.Sleep(outboxBreakerCooldown + 50*time.Millisecond)
	if err := o.TryPut(dead, wire.Header{MessageID: 100}, nil); !errors.Is(err, errOutboxFull) {
		t.Fatalf("probe try-put on full box = %v, want errOutboxFull", err)
	}
	if n := deadSends.Load(); n != 0 {
		t.Fatalf("dead sends = %d, want 0 (capacity-rejected probe must not reach send)", n)
	}
	q := o.targets[dead]
	q.mu.Lock()
	probing := q.probing
	q.mu.Unlock()
	if probing {
		t.Fatal("half-open flag left set after capacity-rejected probe")
	}
}

// TestOutbox_BreakerReopenNoDoubleCount 探针失败复开不得对已开断目标重复
// 计数：openCount 漂移让 NotifyNodeAlive 的零成本快路径永久失效，
// breakerOpened 不再反映真实开断次数。openCount 无外部读数（仅驱动上述
// 快路径），漂移只能在包内断言。
func TestOutbox_BreakerReopenNoDoubleCount(t *testing.T) {
	dead := rans.NodeID(6)
	var alive atomic.Bool
	var delivered atomic.Int32
	send := func(ctx context.Context, target rans.NodeID, hdr wire.Header, body []byte) error {
		if !alive.Load() {
			return errors.New("node down")
		}
		delivered.Add(1)
		return nil
	}
	reg := metrics.New("outbox-reopen-test")
	o, err := NewOutbox(OutboxConfig{Capacity: 64, Send: send, Metrics: reg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Close)

	o.NotifyTargetDown(dead) // 首次开断：计数基线 1
	if v := waitReading(t, reg, metrics.NameOutboxBreakerOpened, 1); v != 1 {
		t.Fatalf("breaker openings = %v, want 1", v)
	}
	time.Sleep(outboxBreakerCooldown + 50*time.Millisecond)
	// 探针帧失败（连续失败 1，不开断）；随后一帧在探针窗口照常入队，失败
	// 计满阈值 → 复开。熔断未曾闭合：计数不得再 +1。
	if err := o.Put(context.Background(), dead, wire.Header{MessageID: 1}, nil); err != nil {
		t.Fatalf("probe put: %v", err)
	}
	if err := o.Put(context.Background(), dead, wire.Header{MessageID: 2}, nil); err != nil {
		t.Fatalf("follow-up put: %v", err)
	}
	if v := waitReading(t, reg, metrics.NameOutboxSendFailed, 2); v != 2 {
		t.Fatalf("send failures = %v, want 2 (probe + follow-up)", v)
	}
	if v := reading(t, reg, metrics.NameOutboxBreakerOpened); v != 1 {
		t.Fatalf("breaker openings after reopen = %v, want 1 (circuit never closed)", v)
	}
	if n := o.openCount.Load(); n != 1 {
		t.Fatalf("open target count after reopen = %d, want 1 (no double count)", n)
	}
	// 复开后的新帧在冷却窗内快速失败（熔断语义完好）。
	if err := o.TryPut(dead, wire.Header{MessageID: 3}, nil); !errors.Is(err, errOutboxBreaker) {
		t.Fatalf("try-put after reopen = %v, want errOutboxBreaker", err)
	}
	// NotifyNodeAlive 快路径有效（openCount 归位）且复开目标可复活。
	o.NotifyNodeAlive(dead)
	if n := o.openCount.Load(); n != 0 {
		t.Fatalf("open target count after alive = %d, want 0", n)
	}
	alive.Store(true)
	if err := o.Put(context.Background(), dead, wire.Header{MessageID: 4}, nil); err != nil {
		t.Fatalf("put after revive: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for delivered.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if delivered.Load() == 0 {
		t.Fatal("frame after revive never delivered")
	}
}

// TestOutbox_ControlFramesReservedCapacity 数据帧占满全箱容量时，控制面帧
// （MsgOwnershipLost）仍可入箱（独立预留容量）；数据帧继续满即丢。
func TestOutbox_ControlFramesReservedCapacity(t *testing.T) {
	gate := make(chan struct{})
	send := func(context.Context, rans.NodeID, wire.Header, []byte) error { <-gate; return nil }
	reg := metrics.New("outbox-ctl-test")
	o, err := NewOutbox(OutboxConfig{Capacity: 4, Send: send, Metrics: reg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { close(gate); o.Close() })

	tgt := rans.NodeID(9)
	for i := 0; i < 5; i++ { // 首帧进发送段 + 4 帧在箱 = 容量占满
		if err := o.Put(context.Background(), tgt, wire.Header{MessageID: uint64(i)}, nil); err != nil {
			t.Fatalf("fill put %d: %v", i, err)
		}
	}
	if v := waitReading(t, reg, metrics.NameOutboxDepth, 4); v != 4 {
		t.Fatalf("depth = %v, want 4 (capacity full)", v)
	}
	if err := o.TryPut(tgt, wire.Header{MessageID: 100}, nil); !errors.Is(err, errOutboxFull) {
		t.Fatalf("data try-put on full box = %v, want errOutboxFull", err)
	}
	ctl := wire.Header{Type: protocol.MsgOwnershipLost, MessageID: 101}
	if err := o.TryPut(tgt, ctl, nil); err != nil {
		t.Fatalf("control frame must use reserved capacity, got %v", err)
	}
}
