package pawnite

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// testDelivery 记录投递/回绝，供断言。
type testDelivery struct {
	id    rans.ID
	mu    sync.Mutex
	deliv int
	rejst rans.Code
	rejrs rans.Reason
}

func (d *testDelivery) UUID() rans.ID { return d.id }
func (d *testDelivery) Deliver(p *Pawnite) {
	d.mu.Lock()
	d.deliv++
	d.mu.Unlock()
}
func (d *testDelivery) Reject(st rans.Code, reason rans.Reason) {
	d.mu.Lock()
	d.rejst, d.rejrs = st, reason
	d.mu.Unlock()
}
func (d *testDelivery) delivered() int { d.mu.Lock(); defer d.mu.Unlock(); return d.deliv }

// blockingDelivery 占住 loop 直到 release 关闭。
type blockingDelivery struct {
	id      rans.ID
	release chan struct{}
}

func (b *blockingDelivery) UUID() rans.ID                 { return b.id }
func (b *blockingDelivery) Deliver(p *Pawnite)            { <-b.release }
func (b *blockingDelivery) Reject(rans.Code, rans.Reason) {}

// enterBlockDelivery 进入 Deliver 即投票，随后阻塞至 release 关闭。
type enterBlockDelivery struct {
	id      rans.ID
	entered chan struct{}
	release chan struct{}
}

func (e *enterBlockDelivery) UUID() rans.ID { return e.id }
func (e *enterBlockDelivery) Deliver(p *Pawnite) {
	e.entered <- struct{}{}
	<-e.release
}
func (e *enterBlockDelivery) Reject(rans.Code, rans.Reason) {}

type seqDelivery struct {
	id    rans.ID
	i     int
	mu    *sync.Mutex
	order *[]int
}

func (s *seqDelivery) UUID() rans.ID { return s.id }
func (s *seqDelivery) Deliver(p *Pawnite) {
	time.Sleep(time.Millisecond) // 放大交错窗口
	s.mu.Lock()
	*s.order = append(*s.order, s.i)
	s.mu.Unlock()
}
func (s *seqDelivery) Reject(rans.Code, rans.Reason) {}

func mustUUID(t *testing.T, kind, inst string) rans.ID {
	t.Helper()
	u, err := rans.NewID(kind, inst)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestActivateThenDeliver_InOrder(t *testing.T) {
	id := mustUUID(t, "player", "p1")
	var activated atomic.Bool
	d := &testDelivery{id: id}
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error {
		activated.Store(true)
		return nil
	}})
	p.Start(context.Background())

	// 激活完成前投递也必须排队保序（规格 §4.4：裁决未落原地留队）。
	if !p.ReceiveRequest(d) {
		t.Fatal("admission while activating must succeed (queued)")
	}
	deadline := time.Now().Add(2 * time.Second)
	for d.delivered() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if d.delivered() != 1 {
		t.Fatal("delivery must be processed after activation")
	}
	if !activated.Load() {
		t.Fatal("load-first violated: delivered before Activate ran")
	}
	p.Destroy(context.Background())
}

func TestActivateFailure_RejectsQueued(t *testing.T) {
	id := mustUUID(t, "player", "p2")
	d := &testDelivery{id: id}
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error {
		return context.DeadlineExceeded
	}})
	p.Start(context.Background())
	p.ReceiveRequest(d)
	<-p.Done()
	if d.rejst != rans.CodeUnavailable {
		t.Fatalf("queued message on failed activation: status=%v, want StatusUnavailable (pre-execution failure)", d.rejst)
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state after failed activation = %v", p.State())
	}
}

func TestDestroy_RejectsQueuedAndAdmission(t *testing.T) {
	id := mustUUID(t, "player", "p3")
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	p.ReceiveRequest(&enterBlockDelivery{id: id, entered: entered, release: block}) // 长消息占住 loop
	<-entered                                                                       // 屏障：loop 确认在长消息内，Destroying 一旦置位必持续到放行
	d2 := &testDelivery{id: id}
	p.ReceiveRequest(d2) // 排队
	go func() { p.Destroy(context.Background()) }()
	// 轮询替代定时睡眠，不依赖调度时序；有 enter 屏障在，loop 出不了 Deliver，
	// Destroying 不可能在采样间隙一闪而过（finish 需先离开 Deliver）。
	deadline := time.Now().Add(2 * time.Second)
	for p.State() != StateDestroying && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.State() != StateDestroying {
		t.Fatal("Destroy never reached Destroying")
	}
	close(block) // 放行长消息
	<-p.Done()
	if d2.rejst != rans.CodeUnavailable {
		t.Fatalf("queued message during destroy must be rejected, got %v", d2.rejst)
	}
	after := &testDelivery{id: id}
	if p.ReceiveRequest(after) {
		t.Fatal("admission after Destroyed must be rejected")
	}
	if after.rejst != rans.CodeUnavailable {
		t.Fatalf("post-destroy admission status = %v", after.rejst)
	}
}

func TestInboxFull_Rejects(t *testing.T) {
	id := mustUUID(t, "player", "p4")
	block := make(chan struct{})
	activated := make(chan struct{})
	releaseAct := make(chan struct{})
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error {
			close(activated)
			<-releaseAct
			return nil
		},
		InboxCap: 2,
	})
	p.Start(context.Background())
	// 激活屏障：loop 停在 Activate 内不消费收件箱（规格 §12），占用在此确定，
	// 消除"loop 抢先取走首条 → over 以 2/2 入箱"的调度竞态（既证 flake）。
	<-activated
	p.ReceiveRequest(&blockingDelivery{id: id, release: block})
	p.ReceiveRequest(&blockingDelivery{id: id, release: block}) // 占满
	over := &testDelivery{id: id}
	if p.ReceiveRequest(over) {
		t.Fatal("overflow admission must fail")
	}
	if over.rejst != rans.CodeUnavailable {
		t.Fatalf("overflow must reject Unavailable, got %v", over.rejst)
	}
	close(releaseAct)
	close(block)
	p.Destroy(context.Background())
}

func TestSerialization_AllDeliveriesOnOneGoroutine(t *testing.T) {
	id := mustUUID(t, "player", "p5")
	var mu sync.Mutex
	order := []int{}
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if !p.ReceiveRequest(&seqDelivery{id: id, i: i, mu: &mu, order: &order}) {
				t.Errorf("delivery %d rejected", i)
			}
		}(i)
	}
	wg.Wait()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n == 50 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 50 {
		t.Fatalf("processed %d/50", len(order))
	}
	p.Destroy(context.Background())
}

func TestDestroyBeforeStart_ReachesDestroyed(t *testing.T) {
	id := mustUUID(t, "player", "p6")
	d := &testDelivery{id: id}
	evicted := 0
	p := New(id, Options{OnEvict: func(p *Pawnite) { evicted++ }})
	if !p.ReceiveRequest(d) {
		t.Fatal("admission in Created state must succeed")
	}

	destroyed := make(chan struct{})
	go func() {
		p.Destroy(context.Background())
		close(destroyed)
	}()
	select {
	case <-destroyed:
	case <-time.After(2 * time.Second):
		t.Fatal("Destroy before Start must not block on a loop that will never run")
	}

	if p.State() != StateDestroyed {
		t.Fatalf("state after Destroy-before-Start = %v", p.State())
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done must be closed after Destroy-before-Start")
	}
	if d.rejst != rans.CodeUnavailable {
		t.Fatalf("queued message must be rejected Unavailable, got %v", d.rejst)
	}
	after := &testDelivery{id: id}
	if p.ReceiveRequest(after) {
		t.Fatal("admission after Destroyed must be rejected")
	}
	if after.rejst != rans.CodeUnavailable {
		t.Fatalf("post-destroy admission status = %v", after.rejst)
	}
	if evicted != 1 {
		t.Fatalf("OnEvict must fire exactly once on fast-path destroy, got %d", evicted)
	}
}

func TestDoubleStart_StillSingleLoop(t *testing.T) {
	id := mustUUID(t, "player", "p7")
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	p := New(id, Options{})
	p.Start(context.Background())
	p.Start(context.Background()) // 双 Start：第二次必须不起 loop

	p.ReceiveRequest(&enterBlockDelivery{id: id, entered: entered, release: release})
	p.ReceiveRequest(&enterBlockDelivery{id: id, entered: entered, release: release})

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first delivery never started; loop missing")
	}
	select {
	case <-entered:
		t.Fatal("second delivery ran concurrently: double Start spawned a second loop")
	case <-time.After(100 * time.Millisecond):
		// 单 loop：第二条仍在收件箱排队，符合串行不变量。
	}
	close(release)
	p.Destroy(context.Background())
	if p.State() != StateDestroyed {
		t.Fatalf("state after destroy = %v", p.State())
	}
}

// TestActivateBlockedThenDestroy_NoActiveRegression 钉死状态机单调性：
// Activate 阻塞期间 Destroy 已置 Destroying 后，Activate 返回 nil 不得把状态
// 回退到 Active（run 仅在 StateActivating 才置 Active，Destroying/Destroyed
// 不动——若无条件置 Active，「卡死在 Deactivate 的销毁」会以 Active 面目
// 短暂可见，Destroying→Active→Destroyed 瞬态窗口极窄、轮询未必采到；
// 条件化转移使回退在代码上不可能，本测试确定性绿。
func TestActivateBlockedThenDestroy_NoActiveRegression(t *testing.T) {
	id := mustUUID(t, "player", "p8")
	activated := make(chan struct{}) // 握手：Activate 已进入且阻塞中
	release := make(chan struct{})   // 放行 Activate（此后返回 nil）
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error {
		close(activated)
		<-release
		return nil
	}})
	p.Start(context.Background())
	<-activated // run 已进入 Activate（StateActivating 已置位）

	go func() { p.Destroy(context.Background()) }() // 并发 Destroy：阻塞至终态
	deadline := time.Now().Add(2 * time.Second)
	for p.State() != StateDestroying && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.State() != StateDestroying {
		t.Fatal("Destroy never reached Destroying while Activate blocked")
	}

	close(release) // Activate 返回 nil：临界窗口开启
	// 密集采样直至终态：全程不得观测到 StateActive；Done 落地后必须已是 Destroyed。
	watchdog := time.After(2 * time.Second)
	for {
		if s := p.State(); s == StateActive {
			t.Fatal("state regressed Destroying→Active after Activate returned nil")
		}
		select {
		case <-p.Done():
			if s := p.State(); s != StateDestroyed {
				t.Fatalf("post-Done state = %v, want StateDestroyed", s)
			}
			return
		default:
			runtime.Gosched()
		}
		select {
		case <-watchdog:
			t.Fatal("pawnite never reached terminal state")
		default:
		}
	}
}
