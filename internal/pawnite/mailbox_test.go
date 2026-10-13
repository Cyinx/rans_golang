package pawnite

// 收件箱语义锚定测试：切片 FIFO + 就绪信号改造前后必须保持的不变量——
// stop 预检优先（销毁介入后排队邮件回绝不执行）、满箱回绝语义、邮件间隙
// stop 可达（控制信号不被连续邮件饿死）。空闲回收大缓冲是改造新增行为，
// 见 TestMailbox_IdleShrinksLargeBuffer。

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// countingDelivery 共享计数投递（邮件风暴用：所有实例递增同一计数器）。
type countingDelivery struct {
	id    rans.ID
	mu    *sync.Mutex
	count *int
}

func (c *countingDelivery) UUID() rans.ID { return c.id }
func (c *countingDelivery) Deliver(*Pawnite) {
	c.mu.Lock()
	*c.count++
	c.mu.Unlock()
}
func (c *countingDelivery) Reject(rans.Code, rans.Reason) {}

// TestMailbox_StopPreemptsQueuedMail stop 关闭后：已入箱消息被回绝而非
// 执行（stop 预检优先语义保持——销毁介入后排队邮件不得再获投递）。
func TestMailbox_StopPreemptsQueuedMail(t *testing.T) {
	id := mustUUID(t, "player", "mb-stop")
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	p.ReceiveRequest(&enterBlockDelivery{id: id, entered: entered, release: block}) // 长消息占住 loop
	<-entered                                                                       // 屏障：loop 确认在长消息内
	d1 := &testDelivery{id: id}
	d2 := &testDelivery{id: id}
	if !p.ReceiveRequest(d1) || !p.ReceiveRequest(d2) {
		t.Fatal("queued admission while delivery in flight must succeed")
	}
	go func() { p.Destroy(context.Background()) }()
	// loop 仍卡在长消息内：Destroying 一旦置位必持续可见（finish 需先离开 Deliver）。
	deadline := time.Now().Add(2 * time.Second)
	for p.State() != StateDestroying && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.State() != StateDestroying {
		t.Fatal("Destroy never reached Destroying")
	}
	close(block) // 放行长消息：下一轮迭代 stop 预检必胜，排队邮件走回绝
	<-p.Done()
	for _, d := range []*testDelivery{d1, d2} {
		if d.rejst != rans.CodeUnavailable {
			t.Fatalf("queued mail must be rejected Unavailable, got %v", d.rejst)
		}
		if d.delivered() != 0 {
			t.Fatal("queued mail must not be delivered after stop preemption")
		}
	}
}

// TestMailbox_FullRejectionSemantics 深度到达上限（InboxCap=2）→ 第 3 条
// 以 Unavailable/ReasonBusy 回绝（满箱回绝语义）。
func TestMailbox_FullRejectionSemantics(t *testing.T) {
	id := mustUUID(t, "player", "mb-full")
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
	// 激活屏障：loop 停在 Activate 内不消费收件箱，消除"loop 抢先取走首条"
	// 的调度竞态。
	<-activated
	p.ReceiveRequest(&blockingDelivery{id: id, release: block})
	p.ReceiveRequest(&blockingDelivery{id: id, release: block}) // 占满 2/2
	over := &testDelivery{id: id}
	if p.ReceiveRequest(over) {
		t.Fatal("overflow admission must fail")
	}
	if over.rejst != rans.CodeUnavailable || over.rejrs != rans.ReasonBusy {
		t.Fatalf("overflow must reject Unavailable/ReasonBusy, got %v/%v", over.rejst, over.rejrs)
	}
	close(releaseAct)
	close(block)
	p.Destroy(context.Background())
}

// TestMailbox_IdleShrinksLargeBuffer 空闲回收：灌 300 条再排空后，底层
// 大缓冲被释放（包内持 mu 观测 cap(p.mailbox)==0；小缓冲 ≤256 原地复用，
// 不回收——避免浅队列乒乓流量下的反复分配）。长消息先占住 loop 保证
// 300 条全部堆积后才被消费（并发深度必超 256）。
func TestMailbox_IdleShrinksLargeBuffer(t *testing.T) {
	id := mustUUID(t, "player", "mb-shrink")
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	var mu sync.Mutex
	count := 0
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	p.ReceiveRequest(&enterBlockDelivery{id: id, entered: entered, release: block}) // 长消息占住 loop
	<-entered
	const N = 300
	for i := 0; i < N; i++ {
		if !p.ReceiveRequest(&countingDelivery{id: id, mu: &mu, count: &count}) {
			t.Fatalf("admission %d must succeed while loop blocked", i)
		}
	}
	close(block) // 放行：loop 排空整批
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := count
		mu.Unlock()
		if n == N {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("drained %d/%d", n, N)
		}
		time.Sleep(time.Millisecond)
	}
	// 第 N 条 Deliver 计数发生在其 popMail 之后：计数到 N 即缓冲已回收。
	p.mu.Lock()
	c := cap(p.mailbox)
	p.mu.Unlock()
	if c != 0 {
		t.Fatalf("large buffer must be released after idle drain, cap=%d", c)
	}
	if got := p.InboxLen(); got != 0 {
		t.Fatalf("InboxLen after drain = %d, want 0", got)
	}
	p.Destroy(context.Background())
}

// slowDelivery 慢消费投递（维持持续积压：消费慢于生产）。
type slowDelivery struct {
	id rans.ID
}

func (s *slowDelivery) UUID() rans.ID                 { return s.id }
func (s *slowDelivery) Deliver(*Pawnite)              { time.Sleep(200 * time.Microsecond) }
func (s *slowDelivery) Reject(rans.Code, rans.Reason) {}

// TestMailbox_SustainedBacklog_BoundedLength 持续积压（队列贴近 inboxCap
// 振荡、从不全排空）下底层数组长度有界：≈峰值积压，不随累计入箱数增长。
// 满箱回绝机制针对的持续洪泛正是该场景——数组若按累计入箱数单调增长，
// 过载域内存反而劣于被替换的定容 chan。
func TestMailbox_SustainedBacklog_BoundedLength(t *testing.T) {
	id := mustUUID(t, "player", "mb-backlog")
	const capv = 64
	var admitted atomic.Int64
	prodStop := make(chan struct{})
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		InboxCap: capv,
	})
	p.Start(context.Background())
	go func() { // 生产者：持续入箱，被回绝即短暂退避重试（积压贴近 cap 振荡）
		for {
			select {
			case <-prodStop:
				return
			default:
			}
			if p.ReceiveRequest(&slowDelivery{id: id}) {
				admitted.Add(1)
			} else {
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()
	// 累计成功入箱 2000 条（消费 200µs/条：队列全程非空，在 cap 附近振荡）。
	deadline := time.Now().Add(10 * time.Second)
	for admitted.Load() < 2000 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if admitted.Load() < 2000 {
		t.Fatalf("storm only admitted %d mails", admitted.Load())
	}
	// 生产者仍在跑（队列必非空）：此刻采样，数组长度不得随累计入箱数增长。
	p.mu.Lock()
	n := len(p.mailbox)
	depth := n - p.mailHead
	p.mu.Unlock()
	close(prodStop)
	p.Destroy(context.Background())
	if n > 2*capv {
		t.Fatalf("mailbox array grows with cumulative admissions: len=%d (admitted=%d, cap=%d) —— 持续积压下未压缩",
			n, admitted.Load(), capv)
	}
	if depth > capv {
		t.Fatalf("backlog depth %d exceeds inbox cap %d", depth, capv)
	}
}

// TestMailbox_StopReachableBetweenMails 邮件连续到达（风暴灌箱）时 stop
// 仍能在两条邮件的间隙被选中：Destroy 必须收敛（select 各分支随机选择 +
// 每轮迭代 stop 预检），且 Done 之后不再有任何投递发生。
func TestMailbox_StopReachableBetweenMails(t *testing.T) {
	id := mustUUID(t, "player", "mb-gap")
	var mu sync.Mutex
	count := 0
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())

	// 风暴：只要准入放行就持续入箱（销毁介入后准入回绝，风暴自然终止）。
	stormDone := make(chan struct{})
	go func() {
		defer close(stormDone)
		for {
			d := &countingDelivery{id: id, mu: &mu, count: &count}
			if !p.ReceiveRequest(d) {
				return
			}
		}
	}()

	// 先让邮件确实流动起来（队列非空、投递在推进），再发起销毁。
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := count
		mu.Unlock()
		if n >= 100 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("storm never delivered 100 mails")
		}
		time.Sleep(time.Millisecond)
	}

	go func() { p.Destroy(context.Background()) }()
	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("stop unreachable between mails: loop never terminated")
	}
	select {
	case <-stormDone:
	case <-time.After(5 * time.Second):
		t.Fatal("storm goroutine never exited after admission cutoff")
	}
	// 窄化断言：Done 之后投递绝不再发生（间隙语义的收尾面）。
	mu.Lock()
	before := count
	mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	after := count
	mu.Unlock()
	if after != before {
		t.Fatalf("deliveries continued after Done: %d -> %d", before, after)
	}
}
