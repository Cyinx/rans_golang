package testkit

// HR4 验收落点（H12 决策 16/17 后半，票 12）：ReenterAfter 让出重入——
// 让出期间邮箱照常服务（可重入；-race 无锁状态锚）、work 调回不判环、
// work 调回同步等待上游立即判环、交接窗等待在途 work（预算取消 wctx）、
// MsgCancel 取消面扩展（wctx 取消 + 已 Deferred 未 Reply 标记）、本地
// Tell 到达腿的登记点（单向形态只注入让出面，与远端单向同构）。

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// reenterEntity 让出重入道具实体。无锁字段（thenCount/thenErrText/pingCount）
// 只在实体 loop 读写——续体回 loop 的串行性是唯一同步依据，-race 下「then
// 在 work goroutine 直接执行」的突变由无锁并发访问暴露（突变自证锚①）。
type reenterEntity struct {
	target  rans.ID       // 对端身份（caller/peer/chain/yielder 形态布线）
	out     chan<- error  // yielder：work 内层调用错误的直捕观测（非阻塞）
	started chan struct{} // work 起跑信号（缓冲 1 非阻塞——时序锚）
	release chan struct{} // work 放行闸（close = work 返回；nil = 自定形态）
	gate    chan struct{} // then 扣持闸（非 nil：then 进入即扣住——loop 串行判据的进入锚）
	entered chan struct{} // then 已开跑信号（缓冲 1 非阻塞）

	thenCount   int    // 无锁：then 落地计数（loop 串行读写）
	thenErrText string // 无锁：then 收到的错误文本（loop 串行读写）
	pingCount   int    // 无锁：让出期间被服务的消息计数
}

func (e *reenterEntity) Activate(context.Context) error   { return nil }
func (e *reenterEntity) Deactivate(context.Context) error { return nil }

func (e *reenterEntity) Handle(ctx context.Context, req any) (any, error) {
	switch cmd, _ := req.(string); cmd {
	case "yield": // 两件套正本形态：Defer + ReenterAfter + Deferred
		r := rans.Defer(ctx)
		e.signalStarted()
		rans.ReenterAfter(ctx, func(wctx context.Context) (any, error) {
			select {
			case <-e.release:
				return "wv", nil
			case <-wctx.Done():
				return nil, wctx.Err()
			}
		}, func(_ context.Context, v any, err error) {
			if e.gate != nil {
				// 串行判据进入锚：then 开跑即扣住（正确实现下扣住的是实体
				// loop——他消息无法插队；突变①下 work goroutine 被扣、loop
				// 照常空闲，判据立现）。
				e.signalEntered()
				<-e.gate
			}
			e.thenCount++
			if err != nil {
				e.thenErrText = err.Error()
			}
			r.Reply(v, err)
		})
		return rans.Deferred, nil
	case "reenter-call": // work 调 B（B 再调回 A）：wctx 不追加自身 → 正常完成
		r := rans.Defer(ctx)
		e.signalStarted()
		rans.ReenterAfter(ctx, func(wctx context.Context) (any, error) {
			return rans.ClientFrom(wctx).Call(wctx, e.target, "ping")
		}, func(_ context.Context, v any, err error) {
			r.Reply(v, err)
		})
		return rans.Deferred, nil
	case "reenter-cycle": // work 调回同步等待的上游：上游已在路径中 → 立即判环
		r := rans.Defer(ctx)
		e.signalStarted()
		rans.ReenterAfter(ctx, func(wctx context.Context) (any, error) {
			_, err := rans.ClientFrom(wctx).Call(wctx, e.target, "ping")
			select {
			case e.out <- err:
			default:
			}
			return nil, err
		}, func(_ context.Context, v any, err error) {
			r.Reply(v, err)
		})
		return rans.Deferred, nil
	case "reenter-slow": // 长 work（协作取消观测形态）：wctx 取消即快速返回
		r := rans.Defer(ctx)
		e.signalStarted()
		rans.ReenterAfter(ctx, func(wctx context.Context) (any, error) {
			timer := time.NewTimer(30 * time.Second)
			defer timer.Stop()
			select {
			case <-wctx.Done():
				// 收尾 unwind：被取消的外部 IO 返回需要时间（现实形态）——
				// 同时给调用方留出按自身取消收场的确定窗口。
				time.Sleep(50 * time.Millisecond)
				return nil, wctx.Err()
			case <-timer.C:
				return "ok", nil
			}
		}, func(_ context.Context, v any, err error) {
			if err != nil {
				e.thenErrText = err.Error()
			}
			r.Reply(v, err)
		})
		return rans.Deferred, nil
	case "ping":
		e.pingCount++
		if e.target != (rans.ID{}) {
			// peer 形态：同步调回目标（WorkCallsBackOk 的回环腿）。
			return rans.ClientFrom(ctx).Call(ctx, e.target, "pong")
		}
		return "ok", nil
	case "pong":
		return "pong!", nil
	case "chain": // 同步等待下游（WorkCallsWaitingUpstreamCycle 的上游等待腿）
		return rans.ClientFrom(ctx).Call(ctx, e.target, "reenter-cycle")
	case "state":
		return e.thenCount, nil
	case "then-err":
		return e.thenErrText, nil
	}
	return "ok", nil
}

func (e *reenterEntity) signalStarted() {
	select {
	case e.started <- struct{}{}:
	default:
	}
}

func (e *reenterEntity) signalEntered() {
	select {
	case e.entered <- struct{}{}:
	default:
	}
}

// reenterLegs 测试参数：local = 单节点（本地投递腿）；remote = 双节点（实体
// 预激活节点 2，远端投递腿）。
var reenterLegs = []struct {
	name  string
	nodes int
}{
	{"local", 1},
	{"remote", 2},
}

// TestReenterAfter_MailboxUnblocked 让出期间同实体他消息照常处理（顺序完成
// 计数）；then 回 loop 后状态写入对后续消息可见；续体执行期扣住 loop（串行
// 判据：扣住的续体不允许他消息插队——突变①下 then 落在 work goroutine，
// loop 照常空闲，判据必红）；-race 下实体状态访问无竞态（loop 串行是唯一
// 同步依据）。
// 突变自证①：then 不回 loop（work goroutine 直接执行）→ 串行判据红 +
// thenCount 写读对 -race 红。
func TestReenterAfter_MailboxUnblocked(t *testing.T) {
	for _, leg := range reenterLegs {
		t.Run(leg.name, func(t *testing.T) {
			e := &reenterEntity{
				started: make(chan struct{}, 1),
				release: make(chan struct{}),
				gate:    make(chan struct{}),
				entered: make(chan struct{}, 1),
			}
			c := NewCluster(t, leg.nodes, map[string]rans.Factory{
				"reent": func(rans.ID) rans.Entity { return e },
			})
			id := mustID(t, "reent", "yield-1")
			node := c.Node(0)
			ctx := context.Background()
			if leg.nodes == 2 {
				if _, err := c.Node(1).Call(ctx, id, "warm"); err != nil {
					t.Fatalf("pre-activate entity on node 2: %v", err)
				}
			}

			type outcome struct {
				v   any
				err error
			}
			out := make(chan outcome, 1)
			go func() {
				v, err := node.Call(ctx, id, "yield")
				out <- outcome{v, err}
			}()
			<-e.started // work 已起跑：Handle 已返回 Deferred，loop 让出

			// 让出期间灌多条消息：照常处理、顺序完成（单 goroutine 调用的
			// FIFO 应答序；计数在实体 pingCount——无锁 loop 串行访问）。
			for i := 0; i < 3; i++ {
				if v, err := node.Call(ctx, id, "ping"); err != nil || v != "ok" {
					t.Fatalf("ping %d during yield = (%v, %v), want (ok, nil)", i, v, err)
				}
			}

			close(e.release) // 放行 work：then 回 loop
			<-e.entered      // then 已开跑（正确实现下 loop 已被续体扣住）

			// 串行判据：续体执行期他消息不得插队——"state" 查询必须等不到
			// 执行（短预算 Timeout）。突变①下 loop 空闲、查询立即返回。
			if _, err := node.CallOpts(ctx, id, "state", rans.Timeout(300*time.Millisecond)); !errors.Is(err, rans.ErrTimeout) {
				t.Fatalf("state probe during continuation = %v, want ErrTimeout (continuation must hold the loop)", err)
			}
			close(e.gate) // 放行续体：thenCount++ → Reply 收场
			select {
			case o := <-out:
				if o.err != nil {
					t.Fatalf("yielded call must complete via then's Reply, got %v", o.err)
				}
				if o.v != "wv" {
					t.Fatalf("yielded call value = %v, want wv", o.v)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("caller never received the deferred reply")
			}

			// then 的状态写入对后续消息可见（续体确已回 loop 串行落地；
			// 突变①下本读与 work goroutine 的写构成 -race 并发对）。
			if v, err := node.Call(ctx, id, "state"); err != nil || v != 1 {
				t.Fatalf("then must land on the loop (state = %v, %v), want (1, nil)", v, err)
			}
			if v, err := node.Call(ctx, id, "ping"); err != nil || v != "ok" {
				t.Fatalf("entity loop must stay healthy, got (%v, %v)", v, err)
			}
		})
	}
}

// TestReenterAfter_WorkCallsBackOk A 的 work 调 B、B 的 Handle 再调回 A——
// 正常完成（A 的邮箱在让出期空闲可服务 B 的调用；wctx 不含 A 自身不判环）。
// 突变自证②：wctx 追加自身哈希 → B→A 的准入判环回绝，本测试红。
func TestReenterAfter_WorkCallsBackOk(t *testing.T) {
	aID := mustID(t, "reent", "ra")
	bID := mustID(t, "reent", "rb")
	a := &reenterEntity{started: make(chan struct{}, 1), target: bID}
	b := &reenterEntity{target: aID}
	c := NewCluster(t, 1, map[string]rans.Factory{
		"reent": func(id rans.ID) rans.Entity {
			if id.Key == "ra" {
				return a
			}
			return b
		},
	})
	ctx := context.Background()

	v, err := c.Node(0).Call(ctx, aID, "reenter-call")
	if err != nil {
		t.Fatalf("work calls B and B calls back A must complete (wctx carries no self hop), got %v", err)
	}
	if v != "pong!" {
		t.Fatalf("value = %v, want pong!", v)
	}
	// 实体对照常服务。
	if v, err := c.Node(0).Call(ctx, aID, "warm"); err != nil || v != "ok" {
		t.Fatalf("entity loop must stay healthy, got (%v, %v)", v, err)
	}
}

// TestReenterAfter_WorkCallsWaitingUpstreamCycle A 等 B（同步 Call）、B 的
// work 里调 A（A 仍在同步等待）→ 立即判环（上游 A 已在 B 的 work 出站路径
// 中）：内层直捕 (CodeError, ReasonCycle)、文本带完整环、快速失败（不卡到
// 超时）；外层经 Handle 错误边界折叠（Reason 归 Handler，环文本保留）。
func TestReenterAfter_WorkCallsWaitingUpstreamCycle(t *testing.T) {
	aID := mustID(t, "reent", "ca")
	bID := mustID(t, "reent", "cb")
	inner := make(chan error, 4) // work 内层调用的直捕观测（准入回绝原样）
	a := &reenterEntity{target: bID}
	b := &reenterEntity{started: make(chan struct{}, 1), target: aID, out: inner}
	c := NewCluster(t, 1, map[string]rans.Factory{
		"reent": func(id rans.ID) rans.Entity {
			if id.Key == "ca" {
				return a
			}
			return b
		},
	})
	ctx := context.Background()

	start := time.Now()
	_, err := c.Node(0).Call(ctx, aID, "chain")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("work calling back the synchronously-waiting upstream must fail, got nil error")
	}
	select {
	case ie := <-inner:
		if rans.CodeOf(ie) != rans.CodeError || rans.ReasonOf(ie) != rans.ReasonCycle {
			t.Fatalf("inner call error = %v, want CodeError+ReasonCycle", ie)
		}
		if !strings.Contains(ie.Error(), hexHash(aID)) {
			t.Fatalf("cycle text must carry the full ring (hash of A %s): %v", hexHash(aID), ie)
		}
		if elapsed > 2*time.Second {
			t.Fatalf("cycle must fast-fail (not wait out the budget), took %v", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no inner outcome captured")
	}
	if rans.ReasonOf(err) != rans.ReasonHandler || !strings.Contains(err.Error(), hexHash(aID)) {
		t.Fatalf("outer fold = %v, want ReasonHandler with the ring text preserved", err)
	}
}

// tellArmEntity 本地 Tell 让出形态的道具实体：Handle 内 ReenterAfter（无
// Defer 的单向形态——契约「只 ReenterAfter 不 Defer 也合法」），续体 then
// 落 flag。逐激活独立实例（factory 逐次新建），起跑/放行信号通道跨实例
// 共享（测试侧握把）；两闸皆 nil 的实例 work 即刻返回（对照组形态）。
// flag 无锁：Handle 与续体都回实体 loop 串行读写。
type tellArmEntity struct {
	started chan struct{} // work 起跑信号（缓冲 1 非阻塞——时序锚）
	release chan struct{} // work 放行闸（close = work 返回；nil = 即刻返回）
	flag    bool          // 续体落地标记（loop 串行读写）
}

func (e *tellArmEntity) Activate(context.Context) error   { return nil }
func (e *tellArmEntity) Deactivate(context.Context) error { return nil }

func (e *tellArmEntity) Handle(ctx context.Context, req any) (any, error) {
	switch cmd, _ := req.(string); cmd {
	case "arm":
		rans.ReenterAfter(ctx, func(wctx context.Context) (any, error) {
			e.signalStarted()
			if e.release != nil {
				select {
				case <-e.release:
				case <-wctx.Done():
					return nil, wctx.Err()
				}
			}
			return nil, nil
		}, func(context.Context, any, error) { e.flag = true })
		return nil, nil
	case "get":
		return e.flag, nil
	case "ping":
		return "ok", nil
	}
	return nil, nil
}

func (e *tellArmEntity) signalStarted() {
	if e.started == nil {
		return
	}
	select {
	case e.started <- struct{}{}:
	default:
	}
}

// TestReenterAfter_LocalTellArm 本地 Tell 到达腿的让出登记点验收：同一实体
// 代码在本地 Tell 到达时 ReenterAfter 正常让出——work 起跑、让出窗邮箱照常
// 服务（与 Call 腿同语义的可重入）、续体 then 回 loop 落 flag；对照组本地
// Call 同一断言面常绿（登记点不随消息到达路径分叉）。缺登记点时 Handle 于
// ReenterAfter 调用点 panic（pawnite recover 折叠回绝，单向无回执面 →
// 静默丢），started 永不到达——本测试红。
func TestReenterAfter_LocalTellArm(t *testing.T) {
	sig := &tellArmEntity{started: make(chan struct{}, 1), release: make(chan struct{})}
	c := NewCluster(t, 1, map[string]rans.Factory{
		"reent": func(id rans.ID) rans.Entity {
			if id.Key == "arm-call" {
				return &tellArmEntity{} // 对照组：work 即刻返回
			}
			return &tellArmEntity{started: sig.started, release: sig.release}
		},
	})
	n := c.Node(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 对照组：本地 Call 携带登记点 → then 落地（回归锚）。
	idCall := mustID(t, "reent", "arm-call")
	if _, err := n.Call(ctx, idCall, "arm"); err != nil {
		t.Fatalf("call arm: %v", err)
	}
	waitTellArmFlag(t, n, ctx, idCall)

	// 实验组：本地 Tell 携带登记点 → work 起跑（让出成立）。
	idTell := mustID(t, "reent", "arm-tell")
	if err := n.Tell(ctx, idTell, "arm"); err != nil {
		t.Fatalf("tell arm: %v", err)
	}
	select {
	case <-sig.started:
	case <-time.After(5 * time.Second):
		t.Fatal("tell arm: work never started (ReenterAfter must not panic on tell delivery)")
	}
	// 让出窗判据：同实体他消息照常服务。
	if v, err := n.Call(ctx, idTell, "ping"); err != nil || v != "ok" {
		t.Fatalf("ping during tell yield = (%v, %v), want (ok, nil)", v, err)
	}
	close(sig.release) // 放行 work：续体回 loop
	waitTellArmFlag(t, n, ctx, idTell)
}

// waitTellArmFlag 有界轮询续体落地标记（then 回 loop 与查询的相对时序不定，
// 窗口内必须可见）。
func waitTellArmFlag(t *testing.T, n *Node, ctx context.Context, id rans.ID) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		v, err := n.Call(ctx, id, "get")
		if err != nil {
			t.Fatalf("get flag: %v", err)
		}
		if b, _ := v.(bool); b {
			return
		}
		select {
		case <-deadline:
			t.Fatal("reenter continuation never landed (flag stayed false)")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
