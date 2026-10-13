package rpc

// 排空次序与 draining 标记契约。门闸语义——排空中
// 本地表命中的目标照常准入（不进激活路径）、未命中回 Unavailable 促重路
// 由；Retire 交接窗的留箱消息在注销后以 Unavailable 回绝、调用方重试落他
// 节点；Shutdown 的阶段④停序列支持宿主前置收口（Config.ShutdownTeardown
// ——停心跳先于节点注销与关传输）。复用 dispatcher_test/remote_test/
// drain_test 的既有基座（dispEnv/startRemNode/gateDeactEntity），时序断言
// 走 channel。

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/protocol"
)

// ---- 排空门闸（dispatcher 层）----

// 1. 排空中本地表命中的目标照常准入：非幂等调用成功执行并回 Response；
// 零新激活（工厂只调一次）；本地 miss 的新实体得 Error(StatusUnavailable)
// （促重路由）；miss 的 OneWay 丢弃留痕。
func TestDispatcherDrainGateAdmitsLocalWhileDraining(t *testing.T) {
	handled := make(chan int, 8)
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		handled <- req.(int)
		return req.(int) + 1, nil
	}, false)

	// 先激活 cell/lv：建立本地表命中前提
	env.feed(reqHdrFor(protocol.MsgRequest, 701, "lv", nil), envBody(t, 1))
	fr := mustSent(t, env.fake)
	if fr.hdr.Type != protocol.MsgResponse {
		t.Fatalf("pre-drain request must activate and respond, got %+v", fr.hdr)
	}
	env.pool.Put(fr.body)
	if v := <-handled; v != 1 {
		t.Fatalf("pre-drain handle = %v, want 1", v)
	}

	env.disp.BeginDraining()

	// 本地命中：照常准入执行（非幂等调用成功）
	env.feed(reqHdrFor(protocol.MsgRequest, 702, "lv", nil), envBody(t, 2))
	fr2 := mustSent(t, env.fake)
	if fr2.hdr.Type != protocol.MsgResponse || fr2.hdr.Status != rans.CodeOK {
		t.Fatalf("draining must admit local-table hits, got %+v", fr2.hdr)
	}
	env.pool.Put(fr2.body)
	if v := <-handled; v != 2 {
		t.Fatalf("local hit must execute while draining, handle = %v", v)
	}
	if n := env.led.count(); n != 1 {
		t.Fatalf("draining admission must not activate: %d entities created, want 1", n)
	}

	// 本地 miss：Error(StatusUnavailable)（促重路由），零激活
	env.feed(reqHdrFor(protocol.MsgRequest, 703, "ghost", nil), envBody(t, 3))
	fr3 := mustSent(t, env.fake)
	if fr3.hdr.Type != protocol.MsgError || fr3.hdr.Status != rans.CodeUnavailable {
		t.Fatalf("draining local miss must get Error(StatusUnavailable), got %+v", fr3.hdr)
	}
	if reason := string(fr3.body); !strings.Contains(reason, "drain") {
		t.Fatalf("miss reason=%q must mention draining", reason)
	}
	env.pool.Put(fr3.body)
	if n := env.led.count(); n != 1 {
		t.Fatalf("draining local miss must not activate: %d entities, want 1", n)
	}

	// 本地 miss 的 OneWay：丢弃留痕（零帧零激活）
	env.feed(reqHdrFor(protocol.MsgOneWay, 704, "ghost", nil), envBody(t, 4))
	if n := sentCount(env.fake); n != 0 {
		t.Fatalf("draining one-way miss must produce no frame, got %d", n)
	}
	if n := env.led.count(); n != 1 {
		t.Fatalf("draining one-way miss must not activate: %d entities, want 1", n)
	}

	// 本地命中的 OneWay：照常准入执行（无回执面）
	env.feed(reqHdrFor(protocol.MsgOneWay, 705, "lv", nil), envBody(t, 5))
	select {
	case v := <-handled:
		if v != 5 {
			t.Fatalf("local-hit one-way must execute, handle = %v", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("local-hit one-way was not delivered while draining")
	}
	if n := sentCount(env.fake); n != 0 {
		t.Fatalf("one-way must not produce a reply frame, got %d", n)
	}
}

// ---- 停序（Remote.Shutdown 装配层）----

// 2. Config.ShutdownTeardown 注入的停序头在阶段④最先执行且先于关传输：回
// 调运行时监听器仍可连接（阶段②③期间宿主组件——心跳/轮询——照常存活），
// Shutdown 返回后传输已关。
func TestRemoteShutdownTeardownHeadRunsBeforeTransportClose(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	headRan := make(chan struct{})
	release := make(chan struct{})
	node := startRemNode(t, dir, rt, nodeSpec{
		id:     1,
		handle: echoHandle,
		shutdownTeardown: []func() error{func() error {
			close(headRan)
			<-release // 按住停序头：测试在回调悬停期间验证传输仍可连
			return nil
		}},
	})
	rt.set(map[rans.NodeID]string{1: node.rem.Addr()})

	drained := make(chan error, 1)
	go func() {
		drained <- node.rem.Shutdown(context.Background())
	}()
	select {
	case <-headRan:
	case <-time.After(5 * time.Second):
		t.Fatal("drain never reached the teardown head")
	}
	// 停序头执行点：传输未关（宿主心跳照常语义的结构锚）
	conn, err := net.DialTimeout("tcp", node.rem.Addr(), time.Second)
	if err != nil {
		t.Fatalf("teardown head must run before transport close: %v", err)
	}
	conn.Close()
	close(release)
	if err := <-drained; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	// Shutdown 返回：传输已关
	if conn, err := net.DialTimeout("tcp", node.rem.Addr(), 200*time.Millisecond); err == nil {
		conn.Close()
		t.Fatal("transport must be closed after drain returns")
	}
}

// ---- Retire 交接窗 × 门闸（双节点端到端）----
// 交接窗留箱 → 注销后 Unavailable 回绝 → 重试他节点成功的端到端行为锁在
// drain_test.go 的 TestRemoteDrainGateRejectsNewRequests。

// ---- 并发安全（-race 摇测面）----

// 4. 门闸置位与本地投递并发：BeginDraining 与 HandleMessage 交叉跑不产生
// 数据竞争、不丢页（页恰归还一次——池收支平衡断言；置位幂等——生产路径
// 只进不退，反复置位即真实并发形态）。
func TestDispatcherDrainGateConcurrentToggle(t *testing.T) {
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		return req.(int) + 1, nil
	}, false)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	consumerStop := make(chan struct{})
	wg.Add(1)
	go func() { // 门闸翻转器
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				env.disp.BeginDraining()
			}
		}
	}()
	go func() { // 出帧消费者：替身 channel 容量有限，不消费会队头阻塞发送路径
		//（收场点在池收支平衡断言之后——应答页经 Deliver 归还依赖发送完成）
		for {
			select {
			case <-env.fake.sent: // 帧 body 均为非池化缓冲（应答/错误文本），丢弃即可
			case <-consumerStop:
				return
			}
		}
	}()
	for i := 0; i < 200; i++ {
		env.feed(reqHdrFor(protocol.MsgRequest, uint64(800+i), "lv", nil), envBody(t, i))
	}
	close(stop)
	wg.Wait()
	// 收场：放掉在途应答帧（页归还由 gw/pending 路径承担，此处只断言
	// 池收支平衡——Gets 与 Puts 相等即无泄漏）
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s := env.pool.Stats()
		if s.Puts >= s.Gets {
			close(consumerStop)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(consumerStop)
	s := env.pool.Stats()
	t.Fatalf("pool leaked pages under concurrent drain toggling: gets=%d puts=%d", s.Gets, s.Puts)
}
