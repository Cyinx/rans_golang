package rpc

// 自源应答回归测试（trap 4 self case）：转发消息的 SourceNodeID 等于
// 本进程节点号时，Dispatcher.reply 不得走网络发送——SendToNode(self) 的
// 回环双端连接会被 tcp.Manager 的同节点顶替策略掐断，应答永远到不了
// 挂起中的 pending 表项（调用方固定超时收场）。正解是短路进本进程
// pending 表：reply 按 SourceNodeID 寻址，而源即本进程时「路由」就是
// Gateway.HandleResponse 本身。
//
// 编排（生产形态：陈旧 Lookup 视图 + 激活仲裁回弹）：
// W（节点 1）= 调用方兼赢家，L（节点 2）= 输家转发者，双真 Remote 经回环
// TCP 互联、共享 inmem 目录。目录预注册 target 归 L（W 的 Lookup 因此把
// Call 路由给 L）；L 的目录 Register 被门控（register-first：注册
// 先于装载、仲裁在 Register 原子域裁决），请求入箱后测试把目录条目翻转给 W（等效于 W 的注册在
// L 注册门控期间胜出）→ 放行后 L 的注册真实落败 → 整箱转发原始字节给 W
//（SourceNodeID=W 原样保留）→ W 激活本地 pawnite 执行并应答「源=W=自身」。
// 全程握手 channel + 顺序探针，无承重 sleep；请求入箱的确定性由探针 Call
// 的响应返回钉死（同连接 FIFO）。

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
)

// probeEntity 顺序探针实体（testkit 验收③同型）：Activate/Deactivate 空转，
// Handle 原样回显。独立于被门控的 cell kind，预激活在 L 上常驻。
type probeEntity struct{}

func (probeEntity) Activate(context.Context) error   { return nil }
func (probeEntity) Deactivate(context.Context) error { return nil }
func (probeEntity) Handle(_ context.Context, req any) (any, error) {
	return req, nil
}

// dialLedger 出站拨号台账：记录每次拨号的目标地址（自拨检测锚点——
// 零自拨断言直接比对地址）。
type dialLedger struct {
	mu    sync.Mutex
	addrs []string
}

func (d *dialLedger) dial(ctx context.Context, addr string) (net.Conn, error) {
	d.mu.Lock()
	d.addrs = append(d.addrs, addr)
	d.mu.Unlock()
	var nd net.Dialer
	return nd.DialContext(ctx, "tcp", addr)
}

func (d *dialLedger) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.addrs...)
}

// selfReplyGateDir L 侧注册门控目录：target 的 Register（loser 节点发起）
// 进入即上报并停在 gate 上——把「输家的注册在途」钉成确定性窗口
// （register-first：注册即仲裁，门控在注册面）。
// 其余调用（含 W 的 Register 与测试侧翻转操作）透传。
type selfReplyGateDir struct {
	directory.Directory
	target  rans.ID
	loser   rans.NodeID
	entered chan struct{}
	once    sync.Once
	gate    chan struct{}
}

func (d *selfReplyGateDir) Register(ctx context.Context, id rans.ID, self directory.Owner, expected *directory.Owner) (bool, directory.Owner, error) {
	if id == d.target && self.Node == d.loser {
		d.once.Do(func() { close(d.entered) })
		<-d.gate
	}
	return d.Directory.Register(ctx, id, self, expected)
}

// selfReplyNode 自源回归测试节点。
type selfReplyNode struct {
	rem *Remote
}

// startSelfReplyNode 构造并启动节点：cell 工厂由测试注入（L 侧带门控）、
// probe 工厂恒为回显实体；Dial 注入拨号台账；timeout ≤0 取测试缺省 5s。
func startSelfReplyNode(t *testing.T, id rans.NodeID, dir directory.Directory, rt *resolveTable,
	dial func(context.Context, string) (net.Conn, error), timeout time.Duration,
	cell rans.Factory) *selfReplyNode {
	t.Helper()
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	mgr := activation.New(dir, id, map[string]rans.Factory{
		"cell":  cell,
		"probe": func(_ rans.ID) rans.Entity { return probeEntity{} },
	})
	rem, err := New(Config{
		NodeID:             id,
		ClusterName:        "loop-cluster",
		Addr:               "127.0.0.1:0",
		Secret:             []byte("loop-secret"),
		Dir:                dir,
		Manager:            mgr,
		Resolve:            rt.resolve,
		DefaultCallTimeout: timeout,
		Dial:               dial,
	})
	if err != nil {
		t.Fatalf("new remote node %d: %v", id, err)
	}
	if err := rem.Start(context.Background()); err != nil {
		t.Fatalf("start remote node %d: %v", id, err)
	}
	t.Cleanup(func() { _ = rem.Close() })
	return &selfReplyNode{rem: rem}
}

// TestForwardedSelfSourceReplyShortCircuitsToLocalPending：转发回弹的
// 自源应答必须短路进本地 pending 表——W 的 Call 拿到正确响应值且全程
// 零自拨（拨号台账里没有指向 W 自身地址的条目）。
func TestForwardedSelfSourceReplyShortCircuitsToLocalPending(t *testing.T) {
	inner := inmem.New()
	rt := &resolveTable{}

	// L 的注册门控：L 的 target Register 停在 gate 上，测试完成入箱证明 +
	// 目录翻转后再放行（注册先于装载——输家在装载前即被 Register 裁定出局）。
	gate := make(chan struct{})
	var openGate sync.Once
	defer openGate.Do(func() { close(gate) }) // 失败路径兜底放行，防 goroutine 悬挂
	lCreated := make(chan struct{}, 4)        // L 侧目标实体创建信号（factory 同步上报）
	gdir := &selfReplyGateDir{
		Directory: inner,
		target:    mustUUID(t, "self-src"),
		loser:     2,
		entered:   make(chan struct{}),
		gate:      gate,
	}
	dir := directory.Directory(gdir)

	wDials := &dialLedger{}
	lDials := &dialLedger{}

	w := startSelfReplyNode(t, 1, dir, rt, wDials.dial, 2*time.Second,
		func(_ rans.ID) rans.Entity {
			return &testEntity{handle: func(ctx context.Context, req any) (any, error) {
				return req.(int) + 1000, nil // 赢家执行面：返回值 1007 是断言锚点
			}}
		})
	l := startSelfReplyNode(t, 2, dir, rt, lDials.dial, 0,
		func(_ rans.ID) rans.Entity {
			lCreated <- struct{}{}
			return &testEntity{handle: func(ctx context.Context, req any) (any, error) {
				t.Error("loser must not run entity Handle")
				return nil, nil
			}}
		})
	rt.set(map[rans.NodeID]string{1: w.rem.Addr(), 2: l.rem.Addr()})

	ctx := context.Background()
	target := mustUUID(t, "self-src")
	probeID, err := rans.NewID("probe", "order")
	if err != nil {
		t.Fatal(err)
	}

	// 预注册 target 归 L：W 的 Lookup 据此把 Call 路由给 L（陈旧视图等效构造）。
	// 直连 inner 注册——包装层的 Register 对 (target, loser=2) 门控，测试
	// 自己的播种不得进 gate。
	if won, _, err := inner.Register(ctx, target, directory.Owner{Node: 2}, nil); err != nil || !won {
		t.Fatalf("pre-register target at L: won=%v err=%v", won, err)
	}
	// 预热顺序探针：L 本地激活 probeID（miss → 本地路径），常驻可回快应答。
	if _, err := l.rem.Call(ctx, probeID, 0); err != nil {
		t.Fatalf("probe warm-up: %v", err)
	}

	// W 发起目标 Call（挂起等应答；MessageID 已进 W 网关 pending 表）。
	type callRes struct {
		v   any
		err error
	}
	callDone := make(chan callRes, 1)
	go func() {
		v, err := w.rem.Call(ctx, target, 7)
		callDone <- callRes{v: v, err: err}
	}()

	// 等 L 侧目标实体创建（= W 的请求帧已被 L 的读循环处理到 GetOrActivate）
	// 与 L 的 Register 进入门控（激活闭包首步——装载/Activate 尚未开始）。
	select {
	case <-lCreated:
	case <-time.After(5 * time.Second):
		t.Fatal("loser target entity was not created within 5s")
	}
	select {
	case <-gdir.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("loser register never entered the gate within 5s")
	}
	// 顺序探针：其响应返回 ⟹ 同一条 W→L 连接上先到的目标帧已被完整处理，
	// 即 remoteDelivery 确定已入 L 的 pawnite 收件箱（此刻激活仍被门控）。
	if _, err := w.rem.Call(ctx, probeID, 0); err != nil {
		t.Fatalf("order probe: %v", err)
	}

	// 目录翻转：L → W（等效于 W 的注册在 L 注册门控期间胜出；L 的注册仍在
	// 途、从未落位——翻转的是测试预注册的条目）。实例级注销按现归属标记。
	cur, found, err := dir.Lookup(ctx, target)
	if err != nil || !found {
		t.Fatalf("lookup before flip: found=%v err=%v", found, err)
	}
	if ok, err := dir.UnregisterIfOwner(ctx, target, cur); err != nil || !ok {
		t.Fatalf("flip unregister: ok=%v err=%v", ok, err)
	}
	if won, _, err := dir.Register(ctx, target, directory.Owner{Node: 1}, nil); err != nil || !won {
		t.Fatalf("flip register: won=%v err=%v", won, err)
	}

	// 放行：L 的注册真实落败于 W → pawnite 整箱转发原始字节给 W
	// （MessageID/SourceNodeID=W 原样保留）→ W 激活本地 pawnite 执行并应答
	// 「源=W=自身」——短路进本地 pending 表，W 的 Call 就此收场。
	openGate.Do(func() { close(gate) })

	select {
	case r := <-callDone:
		if r.err != nil {
			t.Fatalf("self-source forwarded call must succeed, got: %v", r.err)
		}
		if r.v != 1007 {
			t.Fatalf("call returned %v, want 1007", r.v)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("call did not finish within 10s (reply never reached the local pending entry)")
	}

	// 零自拨：W 全程只拨过 L 一条连接（应答回程走短路，不碰网络）；
	// L 复用接入连接回执与转发，也从未拨号。
	if wd := wDials.snapshot(); len(wd) != 1 || wd[0] != l.rem.Addr() {
		t.Fatalf("winner must dial only the loser once and never itself, dialed %v (want [%s])",
			wd, l.rem.Addr())
	}
	if n := len(lDials.snapshot()); n != 0 {
		t.Fatalf("loser must reuse the accepted connection, %d extra dials", n)
	}
}
