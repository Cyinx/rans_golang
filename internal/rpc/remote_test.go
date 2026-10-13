package rpc

// Remote 组合体闭环测试。同进程两个真 Remote 经回环 TCP 互联，
// 共享 inmem 目录；resolver 用 Start 后填充的原子地址表（可更新 resolver
// 的 testkit 等价物）。时序敏感断言一律走 channel，不用 sleep；
// 连接建立/就绪由 tcp.Manager 的 SendToNode 就绪等待点承接，无需编排。

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/ctxkey"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/routing"
	"github.com/cyinx/rans_golang/internal/wire"
)

// ---- 测试基座 ----

// resolveTable 可更新节点地址表：两节点 Start 前表空（Resolve 报错），
// 双方 Start 后由测试一次性填充。
type resolveTable struct {
	m atomic.Pointer[map[rans.NodeID]string]
}

// resolve 实现 Config.Resolve（调用方 ctx 贯穿；查表瞬时完成）：查表，未
// 登记报错。
func (rt *resolveTable) resolve(_ context.Context, node rans.NodeID) (string, error) {
	if m := rt.m.Load(); m != nil {
		if a, ok := (*m)[node]; ok {
			return a, nil
		}
	}
	return "", fmt.Errorf("node %d not in resolve table", node)
}

// set 填充地址表（整体替换）。
func (rt *resolveTable) set(m map[rans.NodeID]string) { rt.m.Store(&m) }

// remNode 测试节点：Remote 组合体 + 本地 manager + 实体激活台账。
type remNode struct {
	rem *Remote
	mgr *activation.Table
	led *entityLedger
}

// nodeSpec 节点构造参数；零值字段取测试缺省。
type nodeSpec struct {
	id        rans.NodeID
	handle    func(ctx context.Context, req any) (any, error)
	secret    []byte
	timeout   time.Duration
	wrap      func(net.Conn) net.Conn
	placement Placement // nil = 不接放置（miss 全本地）
	cache     *routing.LocationCache
	// wrapEntity 实体工厂外包装（retry_wiring_test：注入 Activate 首败
	// 重试场景）；nil = 不包装。
	wrapEntity func(rans.Entity) rans.Entity
	// factories 工厂表整体覆盖（kind 集互异类测试需要节点不承载 cell）；
	// nil = 缺省 {cell} + extraKinds。
	factories map[string]rans.Factory
	// extraKinds 追加注册的实体工厂（默认仅 cell——kind 集互异类测试需要
	// 目标 kind 的真实工厂面）。
	extraKinds map[string]rans.Factory
	// metrics 节点指标注册表（Config.Metrics 注入面）；
	// nil = metrics.Default()（remote.New 的缺省口径）。
	metrics *metrics.Registry
	// strict 透传 Config.StrictSchema（H9 严格摘要面）。
	strict bool
	// shutdownTeardown 透传 Config.ShutdownTeardown（停序头
	// 构造期注入）。
	shutdownTeardown []func() error
	// dial 透传 Config.Dial（发件箱 flush 等测试的拨号注入面）；nil =
	// 标准拨号。
	dial func(context.Context, string) (net.Conn, error)
}

// startRemNode 构造并启动一个节点：manager 注册 "cell" 工厂（实体台账登记
// 每次激活——激活发生在哪个节点的行为锁），Remote 监听回环随机端口。
// 握手摘要在构造期自 codec 全局登记表快照（形状自动登记、
// kinds 不进握手——kind 集互异节点照常互通）。
func startRemNode(t *testing.T, dir directory.Directory, rt *resolveTable, spec nodeSpec) *remNode {
	t.Helper()
	if spec.timeout == 0 {
		spec.timeout = 5 * time.Second // 测试缺省：卡死时按超时快速失败而非挂 30s
	}
	if spec.secret == nil {
		spec.secret = []byte("loop-secret") // 双端一致，顺路覆盖握手 MAC 路径
	}
	led := &entityLedger{}
	factories := map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity {
			e := &testEntity{handle: spec.handle}
			led.add(e)
			if spec.wrapEntity != nil {
				return spec.wrapEntity(e)
			}
			return e
		},
	}
	// extraKinds 并入工厂表（幂等覆盖）。
	for k, f := range spec.extraKinds {
		factories[k] = f
	}
	// 工厂表整体覆盖面（kind 集互异类测试：给出即替换缺省 cell）。
	if spec.factories != nil {
		factories = spec.factories
	}
	// 缓存失效/回填钩子（归表构造期，经 Options 注入；缓存未配置 = nil
	// 钩子）。
	var hooks activation.Options
	if spec.cache != nil {
		hooks = activation.Options{
			OnEvicted:         func(id rans.ID) { spec.cache.Invalidate(id) },
			OnArbitrationLost: func(id rans.ID, winner directory.Owner) { spec.cache.PutPositive(id, winner) },
			OnMigrated:        func(id rans.ID, to rans.NodeID) { spec.cache.PutPositive(id, directory.Owner{Node: to}) },
		}
	}
	mgr := activation.New(dir, spec.id, factories, hooks)
	rem, err := New(Config{
		NodeID:             spec.id,
		ClusterName:        "loop-cluster",
		Addr:               "127.0.0.1:0",
		Secret:             spec.secret,
		Dir:                dir,
		Manager:            mgr,
		Resolve:            rt.resolve,
		DefaultCallTimeout: spec.timeout,
		WrapConn:           spec.wrap,
		Dial:               spec.dial,
		Placement:          spec.placement,
		LocationCache:      spec.cache,
		Metrics:            spec.metrics,
		StrictSchema:       spec.strict,
		ShutdownTeardown:   spec.shutdownTeardown,
	})
	if err != nil {
		t.Fatalf("new remote node %d: %v", spec.id, err)
	}
	if err := rem.Start(context.Background()); err != nil {
		t.Fatalf("start remote node %d: %v", spec.id, err)
	}
	t.Cleanup(func() { _ = rem.Close() })
	return &remNode{rem: rem, mgr: mgr, led: led}
}

// mustUUID 构造测试身份（kind 固定 cell）。
func mustUUID(t *testing.T, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID("cell", inst)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// 1. 双节点回环 RPC：node1 先 Call（miss→本地激活）→ node2 Call 同 ID
// （Lookup 命中 node1→远程执行）→ 值正确；后续多轮计数递增证明远端实体
// 状态连续（实体一直活在 node1，node2 从未激活它）。
func TestRemoteLoopbackTwoNodesCall(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	var n1 atomic.Int32
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id: 1,
		handle: func(ctx context.Context, req any) (any, error) {
			return int(n1.Add(1)), nil // 返回累计计数：状态连续性的观测面
		},
	})
	node2 := startRemNode(t, dir, rt, nodeSpec{
		id: 2,
		handle: func(ctx context.Context, req any) (any, error) {
			return -1, nil // 若被调用，返回值立刻暴露路由错误
		},
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	ctx := context.Background()
	id := mustUUID(t, "loop-1")

	// 第一轮：node1 miss → 本地激活，计数 1
	v, err := node1.rem.Call(ctx, id, 0)
	if err != nil {
		t.Fatalf("first local call: %v", err)
	}
	if v != 1 {
		t.Fatalf("first local call returned %v, want 1", v)
	}

	// 后续四轮：node2 命中 node1 → 远程执行，远端计数 2..5 连续递增
	for want := 2; want <= 5; want++ {
		v, err := node2.rem.Call(ctx, id, 0)
		if err != nil {
			t.Fatalf("remote round %d: %v", want, err)
		}
		if v != want {
			t.Fatalf("remote round %d returned %v, want %d", want, v, want)
		}
	}
	if n := n1.Load(); n != 5 {
		t.Fatalf("node1 entity executed %d times, want 5", n)
	}
	if n := node2.led.count(); n != 0 {
		t.Fatalf("node2 must never activate the entity (owner is node1), %d activations", n)
	}
}

// 2. hit=self → 本地执行、零网络：WrapConn 对后续一切拨入/拨出裸连接生效，
// 计数恒 0 即证明两次 Call（miss 激活 + hit=self）都没有碰过网络。
func TestRemoteCallRoutesToLocalWhenSelf(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{} // 故意不填表：本测试预期零网络
	var wraps atomic.Int32
	node := startRemNode(t, dir, rt, nodeSpec{
		id: 1,
		handle: func(ctx context.Context, req any) (any, error) {
			return "local-ok", nil
		},
		wrap: func(c net.Conn) net.Conn {
			wraps.Add(1)
			return c
		},
	})
	ctx := context.Background()
	id := mustUUID(t, "self-1")

	v1, err := node.rem.Call(ctx, id, "ping") // miss → 本地激活
	if err != nil {
		t.Fatalf("miss call: %v", err)
	}
	if v1 != "local-ok" {
		t.Fatalf("miss call returned %v, want local-ok", v1)
	}
	v2, err := node.rem.Call(ctx, id, "ping") // 目录 hit=self → 本地直调
	if err != nil {
		t.Fatalf("self-hit call: %v", err)
	}
	if v2 != "local-ok" {
		t.Fatalf("self-hit call returned %v, want local-ok", v2)
	}
	if n := wraps.Load(); n != 0 {
		t.Fatalf("self-routed calls must not touch the network, %d connections were established", n)
	}
}

// 3. 跨节点因果透传（H12 决策 14 v2）：顶层远程 Call 由出站挂点生成根
// trace 并携空路径过线（实体侧读到非 nil、空路径）；调用方 ctx 预置因果
// 路径（ctxkey.WithCausal）→ 实体读到同 Root 与同路径（顶层无自身身份，
// 挂点不追加）。Call 返回即 Handle 已完成（应答在 Handle 之后构造），
// channel 断言确定性成立。
func TestRemoteCrossNodeCausalAttached(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	got := make(chan *ctxkey.CausalPath, 4)
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id: 1,
		handle: func(ctx context.Context, req any) (any, error) {
			got <- ctxkey.CausalFrom(ctx)
			return 1, nil
		},
	})
	node2 := startRemNode(t, dir, rt, nodeSpec{
		id:     2,
		handle: func(ctx context.Context, req any) (any, error) { return 1, nil },
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	id := mustUUID(t, "causal-1")

	if _, err := node1.rem.Call(context.Background(), id, 0); err != nil {
		t.Fatalf("local activation call: %v", err)
	}
	select { // 顶层调用：出站挂点生成根 trace、路径为空（调用方非实体）
	case cp := <-got:
		if cp == nil || cp.Root == "" || len(cp.Path) != 0 {
			t.Fatalf("top-level call must carry a generated root with empty path, got %+v", cp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("local Handle did not run")
	}

	cctx := ctxkey.WithCausal(context.Background(),
		&ctxkey.CausalPath{Root: "trace-cross", Path: []uint64{2}})
	if _, err := node2.rem.Call(cctx, id, 0); err != nil {
		t.Fatalf("remote call with causal: %v", err)
	}
	select {
	case cp := <-got:
		if cp == nil || cp.Root != "trace-cross" || len(cp.Path) != 1 || cp.Path[0] != 2 {
			t.Fatalf("entity must observe the caller's causal path, got %+v", cp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remote Handle did not run")
	}
}

// 4. Stop 幂等：建立过连接后连续两次 Stop（每节点）不 panic，且 listener
// 已关（再拨得到拒绝）。
func TestRemoteStopIdempotent(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id:     1,
		handle: func(ctx context.Context, req any) (any, error) { return 1, nil },
	})
	node2 := startRemNode(t, dir, rt, nodeSpec{
		id:     2,
		handle: func(ctx context.Context, req any) (any, error) { return 1, nil },
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	ctx := context.Background()
	id := mustUUID(t, "stop-1")

	if _, err := node1.rem.Call(ctx, id, 0); err != nil {
		t.Fatalf("local activation call: %v", err)
	}
	if _, err := node2.rem.Call(ctx, id, 0); err != nil { // 建立跨节点连接
		t.Fatalf("cross-node call: %v", err)
	}

	addr1 := node1.rem.Addr()
	_ = node1.rem.Close()
	_ = node1.rem.Close() // 幂等：第二次必须安静
	_ = node2.rem.Close()
	_ = node2.rem.Close()
	if c, err := net.DialTimeout("tcp", addr1, time.Second); err == nil {
		c.Close()
		t.Fatal("listener must be closed after Stop")
	}
}

// 5. kind 集互异互通 + 放置不跨 kind（规格 §6.1 H9：
// 指纹不含 kind 维——异构 kind 由 kind 感知放置承担）：工厂集互不相同的
// 两节点照常握手并互调；未登记身份的 miss 收口经 PickForKind 只落在声明
// 承载该 kind 的节点（另一个节点从未激活它）。
func TestRemoteKindHeterogeneousHandshakeAndPlacement(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	var cellRuns, voxelRuns atomic.Int32
	kindFactories := func(kind string, runs *atomic.Int32) map[string]rans.Factory {
		return map[string]rans.Factory{
			kind: func(_ rans.ID) rans.Entity {
				return &testEntity{handle: func(ctx context.Context, req any) (any, error) {
					return int(runs.Add(1)), nil
				}}
			},
		}
	}
	place := kindFixedPlacement{byKind: map[string]rans.NodeID{"cell": 1, "voxel": 2}}
	// node1 仅承载 cell、node2 仅承载 voxel：工厂集互异 = kind 集互异。
	node1 := startRemNode(t, dir, rt, nodeSpec{id: 1, factories: kindFactories("cell", &cellRuns), placement: place})
	node2 := startRemNode(t, dir, rt, nodeSpec{id: 2, factories: kindFactories("voxel", &voxelRuns), placement: place})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	ctx := context.Background()

	// ① 目录路由腿：cell 归属 node1，从 node2 调用 → 远程执行于 node1。
	//    kind 集互异的两节点握手成功（指纹不再含 kind 维的承重面）。
	cellID := mustUUID(t, "hetero-1")
	if _, _, err := dir.Register(ctx, cellID, directory.Owner{Node: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if v, err := node2.rem.Call(ctx, cellID, 0); err != nil || v != 1 {
		t.Fatalf("cross-kind-set call must succeed: v=%v err=%v", v, err)
	}
	if n := cellRuns.Load(); n != 1 {
		t.Fatalf("cell entity must run on node1 exactly once, got %d", n)
	}
	if n := node2.led.count(); n != 0 {
		t.Fatalf("node2 must never activate a kind it does not host, got %d activations", n)
	}

	// ② 放置腿：未登记的 voxel 身份从 node1 调用 → 目录 miss → PickForKind
	//    只落 node2（放置不跨 kind），node2 激活并执行。
	var voxelID rans.ID
	if v, err := rans.NewID("voxel", "hetero-2"); err != nil {
		t.Fatal(err)
	} else {
		voxelID = v
	}
	if v, err := node1.rem.Call(ctx, voxelID, 0); err != nil || v != 1 {
		t.Fatalf("kind-placed call must succeed: v=%v err=%v", v, err)
	}
	if n := voxelRuns.Load(); n != 1 {
		t.Fatalf("voxel entity must run on node2 exactly once, got %d", n)
	}
	if n := node1.led.count(); n != 0 {
		t.Fatalf("node1 must never activate a kind it does not host, got %d activations", n)
	}
}

// kindFixedPlacement 测试放置：按 kind 固定映射（PickForKind 命中表内映射，
// 其余回绝；Pick 恒不选——路由梯子优先探测 KindPlacement 扩展面）。
type kindFixedPlacement struct{ byKind map[string]rans.NodeID }

func (p kindFixedPlacement) Pick(rans.NodeID) (rans.NodeID, bool) { return 0, false }

func (p kindFixedPlacement) PickForKind(kind string, exclude rans.NodeID) (rans.NodeID, bool) {
	tgt, ok := p.byKind[kind]
	if !ok || tgt == exclude || tgt == 0 {
		return 0, false
	}
	return tgt, true
}

// 5b. SchemaDigest 观测与严格模式（规格 §6.1 H9：指纹
// 降级为观测）：
//   - 默认（非严格）：摘要失配只告警+计数（rans.handshake.schema_mismatch），
//     连接保持、跨节点调用照常成功；
//   - StrictSchema=true：失配拒连（ErrHandshakeSchema），调用有界失败。
//
// 进程内构造真实失配：remote.New 在构造期快照 codec.SchemaDigest()，两次
// 构造之间经公开登记面 codec.RegisterShapes 追加形状 → 两节点摘要不等
// （无任何摘要覆盖配置——生产形态的失配即如此产生）。
func TestRemoteSchemaDigestObservationAndStrict(t *testing.T) {
	ctx := context.Background()
	handle := func(ctx context.Context, req any) (any, error) { return 1, nil }

	// ---- 观测面：失配不拒连 + 计数 ----
	dir := inmem.New()
	rt := &resolveTable{}
	reg1 := metrics.New("obs-1")
	ok1 := startRemNode(t, dir, rt, nodeSpec{id: 1, handle: handle, metrics: reg1})
	codec.RegisterShapes([]string{"SchemaDigestProbeMsg:Field:1:int64"}) // ok2 的摘要 ≠ ok1
	reg2 := metrics.New("obs-2")
	ok2 := startRemNode(t, dir, rt, nodeSpec{id: 2, handle: handle, metrics: reg2})
	rt.set(map[rans.NodeID]string{1: ok1.rem.Addr(), 2: ok2.rem.Addr()})
	obsID := mustUUID(t, "digest-obs-1")
	if _, _, err := dir.Register(ctx, obsID, directory.Owner{Node: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if v, err := ok2.rem.Call(ctx, obsID, 0); err != nil || v != 1 {
		t.Fatalf("digest mismatch must keep the connection (observation mode): v=%v err=%v", v, err)
	}
	// 计数：接受端（ok1 对入站握手）与拨号端（ok2 对入站 Ack）各自 +1。
	for name, reg := range map[string]*metrics.Registry{"listener": reg1, "dialer": reg2} {
		if got := reg.Counter(metrics.NameHandshakeSchemaMismatch, "").Value(); got < 1 {
			t.Fatalf("%s-side schema mismatch counter=%v want >=1", name, got)
		}
	}

	// ---- 严格面：失配拒连 ----
	dir2 := inmem.New()
	rt2 := &resolveTable{}
	strict1 := startRemNode(t, dir2, rt2, nodeSpec{id: 3, handle: handle, strict: true})
	codec.RegisterShapes([]string{"SchemaDigestProbeMsg2:Field:1:int64"}) // strict2 的摘要 ≠ strict1
	strict2 := startRemNode(t, dir2, rt2, nodeSpec{id: 4, handle: handle})
	rt2.set(map[rans.NodeID]string{3: strict1.rem.Addr(), 4: strict2.rem.Addr()})
	badID := mustUUID(t, "digest-strict-1")
	if _, _, err := dir2.Register(ctx, badID, directory.Owner{Node: 3}, nil); err != nil {
		t.Fatal(err)
	}
	strict2.rem.gw.retryInterval = 20 * time.Millisecond // 缩短重试间隔避免 2s×5 拖慢
	if _, err := strict2.rem.Call(ctx, badID, 0); err == nil {
		t.Fatal("strict side must refuse digest mismatch, got call success")
	}
}

// 5c. 握手门禁组合面（raw 帧构造需要 internal/wire，在本包验证）：
// 版本错/集群名错 → 静默断连；摘要错 + 严格 → 静默断连（默认
// 摘要失配降级为观测不拒连，严格面拒连）；握手前直发应用
// Request → 断连且零字节回传（pre-auth 门禁不回错帧，不给探测面，
// 陷阱 10/13）。对完整 Remote 组合体（监听器+帧循环+分发器）验证。
func TestRemoteHandshakeGates(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	node := startRemNode(t, dir, rt, nodeSpec{id: 1})
	addr := node.rem.Addr()
	strictNode := startRemNode(t, dir, rt, nodeSpec{id: 2, strict: true})

	mkHandshake := func(cluster string, digest uint64) wire.Handshake {
		var nonce [16]byte
		for i := range nonce {
			nonce[i] = byte(i + 1)
		}
		return wire.Handshake{
			NodeID:       9,
			ClusterName:  cluster,
			SchemaDigest: digest,
			Nonce:        nonce,
		}
	}

	assertClosedSoon := func(c net.Conn, what string) {
		t.Helper()
		defer c.Close()
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if n, err := c.Read(make([]byte, 1)); err == nil {
			t.Fatalf("%s: connection must be closed, got %d bytes of data", what, n)
		}
	}
	rawHandshake := func(target string, hs wire.Handshake, verMin, verMax uint16) net.Conn {
		t.Helper()
		c, err := net.Dial("tcp", target)
		if err != nil {
			t.Fatalf("raw dial %s: %v", target, err)
		}
		body := hs.Encode([]byte("loop-secret"))
		if verMin != 0 || verMax != 0 {
			binary.LittleEndian.PutUint16(body[0:2], verMin) // VerMin 占握手体首 2 字节
			binary.LittleEndian.PutUint16(body[2:4], verMax) // VerMax 随后 2 字节
		}
		hsHdr := wire.Header{Type: protocol.MsgHandshake}
		frame, err := wire.EncodeFrame(hsHdr.Encode(), body)
		if err != nil {
			c.Close()
			t.Fatalf("encode handshake frame: %v", err)
		}
		if _, err := c.Write(frame); err != nil {
			c.Close()
			t.Fatalf("write handshake frame: %v", err)
		}
		return c
	}

	assertClosedSoon(rawHandshake(addr, mkHandshake("loop-cluster", 1), 2, 2), "wrong version range")
	assertClosedSoon(rawHandshake(addr, mkHandshake("not-loop-cluster", 1), 0, 0), "wrong cluster name")
	assertClosedSoon(rawHandshake(strictNode.rem.Addr(), mkHandshake("loop-cluster", 2), 0, 0), "wrong digest under strict")

	// pre-auth 门禁：合法 Request 帧先于握手到达 → 断连且无任何回传字节。
	c2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("raw dial %s: %v", addr, err)
	}
	defer c2.Close()
	typeName, payload, err := codec.Encode(0)
	if err != nil {
		t.Fatal(err)
	}
	hdr := wire.Header{
		Type:         protocol.MsgRequest,
		Flags:        protocol.FlagHasTargetUUID | protocol.FlagHasSourceNodeID,
		MessageID:    1,
		SourceNodeID: 9,
		TargetKind:   "cell",
		TargetInst:   "pre-auth-probe",
	}
	frame, err := wire.EncodeFrame(hdr.Encode(), wire.MarshalEnvelope(typeName, payload))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c2.Write(frame); err != nil {
		t.Fatalf("write pre-auth request: %v", err)
	}
	c2.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, rerr := c2.Read(make([]byte, 64))
	if n != 0 || !errors.Is(rerr, io.EOF) {
		t.Fatalf("pre-auth app message must close with zero bytes and clean EOF, got n=%d err=%v", n, rerr)
	}
}

// 6. OneWay 交付：node2 SendOneWay → node1 实体收到（channel 断言）；
// 本地 OneWay（hit=self）同样入箱执行。
func TestRemoteOneWayDelivered(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	got := make(chan any, 4)
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id: 1,
		handle: func(ctx context.Context, req any) (any, error) {
			got <- req
			return req, nil
		},
	})
	node2 := startRemNode(t, dir, rt, nodeSpec{
		id:     2,
		handle: func(ctx context.Context, req any) (any, error) { return nil, nil },
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	ctx := context.Background()
	id := mustUUID(t, "oneway-1")

	if _, err := node1.rem.Call(ctx, id, 0); err != nil { // 归属 node1
		t.Fatalf("local activation call: %v", err)
	}
	select { // 排空激活调用的入队（值为 0），后续断言才不被挤位
	case v := <-got:
		if v != 0 {
			t.Fatalf("activation call delivered %v, want 0", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("local Handle did not run")
	}
	if err := node2.rem.SendOneWay(ctx, id, 99); err != nil {
		t.Fatalf("remote one-way send: %v", err)
	}
	select {
	case v := <-got:
		if v != 99 {
			t.Fatalf("remote one-way delivered %v, want 99", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remote one-way message was not delivered")
	}

	if err := node1.rem.SendOneWay(ctx, id, 55); err != nil {
		t.Fatalf("local one-way send: %v", err)
	}
	select {
	case v := <-got:
		if v != 55 {
			t.Fatalf("local one-way delivered %v, want 55", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("local one-way message was not delivered")
	}
}

// TestRemoteStartFailureTearsDown listen 失败即终态：New 已起网关扫描
// goroutine，Start 的 listen 失败路径必须收口——否则消费方弃置对象时
// 扫描器与 runCtx 永不终止（每实例恒漏 1 goroutine）。Start 失败走
// Close 收口（组合体终态，重建重试）。
func TestRemoteStartFailureTearsDown(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	mgr := activation.New(dir, 9, map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity { return &testEntity{} },
	})
	rem, err := New(Config{
		NodeID:      9,
		ClusterName: "loop-cluster",
		Addr:        "256.0.0.1:0", // 不可绑定地址：listen 必败
		Secret:      []byte("loop-secret"),
		Dir:         dir,
		Manager:     mgr,
		Resolve:     rt.resolve,
	})
	if err != nil {
		t.Fatalf("new remote: %v", err)
	}
	if err := rem.Start(context.Background()); err == nil {
		t.Fatal("start on unbindable addr must fail")
	}
	// 网关扫描器必须随失败收口退出（sweepDone 关闭）。
	select {
	case <-rem.gw.sweepDone:
	case <-time.After(2 * time.Second):
		t.Fatal("gateway sweeper goroutine leaked after Start failure")
	}
	// runCtx 收口（幂等 Stop 无害）。
	_ = rem.Close()
	if err := rem.runCtx.Err(); err == nil {
		t.Fatal("runCtx must be cancelled after Start failure teardown")
	}
}
