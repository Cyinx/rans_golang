package rpc

// 重试环接线测试（规格 §9）：Call/SendOneWay 走重试环 + 位置缓存、五失效
// 钩子、快速失败主干、supersede-close 变体、本地激活失败
// 重试一次、Shutdown 关停网关扫描器。回环双节点为主，NotFound 重试用真
// tcp.Manager 伪节点（恒回 StatusNotFound）+ 可翻转目录（首查陈旧→重查
// 命中）编排；时序敏感断言一律走 channel/预算轮询，不用裸 sleep。

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/lifecycle"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/routing"
	"github.com/cyinx/rans_golang/internal/transport/tcp"
	"github.com/cyinx/rans_golang/internal/wire"
)

// ---- 测试替身与助手 ----

// flippingDir 首查陈旧、重查命中的假目录（NotFound 重试编排）：第 1 次
// Lookup 返回 first（指向恒回 NotFound 的伪节点），其后返回 second（目录
// 已更新的新归属）。其余方法透传内嵌目录。
type flippingDir struct {
	directory.Directory
	mu      sync.Mutex
	lookups int
}

func (d *flippingDir) Lookup(_ context.Context, _ rans.ID) (directory.Owner, bool, error) {
	d.mu.Lock()
	d.lookups++
	n := d.lookups
	d.mu.Unlock()
	if n == 1 {
		return directory.Owner{Node: 3}, true, nil
	}
	return directory.Owner{Node: 1}, true, nil
}

// notFoundPeer 伪节点 3：真 tcp.Manager + 恒回 StatusNotFound 的应用层
// handler（回程按 Source 寻址经本管理器拨回，与真节点同口径）。
type notFoundPeer struct {
	m       *tcp.Manager
	resolve func(ctx context.Context, n rans.NodeID) (string, error)
	pool    *wire.PagePool
	addr    string
	reqs    atomic.Int32
}

// HandleMessage 收到 Request 即回 NotFound+StaleLocation 结构化 Error 帧
// （位置陈旧语义；reason 显式过线，线上判定以帧为准）。
func (p *notFoundPeer) HandleMessage(hdr wire.Header, body []byte) {
	if body != nil {
		p.pool.Put(body)
	}
	if hdr.Type != protocol.MsgRequest {
		return
	}
	p.reqs.Add(1)
	rh := wire.Header{
		Type:         protocol.MsgError,
		Flags:        protocol.FlagHasStatus | protocol.FlagHasSourceNodeID,
		MessageID:    hdr.MessageID,
		Status:       rans.CodeNotFound,
		SourceNodeID: 3,
	}
	_ = p.m.SendToNode(context.Background(), rans.NodeID(hdr.SourceNodeID), p.resolve, rh,
		wire.MarshalError(rans.NewError(rans.CodeNotFound, rans.ReasonStaleLocation,
			"stale location: entity moved")))
}

func (p *notFoundPeer) OnHandshakeComplete(rans.NodeID) {}
func (p *notFoundPeer) OnConnectionLost(rans.NodeID)    {}

// startNotFoundPeer 拉起伪节点 listener，返回绑定地址（Secret/指纹与
// startRemNode 缺省一致，握手可成）。
func startNotFoundPeer(t *testing.T, rt *resolveTable) *notFoundPeer {
	t.Helper()
	pool := wire.NewPagePool()
	peer := &notFoundPeer{resolve: rt.resolve, pool: pool}
	peer.m = tcp.NewManager(tcp.Config{
		NodeID:       3,
		ClusterName:  "loop-cluster",
		Secret:       []byte("loop-secret"),
		SchemaDigest: 7,
	}, peer, pool)
	addr, err := peer.m.Listen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("notfound peer listen: %v", err)
	}
	t.Cleanup(peer.m.Close)
	peer.addr = addr
	return peer
}

// flakyActivateEntity Activate 首次失败、其后成功的包装实体（本地
// ErrActivationFailed 重试一次的编排锚点）。
type flakyActivateEntity struct {
	inner rans.Entity
	fails *atomic.Int32
}

func (e *flakyActivateEntity) Activate(ctx context.Context) error {
	if e.fails.Add(1) == 1 {
		return errors.New("rans: activate: transient activation failure")
	}
	return e.inner.Activate(ctx)
}

func (e *flakyActivateEntity) Deactivate(ctx context.Context) error { return e.inner.Deactivate(ctx) }
func (e *flakyActivateEntity) Handle(ctx context.Context, req any) (any, error) {
	return e.inner.Handle(ctx, req)
}

// waitForCacheMiss 预算内轮询缓存条目消失（钩子异步触发的确定性收口）。
func waitForCacheMiss(t *testing.T, c *routing.LocationCache, id rans.ID, budget time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if _, found, _ := c.Get(id); !found {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// waitForLocalMiss 预算内轮询本地注册表条目消失（激活失败收场的观测面）。
func waitForLocalMiss(t *testing.T, m *activation.Table, id rans.ID, budget time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if _, ok := m.GetLocal(id); !ok {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// fastHandle 快速应答的缺省 handler。
func fastHandle(_ context.Context, _ any) (any, error) { return "ok", nil }

// ---- 1. NotFound 重试：缓存失效 + 目录重查 → 新归属成功 ----

// TestCallNotFoundRetryRequeriesDirectory：node2 的目录首查陈旧（指向恒回
// NotFound 的伪节点 3）→ 首次 attempt 得 *rans.Error{NotFound} → 接线层失效
// 缓存条目 → 环重试重走路由梯子 → 目录重查命中 node1 → 成功。伪节点恰收
// 到 1 个请求、重路由后实体落在 node1。
func TestCallNotFoundRetryRequeriesDirectory(t *testing.T) {
	rt := &resolveTable{}
	peer := startNotFoundPeer(t, rt)
	cache := routing.NewLocationCache(0, 0, 0)
	node1 := startRemNode(t, inmem.New(), rt, nodeSpec{
		id:     1,
		handle: func(_ context.Context, _ any) (any, error) { return "at-new-owner", nil },
	})
	node2 := startRemNode(t, &flippingDir{Directory: inmem.New()}, rt, nodeSpec{
		id:     2,
		cache:  cache,
		handle: fastHandle,
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 3: peer.addr})
	id := mustUUID(t, "notfound-1")

	start := time.Now()
	v, err := node2.rem.CallOpts(context.Background(), id, 0, routing.DefaultOptions())
	if err != nil {
		t.Fatalf("NotFound must be retried through directory re-query: %v", err)
	}
	if v != "at-new-owner" {
		t.Fatalf("retried call returned %v, want at-new-owner", v)
	}
	if n := peer.reqs.Load(); n != 1 {
		t.Fatalf("stale owner must see exactly 1 request, saw %d", n)
	}
	if n := node1.led.count(); n != 1 {
		t.Fatalf("entity must activate exactly once on new owner, activations=%d", n)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("retry must settle well under gateway timeout, took %v", elapsed)
	}
}

// ---- 2/3. 模糊超时：非幂等即抛 / 幂等重试成功 ----

// TestCallAmbiguousTimeoutNonIdempotentRaises：慢实体（首请求挂 1.2s）+
// 400ms 网关超时 → *rans.Error{Timeout}（模糊投递）→ 未标注幂等 → 直接上抛，
// 且在慢实体收场前返回。
func TestCallAmbiguousTimeoutNonIdempotentRaises(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	var calls atomic.Int32
	arrive := make(chan struct{}, 4)
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id: 1,
		handle: func(_ context.Context, _ any) (any, error) {
			if calls.Add(1) == 1 {
				arrive <- struct{}{}
				// 慢实体：故障注入（时长 > 超时+扫描周期，保证首请求必超时）
				time.Sleep(1200 * time.Millisecond)
				return "slow", nil
			}
			return "second", nil
		},
	})
	node2 := startRemNode(t, dir, rt, nodeSpec{
		id:      2,
		cache:   routing.NewLocationCache(0, 0, 0),
		timeout: 400 * time.Millisecond,
		handle:  fastHandle,
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	id := mustUUID(t, "amb-1")
	// 预注册归属 node1：node2 的调用必走远程路径
	if _, _, err := dir.Register(context.Background(), id, directory.Owner{Node: 1}, nil); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	_, err := node2.rem.Call(context.Background(), id, 0)
	if !errors.Is(err, rans.ErrTimeout) {
		t.Fatalf("non-idempotent ambiguous timeout must raise *rans.Error{Timeout}, got %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 1100*time.Millisecond {
		t.Fatalf("must raise before the slow entity settles, took %v", elapsed)
	}
}

// TestCallOptsIdempotentRetriesAmbiguousTimeout：同上场景 + Idempotent →
// 环重试（缓存失效 + 重路由）→ 第二次请求在慢实体收场后执行 → 成功。
func TestCallOptsIdempotentRetriesAmbiguousTimeout(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	var calls atomic.Int32
	arrive := make(chan struct{}, 4)
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id: 1,
		handle: func(_ context.Context, _ any) (any, error) {
			if calls.Add(1) == 1 {
				arrive <- struct{}{}
				time.Sleep(1200 * time.Millisecond) // 慢实体：首请求必超时
				return "slow", nil
			}
			return "second", nil
		},
	})
	node2 := startRemNode(t, dir, rt, nodeSpec{
		id:      2,
		cache:   routing.NewLocationCache(0, 0, 0),
		timeout: 400 * time.Millisecond,
		handle:  fastHandle,
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	id := mustUUID(t, "amb-2")
	if _, _, err := dir.Register(context.Background(), id, directory.Owner{Node: 1}, nil); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	v, err := node2.rem.CallOpts(context.Background(), id, 0, routing.InvokeOptions{Idempotent: true})
	if err != nil {
		t.Fatalf("idempotent call must retry through ambiguous timeout: %v", err)
	}
	if v != "second" {
		t.Fatalf("retried call returned %v, want second", v)
	}
	if n := calls.Load(); n < 2 || n > 3 {
		t.Fatalf("entity must execute 2-3 times (first timed out, retry landed; ring caps at 3 attempts), executions=%d", n)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("idempotent retry must settle promptly, took %v", elapsed)
	}
}

// ---- 4. 钩子 1：本地销毁 → Invalidate ----

// TestHookLocalDestroyInvalidatesCache：本地激活（目录 miss → 本地）后缓存
// 持负条目（miss 回填，防穿透——实体归本端，负条目同样路由本地）；Destroy →
// manager OnEvicted 钩子（rpc.New 接线）失效条目；再调用走全新激活。
func TestHookLocalDestroyInvalidatesCache(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	cache := routing.NewLocationCache(0, 0, 0)
	node := startRemNode(t, dir, rt, nodeSpec{id: 1, cache: cache, handle: fastHandle})
	ctx := context.Background()
	id := mustUUID(t, "evict-1")

	if _, err := node.rem.Call(ctx, id, 0); err != nil {
		t.Fatalf("local activation call: %v", err)
	}
	if _, found, _ := cache.Get(id); !found {
		t.Fatal("cache must hold an entry after the first call (negative, from directory miss)")
	}

	lifecycle.DestroyLocal(ctx, node.mgr, id)
	if !waitForCacheMiss(t, cache, id, 2*time.Second) {
		t.Fatal("OnEvicted hook must invalidate the cache entry on destroy")
	}

	if _, err := node.rem.Call(ctx, id, 0); err != nil {
		t.Fatalf("call after destroy+invalidate: %v", err)
	}
	if n := node.led.count(); n != 2 {
		t.Fatalf("entity must reactivate after invalidation, activations=%d", n)
	}
}

// ---- 5. 钩子 2：ChangePoller Unregistered → Invalidate（testkit 接线示范） ----

func TestHookArbitrationLostPutsWinner(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	cache1 := routing.NewLocationCache(0, 0, 0)
	node1 := startRemNode(t, dir, rt, nodeSpec{id: 1, cache: cache1, handle: fastHandle})
	node2 := startRemNode(t, dir, rt, nodeSpec{
		id:     2,
		handle: func(_ context.Context, _ any) (any, error) { return "winner-node", nil },
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	ctx := context.Background()
	id := mustUUID(t, "cas-1")
	// 预注册归属 node2：node1 的激活必在输家分支落败
	if _, _, err := dir.Register(ctx, id, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatal(err)
	}

	// 激活在途时 GetOrActivate 异步返回（err 可为 nil）——同步/异步两种
	// 收场都以本地表 miss 为终局（输家 pawnite 必销毁清理）
	_, _ = node1.mgr.GetOrActivate(ctx, id)
	if !waitForLocalMiss(t, node1.mgr, id, 2*time.Second) {
		t.Fatal("losing activation must settle to eviction")
	}

	// 输家清理次序（manager cleanupOnce）：先 evict（本地表消失——上方
	// waitForLocalMiss 的观测面）、后 fireOnArbitrationLost——本地 miss 先于
	// 赢家条目落缓存，紧随其后的立即 Get 会竞态。有界轮询直到
	// 正条目出现（testkit 孪生场景 TestAcceptance_LocationCacheHooks 的
	// 同款收口）。
	deadline := time.Now().Add(2 * time.Second)
	for {
		loc, found, positive := cache1.Get(id)
		if found && positive && loc.Node == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("arbitration-lost hook must PutPositive winner node 2, got (loc=%+v found=%v positive=%v)",
				loc, found, positive)
		}
		time.Sleep(2 * time.Millisecond)
	}

	v, err := node1.rem.Call(ctx, id, 0)
	if err != nil {
		t.Fatalf("call after CAS loss must route to the winner: %v", err)
	}
	if v != "winner-node" {
		t.Fatalf("call must execute on winner node2, returned %v", v)
	}
	if n := node2.led.count(); n != 1 {
		t.Fatalf("winner must activate the entity once, activations=%d", n)
	}
}

// ---- 7. 钩子 4：发送失败 → Invalidate → 重试经重路由本地恢复 ----

// TestHookSendFailureInvalidatesCacheAndRecoversLocally：缓存陈旧条目指向
// 不可解析节点 7 → 发送重试耗尽（Unavailable+SendFailed）→ 接线层失效条目 → 环重试
// 重走路由梯子 → 目录 miss → 负缓存 → 本地激活成功。
func TestHookSendFailureInvalidatesCacheAndRecoversLocally(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{} // 故意不填表：节点 7 不可解析
	cache := routing.NewLocationCache(30*time.Second, 5*time.Second, 0)
	node := startRemNode(t, dir, rt, nodeSpec{id: 2, cache: cache, handle: fastHandle})
	node.rem.gw.retryInterval = 20 * time.Millisecond // 发送重试快速收场
	ctx := context.Background()
	id := mustUUID(t, "sendfail-1")
	cache.PutPositive(id, directory.Owner{Node: 7}) // 陈旧路由

	v, err := node.rem.Call(ctx, id, 0)
	if err != nil {
		t.Fatalf("ring must retry send failure and recover via re-route: %v", err)
	}
	if v != "ok" {
		t.Fatalf("recovered call returned %v, want ok", v)
	}
	if _, found, positive := cache.Get(id); !found || positive {
		t.Fatalf("re-route must leave a negative entry after directory miss (found=%v positive=%v)", found, positive)
	}
	if n := node.led.count(); n != 1 {
		t.Fatalf("entity must activate locally exactly once, activations=%d", n)
	}
}

// ---- 8. 钩子 5：节点下线（SeverNode）→ InvalidateNode + pending 快速失败 ----

// TestHookNodeLostSeverInvalidatesAndFastFails：在途请求挂于慢实体 → 本端
// SeverNode 关断连接 → OnConnectionLost 接线失效节点条目 + FailPendingForNode
// → pending 毫秒级收场（远小于 5s 请求超时）。
func TestHookNodeLostSeverInvalidatesAndFastFails(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	cache2 := routing.NewLocationCache(0, 0, 0)
	var n1calls atomic.Int32
	arrive := make(chan struct{}, 4)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id: 1,
		handle: func(_ context.Context, _ any) (any, error) {
			if n1calls.Add(1) == 1 {
				return "warm", nil
			}
			arrive <- struct{}{}
			select {
			case <-release:
				return "late", nil
			case <-time.After(10 * time.Second):
				return "late-timeout", nil
			}
		},
	})
	node2 := startRemNode(t, dir, rt, nodeSpec{id: 2, cache: cache2, handle: fastHandle})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	ctx := context.Background()
	id := mustUUID(t, "nodelost-1")
	// 预注册归属 node1（node2 必走远程）并暖连接/缓存
	if _, _, err := dir.Register(ctx, id, directory.Owner{Node: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if v, err := node2.rem.Call(ctx, id, 0); err != nil || v != "warm" {
		t.Fatalf("warm-up call: v=%v err=%v", v, err)
	}
	if _, found, positive := cache2.Get(id); !found || !positive {
		t.Fatal("cache must hold node1 entry after warm call")
	}

	type outcome struct {
		err error
		d   time.Duration
	}
	done := make(chan outcome, 1)
	go func() {
		start := time.Now()
		_, err := node2.rem.Call(ctx, id, 0) // 非幂等：快速失败即上抛
		done <- outcome{err: err, d: time.Since(start)}
	}()
	<-arrive // 请求已挂起（pending 在途）

	t0 := time.Now()
	node2.rem.conns.SeverNode(1)
	select {
	case r := <-done:
		if r.err == nil || !errors.Is(r.err, rans.ErrConnectionLost) {
			t.Fatalf("pending must fast-fail with *rans.Error{ConnectionLost}, got %v", r.err)
		}
		if d := time.Since(t0); d > 2*time.Second {
			t.Fatalf("fast-fail must beat the 5s request timeout, took %v", d)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("pending call did not settle after node lost")
	}
	if _, found, _ := cache2.Get(id); found {
		t.Fatal("InvalidateNode must drop node1 entries on connection loss")
	}
}

// ---- 9. 快速失败时序：kill 节点 → pending 毫秒级失败（非等满超时） ----

// TestKillNodeFastFailsPending：对端停机（Stop）→ 本端连接 FIN → connLost →
// 快速失败主干把 pending 即刻摊派失败。
func TestKillNodeFastFailsPending(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	arrive := make(chan struct{}, 4)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id: 1,
		handle: func(_ context.Context, _ any) (any, error) {
			arrive <- struct{}{}
			select {
			case <-release:
				return "late", nil
			case <-time.After(10 * time.Second):
				return "late-timeout", nil
			}
		},
	})
	node2 := startRemNode(t, dir, rt, nodeSpec{id: 2, cache: routing.NewLocationCache(0, 0, 0), handle: fastHandle})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	ctx := context.Background()
	id := mustUUID(t, "kill-1")
	if _, _, err := dir.Register(ctx, id, directory.Owner{Node: 1}, nil); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := node2.rem.Call(ctx, id, 0)
		done <- err
	}()
	<-arrive

	t0 := time.Now()
	_ = node1.rem.Close() // kill：listener + 全部连接
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, rans.ErrConnectionLost) {
			t.Fatalf("pending must fast-fail with *rans.Error{ConnectionLost} after kill, got %v", err)
		}
		if d := time.Since(t0); d > 2500*time.Millisecond {
			t.Fatalf("fast-fail must be millisecond-scale vs the 5s timeout, took %v", d)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("pending call did not settle after kill")
	}
}

// ---- 10. supersede-close 变体：并发首拨无暖场 ----

// TestSupersedeCloseConcurrentFirstDialsNoHang：不预热直接对拨 + 并发首批
// 请求（同节点对双连接按顶替策略关旧）。顶替窗内两类在途失败：
//   - 写出错误 → Unavailable+SendFailed（确定未投递）→ 环即刻重试，骑上替代连接
//     成功——快速失败换快速恢复，不等满请求超时；
//   - 顶替收敛期的瞬态收场（superseded 连接的 Lost 按设计抑制——节点仍有
//     新连接，不触发快速失败）→ 按模糊类收场（缩短超时封顶），幂等重试经
//     替代连接恢复。
//
// 断言三件：每个调用有界收场且错误只属分类过的传输形态（无未分类错误/
// 无挂死）；总耗时远小于生产 30s 超时；风暴落定后双向调用即刻成功
// （收敛——连接表稳定、无残留坏路由）。
func TestSupersedeCloseConcurrentFirstDialsNoHang(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	short := 1 * time.Second // 缩短请求超时：残余丢帧窗的最坏代价=一个超时+重试
	node1 := startRemNode(t, dir, rt, nodeSpec{id: 1, timeout: short, handle: fastHandle})
	node2 := startRemNode(t, dir, rt, nodeSpec{id: 2, timeout: short, handle: fastHandle})
	node1.rem.gw.retryInterval = 20 * time.Millisecond // 发送重试快速收场
	node2.rem.gw.retryInterval = 20 * time.Millisecond
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	ctx := context.Background()

	const pairs = 4
	aIDs := make([]rans.ID, pairs) // 归属 node1（node2 调）
	bIDs := make([]rans.ID, pairs) // 归属 node2（node1 调）
	for i := 0; i < pairs; i++ {
		aIDs[i] = mustUUID(t, fmt.Sprintf("super-a-%d", i))
		bIDs[i] = mustUUID(t, fmt.Sprintf("super-b-%d", i))
		if _, _, err := dir.Register(ctx, aIDs[i], directory.Owner{Node: 1}, nil); err != nil {
			t.Fatal(err)
		}
		if _, _, err := dir.Register(ctx, bIDs[i], directory.Owner{Node: 2}, nil); err != nil {
			t.Fatal(err)
		}
	}

	start := make(chan struct{})
	results := make(chan error, 2*pairs)
	t0 := time.Now()
	for i := 0; i < pairs; i++ {
		go func(id rans.ID) {
			<-start
			_, err := node2.rem.CallOpts(ctx, id, 0, routing.InvokeOptions{Idempotent: true})
			results <- err
		}(aIDs[i])
		go func(id rans.ID) {
			<-start
			_, err := node1.rem.CallOpts(ctx, id, 0, routing.InvokeOptions{Idempotent: true})
			results <- err
		}(bIDs[i])
	}
	close(start)
	for i := 0; i < 2*pairs; i++ {
		select {
		case err := <-results:
			// 顶替风暴可能连续快速失败并耗尽环的 3 次尝试（对端侧的非
			// superseded Lost 逐次摊派在途请求）——允许失败，但只允许
			// 分类过的快速失败形态（Unavailable+SendFailed / ConnectionLost /
			// Timeout），绝不许未分类错误或挂死。
			if err != nil {
				ok := rans.CodeOf(err) == rans.CodeUnavailable && rans.ReasonOf(err) == rans.ReasonSendFailed ||
					errors.Is(err, rans.ErrConnectionLost) || errors.Is(err, rans.ErrTimeout)
				if !ok {
					t.Fatalf("first-dial collision must succeed or fail with classified transport errors, got %v", err)
				}
			}
		case <-time.After(6 * time.Second):
			t.Fatal("concurrent first-dials hung past bound (supersede-close must not wait out the production timeout)")
		}
	}
	// 总时长断言锚「无全超时挂死」（生产 30s 的量级对照）。
	if d := time.Since(t0); d > 5*time.Second {
		t.Fatalf("all first-dial calls must settle far under the production timeout, took %v", d)
	}
	// 收敛断言：风暴落定后双向调用即刻成功（连接表已稳定，无残留坏路由）。
	convIDa := mustUUID(t, "super-conv-a")
	convIDb := mustUUID(t, "super-conv-b")
	if _, _, err := dir.Register(ctx, convIDa, directory.Owner{Node: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dir.Register(ctx, convIDb, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := node2.rem.Call(ctx, convIDa, 0); err != nil {
		t.Fatalf("post-storm call node2→node1 must succeed: %v", err)
	}
	if _, err := node1.rem.Call(ctx, convIDb, 0); err != nil {
		t.Fatalf("post-storm call node1→node2 must succeed: %v", err)
	}
}

// ---- 11. 本地 ErrActivationFailed 重试一次 ----

// TestLocalActivationFailedRetriedOnce：Activate 首败（执行前确定性失败）→
// 接线层本地重试一次（re-GetOrActivate）→ 第二次激活成功、请求恰好执行
// 一次。
func TestLocalActivationFailedRetriedOnce(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	var actAttempts, handled atomic.Int32
	node := startRemNode(t, dir, rt, nodeSpec{
		id: 1,
		handle: func(_ context.Context, _ any) (any, error) {
			handled.Add(1)
			return "recovered", nil
		},
		wrapEntity: func(e rans.Entity) rans.Entity {
			return &flakyActivateEntity{inner: e, fails: &actAttempts}
		},
	})
	ctx := context.Background()
	id := mustUUID(t, "actfail-1")

	v, err := node.rem.Call(ctx, id, 0)
	if err != nil {
		t.Fatalf("local ErrActivationFailed must be retried once: %v", err)
	}
	if v != "recovered" {
		t.Fatalf("retried call returned %v, want recovered", v)
	}
	if n := actAttempts.Load(); n != 2 {
		t.Fatalf("Activate must run exactly twice (fail once + retry), ran %d", n)
	}
	if n := handled.Load(); n != 1 {
		t.Fatalf("request must execute exactly once, executions=%d", n)
	}
}

// ---- 12. Remote.Close 关停网关扫描器（无 goroutine 泄漏） ----

// TestRemoteStopClosesGatewaySweeper：Close 后网关 deadline 扫描器 goroutine
// 必须退出（sweepDone 关闭）。
func TestRemoteStopClosesGatewaySweeper(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	node := startRemNode(t, dir, rt, nodeSpec{id: 1, handle: fastHandle})
	_ = node.rem.Close()
	select {
	case <-node.rem.gw.sweepDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Remote.Stop must close the gateway sweeper goroutine")
	}
}

// ---- 13. SendOneWayOpts：Tell 腿入发件箱后的失败语义（H11 决策 11/12） ----

// TestSendOneWayOptsRetriesSendFailure：陈旧缓存指向不可解析节点 → Tell
// 入箱即返回（发送失败不进调用方——满箱之外的同步失败面只剩路由/预算），
// worker 重试耗尽弃置（发送失败计数）→ 端点判停回调失效该节点缓存
// （钩子⑤经发件箱发送面依旧生效）→ 后续 Tell 重路由本地投递成功。
func TestSendOneWayOptsRetriesSendFailure(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{} // 故意不填表：节点 7 不可解析
	cache := routing.NewLocationCache(30*time.Second, 5*time.Second, 0)
	got := make(chan any, 4)
	node := startRemNode(t, dir, rt, nodeSpec{
		id:    2,
		cache: cache,
		handle: func(_ context.Context, req any) (any, error) {
			got <- req
			return nil, nil
		},
	})
	node.rem.gw.retryInterval = 20 * time.Millisecond
	ctx := context.Background()
	id := mustUUID(t, "ow-1")
	cache.PutPositive(id, directory.Owner{Node: 7}) // 陈旧路由

	if err := node.rem.SendOneWayOpts(ctx, id, 77, routing.InvokeOptions{Idempotent: true}); err != nil {
		t.Fatalf("tell must return at enqueue (H11 outbox semantics): %v", err)
	}
	select {
	case v := <-got:
		t.Fatalf("stale-route tell must not deliver locally, got %v", v)
	case <-time.After(200 * time.Millisecond):
	}
	// 端点判停失效缓存收敛（worker 发送失败的异步面），随后重路由本地。
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, found, _ := cache.Get(id); !found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stale cache entry never invalidated after outbox send failure")
		}
		time.Sleep(time.Millisecond)
	}
	if err := node.rem.SendOneWayOpts(ctx, id, 78, routing.InvokeOptions{Idempotent: true}); err != nil {
		t.Fatalf("re-routed tell: %v", err)
	}
	select {
	case v := <-got:
		if v != 78 {
			t.Fatalf("local entity received %v, want 78", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("re-routed one-way was not delivered locally")
	}
}
