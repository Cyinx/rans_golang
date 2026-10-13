package node

// redis 目录形态的接线与行为回归（自 host/redis_test.go 迁入，断言不弱化）：
// 心跳/成员视图收敛、kind 感知放置（未承载目标 kind 的节点永不被放置选中）、
// 手工装配等价性。目录经 DirectoryConfig.Injected 注入
// （测试持有——node 自建形态的关闭行为由 Shutdown 生命周期覆盖，观察面不
// 依赖私有把手）。

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/redis"
	mgr "github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/lifecycle"
	"github.com/cyinx/rans_golang/internal/membership"
	"github.com/cyinx/rans_golang/internal/placement"
	"github.com/cyinx/rans_golang/internal/routing"
	"github.com/cyinx/rans_golang/internal/rpc"
	goredis "github.com/redis/go-redis/v9"
)

// redis 门禁（对齐 directory/redis/redis_test.go 约定）：RANS_TEST_REDIS_ADDR
// 优先，缺省 127.0.0.1:16379；不可达 skip（make test 自动拉起 docker redis）。
func redisGate(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("RANS_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:16379"
	}
	probe := redis.New(redis.Options{Addr: addr, Password: os.Getenv("RANS_TEST_REDIS_PASSWORD"), KeyPrefix: "rans:node:probe:"})
	defer probe.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := probe.NodeIDs(ctx); err != nil {
		t.Skipf("redis unavailable at %s — run make test-redis-up: %v", addr, err)
	}
	return addr
}

// sweepTestKeys 登记测试残键清扫：t.Cleanup 里 SCAN+DEL 本
// KeyPrefix 前缀下全部键——设计性无 TTL 的键族（{reg}:epoch 代际 Hash、
// {reg}:nodes 注册总表）与未到期的 TTL 键随测试结束物理清除，共享 Redis
// 实例不随测试轮次累积。仅测试侧使用：生产 Close 只关客户端绝不清键
// （红线）。在 nodeRedisOptions（夹具铸造点）登记——早于各测试的节点停机
// defer/Cleanup，LIFO 保证清扫最后执行，届时本前缀已无写入方。
func sweepTestKeys(t *testing.T, addr, prefix string) {
	t.Helper()
	cli := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: []string{addr}, Password: os.Getenv("RANS_TEST_REDIS_PASSWORD")})
	t.Cleanup(func() {
		defer cli.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		var cursor uint64
		for {
			keys, next, err := cli.Scan(ctx, cursor, prefix+"*", 500).Result()
			if err != nil {
				t.Errorf("sweep test keys (prefix %s): scan: %v", prefix, err)
				return
			}
			if len(keys) > 0 {
				if err := cli.Del(ctx, keys...).Err(); err != nil {
					t.Errorf("sweep test keys (prefix %s): del %d keys: %v", prefix, len(keys), err)
					return
				}
			}
			cursor = next
			if cursor == 0 {
				return
			}
		}
	})
}

// nodeRedisOptions 测试共享目录参数（同 KeyPrefix 即同逻辑目录——本文件
// 迁移后统一注入单一共享实例，前缀唯一性只防 -count=N 重跑串场）。
func nodeRedisOptions(t *testing.T) redis.Options {
	t.Helper()
	prefix := "rans:node:test:" + t.Name() + ":"
	sweepTestKeys(t, redisGate(t), prefix) // 残键清扫，LIFO 最后执行
	return redis.Options{
		Addr:         redisGate(t),
		Password:     os.Getenv("RANS_TEST_REDIS_PASSWORD"),
		HeartbeatTTL: 8 * time.Second,
		KeyPrefix:    prefix,
	}
}

// localPlacement 恒不放置（首激活本地收口）——「确定性本地激活」的测试
// 形态（spec §5.4：Pick 返回 false → 本节点声明承载该 kind 就本地激活）。
type localPlacement struct{}

func (localPlacement) Pick(string, rans.NodeID, []rans.NodeID) (rans.NodeID, bool) { return 0, false }

// mkRedisNode 注入共享目录的标准节点（1s 心跳、5s 请求超时，Start 后返回）。
func mkRedisNode(t *testing.T, dir directory.Directory, id rans.NodeID, mut func(*Config)) *Node {
	t.Helper()
	cfg := Config{
		NodeID: id, ClusterName: "node-test", Security: SecurityConfig{Insecure: true},
		Directory: DirectoryConfig{Injected: dir},
		Kinds:     testKinds(),
		Lease:     LeaseConfig{Interval: time.Second},
		Calls:     CallConfig{Default: 5 * time.Second},
	}
	if mut != nil {
		mut(&cfg)
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatalf("New node %d: %v", id, err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("Start node %d: %v", id, err)
	}
	return n
}

// shutdownReverse 多节点收口助手（倒序停——后起先停，对齐既有 defer 形态）。
func shutdownReverse(nodes ...*Node) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := len(nodes) - 1; i >= 0; i-- {
		_ = nodes[i].Shutdown(ctx)
	}
}

func TestRedisFullWiring(t *testing.T) {
	dir := redis.New(nodeRedisOptions(t))
	defer dir.Close()
	n1, n2 := mkRedisNode(t, dir, 1, nil), mkRedisNode(t, dir, 2, nil)
	defer shutdownReverse(n1, n2)

	// 成员视图收敛到双节点（节点键启动即写；注入目录测试持有——Shutdown
	// 后仍可读，无「自建目录先关」的观察窗问题）。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if !pollUntil(t, ctx, 5*time.Second, func() bool {
		ns, err := dir.NodeIDs(ctx)
		return err == nil && len(ns) == 2
	}) {
		t.Fatal("membership view did not converge to 2 nodes")
	}

	// 跨节点 Call（真实路径——放置排除自身，首调 miss 落对端）：node1 首调
	// 经放置远发 node2（实体在对端激活），node2 后续调用经目录命中本地执行。
	id := mustID(t, "counter", "redis-1")
	if v, err := n1.Client().Call(ctx, id, 1); err != nil || v != 1 {
		t.Fatalf("node1 cross call (placement landed on peer): v=%v err=%v", v, err)
	}
	if v, err := n2.Client().Call(ctx, id, 2); err != nil || v != 2 {
		t.Fatalf("node2 local call (owner after placement): v=%v err=%v", v, err)
	}

	// 心跳接线可观察：实体在 Track 集（经激活计数侧证——激活成功且无自围
	// 止损销毁即间接成立；直接断言面私有，不越界）。
	// 优雅停机后节点即时退视图（Drain 目录注销顺带清节点成员键）。
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	if err := n1.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown n1: %v", err)
	}
	if !pollUntil(t, ctx, 5*time.Second, func() bool {
		ns, err := dir.NodeIDs(ctx)
		if err != nil {
			return false
		}
		for _, n := range ns {
			if n == 1 {
				return false
			}
		}
		return true
	}) {
		t.Fatal("node 1 must leave membership view after graceful shutdown")
	}
}

// TestEquivalenceWithManualQUICKSTART node.New 装配与 README QUICKSTART
// 手工装配的等价性（行为断言两侧逐一成立，装配面不许有暗差异）。
func TestEquivalenceWithManualQUICKSTART(t *testing.T) {
	base := nodeRedisOptions(t)

	// 手工集群（KeyPrefix 独立）。放置共享一个实例：数据源 = 独立目录句柄
	//（View 只要 NodesFunc；shutdown 一并 Close）。
	manualOpts := base
	manualOpts.KeyPrefix += "manual:"
	viewDir := redis.New(manualOpts) // 只为放置视图提供数据源
	defer viewDir.Close()
	policy := placement.NewRandomPlacement(viewDir.NodeIDs)
	m1 := newManualNode(t, manualOpts, policy, 1, testKinds())
	m2 := newManualNode(t, manualOpts, policy, 2, testKinds())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m2.shutdown(ctx)
		m1.shutdown(ctx)
	}()

	// node 集群（KeyPrefix 独立，注入共享目录）。
	nodeOpts := base
	nodeOpts.KeyPrefix += "node:"
	nDir := redis.New(nodeOpts)
	defer nDir.Close()
	mkNode := func(id rans.NodeID) *Node {
		return mkRedisNode(t, nDir, id, nil)
	}
	n1, n2 := mkNode(1), mkNode(2)
	defer shutdownReverse(n1, n2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 行为断言两侧逐一成立：① 首调 miss 放置落对端（放置排除自身——node1
	// 首调经放置远发 node2，node2 后续调用本地执行）；② 成员视图双节点。
	assertRoundTrip := func(name string, view *redis.Directory,
		call1, call2 func(context.Context, rans.ID, int) (any, error)) {
		t.Helper()
		id := mustID(t, "counter", "equiv-"+name)
		if v, err := call1(ctx, id, 1); err != nil || v != 1 {
			t.Fatalf("[%s] node1 cross call (placement landed on peer): v=%v err=%v", name, v, err)
		}
		if v, err := call2(ctx, id, 2); err != nil || v != 2 {
			t.Fatalf("[%s] node2 local call (owner after placement): v=%v err=%v", name, v, err)
		}
		if !pollUntil(t, ctx, 5*time.Second, func() bool {
			ns, err := view.NodeIDs(ctx)
			return err == nil && len(ns) == 2
		}) {
			t.Fatalf("[%s] membership view != 2 nodes", name)
		}
	}
	assertRoundTrip("manual", m1.dir,
		func(ctx context.Context, id rans.ID, n int) (any, error) { return m1.rem.Call(ctx, id, n) },
		func(ctx context.Context, id rans.ID, n int) (any, error) { return m2.rem.Call(ctx, id, n) })
	assertRoundTrip("node", nDir,
		func(ctx context.Context, id rans.ID, n int) (any, error) { return n1.Client().Call(ctx, id, n) },
		func(ctx context.Context, id rans.ID, n int) (any, error) { return n2.Client().Call(ctx, id, n) })
}

// manualNode QUICKSTART 手工装配的最小复刻（README 代码块的测试内形态）：
// 与 node.New+Start+Shutdown 同参数对照，行为断言两侧一致。
type manualNode struct {
	dir    *redis.Directory
	mgr    *mgr.Table
	lv     *membership.Liveness // 节点存活信号（成员键+自围栏）
	rem    *rpc.Remote
	cancel context.CancelFunc
}

func newManualNode(t *testing.T, opts redis.Options, policy rpc.Placement, id rans.NodeID,
	kinds map[string]rans.Factory) *manualNode {
	t.Helper()
	dir := redis.New(opts)
	cache := routing.NewLocationCache(0, 0, 0)
	m := mgr.New(dir, id, kinds, mgr.Options{
		OnEvicted:         func(fid rans.ID) { cache.Invalidate(fid) },
		OnArbitrationLost: func(fid rans.ID, winner directory.Owner) { cache.PutPositive(fid, winner) },
		OnMigrated:        func(fid rans.ID, to rans.NodeID) { cache.PutPositive(fid, directory.Owner{Node: to}) },
	})
	// 节点存活（手工装配形态；续租面已随 H13 拆除——自围栏兜底接管）。
	lv, lvErr := membership.NewLiveness(membership.LivenessConfig{
		Reg: dir, Self: id, Interval: time.Second,
		Fence:    m.Fence,
		LocalIDs: m.LocalIDs,
		OnSelfFenceLost: func(ctx context.Context, fid rans.ID) error {
			if o, ok := m.InstanceOwner(fid); ok {
				return lifecycle.DestroyInstance(ctx, m, fid, o.Incarnation)
			}
			return nil
		},
	})
	if lvErr != nil {
		t.Fatalf("manual node %d liveness: %v", id, lvErr)
	}
	rem, err := rpc.New(rpc.Config{
		NodeID: id, ClusterName: "equiv-test", Addr: "127.0.0.1:0", NoHMAC: true,
		Dir: dir, Manager: m,
		Resolve: dir.NodeAddrResolver(), DefaultCallTimeout: 5 * time.Second,
		Placement: policy, LocationCache: cache,
	})
	if err != nil {
		t.Fatalf("manual node %d: %v", id, err)
	}
	if err := rem.Start(context.Background()); err != nil {
		t.Fatalf("manual start %d: %v", id, err)
	}
	lv.PublishSelf(rem.Addr(), nil)
	runCtx, cancel := context.WithCancel(context.Background())
	if err := lv.Start(runCtx); err != nil {
		t.Fatalf("start liveness %d: %v", id, err)
	}
	m.BindSelfEpoch(lv.NodeEpoch)
	return &manualNode{dir: dir, mgr: m, lv: lv, rem: rem, cancel: cancel}
}

func (n *manualNode) shutdown(ctx context.Context) {
	n.cancel()
	_ = n.lv.Shutdown(ctx)
	_ = n.rem.Shutdown(ctx)
	_ = n.dir.Close()
}

func pollUntil(t *testing.T, ctx context.Context, d time.Duration, check func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if check() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(20 * time.Millisecond):
		}
	}
	return false
}

// mkKindNode kind 感知放置三节点形态的标准节点：kinds 即本节点承载的
// 工厂表（成员键 kinds = 工厂表键集——放置过滤与存活视图同源）。
func mkKindNode(t *testing.T, dir directory.Directory, id rans.NodeID, kinds map[string]rans.Factory) *Node {
	t.Helper()
	cfg := Config{
		NodeID: id, ClusterName: "node-kind", Security: SecurityConfig{Insecure: true},
		Directory: DirectoryConfig{Injected: dir},
		Lease:     LeaseConfig{Interval: time.Second},
		Calls:     CallConfig{Default: 5 * time.Second},
		Kinds:     kinds,
	}
	return mkRedisNode(t, dir, id, func(c *Config) { *c = cfg })
}

// sortedNodes 排序副本（视图断言的归一形态）。
func sortedNodes(in []rans.NodeID) []rans.NodeID {
	out := append([]rans.NodeID(nil), in...)
	slices.Sort(out)
	return out
}

// greetKinds 不承载 counter 的工厂表（greet 实体复用 testCounter——只为
// 成员键携带非 counter kind，供放置过滤排除验证）。
func greetKinds() map[string]rans.Factory {
	return map[string]rans.Factory{
		"greet": func(_ rans.ID) rans.Entity { return &testCounter{} },
	}
}

// TestKindAwarePlacementNonHostNeverPicked kind 感知放置主锚（redis 门
// 禁，钉死「未承载目标 kind 的节点不被放置选中」）：三节点共享 redis
// ——n1/n2 承载 counter、n3 只承载 greet。成员视图直接锚定形态
// （NodesByKind(counter) 只含 n1/n2、greet 视图三节点中含 n2/n3——n3 注册
// 在册、存活）；此后从三个节点各自激活多个 counter 新鲜身份（多次、跨
// 目标），每次经目录归属断言 owner≠3——kind 感知放置（缺省随机×kind
// 过滤）永不把业务实体放到未承载节点。
func TestKindAwarePlacementNonHostNeverPicked(t *testing.T) {
	dir := redis.New(nodeRedisOptions(t))
	defer dir.Close()
	n1 := mkKindNode(t, dir, 1, testKinds())
	n2Kinds := testKinds()
	for k, f := range greetKinds() {
		n2Kinds[k] = f
	}
	n2 := mkKindNode(t, dir, 2, n2Kinds)
	n3 := mkKindNode(t, dir, 3, greetKinds())
	defer shutdownReverse(n1, n2, n3)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 成员视图收敛（启动即写 + 1s 心跳）：全集三节点；counter 视图只含
	// 承载节点（n3 不在其中）；greet 视图含 n2/n3（n3 注册在册、存活
	// ——counter 视图排除它是放置过滤的结果，不是节点缺席）。
	if !pollUntil(t, ctx, 10*time.Second, func() bool {
		ns, err := dir.NodeIDs(ctx)
		return err == nil && len(ns) == 3
	}) {
		t.Fatal("membership view did not converge to 3 nodes")
	}
	if got, err := dir.NodesByKind(ctx, "counter"); err != nil ||
		!slices.Equal(sortedNodes(got), []rans.NodeID{1, 2}) {
		t.Fatalf("NodesByKind(counter) = %v (%v), want [1 2] (non-host node 3 excluded)", got, err)
	}
	if got, err := dir.NodesByKind(ctx, "greet"); err != nil ||
		!slices.Equal(sortedNodes(got), []rans.NodeID{2, 3}) {
		t.Fatalf("NodesByKind(greet) = %v (%v), want [2 3] (n2 and n3 host it)", got, err)
	}

	// 放置轮：三个源节点 × 各 5 个新鲜身份。n1 排除自身后唯一候选是 2
	//（确定性）；n2 对称必落 1；n3 不承载 counter 不自排（exclude=3），候选
	// {1,2} 随机——三者共同的不变量：owner ∈ {1,2}，永不 3。
	type src struct {
		n    *Node
		want func(owner rans.NodeID) bool
	}
	for round, s := range []src{
		{n1, func(o rans.NodeID) bool { return o == 2 }},
		{n2, func(o rans.NodeID) bool { return o == 1 }},
		{n3, func(o rans.NodeID) bool { return o == 1 || o == 2 }},
	} {
		for i := 0; i < 5; i++ {
			id := mustID(t, "counter", fmt.Sprintf("kind-place-r%d-%d", round, i))
			if _, err := s.n.Client().Call(ctx, id, 1); err != nil {
				t.Fatalf("round %d activation call %d: %v", round, i, err)
			}
			loc, found, err := dir.Lookup(ctx, id)
			if err != nil || !found {
				t.Fatalf("lookup %s after activation: found=%v err=%v", id.String(), found, err)
			}
			if loc.Node == 3 || !s.want(loc.Node) {
				t.Fatalf("round %d id %d: directory owner = %d, want counter host (non-host node 3 must never own a counter entity)", round, i, loc.Node)
			}
		}
	}

	// 非承载节点零激活残留：n3 本地表空（greet 从未被调用、counter 激活
	// 全部路由到了承载节点）。
	if got := len(n3.LocalIDs()); got != 0 {
		t.Fatalf("non-host node holds %d local entities, want 0", got)
	}
}
