package testkit

// 三节点 redis 模式验收（ROADMAP 3 验收②③）。
//   - redis 真跑门禁：RANS_TEST_REDIS_ADDR（默认 127.0.0.1:16379）不可达即
//     skip（与 directory/redis 测试同口径，bare `go test ./...` 仍全绿）；
//   - 短租约（1.5s）短心跳（300ms），等待一律有界轮询，无承重 sleep；
//   - -race 稳定由 make test 保底，另以 -count=3 复验（KeyPrefix 含 UnixNano，
//     重跑间键隔离）。

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	goredis "github.com/redis/go-redis/v9"
)

// redisAddr 返回测试 Redis 地址：RANS_TEST_REDIS_ADDR 优先，默认 127.0.0.1:16379
// （Makefile test-redis-up 拉起同一容器；与 directory/redis 测试同口径）。
func redisAddr() string {
	if v := os.Getenv("RANS_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:16379"
}

// redisPassword 返回测试 Redis 密码：RANS_TEST_REDIS_PASSWORD（内网带认证
// Redis 直连模式用；docker 本地实例无密码，缺省为空）。
func redisPassword() string { return os.Getenv("RANS_TEST_REDIS_PASSWORD") }

// mustRedis 探活门禁：Redis 不可达即整测试 skip。
func mustRedis(t *testing.T) {
	t.Helper()
	cli := goredis.NewClient(&goredis.Options{Addr: redisAddr(), Password: redisPassword(), DialTimeout: 500 * time.Millisecond})
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable at %s — run make test-redis-up", redisAddr())
	}
}

// pollUntil 有界轮询：check 每 20ms 重试，超时即致命。异步收敛点（租约过期、
// 心跳触发、存活视图）全部经此等待——不用裸 sleep 对齐时序。
func pollUntil(t *testing.T, d time.Duration, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s did not converge within %s", what, d)
}

// 验收②：Kill 后租约自然过期 → 重激活成功且归属翻新。
//
// node2 激活实体并 Call（值 1）→ Kill(node2)（停 remote+停心跳，本地实例
// 成陈旧孤儿）→ 租约到期（目录 miss，有界轮询）→ node3 Call 同 id：miss →
// 本地重激活（计数从头——演示不接 checkpoint Store）→ 目录指向新 owner(≠2)；
// node1 再 Call：经目录路由到新 owner，计数连续——陈旧孤儿从未服务任何请求。
func TestAcceptance_ThreeNodeKillLeaseExpiryReactivation(t *testing.T) {
	mustRedis(t)
	const (
		leaseTTL   = 1500 * time.Millisecond
		hbInterval = 300 * time.Millisecond
	)
	c := NewCluster(t, 3, counterKinds(nil), WithRedis(redisAddr(), leaseTTL, hbInterval))
	n1, n2, n3 := c.Node(0), c.Node(1), c.Node(2)
	ctx := context.Background()
	dir := c.Directory()
	id := mustID(t, "counter", "kill-lease-react")

	v, err := n2.Call(ctx, id, 0)
	if err != nil || v != 1 {
		t.Fatalf("node2 activation call: v=%v err=%v", v, err)
	}

	c.Kill(1) // 停 remote + 停心跳 → 成员键过期（v3：条目常驻，不再靠租约腾位）
	reg, hasReg := dir.(directory.NodeRegistry)
	pollUntil(t, 10*time.Second, "membership eviction of killed node", func() bool {
		if !hasReg {
			return true
		}
		ns, err := reg.NodeIDs(ctx)
		if err != nil {
			return false
		}
		for _, n := range ns {
			if n == 2 {
				return false
			}
		}
		return true
	})

	// node3 重激活：新鲜身份语义（无 checkpoint，计数从 1 重新开始）。
	v, err = n3.Call(ctx, id, 0)
	if err != nil || v != 1 {
		t.Fatalf("node3 reactivation call: v=%v err=%v", v, err)
	}
	loc, found, err := dir.Lookup(ctx, id)
	if err != nil || !found {
		t.Fatalf("directory after reactivation: found=%v err=%v", found, err)
	}
	if loc.Node == 2 {
		t.Fatalf("directory still points at killed node2 (owner=%d)", loc.Node)
	}
	if loc.Node != 3 {
		t.Fatalf("miss→local reactivation must own on node3, got node %d", loc.Node)
	}

	// 陈旧孤儿不服务：node1 的调用经目录路由到新 owner，node3 计数连续到 2；
	// 若 node2 的陈旧实例仍在服务，返回值将来自它的计数器而破坏连续性。
	v, err = n1.Call(ctx, id, 0)
	if err != nil || v != 2 {
		t.Fatalf("call after reactivation must serve from new owner: v=%v err=%v (want 2)", v, err)
	}
}

// 验收③（租约自毁）：归属被外部窃取 → 心跳续租失败 → Fence 关准入 +
// OnLost 本地 Destroy → 再次 Call 经目录命中新 owner 远程成功。
//
// 窃取用目录原生操作直连完成（实例级 UnregisterIfOwner 按现归属标记 +
// Register 改归属 node3）：测试持有目录引用，等价于「别处抢注」的真实
// 外部竞态。H13 语义：外部窃取不再有续租失败检测面（续租已拆除）——
// 可用性断言收敛为「归属翻转后调用重路由到新 owner」；被窃取方的本地
// 陈旧实例不再承诺 5s 内自毁（其止损由死 owner 接管通知与闲置钝化承接，
// 正确性由纪元围栏保证）。
func TestAcceptance_ExternalStealReRoutes(t *testing.T) {
	mustRedis(t)
	const (
		leaseTTL   = 1500 * time.Millisecond
		hbInterval = 300 * time.Millisecond
	)
	c := NewCluster(t, 3, counterKinds(nil), WithRedis(redisAddr(), leaseTTL, hbInterval))
	n1, n2, _ := c.Node(0), c.Node(1), c.Node(2)
	ctx := context.Background()
	dir := c.Directory()
	id := mustID(t, "counter", "external-steal")

	v, err := n1.Call(ctx, id, 0)
	if err != nil || v != 1 {
		t.Fatalf("node1 activation call: v=%v err=%v", v, err)
	}

	// 外部窃取：node1 的条目改归属 node3（node3 尚无本地实例——纯目录层
	// 抢注）。
	cur, found, err := dir.Lookup(ctx, id)
	if err != nil || !found {
		t.Fatalf("lookup before steal: found=%v err=%v", found, err)
	}
	ok, err := dir.UnregisterIfOwner(ctx, id, cur)
	if err != nil || !ok {
		t.Fatalf("steal unregister: ok=%v err=%v", ok, err)
	}
	if won, _, err := dir.Register(ctx, id, directory.Owner{Node: 3}, nil); err != nil || !won {
		t.Fatalf("steal register to node3: won=%v err=%v", won, err)
	}

	// 重路由：node2 Call → 目录/有效性判定命中新 owner → node3 首次真实
	// 激活（epoch-0 注入形态收养）计数从 1 开始。
	v, err = n2.Call(ctx, id, 0)
	if err != nil || v != 1 {
		t.Fatalf("re-routed call to new owner: v=%v err=%v", v, err)
	}
	loc, found, err := dir.Lookup(ctx, id)
	if err != nil || !found || loc.Node != 3 {
		t.Fatalf("owner after re-route: loc=%v found=%v err=%v (want node3)", loc, found, err)
	}
}

// 放置分布：三节点 redis 模式 + 放置开启，并发激活多个新身份 →
// 实体承载在 ≥2 个不同节点。
//
// 播种：每节点先各自本地激活一个身份（直接 mgr.Call 绕过放置路由，确定性
// 落本节点）——心跳键只由首个心跳批落（注册不落），有界轮询存活视图等到
// 三节点齐备后才放行放置流量。
//
// 分布断言的结构保证（非概率）：调用方轮转，Pick 排除自身——来自 node k 的
// 放置永不落在 node k，而 9 个身份的调用方覆盖全部三节点，故「全部落在单一
// 节点」不可能成立。
func TestAcceptance_PlacementDistributes(t *testing.T) {
	mustRedis(t)
	const (
		leaseTTL   = 1500 * time.Millisecond
		hbInterval = 300 * time.Millisecond
	)
	c := NewCluster(t, 3, counterKinds(nil), WithRedis(redisAddr(), leaseTTL, hbInterval), WithPlacement())
	nodes := []*Node{c.Node(0), c.Node(1), c.Node(2)}
	ctx := context.Background()
	// 存活节点扫描面（node 内置放置的候选数据源——redis 目录成员视图）。
	scanner, ok := c.Directory().(interface {
		NodeIDs(ctx context.Context) ([]rans.NodeID, error)
	})
	if !ok {
		t.Fatal("redis directory must implement NodeIDs")
	}

	// 播种：每节点一个确定性的本地激活 → 心跳首个 tick 后进存活视图。
	seeds := make([]rans.ID, len(nodes))
	for i, n := range nodes {
		seeds[i] = mustID(t, "counter", fmt.Sprintf("placement-seed-%d", i+1))
		if v, err := n.Call(ctx, seeds[i], 0); err != nil || v != 1 {
			t.Fatalf("seed activation on node%d: v=%v err=%v", i+1, v, err)
		}
	}

	// 连接预热（环：node2→seed1、node3→seed2、node1→seed3——三对节点各拨
	// 一次，无互向并发）：一次成功调用即在两端 byNode 落表，此后双向复用。
	// 跳过本步会让 9 路并发首拨互撞——同节点对的双连接按顶替策略关旧，骑在
	// 被顶替连接上的在途请求其应答随连接失效（等请求超时）。并发首拨顶替窗
	// 是既知传输层时序，测试侧预热消除而非依赖它。
	for k := range nodes {
		from, to := (k+1)%3, k // node(k+1) 调用 node(k) 的种子
		if v, err := nodes[from].Call(ctx, seeds[to], 0); err != nil || v != 2 {
			t.Fatalf("warm-up call node%d→seed%d: v=%v err=%v", from+1, to+1, v, err)
		}
	}
	pollUntil(t, 5*time.Second, "all nodes visible in heartbeat scan", func() bool {
		ns, err := scanner.NodeIDs(ctx)
		return err == nil && len(ns) == 3
	})

	// 9 个新身份并发激活，调用方轮转（身份先备好——mustID 不能在测试 goroutine
	// 外 Fatal）。
	const total = 9
	ids := make([]rans.ID, total)
	for k := range ids {
		ids[k] = mustID(t, "counter", fmt.Sprintf("placement-dist-%d", k))
	}
	vals := make([]int, total)
	errs := make([]error, total)
	var wg sync.WaitGroup
	for k := 0; k < total; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			v, err := nodes[k%3].Call(ctx, ids[k], 0)
			vals[k], errs[k] = -1, err
			if err == nil {
				vals[k] = v.(int)
			}
		}(k)
	}
	wg.Wait()
	for k, err := range errs {
		if err != nil {
			t.Fatalf("placement call %d failed: %v", k, err)
		}
		if vals[k] != 1 {
			t.Fatalf("placement call %d returned %d, want 1 (fresh entity)", k, vals[k])
		}
	}

	// 承载节点统计：Call 返回即 Handle 已在宿主节点执行完（应答后于 Handle），
	// 宿主扫描即「实体执行所在节点」。共享工厂无法把节点号注入实体，GetLocal
	// 扫描是确定性的等价观测面。
	hosts := make(map[rans.NodeID]int)
	for _, id := range ids {
		host, found := hostOf(t, nodes, id)
		if !found {
			t.Fatalf("entity %s hosted nowhere", id.String())
		}
		hosts[host]++
	}
	if len(hosts) < 2 {
		t.Fatalf("placement must distribute entities over >=2 nodes, all %d on node %d", total, onlyKey(hosts))
	}
}

// hostOf 扫描三节点本地激活表（node.LocalIDs 公开观测面），返回 id 的承载
// 节点号。
func hostOf(t *testing.T, nodes []*Node, id rans.ID) (rans.NodeID, bool) {
	t.Helper()
	for i, n := range nodes {
		if slices.Contains(n.LocalIDs(), id) {
			return rans.NodeID(i + 1), true
		}
	}
	return 0, false
}

// onlyKey 取单键 map 的键（失败消息用；仅 len==1 时调用）。
func onlyKey(m map[rans.NodeID]int) rans.NodeID {
	for k := range m {
		return k
	}
	return 0
}
