// arbitration 演示：双节点共享 Redis 目录的激活仲裁与故障接管（ROADMAP 3.5）。
//
// 前置：先 `make test-redis-up` 拉起测试 Redis（127.0.0.1:16379，可用环境变量
// RANS_TEST_REDIS_ADDR 覆盖——与 Makefile/测试同口径），再 `go run ./examples/arbitration`。
//
// 每节点经 node.New 装配（短租约演示参数：租约 4s / 心跳 1s / 心跳键存活 8s）：
// node 内部即 QUICKSTART 手工装配序（心跳续租/位置缓存+变更轮询/放置/地址
// 解析全内置），各节点 Config.Directory.Redis 自建同前缀目录实例（逻辑同库
// 同前缀，等价共享）。放置 = preferSelf（self 在候选中即选自身——种子与
// 接管确定性落本节点，新鲜身份的并发首请求在两侧同时本地激活、撞同一把目
// 录 CAS）；闲置回收与再平衡只在 node1 启用（KindOptions[counter].Idle 迁
// 移档 + Rebalance 超阈卸载——自动执行，无手动入口）。演示 main 顶部另自建
// 同前缀 *redis.Directory 供直查目录（Lookup/NodeIDs）。机制全程承重，不做
// 裸目录操作编排：
//
//	Phase A 播种与成员视图：节点成员键启动即写（零实体也可见——空节点不死
//	  锁，冷启动窗消除）；每节点本地激活一个种子实体供连接预热，一次跨节点
//	  种子调用建立节点对连接（规避并发首拨顶替窗，与 testkit 同法）。
//	Phase A 仲裁：两节点对同一新鲜身份并发首请求——放置选自身，两路请求各
//	  自本地激活、撞同一把目录 CAS：赢家本地激活并执行；输家激活落败，其
//	  调用经重试环按目录归属改道赢家执行（落败实例终局逐出）。
//	Phase C 再平衡：node1 本地实体数超阈 → 自动卸载闲置候选（闲置超阈值、
//	最冷优先）迁给放置远端。
//	Phase B 接管：杀赢家（Close——不排空不注销，模拟崩溃）→ 有界轮询等租
//	约过期（Lookup miss）且存活视图淘汰死者 → 幸存者再 Call → miss 本地重
//	激活 → 目录归属翻转。
package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/redis"
	"github.com/cyinx/rans_golang/node"
	goredis "github.com/redis/go-redis/v9"
)

// 演示参数：短租约换演示可见性（附录 A 生产值为 5min/30s/2min）。
const (
	leaseTTL          = 4 * time.Second
	heartbeatInterval = time.Second
	heartbeatKeyTTL   = 2 * leaseTTL // 与 testkit WithRedis 同口径
	callTimeout       = 5 * time.Second
	idleAfter         = 800 * time.Millisecond // counter 闲置阈值（Phase C 迁移候选）
	rebalancePeriod   = 500 * time.Millisecond // 自动卸载扫描周期（演示可见性）
)

// redisAddr 测试 Redis 地址：RANS_TEST_REDIS_ADDR 优先（与 Makefile/测试同口径）。
func redisAddr() string {
	if v := os.Getenv("RANS_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:16379"
}

// redisPassword 测试 Redis 密码：RANS_TEST_REDIS_PASSWORD（内网直连模式）。
func redisPassword() string { return os.Getenv("RANS_TEST_REDIS_PASSWORD") }

// arbCounter 仲裁演示实体：计数 + 身份记账。host 由宿主节点的工厂盖戳（每节点
// 各自的 kinds 工厂闭包注入）；int 请求携带调用方节点号，Handle 记入 callers
// （执行序）；string 请求返回身份报告——输出里同时可见宿主节点与调用方节点。
type arbCounter struct {
	host    rans.NodeID
	calls   int
	callers []rans.NodeID
}

// Activate 激活钩子（本实体无状态初始化，no-op）。
func (c *arbCounter) Activate(context.Context) error   { return nil }
func (c *arbCounter) Deactivate(context.Context) error { return nil }

// Handle 计数/报告双协议（int = 计数并记调用方；string = 身份报告）。
func (c *arbCounter) Handle(_ context.Context, req any) (any, error) {
	switch v := req.(type) {
	case int:
		c.calls++
		c.callers = append(c.callers, rans.NodeID(v))
		return c.calls, nil
	case string:
		return fmt.Sprintf("host=node%d calls=%d callers=%v", c.host, c.calls, c.callers), nil
	}
	return nil, fmt.Errorf("unsupported request %T", req)
}

// preferSelf 放置：self 在候选中即选自身（确定性本地激活与接管），否则在
// 候选中随机（spec §5.4 的消费方自定义策略示例形态）。
type preferSelf struct {
	fallback *node.RandomPlacement
}

// Pick self 在候选中即选自身（确定性本地激活与接管），否则随机。不阻塞；
// 并发安全（回落策略自带锁）；由运行时在放置点调用。
func (p preferSelf) Pick(_ string, self rans.NodeID, candidates []rans.NodeID) (rans.NodeID, bool) {
	if slices.Contains(candidates, self) {
		return self, true
	}
	return p.fallback.Pick("", self, candidates)
}

// arbNode 演示节点：node 组装把手。
type arbNode struct {
	id rans.NodeID
	n  *node.Node
}

// kill 模拟崩溃：node.Close（停续租 + 停传输，不排空不注销——租约自然
// 过期，本地实例自此成为陈旧孤儿）。
func (n *arbNode) kill() { _ = n.n.Close() }

func main() {
	addr := redisAddr()
	preflight(addr)

	// 演示前缀一次算定：main 直查目录与各节点自建目录同库同前缀（逻辑
	// 等价共享）。
	prefix := fmt.Sprintf("rans:demo:arbitration:%d:", time.Now().UnixNano())
	dir := redis.New(redis.Options{
		Addr:         addr,
		Password:     redisPassword(),
		HeartbeatTTL: heartbeatKeyTTL,
		KeyPrefix:    prefix,
	})
	defer dir.Close()

	placement := preferSelf{fallback: node.NewRandomPlacement()}
	mkNode := func(id rans.NodeID, rebalance bool) *arbNode {
		cfg := node.Config{
			NodeID:      id,
			ClusterName: "arb-demo",
			ListenAddr:  "127.0.0.1:0",
			// 演示无密钥运行（fail-closed 缺省须显式声明）；真实部署经
			// node.LoadHMACKeyFromEnv 配钥。
			Security: node.SecurityConfig{Insecure: true},
			// 工厂按节点盖戳：实体报告里能区分宿主身份。
			Kinds: map[string]rans.Factory{
				"counter": func(_ rans.ID) rans.Entity { return &arbCounter{host: id} },
			},
			// 自建同前缀目录实例（逻辑同库同前缀，等价共享）。
			Directory: node.DirectoryConfig{Redis: &redis.Options{
				Addr:         addr,
				Password:     redisPassword(),
				HeartbeatTTL: heartbeatKeyTTL,
				KeyPrefix:    prefix,
			}},
			Lease:     node.LeaseConfig{Interval: heartbeatInterval},
			Calls:     node.CallConfig{Default: callTimeout},
			Placement: placement,
		}
		if rebalance {
			// 闲置回收（Phase C 再平衡演示用，仅 node1 启用——卸载单向：
			// node1 超阈把闲置候选迁给放置远端，node2 无策略不回迁）：
			// counter 闲置 idleAfter 后成为迁移候选；迁移目标 = 放置全集
			// 随机（排除自身——双节点集群下唯一候选）。
			cfg.KindOptions = map[string]node.KindOptions{
				"counter": {Idle: &node.IdlePolicy{After: idleAfter, Migrate: true}},
			}
			cfg.Rebalance = node.RebalanceConfig{Threshold: 1, Period: rebalancePeriod, MaxPerRound: 10}
		}
		n, err := node.New(cfg)
		if err != nil {
			panic(err)
		}
		// 例外①（进程入口）：main 是进程根，节点根 ctx 由 Start 内部自建
		// ——§5.1 第 2 条管辖库代码，main 不在列。
		if err := n.Start(context.Background()); err != nil {
			panic(err)
		}
		return &arbNode{id: id, n: n}
	}
	nodes := []*arbNode{mkNode(1, true), mkNode(2, false)}
	defer nodes[0].kill()
	defer nodes[1].kill()
	fmt.Printf("[setup] redis directory at %s (lease %s / heartbeat %s), placement=preferSelf, resolve=node-key\n",
		addr, leaseTTL, heartbeatInterval)
	fmt.Printf("[setup] node1 %s, node2 %s\n", nodes[0].n.Addr(), nodes[1].n.Addr())

	// 决策文档约束：调用方必须传带 deadline 的 ctx。
	ctx, cancel := context.WithTimeout(context.Background() /* 例外①（进程入口）：演示进程的显式预算 */, 45*time.Second)
	defer cancel()
	target := mustID("counter", "arb-1")

	phaseA(ctx, dir, nodes, target)
	phaseC(ctx, dir, nodes)
	phaseB(ctx, dir, nodes, target)
	fmt.Println("[done] dual-node arbitration demo finished")
}

// preflight 探活测试 Redis：不可达给出可操作的提示后退出（非 0）。
func preflight(addr string) {
	cli := goredis.NewClient(&goredis.Options{Addr: addr, Password: redisPassword(), DialTimeout: time.Second})
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background() /* 例外①（进程入口）：演示进程的显式预算 */, 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "arbitration: redis unavailable at %s: %v\nfirst run `make test-redis-up`\n", addr, err)
		os.Exit(1)
	}
}

func mustID(kind, inst string) rans.ID {
	id, err := rans.NewID(kind, inst)
	if err != nil {
		panic(err)
	}
	return id
}

// fail 演示失败出口：stderr 提示 + 退出码 1（正常路径保持 exit 0）。
func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "arbitration: "+format+"\n", args...)
	os.Exit(1)
}

// pollUntil 有界轮询：check 每 20ms 重试，超时返回 false（调用方决定报错口径）。
func pollUntil(d time.Duration, check func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if check() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// aliveView 读当前存活节点集（演示打印用；数据源 = 节点成员键）。
func aliveView(ctx context.Context, dir *redis.Directory) []rans.NodeID {
	ns, err := dir.NodeIDs(ctx)
	if err != nil {
		fail("membership scan: %v", err)
	}
	return ns
}

// phaseA 播种 + 并发首请求仲裁。
func phaseA(ctx context.Context, dir *redis.Directory, nodes []*arbNode, target rans.ID) {
	fmt.Printf("--- Phase A: concurrent first requests on fresh id %s ---\n", target.String())

	// 成员视图起步即双节点：节点键启动即写（零实体也可见），无需等心跳
	// 首轮——有界等待只是兜 goroutine 调度抖动。
	if !pollUntil(5*time.Second, func() bool {
		return len(aliveView(ctx, dir)) == 2
	}) {
		fail("membership view did not converge to 2 nodes within 5s")
	}
	fmt.Printf("[seed] both nodes alive in membership view (node keys written at start): %v\n", aliveView(ctx, dir))

	// 播种：每节点本地激活一个种子（放置选自身——确定性落本节点）——种子
	// 的作用是连接预热与 Phase C 的迁移候选。
	for _, n := range nodes {
		seed := mustID("counter", fmt.Sprintf("seed-node%d", n.id))
		if v, err := n.n.Client().Call(ctx, seed, int(n.id)); err != nil || v != 1 {
			fail("seed activation on node%d: v=%v err=%v", n.id, v, err)
		}
	}

	// 连接预热：node2 调 node1 的种子（跨节点路由 + 节点对连接建立——一次
	// 成功调用即在两端落表，此后双向复用；规避并发首拨的顶替窗）。
	seed1 := mustID("counter", "seed-node1")
	if v, err := nodes[1].n.Client().Call(ctx, seed1, 2); err != nil || v != 2 {
		fail("warm-up cross-node call: v=%v err=%v", v, err)
	}
	fmt.Println("[seed] warm-up: node2 -> node1 seed served cross-node (counter 2)")

	// 并发首请求：两节点同拍出发。放置选自身 → 两路请求各自本地激活、撞
	// 同一把目录 CAS（register-first 仲裁）。
	type callRes struct {
		v   int
		err error
	}
	results := make([]callRes, len(nodes))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, n *arbNode) {
			defer wg.Done()
			<-start
			v, err := n.n.Client().Call(ctx, target, int(n.id))
			if err == nil {
				results[i] = callRes{v: v.(int)}
				return
			}
			results[i] = callRes{err: err}
		}(i, n)
	}
	close(start)
	wg.Wait()

	for i, r := range results {
		if r.err != nil {
			fail("phase A request from node%d failed: %v", nodes[i].id, r.err)
		}
	}
	loc, found, err := dir.Lookup(ctx, target)
	if err != nil || !found {
		fail("phase A directory lookup: found=%v err=%v", found, err)
	}
	loserIdx := 1 - nodeIndexOf(nodes, loc.Node)
	fmt.Printf("[A] winner: node%d (won directory CAS, entity activated and executing locally)\n", loc.Node)

	// 输家证据：落败激活的终态逐出（本地表清空）有界等待——正常时序必然
	// 发生（输家激活落败后终局清理）；罕见交错（输家调用方目录已命中直接
	// 路由、无本地激活尝试）如实改口。
	loser := nodes[loserIdx]
	if pollUntil(2*time.Second, func() bool {
		return !slices.Contains(loser.n.LocalIDs(), target)
	}) {
		fmt.Printf("[A] loser: node%d (activation lost arbitration, its request re-routed to the winner by directory ownership)\n", loser.id)
	} else {
		fmt.Printf("[A] loser: node%d (its request hit the directory entry first and was routed to winner directly)\n", loser.id)
	}
	for i, r := range results {
		fmt.Printf("[A] request from node%d -> counter %d\n", nodes[i].id, r.v)
	}
	a, b := results[0].v, results[1].v
	if !((a == 1 && b == 2) || (a == 2 && b == 1)) {
		fail("phase A counters must be exactly {1,2}, got %d/%d", a, b)
	}
	fmt.Printf("[A] all %d requests succeeded, final counter = %d\n", len(results), max(a, b))

	// 身份报告：经输家节点 Call（目录命中赢家，展示仲裁落定后的路由）。
	report, err := loser.n.Client().Call(ctx, target, "report")
	if err != nil {
		fail("phase A report call: %v", err)
	}
	fmt.Printf("[A] entity report via node%d: %s\n", loser.id, report)
}

// phaseB 杀赢家 → 租约过期 → 幸存者重激活接管。
func phaseB(ctx context.Context, dir *redis.Directory, nodes []*arbNode, target rans.ID) {
	fmt.Println("--- Phase B: crash the winner, wait lease expiry, takeover by survivor ---")

	loc, found, err := dir.Lookup(ctx, target)
	if err != nil || !found {
		fail("phase B owner-before lookup: found=%v err=%v", found, err)
	}
	winnerIdx := nodeIndexOf(nodes, loc.Node)
	survivorIdx := 1 - winnerIdx
	winner, survivor := nodes[winnerIdx], nodes[survivorIdx]
	fmt.Printf("[B] directory owner before crash: node%d\n", loc.Node)

	// 模拟崩溃：node.Close（停传输 + 停续租，不排空不注销）→ 租约不再续
	// 期，本地实例成陈旧孤儿。
	killed := time.Now()
	winner.kill()
	fmt.Printf("[B] node%d killed (transport + heartbeat stopped)\n", winner.id)

	// 有界等待两件事同时成立：目录租约过期（Lookup miss）+ 死节点心跳键
	// 过期（存活视图淘汰——放置候选里不能再有死者，否则 miss 路径会把
	// 重激活请求远发给死节点）。
	if !pollUntil(15*time.Second, func() bool {
		_, found, err := dir.Lookup(ctx, target)
		if err != nil || found {
			return false
		}
		for _, n := range aliveView(ctx, dir) {
			if n == winner.id {
				return false
			}
		}
		return true
	}) {
		fail("lease expiry / membership eviction did not converge within 15s")
	}
	fmt.Printf("[B] lease expired after %s (directory miss, node%d evicted from membership view: %v)\n",
		time.Since(killed).Round(time.Millisecond), winner.id, aliveView(ctx, dir))

	// 幸存者重激活：目录 miss、放置选自身（唯一存活节点）→ 本地激活。本
	// 演示不接 checkpoint Store（钝化/迁移恢复面见 testkit 验收），计数从
	// 1 重新开始。
	v, err := survivor.n.Client().Call(ctx, target, int(survivor.id))
	if err != nil {
		fail("phase B reactivation call: %v", err)
	}
	fmt.Printf("[B] node%d reactivated the entity: counter = %v (fresh state -- demo wires no checkpoint store)\n",
		survivor.id, v)

	loc2, found, err := dir.Lookup(ctx, target)
	if err != nil || !found {
		fail("phase B owner-after lookup: found=%v err=%v", found, err)
	}
	if loc2.Node != survivor.id {
		fail("phase B owner must flip to survivor node%d, got node%d", survivor.id, loc2.Node)
	}
	fmt.Printf("[B] directory owner flipped: node%d -> node%d\n", loc.Node, loc2.Node)

	// 接管后继续服务 + 身份报告（宿主已是幸存者）。
	v2, err := survivor.n.Client().Call(ctx, target, int(survivor.id))
	if err != nil || v2 != 2 {
		fail("phase B second call: v=%v err=%v (want 2)", v2, err)
	}
	report, err := survivor.n.Client().Call(ctx, target, "report")
	if err != nil {
		fail("phase B report call: %v", err)
	}
	fmt.Printf("[B] survivor keeps serving: counter = %v, entity report: %s\n", v2, report)
}

// phaseC 自动再平衡：node1 超阈（KindOptions[counter].Idle 迁移档 +
// Rebalance.Threshold=1——构造期声明、自动执行，无手动入口）→ 闲置候选
// （闲置超 idleAfter、最冷优先）经迁移交给放置远端 node2。
//
// 排在 Phase B 之前：再平衡需要放置有候选（Close 是终态，Phase B 后集群
// 只剩单节点，卸载会以无候选短路——那也是如实语义，但演示看不到迁移）。
func phaseC(ctx context.Context, dir *redis.Directory, nodes []*arbNode) {
	fmt.Println("--- Phase C: automatic rebalance offloads idle entities to the placement remote ---")

	// 超阈：node1 已有 Phase A 的种子（+仲裁若 node1 是赢家则还有 target），
	// 再激活一个实体补足闲置候选。
	cold := mustID("counter", "rebal-cold")
	if _, err := nodes[0].n.Client().Call(ctx, cold, 1); err != nil {
		fail("phase C cold activation: %v", err)
	}
	// 等闲置窗越界 + 自动卸载轮（周期 rebalancePeriod）：node1 本地 >1 个
	// 实体即超阈，闲置候选最冷优先迁给 node2（卸载单向——仅 node1 配置了
	// 策略）。
	time.Sleep(idleAfter + 2*rebalancePeriod)
	fmt.Printf("[C] node1 local entities = %d (threshold 1), waiting for automatic offload\n",
		len(nodes[0].n.LocalIDs()))

	// 迁移结果如实呈现：闲置候选（最冷优先）归属翻转到放置远端 node2
	//（放置全集排除自身，双节点集群下唯一候选）；有界轮询等自动卸载落地。
	migrated := 0
	if !pollUntil(10*time.Second, func() bool {
		migrated = 0
		for _, id := range []rans.ID{cold, mustID("counter", "seed-node1")} {
			loc, found, err := dir.Lookup(ctx, id)
			if err != nil || !found {
				return false
			}
			if loc.Node == 2 {
				migrated++
			}
		}
		return migrated > 0
	}) {
		fmt.Println("[C] no idle candidate migrated (best-effort: losers stay Active, next round retries)")
		return
	}
	var report any
	var err error
	for _, id := range []rans.ID{cold, mustID("counter", "seed-node1")} {
		if report, err = nodes[0].n.Client().Call(ctx, id, "report"); err == nil {
			fmt.Printf("[C] idle entity migrated node1 -> node2, served cross-node: %v\n", report)
			break
		}
	}
	if err != nil {
		fail("phase C call after migration: %v", err)
	}
	fmt.Printf("[C] node1 local entities after offload = %d (migrated %d)\n",
		len(nodes[0].n.LocalIDs()), migrated)
}

// nodeIndexOf 节点号 → nodes 下标（不在则致命）。
func nodeIndexOf(nodes []*arbNode, id rans.NodeID) int {
	for i, n := range nodes {
		if n.id == id {
			return i
		}
	}
	fail("node %d not in demo cluster", id)
	return -1
}
