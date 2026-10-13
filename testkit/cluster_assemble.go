package testkit

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/directory/redis"
	"github.com/cyinx/rans_golang/internal/testhooks"
	"github.com/cyinx/rans_golang/node"
)

// resolveTable 可更新节点地址表：全部节点 Start 前表空（Resolve 报错），
// Start 后一次性填充（TestHooks.Resolve 在 node.New 期定死，故用原子表
// 而非重建节点）。redis 模式不用本表（目录派生解析器）。
type resolveTable struct {
	m atomic.Pointer[map[rans.NodeID]string]
}

func (rt *resolveTable) resolve(_ context.Context, node rans.NodeID) (string, error) {
	if m := rt.m.Load(); m != nil {
		if a, ok := (*m)[node]; ok {
			return a, nil
		}
	}
	return "", fmt.Errorf("testkit: node %d not in resolve table", node)
}

func (rt *resolveTable) set(m map[rans.NodeID]string) { rt.m.Store(&m) }

// fixedLocalPlacement 恒不放置（首激活/迁移选点全部本地收口）——缺省
// 确定性编排面（spec §5.4：Pick 返回 false → 本节点声明承载该 kind 就本地
// 激活；迁移失败）。node 测试的 localPlacement 同款形态。
type fixedLocalPlacement struct{}

// Pick 恒返回 (0, false)：miss 一律本地激活。
func (fixedLocalPlacement) Pick(string, rans.NodeID, []rans.NodeID) (rans.NodeID, bool) {
	return 0, false
}

// clusterNode 节点私有状态：Slow 注入的每帧读写延迟（原子读，WrapConn 包装
// 的已建立连接即时生效）。心跳归 node 内置（redis 形态自动接线）。
type clusterNode struct {
	node    Node
	latency atomic.Int64 // ns；0 = 无延迟
}

// NewCluster 拉起 n 个节点（节点号 1..n）：逐个 node.New（注入 TestHooks：
// 拨号/连接包装/解析全部逐节点闭包）→ Start → 回填地址表。共享目录与
// kinds 贯穿全集群；redis 模式（WithRedis）每节点的心跳由 node 内置。
// t.Cleanup 统一 Close 所有节点与自建目录（幂等，Kill 过的节点重复 Close
// 无害）。阻塞到全部节点 Start 完成（监听就绪）；并发安全（测试装配面）。
func NewCluster(t *testing.T, n int, kinds map[string]rans.Factory, opts ...Option) *Cluster {
	t.Helper()
	if n < 1 {
		t.Fatal("testkit: cluster needs at least 1 node")
	}
	o := options{callDefault: defaultCallTimeout, epochFailNode: -1}
	for _, f := range opts {
		f(&o)
	}
	if o.dir != nil && o.redis != nil {
		t.Fatal("testkit: WithDirectory and WithRedis are mutually exclusive (redis mode brings its own directory)")
	}
	if o.epochFailNode < -1 || o.epochFailNode >= n {
		t.Fatalf("testkit: WithNodeEpochFailure node index %d out of range (n=%d)", o.epochFailNode, n)
	}
	// redis 模式：目录由 testkit 构建并注入（测试持有——外部干预面与
	// Kill 语义都靠它；node 只在 Directory.Redis 自建形态才拥有关闭权）。
	redisPrefix := ""
	if o.redis != nil {
		// 独立 KeyPrefix：集群实例间隔离，-count=N 重跑间也隔离（短租约键
		// 不串场；沿用 directory/redis 测试的口径）。
		redisPrefix = fmt.Sprintf("rans:testkit:%s:%d:", t.Name(), time.Now().UnixNano())
		redisDir := redis.New(redis.Options{
			Addr:     o.redis.addr,
			Password: os.Getenv("RANS_TEST_REDIS_PASSWORD"),
			// 内网带认证 Redis 直连模式：密码经 RANS_TEST_REDIS_PASSWORD 注入
			//（与测试门禁同口径；docker 本地实例为空）。
			HeartbeatTTL: 2 * o.redis.leaseTTL,
			KeyPrefix:    redisPrefix,
		})
		o.dir = redisDir
		// 残键清扫登记先于其余 Cleanup——LIFO 下最后执行：节点停机与
		// 目录 Close 之后 SCAN+DEL 本前缀（含设计性无 TTL 的 {reg}:epoch
		// 代际 Hash 与 {reg}:nodes 注册总表）。仅测试侧：生产 Close 只关
		// 客户端绝不清键（红线）。
		t.Cleanup(func() {
			if err := sweepRedisTestKeys([]string{o.redis.addr}, os.Getenv("RANS_TEST_REDIS_PASSWORD"), redisPrefix); err != nil {
				t.Errorf("testkit: sweep redis test keys (prefix %s): %v", redisPrefix, err)
			}
		})
		t.Cleanup(func() { redisDir.Close() })
	}
	if o.dir == nil {
		o.dir = inmem.New()
	}
	c := &Cluster{
		t:       t,
		dir:     o.dir,
		blocked: make(map[rans.NodeID]map[rans.NodeID]bool),
		tracked: make(map[rans.NodeID]map[rans.NodeID][]net.Conn),
		byAddr:  make(map[string]rans.NodeID),
	}
	if o.redis != nil {
		c.redisAddr = o.redis.addr
		c.redisPrefix = redisPrefix
		// FakeDeath 的落定等待基准：节点实际生效的心跳周期（≤0 → node 侧
		// membership 缺省 30s；测试一律显式配短值）。
		c.hbInterval = o.redis.heartbeatInterval
		if c.hbInterval <= 0 {
			c.hbInterval = 30 * time.Second
		}
	}
	// 成员视图注入面（WithFakeDeath / WithNodeEpochFailure 装配）：逐节点
	// 成员写门控句柄，与 nodes 平行（下标同序）；未启用时 gates 为 nil。
	if o.fakeDeath || o.epochFailNode >= 0 {
		c.gates = make([]*membershipGate, n)
	}
	// 清理先于构造注册：构造中途 Fatal 时已起的节点 goroutine 也能被收掉。
	// LIFO 下它先于 redis 目录的 Close（更早登记）执行——节点先停、目录后关。
	t.Cleanup(func() {
		for i := range c.nodes {
			_ = c.nodes[i].node.Close() // 幂等；Kill/Shutdown 过的节点无害
		}
	})
	// TLS 模式（WithTLS）：集群级自签 CA 一次生成，逐节点证书按节点号
	// 签发（serial = 1000+节点号，节点号从 1 起 → 节点 serial ≥1001；
	// CA 自签 serial=1 与之无重叠，满足 newClusterCA 的序列号相异契约）。
	var ca *clusterCA
	if o.tls {
		ca = newClusterCA(t, 1)
	}
	addrs := make(map[rans.NodeID]string, n)
	tableResolve := true
	for i := 1; i <= n; i++ {
		idx := i - 1
		id := rans.NodeID(i)
		cn := &clusterNode{} // 稳定实例指针：WrapConn 闭包经它读延迟，避免索引 c.nodes 与 append 竞态
		resolve := c.table.resolve
		if o.redis != nil {
			resolve = nil // redis 模式：目录派生 Resolver（node 内置分支）
			tableResolve = false
		} else if _, hasResolver := o.dir.(interface {
			NodeAddrResolver() func(ctx context.Context, n rans.NodeID) (string, error)
		}); hasResolver {
			// 注入目录自带成员键解析（包装 redis 目录等形态）→ 同样落
			// node 的目录派生分支；地址表不再需要。
			resolve = nil
			tableResolve = false
		}
		// 假死注入面：本节点的目录换成成员写门控包装（其余节点直用共享
		// 目录；门控布防前行为与裸目录全等）。取号失败布防在此落定——
		// 先于 node.New/Start 的唯一确定性时机。
		dirFor := o.dir
		if c.gates != nil {
			g, err := newMembershipGate(o.dir)
			if err != nil {
				t.Fatalf("testkit: node %d: %v", id, err)
			}
			if idx == o.epochFailNode {
				g.failEpoch()
			}
			c.gates[idx] = g
			dirFor = g
		}
		cfg := node.Config{
			NodeID:      id,
			ClusterName: clusterName,
			Kinds:       kinds,
			Directory:   node.DirectoryConfig{Injected: dirFor},
			Security:    node.SecurityConfig{HMACKey: clusterSecret},
			Calls:       node.CallConfig{Default: o.callDefault, Max: o.callMax},
			Store:       o.store,
			Outbox:      node.OutboxConfig{Capacity: o.outboxCapacity},
			// 解析：inmem 模式注入 Start 后回填的地址表（TestHooks.Resolve）；
			// redis 模式保持 nil——落到 node 的目录派生分支（成员键读地址，
			// Start 后由心跳写入）。
			TestHooks: &testhooks.Hooks{
				Dial:     c.dialFor(id),
				WrapConn: wrapForNode(cn),
				Resolve:  resolve,
			},
		}
		if ca != nil {
			cfg.Security.TLS = ca.nodeTLSConfig(t, int64(1000+i))
		}
		if o.checkpointInterval > 0 {
			cfg.Tuning.CheckpointInterval = o.checkpointInterval
		}
		// 放置三分支：逐节点覆盖（WithNodePlacement，迁移选点编排）＞
		// 随机（WithPlacement，node 缺省策略）＞ 固定本地（缺省——miss
		// 确定性本地激活）。
		switch {
		case o.nodePlacements[idx] != nil:
			cfg.Placement = o.nodePlacements[idx]
		case o.randomPlacement:
			cfg.Placement = nil // node 缺省 = 随机（成员视图候选）
		default:
			cfg.Placement = fixedLocalPlacement{}
		}
		if o.redis != nil {
			// 存活节拍周期（node 内置接线：Lease.Interval 驱动成员键刷新；
			// ≤0 → membership 缺省 30s——测试注入短值即缩短失效传播延迟）。
			cfg.Lease.Interval = o.redis.heartbeatInterval
			// 存活节拍自围栏阈值逐节点覆盖（WithNodeSelfFence）：取号失败
			// 形态的止损时长控制面。
			if d, ok := o.nodeSelfFences[idx]; ok {
				cfg.Lease.SelfFence = d
			}
		}
		if o.watchdog != nil {
			wd := *o.watchdog
			cfg.Watchdog = wd
		}
		if o.idleKind != "" {
			cfg.KindOptions = map[string]node.KindOptions{
				o.idleKind: {Idle: &node.IdlePolicy{After: o.idleAfter, Migrate: false}},
			}
			cfg.Rebalance.Period = o.idleLoop
		}
		nod, err := node.New(cfg)
		if err != nil {
			t.Fatalf("testkit: new node %d: %v", id, err)
		}
		if err := nod.Start(t.Context()); err != nil { // 测试基建装配入口：Start ctx 只辖监听建立（t.Context 随测试生命期收口）
			t.Fatalf("testkit: start node %d: %v", id, err)
		}
		addrs[id] = nod.Addr()
		c.mu.Lock()
		c.byAddr[nod.Addr()] = id
		c.mu.Unlock()
		cn.node = Node{ID: id, Node: nod}
		c.nodes = append(c.nodes, cn)
	}
	if tableResolve {
		// 表解析形态：全部 Start 后一次性填地址表（resolver 已在构造时
		// 定死，用原子表回填）。成员键解析形态地址走目录，无需回填。
		c.table.set(addrs)
	}
	return c
}
