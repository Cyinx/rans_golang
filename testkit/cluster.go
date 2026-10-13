// Package testkit 提供进程内多节点测试集群与故障注入原语（ROADMAP 2.10）。
// 装配基于公开组装根 node.New（集群 = 逐个 node 装配）；依赖只经公开包 +
// internal/testhooks（约定 spec §3.2 导入图表 testkit 行），不得被反向依赖。
// 故障原语（Kill/Partition/Heal/Slow/HealSlow）经 node.Config.TestHooks
// **逐节点**注入——各集群的注入闭包只捕获本集群状态，并行多集群零共享
// 可变状态（cluster_test.go 并行隔离行为锁）。
package testkit

import (
	"context"
	"errors"
	"fmt"
	goredis "github.com/redis/go-redis/v9"
	"net"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/node"
)

// 测试集群共享配置：双端一致的集群名与密钥（顺路覆盖握手 MAC 路径）。
const clusterName = "testkit-cluster"

var clusterSecret = []byte("testkit-secret")

// defaultCallTimeout 测试集群的缺省调用预算（装配进 Calls.Default）：比
// 生产缺省 30s 短，测试卡死时快速失败；需要更短/更长经 WithCallDefault/
// WithCallMax 调整。
const defaultCallTimeout = 3 * time.Second

// Node 集群中的一个节点：公开组装根 *node.Node 的薄包装。Call/Tell 经
// 统一调用面（node.Client()）转发——测试脚手架不吞错误、不隐式重试，
// 重试语义由具体测试自行编排。节点的目录/激活表/续租/轮询/缓存全部内置
// 于 node，测试改用公开面断言。
//
// 各转发方法（Call/Tell/Migrate/Destroy/Shutdown/Close 等）的阻塞、并发
// 与错误语义同 node 包同名词，不在此重复。
type Node struct {
	ID   rans.NodeID
	Node *node.Node
}

// Client 返回该节点的统一调用面（node.Client() 同一面；node.New 即装配，
// 并发安全）。
func (n *Node) Client() rans.Client { return n.Node.Client() }

// Call 转发统一调用面（目录路由：远程/本地由集群状态决定）。
func (n *Node) Call(ctx context.Context, id rans.ID, req any) (any, error) {
	return n.Client().Call(ctx, id, req)
}

// CallOpts 带调用级选项的转发（rans.CallOption 公开面，如 rans.Idempotent()）。
func (n *Node) CallOpts(ctx context.Context, id rans.ID, req any, opts ...rans.CallOption) (any, error) {
	return n.Client().Call(ctx, id, req, opts...)
}

// Tell 转发统一调用面单向发送（at-most-once，入箱即返回；与
// rans.Client.Tell 同词——单向动词唯一，无 SendOneWay 旧名）。
func (n *Node) Tell(ctx context.Context, id rans.ID, req any) error {
	return n.Client().Tell(ctx, id, req)
}

// Addr 转发节点实际监听地址（Start 后有效）。
func (n *Node) Addr() string { return n.Node.Addr() }

// LocalIDs 转发本地激活表快照（localonly 断言面：实体归属的公开观测口）。
func (n *Node) LocalIDs() []rans.ID { return n.Node.LocalIDs() }

// Holds 报告 id 是否在本地激活表（GetLocal 语义的公开等价物）。
func (n *Node) Holds(id rans.ID) bool { return slices.Contains(n.LocalIDs(), id) }

// Migrate 转发管理操作（本地实例迁移；非本地 NotFound+NotLocal）。
func (n *Node) Migrate(ctx context.Context, id rans.ID) error { return n.Node.Migrate(ctx, id) }

// Destroy 转发管理操作（本地实例优雅终止；非本地 NotFound+NotLocal）。
func (n *Node) Destroy(ctx context.Context, id rans.ID) error { return n.Node.Destroy(ctx, id) }

// Shutdown 转发优雅停机（排空序）。
func (n *Node) Shutdown(ctx context.Context) error { return n.Node.Shutdown(ctx) }

// Close 转发立即停机（Kill 原语的逐节点落点；幂等）。
func (n *Node) Close() error { return n.Node.Close() }

// Cluster 进程内测试集群：n 个 node 节点回环互联（端口 0 自动分配）、共享
// 目录、地址表在全部 Start 后填充（节点间 resolver 自动互指；redis 模式走
// 目录成员键派生解析）。
//
// 故障原语（逐节点 TestHooks 注入）：
//   - Kill(i)：Close 节点 i（listener+连接+心跳环+续租/轮询 runCtx 收口；
//     redis 模式租约不再续期，到期后目录条目自然腾位；本地实体成陈旧孤儿
//     不再服务）。终态操作，无复活原语；
//   - Partition(a,b)/Heal(a,b)：双向拨号屏蔽（各自 Dial 钩子查本节点屏蔽
//     表）+ 关断既有连接（拨号侧持有的原生连接直接 Close，对端读循环随
//     FIN 走 connLost 清表上报）；
//   - Slow(i,d)/HealSlow(i)：节点 i 全部连接每帧读写注入延迟 d（动态生效）。
//
// 并发约定（spec §5.8 的测试工具口径，Cluster 全部公开方法适用）：Shutdown
// 阻塞到全部节点排空完成或 ctx 耗尽（幂等，聚合逐节点错误）；Kill/
// Partition/Heal/Slow/HealSlow 不阻塞于长操作（终态/注入即刻生效）；全部
// 方法并发安全；全部为测试编排/断言面，不存在实体 Handle 内调用场景。
type Cluster struct {
	t     *testing.T
	table resolveTable
	dir   directory.Directory // 共享目录（redis 模式下为 *redis.Directory）

	mu      sync.Mutex
	blocked map[rans.NodeID]map[rans.NodeID]bool       // 节点 → 分区屏蔽的对端集合
	tracked map[rans.NodeID]map[rans.NodeID][]net.Conn // 拨号节点 → 对端 → 原生连接
	byAddr  map[string]rans.NodeID                     // 地址 → 节点号（Start 后冻结）

	nodes []*clusterNode
	// redis 模式参数（KillMembership 的成员键操纵面；非 redis 模式为零值）。
	redisAddr   string
	redisPrefix string
}

// KillMembership 模拟节点成员死亡（确定性快路径）：关闭节点（停存活刷新/
// 传输，与 Kill 同语义）后**立即双删**其判活键与注册总表 field——成员键
// 过期的免等形态（真等 heartbeatTTL 收敛的旧路径在故障用例里是纯时延）。
// 实体目录条目保持死 owner 常驻（v3 崩溃残留形态，不墓碑化）；归属翻转
// 由幸存者激活的 Register CAS 接管完成（H13 决策 21/22）。仅 redis 模式
// （成员键承载于 Redis；inmem 无成员键语义）。
func (c *Cluster) KillMembership(i int) {
	c.t.Helper()
	if c.redisAddr == "" {
		c.t.Fatal("testkit: KillMembership requires redis mode (membership keys live in Redis)")
	}
	c.Node(i).Close() // 幂等；Kill 同款停机（停存活刷新是双删不被覆写的前提）
	node := uint16(i + 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli := goredis.NewClient(&goredis.Options{Addr: c.redisAddr, Password: os.Getenv("RANS_TEST_REDIS_PASSWORD")})
	defer cli.Close()
	if err := cli.Del(ctx, c.redisPrefix+fmt.Sprintf("rans:{reg}:node:%d", node)).Err(); err != nil {
		c.t.Fatalf("testkit: KillMembership: del liveness key of node %d: %v", node, err)
	}
	if err := cli.HDel(ctx, c.redisPrefix+"rans:{reg}:nodes", strconv.Itoa(int(node))).Err(); err != nil {
		c.t.Fatalf("testkit: KillMembership: hdel roster field of node %d: %v", node, err)
	}
}

// Directory 返回集群共享目录（redis 模式下为 *redis.Directory，测试可直连
// 做外部干预：窃取归属、探存活视图等）。
func (c *Cluster) Directory() directory.Directory { return c.dir }

// Node 返回第 i 个节点（0 基；节点号 = i+1）；越界即 Fatal（测试断言路径）。
func (c *Cluster) Node(i int) *Node {
	c.t.Helper()
	if i < 0 || i >= len(c.nodes) {
		c.t.Fatalf("testkit: node index %d out of range (n=%d)", i, len(c.nodes))
	}
	return &c.nodes[i].node
}

// Kill 停机节点 i：Close 其 node（listener + 全部连接 + 心跳环 + 续租/轮询
// 的 runCtx 收口——不排空不注销，模拟崩溃）；redis 模式下租约不再续期，
// 到期后目录条目自然腾位（Kill 的本地实体成为陈旧孤儿，不再服务任何请求）。
// inmem 模式同语义（终态，不可恢复）。
func (c *Cluster) Kill(i int) {
	c.t.Helper()
	c.Node(i).Close() // Node.Close 幂等；注入目录归调用方（不关）
}

// Shutdown 优雅关停集群：按节点逆序 Shutdown（末节点先排空——依赖方向靠
// 后的消费者先让位；node.Shutdown 内置排空标记/续租照常/停序头等次序）。
// 节点间独立：单节点排空失败聚合进返回错误、不阻断其余节点。幂等。
func (c *Cluster) Shutdown(ctx context.Context) error {
	c.t.Helper()
	var errs []error
	for i := len(c.nodes) - 1; i >= 0; i-- {
		cn := c.nodes[i]
		if err := cn.node.Node.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("testkit: shutdown node %d: %w", cn.node.ID, err))
		}
	}
	return errors.Join(errs...)
}
