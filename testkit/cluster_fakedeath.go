package testkit

// 成员视图故障注入原语（误判接管的触发形态，H13 决策 21/22 接管围栏链
// 路的 e2e 注入面），两种形态共用 membershipGate 门控目录：
//   - 假死（WithFakeDeath 装配，Cluster.FakeDeath 触发）：节点活体但成员
//     视图判死。与 KillMembership 的差别：节点不停机——TCP 监听、目录实
//     体键、对客户端服务全部照常，仅成员键写入被门控吞掉，使 liveness 周
//     期无法复活键；
//   - 取号失败（WithNodeEpochFailure 装配期布防）：节点代际取号恒报错，
//     成员键停写、节点对成员视图不可见，持续失败经 SelfFence 记账推进止
//     损时钟。
//
// 均 redis 模式专用（成员键承载于 Redis）。

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	goredis "github.com/redis/go-redis/v9"
)

// membershipGate 单节点成员写门控目录：包装共享目录，布防后仅 RegisterNode
// 被吞（静默成功——写方视角零失败，SelfFence 不触发，假死形态才成立）或
// NextNodeEpoch 被拒（取号失败形态，报错入账 SelfFence），其余方法全部透
// 传。内嵌接口逐方法委托 Directory + NodeRegistry 两能力面（node 装配与
// 路由有效性判定按能力探测接线；KindScanner 经 NodeRegistry 方法集自然满
// 足），NodeAddrResolver 显式透传。
type membershipGate struct {
	directory.Directory
	directory.NodeRegistry
	armed     atomic.Bool // 假死布防：RegisterNode 静默吞
	epochFail atomic.Bool // 取号失败布防：NextNodeEpoch 一律报错
}

// newMembershipGate 包装带成员能力的目录；无成员能力的目录报错（假死语
// 义依赖成员键承载，inmem 等基础面不适用）。
func newMembershipGate(dir directory.Directory) (*membershipGate, error) {
	reg, ok := dir.(directory.NodeRegistry)
	if !ok {
		return nil, fmt.Errorf("testkit: fake-death needs a directory with NodeRegistry (redis mode)")
	}
	return &membershipGate{Directory: dir, NodeRegistry: reg}, nil
}

// arm 布防门控：后续 RegisterNode 一律静默吞掉（幂等）。
func (g *membershipGate) arm() { g.armed.Store(true) }

// failEpoch 布防取号失败：后续 NextNodeEpoch 一律报错（幂等）。必须在节
// 点 Start 前布防——取号仅发生在存活节拍的未取号期（成功即缓存、此后不
// 再取号），装配期布防是该形态唯一的确定性注入时机。
func (g *membershipGate) failEpoch() { g.epochFail.Store(true) }

// RegisterNode 门控点：布防后吞写报成功（键保持缺席，写方无感知——写失
// 败会触发 SelfFence 兜底，那是另一条防线，不属于假死形态）。
func (g *membershipGate) RegisterNode(ctx context.Context, node rans.NodeID, info directory.NodeInfo) error {
	if g.armed.Load() {
		return nil
	}
	return g.NodeRegistry.RegisterNode(ctx, node, info)
}

// RenewNode 门控点：布防后吞写报 Renewed（比对续期不得破坏假死——续期
// 探测会把键缺席暴露成 Absent，触发复活处置）。
func (g *membershipGate) RenewNode(ctx context.Context, node rans.NodeID, info directory.NodeInfo) (directory.RenewResult, error) {
	if g.armed.Load() {
		return directory.RenewRenewed, nil
	}
	return g.NodeRegistry.RenewNode(ctx, node, info)
}

// NextNodeEpoch 门控点：布防后报错（存活节拍的取号失败形态——registerSelf
// 取号失败即返回、成员键停写，节点对成员视图不可见；失败经 SelfFence 记
// 账推进止损时钟，与写失败同一防线、同一阈值）。
func (g *membershipGate) NextNodeEpoch(ctx context.Context) (uint64, error) {
	if g.epochFail.Load() {
		return 0, fmt.Errorf("testkit: injected node epoch allocation failure")
	}
	return g.NodeRegistry.NextNodeEpoch(ctx)
}

// NodeAddrResolver 透传成员键地址解析（node 装配的解析能力探测面）。
func (g *membershipGate) NodeAddrResolver() func(context.Context, rans.NodeID) (string, error) {
	type resolverDir interface {
		NodeAddrResolver() func(context.Context, rans.NodeID) (string, error)
	}
	if rd, ok := g.Directory.(resolverDir); ok {
		return rd.NodeAddrResolver()
	}
	return nil
}

// FakeDeath 注入节点 i（0 基）假死：布防其成员键写门控（liveness 周期写
// 从此被静默吞掉），待一个心跳周期让布防前的在飞写落定，再删除其成员键
// 与注册总表 field——成员视图即刻判死，且不因 liveness 周期自动复活（节
// 点侧无从感知：写恒成功、SelfFence 不触发）。仅 redis 模式 + WithFakeDeath
// 装配可用。终态注入，无解除原语（-count 重跑经 KeyPrefix 隔离）。
func (c *Cluster) FakeDeath(i int) {
	c.t.Helper()
	if c.redisAddr == "" {
		c.t.Fatal("testkit: FakeDeath requires redis mode (membership keys live in Redis)")
	}
	if i < 0 || i >= len(c.gates) || c.gates[i] == nil {
		c.t.Fatal("testkit: FakeDeath requires WithFakeDeath (per-node membership gate)")
	}
	c.gates[i].arm()
	// 布防先于删键；等一个完整心跳周期（含余量）让布防前的在飞写全部
	// 落定——此后的周期写已被门控吞掉，删除才不会被复活。
	time.Sleep(c.hbInterval + 100*time.Millisecond)
	node := uint16(i + 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli := goredis.NewClient(&goredis.Options{Addr: c.redisAddr, Password: os.Getenv("RANS_TEST_REDIS_PASSWORD")})
	defer cli.Close()
	key := c.redisPrefix + fmt.Sprintf("rans:{reg}:node:%d", node)
	roster := c.redisPrefix + "rans:{reg}:nodes"
	// 双删后校验键缺席（复验兜底：极端调度下仍有残留在飞写即重删）。
	for attempt := 0; attempt < 3; attempt++ {
		if err := cli.Del(ctx, key).Err(); err != nil {
			c.t.Fatalf("testkit: FakeDeath: del liveness key of node %d: %v", node, err)
		}
		if err := cli.HDel(ctx, roster, strconv.Itoa(int(node))).Err(); err != nil {
			c.t.Fatalf("testkit: FakeDeath: hdel roster field of node %d: %v", node, err)
		}
		n, err := cli.Exists(ctx, key).Result()
		if err != nil {
			c.t.Fatalf("testkit: FakeDeath: check liveness key of node %d: %v", node, err)
		}
		if n == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("testkit: FakeDeath: member key of node %d keeps resurrecting", node)
}
