// 能力接口面：基础 Directory（directory.go，合并端口：注册/注销/查找/换锁）
// 之上叠加节点成员面 NodeRegistry（注册总表 + 判活键 + 聚合视图/代际/
// 地址解析），以及激活与路由共用的条目有效性判定 OwnerAlive。能力可选：
// 未注入成员面的目录（inmem 单机形态）仅实现基础 Directory。

package directory

import (
	"context"
	"errors"
	"time"

	"github.com/cyinx/rans_golang"
)

// OwnerAlive 成员代际有效性判定（H13 决策 20 + 票 15 严格化）：条目有效
// ⟺ owner(node, epoch) 在成员视图存活**且条目代际非零**——目录实现
// NodeRegistry 即查（存活且 epoch 一致）；epoch-0 条目在 registry 形态
// 是遗留形态（票 14 起取号同步化不再产生）：判死→可接管（分支 3 CAS 自
// 愈）。读错误保守返回 true（err 无法确认成员键状态，判死会让瞬时故障
// 触发 CAS 接管与缓存失效，真死由发送失败/重试环兜底）；键不在/已过期
// 才判死；无成员面的目录保守返回 true（不可判死，维持既有转发语义）。
// 激活接管闸门与本判定共用。提示条目（放置提示/迁移提示缓存，epoch=0
// 是「未知」语义）不得经本面判定——用 NodeRegistry.NodeEpoch 的存活面。
func OwnerAlive(ctx context.Context, dir Directory, o Owner) bool {
	reg, ok := dir.(NodeRegistry)
	if !ok {
		return true
	}
	if o.Epoch == 0 {
		return false // registry 形态遗留 epoch-0 条目：判死可接管（票 15）
	}
	ep, alive, err := reg.NodeEpoch(ctx, o.Node)
	if err != nil {
		return true
	}
	if !alive {
		return false
	}
	return ep == o.Epoch
}

// ErrNotOwner 是 Transfer 归属校验失败的哨兵（条目在但归属不符——含墓碑；
// 条目不存在返回 (false, zero, nil)，见 directory.Directory.Transfer 的
// 返回值表）。
var ErrNotOwner = errors.New("rans: register: ownership mismatch")

// NodeRegistry 是 **Redis Registry（节点注册表）** 的接口面（术语与键方案
// 裁定出处：docs/decisions/2026-10-02-node-membership-key.md——节点成员表
// 与实体目录 Directory 严格分离）。键方案两层：
//
//	总表   <p>rans:nodes          Hash：field=节点号，value=续命时间戳——枚举面
//	                              （HGETALL 一条命令拿候选集，与 keyspace
//	                              规模解耦——SCAN 要遍历全库桶，万级实体键
//	                              下是百次往返级且会击穿放置的 1s 查询超时）
//	判活键 <p>rans:node:<nodeId>  String：value=JSON，SET PX——**存活的唯一
//	                              权威**（服务端单时钟 TTL，不依赖读方时钟）
//
// 判活键值格式（决策出处：docs/decisions/2026-10-05-node-key-kinds.md）：
//
//	{"addr":"127.0.0.1:1","kinds":["workspace","greet"]},"draining":true,"epoch":7}
//
// addr 即监听地址；kinds 是节点声明承载的实体 kind 集（kind 感知放置的
// 过滤依据），nil/空数组 = 纯心跳节点（只在 NodeIDs 全集，不进任何
// NodesByKind 视图）；draining 是排空标记（false 缺省不写——JSON 增字段
// 对不识别它的旧读端透明）；epoch 是节点代际（NodeInfo.Epoch，0 缺省不写
// ——未接代际面的调用方写法，读回 0）。旧值（裸 addr 字符串）不解析不兼
// 容——非 JSON 值在读面报错（不 panic、不静默跳过；不做混跑，双端同版本）。
//
// 视图语义：draining 节点退出三个聚合视图（NodeIDs/NodeAddrs/NodesByKind
// 一致过滤——放置/迁移不再选中排空节点），单点解析 NodeAddr 不过滤
// （在途请求的应答与转发照常寻址回程）。
//
// 双写容忍撕裂（总表是超集）：判活键活 ⟺ 在视图；总表 field 漂移由读面
// 逐个探活收敛 + 惰性清理死 field。Redis Cluster 兼容性是键方案的硬约束：
// 全部操作为单键命令（HSET/HGETALL/HDEL、SET PX/GET/DEL），无多键 Lua、
// 无跨键事务，双写 pipeline 两键散布不同 slot 无害。值改 JSON 只是同一
// String 键的 payload 编码变化，命令面与 {reg} hash tag 零变更。
//
// 写入方：membership.Liveness 每周期无条件 RegisterNode(self)
// （零实体也写、启动即写一次——冷启动窗口消除；SetDraining 置位即 kick
// 一次即时刷新）；消费方：NodeIDs 喂 membership.View（放置/迁移候选）、
// NodeAddr 派生 rpc.Config.Resolve 的注入形态（redis 包提供
// NodeAddrResolver 构造器，依赖方向不破）、NodesByKind 供 kind 感知放置
// 按形态过滤候选。
// 契约由 contract.RunNodeMembershipSuite 锁定。并发约定同 Directory（阻塞
// 至 IO 或 ctx 结束、并发安全、可在实体 Handle 内调用）。
type NodeRegistry interface {
	// NextNodeEpoch 取节点代际：{reg} 键族全局计数器单调取号（redis 形态
	// HINCRBY、无 TTL——跨节点重启与优雅停机存续；inmem 形态进程内计数
	// 器）。节点启动（首写成员键前）调用恰一次并全程携带——同一存活期的
	// 每次 RegisterNode 携带同一值；判活键死亡/注销后的再注册必得严格更
	// 大的代际（「同节点号重启 → epoch 递增 → 旧条目自然失效」的前提，
	// H13 成员代际裁定）。
	NextNodeEpoch(ctx context.Context) (uint64, error)

	// MemberTTL 判活键存活期（Liveness 自围栏缺省推导基准；无 TTL 语义
	// 的实现返回 0）。
	MemberTTL() time.Duration

	// UnregisterNode 成员注销：epoch ≠ 0 时仅当键内代际 == epoch 才删
	//（守卫注销——迟到停机不得误删已让位者的键；不匹配为 no-op）；0 =
	// 无条件（现语义）。删判活键 + 总表 field（即时退出成员视图，优雅
	// 停机停序用）。实体条目不清扫——残留由死 owner 自愈承接。
	UnregisterNode(ctx context.Context, node rans.NodeID, epoch uint64) error

	// RegisterNode 无条件写/续期 node 的注册（双写：总表登记 + 判活键
	// SET PX）：值=JSON{addr,kinds,draining,epoch}（nil kinds 规范化为空
	// 数组，draining/epoch=0 缺省不写），过期阈值与成员键同源（实现自身
	// 的 HeartbeatTTL 配置）。幂等可重复调用（覆盖），重注册即地址/
	// kinds/draining/epoch 更新。首写与换代后的重建用它——存活节拍的
	// 周期续期走 RenewNode（比对裁定，不复活）。
	RegisterNode(ctx context.Context, node rans.NodeID, info NodeInfo) error

	// RenewNode 比对续期 node 的成员键：键缺席 → RenewAbsent 且不重建
	//（复活须走 RegisterNode 重取代际）；键内代际 > info.Epoch →
	// RenewSuperseded 且值不动（本进程已被顶替）；否则（相等或更小）以
	// info 覆写并续 TTL → RenewRenewed，总表照旧双写（容忍撕裂）。
	RenewNode(ctx context.Context, node rans.NodeID, info NodeInfo) (RenewResult, error)

	// NodeEpoch 读 node 的当前代际（存活才返回）：判活键在 → 解析值 JSON
	// 返回 (epoch, true, nil)；不在/已过期 → (0, false, nil)；值非合法
	// JSON → 报错不 panic（与 NodeAddr 同纪律）。不过滤 draining（单点查
	// 询面——排空节点仍是存活节点）。成员视图的 node→epoch 查询面：目录
	// 条目有效性按调用方内存判定（条目有效 ⟺ owner(node, epoch) 在本端
	// 成员视图存活——H13 成员代际裁定，Redis 单键 Lua 不跨 slot 查成员键）。
	NodeEpoch(ctx context.Context, node rans.NodeID) (uint64, bool, error)

	// NodeAddr 读 node 的成员地址（不过滤 draining——在途应答照常回程）：
	// 判活键在 → 解析值 JSON 取 (addr, true, nil)；不在/已过期 →
	// ("", false, nil)；值存在但不是合法的 {addr,kinds} JSON（含旧版裸
	// addr 值）→ 报错不 panic（不做混跑，坏值只可能来自版本混配或外力
	// 损坏，响亮失败优于静默跳过）。
	NodeAddr(ctx context.Context, node rans.NodeID) (string, bool, error)

	// NodeIDs 返回成员视图当前节点集（升序去重）：总表枚举 + 逐个判活
	// 键探活收敛（总表漂移不出现在视图）；draining 节点不出现（放置/
	// 迁移候选面排空即让位）。固定 2-3 次往返，与 keyspace 规模无关。
	NodeIDs(ctx context.Context) ([]rans.NodeID, error)

	// NodeAddrs 返回成员视图全量快照（节点号→监听地址）：探活语义同
	// NodeIDs（draining 节点不出现，两视图一致），一次拿到「谁活 + 在哪」。
	// 返回 map 归调用方所有。
	NodeAddrs(ctx context.Context) (map[rans.NodeID]string, error)

	// Nodes 返回存活节点全量信息（含 draining——启动代际检查不过滤排空
	// 节点：排空中的旧版本节点同样阻断混版本启动）：判活收敛内核的完整
	// 视图（NodeIDs/NodeAddrs/NodesByKind 的数据源）。返回 map 归调用方。
	Nodes(ctx context.Context) (map[rans.NodeID]NodeInfo, error)
	// NodesByKind 返回声明承载 kind 的存活节点集（升序去重，kind 感知
	// 放置的候选面）：探活内核收敛后按 kinds 精确匹配过滤，draining 节点
	// 不出现。空 kinds 节点不进任何 kind 视图（纯心跳节点不被业务放置
	// 选中）；无人声明该 kind 返回空集（消费方据此走 ErrNoEligibleTarget
	// 面，非错误）。
	NodesByKind(ctx context.Context, kind string) ([]rans.NodeID, error)
}

// RenewResult 比对续期（RenewNode）结果三态。
type RenewResult int

const (
	// RenewRenewed 续期成功：以调用方值覆写并延长 TTL。
	RenewRenewed RenewResult = iota
	// RenewAbsent 判活键缺席：不重建——复活走 RegisterNode 重取代际。
	RenewAbsent
	// RenewSuperseded 键内代际更大：本进程已让位，值不动。
	RenewSuperseded
)

// NodeInfo 是判活键值的承载四元组（RegisterNode 入参）：监听地址 + 声明
// 承载的 kind 集 + 排空标记 + 节点代际。Draining 置位表示节点正在
// 排空——聚合视图（NodeIDs/NodeAddrs/NodesByKind）不再返回它（放置让位），
// 单点解析 NodeAddr/NodeEpoch 不受影响（在途应答照常回程）。
type NodeInfo struct {
	Addr     string
	Kinds    []string
	Draining bool

	// Epoch 是节点代际：NextNodeEpoch 启动取号的单调值，同一存活期的每次
	// 注册携带同一值（membership.Liveness 取号一次全程携带）。0 = 未携带
	// （未接代际面的调用方写法；值上缺省不写，NodeEpoch 读回 0——代际有
	// 效性判定只对非零代际有意义）。
	Epoch uint64
	// ProtoMin/ProtoMax 本节点握手协议版本区间（wire.HandshakeVersion*）：
	// node.Start 启动代际检查据此拒绝混版本集群（缺字段 0/0 恒不相交——
	// 断点前版本节点按不兼容处理）。
	ProtoMin uint16
	ProtoMax uint16
}
