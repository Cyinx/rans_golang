package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/log"
	goredis "github.com/redis/go-redis/v9"
)

// nodeValue 是判活键的值（决策出处：docs/decisions/2026-10-05-node-key-kinds.md）：
// {"addr":...,"kinds":[...],"draining":true,"epoch":7}。kinds 是节点声明承载的
// 实体 kind 集（kind 感知放置的过滤依据），nil 规范化为空数组——纯心跳节点的
// 合法形态（只进 NodeIDs 全集，不进任何 NodesByKind 视图）。draining 是
// 排空标记：false 缺省不写（omitempty——JSON 增字段，不识别它的旧读端
// 透明）；true 时聚合视图（NodeIDs/NodeAddrs/NodesByKind）过滤、单点
// NodeAddr 不过滤（过滤裁定在接口层，见 directory.NodeRegistry）。epoch 是
// 节点代际：0 缺省不写（未接代际面的调用方写法，读回 0）。
type nodeValue struct {
	Addr     string   `json:"addr"`
	Kinds    []string `json:"kinds"`
	Draining bool     `json:"draining,omitempty"`
	Epoch    uint64   `json:"epoch,omitempty"`
	ProtoMin uint16   `json:"pmin,omitempty"`
	ProtoMax uint16   `json:"pmax,omitempty"`
}

// encodeNodeValue 编码判活键值：nil kinds 归一为 []（线上格式恒为 JSON
// 数组，redis-cli 肉眼可辨；解码侧 null/[]/缺字段均容错为 nil）；
// draining=false、epoch=0 缺省不写。
func encodeNodeValue(info directory.NodeInfo) (string, error) {
	kinds := info.Kinds
	if kinds == nil {
		kinds = []string{}
	}
	b, err := json.Marshal(nodeValue{
		Addr:     info.Addr,
		Kinds:    kinds,
		Draining: info.Draining,
		Epoch:    info.Epoch,
		ProtoMin: info.ProtoMin,
		ProtoMax: info.ProtoMax,
	})
	if err != nil {
		return "", fmt.Errorf("rans: register: encode node value: %w", err)
	}
	return string(b), nil
}

// decodeNodeValue 解码判活键值。非 JSON / 结构不符（含旧版裸 addr 值）/
// addr 缺失或空（null、{}、缺 addr 字段——addr 是值的必填成分，空地址入
// 视图只会产出必然拨号失败的 Resolve 候选）报错：
// 不做混跑（双端同版本），坏值只可能来自版本混配或外力损坏，响亮失败
// 优于静默跳过——静默跳过会把「集群混着旧版本」伪装成节点消失/容量缩水，
// 排障方向被带偏。
func decodeNodeValue(node rans.NodeID, val string) (nodeValue, error) {
	var v nodeValue
	if err := json.Unmarshal([]byte(val), &v); err != nil {
		return nodeValue{}, fmt.Errorf("rans: membership: node %d key holds a malformed value %q (want JSON {addr,kinds}; mixed old/new cluster versions are unsupported)", node, val)
	}
	if v.Addr == "" {
		return nodeValue{}, fmt.Errorf("rans: membership: node %d key holds a malformed value %q (addr must not be empty)", node, val)
	}
	return v, nil
}

// RegisterNode 写/续期 node 的注册（directory.NodeRegistry 契约）：pipeline
// 双写——判活键 SET JSON{addr,kinds,draining,epoch} PX
// heartbeatTTL（存活的唯一权威，服务端单时钟）+ 总表 HSET（枚举登记）。
// 无条件覆盖，幂等可重复调用（重注册即地址/kinds/draining/epoch 更新；
// info.Epoch 由调用方携带——Liveness 取号一次全程传同一值，注册面不二次
// 取号）。过期阈值与逐实体心跳键同源同值（节点死亡后 ~heartbeatTTL 退出
// 视图）。双写容忍撕裂：两键不同 slot（Redis Cluster 下 pipeline 分发各自
// 节点）非原子，总表为超集、读面探活收敛（见 liveNodes）。写入方是
// membership.Liveness 的每周期动作（零实体节点照写）。
func (d *Directory) RegisterNode(ctx context.Context, node rans.NodeID, info directory.NodeInfo) error {
	val, err := encodeNodeValue(info)
	if err != nil {
		return err
	}
	pipe := d.cli.Pipeline()
	pipe.Set(ctx, d.nodeKey(node), val, d.heartbeatTTL)
	// 总表 value=续命时间戳（写方时钟，unix ms）：判活初筛
	// 面，为未来判活三件套预留——当前判活权威仍是判活键的服务端 TTL，
	// 时间戳仅记录（读面不消费）。地址与 kinds 单载于节点键。
	pipe.HSet(ctx, d.rosterKey(), strconv.Itoa(int(node)),
		strconv.FormatInt(time.Now().UnixMilli(), 10))
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	return nil
}

// RenewNode 比对续期（directory.NodeRegistry 契约）：判活键上的单键
// luaRenew——键缺席 → RenewAbsent 且不重建；键内代际更大 →
// RenewSuperseded 且值不动；否则覆写并续 TTL → RenewRenewed，总表照旧
// 双写（撕裂容忍面同 RegisterNode）。
func (d *Directory) RenewNode(ctx context.Context, node rans.NodeID, info directory.NodeInfo) (directory.RenewResult, error) {
	val, err := encodeNodeValue(info)
	if err != nil {
		return 0, err
	}
	res, err := scriptRenew.Run(ctx, d.cli,
		[]string{d.nodeKey(node)},
		[]interface{}{val, d.heartbeatTTL.Milliseconds(), info.Epoch},
	).Slice()
	if err != nil {
		return 0, err
	}
	if len(res) != 1 {
		return 0, fmt.Errorf("rans: renew: script reply %v, want 1-element array", res)
	}
	switch v := res[0].(type) {
	case int64:
		switch v {
		case 1:
			// Renewed：总表照旧双写（撕裂容忍面同 RegisterNode）。
			return directory.RenewRenewed, d.cli.HSet(ctx, d.rosterKey(),
				strconv.Itoa(int(node)),
				strconv.FormatInt(time.Now().UnixMilli(), 10)).Err()
		case -1:
			return directory.RenewSuperseded, nil
		default:
			return directory.RenewAbsent, nil
		}
	default:
		return 0, fmt.Errorf("rans: renew: script reply %v, want integer element", res)
	}
}

// NextNodeEpoch 取节点代际（directory.NodeRegistry 契约）：{reg} 键族全局
// 计数器 HINCRBY 单调取号（epochKey，无 TTL——跨节点重启与优雅停机存续）。
// 节点启动（首写成员键前）调用恰一次并全程携带——写入方是 membership.
// Liveness 的启动取号，每次 RegisterNode 携带同一值。
func (d *Directory) NextNodeEpoch(ctx context.Context) (uint64, error) {
	n, err := d.cli.HIncrBy(ctx, d.epochKey(), epochCounterField, 1).Result()
	if err != nil {
		return 0, err
	}
	return uint64(n), nil
}

// NodeEpoch 读 node 的当前代际（directory.NodeRegistry 契约；**不过滤
// draining**——单点查询面，排空节点仍是存活节点）：判活键在 → 解析值
// JSON 取 (epoch, true, nil)（未携带代际的值读回 0）；键不在/已过期 →
// (0, false, nil)；值非合法 JSON → 报错不 panic（与 NodeAddr 同纪律，见
// decodeNodeValue）。
func (d *Directory) NodeEpoch(ctx context.Context, node rans.NodeID) (uint64, bool, error) {
	val, err := d.cli.Get(ctx, d.nodeKey(node)).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return 0, false, nil
		}
		return 0, false, err
	}
	v, err := decodeNodeValue(node, val)
	if err != nil {
		return 0, false, err
	}
	return v.Epoch, true, nil
}

// NodeAddr 读 node 的成员地址（directory.NodeRegistry 契约；**不过滤
// draining**——单点解析面，在途请求的应答与转发照常寻址回程）：
// 键在 → 解析 JSON{addr,kinds,draining} 取 (addr, true, nil)；键不在/已
// 过期 → ("", false, nil)；值非合法 JSON（含旧版裸 addr 值）→ 报错不
// panic（不做混跑，见 decodeNodeValue）。地址发现的数据源（NodeAddrResolver
// 的后端）。
func (d *Directory) NodeAddr(ctx context.Context, node rans.NodeID) (string, bool, error) {
	val, err := d.cli.Get(ctx, d.nodeKey(node)).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return "", false, nil
		}
		return "", false, err
	}
	v, err := decodeNodeValue(node, val)
	if err != nil {
		return "", false, err
	}
	return v.Addr, true, nil
}

// liveNodes 是 registry 读面的探活内核：总表 HGETALL 拿候选集
// （一条命令，与 keyspace 规模解耦——SCAN 要遍历全库桶，实体键万级时
// 是千次往返级且会击穿放置路径的 1s 查询超时）→ pipeline 逐个 GET
// 判活键收敛（nil 即死；活的以判活键当前值为准——它是权威）→ 死 field
// 惰性 HDEL（总表收缩回存活集；幂等，多清理者并发无害）。固定 2-3 次
// 往返。非数值 field（漂移）跳过且同样惰性清理。判活键值解析失败
// （非 JSON，含旧版裸 addr）报错——与 NodeAddr 同纪律，混跑/损坏响亮
// 失败而非静默缩视图。draining 节点**不**在本内核过滤——返回全量存活集
// （含 draining），过滤裁定归三个聚合视图（NodeIDs/NodeAddrs/NodesByKind
// 统一口径）；单点 NodeAddr 消费同一内核语义且不过滤。
func (d *Directory) liveNodes(ctx context.Context) (map[rans.NodeID]nodeValue, error) {
	fields, err := d.cli.HGetAll(ctx, d.rosterKey()).Result()
	if err != nil {
		return nil, err
	}
	type probe struct {
		node rans.NodeID
		key  string
	}
	probes := make([]probe, 0, len(fields))
	var deadFields []string
	for field := range fields {
		n, err := strconv.ParseUint(field, 10, 16)
		if err != nil {
			// 漂移 field（节点号段非数值）：不进视图，直接进惰性清理集
			deadFields = append(deadFields, field)
			continue
		}
		probes = append(probes, probe{node: rans.NodeID(n), key: d.nodeKey(rans.NodeID(n))})
	}
	if len(probes) == 0 {
		return map[rans.NodeID]nodeValue{}, nil
	}
	pipe := d.cli.Pipeline()
	cmds := make([]*goredis.StringCmd, len(probes))
	for i, p := range probes {
		cmds[i] = pipe.Get(ctx, p.key)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		// pipeline 整体错误（连接级）；单键 miss 体现为各 cmd 的 Nil，不在此列
		return nil, err
	}
	live := make(map[rans.NodeID]nodeValue, len(probes))
	for i, cmd := range cmds {
		val, err := cmd.Result()
		if errors.Is(err, goredis.Nil) {
			deadFields = append(deadFields, strconv.Itoa(int(probes[i].node)))
			continue
		}
		if err != nil {
			return nil, err
		}
		v, err := decodeNodeValue(probes[i].node, val)
		if err != nil {
			return nil, err
		}
		live[probes[i].node] = v
	}
	if len(deadFields) > 0 {
		pipe := d.cli.Pipeline()
		for _, f := range deadFields {
			pipe.HDel(ctx, d.rosterKey(), f)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			// 惰性清理失败不影响本轮视图（下轮再清）；总表超集语义容忍
			log.Debug("rans/directory/redis: roster lazy-clean failed (retry next read)",
				"fields", len(deadFields), "err", err)
		}
	}
	return live, nil
}

// Nodes 存活节点全量信息（directory.NodeRegistry 契约；含 draining）：
// liveNodes 判活收敛内核直出。
func (d *Directory) Nodes(ctx context.Context) (map[rans.NodeID]directory.NodeInfo, error) {
	live, err := d.liveNodes(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[rans.NodeID]directory.NodeInfo, len(live))
	for n, v := range live {
		out[n] = directory.NodeInfo{
			Addr: v.Addr, Kinds: v.Kinds, Draining: v.Draining,
			Epoch: v.Epoch, ProtoMin: v.ProtoMin, ProtoMax: v.ProtoMax,
		}
	}
	return out, nil
}

// NodeIDs 返回成员视图当前节点集（升序去重，directory.NodeRegistry）：
// 探活内核收敛后的存活集，draining 节点不出现（放置/迁移候选面排空即让位
// ；与 NodeAddrs/NodesByKind 同一过滤口径，视图一致）。
// 这是存活集查询函数（placement.RandomPlacement 候选源、membership.View
// 数据源同形）的 redis 侧实现。
func (d *Directory) NodeIDs(ctx context.Context) ([]rans.NodeID, error) {
	live, err := d.liveNodes(ctx)
	if err != nil {
		return nil, err
	}
	nodes := make([]rans.NodeID, 0, len(live))
	for n, v := range live {
		if v.Draining {
			continue
		}
		nodes = append(nodes, n)
	}
	slices.Sort(nodes)
	return nodes, nil
}

// NodeAddrs 返回成员视图全量快照（节点号→监听地址）：探活内核一次拿到
// 「谁活 + 在哪」，draining 节点不出现（与 NodeIDs 同一过滤口径）。返回
// map 归调用方所有。
func (d *Directory) NodeAddrs(ctx context.Context) (map[rans.NodeID]string, error) {
	live, err := d.liveNodes(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[rans.NodeID]string, len(live))
	for n, v := range live {
		if v.Draining {
			continue
		}
		out[n] = v.Addr
	}
	return out, nil
}

// NodesByKind 返回声明承载 kind 的存活节点集（升序去重，directory.
// NodeRegistry 契约；kind 感知放置的候选面）：探活内核收敛后按
// kinds 精确匹配过滤，draining 节点不出现。空 kinds
// 节点不进任何 kind 视图（纯心跳节点不被业务放置选中）；无人声明该 kind
// 返回空集（ErrNoEligibleTarget 面，非错误）。
func (d *Directory) NodesByKind(ctx context.Context, kind string) ([]rans.NodeID, error) {
	live, err := d.liveNodes(ctx)
	if err != nil {
		return nil, err
	}
	nodes := make([]rans.NodeID, 0, len(live))
	for n, v := range live {
		if v.Draining {
			continue
		}
		if slices.Contains(v.Kinds, kind) {
			nodes = append(nodes, n)
		}
	}
	slices.Sort(nodes)
	return nodes, nil
}

// NodeAddrResolver 返回节点号→地址解析函数（rpc.Config.Resolve 的
// redis 目录派生实现——依赖方向不破，rpc
// 不 import 本包，装配方接线）。读成员键单键 GET：Resolve 只在
// tcp.Manager 懒建连接时调用（每节点约一次，成本可忽略）。节点不在
// 成员视图（未注册/已过期/已注销）返回错误——由上游发送重试环接手。
// ctx 为调用方传入（解析尊重调用预算；goredis 另有
// 拨号/读超时兜底）。不阻塞长于调用预算；并发安全。
func (d *Directory) NodeAddrResolver() func(ctx context.Context, n rans.NodeID) (string, error) {
	return func(ctx context.Context, node rans.NodeID) (string, error) {
		addr, ok, err := d.NodeAddr(ctx, node)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", fmt.Errorf("rans: resolve: node %d not in membership view (unregistered or expired)", node)
		}
		return addr, nil
	}
}
