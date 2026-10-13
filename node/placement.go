package node

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/placement"
)

// Placement 为新激活或迁移选择节点（约定 spec §5.4 放置——消费方唯一需要
// 自定义的策略面）。
//
// candidates 是声明承载该 kind 且存活的节点（按 NodeID 升序，已排除
// self；排空让位的候选过滤随成员视图语义演进，当前候选源未携带）。首次
// 激活与迁移共用本策略（迁移选点带 id.Kind，经 placementBridge.PickForKind
// 过滤候选）。自定义策略不需要也不能访问成员视图。
//
// 返回 (0, false) 表示放弃选点：首次激活回落本地激活（本节点声明承载该
// kind 时）或以 Unavailable+NoEligibleNode 终止；迁移失败。
type Placement interface {
	Pick(kind string, self rans.NodeID, candidates []rans.NodeID) (rans.NodeID, bool)
}

// RandomPlacement 缺省放置：在候选节点中随机（nil Placement 时由 Node 装
// 配，语义与 spec §5.4「nil → 在候选节点中随机」一致）。
type RandomPlacement struct {
	mu sync.Mutex
	r  *rand.Rand // rand.Rand 非并发安全，逐次选取经 mu 串行化
}

// NewRandomPlacement 构造（时间种子；测试分布统计可用 NewSeededRandomPlacement）。
func NewRandomPlacement() *RandomPlacement {
	return &RandomPlacement{r: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

// NewSeededRandomPlacement 用固定种子构造（可复现形态）。
func NewSeededRandomPlacement(seed int64) *RandomPlacement {
	return &RandomPlacement{r: rand.New(rand.NewSource(seed))}
}

// Pick 从候选中随机选一个；空候选 → (0, false)。不阻塞；并发安全
// （rand 经互斥串行化）；由运行时在放置点调用（消费方自定义策略同面）。
func (p *RandomPlacement) Pick(_ string, _ rans.NodeID, candidates []rans.NodeID) (rans.NodeID, bool) {
	if len(candidates) == 0 {
		return 0, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return candidates[p.r.Intn(len(candidates))], true
}

// 编译期锚：缺省放置满足策略接口。
var _ Placement = (*RandomPlacement)(nil)

// placementBridge node.Placement 策略到内部路由/迁移面的桥接（依赖单向：
// node 消费 internal/rpc 与 internal/placement 的接口，二者不见 node）。
//
//   - 对 internal/rpc（路由梯子 miss 收口）：实现 Placement + KindPlacement
//     鸭子接口——候选集由本桥按 kind 经 KindScanner 计算后传入策略；
//   - 对 internal/activation（迁移选点）：经 mgr.SetPlacement 注入
//     PickForKind（kind 维候选过滤——迁移与首激活共用同一策略面）；
//   - 候选查询复用 placement.QueryCandidates（选点内核候选计算步，
//     pickTimeout 单点在 internal/placement）；随机取一步留给用户策略。
type placementBridge struct {
	policy  Placement             // nil → RandomPlacement（spec：nil → 随机）
	scanner placement.KindScanner // nil → 恒空候选（inmem 等无成员视图形态）
}

// pickForKind kind 维候选计算 + 策略选点。scanner 缺席 → 空候选交策略自
// 行收口（缺省随机与 (0,false) 同义）；候选查询失败收敛 (0, false)。
func (b *placementBridge) pickForKind(kind string, exclude rans.NodeID) (rans.NodeID, bool) {
	policy := b.policy
	if policy == nil {
		policy = defaultRandomPlacement
	}
	if b.scanner == nil {
		return policy.Pick(kind, exclude, nil)
	}
	candidates, ok := placement.QueryCandidates(func(ctx context.Context) ([]rans.NodeID, error) {
		return b.scanner.NodesByKind(ctx, kind)
	}, exclude)
	if !ok {
		return 0, false
	}
	return policy.Pick(kind, exclude, candidates)
}

// pickAll 全集候选选点（kind 维不可用的兜底面：迁移选点与 rpc.Placement）。
func (b *placementBridge) pickAll(exclude rans.NodeID) (rans.NodeID, bool) {
	policy := b.policy
	if policy == nil {
		policy = defaultRandomPlacement
	}
	if b.scanner == nil {
		return policy.Pick("", exclude, nil)
	}
	candidates, ok := placement.QueryCandidates(func(ctx context.Context) ([]rans.NodeID, error) {
		return b.scanner.NodeIDs(ctx)
	}, exclude)
	if !ok {
		return 0, false
	}
	return policy.Pick("", exclude, candidates)
}

// Pick 满足 internal/rpc.Placement（全节点兜底面——桥同时实现 KindPlacement，
// 路由梯子优先走 kind 维，本方法仅在对端未探测到扩展面时到达）。
func (b *placementBridge) Pick(exclude rans.NodeID) (rans.NodeID, bool) {
	return b.pickAll(exclude)
}

// PickForKind 满足 internal/rpc.KindPlacement：候选只含声明承载 kind 的存
// 活节点（纯接入节点不进业务 kind 候选）。
func (b *placementBridge) PickForKind(kind string, exclude rans.NodeID) (rans.NodeID, bool) {
	return b.pickForKind(kind, exclude)
}

// defaultRandomPlacement 缺省策略单例（RandomPlacement 并发安全）。
var defaultRandomPlacement = NewRandomPlacement()
