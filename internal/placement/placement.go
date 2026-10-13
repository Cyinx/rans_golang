// Package placement 提供放置策略 v1（规格 §12 放置「存活节点 × 策略
// [缺省 Random]」）。
//
// 数据源经注入解耦：KindScanner（kind 感知两面视图）与候选查询函数
// （RandomPlacement）由消费方注入——redis 侧实现是
// directory/redis.Directory（节点级成员键 `rans:node:<id>`，「键在=节点
// 活」），测试注入
// 内存假——placement 只向下依赖（不导入 rpc/routing；rpc.Placement 由
// 鸭子类型满足，编译期锚点在 rpc/placement_test.go 与本包测试）。
package placement

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/cyinx/rans_golang"
)

// pickTimeout 是选点内核数据源查询的兜底期限：放置在调用方的关键路径
// （目录 miss 后选目标），不能被慢数据源无限挂住；到期视同无候选。
const pickTimeout = time.Second

// KindScanner 是 kind 感知放置的数据源（spec §10）：全节点成员
// 视图（NodeIDs——Pick 的候选面）+ per-kind 过滤视图（NodesByKind——
// PickForKind 的候选面）。redis 侧实现是 directory/redis.Directory（经
// directory.NodeRegistry 提供两面）；测试注入内存假。两面共用同一探活
// 内核，语义一致（空 kinds 节点只在全集，不进任何 kind 视图）。
type KindScanner interface {
	// NodeIDs 返回成员视图当前节点集（升序去重）。
	NodeIDs(ctx context.Context) ([]rans.NodeID, error)
	// NodesByKind 返回声明承载 kind 的存活节点集（升序去重）。
	NodesByKind(ctx context.Context, kind string) ([]rans.NodeID, error)
}

// QueryCandidates 选点内核候选计算步：兜底超时查询 + 排除 exclude；失败 → (nil, false)。
func QueryCandidates(query func(context.Context) ([]rans.NodeID, error), exclude rans.NodeID) ([]rans.NodeID, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), pickTimeout) // 例外②：选点数据源查询的兜底预算（pickTimeout 集中定义；Placement 接口无 ctx 形参——spec §5.4 固定形状，预算即目录 IO 的隔离界面）
	defer cancel()
	nodes, err := query(ctx)
	if err != nil {
		return nil, false
	}
	candidates := make([]rans.NodeID, 0, len(nodes))
	for _, n := range nodes {
		if n != exclude {
			candidates = append(candidates, n)
		}
	}
	return candidates, true
}

// picker 选点随机源：rand.Rand 非并发安全，Intn 经 mu 串行化。
type picker struct {
	mu sync.Mutex
	r  *rand.Rand
}

// newPicker 时间种子构造。
func newPicker() *picker {
	return &picker{r: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

// newSeededPicker 固定种子构造：测试分布统计的可复现形态。
func newSeededPicker(seed int64) *picker {
	return &picker{r: rand.New(rand.NewSource(seed))}
}

// pick 选取内核：QueryCandidates 候选计算 → 随机取一；失败或空一律 (0, false)。
func (p *picker) pick(query func(context.Context) ([]rans.NodeID, error), exclude rans.NodeID) (rans.NodeID, bool) {
	candidates, ok := QueryCandidates(query, exclude)
	if !ok || len(candidates) == 0 {
		return 0, false
	}
	p.mu.Lock()
	i := p.r.Intn(len(candidates))
	p.mu.Unlock()
	return candidates[i], true
}

// KindPlacement 是 kind 感知放置策略（spec §10「放置按 kind 过滤
// 候选」）：同时实现 rpc.Placement（Pick——全节点 NodeIDs 随机，消费方
// 不区分 kind 时的兜底面）与 rpc.KindPlacement（PickForKind——
// NodesByKind 过滤，未承载该 kind 的节点不进候选）。经鸭子类型满足两个
// 消费接口（placement 不导入 rpc，依赖单向向下）。空候选或数据源失败一律
// 收敛为 (0, false)——与 RandomPlacement 同纪律（错误本体在数据源层响亮
// 传播，接口无错误通道）。并发安全。
type KindPlacement struct {
	scanner KindScanner
	rng     *picker
}

// NewKindPlacement 构造（时间种子）。scanner 传 nil 视同恒空集。
func NewKindPlacement(scanner KindScanner) *KindPlacement {
	return &KindPlacement{scanner: scanner, rng: newPicker()}
}

// NewSeededKindPlacement 用固定种子构造：测试分布统计的可复现形态。
func NewSeededKindPlacement(scanner KindScanner, seed int64) *KindPlacement {
	return &KindPlacement{scanner: scanner, rng: newSeededPicker(seed)}
}

// Pick 从全节点成员视图（NodeIDs）随机选一个（排除 exclude）——kind 维
// 不参与的兜底面，候选语义与 RandomPlacement 等价。数据源查询带 1s 兜底
// 超时（见 pickTimeout）。
func (p *KindPlacement) Pick(exclude rans.NodeID) (rans.NodeID, bool) {
	if p.scanner == nil {
		return 0, false
	}
	return p.rng.pick(func(ctx context.Context) ([]rans.NodeID, error) {
		return p.scanner.NodeIDs(ctx)
	}, exclude)
}

// PickForKind 从声明承载 kind 的存活节点集（NodesByKind）随机选一个
// （排除 exclude）：纯接入节点（kinds 不含该 kind）永不入候选；无人声明
// 该 kind → (0, false)（调用方记 NoEligibleTarget 保持 Active）。数据源
// 查询带 1s 兜底超时（见 pickTimeout）。
func (p *KindPlacement) PickForKind(kind string, exclude rans.NodeID) (rans.NodeID, bool) {
	if p.scanner == nil {
		return 0, false
	}
	return p.rng.pick(func(ctx context.Context) ([]rans.NodeID, error) {
		return p.scanner.NodesByKind(ctx, kind)
	}, exclude)
}

// RandomPlacement 是放置策略 v1：从存活节点集随机选一个，排除 exclude
// （规格 §12 放置「存活节点 × 策略[缺省 Random]」；§13 迁移「放置目标
// (排除本地;无候选 → NoEligibleTarget)」）。空候选或数据源失败一律收敛为
// (0, false)——调用方按 bool 分流（记 NoEligibleTarget 保持 Active），
// 错误不炸放置路径。并发安全。
type RandomPlacement struct {
	nodes func(ctx context.Context) ([]rans.NodeID, error)
	rng   *picker
}

// NewRandomPlacement 构造（时间种子）。nodes 是候选查询函数（存活节点集
// ——membership.View.AliveNodes / redis Directory.NodeIDs 同形）；传 nil
// 视同恒空集。
func NewRandomPlacement(nodes func(ctx context.Context) ([]rans.NodeID, error)) *RandomPlacement {
	return &RandomPlacement{nodes: nodes, rng: newPicker()}
}

// NewSeededRandomPlacement 用固定种子构造：测试分布统计的可复现形态。
func NewSeededRandomPlacement(nodes func(ctx context.Context) ([]rans.NodeID, error), seed int64) *RandomPlacement {
	return &RandomPlacement{nodes: nodes, rng: newSeededPicker(seed)}
}

// Pick 随机选一个存活节点（排除 exclude）。数据源查询带 1s 兜底超时
// （见 pickTimeout）；exclude=0 不排除任何节点（0 是匿名源哨兵，数据源
// 正常不会含它）。
func (p *RandomPlacement) Pick(exclude rans.NodeID) (rans.NodeID, bool) {
	if p.nodes == nil {
		return 0, false
	}
	return p.rng.pick(p.nodes, exclude)
}
