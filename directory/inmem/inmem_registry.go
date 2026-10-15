// 进程内节点注册表（directory.NodeRegistry 的 inmem 形态）：与 redis 实现同
// 语义的成员面——记录在 ⟺ 节点可见、RegisterNode 续期、聚合视图过滤
// draining、NodesByKind 按声明过滤；判活键等价物是带过期时刻的进程内记录
// （惰性过期：读面收敛，无后台清扫协程），值直接承载 NodeInfo（含节点代
// 际 epoch）。代际取号是进程内单调计数器（进程内实现无需共享存储：注册表
// 实例的存活期即目录服务的存活期，同节点号跨记录死亡再注册必得严格更大
// 的代际）。构造经 NewNodeRegistry；缺省 New()（单机最小形态）不含本面。
package inmem

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

// defaultNodeTTL 节点记录缺省过期阈值（与 redis 实现的缺省 HeartbeatTTL
// 同值——成员面语义同构，过期阈值同源）。
const defaultNodeTTL = 2 * time.Minute

// nodeRecord 是判活键的进程内等价物：成员键值（NodeInfo 原样承载，含代
// 际）+ 过期时刻。记录在且未过期 ⟺ 节点活。
type nodeRecord struct {
	info      directory.NodeInfo
	expiresAt time.Time
}

// registry 进程内节点注册表。并发约定同包内目录实现（互斥串行化）；
// 阻塞面为纯内存操作（立即返回，ctx 不影响结果）。
type registry struct {
	mu    sync.Mutex
	hbTTL time.Duration
	epoch uint64 // 全局代际计数器（进程内单调，跨记录死亡存续）
	nodes map[rans.NodeID]nodeRecord
}

// 编译期断言：成员面能力接口实现。
var _ directory.NodeRegistry = (*registry)(nil)

// NewNodeRegistry 构造进程内节点注册表。hbTTL 是节点记录的过期阈值
// （RegisterNode 续期；≤0 取缺省 2min）。与 redis 形态同语义：记录在 ⟺
// 节点在视图，过期由读面惰性收敛；代际经 NextNodeEpoch 取号、随
// NodeInfo.Epoch 全程携带。
func NewNodeRegistry(hbTTL time.Duration) directory.NodeRegistry {
	if hbTTL <= 0 {
		hbTTL = defaultNodeTTL
	}
	return &registry{
		hbTTL: hbTTL,
		nodes: make(map[rans.NodeID]nodeRecord),
	}
}

// MemberTTL 节点记录过期阈值（RegisterNode/RenewNode 续期）。
func (r *registry) MemberTTL() time.Duration { return r.hbTTL }

// UnregisterNode 成员注销：删除节点记录（即时退出视图）。
// UnregisterNode 成员注销（directory.NodeRegistry 契约）：epoch≠0 时仅当
// 记录代际 == epoch 才删（守卫；不匹配 no-op）；0 = 无条件。即时退出视图。
func (r *registry) UnregisterNode(_ context.Context, node rans.NodeID, epoch uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if epoch != 0 {
		rec, ok := r.liveLocked(node)
		if !ok || rec.info.Epoch != epoch {
			return nil
		}
	}
	delete(r.nodes, node)
	return nil
}

// NextNodeEpoch 取节点代际（directory.NodeRegistry 契约）：进程内计数器
// 单调递增，首个为 1。取号语义与 redis 形态一致——同节点号跨记录死亡
// （过期/注销）再注册必得严格更大的代际；优雅停机不触碰计数器。
func (r *registry) NextNodeEpoch(_ context.Context) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.epoch++
	return r.epoch, nil
}

// RegisterNode 写/续期 node 的注册（directory.NodeRegistry 契约）：覆盖写
// 成员记录并续期（过期时刻 = now + hbTTL），幂等可重复调用。
func (r *registry) RegisterNode(_ context.Context, node rans.NodeID, info directory.NodeInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[node] = nodeRecord{info: info, expiresAt: time.Now().Add(r.hbTTL)}
	return nil
}

// RenewNode 比对续期（directory.NodeRegistry 契约）：记录缺席（含惰性
// 过期）→ RenewAbsent 且不重建；记录代际更大 → RenewSuperseded 值不动；
// 否则覆写并续期 → RenewRenewed。
func (r *registry) RenewNode(_ context.Context, node rans.NodeID, info directory.NodeInfo) (directory.RenewResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.liveLocked(node)
	if !ok {
		return directory.RenewAbsent, nil
	}
	if rec.info.Epoch > info.Epoch {
		return directory.RenewSuperseded, nil
	}
	r.nodes[node] = nodeRecord{info: info, expiresAt: time.Now().Add(r.hbTTL)}
	return directory.RenewRenewed, nil
}

// NodeEpoch 读 node 的当前代际（directory.NodeRegistry 契约；不过滤
// draining——单点查询面）：记录在且未过期 → (epoch, true, nil)；不在/
// 已过期 → (0, false, nil)。
func (r *registry) NodeEpoch(_ context.Context, node rans.NodeID) (uint64, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.liveLocked(node)
	if !ok {
		return 0, false, nil
	}
	return rec.info.Epoch, true, nil
}

// NodeAddr 读 node 的成员地址（directory.NodeRegistry 契约；不过滤
// draining）：记录在且未过期 → (addr, true, nil)；不在/已过期 →
// ("", false, nil)。
func (r *registry) NodeAddr(_ context.Context, node rans.NodeID) (string, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.liveLocked(node)
	if !ok {
		return "", false, nil
	}
	return rec.info.Addr, true, nil
}

// Nodes 存活节点全量信息（directory.NodeRegistry 契约；含 draining）：
// 惰性过期收敛后的完整视图。
func (r *registry) Nodes(_ context.Context) (map[rans.NodeID]directory.NodeInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[rans.NodeID]directory.NodeInfo, len(r.nodes))
	for n, rec := range r.nodes {
		if r.expiredLocked(n, rec) {
			continue
		}
		out[n] = rec.info
	}
	return out, nil
}

// NodeIDs 返回成员视图当前节点集（升序去重，directory.NodeRegistry 契约）：
// 惰性过期收敛后的存活集，draining 节点不出现（与 NodeAddrs/NodesByKind
// 同一过滤口径）。
func (r *registry) NodeIDs(_ context.Context) ([]rans.NodeID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	nodes := make([]rans.NodeID, 0, len(r.nodes))
	for n, rec := range r.nodes {
		if r.expiredLocked(n, rec) || rec.info.Draining {
			continue
		}
		nodes = append(nodes, n)
	}
	slices.Sort(nodes)
	return nodes, nil
}

// NodeAddrs 返回成员视图全量快照（directory.NodeRegistry 契约）：惰性过期
// 收敛 + draining 过滤（与 NodeIDs 同一口径）。返回 map 归调用方所有。
func (r *registry) NodeAddrs(_ context.Context) (map[rans.NodeID]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[rans.NodeID]string, len(r.nodes))
	for n, rec := range r.nodes {
		if r.expiredLocked(n, rec) || rec.info.Draining {
			continue
		}
		out[n] = rec.info.Addr
	}
	return out, nil
}

// NodesByKind 返回声明承载 kind 的存活节点集（directory.NodeRegistry 契约）：
// 惰性过期收敛后按 kinds 精确匹配过滤，draining 节点不出现；空 kinds 节点
// 不进任何 kind 视图。
func (r *registry) NodesByKind(_ context.Context, kind string) ([]rans.NodeID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	nodes := make([]rans.NodeID, 0, len(r.nodes))
	for n, rec := range r.nodes {
		if r.expiredLocked(n, rec) || rec.info.Draining {
			continue
		}
		if slices.Contains(rec.info.Kinds, kind) {
			nodes = append(nodes, n)
		}
	}
	slices.Sort(nodes)
	return nodes, nil
}

// liveLocked 取节点记录并做惰性过期收敛（过期即物理删除；调用方必须已
// 持 r.mu）。
func (r *registry) liveLocked(node rans.NodeID) (nodeRecord, bool) {
	rec, ok := r.nodes[node]
	if !ok {
		return nodeRecord{}, false
	}
	if r.expiredLocked(node, rec) {
		return nodeRecord{}, false
	}
	return rec, true
}

// expiredLocked 过期判定 + 惰性物理删除（调用方必须已持 r.mu）。
func (r *registry) expiredLocked(node rans.NodeID, rec nodeRecord) bool {
	if time.Now().Before(rec.expiresAt) {
		return false
	}
	delete(r.nodes, node)
	return true
}
