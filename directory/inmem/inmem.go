// Package inmem 提供目录的进程内实现（单机形态与测试基座，规格 §11）。
package inmem

import (
	"context"
	"sync"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

// entry 是主键值的进程内等价物（键值 v3）：owner 归属三元组 + tombstone
// 无主标记。墓碑条目保留在表中（主键永不清删的进程内同构），owner 零值
// 且 tombstone 置位即无主——Lookup miss、Register 按分支 1 收养。
type entry struct {
	owner     directory.Owner
	tombstone bool
}

type impl struct {
	mu sync.Mutex
	// m 身份→条目。全部条目经 Register/换锁/注销写入；注销写墓碑（主键
	// 永不清删），实例归属携带取号纪元（实例级三元组，恒 ≥1）。
	m map[rans.ID]entry
	// inc 按 id 的激活纪元序列：与条目存亡解耦——注销/墓碑不清纪元
	// （单调性跨墓碑存续是 zombie 围栏的前提）。进程内单机形态，
	// 单调性以进程为界。
	inc map[rans.ID]uint64
}

var _ directory.Directory = (*impl)(nil)

// New 返回进程内目录（单机最小形态，不含节点注册面——注册面见
// NewNodeRegistry）。条目常驻：无 TTL，注销写墓碑（纪元跨墓碑存续），
// 条目有效性由调用方按成员代际判定。
func New() directory.Directory {
	return &impl{
		m:   make(map[rans.ID]entry),
		inc: make(map[rans.ID]uint64),
	}
}

// nextIncLocked 取下一个纪元（调用方必须已持 i.mu）：per-id 严格递增，
// 首个为 1，跨墓碑存续（墓碑/注销不清序列）；floor 为纪元下限（0=无），
// 先抬序列再 +1。
func (i *impl) nextIncLocked(id rans.ID, floor uint64) uint64 {
	if floor > i.inc[id] {
		i.inc[id] = floor
	}
	i.inc[id]++
	return i.inc[id]
}

// Register 原子注册（契约见 directory.Directory：四分支语义）：取号与
// 写入在同一临界区内完成；self.Incarnation 作纪元下限（0=无下限）。
func (i *impl) Register(_ context.Context, id rans.ID, self directory.Owner, expected *directory.Owner) (bool, directory.Owner, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	cur, exists := i.m[id]
	// 分支 2：现值归属即本实例（node+epoch 相等——票 15：epoch-0 条目不
	// 再收养，经分支 3 CAS 接管自愈；registry-less 形态 0==0 自然命中）
	if exists && !cur.tombstone && cur.owner.Node == self.Node && cur.owner.Epoch == self.Epoch {
		owner := directory.Owner{Node: self.Node, Epoch: self.Epoch, Incarnation: i.nextIncLocked(id, self.Incarnation)}
		i.m[id] = entry{owner: owner}
		return true, owner, nil
	}
	// 分支 3：expected 与现值全等 → CAS 接管 inc+1 写自归属
	if exists && !cur.tombstone && expected != nil && cur.owner == *expected {
		owner := directory.Owner{Node: self.Node, Epoch: self.Epoch, Incarnation: i.nextIncLocked(id, self.Incarnation)}
		i.m[id] = entry{owner: owner}
		return true, owner, nil
	}
	// 分支 1：无值或墓碑 → 纪元从残值+1 起写自归属
	if !exists || cur.tombstone {
		owner := directory.Owner{Node: self.Node, Epoch: self.Epoch, Incarnation: i.nextIncLocked(id, self.Incarnation)}
		i.m[id] = entry{owner: owner}
		return true, owner, nil
	}
	// 分支 4：他实例持有且不可接管 → 输，返回现归属
	return false, cur.owner, nil
}

// UnregisterIfOwner 实例级条件注销：归属三元与 owner 全等才改写墓碑
// （inc 保留——纪元单调跨墓碑存续）；不符（新实例已接管/换代/已墓碑）
// → false，不误删。
func (i *impl) UnregisterIfOwner(_ context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	cur, exists := i.m[id]
	if !exists || cur.tombstone || cur.owner != owner {
		return false, nil
	}
	i.m[id] = entry{tombstone: true, owner: directory.Owner{Incarnation: cur.owner.Incarnation}}
	return true, nil
}

// Transfer 实例级换锁：归属三元与 expected 全等即取新纪元并写目标归属
// node+epoch（target.Incarnation 入参被忽略；新纪元与注册共用同一序列，
// 严格大于旧值）。墓碑/缺失/不符的返回值表见 directory.Directory.Transfer。
func (i *impl) Transfer(_ context.Context, id rans.ID, expected, target directory.Owner) (bool, uint64, directory.Owner, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	cur, exists := i.m[id]
	if !exists {
		return false, 0, directory.Owner{}, nil
	}
	if cur.tombstone || cur.owner != expected {
		return false, 0, cur.owner, directory.ErrNotOwner
	}
	next := i.nextIncLocked(id, 0)
	owner := directory.Owner{Node: target.Node, Epoch: target.Epoch, Incarnation: next}
	i.m[id] = entry{owner: owner}
	return true, next, owner, nil
}

func (i *impl) Lookup(_ context.Context, id rans.ID) (directory.Owner, bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	e, exists := i.m[id]
	if !exists || e.tombstone {
		return directory.Owner{}, false, nil
	}
	return e.owner, true, nil
}
