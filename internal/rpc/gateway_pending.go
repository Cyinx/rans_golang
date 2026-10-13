package rpc

// pendingTable（H15）：网关 pending 表的 256 分片形态——每分片独立锁 +
// 到期最小堆 + 按节点二级索引。登记/投递按 MessageID 哈希落分片（热路径
// 零全局锁）；deadline 扫描只看各分片堆顶（O(分片数+过期数)，替代 250ms
// 全表扫）；FailPendingForNode 走按节点索引（替代全表过滤）。完成先于
// 释放门的陷阱 11 次序在各分片内保持（分片锁内先摘表、锁外完成 waiter）。

import (
	"container/heap"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
)

// pendingShards 分片数（静态——单分片面 = pending/256）。
const pendingShards = 256

// pendingShard 单分片：pending 表 + 到期堆 + 按节点索引（同一把锁全
// 序化——三结构互不漂移）。
type pendingShard struct {
	mu      sync.Mutex
	pending map[uint64]*pendingEntry
	dlHeap  pendingHeap
	byNode  map[rans.NodeID]map[uint64]struct{}
}

// pendingTable 分片化 pending 表。容量拒绝保持**全局精确**语义（live/max
// 原子对——附录 A NET-8 的 100k 满载拒新契约不变）；分片解决锁竞争与
// 扫描面。
type pendingTable struct {
	shards [pendingShards]pendingShard
	live   atomic.Int64
	max    int
}

func newPendingTable(maxPending int) *pendingTable {
	if maxPending <= 0 {
		maxPending = defaultMaxPending
	}
	pt := &pendingTable{max: maxPending}
	for i := range pt.shards {
		s := &pt.shards[i]
		s.pending = make(map[uint64]*pendingEntry)
		s.byNode = make(map[rans.NodeID]map[uint64]struct{})
	}
	return pt
}

func (pt *pendingTable) shardOf(id uint64) *pendingShard {
	return &pt.shards[id%pendingShards]
}

// register 登记表项（重复 id / 分片满员报错——ErrDuplicateMessageID /
// ErrPendingCapacity 由调用方映射）。
func (pt *pendingTable) register(id uint64, target rans.NodeID, e *pendingEntry) error {
	s := pt.shardOf(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.pending[id]; dup {
		return ErrDuplicateMessageID
	}
	if pt.live.Add(1) > int64(pt.max) {
		pt.live.Add(-1)
		return ErrPendingCapacity
	}
	s.pending[id] = e
	heap.Push(&s.dlHeap, dlItem{id: id, deadline: e.deadline})
	set, ok := s.byNode[target]
	if !ok {
		set = make(map[uint64]struct{})
		s.byNode[target] = set
	}
	set[id] = struct{}{}
	return nil
}

// remove 摘表（幂等）：返回表项是否存在（存在即调用方负责竞速回收）。
// 堆项惰性失效（pop 时校验存在性与期限）。
func (pt *pendingTable) remove(id uint64) bool {
	s := pt.shardOf(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.pending[id]
	if !ok {
		return false
	}
	pt.removeLocked(s, id, e)
	return true
}

func (pt *pendingTable) removeLocked(s *pendingShard, id uint64, e *pendingEntry) {
	pt.live.Add(-1)
	delete(s.pending, id)
	if set := s.byNode[e.target]; set != nil {
		delete(set, id)
		if len(set) == 0 {
			delete(s.byNode, e.target)
		}
	}
	// 堆不摘（惰性）：pop/清扫时按 map 存在性过滤。
}

// takeForDeliver 投递路径的原子「查表+过期判+摘表」（陷阱 11：过期项不
// 摘表不投递——完成权归扫描器）。返回 (entry, ok)。
func (pt *pendingTable) takeForDeliver(id uint64, now time.Time) (*pendingEntry, bool) {
	s := pt.shardOf(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.pending[id]
	if !ok {
		return nil, false
	}
	if !now.Before(e.deadline) {
		return nil, false // 已过期＝迟到语义，不投递也不摘表（留给扫描器）
	}
	pt.removeLocked(s, id, e)
	return e, true
}

// reapedEntry 扫描器摘除产物（锁内收集、锁外完成的载体）。
type reapedEntry struct {
	id uint64
	e  *pendingEntry
}

// reapExpired 摘除全部到期表项（扫描器一轮；只看各分片堆顶——O(分片数+
// 过期数)）。锁内收集，锁外完成的次序由调用方承担。
func (pt *pendingTable) reapExpired(now time.Time) []reapedEntry {
	var out []reapedEntry
	for i := range pt.shards {
		s := &pt.shards[i]
		s.mu.Lock()
		for s.dlHeap.Len() > 0 {
			top := s.dlHeap[0]
			e, ok := s.pending[top.id]
			if !ok || !e.deadline.Equal(top.deadline) {
				heap.Pop(&s.dlHeap) // 惰性失效项（已摘/改期）
				continue
			}
			if now.Before(e.deadline) {
				break // 堆顶未到期：本分片收口
			}
			heap.Pop(&s.dlHeap)
			pt.removeLocked(s, top.id, e)
			out = append(out, reapedEntry{id: top.id, e: e})
		}
		s.mu.Unlock()
	}
	return out
}

// reapForNode 摘除目标节点全部在途表项（快速失败；按节点索引——与
// pending 总量解耦，只随该节点在途数线性）。锁内收集，锁外完成由调用方
// 承担。
func (pt *pendingTable) reapForNode(node rans.NodeID) []*pendingEntry {
	var out []*pendingEntry
	for i := range pt.shards {
		s := &pt.shards[i]
		s.mu.Lock()
		set := s.byNode[node]
		ids := make([]uint64, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
		for _, id := range ids {
			if e, ok := s.pending[id]; ok {
				pt.removeLocked(s, id, e)
				out = append(out, e)
			}
		}
		s.mu.Unlock()
	}
	return out
}

// count 存活表项总量（排空 flush 观测面；非热路径）。
func (pt *pendingTable) count() int {
	n := 0
	for i := range pt.shards {
		s := &pt.shards[i]
		s.mu.Lock()
		n += len(s.pending)
		s.mu.Unlock()
	}
	return n
}

// ---- 到期最小堆（container/heap 适配）----

type dlItem struct {
	id       uint64
	deadline time.Time
}

type pendingHeap []dlItem

func (h pendingHeap) Len() int           { return len(h) }
func (h pendingHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h pendingHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pendingHeap) Push(x any)        { *h = append(*h, x.(dlItem)) }
func (h *pendingHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}
