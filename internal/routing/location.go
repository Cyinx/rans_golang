package routing

// LocationCache（H15 分片形态 + 票 07 LRU 化）：身份→位置缓存，64 分片
// 各自持锁——分片内 LRU（命中/覆写移到 MRU 端，饱和准入从 LRU 端逐出）
//；InvalidateNode 经分片内节点二级索引直达相关条目（不再全表扫描）；
// 分片哈希直接对 Kind/Key 字节做 FNV-1a（免 id.String() 拼接分配）。
// 正 TTL 30s / 负 TTL 1s / 惰性过期（Get 时判 deadline）不变；容量语义
// 按分片均摊（总量 MaxEntries，分片预算 = MaxEntries/64——公平性近似，
// 换取零全局锁热路径）。

import (
	"container/list"
	"sync"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

// DefaultLocationCacheMaxEntries 存活条目总量缺省上限（分片均摊）。
const DefaultLocationCacheMaxEntries = 100_000

// locationShards 分片数（静态——准入扫描面 = 容量/分片数）。
const locationShards = 64

// DefaultPosTTL / DefaultNegTTL 正负条目缺省存活期。
const (
	DefaultPosTTL = 30 * time.Second
	DefaultNegTTL = 1 * time.Second
)

type cacheEntry struct {
	loc      directory.Owner
	positive bool
	deadline time.Time
}

type shard struct {
	mu      sync.Mutex
	entries map[rans.ID]*list.Element // id → LRU 链节点（Value=*cacheItem）
	lru     *list.List                // 前=LRU 端（逐出），后=MRU 端
	// byNode 正条目的节点二级索引（票 07）：InvalidateNode 直达相关条目，
	// 不再全表扫描。仅正条目登记（负条目无归属）。
	byNode map[rans.NodeID]map[rans.ID]struct{}
	max    int // 分片容量预算（总量 MaxEntries/locationShards）
}

type cacheItem struct {
	id  rans.ID
	ent cacheEntry
}

// LocationCache 身份→位置缓存。Get/Put*/Invalidate* 分片互斥锁保护；
// MaxEntries 经 SetMaxEntries/构造设置（总量上限，分片预算随之重算）。
type LocationCache struct {
	shards [locationShards]shard
	posTTL time.Duration
	negTTL time.Duration
}

// NewLocationCache 构造（posTTL/negTTL ≤0 取缺省；maxEntries ≤0 取缺省）。
func NewLocationCache(posTTL, negTTL time.Duration, maxEntries int) *LocationCache {
	if posTTL <= 0 {
		posTTL = DefaultPosTTL
	}
	if negTTL <= 0 {
		negTTL = DefaultNegTTL
	}
	if maxEntries <= 0 {
		maxEntries = DefaultLocationCacheMaxEntries
	}
	c := &LocationCache{posTTL: posTTL, negTTL: negTTL}
	for i := range c.shards {
		c.shards[i].entries = make(map[rans.ID]*list.Element)
		c.shards[i].lru = list.New()
		c.shards[i].byNode = make(map[rans.NodeID]map[rans.ID]struct{})
		c.shards[i].max = max(maxEntries/locationShards, 1)
	}
	return c
}

// SetMaxEntries 更新总量上限（分片预算同步重算）。
func (c *LocationCache) SetMaxEntries(n int) {
	if n <= 0 {
		n = DefaultLocationCacheMaxEntries
	}
	for i := range c.shards {
		c.shards[i].mu.Lock()
		c.shards[i].max = max(n/locationShards, 1)
		c.shards[i].mu.Unlock()
	}
}

// shardOf 分片哈希：FNV-1a 直扫 Kind/Key 字节（票 07：免 id.String() 拼接
// 分配——热路径逐操作一次分配的消除）。
func (c *LocationCache) shardOf(id rans.ID) *shard {
	h := uint32(2166136261)
	for i := 0; i < len(id.Kind); i++ {
		h ^= uint32(id.Kind[i])
		h *= 16777619
	}
	for i := 0; i < len(id.Key); i++ {
		h ^= uint32(id.Key[i])
		h *= 16777619
	}
	return &c.shards[h%locationShards]
}

// Get 读取缓存：过期条目视同未命中（惰性删除就地清出，不占逐出名额）；
// 命中刷新 LRU 位（移到 MRU 端——票 07）。返回 (owner, found, positive)。
func (c *LocationCache) Get(id rans.ID) (directory.Owner, bool, bool) {
	sh := c.shardOf(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	el, ok := sh.entries[id]
	if !ok {
		return directory.Owner{}, false, false
	}
	it := el.Value.(*cacheItem)
	if time.Now().After(it.ent.deadline) {
		sh.removeLocked(el)
		return directory.Owner{}, false, false
	}
	sh.lru.MoveToBack(el)
	return it.ent.loc, true, it.ent.positive
}

// PutPositive 写正条目（位置命中/CAS 赢家覆盖）。
// 覆盖同 id 的负条目。
func (c *LocationCache) PutPositive(id rans.ID, loc directory.Owner) {
	c.put(id, cacheEntry{loc: loc, positive: true, deadline: time.Now().Add(c.posTTL)})
}

// PutPositiveTTL 写短 TTL 正条目（放置决策回填面，H14：只覆盖激活窗）。
func (c *LocationCache) PutPositiveTTL(id rans.ID, loc directory.Owner, ttl time.Duration) {
	c.put(id, cacheEntry{loc: loc, positive: true, deadline: time.Now().Add(ttl)})
}

// PutNegative 写负条目（目录 Lookup miss 后防穿透）；覆盖同 id 的过期前
// 正条目（目录已查无，旧正位置是陈旧路由）。
func (c *LocationCache) PutNegative(id rans.ID) {
	c.put(id, cacheEntry{positive: false, deadline: time.Now().Add(c.negTTL)})
}

func (c *LocationCache) put(id rans.ID, e cacheEntry) {
	sh := c.shardOf(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if el, exists := sh.entries[id]; exists {
		it := el.Value.(*cacheItem)
		sh.unindexNodeLocked(it) // 旧条目（可能异归属/异极性）出索引
		it.ent = e
		sh.indexNodeLocked(it)
		sh.lru.MoveToBack(el) // 覆写即 MRU
		return
	}
	sh.ensureCapacityLocked()
	it := &cacheItem{id: id, ent: e}
	sh.entries[id] = sh.lru.PushBack(it)
	sh.indexNodeLocked(it)
}

// indexNodeLocked/unindexNodeLocked 节点二级索引维护（调用方必须已持
// 分片 mu；仅正条目）。
func (sh *shard) indexNodeLocked(it *cacheItem) {
	if !it.ent.positive {
		return
	}
	set, ok := sh.byNode[it.ent.loc.Node]
	if !ok {
		set = make(map[rans.ID]struct{})
		sh.byNode[it.ent.loc.Node] = set
	}
	set[it.id] = struct{}{}
}

func (sh *shard) unindexNodeLocked(it *cacheItem) {
	if !it.ent.positive {
		return
	}
	set := sh.byNode[it.ent.loc.Node]
	delete(set, it.id)
	if len(set) == 0 {
		delete(sh.byNode, it.ent.loc.Node)
	}
}

// removeLocked 就地清出一个条目（调用方必须已持分片 mu）：entries/LRU/
// 节点索引三面同删。
func (sh *shard) removeLocked(el *list.Element) {
	it := el.Value.(*cacheItem)
	sh.unindexNodeLocked(it)
	delete(sh.entries, it.id)
	sh.lru.Remove(el)
}

// ensureCapacityLocked 分片内腾位（调用方必须已持分片 mu）：存活数低于
// 预算直接返回；满员先清扫本分片过期条目（负条目 1s TTL 使「过期」与
// 「最久未用」不同序），仍满再从 LRU 端逐出最久未用条目（票 07：FIFO
// → LRU——倾斜访问下热点条目不再被周期性逐出）。
func (sh *shard) ensureCapacityLocked() {
	if len(sh.entries) < sh.max {
		return
	}
	now := time.Now()
	for _, el := range sh.entries {
		if now.After(el.Value.(*cacheItem).ent.deadline) {
			sh.removeLocked(el)
		}
	}
	for len(sh.entries) >= sh.max {
		sh.removeLocked(sh.lru.Front())
	}
}

// Invalidate 删除单 id 条目（本地销毁/缓存失效钩子）。
func (c *LocationCache) Invalidate(id rans.ID) {
	sh := c.shardOf(id)
	sh.mu.Lock()
	if el, ok := sh.entries[id]; ok {
		sh.removeLocked(el)
	}
	sh.mu.Unlock()
}

// InvalidateNode 删除归属到 node 的全部正条目（节点下线钩子）：经分片
// 内节点二级索引直达（票 07：不再全表扫描——64 分片×全表遍历的锁持有
// 面收敛为各分片一次索引查删）。
func (c *LocationCache) InvalidateNode(node rans.NodeID) {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		for id := range sh.byNode[node] {
			if el, ok := sh.entries[id]; ok {
				sh.removeLocked(el)
			}
		}
		delete(sh.byNode, node)
		sh.mu.Unlock()
	}
}

// Len 存活条目总量（测试/观测面；遍历分片加和——非热路径）。
func (c *LocationCache) Len() int {
	n := 0
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		n += len(sh.entries)
		sh.mu.Unlock()
	}
	return n
}
