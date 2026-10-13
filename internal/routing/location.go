package routing

// LocationCache（H15 分片形态）：身份→位置缓存，64 分片各自持锁——
// 饱和后新 id 准入 O(分片容量)（EX7 消除），InvalidateNode 广播各分片。
// 正 TTL 30s / 负 TTL 1s / 惰性过期（Get 时判 deadline）不变；容量语义
// 按分片均摊（总量 MaxEntries，分片预算 = MaxEntries/64——公平性近似，
// 换取零全局锁热路径）。

import (
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
	entries map[rans.ID]cacheEntry
	order   []rans.ID // 插入序（FIFO 逐出面；墓碑压缩见 compactOrderLocked）
	max     int       // 分片容量预算（总量 MaxEntries/locationShards）
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
		c.shards[i].entries = make(map[rans.ID]cacheEntry)
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

func (c *LocationCache) shardOf(id rans.ID) *shard {
	return &c.shards[id.Hash()%locationShards]
}

// Get 读取缓存：过期条目视同未命中（惰性删除就地清出，不占逐出名额）。
// 返回 (owner, found, positive)。
func (c *LocationCache) Get(id rans.ID) (directory.Owner, bool, bool) {
	sh := c.shardOf(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := sh.entries[id]
	if !ok {
		return directory.Owner{}, false, false
	}
	if time.Now().After(e.deadline) {
		delete(sh.entries, id)
		return directory.Owner{}, false, false
	}
	return e.loc, true, e.positive
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
	if _, exists := sh.entries[id]; !exists {
		sh.ensureCapacityLocked()
		sh.order = append(sh.order, id)
	}
	sh.entries[id] = e
	sh.compactOrderLocked()
}

// ensureCapacityLocked 分片内腾位（调用方必须已持分片 mu）：存活数低于
// 预算直接返回；满员先清扫本分片过期条目（负条目 1s TTL 使「过期」与
// 「插入最旧」不同序），仍满再按插入序 FIFO 逐出最旧存活条目。
func (sh *shard) ensureCapacityLocked() {
	if len(sh.entries) < sh.max {
		return
	}
	now := time.Now()
	for id, e := range sh.entries {
		if now.After(e.deadline) {
			delete(sh.entries, id)
		}
	}
	if len(sh.entries) < sh.max {
		return
	}
	live := sh.order[:0]
	for _, id := range sh.order {
		if _, ok := sh.entries[id]; !ok {
			continue // 墓碑
		}
		if len(sh.entries) < sh.max {
			live = append(live, id)
			continue
		}
		delete(sh.entries, id) // FIFO 逐出最旧存活（腾位至 max-1）
	}
	sh.order = live
}

// compactOrderLocked 压缩插入序（调用方必须已持分片 mu）：比例阈值触发；
// 墓碑（已删 id）滑出 + 同 id 重复追加按最旧位置去重（热 id churn——
// 「回填→失效→再回填」每次重填都追加同名，按存活过滤无法收缩）。
func (sh *shard) compactOrderLocked() {
	if len(sh.order) < 2*len(sh.entries)+8 {
		return
	}
	live := sh.order[:0]
	seen := make(map[rans.ID]struct{}, len(sh.entries))
	for _, id := range sh.order {
		if _, ok := sh.entries[id]; !ok {
			continue // 墓碑
		}
		if _, dup := seen[id]; dup {
			continue // 重复追加：保最旧 FIFO 位
		}
		seen[id] = struct{}{}
		live = append(live, id)
	}
	sh.order = live
}

// Invalidate 删除单 id 条目（本地销毁/缓存失效钩子）。
func (c *LocationCache) Invalidate(id rans.ID) {
	sh := c.shardOf(id)
	sh.mu.Lock()
	delete(sh.entries, id)
	sh.mu.Unlock()
}

// InvalidateNode 删除归属到 node 的全部正条目（节点下线钩子）：广播各
// 分片，按值过滤正条目。
func (c *LocationCache) InvalidateNode(node rans.NodeID) {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		for id, e := range sh.entries {
			if e.positive && e.loc.Node == node {
				delete(sh.entries, id)
			}
		}
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
