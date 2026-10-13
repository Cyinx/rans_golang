package rpc

// 随迁包缓存（规格 §13.3 E1 快路径的目标端落点）：dispatcher 收到
// MigrationPackage 后 Put，manager 激活闭包经 StateLoader 适配 Take。

import (
	"context"
	"sync"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/activation"
)

// DefaultPackageTTL 随迁包缺省存活窗（5min）：到包先驻、激活时消费；窗口内
// 未被激活消费的包视为过期（迁移后一直无人调用的实体，其包不应无限驻留）。
const DefaultPackageTTL = 5 * time.Minute

// DefaultPackageCacheMaxEntries 包缓存容量缺省：惰性过期
// 只清「同 id 再访问」的条目，永不 Take 的残留包在高迁移 churn 下会
// 无界驻留（仲裁输家残留、迁移取消后目标端永不激活、实体迁走后的
// straggler 包）——容量上限即此缺口的对冲面，语义与 LocationCache
// 同款。导出常量：消费方
// 与测试以之为容量语义的锚点。
const DefaultPackageCacheMaxEntries = 100_000

// PackageCache 迁移随迁包缓存（规格 §13.3 目标端半边）：
//   - Put（dispatcher 侧）：copy-in——包体来自池化页，Put 后页立即归还
//     页池，缓存必须自持拷贝；
//   - Take（激活 loader 侧）：consume-on-read——包恢复恰一次，取出即删除
//     条目，返回切片所有权移交调用方；后续同 id 激活的装载 miss 由
//     manager 内置的 checkpoint.Store 兜底接管（Store 才是正确性依赖，
//     包只是免读 Store 的快路径）；
//   - TTL 惰性过期：无后台扫描 goroutine，Take 时按 deadline 判定，过期
//     视同 miss；零后台成本。
//
// 容量上限（与 LocationCache 同款语义）：MaxEntries 限制存活条目数
// （≤0 回落 DefaultPackageCacheMaxEntries）。新 id 准入致满员时的腾位次序：
// 先清扫过期条目，仍满才按插入序 FIFO 逐出最旧存活条目（Take 消费与过期
// 清扫留下的 order 墓碑不占逐出名额）。包是低频系统面流量（一次迁移至多
// 一包），缺省容量属高基数兜底而非常规路径。
//
// 并发安全：单互斥（Put/Take 两个同步点，低频系统面流量，规格 §17 惯例）。
type PackageCache struct {
	mu  sync.Mutex
	ttl time.Duration
	now func() time.Time // 时钟注入点（测试确定性驱动惰性过期）
	m   map[rans.ID]pkgEntry

	// MaxEntries 存活条目容量上限；≤0（零值缺省）→
	// DefaultPackageCacheMaxEntries。构造后可改（与 Put 并发读写有 mu
	// 保护，语义上建议投入使用前配置——同仓库 hooks 同族约定）。
	MaxEntries int

	// order 插入序 id 队列（FIFO 逐出依据）。已被 Take/过期清扫删除的 id
	// 在其中留墓碑，容量动作/压缩时跳过——重写同 id 不改变其 FIFO 位。
	order []rans.ID
}

// pkgEntry 单个待消费包：快照字节（缓存独占）+ 过期时刻。
type pkgEntry struct {
	snap     []byte
	deadline time.Time
}

// NewPackageCache 构造缓存。ttl ≤0 → DefaultPackageTTL。
func NewPackageCache(ttl time.Duration) *PackageCache {
	if ttl <= 0 {
		ttl = DefaultPackageTTL
	}
	return &PackageCache{ttl: ttl, now: time.Now, m: make(map[rans.ID]pkgEntry)}
}

// Put 拷贝入快照并刷新 TTL（同 id 重复投递为覆盖——at-most-once 投递语义下
// 不应发生，覆盖是防御口径）。新 id 准入致满员时按容量策略腾位（先清过期
// 再 FIFO 逐出，见 PackageCache 注释）。
func (c *PackageCache) Put(id rans.ID, snap []byte) {
	cp := make([]byte, len(snap))
	copy(cp, snap)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.m[id]; !exists {
		c.ensureCapacityLocked()
		c.order = append(c.order, id)
	}
	c.m[id] = pkgEntry{snap: cp, deadline: c.now().Add(c.ttl)}
	c.compactOrderLocked()
}

// ensureCapacityLocked 为一个新准入腾位（调用方必须已持 mu）：存活数低于
// 容量直接返回；满员则先全表清扫过期条目（过期优先于逐出），仍满再按
// 插入序 FIFO 逐出最旧存活条目（墓碑跳过——Take/过期清扫删除的 id 不占
// 逐出名额）。腾位恰留一个空位（准入后回到容量值）。
func (c *PackageCache) ensureCapacityLocked() {
	max := c.MaxEntries
	if max <= 0 {
		max = DefaultPackageCacheMaxEntries
	}
	if len(c.m) < max {
		return
	}
	now := c.now()
	for id, e := range c.m {
		if now.After(e.deadline) {
			delete(c.m, id)
		}
	}
	if len(c.m) < max {
		return // 过期清扫已腾位：不逐出任何存活条目
	}
	live := c.order[:0]
	for _, id := range c.order {
		if _, ok := c.m[id]; !ok {
			continue // 墓碑
		}
		if len(c.m) >= max {
			delete(c.m, id) // FIFO 逐出最旧存活条目
			continue
		}
		live = append(live, id)
	}
	c.order = live
}

// compactOrderLocked order 簿记上限：墓碑（已不在 m 的 id）过滤 + 同 id
// 重复项去重（保留最旧 FIFO 位）——「写→消费→再写」的同 id churn 会给
// order 追加同名条目，而它们在重新驻留期间全是「存活」态，纯墓碑过滤
// 对其无效（热 id churn 下会无界增长）。阈值：墓碑+重复
// 占比过半加常数余量时原地过滤。调用方必须已持 mu。
func (c *PackageCache) compactOrderLocked() {
	if len(c.order) <= 2*len(c.m)+8 {
		return
	}
	seen := make(map[rans.ID]struct{}, len(c.m))
	live := c.order[:0]
	for _, id := range c.order {
		if _, ok := c.m[id]; !ok {
			continue // 墓碑
		}
		if _, dup := seen[id]; dup {
			continue // 同 id 重复簿记：保留最旧 FIFO 位即可
		}
		seen[id] = struct{}{}
		live = append(live, id)
	}
	c.order = live
}

// Take 取出并消费快照（consume-on-read）：命中即删除条目并移交字节所有权
// （缓存不再引用，调用方可安全持有）；未命中或已惰性过期返回 (nil, false)。
func (c *PackageCache) Take(id rans.ID) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[id]
	if !ok {
		return nil, false
	}
	delete(c.m, id)
	if c.now().After(e.deadline) {
		return nil, false // 惰性过期：过期包视同 miss，调用方走 Store 兜底
	}
	return e.snap, true
}

// peek 只读探查（不消费、拷贝出）：测试等包到达的确定性握手点，非运行时 API。
func (c *PackageCache) peek(id rans.ID) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[id]
	if !ok {
		return nil, false
	}
	cp := make([]byte, len(e.snap))
	copy(cp, e.snap)
	return cp, true
}

// cacheStateLoader PackageCache → activation.StateLoader 适配（rpc.New 接线
// 面）：LoadState 即 Take——包恢复恰一次，之后的激活 miss 由 manager 的
// Store 兜底接管。
type cacheStateLoader struct {
	cache *PackageCache
}

// 编译期断言：cacheStateLoader 满足激活期装载契约。
var _ activation.StateLoader = (*cacheStateLoader)(nil)

// LoadState 消费式装载（见 Take 的 consume-on-read 契约）。
func (l *cacheStateLoader) LoadState(_ context.Context, id rans.ID) ([]byte, bool) {
	return l.cache.Take(id)
}
