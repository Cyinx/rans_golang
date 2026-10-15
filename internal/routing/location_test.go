package routing

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

// shortCache 测试用短 TTL 缓存（正 400ms/负 250ms——留慢机停摆余量：
// 更短窗口在负载机上一次调度停摆即假红）。
func shortCache() *LocationCache {
	return NewLocationCache(400*time.Millisecond, 250*time.Millisecond, 0)
}

func mustID(t *testing.T, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID("wallet", inst)
	if err != nil {
		t.Fatalf("rans.NewID: %v", err)
	}
	return id
}

// TestLocationCachePositiveHitAndTTLExpiry 正缓存：写入即命中（found=true,
// positive=true，返回位置）；过 posTTL 后惰性过期 → miss（found=false，
// 调用方需查目录）。
func TestLocationCachePositiveHitAndTTLExpiry(t *testing.T) {
	c := shortCache()
	id := mustID(t, "a")
	want := directory.Owner{Node: 7}
	c.PutPositive(id, want)

	loc, found, positive := c.Get(id)
	if !found || !positive {
		t.Fatalf("fresh positive entry: found=%v positive=%v, want true/true", found, positive)
	}
	if loc != want {
		t.Fatalf("positive entry loc = %+v, want %+v", loc, want)
	}

	time.Sleep(500 * time.Millisecond) // > posTTL 400ms
	loc, found, positive = c.Get(id)
	if found || positive {
		t.Fatalf("expired positive entry: found=%v positive=%v, want miss (false/false)", found, positive)
	}
	if loc != (directory.Owner{}) {
		t.Fatalf("miss must return zero Location, got %+v", loc)
	}
}

// TestLocationCacheNegativeSemanticsAndTTL 负缓存语义（防穿透）：
// PutNegative 后 negTTL 内 Get 返回 found=true/positive=false——调用方
// 应视为 miss 但**不查目录**（该 id 近期无目录条目）；过 negTTL 后惰性
// 过期 → 真 miss（found=false，调用方恢复查目录）。
func TestLocationCacheNegativeSemanticsAndTTL(t *testing.T) {
	c := shortCache()
	id := mustID(t, "b")
	c.PutNegative(id)

	loc, found, positive := c.Get(id)
	if !found || positive {
		t.Fatalf("fresh negative entry: found=%v positive=%v, want true/false (skip directory)", found, positive)
	}
	if loc != (directory.Owner{}) {
		t.Fatalf("negative entry must carry zero Location, got %+v", loc)
	}

	time.Sleep(300 * time.Millisecond) // > negTTL 250ms
	loc, found, positive = c.Get(id)
	if found || positive {
		t.Fatalf("expired negative entry: found=%v positive=%v, want miss (false/false)", found, positive)
	}
	if loc != (directory.Owner{}) {
		t.Fatalf("miss must return zero Location, got %+v", loc)
	}
}

// TestLocationCacheNegativePreventsDirectoryBounce 负缓存防穿透锚点：
// negTTL 窗口内反复 Get 都返回负（调用方按契约跳过目录查询），窗口到期
// 后第一次 Get 变 miss（恢复目录解析）。
func TestLocationCacheNegativePreventsDirectoryBounce(t *testing.T) {
	c := NewLocationCache(time.Minute, 250*time.Millisecond, 0) // 正 TTL 长到不干扰
	id := mustID(t, "c")
	c.PutNegative(id)

	for i := 0; i < 3; i++ {
		if _, found, positive := c.Get(id); !found || positive {
			t.Fatalf("Get #%d inside negTTL: found=%v positive=%v, want true/false", i, found, positive)
		}
	}
	time.Sleep(300 * time.Millisecond)
	if _, found, _ := c.Get(id); found {
		t.Fatal("after negTTL expiry Get must miss (caller re-queries directory)")
	}
}

// TestLocationCacheDefaults 零值 → 默认正 30s/负 1s（规格 §8.1 ②/陷阱 14）。
func TestLocationCacheDefaults(t *testing.T) {
	c := NewLocationCache(0, 0, 0)
	if c.posTTL != 30*time.Second {
		t.Fatalf("default posTTL = %v, want 30s", c.posTTL)
	}
	if c.negTTL != time.Second {
		t.Fatalf("default negTTL = %v, want 1s", c.negTTL)
	}
	// 单侧零值也落默认（防御：<=0 均取默认）。
	c2 := NewLocationCache(5*time.Second, 0, 0)
	if c2.negTTL != time.Second {
		t.Fatalf("negTTL zero with pos set = %v, want 1s", c2.negTTL)
	}
}

// TestLocationCacheInvalidate Invalidate：本地销毁/目录注销/发送失败三类
// 钩子共用本原语——正/负条目一并删除。
func TestLocationCacheInvalidate(t *testing.T) {
	c := shortCache()
	posID, negID := mustID(t, "p"), mustID(t, "n")
	c.PutPositive(posID, directory.Owner{Node: 3})
	c.PutNegative(negID)

	c.Invalidate(posID)
	if _, found, _ := c.Get(posID); found {
		t.Fatal("Invalidate must evict positive entry")
	}
	c.Invalidate(negID)
	if _, found, _ := c.Get(negID); found {
		t.Fatal("Invalidate must evict negative entry")
	}
	// 对不存在 id 的 Invalidate 是 no-op（不 panic）。
	c.Invalidate(mustID(t, "ghost"))
}

// TestLocationCacheCASHookOverwritesNegative CAS 冲突钩子形状：
// PutNegative（仲裁输家视角陈旧负缓存）后 PutPositive(赢家位置) →
// Get 命中赢家位置（positive 覆盖 negative）。
func TestLocationCacheCASHookOverwritesNegative(t *testing.T) {
	c := shortCache()
	id := mustID(t, "cas")
	c.PutNegative(id)
	winner := directory.Owner{Node: 9}
	c.PutPositive(id, winner)

	loc, found, positive := c.Get(id)
	if !found || !positive {
		t.Fatalf("after CAS-winner PutPositive: found=%v positive=%v, want true/true", found, positive)
	}
	if loc != winner {
		t.Fatalf("CAS winner loc = %+v, want %+v", loc, winner)
	}
}

// TestLocationCachePutNegativeOverwritesPositive 反向覆盖：
// 目录查无此 id 后写负缓存 → 原正条目被负覆盖（防陈旧正路由）。
func TestLocationCachePutNegativeOverwritesPositive(t *testing.T) {
	c := shortCache()
	id := mustID(t, "flip")
	c.PutPositive(id, directory.Owner{Node: 4})
	c.PutNegative(id)
	if _, found, positive := c.Get(id); !found || positive {
		t.Fatalf("after PutNegative over positive: found=%v positive=%v, want true/false", found, positive)
	}
}

// TestLocationCacheInvalidateNodeBulk InvalidateNode（节点下线钩子）：
// 批量失效目标节点的全部正条目；其他节点条目与负条目（无节点归属）
// 不受影响。
func TestLocationCacheInvalidateNodeBulk(t *testing.T) {
	c := shortCache()
	n1a, n1b, n2, neg := mustID(t, "n1a"), mustID(t, "n1b"), mustID(t, "n2"), mustID(t, "neg")
	c.PutPositive(n1a, directory.Owner{Node: 1})
	c.PutPositive(n1b, directory.Owner{Node: 1})
	c.PutPositive(n2, directory.Owner{Node: 2})
	c.PutNegative(neg)

	c.InvalidateNode(1)

	if _, found, _ := c.Get(n1a); found {
		t.Fatal("node-1 entry a must be bulk-evicted")
	}
	if _, found, _ := c.Get(n1b); found {
		t.Fatal("node-1 entry b must be bulk-evicted")
	}
	if _, found, _ := c.Get(n2); !found {
		t.Fatal("node-2 entry must survive InvalidateNode(1)")
	}
	if _, found, positive := c.Get(neg); !found || positive {
		t.Fatalf("negative entry must survive InvalidateNode (no node association): found=%v positive=%v", found, positive)
	}
}

// TestLocationCacheConcurrentAccess 并发安全锚点（配 -race）：混合
// Get/PutPositive/PutNegative/Invalidate/InvalidateNode 并发打满，
// 不死锁不 panic，终态可读。
func TestLocationCacheConcurrentAccess(t *testing.T) {
	c := NewLocationCache(2*time.Millisecond, 1*time.Millisecond, 0) // 短 TTL 制造过期竞态
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				id := mustID(t, fmt.Sprintf("w%d-%d", w, i%4))
				switch i % 5 {
				case 0:
					c.PutPositive(id, directory.Owner{Node: rans.NodeID(w)})
				case 1:
					c.PutNegative(id)
				case 2:
					c.Invalidate(id)
				case 3:
					c.InvalidateNode(rans.NodeID(w))
				default:
					c.Get(id)
				}
			}
		}(w)
	}
	wg.Wait()
	// 终态无死锁可读即通过（值不作断言——并发写手下值不确定）。
	c.PutPositive(mustID(t, "final"), directory.Owner{Node: 5})
	if loc, found, positive := c.Get(mustID(t, "final")); !found || !positive || loc.Node != 5 {
		t.Fatalf("post-concurrency sanity: found=%v positive=%v loc=%+v", found, positive, loc)
	}
}

// ---- 容量上限（封住高基数不再查询的 id 上的无界增长）----

// sameShardIDs 生成 n 个落位同一分片的身份（分片预算语义下 FIFO 断言的
// 确定性编排——按 id.Hash()%locationShards 分桶挑选）。
func sameShardIDs(t *testing.T, c *LocationCache, prefix string, n int) []rans.ID {
	t.Helper()
	// 票 07：分片哈希改为 Kind/Key 直扫 FNV（不再同源于 id.Hash）——按
	// 实际分片指针分桶。
	buckets := make(map[*shard][]rans.ID)
	for i := 0; ; i++ {
		id, err := rans.NewID("k", prefix+"-"+strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		b := c.shardOf(id)
		buckets[b] = append(buckets[b], id)
		if len(buckets[b]) == n {
			return buckets[b]
		}
	}
}

// TestLocationCacheMaxEntriesFIFOEviction：容量上限强制——满员插入新 id 时
// 逐出插入序最旧者（FIFO），最新条目留存。
func TestLocationCacheMaxEntriesFIFOEviction(t *testing.T) {
	c := shortCache() // 正 400ms——测试窗口内不过期
	ids := sameShardIDs(t, c, "cap", 4)
	c.shardOf(ids[0]).mu.Lock()
	c.shardOf(ids[0]).max = 2 // 分片预算直设（总量/64 均摊的确定性等价面）
	c.shardOf(ids[0]).mu.Unlock()
	id1, id2, id3, id4 := ids[0], ids[1], ids[2], ids[3]
	loc := directory.Owner{Node: 3}
	for _, id := range []rans.ID{id1, id2, id3} { // id1 被逐出
		c.PutPositive(id, loc)
	}
	if _, found, _ := c.Get(id1); found {
		t.Fatal("oldest entry (id1) must be FIFO-evicted at capacity")
	}
	for _, id := range []rans.ID{id2, id3} {
		if _, found, _ := c.Get(id); !found {
			t.Fatalf("recent entries must survive, %v evicted", id)
		}
	}
	c.PutPositive(id4, loc) // id2 成为最旧，被逐出
	if _, found, _ := c.Get(id2); found {
		t.Fatal("id2 must be evicted next (FIFO order)")
	}
	for _, id := range []rans.ID{id3, id4} {
		if _, found, _ := c.Get(id); !found {
			t.Fatalf("entries within capacity must survive, %v evicted", id)
		}
	}
}

// TestLocationCacheExpiryPreferredOverEviction：满员时先清过期再逐出——
// 重写过 deadline 的最旧插入条目（id1）不得被 FIFO 误逐：id2 过期被清、
// id1 新鲜留存、id3 准入。
func TestLocationCacheExpiryPreferredOverEviction(t *testing.T) {
	c := NewLocationCache(600*time.Millisecond, 250*time.Millisecond, 0)
	c.SetMaxEntries(2)
	id1, id2, id3 := mustID(t, "pref1"), mustID(t, "pref2"), mustID(t, "pref3")
	loc := directory.Owner{Node: 3}
	c.PutPositive(id1, loc) // deadline t0+600；FIFO 位=队头
	time.Sleep(100 * time.Millisecond)
	c.PutPositive(id2, loc) // deadline t0+700
	time.Sleep(100 * time.Millisecond)
	// t0+200：重写 id1（同 id 覆盖，不刷新 FIFO 位）——deadline 重置为
	// t0+800，比 id2 的 t0+700 晚过期。
	c.PutPositive(id1, loc)
	time.Sleep(550 * time.Millisecond) // t0+750：id2 已过期、id1 仍新鲜（两侧各 50ms 余量）
	c.PutPositive(id3, loc)            // 满员：先清过期（id2）腾位，不逐出 id1

	if _, found, _ := c.Get(id1); !found {
		t.Fatal("fresh-but-FIFO-oldest entry must NOT be evicted (expiry sweep preferred)")
	}
	if _, found, _ := c.Get(id2); found {
		t.Fatal("expired entry must be swept at capacity")
	}
	if _, found, _ := c.Get(id3); !found {
		t.Fatal("new entry must be admitted after sweep")
	}
}

// TestLocationCacheEvictionSkipsInvalidatedTombstones：Invalidate 留下的
// FIFO 墓碑不占逐出名额——逐出目标是最旧的**存活**条目。
func TestLocationCacheEvictionSkipsInvalidatedTombstones(t *testing.T) {
	c := shortCache()
	ids := sameShardIDs(t, c, "tomb", 5)
	c.shardOf(ids[0]).mu.Lock()
	c.shardOf(ids[0]).max = 3 // 分片预算直设（墓碑语义的分片内确定性面）
	c.shardOf(ids[0]).mu.Unlock()
	loc := directory.Owner{Node: 3}
	for _, id := range ids[:3] {
		c.PutPositive(id, loc)
	}
	c.Invalidate(ids[0]) // FIFO 头变墓碑（不占容量）
	c.PutPositive(ids[3], loc)
	// 此刻存活：tomb2/tomb3/tomb4（满员）。再插入 tomb5 → 逐出最旧存活
	// tomb2（墓碑 tomb1 不消耗名额、不误伤后来者）。
	c.PutPositive(ids[4], loc)
	if _, found, _ := c.Get(ids[1]); found {
		t.Fatal("oldest LIVE entry must be evicted at capacity")
	}
	for _, id := range ids[2:] {
		if _, found, _ := c.Get(id); !found {
			t.Fatalf("live entries must survive, %v evicted", id)
		}
	}
}

// TestLocationCacheDefaultMaxEntries：MaxEntries 零值 → 缺省 100_000——
// 高于缺省容量的常驻集合才触发逐出。
func TestLocationCacheDefaultMaxEntries(t *testing.T) {
	c := shortCache() // MaxEntries 零值
	loc := directory.Owner{Node: 3}
	first := mustID(t, "def-first")
	last := mustID(t, "def-last")
	c.PutPositive(first, loc)
	for i := 0; i < DefaultLocationCacheMaxEntries; i++ {
		c.PutPositive(mustID(t, fmt.Sprintf("def-%d", i)), loc)
	}
	c.PutPositive(last, loc)
	// 分片化容量语义（H15）：逐出按分片预算发生——「全局最旧恰被逐出」
	// 不再是确定断言（first 所在分片未必满员）。总量有界性改经 Len 断言
	//（±分片余量 slack），最新条目必须存活。
	if _, found, _ := c.Get(last); !found {
		t.Fatal("newest entry must survive at default cap")
	}
	if n := c.Len(); n > DefaultLocationCacheMaxEntries+locationShards {
		t.Fatalf("total entries = %d, want bounded ≈ %d (per-shard slack %d)",
			n, DefaultLocationCacheMaxEntries, locationShards)
	}
	_ = first
}

// TestLocationCacheOrderDedupOnHotIdChurn（票 07 语义迁移）：热 id 的
// 「回填→失效→再回填」churn 下簿记不得无界增长——LRU 形态（map 为真相
// 源 + 链表节点唯一）结构上无重复追加，断言收紧为「churn 后链表与 map
// 恰等量、热 id 驻留」。
func TestLocationCacheOrderDedupOnHotIdChurn(t *testing.T) {
	c := shortCache() // 正 400ms——churn 循环内不过期
	id := mustID(t, "hot-churn")
	loc := directory.Owner{Node: 3}
	for i := 0; i < 64; i++ {
		c.PutPositive(id, loc)
		c.Invalidate(id) // 钩子①/②/④形态的逐次失效
	}
	c.PutPositive(id, loc) // 终态：热 id 驻留
	sh := c.shardOf(id)
	sh.mu.Lock()
	lruLen, liveLen := sh.lru.Len(), len(sh.entries)
	sh.mu.Unlock()
	if lruLen != liveLen || liveLen != 1 {
		t.Fatalf("bookkeeping = lru %d / entries %d, want exactly 1/1 (no churn growth)", lruLen, liveLen)
	}
	if _, found, _ := c.Get(id); !found {
		t.Fatal("hot id must remain cached after churn")
	}
}
