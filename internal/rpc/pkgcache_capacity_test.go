package rpc

// PackageCache 容量上界契约（与 LocationCache 同款语义的孪生面）：
// 惰性过期只清「同 id 再访问」的条目——仲裁输家残留包、迁移
// 取消后目标端永不激活、实体迁走后的 straggler 包在高迁移 churn 下会无界
// 驻留，容量上限即对冲。容量语义与 LocationCache 同款：满员准入先清过期，
// 仍满按插入序
// FIFO 逐出最旧存活条目；order 簿记墓碑压缩防无界增长。

import (
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// capID 构造测试身份。
func capID(t *testing.T, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID("mig", inst)
	if err != nil {
		t.Fatalf("rans.NewID: %v", err)
	}
	return id
}

// 1. FIFO 逐出：容量 2 下第三个 id 准入逐出最旧者（时钟冻结——全部新鲜，
// 只验证逐出面）。
func TestPackageCacheMaxEntriesFIFOEviction(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	c := NewPackageCache(0)
	c.now = clock.Now
	c.MaxEntries = 2
	id1, id2, id3, id4 := capID(t, "p1"), capID(t, "p2"), capID(t, "p3"), capID(t, "p4")
	for _, id := range []rans.ID{id1, id2, id3} { // id1 被逐出
		c.Put(id, migSnap(1), 0)
	}
	if _, _, ok := c.Take(id1); ok {
		t.Fatal("oldest package must be FIFO-evicted at capacity")
	}
	for _, id := range []rans.ID{id2, id3} {
		if _, _, ok := c.Take(id); !ok {
			t.Fatalf("recent packages must survive, %v evicted", id)
		}
	}
	c.Put(id4, migSnap(2), 0) // 容量回到 2（两次 Take 消费），再准入不受逐出
	if _, _, ok := c.Take(id4); !ok {
		t.Fatal("fresh admission within capacity must survive")
	}
}

// 2. 满员准入先清过期：容量 1，id1 过期后 id2 准入 → 过期清扫腾位，无需
// FIFO 逐出（Take(id2) 命中）。
func TestPackageCacheExpiryPreferredOverEviction(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	c := NewPackageCache(time.Minute)
	c.now = clock.Now
	c.MaxEntries = 1
	id1, id2 := capID(t, "exp1"), capID(t, "exp2")
	c.Put(id1, migSnap(1), 0)
	clock.Advance(2 * time.Minute) // id1 过期
	c.Put(id2, migSnap(2), 0)      // 满员准入：过期清扫腾位
	if _, _, ok := c.Take(id2); !ok {
		t.Fatal("admission must reuse the slot freed by the expiry sweep")
	}
}

// 3. consume-once 的 Take 消费留墓碑：墓碑不占逐出名额（逐出目标是最旧的
// 存活条目），order 簿记经压缩有界。
func TestPackageCacheTakeTombstonesDontEvict(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	c := NewPackageCache(0)
	c.now = clock.Now
	c.MaxEntries = 2
	ids := []rans.ID{capID(t, "t1"), capID(t, "t2"), capID(t, "t3"), capID(t, "t4")}
	for _, id := range ids[:2] {
		c.Put(id, migSnap(1), 0)
	}
	if _, _, ok := c.Take(ids[0]); !ok { // t1 消费 → FIFO 头变墓碑
		t.Fatal("first Take must hit")
	}
	c.Put(ids[2], migSnap(1), 0) // 存活 t2/t3（满员）
	c.Put(ids[3], migSnap(1), 0) // 逐出最旧存活 t2（墓碑 t1 不占名额）
	if _, _, ok := c.Take(ids[1]); ok {
		t.Fatal("oldest LIVE package must be evicted, not the tombstone's neighbor")
	}
	for _, id := range ids[2:] {
		if _, _, ok := c.Take(id); !ok {
			t.Fatalf("live packages must survive, %v evicted", id)
		}
	}
}

// 4. order 簿记压缩：高 churn（Take 产墓碑）下 order 切片不随删除无界增长。
func TestPackageCacheOrderCompaction(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	c := NewPackageCache(0)
	c.now = clock.Now
	c.MaxEntries = 4
	for i := 0; i < 64; i++ {
		id := capID(t, "churn")
		c.Put(id, migSnap(1), 0)
		if _, _, ok := c.Take(id); !ok {
			t.Fatalf("Take at iteration %d must hit", i)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.order) > 2*len(c.m)+8 {
		t.Fatalf("order bookkeeping = %d for %d live entries, want compacted", len(c.order), len(c.m))
	}
}
