package routing

// 票 07 红测：位置缓存 LRU 化——容量饱和下持续命中的条目不被逐出（修复
// 前 FIFO：最旧插入先逐，热点条目周期性被逐出）；InvalidateNode 行为面
// 回归锚（其他节点条目保留、目标节点条目全失效、负条目不受影响）。

import (
	"fmt"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

func mustCacheID(t *testing.T, kind, key string) rans.ID {
	t.Helper()
	id, err := rans.NewID(kind, key)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestLocationCache_HotEntrySurvivesSaturation 同分片饱和准入下，持续
// Get 刷新的热条目必须存活；冷条目按 LRU 让位（修复前 FIFO 把最旧插入
// 的热条目先逐出）。
func TestLocationCache_HotEntrySurvivesSaturation(t *testing.T) {
	c := NewLocationCache(time.Minute, time.Second, locationShards*4) // 分片预算 4
	hot := mustCacheID(t, "hot", "a")
	sh := c.shardOf(hot)
	c.PutPositive(hot, directory.Owner{Node: 1})
	// 找同分片的搅动 id（分片预算 4：hot + 3 填充即满）。
	var churn []rans.ID
	for i := 0; len(churn) < 3+20 && i < 5000; i++ {
		id := mustCacheID(t, "hot", fmt.Sprintf("c%d", i))
		if c.shardOf(id) == sh {
			churn = append(churn, id)
		}
	}
	if len(churn) < 23 {
		t.Fatalf("cannot find enough same-shard ids, got %d", len(churn))
	}
	for _, id := range churn[:3] { // 填满分片
		c.PutPositive(id, directory.Owner{Node: 2})
	}
	for i, id := range churn[3:] { // 饱和准入 + 热条目持续命中刷新
		if _, ok, _ := c.Get(hot); !ok {
			t.Fatalf("hot entry evicted at churn %d (LRU must keep it alive)", i)
		}
		c.PutPositive(id, directory.Owner{Node: 2})
	}
	if _, ok, _ := c.Get(hot); !ok {
		t.Fatal("hot entry evicted after churn (LRU must keep it alive)")
	}
}

// TestLocationCache_InvalidateNodeTouchesOnlyThatNode 行为面回归锚。
func TestLocationCache_InvalidateNodeTouchesOnlyThatNode(t *testing.T) {
	c := NewLocationCache(time.Minute, time.Second, 1000)
	n2a := mustCacheID(t, "k", "n2a")
	n2b := mustCacheID(t, "k", "n2b")
	n3 := mustCacheID(t, "k", "n3")
	neg := mustCacheID(t, "k", "neg")
	c.PutPositive(n2a, directory.Owner{Node: 2})
	c.PutPositive(n2b, directory.Owner{Node: 2})
	c.PutPositive(n3, directory.Owner{Node: 3})
	c.PutNegative(neg)

	c.InvalidateNode(2)

	if _, ok, _ := c.Get(n2a); ok {
		t.Fatal("node 2 entry a must be invalidated")
	}
	if _, ok, _ := c.Get(n2b); ok {
		t.Fatal("node 2 entry b must be invalidated")
	}
	if _, ok, _ := c.Get(n3); !ok {
		t.Fatal("node 3 entry must survive")
	}
	if _, ok, _ := c.Get(neg); !ok {
		t.Fatal("negative entry must survive (invalidate-node only touches positive)")
	}
}
