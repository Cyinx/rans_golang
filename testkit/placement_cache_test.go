package testkit

// 放置正缓存验收（H14 决策 25）：routeMiss 选点结果回填短 TTL 正条目——
// 同 id 的重复请求在激活窗内不再重复触发目录 Lookup（选点数据源查询计
// 数断言）；激活窗过后条目过期（TTL 生效锚）。

import (
	"context"

	"github.com/cyinx/rans_golang"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// lookupCounting 目录查询计数（放置/路由面经 Lookup——正缓存命中时重复
// 请求不再查目录）。
type lookupCounting struct {
	directory.Directory
	lookups int
}

func (d *lookupCounting) Lookup(ctx context.Context, id rans.ID) (directory.Owner, bool, error) {
	d.lookups++
	return d.Directory.Lookup(ctx, id)
}

func TestPlacement_PositiveCacheHit(t *testing.T) {
	counting := &lookupCounting{Directory: inmem.New()}
	c := NewCluster(t, 2, counterKinds(nil), WithDirectory(counting), WithPlacement())
	n1 := c.Node(0)
	ctx := context.Background()

	id := mustID(t, "counter", "pc-1")
	if v, err := n1.Call(ctx, id, 0); err != nil || v != 1 {
		t.Fatalf("first call: v=%v err=%v", v, err)
	}
	base := counting.lookups
	// 激活窗内的重复调用：命中正缓存（放置选点不再触发数据源查询）。
	if v, err := n1.Call(ctx, id, 0); err != nil || v != 2 {
		t.Fatalf("second call: v=%v err=%v", v, err)
	}
	if counting.lookups != base {
		t.Fatalf("placement re-queried within activation window: lookups %d → %d", base, counting.lookups)
	}
}

func TestPlacement_CacheEntryExpires(t *testing.T) {
	// TTL 生效锚：条目 5s 过期后再次调用重新选点（数据源再查）。
	counting := &lookupCounting{Directory: inmem.New()}
	c := NewCluster(t, 2, counterKinds(nil), WithDirectory(counting), WithPlacement())
	n1 := c.Node(0)
	ctx := context.Background()
	id := mustID(t, "counter", "pc-ttl")
	if _, err := n1.Call(ctx, id, 0); err != nil {
		t.Fatal(err)
	}
	// 等 TTL 过期（5s 常量 + 余量）：过期后调用经负缓存/重新查目录——
	// 断言 lookups 增长（条目确实过期，非永久驻留）。
	before := counting.lookups
	time.Sleep(6 * time.Second)
	if _, err := n1.Call(ctx, id, 0); err != nil {
		t.Fatal(err)
	}
	if counting.lookups <= before {
		t.Fatal("placement cache entry never expired (lookups did not grow after TTL)")
	}
	if _, err := n1.Call(ctx, id, 0); err != nil {
		t.Fatal(err)
	}
}
