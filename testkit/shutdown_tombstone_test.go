package testkit

// 优雅停机批量墓碑化验收（H13 决策 23）：node.Shutdown 的排空阶段②逐实体
// Retire → 条件注销改写无主墓碑——停机完成后本节点全部实体条目 Lookup
// miss（崩溃残留才常驻；优雅停机必须腾位，调用方重试即重激活）。

import (
	"context"

	"github.com/cyinx/rans_golang"
	"testing"
	"time"
)

func TestShutdown_BatchTombstonesAll(t *testing.T) {
	mustRedis(t)
	c := NewCluster(t, 2, counterKinds(nil), WithRedis(redisAddr(), 2*time.Second, 300*time.Millisecond))
	n1 := c.Node(0)
	ctx := context.Background()
	dir := c.Directory()

	ids := []rans.ID{
		mustID(t, "counter", "sdtomb-1"),
		mustID(t, "counter", "sdtomb-2"),
		mustID(t, "counter", "sdtomb-3"),
	}
	for _, id := range ids {
		if v, err := n1.Call(ctx, id, 0); err != nil || v != 1 {
			t.Fatalf("activation %s: v=%v err=%v", id.String(), v, err)
		}
	}
	for _, id := range ids {
		if _, found, _ := dir.Lookup(ctx, id); !found {
			t.Fatalf("precondition: %s must be owned before shutdown", id.String())
		}
	}

	if err := n1.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	for _, id := range ids {
		if _, found, err := dir.Lookup(ctx, id); err != nil || found {
			t.Fatalf("entry %s must be tombstoned after graceful shutdown: found=%v err=%v",
				id.String(), found, err)
		}
	}
}
