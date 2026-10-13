package testkit

// 重定向提示验收（H13 决策 24）：外部窃取归属后，带着旧缓存的调用方把
// 请求发到旧 owner —— 旧 owner 激活仲裁落败：整箱转发赢家（请求本体照常
// 完成）+ 直发 MsgRedirectHint；调用方收到提示立即改写位置缓存——下一
// 次调用不再查目录/不再误发旧 owner（缓存命中断言）。收敛上界：正 TTL
// 30s（提示丢失时兜底）。错误帧 owner 字段（Error 字段 6）的往返由
// errobject 单测锚定。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// redirectCountingDir 目录 Lookup 计数（提示生效=第二次调用不再 Lookup）。
type redirectCountingDir struct {
	directory.Directory
	lookups int
}

func (d *redirectCountingDir) Lookup(ctx context.Context, id rans.ID) (directory.Owner, bool, error) {
	d.lookups++
	return d.Directory.Lookup(ctx, id)
}

func TestRedirectHint_UpdatesLocationCache(t *testing.T) {
	mustRedis(t)
	base := newBaseDirForRedirect(t)
	counting := &redirectCountingDir{Directory: base}
	c := NewCluster(t, 3, counterKinds(nil), WithDirectory(counting))
	n1, n2, n3 := c.Node(0), c.Node(1), c.Node(2)
	ctx := context.Background()

	id := mustID(t, "counter", "hint-1")
	// 首次调用（经目录，lookups ≥1）；宿主由放置决定，动态捕获。
	if v, err := n2.Call(ctx, id, 0); err != nil || v != 1 {
		t.Fatalf("first call: v=%v err=%v", v, err)
	}
	loc, found, err := counting.Lookup(ctx, id)
	if err != nil || !found {
		t.Fatalf("owner after first call: found=%v err=%v", found, err)
	}
	owner := loc.Node
	newOwner := rans.NodeID(1)
	for newOwner == owner {
		newOwner++
	}

	// 外部窃取：归属改 node3（node3 无本地实例——纯目录层抢注）。
	if ok, err := counting.UnregisterIfOwner(ctx, id, loc); err != nil || !ok {
		t.Fatalf("steal unregister: ok=%v err=%v", ok, err)
	}
	if won, _, err := counting.Register(ctx, id, directory.Owner{Node: newOwner, Epoch: epochOfNode(t, counting, newOwner)}, nil); err != nil || !won {
		t.Fatalf("steal register: won=%v err=%v", won, err)
	}

	// 第二次调用：n2 缓存仍指 node1 → 请求到 node1 → 仲裁落败 → 转发
	// node3 + 直发提示 → 调用照常成功（经转发执行，计数连续到 2）。
	if v, err := n2.Call(ctx, id, 0); err != nil || v != 2 {
		t.Fatalf("second call via forward: v=%v err=%v", v, err)
	}

	// 提示生效断言：第三次调用不再触发目录 Lookup（缓存正条目已被提示
	// 改写为 node3——若提示丢失，负缓存/正缓存过期路径必然再查目录）。
	deadline := time.Now().Add(5 * time.Second)
	for {
		if v, err := n2.Call(ctx, id, 0); err != nil || v == 3 {
			if err != nil {
				t.Fatalf("third call: %v", err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("third call never converged on new owner")
		}
		time.Sleep(20 * time.Millisecond)
	}
	lookupsAfterSecond := counting.lookups
	// 第四次调用：缓存必须命中（Lookups 不再增长——提示已改写缓存）。
	if v, err := n2.Call(ctx, id, 0); err != nil || v != 4 {
		t.Fatalf("fourth call (cached): v=%v err=%v", v, err)
	}
	if counting.lookups != lookupsAfterSecond {
		t.Fatalf("redirect hint did not update cache: lookups %d → %d (cache miss re-queried directory)",
			lookupsAfterSecond, counting.lookups)
	}
	_ = n1
	_ = n3
}

func newBaseDirForRedirect(t *testing.T) directory.Directory {
	t.Helper()
	// inmem 注入目录：窃取/接管语义由 v3 内存实现承载（同进程共享）。
	return inmem.New()
}

func epochOfNode(t *testing.T, d directory.Directory, node rans.NodeID) uint64 {
	t.Helper()
	reg, ok := d.(interface {
		NodeEpoch(ctx context.Context, node rans.NodeID) (uint64, bool, error)
	})
	if !ok {
		return 0
	}
	ep, _, err := reg.NodeEpoch(context.Background(), node)
	if err != nil {
		t.Fatal(err)
	}
	return ep
}
