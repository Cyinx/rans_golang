package testkit

// 生成物×双节点冒烟（审查报告 §5 补测建议三）：ransgen 生成实体代理首次
// 上真 wire 跑行为断言——生成物（消费方 wanju_agent_server 的实际集成
// 形态）此前只在 examples/chat 有一跨节点演示而无断言，序列线型编解码
// 只存在于生成物从未在测试中真跑。三腿各一：
//  1. 跨节点 Call——请求与应答的 []string 经生成序列化器在真 TCP wire 上
//     往返（WireSequence 路径首跑）；
//  2. 跨节点 Tell——OneWay 无应答无回执，送达以状态可见为准；
//  3. 迁移——有状态实体（快照面）从 node1 迁到 node2，快照状态随行、
//     跨节点 Call 在新家继续正确。
//
// 装配口径：目录 = 缺省 inmem（迁移需实例级换锁面，inmem 具备）；有状态
// 实体的迁移以配置 Store 为前提（ErrMigrationNoStore 拒迁语义）——
// WithCheckpointStore 双端共享同一实例。实体面 = testkit/genfixture
// （kind=gen；生成物入库 + 漂移锁，消费方集成惯例与 examples/chat 同款）。

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/testkit/genfixture"
)

// TestAcceptance_GenEntityCrossNodeSmoke 生成物×双节点三腿冒烟（Call/
// Tell/迁移）。编排：node1 首调本地激活（未武装选点 miss 一律本地）→
// node2 经目录远程调用 → Tell 送达读回 → 武装选点固定 node2 迁移 →
// 归属与状态连续性断言。迁移前行清单（含 Tell 腿的标记行）只可能经
// 快照随行到达 node2——值连续即快照面+生成序列化器双双正确的证据。
func TestAcceptance_GenEntityCrossNodeSmoke(t *testing.T) {
	migPlacement := &migrateArmedPlacement{target: 2}
	c := NewCluster(t, 2, genfixture.Kinds, WithCheckpointStore(checkpoint.New()),
		WithNodePlacement(0, migPlacement)) // 源 node1 迁移选点武装后固定 node2
	src, dst := c.Node(0), c.Node(1)
	ctx := context.Background()
	id := mustID(t, "gen", "note-1")

	fromSrc := genfixture.NotebookClient{C: src.Client()}
	fromDst := genfixture.NotebookClient{C: dst.Client()}

	// 腿 1：跨节点 Call。node1 首调本地激活并确认归属；node2 经目录远程
	// 调用——[]string 请求与应答双侧过生成序列化器，值逐项往返断言。
	got, err := fromSrc.Append(ctx, id, []string{"a"})
	if err != nil {
		t.Fatalf("local warm-up Append: %v", err)
	}
	if !slices.Equal(got.Lines, []string{"a"}) {
		t.Fatalf("local Append reply = %v, want [a]", got.Lines)
	}
	if !src.Holds(id) {
		t.Fatal("entity must activate on node1 after the local warm-up call")
	}
	got, err = fromDst.Append(ctx, id, []string{"b", "c"})
	if err != nil {
		t.Fatalf("remote Append (node2→node1): %v", err)
	}
	if want := []string{"a", "b", "c"}; !slices.Equal(got.Lines, want) {
		t.Fatalf("remote Append reply = %v, want %v", got.Lines, want)
	}

	// 腿 2：跨节点 Tell。OneWay 入箱即返回——送达以状态可见为准；同目标
	// FIFO（Tell 先于后续 Call 出箱）本已确定，有界轮询作容忍兜底。
	if err := fromDst.Mark(ctx, id, "tell-leg"); err != nil {
		t.Fatalf("remote Mark (Tell): %v", err)
	}
	delivered := false
	deadline := time.After(5 * time.Second)
	for !delivered {
		got, err = fromDst.Append(ctx, id, nil)
		if err != nil {
			t.Fatalf("Append after Tell: %v", err)
		}
		if delivered = slices.Contains(got.Lines, "mark:tell-leg"); delivered {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("Tell delivery not visible within deadline: %v", got.Lines)
		case <-time.After(10 * time.Millisecond):
		}
	}

	// 腿 3：迁移。武装选点固定 node2 → Migrate 赢（换锁 CAS 是线性化点；
	// 随迁包尽力而为，目标端激活由首笔路由请求驱动）→ 源逐出、目录翻向
	// 目标 → 迁移后从原源节点远程 Call：状态连续（含 Tell 腿标记行）且新
	// 追加行经生成序列化器往返；归属在目标落定为终态断言。
	migPlacement.arm()
	if err := src.Migrate(ctx, id); err != nil {
		t.Fatalf("Migrate to node2: %v", err)
	}
	if src.Holds(id) {
		t.Fatal("source must evict the entity after migration won")
	}
	if loc, found, _ := c.Directory().Lookup(ctx, id); !found || loc.Node != 2 {
		t.Fatalf("directory after migration = (%v, %v), want node 2", loc, found)
	}
	got, err = fromSrc.Append(ctx, id, []string{"after-migrate"})
	if err != nil {
		t.Fatalf("post-migration remote Append (node1→node2): %v", err)
	}
	want := []string{"a", "b", "c", "mark:tell-leg", "after-migrate"}
	if !slices.Equal(got.Lines, want) {
		t.Fatalf("post-migration Append reply = %v, want %v", got.Lines, want)
	}
	pollUntil(t, 5*time.Second, "target activation after migration", func() bool {
		return dst.Holds(id)
	})
}
