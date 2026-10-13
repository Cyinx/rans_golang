package lifecycle

// 失败迁移的过期随迁包复现（审查报告 M6）：migCore 次序为快照→Store.Put→
// 随迁包直发→Transfer CAS，CAS 判输时前三步已发生——曾收包的节点缓存里留下
// 迁移时刻快照 S1（TTL 窗内有效），而判输后源端回退 Active 继续服役，周期
// sweeper 以同 incarnation 把更新的 S2 落 Store。接管恢复若包命中即不读
// Store，S1 会以接管新纪元写回 Store 高水位——已确认的 S2 被静默回滚。
// 复现配方：Transfer 桩成首跑判输 + 源端续落新快照 + 接管恢复断言读 S2。

import (
	"context"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/activation"
)

// staleCluster 双 manager 迁移夹具（fenceCluster 变体）：源（node 1）/目标
// （node 2）共享目录与 Store；Transfer 桩成首跑判输（条目消失——migCore
// 文档化的输形态，接管侧以 Register 胜出）；sendPackage 回调把随迁包喂给
// 目标端 loader（等价 dispatcher 落缓存）。
type staleCluster struct {
	dir    *fakeTransferDir
	store  *fenceProbeStore
	source *activation.Table
	target *activation.Table
	loader *chanLoader
}

// newStaleCluster 构造夹具（目标端实体经 EntityOf 取用——恢复结果的读点）。
func newStaleCluster(t *testing.T) *staleCluster {
	t.Helper()
	c := &staleCluster{dir: newFakeTransferDir(), store: newFenceProbeStore(), loader: &chanLoader{}}
	c.dir.override = func(_ context.Context, id rans.ID, _ directory.Owner, _ directory.Owner) (bool, uint64, directory.Owner, error) {
		// 首跑判输：条目竞态消失形态（换锁 CAS 落败，无特定输因）——
		// 目录条目让位，接管方以 Register 胜出（接管/重激活链）。
		c.dir.mu.Lock()
		delete(c.dir.m, id)
		c.dir.mu.Unlock()
		return false, 0, directory.Owner{}, nil
	}
	var targetEnt *acctEntity
	c.source = activation.New(c.dir, 1, map[string]rans.Factory{
		"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} },
	}, activation.Options{
		Store:     c.store,
		Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 2, true },
		SendPackage: func(_ context.Context, _ rans.NodeID, _ rans.ID, snap []byte, srcInc uint64) error {
			c.loader.supply(append([]byte(nil), snap...), srcInc)
			return nil
		},
	})
	c.target = activation.New(c.dir, 2, map[string]rans.Factory{
		"acct": func(_ rans.ID) rans.Entity {
			targetEnt = &acctEntity{}
			return targetEnt
		},
	}, activation.Options{Store: c.store, StateLoader: c.loader})
	return c
}

// TestMigrationLostStalePackageTakeoverRestoresLatest：首跑判输 → 源端续落
// 新快照 S2 → 曾收过期包（S1）的节点接管 → 恢复必须落到 S2（最新已确认
// 状态），S1 不得以接管新纪元写回 Store 高水位覆盖 S2。
func TestMigrationLostStalePackageTakeoverRestoresLatest(t *testing.T) {
	c := newStaleCluster(t)
	id := mustTestID(t, "acct", "stale-pkg")
	ctx := context.Background()

	p1, err := c.source.GetOrActivate(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p1, "source pawnite")
	if _, err := c.source.InvokeLocal(ctx, id, "add:1"); err != nil {
		t.Fatal(err)
	}

	// 迁移首跑判输：快照（余额 1）→ Store.Put → 随迁包直发已发生，CAS 输。
	if err := Migrate(ctx, c.source, id); err == nil {
		t.Fatal("stubbed first-run transfer must lose")
	}
	waitActive(t, p1, "source pawnite after loss (reverted Active)")

	// 源端继续确认变更（余额 2）并以同 incarnation 落新快照 S2（周期
	// sweeper 语义：判输不换 incarnation，同纪元覆盖允许）。
	if _, err := c.source.InvokeLocal(ctx, id, "add:1"); err != nil {
		t.Fatal(err)
	}
	if n := CheckpointSweep(ctx, c.source, 10); n != 1 {
		t.Fatalf("post-loss sweep candidates = %d, want 1", n)
	}
	waitFor(t, "S2 landing", func() bool {
		b, ok, _ := c.store.Get(ctx, id)
		return ok && string(b) == "2"
	})

	// 源端退场后由曾收过期包的节点接管：恢复必须读 S2。
	p2, err := c.target.GetOrActivate(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p2, "takeover pawnite")
	ent, ok := c.target.EntityOf(id)
	if !ok {
		t.Fatal("takeover entity vanished from the local table")
	}
	targetEnt := ent.(*acctEntity)
	if targetEnt.bal != 2 {
		t.Fatalf("takeover restored balance = %d, want 2 (latest confirmed state, not first-run snapshot 1)", targetEnt.bal)
	}
	if b, ok, _ := c.store.Get(ctx, id); !ok || string(b) != "2" {
		t.Fatalf("store after takeover = (%q, %v), want S2 %q (stale package must not overwrite it)", b, ok, "2")
	}
}
