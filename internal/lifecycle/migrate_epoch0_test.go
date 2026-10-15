package lifecycle

// 迁移换锁的代际语义（票 15 重塑）：目标代际未知即失败（原 M7「启动窗
// epoch-0 条目」的前提随票 14 同步取号消亡）；捕获值语义（换锁期望=激
// 活时刻 InstanceOwner 而非当下 SelfEpoch）由 InForceEpoch 测试继续锚定。

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// regWrap 目录 + 注册表组合（迁移成员面读取的能力面）。
type regWrap struct {
	directory.Directory
	directory.NodeRegistry
}

// TestMigrate_FailsWhenTargetEpochUnknown（票 15）：registry 形态下迁移
// 目标代际取不到（目标不在成员视图/读失败）→ 迁移失败——不再以
// {target, 0} 换锁（epoch-0 归属是遗留形态，写给它只会制造下一个待接
// 管条目）。原「启动窗 epoch-0 条目迁移换锁」的前提（先激活后代际就位）
// 随票 14 同步取号消亡——捕获值语义（InstanceOwner 而非当下 SelfEpoch）
// 由 TestMigrate_InForceEpochEntryTransfers 继续锚定。
func TestMigrate_FailsWhenTargetEpochUnknown(t *testing.T) {
	// registry 形态：fake 目录叠 inmem 注册表（目标 7 无成员键——不在
	// 存活视图）。注：迁移的成员面读取走 tbl.Directory() 的
	// NodeRegistry 能力面，组合目录满足。
	dir := regWrap{Directory: newFakeTransferDir(), NodeRegistry: inmem.NewNodeRegistry(time.Minute)}
	var epoch atomic.Uint64
	epoch.Store(3)
	m, id := newArbManager(1, dir, "mig-unknown-epoch", migPlacement(7))
	m.BindSelfEpoch(epoch.Load)

	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "source pawnite")
	// 目标节点 7 不在成员视图（fake 注册表无其键）→ 迁移必须失败。
	migErr := Migrate(context.Background(), m, id)
	if migErr == nil {
		t.Fatal("migration to an epoch-unknown target must fail (no {target,0} transfer)")
	}
	if loc, found, _ := dir.Lookup(context.Background(), id); !found || loc.Node != 1 {
		t.Fatalf("ownership must stay with source: loc=%+v found=%v", loc, found)
	}
}

// TestMigrate_InForceEpochEntryTransfers 正常路径锚：代际在位时激活
// （捕获纪元=当下代际≥1）→ 迁移照常成功、期望归属携带非零捕获纪元——
// 换用捕获值不得破坏在役代际路径（也防「恒传 0」式的假修）。
func TestMigrate_InForceEpochEntryTransfers(t *testing.T) {
	dir := newFakeTransferDir()
	var epoch atomic.Uint64
	epoch.Store(3) // 代际先于激活就位（无窗口形态）
	m, id := newArbManager(1, dir, "mig-inforce", migPlacement(7))
	m.BindSelfEpoch(epoch.Load)

	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "in-force pawnite")
	captured, ok := m.InstanceOwner(id)
	if !ok || captured.Epoch != 3 {
		t.Fatalf("captured owner = (%+v, %v), want epoch 3", captured, ok)
	}

	if err := Migrate(context.Background(), m, id); err != nil {
		t.Fatalf("in-force epoch entry migration must succeed: %v", err)
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("won migration must reach terminal state")
	}
	trs := dir.transfersSnapshot()
	if len(trs) != 1 || !trs[0].won {
		t.Fatalf("transfers = %+v, want single successful CAS", trs)
	}
	if trs[0].expected.Epoch != 3 || trs[0].expected.Incarnation != captured.Incarnation {
		t.Fatalf("transfer expected = %+v, want captured epoch 3 with matching incarnation", trs[0].expected)
	}
	if loc, found, _ := dir.Lookup(context.Background(), id); !found || loc.Node != 7 {
		t.Fatalf("directory = (%+v, %v), want target 7", loc, found)
	}
}
