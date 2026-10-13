package lifecycle

// epoch-0 启动窗口条目的迁移换锁复现（审查报告 M7）：条目纪元是激活时刻的
// 捕获值（注册原子域返回的自归属，逐实例捕获进表）——启动窗口（节点即刻
// 可服务先于 liveness 首轮取号）内 SelfEpoch 仍为 0，条目以 Epoch=0 落目录；
// 代际就位后（SelfEpoch≥1）若换锁期望纪元读当下 SelfEpoch()，与条目捕获值
// 错位——三元比对 0≠N 恒 ErrNotOwner，窗口内激活的实体此后每次迁移都失败
//（静默：实体保持 Active）。真实窗口 µs 级，本测试把「先激活、后代际就位」
// 钉成确定性次序（代际读取面绑定量值），与真实启动窗口的次序错位同构。

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestMigrate_Epoch0EntryTransfersWithCapturedOwner 窗口内激活（捕获纪元
// 0）→ 代际就位（当下 SelfEpoch≥1）→ 迁移必须成功：换锁期望归属用激活时刻
// 捕获值（Table.InstanceOwner），不得读当下 SelfEpoch()。
func TestMigrate_Epoch0EntryTransfersWithCapturedOwner(t *testing.T) {
	dir := newFakeTransferDir()
	var epoch atomic.Uint64 // 0 = 启动窗内（liveness 未取号）
	m, id := newArbManager(1, dir, "mig-epoch0", migPlacement(7))
	m.BindSelfEpoch(epoch.Load)

	// 窗口内激活：条目以 Epoch=0 落目录，捕获归属即注册返回值。
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "startup-window pawnite")
	captured, ok := m.InstanceOwner(id)
	if !ok || captured.Epoch != 0 {
		t.Fatalf("captured owner = (%+v, %v), want epoch 0 (window semantics must hold)", captured, ok)
	}

	// liveness 首轮取号：代际就位（真实启动窗关闭后的常态）。
	epoch.Store(5)

	// 复现断言：epoch-0 条目迁移必须成功；期望纪元读当下 SelfEpoch() 时
	// 三元比对恒 ErrNotOwner（迁移输，实体回 Active）。
	if err := Migrate(context.Background(), m, id); err != nil {
		t.Fatalf("epoch-0 entry migration must succeed with the captured owner: %v", err)
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("won migration must reach terminal state")
	}
	if loc, found, _ := dir.Lookup(context.Background(), id); !found || loc.Node != 7 {
		t.Fatalf("directory = (%+v, %v), want target 7", loc, found)
	}
	// 换锁台账精确锚定：期望归属恰为捕获值（node+捕获纪元+inc 三元），
	// 而非当下 SelfEpoch 拼出的归属。
	trs := dir.transfersSnapshot()
	if len(trs) != 1 || !trs[0].won || trs[0].err != nil {
		t.Fatalf("transfers = %+v, want single successful CAS", trs)
	}
	if trs[0].expected != captured {
		t.Fatalf("transfer expected = %+v, want captured owner %+v (activation-time value)", trs[0].expected, captured)
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
