package contract

// Register 纪元下限（floor）契约用例：self.Incarnation 作纪元下限（0 =
// 无下限）——三个赢分支统一 inc = max(现值, floor)+1；CAS 比对与输家
// 返回不受 floor 影响。经 RunRegisterSuite 调用，inmem/redis/cluster
// 自动覆盖。

import (
	"context"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

// floorSelf 构造带下限的注册归属（Incarnation 即 floor）。
func floorSelf(node rans.NodeID, epoch, floor uint64) directory.Owner {
	return directory.Owner{Node: node, Epoch: epoch, Incarnation: floor}
}

// plainSelf 构造无下限的注册归属（既有调用方形态）。
func plainSelf(node rans.NodeID, epoch uint64) directory.Owner {
	return floorSelf(node, epoch, 0)
}

// mustRegister 注册失败即 Fatal，返回赢侧归属（含新纪元）。
func mustRegister(t *testing.T, ctx context.Context, d directory.Directory, id rans.ID, self directory.Owner, expected *directory.Owner) directory.Owner {
	t.Helper()
	won, cur, err := d.Register(ctx, id, self, expected)
	if err != nil {
		t.Fatal(err)
	}
	if !won {
		t.Fatalf("register must win: self=%v", self)
	}
	return cur
}

// runFloorSuite 纪元下限契约用例体（由 RunRegisterSuite 调用）。
func runFloorSuite(t *testing.T, ctx context.Context, newDir func(t *testing.T) directory.Directory) {
	t.Run("纪元下限：新条目/重注册/收养/CAS/输家/跳号续接", func(t *testing.T) {
		d := newDir(t)
		mk := func(name string) rans.ID {
			id, err := rans.NewID("player", name)
			if err != nil {
				t.Fatal(err)
			}
			return id
		}

		// 新条目：floor=7 → inc=8；floor=0 → 1。
		if cur := mustRegister(t, ctx, d, mk("floor-new"), floorSelf(1, 5, 7), nil); cur.Incarnation != 8 {
			t.Fatalf("fresh entry floor=7 inc = %d, want 8", cur.Incarnation)
		}
		if cur := mustRegister(t, ctx, d, mk("floor-zero"), floorSelf(1, 5, 0), nil); cur.Incarnation != 1 {
			t.Fatalf("fresh entry floor=0 inc = %d, want 1", cur.Incarnation)
		}

		// 墓碑 + floor：残值 3 + floor=7 → 8；残值 9 + floor=7 → 10。
		low := mk("floor-tomb-low")
		if cur := mustRegister(t, ctx, d, low, plainSelf(2, 1), nil); cur.Incarnation != 1 {
			t.Fatalf("tomb setup inc = %d, want 1", cur.Incarnation)
		}
		for i := uint64(2); i <= 3; i++ {
			if cur := mustRegister(t, ctx, d, low, plainSelf(2, 1), nil); cur.Incarnation != i {
				t.Fatalf("tomb setup inc = %d, want %d", cur.Incarnation, i)
			}
		}
		if ok, err := d.UnregisterIfOwner(ctx, low, directory.Owner{Node: 2, Epoch: 1, Incarnation: 3}); err != nil || !ok {
			t.Fatalf("tomb setup unregister: ok=%v err=%v", ok, err)
		}
		if cur := mustRegister(t, ctx, d, low, floorSelf(3, 2, 7), nil); cur.Incarnation != 8 {
			t.Fatalf("tombstone inc=3 + floor=7 → inc = %d, want 8", cur.Incarnation)
		}

		high := mk("floor-tomb-high")
		for i := uint64(1); i <= 9; i++ {
			if cur := mustRegister(t, ctx, d, high, plainSelf(2, 1), nil); cur.Incarnation != i {
				t.Fatalf("tomb setup inc = %d, want %d", cur.Incarnation, i)
			}
		}
		if ok, err := d.UnregisterIfOwner(ctx, high, directory.Owner{Node: 2, Epoch: 1, Incarnation: 9}); err != nil || !ok {
			t.Fatalf("tomb setup unregister: ok=%v err=%v", ok, err)
		}
		if cur := mustRegister(t, ctx, d, high, floorSelf(3, 2, 7), nil); cur.Incarnation != 10 {
			t.Fatalf("tombstone inc=9 + floor=7 → inc = %d, want 10", cur.Incarnation)
		}

		// 同节点重注册计入 floor：现值 inc=5，floor=9 → 10。
		re := mk("floor-rereg")
		for i := uint64(1); i <= 5; i++ {
			mustRegister(t, ctx, d, re, plainSelf(4, 2), nil)
		}
		if cur := mustRegister(t, ctx, d, re, floorSelf(4, 2, 9), nil); cur.Incarnation != 10 {
			t.Fatalf("re-register floor=9 over inc=5 → inc = %d, want 10", cur.Incarnation)
		}

		// 遗留 epoch-0 条目的 CAS 接管计入 floor（票 15：分支 2 不再收养
		//——expected=遗留值走分支 3）：条目 epoch=0 inc=1，接管方 floor=5
		// → 6。
		adopt := mk("floor-adopt")
		legacy := mustRegister(t, ctx, d, adopt, plainSelf(5, 0), nil)
		if legacy.Epoch != 0 || legacy.Incarnation != 1 {
			t.Fatalf("adopt setup = %+v, want epoch=0 inc=1", legacy)
		}
		if cur, _, err := d.Register(ctx, adopt, floorSelf(5, 4, 5), nil); err != nil || cur {
			t.Fatalf("epoch-0 re-register must lose (no adoption): won=%v err=%v", cur, err)
		}
		if cur := mustRegister(t, ctx, d, adopt, floorSelf(5, 4, 5), &legacy); cur.Incarnation != 6 || cur.Epoch != 4 {
			t.Fatalf("epoch-0 CAS takeover floor=5 → (%d,%d), want inc=6 epoch=4", cur.Incarnation, cur.Epoch)
		}

		// CAS 接管：floor>ci → floor+1；floor<ci → ci+1；期望纪元不符仍输。
		cas := mk("floor-cas")
		hold := mustRegister(t, ctx, d, cas, plainSelf(6, 2), nil) // inc=1
		for i := uint64(2); i <= 4; i++ {
			hold = mustRegister(t, ctx, d, cas, plainSelf(6, 2), nil)
		}
		exp := directory.Owner{Node: hold.Node, Epoch: hold.Epoch, Incarnation: hold.Incarnation}
		won, cur, err := d.Register(ctx, cas, floorSelf(7, 3, 9), &exp)
		if err != nil || !won || cur.Incarnation != 10 {
			t.Fatalf("CAS floor=9 over ci=4: won=%v cur=%+v err=%v, want inc=10", won, cur, err)
		}
		exp2 := directory.Owner{Node: 7, Epoch: 3, Incarnation: 10}
		if _, cur, err := d.Register(ctx, cas, floorSelf(8, 4, 2), &exp2); err != nil || cur.Incarnation != 11 {
			t.Fatalf("CAS floor=2 over ci=10: cur=%+v err=%v, want inc=11", cur, err)
		}
		// self 用异于持有者的节点号：期望不符即输（self 若同节点会合法命中
		// 重注册分支，测不到 CAS 裁决）。
		bad := directory.Owner{Node: 7, Epoch: 3, Incarnation: 999}
		lost2, cur2, err := d.Register(ctx, cas, floorSelf(12, 5, 9), &bad)
		if err != nil || lost2 || cur2.Node != 8 || cur2.Epoch != 4 || cur2.Incarnation != 11 {
			t.Fatalf("CAS with mismatched expected must lose: won=%v cur=%+v err=%v", lost2, cur2, err)
		}

		// 输家不受 floor 影响：他实例持有，floor=9 仍输，条目原样。
		lost := mk("floor-loser")
		held := mustRegister(t, ctx, d, lost, plainSelf(9, 2), nil)
		if won, cur, err := d.Register(ctx, lost, floorSelf(10, 3, 9), nil); err != nil || won || cur != held {
			t.Fatalf("loser must keep entry intact: won=%v cur=%+v err=%v", won, cur, err)
		}
		if loc, found, err := d.Lookup(ctx, lost); err != nil || !found || loc != held {
			t.Fatalf("entry after losing register = (%v,%v,%v), want held %+v", loc, found, err, held)
		}

		// 跳号续接：floor 跳到 8 后，普通 Register 与 Transfer 各自 +1。
		cont := mk("floor-cont")
		if cur := mustRegister(t, ctx, d, cont, floorSelf(1, 5, 7), nil); cur.Incarnation != 8 {
			t.Fatalf("cont setup inc = %d, want 8", cur.Incarnation)
		}
		if cur := mustRegister(t, ctx, d, cont, plainSelf(1, 5), nil); cur.Incarnation != 9 {
			t.Fatalf("plain register after floor jump → inc = %d, want 9", cur.Incarnation)
		}
		won, newInc, _, err := d.Transfer(ctx, cont, directory.Owner{Node: 1, Epoch: 5, Incarnation: 9}, directory.Owner{Node: 2, Epoch: 5})
		if err != nil || !won || newInc != 10 {
			t.Fatalf("transfer after floor jump: won=%v newInc=%d err=%v, want 10", won, newInc, err)
		}
	})
}
