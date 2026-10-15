// Package contract 提供目录实现的契约测试套件（规格：契约测试可平移，作为移植主护栏）。
// inmem 与 redis 实现共用本套件。
package contract

import (
	"errors"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

// RunRegisterSuite 对目录实现（directory.Directory 合并端口）跑全部契约
// 断言（键值 v3 语义）：Register 单键四分支（无值/墓碑收养、同 node+epoch
// 重注册、CAS 接管、输返回现归属）、遗留 epoch-0 条目的 CAS 接管自愈（票 15）、同节
// 点换代 CAS 重占、注销写墓碑且纪元单调跨墓碑存续、条件注销三元全等无通
// 配、Transfer 三态（匹配赢/不符 ErrNotOwner/缺失 nil）与目标 epoch 写入。
// 并发用例钉死取号原子性（异节点竞争恰一赢、同节点收养纪元无重复）与输
// 者观测一致性。归属三元 Owner{node, epoch, inc}：套件以 (node, epoch) 线
// 注记身份（每节点独立 epoch，模拟成员代际）。阻塞到全部子测试完成；
// 测试编排面——仅测试进程内使用，非运行时路径。
func RunRegisterSuite(t *testing.T, newDir func(t *testing.T) directory.Directory) {
	ctx := t.Context() // 契约套件：ctx 随测试生命期

	mkID := func(name string) rans.ID {
		id, err := rans.NewID("player", name)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	// self 构造归属三元（inc 由目录派生，入参 0 恒被忽略）。
	self := func(node rans.NodeID, epoch uint64) directory.Owner {
		return directory.Owner{Node: node, Epoch: epoch}
	}

	t.Run("基础面：未注册条目 Lookup miss、条件注销 false", func(t *testing.T) {
		d := newDir(t)
		id := mkID("bare")
		// 未注册：Lookup miss
		if _, found, err := d.Lookup(ctx, id); err != nil || found {
			t.Fatalf("expected miss, found=%v err=%v", found, err)
		}
		// 不存在条目的条件注销：无错、返回 false
		ok, err := d.UnregisterIfOwner(ctx, id, self(1, 11))
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("unregister of a missing entry must return false without error")
		}
	})

	t.Run("分支 1：全新 id Register 赢且纪元 ≥1，Lookup 得同 Owner", func(t *testing.T) {
		d := newDir(t)
		id := mkID("fresh")
		won, owner, err := d.Register(ctx, id, self(1, 11), nil)
		if err != nil || !won {
			t.Fatalf("fresh register: won=%v err=%v", won, err)
		}
		if owner.Incarnation < 1 {
			t.Fatalf("first incarnation = %d, want ≥1", owner.Incarnation)
		}
		if owner != (directory.Owner{Node: 1, Epoch: 11, Incarnation: owner.Incarnation}) {
			t.Fatalf("won register owner = %v, want {1,11,inc}", owner)
		}
		got, found, err := d.Lookup(ctx, id)
		if err != nil || !found || got != owner {
			t.Fatalf("lookup after register: owner=%v found=%v err=%v, want %v", got, found, err, owner)
		}
	})

	t.Run("分支 4：他节点已持有时 Register 输且返回现归属（含 epoch）", func(t *testing.T) {
		d := newDir(t)
		id := mkID("held")
		if won, _, err := d.Register(ctx, id, self(1, 11), nil); err != nil || !won {
			t.Fatalf("first register: won=%v err=%v", won, err)
		}
		before, found, err := d.Lookup(ctx, id)
		if err != nil || !found {
			t.Fatalf("precondition lookup: found=%v err=%v", found, err)
		}
		won, owner, err := d.Register(ctx, id, self(2, 22), nil)
		if err != nil {
			t.Fatal(err)
		}
		if won {
			t.Fatal("register from another node must lose")
		}
		if owner != before {
			t.Fatalf("lost register owner = %v, want holder %v (with epoch)", owner, before)
		}
		after, found, err := d.Lookup(ctx, id)
		if err != nil || !found || after != before {
			t.Fatalf("entry must stay intact on lost register: before=%v after=%v found=%v err=%v", before, after, found, err)
		}
	})

	t.Run("分支 2：同 node+epoch 重注册收养且纪元严格递增", func(t *testing.T) {
		d := newDir(t)
		id := mkID("adopt")
		_, first, err := d.Register(ctx, id, self(1, 11), nil)
		if err != nil {
			t.Fatal(err)
		}
		// 同 node 同 epoch 二次注册 = 迁移目标首激活/进程重启孤儿的收养形态
		won, owner, err := d.Register(ctx, id, self(1, 11), nil)
		if err != nil || !won {
			t.Fatalf("self-pointing register: won=%v err=%v", won, err)
		}
		if owner.Incarnation <= first.Incarnation {
			t.Fatalf("adoption must bump incarnation: %d then %d", first.Incarnation, owner.Incarnation)
		}
		if owner.Node != 1 || owner.Epoch != 11 {
			t.Fatalf("adopted owner = %v, want {1,11,inc}", owner)
		}
		got, found, _ := d.Lookup(ctx, id)
		if !found || got != owner {
			t.Fatalf("lookup after adoption: %v found=%v, want %v", got, found, owner)
		}
	})

	t.Run("遗留 epoch-0 条目：非收养、CAS 接管自愈（票 15）", func(t *testing.T) {
		d := newDir(t)
		id := mkID("adoptzero")
		// epoch-0 条目是遗留形态（票 14 起不再产生）——同节点/异节点以
		// 真实代际重注册不再经分支 2 收养：
		if won, cur, err := d.Register(ctx, id, self(1, 0), nil); err != nil || !won {
			t.Fatalf("epoch-0 register: won=%v err=%v", won, err)
		} else {
			_ = cur
		}
		legacy, found, err := d.Lookup(ctx, id)
		if err != nil || !found {
			t.Fatalf("legacy lookup: found=%v err=%v", found, err)
		}
		won, _, err := d.Register(ctx, id, self(1, 5), nil)
		if err != nil || won {
			t.Fatalf("non-epoch re-register must lose (no adoption): won=%v err=%v", won, err)
		}
		won, _, err = d.Register(ctx, id, self(2, 5), nil)
		if err != nil || won {
			t.Fatalf("cross-node re-register must lose (no adoption): won=%v err=%v", won, err)
		}
		// CAS 接管（分支 3，expected=遗留值）自愈：升写接管方真实代际，
		// 返回值与落库一致、可按返回值注销。
		won, owner, err := d.Register(ctx, id, self(2, 5), &legacy)
		if err != nil || !won {
			t.Fatalf("CAS takeover of legacy epoch-0: won=%v err=%v", won, err)
		}
		if owner.Node != 2 || owner.Epoch != 5 {
			t.Fatalf("taken owner = %v, want {2,5,inc}", owner)
		}
		got, found, err := d.Lookup(ctx, id)
		if err != nil || !found || got != owner {
			t.Fatalf("lookup after takeover = (%v,%v), want %v", got, found, owner)
		}
		if ok, err := d.UnregisterIfOwner(ctx, id, owner); err != nil || !ok {
			t.Fatalf("unregister by returned owner: ok=%v err=%v", ok, err)
		}
	})

	t.Run("分支 2 反例：同 node 异 epoch（换代）不收养——输返回旧代归属", func(t *testing.T) {
		d := newDir(t)
		id := mkID("genlost")
		if won, _, err := d.Register(ctx, id, self(1, 11), nil); err != nil || !won {
			t.Fatalf("first register: won=%v err=%v", won, err)
		}
		before, found, err := d.Lookup(ctx, id)
		if err != nil || !found {
			t.Fatalf("precondition lookup: found=%v err=%v", found, err)
		}
		// 同节点重启（epoch 变大）后旧条目不再属于本代：不可收养（成员代
		// 际判定使其对路由不可见，重占走 CAS 接管——分支 3）
		won, owner, err := d.Register(ctx, id, self(1, 12), nil)
		if err != nil || won {
			t.Fatalf("cross-epoch self register must lose: won=%v err=%v", won, err)
		}
		if owner != before {
			t.Fatalf("lost register owner = %v, want old-generation holder %v", owner, before)
		}
	})

	t.Run("同节点换代 CAS 重占：expected 三元全等赢且写新纪元自归属", func(t *testing.T) {
		d := newDir(t)
		id := mkID("genrecas")
		_, holder, err := d.Register(ctx, id, self(1, 11), nil)
		if err != nil {
			t.Fatal(err)
		}
		// 同节点换代（epoch 11→12）裸注册不收养（分支 2 反例）；携与现值
		// 三元全等的 expected 则按分支 3 接管——写调用方新纪元，返回值与
		// 落库一致
		won, owner, err := d.Register(ctx, id, self(1, 12), &holder)
		if err != nil || !won {
			t.Fatalf("same-node next-generation CAS: won=%v err=%v", won, err)
		}
		if owner.Node != 1 || owner.Epoch != 12 || owner.Incarnation <= holder.Incarnation {
			t.Fatalf("re-occupied owner = %v, want {1,12,inc>%d}", owner, holder.Incarnation)
		}
		got, found, err := d.Lookup(ctx, id)
		if err != nil || !found || got != owner {
			t.Fatalf("lookup after re-occupancy: %v found=%v err=%v, want %v", got, found, err, owner)
		}
	})

	t.Run("分支 3：CAS 接管——expected 匹配赢；不匹配输返回新现归属", func(t *testing.T) {
		d := newDir(t)
		id := mkID("cas")
		_, holder, err := d.Register(ctx, id, self(1, 11), nil)
		if err != nil {
			t.Fatal(err)
		}
		// expected 匹配现值 → 接管赢：写自归属、纪元严格大于旧值
		won, owner, err := d.Register(ctx, id, self(2, 22), &holder)
		if err != nil || !won {
			t.Fatalf("CAS takeover: won=%v err=%v", won, err)
		}
		if owner.Node != 2 || owner.Epoch != 22 || owner.Incarnation <= holder.Incarnation {
			t.Fatalf("takeover owner = %v, want {2,22,inc>%d}", owner, holder.Incarnation)
		}
		// 陈旧 expected（他人已接管）→ 输，返回新现归属
		won, cur, err := d.Register(ctx, id, self(3, 33), &holder)
		if err != nil || won {
			t.Fatalf("stale expected register must lose: won=%v err=%v", won, err)
		}
		if cur != owner {
			t.Fatalf("lost CAS owner = %v, want current holder %v", cur, owner)
		}
		if _, found, _ := d.Lookup(ctx, id); !found {
			t.Fatal("entry must survive lost CAS")
		}
	})

	t.Run("墓碑保单调：注销→重注册跨墓碑纪元严格递增（Store 围栏视角）", func(t *testing.T) {
		d := newDir(t)
		id := mkID("tomb")
		_, first, err := d.Register(ctx, id, self(1, 11), nil)
		if err != nil {
			t.Fatal(err)
		}
		if won, err := d.UnregisterIfOwner(ctx, id, first); err != nil || !won {
			t.Fatalf("unregister: won=%v err=%v", won, err)
		}
		if _, found, _ := d.Lookup(ctx, id); found {
			t.Fatal("tombstoned entry must be invisible to Lookup")
		}
		// 同 node 重注册：纪元从墓碑残值 +1（> first）——旧纪元的迟到持久
		// 写被 Store 围栏拒绝的前提（新 inc > 旧 inc 恒成立）
		won, second, err := d.Register(ctx, id, self(1, 11), nil)
		if err != nil || !won || second.Incarnation <= first.Incarnation {
			t.Fatalf("re-register over tombstone: won=%v owner=%v first=%d err=%v", won, second, first.Incarnation, err)
		}
		if won, err := d.UnregisterIfOwner(ctx, id, second); err != nil || !won {
			t.Fatalf("second unregister: won=%v err=%v", won, err)
		}
		// 他节点接管重注册：纪元仍严格递增（单调性跨墓碑与他节点存续）
		won, third, err := d.Register(ctx, id, self(2, 22), nil)
		if err != nil || !won || !(third.Incarnation > second.Incarnation && third.Incarnation > first.Incarnation) {
			t.Fatalf("takeover re-register over tombstone: won=%v owner=%v first=%d second=%d err=%v",
				won, third, first.Incarnation, second.Incarnation, err)
		}
	})

	t.Run("墓碑形态：重复注销 false、墓碑上他节点 Register 按分支 1 赢", func(t *testing.T) {
		d := newDir(t)
		id := mkID("tombshape")
		_, owner, err := d.Register(ctx, id, self(1, 11), nil)
		if err != nil {
			t.Fatal(err)
		}
		if won, err := d.UnregisterIfOwner(ctx, id, owner); err != nil || !won {
			t.Fatalf("first unregister: won=%v err=%v", won, err)
		}
		// 重复注销（条目已墓碑）→ false
		if won, err := d.UnregisterIfOwner(ctx, id, owner); err != nil || won {
			t.Fatalf("repeat unregister over tombstone: won=%v err=%v", won, err)
		}
		// 墓碑 = 无主：他节点直接 Register（不带 expected）即收养
		won, cur, err := d.Register(ctx, id, self(2, 22), nil)
		if err != nil || !won {
			t.Fatalf("register over tombstone: won=%v err=%v", won, err)
		}
		if cur.Node != 2 || cur.Epoch != 22 || cur.Incarnation <= owner.Incarnation {
			t.Fatalf("adopted owner = %v, want {2,22,inc>%d}", cur, owner.Incarnation)
		}
	})

	t.Run("Transfer：expected 匹配赢且新纪元严格大于旧；陈旧纪元不换锁", func(t *testing.T) {
		d := newDir(t)
		id := mkID("transfer")
		_, owner, err := d.Register(ctx, id, self(1, 11), nil)
		if err != nil {
			t.Fatal(err)
		}
		// expected 陈旧（旧纪元）→ 不换锁（ErrNotOwner 哨兵），条目原样
		stale := directory.Owner{Node: 1, Epoch: 11, Incarnation: owner.Incarnation - 1}
		if _, _, cur, err := d.Transfer(ctx, id, stale, self(2, 22)); cur != owner || !errors.Is(err, directory.ErrNotOwner) {
			t.Fatalf("stale-inc transfer: cur=%v err=%v, want (false, holder, ErrNotOwner)", cur, err)
		}
		if got, found, _ := d.Lookup(ctx, id); !found || got != owner {
			t.Fatalf("entry must stay intact on stale transfer: got=%v found=%v, want %v", got, found, owner)
		}
		// 条目不存在 → (false, zero, nil)
		absent := mkID("absent")
		if won, _, cur, err := d.Transfer(ctx, absent, owner, self(2, 22)); err != nil || won || cur != (directory.Owner{}) {
			t.Fatalf("transfer of missing entry: won=%v cur=%v err=%v, want (false, zero, nil)", won, cur, err)
		}
		// expected 匹配 → ok，新纪元严格大于旧，Lookup 归 target（node+epoch）
		won, newInc, cur, err := d.Transfer(ctx, id, owner, self(2, 22))
		if err != nil || !won {
			t.Fatalf("matching transfer: won=%v err=%v", won, err)
		}
		if newInc <= owner.Incarnation {
			t.Fatalf("transfer incarnation %d must exceed old %d", newInc, owner.Incarnation)
		}
		if cur != (directory.Owner{Node: 2, Epoch: 22, Incarnation: newInc}) {
			t.Fatalf("transfer current = %v, want {2,22,%d}", cur, newInc)
		}
		got, found, _ := d.Lookup(ctx, id)
		if !found || got != cur {
			t.Fatalf("lookup after transfer: %v found=%v, want %v", got, found, cur)
		}
		// 墓碑不可让渡：先注销再 Transfer → ErrNotOwner（无主条目）
		if won2, err := d.UnregisterIfOwner(ctx, id, cur); err != nil || !won2 {
			t.Fatalf("unregister after transfer: won=%v err=%v", won2, err)
		}
		if _, _, _, err := d.Transfer(ctx, id, cur, self(3, 33)); !errors.Is(err, directory.ErrNotOwner) {
			t.Fatalf("transfer over tombstone: err=%v, want ErrNotOwner", err)
		}
	})

	t.Run("实例级条件注销：三元全匹配才墓碑化（实例级核心用例）", func(t *testing.T) {
		d := newDir(t)
		id := mkID("unreg")
		_, owner, err := d.Register(ctx, id, self(1, 11), nil)
		if err != nil {
			t.Fatal(err)
		}
		// 部分匹配（node 相同但 epoch/inc 不符；node 不同）→ false，条目存活
		for _, wrong := range []directory.Owner{
			{Node: 1, Epoch: 11, Incarnation: owner.Incarnation + 1},
			{Node: 1, Epoch: 12, Incarnation: owner.Incarnation},
			{Node: 2, Epoch: 11, Incarnation: owner.Incarnation},
		} {
			if ok, err := d.UnregisterIfOwner(ctx, id, wrong); err != nil || ok {
				t.Fatalf("unregister with %v: ok=%v err=%v, want false", wrong, ok, err)
			}
		}
		if _, found, _ := d.Lookup(ctx, id); !found {
			t.Fatal("entry must survive mismatched unregisters")
		}
		// 全匹配（三元）→ true，条目墓碑化（Lookup miss）
		if ok, err := d.UnregisterIfOwner(ctx, id, owner); err != nil || !ok {
			t.Fatalf("exact-instance unregister: ok=%v err=%v", ok, err)
		}
		if _, found, _ := d.Lookup(ctx, id); found {
			t.Fatal("entry must be invisible after exact-instance unregister")
		}
	})

	t.Run("条件注销无通配：零值 epoch/inc 的 Owner 不命中任何条目", func(t *testing.T) {
		d := newDir(t)
		id := mkID("nowild")
		_, owner, err := d.Register(ctx, id, self(1, 11), nil)
		if err != nil {
			t.Fatal(err)
		}
		// 仅 node 匹配、epoch/inc 缺省的 Owner 是漏填形态：三元全等判据下
		// 必须 no-op false（条目完好）——不存在按 node 通配的节点粒度清扫
		if ok, err := d.UnregisterIfOwner(ctx, id, directory.Owner{Node: 1}); err != nil || ok {
			t.Fatalf("wildcard-shaped unregister: ok=%v err=%v, want false no-op", ok, err)
		}
		got, found, err := d.Lookup(ctx, id)
		if err != nil || !found || got != owner {
			t.Fatalf("entry must survive wildcard-shaped unregister: %v found=%v err=%v, want %v", got, found, err, owner)
		}
	})

	t.Run("并发 Register（异节点竞争）：恰一赢且输者返回持有者", func(t *testing.T) {
		d := newDir(t)
		id := mkID("racy-distinct")
		const n = 32
		type outcome struct {
			won   bool
			owner directory.Owner
		}
		out := make(chan outcome, n)
		for i := 0; i < n; i++ {
			go func(node rans.NodeID) {
				won, owner, err := d.Register(ctx, id, self(node, uint64(100+node)), nil)
				if err != nil {
					t.Error(err)
				}
				out <- outcome{won: won, owner: owner}
			}(rans.NodeID(i + 1))
		}
		wins, holders := 0, map[directory.Owner]int{}
		var winnerOwner directory.Owner
		for i := 0; i < n; i++ {
			o := <-out
			if o.won {
				wins++
				winnerOwner = o.owner
			}
			holders[o.owner]++
		}
		if wins != 1 {
			t.Fatalf("concurrent register winners = %d, want 1", wins)
		}
		// 全部输者观测到同一持有者（赢家）：CAS 竞争的原子性判据
		if holders[winnerOwner] != n {
			t.Fatalf("all %d racers must observe the single winner owner %v, observed owners = %v", n, winnerOwner, holders)
		}
		got, found, err := d.Lookup(ctx, id)
		if err != nil || !found || got != winnerOwner {
			t.Fatalf("lookup after race: %v found=%v err=%v, want winner owner %v", got, found, err, winnerOwner)
		}
	})

	t.Run("并发 Register（同节点收养）：全赢且纪元无重复", func(t *testing.T) {
		d := newDir(t)
		id := mkID("racy-adopt")
		const workers, per = 8, 25
		got := make(chan uint64, workers*per)
		for w := 0; w < workers; w++ {
			go func() {
				for i := 0; i < per; i++ {
					won, owner, err := d.Register(ctx, id, self(9, 9), nil)
					if err != nil || !won {
						t.Errorf("concurrent adoption register: won=%v err=%v", won, err)
						got <- 0 // 占位保序（0 非法纪元——首个为 1，仅错误路径出现）
						continue
					}
					got <- owner.Incarnation
				}
			}()
		}
		seen := make(map[uint64]bool, workers*per)
		for i := 0; i < workers*per; i++ {
			n := <-got
			if n != 0 && seen[n] {
				t.Fatalf("duplicate incarnation %d", n)
			}
			seen[n] = true
		}
		if len(seen) != workers*per {
			t.Fatalf("distinct incarnations = %d, want %d", len(seen), workers*per)
		}
	})

	runFloorSuite(t, ctx, newDir)
}
