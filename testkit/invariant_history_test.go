package testkit

// 不变量历史检查器（INV1「确认即持久」）的场景矩阵与工具自测。
//
// 分层：
//   - 工具自测（TestInvHistory_* / TestCountView_*）：记录、比对、例外窗口
//     逻辑的正反两面——断言能过也能在违约时精确报错；
//   - 场景矩阵（TestAcceptance_InvMatrix_*）：钝化 / Drain / 迁移 / 强弃 /
//     Store 读写故障 / 续租丢失 / 节点 Kill 的集成面，逐场景断言
//     「确认 ⟹ 持久可见」——优雅转换（零丢失，严格）与崩溃
//    （允许损失 ≤ 最近一次成功 checkpoint，显式列报）两种语义分开断言。
//
// 记录点约定：调用方在 Call 成功返回处 Record（「被确认」的唯一定义），
// Seq 取实体返回的效果量（计数实体 = 新计数值）。测量面约定：断言前把
// 在役实体的效果量经周期 checkpoint（WithCheckpointInterval 调短的内置
// 扫描器）或转换自身的终快照收口落储，再经 Store 读持久可见效果量
//（CountView）——INV1 的「对下一次激活可见」以 Store 承载面为准。
//
// 工具面（testkit 依赖面 = 公开包 + internal/testhooks）：
//   - 落储驱动：WithCheckpointInterval 调短内置扫描器（invAwaitCheckpoint
//     轮询落储）或直写基线（invSeedCheckpoint——扫描器缺省 1min 不在秒级
//     测试窗内触发，直写基线零漂移）；
//   - 闲置策略：WithIdlePassivation（内置收集循环）；
//   - 生命周期动词：Node.Shutdown / Node.Holds；
//   - rans.checkpoint.final_failed 指标读数不在 testkit 依赖面（wiring 面
//     在 internal/activation/retire.go，其行为面由本场景的「无半写 +
//     损失列报」断言覆盖）。

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory"
)

// ---- 工具自测：InvHistory 记录与比对 ----

// CountView 经 Store 读计数实体的持久可见效果量（8 字节大端计数契约，
// accCarrier/accSnap 同款；形状不符报错）。放在测试侧：快照形状是场景
// 实体的私有契约，检查器本体保持形状无关。
func CountView(store checkpoint.Store) DurableView {
	return func(ctx context.Context, id rans.ID) (int64, bool, error) {
		b, found, err := store.Get(ctx, id)
		if err != nil || !found {
			return 0, found, err
		}
		if len(b) != 8 {
			return 0, true, fmt.Errorf("testkit: CountView: snapshot shape mismatch: %d bytes, want 8", len(b))
		}
		return accSnapVal(b), true, nil
	}
}

// idOf mustID 的无 t 版本（工具自测里批量造身份，失败 panic 让测试炸出）。
func idOf(kind, inst string) rans.ID {
	id, err := rans.NewID(kind, inst)
	if err != nil {
		panic(err)
	}
	return id
}

// constView 恒值读取面（found 可控）。
func constView(v int64, found bool) DurableView {
	return func(context.Context, rans.ID) (int64, bool, error) { return v, found, nil }
}

// errView 恒错读取面。
func errView() DurableView {
	return func(context.Context, rans.ID) (int64, bool, error) { return 0, false, errors.New("boom") }
}

// TestInvHistoryRecordAndMaxSeq 记录面：逐实体追加、MaxSeq 取最大（乱序
// 安全）、Records 返回副本（外部改不动内部状态）、未记录 id 零值。
func TestInvHistoryRecordAndMaxSeq(t *testing.T) {
	h := NewInvHistory()
	a, b := idOf("k", "a"), idOf("k", "b")
	if h.MaxSeq(a) != 0 {
		t.Fatal("未记录 id 的 MaxSeq 必须为 0")
	}
	h.Record(a, "inc", 1)
	h.Record(a, "inc", 3)
	h.Record(a, "inc", 2) // 乱序：MaxSeq 仍取 3
	h.Record(b, "inc", 7)

	if got := h.MaxSeq(a); got != 3 {
		t.Fatalf("MaxSeq(a) = %d, want 3", got)
	}
	if got := h.MaxSeq(b); got != 7 {
		t.Fatalf("MaxSeq(b) = %d, want 7", got)
	}
	recs := h.Records(a)
	if len(recs) != 3 {
		t.Fatalf("Records(a) len = %d, want 3", len(recs))
	}
	recs[0].Op = "mutated" // 副本：改不动内部
	if h.Records(a)[0].Op == "mutated" {
		t.Fatal("Records 必须返回副本")
	}
	ids := h.IDs()
	if len(ids) != 2 {
		t.Fatalf("IDs len = %d, want 2", len(ids))
	}
}

// TestInvHistoryRecordConcurrent 并发记录面（-race 下由 make test 保底）：
// 多 goroutine 各自 Record 同一 id，总量与 MaxSeq 不丢不重。
func TestInvHistoryRecordConcurrent(t *testing.T) {
	h := NewInvHistory()
	id := idOf("k", "conc")
	const goroutines, per = 8, 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 1; i <= per; i++ {
				h.Record(id, "inc", int64(g*per+i))
			}
		}(g)
	}
	wg.Wait()
	if got := len(h.Records(id)); got != goroutines*per {
		t.Fatalf("并发记录丢失: len = %d, want %d", got, goroutines*per)
	}
	if got := h.MaxSeq(id); got != goroutines*per {
		t.Fatalf("MaxSeq = %d, want %d", got, goroutines*per)
	}
}

// TestInvHistoryCheckStrict 零丢失（严格）比对面：全可见 → nil；任一实体
// 持久量 < 最高确认序 → 违约错误（点名 id 与两个量）；读失败透传；空历史
// → 恒过（无确认即无义务）。
func TestInvHistoryCheckStrict(t *testing.T) {
	ctx := context.Background()
	a := idOf("k", "strict-a")

	t.Run("all visible passes", func(t *testing.T) {
		h := NewInvHistory()
		h.Record(a, "inc", 1)
		h.Record(a, "inc", 2)
		if err := h.CheckStrict(ctx, constView(2, true)); err != nil {
			t.Fatalf("持久量 2 ≥ 确认 2 必须通过: %v", err)
		}
		// 持久量超过确认量同样通过（单向蕴含：确认 ⊆ 持久，多的效果
		// 来自未确认执行是 at-most-once 语义的合法形态，不归本检查裁）。
		if err := h.CheckStrict(ctx, constView(9, true)); err != nil {
			t.Fatalf("持久量超确认必须通过: %v", err)
		}
	})

	t.Run("missing effect fails with detail", func(t *testing.T) {
		h := NewInvHistory()
		h.Record(a, "inc", 2)
		err := h.CheckStrict(ctx, constView(1, true))
		if err == nil {
			t.Fatal("持久量 1 < 确认 2 必须违约")
		}
		for _, want := range []string{a.String(), "2", "1"} {
			if !contains(err.Error(), want) {
				t.Fatalf("违约错误必须含 %q: %v", want, err)
			}
		}
	})

	t.Run("absent durable entry fails", func(t *testing.T) {
		h := NewInvHistory()
		h.Record(a, "inc", 1)
		if err := h.CheckStrict(ctx, constView(0, false)); err == nil {
			t.Fatal("持久面无条目但有确认必须违约")
		}
	})

	t.Run("view error propagates", func(t *testing.T) {
		h := NewInvHistory()
		h.Record(a, "inc", 1)
		if err := h.CheckStrict(ctx, errView()); err == nil || !contains(err.Error(), "boom") {
			t.Fatalf("读取错误必须透传, got %v", err)
		}
	})

	t.Run("empty history passes", func(t *testing.T) {
		h := NewInvHistory()
		if err := h.CheckStrict(ctx, constView(0, false)); err != nil {
			t.Fatalf("空历史无义务, got %v", err)
		}
	})
}

// TestInvHistoryCheckCrashBound 崩溃窗例外语义：基线（最近一次成功
// checkpoint 的效果量）及之前的确认必须存活，基线之后的确认列为允许损失；
// 基线被击穿 → 违约；无条目 + 基线 0 → 全部确认可失（新鲜身份语义）。
func TestInvHistoryCheckCrashBound(t *testing.T) {
	ctx := context.Background()
	a := idOf("k", "crash-a")

	t.Run("loss above baseline reported", func(t *testing.T) {
		h := NewInvHistory()
		h.Record(a, "inc", 1)
		h.Record(a, "inc", 2) // checkpoint 基线 1：op2 在崩溃中丢失
		h.Record(a, "inc", 3)
		lost, err := h.CheckCrashBound(ctx, constView(1, true), map[rans.ID]int64{a: 1})
		if err != nil {
			t.Fatalf("基线存活时必须通过: %v", err)
		}
		if len(lost) != 2 || lost[0].Seq != 2 || lost[1].Seq != 3 {
			t.Fatalf("允许损失必须恰为 {2,3}, got %v", lost)
		}
	})

	t.Run("baseline violation fails", func(t *testing.T) {
		h := NewInvHistory()
		h.Record(a, "inc", 1)
		h.Record(a, "inc", 2)
		// 持久量 0 < 基线 1：checkpoint 承诺的效果本身丢了——这是违约
		// （INV1 崩溃窗只允许基线之后的增量损失）。
		_, err := h.CheckCrashBound(ctx, constView(0, true), map[rans.ID]int64{a: 1})
		if err == nil {
			t.Fatal("基线被击穿必须违约")
		}
	})

	t.Run("absent entry with zero baseline loses all", func(t *testing.T) {
		h := NewInvHistory()
		h.Record(a, "inc", 1)
		h.Record(a, "inc", 2)
		lost, err := h.CheckCrashBound(ctx, constView(0, false), nil) // 无 checkpoint 承诺
		if err != nil {
			t.Fatalf("基线 0 时全部损失在界内: %v", err)
		}
		if len(lost) != 2 {
			t.Fatalf("全部确认应列为允许损失, got %v", lost)
		}
	})

	t.Run("baseline zero survives nothing expected", func(t *testing.T) {
		h := NewInvHistory()
		h.Record(a, "inc", 4)
		// 持久量 2 ≥ 基线 0：损失 {4}，但 2 之前无承诺。
		lost, err := h.CheckCrashBound(ctx, constView(2, true), map[rans.ID]int64{a: 0})
		if err != nil || len(lost) != 1 || lost[0].Seq != 4 {
			t.Fatalf("got lost=%v err=%v, want {{4}}/nil", lost, err)
		}
	})
}

// TestCountView Store 计数读取面：8 字节大端契约（accCarrier 同款）、
// 未命中 (0,false,nil)、形状不符报错、Store 错误透传。
func TestCountView(t *testing.T) {
	ctx := context.Background()
	store := checkpoint.New()
	id := idOf("carrier", "view")
	view := CountView(store)

	if _, found, err := view(ctx, id); found || err != nil {
		t.Fatalf("未命中必须 (0,false,nil), got (%v,%v,%v)", 0, found, err)
	}
	if err := store.Put(ctx, id, 1, []byte{0, 0, 0, 0, 0, 0, 0, 5}); err != nil {
		t.Fatal(err)
	}
	v, found, err := view(ctx, id)
	if err != nil || !found || v != 5 {
		t.Fatalf("got (%d,%v,%v), want (5,true,nil)", v, found, err)
	}
	if err := store.Put(ctx, id, 1, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := view(ctx, id); err == nil {
		t.Fatal("形状不符必须报错")
	}
}

// contains 子串判定（自测辅助）。
func contains(s, sub string) bool {
	return len(sub) == 0 || len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// ---- 场景矩阵共享编排 ----

// invRecordCall 发起一次确认调用并记录进历史（记录点 = Call 返回成功处；
// seq 取实体返回的效果量）。want < 0 表示不校验返回值（只要求成功）。
func invRecordCall(t *testing.T, h *InvHistory, n *Node, id rans.ID, want int64) {
	t.Helper()
	v, err := n.Call(context.Background(), id, int64(0))
	if err != nil {
		t.Fatalf("确认调用失败 (id=%s): %v", id.String(), err)
	}
	seq, ok := v.(int64)
	if !ok {
		t.Fatalf("确认调用返回 %T, want int64", v)
	}
	if want >= 0 && seq != want {
		t.Fatalf("确认调用返回 %d, want %d", seq, want)
	}
	h.Record(id, "increment", seq)
}

// invAwaitCheckpoint 等内置周期 checkpoint 扫描器把在役实体的效果量落储
// （确定性测量面收口：搭配 WithCheckpointInterval 调短扫描周期——want <0
// 时不校验落储值）。
func invAwaitCheckpoint(t *testing.T, store checkpoint.Store, id rans.ID, want int64) {
	t.Helper()
	ctx := context.Background()
	pollUntil(t, 5*time.Second, "checkpoint 落储", func() bool {
		b, found, err := store.Get(ctx, id)
		return err == nil && found && (want < 0 || accSnapVal(b) == want)
	})
}

// invSeedCheckpoint 直写基线快照（崩溃窗检查的「最近一次成功 checkpoint」
// 基线——扫描器缺省 1min 不在秒级测试窗内触发，直写基线零漂移；原
// CheckpointSweep 手工驱动的落储语义等价：基线值即快照值）。原形态的
// 「sweep 未捕获任何实体即 Fatal」活性断言由 invAwaitCheckpoint 场景
// （InvMatrixMigration）承载。
func invSeedCheckpoint(t *testing.T, store checkpoint.Store, id rans.ID, v int64) {
	t.Helper()
	if err := store.Put(context.Background(), id, 0, accSnap(v)); err != nil {
		t.Fatalf("seed checkpoint baseline: %v", err)
	}
}

// ---- 场景①：钝化（idle collect）后重激活 → 零丢失（严格） ----

// TestAcceptance_InvMatrixPassivate 单节点：确认 1..2 → **负控：钝化前
// 效果量只在役未落储，严格检查必须报违约（判别力实证——活集群状态，与
// 工具自测的常值读取面互补）** → 闲置钝化（终快照落储）→ 严格断言零丢失
// → 重激活从 Store 恢复，计数连续 3。
func TestAcceptance_InvMatrixPassivate(t *testing.T) {
	store := checkpoint.New()
	c := NewCluster(t, 1, accCarrierKinds(accCarrierOpt{}), WithCheckpointStore(store),
		WithIdlePassivation("carrier", 30*time.Millisecond))
	n1 := c.Node(0)
	ctx := context.Background()
	id := mustID(t, "carrier", "inv-passivate")
	view := CountView(store)

	h := NewInvHistory()
	invRecordCall(t, h, n1, id, 1)
	invRecordCall(t, h, n1, id, 2)

	// 负控：钝化前 Store 无条目——严格检查必须抓到「确认而未持久」。
	//（周期扫描器缺省 1min 不在本测试窗内触发——负控不受漂移干扰。）
	if err := h.CheckStrict(ctx, view); err == nil {
		t.Fatal("钝化前（无快照落储）严格检查必须报违约——检查器失去判别力")
	}

	// 闲置钝化（node 内置收集循环，20ms/轮）。
	pollUntil(t, 5*time.Second, "闲置钝化（collect 轮）", func() bool {
		return !n1.Holds(id)
	})
	if n1.Holds(id) {
		t.Fatal("钝化后实体必须已本地逐出")
	}

	// 严格断言：确认 ⟹ 持久可见（终快照 2 承载全部确认）。
	AssertDurable(t, ctx, h, view)

	// 重激活：恢复 2 → 计数 3（连续性端到端）。
	invRecordCall(t, h, n1, id, 3)
}

// ---- 场景②：Drain 后重激活 → 零丢失（严格） ----

// TestAcceptance_InvMatrixDrain 双节点：实体在 node2 确认 1..3 → Drain
// （Retire 终快照落储）→ 严格断言零丢失 → node1 重激活恢复 3 续走 4。
func TestAcceptance_InvMatrixDrain(t *testing.T) {
	store := checkpoint.New()
	c := NewCluster(t, 2, accCarrierKinds(accCarrierOpt{}), WithCheckpointStore(store))
	host, survivor := c.Node(1), c.Node(0)
	ctx := context.Background()
	id := mustID(t, "carrier", "inv-drain")
	view := CountView(store)

	h := NewInvHistory()
	invRecordCall(t, h, host, id, 1)
	invRecordCall(t, h, host, id, 2)
	invRecordCall(t, h, host, id, 3)

	if err := host.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown (drain): %v", err)
	}
	pollUntil(t, 5*time.Second, "drain 后本地逐出", func() bool {
		return !host.Holds(id)
	})

	AssertDurable(t, ctx, h, view)

	// 重激活在幸存节点：恢复 3 → 计数 4（确认历史与新实例一致，端到端）。
	invRecordCall(t, h, survivor, id, 4)
}

// ---- 场景③：迁移后 → 零丢失（严格） ----

// TestAcceptance_InvMatrixMigration 双节点（进程内换锁目录）：node1 确认
// 1..2 → 迁移 node2 → 新家确认 3..4 → 周期 checkpoint 收口落储 → 严格
// 断言零丢失（迁移快照 + 新家增量的两段持久链都被覆盖）。
func TestAcceptance_InvMatrixMigration(t *testing.T) {
	store := checkpoint.New()
	migPlacement := &migrateArmedPlacement{target: 2}
	c := NewCluster(t, 2, accCarrierKinds(accCarrierOpt{}),
		WithDirectory(newTkTransferDir()), WithCheckpointStore(store),
		WithCheckpointInterval(30*time.Millisecond), // 新家增量经内置扫描器落储
		WithNodePlacement(0, migPlacement))          // 源 node1 迁移选点武装后固定 node2
	src := c.Node(0)
	ctx := context.Background()
	id := mustID(t, "carrier", "inv-migration")
	view := CountView(store)

	h := NewInvHistory()
	invRecordCall(t, h, src, id, 1)
	invRecordCall(t, h, src, id, 2)

	migPlacement.arm() // 迁移选点武装：固定 node2（激活期放弃选点回落本地的前提不变）
	if err := src.Migrate(ctx, id); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	// 目标端激活是懒式的（随迁包预置状态、首请求装载）——归属落定以目录
	// 翻向为准；实体在住由下方确认调用的连续性证明（恢复 2 续走 3）。
	pollUntil(t, 5*time.Second, "目录翻向目标节点", func() bool {
		loc, found, err := c.Directory().Lookup(ctx, id)
		return err == nil && found && loc.Node == 2
	})
	if src.Holds(id) {
		t.Fatal("迁移赢后源端必须逐出实体")
	}

	invRecordCall(t, h, src, id, 3) // 经目录路由到新家（懒激活恢复 2 → 3）
	invRecordCall(t, h, src, id, 4)

	// 测量面收口：新家在役效果量经周期 checkpoint 落储（非破坏）。
	invAwaitCheckpoint(t, store, id, 4)

	AssertDurable(t, ctx, h, view)
}

// ---- 场景④：强弃（看门狗）→ 允许损失 ≤ checkpoint 周期（显式列报） ----

// TestAcceptance_InvMatrixWatchdog 单节点：确认 1 → checkpoint 落储（基线
// 1）→ 确认 2 → 第三次调用卡死（未确认）→ 看门狗强弃：确认 2 的效果随
// zombie 丢弃——崩溃窗例外。断言三层：严格检查必须**报违约**（损失真实
// 存在且检查器能抓到，负控）；崩溃窗检查通过且允许损失恰为 {2}（显式
// 列报，全部 > 基线）；重激活恢复基线 1 续走 2（卡死的未确认增量丢弃）。
func TestAcceptance_InvMatrixWatchdog(t *testing.T) {
	store := checkpoint.New()
	stick := make(chan struct{})
	t.Cleanup(func() { close(stick) }) // zombie handler 放行（防泄漏）
	opt := accCarrierOpt{
		arrivals:  make(chan int64, 16),
		stickFrom: 3,
		stick:     stick,
		stuckDone: &atomic.Bool{},
	}
	c := NewCluster(t, 1, accCarrierKinds(opt), WithCheckpointStore(store),
		WithWatchdog(10*time.Millisecond, 150*time.Millisecond))
	n1 := c.Node(0)
	ctx := context.Background()
	id := mustID(t, "carrier", "inv-watchdog")
	view := CountView(store)

	h := NewInvHistory()
	invRecordCall(t, h, n1, id, 1)
	invSeedCheckpoint(t, store, id, 1) // 基线 = 1（直写——扫描器不在本窗触发，零漂移）

	invRecordCall(t, h, n1, id, 2) // 已确认——但在下次 checkpoint 前崩溃即失

	// 卡死调用（效果 3，永不被确认）。
	stuckErr := make(chan error, 1)
	go func() { _, err := n1.Call(ctx, id, int64(0)); stuckErr <- err }()
	waitArrivalValue(t, opt.arrivals, 3)

	// 看门狗强弃（构造期注入短执行超时——node 内置看门狗周期扫描）。
	pollUntil(t, 5*time.Second, "看门狗强弃（实体逐出）", func() bool {
		return !n1.Holds(id)
	})
	select {
	case err := <-stuckErr:
		if err == nil {
			t.Fatal("卡死调用必须以错误解绑（不悬挂）")
		}
		// 回执形态细分（StatusTimeout）走 rans 错误模型公开面；本场景锚定
		// 「强弃后调用方有界解绑」。
	case <-time.After(5 * time.Second):
		t.Fatal("卡死调用未被解绑")
	}

	// 负控：严格检查必须报违约（确认 2 的效果确已丢失）。
	if err := h.CheckStrict(ctx, view); err == nil {
		t.Fatal("崩溃后严格检查必须报违约（确认 2 的效果丢失）——检查器失去判别力")
	}

	// 崩溃窗检查：基线 1 存活，允许损失 = {2}。
	lost := AssertCrashLoss(t, ctx, h, view, map[rans.ID]int64{id: 1})
	if len(lost) != 1 || lost[0].Seq != 2 {
		t.Fatalf("允许损失必须恰为 {2}, got %v", lost)
	}

	// 重激活：恢复基线 1 → 计数 2（zombie 增量 3 丢弃是规格语义）。
	invRecordCall(t, h, n1, id, 2)
}

// ---- 场景⑤：Store 读写故障注入组合 ----

// TestAcceptance_InvMatrixStoreFaultPassivateRetry 腿一（Put 持续失败下
// Passivate 回退）：确认 1..2 → Put 持续失败 → 闲置收集的钝化被中止
// （窗回退 Active，实体继续服役）→ 修复 Put → 下轮收集钝化成功 → 严格
// 断言零丢失。回退路径的零义务（失败轮无静默承诺）由「实体仍在服役 +
// Store 无条目」锚定。
func TestAcceptance_InvMatrixStoreFaultPassivateRetry(t *testing.T) {
	fstore := NewFaultStore(checkpoint.New())
	c := NewCluster(t, 1, accCarrierKinds(accCarrierOpt{}), WithCheckpointStore(fstore),
		WithIdlePassivation("carrier", 30*time.Millisecond))
	n1 := c.Node(0)
	ctx := context.Background()
	id := mustID(t, "carrier", "inv-storefault-passivate")
	view := CountView(fstore)

	h := NewInvHistory()
	invRecordCall(t, h, n1, id, 1)
	invRecordCall(t, h, n1, id, 2)

	fstore.FailPuts(-1) // 持续失败
	// 候选在闲置超过阈值后才出现——内置收集循环（20ms/轮）尝试钝化即触发
	// 注入失败（Put 尝试计数增长即「失败轮已发生」的公开观测面；阈值前的
	// 空转轮不产生 Put）。
	pollUntil(t, 5*time.Second, "Put 失败轮（钝化被拒）", func() bool {
		attempts, _ := fstore.PutStats()
		return attempts > 0
	})
	// 回退语义：实体仍在本地服役（零义务——无快照落储即不放弃在役）。
	if !n1.Holds(id) {
		t.Fatal("钝化被拒后实体必须回退 Active 继续服役")
	}
	v, err := n1.Call(ctx, id, int64(0))
	if err != nil || v != int64(3) {
		t.Fatalf("回退后必须继续服役: v=%v err=%v", v, err)
	}
	h.Record(id, "increment", 3)

	fstore.HealPuts()
	pollUntil(t, 5*time.Second, "修复后钝化成功", func() bool {
		return !n1.Holds(id)
	})

	AssertDurable(t, ctx, h, view) // 终快照 3 承载全部确认（零丢失）
}

// TestAcceptance_InvMatrixStoreFaultRetireTerminal 腿二（Retire 告警终局）：
// 确认 1..2 → Put 持续失败 → Drain（Retire 交接窗预算内重试耗尽）→ 记
// Error + rans.checkpoint.final_failed 指标、照样终局——INV1 的条件例外
// （「只要 Store 在转换预算内可写」前提不成立，损失显式告警而非静默）。
// 断言：实体终局逐出、Store 全程无条目（无半写）、确认历史全部损失且
// 崩溃窗检查以基线 0 列报（承认损失面，不静默）。
func TestAcceptance_InvMatrixStoreFaultRetireTerminal(t *testing.T) {
	fstore := NewFaultStore(checkpoint.New())
	c := NewCluster(t, 1, accCarrierKinds(accCarrierOpt{}), WithCheckpointStore(fstore))
	n1 := c.Node(0)
	ctx := context.Background()
	id := mustID(t, "carrier", "inv-storefault-retire")

	h := NewInvHistory()
	invRecordCall(t, h, n1, id, 1)
	invRecordCall(t, h, n1, id, 2)

	fstore.FailPuts(-1)
	if err := n1.Shutdown(ctx); err != nil {
		// Retire 照样终局（错误只在预算/等待段语义），Drain 单实体无他错
		// ——返回 nil 或纯终局错误均收口，终态以本地逐出为准。
		t.Logf("Shutdown 返回（Retire 告警终局路径）: %v", err)
	}
	pollUntil(t, 5*time.Second, "Retire 告警终局（实体逐出）", func() bool {
		return !n1.Holds(id)
	})

	// rans.checkpoint.final_failed 指标读数不在 testkit 依赖面（wiring 在
	// internal/activation/retire.go；行为面由下方「无半写 + 损失列报」断言
	// 覆盖）。
	if _, found, err := fstore.Get(ctx, id); found || err != nil {
		t.Fatalf("终快照失败路径不得有半写: found=%v err=%v", found, err)
	}

	// 损失显式列报：基线 0（从未有 checkpoint），全部确认列为允许损失
	// ——「告警终局」的例外形态在检查器语义下不静默。
	lost := AssertCrashLoss(t, ctx, h, CountView(fstore), nil)
	if len(lost) != 2 {
		t.Fatalf("全部确认应显式列为损失, got %v", lost)
	}
}

// ---- 场景⑥：续租丢失（Redis 丢键）→ Fence+销毁链路 → 零丢失（严格） ----

func TestAcceptance_InvMatrixKill(t *testing.T) {
	mustRedis(t)
	const (
		leaseTTL   = 1500 * time.Millisecond
		hbInterval = 300 * time.Millisecond
	)
	store := checkpoint.New()
	c := NewCluster(t, 3, accCarrierKinds(accCarrierOpt{}),
		WithRedis(redisAddr(), leaseTTL, hbInterval), WithCheckpointStore(store))
	host, other := c.Node(1), c.Node(2)
	ctx := context.Background()
	dir := c.Directory()
	id := mustID(t, "carrier", "inv-kill")
	view := CountView(store)

	h := NewInvHistory()
	invRecordCall(t, h, host, id, 1)
	invRecordCall(t, h, host, id, 2)
	invRecordCall(t, h, host, id, 3)
	invSeedCheckpoint(t, store, id, 3) // 基线 = 3（直写——扫描器不在本窗触发，零漂移）

	invRecordCall(t, h, host, id, 4)
	invRecordCall(t, h, host, id, 5)

	c.Kill(1) // node2 急停：续租停摆 + 实例成陈旧孤儿（无终快照；v3 条目常驻）
	reg, hasReg := dir.(directory.NodeRegistry)
	pollUntil(t, 10*time.Second, "成员淘汰（死节点退出成员视图）", func() bool {
		if !hasReg {
			return true
		}
		ns, err := reg.NodeIDs(ctx)
		if err != nil {
			return false
		}
		for _, n := range ns {
			if n == 2 {
				return false
			}
		}
		return true
	})

	// 负控：严格检查必须报违约。
	if err := h.CheckStrict(ctx, view); err == nil {
		t.Fatal("Kill 后严格检查必须报违约（确认 4..5 的效果丢失）")
	}

	// 崩溃窗检查：基线 3 存活，允许损失 = {4,5}。
	lost := AssertCrashLoss(t, ctx, h, view, map[rans.ID]int64{id: 3})
	if len(lost) != 2 || lost[0].Seq != 4 || lost[1].Seq != 5 {
		t.Fatalf("允许损失必须恰为 {4,5}, got %v", lost)
	}

	// 他节点重激活：恢复 3 → 计数 4（恰从基线续走）。
	invRecordCall(t, h, other, id, 4)
}
