package lifecycle

// 调用面接线契约测试（《加固 spec》§5.1 第 7 点）：把公共销毁/
// 钝化调用面接到 Retire/Passivate 交接窗语义上——
//   - Manager.Passivate 公共面：有状态实体走终快照（重激活恢复）、无状态
//     实体直达终局、非本地 no-op（与 DestroyLocal 同口径）；
//   - DestroyLocal/Shutdown/DestroyCtxInstance 走 Retire：终快照生效
//     （包内面 retireInstance 已锚定同行为，本组测试锁
//     公共调用面）、组合矩阵等待（DestroyLocal 撞上进行中的 Passivate 窗时
//     等在飞结果而非关 stop 抢占）；
//   - 看门狗强弃在 Retire 窗内依旧不写快照（manager 层豁免锚）。

import (
	"context"
	"errors"
	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/activation"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// TestPassivatePublic_StatefulFinalSnapshotRestores（有状态直调）：
// Passivate 走终快照——确认过的写落入 Store，重激活从 Store 恢复（计数
// 连续）；本地逐出 + 目录注销 + Done 关闭（钝化即终局）。
func TestPassivatePublic_StatefulFinalSnapshotRestores(t *testing.T) {
	base := checkpoint.New()
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, activation.Options{Store: base})

	X := mustTestID(t, "acct", "pass-api-stateful")
	ctx := context.Background()

	p, err := m.GetOrActivate(ctx, X)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-passivate pawnite")
	if v, err := m.InvokeLocal(ctx, X, "add:7"); err != nil || v != 7 {
		t.Fatalf("add:7 = (%v, %v), want (7, nil)", v, err)
	}

	if err := Passivate(ctx, m, X); err != nil {
		t.Fatalf("Passivate = %v, want nil", err)
	}
	if _, ok := m.GetLocal(X); ok {
		t.Fatal("passivated entity must be evicted from the local registry")
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("passivated entity must reach terminal state")
	}
	if b, found, _ := base.Get(ctx, X); !found || string(b) != "7" {
		t.Fatalf("store snapshot = (%q, %v), want (\"7\", true) — final snapshot", string(b), found)
	}
	// 重激活从 Store 恢复：计数连续（add:1 → 8）。
	if v, err := m.InvokeLocal(ctx, X, "add:1"); err != nil || v != 8 {
		t.Fatalf("reactivated add:1 = (%v, %v), want (8, nil) — restore from final snapshot", v, err)
	}
}

// TestPassivatePublic_StatelessDirectTerminal（无状态直调）：无
// carrier 面的实体直达终局（不需要 Store——core 矩阵「无状态」列）。
func TestPassivatePublic_StatelessDirectTerminal(t *testing.T) {
	m := newTestTable(t) // counterEntity：无快照面，Store 未注入
	X := mustTestID(t, "counter", "pass-api-stateless")
	ctx := context.Background()

	p, err := m.GetOrActivate(ctx, X)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-passivate pawnite")
	if _, err := m.InvokeLocal(ctx, X, nil); err != nil {
		t.Fatal(err)
	}

	if err := Passivate(ctx, m, X); err != nil {
		t.Fatalf("Passivate(stateless) = %v, want nil (direct terminal)", err)
	}
	if _, ok := m.GetLocal(X); ok {
		t.Fatal("passivated entity must be evicted from the local registry")
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("passivated entity must reach terminal state")
	}
}

// TestPassivatePublic_NotLocalNoop（非本地）：不在册身份 no-op 返回
// nil——与 DestroyLocal 的「不在册 no-op」一致；闲置
// 收集循环扫描与钝化之间候选消失（他路销毁/迁移收口）是常态竞态，不得
// 以错误刷屏。
func TestPassivatePublic_NotLocalNoop(t *testing.T) {
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, activation.Options{Store: checkpoint.New()})

	ghost := mustTestID(t, "acct", "pass-api-ghost")
	if err := Passivate(context.Background(), m, ghost); err != nil {
		t.Fatalf("Passivate(not local) = %v, want nil (no-op, DestroyLocal parity)", err)
	}
}

// TestShutdownWritesFinalSnapshotPublicFace 停机公共面终快照：
// checkpoint 持久基线 100 之上确认 add:50（150）→ Manager.Shutdown →
// 重激活余额 150（包内面 retireInstance 已锚定同一行为，本
// 测试把断言搬到公共停机面）：Shutdown 逐实体走 Retire 交接窗，终快照生效
// （销毁若不写快照，重激活回落基线 100——本测试即红）。同时锚定终快照先于
// 用户 Deactivate（Deactivate 写入的 999 不进快照）。
func TestShutdownWritesFinalSnapshotPublicFace(t *testing.T) {
	base := checkpoint.New()
	dir := inmem.New()
	X := mustTestID(t, "acct", "ex10-shutdown")
	seedDurableState(t, dir, base, X, 2, "100") // 上一轮 checkpoint 的持久基线

	m := activation.New(dir, 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity {
		return &deactivateMutatingEntity{}
	}}, activation.Options{Store: base})

	ctx := context.Background()

	if v, err := m.InvokeLocal(ctx, X, "add:50"); err != nil || v != 150 {
		t.Fatalf("acked add:50 = (%v, %v), want (150, nil)", v, err)
	}

	if err := Shutdown(ctx, m); err != nil {
		t.Fatalf("Shutdown = %v, want nil", err)
	}

	// 终快照内容 = 确认过的 150（不是 Deactivate 写入的 999）。
	if b, _, _ := base.Get(ctx, X); string(b) != "150" {
		t.Fatalf("store final value = %q, want \"150\" (final snapshot via Shutdown)", string(b))
	}
	// 重激活：从 Store 恢复终快照 → 余额 150。
	if v, err := m.InvokeLocal(ctx, X, "get"); err != nil || v != 150 {
		t.Fatalf("reactivated balance = (%v, %v), want (150, nil)", v, err)
	}
}

// TestDestroyLocalWritesFinalSnapshot（DestroyLocal→Retire）：单实体
// 公共销毁面写终快照——确认过的 add:7 落 Store，重激活恢复（销毁若不写
// 快照，重激活裸激活归零——本测试即红）。
func TestDestroyLocalWritesFinalSnapshot(t *testing.T) {
	base := checkpoint.New()
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, activation.Options{Store: base})

	X := mustTestID(t, "acct", "ex10-destroyctx")
	ctx := context.Background()

	p, err := m.GetOrActivate(ctx, X)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-destroy pawnite")
	if v, err := m.InvokeLocal(ctx, X, "add:7"); err != nil || v != 7 {
		t.Fatalf("add:7 = (%v, %v), want (7, nil)", v, err)
	}

	if err := DestroyLocal(ctx, m, X); err != nil {
		t.Fatalf("DestroyLocal = %v, want nil", err)
	}
	if b, found, _ := base.Get(ctx, X); !found || string(b) != "7" {
		t.Fatalf("store snapshot = (%q, %v), want (\"7\", true) — final snapshot via DestroyLocal", string(b), found)
	}
	if v, err := m.InvokeLocal(ctx, X, "get"); err != nil || v != 7 {
		t.Fatalf("reactivated balance = (%v, %v), want (7, nil)", v, err)
	}
}

// TestDestroyLocalFollowsInFlightPassivateWindow（组合矩阵）：DestroyLocal
// 撞上进行中的 Passivate 窗时等待在飞结果而非关
// stop 抢占——钝化成功 ⟹ DestroyLocal 视为已完成（nil），窗内准入照收
// （留箱等待），终快照是钝化那一写（Retire core 不重复执行）。
func TestDestroyLocalFollowsInFlightPassivateWindow(t *testing.T) {
	base := checkpoint.New()
	st := &gatedStore{Store: base, gate: make(chan struct{}), entered: make(chan struct{})}
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, activation.Options{Store: st})

	X := mustTestID(t, "acct", "ex10-window")
	ctx := context.Background()

	p, err := m.GetOrActivate(ctx, X)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-passivate pawnite")
	if v, err := m.InvokeLocal(ctx, X, "add:3"); err != nil || v != 3 {
		t.Fatalf("add:3 = (%v, %v), want (3, nil)", v, err)
	}

	// 开钝化窗：core 卡在 Put 门闸。
	passRes := make(chan error, 1)
	go func() { passRes <- Passivate(ctx, m, X) }()
	select {
	case <-st.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("passivate core never reached store put")
	}

	// DestroyLocal 撞窗：必须等待（旧行为此刻关 stop 抢占 → 钝化被判输）。
	destroyRes := make(chan error, 1)
	go func() { destroyRes <- DestroyLocal(ctx, m, X) }()

	close(st.gate) // 放行 Put → 钝化成功终局

	select {
	case err := <-passRes:
		if err != nil {
			t.Fatalf("Passivate = %v, want nil (must not be preempted by DestroyLocal)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Passivate never settled")
	}
	select {
	case err := <-destroyRes:
		if err != nil {
			t.Fatalf("DestroyLocal = %v, want nil (in-flight passivate success = retire complete)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DestroyLocal never settled")
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("entity must reach terminal state")
	}
	if _, ok := m.GetLocal(X); ok {
		t.Fatal("entity must be evicted after the followed passivate")
	}
	// 终快照是钝化的那一写（3）；Retire core 未重复落库（Store 仅一写）。
	if b, found, _ := base.Get(ctx, X); !found || string(b) != "3" {
		t.Fatalf("store final value = (%q, %v), want (\"3\", true)", string(b), found)
	}
	if got := st.putsSnapshot(); got != 1 {
		t.Fatalf("store puts = %d, want 1 (passivate only; retire core must not run)", got)
	}
}

// TestDestroyLocalAfterFailedPassivateTakesOver（组合矩阵另一半）：在飞
// 钝化失败（窗回退 Active）→ DestroyLocal 接棒自开 Retire 窗执行——终局照常
// 收口；Retire 的失败语义是「预算内重试后照样终局」（Store 恒败不挂死销
// 毁），重激活裸激活（无快照可恢复——如实代价）。
func TestDestroyLocalAfterFailedPassivateTakesOver(t *testing.T) {
	st := &putFailStore{}
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, activation.Options{Store: st})

	X := mustTestID(t, "acct", "ex10-takeover")
	ctx := context.Background()

	p, err := m.GetOrActivate(ctx, X)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-passivate pawnite")
	if v, err := m.InvokeLocal(ctx, X, "add:5"); err != nil || v != 5 {
		t.Fatalf("add:5 = (%v, %v), want (5, nil)", v, err)
	}

	passRes := make(chan error, 1)
	go func() { passRes <- Passivate(ctx, m, X) }()
	destroyRes := make(chan error, 1)
	go func() { destroyRes <- DestroyLocal(ctx, m, X) }()

	select {
	case err := <-passRes:
		if err == nil {
			t.Fatal("Passivate with failing store must be aborted with an error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Passivate never settled")
	}
	select {
	case err := <-destroyRes:
		if err != nil {
			t.Fatalf("DestroyLocal = %v, want nil (retire proceeds after rollback)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DestroyLocal never settled")
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("entity must reach terminal state")
	}
	if _, ok := m.GetLocal(X); ok {
		t.Fatal("entity must be evicted after takeover retire")
	}
	// 重激活裸激活：终快照未落地（Store 恒败）——「停机不能被 Store 故障
	// 挂死」的如实代价。
	if v, err := m.InvokeLocal(ctx, X, "get"); err != nil || v != 0 {
		t.Fatalf("reactivated balance = (%v, %v), want (0, nil) — bare activation", v, err)
	}
}

// TestDestroyCtxInstanceRetireForm（失租销毁 Retire 形态）：纪元匹配的
// DestroyCtxInstance（membership OnLost 消费面）走 Retire 交接窗——终快照
// 生效；纪元不匹配与不在册保持 no-op nil（实例级止损口径不变）。
func TestDestroyCtxInstanceRetireForm(t *testing.T) {
	base := checkpoint.New()
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, activation.Options{Store: base})

	X := mustTestID(t, "acct", "ex10-lease-lost")
	ctx := context.Background()

	p, err := m.GetOrActivate(ctx, X)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-destroy pawnite")
	inc := p.Incarnation()
	if v, err := m.InvokeLocal(ctx, X, "add:9"); err != nil || v != 9 {
		t.Fatalf("add:9 = (%v, %v), want (9, nil)", v, err)
	}

	// 纪元不匹配 / 不在册：no-op nil（既有口径不变）。
	if err := DestroyInstance(ctx, m, X, inc+1); err != nil {
		t.Fatalf("mismatched epoch = %v, want nil (no-op)", err)
	}
	if st := p.State(); st != pawnite.StateActive {
		t.Fatalf("state after mismatched epoch = %v, want Active", st)
	}

	if err := DestroyInstance(ctx, m, X, inc); err != nil {
		t.Fatalf("matched epoch = %v, want nil", err)
	}
	if b, found, _ := base.Get(ctx, X); !found || string(b) != "9" {
		t.Fatalf("store snapshot = (%q, %v), want (\"9\", true) — retire final snapshot", string(b), found)
	}
	if v, err := m.InvokeLocal(ctx, X, "get"); err != nil || v != 9 {
		t.Fatalf("reactivated balance = (%v, %v), want (9, nil)", v, err)
	}
}

// TestDestroyLocalRetireWindowAbandonNoSnapshot（表层豁免锚的
// 窗口形态补锚）：Retire 窗内的终快照 Put 被看门狗强弃打断——强弃不走
// 交接窗收口、零快照落地（gate 永不放行，基线 Store 保持为空），实体
// 直达终局并被逐出。
func TestDestroyLocalRetireWindowAbandonNoSnapshot(t *testing.T) {
	base := checkpoint.New()
	st := &gatedStore{Store: base, gate: make(chan struct{}), entered: make(chan struct{})}
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, activation.Options{Store: st})

	X := mustTestID(t, "acct", "ex10-abandon-window")
	ctx := context.Background()

	p, err := m.GetOrActivate(ctx, X)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-destroy pawnite")
	if v, err := m.InvokeLocal(ctx, X, "add:5"); err != nil || v != 5 {
		t.Fatalf("add:5 = (%v, %v), want (5, nil)", v, err)
	}

	destroyRes := make(chan error, 1)
	go func() { destroyRes <- DestroyLocal(ctx, m, X) }()
	select {
	case <-st.entered: // retire core 已卡在 Put 门闸（窗内）
	case <-time.After(2 * time.Second):
		t.Fatal("retire core never reached store put")
	}

	if !AbandonStuckDeactivation(m, X) {
		t.Fatal("watchdog abandon must win on the retire window")
	}
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("abandoned entity must reach terminal state")
	}
	select {
	case err := <-destroyRes:
		// core 已被 loop 消费（卡在 Put）→ 强弃走 zombie core 收束路径，
		// 裁决以成功形态回传（终局早已由强弃单点落位）——返回 nil 属实；
		// 若交错使 abort 抢先回输则是 ErrMigrationLost。两者都是「单位已
		// 终局」的诚实表达。
		if err != nil && !errors.Is(err, pawnite.ErrMigrationLost) {
			t.Fatalf("DestroyLocal after abandon = %v, want nil or ErrMigrationLost", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DestroyLocal never settled after abandon")
	}
	// 豁免锚：强弃全程零快照落地（gate 未放行，基线 Store 保持为空）。
	if _, found, _ := base.Get(ctx, X); found {
		t.Fatal("abandon must not land a snapshot (gate never opened)")
	}
	if _, ok := m.GetLocal(X); ok {
		t.Fatal("abandoned entity must be evicted")
	}
}
