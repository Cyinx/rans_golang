package activation

// 配对约束测试：用户 Deactivate 只与成功的激活配对——注册失败、恢复失败、
// Activate 失败、仲裁落败的实例在终局（finish）都不得调用用户 Deactivate；
// 正常激活 + 销毁恰一次配对。判据是「激活整体成功」（目录注册与实体
// Activate 都成功）。register-first 下仲裁输家零用户代码（注册先于
// 装载，输家在装载前即被 Register 原子域裁定出局）。

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// countingEntity 计数实体：Activate/Deactivate 各计数一次，Activate 可注入
// 失败。实现迁移快照面（rans.Snapshotter）——restoreState 只对 carrier 实体
// 读 Store，restore-failed 子测试据此走到 Store.Get 的 IO 失败注入点。
type countingEntity struct {
	activateErr error
	activated   atomic.Int32
	deactivated atomic.Int32
}

func (e *countingEntity) Activate(ctx context.Context) error {
	e.activated.Add(1)
	return e.activateErr
}
func (e *countingEntity) Handle(ctx context.Context, req any) (any, error) {
	return nil, nil
}
func (e *countingEntity) Deactivate(ctx context.Context) error {
	e.deactivated.Add(1)
	return nil
}
func (e *countingEntity) Snapshot() ([]byte, error) { return nil, nil }
func (e *countingEntity) Restore(b []byte) error    { return nil }

// unregisterSignalDir 目录包装：实例级条件注销 UnregisterIfOwner(Owner)
// 首次进入即关闭 entered（幂等，纯委托不改行为）——终局收尾的确定性握手
// 信号。时序：finish 置 Destroyed →
// OnEvict → cleanup 的注销步，信号触发时 Destroyed 必已置位、本地逐出尚未
// 发生。owner 未捕获的 cleanup 零目录调用——本
// 信号面只覆盖 owner 已捕获的失败路径（注册成功后 Activate/恢复失败的
// 实例，复检类测试的握手场景）。嵌入透传（inmem 具体实现满足）。
type unregisterSignalDir struct {
	directory.Directory
	entered chan struct{}
	once    sync.Once
}

func newUnregisterSignalDir() *unregisterSignalDir {
	return &unregisterSignalDir{
		Directory: inmem.New(),
		entered:   make(chan struct{}),
	}
}

func (d *unregisterSignalDir) UnregisterIfOwner(ctx context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	d.once.Do(func() { close(d.entered) })
	return d.Directory.UnregisterIfOwner(ctx, id, owner)
}

// waitTerminalEviction 等待失败激活的终局收尾完成：本地表逐出（OnEvict →
// cleanup 已跑完）⟹ finish 的 Deactivate 包装已被经过，计数稳定可断言。
func waitTerminalEviction(t *testing.T, m *Table, id rans.ID) {
	t.Helper()
	waitFor(t, "terminal cleanup (local eviction)", func() bool {
		_, ok := m.GetLocal(id)
		return !ok
	})
}

func TestDeactivate_PairedWithSuccessfulActivateOnly(t *testing.T) {
	t.Run("restore-failed", func(t *testing.T) {
		base := checkpoint.New()
		rec := &recStore{Store: base}
		rec.getFails.Store(1) // 激活读快照时一次 IO 失败
		ent := &countingEntity{}
		m := New(inmem.New(), 1, map[string]rans.Factory{
			"cnt": func(_ rans.ID) rans.Entity { return ent },
		}, Options{Store: rec})

		X := mustTestID(t, "cnt", "pair-restore")

		// 经 GetOrActivate 直调断言（比 Call 面更强：错误窗保留根因链）。
		// 两个时序窗都合法：复检同步窗返回错误；在途形态返回 pawnite 后
		// 由其终局收场——配对断言在终局逐出之后做，与窗无关。
		p, err := m.GetOrActivate(context.Background(), X)
		if err == nil {
			<-p.Done() // 在途形态：等失败终局
		} else if !errors.Is(err, ErrActivationFailed) || !errors.Is(err, ErrRestoreFailed) {
			t.Fatalf("err = %v, want ErrActivationFailed wrapping ErrRestoreFailed", err)
		}
		waitTerminalEviction(t, m, X)
		if got := ent.activated.Load(); got != 0 {
			t.Fatalf("activated = %d, want 0（恢复失败：Activate 从未执行）", got)
		}
		if got := ent.deactivated.Load(); got != 0 {
			t.Fatalf("deactivated = %d, want 0（未成功 Activate 的实例不得调用用户 Deactivate）", got)
		}
	})

	t.Run("activate-error", func(t *testing.T) {
		ent := &countingEntity{activateErr: errors.New("user activate failed")}
		m := New(inmem.New(), 1, map[string]rans.Factory{
			"cnt": func(_ rans.ID) rans.Entity { return ent },
		})
		X := mustTestID(t, "cnt", "pair-acterr")

		// GetOrActivate 双窗处理（快速失败下 Call 的准入窗会以 ErrBusy
		// 形态收场——与本断言无关的第三窗，直调面避免之）。
		p, err := m.GetOrActivate(context.Background(), X)
		if err == nil {
			<-p.Done() // 在途形态：等失败终局
		} else if !errors.Is(err, ErrActivationFailed) {
			t.Fatalf("err = %v, want ErrActivationFailed", err)
		}
		waitTerminalEviction(t, m, X)
		if got := ent.activated.Load(); got != 1 {
			t.Fatalf("activated = %d, want 1（Activate 执行过一次且失败）", got)
		}
		if got := ent.deactivated.Load(); got != 0 {
			t.Fatalf("deactivated = %d, want 0（Activate 失败的实例不得调用用户 Deactivate）", got)
		}
	})

	t.Run("arbitration-lost", func(t *testing.T) {
		dir := inmem.New()
		entWinner := &countingEntity{}
		entLoser := &countingEntity{}
		m1 := New(dir, 1, map[string]rans.Factory{
			"cnt": func(_ rans.ID) rans.Entity { return entWinner },
		})
		m2 := New(dir, 2, map[string]rans.Factory{
			"cnt": func(_ rans.ID) rans.Entity { return entLoser },
		})
		X := mustTestID(t, "cnt", "pair-arb")

		p1, err := m1.GetOrActivate(context.Background(), X)
		if err != nil {
			t.Fatal(err)
		}
		waitActive(t, p1, "winner pawnite") // m1 注册已落定，m2 的 CAS 必输

		p2, err := m2.GetOrActivate(context.Background(), X)
		if err == nil {
			<-p2.Done() // 在途形态：等输家终局（错误形态断言由错误窗覆盖）
		} else {
			var lost *ArbitrationLostError
			if !errors.As(err, &lost) || lost.Winner.Node != 1 {
				t.Fatalf("err = %v, want ArbitrationLostError carrying winner node 1", err)
			}
			if !errors.Is(err, ErrActivationFailed) {
				t.Fatalf("err = %v, want ErrActivationFailed sentinel", err)
			}
		}
		waitTerminalEviction(t, m2, X)
		if got := entLoser.activated.Load(); got != 0 {
			t.Fatalf("loser activated = %d, want 0（register-first：注册先于装载，仲裁输家不运行任何用户代码）", got)
		}
		if got := entLoser.deactivated.Load(); got != 0 {
			t.Fatalf("loser deactivated = %d, want 0（仲裁落败：激活未成功，不得调用用户 Deactivate）", got)
		}
	})

	t.Run("happy-path", func(t *testing.T) {
		ent := &countingEntity{}
		m := New(inmem.New(), 1, map[string]rans.Factory{
			"cnt": func(_ rans.ID) rans.Entity { return ent },
		})
		X := mustTestID(t, "cnt", "pair-happy")

		p, err := m.GetOrActivate(context.Background(), X)
		if err != nil {
			t.Fatal(err)
		}
		waitActive(t, p, "happy pawnite")
		_ = p.Destroy(context.Background())
		if got := ent.activated.Load(); got != 1 {
			t.Fatalf("activated = %d, want 1", got)
		}
		if got := ent.deactivated.Load(); got != 1 {
			t.Fatalf("deactivated = %d, want 1（成功激活恰与一次用户 Deactivate 配对）", got)
		}
	})
}
