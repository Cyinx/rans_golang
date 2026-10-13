package lifecycle

// Store 恢复后标记干净的端到端测试：激活从 Store 读出的状态已是持久态，
// 首轮周期 checkpoint 不得把它原样写回（多余 IO）；随迁包恢复不标记——
// 包里可能是比 Store 新的状态，首轮 checkpoint 照常落库。

import (
	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/activation"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// TestCheckpoint_CleanAfterStoreRestore：从 Store 恢复成功 → 首轮
// CheckpointSweep 判干净（起始捕获 0、Put 次数 0）——状态没变过就不回写。
// 激活经 GetOrActivate（不经 Call：每次投递推进活动纪元，脏门控按保守
// 判据视为可能变更——投递脏化与本断言的「恢复即干净」是两件事）；恢复
// 正确性在 sweep 判定之后经 Call("get") 验证 700。
func TestCheckpoint_CleanAfterStoreRestore(t *testing.T) {
	dir := inmem.New()
	X := mustTestID(t, "acct", "clean")
	base := checkpoint.New()
	seedDurableState(t, dir, base, X, 3, "700")

	rec := &recStore{Store: base}
	m := activation.New(dir, 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, activation.Options{Store: rec})

	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()
	p, err := m.GetOrActivate(ctx, X)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	waitActive(t, p, "acct")
	if n := CheckpointSweep(ctx, m, 10); n != 0 {
		t.Fatalf("首轮 sweep 起始捕获 = %d, want 0（Store 恢复即干净）", n)
	}
	time.Sleep(500 * time.Millisecond) // 给潜在误写留窗口
	if rec.puts() != 0 {
		t.Fatalf("首轮 checkpoint 回写 %d 次, want 0（刚读出的状态不回写）", rec.puts())
	}
	// 恢复正确性：Store 里的 700 已就位（Call 排在 sweep 之后——投递推进
	// 纪元属正常脏化，不在本断言面内，此后不再扫）。
	v, err := m.InvokeLocal(ctx, X, "get")
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if v != 700 {
		t.Fatalf("恢复后余额 = %v, want 700", v)
	}
}

// TestCheckpoint_PutsAfterPackageRestore：从随迁包恢复 → 首轮 checkpoint
// 正常落库（包可能比 Store 新，需要写回）；包命中优先，不读 Store。
func TestCheckpoint_PutsAfterPackageRestore(t *testing.T) {
	dir := inmem.New()
	X := mustTestID(t, "acct", "pkg")
	base := checkpoint.New()
	rec := &recStore{Store: base}
	m := activation.New(dir, 1, map[string]rans.Factory{"acct": func(_ rans.ID) rans.Entity { return &acctEntity{} }}, activation.Options{StateLoader: &stubStateLoader{b: []byte("900"), ok: true}, Store: // 包命中优先，不读 Store
	rec})

	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()
	v, err := m.InvokeLocal(ctx, X, "get")
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if v != 900 {
		t.Fatalf("随迁包恢复后余额 = %v, want 900", v)
	}
	if n := CheckpointSweep(ctx, m, 10); n != 1 {
		t.Fatalf("首轮 sweep 起始捕获 = %d, want 1（包恢复不标记干净）", n)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && rec.puts() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	if rec.puts() != 1 {
		t.Fatalf("首轮 checkpoint 落库 %d 次, want 1", rec.puts())
	}
	if b, ok, _ := base.Get(ctx, X); !ok || string(b) != "900" {
		t.Fatalf("Store 终态 = %q hit=%v, want 900", string(b), ok)
	}
}
