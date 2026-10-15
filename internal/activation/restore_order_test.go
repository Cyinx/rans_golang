package activation

// 票 19 红测（B12）：随迁包先判旧后 Restore——包旧时实体零污染（Restore
// 恰一次且只用 Store 内容）；包旧且 Store 未命中 → 裸激活（已删除状态不
// 经包复活）；同纪元 + 内容已删（Delete 留高水位）→ 弃包。

import (
	"context"
	"testing"

	"github.com/cyinx/rans_golang/directory/inmem"
)

// restoreSpyEntity 记录 Restore 调用序列的 acctEntity 变体（载荷逐次入账）。
type restoreSpyEntity struct {
	acctEntity
	restored []string
}

func (a *restoreSpyEntity) Restore(b []byte) error {
	a.restored = append(a.restored, string(b))
	return a.acctEntity.Restore(b)
}

// TestRestore_StalePackageNeverApplied 包旧（hw 5 > srcInc 3）且 Store 命中：
// 实体 Restore 恰一次、载荷为 Store 内容（修复前：先应用包再应用 Store——
// 两次 Restore 且合并态污染，审查 TestX13b 计数 = 2 的复现）。
func TestRestore_StalePackageNeverApplied(t *testing.T) {
	ctx := context.Background()
	m := New(inmem.New(), 1, nil, Options{
		Store:       &hwStore{errStore{snap: []byte("77"), hit: true}, 5, true},
		StateLoader: &stubStateLoader{b: []byte("30"), srcInc: 3, ok: true},
	})
	ent := &restoreSpyEntity{}
	src, _, err := m.restoreState(ctx, mustTestID(t, "acct", "stale-once"), ent)
	if err != nil {
		t.Fatalf("restoreState: %v", err)
	}
	if src != restoreStore {
		t.Fatalf("source = %v, want restoreStore", src)
	}
	if len(ent.restored) != 1 || ent.restored[0] != "77" {
		t.Fatalf("Restore calls = %v, want exactly [\"77\"]（stale package must never be applied）", ent.restored)
	}
	if ent.bal != 77 {
		t.Fatalf("bal = %d, want 77", ent.bal)
	}
}

// TestRestore_StalePackageWithStoreMissIsBareActivation 包旧且 Store 未命中
// （hw 5 > srcInc 3，内容已删）：裸激活——零次 Restore（修复前：包先应用
// 再判旧，restoreNone 返回时实体已带旧包状态，首轮周期 checkpoint 把已
// 删除状态以新纪元写回——审查 TestX13 复活复现）。
func TestRestore_StalePackageWithStoreMissIsBareActivation(t *testing.T) {
	ctx := context.Background()
	m := New(inmem.New(), 1, nil, Options{
		Store:       &hwStore{errStore{}, 5, true}, // hw=5, Get miss
		StateLoader: &stubStateLoader{b: []byte("30"), srcInc: 3, ok: true},
	})
	ent := &restoreSpyEntity{}
	src, _, err := m.restoreState(ctx, mustTestID(t, "acct", "stale-miss"), ent)
	if err != nil {
		t.Fatalf("restoreState: %v", err)
	}
	if src != restoreNone {
		t.Fatalf("source = %v, want restoreNone (bare activation)", src)
	}
	if len(ent.restored) != 0 || ent.bal != 0 {
		t.Fatalf("entity polluted by stale package: restored=%v bal=%d, want zero", ent.restored, ent.bal)
	}
}

// TestRestore_SameEpochDeletedContentDropsPackage 同纪元（hw == srcInc）且
// 内容已删：视同已删除、弃包裸激活（修复前：Get 未命中不弃包——已删除
// 状态经包复活）。
func TestRestore_SameEpochDeletedContentDropsPackage(t *testing.T) {
	ctx := context.Background()
	m := New(inmem.New(), 1, nil, Options{
		Store:       &hwStore{errStore{}, 4, true}, // hw=4 == srcInc, Get miss
		StateLoader: &stubStateLoader{b: []byte("30"), srcInc: 4, ok: true},
	})
	ent := &restoreSpyEntity{}
	src, _, err := m.restoreState(ctx, mustTestID(t, "acct", "same-del"), ent)
	if err != nil {
		t.Fatalf("restoreState: %v", err)
	}
	if src != restoreNone {
		t.Fatalf("source = %v, want restoreNone (deleted, package dropped)", src)
	}
	if len(ent.restored) != 0 {
		t.Fatalf("deleted entity resurrected via package: restored=%v", ent.restored)
	}
}

// TestRestore_SameEpochSameContentKeepsPackage 回归锚：同纪元内容一致 →
// 包优先快路径不变（hw==srcInc 且 Get 命中同内容）。
func TestRestore_SameEpochSameContentKeepsPackage(t *testing.T) {
	ctx := context.Background()
	m := New(inmem.New(), 1, nil, Options{
		Store:       &hwStore{errStore{snap: []byte("30"), hit: true}, 4, true},
		StateLoader: &stubStateLoader{b: []byte("30"), srcInc: 4, ok: true},
	})
	ent := &restoreSpyEntity{}
	src, b, err := m.restoreState(ctx, mustTestID(t, "acct", "same-keep"), ent)
	if err != nil {
		t.Fatalf("restoreState: %v", err)
	}
	if src != restorePackage || string(b) != "30" {
		t.Fatalf("source=%v b=%v, want restorePackage with package bytes", src, b)
	}
	if len(ent.restored) != 1 || ent.restored[0] != "30" {
		t.Fatalf("restored=%v, want one package restore", ent.restored)
	}
}
