package testkit

// 票 19 红测（testkit 公开面，移植审查 eval-api TestX13）：迁移判输留下
// 过期随迁包 + 宿主 Destroy + Store.Delete 后，目标端重激活不得把已删除
// 状态经包复活（修复前：包先应用再判旧，restoreNone 返回时实体已带旧包
// 状态，首轮周期 checkpoint 以新纪元把已删状态写回 Store）。

import (
	"context"
	"sync/atomic"

	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
)

// spyCounterEntity 计数实体：Restore 调用计数 + 余额可读。
type spyCounterEntity struct {
	restores atomic.Int64
	bal      atomic.Int64
}

func (e *spyCounterEntity) Activate(context.Context) error   { return nil }
func (e *spyCounterEntity) Deactivate(context.Context) error { return nil }

func (e *spyCounterEntity) Handle(_ context.Context, req any) (any, error) {
	if req == "inc" {
		return e.bal.Add(1), nil
	}
	if req == "get" {
		return int(e.bal.Load()), nil
	}
	return nil, nil
}

func (e *spyCounterEntity) Snapshot() ([]byte, error) {
	return []byte{byte(e.bal.Load())}, nil
}

func (e *spyCounterEntity) Restore(b []byte) error {
	e.restores.Add(1)
	e.bal.Store(int64(b[0]))
	return nil
}

func TestMigration_DeletedStateNotResurrectedByStalePackage(t *testing.T) {
	store := checkpoint.New()
	dir := newTkTransferDir()
	var failCAS atomic.Bool
	dir.onCAS = func(rans.ID) {
		if failCAS.Load() {
			panic("CAS-GATE") // tkTransferDir 的 onCAS 无错误通道——panic 由目录捕获？
		}
	}
	_ = dir
	// 用 onCAS 卡住迁移窗：pending+Destroying 落位、快照落 Store、随迁包
	// 已发——再以外部抢注（setOwner）判输迁移，留下过期包在目标端缓存。
	gate := make(chan struct{})
	armed := make(chan struct{}) // 只卡迁移 CAS——激活注册也走同钩子
	entered := make(chan struct{}, 1)
	dir.onCAS = func(rans.ID) {
		select {
		case <-armed:
		default:
			return
		}
		select {
		case entered <- struct{}{}:
			<-gate // 首次（迁移）CAS 卡住直至放行
		default:
		}
	}
	migPlacement := &migrateArmedPlacement{target: 2}
	c := NewCluster(t, 2, map[string]rans.Factory{
		"spycounter": func(rans.ID) rans.Entity { return &spyCounterEntity{} },
	}, WithDirectory(dir), WithCheckpointStore(store), WithNodePlacement(0, migPlacement))
	ctx := context.Background()
	id := mustID(t, "spycounter", "del-resurrect")
	for i := 0; i < 3; i++ {
		if _, err := c.Node(0).Call(ctx, id, "inc"); err != nil {
			t.Fatalf("inc %d: %v", i, err)
		}
	}
	// 卡住迁移 CAS 窗：快照已落 Store（尽力而为）、随迁包已发 node2；
	// 窗内外部抢注归属 node2 → 本迁移判输 → node2 缓存里是过期包。
	close(armed)
	migPlacement.arm()
	migDone := make(chan error, 1)
	go func() { migDone <- c.Node(0).Migrate(ctx, id) }()
	<-entered
	dir.setOwner(id, 2) // 窗内外部抢注：迁移 CAS 必判输
	close(gate)
	_ = <-migDone
	// 宿主删除实体与状态：Destroy（终快照+墓碑）+ Store.Delete。
	if err := c.Node(0).Destroy(ctx, id); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("store delete: %v", err)
	}
	if _, ok, _ := store.Get(ctx, id); ok {
		t.Fatal("precondition: store must miss after delete")
	}
	// 目标端重激活（node2 首调）：不得复活已删除状态（bal 必须 0、
	// Store 不得被写回旧值）。
	time.Sleep(200 * time.Millisecond)
	if v, err := c.Node(1).Call(ctx, id, "get"); err != nil || v != 0 {
		t.Fatalf("reactivated deleted entity: v=%v err=%v (want 0 — deleted state must not resurrect via stale package)", v, err)
	}
	if b, ok, _ := store.Get(ctx, id); ok && len(b) > 0 && b[0] != 0 {
		t.Fatalf("store rewritten with deleted state: %v", b)
	}
}
