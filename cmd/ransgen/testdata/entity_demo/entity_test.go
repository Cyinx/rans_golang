// entity_test 锁死实体面生成物行为：Kinds 注册表、wrapper 生命周期委托、
// Handle 未知请求防线。（代理/分发往返在 proxy_test。）
package entitydemo

import (
	"context"
	"strings"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/lifecycle"
)

// TestKindsRegistry Kinds 注册表：kind 命中且 factory 产可用 Entity。
func TestKindsRegistry(t *testing.T) {
	f, ok := Kinds["counter"]
	if !ok {
		t.Fatal(`Kinds["counter"] 缺失`)
	}
	var ent rans.Entity = f(rans.ID{Kind: "counter", Key: "k1"})
	if ent == nil {
		t.Fatal("factory 产 nil Entity")
	}
}

// TestWrapperLifecycleDelegation wrapper 生命周期委托用户实体方法。
func TestWrapperLifecycleDelegation(t *testing.T) {
	c := &Counter{}
	w := &CounterPawnite{E: c}
	ctx := context.Background()
	if err := w.Activate(ctx); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if !c.alive {
		t.Fatal("Activate 未委托到实体")
	}
	if err := w.Deactivate(ctx); err != nil {
		t.Fatalf("Deactivate: %v", err)
	}
	if c.alive {
		t.Fatal("Deactivate 未委托到实体")
	}
}

// TestWrapperHandleUnknownRequest 骨架期 Handle：未知请求一律报错。
func TestWrapperHandleUnknownRequest(t *testing.T) {
	w := &CounterPawnite{E: &Counter{}}
	_, err := w.Handle(context.Background(), struct{}{})
	if err == nil || !strings.Contains(err.Error(), "rans: unknown request") {
		t.Fatalf("err = %v, want 含 rans: unknown request", err)
	}
}

// TestWrapperSnapshotCarrierFace wrapper 快照透传：用户实体
// 实现 rans.Snapshotter 时 wrapper 同步实现并透传——编译期接口
// 断言 + 捕获/恢复往返（经 wrapper 到达用户实体）。
func TestWrapperSnapshotCarrierFace(t *testing.T) {
	var _ rans.Snapshotter = (*CounterPawnite)(nil)

	c := &Counter{n: 42, alive: true}
	w := &CounterPawnite{E: c}
	b, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(b) != 9 {
		t.Fatalf("snapshot len = %d, want 9", len(b))
	}
	restored := &Counter{}
	w2 := &CounterPawnite{E: restored}
	if err := w2.Restore(b); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if restored.n != 42 || !restored.alive {
		t.Fatalf("restored = (%d, %v), want (42, true)", restored.n, restored.alive)
	}
	if err := w2.Restore([]byte("short")); err == nil {
		t.Fatal("bad snapshot shape must error (carrier contract)")
	}
}

// TestPassivateRestoresGeneratedEntity 生成形态实体端到端（钝化恢复）：
// wrapper 透传快照面后，manager 钝化（终快照落 Store）→ 重激活从 Store
// 恢复——生成实体的状态跨钝化存活（此前 wrapper 不透传，快照面不可见，
// 钝化把生成实体当无状态丢弃）。
func TestPassivateRestoresGeneratedEntity(t *testing.T) {
	store := checkpoint.New()
	m := activation.New(inmem.New(), 1, Kinds, activation.Options{Store: store})
	ctx := context.Background()

	id, err := rans.NewID("counter", "carrier-e2e")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		v, err := m.InvokeLocal(ctx, id, CounterAddRequest{Delta: 5})
		if err != nil {
			t.Fatalf("add #%d: %v", i+1, err)
		}
		if v != int64(5*(i+1)) {
			t.Fatalf("add #%d = %v, want %d", i+1, v, 5*(i+1))
		}
	}

	if err := lifecycle.Passivate(ctx, m, id); err != nil {
		t.Fatalf("Passivate = %v, want nil", err)
	}
	if snap, found, _ := store.Get(ctx, id); !found || len(snap) != 9 {
		t.Fatalf("store snapshot = (%d bytes, %v), want 9 bytes/found — final snapshot via wrapper", len(snap), found)
	}

	// 重激活从 Store 恢复：计数连续（15 + 5 = 20）。
	v, err := m.InvokeLocal(ctx, id, CounterAddRequest{Delta: 5})
	if err != nil {
		t.Fatalf("reactivated add: %v", err)
	}
	if v != int64(20) {
		t.Fatalf("reactivated add = %v, want 20 (restored from final snapshot)", v)
	}
	if err := lifecycle.Shutdown(ctx, m); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
