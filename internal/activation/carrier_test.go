package activation

import (
	"context"
	"errors"
	"github.com/cyinx/rans_golang"
	"testing"
)

// fakeCarrierEntity 同时实现 rans.Entity 最小面与 rans.Snapshotter——编译级
// 钉死：两接口可由同一实体实现，能力面经类型断言发现（规格 §13.3）。
type fakeCarrierEntity struct {
	state  []byte
	broken bool // Restore 拒绝的形状
}

func (f *fakeCarrierEntity) Activate(ctx context.Context) error { return nil }
func (f *fakeCarrierEntity) Handle(ctx context.Context, req any) (any, error) {
	return nil, nil
}
func (f *fakeCarrierEntity) Deactivate(ctx context.Context) error { return nil }

func (f *fakeCarrierEntity) Snapshot() ([]byte, error) {
	if f.broken {
		return nil, errors.New("capture unavailable")
	}
	out := make([]byte, len(f.state))
	copy(out, f.state)
	return out, nil
}

func (f *fakeCarrierEntity) Restore(b []byte) error {
	if f.broken {
		return errors.New("shape mismatch") // 形状不匹配 → error → 上层退化 Store
	}
	f.state = append(f.state[:0], b...)
	return nil
}

// 编译期断言：fakeCarrierEntity 同时满足 rans.Entity 与 rans.Snapshotter。
var (
	_ rans.Entity      = (*fakeCarrierEntity)(nil)
	_ rans.Snapshotter = (*fakeCarrierEntity)(nil)
)

// 能力发现机制：实体是否实现 rans.Snapshotter 由运行时类型断言裁决——
// 实现 → 走随迁包快路径；未实现 → Store 兜底（零接入成本，不改 rans.Entity 主面）。
func TestSnapshotterTypeAssertionDiscovery(t *testing.T) {
	var e rans.Entity = &fakeCarrierEntity{state: []byte("v1")}

	carrier, ok := e.(rans.Snapshotter)
	if !ok {
		t.Fatal("carrier-capable entity must be discoverable via type assertion")
	}
	snap, err := carrier.Snapshot()
	if err != nil || string(snap) != "v1" {
		t.Fatalf("Snapshot = (%q, %v), want (v1, nil)", snap, err)
	}
	if err := carrier.Restore([]byte("v2")); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if string(carrier.(*fakeCarrierEntity).state) != "v2" {
		t.Fatalf("state = %q, want v2", carrier.(*fakeCarrierEntity).state)
	}
}
