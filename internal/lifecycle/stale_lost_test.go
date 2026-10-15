package lifecycle

// 落库点遇 ErrStaleIncarnation 即失主（陈旧即失主）：钝化按无快照终局、
// 迁移判输后源实例不得回退 Active 继续服役。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
)

// staleStore Put 恒 ErrStaleIncarnation（模拟新化身已落库的高水位）。
type staleStore struct{ checkpoint.Store }

func (s *staleStore) Put(context.Context, rans.ID, uint64, []byte) error {
	return checkpoint.ErrStaleIncarnation
}

// newStaleTable 单节点表：有状态实体 + 恒 stale 的 Store。
func newStaleTable(t *testing.T, opts ...activation.Options) *activation.Table {
	t.Helper()
	return activation.New(inmem.New(), 1, map[string]rans.Factory{
		"carrier": func(_ rans.ID) rans.Entity { return &acctEntity{bal: 1} },
	}, opts...)
}

// TestPassivate_StaleTerminates 钝化遇 stale 按无快照直接终局：返回成功
// 形态且实例终局（陈旧状态本就不可落库，失败形态会把可安全回收的实体
// 滞留在 Active）。
func TestPassivate_StaleTerminates(t *testing.T) {
	m := newStaleTable(t, activation.Options{Store: &staleStore{Store: checkpoint.New()}})
	id := mustTestID(t, "carrier", "passi-stale")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-passivate")
	pctx, pcancel := ctxT(2 * time.Second)
	defer pcancel()
	if err := Passivate(pctx, m, id); err != nil {
		t.Fatalf("passivate on stale store must terminate as success, got %v", err)
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("passivated instance must reach terminal")
	}
}

// TestMigrate_StaleDropsSource 迁移快照 Put 被拒为 stale：判输返回错误，
// 且源实例被围栏销毁——不得回退 Active 继续服务（更新的化身已在落库，
// 旧实例继续接收写入即制造已确认写丢失）。
func TestMigrate_StaleDropsSource(t *testing.T) {
	m := newStaleTable(t, activation.Options{
		Store:     &staleStore{Store: checkpoint.New()},
		Placement: func(string, rans.NodeID) (rans.NodeID, bool) { return 7, true },
	})
	id := mustTestID(t, "carrier", "mig-stale")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "pre-migrate")
	mctx, mcancel := ctxT(3 * time.Second)
	defer mcancel()
	if err := Migrate(mctx, m, id); err == nil {
		t.Fatal("stale migration must be judged lost")
	}
	waitFor(t, "stale source dropped from local table", func() bool {
		_, ok := m.GetLocal(id)
		return !ok
	})
}
