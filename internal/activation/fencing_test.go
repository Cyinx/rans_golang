package activation

// 实例身份围栏原语与 InstanceOwner 并发面：围栏必须精确到实例身份——按
// id 围栏会误伤同 id 新化身；InstanceOwner 与逐出的 map 访问必须是同一
// 把锁的同一口径。

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// TestTable_InstanceOwnerVsEvict InstanceOwner（协作读面）与激活/逐出
// （写面）并发运行：-race 下不得报竞态。
func TestTable_InstanceOwnerVsEvict(t *testing.T) {
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity { return &counterEntity{} },
	})
	id, _ := rans.NewID("cell", "race-1")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					m.InstanceOwner(id)
				}
			}
		}()
	}
	for i := 0; i < 100; i++ {
		p, err := m.GetOrActivate(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		m.evict(id, p) // 与 GetLocal/终局 cleanup 同路径的本地逐出写面
	}
	close(stop)
	wg.Wait()
}

// TestTable_FenceIfOwnerSkipsNewer 条件围栏只命中 owner 全等的实例：旧
// 归属请求不得围栏已换代的新实例。
func TestTable_FenceIfOwnerSkipsNewer(t *testing.T) {
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity { return &counterEntity{} },
	})
	id, _ := rans.NewID("cell", "fio-1")
	ctx := context.Background()
	p1, err := m.GetOrActivate(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	o1, ok := waitInstanceOwner(m, id)
	if !ok {
		t.Fatal("precondition: activated instance must expose its owner")
	}
	if _, ok := m.FenceIfOwner(id, o1); !ok {
		t.Fatal("exact owner match must fence")
	}
	m.evict(id, p1) // 模拟换代：旧实例逐出、同 id 重新激活
	if _, err := m.GetOrActivate(ctx, id); err != nil {
		t.Fatal(err)
	}
	o2, ok := waitInstanceOwner(m, id)
	if !ok {
		t.Fatal("re-activated instance must expose its owner")
	}
	if o2.Incarnation <= o1.Incarnation {
		t.Fatalf("precondition: newer incarnation %d must exceed old %d", o2.Incarnation, o1.Incarnation)
	}
	if _, hit := m.FenceIfOwner(id, o1); hit {
		t.Fatal("stale owner must not fence the newer instance")
	}
	stale := directory.Owner{Node: o1.Node, Epoch: o1.Epoch + 1, Incarnation: o1.Incarnation}
	if _, hit := m.FenceIfOwner(id, stale); hit {
		t.Fatal("owner differing from the registered one must not fence")
	}
	if _, ok := m.FenceIfOwner(id, o2); !ok {
		t.Fatal("current owner must still fence")
	}
}

// waitInstanceOwner 有界等待激活闭包把实例归属落位（注册原子域在 pawnite
// run goroutine 上异步完成）。
func waitInstanceOwner(m *Table, id rans.ID) (directory.Owner, bool) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if o, ok := m.InstanceOwner(id); ok {
			return o, true
		}
		time.Sleep(time.Millisecond)
	}
	return directory.Owner{}, false
}
