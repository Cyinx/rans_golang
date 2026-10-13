package lifecycle

// 终局 cleanup 的目录注销调用带超时 ctx 契约——
// Background 无界时慢目录会冻结看门狗扫描环（AbandonStuck* 的
// cleanup 在扫描 goroutine 上同步执行）。断言收敛为确定性信号：
// 注销调用收到的 ctx 必带 deadline（不依赖时序/时长测量）。

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
)

// deadlineRecordingDir 包装目录记录实例级 UnregisterIfOwner 收到的 ctx
// （注销面即 Owner 实例标记形态）。
type deadlineRecordingDir struct {
	directory.Directory
	mu     sync.Mutex
	ctxs   []context.Context
	deadls []bool
}

func (d *deadlineRecordingDir) UnregisterIfOwner(ctx context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	d.mu.Lock()
	d.ctxs = append(d.ctxs, ctx)
	_, has := ctx.Deadline()
	d.deadls = append(d.deadls, has)
	d.mu.Unlock()
	return d.Directory.UnregisterIfOwner(ctx, id, owner)
}

func (d *deadlineRecordingDir) recorded() (n, withDeadline int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, has := range d.deadls {
		if has {
			withDeadline++
		}
	}
	return len(d.deadls), withDeadline
}

// TestCleanupUnregisterHasDeadline 销毁终局的目录注销带超时预算：
// 激活（owner 已捕获）→ Destroy → cleanup 的实例级 UnregisterIfOwner
// 收到的 ctx 必带 deadline。
func TestCleanupUnregisterHasDeadline(t *testing.T) {
	dir := &deadlineRecordingDir{Directory: inmem.New()}
	m := activation.New(dir, 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &counterEntity{} },
	})
	id, _ := rans.NewID("counter", "cleanup-ctx")
	if _, err := m.GetOrActivate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	DestroyLocal(ctx, m, id)
	n, withDeadline := dir.recorded()
	if n == 0 {
		t.Fatal("cleanup never issued UnregisterIfOwner (test wiring broken)")
	}
	if withDeadline != n {
		t.Fatalf("cleanup UnregisterIfOwner ctx with deadline = %d/%d; want all (bounded, not Background)", withDeadline, n)
	}
}
