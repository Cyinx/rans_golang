package activation

// 激活实体常驻内存预算（收件箱按需增长的行为锚）。

import (
	"context"
	"github.com/cyinx/rans_golang"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/directory/inmem"
)

// heapNow 稳态存活堆采样：双 GC 排掉浮动垃圾后读 HeapAlloc。
func heapNow() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// TestActivation_MemoryPerEntityWithinBudget 2000 个激活实体的常驻堆
// < 8 KiB/实体（收件箱按需增长前 ≈163 KiB/实体——New 时按 InboxDepthLimit
// 预分配容量的 chan 收件箱所致）。
func TestActivation_MemoryPerEntityWithinBudget(t *testing.T) {
	const N = 2000
	m := New(inmem.New(), 1, map[string]rans.Factory{"k": func(_ rans.ID) rans.Entity { return nopTestEntity{} }})
	h0 := heapNow()
	for i := 0; i < N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := m.InvokeLocal(ctx, mustTestID(t, "k", strconv.Itoa(i)), "ping"); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
	}
	per := float64(heapNow()-h0) / N
	t.Logf("heap/实体=%.1f KiB", per/1024)
	if per >= 8*1024 {
		t.Fatalf("每实体内存 %.1f KiB 超出 8 KiB 预算（收件箱未按需增长？）", per/1024)
	}
}
