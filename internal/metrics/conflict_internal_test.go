package metrics

import "testing"

// TestCounterTypeConflictRegistersManualTrack：跨类型名冲突（接线 bug）的
// Counter 降级手动轨后必须入 r.counters，与 Gauge 侧冲突分支对称；
// Snapshot 同名键以 gauge 读数呈现（既定契约，见包注释）。
func TestCounterTypeConflictRegistersManualTrack(t *testing.T) {
	r := New("t")
	r.Gauge("dup", "g", func() float64 { return 1 })
	c := r.Counter("dup", "c")
	c.Add(2)
	if _, ok := r.counters["dup"]; !ok {
		t.Fatal("type-conflict counter must stay registered in r.counters (manual track visible)")
	}
	if got := r.Snapshot()["dup"]; got != 1 {
		t.Fatalf("Snapshot[dup] = %v, want gauge value 1 (documented precedence)", got)
	}
}
