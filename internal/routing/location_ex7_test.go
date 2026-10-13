package routing

// EX7（H15）：LocationCache 饱和后新 id 准入延迟——分片化后准入扫描面 =
// 容量/64（对照实测：1 万容量 246µs、10 万容量 3.31ms 的全表扫描形态）。
// 断言：10 万容量饱和后新 id 准入亚毫秒级。

import (
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

func TestHR4_EX7_AdmissionLatency(t *testing.T) {
	c := NewLocationCache(time.Hour, time.Hour, 100_000) // 长 TTL：饱和态
	fill := make([]rans.ID, 0, 100_000)
	for i := 0; i < 100_000; i++ {
		id, err := rans.NewID("k", "fill-"+itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		fill = append(fill, id)
	}
	for _, id := range fill {
		c.PutPositive(id, directory.Owner{Node: 3})
	}
	fresh, _ := rans.NewID("k", "fresh-admission")
	start := time.Now()
	c.PutPositive(fresh, directory.Owner{Node: 3})
	if d := time.Since(start); d > time.Millisecond {
		t.Fatalf("saturated admission = %v, want sub-millisecond (EX7: 10w capacity was 3.31ms full-scan)", d)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
