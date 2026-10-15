package wire

// 播种生成器：随机起点跨实例不同且非 0、单调递增；零值生成器保留从 1
// 起的既有语义（线上字段格式与既有单测依赖零值行为不变）。

import "testing"

func TestIDGen_RandomOriginPerGenerator(t *testing.T) {
	g1, g2 := NewIDGen(), NewIDGen()
	f1, f2 := g1.Next(), g2.Next()
	if f1 == 0 || f2 == 0 {
		t.Fatalf("seeded origins must be non-zero: %d, %d", f1, f2)
	}
	if f1 == f2 {
		t.Fatalf("two seeded generators must not share an origin: %d", f1)
	}
	for _, g := range []*IDGen{g1, g2} {
		prev := g.Next()
		for i := 0; i < 1000; i++ {
			next := g.Next()
			if next <= prev {
				t.Fatalf("id regressed: %d -> %d", prev, next)
			}
			prev = next
		}
	}
}
