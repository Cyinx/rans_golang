package wire

import "testing"

func TestPagePoolRoundTrip(t *testing.T) {
	p := NewPagePool()
	b := p.Get(100)
	if cap(b) < 100 {
		t.Fatal("capacity")
	}
	b = append(b, 1, 2, 3)
	p.Put(b[:0]) // 归还重置长度
	b2 := p.Get(100)
	if cap(b2) < 100 {
		t.Fatal("cap after put")
	}
}

func TestPagePoolClasses(t *testing.T) {
	p := NewPagePool()
	for _, n := range []int{1, 5000, 100_000, 3 << 20} {
		b := p.Get(n)
		if cap(b) < n {
			t.Fatalf("n=%d cap=%d", n, cap(b))
		}
		for i := range b { // 页内容可用
			b[i] = byte(i)
		}
	}
}

func TestPagePoolStatsBalance(t *testing.T) {
	// 获取-归还收支平衡：漏还可被 Gets-Puts 检出（陷阱 1 的可测性锚点）。
	p := NewPagePool()
	for i := 0; i < 100; i++ {
		b := p.Get(64)
		p.Put(b[:0])
	}
	s := p.Stats()
	if s.Gets != 100 || s.Puts != 100 {
		t.Fatalf("stats: %+v", s)
	}
}
