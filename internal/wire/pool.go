package wire

import (
	"sync"
	"sync/atomic"
)

// 页池三档容量边界（决策出处：docs/decisions/2026-09-29-phase2-buffer-pool.md）：
// 小页高频复用；超 1MiB 直接分配交 GC——帧上限虽为 4MiB，但大页入池会长期
// 滞留高内存，得不偿失。
const (
	pageClassSmall  = 4 << 10  // 4KiB
	pageClassMedium = 64 << 10 // 64KiB
	pageClassLarge  = 1 << 20  // 1MiB
	pageClassCount  = 3
)

// PageStats Get/Put 计数快照：测试锚点（获取-归还收支平衡，漏还可由
// Gets-Puts 检出，陷阱 1 的可测性锚点），非运行时约束。只计池档内操作
// （非池档 Get 不入计、Put 被忽略），保证差值语义封闭在池化宇宙内。
type PageStats struct {
	Gets uint64
	Puts uint64
}

// PagePool 分档页池：≤4KiB/≤64KiB/≤1MiB 三档 sync.Pool。
// 所有权纪律：获取方 defer 归还，路由梯子每层失败路径归还（决策文档）；
// Put 后调用方不得再引用该页。
type PagePool struct {
	pools [pageClassCount]sync.Pool
	gets  atomic.Uint64
	puts  atomic.Uint64
}

func NewPagePool() *PagePool { return &PagePool{} }

// pageClass 返回 n 所属池档序号（0/1/2）；-1 表示不入池（超 1MiB）。
func pageClass(n int) int {
	switch {
	case n <= pageClassSmall:
		return 0
	case n <= pageClassMedium:
		return 1
	case n <= pageClassLarge:
		return 2
	default:
		return -1
	}
}

// Get 取一页：len=n、cap≥n，可直接写入（io.ReadFull(page) 就位）。
// 池化命中时内容为旧值残留，不保证清零。n<0 归一为 0（不可信长度防御，
// 陷阱 10：校验先于分配）。超 1MiB 直接 make 不入池亦不计计数。
func (p *PagePool) Get(n int) []byte {
	if n < 0 {
		n = 0
	}
	c := pageClass(n)
	if c < 0 {
		return make([]byte, n)
	}
	p.gets.Add(1)
	if b, ok := p.pools[c].Get().(*[]byte); ok && cap(*b) >= n {
		return (*b)[:n] // 档内取出的页可能偏小（cap<n）：丢弃换新页
	}
	return make([]byte, n)
}

// Put 归还一页：按 cap 分档入池，非池档忽略（含计数）。归还前重置长度
// 为 0（调用方习惯传 b[:0]，此处兜底保证）。归还后页所有权转移给池。
func (p *PagePool) Put(b []byte) {
	c := pageClass(cap(b))
	if c < 0 {
		return
	}
	p.puts.Add(1)
	b = b[:0] // 重置长度后再归还（地址入池避免装箱分配）
	p.pools[c].Put(&b)
}

// Stats 返回计数快照（两原子各自读，非一致性点快照）。
func (p *PagePool) Stats() PageStats {
	return PageStats{Gets: p.gets.Load(), Puts: p.puts.Load()}
}
