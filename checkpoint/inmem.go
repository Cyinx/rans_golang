package checkpoint

import (
	"context"
	"sync"

	"github.com/cyinx/rans_golang"
)

// InMemory 是 Store 的内存实现（测试/单机形态，不跨进程/重启）。
// Put 拷贝入、Get 拷贝出——调用方对切片的任何后续改写都不影响已存内容；
// 并发安全（单互斥足够：写多读少，规格 §17 惯例）；零值可用（内部 map
// 懒初始化，&InMemory{} 与 New() 等价）。围栏语义同 Store.Put 契约。
// 并发约定（spec §5.8）：全部方法不阻塞（纯内存互斥，微秒级）；可在实体
// Handle 内调用。
type InMemory struct {
	mu   sync.Mutex
	data map[rans.ID][]byte
	inc  map[rans.ID]uint64 // per-id 已接受的最大 incarnation（0 直通不记）
}

// 编译期断言：InMemory 满足 Store。
var _ Store = (*InMemory)(nil)

// New 返回可用的空 InMemory Store。
func New() *InMemory { return &InMemory{data: make(map[rans.ID][]byte)} }

// Get 拷贝出存储的快照副本；未命中 (nil, false, nil)。
func (s *InMemory) Get(ctx context.Context, id rans.ID) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.data[id]
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, true, nil
}

// Put 拷贝入快照；incarnation 回退（小于已存最大值）按 ErrStaleIncarnation
// 拒绝，相等覆盖（同 incarnation 重试），0 直通（未围栏形态）。
func (s *InMemory) Put(ctx context.Context, id rans.ID, incarnation uint64, snapshot []byte) error {
	cp := make([]byte, len(snapshot))
	copy(cp, snapshot)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[rans.ID][]byte)
	}
	if s.inc == nil {
		s.inc = make(map[rans.ID]uint64)
	}
	if incarnation != 0 && incarnation < s.inc[id] {
		return ErrStaleIncarnation
	}
	if incarnation != 0 {
		s.inc[id] = incarnation
	}
	s.data[id] = cp
	return nil
}

// Delete 幂等删除：未命中为 no-op（nil map 上 delete 同样安全）。不撤销
// 纪元高水位（单调性跨删除存续——zombie 围栏的前提，删除不是重置）。
func (s *InMemory) Delete(ctx context.Context, id rans.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, id)
	return nil
}
