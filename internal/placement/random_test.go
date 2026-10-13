package placement

// RandomPlacement 测试：
// 候选查询函数内存假实现驱动全部行为（数据源从 *View 收窄为查询函数：
// NewSeededRandomPlacement(f.get, seed)）。

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/cyinx/rans_golang"
)

// fakeNodes 是可变内存数据源：锁保护nodes/err，get 返回副本。
type fakeNodes struct {
	mu    sync.Mutex
	nodes []rans.NodeID
	err   error
}

func (f *fakeNodes) set(nodes []rans.NodeID, err error) {
	f.mu.Lock()
	f.nodes, f.err = nodes, err
	f.mu.Unlock()
}

func (f *fakeNodes) get(context.Context) ([]rans.NodeID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]rans.NodeID, len(f.nodes))
	copy(out, f.nodes)
	return out, f.err
}

// TestRandomPlacementDistribution 种子统计：固定种子下 300 次拾取全部
// 落在候选集内且命中不少于 2 个不同节点（随机性最弱 sanity——完全退化成
// 常数选择即失败）。
func TestRandomPlacementDistribution(t *testing.T) {
	f := &fakeNodes{}
	f.set([]rans.NodeID{1, 2, 3}, nil)
	p := NewSeededRandomPlacement(f.get, 42)

	seen := make(map[rans.NodeID]int)
	for i := 0; i < 300; i++ {
		n, ok := p.Pick(0)
		if !ok {
			t.Fatalf("pick %d: unexpected (0,false) over 3 live nodes", i)
		}
		if n != 1 && n != 2 && n != 3 {
			t.Fatalf("pick %d = %d, outside candidate set", i, n)
		}
		seen[n]++
	}
	if len(seen) < 2 {
		t.Fatalf("300 picks hit only %d distinct node(s): %v", len(seen), seen)
	}
}

// TestRandomPlacementExclude 排除语义：exclude 之外的候选恒可选；
// 排除后空集 → (0,false)。
func TestRandomPlacementExclude(t *testing.T) {
	f := &fakeNodes{}
	f.set([]rans.NodeID{1, 2, 3}, nil)
	p := NewSeededRandomPlacement(f.get, 7)

	for i := 0; i < 100; i++ {
		n, ok := p.Pick(2)
		if !ok || (n != 1 && n != 3) {
			t.Fatalf("Pick(2) = %d, %v; want 1 or 3, true", n, ok)
		}
	}

	// 候选恰为单节点且被排除 → 空集
	f.set([]rans.NodeID{5}, nil)
	if n, ok := p.Pick(5); ok || n != 0 {
		t.Fatalf("Pick(5) over {5} = %d, %v; want 0, false", n, ok)
	}
	if n, ok := p.Pick(0); !ok || n != 5 {
		t.Fatalf("Pick(0) over {5} = %d, %v; want 5, true", n, ok)
	}
}

// TestRandomPlacementEmptyAndError 空集与数据源失败：都收敛为 (0,false)
// （调用方按 bool 分流，错误不炸放置路径）。
func TestRandomPlacementEmptyAndError(t *testing.T) {
	f := &fakeNodes{}
	p := NewSeededRandomPlacement(f.get, 1)

	f.set(nil, nil)
	if n, ok := p.Pick(0); ok || n != 0 {
		t.Fatalf("Pick over empty set = %d, %v; want 0, false", n, ok)
	}

	f.set(nil, errors.New("redis down"))
	if n, ok := p.Pick(0); ok || n != 0 {
		t.Fatalf("Pick over failing source = %d, %v; want 0, false", n, ok)
	}
}
