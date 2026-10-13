package placement

// KindPlacement 单元测试（spec §10 kind 感知放置）：
// Pick = NodeIDs 全集随机（排除 exclude）、PickForKind = NodesByKind 过滤
// （无候选 false）、数据源失败的收敛形态。错误在数据源/View 层响亮传播
// （View 直通锚定见 membership 的 view_test.go TestAliveNodesPassThrough、redis 坏值
// 锚定见 directory/redis TestNodeKeyBadValueErrors）——Placement 接口无错误
// 通道，坏值让放置收敛为「无候选 → 调用方保持本地激活」，与
// RandomPlacement 同纪律。

import (
	"context"
	"errors"
	"github.com/cyinx/rans_golang"
	"testing"
)

// fakeKindScanner kind 感知放置数据源桩：全节点视图 + per-kind 过滤视图，
// err 非-nil 时两面同错（坏值场景的等价形态）。
type fakeKindScanner struct {
	all    []rans.NodeID
	byKind map[string][]rans.NodeID
	err    error
}

func (f *fakeKindScanner) NodeIDs(context.Context) ([]rans.NodeID, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.all, nil
}

func (f *fakeKindScanner) NodesByKind(_ context.Context, kind string) ([]rans.NodeID, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.byKind[kind], nil
}

// TestKindPlacementPickUsesFullNodeSet Pick 走 NodeIDs 全集（业务节点 +
// 纯接入节点），exclude 排除 honored——全集语义是 kind 过滤的兜底面
// （消费方不区分 kind 时行为与 RandomPlacement 等价）。
func TestKindPlacementPickUsesFullNodeSet(t *testing.T) {
	p := NewSeededKindPlacement(&fakeKindScanner{
		all:    []rans.NodeID{2, 5, 9},
		byKind: map[string][]rans.NodeID{},
	}, 7)

	for i := 0; i < 50; i++ {
		n, ok := p.Pick(0)
		if !ok || (n != 2 && n != 5 && n != 9) {
			t.Fatalf("Pick(0) = %d, %v; want a member of the full node set", n, ok)
		}
	}
	// exclude=9：候选只剩 {2,5}
	for i := 0; i < 50; i++ {
		n, ok := p.Pick(9)
		if !ok || (n != 2 && n != 5) {
			t.Fatalf("Pick(9) = %d, %v; want 2 or 5 (exclude honored)", n, ok)
		}
	}
	// exclude 耗尽全集 → 无候选 false（单节点集 + 排除该节点）
	solo := NewSeededKindPlacement(&fakeKindScanner{
		all:    []rans.NodeID{4},
		byKind: map[string][]rans.NodeID{"solo": {4}},
	}, 13)
	if n, ok := solo.Pick(4); ok || n != 0 {
		t.Fatalf("Pick(4) with full set {%d} excluded = %d, %v; want 0, false", 4, n, ok)
	}
	if n, ok := solo.PickForKind("solo", 4); ok || n != 0 {
		t.Fatalf("PickForKind(solo, 4) = %d, %v; want 0, false", n, ok)
	}
}

// TestKindPlacementPickForKindFiltersByKind PickForKind 走 NodesByKind 过滤：
// 未声明目标 kind 的节点（节点 3 不声明 counter）永不进该 kind 候选；
// exclude honored；无人声明的 kind → (0, false)。
func TestKindPlacementPickForKindFiltersByKind(t *testing.T) {
	p := NewSeededKindPlacement(&fakeKindScanner{
		all: []rans.NodeID{1, 2, 3},
		byKind: map[string][]rans.NodeID{
			// 节点 3 只声明 greet，不声明 counter
			"counter": {1, 2},
			"greet":   {1, 2, 3},
		},
	}, 11)

	for i := 0; i < 50; i++ {
		n, ok := p.PickForKind("counter", 0)
		if !ok || (n != 1 && n != 2) {
			t.Fatalf("PickForKind(counter, 0) = %d, %v; want 1 or 2 (non-host node 3 excluded)", n, ok)
		}
	}
	// exclude 排除后仅剩单候选：确定性命中
	if n, ok := p.PickForKind("counter", 2); !ok || n != 1 {
		t.Fatalf("PickForKind(counter, 2) = %d, %v; want 1, true", n, ok)
	}
	// 无人声明的 kind：空集（ErrNoEligibleTarget 面，非错误）
	if n, ok := p.PickForKind("workspace", 0); ok || n != 0 {
		t.Fatalf("PickForKind(workspace, 0) = %d, %v; want 0, false (kind nobody declares)", n, ok)
	}
	// 声明面如实承载（kinds 视图本身不做业务过滤）
	if n, ok := p.PickForKind("greet", 0); !ok || (n != 1 && n != 2 && n != 3) {
		t.Fatalf("PickForKind(greet, 0) = %d, %v; want a member of {1,2,3}", n, ok)
	}
}

// TestKindPlacementScannerErrorDegradesToNoCandidate 数据源失败（含 redis
// 坏值场景在 liveNodes 层报错后传导到此）→ (0, false)：Placement 接口无
// 错误通道，按无候选收敛（调用方保持 Active/本地激活，与 RandomPlacement
// 同纪律）；错误本体在数据源与 View 层响亮传播，不在此吞成空集后无声。
func TestKindPlacementScannerErrorDegradesToNoCandidate(t *testing.T) {
	sentinel := errors.New("boom: malformed node key value")
	p := NewKindPlacement(&fakeKindScanner{err: sentinel})

	if n, ok := p.Pick(0); ok || n != 0 {
		t.Fatalf("Pick on scanner error = %d, %v; want 0, false", n, ok)
	}
	if n, ok := p.PickForKind("counter", 0); ok || n != 0 {
		t.Fatalf("PickForKind on scanner error = %d, %v; want 0, false", n, ok)
	}
}

// TestKindPlacementNilScanner nil 数据源视同恒空集（防御形态，与
// NewRandomPlacement(nil view) 同口径）。
func TestKindPlacementNilScanner(t *testing.T) {
	p := NewKindPlacement(nil)
	if n, ok := p.Pick(0); ok || n != 0 {
		t.Fatalf("Pick(nil scanner) = %d, %v; want 0, false", n, ok)
	}
	if n, ok := p.PickForKind("counter", 0); ok || n != 0 {
		t.Fatalf("PickForKind(nil scanner) = %d, %v; want 0, false", n, ok)
	}
}
