package rpc

// Placement 鸭子类型锚点：接口落在消费方 rpc，
// placement 不导入 rpc（依赖单向向下）——结构性满足由本文件在
// 消费方测试里编译期锁定，防两侧签名漂移。

import (
	"context"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/placement"
)

// 编译期断言：placement.RandomPlacement 满足 rpc.Placement。
var _ Placement = (*placement.RandomPlacement)(nil)

// 编译期断言：placement.KindPlacement 同时满足 rpc.Placement 与
// rpc.KindPlacement（路由梯子经 KindPlacement
// 断言探测，按 kind 询问优先于 Pick）。
var _ Placement = (*placement.KindPlacement)(nil)
var _ KindPlacement = (*placement.KindPlacement)(nil)

// TestPlacementSatisfiedByRandomPlacement 装配形态：候选查询函数 +
// RandomPlacement 直接作为 rpc.Placement 消费（规格 §12 放置「存活节点 ×
// 策略[缺省 Random]」——目录 miss 路径的目标选择器）。
func TestPlacementSatisfiedByRandomPlacement(t *testing.T) {
	var p Placement = placement.NewSeededRandomPlacement(
		func(context.Context) ([]rans.NodeID, error) {
			return []rans.NodeID{2, 5}, nil
		}, 3)

	n, ok := p.Pick(2)
	if !ok || n != 5 {
		t.Fatalf("Pick(2) = %d, %v; want 5, true (exclude honored)", n, ok)
	}
	if n, ok := p.Pick(9); !ok || (n != 2 && n != 5) {
		t.Fatalf("Pick(9) = %d, %v; want 2 or 5, true", n, ok)
	}
}
