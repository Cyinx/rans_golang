package node

import (
	"context"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/lifecycle"
)

// LocalIDs 返回本地激活表的身份快照（只读观测面：排空效果断言、运维自
// 检）。不阻塞；并发安全；可在实体 Handle 内调用。
func (n *Node) LocalIDs() []rans.ID { return n.mgr.LocalIDs() }

// Destroy 优雅终止一个本地激活（Retire 交接窗：终快照 → Deactivate → 条
// 件注销）。只作用于本地实例：目标不在本地返回 NotFound + ReasonNotLocal
// （不再静默成功——消费方据此分辨「已销毁」与「不在本节点」；路由到
// owner 执行的管理操作是后续扩展点）。
//
// 阻塞到终态或 ctx 耗尽（ctx 是等待预算，耗尽只解除等待，终态由实体侧自
// 驱闭环）；并发安全；可在实体 Handle 内调用（对自身经串行队列等待）。
func (n *Node) Destroy(ctx context.Context, id rans.ID) error {
	if _, ok := n.mgr.GetLocal(id); !ok {
		return rans.NewError(rans.CodeNotFound, rans.ReasonNotLocal,
			"not local: "+id.String())
	}
	return lifecycle.DestroyLocal(ctx, n.mgr, id)
}

// Migrate 把本地激活交接给放置选出的目标节点（快照落 Store / 随迁包直发 /
// 目录换锁 / 在箱整箱转发 / 输回退）。只作用于本地实例：目标不在本地返回
// NotFound + ReasonNotLocal。放置缺省在候选节点中随机（Config.Placement
// 自定义）；无候选 → Unavailable + NoEligibleNode。
//
// 阻塞到迁移裁决或预算耗尽（输因经错误链保留）；并发安全；可在实体
// Handle 内调用（对自身经串行队列等待）。
func (n *Node) Migrate(ctx context.Context, id rans.ID) error {
	if _, ok := n.mgr.GetLocal(id); !ok {
		return rans.NewError(rans.CodeNotFound, rans.ReasonNotLocal,
			"not local: "+id.String())
	}
	return lifecycle.Migrate(ctx, n.mgr, id)
}
