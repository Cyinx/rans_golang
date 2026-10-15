// Package directory 定义集群身份唯一真相的抽象（移植规格 §11）。
package directory

import (
	"context"

	"github.com/cyinx/rans_golang"
)

// Owner 是身份归属的实例级三元组：Node 归属节点、Epoch 该节点的成员代际
// （NodeRegistry.NextNodeEpoch 取号的单调值——同节点号重启代际严格递增，
// 旧条目据此自然失效）、Incarnation 该归属的激活纪元（Register/Transfer
// 原子域内取号）。实例级相等（三元全等）是注销/换锁的归属校验判据——
// 同 node 的新实例或换代节点都不被旧实例的迟到操作误伤。
type Owner struct {
	Node        rans.NodeID
	Epoch       uint64
	Incarnation uint64
}

// Directory 是身份目录的合并端口（spec §5.2）：注册/注销/查找/换锁是身份
// 定位的最小完备面。归属判据是实例粒度 Owner 三元组——同 node 的新实例、
// 同 node 重启后的新代际都不被旧实例的迟到注销误伤（INV3：纪元序 = 所有权
// 序）。
//
// 条目值 v3（键值 v3 重整，主键逐实体 hash tag）：`{node, epoch, inc}`，
// **主键永不清删**——注销写无主墓碑（node/epoch 置零、inc 保留），纪元
// 单调性跨墓碑存续（重注册从墓碑 inc+1 起，被强弃纪元的残留写入恒被
// Store 围栏拒绝）；条目有效性由调用方按成员代际判定（条目有效 ⟺
// owner(node, epoch) 在本端成员视图存活），崩溃残留无 TTL 躺置无害。
//
// 并发约定（实现面共同契约，方法注释不再重复）：全部方法阻塞至目录 IO
// 完成或 ctx 结束（耗尽返回包装 ctx.Err() 的错误）；实现须并发安全；可
// 在实体 Handle 内调用（调用方自备预算——运行时内部调用均带预算）。
type Directory interface {
	// Register 原子注册（单键读-比-写，H13 四分支语义）。self 是本实例
	// 归属（Node/Epoch 参与比对与写入；Incarnation 作纪元下限——赢分支
	// 取号 = max(现值, 下限)+1，0 = 无下限，裁决不受影响）。expected
	// 非空时为 CAS 接管的期望归属（与现值全等才赢）。返回值 current 恒为
	// 「本调用后的条目归属」：
	//   - 赢（true）：条目此前无值/墓碑（纪元 = 墓碑残值+1 起写自归属）、
	//     或现值归属即本实例 node+epoch（重注册/收养，inc+1）、或 expected
	//     与现值全等（CAS 接管，inc+1 写自归属）——current 是含新纪元的
	//     自归属，调用方必须采信此值做围栏/续租簿记；
	//   - 输（false）：他实例持有且不可接管——current 是现持有者归属
	//     （转发/提示用），条目原样。
	// 纪元性质：per-id 严格单调递增、并发取号必不重复、单调性跨墓碑存续
	// （注销不清序列）；0 保留给「未围栏」哨兵。
	Register(ctx context.Context, id rans.ID, self Owner, expected *Owner) (won bool, current Owner, err error)

	// UnregisterIfOwner 实例级条件注销：仅当条目归属恰为 owner（三元全
	// 等）时改写无主墓碑（inc 保留）；不符（新实例已接管/换代/已墓碑）
	// → false，不误删。墓碑后 Lookup miss、Register 按分支 1 收养。
	UnregisterIfOwner(ctx context.Context, id rans.ID, owner Owner) (bool, error)

	// Lookup 读取归属位置（实例级 Owner 三元组）。墓碑/无值/值解析失败
	// → miss（墓碑 = 无主，对路由不可见；解析失败视同 miss：注册可覆写）。
	Lookup(ctx context.Context, id rans.ID) (Owner, bool, error)

	// Transfer 实例级换锁（迁移编排的所有权让渡，规格 §13.2）：仅当条目
	// 归属恰为 expected（三元全等）时，原子写目标归属（Node/Epoch，入参
	// Incarnation 被忽略）并取新纪元（现值+1，与 Register 共用 per-id 序
	// 列）。返回值：
	//	(false, 0, Owner{}, nil)           条目不存在
	//	(false, 0, current, ErrNotOwner)   归属不符（含墓碑——无主条目不可让渡），条目原样
	//	(true, newInc, 新归属, nil)        成功：current 为含新纪元的目标归属
	Transfer(ctx context.Context, id rans.ID, expected Owner, target Owner) (won bool, newInc uint64, current Owner, err error)
}
