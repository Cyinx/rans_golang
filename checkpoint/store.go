// Package checkpoint 提供实体快照存取契约（规格 §15 持久化）。
package checkpoint

import (
	"context"
	"errors"

	"github.com/cyinx/rans_golang"
)

// ErrStaleIncarnation 围栏拒写：Put 携带的 incarnation 纪元
// 小于该 id 已接受的最大纪元——被强弃/逐出的旧 incarnation 残留在飞写入。
// 实现必须以本哨兵（或其包装）返回，供调用方按「围栏生效」留痕，与 IO
// 错误区分。
var ErrStaleIncarnation = errors.New("rans: checkpoint put: stale incarnation")

// Store 是快照存取契约（规格 §15 IPawniteEntityStore 同构）：迁移落快照
// 与激活 loader 兜底读写都经由本接口。MySQL 等持久化实现归消费方——本仓库
// 只定接口；InMemory 为测试/单机形态。
//
// 并发约定（实现面共同契约，spec §5.8）：实现须并发安全；方法可阻塞于
// IO 至 ctx 结束（耗尽返回包装 ctx.Err() 的错误——纯内存实现不阻塞）；
// 运行时在激活/交接/停机路径调用（自带预算），实体 Handle 内不直接消费
// 本面。
type Store interface {
	// Get 读取快照。未命中返回 (nil, false, nil)，不视为错误。
	Get(ctx context.Context, id rans.ID) ([]byte, bool, error)
	// Put 写入快照，携带 incarnation 纪元（fencing 令牌）。
	// 实现必须维护 per-id 已接受的最大纪元并拒绝回退写入：
	//   - incarnation < 已存最大值 → 拒绝，返回 ErrStaleIncarnation；
	//   - incarnation == 已存最大值 → 同 incarnation 的 checkpoint 重试，
	//     允许覆盖；
	//   - incarnation == 0 → 未围栏直通（目录无纪元源的裸形态），
	//     不参与比较。
	// 实现应防御性拷贝入参（见 InMemory）。
	Put(ctx context.Context, id rans.ID, incarnation uint64, snapshot []byte) error
	// Delete 删除快照。未命中为 no-op（幂等），不返回错误。
	Delete(ctx context.Context, id rans.ID) error
}
