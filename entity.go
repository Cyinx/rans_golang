package rans

import "context"

// Entity 是宿主实体契约：三个生命周期钩子。Activate 在注册成功之后调用，
// Deactivate 只与成功的 Activate 配对；Handle 处理一次投递。
// 调用约定（spec §5.7/§5.8）：三钩子恒在实体自身的串行 loop goroutine 上
// 执行（同一实体的钩子永不并发——实体无须加锁）；ctx 携带预算与取消、
// Self/Incarnation/ClientFrom 自省面；阻塞钩子受调用方预算约束（卡死由
// 看门狗强弃兜底）。
type Entity interface {
	Activate(ctx context.Context) error
	Handle(ctx context.Context, msg any) (any, error)
	Deactivate(ctx context.Context) error
}

// Factory 按身份构造实体。必须无副作用：工厂可能在激活竞争中被调用后丢弃。
type Factory func(id ID) Entity

// Snapshotter 是实体的可选迁移快照能力面：实现即启用快照面——配置了
// checkpoint.Store 时参与 checkpoint、钝化与停机终快照；未配置 Store 时
// 只用于随迁包，且有状态实体的迁移与钝化会被拒绝。
//
// 并发契约：Snapshot 恒在实体 loop goroutine（串行静默点）调用，实体无须
// 自行加锁；Restore 在激活闭包（同 loop 语境）调用，形状不匹配必须返回
// error（不得 panic 或静默截断）——上层据此放弃随迁包、退化 Store 兜底。
type Snapshotter interface {
	// Snapshot 捕获实体当前状态的序列化快照。
	Snapshot() ([]byte, error)
	// Restore 从快照恢复实体状态；形状不匹配必须返回 error。
	Restore(b []byte) error
}
