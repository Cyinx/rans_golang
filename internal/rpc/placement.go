package rpc

import "github.com/cyinx/rans_golang"

// Placement 是放置策略的消费接口（规格 §12 放置、§13 迁移目标选取）：
// 从存活节点中选一个目标节点，exclude 排除调用方指定的节点（通常是
// 本地——规格 §13「放置目标(排除本地)」）。无候选 → (0, false)，调用方
// 记 NoEligibleTarget 并保持 Active。
//
// membership.RandomPlacement 经鸭子类型满足本接口（membership 不导入
// placement，依赖单向向下——接口定义在消费方）；编译期锚点见
// placement_test.go。
type Placement interface {
	Pick(exclude rans.NodeID) (rans.NodeID, bool)
}

// KindPlacement 是 Placement 的 kind 感知扩展面（「放置按 kind 过滤候选」
// 的规格出处见 edge 宿主 spec §10，机制已泛化为独立于 edge 的通用面）：
// 可选能力——Placement 实现本接口时，路由
// 梯子的 miss 收口改为按目标 kind 询问（PickForKind），候选集只含声明承载
// 该 kind 的存活节点；未实现则沿用 Pick
// （全节点随机，既有行为）。经类型断言探测，仍是鸭子耦合：membership 不
// 导入 rpc，锚点见 placement_test.go。
type KindPlacement interface {
	// PickForKind 从声明承载 kind 的存活节点中选一个（排除 exclude）。
	// 无人声明该 kind → (0, false)，调用方回退本地激活/保持 Active。
	PickForKind(kind string, exclude rans.NodeID) (rans.NodeID, bool)
}
