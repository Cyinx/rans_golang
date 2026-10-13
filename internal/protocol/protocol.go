// Package protocol 承载线上数值契约：MessageType 与 MessageFlag（Code 在 rans）。
package protocol

// MessageType 线协议消息类型，数值即契约（移植规格 §7.2/附录 B）。
type MessageType uint8

const (
	MsgRequest          MessageType = 1
	MsgResponse         MessageType = 2
	MsgOneWay           MessageType = 3
	MsgError            MessageType = 4
	MsgMigrationPackage MessageType = 9  // 实体随迁包
	MsgPing             MessageType = 10 // 控制面心跳探测
	MsgPong             MessageType = 11
	MsgHandshake        MessageType = 12
	MsgHandshakeAck     MessageType = 13
	// MsgRetire 连接退役通知（本仓裁定，非规格帧）：优雅退役方在半关前发出，
	// 告知对端「本端因同节点收敛已有替代连接」——对端同 socket 副本的死亡按
	// 抑制处理（不误报节点断连、不触发上层 FailPendingForNode 杀在途请求）。
	MsgRetire MessageType = 14
	// MsgCancel 尽力取消通知（H10 决策 9，本仓裁定）：调用方 ctx 因主动取消
	// 收场时由本端网关向 pending 目标节点发出的单向控制消息。头部
	// MessageID+SourceNodeID 即定位键，无载荷无回包；幂等、迟到无害。
	MsgCancel MessageType = 15
	// MsgOwnershipLost 所有权丢失通知（H13 决策 22）：接管赢方直发旧
	// owner 节点的止损控制消息（走控制帧优先通道）——旧节点对归属全等
	// 的实体关准入+销毁（迟到/重复幂等）。TargetUUID=实体，body={epoch,inc}。
	MsgOwnershipLost MessageType = 16
	// MsgRedirectHint 重定向提示（H13 决策 24）：非 owner 节点转发请求的
	// 同时直发源端的缓存加速帧——源端立即改写位置缓存（不等正 TTL），
	// 不参与 pending 裁决（区别于 Error 帧）。TargetUUID=实体；
	// body={1:node,2:epoch}（复用 OwnershipLost 载荷形态）。
	MsgRedirectHint MessageType = 17
)

// MessageFlag 消息头条件编码位（移植规格 §7.2 Flags）。位分配为本仓裁定：
// 规格未定义数值，Go-to-Go 自洽即可；跨语言互通时以此注册表为准。
type MessageFlag uint32

const (
	FlagHasStatus       MessageFlag = 1 << iota // HasStatus(1B)
	FlagHasMessagerHash                         // HasMessagerHash(4B)
	FlagHasMethodHash                           // HasMethodHash(4B)
	FlagHasSourceNodeID                         // HasSourceNodeId(2B)
	FlagHasTargetUUID                           // HasTargetUUID(kind str + inst str)
	// FlagHasCausalContext 已退役（H12 因果路径 v2 移扩展块字段 3）：位值是
	// 线契约、只退役不复用；编码端不再置位，解码端不消费该位。
	FlagHasCausalContext
	FlagForwarded // 输家转发单跳防环标记（规格 §12）

	// FlagHasExtensions 头部扩展块（规格 §7.2/§6.1 H9）：置位时
	// 头部尾部追加一个 field-tag 对象（Header.Ext）。显式 1<<7 而非延续 iota：
	// 位分配即线契约，H10/H12 的 budget_ms/trace_id/causal 等新头部字段一律
	// 进扩展块——旧读端对整块原字节跳过，头部演进不依赖版本升级。
	FlagHasExtensions MessageFlag = 1 << 7
)

// IsControlPlane：握手完成前仅允许控制面消息（移植规格 §10.2 pre-auth 门禁）。
func IsControlPlane(t MessageType) bool {
	switch t {
	case MsgHandshake, MsgHandshakeAck, MsgPing, MsgPong, MsgRetire, MsgOwnershipLost:
		return true
	}
	return false
}
