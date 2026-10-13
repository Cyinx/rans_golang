package rans

// Code 是调用状态码，数值即契约（移植规格附录 B）；同时充当自动重试的
// 分类维度（重试分类表见约定 spec §5.3）。
type Code uint8

const (
	CodeOK            Code = 0
	CodeError         Code = 1
	CodeNotFound      Code = 2
	CodeTimeout       Code = 3
	CodeUnavailable   Code = 4
	CodeSerialization Code = 5
	CodeDecode        Code = 6

	// 后两个只在本地产生，不过线。
	CodeCanceled       Code = 64 // 调用随 ctx 结束/停机被取消
	CodeConnectionLost Code = 65 // 连接丢失，应答是否送达未知（仅幂等调用自动重试）
)
