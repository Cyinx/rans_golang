// Package codec 是生成代码与手写序列化器依赖的编码面：值注册表
// （Register/Encode/Decode）、线编码原语（Writer/Reader 与 field-tag 对象）
// 与形状登记摘要（RegisterShapes/SchemaDigest）。帧、头部、握手等协议
// 结构在 internal/wire。
package codec

import "errors"

// 解码错误哨兵族：ErrTruncated 输入提前耗尽（长度域声明超出缓冲剩余量）；
// ErrMalformed 形态非法（varint 溢出、fieldId 越界、未定义线型、负长度域等）。
var (
	ErrTruncated = errors.New("rans: decode: truncated input")
	ErrMalformed = errors.New("rans: decode: malformed input")
)

// WireType 4bit 线型（规格 §7.3，共 14 种）。数值为本仓裁定：
// 规格未给数值，Go-to-Go 自洽；VarInt 为规格列举 13 种外的第 14 种
// （紧凑整数直接 varint 存储）。
type WireType uint8

const (
	WireVarInt          WireType = iota // 0
	WireFixed8                          // 1
	WireFixed16                         // 2
	WireFixed32                         // 3
	WireFixed64                         // 4
	WireString                          // 5
	WireBytes                           // 6
	WireNullableFixed8                  // 7
	WireNullableFixed32                 // 8
	WireNullableFixed64                 // 9
	WireMessage                         // 10 嵌套对象计数终结
	WireNullableMessage                 // 11
	WireSequence                        // 12
	WireDictionary                      // 13
)
