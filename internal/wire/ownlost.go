package wire

// MsgOwnershipLost 载荷编解码（H13 决策 22）：body = {1: epoch, 2: inc}
//（被夺走旧归属的两半；node 即收端）。计数终结、未知字段跳过。

import (
	"fmt"

	"github.com/cyinx/rans_golang/codec"
)

// OwnershipLost 是 MsgOwnershipLost 的载荷（被夺走的旧归属）。
type OwnershipLost struct {
	Epoch       uint64
	Incarnation uint64
}

// EncodeOwnershipLost 编码载荷（纯写路径不返回错误——codec.Writer 语义）。
func EncodeOwnershipLost(v OwnershipLost) []byte {
	w := codec.NewWriter()
	w.BeginObject(2)
	w.WriteTag(1, codec.WireVarInt)
	w.WriteVarUInt(v.Epoch)
	w.WriteTag(2, codec.WireVarInt)
	w.WriteVarUInt(v.Incarnation)
	return w.Bytes()
}

// RedirectHint 是 MsgRedirectHint 的载荷：{1:node, 2:epoch}（正确归属）。
type RedirectHint struct {
	Node  uint16
	Epoch uint64
}

// EncodeRedirectHint 编码重定向提示载荷。
func EncodeRedirectHint(v RedirectHint) []byte {
	w := codec.NewWriter()
	w.BeginObject(2)
	w.WriteTag(1, codec.WireVarInt)
	w.WriteVarUInt(uint64(v.Node))
	w.WriteTag(2, codec.WireVarInt)
	w.WriteVarUInt(v.Epoch)
	return w.Bytes()
}

// DecodeRedirectHint 解码重定向提示；畸形输入报错（收端丢弃）。
func DecodeRedirectHint(b []byte) (RedirectHint, error) {
	var out RedirectHint
	r := codec.NewReader(b)
	count, err := r.BeginObject()
	if err != nil {
		return out, err
	}
	if count < 0 {
		return out, fmt.Errorf("rans: wire: redirect hint: null object")
	}
	for i := 0; i < count; i++ {
		id, wt, err := r.ReadFieldTag()
		if err != nil {
			return out, err
		}
		switch {
		case id == 1 && wt == codec.WireVarInt:
			v, err := r.ReadVarUInt()
			if err != nil {
				return out, err
			}
			out.Node = uint16(v)
		case id == 2 && wt == codec.WireVarInt:
			v, err := r.ReadVarUInt()
			if err != nil {
				return out, err
			}
			out.Epoch = v
		default:
			if err := r.SkipField(wt); err != nil {
				return out, err
			}
		}
	}
	return out, nil
}

// DecodeOwnershipLost 解码载荷；畸形输入报错（收端丢弃通知）。
func DecodeOwnershipLost(b []byte) (OwnershipLost, error) {
	var out OwnershipLost
	r := codec.NewReader(b)
	count, err := r.BeginObject()
	if err != nil {
		return out, err
	}
	if count < 0 {
		return out, fmt.Errorf("rans: wire: ownership-lost payload: null object")
	}
	for i := 0; i < count; i++ {
		id, wt, err := r.ReadFieldTag()
		if err != nil {
			return out, err
		}
		switch {
		case id == 1 && wt == codec.WireVarInt:
			v, err := r.ReadVarUInt()
			if err != nil {
				return out, err
			}
			out.Epoch = v
		case id == 2 && wt == codec.WireVarInt:
			v, err := r.ReadVarUInt()
			if err != nil {
				return out, err
			}
			out.Incarnation = v
		default:
			if err := r.SkipField(wt); err != nil {
				return out, err
			}
		}
	}
	return out, nil
}
