package wire

// Error 帧体对象（规格 §5.3「线上半边」）：field-tag 对象——
// Code/Reason/AppCode/AppData 是线协议字段，
// 本地与远端的统一错误模型全维过线。与旧文本帧体之间无兼容读取
// （同集群双端同版本）。
//
// 线格式（field-tag 对象，计数终结、无长度前缀——演进语义同包内 Envelope）：
//
//	field 1 = Code   （varint，无符号；rans.Code）
//	field 2 = Reason （varint，无符号；rans.Reason）
//	field 3 = Message （string）
//	field 4 = AppCode （varint，ZigZag 有符号；int64 可负）
//	field 5 = AppData （bytes；不透明载荷 ≤ 64KiB）
//
// 演进承诺：新字段只追加字段号，读端对未知字段号按线型跳过、缺失字段取
// 零值——消费方/未来版本加字段互不破坏。

import (
	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
)

// MarshalError 编码统一错误为 Error 帧 body。Node 不编码：节点身份由帧头
// SourceNodeID 承载（body 对端归属无关，Node 是接收方按到达帧回填的端语
// 义）。Message/AppCode/AppData 为零值时不写字段（计数终结按实际字段数）；
// 解码侧缺失字段取零值，两侧对称。nil 错误按零值对象编码（Code=0 即
// CodeOK，防御形态，调用方不应依赖）。纯写路径不返回错误（codec.Writer
// 语义）。
func MarshalError(e *rans.Error) []byte {
	if e == nil {
		e = &rans.Error{}
	}
	n := 2 // Code、Reason 恒写（错误帧的最小语义面）
	if e.Message != "" {
		n++
	}
	if e.AppCode != 0 {
		n++
	}
	if len(e.AppData) > 0 {
		n++
	}
	if e.OwnerNode != 0 {
		n++
	}
	w := codec.NewWriter()
	w.BeginObject(n)
	w.WriteTag(1, codec.WireVarInt)
	w.WriteVarUInt(uint64(e.Code))
	w.WriteTag(2, codec.WireVarInt)
	w.WriteVarUInt(uint64(e.Reason))
	if e.Message != "" {
		w.WriteFieldString(3, e.Message)
	}
	if e.AppCode != 0 {
		w.WriteFieldVarInt(4, e.AppCode)
	}
	if len(e.AppData) > 0 {
		w.WriteFieldBytes(5, e.AppData)
	}
	if e.OwnerNode != 0 {
		ow := codec.NewWriter()
		ow.BeginObject(2)
		ow.WriteTag(1, codec.WireVarInt)
		ow.WriteVarUInt(uint64(e.OwnerNode))
		ow.WriteTag(2, codec.WireVarInt)
		ow.WriteVarUInt(e.OwnerEpoch)
		w.WriteFieldBytes(6, ow.Bytes())
	}
	return w.Bytes()
}

// UnmarshalError 解码 Error 帧 body。校验纪律与 UnmarshalEnvelope 同族：
// count<0（null 对象）畸形——错误帧必为非空对象；声明字段数多于实际（缓冲
// 提前耗尽）由字段循环报 codec.ErrTruncated；声明字段消费完仍有残余字节视
// 为畸形（body 即整个帧体，尾随垃圾=帧错位）。未知字段号按线型跳过、已知
// 字段号携带未知线型同样跳过（不静默重解释）。AppData 拷出输入缓冲——Error
// 帧的池化页在解码后就地归还，错误值却存活到调用方手里（Message 天然拷贝：
// string 转换）。Node 恒为零值，由调用方（gateway）按帧头 SourceNodeID 回填。
func UnmarshalError(b []byte) (*rans.Error, error) {
	r := codec.NewReader(b)
	count, err := r.BeginObject()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, codec.ErrMalformed
	}
	e := &rans.Error{}
	for i := 0; i < count; i++ {
		id, wt, err := r.ReadFieldTag()
		if err != nil {
			return nil, err
		}
		switch {
		case id == 1 && wt == codec.WireVarInt:
			var v uint64
			if v, err = r.ReadVarUInt(); err != nil {
				return nil, err
			}
			e.Code = rans.Code(v)
		case id == 2 && wt == codec.WireVarInt:
			var v uint64
			if v, err = r.ReadVarUInt(); err != nil {
				return nil, err
			}
			e.Reason = rans.Reason(v)
		case id == 3 && wt == codec.WireString:
			if e.Message, err = r.ReadString(); err != nil {
				return nil, err
			}
		case id == 4 && wt == codec.WireVarInt:
			if e.AppCode, err = r.ReadVarInt(); err != nil {
				return nil, err
			}
		case id == 5 && wt == codec.WireBytes:
			var v []byte
			if v, err = r.ReadBytes(); err != nil {
				return nil, err
			}
			if len(v) > 0 {
				e.AppData = append([]byte(nil), v...)
			}
		case id == 6 && wt == codec.WireBytes:
			var v []byte
			if v, err = r.ReadBytes(); err != nil {
				return nil, err
			}
			// 重定向提示：{1:node, 2:epoch}；畸形嵌套整体跳过（提示是
			// 加速面，不值得让错误帧失败）。
			or := codec.NewReader(v)
			oc, oerr := or.BeginObject()
			if oerr != nil || oc < 0 {
				continue
			}
			for j := 0; j < oc; j++ {
				oid, owt, oerr := or.ReadFieldTag()
				if oerr != nil {
					break
				}
				switch {
				case oid == 1 && owt == codec.WireVarInt:
					if ov, oerr := or.ReadVarUInt(); oerr == nil {
						e.OwnerNode = rans.NodeID(ov)
					}
				case oid == 2 && owt == codec.WireVarInt:
					if ov, oerr := or.ReadVarUInt(); oerr == nil {
						e.OwnerEpoch = ov
					}
				default:
					if oerr := or.SkipField(owt); oerr != nil {
						break
					}
				}
			}
		default:
			if err := r.SkipField(wt); err != nil {
				return nil, err
			}
		}
	}
	if r.Remaining() != 0 {
		return nil, codec.ErrMalformed
	}
	return e, nil
}
