package codec

import "fmt"

// MaxFieldID fieldId 上限：fieldId<<4 后仍在 varint 舒适区（4 字节内）。
// 规格未限制 fieldId 域，此为本仓裁定；读侧对越界 tag 报 ErrMalformed。
const MaxFieldID = 1<<24 - 1

// FieldTag 构造字段 tag：varint(fieldId<<4 | wireType)（规格 §7.3）。
// fieldID 是否在 MaxFieldID 内由调用方保证；越界产出的 tag 读侧会被拒绝。
func FieldTag(fieldID uint32, wt WireType) uint64 {
	return uint64(fieldID)<<4 | uint64(wt&0x0F)
}

// BeginObject 写对象头：varint(字段数)。计数终结、无长度前缀（规格 §7.3，
// 移植不要改成 protobuf 式 length-delim——那要求嵌套对象先序列化到临时缓冲）。
func (w *Writer) BeginObject(fieldCount int) { w.WriteVarInt(int64(fieldCount)) }

// WriteTag 写裸字段 tag（无值）：序列等复合形态的转义口——值由调用方
// 按线型续写（普通字段一律走 WriteField* 原语）。
func (w *Writer) WriteTag(fieldID uint32, wt WireType) { w.WriteVarUInt(FieldTag(fieldID, wt)) }

// WriteFieldVarInt 写 varint 字段：tag + ZigZag varint 值。
func (w *Writer) WriteFieldVarInt(id uint32, v int64) {
	w.WriteTag(id, WireVarInt)
	w.WriteVarInt(v)
}

// WriteFieldFixed64 写 fixed64 字段：tag + 8 字节小端值。
func (w *Writer) WriteFieldFixed64(id uint32, v uint64) {
	w.WriteTag(id, WireFixed64)
	w.WriteFixed64(v)
}

// WriteFieldString 写 string 字段：tag + 长度域 + 原始字节。
func (w *Writer) WriteFieldString(id uint32, s string) {
	w.WriteTag(id, WireString)
	w.WriteString(s)
}

// WriteFieldBytes 写 bytes 字段：tag + 长度域 + 原始字节。
func (w *Writer) WriteFieldBytes(id uint32, b []byte) {
	w.WriteTag(id, WireBytes)
	w.WriteBytes(b)
}

// WriteFieldRaw 写任意线型字段：payload 必须已是该 wireType 的完整值编码
// （嵌套 Message 时 payload 为完整对象字节——自身以字段数开头，计数终结，
// 无长度前缀）。本层未提供类型化写方法的线型（fixed8/16/32、nullable、
// 集合类）经此入口写入。
func (w *Writer) WriteFieldRaw(id uint32, wt WireType, payload []byte) {
	w.WriteTag(id, wt)
	w.buf = append(w.buf, payload...)
}

// BeginObject 读对象头字段数。count<0 为 null 对象语义，统一归一为 -1
// （规格 §7.3：集合计数 <0 = null）。
func (r *Reader) BeginObject() (int, error) {
	v, err := r.ReadVarInt()
	if err != nil {
		return 0, err
	}
	if v < 0 {
		return -1, nil
	}
	return int(v), nil
}

// ReadFieldTag 读字段 tag，拆出 fieldId 与 4bit 线型。
// fieldId 超出 MaxFieldID 视为畸形（溢出防御，不可静默截断）。
func (r *Reader) ReadFieldTag() (uint32, WireType, error) {
	v, err := r.ReadVarUInt()
	if err != nil {
		return 0, 0, err
	}
	if v>>4 > MaxFieldID {
		return 0, 0, ErrMalformed
	}
	return uint32(v >> 4), WireType(v & 0x0F), nil
}

// MaxSkipFieldDepth SkipField 递归深度上限：与 MaxCausalDepth 同族的资源
// 界口径（附录 A CausalPathLimit=64）——每层嵌套最小编码 2 字节，无上界的
// 递归跳过会让小体积帧放大成深递归栈（pre-auth 头 64KiB ≈ 3 万层；post-auth
// 帧 4MiB 可达百万层）。合法 schema 的实际嵌套深度远低于
// 此（生成器产出的消息形状是编译期常量）。
const MaxSkipFieldDepth = 64

// SkipField 按线型自描述跳过字段值（规格 §7.3 伪码逐分支对照）。
// 集合类线型内部自描述，无需外部类型即可递归跳过；全程只推进偏移不分配。
// 集合/消息线型的递归深度受 MaxSkipFieldDepth 封顶——超深即 ErrMalformed
// （畸形：合法形状到不了这个深度）。公开入口每次调用从 0 起算深度（生成
// 物对未知字段的跳过是各自独立的顶层入口，编译期形状已约束其余路径）。
func (r *Reader) SkipField(wt WireType) error {
	return r.skipField(wt, 0)
}

// skipField 携带递归深度的内部形态（depth = 当前层号，顶层 0）。
func (r *Reader) skipField(wt WireType, depth int) error {
	if depth > MaxSkipFieldDepth {
		return fmt.Errorf("%w (skip nesting deeper than %d levels)", ErrMalformed, MaxSkipFieldDepth)
	}
	switch wt {
	case WireVarInt:
		_, err := r.ReadVarInt()
		return err
	case WireFixed8:
		_, err := r.ReadFixed8()
		return err
	case WireFixed16:
		_, err := r.ReadFixed16()
		return err
	case WireFixed32:
		_, err := r.ReadFixed32()
		return err
	case WireFixed64:
		_, err := r.ReadFixed64()
		return err
	case WireString:
		_, err := r.ReadString()
		return err
	case WireBytes:
		_, err := r.ReadBytes()
		return err
	case WireNullableFixed8: // flag(1B)：0=null 无载荷；非 0 后跟 Fixed8 值
		flag, err := r.ReadFixed8()
		if err != nil {
			return err
		}
		if flag == 0 {
			return nil
		}
		_, err = r.ReadFixed8()
		return err
	case WireNullableFixed32:
		flag, err := r.ReadFixed8()
		if err != nil {
			return err
		}
		if flag == 0 {
			return nil
		}
		_, err = r.ReadFixed32()
		return err
	case WireNullableFixed64:
		flag, err := r.ReadFixed8()
		if err != nil {
			return err
		}
		if flag == 0 {
			return nil
		}
		_, err = r.ReadFixed64()
		return err
	case WireMessage: // 嵌套对象计数终结；count<0 为 null 对象，循环自然跳过
		count, err := r.BeginObject()
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			_, ewt, err := r.ReadFieldTag()
			if err != nil {
				return err
			}
			if err := r.skipField(ewt, depth+1); err != nil {
				return err
			}
		}
		return nil
	case WireNullableMessage: // flag(1B)：0=null；非 0 后跟完整对象
		flag, err := r.ReadFixed8()
		if err != nil {
			return err
		}
		if flag == 0 {
			return nil
		}
		return r.skipField(WireMessage, depth+1)
	case WireSequence: // varint(elemWire) + varint(count) + 元素
		elemWire, err := r.ReadVarUInt()
		if err != nil {
			return err
		}
		count, err := r.BeginObject()
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			if err := r.skipField(WireType(elemWire), depth+1); err != nil {
				return err
			}
		}
		return nil
	case WireDictionary: // varint(keyWire) + varint(valWire) + varint(count) + 键值对
		keyWire, err := r.ReadVarUInt()
		if err != nil {
			return err
		}
		valWire, err := r.ReadVarUInt()
		if err != nil {
			return err
		}
		count, err := r.BeginObject()
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			if err := r.skipField(WireType(keyWire), depth+1); err != nil {
				return err
			}
			if err := r.skipField(WireType(valWire), depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return ErrMalformed // 4bit 线型域 14/15 未定义
}
