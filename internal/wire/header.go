package wire

import (
	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
	"github.com/cyinx/rans_golang/internal/protocol"
	"sync/atomic"
)

// Header 消息头（规格 §7.2）：位置编码，Flags 驱动条件字段，不用 field-tag——
// 头部是热路径固定形状，位置即契约（同 RPC 顶层参数，规格 §7.3）。
// 编码序固定：Type(1B) → Flags(4B LE) → MessageID(8B LE) →
// HasStatus(1B) → HasMessagerHash(4B) → HasMethodHash(4B) →
// HasSourceNodeID(2B) → HasTargetUUID(kind str + inst str) →
// [扩展块（FlagHasExtensions）]。
// 未知 flag 位不解析、不读载荷：Flags 值原样保留往返，故无载荷的新位对旧
// 读端透明。头部的演进通道（规格 §6.1 H9）：FlagHasExtensions
// 置位时尾部追加一个 field-tag 对象（Ext，已编码对象体含计数字段）——位置
// 段自此冻结，此后头部的新字段（H10 budget_ms/trace_id、H12 causal 等）一律
// 进扩展块，旧读端对整块跳过，不再依赖版本升级。HasCausalContext 位已退役
// （v0.23 因果路径 v2 移扩展块字段 3）：编码端不再置位、解码端不消费——置位
// 输入无合法生产方（同版本断点），位值只退役不复用。
type Header struct {
	Type         protocol.MessageType
	Flags        protocol.MessageFlag
	MessageID    uint64
	Status       rans.Code
	MessagerHash uint32
	MethodHash   uint32
	SourceNodeID uint16
	TargetKind   string
	TargetInst   string
	// Ext 已编码扩展对象体（field-tag，计数终结；nil = 不写扩展块）。
	// 编码端原样追加在头部尾部；解码端走 BeginObject+ReadFieldTag+SkipField
	// 验证合法后原字节拷贝（内容对本层不透明——解释权归未来的消费方），
	// 余量 0（flag 置位而无载荷）→ nil。
	Ext []byte
}

// Encode 位置编码消息头。纯写路径不返回错误（codec.Writer 语义）。
func (h *Header) Encode() []byte {
	w := codec.NewWriter()
	w.WriteFixed8(uint8(h.Type))
	w.WriteFixed32(uint32(h.Flags))
	w.WriteFixed64(h.MessageID)
	if h.Flags&protocol.FlagHasStatus != 0 {
		w.WriteFixed8(uint8(h.Status))
	}
	if h.Flags&protocol.FlagHasMessagerHash != 0 {
		w.WriteFixed32(h.MessagerHash)
	}
	if h.Flags&protocol.FlagHasMethodHash != 0 {
		w.WriteFixed32(h.MethodHash)
	}
	if h.Flags&protocol.FlagHasSourceNodeID != 0 {
		w.WriteFixed16(h.SourceNodeID)
	}
	if h.Flags&protocol.FlagHasTargetUUID != 0 {
		w.WriteString(h.TargetKind)
		w.WriteString(h.TargetInst)
	}
	if h.Flags&protocol.FlagHasExtensions != 0 && h.Ext != nil {
		w.WriteRaw(h.Ext) // 已编码对象体原样追加（nil = 不写，解码端容忍）
	}
	return w.Bytes()
}

// DecodeHeader 位置解码消息头（Encode 的对称读，不带 tag）。
// 条件字段按 flag 依序读取；读毕仍有残余字节视为畸形（头部即帧内定界，
// 尾随垃圾=帧错位）。
func DecodeHeader(b []byte) (Header, error) {
	r := codec.NewReader(b)
	typ, err := r.ReadFixed8()
	if err != nil {
		return Header{}, err
	}
	flags, err := r.ReadFixed32()
	if err != nil {
		return Header{}, err
	}
	id, err := r.ReadFixed64()
	if err != nil {
		return Header{}, err
	}
	h := Header{
		Type:      protocol.MessageType(typ),
		Flags:     protocol.MessageFlag(flags),
		MessageID: id,
	}
	if h.Flags&protocol.FlagHasStatus != 0 {
		v, err := r.ReadFixed8()
		if err != nil {
			return Header{}, err
		}
		h.Status = rans.Code(v)
	}
	if h.Flags&protocol.FlagHasMessagerHash != 0 {
		if h.MessagerHash, err = r.ReadFixed32(); err != nil {
			return Header{}, err
		}
	}
	if h.Flags&protocol.FlagHasMethodHash != 0 {
		if h.MethodHash, err = r.ReadFixed32(); err != nil {
			return Header{}, err
		}
	}
	if h.Flags&protocol.FlagHasSourceNodeID != 0 {
		if h.SourceNodeID, err = r.ReadFixed16(); err != nil {
			return Header{}, err
		}
	}
	if h.Flags&protocol.FlagHasTargetUUID != 0 {
		if h.TargetKind, err = r.ReadString(); err != nil {
			return Header{}, err
		}
		if h.TargetInst, err = r.ReadString(); err != nil {
			return Header{}, err
		}
	}
	// 扩展块：flag 置位且有余量 → walk 验证合法（未知字段按
	// 线型跳过）并原字节拷贝；余量 0 → nil（flag 置位而无载荷的容忍形态）。
	// flag 未置位仍有尾随字节 → 畸形（帧错位防线不变）。
	//（HasCausalContext 位已退役：不再消费，置位输入无合法生产方。）
	if h.Flags&protocol.FlagHasExtensions != 0 && r.Remaining() > 0 {
		extStart := len(b) - r.Remaining()
		count, err := r.BeginObject()
		if err != nil {
			return Header{}, err
		}
		if count < 0 {
			return Header{}, codec.ErrMalformed // 扩展块必为非空容器语义
		}
		for i := 0; i < count; i++ {
			_, wt, err := r.ReadFieldTag()
			if err != nil {
				return Header{}, err
			}
			if err := r.SkipField(wt); err != nil {
				return Header{}, err
			}
		}
		h.Ext = append([]byte(nil), b[extStart:len(b)-r.Remaining()]...) // 拷贝脱离输入缓冲
	}
	if r.Remaining() != 0 {
		return Header{}, codec.ErrMalformed
	}
	return h, nil
}

// IDGen 进程内消息 ID 发生器（陷阱 7）：MessageID 是请求/响应关联键，
// 必须单调不复用——回绕前复用会让 pending 表错配到旧请求。零值可用，
// 从 1 起（0 保留为"无 ID"哨兵）。
type IDGen struct {
	last atomic.Uint64
}

// Next 返回下一个 ID；atomic 单调递增，多 goroutine 安全。
func (g *IDGen) Next() uint64 {
	return g.last.Add(1)
}
