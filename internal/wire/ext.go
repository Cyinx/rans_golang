package wire

import (
	"github.com/cyinx/rans_golang/codec"
	"github.com/cyinx/rans_golang/internal/ctxkey"
)

// 头部扩展块请求字段（H10 决策 5 / H12 决策 14）：FlagHasExtensions 的
// field-tag 对象内的字段分配，字段号即线契约、只追加不复用；未知字段号
// 读端按线型跳过（DecodeHeader 的整块 walk 语义复用）。
const (
	// ExtFieldBudgetMS 预算字段（varint，毫秒）：发送时刻剩余预算的相对值
	//——相对值防两端时钟偏差，收端以「收到时刻 + budget」重锚 deadline。
	ExtFieldBudgetMS = 1
	// ExtFieldTraceID 根 trace 标识（string）：与 causal.root 同源双载
	//（H12 决策 14 裁定——同一生成源；本字段供通用观测面，causal.root 为
	// 环判定语义面），Call 出站随因果路径一并书写。
	ExtFieldTraceID = 2
	// ExtFieldCausal 因果路径对象（H12 决策 14）：{1=root string,
	// 2=重复 varint 实体身份哈希}；仅 Call（等待边）携带，Tell 不写。
	ExtFieldCausal = 3
	// ExtFieldCtx 登记键值对象（H10 决策 5/8）：重复项字段——每个在场项
	// 是 {1=name, 2=value} 的嵌套对象，重复即多键（登记序逐项书写）。
	ExtFieldCtx = 4
	// ExtFieldSrcIncarnation 随迁包源纪元（varint）：源端落库快照时的围栏
	// incarnation，收端恢复时与 Store 高水位比较、包旧弃包；缺位（旧端
	// 发送方）按无版本处理，恢复退回包优先历史行为。
	ExtFieldSrcIncarnation = 5
)

// causal 对象的嵌套字段号：1 = 根 trace（string），2 = 单跳哈希（varint，重复即路径序列）。
const (
	ExtCausalRoot = 1
	ExtCausalHop  = 2
)

// MaxCausalDepth 因果路径深度上限（附录 A：CausalPathLimit=64，H12 决策
// 14）：出站追加达上限后降级直通不再追加（深链真环不判的接受性降级），
// 告警计数归出站挂点。
const MaxCausalDepth = 64

// ctx 项的嵌套对象字段号：1 = 登记名（string），2 = 编码值（bytes）。
const (
	ExtCtxItemName  = 1
	ExtCtxItemValue = 2
)

// CtxItem 扩展块 ctx 字段的单键值项（Name 为登记名，Value 为编码值）。
type CtxItem struct {
	Name  string
	Value []byte
}

// RequestExt 扩展块请求字段的类型化读写视图：Has* 为 false 的字段不写
// （省略即不在场的过线语义）；解码端对未知字段号/未知线型一律跳过。
type RequestExt struct {
	BudgetMS   uint64 // 预算毫秒数（线值原样，毫秒→时长的换算归消费方）
	HasBudget  bool
	TraceID    string             // 根 trace（与 Causal.Root 同源双载）
	HasTraceID bool               //
	Causal     *ctxkey.CausalPath // 因果路径（nil = 字段不写——Tell 形态）
	Ctx        []CtxItem          // 登记键值项（登记序；nil = 字段不写）
	// SrcIncarnation 随迁包源纪元（HasSrcIncarnation 为 false 的字段不写）：
	// 源端落库快照时的围栏 incarnation，收端恢复时与 Store per-id 高水位
	// 比较（包旧弃包落 Store 梯级）。
	SrcIncarnation    uint64
	HasSrcIncarnation bool
}

// EncodeRequestExt 编码扩展块对象体（field-tag，计数终结）：无任何在场
// 字段 → nil（调用方不置扩展块，头部不置 FlagHasExtensions）。
func EncodeRequestExt(e RequestExt) []byte {
	fields := 0
	if e.HasBudget {
		fields++
	}
	if e.HasTraceID {
		fields++
	}
	if e.Causal != nil {
		fields++
	}
	if e.HasSrcIncarnation {
		fields++
	}
	fields += len(e.Ctx)
	if fields == 0 {
		return nil
	}
	w := codec.NewWriter()
	w.BeginObject(fields)
	if e.HasBudget {
		w.WriteTag(ExtFieldBudgetMS, codec.WireVarInt) // 非负毫秒走裸 varuint（errobject Code/Reason 同款；ZigZag 是带符号通道）
		w.WriteVarUInt(e.BudgetMS)
	}
	if e.HasTraceID {
		w.WriteFieldString(ExtFieldTraceID, e.TraceID)
	}
	if e.Causal != nil {
		w.WriteTag(ExtFieldCausal, codec.WireMessage)
		w.BeginObject(1 + len(e.Causal.Path)) // root 一项 + 每跳一项（重复字段逐项书写）
		w.WriteFieldString(ExtCausalRoot, e.Causal.Root)
		for _, hop := range e.Causal.Path {
			w.WriteTag(ExtCausalHop, codec.WireVarInt)
			w.WriteVarUInt(hop)
		}
	}
	if e.HasSrcIncarnation {
		w.WriteTag(ExtFieldSrcIncarnation, codec.WireVarInt) // 非负纪元走裸 varuint
		w.WriteVarUInt(e.SrcIncarnation)
	}
	for _, it := range e.Ctx {
		w.WriteTag(ExtFieldCtx, codec.WireMessage)
		w.BeginObject(2)
		w.WriteFieldString(ExtCtxItemName, it.Name)
		w.WriteFieldBytes(ExtCtxItemValue, it.Value)
	}
	return w.Bytes()
}

// DecodeRequestExt 解码扩展块请求字段：未知字段号/线型不符一律按线型跳过
// （同 decodeCausal 演进语义，缺字段取零值）；结构性畸形（截断/容器非法）报错。
func DecodeRequestExt(ext []byte) (RequestExt, error) {
	var out RequestExt
	if len(ext) == 0 {
		return out, nil
	}
	r := codec.NewReader(ext)
	count, err := r.BeginObject()
	if err != nil {
		return out, err
	}
	if count < 0 {
		return out, codec.ErrMalformed
	}
	for i := 0; i < count; i++ {
		id, wt, err := r.ReadFieldTag()
		if err != nil {
			return out, err
		}
		switch {
		case id == ExtFieldBudgetMS && wt == codec.WireVarInt:
			if out.BudgetMS, err = r.ReadVarUInt(); err != nil {
				return out, err
			}
			out.HasBudget = true
		case id == ExtFieldTraceID && wt == codec.WireString:
			if out.TraceID, err = r.ReadString(); err != nil {
				return out, err
			}
			out.HasTraceID = true
		case id == ExtFieldCausal && wt == codec.WireMessage:
			if out.Causal, err = decodeCausalObject(r); err != nil {
				return out, err
			}
		case id == ExtFieldCtx && wt == codec.WireMessage:
			it, err := decodeCtxItem(r)
			if err != nil {
				return out, err
			}
			if it != nil {
				out.Ctx = append(out.Ctx, *it)
			}
		case id == ExtFieldSrcIncarnation && wt == codec.WireVarInt:
			if out.SrcIncarnation, err = r.ReadVarUInt(); err != nil {
				return out, err
			}
			out.HasSrcIncarnation = true
		default:
			if err := r.SkipField(wt); err != nil {
				return out, err
			}
		}
	}
	return out, nil
}

// decodeCausalObject 读 causal 对象（{1=root string, 2=重复 varint 哈希}，
// H12 决策 14）：重复字段逐项追加为路径序列（途经序）；未知子字段按线型
// 跳过、缺 root 取零值（与 ctx 项同甄别口径）。容量校验先于分配（陷阱
// 10）：每字段至少 tag+值 2 字节，项数 ≤ 剩余字节的一半为必要界。
func decodeCausalObject(r *codec.Reader) (*ctxkey.CausalPath, error) {
	count, err := r.BeginObject()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, codec.ErrMalformed
	}
	if count > r.Remaining()/2 {
		return nil, codec.ErrTruncated
	}
	p := &ctxkey.CausalPath{}
	for i := 0; i < count; i++ {
		id, wt, err := r.ReadFieldTag()
		if err != nil {
			return nil, err
		}
		switch {
		case id == ExtCausalRoot && wt == codec.WireString:
			if p.Root, err = r.ReadString(); err != nil {
				return nil, err
			}
		case id == ExtCausalHop && wt == codec.WireVarInt:
			var hop uint64
			if hop, err = r.ReadVarUInt(); err != nil {
				return nil, err
			}
			p.Path = append(p.Path, hop)
		default:
			if err := r.SkipField(wt); err != nil {
				return nil, err
			}
		}
	}
	return p, nil
}

// decodeCtxItem 读 ctx 项嵌套对象（{1=name, 2=value}）：未知子字段按线型
// 跳过；缺 name 或缺 value 的语义不完整项返回 nil 项（结构性畸形仍报错，
// 项级残缺不升级为请求失败——重复项字段允许读端逐项甄别）。
func decodeCtxItem(r *codec.Reader) (*CtxItem, error) {
	count, err := r.BeginObject()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, codec.ErrMalformed
	}
	var it CtxItem
	for i := 0; i < count; i++ {
		id, wt, err := r.ReadFieldTag()
		if err != nil {
			return nil, err
		}
		switch {
		case id == ExtCtxItemName && wt == codec.WireString:
			if it.Name, err = r.ReadString(); err != nil {
				return nil, err
			}
		case id == ExtCtxItemValue && wt == codec.WireBytes:
			if it.Value, err = r.ReadBytes(); err != nil {
				return nil, err
			}
		default:
			if err := r.SkipField(wt); err != nil {
				return nil, err
			}
		}
	}
	if it.Name == "" || it.Value == nil {
		return nil, nil
	}
	return &it, nil
}
