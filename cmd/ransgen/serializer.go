// serializer 生成器（规格 §6/§7.3）：把 PackageDecl 的消息发现
// 结果计划成序列化器模板数据，产出每消息一个 codec.Register 注册与
// 编码/解码助手。字段类型门卫（S004/M405 等价）在此落地：支持集外类型
// 生成失败，错误文本含消息名/字段名/类型名。
package main

import (
	"fmt"
	"sort"
	"strings"
)

// fieldKind 字段值形态（决定线型与读写语句）。
type fieldKind int

const (
	kVarInt  fieldKind = iota // int/int64/bool：ZigZag varint
	kFixed64                  // uint64/float64：8 字节小端
	kString                   // string
	kBytes                    // []byte blob
	kMessage                  // 已声明消息值类型：嵌套对象
	kSeq                      // 元素切片：WireSequence
	kNone                     // 支持集外（门卫报错）
)

// leafKinds 直接映射的叶子类型集（S004/M405 支持集的标量面）。
// int32/float32 等其余命名类型、chan/map/func/interface/指针均不在集内。
var leafKinds = map[string]fieldKind{
	"int":     kVarInt,
	"int64":   kVarInt,
	"bool":    kVarInt,
	"uint64":  kFixed64,
	"float64": kFixed64,
	"string":  kString,
	"[]byte":  kBytes,
}

// planMessages 计划消息清单（用户消息 + 方法派生/改名请求统一走此路径，
// S004/M405 字段门卫因此对请求参数同样生效）。msgs 须已按 Name 升序。
func planMessages(msgs []MessageDecl) ([]messageModel, bool, error) {
	msgTypes := make(map[string]bool, len(msgs))
	for _, m := range msgs {
		msgTypes[m.TypeName] = true
	}
	var out []messageModel
	useMath := false
	for _, msg := range msgs {
		mm := messageModel{Name: msg.Name, TypeName: msg.TypeName}
		for _, f := range msg.Fields {
			fm, um, err := planField(msg.Name, f, msgTypes)
			if err != nil {
				return nil, false, err
			}
			useMath = useMath || um
			mm.Fields = append(mm.Fields, fm)
		}
		out = append(out, mm)
	}
	return out, useMath, nil
}

// planShapes 计划消息形状清单（完整指纹形态）：每消息每字段一条
// "TypeName:Field:Order:Type"（Type 为源码形态类型串），整体字典序；
// 零字段消息一条仅类型名的 "TypeName" 条目——零字段消息的增删/改名同样
// 必须翻指纹（无参方法派生的请求、空结构体消息都进清单）。消费方把清单
// 交给装配根的形状声明口进握手 schema 指纹——字段增删/改序/改型/改名都
// 翻指纹。msgs 须为合并后的完整消息清单（用户消息 + 方法派生/改名请求）。
func planShapes(msgs []MessageDecl) []string {
	var out []string
	for _, m := range msgs {
		if len(m.Fields) == 0 {
			out = append(out, m.TypeName) // 零字段消息：类型名即形状（身份）
			continue
		}
		for _, f := range m.Fields {
			out = append(out, fmt.Sprintf("%s:%s:%d:%s", m.TypeName, f.Name, f.Order, f.Type))
		}
	}
	sort.Strings(out)
	return out
}

// classifyType 按支持集分类字段类型表达式（发现层产出的源码形态串）。
// 第二返回值：kMessage 为目标类型名，kSeq 为元素 Go 类型串。
func classifyType(typ string, msgTypes map[string]bool) (fieldKind, string) {
	if k, ok := leafKinds[typ]; ok {
		return k, ""
	}
	if elem, ok := strings.CutPrefix(typ, "[]"); ok {
		if _, isLeaf := leafKinds[elem]; isLeaf {
			return kSeq, elem
		}
		if msgTypes[elem] {
			return kSeq, elem
		}
		return kNone, ""
	}
	if msgTypes[typ] {
		return kMessage, typ
	}
	return kNone, ""
}

// planField 产出单字段的模板数据。usesMath 报告该字段是否用到 math
// （float64 直接值或序列元素）。
func planField(msgName string, f FieldDecl, msgTypes map[string]bool) (fieldModel, bool, error) {
	kind, ref := classifyType(f.Type, msgTypes)
	if kind == kNone {
		return fieldModel{}, false, fmt.Errorf(
			"ransgen: message %s: field %s: unsupported field type %q [S004/M405] "+
				"(supported: int, int64, uint64, string, bool, float64, []byte, "+
				"declared message values, and slices thereof)",
			msgName, f.Name, f.Type)
	}
	fm := fieldModel{ID: f.Order, Name: f.Name, Kind: kind}
	usesMath := false
	switch kind {
	case kVarInt:
		fm.Cond = fmt.Sprintf("id == %d && wt == codec.WireVarInt", f.Order)
		switch f.Type {
		case "int":
			fm.Enc = fmt.Sprintf("w.WriteFieldVarInt(%d, int64(v.%s))", f.Order, f.Name)
			fm.Dec = lines(
				"n, err := r.ReadVarInt()",
				"if err != nil {",
				"return v, err",
				"}",
				fmt.Sprintf("v.%s = int(n)", f.Name))
		case "int64":
			fm.Enc = fmt.Sprintf("w.WriteFieldVarInt(%d, v.%s)", f.Order, f.Name)
			fm.Dec = lines(
				fmt.Sprintf("if v.%s, err = r.ReadVarInt(); err != nil {", f.Name),
				"return v, err",
				"}")
		case "bool":
			fm.Enc = lines(
				fmt.Sprintf("if v.%s {", f.Name),
				fmt.Sprintf("w.WriteFieldVarInt(%d, 1)", f.Order),
				"} else {",
				fmt.Sprintf("w.WriteFieldVarInt(%d, 0)", f.Order),
				"}")
			fm.Dec = lines(
				"n, err := r.ReadVarInt()",
				"if err != nil {",
				"return v, err",
				"}",
				fmt.Sprintf("v.%s = n != 0", f.Name))
		}
	case kFixed64:
		fm.Cond = fmt.Sprintf("id == %d && wt == codec.WireFixed64", f.Order)
		if f.Type == "float64" {
			usesMath = true
			fm.Enc = fmt.Sprintf("w.WriteFieldFixed64(%d, math.Float64bits(v.%s))", f.Order, f.Name)
			fm.Dec = lines(
				"bits, err := r.ReadFixed64()",
				"if err != nil {",
				"return v, err",
				"}",
				fmt.Sprintf("v.%s = math.Float64frombits(bits)", f.Name))
		} else {
			fm.Enc = fmt.Sprintf("w.WriteFieldFixed64(%d, v.%s)", f.Order, f.Name)
			fm.Dec = lines(
				fmt.Sprintf("if v.%s, err = r.ReadFixed64(); err != nil {", f.Name),
				"return v, err",
				"}")
		}
	case kString:
		fm.Cond = fmt.Sprintf("id == %d && wt == codec.WireString", f.Order)
		fm.Enc = fmt.Sprintf("w.WriteFieldString(%d, v.%s)", f.Order, f.Name)
		fm.Dec = lines(
			fmt.Sprintf("if v.%s, err = r.ReadString(); err != nil {", f.Name),
			"return v, err",
			"}")
	case kBytes:
		fm.Cond = fmt.Sprintf("id == %d && wt == codec.WireBytes", f.Order)
		fm.Enc = fmt.Sprintf("w.WriteFieldBytes(%d, v.%s)", f.Order, f.Name)
		// 拷出而非引用输入：注册表值须独立于载荷缓冲（池化页所有权纪律）。
		fm.Dec = lines(
			"bs, err := r.ReadBytes()",
			"if err != nil {",
			"return v, err",
			"}",
			fmt.Sprintf("v.%s = append([]byte(nil), bs...)", f.Name))
	case kMessage:
		fm.Cond = fmt.Sprintf("id == %d && wt == codec.WireMessage", f.Order)
		fm.Enc = fmt.Sprintf("w.WriteFieldRaw(%d, codec.WireMessage, ransEncode%s(v.%s))", f.Order, ref, f.Name)
		fm.Dec = lines(
			fmt.Sprintf("if v.%s, err = ransDecode%s(r); err != nil {", f.Name, ref),
			"return v, err",
			"}")
	case kSeq:
		elemKind, _ := classifyType(ref, msgTypes)
		usesMath = ref == "float64"
		fm.Cond = fmt.Sprintf("id == %d && wt == codec.WireSequence", f.Order)
		// 整段裹进独立块：同一消息多个序列字段时 sw 不互相重声明。
		fm.Enc = lines(
			"{",
			"sw := codec.NewWriter()",
			fmt.Sprintf("sw.WriteVarUInt(uint64(codec.%s))", wireConstName(elemKind)),
			fmt.Sprintf("sw.WriteVarInt(int64(len(v.%s)))", f.Name),
			fmt.Sprintf("for _, e := range v.%s {", f.Name),
			seqElemEnc(ref),
			"}",
			fmt.Sprintf("w.WriteFieldRaw(%d, codec.WireSequence, sw.Bytes())", f.Order),
			"}")
		fm.Dec = lines(
			"ew, err := r.ReadVarUInt()",
			"if err != nil {",
			"return v, err",
			"}",
			fmt.Sprintf("if codec.WireType(ew) != codec.%s {", wireConstName(elemKind)),
			"return v, codec.ErrMalformed",
			"}",
			"n, err := r.ReadVarInt()",
			"if err != nil {",
			"return v, err",
			"}",
			"if n < 0 || int(n) > r.Remaining() {", // 校验先于分配（陷阱 10）
			"return v, codec.ErrTruncated",
			"}",
			fmt.Sprintf("items := make([]%s, 0, int(n))", ref),
			"for j := 0; j < int(n); j++ {",
			seqElemDec(ref),
			"}",
			fmt.Sprintf("v.%s = items", f.Name))
	}
	return fm, usesMath, nil
}

// wireConstName 线型常量名（模板里拼 codec.<name>）。
func wireConstName(k fieldKind) string {
	switch k {
	case kVarInt:
		return "WireVarInt"
	case kFixed64:
		return "WireFixed64"
	case kString:
		return "WireString"
	case kBytes:
		return "WireBytes"
	case kMessage:
		return "WireMessage"
	}
	return ""
}

// seqElemEnc 序列元素编码语句（写进 sw，元素标识符 e）。elemType 为元素
// Go 类型串；default 分支即已声明消息类型（ransWrite<T> 写对象入 sw）。
func seqElemEnc(elemType string) string {
	switch elemType {
	case "int":
		return "sw.WriteVarInt(int64(e))"
	case "int64":
		return "sw.WriteVarInt(e)"
	case "bool":
		return lines(
			"if e {",
			"sw.WriteVarInt(1)",
			"} else {",
			"sw.WriteVarInt(0)",
			"}")
	case "uint64":
		return "sw.WriteFixed64(e)"
	case "float64":
		return "sw.WriteFixed64(math.Float64bits(e))"
	case "string":
		return "sw.WriteString(e)"
	case "[]byte":
		return "sw.WriteBytes(e)"
	default:
		return fmt.Sprintf("ransWrite%s(sw, e)", elemType)
	}
}

// seqElemDec 序列元素解码语句（读 r、追加进 items，元素标识符 x）。
// []byte 元素拷出——注册表值不引用输入载荷缓冲。
func seqElemDec(elemType string) string {
	read := ""
	switch elemType {
	case "int", "int64", "bool":
		read = "x, err := r.ReadVarInt()"
	case "uint64":
		read = "x, err := r.ReadFixed64()"
	case "float64":
		read = "x, err := r.ReadFixed64()"
	case "string":
		read = "x, err := r.ReadString()"
	case "[]byte":
		read = "x, err := r.ReadBytes()"
	default:
		read = fmt.Sprintf("x, err := ransDecode%s(r)", elemType)
	}
	appendExpr := ""
	switch elemType {
	case "int":
		appendExpr = "items = append(items, int(x))"
	case "bool":
		appendExpr = "items = append(items, x != 0)"
	case "float64":
		appendExpr = "items = append(items, math.Float64frombits(x))"
	case "[]byte":
		appendExpr = "items = append(items, append([]byte(nil), x...))"
	default:
		appendExpr = "items = append(items, x)"
	}
	return lines(
		read,
		"if err != nil {",
		"return v, err",
		"}",
		appendExpr)
}

// lines 多行语句串接（缩进交给 gofmt 归一）。
func lines(ss ...string) string { return strings.Join(ss, "\n") }
