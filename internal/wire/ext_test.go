package wire

import (
	"testing"

	"github.com/cyinx/rans_golang/codec"
	"github.com/cyinx/rans_golang/internal/ctxkey"
)

// 扩展块请求字段编解码（H10 决策 5）：budget_ms/trace_id 的读写通道——
// 合法往返、缺省不在场、未知字段跳过、线型不符跳过、畸形报错。

func TestRequestExtRoundTripBothFields(t *testing.T) {
	e := RequestExt{BudgetMS: 12345, HasBudget: true, TraceID: "trace-7", HasTraceID: true}
	got, err := DecodeRequestExt(EncodeRequestExt(e))
	if err != nil {
		t.Fatal(err)
	}
	if got.BudgetMS != 12345 || !got.HasBudget || got.TraceID != "trace-7" || !got.HasTraceID {
		t.Fatalf("mismatch: %+v", got)
	}
}

// 随迁包源纪元字段：单字段在场的编解码回环；与既有字段混编互不干扰；
// 缺场（旧端发送方形态）解码为零值不在场——同 VerMax 只追加字段号的演进
// 语义由未知字段跳过测试与本测试共同锚定。
func TestRequestExtSrcIncarnationRoundTrip(t *testing.T) {
	e := RequestExt{HasSrcIncarnation: true, SrcIncarnation: 42}
	got, err := DecodeRequestExt(EncodeRequestExt(e))
	if err != nil {
		t.Fatal(err)
	}
	if !got.HasSrcIncarnation || got.SrcIncarnation != 42 {
		t.Fatalf("mismatch: %+v", got)
	}

	mixed := RequestExt{BudgetMS: 7, HasBudget: true, HasSrcIncarnation: true, SrcIncarnation: 1 << 40}
	got, err = DecodeRequestExt(EncodeRequestExt(mixed))
	if err != nil {
		t.Fatal(err)
	}
	if got.BudgetMS != 7 || !got.HasBudget || !got.HasSrcIncarnation || got.SrcIncarnation != 1<<40 {
		t.Fatalf("mismatch: %+v", got)
	}

	got, err = DecodeRequestExt(nil)
	if err != nil || got.HasSrcIncarnation {
		t.Fatalf("absent field must decode zero-value not-present: %+v, %v", got, err)
	}
}

func TestRequestExtBudgetOnly(t *testing.T) {
	e := RequestExt{BudgetMS: 999, HasBudget: true}
	got, err := DecodeRequestExt(EncodeRequestExt(e))
	if err != nil {
		t.Fatal(err)
	}
	if got.BudgetMS != 999 || !got.HasBudget || got.TraceID != "" || got.HasTraceID {
		t.Fatalf("mismatch: %+v", got)
	}
}

func TestRequestExtTraceOnly(t *testing.T) {
	// trace_id 本票只落编解码通道（生产者随因果路径 v2 接入）：空预算、
	// 仅 trace 的形态必须可编码可解码。
	e := RequestExt{TraceID: "root-1", HasTraceID: true}
	got, err := DecodeRequestExt(EncodeRequestExt(e))
	if err != nil {
		t.Fatal(err)
	}
	if got.HasBudget || got.TraceID != "root-1" || !got.HasTraceID {
		t.Fatalf("mismatch: %+v", got)
	}
}

func TestRequestExtEmptyEncodesNil(t *testing.T) {
	if b := EncodeRequestExt(RequestExt{}); b != nil {
		t.Fatalf("empty ext must encode nil, got %v", b)
	}
	got, err := DecodeRequestExt(nil)
	if err != nil || got.HasBudget || got.HasTraceID {
		t.Fatalf("nil ext must decode to zero value, got %+v err %v", got, err)
	}
}

func TestRequestExtUnknownFieldAndWireTypeSkipped(t *testing.T) {
	// 未来版本新字段（号 9）+ 已知字段号线型不符（budget_ms 以 string 在场、
	// trace_id 以 varint 在场）——不静默重解释为对应字段值，按线型跳过后
	// 两个已知字段均不在场，解码不报错。
	w := codec.NewWriter()
	w.BeginObject(3)
	w.WriteFieldString(ExtFieldBudgetMS, "not-a-budget")
	w.WriteFieldString(9, "future-field")
	w.WriteTag(ExtFieldTraceID, codec.WireVarInt)
	w.WriteVarInt(0)
	got, err := DecodeRequestExt(w.Bytes())
	if err != nil {
		t.Fatalf("unknown fields must be skipped: %v", err)
	}
	if got.HasBudget || got.HasTraceID {
		t.Fatalf("mismatched known fields must not be adopted: %+v", got)
	}
}

func TestRequestExtTruncatedRejected(t *testing.T) {
	full := EncodeRequestExt(RequestExt{BudgetMS: 5, HasBudget: true, TraceID: "t", HasTraceID: true})
	if _, err := DecodeRequestExt(full[:len(full)-1]); err == nil {
		t.Fatal("truncated ext must error")
	}
	if _, err := DecodeRequestExt([]byte{0x01}); err == nil {
		t.Fatal("truncated container must error")
	}
}

func TestRequestExtHugeBudgetVarint(t *testing.T) {
	// varint 满 10 字节的极值预算：解码不溢出不 panic（毫秒→时长的换算与
	// 封顶归消费方，线层原样保真）。
	e := RequestExt{BudgetMS: ^uint64(0), HasBudget: true}
	got, err := DecodeRequestExt(EncodeRequestExt(e))
	if err != nil {
		t.Fatal(err)
	}
	if got.BudgetMS != ^uint64(0) {
		t.Fatalf("budget %d, want max uint64", got.BudgetMS)
	}
}

func TestRequestExtCtxItemsRoundTrip(t *testing.T) {
	// ctx 重复项（H10 决策 8）：多键有序往返，与预算字段共存。
	e := RequestExt{
		BudgetMS: 500, HasBudget: true,
		Ctx: []CtxItem{
			{Name: "pkg.a", Value: []byte("v1")},
			{Name: "pkg.b", Value: []byte{}},
		},
	}
	got, err := DecodeRequestExt(EncodeRequestExt(e))
	if err != nil {
		t.Fatal(err)
	}
	if !got.HasBudget || got.BudgetMS != 500 {
		t.Fatalf("budget mismatch: %+v", got)
	}
	if len(got.Ctx) != 2 || got.Ctx[0].Name != "pkg.a" || string(got.Ctx[0].Value) != "v1" ||
		got.Ctx[1].Name != "pkg.b" || len(got.Ctx[1].Value) != 0 {
		t.Fatalf("ctx items mismatch: %+v", got.Ctx)
	}
}

func TestRequestExtCtxIncompleteItemSkipped(t *testing.T) {
	// 语义不完整项（缺 value / 缺 name）逐项甄别跳过，不报错不采纳；
	// 项内未知子字段跳过。
	w := codec.NewWriter()
	w.BeginObject(3)
	w.WriteTag(ExtFieldCtx, codec.WireMessage)
	w.BeginObject(1) // 缺 value
	w.WriteFieldString(ExtCtxItemName, "only-name")
	w.WriteTag(ExtFieldCtx, codec.WireMessage)
	w.BeginObject(3) // 未知子字段 + 合法项
	w.WriteFieldString(9, "future")
	w.WriteFieldString(ExtCtxItemName, "k2")
	w.WriteFieldBytes(ExtCtxItemValue, []byte("v2"))
	w.WriteTag(ExtFieldCtx, codec.WireMessage)
	w.BeginObject(0) // 空项
	got, err := DecodeRequestExt(w.Bytes())
	if err != nil {
		t.Fatalf("incomplete items must be skipped: %v", err)
	}
	if len(got.Ctx) != 1 || got.Ctx[0].Name != "k2" || string(got.Ctx[0].Value) != "v2" {
		t.Fatalf("ctx items = %+v, want only the complete item k2", got.Ctx)
	}
}

func TestRequestExtCtxWrongWireTypeSkipped(t *testing.T) {
	// ctx 字段线型不符（string 在场）：按线型跳过，不重解释。
	w := codec.NewWriter()
	w.BeginObject(1)
	w.WriteFieldString(ExtFieldCtx, "not-a-message")
	got, err := DecodeRequestExt(w.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Ctx) != 0 {
		t.Fatalf("mismatched ctx field must not be adopted: %+v", got.Ctx)
	}
}

func TestRequestExtCausalRoundTrip(t *testing.T) {
	// causal 对象（H12 决策 14）：root + 重复 varint 哈希序列按途经序往返，
	// 与预算/trace_id/ctx 项共存（三类字段同块书写）。
	e := RequestExt{
		BudgetMS: 900, HasBudget: true,
		TraceID: "root-1", HasTraceID: true,
		Causal: &ctxkey.CausalPath{Root: "root-1", Path: []uint64{1, 2, 1 << 40}},
		Ctx:    []CtxItem{{Name: "pkg.a", Value: []byte("v1")}},
	}
	got, err := DecodeRequestExt(EncodeRequestExt(e))
	if err != nil {
		t.Fatal(err)
	}
	if got.TraceID != "root-1" || got.Causal == nil || got.Causal.Root != "root-1" {
		t.Fatalf("root/trace mismatch: %+v", got)
	}
	if len(got.Causal.Path) != 3 || got.Causal.Path[0] != 1 || got.Causal.Path[1] != 2 || got.Causal.Path[2] != 1<<40 {
		t.Fatalf("causal path mismatch: %+v", got.Causal.Path)
	}
}

func TestRequestExtCausalEmptyPathRoundTrip(t *testing.T) {
	// 顶层调用形态：root 在场、路径为空（对象只含 root 一项）。
	e := RequestExt{Causal: &ctxkey.CausalPath{Root: "top"}}
	got, err := DecodeRequestExt(EncodeRequestExt(e))
	if err != nil {
		t.Fatal(err)
	}
	if got.Causal == nil || got.Causal.Root != "top" || len(got.Causal.Path) != 0 {
		t.Fatalf("empty-path causal mismatch: %+v", got.Causal)
	}
	if got.HasTraceID {
		t.Fatalf("trace channel is independent of causal, got %+v", got)
	}
}

func TestRequestExtCausalHugeHopCountRejected(t *testing.T) {
	// 对抗样本：causal 对象声明巨型项数而缓冲无元素字节——容量校验先于
	// 分配（陷阱 10），返回 ErrTruncated 绝不 panic。
	w := codec.NewWriter()
	w.BeginObject(1)
	w.WriteTag(ExtFieldCausal, codec.WireMessage)
	w.BeginObject(1 << 62) // 巨型项数声明，缓冲无元素字节
	if _, err := DecodeRequestExt(w.Bytes()); err != codec.ErrTruncated {
		t.Fatalf("huge hop count must return codec.ErrTruncated, got %v", err)
	}
}

func TestRequestExtCausalWrongWireTypeSkipped(t *testing.T) {
	// causal 字段线型不符（string 在场）：整体按线型跳过，不重解释。
	w := codec.NewWriter()
	w.BeginObject(1)
	w.WriteFieldString(ExtFieldCausal, "not-a-message")
	got, err := DecodeRequestExt(w.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got.Causal != nil {
		t.Fatalf("mismatched causal field must not be adopted: %+v", got.Causal)
	}
}

func TestRequestExtCausalUnknownSubfieldSkipped(t *testing.T) {
	// causal 对象内未知子字段按线型跳过（演进语义），已知字段照常读取。
	w := codec.NewWriter()
	w.BeginObject(1)
	w.WriteTag(ExtFieldCausal, codec.WireMessage)
	w.BeginObject(3)
	w.WriteFieldVarInt(9, 1) // 未来版本的子字段
	w.WriteFieldString(ExtCausalRoot, "r")
	w.WriteTag(ExtCausalHop, codec.WireVarInt)
	w.WriteVarUInt(7)
	got, err := DecodeRequestExt(w.Bytes())
	if err != nil {
		t.Fatalf("unknown causal subfields must be skipped: %v", err)
	}
	if got.Causal == nil || got.Causal.Root != "r" || len(got.Causal.Path) != 1 || got.Causal.Path[0] != 7 {
		t.Fatalf("causal mismatch: %+v", got.Causal)
	}
}
