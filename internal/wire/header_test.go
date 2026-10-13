package wire

import (
	"bytes"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
	"github.com/cyinx/rans_golang/internal/protocol"
)

func TestHeaderRoundTripAllFields(t *testing.T) {
	h := Header{
		Type:         protocol.MsgRequest,
		Flags:        protocol.FlagHasStatus | protocol.FlagHasMessagerHash | protocol.FlagHasMethodHash | protocol.FlagHasSourceNodeID | protocol.FlagHasTargetUUID,
		MessageID:    12345,
		Status:       rans.CodeOK,
		MessagerHash: 7,
		MethodHash:   9,
		SourceNodeID: 3,
		TargetKind:   "player",
		TargetInst:   "p1",
	}
	got, err := DecodeHeader(h.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != h.Type || got.Flags != h.Flags || got.MessageID != h.MessageID ||
		got.Status != h.Status || got.MessagerHash != h.MessagerHash || got.MethodHash != h.MethodHash ||
		got.SourceNodeID != h.SourceNodeID || got.TargetKind != h.TargetKind ||
		got.TargetInst != h.TargetInst {
		t.Fatalf("mismatch: %+v", got)
	}
}

func TestHeaderMinimalRoundTrip(t *testing.T) {
	h := Header{Type: protocol.MsgPing, Flags: 0, MessageID: 1}
	got, err := DecodeHeader(h.Encode())
	if err != nil || got.Type != protocol.MsgPing || got.MessageID != 1 {
		t.Fatalf("got %+v err %v", got, err)
	}
}

func TestHeaderForwardedFlagPreserved(t *testing.T) {
	h := Header{Type: protocol.MsgRequest, Flags: protocol.FlagForwarded | protocol.FlagHasSourceNodeID, MessageID: 9, SourceNodeID: 4}
	got, err := DecodeHeader(h.Encode())
	if err != nil || got.Flags&protocol.FlagForwarded == 0 {
		t.Fatal("forwarded flag must round-trip")
	}
}

// ---- 头部扩展块（规格 §7.2：位置段冻结 + 尾部 field-tag 对象）----

// extHeader 构造带扩展块的头部与其已编码 Ext 对象体（计数终结）。
func extHeader(ext *codec.Writer, fields int) Header {
	w := codec.NewWriter()
	w.BeginObject(fields)
	if ext != nil {
		w.WriteRaw(ext.Bytes())
	}
	return Header{
		Type:         protocol.MsgRequest,
		Flags:        protocol.FlagHasSourceNodeID | protocol.FlagHasExtensions,
		MessageID:    42,
		SourceNodeID: 7,
		Ext:          w.Bytes(),
	}
}

// TestHeaderExtRoundTrip FlagHasExtensions 置位时尾部追加已编码对象体：
// 解码原字节拷贝回 Ext、其余位置段字段不受影响。
func TestHeaderExtRoundTrip(t *testing.T) {
	body := codec.NewWriter()
	body.WriteFieldString(1, "trace-9")
	body.WriteFieldVarInt(2, 5)
	h := extHeader(body, 2)
	got, err := DecodeHeader(h.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Ext, h.Ext) {
		t.Fatalf("ext bytes must round-trip verbatim: got %v want %v", got.Ext, h.Ext)
	}
	if got.MessageID != 42 || got.SourceNodeID != 7 || got.Type != protocol.MsgRequest {
		t.Fatalf("positional fields must be intact: %+v", got)
	}
}

// TestHeaderExtUnknownFieldSkipped 含未知字段号（9，极大字段号同族）的扩展
// 对象：读端按线型跳过、解码成功（此后头部新字段一律进扩展块——旧端跳过
// 即前向兼容的承重行为）。
func TestHeaderExtUnknownFieldSkipped(t *testing.T) {
	body := codec.NewWriter()
	body.WriteFieldVarInt(1, 1)
	body.WriteTag(9, codec.WireVarInt) // 未来版本的新字段
	body.WriteVarInt(3)
	h := extHeader(body, 2)
	got, err := DecodeHeader(h.Encode())
	if err != nil {
		t.Fatalf("unknown ext field must be skipped: %v", err)
	}
	if len(got.Ext) == 0 {
		t.Fatal("ext bytes must be captured even with unknown fields")
	}
}

// TestHeaderExtHugeFieldIDRejected 扩展对象 fieldId 超出 MaxFieldID → 畸形
// （溢出防御，与 codec.ReadFieldTag 同纪律，不可静默截断）。
func TestHeaderExtHugeFieldIDRejected(t *testing.T) {
	w := codec.NewWriter()
	w.WriteFixed8(uint8(protocol.MsgRequest))
	w.WriteFixed32(uint32(protocol.FlagHasExtensions))
	w.WriteFixed64(1)
	w.BeginObject(1)
	w.WriteVarUInt(codec.FieldTag(codec.MaxFieldID+1, codec.WireVarInt))
	w.WriteVarInt(1)
	if _, err := DecodeHeader(w.Bytes()); err == nil {
		t.Fatal("ext field id beyond MaxFieldID must be rejected")
	}
}

// TestHeaderExtUndefinedWireTypeRejected 扩展对象携带未定义线型（4bit 域
// 14/15）→ 报错（SkipField 的畸形语义；不 panic、不静默吞）。
func TestHeaderExtUndefinedWireTypeRejected(t *testing.T) {
	for _, wt := range []codec.WireType{14, 15} {
		w := codec.NewWriter()
		w.WriteFixed8(uint8(protocol.MsgPing))
		w.WriteFixed32(uint32(protocol.FlagHasExtensions))
		w.WriteFixed64(1)
		w.BeginObject(1)
		w.WriteTag(3, wt)
		if _, err := DecodeHeader(w.Bytes()); err == nil {
			t.Fatalf("undefined wire type %d must be rejected", wt)
		}
	}
}

// TestHeaderExtNilAndZeroRemaining Ext nil = 不写扩展块；flag 置位而余量 0
// → Ext=nil（容忍形态）；flag 未置位而尾随字节 → 畸形（帧错位防线不变）。
func TestHeaderExtNilAndZeroRemaining(t *testing.T) {
	// flag 置位 + Ext nil：编码不写块，解码得 nil。
	h := Header{Type: protocol.MsgPing, Flags: protocol.FlagHasExtensions, MessageID: 1}
	got, err := DecodeHeader(h.Encode())
	if err != nil || got.Ext != nil {
		t.Fatalf("flag without payload must decode to nil ext, got %v err %v", got.Ext, err)
	}
	// flag 未置位 + 尾随垃圾 → 畸形。
	bare := Header{Type: protocol.MsgPing, MessageID: 1}
	raw := bare.Encode()
	if _, err := DecodeHeader(append(raw, 0xFF)); err == nil {
		t.Fatal("trailing garbage without ext flag must stay malformed")
	}
}

// TestHeaderExtDetachedFromInput 扩展块解码产物拷贝脱离输入缓冲（页归还后
// Ext 仍可用）。
func TestHeaderExtDetachedFromInput(t *testing.T) {
	body := codec.NewWriter()
	body.WriteFieldString(1, "payload")
	h := extHeader(body, 1)
	raw := h.Encode()
	got, err := DecodeHeader(raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		raw[i] = 0
	}
	if !bytes.Equal(got.Ext, h.Ext) {
		t.Fatal("ext must be detached from the input buffer")
	}
}

// TestHeaderRetiredCausalFlagNotWritten 退役位（HasCausalContext）不再被
// 编码端置位、解码端不消费：扩展块 causal 字段（H12 决策 14）取代头部
// 嵌套因果对象——载荷经 Ext 原样往返，位置段不再有因果条件字段。
func TestHeaderRetiredCausalFlagNotWritten(t *testing.T) {
	w := codec.NewWriter()
	w.BeginObject(1)
	w.WriteTag(ExtFieldCausal, codec.WireMessage)
	w.BeginObject(2)
	w.WriteFieldString(ExtCausalRoot, "trace-1")
	w.WriteTag(ExtCausalHop, codec.WireVarInt)
	w.WriteVarUInt(42)
	h := Header{
		Type:      protocol.MsgRequest,
		Flags:     protocol.FlagHasSourceNodeID | protocol.FlagHasExtensions,
		MessageID: 5,
		Ext:       w.Bytes(),
	}
	raw := h.Encode()
	if raw == nil || int(raw[1])&int(protocol.FlagHasCausalContext) != 0 {
		t.Fatal("encoder must never set the retired causal flag bit")
	}
	got, err := DecodeHeader(raw)
	if err != nil {
		t.Fatal(err)
	}
	ext, err := DecodeRequestExt(got.Ext)
	if err != nil {
		t.Fatal(err)
	}
	if ext.Causal == nil || ext.Causal.Root != "trace-1" ||
		len(ext.Causal.Path) != 1 || ext.Causal.Path[0] != 42 {
		t.Fatalf("causal object must round-trip via the ext block: %+v", ext.Causal)
	}
}

func TestIDGenMonotonic(t *testing.T) {
	g := &IDGen{}
	prev := g.Next()
	if prev != 1 {
		t.Fatalf("first id = %d, want 1", prev)
	}
	for i := 0; i < 1000; i++ {
		next := g.Next()
		if next <= prev {
			t.Fatalf("id regressed: %d -> %d", prev, next)
		}
		prev = next
	}
}
