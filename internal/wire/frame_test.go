package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/cyinx/rans_golang/codec"
	"github.com/cyinx/rans_golang/internal/protocol"
)

func TestFrameRoundTrip(t *testing.T) {
	hdr := []byte{1, 2, 3}
	body := []byte("payload")
	frame, err := EncodeFrame(hdr, body)
	if err != nil {
		t.Fatal(err)
	}
	if hl := binary.LittleEndian.Uint32(frame[0:4]); hl != 3 {
		t.Fatal("header length prefix")
	}
	d := NewFrameDecoder(bytes.NewReader(frame), NewPagePool())
	gotHdr, gotBody, err := d.Next()
	if err != nil || !bytes.Equal(gotHdr, hdr) || !bytes.Equal(gotBody, body) {
		t.Fatalf("hdr=%v body=%v err=%v", gotHdr, gotBody, err)
	}
	if _, _, err := d.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF, got %v", err)
	}
}

func TestFrameEmptyBody(t *testing.T) {
	frame, _ := EncodeFrame([]byte{9}, nil)
	d := NewFrameDecoder(bytes.NewReader(frame), NewPagePool())
	_, body, err := d.Next()
	if err != nil || len(body) != 0 {
		t.Fatalf("body=%v err=%v", body, err)
	}
}

func TestFrameRejectsOversizeBeforeAlloc(t *testing.T) {
	// 陷阱 10：超限长度必须在校验阶段拒绝，绝不能先分配再判断。
	// 断言钉死 codec.ErrMalformed 而非泛化 err!=nil：输入故意只给半截帧，
	// 若实现先分配再校验，会以 ErrUnexpectedEOF 收场而非在校验阶段拒收，
	// 本测试即红——回归护栏。
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], MaxHeaderSize+1) // 头超 64KiB
	r := bytes.NewReader(append(hdr[:], make([]byte, 8)...))
	d := NewFrameDecoder(r, NewPagePool())
	if _, _, err := d.Next(); !errors.Is(err, codec.ErrMalformed) {
		t.Fatalf("oversize header must be rejected as malformed, got %v", err)
	}

	binary.LittleEndian.PutUint32(hdr[0:4], 8)
	binary.LittleEndian.PutUint32(hdr[4:8], MaxFrameSize+1) // body 超 4MiB
	d = NewFrameDecoder(bytes.NewReader(hdr[:]), NewPagePool())
	if _, _, err := d.Next(); !errors.Is(err, codec.ErrMalformed) {
		t.Fatalf("oversize body must be rejected as malformed, got %v", err)
	}
}

func TestFrameTruncatedHeader(t *testing.T) {
	frame, _ := EncodeFrame([]byte{1, 2, 3}, []byte("x"))
	d := NewFrameDecoder(bytes.NewReader(frame[:len(frame)-2]), NewPagePool())
	if _, _, err := d.Next(); err == nil || errors.Is(err, io.EOF) {
		t.Fatal("truncated frame must error, not EOF")
	}
}

func TestFrameZeroHeaderLength(t *testing.T) {
	var b [8]byte
	d := NewFrameDecoder(bytes.NewReader(b[:]), NewPagePool())
	if _, _, err := d.Next(); err == nil {
		t.Fatal("zero header length is malformed")
	}
}

func FuzzFrameDecode(f *testing.F) {
	// 种子入库：合法帧、截断、超限、零长、垃圾。
	valid, _ := EncodeFrame([]byte{1, 2, 3}, []byte("hello"))
	f.Add(valid)
	f.Add(valid[:4])
	f.Add(valid[:0])
	var oversize [8]byte
	binary.LittleEndian.PutUint32(oversize[0:4], 1<<30)
	f.Add(oversize[:])
	binary.LittleEndian.PutUint32(oversize[0:4], 16)
	binary.LittleEndian.PutUint32(oversize[4:8], 0xFFFFFFFF)
	f.Add(oversize[:])
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00})
	// 头部扩展块种子：含扩展块的最小帧、未知/极大字段号、
	// 未定义线型——扩展块 walk（BeginObject+ReadFieldTag+SkipField）是
	// 解析面，对抗样本与合法样本都要进语料库。
	extFrame := func(build func(w *codec.Writer)) []byte {
		w := codec.NewWriter()
		w.WriteFixed8(uint8(protocol.MsgRequest))
		w.WriteFixed32(uint32(protocol.FlagHasSourceNodeID | protocol.FlagHasExtensions))
		w.WriteFixed64(1)
		w.WriteFixed16(7)
		build(w)
		frame, _ := EncodeFrame(w.Bytes(), []byte("body"))
		return frame
	}
	// 最小合法扩展块（空对象，计数 0）。
	f.Add(extFrame(func(w *codec.Writer) { w.BeginObject(0) }))
	// 未知字段号（9）+ 极大合法字段号（MaxFieldID）+ 未定义线型（14/15）混合。
	f.Add(extFrame(func(w *codec.Writer) {
		w.BeginObject(4)
		w.WriteFieldVarInt(9, 1)
		w.WriteTag(codec.MaxFieldID, codec.WireVarInt)
		w.WriteVarInt(2)
		w.WriteTag(3, codec.WireType(14))
		w.WriteTag(4, codec.WireType(15))
	}))
	// 超限字段号（MaxFieldID+1 → ReadFieldTag 拒）与截断的扩展块。
	f.Add(extFrame(func(w *codec.Writer) {
		w.BeginObject(1)
		w.WriteVarUInt(codec.FieldTag(codec.MaxFieldID+1, codec.WireVarInt))
		w.WriteVarInt(1)
	}))
	// 请求扩展块字段种子（H10 决策 5 解码面）：budget_ms/trace_id 合法
	// 形态、极值预算 varint、已知字段号的线型错配——DecodeRequestExt 与
	// 头部 walk 的双解析面都要有对抗样本。
	f.Add(extFrame(func(w *codec.Writer) {
		w.BeginObject(2)
		w.WriteTag(ExtFieldBudgetMS, codec.WireVarInt)
		w.WriteVarUInt(600000)
		w.WriteFieldString(ExtFieldTraceID, "trace-1")
	}))
	f.Add(extFrame(func(w *codec.Writer) {
		w.BeginObject(1)
		w.WriteTag(ExtFieldBudgetMS, codec.WireVarInt)
		w.WriteVarUInt(^uint64(0))
	}))
	f.Add(extFrame(func(w *codec.Writer) {
		w.BeginObject(2)
		w.WriteFieldString(ExtFieldBudgetMS, "wrong-wire-type")
		w.WriteTag(ExtFieldTraceID, codec.WireVarInt)
		w.WriteVarInt(-3)
	}))
	// ctx 字段种子（H10 决策 8 解码面）：合法重复项 + 语义不完整项（缺
	// value）+ 线型错配——重复项与嵌套对象的双解析面都要有对抗样本。
	f.Add(extFrame(func(w *codec.Writer) {
		w.BeginObject(2)
		w.WriteTag(ExtFieldCtx, codec.WireMessage)
		w.BeginObject(2)
		w.WriteFieldString(ExtCtxItemName, "pkg.a")
		w.WriteFieldBytes(ExtCtxItemValue, []byte("v1"))
		w.WriteTag(ExtFieldCtx, codec.WireMessage)
		w.BeginObject(1)
		w.WriteFieldString(ExtCtxItemName, "pkg.no-value")
	}))
	f.Add(extFrame(func(w *codec.Writer) {
		w.BeginObject(2)
		w.WriteFieldString(ExtFieldCtx, "wrong-wire-type")
		w.WriteTag(ExtFieldCtx, codec.WireMessage)
		w.BeginObject(0)
	}))
	// causal 字段种子（H12 决策 14 解码面）：合法对象（root + 多跳）、巨型
	// 跳数声明（陷阱 10 的容量守卫）、线型错配与截断——嵌套重复字段的
	// 解析面要有对抗样本。
	f.Add(extFrame(func(w *codec.Writer) {
		w.BeginObject(1)
		w.WriteTag(ExtFieldCausal, codec.WireMessage)
		w.BeginObject(4)
		w.WriteFieldString(ExtCausalRoot, "trace-cyc")
		w.WriteTag(ExtCausalHop, codec.WireVarInt)
		w.WriteVarUInt(0xdeadbeef)
		w.WriteTag(ExtCausalHop, codec.WireVarInt)
		w.WriteVarUInt(^uint64(0))
	}))
	f.Add(extFrame(func(w *codec.Writer) {
		w.BeginObject(1)
		w.WriteTag(ExtFieldCausal, codec.WireMessage)
		w.BeginObject(2)
		w.WriteFieldString(ExtCausalRoot, "huge")
		w.WriteTag(ExtCausalHop, codec.WireVarInt)
		w.WriteVarInt(int64(1) << 62) // 巨型跳数声明，缓冲无元素字节
	}))
	f.Add(extFrame(func(w *codec.Writer) {
		w.BeginObject(1)
		w.WriteFieldString(ExtFieldCausal, "wrong-wire-type") // 线型错配整体跳过
	}))
	// MsgCancel 帧种子（H10 决策 9 解码面）：Type 新值（15）+ 仅头部无载荷
	// 形态——取消通知是「头部即全量」的控制消息，空载荷帧要进语料库。
	cw := codec.NewWriter()
	cw.WriteFixed8(uint8(protocol.MsgCancel))
	cw.WriteFixed32(uint32(protocol.FlagHasSourceNodeID | protocol.FlagHasTargetUUID))
	cw.WriteFixed64(7)
	cw.WriteFixed16(1)
	cw.WriteString("cell")
	cw.WriteString("inst-7")
	cancelFrame, _ := EncodeFrame(cw.Bytes(), nil)
	f.Add(cancelFrame)
	trunc := extFrame(func(w *codec.Writer) {
		w.BeginObject(2)
		w.WriteFieldVarInt(1, 1)
	})
	f.Add(trunc[:len(trunc)-2])
	f.Fuzz(func(t *testing.T, data []byte) {
		d := NewFrameDecoder(bytes.NewReader(data), NewPagePool())
		for {
			raw, _, err := d.Next()
			if err != nil {
				return // 任何错误都可接受，panic 不可以
			}
			// 请求扩展块解码面同库（H10 决策 5）：帧内头部解码成功后再过
			// DecodeRequestExt——已知字段的线型甄别是 walk 之外的独立解析面，
			// 同一语料库覆盖，panic 不可以。
			if h, herr := DecodeHeader(raw); herr == nil {
				_, _ = DecodeRequestExt(h.Ext)
			}
		}
	})
}

// FuzzHandshakeDecode 握手体解码 fuzz（v3 扩展对象是 pre-auth
// 解析面——未知字段跳过、版本区间门禁、MAC 长度自洽都要在语料库里）。
// Makefile fuzz 目标覆盖本函数（与 FuzzFrameDecode 同门禁）。
func FuzzHandshakeDecode(f *testing.F) {
	seed := func(vmin, vmax uint16, build func(w *codec.Writer)) []byte {
		w := codec.NewWriter()
		w.WriteFixed16(vmin)
		w.WriteFixed16(vmax)
		build(w)
		var nonce [16]byte
		for i := range nonce {
			nonce[i] = byte(i)
		}
		w.WriteRaw(nonce[:])
		return w.Bytes()
	}
	// 合法最小握手（无 MAC）。
	f.Add(seed(3, 3, func(w *codec.Writer) {
		w.BeginObject(3)
		w.WriteTag(1, codec.WireFixed16)
		w.WriteFixed16(5)
		w.WriteFieldString(2, "cluster")
		w.WriteFieldFixed64(3, 0xABCD)
	}))
	// 未知字段号 9 + 极大字段号 + 未定义线型。
	f.Add(seed(3, 5, func(w *codec.Writer) {
		w.BeginObject(4)
		w.WriteTag(1, codec.WireFixed16)
		w.WriteFixed16(5)
		w.WriteFieldString(2, "cluster")
		w.WriteFieldFixed64(3, 0xABCD)
		w.WriteFieldVarInt(9, 77)
		w.WriteTag(codec.MaxFieldID, codec.WireVarInt)
		w.WriteVarInt(1)
		w.WriteTag(4, codec.WireType(14))
	}))
	// 不相交版本区间与区间倒置。
	f.Add(seed(2, 2, func(w *codec.Writer) { w.BeginObject(0) }))
	f.Add(seed(4, 9, func(w *codec.Writer) { w.BeginObject(0) }))
	f.Add(seed(5, 3, func(w *codec.Writer) { w.BeginObject(0) }))
	// 带 MAC 的完整握手 + 截断体。
	hs := Handshake{NodeID: 1, ClusterName: "t", SchemaDigest: 9}
	f.Add(hs.Encode([]byte("k")))
	f.Add(hs.Encode(nil))
	noMAC := hs.Encode(nil)
	f.Add(noMAC[:len(noMAC)-4])
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeHandshake(data) // 任何错误都可接受，panic 不可以
	})
}
