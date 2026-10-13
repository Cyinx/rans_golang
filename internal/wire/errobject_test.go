package wire

// Error 帧体对象测试（规格 §5.3「线上半边」）：
// Error 帧 body 为 field-tag 对象——编解码往返、缺省字段零值、
// 未知字段号按线型跳过（含极大字段号）、畸形输入 fail-closed。

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
)

// TestErrorObjectRoundTrip 全字段往返（AppCode 取负值钉死 ZigZag 有符号
// varint；AppData 任意字节钉死 bytes 透明载荷）。Node 是端语义（由 header
// SourceNodeID 承载），不进 body：编码输入带 Node、解码产物 Node 必为 0。
func TestErrorObjectRoundTrip(t *testing.T) {
	in := &rans.Error{
		Code:    rans.CodeUnavailable,
		Reason:  rans.ReasonBusy,
		Message: "target busy or unavailable",
		Node:    7, // 端语义：不得进入 body
		AppCode: -4242,
		AppData: []byte{0x00, 0xFF, 0x7F, 0x80},
	}
	out, err := UnmarshalError(MarshalError(in))
	if err != nil {
		t.Fatalf("UnmarshalError: %v", err)
	}
	if out.Node != 0 {
		t.Fatalf("decoded Node = %d, want 0 (node identity rides the header, not the body)", out.Node)
	}
	in.Node = 0 // 对齐端语义后逐字段比对
	if out.Code != in.Code || out.Reason != in.Reason || out.Message != in.Message ||
		out.AppCode != in.AppCode || !bytes.Equal(out.AppData, in.AppData) {
		t.Fatalf("round trip mismatch: got %+v want %+v", out, in)
	}
}

// TestErrorObjectMissingFieldsZero 缺省字段取零值：只写 Code 的最小对象，
// 解码产物 Reason/Message/AppCode/AppData 均为零值。
func TestErrorObjectMissingFieldsZero(t *testing.T) {
	out, err := UnmarshalError(MarshalError(&rans.Error{Code: rans.CodeDecode}))
	if err != nil {
		t.Fatalf("UnmarshalError: %v", err)
	}
	if out.Code != rans.CodeDecode {
		t.Fatalf("code = %d, want CodeDecode", out.Code)
	}
	if out.Reason != rans.ReasonNone || out.Message != "" || out.AppCode != 0 || out.AppData != nil {
		t.Fatalf("missing fields must decode to zero values, got %+v", out)
	}
}

// TestErrorObjectSkipsUnknownFields 未知字段号按线型跳过（前向兼容承重面）：
// 手工构造含未知字段的对象——未知 varint 字段 9、极大字段号（MaxFieldID）
// 的 bytes 字段、已知字段号携带未知线型（field 3 fixed64）——读端跳过且
// 其余字段正确；未知字段出现在已知字段之前同样跳过（顺序无关）。
func TestErrorObjectSkipsUnknownFields(t *testing.T) {
	w := codec.NewWriter()
	w.BeginObject(6)
	w.WriteTag(9, codec.WireVarInt) // 未知字段号（varint 线型）
	w.WriteVarUInt(300)
	w.WriteTag(1, codec.WireVarInt)
	w.WriteVarUInt(uint64(rans.CodeNotFound))
	w.WriteTag(codec.MaxFieldID, codec.WireBytes) // 极大字段号
	w.WriteBytes([]byte("future payload"))
	w.WriteTag(2, codec.WireVarInt)
	w.WriteVarUInt(uint64(rans.ReasonUnknownKind))
	w.WriteTag(3, codec.WireFixed64) // 已知字段号、未知线型：同样跳过
	w.WriteFixed64(0xDEADBEEF)
	w.WriteTag(5, codec.WireBytes)
	w.WriteBytes([]byte("app"))

	out, err := UnmarshalError(w.Bytes())
	if err != nil {
		t.Fatalf("UnmarshalError with unknown fields: %v", err)
	}
	if out.Code != rans.CodeNotFound || out.Reason != rans.ReasonUnknownKind {
		t.Fatalf("known fields corrupted by unknown-field skip: code=%d reason=%d", out.Code, out.Reason)
	}
	if out.Message != "" || out.AppCode != 0 {
		t.Fatalf("skipped field 3/9 must not leak into decoded values: %+v", out)
	}
	if string(out.AppData) != "app" {
		t.Fatalf("app data = %q, want %q", out.AppData, "app")
	}
}

// TestErrorObjectMalformed 畸形输入 fail-closed：null 对象（count=-1）、
// 截断、声明字段数多于实际、声明字段消费完仍有残余字节。
func TestErrorObjectMalformed(t *testing.T) {
	cases := []struct {
		name string
		body []byte
	}{
		{"empty", nil},
		{"null object", []byte{0x01}}, // varint -1（ZigZag）：null 对象
		{"truncated count", []byte{0x80}},
		{"truncated field", func() []byte {
			w := codec.NewWriter()
			w.BeginObject(2)
			w.WriteTag(1, codec.WireVarInt)
			w.WriteVarUInt(2)
			return w.Bytes() // 声明 2 个字段只写 1 个
		}()},
		{"trailing garbage", func() []byte {
			b := MarshalError(&rans.Error{Code: rans.CodeOK})
			return append(b, 0x00)
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := UnmarshalError(tc.body); err == nil {
				t.Fatalf("UnmarshalError(% x) must fail", tc.body)
			} else if !errors.Is(err, codec.ErrTruncated) && !errors.Is(err, codec.ErrMalformed) {
				t.Fatalf("err = %v, want codec.ErrTruncated/ErrMalformed", err)
			}
		})
	}
}

// TestErrorObjectAppDataCopied 解码产物不引用输入缓冲：AppData 拷出——
// Error 帧页在网关解码后就地归还池，错误值却存活到调用方手里。
func TestErrorObjectAppDataCopied(t *testing.T) {
	body := MarshalError(&rans.Error{Code: rans.CodeError, AppData: []byte("sensitive")})
	out, err := UnmarshalError(body)
	if err != nil {
		t.Fatalf("UnmarshalError: %v", err)
	}
	for i := range body {
		body[i] = 0x00 // 模拟页归还后被复写
	}
	if string(out.AppData) != "sensitive" {
		t.Fatalf("AppData must be copied out of the input buffer, got %q", out.AppData)
	}
}
