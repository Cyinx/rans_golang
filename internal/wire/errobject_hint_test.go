package wire

// Error 帧重定向提示字段（字段 6，H13 决策 24）往返锚定：OwnerNode/
// OwnerEpoch 编解码对称；零值不上线；畸形嵌套不致错误帧失败（提示是
// 加速面）。

import (
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
)

func TestErrorOwnerHintRoundTrip(t *testing.T) {
	e := rans.NewError(rans.CodeNotFound, rans.ReasonStaleLocation, "moved")
	e.OwnerNode, e.OwnerEpoch = 7, 42
	got, err := UnmarshalError(MarshalError(e))
	if err != nil {
		t.Fatal(err)
	}
	if got.OwnerNode != 7 || got.OwnerEpoch != 42 {
		t.Fatalf("owner hint round trip: node=%d epoch=%d, want 7/42", got.OwnerNode, got.OwnerEpoch)
	}
	// 零值不上线：无提示错误解码后保持零值。
	plain, err := UnmarshalError(MarshalError(rans.NewError(rans.CodeTimeout, rans.ReasonNone, "t")))
	if err != nil {
		t.Fatal(err)
	}
	if plain.OwnerNode != 0 || plain.OwnerEpoch != 0 {
		t.Fatalf("zero hint must stay zero: %d/%d", plain.OwnerNode, plain.OwnerEpoch)
	}
	// 畸形嵌套（字段 6 非法对象）：错误帧本体仍解出（提示跳过）。
	w := codec.NewWriter()
	w.BeginObject(1)
	iw := codec.NewWriter()
	iw.BeginObject(0) // 合法空对象：无 node/epoch 字段——提示解码为空（零值）
	w.WriteFieldBytes(6, iw.Bytes())
	mal := w.Bytes()
	got2, err := UnmarshalError(mal)
	if err != nil {
		t.Fatalf("malformed hint must not fail error frame: %v", err)
	}
	if got2.OwnerNode != 0 {
		t.Fatalf("malformed hint must leave zero owner, got %d", got2.OwnerNode)
	}
}
