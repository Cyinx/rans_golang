// serial_test 锁死生成序列化器的行为契约：注册表命中、全形态往返、演进
// 语义三锁（未知字段跳过 / 缺失默认零值 / 线型不符跳过）与序列计数防线。
// 演进场景按对象测试手法手搓 writer 字节构造。
package serialdemo

import (
	"reflect"
	"testing"

	"github.com/cyinx/rans_golang/codec"
)

// TestRegisteredNames 注册表命中锁：EncodeValue 产注册名而非 JSON 兜底名
// （兜底名形如 "serialdemo.Shape"）。
func TestRegisteredNames(t *testing.T) {
	cases := []struct {
		v    any
		want string
	}{
		{Shape{}, "serial.Shape"},
		{Point{}, "Point"},
		{Alpha{}, "Alpha"},
		{Zeta{}, "Zeta"},
	}
	for _, tc := range cases {
		name, _, err := codec.Encode(tc.v)
		if err != nil {
			t.Fatalf("EncodeValue(%T): %v", tc.v, err)
		}
		if name != tc.want {
			t.Fatalf("EncodeValue(%T) name = %q, want %q", tc.v, name, tc.want)
		}
	}
}

// TestShapeRoundTrip 全形态往返：13 类字段各带非零值，DeepEqual 逐位比对。
func TestShapeRoundTrip(t *testing.T) {
	in := Shape{
		Label:   "tri",
		Count:   -3,
		Max:     1 << 40,
		Ratio:   2.5,
		On:      true,
		Blob:    []byte{1, 2, 3},
		Origin:  Point{X: -7, Y: 9},
		Tags:    []string{"a", "b"},
		Coords:  []Point{{X: 1, Y: 2}, {X: -3, Y: 4}},
		Flags:   []bool{true, false},
		Matrix:  [][]byte{{9}, {8, 8}},
		Weights: []float64{1.5, -2.25},
		Ids:     []uint64{1, 1 << 50},
	}
	name, payload, err := codec.Encode(in)
	if err != nil {
		t.Fatalf("EncodeValue: %v", err)
	}
	if name != "serial.Shape" {
		t.Fatalf("name = %q, want serial.Shape", name)
	}
	got, err := codec.Decode(name, payload)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("round-trip 不一致:\ngot  %+v\nwant %+v", got, in)
	}
}

// TestNestedRoundTrip 嵌套消息往返（Alpha→Zeta 前向引用形态）。
func TestNestedRoundTrip(t *testing.T) {
	in := Alpha{Ref: Zeta{V: 42}}
	name, payload, err := codec.Encode(in)
	if err != nil {
		t.Fatalf("EncodeValue: %v", err)
	}
	got, err := codec.Decode(name, payload)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("round-trip 不一致: got %+v want %+v", got, in)
	}
}

// TestEvolutionUnknownFieldSkipped 演进锁 1：未知 fieldId 的字段整段跳过，
// 已知字段照常解码。
func TestEvolutionUnknownFieldSkipped(t *testing.T) {
	w := codec.NewWriter()
	w.BeginObject(3)
	w.WriteFieldString(1, "L")
	w.WriteFieldVarInt(2, 7)
	w.WriteFieldString(99, "future")
	got, err := codec.Decode("serial.Shape", w.Bytes())
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	s := got.(Shape)
	if s.Label != "L" || s.Count != 7 {
		t.Fatalf("已知字段未被解码: %+v", s)
	}
}

// TestEvolutionMissingFieldDefaultsZero 演进锁 2：对端旧版本少发字段，
// 缺失字段保持零值。
func TestEvolutionMissingFieldDefaultsZero(t *testing.T) {
	w := codec.NewWriter()
	w.BeginObject(1)
	w.WriteFieldString(1, "L")
	got, err := codec.Decode("serial.Shape", w.Bytes())
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	s := got.(Shape)
	if s.Label != "L" {
		t.Fatalf("字段 1 未解码: %+v", s)
	}
	if s.Count != 0 || s.Max != 0 || s.Ratio != 0 || s.On ||
		s.Blob != nil || s.Origin != (Point{}) ||
		s.Tags != nil || s.Coords != nil || s.Flags != nil ||
		s.Matrix != nil || s.Weights != nil || s.Ids != nil {
		t.Fatalf("缺失字段未保持零值: %+v", s)
	}
}

// TestEvolutionWireTypeMismatchSkipped 演进锁 3：同 fieldId 不同线型
// （字段 2 Count 为 varint，收到 fixed64）按 (id,wt) 双匹配落入 default
// 跳过，不留半读状态。
func TestEvolutionWireTypeMismatchSkipped(t *testing.T) {
	w := codec.NewWriter()
	w.BeginObject(2)
	w.WriteFieldString(1, "L")
	w.WriteFieldFixed64(2, 77)
	got, err := codec.Decode("serial.Shape", w.Bytes())
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	s := got.(Shape)
	if s.Label != "L" || s.Count != 0 {
		t.Fatalf("线型不符字段未被跳过: %+v", s)
	}
}

// TestSequenceCountGuard 陷阱 10 防线：序列 count 远超剩余字节时拒绝
// （ErrTruncated），不按 count 巨量分配。
func TestSequenceCountGuard(t *testing.T) {
	seq := codec.NewWriter()
	seq.WriteVarUInt(uint64(codec.WireString))
	seq.WriteVarInt(1 << 30)
	w := codec.NewWriter()
	w.BeginObject(1)
	w.WriteFieldRaw(8, codec.WireSequence, seq.Bytes())
	if _, err := codec.Decode("serial.Shape", w.Bytes()); err == nil {
		t.Fatal("超限序列 count 未被拒绝")
	}
}

// TestRansShapesContent 形状清单内容锁：生成物 RansShapes 与消息面逐条
// 对账——每消息每字段一条 "TypeName:Field:Order:Type"（Type 为源码形态
// 类型串），整体字典序；生成物 init 以 codec.RegisterShapes 登记、握手
// SchemaDigest 据此计算（登记口由生成物 init 承载）。
func TestRansShapesContent(t *testing.T) {
	want := []string{
		"Alpha:Ref:1:Zeta",
		"Point:X:1:int64",
		"Point:Y:2:int64",
		"Shape:Blob:6:[]byte",
		"Shape:Coords:9:[]Point",
		"Shape:Count:2:int",
		"Shape:Flags:10:[]bool",
		"Shape:Ids:13:[]uint64",
		"Shape:Label:1:string",
		"Shape:Matrix:11:[][]byte",
		"Shape:Max:3:uint64",
		"Shape:On:5:bool",
		"Shape:Origin:7:Point",
		"Shape:Ratio:4:float64",
		"Shape:Tags:8:[]string",
		"Shape:Weights:12:[]float64",
		"Zeta:V:1:int64",
	}
	if !reflect.DeepEqual(RansShapes, want) {
		t.Fatalf("RansShapes = %#v, want %#v", RansShapes, want)
	}
}
