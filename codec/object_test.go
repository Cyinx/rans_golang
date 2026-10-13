package codec

import (
	"errors"
	"testing"
)

// writePoint 演示类型：字段 1=Name(string)、2=Score(varint)。
func writePoint(name string, score int64) []byte {
	w := NewWriter()
	w.BeginObject(2)
	w.WriteFieldString(1, name)
	w.WriteFieldVarInt(2, score)
	return w.Bytes()
}

func TestFieldTagShape(t *testing.T) {
	// tag 数值即契约：varint(fieldId<<4 | wireType)（规格 §7.3）。
	if got := FieldTag(0xABC, WireString); got != 0xABC5 {
		t.Fatalf("FieldTag = %#x, want 0xabc5", got)
	}
	if got := FieldTag(MaxFieldID, WireDictionary); got != uint64(MaxFieldID)<<4|13 {
		t.Fatalf("max field tag = %#x", got)
	}
}

func TestReadFieldTagRejectsFieldIDOverflow(t *testing.T) {
	// fieldId 超出 MaxFieldID 的 tag 视为畸形，不可静默截断。
	w := NewWriter()
	w.WriteVarUInt(uint64(MaxFieldID+1) << 4)
	r := NewReader(w.Bytes())
	if _, _, err := r.ReadFieldTag(); err != ErrMalformed {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
}

func TestBeginObjectNull(t *testing.T) {
	// 负计数 = null 对象语义，统一归一为 -1（规格 §7.3：集合计数 <0 = null）。
	for _, v := range []int64{-1, -5} {
		w := NewWriter()
		w.WriteVarInt(v)
		r := NewReader(w.Bytes())
		count, err := r.BeginObject()
		if err != nil || count != -1 {
			t.Fatalf("v=%d: count=%d err=%v", v, count, err)
		}
	}
}

func TestObjectRoundTrip(t *testing.T) {
	b := writePoint("alice", 42)
	r := NewReader(b)
	count, err := r.BeginObject()
	if err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	var name string
	var score int64
	for i := 0; i < count; i++ {
		id, wt, err := r.ReadFieldTag()
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case id == 1 && wt == WireString:
			if name, err = r.ReadString(); err != nil {
				t.Fatal(err)
			}
		case id == 2 && wt == WireVarInt:
			if score, err = r.ReadVarInt(); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unexpected field id=%d wt=%d", id, wt)
		}
	}
	if name != "alice" || score != 42 {
		t.Fatalf("name=%q score=%d", name, score)
	}
	if r.Remaining() != 0 {
		t.Fatalf("remaining=%d, want 0", r.Remaining())
	}
}

// 演进语义一：未知 fieldId 按 wireType 跳过（新增字段旧读者取默认，规格 §7.3）。
func TestEvolutionUnknownFieldSkipped(t *testing.T) {
	w := NewWriter()
	w.BeginObject(4)
	w.WriteFieldString(99, "future-field") // 未知 id：跳过
	w.WriteFieldVarInt(1, 7)               // 已知
	w.WriteFieldString(1, "known")         // id 已知但线型不符 + id 重复出现：均合法，跳过
	w.WriteFieldVarInt(2, 100)
	r := NewReader(w.Bytes())
	count, err := r.BeginObject()
	if err != nil {
		t.Fatal(err)
	}
	var v1, v2 int64
	for i := 0; i < count; i++ {
		id, wt, err := r.ReadFieldTag()
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case id == 1 && wt == WireVarInt:
			if v1, err = r.ReadVarInt(); err != nil {
				t.Fatal(err)
			}
		case id == 2 && wt == WireVarInt:
			if v2, err = r.ReadVarInt(); err != nil {
				t.Fatal(err)
			}
		default:
			if err := r.SkipField(wt); err != nil {
				t.Fatal(err)
			}
		}
	}
	if v1 != 7 || v2 != 100 {
		t.Fatalf("v1=%d v2=%d", v1, v2)
	}
}

// 演进语义二：缺失字段取默认（删除字段，规格 §7.3）。
func TestEvolutionMissingFieldDefaults(t *testing.T) {
	w := NewWriter()
	w.BeginObject(1)
	w.WriteFieldString(1, "only")
	r := NewReader(w.Bytes())
	count, err := r.BeginObject()
	if err != nil {
		t.Fatal(err)
	}
	var score int64 = -1 // 读者默认
	for i := 0; i < count; i++ {
		id, wt, err := r.ReadFieldTag()
		if err != nil {
			t.Fatal(err)
		}
		if id == 2 && wt == WireVarInt {
			if score, err = r.ReadVarInt(); err != nil {
				t.Fatal(err)
			}
		} else if err := r.SkipField(wt); err != nil {
			t.Fatal(err)
		}
	}
	if score != -1 {
		t.Fatal("missing field must keep default")
	}
}

// 演进语义三：wireType 不匹配跳过（类型变更，规格 §7.3）。
func TestEvolutionWireTypeMismatchSkipped(t *testing.T) {
	w := NewWriter()
	w.BeginObject(2)
	w.WriteFieldString(1, "now-a-string") // 读者期待 field1 是 varint
	w.WriteFieldVarInt(2, 9)
	r := NewReader(w.Bytes())
	count, err := r.BeginObject()
	if err != nil {
		t.Fatal(err)
	}
	var v2 int64
	for i := 0; i < count; i++ {
		id, wt, err := r.ReadFieldTag()
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case id == 1 && wt == WireVarInt:
			t.Fatal("unexpected")
		case id == 2 && wt == WireVarInt:
			if v2, err = r.ReadVarInt(); err != nil {
				t.Fatal(err)
			}
		default:
			if err := r.SkipField(wt); err != nil {
				t.Fatal(err)
			}
		}
	}
	if v2 != 9 {
		t.Fatal("mismatched field must be skipped, others intact")
	}
}

// 陷阱 8：count 与实际字段不符必须报错，不可静默容忍。
func TestObjectCountMismatchErrors(t *testing.T) {
	w := NewWriter()
	w.BeginObject(5) // 声明 5 个字段
	w.WriteFieldVarInt(1, 1)
	r := NewReader(w.Bytes())
	count, err := r.BeginObject()
	if err != nil || count != 5 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	// 第 1 个字段真实存在：tag 与值都消费掉。
	if _, wt, err := r.ReadFieldTag(); err != nil || wt != WireVarInt {
		t.Fatalf("first tag: wt=%d err=%v", wt, err)
	}
	if _, err := r.ReadVarInt(); err != nil {
		t.Fatal(err)
	}
	// 第 2 个字段起缓冲已耗尽：必须报错。
	if _, _, err := r.ReadFieldTag(); err == nil {
		t.Fatal("exhausted object must error, not silently tolerate")
	}
}

func TestSkipFieldAllWireTypes(t *testing.T) {
	// 构造覆盖全部 14 种线型的字段流，全部 SkipField 后哨兵字段仍可正常读取。
	pay := func(build func(*Writer)) []byte {
		pw := NewWriter()
		build(pw)
		return pw.Bytes()
	}
	cat := func(parts ...[]byte) []byte {
		var out []byte
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	inner := func() []byte { // 嵌套对象：自身也是 字段数 + 字段流
		iw := NewWriter()
		iw.BeginObject(2)
		iw.WriteFieldString(1, "inner")
		iw.WriteFieldVarInt(2, -9)
		return iw.Bytes()
	}
	nullMsg := pay(func(pw *Writer) { pw.WriteVarInt(-1) }) // null 对象计数
	seqOfVarInt := pay(func(pw *Writer) {
		pw.WriteVarUInt(uint64(WireVarInt)) // 元素线型
		pw.WriteVarInt(3)                   // count
		pw.WriteVarInt(10)
		pw.WriteVarInt(20)
		pw.WriteVarInt(30)
	})
	seqOfMessage := cat( // 元素为嵌套对象：集合→对象→字段 递归跳过
		pay(func(pw *Writer) {
			pw.WriteVarUInt(uint64(WireMessage))
			pw.WriteVarInt(2) // count
		}),
		inner(), inner(),
	)
	nullSeq := pay(func(pw *Writer) {
		pw.WriteVarUInt(uint64(WireFixed64))
		pw.WriteVarInt(-1) // null 集合（规格 §7.3：count<0）
	})
	dict := pay(func(pw *Writer) {
		pw.WriteVarUInt(uint64(WireString)) // keyWire
		pw.WriteVarUInt(uint64(WireVarInt)) // valWire
		pw.WriteVarInt(2)                   // 对数
		pw.WriteString("a")
		pw.WriteVarInt(1)
		pw.WriteString("b")
		pw.WriteVarInt(2)
	})
	nullDict := pay(func(pw *Writer) {
		pw.WriteVarUInt(uint64(WireVarInt))
		pw.WriteVarUInt(uint64(WireVarInt))
		pw.WriteVarInt(-1)
	})

	w := NewWriter()
	w.BeginObject(23)
	w.WriteFieldVarInt(1, -5)
	w.WriteFieldRaw(2, WireFixed8, pay(func(pw *Writer) { pw.WriteFixed8(0xAB) }))
	w.WriteFieldRaw(3, WireFixed16, pay(func(pw *Writer) { pw.WriteFixed16(0xCDEF) }))
	w.WriteFieldRaw(4, WireFixed32, pay(func(pw *Writer) { pw.WriteFixed32(0x01020304) }))
	w.WriteFieldFixed64(5, 1<<40)
	w.WriteFieldString(6, "s")
	w.WriteFieldBytes(7, []byte{9, 9})
	w.WriteFieldRaw(8, WireNullableFixed8, pay(func(pw *Writer) { pw.WriteFixed8(0) }))                       // null
	w.WriteFieldRaw(9, WireNullableFixed8, pay(func(pw *Writer) { pw.WriteFixed8(1); pw.WriteFixed8(0x5A) })) // 非 null
	w.WriteFieldRaw(10, WireNullableFixed32, pay(func(pw *Writer) { pw.WriteFixed8(0) }))
	w.WriteFieldRaw(11, WireNullableFixed32, pay(func(pw *Writer) { pw.WriteFixed8(1); pw.WriteFixed32(7) }))
	w.WriteFieldRaw(12, WireNullableFixed64, pay(func(pw *Writer) { pw.WriteFixed8(0) }))
	w.WriteFieldRaw(13, WireNullableFixed64, pay(func(pw *Writer) { pw.WriteFixed8(1); pw.WriteFixed64(8) }))
	w.WriteFieldRaw(14, WireMessage, inner())
	w.WriteFieldRaw(15, WireMessage, nullMsg)
	w.WriteFieldRaw(16, WireNullableMessage, pay(func(pw *Writer) { pw.WriteFixed8(0) })) // null
	w.WriteFieldRaw(17, WireNullableMessage, cat(pay(func(pw *Writer) { pw.WriteFixed8(1) }), inner()))
	w.WriteFieldRaw(18, WireSequence, seqOfVarInt)
	w.WriteFieldRaw(19, WireSequence, seqOfMessage)
	w.WriteFieldRaw(20, WireSequence, nullSeq)
	w.WriteFieldRaw(21, WireDictionary, dict)
	w.WriteFieldRaw(22, WireDictionary, nullDict)
	w.WriteFieldVarInt(23, 77) // 哨兵：跳过一切后继续读取必须完好

	r := NewReader(w.Bytes())
	count, err := r.BeginObject()
	if err != nil || count != 23 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	skipped := 0
	var sentinel int64
	for i := 0; i < count; i++ {
		id, wt, err := r.ReadFieldTag()
		if err != nil {
			t.Fatal(err)
		}
		if id == 23 && wt == WireVarInt {
			if sentinel, err = r.ReadVarInt(); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := r.SkipField(wt); err != nil {
			t.Fatalf("skip id=%d wt=%d: %v", id, wt, err)
		}
		skipped++
	}
	if sentinel != 77 {
		t.Fatalf("post-skip read broken: sentinel=%d", sentinel)
	}
	if skipped != 22 {
		t.Fatalf("skipped=%d, want 22", skipped)
	}
	if r.Remaining() != 0 {
		t.Fatalf("remaining=%d, want 0", r.Remaining())
	}
}

func TestSkipFieldRejectsUnknownWireType(t *testing.T) {
	// 4bit 线型域中 14/15 未定义：跳过必须报错而非静默。
	if err := NewReader(nil).SkipField(WireType(14)); err != ErrMalformed {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
}

// nestedMessageBytes 构造 depth 层嵌套 Message（每层单字段，字段值是下一层
// 对象的字节；最内层一个 varint 字段）——SkipField 递归深度的攻击面形状。
func nestedMessageBytes(t *testing.T, depth int) []byte {
	t.Helper()
	inner := NewWriter()
	inner.BeginObject(1)
	inner.WriteFieldVarInt(1, 7)
	cur := inner.Bytes()
	for i := 0; i < depth; i++ {
		w := NewWriter()
		w.BeginObject(1)
		w.WriteFieldRaw(1, WireMessage, cur)
		cur = w.Bytes()
	}
	return cur
}

// TestSkipFieldDepthCap：递归跳过深度受 MaxSkipFieldDepth 封顶（威胁模型
// 与资源界口径见该常量注释）——浅嵌套照常跳过，超深报 ErrMalformed。
func TestSkipFieldDepthCap(t *testing.T) {
	// 合法浅嵌套（10 层）：照常跳过。
	r := NewReader(nestedMessageBytes(t, 10))
	if _, err := r.BeginObject(); err != nil {
		t.Fatalf("begin object: %v", err)
	}
	if _, wt, err := r.ReadFieldTag(); err != nil {
		t.Fatalf("read tag: %v", err)
	} else if err := r.SkipField(wt); err != nil {
		t.Fatalf("shallow nesting must skip cleanly: %v", err)
	}

	// 超深嵌套（上限 + 10 层）：ErrMalformed，不再无限递归。
	r = NewReader(nestedMessageBytes(t, MaxSkipFieldDepth+10))
	if _, err := r.BeginObject(); err != nil {
		t.Fatalf("begin object: %v", err)
	}
	if _, wt, err := r.ReadFieldTag(); err != nil {
		t.Fatalf("read tag: %v", err)
	} else if err := r.SkipField(wt); !errors.Is(err, ErrMalformed) {
		t.Fatalf("deep nesting must be rejected as ErrMalformed, got %v", err)
	}
}
