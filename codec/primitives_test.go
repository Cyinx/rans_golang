package codec

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func TestVarUIntRoundTrip(t *testing.T) {
	vals := []uint64{0, 1, 127, 128, 300, 1<<32 - 1, math.MaxUint64}
	for _, v := range vals {
		w := NewWriter()
		w.WriteVarUInt(v)
		r := NewReader(w.Bytes())
		got, err := r.ReadVarUInt()
		if err != nil || got != v {
			t.Fatalf("varuint %d: got %d err %v", v, got, err)
		}
	}
}

func TestVarIntZigZagRoundTrip(t *testing.T) {
	vals := []int64{0, -1, 1, -64, 63, math.MaxInt64, math.MinInt64}
	for _, v := range vals {
		w := NewWriter()
		w.WriteVarInt(v)
		r := NewReader(w.Bytes())
		got, err := r.ReadVarInt()
		if err != nil || got != v {
			t.Fatalf("varint %d: got %d err %v", v, got, err)
		}
	}
	// ZigZag 编码形状锁定：0→0x00, -1→0x01, 1→0x02（LEB128 带符号规格 §7.3）。
	w := NewWriter()
	w.WriteVarInt(-1)
	if !bytes.Equal(w.Bytes(), []byte{0x01}) {
		t.Fatalf("zigzag(-1) = %x, want 01", w.Bytes())
	}
}

func TestFixedRoundTrip(t *testing.T) {
	w := NewWriter()
	w.WriteFixed8(0xAB)
	w.WriteFixed16(0xCDEF)
	w.WriteFixed32(0x01020304)
	w.WriteFixed64(0x0102030405060708)
	r := NewReader(w.Bytes())
	if v, _ := r.ReadFixed8(); v != 0xAB {
		t.Fatal("fixed8")
	}
	if v, _ := r.ReadFixed16(); v != 0xCDEF {
		t.Fatal("fixed16")
	}
	if v, _ := r.ReadFixed32(); v != 0x01020304 {
		t.Fatal("fixed32")
	}
	if v, _ := r.ReadFixed64(); v != 0x0102030405060708 {
		t.Fatal("fixed64")
	}
}

func TestStringBytesNullLen(t *testing.T) {
	w := NewWriter()
	w.WriteString("hello")
	w.WriteBytes([]byte{1, 2, 3})
	w.WriteNullLen()
	r := NewReader(w.Bytes())
	if s, err := r.ReadString(); err != nil || s != "hello" {
		t.Fatalf("string: %q err %v", s, err)
	}
	if b, err := r.ReadBytes(); err != nil || !bytes.Equal(b, []byte{1, 2, 3}) {
		t.Fatalf("bytes: %v err %v", b, err)
	}
	n, isNull, err := r.ReadLen()
	if err != nil || !isNull || n != -1 {
		t.Fatalf("null len: n=%d isNull=%v err=%v", n, isNull, err)
	}
}

func TestReaderTruncated(t *testing.T) {
	w := NewWriter()
	w.WriteFixed64(1)
	r := NewReader(w.Bytes()[:3])
	if _, err := r.ReadFixed64(); err != ErrTruncated {
		t.Fatalf("want ErrTruncated, got %v", err)
	}
	r2 := NewReader([]byte{0xFF}) // 不完整的 uvarint
	if _, err := r2.ReadVarUInt(); err != ErrTruncated {
		t.Fatalf("want ErrTruncated, got %v", err)
	}
}

func TestReadBytesRejectsLenBeyondBuffer(t *testing.T) {
	// 陷阱 10 同型：长度声明不可信，校验先于分配。
	w := NewWriter()
	w.WriteVarInt(1 << 20) // 长度 1MiB
	r := NewReader(w.Bytes())
	if _, err := r.ReadBytes(); err != ErrTruncated {
		t.Fatalf("want ErrTruncated, got %v", err)
	}
}

func TestVarUIntRejectsOverflow(t *testing.T) {
	// 超过 64bit 的 uvarint 必须报错，不可回绕。
	w := NewWriter()
	for i := 0; i < 11; i++ {
		w.WriteFixed8(0xFF)
	}
	r := NewReader(w.Bytes())
	if _, err := r.ReadVarUInt(); err != ErrMalformed {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
}

// TestWriteLenRejectsNegative：负长度域（int32 回绕形态）不得静默入帧，
// 显式 panic——完整契约见 Writer.WriteLen 注释。
func TestWriteLenRejectsNegative(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("WriteLen(negative) 必须显式失败（防 int32 截断静默入帧）")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "int32") {
			t.Fatalf("panic = %v, want 含 int32 域说明", r)
		}
	}()
	w := NewWriter()
	w.WriteLen(-5) // 2GiB 长度回绕后的典型形态
}
