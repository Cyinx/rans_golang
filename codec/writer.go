package codec

import (
	"encoding/binary"
	"fmt"
)

// Writer 顺序写缓冲；错误状态由调用方在更上层裁决，写路径不返回错误。
// 并发约定（spec §5.8）：全部方法不阻塞（纯内存追加）；非并发安全（单
// 编码方持有——值语义使用面）；与运行时实体面无关。
type Writer struct{ buf []byte }

// NewWriter 构造写缓冲（初始容量 64）。
func NewWriter() *Writer { return &Writer{buf: make([]byte, 0, 64)} }

// Bytes 返回已写入字节（内部缓冲引用，只读使用安全；跨调用写持有须拷贝）。
func (w *Writer) Bytes() []byte { return w.buf }

// WriteRaw 原样追加字节（协议定长域的转义口：Nonce/MAC 等无长度前缀的
// 定长段；普通字段一律走 WriteField* 对象原语）。
func (w *Writer) WriteRaw(b []byte) { w.buf = append(w.buf, b...) }

// Len 返回已写入字节数。
func (w *Writer) Len() int { return len(w.buf) }

// WriteVarUInt LEB128 无符号 varint：低 7 位一组，最高位 continuation。
func (w *Writer) WriteVarUInt(v uint64) {
	for v >= 0x80 {
		w.buf = append(w.buf, byte(v)|0x80)
		v >>= 7
	}
	w.buf = append(w.buf, byte(v))
}

// WriteVarInt 带符号 varint，ZigZag 编码（规格 §7.3）。
func (w *Writer) WriteVarInt(v int64) { w.WriteVarUInt(uint64(v<<1) ^ uint64(v>>63)) }

// WriteFixed8 写 1 字节小端定长域。
func (w *Writer) WriteFixed8(v uint8) { w.buf = append(w.buf, v) }

// WriteFixed16 写 2 字节小端定长域。
func (w *Writer) WriteFixed16(v uint16) { w.buf = binary.LittleEndian.AppendUint16(w.buf, v) }

// WriteFixed32 写 4 字节小端定长域。
func (w *Writer) WriteFixed32(v uint32) { w.buf = binary.LittleEndian.AppendUint32(w.buf, v) }

// WriteFixed64 写 8 字节小端定长域。
func (w *Writer) WriteFixed64(v uint64) { w.buf = binary.LittleEndian.AppendUint64(w.buf, v) }

// WriteLen 长度域：ZigZag varint，只辖非负长度（null 哨兵走 WriteNullLen，
// 规格 §7.3）。负值是 >2GiB 长度经 int32 强转回绕的形态（或调用方 bug）
// ——静默入帧会被对端按 null 哨兵/非法长度解读。写路径不
// 返回错误（错误状态由上层裁决），越域属编程错误，显式 panic；网络路径
// 上不可达（4MiB 帧上限在 EncodeFrame 先行拒绝）。
func (w *Writer) WriteLen(n int32) {
	if n < 0 {
		panic(fmt.Sprintf("rans: write: negative length field %d (int32 truncation of >2GiB length or caller bug); null is written via WriteNullLen", n))
	}
	w.WriteVarInt(int64(n))
}

// WriteNullLen 写 null 长度域（ZigZag varint 的 -1，规格 §7.3）。
func (w *Writer) WriteNullLen() { w.WriteVarInt(-1) }

// WriteString / WriteBytes：长度域 + 原始字节。
func (w *Writer) WriteString(s string) {
	w.WriteLen(int32(len(s)))
	w.buf = append(w.buf, s...)
}

func (w *Writer) WriteBytes(b []byte) {
	w.WriteLen(int32(len(b)))
	w.buf = append(w.buf, b...)
}
