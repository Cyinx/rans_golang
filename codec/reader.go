package codec

import (
	"encoding/binary"
	"math"
)

// Reader 顺序读缓冲。读失败即终态：调用方应中止本次解码，
// 出错后的偏移位置不作保证（无断点续读语义）。
// 并发约定（spec §5.8）：全部方法不阻塞（纯内存缓冲读，错误即刻返回）；
// 非并发安全（单解码方持有——值语义使用面）；与运行时实体面无关。
type Reader struct {
	buf []byte
	off int
}

// NewReader 构造读缓冲（b 由调用方持有，Reader 不拷贝）。
func NewReader(b []byte) *Reader { return &Reader{buf: b} }

// Remaining 返回尚未消费的字节数。
func (r *Reader) Remaining() int { return len(r.buf) - r.off }

// ReadRaw 读 n 字节定长域（Nonce/MAC 等无长度前缀的定长段；普通字段一律
// 走 field-tag 对象原语）。返回切片引用底层缓冲：需跨缓冲持有时由调用方
// 拷贝。不足 n 字节报 ErrTruncated，读指针不动。
func (r *Reader) ReadRaw(n int) ([]byte, error) {
	if n < 0 || r.Remaining() < n {
		return nil, ErrTruncated
	}
	b := r.buf[r.off : r.off+n]
	r.off += n
	return b, nil
}

// ReadVarUInt LEB128 无符号 varint。超过 10 字节或第 10 字节超出
// 64bit 可表示范围（回绕）返回 ErrMalformed；字节不足返回 ErrTruncated。
func (r *Reader) ReadVarUInt() (uint64, error) {
	var x uint64
	var s uint
	for i := 0; ; i++ {
		if i == 10 {
			return 0, ErrMalformed // 第 11 字节仍在 continuation：必超 64bit
		}
		if r.off >= len(r.buf) {
			return 0, ErrTruncated
		}
		b := r.buf[r.off]
		r.off++
		if b < 0x80 {
			if i == 9 && b > 1 {
				return 0, ErrMalformed // 末字节最多 1 个有效位
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}

// ReadVarInt 带符号 varint，ZigZag 解码。
func (r *Reader) ReadVarInt() (int64, error) {
	x, err := r.ReadVarUInt()
	if err != nil {
		return 0, err
	}
	return int64(x>>1) ^ -int64(x&1), nil
}

// ReadFixed8 读 1 字节小端定长域。
func (r *Reader) ReadFixed8() (uint8, error) {
	if r.Remaining() < 1 {
		return 0, ErrTruncated
	}
	v := r.buf[r.off]
	r.off++
	return v, nil
}

// ReadFixed16 读 2 字节小端定长域。
func (r *Reader) ReadFixed16() (uint16, error) {
	if r.Remaining() < 2 {
		return 0, ErrTruncated
	}
	v := binary.LittleEndian.Uint16(r.buf[r.off:])
	r.off += 2
	return v, nil
}

// ReadFixed32 读 4 字节小端定长域。
func (r *Reader) ReadFixed32() (uint32, error) {
	if r.Remaining() < 4 {
		return 0, ErrTruncated
	}
	v := binary.LittleEndian.Uint32(r.buf[r.off:])
	r.off += 4
	return v, nil
}

// ReadFixed64 读 8 字节小端定长域。
func (r *Reader) ReadFixed64() (uint64, error) {
	if r.Remaining() < 8 {
		return 0, ErrTruncated
	}
	v := binary.LittleEndian.Uint64(r.buf[r.off:])
	r.off += 8
	return v, nil
}

// ReadLen 长度域：ZigZag varint，-1 表示 null（规格 §7.3）。
// 长度域是 int32 语义，超出 int32 域或小于 -1 视为畸形。
func (r *Reader) ReadLen() (int32, bool, error) {
	v, err := r.ReadVarInt()
	if err != nil {
		return 0, false, err
	}
	if v == -1 {
		return -1, true, nil
	}
	if v < -1 || v > math.MaxInt32 {
		return 0, false, ErrMalformed
	}
	return int32(v), false, nil
}

// ReadString 读长度域 + 原始字节并转 string（转拷不可避免）。
// null 长度域返回零值 string。
func (r *Reader) ReadString() (string, error) {
	n, isNull, err := r.ReadLen()
	if err != nil {
		return "", err
	}
	if isNull {
		return "", nil
	}
	if int64(n) > int64(r.Remaining()) {
		return "", ErrTruncated // 校验先于分配（陷阱 10）
	}
	s := string(r.buf[r.off : r.off+int(n)])
	r.off += int(n)
	return s, nil
}

// ReadBytes 读长度域 + 原始字节。返回切片直接引用底层缓冲（零拷贝）；
// 需跨缓冲持有时由调用方决定拷贝——帧解码路径会拷入池化页。
// null 长度域返回 nil。
func (r *Reader) ReadBytes() ([]byte, error) {
	n, isNull, err := r.ReadLen()
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	if int64(n) > int64(r.Remaining()) {
		return nil, ErrTruncated // 校验先于分配（陷阱 10）
	}
	b := r.buf[r.off : r.off+int(n)]
	r.off += int(n)
	return b, nil
}
