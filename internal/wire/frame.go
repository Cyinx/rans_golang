package wire

import (
	"encoding/binary"
	"github.com/cyinx/rans_golang/codec"
	"io"
)

// 帧上限（规格 §7.1）：MaxFrameSize 封顶整帧（含 8B 长度前缀），
// MaxHeaderSize 为 pre-auth 栈溢出封顶——头部先于认证落解析路径。
const (
	MaxFrameSize  = 4 << 20  // 4MiB
	MaxHeaderSize = 64 << 10 // 64KiB
)

// EncodeFrame 编码一帧：[HeaderLength 4B LE][BodyLength 4B LE][Header][Body]。
// header 必须非空且 ≤MaxHeaderSize；整帧 ≤MaxFrameSize（与解码端同口径，
// 保证本仓编码出的帧必可解码）。超限返回 codec.ErrMalformed。
func EncodeFrame(header, body []byte) ([]byte, error) {
	if len(header) == 0 || len(header) > MaxHeaderSize {
		return nil, codec.ErrMalformed
	}
	if int64(len(header))+int64(len(body))+8 > MaxFrameSize {
		return nil, codec.ErrMalformed
	}
	out := make([]byte, 8, 8+len(header)+len(body))
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(header)))
	binary.LittleEndian.PutUint32(out[4:8], uint32(len(body)))
	out = append(out, header...)
	out = append(out, body...)
	return out, nil
}

// FrameDecoder 流式帧解码器：从 r 逐帧读取，body 拷入池化页。
type FrameDecoder struct {
	r    io.Reader
	pool *PagePool
}

func NewFrameDecoder(r io.Reader, pool *PagePool) *FrameDecoder {
	return &FrameDecoder{r: r, pool: pool}
}

// Next 读取一帧。长度校验先于一切分配（陷阱 10：不可信长度先封顶再动内存，
// 杜绝 DoS 放大面）。body 拷入池化页（Get 返回 len==bl、cap≥bl，与连接读缓冲
// 生命周期解耦，规格 §7.1），所有权随返回转移给调用方——用完必须 pool.Put
// 归还；半截 body 的失败路径在内部归还，不外泄。io.EOF 为干净收尾，半截
// 长度前缀/截断载荷返回 ErrUnexpectedEOF，畸形长度返回 codec.ErrMalformed。
func (d *FrameDecoder) Next() (header, body []byte, err error) {
	var lens [8]byte
	if _, err := io.ReadFull(d.r, lens[:]); err != nil {
		return nil, nil, err // 0 字节残余=io.EOF；半截前缀=ErrUnexpectedEOF
	}
	hl := binary.LittleEndian.Uint32(lens[0:4])
	bl := binary.LittleEndian.Uint32(lens[4:8])
	// 校验先于分配：零长头视为畸形（外封必为非空对象），超限/整帧超 4MiB 拒收。
	if hl == 0 || hl > MaxHeaderSize || bl > MaxFrameSize || int64(hl)+int64(bl)+8 > MaxFrameSize {
		return nil, nil, codec.ErrMalformed
	}
	header = make([]byte, hl)
	if _, err := io.ReadFull(d.r, header); err != nil {
		return nil, nil, err
	}
	body = d.pool.Get(int(bl)) // len==bl：ReadFull 就位，无需再切片
	if _, err := io.ReadFull(d.r, body); err != nil {
		d.pool.Put(body) // 半截 body：页就地归还，保持获取-归还收支平衡
		return nil, nil, err
	}
	return header, body, nil
}
