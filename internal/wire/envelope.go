package wire

import "github.com/cyinx/rans_golang/codec"

// Envelope RPC Request/Response body 统一外封：field 1=TypeName(string)、
// field 2=Payload(bytes)。TypeName 喂 DecodeValue 选路（注册名或 JSON 兜底名），
// Payload 为 EncodeValue 产出。field-tag 对象编码（计数终结），演进语义同
// 包内其他对象：未知字段按线型跳过、缺失字段取零值。
type Envelope struct {
	TypeName string
	Payload  []byte
}

// MarshalEnvelope 打包外封。纯写路径不返回错误（codec.Writer 语义）。
func MarshalEnvelope(typeName string, payload []byte) []byte {
	w := codec.NewWriter()
	w.BeginObject(2)
	w.WriteFieldString(1, typeName)
	w.WriteFieldBytes(2, payload)
	return w.Bytes()
}

// UnmarshalEnvelope 解包外封。校验（陷阱 8）：count<0（null 对象）视为畸形——
// 外封必为非空对象；声明字段数多于实际（缓冲提前耗尽）由字段循环报
// codec.ErrTruncated；消费完声明字段后仍有残余字节视为畸形（外封即整个 body，
// 尾随垃圾=帧错位）。Payload 引用输入切片（零拷贝），需跨缓冲持有由调用方拷贝。
func UnmarshalEnvelope(b []byte) (Envelope, error) {
	r := codec.NewReader(b)
	count, err := r.BeginObject()
	if err != nil {
		return Envelope{}, err
	}
	if count < 0 {
		return Envelope{}, codec.ErrMalformed
	}
	var env Envelope
	for i := 0; i < count; i++ {
		id, wt, err := r.ReadFieldTag()
		if err != nil {
			return Envelope{}, err
		}
		switch {
		case id == 1 && wt == codec.WireString:
			if env.TypeName, err = r.ReadString(); err != nil {
				return Envelope{}, err
			}
		case id == 2 && wt == codec.WireBytes:
			if env.Payload, err = r.ReadBytes(); err != nil {
				return Envelope{}, err
			}
		default:
			if err := r.SkipField(wt); err != nil {
				return Envelope{}, err
			}
		}
	}
	if r.Remaining() != 0 {
		return Envelope{}, codec.ErrMalformed
	}
	return env, nil
}
