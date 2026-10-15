package wire

// 握手 v4（规格 §10.2 + §6.1 H9）：版本区间协商 +
// 可扩展握手体 + 摘要降级为观测。v3 → v4 是同版本断点（无兼容
// 读取：同集群双端同版本，升级先停全部再升——注册/成员语义随 v4 变更，
// node.Start 按成员键协议区间拒绝混版本启动）；此后同一 VerMax 内向后
// 兼容：新字段只追加字段号、旧端跳过；破坏性变更提升 VerMin 并在
// CHANGELOG 标「滚动不兼容」。

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"

	"github.com/cyinx/rans_golang/codec"
)

// HandshakeVersionMin/Max 本端支持的握手协议版本闭区间（协商按区间——
// 双端区间相交即可互通，同一 VerMax 内的增量演进不升级版本）。
const (
	HandshakeVersionMin uint16 = 5
	HandshakeVersionMax uint16 = 5
)

// HandshakeCompatible 报告对端区间 [min, max] 与本端支持区间是否相交
// （成员键协议区间的启动代际检查用；缺字段形态 0/0 恒不相交——旧版本
// 节点按不兼容处理）。
func HandshakeCompatible(min, max uint16) bool {
	return min <= HandshakeVersionMax && max >= HandshakeVersionMin
}

// 握手扩展对象字段号正本（只追加不复用）：新字段只追加字段号，旧端按线型跳过。
const (
	hsFieldNodeID       uint32 = 1 // fixed16
	hsFieldClusterName  uint32 = 2 // string
	hsFieldSchemaDigest uint32 = 3 // fixed64
)

// 握手校验错误（校验序：版本区间相交[在 DecodeHandshake] → 集群名 →
// HMAC → SchemaDigest；文本供测试与上层日志断言）。
var (
	ErrHandshakeCluster = errors.New("rans: handshake: cluster mismatch")
	ErrHandshakeVersion = errors.New("rans: handshake: version range incompatible")
	ErrHandshakeSchema  = errors.New("rans: handshake: schema digest mismatch")
	ErrHandshakeMAC     = errors.New("rans: handshake: MAC mismatch")
)

// Handshake 握手消息体（规格 §10.2 v3 Body 布局）：
//
//	[VerMin 2B LE][VerMax 2B LE][扩展对象][Nonce 16B][MAC 32B?]
//	扩展对象（field-tag，计数终结）：1=NodeID(fixed16) 2=ClusterName(string)
//	3=SchemaDigest(fixed64)，未知字段号跳过。
//
// MAC 有无即对端是否配置密钥的声明：Encode(nil) 不写、DecodeHandshake 按
// 剩余长度自洽判定。版本区间不进结构体——DecodeHandshake 已拒绝区间不相交
// 的握手体，解码成功的 Handshake 版本可协商即契约锁定。authed 为 MAC 之前
// 的整个握手体前缀（解码期拷贝脱离输入缓冲）：MAC = HMAC-SHA256(secret,
// 该前缀)——版本区间与扩展对象全部受认证（指纹/版本不可被篡改以降级
// 或借严格模式拒连）。
type Handshake struct {
	NodeID       uint16
	ClusterName  string
	SchemaDigest uint64
	Nonce        [16]byte
	MAC          []byte

	authed []byte // MAC 认证覆盖的前缀字节（= 整个握手体去掉 MAC 段；解码期拷贝）
}

// Encode 编码握手体。secret 非 nil 时计算并追加 32B MAC（对 MAC 之前的
// 整个握手体——版本区间与扩展对象全部受认证）；nil（未配置）不写 MAC——
// 空而非 nil 的 secret 视为已配置空钥，照常计算（nil/空二分是"是否启用
// 认证"的声明，不是密钥内容）。
func (h *Handshake) Encode(secret []byte) []byte {
	w := codec.NewWriter()
	w.WriteFixed16(HandshakeVersionMin)
	w.WriteFixed16(HandshakeVersionMax)
	w.BeginObject(3)
	w.WriteTag(hsFieldNodeID, codec.WireFixed16)
	w.WriteFixed16(h.NodeID)
	w.WriteFieldString(hsFieldClusterName, h.ClusterName)
	w.WriteFieldFixed64(hsFieldSchemaDigest, h.SchemaDigest)
	w.WriteRaw(h.Nonce[:])
	if secret != nil {
		m := hmac.New(sha256.New, secret)
		m.Write(w.Bytes()) // MAC 覆盖此前整个握手体
		w.WriteRaw(m.Sum(nil))
	}
	return w.Bytes()
}

// DecodeHandshake 解码握手体。版本区间与本端支持区间不相交 →
// ErrHandshakeVersion（pre-auth 第一道门禁，先于一切字段语义）；区间倒置 /
// null 扩展对象 / 剩余长度与 MAC 不自洽 → codec.ErrMalformed / ErrTruncated。
// 未知扩展字段按线型跳过（此后新字段只追加字段号的前向兼容承重面）。
// MAC 与认证前缀均拷贝脱离输入缓冲（握手体常驻连接状态）。
func DecodeHandshake(b []byte) (Handshake, error) {
	r := codec.NewReader(b)
	verMin, err := r.ReadFixed16()
	if err != nil {
		return Handshake{}, err
	}
	verMax, err := r.ReadFixed16()
	if err != nil {
		return Handshake{}, err
	}
	if verMin > verMax {
		return Handshake{}, codec.ErrMalformed
	}
	if !versionsIntersect(verMin, verMax, HandshakeVersionMin, HandshakeVersionMax) {
		return Handshake{}, ErrHandshakeVersion
	}
	var h Handshake
	count, err := r.BeginObject()
	if err != nil {
		return Handshake{}, err
	}
	if count < 0 {
		return Handshake{}, codec.ErrMalformed // 握手扩展对象必为非空容器语义
	}
	for i := 0; i < count; i++ {
		id, wt, err := r.ReadFieldTag()
		if err != nil {
			return Handshake{}, err
		}
		switch {
		case id == hsFieldNodeID && wt == codec.WireFixed16:
			if h.NodeID, err = r.ReadFixed16(); err != nil {
				return Handshake{}, err
			}
		case id == hsFieldClusterName && wt == codec.WireString:
			if h.ClusterName, err = r.ReadString(); err != nil {
				return Handshake{}, err
			}
		case id == hsFieldSchemaDigest && wt == codec.WireFixed64:
			if h.SchemaDigest, err = r.ReadFixed64(); err != nil {
				return Handshake{}, err
			}
		default:
			if err := r.SkipField(wt); err != nil { // 未知字段号（或已知号未知线型）跳过
				return Handshake{}, err
			}
		}
	}
	nonce, err := readFixed(r, 16)
	if err != nil {
		return Handshake{}, err
	}
	copy(h.Nonce[:], nonce)
	// 认证前缀 = 至此为止的整个握手体（MAC 段之前）——解码期拷贝。
	h.authed = append([]byte(nil), b[:len(b)-r.Remaining()]...)
	switch r.Remaining() {
	case 0: // 无 MAC：对端未配置密钥的兼容形态
	case sha256.Size:
		view, err := readFixed(r, sha256.Size)
		if err != nil {
			return Handshake{}, err
		}
		h.MAC = append([]byte(nil), view...) // 拷贝脱离输入缓冲（握手体常驻连接状态）
	default:
		return Handshake{}, codec.ErrMalformed
	}
	return h, nil
}

// Validate 校验入站握手（规格 §10.2 v3 校验序：版本区间相交[已在
// DecodeHandshake] → 集群名 → HMAC → SchemaDigest）。集群名按 Ordinal
// （字节精确）比较；secret 为 nil → 跳过 MAC 校验（nil/非 nil 的策略归
// 传输层：tcp 以 Config.NoHMAC 显式 opt-in 且默认 fail-closed，本层不设
// 策略；跳过时的告警亦由调用方负责），secret 非 nil 而对端未带 MAC → 拒
// （红组④：配置密钥的端不接受裸握手）。digest 失配在 strict=false 时放行
// （观测模式：告警与计数归传输层），strict=true 时 ErrHandshakeSchema 拒连
// （此时摘要已受 MAC 保护，失配不可伪造）。比较用 hmac.Equal 常量时间
// （规格 §10.2，防时序侧信道）。
func (h *Handshake) Validate(cluster string, digest uint64, secret []byte, strict bool) error {
	if h.ClusterName != cluster {
		return ErrHandshakeCluster
	}
	if secret != nil {
		if len(h.MAC) == 0 || len(h.authed) == 0 {
			return ErrHandshakeMAC // 对端未带 MAC（或解码产物缺认证前缀）：拒
		}
		m := hmac.New(sha256.New, secret)
		m.Write(h.authed)
		if !hmac.Equal(m.Sum(nil), h.MAC) {
			return ErrHandshakeMAC
		}
	}
	if strict && h.SchemaDigest != digest {
		return ErrHandshakeSchema
	}
	return nil
}

// versionsIntersect 报告两个版本闭区间是否相交（协商判定核心：双端支持
// 区间有公共版本即可互通）。纯判定面，表驱动测试锚点。
func versionsIntersect(aMin, aMax, bMin, bMax uint16) bool {
	return aMin <= bMax && bMin <= aMax
}

// readFixed 读 n 字节定长域（Nonce/MAC 无长度前缀）。返回切片引用底层
// 缓冲；MAC 侧需跨缓冲持有时由调用方拷贝。
func readFixed(r *codec.Reader, n int) ([]byte, error) {
	if r.Remaining() < n {
		return nil, codec.ErrTruncated
	}
	return r.ReadRaw(n)
}
