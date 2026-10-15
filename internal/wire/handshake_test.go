package wire

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/cyinx/rans_golang/codec"
)

func mkHandshake() Handshake {
	var h Handshake
	h.NodeID = 5
	h.ClusterName = "rans-dev"
	h.SchemaDigest = 0xABCD
	rand.Read(h.Nonce[:])
	return h
}

// ---- 红组①：版本区间相交判定（表驱动纯判定 + 解码层门禁）----

// TestHandshakeVersionIntersectionTable 版本区间相交判定表（规格 §10.2 v3：
// 握手协商从精确版本改为闭区间 [VerMin,VerMax]；同 VerMax 内向后兼容）。
// 判据：两闭区间相交 ⇔ aMin ≤ bMax && bMin ≤ aMax。
func TestHandshakeVersionIntersectionTable(t *testing.T) {
	cases := []struct {
		name                   string
		aMin, aMax, bMin, bMax uint16
		want                   bool
	}{
		{"[3,3]x[3,3] intersect", 3, 3, 3, 3, true},
		{"[2,2]x[3,3] disjoint", 2, 2, 3, 3, false},
		{"[3,5]x[5,6] intersect at 5", 3, 5, 5, 6, true},
		{"[3,3]x[3,5] intersect at 3", 3, 3, 3, 5, true},
		{"[5,5]x[4,4] disjoint", 5, 5, 4, 4, false},
		{"[1,9]x[5,5] intersect", 1, 9, 5, 5, true},
	}
	for _, tc := range cases {
		if got := versionsIntersect(tc.aMin, tc.aMax, tc.bMin, tc.bMax); got != tc.want {
			t.Fatalf("%s: versionsIntersect(%d,%d,%d,%d)=%v want %v",
				tc.name, tc.aMin, tc.aMax, tc.bMin, tc.bMax, got, tc.want)
		}
	}
}

// TestHandshakeDecodeVersionGate 解码层版本门禁：对端区间与本端支持区间
// [HandshakeVersionMin, HandshakeVersionMax] 不相交 → ErrHandshakeVersion
// （pre-auth 第一道门禁，先于一切字段语义，v2 同纪律）；区间倒置 → 畸形。
func TestHandshakeDecodeVersionGate(t *testing.T) {
	mk := func(vmin, vmax uint16) []byte {
		h := mkHandshake()
		raw := h.Encode(nil)
		// 版本区间占握手体首 4 字节：[0:2]=VerMin、[2:4]=VerMax（LE）。
		raw[0] = byte(vmin)
		raw[1] = byte(vmin >> 8)
		raw[2] = byte(vmax)
		raw[3] = byte(vmax >> 8)
		return raw
	}
	// [5,5]×[5,5]：通。
	if _, err := DecodeHandshake(mk(5, 5)); err != nil {
		t.Fatalf("[5,5] must decode: %v", err)
	}
	// [5,7]×[5,5]：交于 5，通（未来对端向前兼容本端）。
	if _, err := DecodeHandshake(mk(5, 7)); err != nil {
		t.Fatalf("[5,7] must decode against [5,5]: %v", err)
	}
	// [4,4]×[5,5]：拒——ErrHandshakeVersion（v4 集群属断点前版本）。
	if _, err := DecodeHandshake(mk(4, 4)); !errors.Is(err, ErrHandshakeVersion) {
		t.Fatalf("[4,4] must be rejected with ErrHandshakeVersion, got %v", err)
	}
	// [6,6]×[5,5]：拒——对端只讲 ≥6，本端到不了。
	if _, err := DecodeHandshake(mk(6, 6)); !errors.Is(err, ErrHandshakeVersion) {
		t.Fatalf("[6,6] must be rejected with ErrHandshakeVersion, got %v", err)
	}
	// 区间倒置：畸形。
	if _, err := DecodeHandshake(mk(6, 4)); !errors.Is(err, codec.ErrMalformed) {
		t.Fatalf("inverted version range must be malformed, got %v", err)
	}
}

// ---- 基础往返与 MAC ----

func TestHandshakeRoundTripWithMAC(t *testing.T) {
	secret := []byte("s3cret")
	h := mkHandshake()
	enc := h.Encode(secret)
	got, err := DecodeHandshake(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got.NodeID != h.NodeID || got.ClusterName != h.ClusterName || got.SchemaDigest != h.SchemaDigest || got.Nonce != h.Nonce {
		t.Fatalf("field mismatch: %+v want %+v", got, h)
	}
	if err := got.Validate("rans-dev", 0xABCD, secret, false); err != nil {
		t.Fatalf("valid handshake rejected: %v", err)
	}
}

func TestHandshakeNoSecretSkipsMAC(t *testing.T) {
	h := mkHandshake()
	got, err := DecodeHandshake(h.Encode(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := got.Validate("rans-dev", 0xABCD, nil, false); err != nil {
		t.Fatal("no-secret compat mode must pass")
	}
	// 但对端配了密钥而我方没带 MAC → 拒绝（红组④：缺 MAC 对配置密钥必拒）。
	if err := got.Validate("rans-dev", 0xABCD, []byte("s3cret"), false); !errors.Is(err, ErrHandshakeMAC) {
		t.Fatalf("missing MAC against configured secret must fail with ErrHandshakeMAC, got %v", err)
	}
}

func TestHandshakeValidationOrder(t *testing.T) {
	secret := []byte("s3cret")
	h := mkHandshake()
	got, _ := DecodeHandshake(h.Encode(secret))
	// 集群名不匹配（MAC 也错 + 摘要也错）→ 必须报集群名（校验序第一）。
	if err := got.Validate("other", 0xFFFF, secret, false); err == nil || !bytes.Contains([]byte(err.Error()), []byte("cluster")) {
		t.Fatalf("cluster mismatch must be reported first: %v", err)
	}
	// MAC 不匹配 + 摘要不匹配 → 报 MAC（校验序：集群名 → HMAC → SchemaDigest，
	// 摘要在 MAC 之后——严格模式下摘要受 MAC 保护后才可比）。
	if err := got.Validate("rans-dev", 0xFFFF, []byte("wrong"), false); err == nil || !errors.Is(err, ErrHandshakeMAC) {
		t.Fatalf("MAC must be checked before schema digest: %v", err)
	}
	// 摘要不匹配（MAC 正确）+ 非严格 → Validate 放行（观测面：连接保持，
	// 告警与计数归传输层）。
	ok, _ := DecodeHandshake(h.Encode(secret))
	if err := ok.Validate("rans-dev", 0xFFFF, secret, false); err != nil {
		t.Fatalf("digest mismatch must pass in observation mode: %v", err)
	}
	// 同失配 + 严格 → ErrHandshakeSchema 拒连。
	if err := ok.Validate("rans-dev", 0xFFFF, secret, true); !errors.Is(err, ErrHandshakeSchema) {
		t.Fatalf("digest mismatch must be refused in strict mode, got %v", err)
	}
}

// ---- 红组③：版本区间/扩展对象篡改 → MAC 拒（全Body受认证）----

// TestHandshakeMACTamperRejected v3 MAC = HMAC-SHA256(secret, MAC 之前的整个
// 握手体)：版本区间与扩展对象全部受认证。篡改任一字节 → Validate 拒。
func TestHandshakeMACTamperRejected(t *testing.T) {
	secret := []byte("s3cret")
	h := mkHandshake()
	enc := h.Encode(secret)

	// 篡改版本区间字节（VerMax 低位，[2:4] 域内）→ MAC 拒。
	verTampered := bytes.Clone(enc)
	verTampered[3] ^= 0x08
	got, err := DecodeHandshake(verTampered) // [3,11] 与 [3,3] 相交：解码放行
	if err != nil {
		t.Fatalf("tampered-but-intersecting version must decode: %v", err)
	}
	if err := got.Validate("rans-dev", 0xABCD, secret, false); !errors.Is(err, ErrHandshakeMAC) {
		t.Fatalf("tampered version range must fail MAC, got %v", err)
	}

	// 篡改扩展对象字节（SchemaDigest fixed64 域内一位翻转）→ MAC 拒。
	extTampered := bytes.Clone(enc)
	extTampered[len(extTampered)-32-16-1] ^= 0x01 // MAC(32B)+Nonce(16B) 之前 1 字节
	got2, err := DecodeHandshake(extTampered)
	if err != nil {
		t.Fatalf("tampered ext object must still decode: %v", err)
	}
	if err := got2.Validate("rans-dev", 0xABCD, secret, false); !errors.Is(err, ErrHandshakeMAC) {
		t.Fatalf("tampered ext object must fail MAC, got %v", err)
	}
}

// TestHandshakeMACBindsWholeBody MAC 确定性 + 全 Body 绑定：对同一握手体
// 重算一致；任一域（版本区间/扩展对象/Nonce）变化 → MAC 变。
func TestHandshakeMACBindsWholeBody(t *testing.T) {
	secret := []byte("s3cret")
	h := mkHandshake()
	enc := h.Encode(secret)
	macVal := enc[len(enc)-sha256.Size:]
	if len(macVal) != sha256.Size {
		t.Fatalf("MAC must be %d bytes, got %d", sha256.Size, len(macVal))
	}
	// 整体重算：Encode 的 MAC 段 = HMAC(secret, 前缀)。
	m := hmac.New(sha256.New, secret)
	m.Write(enc[:len(enc)-sha256.Size])
	if !hmac.Equal(m.Sum(nil), macVal) {
		t.Fatal("MAC must equal HMAC-SHA256 over the whole body preceding it")
	}
	// Nonce 变 → MAC 变。
	h2 := h
	rand.Read(h2.Nonce[:])
	if hmac.Equal(h2.Encode(secret)[len(enc)-sha256.Size:], macVal) {
		t.Fatal("MAC must bind nonce")
	}
	// 摘要变 → MAC 变。
	h3 := h
	h3.SchemaDigest++
	if hmac.Equal(h3.Encode(secret)[len(enc)-sha256.Size:], macVal) {
		t.Fatal("MAC must bind schema digest")
	}
}

// ---- 红组②：未知扩展字段跳过 ----

// TestHandshakeUnknownExtFieldSkipped 含未知字段号（9）扩展对象的握手体：
// v3 读端按线型跳过且其余字段正确（此后新字段只追加字段号——旧端跳过即
// 前向兼容的承重行为）。
func TestHandshakeUnknownExtFieldSkipped(t *testing.T) {
	h := mkHandshake()
	base := h.Encode(nil) // 无 MAC 形态：尾部 = 扩展对象 + Nonce

	// 手工重排：在扩展对象里追加 field 9（varint 值），字段计数 3→4。
	// base 布局：[VerMin 2B][VerMax 2B][count varint][f1 tag][f1 val]
	// [f2 tag][f2 val][f3 tag][f3 val][Nonce 16B]
	r := codec.NewReader(base)
	if _, err := r.ReadFixed16(); err != nil { // VerMin
		t.Fatal(err)
	}
	if _, err := r.ReadFixed16(); err != nil { // VerMax
		t.Fatal(err)
	}
	extLen := r.Remaining() - 16 // 扩展对象 + Nonce
	ext := base[len(base)-r.Remaining():][:extLen]
	prefix := base[:len(base)-extLen-16]
	nonce := base[len(base)-16:]

	w := codec.NewWriter()
	w.WriteRaw(prefix)
	w.BeginObject(4)
	w.WriteRaw(ext[1:]) // 原三字段（剥掉原 count 字节）
	w.WriteTag(9, codec.WireVarInt)
	w.WriteVarInt(77)
	w.WriteRaw(nonce)

	got, err := DecodeHandshake(w.Bytes())
	if err != nil {
		t.Fatalf("unknown ext field must be skipped: %v", err)
	}
	if got.NodeID != h.NodeID || got.ClusterName != h.ClusterName || got.SchemaDigest != h.SchemaDigest {
		t.Fatalf("known fields must survive unknown-field skip: %+v", got)
	}
}

// TestHandshakeMACDetachedFromInput 解码产物脱离输入缓冲（握手体校验所需
// 的认证前缀在解码期拷贝——帧缓冲复用/回收不得影响 Validate）。
func TestHandshakeMACDetachedFromInput(t *testing.T) {
	secret := []byte("s3cret")
	h := mkHandshake()
	enc := h.Encode(secret)
	got, err := DecodeHandshake(enc)
	if err != nil {
		t.Fatal(err)
	}
	for i := range enc { // 模拟帧缓冲复用/回收：解码结果不得再引用输入
		enc[i] = 0
	}
	if err := got.Validate("rans-dev", 0xABCD, secret, false); err != nil {
		t.Fatalf("decoded handshake must be detached from input buffer: %v", err)
	}
}
