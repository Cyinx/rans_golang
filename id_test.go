package rans_test

import (
	"strings"
	"testing"

	rans "github.com/cyinx/rans_golang"
)

// TestID_StringFormatPinned 钉死键格式：ID.String() 与 rans.ID.String()
// 逐字节一致（"kind/key"）——Redis 目录键方案零迁移的回归锚。
func TestID_StringFormatPinned(t *testing.T) {
	id, err := rans.NewID("chat", "room-1")
	if err != nil {
		t.Fatal(err)
	}
	if got := id.String(); got != "chat/room-1" {
		t.Fatalf("ID.String() = %q, want %q", got, "chat/room-1")
	}
	// fmt.Stringer 契约。
	var s interface{ String() string } = id
	if s.String() != "chat/room-1" {
		t.Fatalf("Stringer = %q", s.String())
	}
}

func TestID_NewKindValidation(t *testing.T) {
	cases := []struct {
		kind string
		ok   bool
	}{
		{"player", true}, {"_x", true}, {"A-b_9", true},
		{"1abc", false}, {"a b", false}, {"a/b", false}, {"", false},
		{strings.Repeat("a", 64), true},  // 长度上限：合法
		{strings.Repeat("a", 65), false}, // 超限一位
	}
	for _, c := range cases {
		_, err := rans.NewID(c.kind, "i")
		if (err == nil) != c.ok {
			t.Errorf("NewID(kind=%q) err=%v, want ok=%v", c.kind, err, c.ok)
		}
	}
}

func TestID_RejectsEmptyKey(t *testing.T) {
	if _, err := rans.NewID("player", ""); err == nil {
		t.Fatal("empty key must be rejected")
	}
}

// TestID_RejectsBraces 身段禁花括号（键值 v3：主键 `rans:e:{<kind>/<key>}`
// 的 hash tag 承载完整实体身份——花括号入身份会撕裂 tag 边界，构造点禁入；
// kind 与 key 各自校验）。
func TestID_RejectsBraces(t *testing.T) {
	for _, c := range []struct{ kind, key string }{
		{"player", "a{b"},
		{"player", "a}b"},
		{"player", "{}"},
		{"pl{ayer", "k"},
		{"pl}ayer", "k"},
	} {
		if _, err := rans.NewID(c.kind, c.key); err == nil {
			t.Fatalf("NewID(%q, %q) must reject braces in identity", c.kind, c.key)
		}
	}
	if _, err := rans.NewID("player", "a(b)c[no-braces]"); err != nil {
		t.Fatalf("non-brace specials must stay legal: %v", err)
	}
}

// TestID_ValueEqualityAsMapKey：ID 仅含 string，值相等即可作 map 键
// （规格 §3.2 移植要点）。
func TestID_ValueEqualityAsMapKey(t *testing.T) {
	a, _ := rans.NewID("agent", "u1")
	b, _ := rans.NewID("agent", "u1")
	c, _ := rans.NewID("agent", "u2")
	m := map[rans.ID]int{a: 1}
	if m[b] != 1 {
		t.Fatal("value-equal IDs must hit same map key")
	}
	if _, ok := m[c]; ok {
		t.Fatal("different keys must not collide")
	}
	if !(rans.ID{}).IsZero() || a.IsZero() {
		t.Fatal("IsZero semantics: zero value true, constructed false")
	}
}
