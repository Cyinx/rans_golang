// parse_gate_test 覆盖解析层门卫的失败断言（注解值、字段/参数名形态、
// 目录级错误）：门卫错误文本含稳定子串，供 run 全流程冒出（红测先行的
// 既有惯例同 parse_test 的表驱动形态）。
package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadPackageErrors(t *testing.T) {
	cases := []struct {
		name    string
		src     string
		wantSub string
	}{
		{
			name:    "empty kind name",
			src:     "package x\n\n// A 空 kind。\n//\n//rans:kind=\ntype A struct{}\n",
			wantSub: "empty kind name",
		},
		{
			name:    "kind charset violation",
			src:     "package x\n\n// A 非法 kind。\n//\n//rans:kind=9room\ntype A struct{}\n",
			wantSub: "invalid kind",
		},
		{
			name:    "entity annotation on non-struct",
			src:     "package x\n\n// A 非结构体实体。\n//\n//rans:kind=a\ntype A int\n",
			wantSub: "non-struct",
		},
		{
			name: "duplicate explicit rans name",
			src: "package x\n\ntype A struct {\n\tF string " + orderTag(1) + "\n}\n\n" +
				"// B 同名消息。\n//\n//rans:name=dup\ntype B struct {\n\tG string " + orderTag(1) + "\n}\n\n" +
				"// C 再一个同名。\n//\n//rans:name=dup\ntype C struct {\n\tH string " + orderTag(1) + "\n}\n",
			wantSub: "duplicate message name",
		},
		{
			name: "explicit name collides with type name",
			src: "package x\n\n" +
				"// A 显式命名撞类型名。\n//\n//rans:name=B\ntype A struct {\n\tF string " + orderTag(1) + "\n}\n\n" +
				"type B struct {\n\tG string " + orderTag(1) + "\n}\n",
			wantSub: "duplicate message name",
		},
		{
			name:    "malformed order tag non-int",
			src:     "package x\n\ntype A struct {\n\tF string `rans:\"order=abc\"`\n}\n",
			wantSub: "malformed",
		},
		{
			name:    "duplicate order in one struct",
			src:     "package x\n\ntype A struct {\n\tF string " + orderTag(1) + "\n\tG string " + orderTag(1) + "\n}\n",
			wantSub: "duplicate order",
		},
		{
			name:    "duplicate rans kind on one type",
			src:     "package x\n\n// A 双 kind。\n//\n//rans:kind=a\n//rans:kind=b\ntype A struct{}\n",
			wantSub: "duplicate rans:kind",
		},
		{
			name:    "unknown annotation key",
			src:     "package x\n\n// A 未知注解。\n//\n//rans:kinds=a\ntype A struct{}\n",
			wantSub: "unknown annotation",
		},
		{
			// 多名字段共享一条 tag：order 只登记一次但产出两个同 order 的
			// FieldDecl → 两份重复 case 臂，round-trip 错乱（A 被覆写、B 恒零）。
			name:    "multi-name tagged field",
			src:     "package x\n\ntype M struct {\n\tA, B string " + orderTag(1) + "\n}\n",
			wantSub: "multi-name",
		},
		{
			// 嵌入字段无 Names：tag 被解析、order 被 seen 占位，但无 FieldDecl
			// 产出 → 消息静默缺字段。
			name:    "embedded tagged field",
			src:     "package x\n\ntype Inner struct{}\n\ntype M struct {\n\tInner " + "`rans:\"order=1\"`" + "\n}\n",
			wantSub: "embedded",
		},
		{
			// 空名 _：产出 FieldDecl{Name:"_"} → 生成 v._ 非法源码。
			name:    "blank tagged field name",
			src:     "package x\n\ntype M struct {\n\t_ string " + orderTag(1) + "\n}\n",
			wantSub: "blank",
		},
		{
			name:    "generics on kind-annotated type",
			src:     "package x\n\n// G 泛型实体。\n//\n//rans:kind=g\ntype G[T any] struct{}\n",
			wantSub: "generics not supported",
		},
		{
			name:    "generics on message candidate struct",
			src:     "package x\n\ntype G[T any] struct {\n\tA string " + orderTag(1) + "\n}\n",
			wantSub: "generics not supported",
		},
		{
			// idempotent 是方法级旗标注解：带值即畸形。
			name:    "idempotent annotation with value",
			src:     "package x\n\nimport \"context\"\n\n// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n// M 带值注解。\n//\n//rans:idempotent=true\nfunc (a *A) M(ctx context.Context) error { return nil }\n",
			wantSub: "rans:idempotent",
		},
		{
			// 注解在类型声明上：位置错——idempotent 只认方法声明 doc。
			name:    "idempotent annotation on type decl",
			src:     "package x\n\n// A 实体误挂方法注解。\n//\n//rans:kind=a\n//rans:idempotent\ntype A struct{}\n",
			wantSub: "method-level",
		},
		{
			// 同一方法重复标注。
			name:    "duplicate idempotent annotation",
			src:     "package x\n\nimport \"context\"\n\n// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n// M 重复标注。\n//\n//rans:idempotent\n//rans:idempotent\nfunc (a *A) M(ctx context.Context) error { return nil }\n",
			wantSub: "duplicate rans:idempotent",
		},
		{
			// 载荷参数 `_`：参数名直接成为请求字段名，生成 v._ / 字面量键
			// _ 形态的非法源码（消息字段侧孪生门卫见 checkTaggedFieldNames）。
			name:    "blank payload parameter",
			src:     "package x\n\nimport \"context\"\n\n// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\nfunc (a *A) Send(ctx context.Context, _ int64) error { return nil }\n",
			wantSub: "blank parameter",
		},
		{
			// 参数名 capitalize 归一碰撞：foo/Foo 共存产出重复字段声明。
			name:    "parameter capitalize collision",
			src:     "package x\n\nimport \"context\"\n\n// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\nfunc (a *A) Put(ctx context.Context, foo int64, Foo string) error { return nil }\n",
			wantSub: "both derive request field",
		},
		{
			// //rans:name 空名：撞无值应答协议哨兵（codec.Encode(nil) 的空
			// wire 名形态），必须在门卫层拦。
			name:    "empty rans:name",
			src:     "package x\n\n// A 空名消息。\n//\n//rans:name=\ntype A struct {\n\tF string " + orderTag(1) + "\n}\n",
			wantSub: "empty rans:name",
		},
		{
			// //rans:name 非法字符：wire 注册名字符集门卫（kind 侧先例）。
			name:    "invalid rans:name",
			src:     "package x\n\n// A 非法名消息。\n//\n//rans:name=has space\ntype A struct {\n\tF string " + orderTag(1) + "\n}\n",
			wantSub: "invalid rans:name",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := writePkg(t, map[string]string{"x.go": tc.src})
			_, err := LoadPackage(dir, "")
			if err == nil {
				t.Fatalf("LoadPackage 成功，want 错误含 %q", tc.wantSub)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("err = %v, want 含 %q", err, tc.wantSub)
			}
		})
	}
}

// TestLoadPackageLeavesUnrelatedGenerics 泛型门卫不误伤生成面之外的类型：
// 无 kind 注解、无 rans tag 字段的泛型声明原样放行（不进实体/消息面）。
func TestLoadPackageDirLevelErrors(t *testing.T) {
	t.Run("no go files", func(t *testing.T) {
		dir := t.TempDir()
		if _, err := LoadPackage(dir, ""); err == nil || !strings.Contains(err.Error(), "no non-test Go files") {
			t.Fatalf("err = %v, want 含 no non-test Go files", err)
		}
	})
	t.Run("multiple packages", func(t *testing.T) {
		dir := writePkg(t, map[string]string{
			"a.go": "package a\n",
			"b.go": "package b\n",
		})
		if _, err := LoadPackage(dir, ""); err == nil || !strings.Contains(err.Error(), "multiple packages") {
			t.Fatalf("err = %v, want 含 multiple packages", err)
		}
	})
	t.Run("not exist dir", func(t *testing.T) {
		if _, err := LoadPackage(filepath.Join(t.TempDir(), "nope"), ""); err == nil {
			t.Fatal("LoadPackage 成功，want 错误")
		}
	})
}

func TestParamBlankNameGate(t *testing.T) {
	src := "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) Send(ctx context.Context, _ int64) error { return nil }\n"
	dir := writePkg(t, map[string]string{"x.go": src})
	err := run(dir, "rans_gen.go")
	if err == nil || !strings.Contains(err.Error(), "blank parameter") {
		t.Fatalf("err = %v, want 含 blank parameter", err)
	}
	ok := "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) Ping(_ context.Context) error { return nil }\n"
	dir2 := writePkg(t, map[string]string{"x.go": ok})
	if err := run(dir2, "rans_gen.go"); err != nil {
		t.Fatalf("ctx 位空白名被误拒: %v", err)
	}
}

// TestParamCapitalizeCollisionGate 参数名 capitalize 归一碰撞（foo/Foo
// 共存）→ 生成期报错，错误点名碰撞字段与两个来源参数。（门卫对合成名
// ArgN 一并查重；未命名与具名混排的参数表被 parser 拒绝，合成名碰撞在
// 合法源码里不可达，无独立用例。）
func TestParamCapitalizeCollisionGate(t *testing.T) {
	src := "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) Put(ctx context.Context, foo int64, Foo string) error { return nil }\n"
	dir := writePkg(t, map[string]string{"x.go": src})
	err := run(dir, "rans_gen.go")
	if err == nil || !strings.Contains(err.Error(), `request field "Foo"`) ||
		!strings.Contains(err.Error(), "foo") {
		t.Fatalf("err = %v, want 含碰撞字段 Foo 与参数名 foo", err)
	}
}

// TestRansNameValueGate //rans:name 空名/非法字符 → 生成期报错：空名撞
// 无值应答协议哨兵（codec.Encode(nil) → ("", nil, nil) 的空 wire 名形态，
// 注册表里同名消息会吞掉无值应答的解码），非法字符借 kind 侧字符集门卫
// 先例。合法形态（含点号的命名空间名）由 TestLoadPackageMessages 正向锚定。
func TestRansNameValueGate(t *testing.T) {
	cases := []struct {
		name    string
		ann     string
		wantSub string
	}{
		{"empty", "// A 空名消息。\n//\n//rans:name=\n", "empty rans:name"},
		{"charset", "// A 非法名消息。\n//\n//rans:name=has space\n", "invalid rans:name"},
		{"leading digit", "// A 数字起头消息。\n//\n//rans:name=9room\n", "invalid rans:name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := "package x\n\n" + tc.ann + "type A struct {\n\tF string " + orderTag(1) + "\n}\n"
			dir := writePkg(t, map[string]string{"x.go": src})
			if err := run(dir, "rans_gen.go"); err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("err = %v, want 含 %q", err, tc.wantSub)
			}
		})
	}
}
