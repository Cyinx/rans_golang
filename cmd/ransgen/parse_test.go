package main

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// orderTag 生成 rans:"order=N" 字段 tag 字面量（避开 raw string 反引号冲突）。
func orderTag(n int) string { return "`rans:\"order=" + strconv.Itoa(n) + "\"`" }

// writePkg 把内联 fixture 源码落进临时目录，返回目录路径。
func writePkg(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, src := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
			t.Fatalf("write fixture %s: %v", name, err)
		}
	}
	return dir
}

// fixtureSrc 主 fixture：实体+方法+消息（默认名/显式名）+ 干扰项。
const fixtureSrc = `package chat

import "context"

// Room 聊天室实体。
//
//rans:kind=room
type Room struct {
	members []string // 本地状态：无 order tag，不进消息
}

// Say 发言并回声。
func (r *Room) Say(ctx context.Context, msg string) (string, error) {
	return msg, nil
}

// Join 入会。
func (r *Room) Join(ctx context.Context, who string) error {
	return nil
}

// Leave 值接收者方法。
func (r Room) Leave(ctx context.Context) error { return nil }

// broadcast 包级函数：不归属实体。
func broadcast() {}

// Other 非实体类型。
type Other struct{}

// Off 接收者非实体类型：不进任何实体方法集。
func (o Other) Off() {}
`

// fixtureMsgs 消息部分（含 tag 占位符 @TAG1/@TAG2，渲染前替换）。
const fixtureMsgs = `
// chatMsg 消息：默认名=类型名，字段乱序声明验证按 order 排序。
type chatMsg struct {
	Body string @TAG2@
	From string @TAG1@
	Note string // 无 rans tag：跳过
}

// historyReq 显式命名消息（wire TypeName 可含点号）。
//
//rans:name=chat.history
type historyReq struct {
	Limit int @TAG1@
}
`

func mainFixture(t *testing.T) string {
	t.Helper()
	r := strings.NewReplacer("@TAG1@", orderTag(1), "@TAG2@", orderTag(2))
	return r.Replace(fixtureSrc + fixtureMsgs)
}

func TestLoadPackageEntityAndMethods(t *testing.T) {
	dir := writePkg(t, map[string]string{"chat.go": mainFixture(t)})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	if pkg.Name != "chat" {
		t.Fatalf("package name = %q, want %q", pkg.Name, "chat")
	}
	want := []EntityDecl{{
		KindName: "room",
		TypeName: "Room",
		Methods: []MethodDecl{
			{
				Name: "Join",
				Args: []FieldDecl{
					{Name: "ctx", Type: "context.Context"},
					{Name: "who", Type: "string"},
				},
				Returns: []FieldDecl{{Name: "", Type: "error"}},
			},
			{
				Name:    "Leave",
				Args:    []FieldDecl{{Name: "ctx", Type: "context.Context"}},
				Returns: []FieldDecl{{Name: "", Type: "error"}},
			},
			{
				Name: "Say",
				Args: []FieldDecl{
					{Name: "ctx", Type: "context.Context"},
					{Name: "msg", Type: "string"},
				},
				Returns: []FieldDecl{{Name: "", Type: "string"}, {Name: "", Type: "error"}},
			},
		},
	}}
	if !reflect.DeepEqual(pkg.Entities, want) {
		t.Fatalf("entities = %+v\nwant %+v", pkg.Entities, want)
	}
}

func TestLoadPackageMessages(t *testing.T) {
	dir := writePkg(t, map[string]string{"chat.go": mainFixture(t)})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	want := []MessageDecl{
		{
			Name:         "chat.history", // "chat." < "chatM"（字典序）
			NameExplicit: true,           // 来自显式 //rans:name
			TypeName:     "historyReq",
			Fields:       []FieldDecl{{Name: "Limit", Type: "int", Order: 1}},
		},
		{
			Name:     "chatMsg",
			TypeName: "chatMsg",
			Fields: []FieldDecl{
				{Name: "From", Type: "string", Order: 1},
				{Name: "Body", Type: "string", Order: 2},
			},
		},
	}
	if !reflect.DeepEqual(pkg.Messages, want) {
		t.Fatalf("messages = %+v\nwant %+v", pkg.Messages, want)
	}
}

func TestLoadPackageEntityAlsoMessage(t *testing.T) {
	// 同一 struct 既是实体（kind 注解）又带 order tag 字段：允许双登记
	//（实体状态的序列化面不在此拦截）。
	src := "package x\n\n// E 实体兼消息。\n//\n//rans:kind=e\ntype E struct {\n\tA string " + orderTag(1) + "\n}\n"
	dir := writePkg(t, map[string]string{"x.go": src})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	if len(pkg.Entities) != 1 || pkg.Entities[0].KindName != "e" {
		t.Fatalf("entities = %+v, want kind e", pkg.Entities)
	}
	if len(pkg.Messages) != 1 || pkg.Messages[0].Name != "E" {
		t.Fatalf("messages = %+v, want name E", pkg.Messages)
	}
}

func TestLoadPackageSkipsTestFiles(t *testing.T) {
	dir := writePkg(t, map[string]string{
		"x.go":      "package x\n\ntype M struct {\n\tA string " + orderTag(1) + "\n}\n",
		"x_test.go": "package x\n\n// Ghost 不应被发现。\n//\n//rans:kind=ghost\ntype Ghost struct{}\n",
	})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	if len(pkg.Entities) != 0 {
		t.Fatalf("_test.go 中的实体注解被误发现: %+v", pkg.Entities)
	}
	if len(pkg.Messages) != 1 {
		t.Fatalf("messages = %+v, want 1", pkg.Messages)
	}
}

func TestLoadPackageGroupedTypeDecl(t *testing.T) {
	// 分组 type 声明：注解接在组内 spec 上（spec.Doc）而非 GenDecl.Doc。
	src := "package x\n\ntype (\n\t// G 分组内实体。\n\t//\n\t//rans:kind=g\n\tG struct{}\n\n\tH struct {\n\t\tA string " + orderTag(1) + "\n\t}\n)\n"
	dir := writePkg(t, map[string]string{"x.go": src})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	if len(pkg.Entities) != 1 || pkg.Entities[0].TypeName != "G" {
		t.Fatalf("grouped entities = %+v, want G", pkg.Entities)
	}
	if len(pkg.Messages) != 1 || pkg.Messages[0].TypeName != "H" {
		t.Fatalf("grouped messages = %+v, want H", pkg.Messages)
	}
}

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
func TestLoadPackageLeavesUnrelatedGenerics(t *testing.T) {
	src := "package x\n\ntype L[T any] []T\n\ntype S[T any] struct {\n\tX T\n}\n"
	dir := writePkg(t, map[string]string{"x.go": src})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	if len(pkg.Entities) != 0 || len(pkg.Messages) != 0 {
		t.Fatalf("无关泛型被误入生成面: entities=%+v messages=%+v", pkg.Entities, pkg.Messages)
	}
}

// TestLoadPackageMethodIdempotentFlag 方法注解 //rans:idempotent（方法声明
// doc 上的旗注解，规格 §9.2）：提取为 MethodDecl.Idempotent；无注解
// 方法零值 false。非实体类型的方法注解不进发现面（与既有方法归属规则一致）。
func TestLoadPackageMethodIdempotentFlag(t *testing.T) {
	src := `package x

import "context"

// A 实体。
//
//rans:kind=a
type A struct{}

// Marked 标注幂等：代理生成走 CallOpts 幂等重试。
//
//rans:idempotent
func (a *A) Marked(ctx context.Context, v int64) (int64, error) { return v, nil }

// Bare 无注解。
func (a *A) Bare(ctx context.Context) error { return nil }

// Other 非实体类型。
type Other struct{}

// Off 非实体方法上的注解：不进发现面。
//
//rans:idempotent
func (o Other) Off(ctx context.Context) error { return nil }
`
	dir := writePkg(t, map[string]string{"x.go": src})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	if len(pkg.Entities) != 1 {
		t.Fatalf("entities = %+v, want 1", pkg.Entities)
	}
	byName := map[string]MethodDecl{}
	for _, m := range pkg.Entities[0].Methods {
		byName[m.Name] = m
	}
	if !byName["Marked"].Idempotent {
		t.Fatal("Marked.Idempotent = false, want true")
	}
	if byName["Bare"].Idempotent {
		t.Fatal("Bare.Idempotent = true, want false")
	}
}

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

func TestRunWriteIfDiff(t *testing.T) {
	dir := writePkg(t, map[string]string{"chat.go": mainFixture(t)})
	outPath := filepath.Join(dir, "rans_gen.go")

	if err := run(dir, "rans_gen.go"); err != nil {
		t.Fatalf("first run: %v", err)
	}
	first, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("生成物未落盘: %v", err)
	}
	if !bytes.Contains(first, []byte("package chat")) {
		t.Fatalf("生成物内容异常:\n%s", first)
	}

	// 把 mtime 拨回一小时前：再次生成若重写文件，mtime 会跳到现在。
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(outPath, past, past); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	if err := run(dir, "rans_gen.go"); err != nil {
		t.Fatalf("second run: %v", err)
	}
	fi, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !fi.ModTime().Equal(past) {
		t.Fatalf("内容未变时重写了文件: mtime=%v, want %v", fi.ModTime(), past)
	}

	// 输入变化 → 生成物随之更新。
	extra := "package chat\n\ntype Extra struct {\n\tS string " + orderTag(1) + "\n}\n"
	if err := os.WriteFile(filepath.Join(dir, "extra.go"), []byte(extra), 0o644); err != nil {
		t.Fatalf("write extra: %v", err)
	}
	if err := run(dir, "rans_gen.go"); err != nil {
		t.Fatalf("third run: %v", err)
	}
	second, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if !bytes.Contains(second, []byte(`codec.Register("Extra"`)) {
		t.Fatalf("输入变化后生成物未更新:\n%s", second)
	}

	// 门卫失败 → run 返回错误（main 转非零退出）。
	bad := "package chat\n\n// B 坏注解。\n//\n//rans:kind=\ntype B struct{}\n"
	if err := os.WriteFile(filepath.Join(dir, "bad.go"), []byte(bad), 0o644); err != nil {
		t.Fatalf("write bad: %v", err)
	}
	if err := run(dir, "rans_gen.go"); err == nil || !strings.Contains(err.Error(), "empty kind name") {
		t.Fatalf("门卫失败 err = %v, want 含 empty kind name", err)
	}

	// 类型门卫（S004/M405）失败 → run 返回错误，含字段与类型。
	// （先清掉上面的注解门卫残留，让本例只命中类型门卫。）
	if err := os.Remove(filepath.Join(dir, "bad.go")); err != nil {
		t.Fatalf("remove bad: %v", err)
	}
	badType := "package chat\n\ntype BadTy struct {\n\tC chan int " + orderTag(1) + "\n}\n"
	if err := os.WriteFile(filepath.Join(dir, "bad_type.go"), []byte(badType), 0o644); err != nil {
		t.Fatalf("write bad_type: %v", err)
	}
	if err := run(dir, "rans_gen.go"); err == nil || !strings.Contains(err.Error(), "chan int") {
		t.Fatalf("类型门卫 err = %v, want 含 chan int", err)
	}
}
