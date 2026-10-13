// schema_test 覆盖 rans_schema.json 产出与 -check 兼容检查（加固 spec
// §6.1 第 5 点）：三判据（同字段号改线型/字段号复用 → 失败，删字段/删
// 消息 → 告警）、产出确定性、跨包 wire 名唯一、基线新鲜度锁。
package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// wireDirs 模块的线面包全集（与 Makefile schema-check 的 SCHEMA_DIRS 同一
// 组，改动须两处同步）：examples/chat + 两个 ransgen fixture。基线即这三包
// 消息面的合并清单。
var wireDirs = []string{
	filepath.Join("..", "..", "examples", "chat"),
	filepath.Join("testdata", "entity_demo"),
	filepath.Join("testdata", "serial_demo"),
}

// schemaPkg 便捷：源码串写临时包并建清单。
func schemaPkg(t *testing.T, src string) *ransSchema {
	t.Helper()
	dir := writePkg(t, map[string]string{"x.go": src})
	s, err := buildSchema([]string{dir})
	if err != nil {
		t.Fatalf("buildSchema: %v", err)
	}
	return s
}

// mustRender 渲染清单为稳定 JSON 字节（含尾换行）。
func mustRender(t *testing.T, s *ransSchema) []byte {
	t.Helper()
	b, err := renderSchemaJSON(s)
	if err != nil {
		t.Fatalf("renderSchemaJSON: %v", err)
	}
	return b
}

// msg 便捷构造 schemaMessage。
func msg(name string, fields ...schemaField) schemaMessage {
	return schemaMessage{Name: name, Fields: fields}
}

// fld 便捷构造 schemaField。
func fld(id int, name, wt string) schemaField {
	return schemaField{ID: id, Name: name, WireType: wt}
}

// TestSchemaDeterministicOrdering 产出确定性：声明序不同的同构输入渲染
// 字节一致；消息按 wire 名、字段按 id 升序。
func TestSchemaDeterministicOrdering(t *testing.T) {
	a := schemaPkg(t, "package x\n"+
		"type M2 struct {\n\tB string `rans:\"order=2\"`\n\tA int64 `rans:\"order=1\"`\n}\n"+
		"type M1 struct {\n\tZ int64 `rans:\"order=1\"`\n}\n")
	b := schemaPkg(t, "package x\n"+
		"type M1 struct {\n\tZ int64 `rans:\"order=1\"`\n}\n"+
		"type M2 struct {\n\tA int64 `rans:\"order=1\"`\n\tB string `rans:\"order=2\"`\n}\n")
	if !bytes.Equal(mustRender(t, a), mustRender(t, b)) {
		t.Fatalf("同构输入渲染不一致:\n%s\n---\n%s", mustRender(t, a), mustRender(t, b))
	}
	want := mustRender(t, schemaOf(msg("M1", fld(1, "Z", "varint")), msg("M2", fld(1, "A", "varint"), fld(2, "B", "string"))))
	if !bytes.Equal(mustRender(t, a), want) {
		t.Fatalf("排序口径不符（消息按名、字段按 id）:\n%s\n--- want ---\n%s", mustRender(t, a), want)
	}
}

// schemaOf 便捷装配清单（测试断言的期望值形态）。
func schemaOf(msgs ...schemaMessage) *ransSchema {
	return &ransSchema{Messages: msgs}
}

// TestSchemaZeroFieldMessage 零字段消息入清单（fields 为空数组而非 null
// ——格式稳定可比对）。发现层只有方法派生请求会产零字段消息（无参方法）。
func TestSchemaZeroFieldMessage(t *testing.T) {
	s := schemaPkg(t, "package x\n"+
		"import \"context\"\n"+
		"\n//rans:kind=k\n"+
		"type E struct {}\n"+
		"func (e *E) Ping(ctx context.Context) {}\n")
	if len(s.Messages) != 1 || s.Messages[0].Fields == nil {
		t.Fatalf("零字段消息未入清单或 fields 为 nil: %+v", s.Messages)
	}
	if s.Messages[0].Name != "k.Ping" {
		t.Fatalf("派生请求 wire 名 = %q, want k.Ping", s.Messages[0].Name)
	}
	if !bytes.Contains(mustRender(t, s), []byte(`"fields": []`)) {
		t.Fatalf("零字段消息未渲染为空数组:\n%s", mustRender(t, s))
	}
}

// TestSchemaWireTypes 序列元素/嵌套消息/标量的线型命名口径（codec.Wire*
// 的稳定字符串形）。
func TestSchemaWireTypes(t *testing.T) {
	s := schemaPkg(t, "package x\n"+
		"type P struct {\n\tX int64 `rans:\"order=1\"`\n}\n"+
		"type M struct {\n\tA int `rans:\"order=1\"`\n\tB uint64 `rans:\"order=2\"`\n"+
		"\tC string `rans:\"order=3\"`\n\tD []byte `rans:\"order=4\"`\n"+
		"\tE float64 `rans:\"order=5\"`\n\tF bool `rans:\"order=6\"`\n"+
		"\tG P `rans:\"order=7\"`\n\tH []string `rans:\"order=8\"`\n\tI []P `rans:\"order=9\"`\n}\n")
	byName := map[string]schemaField{}
	for _, m := range s.Messages {
		if m.Name != "M" {
			continue
		}
		for _, f := range m.Fields {
			byName[f.Name] = f
		}
	}
	want := map[string]string{
		"A": "varint", "B": "fixed64", "C": "string", "D": "bytes", "E": "fixed64",
		"F": "varint", "G": "message", "H": "sequence", "I": "sequence",
	}
	for name, wt := range want {
		if got := byName[name].WireType; got != wt {
			t.Fatalf("字段 %s wiretype = %q, want %q", name, got, wt)
		}
	}
}

// TestCheckWireTypeChangeFails 判据一：同字段号改线型 → 失败（非零退出）。
func TestCheckWireTypeChangeFails(t *testing.T) {
	base := schemaOf(msg("m", fld(1, "A", "varint")))
	cur := schemaOf(msg("m", fld(1, "A", "string")))
	res := checkSchema(base, cur)
	if len(res.Failures) != 1 {
		t.Fatalf("线型变更未判失败: %+v", res)
	}
	if !strings.Contains(res.Failures[0], "线型") || !strings.Contains(res.Failures[0], "varint") {
		t.Fatalf("失败明细不含线型变更说明: %q", res.Failures[0])
	}
}

// TestCheckFieldIDReuseFails 判据二：同消息内字段号复用（同号换字段名）
// → 失败。
func TestCheckFieldIDReuseFails(t *testing.T) {
	base := schemaOf(msg("m", fld(1, "A", "varint")))
	cur := schemaOf(msg("m", fld(1, "B", "varint")))
	res := checkSchema(base, cur)
	if len(res.Failures) != 1 {
		t.Fatalf("字段号复用未判失败: %+v", res)
	}
	if !strings.Contains(res.Failures[0], "复用") {
		t.Fatalf("失败明细不含复用说明: %q", res.Failures[0])
	}
}

// TestCheckFieldDeletedWarns 判据三：删字段 → 告警不失败（退出零）。
func TestCheckFieldDeletedWarns(t *testing.T) {
	base := schemaOf(msg("m", fld(1, "A", "varint"), fld(2, "B", "string")))
	cur := schemaOf(msg("m", fld(1, "A", "varint")))
	res := checkSchema(base, cur)
	if len(res.Failures) != 0 {
		t.Fatalf("删字段误判失败: %+v", res)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "B") {
		t.Fatalf("删字段未告警: %+v", res)
	}
}

// TestCheckMessageDeletedWarns 删消息 → 告警不失败。
func TestCheckMessageDeletedWarns(t *testing.T) {
	base := schemaOf(msg("m1", fld(1, "A", "varint")), msg("m2", fld(1, "B", "string")))
	cur := schemaOf(msg("m1", fld(1, "A", "varint")))
	res := checkSchema(base, cur)
	if len(res.Failures) != 0 {
		t.Fatalf("删消息误判失败: %+v", res)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "m2") {
		t.Fatalf("删消息未告警: %+v", res)
	}
}

// TestCheckCompatibleSilent 兼容演进静默通过：清单全同/增字段/增消息
// 零失败零告警（增面走兼容通道，规格 §6.1 第 7 点）。
func TestCheckCompatibleSilent(t *testing.T) {
	base := schemaOf(msg("m1", fld(1, "A", "varint")))
	cur := schemaOf(
		msg("m1", fld(1, "A", "varint"), fld(2, "N", "string")),
		msg("m2.new", fld(1, "X", "bytes")))
	res := checkSchema(base, cur)
	if len(res.Failures) != 0 || len(res.Warnings) != 0 {
		t.Fatalf("兼容演进误报: %+v", res)
	}
}

// TestRunCheckBaselineUnreadable 基线不可读/非法 → 基础设施错误（main 转
// 非零退出，与判据失败同一退出码、独立错误文本）。
func TestRunCheckBaselineUnreadable(t *testing.T) {
	dir := writePkg(t, map[string]string{"x.go": "package x\ntype M struct {\n\tA int64 `rans:\"order=1\"`\n}\n"})
	if _, err := runCheck([]string{dir}, filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatalf("基线缺失未报错")
	}
	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := runCheck([]string{dir}, bad); err == nil {
		t.Fatalf("基线非法未报错")
	}
}

// TestSchemaDuplicateWireNameAcrossDirs 跨包 wire 名撞车在建清单期报错
// （清单按 wire 名索引，重名即歧义）。
func TestSchemaDuplicateWireNameAcrossDirs(t *testing.T) {
	src := "package x\ntype Dup struct {\n\tA int64 `rans:\"order=1\"`\n}\n"
	d1 := writePkg(t, map[string]string{"a.go": src})
	d2 := writePkg(t, map[string]string{"b.go": src})
	if _, err := buildSchema([]string{d1, d2}); err == nil {
		t.Fatalf("跨包重名未报错")
	}
}

// TestSchemaBaselineFresh 基线新鲜度锁：入库 schema_baseline.json 必须与
// 三线面包的当前清单逐字节一致——线面改动未重产基线即红（Makefile
// schema-check 的测试级孪生）。
func TestSchemaBaselineFresh(t *testing.T) {
	s, err := buildSchema(wireDirs)
	if err != nil {
		t.Fatalf("buildSchema(wireDirs): %v", err)
	}
	fresh := mustRender(t, s)
	committed, err := os.ReadFile(filepath.Join("testdata", "schema_baseline.json"))
	if err != nil {
		t.Fatalf("读基线: %v（首次产出：go run ./cmd/ransgen -dir … -schema cmd/ransgen/testdata/schema_baseline.json）", err)
	}
	if !bytes.Equal(fresh, committed) {
		t.Fatalf("schema_baseline.json 与当前线面不一致（线面改动后需重产基线并随发版更新）\n--- fresh ---\n%s\n--- committed ---\n%s", fresh, committed)
	}
}

// TestSchemaBaselineShape 基线内容抽查：消息按 wire 名排序、覆盖三包的
// 代表消息（chat / counter 派生请求 / serial 消息）。
func TestSchemaBaselineShape(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "schema_baseline.json"))
	if err != nil {
		t.Fatal(err)
	}
	var s ransSchema
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("基线非法 JSON: %v", err)
	}
	names := make([]string, 0, len(s.Messages))
	for _, m := range s.Messages {
		names = append(names, m.Name)
	}
	for i := 1; i < len(names); i++ {
		if names[i-1] >= names[i] {
			t.Fatalf("基线消息未按 wire 名升序: %v", names)
		}
	}
	for _, want := range []string{"chat.Say", "chat.History", "chatMsg", "counter.Add", "serial.Shape", "Alpha"} {
		found := false
		for _, n := range names {
			if n == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("基线缺消息 %q（有 %v）", want, names)
		}
	}
}

// TestGeneratedInitRegistersShapes 生成物 init 登记口：模板恒发射
// codec.RegisterShapes(RansShapes)——codec.SchemaDigest() 的登记来源，
// 形状声明随生成物自动接线，不经 node 配置传入。
func TestGeneratedInitRegistersShapes(t *testing.T) {
	pkg, err := LoadPackage(filepath.Join("testdata", "serial_demo"), "rans_gen.go")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	out := renderPlan(t, pkg)
	for _, want := range []string{
		"func init() {",
		"codec.RegisterShapes(RansShapes)",
	} {
		if !bytes.Contains(out, []byte(want)) {
			t.Fatalf("生成物缺 init 登记口 %q:\n%s", want, out)
		}
	}
}
