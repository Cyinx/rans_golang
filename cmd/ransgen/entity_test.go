// entity_test 覆盖实体生成器（规格 §6）：E316 kind↔实体双射门卫、
// 生命周期方法形态门卫、wrapper 骨架与 Kinds 注册渲染、fixture 漂移锁与
// 行为绿。
package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// entityFixtureDir 实体面 fixture：testdata 下的真实包（input.go 手写 +
// rans_gen.go 生成入库）。
var entityFixtureDir = filepath.Join("testdata", "entity_demo")

// entityRender 走全渲染管线（planGen + 模板），供实体面断言复用。
func entityRender(t *testing.T, pkg *PackageDecl) []byte {
	t.Helper()
	model, err := planGen(pkg)
	if err != nil {
		t.Fatalf("planGen: %v", err)
	}
	out, err := Render(model, ransgenTemplate)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	return out
}

// TestE316SameKindTwoTypes 双射方向一：同 kind 注解在两个类型上 → 生成失败。
func TestE316SameKindTwoTypes(t *testing.T) {
	src := "package x\n\n" +
		"// A 实体一。\n//\n//rans:kind=room\ntype A struct{}\n\n" +
		"// B 实体二撞 kind。\n//\n//rans:kind=room\ntype B struct{}\n"
	dir := writePkg(t, map[string]string{"x.go": src})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	_, err = planGen(pkg)
	if err == nil || !strings.Contains(err.Error(), "E316") ||
		!strings.Contains(err.Error(), "A") || !strings.Contains(err.Error(), "B") {
		t.Fatalf("err = %v, want 含 E316 与两个类型名", err)
	}
}

// TestE316SameTypeTwoKinds 双射方向二：同类型两 kind 注解 → 生成失败。
// 单注释组内的重复 rans:kind 由解析层拦截（parse_test 已锁）；跨文件重复
// 声明同类型携带不同 kind 由实体集层拦截（parser 不查重复声明）。
func TestE316SameTypeTwoKinds(t *testing.T) {
	a := "package x\n\n// Dup 实体。\n//\n//rans:kind=a\ntype Dup struct{}\n"
	b := "package x\n\n// Dup 重复声明另一 kind。\n//\n//rans:kind=b\ntype Dup struct{}\n"
	dir := writePkg(t, map[string]string{"a.go": a, "b.go": b})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	_, err = planGen(pkg)
	if err == nil || !strings.Contains(err.Error(), "E316") ||
		!strings.Contains(err.Error(), "Dup") {
		t.Fatalf("err = %v, want 含 E316 与类型名 Dup", err)
	}
}

// TestEntityKindCharsetGate kind 字符集门卫经 run 全流程冒出（解析层同款
// 正则，rans.NewID 本地复制）。
func TestEntityKindCharsetGate(t *testing.T) {
	src := "package x\n\n// A 非法 kind。\n//\n//rans:kind=9room\ntype A struct{}\n"
	dir := writePkg(t, map[string]string{"x.go": src})
	err := run(dir, "rans_gen.go")
	if err == nil || !strings.Contains(err.Error(), "invalid kind") {
		t.Fatalf("err = %v, want 含 invalid kind", err)
	}
}

// TestEntityLifecycleShapeGate 用户 Activate/Deactivate 形态不符 → 生成失败
// （生成的委托调用形态必须与用户方法签名对得上）。
func TestEntityLifecycleShapeGate(t *testing.T) {
	src := "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) Activate(ctx context.Context) {}\n"
	dir := writePkg(t, map[string]string{"x.go": src})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	_, err = planGen(pkg)
	if err == nil || !strings.Contains(err.Error(), "Activate") ||
		!strings.Contains(err.Error(), "func(context.Context) error") {
		t.Fatalf("err = %v, want 含 Activate 形态要求", err)
	}
}

// TestEntityRenderWrapperAndKinds 合法实体渲染 wrapper 骨架与 Kinds 注册：
// 无生命周期方法时 Activate/Deactivate 返回 nil，Handle 骨架一律未知请求。
func TestEntityRenderWrapperAndKinds(t *testing.T) {
	dir := writePkg(t, map[string]string{"chat.go": mainFixture(t)})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	out := entityRender(t, pkg)
	for _, want := range []string{
		"var Kinds = map[string]rans.Factory{",
		`"room": func(_ rans.ID) rans.Entity { return &RoomPawnite{E: &Room{}} },`,
		"type RoomPawnite struct {\n\tE *Room\n}",
		"var _ rans.Entity = (*RoomPawnite)(nil)",
		"func (w *RoomPawnite) Activate(ctx context.Context) error {\n\treturn nil\n}",
		"func (w *RoomPawnite) Deactivate(ctx context.Context) error {\n\treturn nil\n}",
		"func (w *RoomPawnite) Handle(ctx context.Context, req any) (any, error) {\n\tswitch r := req.(type) {",
		"return nil, fmt.Errorf(\"rans: unknown request %T\", r)",
	} {
		if !bytes.Contains(out, []byte(want)) {
			t.Fatalf("生成物缺 %q:\n%s", want, out)
		}
	}
}

// TestEntityRenderLifecycleDelegation 用户声明 Activate/Deactivate → wrapper
// 委托实体方法。
func TestEntityRenderLifecycleDelegation(t *testing.T) {
	src := "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) Activate(ctx context.Context) error { return nil }\n" +
		"func (a *A) Deactivate(ctx context.Context) error { return nil }\n"
	dir := writePkg(t, map[string]string{"x.go": src})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	out := entityRender(t, pkg)
	for _, want := range []string{
		"func (w *APawnite) Activate(ctx context.Context) error {\n\treturn w.E.Activate(ctx)\n}",
		"func (w *APawnite) Deactivate(ctx context.Context) error {\n\treturn w.E.Deactivate(ctx)\n}",
	} {
		if !bytes.Contains(out, []byte(want)) {
			t.Fatalf("生成物缺 %q:\n%s", want, out)
		}
	}
}

// TestEntityRenderSnapshotCarrierDelegation 快照透传契约：
// 用户实体实现快照能力面（rans.Snapshotter 的两个方法、形态正确）
// → wrapper 同步实现并逐字节透传——钝化/停机/迁移的终快照捕获与激活期
// 恢复都经 wrapper 到达用户实体（未实现快照面的实体不透传，保持无状态
// 语义）。
func TestEntityRenderSnapshotCarrierDelegation(t *testing.T) {
	src := "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) Activate(ctx context.Context) error { return nil }\n" +
		"func (a *A) Snapshot() ([]byte, error) { return nil, nil }\n" +
		"func (a *A) Restore(b []byte) error { return nil }\n"
	dir := writePkg(t, map[string]string{"x.go": src})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	out := entityRender(t, pkg)
	for _, want := range []string{
		"func (w *APawnite) Snapshot() ([]byte, error) {\n\treturn w.E.Snapshot()\n}",
		"func (w *APawnite) Restore(b []byte) error {\n\treturn w.E.Restore(b)\n}",
	} {
		if !bytes.Contains(out, []byte(want)) {
			t.Fatalf("生成物缺快照透传 %q:\n%s", want, out)
		}
	}
}

// TestEntityRenderNoSnapshotFaceByDefault 反向锚定：用户实体未实现快照面
// → wrapper 不生成透传方法（wrapper 不满足 rans.Snapshotter，manager 侧
// 维持「无状态实体」语义——不因透传面常在而把无状态实体误判为有状态）。
func TestEntityRenderNoSnapshotFaceByDefault(t *testing.T) {
	dir := writePkg(t, map[string]string{"chat.go": mainFixture(t)})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	out := entityRender(t, pkg)
	if bytes.Contains(out, []byte("Snapshot")) {
		t.Fatalf("未实现快照面的实体不得生成透传方法:\n%s", out)
	}
}

// TestEntitySnapshotFaceShapeGate 快照面形态门卫：签名与 rans.Snapshotter
// 不符（生成体将无法编译或静默改变语义）→ 生成失败。各用例只带一个畸形
// 快照方法（不带合法对侧）：成对预扫描会在畸形形态之前先撞派生名冲突，
// 掩盖形态门卫的错误文本。
func TestEntitySnapshotFaceShapeGate(t *testing.T) {
	for name, method := range map[string]string{
		"capture bad shape":  "func (a *A) Snapshot(ctx context.Context) ([]byte, error) { return nil, nil }\n",
		"restore bad shape":  "func (a *A) Restore(b []byte, n int) error { return nil }\n",
		"capture bad return": "func (a *A) Snapshot() []byte { return nil }\n",
		"restore bad arg":    "func (a *A) Restore() error { return nil }\n",
	} {
		src := "package x\n\nimport \"context\"\n\n" +
			"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
			method
		dir := writePkg(t, map[string]string{"x.go": src})
		pkg, err := LoadPackage(dir, "")
		if err != nil {
			t.Fatalf("LoadPackage: %v", err)
		}
		_, err = planGen(pkg)
		if err == nil || !strings.Contains(err.Error(), "snapshot face") {
			t.Fatalf("%s: err = %v, want 含 snapshot face 形态要求", name, err)
		}
	}
}

// TestEntitySnapshotFacePairGate 成对门卫：只实现两个方法之一无法满足
// rans.Snapshotter——静默忽略会掩盖意图（生成半边透传、接口不满足），
// 生成期拦截。
func TestEntitySnapshotFacePairGate(t *testing.T) {
	src := "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) Snapshot() ([]byte, error) { return nil, nil }\n"
	dir := writePkg(t, map[string]string{"x.go": src})
	pkg, err := LoadPackage(dir, "")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	_, err = planGen(pkg)
	if err == nil || !strings.Contains(err.Error(), "Snapshot") ||
		!strings.Contains(err.Error(), "Restore") {
		t.Fatalf("err = %v, want 指出快照面须成对实现", err)
	}
}

// TestEntityDemoDriftLock 入库 rans_gen.go 与再生成一致（生成器改动未重新
// 生成即红）。
func TestEntityDemoDriftLock(t *testing.T) {
	pkg, err := LoadPackage(entityFixtureDir, "rans_gen.go")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	out := entityRender(t, pkg)
	committed, err := os.ReadFile(filepath.Join(entityFixtureDir, "rans_gen.go"))
	if err != nil {
		t.Fatalf("读入库生成物: %v", err)
	}
	if !bytes.Equal(out, committed) {
		t.Fatalf("入库 rans_gen.go 与再生成不一致（需重新生成并入库）:\n%s", out)
	}
}

// TestEntityDemoPackageGreen 编译 + 行为验证：显式路径跑 fixture 包自身的
// 测试（testdata 不进 ./... 遍历）。
func TestEntityDemoPackageGreen(t *testing.T) {
	cmd := exec.Command("go", "test", "./testdata/entity_demo")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go test %s 失败:\n%s", entityFixtureDir, out)
	}
}
