// proxy_test 覆盖调用生成器（规格 §6）：M402 返回形态门卫、参数
// 形态门卫、派生请求类型与用户手写共存规则、proxy/dispatch 渲染细节、
// -out 生成文件排除（幂等根基）与 run 两跑幂等。
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// proxySrc 代理渲染 fixture：三形态方法齐备（(T, error)/(error)/OneWay）。
const proxySrc = `package chat

import "context"

// Room 实体。
//
//rans:kind=room
type Room struct{}

// Say (T, error) 形态。
func (r *Room) Say(ctx context.Context, msg string) (string, error) { return msg, nil }

// Join (error) 形态。
func (r *Room) Join(ctx context.Context, who string) error { return nil }

// Ping OneWay 形态。
func (r *Room) Ping(ctx context.Context) {}
`

// badReturnsSrc 构造 M402 fixture：kind=a 实体带一个 Bad 方法（返回形态可变）。
func badReturnsSrc(rets string) string {
	return "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) Bad(ctx context.Context) " + rets + " {}\n"
}

// planFromSrc 内联源码 → LoadPackage + planGen（门卫断言复用）。
func planFromSrc(t *testing.T, src string, files map[string]string) error {
	t.Helper()
	if files == nil {
		files = map[string]string{"x.go": src}
	}
	dir := writePkg(t, files)
	pkg, err := LoadPackage(dir, "rans_gen.go")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	_, err = planGen(pkg)
	return err
}

// TestM402ReturnShapeGate M402：支持集外返回形态生成失败，错误含实体/方法名。
func TestM402ReturnShapeGate(t *testing.T) {
	for _, rets := range []string{
		"int",                  // 裸类型无 error
		"(int, string, error)", // 多返回值（三值）
		"(int, int)",           // 双返回无 error
		"*Room",                // 指针裸类型
		"(string)",             // 括号单返回（裸类型）
	} {
		t.Run(rets, func(t *testing.T) {
			err := planFromSrc(t, badReturnsSrc(rets), nil)
			if err == nil {
				t.Fatalf("返回形态 %s 未被 M402 拦截", rets)
			}
			for _, want := range []string{"Bad", "M402"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want 含 %q", err, want)
				}
			}
		})
	}
}

// TestM402AcceptedShapes 命名/括号化 (T, error) 与 (error) 均为合法形态。
func TestM402AcceptedShapes(t *testing.T) {
	for _, rets := range []string{
		"(out int64, err error)", // 命名返回
		"(int64, error)",         // 括号化未命名
		"error",                  // 单 error
	} {
		t.Run(rets, func(t *testing.T) {
			if err := planFromSrc(t, badReturnsSrc(rets), nil); err != nil {
				t.Fatalf("合法形态 %s 被误拦: %v", rets, err)
			}
		})
	}
}

// TestMethodContextFirstGate 方法首参必须是 context.Context。
func TestMethodContextFirstGate(t *testing.T) {
	src := "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) Bad(who string) error { return nil }\n"
	err := planFromSrc(t, src, nil)
	if err == nil || !strings.Contains(err.Error(), "Bad") ||
		!strings.Contains(err.Error(), "context.Context") {
		t.Fatalf("err = %v, want 含 Bad 与 context.Context", err)
	}
}

// TestArgTypeGateReusesS004 方法参数类型支持集外 → 复用序列化器字段门卫
// （S004/M405），错误含 wire 路由名与参数类型。
func TestArgTypeGateReusesS004(t *testing.T) {
	src := "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) Bad(ctx context.Context, c chan int) error { return nil }\n"
	err := planFromSrc(t, src, nil)
	if err == nil || !strings.Contains(err.Error(), "S004/M405") ||
		!strings.Contains(err.Error(), "chan int") ||
		!strings.Contains(err.Error(), "a.Bad") {
		t.Fatalf("err = %v, want 含 S004/M405、chan int 与路由名 a.Bad", err)
	}
}

// userReqSrc 用户手写请求类型共存 fixture：字段名与参数名不同（按 order 对位）。
const userReqSrc = `package x

import "context"

// A 实体。
//
//rans:kind=a
type A struct{}

// Reset (error) 形态。
func (a *A) Reset(ctx context.Context, to int64) error { return nil }

// AResetRequest 手写请求类型：order 对位（order=1 ↔ 第一个参数 to）。
type AResetRequest struct {
	Val int64 ` + "`rans:\"order=1\"`" + `
}
`

// TestUserRequestDiscoveryAndSkip 发现规则：用户手写同名请求类型 → 不重复
// 声明、注册名挂 <Kind>.<Method>、分发/构造按用户字段名。
func TestUserRequestDiscoveryAndSkip(t *testing.T) {
	dir := writePkg(t, map[string]string{"x.go": userReqSrc})
	pkg, err := LoadPackage(dir, "rans_gen.go")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	model, err := planGen(pkg)
	if err != nil {
		t.Fatalf("planGen: %v", err)
	}
	out, err := Render(model, ransgenTemplate)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if n := bytes.Count(out, []byte("type AResetRequest")); n != 0 {
		t.Fatalf("生成物重复声明了用户手写的 AResetRequest（%d 次）:\n%s", n, out)
	}
	for _, want := range []string{
		`codec.Register("a.Reset", AResetRequest{}`,
		"w.E.Reset(ctx, r.Val)",
		"AResetRequest{Val: Val}, opts...",
	} {
		if !bytes.Contains(out, []byte(want)) {
			t.Fatalf("生成物缺 %q:\n%s", want, out)
		}
	}
}

// TestUserRequestWithoutTagsFails 手写同名请求类型无 order tag 字段 → 生成
// 失败（字段无法对位）。
func TestUserRequestWithoutTagsFails(t *testing.T) {
	src := strings.Replace(userReqSrc,
		"Val int64 `rans:\"order=1\"`", "Val int64", 1)
	err := planFromSrc(t, src, nil)
	if err == nil || !strings.Contains(err.Error(), "AResetRequest") ||
		!strings.Contains(err.Error(), "order") {
		t.Fatalf("err = %v, want 含 AResetRequest 与 order 提示", err)
	}
}

// TestUserRequestFieldMismatch 用户请求字段类型与参数类型不符 → 生成失败
// （分发调用形态对不上）。
func TestUserRequestFieldMismatch(t *testing.T) {
	src := strings.Replace(userReqSrc, "Val int64", "Val string", 1)
	err := planFromSrc(t, src, nil)
	if err == nil || !strings.Contains(err.Error(), "Val") ||
		!strings.Contains(err.Error(), "does not match") {
		t.Fatalf("err = %v, want 含字段名与 does not match", err)
	}
}

// TestUserRequestMissingOrderField 用户请求缺某参数序的字段 → 生成失败。
func TestUserRequestMissingOrderField(t *testing.T) {
	src := strings.Replace(userReqSrc, `rans:"order=1"`, `rans:"order=2"`, 1)
	err := planFromSrc(t, src, nil)
	if err == nil || !strings.Contains(err.Error(), "order=1") {
		t.Fatalf("err = %v, want 含 order=1 缺失提示", err)
	}
}

// TestDerivedWireNameCollision 派生请求 wire 名与用户消息显式名相撞 → 生成
// 失败（注册名唯一）。
func TestDerivedWireNameCollision(t *testing.T) {
	src := userReqSrc + "\n// M 显式命名撞派生请求名。\n//\n//rans:name=a.Reset\ntype M struct {\n\tF int64 " + orderTag(1) + "\n}\n"
	err := planFromSrc(t, src, nil)
	if err == nil || !strings.Contains(err.Error(), "duplicate message name") {
		t.Fatalf("err = %v, want 含 duplicate message name", err)
	}
}

// TestProxyRenderDetails 代理/分发渲染细节：请求类型生成、类型 switch 分发、
// Call/SendOneWay 分路、应答类型断言与未知请求防线。
func TestProxyRenderDetails(t *testing.T) {
	dir := writePkg(t, map[string]string{"chat.go": proxySrc})
	pkg, err := LoadPackage(dir, "rans_gen.go")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	model, err := planGen(pkg)
	if err != nil {
		t.Fatalf("planGen: %v", err)
	}
	out, err := Render(model, ransgenTemplate)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	for _, want := range []string{
		"type RoomClient struct {\n\tC rans.Client\n}",
		"func (p RoomClient) Say(ctx context.Context, id rans.ID, Msg string, opts ...rans.CallOption) (string, error)",
		"resp, err := p.C.Call(ctx, id, RoomSayRequest{Msg: Msg}, opts...)",
		"v, ok := resp.(string)",
		"return p.C.Tell(ctx, id, RoomPingRequest{}, opts...)",
		"// Ping 单向发送",
		`codec.Register("room.Say"`,
		`codec.Register("room.Ping"`,
		"type RoomSayRequest struct {\n\tMsg string " + orderTag(1) + "\n}",
		"type RoomPingRequest struct {",
		"case RoomSayRequest:",
		"w.E.Say(ctx, r.Msg)",
		"if err := w.E.Join(ctx, r.Who); err != nil {",
		`"github.com/cyinx/rans_golang"`,
	} {
		if !bytes.Contains(out, []byte(want)) {
			t.Fatalf("生成物缺 %q:\n%s", want, out)
		}
	}
}

// TestOutFileExcludedFromParse -out 排除：生成物内派生请求
// 类型二跑不得被重扫为消息——LoadPackage 带 skip 时不出现以类型名为注册名
// 的消息，不带 skip 时出现（对照组，证明确有排除必要）。
func TestOutFileExcludedFromParse(t *testing.T) {
	// proxySrc 无用户 Room*Request 类型：生成后文件里的 RoomSayRequest 只能
	// 来自重扫。
	dir := writePkg(t, map[string]string{"chat.go": proxySrc})
	if err := run(dir, "rans_gen.go"); err != nil {
		t.Fatalf("first run: %v", err)
	}
	hasTypeNameRegistration := func(skip string) bool {
		pkg, err := LoadPackage(dir, skip)
		if err != nil {
			t.Fatalf("LoadPackage(skip=%q): %v", skip, err)
		}
		for _, m := range pkg.Messages {
			if m.Name == "RoomSayRequest" { // 重扫特征：注册名落回类型名
				return true
			}
		}
		return false
	}
	if hasTypeNameRegistration("rans_gen.go") {
		t.Fatal("排除 -out 后仍重扫到生成文件里的请求类型")
	}
	if !hasTypeNameRegistration("") {
		t.Fatal("对照组未重扫到生成文件里的请求类型（排除语义未被证伪）")
	}
}

// TestRunIdempotentWithDerivedRequests 两跑幂等：run 排除 -out 后输入视角
// 不变，第二跑零写入（mtime 保持）且生成物字节稳定。
func TestRunIdempotentWithDerivedRequests(t *testing.T) {
	dir := writePkg(t, map[string]string{"x.go": proxySrc})
	outPath := filepath.Join(dir, "rans_gen.go")

	if err := run(dir, "rans_gen.go"); err != nil {
		t.Fatalf("first run: %v", err)
	}
	first, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("生成物未落盘: %v", err)
	}

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
		t.Fatal("第二跑重写了文件（生成不幂等）")
	}
	second, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("两跑生成物字节不一致")
	}
}

// TestUnnamedParamArgNSynthesis 未命名参数 → 按参数位合成 ArgN 字段名；
// run 全流程渲染干净（format.Source 通过 = 无空字段名碎裂）。
func TestUnnamedParamArgNSynthesis(t *testing.T) {
	src := "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) Mix(context.Context, int64, string) (int64, error) { return 0, nil }\n"
	dir := writePkg(t, map[string]string{"x.go": src})
	if err := run(dir, "rans_gen.go"); err != nil {
		t.Fatalf("run: %v", err)
	}
	out, err := os.ReadFile(filepath.Join(dir, "rans_gen.go"))
	if err != nil {
		t.Fatalf("读生成物: %v", err)
	}
	for _, want := range []string{
		"type AMixRequest struct {",
		"Arg1 int64",
		"Arg2 string",
		orderTag(1),
		orderTag(2),
		"w.E.Mix(ctx, r.Arg1, r.Arg2)",
		"AMixRequest{Arg1: Arg1, Arg2: Arg2}",
		`codec.Register("a.Mix"`,
	} {
		if !bytes.Contains(out, []byte(want)) {
			t.Fatalf("生成物缺 %q:\n%s", want, out)
		}
	}
}

// TestUnexportedMethodsExcludedFromFace 未导出方法不入 RPC 面（无请求类型/
// 注册/代理/分发 case）；导出方法照常进面。
func TestUnexportedMethodsExcludedFromFace(t *testing.T) {
	src := `package x

import "context"

// A 实体。
//
//rans:kind=a
type A struct{}

// Pub 导出面。
func (a *A) Pub(ctx context.Context, s string) error { return nil }

// helper 内部辅助：不入 RPC 面。
func (a *A) helper(ctx context.Context, x int64) error { return nil }

// secret 未导出且无参数。
func (a *A) secret() {}
`
	dir := writePkg(t, map[string]string{"x.go": src})
	pkg, err := LoadPackage(dir, "rans_gen.go")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	model, err := planGen(pkg)
	if err != nil {
		t.Fatalf("planGen: %v", err)
	}
	out, err := Render(model, ransgenTemplate)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !bytes.Contains(out, []byte("APubRequest")) {
		t.Fatalf("导出方法未进 RPC 面:\n%s", out)
	}
	for _, banned := range []string{
		"AHelperRequest", "ASecretRequest",
		`"a.helper"`, `"a.secret"`,
		"func (p AClient) helper", "func (p AClient) secret",
	} {
		if bytes.Contains(out, []byte(banned)) {
			t.Fatalf("未导出方法泄漏进生成面（%q）:\n%s", banned, out)
		}
	}
}

// m405Src 构造 M405 返回类型 fixture：ret 为首个返回类型，extra 为附加的
// 类型声明源（如未注册 struct）。
func m405Src(ret string, extra string) string {
	return "package x\n\nimport \"context\"\n\n" + extra + "\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) Bad(ctx context.Context) (" + ret + ", error) { var v " + ret + "; return v, nil }\n"
}

// TestM405ReturnTypeGate M405 返回类型门卫：非标量且非已声明消息（指针、
// 未声明 struct、接口 error、未注册切片）→ 生成失败，错误含实体/方法/类型
// 与 [M405]。
func TestM405ReturnTypeGate(t *testing.T) {
	pointNoTags := "// Point 无 rans tag：声明类型但非消息。\ntype Point struct{ X int64 }\n"
	pointTagged := "// Point 已声明消息。\ntype Point struct {\n\tX int64 " + orderTag(1) + "\n}\n"
	for _, tc := range []struct {
		name  string
		ret   string
		extra string
	}{
		{"pointer to scalar", "*string", ""},
		{"pointer to declared message", "*Point", pointTagged},
		{"undeclared struct", "Point", pointNoTags},
		{"interface error (double error shape)", "error", ""},
		{"unregistered slice", "[]string", ""},
		{"slice of messages", "[]Point", pointTagged},
		{"qualified type", "time.Time", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := planFromSrc(t, m405Src(tc.ret, tc.extra), nil)
			if err == nil {
				t.Fatalf("返回类型 %s 未被 M405 拦截", tc.ret)
			}
			for _, want := range []string{"Bad", tc.ret, "M405"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want 含 %q", err, want)
				}
			}
		})
	}
}

// TestM405AcceptedReturnTypes 标量全集与已声明消息类型可作返回类型。
func TestM405AcceptedReturnTypes(t *testing.T) {
	pointTagged := "// Point 已声明消息。\ntype Point struct {\n\tX int64 " + orderTag(1) + "\n}\n"
	for _, tc := range []struct {
		name  string
		ret   string
		extra string
	}{
		{"int64", "int64", ""},
		{"int", "int", ""},
		{"uint64", "uint64", ""},
		{"string", "string", ""},
		{"bool", "bool", ""},
		{"float64", "float64", ""},
		{"bytes", "[]byte", ""},
		{"declared message", "Point", pointTagged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := planFromSrc(t, m405Src(tc.ret, tc.extra), nil)
			if err != nil {
				t.Fatalf("合法返回类型 %s 被误拦: %v", tc.ret, err)
			}
		})
	}
	// 消息返回的代理断言按声明类型解码。
	dir := writePkg(t, map[string]string{"x.go": m405Src("Point", pointTagged)})
	pkg, err := LoadPackage(dir, "rans_gen.go")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	model, err := planGen(pkg)
	if err != nil {
		t.Fatalf("planGen: %v", err)
	}
	out, err := Render(model, ransgenTemplate)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !bytes.Contains(out, []byte("v, ok := resp.(Point)")) {
		t.Fatalf("生成物缺消息类型应答断言:\n%s", out)
	}
}

// idemSrc 幂等注解 fixture：请求-响应（注解/未注解）+ OneWay（注解——
// SendOneWay 路径不受注解影响，OneWay 无应答语义）。
const idemSrc = `package chat

import "context"

// Room 实体。
//
//rans:kind=room
type Room struct{}

// Set 标注幂等（绝对值设置天然可重放）。
//
//rans:idempotent
func (r *Room) Set(ctx context.Context, v int64) (int64, error) { return v, nil }

// Nudge (error) 形态标注幂等。
//
//rans:idempotent
func (r *Room) Nudge(ctx context.Context) error { return nil }

// Plain 未注解：走默认 Call。
func (r *Room) Plain(ctx context.Context) error { return nil }

// Notify OneWay + 注解：SendOneWay 路径不变。
//
//rans:idempotent
func (r *Room) Notify(ctx context.Context) {}
`

// TestIdempotentProxyAppendsOption //rans:idempotent 注解 → 请求-响应代理
// 默认追加 rans.Idempotent()（调用方选项仍透传）；未注解方法只透传 opts；
// OneWay 路径不受注解影响；生成物不出现 internal 导入（公开包承诺）。
func TestIdempotentProxyAppendsOption(t *testing.T) {
	dir := writePkg(t, map[string]string{"chat.go": idemSrc})
	pkg, err := LoadPackage(dir, "rans_gen.go")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	model, err := planGen(pkg)
	if err != nil {
		t.Fatalf("planGen: %v", err)
	}
	out, err := Render(model, ransgenTemplate)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	for _, want := range []string{
		"resp, err := p.C.Call(ctx, id, RoomSetRequest{V: V}, append(opts, rans.Idempotent())...)",
		"resp, err := p.C.Call(ctx, id, RoomNudgeRequest{}, append(opts, rans.Idempotent())...)",
		"resp, err := p.C.Call(ctx, id, RoomPlainRequest{}, opts...)",
		"return p.C.Tell(ctx, id, RoomNotifyRequest{}, opts...)",
		"opts ...rans.CallOption",
	} {
		if !bytes.Contains(out, []byte(want)) {
			t.Fatalf("生成物缺 %q:\n%s", want, out)
		}
	}
}

// TestGeneratedImportsArePublicOnly 生成物只 import 公开包（约定 spec §5.9
// 的消费方可见性承诺）：不得出现仓内 internal 导入。
func TestGeneratedImportsArePublicOnly(t *testing.T) {
	dir := writePkg(t, map[string]string{"chat.go": proxySrc})
	pkg, err := LoadPackage(dir, "rans_gen.go")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	model, err := planGen(pkg)
	if err != nil {
		t.Fatalf("planGen: %v", err)
	}
	out, err := Render(model, ransgenTemplate)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if bytes.Contains(out, []byte("rans_golang/internal")) {
		t.Fatalf("生成物不得导入 internal 包:\n%s", out)
	}
	if bytes.Contains(out, []byte("rans_golang/codegen")) {
		t.Fatalf("生成物不得导入 codegen/runtime:\n%s", out)
	}
}

// TestIdempotentLifecycleGate 注解在生命周期方法（Activate/Deactivate）上
// → 生成失败（生命周期走 wrapper 委托，无 RPC 面可标幂等）。
func TestIdempotentLifecycleGate(t *testing.T) {
	for _, name := range []string{"Activate", "Deactivate"} {
		t.Run(name, func(t *testing.T) {
			src := "package x\n\nimport \"context\"\n\n" +
				"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
				"// " + name + " 误挂幂等注解。\n//\n//rans:idempotent\n" +
				"func (a *A) " + name + "(ctx context.Context) error { return nil }\n"
			err := planFromSrc(t, src, nil)
			if err == nil || !strings.Contains(err.Error(), name) ||
				!strings.Contains(err.Error(), "idempotent") {
				t.Fatalf("err = %v, want 含方法名与 idempotent", err)
			}
		})
	}
}

// TestIdempotentUnexportedGate 注解在未导出方法上 → 生成失败（未导出方法
// 不入 RPC 面，注解无处生效——静默忽略会掩盖标注笔误）。
func TestIdempotentUnexportedGate(t *testing.T) {
	src := "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"// helper 未导出方法误挂注解。\n//\n//rans:idempotent\n" +
		"func (a *A) helper(ctx context.Context) error { return nil }\n"
	err := planFromSrc(t, src, nil)
	if err == nil || !strings.Contains(err.Error(), "helper") ||
		!strings.Contains(err.Error(), "idempotent") {
		t.Fatalf("err = %v, want 含 helper 与 idempotent", err)
	}
}

// TestDerivedReqTypeCrossCollision 派生请求类型名跨（实体,方法）碰撞门卫：
// A.BCd 与 AB.Cd 同派 ABCdRequest——不得双双渲染 `type ABCdRequest struct`
// 交由编译器重声明兜底（无 file:line 指引），生成期须报出类型名与两个来源。
func TestDerivedReqTypeCrossCollision(t *testing.T) {
	src := "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) BCd(ctx context.Context, v int64) error { return nil }\n\n" +
		"// AB 实体。\n//\n//rans:kind=ab\ntype AB struct{}\n\n" +
		"func (ab *AB) Cd(ctx context.Context, v int64) error { return nil }\n"
	err := planFromSrc(t, src, nil)
	if err == nil || !strings.Contains(err.Error(), "ABCdRequest") ||
		!strings.Contains(err.Error(), "A.BCd") || !strings.Contains(err.Error(), "AB.Cd") {
		t.Fatalf("err = %v, want 含 ABCdRequest 与两个来源 A.BCd/AB.Cd", err)
	}
}

// TestAdoptDoesNotSilentlyOverrideExplicitName 采纳同名用户请求类型的门卫：
// 不得静默丢弃其显式 //rans:name（显式名表达独立注册意图，无声改挂 wire
// 路由名会丢失之）。带显式名的类型被采纳 → 生成期报错（去注解或改类型名
// 二选一）；不带显式名的手写类型照常采纳（既有夹具 CounterResetRequest
// 覆盖）。
func TestAdoptDoesNotSilentlyOverrideExplicitName(t *testing.T) {
	src := "package x\n\nimport \"context\"\n\n" +
		"// A 实体。\n//\n//rans:kind=a\ntype A struct{}\n\n" +
		"func (a *A) Reset(ctx context.Context, v int64) error { return nil }\n\n" +
		"// 手写请求类型恰命中派生名，且带显式命名。\n//\n//rans:name=my.reset\n" +
		"type AResetRequest struct {\n\tV int64 `rans:\"order=1\"`\n}\n"
	err := planFromSrc(t, src, nil)
	if err == nil || !strings.Contains(err.Error(), "my.reset") ||
		!strings.Contains(err.Error(), "a.Reset") {
		t.Fatalf("err = %v, want 显式名 my.reset 与路由名 a.Reset 同时出现在报错中", err)
	}
}
