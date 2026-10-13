package rans_test

// 导入图门禁（约定 spec §3.2「导入图测试按下表逐条锁定」+ HR3 票 17 的
// testhooks 扩展行）：遍历模块内全部包的非测试导入，断言「可依赖」之外的模块内导入为
// 零。表与实况的对应裁定（票 21 并入 spec 前的正本记录）：
//
//   - 「叶包」= internal/ctxkey、internal/log、internal/protocol、
//     internal/metrics 四个共享小包——规格把 protocol/metrics 与 codec 并列
//     在「可依赖 rans、叶包」一行，即三者同属叶层工具面（metrics 依赖 log，
//     非零依赖，但仍是全内部包共用的叶层小包）；
//   - 同一行内的包组互可依赖（行分组的自然读法）：internal/transport ↔
//     internal/transport/tcp、routing/placement/membership 三包之间；
//   - 门禁只辖生产依赖面：测试文件的导入不进图（_test.go 可自由引用被测
//     面之外的道具——锚点/脚手架不构成包间生产耦合）。
//
// 历史行：edge 与 internal/edgesrv 两行随 v0.24.0 edge 部件整体退役删除
// （公开包收敛、rpc 行「以上全部内部包」的字面收口随之自然成立）。

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// modulePath 本模块的导入路径前缀（go.mod module 行）。
const modulePath = "github.com/cyinx/rans_golang"

// leafPackages 「叶包」：全内部包可依赖的四个共享小包（见文件头裁定）。
var leafPackages = []string{
	"internal/ctxkey",
	"internal/log",
	"internal/protocol",
	"internal/metrics",
}

// depRule 一行依赖规则：成员包各自可依赖 allowed 中的模块内包；
// intra 为真时行内成员互可依赖（行分组的自然读法）。
type depRule struct {
	members  []string
	allowed  []string
	intraRow bool
}

func concat(groups ...[]string) []string {
	var out []string
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

// ransPublic 全部公开运行时包（「公开包」的收口：不含 testkit、examples
// 与 cmd/ransgen——工具/示例不进运行时依赖面）。
var ransPublic = []string{
	"", // package rans（仓库根）
	"node",
	"codec",
	"checkpoint",
	"directory",
	"directory/inmem",
	"directory/redis",
	"directory/contract",
}

// allInternal 表中 rpc 行之前声明的全部内部包（internal/testhooks 除外）。
var allInternalExceptTesthooks = []string{
	"internal/activation",
	"internal/lifecycle",
	"internal/ctxkey",
	"internal/log",
	"internal/membership",
	"internal/metrics",
	"internal/pawnite",
	"internal/placement",
	"internal/protocol",
	"internal/routing",
	"internal/rpc",
	"internal/transport",
	"internal/transport/tcp",
	"internal/wire",
}

// depRules §3.2 依赖规则表 + 扩展行（testhooks），逐行对应 spec 原文；与
// 实况的裁定差异见文件头注释。
var depRules = []depRule{
	// 零依赖叶包。
	{members: []string{"internal/ctxkey", "internal/log"}},
	// rans 只依赖两个零依赖叶包。
	{members: []string{""}, allowed: []string{"internal/ctxkey", "internal/log"}},
	{members: []string{"codec", "internal/protocol", "internal/metrics"},
		allowed: concat([]string{""}, leafPackages)},
	{members: []string{"directory", "checkpoint"}, allowed: []string{""}},
	{members: []string{"directory/redis", "directory/inmem", "directory/contract"},
		allowed: []string{"directory", "", "internal/log"}},
	{members: []string{"internal/wire"},
		allowed: concat([]string{"codec", "internal/protocol", ""}, leafPackages)},
	{members: []string{"internal/transport", "internal/transport/tcp"}, intraRow: true,
		allowed: concat([]string{"internal/wire", "internal/protocol", ""}, leafPackages)},
	{members: []string{"internal/pawnite"},
		allowed: concat([]string{"", "internal/protocol"}, leafPackages)},
	{members: []string{"internal/activation"},
		allowed: concat([]string{"internal/pawnite", "directory", "checkpoint", ""}, leafPackages)},
	{members: []string{"internal/lifecycle"},
		allowed: concat([]string{"internal/activation", "internal/pawnite", "directory", "checkpoint", ""}, leafPackages)},
	{members: []string{"internal/routing", "internal/placement", "internal/membership"}, intraRow: true,
		allowed: concat([]string{"directory", ""}, leafPackages)},
	// rpc：「以上全部内部包（testhooks 除外）」+ codec/directory/checkpoint/rans。
	{members: []string{"internal/rpc"},
		allowed: concat([]string{"codec", "directory", "checkpoint", ""}, allInternalExceptTesthooks)},
	// node：全部公开包（testkit 除外）与全部内部包。
	{members: []string{"node"},
		allowed: concat(ransPublic, allInternalExceptTesthooks, []string{"internal/testhooks"})},
	// testkit：node、公开包、internal/testhooks。
	{members: []string{"testkit"},
		allowed: concat(ransPublic, []string{"node", "internal/testhooks"})},
	// examples、testkit/sigtermhelper、testkit/genfixture、cmd/ransgen：公开包
	//（ransgen 不依赖运行时；genfixture 是生成实体冒烟 fixture——生成物入库
	// 形态与 examples 同款，只 import rans/codec 公开包）。
	{members: []string{"examples/arbitration", "examples/chat", "examples/counter", "examples/reenter", "testkit/sigtermhelper", "testkit/genfixture", "cmd/ransgen"},
		allowed: ransPublic},
	// ---- 扩展行 ----
	// internal/testhooks：仅 testkit 使用的逐节点注入面；模块内依赖实况 =
	// {rans}（Hooks.Resolve 携带 rans.NodeID——票 10 落位实况，票 21 并入
	// spec 时一并记）。
	{members: []string{"internal/testhooks"}, allowed: []string{""}},
}

// ruleFor 返回 pkg 的规则行；未知包组即失败（表完备性：新包必须登记）。
func ruleFor(t *testing.T, pkg string) depRule {
	t.Helper()
	for _, r := range depRules {
		if slices.Contains(r.members, pkg) {
			return r
		}
	}
	t.Errorf("package %q is not covered by any §3.2 dependency rule row (register it in depgraph_test.go)", pkg)
	return depRule{}
}

// TestImportGraph_AllowedEdges 遍历模块目录树，对每个包断言：非测试文件的
// 模块内导入 ⊆ 该包所属规则行的「可依赖」集合（+ 行内互依）。未知包组、
// 越界导入逐条列出——§3.2 表之外的模块内依赖一律失败。
func TestImportGraph_AllowedEdges(t *testing.T) {
	root, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	type violation struct{ pkg, imp string }
	var violations []violation
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			// 隐藏目录（.git 等）与 testdata 约定目录不入图。
			if name != "." && (strings.HasPrefix(name, ".") || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		pkg := strings.ReplaceAll(rel, string(filepath.Separator), "/")
		if pkg == "." {
			pkg = "" // 仓库根 = package rans
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		rule := ruleFor(t, pkg)
		allowed := append([]string(nil), rule.allowed...)
		if rule.intraRow {
			allowed = append(allowed, rule.members...)
		}
		for _, imp := range file.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)
			if !strings.HasPrefix(importPath, modulePath) {
				continue // 模块外依赖不在本门禁范围
			}
			inModule := strings.Trim(strings.TrimPrefix(importPath, modulePath), "/")
			if !slices.Contains(allowed, inModule) {
				violations = append(violations, violation{pkg: pkg, imp: inModule})
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range violations {
		t.Errorf("import graph violation: %q must not import %q (§3.2 dependency rules)", v.pkg, v.imp)
	}
}
