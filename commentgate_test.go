package rans_test

// 注释/体量门禁：《约定 spec》§6 的可执行形态，四类指标全部在 make test
// 里看住（裁定正本 docs/decisions/2026-10-10-comment-governance-rulings.md
// 裁定 2/4/5）。治理已完成，门禁为终态硬断言：ISSUES/日期字面 =0（注释面
// 与代码面）、生产体量白名单 = 空（生产文件超 500 行即违规）、测试体量
// 白名单为登记例外只减不增、占比基线只降不升。起步史实：门禁落地时为
// ratchet 形态（生产 ISSUES 78 处/日期 38 处、测试 45/19、占比 28.4%），
// 随治理推进逐步收紧，收尾定格为现形态。
//
// 测量口径（与裁定正本基线节一致，改动口径必须同步基线数据）：
//   - 注释行 = 去首尾空白后以 // 开头；占比分母 = 生产非生成物的全部行；
//   - 代码面 = 其余行（字符串字面与行内块注释在此面，ISSUES 字面同样 =0；
//     日期不查代码面——日期格式串是合法代码字面）；
//   - 显示列 = 中日韩字符（> U+2E80）按 2 列，其余（含 tab）按 1 列；
//   - 生成物以显式清单豁免（不做脆弱的标记探测）；
//   - 含 docs/decisions/ 路径的注释行豁免日期断言（历史指引行是合法形态）。

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// generatedFiles 生成物豁免清单：重生成即覆写，一切本门禁不适用。
var generatedFiles = map[string]bool{
	"examples/chat/rans_gen.go":                    true,
	"cmd/ransgen/testdata/entity_demo/rans_gen.go": true,
	"cmd/ransgen/testdata/serial_demo/rans_gen.go": true,
}

// sizeWhitelist 超 500 行文件登记例外（生产段已清空：软约束只约束生产代码，
// 拆分完成后超标即违规；测试段按裁定 2 登记）。门禁断言「实际超标集合与
// 清单精确一致」：清单外超标是违规，清单内已达标是腐化条目——文件拆小后
// 必须同 commit 移出清单，清单因此只减不增。
var sizeWhitelist = map[string]bool{
	// 生产（0）
	// 测试（19，登记例外：软约束只约束生产代码，裁定 2）
	"directory/redis/redis_test.go":                true,
	"internal/lifecycle/migrate_test.go":           true,
	"internal/transport/tcp/manager_test.go":       true,
	"testkit/migration_acceptance_test.go":         true,
	"internal/pawnite/migrate_test.go":             true,
	"internal/rpc/retry_wiring_test.go":            true,
	"internal/rpc/migration_pkg_test.go":           true,
	"internal/rpc/gateway_test.go":                 true,
	"testkit/invariant_history_test.go":            true,
	"internal/rpc/remote_test.go":                  true,
	"internal/transport/tcp/conn_test.go":          true,
	"internal/pawnite/pawnite_abandon_test.go":     true,
	"internal/pawnite/handoff_test.go":             true,
	"cmd/ransgen/proxy_test.go":                    true,
	"internal/rpc/dispatcher_test.go":              true,
	"internal/lifecycle/idle_test.go":              true,
	"testkit/hr3_acceptance_test.go":               true,
	"testkit/cluster_test.go":                      true,
	"internal/lifecycle/handoff_zero_loss_test.go": true,
}

// 占比基线 = 治理完成实测值内嵌（只降不升）。约束目标 25%：实测跌破 25%
// 后收紧 maxCommentRatio 为 0.25，不为数字删注释。基线重定记录：v0.24.0
// edge 部件整体退役删除约 5.6k 行生产代码（该面注释占比低于全仓均值），
// 分母结构性变化致占比被动抬升——按删除后实测 28.3% 重定基线（非注释
// 回潮，门禁语义不变：此后仍只降不升）。
const (
	maxCommentRatio = 0.284
	maxLineWidth    = 100
	sizeLimit       = 500
)

var (
	reIssues = regexp.MustCompile(`ISSUES [A-Z][0-9]+`)
	reDate   = regexp.MustCompile(`20[0-9]{2}-[0-9]{2}-[0-9]{2}`)
)

type fileStats struct {
	path       string
	isTest     bool
	lines      int
	comments   int
	issues     int
	dates      int
	codeIssues int
	wide       []string
}

// displayWidth 单行显示列：中日韩字符按 2 列，其余（含 tab）按 1 列。
func displayWidth(s string) int {
	w := 0
	for _, r := range s {
		if r > 0x2E80 {
			w += 2
		} else {
			w++
		}
	}
	return w
}

// collectCommentStats 遍历仓库全部 Go 文件（跳过隐藏目录与生成物），按门禁
// 口径汇总每文件指标。
func collectCommentStats(t *testing.T) []fileStats {
	t.Helper()
	root, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	var all []fileStats
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if name != "." && strings.HasPrefix(name, ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if generatedFiles[rel] {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		st := fileStats{path: rel, isTest: strings.HasSuffix(name, "_test.go")}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			s := strings.TrimSpace(sc.Text())
			st.lines++
			if !strings.HasPrefix(s, "//") {
				// 代码面：字符串字面与行内块注释随代码行落在此面。
				if reIssues.MatchString(s) {
					st.codeIssues++
				}
				continue
			}
			st.comments++
			if reIssues.MatchString(s) {
				st.issues++
			}
			if reDate.MatchString(s) && !strings.Contains(s, "docs/decisions/") {
				st.dates++
			}
			if displayWidth(s) > maxLineWidth {
				st.wide = append(st.wide, s)
			}
		}
		if err := sc.Err(); err != nil {
			return err
		}
		all = append(all, st)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return all
}

// TestCommentGate_HistoryMarkers ISSUES 编号与日期字面是沿革叙事的标志物，
// 注释面与代码面（字符串字面/行内块注释）一律 =0 硬断言；含 docs/decisions/
// 路径的指引行豁免日期断言。
func TestCommentGate_HistoryMarkers(t *testing.T) {
	var pi, pd, ti, td, pci, tci int
	for _, st := range collectCommentStats(t) {
		if st.isTest {
			ti += st.issues
			td += st.dates
			tci += st.codeIssues
		} else {
			pi += st.issues
			pd += st.dates
			pci += st.codeIssues
		}
	}
	if pi != 0 {
		t.Errorf("生产注释 ISSUES 字面 %d 处，要求 0", pi)
	}
	if pd != 0 {
		t.Errorf("生产注释日期字面 %d 处，要求 0", pd)
	}
	if pci != 0 {
		t.Errorf("生产代码面 ISSUES 字面 %d 处，要求 0", pci)
	}
	if ti != 0 {
		t.Errorf("测试注释 ISSUES 字面 %d 处，要求 0", ti)
	}
	if td != 0 {
		t.Errorf("测试注释日期字面 %d 处，要求 0", td)
	}
	if tci != 0 {
		t.Errorf("测试代码面 ISSUES 字面 %d 处，要求 0", tci)
	}
	t.Logf("=0 硬断言通过：注释面 ISSUES/日期 生产 %d/%d、测试 %d/%d；代码面 ISSUES 生产 %d、测试 %d",
		pi, pd, ti, td, pci, tci)
}

// TestCommentGate_LineWidth 单行注释不超过 100 显示列（中日韩按 2 列计）。
func TestCommentGate_LineWidth(t *testing.T) {
	for _, st := range collectCommentStats(t) {
		for _, w := range st.wide {
			t.Errorf("%s 注释行超 %d 显示列（%d 列）：%s", st.path, maxLineWidth, displayWidth(w), w)
		}
	}
}

// TestCommentGate_FileSizeWhitelist 文件体量门禁：生产白名单已清空（超 500
// 行即违规）；测试白名单是登记例外（软约束只约束生产代码，裁定 2），实际
// 超 500 行集合与清单精确一致——清单外超标违规、清单内已达标腐化，双向断言。
func TestCommentGate_FileSizeWhitelist(t *testing.T) {
	actual := map[string]int{}
	var prod, test int
	for _, st := range collectCommentStats(t) {
		if st.lines > sizeLimit {
			actual[st.path] = st.lines
			if st.isTest {
				test++
			} else {
				prod++
			}
		}
	}
	if prod != 0 {
		t.Errorf("生产文件超 %d 行 %d 个：生产白名单已清空，超标即违规", sizeLimit, prod)
	}
	for p, n := range actual {
		if !sizeWhitelist[p] {
			t.Errorf("白名单外文件超 %d 行：%s（%d 行）", sizeLimit, p, n)
		}
	}
	for p := range sizeWhitelist {
		if _, ok := actual[p]; !ok {
			t.Errorf("白名单腐化：%s 已 ≤%d 行，必须移出清单", p, sizeLimit)
		}
	}
	t.Logf("超 %d 行文件：生产 %d + 测试 %d（白名单登记例外）", sizeLimit, prod, test)
}

// TestCommentGate_CommentRatio 生产非生成物注释占比：断言不超治理完成基线
// （只降不升；约束目标 25%，实测跌破后收紧常量为 0.25）。
func TestCommentGate_CommentRatio(t *testing.T) {
	var lines, comments int
	for _, st := range collectCommentStats(t) {
		if st.isTest {
			continue
		}
		lines += st.lines
		comments += st.comments
	}
	ratio := float64(comments) / float64(lines)
	if ratio > maxCommentRatio {
		t.Errorf("生产注释占比 %.1f%%，超治理完成基线 %.1f%%", ratio*100, maxCommentRatio*100)
	}
	t.Logf("生产注释占比 %.1f%%（治理完成基线 %.1f%%，只降不升）", ratio*100, maxCommentRatio*100)
}
