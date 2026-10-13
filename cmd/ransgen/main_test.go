// main_test 覆盖 main 流程面（多目录 generate 编排）：跨包 wire 注册名
// 唯一性门卫与「失败即零落盘」语义——各包生成物的 init 都向全局 codec
// 注册表登记同名消息，冲突漏过生成期就会延迟到运行期 init panic。
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGenerateCrossPackageWireNameGate generate 多目录模式下 wire 注册名
// 跨包冲突 → 生成期报错（带双方 file:line），且任何目录都不落盘。
func TestGenerateCrossPackageWireNameGate(t *testing.T) {
	src := "package x\n\n// Dup 跨包重名消息。\ntype Dup struct {\n\tA int64 " + orderTag(1) + "\n}\n"
	d1 := writePkg(t, map[string]string{"a.go": src})
	d2 := writePkg(t, map[string]string{"b.go": src})
	err := runAll([]string{d1, d2}, "rans_gen.go")
	if err == nil || !strings.Contains(err.Error(), "duplicate wire message") {
		t.Fatalf("err = %v, want 含 duplicate wire message", err)
	}
	for _, frag := range []string{`"Dup"`, filepath.Join(d1, "a.go") + ":", filepath.Join(d2, "b.go") + ":"} {
		if !strings.Contains(err.Error(), frag) {
			t.Fatalf("err 缺冲突位置 %q:\n%v", frag, err)
		}
	}
	for _, dir := range []string{d1, d2} {
		if _, serr := os.Stat(filepath.Join(dir, "rans_gen.go")); serr == nil {
			t.Fatalf("冲突未拦截即落盘 %s", dir)
		}
	}
}

// TestGenerateSingleDirStillWrites 单目录经同一编排面照常落盘（门卫只在
// 冲突时收窄，合法输入行为不变）。
func TestGenerateSingleDirStillWrites(t *testing.T) {
	d1 := writePkg(t, map[string]string{"a.go": mainFixture(t)})
	if err := runAll([]string{d1}, "rans_gen.go"); err != nil {
		t.Fatalf("runAll: %v", err)
	}
	if _, err := os.Stat(filepath.Join(d1, "rans_gen.go")); err != nil {
		t.Fatalf("单目录生成物未落盘: %v", err)
	}
}
