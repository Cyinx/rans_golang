package main

// 示例自检测试（票 13 红测形态 TestExampleReenterAfter）：示例编排可运行、
// 输出序列逐行断言——让出起跑 → 让出期间 note 照常服务 → 慢 IO 返回 →
// then 聚合回复。run 的三重同步（起跑信号/应答回执/放行闸）使输出序列
// 确定，断言按出现序扫描（marker 有序即全序列有序）。

import (
	"bytes"
	"strings"
	"testing"
)

func TestExampleReenterAfter(t *testing.T) {
	var buf bytes.Buffer
	if err := run(&buf); err != nil {
		t.Fatalf("run reenter example: %v", err)
	}
	out := buf.String()
	markers := []string{
		"[A] gather 请求：Defer + ReenterAfter 让出",
		`[A] 让出期间照常服务：note "n1"`,
		`[A] 让出期间照常服务：note "n2"`,
		"[main] 放行慢 IO（A 让出期间已服务 2 条 note）",
		"[B] slowio/b-1 慢 IO 返回：n=3",
		"[A] then 聚合：slow=3 notes=[n1 n2]",
		"[main] gather 回复：slow=3 notes=[n1 n2]",
	}
	pos := 0
	for _, m := range markers {
		i := strings.Index(out[pos:], m)
		if i < 0 {
			t.Fatalf("output missing marker %q (or out of order); got:\n%s", m, out)
		}
		pos += i + len(m)
	}
}
