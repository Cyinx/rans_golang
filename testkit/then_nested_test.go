package testkit

// 续体内再让出（P3）验收：then 的 ctx 携带让出登记点与定时器点，因此「IO →
// 记账 → 再 IO」不必再给自身自投一条消息；嵌套 work 继承续体剩余预算。

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// TestReenterAfter_NestedYieldInThen 续体内再让出生效（修复前 panic 于「called
// outside a runtime Handle ctx」——与公开 godoc「让出面保持」正面冲突）。
func TestReenterAfter_NestedYieldInThen(t *testing.T) {
	report := make(chan string, 8)
	c := NewCluster(t, 1, timerKinds(report))
	id, _ := rans.NewID("timerprobe", "n1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, err := c.Node(0).Call(ctx, id, "yield-then-yield")
	if err != nil || v != "yielded" {
		t.Fatalf("yield-then-yield: v=%v err=%v", v, err)
	}
	got := collectReports(t, report, 6)
	wantSameDeadline(t, got)
	wantReportSet(t, dropPrefix(got, "handle-deadline="), "outer-work", "outer-then", "outer-then-returned",
		"inner-work has-deadline=true deadline="+innerDeadline(got), "inner-then v=2")
}

// dropPrefix 过滤掉带指定前缀的报告（它们由专门的断言面消费）。
func dropPrefix(got []string, prefix string) []string {
	out := make([]string, 0, len(got))
	for _, s := range got {
		if !strings.HasPrefix(s, prefix) {
			out = append(out, s)
		}
	}
	return out
}

// innerDeadline 从若干报告里挑出内层 work 的 deadline 值（原请求 deadline 的
// 独立事实源在 handle-deadline 报告里，见 wantSameDeadline）。
func innerDeadline(got []string) string {
	for _, s := range got {
		if strings.HasPrefix(s, "inner-work has-deadline=true deadline=") {
			return strings.TrimPrefix(s, "inner-work has-deadline=true deadline=")
		}
	}
	return ""
}

// wantSameDeadline 断言 Handle ctx 与嵌套 work 的 deadline 完全相同（嵌套让出
// 继承续体剩余预算 = 原请求 deadline）。
func wantSameDeadline(t *testing.T, got []string) {
	t.Helper()
	handle := ""
	for _, s := range got {
		if strings.HasPrefix(s, "handle-deadline=") {
			handle = strings.TrimPrefix(s, "handle-deadline=")
		}
	}
	if handle == "" || handle != innerDeadline(got) {
		t.Fatalf("嵌套 work 未继承原请求 deadline：handle=%q inner=%q（报告 %v）",
			handle, innerDeadline(got), got)
	}
}
