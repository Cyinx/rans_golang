package lifecycle

// Retire「有状态实体 + 未配置 Store」部署缺口告警的锚：
// warnStatefulNoStore 的单胜粒度为 Table 级——同 Table
// 内多次退役有状态实体只告警一次；不同 Table 各自告警一次（进程级
// sync.Once 会让第二个 Table 的同类缺口静默，且无法在进程内
// 复验）。日志断言经 log.SetLogger 注入捕获 handler（门面注释明示的
// 测试用法），Cleanup 恢复。

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/log"
)

// warnCapture 捕获 WARN 记录的 slog handler（含消息文本的 append-only 面）。
type warnCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (h *warnCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *warnCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}
func (h *warnCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warnCapture) WithGroup(string) slog.Handler      { return h }

func (h *warnCapture) countContaining(sub string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, m := range h.msgs {
		if strings.Contains(m, sub) {
			n++
		}
	}
	return n
}

// TestRetireStatefulNoStoreWarnsOncePerManager 同 Table 两次退役有状态
// 实体（无 Store）恰一次告警；第二个 Table 首次退役再告警一次——单胜
// 粒度 = Table。
func TestRetireStatefulNoStoreWarnsOncePerManager(t *testing.T) {
	const sub = "retiring stateful entity without checkpoint store"
	capture := &warnCapture{}
	prev := log.Logger()
	log.SetLogger(slog.New(capture))
	t.Cleanup(func() { log.SetLogger(prev) })

	kinds := map[string]rans.Factory{"snap": func(_ rans.ID) rans.Entity { return &snapEntity{} }}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	retire := func(m *activation.Table, id rans.ID) {
		t.Helper()
		if _, err := m.InvokeLocal(ctx, id, 0); err != nil { // 激活
			t.Fatalf("activate %s: %v", id.String(), err)
		}
		DestroyLocal(ctx, m, id) // DestroyLocal → Retire 终局（等待由 Done 契约承载）
		if _, ok := m.GetLocal(id); ok {
			t.Fatalf("destroy %s: 实体未被终局逐出", id.String())
		}
	}

	m1 := activation.New(inmem.New(), 1, kinds)
	x1 := mustTestID(t, "snap", "no-store-1a")
	x2 := mustTestID(t, "snap", "no-store-1b")
	retire(m1, x1)
	retire(m1, x2)
	if got := capture.countContaining(sub); got != 1 {
		t.Fatalf("同 Table 两次退役有状态实体（无 Store）告警 %d 次, want 1", got)
	}

	m2 := activation.New(inmem.New(), 2, kinds)
	retire(m2, mustTestID(t, "snap", "no-store-2a"))
	if got := capture.countContaining(sub); got != 2 {
		t.Fatalf("第二个 Table 首次退役必须再告警一次（Table 级单胜）, 共 %d 条, want 2", got)
	}
}
