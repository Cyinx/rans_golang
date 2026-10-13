package log

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// newCapture 构造文本 handler 收集 logger（线程安全：handler 写入走锁）。
type capture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *capture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *capture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

func (c *capture) count(sub string) int { return strings.Count(c.String(), sub) }

// captureLogger 经 log 门面注入的收集 logger。
func captureLogger(c *capture) *slog.Logger { return slog.New(slog.NewTextHandler(c, nil)) }

// TestDefaultFollowsSlogDefault 未设置时门面透传 slog.Default()：Logger()
// 返回同一实例，且宿主换 slog.SetDefault 后门面跟随（默认行为与库内
// 直用包级 slog 完全一致——门面不冻结默认值）。
func TestDefaultFollowsSlogDefault(t *testing.T) {
	SetLogger(nil) // 显式回到默认态（不依赖测试顺序）
	if got := Logger(); got != slog.Default() {
		t.Fatalf("unset facade Logger()=%v want slog.Default()", got)
	}
	swap := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	old := slog.Default()
	slog.SetDefault(swap)
	t.Cleanup(func() { slog.SetDefault(old) })
	if got := Logger(); got != swap {
		t.Fatalf("facade must follow slog.SetDefault, Logger()=%v want swap", got)
	}
}

// TestSetLoggerSwapsAndRestores 门面可换：SetLogger 后 Warn/Error/Info/Debug
// 全部经注入 logger 输出（结构化字段在位）；SetLogger(nil) 恢复默认透传。
func TestSetLoggerSwapsAndRestores(t *testing.T) {
	t.Cleanup(func() { SetLogger(nil) })
	c := &capture{}
	SetLogger(captureLogger(c))

	Warn("warn-msg", "k", "v")
	Error("error-msg")
	Info("info-msg")
	Debug("debug-msg") // 默认 handler 级别 INFO，Debug 不落盘——只验证不 panic

	for _, want := range []string{"warn-msg", "k=v", "error-msg", "info-msg"} {
		if c.count(want) == 0 {
			t.Fatalf("facade must route through injected logger, missing %q in %q", want, c.String())
		}
	}

	// 恢复默认：后续输出不再进 capture
	SetLogger(nil)
	Warn("after-restore")
	if c.count("after-restore") != 0 {
		t.Fatalf("restored facade must not write to previous logger, got %q", c.String())
	}
	if got := Logger(); got != slog.Default() {
		t.Fatalf("restored Logger()=%v want slog.Default()", got)
	}
}

// TestConcurrentSwapAndLog 换 logger 与打日志并发（-race 下验证热路径
// 无锁原子读写的安全性：SetLogger 与 Warn/Error 并发不竞态、不丢 panic）。
func TestConcurrentSwapAndLog(t *testing.T) {
	t.Cleanup(func() { SetLogger(nil) })
	c := &capture{}
	SetLogger(captureLogger(c))
	var logWG, swapWG sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 2; i++ {
		swapWG.Add(1)
		go func() {
			defer swapWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
					SetLogger(captureLogger(c))
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		logWG.Add(1)
		go func() {
			defer logWG.Done()
			for j := 0; j < 500; j++ {
				Warn("concurrent-warn", "j", j)
				Error("concurrent-error")
			}
		}()
	}
	logWG.Wait() // 先等打日志侧结束，再停换 logger 侧
	close(stop)
	swapWG.Wait()
	if c.count("concurrent-warn") != 2000 {
		t.Fatalf("concurrent warns=%d want 2000", c.count("concurrent-warn"))
	}
}
