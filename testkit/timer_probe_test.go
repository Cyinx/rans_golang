package testkit

// 每 Pawnite 定时器验收的道具：探针实体（命令驱动）、工厂表与断言辅助。
import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// timerProbeEntity 定时器探针实体：命令形如
//   - arm:<name>:<duration>  预约一条定时回调，回调把 name 与 ctx 观测发回；
//   - remove:<id>            移除并回传 bool。
type timerProbeEntity struct {
	report  chan string
	release chan struct{} // nil = 不阻塞（timer-yield-blocking / timer-stuck 的释放面）
	stuck   chan struct{} // timer-stuck 的卡死闸（关闭放行；避免 zombie 泄漏）
	// reportDeactivate 在 Deactivate 里上报 "deactivated"（闲置钝化用例；其余
	// 用例关着，避免集群收尾的钝化报告污染「静默」断言）。
	reportDeactivate bool
}

func (e *timerProbeEntity) Activate(context.Context) error { return nil }
func (e *timerProbeEntity) Deactivate(context.Context) error {
	if e.reportDeactivate {
		e.report <- "deactivated"
	}
	return nil
}

func (e *timerProbeEntity) Handle(ctx context.Context, req any) (any, error) {
	cmd, _ := req.(string)
	switch {
	case strings.HasPrefix(cmd, "arm:"):
		rest := strings.TrimPrefix(cmd, "arm:")
		sep := strings.LastIndex(rest, ":")
		if sep < 0 {
			return nil, fmt.Errorf("bad arm command %q", cmd)
		}
		name, d := rest[:sep], rest[sep+1:]
		delay, err := time.ParseDuration(d)
		if err != nil {
			return nil, err
		}
		id := rans.AddTimer(ctx, delay, func(cctx context.Context) {
			self, ok := rans.Self(cctx)
			e.report <- fmt.Sprintf("%s self=%s/%s ok=%v client=%v",
				name, self.Kind, self.Key, ok, rans.ClientFrom(cctx) != nil)
		})
		return id, nil
	case cmd == "timer-yield":
		// 回调内让出：契约上 work 在 loop 外执行、then 回 loop 串行。修复前
		// 回调 ctx 无让出登记点，ReenterAfter 在回调里 panic。
		rans.AddTimer(ctx, 10*time.Millisecond, func(cctx context.Context) {
			defer func() {
				if r := recover(); r != nil {
					e.report <- fmt.Sprintf("callback-panicked: %v", r)
				}
			}()
			rans.ReenterAfter(cctx, func(wctx context.Context) (any, error) {
				e.report <- "work-ran"
				return "v", nil
			}, func(tctx context.Context, v any, err error) {
				e.report <- "then-ran"
			})
			e.report <- "callback-returned"
		})
		return "armed", nil
	case cmd == "timer-yield-blocking":
		// 回调让出一段（work 等 release）：期间 loop 必须能继续服务投递。
		rans.AddTimer(ctx, 10*time.Millisecond, func(cctx context.Context) {
			defer func() {
				if r := recover(); r != nil {
					e.report <- fmt.Sprintf("callback-panicked: %v", r)
				}
			}()
			rans.ReenterAfter(cctx, func(wctx context.Context) (any, error) {
				e.report <- "work-started"
				<-e.release
				return nil, nil
			}, func(tctx context.Context, v any, err error) {
				e.report <- "then-ran"
			})
		})
		return "armed", nil
	case cmd == "arm-inc":
		// 回调 ctx 的纪元必须与本次投递 Handle ctx 的纪元一致（触发时现读）。
		hInc, hOk := rans.Incarnation(ctx)
		e.report <- fmt.Sprintf("handle inc=%d ok=%v", hInc, hOk)
		rans.AddTimer(ctx, 10*time.Millisecond, func(cctx context.Context) {
			cInc, cOk := rans.Incarnation(cctx)
			e.report <- fmt.Sprintf("callback inc=%d ok=%v", cInc, cOk)
		})
		return "armed", nil
	case cmd == "timer-defer":
		// 回调无延迟应答面：Defer 必 panic（不得武装原请求的迟到应答）。
		rans.AddTimer(ctx, 0, func(cctx context.Context) {
			defer func() {
				if r := recover(); r != nil {
					e.report <- "defer-panicked"
				}
			}()
			rans.Defer(cctx)
			e.report <- "defer-no-panic"
		})
		return "armed", nil
	case cmd == "timer-yield-pair":
		// 两个定时器各自让出一段：work 生命周期互相独立（每次触发独立执行面）。
		rans.AddTimer(ctx, 10*time.Millisecond, func(cctx context.Context) {
			rans.ReenterAfter(cctx, func(wctx context.Context) (any, error) {
				e.report <- "pair-a-started"
				<-e.release
				return nil, nil
			}, func(context.Context, any, error) { e.report <- "pair-a-then" })
		})
		rans.AddTimer(ctx, 10*time.Millisecond, func(cctx context.Context) {
			rans.ReenterAfter(cctx, func(wctx context.Context) (any, error) {
				e.report <- "pair-b-started"
				<-e.release
				return nil, nil
			}, func(context.Context, any, error) { e.report <- "pair-b-then" })
		})
		return "armed", nil
	case cmd == "timer-panic":
		// 回调 panic：策略 Default 下 loop 必须存活、实体继续服役。
		rans.AddTimer(ctx, 0, func(context.Context) {
			e.report <- "timer-panicking"
			panic("timer boom")
		})
		return "armed", nil
	case cmd == "timer-stuck":
		// 回调卡死：执行段必须计入执行看门狗（否则实体无声吊住、永不强弃）。
		rans.AddTimer(ctx, 0, func(context.Context) { <-e.stuck })
		return "armed", nil
	case strings.HasPrefix(cmd, "timer-forever:"):
		// 无限重排的周期定时器：只在上报首跳（证明它在跑），此后静默重排。
		d, err := time.ParseDuration(strings.TrimPrefix(cmd, "timer-forever:"))
		if err != nil {
			return nil, err
		}
		fires := 0
		var tick func(context.Context)
		tick = func(tctx context.Context) {
			fires++
			if fires == 1 {
				e.report <- "timer-fired"
			}
			rans.AddTimer(tctx, d, tick)
		}
		rans.AddTimer(ctx, d, tick)
		return "armed", nil
	case cmd == "yield-then-yield":
		if dl, ok := ctx.Deadline(); ok {
			e.report <- fmt.Sprintf("handle-deadline=%d", dl.UnixNano())
		}
		// 续体内再让出（P3）：outer 的 then 里发起第二次让出，inner work 的
		// wctx 必须继承续体剩余预算（原请求 deadline）。
		rans.ReenterAfter(ctx, func(wctx context.Context) (any, error) {
			e.report <- "outer-work"
			return 1, nil
		}, func(tctx context.Context, v any, err error) {
			defer func() {
				if r := recover(); r != nil {
					e.report <- fmt.Sprintf("then-panicked: %v", r)
				}
			}()
			e.report <- "outer-then"
			rans.ReenterAfter(tctx, func(wctx2 context.Context) (any, error) {
				dl, has := wctx2.Deadline()
				e.report <- fmt.Sprintf("inner-work has-deadline=%v deadline=%d", has, dl.UnixNano())
				return 2, nil
			}, func(_ context.Context, v2 any, _ error) {
				e.report <- fmt.Sprintf("inner-then v=%v", v2)
			})
			e.report <- "outer-then-returned"
		})
		return "yielded", nil
	case cmd == "arm-self-remove":
		// 回调用自己的句柄移除：摘表先于执行 ⇒ 必为 false。
		var id int64
		id = rans.AddTimer(ctx, 0, func(cctx context.Context) {
			e.report <- "self-remove=" + strconv.FormatBool(rans.RemoveTimer(cctx, id))
		})
		return id, nil
	case cmd == "ping":
		return "pong", nil
	case strings.HasPrefix(cmd, "period:"):
		// 周期 = 回调内重排（每次新 ID）：报告剩余次数，最后一次不再重排。
		parts := strings.Split(strings.TrimPrefix(cmd, "period:"), ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("bad period command %q", cmd)
		}
		delay, err := time.ParseDuration(parts[1])
		if err != nil {
			return nil, err
		}
		left, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil, err
		}
		var tick func(context.Context)
		tick = func(tctx context.Context) {
			left--
			if left > 0 {
				// 先重排后上报：测试收到报告即证明下一跳已入表——否则「推进
				// 时钟」可能跑在新定时器入表之前，把下一跳推到再下一轮。
				rans.AddTimer(tctx, delay, tick)
			}
			e.report <- fmt.Sprintf("%s tick left=%d", parts[0], left)
		}
		rans.AddTimer(ctx, delay, tick)
		return "periodic", nil
	case cmd == "yield-armtimer":
		// work ctx 不带定时器面：两种调用都必须 panic（loop 外武装会丢唤醒）。
		rans.ReenterAfter(ctx, func(wctx context.Context) (any, error) {
			e.report <- "work-" + probeTimerCall(func() { rans.AddTimer(wctx, time.Second, func(context.Context) {}) })
			e.report <- "work-" + probeTimerCall(func() { rans.RemoveTimer(wctx, 1) })
			e.report <- "work-" + probeTimerCall(func() {
				rans.ReenterAfter(wctx, func(context.Context) (any, error) { return nil, nil }, nil)
			})
			return nil, nil
		}, nil)
		return "yielded", nil
	case strings.HasPrefix(cmd, "remove:"):
		id, err := strconv.ParseInt(strings.TrimPrefix(cmd, "remove:"), 10, 64)
		if err != nil {
			return nil, err
		}
		return rans.RemoveTimer(ctx, id), nil
	}
	return nil, fmt.Errorf("unknown command %q", cmd)
}

// timerKinds 单探针 kind 的工厂表。
func timerKinds(report chan string) map[string]rans.Factory {
	return timerKindsRelease(report, nil)
}

// timerKindsIdle 闲置钝化用例的工厂表：Deactivate 里上报 "deactivated"。
func timerKindsIdle(report chan string) map[string]rans.Factory {
	return map[string]rans.Factory{
		"timerprobe": func(rans.ID) rans.Entity {
			return &timerProbeEntity{report: report, reportDeactivate: true}
		},
	}
}

// timerKindsRelease 带让出释放面的工厂表（timer-yield-blocking 用例）。
func timerKindsRelease(report chan string, release chan struct{}) map[string]rans.Factory {
	return map[string]rans.Factory{
		"timerprobe": func(rans.ID) rans.Entity {
			return &timerProbeEntity{report: report, release: release, stuck: release}
		},
	}
}

// callEntity 投递一条命令并返回结果。
func callEntity(t *testing.T, c *Cluster, id rans.ID, cmd string) any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, err := c.Node(0).Call(ctx, id, cmd)
	if err != nil {
		t.Fatalf("%s: %v", cmd, err)
	}
	return v
}

// collectReports 有界收集 n 条回调报告。
func collectReports(t *testing.T, report chan string, n int) []string {
	t.Helper()
	got := make([]string, 0, n)
	deadline := time.After(2 * time.Second)
	for len(got) < n {
		select {
		case s := <-report:
			got = append(got, s)
		case <-deadline:
			t.Fatalf("只收到 %d/%d 条报告：%v", len(got), n, got)
		}
	}
	return got
}

// wantReportSet 断言收集到的报告集合与期望一致（同批报告的顺序不作契约）。
func wantReportSet(t *testing.T, got []string, want ...string) {
	t.Helper()
	seen := make(map[string]bool, len(got))
	for _, g := range got {
		seen[g] = true
	}
	for _, w := range want {
		if !seen[w] {
			t.Fatalf("报告集合缺 %q，实收 %v", w, got)
		}
		delete(seen, w)
	}
	if len(seen) != 0 {
		t.Fatalf("报告集合多出 %v", seen)
	}
}

// armTimer 经一次投递预约定时器并取回句柄 ID。
func armTimer(t *testing.T, c *Cluster, id rans.ID, name string, d time.Duration) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, err := c.Node(0).Call(ctx, id, "arm:"+name+":"+d.String())
	if err != nil {
		t.Fatalf("arm %s: %v", name, err)
	}
	tid, ok := v.(int64)
	if !ok || tid <= 0 {
		t.Fatalf("arm %s 返回句柄 = %v, want 正整数 ID", name, v)
	}
	return tid
}

// expectReport 有界等待一条回调报告并比对。
func expectReport(t *testing.T, report chan string, want string) {
	t.Helper()
	select {
	case got := <-report:
		if got != want {
			t.Fatalf("回调报告 = %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("等不到回调报告 %q", want)
	}
}

// expectSilent 有界窗口内不得出现回调报告。
func expectSilent(t *testing.T, report chan string, why string) {
	t.Helper()
	select {
	case got := <-report:
		t.Fatalf("%s：却收到回调报告 %q", why, got)
	case <-time.After(50 * time.Millisecond):
	}
}

func removeTimer(t *testing.T, c *Cluster, id rans.ID, tid int64) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, err := c.Node(0).Call(ctx, id, "remove:"+strconv.FormatInt(tid, 10))
	if err != nil {
		t.Fatalf("remove %d: %v", tid, err)
	}
	ok, _ := v.(bool)
	return ok
}

func probeTimerCall(fn func()) string {
	panicked := true
	func() {
		defer func() { panicked = recover() != nil }()
		fn()
	}()
	if panicked {
		return "panicked"
	}
	return "no-panic"
}
