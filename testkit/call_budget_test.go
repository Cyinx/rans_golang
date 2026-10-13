package testkit

// 调用侧预算合成验收（H10 决策 4，缝合点 rans.Client.Call）：本次预算 =
// min(调用方 ctx 剩余, rans.Timeout(d) 选项, Calls.Max)，无 deadline 无选项
// 取 Calls.Default；本地腿 Handle ctx 的 deadline 按同一合成重导出（调用方
// 取消穿透保持）。观测口：probe 实体记录 Handle 入口的 ctx 剩余 deadline，
// 慢 probe 记录调用方收场时序。TestCallConfig_SplitMigration 另钉配置拆分
// 迁移：Default/Max 缺省常量与构造期回落、testkit 测试缺省迁 Calls.Default、
// 旧固定超时字段名全仓零残留（字符串拼接规避自匹配）。

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/node"
)

// budgetProbe 预算观测实体：Handle 入口记录 ctx deadline 的剩余量（合成
// 预算的本地腿观测口），随即应答——Call 返回后读取（应答 channel 的
// happens-before 保证可见）。
type budgetProbe struct {
	mu  sync.Mutex
	rem time.Duration // Handle 入口观测的 ctx 剩余 deadline
	ok  bool          // ctx 是否携带 deadline（合成未生效即 false）
}

func (p *budgetProbe) Handle(ctx context.Context, _ any) (any, error) {
	dl, has := ctx.Deadline()
	p.mu.Lock()
	p.rem, p.ok = time.Until(dl), has
	p.mu.Unlock()
	return "ok", nil
}

func (p *budgetProbe) Activate(context.Context) error   { return nil }
func (p *budgetProbe) Deactivate(context.Context) error { return nil }

// slowBudgetProbe 慢实体（无视 ctx 的固定耗时 Handle）：调用方侧预算收场
// 时序的观测前提——本地腿等待段必须在预算边界收场，不等 Handle 完成。
type slowBudgetProbe struct{ took time.Duration }

func (p *slowBudgetProbe) Handle(context.Context, any) (any, error) {
	time.Sleep(p.took)
	return "ok", nil
}

func (p *slowBudgetProbe) Activate(context.Context) error   { return nil }
func (p *slowBudgetProbe) Deactivate(context.Context) error { return nil }

func budgetProbeKinds(p rans.Entity) map[string]rans.Factory {
	return map[string]rans.Factory{"probe": func(_ rans.ID) rans.Entity { return p }}
}

// assertBudgetRemaining 断言观测剩余 ≈ 预算：上界放 50ms（clock 粒度），
// 下界放 250ms（调度暂停容忍）——上界是 min 合成的判别面（错拿更大来源
// 即超上界），下界排除 deadline 缺失/近乎耗尽的形态。
func assertBudgetRemaining(t *testing.T, p *budgetProbe, budget time.Duration) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.ok {
		t.Fatalf("Handle ctx 无 deadline：合成未生效（预算 %s）", budget)
	}
	if p.rem > budget+50*time.Millisecond || p.rem < budget-250*time.Millisecond {
		t.Fatalf("Handle ctx 剩余 %s，want ≈%s（区间 [%s, %s]）",
			p.rem, budget, budget-250*time.Millisecond, budget+50*time.Millisecond)
	}
}

// TestCall_BudgetSynthesis 预算合成 min 三源 + Default 兜底 + 本地腿
// deadline 重导出（全部本地命中路径——合成规则两腿同源，远端腿的
// pending 登记面由网关侧测试钉住）。
func TestCall_BudgetSynthesis(t *testing.T) {
	t.Run("default_fill_no_deadline_no_option", func(t *testing.T) {
		p := &budgetProbe{}
		c := NewCluster(t, 1, budgetProbeKinds(p))
		id := mustID(t, "probe", "budget-default")
		if _, err := c.Node(0).Call(context.Background(), id, nil); err != nil {
			t.Fatal(err)
		}
		// testkit 缺省（3s）经 Calls.Default 生效：无 deadline 无选项的
		// 调用取配置缺省，而非无界等待。
		assertBudgetRemaining(t, p, 3*time.Second)
	})

	t.Run("timeout_option_below_default", func(t *testing.T) {
		p := &budgetProbe{}
		c := NewCluster(t, 1, budgetProbeKinds(p))
		id := mustID(t, "probe", "budget-option")
		if _, err := c.Node(0).CallOpts(context.Background(), id, nil, rans.Timeout(700*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		assertBudgetRemaining(t, p, 700*time.Millisecond)
	})

	t.Run("max_caps_option", func(t *testing.T) {
		p := &budgetProbe{}
		c := NewCluster(t, 1, budgetProbeKinds(p), WithCallMax(600*time.Millisecond))
		id := mustID(t, "probe", "budget-max-opt")
		if _, err := c.Node(0).CallOpts(context.Background(), id, nil, rans.Timeout(5*time.Second)); err != nil {
			t.Fatal(err)
		}
		assertBudgetRemaining(t, p, 600*time.Millisecond)
	})

	t.Run("max_caps_caller_ctx", func(t *testing.T) {
		p := &budgetProbe{}
		c := NewCluster(t, 1, budgetProbeKinds(p), WithCallMax(600*time.Millisecond))
		id := mustID(t, "probe", "budget-max-ctx")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := c.Node(0).Call(ctx, id, nil); err != nil {
			t.Fatal(err)
		}
		// 调用方不能经 ctx 延长超 Max 的预算：5s 的 ctx 被封顶到 600ms。
		assertBudgetRemaining(t, p, 600*time.Millisecond)
	})

	t.Run("caller_ctx_shortest_wins", func(t *testing.T) {
		p := &budgetProbe{}
		c := NewCluster(t, 1, budgetProbeKinds(p))
		id := mustID(t, "probe", "budget-ctx-short")
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		if _, err := c.Node(0).CallOpts(ctx, id, nil, rans.Timeout(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		// 调用方 ctx 缩短仍生效：400ms 的 ctx 短于 2s 选项与 3s 缺省。
		assertBudgetRemaining(t, p, 400*time.Millisecond)
	})

	t.Run("caller_timeout_at_budget", func(t *testing.T) {
		c := NewCluster(t, 1, budgetProbeKinds(&slowBudgetProbe{took: 2 * time.Second}))
		id := mustID(t, "probe", "budget-slow")
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := c.Node(0).CallOpts(ctx, id, nil, rans.Timeout(2*time.Second))
		elapsed := time.Since(start)
		if !errors.Is(err, rans.ErrTimeout) {
			t.Fatalf("err = %v, want rans.ErrTimeout", err)
		}
		if elapsed >= time.Second {
			t.Fatalf("call returned at %v, want bounded by the 400ms budget (not the 2s Handle)", elapsed)
		}
	})
}

// TestCallConfig_SplitMigration 配置拆分迁移：Default/Max 缺省常量与构造
// 期回落、testkit 测试缺省迁 Calls.Default、旧字段名 .go 全仓零残留。
func TestCallConfig_SplitMigration(t *testing.T) {
	t.Run("default_constants", func(t *testing.T) {
		if node.DefaultCallTimeout != 30*time.Second {
			t.Fatalf("DefaultCallTimeout = %s, want 30s", node.DefaultCallTimeout)
		}
		if node.DefaultMaxCallTimeout != 10*time.Minute {
			t.Fatalf("DefaultMaxCallTimeout = %s, want 10min", node.DefaultMaxCallTimeout)
		}
	})

	t.Run("validate_fills_zero_config", func(t *testing.T) {
		// Standalone 单节点 + 零值 Calls：构造期回落缺省——probe 观测的
		// 本地腿 deadline = DefaultCallTimeout（行为面证明 fill 生效）。
		p := &budgetProbe{}
		n, err := node.New(node.Config{NodeID: 1, Standalone: true, Kinds: budgetProbeKinds(p)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Close() })
		id := rans.ID{Kind: "probe", Key: "split-fill"}
		if _, err := n.Client().Call(context.Background(), id, nil); err != nil {
			t.Fatal(err)
		}
		assertBudgetRemaining(t, p, node.DefaultCallTimeout)
	})

	t.Run("testkit_default_migrates_to_calls_default", func(t *testing.T) {
		p := &budgetProbe{}
		c := NewCluster(t, 1, budgetProbeKinds(p), WithCallDefault(time.Second))
		id := mustID(t, "probe", "split-testkit")
		if _, err := c.Node(0).Call(context.Background(), id, nil); err != nil {
			t.Fatal(err)
		}
		assertBudgetRemaining(t, p, time.Second)
	})

	t.Run("old_field_name_zero_residue", func(t *testing.T) {
		root, err := filepath.Abs("..")
		if err != nil {
			t.Fatal(err)
		}
		needle := "Request" + "Timeout" // 拼接构串：本文件自身不得命中
		var hits []string
		err = filepath.WalkDir(root, func(path string, d os.DirEntry, werr error) error {
			if werr != nil {
				return werr
			}
			if d.IsDir() {
				if name := d.Name(); name != "." && strings.HasPrefix(name, ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(d.Name(), ".go") {
				return nil
			}
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			if strings.Contains(string(b), needle) {
				rel, _ := filepath.Rel(root, path)
				hits = append(hits, filepath.ToSlash(rel))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) > 0 {
			t.Fatalf("旧固定超时字段名在 .go 残留 %d 处: %v", len(hits), hits)
		}
	})
}
