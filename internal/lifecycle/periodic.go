package lifecycle

// runPeriodic 周期循环共用骨架：Watchdog/
// CheckpointSweeper/RebalancerService 三个 Run 的同构
// ticker+select 环——「控制信号（ctx 取消）优先于数据（tick）」的 select
// 纪律（双就绪随机选择下 ctx 分支可能被 tick 分支压后）在此单点维护。
//
// 首轮在 interval 到点后执行 tick（纠正面而非拦截面，三编排器同约定：
// 无需立即扫）。tick 的错误留痕/静默语义归各编排器（闭包内自理）。

import (
	"context"
	"time"
)

func runPeriodic(ctx context.Context, interval time.Duration, tick func(ctx context.Context)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick(ctx)
		}
	}
}
