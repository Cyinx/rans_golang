// 生命周期钩子包裹面：在飞留痕与 panic 监管（Activate/Deactivate/OnEvict）。
package pawnite

import (
	"context"
	"fmt"

	"github.com/cyinx/rans_golang/internal/log"
)

// cancelHooksForAbandon 强弃取消面（三 gate 共用）：锁内收集
// cancel 与在飞钩子名，锁外先留痕后取消——Warn 让运维看到 zombie 窗口
// 打开（宿主侧效应仅靠取消收敛，不设硬围栏），cancel 让协作宿主尽早返回。
// 调用点在 gate 解锁后、其余锁外处置之前（zombie 尽早收信号）。Start 前
// 强弃（hookCancel nil）天然 no-op。
func (p *Pawnite) cancelHooksForAbandon() {
	p.mu.Lock()
	cancel, inFlight := p.hookCancel, p.hookInFlight
	p.mu.Unlock()
	if inFlight != "" {
		log.Warn("rans/pawnite: watchdog abandon with lifecycle hook in flight (zombie continues until it honors ctx)",
			"uuid", p.id.String(), "hook", inFlight)
	}
	if cancel != nil {
		cancel()
	}
}

// releaseHooks 回收钩子取消面资源（run 退出路径，幂等——CancelFunc 可重复
// 调用，强弃已取消时 no-op）。loop 正常退出时钩子必已返回（finish 阻塞
// loop），强弃退出时已由 gate 取消——此处的取消不产生额外效应，仅防泄漏。
func (p *Pawnite) releaseHooks() {
	p.mu.Lock()
	cancel := p.hookCancel
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// enterHook/exitHook 维护在飞钩子簿记（mu 短临界区；钩子本体锁外执行）：
// 强弃 gate 据此留痕「钩子仍在飞」。仅在 loop goroutine 的钩子包裹段调用。
func (p *Pawnite) enterHook(kind string) {
	p.mu.Lock()
	p.hookInFlight = kind
	p.mu.Unlock()
}

func (p *Pawnite) exitHook() {
	p.mu.Lock()
	p.hookInFlight = ""
	p.mu.Unlock()
}

// safeActivate 包裹 Activate 钩子：panic 转为激活失败错误（沿用失败回绝路径，
// 不崩进程）。ctx 是钩子取消面（hookCtx）。
func (p *Pawnite) safeActivate(ctx context.Context) (err error) {
	p.enterHook("activate")
	defer p.exitHook()
	defer func() {
		if r := recover(); r != nil {
			log.Warn("pawnite activate hook panicked", "uuid", p.id.String(), "panic", r)
			err = fmt.Errorf("pawnite activate panic: %v", r)
		}
	}()
	return p.opts.Activate(ctx, p)
}

// safeDeactivate 包裹 Deactivate 钩子：panic 吞掉仅记录，继续走向终态。
// ctx 是钩子取消面（hookCtx）。
func (p *Pawnite) safeDeactivate(ctx context.Context) {
	p.enterHook("deactivate")
	defer p.exitHook()
	defer func() {
		if r := recover(); r != nil {
			log.Warn("pawnite deactivate hook panicked", "uuid", p.id.String(), "panic", r)
		}
	}()
	_ = p.opts.Deactivate(ctx, p)
}

// safeOnEvict 包裹 OnEvict 钩子：panic 吞掉仅记录（回调在锁外，回收方/管理器
// 不因逐出回调崩溃）。
func (p *Pawnite) safeOnEvict() {
	defer func() {
		if r := recover(); r != nil {
			log.Warn("pawnite onevict hook panicked", "uuid", p.id.String(), "panic", r)
		}
	}()
	p.opts.OnEvict(p)
}
