package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/lifecycle"
)

// beginDraining 排空标记发布（nil-safe：存活面未装配时 no-op——排空标记
// 随成员键发布，无成员键即无视图可标记）。
func (n *Node) beginDraining() {
	if n.lv != nil {
		n.lv.BeginDraining()
	}
}

// stopHeartbeat 停序头确定性收口：续租（在途一轮刷新 + 在飞销毁）与存活
// （在途成员键写）逐一等待。自备独立小预算（Shutdown 停序头组件收口共用
// 常量——排空 ctx 可能已被阶段②③耗尽，等待不该因此被跳过，也不能因组件
// 僵死挂死停序）。nil-safe：续租/存活面未装配（inmem 形态）时 no-op。
func (n *Node) stopHeartbeat() error {
	if n.lv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownWaitBudget) // 例外②：停序头收口预算（集中定义）
	defer cancel()
	return n.lv.Shutdown(ctx)
}

// kindNames 工厂 kind 名单。
func kindNames(kinds map[string]rans.Factory) []string {
	names := make([]string, 0, len(kinds))
	for k := range kinds {
		names = append(names, k)
	}
	return names
}

// Start 起跑节点（QUICKSTART 次序）：rem.Start（监听+心跳环+周期任务族）
// → 续租注入实际地址后 Start → 轮询 Start → 自动闲置回收/再平衡循环。
// ctx 只辖监听的建立；周期体挂 node
// 自有 runCtx（Shutdown/Close 收口）。Start 恰一次（二次调用返回错误）；
// 失败即终态（内部已自收口），重建而非重启。
//
// Standalone 形态无传输：跳过监听（其余次序不变）。阻塞到内部就绪（非
// 优雅路径）；并发安全（单次性由 started 守卫）。
func (n *Node) Start(ctx context.Context) error {
	if !n.started.CompareAndSwap(false, true) {
		return errors.New("rans: start: already called; Node is single-use — rebuild instead of restarting")
	}
	if !n.cfg.Standalone {
		if err := n.rem.Start(ctx); err != nil {
			n.runCancel() // 自有 ctx 家族同步收口，不留半开组合体
			return fmt.Errorf("rans: start: node %d: %w", n.cfg.NodeID, err)
		}
		addr := n.rem.Addr()
		n.addr.Store(&addr)
	}
	if n.lv != nil {
		// 先发布实际地址/承载 kinds 再 Start——启动即写即带真地址。成员键
		// 承载 kinds = 本节点工厂表（kind 感知放置据此过滤候选）。
		n.lv.PublishSelf(n.Addr(), kindNames(n.cfg.Kinds))
		if err := n.lv.Start(n.runCtx); err != nil {
			n.runCancel()
			_ = n.rem.Close()
			return fmt.Errorf("rans: start: liveness: %w", err)
		}
	}
	go n.runIdleCollection(n.runCtx)
	return nil
}

// runIdleCollection 自动闲置回收/再平衡循环（构造期登记策略的执行面）：
//   - Passivate 策略存在 → 每轮 CollectIdle（闲置钝化：快照落 Store + 本地
//     销毁，下次访问重激活恢复）；
//   - Rebalance.Threshold > 0 → 本地实体数严格超阈 → NewRebalancer 把
//     Migrate 候选迁给放置远端。
//
// 挂 runCtx（Shutdown/Close 收口）；周期 ≤0 → 60s。尽力而为：输的实体保
// 持 Active，下轮自然重试。
func (n *Node) runIdleCollection(ctx context.Context) {
	period := n.cfg.Rebalance.Period
	if period <= 0 {
		period = DefaultRebalancePeriod
	}
	passivate := false
	rebalance := false
	for _, ko := range n.cfg.KindOptions {
		if ko.Idle == nil {
			continue
		}
		if ko.Idle.Migrate {
			rebalance = true
		} else {
			passivate = true
		}
	}
	if !passivate && n.cfg.Rebalance.Threshold <= 0 {
		return // 无策略无再平衡：零成本空转也不必
	}
	var rb *lifecycle.RebalancerService
	if rebalance && n.cfg.Rebalance.Threshold > 0 {
		rb = lifecycle.NewRebalancer(n.mgr, n.cfg.Rebalance.Threshold, n.maxPerRound())
	}
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), period)
		if passivate {
			_, _ = lifecycle.CollectIdle(cctx, n.mgr, n.maxPerRound())
		}
		if rb != nil {
			rb.Scan(cctx)
		}
		cancel()
	}
}

// maxPerRound 单轮回收上限（≤0 → DefaultMaxPerRound）。
func (n *Node) maxPerRound() int {
	if n.cfg.Rebalance.MaxPerRound <= 0 {
		return DefaultMaxPerRound
	}
	return n.cfg.Rebalance.MaxPerRound
}

// shutdownWaitBudget Shutdown 停序头里各组件确定性收口（成员键/续租/轮询
// 的 Shutdown 等待）的独立小预算：与 remote 的注销窗同族——排空 ctx 可能
// 已被排空阶段耗尽，等待不该因此被跳过，也不能因组件僵死挂死停序。
const shutdownWaitBudget = 2 * time.Second

// Shutdown 优雅停机（排空序，受 ctx 预算约束）：标记 draining（成员键即
// 时携带排空标记，放置让位；续租继续）→ rem.Shutdown 四段排空（门闸 →
// 并行 Retire 全部本地实体 → 出站 flush——此三段期间续租/轮询照常）→
// 停序头确定性收口续租与轮询（New 时经 Config.ShutdownTeardown 登记，
// 先于节点注销——次序颠倒会被在途续租把注销过的成员键写活）→ 节点注销 +
// 关传输 → 关闭自建目录（redis 形态；注入目录归调用方）。
//
// 预算语义（spec §5.1 第 1 条）：阻塞到排空完成或 ctx 耗尽；预算耗尽返回
// 包装 ctx.Err() 的错误（排空不回滚，停序照常收口——错误面如实上报「未在
// 预算内走完」）。幂等（首次结果缓存，二次调用返回同值）；并发安全；终
// 态后不可复用。可在实体 Handle 内调用（对自身等价于 Destroy 语义的编排
// 面，受串行队列约束）。
func (n *Node) Shutdown(ctx context.Context) error {
	n.stopOnce.Do(func() {
		var errs []error
		n.beginDraining() // 排空标记即时发布；续租照常（排空与 flush 期间）
		errs = append(errs, n.rem.Shutdown(ctx))
		n.stopErr = errors.Join(errs...)
		if n.closeDir && !n.crashKeepsDir() {
			if closer, ok := n.dir.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		}
	})
	return n.stopErr
}

// crashKeepsDir 崩溃模拟开关：Close/终态不关自建目录（testkit Kill 语义
// ——目录由测试直接持有）。Shutdown 仍按优雅语义关目录（开关只辖 Close）。
func (n *Node) crashKeepsDir() bool {
	return n.cfg.TestHooks != nil && n.cfg.TestHooks.CrashKeepsDir
}

// Close 立即停止节点（不排空；幂等）：停续租 + 停存活 + 关传输。本地实体
// 直接丢弃（无 Deactivate 无终快照），目录条目与成员键留待租约/TTL 自然
// 过期——消费方据此能分辨「已优雅下线」与「疑似崩溃」。自建 redis 目录
// 默认关闭（CrashKeepsDir 开关豁免——testkit 崩溃模拟形态）；注入目录归
// 调用方。返回错误仅自建目录关闭失败（罕见）。不阻塞在长操作上；并发
// 安全；不得在实体 Handle 内调用（整进程生命期面）。
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		if n.lv != nil {
			_ = n.lv.Close()
		}
		n.runCancel()
		// rem.Close 关传输与网关扫描器（New 即起的 goroutine——Standalone
		// 也必须收口）。
		_ = n.rem.Close()
		if n.closeDir && !n.crashKeepsDir() {
			if closer, ok := n.dir.(interface{ Close() error }); ok {
				n.closeErr = closer.Close()
			}
		}
	})
	return n.closeErr
}

// Run 一站式生命周期：Start → 阻塞至 ctx 取消或 SIGTERM/SIGINT → 优雅停
// 机（预算 ShutdownBudget，≤0/缺省 DefaultShutdownBudget）。ctx 分支与信
// 号分支语义对称；阻塞直到停机完成；并发安全（单次性随 Start）；不得在
// 实体 Handle 内调用（整进程生命期面）。
func (n *Node) Run(ctx context.Context) error {
	if err := n.Start(ctx); err != nil {
		return err
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sig)
	select {
	case <-ctx.Done():
	case <-sig:
	}
	budget := n.cfg.ShutdownBudget
	if budget <= 0 {
		budget = DefaultShutdownBudget
	}
	sctx, cancel := context.WithTimeout(context.Background(), budget) // 例外②：Run 的停机预算（ShutdownBudget 配置，DefaultShutdownBudget 缺省）
	defer cancel()
	return n.Shutdown(sctx)
}
