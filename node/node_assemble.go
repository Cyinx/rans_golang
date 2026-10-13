package node

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/directory/redis"
	mgr "github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/lifecycle"
	"github.com/cyinx/rans_golang/internal/membership"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/placement"
	"github.com/cyinx/rans_golang/internal/routing"
	"github.com/cyinx/rans_golang/internal/rpc"
	"github.com/cyinx/rans_golang/internal/testhooks"
)

// New 按 QUICKSTART 装配序收编一个节点（目录来源三分支、续租/轮询/放置
// 缺省、remote 装配、lease tracker、缓存失效钩子、指标注册）。
// 目录来源：Directory.Injected 注入（调用方持有）> Directory.Redis 自建
// （Shutdown 时关闭）> 缺省 inmem（单节点形态）。inmem 形态的地址解析缺
// 省 = 自解析（本节点 → Start 后实际地址；其他节点号报错——单节点形态无
// 合法远端，多节点 in-process 测试经 TestHooks.Resolve 注入共享表）；
// redis 形态的解析/放置/续租/轮询缺省全部内置。装配失败返回错误并关闭
// 自建目录（注入目录归调用方），不留半开组合体。不阻塞（无网络 IO——
// redis 目录的连接懒建于 Start）；非并发安全（装配期单线程约定——构造
// 一次给全，无运行期 Set 面）。
//
// 配置一次给全：激活表的钩子/续租接线/迁移注入面/闲置策略全部经 Options
// 在构造期定死；随迁包两端由 rpc.New 经 BindMigrationPackages 在装配期
// 补齐（Start 前单写，之后同样不可变）。
func New(cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	// 节点自有指标注册表（观测面）先行：本节点装配期注册的全部仪表
	//（rpc 五面 + lease.self_fenced）落此处，Metrics() 快照即其读数——
	// 自检止损是逐节点事实（哪个节点成员写失联就告警谁），不进进程级
	// 聚合面。进程级聚合面（activation.failed / checkpoint.final_failed
	// 两项 Counter）仍在 metrics.Default()——它们由 internal 包直接注册
	//（包级 OnceValue 单例），按设计跨节点聚合。
	mreg := metrics.New("")
	dir, closeDir, err := openDirectory(cfg.Directory)
	if err != nil {
		return nil, err
	}
	n := &Node{cfg: cfg, dir: dir, closeDir: closeDir, mreg: mreg}
	// 例外①（spec §5.1）：节点根 ctx（进程级生命周期）——Start 的调用方
	// ctx 只辖监听建立；周期体（续租/存活/轮询/闲置回收）挂本 ctx，由
	// Shutdown/Close 的停序 cancel 收口。
	n.runCtx, n.runCancel = context.WithCancel(context.Background()) // 例外①（见上注）
	// 位置缓存先于激活表构造（缓存失效钩子经 Options 引用它）。
	n.cache = routing.NewLocationCache(0, 0, 0)
	// kind 感知放置桥：首激活与迁移共用（Options.Placement 构造期定死；
	// 迁移同走 PickForKind，候选只含声明承载该 kind 的节点）。
	bridge := &placementBridge{policy: cfg.Placement, scanner: kindScannerOf(dir)}
	opts := mgr.Options{
		Placement: bridge.PickForKind,
		Store:     cfg.Store,
		// 位置缓存失效/更新钩子（规格 §9.2/陷阱 14 六类中的本地三类；
		// 组装根构造期直接接线）：
		// ① 本地销毁 → Invalidate（目录注销+本地逐出完成后触发）；
		// ③ CAS 冲突 → PutPositive(赢家)：随输家实例的终局清理触发——排在
		//    ①之后（否则赢家条目会被本实例的失效清掉，时序约束见表的
		//    arbLost 槽注释）；
		// ⑥ 迁移换家 → PutPositive(新位置)：次序在 ① 之后（onMigrated 晚
		//    于 onEvicted——同 ③ 的防线）。
		OnEvicted:         func(id rans.ID) { n.cache.Invalidate(id) },
		OnArbitrationLost: func(id rans.ID, winner directory.Owner) { n.cache.PutPositive(id, winner) },
		OnMigrated:        func(id rans.ID, to rans.NodeID) { n.cache.PutPositive(id, directory.Owner{Node: to}) },
	}
	// 闲置回收策略登记（自动执行面：Passivate 周期钝化 / Migrate 随
	// Rebalance 卸载；构造期一次给全）。
	for kind, ko := range cfg.KindOptions {
		if ko.Idle == nil {
			continue
		}
		mode := mgr.CollectPassivate
		if ko.Idle.Migrate {
			mode = mgr.CollectMigrate
		}
		if opts.IdlePolicies == nil {
			opts.IdlePolicies = make(map[string]mgr.IdlePolicy, len(cfg.KindOptions))
		}
		opts.IdlePolicies[kind] = mgr.IdlePolicy{IdleAfter: ko.Idle.After, Mode: mode}
	}
	n.mgr = mgr.New(dir, cfg.NodeID, cfg.Kinds, opts)
	if !cfg.Standalone {
		// 节点存活信号（独立接线面）：目录实现 NodeRegistry 才构造
		//（inmem 等基础面无成员键语义，不构造）。
		if reg, ok := dir.(directory.NodeRegistry); ok {
			selfFenced := mreg.Counter(metrics.NameLeaseSelfFenced,
				"self-fence triggered (membership writes failing past threshold)")
			lv, err := membership.NewLiveness(membership.LivenessConfig{
				Reg:       reg,
				Self:      cfg.NodeID,
				Interval:  n.leaseInterval(),
				SelfFence: cfg.Lease.SelfFence,
				MemberTTL: reg.MemberTTL(),
				Fence: func(id rans.ID) {
					selfFenced.Add(1)
					n.mgr.Fence(id)
				},
				LocalIDs: n.mgr.LocalIDs,
				OnSelfFenceLost: func(ctx context.Context, id rans.ID) error {
					// 实例级止损销毁（与所有权丢失通知同一销毁面）。
					if o, ok := n.mgr.InstanceOwner(id); ok {
						return lifecycle.DestroyInstance(ctx, n.mgr, id, o.Incarnation)
					}
					return nil
				},
			})
			if err != nil {
				n.abandon(closeDir)
				return nil, fmt.Errorf("rans: new: liveness: %w", err)
			}
			n.lv = lv
			// 节点代际读取面接线（组装序：表先建、Liveness 后建，故经
			// Bind 单写一次）：Register/Transfer 写入 Owner.Epoch 的取值源。
			n.mgr.BindSelfEpoch(lv.NodeEpoch)
		}
		// 位置缓存失效面（H13）：目录变更轮询（DirectoryWatcher）已随
		// 续租拆除退役——陈旧正条目由路由梯子的成员代际有效性判定
		//（死 owner 按 miss 收口）+ 正 TTL 30s 收敛。
	}
	resolve := n.resolver()
	n.rem, err = rpc.New(rpc.Config{
		NodeID:             cfg.NodeID,
		ClusterName:        cfg.ClusterName,
		Addr:               cfg.ListenAddr,
		Secret:             cfg.Security.HMACKey,
		NoHMAC:             cfg.Security.Insecure,
		StrictSchema:       cfg.Security.StrictSchema,
		TLS:                cfg.Security.TLS,
		Dir:                dir,
		Manager:            n.mgr,
		Resolve:            resolve,
		DefaultCallTimeout: cfg.Calls.Default,
		MaxCallTimeout:     cfg.Calls.Max,
		OutboxCapacity:     cfg.Outbox.Capacity,
		OutboxShards:       cfg.Outbox.Shards,
		CheckpointStore:    cfg.Store,
		LocationCache:      n.cache,
		Placement:          bridge,
		Watchdog:           n.watchdogConfig(),
		PackageTTL:         cfg.Tuning.PackageTTL,
		CheckpointInterval: cfg.Tuning.CheckpointInterval,
		Metrics:            n.mreg,
		Dial:               testDial(cfg.TestHooks),
		WrapConn:           testWrapConn(cfg.TestHooks),
		// 停序头（Remote.Shutdown 阶段④最先执行，先于节点注销与关传输）：
		// 先停续租/存活（在途一轮写完——次序颠倒会把注销过的成员键写活）、
		// 再停轮询、最后收口自有 ctx 家族；错误并入 Shutdown 结果。
		ShutdownTeardown: []func() error{n.stopHeartbeat,
			func() error { n.runCancel(); return nil }},
	})
	if err != nil {
		n.abandon(closeDir)
		return nil, fmt.Errorf("rans: new: remote: %w", err)
	}
	// 统一调用入口：薄适配 Call/Tell → 路由面，并把自身注入 dispatcher
	// 的投递基 ctx（实体经 rans.ClientFrom 自省）。
	n.client = rpc.NewClient(n.rem)
	return n, nil
}

// abandon 装配失败清理：自建目录随失败关闭（注入目录归调用方）。
func (n *Node) abandon(closeDir bool) {
	if closeDir {
		if closer, ok := n.dir.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}
	n.runCancel()
}

// openDirectory 目录来源三分支：Redis 自建（KeyPrefix 缺省补齐）＞ 注入
// ＞ inmem。返回（目录, 是否 node 自建）。
func openDirectory(dc DirectoryConfig) (directory.Directory, bool, error) {
	if dc.Redis != nil {
		ro := *dc.Redis
		if ro.KeyPrefix == "" {
			ro.KeyPrefix = DefaultRedisKeyPrefix
		}
		return redis.New(ro), true, nil
	}
	if dc.Injected != nil {
		return dc.Injected, false, nil
	}
	return inmem.New(), false, nil
}

func (n *Node) leaseInterval() time.Duration { return n.cfg.Lease.Interval }

// resolver 地址解析三分支（调用方 ctx 贯穿——解析尊重调用预算，spec §5.1
// 第 5 点）：TestHooks.Resolve 注入 ＞ 目录派生（redis 节点成员键）＞ inmem
// 自解析（本节点 → Start 后实际地址；其他节点号报错）。
func (n *Node) resolver() func(ctx context.Context, node rans.NodeID) (string, error) {
	if n.cfg.TestHooks != nil && n.cfg.TestHooks.Resolve != nil {
		return n.cfg.TestHooks.Resolve
	}
	if nodeResolver, ok := n.dir.(interface {
		NodeAddrResolver() func(ctx context.Context, n rans.NodeID) (string, error)
	}); ok {
		return nodeResolver.NodeAddrResolver()
	}
	self := n.cfg.NodeID
	return func(_ context.Context, node rans.NodeID) (string, error) {
		if node == self {
			if a := n.Addr(); a != "" {
				return a, nil
			}
			return "", fmt.Errorf("rans: resolve: node %d address not bound yet (Start first)", node)
		}
		return "", fmt.Errorf("rans: resolve: inmem single-node form has no peer %d (use TestHooks.Resolve for in-process clusters)", node)
	}
}

// kindScannerOf 目录的 kind 感知成员视图（redis 形态满足；inmem 等返回
// nil——放置候选为空，miss 回落本地激活）。
func kindScannerOf(dir directory.Directory) placement.KindScanner {
	if s, ok := dir.(placement.KindScanner); ok {
		return s
	}
	return nil
}

// testDial / testWrapConn 测试注入点的读出面（TestHooks 在 New 期一次性
// 接线进 rpc/tcp 装配，运行中不可变——无运行期 Set 面）。
func testDial(h *testhooks.Hooks) func(context.Context, string) (net.Conn, error) {
	if h == nil {
		return nil
	}
	return h.Dial
}

func testWrapConn(h *testhooks.Hooks) func(net.Conn) net.Conn {
	if h == nil {
		return nil
	}
	return h.WrapConn
}

// watchdogConfig 全局看门狗参数换算（nil → 缺省参数开跑；零值字段回落缺
// 省——透传内部形态，per-kind 覆盖在 KindOptions 装配期合入）。
func (n *Node) watchdogConfig() *lifecycle.WatchdogConfig {
	wd := &lifecycle.WatchdogConfig{
		Interval:          n.cfg.Watchdog.Interval,
		UnloadTimeout:     n.cfg.Watchdog.UnloadTimeout,
		ExecTimeout:       n.cfg.Watchdog.ExecTimeout,
		ActivationTimeout: n.cfg.Watchdog.ActivationTimeout,
	}
	for kind, p := range n.cfg.Watchdog.KindOverrides {
		if wd.KindOverrides == nil {
			wd.KindOverrides = make(map[string]lifecycle.WatchdogParams, len(n.cfg.Watchdog.KindOverrides))
		}
		wd.KindOverrides[kind] = lifecycle.WatchdogParams(p)
	}
	return wd
}
