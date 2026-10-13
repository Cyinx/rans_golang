package membership

// Liveness 测试：每周期无条件 RegisterNode(self, addr)——零实体照写、
// 启动即写一次、写失败只 Warn。「目录无 NodeRegistry 能力」是装配语义
//（node 按 directory.NodeRegistry 能力探测构造
// Liveness，无能力不构造），包内锚 = TestLivenessConfigValidation（nil
// Reg 响亮报错）；「租约路径不受影响」半边由续租面用例族覆盖（续租
// 组件不含成员键逻辑）。跨面不变量（销毁阻塞不冻结节点键、排空标记下
// 续租照常、启动首写先于首轮续租）以 Liveness+LeaseKeeper 组合测试锚定
// ——断言与拆分前逐一对应。测试名映射记录于各测试注释与 commit 正文。

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

// TestLivenessConfigValidation 构造校验：Reg 必填，nil 报错（原
// TestHeartbeatPlainLeaseDirNoRegistry 的拆分形态——无注册能力的目录不构
// 造本组件是 node 装配语义，包内响亮锚定缺 Reg 的构造失败）。
func TestLivenessConfigValidation(t *testing.T) {
	if _, err := NewLiveness(LivenessConfig{Self: 1}); err == nil {
		t.Fatal("NewLiveness without Reg must fail")
	}
}

// TestLivenessWritesNodeKeyOnStartAndEveryBeat 启动即写 + 每周期无条件写：
// PublishSelf 后 Run，不等首个 tick 就有一次 RegisterNode(self, addr)；
// 多周期后调用数持续增长（零实体照写——续租面缺席不影响成员键节奏）。
func TestLivenessWritesNodeKeyOnStartAndEveryBeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := newFakeNodeRegistry(nil) // 零实体
	svc := newLiveness(t, reg, 5, 20*time.Millisecond)
	svc.PublishSelf("10.0.0.5:7005", nil)
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Close()

	// 启动即写：不等 20ms 周期，100ms 内必有首写（宽松上界防抖动）。
	deadline := time.After(100 * time.Millisecond)
	for {
		if calls := reg.registrations(); len(calls) >= 1 {
			if calls[0].node != 5 || calls[0].info.Addr != "10.0.0.5:7005" {
				t.Fatalf("first RegisterNode = (node %d, addr %q); want (5, 10.0.0.5:7005)", calls[0].node, calls[0].info.Addr)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("RegisterNode not called at Run start (cold-start window not eliminated)")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// 多周期持续重注册（每周期一次 + 启动一次）
	time.Sleep(120 * time.Millisecond)
	if n := len(reg.registrations()); n < 3 {
		t.Fatalf("RegisterNode called %d times in ~6 ticks; want unconditional per-beat writes (>=3)", n)
	}
}

// TestLivenessStartupWritePrecedesFirstRenewalTick：启动即写先于首个 tick
// （续租面已拆除，断言收敛为 Liveness 单面——启动首写存在且带本节点号）。
func TestLivenessStartupWritePrecedesFirstRenewalTick(t *testing.T) {
	reg := newFakeNodeRegistry(nil)
	lv := newLiveness(t, reg, 7, 20*time.Millisecond)
	lv.PublishSelf("10.0.0.7:1", nil)
	startLiveness(t, lv)

	reg.waitCalled(t)
	calls := reg.registrations()
	if len(calls) == 0 {
		t.Fatal("RegisterNode startup write must precede the first tick")
	}
	if calls[0].node != 7 {
		t.Fatalf("RegisterNode node = %d; want self 7", calls[0].node)
	}
}

func TestLivenessErrorNonFatalToRenewal(t *testing.T) {
	reg := newFakeNodeRegistry(nil)
	reg.failWrites(errors.New("redis write failed"))
	lv, err := NewLiveness(LivenessConfig{Reg: reg, Self: 3, Interval: 20 * time.Millisecond, SelfFence: -1})
	if err != nil {
		t.Fatal(err)
	}
	lv.PublishSelf("10.0.0.3:1", nil)
	startLiveness(t, lv)

	reg.waitCalled(t)
	reg.waitCalled(t)
	if calls := reg.registrations(); len(calls) < 2 {
		t.Fatalf("RegisterNode retries expected, got %d calls", len(calls))
	}
}

// TestLivenessNoAddrSkipsRegister 未设置地址时不写成员键：
// 空地址入键会让地址发现拿到 ""（dial 必败），宁可跳过——装配方漏调
// SetNodeAddr 的行为降级为旧水平（无成员键 → 节点不可见）。
func TestLivenessNoAddrSkipsRegister(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := newFakeNodeRegistry(nil)
	svc := newLiveness(t, reg, 9, 20*time.Millisecond)
	// 不调 SetNodeAddr
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Close()

	time.Sleep(80 * time.Millisecond)
	if calls := reg.registrations(); len(calls) != 0 {
		t.Fatalf("RegisterNode called %d times without SetNodeAddr; want 0 (no empty addresses in the membership view)", len(calls))
	}
}

// TestLivenessPublishSelfKinds kinds 注入（判活
// 键值 {addr,kinds}）：PublishSelf 后每轮 RegisterNode 携带注入值；未注入
// （nil）时携带 nil kinds——纯心跳节点；注入切片被复制，调用方后续改动
// 不影响已发布值。
func TestLivenessPublishSelfKinds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 注入形态：kinds 随每轮注册写入
	kinds := []string{"workspace", "greet"}
	reg := newFakeNodeRegistry(nil)
	svc := newLiveness(t, reg, 5, 20*time.Millisecond)
	svc.PublishSelf("10.0.0.5:1", kinds)
	kinds[0] = "mutated" // 调用方改动不得污染已发布值（复制语义）
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Close()

	deadline := time.After(100 * time.Millisecond)
	for {
		if calls := reg.registrations(); len(calls) >= 1 {
			got := calls[0].info.Kinds
			if len(got) != 2 || got[0] != "workspace" || got[1] != "greet" {
				t.Fatalf("first RegisterNode kinds = %v, want [workspace greet]", got)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("RegisterNode not called at Run start")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// 未注入形态：nil kinds（纯心跳节点）——PublishSelf 只给地址的缺省行为
	reg2 := newFakeNodeRegistry(nil)
	svc2 := newLiveness(t, reg2, 6, 20*time.Millisecond)
	svc2.PublishSelf("10.0.0.6:1", nil)
	if err := svc2.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc2.Close()

	deadline = time.After(100 * time.Millisecond)
	for {
		if calls := reg2.registrations(); len(calls) >= 1 {
			if got := calls[0].info.Kinds; got != nil {
				t.Fatalf("RegisterNode kinds = %v, want nil (pure heartbeat node by default)", got)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("RegisterNode not called at Run start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// TestLivenessBeginDrainingPublishesAndLeaseKeeperKeepsRenewing 双面组合：
// BeginDraining 是运行期状态命令一次性置位（排空是终态动作，无解除半边）。
// BeginDraining 后节点键 promptly 携带 Draining=true（kick 即时刷新，不等
// 下一周期——排空标记要赶在注销前进入放置视图）；draining 期间租约续租
// 照常（HeartbeatBatch 持续调用——阶段②长排空不过租约 TTL 的锚）。
func TestLivenessBeginDrainingPublishesAndKicksImmediateRefresh(t *testing.T) {
	reg := newFakeNodeRegistry(nil)
	lv := newLiveness(t, reg, 4, 20*time.Millisecond)
	lv.PublishSelf("10.0.0.4:1", nil)
	startLiveness(t, lv)

	reg.waitCalled(t) // 启动首写已发生
	if calls := reg.registrations(); calls[0].info.Draining {
		t.Fatal("default registration must not be draining")
	}

	lv.BeginDraining()
	// kick 即时刷新：BeginDraining 返回后的下一次 RegisterNode 携带 Draining=true
	deadline := time.After(2 * time.Second)
	for {
		calls := reg.registrations()
		if n := len(calls); n >= 2 && calls[n-1].info.Draining {
			break
		}
		select {
		case <-deadline:
			t.Fatal("draining mark never published in the node key after BeginDraining()")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// draining 期间存活刷新照常（不被 BeginDraining 停掉）
	batches := reg.callCount()
	time.Sleep(60 * time.Millisecond)
	if reg.callCount() <= batches {
		t.Fatal("membership refresh must continue while draining")
	}
}

// TestLiveness_NodeKeyNotFrozenByBlockingDestroy（EX5 族，H13 形态）：
// 自围栏触发的止损销毁阻塞 2s 期间，节点键刷新间隔 ≤ 1.5×Interval——
// 止损销毁在独立 goroutine 上执行（noteWriteOutcome 的 go 分支），存活
// 循环永不等待实体级工作（INV5）。触发编排：写失败一幕（触发围栏）后
// 恢复写入，阻塞窗内的成功写序列即新鲜度证据。
func TestLiveness_NodeKeyNotFrozenByBlockingDestroy(t *testing.T) {
	reg := newFakeNodeRegistry(nil)
	X := mustID(t, "k", "1")
	const blockedTurn = 2 * time.Second
	destroyEntered := make(chan struct{}, 16)
	lv, err := NewLiveness(LivenessConfig{
		Reg: reg, Self: 1, Interval: 100 * time.Millisecond,
		SelfFence: 150 * time.Millisecond, // 2 轮失败即触发（显式短阈值）
		Fence:     func(rans.ID) {},
		LocalIDs:  func() []rans.ID { return []rans.ID{X} },
		OnSelfFenceLost: func(context.Context, rans.ID) error {
			destroyEntered <- struct{}{}
			time.Sleep(blockedTurn)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	lv.PublishSelf("127.0.0.1:1", nil)
	startLiveness(t, lv)
	time.Sleep(150 * time.Millisecond)            // 至少一轮成功写
	reg.failWrites(errors.New("redis partition")) // 失败幕：触发围栏
	<-destroyEntered                              // 止损销毁已开始阻塞
	reg.healWrites()                              // 恢复写入：阻塞窗内的成功写即证据
	base := len(reg.okTimestamps())
	time.Sleep(600 * time.Millisecond)

	got := reg.okTimestamps()[base:]
	if len(got) < 4 {
		t.Fatalf("expected fresh writes during blocked destroy, got %d", len(got))
	}
	maxGap := time.Duration(0)
	for i := 1; i < len(got); i++ {
		if g := got[i].Sub(got[i-1]); g > maxGap {
			maxGap = g
		}
	}
	if maxGap > 150*time.Millisecond {
		t.Fatalf("节点键刷新被销毁阻塞：最大间隔 %v > 1.5×Interval", maxGap)
	}
}

// TestLivenessShutdownTimeout ctx 先耗尽：在途节点键写（慢目录）未完
// 成时返回 ctx.Err 而非挂死（原心跳等待面用例迁此，成员键
// 等待面归 Liveness——断言逐一保留）。
func TestLivenessShutdownTimeout(t *testing.T) {
	release := make(chan struct{})
	blocking := newBlockNodeRegistry(release)
	svc := newLiveness(t, blocking, 1, 20*time.Millisecond)
	svc.PublishSelf("10.0.0.1:1", nil) // 触发 registerSelf 走阻塞目录
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// 等 loop 进入阻塞的 RegisterNode，再以已耗尽的 ctx 调 Shutdown
	blocking.waitEntered(t)
	expired, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := svc.Shutdown(expired); err == nil {
		t.Fatal("Shutdown with expired ctx must return ctx.Err, got nil")
	}
	close(release)
	wait, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := svc.Shutdown(wait); err != nil {
		t.Fatalf("Shutdown after release: %v", err)
	}
}

// blockNodeRegistry 在 RegisterNode 上阻塞直到 release 关闭（模拟慢目录——
// 在途节点键写的收口等待面）。
type blockNodeRegistry struct {
	*fakeNodeRegistry
	release chan struct{}
	entered chan struct{}
}

func newBlockNodeRegistry(release chan struct{}) *blockNodeRegistry {
	return &blockNodeRegistry{fakeNodeRegistry: newFakeNodeRegistry(nil), release: release, entered: make(chan struct{}, 4)}
}

func (b *blockNodeRegistry) RegisterNode(ctx context.Context, node rans.NodeID, info directory.NodeInfo) error {
	b.entered <- struct{}{}
	<-b.release
	return b.fakeNodeRegistry.RegisterNode(ctx, node, info)
}

func (b *blockNodeRegistry) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-b.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("liveness loop never entered RegisterNode within 2s")
	}
}
