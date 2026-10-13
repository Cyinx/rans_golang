package testkit

// 集群验收测试本体（ROADMAP 2.10/验收 ②③④）。时序敏感断言一律走握手
// channel，不用 sleep；所有等待有界（time.After 上限）；-race 稳定由
// make test 保底，另以 -count=3 复验。
//
// 集群装配基于 node.New + TestHooks 逐节点注入；激活表/传输的白盒断言
// 改公开面等价形态（LocalIDs/Holds + 有界轮询终态逐出）——断言终点不变
//（输家终态逐出、赢家本地持有）。raw 帧级的握手门禁组合测试在
// internal/rpc（TestRemoteHandshakeGates——testkit 依赖面不含 internal/wire）。

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// ---- 测试实体与基座 ----

// counterEntity 计数实体：实例内计数（只在 pawnite 串行 loop 上读写，无锁），
// 每次 Handle 自增并返回新值——返回值即该身份的累计执行次数，跨调用方汇总
// 后可证「恰好一次、无丢失更新」。可选观测点（容量须覆盖全部上报次数，否则
// 实体的串行 loop 会阻塞在上报上）：hit 每次 Handle 上报新值；activated 每次
// Activate 上报（pawnite 创建的确定性观测点）。
type counterEntity struct {
	n         int
	hit       chan int
	activated chan struct{}
}

func (e *counterEntity) Activate(context.Context) error {
	if e.activated != nil {
		e.activated <- struct{}{}
	}
	return nil
}
func (e *counterEntity) Deactivate(context.Context) error { return nil }
func (e *counterEntity) Handle(context.Context, any) (any, error) {
	e.n++
	if e.hit != nil {
		e.hit <- e.n
	}
	return e.n, nil
}

// probeEntity 探针实体（独立 kind）：Handle 原样返回请求——仅作帧序/回程
// 观测，不进计数上报通道。
type probeEntity struct{}

func (probeEntity) Activate(context.Context) error                 { return nil }
func (probeEntity) Deactivate(context.Context) error               { return nil }
func (probeEntity) Handle(_ context.Context, req any) (any, error) { return req, nil }

// counterOpt 计数实体观测点（nil 字段不上报）。
type counterOpt struct {
	hit       chan int
	activated chan struct{}
}

// counterKinds 单 kind 计数工厂（opt 为 nil 时无观测点）。
func counterKinds(opt *counterOpt) map[string]rans.Factory {
	return map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity {
			e := &counterEntity{}
			if opt != nil {
				e.hit, e.activated = opt.hit, opt.activated
			}
			return e
		},
	}
}

// mustID 构造测试身份（kind 必须在集群 kinds 中声明）。
func mustID(t *testing.T, kind, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID(kind, inst)
	if err != nil {
		t.Fatalf("rans.NewID(%s,%s): %v", kind, inst, err)
	}
	return id
}

// wantRange 生成闭区间整数切片（期望值序列）。
func wantRange(lo, hi int) []int {
	out := make([]int, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		out = append(out, i)
	}
	return out
}

// waitLocalGone 有界等待实体从节点本地激活表消失（终态逐出的公开观测面：
// 旧 Mgr.GetLocal+pawnite Done 白盒断言的 localonly 等价形态——终态收尾
// 后逐出必达，轮询终点与原断言一致）。
func waitLocalGone(t *testing.T, n *Node, id rans.ID, budget time.Duration) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if !n.Holds(id) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("entity %s still held locally after %s", id.String(), budget)
}

// ---- 验收 ②：双节点跨节点 Call，-race 稳定 ----

// TestAcceptance_TwoNodeCrossNodeCall_RaceStable：node2 先激活（本地 Call，
// 计数 1），随后 node1 以 50 goroutine × 4 轮并发 Call 同一身份——每次都经
// 目录命中 node2 远程执行。全部成功且 201 个返回值恰为 1..201（无重复、无
// 空洞、无丢更新），即跨节点 RPC 在并发下恰好一次执行。
func TestAcceptance_TwoNodeCrossNodeCall_RaceStable(t *testing.T) {
	c := NewCluster(t, 2, counterKinds(nil))
	n1, n2 := c.Node(0), c.Node(1)
	ctx := context.Background()
	id := mustID(t, "counter", "cross-node")

	got := []int{0}
	v, err := n2.Call(ctx, id, 0) // miss → node2 本地激活
	if err != nil {
		t.Fatalf("activation call on node2: %v", err)
	}
	got[0] = v.(int)
	if got[0] != 1 {
		t.Fatalf("activation call returned %d, want 1", got[0])
	}

	const rounds, perRound = 4, 50
	for round := 0; round < rounds; round++ {
		var wg sync.WaitGroup
		vals := make([]int, perRound)
		errs := make([]error, perRound)
		for i := 0; i < perRound; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				v, err := n1.Call(ctx, id, 0) // 目录命中 node2 → 远程执行
				vals[i], errs[i] = -1, err
				if err == nil {
					vals[i] = v.(int)
				}
			}(i)
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("round %d call %d failed: %v", round, i, err)
			}
		}
		got = append(got, vals...)
	}

	sort.Ints(got)
	want := wantRange(1, 1+rounds*perRound)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("returned values must be exactly 1..%d (no dup, no gap, no lost update), got %v", len(want), got)
	}
}

// ---- 验收 ③（确定性版）：并发激活输家整箱转发零丢失 ----

// gatedDir 确定性目录包装（输家转发验收专用）：在目标身份上模拟
// 「输家的目录注册在途」的窗口（register-first：注册先于装载，仲裁在
// Register 原子域裁决）——
//   - Register(目标, 输家) 首次上报 entered 并停在 gate 上直到放行；放行后
//     落到真实目录执行（此刻赢家条目已就位 → 真实落败，非伪造的返回值）；
//   - 放行前 Lookup(目标) 虚拟返回输家归属：调用方据此把请求路由给输家，
//     经输家 dispatcher 落入其 pawnite 收件箱——成为可整箱转发的远端投递
//     （remoteDelivery）。本地 Call 的 callDelivery 不实现 Forwardable、
//     落败时按 Unavailable 回绝（调用方重试语义），无法用于本验收，
//     故 8 条在箱请求全部经网络面注入。
//
// 其余面（含输家/赢家之外的 Register 与基础面）经嵌入透传（inmem 具体实现
// 满足合并端口 Directory）。
type gatedDir struct {
	inner directory.Directory // 测试直连面（播种/检视）

	directory.Directory
	target  rans.ID
	loser   rans.NodeID
	entered chan struct{}
	gate    chan struct{}

	enterOnce   sync.Once
	releaseOnce sync.Once
	released    atomic.Bool
}

func (g *gatedDir) Register(ctx context.Context, id rans.ID, self directory.Owner, expected *directory.Owner) (bool, directory.Owner, error) {
	if id == g.target && self.Node == g.loser {
		g.enterOnce.Do(func() { close(g.entered) })
		g.releaseOnce.Do(func() {
			<-g.gate
			g.released.Store(true)
		})
	}
	return g.Directory.Register(ctx, id, self, expected)
}

func (g *gatedDir) Lookup(ctx context.Context, id rans.ID) (directory.Owner, bool, error) {
	if id == g.target && !g.released.Load() {
		return directory.Owner{Node: g.loser}, true, nil
	}
	return g.inner.Lookup(ctx, id)
}

// TestAcceptance_ConcurrentActivationLoserForwardsZeroLoss：输家整箱转发
// 的零丢失验收（ROADMAP 2.8）。三节点编排（调用方
// node3 / 输家 node1 / 赢家 node2——应答回程因此是常规跨节点路径；调用方与
// 赢家同节点的自源回程形态已由 Dispatcher.reply 短路修复并以
// remote/self_reply_test.go 锚定，本测试不重复覆盖）。
// 全程握手 channel，无承重 sleep：
//
//  1. 预热探针：node1 本地激活 probeID（真实注册，收件箱就绪）；
//  2. node3 对目标身份发 1 个 Call（经 gatedDir 的虚拟 Lookup 路由到 node1，
//     阻塞等应答）——等输家注册进入门控的信号（register-first：Register
//     是激活闭包首步，此刻实体已建、装载未起）：此刻该 Call 帧是
//     线上唯一的目标流量，其入箱首位由此钉死；
//  3. 追发 7 个 OneWay（Tell 返回即完成连接出队入序，帧序确定）；
//  4. 同步探针 Call（与上述 8 帧同一条 node3→node1 连接）：其应答返回即证明
//     读循环已按序处理完先到的 8 帧——8 条请求确定已全部在箱；
//  5. 测试以真实目录 CAS 替 node2 注册赢家条目（等效于 node2 的注册在输家
//     被门控期间胜出）；
//  6. 放行 gate：node1 的注册真实落败 → pawnite 整箱排空，8 条 remoteDelivery
//     以原始字节转发给赢家（MessageID/Source 原样保留）；
//  7. 断言：Call 经转发回程拿到正确响应（值 1，FIFO 首位）；8 条在箱消息全部
//     且恰好一次在赢家执行（计数序列 1..8）；输家本地表已清、赢家持有实例。
func TestAcceptance_ConcurrentActivationLoserForwardsZeroLoss(t *testing.T) {
	inner := inmem.New()
	target := mustID(t, "counter", "loser-forward")
	gate := make(chan struct{})
	gdir := &gatedDir{
		inner:     inner,
		Directory: inner,
		target:    target,
		loser:     1,
		entered:   make(chan struct{}),
		gate:      gate,
	}
	var openGate sync.Once
	defer openGate.Do(func() { close(gate) }) // 失败路径兜底放行，防 goroutine 悬挂

	// activated 容量 4 ≥ 全集群目标身份的 Activate 次数（register-first 下
	// 输家零 Activate，实际只有赢家 1 次；activated 通道保留——探测面不承重）；
	// hit 容量 16 ≥ 目标身份的全部 Handle 次数（8）。
	hit := make(chan int, 16)
	activated := make(chan struct{}, 4)
	kinds := counterKinds(&counterOpt{hit: hit, activated: activated})
	kinds["probe"] = func(_ rans.ID) rans.Entity { return probeEntity{} }
	c := NewCluster(t, 3, kinds, WithDirectory(gdir))
	loser, winner, caller := c.Node(0), c.Node(1), c.Node(2) // ID1=输家 ID2=赢家 ID3=调用方
	ctx := context.Background()
	probeID := mustID(t, "probe", "order-probe")

	// 1. 预热探针：node1 本地激活（真实注册进 inner，实体常驻可回快应答）。
	// probe 为独立 kind，不进 Activate/Handle 上报通道——后续等待只指向目标身份。
	if _, err := loser.Call(ctx, probeID, 0); err != nil {
		t.Fatalf("probe warm-up: %v", err)
	}

	// 2. 目标 Call 从 node3 出发，等输家的注册进门控（目标首帧已进
	// dispatcher；pawnite 已创建、装载未起——register-first 输家零用户代码）。
	type callRes struct {
		v   any
		err error
	}
	callDone := make(chan callRes, 1)
	go func() {
		v, err := caller.Call(ctx, target, 0)
		callDone <- callRes{v, err}
	}()
	select {
	case <-gdir.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("loser register never entered the gate within 5s")
	}

	// 3. 追发 7 个 OneWay（返回即完成出队入序）。
	for i := 0; i < 7; i++ {
		if err := caller.Tell(ctx, target, i); err != nil {
			t.Fatalf("seed one-way %d: %v", i, err)
		}
	}

	// 4. 探针 Call 同步收回应答 ⇒ 先于它的 8 帧已被 node1 读循环依序处理入箱。
	if _, err := caller.Call(ctx, probeID, 0); err != nil {
		t.Fatalf("order probe: %v", err)
	}

	// 5. 赢家注册：真实 CAS 在 inner 上落位（此刻输家的 Register 仍被门控）。
	if won, _, err := gdir.Register(ctx, target, directory.Owner{Node: 2}, nil); err != nil || !won {
		t.Fatalf("winner registration: won=%v err=%v", won, err)
	}

	// 6. 放行：输家的注册此刻真实落败于赢家条目 → 整箱转发。
	openGate.Do(func() { close(gate) })

	// 7a. 被转发的 Call 收到正确响应（转发保留 MessageID/Source，回程完整）。
	select {
	case r := <-callDone:
		if r.err != nil {
			t.Fatalf("forwarded call failed: %v", r.err)
		}
		if r.v != 1 {
			t.Fatalf("forwarded call returned %v, want 1 (FIFO head)", r.v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("forwarded call did not return within 5s")
	}

	// 7b. 零丢失：8 条在箱消息（1 Call + 7 OneWay）在赢家全部且恰好一次执行。
	seq := make([]int, 0, 8)
	for len(seq) < 8 {
		select {
		case v := <-hit:
			seq = append(seq, v)
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d/8 in-box messages reached the winner: %v", len(seq), seq)
		}
	}
	if fmt.Sprint(seq) != fmt.Sprint(wantRange(1, 8)) {
		t.Fatalf("winner handled %v, want exactly 1..8 (zero loss, exactly once)", seq)
	}

	// 7c. 归属落定：输家本地不留实例（终态逐出），赢家持有。输家 run 协程的
	// 收尾与第 8 次上报无相互 happens-before——命中残留时有界等其终态逐出
	//（waitLocalGone：公开面等价断言，终点与原 GetLocal+Done 白盒断言一致）。
	if loser.Holds(target) {
		waitLocalGone(t, loser, target, 5*time.Second)
	}
	if !winner.Holds(target) {
		t.Fatal("winner must hold the pawnite locally")
	}
}

// ---- 验收 ③（自由竞争面）：双节点对新鲜身份并发全发 ----

// callWithRetry 有界重试调用：激活竞态（并发首激 CAS 一赢一输）的失败从未
// 执行实体——重试不破坏恰好一次语义，本助手在预算内对一切错误重试；「只
// 重试确定未执行类」的约束由调用方的计数序列断言兜底（若误重试已执行请求，
// 返回值序列即出现重复/空洞——1..40 恰一断言会红）。注：公开面无「未执行
// 类错误」判别（错误模型不暴露瞬态哨兵）——序列断言是等效安全网（判别力
// 不弱化：误重试必红）。
func callWithRetry(ctx context.Context, n *Node, id rans.ID, req any, budget time.Duration) (int, error) {
	deadline := time.Now().Add(budget)
	for {
		v, err := n.Call(ctx, id, req)
		if err == nil {
			n, ok := v.(int)
			if !ok {
				return 0, fmt.Errorf("counter entity must return int, got %T", v)
			}
			return n, nil
		}
		if time.Now().After(deadline) {
			return 0, err
		}
	}
}

// TestAcceptance_FreeForAll_BothNodesNoLostUpdate：双节点各 20 并发 Call
// 同一新鲜身份（无预激活）：两节点各自首个请求触发激活 CAS，一赢一输；
// 输家侧本地调用按既定语义回绝后经重试改走远程路径直达赢家。断言：40 个
// 调用全部成功、返回值恰为 1..40（无丢失更新、无重复执行）。
func TestAcceptance_FreeForAll_BothNodesNoLostUpdate(t *testing.T) {
	c := NewCluster(t, 2, counterKinds(nil))
	n1, n2 := c.Node(0), c.Node(1)
	ctx := context.Background()
	id := mustID(t, "counter", "free-for-all")

	const perNode = 20
	var wg sync.WaitGroup
	vals := make([]int, perNode*2)
	errs := make([]error, perNode*2)
	for i, n := range []*Node{n1, n2} {
		for j := 0; j < perNode; j++ {
			idx := i*perNode + j
			wg.Add(1)
			go func(idx int, n *Node) {
				defer wg.Done()
				vals[idx], errs[idx] = callWithRetry(ctx, n, id, 0, 5*time.Second)
			}(idx, n)
		}
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("free-for-all call %d failed: %v", i, err)
		}
	}

	sort.Ints(vals)
	if want := wantRange(1, perNode*2); fmt.Sprint(vals) != fmt.Sprint(want) {
		t.Fatalf("returned values must be exactly 1..%d, got %v", perNode*2, vals)
	}
}

// ---- 故障原语 v1 可用性自证（Kill/Partition/Slow）----

// mustFailWithin 有界失败断言：故障注入下该调用必须以错误收场（超时/发送
// 错误，不 panic）。ctx 自带 deadline——网关的发送重试环（5×2s）先于请求
// 超时计时，无界 ctx 会让失败路径拖满全部重试（调用方必须传带 deadline 的
// ctx 是既定契约）。
func mustFailWithin(t *testing.T, n *Node, id rans.ID, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if _, err := n.Call(ctx, id, 0); err == nil {
		t.Fatalf("%s must fail", what)
	}
}

// TestKitKillPartitionSlow：Kill 后调用其归属失败（超时/发送错误，不 panic）；
// Partition 双向断网 → 双向调用失败、Heal 后恢复；Slow 注入延迟下调用仍成功。
func TestKitKillPartitionSlow(t *testing.T) {
	c := NewCluster(t, 2, counterKinds(nil), WithCallDefault(time.Second))
	n1, n2 := c.Node(0), c.Node(1)
	ctx := context.Background()
	idA := mustID(t, "counter", "kps-a") // 归属 node1
	idB := mustID(t, "counter", "kps-b") // 归属 node2

	if v, err := n1.Call(ctx, idA, 0); err != nil || v != 1 {
		t.Fatalf("activate idA on node1: v=%v err=%v", v, err)
	}
	if v, err := n2.Call(ctx, idB, 0); err != nil || v != 1 {
		t.Fatalf("activate idB on node2: v=%v err=%v", v, err)
	}

	// Slow：node1 每帧读写注入 40ms 延迟（含握手帧），跨节点调用仍须成功。
	c.Slow(0, 40*time.Millisecond)
	if v, err := n2.Call(ctx, idA, 0); err != nil || v != 2 {
		t.Fatalf("slow-path call: v=%v err=%v (latency must be tolerated)", v, err)
	}
	c.HealSlow(0)

	// Partition：双向既有连接关断 + 双向拨号屏蔽 → 双向调用失败。
	c.Partition(0, 1)
	mustFailWithin(t, n2, idA, "call across partition (n2→n1)")
	mustFailWithin(t, n1, idB, "call across partition (n1→n2)")

	// Heal：屏蔽解除，双向调用恢复。
	c.Heal(0, 1)
	if v, err := n2.Call(ctx, idA, 0); err != nil || v != 3 {
		t.Fatalf("healed call n2→n1: v=%v err=%v", v, err)
	}
	if v, err := n1.Call(ctx, idB, 0); err != nil || v != 2 {
		t.Fatalf("healed call n1→n2: v=%v err=%v", v, err)
	}

	// Kill：node1 停机（listener+连接+心跳），调用其归属只能超时/发送失败。
	c.Kill(0)
	mustFailWithin(t, n2, idA, "call to killed node's entity")
}

// ---- 并行两集群互不干扰的行为锁 ----

// TestParallelClustersIndependentInjection 并行注入隔离：
// 两集群同节点号、同 kind、同身份名，只有各自的 TestHooks 注入闭包——
// A 集群的 Partition/Slow/Kill 全程不得泄漏进 B 集群（拨号屏蔽表、延迟值、
// 地址表若为共享可变状态，任一断言即红；解析表互串则首段激活计数即错位）。
// 语义链：
//  1. 解析隔离：两集群各自激活同名身份、计数独立起步（互串即错位）；
//  2. 拨号钩子隔离：A 分区 → A 跨节点调用必须失败，同期 B 跨节点调用成功；
//  3. 延迟钩子隔离：B 慢化 → B 慢而通；A（Heal 后）快速完成（有界上界
//     排除 A 吃到 B 的延迟——共享延迟下跨节点调用 ≥4 帧×40ms=160ms）；
//  4. Kill 隔离：Kill A-n1 → A 归属调用失败，B 同号节点照常本地服务。
func TestParallelClustersIndependentInjection(t *testing.T) {
	cA := NewCluster(t, 2, counterKinds(nil), WithCallDefault(time.Second))
	cB := NewCluster(t, 2, counterKinds(nil), WithCallDefault(time.Second))
	a1, a2 := cA.Node(0), cA.Node(1)
	b1, b2 := cB.Node(0), cB.Node(1)
	ctx := context.Background()
	idA := mustID(t, "counter", "shared-name") // 两集群同名身份：计数序列各自独立
	idB := mustID(t, "counter", "shared-name")

	// 1. 各自激活（同名身份、各自目录）。
	if v, err := a1.Call(ctx, idA, 0); err != nil || v != 1 {
		t.Fatalf("cluster A activation: v=%v err=%v", v, err)
	}
	if v, err := b1.Call(ctx, idB, 0); err != nil || v != 1 {
		t.Fatalf("cluster B activation: v=%v err=%v", v, err)
	}

	// 2. 拨号钩子隔离：A 分区不影响 B。
	cA.Partition(0, 1)
	mustFailWithin(t, a2, idA, "partitioned call in cluster A (a2→a1)")
	if v, err := b2.Call(ctx, idB, 0); err != nil || v != 2 {
		t.Fatalf("cluster B cross call during A partition must be unaffected: v=%v err=%v", v, err)
	}

	// 3. 延迟钩子隔离：B 慢而通；A 的调用不吃 B 的延迟（有界上界 150ms：
	// 共享延迟下 ≥4 慢帧=160ms，正常跨节点调用在 -race 下也远低于该界）。
	cB.Slow(0, 40*time.Millisecond)
	if v, err := b2.Call(ctx, idB, 0); err != nil || v != 3 {
		t.Fatalf("cluster B slowed cross call must still succeed: v=%v err=%v", v, err)
	}
	cA.Heal(0, 1)
	start := time.Now()
	if v, err := a2.Call(ctx, idA, 0); err != nil || v != 2 {
		t.Fatalf("cluster A healed call: v=%v err=%v", v, err)
	}
	if d := time.Since(start); d > 150*time.Millisecond {
		t.Fatalf("cluster A call took %v — latency injection leaked from cluster B", d)
	}
	cB.HealSlow(0)

	// 4. Kill 隔离：A 死其 1 号节点，B 的 1 号节点照常。
	cA.Kill(0)
	mustFailWithin(t, a2, idA, "call into killed cluster-A node 1")
	if v, err := b1.Call(ctx, idB, 0); err != nil || v != 4 {
		t.Fatalf("cluster B node 1 must keep serving after cluster A kill: v=%v err=%v", v, err)
	}
}
