package testkit

// 迁移验收测试集（ROADMAP §5 验收①②③④ + §8 testkit 段）：迁移窗口零
// 丢失 / 换锁失败回退 / 包丢失退化 Store / 排空四段 + 重路由 / kill -TERM
// 优雅排空演练 / 看门狗强弃解绑 / 闲置钝化重激活。
//
// 编排纪律（与既有验收套件同口径）：
//   - 时序敏感断言一律走握手 channel / 有界 deadline（历史教训 6），无承重
//     sleep；异步收敛点用 pollUntil（cluster_redis_test.go，同包共享）；
//   - 迁移需要目录具备实例级换锁（TransferDirectory 内的
//     TryTransferOwnership）：inmem 亦具备该面，进程内
//     场景仍用 tkTransferDir——它提供 CAS 入口钩子（迁移窗的确定性抓手）；
//     真实 Redis Lua 换锁路径的回退场景按探活门禁 skip（bare
//     `go test ./...` 仍全绿）；
//   - 快照形状 = 8 字节大端计数（accCarrier 私有契约，与 remote 包迁移测试
//     同口径——形状不匹配即报错）。

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory"
)

// 公开面编排（testkit 依赖面 = 公开包 + internal/testhooks，断言终点逐条
// 核对，不弱化）：
//   - 固定迁移选点 → WithNodePlacement + migrateArmedPlacement（武装编排；
//     构造期注入，迁移发起前 Arm——选点与首激活共用 kind 维面，策略面不能
//     自行区分两者）；
//   - 迁移/排空/查询/销毁：Node.Migrate / Node.Shutdown / Node.Holds、
//     Node.LocalIDs / Node.Destroy；
//   - 看门狗：WithWatchdog（node 内置周期扫描——生产路径本身，轮询收敛点
//     替代手工 Scan）；
//   - 闲置策略：WithIdlePassivation（node 内置闲置收集循环）；
//   - 包丢失注入：Partition 传输级丢失（发送钩子是内部面；传输分区是包
//     丢失的真实形态，Store 兜底链路等价覆盖）；
//   - 错误形态细分：有界错误收场断言 + rans.Code/Reason 公开面。

// migrateArmedPlacement 带武装开关的迁移选点固定放置策略：未武装时放弃选点
// （首次激活回落本地激活——「实体先在源节点激活、再迁移」的编排前提）；
// Arm 后固定返回 target（迁移选点确定化）。选点与首激活共用同一 kind 维
// 面（迁移选点带 id.Kind），策略面不能自行区分两者——改为测试编排显式
// 武装（断言面不变：迁移目标确定性与激活回落语义都保留）。
type migrateArmedPlacement struct {
	target rans.NodeID
	armed  atomic.Bool
}

func (p *migrateArmedPlacement) Pick(_ string, _ rans.NodeID, _ []rans.NodeID) (rans.NodeID, bool) {
	if p.armed.Load() {
		return p.target, true
	}
	return 0, false
}

// arm 武装：此后的选点固定返回 target（迁移发起前调用）。
func (p *migrateArmedPlacement) arm() { p.armed.Store(true) }

// ---- 测试实体与目录 ----

// accCarrierOpt 计数迁移实体的观测点与卡点（nil/零值字段不启用）。通道由
// 测试持有并保证容量（实体在 pawnite 串行 loop 上上报，容量不足会阻塞 loop）。
// stuckDone 是跨实例共享的「已卡死」旗标：强弃后重激活的新实例不再卡——
// 卡死是编排一次性的（看门狗强弃后实体应可正常服务）。
type accCarrierOpt struct {
	activated    chan struct{} // Activate 上报（每次激活一次）
	arrivals     chan int64    // Handle 上报新计数值（每次执行一次）
	stickFrom    int64         // 计数达到该值的 Handle 卡在 stick 上（≤0 关闭）
	stick        chan struct{} // 卡死放行闸（关闭放行；10s 安全上限防泄漏）
	stuckDone    *atomic.Bool  // 共享一次性卡死旗标（nil = stickFrom 未启用）
	deactEntered chan struct{} // Deactivate 进入上报
	deactGate    chan struct{} // Deactivate 放行闸（nil 不阻塞；关闭放行）
}

// accCarrier 计数迁移实体：Handle 自增返回新值；快照 = 8 字节大端计数，
// restore 对长度≠8 报错（形状不匹配行为锁）。实现 rans.Snapshotter
// ——迁移/钝化/看门狗恢复场景的状态载体。
type accCarrier struct {
	mu    sync.Mutex
	count int64
	opt   accCarrierOpt
}

var (
	_ rans.Entity      = (*accCarrier)(nil)
	_ rans.Snapshotter = (*accCarrier)(nil)
)

func (e *accCarrier) Activate(context.Context) error {
	if e.opt.activated != nil {
		e.opt.activated <- struct{}{}
	}
	return nil
}

func (e *accCarrier) Deactivate(context.Context) error {
	if e.opt.deactEntered != nil {
		e.opt.deactEntered <- struct{}{}
	}
	if e.opt.deactGate != nil {
		select {
		case <-e.opt.deactGate:
		case <-time.After(10 * time.Second): // 泄漏安全上限
		}
	}
	return nil
}

func (e *accCarrier) Handle(_ context.Context, _ any) (any, error) {
	e.mu.Lock()
	e.count++
	n := e.count
	stick := e.opt.stickFrom > 0 && n >= e.opt.stickFrom &&
		e.opt.stuckDone != nil && !e.opt.stuckDone.Load()
	e.mu.Unlock()
	if e.opt.arrivals != nil {
		e.opt.arrivals <- n
	}
	if stick {
		e.opt.stuckDone.Store(true) // 一次性：重激活实例不再卡
		select {
		case <-e.opt.stick:
		case <-time.After(10 * time.Second): // 泄漏安全上限（zombie 收场）
		}
	}
	return n, nil
}

func (e *accCarrier) Snapshot() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return accSnap(e.count), nil
}

func (e *accCarrier) Restore(b []byte) error {
	if len(b) != 8 {
		return fmt.Errorf("accCarrier: snapshot shape mismatch: %d bytes, want 8", len(b))
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.count = int64(binary.BigEndian.Uint64(b))
	return nil
}

// accCarrierKinds 单 kind（carrier）工厂；需搭配探针等额外 kind 时在返回
// map 上自行追加。
func accCarrierKinds(opt accCarrierOpt) map[string]rans.Factory {
	return map[string]rans.Factory{
		"carrier": func(_ rans.ID) rans.Entity { return &accCarrier{opt: opt} },
	}
}

// accSnap 构造计数快照（8 字节大端）/ accSnapVal 解码。
func accSnap(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func accSnapVal(b []byte) int64 { return int64(binary.BigEndian.Uint64(b)) }

// waitArrivalValue 有界等待指定计数值的到达上报（更早的上报丢弃继续等）。
func waitArrivalValue(t *testing.T, ch chan int64, want int64) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case n := <-ch:
			if n == want {
				return
			}
		case <-deadline:
			t.Fatalf("execution #%d never arrived on the observation channel", want)
		}
	}
}

// tkTransferDir 具备实例级换锁的进程内测试目录（testkit 的迁移场景基座）：
// map+单锁，查主/换主同一临界区（CAS 原子性）；onCAS 是 TryTransferOwnership
// 的入口钩子（锁外调用）——迁移窗编排的确定性抓手（卡住 CAS 即卡住线性化点，
// pending+Destroying 已落位、快照/Store/随迁包已尽力而为）。setOwner 供测试
// 直接改写归属（第三方抢注的外部竞态等价物）。
type tkTransferDir struct {
	mu    sync.Mutex
	m     map[rans.ID]directory.Owner
	inc   map[rans.ID]uint64 // per-id 取号序列（跨条目死亡存续）
	onCAS func(id rans.ID)
}

func newTkTransferDir() *tkTransferDir {
	return &tkTransferDir{
		m:   make(map[rans.ID]directory.Owner),
		inc: make(map[rans.ID]uint64),
	}
}

var (
	_ directory.Directory = (*tkTransferDir)(nil)
	_ directory.Directory = (*tkTransferDir)(nil)
)

// nextIncLocked 取下一纪元（调用方必须已持 mu）：per-id 严格递增，首个 1。
func (d *tkTransferDir) nextIncLocked(id rans.ID) uint64 {
	d.inc[id]++
	return d.inc[id]
}

// Register 注册取号原子域（register-first，inmem 同语义）：缺失或自指向（收养）→
// 赢并升纪元；他节点持有 → 输并返回当前 owner。
func (d *tkTransferDir) Register(_ context.Context, id rans.ID, self directory.Owner, expected *directory.Owner) (bool, directory.Owner, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if cur, exists := d.m[id]; exists && cur.Node != 0 {
		switch {
		case cur.Node == self.Node && cur.Epoch == self.Epoch:
		case expected != nil && cur == *expected:
		default:
			return false, cur, nil
		}
	}
	owner := directory.Owner{Node: self.Node, Epoch: self.Epoch, Incarnation: d.nextIncLocked(id)}
	d.m[id] = owner
	return true, owner, nil
}

// UnregisterIfOwner 实例级条件注销：node+inc 同时匹配才删。
func (d *tkTransferDir) UnregisterIfOwner(_ context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	loc, found := d.m[id]
	if !found || loc != owner {
		return false, nil
	}
	delete(d.m, id)
	return true, nil
}

// Transfer 实例级换锁（Directory 本面方法，迁移的目录侧消费目标）：归属
// 匹配即取新纪元并改写归属；onCAS 入口钩子先于取锁触发。
func (d *tkTransferDir) Transfer(_ context.Context, id rans.ID, expected, target directory.Owner) (bool, uint64, directory.Owner, error) {
	if d.onCAS != nil {
		d.onCAS(id) // 迁移窗钩子：阻塞在此 = 线性化点被按住
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	cur, found := d.m[id]
	switch {
	case !found:
		return false, 0, directory.Owner{}, nil // 条目竞态消失：输但无特定输因
	case cur != expected:
		return false, 0, cur, directory.ErrNotOwner
	default:
		next := d.nextIncLocked(id)
		won := directory.Owner{Node: target.Node, Epoch: target.Epoch, Incarnation: next}
		d.m[id] = won
		return true, next, won, nil
	}
}

func (d *tkTransferDir) Lookup(_ context.Context, id rans.ID) (directory.Owner, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	loc, exists := d.m[id]
	return loc, exists, nil
}

// setOwner 直接改写归属（测试直连目录的外部干预面）。
func (d *tkTransferDir) setOwner(id rans.ID, node rans.NodeID) {
	d.mu.Lock()
	d.m[id] = directory.Owner{Node: node}
	d.mu.Unlock()
}

// ---- 验收①-a：迁移窗零丢失 ----

// TestAcceptance_MigrationWindowZeroLoss 三节点（源 node1 / 目标 node2 /
// 调用方 node3）：实体在 node1 计数到 2 → Migrate 卡在换锁 CAS 钩子上
// （迁移窗打开：pending+Destroying 已落位，快照已落 Store、随迁包已发）→
// 窗内从 node3 灌 8 个并发 Call——经目录仍指向 node1，全部准入在箱持有
// （pending 窗不回绝）→ 放行 CAS → 迁移赢 → 在箱消息整箱转发 node2 →
// 激活恢复（随迁包/Store 承载计数 2）→ 8 笔全部执行。
//
// 断言：8 个调用方全部成功（零回绝）；返回值恰为 3..10（计数连续——总数
// 8 + 迁移前 2，无丢失更新、无重复执行）；目录翻向 node2、源逐出、目标持有。
//
// 交错说明：8 笔请求出发于窗开之后，但各自「经窗内转发 / 翻锁后直接路由到
// 新家」取决于该帧写入连接的时序（probe 应答只锚定先于它写入的帧）——两种
// 交错都经 node2 的同一恢复实例执行，零丢失/连续性断言对全部交错成立；
// 窗内整箱转发的机制面已由 pawnite/migration 单元测试钉死。
func TestAcceptance_MigrationWindowZeroLoss(t *testing.T) {
	store := checkpoint.New() // 双端共享：Store 是正确性依赖（包竞态的兜底）
	dir := newTkTransferDir()
	gate := make(chan struct{})
	var openOnce sync.Once
	defer openOnce.Do(func() { close(gate) }) // 失败路径兜底放行
	entered := make(chan struct{}, 1)
	dir.onCAS = func(rans.ID) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-gate
	}

	kinds := accCarrierKinds(accCarrierOpt{})
	kinds["probe"] = func(_ rans.ID) rans.Entity { return probeEntity{} }
	migPlacement := &migrateArmedPlacement{target: 2}
	c := NewCluster(t, 3, kinds, WithDirectory(dir), WithCheckpointStore(store),
		WithNodePlacement(0, migPlacement)) // 源 node1 迁移选点武装后固定 node2
	src, dst, caller := c.Node(0), c.Node(1), c.Node(2)
	ctx := context.Background()
	id := mustID(t, "carrier", "mig-window")
	probeID := mustID(t, "probe", "mig-window-order")

	// 预热：探针实体预注册在 node1，node3 的一次调用建连（此后 node3→node1
	// 全部流量复用该连接——FIFO 锚的前提）。
	if won, _, err := c.Directory().Register(ctx, probeID, directory.Owner{Node: 1}, nil); err != nil || !won {
		t.Fatalf("probe pre-register: won=%v err=%v", won, err)
	}
	if _, err := caller.Call(ctx, probeID, 0); err != nil {
		t.Fatalf("probe warm-up call: %v", err)
	}
	// 源端计数到 2。
	for want := int64(1); want <= 2; want++ {
		v, err := src.Call(ctx, id, 0)
		if err != nil || v != want {
			t.Fatalf("source call %d: v=%v err=%v", want, v, err)
		}
	}

	// 发起迁移（选点武装后固定 node2）：core 卡在 CAS 钩子上——entered
	// 即「窗已开」的确定性握手（pending+Destroying 先于 core 执行落位）。
	migPlacement.arm()
	migDone := make(chan error, 1)
	go func() { migDone <- src.Migrate(ctx, id) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("migration core never reached the gated CAS within 5s")
	}

	// 窗内灌 8 个并发 Call（node3 → node1：目录未翻、pending 窗准入持有）。
	const n = 8
	type callRes struct {
		v   int64
		err error
	}
	results := make([]callRes, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := caller.Call(ctx, id, 0)
			var vv int64
			if err == nil {
				vv = v.(int64)
			}
			results[i] = callRes{v: vv, err: err}
		}(i)
	}
	// FIFO 锚：同步 probe 应答返回 ⟹ 先于它写入连接的请求帧已被 node1 处理
	//（准入在箱）。慢于 probe 写入的帧走翻锁后路由（见函数注释的交错说明）。
	if _, err := caller.Call(ctx, probeID, 0); err != nil {
		t.Fatalf("order probe call: %v", err)
	}

	openOnce.Do(func() { close(gate) }) // 放行 CAS → 迁移赢 → 整箱转发

	select {
	case err := <-migDone:
		if err != nil {
			t.Fatalf("Migrate must win after gate release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Migrate did not settle within 5s")
	}
	allDone := make(chan struct{})
	go func() { wg.Wait(); close(allDone) }()
	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatal("in-window calls did not settle within 10s")
	}

	vals := make([]int64, 0, n)
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("in-window call %d failed (zero-reject invariant): %v", i, r.err)
		}
		vals = append(vals, r.v)
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	want := make([]int64, 0, n)
	for v := int64(3); v <= 10; v++ { // 迁移前 2 + 8 笔
		want = append(want, v)
	}
	if fmt.Sprint(vals) != fmt.Sprint(want) {
		t.Fatalf("executions must be exactly 3..10 (continuity from pre-migration 2), got %v", vals)
	}

	// 归属落定：目录指向目标、源逐出、目标持有。
	if loc, found, _ := c.Directory().Lookup(ctx, id); !found || loc.Node != 2 {
		t.Fatalf("directory after migration = (%v, %v), want node 2", loc, found)
	}
	if src.Holds(id) {
		t.Fatal("source must evict the entity after migration won")
	}
	if !dst.Holds(id) {
		t.Fatal("target must hold the entity after migration won")
	}
}

// ---- 验收①-b：换锁失败回退（真实 Redis Lua CAS）----

// TestAcceptance_TransferFailRollback 三节点 redis 模式（真实 TryTransferOwnership
// Lua 的输路径）：实体在 node1 计数到 1 → 目录条目被第三方抢注（node1 注销 +
// node3 注册——外部竞态的目录原生操作等价物）→ Migrate 的 CAS 归属校验
// 落败（ErrNotOwner）→ 迁移输：pawnite 回退 Active、源继续服役；随后销毁源
// 实例（cleanup 条件注销）不得误删第三方条目。
//
// 长成员键存活期/存活节拍（60s/60s）：本测试锁 CAS 输回退语义而非成员失效
// 面——外部翻转只动目录条目、不动成员视图，长参数保证窗口内不叠加接管/
// 止损路径（成员键写持续失败的自检止损属独立语义，自有故障套件锁定）。
func TestAcceptance_TransferFailRollback(t *testing.T) {
	mustRedis(t)
	// 有状态实体的迁移以 Store 落库为前提（spec §5.1 第 3 点 core 矩阵）：
	// 未配置 Store 即拒绝迁移。本测试锚的是 CAS 输回退，注入空 Store 让
	// 流程到达换锁步。
	migPlacement := &migrateArmedPlacement{target: 2}
	c := NewCluster(t, 3, accCarrierKinds(accCarrierOpt{}),
		WithRedis(redisAddr(), time.Minute, time.Minute),
		WithCheckpointStore(checkpoint.New()),
		WithNodePlacement(0, migPlacement)) // 源 node1 迁移选点武装后固定 node2
	src := c.Node(0)
	ctx := context.Background()
	dir := c.Directory()
	id := mustID(t, "carrier", "mig-cas-lose")

	v, err := src.Call(ctx, id, 0)
	if err != nil || v != int64(1) {
		t.Fatalf("source activation call: v=%v err=%v", v, err)
	}

	// 第三方抢注：条目归属翻给 node3（node3 无本地实例——纯目录层抢注）。
	// 实例级注销按目录现值取归属标记（节点粒度签名已退役）。
	cur, found, err := dir.Lookup(ctx, id)
	if err != nil || !found {
		t.Fatalf("lookup before steal: found=%v err=%v", found, err)
	}
	if ok, err := dir.UnregisterIfOwner(ctx, id, cur); err != nil || !ok {
		t.Fatalf("third-party steal unregister: ok=%v err=%v", ok, err)
	}
	if won, _, err := dir.Register(ctx, id, directory.Owner{Node: 3}, nil); err != nil || !won {
		t.Fatalf("third-party steal register: won=%v err=%v", won, err)
	}

	// 迁移：CAS 输（归属已是 node3 ≠ 期望 node1）→ 迁移输因链携带
	// directory.ErrNotOwner（公开面断言锚——判别 CAS 输因的承载者）。
	migPlacement.arm()
	err = src.Migrate(ctx, id)
	if !errors.Is(err, directory.ErrNotOwner) {
		t.Fatalf("loss cause must carry directory.ErrNotOwner, got %v", err)
	}

	// 源恢复 Active：CAS 输家回退不逐出——实体仍在本地激活表（回退 Active
	// 的公开观测面；实例连续性由下一腿「销毁成功」闭合——若实例已失，
	// Destroy 返回 NotFound+NotLocal 即红）。
	if !src.Holds(id) {
		t.Fatal("source must roll the entity back to Active after CAS loss (still held locally)")
	}

	// 兜底注销不误删：销毁源实例（cleanup 的 UnregisterIfOwner 按归属匹配，
	// 条目属 node3）→ 第三方条目原样保留。
	if err := src.Destroy(ctx, id); err != nil {
		t.Fatalf("source destroy after rollback: %v", err)
	}
	pollUntil(t, 5*time.Second, "source eviction after destroy", func() bool {
		return !slices.Contains(src.LocalIDs(), id)
	})
	loc, found, err := dir.Lookup(ctx, id)
	if err != nil || !found {
		t.Fatalf("third-party entry must survive source cleanup: found=%v err=%v", found, err)
	}
	if loc.Node != 3 {
		t.Fatalf("third-party entry must stay owned by node 3, got node %d", loc.Node)
	}
}

// ---- 验收①-c：随迁包丢失退化 Store ----

// TestAcceptance_PackageLossStoreFallback 双节点（进程内换锁目录 + 共享
// InMemory Store）：源计数到 2 → 随迁包**传输级丢失**（Partition 双向分区：
// 随迁包的直发是迁移流程唯一的跨节点动作，拨号被屏蔽即包永不达——包丢失
// 的真实网络形态；发送钩子注入是内部装配面，不进公开编排）→
// 迁移照常赢（CAS 与包无关，best-effort）→ Store 落有 CAS 前写入的快照
// （值 2）→ Heal 后目标激活无包可取 → Store 兜底恢复 → 计数连续（3）；源端
// 后续调用经目录路由到新家续走（4）。
//
// 时序说明：分区下包直发经网关发送重试环耗尽（maxSendRetries×退避）后
// best-effort 收场——Migrate 随之阻塞到发送重试耗尽，属该注入形态的
// 预期时长（无响应值断言不受影响）。
func TestAcceptance_PackageLossStoreFallback(t *testing.T) {
	store := checkpoint.New()
	migPlacement := &migrateArmedPlacement{target: 2}
	c := NewCluster(t, 2, accCarrierKinds(accCarrierOpt{}),
		WithDirectory(newTkTransferDir()), WithCheckpointStore(store),
		WithNodePlacement(0, migPlacement)) // 源 node1 迁移选点武装后固定 node2
	n1, n2 := c.Node(0), c.Node(1)
	ctx := context.Background()
	id := mustID(t, "carrier", "mig-pkg-loss")

	for want := int64(1); want <= 2; want++ {
		v, err := n1.Call(ctx, id, 0)
		if err != nil || v != want {
			t.Fatalf("source call %d: v=%v err=%v", want, v, err)
		}
	}

	// 包丢失注入：Partition 关断 n1↔n2 全部通路——随迁包直发必然失败。
	migPlacement.arm()
	c.Partition(0, 1)
	if err := n1.Migrate(ctx, id); err != nil {
		t.Fatalf("Migrate must survive package loss (best effort): %v", err)
	}
	c.Heal(0, 1)

	if loc, found, _ := c.Directory().Lookup(ctx, id); !found || loc.Node != 2 {
		t.Fatalf("directory after migration = (%v, %v), want node 2", loc, found)
	}
	if b, found, _ := store.Get(ctx, id); !found || accSnapVal(b) != 2 {
		t.Fatalf("checkpoint store must carry the pre-CAS snapshot of 2, got (%v bytes, found=%v)", len(b), found)
	}

	// 目标恢复走 Store 兜底（包确定未达——分区内直发全败）：计数连续 3；
	// 源不再服役，调用路由新家续走 4。
	v, err := n2.Call(ctx, id, 0)
	if err != nil || v != int64(3) {
		t.Fatalf("target call after package loss: v=%v err=%v (want 3, store-restored)", v, err)
	}
	v, err = n1.Call(ctx, id, 0)
	if err != nil || v != int64(4) {
		t.Fatalf("source post-migration call must be served by target: v=%v err=%v (want 4)", v, err)
	}
}

// ---- 验收②：排空四段 + 门闸重路由 ----

// drainGateCell 排空时序实体：Handle 计数；Deactivate 先上报进入信号再停在
// 放行闸上——把 Drain 的阶段②按住在「销毁已开始、未终态」的窗口（门闸观测
// 抓手）。
type drainGateCell struct {
	n       int64
	entered chan struct{}
	gate    chan struct{}
}

func (e *drainGateCell) Activate(context.Context) error { return nil }
func (e *drainGateCell) Handle(context.Context, any) (any, error) {
	e.n++
	return e.n, nil
}
func (e *drainGateCell) Deactivate(context.Context) error {
	e.entered <- struct{}{}
	<-e.gate
	return nil
}

// TestAcceptance_DrainFourStages 三节点（重路由承接方 node1 / 被排空节点
// node2 / 探针调用方 node3）：实体落 node2 → Drain 启动，阶段②的 Deactivate
// 被按住（销毁进行中）→ 组合面两腿验收（门闸语义——本地表命中照常准入、
// miss 才回绝）：
//   - 腿零（门闸 miss）：目录条目指向 node2 但本地从未激活的 id → 新到
//     Request 撞门闸 miss → *rans.Error{Unavailable}（促重路由；环的 3 次
//     attempt 全撞同一 miss——条目未被排空收编、门闸不复位，attempt 间无
//     可变性）；
//   - 腿一（交接窗留箱）：发往退役中实体（本地表命中）的调用**留箱**——
//     不执行、不立即回绝；放行后销毁收尾注销条目，留箱消息按确定未执行
//     回绝 Unavailable，调用方环内重试重路由 → node1 本地激活接手成功
//     ——「重路由落到另一节点成功」的完整闭环（被回绝投递零重复执行）。
//
// 四段走完：Drain 返回 nil、node2 本地表清空。
func TestAcceptance_DrainFourStages(t *testing.T) {
	entered := make(chan struct{}, 4)
	gate := make(chan struct{})
	var openOnce sync.Once
	openGate := func() { openOnce.Do(func() { close(gate) }) }
	t.Cleanup(openGate) // 失败路径兜底放行（幂等）
	kinds := map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity { return &drainGateCell{entered: entered, gate: gate} },
	}
	c := NewCluster(t, 3, kinds)
	n1, n2, n3 := c.Node(0), c.Node(1), c.Node(2)
	ctx := context.Background()
	id := mustID(t, "cell", "drain-stages")

	if v, err := n2.Call(ctx, id, 0); err != nil || v != int64(1) {
		t.Fatalf("activation on node2: v=%v err=%v", v, err)
	}

	// 排空启动：门闸（阶段①）→ 销毁（阶段②）进入 Deactivate 被按住。
	drainDone := make(chan error, 1)
	go func() { drainDone <- n2.Shutdown(ctx) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("drain stage 2 never reached Deactivate within 5s")
	}

	// 门闸 miss 观测：目录指向 node2、本地从未激活的 id——排空节点不再
	// 产生新激活，新到 Request 收 Unavailable（非幂等直抛；条目无变化，
	// 环内 3 次 attempt 全撞同一 miss）。
	missID := mustID(t, "cell", "drain-gate-miss")
	if _, _, err := c.Directory().Register(ctx, missID, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatal(err)
	}
	// 门闸 miss 回绝的形态细分（StatusUnavailable）走 rans 错误模型公开面；
	// 本组合面锚定「排空节点对新目标确定回绝不激活」。
	_, err := n3.Call(ctx, missID, 0)
	if err == nil {
		t.Fatal("request for a local-miss target during drain must be rejected, got success")
	}

	// 交接窗留箱（腿一）：发往退役中实体的调用入箱悬挂——不执行、不立即
	// 回绝（实例 Handle 计数不变；Deactivate 被按住 ⟹ 终局处置未到）。
	type callResult struct {
		v   any
		err error
	}
	boxed := make(chan callResult, 1)
	go func() {
		v, err := n1.CallOpts(ctx, id, 0, rans.Idempotent())
		boxed <- callResult{v, err}
	}()
	select {
	case r := <-boxed:
		t.Fatalf("call to a retiring entity must stay in the box, got v=%v err=%v", r.v, r.err)
	case <-time.After(300 * time.Millisecond):
	}

	// 放行销毁 → 终局注销 → 留箱消息按确定未执行回绝 → 环内重试重路由：
	// miss → node1 本地激活接手成功（新实例——排空销毁丢弃旧状态是既定
	// 语义，本场景无快照面）。随后阶段③ flush → 阶段④ 停序，Drain 返回 nil。
	openGate()
	select {
	case r := <-boxed:
		if r.err != nil {
			t.Fatalf("boxed call must converge via reroute after the deferred rejection: %v", r.err)
		}
		if r.v != int64(1) {
			t.Fatalf("re-routed call returned %v, want 1 (fresh instance on the re-route target)", r.v)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("boxed call did not converge within 10s after gate release")
	}
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("drain did not return within 10s")
	}
	// 目录终态：旧实例的条目随终局注销，重路由的新实例已在 node1 落位
	//（register-first 重注册——条目归属翻转到承接节点）。
	pollUntil(t, 5*time.Second, "directory entry owned by the re-route target", func() bool {
		loc, found, err := c.Directory().Lookup(ctx, id)
		return err == nil && found && loc.Node == 1
	})

	// 落点：node1 接手（重路由成功），node2 排空后本地表清空。
	if !n1.Holds(id) {
		t.Fatal("re-route target node1 must hold the entity")
	}
	if ids := n2.LocalIDs(); len(ids) != 0 {
		t.Fatalf("drained node2 must hold no local entities, got %v", ids)
	}
}

// ---- 验收②补：kill -TERM 优雅排空演练（子进程）----

// TestAcceptance_SIGTERMGracefulDrain 把 testkit/sigtermhelper 构建成独立
// 二进制并做 kill -TERM 演练：READY → SIGTERM → 进程内走 Remote.Drain（预算
// 5s，四段排空：门闸 → 销毁本地实体 → 出站 flush → 停序）→ DRAINED → 退出码
// 0。helper 本体是宿主接线优雅停机的活样例（见 testkit/sigtermhelper/main.go）。
func TestAcceptance_SIGTERMGracefulDrain(t *testing.T) {
	runSigtermDrill(t)
}

// TestAcceptance_SIGTERMDrainStuckDeactivateDoesNotHang 卡死销毁下的排空边界：
// SIGTERM_DRILL_STUCK=1 激活一个 Deactivate 永不返回的实体
// 后发 SIGTERM——阶段②对该实体的 DestroyLocal 按排空预算返回（Drain 携带其
// DeadlineExceeded 错误，helper 打印 DRAINBUDGET 留痕），阶段③/④照常完成
// （DRAINED 打印、退出码 0）、进程在界内退出。卡死实体留给看门狗/其自身设备
// （§14.2；helper 不接线看门狗——排空的契约是停机不失去边界，不是看门狗收敛）。
func TestAcceptance_SIGTERMDrainStuckDeactivateDoesNotHang(t *testing.T) {
	runSigtermDrill(t, "SIGTERM_DRILL_STUCK=1")
}

// runSigtermDrill 构建并演练 sigtermhelper 子进程：READY → SIGTERM →
// DRAINED → 退出码 0 的完整链路断言（中间行如 DRAINBUDGET 被行级握手自然
// 跳过）。extraEnv 是附加的子进程环境变量（stuck 模式等）。
func runSigtermDrill(t *testing.T, extraEnv ...string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM drill requires POSIX signal semantics")
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source file")
	}
	helperDir := filepath.Join(filepath.Dir(thisFile), "sigtermhelper")
	bin := filepath.Join(t.TempDir(), "sigtermhelper")

	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancelBuild()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", bin, ".")
	build.Dir = helperDir
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build sigtermhelper: %v\n%s", err, out)
	}

	var stderr strings.Builder
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), extraEnv...)
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() }) // 失败路径兜底（已退出时无害）

	// 行级握手：READY（节点就绪）与 DRAINED（四段排空完成）各等到一次。
	lines := make(chan string, 16)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	waitLine := func(want string) {
		t.Helper()
		for {
			select {
			case line, ok := <-lines:
				if !ok {
					t.Fatalf("helper stdout closed before %q (stderr: %s)", want, stderr.String())
				}
				if strings.TrimSpace(line) == want {
					return
				}
			case <-time.After(15 * time.Second):
				t.Fatalf("timed out waiting for helper line %q (stderr: %s)", want, stderr.String())
			}
		}
	}

	waitLine("READY")
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}
	waitLine("DRAINED")

	exitDone := make(chan error, 1)
	go func() { exitDone <- cmd.Wait() }()
	select {
	case err := <-exitDone:
		if err != nil {
			t.Fatalf("helper must exit 0 after graceful drain, got %v (stderr: %s)", err, stderr.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("helper did not exit within 15s after SIGTERM")
	}
}

// ---- 验收③：看门狗强弃 + 解绑 + Store 恢复 ----

// TestAcceptance_WatchdogUnbind 双节点：实体落 node2、计数 1，Store 预置同值
// 快照（上轮 checkpoint——强弃丢弃 zombie 效果，恢复回到本点）→ 第二次
// Handle 卡死（stickFrom=2）→ 执行看门狗（短执行超时 + 主动 Scan 轮）强弃：
// 调用方解绑（Timeout 回执收场，不悬挂）、实体逐出 → 重激活经 Store 恢复
// 计数 1 → 下一笔 2（卡死的增量按 zombie 丢弃是规格语义）。
func TestAcceptance_WatchdogUnbind(t *testing.T) {
	store := checkpoint.New()
	stick := make(chan struct{})
	t.Cleanup(func() { close(stick) }) // zombie handler 放行（防 goroutine 泄漏）
	opt := accCarrierOpt{
		arrivals:  make(chan int64, 16),
		stickFrom: 2,
		stick:     stick,
		stuckDone: &atomic.Bool{},
	}
	c := NewCluster(t, 2, accCarrierKinds(opt), WithCheckpointStore(store),
		WithWatchdog(10*time.Millisecond, 150*time.Millisecond))
	n1, n2 := c.Node(0), c.Node(1)
	ctx := context.Background()
	id := mustID(t, "carrier", "watchdog-unbind")

	v, err := n2.Call(ctx, id, 0)
	if err != nil || v != int64(1) {
		t.Fatalf("activation on node2: v=%v err=%v", v, err)
	}
	if err := store.Put(ctx, id, 0, accSnap(1)); err != nil {
		t.Fatalf("seed checkpoint store: %v", err)
	}

	// 卡死调用：第二次 Handle 停驻（n1 发起 → node2 执行）。
	type callRes struct {
		v   int64
		err error
	}
	done := make(chan callRes, 1)
	go func() {
		v, err := n1.Call(ctx, id, 0)
		var vv int64
		if err == nil {
			vv = v.(int64)
		}
		done <- callRes{v: vv, err: err}
	}()
	// 等第二次执行到达（第一次调用的上报仍在缓冲里——丢弃继续等）。
	waitArrivalValue(t, opt.arrivals, 2)

	// 看门狗强弃（构造期注入短执行超时——node 内置看门狗按周期扫描，即
	// 生产路径本身；轮询实体逐出替代手工 Scan 驱动）。
	pollUntil(t, 5*time.Second, "watchdog force-abandon (entity evicted)", func() bool {
		return !n2.Holds(id)
	})

	// 调用方解绑：错误收场（强弃按 Timeout 回绝），不悬挂。
	select {
	case r := <-done:
		if r.err == nil {
			t.Fatal("stuck call must resolve with an error after force-abandon")
		}
		// 回执形态细分（StatusTimeout）走 rans 错误模型公开面；本组合面
		// 锚定「强弃后调用方有界解绑」。
	case <-time.After(5 * time.Second):
		t.Fatal("caller was not unbound — force-abandon must resolve the in-flight call")
	}

	// 重激活从 Store 恢复：计数回到 checkpoint(1) 续走（zombie 增量丢弃）。
	// 从宿主 node2 发起（其缓存已随强弃钩子①失效——确定性本地重激活；node
	// 恒装配位置缓存后，从 node1 发起会被其未失效的陈旧正条目钉回 node2，
	// 归属落点随缓存 TTL 漂移）。断言终点不变：Store 承载的连续性恢复。
	v, err = n2.Call(ctx, id, 0)
	if err != nil || v != int64(2) {
		t.Fatalf("post-abandon reactivation must restore from store: v=%v err=%v (want 2)", v, err)
	}
	if !n2.Holds(id) {
		t.Fatal("reactivation must host the entity on node2")
	}
}

// ---- 验收④：闲置钝化 → 重激活 ----

// TestAcceptance_IdlePassivation 单节点（进程内目录 + 共享 Store）：闲置策略
// （30ms）+ IdleCollector → 闲置后钝化（快照落 Store + 本地销毁：GetLocal
// miss、Store 持计数 2 快照）→ 下一笔 Call 重激活从 Store 恢复 → 计数连续
// （3）。收集轮由 pollUntil 驱动（阈值前的轮次空转返回 0——轮询本身即等待
// 面，无裸 sleep）。
func TestAcceptance_IdlePassivation(t *testing.T) {
	store := checkpoint.New()
	c := NewCluster(t, 1, accCarrierKinds(accCarrierOpt{}), WithCheckpointStore(store),
		WithIdlePassivation("carrier", 30*time.Millisecond))
	n1 := c.Node(0)
	ctx := context.Background()
	id := mustID(t, "carrier", "idle-passivation")

	for want := int64(1); want <= 2; want++ {
		v, err := n1.Call(ctx, id, 0)
		if err != nil || v != want {
			t.Fatalf("warm-up call %d: v=%v err=%v", want, v, err)
		}
	}

	// 闲置钝化（node 内置收集循环，20ms/轮）：闲置超阈 → 快照落 Store +
	// 本地销毁。轮询本地表清空即收集轮生效的公开观测面。
	pollUntil(t, 5*time.Second, "idle passivation (collect round)", func() bool {
		return !n1.Holds(id)
	})

	// 钝化落定：本地 miss + Store 有快照（计数 2）。
	if n1.Holds(id) {
		t.Fatal("passivated entity must be evicted locally (LocalIDs miss)")
	}
	if b, found, _ := store.Get(ctx, id); !found || accSnapVal(b) != 2 {
		t.Fatalf("store must hold the passivation snapshot of 2, got (%v bytes, found=%v)", len(b), found)
	}

	// 重激活：恢复 2 → 计数 3（连续性）。
	v, err := n1.Call(ctx, id, 0)
	if err != nil || v != int64(3) {
		t.Fatalf("reactivation after passivation: v=%v err=%v (want 3, store-restored)", v, err)
	}
}
