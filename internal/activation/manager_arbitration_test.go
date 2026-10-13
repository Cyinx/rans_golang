package activation

// 仲裁与目录一致性测试（规格 §12）：输家携带赢家、自指向残留清理、
// OnEvict 目录注销、陈旧 Destroyed 实例重活化。

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// noopEntity 最小实体：Activate/Deactivate 恒成功。
type noopEntity struct{}

func (noopEntity) Activate(context.Context) error           { return nil }
func (noopEntity) Handle(context.Context, any) (any, error) { return nil, nil }
func (noopEntity) Deactivate(context.Context) error         { return nil }

// newArbManager 仲裁测试基座：指定节点号与目录，kind "k" 用 noop 实体，
// 返回该 kind 下的一个身份。
func newArbManager(node rans.NodeID, dir directory.Directory, key string) (*Table, rans.ID) {
	m := New(dir, node, map[string]rans.Factory{
		"k": func(_ rans.ID) rans.Entity { return noopEntity{} },
	})
	id, _ := rans.NewID("k", key)
	return m, id
}

// gateEntity Activate 即成功。终局握手信号在目录包装 unregisterSignalDir
// 上（finish 置 Destroyed 后 cleanup 的第一步）——配对约束下仲裁落败
// 路径不再调用用户 Deactivate，旧「Deactivate 通道」握手面已随其退役。
type gateEntity struct {
}

func (e *gateEntity) Activate(context.Context) error { return nil }
func (e *gateEntity) Handle(context.Context, any) (any, error) {
	return nil, nil
}
func (e *gateEntity) Deactivate(context.Context) error { return nil }

// waitActive 有界轮询等待实例进入 Active；进入 Destroyed 或超时即失败。
func waitActive(t *testing.T, p *pawnite.Pawnite, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for p.State() != pawnite.StateActive {
		if p.State() == pawnite.StateDestroyed || time.Now().After(deadline) {
			t.Fatalf("%s state = %v, want Active", what, p.State())
		}
		time.Sleep(time.Millisecond)
	}
}

// loserRegGateDir 输家注册门控目录：target 的 Register 落败回包返回前经
// lostCh 上报（一次性）——输家终局的确定性观测锚。输家（owner
// 未捕获）的 cleanup 零目录调用，无「cleanup 注销进入」信号可用；本包装
// 改锚 Register 原子域的落败裁定（finish 置 Destroyed 只
// 剩纯内存步，紧随其后）。其余调用透传。
type loserRegGateDir struct {
	directory.Directory
	target rans.ID
	lostCh chan struct{}
	once   sync.Once
}

func (d *loserRegGateDir) Register(ctx context.Context, id rans.ID, self directory.Owner, expected *directory.Owner) (bool, directory.Owner, error) {
	won, owner, err := d.Directory.Register(ctx, id, self, expected)
	if id == d.target && !won {
		d.once.Do(func() { close(d.lostCh) })
	}
	return won, owner, err
}

// TestArbitrationLostErrorCarriesWinner 目录已有他人注册（节点 42）时，
// 本节点（1）激活必须落败且错误携带赢家位置（规格 §12 输家 Redirect 语义）。
// 激活错误只经入表后复检分支同步返回给调用方（在途激活按异步契约返回
// (p,nil)）。握手：readMu 读锁把 g1 停在入表写锁前，等
// Register 落败裁定信号后放锁——落败先于复检观测，同步窗为近似必然的
// 主窗；输家 cleanup 零目录调用后无「cleanup 进入」的更晚
// 信号，极端调度下残留的异步窗按双窗容忍断言（终态一致，非行为放宽）。
func TestArbitrationLostErrorCarriesWinner(t *testing.T) {
	inner := inmem.New()
	id, _ := rans.NewID("k", "arb-winner")
	if won, _, err := inner.Register(context.Background(), id, directory.Owner{Node: 42}, nil); err != nil || !won {
		t.Fatalf("pre-register winner node 42: won=%v err=%v", won, err)
	}
	dir := &loserRegGateDir{Directory: inner, target: id, lostCh: make(chan struct{})}

	ent := &gateEntity{}
	seated := make(chan struct{})
	proceed := make(chan struct{})
	m := New(dir, 1, map[string]rans.Factory{
		"k": func(_ rans.ID) rans.Entity {
			close(seated)
			<-proceed // g1 停在工厂（锁外），重锁双检在其后
			return ent
		},
	})

	g1done := make(chan struct{})
	var p1 *pawnite.Pawnite
	var err1 error
	go func() {
		defer close(g1done)
		p1, err1 = m.GetOrActivate(context.Background(), id)
	}()

	<-seated         // g1 已停在工厂（锁外），manager 锁上无人
	m.readMu.RLock() // 读锁占位：挡住入表/evict 的写锁，不挡双检的读锁
	close(proceed)
	<-dir.lostCh       // 输家的 Register 已真实落败（finish→Destroyed 只剩纯内存步）
	m.readMu.RUnlock() // 放行：g1 入表 → 复检（近似必然命中 Destroyed）
	<-g1done

	if err1 != nil {
		// 同步窗（主窗）：复检分支同步返回，错误链完整可断言。
		var lost *ArbitrationLostError
		if !errors.As(err1, &lost) || lost.Winner.Node != 42 || lost.WinnerNode() != 42 {
			t.Fatalf("err1 = %v, must carry winner node 42 via *ArbitrationLostError", err1)
		}
		if !errors.Is(err1, ErrActivationFailed) {
			t.Fatalf("err1 = %v, activation-failure sentinel semantics must survive wrapping", err1)
		}
		if p1 != nil {
			t.Fatal("arbitration-lost activation must not return a pawnite")
		}
		return
	}
	// 异步窗（防御性）：在途契约返回 (p, nil)，输家终局异步收场——
	// 终态与同步窗一致（pawnite Destroyed、目录仍归属赢家 42）。
	if p1 == nil {
		t.Fatal("async window must return the in-flight pawnite")
	}
	// 带期限收口（防御窗的等待不无限挂起——挂死防护仍由全局测试超时兜底，
	// 这里保证失败可诊断）。
	select {
	case <-p1.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("async loser must reach terminal state within deadline, state=%v", p1.State())
	}
	if loc, found, _ := inner.Lookup(context.Background(), id); !found || loc.Node != 42 {
		t.Fatalf("winner entry must stay intact after loser terminal: (%v, %v)", loc, found)
	}
}

// TestSelfResidueCleanedAndReactivated 自指向残留（规格 §12）：目录条目指向
// 本节点但本地无实例（如进程重启后的孤儿注册）→ 注销残留后重试注册一次，
// 激活必须成功且目录指向新本地实例。
func TestSelfResidueCleanedAndReactivated(t *testing.T) {
	dir := inmem.New()
	id, _ := rans.NewID("k", "arb-residue")
	if won, _, err := dir.Register(context.Background(), id, directory.Owner{Node: 1}, nil); err != nil || !won {
		t.Fatalf("seed self-pointing residue: won=%v err=%v", won, err)
	}
	m, id := newArbManager(1, dir, "arb-residue")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil || p == nil {
		t.Fatalf("self residue must be cleaned and activation retried: err=%v p=%v", err, p)
	}
	waitActive(t, p, "residue-cleaned pawnite")
	loc, found, err := dir.Lookup(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !found || loc.Node != 1 {
		t.Fatalf("directory = (%v, %v), want entry at node 1 pointing at the new local instance", loc, found)
	}
}

// TestOnEvictUnregistersDirectory 逐出必须注销目录：陈旧条目是跨节点路由毒源
// （他节点 Lookup 命中本节点却无实例，转发过来只能回绝）。Destroy 同步等待
// 终态且 OnEvict 先于 done 关闭执行，故注销可直接断言，无需轮询。
func TestOnEvictUnregistersDirectory(t *testing.T) {
	dir := inmem.New()
	m, id := newArbManager(1, dir, "arb-evict")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p, "evict-test pawnite") // 注册在 Active 之前完成
	if loc, found, _ := dir.Lookup(context.Background(), id); !found {
		t.Fatalf("precondition: activation must register in directory, got (%v, %v)", loc, found)
	}
	_ = p.Destroy(context.Background()) // 终局闭环直驱（DestroyLocal 已迁 lifecycle）
	if loc, found, _ := dir.Lookup(context.Background(), id); found {
		t.Fatalf("OnEvict must unregister directory entry (stale entry poisons cross-node routing), still: %v", loc)
	}
}

// TestGetLocalStaleDestroyedTreatedAsMiss 已销毁实例不得再从 GetLocal 售出：
// 命中但 State()==Destroyed 视为 miss（写锁+身份守卫逐出），后续 GetOrActivate
// 建新实例（≠旧实例）并真正激活成功，闭环「销毁后重激活」。
func TestGetLocalStaleDestroyedTreatedAsMiss(t *testing.T) {
	dir := inmem.New()
	m, id := newArbManager(1, dir, "arb-stale")
	p1, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	_ = p1.Destroy(context.Background()) // 终局闭环直驱（DestroyLocal 已迁 lifecycle）
	<-p1.Done()
	if p, ok := m.GetLocal(id); ok {
		t.Fatalf("destroyed pawnite must not be served from GetLocal, got state=%v", p.State())
	}
	p2, err := m.GetOrActivate(context.Background(), id)
	if err != nil || p2 == p1 {
		t.Fatalf("re-activation after destroy must return a fresh pawnite: err=%v same=%v", err, p2 == p1)
	}
	waitActive(t, p2, "re-activated pawnite")
	if got, ok := m.GetLocal(id); !ok || got != p2 {
		t.Fatal("local registry must hold the fresh pawnite")
	}
}

// ---- 注销先于逐出（exactly-once cleanup）----

// gatedUnregisterDir 目录包装：实例级条件注销 UnregisterIfOwner(Owner)
// 进入即上报 entered，其中第一个调用停在 gate 上直到放行，其余直通——用于
// 确定性编排「旧实例的注销被延迟到新实例注册之后」的交错（owner 已捕获
// 的终局路径才会触发本面；owner=nil 的 cleanup 零目录调用）。
// 嵌入透传（inmem 具体实现满足）。
type gatedUnregisterDir struct {
	directory.Directory
	entered chan struct{}
	gate    chan struct{}

	mu    sync.Mutex
	gated bool
}

func (g *gatedUnregisterDir) UnregisterIfOwner(ctx context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	g.enterUnregisterGate()
	return g.Directory.UnregisterIfOwner(ctx, id, owner)
}

func (g *gatedUnregisterDir) enterUnregisterGate() {
	g.mu.Lock()
	first := !g.gated
	g.gated = true
	g.mu.Unlock()
	g.entered <- struct{}{}
	if first {
		<-g.gate
	}
}

// TestEvictUnregisterDoesNotDeleteNewerRegistration：旧实例迟到的目录注销不得误删新实例刚注册的
// 条目——那会留下活实例无目录条目（静默路由丢失 + 脑裂窗）。
//
// 编排路径：优雅销毁走 Retire 交接窗，终局次序按 spec
// §5.1 第 4 点是「注销先于 Destroyed 翻转」——同 id 再激活在窗语义下被
// 串行化（留箱/回绝，见 staleasync 锚点 1），不产出「注销迟到 vs 新注册」
// 的交错。该交错落在强弃路径（abandon 单点：先翻转终态、后跑
// cleanup——Destroyed 即逐即走，异步化语义存续）：P1 执行卡死 → 强弃
// → 其注销被 gate 阻塞；并发 GetOrActivate 照常经自指向收养落位（Register
// 原子域换锁升纪元）；随后放行的旧注销以实例级标记 {node, inc} 比对：纪元
// 已换代，注销为 no-op，新条目幸存（绿）。安全性由实例级标记承载。
// 200ms 超时分支降级为防御性慢路径兜底（调度饥饿等极端情况下快速分支未
// 及时就绪时收敛，断言与快速分支一致），非承重 sleep。
func TestEvictUnregisterDoesNotDeleteNewerRegistration(t *testing.T) {
	inner := inmem.New()
	gdir := &gatedUnregisterDir{
		Directory: inner,
		entered:   make(chan struct{}, 8),
		gate:      make(chan struct{}),
	}
	release := make(chan struct{})
	ent := &stuckHandleEntity{release: release}
	m := New(gdir, 1, map[string]rans.Factory{
		"k": func(_ rans.ID) rans.Entity { return ent },
	})
	id, err := rans.NewID("k", "arb-regen")
	if err != nil {
		t.Fatal(err)
	}
	p1, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitActive(t, p1, "first pawnite")
	if loc, found, _ := inner.Lookup(context.Background(), id); !found || loc.Node != 1 {
		t.Fatalf("precondition: first activation must register at self, got (%v, %v)", loc, found)
	}
	// 执行卡死入场：Handle 阻塞（强弃执行时钟非零的前提）。
	callDone := make(chan error, 1)
	go func() { _, err := m.InvokeLocal(context.Background(), id, nil); callDone <- err }()
	waitFor(t, "stuck handle entered", ent.entered.Load)

	abandonDone := make(chan struct{})
	go func() {
		defer close(abandonDone)
		p1.Abandon(pawnite.StateActive, pawnite.TerminalAbandoned) // abandon 单点：先翻转终态，后跑 cleanup（复合体的强弃半步；清理重放已随迁移归 lifecycle，OnEvict 闭环不受影响）
	}()
	<-gdir.entered // P1 的注销已进入且被阻塞
	if st := p1.State(); st != pawnite.StateDestroyed {
		t.Fatalf("abandon must flip terminal before cleanup, got %v", st)
	}

	// 同 id 新激活：Destroyed 即逐即走（H7 异步化）——旧注销阻塞时照常
	// 经自指向收养落位。
	p2c := make(chan *pawnite.Pawnite, 1)
	errc := make(chan error, 1)
	go func() {
		p2, err := m.GetOrActivate(context.Background(), id)
		p2c <- p2
		errc <- err
	}()

	var p2 *pawnite.Pawnite
	select {
	case p2 = <-p2c:
		// 主路径（陈旧清理异步化）：陈旧清理只做本地逐出、不等旧注销——
		// 新实例在旧注销仍被阻塞时经自指向收养完成注册落位。
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
		close(gdir.gate) // 放行旧注销：实例级标记比对纪元已换代 → no-op
	case <-time.After(200 * time.Millisecond):
		// 防御性慢路径兜底：调度饥饿等极端情况下快速分支未及时就绪时收敛
		//（断言与快速分支一致，两分支终态等价），非承重 sleep。
		close(gdir.gate)
		p2 = <-p2c
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
	<-abandonDone
	// zombie Handle 收尾（强弃后续体），不留跨测试 goroutine。
	close(release)
	<-callDone

	waitActive(t, p2, "re-registered pawnite")
	if got, ok := m.GetLocal(id); !ok || got != p2 {
		t.Fatal("local registry must hold the re-registered pawnite")
	}
	if loc, found, _ := inner.Lookup(context.Background(), id); !found || loc.Node != 1 {
		t.Fatalf("newer registration must survive the old instance's delayed unregister, got (%v, %v)", loc, found)
	}
}

// stuckHandleEntity Handle 阻塞至放行的实体（执行卡死强弃的测试载体）。
type stuckHandleEntity struct {
	entered atomic.Bool
	release chan struct{}
}

func (e *stuckHandleEntity) Activate(ctx context.Context) error   { return nil }
func (e *stuckHandleEntity) Deactivate(ctx context.Context) error { return nil }
func (e *stuckHandleEntity) Handle(ctx context.Context, req any) (any, error) {
	e.entered.Store(true)
	<-e.release
	return nil, nil
}

// unregisterSpyDir 注销间谍目录：计数实例级注销调用（不改变行为）。
// 嵌入透传。
type unregisterSpyDir struct {
	directory.Directory
	unregisterCalls atomic.Int32
}

func (d *unregisterSpyDir) UnregisterIfOwner(ctx context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	d.unregisterCalls.Add(1)
	return d.Directory.UnregisterIfOwner(ctx, id, owner)
}

// TestSelfOwnedEntryAdoptedWithoutUnregister（迁移接管竞窗回归）：
// Register 原子域遇「条目已指向本节点」（自指向）必须直接收养（升纪元
// 覆写）——注销后重注册会在两步之间留下条目不存在的瞬时窗口：
// 迁移换锁（CAS 已把归属让渡给本节点）后的目标端激活恰经此分支，窗口内
// 源端 straggler 的注册可抢注成功，迁移的线性化结果被静默撤销
// （ISSUES 观察哨 TestAcceptance_MigrationWindowZeroLoss 根因之一，
// GOMAXPROCS=2 压测实测 directory 被翻回源节点）。
// 钉死面：激活全程注销零调用 + 条目持续存在 + 激活成功。
// 自指向收养的语义等价性：条目内容（Location{NodeID:self}）与重注册产物
// 逐字节相同；租约新鲜度由迁移换锁的 PEXPIRE 重置或心跳批量续租覆盖。
func TestSelfOwnedEntryAdoptedWithoutUnregister(t *testing.T) {
	dir := inmem.New()
	id, _ := rans.NewID("k", "arb-adopt")
	if won, _, err := dir.Register(context.Background(), id, directory.Owner{Node: 1}, nil); err != nil || !won {
		t.Fatalf("seed self-owned entry: won=%v err=%v", won, err)
	}
	spy := &unregisterSpyDir{Directory: dir}
	m, id := newArbManager(1, spy, "arb-adopt")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil || p == nil {
		t.Fatalf("self-owned entry must be adopted by activation: err=%v p=%v", err, p)
	}
	waitActive(t, p, "adopted pawnite")
	if n := spy.unregisterCalls.Load(); n != 0 {
		t.Fatalf("adoption must not unregister the self-owned entry (transient steal window), calls=%d", n)
	}
	loc, found, err := dir.Lookup(context.Background(), id)
	if err != nil || !found || loc.Node != 1 {
		t.Fatalf("entry must remain intact through adoption: (%v, %v, %v)", loc, found, err)
	}
}

// waitFor 带界轮询直至 cond 为真（原定义随看门狗复合体迁移至
// internal/lifecycle；本包多个测试仍以其为轮询助手，保留等语义副本）。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}
