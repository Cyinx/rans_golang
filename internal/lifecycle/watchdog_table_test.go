package lifecycle

// 看门狗编排测试（规格 §14.2）：
//   - manager 复合体 AbandonStuckDeactivation/AbandonStuckExecution：
//     pawnite 强弃成功 ⟹ 条件目录注销 + 本地逐出 + 钩子恰一次；
//   - 本地调用方解绑：卡死执行强弃后 manager.Call 收到 rans.ErrTimeout 类错误
//     （非死等）；
//   - 强弃后再激活：checkpoint.Store 兜底恢复（load-first）；
//   - WatchdogView 适配 + NewWatchdog 端到端自动强弃。

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// stuckExecEntity：Handle 永阻塞（进入即投票）；实现 rans.Snapshotter，
// 快照即计数字符串。
type stuckExecEntity struct {
	mu       sync.Mutex
	n        int
	entered  atomic.Int32
	release  chan struct{}
	restores atomic.Int32
}

func (e *stuckExecEntity) Activate(ctx context.Context) error { return nil }
func (e *stuckExecEntity) Handle(ctx context.Context, req any) (any, error) {
	e.entered.Add(1)
	<-e.release
	e.mu.Lock()
	defer e.mu.Unlock()
	e.n++
	return e.n, nil
}
func (e *stuckExecEntity) Deactivate(ctx context.Context) error { return nil }

func (e *stuckExecEntity) Snapshot() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return []byte(strconv.Itoa(e.n)), nil
}
func (e *stuckExecEntity) Restore(b []byte) error {
	n, err := strconv.Atoi(string(b))
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.n = n
	e.restores.Add(1)
	return nil
}

// stuckDeactEntity：Deactivate 永阻塞（进入即投票）。
type stuckDeactEntity struct {
	entered chan struct{}
	release chan struct{}
}

func (e *stuckDeactEntity) Activate(ctx context.Context) error { return nil }
func (e *stuckDeactEntity) Handle(ctx context.Context, req any) (any, error) {
	return nil, nil
}
func (e *stuckDeactEntity) Deactivate(ctx context.Context) error {
	close(e.entered)
	<-e.release
	return nil
}

// recDelivery 记录回绝的裸投递（在箱滞留者的解绑断言面）。
type recDelivery struct {
	id rans.ID
	mu sync.Mutex
	n  int
	st rans.Code
}

func (d *recDelivery) UUID() rans.ID              { return d.id }
func (d *recDelivery) Deliver(p *pawnite.Pawnite) {}
func (d *recDelivery) Reject(st rans.Code, _ rans.Reason) {
	d.mu.Lock()
	d.n++
	d.st = st
	d.mu.Unlock()
}
func (d *recDelivery) rejected() (int, rans.Code) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.n, d.st
}

// TestAbandonStuckDeactivation_Composite 卡死销毁复合体：目录条件注销 +
// 本地逐出 + onEvicted 恰一次；zombie Deactivate 事后返回无二次效应。
func TestAbandonStuckDeactivation_Composite(t *testing.T) {
	dir := newFakeTransferDir()
	ent := &stuckDeactEntity{entered: make(chan struct{}), release: make(chan struct{})}
	var evictions atomic.Int32
	m := activation.New(dir, 1, map[string]rans.Factory{
		"stuckd": func(_ rans.ID) rans.Entity { return ent },
	}, activation.Options{OnEvicted: func(id rans.ID) { evictions.Add(1) }})

	id, _ := rans.NewID("stuckd", "sd1")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	// 钉死时序：等激活闭包完成注册再销毁——否则 Destroy 与注册竞态，
	// 强弃/销毁让位守卫下销毁先赢会合法地跳过注册，
	// deleteCount 断言随调度漂移。
	waitFor(t, "directory registration", func() bool {
		_, found, _ := dir.Lookup(context.Background(), id)
		return found
	})
	go DestroyLocal(context.Background(), m, id)
	<-ent.entered // Deactivate 已卡死（Destroying 停驻）
	waitFor(t, "Destroying", func() bool { return p.State() == pawnite.StateDestroying })

	if !AbandonStuckDeactivation(m, id) {
		t.Fatal("composite abandon must win")
	}
	waitFor(t, "eviction", func() bool { _, ok := m.GetLocal(id); return !ok })
	if _, found, _ := dir.Lookup(context.Background(), id); found {
		t.Fatal("directory entry must be conditionally unregistered")
	}
	if dir.deleteCount() != 1 {
		t.Fatalf("unregister deletes = %d, want 1", dir.deleteCount())
	}
	if got := evictions.Load(); got != 1 {
		t.Fatalf("onEvicted = %d, want 1", got)
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("pawnite Done must be closed")
	}

	close(ent.release) // zombie Deactivate 返回：无二次注销/逐出/钩子
	time.Sleep(100 * time.Millisecond)
	if dir.deleteCount() != 1 {
		t.Fatalf("unregister deletes after zombie = %d, want 1", dir.deleteCount())
	}
	if got := evictions.Load(); got != 1 {
		t.Fatalf("onEvicted after zombie = %d, want 1", got)
	}
}

// TestAbandonStuckExecution_Composite 卡死执行复合体：在途/在箱本地调用方
// 解绑（rans.ErrTimeout 类，非死等）+ 目录注销 + 逐出；再激活从 Store 恢复。
func TestAbandonStuckExecution_Composite(t *testing.T) {
	dir := newFakeTransferDir()
	ent := &stuckExecEntity{release: make(chan struct{})}
	store := checkpoint.New()
	m := activation.New(dir, 1, map[string]rans.Factory{
		"stucke": func(_ rans.ID) rans.Entity { return ent },
	}, activation.Options{Store: store})

	id, _ := rans.NewID("stucke", "se1")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}

	// 预置快照（模拟上轮 checkpoint 落盘）：n=7。
	ent.mu.Lock()
	ent.n = 7
	ent.mu.Unlock()
	snap, _ := ent.Snapshot()
	if err := store.Put(context.Background(), id, 0, snap); err != nil {
		t.Fatal(err)
	}

	// 在途调用方（handler 永阻塞）：真实 manager.Call 面。
	errCh := make(chan error, 1)
	go func() {
		_, err := m.InvokeLocal(context.Background(), id, "req")
		errCh <- err
	}()
	waitFor(t, "handler entered", func() bool { return ent.entered.Load() >= 1 })

	// 在箱调用方：直接入同一 pawnite 的收件箱（避免与强弃竞态二次激活——
	// 后到的 Call 可能落到强弃后新建的 pawnite 上死等）。
	queued := &recDelivery{id: id}
	if !p.ReceiveRequest(queued) {
		t.Fatal("queued delivery must be admitted before abandon")
	}

	if !AbandonStuckExecution(m, id) {
		t.Fatal("composite force abandon must win")
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, rans.ErrTimeout) {
			t.Fatalf("unbound caller err = %v, want rans.ErrTimeout", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("caller was not unbound by force abandon")
	}
	if n, st := queued.rejected(); n != 1 || st != rans.CodeTimeout {
		t.Fatalf("queued delivery rejects = (%d, %v), want (1, StatusTimeout)", n, st)
	}
	waitFor(t, "eviction", func() bool { _, ok := m.GetLocal(id); return !ok })
	if _, found, _ := dir.Lookup(context.Background(), id); found {
		t.Fatal("directory entry must be unregistered")
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("pawnite Done must be closed")
	}

	// 再激活：load-first 从 Store 恢复（n=7 回来，免裸激活）。
	p2, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if p2 == p {
		t.Fatal("reactivation must build a fresh pawnite")
	}
	waitFor(t, "state restored from store", func() bool { return ent.restores.Load() >= 1 })
	ent.mu.Lock()
	n := ent.n
	ent.mu.Unlock()
	if n != 7 {
		t.Fatalf("restored state n = %d, want 7", n)
	}

	close(ent.release) // zombie handler 返回：效果丢弃，无二次逐出
	time.Sleep(100 * time.Millisecond)
	if _, ok := m.GetLocal(id); !ok {
		t.Fatal("fresh reactivated pawnite must survive zombie return")
	}
}

// TestWatchdogView_Adapter 适配面：枚举本地实体并携带状态与时钟快照。
func TestWatchdogView_Adapter(t *testing.T) {
	m := newTestTable(t)
	id, _ := rans.NewID("counter", "wv1")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "activation", func() bool { return p.State() == pawnite.StateActive })
	ents := WatchdogView(m)
	if len(ents) != 1 {
		t.Fatalf("view size = %d, want 1", len(ents))
	}
	e := ents[0]
	if e.ID != id || e.Kind != "counter" || !e.Active || e.Destroying {
		t.Fatalf("view entity = %+v, want Active counter entity", e)
	}
	if !e.ExecutingSince.IsZero() {
		t.Fatal("idle pawnite must report zero ExecutingSince")
	}
}

// TestWatchdogE2E_AutoAbandonsStuckExecution 端到端：短周期看门狗自动
// 强弃卡死执行实体并解绑调用方；ctx 取消收掉扫描 goroutine。
func TestWatchdogE2E_AutoAbandonsStuckExecution(t *testing.T) {
	dir := newFakeTransferDir()
	ent := &stuckExecEntity{release: make(chan struct{})}
	m := activation.New(dir, 1, map[string]rans.Factory{
		"stucke": func(_ rans.ID) rans.Entity { return ent },
	})
	id, _ := rans.NewID("stucke", "se2")
	if _, err := m.GetOrActivate(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := m.InvokeLocal(context.Background(), id, "req")
		errCh <- err
	}()
	waitFor(t, "handler entered", func() bool { return ent.entered.Load() >= 1 })

	wd := NewWatchdog(m, WatchdogConfig{
		Interval:      10 * time.Millisecond,
		ExecTimeout:   40 * time.Millisecond,
		UnloadTimeout: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); wd.Run(ctx) }()

	select {
	case err := <-errCh:
		if !errors.Is(err, rans.ErrTimeout) {
			t.Fatalf("caller err = %v, want rans.ErrTimeout", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("watchdog never unbound the stuck caller")
	}
	waitFor(t, "eviction", func() bool { _, ok := m.GetLocal(id); return !ok })

	cancel()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog Run leaked past ctx cancel")
	}
	defer func() { close(ent.release) }() // 收尾 zombie
}

// stuckActEntity：Activate 永阻塞（进入即投票）——loader IO 挂死形态
// （激活卡死强弃的目标场景）；Handle 正常计数。
type stuckActEntity struct {
	entered atomic.Bool // 投票面用原子标志：工厂复用同一实例，二次激活不得重复 close
	release chan struct{}
	mu      sync.Mutex
	n       int
}

func (e *stuckActEntity) Activate(ctx context.Context) error {
	e.entered.Store(true)
	<-e.release
	return nil
}
func (e *stuckActEntity) Handle(ctx context.Context, req any) (any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.n++
	return e.n, nil
}
func (e *stuckActEntity) Deactivate(ctx context.Context) error { return nil }

// TestAbandonStuckActivation_Composite 激活卡死复合体（register-first
// 形态）：卡死 Activate 的实例已入 local
// （GetLocal 粘性命中）→ 复合强弃解毒（逐出）；注册先于装载——卡死时条目
// 已落位，强弃终局清理以实例标记条件注销（deleteCount 1、Lookup miss，
// 集群面干净）；zombie Activate 迟回后**不得**触发 onActivated/留下条目
// （钩子时序倒置 + 幽灵 Track 面板）；再激活干净直注（全新纪元）。
func TestAbandonStuckActivation_Composite(t *testing.T) {
	dir := newFakeTransferDir()
	ent := &stuckActEntity{release: make(chan struct{})}
	var evictions atomic.Int32
	var activations atomic.Int32
	m := activation.New(dir, 1, map[string]rans.Factory{
		"stucka": func(_ rans.ID) rans.Entity { return ent },
	}, activation.Options{OnEvicted: func(id rans.ID) { evictions.Add(1) }, OnActivated: func(id rans.ID) { activations.Add(1) }})

	id, _ := rans.NewID("stucka", "sa1")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "Activate entered (stuck)", func() bool { return ent.entered.Load() })
	waitFor(t, "Activating dwell", func() bool { return p.State() == pawnite.StateActivating })
	if _, ok := m.GetLocal(id); !ok {
		t.Fatal("precondition: stuck instance must be sticky in local (black hole)")
	}

	if !AbandonStuckActivation(m, id) {
		t.Fatal("composite abandon must win")
	}
	waitFor(t, "black hole relieved", func() bool { _, ok := m.GetLocal(id); return !ok })
	if _, found, _ := dir.Lookup(context.Background(), id); found {
		t.Fatal("directory must be clean (instance-level unregister at abandon)")
	}
	if dir.deleteCount() != 1 {
		t.Fatalf("unregister deletes = %d, want 1 (register-first：装载期卡死的注册条目由强弃终局清理注销)", dir.deleteCount())
	}
	if got := evictions.Load(); got != 1 {
		t.Fatalf("onEvicted = %d, want 1", got)
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("pawnite Done must be closed")
	}

	// zombie Activate 迟回 nil：激活闭包的让位守卫必须让位——不得补发
	// onActivated（onEvicted 已消费——补发即钩子时序倒置 + 幽灵 Track）、
	// 不得再动目录（条目已随强弃注销，无二次注销/残留）。
	close(ent.release)
	time.Sleep(100 * time.Millisecond) // zombie 闭包收尾无外部可见信号，按既有 zombie 断言惯例
	if _, found, _ := dir.Lookup(context.Background(), id); found {
		t.Fatal("zombie activation continuation must NOT leave a directory entry")
	}
	if got := activations.Load(); got != 0 {
		t.Fatalf("onActivated after abandon = %d, want 0 (generation already evicted)", got)
	}
	if dir.deleteCount() != 1 {
		t.Fatalf("unregister deletes = %d, want 1 (no second unregister from the zombie continuation)", dir.deleteCount())
	}

	// 再激活：目录干净，直接注册成功（全新纪元），全新实例服役且
	// onActivated 恰一次（第二世代）。
	p2, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if p2 == p {
		t.Fatal("reactivation must build a fresh pawnite")
	}
	waitFor(t, "fresh activation", func() bool { return p2.State() == pawnite.StateActive })
	if got := activations.Load(); got != 1 {
		t.Fatalf("onActivated total = %d, want 1 (fresh generation only)", got)
	}
	if dir.deleteCount() != 1 {
		t.Fatalf("deletes after clean reactivation = %d, want 1 (fresh registration survives; no self-residue cleanup)", dir.deleteCount())
	}
}

// TestWatchdogView_ActivatingFace View 激活面：Activating 停驻实例被枚举为
// Activating=true + 非零进驻时钟（第三形态的扫描判据）。
func TestWatchdogView_ActivatingFace(t *testing.T) {
	dir := newFakeTransferDir()
	ent := &stuckActEntity{release: make(chan struct{})}
	m := activation.New(dir, 1, map[string]rans.Factory{
		"stucka": func(_ rans.ID) rans.Entity { return ent },
	})
	id, _ := rans.NewID("stucka", "sa2")
	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { // 收尾：放行 zombie 并销毁，不留跨测试 goroutine
		close(ent.release)
		DestroyLocal(context.Background(), m, id)
	}()
	waitFor(t, "Activate entered (stuck)", func() bool { return ent.entered.Load() })
	waitFor(t, "Activating dwell", func() bool { return p.State() == pawnite.StateActivating })

	ents := WatchdogView(m)
	if len(ents) != 1 {
		t.Fatalf("view size = %d, want 1", len(ents))
	}
	e := ents[0]
	if !e.Activating || e.Active || e.Destroying {
		t.Fatalf("view entity = %+v, want Activating only", e)
	}
	if e.ActivatingSince.IsZero() {
		t.Fatal("ActivatingSince must be stamped (unified state clock)")
	}
	if !e.DestroyingSince.IsZero() {
		t.Fatal("DestroyingSince must be zero while not Destroying")
	}
}

// TestWatchdogE2E_AutoAbandonsStuckActivation 端到端：短激活超时的看门狗
// 自动强弃卡死激活实体，粘性黑洞解除（GetLocal miss）。
func TestWatchdogE2E_AutoAbandonsStuckActivation(t *testing.T) {
	dir := newFakeTransferDir()
	ent := &stuckActEntity{release: make(chan struct{})}
	defer close(ent.release)
	m := activation.New(dir, 1, map[string]rans.Factory{
		"stucka": func(_ rans.ID) rans.Entity { return ent },
	})
	id, _ := rans.NewID("stucka", "sa3")
	if _, err := m.GetOrActivate(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "Activate entered (stuck)", func() bool { return ent.entered.Load() })

	wd := NewWatchdog(m, WatchdogConfig{
		Interval:          10 * time.Millisecond,
		ActivationTimeout: 40 * time.Millisecond,
		UnloadTimeout:     time.Hour,
		ExecTimeout:       time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); wd.Run(ctx) }()

	waitFor(t, "auto abandon relieves black hole", func() bool { _, ok := m.GetLocal(id); return !ok })

	cancel()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog Run leaked past ctx cancel")
	}
}
