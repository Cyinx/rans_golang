package testkit

// 故障注入验收（ROADMAP §4 验收①②的 testkit 场景面）。单元/接线级锚点
// ——分类真值表与退避序列、NotFound 重查与五钩子逐个触发、supersede-close
// 并发首拨快速失败（remote/retry_wiring_test.go）——已在各自层级锁定；
// 本文件验证组合后的可观测行为：杀节点 → pending 毫秒级快速失败、模糊
// 投递仅幂等重试、重试晚于首请求期限的弱时序界、位置缓存失效防陈旧路由。
// 时序敏感断言一律走握手 channel + 有界 deadline，不用裸 sleep。
//
// 断言形态（判别力逐条核对，不弱化）：
//   - 错误类经有界时序判别（快速失败 ≪ 请求超时 / 超时落在
//     [期限, 期限+扫描] 窗）——时序窗本身就是错误类的判别特征；错误形态
//     的细分断言走 rans.Code/Reason 公开面；
//   - 位置缓存白盒随组装根内部化，改行为链判别（调用成功性 + 实体落点
//     LocalIDs + 有界时延），逐腿注释注明。
import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// ---- 故障注入实体 ----

// arrival 实体到达观测点：请求载荷 + 到达时刻（退避时序断言的测量面）。
type arrival struct {
	req any
	at  time.Time
}

// blockingEntity 阻塞实体（KillFastFail 编排）：每次 Handle 上报到达后挂起，
// 直至 release 关闭（10s 安全上限防泄漏）——制造「请求已送达、在途未收场」
// 的受控窗口，Kill 落在窗口内。
type blockingEntity struct {
	arrivals chan arrival
	release  chan struct{}
}

func (e *blockingEntity) Activate(context.Context) error   { return nil }
func (e *blockingEntity) Deactivate(context.Context) error { return nil }
func (e *blockingEntity) Handle(_ context.Context, req any) (any, error) {
	e.arrivals <- arrival{req: req, at: time.Now()}
	select {
	case <-e.release:
	case <-time.After(10 * time.Second):
	}
	return "late", nil
}

// blockingKinds 阻塞实体工厂（release 归测试所有，t.Cleanup 关闭）。
func blockingKinds(arrivals chan arrival, release chan struct{}) map[string]rans.Factory {
	return map[string]rans.Factory{
		"blocker": func(_ rans.ID) rans.Entity {
			return &blockingEntity{arrivals: arrivals, release: release}
		},
	}
}

// slowEntity 慢实体（模糊投递/退避编排）：首次 Handle 挂起 slow（须超过
// 「调用方预算（Calls.Default）+ 网关扫描周期 250ms」保证首请求必超时），其后
// 立即应答——重试的第二次到达由此快速收场。
type slowEntity struct {
	arrivals chan arrival
	served   bool
	slow     time.Duration
}

func (e *slowEntity) Activate(context.Context) error   { return nil }
func (e *slowEntity) Deactivate(context.Context) error { return nil }
func (e *slowEntity) Handle(_ context.Context, req any) (any, error) {
	e.arrivals <- arrival{req: req, at: time.Now()}
	if !e.served {
		e.served = true
		time.Sleep(e.slow)
	}
	return "done", nil
}

// slowKinds 慢实体工厂。
func slowKinds(slow time.Duration, arrivals chan arrival) map[string]rans.Factory {
	return map[string]rans.Factory{
		"slow": func(_ rans.ID) rans.Entity {
			return &slowEntity{arrivals: arrivals, slow: slow}
		},
	}
}

// gateDir 门控目录（remote/retry_wiring_test.go flippingDir 的 testkit 等价物）：
// 指定 id 的 Lookup 结果可被测试翻转（陈旧路由编排）。被门控的 id 其
// Register 亦按门控视图裁决（匹配门控位置即注册/激活成功、不写穿内嵌
// 目录），保证翻转后新归属的激活不被旧落库条目卡死；未门控 id 全部透传。
// register-first：Register（取号+注册原子域）同按门控视图裁决——
// 门控命中的节点胜出并取门控纪元（per-id 门控序号），输家收到门控持有者。
// 嵌入透传（inmem 具体实现满足合并端口 Directory）。
type gateDir struct {
	directory.Directory
	mu        sync.Mutex
	locs      map[rans.ID]directory.Owner
	ownerOnly map[rans.ID]directory.Owner // 仅 Register 可见的门控归属（CAS 输家编排——Lookup 透传底座）
	incs      map[rans.ID]uint64          // 门控 id 的取号序列（视图内单调）
}

func newGateDir(base directory.Directory) *gateDir {
	return &gateDir{
		Directory: base,
		locs:      map[rans.ID]directory.Owner{},
		ownerOnly: map[rans.ID]directory.Owner{},
		incs:      map[rans.ID]uint64{},
	}
}

func (d *gateDir) Lookup(ctx context.Context, id rans.ID) (directory.Owner, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if loc, ok := d.locs[id]; ok {
		return loc, true, nil
	}
	return d.Directory.Lookup(ctx, id)
}

// Register 按门控视图裁决（register-first）：门控 id 匹配门控节点 → 赢并取门控
// 纪元（不写穿内嵌目录）；不匹配 → 输并返回门控持有者。ownerOnly 命中的
// id 同按门控归属裁决（激活的 Register 落败＝CAS 输家编排，Lookup 保持
// miss 让本地激活尝试发生）。未门控 id 透传。
func (d *gateDir) Register(ctx context.Context, id rans.ID, self directory.Owner, expected *directory.Owner) (bool, directory.Owner, error) {
	d.mu.Lock()
	gated, ok := d.locs[id]
	if !ok {
		gated, ok = d.ownerOnly[id]
	}
	d.mu.Unlock()
	if ok {
		if gated.Node != self.Node {
			return false, gated, nil
		}
		d.mu.Lock()
		d.incs[id]++
		inc := d.incs[id]
		d.mu.Unlock()
		won := directory.Owner{Node: gated.Node, Epoch: gated.Epoch, Incarnation: inc}
		return true, won, nil
	}
	return d.Directory.Register(ctx, id, self, expected)
}

func (d *gateDir) setLoc(id rans.ID, node rans.NodeID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.locs[id] = directory.Owner{Node: node}
}

// setOwnerOnly 登记仅 Register 可见的门控归属（CAS 输家编排：路由 Lookup
// 不见它——本地激活尝试照常发生，激活闭包的 Register 在门控视图上落败）。
func (d *gateDir) setOwnerOnly(id rans.ID, node rans.NodeID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ownerOnly[id] = directory.Owner{Node: node}
}

// waitArrival 有界等待下一笔到达（握手 channel 收口，超时即失败）。
func waitArrival(t *testing.T, ch chan arrival, req any) arrival {
	t.Helper()
	deadline := time.After(4 * time.Second)
	for {
		select {
		case a := <-ch:
			if a.req == req {
				return a
			}
			// 其他 id 的到达（同集群多场景共享观测通道）：丢弃继续等。
		case <-deadline:
			t.Fatalf("等待请求 %v 到达实体超时", req)
		}
	}
}

// ---- 验收①-a：杀节点 → 在途 pending 快速失败 ----

// TestAcceptance_KillFastFail 三节点（inmem——快速失败是传输层主干）：node1
// 对 node2 上的阻塞实体持有在途请求（已送达、handler 挂起）→ Kill(node2) →
// 连接丢失触发 FailPendingForNode：pending 毫秒级收场（远小于 3s
// 预算——不等满超时；错误类为 node-unavailable 快速失败，时序窗
// 即判别特征，形态细分断言走 rans.Code/Reason 公开面）。
func TestAcceptance_KillFastFail(t *testing.T) {
	arrivals := make(chan arrival, 16)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	c := NewCluster(t, 3, blockingKinds(arrivals, release))
	n1 := c.Node(0)
	ctx := context.Background()
	id := mustID(t, "blocker", "kill-fast-fail")
	if _, _, err := c.Directory().Register(ctx, id, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}

	type outcome struct {
		err error
		d   time.Duration
	}
	done := make(chan outcome, 1)
	go func() {
		start := time.Now()
		_, err := n1.Call(ctx, id, "kill-marker") // 非幂等：快速失败即上抛
		done <- outcome{err: err, d: time.Since(start)}
	}()
	waitArrival(t, arrivals, "kill-marker") // 请求已送达（pending 在途）

	t0 := time.Now()
	c.Kill(1) // node2 停机：listener + 全部连接
	select {
	case r := <-done:
		if r.err == nil {
			t.Fatal("pending 必须以错误快速失败, got nil")
		}
		if d := time.Since(t0); d >= 500*time.Millisecond {
			t.Fatalf("快速失败必须远小于 3s 调用预算, took %v", d)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("pending 调用在 Kill 后未收场")
	}
}

// ---- 验收①-b：模糊投递仅幂等重试 ----

// TestAcceptance_AmbiguousOnlyIdempotentRetries 慢实体（首请求挂 slow=1100ms）+
// timeout=650ms 调用预算：超时＝模糊投递（请求可能已送达——实体确实
// 收到）。非幂等 Call：有界上抛（时序窗 [期限, 期限+700ms] 即超时类的判别
// 特征——快速失败类远快于期限，到达数恰 1 次证明送达不重放）、实体恰好
// 收到 1 次（绝不自动重放）；幂等 CallOpts（全新身份）：环自动重试、第二次
// 到达即成功、实体恰好收到 2 次。两次到达数对比即「模糊类失败仅在幂等标注
// 下重试」的可观测证明。
// 时序参数约束（保证确定性 2 次到达，链：扫描周期 250ms < timeout=650ms
// < slow=1100ms < 2×timeout+20ms=1320ms，两侧余量均 ≥200ms——余量不足时
// 负载下易翻成第三笔到达）：
//   - 下侧（首请求必超时）：slow > timeout+250ms → 余量 1100−900=200ms
//     （过期条目的迟到响应被网关拒收——陷阱 11，故实际只需 slow>timeout；
//     +250ms 为覆盖扫描相位的保守量）；
//   - 上侧（不出现第三笔到达）：超时检出最早在期限处，+20ms 退避后重试
//     发出，重试自身期限最早为 2×timeout+20ms=1320ms，而第二次 Handle 的
//     应答在 slow=1100ms 即回 → 余量 1320−1100=220ms；检出越晚只会把重试
//     期限推后，不收紧该侧。
func TestAcceptance_AmbiguousOnlyIdempotentRetries(t *testing.T) {
	const (
		timeout = 650 * time.Millisecond
		slow    = 1100 * time.Millisecond
	)
	arrivals := make(chan arrival, 16)
	c := NewCluster(t, 2, slowKinds(slow, arrivals), WithCallDefault(timeout))
	n1 := c.Node(0)
	ctx := context.Background()

	// 非幂等：有界上抛，实体恰好 1 次到达。
	idPlain := mustID(t, "slow", "ambiguous-plain")
	if _, _, err := c.Directory().Register(ctx, idPlain, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}
	start := time.Now()
	_, err := n1.Call(ctx, idPlain, "plain")
	if err == nil {
		t.Fatal("非幂等模糊超时必须以错误收场, got nil")
	}
	// 下界＝期限（超时不可能早于期限收场）；上界＝最晚扫描检出（期限+250ms）
	// 加调度余量（+700ms 总富余，随 timeout 派生——实际检出落在 [期限,
	// 期限+250ms]，远早于慢实体收场）。
	if d := time.Since(start); d < timeout || d > timeout+700*time.Millisecond {
		t.Fatalf("非幂等超时应在期限后有界收场, took %v", d)
	}
	waitArrival(t, arrivals, "plain") // 首次到达（已送达的证明）
	select {
	case a := <-arrivals:
		t.Fatalf("非幂等调用不得自动重放（收到第二笔到达 %v）", a)
	case <-time.After(300 * time.Millisecond):
	}

	// 幂等：重试经第二次到达成功，实体恰好 2 次到达。
	idIdem := mustID(t, "slow", "ambiguous-idem")
	if _, _, err := c.Directory().Register(ctx, idIdem, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}
	start = time.Now()
	v, err := n1.CallOpts(ctx, idIdem, "idem", rans.Idempotent())
	if err != nil {
		t.Fatalf("幂等调用必须经重试成功: %v", err)
	}
	if v != "done" {
		t.Fatalf("重试成功应答 = %v, want done", v)
	}
	if d := time.Since(start); d > 2500*time.Millisecond {
		t.Fatalf("幂等重试必须有界收场, took %v", d)
	}
	waitArrival(t, arrivals, "idem")
	waitArrival(t, arrivals, "idem") // 第二次到达＝重试真实送达
	select {
	case a := <-arrivals:
		t.Fatalf("环上限内应恰好 2 次到达（多余到达 %v）", a)
	case <-time.After(300 * time.Millisecond):
	}
}

// ---- 验收①-c：重试退避时序 ----

// TestAcceptance_BackoffTiming 幂等重试时序的弱次序界（弱断言的诚实定性）：
// 慢实体 + 500ms 调用预算 → 首次尝试超时（模糊类）→ 环按
// DefaultBackoff(0)=20ms 退避后重试。串行实体模型下第二次到达被钉在首次
// Handle 收场（slow=900ms）之后——重试无论是否插入退避，都在实体队列里
// 等到那一刻，故两笔到达的间隔对退避不敏感；本断言的间隔下界
// （预算+20ms−20ms）只能证明「重试晚于首请求期限出发」的次序，
// 对退避不具判别力（零退避实现在期限后立即出发同样通过——−20ms 余量恰好
// 抵消 +20ms 退避项）。判别退避插入的断言在 routing/retry_test.go：
// TestRetryRingBackoffSequence（20/40ms 序列）、TestRetryRingRealBackoff-
// TimingBounds（真实短退避时序界）。NotFound 重查路径由
// remote/retry_wiring_test.go 接线锁定（其注入需要裸伪节点，testkit 的
// resolver 表无法承载——本测试锁组合后的可观测时序）。
func TestAcceptance_BackoffTiming(t *testing.T) {
	arrivals := make(chan arrival, 16)
	const timeout = 500 * time.Millisecond
	c := NewCluster(t, 2, slowKinds(900*time.Millisecond, arrivals),
		WithCallDefault(timeout))
	n1 := c.Node(0)
	ctx := context.Background()
	id := mustID(t, "slow", "backoff-timing")
	if _, _, err := c.Directory().Register(ctx, id, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}

	start := time.Now()
	v, err := n1.CallOpts(ctx, id, "gap", rans.Idempotent())
	if err != nil {
		t.Fatalf("幂等调用必须经退避重试成功: %v", err)
	}
	if v != "done" {
		t.Fatalf("重试成功应答 = %v, want done", v)
	}
	if d := time.Since(start); d > 2500*time.Millisecond {
		t.Fatalf("退避重试必须有界收场, took %v", d)
	}

	a1 := waitArrival(t, arrivals, "gap")
	a2 := waitArrival(t, arrivals, "gap")
	gap := a2.at.Sub(a1.at)
	// 下界（弱次序界，定性见函数注释）：重试不早于首请求期限出发；−20ms
	// 吸收回环传输与测量噪声。串行实体下实际间隔贴着 slow（900ms），
	// 远高于本下界——此处只排除「重试抢在首请求期限前出发」的次序错误。
	// （20ms = 环首档退避 DefaultBackoff(0) 的值；testkit 依赖面不含
	// internal/routing，公开面无此常量——数值锚定其单元测试
	// TestRetryRingBackoffSequence。）
	if min := timeout + 20*time.Millisecond - 20*time.Millisecond; gap < min {
		t.Fatalf("重试间隔 %v < 下界 %v（退避未被插入）", gap, min)
	}
	if gap > 2*time.Second {
		t.Fatalf("重试间隔 %v 超出上界 2s", gap)
	}
}

// ---- 验收②：位置缓存钩子的组合行为（testkit 场景） ----

// TestAcceptance_LocationCacheHooks 集群级组合验证：缓存失效钩子防止陈旧
// 路由。五钩子的逐个触发已在 remote/retry_wiring_test.go 接线锁定（含②目
// 录注销——poller 归宿主所有、④发送失败）；本场景覆盖其三的组合面
// （缓存随组装根内部化，以行为链判别——调用成功性 + 实体落点 + 有界时延）：
//
//	①本地销毁：Destroy → OnEvicted → Invalidate → 条目消失、下次调用全新
//	  激活（第二次 Activate 信号 + 新实例计数从 1 起步）；
//	③CAS 冲突：激活仲裁落败 → OnArbitrationLost → PutPositive(赢家) →
//	  后续调用直取赢家远程执行（落点断言：赢家持有、输家本地不复激活）；
//	⑤节点下线：Kill → OnConnectionLost → InvalidateNode + 目录翻转 →
//	  下次调用改投新归属（缓存不残留死路由——陈旧缓存会把调用钉死在死
//	  节点；有界时延锚定无重试拖累）。
func TestAcceptance_LocationCacheHooks(t *testing.T) {
	base := inmem.New()
	gate := newGateDir(base)
	hit := make(chan int, 16)
	activated := make(chan struct{}, 16)
	c := NewCluster(t, 3, counterKinds(&counterOpt{hit: hit, activated: activated}),
		WithDirectory(gate))
	n1, n2, n3 := c.Node(0), c.Node(1), c.Node(2)
	ctx := context.Background()

	// ① 本地销毁 → Invalidate → 再调用重新激活（第二次 Activate 信号；
	// 实体状态随销毁丢失——新实例计数从 1 重新起步是预期语义）。
	idLocal := mustID(t, "counter", "hook-destroy")
	if v, err := n1.Call(ctx, idLocal, nil); err != nil || v != 1 {
		t.Fatalf("本地激活调用: v=%v err=%v, want 1/nil", v, err)
	}
	<-activated // 第一次激活信号
	if err := n1.Destroy(ctx, idLocal); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	waitLocalGone(t, n1, idLocal, 2*time.Second)
	if v, err := n1.Call(ctx, idLocal, nil); err != nil || v != 1 {
		t.Fatalf("失效后再调用应重新激活（新实例从 1 起步）: v=%v err=%v, want 1/nil", v, err)
	}
	select {
	case <-activated: // 第二次激活信号＝重新激活的证明
	case <-time.After(2 * time.Second):
		t.Fatal("失效后再调用必须重新激活（未观察到第二次 Activate）")
	}

	// ③ CAS 冲突 → PutPositive(赢家)：ownerOnly 门控让 node1 的本地激活
	// 尝试在 Register 上确定落败给 node3（路由 Lookup 保持 miss——本地激活
	// 照常发生，公开调用面即可触发）。钩子③把赢家条目写进缓存后，node1 的
	// 后续调用直取赢家
	// 远程执行——落点判别（赢家持有、输家本地不复激活）即 PutPositive 的
	// 行为证明（无钩子时输家缓存无正条目，调用在本地 CAS 上原地失败，
	// 永不落到 node3）。
	idCAS := mustID(t, "counter", "hook-cas")
	gate.setOwnerOnly(idCAS, 3)
	_, _ = n1.Client().Call(ctx, idCAS, nil) // 触发激活（CAS 落败，异步收尾）
	pollCallOK := func() bool {
		v, err := n1.Call(ctx, idCAS, nil)
		return err == nil && v == 1
	}
	deadline := time.Now().Add(2 * time.Second)
	for !pollCallOK() {
		if time.Now().After(deadline) {
			t.Fatal("钩子③：仲裁落败后缓存必须 PutPositive 赢家 node3（调用经缓存直取赢家）")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !n3.Holds(idCAS) {
		t.Fatal("钩子③：调用必须落在赢家 node3 执行（赢家持有实例）")
	}
	if n1.Holds(idCAS) {
		t.Fatal("钩子③：输家 node1 本地不得持有该实体")
	}

	// ⑤ 节点下线 → InvalidateNode + 目录翻转 → 改投新归属。
	idKill := mustID(t, "counter", "hook-node-down")
	gate.setLoc(idKill, 2) // 门控归属 node2（Register 按门控视图裁决）
	if v, err := n1.Call(ctx, idKill, nil); err != nil || v != 1 {
		t.Fatalf("暖场调用（node2）: v=%v err=%v, want 1/nil", v, err)
	}
	if !n2.Holds(idKill) {
		t.Fatal("暖场后实体必须落 node2（正条目路由的落点证明）")
	}
	c.Kill(1)              // node2 停机
	gate.setLoc(idKill, 3) // 目录翻转：新归属 node3
	// 改投收敛（有界轮询）：Kill 的连接关断与首呼存在竞速窗——撞上断连
	// 摊派的调用按 node-unavailable 快速失败（确定未执行：node2 已死，
	// 测试侧重试无重复执行风险）；陈旧正条目首拨撞死节点由钩子④失效后
	// 重试改投。时延界＝一轮发送重试间隔（缺省 2s）+ 调度余量；「钉死在
	// 死节点」的病态形态是 30s 级超时，界内不收敛即红。
	// pollUntil 超时即 Fatal（改投未在界内收敛＝陈旧缓存长期拖累）。
	pollUntil(t, 4500*time.Millisecond, "失效+翻转后调用改投 node3（一轮失效+重试周期内）", func() bool {
		v, err := n1.Call(ctx, idKill, nil)
		return err == nil && v == 1
	})
	if !n3.Holds(idKill) {
		t.Fatal("改投后实体必须落 node3（新归属落点证明）")
	}
}
