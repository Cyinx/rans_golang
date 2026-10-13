// Package pawnite 实现执行单元：一条身份 + 一个收件箱 + 至多一个实体实例，
// 逻辑串行、线程无关（移植规格 §4）。
package pawnite

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/log"
)

type State int32

const (
	StateCreated State = iota
	StateActivating
	StateActive
	StateDestroying
	StateDestroyed
)

// Pawnite：状态转移与收件箱准入持 mu（锁内无 IO/回调，
// 规格 §19 陷阱 2）；消息处理全部发生在 loop goroutine（锁外执行）。
type Pawnite struct {
	id   rans.ID
	opts Options

	// selfHash 本实体身份的 FNV-1a 64 哈希（构造期算定，热路径零分配）——
	// 准入判环的自身比对项（H12 决策 15）。
	selfHash uint64

	mu       sync.Mutex
	state    State
	started  bool
	mailbox  []InboundDelivery // mu：FIFO 邮件队列，按需增长；准入上限 inboxCap（满箱回绝语义不变）
	mailHead int               // mu：mailbox 已消费前缀长度（head==len 即整批耗尽：回收大缓冲/复用小缓冲）
	mailCh   chan struct{}     // 容量 1 的就绪信号（非阻塞投递；队列有余量时消费侧重武装）
	inboxCap int               // 收件箱准入上限（New 定值：InboxCap<=0 → InboxDepthLimit；仅作上限，不预分配）

	stop chan struct{}
	done chan struct{}

	// 交接窗状态（mu 保护；规格 §5.1 H6 泛化 + §13.1/§13.2 + §19 陷阱 3）。
	handoffCh        chan handoffReq // 交接信号（loop 消费；容量 1——状态机门控单飞）
	handoffPending   bool            // 窗标志：先于 Destroying 置位（陷阱 3）
	handoffFollowers []chan error    // 组合规则等待者：窗内 Retire 等在飞结果（Handoff 登记，各持容量 1 通道）
	stopClosed       bool            // stop 已关（mu 内判销毁介入，替代锁内通道探测）
	fenced           bool            // mu：租约失守后的准入关断（Fence() 置位，只升不降；在途投递不受影响）

	// 让出重入（H12 决策 16/17 后半，机制见 reenter.go）：续体信号 + mu 队列
	//（then 回 loop 的串行执行序）、在飞 work 登记表与挂起交接。
	reenterCh   chan struct{}          // 续体就绪信号（loop 消费；容量 1 非阻塞投递）
	reenterQ    []func()               // mu：续体 FIFO 队列
	works       map[*workUnit]struct{} // mu：实例级在飞 work 登记表
	heldHandoff *heldHandoff           // mu：等在途 work 的挂起交接请求

	// 终局归一（mu 保护）：五条终局路径（优雅销毁、Start 前快路径、迁移赢、
	// 仲裁输、看门狗强弃）的原因单点——写一次（setTerminalLocked）、迟到
	// 处置单点（disposeLate 终局矩阵）。Migrated/ArbitrationLost 携带转发
	// 目标：迟到者（终局翻转前后准入/敲门的——调用方位置是目录归属让渡前
	// 的合法观测，规格 §12/§13.2 零丢失）转投目标而非回绝。
	term terminal

	// 生命周期钩子取消面（规格 §14.2 Go 侧注记）：Activate/Deactivate
	// 钩子消费的 ctx 是此派生 hookCtx——values 保留、parent 取消照旧传播，
	// 另有「强弃取消」：三个强弃 gate 置终态时取消它，在飞 zombie 钩子
	// （协作宿主）尽早返回，「强弃后新 incarnation 与 zombie 并发」窗口
	// 从无界收敛到宿主配合度界。优雅路径永不取消（销毁收尾不因调用方
	// 取消而中断）。hookCtx/hookCancel 由 Start 在 go run 之前写一次
	// （goroutine 起跑建立可见性，run 侧免锁读；gate/releaseHooks 侧持 mu 读）。
	hookCtx      context.Context    // mu：钩子专用 ctx（Start 写一次）
	hookCancel   context.CancelFunc // mu：hookCtx 取消面（锁外调用）
	hookInFlight string             // mu：当前在飞钩子名（"activate"/"deactivate"/""），gate 留痕依据

	// 强弃面状态（规格 §14.2 三看门狗 + §19 陷阱 12 单写者纪律）。
	// stateSince 统一状态进驻时钟：Created→Activating / →Active / →Destroying
	// 每次转移盖戳（迁移输回退 Destroying→Active 同样盖戳），→Destroyed 清零
	// ——Activating/Destroying 停驻判据同源一个时钟。
	// abandoned 是强弃让位标志，与 write-once 的 term.reason 分属两个维度：
	// term 记「迟到处置依据的首个终局原因」（先到权威——强弃迟到时补写被
	// 忽略，处置按先到原因分流）；abandoned 记「看门狗强弃已赢得终局竞赛」
	// 这一事实（gate 单临界区内无条件置位）。终局原因先落（Migrated/
	// ArbitrationLost）+ 强弃后胜的交错下（如迟到处置的锁外回调段挂死），
	// 强弃的终局化不因 terminal 已写而丢失——loop 侧续体据此让位。
	stateSince time.Time // mu：当前状态进驻时刻；Created/Destroyed 为零
	abandoned  bool      // mu：看门狗强弃已置终态——loop 侧续体（stop 分支/激活失败分支）据此让位
	doneClosed bool      // mu：done 关闭幂等标志（run defer 与强弃两条关闭路径）
	evictOnce  sync.Once // 终局效应（OnEvict）恰一次守卫：finish 与强弃竞争单胜

	// 快照捕获面（规格 §15）：loop 任务单飞通道 + 活动纪元（脏门控依据：
	// 进入 Active 记 1、每次投递 +1；manager 侧比对已 checkpoint 纪元，
	// 闲置实体永不重写）。机制见 checkpoint.go。
	// 注意：activityEpoch 是实例内相对纪元（脏门控专用），与 incarnation
	//（跨 incarnation 单调的 fencing 纪元）语义不同层。
	captureCh     chan *captureReq
	activityEpoch atomic.Int64

	// incarnation 本 incarnation 的 fencing 纪元。落位两条路径：New 时
	// Store Options.Incarnation（直构注入）；激活方在激活闭包首步取号后
	// 经 AssignIncarnation 首写（CAS(0, inc)）。落位后不可变，但落位点可能
	// 晚于构造（run goroutine 起跑后），投递层跨 goroutine 读取——atomic。
	// 0 = 未围栏。
	incarnation atomic.Uint64

	executing          atomic.Pointer[InboundDelivery] // 在途投递（强弃解绑目标；loop invoke 置位/清零）
	executingSinceNano atomic.Int64                    // 执行停驻时钟（执行看门狗依据）：invoke 起置位、归零=空闲

	lastDeliveryNano atomic.Int64 // 闲置时钟（规格 §4.1）：invoke 入口原子更新，零值=从未投递
}

func New(id rans.ID, opts Options) *Pawnite {
	capv := opts.InboxCap
	if capv <= 0 {
		capv = InboxDepthLimit
	}
	p := &Pawnite{
		id:        id,
		opts:      opts,
		selfHash:  id.Hash(),
		inboxCap:  capv,
		mailCh:    make(chan struct{}, 1),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
		handoffCh: make(chan handoffReq, 1),
		captureCh: make(chan *captureReq, 1),
		reenterCh: make(chan struct{}, 1),
		works:     make(map[*workUnit]struct{}),
	}
	p.incarnation.Store(opts.Incarnation)
	return p
}

func (p *Pawnite) UUID() rans.ID { return p.id }

// Incarnation 返回本 incarnation 的 fencing 纪元（落位路径见字段注释；
// 落位后不可变）。0 = 未围栏。宿主侧请优先读 ctx（rans.Incarnation）——钩子与
// Handle 的 ctx 都已携带；本访问面是投递层（manager/rpc Deliver）的
// 接线点。
func (p *Pawnite) Incarnation() uint64 { return p.incarnation.Load() }

// AssignIncarnation 落位本实例的 fencing 纪元（非配置项，激活路径的
// 一次性令牌落位）：激活方的取号落点（激活闭包首步取号后、实体 Activate
// 前调用）。首次写入生效，后续
// 调用静默忽略；0 保留为未围栏哨兵（目录纪元源 HINCRBY 产物恒 ≥1）。
// 并发安全（与投递层的 Incarnation 读取无锁协调）。
func (p *Pawnite) AssignIncarnation(inc uint64) {
	if inc == 0 {
		return
	}
	p.incarnation.CompareAndSwap(0, inc)
}

// Fence 关闭准入：之后 ReceiveRequest 一律以 StatusUnavailable（"lease lost"）
// 回绝新投递；在途投递不受影响。幂等、非阻塞、只改内存状态。供租约失守时
// 的本地止损调用（失租销毁前的第一步；销毁本身走 Destroy）。
func (p *Pawnite) Fence() {
	p.mu.Lock()
	p.fenced = true
	p.mu.Unlock()
}

func (p *Pawnite) State() State {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state
}

// Start 起 loop goroutine。激活动作（宿主 Activate 闭包）在 loop 内最先
// 执行，完成前收件箱消息原地留队（规格 §12 register-first：目录注册在
// 装载之前，由宿主激活闭包完成）。
// 重复 Start 或已进入销毁态时不再起第二个 loop——单 loop 是串行不变量的根基。
func (p *Pawnite) Start(ctx context.Context) {
	p.mu.Lock()
	if p.started || p.state == StateDestroying || p.state == StateDestroyed {
		p.mu.Unlock()
		return
	}
	p.started = true
	// 钩子取消面派生点：先于 go run 写入，run 侧免锁读成立。
	p.hookCtx, p.hookCancel = context.WithCancel(ctx)
	p.mu.Unlock()
	go p.run(ctx)
}

func (p *Pawnite) run(ctx context.Context) {
	defer p.releaseHooks() // 钩子取消面回收（幂等；loop 退出时钩子必已返回或已被强弃取消）
	defer p.closeDone()    // 幂等关闭：强弃可能已关（终局效应单胜，见 closeDone）
	hookCtx := p.hookCtx   // Start 先于 go run 写入：起跑后即不可变（gate 只读 cancel，不复写）
	p.mu.Lock()
	// 条件化转移（状态机单调性，与下方 Activate 后的转移同族）：Destroy 若在
	// loop goroutine 起跑前抢先置 Destroying（Start 已置 started 后、run 尚未
	// 持锁的窗口），此处不得回退——回退会让"卡死在 Deactivate 的销毁"以
	// Active 面目对外可见，卸载看门狗的 Destroying 停驻判据失效（规格 §14.2）。
	if p.state == StateCreated {
		p.state = StateActivating
		p.stateSince = time.Now() // 状态进驻时钟：激活停驻判据
	}
	p.mu.Unlock()

	if p.opts.Activate != nil {
		if err := p.safeActivate(hookCtx); err != nil {
			// 激活失败分支（规格 §12）：目录仲裁落败 → 在箱消息整箱转发给赢家；
			// 其余失败（含 Activate panic）→ 按 Unavailable 回绝（注册之后的
			// 失败都发生在执行之前，属确定未执行）。两者都直达终态。
			// 落败先落终局原因（ArbitrationLost+赢家，写一次）：终局翻转前后
			// 的迟到者都据此转发给赢家而非回绝——零丢失（见 finish 复扫与
			// ReceiveRequest，统一经 disposeLate 矩阵分流）。
			var lost ArbitrationLost
			if errors.As(err, &lost) {
				// NoteArbitrationLost 是输家通知的唯一实现（manager 闭包先调
				// 过则此处 terminal 写一次幂等、整箱排空扑空——防御性双入口，
				// 任何 Activate 错误实现 ArbitrationLost 都保住 pawnite 独立
				// 构造形态下的转发契约）。
				p.NoteArbitrationLost(lost.WinnerNode())
			} else {
				// 激活失败终局原因先落（ActivationFailed，写一次——非仲裁
				// 路径的显式化，迟到处置按确定未执行回绝，与下方排空行同
				// 口径），再排空在箱投递。
				p.mu.Lock()
				p.setTerminalLocked(TerminalActivationFailed, 0)
				p.mu.Unlock()
				p.drainReject(rans.CodeUnavailable, rans.ReasonActivationFailed)
			}
			// 强弃让位（stopShutdown 让位检查的对偶）：激活停驻被看门狗强弃后
			// Activate 才以错误收场——终局（OnEvict/done）已由强弃单点收口，
			// 不得再走 finish 补跑宿主 Deactivate。
			p.mu.Lock()
			abandoned := p.abandoned && p.state == StateDestroyed
			p.mu.Unlock()
			if !abandoned {
				p.finish(hookCtx)
			}
			return
		}
	}

	p.mu.Lock()
	// 条件化转移：Destroy 若在 Activate 阻塞期间已置 Destroying/Destroyed，
	// 此处不得把状态回退到 Active（状态机单调性；stop 通道照旧门控循环，
	// 行为不变，只消除公开可观测的 Destroying→Active 瞬态回退）。
	if p.state == StateActivating {
		p.state = StateActive
		p.stateSince = time.Now() // 状态进驻时钟：Active 进驻重盖
		p.activityEpoch.Add(1)    // 进入 Active 记一次活动（脏门控基准之上）
	}
	p.mu.Unlock()

	for {
		// stop 优先于 mailCh/migrateCh：select 对多个就绪分支随机选择，若不先
		// 探测 stop，排队消息可能在 Destroying 置位后仍被投递（规格 §4.2 要求
		// 销毁中回绝）。
		//
		// 残余竞态：预检返回 false 后、select 执行前
		// stop 才关闭且收件箱非空时，select 仍可能随机选中 mailCh 分支——一条已
		// 准入消息在 Destroying 期间再获一次投递机会。该残余无害：
		//   - 准入（ReceiveRequest）持 mu 查状态，而 Destroy 在同一临界区内先置
		//     Destroying 再关 stop——stop 关闭后不可能再有新消息入箱；
		//   - 残余窗口能投递的只有 Destroying 置位前已合法准入的消息（在串行
		//     loop 上按 FIFO 正常执行一次，不破坏恰好一次语义）；
		//   - stop 分支一旦胜出，drainReject 回绝余量；finish 的终局复扫
		//     （Destroyed 置位后 takeInboxLocked）兜底回绝一切滞留者。
		if p.stopping() {
			p.stopShutdown(hookCtx)
			return
		}
		select {
		case <-p.stop:
			p.stopShutdown(hookCtx)
			return
		case req := <-p.handoffCh:
			// 交接核心即 loop 任务（规格 §13.1 A：串行即静默；§5.1 H6 泛化
			// 为 Migrate/Passivate/Retire 共用骨架）。select 对 mailCh 与
			// handoffCh 的就绪分支随机选择——交接信号入队前的在箱消息可能
			// 先于 core 执行（合法），也可能在终局处置中整箱转发/回绝；
			// Go 形态偏差见 runHandoff 注释。ctx 传 hookCtx：runHandoff 把
			// 它转喂成功终局的 finish（→Deactivate 钩子）并在其上派生
			// core 的取消并集。
			if p.holdHandoffForWorks(req) {
				continue
			}
			if p.runHandoff(hookCtx, req) {
				return
			}
		case req := <-p.captureCh:
			p.runCapture(req)
		case <-p.reenterCh:
			// 续体任务（ReenterAfter 的 then）回 loop 串行执行（H12 决策 16），
			// 静默判据满足时驱动挂起的交接 core（交接窗等待释放点）。
			if fn, ok := p.popReenter(); ok {
				p.runContinuation(fn)
			}
			if req, stopBudget, ok := p.releaseHeldHandoffIfQuiesced(); ok {
				if stopBudget != nil {
					stopBudget()
				}
				if p.runHandoff(hookCtx, req) {
					return
				}
			}
		case <-p.mailCh:
			// 交接窗挂起等在途 work：等待期只处理续体任务，邮件按 H6 留箱
			//（信号回填防丢唤醒）。
			if p.holdingHandoff() {
				p.rearmMailSignal()
				continue
			}
			d, ok := p.popMail()
			if !ok {
				continue // 空信号（竞态下已被取空）：回到 select，不改变各分支随机性
			}
			p.invoke(ctx, d)
		}
	}
}

// stopShutdown 销毁分支统一收尾：抢占在飞交接（core 未跑则绝不执行、
// 请求回输输）→ 回绝余量 → 终局。
// 看门狗强弃让位（§19 陷阱 12 单写者的 loop 侧对偶）：终态与终局效应已由
// 强弃单点完成，zombie 续体（卡死的 invoke/Deactivate/交接 core 事后返回）
// 到此即止，不得再走 finish 二次收尾。
func (p *Pawnite) stopShutdown(ctx context.Context) {
	p.mu.Lock()
	abandoned := p.abandoned && p.state == StateDestroyed
	p.mu.Unlock()
	if abandoned {
		return
	}
	p.cancelWorks() // 终局收尾的 work 清场：zombie work 尽早收取消信号（协作返回，续体随终局自然落空）
	p.abortPendingHandoff()
	p.drainPendingCaptures()
	p.drainReject(rans.CodeUnavailable, rans.ReasonDeactivating)
	p.finish(ctx)
}

// stopping 非阻塞探测销毁信号（不持 mu；stop 的关闭是单次终局事件）。
func (p *Pawnite) stopping() bool {
	select {
	case <-p.stop:
		return true
	default:
		return false
	}
}

// invoke 在 loop goroutine 上执行投递；业务 panic 不允许杀死 loop
// （被 panic 的投递按 StatusError 回结对端，回绝后按 PanicPolicy 处置
// ——Default 继续循环，Isolate 就地销毁实体）。
// Reject 实现须 panic-safe（非阻塞回绝——正片实现均如此）。
func (p *Pawnite) invoke(ctx context.Context, d InboundDelivery) {
	// 出队已过期检查（H10 决策 7，本地/远端共用的投递执行入口）：入箱后
	// 睡过预算的消息不执行、按 (CodeTimeout, ReasonExpired) 回绝并触发
	// OnExpired 计数——实体侧零副作用。先于执行簿记：弃置不计为投递活动。
	if ex, ok := d.(Expirable); ok {
		if dl, has := ex.DeliveryDeadline(); has && time.Now().After(dl) {
			if p.opts.OnExpired != nil {
				p.opts.OnExpired()
			}
			d.Reject(rans.CodeTimeout, rans.ReasonExpired)
			return
		}
	}
	// 出队已取消检查（H10 决策 9，本地/远端共用的投递执行入口）：在箱被
	// MsgCancel 标记的投递不执行、按 CodeCanceled 回绝并触发 OnCanceled
	// 计数（本地投递不实现 CancelAware 即零开销跳过）。
	if ca, ok := d.(CancelAware); ok && ca.DeliveryCanceled() {
		p.NoteCanceled()
		d.Reject(rans.CodeCanceled, rans.ReasonNone)
		return
	}
	now := time.Now().UnixNano()
	p.lastDeliveryNano.Store(now)
	p.activityEpoch.Add(1)
	// 执行停驻时钟 + 在途投递槽（看门狗强弃的解绑目标，规格 §14.2）：
	// invoke 入口置位、出口清零（panic 路径经 defer 同样清零）。
	p.executingSinceNano.Store(now)
	p.executing.Store(&d)
	defer func() {
		p.executing.Store(nil)
		p.executingSinceNano.Store(0)
		if r := recover(); r != nil {
			log.Warn("pawnite delivery panicked, rejecting",
				"uuid", p.id.String(), "panic", r, "policy", p.opts.PanicPolicy)
			d.Reject(rans.CodeError, rans.ReasonHandler)
			if p.opts.PanicPolicy == PanicPolicyIsolate {
				// 隔离策略：回结对端后就地销毁。本 defer
				// 运行在 loop goroutine 上——只置 Destroying + 关 stop，
				// 终局收尾交还 run 循环的 stop 分支（不得在此等 done）。
				p.isolateAfterPanic()
			}
		}
	}()
	d.Deliver(p)
}

// NoteCanceled 在箱取消弃置事件钩子的触发单点（invoke 出队检查与投递执行
// 内的竞态复查共用；loop 上执行，必须非阻塞——与 OnExpired 同纪律）。
func (p *Pawnite) NoteCanceled() {
	if p.opts.OnCanceled != nil {
		p.opts.OnCanceled()
	}
}

// isolateAfterPanic 隔离销毁的发起段（仅 loop goroutine 调用，invoke 的
// panic 分支）：置 Destroying + 关 stop，随后 run 循环的下一轮迭代经
// stopShutdown 完成统一收尾（drainReject 余量 → finish：Deactivate →
// Destroyed → OnEvict → closeDone）。与外部 Destroy 的竞态在 gate 内
// 让位（已 Destroying/Destroyed 时本方法空转，终局归先手路径）。
func (p *Pawnite) isolateAfterPanic() {
	p.mu.Lock()
	if p.state == StateDestroying || p.state == StateDestroyed {
		p.mu.Unlock()
		return // 并发销毁已抢窗：终局由先手路径完成
	}
	p.enterDestroyingLocked()
	p.stopClosed = true
	p.mu.Unlock()
	close(p.stop) // 锁外关（与 Destroy 同纪律）；run 循环即走 stop 分支
}

// ExecutingSince 当前在途投递的执行起点（规格 §14.2 执行看门狗依据：
// _activeTopLevel 停驻时钟的 Go 形态——invoke 起 atomic 置位、出口清零）。
// 零值 = 空闲（无顶层执行在途）。读取无锁。
func (p *Pawnite) ExecutingSince() time.Time {
	n := p.executingSinceNano.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// LastDeliveryTime 最近一次投递处理时刻（规格 §4.1 _lastDeliveryTickCount
// 闲置时钟——G1 回收/E2 再平衡的冷热判据）。零值 = 从未投递。
// invoke 入口原子更新，读取无锁。
func (p *Pawnite) LastDeliveryTime() time.Time {
	n := p.lastDeliveryNano.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// waitDone 销毁等待段：终态先落返回 nil；ctx 先耗尽返回 ctx.Err()（销毁
// 推进不归本方法管——loop 自驱，调用方重试即 Destroy 再进等待段）。
func (p *Pawnite) waitDone(ctx context.Context) error {
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pawnite) Done() <-chan struct{} { return p.done }
