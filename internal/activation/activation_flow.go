package activation

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/ctxkey"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// cleanupUnregisterTimeout 是终局 cleanup 里目录注销的超时预算：cleanup
// 会在看门狗扫描 goroutine 上同步执行，
// 无界注销（Background）会被慢目录冻结整个扫描环。5s 级——goredis 自带
// 拨号/读超时兜底，这里显式收口；注销失败本就丢弃（残留由下次激活收养），
// 预算耗尽同语义。
const cleanupUnregisterTimeout = 5 * time.Second

// GetOrActivate：本地快查 → creationLock 双检 → 建 Pawnite → register-first
// 激活（目录注册取号 + 实体 Activate 都在 Pawnite 队列语境内完成，规格
// §12 register-first：注册先于装载，输家零用户代码）。
// 生命周期钩子（onActivated/onEvicted）全程在 表锁外触发：复检与
// 双检的 cleanup 都在释放 m.mu 之后执行——onEvicted 重入本方法/Destroy
// 不会死锁（onActivated 内对同一 id 调 Destroy 会自等待 Pawnite 串行
// 队列，见字段注释的重入边界）。
func (m *Table) GetOrActivate(ctx context.Context, id rans.ID) (*pawnite.Pawnite, error) {
	if p, ok := m.GetLocal(id); ok {
		return p, nil
	}
	m.mu.Lock()
	for {
		// 双检（只看不清理，见 peekLocal）：命中活实例即复用；命中陈旧
		// Destroyed 条目则放锁做本地逐出（不等终局注销——异步化语义与
		// GetLocal 同口径）后重试创建，目录串行化约束已由实例级标记取代。
		if inst := m.peekLocal(id); inst != nil {
			if inst.p.State() != pawnite.StateDestroyed {
				m.mu.Unlock()
				return inst.p, nil
			}
			m.mu.Unlock()
			m.evict(id, inst.p)
			m.mu.Lock()
			continue
		}
		f, ok := m.kinds[id.Kind]
		if !ok {
			m.mu.Unlock()
			return nil, ErrUnknownKind
		}
		// 工厂出锁（吞吐项，与取号出锁同族）：构造用户实体是用户代码
		//（可能重、可能 panic、可能被竞争丢弃），在 m.mu 临界区外执行；
		// 重锁后双检——命中既有条目即回到环首判定（活实例复用返回、陈旧
		// 清理后重试，构造物按工厂契约无副作用、直接丢弃）。工厂 panic
		// 时无锁在手，向上传播即无死锁面。
		m.mu.Unlock()
		ent := f(id)
		m.mu.Lock()
		if m.peekLocal(id) != nil {
			continue // 双检命中：既有实例接管，构造物让位（环首同款判定）
		}
		// 创建段闭包化：defer 接管本段解锁——临界区内代码
		// panic 经 defer 释放 m.mu 后继续向上传播（编程错误不吞，但爆炸
		// 半径不得从单个实体扩大为全表死锁）。复检段保持在锁外执行
		//（cleanup 含目录 IO 与 onEvicted 钩子，持 m.mu 调用会死锁）。
		var (
			p       *pawnite.Pawnite
			actErr  error
			cleanup func(*pawnite.Pawnite)
		)
		func() {
			defer m.mu.Unlock()

			// actErr 捕获激活失败原因：仅 run goroutine 写（必在终态置位前），
			// 复检命中 Destroyed 后才读取——p.mu 的状态转移建立 happens-before，
			// 无数据竞态。仲裁落败时调用方据此可 errors.As 取出赢家做重定向。

			// cleanup 本实例的终局清理：续租让出 → 目录注销严格一次
			//（sync.Once）且先于任何本地逐出 → onEvicted → arbLost。
			//
			// 异步化：本地表 miss 不承诺「本实例注销已完成」——
			// 终局由 pawnite 自驱（finish/强弃单点跑 OnEvict 进入本闭包），
			// 读路径（GetLocal/环首双检）遇陈旧条目只做本地逐出、不等本
			// 闭包。同节点新旧实例因此在目录上不再串行化；交错安全由实例
			// 级标记承载：注销按 {node, inc} 同时匹配才删——即使交错下旧
			// 实例的迟到注销遇到新实例条目，标记不符也是 no-op，不会留下
			// 活实例无目录条目（静默路由丢失 + 脑裂窗）；新注册侧的收养/
			// 换锁升纪元（Register 原子域）保证旧条目在场时新实例仍能落位。
			//
			// 注销形态：owner 已捕获（注册完成过）⟹ 实例级 UnregisterIfOwner
			//（Owner{node, inc} 同时匹配才删）；未捕获（输家/注册失败/未起步
			// ——本实例无目录主张）⟹ **零目录调用**（INV3 纪元序=所有权序：
			// 节点粒度匹配会误删同 node 新实例的条目）。残留自愈由两道既有
			// 机制承担：下次激活的 Register 自指向收养（升纪元覆写——条目指向
			// 本节点即注册目标状态）+ 租约 TTL（未纳入续租追踪的条目 ≤5min
			// 过期腾位）。
			// 迁移赢家例外（规格 §13.2 换锁不变量）：
			// 目录所有权已随 CAS 让渡给目标节点，注销会误删新家的注册——经
			// pawnite 的 SkipDirUnregister 查询面跳过注销；本地逐出与钩子
			// 照常（OnEvict 回调把到达终态的 pawnite 传入；陈旧分支的调用方
			// 传各自持有的实例引用）。
			//
			// 注销 ctx 带超时预算（见 cleanupUnregisterTimeout）：cleanup 会在看门狗
			// 扫描 goroutine 上同步执行（AbandonStuck* 的终局路径），Background
			// 无界的慢目录会冻结整个扫描环——goredis 自带拨号/读超时使冻结
			// 有界，这里再显式收口为 5s 级预算；注销失败本就丢弃（残留条目
			// 由下次激活收养，条目已指向本节点即注册目标状态），预算耗尽同
			// 语义。evict 幂等重放保留：覆盖「OnEvict 在入表前跑完 cleanup、
			// 死实例事后才入表」的交错（复检与陈旧路径再次逐出晚入表条目，
			// 维持 ErrActivationFailed 的同步移除契约）；身份守卫保证重放不
			// 会误删后来者。
			// onEvicted 钩子在 Do 内注销+逐出之后触发，故 cleanup 整体不得
			// 在持 m.mu 时调用（见 GetOrActivate 各调用点）。
			var (
				cleanupOnce sync.Once
				// arbLost 仲裁落败赢家槽：activate 闭包的
				// 输家分支（Register 原子域裁定落败）写入（run goroutine，
				// 必在终态置位前）；cleanup 在 onEvicted 之后据此触发
				// onArbitrationLost。次序约束：输家钩子若在注册路径内直发，
				// 会先于本实例的 onEvicted（Invalidate）落缓存又被后者清掉——
				// PutPositive(赢家) 必须排在逐出失效之后，赢家条目才留得住。
				// 可见性同 actErr 契约：终态转移建立 happens-before。
				arbLost *directory.Owner
				// activated 配对标志：激活整体成功（目录注册与实体 Activate
				// 都成功）置位——与 onActivated 同点位，由 pawnite run
				// goroutine 在终态前置位。Deactivate 包装层据此决定是否调
				// 用户钩子：注册失败、恢复失败、Activate 失败、仲裁落败的
				// 实例都不调，维持「用户 Deactivate 调用次数 = 成功激活次数」
				// 的实例级配对。原子量：终局读取可能发生在别的 goroutine。
				activated atomic.Bool
			)
			// inst 声明先于 cleanup/activate 闭包：activate 在 run goroutine
			// 上据 restoreState 的恢复来源（restoreStore / restorePackage 落库
			// 成功）标记 ckptEpoch、在赢得注册后首写
			// owner；cleanup 据 owner 做实例级条件注销与续租让出。构造点在
			// pawnite.New 之后（需要 p）、p.Start 之前——赋值先于 Start（程序
			// 序），闭包运行时 inst 必非 nil；owner/ckptEpoch 是原子量，
			// leaseTracked/leaseClosed 由 leaseMu 串行化，与 Start 之后的
			// 入表/扫轮/终局读取无竞态。
			var inst *instance

			cleanup = func(ep *pawnite.Pawnite) {
				cleanupOnce.Do(func() {
					if !ep.SkipDirUnregister() {
						if owner := inst.owner.Load(); owner != nil {
							// owner 已捕获：实例级条件注销（预算 ctx）
							regCtx, regCancel := context.WithTimeout(context.Background(), cleanupUnregisterTimeout) // 例外②：终局后的条件注销（预算集中定义，终局收尾不能被无界目录 IO 挂死）
							_, _ = m.dir.UnregisterIfOwner(regCtx, id, *owner)
							regCancel()
						}
						// owner 未捕获：零目录调用（回落面已删除，残留由下次
						// 激活收养 + 租约 TTL 自愈——见上方注销形态注释）。
					}
					m.evict(id, p)
					// onEvicted 钩子：目录注销+本地逐出都完成后严格一次触发。
					// 全部调用点（OnEvict / 让位守卫后的终局路径）都在表
					// 锁之外——钩子重入 GetOrActivate/Destroy 不会死锁（续租
					// Untrack、路由缓存失效等接线点都依赖此前提）。
					// 交错容忍：旧实例 cleanup 的
					// fireOnEvicted(id) 可晚于同 id 新实例的激活——rpc 的
					// id-only 缓存失效会空打一次活实例的路由，下次目录查找
					// 自愈（poller 同类失效），无正确性影响。
					m.fireOnEvicted(id)
					// 仲裁落败钩子（若本次终局是输家）：在 onEvicted 之后触发
					// （次序约束见 arbLost 字段注释）。
					if arbLost != nil {
						m.fireOnArbitrationLost(id, *arbLost)
					}
				})
				m.evict(id, p)
			}

			// activate 闭包的 ctx 形参来自 p.Start 传入的 actCtx（pawnite.run 原样
			// 转交），激活链路全部消费解耦后的激活 ctx。
			activate := func(ctx context.Context, p *pawnite.Pawnite) error {
				// register-first（规格 INV3）：先认领再装载。注册与取号
				// 同一原子域——令牌只发给注册成功的 owner，仲裁输家零用户代码（输家钩子
				// ctx 从不携带令牌）。注册是一次目录 IO，放在闭包首步即移出
				// 全局创建锁——在各 pawnite 的 run goroutine 上并行，K 个并发
				// 激活不被串行成 K×RTT。注册失败（IO 错误）按激活失败收场：
				// 此刻消息已入箱，经 drainReject(StatusUnavailable,
				// ReasonActivationFailed) 回绝（注册后失败属执行前确定性失败），
				// 本地调用方拿到 ErrActivationFailed 后可重试——不同步阻断
				// pawnite 构造。
				won, cur, err := m.dir.Register(ctx, id,
					directory.Owner{Node: m.self, Epoch: m.SelfEpoch()}, nil)
				if err != nil {
					actErr = fmt.Errorf("register: %w", err)
					m.observeRegisterFailure(id, err)
					return actErr
				}
				if !won && !directory.OwnerAlive(ctx, m.dir, cur) {
					// 死 owner 接管（H13 决策 21）：携 expected 做 CAS 接管
					//（误判由 inc 围栏 + SelfFence 兜底）；仍输按输家收场。
					prev := cur
					won, cur, err = m.dir.Register(ctx, id,
						directory.Owner{Node: m.self, Epoch: m.SelfEpoch()}, &prev)
					if err != nil {
						actErr = fmt.Errorf("register takeover: %w", err)
						m.observeRegisterFailure(id, err)
						return actErr
					}
					if won {
						// 接管赢方直发所有权丢失通知（H13 决策 22）。
						m.notifyTakeover(prev, id)
					}
				}
				if !won {
					// 输家：pawnite 输家通知面置 terminal{ArbitrationLost,
					// winner} 并整箱转发在箱投递（幂等；run 激活失败分支的
					// errors.As 探测是防御性双入口）；arbLost 槽供 cleanup
					// 逐出后的 onArbitrationLost 钩子（次序约束见字段注释）。
					p.NoteArbitrationLost(cur.Node)
					winner := cur
					arbLost = &winner
					actErr = &ArbitrationLostError{Winner: winner}
					return actErr
				}
				owner := cur // 赢：current 即含新纪元的自归属（epoch/inc 采信目录返回值）
				inc := cur.Incarnation
				inst.owner.Store(&owner) // 实例标记首写：终局 cleanup 据此实例级注销（先于下方守卫，覆盖注册返回与守卫之间的强弃交错）
				p.AssignIncarnation(inc) // 令牌落位（投递层经 p.Incarnation() 包装 Handle ctx；Register 取号恒 ≥1，0=未围栏哨兵不占）
				ctx = ctxkey.WithIncarnation(ctx, inc)
				// 注册后让位守卫：注册 IO 在飞期间看门狗强弃（StateDestroyed）
				// 或 Destroy 抢先置 Destroying——实例已死，刚赢得的注册是幽灵
				// 条目（无本地实例却钉死归属），就地实例级注销后让位返回 nil
				//（终局已由强弃/销毁路径收口，不走 finish 补跑宿主 Deactivate）。
				if p.State() != pawnite.StateActivating {
					m.unregisterInstance(id, owner)
					return nil
				}
				// 装载（规格 §13.3「实体先就绪」）：先恢复状态再 Activate——
				// 实体 Activate 观测到的即恢复后的状态。恢复梯子：随迁包快路径
				// → Store 正确性依赖 → 未命中裸激活（见 restoreState）。Store
				// 读失败即激活失败：绝不以空状态对外服务——actErr 记根因，
				// 复检同步窗的错误链保留 ErrRestoreFailed；在箱投递的异步窗
				// 只携带状态码与 reason 字符串（drainReject 形态），根因不跨
				// 投递面。刚赢得的条目由此失败的终局 cleanup（OnEvict）以
				// 实例标记注销（严格一次），本闭包不重复注销。
				src, snap, rerr := m.restoreState(ctx, id, ent)
				if rerr != nil {
					actErr = rerr
					m.observeRestoreFailure(id, rerr)
					return rerr
				}
				switch src {
				case restoreStore:
					// 从 Store 恢复的状态已持久：把 ckptEpoch 抬到进入 Active 时的活动纪元
					//（恢复发生在 Active 转移之前，此刻 Epoch()==0，+1 即转移后的 1），
					// 首轮 sweep 判干净，不把刚读出的状态原样写回。
					inst.ckptEpoch.Store(p.Epoch() + 1)
				case restorePackage:
					// 恢复即落库（spec §5.2 第 6 点）：包可能比 Store 新——在
					// Activate 之前以本实例新纪元把包字节原样落库一次，Store 的
					// per-id 高水位即刻越过源端旧纪元，源端迟到写入从这一次落库
					// 起被 ErrStaleIncarnation 拒（否则围栏窗要敞到首轮周期
					// checkpoint）。成功即「当前状态已持久」（此刻实体状态=包字节
					// =落库字节，恢复后 Activate 前无任何执行）：ckptEpoch 同款
					// 抬升，首轮 sweep 判干净，省一次同内容重写；失败仅 Warn 不
					// 阻断激活（尽力而为档）——不标记，首轮 sweep 的 Put 落库
					// 时围栏窗才收口。
					if m.store != nil {
						if perr := m.store.Put(ctx, id, p.Incarnation(), snap); perr != nil {
							log.Warn("rans/activation: post-restore snapshot persist failed; stale-write fencing deferred to first checkpoint (best effort)",
								"uuid", id.String(), "err", perr)
						} else {
							inst.ckptEpoch.Store(p.Epoch() + 1)
						}
					}
				}
				if aerr := ent.Activate(ctx); aerr != nil {
					actErr = aerr
					return aerr
				}
				// 强弃/销毁让位守卫（register-first 形态）：
				// Activate 迟回时实体可能已被激活看门狗强弃（StateDestroyed）
				// 或被 Destroy 抢先置 Destroying——激活整体未对外成功：不置
				// 配对标志、不补发 onActivated（onEvicted 已随强弃/销毁消费，
				// 钩子时序不得倒置）；条目注销归终局 cleanup（强弃路径的
				// OnEvict 已带实例标记执行；Destroy 抢先路径由本返回后的
				// stop 分支收尾）。让位返回 nil：zombie loop 经 stop 分支自然
				// 退出。
				if p.State() != pawnite.StateActivating {
					return nil
				}
				// 配对标志在激活整体成功时置位：注册失败、恢复失败、Activate
				// 失败、仲裁落败的实例从未对外激活成功——不置位，其终局不跑
				// 用户 Deactivate。
				activated.Store(true)
				// onActivated 钩子：目录注册与实体 Activate 都成功之后触发；
				// 运行在 Pawnite run goroutine 上，不持任何表锁。
				// （续租不在此接线：Track 前移到注册成功点
				// trackLease——装载期也在续租域内；本钩子是纯用户通知面。）
				m.fireOnActivated(id)
				return nil
			}

			// actCtx：激活预算与调用方 ctx 解耦。激活是节点级行为（目录注册+实体加载）而非
			// 请求级行为——首调用者取消不得中止在途激活，否则其余调用者会反复撞上
			// 半激活状态。WithoutCancel 保留 ctx 携带的值（链路追踪等），仅剥离取消/
			// 超时；目录注册、实体 Activate 与 pawnite.Deactivate（finish 消费同一
			// run ctx）全部使用 actCtx——Deactivate 因此同样不受调用方取消影响，
			// 保持既有语义（销毁收尾不因调用方取消而中断）。fencing 挂值不在此处：
			// 纪元在激活闭包首步的注册原子域才取到（见 activate 注释），闭包内对
			// 形参 ctx 挂值。
			actCtx := ctxkey.WithSelf(context.WithoutCancel(ctx), id.Kind, id.Key)
			p = pawnite.New(id, pawnite.Options{
				Activate: activate,
				Deactivate: func(ctx context.Context, p *pawnite.Pawnite) error {
					// 配对约束：未成功激活的实例（注册失败 / 恢复失败 /
					// Activate 失败 / 仲裁落败）不调用户 Deactivate——Deactivate
					// 只与成功的激活配对。
					if !activated.Load() {
						return nil
					}
					// activated 已置位 ⟹ 注册早已完成（闭包首步），AssignIncarnation
					// 紧随其后落位，此处读到的必是本 incarnation 值。finish 透传
					// 的 hookCtx 不含挂值（纪元挂值只包激活闭包内的 ctx
					// 形参），按已落位纪元补挂——Deactivate 钩子与 Activate
					// 钩子读到同一纪元（实例级围栏配对）。
					return ent.Deactivate(ctxkey.WithSelf(ctxkey.WithIncarnation(ctx, p.Incarnation()), id.Kind, id.Key))
				},
				// 逐出+目录注销统一走 cleanup（注销先于逐出，见其注释；迁移
				// 赢家经 pawnite 标志跳过注销——cleanup 内接线）。
				OnEvict: func(ep *pawnite.Pawnite) { cleanup(ep) },
				// 出队过期即弃计数钩子（H10 决策 7；nil = 未接线不观测）。
				OnExpired: m.onExpired,
				// 出队取消弃置计数钩子（H10 决策 9；nil = 未接线不观测）。
				OnCanceled: m.onCanceled,
				// fencing 纪元不经 Options 注入：激活闭包首步的注册原子域
				// 取号后经 AssignIncarnation 落位（Options.Incarnation 保留给
				// 直接构造 pawnite 的用法）；checkpoint/钝化/迁移的 Store.Put
				// 与宿主 Handle ctx 都以落位值为准。
			})
			inst = &instance{p: p, cleanup: cleanup}
			p.Start(actCtx) // 先 Start 再入表（守卫兜底 + 收窄窗口）
			m.readMu.Lock()
			m.local[id] = inst
			m.entity[id] = ent
			m.readMu.Unlock()
		}()
		// 入表后复检终态。激活失败时 OnEvict 可能在入表前已跑完
		// （其对未入表条目是 no-op），若不复检会把 Destroyed 死 pawnite 永久缓存在
		// local——快路径永远返回它且 err=nil，Destroy 对它也无事可做。
		if p.State() == pawnite.StateDestroyed {
			// 身份守卫逐出晚入表的死条目（幂等；OnEvict 的 cleanup 可能已
			// 抢先做过）。目录注销不等：终态已翻转 ⟹ OnEvict→cleanup 已由
			// pawnite 自驱跑毕或在跑（异步化语义，见 cleanup 注释）。
			m.evict(id, p)
			if actErr != nil {
				// 错误链同时保留 ErrActivationFailed 哨兵语义（旧调用方 errors.Is
				// 兼容）与激活根因——仲裁落败时根因是 *ArbitrationLostError，
				// 调用方 errors.As 可取赢家位置。
				return nil, fmt.Errorf("%w: %w", ErrActivationFailed, actErr)
			}
			return nil, ErrActivationFailed
		}
		// 激活在途（Created/Activating）或已成功（Active）或与外部 Destroy 竞态
		// （Destroying）：维持异步语义；失败路径的终局清理由 OnEvict 身份守卫逐出闭环。
		return p, nil
	}
}

// unregisterInstance 实例级条件注销（register-first）：注册已先行、
// 让位守卫裁定实例已死（注册 IO 在飞期间被强弃/销毁抢占）时，把刚赢得的
// 目录条目就地让出——仅当条目仍属本实例（node+inc 同时匹配）时删除，同
// 节点新实例的条目不被误删。预算与终局 cleanup 同源（cleanupUnregister-
// Timeout）：本方法运行在 pawnite run goroutine 上，无界目录 IO 会挂起
// 终局收尾。注销失败仅留痕丢弃（残留条目由下次激活的同节点收养/租约过期
// 自愈）。
func (m *Table) unregisterInstance(id rans.ID, owner directory.Owner) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupUnregisterTimeout) // 例外②：让渡注册的就地条件注销（同 cleanup 预算源——无界目录 IO 会挂终局）
	defer cancel()
	if _, err := m.dir.UnregisterIfOwner(ctx, id, owner); err != nil {
		log.Warn("rans/activation: instance unregister after yielded registration failed (residue self-heals via next activation)",
			"uuid", id.String(), "err", err)
	}
}
