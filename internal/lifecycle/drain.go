// Package lifecycle 实现节点生命周期编排（规格 §14）：排空（§14.1，本
// 文件）、看门狗（§14.2，watchdog.go）、再平衡（§14.3，rebalancer.go）、
// 闲置钝化（passivator.go）、周期任务（sweeper.go）与交接窗终局编排
// （迁移/钝化/停机 core——migrate.go/passivator.go/drain.go）。编排经 *activation.Table 协作（实例查
// 询/纪元等待等内部导出协作面），实例级注销/终局处置机制仍归 activation。
//
// 本文件是 §14.1 四段排空的通用编排器：全部阶段面以回调注入，零运行时
// 依赖——rpc（Remote.Shutdown 装配）、testkit（Cluster.Shutdown）与宿主
// 进程各自组装，语义唯一出处 docs/porting/ARCHITECTURE.zh-CN.md §14.1。
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// DefaultFlushWindow 出站 flush 缺省有界窗口（规格附录 A：500ms）。
const DefaultFlushWindow = 500 * time.Millisecond

// Drainer 四段排空编排器（规格 §14.1）：
//
//	① BeginDraining——消息面封锁（dispatcher 门闸）
//	② 并行销毁全部枚举实体（信号量限流，单实体失败收集不阻断）
//	③ 有界窗口出站 flush
//	④ 停序回调按登记序全数执行
//
// 全字段可注入：nil 段跳过、零值参数回落缺省；编排器不持有任何运行时
// 组件，重入安全（同一 Drainer 顺序复用无内部状态残留）。
type Drainer struct {
	// BeginDraining 阶段①：消息面封锁（dispatcher 门闸）。nil → 跳过。
	BeginDraining func()
	// Entities 阶段②枚举面：返回全部待排空实体身份。nil → 跳过阶段②。
	Entities func() []rans.ID
	// Destroy 阶段②逐实体销毁（Retire 形态：
	// 终快照生效 + 条件注销由实现自带——DestroyLocal 即交接窗复合体）。
	// 错误被收集、不阻断其余实体（单实体失败 ≠ 节点停机失败）。nil →
	// 枚举结果被丢弃（阶段②空转）。逐实体注入形态：Shutdown（下）是
	// 阶段②的同语义复合形态（快照 + 并行 Retire + Join 汇总 + 空表校验，
	// 并行度同 max(8, CPU×8)）——需要门闸/flush/停序的四段编排用本注入面，
	// 宿主只收尾本地实体可直接用复合形态。
	Destroy func(ctx context.Context, id rans.ID) error
	// Parallelism 阶段②并发上限；≤0 → max(8, runtime.NumCPU()*8)
	//（规格附录 A 公式）。
	Parallelism int
	// Flush 阶段③出站 flush：以 FlushWindow 有界的 ctx 调用；窗口耗尽时
	// 编排器弃等继续（goroutine 留给实现自行收场——编排不强行取消阻塞中
	// 的系统调用）。nil → 跳过。
	Flush func(ctx context.Context)
	// FlushWindow 阶段③窗口；≤0 → DefaultFlushWindow。
	FlushWindow time.Duration
	// Teardown 阶段④停序回调：按登记序全部执行，不因任何回调错误而中断
	//（兜底清扫顺序必须完整——规格 §14.1 ④）；返回的错误收集并入返回值
	//（宿主停序头的等待错误由此上浮）。
	Teardown []func() error
}

// Drain 执行四段排空。返回 errors.Join 的错误面：阶段②逐实体销毁错误、
// 预算耗尽（阶段②/③后 ctx 已死——包装 ctx.Err()，spec §5.1 第 1 条；排空
// 不回滚，停序照常执行，调用方据此得知未在预算内走完）、阶段④停序回调
// 错误；无失败返回 nil。
//
// ctx 是阶段②/③的预算而非总开关：排空是终态动作，ctx 耗尽不回滚已排空
// 进度，阶段④无条件执行（停机路径上的停序回调不能因预算枯竭被跳过）。
func (d *Drainer) Drain(ctx context.Context) error {
	if d.BeginDraining != nil {
		d.BeginDraining() // 阶段①：门闸先于任何销毁（时序由本方法结构保证）
	}
	errs := d.destroyAll(ctx) // 阶段②
	d.flush(ctx)              // 阶段③
	// 预算耗尽面：阶段②③在预算内推进，ctx 已死即如实
	// 上报——零实体或全部瞬时完成的排空同样不静默（否则「预算内完成」与
	// 「预算耗尽硬闯」对调用方不可分辨）。
	if cerr := ctx.Err(); cerr != nil {
		errs = append(errs, fmt.Errorf("drain: shutdown budget exhausted: %w", cerr))
	}
	for _, fn := range d.Teardown { // 阶段④：错误收集不中断停序
		if err := fn(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// destroyAll 阶段②：枚举实体按 Parallelism 信号量并行销毁。主循环侧获取
// 槽位（发车即限流——在途 Destroy 数 ≤ Parallelism 的结构保证）；单实体
// 错误锁内收集，全部收场（wg.Wait）后才进入下一段。
func (d *Drainer) destroyAll(ctx context.Context) []error {
	if d.Entities == nil || d.Destroy == nil {
		return nil
	}
	ids := d.Entities()
	if len(ids) == 0 {
		return nil
	}
	par := d.Parallelism
	if par <= 0 {
		par = runtime.NumCPU() * 8
		if par < 8 {
			par = 8
		}
	}
	sem := make(chan struct{}, par)
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, id := range ids {
		wg.Add(1)
		sem <- struct{}{}
		go func(id rans.ID) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := d.Destroy(ctx, id); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("drain: destroy %s: %w", id.String(), err))
				mu.Unlock()
			}
		}(id)
	}
	wg.Wait()
	return errs
}

// flush 阶段③：以 WithTimeout(ctx, FlushWindow) 的有界 ctx 运行 Flush。
// Flush 阻塞不归时在窗口边界弃等继续阶段④（进程停机不因单个 flush 挂死
// 而中断——有界窗口即解耦点）；Flush 服从 ctx 则自然早退。
func (d *Drainer) flush(ctx context.Context) {
	if d.Flush == nil {
		return
	}
	window := d.FlushWindow
	if window <= 0 {
		window = DefaultFlushWindow
	}
	fctx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.Flush(fctx)
	}()
	select {
	case <-done:
	case <-fctx.Done():
	}
}

// ---- 实例终局编排（经 *activation.Table 协作）----
//
// 交接窗三种 core 的钝化/停机半边（《加固 spec》§5.1 第 3 点 core 矩阵 + 第 4 点
// 终局次序）。Passivate/Retire 与 Migrate（migrate.go）共用
// pawnite.Handoff 骨架——窗内准入照收、消息留箱等待；core 是 loop 任务
//（静默点，无在途 Handle），首步统一 AwaitCheckpointSettled（等在途周期
// checkpoint 落定，受窗预算约束——终快照之后不会再有更旧的同纪元快照落库），
// 差异只在快照落库策略与失败语义：
//
//	core      | 有状态且配置 Store                                | 有状态但未配置 Store   | 无状态
//	----------|---------------------------------------------------|------------------------|-------
//	Migrate   | 快照→Put→随迁包→换锁；Put 失败判输                | 拒绝迁移               | 直接换锁
//	Passivate | 快照→Put；失败中止（回退 Active，下轮再试）       | 拒绝钝化               | 直接成功
//	Retire    | 快照→Put 预算内重试；仍失败记 Error+指标照样终局  | 不写快照+启动告警一次  | 直接成功
//
// 「有状态」= 实体实现 rans.Snapshotter（快照能力面）；快照捕获失败一律按
// Put 失败处理。快照先于用户 Deactivate：Snapshot 在 core 内
// 先拍（core 即 loop 任务，快照一致），Deactivate 对实体状态的修改不进快照
//（godoc 契约，规格 §5.1 第 5 点）。Passivate/Retire 成功后的终局次序
//（Deactivate → 实例级条件注销 → 在箱 Unavailable 回绝 → Destroyed）由
// pawnite 的 finishRetiredInOrder 承载。
//
// 调用面说明：Passivate（passivator.go）是钝化的
// 公共导出面；DestroyLocal/DestroyInstance/Shutdown 三类销毁面一律经
// destroyRetire 走 Retire 交接窗（《加固 spec》§5.1 第 7 点——终快照生效）；
// retireInstance 是包内驱动面（测试与未来编排消费）。实例级注销/终局处置
// 机制（条件注销、cleanup 闭包、身份守卫逐出）留在 activation（Table）。

// retirePutAttempts Retire 终快照落库的预算内尝试次数（含首次；spec §5.1
// 第 3 点表缺省 3 次）。仅 Retire 重试——迁移/钝化的 Put 失败立即判输/中止
// （两者都可安全重试，停机是单次终局动作）。
const retirePutAttempts = 3

// retirePutBackoff Retire 相邻两次重试的线性退避基数：第 n 次重试前等待
// n*retirePutBackoff（总追加 ≤ 2*retirePutBackoff；预算 ctx 可中断）。
const retirePutBackoff = 50 * time.Millisecond

// checkpointFinalFailedCounter 懒建的终快照失败计数器（metrics.Default 进程
// 级共享注册表，同名幂等注册——与 carrier.go 的激活失败计数器同形态）。
var checkpointFinalFailedCounter = sync.OnceValue(func() *metrics.Counter {
	return metrics.Default().Counter(metrics.NameCheckpointFinalFailed,
		"final snapshot persist failed at retire after budgeted retries (terminal proceeds; incremental writes since last checkpoint lost)")
})

// retireInstance 优雅退役本地实体（《加固 spec》§5.1 的 Retire 形态）：开
// HandoffRetire 窗并以 retireCore 裁决。core 内部遵循「停机不能被 Store
// 故障挂死」：终快照预算内重试、仍失败记 Error+指标后照样终局（返回
// nil）；目标不在册、预算耗尽于等待段等前置失败如实返回错误（窗回退
// Active，调用方可重试或由看门狗强弃兜底——强弃本就不写快照）。公共
// 销毁面（DestroyLocal 族）经 destroyRetire 消费同一 core。
func retireInstance(ctx context.Context, tbl *activation.Table, id rans.ID) error {
	p, ent, err := handoffTarget(tbl, id)
	if err != nil {
		return err
	}
	return retireWindow(ctx, tbl, id, ent, p)
}

// retireWindow 开 Retire 交接窗并等待裁决（retireInstance 与 destroyRetire
// 的共同开窗形态；core 即 retireCore）。
func retireWindow(ctx context.Context, tbl *activation.Table, id rans.ID, ent rans.Entity, p *pawnite.Pawnite) error {
	return p.Handoff(ctx, pawnite.HandoffRetire, 0, func(cctx context.Context) pawnite.HandoffOutcome {
		return retireCore(cctx, tbl, id, ent, p)
	})
}

// DestroyLocal 本地实例销毁（内部收尾面：管理操作以
// node.Destroy/Migrate 为唯一消费方入口，非本地目标在 node 层回
// NotFound+ReasonNotLocal；本面只服务仓内编排方：node.Destroy、
// rpc.Remote.Shutdown 阶段②）。
// 销毁并等待终态的 ctx-honoring 形态：走 Retire 交接窗（destroyRetire）
// ——有状态实体先等在飞
// 周期 checkpoint 落定再拍终快照落 Store（《加固 spec》§5.1 第 7 点：终快照
// 生效），成功后补等终态关闭维持「销毁并等待终态」契约。
// 组合规则：撞上进行中的 Passivate/Migrate 窗时等待在飞结果而非抢占
// （成功 → 视为已完成；失败 → 窗回退后接棒执行 Retire）。例外面（非
// Active：激活中/他方 stop 销毁或强弃在飞/已终局）回既有 stop 面收口
// ——无快照直达终局（激活未完成无确认状态）。
// ctx 是等待预算，耗尽返回 ctx.Err()（终态推进由 loop 自驱，卡死
// Deactivate 交看门狗/自身设备兜底，规格 §14.1/§14.2），调用方可凭新
// ctx 重试。不在册时 no-op 返回 nil。
// 本面是「任意世代」形态：不校验纪元，在册即销毁（排空/停机面语义——
// Drainer 阶段②（Remote.Shutdown）与 Shutdown 把排空预算贯通到逐实体等待段，
// 单实体 Deactivate 卡死只在预算内挂住，不再挂死整个优雅停机；租约失守的
// 止损销毁走实例级的 DestroyInstance，换代交错下不误伤新实例）。
func DestroyLocal(ctx context.Context, tbl *activation.Table, id rans.ID) error {
	p, ok := tbl.PeekLocal(id)
	if !ok {
		return nil
	}
	return destroyRetire(ctx, tbl, id, p)
}

// DestroyInstance 实例级销毁面：
// 仅当本地实例在册且其注册纪元与 inc 匹配时销毁并等待终态（Retire 形态，
// 终快照生效——组合规则与例外面见 destroyRetire）。
// 纪元不匹配（实例已换代/迟到的失败报告）或注册未完成（owner 未落位——
// 本实例从无目录主张）一律 no-op 返回 nil：心跳 OnLost 携带的纪元是失败
// 租约的纪元，本面据此把止损精确到触发实例，换代交错下不误伤新实例。
// 不在册 id 同 DestroyLocal 口径 no-op 返回 nil。
//
// 失租场景语义（godoc 契约，spec §5.1 第 7 点）：失租时他节点可能已成为
// owner 并写过新纪元快照——本实例的终快照写入受 Store 围栏保护（携带本
// 实例纪元，新 owner 落库后旧纪元的 Put 被 ErrStaleIncarnation 拒），且
// Retire 的「预算内重试后照样终局」语义使销毁不被 Store 故障/围栏拒绝
// 挂死：尽力而为是正确语义——丢的只是上一份 checkpoint 之后的增量，归属
// 归属已随租约让渡，新 owner 的写才是权威。
func DestroyInstance(ctx context.Context, tbl *activation.Table, id rans.ID, inc uint64) error {
	registered, ok := tbl.InstanceIncarnation(id)
	if !ok || registered != inc {
		return nil // 纪元不匹配：陈旧 OnLost / 注册未完成——零副作用
	}
	return DestroyLocal(ctx, tbl, id)
}

// DestroyPawn 实例身份守卫销毁（stop 形态）：仅当在册实例仍是 p 时销毁，
// 不落终快照（陈旧实例的状态本就不可落库——stale 即失主）。换代/已逐出
// no-op 返回 nil。围栏后销毁的止损面共用（sweeper/迁移 stale、所有权
// 丢失通知收端）。
func DestroyPawn(ctx context.Context, tbl *activation.Table, id rans.ID, p *pawnite.Pawnite) error {
	if cur, ok := tbl.PeekLocal(id); !ok || cur != p {
		return nil
	}
	return p.Destroy(ctx)
}

// RetirePawn 实例身份守卫销毁（Retire 形态）：仅当在册实例仍是 p 时销毁
// 并等终态，终快照照落——成员面换代围栏的止损面（键丢失但无人接管时本
// 实例仍是合法 owner，终快照有效；已被接管时 Store 围栏拒写，尽力而为
// 照常终局）。换代/已逐出 no-op 返回 nil。
func RetirePawn(ctx context.Context, tbl *activation.Table, id rans.ID, p *pawnite.Pawnite) error {
	if cur, ok := tbl.PeekLocal(id); !ok || cur != p {
		return nil
	}
	return destroyRetire(ctx, tbl, id, p)
}

// Shutdown 本地注册表整体收尾：快照 local
// 全部条目 → 并行逐实体 Retire 形态销毁（destroyRetire——终快照生效）
// → errors.Join 汇总 → 空表校验。
// 它是 Drainer 阶段②的同语义复合形态（并行度同为 max(8, CPU×8)）——
// Drainer 面向四段编排（门闸/flush/停序），本函数面向宿主直接收尾本地实体。
//
// 语义要点：
//   - ctx 是逐实体等待预算而非销毁开关：destroyRetire 耗尽只让本函数先
//     返回 ctx.Err()，loop 仍自驱终态，OnEvict→cleanup 的注销+逐出照常
//     闭环（放行后异步完成，无需重试销毁动作本身）；
//   - 单实体等待耗尽不阻断其余实体（并行等待互不依赖），其错误以
//     `shutdown: retire <id>: <err>` 计入 Join；
//   - 空表校验：Done 关闭 ⟹ OnEvict 已跑 ⟹ cleanup 已逐出（结构保证），
//     故全绿路径 Shutdown 返回时本地表必空（goroutine 泄漏测试可在
//     返回后断言全部 Done 关闭）；ctx 耗尽留下的滞留条目以独立错误面
//     呈现计数——条目随后由各自 loop 自主清出；
//   - 快照语义同 LocalIDs：快照后新激活者不归本函数（排空是终态动作，
//     宿主应先封消息面再 Shutdown；逐实体以快照实例引用销毁——不追换代
//     ——实例纪元匹配责任归调用面，快照实例已终局时是幂等 no-op）；
//     state-loader/hooks 不在收尾面（迁移注入随进程退出，无逐出动作）。
func Shutdown(ctx context.Context, tbl *activation.Table) error {
	locals := tbl.Locals()
	if len(locals) == 0 {
		return nil
	}

	par := runtime.NumCPU() * 8
	if par < 8 {
		par = 8
	}
	sem := make(chan struct{}, par)
	var (
		wg   sync.WaitGroup
		emu  sync.Mutex
		errs []error
	)
	for _, loc := range locals {
		wg.Add(1)
		sem <- struct{}{}
		go func(loc activation.PawnRef) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := destroyRetire(ctx, tbl, loc.ID, loc.Pawn); err != nil {
				emu.Lock()
				errs = append(errs, fmt.Errorf("shutdown: retire %s: %w", loc.ID.String(), err))
				emu.Unlock()
			}
		}(loc)
	}
	wg.Wait()

	if residual := len(tbl.Locals()); residual > 0 {
		errs = append(errs, fmt.Errorf(
			"rans: shutdown: left %d local entities behind (destroy in flight or stuck; eviction self-completes)", residual))
	}
	return errors.Join(errs...)
}

// destroyRetire Retire 形态销毁的公共实现（DestroyLocal/DestroyInstance/
// Shutdown 的共同接线——《加固 spec》§5.1 第 7 点：DestroyLocal 走
// Retire 交接窗，终快照生效）。p 是调用方持有的实例引用（快照语义：
// Shutdown/DestroyInstance 对快照实例销毁，不追换代）。
//
// 主路径：开 Handoff(HandoffRetire) 窗跑 retireCore。成功后补等 Done 关闭
// ——组合矩阵的跟随者结算点早于终局翻转（pawnite.Handoff 时序注记），
// 「销毁并等待终态」契约由这次等待闭环。
//
// 组合矩阵（pawnite.Handoff 承载）：DestroyLocal 撞上进行中
// 的 Passivate/Migrate 窗时不关 stop 抢占（抢占会丢终快照），而是
// 登记为跟随者等待在飞结果：成功 → 视为已完成（nil，终局随后落地）；失败
// → 窗回退 Active 后接棒自开 Retire 窗执行。
//
// 例外面（不写快照直达终局，回退既有例外面语义）：实例非 Active 时
// Handoff 以 ErrNotActive 拒绝开窗——激活中（注册/装载未完成，无确认状态
// 可拍终快照；stop 面直达终局）、他方 stop 销毁/看门狗强弃在飞（Destroying
// 无交接窗，本调用回旧面纯等待）或已终局（no-op）。强弃介入交接窗属豁免
// 路径：不走任何 core、零快照（pawnite 层锚定 + 窗口锚，见
// TestDestroyLocalRetireWindowAbandonNoSnapshot）。
func destroyRetire(ctx context.Context, tbl *activation.Table, id rans.ID, p *pawnite.Pawnite) error {
	ent, ok := tbl.EntityOf(id)
	if !ok {
		// 实体引用缺失（与 local 同临界区成对写，理论不可达）：保守回旧面
		// ——无实体引用无法拍快照，宁可退化为不写快照的销毁。
		return p.Destroy(ctx)
	}
	err := retireWindow(ctx, tbl, id, ent, p)
	switch {
	case err == nil:
		// 裁决既定成功（含跟随者结算点早于终局翻转的形态）：补等终态关闭，
		// 维持「销毁并等待终态」契约。
		select {
		case <-p.Done():
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	case errors.Is(err, pawnite.ErrNotActive):
		// 非 Active（检查后翻转或本就非 Active）：既有 stop 面收口——激活
		// 中抢占直达终局（无快照）；Destroying 无窗时是纯等待；已终局
		// no-op。此时 handoffPending 为假（有窗必走跟随者格），无抢占面。
		return p.Destroy(ctx)
	default:
		return err
	}
}

// retireCore 停机核心（spec §5.1 第 3 点表 Retire 行）：运行在 pawnite loop
// goroutine 上。与 Passivate 的差异是失败语义——停机不能被 Store 故障挂死：
//   - 有状态 + 配置 Store：快照捕获+Put 预算内重试（retirePutAttempts 次，
//     线性退避；捕获失败按 Put 失败进入同一重试环）；重试耗尽记 Error +
//     rans.checkpoint.final_failed 指标，照样终局（返回成功——丢失面是上一
//     份 checkpoint 之后的增量写，指标即告警源）；
//   - 预算 ctx 在重试环内耗尽：同「重试耗尽」收口（ctx 已死，继续重试只会
//     重复失败，挂死停机违反本行宗旨）；
//   - 有状态无 Store：不写快照 + 启动告警一次，照样终局；
//   - 无状态 / 空快照：直接终局。
//
// 例外：首步 AwaitCheckpointSettled 失败（预算耗尽于等待段）不终局——在飞
// 周期 Put 未落定时拍终快照有被旧快照后到覆盖的风险，如实返回错误回退
// Active（窗可重开；停机编排面的兜底见 retireInstance 注释）。
func retireCore(ctx context.Context, tbl *activation.Table, id rans.ID, ent rans.Entity, p *pawnite.Pawnite) pawnite.HandoffOutcome {
	if err := awaitSettled(ctx, tbl, p, "retire"); err != nil {
		return pawnite.HandoffOutcome{Err: err}
	}
	carrier, stateful := ent.(rans.Snapshotter)
	if !stateful {
		return pawnite.HandoffOutcome{} // 无状态：直接终局
	}
	store := tbl.SnapshotStore()
	if store == nil {
		tbl.WarnStatefulNoStore(id)
		return pawnite.HandoffOutcome{} // 不写快照，照样终局
	}
	var lastErr error
	for attempt := 0; attempt < retirePutAttempts; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, time.Duration(attempt)*retirePutBackoff); err != nil {
				break // 预算耗尽：同「重试耗尽」收口（下方统一终局）
			}
		}
		snap, err := carrier.Snapshot()
		if err != nil {
			lastErr = fmt.Errorf("rans: retire: snapshot capture failed: %w", err)
			continue // 捕获失败按 Put 失败处理：同环重试
		}
		if len(snap) == 0 {
			return pawnite.HandoffOutcome{} // 空快照：无内容可落，直接终局
		}
		// 围栏：Put 携带本 incarnation 纪元；stale 拒写=更新的化身已落库，
		// 终快照注定不可落——立即停止重试（R11），照常终局。
		if err := store.Put(ctx, id, p.Incarnation(), snap); err != nil {
			if errors.Is(err, checkpoint.ErrStaleIncarnation) {
				log.Info("rans/activation: retire final snapshot rejected as stale; terminal proceeds without retry",
					"uuid", id.String())
				tbl.MetricsRegistry().Counter(metrics.NameOwnershipStaleFenced,
					"stale-incarnation writes that fenced or dropped the writing instance").Add(1)
				return pawnite.HandoffOutcome{}
			}
			lastErr = fmt.Errorf("rans: retire: snapshot put failed: %w", err)
			continue
		}
		return pawnite.HandoffOutcome{} // 终快照落地
	}
	// 重试耗尽（或预算耗尽）：Error + 指标，照样终局——停机不能被 Store
	// 故障挂死（spec §5.1 第 3 点表 Retire 行）。
	log.Error("rans/activation: retire final snapshot failed after budgeted retries; proceeding to terminal",
		"uuid", id.String(), "attempts", retirePutAttempts, "err", lastErr)
	checkpointFinalFailedCounter().Add(1)
	return pawnite.HandoffOutcome{}
}

// sleepCtx 可中断退避：ctx 先到返回其错误。
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
