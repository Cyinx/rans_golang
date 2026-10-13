// 收件箱与准入：容量上限、FIFO 队列、按需增长与满箱回绝。
package pawnite

import (
	"github.com/cyinx/rans_golang"
)

// InboxDepthLimit 收件箱容量上限（规格附录 A：10_000）。
const InboxDepthLimit = 10_000

// InboxLen 返回收件箱当前积压深度（inbox_depth Gauge 的
// 逐实体数据源，manager.MetricsView 求和消费）。mu 惯例内包裹读取
// ——与状态读取同一锁口径便于推理。入箱/取出发生在准入面（持 mu）与
// loop goroutine 的 popMail——读数是采样瞬间的近似（观测语义，不承担
// 精确记账）。
func (p *Pawnite) InboxLen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.mailbox) - p.mailHead
}

// popMail 取出队首一条邮件（mu 内）。队列有余量时重武装就绪信号（容量 1，
// 本分支消费 token 后必须补发——否则第二条排队邮件会在信号耗尽后滞留）。
// 弹出槽位随即置 nil：底层数组按整块存续（len 视野外的槽位也被扫描），
// 不置 nil 会把已投递引用钉到下一次压缩/排空。整批耗尽（head==len）后
// 回收大缓冲：cap>256 置 nil 交还堆，否则 head 归零原地复用（槽位已全
// nil，复用不残留引用）。每次只取一条——控制信号（stop/迁移/捕获）在
// 两条消息之间保持被选中的机会（与既有 select 随机语义一致）。
func (p *Pawnite) popMail() (InboundDelivery, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mailHead == len(p.mailbox) {
		return nil, false
	}
	d := p.mailbox[p.mailHead]
	p.mailbox[p.mailHead] = nil
	p.mailHead++
	if p.mailHead == len(p.mailbox) {
		if cap(p.mailbox) > 256 {
			p.mailbox = nil
		} else {
			p.mailbox = p.mailbox[:0]
		}
		p.mailHead = 0
	}
	if p.mailHead < len(p.mailbox) {
		select {
		case p.mailCh <- struct{}{}:
		default: // 信号已在（入箱面刚补发）：无需重复
		}
	}
	return d, true
}

// rearmMailSignal 重武装收件箱就绪信号（mailCh 消费后未取件的让位分支用；
// 入箱面的并发补发由容量 1 非阻塞投递天然去重）。
func (p *Pawnite) rearmMailSignal() {
	select {
	case p.mailCh <- struct{}{}:
	default: // 信号已在（入箱面刚补发）：无需重复
	}
}

// compactLocked 就地压缩已消费前缀（调用方必须已持 mu）：存活段 [head,len)
// 前移、head 归零。入箱路径在底层数组写满且存在已消费前缀时先压缩再
// append——持续积压（队列从不全排空）时 len 不随累计入箱数单调增长，
// 数组容量收敛到峰值积压附近；前移覆盖与弹出置 nil 保证数组不残留已
// 处理引用。压缩成本 O(存活段) 与扩容拷贝同阶，仅在 len==cap 边界触发。
func (p *Pawnite) compactLocked() {
	n := len(p.mailbox) - p.mailHead
	copy(p.mailbox, p.mailbox[p.mailHead:])
	p.mailbox = p.mailbox[:n]
	p.mailHead = 0
}

// ReceiveRequest 收件箱准入：任何状态下没有第二条路径把消息送进实体队列（规格不变量 2）。
// 返回 true=已入箱；false=已就地处置（Reject 已被调用；迁移赢家与仲裁输家
// 终态的迟到者例外——转发给目标/赢家，调用方零丢失）。
// 交接窗（Destroying 且 handoffPending）放行——留箱等待（规格 §13.2
// "不转发会转回自己"）：窗内消息在终局处置按 kind 分流（Migrate 赢家整箱
// 转发新家；Passivate/Retire 按确定未执行回绝，调用方重试落新激活）。
// 陷阱 3 的对偶面：Handoff 同临界区先置 pending 再置 Destroying，
// 故此处读到 Destroying 时 pending 必已可读，不存在误回绝窗口。
// 非交接的 Destroying 照旧回绝（规格 §4.2）；迁移赢家实例（Won 终局翻转后）
// 与仲裁输家实例（激活落败直达终态）的迟到者一律转发——调用方的位置是
// 终局前的合法观测（§13.2/§12 零丢失）。
func (p *Pawnite) ReceiveRequest(d InboundDelivery) bool {
	p.mu.Lock()
	if p.state == StateDestroyed || (p.state == StateDestroying && !p.handoffPending) {
		p.mu.Unlock()
		// 终态（或非迁移的销毁中）迟到者：就地经 disposeLate 终局矩阵处置
		// ——迁移赢家/仲裁输家转发给目标（目录归属已让渡，按销毁回绝即
		// 丢失；调用方位置是让渡前源端的合法观测），优雅销毁/强弃回绝。
		p.disposeLate([]InboundDelivery{d})
		return false
	}
	// 准入判环（H12 决策 15，可选接口 CycleAware）：路径含本实体自身哈希
	// → 立即 (CodeError, ReasonCycle) 回绝——消息未执行、不可重试（重试必
	// 然再成环）、文本带完整环（渲染归投递实现）。置于终局转发之后：终态
	// 迟到者的整箱转发是零丢失语义，赢家按同身份再判环不冲突；置于
	// fenced/busy 之前：环是身份维的确定性诊断，与实例状态无关。
	if ca, ok := d.(CycleAware); ok {
		for _, hop := range ca.DeliveryCausalPath() {
			if hop == p.selfHash {
				p.mu.Unlock()
				d.Reject(rans.CodeError, rans.ReasonCycle)
				return false
			}
		}
	}
	if p.fenced {
		p.mu.Unlock()
		d.Reject(rans.CodeUnavailable, rans.ReasonLeaseLost)
		return false
	}
	if len(p.mailbox)-p.mailHead >= p.inboxCap {
		p.mu.Unlock()
		d.Reject(rans.CodeUnavailable, rans.ReasonBusy)
		return false
	}
	if len(p.mailbox) >= cap(p.mailbox) && p.mailHead > 0 {
		p.compactLocked() // 数组写满且已消费前缀可腾：先压缩再入箱，防持续积压下无界增长
	}
	p.mailbox = append(p.mailbox, d)
	p.mu.Unlock()
	select {
	case p.mailCh <- struct{}{}:
	default: // 信号已在：无需重复
	}
	return true
}

// drainReject 回绝收件箱中全部排队消息：收集持 mu，Reject 在锁外逐个调用
// （规格 §19 陷阱 2：锁内禁回调；Go sync.Mutex 不可重入，Reject 若回调本单元
// 的 State/ReceiveRequest 会死锁）。保持 FIFO 回绝顺序。
func (p *Pawnite) drainReject(st rans.Code, reason rans.Reason) {
	p.mu.Lock()
	pending := p.takeInboxLocked()
	p.mu.Unlock()
	for _, d := range pending {
		d.Reject(st, reason)
	}
}

// takeInboxLocked 非阻塞取空收件箱（调用方必须已持 mu）。取空即回收
// （终局路径，无需保留缓冲）。
func (p *Pawnite) takeInboxLocked() []InboundDelivery {
	pending := p.mailbox[p.mailHead:]
	p.mailbox = nil
	p.mailHead = 0
	return pending
}
