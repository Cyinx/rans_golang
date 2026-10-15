// 节点本地成员视图：周期刷新（缺省 1s，Tuning.MembershipRefresh
// 可调、下限 100ms）经 NodeRegistry.Nodes 一次取全量 NodeInfo；三个消费面
// （路由有效性/放置候选/地址解析）读视图，接管闸门/迁移代际读/销毁清理
// 保持实时注册表读（双轨判定——路由答错代价=一次失败重试，接管答错代价
// =双活，决策记录随票提交）。
package membership

import (
	"context"
	"sync"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

// 本地视图缺省参数（可调面经 node.Config.Tuning / 装配参数注入）。
const (
	// DefaultRefreshInterval 周期刷新缺省（grill 拍板 1s）。
	DefaultRefreshInterval = time.Second
	// MinRefreshInterval 刷新周期下限（node.New 拒绝更小——低于此等于
	// 没有视图，退回每跳读还多一层间接）。
	MinRefreshInterval = 100 * time.Millisecond
	// firstQueryBudget 首查同步刷新预算（超时按空视图保守处理并计失败）。
	firstQueryBudget = time.Second
	// invalidateCoalesce 即时失效的全局合并窗（刷新本为全量，按节点去抖
	// 无意义——窗内后续失效事件并入同一次刷新）。
	invalidateCoalesce = 200 * time.Millisecond
)

// NodeInfoSource 全量节点信息数据源（NodeRegistry.Nodes 的窄化缝——测试
// 注入替身，生产接目录注册表）。
type NodeInfoSource func(ctx context.Context) (map[rans.NodeID]directory.NodeInfo, error)

// LocalView 节点本地成员视图：快照 + 周期刷新 + 即时失效 + 查询面。并发
// 安全；Close 收口刷新循环与在途合并刷新。
type LocalView struct {
	src     NodeInfoSource
	ttl     time.Duration // 成员键 TTL：视图年龄上限（快照最长可信期）
	refresh time.Duration

	// 观测三件（grill 拍板）：年龄 gauge 取 Age()；两个 counter 为钩子
	//（node 装配接线，nil 静默）。
	onRefreshFailed func()
	onInvalidation  func()

	mu          sync.Mutex
	snapshot    map[rans.NodeID]directory.NodeInfo
	refreshedAt time.Time
	firstDone   bool

	refreshMu  sync.Mutex
	refreshing bool
	lastInval  time.Time

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewLocalView 构造并启动周期刷新循环（首刷由首查同步触发或本循环先到
// 者承担）。ttl 为装配时传入注册表的成员键 TTL（redis HeartbeatTTL /
// inmem hbTTL）——快照比判活键本身还老时完全不可信，查询退保守存活；
// ≤0 视为无年龄上限（数据源不老化形态）。
func NewLocalView(src NodeInfoSource, ttl, refresh time.Duration) *LocalView {
	if refresh <= 0 {
		refresh = DefaultRefreshInterval
	}
	v := &LocalView{
		src:     src,
		ttl:     ttl,
		refresh: refresh,
		stopCh:  make(chan struct{}),
	}
	v.wg.Add(1)
	go v.loop()
	return v
}

// BindMetrics 观测接线（装配期）：刷新失败与即时失效的计数钩子；年龄
// gauge 由装配侧经 Age() 读数注册。
func (v *LocalView) BindMetrics(onRefreshFailed, onInvalidation func()) {
	v.onRefreshFailed = onRefreshFailed
	v.onInvalidation = onInvalidation
}

// Close 停止刷新循环（幂等；在途刷新自然收场——doRefresh 无重入危险）。
func (v *LocalView) Close() {
	v.stopOnce.Do(func() {
		close(v.stopCh)
		v.wg.Wait()
	})
}

// Age 快照年龄（未首刷为负值哨兵——观测面按缺省呈现）。
func (v *LocalView) Age() time.Duration {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.firstDone {
		return -1
	}
	return time.Since(v.refreshedAt)
}

// loop 周期刷新。
func (v *LocalView) loop() {
	defer v.wg.Done()
	t := time.NewTicker(v.refresh)
	defer t.Stop()
	for {
		select {
		case <-v.stopCh:
			return
		case <-t.C:
			v.refreshNow(context.Background())
		}
	}
}

// snap 快照读取：valid=已首刷且未超龄（超龄=比成员键 TTL 还老的快照完全
// 不可信，查询退保守存活）。
func (v *LocalView) snap() (map[rans.NodeID]directory.NodeInfo, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	// 非空守卫：registry 形态下自身必在快照中——空快照（如接管窗内键被
	// 逐个删落的瞬间全量读）是伪有效，按不可信退保守存活（Form4 实证：
	// 空「有效」视图把一切判死 → 放置恒本地 → 仲裁败连环）。
	valid := v.firstDone && len(v.snapshot) > 0 && (v.ttl <= 0 || time.Since(v.refreshedAt) <= v.ttl)
	return v.snapshot, valid
}

// Alive 严格判定（路由有效性消费面）：节点存活且代际一致（epoch==0 的
// 查询视为代际未知不过滤——提示条目语义，票 15 严格化时收紧）。视图不
// 可信（未首刷/超龄/刷新失败后老化）→ 保守存活（与 OwnerAlive 读错误同
// 口径，发送失败+重试环兜底）；可信视图无此节点 → 死（v0.26 一死永死后
// 缓存判死恒安全：复活=新代际，epoch 比对拦截）。
func (v *LocalView) Alive(node rans.NodeID, epoch uint64) bool {
	v.ensureFirst()
	nodes, valid := v.snap()
	if !valid {
		return true
	}
	info, ok := nodes[node]
	if !ok {
		return false
	}
	return epoch == 0 || info.Epoch == epoch
}

// Present 提示判定（票 15 的放置提示/迁移提示缓存条目消费面；本票自锚
// 不接消费）：节点存活任意代际。视图不可信 → 保守存活。
func (v *LocalView) Present(node rans.NodeID) bool {
	v.ensureFirst()
	nodes, valid := v.snap()
	if !valid {
		return true
	}
	_, ok := nodes[node]
	return ok
}

// Candidates kind 感知放置候选（同 NodesByKind 口径：排除 draining 与不
// 承载该 kind 的节点；nil kinds = 纯心跳节点不进候选）。视图不可信 → 空
// （放置 miss 由调用面收口本地激活——冷启动直读注册表的同形空窗）。
func (v *LocalView) Candidates(kind string) []rans.NodeID {
	v.ensureFirst()
	nodes, valid := v.snap()
	if !valid {
		return nil
	}
	var out []rans.NodeID
	for id, info := range nodes {
		if info.Draining || !kindCarries(info.Kinds, kind) {
			continue
		}
		out = append(out, id)
	}
	sortAsc(out)
	return out
}

// Addr 节点地址（不过滤 draining——排空中的节点仍需寻址发帧；回包/转
// 发面）。视图命中返回快照值；miss/不可信由调用面回落注册表实时读。
func (v *LocalView) Addr(node rans.NodeID) (string, bool) {
	v.ensureFirst()
	nodes, valid := v.snap()
	if !valid {
		return "", false
	}
	info, ok := nodes[node]
	if !ok || info.Addr == "" {
		return "", false
	}
	return info.Addr, true
}

// Invalidate 即时失效（连接丢失/端点判停/重定向提示=拓扑信号，非判死）：
// 合并窗后刷一次全量（窗内后续事件并入）。关箱后静默。
func (v *LocalView) Invalidate() {
	select {
	case <-v.stopCh:
		return
	default:
	}
	if v.onInvalidation != nil {
		v.onInvalidation()
	}
	v.refreshMu.Lock()
	if v.refreshing || time.Since(v.lastInval) < invalidateCoalesce {
		v.refreshMu.Unlock()
		return
	}
	v.refreshing = true
	v.lastInval = time.Now()
	v.refreshMu.Unlock()
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		defer func() {
			v.refreshMu.Lock()
			v.refreshing = false
			v.refreshMu.Unlock()
		}()
		time.Sleep(invalidateCoalesce)
		select {
		case <-v.stopCh:
			return
		default:
		}
		v.doRefresh(context.Background())
	}()
}

// ensureFirst 首查窗口（grill 拍板）：首次查询同步触发一次刷新
// （singleflight——并发首查等在途者；预算 1s），失败/超时按空视图保守
// 处理——冷启动无「放置全落本节点」偏斜窗口，且不阻塞启动路径。
func (v *LocalView) ensureFirst() {
	v.mu.Lock()
	firstDone := v.firstDone
	v.mu.Unlock()
	if firstDone {
		return
	}
	v.refreshMu.Lock()
	if v.refreshing {
		v.refreshMu.Unlock()
		deadline := time.Now().Add(firstQueryBudget)
		for time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
			v.mu.Lock()
			done := v.firstDone
			v.mu.Unlock()
			if done {
				return
			}
		}
		return
	}
	v.refreshing = true
	v.refreshMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), firstQueryBudget)
	defer cancel()
	v.doRefresh(ctx)
	v.refreshMu.Lock()
	v.refreshing = false
	v.refreshMu.Unlock()
}

// refreshNow 周期刷新入口（与失效/首查共用 doRefresh；并发安全——快照
// 换装原子，旧者被新者覆盖无害）。
func (v *LocalView) refreshNow(ctx context.Context) {
	v.doRefresh(ctx)
}

// doRefresh 单次全量刷新：成功换快照；失败保留旧快照并计数（超龄判据
// 归查询面 snap，不在此处置）。
func (v *LocalView) doRefresh(ctx context.Context) {
	nodes, err := v.src(ctx)
	if err != nil {
		if v.onRefreshFailed != nil {
			v.onRefreshFailed()
		}
		return
	}
	v.mu.Lock()
	v.snapshot = nodes
	v.refreshedAt = time.Now()
	v.firstDone = true
	v.mu.Unlock()
}

// kindCarries kinds 声明承载判定。
func kindCarries(kinds []string, kind string) bool {
	for _, k := range kinds {
		if k == kind {
			return true
		}
	}
	return false
}

// sortAsc 升序（视图遍历 map 的确定性输出；候选集小，插入序足够）。
func sortAsc(ids []rans.NodeID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
}
