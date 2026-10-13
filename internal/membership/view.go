// 成员视图（spec §3.1「membership/ 节点存活（Liveness）与成员视图、续租
// （LeaseKeeper）」的「成员视图」半边）：目录派生的存活节点集查询与快照
// 订阅流。数据源经 NodesFunc 依赖注入（redis 侧实现是
// directory/redis.Directory.NodeIDs——SCAN 节点级成员键，「键在=节点活」）；
// 放置策略消费此视图（internal/placement 的候选查询函数同形）。
package membership

import (
	"context"
	"errors"
	"sync"

	"github.com/cyinx/rans_golang"
)

// NodesFunc 是存活节点集合数据源：返回成员视图当前的节点号。
// 返回切片归调用方所有（View 不缓存不排序；redis 实现已去重升序）。
type NodesFunc func(ctx context.Context) ([]rans.NodeID, error)

// watchBuffer 是每个 Watch 订阅通道的缓冲上限：快照流只有最新值有意义，
// 消费停滞时宁可丢旧快照也不阻塞 Refresh（Refresh 由调用方显式驱动，
// 慢订阅者不得拖停刷新方）。
const watchBuffer = 16

// ErrNoNodesFunc 是 View 未注入数据源的装配错误（NewView(nil) 之后的首个
// 查询返回它，不伪装成空集）。
var ErrNoNodesFunc = errors.New("rans: view: no NodesFunc")

// View 是存活节点视图：AliveNodes 即时查询数据源；Watch 订阅快照推送流。
// 零值不可用，经 NewView 构造；并发安全。
type View struct {
	nodes NodesFunc

	mu   sync.Mutex
	subs map[chan []rans.NodeID]struct{}
}

// NewView 构造视图。nodes 传 nil 不在构造期炸（装配顺序自由），
// 首个查询返回 ErrNoNodesFunc。
func NewView(nodes NodesFunc) *View {
	return &View{nodes: nodes, subs: make(map[chan []rans.NodeID]struct{})}
}

// AliveNodes 返回当前存活节点集（数据源直通，无缓存；切片归调用方）。
func (v *View) AliveNodes(ctx context.Context) ([]rans.NodeID, error) {
	return v.aliveNodes(ctx)
}

// Refresh 拉取一次快照并推送给全部 Watch 订阅者（非阻塞，见
// watchBuffer）；数据源失败原样返回错误且不推送。本方法只由调用方显式
// 驱动（View 自身不轮询，当前唯一刷新方是消费方手动调用；未来轮询器以
// 周期调用本方法为搭载点）。
func (v *View) Refresh(ctx context.Context) error {
	snap, err := v.aliveNodes(ctx)
	if err != nil {
		return err
	}
	v.push(snap)
	return nil
}

// Watch 订阅快照流：订阅即刻推送当前快照一次（数据源失败跳过首推，留待
// 下次 Refresh 补推——订阅不因首查失败变成死通道）；此后每次 Refresh 成功
// 都向全部订阅者推送。ctx 只门控订阅时的首次快照拉取——订阅生命周期由
// cancel 治理（ctx 取消不注销订阅）。cancel 注销订阅并关闭通道（幂等）。
// 本版本无内部轮询：推送只由显式 Refresh 驱动。
func (v *View) Watch(ctx context.Context) (<-chan []rans.NodeID, func()) {
	ch := make(chan []rans.NodeID, watchBuffer)
	// 取数在锁外（数据源可能走网络）；首推与注册都在锁内，与 cancel 的
	// delete+close 互斥，杜绝 send-on-closed。刚建的通道必能入 buffer。
	snap, err := v.aliveNodes(ctx)
	v.mu.Lock()
	if err == nil {
		ch <- copySnapshot(snap)
	}
	v.subs[ch] = struct{}{}
	v.mu.Unlock()

	cancel := sync.OnceFunc(func() {
		v.mu.Lock()
		defer v.mu.Unlock()
		if _, ok := v.subs[ch]; ok {
			delete(v.subs, ch)
			close(ch)
		}
	})
	return ch, cancel
}

// aliveNodes 是查询内核：数据源直通 + 装配校验。
func (v *View) aliveNodes(ctx context.Context) ([]rans.NodeID, error) {
	if v.nodes == nil {
		return nil, ErrNoNodesFunc
	}
	return v.nodes(ctx)
}

// push 向全部订阅者非阻塞推送快照副本（每个订阅者独立副本，防别名互踩）。
func (v *View) push(snap []rans.NodeID) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for ch := range v.subs {
		select {
		case ch <- copySnapshot(snap):
		default: // 消费停滞：丢弃这份快照，不阻塞 Refresh
		}
	}
}

// copySnapshot 复制快照切片（订阅者拿到的永远是私有副本）。
func copySnapshot(snap []rans.NodeID) []rans.NodeID {
	out := make([]rans.NodeID, len(snap))
	copy(out, snap)
	return out
}
