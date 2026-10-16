// Package node 是 rans 节点的唯一组装根（约定 spec §3.1）：目录/激活表/
// 存活节拍（成员键刷新+自围栏）/位置缓存/放置/传输与启动停机次序全部
// 内置，消费方只面对 Config 与 Node 公开面。本包零业务概念。
//
// 生命周期动词（约定 spec §4.3 唯一正本）：Start（恰一次）→ Shutdown
// （优雅排空）/ Close（立即、幂等）；Run 为便捷形态（Start → 等信号/ctx →
// Shutdown）。调用一律经 Client()（唯一调用入口）；管理操作
// Destroy/Migrate 只作用于本地实例。
package node

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	mgr "github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/membership"
	"github.com/cyinx/rans_golang/internal/metrics"
	"github.com/cyinx/rans_golang/internal/routing"
	"github.com/cyinx/rans_golang/internal/rpc"
)

// Node 一个 rans 节点进程的全部把手。New 构造即完整（全部校验在构造期，
// 失败返回错误，无半开组合体——失败路径内部已自收口）；New 期即启动常驻
// goroutine（网关扫描器、成员视图刷新；发件箱 worker 按目标队列懒起——
// 均 Close/Shutdown 收口）；Start 起跑，Shutdown
// 按排空序优雅收口，Close 立即停止（不排空）。Start 恰一次；终态后不可
// 复用，重建而非重启。
type Node struct {
	cfg      Config
	dir      directory.Directory
	closeDir bool // node 自建目录（redis 形态）Shutdown 才关；注入目录归调用方
	mgr      *mgr.Table
	lv       *membership.Liveness   // 节点存活信号（目录实现 NodeRegistry 才有；inmem nil）
	view     *membership.LocalView  // 节点本地成员视图（票 01；nil = 无注册表形态）
	cache    *routing.LocationCache // 位置缓存（恒装配）
	rem      *rpc.Remote
	client   rans.Client // 唯一调用入口（统一路由：本地快路径/远程一形态）

	mreg      *metrics.Registry // 本节点自有注册表（Metrics() 快照的落点）
	runCtx    context.Context
	runCancel context.CancelFunc
	addr      atomic.Pointer[string]
	started   atomic.Bool // Start 单次性守卫（二次 Start 返回错误）
	stopOnce  sync.Once   // Shutdown 幂等
	closeOnce sync.Once   // Close 幂等（与 Shutdown 独立——Close 后 Shutdown 仍是合法清理路径）
	stopErr   error       // 首次 Shutdown 的合并错误缓存（二次调用返回同值）
	closeErr  error       // 首次 Close 的错误缓存
}

// Client 返回本节点的统一调用入口：一切调用经路由——本地命中走本地快速
// 路径，远端经目录+传输；错误形态一致。不阻塞；并发安全；可在实体 Handle
// 内调用（嵌套调用，注意 H12 调用环约定）。
func (n *Node) Client() rans.Client { return n.client }

// Addr 返回 Start 绑定成功的实际监听地址；未 Start（或 Standalone 无传输）
// 返回空串。不阻塞；并发安全。
func (n *Node) Addr() string {
	if p := n.addr.Load(); p != nil {
		return *p
	}
	return ""
}
