package rpc

// 路由梯子（位置缓存 → 目录 Lookup → 放置/本地激活）与成员代际有效性
// 判定（H13：死 owner 按陈旧归属处理，翻转由激活 CAS 接管完成）。

import (
	"context"
	"fmt"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/routing"
)

// route 路由梯子（规格 §8.1 ②/§9.2）：返回（目标节点, 是否远程, 错误）。
// 缓存未配置 → 直查目录（零缓存缺省形态）。缓存已配置 →
//  1. 缓存 Get：正命中 → 缓存位置（本节点 → 本地，他节点 → 远程）；负命中
//     → 跳过目录查询（防穿透），按 miss 走放置/本地；
//  2. miss → 目录 Lookup：命中 → PutPositive 回填（本地/远程同前）；查无
//     → PutNegative 回填 → 放置决策；
//  3. miss 放置：Placement 非 nil 且 Pick(self) 命中 →
//     远程（对选中节点远发，对端 dispatcher 的 GetOrActivate 把实体落在
//     那儿）；Placement 为 nil 或无候选 → 本地激活（缺省行为）。
func (r *Remote) route(ctx context.Context, id rans.ID) (tgt rans.NodeID, rem bool, rerr error) {
	if r.cfg.LocationCache != nil {
		if loc, found, positive := r.cfg.LocationCache.Get(id); found {
			if positive {
				if loc.Node == r.cfg.NodeID {
					return 0, false, nil
				}
				if !r.ownerAlive(ctx, loc) && !r.ownerAliveConfirmed(ctx, loc) {
					// 死 owner 正缓存：失效按 miss 收口（归属翻转由接管完成）。
					r.cfg.LocationCache.Invalidate(id)
					return r.routeMiss(id)
				}
				return loc.Node, true, nil
			}
			// 负缓存：negTTL 窗口内该 id 无目录条目——跳过目录查询（防
			// 穿透），按 miss 收口到放置/本地。
			return r.routeMiss(id)
		}
	}
	loc, found, err := r.cfg.Dir.Lookup(ctx, id)
	if err != nil {
		if ctx.Err() != nil {
			// ctx 结束不包装原始错误（票 13）：统一 *rans.Error 收场，
			// 调用错误面不再泄漏裸 ctx 错误。
			return 0, false, routing.CtxOutcome(ctx)
		}
		return 0, false, fmt.Errorf("rans: invoke: directory lookup %s: %w", id.String(), err)
	}
	if found {
		// 遗留 epoch-0 目录条目（票 15）：registry 形态不再产生（票 14 同
		// 步取号）——真值面判死，按 miss 收口走放置/本地激活（CAS 自愈）。
		// 缓存路径不进此判定（提示条目 epoch=0 是「未知」语义——视图
		// Alive 的 epoch-0 不过滤）。
		if loc.Epoch == 0 && r.registryForm() && loc.Node != r.cfg.NodeID {
			return r.routeMiss(id)
		}
		if loc.Node != r.cfg.NodeID && !r.ownerAlive(ctx, loc) && !r.ownerAliveConfirmed(ctx, loc) {
			// 死 owner 目录条目（H13：条目有效 ⟺ owner 在成员视图存活）
			// 按 miss 收口：不向死者投递，走放置/本地激活（激活 Register
			// 的 CAS 接管完成归属翻转）。视图判死后经 ownerAliveConfirmed
			// 实时坐实（快照可能滞后于成员面动荡窗——见其注释）。
			return r.routeMiss(id)
		}
		if r.cfg.LocationCache != nil {
			r.cfg.LocationCache.PutPositive(id, loc)
		}
		if loc.Node == r.cfg.NodeID {
			return 0, false, nil
		}
		return loc.Node, true, nil
	}
	if r.cfg.LocationCache != nil {
		r.cfg.LocationCache.PutNegative(id)
	}
	return r.routeMiss(id)
}

// registryForm 目录是否带成员面（遗留 epoch-0 判定的前提）。
func (r *Remote) registryForm() bool {
	_, ok := r.cfg.Dir.(directory.NodeRegistry)
	return ok
}

// ownerAliveConfirmed 视图判死后的实时确认（票 01 双轨的收口缝）：视图
// 快照可能滞后来自成员面动荡窗（接管/换代/分区恢复的一瞬），凭一次撒谎
// 的快照把存活 owner 判死会诱发 miss 重洗 + 本地 CAS 连环拒——判死是
// 改变路由走向的决策，与接管闸门同族，值得一次实时读坐实（死 owner 是
// 稀有分支，每跳读放大不回归）。无视图形态恒真（ownerAlive 已是实时）。
func (r *Remote) ownerAliveConfirmed(ctx context.Context, loc directory.Owner) bool {
	if r.cfg.RoutingView == nil {
		return true
	}
	return directory.OwnerAlive(ctx, r.cfg.Dir, loc)
}

// ownerAlive 路由有效性判定（票 01 双轨判定的热路径轨）：视图优先（快照
// 判定，零注册表读）；无视图回落 directory.OwnerAlive 实时读（单机/测试
// 形态）。接管闸门（activation）不经此面——保持实时读。
func (r *Remote) ownerAlive(ctx context.Context, loc directory.Owner) bool {
	if r.cfg.RoutingView != nil {
		return r.cfg.RoutingView.Alive(loc.Node, loc.Epoch)
	}
	return directory.OwnerAlive(ctx, r.cfg.Dir, loc)
}
