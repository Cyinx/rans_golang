package rpc

// 路由梯子（位置缓存 → 目录 Lookup → 放置/本地激活）与成员代际有效性
// 判定（H13：死 owner 按陈旧归属处理，翻转由激活 CAS 接管完成）。

import (
	"context"
	"fmt"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
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
func (r *Remote) route(ctx context.Context, id rans.ID) (rans.NodeID, bool, error) {
	if r.cfg.LocationCache != nil {
		if loc, found, positive := r.cfg.LocationCache.Get(id); found {
			if positive {
				if loc.Node == r.cfg.NodeID {
					return 0, false, nil
				}
				if !directory.OwnerAlive(ctx, r.cfg.Dir, loc) {
					// 死 owner 正缓存：失效按 miss 收口（v3 归属翻转由接管完成）。
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
		return 0, false, fmt.Errorf("rans: invoke: directory lookup %s: %w", id.String(), err)
	}
	if found {
		if loc.Node != r.cfg.NodeID && !directory.OwnerAlive(ctx, r.cfg.Dir, loc) {
			// 死 owner 目录条目（H13：条目有效 ⟺ owner 在成员视图存活）
			// 按陈旧归属处理：不向死者投递，按 miss 收口走放置/本地激活
			//（激活 Register 的 CAS 接管完成归属翻转）。
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
