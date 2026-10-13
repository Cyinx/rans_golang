package rpc

// 系统控制面消息的收端处置（H10 取消 / H13 所有权丢失与重定向提示）：
// 全部为「不裁决 pending 的节点级消息」——处置只做缓存/围栏/取消面，
// 应答照常走既有 reply 通道。

import (
	"context"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/lifecycle"
	"github.com/cyinx/rans_golang/internal/log"
	"github.com/cyinx/rans_golang/internal/wire"
)

// handleOwnershipLost 所有权丢失止损（H13 决策 22 收端）：归属全等 →
// Fence + 异步销毁（幂等）；畸形载荷 warn 丢弃。
func (d *Dispatcher) handleOwnershipLost(hdr wire.Header, body []byte) {
	defer func() {
		if body != nil {
			d.pool.Put(body)
		}
	}()
	v, err := wire.DecodeOwnershipLost(body)
	if err != nil {
		log.Warn("rans/rpc: ownership-lost dropped (malformed payload)",
			"message", hdr.MessageID, "err", err)
		return
	}
	id, err := rans.NewID(hdr.TargetKind, hdr.TargetInst)
	if err != nil {
		log.Warn("rans/rpc: ownership-lost dropped (malformed target uuid)",
			"message", hdr.MessageID, "err", err)
		return
	}
	prev := directory.Owner{Node: d.self, Epoch: v.Epoch, Incarnation: v.Incarnation}
	if o, ok := d.mgr.InstanceOwner(id); !ok || o != prev {
		return // 无实例/归属不符（已换代/迟到/重复）：幂等 no-op
	}
	d.mgr.Fence(id)
	go func() {
		// 例外②：止损销毁预算（ownershipLostDestroyBudget 集中定义——通知
		// 处置无外生 ctx，读循环不等待本 goroutine）。
		ctx, cancel := context.WithTimeout(context.Background(), ownershipLostDestroyBudget)
		defer cancel()
		if err := lifecycle.DestroyInstance(ctx, d.mgr, id, v.Incarnation); err != nil {
			log.Warn("rans/rpc: ownership-lost destroy failed (entity stays fenced)",
				"uuid", id.String(), "err", err)
		}
	}()
	log.Info("rans/rpc: ownership lost notification fenced local instance",
		"uuid", id.String(), "prev_epoch", v.Epoch, "prev_inc", v.Incarnation)
}

// handleRedirectHint 重定向提示（H13 决策 24 收端）：携带的正确归属即时
// 改写位置缓存（仅缓存面，不裁决任何 pending——请求本体已被转发，正常
// 应答照常到达）。畸形载荷/无缓存接线 → 丢弃（加速面，丢失由正 TTL
// 兜底收敛）。
func (d *Dispatcher) handleRedirectHint(hdr wire.Header, body []byte) {
	defer func() {
		if body != nil {
			d.pool.Put(body)
		}
	}()
	if d.cache == nil {
		return
	}
	v, err := wire.DecodeRedirectHint(body)
	if err != nil {
		return
	}
	id, err := rans.NewID(hdr.TargetKind, hdr.TargetInst)
	if err != nil {
		return
	}
	d.cache.PutPositive(id, directory.Owner{Node: rans.NodeID(v.Node), Epoch: v.Epoch})
}
