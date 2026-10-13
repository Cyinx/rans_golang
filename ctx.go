package rans

import (
	"context"

	"github.com/cyinx/rans_golang/internal/ctxkey"
)

// Self 返回当前实体身份（Activate / Handle / Deactivate 的 ctx 都携带；
// 由运行时在激活与投递边界注入）。不阻塞；并发安全；专供实体钩子内调用。
func Self(ctx context.Context) (ID, bool) {
	kind, key, ok := ctxkey.SelfFrom(ctx)
	if !ok {
		return ID{}, false
	}
	return ID{Kind: kind, Key: key}, true
}

// Incarnation 返回当前实例的纪元（fencing 令牌自省面）。false = 未围栏
// （无纪元源的裸形态或非 rans 接线的 ctx）。不阻塞；并发安全；专供实体
// 钩子内调用。
func Incarnation(ctx context.Context) (uint64, bool) {
	return ctxkey.IncarnationFrom(ctx)
}

// ClientFrom 返回 ctx 携带的调用面（实体在其钩子内发起调用用）；未注入
// 或注入值类型不符时返回 nil。不阻塞；并发安全；专供实体钩子内调用。
func ClientFrom(ctx context.Context) Client {
	v, ok := ctxkey.ClientFrom(ctx)
	if !ok {
		return nil
	}
	c, _ := v.(Client)
	return c
}
