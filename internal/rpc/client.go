package rpc

import (
	"context"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/ctxkey"
	"github.com/cyinx/rans_golang/internal/routing"
)

// NewClient 返回实现 rans.Client 的路由客户端（统一调用入口的执行体——
// node.Client() 的装配点）：Call/Tell 薄
// 适配到路由面 CallOpts/SendOneWayOpts（本地命中走本地快路径，错误形态与
// 远端一致），并把自身注入 dispatcher 的投递基 ctx——入站请求与激活闭包
// 派生的实体 ctx 由此携带调用面（实体自省 rans.ClientFrom 可用）。
//
// 选项映射：rans.Idempotent() → InvokeOptions.Idempotent（重试分类唯一表
// 的幂等标注过线语义不变）；rans.Timeout(d) → InvokeOptions.Timeout
// （逐请求预算合成的选项源，合成落点在 callBudget）。
//
// 装配序约定：须在该 Remote Start 之前调用（dispatcher 的基 ctx 在投递时
// 读取，Start 前单写建立 happens-before）。
func NewClient(r *Remote) rans.Client {
	c := &routeClient{r: r}
	r.disp.setBaseClient(c)
	return c
}

// routeClient rans.Client 的路由薄适配（一个 Remote 一个实例——调用面即
// 节点本身的路由视角）。
type routeClient struct {
	r *Remote
}

// Call 请求-响应调用：阻塞到应答、错误或 ctx 结束。出站 ctx 注入调用面
// （本地路径的激活闭包与实体 Handle ctx 派生链由此保值）。
func (c *routeClient) Call(ctx context.Context, id rans.ID, msg any, opts ...rans.CallOption) (any, error) {
	return c.r.CallOpts(ctxkey.WithClient(ctx, c), id, msg, invokeOptions(opts))
}

// Tell 单向发送：入箱即返回，无应答无回执（at-most-once 语义与 OneWay 环
// 不变）。出站 ctx 注入调用面（同 Call）。
func (c *routeClient) Tell(ctx context.Context, id rans.ID, msg any, opts ...rans.CallOption) error {
	return c.r.SendOneWayOpts(ctxkey.WithClient(ctx, c), id, msg, invokeOptions(opts))
}

// TryTell 非阻塞单向发送（H11 决策 12 的事件扇出生产者分野）：路由与编码
// 与 Tell 同面，唯一差异在发件箱满箱语义——满即丢计数（errOutboxFull）
// 不阻塞调用方。不进 rans.Client 公开接口（显式 Tell 的满箱阻塞语义不
// 变）：消费方经能力探测取用（对实体 ctx 携带的统一调用面做接口断言）。
func (c *routeClient) TryTell(ctx context.Context, id rans.ID, msg any) error {
	return c.r.sendOneWayLeg(ctxkey.WithClient(ctx, c), id, msg, routing.DefaultOptions(), true)
}

// invokeOptions CallOption 集合 → 路由面选项。
func invokeOptions(opts []rans.CallOption) routing.InvokeOptions {
	return routing.InvokeOptions{
		Idempotent: rans.IsIdempotent(opts...),
		Timeout:    rans.TimeoutOf(opts...),
	}
}
