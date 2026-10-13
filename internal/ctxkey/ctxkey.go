// Package ctxkey 承载运行时上下文键（零依赖叶包）：身份、纪元与调用面的
// 注入/读取原语。键用未导出类型——模块外无法伪造身份、令牌或调用面；
// 类型化访问器在 rans 包（Self / Incarnation / ClientFrom）。
package ctxkey

import "context"

// key 是本包私有键类型（模块外不可构造等价键）。
type key int

const (
	keySelf key = iota
	keyIncarnation
	keyClient
	keyCausal
	keyDefer
	keyReenter
)

// self 是身份载荷（kind/key 对，rans.ID 的存储形态——叶包不依赖 rans）。
// 指针形态：nil 载荷 = WithoutSelf 的遮蔽哨兵（ctx 值无法移除，遮蔽即
// 「不携带」的唯一实现，同 WithoutCausal 的 typed-nil 手法）。
type self struct {
	kind string
	key  string
}

// WithSelf 把实体身份（kind/key）挂进 ctx：运行时在激活与投递边界注入，
// 实体在 Activate/Handle/Deactivate 内经 rans.Self 读取。
func WithSelf(ctx context.Context, kind, key string) context.Context {
	return context.WithValue(ctx, keySelf, &self{kind: kind, key: key})
}

// WithoutSelf 以 nil 载荷遮蔽 ctx 上的实体身份（ReenterAfter 的 wctx 派生用
// ——让出期间 loop 空闲不构成等待边，出站调用的自身哈希追加以身份在场为
// 判据）。SelfFrom 读到遮蔽返回 false。
func WithoutSelf(ctx context.Context) context.Context {
	return context.WithValue(ctx, keySelf, (*self)(nil))
}

// SelfFrom 读出 ctx 上的实体身份。
func SelfFrom(ctx context.Context) (kind, key string, ok bool) {
	s, ok := ctx.Value(keySelf).(*self)
	if !ok || s == nil {
		return "", "", false
	}
	return s.kind, s.key, true
}

// WithIncarnation 把当前实例的纪元（fencing 令牌）挂进 ctx。
func WithIncarnation(ctx context.Context, inc uint64) context.Context {
	return context.WithValue(ctx, keyIncarnation, inc)
}

// IncarnationFrom 读出 ctx 上的纪元；0 值挂载与未挂载同义（0 不可观测），
// 一律返回 false。
func IncarnationFrom(ctx context.Context) (uint64, bool) {
	v, ok := ctx.Value(keyIncarnation).(uint64)
	if !ok || v == 0 {
		return 0, false
	}
	return v, true
}

// WithClient 把调用面挂进 ctx（运行时不透明载荷，类型化访问器在 rans）。
func WithClient(ctx context.Context, c any) context.Context {
	return context.WithValue(ctx, keyClient, c)
}

// ClientFrom 读出 ctx 上的调用面载荷。
func ClientFrom(ctx context.Context) (any, bool) {
	v, ok := ctx.Value(keyClient).(any)
	return v, ok
}

// CausalPath 因果路径（H12 决策 14 v2 形态）：Root 为根 trace 标识（顶层
// 调用首次出站生成、全链共用），Path 为途经实体身份的 FNV-1a 64 哈希序列
// （等待边逐跳追加、Tell 不入链）。Path 切片只读共享，追加方复制新序列。
type CausalPath struct {
	Root string
	Path []uint64
}

// WithCausal 将因果路径挂进 ctx。p 为 nil 时等价 WithoutCausal（遮蔽）。
func WithCausal(ctx context.Context, p *CausalPath) context.Context {
	return context.WithValue(ctx, keyCausal, p)
}

// WithoutCausal 以 typed-nil 遮蔽 ctx 上的因果路径（ctx 值无法移除，遮蔽
// 即「不携带」的唯一实现）：单向消息不是等待边、不入链——投递构造点用它
// 把调用方的路径拦在目标 Handle ctx 之外（携带会诱发准入误判环，决策 15）。
func WithoutCausal(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyCausal, (*CausalPath)(nil))
}

// CausalFrom 读出 ctx 上的因果路径；未携带（或被遮蔽）返回 nil。Path 只读共享。
func CausalFrom(ctx context.Context) *CausalPath {
	p, _ := ctx.Value(keyCausal).(*CausalPath)
	return p
}

// WithDeferPoint 把请求投递的延迟应答登记点挂进 ctx（H12 决策 16 前半）：
// 运行时投递边界在 Deliver 构造 Handle ctx 时注入（仅请求形态投递携带）；
// 载荷不透明，类型化判读归 rans.Defer。
func WithDeferPoint(ctx context.Context, point any) context.Context {
	return context.WithValue(ctx, keyDefer, point)
}

// DeferPointFrom 读出 ctx 上的延迟应答登记点；未注入返回 (nil, false)。
func DeferPointFrom(ctx context.Context) (any, bool) {
	v := ctx.Value(keyDefer)
	if v == nil {
		return nil, false
	}
	return v, true
}

// WithReenterPoint 把投递的让出重入执行面挂进 ctx（H12 决策 16 后半）：投递
// 边界在 Deliver 注入，请求与单向都携带（Tell 形态的 ReenterAfter 合法）。
func WithReenterPoint(ctx context.Context, arm any) context.Context {
	return context.WithValue(ctx, keyReenter, arm)
}

// ReenterPointFrom 读出 ctx 上的让出重入执行面；未注入返回 (nil, false)。
func ReenterPointFrom(ctx context.Context) (any, bool) {
	v := ctx.Value(keyReenter)
	if v == nil {
		return nil, false
	}
	return v, true
}
