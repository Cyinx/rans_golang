package rans

import "github.com/cyinx/rans_golang/internal/ctxkey"

// RegisterPropagation init 期全局登记传播键（H10 决策 8）。请求级 ctx 值
// 分两类（CONTEXT.md「传播键 / 仅本地键」词条）：
//
//   - 传播键（本面登记）：值随调用过线，远端 Handle ctx 经解码注入可见；
//     本地投递同语义重建——Handle ctx 可见值 = decode(encode(调用方值))，
//     两路径同形态（INV6 位置透明）。值形态契约：encode 序列化调用方在
//     ctx 上挂的值，decode 还原为 Handle 侧读取形态（产物可为与原值不同
//     的 Go 形态，如指针——消费方读写两侧以 decode 产物为准）。
//   - 仅本地键（不登记）：永不序列化、远端不可见；同节点投递照常可见
//     （宿主本地状态的承载形态）。
//
// 键本体沿用 internal/ctxkey 键惯例：消费方以自有具名类型（惯例 struct{}
// 空类型）作键；运行时注入键（Self/Incarnation/Client）的键类型不导出，
// 天然不可登记。过线以键类型字符串为登记名标识。护栏：单值 4 KiB、单
// 请求合计 32 KiB，超限丢弃该键 + 计数告警（不使调用失败；护栏护线，
// 本地重建不设限）。重复登记同名（同键类型）、key 为 nil 或无包路径的
// 类型实例（内建类型/匿名类型——登记名不稳定）、encode/decode 为 nil 时
// panic（编程错误）。不阻塞；并发安全；init 期登记为主（测试期登记亦
// 安全）。
func RegisterPropagation(key any, encode func(v any) ([]byte, error), decode func(b []byte) (any, error)) {
	ctxkey.RegisterPropagation(key, encode, decode)
}
