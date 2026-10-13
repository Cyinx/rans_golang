// 传播键登记与过线面（H10 决策 8，形态 A）：请求级 ctx 值分两类——登记的
// 传播键（随调用过线、本地远端同语义可见）与未登记的仅本地键（永不序列化，
// 宿主本地授权状态即此形态）。键本体沿用本包键惯例（具名类型
// 实例，运行时注入键的键类型不导出、天然不可登记）；过线以键类型字符串为
// 登记名标识，值经登记的编解码函数序列化。登记面在 rans 契约包
//（RegisterPropagation 薄包装），本包持登记表与打包/注入原语。

package ctxkey

import (
	"context"
	"reflect"
	"sync"
)

// 护栏（H10 决策 8）：过线值的尺寸上限。超限丢弃该键 + 计数告警，不使
// 调用失败；护栏护的是线——本地同语义重建不设限（无过线动作）。
const (
	// MaxPropValueBytes 单值上限（4 KiB）：编码产物超限的键不过线，
	// 且不占用单请求合计额度。
	MaxPropValueBytes = 4 << 10
	// MaxPropTotalBytes 单请求合计上限（32 KiB）：按登记序逐键累计，
	// 触发后该键及其余在场登记键一律不再过线（先到先得）。
	MaxPropTotalBytes = 32 << 10
)

// Propagation 一个传播键的登记项：Name 是过线标识（键类型字符串），Key
// 是 ctx 键本体，Encode/Decode 是值的线编解码。值形态契约：Handle ctx
// 可见值 = Decode(Encode(调用方值))——本地同语义重建与远端解码注入共守
// 此形态（decode 产物可为与原值不同的 Go 形态，消费方读写两侧以 decode
// 产物为准）。
type Propagation struct {
	Name   string
	Key    any
	Encode func(v any) ([]byte, error)
	Decode func(b []byte) (any, error)
}

var (
	propMu   sync.Mutex
	propRegs []Propagation
)

// RegisterPropagation 全局登记一个传播键：登记后调用方 ctx 上的键值随
// 调用过线（远端解码注入）并参与本地投递的同语义重建；未登记的键 = 仅
// 本地（不序列化、远端不可见）。重复登记同名（同键类型）、key 为 nil 或
// 无包路径的类型实例（内建类型/匿名类型——登记名不稳定）、encode/decode
// 为 nil 时 panic（编程错误）。并发安全（登记表只增）。
func RegisterPropagation(key any, encode func(v any) ([]byte, error), decode func(b []byte) (any, error)) {
	t := reflect.TypeOf(key)
	if key == nil || t == nil || t.PkgPath() == "" {
		panic("rans: RegisterPropagation: key must be a non-nil value of a package-defined named type")
	}
	if encode == nil || decode == nil {
		panic("rans: RegisterPropagation: encode and decode must be non-nil")
	}
	p := Propagation{Name: t.String(), Key: key, Encode: encode, Decode: decode}
	propMu.Lock()
	defer propMu.Unlock()
	for _, r := range propRegs {
		if r.Name == p.Name {
			panic("rans: RegisterPropagation: duplicate propagation name " + p.Name)
		}
	}
	propRegs = append(propRegs, p)
}

// Propagations 登记快照（登记序，打包顺序即此序）。返回副本，调用方可
// 持有；登记项本身不可变。
func Propagations() []Propagation {
	propMu.Lock()
	defer propMu.Unlock()
	return append([]Propagation(nil), propRegs...)
}

// propByName 按过线名查登记项（值拷贝）。
func propByName(name string) (Propagation, bool) {
	propMu.Lock()
	defer propMu.Unlock()
	for _, r := range propRegs {
		if r.Name == name {
			return r, true
		}
	}
	return Propagation{}, false
}

// PropItem 一个待过线的键值项（Name 为登记名，Value 为 encode 产物）。
type PropItem struct {
	Name  string
	Value []byte
}

// PropDrop 一次过线丢弃事件（护栏告警面）：TotalCap 区分单值护栏（编码
// 失败或产物超单值上限）与合计护栏（合计超限触发后的丢弃）；Name 供
// 日志定位。
type PropDrop struct {
	Name     string
	TotalCap bool
}

// PackPropagationForWire 发送侧过线打包（远端腿）：按登记序逐键从 ctx
// 取值、编码、过护栏——编码失败或产物超单值上限的键丢弃；合计超限起该
// 键及其余在场登记键全部丢弃。返回过线条目与丢弃明细（计数告警归调用
// 方）。
func PackPropagationForWire(ctx context.Context) (items []PropItem, drops []PropDrop) {
	regs := Propagations()
	if len(regs) == 0 || ctx == nil {
		return nil, nil
	}
	var (
		total int
		full  bool
	)
	for _, r := range regs {
		v := ctx.Value(r.Key)
		if v == nil {
			continue // 未挂载：不携带（无告警——非在场即非请求级数据）
		}
		if full {
			drops = append(drops, PropDrop{Name: r.Name, TotalCap: true})
			continue
		}
		b, err := r.Encode(v)
		if err != nil || len(b) > MaxPropValueBytes {
			drops = append(drops, PropDrop{Name: r.Name})
			continue
		}
		if total+len(b) > MaxPropTotalBytes {
			full = true
			drops = append(drops, PropDrop{Name: r.Name, TotalCap: true})
			continue
		}
		total += len(b)
		items = append(items, PropItem{Name: r.Name, Value: b})
	}
	return items, drops
}

// RebuildPropagation 本地同语义重建（H10 决策 8：本地 Handle ctx 与远端
// 同语义，INV6）：逐登记键取值经 encode→decode 回填（外层遮蔽调用方原
// 值——Handle 可见值与远端解码注入同形态）。不设护栏（护栏护线，本地
// 无过线动作，在场登记键全部参与重建）；编码或解码失败的键跳过（原值
// 按仅本地键残留——消费方编解码缺陷，两路径可见性不保证一致）。
func RebuildPropagation(ctx context.Context) context.Context {
	regs := Propagations()
	if len(regs) == 0 || ctx == nil {
		return ctx
	}
	for _, r := range regs {
		v := ctx.Value(r.Key)
		if v == nil {
			continue
		}
		b, err := r.Encode(v)
		if err != nil {
			continue
		}
		dv, err := r.Decode(b)
		if err != nil || dv == nil {
			continue
		}
		ctx = context.WithValue(ctx, r.Key, dv)
	}
	return ctx
}

// InjectPropagation 收端注入（远端腿）：逐项按登记名查表解码并以原键
// 回填 ctx。未登记名（对端新版本新增键）与解码失败项跳过，返回跳过数
// 供留痕。items 为空原样返回。
func InjectPropagation(ctx context.Context, items []PropItem) (context.Context, int) {
	if len(items) == 0 {
		return ctx, 0
	}
	skipped := 0
	for _, it := range items {
		r, ok := propByName(it.Name)
		if !ok {
			skipped++
			continue
		}
		v, err := r.Decode(it.Value)
		if err != nil || v == nil {
			skipped++
			continue
		}
		ctx = context.WithValue(ctx, r.Key, v)
	}
	return ctx, skipped
}
