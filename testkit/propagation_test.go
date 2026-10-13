package testkit

// 传播键两端可见与护栏（H10 决策 8，缝合点 rans.RegisterPropagation +
// 实体 Handle ctx 读键）：登记键本地/远端同语义可见（Handle 可见值 =
// decode(encode(调用方值))——INV6 位置透明），未登记键仅本地（远端不可
// 见）；护栏（单值 4KiB/合计 32KiB）超限丢键不使调用失败、计数可见。
// 突变自证锚：custom 键的 decode 产物为指针形态——本地若退化为调用方
// ctx 全透传（回填缺失），值形态断言即红。

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// ---- 键与编解码（测试私有键类型——internal/ctxkey 键惯例的消费方形态）----

type propStringKey struct{}
type propCustomKey struct{}
type propLocalKey struct{} // 不登记（仅本地对照）

type propBigKey struct{}
type propCapAKey struct{}
type propCapBKey struct{}
type propCapCKey struct{}
type propCapDKey struct{}
type propCapEKey struct{}
type propCapFKey struct{}
type propCapGKey struct{}
type propCapHKey struct{}
type propCapIKey struct{}
type propCapJKey struct{}

// propCustomPayload 自定义类型传播值：encode 序列化 N+S，decode 还原为
// *propCustomPayload（指针产物——「可见值 = decode 产物形态」契约的
// 观测面，全透传突变下本地可见原值形态、断言即红）。
type propCustomPayload struct {
	N int32
	S string
}

func propEncodeString(v any) ([]byte, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("prop: string value is %T", v)
	}
	return []byte(s), nil
}

func propDecodeString(b []byte) (any, error) { return string(b), nil }

func propEncodeCustom(v any) ([]byte, error) {
	p, ok := v.(propCustomPayload)
	if !ok {
		return nil, fmt.Errorf("prop: custom value is %T", v)
	}
	b := make([]byte, 4, 4+len(p.S))
	binary.LittleEndian.PutUint32(b, uint32(p.N))
	return append(b, p.S...), nil
}

func propDecodeCustom(b []byte) (any, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("prop: custom payload short: %d bytes", len(b))
	}
	return &propCustomPayload{
		N: int32(binary.LittleEndian.Uint32(b)),
		S: string(b[4:]),
	}, nil
}

func propEncodeBytes(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("prop: bytes value is %T", v)
	}
	return b, nil
}

func propDecodeBytes(b []byte) (any, error) { return append([]byte(nil), b...), nil }

// ---- 观测实体 ----

// propSaw 一次 Handle 的读键观测（传播键集合 + 未登记键可见性）。
type propSaw struct {
	Str      string
	StrOK    bool
	Custom   propCustomPayload
	CustomOK bool
	LocalOK  bool
}

type propProbe struct {
	mu  sync.Mutex
	saw propSaw
}

func (p *propProbe) Handle(ctx context.Context, _ any) (any, error) {
	s := propSaw{}
	if v, ok := ctx.Value(propStringKey{}).(string); ok {
		s.Str, s.StrOK = v, true
	}
	if cp, ok := ctx.Value(propCustomKey{}).(*propCustomPayload); ok {
		s.Custom, s.CustomOK = *cp, true
	}
	if _, ok := ctx.Value(propLocalKey{}).(string); ok {
		s.LocalOK = true
	}
	p.mu.Lock()
	p.saw = s
	p.mu.Unlock()
	return "ok", nil
}

func (p *propProbe) Activate(context.Context) error   { return nil }
func (p *propProbe) Deactivate(context.Context) error { return nil }

func propSawOf(t *testing.T, p *propProbe) propSaw {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.saw
}

// propCapProbe 护栏观测：Handle 入口记录各登记键的可见字节长度（缺位
// = 键未到达）。
type propCapProbe struct {
	mu   sync.Mutex
	seen map[string]int
}

func (p *propCapProbe) Handle(ctx context.Context, _ any) (any, error) {
	s := map[string]int{}
	for _, e := range []struct {
		key  any
		name string
	}{
		{propBigKey{}, "big"},
		{propCapAKey{}, "A"},
		{propCapBKey{}, "B"},
		{propCapCKey{}, "C"},
		{propCapDKey{}, "D"},
		{propCapEKey{}, "E"},
		{propCapFKey{}, "F"},
		{propCapGKey{}, "G"},
		{propCapHKey{}, "H"},
		{propCapIKey{}, "I"},
		{propCapJKey{}, "J"},
	} {
		if b, ok := ctx.Value(e.key).([]byte); ok {
			s[e.name] = len(b)
		}
	}
	p.mu.Lock()
	p.seen = s
	p.mu.Unlock()
	return "ok", nil
}

func (p *propCapProbe) Activate(context.Context) error   { return nil }
func (p *propCapProbe) Deactivate(context.Context) error { return nil }

func (p *propCapProbe) seenOf(t *testing.T) map[string]int {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seen
}

// propCallCtx 调用方 ctx：预算 + 全部测试键值（cancel 挂测试收场）。
func propCallCtx(t *testing.T, values map[any]any) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	for k, v := range values {
		ctx = context.WithValue(ctx, k, v)
	}
	return ctx
}

// ---- 验收 ----

// TestPropagation_KeysVisibleBothPaths 登记键本地/远端同语义可见（INV6）：
// 两个传播键（string 值 + 自定义类型经 encode/decode）在两条路径的
// Handle ctx 可见且值等价（custom 以 decode 产物指针形态），未登记键
// 本地可见、远端不可见（仅本地语义）。
func TestPropagation_KeysVisibleBothPaths(t *testing.T) {
	rans.RegisterPropagation(propStringKey{}, propEncodeString, propDecodeString)
	rans.RegisterPropagation(propCustomKey{}, propEncodeCustom, propDecodeCustom)

	p := &propProbe{}
	c := NewCluster(t, 2, map[string]rans.Factory{"probe": func(_ rans.ID) rans.Entity { return p }})
	id := mustID(t, "probe", "prop-both")

	// 预热：节点 1 本地激活（testkit 缺省放置 miss → 本地收口），此后节点
	// 0 的同 kind 调用经目录命中节点 1 走远端腿（call_wire_budget 同款）。
	if _, err := c.Node(1).Call(context.Background(), id, nil); err != nil {
		t.Fatal(err)
	}

	values := map[any]any{
		propStringKey{}: "wire-1",
		propCustomKey{}: propCustomPayload{N: 7, S: "x"},
		propLocalKey{}:  "local-only",
	}
	wantCustom := propCustomPayload{N: 7, S: "x"}

	t.Run("local_path", func(t *testing.T) {
		if _, err := c.Node(1).Call(propCallCtx(t, values), id, nil); err != nil {
			t.Fatal(err)
		}
		saw := propSawOf(t, p)
		if !saw.StrOK || saw.Str != "wire-1" {
			t.Fatalf("本地 string 传播键 = %q ok=%v, want wire-1", saw.Str, saw.StrOK)
		}
		if !saw.CustomOK || saw.Custom != wantCustom {
			t.Fatalf("本地 custom 传播键 = %+v ok=%v, want %+v（decode 产物指针形态）",
				saw.Custom, saw.CustomOK, wantCustom)
		}
		if !saw.LocalOK {
			t.Fatal("未登记键本地必须可见（仅本地键语义）")
		}
	})

	t.Run("remote_path", func(t *testing.T) {
		if _, err := c.Node(0).Call(propCallCtx(t, values), id, nil); err != nil {
			t.Fatal(err)
		}
		saw := propSawOf(t, p)
		if !saw.StrOK || saw.Str != "wire-1" {
			t.Fatalf("远端 string 传播键 = %q ok=%v, want wire-1", saw.Str, saw.StrOK)
		}
		if !saw.CustomOK || saw.Custom != wantCustom {
			t.Fatalf("远端 custom 传播键 = %+v ok=%v, want %+v（解码注入）",
				saw.Custom, saw.CustomOK, wantCustom)
		}
		if saw.LocalOK {
			t.Fatal("未登记键不得过线（远端不可见）")
		}
	})
}

// TestPropagation_SizeCaps 护栏（H10 决策 8）：单值超 4KiB 丢该键不使调
// 用失败；合计超 32KiB 起触发键及其余登记键全部丢弃（登记序先到先得），
// 恰在限额上的单值/合计不丢（`>` 边界语义两面都钉）；丢弃计数可见（发
// 送侧节点指标）。本地无过线动作——在场登记键全部参与同语义重建（护栏
// 护线）。
func TestPropagation_SizeCaps(t *testing.T) {
	for _, k := range []any{
		propBigKey{}, propCapAKey{}, propCapBKey{}, propCapCKey{}, propCapDKey{},
		propCapEKey{}, propCapFKey{}, propCapGKey{}, propCapHKey{}, propCapIKey{}, propCapJKey{},
	} {
		rans.RegisterPropagation(k, propEncodeBytes, propDecodeBytes)
	}

	p := &propCapProbe{}
	c := NewCluster(t, 2, map[string]rans.Factory{"probe": func(_ rans.ID) rans.Entity { return p }})
	id := mustID(t, "probe", "prop-caps")

	if _, err := c.Node(1).Call(context.Background(), id, nil); err != nil {
		t.Fatal(err)
	}

	values := map[any]any{
		propBigKey{}:  make([]byte, 4097), // 超单值上限：丢弃（不占合计额度）
		propCapAKey{}: make([]byte, 4096), // 恰单值上限：不丢
		propCapBKey{}: make([]byte, 4096),
		propCapCKey{}: make([]byte, 4096),
		propCapDKey{}: make([]byte, 4096), // A..H = 32KiB 恰合计上限：全部过线
		propCapEKey{}: make([]byte, 4096),
		propCapFKey{}: make([]byte, 4096),
		propCapGKey{}: make([]byte, 4096),
		propCapHKey{}: make([]byte, 4096),
		propCapIKey{}: make([]byte, 4096), // 触发合计护栏：丢弃
		propCapJKey{}: make([]byte, 1024), // 触发后的后续键：丢弃
	}

	// 远端腿：调用成功（不使调用失败），丢弃键不可见、在额键可见。
	if _, err := c.Node(0).Call(propCallCtx(t, values), id, nil); err != nil {
		t.Fatalf("超限丢弃不得使调用失败: %v", err)
	}
	seen := p.seenOf(t)
	for _, name := range []string{"A", "B", "C", "D", "E", "F", "G", "H"} {
		if seen[name] != 4096 {
			t.Fatalf("在额键 %s = %d 字节, want 4096（恰限额不丢）", name, seen[name])
		}
	}
	for _, name := range []string{"big", "I", "J"} {
		if n, ok := seen[name]; ok {
			t.Fatalf("超限键 %s 远端可见 %d 字节, want 不可见", name, n)
		}
	}

	// 本地腿：无过线动作，在场登记键全部同语义重建（超限键不降级）。
	if _, err := c.Node(1).Call(propCallCtx(t, values), id, nil); err != nil {
		t.Fatal(err)
	}
	seen = p.seenOf(t)
	wantLocal := map[string]int{
		"big": 4097, "A": 4096, "B": 4096, "C": 4096, "D": 4096, "E": 4096,
		"F": 4096, "G": 4096, "H": 4096, "I": 4096, "J": 1024,
	}
	for name, want := range wantLocal {
		if seen[name] != want {
			t.Fatalf("本地重建 %s = %d 字节, want %d（护栏护线不护本地）", name, seen[name], want)
		}
	}

	// 计数可见（发送侧 = 节点 0 指标）：单值护栏 ≥1、合计护栏 ≥2。
	for _, e := range []struct {
		name string
		min  float64
	}{{"rans.prop.value_dropped", 1}, {"rans.prop.total_dropped", 2}} {
		propWaitCounter(t, c, e.name, e.min)
	}
}

// propWaitCounter 轮询等待节点指标计数到达下界（计数落点在发送收场路径，
// 与读数方异步）。
func propWaitCounter(t *testing.T, c *Cluster, name string, min float64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		for _, m := range c.Node(0).Node.Metrics() {
			if m.Name == name && m.Value >= min {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("指标 %s 未到达 %v（节点 0 读数无该计数或偏小）", name, min)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
