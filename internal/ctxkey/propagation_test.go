package ctxkey_test

// 传播键登记表与打包/注入原语（H10 决策 8）：登记 panic 面（重复同名、
// 非具名键、nil 编解码）、护栏语义（单值 `>` 丢弃不占合计额度、合计触发
// 后逐键丢弃）、本地同语义重建（decode 产物遮蔽原值、编解码失败跳过）、
// 收端注入（未登记名/解码失败跳过留痕）。护栏的全量边界（恰限额不丢、
// 触发后后续键全弃、计数可见）由 testkit 护栏验收钉住。

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cyinx/rans_golang/internal/ctxkey"
)

type regKeyA struct{}
type regKeyB struct{}
type regKeyC struct{}
type regKeyD struct{}
type regKeyE struct{}
type regKeyF struct{}
type regCap1 struct{}
type regCap2 struct{}
type regCap3 struct{}
type regCap4 struct{}
type regCap5 struct{}
type regCap6 struct{}
type regCap7 struct{}
type regCap8 struct{}
type regCap9 struct{}

func regIdentity(v any) ([]byte, error) { return v.([]byte), nil }
func regBytes(b []byte) (any, error)    { return append([]byte(nil), b...), nil }

// regPadEncode 恒产恰 4096 字节（单值护栏下界）：9 键即触达合计护栏的
// 最小形态（8×4096 恰满、第 9 键触发）。
func regPadEncode(any) ([]byte, error) { return make([]byte, 4096), nil }

func TestRegisterPropagationDuplicateNamePanics(t *testing.T) {
	ctxkey.RegisterPropagation(regKeyA{}, regIdentity, regBytes)
	defer func() {
		if recover() == nil {
			t.Fatal("重复登记同名必须 panic（编程错误）")
		}
	}()
	ctxkey.RegisterPropagation(regKeyA{}, regIdentity, regBytes)
}

func TestRegisterPropagationInvalidKeyPanics(t *testing.T) {
	for name, key := range map[string]any{
		"nil key":        nil,
		"builtin string": "str-key",
		"builtin bytes":  []byte("b"),
		"pointer key":    &regKeyA{},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: 必须 panic（登记名不稳定）", name)
				}
			}()
			ctxkey.RegisterPropagation(key, regIdentity, regBytes)
		}()
	}
}

func TestRegisterPropagationNilCodecPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("nil encode/decode 必须 panic")
		}
	}()
	ctxkey.RegisterPropagation(regKeyB{}, nil, regBytes)
}

func TestPackPropagationForWireValueCap(t *testing.T) {
	ctxkey.RegisterPropagation(regKeyC{}, regIdentity, regBytes)
	ctxkey.RegisterPropagation(regKeyD{}, regIdentity, regBytes)

	ctx := context.Background()
	ctx = context.WithValue(ctx, regKeyC{}, make([]byte, 5000)) // 超单值上限
	ctx = context.WithValue(ctx, regKeyD{}, make([]byte, 4096)) // 恰单值上限：不丢
	items, drops := ctxkey.PackPropagationForWire(ctx)
	if len(items) != 1 || items[0].Name != "ctxkey_test.regKeyD" || len(items[0].Value) != 4096 {
		t.Fatalf("items = %+v, want 仅 regKeyD 4096 字节", items)
	}
	if len(drops) != 1 || drops[0].Name != "ctxkey_test.regKeyC" || drops[0].TotalCap {
		t.Fatalf("drops = %+v, want 仅 regKeyC 的单值护栏丢弃", drops)
	}
}

func TestPackPropagationForWireTotalCapDropsFollowing(t *testing.T) {
	keys := []any{regCap1{}, regCap2{}, regCap3{}, regCap4{}, regCap5{}, regCap6{}, regCap7{}, regCap8{}, regCap9{}}
	for _, k := range keys {
		ctxkey.RegisterPropagation(k, regPadEncode, regBytes)
	}
	ctx := context.Background()
	for _, k := range keys {
		ctx = context.WithValue(ctx, k, "v")
	}
	items, drops := ctxkey.PackPropagationForWire(ctx)
	if len(items) != 8 {
		t.Fatalf("items = %d 项, want 8（8×4096 恰满合计上限）", len(items))
	}
	if items[0].Name != "ctxkey_test.regCap1" || items[7].Name != "ctxkey_test.regCap8" {
		t.Fatalf("items 首尾 = %s/%s, want regCap1/regCap8（登记序）", items[0].Name, items[7].Name)
	}
	if len(drops) != 1 || drops[0].Name != "ctxkey_test.regCap9" || !drops[0].TotalCap {
		t.Fatalf("drops = %+v, want 仅 regCap9 的合计护栏丢弃", drops)
	}
}

func TestRebuildPropagationShadowsWithDecoded(t *testing.T) {
	ctxkey.RegisterPropagation(regKeyB{}, func(v any) ([]byte, error) {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("want string, got %T", v)
		}
		return []byte(s), nil
	}, func(b []byte) (any, error) {
		return "decoded:" + string(b), nil
	})

	ctx := context.WithValue(context.Background(), regKeyB{}, "raw")
	out := ctxkey.RebuildPropagation(ctx)
	if v, _ := out.Value(regKeyB{}).(string); v != "decoded:raw" {
		t.Fatalf("重建后值 = %q, want decode 产物（外层遮蔽原值）", v)
	}
}

func TestRebuildPropagationSkipsOnCodecFailure(t *testing.T) {
	ctxkey.RegisterPropagation(regKeyE{}, func(any) ([]byte, error) {
		return nil, errors.New("boom")
	}, regBytes)
	ctx := context.WithValue(context.Background(), regKeyE{}, "x")
	if v, ok := ctx.Value(regKeyE{}).(string); !ok || v != "x" {
		t.Fatalf("编码失败键必须按仅本地键残留, got %v ok=%v", v, ok)
	}
}

func TestInjectPropagationSkipsUnknownAndFailures(t *testing.T) {
	ctxkey.RegisterPropagation(regKeyF{}, regIdentity, func(b []byte) (any, error) {
		if len(b) == 0 {
			return nil, errors.New("empty")
		}
		return string(b), nil
	})
	ctx := context.Background()
	ctx, skipped := ctxkey.InjectPropagation(ctx, []ctxkey.PropItem{
		{Name: "ctxkey_test.regKeyF", Value: []byte("v")},
		{Name: "future.newkey", Value: []byte("x")}, // 未登记名
		{Name: "ctxkey_test.regKeyF", Value: nil},   // 解码失败
	})
	if skipped != 2 {
		t.Fatalf("skipped = %d, want 2", skipped)
	}
	if v, _ := ctx.Value(regKeyF{}).(string); v != "v" {
		t.Fatalf("注入值 = %q, want v", v)
	}
}
