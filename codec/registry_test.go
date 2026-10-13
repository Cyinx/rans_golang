package codec

import (
	"errors"
	"testing"
)

func TestRegistryBuiltinScalars(t *testing.T) {
	for _, v := range []any{42, int64(-7), uint64(1 << 40), "s", true, 3.5, []byte{1, 2}} {
		name, payload, err := Encode(v)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Decode(name, payload)
		if err != nil {
			t.Fatal(err)
		}
		switch v.(type) {
		case []byte:
			b := v.([]byte)
			g := got.([]byte)
			if len(g) != len(b) {
				t.Fatalf("[]byte %v != %v", g, b)
			}
		default:
			if got != v {
				t.Fatalf("%v: got %v (%T)", v, got, got)
			}
		}
	}
}

type demoMsg struct {
	N int64
	S string
}

// 手写序列化器（与生成器产物同构的字面量构造形态）。
func TestRegistryCustomType(t *testing.T) {
	Register("demoMsg", demoMsg{}, Serializer{
		Encode: func(v any) ([]byte, error) {
			m := v.(demoMsg)
			w := NewWriter()
			w.BeginObject(2)
			w.WriteFieldVarInt(1, m.N)
			w.WriteFieldString(2, m.S)
			return w.Bytes(), nil
		},
		Decode: func(b []byte) (any, error) {
			r := NewReader(b)
			count, err := r.BeginObject()
			if err != nil {
				return nil, err
			}
			var m demoMsg
			for i := 0; i < count; i++ {
				id, wt, _ := r.ReadFieldTag()
				switch {
				case id == 1 && wt == WireVarInt:
					m.N, _ = r.ReadVarInt()
				case id == 2 && wt == WireString:
					m.S, _ = r.ReadString()
				default:
					if err := r.SkipField(wt); err != nil {
						return nil, err
					}
				}
			}
			return m, nil
		},
	})
	name, payload, err := Encode(demoMsg{N: 5, S: "x"})
	if err != nil || name != "demoMsg" {
		t.Fatalf("name=%q err=%v", name, err)
	}
	got, err := Decode(name, payload)
	if err != nil || got.(demoMsg).S != "x" || got.(demoMsg).N != 5 {
		t.Fatalf("got %#v err %v", got, err)
	}
}

// TestRegistryUnregisteredHardError：未注册类型/未知名字一律硬错
// （ErrUnregistered），不落无类型兜底解码——错位在编解码点即报，
// 而非延迟到对端断言。
func TestRegistryUnregisteredHardError(t *testing.T) {
	type adhoc struct{ A int }
	if _, _, err := Encode(adhoc{A: 3}); !errors.Is(err, ErrUnregistered) {
		t.Fatalf("encode unregistered: err=%v, want ErrUnregistered", err)
	}
	if _, err := Decode("never.registered", []byte(`{"A":3}`)); !errors.Is(err, ErrUnregistered) {
		t.Fatalf("decode unknown name: err=%v, want ErrUnregistered", err)
	}
}

// TestRegisterValueIdempotentSameEntry 同名同类型重复注册幂等（防御性双
// 注册不误杀），异体冲突见下。
func TestRegisterValueIdempotentSameEntry(t *testing.T) {
	type idemType struct{ X int }
	ser := Serializer{Encode: func(v any) ([]byte, error) { return nil, nil }}
	Register("idem.same", idemType{}, ser)
	Register("idem.same", idemType{}, ser) // 名与类型都相同：幂等
	if _, _, err := Encode(idemType{}); err != nil {
		t.Fatalf("idempotent re-register must not break: %v", err)
	}
}

// TestRegisterValueNameConflictPanics：跨包同名异体注册必须 fail-fast
// panic（注册是 init 期动作）——静默覆盖即后写赢，先注册侧的值会按错误
// 的序列化器解型。
func TestRegisterValueNameConflictPanics(t *testing.T) {
	type first struct{ X int }
	type other struct{ Y int }
	ser := Serializer{Encode: func(v any) ([]byte, error) { return nil, nil }}
	Register("conflict.name", first{}, ser)
	defer func() {
		if recover() == nil {
			t.Fatal("same name + different type must panic at registration")
		}
	}()
	Register("conflict.name", other{}, ser)
}

// TestRegisterValueTypeTwoNamesPanics：同一类型注册两个名字必须 fail-fast
// panic——两端按不同名编码互不相认。
func TestRegisterValueTypeTwoNamesPanics(t *testing.T) {
	type dual struct{ X int }
	ser := Serializer{Encode: func(v any) ([]byte, error) { return nil, nil }}
	Register("conflict.a", dual{}, ser)
	defer func() {
		if recover() == nil {
			t.Fatal("same type + different names must panic at registration")
		}
	}()
	Register("conflict.b", dual{}, ser)
}
