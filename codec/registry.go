package codec

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
)

// Serializer 值级序列化器（规格 §7.3 注册表）：Encode 产负载、Decode 还值。
// 带函数字段的结构体，手写序列化器与生成器产物都用调用侧字面量构造。
type Serializer struct {
	Encode func(v any) ([]byte, error)
	Decode func(b []byte) (any, error)
}

// ErrUnregistered 未注册类型/未知名字：编码遇到未注册类型、解码遇到未知
// 名字一律返回本哨兵（不落无类型兜底解码）——类型/名字错位在编解码点即报，
// 不延迟到对端断言。
var ErrUnregistered = errors.New("rans: lookup: type or name not registered")

// valueRegistry 全局值序列化注册表，双 map：
// 名字→Serializer（解码侧按名字取）+ reflect.Type→名字（编码侧按类型取）。
// protoOf 记名字→prototype 类型（注册冲突裁决依据）。
// Register 双写（写锁），Encode/Decode 单读（读锁）。
type valueRegistry struct {
	mu      sync.RWMutex
	byName  map[string]Serializer
	byType  map[reflect.Type]string
	protoOf map[string]reflect.Type
}

var reg = valueRegistry{
	byName:  make(map[string]Serializer),
	byType:  make(map[reflect.Type]string),
	protoOf: make(map[string]reflect.Type),
}

// Register 注册值序列化器：typeName→s 与 typeof(prototype)→typeName
// 双向映射。冲突 fail-fast（panic）：注册是 init 期动作，跨包同名异体
// （静默覆盖即后写赢，先注册侧的值按错误的序列化器解码）或同一类型注册
// 两个名字（两端按不同名编码互不相认）都是编程错误，错位必须在注册点
// 即暴露。同名同类型的重复注册幂等（防御性双注册不误杀）。
// prototype 为 nil 时只建名字侧映射（解码可用；编码侧该类型无映射即硬错）。
// 不阻塞（内存查表）；并发安全（写锁）；仅限 init 期/装配期调用。
func Register(typeName string, prototype any, s Serializer) {
	t := reflect.TypeOf(prototype)
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if prev, ok := reg.protoOf[typeName]; ok {
		if prev != t {
			panic(fmt.Sprintf("rans: register: name %q already registered with prototype %v (got %v) -- cross-package name collision", typeName, prev, t))
		}
		return // 同名同类型：幂等
	}
	if t != nil {
		if prevName, ok := reg.byType[t]; ok {
			panic(fmt.Sprintf("rans: register: type %v already registered as %q (trying %q) -- one type must have one wire name", t, prevName, typeName))
		}
		reg.byType[t] = typeName
	}
	reg.protoOf[typeName] = t
	reg.byName[typeName] = s
}

// Encode 按注册表编码值：类型→名字命中→注册序列化器；
// 未命中硬错（ErrUnregistered，不做无类型兜底）。
// nil 值返回 ("", nil, nil) 空载荷语义。编码侧查表解指针：*T 用 T 的
// 序列化器（值随非空指针解引用；nil 指针不解引用，按 *T 类型查表）。
// 不阻塞（内存查表+编码）；并发安全（注册表读锁）；可在实体 Handle 内
// 调用（运行时编码路径即在其上）。
func Encode(v any) (typeName string, payload []byte, err error) {
	if v == nil {
		return "", nil, nil
	}
	val := v
	for rv := reflect.ValueOf(val); rv.Kind() == reflect.Pointer && !rv.IsNil(); rv = reflect.ValueOf(val) {
		val = rv.Elem().Interface()
	}
	t := reflect.TypeOf(val)
	reg.mu.RLock()
	name, hit := reg.byType[t]
	s := reg.byName[name]
	reg.mu.RUnlock()
	if hit {
		b, err := s.Encode(val)
		if err != nil {
			return "", nil, err
		}
		return name, b, nil
	}
	return "", nil, fmt.Errorf("%w: encode: %s (missing codec.Register or stale codegen output?)", ErrUnregistered, t)
}

// Decode 按注册表解码：名字命中→注册序列化器还类型化值；
// 未知名字硬错（ErrUnregistered，不做无类型兜底解 map）。
// ("", nil) 空载荷解为 nil，与 Encode(nil) 对称为往返同一语义。
// 不阻塞；并发安全；可在实体 Handle 内调用（对称面同 Encode）。
func Decode(typeName string, payload []byte) (any, error) {
	if typeName == "" && len(payload) == 0 {
		return nil, nil
	}
	reg.mu.RLock()
	s, hit := reg.byName[typeName]
	reg.mu.RUnlock()
	if hit {
		return s.Decode(payload)
	}
	return nil, fmt.Errorf("%w: decode: %q (peer schema drifted or stale codegen output?)", ErrUnregistered, typeName)
}

// init 内置标量注册：名字与 reflect Type.String() 一致（未注册值的错误
// 诊断名与内置名同构）。编码一律用 wire 原语。
func init() {
	Register("int", int(0), Serializer{
		Encode: func(v any) ([]byte, error) {
			w := NewWriter()
			w.WriteVarInt(int64(v.(int)))
			return w.Bytes(), nil
		},
		Decode: func(b []byte) (any, error) {
			n, err := NewReader(b).ReadVarInt()
			return int(n), err
		},
	})
	Register("int64", int64(0), Serializer{
		Encode: func(v any) ([]byte, error) {
			w := NewWriter()
			w.WriteVarInt(v.(int64))
			return w.Bytes(), nil
		},
		Decode: func(b []byte) (any, error) {
			return NewReader(b).ReadVarInt()
		},
	})
	Register("uint64", uint64(0), Serializer{
		Encode: func(v any) ([]byte, error) {
			w := NewWriter()
			w.WriteVarUInt(v.(uint64))
			return w.Bytes(), nil
		},
		Decode: func(b []byte) (any, error) {
			return NewReader(b).ReadVarUInt()
		},
	})
	Register("string", "", Serializer{
		Encode: func(v any) ([]byte, error) {
			w := NewWriter()
			w.WriteString(v.(string))
			return w.Bytes(), nil
		},
		Decode: func(b []byte) (any, error) {
			return NewReader(b).ReadString()
		},
	})
	Register("bool", false, Serializer{
		Encode: func(v any) ([]byte, error) {
			w := NewWriter()
			if v.(bool) {
				w.WriteFixed8(1)
			} else {
				w.WriteFixed8(0)
			}
			return w.Bytes(), nil
		},
		Decode: func(b []byte) (any, error) {
			n, err := NewReader(b).ReadFixed8()
			return n != 0, err
		},
	})
	Register("float64", float64(0), Serializer{
		Encode: func(v any) ([]byte, error) {
			w := NewWriter()
			w.WriteFixed64(math.Float64bits(v.(float64)))
			return w.Bytes(), nil
		},
		Decode: func(b []byte) (any, error) {
			bits, err := NewReader(b).ReadFixed64()
			return math.Float64frombits(bits), err
		},
	})
	Register("[]byte", []byte(nil), Serializer{
		Encode: func(v any) ([]byte, error) {
			w := NewWriter()
			w.WriteBytes(v.([]byte))
			return w.Bytes(), nil
		},
		Decode: func(b []byte) (any, error) {
			// 拷出而非引用输入：注册表值须独立于载荷缓冲
			//（载荷可能落在池化页上，引用即逃逸所有权纪律）。
			bs, err := NewReader(b).ReadBytes()
			if err != nil {
				return nil, err
			}
			return append([]byte(nil), bs...), nil
		},
	})
}
