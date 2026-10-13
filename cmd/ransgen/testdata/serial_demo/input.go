// Package serialdemo 是 ransgen 序列化器生成器的行为 fixture：真实包
// （手写 input.go + 生成入库 rans_gen.go），覆盖全部支持字段形态。
// testdata 不进 ./... 常规遍历，由 ransgen 的 serializer_test 以显式路径
// `go test ./testdata/serial_demo` 驱动（编译验证 + 往返/演进锁一肩挑）。
package serialdemo

// Alpha 前向引用 Zeta：模板按消息名排序输出，Alpha 的助手先于 Zeta 落盘，
// 锁死"跨结构引用与注册顺序无关"（包级声明顺序自由）。
type Alpha struct {
	Ref Zeta `rans:"order=1"`
}

// Zeta 被嵌套引用的消息（默认名=类型名）。
type Zeta struct {
	V int64 `rans:"order=1"`
}

// Point 嵌套消息：既作字段值（Shape.Origin）也作序列元素（Shape.Coords）。
type Point struct {
	X int64 `rans:"order=1"`
	Y int64 `rans:"order=2"`
}

// Shape 全形态消息（显式命名 serial.Shape）：六种标量 + []byte + 嵌套消息 +
// 各类元素切片（含 [][]byte 与 []Point）。
//
//rans:name=serial.Shape
type Shape struct {
	Label   string    `rans:"order=1"`
	Count   int       `rans:"order=2"`
	Max     uint64    `rans:"order=3"`
	Ratio   float64   `rans:"order=4"`
	On      bool      `rans:"order=5"`
	Blob    []byte    `rans:"order=6"`
	Origin  Point     `rans:"order=7"`
	Tags    []string  `rans:"order=8"`
	Coords  []Point   `rans:"order=9"`
	Flags   []bool    `rans:"order=10"`
	Matrix  [][]byte  `rans:"order=11"`
	Weights []float64 `rans:"order=12"`
	Ids     []uint64  `rans:"order=13"`
}
