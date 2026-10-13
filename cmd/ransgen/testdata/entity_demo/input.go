// Package entitydemo 是 ransgen 实体/代理生成器的行为 fixture：真实包
// （手写 input.go + 生成入库 rans_gen.go）。Counter 实体覆盖三形态方法
// （(T, error)/(error)/OneWay）+ 用户手写生命周期方法与手写请求类型共存
// 规则。testdata 不进 ./... 常规遍历，由 ransgen 的 entity_test/proxy_test
// 以显式路径 `go test ./testdata/entity_demo` 驱动。
package entitydemo

import (
	"context"
	"encoding/binary"
	"fmt"
)

// Counter 计数实体（kind=counter）：本地状态 n 与激活标志 alive。
//
//rans:kind=counter
type Counter struct {
	n     int64 // 本地状态：无 order tag，不进消息面
	alive bool
}

// Activate 用户手写生命周期方法：wrapper 委托到此。
func (c *Counter) Activate(ctx context.Context) error {
	c.alive = true
	return nil
}

// Deactivate 用户手写生命周期方法：wrapper 委托到此。
func (c *Counter) Deactivate(ctx context.Context) error {
	c.alive = false
	return nil
}

// Add (T, error) 形态：累加并返回新值。
func (c *Counter) Add(ctx context.Context, delta int64) (int64, error) {
	c.n += delta
	return c.n, nil
}

// Put (T, error) 形态 + //rans:idempotent 方法注解（规格 §9.2）：
// 绝对值设置天然可重放——生成代理默认追加 rans.Idempotent()，模糊投递
// 失败由重试环自动重放；未注解方法只透传调用方选项。（方法名不用 Set：
// 避开「配置 Set 面」的门禁 grep（'func .*Set[A-Z]'）——本方法是实体业务
// 方法，不是运行时配置注入。）
//
//rans:idempotent
func (c *Counter) Put(ctx context.Context, to int64) (int64, error) {
	c.n = to
	return c.n, nil
}

// Reset (error) 形态：请求类型 CounterResetRequest 由用户手写（发现规则：
// 同名类型不再生成，字段按 order 对位）。
func (c *Counter) Reset(ctx context.Context, to int64) error {
	c.n = to
	return nil
}

// Ping OneWay 形态（无返回值）：单向清零。
func (c *Counter) Ping(ctx context.Context) {
	c.n = 0
}

// Bump (error) 形态：参数全未命名——派生请求字段按参数位合成 ArgN
// （order=1 ↔ 第一个参数，字段名 Arg1）。
func (c *Counter) Bump(context.Context, int64) error {
	c.n++
	return nil
}

// CounterResetRequest 手写请求类型：字段名 Val 与方法参数名 to 不同——
// 分发/代理构造按 order 对位（order=1 ↔ 第一个参数），不要求同名；
// 额外字段 Who 超出参数集，分发忽略。
type CounterResetRequest struct {
	Val int64  `rans:"order=1"`
	Who string `rans:"order=2"`
}

// Snapshot 快照能力面（rans.Snapshotter wrapper 透传的行为载体）：
// n 8 字节大端 + alive 1 字节。
func (c *Counter) Snapshot() ([]byte, error) {
	b := make([]byte, 9)
	binary.BigEndian.PutUint64(b, uint64(c.n))
	if c.alive {
		b[8] = 1
	}
	return b, nil
}

// Restore 快照恢复：形状不符报错（rans.Snapshotter 契约
// ——不得 panic 或静默截断，上层据此退化兜底）。
func (c *Counter) Restore(b []byte) error {
	if len(b) != 9 {
		return fmt.Errorf("counter: bad snapshot shape %d", len(b))
	}
	c.n = int64(binary.BigEndian.Uint64(b))
	c.alive = b[8] == 1
	return nil
}
