// counter 演示：node 装配（Standalone 单节点无传输形态）→ 统一调用面
// Client().Call 递增，验证本地内核全链路。调用一律经路由入口——本地命中
// 走本地快路径，与远端调用同一错误形态。
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/node"
)

// counter 演示实体：每次 Handle 计数 +1 并返回累计值。
type counter struct{ n int }

// Activate 激活钩子（本实体无状态初始化，no-op）。
func (c *counter) Activate(ctx context.Context) error { return nil }

// Handle 计数请求：递增并返回累计值。
func (c *counter) Handle(ctx context.Context, _ any) (any, error) {
	c.n++
	return c.n, nil
}

// Deactivate 停用钩子（计数不落盘，随实例丢弃——演示不接 Store）。
func (c *counter) Deactivate(ctx context.Context) error { return nil }

func main() {
	n, err := node.New(node.Config{
		NodeID:     1,
		Standalone: true,
		Kinds: map[string]rans.Factory{
			"counter": func(_ rans.ID) rans.Entity { return &counter{} },
		},
	})
	if err != nil {
		panic(err)
	}
	defer n.Close()
	// 例外①（进程入口）：main 是进程根，节点根 ctx 由 Start 内部自建
	// ——§5.1 第 2 条管辖库代码，main 不在列。
	if err := n.Start(context.Background()); err != nil {
		panic(err)
	}
	id, err := rans.NewID("counter", "demo-1")
	if err != nil {
		panic(err)
	}
	// 调用方必须传带 deadline 的 ctx：无 deadline 的 Call 在吞 panic 场景
	// 会无限阻塞（决策出处：docs/decisions/2026-09-29-phase1-scope.md）。
	ctx, cancel := context.WithTimeout(context.Background() /* 例外①（进程入口）：演示进程的显式预算 */, 5*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		v, err := n.Client().Call(ctx, id, nil)
		if err != nil {
			panic(err)
		}
		fmt.Printf("counter = %v\n", v)
	}
}
