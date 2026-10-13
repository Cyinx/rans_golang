// sigtermhelper 是 kill -TERM 优雅排空演练的测试夹具（testkit/
// migration_acceptance_test.go 的 TestAcceptance_SIGTERMGracefulDrain 把本
// 目录 go build 成二进制后 exec + 发信号），同时也是**宿主接线优雅停机的
// 活样例**：单 node 组合体 + 实体，READY 后待命；收到 SIGTERM →
// Node.Shutdown（预算 5s，规格 §14.1 四段：门闸 → 并行销毁本地实体 → 出站
// flush → 停序）→ DRAINED → 退出码 0。
//
// 宿主接入 rans 时的停机接线抄本（README「四段排空」条目引用）：
//  1. node.Run 一站式（Start → 等信号 → Shutdown）；需自定义就绪标记/
//     收号面时按下述手动展开；
//  2. 收号后以 WithTimeout 构造排空预算（勿用已取消/无界的 ctx）；
//  3. Node.Shutdown 阻塞至四段完成，返回 errors.Join 的逐实体销毁错误；
//  4. Shutdown 后退出码即停机结论（非零留给销毁失败）。
//
// 卡死销毁演练：环境变量 SIGTERM_DRILL_STUCK=1 时额外激活
// 一个 Deactivate 永不返回的实体——阶段②的逐实体销毁尊重排空预算：预算
// 返回（Shutdown 带该实体的 DeadlineExceeded 错误）而**不挂死**，阶段③/④
// 照常完成（DRAINBUDGET 留痕 → DRAINED → 退出码 0）。卡死实体本身留给
// 看门狗（§14.2，本夹具不接线）或其自身设备——排空演练的契约是「停机不因
// 单实体卡死失去边界」，不是「看门狗收敛」。
//
// 装配基于 node.New 组装根（testkit 目录内无 internal 装配依赖）。
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/node"
)

// drillCell 演练实体：计数器（无快照面——排空演练只验证销毁与停序，状态
// 延续场景见 testkit 迁移/钝化验收）。
type drillCell struct{ n int }

func (c *drillCell) Activate(context.Context) error   { return nil }
func (c *drillCell) Deactivate(context.Context) error { return nil }
func (c *drillCell) Handle(context.Context, any) (any, error) {
	c.n++
	return c.n, nil
}

// stuckCell 卡死销毁实体（SIGTERM_DRILL_STUCK=1 时激活）：Deactivate 永不
// 返回——演练排空预算贯通（阶段②逐实体销毁尊重 ctx）。
type stuckCell struct{ n int }

func (c *stuckCell) Activate(context.Context) error { return nil }
func (c *stuckCell) Deactivate(context.Context) error {
	select {} // 永久挂起：卡死销毁的确定性形态（空 select 是合法终结语句）
}
func (c *stuckCell) Handle(context.Context, any) (any, error) {
	c.n++
	return c.n, nil
}

func main() {
	// 单节点装配：inmem 目录（演练不需要跨进程仲裁；多进程形态见
	// examples/arbitration）。单飞节点无对端——解析缺省自解析（其他节点号
	// 报错，不会被调用到）；无密钥运行按 fail-closed 缺省须显式声明：
	// Insecure opt-in（本地演练无集群认证面，见规格 §10.2/决策文档）。
	stuck := os.Getenv("SIGTERM_DRILL_STUCK") == "1"
	n, err := node.New(node.Config{
		NodeID:      1,
		ClusterName: "sigterm-drill",
		Kinds: map[string]rans.Factory{
			"cell":  func(_ rans.ID) rans.Entity { return &drillCell{} },
			"stuck": func(_ rans.ID) rans.Entity { return &stuckCell{} },
		},
		Directory: node.DirectoryConfig{Injected: inmem.New()},
		Security:  node.SecurityConfig{Insecure: true}, // 本地单节点演练：显式无密钥形态
		Calls:     node.CallConfig{Default: 5 * time.Second},
	})
	if err != nil {
		fail("assemble node: %v", err)
	}
	// 例外①（进程入口）：main 是进程根，节点根 ctx 由 Start 内部自建
	// ——§5.1 第 2 条管辖库代码，main 不在列。
	if err := n.Start(context.Background()); err != nil {
		fail("start node: %v", err)
	}

	// 激活一个实体（排空阶段②有活可干；调用方必须传带 deadline 的 ctx
	// ——决策文档既定约束，出处见 examples/counter）。
	id, err := rans.NewID("cell", "drill-1")
	if err != nil {
		fail("identity: %v", err)
	}
	callCtx, cancelCall := context.WithTimeout(context.Background() /* 例外①（进程入口）：演示进程的显式预算 */, 5*time.Second)
	defer cancelCall()
	if v, err := n.Client().Call(callCtx, id, nil); err != nil || v != 1 {
		fail("activation call: v=%v err=%v", v, err)
	}
	// 卡死销毁实体（可选）：同批激活——阶段②对它的销毁将在预算边界返回
	// 而非收场。
	if stuck {
		sid, err := rans.NewID("stuck", "drill-stuck")
		if err != nil {
			fail("identity: %v", err)
		}
		if _, err := n.Client().Call(callCtx, sid, nil); err != nil {
			fail("stuck activation call: %v", err)
		}
	}

	// 就绪标记：测试/运维以本行判定节点可收号。
	fmt.Println("READY")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	<-sig

	// 优雅排空：预算 5s（四段语义见 node.Shutdown 文档注释）。
	drainCtx, cancelDrain := context.WithTimeout(context.Background() /* 例外①（进程入口）：演示进程的显式预算 */, 5*time.Second)
	defer cancelDrain()
	drainErr := n.Shutdown(drainCtx)
	if drainErr != nil {
		if !stuck {
			fail("drain: %v", drainErr)
		}
		// stuck 模式：卡死实体的销毁在预算内无法收场（销毁返回 ctx.Err()）
		// 是预期结果——打印 DRAINBUDGET 行留痕，继续走 DRAINED/退出码 0
		//（阶段④停序已完成；卡死实体留给看门狗/自身设备，见包注释）。
		fmt.Printf("DRAINBUDGET %v\n", drainErr)
	}

	fmt.Println("DRAINED")
	os.Exit(0)
}

// fail 失败出口：stderr 提示 + 退出码 1（演练断言非零退出即失败）。
func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "sigtermhelper: "+format+"\n", args...)
	os.Exit(1)
}
