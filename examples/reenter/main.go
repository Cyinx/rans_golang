// reenter 演示：Defer + ReenterAfter 让出重入两件套（H12 让出原语）。实体 A
// 的 Handle 收到长调用请求后立即让出——work 在 loop 外对同集群慢实体 B 发起
// 慢 IO 调用；让出期间 A 的 loop 照常服务其他消息（note 请求，输出可见）；
// 慢调用完成后 then 续体回 loop 串行执行，聚合慢结果与让出期间累积的 note
// 一并延迟回复。实体内零自建 goroutine：出站并发与续体调度全部由运行时
// 托管。输出序列确定（起跑信号/应答回执/放行闸三重同步），自检测试见
// main_test.go（TestExampleReenterAfter）。
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/node"
)

// gatherReq 长调用请求（A 收到即让出，延迟应答）。
type gatherReq struct{ N int }

// gatherReply 聚合回复：慢 IO 结果 + 让出期间 A 照常服务的全部 note。
type gatherReply struct {
	Slow  int
	Notes []string
}

// noteReq 让出期间的插队消息（A 的 loop 空闲即服务，证明可重入）。
type noteReq struct{ Text string }

// slowReq 慢实体请求载荷；slowReply 回显 N（聚合校验用）。
type slowReq struct{ N int }
type slowReply struct{ Value int }

// syncWriter 并发收敛写面：实体 loop（A/B）、work goroutine 与 main 的打印
// 各在不同 goroutine——输出汇入单写面防字节交错。
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// aggregator 实体 A（kind=aggregator）：notes 是 loop 限定状态——Handle 与
// then 都在 loop 上串行读写（让出点前后状态一致性由 loop 串行保证）；work
// goroutine 绝不回触（ReenterAfter 纪律）。
type aggregator struct {
	slowID  rans.ID       // 慢实体身份（work 的调用目标）
	started chan struct{} // work 起跑信号（缓冲 1 非阻塞——演示编排的时序锚）
	notes   []string      // loop 限定状态：让出期间累积的 note（then 聚合）
	out     io.Writer
}

func (a *aggregator) Activate(context.Context) error   { return nil }
func (a *aggregator) Deactivate(context.Context) error { return nil }

func (a *aggregator) Handle(ctx context.Context, req any) (any, error) {
	switch r := req.(type) {
	case gatherReq:
		// 两件套正本形态：Defer 拿应答器 → ReenterAfter 让出 → 返回哨兵。
		fmt.Fprintf(a.out, "[A] gather 请求：Defer + ReenterAfter 让出，work 将调 slowio/b-1\n")
		reply := rans.Defer(ctx)
		rans.ReenterAfter(ctx,
			// work（loop 外）：慢 IO 调用。先发起跑信号再调用。
			func(wctx context.Context) (any, error) {
				select {
				case a.started <- struct{}{}:
				default:
				}
				return rans.ClientFrom(wctx).Call(wctx, a.slowID, slowReq{N: r.N})
			},
			// then（回 loop 串行）：聚合慢结果与让出期间的 note，延迟回复。
			func(_ context.Context, v any, err error) {
				agg := gatherReply{Notes: a.notes}
				if sr, ok := v.(slowReply); ok {
					agg.Slow = sr.Value
				}
				a.notes = nil
				fmt.Fprintf(a.out, "[A] then 聚合：slow=%d notes=%v（回 loop 串行收场）\n", agg.Slow, agg.Notes)
				reply.Reply(agg, err)
			})
		return rans.Deferred, nil
	case noteReq:
		a.notes = append(a.notes, r.Text)
		fmt.Fprintf(a.out, "[A] 让出期间照常服务：note %q\n", r.Text)
		return "ok", nil
	}
	return nil, fmt.Errorf("reenter: aggregator unexpected request %T", req)
}

// slowIO 实体 B（kind=slowio）：Handle 阻塞在放行闸上模拟慢 IO（生产形态
// 是任意外部 IO——HTTP/DB 等），尊重调用方 ctx 取消。
type slowIO struct {
	release chan struct{}
	out     io.Writer
}

func (s *slowIO) Activate(context.Context) error   { return nil }
func (s *slowIO) Deactivate(context.Context) error { return nil }

func (s *slowIO) Handle(ctx context.Context, req any) (any, error) {
	if r, ok := req.(slowReq); ok {
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		fmt.Fprintf(s.out, "[B] slowio/b-1 慢 IO 返回：n=%d\n", r.N)
		return slowReply{Value: r.N}, nil
	}
	return nil, fmt.Errorf("reenter: slowio unexpected request %T", req)
}

// run 装配单节点（Standalone）并驱动演示：输出序列由三重同步钉死——
// work 起跑信号、note 应答回执、慢 IO 放行闸。返回 nil 即全链路成立。
func run(w io.Writer) error {
	out := &syncWriter{w: w}
	slowID, err := rans.NewID("slowio", "b-1")
	if err != nil {
		return err
	}
	started := make(chan struct{}, 1) // work 起跑信号（编排时序锚，实体工厂注入）
	release := make(chan struct{})    // 慢 IO 放行闸（编排时序锚，实体工厂注入）
	n, err := node.New(node.Config{
		NodeID:     1,
		Standalone: true,
		Kinds: map[string]rans.Factory{
			"aggregator": func(_ rans.ID) rans.Entity {
				return &aggregator{slowID: slowID, started: started, out: out}
			},
			"slowio": func(_ rans.ID) rans.Entity { return &slowIO{release: release, out: out} },
		},
	})
	if err != nil {
		return err
	}
	defer n.Close()
	// 例外①（进程入口）：main 是进程根，节点根 ctx 由 Start 内部自建
	// ——§5.1 第 2 条管辖库代码，main 不在列。
	if err := n.Start(context.Background()); err != nil {
		return err
	}
	aggID, err := rans.NewID("aggregator", "a-1")
	if err != nil {
		return err
	}
	// 调用方必须传带 deadline 的 ctx（无 deadline 的 Call 在吞 panic 场景
	// 会无限阻塞——决策出处见 examples/counter）。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type callRes struct {
		v   any
		err error
	}
	res := make(chan callRes, 1)
	go func() { // 长调用异步发起：main 随后插队 note，聚合同步收场
		v, cerr := n.Client().Call(ctx, aggID, gatherReq{N: 3})
		res <- callRes{v, cerr}
	}()
	<-started // work 已起跑：A 已让出 loop
	for _, t := range []string{"n1", "n2"} {
		if _, err := n.Client().Call(ctx, aggID, noteReq{Text: t}); err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "[main] 放行慢 IO（A 让出期间已服务 2 条 note）\n")
	close(release)
	r := <-res
	if r.err != nil {
		return r.err
	}
	agg, ok := r.v.(gatherReply)
	if !ok {
		return fmt.Errorf("reenter: unexpected gather reply %T", r.v)
	}
	fmt.Fprintf(out, "[main] gather 回复：slow=%d notes=%v\n", agg.Slow, agg.Notes)
	return nil
}

func main() {
	if err := run(os.Stdout); err != nil {
		panic(err)
	}
}
