package testkit

// 发件箱公开面验收（审查报告 §5 补测建议二）：出站保序与满箱/排空从
// internal 装配面升为 testkit 公开面——双节点/三节点真 TCP 真 wire，全部
// 经统一调用面（node.Client()）驱动，断言只碰公开原语（Fault 注入 +
// Node.Metrics() 快照 + WithOutboxCapacity 容量编排）。行为锚在
// internal/rpc：outbox_test.go（满箱阻塞/分片 FIFO/指标）、
// outbox_remote_test.go（装配接线/停序 flush）。时序断言一律 channel 与
// 有界轮询（pollUntil），不用裸 sleep 承重。

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// histEntity 历史实体：非查询消息按到达序追加，"history" 查询返回逗号
// 拼接的全部历史（read-your-writes 读侧面 + 送达序观测面）。pawnite 单
// loop 串行执行——实例内状态无锁；hit 为可选送达观测点（nil 不上报；
// 容量须覆盖全部消息，否则串行 loop 阻塞在上报上）。
type histEntity struct {
	hist []string
	hit  chan string
}

func (e *histEntity) Activate(context.Context) error   { return nil }
func (e *histEntity) Deactivate(context.Context) error { return nil }
func (e *histEntity) Handle(_ context.Context, req any) (any, error) {
	s := req.(string)
	if s == "history" {
		return strings.Join(e.hist, ","), nil
	}
	e.hist = append(e.hist, s)
	if e.hit != nil {
		e.hit <- s
	}
	return nil, nil
}

// histKinds 单 kind 历史实体工厂（观测点经闭包共享给测试）。
func histKinds(hit chan string) map[string]rans.Factory {
	return map[string]rans.Factory{
		"hist": func(_ rans.ID) rans.Entity { return &histEntity{hit: hit} },
	}
}

// outboxMetric 读节点快照中某发件箱仪表读数（缺名即致命——三仪表恒装配
// 进节点注册表）。
func outboxMetric(t *testing.T, n *Node, name string) float64 {
	t.Helper()
	for _, m := range n.Node.Metrics() {
		if m.Name == name {
			return m.Value
		}
	}
	t.Fatalf("metric %q missing from node snapshot", name)
	return 0
}

// TestPublicOutbox_TellThenCallOrdering_CrossNode 公开面跨节点保序（C1
// 锚的 testkit 复本）：实体落在节点 2，同一调用方（节点 1）先 Tell 后
// Call 同一实体——两腿全部经节点 1 出站发件箱（同分片同 FIFO，Call 请求
// 帧不超车在箱 Tell），Call 应答必须看见此前全部 Tell 的写入。
func TestPublicOutbox_TellThenCallOrdering_CrossNode(t *testing.T) {
	c := NewCluster(t, 2, histKinds(nil))
	na, nb := c.Node(0), c.Node(1)
	ctx := context.Background()
	id := mustID(t, "hist", "ob-order")

	if _, err := nb.Client().Call(ctx, id, "history"); err != nil { // 节点 2 本地激活
		t.Fatalf("activate on node 2: %v", err)
	}
	if err := na.Client().Tell(ctx, id, "hello"); err != nil {
		t.Fatalf("tell hello: %v", err)
	}
	if err := na.Client().Tell(ctx, id, "world"); err != nil {
		t.Fatalf("tell world: %v", err)
	}
	v, err := na.Client().Call(ctx, id, "history")
	if err != nil {
		t.Fatalf("call history: %v", err)
	}
	if got := v.(string); got != "hello,world" {
		t.Fatalf("history = %q, want %q（先到的 Tell 必须对紧随的 Call 可见）", got, "hello,world")
	}
}

// TestPublicOutbox_TellBlocksAtCapacityUntilBudget 满箱阻塞语义（小容量
// 发件箱 + 分区不可达目标）：队头帧卡在发送重试段、后继帧占满小箱后，
// 满箱 Tell 阻塞至调用方预算到期回 Timeout（绝不永久阻塞、绝不满即丢）；
// 解除分区后整队按 FIFO 排空送达、腾位恢复即入，全程零丢弃零发送失败
// （at-most-once 的滞留而非丢失）。
func TestPublicOutbox_TellBlocksAtCapacityUntilBudget(t *testing.T) {
	hits := make(chan string, 8)
	c := NewCluster(t, 2, histKinds(hits), WithOutboxCapacity(2))
	na, nb := c.Node(0), c.Node(1)
	ctx := context.Background()
	id := mustID(t, "hist", "ob-full")

	if _, err := nb.Client().Call(ctx, id, "history"); err != nil {
		t.Fatalf("activate on node 2: %v", err)
	}
	c.Partition(0, 1) // 拨号屏蔽：队头帧卡发送重试段，箱对后继帧呈现满箱
	tells := []string{"m1", "m2", "m3"}
	for _, m := range tells {
		if err := na.Client().Tell(ctx, id, m); err != nil {
			t.Fatalf("tell %q: %v", m, err)
		}
	}
	pollUntil(t, 5*time.Second, "outbox depth reaches capacity 2", func() bool {
		return outboxMetric(t, na, "rans.outbox.depth") == 2
	})

	// 满箱 Tell：阻塞至预算到期回 Timeout，时长夹在预算下界与失控上界。
	start := time.Now()
	err := na.Client().Tell(ctx, id, "m4", rans.Timeout(400*time.Millisecond))
	elapsed := time.Since(start)
	if !errors.Is(err, rans.ErrTimeout) {
		t.Fatalf("full-box tell = %v, want rans.ErrTimeout", err)
	}
	if elapsed < 300*time.Millisecond {
		t.Fatalf("full-box tell returned in %v; must block until its budget", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("full-box tell blocked %v; must be bounded by its 400ms budget", elapsed)
	}

	// 解除分区：三帧按 FIFO 排空送达（经真实 wire 的同分片 FIFO）。
	c.Heal(0, 1)
	for _, m := range tells {
		select {
		case got := <-hits:
			if got != m {
				t.Fatalf("delivery order broken: got %q, want %q", got, m)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("queued tell %q never drained after heal", m)
		}
	}
	// 腾位恢复即入：满箱态不残留。
	if err := na.Client().Tell(ctx, id, "m4", rans.Timeout(2*time.Second)); err != nil {
		t.Fatalf("tell after drain: %v", err)
	}
	select {
	case got := <-hits:
		if got != "m4" {
			t.Fatalf("post-drain tell delivered %q, want %q", got, "m4")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("post-drain tell never delivered")
	}
	if v := outboxMetric(t, na, "rans.outbox.dropped"); v != 0 {
		t.Fatalf("outbox dropped = %v, want 0（滞留帧在解除分区后全部送达）", v)
	}
	if v := outboxMetric(t, na, "rans.outbox.send_failed"); v != 0 {
		t.Fatalf("outbox send_failed = %v, want 0（重试窗口未在测试窗内耗尽）", v)
	}
}

// TestPublicOutbox_ShutdownFlushDrainsQueuedTells 停机排空正路（停序③
// flush 的公开面）：分区滞留的在箱帧在解除分区后全部送达（flush 的等待
// 面——在箱深度归零），随后停机即刻干净收场、零弃置。
func TestPublicOutbox_ShutdownFlushDrainsQueuedTells(t *testing.T) {
	hits := make(chan string, 8)
	c := NewCluster(t, 2, histKinds(hits), WithOutboxCapacity(4))
	na, nb := c.Node(0), c.Node(1)
	ctx := context.Background()
	id := mustID(t, "hist", "ob-flush")

	if _, err := nb.Client().Call(ctx, id, "history"); err != nil {
		t.Fatalf("activate on node 2: %v", err)
	}
	c.Partition(0, 1)
	tells := []string{"f1", "f2", "f3"}
	for _, m := range tells {
		if err := na.Client().Tell(ctx, id, m); err != nil {
			t.Fatalf("tell %q: %v", m, err)
		}
	}
	pollUntil(t, 5*time.Second, "outbox depth reaches 2 (head in flight)", func() bool {
		return outboxMetric(t, na, "rans.outbox.depth") == 2
	})

	c.Heal(0, 1)
	for _, m := range tells {
		select {
		case got := <-hits:
			if got != m {
				t.Fatalf("delivery order broken: got %q, want %q", got, m)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("queued tell %q never drained after heal", m)
		}
	}
	pollUntil(t, 5*time.Second, "outbox drains to zero", func() bool {
		return outboxMetric(t, na, "rans.outbox.depth") == 0
	})

	// 停机：flush 等待面（深度/pending）已归零——有界预算内干净收场。
	sctx, scancel := context.WithTimeout(ctx, 5*time.Second)
	defer scancel()
	if err := na.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if v := outboxMetric(t, na, "rans.outbox.dropped"); v != 0 {
		t.Fatalf("outbox dropped = %v after shutdown, want 0（排空在停机前完成，无帧弃置）", v)
	}
}

// TestPublicOutbox_ShutdownBoundedWhenBoxStuck 停机 flush 的有界收场
// （箱非空形态，internal 锚 TestRemoteDrainFlushWaitsOutbox 的升面）：
// 分区未解、箱内有滞留帧时停机——flush 必须先等排空窗（非空箱不即时
// 返回），窗口耗尽后照常收口（不挂死）；未送达帧关箱弃置计数留痕，
// 分区对端零送达。
func TestPublicOutbox_ShutdownBoundedWhenBoxStuck(t *testing.T) {
	hits := make(chan string, 8)
	c := NewCluster(t, 2, histKinds(hits), WithOutboxCapacity(2))
	na, nb := c.Node(0), c.Node(1)
	ctx := context.Background()
	id := mustID(t, "hist", "ob-stuck")

	if _, err := nb.Client().Call(ctx, id, "history"); err != nil {
		t.Fatalf("activate on node 2: %v", err)
	}
	c.Partition(0, 1)
	for _, m := range []string{"s1", "s2", "s3"} {
		if err := na.Client().Tell(ctx, id, m); err != nil {
			t.Fatalf("tell %q: %v", m, err)
		}
	}
	pollUntil(t, 5*time.Second, "outbox depth reaches capacity 2", func() bool {
		return outboxMetric(t, na, "rans.outbox.depth") == 2
	})

	// 非空箱停机：flush 等待至排空窗（500ms）边界后照常收口——返回时长
	// 夹在窗口下界与停机预算之间。
	sctx, scancel := context.WithTimeout(ctx, 10*time.Second)
	defer scancel()
	start := time.Now()
	if err := na.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Fatalf("shutdown returned in %v; flush must wait the drain window on a non-empty box", elapsed)
	}
	// 未送达帧的收场留痕：在箱两帧关箱弃置、在途队头随关箱取消计发送失败。
	pollUntil(t, 5*time.Second, "close accounting (dropped=2, send_failed=1)", func() bool {
		return outboxMetric(t, na, "rans.outbox.dropped") == 2 &&
			outboxMetric(t, na, "rans.outbox.send_failed") == 1
	})
	// 分区仍在：滞留帧从未抵达对端。
	select {
	case got := <-hits:
		t.Fatalf("partitioned peer received %q; stuck frames must stay undelivered", got)
	default:
	}
}

// TestPublicOutbox_CrossTargetIndependentProgress 跨目标不互相阻塞守护
// （分片并行性质的公开面冒烟）：不可达目标（节点 2，与调用方分区）卡住
// 其分片队头时，异分片健康目标（节点 3——发件箱按目标节点散布，两目标
// 落不同分片）的 Tell→Call 照常在自身预算内完成；jam 对端零送达。
func TestPublicOutbox_CrossTargetIndependentProgress(t *testing.T) {
	jamHits := make(chan string, 8)
	okHits := make(chan string, 8)
	kinds := map[string]rans.Factory{
		"jamkind": func(_ rans.ID) rans.Entity { return &histEntity{hit: jamHits} },
		"okkind":  func(_ rans.ID) rans.Entity { return &histEntity{hit: okHits} },
	}
	c := NewCluster(t, 3, kinds)
	na, njam, nok := c.Node(0), c.Node(1), c.Node(2)
	ctx := context.Background()
	jamID := mustID(t, "jamkind", "ob-cross-jam")
	okID := mustID(t, "okkind", "ob-cross-ok")

	if _, err := njam.Client().Call(ctx, jamID, "history"); err != nil {
		t.Fatalf("activate jam entity: %v", err)
	}
	if _, err := nok.Client().Call(ctx, okID, "history"); err != nil {
		t.Fatalf("activate ok entity: %v", err)
	}
	c.Partition(0, 1) // 节点 1 ↔ 节点 2 分区：jam 分片队头卡发送重试段
	for _, m := range []string{"stuck-1", "stuck-2"} {
		if err := na.Client().Tell(ctx, jamID, m); err != nil {
			t.Fatalf("tell jam %q: %v", m, err)
		}
	}
	pollUntil(t, 5*time.Second, "jam shard backlog (depth >= 1)", func() bool {
		return outboxMetric(t, na, "rans.outbox.depth") >= 1
	})

	// 健康目标照常：Call 在自身预算内完成（若被 jam 分片拖死，调用帧
	// 排队至预算耗尽即红）。
	if err := na.Client().Tell(ctx, okID, "ping"); err != nil {
		t.Fatalf("tell ok: %v", err)
	}
	v, err := na.Client().Call(ctx, okID, "history", rans.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("call on healthy target dragged by jammed shard: %v", err)
	}
	if got := v.(string); got != "ping" {
		t.Fatalf("history = %q, want %q", got, "ping")
	}
	select {
	case got := <-jamHits:
		t.Fatalf("partitioned jam target received %q", got)
	default:
	}
}
