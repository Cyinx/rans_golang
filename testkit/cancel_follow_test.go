package testkit

// 票 12 红测：取消跟随仲裁输家转发到赢家——调用方经陈旧位置缓存把请求
// 发给输家，输家转发赢家并回重定向提示；调用方随后取消，取消必须改道赢
// 家并取消在途执行（修复前：源端 pending 仍指向输家，取消发给输家被忽略，
// 赢家的执行干等到请求预算耗尽）。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory"
)

// hangEntity "hang" 占住 Handle 直到 ctx 取消（报告观察到的收场）。
type hangEntity struct {
	started chan struct{}
	done    chan string
}

func (e *hangEntity) Activate(context.Context) error   { return nil }
func (e *hangEntity) Deactivate(context.Context) error { return nil }

func (e *hangEntity) Handle(ctx context.Context, msg any) (any, error) {
	if s, _ := msg.(string); s == "hang" {
		select {
		case e.started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		e.done <- "ctx:" + ctx.Err().Error()
		return nil, ctx.Err()
	}
	return "ok", nil
}

func newHangKinds() (map[string]rans.Factory, *hangEntity) {
	e := &hangEntity{started: make(chan struct{}, 1), done: make(chan string, 1)}
	return map[string]rans.Factory{
		"hang": func(rans.ID) rans.Entity { return e },
	}, e
}

func TestCancel_FollowsLoserForwardToWinner(t *testing.T) {
	mustRedis(t)
	kinds, e := newHangKinds()
	c := NewCluster(t, 3, kinds, WithRedis(redisAddr(), 1500*time.Millisecond, 300*time.Millisecond),
		WithCheckpointStore(checkpoint.New()), WithIdlePassivation("hang", 100*time.Millisecond))
	n1, _, n3 := c.Node(0), c.Node(1), c.Node(2)
	ctx := context.Background()
	dir := c.Directory()
	id := mustID(t, "hang", "follow")

	if _, err := n1.Call(ctx, id, "warm"); err != nil { // 实体落 node1
		t.Fatalf("activate on node1: %v", err)
	}
	if _, err := n3.Call(ctx, id, "warm"); err != nil { // node3 位置缓存 ← node1
		t.Fatalf("warm from node3: %v", err)
	}

	// 外部窃取到 node2：node3 的缓存仍指 node1（正 TTL 窗内）。
	cur, found, err := dir.Lookup(ctx, id)
	if err != nil || !found {
		t.Fatalf("lookup before steal: found=%v err=%v", found, err)
	}
	if ok, err := dir.UnregisterIfOwner(ctx, id, cur); err != nil || !ok {
		t.Fatalf("steal unregister: ok=%v err=%v", ok, err)
	}
	if won, _, err := dir.Register(ctx, id, directory.Owner{Node: 2}, nil); err != nil || !won {
		t.Fatalf("steal register to node2: won=%v err=%v", won, err)
	}

	// node1 的本地实例闲置钝化（快照落 Store 清场）——node3 的 hang 到达
	// node1 时无本地实例可落，必经仲裁输家转发（外部窃取走墓碑无前主，
	// 不触发接管通知，陈旧实例只能靠钝化让位）。
	for range 200 {
		if !n1.Holds(id) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n1.Holds(id) {
		t.Fatal("node1 instance never passivated")
	}

	// node3 经陈旧缓存把 hang 发给输家 node1 → 转发赢家 node2 执行。
	callCtx, cancel := context.WithCancel(context.Background())
	callDone := make(chan error, 1)
	go func() {
		_, err := n3.Call(callCtx, id, "hang")
		callDone <- err
	}()
	select {
	case <-e.started: // 赢家（或缓存未失效时的输家本地）已开始执行
	case <-time.After(3 * time.Second):
		t.Fatal("hang never started")
	}
	// 等转发+提示落地（输家本地无实例——started 必来自转发后的赢家）。
	time.Sleep(300 * time.Millisecond)
	cancelAt := time.Now()
	cancel()

	select {
	case outcome := <-e.done:
		elapsed := time.Since(cancelAt)
		if elapsed > 2*time.Second {
			t.Fatalf("hang observed %q only after %v — cancel did not follow the forward to the winner (want <2s)", outcome, elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("hang never observed cancellation (cancel lost at the loser)")
	}
	select {
	case err := <-callDone:
		if err == nil {
			t.Fatal("canceled call must not succeed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("caller never returned")
	}
}
