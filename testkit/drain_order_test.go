package testkit

// 排空次序的端到端锚（redis 模式）。停序 = 门闸 + draining 标记（心跳
// 继续）→ 并行 Retire → 出站 flush → 停心跳 → 节点注销 → 关传输。
// 关键锚是「排空期间续租照常」：Shutdown 排空阻塞在实体
// Deactivate 的整个窗口内，实体租约与节点成员键必须持续刷新（长排空不过
// 租约 TTL）；停机完成后实体条目与节点键随注销消失。等待一律有界轮询。

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// gateDeactEntity Deactivate 停点实体：Shutdown 阶段②的确定性阻塞点
// （remote 包 drain_test.go 同款形态，testkit 自含复制——不跨包引测试代码）。
type gateDeactEntity struct {
	rans.Entity
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *gateDeactEntity) Deactivate(ctx context.Context) error {
	e.once.Do(func() { close(e.entered) })
	<-e.release
	return e.Entity.Deactivate(ctx)
}

// TestShutdownRenewsLeaseAndNodeKeyDuringDrain 排空阻塞窗内租约与节点键
// 持续刷新（旧停序「先停心跳再排空」下二者在窗内过期——红）；停机完成后
// 条目与节点键随注销消失。租约 600ms/心跳 150ms，阻塞窗 ~1.4s > 心跳 TTL
// （2×leaseTTL=1.2s）：任一刷新面停摆都会在本窗内退出视图。
func TestShutdownRenewsLeaseAndNodeKeyDuringDrain(t *testing.T) {
	mustRedis(t)
	ctx := context.Background()

	deactEntered := make(chan struct{})
	deactRelease := make(chan struct{})
	kinds := map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity {
			return &gateDeactEntity{
				Entity:  noopEntity{},
				entered: deactEntered,
				release: deactRelease,
			}
		},
	}
	c := NewCluster(t, 2, kinds,
		WithRedis(redisAddr(), 600*time.Millisecond, 150*time.Millisecond))

	// 实体落在节点 2（节点 2 本地调用激活）
	id, err := rans.NewID("cell", "drainlease")
	if err != nil {
		t.Fatal(err)
	}
	nodeB := c.Node(1)
	if v, err := nodeB.Call(ctx, id, 1); err != nil || v != 1 {
		t.Fatalf("activate on node B: v=%v err=%v", v, err)
	}

	dir := c.Directory()
	reg, ok := dir.(interface {
		NodeIDs(ctx context.Context) ([]rans.NodeID, error)
		NodeAddr(ctx context.Context, node rans.NodeID) (string, bool, error)
	})
	if !ok {
		t.Fatal("redis mode directory must expose the node membership view (NodeIDs/NodeAddr)")
	}
	// 节点键探活走单点 NodeAddr（不过滤 draining——排空期间聚合视图
	// NodeIDs 已按设计不再返回本节点，键本身仍须存活）。
	nodeAddrAlive := func() bool {
		addr, found, err := reg.NodeAddr(ctx, 2)
		return err == nil && found && addr != ""
	}
	nodeVisible := func() bool {
		nodes, err := reg.NodeIDs(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range nodes {
			if n == 2 {
				return true
			}
		}
		return false
	}
	pollUntil(t, 5*time.Second, "node 2 membership key", nodeAddrAlive)
	if _, found, err := dir.Lookup(ctx, id); err != nil || !found {
		t.Fatalf("entity entry before shutdown: found=%v err=%v", found, err)
	}

	// Shutdown（逆序：节点 2 先排空）——阶段②阻塞在 Deactivate
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- c.Shutdown(ctx) }()
	select {
	case <-deactEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown never reached entity deactivate (stage ②)")
	}

	// 阻塞窗（1.4s > 心跳 TTL 1.2s > 租约 TTL 0.6s）：二者必须持续刷新
	deadline := time.Now().Add(1400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, found, err := dir.Lookup(ctx, id); err != nil || !found {
			t.Fatalf("lease renewal stopped during drain stage ②: found=%v err=%v", found, err)
		}
		if !nodeAddrAlive() {
			t.Fatal("node membership key renewal stopped during drain stage ②")
		}
		time.Sleep(50 * time.Millisecond)
	}
	close(deactRelease)

	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	// 停机完成：实体条目与节点 2 成员键随排空注销消失；聚合视图同步收敛
	pollUntil(t, 5*time.Second, "entity entry unregistered after drain", func() bool {
		_, found, err := dir.Lookup(ctx, id)
		return err == nil && !found
	})
	pollUntil(t, 5*time.Second, "node 2 membership key deleted", func() bool {
		return !nodeAddrAlive()
	})
	pollUntil(t, 5*time.Second, "node 2 left the membership view", func() bool {
		return !nodeVisible()
	})
}

// noopEntity 回显实体（gateDeactEntity 的宿主——Handle 原样返回请求，
// 供激活成功断言取值）。
type noopEntity struct{}

func (noopEntity) Activate(context.Context) error   { return nil }
func (noopEntity) Deactivate(context.Context) error { return nil }
func (noopEntity) Handle(_ context.Context, req any) (any, error) {
	return req, nil
}
