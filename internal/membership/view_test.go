package membership

// View 测试：NodesFunc 内存假实现驱动 View 全部行为；redis 数据
// 源（NodeIDsByHeartbeat）一条 env 门禁真跑用例。放置策略（Random/
// KindPlacement）的用例面在 internal/placement。

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/redis"
	goredis "github.com/redis/go-redis/v9"
)

// fakeNodes 是可变内存数据源：锁保护nodes/err，get 返回副本（View 侧
// 不得依赖源切片的别名）。
type fakeNodes struct {
	mu    sync.Mutex
	nodes []rans.NodeID
	err   error
}

func (f *fakeNodes) set(nodes []rans.NodeID, err error) {
	f.mu.Lock()
	f.nodes, f.err = nodes, err
	f.mu.Unlock()
}

func (f *fakeNodes) get(context.Context) ([]rans.NodeID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]rans.NodeID, len(f.nodes))
	copy(out, f.nodes)
	return out, f.err
}

// recvWatch 带 2s 超时收一条快照（防死锁挂死测试进程）。
func recvWatch(t *testing.T, ch <-chan []rans.NodeID) []rans.NodeID {
	t.Helper()
	select {
	case snap := <-ch:
		return snap
	case <-time.After(2 * time.Second):
		t.Fatal("watch channel: no snapshot within 2s")
		return nil
	}
}

func equalNodes(a, b []rans.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestAliveNodesPassThrough 直通语义：数据源结果原样返回、错误原样传播、
// 未注入数据源响亮报错（装配期漏注入不得伪装成空集）。
func TestAliveNodesPassThrough(t *testing.T) {
	ctx := context.Background()

	f := &fakeNodes{}
	f.set([]rans.NodeID{3, 1, 2}, nil)
	got, err := NewView(f.get).AliveNodes(ctx)
	if err != nil || !equalNodes(got, []rans.NodeID{3, 1, 2}) {
		t.Fatalf("AliveNodes = %v, %v; want [3 1 2], nil", got, err)
	}

	sentinel := errors.New("boom")
	f.set(nil, sentinel)
	if _, err := NewView(f.get).AliveNodes(ctx); !errors.Is(err, sentinel) {
		t.Fatalf("AliveNodes error = %v, want sentinel", err)
	}

	if _, err := NewView(nil).AliveNodes(ctx); err == nil {
		t.Fatal("View without NodesFunc must error, got nil")
	}
}

// TestWatchSnapshotSemantics Watch 快照语义：订阅即推当前快照一次；
// 每次 Refresh 成功推新快照；cancel 关通道且不再收；其余订阅者不受影响；
// 订阅时数据源失败跳过首推、修好后 Refresh 补推。
func TestWatchSnapshotSemantics(t *testing.T) {
	ctx := context.Background()
	f := &fakeNodes{}
	f.set([]rans.NodeID{1}, nil)
	v := NewView(f.get)

	ch, cancel := v.Watch(ctx)
	if snap := recvWatch(t, ch); !equalNodes(snap, []rans.NodeID{1}) {
		t.Fatalf("initial snapshot = %v, want [1]", snap)
	}

	// Refresh 推新快照给全部订阅者
	f.set([]rans.NodeID{1, 4}, nil)
	ch2, cancel2 := v.Watch(ctx) // 第二订阅者：首推即最新快照
	if snap := recvWatch(t, ch2); !equalNodes(snap, []rans.NodeID{1, 4}) {
		t.Fatalf("second watcher initial = %v, want [1 4]", snap)
	}
	if err := v.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if snap := recvWatch(t, ch); !equalNodes(snap, []rans.NodeID{1, 4}) {
		t.Fatalf("after refresh = %v, want [1 4]", snap)
	}
	if snap := recvWatch(t, ch2); !equalNodes(snap, []rans.NodeID{1, 4}) {
		t.Fatalf("second watcher after refresh = %v, want [1 4]", snap)
	}

	// Refresh 失败不推送
	f.set(nil, errors.New("boom"))
	if err := v.Refresh(ctx); err == nil {
		t.Fatal("Refresh must propagate source error")
	}

	// cancel：通道关闭、后续 Refresh 不再投递也不 panic
	cancel()
	select {
	case snap, open := <-ch:
		if open {
			t.Fatalf("cancelled channel must be closed, got snapshot %v", snap)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cancelled channel must be closed")
	}
	f.set([]rans.NodeID{9}, nil)
	if err := v.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case _, open := <-ch:
		if open {
			t.Fatal("cancelled watcher must not receive new snapshots after Refresh")
		}
		// 通道已关闭：立即返回 open=false 正是期望（不再有新快照入通道）
	case <-time.After(100 * time.Millisecond):
	}
	if snap := recvWatch(t, ch2); !equalNodes(snap, []rans.NodeID{9}) {
		t.Fatalf("surviving watcher after refresh = %v, want [9]", snap)
	}

	// 重复 cancel 幂等
	cancel()
	cancel2()
}

// TestWatchInitialSourceError 订阅时数据源失败：跳过首推，修好后 Refresh
// 补推——订阅不因首查失败而变成死通道。
func TestWatchInitialSourceError(t *testing.T) {
	ctx := context.Background()
	f := &fakeNodes{}
	f.set(nil, errors.New("down"))
	v := NewView(f.get)

	ch, cancel := v.Watch(ctx)
	defer cancel()
	select {
	case <-ch:
		t.Fatal("source failing at Watch must not push initial snapshot")
	case <-time.After(100 * time.Millisecond):
	}
	f.set([]rans.NodeID{2, 6}, nil)
	if err := v.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if snap := recvWatch(t, ch); !equalNodes(snap, []rans.NodeID{2, 6}) {
		t.Fatalf("after heal = %v, want [2 6]", snap)
	}
}

// ---- redis 数据源（NodeIDs，节点级成员键）env 门禁真跑 ----

// redisAddr 返回测试 Redis 地址：RANS_TEST_REDIS_ADDR 优先，默认
// 127.0.0.1:16379（make test-redis-up 拉起同一容器）。
func redisAddr() string {
	if v := os.Getenv("RANS_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:16379"
}

// redisPassword 返回测试 Redis 密码：RANS_TEST_REDIS_PASSWORD（内网带认证
// Redis 直连模式用；docker 本地实例无密码，缺省为空）。
func redisPassword() string { return os.Getenv("RANS_TEST_REDIS_PASSWORD") }

// requireRedis 探活门禁：不可达即 skip——bare `go test ./...` 无 Redis
// 仍须全绿（决策 docs/decisions/2026-09-30-phase3-redis-env.md）。
func requireRedis(t *testing.T) string {
	t.Helper()
	addr := redisAddr()
	cli := goredis.NewClient(&goredis.Options{Addr: addr, Password: redisPassword(), DialTimeout: 500 * time.Millisecond})
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable at %s — run make test-redis-up", addr)
	}
	return addr
}

// TestNodeIDsRedis redis 侧 NodesFunc 实现：写了
// 节点成员键的节点入集（升序），未写成员键的节点（哪怕持有实体条目）
// 不入集——实体心跳与节点存活彻底解耦；漂移键跳过，
// membership.NewView 直连该数据源可取同一视图。
func TestNodeIDsRedis(t *testing.T) {
	addr := requireRedis(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("rans:test:%s:%d:", t.Name(), time.Now().UnixNano())
	// 残键清扫：{reg}:nodes 注册总表是设计性无 TTL 键——
	// defer 的目录 Close 之后由 Cleanup 清扫本前缀（LIFO：登记早于下方
	// 其余 Cleanup 即最后执行）。仅测试侧：生产 Close 只关客户端绝不清键。
	{
		cli := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: []string{addr}, Password: redisPassword()})
		t.Cleanup(func() {
			defer cli.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			var cursor uint64
			for {
				keys, next, err := cli.Scan(ctx, cursor, prefix+"*", 500).Result()
				if err != nil {
					t.Errorf("sweep test keys (prefix %s): scan: %v", prefix, err)
					return
				}
				if len(keys) > 0 {
					if err := cli.Del(ctx, keys...).Err(); err != nil {
						t.Errorf("sweep test keys (prefix %s): del %d keys: %v", prefix, len(keys), err)
						return
					}
				}
				cursor = next
				if cursor == 0 {
					return
				}
			}
		})
	}
	d := redis.New(redis.Options{
		Addr:         addr,
		Password:     redisPassword(),
		HeartbeatTTL: 2 * time.Minute,
		KeyPrefix:    prefix,
	})
	defer d.Close()

	// node 3 持有实体条目但从未 RegisterNode（零成员键）：不入集——存活
	// 派生不看实体面（成员键在=节点活）。
	id3 := rans.ID{Kind: "player", Key: "mD"} // 直构合法身份（无校验开销路径，与注册面一致）
	if won, _, err := d.Register(ctx, id3, directory.Owner{Node: 3}, nil); err != nil || !won {
		t.Fatalf("register %s: won=%v err=%v", id3.String(), won, err)
	}
	if got, err := d.NodeIDs(ctx); err != nil || len(got) != 0 {
		t.Fatalf("NodeIDs before RegisterNode = %v, %v; want empty (entity ownership is not liveness)", got, err)
	}

	// RegisterNode 即可见（零实体节点照常——node 1/2 不注册任何实体）。
	if err := d.RegisterNode(ctx, 1, directory.NodeInfo{Addr: "10.0.0.1:7001"}); err != nil {
		t.Fatal(err)
	}
	if err := d.RegisterNode(ctx, 2, directory.NodeInfo{Addr: "10.0.0.2:7002"}); err != nil {
		t.Fatal(err)
	}

	// 总表漂移 field（nodeID 段非数值）：跳过不炸 + 惰性清理（读面顺带
	// HDEL——B3 总表批后漂移守卫从 SCAN 键名检查移到 HGETALL field 解析）
	drift := goredis.NewClient(&goredis.Options{Addr: addr, Password: redisPassword()})
	defer drift.Close()
	if err := drift.HSet(ctx, prefix+"rans:{reg}:nodes", "notanumber", "1").Err(); err != nil {
		t.Fatal(err)
	}

	got, err := d.NodeIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !equalNodes(got, []rans.NodeID{1, 2}) {
		t.Fatalf("NodeIDs = %v, want [1 2] (sorted, entity-owner node 3 excluded, drift field skipped)", got)
	}
	// 漂移 field 被惰性清理摘除（下一读不见）
	if n, err := drift.HLen(ctx, prefix+"rans:{reg}:nodes").Result(); err != nil || n != 2 {
		t.Fatalf("roster must shed the drift field: HLen=%d err=%v, want 2", n, err)
	}

	// 接线形态：redis 目录直接作为 View 数据源
	v := NewView(d.NodeIDs)
	if nodes, err := v.AliveNodes(ctx); err != nil || !equalNodes(nodes, []rans.NodeID{1, 2}) {
		t.Fatalf("View(redis).AliveNodes = %v, %v; want [1 2]", nodes, err)
	}
}
