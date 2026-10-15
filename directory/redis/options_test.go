package redis

// 票 16 红测（构造面断言）：Options 补齐——哨兵 MasterName、ACL
// Username、TLS、拨号/读写超时、显式 Cluster（单种子也走 Cluster 客户端）、
// ContextTimeoutEnabled 恒开（ctx 预算真正约束 Redis IO）。

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func clientOf(t *testing.T, d *Directory) goredis.UniversalClient {
	t.Helper()
	return d.cli
}

func TestNew_MasterNameSelectsFailover(t *testing.T) {
	d := New(Options{
		Addrs:      []string{"127.0.0.1:26379"},
		MasterName: "mymaster",
	})
	defer d.Close()
	if _, ok := clientOf(t, d).(*goredis.Client); !ok {
		t.Fatalf("MasterName must select the failover (sentinel) client, got %T", d.cli)
	}
}

func TestNew_SingleSeedClusterUsesClusterClient(t *testing.T) {
	d := New(Options{
		Addr:    "127.0.0.1:7000",
		Cluster: true, // 单种子显式声明集群——跟随 MOVED 的前提
	})
	defer d.Close()
	if _, ok := clientOf(t, d).(*goredis.ClusterClient); !ok {
		t.Fatalf("single-seed Cluster must construct a ClusterClient, got %T", d.cli)
	}
	// 多地址缺省仍是 Cluster（go-redis Universal 规则不变）。
	d2 := New(Options{Addrs: []string{"127.0.0.1:7000", "127.0.0.1:7001"}})
	defer d2.Close()
	if _, ok := clientOf(t, d2).(*goredis.ClusterClient); !ok {
		t.Fatalf("multi-addr must construct a ClusterClient, got %T", d2.cli)
	}
}

func TestNew_ContextTimeoutEnabled(t *testing.T) {
	// 修复前：Options 无该面（编译红承载）；行为面经慢命令门禁测试锚定
	//（TestDirectory_CtxDeadlineBoundsRedisIO）。本断言锚「构造即恒开」：
	// 单地址客户端的选项快照可经 Options() 读回。
	d := New(Options{Addr: "127.0.0.1:16379", Username: "u1", DialTimeout: 2 * time.Second})
	defer d.Close()
	c, ok := clientOf(t, d).(*goredis.Client)
	if !ok {
		t.Fatalf("single addr must construct a plain client, got %T", d.cli)
	}
	o := c.Options()
	if o == nil || !o.ContextTimeoutEnabled {
		t.Fatal("ContextTimeoutEnabled must be forced on (ctx budgets must govern Redis IO)")
	}
	if o.Username != "u1" || o.DialTimeout != 2*time.Second { // 超时/ACL 面透传
		t.Fatalf("username/dial timeout not mapped: %+v", o)
	}
}

func TestDirectory_CtxDeadlineBoundsRedisIO(t *testing.T) {
	addr := redisAddr() // 包内既有测试同款（门控注入）
	d := New(Options{Addr: addr})
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	// CLIENT PAUSE ALL：服务端阻塞一切命令 1s——ctx 300ms 必须先到
	//（修复前 go-redis 不受 ctx 约束，等到自身读超时，缺省 3s）。
	if err := d.cli.Do(context.Background(), "CLIENT", "PAUSE", 1000, "ALL").Err(); err != nil {
		t.Fatalf("client pause: %v", err)
	}
	start := time.Now()
	_, err := d.NodeIDs(ctx)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("paused redis must not answer within the ctx budget")
	}
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("NodeIDs took %v — ctx deadline did not bound Redis IO (want ~300ms)", elapsed)
	}
}
