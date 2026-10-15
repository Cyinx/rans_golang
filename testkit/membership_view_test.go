package testkit

// 票 01 红测（集成面，移植审查 eval R3/R3b）：路由热路径与放置不再逐跳/
// 逐 miss 读注册表（GET/HGETALL 进入视图周期节奏）；死 owner 在约一个刷
// 新周期内收敛到接管者（既有形态①–④不回归）。

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func cmdStats(t *testing.T, cli *goredis.Client) map[string]int64 {
	t.Helper()
	s, err := cli.Info(context.Background(), "commandstats").Result()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "cmdstat_") {
			continue
		}
		name, rest, _ := strings.Cut(strings.TrimPrefix(line, "cmdstat_"), ":")
		for _, kv := range strings.Split(rest, ",") {
			k, v, _ := strings.Cut(kv, "=")
			if k == "calls" {
				n, _ := strconv.ParseInt(v, 10, 64)
				out[name] = n
			}
		}
	}
	return out
}

func viewRedisCli(t *testing.T) *goredis.Client {
	t.Helper()
	cli := goredis.NewClient(&goredis.Options{Addr: redisAddr()})
	t.Cleanup(func() { cli.Close() })
	return cli
}

// TestRouting_RemoteCallsDoNotReadRegistryPerHop 预热后 1000 次远程调用期间
// Redis GET ≈ 视图刷新次数级（每秒一次 × 节点数），远小于 1000（修复前
// 每跳恰好 1000 次——审查 R3 实测 1.00 GET/调用）。
func TestRouting_RemoteCallsDoNotReadRegistryPerHop(t *testing.T) {
	mustRedis(t)
	cli := viewRedisCli(t)
	c := NewCluster(t, 2, counterKinds(nil), WithRedis(redisAddr(), time.Minute, 30*time.Second))
	ctx := context.Background()
	id := mustID(t, "counter", "hot")
	if _, err := c.Node(0).Call(ctx, id, 0); err != nil { // 激活于 node1
		t.Fatalf("activate: %v", err)
	}
	if _, err := c.Node(1).Call(ctx, id, 0); err != nil { // node2 正缓存回填
		t.Fatalf("warm: %v", err)
	}
	if err := cli.ConfigResetStat(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	const n = 1000
	for i := 0; i < n; i++ {
		if _, err := c.Node(1).Call(ctx, id, 0); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	st := cmdStats(t, cli)
	gets := st["get"]
	// 视图刷新（2 节点 × ~1s 周期 × 测量窗 ~2s ≈ 4 次）+ 位置缓存失效等
	// 少量残余——远小于调用量级即成立。
	if gets > n/10 {
		t.Fatalf("GET per remote call = %.2f (total %d): per-hop registry reads not eliminated", float64(gets)/n, gets)
	}
	t.Logf("%d remote calls -> %d GET (view-refresh cadence)", n, gets)
}

// TestPlacement_CandidatesFromView 视图就绪后新实体首调不触发注册表
// HGETALL（放置候选读快照；修复前每次 routeMiss 一次 HGETALL——审查
// R3b 实测每新实体 1 HGETALL + N GET）。
func TestPlacement_CandidatesFromView(t *testing.T) {
	mustRedis(t)
	cli := viewRedisCli(t)
	c := NewCluster(t, 3, counterKinds(nil), WithRedis(redisAddr(), time.Minute, 30*time.Second), WithPlacement())
	ctx := context.Background()
	// 预热（三节点互连 + 视图首刷）。
	for i := 0; i < 30; i++ {
		if _, err := c.Node(0).Call(ctx, mustID(t, "counter", fmt.Sprintf("warm-%d", i)), 0); err != nil {
			t.Fatalf("warm %d: %v", i, err)
		}
	}
	time.Sleep(1200 * time.Millisecond) // 等视图至少一轮周期刷新就绪
	if err := cli.ConfigResetStat(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	const n = 50
	for i := 0; i < n; i++ {
		if _, err := c.Node(0).Call(ctx, mustID(t, "counter", fmt.Sprintf("new-%d", i)), 0); err != nil {
			t.Fatalf("first call %d: %v", i, err)
		}
	}
	st := cmdStats(t, cli)
	if st["hgetall"] >= n/2 {
		t.Fatalf("HGETALL per placement = %.2f (total %d): placement still reads the registry per miss", float64(st["hgetall"])/n, st["hgetall"])
	}
	t.Logf("%d first calls -> hgetall=%d get=%d", n, st["hgetall"], st["get"])
}

// TestRouting_DeadOwnerConvergesWithinRefresh owner 成员键被清（模拟成员
// 面死亡）后，正缓存命中的调用在一个刷新周期 + 缓冲内改道接管——死
// owner 不再被当作有效路由目标（v0.26 前由接管通知/自围栏承担真实死亡
// 路径，此处只锚路由面的视图收敛面）。
func TestRouting_DeadOwnerConvergesWithinRefresh(t *testing.T) {
	mustRedis(t)
	c := NewCluster(t, 2, counterKinds(nil), WithRedis(redisAddr(), 1500*time.Millisecond, 300*time.Millisecond))
	ctx := context.Background()
	id := mustID(t, "counter", "converge")
	if _, err := c.Node(0).Call(ctx, id, 0); err != nil { // owner = node1
		t.Fatalf("activate: %v", err)
	}
	if _, err := c.Node(1).Call(ctx, id, 0); err != nil { // node2 正缓存 ← node1
		t.Fatalf("warm: %v", err)
	}
	// 清 node1 的成员面（判活键 + 总表 field——KillMembership 同款干预，
	// 但节点本体保持存活：只锚路由面的视图收敛，不混入接管通知/自围栏）。
	c.KillMembership(0)
	deadline := time.Now().Add(3 * time.Second)
	converged := false
	for time.Now().Before(deadline) {
		if _, err := c.Node(1).Call(ctx, id, 0); err == nil {
			converged = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !converged {
		t.Fatal("calls did not converge to a taker within one refresh period + slack")
	}
}
