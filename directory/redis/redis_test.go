package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/contract"
	goredis "github.com/redis/go-redis/v9"
)

// redisAddr 返回测试 Redis 地址：RANS_TEST_REDIS_ADDR 优先，
// 默认 127.0.0.1:16379（make test-redis-up 拉起同一容器）。
func redisAddr() string {
	if v := os.Getenv("RANS_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:16379"
}

// redisPassword 返回测试 Redis 密码：RANS_TEST_REDIS_PASSWORD（内网带认证
// Redis 直连模式用；docker 本地实例无密码，缺省为空）。
func redisPassword() string { return os.Getenv("RANS_TEST_REDIS_PASSWORD") }

// sweepTestKeys 登记测试残键清扫：t.Cleanup 里 SCAN+DEL 本
// KeyPrefix 前缀下全部键——设计性无 TTL 的键族（实体主键（墓碑）、
// {reg}:nodes 注册总表、{reg}:epoch 代际 Hash）随测试结束
// 物理清除，共享 Redis 实例不随测试轮次累积。仅测试侧使用：生产 Close
// 只关客户端绝不清键（红线）。必须在夹具构造点调用（登记早于其余
// Cleanup）——LIFO 保证清扫在目录 Close 之后执行，届时本前缀已无写入方。
func sweepTestKeys(t *testing.T, addrs []string, prefix string) {
	t.Helper()
	cli := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: addrs, Password: redisPassword()})
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
				// 逐实体 hash tag 后测试键散布多 slot：单键逐删（cluster 下
				// 多键 DEL 是 CROSSSLOT）。
				for _, k := range keys {
					if err := cli.Del(ctx, k).Err(); err != nil {
						t.Errorf("sweep test keys (prefix %s): del %s: %v", prefix, k, err)
						return
					}
				}
			}
			cursor = next
			if cursor == 0 {
				return
			}
		}
	})
}

// mustConnect 探活门禁：Redis 不可达即整测试 skip——
// bare `go test ./...`（无 Redis）必须仍然全绿。
func mustConnect(t *testing.T) *goredis.Client {
	t.Helper()
	addr := redisAddr()
	cli := goredis.NewClient(&goredis.Options{Addr: addr, Password: redisPassword(), DialTimeout: 500 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		cli.Close()
		t.Skipf("redis unavailable at %s — run make test-redis-up", addr)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// newTestDir 以独立 KeyPrefix 构造被测目录：前缀含 t.Name() + UnixNano，
// 子测试间隔离、-count=N 重跑间也隔离（设计性无 TTL 的主键不会串场）。
// hbTTL<=0 时落到实现默认值（2min）。
func newTestDir(t *testing.T, hbTTL time.Duration) *Directory {
	t.Helper()
	prefix := fmt.Sprintf("rans:test:%s:%d:", t.Name(), time.Now().UnixNano())
	sweepTestKeys(t, []string{redisAddr()}, prefix) // 残键清扫，LIFO 在 Close 后执行
	d := New(Options{
		Addr:         redisAddr(),
		Password:     redisPassword(),
		HeartbeatTTL: hbTTL,
		KeyPrefix:    prefix,
	})
	t.Cleanup(func() { d.Close() })
	return d
}

func mustUUID(t *testing.T, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID("player", inst)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// epochOf 测试归属代际：每节点固定 epoch（节点号 ×10），模拟成员代际取号
// 的「每节点一值」形态。
func epochOf(node rans.NodeID) uint64 { return uint64(node) * 10 }

// selfOf 测试归属三元构造（inc 由目录派生，入参 0 被忽略）。
func selfOf(node rans.NodeID) directory.Owner {
	return directory.Owner{Node: node, Epoch: epochOf(node)}
}

func TestRegisterSuite(t *testing.T) {
	mustConnect(t)
	contract.RunRegisterSuite(t, func(t *testing.T) directory.Directory {
		return newTestDir(t, 0)
	})
}

// TestRegisterV3_TombstoneKeepsIncMonotonic 墓碑保单调（键值 v3 红测）：
// 注册(inc=1) → 注销(墓碑) → 重注册(inc=2) → 注销 → 接管重注册(inc=3)
// ——inc 严格递增，且「Store 围栏视角」断言每个新纪元严格大于全部历史
// 纪元（被强弃纪元的残留持久写必被拒）。突变对照：墓碑不保留 inc（置 0
// 重开序列）时本测试必红。
func TestRegisterV3_TombstoneKeepsIncMonotonic(t *testing.T) {
	cli := mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)
	id := mustUUID(t, "tomb-mono")

	_, o1, err := d.Register(ctx, id, selfOf(1), nil)
	if err != nil || !won(o1) {
		t.Fatalf("register 1: owner=%v err=%v", o1, err)
	}
	if o1.Incarnation != 1 {
		t.Fatalf("first incarnation = %d, want 1 (fresh sequence)", o1.Incarnation)
	}
	if ok, err := d.UnregisterIfOwner(ctx, id, o1); err != nil || !ok {
		t.Fatalf("unregister 1: ok=%v err=%v", ok, err)
	}
	// 墓碑对 Lookup 不可见，但主键仍在（主键永不清删）——重注册按分支 1
	// 从墓碑残值 +1 起
	if _, found, err := d.Lookup(ctx, id); err != nil || found {
		t.Fatalf("tombstoned entry must miss: found=%v err=%v", found, err)
	}
	_, o2, err := d.Register(ctx, id, selfOf(1), nil)
	if err != nil || !won(o2) {
		t.Fatalf("re-register over tombstone: owner=%v err=%v", o2, err)
	}
	if o2.Incarnation != o1.Incarnation+1 {
		t.Fatalf("re-register inc = %d, want %d (tombstone residue +1)", o2.Incarnation, o1.Incarnation+1)
	}
	if ok, err := d.UnregisterIfOwner(ctx, id, o2); err != nil || !ok {
		t.Fatalf("unregister 2: ok=%v err=%v", ok, err)
	}
	// 他节点接管重注册：纪元仍严格递增（单调性跨墓碑与他节点存续）
	_, o3, err := d.Register(ctx, id, selfOf(2), nil)
	if err != nil || !won(o3) {
		t.Fatalf("takeover re-register over tombstone: owner=%v err=%v", o3, err)
	}
	if o3.Incarnation != o2.Incarnation+1 {
		t.Fatalf("takeover inc = %d, want %d", o3.Incarnation, o2.Incarnation+1)
	}
	// Store 围栏视角：新纪元恒大于全部历史纪元（重注册与接管两侧）
	for _, prev := range []directory.Owner{o1, o2} {
		if o3.Incarnation <= prev.Incarnation {
			t.Fatalf("store fence violated: new inc %d must exceed history %d", o3.Incarnation, prev.Incarnation)
		}
	}
	// 原始值断言：墓碑保留 inc（node/epoch 置零）——纪元单调的载体
	if ok, err := d.UnregisterIfOwner(ctx, id, o3); err != nil || !ok {
		t.Fatalf("unregister 3: ok=%v err=%v", ok, err)
	}
	raw, err := cli.Get(ctx, d.primaryKey(id)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if raw != tombstoneValue(o3.Incarnation) {
		t.Fatalf("tombstone value = %s, want %s (inc preserved, owner zeroed)", raw, tombstoneValue(o3.Incarnation))
	}
}

// won 测试辅助：赢时返回的归属必为本实例（Node 非零即持有者形态）。
func won(o directory.Owner) bool { return o.Node != 0 && o.Incarnation >= 1 }

// TestRegisterV3_TakeoverCAS CAS 接管（键值 v3 红测）：expected 匹配现值
// → 接管赢（写自归属、纪元严格大于旧值）；expected 不匹配（他人先接管）
// → 输并返回新现归属（转发/提示用）。
func TestRegisterV3_TakeoverCAS(t *testing.T) {
	mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)
	id := mustUUID(t, "cas-takeover")

	_, holder, err := d.Register(ctx, id, selfOf(1), nil)
	if err != nil {
		t.Fatal(err)
	}
	// expected 匹配 → 接管赢
	wonRes, cur, err := d.Register(ctx, id, selfOf(2), &holder)
	if err != nil || !wonRes {
		t.Fatalf("matching CAS: won=%v err=%v", wonRes, err)
	}
	if cur.Node != 2 || cur.Epoch != epochOf(2) || cur.Incarnation <= holder.Incarnation {
		t.Fatalf("takeover owner = %v, want {2,%d,inc>%d}", cur, epochOf(2), holder.Incarnation)
	}
	// 陈旧 expected（他人已接管）→ 输并返回新现归属
	wonRes, cur2, err := d.Register(ctx, id, selfOf(3), &holder)
	if err != nil || wonRes {
		t.Fatalf("stale CAS: won=%v err=%v", wonRes, err)
	}
	if cur2 != cur {
		t.Fatalf("lost CAS owner = %v, want current holder %v", cur2, cur)
	}
	if got, found, _ := d.Lookup(ctx, id); !found || got != cur {
		t.Fatalf("entry after lost CAS: %v found=%v, want %v", got, found, cur)
	}
	// 无 expected 的裸注册同样输（他实例持有）
	wonRes, cur3, err := d.Register(ctx, id, selfOf(4), nil)
	if err != nil || wonRes || cur3 != cur {
		t.Fatalf("plain register over held entry: won=%v owner=%v err=%v, want lose with holder %v", wonRes, cur3, err, cur)
	}
}

// TestKeyTag_PerEntitySharding 逐实体 hash tag 分片（键值 v3 红测；
// cluster 形态实测）：多实体主键的 tag 互异且经真 CLUSTER KEYSLOT 落于
// 多个 slot（目录真分片）；同实体的全部单键操作天然同 slot（单键命令
// 无跨键面）。单实例形态（CLUSTER KEYSLOT 不可用）由
// TestDirectoryClusterSlotUnity 以「第一对 {} 内文本定槽」语义做确定性
// 等价断言。
func TestKeyTag_PerEntitySharding(t *testing.T) {
	addrs := mustConnectCluster(t)
	ctx := context.Background()
	cli := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addrs, Password: redisPassword()})
	defer cli.Close()
	d := newClusterTestDir(t, addrs, 0)

	ids := make([]rans.ID, 0, 6)
	for i := 0; i < 6; i++ {
		id, err := rans.NewID(fmt.Sprintf("kind%d", i%3), fmt.Sprintf("shard-%02d", i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	slots := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		key := d.primaryKey(id)
		// 同实体：主键即唯一操作键，CLUSTER KEYSLOT 与 tag 内身份一致
		n, err := cli.ClusterKeySlot(ctx, key).Result()
		if err != nil {
			t.Fatalf("CLUSTER KEYSLOT %q: %v", key, err)
		}
		slots[n] = struct{}{}
		// tag = 完整实体身份（逐字节，含 "/"）
		if tag := hashTagOf(key); tag != id.String() {
			t.Fatalf("primary key %q tag = %q, want identity %q", key, tag, id.String())
		}
	}
	if len(slots) < 2 {
		t.Fatalf("per-entity keys must spread over >1 slot, got slots %v (ids %v)", slots, ids)
	}
}

// hashTagOf 提取键名第一对 {} 内文本（Redis Cluster 定槽语义的本地镜像）。
func hashTagOf(key string) string {
	open := strings.IndexByte(key, '{')
	if open < 0 {
		return ""
	}
	if close := strings.IndexByte(key[open:], '}'); close > 0 {
		return key[open+1 : open+close]
	}
	return ""
}

// TestRegisterLua_Semantics luaRegister（注册/收养/接管原子域）语义四态
// （真 Redis）：①全新 id 注册即赢且主键写 v3 值；②他实例持有输且返回现
// 归属（三元）；③同 node+epoch 收养升纪元；④漂移值（非 JSON 垃圾）视同
// 无值收养覆写 v3（自愈面，不做混跑兼容）。
func TestRegisterLua_Semantics(t *testing.T) {
	cli := mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)

	t.Run("全新 id 注册即赢且主键写 v3 值", func(t *testing.T) {
		id := mustUUID(t, "reg-fresh")
		wonRes, owner, err := d.Register(ctx, id, selfOf(5), nil)
		if err != nil || !wonRes {
			t.Fatalf("fresh register: won=%v err=%v", wonRes, err)
		}
		if owner.Incarnation < 1 {
			t.Fatalf("fresh register incarnation = %d, want ≥1", owner.Incarnation)
		}
		raw, err := cli.Get(ctx, d.primaryKey(id)).Result()
		if err != nil {
			t.Fatal(err)
		}
		if want := entryValue(5, epochOf(5), owner.Incarnation); raw != want {
			t.Fatalf("primary value = %s, want %s (v3 format)", raw, want)
		}
	})

	t.Run("他实例持有：输且返回现归属（三元）", func(t *testing.T) {
		id := mustUUID(t, "reg-held")
		if _, _, err := d.Register(ctx, id, selfOf(1), nil); err != nil {
			t.Fatal(err)
		}
		holder, found, err := d.Lookup(ctx, id)
		if err != nil || !found {
			t.Fatalf("precondition lookup: found=%v err=%v", found, err)
		}
		wonRes, owner, err := d.Register(ctx, id, selfOf(2), nil)
		if err != nil {
			t.Fatal(err)
		}
		if wonRes {
			t.Fatal("register from another node must lose")
		}
		if owner != holder {
			t.Fatalf("lost register owner = %v, want holder %v (with epoch)", owner, holder)
		}
	})

	t.Run("同 node+epoch 收养升纪元", func(t *testing.T) {
		id := mustUUID(t, "reg-adopt")
		_, first, err := d.Register(ctx, id, selfOf(3), nil)
		if err != nil {
			t.Fatal(err)
		}
		wonRes, owner, err := d.Register(ctx, id, selfOf(3), nil)
		if err != nil || !wonRes {
			t.Fatalf("self-pointing register: won=%v err=%v", wonRes, err)
		}
		if owner.Incarnation <= first.Incarnation {
			t.Fatalf("adoption must bump incarnation: %d then %d", first.Incarnation, owner.Incarnation)
		}
		if owner.Node != 3 || owner.Epoch != epochOf(3) {
			t.Fatalf("adopted owner = %v, want {3,%d,inc}", owner, epochOf(3))
		}
	})

	t.Run("漂移值视同无值收养覆写 v3", func(t *testing.T) {
		id := mustUUID(t, "reg-drift")
		if err := cli.Set(ctx, d.primaryKey(id), "\x00not json", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		wonRes, owner, err := d.Register(ctx, id, selfOf(7), nil)
		if err != nil || !wonRes {
			t.Fatalf("drift-value register: won=%v err=%v", wonRes, err)
		}
		if owner.Incarnation != 1 {
			t.Fatalf("adopted incarnation = %d, want 1 (no residue in garbage)", owner.Incarnation)
		}
		raw, err := cli.Get(ctx, d.primaryKey(id)).Result()
		if err != nil {
			t.Fatal(err)
		}
		if want := entryValue(7, epochOf(7), 1); raw != want {
			t.Fatalf("primary value after adoption = %s, want %s (overwritten to v3)", raw, want)
		}
	})
}

// TestTransferLua_BumpsIncarnation luaTransfer 换锁升纪元（实例级 CAS）：
// 换锁后主键值含新归属三元（v3：目标 node+epoch + 新 inc = 旧值+1）；
// 陈旧纪元不再换锁（ErrNotOwner）；缺失条目 (false, zero, nil)。
func TestTransferLua_BumpsIncarnation(t *testing.T) {
	mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)
	id := mustUUID(t, "xfersinc")

	_, owner, err := d.Register(ctx, id, selfOf(1), nil)
	if err != nil || owner.Incarnation != 1 {
		t.Fatalf("fresh register: owner=%v err=%v, want true 1 (fresh sequence)", owner, err)
	}
	wonRes, newInc, cur, err := d.Transfer(ctx, id, owner, selfOf(2))
	if err != nil || !wonRes {
		t.Fatalf("transfer: won=%v err=%v", wonRes, err)
	}
	if newInc != owner.Incarnation+1 {
		t.Fatalf("transfer incarnation = %d, want %d (bump by exactly one)", newInc, owner.Incarnation+1)
	}
	if cur != (directory.Owner{Node: 2, Epoch: epochOf(2), Incarnation: newInc}) {
		t.Fatalf("transfer current = %v, want {2,%d,%d}", cur, epochOf(2), newInc)
	}
	raw, err := d.cli.Get(ctx, d.primaryKey(id)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if want := entryValue(2, epochOf(2), newInc); raw != want {
		t.Fatalf("primary value after transfer = %s, want %s (v3 with target owner)", raw, want)
	}
	// 陈旧纪元（换锁前的 owner）不再换锁：实例级 mismatch → ErrNotOwner
	if _, _, _, err := d.Transfer(ctx, id, owner, selfOf(3)); !errors.Is(err, directory.ErrNotOwner) {
		t.Fatalf("stale owner transfer: err=%v, want ErrNotOwner", err)
	}
}

func TestNodeMembershipSuite(t *testing.T) {
	mustConnect(t)
	contract.RunNodeMembershipSuite(t, newEpochSuiteReg)
}

// newEpochSuiteReg 成员契约套件的独立目录构造（前缀隔离 + 残键清扫 + 注入
// 过期阈值），成员套件与代际套件共用。
func newEpochSuiteReg(t *testing.T, hbTTL time.Duration) directory.NodeRegistry {
	t.Helper()
	prefix := fmt.Sprintf("rans:test:%s:%d:", t.Name(), time.Now().UnixNano())
	sweepTestKeys(t, []string{redisAddr()}, prefix) // 残键清扫
	d := New(Options{
		Addr:         redisAddr(),
		Password:     redisPassword(),
		HeartbeatTTL: hbTTL,
		KeyPrefix:    prefix,
	})
	t.Cleanup(func() { d.Close() })
	return d
}

// guardRaceHook 守卫注销时序注入钩子：武装后，在守卫注销 Lua 成功返回的
// 那一刻（单命令 eval/evalsha 面）、收尾 pipeline 执行之前，注入一次同
// NodeID 的重注册——复现「迟到的停机注销守卫放行后、让位者节点重开」的
// 窄窗。RegisterNode 走 pipeline 面（ProcessPipelineHook 直通），不会递归
// 本钩子；守卫拒绝（回复非 1）不注入。
type guardRaceHook struct {
	dir    *Directory
	node   rans.NodeID
	info   directory.NodeInfo
	armed  bool
	fired  bool
	regErr error
}

func (h *guardRaceHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h *guardRaceHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

func (h *guardRaceHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		err := next(ctx, cmd)
		if !h.armed || err != nil || !isUnregNodeGuardCmd(cmd) {
			return err
		}
		if c, ok := cmd.(*goredis.Cmd); ok {
			if v, vErr := c.Int64(); vErr != nil || v != 1 {
				return err // 守卫拒绝：注入前提不成立
			}
		}
		h.armed = false
		h.fired = true
		h.regErr = h.dir.RegisterNode(ctx, h.node, h.info)
		return err
	}
}

// isUnregNodeGuardCmd 识别守卫注销脚本的执行命令：EVALSHA 命中形态第 2 参
// 是脚本 sha，NOSCRIPT 兜底的 EVAL 形态第 2 参是脚本原文——两者都与
// scriptUnregNode 同源比对。
func isUnregNodeGuardCmd(cmd goredis.Cmder) bool {
	switch cmd.Name() {
	case "eval", "evalsha":
	default:
		return false
	}
	args := cmd.Args()
	if len(args) < 2 {
		return false
	}
	ident, ok := args[1].(string)
	if !ok {
		return false
	}
	return ident == scriptUnregNode.Hash() || ident == luaUnregisterNodeGuard
}

// TestUnregisterNode_GuardRaceKeepsReplacedKey 守卫注销迟到重入（真 Redis）：
// 守卫 Lua 成功删旧键后、收尾 pipeline 执行前，同 NodeID 重开节点重注册
// 新代际成员键——收尾不得补删新键。误删后果：让位者的 RenewNode 撞缺席
// （缺席不重建）被误判死亡。时序注入见 guardRaceHook；inmem 无守卫/收尾
// 两段结构，此竞态不存在，不经契约套件覆盖。
func TestUnregisterNode_GuardRaceKeepsReplacedKey(t *testing.T) {
	mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)

	if err := d.RegisterNode(ctx, 7, directory.NodeInfo{Addr: "10.0.0.7:1", Epoch: 7}); err != nil {
		t.Fatal(err)
	}
	hook := &guardRaceHook{
		dir:   d,
		node:  7,
		info:  directory.NodeInfo{Addr: "10.0.0.7:2", Epoch: 8},
		armed: true,
	}
	d.cli.AddHook(hook)

	// 迟到的停机注销：携带旧代际，守卫按键内 epoch==7 放行并删除旧键。
	if err := d.UnregisterNode(ctx, 7, 7); err != nil {
		t.Fatal(err)
	}
	if !hook.fired || hook.regErr != nil {
		t.Fatalf("injected re-register did not run cleanly: fired=%v regErr=%v", hook.fired, hook.regErr)
	}
	// 让位者的新键必须存活（守卫已删旧键；收尾只清总表 field——超集惰性
	// 收敛面，让位者的下一次续期 HSet 即自愈，与键丢失的假死亡不同级）。
	addr, ok, err := d.NodeAddr(ctx, 7)
	if err != nil || !ok || addr != "10.0.0.7:2" {
		t.Fatalf("replaced key after guarded unregister = (%q,%v,%v), want (10.0.0.7:2,true,nil)", addr, ok, err)
	}
}

// TestNodeEpoch_MonotonicAcrossRestart 节点代际单调（真 Redis）：取号全局
// 单调，同节点号「注册 → 停（注销/判活键过期）→ 再注册」新代际严格更大，
// 优雅停机注销不触碰计数器（契约套件平移，inmem 同型测试见 directory/inmem）。
func TestNodeEpoch_MonotonicAcrossRestart(t *testing.T) {
	mustConnect(t)
	contract.RunNodeEpochMonotonicSuite(t, newEpochSuiteReg)
}

// TestMemberView_NodeEpochQuery 成员视图代际查询（真 Redis）：多节点注册
// 后各存活节点的 epoch 可查且各归各，未注册/判活键过期查不到。
func TestMemberView_NodeEpochQuery(t *testing.T) {
	mustConnect(t)
	contract.RunNodeEpochQuerySuite(t, newEpochSuiteReg)
}

// TestNodeAddrResolver Resolver 注入形态（rpc.Config.Resolve
// 的 redis 目录派生实现——读成员键）：注册节点可解析出地址；未注册节点
// 返回错误（懒建连接的上游按发送重试环重试）。
func TestNodeAddrResolver(t *testing.T) {
	mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)

	resolve := d.NodeAddrResolver()
	if err := d.RegisterNode(ctx, 11, directory.NodeInfo{Addr: "10.1.1.11:7111"}); err != nil {
		t.Fatal(err)
	}
	addr, err := resolve(ctx, 11)
	if err != nil || addr != "10.1.1.11:7111" {
		t.Fatalf("resolve(11) = (%q,%v), want (10.1.1.11:7111,nil)", addr, err)
	}
	if _, err := resolve(ctx, 12); err == nil {
		t.Fatal("resolve of unregistered node must return an error")
	}
}

// TestNodeKeyValueJSONFormat 判活键值格式（决策出处：
// docs/decisions/2026-10-05-node-key-kinds.md）：线上形态是
// {"addr":...,"kinds":[...]}——kinds 非空与 nil（规范化为 []）两形态直读
// 原始键断言；NodeAddr 经 JSON 解析往返取 addr。总表值仍为时间戳（kinds
// 不进总表——枚举面与判活面职责分离不变）。
func TestNodeKeyValueJSONFormat(t *testing.T) {
	cli := mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)

	if err := d.RegisterNode(ctx, 21, directory.NodeInfo{Addr: "127.0.0.1:1", Kinds: []string{"workspace", "greet"}}); err != nil {
		t.Fatal(err)
	}
	raw, err := cli.Get(ctx, d.nodeKey(21)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"addr":"127.0.0.1:1","kinds":["workspace","greet"]}`; raw != want {
		t.Fatalf("node key value = %s, want %s (ruling format)", raw, want)
	}
	// nil kinds 规范化为空数组：纯心跳节点的合法形态
	if err := d.RegisterNode(ctx, 22, directory.NodeInfo{Addr: "127.0.0.1:2"}); err != nil {
		t.Fatal(err)
	}
	raw, err = cli.Get(ctx, d.nodeKey(22)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"addr":"127.0.0.1:2","kinds":[]}`; raw != want {
		t.Fatalf("node key value (nil kinds) = %s, want %s", raw, want)
	}

	// NodeAddr 解析新格式（JSON 解码取 addr）
	if addr, ok, err := d.NodeAddr(ctx, 21); err != nil || !ok || addr != "127.0.0.1:1" {
		t.Fatalf("NodeAddr(21) = (%q,%v,%v), want (127.0.0.1:1,true,nil)", addr, ok, err)
	}
	// 总表值仍是时间戳：kinds 不进总表
	rosterVal, err := cli.HGet(ctx, d.rosterKey(), "21").Result()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := strconv.ParseInt(rosterVal, 10, 64); err != nil {
		t.Fatalf("roster value %q must stay a numeric renewal timestamp (kinds live in the node key only): %v", rosterVal, err)
	}
}

// TestNodeKeyValueEpochField 判活键值携带节点代际（值格式增 epoch 字段）：
// 携带形态直读原始键断言（epoch 入 JSON）；未携带（Epoch=0）缺省不写、
// NodeEpoch 读回 0——未接代际面的调用方写法保持透明；总表值仍为时间戳
// （代际只载于判活键值）。
func TestNodeKeyValueEpochField(t *testing.T) {
	cli := mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)

	if err := d.RegisterNode(ctx, 26, directory.NodeInfo{Addr: "127.0.0.1:6", Kinds: []string{"workspace"}, Epoch: 7}); err != nil {
		t.Fatal(err)
	}
	raw, err := cli.Get(ctx, d.nodeKey(26)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"addr":"127.0.0.1:6","kinds":["workspace"],"epoch":7}`; raw != want {
		t.Fatalf("node key value with epoch = %s, want %s", raw, want)
	}
	if e, ok, err := d.NodeEpoch(ctx, 26); err != nil || !ok || e != 7 {
		t.Fatalf("NodeEpoch(26) = (%d,%v,%v), want (7,true,nil)", e, ok, err)
	}

	// 未携带形态（Epoch=0）：JSON 缺省不写，读回 0
	if err := d.RegisterNode(ctx, 27, directory.NodeInfo{Addr: "127.0.0.1:7"}); err != nil {
		t.Fatal(err)
	}
	raw, err = cli.Get(ctx, d.nodeKey(27)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"addr":"127.0.0.1:7","kinds":[]}`; raw != want {
		t.Fatalf("node key value without epoch = %s, want %s (epoch omitted when zero)", raw, want)
	}
	if e, ok, err := d.NodeEpoch(ctx, 27); err != nil || !ok || e != 0 {
		t.Fatalf("NodeEpoch(27) = (%d,%v,%v), want (0,true,nil)", e, ok, err)
	}
}

// TestNodeKeyBadValueErrors 坏值（非 JSON）报错不 panic（旧值=裸 addr
// 字符串不解析不兼容，双端同版本）。三个读面（NodeAddr /
// NodeIDs / NodesByKind）对坏值一律返回错误——不做混跑的响亮失败，静默
// 跳过会把「集群混着旧版本」伪装成节点消失。
func TestNodeKeyBadValueErrors(t *testing.T) {
	cli := mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)

	// 形态一：旧版裸 addr 值（历史写入形态，仅出现在残留键上）
	if err := d.RegisterNode(ctx, 31, directory.NodeInfo{Addr: "10.0.0.31:1"}); err != nil {
		t.Fatal(err)
	}
	if err := cli.Set(ctx, d.nodeKey(31), "10.0.0.31:1", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.NodeAddr(ctx, 31); err == nil {
		t.Fatal("NodeAddr on legacy bare-addr value must return an error (no mixed-run)")
	}
	if _, err := d.NodeIDs(ctx); err == nil {
		t.Fatal("NodeIDs must surface the malformed node key value as an error")
	}
	if _, err := d.NodesByKind(ctx, "workspace"); err == nil {
		t.Fatal("NodesByKind must surface the malformed node key value as an error")
	}

	// 形态二：任意非 JSON 垃圾值
	if err := cli.Set(ctx, d.nodeKey(31), "\x00not json", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.NodeAddr(ctx, 31); err == nil {
		t.Fatal("NodeAddr on non-JSON value must return an error")
	}

	// 形态三：合法 JSON 但结构不符（kinds 非数组——解码失败面）
	if err := cli.Set(ctx, d.nodeKey(31), `{"addr":"10.0.0.31:1","kinds":"workspace"}`, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.NodeAddr(ctx, 31); err == nil {
		t.Fatal("NodeAddr on structurally invalid JSON must return an error")
	}

	// 形态四：合法 JSON 且结构相符但 addr 缺失/空：null/{} /缺 addr 字段
	// 的值不再当合法——addr 是判活键值的必填成分，空地址入视图只会产出
	// 必然拨号失败的 Resolve 候选。
	for _, bad := range []string{"null", `{}`, `{"kinds":["workspace"]}`, `{"addr":"","kinds":["workspace"]}`} {
		if err := cli.Set(ctx, d.nodeKey(31), bad, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := d.NodeAddr(ctx, 31); err == nil {
			t.Fatalf("NodeAddr on %q must return an error (addr must not be empty)", bad)
		}
	}
	if _, err := d.NodeIDs(ctx); err == nil {
		t.Fatal("NodeIDs must surface the missing-addr value as an error")
	}
	if _, err := d.NodesByKind(ctx, "workspace"); err == nil {
		t.Fatal("NodesByKind must surface the missing-addr value as an error")
	}

	// 自愈面：心跳重注册覆写坏值后读面恢复（活节点每周期双写，坏值最迟
	// 一个周期被覆盖——外力损坏的收敛路径）
	if err := d.RegisterNode(ctx, 31, directory.NodeInfo{Addr: "10.0.0.31:2", Kinds: []string{"workspace"}}); err != nil {
		t.Fatal(err)
	}
	if addr, ok, err := d.NodeAddr(ctx, 31); err != nil || !ok || addr != "10.0.0.31:2" {
		t.Fatalf("after re-register: NodeAddr = (%q,%v,%v), want (10.0.0.31:2,true,nil)", addr, ok, err)
	}
	if nodes, err := d.NodesByKind(ctx, "workspace"); err != nil || len(nodes) != 1 || nodes[0] != 31 {
		t.Fatalf("after re-register: NodesByKind(workspace) = %v (%v), want [31]", nodes, err)
	}
}

// TestEntryValueV3Format 主键值 v3 格式（Lua string.match 按字段名全量
// 解析的格式假设）：字段名/顺序/数值边界——`"inc":32` 解析为 32 而非 3
// （正则全量吞位，无子串边界问题）；墓碑形态 node/epoch 置零、inc 保留；
// 解析往返一致。
func TestEntryValueV3Format(t *testing.T) {
	v := entryValue(6, 7, 32)
	if v != `{"node":6,"epoch":7,"inc":32,"v":3}` {
		t.Fatalf("entryValue(6,7,32) = %s, want v3 field order", v)
	}
	e, ok := parseEntry(v)
	if !ok || e.Node != 6 || e.Epoch != 7 || e.Incarnation != 32 {
		t.Fatalf("parseEntry roundtrip = %+v ok=%v, want exact fields", e, ok)
	}
	// 数值边界（原子串守卫的等价形态）：32 的 inc 不被解析为 3
	e32, _ := parseEntry(entryValue(65, 8, 32))
	if e32.Node != 65 || e32.Incarnation != 32 {
		t.Fatalf("multi-digit fields must parse exactly: %+v", e32)
	}
	// 墓碑：node/epoch 置零、inc 保留
	tb := tombstoneValue(9)
	if tb != `{"node":0,"epoch":0,"inc":9,"v":3}` {
		t.Fatalf("tombstoneValue(9) = %s, want owner-zeroed inc-kept form", tb)
	}
	if e, ok := parseEntry(tb); !ok || e.Node != 0 || e.Incarnation != 9 {
		t.Fatalf("tombstone parse = %+v ok=%v, want node=0 inc=9", e, ok)
	}
	// 垃圾值解析失败 → miss 语义（读面自愈入口）
	if _, ok := parseEntry("junk"); ok {
		t.Fatal("non-JSON value must fail parsing")
	}
}

// TestTransferStates 所有权转移四态（luaTransfer 返回四元组的 Go 映射，
// 实例级形态——期望归属是 Owner 三元组整体）：
// 无条目→(false, zero, nil)；归属不符（他节点/陈旧纪元/墓碑）→ ErrNotOwner
// +现归属；成功→新纪元严格大于旧值，Lookup 指向新归属（node+epoch）。
func TestTransferStates(t *testing.T) {
	mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)

	id := mustUUID(t, "transfer")

	// 态 1：无条目 → (false, zero, nil)
	if wonRes, inc, cur, err := d.Transfer(ctx, id, selfOf(1), selfOf(9)); err != nil || wonRes || inc != 0 || cur != (directory.Owner{}) {
		t.Fatalf("transfer of missing entry: won=%v inc=%d cur=%v err=%v, want false 0 zero nil", wonRes, inc, cur, err)
	}

	// 备好条目（node 1 持有）
	_, regOwner, err := d.Register(ctx, id, selfOf(1), nil)
	if err != nil {
		t.Fatalf("register: err=%v", err)
	}

	// 态 2a：归属不符（他节点期望）→ ErrNotOwner，条目保持原状
	if _, _, cur, err := d.Transfer(ctx, id, selfOf(2), selfOf(9)); cur != regOwner || !errors.Is(err, directory.ErrNotOwner) {
		t.Fatalf("transfer with wrong owner: cur=%v err=%v, want (false, holder, ErrNotOwner)", cur, err)
	}
	// 态 2b：归属不符（期望纪元陈旧——同 node 旧实例）→ 同哨兵
	stale := directory.Owner{Node: 1, Epoch: epochOf(1), Incarnation: regOwner.Incarnation - 1}
	if _, _, _, err := d.Transfer(ctx, id, stale, selfOf(9)); !errors.Is(err, directory.ErrNotOwner) {
		t.Fatalf("transfer with stale incarnation: err=%v, want ErrNotOwner", err)
	}
	if loc, found, err := d.Lookup(ctx, id); err != nil || !found || loc != regOwner {
		t.Fatalf("entry must stay intact: loc=%v found=%v err=%v", loc, found, err)
	}

	// 态 3：成功搬移 → (true, newInc, 目标归属)，新纪元严格大于旧值
	wonRes, newInc, cur, err := d.Transfer(ctx, id, regOwner, selfOf(9))
	if err != nil || !wonRes {
		t.Fatalf("transfer by owner: won=%v err=%v", wonRes, err)
	}
	if newInc <= regOwner.Incarnation {
		t.Fatalf("transfer incarnation %d must exceed old %d", newInc, regOwner.Incarnation)
	}
	if cur != (directory.Owner{Node: 9, Epoch: epochOf(9), Incarnation: newInc}) {
		t.Fatalf("transfer current = %v, want {9,%d,%d}", cur, epochOf(9), newInc)
	}

	// 态 4：搬移后 Lookup 指向新归属（node+epoch 写入验证）
	if loc, found, err := d.Lookup(ctx, id); err != nil || !found || loc != cur {
		t.Fatalf("lookup after transfer: loc=%v found=%v err=%v, want %v", loc, found, err, cur)
	}
}

func TestRegistryRosterDriftConvergence(t *testing.T) {
	cli := mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)

	if err := d.RegisterNode(ctx, 3, directory.NodeInfo{Addr: "10.0.0.3:3"}); err != nil {
		t.Fatal(err)
	}
	// 伪造漂移：总表登记 field 99，判活键缺席（双写撕裂/历史残渣形态）
	if err := cli.HSet(ctx, d.rosterKey(), "99", "10.0.0.99:9").Err(); err != nil {
		t.Fatal(err)
	}

	if nodes, err := d.NodeIDs(ctx); err != nil {
		t.Fatal(err)
	} else if len(nodes) != 1 || nodes[0] != 3 {
		t.Fatalf("NodeIDs = %v, want [3] (drift field 99 without liveness key must be excluded)", nodes)
	}
	// 惰性清理：读面已把死 field 从总表摘除
	if n, err := cli.HLen(ctx, d.rosterKey()).Result(); err != nil || n != 1 {
		t.Fatalf("roster must lazily shed the dead field: HLen=%d err=%v, want 1", n, err)
	}

	// 反向漂移：判活键被外力删除（不等 TTL）→ 即刻退出视图
	if err := cli.Del(ctx, d.nodeKey(3)).Err(); err != nil {
		t.Fatal(err)
	}
	if nodes, err := d.NodeIDs(ctx); err != nil {
		t.Fatal(err)
	} else if len(nodes) != 0 {
		t.Fatalf("NodeIDs = %v after liveness key deleted, want empty", nodes)
	}
}

// TestRegisterNodeTearingSelfHeal 双写撕裂自愈：判活键半写失败（DEL 模拟）
// → 视图排除该节点；下一轮 RegisterNode 双写恢复 → 重新可见且地址更新。
func TestRegisterNodeTearingSelfHeal(t *testing.T) {
	cli := mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)

	if err := d.RegisterNode(ctx, 5, directory.NodeInfo{Addr: "10.0.0.5:1"}); err != nil {
		t.Fatal(err)
	}
	if err := cli.Del(ctx, d.nodeKey(5)).Err(); err != nil { // 撕裂：SET 半边丢失
		t.Fatal(err)
	}
	if nodes, err := d.NodeIDs(ctx); err != nil {
		t.Fatal(err)
	} else if len(nodes) != 0 {
		t.Fatalf("torn node must leave the view, got %v", nodes)
	}

	// 下一轮续命双写：恢复可见，地址随之更新
	if err := d.RegisterNode(ctx, 5, directory.NodeInfo{Addr: "10.0.0.5:2"}); err != nil {
		t.Fatal(err)
	}
	if addr, ok, err := d.NodeAddr(ctx, 5); err != nil || !ok || addr != "10.0.0.5:2" {
		t.Fatalf("after re-register: addr=(%q,%v,%v), want (10.0.0.5:2,true,nil)", addr, ok, err)
	}
	if nodes, err := d.NodeIDs(ctx); err != nil || len(nodes) != 1 || nodes[0] != 5 {
		t.Fatalf("NodeIDs after heal = %v (%v), want [5]", nodes, err)
	}
}

// TestNodeAddrsSnapshot 全量快照：注册两个节点返回「谁活+在哪」，
// 死者（判活键删除）不出现在快照。
func TestNodeAddrsSnapshot(t *testing.T) {
	cli := mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)

	if err := d.RegisterNode(ctx, 7, directory.NodeInfo{Addr: "10.0.0.7:7"}); err != nil {
		t.Fatal(err)
	}
	if err := d.RegisterNode(ctx, 8, directory.NodeInfo{Addr: "10.0.0.8:8"}); err != nil {
		t.Fatal(err)
	}
	m, err := d.NodeAddrs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 2 || m[7] != "10.0.0.7:7" || m[8] != "10.0.0.8:8" {
		t.Fatalf("NodeAddrs = %v, want both alive nodes with addresses", m)
	}

	if err := cli.Del(ctx, d.nodeKey(8)).Err(); err != nil {
		t.Fatal(err)
	}
	m, err = d.NodeAddrs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 1 || m[7] != "10.0.0.7:7" {
		t.Fatalf("NodeAddrs after death = %v, want only node 7", m)
	}
}

// TestRegistryClusterSlotUnity registry 键的 slot 统一性（未来可能有 Lua
// 同时访问总表与节点键——Redis Cluster 要求多键操作同
// slot，键名嵌 hash tag `{reg}` 保证）。Redis 语义保证「第一对 {} 内文本
// 相同的键必落同一 slot」，本地提取 tag 比对即得确定性验证——不依赖
// CLUSTER KEYSLOT（内网实例编译时禁用了 cluster 支持，该命令不可用）。
func TestRegistryClusterSlotUnity(t *testing.T) {
	mustConnect(t)
	d := newTestDir(t, time.Minute)

	rosterTag := hashTagOf(d.rosterKey())
	if rosterTag != "reg" {
		t.Fatalf("roster key %q must carry the {reg} hash tag, got tag %q", d.rosterKey(), rosterTag)
	}
	for _, key := range []string{d.epochKey()} {
		if tag := hashTagOf(key); tag != rosterTag {
			t.Fatalf("registry key %q tag %q differs from roster tag %q -- registry keys would scatter across slots, breaking future multi-key Lua",
				key, tag, rosterTag)
		}
	}
	for _, node := range []rans.NodeID{1, 2, 65535} {
		if tag := hashTagOf(d.nodeKey(node)); tag != rosterTag {
			t.Fatalf("node key %q tag %q differs from roster tag %q -- registry keys would scatter across slots, breaking future multi-key Lua",
				d.nodeKey(node), tag, rosterTag)
		}
	}
}

// TestRegistryRosterTimestampValue 总表 field 值是续命时间戳（为判活
// 三件套预留的初筛面），地址单载于节点键——总表不再冗余地址；
// 重注册时间戳单调不减。
func TestRegistryRosterTimestampValue(t *testing.T) {
	cli := mustConnect(t)
	ctx := context.Background()
	d := newTestDir(t, time.Minute)

	if err := d.RegisterNode(ctx, 4, directory.NodeInfo{Addr: "10.0.0.4:4"}); err != nil {
		t.Fatal(err)
	}
	rosterVal, err := cli.HGet(ctx, d.rosterKey(), "4").Result()
	if err != nil {
		t.Fatal(err)
	}
	ts1, err := strconv.ParseInt(rosterVal, 10, 64)
	if err != nil || ts1 <= 0 {
		t.Fatalf("roster value %q must be a numeric renewal timestamp (unix ms), got err=%v", rosterVal, err)
	}
	// 地址只在节点键：总表值不是地址
	if rosterVal == "10.0.0.4:4" {
		t.Fatal("roster value must not carry the address (address lives in the node key only)")
	}
	if addr, ok, err := d.NodeAddr(ctx, 4); err != nil || !ok || addr != "10.0.0.4:4" {
		t.Fatalf("NodeAddr(4) = (%q,%v,%v), want address from the node key", addr, ok, err)
	}

	// 重注册：时间戳推进（单调不减）
	time.Sleep(5 * time.Millisecond)
	if err := d.RegisterNode(ctx, 4, directory.NodeInfo{Addr: "10.0.0.4:4b"}); err != nil {
		t.Fatal(err)
	}
	rosterVal2, err := cli.HGet(ctx, d.rosterKey(), "4").Result()
	if err != nil {
		t.Fatal(err)
	}
	ts2, _ := strconv.ParseInt(rosterVal2, 10, 64)
	if ts2 < ts1 {
		t.Fatalf("roster timestamp must not go backwards on re-register: %d -> %d", ts1, ts2)
	}
	if addr, _, _ := d.NodeAddr(ctx, 4); addr != "10.0.0.4:4b" {
		t.Fatalf("address update must land in the node key, got %q", addr)
	}
}

// TestDirectoryClusterSlotUnity 键值 v3 的 slot 形态（单实例确定性断言，
// cluster 实测补全见 TestKeyTag_PerEntitySharding）：实体主键携带**逐实体
// hash tag**（tag = 完整实体身份——同实体单键操作同 slot、异实体互异分
// 片）。
func TestDirectoryClusterSlotUnity(t *testing.T) {
	mustConnect(t)
	d := newTestDir(t, time.Minute)

	idA := mustUUID(t, "slotA")
	idB, _ := rans.NewID("room", "slotB")

	if tag := hashTagOf(d.primaryKey(idA)); tag != idA.String() {
		t.Fatalf("primary key %q tag = %q, want full identity %q", d.primaryKey(idA), tag, idA.String())
	}
	if tag := hashTagOf(d.primaryKey(idB)); tag != idB.String() {
		t.Fatalf("primary key %q tag = %q, want full identity %q", d.primaryKey(idB), tag, idB.String())
	}
	// 异实体 tag 互异（分片前提）；inst 含 "/" 时 tag 仍为完整身份
	idSlash, _ := rans.NewID("room", "a/b")
	if tag := hashTagOf(d.primaryKey(idSlash)); tag != "room/a/b" {
		t.Fatalf("primary key with slashed inst tag = %q, want room/a/b", tag)
	}
	if hashTagOf(d.primaryKey(idA)) == hashTagOf(d.primaryKey(idB)) {
		t.Fatal("distinct entities must carry distinct tags (sharding premise)")
	}
	// registry 域保持独立 tag（与逐实体目录域互不挤压）
	if tag := hashTagOf(d.rosterKey()); tag != "reg" {
		t.Fatalf("roster key %q must keep the {reg} tag", d.rosterKey())
	}
}

// ---- Redis Cluster 实测套件 ----
//
// 契约套件与键族 slot 断言在真实 cluster 上的平移实测：单键 Lua 任意 slot
// 执行、pipeline 跨 slot 分发、SCAN 跨 master 聚合、真槽号数学。本地单实
// 例断言用「第一对 {} 内文本定槽」语义做确定性验证（单实例编译时可能禁用
// cluster 支持，CLUSTER KEYSLOT 不可用）。
// 门禁：RANS_TEST_REDIS_CLUSTER_ADDRS（逗号分隔种子地址）未设即 skip——
// 裸 `go test ./...` 仍全绿（与单实例门禁同纪律）。

// clusterAddrs 解析 cluster 种子地址 env。
func clusterAddrs() []string {
	v := strings.TrimSpace(os.Getenv("RANS_TEST_REDIS_CLUSTER_ADDRS"))
	if v == "" {
		return nil
	}
	return strings.Split(v, ",")
}

// mustConnectCluster cluster 探活门禁：env 未设或不可达即 skip。
func mustConnectCluster(t *testing.T) []string {
	t.Helper()
	addrs := clusterAddrs()
	if len(addrs) == 0 {
		t.Skip("RANS_TEST_REDIS_CLUSTER_ADDRS unset — cluster suite skipped")
	}
	cli := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs: addrs, Password: redisPassword(), DialTimeout: 500 * time.Millisecond,
	})
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		t.Skipf("redis cluster unavailable at %v: %v", addrs, err)
	}
	return addrs
}

// newClusterTestDir 以 cluster 种子构造被测目录（KeyPrefix 隔离同单实例
// 口径；DB 恒 0——cluster 的 Redis 侧约束）。hbTTL<=0 取实现默认。
func newClusterTestDir(t *testing.T, addrs []string, hbTTL time.Duration) *Directory {
	t.Helper()
	prefix := fmt.Sprintf("rans:test:%s:%d:", t.Name(), time.Now().UnixNano())
	sweepTestKeys(t, addrs, prefix) // 残键清扫，LIFO 在 Close 后执行
	d := New(Options{
		Addrs:        addrs,
		Password:     redisPassword(),
		HeartbeatTTL: hbTTL,
		KeyPrefix:    prefix,
	})
	t.Cleanup(func() { d.Close() })
	return d
}

// TestClusterRealSlotUnity 真 CLUSTER KEYSLOT 槽号数学（单实例本地断言的
// 实测补全）：registry 域（{reg}）总表与节点键同槽；实体主键逐实体 tag
// 落多 slot（真分片）；两域分离。
func TestClusterRealSlotUnity(t *testing.T) {
	addrs := mustConnectCluster(t)
	ctx := context.Background()
	cli := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addrs, Password: redisPassword()})
	defer cli.Close()
	d := newClusterTestDir(t, addrs, 0)

	slot := func(key string) int64 {
		n, err := cli.ClusterKeySlot(ctx, key).Result()
		if err != nil {
			t.Fatalf("CLUSTER KEYSLOT %q: %v", key, err)
		}
		return n
	}
	regRoster := slot(d.rosterKey())
	if s := slot(d.epochKey()); s != regRoster {
		t.Fatalf("{reg} domain split: roster=%d epoch=%d", regRoster, s)
	}
	if s := slot(d.nodeKey(1)); s != regRoster {
		t.Fatalf("{reg} domain split: roster=%d node=%d", regRoster, s)
	}
	// 实体主键：逐实体 tag（真分片——多实体多槽），与 registry 域分离
	idA := mustUUID(t, "slotUnityA")
	idB, _ := rans.NewID("room", "slotUnityB")
	slotA, slotB := slot(d.primaryKey(idA)), slot(d.primaryKey(idB))
	if hashTagOf(d.primaryKey(idA)) != idA.String() {
		t.Fatalf("primary key must carry full identity tag, got %q", hashTagOf(d.primaryKey(idA)))
	}
	if regRoster == slotA {
		t.Fatalf("registry and directory domains collapsed into one slot %d", regRoster)
	}
	_ = slotB // 分片散布断言的正本在 TestKeyTag_PerEntitySharding
}

func TestClusterRegisterSuite(t *testing.T) {
	addrs := mustConnectCluster(t)
	contract.RunRegisterSuite(t, func(t *testing.T) directory.Directory {
		return newClusterTestDir(t, addrs, 0)
	})
}

// TestClusterNodeMembershipSuite registry 契约平移（双写 pipeline 跨 slot
// 分发 + 判活键探活 + 惰性清理在 cluster 下的行为）。
func TestClusterNodeMembershipSuite(t *testing.T) {
	addrs := mustConnectCluster(t)
	contract.RunNodeMembershipSuite(t, func(t *testing.T, hbTTL time.Duration) directory.NodeRegistry {
		t.Helper()
		prefix := fmt.Sprintf("rans:test:%s:%d:", t.Name(), time.Now().UnixNano())
		sweepTestKeys(t, addrs, prefix) // 残键清扫
		d := New(Options{
			Addrs:        addrs,
			Password:     redisPassword(),
			HeartbeatTTL: hbTTL,
			KeyPrefix:    prefix,
		})
		t.Cleanup(func() { d.Close() })
		return d
	})
}

// TestClusterDirectoryE2E 目录全链路 cluster 冒烟：多实体散布分片（逐实体
// tag）→ Lookup 逐实体可见 → 迁移换锁（单键 luaTransfer 实跑）→ registry
// 全量快照 + 节点注销。
func TestClusterDirectoryE2E(t *testing.T) {
	addrs := mustConnectCluster(t)
	ctx := context.Background()
	d := newClusterTestDir(t, addrs, time.Minute)

	// 注册多个实体（逐实体 tag，天然散布到不同 master）
	ids := make([]rans.ID, 0, 9)
	for i := 0; i < 9; i++ {
		id := mustUUID(t, fmt.Sprintf("e2e-%02d", i))
		if _, _, err := d.Register(ctx, id, selfOf(rans.NodeID(i%3+1)), nil); err != nil {
			t.Fatalf("register %s: err=%v", id.String(), err)
		}
		ids = append(ids, id)
	}
	for _, id := range ids {
		if _, found, err := d.Lookup(ctx, id); err != nil || !found {
			t.Fatalf("lookup %s after e2e registers: found=%v err=%v", id.String(), found, err)
		}
	}

	// 迁移换锁（单键 luaTransfer：期望归属按 Lookup 现值整体给出）。
	cur, found, err := d.Lookup(ctx, ids[0])
	if err != nil || !found {
		t.Fatalf("lookup before transfer: found=%v err=%v", found, err)
	}
	if wonRes, _, _, err := d.Transfer(ctx, ids[0], cur, selfOf(3)); err != nil || !wonRes {
		t.Fatalf("transfer on cluster: won=%v err=%v", wonRes, err)
	}
	if loc, found, err := d.Lookup(ctx, ids[0]); err != nil || !found || loc.Node != 3 {
		t.Fatalf("lookup after transfer: loc=%v found=%v err=%v", loc, found, err)
	}

	// registry 全量快照 + 注销清理
	for _, n := range []rans.NodeID{1, 2, 3} {
		if err := d.RegisterNode(ctx, n, directory.NodeInfo{Addr: fmt.Sprintf("10.0.0.%d:700%d", n, n)}); err != nil {
			t.Fatal(err)
		}
	}
	m, err := d.NodeAddrs(ctx)
	if err != nil || len(m) != 3 {
		t.Fatalf("NodeAddrs on cluster = %v (%v), want 3 nodes", m, err)
	}
	if err := d.UnregisterNode(ctx, 1, 0); err != nil {
		t.Fatalf("UnregisterNode on cluster: %v", err)
	}
	if nodes, err := d.NodeIDs(ctx); err != nil || len(nodes) != 2 {
		t.Fatalf("NodeIDs after unregister = %v (%v), want 2", nodes, err)
	}
}
