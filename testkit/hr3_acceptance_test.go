package testkit

// 验收测试落点（约定 spec §8）：统一调用入口——本地专用调用入口不存在，
// 一切调用经 Client 路由：本地命中走本地快路径，远端经目录+传输，错误
// 形态一致。统一错误模型——本地与远端超时同构
//（errors.Is(err, rans.ErrTimeout)）、Handle 错误折叠为 CodeError+
// ReasonHandler 的 *rans.Error（调用方不可 Unwrap 到嵌套运行时错误、
// 不诱发重试）。本文件手工装配双节点（不经 testkit.Cluster，保持对 node
// 公开面的最小依赖）。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/testhooks"
	"github.com/cyinx/rans_golang/node"
)

// ex11Counter 统一调用入口验收实体：int 请求只激活不计数（归属铺底）；string "inc"
// 递增计数并返回——node1 侧计数=1 即「node2 的调用确实在 node1 执行」；
// string "client" 返回 Handle ctx 是否携带调用面（rans.ClientFrom 写入侧
// 接线的过线锚——入站路径由 dispatcher 投递基 ctx 注入）。
type ex11Counter struct {
	calls     int
	hadClient bool
}

func (c *ex11Counter) Activate(context.Context) error   { return nil }
func (c *ex11Counter) Deactivate(context.Context) error { return nil }
func (c *ex11Counter) Handle(ctx context.Context, req any) (any, error) {
	switch req.(type) {
	case int:
		return c.calls, nil
	case string:
		c.hadClient = rans.ClientFrom(ctx) != nil
		if req.(string) == "inc" {
			c.calls++
			return c.calls, nil
		}
		return c.hadClient, nil
	}
	return nil, nil
}

// ex11AddrTable 可更新地址表（cluster.go resolveTable 同款手法：Resolve
// 钩子在 New 期定死，Start 后一次性回填实际地址）。
type ex11AddrTable struct {
	m atomic.Pointer[map[rans.NodeID]string]
}

func (t *ex11AddrTable) resolve(ctx context.Context, n rans.NodeID) (string, error) {
	if m := t.m.Load(); m != nil {
		if a, ok := (*m)[n]; ok {
			return a, nil
		}
	}
	return "", context.DeadlineExceeded
}

func (t *ex11AddrTable) set(m map[rans.NodeID]string) { t.m.Store(&m) }

// TestHR3_UnifiedClientRoutes 统一调用入口跨节点路由（主锚）：
// 双节点共享 inmem 目录 + TestHooks.Resolve 地址表；X 经 node1 激活（归属
// node1）；从 node2 `n2.Client().Call(ctx, X, "inc")` 成功且 node1 侧实体
// 计数=1（确实远程执行在 node1）；node2 本地未激活该 id（统一入口没有
// 「本地专用」旁路——请求必须路由到 owner）。
func TestHR3_UnifiedClientRoutes(t *testing.T) {
	dir := inmem.New()
	tbl := &ex11AddrTable{}
	kinds := map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &ex11Counter{} },
	}
	mk := func(id rans.NodeID) *node.Node {
		t.Helper()
		n, err := node.New(node.Config{
			NodeID:      id,
			ClusterName: "ex11",
			Kinds:       kinds,
			Directory:   node.DirectoryConfig{Injected: dir},
			Security:    node.SecurityConfig{HMACKey: []byte("ex11-secret")},
			TestHooks:   &testhooks.Hooks{Resolve: tbl.resolve},
		})
		if err != nil {
			t.Fatalf("node.New(%d): %v", id, err)
		}
		if err := n.Start(context.Background()); err != nil {
			t.Fatalf("node %d Start: %v", id, err)
		}
		return n
	}
	n1, n2 := mk(1), mk(2)
	tbl.set(map[rans.NodeID]string{1: n1.Addr(), 2: n2.Addr()})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = n2.Shutdown(ctx)
		_ = n1.Shutdown(ctx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	x, err := rans.NewID("counter", "ex11-x")
	if err != nil {
		t.Fatal(err)
	}

	// X 经 node1 激活（int 请求不计计数——归属铺底）。
	if _, err := n1.Client().Call(ctx, x, 0); err != nil {
		t.Fatalf("node1 activation call: %v", err)
	}

	// 从 node2 经统一入口调用：必须远程路由到 node1 执行（计数=1）。
	v, err := n2.Client().Call(ctx, x, "inc")
	if err != nil {
		t.Fatalf("node2 unified client call: %v", err)
	}
	if got, ok := v.(int); !ok || got != 1 {
		t.Fatalf("node2 call result = %v (%T), want 1 (executed on node1)", v, v)
	}

	// node1 侧实体确实执行（本地读回计数）。
	if v, err := n1.Client().Call(ctx, x, 0); err != nil || v.(int) != 1 {
		t.Fatalf("node1 side count = %v (err=%v), want 1", v, err)
	}

	// node2 本地未激活该 id（请求路由到 owner，没有本地旁路激活）。
	for _, id := range n2.LocalIDs() {
		if id == x {
			t.Fatalf("node2 must not activate %s locally (unified entry routes to owner)", x.String())
		}
	}

	// ClientFrom 写入侧过线锚：远程入站的 Handle ctx 携带调用面（dispatcher
	// 投递基 ctx 注入——node1 侧实体自省可用）。
	cv, err := n2.Client().Call(ctx, x, "client")
	if err != nil {
		t.Fatalf("client-probe call: %v", err)
	}
	if had, ok := cv.(bool); !ok || !had {
		t.Fatalf("entity Handle ctx must carry rans.ClientFrom on the remote inbound path, got %v", cv)
	}
}

// ---- 统一错误模型 ----

// ex12SlowEntity 慢实体：req=="fast" 立即返回（激活铺底）；其余睡 1s 后
// 返回——对 100ms 调用方 ctx 预算 / 150ms 调用缺省预算（+至多一个 250ms
// 扫描周期）构成必超时窗且两侧边界可分辨。
type ex12SlowEntity struct{}

func (e *ex12SlowEntity) Activate(context.Context) error   { return nil }
func (e *ex12SlowEntity) Deactivate(context.Context) error { return nil }
func (e *ex12SlowEntity) Handle(ctx context.Context, req any) (any, error) {
	if s, ok := req.(string); ok && s == "fast" {
		return "fast", nil
	}
	select {
	case <-time.After(time.Second):
		return "slow", nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ex12FoldingEntity 折叠锚实体：Handle 返回「包装了嵌套调用运行时错误」的
// 业务错误（嵌套调用撞上 Busy 回绝的最危险形态）。计数器锚定执行次数。
type ex12FoldingEntity struct {
	execs atomic.Int32
}

func (e *ex12FoldingEntity) Activate(context.Context) error   { return nil }
func (e *ex12FoldingEntity) Deactivate(context.Context) error { return nil }
func (e *ex12FoldingEntity) Handle(_ context.Context, _ any) (any, error) {
	e.execs.Add(1)
	nested := rans.NewError(rans.CodeUnavailable, rans.ReasonBusy, "nested call hit busy")
	return nil, fmt.Errorf("nested call failed: %w", nested)
}

// ex12AppEntity AppError/AppDataError 实体：返回实现两类接口的业务错误，
// 锚定折叠边界携带 AppCode/AppData（约定 spec §5.3 Handle 错误边界）。
type ex12AppEntity struct{}

func (e *ex12AppEntity) Activate(context.Context) error   { return nil }
func (e *ex12AppEntity) Deactivate(context.Context) error { return nil }
func (e *ex12AppEntity) Handle(_ context.Context, _ any) (any, error) {
	return nil, ex12AppErr{}
}

type ex12AppErr struct{}

func (e ex12AppErr) Error() string  { return "order rejected: inventory conflict" }
func (e ex12AppErr) AppCode() int64 { return 4242 }
func (e ex12AppErr) AppData() []byte {
	return []byte(`{"http":503}`)
}

var _ rans.AppDataError = ex12AppErr{}

// TestHR3_UnifiedErrorModelLocalRemoteUniform 统一错误模型：
//   - 本地超时（调用方 ctx 预算 100ms、Handle 睡 1s）与远端超时（网关
//     固定超时 150ms）都必须 errors.Is(err, rans.ErrTimeout)——本地与远端
//     同构，无裸哨兵或内部错误形态的分叉；
//   - 实体 Handle 返回包装了嵌套调用运行时错误的业务错误 → 调用方
//     errors.Is(err, rans.ErrUnavailable)==false（折叠边界：不可 Unwrap 到
//     嵌套运行时形态）且不重试（重复执行计数 0，恰好执行一次）；本地腿
//     折叠形态为 CodeError+ReasonHandler；
//   - 实体 AppError/AppDataError 经本地折叠边界携带 AppCode/AppData。
func TestHR3_UnifiedErrorModelLocalRemoteUniform(t *testing.T) {
	dir := inmem.New()
	tbl := &ex11AddrTable{}
	remoteFolder := &ex12FoldingEntity{}
	localFolder := &ex12FoldingEntity{}
	kinds := map[string]rans.Factory{
		"slow": func(_ rans.ID) rans.Entity { return &ex12SlowEntity{} },
		"folder": func(id rans.ID) rans.Entity {
			if id.Key == "ex12-fold" {
				return remoteFolder
			}
			return localFolder
		},
		"appfold": func(_ rans.ID) rans.Entity { return &ex12AppEntity{} },
	}
	mk := func(id rans.NodeID, timeout time.Duration) *node.Node {
		t.Helper()
		n, err := node.New(node.Config{
			NodeID:      id,
			ClusterName: "ex12",
			Kinds:       kinds,
			Directory:   node.DirectoryConfig{Injected: dir},
			Security:    node.SecurityConfig{HMACKey: []byte("ex12-secret")},
			Calls:       node.CallConfig{Default: timeout},
			TestHooks:   &testhooks.Hooks{Resolve: tbl.resolve},
		})
		if err != nil {
			t.Fatalf("node.New(%d): %v", id, err)
		}
		if err := n.Start(context.Background()); err != nil {
			t.Fatalf("node %d Start: %v", id, err)
		}
		return n
	}
	n1 := mk(1, 0) // 实体归属节点：调用缺省预算回落缺省 30s
	n2 := mk(2, 150*time.Millisecond)
	tbl.set(map[rans.NodeID]string{1: n1.Addr(), 2: n2.Addr()})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = n2.Shutdown(ctx)
		_ = n1.Shutdown(ctx)
	}()

	slowID, err := rans.NewID("slow", "ex12-slow")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 铺底：slow 实体经 node1 激活（"fast" 立即返回），目录归属 node1。
	if v, err := n1.Client().Call(ctx, slowID, "fast"); err != nil || v != "fast" {
		t.Fatalf("warm-up call: v=%v err=%v", v, err)
	}

	t.Run("local timeout is rans.ErrTimeout", func(t *testing.T) {
		ctx100, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := n1.Client().Call(ctx100, slowID, "slow")
		if !errors.Is(err, rans.ErrTimeout) {
			t.Fatalf("local timeout err = %v, want errors.Is(err, rans.ErrTimeout)", err)
		}
		if elapsed := time.Since(start); elapsed >= time.Second {
			t.Fatalf("local timeout must fire at the caller budget (~100ms), not the entity wake, took %v", elapsed)
		}
	})

	t.Run("remote gateway timeout is rans.ErrTimeout", func(t *testing.T) {
		// 无 deadline 无选项：预算取调用节点的 Calls.Default（n2=150ms）——
		// 逐请求 deadline 按合成结果登记（+至多一个 250ms 扫描周期），不等
		// 慢实体 1s 睡完。
		start := time.Now()
		_, err := n2.Client().Call(context.Background(), slowID, "slow")
		if !errors.Is(err, rans.ErrTimeout) {
			t.Fatalf("remote gateway timeout err = %v, want errors.Is(err, rans.ErrTimeout)", err)
		}
		if elapsed := time.Since(start); elapsed >= time.Second {
			t.Fatalf("gateway timeout must fire at ~150ms (+sweep), not the entity wake, took %v", elapsed)
		}
	})

	t.Run("handler error folded no retry no unavailable", func(t *testing.T) {
		foldID, err := rans.NewID("folder", "ex12-fold")
		if err != nil {
			t.Fatal(err)
		}
		// 远端腿：node2 调 node1 上的折叠实体——折叠后的错误对重试环必判
		// NoRetry（重复执行计数 0），且摸不到嵌套的 Unavailable 形态。
		_, err = n2.Client().Call(ctx, foldID, nil)
		if err == nil {
			t.Fatal("handler error must surface to the remote caller")
		}
		if errors.Is(err, rans.ErrUnavailable) {
			t.Fatalf("folded handler error must not match rans.ErrUnavailable, got %v", err)
		}
		if n := remoteFolder.execs.Load(); n != 1 {
			t.Fatalf("remote: entity must execute exactly once (duplicates=%d), executions=%d", n-1, n)
		}

		// 本地腿：node1 直调折叠实体（新 id，本地激活）——折叠形态为
		// CodeError+ReasonHandler 的 *rans.Error。
		localID, err := rans.NewID("folder", "ex12-fold-local")
		if err != nil {
			t.Fatal(err)
		}
		_, err = n1.Client().Call(ctx, localID, nil)
		if err == nil {
			t.Fatal("handler error must surface to the local caller")
		}
		if errors.Is(err, rans.ErrUnavailable) {
			t.Fatalf("folded handler error must not match rans.ErrUnavailable, got %v", err)
		}
		if got := rans.CodeOf(err); got != rans.CodeError {
			t.Fatalf("local fold CodeOf = %d, want CodeError", got)
		}
		if got := rans.ReasonOf(err); got != rans.ReasonHandler {
			t.Fatalf("local fold ReasonOf = %d, want ReasonHandler", got)
		}
		if n := localFolder.execs.Load(); n != 1 {
			t.Fatalf("local: entity must execute exactly once (duplicates=%d), executions=%d", n-1, n)
		}
	})

	t.Run("app error carries app code through local fold", func(t *testing.T) {
		appID, err := rans.NewID("appfold", "ex12-app")
		if err != nil {
			t.Fatal(err)
		}
		_, err = n1.Client().Call(ctx, appID, nil)
		if err == nil {
			t.Fatal("app error must surface to the caller")
		}
		if got := rans.CodeOf(err); got != rans.CodeError {
			t.Fatalf("app fold CodeOf = %d, want CodeError", got)
		}
		var e *rans.Error
		if !errors.As(err, &e) {
			t.Fatalf("app fold must be *rans.Error, got %T", err)
		}
		if e.AppCode != 4242 {
			t.Fatalf("folded AppCode = %d, want 4242 (entity AppError carried through the fold)", e.AppCode)
		}
		if string(e.AppData) != `{"http":503}` {
			t.Fatalf("folded AppData = %q, want entity AppDataError payload", e.AppData)
		}
	})
}

// ---- Error 帧结构化 + 未知 kind 归一 ----

// hr13FixedPlacement 静态放置：无视候选恒选 target（把 node2 的目录 miss
// 确定性导向 node1——未知 kind 的远端腿编排：kind 两端都未注册、目录无
// 条目，唯一到达远端的路径就是放置策略）。
type hr13FixedPlacement struct{ target rans.NodeID }

func (p hr13FixedPlacement) Pick(string, rans.NodeID, []rans.NodeID) (rans.NodeID, bool) {
	return p.target, true
}

// TestHR3_UnknownKindLocalRemoteUniform 未知 kind 本地/远端同构：目标
// kind 不在任何节点注册表——
//   - 本地腿（node1，目录 miss + 无放置 → 本地激活）：GetOrActivate 直接
//     ErrUnknownKind（NotFound+UnknownKind 归一形态）；
//   - 远端腿（node2，静态放置 → node1 的 dispatcher）：必须
//     CodeNotFound+ReasonUnknownKind（促源端重解析重试）且 Node 携带
//     产生错误的节点号——不得以非结构化文本回退。
func TestHR3_UnknownKindLocalRemoteUniform(t *testing.T) {
	dir := inmem.New()
	tbl := &ex11AddrTable{}
	kinds := map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &ex11Counter{} },
	}
	mk := func(id rans.NodeID, placement node.Placement) *node.Node {
		t.Helper()
		n, err := node.New(node.Config{
			NodeID:      id,
			ClusterName: "hr13",
			Kinds:       kinds,
			Placement:   placement,
			Directory:   node.DirectoryConfig{Injected: dir},
			Security:    node.SecurityConfig{HMACKey: []byte("hr13-secret")},
			Calls:       node.CallConfig{Default: 2 * time.Second},
			TestHooks:   &testhooks.Hooks{Resolve: tbl.resolve},
		})
		if err != nil {
			t.Fatalf("node.New(%d): %v", id, err)
		}
		if err := n.Start(context.Background()); err != nil {
			t.Fatalf("node %d Start: %v", id, err)
		}
		return n
	}
	n1 := mk(1, nil)                           // 无放置：目录 miss 回落本地激活
	n2 := mk(2, hr13FixedPlacement{target: 1}) // 目录 miss 恒导向 node1
	tbl.set(map[rans.NodeID]string{1: n1.Addr(), 2: n2.Addr()})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = n2.Shutdown(ctx)
		_ = n1.Shutdown(ctx)
	}()

	ghost, err := rans.NewID("hr13-no-such-kind", "ghost")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 本地腿：kind 不在 node1 注册表 → 激活路径归一（NotFound+UnknownKind）。
	_, err = n1.Client().Call(ctx, ghost, nil)
	if rans.CodeOf(err) != rans.CodeNotFound || rans.ReasonOf(err) != rans.ReasonUnknownKind {
		t.Fatalf("local unknown kind: code=%d reason=%d, want NotFound+UnknownKind (err=%v)",
			rans.CodeOf(err), rans.ReasonOf(err), err)
	}

	// 远端腿：node2 经放置把请求送到 node1，node1 dispatcher 对未知 kind
	// 回结构化 Error 帧——源端拿到的 (Code, Reason) 与本地腿同构。
	_, err = n2.Client().Call(ctx, ghost, nil)
	if rans.CodeOf(err) != rans.CodeNotFound || rans.ReasonOf(err) != rans.ReasonUnknownKind {
		t.Fatalf("remote unknown kind: code=%d reason=%d, want NotFound+UnknownKind (err=%v)",
			rans.CodeOf(err), rans.ReasonOf(err), err)
	}
	var re *rans.Error
	if !errors.As(err, &re) {
		t.Fatalf("remote unknown kind must surface as *rans.Error, got %T", err)
	}
	if re.Node != 1 {
		t.Fatalf("remote error Node = %d, want 1 (producing node rides the frame header)", re.Node)
	}
	if !strings.Contains(re.Message, "hr13-no-such-kind") {
		t.Fatalf("remote error Message = %q, want the offending kind name", re.Message)
	}
}

// TestHR3_AppErrorAppDataRoundTripRemote AppError/AppDataError 过线往返：
// 实体返回的业务错误经远端调用原样到达源端——折叠面
// （CodeError+ReasonHandler）与业务维（AppCode/AppData）都走结构化 Error
// 帧字段（不落纯文本 body）。entityHandle 每次调用都返回 ex12AppErr（与
// 统一错误模型验收同一实体）——node1 上的首调即激活铺底（注册进共享目录）。
func TestHR3_AppErrorAppDataRoundTripRemote(t *testing.T) {
	dir := inmem.New()
	tbl := &ex11AddrTable{}
	kinds := map[string]rans.Factory{
		"appfold": func(_ rans.ID) rans.Entity { return &ex12AppEntity{} },
	}
	mk := func(id rans.NodeID) *node.Node {
		t.Helper()
		n, err := node.New(node.Config{
			NodeID:      id,
			ClusterName: "hr13app",
			Kinds:       kinds,
			Directory:   node.DirectoryConfig{Injected: dir},
			Security:    node.SecurityConfig{HMACKey: []byte("hr13app-secret")},
			Calls:       node.CallConfig{Default: 2 * time.Second},
			TestHooks:   &testhooks.Hooks{Resolve: tbl.resolve},
		})
		if err != nil {
			t.Fatalf("node.New(%d): %v", id, err)
		}
		if err := n.Start(context.Background()); err != nil {
			t.Fatalf("node %d Start: %v", id, err)
		}
		return n
	}
	n1, n2 := mk(1), mk(2)
	tbl.set(map[rans.NodeID]string{1: n1.Addr(), 2: n2.Addr()})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = n2.Shutdown(ctx)
		_ = n1.Shutdown(ctx)
	}()

	appID, err := rans.NewID("appfold", "hr13-app")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 铺底＝本地腿：node1 首调激活并注册目录，Handle 错误经本地折叠面。
	if _, err := n1.Client().Call(ctx, appID, nil); rans.ReasonOf(err) != rans.ReasonHandler {
		t.Fatalf("local app fold reason=%d (err=%v), want ReasonHandler", rans.ReasonOf(err), err)
	}

	// 远端腿：node2 按目录路由到 node1——AppCode/AppData/Reason 必须原样
	// 过线（结构化 Error 帧的 field 2/4/5）。
	_, err = n2.Client().Call(ctx, appID, nil)
	if err == nil {
		t.Fatal("app error must surface to the remote caller")
	}
	if rans.CodeOf(err) != rans.CodeError || rans.ReasonOf(err) != rans.ReasonHandler {
		t.Fatalf("remote app fold: code=%d reason=%d, want CodeError+ReasonHandler (err=%v)",
			rans.CodeOf(err), rans.ReasonOf(err), err)
	}
	var re *rans.Error
	if !errors.As(err, &re) {
		t.Fatalf("remote app error must be *rans.Error, got %T", err)
	}
	if re.AppCode != 4242 {
		t.Fatalf("remote AppCode = %d, want 4242 (must survive the wire round trip)", re.AppCode)
	}
	if string(re.AppData) != `{"http":503}` {
		t.Fatalf("remote AppData = %q, want entity payload surviving the wire round trip", re.AppData)
	}
	if re.Node != 1 {
		t.Fatalf("remote error Node = %d, want 1", re.Node)
	}
}
