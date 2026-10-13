package rpc

// Unavailable 重分类的端到端契约面（决策出处：
// docs/decisions/2026-10-09-hr2-unavailable-retry.md）：
// Unavailable 重试（确定未执行→无条件重试）、Handle 错误边界
// 折叠的远端线协议契约、重试环内路由翻转到本地路径的不重复执行锚。
// 回环双
// 节点 + 伪节点编排，时序敏感断言走计数与内容，不靠 sleep。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/routing"
	"github.com/cyinx/rans_golang/internal/transport/tcp"
	"github.com/cyinx/rans_golang/internal/wire"
)

// unavailPeer 伪节点 3：真 tcp.Manager + 恒回 StatusUnavailable 的应用层
// handler（端到端编排：执行前回绝=确定未执行）。
type unavailPeer struct {
	m       *tcp.Manager
	resolve func(ctx context.Context, n rans.NodeID) (string, error)
	pool    *wire.PagePool
	addr    string
	reqs    atomic.Int32
}

// HandleMessage 收到 Request 即回 Unavailable 结构化 Error 帧（执行前回绝
// 语义；ReasonNone——分类唯一表对无细分 reason 的 Unavailable 归
// RetryUnconditional，重路由成功路径的编排锚）。
func (p *unavailPeer) HandleMessage(hdr wire.Header, body []byte) {
	if body != nil {
		p.pool.Put(body)
	}
	if hdr.Type != protocol.MsgRequest {
		return
	}
	p.reqs.Add(1)
	rh := wire.Header{
		Type:         protocol.MsgError,
		Flags:        protocol.FlagHasStatus | protocol.FlagHasSourceNodeID,
		MessageID:    hdr.MessageID,
		Status:       rans.CodeUnavailable,
		SourceNodeID: 3,
	}
	_ = p.m.SendToNode(context.Background(), rans.NodeID(hdr.SourceNodeID), p.resolve, rh,
		wire.MarshalError(rans.NewError(rans.CodeUnavailable, rans.ReasonNone, "inbox full")))
}

func (p *unavailPeer) OnHandshakeComplete(rans.NodeID) {}
func (p *unavailPeer) OnConnectionLost(rans.NodeID)    {}

// startUnavailPeer 拉起伪节点 listener（Secret/指纹与 startNotFoundPeer
// 缺省一致，握手可成）。
func startUnavailPeer(t *testing.T, rt *resolveTable) *unavailPeer {
	t.Helper()
	pool := wire.NewPagePool()
	peer := &unavailPeer{resolve: rt.resolve, pool: pool}
	peer.m = tcp.NewManager(tcp.Config{
		NodeID:       3,
		ClusterName:  "loop-cluster",
		Secret:       []byte("loop-secret"),
		SchemaDigest: 7,
	}, peer, pool)
	addr, err := peer.m.Listen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unavailable peer listen: %v", err)
	}
	t.Cleanup(peer.m.Close)
	peer.addr = addr
	return peer
}

// TestCallUnavailableRetriedUnconditionally（Unavailable 无条件重试的端到端）：
// 远端回 Error(StatusUnavailable)＝确定未执行，非幂等调用也必须重试——
// 首查指向恒回 Unavailable 的伪节点 3 → 环重试并重走路由梯子 → 目录重查
// 命中 node1 → 成功。伪节点恰收 1 个请求（回绝不放大流量），实体恰好一次
// 执行。
func TestCallUnavailableRetriedUnconditionally(t *testing.T) {
	rt := &resolveTable{}
	peer := startUnavailPeer(t, rt)
	cache := routing.NewLocationCache(0, 0, 0)
	node1 := startRemNode(t, inmem.New(), rt, nodeSpec{
		id:     1,
		handle: func(_ context.Context, _ any) (any, error) { return "at-new-owner", nil },
	})
	node2 := startRemNode(t, &flippingDir{Directory: inmem.New()}, rt, nodeSpec{
		id:     2,
		cache:  cache,
		handle: fastHandle,
	})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 3: peer.addr})
	id := mustUUID(t, "unavail-1")

	// DefaultOptions＝非幂等：Unavailable 属确定未执行——无条件重试生效，
	// 重路由成功；若误归模糊类则非幂等调用直接上抛（分类反转即本测试变红）。
	v, err := node2.rem.CallOpts(context.Background(), id, 0, routing.DefaultOptions())
	if err != nil {
		t.Fatalf("unavailable must be retried unconditionally (determined-not-executed): %v", err)
	}
	if v != "at-new-owner" {
		t.Fatalf("retried call returned %v, want at-new-owner", v)
	}
	if n := peer.reqs.Load(); n != 1 {
		t.Fatalf("rejecting owner must see exactly 1 request, saw %d", n)
	}
	if n := node1.led.count(); n != 1 {
		t.Fatalf("entity must activate exactly once on new owner, activations=%d", n)
	}
}

// TestCallFlipToLocalHandlerErrorNoDuplicateExecution（翻转本地不重复执行
// 的端到端锚）：重试环内路由翻转到本地后，实体 Handle 返回
// 的错误（嵌套 NotFound 传输形态）不得原样直达 Classify 诱发重复执行——
// 折叠面（callDelivery.Deliver）先行，Classify 必判 NoRetry：本地恰好执行
// 一次，调用方仍可读错误文本、摸不到嵌套的传输形态。
func TestCallFlipToLocalHandlerErrorNoDuplicateExecution(t *testing.T) {
	rt := &resolveTable{}
	peer := startNotFoundPeer(t, rt) // 首查目标：恒回 NotFound 的伪节点 3
	var execs atomic.Int32
	node1 := startRemNode(t, &flippingDir{Directory: inmem.New()}, rt, nodeSpec{
		id: 1, // flippingDir 重查返回 node1＝调用方自己 → 环内翻转到本地
		handle: func(_ context.Context, _ any) (any, error) {
			execs.Add(1)
			return nil, fmt.Errorf("nested lookup failed: %w",
				rans.NewError(rans.CodeNotFound, rans.ReasonStaleLocation, "nested stale"))
		},
	})
	rt.set(map[rans.NodeID]string{3: peer.addr})
	id := mustUUID(t, "flip-fold-1")

	_, err := node1.rem.Call(context.Background(), id, 0)
	if err == nil {
		t.Fatal("handler error must surface to the caller")
	}
	if n := execs.Load(); n != 1 {
		t.Fatalf("local handler must execute exactly once (no duplicate execution from retry misclassification), executions=%d", n)
	}
	if !strings.Contains(err.Error(), "nested lookup failed") {
		t.Fatalf("caller must still read the folded error text, got %q", err.Error())
	}
	// 统一错误模型：折叠形态必为 CodeError+ReasonHandler——嵌套的运行时形态不可达。
	var re *rans.Error
	if !errors.As(err, &re) || re.Code != rans.CodeError || re.Reason != rans.ReasonHandler {
		t.Fatalf("folded error = %v, want *rans.Error{CodeError, ReasonHandler}", err)
	}
}

// TestRemoteHandlerErrorStaysStatusErrorOnWire（Handle 错误边界的远端线协议
// 半边，pin）：实体 Handle 返回的错误（哪怕嵌着 ErrBusy）在
// 远端投递边界折叠为 StatusError 文本，线上错误码不可能来自
// 实体业务错误；调用方 errors.Is 摸不到 ErrUnavailable，重复执行为 0。
func TestRemoteHandlerErrorStaysStatusErrorOnWire(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	var execs atomic.Int32
	node1 := startRemNode(t, dir, rt, nodeSpec{
		id: 1,
		handle: func(_ context.Context, _ any) (any, error) {
			execs.Add(1)
			return nil, fmt.Errorf("nested call failed: %w", activation.ErrBusy)
		},
	})
	node2 := startRemNode(t, dir, rt, nodeSpec{id: 2, handle: fastHandle})
	rt.set(map[rans.NodeID]string{1: node1.rem.Addr(), 2: node2.rem.Addr()})
	ctx := context.Background()
	id := mustUUID(t, "wire-fold-1")
	if _, _, err := dir.Register(ctx, id, directory.Owner{Node: 1}, nil); err != nil {
		t.Fatal(err)
	}

	_, err := node2.rem.Call(ctx, id, 0)
	var re *rans.Error
	if !errors.As(err, &re) {
		t.Fatalf("caller must see *rans.Error, got %v", err)
	}
	if re.Code != rans.CodeError {
		t.Fatalf("wire code = %d, want CodeError (entity business errors must not derive wire status)", re.Code)
	}
	if errors.Is(err, rans.ErrUnavailable) {
		t.Fatal("caller must not unwrap the entity's nested busy chain through the wire")
	}
	if n := execs.Load(); n != 1 {
		t.Fatalf("exactly-once violated, executions=%d", n)
	}
}
