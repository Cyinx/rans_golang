package rpc

// 排空门闸（规格 §14.1 ①/§10.3 同口径）+ Gateway.PendingCount +
// Remote.Shutdown 装配的行为锁。门闸用例跑在 dispEnv 替身基座上（dispatcher_test.go）；
// Shutdown 装配用例跑在回环双节点基座上（remote_test.go）；时序断言一律走
// channel 握手。

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

// ---- dispatcher 门闸 ----

// 1. Request 在排空中：回 Error(StatusUnavailable)（促重路由——Unavailable
// 属模糊类，幂等重试自然换台）给源节点；实体零激活；池页恰好归还一次；
// 门闸原子态本身可逆（包内直读 draining 复位验证——生产路径 BeginDraining
// 一次性置位，排空是终态动作）。
func TestDispatcherDrainGateRequestUnavailable(t *testing.T) {
	got := make(chan any, 4)
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		got <- req
		return req.(int) + 1, nil
	}, false)
	base := env.pool.Stats()
	env.disp.BeginDraining()

	const reqID = 601
	env.feed(reqHdrFor(protocol.MsgRequest, reqID, "dr", nil), envBody(t, 1))
	fr := mustSent(t, env.fake)
	if fr.hdr.Type != protocol.MsgError || fr.hdr.Status != rans.CodeUnavailable {
		t.Fatalf("want Error(StatusUnavailable) while draining, got %+v", fr.hdr)
	}
	if fr.node != 1 || fr.hdr.MessageID != reqID {
		t.Fatalf("drain-gate error frame must reply to source (trap 4): hdr=%+v node=%d", fr.hdr, fr.node)
	}
	if reason := string(fr.body); !strings.Contains(reason, "drain") {
		t.Fatalf("drain-gate reason=%q must mention draining", reason)
	}
	if n := env.led.count(); n != 0 {
		t.Fatalf("draining must reject before activation, %d entities created", n)
	}
	s := env.pool.Stats()
	if s.Gets != base.Gets+1 || s.Puts != base.Puts+1 {
		t.Fatalf("gated request page must be returned exactly once: gets %d→%d puts %d→%d",
			base.Gets, s.Gets, base.Puts, s.Puts)
	}
	env.pool.Put(fr.body)

	// 门闸原子态可逆（包内验证）：复位后同一分发器恢复投递
	env.disp.draining.Store(false)
	env.feed(reqHdrFor(protocol.MsgRequest, 602, "dr", nil), envBody(t, 10))
	select {
	case v := <-got:
		if v != 10 {
			t.Fatalf("entity received %v after un-drain, want 10", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("un-drained dispatcher must deliver requests again")
	}
	fr2 := mustSent(t, env.fake)
	if fr2.hdr.Type != protocol.MsgResponse || fr2.hdr.Status != rans.CodeOK {
		t.Fatalf("un-drained request must get a normal response, got %+v", fr2.hdr)
	}
	env.pool.Put(fr2.body)
}

// 2. OneWay 在排水中：丢弃留痕——零回帧、零激活、页归还恰一次。
func TestDispatcherDrainGateOneWayDropped(t *testing.T) {
	var calls int32
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		calls++
		return nil, nil
	}, false)
	base := env.pool.Stats()
	env.disp.BeginDraining()

	env.feed(reqHdrFor(protocol.MsgOneWay, 603, "dr2", nil), envBody(t, 2))
	if n := sentCount(env.fake); n != 0 {
		t.Fatalf("draining one-way must produce no frame, got %d", n)
	}
	if n := env.led.count(); n != 0 {
		t.Fatalf("draining one-way must not activate, %d entities created", n)
	}
	s := env.pool.Stats()
	if s.Gets != base.Gets+1 || s.Puts != base.Puts+1 {
		t.Fatalf("dropped one-way page must be returned exactly once: gets %d→%d puts %d→%d",
			base.Gets, s.Gets, base.Puts, s.Puts)
	}
}

// 3. Response 在排空中照常过闸：pending 关联不受门闸影响（排空要 flush 的
// 正是这些应答）。
func TestDispatcherDrainGateResponsePassesThrough(t *testing.T) {
	env := newDispEnv(t, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}, false)
	env.disp.BeginDraining()
	id := env.gw.NextMessageID()
	type result struct {
		hdr  wire.Header
		body []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		h, b, err := env.gw.SendRequest(context.Background(), 7,
			wire.Header{Type: protocol.MsgRequest, Flags: protocol.FlagHasSourceNodeID,
				MessageID: id, SourceNodeID: 2}, nil)
		done <- result{hdr: h, body: b, err: err}
	}()
	if fr := mustSent(t, env.fake); fr.node != 7 {
		t.Fatalf("outgoing request must target node 7, got %d", fr.node)
	}
	page := env.pool.Get(4)
	copy(page, "pong")
	env.disp.HandleMessage(wire.Header{
		Type:         protocol.MsgResponse,
		Flags:        protocol.FlagHasStatus | protocol.FlagHasSourceNodeID,
		MessageID:    id,
		Status:       rans.CodeOK,
		SourceNodeID: 7,
	}, page)
	select {
	case r := <-done:
		if r.err != nil || string(r.body) != "pong" {
			t.Fatalf("response must pass the drain gate: err=%v body=%q", r.err, r.body)
		}
		env.pool.Put(r.body)
	case <-time.After(5 * time.Second):
		t.Fatal("drain gate must not intercept responses")
	}
}

// 4. MigrationPackage 在排空中照常落缓存：迁移在途的包不因本端排空而丢失
// （目标端激活闭包消费，TTL 5min 自行过期）。
func TestDispatcherDrainGateMigrationPackageCached(t *testing.T) {
	dir := inmem.New()
	mgr := activation.New(dir, 2, map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity {
			return &testEntity{handle: func(context.Context, any) (any, error) { return nil, nil }}
		},
	})
	pool := wire.NewPagePool()
	fake := newFakeConnManager()
	gw := NewGateway(fake, func(_ context.Context, _ rans.NodeID) (string, error) {
		return "", errors.New("no address in test")
	}, pool, time.Second, nil)
	pkgs := NewPackageCache(0)
	disp := NewDispatcher(mgr, gw, 2, pool, pkgs, 0, newDispOutbox(t, gw), nil)
	disp.BeginDraining()

	id, err := rans.NewID("cell", "mp")
	if err != nil {
		t.Fatal(err)
	}
	page := pool.Get(4)
	copy(page, "snap")
	disp.HandleMessage(wire.Header{
		Type:       protocol.MsgMigrationPackage,
		Flags:      protocol.FlagHasTargetUUID,
		MessageID:  604,
		TargetKind: "cell",
		TargetInst: "mp",
	}, page)
	snap, ok := pkgs.peek(id)
	if !ok || string(snap) != "snap" {
		t.Fatalf("migration package must land in cache while draining, got %q ok=%v", snap, ok)
	}
	if n := sentCount(fake); n != 0 {
		t.Fatalf("cached migration package must produce no frame, got %d", n)
	}
}

// ---- Gateway.PendingCount ----

// 5. PendingCount 跟随 pending 表：请求发出（替身收到帧 = 已登记）→ 1；
// 应答到达、SendRequest 收场 → 0。（排空 flush 的等待面。）
func TestGatewayPendingCount(t *testing.T) {
	fake := newFakeConnManager()
	pool := wire.NewPagePool()
	gw := NewGateway(fake, func(_ context.Context, _ rans.NodeID) (string, error) {
		return "", errors.New("no address in test")
	}, pool, time.Second, nil)
	if n := gw.PendingCount(); n != 0 {
		t.Fatalf("fresh gateway pending count %d, want 0", n)
	}
	id := gw.NextMessageID()
	done := make(chan error, 1)
	go func() {
		_, _, err := gw.SendRequest(context.Background(), 7,
			wire.Header{Type: protocol.MsgRequest, Flags: protocol.FlagHasSourceNodeID,
				MessageID: id, SourceNodeID: 2}, nil)
		done <- err
	}()
	if fr := mustSent(t, fake); fr.node != 7 {
		t.Fatalf("request must target node 7, got %d", fr.node)
	}
	if n := gw.PendingCount(); n != 1 {
		t.Fatalf("in-flight request pending count %d, want 1", n)
	}
	page := pool.Get(4)
	copy(page, "pong")
	gw.HandleResponse(wire.Header{
		Type:      protocol.MsgResponse,
		Flags:     protocol.FlagHasStatus | protocol.FlagHasSourceNodeID,
		MessageID: id,
		Status:    rans.CodeOK,
	}, page)
	// body 页所有权经 pending 移交 SendRequest 调用方——收场信号：done 返回
	if err := <-done; err != nil {
		t.Fatalf("send request: %v", err)
	}
	if n := gw.PendingCount(); n != 0 {
		t.Fatalf("settled request pending count %d, want 0", n)
	}
}

// ---- Remote.Drain 装配 ----

// gateDeactEntity Deactivate 停在 gate 上的实体包装：排空阶段②的确定性
// 停点——销毁推进到 Deactivate 即关 entered，阻塞至 release。
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

// echoHandle 回显请求的缺省 Handle。
func echoHandle(_ context.Context, req any) (any, error) { return req.(int) + 1, nil }

// 6. Drain 优雅停机装配：本地实体全数销毁（统一销毁路径带目录条件注销）、
// 目录条目腾清、pending 归零、返回 nil；Drain 后 Stop 无害（幂等收尾）。
func TestRemoteDrainDestroysLocalEntities(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	nodeA := startRemNode(t, dir, rt, nodeSpec{id: 1, handle: echoHandle})
	nodeB := startRemNode(t, dir, rt, nodeSpec{id: 2, handle: echoHandle})
	rt.set(map[rans.NodeID]string{1: nodeA.rem.Addr(), 2: nodeB.rem.Addr()})
	ctx := context.Background()

	// 预注册指向节点 2：A 的调用走远程，实体落在 B
	ids := []rans.ID{mustUUID(t, "d1"), mustUUID(t, "d2")}
	for _, id := range ids {
		if _, _, err := dir.Register(ctx, id, directory.Owner{Node: 2}, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		if v, err := nodeA.rem.Call(ctx, id, 1); err != nil || v != 2 {
			t.Fatalf("call %s: v=%v err=%v", id.String(), v, err)
		}
	}
	if got := len(nodeB.mgr.LocalIDs()); got != 2 {
		t.Fatalf("node B must hold 2 local entities before drain, got %d", got)
	}

	if err := nodeB.rem.Shutdown(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := len(nodeB.mgr.LocalIDs()); got != 0 {
		t.Fatalf("drain must destroy all local entities, %d left", got)
	}
	for _, id := range ids {
		if _, found, err := dir.Lookup(ctx, id); err != nil || found {
			t.Fatalf("entity %s must be unregistered from directory after drain (found=%v err=%v)",
				id.String(), found, err)
		}
	}
	if n := nodeB.rem.gw.PendingCount(); n != 0 {
		t.Fatalf("pending table must be empty after drain flush, got %d", n)
	}
	_ = nodeB.rem.Close() // Shutdown 后 Close：幂等无害
}

// 7. 排空门闸的线上行为：Shutdown 停在阶段②（实体 Deactivate
// 阻塞——确定性停点）时，发往交接窗实体（本地表命中）的新请求**留箱**——
// 不执行、不立即回绝；实体终局（含目录条件注销）后留箱消息按确定未执行以
// StatusUnavailable 回绝；调用方重试时目录已空，路由翻转在他节点激活成功。
func TestRemoteDrainGateRejectsNewRequests(t *testing.T) {
	dir := inmem.New()
	rt := &resolveTable{}
	deactEntered := make(chan struct{})
	deactRelease := make(chan struct{})
	var bHandles atomic.Int32
	nodeA := startRemNode(t, dir, rt, nodeSpec{id: 1, handle: echoHandle})
	nodeB := startRemNode(t, dir, rt, nodeSpec{
		id: 2,
		handle: func(ctx context.Context, req any) (any, error) {
			bHandles.Add(1)
			return echoHandle(ctx, req)
		},
		wrapEntity: func(e rans.Entity) rans.Entity {
			return &gateDeactEntity{Entity: e, entered: deactEntered, release: deactRelease}
		},
	})
	rt.set(map[rans.NodeID]string{1: nodeA.rem.Addr(), 2: nodeB.rem.Addr()})
	ctx := context.Background()

	id := mustUUID(t, "g1")
	if _, _, err := dir.Register(ctx, id, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatal(err)
	}
	if v, err := nodeA.rem.Call(ctx, id, 1); err != nil || v != 2 {
		t.Fatalf("call: v=%v err=%v", v, err)
	}
	if got := bHandles.Load(); got != 1 {
		t.Fatalf("pre-drain handles = %d, want 1", got)
	}

	drained := make(chan error, 1)
	go func() { drained <- nodeB.rem.Shutdown(context.Background()) }()
	select {
	case <-deactEntered: // 阶段②已推进到实体 Deactivate ⟹ 阶段①门闸必已置位
	case <-time.After(5 * time.Second):
		t.Fatal("drain never reached entity deactivate (stage ②)")
	}

	// 交接窗期间的调用：留箱（不执行、不立即回绝）
	type callResult struct {
		v   any
		err error
	}
	pending := make(chan callResult, 1)
	go func() {
		v, err := nodeA.rem.Call(context.Background(), id, 2)
		pending <- callResult{v, err}
	}()
	select {
	case r := <-pending:
		t.Fatalf("call during retire window must stay in the box, got v=%v err=%v", r.v, r.err)
	case <-time.After(300 * time.Millisecond):
	}
	if got := bHandles.Load(); got != 1 {
		t.Fatalf("boxed call must not execute while retiring, handles = %d", got)
	}

	// 放行：终局注销完成 → 留箱消息按确定未执行以 Unavailable 回绝 → 调用方
	// 的环内重试（Unavailable 属确定未执行必重试）重路由——目录已空，翻转本节点
	// 激活执行。对调用方呈现为同一次调用最终成功（零重复执行）。
	close(deactRelease)
	var r callResult
	select {
	case r = <-pending:
	case <-time.After(5 * time.Second):
		t.Fatal("boxed call was never settled after drain finished")
	}
	if r.err != nil {
		t.Fatalf("boxed call must converge via reroute after the deferred rejection, got err=%v", r.err)
	}
	if r.v != 3 {
		t.Fatalf("rerouted call = %v, want 3", r.v)
	}
	if err := <-drained; err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := len(nodeB.mgr.LocalIDs()); got != 0 {
		t.Fatalf("drain must finish destroying, %d left", got)
	}
	// 交接窗实体上被回绝的投递零重复执行（B 只执行过第一次调用）；
	// 重试的执行落在他节点 A（本地激活恰一次）。
	if got := bHandles.Load(); got != 1 {
		t.Fatalf("rerouted call must not re-execute on the drained node, handles = %d", got)
	}
	if n := len(nodeA.mgr.LocalIDs()); n != 1 {
		t.Fatalf("rerouted entity must be local to node A, locals = %d", n)
	}
}
