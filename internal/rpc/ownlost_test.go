package rpc

// 所有权丢失通知（MsgOwnershipLost，H13 决策 22 收端半边）的 dispatcher
// 验收：本地实例归属与被夺走旧归属三元全等 → Fence（毫秒级关准入）+
// 有界异步销毁（LocalIDs 收敛）；归属不符/无实例（迟到/重复/换代）→
// 幂等 no-op。发送半边（接管赢方单调用点）不在本套件覆盖内——它由
// testkit 的 TestFault_TakeoverForm1MemberKeyDeleted e2e 经真实发送链
// 锁定（活体假死 → CAS 接管 → 通知在真 wire 上到达活接收方）。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

func ownLostHdrFor(id uint64, inst string) wire.Header {
	return wire.Header{
		Type:         protocol.MsgOwnershipLost,
		Flags:        protocol.FlagHasSourceNodeID | protocol.FlagHasTargetUUID,
		MessageID:    id,
		SourceNodeID: 1,
		TargetKind:   "cell",
		TargetInst:   inst,
	}
}

// TestOwnershipLoss_DirectNotifyFences：激活实例后注入携当前归属的
// MsgOwnershipLost → 实例被围栏并销毁（LocalIDs 收敛，秒级）；随后注入
// 陈旧纪元与无关身份的通知 → 幂等无害（无 panic、无额外销毁）。
func TestOwnershipLoss_DirectNotifyFences(t *testing.T) {
	served := make(chan struct{}, 4)
	env := newDispEnv(t, func(_ context.Context, _ any) (any, error) {
		served <- struct{}{}
		return "ok", nil
	}, false)

	const reqID = 951
	env.feed(reqHdrFor(protocol.MsgRequest, reqID, "own-1", nil), envBody(t, 1))
	select {
	case <-served:
	case <-time.After(2 * time.Second):
		t.Fatal("precondition: request must execute to activate the instance")
	}

	id, err := rans.NewID("cell", "own-1")
	if err != nil {
		t.Fatal(err)
	}
	owner, ok := env.mgr.InstanceOwner(id)
	if !ok {
		t.Fatal("precondition: instance owner must be captured")
	}
	if !envMgrHas(env, id) {
		t.Fatal("precondition: instance must be local before notify")
	}

	env.feed(ownLostHdrFor(952, "own-1"), wire.EncodeOwnershipLost(
		wire.OwnershipLost{Epoch: owner.Epoch, Incarnation: owner.Incarnation}))

	deadline := time.Now().Add(3 * time.Second)
	for envMgrHas(env, id) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if envMgrHas(env, id) {
		t.Fatal("ownership-lost must fence and destroy the matching instance promptly")
	}

	// 幂等/换代无害：同 id 陈旧纪元、无关 id 的通知不产生副作用。
	env.feed(ownLostHdrFor(953, "own-1"), wire.EncodeOwnershipLost(
		wire.OwnershipLost{Epoch: owner.Epoch, Incarnation: owner.Incarnation + 7}))
	env.feed(ownLostHdrFor(954, "other-9"), wire.EncodeOwnershipLost(
		wire.OwnershipLost{Epoch: 1, Incarnation: 1}))
	time.Sleep(50 * time.Millisecond)
}

// TestOwnershipLoss_SiblingWithSameOwnerTripleUnaffected：同节点同代际两个
// 首发实体（Incarnation per-id ⟹ owner 三元组同为 {self,E,1}），对 A 的
// ownership-lost 只准作用于 A——A 被围栏并销毁；B 不被围栏（仍可收投递）、
// 不被销毁（仍在激活表）。围栏只升不降，误围即永久损伤：围栏判据必须
// id + owner 双维度全等，owner 全等单独不构成围栏依据。
func TestOwnershipLoss_SiblingWithSameOwnerTripleUnaffected(t *testing.T) {
	served := make(chan int, 8)
	env := newDispEnv(t, func(_ context.Context, req any) (any, error) {
		served <- req.(int)
		return "ok", nil
	}, false)

	// 前置：A/B 先后首发激活（同工厂、同节点、同代际）；两实例并发执行，
	// 完成序不确定，按载荷集合断言。
	env.feed(reqHdrFor(protocol.MsgRequest, 961, "own-a", nil), envBody(t, 1))
	env.feed(reqHdrFor(protocol.MsgRequest, 962, "own-b", nil), envBody(t, 2))
	gotPre := map[int]bool{}
	for range [2]struct{}{} {
		select {
		case got := <-served:
			gotPre[got] = true
		case <-time.After(2 * time.Second):
			t.Fatal("precondition: both siblings must activate via first request")
		}
	}
	if !gotPre[1] || !gotPre[2] {
		t.Fatalf("precondition: both siblings must serve, got %v", gotPre)
	}

	idA, err := rans.NewID("cell", "own-a")
	if err != nil {
		t.Fatal(err)
	}
	idB, err := rans.NewID("cell", "own-b")
	if err != nil {
		t.Fatal(err)
	}
	ownerA, ok := env.mgr.InstanceOwner(idA)
	if !ok {
		t.Fatal("precondition: sibling A owner must be captured")
	}
	ownerB, ok := env.mgr.InstanceOwner(idB)
	if !ok {
		t.Fatal("precondition: sibling B owner must be captured")
	}
	// 碰撞前提锚定：同节点同代际首发 ⟹ 三元组全等（本测试的立论前提）。
	if ownerA != ownerB {
		t.Fatalf("precondition: same-generation activations must share the owner triple, got %+v vs %+v", ownerA, ownerB)
	}
	// 实例身份锚：存活断言认的是 B 的**这一个**实例——误杀后按需再激活
	// 会造出新化身顶替（外部看 B「还在」），身份漂移即数据丢失，必须锚死。
	pawnB0, ok := env.mgr.GetLocal(idB)
	if !ok {
		t.Fatal("precondition: sibling B must be locally activated")
	}

	env.feed(ownLostHdrFor(963, "own-a"), wire.EncodeOwnershipLost(
		wire.OwnershipLost{Epoch: ownerA.Epoch, Incarnation: ownerA.Incarnation}))

	// A 被销毁（围栏后止损形态，LocalIDs 收敛）。
	deadline := time.Now().Add(3 * time.Second)
	for envMgrHas(env, idA) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if envMgrHas(env, idA) {
		t.Fatal("ownership-lost must fence and destroy the targeted instance promptly")
	}

	// B 不受影响：先证仍可收投递（被围栏即回绝、被销毁即 NotFound/再激活，
	// 误伤形态都到不了原实例的 handler），再证原实例仍在激活表、工厂未为
	// B 再造新化身。
	env.feed(reqHdrFor(protocol.MsgRequest, 964, "own-b", nil), envBody(t, 3))
	select {
	case got := <-served:
		if got != 3 {
			t.Fatalf("sibling served wrong payload %d, want 3", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sibling entity must still receive deliveries after the targeted instance's ownership-lost")
	}
	if !envMgrHas(env, idB) {
		t.Fatal("sibling entity with the same owner triple must stay activated")
	}
	if pawnB1, ok := env.mgr.GetLocal(idB); !ok || pawnB1 != pawnB0 {
		t.Fatal("the original sibling instance must survive untouched (same pawn), not be fenced or replaced")
	}
	if n := env.led.count(); n != 2 {
		t.Fatalf("unaffected sibling must not trigger re-activation, factory created %d entities", n)
	}
}

func envMgrHas(env *dispEnv, id rans.ID) bool {
	for _, l := range env.mgr.LocalIDs() {
		if l == id {
			return true
		}
	}
	return false
}

var _ = directory.Owner{} // 依赖锚：归属三元组类型（防误删导入）
