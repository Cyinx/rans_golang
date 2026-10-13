package rpc

// 所有权丢失通知（MsgOwnershipLost，H13 决策 22 收端半边）的 dispatcher
// 验收：本地实例归属与被夺走旧归属三元全等 → Fence（毫秒级关准入）+
// 有界异步销毁（LocalIDs 收敛）；归属不符/无实例（迟到/重复/换代）→
// 幂等 no-op。发送半边（接管赢方单调用点）经 testkit 接管链与终审复核。

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

func envMgrHas(env *dispEnv, id rans.ID) bool {
	for _, l := range env.mgr.LocalIDs() {
		if l == id {
			return true
		}
	}
	return false
}

var _ = directory.Owner{} // 依赖锚：归属三元组类型（防误删导入）
