package contract

// RenewNode/守卫注销契约用例（A1/B10 注册表原语）：键缺席不重建、同代际
// 续期延寿、更大代际 Superseded 值不动、更小代际覆写；注销带纪元守卫。
// 经 RunNodeMembershipSuite 调用，inmem/redis/cluster 自动覆盖。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang/directory"
)

// runRenewSuite 比对续期与守卫注销契约体（由 RunNodeMembershipSuite 调用）。
func runRenewSuite(t *testing.T, ctx context.Context, newReg func(t *testing.T, hbTTL time.Duration) directory.NodeRegistry) {
	t.Run("RenewNode：缺席不重建、续期延寿、代际裁定、覆写旧值", func(t *testing.T) {
		d := newReg(t, 300*time.Millisecond)

		// 键缺席 → Absent 且不重建（复活须走 RegisterNode 重取号）。
		res, err := d.RenewNode(ctx, 5, directory.NodeInfo{Addr: "10.0.0.5:1", Epoch: 4})
		if err != nil || res != directory.RenewAbsent {
			t.Fatalf("renew on absent key = (%v,%v), want (RenewAbsent,nil)", res, err)
		}
		if _, ok, err := d.NodeAddr(ctx, 5); err != nil || ok {
			t.Fatalf("absent renew must not recreate the key: (%v,%v)", ok, err)
		}

		// 同代际续期 → Renewed，跨多个 TTL 周期仍在视图。
		if err := d.RegisterNode(ctx, 5, directory.NodeInfo{Addr: "10.0.0.5:1", Epoch: 4}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(900 * time.Millisecond) // 3×TTL
		for time.Now().Before(deadline) {
			res, err := d.RenewNode(ctx, 5, directory.NodeInfo{Addr: "10.0.0.5:1", Epoch: 4})
			if err != nil || res != directory.RenewRenewed {
				t.Fatalf("renew = (%v,%v), want (RenewRenewed,nil)", res, err)
			}
			time.Sleep(80 * time.Millisecond)
		}
		if nodes, err := d.NodeIDs(ctx); err != nil || len(nodes) != 1 || nodes[0] != 5 {
			t.Fatalf("renewed node must stay in view past multiple TTLs: %v (err=%v)", nodes, err)
		}

		// 键内代际更大 → Superseded 且值不动。
		if res, err := d.RenewNode(ctx, 5, directory.NodeInfo{Addr: "10.0.0.5:OLD", Epoch: 3}); err != nil || res != directory.RenewSuperseded {
			t.Fatalf("renew with older epoch = (%v,%v), want (RenewSuperseded,nil)", res, err)
		}
		if ep, ok, err := d.NodeEpoch(ctx, 5); err != nil || !ok || ep != 4 {
			t.Fatalf("superseded renew must not touch the value: epoch=(%v,%v,%v), want (4,true,nil)", ep, ok, err)
		}
		if addr, ok, _ := d.NodeAddr(ctx, 5); !ok || addr != "10.0.0.5:1" {
			t.Fatalf("superseded renew must not touch the value: addr=%q ok=%v", addr, ok)
		}

		// 键内代际更小 → 覆写为本代际并 Renewed。
		if res, err := d.RenewNode(ctx, 5, directory.NodeInfo{Addr: "10.0.0.5:2", Epoch: 9}); err != nil || res != directory.RenewRenewed {
			t.Fatalf("renew with newer epoch = (%v,%v), want (RenewRenewed,nil)", res, err)
		}
		if ep, ok, err := d.NodeEpoch(ctx, 5); err != nil || !ok || ep != 9 {
			t.Fatalf("renew must overwrite with the caller epoch: (%v,%v,%v), want (9,true,nil)", ep, ok, err)
		}
	})

	t.Run("UnregisterNode：纪元守卫与无条件档", func(t *testing.T) {
		d := newReg(t, time.Minute)
		if err := d.RegisterNode(ctx, 6, directory.NodeInfo{Addr: "10.0.0.6:1", Epoch: 7}); err != nil {
			t.Fatal(err)
		}
		// 陈旧代际不删更新代际的键。
		if err := d.UnregisterNode(ctx, 6, 6); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := d.NodeAddr(ctx, 6); !ok {
			t.Fatal("stale-epoch unregister must not delete a newer-epoch key")
		}
		// 当前代际删除。
		if err := d.UnregisterNode(ctx, 6, 7); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := d.NodeAddr(ctx, 6); ok {
			t.Fatal("current-epoch unregister must delete the key")
		}
		// 无条件档（0 = 现语义）。
		if err := d.RegisterNode(ctx, 6, directory.NodeInfo{Addr: "10.0.0.6:1", Epoch: 8}); err != nil {
			t.Fatal(err)
		}
		if err := d.UnregisterNode(ctx, 6, 0); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := d.NodeAddr(ctx, 6); ok {
			t.Fatal("unconditional unregister must delete the key")
		}
	})
}
