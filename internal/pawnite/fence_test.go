package pawnite

import (
	"context"
	"testing"

	"github.com/cyinx/rans_golang"
)

type fenceDelivery struct {
	id        rans.ID
	onDeliver func()
	rejected  bool
	rejStatus rans.Code
	rejReason rans.Reason
}

func (d *fenceDelivery) UUID() rans.ID { return d.id }
func (d *fenceDelivery) Deliver(*Pawnite) {
	if d.onDeliver != nil {
		d.onDeliver()
	}
}
func (d *fenceDelivery) Reject(st rans.Code, reason rans.Reason) {
	d.rejected, d.rejStatus, d.rejReason = true, st, reason
}

func fenceID(t testing.TB, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID("k", inst)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// Fence 后新投递被回绝（Unavailable/lease lost），在途投递不受影响，幂等。
func TestFence_RejectsNewDeliveriesInFlightUnaffected(t *testing.T) {
	inflight := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	p := New(fenceID(t, "1"), Options{Activate: func(context.Context, *Pawnite) error { return nil }})
	p.Start(context.Background())
	defer p.Destroy(context.Background())

	slow := &fenceDelivery{id: fenceID(t, "1"), onDeliver: func() {
		close(inflight)
		<-release
		close(done)
	}}
	if !p.ReceiveRequest(slow) {
		t.Fatal("在途投递应被准入")
	}
	<-inflight // 确认 Handle 已在跑

	p.Fence()
	p.Fence() // 幂等

	rej := &fenceDelivery{id: fenceID(t, "1")}
	if p.ReceiveRequest(rej) {
		t.Fatal("Fence 后新投递必须被回绝")
	}
	if rej.rejStatus != rans.CodeUnavailable || rej.rejReason != rans.ReasonLeaseLost {
		t.Fatalf("期望 Unavailable/LeaseLost，得到 %v/%v", rej.rejStatus, rej.rejReason)
	}

	close(release)
	<-done // 在途投递正常完成
}
