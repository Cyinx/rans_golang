package activation

// MetricsView / pawnite.InboxLen 观测面测试（规格 §16
// queue_depth/inbox_depth Gauge 的数据源）：本地实体数与 Σ 收件箱积压深度
// 在实体执行阻塞期间如实反映。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// blockDelivery 阻塞型入站投递：Deliver 进入即发信号、随后阻塞到 release
// ——把 loop goroutine 钉死在首条投递上，后续投递滞留收件箱（inbox 深度
// 观测的时序锚点）。Reject 仅回执计数（销毁排空路径）。
type blockDelivery struct {
	id      rans.ID
	entered chan struct{}
	release chan struct{}
}

func (d *blockDelivery) UUID() rans.ID { return d.id }
func (d *blockDelivery) Deliver(*pawnite.Pawnite) {
	d.entered <- struct{}{}
	<-d.release
}
func (d *blockDelivery) Reject(rans.Code, rans.Reason) {}

// TestManagerMetricsView 实体阻塞执行期间：entities=1（本地注册表条目数）、
// inboxDepth=滞留投递数（首条在执行、其余在箱）；pawnite.InboxLen 与视图
// 一致。放行销毁后视图归零。
func TestManagerMetricsView(t *testing.T) {
	m := newTestManager(t) // kind "counter"：实体 Handle 不参与（直投 ReceiveRequest）
	id, _ := rans.NewID("counter", "mv-1")
	release := make(chan struct{})

	p, err := m.GetOrActivate(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	// 首投递占据执行位（进入信号钉死「loop 已拿起首条」时点）。
	first := &blockDelivery{id: id, entered: make(chan struct{}, 1), release: release}
	if !p.ReceiveRequest(first) {
		t.Fatal("first delivery must be admitted")
	}
	select {
	case <-first.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first delivery not picked up by loop")
	}

	// 三条滞留投递：全部滞留收件箱。
	const pending = 3
	for i := 0; i < pending; i++ {
		d := &blockDelivery{id: id, entered: make(chan struct{}, 1), release: release}
		if !p.ReceiveRequest(d) {
			t.Fatalf("queued delivery %d must be admitted", i)
		}
	}

	entities, inboxDepth := m.MetricsView()
	if entities != 1 {
		t.Fatalf("MetricsView entities = %d, want 1", entities)
	}
	if inboxDepth != pending {
		t.Fatalf("MetricsView inboxDepth = %d, want %d", inboxDepth, pending)
	}
	if got := p.InboxLen(); got != pending {
		t.Fatalf("pawnite.InboxLen = %d, want %d", got, pending)
	}

	// 放行 + 销毁收口：视图归零（销毁排空回绝滞留投递）。
	close(release)
	_ = p.Destroy(context.Background()) // 终局闭环直驱（DestroyLocal 已迁 lifecycle）
	if entities, inboxDepth = m.MetricsView(); entities != 0 || inboxDepth != 0 {
		t.Fatalf("MetricsView after destroy = (%d, %d), want (0, 0)", entities, inboxDepth)
	}
}
