package testkit

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// 每 Pawnite 定时器验收（票 03）：终局（销毁/迁移）之后彻底静默——到期点不再
// 触发，定时器不随迁移转移。

func TestAddTimer_SilentAfterDestroy(t *testing.T) {
	report := make(chan string, 4)
	clock := NewVirtualClock()
	c := NewCluster(t, 1, timerKinds(report), WithTimerClock(clock))
	id, _ := rans.NewID("timerprobe", "x1")
	armTimer(t, c, id, "never", 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Node(0).Destroy(ctx, id); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	clock.Advance(time.Hour)
	expectSilent(t, report, "销毁后推进到原到期点")
}

func TestAddTimer_SilentAfterMigrate(t *testing.T) {
	report := make(chan string, 4)
	clock := NewVirtualClock()
	placement := &migrateArmedPlacement{target: 2}
	c := NewCluster(t, 2, timerKinds(report), WithTimerClock(clock), WithNodePlacement(0, placement))
	id, _ := rans.NewID("timerprobe", "x2")
	armTimer(t, c, id, "never", 10*time.Millisecond)
	placement.arm()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Node(0).Migrate(ctx, id); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if c.Node(0).Holds(id) {
		t.Fatal("迁移后源节点不应再持有实例")
	}
	clock.Advance(time.Hour)
	expectSilent(t, report, "迁出后推进到原到期点")
}
