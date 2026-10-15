package testkit

// 票 13 红测：本地与远端调用超时都满足标准库判错 errors.Is(err,
// context.DeadlineExceeded) 且仍可判 rans.CodeOf == CodeTimeout。

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// sleepEntity Handle 睡 2s（远超调用预算）。
type sleepEntity struct{}

func (sleepEntity) Activate(context.Context) error   { return nil }
func (sleepEntity) Deactivate(context.Context) error { return nil }

func (sleepEntity) Handle(ctx context.Context, _ any) (any, error) {
	select {
	case <-time.After(2 * time.Second):
		return "slow", nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestNodeCall_TimeoutIsDeadlineExceeded(t *testing.T) {
	kinds := map[string]rans.Factory{"sleeper": func(rans.ID) rans.Entity { return sleepEntity{} }}

	// 本地腿：单节点，调用预算 150ms。
	c1 := NewCluster(t, 1, kinds)
	localID, _ := rans.NewID("sleeper", "l1")
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := c1.Node(0).Call(ctx, localID, "go")
	if err == nil {
		t.Fatal("local: expected timeout")
	}
	if rans.CodeOf(err) != rans.CodeTimeout || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("local: err = %v (code=%v), want Timeout matching DeadlineExceeded", err, rans.CodeOf(err))
	}

	// 远端腿：实体落 node2（首调激活），node1 远调。
	c2 := NewCluster(t, 2, kinds)
	remoteID, _ := rans.NewID("sleeper", "r1")
	if _, err := c2.Node(1).Call(context.Background(), remoteID, "warm"); err != nil {
		t.Fatalf("pre-activate on node2: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel2()
	_, err = c2.Node(0).Call(ctx2, remoteID, "go")
	if err == nil {
		t.Fatal("remote: expected timeout")
	}
	if rans.CodeOf(err) != rans.CodeTimeout || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("remote: err = %v (code=%v), want Timeout matching DeadlineExceeded", err, rans.CodeOf(err))
	}
}
