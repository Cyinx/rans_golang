package pawnite

import (
	"context"
	"errors"
	"testing"
	"time"
)

// waitForState 轮询等待目标状态（有界），避免定时睡眠依赖调度时序。
func waitForState(t *testing.T, p *Pawnite, want State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for p.State() != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.State() != want {
		t.Fatalf("state = %v, want %v (timed out)", p.State(), want)
	}
}

// TestDestroy_StuckDeactivate_HonorsCtxRetriable 钉死 ctx-honoring 销毁：
// Deactivate 不合作阻塞时，Destroy 在 ctx 耗尽
// 边界返回 ctx.Err() 且不再改状态（loop 自驱终态）；放行后凭新 ctx 重试
// 必须完成，Done 最终关闭。
func TestDestroy_StuckDeactivate_HonorsCtxRetriable(t *testing.T) {
	id := mustUUID(t, "player", "d1")
	entered := make(chan struct{}) // Deactivate 已进入
	release := make(chan struct{}) // 放行 Deactivate
	p := New(id, Options{
		Activate: func(ctx context.Context, p *Pawnite) error { return nil },
		Deactivate: func(ctx context.Context, p *Pawnite) error {
			close(entered)
			<-release
			return nil
		},
	})
	p.Start(context.Background())
	waitForState(t, p, StateActive)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := p.Destroy(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Destroy err = %v, want context.DeadlineExceeded", err)
	}
	// ctx 返回后状态不得被进一步改动：loop 仍停在 Deactivate，终态未落。
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Deactivate never ran; destroy not stuck as arranged")
	}
	if s := p.State(); s != StateDestroying {
		t.Fatalf("state after ctx exhaustion = %v, want StateDestroying (must not mutate further)", s)
	}
	select {
	case <-p.Done():
		t.Fatal("Done must not be closed while Deactivate is stuck")
	default:
	}

	close(release) // 放行卡死的 Deactivate
	if err := p.Destroy(context.Background()); err != nil {
		t.Fatalf("retry Destroy err = %v, want nil", err)
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state after retry = %v, want StateDestroyed", p.State())
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done must be closed after retry completes")
	}
}

// TestDestroy_PreCanceledCtx_StillTransitions 已取消 ctx：Destroying 转移
// 照常发生（转移先于等待），等待段立即返回 ctx.Err()；重试完成闭环。
// （ctx-honoring 是唯一销毁语义——不存在忽略形参 ctx 无条件等终态的形态。）
func TestDestroy_PreCanceledCtx_StillTransitions(t *testing.T) {
	id := mustUUID(t, "player", "d2")
	p := New(id, Options{Activate: func(ctx context.Context, p *Pawnite) error { return nil }})
	p.Start(context.Background())
	waitForState(t, p, StateActive)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Destroy(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Destroy err = %v, want context.Canceled", err)
	}
	if s := p.State(); s != StateDestroying && s != StateDestroyed {
		t.Fatalf("state after pre-canceled Destroy = %v, want Destroying/Destroyed", s)
	}
	if err := p.Destroy(context.Background()); err != nil {
		t.Fatalf("retry Destroy err = %v, want nil", err)
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state after retry = %v, want StateDestroyed", p.State())
	}
}

// TestDestroy_BeforeStart_FastPathNil Start 前快路径不经等待段：就地终态、
// done 关闭，必返回 nil（同 Destroy 快路径语义）。
func TestDestroy_BeforeStart_FastPathNil(t *testing.T) {
	id := mustUUID(t, "player", "d3")
	p := New(id, Options{})
	if err := p.Destroy(context.Background()); err != nil {
		t.Fatalf("Destroy before Start err = %v, want nil", err)
	}
	if p.State() != StateDestroyed {
		t.Fatalf("state = %v, want StateDestroyed", p.State())
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Done must be closed on fast path")
	}
}
