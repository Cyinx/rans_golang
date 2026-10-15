package testkit

// 周期 checkpoint 落库 ctx 的集群面验收：Store 契约允许实现「阻塞于 IO 至
// ctx 结束」（database/sql 的 ExecContext 先查 ctx.Err()）——周期 Put 的
// ctx 不得随扫描轮返回而取消，但仍受本轮 deadline 约束（到点未完即弃）。

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
)

// ctxStore 尊重 ctx 的 Store 包装：Put 在已结束的 ctx 上直接失败，否则
// 委托底座。canceled 计被取消 ctx 拒绝的次数（红因判别）；unbounded 计
// 落库时 ctx 无 deadline 或剩余时限超 bound 的次数（时限保留判别）。
type ctxStore struct {
	checkpoint.Store
	bound     time.Duration
	canceled  atomic.Int64
	unbounded atomic.Int64
}

func (s *ctxStore) Put(ctx context.Context, id rans.ID, inc uint64, snap []byte) error {
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			s.canceled.Add(1)
		}
		return err
	}
	if dl, ok := ctx.Deadline(); !ok || time.Until(dl) > s.bound {
		s.unbounded.Add(1)
	}
	return s.Store.Put(ctx, id, inc, snap)
}

// TestCheckpoint_PeriodicPutSurvivesSweepReturn 单节点 + 尊重 ctx 的 Store：
// 写一次后周期 checkpoint 必须在有界时间内落库（收尾 goroutine 的 Put 晚于
// 扫描轮返回，ctx 不得已被取消），且落库 ctx 的剩余时限不超扫描间隔。
func TestCheckpoint_PeriodicPutSurvivesSweepReturn(t *testing.T) {
	const interval = 50 * time.Millisecond
	base := checkpoint.New()
	store := &ctxStore{Store: base, bound: interval}
	c := NewCluster(t, 1, accCarrierKinds(accCarrierOpt{}), WithCheckpointStore(store),
		WithCheckpointInterval(interval))
	ctx := context.Background()
	id := mustID(t, "carrier", "ckpt-put-ctx")

	if v, err := c.Node(0).Call(ctx, id, 0); err != nil || v != int64(1) {
		t.Fatalf("call: v=%v err=%v (want 1)", v, err)
	}
	const wait = 5 * time.Second
	for deadline := time.Now().Add(wait); ; time.Sleep(10 * time.Millisecond) {
		b, found, err := base.Get(ctx, id)
		if err == nil && found && accSnapVal(b) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("periodic checkpoint never landed within %s: %d Put(s) rejected by a canceled ctx",
				wait, store.canceled.Load())
		}
	}
	if n := store.unbounded.Load(); n != 0 {
		t.Fatalf("%d landed Put(s) had no deadline or one beyond the scan interval %s", n, interval)
	}
}
