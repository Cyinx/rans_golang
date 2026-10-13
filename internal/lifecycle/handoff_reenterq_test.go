package lifecycle

// 交接窗挂起判据与续体排队的信号同时就绪交错测试（审查 E5 蓝图回迁，断言
// 反向：终快照必含续体写）。completeWork 在单临界区完成「works 清零 + 续体
// 入队 + 续体信号」；此后 handoffCh 与 reenterCh 同时就绪时由 loop select
// 随机选择——判据漏查续体队列时选中 handoff 即拍终快照，续体永久蒸发。
// 两种触发次序各半摇测：A=先放行 work（续体先入队）再销毁；B=先销毁再放行
// （挂起判据先见在飞 work，随后与续体入队交错）。

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/activation"
)

// reenterRaceEntity 交错道具：arm 挂一个阻塞 work，then 落续体写（flag）；
// 快照以 flag 分形——终快照缺 THEN-RAN 即续体写丢失。
type reenterRaceEntity struct {
	flag    bool
	release chan struct{}
	started chan struct{}
	once    sync.Once
}

func (e *reenterRaceEntity) Activate(context.Context) error   { return nil }
func (e *reenterRaceEntity) Deactivate(context.Context) error { return nil }

func (e *reenterRaceEntity) Handle(ctx context.Context, msg any) (any, error) {
	m, _ := msg.(string)
	switch m {
	case "arm":
		rans.ReenterAfter(ctx,
			func(ctx context.Context) (any, error) {
				e.once.Do(func() { close(e.started) })
				<-e.release
				return nil, nil
			},
			func(any, error) { e.flag = true })
		return nil, nil
	case "get":
		return e.flag, nil
	}
	return nil, nil
}

func (e *reenterRaceEntity) Snapshot() ([]byte, error) {
	if e.flag {
		return []byte("THEN-RAN"), nil
	}
	return []byte("NO-THEN"), nil
}

func (e *reenterRaceEntity) Restore(b []byte) error {
	e.flag = strings.Contains(string(b), "THEN-RAN")
	return nil
}

var (
	raceMu    sync.Mutex
	raceInsts = map[rans.ID]*reenterRaceEntity{}
	raceSeq   atomic.Int64
)

func reenterRaceFactory(id rans.ID) rans.Entity {
	e := &reenterRaceEntity{release: make(chan struct{}), started: make(chan struct{})}
	raceMu.Lock()
	raceInsts[id] = e
	raceMu.Unlock()
	return e
}

// TestHandoffFinalSnapshotWaitsQueuedContinuation 终快照必含已确认的续体写：
// work 已完成、续体已入队的窗口不得放行 Retire core。销毁全程事件驱动，
// 无定时依赖；次序 A/B 各半覆盖「续体先就绪」与「两信号同时就绪」两种
// select 交错形态。
func TestHandoffFinalSnapshotWaitsQueuedContinuation(t *testing.T) {
	const rounds = 120
	store := checkpoint.New()
	m := activation.New(inmem.New(), 1, map[string]rans.Factory{
		"raceq": reenterRaceFactory,
	}, activation.Options{Store: store})
	ctx := context.Background()

	for i := 0; i < rounds; i++ {
		id := mustTestID(t, "raceq", fmt.Sprintf("rq-%d-%d", raceSeq.Add(1), i))
		if _, err := m.InvokeLocal(ctx, id, "arm"); err != nil {
			t.Fatalf("round %d: arm: %v", i, err)
		}
		raceMu.Lock()
		e := raceInsts[id]
		raceMu.Unlock()
		if e == nil {
			t.Fatalf("round %d: entity instance not registered by factory", i)
		}
		select {
		case <-e.started:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: reenter work never started", i)
		}
		done := make(chan error, 1)
		if i%2 == 0 {
			// 次序 A：先放行 work（续体入队+信号）再触发销毁。
			close(e.release)
			go func() { done <- DestroyLocal(ctx, m, id) }()
		} else {
			// 次序 B：先触发销毁再放行——挂起判据先见在飞 work，
			// completeWork 与挂起唤醒在 loop select 上交错。
			go func() { done <- DestroyLocal(ctx, m, id) }()
			close(e.release)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("round %d: destroy: %v", i, err)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("round %d: destroy never returned", i)
		}
		v, ok, err := store.Get(ctx, id)
		if err != nil {
			t.Fatalf("round %d: store get: %v", i, err)
		}
		if !ok {
			t.Fatalf("round %d: final snapshot missing (retire core never ran)", i)
		}
		if !strings.Contains(string(v), "THEN-RAN") {
			t.Fatalf("round %d: final snapshot lacks continuation write "+
				"(then never ran before core) — quiescence check missed reenterQ", i)
		}
	}
}
