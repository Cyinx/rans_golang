package activation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// mustTestID 构造测试身份（失败即 Fatal）。
func mustTestID(t testing.TB, kind, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID(kind, inst)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// funcEntity 把 Handle 函数适配为 rans.Entity（Activate/Deactivate 恒成功）。
type funcEntity func(ctx context.Context, req any) (any, error)

func (f funcEntity) Activate(ctx context.Context) error { return nil }
func (f funcEntity) Handle(ctx context.Context, req any) (any, error) {
	return f(ctx, req)
}
func (f funcEntity) Deactivate(ctx context.Context) error { return nil }

// nopTestEntity 恒成功实体。
type nopTestEntity struct{}

func (nopTestEntity) Activate(ctx context.Context) error { return nil }
func (nopTestEntity) Handle(ctx context.Context, req any) (any, error) {
	return "ok", nil
}
func (nopTestEntity) Deactivate(ctx context.Context) error { return nil }

// manager.Fence 只关本地实例准入：新 Call 以 rans.ErrUnavailable 族回绝，在途 Handle 完成。
func TestFence_LocalAdmissionCutoff(t *testing.T) {
	var m *Table
	release := make(chan struct{})
	inflight := make(chan struct{})
	X := mustTestID(t, "k", "f1")
	m = New(inmem.New(), 1, map[string]rans.Factory{
		"k": func(_ rans.ID) rans.Entity {
			return funcEntity(func(ctx context.Context, req any) (any, error) {
				if req == "slow" {
					close(inflight)
					<-release
				}
				return "ok", nil
			})
		},
	})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = m.InvokeLocal(ctx, X, "slow")
	}()
	<-inflight

	m.Fence(X)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := m.InvokeLocal(ctx, X, "get")
	if !errors.Is(err, rans.ErrUnavailable) {
		t.Fatalf("期望 rans.ErrUnavailable 族（ErrBusy 别名同判），得到 %v", err)
	}
	close(release)
	// 在途投递不受影响：slow Call 不因 Fence 报错（其结果被丢弃在 goroutine 里，无断言面）
	time.Sleep(20 * time.Millisecond)
}

// 非本地 id 的 Fence 是 no-op（不 panic、不影响他实体）。
func TestFence_UnknownIDIsNoop(t *testing.T) {
	m := New(inmem.New(), 1, map[string]rans.Factory{"k": func(_ rans.ID) rans.Entity { return nopTestEntity{} }})
	m.Fence(mustTestID(t, "k", "absent"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := m.InvokeLocal(ctx, mustTestID(t, "k", "other"), "ping"); err != nil {
		t.Fatalf("无关实体不应受影响：%v", err)
	}
}
