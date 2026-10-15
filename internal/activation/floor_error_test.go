package activation

// 下限读取报错 → 激活失败：调用面得到可重试的 ErrActivationFailed
//（Unavailable+ActivationFailed），本地不留残留条目；根因以 %w 保留在
// 激活闭包错误链（异步窗不跨投递面——既定契约）。

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// errHighWaterStore 高水位读面恒报错（实体可快照才触发读取）。
type errHighWaterStore struct{ checkpoint.Store }

func (s *errHighWaterStore) HighWater(context.Context, rans.ID) (uint64, bool, error) {
	return 0, false, errors.New("high-water boom")
}

// TestActivation_FloorErrorFailsActivation 激活期高水位读取报错：激活
// 永不成功，调用面沿 ErrActivationFailed 链回绝（可重试），失败激活不
// 残留本地条目。
func TestActivation_FloorErrorFailsActivation(t *testing.T) {
	m := New(inmem.New(), 1, map[string]rans.Factory{
		"carrier": func(_ rans.ID) rans.Entity { return &acctEntity{bal: 1} },
	}, Options{Store: &errHighWaterStore{Store: checkpoint.New()}})
	id, _ := rans.NewID("carrier", "floor-err")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := m.InvokeLocal(ctx, id, "req")
	if err == nil {
		t.Fatal("activation must fail on high-water read error")
	}
	if !errors.Is(err, ErrActivationFailed) || !errors.Is(err, rans.ErrUnavailable) {
		t.Fatalf("err = %v, want retryable ErrActivationFailed face (Unavailable+ActivationFailed)", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := m.GetLocal(id); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("failed activation must leave no local entry")
		}
		time.Sleep(time.Millisecond)
	}
}
