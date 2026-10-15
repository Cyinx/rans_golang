package activation

// 换代复核：注册 IO 在飞期间 Liveness 换代（成员键丢失重取号）——条目以
// 旧代际落位即失主，激活失败（可重试），失败不残留本地条目。

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// epochFlipDir 注册成功即翻转 SelfEpoch 读数（模拟注册 IO 在飞期间 Liveness
// 换代），其余透传 inmem。
type epochFlipDir struct {
	directory.Directory
}

func (d *epochFlipDir) Register(ctx context.Context, id rans.ID, self directory.Owner, expected *directory.Owner) (bool, directory.Owner, error) {
	won, cur, err := d.Directory.Register(ctx, id, self, expected)
	if won {
		flipSelfEpoch()
	}
	return won, cur, err
}

var flipSelfEpoch = func() {}

func TestActivation_EpochMovedDuringRegisterFails(t *testing.T) {
	dir := &epochFlipDir{Directory: inmem.New()}
	epoch := uint64(11)
	flipSelfEpoch = func() { epoch = 12 } // 注册成功瞬间换代：复核必不相等
	m := New(dir, 1, map[string]rans.Factory{
		"cell": func(_ rans.ID) rans.Entity { return &counterEntity{} },
	})
	m.BindSelfEpoch(func() uint64 { return epoch })
	id, _ := rans.NewID("cell", "epoch-moved")
	ctx := context.Background()
	p, err := m.InvokeLocal(ctx, id, "x")
	if err == nil {
		_ = p
		t.Fatal("activation must fail when the node epoch moved during registration")
	}
	if !errors.Is(err, ErrActivationFailed) {
		t.Fatalf("err = %v, want retryable ErrActivationFailed face", err)
	}
	// 完成信号以条目注销为准（GetLocal 的 Destroyed 逐出可早于 cleanup
	// 注销——完成序：终局 → 注销 → 逐出，逐出可由读路径先行触发）。
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, found, _ := dir.Lookup(ctx, id); !found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("entry registered under the stale epoch must be unregistered by terminal cleanup")
		}
		time.Sleep(time.Millisecond)
	}
	if _, ok := m.GetLocal(id); ok {
		t.Fatal("failed activation must leave no local entry")
	}
}
