// Package contract 提供 checkpoint.Store 的可执行契约套件（票 18，B8）：
// 实现者把新 Store 注入 RunStoreSuite 即得全量行为断言——散文契约
// （store.go 接口注释）的机器形态。宿主围栏（自持存储经
// rans.Incarnation 自行比较）的一页式契约见 README「宿主围栏契约」节。
package contract

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
)

// NewStore 构造被测 Store（每个子测试独立实例——隔离性由构造方保证）。
type NewStore func(t *testing.T) checkpoint.Store

// RunStoreSuite 跑全部契约子测试。用法：
//
//	func TestStoreContract(t *testing.T) {
//		contract.RunStoreSuite(t, func(t *testing.T) checkpoint.Store {
//			return mystore.New()
//		})
//	}
//
// 高水位语义（HighWaterReader）经类型断言探测：实现则断言，未实现跳过
// （可选能力）。ctx 取消行为为可选断言（纯内存实现不阻塞——尊重但不可
// 强测），经 RunStoreSuiteStrict 开启。
func RunStoreSuite(t *testing.T, newStore NewStore) {
	runSuite(t, newStore, false)
}

// RunStoreSuiteStrict 同 RunStoreSuite，附加「已取消 ctx 返回错误」的可
// 选断言（阻塞型持久化实现应开启；纯内存实现免）。
func RunStoreSuiteStrict(t *testing.T, newStore NewStore) {
	runSuite(t, newStore, true)
}

func runSuite(t *testing.T, newStore NewStore, strictCtx bool) {
	t.Run("GetMiss", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		id := mustID(t)
		b, found, err := s.Get(ctx, id)
		if err != nil || found || b != nil {
			t.Fatalf("Get(unstored) = (%v, %v, %v), want (nil, false, nil)", b, found, err)
		}
		// Delete 未命中幂等（不返回错误）。
		if err := s.Delete(ctx, id); err != nil {
			t.Fatalf("Delete(unstored) = %v, want nil (idempotent)", err)
		}
	})
	t.Run("RoundTripAndDefensiveCopy", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		id := mustID(t)
		in := []byte("snap-v1")
		if err := s.Put(ctx, id, 7, in); err != nil {
			t.Fatalf("Put: %v", err)
		}
		in[0] = 'X' // 防御性拷贝：实现不得别名持有调用方缓冲
		got, found, err := s.Get(ctx, id)
		if err != nil || !found {
			t.Fatalf("Get = (%v, %v, %v)", got, found, err)
		}
		if !bytes.Equal(got, []byte("snap-v1")) {
			t.Fatalf("Get = %q, want snap-v1 (defensive copy)", got)
		}
		got[0] = 'Y' // 读侧同款：返回缓冲归调用方
		got2, _, _ := s.Get(ctx, id)
		if got2[0] != 's' {
			t.Fatalf("Get aliased caller buffer: %q", got2)
		}
	})
	t.Run("StaleRejectedEqualOverwrites", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		id := mustID(t)
		if err := s.Put(ctx, id, 5, []byte("v5")); err != nil {
			t.Fatalf("Put(5): %v", err)
		}
		if err := s.Put(ctx, id, 3, []byte("v3")); !errors.Is(err, checkpoint.ErrStaleIncarnation) {
			t.Fatalf("Put(3<5) = %v, want ErrStaleIncarnation", err)
		}
		if b, _, _ := s.Get(ctx, id); !bytes.Equal(b, []byte("v5")) {
			t.Fatalf("stale write must not land: %q", b)
		}
		// 相等覆写：同 incarnation 的重试允许覆盖。
		if err := s.Put(ctx, id, 5, []byte("v5b")); err != nil {
			t.Fatalf("Put(5==5) = %v, want nil (retry overwrite)", err)
		}
		if b, _, _ := s.Get(ctx, id); !bytes.Equal(b, []byte("v5b")) {
			t.Fatalf("equal-incarnation overwrite must land: %q", b)
		}
		// 更大纪元正常推进。
		if err := s.Put(ctx, id, 6, []byte("v6")); err != nil {
			t.Fatalf("Put(6>5): %v", err)
		}
	})
	t.Run("ZeroPassThroughNoHighWater", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		id := mustID(t)
		if err := s.Put(ctx, id, 0, []byte("bare")); err != nil {
			t.Fatalf("Put(0): %v", err)
		}
		// 0 直通不抬高水位：随后的小纪元写入不被拒。
		if err := s.Put(ctx, id, 1, []byte("v1")); err != nil {
			t.Fatalf("Put(1) after Put(0) = %v, want nil (0 must not raise watermark)", err)
		}
		// 高水位可选面：实现则 0 直通不计入。
		if hr, ok := s.(checkpoint.HighWaterReader); ok {
			hw, found, err := hr.HighWater(ctx, id)
			if err != nil || !found || hw != 1 {
				t.Fatalf("HighWater = (%d, %v, %v), want (1, true, nil)", hw, found, err)
			}
		}
	})
	t.Run("DeleteKeepsHighWater", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		id := mustID(t)
		if err := s.Put(ctx, id, 9, []byte("v9")); err != nil {
			t.Fatalf("Put(9): %v", err)
		}
		if err := s.Delete(ctx, id); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if b, found, err := s.Get(ctx, id); err != nil || found || b != nil {
			t.Fatalf("Get after Delete = (%v, %v, %v), want miss", b, found, err)
		}
		// Delete 不撤销高水位：旧纪元残留在飞写仍被拒（围栏单调性锚）。
		if err := s.Put(ctx, id, 8, []byte("v8")); !errors.Is(err, checkpoint.ErrStaleIncarnation) {
			t.Fatalf("Put(8<9) after Delete = %v, want ErrStaleIncarnation (watermark survives delete)", err)
		}
		if err := s.Put(ctx, id, 0, []byte("resurrected-bare")); err != nil {
			t.Fatalf("Put(0) after Delete = %v, want nil (0 直通)", err)
		}
		if hr, ok := s.(checkpoint.HighWaterReader); ok {
			hw, found, _ := hr.HighWater(ctx, id)
			if !found || hw != 9 {
				t.Fatalf("HighWater after Delete = (%d, %v), want (9, true) — delete must not revoke watermark", hw, found)
			}
		}
	})
	t.Run("ConcurrentWritersMaxWins", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		id := mustID(t)
		const writers = 8
		const rounds = 40
		var wg sync.WaitGroup
		for w := 1; w <= writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for r := 0; r < rounds; r++ {
					inc := uint64(w*100 + r)
					_ = s.Put(ctx, id, inc, []byte{byte(w)})
				}
			}(w)
		}
		wg.Wait()
		if hr, ok := s.(checkpoint.HighWaterReader); ok {
			hw, found, _ := hr.HighWater(ctx, id)
			if !found || hw != writers*100+rounds-1 {
				t.Fatalf("HighWater after race = (%d, %v), want (%d, true)", hw, found, writers*100+rounds-1)
			}
		}
		// 低纪元写不得覆盖高纪元内容：胜者内容必须是最高纪元的载荷。
		if err := s.Put(ctx, id, 1, []byte("low")); !errors.Is(err, checkpoint.ErrStaleIncarnation) {
			t.Fatalf("low writer after race = %v, want ErrStaleIncarnation", err)
		}
		b, _, _ := s.Get(ctx, id)
		if len(b) != 1 || b[0] != byte(writers) {
			t.Fatalf("winner content = %v, want writer %d's payload", b, writers)
		}
	})
	if strictCtx {
		t.Run("CanceledCtxRejected", func(t *testing.T) {
			s := newStore(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			id := mustID(t)
			if err := s.Put(ctx, id, 1, []byte("v")); err == nil {
				t.Fatal("Put with canceled ctx must return an error (blocking impl)")
			}
		})
	}
}

// mustID 套件内身份证构造。
func mustID(t *testing.T) rans.ID {
	t.Helper()
	id, err := rans.NewID("contractkind", t.Name())
	if err != nil {
		t.Fatal(err)
	}
	return id
}
