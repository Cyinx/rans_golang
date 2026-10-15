//go:build mutationcheck

package contract_test

// 变异证明（票 18）：故意违约的实现在本标签下被套件抓红——证明套件有
// 效。手动运行：go test -tags mutationcheck ./checkpoint/contract/
// 期望 FAIL 且失败点与违约项对应；结果记入票面 Comments。

import (
	"context"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/checkpoint/contract"
)

// rejectEqualStore 相等纪元拒绝（违约：契约要求相等覆写——同 incarnation
// 重试是迁移落快照的常态路径，拒它会打断重试）。
type rejectEqualStore struct{ inner checkpoint.Store }

func (s *rejectEqualStore) Get(ctx context.Context, id rans.ID) ([]byte, bool, error) {
	return s.inner.Get(ctx, id)
}

func (s *rejectEqualStore) Put(ctx context.Context, id rans.ID, inc uint64, b []byte) error {
	if hr, ok := s.inner.(checkpoint.HighWaterReader); ok {
		if hw, has, _ := hr.HighWater(ctx, id); has && hw == inc {
			return checkpoint.ErrStaleIncarnation
		}
	}
	return s.inner.Put(ctx, id, inc, b)
}

func (s *rejectEqualStore) Delete(ctx context.Context, id rans.ID) error {
	return s.inner.Delete(ctx, id)
}

func TestMutation_EqualRejectedMustFail(t *testing.T) {
	contract.RunStoreSuite(t, func(*testing.T) checkpoint.Store {
		return &rejectEqualStore{inner: checkpoint.New()}
	})
}
