package contract_test

// 票 18：checkpoint.InMemory 跑契约套件。变异证明（故意违约的实现必红）
// 在 mutationcheck 构建标签后文件——手动 `go test -tags mutationcheck
// ./checkpoint/contract/` 跑一次取证后记录票面，常规 make test 不含。

import (
	"testing"

	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/checkpoint/contract"
)

func TestInMemoryStoreContract(t *testing.T) {
	contract.RunStoreSuite(t, func(*testing.T) checkpoint.Store { return checkpoint.New() })
}
