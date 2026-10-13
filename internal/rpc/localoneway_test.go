package rpc

// 本地单向投递的 fencing 面：localOneWay.Deliver 的 Handle ctx 携带执行
// 实例的 incarnation 纪元（与 callDelivery.Deliver / remoteDelivery 同一
// 接线面——zombie 迟回的 Handle 续体直写消费方存储时可凭 ctx 令牌被拒）。

import (
	"context"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/internal/pawnite"
)

// testID 构造测试身份（失败即 Fatal）。
func testID(t *testing.T, kind, inst string) rans.ID {
	t.Helper()
	id, err := rans.NewID(kind, inst)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// funcEntity 把 Handle 函数适配为 Entity（Activate/Deactivate 恒成功）。
type funcEntity func(ctx context.Context, req any) (any, error)

func (f funcEntity) Activate(context.Context) error { return nil }
func (f funcEntity) Handle(ctx context.Context, req any) (any, error) {
	return f(ctx, req)
}
func (f funcEntity) Deactivate(context.Context) error { return nil }

// TestLocalOneWay_HandleCtxCarriesIncarnation 直构 pawnite（Options.
// Incarnation 注入）经 localOneWay 投递，Handle 必须能从 ctx 读到同一纪元。
func TestLocalOneWay_HandleCtxCarriesIncarnation(t *testing.T) {
	var got uint64
	var ok bool
	ent := rans.Entity(funcEntity(func(ctx context.Context, req any) (any, error) {
		got, ok = rans.Incarnation(ctx)
		return nil, nil
	}))
	p := pawnite.New(testID(t, "k", "1"), pawnite.Options{Incarnation: 42})
	(&localOneWay{id: testID(t, "k", "1"), ent: ent, ctx: context.Background(), req: "x"}).Deliver(p)
	if !ok || got != 42 {
		t.Fatalf("期望纪元 42，得到 (%d, %v)", got, ok)
	}
}
