package directory

import (
	"context"
	"errors"
	"testing"

	"github.com/cyinx/rans_golang"
)

// stubRegistry OwnerAlive 判定桩：NodeRegistry 面按用例注入 (epoch, alive,
// err) 三元返回值；Directory 面不被 OwnerAlive 触达（仅接口断言用），内嵌
// 接口即满足。bareDirectory 是无成员面的对照形态（断言不成立 NodeRegistry）。
type (
	stubRegistry struct {
		Directory
		NodeRegistry

		ep    uint64
		alive bool
		err   error
	}

	bareDirectory struct {
		Directory
	}
)

func (s stubRegistry) NodeEpoch(context.Context, rans.NodeID) (uint64, bool, error) {
	return s.ep, s.alive, s.err
}

// TestOwnerAlive_ConservativeOnReadError 判定四分支锚定：注册表读错误
// （超时/连接失败等瞬时故障）不等于死亡——err 无法确认成员键状态，保守
// 判活（不触发 CAS 接管与正缓存失效，真死由发送失败/重试环兜底）；只有
// 明确的「键不在/已过期」（alive=false 且无错）才判死。存活时的代际比对
// 与无成员面的保守语义维持既有行为。
func TestOwnerAlive_ConservativeOnReadError(t *testing.T) {
	ctx := context.Background()
	o := Owner{Node: 7, Epoch: 3}

	t.Run("读错误保守判活", func(t *testing.T) {
		reg := stubRegistry{err: errors.New("injected: registry read failure")}
		if !OwnerAlive(ctx, reg, o) {
			t.Fatal("NodeEpoch 返回 err 时 OwnerAlive=false：瞬时读错误即可判活 owner 为死，触发 CAS 接管与缓存失效")
		}
	})
	t.Run("键真过期判死", func(t *testing.T) {
		reg := stubRegistry{ep: 9, alive: false}
		if OwnerAlive(ctx, reg, o) {
			t.Fatal("alive=false 且无错时 OwnerAlive=true：成员键已明确过期，必须判死放行接管")
		}
	})
	t.Run("存活但代际不符判死", func(t *testing.T) {
		reg := stubRegistry{ep: 9, alive: true}
		if OwnerAlive(ctx, reg, o) {
			t.Fatal("存活且代际不符（9≠3）时 OwnerAlive=true：同节点换代后旧代际归属必须判死")
		}
	})
	t.Run("存活且代际一致判活", func(t *testing.T) {
		reg := stubRegistry{ep: 3, alive: true}
		if !OwnerAlive(ctx, reg, o) {
			t.Fatal("存活且代际一致时 OwnerAlive=false：健康归属被误判")
		}
	})
	t.Run("无成员面保守判活", func(t *testing.T) {
		if !OwnerAlive(ctx, bareDirectory{}, o) {
			t.Fatal("无 NodeRegistry 面时 OwnerAlive=false：不可判死的目录实现必须维持转发语义")
		}
	})
}
