package inmem

import (
	"testing"
	"time"

	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/contract"
)

// TestRegisterSuite 目录合并端口契约（键值 v3 基座套件）：Register 四分支
// （收养/重注册/CAS 接管/输返回现归属）、注销写墓碑且纪元跨墓碑单调、
// Transfer 三态、Lookup（inmem 全实跑）。
func TestRegisterSuite(t *testing.T) {
	contract.RunRegisterSuite(t, func(t *testing.T) directory.Directory {
		return New()
	})
}

// TestNodeMembershipSuite 节点成员键契约（inmem registry 同语义：记录在 ⟺
// 节点可见、TTL 惰性过期退出视图、draining 过滤、节点注销即时退出成员
// 视图——进程内注册表的停机路径由过期收敛兜底）。
func TestNodeMembershipSuite(t *testing.T) {
	contract.RunNodeMembershipSuite(t, func(t *testing.T, hbTTL time.Duration) directory.NodeRegistry {
		return NewNodeRegistry(hbTTL)
	})
}

// TestNodeEpoch_MonotonicAcrossRestart 节点代际单调（inmem 同型测试）：
// 进程内计数器取号单调，同节点号「注册 → 停（判活记录过期）→ 再注册」新
// 代际严格更大（redis 真库测试见 directory/redis）。
func TestNodeEpoch_MonotonicAcrossRestart(t *testing.T) {
	contract.RunNodeEpochMonotonicSuite(t, func(t *testing.T, hbTTL time.Duration) directory.NodeRegistry {
		return NewNodeRegistry(hbTTL)
	})
}

// TestMemberView_NodeEpochQuery 成员视图代际查询（inmem 同型测试）：多节
// 点注册后各存活节点的 epoch 可查且各归各，未注册/判活记录过期查不到。
func TestMemberView_NodeEpochQuery(t *testing.T) {
	contract.RunNodeEpochQuerySuite(t, func(t *testing.T, hbTTL time.Duration) directory.NodeRegistry {
		return NewNodeRegistry(hbTTL)
	})
}
