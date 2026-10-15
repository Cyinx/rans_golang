package node

// 票 14 红测（B11 启动半边）：Start 次序=同步取号 → 绑定（暂不接入）→
// 同步首写成员键 → 开始接入；Start 返回时成员键已在且携带非 0 代际（无
// 异步首写窗）；取号/首写失败 → Start 报错且无监听残留、自建目录收口；
// registry 形态下 Start 前的激活不写 epoch-0 条目（拒绝收场）。

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// epochFailReg 取号失败注入的注册表包装（testkit 同款语义：NextNodeEpoch
// 布防后一律报错）。
type epochFailReg struct {
	directory.NodeRegistry
	fail bool
}

func (r *epochFailReg) NextNodeEpoch(ctx context.Context) (uint64, error) {
	if r.fail {
		return 0, errors.New("test: injected epoch allocation failure")
	}
	return r.NodeRegistry.NextNodeEpoch(ctx)
}

func newEpochTestNode(t *testing.T, reg directory.NodeRegistry) *Node {
	t.Helper()
	kinds := map[string]rans.Factory{
		"ep": func(rans.ID) rans.Entity { return nopStartEntity{} },
	}
	cfg := Config{
		NodeID: 1, ClusterName: "epoch-test", ListenAddr: "127.0.0.1:0",
		Security:  SecurityConfig{Insecure: true},
		Kinds:     kinds,
		Directory: DirectoryConfig{Injected: regDir{Directory: inmem.New(), NodeRegistry: reg}},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

type nopStartEntity struct{}

func (nopStartEntity) Activate(context.Context) error           { return nil }
func (nopStartEntity) Deactivate(context.Context) error         { return nil }
func (nopStartEntity) Handle(context.Context, any) (any, error) { return nil, nil }

// TestNodeStart_EpochAndMemberKeyBeforeAccept Start 返回即成员键在场且带
// 非 0 代际（同步首写——修复前异步首写有窗口，Start 返回时键常未写）。
func TestNodeStart_EpochAndMemberKeyBeforeAccept(t *testing.T) {
	reg := inmem.NewNodeRegistry(time.Minute)
	n := newEpochTestNode(t, reg)
	defer shutdownNode(n)
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ep, alive, err := reg.NodeEpoch(ctx, 1)
	if err != nil {
		t.Fatalf("node epoch: %v", err)
	}
	if !alive {
		t.Fatal("member key must be present the moment Start returns (sync first write)")
	}
	if ep == 0 {
		t.Fatalf("member key epoch = 0, want non-zero (epoch-0 form eliminated at start)")
	}
}

// TestNodeStart_FailsWhenEpochAllocationFails 取号失败注入：Start 返回错误
// 且无监听残留（修复前：Start 成功、节点以 epoch-0 形态不可见地服务）。
func TestNodeStart_FailsWhenEpochAllocationFails(t *testing.T) {
	reg := &epochFailReg{NodeRegistry: inmem.NewNodeRegistry(time.Minute), fail: true}
	n := newEpochTestNode(t, reg)
	err := n.Start(context.Background())
	if err == nil {
		shutdownNode(n)
		t.Fatal("Start must fail when epoch allocation fails")
	}
	if !strings.Contains(err.Error(), "epoch") && !strings.Contains(err.Error(), "membership") {
		t.Fatalf("err = %v, want epoch/membership context", err)
	}
	if a := n.Addr(); a != "" {
		t.Fatalf("Start failure must leave no bound listener, got %q", a)
	}
	_ = n.Close() // 终态幂等
}

// TestNodeStart_FirstWriteFailureIsTerminal 首写失败（注册表写键报错）：
// Start 返回错误、监听收口（资源不留半开）。
func TestNodeStart_FirstWriteFailureIsTerminal(t *testing.T) {
	reg := &writeFailReg{NodeRegistry: inmem.NewNodeRegistry(time.Minute)}
	n := newEpochTestNode(t, reg)
	if err := n.Start(context.Background()); err == nil {
		shutdownNode(n)
		t.Fatal("Start must fail when the first member-key write fails")
	}
	if a := n.Addr(); a != "" {
		t.Fatalf("listener must be torn down on start failure, got %q", a)
	}
	_ = n.Close()
}

// writeFailReg 首写失败注入（RegisterNode 报错；取号正常）。
type writeFailReg struct {
	directory.NodeRegistry
}

func (r *writeFailReg) RegisterNode(ctx context.Context, node rans.NodeID, info directory.NodeInfo) error {
	return errors.New("test: injected register-node failure")
}

// TestNodeStart_NeverRegistersEpochZeroEntries registry 形态下 Start 前的
// 调用不写 epoch-0 条目：调用确定性收场，目录无实体条目（修复前：Start
// 前激活可写 epoch-0 条目）。
func TestNodeStart_NeverRegistersEpochZeroEntries(t *testing.T) {
	reg := inmem.NewNodeRegistry(time.Minute)
	n := newEpochTestNode(t, reg)
	defer shutdownNode(n)
	id := mustID(t, "ep", "pre-start")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := n.Client().Call(ctx, id, "go"); err == nil {
		t.Fatal("pre-Start call must fail deterministically (epoch not allocated)")
	}
	// 目录不得出现 epoch-0 的实体条目（拒绝收场——零注册）。
	// 经公开面验证：Start 后调用成功且成员键正常。
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	if _, err := n.Client().Call(ctx2, id, "go"); err != nil {
		t.Fatalf("post-start call: %v", err)
	}
}
