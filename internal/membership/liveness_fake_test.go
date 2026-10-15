package membership

// Liveness 测试基座：成员注册表 fake（成员面 + 最小目录面）与驱动助手。
// 测试本体在 liveness_test.go / liveness_selffence_test.go。

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

func mustID(t *testing.T, kind, key string) rans.ID {
	t.Helper()
	id, err := rans.NewID(kind, key)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// fakeNodeRegistry 在 fakeDirectory（最小目录面）之上叠加节点成员面：
// registerCalls 记 (node, info) 序列，errReg 非 nil 时 RegisterNode 恒错；
// nextEpoch 进程内单调取号（NextNodeEpoch），NodeEpoch 返回节点最近一次
// 注册携带的代际。
type fakeNodeRegistry struct {
	*fakeDirectory
	mu            sync.Mutex
	registerCalls []struct {
		node rans.NodeID
		info directory.NodeInfo
	}
	errReg        error
	errEpoch      error
	nextEpoch     uint64
	nodes         map[rans.NodeID]uint64 // 成员键代际（fake 判活面）
	dropped       map[rans.NodeID]bool   // 判活键删除注入
	sawSuperseded bool                   // RenewNode 出现过 Superseded
	renewCalls    []struct {
		node rans.NodeID
		info directory.NodeInfo
	} // RenewNode 调用序列
	epochFails uint64
	okTimes    []time.Time
	waitSeen   int
}

func newFakeNodeRegistry(entries map[rans.ID]directory.Owner) *fakeNodeRegistry {
	return &fakeNodeRegistry{
		fakeDirectory: newFakeDirectory(entries),
		nodes:         make(map[rans.NodeID]uint64),
		dropped:       make(map[rans.NodeID]bool),
	}
}

// fakeDirectory 最小目录面（v3 语义从简：entries 即真相，无墓簿区分——
// Liveness/自围栏用例不消费目录行为，只须满足 NodeRegistry 的嵌入基面）。
type fakeDirectory struct {
	mu      sync.Mutex
	entries map[rans.ID]directory.Owner
}

func newFakeDirectory(entries map[rans.ID]directory.Owner) *fakeDirectory {
	if entries == nil {
		entries = make(map[rans.ID]directory.Owner)
	}
	return &fakeDirectory{entries: entries}
}

func (f *fakeDirectory) Register(_ context.Context, id rans.ID, self directory.Owner, _ *directory.Owner) (bool, directory.Owner, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := f.entries[id]; ok && cur.Node != 0 && !(cur.Node == self.Node && (cur.Epoch == self.Epoch || cur.Epoch == 0)) {
		return false, cur, nil
	}
	f.entries[id] = self
	return true, self, nil
}

func (f *fakeDirectory) UnregisterIfOwner(_ context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := f.entries[id]; ok && cur == owner {
		delete(f.entries, id)
		return true, nil
	}
	return false, nil
}

func (f *fakeDirectory) Lookup(_ context.Context, id rans.ID) (directory.Owner, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.entries[id]
	return o, ok, nil
}

func (f *fakeDirectory) Transfer(_ context.Context, id rans.ID, expected, target directory.Owner) (bool, uint64, directory.Owner, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, ok := f.entries[id]
	if !ok {
		return false, 0, directory.Owner{}, nil
	}
	if cur != expected {
		return false, 0, cur, directory.ErrNotOwner
	}
	next := cur.Incarnation + 1
	won := directory.Owner{Node: target.Node, Epoch: target.Epoch, Incarnation: next}
	f.entries[id] = won
	return true, next, won, nil
}

func (f *fakeNodeRegistry) MemberTTL() time.Duration { return 0 }

func (f *fakeNodeRegistry) UnregisterNode(_ context.Context, _ rans.NodeID, _ uint64) error {
	return nil
}

// RenewNode 比对续期（directory.NodeRegistry 契约的最小 fake）：记录在
// 且代际不大于调用方 → Renewed 并记录；记录在且更大 → Superseded；
// 记录缺席（含 dropKey 注入）→ Absent。
func (f *fakeNodeRegistry) RenewNode(_ context.Context, node rans.NodeID, info directory.NodeInfo) (directory.RenewResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewCalls = append(f.renewCalls, struct {
		node rans.NodeID
		info directory.NodeInfo
	}{node, info})
	if err := f.errReg; err != nil {
		return 0, err
	}
	if f.dropped[node] {
		return directory.RenewAbsent, nil
	}
	if cur, ok := f.nodes[node]; ok && cur > info.Epoch {
		f.sawSuperseded = true
		return directory.RenewSuperseded, nil
	}
	f.nodes[node] = info.Epoch
	f.okTimes = append(f.okTimes, time.Now())
	return directory.RenewRenewed, nil
}

// waitRenew 等待下一次续期调用（有界）。
func (f *fakeNodeRegistry) waitRenew(t *testing.T) {
	t.Helper()
	base := len(f.renewals())
	deadline := time.After(2 * time.Second)
	for len(f.renewals()) <= base {
		select {
		case <-deadline:
			t.Fatal("liveness loop never renewed within 2s")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// Nodes fake 成员视图（判活面即 nodes 记录）。
func (f *fakeNodeRegistry) Nodes(_ context.Context) (map[rans.NodeID]directory.NodeInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[rans.NodeID]directory.NodeInfo, len(f.nodes))
	for n, ep := range f.nodes {
		if f.dropped[n] {
			continue
		}
		out[n] = directory.NodeInfo{Addr: "10.0.0.1:1", Epoch: ep}
	}
	return out, nil
}

// renewals 续期调用序列快照。
func (f *fakeNodeRegistry) renewals() []struct {
	node rans.NodeID
	info directory.NodeInfo
} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]struct {
		node rans.NodeID
		info directory.NodeInfo
	}(nil), f.renewCalls...)
}

// dropKey 注入「成员键被删」（票 11 复活形态的确定性注入面）。
func (f *fakeNodeRegistry) dropKey(node rans.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropped[node] = true
	delete(f.nodes, node)
}

// supersedeKey 注入「成员键被更大代际占据」（票 11 让位形态）。
func (f *fakeNodeRegistry) supersedeKey(node rans.NodeID, epoch uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropped[node] = false
	f.nodes[node] = epoch
}

func (f *fakeNodeRegistry) NextNodeEpoch(_ context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errEpoch != nil {
		f.epochFails++
		return 0, f.errEpoch
	}
	f.nextEpoch++
	return f.nextEpoch, nil
}

// NodeEpoch 返回节点最近一次注册携带的代际（fake 的存活语义从简：注册过
// 即存活）。
func (f *fakeNodeRegistry) NodeEpoch(_ context.Context, node rans.NodeID) (uint64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.registerCalls) - 1; i >= 0; i-- {
		if f.registerCalls[i].node == node {
			return f.registerCalls[i].info.Epoch, true, nil
		}
	}
	return 0, false, nil
}

func (f *fakeNodeRegistry) RegisterNode(_ context.Context, node rans.NodeID, info directory.NodeInfo) error {
	f.mu.Lock()
	f.registerCalls = append(f.registerCalls, struct {
		node rans.NodeID
		info directory.NodeInfo
	}{node, info})
	err := f.errReg
	if err == nil {
		f.okTimes = append(f.okTimes, time.Now())
		f.nodes[node] = info.Epoch
		f.dropped[node] = false
	}
	f.mu.Unlock()
	return err
}

// failWrites / healWrites 错误注入的同步写面（errReg 在 RegisterNode 的
// 锁内读取——外部裸写是数据竞态）。
func (f *fakeNodeRegistry) failWrites(err error) {
	f.mu.Lock()
	f.errReg = err
	f.mu.Unlock()
}

func (f *fakeNodeRegistry) healWrites() {
	f.mu.Lock()
	f.errReg = nil
	f.mu.Unlock()
}

// failEpoch / healEpoch 取号错误注入（errEpoch 在 NextNodeEpoch 的锁内
// 读取，与 failWrites 同纪律）；epochFailCount 返回取号失败次数快照
// （取号失败形态下 RegisterNode 永不被调用，写计数不能当节拍证据）。
func (f *fakeNodeRegistry) failEpoch(err error) {
	f.mu.Lock()
	f.errEpoch = err
	f.mu.Unlock()
}

func (f *fakeNodeRegistry) healEpoch() {
	f.mu.Lock()
	f.errEpoch = nil
	f.mu.Unlock()
}

func (f *fakeNodeRegistry) epochFailCount() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.epochFails
}

// waitCalled 阻塞至下一次 RegisterNode 调用发生（含失败调用——写失败
// 也算一轮 beat 的证据）。
func (f *fakeNodeRegistry) waitCalled(t *testing.T) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		f.mu.Lock()
		n := len(f.registerCalls)
		f.mu.Unlock()
		if n > f.waitSeen {
			f.waitSeen = n
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for RegisterNode call")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// callCount 返回 RegisterNode 调用计数。
func (f *fakeNodeRegistry) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.registerCalls)
}

// okTimestamps 返回成功写的时间序列（自围栏阻塞窗新鲜度断言的证据面）。
func (f *fakeNodeRegistry) okTimestamps() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.okTimes...)
}

func (f *fakeNodeRegistry) NodeAddr(context.Context, rans.NodeID) (string, bool, error) {
	return "", false, nil
}

func (f *fakeNodeRegistry) NodeIDs(context.Context) ([]rans.NodeID, error) {
	return nil, nil
}

func (f *fakeNodeRegistry) NodeAddrs(context.Context) (map[rans.NodeID]string, error) {
	return map[rans.NodeID]string{}, nil
}

func (f *fakeNodeRegistry) NodesByKind(context.Context, string) ([]rans.NodeID, error) {
	return nil, nil
}

// nextEpochCount 返回取号次数快照（NextNodeEpoch 恰一次语义断言用）。
func (f *fakeNodeRegistry) nextEpochCount() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nextEpoch
}

func (f *fakeNodeRegistry) registrations() []struct {
	node rans.NodeID
	info directory.NodeInfo
} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]struct {
		node rans.NodeID
		info directory.NodeInfo
	}(nil), f.registerCalls...)
}

var _ directory.NodeRegistry = (*fakeNodeRegistry)(nil)

// newLiveness 测试便捷构造（Reg 非 nil 恒成功）。
func newLiveness(t *testing.T, reg directory.NodeRegistry, self rans.NodeID, interval time.Duration) *Liveness {
	t.Helper()
	l, err := NewLiveness(LivenessConfig{Reg: reg, Self: self, Interval: interval})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// startLiveness 组合驱动：node 装配的 go Run 等价形态（续租面已随 H13
// 拆除，Liveness 是唯一存活循环）。
func startLiveness(t *testing.T, l *Liveness) (cancel context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	if err := l.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(cancel)
	return cancel
}
