package membership

// 票 01 包内契约锚（grill 定稿语义）：严格/提示判定、draining 口径、
// 刷新失败保留旧快照、超龄保守存活、即时失效（合并窗）、首查同步刷新。

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
)

type fakeSource struct {
	mu    sync.Mutex
	calls atomic.Int64
	snap  map[rans.NodeID]directory.NodeInfo
	err   error
}

func (f *fakeSource) get(context.Context) (map[rans.NodeID]directory.NodeInfo, error) {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snap, f.err
}

func nodeset(kv ...any) map[rans.NodeID]directory.NodeInfo {
	m := map[rans.NodeID]directory.NodeInfo{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i].(rans.NodeID)] = kv[i+1].(directory.NodeInfo)
	}
	return m
}

func TestLocalView_AliveAndPresent(t *testing.T) {
	src := &fakeSource{snap: nodeset(
		rans.NodeID(1), directory.NodeInfo{Addr: "a:1", Epoch: 7, Kinds: []string{"k"}},
		rans.NodeID(2), directory.NodeInfo{Addr: "a:2", Epoch: 3, Draining: true},
	)}
	v := NewLocalView(src.get, time.Minute, 50*time.Millisecond)
	defer v.Close()

	if !v.Alive(1, 7) {
		t.Fatal("alive node with matching epoch must be alive")
	}
	if v.Alive(1, 8) {
		t.Fatal("epoch mismatch must be dead")
	}
	if !v.Alive(1, 0) {
		t.Fatal("epoch-0 query (hint entries) must not filter")
	}
	if v.Alive(9, 0) {
		t.Fatal("absent node in a valid view must be dead")
	}
	if !v.Present(2) {
		t.Fatal("present ignores epoch and draining")
	}
	if v.Present(9) {
		t.Fatal("absent node must not be present")
	}
	if a, ok := v.Addr(2); !ok || a != "a:2" {
		t.Fatal("addr must not filter draining nodes")
	}
	if _, ok := v.Addr(9); ok {
		t.Fatal("absent node must miss addr")
	}
}

func TestLocalView_CandidatesFilter(t *testing.T) {
	src := &fakeSource{snap: nodeset(
		rans.NodeID(1), directory.NodeInfo{Kinds: []string{"a"}},
		rans.NodeID(2), directory.NodeInfo{Kinds: []string{"a", "b"}, Draining: true},
		rans.NodeID(3), directory.NodeInfo{Kinds: nil},
		rans.NodeID(4), directory.NodeInfo{Kinds: []string{"b"}},
	)}
	v := NewLocalView(src.get, time.Minute, time.Minute)
	defer v.Close()
	got := v.Candidates("b")
	if len(got) != 1 || got[0] != 4 {
		t.Fatalf("candidates(b) = %v, want [4]（draining 排除、kind 过滤、纯心跳不进）", got)
	}
	if c := v.Candidates("zz"); c != nil {
		t.Fatalf("candidates(unknown) = %v, want nil", c)
	}
}

func TestLocalView_RefreshFailureKeepsSnapshot(t *testing.T) {
	src := &fakeSource{snap: nodeset(
		rans.NodeID(1), directory.NodeInfo{Epoch: 1},
	)}
	var failed atomic.Int64
	v := NewLocalView(src.get, time.Minute, 30*time.Millisecond)
	defer v.Close()
	v.BindMetrics(func() { failed.Add(1) }, nil)
	if !v.Alive(1, 1) {
		t.Fatal("precondition: node alive")
	}
	src.mu.Lock()
	src.err = errors.New("redis down")
	src.snap = nil
	src.mu.Unlock()
	time.Sleep(100 * time.Millisecond) // 至少一次失败刷新
	if !v.Alive(1, 1) {
		t.Fatal("failed refresh must keep the last snapshot")
	}
	if failed.Load() == 0 {
		t.Fatal("refresh failure must be counted")
	}
}

func TestLocalView_OverAgeConservative(t *testing.T) {
	src := &fakeSource{snap: nodeset(
		rans.NodeID(1), directory.NodeInfo{Epoch: 1},
	)}
	v := NewLocalView(src.get, 40*time.Millisecond, time.Minute) // TTL 40ms、刷新 100ms——快照必然超龄
	defer v.Close()
	if !v.Alive(1, 1) {
		t.Fatal("precondition")
	}
	time.Sleep(60 * time.Millisecond)
	if !v.Alive(1, 1) {
		t.Fatal("over-age view must answer conservative-alive")
	}
	if !v.Alive(9, 5) {
		t.Fatal("over-age view must answer conservative-alive for absent nodes too")
	}
	if c := v.Candidates("k"); c != nil {
		t.Fatal("over-age view yields no candidates (placement falls back)")
	}
}

func TestLocalView_InvalidateTriggersRefresh(t *testing.T) {
	src := &fakeSource{snap: nodeset(
		rans.NodeID(1), directory.NodeInfo{Epoch: 1},
	)}
	var inval atomic.Int64
	v := NewLocalView(src.get, time.Minute, time.Minute) // 周期 100ms——失效是唯一刷新源
	defer v.Close()
	v.BindMetrics(nil, func() { inval.Add(1) })
	before := src.calls.Load()
	v.Invalidate()
	time.Sleep(400 * time.Millisecond)
	if src.calls.Load() <= before {
		t.Fatal("invalidate must trigger a refresh")
	}
	if inval.Load() == 0 {
		t.Fatal("invalidate must be counted")
	}
	n := src.calls.Load()
	for i := 0; i < 5; i++ { // 合并窗内的重复失效并入一次刷新
		v.Invalidate()
	}
	time.Sleep(50 * time.Millisecond)
	if got := src.calls.Load(); got > n+1 {
		t.Fatalf("coalesce window breached: %d refreshes for 5 invalidations (want <=1)", got-n)
	}
}

func TestLocalView_FirstQuerySyncRefresh(t *testing.T) {
	src := &fakeSource{snap: nodeset(
		rans.NodeID(1), directory.NodeInfo{Epoch: 1},
	)}
	v := NewLocalView(src.get, time.Minute, time.Minute) // 周期 100ms——首查不等周期
	defer v.Close()
	if !v.Alive(1, 1) {
		t.Fatal("first query must synchronously refresh (no empty-view window)")
	}
	if src.calls.Load() == 0 {
		t.Fatal("first query must have hit the source")
	}
}
