package testkit

// A2 运行时下限（R2 移植，不依赖 Redis）：目录键丢失后纪元从 1 重来而
// Store 高水位更高——激活取号必须永不低于持久层已接受的高水位，否则复活
// 实体的写被 Store 围栏静默拒绝（已确认写丢失）。

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// shardClearDir 目录键丢失模拟：per-id 分片内层目录，clear(id) 丢弃该分片
// （纪元记忆随条目蒸发——等价 redis 键逐出；墓碑/注销不清序列的分片语义
// 与真实目录一致）。四个 id 域方法按分片路由，其余透传共享底座（嵌入
// 接口不提升 NodeRegistry——基础面无成员键语义不变）。
type shardClearDir struct {
	directory.Directory
	mu     sync.Mutex
	shards map[rans.ID]directory.Directory
}

func newShardClearDir() *shardClearDir {
	return &shardClearDir{Directory: inmem.New(), shards: make(map[rans.ID]directory.Directory)}
}

func (d *shardClearDir) shard(id rans.ID) directory.Directory {
	d.mu.Lock()
	defer d.mu.Unlock()
	if s, ok := d.shards[id]; ok {
		return s
	}
	s := inmem.New()
	d.shards[id] = s
	return s
}

func (d *shardClearDir) clear(id rans.ID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.shards, id)
}

func (d *shardClearDir) Register(ctx context.Context, id rans.ID, self directory.Owner, expected *directory.Owner) (bool, directory.Owner, error) {
	return d.shard(id).Register(ctx, id, self, expected)
}

func (d *shardClearDir) Lookup(ctx context.Context, id rans.ID) (directory.Owner, bool, error) {
	return d.shard(id).Lookup(ctx, id)
}

func (d *shardClearDir) UnregisterIfOwner(ctx context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	return d.shard(id).UnregisterIfOwner(ctx, id, owner)
}

func (d *shardClearDir) Transfer(ctx context.Context, id rans.ID, expected directory.Owner, target directory.Owner) (bool, uint64, directory.Owner, error) {
	return d.shard(id).Transfer(ctx, id, expected, target)
}

// floorProbeEntity 有状态计数实体：Handle 记录投递层携带的纪元（下限
// 断言面）。
type floorProbeEntity struct {
	n   int
	inc atomic.Uint64
}

func (e *floorProbeEntity) Activate(context.Context) error   { return nil }
func (e *floorProbeEntity) Deactivate(context.Context) error { return nil }
func (e *floorProbeEntity) Handle(ctx context.Context, _ any) (any, error) {
	if v, ok := rans.Incarnation(ctx); ok {
		e.inc.Store(v)
	}
	e.n++
	return e.n, nil
}
func (e *floorProbeEntity) Snapshot() ([]byte, error) { return []byte(strconv.Itoa(e.n)), nil }
func (e *floorProbeEntity) Restore(b []byte) error {
	n, err := strconv.Atoi(string(b))
	if err != nil {
		return err
	}
	e.n = n
	return nil
}

// TestFault_DirectoryLossIncarnationStaysAboveStore 三代激活/销毁抬高
// Store 高水位后目录条目蒸发（模拟键丢失）：重激活的纪元必须高于高水位，
// 且销毁终快照被接受、再激活计数连续（不再静默丢写）。
func TestFault_DirectoryLossIncarnationStaysAboveStore(t *testing.T) {
	store := checkpoint.New()
	dir := newShardClearDir()
	probe := &floorProbeEntity{}
	c := NewCluster(t, 1, map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return probe },
	}, WithCheckpointStore(store), WithDirectory(dir))
	ctx := context.Background()
	id := mustID(t, "counter", "dir-loss-floor")

	for gen := 1; gen <= 3; gen++ {
		if v, err := c.Node(0).Call(ctx, id, 1); err != nil || v != gen {
			t.Fatalf("gen %d call: v=%v err=%v", gen, v, err)
		}
		dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
		if err := c.Node(0).Destroy(dctx, id); err != nil {
			t.Fatalf("gen %d destroy: %v", gen, err)
		}
		dcancel()
	}
	hw, _, err := store.HighWater(ctx, id)
	if err != nil || hw < 3 {
		t.Fatalf("precondition: store high water = %d (err=%v), want >= 3", hw, err)
	}

	dir.clear(id) // 实体键丢失：目录纪元记忆蒸发
	probe.inc.Store(0)
	v, err := c.Node(0).Call(ctx, id, 1)
	if err != nil {
		t.Fatalf("re-activation call: %v", err)
	}
	if inc := probe.inc.Load(); inc <= hw {
		t.Fatalf("re-activated incarnation %d must exceed store high water %d (writes would be silently fenced)", inc, hw)
	}
	if v != 4 {
		t.Fatalf("counter must continue from the persisted state: v=%v want 4", v)
	}
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	defer dcancel()
	if err := c.Node(0).Destroy(dctx, id); err != nil {
		t.Fatalf("post-loss destroy: %v", err)
	}
	hw2, _, err := store.HighWater(ctx, id)
	if err != nil || hw2 <= hw {
		t.Fatalf("final snapshot must be accepted above the old high water: hw %d -> %d (err=%v)", hw, hw2, err)
	}
	// 再激活计数保留（键丢失前的已确认写不丢）。
	if v, err := c.Node(0).Call(ctx, id, 1); err != nil || v != 5 {
		t.Fatalf("post-final activation: v=%v err=%v want 5", v, err)
	}
}
