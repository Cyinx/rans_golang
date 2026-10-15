package redis

// 票 17 红测：墓碑可选 TTL——开启后注销的墓碑在 TTL 后消失；有纪元下限
// 来源时重激活的纪元仍越过 Store 高水位（floor 兜住单调性）。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory"
	goredis "github.com/redis/go-redis/v9"
)

// primaryEntityKey 实体主键全名（与 keys.go 的 primaryKey 同口径拼接）。
func primaryEntityKey(id rans.ID) string {
	return "rans:tombttl:" + "rans:e:{" + id.String() + "}"
}

func TestTombstoneTTL_ExpiresAndFloorKeepsMonotonic(t *testing.T) {
	store := checkpoint.New()
	dir := New(Options{
		Addr:         redisAddr(),
		Password:     redisPassword(),
		HeartbeatTTL: time.Minute,
		TombstoneTTL: 150 * time.Millisecond,
		KeyPrefix:    "rans:tombttl:",
	})
	defer dir.Close()
	cli := goredis.NewClient(&goredis.Options{Addr: redisAddr(), Password: redisPassword()})
	defer cli.Close()
	ctx := context.Background()
	id, _ := rans.NewID("counter", "tomb-ttl")

	// 激活 → 注销（终态墓碑，带 PX）→ 快照落 Store（高水位）。
	won, cur, err := dir.Register(ctx, id, directory.Owner{Node: 1, Epoch: 7}, nil)
	if err != nil || !won {
		t.Fatalf("register: won=%v err=%v", won, err)
	}
	if err := store.Put(ctx, id, cur.Incarnation, []byte("snap")); err != nil {
		t.Fatal(err)
	}
	ok, err := dir.UnregisterIfOwner(ctx, id, cur)
	if err != nil || !ok {
		t.Fatalf("unregister: ok=%v err=%v", ok, err)
	}
	if _, found, _ := dir.Lookup(ctx, id); found {
		t.Fatal("precondition: tombstone must read as miss")
	}
	// 键在场前置（防空跑：键名拼错会让过期断言恒真）。
	key := primaryEntityKey(id)
	if exists, err := cli.Exists(ctx, key).Result(); err != nil || exists != 1 {
		t.Fatalf("precondition: tombstone key must exist before TTL: exists=%v err=%v", exists, err)
	}
	// TTL 后墓碑消失（键整体过期）。
	time.Sleep(250 * time.Millisecond)
	if exists, err := cli.Exists(ctx, key).Result(); err != nil || exists != 0 {
		t.Fatalf("tombstone key must expire under TTL: exists=%v err=%v", exists, err)
	}
	// 重激活（floor=Store 高水位）：新纪元 > 高水位——墓碑过期不回退
	// 单调性（floor 由调用方传入，review14 票 08 语义）。
	won2, cur2, err := dir.Register(ctx, id, directory.Owner{Node: 1, Epoch: 7, Incarnation: cur.Incarnation}, nil)
	if err != nil || !won2 {
		t.Fatalf("re-register after expiry: won=%v err=%v", won2, err)
	}
	if cur2.Incarnation <= cur.Incarnation {
		t.Fatalf("incarnation regressed after tombstone expiry: %d → %d (floor must keep monotonic)", cur.Incarnation, cur2.Incarnation)
	}
}
