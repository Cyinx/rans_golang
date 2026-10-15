package node

// 票 17 红测（node 公开面）：TombstoneTTL 的构造期校验——无纪元下限来源
// 拒绝构造；配了 HighWaterReader Store 或 FencingFloor 则通过。

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/checkpoint"
	"github.com/cyinx/rans_golang/directory/redis"
)

func ttlKinds() map[string]rans.Factory {
	return map[string]rans.Factory{
		"ttl": func(rans.ID) rans.Entity { return nopStartEntity{} },
	}
}

func TestNodeNew_TombstoneTTLRequiresFloorSource(t *testing.T) {
	base := func(store checkpoint.Store, floor func(context.Context, rans.ID) (uint64, error)) Config {
		return Config{
			NodeID: 1, ListenAddr: "127.0.0.1:0", Kinds: ttlKinds(),
			Security: SecurityConfig{Insecure: true},
			Directory: DirectoryConfig{Redis: &redis.Options{
				Addr: "127.0.0.1:16379", HeartbeatTTL: time.Minute, TombstoneTTL: time.Hour,
			}},
			Store:        store,
			FencingFloor: floor,
		}
	}

	// 无下限来源 → 拒绝。
	if _, err := New(base(nil, nil)); err == nil ||
		!strings.Contains(err.Error(), "epoch-floor source") {
		t.Fatalf("no-floor construct = %v, want epoch-floor source rejection", err)
	}
	// HighWaterReader Store → 通过。
	hwStore := checkpoint.New() // InMemory 实现 HighWaterReader
	if _, err := New(base(hwStore, nil)); err != nil {
		t.Fatalf("HighWaterReader store must satisfy the floor source: %v", err)
	}
	// FencingFloor 钩子 → 通过。
	floor := func(context.Context, rans.ID) (uint64, error) { return 0, nil }
	if _, err := New(base(nil, floor)); err != nil {
		t.Fatalf("FencingFloor must satisfy the floor source: %v", err)
	}
	// 无 TTL → 三个形态全部照旧（缺省不受校验）。
	noTTL := base(nil, nil)
	noTTL.Directory.Redis.TombstoneTTL = 0
	if _, err := New(noTTL); err != nil {
		t.Fatalf("TTL=0 must not require a floor source: %v", err)
	}
}
