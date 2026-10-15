package node

// 构造期 per-kind 键校验：KindOptions 与 Watchdog.KindOverrides 的键必须
// 属于 Kinds——拼错的 kind 静默忽略等于阈值白配（故障现场无从发现）。

import (
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

func TestNewValidation_UnknownKindKeysRejected(t *testing.T) {
	base := func() Config {
		return Config{
			NodeID:     1,
			Standalone: true,
			Kinds: map[string]rans.Factory{
				"ok": func(rans.ID) rans.Entity { return &whereEntity{} },
			},
		}
	}
	cfg := base()
	cfg.KindOptions = map[string]KindOptions{
		"okp": {Idle: &IdlePolicy{After: time.Minute}},
	}
	if _, err := New(cfg); err == nil {
		t.Fatal("KindOptions with unknown kind key must be rejected by New")
	}

	cfg = base()
	cfg.Watchdog.KindOverrides = map[string]WatchdogParams{
		"okp": {UnloadTimeout: time.Minute},
	}
	if _, err := New(cfg); err == nil {
		t.Fatal("Watchdog.KindOverrides with unknown kind key must be rejected by New")
	}

	cfg = base()
	cfg.KindOptions = map[string]KindOptions{
		"ok": {Idle: &IdlePolicy{After: time.Minute}},
	}
	cfg.Watchdog.KindOverrides = map[string]WatchdogParams{
		"ok": {UnloadTimeout: time.Minute},
	}
	if _, err := New(cfg); err != nil {
		t.Fatalf("known kind keys must pass validation: %v", err)
	}
}
