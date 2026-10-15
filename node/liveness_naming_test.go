package node

// 票 23 红测（B9 余项）：租约时代命名收口——node.Config.Liveness/
// LivenessConfig（旧名 Lease/LeaseConfig 删除）与新指标名
// rans.membership.self_fenced 的编译期锚。修复前编译红（旧名不存在侧
// 承载）。

import (
	"testing"
	"time"
)

// TestLivenessConfigNaming 编译期锚：新配置面在场（编译失败=红——修复前
// Liveness/LivenessConfig 不存在）。
func TestLivenessConfigNaming(t *testing.T) {
	cfg := Config{
		NodeID:   1,
		Kinds:    ttlKinds(),
		Liveness: LivenessConfig{Interval: 50 * time.Millisecond},
	}
	if cfg.Liveness.Interval != 50*time.Millisecond {
		t.Fatalf("Liveness.Interval = %v", cfg.Liveness.Interval)
	}
}
