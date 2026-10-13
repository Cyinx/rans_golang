package node

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
)

// node 面回归：安全 fail-closed 与管理操作本地语义。装配/生命周期全族
// 回归见 lifecycle_test.go 等同包测试。

func testKinds() map[string]rans.Factory {
	return map[string]rans.Factory{
		"counter": func(_ rans.ID) rans.Entity { return &testCounter{} },
	}
}

// TestNodeNew_RequiresHMACOrInsecure 安全 fail-closed（约定 spec §5.4；
// 决策出处：docs/decisions/2026-09-30-phase6-security.md）：非 Standalone
// 且 Security.HMACKey 空且未显式声明 Insecure → New 报错；Standalone 无
// 传输不涉此要求；Insecure=true / 配置 HMACKey 均放行。
func TestNodeNew_RequiresHMACOrInsecure(t *testing.T) {
	plain := Config{NodeID: 1, ClusterName: "sec", Kinds: testKinds()}
	if _, err := New(plain); err == nil {
		t.Fatal("non-standalone node without HMACKey and without Insecure must fail to construct")
	}
	standalone := plain
	standalone.Standalone = true
	if _, err := New(standalone); err != nil {
		t.Fatalf("standalone node must not require HMACKey: %v", err)
	}
	insecure := plain
	insecure.Security = SecurityConfig{Insecure: true}
	if _, err := New(insecure); err != nil {
		t.Fatalf("Insecure opt-in must pass: %v", err)
	}
	withKey := plain
	withKey.Security = SecurityConfig{HMACKey: []byte("secret")}
	if _, err := New(withKey); err != nil {
		t.Fatalf("configured HMACKey must pass: %v", err)
	}
}

// TestNodeDestroy_NotLocal 管理操作只作用于本地实例（约定 spec §5.5）：
// 目标不在本地 → NotFound + ReasonNotLocal，不再静默成功。
func TestNodeDestroy_NotLocal(t *testing.T) {
	n, err := New(Config{
		NodeID: 1, ClusterName: "mgmt", Kinds: testKinds(),
		Security: SecurityConfig{Insecure: true},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = n.Shutdown(ctx)
	}()
	id, err := rans.NewID("counter", "never-activated-anywhere")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = n.Destroy(ctx, id)
	if rans.CodeOf(err) != rans.CodeNotFound || rans.ReasonOf(err) != rans.ReasonNotLocal {
		t.Fatalf("Destroy(not local) = %v, want CodeNotFound+ReasonNotLocal", err)
	}
}

// TestNodeMigrate_NotLocal 迁移同样只作用于本地实例：目标不在本地 →
// NotFound + ReasonNotLocal。
func TestNodeMigrate_NotLocal(t *testing.T) {
	n, err := New(Config{
		NodeID: 1, ClusterName: "mgmt", Kinds: testKinds(),
		Security: SecurityConfig{Insecure: true},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = n.Shutdown(ctx)
	}()
	id, err := rans.NewID("counter", "never-activated-anywhere")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = n.Migrate(ctx, id)
	if rans.CodeOf(err) != rans.CodeNotFound || rans.ReasonOf(err) != rans.ReasonNotLocal {
		t.Fatalf("Migrate(not local) = %v, want CodeNotFound+ReasonNotLocal", err)
	}
}
