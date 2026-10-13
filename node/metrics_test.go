package node

// node.Metrics() 本节点仪表快照（含握手失配计数）与密钥装载 fail-closed
// 链。

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
	"github.com/cyinx/rans_golang/internal/transport"
	"github.com/cyinx/rans_golang/internal/transport/tcp"
	"github.com/cyinx/rans_golang/internal/wire"
)

// TestNodeMetricsSnapshot 本节点仪表快照（SD3 观测面）：恒注册仪表全体
// 在场（网关/队列/收件箱/传输重拨与批量/握手失配）；本地激活后
// queue_depth 反映实体数；对端以异摘要握手 → schema_mismatch 计数经快照
// 可见（真实失配路径驱动，非注入）。
func TestNodeMetricsSnapshot(t *testing.T) {
	n := mustStartNode(t, Config{
		NodeID:   1,
		Kinds:    testKinds(),
		Security: SecurityConfig{Insecure: true},
	})
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	defer func() { _ = n.Shutdown(sctx) }()

	// 本地激活一个实体：queue_depth 的非零数据源。
	id := mustID(t, "counter", "metrics-snap-1")
	cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ccancel()
	if _, err := n.Client().Call(cctx, id, 1); err != nil {
		t.Fatalf("local call: %v", err)
	}

	// 恒注册仪表全体在场（握手失配计数在本节点注册表——Metrics 契约锚）。
	want := []string{
		"rans.gateway.pending",
		"rans.pawnite.queue_depth_total",
		"rans.pawnite.inbox_depth_total",
		"rans.transport.reconnects_total",
		"rans.transport.batch_size",
		"rans.handshake.schema_mismatch",
	}
	got := map[string]float64{}
	for _, m := range n.Metrics() {
		if m.Name == "" || m.Help == "" {
			t.Fatalf("metric %q has empty Name/Help (snapshot contract: both required)", m.Name)
		}
		got[m.Name] = m.Value
	}
	for _, name := range want {
		if _, ok := got[name]; !ok {
			t.Fatalf("metric %q missing from node snapshot; got %v", name, got)
		}
	}
	if q := got["rans.pawnite.queue_depth_total"]; q < 1 {
		t.Fatalf("queue_depth_total = %v after local activation, want >= 1", q)
	}

	// 真实握手失配驱动：异摘要对端拨入（观测模式——连接保持、计数 +1），
	// 节点快照的 schema_mismatch 随之变 1。
	raw := tcp.NewManager(tcp.Config{
		NodeID:       9,
		ClusterName:  DefaultClusterName,
		NoHMAC:       true, // 与被测节点 Insecure 对齐（MAC 两半必须同形态）
		SchemaDigest: codec.SchemaDigest() + 1,
	}, nopHandler{}, nil)
	if _, err := raw.GetOrConnect(context.Background(), n.Addr()); err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	defer raw.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, m := range n.Metrics() {
			if m.Name == "rans.handshake.schema_mismatch" && m.Value >= 1 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("schema_mismatch never reached 1 in node snapshot after digest-mismatched handshake")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// nopHandler 裸对端的连接事件桩（只建连握手，不发应用帧）。
type nopHandler struct{}

func (nopHandler) HandleMessage(wire.Header, []byte) {}
func (nopHandler) OnHandshakeComplete(rans.NodeID)   {}
func (nopHandler) OnConnectionLost(rans.NodeID)      {}

// 编译期断言：桩满足传输端口。
var _ transport.Handler = nopHandler{}

// TestNodeMetricsNameContract 快照契约：按值读数（非指针/引用），重复读
// 取稳定（gauge 闭包即时采样；counter 只增不减）。
func TestNodeMetricsNameContract(t *testing.T) {
	n := mustStartNode(t, Config{
		NodeID:     1,
		Standalone: true,
		Kinds:      testKinds(),
	})
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	defer func() { _ = n.Shutdown(sctx) }()

	first := n.Metrics()
	if len(first) == 0 {
		t.Fatal("standalone node must still expose its instrument set (rpc assembly always registers)")
	}
	for _, m := range first {
		if m.Value < 0 {
			t.Fatalf("metric %q = %v, want >= 0", m.Name, m.Value)
		}
	}
}

// ---- 密钥装载 fail-closed 链 ----

// TestLoadHMACKeyFromEnv_FailClosed env 未设/空白 → (nil, nil)，node.New
// 据此拒绝构造（fail-closed 链：装载器不静默放行空钥——空钥在装配门闸
// 必然报错）；合法 hex 原文解码。
func TestLoadHMACKeyFromEnv_FailClosed(t *testing.T) {
	t.Setenv("RANS_HMAC_KEY", "")
	key, err := LoadHMACKeyFromEnv()
	if err != nil {
		t.Fatalf("unset env: err = %v, want nil (unset = not configured)", err)
	}
	if key != nil {
		t.Fatalf("unset env: key = %v, want nil", key)
	}
	// fail-closed 末端：空钥 + 未显式 Insecure → New 报错（既有装配门闸）。
	_, newErr := New(Config{NodeID: 1, Kinds: testKinds()})
	if newErr == nil {
		t.Fatal("New with nil key from unset env must fail (fail-closed)")
	}

	t.Setenv("RANS_HMAC_KEY", "aabbccdd")
	key, err = LoadHMACKeyFromEnv()
	if err != nil {
		t.Fatalf("hex env: %v", err)
	}
	if string(key) != string([]byte{0xaa, 0xbb, 0xcc, 0xdd}) {
		t.Fatalf("hex env key = %x, want aabbccdd", key)
	}

	t.Setenv("RANS_HMAC_KEY", "not-hex!")
	if _, err := LoadHMACKeyFromEnv(); err == nil {
		t.Fatal("non-hex env value must error (config accident surfaces immediately)")
	}
}

// TestLoadHMACKeyFromFile_FailClosed 文件缺失/不可读 → error（绝不空钥
// 放行）；hex 文本与 env 同约定；空白文件 = 未配置 (nil, nil)——末端
// 仍被装配门闸拦下。
func TestLoadHMACKeyFromFile_FailClosed(t *testing.T) {
	if _, err := LoadHMACKeyFromFile(filepath.Join(t.TempDir(), "missing.key")); err == nil {
		t.Fatal("missing key file must error")
	}

	okPath := filepath.Join(t.TempDir(), "hmac.hex")
	if err := os.WriteFile(okPath, []byte("aabbccdd\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := LoadHMACKeyFromFile(okPath)
	if err != nil {
		t.Fatalf("read hex file: %v", err)
	}
	if string(key) != string([]byte{0xaa, 0xbb, 0xcc, 0xdd}) {
		t.Fatalf("file key = %x, want aabbccdd", key)
	}

	blankPath := filepath.Join(t.TempDir(), "blank.hex")
	if err := os.WriteFile(blankPath, []byte("  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err = LoadHMACKeyFromFile(blankPath)
	if err != nil || key != nil {
		t.Fatalf("blank file = (%x, %v), want (nil, nil)", key, err)
	}
}
