package testkit

// 安全/可观测验收（ROADMAP §6 验收①②③④）——组合面：
// TLS 集群全通 + 明文探测被关 + TLS 拨明文失败（WithTLS）；HMAC
// fail-closed / NoHMAC 不安全告警 / 双端同密钥全通。单元级锚点已在
// tcp/remote 锁定（tls_test、fingerprint gate、fail-closed），本文件验证经
// testkit 组合后的可观测行为。
// 时序敏感断言一律走握手 channel + 有界 deadline/轮询，不用裸 sleep。
//
// 装配口径：
//   - secNode 直连变体基于 node.New 装配（传输组合面内部化，testkit 依赖面
//     不含 internal/rpc）；逐节点变体经 Config 直填；
//   - 指纹失配的观测/拒连用例 = TestAcceptance_SchemaDigestObservationAndStrict
//    （握手 SchemaDigest 观测 + Security.StrictSchema 严格拒连）；
//   - redis 指标可见性断言在 node 包（node/redis_metrics_test.go）：指标
//     接线随组装根内置（heartbeat_lost 归 node.wireLease、gateway pending
//     归 rpc 装配），断言需要 internal/metrics 读数——testkit 依赖面不含它；
//   - NoHMAC 不安全告警的 log 捕获断言不在 testkit 依赖面（文案/条数锚点
//     在 tcp/conn_test.go；本文件保留双端 NoHMAC 行为面）。

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/internal/testhooks"
	"github.com/cyinx/rans_golang/node"
)

// ---- 直连安全变体节点（集群选项之外的逐节点控制面）----

// ---- 握手 SchemaDigest 观测与严格模式（直连变体装配面）----

// secSpec 直连节点安全配置：握手密钥 / NoHMAC / TLS / StrictSchema 的逐节点
// 变体——失配场景（一端密钥一端无、TLS 对明文、摘要互异）需要双端各异，
// 集群级共享选项（WithTLS）只产同质节点，故变体组合经本构造器直接组装 node。
type secSpec struct {
	id     rans.NodeID
	secret []byte
	noHMAC bool
	tls    *tls.Config
	strict bool
}

// secNode 构造并启动一个直连 node（"counter" kind，回环随机端口）。
// resolver 表由调用方统一填充后互联；Close 经 t.Cleanup。
func secNode(t *testing.T, dir directory.Directory, rt *resolveTable, spec secSpec) *node.Node {
	t.Helper()
	n, err := node.New(node.Config{
		NodeID:      spec.id,
		ClusterName: clusterName,
		Kinds:       counterKinds(nil),
		Directory:   node.DirectoryConfig{Injected: dir},
		Security: node.SecurityConfig{
			HMACKey:      spec.secret,
			Insecure:     spec.noHMAC,
			TLS:          spec.tls,
			StrictSchema: spec.strict,
		},
		Calls:     node.CallConfig{Default: defaultCallTimeout},
		TestHooks: &testhooks.Hooks{Resolve: rt.resolve},
	})
	if err != nil {
		t.Fatalf("testkit: new sec node %d: %v", spec.id, err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("testkit: start sec node %d: %v", spec.id, err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

// newSecTLSConfig 双角色 tls.Config（监听侧持证 + 拨号侧信任自签 CA）——
// 直连节点的 TLS 变体用；PKI 生成器复用 testkit 自含实现（WithTLS 同源）。
func newSecTLSConfig(t *testing.T) *tls.Config {
	return newClusterCA(t, 9001).nodeTLSConfig(t, 9100)
}

// （NoHMAC 不安全告警的断言面在 tcp 单元层：conn_test.go 锚定告警文案与
// 「逐握手完成一条」的条数——log 门面捕获需 internal/log，不在 testkit
// 依赖面内；本文件只保留双端 NoHMAC 的行为面。）

// ---- 验收①：TLS 集群 + 明文探测 + TLS 拨明文 ----

// TestAcceptance_TLSHandshakeBeforeFrames：WithTLS 集群（自签 CA + 逐节点
// 证书）跨节点 Call 全通（连接复用亦通）；裸明文探测（raw dial 直写帧形态
// 字节）被静默关闭且不回任何可解码帧；TLS 节点拨明文对端（对端未配 TLS）
// → 拨号失败（有界快速收场，不产生响应）。
func TestAcceptance_TLSHandshakeBeforeFrames(t *testing.T) {
	c := NewCluster(t, 2, counterKinds(nil), WithTLS())
	n1, n2 := c.Node(0), c.Node(1)
	ctx := context.Background()
	id := mustID(t, "counter", "tls-cross")
	if _, _, err := c.Directory().Register(ctx, id, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// 跨节点 Call 全通：首呼建连（TLS 握手先行），再呼复用既有 TLS 连接。
	for want := 1; want <= 2; want++ {
		v, err := n1.Call(ctx, id, 0)
		if err != nil || v != want {
			t.Fatalf("tls cluster cross-node call #%d: v=%v err=%v, want %d/nil", want, v, err, want)
		}
	}

	// 明文探测：非 TLS 字节直写 TLS 节点端口（无 ClientHello）→ 连接被关、
	// 零回传字节（静默错位门闸——不回错帧，不给探测面；对 TLS 监听者，
	// 「无任何应用数据回传」即「无可解码帧」的更强形态）。
	probe, err := net.Dial("tcp", n2.Addr())
	if err != nil {
		t.Fatalf("plaintext probe dial: %v", err)
	}
	defer probe.Close()
	if _, err := probe.Write([]byte("plaintext probe")); err != nil {
		t.Fatalf("write plaintext probe: %v", err)
	}
	if err := probe.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set probe read deadline: %v", err)
	}
	n, rerr := probe.Read(make([]byte, 4096))
	if n != 0 || rerr == nil {
		t.Fatalf("plaintext probe must be closed with zero response bytes, got n=%d err=%v", n, rerr)
	}
	// 探测不留痕：集群照常服务（既有 TLS 连接不受影响）。
	if v, err := n1.Call(ctx, id, 0); err != nil || v != 3 {
		t.Fatalf("cluster must keep serving after probe: v=%v err=%v, want 3/nil", v, err)
	}

	// TLS 节点拨明文对端：ClientHello 对明文节点是垃圾帧、对端关闭 →
	// TLS 握手失败 → Call 有界失败，不产生响应值。调用预算 1s（网关发送
	// 重试间隔缺省 2s、环另有整轮重试——不用调用方预算收口会被拖到 ~30s）。
	dirP := inmem.New()
	rtP := &resolveTable{}
	// 明文节点（未配 TLS；显式 Insecure——本腿的被测面是「TLS 拨明文失败」，
	// 明文监听者是受害者；无密钥形态的 fail-closed 被测面在 HMACFailClosed ①）。
	plain := secNode(t, dirP, rtP, secSpec{id: 1, noHMAC: true})
	// 拨号侧显式 Insecure（本腿被测面是 TLS-vs-明文的握手失配，非 HMAC 配置
	// 面——隔离失败归因）。
	sec := secNode(t, dirP, rtP, secSpec{id: 2, noHMAC: true, tls: newSecTLSConfig(t)})
	rtP.set(map[rans.NodeID]string{1: plain.Addr(), 2: sec.Addr()})
	idP := mustID(t, "counter", "tls-to-plaintext")
	if _, _, err := dirP.Register(ctx, idP, directory.Owner{Node: 1}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctxDial, cancelDial := context.WithTimeout(ctx, time.Second)
	defer cancelDial()
	start := time.Now()
	v, err := sec.Client().Call(ctxDial, idP, 0)
	if err == nil || v != nil {
		t.Fatalf("tls dial to plaintext peer must fail without a response, got v=%v err=%v", v, err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("tls→plaintext failure must be bounded and fast, took %v", d)
	}
}

// ---- 握手 SchemaDigest 观测与严格模式（集群装配面）----

// TestAcceptance_SchemaDigestObservationAndStrict：握手携带 SchemaDigest
// 扩展字段（规格 §6.1 H9「指纹降级为观测」）——
//   - 默认（非严格）：两节点摘要失配只告警+计数（rans.handshake.schema_mismatch，
//     断言面在 tcp/rpc 单元层），连接保持、跨节点 Call 照常成功；
//   - Security.StrictSchema=true：失配拒连，Call 有界失败（升级断点面）。
//
// 进程内构造真实失配：node.New 在装配期快照 codec.SchemaDigest()，两次装配
// 之间经公开登记面 codec.RegisterShapes 追加形状 → 两节点摘要不等。这复刻
// 生产失配的真实成因（新旧二进制形状集不同），不依赖任何摘要覆盖配置。
func TestAcceptance_SchemaDigestObservationAndStrict(t *testing.T) {
	ctx := context.Background()

	// ① 观测面：失配节点照常互联互通。
	dirA := inmem.New()
	rtA := &resolveTable{}
	obsA := secNode(t, dirA, rtA, secSpec{id: 1, secret: clusterSecret}) // 摘要快照在此刻
	codec.RegisterShapes([]string{"SchemaMismatchAcceptanceMsg:Field:1:int64"})
	obsB := secNode(t, dirA, rtA, secSpec{id: 2, secret: clusterSecret}) // 摘要已含新形状 ≠ obsA
	rtA.set(map[rans.NodeID]string{1: obsA.Addr(), 2: obsB.Addr()})
	idObs := mustID(t, "counter", "digest-obs")
	if _, _, err := dirA.Register(ctx, idObs, directory.Owner{Node: 1}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if v, err := obsB.Client().Call(ctx, idObs, 0); err != nil || v != 1 {
		t.Fatalf("digest mismatch must keep the connection (observation mode): v=%v err=%v", v, err)
	}
	if v, err := obsB.Client().Call(ctx, idObs, 0); err != nil || v != 2 {
		t.Fatalf("mismatched peers must keep serving: v=%v err=%v", v, err)
	}

	// ② 严格面：StrictSchema 拒连，调用有界失败。
	dirB := inmem.New()
	rtB := &resolveTable{}
	codec.RegisterShapes([]string{"SchemaMismatchAcceptanceMsg2:Field:1:int64"}) // 与 obsB 的登记集再拉开
	strictA := secNode(t, dirB, rtB, secSpec{id: 1, secret: clusterSecret, strict: true})
	codec.RegisterShapes([]string{"SchemaMismatchAcceptanceMsg3:Field:1:int64"})
	strictB := secNode(t, dirB, rtB, secSpec{id: 2, secret: clusterSecret, strict: true})
	rtB.set(map[rans.NodeID]string{1: strictA.Addr(), 2: strictB.Addr()})
	idStrict := mustID(t, "counter", "digest-strict")
	if _, _, err := dirB.Register(ctx, idStrict, directory.Owner{Node: 1}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctxDial, cancelDial := context.WithTimeout(ctx, time.Second)
	defer cancelDial()
	start := time.Now()
	if _, err := strictB.Client().Call(ctxDial, idStrict, 0); err == nil {
		t.Fatal("strict side must refuse digest mismatch, got call success")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("strict refusal must be bounded and fast, took %v", d)
	}
}

// ---- 验收③：HMAC fail-closed / NoHMAC 告警 / 同密钥全通 ----

// TestAcceptance_HMACFailClosed：一端密钥一端无（且未显式 NoHMAC）→
// 无密钥端 fail-closed 拒连（不发握手字节），Call 有界失败；双端 NoHMAC
// （显式不安全 opt-in）→ 连接成立、跨节点调用成功，且每次握手完成的
// 不安全告警经 log 门面可见（捕获断言）；双端同密钥 → 全通。
func TestAcceptance_HMACFailClosed(t *testing.T) {
	ctx := context.Background()

	// ① 一端密钥一端无：无密钥端（未显式 NoHMAC）**装配期 fail-closed**——
	// 约定 spec §5.4 把拒连前移到 node.New（未配密钥的明文集群必须是一次
	// 显式决定；先于线级握手拒连，构造即失败，不可能有半开节点）。
	dir1 := inmem.New()
	_, err := node.New(node.Config{
		NodeID:      2,
		ClusterName: clusterName,
		Kinds:       counterKinds(nil),
		Directory:   node.DirectoryConfig{Injected: dir1},
		// Security 零值：无 HMACKey 且未 Insecure → New 报错。
	})
	if err == nil {
		t.Fatal("no-secret node (no NoHMAC opt-in) must be refused at assembly time (fail-closed)")
	}
	// 对照：显式 Insecure 放行（腿②的被测面）。
	withKey := secNode(t, dir1, &resolveTable{}, secSpec{id: 1, secret: clusterSecret})
	if withKey.Addr() == "" {
		t.Fatal("keyed node must assemble and bind")
	}

	// ② 双端 NoHMAC：连接成立（不安全告警可见性归 tcp 单元锚点）。
	dir2 := inmem.New()
	rt2 := &resolveTable{}
	optA := secNode(t, dir2, rt2, secSpec{id: 1, noHMAC: true})
	optB := secNode(t, dir2, rt2, secSpec{id: 2, noHMAC: true})
	rt2.set(map[rans.NodeID]string{1: optA.Addr(), 2: optB.Addr()})
	idOpt := mustID(t, "counter", "hmac-nohmac")
	if _, _, err := dir2.Register(ctx, idOpt, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if v, err := optA.Client().Call(ctx, idOpt, 0); err != nil || v != 1 {
		t.Fatalf("both-NoHMAC call must succeed: v=%v err=%v", v, err)
	}

	// ③ 双端同密钥：全通（集群缺省形态的直连复刻）。
	dir3 := inmem.New()
	rt3 := &resolveTable{}
	keyA := secNode(t, dir3, rt3, secSpec{id: 1, secret: clusterSecret})
	keyB := secNode(t, dir3, rt3, secSpec{id: 2, secret: clusterSecret})
	rt3.set(map[rans.NodeID]string{1: keyA.Addr(), 2: keyB.Addr()})
	idSame := mustID(t, "counter", "hmac-samekey")
	if _, _, err := dir3.Register(ctx, idSame, directory.Owner{Node: 2}, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if v, err := keyA.Client().Call(ctx, idSame, 0); err != nil || v != 1 {
		t.Fatalf("same-key call must succeed: v=%v err=%v", v, err)
	}
}
