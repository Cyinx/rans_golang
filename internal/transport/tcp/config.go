// HMAC 密钥配置面（规格 §10.2）：fail-closed 门闸
// 判定。密钥装载助手收敛在 node 面（node.LoadHMACKeyFromEnv/File——
// 公开装配入口是 node.New，装载语义与其 fail-closed 门闸同文档）；
// 密钥材料不进 git，决策出处见 docs/decisions/2026-09-30-phase6-security.md。
package tcp

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"time"

	"github.com/cyinx/rans_golang"
)

// HMACKeyEnv 握手 HMAC 密钥的环境变量名（值为 hex 文本；装载助手在
// node.LoadHMACKeyFromEnv——常量留此供门闸错误文本引用，node 面引用同名）。
const HMACKeyEnv = "RANS_HMAC_KEY"

// errHMACRequired fail-closed 门闸错误：未配密钥且未显式 NoHMAC——本地
// 配置事故，Start 即拒连（错误文本含 "HMAC required" 与处置指引）。
var errHMACRequired = errors.New("rans: connect: HMAC required: no secret configured (Config.Secret nil, " +
	HMACKeyEnv + " unset); set a secret (node.LoadHMACKeyFromEnv/LoadHMACKeyFromFile) or explicitly set Config.NoHMAC")

// failClosedHMAC 报告配置是否触发 fail-closed 门闸：未配密钥（Secret nil）
// 且未显式 NoHMAC → true。Secret 非 nil 时旗标不参与（MAC 恒启用）。
func (c Config) failClosedHMAC() bool {
	return c.Secret == nil && !c.NoHMAC
}

// Config 连接配置；零值经 normalize 补默认（SendQueueDepth 1024、
// HandshakeTimeout 5s）。Secret 为握手 HMAC 密钥：非 nil 即 MAC 强制启用
// （NoHMAC 旗标不参与——它只辖 nil 形态）。Secret 为 nil 表示未配置密钥：
// 默认 fail-closed（规格 §10.2——Manager 路径在
// 拨号/接入入口即拒，TLS 握手前零字节；裸 NewConn 用户 Start 即拒连，不发
// 握手字节，错误含 "HMAC required"）；显式 NoHMAC=true 才以不安全形态运行
// （握手照常、每次握手完成告警一次）。密钥装载助手在 node 面
// （node.LoadHMACKeyFromEnv/File；约定正本：docs/decisions/2026-09-30-phase6-security.md）。
// TLS 非 nil 时启用 TLS（规格 §10.1/陷阱 13）：Manager 监听侧以
// tls.NewListener 包装、拨号侧在 NewConn 前包 tls.Client 并显式握手；
// Start 对 TLS 连接在起读写 goroutine 前显式完成握手。nil = 明文，
// 现状行为字节级不变。
type Config struct {
	NodeID      rans.NodeID
	ClusterName string
	Secret      []byte
	NoHMAC      bool // 显式不安全 opt-in（仅 Secret nil 时有意义）
	// SchemaDigest 本端消息形状摘要（规格 §6.1 H9：指纹降级为
	// 观测——由 rpc 装配层以 codec.SchemaDigest() 注入，tcp 不感知登记面）。
	// 失配默认只告警+计数（OnSchemaMismatch），连接保持；StrictSchema=true
	// 时失配拒连（ErrHandshakeSchema——此时摘要受握手 MAC 保护，失配不可
	// 伪造）。kind 集自 v3 起不参与握手（异构 kind 由 kind 感知放置承担）。
	SchemaDigest uint64
	// StrictSchema 严格摘要门闸：true 时对端 SchemaDigest 不等即拒连
	// （ErrHandshakeSchema）；false（默认）为观测模式——失配告警+计数、
	// 连接保持（形状维从此只用于观测与严格模式，不决定默认握手成败）。
	StrictSchema bool
	// OnSchemaMismatch 摘要失配观测回调（每条失配连接的握手完成各一次）：
	// 计数器接线的注入面——tcp 不 import metrics，由 rpc 装配层接计数闭包。
	// nil = 未接线（裸 NewConn 用户）。
	OnSchemaMismatch func()
	// OnReconnect 重拨观察钩子（rans.transport.reconnects_total 数据源，
	// 构造期注入）：同地址的**非首次**
	// 拨号发起时调用一次（判据：该地址曾成功建连——hadConn 集，断连/踢除
	// 后的重拨即命中；首拨与懒建缓存命中不计数）。锁外执行，必须非阻塞。
	// nil = 未接线。构造期定死，无运行期 Set 面。
	OnReconnect      func()
	SendQueueDepth   int
	HandshakeTimeout time.Duration
	TLS              *tls.Config
	// Dial 可选：替换标准拨号（测试注入面——分区模拟/慢拨号；testkit 经
	// node.TestHooks 传入，构造期定死，无运行期 Set 面）。nil = 标准拨号。
	Dial func(ctx context.Context, addr string) (net.Conn, error)
	// WrapConn 可选：包装一切 Accept/Dial 出的裸连接（测试注入面——帧层
	// 之下的故障注入如延迟）。TLS 握手先于包装（见 startConn——
	// 包装连接嵌 net.Conn 接口，握手探测对其失效）；包装层只见 TLS 记录流
	// 或明文帧流。nil = 直通。
	WrapConn func(net.Conn) net.Conn
}

// normalize 补全零值默认（开发阶段无老配置包袱，不保留半缺省形态）。
func (c Config) normalize() Config {
	if c.SendQueueDepth <= 0 {
		c.SendQueueDepth = defaultSendQueueDepth
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = defaultHandshakeTimeout
	}
	return c
}
