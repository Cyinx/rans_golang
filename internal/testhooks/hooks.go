// Package testhooks 提供逐节点的测试注入面（约定 spec §3.2 导入图表的
// testkit 专用行：testkit 只经公开包 + 本包装配测试集群）。类型驻 internal
// ——模块外无法构造生产面之外的语义；生产代码只在 node 装配根读取本结构
// （Config.TestHooks），运行行为与不注入时逐字节一致。
//
// 注入是构造期定死的（node.New 读入后经 rpc.Config 直通 tcp.Config，无
// 运行期 Set 面）；需要运行中变化的语义（Slow 的动态延迟、Partition 的
// 屏蔽表）由钩子闭包自行捕获可变状态实现。
package testhooks

import (
	"context"
	"net"

	"github.com/cyinx/rans_golang"
)

// Hooks 测试注入点（仅 testkit 与进程内多节点测试构造；全部字段 nil =
// 缺省生产行为）。注入经 node.Config.TestHooks 传入，node.New 一次性接线，
// 运行中不可变。
type Hooks struct {
	// Dial 替换标准拨号（分区模拟：屏蔽表命中即报错）。nil = 标准拨号。
	Dial func(ctx context.Context, addr string) (net.Conn, error)
	// WrapConn 包装一切 Accept/Dial 出的连接（延迟注入：包装连接动态读
	// 当前延迟值）。nil = 直通。TLS 握手先于包装（连接建立即完成握手）
	// ——包装层只见记录流，不影响握手。
	WrapConn func(net.Conn) net.Conn
	// Resolve 节点号→地址解析替换（进程内多节点共享地址表——redis 形态
	// 用目录派生解析器，inmem 形态缺省为自解析，多节点必须注入本钩子）。
	Resolve func(ctx context.Context, n rans.NodeID) (string, error)
	// CrashKeepsDir 崩溃模拟开关：node.Close() 不关闭 node 自建的 redis
	// 目录（testkit Kill 语义——目录由测试直接持有/断言，租约自然过期）。
	// 对注入目录无效果（本就归调用方持有）。
	CrashKeepsDir bool
}
