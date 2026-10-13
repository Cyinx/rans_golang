// 节点观测面：Node 自有指标注册表的公开快照。本文件是 node 包唯一定义
// 仪表读数契约的位置。
package node

// Metric 节点仪表快照的单条读数。
type Metric struct {
	// Name 指标名（`rans.*` 常量族，清单见 README「安全与观测」指标表）。
	Name string
	// Help 口径说明（注册时登记的仪表描述，首次为准）。
	Help string
	// Value 当前读数：Gauge 为采样时刻的观察值，Counter 为进程内累计值
	//（只增不减）。
	Value float64
}

// Metrics 返回本节点仪表快照（按名排序，稳定输出；无 SDK 环境即可用）。
//
// 契约：
//   - 覆盖面 = 本节点装配期在其自有注册表上注册的全部仪表：网关在途
//     （rans.gateway.pending）、本地实体数（rans.pawnite.queue_depth_total）、
//     收件箱积压（rans.pawnite.inbox_depth_total）、传输重拨
//     （rans.transport.reconnects_total）、最近写批量
//     （rans.transport.batch_size）、握手摘要失配
//     （rans.handshake.schema_mismatch）、出队过期即弃
//     （rans.call.expired）、自检止损轮数（rans.lease.self_fenced，
//     目录实现 NodeRegistry 的成员面形态才注册——逐节点事实随本快照
//     逐节点观测）；
//   - 快照是即时读数：Gauge 闭包在调用时刻采样，重复调用读数随运行状态
//     演进；不阻塞（全部闭包为廉价内存读）；
//   - 进程级聚合的仪表不在本快照：rans.activation.failed /
//     rans.checkpoint.final_failed 两项 Counter 由 internal 包直接注册在
//     进程级缺省注册表（metrics.Snapshot() 同源可见）；
//   - 并发安全；可在任意时刻调用（Start 前/后皆可——装配期注册的仪表
//     在 New 返回后即有读数）。
func (n *Node) Metrics() []Metric {
	readings := n.mreg.Readings()
	out := make([]Metric, len(readings))
	for i, r := range readings {
		out[i] = Metric{Name: r.Name, Help: r.Help, Value: r.Value}
	}
	return out
}
