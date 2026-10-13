// Package metrics 提供 rans 运行时指标面（规格 §16 可观测性）：
// OTel API 仪表 + 手动读数器（manual snapshot）双轨。
//
// 双轨语义：
//   - OTel 轨：经全局 MeterProvider（宿主接 SDK 后导出；未装 SDK 时全局
//     provider 为 noop——仪表注册照常成功、零输出零报错，规格 §16
//     「无 SDK 时零输出不报错」）；
//   - 手动轨：Counter 自带 per-name 累计器、Gauge 持观察闭包，Snapshot
//     即时读数——测试与无 SDK 环境的观测面，不依赖任何导出管线。
//
// 生产代码只依赖 go.opentelemetry.io/otel（API）；sdk/metric 仅测试用。
// 宿主接 SDK 直接调 otel.SetMeterProvider（不设透传面，委派机制见 otel.go）。
//
// 聚合口径：同名 Gauge 允许多次注册（多 Remote 共享 default 注册表——
// 进程内多节点测试形态），观察值取全部观察闭包之和；同名 Counter 幂等
// 返回同一实例。名字跨类型冲突（同名为 Counter 又为 Gauge）属接线 bug：
// 后到者降级手动轨（仍入表；Snapshot 同名键以 gauge 呈现）并 Warn
// 留痕，不 panic。
package metrics

import (
	"context"
	"sync"

	"github.com/cyinx/rans_golang/internal/log"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// DefaultMeterName 缺省 meter 名（规格 §16 Meter "rans"）。
const DefaultMeterName = "rans"

// 指标名常量（规格 §16 + 网关/传输扩展；接线方一律引用常量，
// 不在业务包散落字符串）。
const (
	// NameQueueDepth 本地实体数（ObservableGauge，规格 §16 queue_depth）。
	NameQueueDepth = "rans.pawnite.queue_depth_total"
	// NameInboxDepth 各 pawnite 收件箱积压深度之和（ObservableGauge，
	// 规格 §16 inbox_depth）。
	NameInboxDepth = "rans.pawnite.inbox_depth_total"
	// NameGatewayPending 网关在途请求-应答条目数（ObservableGauge）。
	NameGatewayPending = "rans.gateway.pending"
	// NameTransportReconnects 传输层同地址重拨次数累计（Counter——
	// 每地址首次拨号不计数，断连后重拨每次 +1）。
	NameTransportReconnects = "rans.transport.reconnects_total"
	// NameTransportBatchSize 最近一次写冲刷的帧数（ObservableGauge——
	// writeLoop 队列清空即冲刷，读数即最近批量）。
	NameTransportBatchSize = "rans.transport.batch_size"
	// NameLeaseSelfFenced 本节点自检触发轮数累计（Counter——成员键写连续
	// 失败超 SelfFence 阈值、对全部本地实体执行止损的轮数，每触发一轮
	// +1）。注册在节点自有注册表（node 装配期），经 Node.Metrics() 逐节点
	// 读数——自检是逐节点事实，进程级聚合会抹掉「哪个节点在失联」。
	NameLeaseSelfFenced = "rans.lease.self_fenced"
	// NameActivationFailed 实体激活失败累计（Counter——激活流程以失败
	// 收场、实体从未进入 Active 的次数；reason 属性区分失败面）。
	NameActivationFailed = "rans.activation.failed"
	// NameCheckpointFinalFailed 停机（Retire 交接）终快照在预算内重试后仍
	// 落库失败的累计（Counter——照样终局：停机不能被 Store 故障挂死；丢失
	// 面 = 上一份 checkpoint 之后的增量写，本指标即该丢失的告警源）。
	NameCheckpointFinalFailed = "rans.checkpoint.final_failed"
	// NameHandshakeSchemaMismatch 握手 SchemaDigest 失配累计（Counter，
	// 规格 §6.1 H9「指纹降级为观测」）：每条失配连接的握手完成各 +1——
	// 默认告警+计数、连接保持；StrictSchema 的拒连不计入
	//（拒连走连接关闭日志）。形状漂移的量化读数，滚动升级窗口的观测面。
	NameHandshakeSchemaMismatch = "rans.handshake.schema_mismatch"
	// NameCallExpired 出队过期即弃累计（Counter，H10 决策 7）：出队执行前
	// 预算已耗尽、未执行按 (Timeout, Expired) 回绝的次数（本地/远端同计）。
	NameCallExpired = "rans.call.expired"
	// NameCallCanceled 出队取消弃置累计（Counter，H10 决策 9）：在箱被
	// MsgCancel 标记、出队弃置未执行的次数（与 expired 分列）。
	NameCallCanceled = "rans.call.canceled"
	// NameCallCausalCapped 因果路径深度上限降级直通累计（Counter，H12 决策
	// 14）：出站 Call 的自身哈希追加因达上限被跳过的次数（发送侧告警面）。
	NameCallCausalCapped = "rans.call.causal_capped"
	// NamePropValueDropped 传播键单值护栏丢弃累计（Counter，H10 决策 8）：
	// 编码失败或编码产物超单值上限（4 KiB）的键被丢弃的次数（发送侧计数，
	// 丢弃不使调用失败——位置透明的降级面）。
	NamePropValueDropped = "rans.prop.value_dropped"
	// NamePropTotalDropped 传播键合计护栏丢弃累计（Counter，H10 决策 8）：
	// 单请求合计超上限（32 KiB）触发后被丢弃的键次数（触发键与其余在场
	// 登记键，发送侧计数）。
	NamePropTotalDropped = "rans.prop.total_dropped"
	// NameOutboxDepth 发件箱在箱条目数（ObservableGauge，H11 决策 11）：
	// 全箱共享计数，入箱即计、worker 出箱即减——停序③ flush 的等待面。
	NameOutboxDepth = "rans.outbox.depth"
	// NameOutboxDropped 发件箱静默弃置累计（Counter，H11 决策 11/12）：
	// 入箱后未发出即弃的帧数（关箱弃置等丢失面；票 09 的满箱即丢生产者
	// 接线后同计于此）。收到错误回执的拒绝不计——丢失已告知调用方。
	NameOutboxDropped = "rans.outbox.dropped"
	// NameOutboxSendFailed 发件箱发送失败累计（Counter，H11 决策 11）：
	// worker 发送尝试以错误收场的帧数（重试耗尽/关箱取消等）。
	NameOutboxSendFailed = "rans.outbox.send_failed"
)

// Registry 指标注册表：meter 名 + 仪表登记。并发安全；Counter/Gauge 可在
// 任意时刻注册（注册与采样并发安全），使用前配置仍是推荐纪律（与 hooks
// 同族约定）。进程生命期注册表：无反注册面（仪表读数随其闭包自然归零）。
type Registry struct {
	meterName string

	mu       sync.Mutex
	counters map[string]*Counter
	gauges   map[string]*gaugeSet
	descs    map[string]string // 指标名 → 口径说明（首次注册为准；Readings 用）
}

// gaugeSet 同名 Gauge 的观察闭包集（求和聚合，见包注释）。
type gaugeSet struct {
	observers []func() float64
}

// New 构造注册表。meterName 空串回落 DefaultMeterName（"rans"）。
func New(meterName string) *Registry {
	if meterName == "" {
		meterName = DefaultMeterName
	}
	return &Registry{
		meterName: meterName,
		counters:  make(map[string]*Counter),
		gauges:    make(map[string]*gaugeSet),
		descs:     make(map[string]string),
	}
}

// defaultRegistry 包级缺省注册表（rpc/testkit 接线的落点）。
var defaultRegistry = sync.OnceValue(func() *Registry { return New(DefaultMeterName) })

// Default 返回包级缺省注册表（meter "rans"；幂等，进程内单实例）。
func Default() *Registry { return defaultRegistry() }

// MeterName 返回注册表的 meter 名（OTel scope 名）。
func (r *Registry) MeterName() string { return r.meterName }

// Counter 注册（或幂等取回）计数器：同名二次注册返回既有实例（描述以
// 首次为准）。OTel 轨建仪表、手动轨始终启用（noop 静默语义见包注释）。
func (r *Registry) Counter(name, desc string) *Counter {
	r.mu.Lock()
	if c, ok := r.counters[name]; ok {
		r.mu.Unlock()
		return c
	}
	if _, conflict := r.gauges[name]; conflict {
		// 名字已被 Gauge 占用：接线 bug（同名字跨类型）。降级仍可见——
		// 手动轨入表、OTel 轨放弃（同名异型无法注册），对称契约与
		// Snapshot 呈现规则见包注释。
		log.Warn("rans/metrics: instrument name type conflict, manual-only fallback",
			"name", name, "meter", r.meterName, "registered", "gauge", "requested", "counter")
		c := newCounter(name, nil)
		r.counters[name] = c
		if _, ok := r.descs[name]; !ok {
			r.descs[name] = desc
		}
		r.mu.Unlock()
		return c
	}
	r.mu.Unlock()

	// OTel 轨：锁外建仪表（provider 内部有自身的锁，不与注册表锁嵌套）。
	var inst metric.Float64Counter
	if c, err := currentMeter(r.meterName).Float64Counter(name, metric.WithDescription(desc)); err != nil {
		log.Warn("rans/metrics: otel counter creation failed, manual-only",
			"name", name, "meter", r.meterName, "err", err)
	} else {
		inst = c
	}
	c := newCounter(name, inst)

	r.mu.Lock()
	// 双检：并发注册同名（首个胜出，本实例降级为手动轨孤儿——读数仍正确）。
	if first, ok := r.counters[name]; ok {
		r.mu.Unlock()
		return first
	}
	r.counters[name] = c
	if _, ok := r.descs[name]; !ok {
		r.descs[name] = desc
	}
	r.mu.Unlock()
	return c
}

// Gauge 注册 ObservableGauge：observe 在每次采样（OTel 回调 / Snapshot）
// 时被调用，全部同名观察值求和后作为该 gauge 读数。observe 必须廉价、
// 无 panic、非阻塞（在采样/导出 goroutine 上执行）。同名多次注册为追加
// 观察者（多 Remote 共享注册表的聚合形态，见包注释）。
func (r *Registry) Gauge(name, desc string, observe func() float64) {
	if observe == nil {
		return
	}
	r.mu.Lock()
	if _, conflict := r.counters[name]; conflict {
		log.Warn("rans/metrics: instrument name type conflict, manual-only fallback",
			"name", name, "meter", r.meterName, "registered", "counter", "requested", "gauge")
		// 手动轨照常追加、OTel 轨放弃（对称契约见包注释）。
		if gs, ok := r.gauges[name]; ok {
			gs.observers = append(gs.observers, observe)
		} else {
			r.gauges[name] = &gaugeSet{observers: []func() float64{observe}}
		}
		r.mu.Unlock()
		return
	}
	first := false
	gs, ok := r.gauges[name]
	if !ok {
		gs = &gaugeSet{}
		r.gauges[name] = gs
		if _, taken := r.descs[name]; !taken {
			r.descs[name] = desc
		}
		first = true
	}
	gs.observers = append(gs.observers, observe)
	r.mu.Unlock()

	if first {
		// 首个观察者落表时建 OTel 仪表 + 回调（一次）；后续同名注册只追加
		// 观察者，回调每次收集时取当前观察者集。
		registerOtelGauge(r, name, desc)
	}
}

// gaugeObservers 快照某 gauge 的当前观察闭包集（锁内拷贝，锁外调用）。
func (r *Registry) gaugeObservers(name string) []func() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if gs, ok := r.gauges[name]; ok {
		return append([]func() float64(nil), gs.observers...)
	}
	return nil
}

// Counter 单调计数器：OTel Float64Counter + 手动累计器双轨。
// Add 并发安全；值只增不减属使用约定（OTel Counter 语义）。
type Counter struct {
	name string
	otel metric.Float64Counter // nil = 手动轨（noop 或创建失败）

	mu    sync.Mutex
	total float64
}

func newCounter(name string, inst metric.Float64Counter) *Counter {
	return &Counter{name: name, otel: inst}
}

// Name 返回指标名。
func (c *Counter) Name() string { return c.name }

// Add 累加 delta（可带属性；手动轨按名聚合不分属性——接线面全部无属性，
// 属性化需求出现时再扩手动键）。两轨先后各自完整执行，互不依赖。
func (c *Counter) Add(delta float64, attrs ...attribute.KeyValue) {
	if c.otel != nil {
		c.otel.Add(context.Background(), delta, metric.WithAttributes(attrs...)) // 例外③：OTel 计数器 Add 的签名所需（非阻塞 API，spec §5.1 示例）
	}
	c.mu.Lock()
	c.total += delta
	c.mu.Unlock()
}

// Value 手动读数（当前累计值）。
func (c *Counter) Value() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}
