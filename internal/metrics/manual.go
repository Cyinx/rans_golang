// 手动读数器（manual snapshot）：无 SDK / 测试环境的
// 观测面。Snapshot 即时读数——Counter 取手动累计器、Gauge 逐闭包求和，
// 与 OTel 轨的聚合口径一致（同名 gauge 求和，见包注释）。
package metrics

import "sort"

// Snapshot 返回注册表全部指标的当前读数：键为指标名，值为
// Counter 累计值 / Gauge 观察闭包之和。观察闭包在注册表锁外调用
// （闭包可安全重入本包 API）；Gauge 无观察者时不出现键。
// 跨类型同名以 gauge 读数呈现（既定契约，见包注释）。
func (r *Registry) Snapshot() map[string]float64 {
	r.mu.Lock()
	counters := make([]*Counter, 0, len(r.counters))
	for _, c := range r.counters {
		counters = append(counters, c)
	}
	gauges := make(map[string][]func() float64, len(r.gauges))
	for name, gs := range r.gauges {
		gauges[name] = append([]func() float64(nil), gs.observers...)
	}
	r.mu.Unlock()

	out := make(map[string]float64, len(counters)+len(gauges))
	for _, c := range counters {
		out[c.Name()] = c.Value()
	}
	for name, observers := range gauges {
		sum := 0.0
		for _, observe := range observers {
			sum += observe()
		}
		out[name] = sum
	}
	return out
}

// Snapshot 缺省注册表的包级手动读数（等价 Default().Snapshot()）。
func Snapshot() map[string]float64 { return Default().Snapshot() }

// Reading 单条仪表读数（Readings 的元素形态）：名字、口径说明与当前值。
type Reading struct {
	Name  string  // 指标名（Name* 常量族）
	Help  string  // 口径说明（注册时登记，首次为准）
	Value float64 // 当前读数（Gauge 采样值 / Counter 累计值）
}

// Readings 返回注册表全部已注册仪表的结构化读数（按名排序——稳定输出，
// 快照面直接可用）。语义与 Snapshot 一致（含锁外调用观察闭包）；
// 无 SDK 环境即可用。
func (r *Registry) Readings() []Reading {
	r.mu.Lock()
	counters := make([]*Counter, 0, len(r.counters))
	for _, c := range r.counters {
		counters = append(counters, c)
	}
	gauges := make(map[string][]func() float64, len(r.gauges))
	for name, gs := range r.gauges {
		gauges[name] = append([]func() float64(nil), gs.observers...)
	}
	descs := make(map[string]string, len(r.descs))
	for name, help := range r.descs {
		descs[name] = help
	}
	r.mu.Unlock()

	out := make([]Reading, 0, len(counters)+len(gauges))
	for _, c := range counters {
		out = append(out, Reading{Name: c.Name(), Help: descs[c.Name()], Value: c.Value()})
	}
	for name, observers := range gauges {
		sum := 0.0
		for _, observe := range observers {
			sum += observe()
		}
		out = append(out, Reading{Name: name, Help: descs[name], Value: sum})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
