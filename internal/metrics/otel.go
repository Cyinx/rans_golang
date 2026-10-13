// OTel 轨绑定：仪表注册与全局 provider 的对接。本文件
// 是包内唯一触碰 otel 全局态的位置。宿主接自己的 SDK 直接调
// otel.SetMeterProvider（包内不设透传面——otel 全局代理
// 机制保证先注册的仪表自动委派到后装的 provider，接线次序无关）。
package metrics

import (
	"context"

	"github.com/cyinx/rans_golang/internal/log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// currentMeter 取当前全局 provider 名下本注册表 meter 名的 meter。
// otel 全局 provider 永不为 nil（未设置时是 noop），Meter 永不返回 nil
// ——无需防御分支。
func currentMeter(meterName string) metric.Meter {
	return otel.GetMeterProvider().Meter(meterName)
}

// registerOtelGauge 为注册表的首个同名 gauge 建 OTel ObservableGauge 与
// 采样回调：每次 OTel 收集时对当前全部观察闭包求和后上报单一观测值。
// 创建失败（含 noop 静默路径不会走到的真失败）只 Warn，手动轨不受影响。
func registerOtelGauge(r *Registry, name, desc string) {
	m := currentMeter(r.meterName)
	g, err := m.Float64ObservableGauge(name, metric.WithDescription(desc))
	if err != nil {
		log.Warn("rans/metrics: otel gauge creation failed, manual-only",
			"name", name, "meter", r.meterName, "err", err)
		return
	}
	if _, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		sum := 0.0
		for _, observe := range r.gaugeObservers(name) {
			sum += observe()
		}
		o.ObserveFloat64(g, sum)
		return nil
	}, g); err != nil {
		log.Warn("rans/metrics: otel gauge callback registration failed, manual-only",
			"name", name, "meter", r.meterName, "err", err)
	}
}
