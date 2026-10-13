package metrics_test

// OTel SDK 导出路径测试：sdk/metric ManualReader + 内存
// MeterProvider（仅测试依赖——生产代码只依赖 otel API）。otel.SetMeterProvider
// 后 Counter.Add 与 Gauge 观察回调必须流经 SDK 导出面；scope 名即 meter 名
// （规格 §16 Meter "rans"）。

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/cyinx/rans_golang/internal/metrics"
)

// collectMetric 从 ManualReader 收集结果中按指标名提取 float64 数值。
// counter 走 metricdata.Sum、gauge 走 metricdata.Gauge；未命中时报错。
func collectMetric(t *testing.T, reader *sdkmetric.ManualReader, name string, wantGauge bool) float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("ManualReader.Collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[float64]:
				if wantGauge {
					t.Fatalf("metric %s is Sum, want Gauge", name)
				}
				if len(data.DataPoints) != 1 {
					t.Fatalf("metric %s: %d datapoints, want 1", name, len(data.DataPoints))
				}
				return data.DataPoints[0].Value
			case metricdata.Gauge[float64]:
				if !wantGauge {
					t.Fatalf("metric %s is Gauge, want Sum", name)
				}
				if len(data.DataPoints) != 1 {
					t.Fatalf("metric %s: %d datapoints, want 1", name, len(data.DataPoints))
				}
				return data.DataPoints[0].Value
			}
		}
	}
	t.Fatalf("metric %s not found in collected metrics", name)
	return 0
}

// TestOTelSDKExportViaManualReader SDK 接线主干：otel.SetMeterProvider(SDK) →
// 仪表注册 → Add/观察 → reader.Collect 可见。二次收集验证计数器持续
// （cumulative）与观察回调即时读数。
func TestOTelSDKExportViaManualReader(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		// 还原全局：动进程全局的测试不得泄漏给包内其余测试
		//（noop 是未接线形态的缺省语义）。
		otel.SetMeterProvider(noop.NewMeterProvider())
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = reader.Shutdown(ctx)
	})

	reg := metrics.New("rans")
	c := reg.Counter("test.otel.counter", "otel export counter")
	c.Add(3)
	reg.Gauge("test.otel.gauge", "otel export gauge", func() float64 { return 9 })

	if got := collectMetric(t, reader, "test.otel.counter", false); got != 3 {
		t.Fatalf("otel counter export = %v, want 3", got)
	}
	if got := collectMetric(t, reader, "test.otel.gauge", true); got != 9 {
		t.Fatalf("otel gauge export = %v, want 9", got)
	}

	// 收集后继续累计：二次收集必须反映增量（cumulative 聚合）。
	c.Add(2)
	if got := collectMetric(t, reader, "test.otel.counter", false); got != 5 {
		t.Fatalf("otel counter export after Add(2) = %v, want 5", got)
	}
}

// TestOTelSDKScopeNameIsMeterName scope 名必须等于注册表 meter 名
// （规格 §16 Meter "rans"——宿主按 scope 归属指标）。
func TestOTelSDKScopeNameIsMeterName(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		// 还原全局：动进程全局的测试不得泄漏给包内其余测试
		//（noop 是未接线形态的缺省语义）。
		otel.SetMeterProvider(noop.NewMeterProvider())
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = reader.Shutdown(ctx)
	})

	reg := metrics.New("rans")
	reg.Counter("test.scope.counter", "d").Add(1)

	var rm metricdata.ResourceMetrics
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("ManualReader.Collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "test.scope.counter" {
				if sm.Scope.Name != "rans" {
					t.Fatalf("scope name = %q, want \"rans\"", sm.Scope.Name)
				}
				return
			}
		}
	}
	t.Fatal("test.scope.counter not found in collected metrics")
}
