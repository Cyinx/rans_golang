module github.com/cyinx/rans_golang

go 1.27.1

require (
	github.com/redis/go-redis/v9 v9.7.0
	go.opentelemetry.io/otel v1.44.0
	go.opentelemetry.io/otel/metric v1.44.0

	// 测试专用（Phase 6 Task 4）：metrics 包的 OTel 导出验证用 SDK
	//（ManualReader）。生产代码只依赖上方 otel API——宿主不接 SDK 时
	// 全局 provider 为 noop，指标零输出零报错（规格 §16）。
	go.opentelemetry.io/otel/sdk/metric v1.44.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/sdk v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
)
