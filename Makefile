# 本地 Redis 直连覆盖（内网带认证实例等）：地址/密码不入公开仓库——
# 在仓库根放 redis.local.env（已 gitignore）写：
#   RANS_TEST_REDIS_ADDR=192.168.x.x:31001
#   RANS_TEST_REDIS_PASSWORD=<内网密码>
# 或直接 export 同名变量。设置了 RANS_TEST_REDIS_ADDR 即绕过 docker 模式。
-include redis.local.env

.PHONY: test test-quick lint vet fuzz schema-check test-redis-up test-redis-down test-redis-maybe

# Redis 测试基建（裁定正本 docs/decisions/2026-09-30-phase3-redis-env.md）：
# docker.io 直连不可达 → daocloud 镜像源；端口 16379 避让本机 6379 冲突。
REDIS_TEST_IMAGE := docker.m.daocloud.io/library/redis:7-alpine
REDIS_TEST_NAME  := rans-test-redis
REDIS_TEST_PORT  := 16379

# test-redis-up 幂等拉起测试 Redis：容器在跑 → no-op；存在但停着 → start；
# 不存在 → run（本地无镜像时从 daocloud 源拉）。收尾 docker exec PING 探活。
test-redis-up:
	@if docker ps --format '{{.Names}}' | grep -qx '$(REDIS_TEST_NAME)'; then \
		echo "$(REDIS_TEST_NAME) 已在运行"; \
	elif docker ps -a --format '{{.Names}}' | grep -qx '$(REDIS_TEST_NAME)'; then \
		echo "启动已停止的 $(REDIS_TEST_NAME)"; \
		docker start $(REDIS_TEST_NAME); \
	else \
		echo "拉起 $(REDIS_TEST_NAME) ($(REDIS_TEST_IMAGE), 端口 $(REDIS_TEST_PORT))"; \
		docker run -d --name $(REDIS_TEST_NAME) -p $(REDIS_TEST_PORT):6379 $(REDIS_TEST_IMAGE); \
	fi
	@i=0; until docker exec $(REDIS_TEST_NAME) redis-cli PING 2>/dev/null | grep -q PONG; do \
		i=$$((i+1)); \
		if [ $$i -ge 50 ]; then echo "$(REDIS_TEST_NAME) 探活超时（10s）" >&2; exit 1; fi; \
		sleep 0.2; \
	done
	@echo "$(REDIS_TEST_NAME) 就绪：127.0.0.1:$(REDIS_TEST_PORT)"

# test-redis-down 停删测试 Redis（幂等：容器不存在即 no-op）。
test-redis-down:
	@docker rm -f $(REDIS_TEST_NAME) >/dev/null 2>&1 || true
	@echo "$(REDIS_TEST_NAME) 已停止并删除"

vet:
# gofmt 门禁只扫 git 跟踪文件：`gofmt -l .` 会递归 .worktrees/，并行
# worktree 的未格式化 WIP 会打挂本仓验证（2026-10-13 实证）。
	@test -z "$$(git ls-files '*.go' | xargs -r gofmt -l)" \
		|| { git ls-files '*.go' | xargs -r gofmt -l; exit 1; }
	go vet ./...

test-quick: vet
	go test ./...

# make test 强约束 Redis 套件真跑；bare `go test ./...`（无 Redis）靠 skip
# 门禁仍绿——两形态都是合法验证。两种 Redis 来源（裁定正本见
# docs/decisions/2026-09-30-phase3-redis-env.md）：
#   ① docker 模式（缺省，零配置）：test-redis-maybe 拉起本地容器；
#   ② 内网直连模式：RANS_TEST_REDIS_ADDR（含 redis.local.env / export）非空即生效，
#      密码经 RANS_TEST_REDIS_PASSWORD 透传，跳过 docker；
#   ③ Redis Cluster 实测模式：RANS_TEST_REDIS_CLUSTER_ADDRS（逗号分隔种子地址）
#      非空即追加跑 cluster 套件（契约平移 + 真槽号断言；未设即 skip，
#      不影响其余门禁）。测试环境部署记录见 test-env（工作区非 git 目录）。
test: vet test-redis-maybe
ifneq ($(RANS_TEST_REDIS_ADDR),)
	RANS_TEST_REDIS_ADDR='$(RANS_TEST_REDIS_ADDR)' RANS_TEST_REDIS_PASSWORD='$(RANS_TEST_REDIS_PASSWORD)' \
		go test ./... -count=1 -race
else
	RANS_TEST_REDIS_ADDR=127.0.0.1:$(REDIS_TEST_PORT) go test ./... -count=1 -race
endif

# test-redis-maybe：内网直连模式 no-op；docker 模式委托 test-redis-up。
test-redis-maybe:
ifneq ($(RANS_TEST_REDIS_ADDR),)
	@echo "使用外部 Redis: $(RANS_TEST_REDIS_ADDR)（跳过 docker）"
else
	@$(MAKE) --no-print-directory test-redis-up
endif

# 帧解码器原生 fuzz（种子已入库 wire/frame_test.go，ROADMAP 2.2）。
# 握手 v3 扩展对象是 pre-auth 解析面（HR3 票 14），FuzzHandshakeDecode
# 并入同门禁（种子见 handshake_test.go）。edge 外部帧解码面随 v0.24.0
# edge 部件退役一并移出。
fuzz:
	go test ./internal/wire -run '^$$' -fuzz '^FuzzFrameDecode$$' -fuzztime 10s
	go test ./internal/wire -run '^$$' -fuzz '^FuzzHandshakeDecode$$' -fuzztime 10s

# schema-check 线协议兼容门禁（HR3 票 15，加固 spec §6.1 第 5 点）：先重生成
#（幂等），再把全部线面包的消息清单对发布基线 -check——同字段号改线型/
# 字段号复用 → 失败（非零退出）；删字段/删消息 → 告警（打印不阻断）。
# SCHEMA_DIRS 与 cmd/ransgen/schema_test.go 的 wireDirs 是同一组线面包，
# 改动须两处同步。基线 cmd/ransgen/testdata/schema_baseline.json = 上一
# 发布 tag 的清单，每次发版更新（流程见 README「代码生成」节末）。
SCHEMA_DIRS := examples/chat cmd/ransgen/testdata/entity_demo cmd/ransgen/testdata/serial_demo
SCHEMA_BASELINE := cmd/ransgen/testdata/schema_baseline.json

schema-check:
	go generate ./...
	go run ./cmd/ransgen $(foreach d,$(SCHEMA_DIRS),-dir $(d)) -check $(SCHEMA_BASELINE)

lint:
	if command -v golangci-lint >/dev/null; then golangci-lint run ./...; else echo "golangci-lint 未安装，跳过"; fi
