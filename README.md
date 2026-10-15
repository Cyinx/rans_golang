# rans_golang

Rans 分布式虚拟 actor 运行时的 Go 实现：单写者实体内核、Redis 目录激活仲裁、
跨节点 RPC、迁移与故障接管。消费方通过 GitHub 模块路径
`github.com/cyinx/rans_golang` 依赖。演进历程与当前版本以 `CHANGELOG.md`
首节为准（最新 semver tag 即当前版本）；升级注意
[docs/consumer-upgrade-notice.md](docs/consumer-upgrade-notice.md)。

## 特性

- **虚拟 actor**：实体按需激活、闲置钝化、节点崩溃后由幸存者接管——业务
  代码只写单实体逻辑，不做放置与寻址
- **单写者执行内核**：每实体一个 pawnite 串行 loop，实体代码天然无并发
- **Redis 目录仲裁**：三个原子单键 Lua（注册/收养/接管、条件注销墓碑、
  换锁），实例级归属 `Owner{Node, Epoch, Incarnation}` 三元组 + 成员代际有效性判定；
  Redis Cluster 兼容（逐实体 hash tag 目录真分片）
- **跨节点 RPC**：握手 v5（HMAC 认证 + SchemaDigest 观测 + 启动代际检查拒绝混版本）、统一调用入口
  （`Call`/`Tell` + CallOption）、失败分类重试与毫秒级快速失败
- **迁移与生命周期**：目录 CAS 换锁迁移（线性化点明确）、四段优雅排空、
  三看门狗（卸载/激活/执行卡死强弃）、闲置钝化与再平衡、checkpoint 快照
- **让出重入**：`Defer` + `ReenterAfter`——Handle 内让出实体 loop 等长 IO，
  让出期间实体照常服务其他消息，完成后续体回 loop 串行执行
- **安全**：HMAC fail-closed 缺省（未配密钥且未显式 Insecure 即拒绝构造）、
  TLS 显式先行握手、常量时间比较
- **工具链**：注解驱动代码生成器 `ransgen`、进程内多节点测试集群 `testkit`
  （故障注入 Kill/Partition/Slow + 不变量检查器）

## 安装

```bash
go get github.com/cyinx/rans_golang
```

Go 工具链版本以 go.mod 为准。Redis 非必须：单节点 Standalone 形态零外部依赖。

## 快速上手

```bash
# 1) 本地内核与跨节点冒烟（无外部依赖）
go run ./examples/counter        # 输出 counter = 1..5（注册 kind → 激活 → Call 递增）
go run ./examples/chat           # 跨节点：节点 2 OneWay 发言 + Call 拉取节点 1 聊天历史（生成器驱动）
go run ./examples/reenter        # 让出重入：实体 A 经 Defer + ReenterAfter 让出调慢实体 B，
                                 # 让出期间 note 消息照常服务（输出序列确定，测试自检）

# 2) 全量验证
make test    # gofmt 门 + go vet + go test -race 全量（自动拉起 docker Redis 16379，契约/租约/集群套件真跑）
make fuzz    # 原生 fuzz 两解码面：wire 帧 + 握手扩展对象（各 10s，种子入库）
# 无 Redis 环境裸 `go test ./...` 仍全绿（redis 套件按探活门禁 skip）
# 外部 Redis 直连：export RANS_TEST_REDIS_ADDR=<host:port> RANS_TEST_REDIS_PASSWORD=<密码>
#（或仓库根 redis.local.env，已 gitignore）——设置后 make test 跳过 docker 直连外部实例

# 3) Redis 目录仲裁演示
make test-redis-up               # 拉起测试 Redis（127.0.0.1:16379）
go run ./examples/arbitration    # 双节点共享 Redis 目录：并发首请求一赢一输（输家原始字节转发）+ 杀赢家 → 租约过期 → 幸存者接管

# 4) 代码生成（examples/chat 即生成器形态）
//go:generate go run github.com/cyinx/rans_golang/cmd/ransgen       # 目标包内任意 .go 文件加指令
go generate ./examples/chat && git diff --exit-code                # 幂等验收：重跑零 diff
make schema-check                                                  # 线协议兼容门禁：重生成 + 全部线面包对发布基线 -check

# 5) TLS / HMAC——密钥装载（Go 侧接线见下一代码块）
export RANS_HMAC_KEY=<hex>       # 握手 MAC 密钥（hex 文本；env 与文件通道同一约定，*.key 不进 git）
```

```go
key, _ := node.LoadHMACKeyFromEnv()  // 或 node.LoadHMACKeyFromFile(path)；未设 → (nil, nil)（New 拒绝构造）
cfg := node.Config{ /* ... */, Security: node.SecurityConfig{HMACKey: key} }
// 未配密钥且未显式 Security.Insecure → node.New fail-closed 拒绝；Insecure: true = 显式不安全 opt-in（每次握手告警）
cfg.Security.TLS = &tls.Config{...}  // TLS 经 Go 标准 crypto/tls 配置（nil = 明文）；握手先于帧循环

// 6) 指标 / 日志一行接入
snap := n.Metrics()          // 本节点仪表快照 []node.Metric{Name,Help,Value}（无 SDK 即可用；清单见「安全与观测」指标表）
rans.SetLogger(myLogger)     // 库日志统一门面（SetLogger(nil) 恢复 slog.Default()）
```

注解语法、方法面门卫与生成物细节见「代码生成」节；失败策略与迁移语义见对应专题节。

## 最小装配清单（QUICKSTART：从零到多进程可服务）

一个节点进程 = `node.New(Config)` 构造（全量校验、不启 goroutine）→
`Start`/`Run` 起跑 → `Shutdown`（优雅排空）或 `Close`（立即停止）。目录、
激活表、节点存活信号（成员键比对续期 + 自围栏止损）、位置缓存+目录变更
轮询、放置、地址解析、传输、看门狗与周期任务全部内置装配——Standalone
形态同样有看门狗与周期 checkpoint（等价性有测试锚定）。`examples/arbitration`
是多进程 Redis 形态的可运行样例，`testkit.NewCluster` 是同一骨架的测试封装。

**单节点形态（Standalone：无传输、无握手，目录 inmem）**：

```go
n, err := node.New(node.Config{
    NodeID: 1,                       // 必填，≠ 0（0 是匿名源哨兵）
    Standalone: true,                // 不监听不握手；与 ListenAddr、Directory.Redis 互斥；无 HMAC 要求
    Kinds: map[string]rans.Factory{  // 业务 kind 工厂（生成物 Kinds 可直接填入）
        "counter": func(_ rans.ID) rans.Entity { return &counter{} },
    },
    Store: myCheckpointStore,        // 有状态实体必配（迁移/钝化/终快照的正确性依赖）
})
if err != nil { /* 配置错误当场暴露（fail-closed） */ }
if err := n.Run(ctx); err != nil { /* ... */ }   // Start → 信号等待 → 优雅停机
// 把手：n.Client()（唯一调用入口）/ n.Addr() / n.Metrics() / n.LocalIDs()
// 管理操作：n.Destroy(ctx, id) / n.Migrate(ctx, id)——只作用于本地实例，非本地 NotFound+NotLocal
```

**多节点集群形态（共享 Redis 目录；各进程持不同 NodeID 指向同一 Redis
即成集群——节点发现、地址解析、放置候选全部由目录承载）**：

```go
key, _ := node.LoadHMACKeyFromEnv()             // 握手密钥（fail-closed：未配且未显式 Insecure → New 拒绝）
n, err := node.New(node.Config{
    NodeID: nodeID, ClusterName: "prod",        // 集群名全集群一致（握手域）
    ListenAddr: "0.0.0.0:7100",                 // 缺省 127.0.0.1:0（内核分配，Start 后 n.Addr() 读实际地址）
    Security:   node.SecurityConfig{HMACKey: key},
    Kinds:      chatgen.Kinds,                  // ransgen 生成物注册表直接填入
    Directory: node.DirectoryConfig{Redis: &redis.Options{
        Addr: redisAddr, Password: redisPwd, KeyPrefix: "rans:", // KeyPrefix 空 → node 补缺省
    }},                                         // 零值即 inmem；注入目录走 Directory.Injected（调用方持有）
    Store:      store,
    Calls:      node.CallConfig{Default: 30 * time.Second},              // 调用缺省预算；上限 Calls.Max（见参数总表）
    Lease:      node.LeaseConfig{Interval: 30 * time.Second},        // 租约目录形态生效；inmem 自动跳过
})
if err != nil { /* ... */ }
if err := n.Start(ctx); err != nil { /* ... */ }
resp, err := n.Client().Call(ctx, id, msg, rans.Idempotent())    // 唯一调用入口
```

要点：

- **配置构造时一次给全，运行中不可变**：全仓无运行期 `Set*`；
  per-kind 闲置回收在 `KindOptions`、看门狗覆盖在 `Watchdog.KindOverrides`；
- **调用一律经路由**：`n.Client()`（或实体内 `rans.ClientFrom(ctx)`）——
  本地命中走本地快路径，与远端同一错误形态（`*rans.Error`）；
- **放置**：`Config.Placement` nil → 候选中随机（`node.RandomPlacement`）；
  custom 策略实现 `Pick(kind, self, candidates)` 三参接口，
  候选集由 node 计算（声明承载该 kind、存活、未排空；迁移时不含本节点）；
- **闲置回收与再平衡**：`KindOptions[k].Idle`（`IdlePolicy{After, Migrate}`）
  + `Rebalance` 配置即自动执行，无手动入口；
- **Redis 部署形态**：Redis Cluster 兼容——registry 域 `{reg}`（总表+
  判活键+代际计数器）同 slot；**实体目录逐实体 hash tag**（主键
  `rans:e:{<kind>/<key>}`，{} 内完整实体身份——目录真分片，
  同实体单键操作同 slot；三个原子单键 Lua：注册四分支/条件注销墓碑/换锁；
  主键无 TTL 永不清删，注销写无主墓碑保留纪元，条目有效性由成员代际判定，
  死 owner 由激活 CAS 接管自愈）；`redis.Options.Addrs` 传种子列表即
  cluster/哨兵形态（DB 须 0）。cluster 兼容性经真 3-master 集群实测背书
  （契约平移 + 真槽号 + 槽号散布断言；`RANS_TEST_REDIS_CLUSTER_ADDRS`
  门禁，未设即 skip）；
- **双端同版本**：v0.26.0 起握手版本即断点（当前 v5）——升级先停全部
  再升；`node.Start` 启动代际检查按成员键协议区间拒绝混版本启动（见
  `docs/consumer-upgrade-notice.md` 对应版本节）。
  此后同一 VerMax 内向后兼容（新字段只追加、旧端跳过），不再要求全
  集群停机。

## 架构

公开面按角色分包；模块内依赖由 `depgraph_test.go`（仓库根导入图门禁）
按「可依赖」白名单逐条锁定——清单之外的模块内导入一律失败，新包必须先
登记再接线。`rans` 只依赖两个零依赖叶包（`internal/ctxkey`、`internal/log`）；
`node` 是唯一组装根；examples 与 testkit 只经公开包使用运行时
（testkit 另可经 `internal/testhooks` 逐节点注入故障面）。

```text
契约层      rans（ID/NodeID/Entity/Factory/Snapshotter/Client/CallOption/
                 Error/Code/Reason/AppError、ctx 访问器、SetLogger）
                │
组装层      node（唯一组装根：Config/New/Start/Shutdown/Close/Run/Addr/Client/
                 Destroy/Migrate/Metrics——目录、激活表、成员存活、
                 位置缓存+目录变更监听、放置、传输、周期任务全部内置）
                │
端口与      directory（端口：Directory + 租约/换锁/成员能力接口）
适配器        ├── directory/redis（三原子单键 Lua + TTL 判活） · directory/inmem
              │        两实现同过 directory/contract 可执行规格
              └── checkpoint（Store 接口 + InMemory + 纪元围栏）
                │
编码        codec（生成代码与手写序列化器依赖的编码面：Register/Encode/Decode/
                 Serializer/RegisterShapes/SchemaDigest）
                │
工具        testkit（进程内多节点集群 + 故障注入） · cmd/ransgen（代码生成器，
                 独立于运行时） · examples（四演示 main 包）
                │
内部实现    internal/pawnite（执行内核） · activation（本地激活表+注册仲裁）
                 · lifecycle（迁移/钝化/checkpoint/看门狗/再平衡/排空）
                 · rpc（统一调用客户端+入站分发） · routing（失败分类/位置缓存/
                 目录变更轮询） · placement · membership（Liveness）
                 · transport(+tcp) · wire（帧/头部/握手/页池） · protocol
                 · metrics · ctxkey · log · testhooks
```

| 包 | 职责 |
|---|---|
| `rans`（仓库根） | 契约层：实体身份 `ID{Kind,Key}`（`kind/key` 规范键）与 `NodeID`、实体契约 `Entity`/`Factory`/`Snapshotter`、统一调用面 `Client`（`Call`/`Tell` + `CallOption`，`Idempotent()`）、统一错误 `*Error{Code,Reason,Message,Node,AppCode,AppData}` + `CodeOf`/`ReasonOf` 与哨兵族、业务错误接口 `AppError`/`AppDataError`、ctx 访问器 `Self`/`Incarnation`/`ClientFrom`、日志入口 `SetLogger` |
| `node` | 组装层（唯一组装根）：`Config`（构造时一次给全，运行中不可变）+ `New` 构造期全量校验、`Start`/`Shutdown`/`Close`/`Run` 生命周期面、统一调用入口 `Client()`、管理操作 `Destroy`/`Migrate`（只作用于本地实例）、观测 `Metrics()`、密钥装载 `LoadHMACKeyFromEnv`/`LoadHMACKeyFromFile`、放置策略 `Placement`/`RandomPlacement`；目录/存活信号/位置缓存+变更轮询/传输/周期任务全部内置装配（见「最小装配清单（QUICKSTART）」） |
| `directory` + `directory/redis` + `directory/inmem` | 目录端口（`Directory{Register,UnregisterIfOwner,Lookup,Transfer}` + 成员能力接口 `NodeRegistry`，实例级归属 `Owner{Node,Epoch,Incarnation}` 三元组；Register 支持纪元下限）与内存/Redis 两实现（Redis：五原子单键 Lua + 判活键）；`directory/contract` 是第三方适配器的可执行规格（`RunRegisterSuite`/`RunNodeMembershipSuite`） |
| `checkpoint` | 快照存储接口 `Store{Get/Put/Delete}`（纪元围栏：回退写拒绝 `ErrStaleIncarnation`）+ InMemory 实现（持久实现归消费方）——迁移 CAS 前落储 / 激活兜底读 / 钝化与停机终快照的统一面 |；可执行契约套件 `checkpoint/contract.RunStoreSuite`（实现者注入即得全量行为断言——未命中/往返防御拷贝/严格小于拒绝/相等覆写/0 直通不抬水位/Delete 不撤销高水位/并发最大值胜出） |

| `codec` | 编码面：varint/ZigZag、field-tag 对象编码原语、值序列化注册表（`Register`/`Encode`/`Decode`/`Serializer`）、形状登记与摘要（`RegisterShapes`/`SchemaDigest`——生成物 init 自动登记，握手表观测） |
| `testkit` | 进程内多节点测试集群与故障注入原语（`NewCluster`/Kill/Partition/Slow + TLS/HMAC/StrictSchema 逐节点配置 + `FaultStore`/`InvHistory` 不变量检查器 + 租约故障注入），全部验收测试载体；故障注入经 `internal/testhooks` 逐节点生效（并行测试互不干扰）；`sigtermhelper/` 为 kill -TERM 优雅排空演练夹具兼宿主停机接线活样例 |
| `cmd/ransgen` | 注解驱动的代码生成器（实体/调用/序列化器/wrapper/代理）+ `rans_schema.json` 清单产出与 `-check` 兼容检查（线协议演进门禁） |
| `internal/*` | 实现细节（不进公开面，升级不受其影响）：执行内核 `pawnite`、激活表 `activation`、生命周期编排 `lifecycle`、调用面 `rpc`、路由/放置/成员 `routing`/`placement`/`membership`、传输 `transport(+tcp)`、线协议 `wire` 与共享小包 `protocol`/`metrics`/`ctxkey`/`log`、测试注入 `testhooks` |
| examples | 四演示 main 包：`counter`（Standalone 单节点）、`chat`（双节点回环 + ransgen 生成物全形态）、`arbitration`（Redis 目录仲裁/再平衡/接管）、`reenter`（Defer + ReenterAfter 让出重入——长 IO 让出、让出期间他消息照常服务、then 聚合延迟回复） |

> 代码注释中的规格/决策编号指向维护者内部文档（`docs/porting/`、`docs/decisions/`——随仓库分发）。

## 宿主围栏契约（自持存储）

消费方不经 `checkpoint.Store` 而在实体 Hook（Activate/Deactivate/Handle）里直写自有存储（如 MySQL）时，必须以 `rans.Incarnation(ctx)` 令牌自行承担围栏——一页式契约：

- **按实体的单调高水位**（不是按行、不是按会话）：每个实体 id 持一个已接受的最大纪元；
- **严格小于拒绝**：`inc < 已存最大值` 的写入必须拒绝（旧 incarnation 的残留在飞写）；**相等放行**（同纪元重试是常态）；**0 = 未围栏**直通且不抬高水位；
- **比较与写入必须原子**（单语句条件更新或事务内 `SELECT … FOR UPDATE`）——先读后写的两步在并发下放行旧纪元；
- **Delete 不撤销高水位**：实体删除后残留的旧纪元在飞写仍须被拒；
- 推荐实现 `checkpoint.HighWaterReader` 同构的只读高水位面，或给运行时配 `node.Config.FencingFloor` 返回该最大纪元（每次激活前调用）。

MySQL 参考配方（单行持高水位；受影响行语义已处理）：

```sql
-- 表：fence(entity_id PK, inc BIGINT UNSIGNED NOT NULL, ...)
-- 写入（比较+写入原子；相等放行、严格小于拒绝）：
UPDATE fence SET inc = ?, payload = ? WHERE entity_id = ? AND inc <= ?;
-- 受影响行 0 且现存 inc > 新 inc → 拒绝（旧纪元）；0 行且行不存在 → 先 INSERT
--（IGNORE 撞主键后重跑 UPDATE——同值重试在缺省受影响行语义下报 0 行，须
-- 以「行不存在」区分，不能把 0 行一律当拒绝）。
-- 高水位只读：SELECT inc FROM fence WHERE entity_id = ?;
```

注意 `INSERT … ON DUPLICATE KEY UPDATE` 不能携带 `WHERE` 条件——无法表达「严格小于拒绝」，不要用它实现本契约。

## 失败策略

调用路径默认带分类重试与快速失败；位置缓存由 node 恒装配（正 30s/负 1s 双
TTL，五类失效钩子全部 node 内部接线，无手工配置面）：

- **统一错误模型**：`Call`/`Tell` 返回的错误一律是 `*rans.Error`
  （`Code` 重试类别 + `Reason` 具体原因，本地与远端同构，`errors.Is` 判
  `rans.ErrTimeout`/`ErrUnavailable`/`ErrNotFound` 等哨兵）；实体 Handle
  返回的错误在投递边界折叠为 `CodeError`+`Reason=Handler`（实现
  `rans.AppError`/`AppDataError` 的再带 AppCode/AppData 过线）——调用方
  **不能** Unwrap 到实体的原始错误（不会把实体内部嵌套路由错误误判为本次
  调用状态）。管理操作作用于非本地实体返回 `NotFound`+`NotLocal`。
- **分类重试环**（唯一分类表）：`Unavailable`（确定未执行）
  无条件重试（退避 + 次数上限）；`Unavailable`+`Busy`/`Overloaded` 不重试
  （过载保护）；`NotFound`+`StaleLocation`/`UnknownKind` 重解析位置后重试；
  `Timeout`/`ConnectionLost`（模糊投递——请求可能已送达）**仅幂等标注的
  调用重试**；其余确定性失败上抛。至多 3 次 attempt + (n+1)×20ms 线性退避；
  每次重试重建载荷并重走路由（池化缓冲随失败归还，同载荷重试会读到已归还
  内存）。本地命中走本地快路径、同一错误形态（本地错误零重试，仅激活仲裁
  类执行前确定性失败本地重试一次）。
- **幂等标注双轨**：调用面 `rans.Idempotent()` CallOption
  （`n.Client().Call(ctx, id, msg, rans.Idempotent())`）；生成器半边
  `//rans:idempotent` 方法注解（见「代码生成」节）——代理内默认追加该选项。
  未标注按非幂等处理（保守侧——重试副作用风险由标注者显式承担）。
- **快速失败**：目标节点连接丢失 / 发送重试耗尽 → 快速失败摊派取消发往该
  节点的全部在途请求，毫秒级以 `ConnectionLost` 收场（不等满固定超时）；
  `Node.Close` 同路径收尾。请求固定超时由网关 deadline 扫描器兜底
  （单 goroutine 250ms 全表扫，O(pending)、零 per-request timer）。

## 迁移与生命周期

迁移换锁、四段排空、三看门狗与闲置钝化全量落地。生命周期动词正本：
`Start`（恰一次）→ `Shutdown`（优雅排空）/ `Close`（立即、幂等）；
`Run` 为便捷形态；`Kill` 是 testkit 专用的崩溃模拟。

- **迁移**：`n.Migrate(ctx, id)`——只作用于本地实例（非本地
  `NotFound`+`NotLocal`）。源端序列：placement 选点（`Config.Placement`，
  排除本地）→ 总预算 30s（内部缺省）→ 快照落 Store（交接窗内）→
  随迁包直发目标（at-most-once 快路径）→ 目录实例级 CAS 换锁。**CAS 是
  线性化点**：赢 ⟺ 目录已指向目标（不存在「源已让渡、目标未接管」窗口）
  ——赢家在箱消息整箱转发搭车去新家、源端跳过目录注销仅逐出本地；输/
  取消回退 Active 继续服役（`Error`+`Reason=MigrationLost` 链携带输因，
  `errors.Is` 可判）。目标端零新代码：换锁后首条消息走既有按需激活，
  恢复梯子 = 随迁包缓存（TTL 5min，免 Store 读）→ Store 兜底；包缺失/
  形状不匹配自动回落。⚠️ 未配置 Store 的部署形态：迁移状态只活在随迁包
  5min TTL 窗口内——目标端在窗口外才激活（迁移取消/目标重启/包丢失）即
  状态归零（裸激活）。有状态实体生产部署必须配置 `Config.Store`
  （at-most-once 语义以 Store 为正确性依赖，Store 写入是持久性记录，包
  只是快路径）。实体实现可选 `rans.Snapshotter{Snapshot,Restore}` 接入
  快照面（迁移/钝化/终快照共用）。
- **四段排空**：`n.Shutdown(ctx)` 内置四段（原 Drain 语义并入优雅
  停机）：① dispatcher 门闸（排空中本地命中放行、已进交接窗实体留箱、
  新请求按确定未执行回绝促重路由）→ ② 并行销毁全部本地实体（并行度
  max(8, CPU×8)，单实体失败不阻断；销毁 = 交接窗：在箱留存 → 终快照落
  Store → 条件注销）→ ③ 出站 flush（500ms 有界窗口内 pending 清空）→
  ④ 停序（停存活信号 → 节点注销（纪元守卫）→ listener/连接 → 网关）。`n.Close()`
  是立即路径：不排空、不注销、不留终快照（目录/成员键留待租约 TTL 过期，
  模拟崩溃即此形态）。kill -TERM 演练与停机接线活样例见
  `testkit/sigtermhelper/`（READY → SIGTERM → 优雅排空 → DRAINED →
  退出码 0，`testkit/migration_acceptance_test.go` 驱动）。
- **三看门狗（卸载/激活/执行）**：`Config.Watchdog` 配置即自动
  周期扫描（缺省 1s；无关闭开关——空闲节点零成本）：卸载卡死（Destroying
  停驻超缺省 60s）、激活卡死（Activating 停驻超缺省 60s——本节点粘性请求
  黑洞的唯一收敛面）与执行卡死（Active 顶层执行停驻
  超缺省 120s）→ 强弃：在箱消息改道回绝、在途调用方解绑（Timeout 收场）、
  条件目录注销、本地逐出。阈值全局 + `Watchdog.KindOverrides[k]` per-kind
  覆盖（LLM 类慢实体按轮长放宽）；强弃在 pawnite gate 内重验
  状态，陈旧快照零副作用。
- **闲置钝化与再平衡**：`KindOptions[k].Idle`（`IdlePolicy`
  两档）声明闲置策略——`Migrate: false` 钝化（快照落 Store + 本地销毁，
  下次访问重激活从 Store 恢复）或
  `Migrate: true` 迁移档（迁给放置远端，最冷优先）；`Rebalance` 本地实体
  数超阈自动卸载（阈值缺省 0=关闭、60s 周期、单轮上限 10，近似档）。全部
  自动执行、无手动入口。周期 checkpoint 随 `Store` 配置自动接线
  （`Tuning.CheckpointInterval` 1min 缺省、脏纪元门控）。

## 代码生成（ransgen）

`cmd/ransgen` 扫描包内注解，产出 `rans_gen.go`（建议入库）：每 RPC 方法的请求类型
（wire 路由名 `<Kind>.<Method>`）+ 值序列化器注册 + 服务端 wrapper 分发 + 客户端代理 +
`Kinds` 注册表 + `RansShapes` 形状清单（init 内自动 `codec.RegisterShapes` 登记，
摘要经 `codec.SchemaDigest()` 随握手观测）。实体只需声明**导出方法**（首参 `context.Context`），
手写分发/手写序列化器退役（`examples/chat` 即此形态，`go run ./examples/chat` 冒烟）。

```bash
// 目标包内任意 .go 文件加指令（go generate 以包目录为 cwd，-dir/-out 全默认对位）：
//go:generate go run github.com/cyinx/rans_golang/cmd/ransgen

go generate ./examples/chat && git diff --exit-code -- examples/chat/rans_gen.go  # 幂等验收：重跑零 diff
```

**注解语法**：

| 注解 | 位置 | 语义 |
|---|---|---|
| `//rans:kind=<name>` | struct | 实体注册：kind 名即目录/身份键（字符集 `[A-Za-z_][A-Za-z0-9_-]{0,63}`）；kind↔实体双射，重复即生成失败（E316） |
| `//rans:name=<name>` | struct | 消息显式命名（wire 注册名）；缺省用类型名 |
| `//rans:idempotent` | 方法 | 幂等标注：该方法生成的代理默认追加 `rans.Idempotent()` CallOption——模糊投递类失败（超时/节点失联）由重试环自动重放，重放安全性由标注者保证。旗注解不带值；只认 RPC 面方法（挂生命周期/未导出方法或类型声明上 → 生成失败）；OneWay（Tell）路径不受影响 |
| `rans:"order=N"` | 字段 tag | 字段进序列化面：N 为 field id（struct 内唯一）；无 tag 字段 = 本地状态，不进消息面 |

**方法面与门卫**（生成期硬拦截）：

- RPC 面 = 导出方法（`Activate`/`Deactivate`/`Handle` 与未导出方法不入面）；生命周期
  方法用户按需声明 `func(context.Context) error`，wrapper 委托，未声明视为无逻辑
- 首参必须 `context.Context`；返回形态限 `(T, error)`（Call）/ `(error)`（Call 无值）/
  无返回（OneWay），其余报错（M402）；`(error, error)` 由 M405 拒绝
- 返回类型与请求字段类型支持集（S004/M405）：标量 `int/int64/bool`（ZigZag varint）、
  `uint64/float64`（fixed64）、`string`、`[]byte`、已声明消息值（嵌套对象）及上述元素
  的切片（WireSequence）；指针/map/chan/接口/未声明 struct 生成失败
- 请求类型默认派生 `<Entity><Method>Request`（字段名 = 参数名首字母大写，未命名参数
  合成 `ArgN`，order=参数序）；**用户手写同名片会沿用**——注册名改挂 `<Kind>.<Method>`，
  字段按 order 对位且类型必须一致，超出参数集的字段分发时忽略

**生成物**：wrapper `<Entity>Pawnite`（实现 `rans.Entity`；用户实体实现
`rans.Snapshotter` 时自动透传快照面——生成形态实体同样参与 checkpoint/迁移/
钝化快照）、代理 `<Entity>Client{C rans.Client}`（唯一调用面，方法签名
`(ctx, id, 参数…, opts ...rans.CallOption)`；`//rans:idempotent` 方法在代理内
默认追加 `rans.Idempotent()`）、`Kinds map[string]rans.Factory`（工厂
`func(id rans.ID) rans.Entity`，直接填入 `node.Config.Kinds`）、`RansShapes`
（消息形状清单——init 内自动 `codec.RegisterShapes(RansShapes)` 登记，摘要
经 `codec.SchemaDigest()` 随握手观测，不再经任何配置传入）、按注册名字典序的
`codec.Register` init（输出字节确定，generate 幂等的根基）。生成代码只 import
公开包（`rans`、`codec`）。

**v1 限制**：单包扫描（实体/消息须在目标包内，跨包引用不在发现面）；方法路由为
TypeName 字符串（`<Kind>.<Method>`，复用 codec 注册表），methodHash 数值路由延后；
用户标识符撞 `Kinds`/`<T>Pawnite`/`<T>Client`/`rans*` 助手名不设门卫；派生请求 wire 名
撞用户 `//rans:name` 显式名 → 生成失败（重复消息名）；codec 导入路径硬编码本仓模块路径
（生成器与运行时同仓同版本，仅支持给本模块内的包生成）；泛型类型（kind 注解类型与
含 rans tag 字段的 struct）不支持，生成失败。

**v1 序列化语义注意**：

- 切片字段 round-trip 后恒为非 nil：解码侧 `make([]T, 0, n)` 重建，nil 切片往返后变为
  非 nil 空切片（`reflect.DeepEqual` 可见差异）；等值比较请用长度/元素比对而非 DeepEqual。
- `codec.Register` 同名重复注册为静默 last-write-wins：残留的同名手写注册与生成物
  init 按文件初始化顺序竞态（谁后初始化谁生效，无任何告警）。手写序列化器/wrapper 的
  退役仅靠约定（删净旧注册），无技术强制——迁移时务必清理。

**schema 兼容检查（线协议演进门禁）**：`make schema-check` 先重生成（幂等），
再把全部线面包（`examples/chat` + `cmd/ransgen/testdata` 两 fixture）的消息清单合并为
`rans_schema.json`（消息 → 字段 `{id, name, wiretype}` 全量，消息按 wire 名、字段按 id
升序，字节稳定可比对），对发布基线 `cmd/ransgen/testdata/schema_baseline.json` 检查：

- **失败（非零退出，发布阻断）**：同字段号改线型、同消息内字段号换用新字段（复用）；
- **告警（打印不阻断）**：删字段、删消息；
- 增字段、增消息走兼容通道静默通过（线协议在同一 VerMax 内向后兼容）。

**基线更新流程（每次发版执行）**：线面有改动时，随发版 commit 重产基线并入库——

```bash
go run ./cmd/ransgen \
  -dir examples/chat \
  -dir cmd/ransgen/testdata/entity_demo \
  -dir cmd/ransgen/testdata/serial_demo \
  -schema cmd/ransgen/testdata/schema_baseline.json
```

基线即「上一发布 tag 的清单」：发版时以本次产出为新基线；破坏性变更（触发了 -check
失败项）必须先提升 VerMin 并在 CHANGELOG 标注「滚动不兼容」。基线新鲜度由
`TestSchemaBaselineFresh` 锁定（线面改动未重产基线即 `go test` 红）。

## 安全与观测

- **TLS**（`node.Config.Security.TLS *tls.Config`，传输侧同配置）：crypto/tls 包装
  net.Conn；TLS 握手在帧循环启动**之前**显式完成（`TestTLSWrapConnHandshakeBeforeWrap`
  锁序）；明文探测静默丢弃不回错帧。testkit 一键 TLS 集群（自签 CA + 逐节点证书）。
- **HMAC**（fail-closed 缺省）：
  `Security.HMACKey == nil && !Security.Insecure`（非 Standalone）→ `node.New` 即拒
  （本地配置事故必须可见，`TestNodeNew_RequiresHMACOrInsecure` 锁定）；
  `Insecure: true` 为显式不安全 opt-in（每次握手完成 `slog.Warn` 留痕）；密钥装载
  `node.LoadHMACKeyFromEnv()`（`RANS_HMAC_KEY`）/`node.LoadHMACKeyFromFile(path)`
  （env 未设/文件空白 → (nil, nil)，装配门闸 fail-closed
  拒绝构造——空钥绝不静默放行），hex 文本两通道同一约定；比较恒 `hmac.Equal`
  常量时间。
- **SchemaDigest 观测**（握手 v3）：摘要 = 全部已登记消息形状（生成物 init
  自动 `codec.RegisterShapes`，`codec.SchemaDigest()` 计算）的 FNV-1a 64，
  随握手 v3 扩展对象过线并受 MAC 认证。失配只告警 + 计数
  `rans.handshake.schema_mismatch`（连接保持，滚动升级窗内新旧二进制形状集
  不同属预期）；`Security.StrictSchema: true` 时失配拒连（部署方按需开启）。
  CI 侧配套 `ransgen -check` 基线兼容检查（见「代码生成」节末）。
- **metrics**（`internal/metrics` 包，双轨 = OTel API 仪表 + 手动读数；
  节点装配的仪表落**节点自有注册表**，`Node.Metrics()` 即其
  结构化快照（Name/Help/Value，按名排序；含握手失配计数），无 SDK 即可用；
  `rans.*` 前缀三项进程级聚合 Counter 仍归进程级缺省注册表）：

  | 指标 | 类型 | 口径 | 观察点 |
  |---|---|---|---|
  | `rans.gateway.pending` | Gauge | 网关在途请求-应答条目数 | `Node.Metrics()` |
  | `rans.pawnite.queue_depth_total` | Gauge | 本地实体数 | `Node.Metrics()` |
  | `rans.pawnite.inbox_depth_total` | Gauge | 各 pawnite 收件箱积压深度之和 | `Node.Metrics()` |
  | `rans.transport.reconnects_total` | Counter | 同地址重拨次数（首拨不计数） | `Node.Metrics()` |
  | `rans.transport.batch_size` | Gauge | **最近一次**写冲刷帧数（非均值/累计；不引入 histogram） | `Node.Metrics()` |
  | `rans.handshake.schema_mismatch` | Counter | 握手 SchemaDigest 失配累计（观测模式：告警+计数、连接保持；StrictSchema 拒连不计入） | `Node.Metrics()` |
  | `rans.call.expired` | Counter | 出队过期即弃累计（出队执行前预算已耗尽、未执行按 Timeout+Expired 回绝） | `Node.Metrics()` |
  | `rans.activation.failed` | Counter | 实体激活失败累计（进程内跨节点聚合） | 进程级注册表（OTel 导出/宿主 SDK 同源可见） |
  | `rans.checkpoint.final_failed` | Counter | 停机终快照预算内重试后仍失败累计（同上） | 进程级注册表 |
  | `rans.lease.self_fenced` | Counter | 存活节拍持续失败触发自围栏轮数累计（逐节点告警可归因） | `Node.Metrics()` |
  | `rans.ownership.takeover_won` | Counter | 死 owner CAS 接管获胜累计 | `Node.Metrics()` |
  | `rans.ownership.stale_fenced` | Counter | 陈旧落库拒写触发的围栏/失主止损累计 | `Node.Metrics()` |
  | `rans.ownership.lost_sent` / `rans.ownership.lost_received` | Counter | 所有权丢失通知发出 / 收到并命中累计 | `Node.Metrics()` |
  | `rans.membership.resurrected` | Counter | 存活期成员键丢失（重取号，旧代际作废）累计 | `Node.Metrics()` |
  | `rans.membership.superseded` | Counter | 同 NodeID 更大代际占据成员键（让位）累计 | `Node.Metrics()` |

  节点配置面无任何运行期 `Set*`——全部配置在
  `node.New(Config)` 构造时一次给全，运行中不可变；`rans.SetLogger` 是唯一
  豁免（进程级日志门面横切装置非节点配置）。

- **日志门面**：库包日志统一经 `internal/log`（零依赖叶包）转发，不持 logger
  实例；默认透传 `slog.Default()`（跟随宿主 `slog.SetDefault`），宿主经
  `rans.SetLogger(l)` 全局换（nil 恢复默认）。

### 生产参数总表（代码字段 → 缺省值 → 备注）

生产部署的可调面全部是**代码字段**（本仓库唯一的 env 读取是 `RANS_HMAC_KEY`
密钥加载助手）；零值一律回落下表缺省。改值即改行为，先读对应字段的 godoc。
落点标注 internal 的为未公开缺省（实现细节不入公开面，缺省即生产值）。

| 参数 | 落点 | 缺省 | 备注 |
|---|---|---|---|
| 调用缺省预算 | `node.Config.Calls.Default` | 30s | 调用方 ctx 无 deadline 且无 `rans.Timeout(d)` 选项时的预算（发送段与等待段共用） |
| 调用预算上限 | `node.Config.Calls.Max` | 10min | 合成结果（ctx 剩余/Timeout(d)/Default 任一）超过即封顶 |
| 网关 pending 容量 | internal（未导出，缺省即生产值） | 100_000 | 满载拒绝 `Unavailable`+`Reason=Overloaded` |
| 心跳 ping / pong 踢除 | internal（传输连接判活内定值） | 10s / 30s | 连接级判活 |
| 发送重试 | internal（间隔 2s × 5 次） | 2s × 5 | 首重试免退避 |
| 应答/转发发送预算 | internal（回复预算常量） | 5s | 读循环/实体 loop 活界 |
| 成员键存活期 | `node.Config.Directory.Redis`（redis.Options：HeartbeatTTL） | 2min | 判活键是节点存活唯一权威；LeaseTTL 已删除——实体主键无 TTL |
| 心跳周期 / 自围栏 / 止损销毁并发与预算 | `node.Config.Lease`（Interval/SelfFence/MaxConcurrentDestroys/DestroyTimeout） | 30s / max(MemberTTL−2×Interval, Interval) / 8 / 30s | SelfFence：成员键写持续失败超阈值 → 本地全集关准入+止损销毁（接管误判的兜底）；<0 关闭 |
| 节点成员键（Redis Registry） | internal（Liveness 每心跳周期无条件双写；零实体照写） | 判活键 TTL 同过期阈值 2min | 总表 `rans:nodes` 枚举 + 判活键 `rans:node:<id>` 探活，读面与 keyspace 规模解耦；判活键值 = JSON `{addr,kinds,draining,epoch}`；节点代际经 `{reg}` 全局计数器 `rans:epoch`（HINCRBY 取号，无 TTL）启动取号一次全程携带，同节点号重启代际严格递增 |
| 看门狗（卸载/激活/执行） | `node.Config.Watchdog`（per-kind 覆盖 `Watchdog.KindOverrides[k]`） | 60s / 60s / 120s，周期 1s | **LLM 类慢实体必须按 kind 放宽** |
| 周期 checkpoint | `node.Config.Tuning.CheckpointInterval` | 1min（单轮上限 1000） | 脏纪元门控 |
| 随迁包 TTL / 包缓存容量 | `node.Config.Tuning.PackageTTL` / internal | 5min / 100_000 | 容量为高基数兜底 |
| 位置缓存 TTL / 容量 | internal（恒装配，`DefaultLocationCacheMaxEntries`） | 正 30s 负 1s / 100_000（64 分片均摊） | 饱和准入 O(分片容量)；失效钩子由 node 内部接线，陈旧正条目由成员代际有效性判定即时收口 |
| 存活节拍（Liveness） | `node.Config.Liveness`（Interval/SelfFence） | Interval 30s / SelfFence 按 MemberTTL 推导 | 成员键比对续期（一死永死）；Start 次序=同步取号→绑定→首写成员键→接入（失败即终态） |
| 节点本地成员视图 | `Tuning.MembershipRefresh` | 1s（下限 100ms） | 路由/放置/寻址读周期快照（注册表读放大收口）；死 owner 实时坐实（双轨判定） |
| 节点发件箱 | `node.Config.Outbox`（Capacity） | 65_536 | 单向出站入箱即返、按目标分队列保同目标 FIFO（每目标专属 worker，跨目标互不阻塞）；死目标熔断（发送重试耗尽/端点判停即开断，冷却后探针自愈）；满策略按生产者分野（显式 Tell 阻塞至 ctx；事件/回包/转发满即丢计数 `rans.outbox.dropped`；控制帧走预留容量）。FIFO 承诺限于**同一连接内**的写出序——例外：连接收敛（新旧交接，发送侧缓解后窗口显著缩小但收端两读循环仍可能交错）、输家转发/重定向、迁移整箱转发、发送失败与故障（跨调用方同样不承诺） |
| 放置正缓存 | internal（placementCacheTTL 常量） | 5s | routeMiss 选点回填短 TTL 正条目，只覆盖「选点→目录注册可见」激活窗 |
| 迁移总预算 | internal（`DefaultMigrationBudget`） | 30s | 源端序列总预算 |
| 再平衡 | `node.Config.Rebalance`（Threshold/Period/MaxPerRound） | 0=关 / 60s / 10 | 超阈自动卸载，配 `KindOptions[k].Idle` 迁移档 |
| 停机预算 | `node.Config.ShutdownBudget` | 15s | `Run` 的停机预算（`Shutdown` 直调时由调用方 ctx 承载） |
| 握手超时 / 出站队列深 / 帧上限 | internal（tcp 连接配置 / `internal/wire` 常量） | 5s / 1024 / 头 64KiB 帧 4MiB | 连接与帧面内定值 |

## 兼容性与升级

- 对外 API 变更走 semver tag + CHANGELOG；破坏性变更提前在 CHANGELOG 标注；
- 线协议承诺：同一握手 VerMax 内向后兼容（新字段只追加字段号、旧端跳过）；
  v0.22.0（握手 v3 + 头部扩展块 + Error 帧结构化）与 v0.23.0（目录键值 v3）
  是同版本断点——同集群双端同版本，升级先停全部再升；
- 消费方跨版本升级的接口映射与操作步骤见
  [docs/consumer-upgrade-notice.md](docs/consumer-upgrade-notice.md)。

## 开发与测试

```bash
make test         # gofmt 门（git 跟踪文件）+ go vet + go test -race 全量（docker Redis 自动拉起）
make test-quick   # vet + go test（无 -race）
make fuzz         # wire 帧 + 握手 v3 扩展对象原生 fuzz（各 10s）
make schema-check # 线协议兼容门禁：重生成 + 对发布基线 -check
```

仓库根门禁测试：`depgraph_test.go`（模块内导入图白名单锁定）、
`commentgate_test.go`（注释规范：无 issue/日期字面、单行 ≤100 显示列、
注释占比基线只降不升）。gofmt 门禁只扫 git 跟踪文件——并行 worktree
（`.worktrees/`）中的 WIP 不影响本仓验证。

核心验收场景（`testkit/`，`-race` 下绿；标注 redis 门禁的测试在无 Redis
环境按探活 skip）：

| 场景 | 锚点测试 |
|---|---|
| 激活竞态 | `TestAcceptance_ConcurrentActivationLoserForwardsZeroLoss`（并发首请求一赢一输）· `TestAcceptance_FreeForAll_BothNodesNoLostUpdate` |
| 输家转发 | `TestAcceptance_ConcurrentActivationLoserForwardsZeroLoss`（输家在箱消息原始字节整箱转发赢家、零丢失） |
| 租约过期重激活 | `TestAcceptance_ThreeNodeKillLeaseExpiryReactivation`（redis 门禁） |
| 迁移窗口 | `TestAcceptance_MigrationWindowZeroLoss`（换锁 CAS 窗内灌并发调用，零回绝零丢失） |
| 排空 | `TestAcceptance_DrainFourStages`（四段时序 + 门闸重路由）· `TestAcceptance_SIGTERMGracefulDrain`（子进程演练） |
| 看门狗 | `TestAcceptance_WatchdogUnbind`（人为卡死 → 强弃 + 在途调用方解绑） |
| 跨节点 RPC | `TestAcceptance_TwoNodeCrossNodeCall_RaceStable` · 握手 v3 门禁（`internal/wire`） |
| TLS/认证/metrics | `TestAcceptance_TLSHandshakeBeforeFrames` · `TestAcceptance_SchemaDigestObservationAndStrict` · `TestAcceptance_HMACFailClosed` · `TestNodeMetricsSnapshot`/`TestNodeMetricsNameContract` |
| 让出重入 | `examples/reenter`（输出序列确定，测试自检） |

## 许可证

Apache License 2.0，见 [LICENSE](LICENSE)。
