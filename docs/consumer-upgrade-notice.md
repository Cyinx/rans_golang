# rans_golang 消费方升级通知（v0.7.0 → v0.26.0 合并稿）

> 面向：wanju_agent_server 等所有 `github.com/cyinx/rans_golang` 消费方。
> 用途：一次性转达 v0.7.0 起的全部消费方可见变更；逐版本细节以仓库
> `CHANGELOG.md` 与 tag 注记为准。**尚未接入的消费方直接按 v0.22.0 接入，
> 只需读「新接入」一节。**

## v0.26.0 专项升级要点（review14 修复批，滚动不兼容）

**本版是断点版本：不支持滚动升级——先停全部节点、升二进制、再起全部。**
`node.Start` 的启动代际检查会把混版本启动拦下：K8s StatefulSet 缺省
`RollingUpdate` 会在新 pod 处失败——改 `OnDelete` 或缩容到 0 再升。
握手版本升至 v4（v3 及更早节点直接拒连）；成员键值新增协议区间字段
（`pmin`/`pmax`），未携带该字段的旧版本节点与 v4 节点互不相容。

- **Redis 前提**（成员面「一死永死」的正确性依赖）：AOF 至少
  `everysec`；`maxmemory-policy noeviction`；独立实例或独立 DB（避免
  共享实例的键逐出）。Redis 故障转移窗口内的激活/围栏仍是尽力而为。
- **宿主自持存储（如 MySQL）**：接 `node.Config.FencingFloor`——返回
  该实体已被宿主存储接受的最大纪元（0=无；每次激活前调用，报错则激活
  失败可重试）。建议升级后核对存量行纪元与当前目录纪元：下限钩子只保
  新激活取号，不回填历史行。
- **成员键丢失即批量止损（设计行为，不是故障）**：Redis 丢键/逐出（或
  进程暂停 ≥ 成员 TTL）会让该节点当轮围栏销毁全部本地实体并重取号；
  实体随访问重激活恢复（计数/状态从 Store 恢复），
  `rans.membership.resurrected` 计数可见。
- **配置迁移**：`node.KindOptions[k].Watchdog` → `node.Config.Watchdog.
  KindOverrides[k]`（前者是从未被读取的死字段，已删除）；直接构造
  `rpc.Config` 的用法删除 `Watchdog`/`CheckpointStore`/
  `CheckpointInterval`（周期任务改由 node 装配，Standalone 亦生效）。
- **第三方 `directory.NodeRegistry` 实现**：须补 `RenewNode`（比对续期
  三态）与 `Nodes`（存活全量信息）方法，`UnregisterNode` 签名加纪元
  参数（0=无条件、非 0 比较删除）；成员键值需承载 `pmin`/`pmax`。
- **新指标**：`rans.ownership.{takeover_won,stale_fenced,lost_sent,
  lost_received}`、`rans.membership.{resurrected,superseded}`；
  `rans.lease.self_fenced` 口径改为按轮计数（原按实体）。

## v0.24.0 专项升级要点（edge 退役 + 全仓审查修复批，2026-10-13）

**删了什么**：`edge` 包整体（WS 接入/会话/外部帧/EventHub/鉴权接口）、
`internal/edgesrv`、`node.Config.Edge` 字段族、`Node.EdgeAddr()`、纯接入
节点形态与 `edge_session` 系统 kind、rpc `LocalOnlyKinds` 守卫、
`node.Config.DeclaredKinds`、仪表 `rans.edge.sessions`/`rans.edge.preauth`。

**逐符号对照**（v0.23.0 → v0.24.0）：

| v0.23.0 | v0.24.0 | 说明 |
|---|---|---|
| `edge.Config` / `edge.Authenticator` / `edge.Claims` / `edge.EventHub` / 外部帧与外部码 | 删除 | 外部接入整体退役 |
| `node.Config.Edge` | 删除 | node 不再装配外部接入面 |
| `node.EdgeAddr()` | 删除 | 无监听器可寻址 |
| `node.Config.Kinds` 可空（纯接入形态） | **必填** | 装配校验收紧：`Kinds` 空 → New 报错 |
| `node.Config.DeclaredKinds` | 删除 | 零读者声明面，随纯接入形态消亡 |
| rpc `Config.LocalOnlyKinds`（internal） | 删除 | 守卫唯一生产者是 edge 会话面 |
| `rans.edge.sessions` / `rans.edge.preauth` | 删除 | 其余 11 项仪表不变 |

**迁移指引**：外部客户端接入归消费方自建网关形态（HTTP/SSE/WS ingress 直
连消费方服务，业务调用经 `rans.Client`）。已完成割接的消费方（wanju_
agent_server gateway-ingress 票 08）**零动作**——其全仓已无 `rans_golang/edge`
import；删除不涉线协议（edge 帧不在 Go/Rust 互通面），内部集群调用、目录、
迁移、排空、testkit 面全部不变。

**修复批的破坏性面（2026-10-13 全仓审查后修复，编译期即可暴露）**：

| v0.23.0 | v0.24.0 | 说明 |
|---|---|---|
| `node.LeaseConfig.MaxConcurrentDestroys` / `DestroyTimeout` | **删除** | 长期零消费、设置即静默 no-op 的死配置（止损销毁已是实例级串行 + 内部固定预算）——已核实消费方零使用 |
| `node.New` 接受 nil 工厂 / 非法 kind 键 / `Calls.Max < Default` | **构造期报错** | 此前 nil 工厂可让一帧远端消息 panic 于 TCP 读循环崩进程；Max<Default 时 Default 绕过封顶 |
| `node.NameHeartbeatLost` 常量 | 删除 | 从未注册的幽灵指标 |
| 新增 `rans.ValidKindName` | 公共 API | kind 字符集判据正本出口（与 `NewID` 共用），装配期校验可复用 |

**修复批的行为面（消费方零动作，线上自动受益）**：接管通知真实接线（误判
接管双活窗口从无界回到毫秒级 + SelfFence 兜底）、OwnerAlive 读错误保守判活
（Redis 抖动不再误抢实体）、交接窗终快照不再丢 ReenterAfter 续体写、
checkpoint 簿记换代守卫、随迁包携带源纪元（失败迁移不再留可回滚的过期包，
扩展块新字段同 VerMax 向后兼容旧端跳过）、启动窗口 epoch-0 实体迁移修复、
目录收养分支返回值=落库值（inmem/redis 双实现一致，直用 directory 的消费方
注意）、条件注销删节点粒度通配（漏填 Owner 从「删光该节点条目」变安全
no-op）、本地 Tell 的 ReenterAfter 不再 panic、acceptLoop 对 EMFILE 退避
重试、ransgen 病态输入生成期报错。

**升级路径**：v0.22.0 起的断点纪律不变——**同集群双端同版本，升级先停全部
再升**（本版含目录 Lua 行为变更，混版本对同一 epoch-0 条目行为分叉，同样
受该纪律管辖）。

## v0.23.0 专项升级要点（HR4 调用语义 + HR5 规模项，2026-10-12）

> v0.23.0 = HR4+HR5 批全部落地（预算过线/发件箱/ReenterAfter/目录键值 v3/
> 热路径分片）。**同集群双端同版本断点**（目录键值 v3 + 新消息类型
> MsgCancel/MsgOwnershipLost/MsgRedirectHint + 头部因果字段退役）——
> 操作序与 v0.22.0 同款（先停全部再升，蓝绿同效）。**允许自 v0.21.x 直跳**
> （断点性质相同，少一次割接窗口；按 v0.22.0 节完成 API 迁移后直接升）。
> 设计正本 `docs/superpowers/specs/2026-10-11-hr4-hr5-call-semantics-and-scale-design.md`。

### ① 断点性质

- **目录键值 v3 = 同集群双端同版本断点**：实体主键从 `{dir}` 集中单 slot
  改为**逐实体 hash tag**（`rans:e:{<kind>/<key>}`，目录真分片）；旧键族
  （`{dir}` 主键/索引/inc 共享 Hash/心跳键）全部废弃不做兼容读取，残留靠
  一次性清理（发版文档附清理命令）或换键空间。
- 头部因果字段退役（位值不复用）+ 新消息类型（MsgCancel=15/
  MsgOwnershipLost=16/MsgRedirectHint=17）——旧二进制不识别即断连，
  双端同版本消除该窗。
- **续租机制整体拆除**：不再有逐实体租约/心跳批——节点存活唯一权威是
  判活键（TTL 2min）；实体条目有效性由成员代际判定，死 owner 由激活 CAS
  接管自愈。

### ② Go API 破坏面（主干）

| 变更 | 消费方动作 |
|---|---|
| `node.Config.Calls.RequestTimeout` → `Calls.Default`（30s）/`Calls.Max`（10min） | 配置改字段；按需用 `rans.Timeout(d)` 选项逐调用覆盖 |
| `redis.Options.LeaseTTL` 删除（`HeartbeatTTL` 保留）；`node.Config.Tuning.CleanupInterval` 删除 | 删配置项 |
| `directory.Owner` 扩 `{Node, Epoch, Incarnation}`；`Register(ctx,id,self,expected *Owner)`/`Transfer` 新签名；`LeaseDirectory`/`Lease`/`Browser`/`LookupByClass·ByNode`/`BrowseAll`/`CleanupExpired` 删除 | 直调面按新签名改写（自定义目录实现跑 `RunRegisterSuite` 重锁） |
| `membership.LeaseKeeper` 删除；`LivenessConfig` 增 SelfFence 族（Fence/LocalIDs/OnSelfFenceLost/MemberTTL） | 续租接线删除（node 内置）；自围栏经 Liveness 自动装配 |
| `LocationCache` 构造签名加容量形参；`MaxEntries` 字段 → `SetMaxEntries`（64 分片） | 直构面适配 |
| 新增 `rans.RegisterPropagation`/`rans.Timeout`/`rans.Defer`+`Responder`/`rans.ReenterAfter` | 按需采用（全部纯新增） |

### ③ 行为变化（消费方需知）

- **请求预算全链传播**：调用方 ctx 剩余经扩展块 `budget_ms` 过线——远端
  Handle ctx 不再无 deadline；出队已过期的请求不执行（回
  `Timeout`+`ReasonExpired`，计 `rans.call.expired`）。
- **调用取消尽力传播**：调用方主动取消时目标节点收到 MsgCancel——执行中
  的 Handle ctx 被取消、在箱的出队即弃（`rans.call.canceled`）。
- **调用环立即失败**：A→B→A/自调用在准入时以 `CodeError`+`Reason=Cycle`
  回绝（不可重试）——此前卡到超时。嵌套调用深度 >64 降级直通。
- **被窃取方陈旧实例不再限时自毁**（续租面消亡）：外部改归属后，原宿主的
  本地实例止损归接管通知/闲置钝化/纪元围栏——正确性不变（Store 围栏拒旧
  纪元写），调用重路由即时生效。
- **事件扇出满即丢**：EventHub 订阅节点失联时事件丢弃计数（at-most-once
  本就是现状语义），Emit 方实体 loop 不再被慢订阅者阻塞。
- F9（client-edge-ingress 按 host 选项装配）：**rans 侧零动作**——消费方
  割接后按 node API（`node.New(node.Config{Edge: ...})`）自行实现。

## v0.22.0 专项升级要点（HR3 包结构重整与线协议断点，2026-10-10）

> v0.22.0 = HR3 批全部落地（《约定 spec》S1 包结构/命名/调用规范 + 加固
> spec H9 握手 v3）。**这是 Go API 大规模破坏 + 同集群双端同版本断点**：
> 公开包从 22 个收敛为 9 个，`host`/`remote`/`manager`/`membership`/
> `lifecycle`/`identity`/`contracts`/`wire`/`transport`/`routing`/`metrics`/
> `codegen` 等包整包删除或内部化；线协议升握手 v3。设计正本
> `docs/superpowers/specs/2026-10-08-structure-naming-conventions-design.md`
> （《约定 spec》）与 `docs/superpowers/specs/2026-10-08-runtime-hardening-design.md`
> §6.1；裁定正本 `docs/decisions/2026-10-09-hr3-package-structure.md`、
> `docs/decisions/2026-10-09-hr3-wire-v3-breakpoint.md`。

### ① 断点性质与操作序（先读这个）

- **握手 v3 + 头部扩展块 + Error 帧结构化 = 同版本断点（滚动不兼容）**：
  握手体升为版本区间协商 + 可扩展对象，v2/v3 无兼容读取。**升级窗内新旧
  二进制不可混跑——同集群双端同版本**。建议操作序（先停全部再升）：
  1. 全量停机（各节点优雅停机或直接停进程均可——Redis 残留键由 TTL 排空）；
  2. 逐节点替换为 v0.22.0 二进制；
  3. 全部替换完成后统一启动。
  蓝绿切换同效（新版本集群整组就绪后切流）。判活键 TTL 2min、租约 TTL
  ≤5min，切换完成后残留键自然排空；
- **Go API 破坏全部是编译期的**（Redis 键方案零变化——v0.21.0 的键值 v2
  + 四 Lua 原样沿用）；构建断裂在 v0.22.0 依赖落地的瞬间发生，按下文
  映射表一次性迁移，无兼容别名（开发期一次做透裁定）；
- **此后线协议恢复演进能力**：同一握手 VerMax 内向后兼容（新字段只追加、
  旧端跳过）；SchemaDigest 降级为观测（失配告警 + `rans.handshake.
  schema_mismatch` 计数，连接保持），kind 集不参与握手——新增可选字段/
  新增 kind/单节点启用 edge 不再互拒握手。需要在线判定一致性的部署可开
  `node.Config.Security.StrictSchema`（失配拒连，由部署方按需开启）。

### ② 割接协议（《约定 spec》§7.3，操作序）

1. **依赖固定**：把 go.mod 依赖固定到 v0.21.x tag（去掉 `replace =>
   ../rans_golang` 的跟随 main 形态）——v0.21.x 是断点前最后稳定版；
2. **进行中的 client-edge-ingress F9（按 host 选项装配）建议推迟**：
   F9 依赖的 host API 已在 HR3 整体替换为 `node.New(node.Config)`——推迟到
   HR3 之后直接按 node API 实现，避免迁移两次；若必须先落地，接受在本次
   割接时一并按映射表迁移；
3. **准备迁移 PR**：以 v0.22.0 为目标分支，按下文 §4.2 逐符号映射表替换；
   **重点修正 task poller 的本地 Call 误用（EX11）**——终态投递原用
   `manager.Call`（只走本地激活表，多节点形态下 workspace 归属他节点时
   投递失败），改走 `n.Client().Call(...)`（统一路由入口：本地命中走本地
   快路径，归属他节点自动路由投递，与其注释声称的「经 rans 路由」终于
   一致）；集群手工接线（目录/心跳/位置缓存/poller/remote 装配序）整段
   删除，改为一个 `node.New(node.Config{...})`；
4. **同窗合入**：rans v0.22.0 发版 → 消费方迁移 PR 合入，依赖固定到
   v0.22.0；
5. **自查**：迁移后 `go build ./...` 零旧包引用（`grep -r
   "rans_golang/\(host\|remote\|manager\|membership\|lifecycle\|identity\|contracts\|wire\|transport\|routing\|metrics\|codegen\)"`）
   且 `go generate ./...` 重生成产物入库、`make schema-check` 绿。

### ③ Go API 逐符号映射表全文（《约定 spec》§4.2 正本转录）

**契约与调用**

| 当前（v0.21.x） | 目标（v0.22.0） | 说明 |
|---|---|---|
| `identity.UUID{Kind, Inst}` | `rans.ID{Kind, Key}` | 不是 RFC 4122 UUID；Inst → Key；`String()` 格式仍为 `kind/key`，Redis 键无需迁移 |
| `identity.New(kind, inst)` | `rans.NewID(kind, key)` | 校验 kind 字符集（规则不变） |
| `UUID.AsKey()` | `ID.String()` | 实现 fmt.Stringer |
| `manager.Entity` | `rans.Entity` | 方法集不变 |
| `manager.EntityFactory func() Entity` | `rans.Factory func(id rans.ID) rans.Entity` | 工厂拿到身份；必须无副作用 |
| `manager.MigrationCarrier{CaptureSnapshotBytes, RestoreFromSnapshotBytes}` | `rans.Snapshotter{Snapshot() ([]byte, error); Restore([]byte) error}` | 迁移、钝化、checkpoint 共用 |
| `manager.Call`、`remote.Call`、`remote.CallOpts` | `rans.Client.Call(ctx, id, msg, opts...)`（经 `Node.Client()` 或 `rans.ClientFrom(ctx)` 获得） | **删除本地专用入口**（EX11） |
| `remote.SendOneWay`、`remote.SendOneWayOpts` | `rans.Client.Tell(ctx, id, msg, opts...)` | actor 惯用动词 |
| `routing.InvokeOptions{Idempotent}` | `rans.Idempotent()`（CallOption） | |
| `codegen/runtime.Caller`、`edge.Caller` | `rans.Client` | 同一概念只留一个名字 |
| `pawnite.Fencing(ctx)` | `rans.Incarnation(ctx) (uint64, bool)` | 概念名即令牌名 |
| `contracts.StatusCode` | `rans.Code` | 线上数值不变 |
| 全部调用错误 | `*rans.Error{Code, Reason, …}` + 哨兵（`errors.Is` 判 `rans.ErrTimeout`/`ErrUnavailable`/`ErrNotFound`；`CodeOf`/`ReasonOf` 取分类） | 本地与远端同构；Handle 错误在投递边界折叠，调用方不可 Unwrap 到实体原始错误 |
| `ranslog.SetLogger` | `rans.SetLogger` | |

**组装与管理**

| 当前（v0.21.x） | 目标（v0.22.0） | 说明 |
|---|---|---|
| `manager.New(dir, node, kinds)` + `*manager.Manager` 句柄 | `node.New(node.Config{Standalone: true, …})` + `*node.Node` | 单节点无传输形态（消费方当前的生产缺省） |
| `host.New(opts...)`、`*host.Host` | `node.New(node.Config)`、`*node.Node` | 结构体配置（见下「host 选项」表） |
| `remote.New(remote.Config)`、`*remote.Remote` | `node.New(node.Config)`、`*node.Node` | 字段对应见同表 |
| `Host.Start`、`Host.Run` | `Node.Start`、`Node.Run` | |
| `Host.Shutdown`、`Remote.Drain`、`Manager.Shutdown` | `Node.Shutdown(ctx)` | 优雅停机（排空） |
| `Remote.Stop` | `Node.Close()` | 立即停止不排空；与 Kill 的区别见《约定 spec》§5.5 |
| `Host.Kill` | `testkit` 内的 Kill（经 `internal/testhooks`） | |
| `Remote.Addr()`、`Host.Addr()` | `Node.Addr()` | |
| `Manager.Destroy`、`Manager.DestroyCtx` | `Node.Destroy(ctx, id) error` | 只作用于本地实例，非本地返回 `Reason=NotLocal` |
| `Manager.MigrateAsync`、`Remote.MigrateAsync` | `Node.Migrate(ctx, id) error` | 去掉 Async；同上 NotLocal 语义 |
| `Manager.SetKindIdlePolicy`、`OffloadIdleAsync`、`lifecycle.RebalancerService` | `node.Config.KindOptions[k].Idle` + `node.Config.Rebalance` | 自动执行，不再暴露手动入口 |
| `lifecycle.WatchdogConfig`、`WatchdogParams`、`KindOverrides` | `node.Config.Watchdog` + `node.Config.Watchdog.KindOverrides[k]` | |
| `remote.LocalOnlyKinds` | internal（启用 edge 时 node 自动设置） | |
| `remote.Placement`、`remote.KindPlacement`、`Manager.SetPlacement` | `node.Placement`（首次激活与迁移选点共用） | 候选集由 node 计算，自定义策略实现 `Pick(kind, self, candidates)` |
| `membership.NewHeartbeatService` 及其 interval | internal；间隔为 `node.Config.Lease.Interval` | |
| `membership.NewChangePoller`、`View`、`RandomPlacement`、`KindPlacement`；`routing.NewLocationCache` | internal（node 恒装配；随机放置公开为 `node.RandomPlacement`） | |
| `redis.Directory` 的 `BrowseAll`、`NodeAddrResolver`、`Close`、`NodeIDs` | node 以 `Directory.Redis` 自建目录时由 node 持有与关闭；注入目录（`Directory.Injected`）时由调用方持有，适配器方法照常可用 | |
| `inmem.New()` | `node.Config.Directory` 零值即 inmem；也可注入 | |
| `metrics.Snapshot`、`metrics.Registry` | `Node.Metrics()` + `internal/metrics` | 指标名清单见 README「安全与观测」参数表 |
| `metrics.SetMeterProvider` | 直接使用 `otel.SetMeterProvider` | 原函数只是透传 |
| `tcp.LoadHMACSecretFromEnv/File` | `node.LoadHMACKeyFromEnv/File` | |

**host 选项 → node.Config**

| host 选项 | node.Config |
|---|---|
| `WithNodeID`、`WithClusterName`、`WithListenAddr` | `NodeID`、`ClusterName`、`ListenAddr` |
| `WithKinds`、`WithDeclaredKinds` | `Kinds`、`DeclaredKinds` |
| `WithDirectory`、`WithRedis` | `Directory.Injected`、`Directory.Redis` |
| `WithHMAC`、`WithHMACFromEnv`、`WithNoHMAC`、`WithTLS` | `Security.HMACKey`（`node.LoadHMACKeyFromEnv`）、`Security.Insecure`、`Security.TLS` |
| `WithCheckpointStore` | `Store` |
| `WithRequestTimeout` | `Calls.Default`（HR4 起拆为 `Calls.Default`/`Calls.Max`，逐调用预算用 `rans.Timeout(d)` 选项） |
| `WithHeartbeatInterval` | `Lease.Interval` |
| `WithShutdownBudget` | `ShutdownBudget` |
| `WithPlacement` | `Placement` |
| `WithWatchdog` | `Watchdog` |
| `WithEdge` | `Edge` |
| `WithMetrics` | 删除（OTel 全局 provider + `Node.Metrics()`） |
| `WithSchemaShapes` | 删除（生成代码自动登记形状） |
| `WithResolver` | `TestHooks`（只有进程内多节点测试需要） |
| `WithOnActivated`、`WithOnEvicted` | 删除（现有用途是心跳 Track/Untrack，已由 node 内部完成） |

**编码、端口与外部接入**

| 当前（v0.21.x） | 目标（v0.22.0） | 说明 |
|---|---|---|
| `wire.RegisterValue`、`Serializer`、`EncodeValue`、`DecodeValue` | `codec.Register`、`codec.Serializer`、`codec.Encode`、`codec.Decode` | 消费方手写序列化器在用 |
| `wire.Writer`、`Reader`、`WireType`、`ErrTruncated`、`ErrMalformed` | `codec` 同名导出 | 生成代码在用 |
| `wire` 其余（帧、头部、握手、页池、因果） | `internal/wire` | |
| `contracts.MessageType`、`MessageFlag` | `internal/protocol` | 不公开 |
| `directory.Directory`、`LeaseDirectory`、`TransferDirectory`、`Browser`、`IncarnationSource` | `directory.Directory`（H7 后的合并端口）+ 可选扩展接口 | 取号已并入 `Register`（v0.21.0） |
| `directory.Location{NodeID}` | `directory.Owner{Node rans.NodeID, Incarnation uint64}` | v0.21.0 已迁 |
| `directory.NodeRegistry`、`directory/redis.Directory`、`Options` | 保留 | |
| `checkpoint.Store`、`InMemory`、`ErrStaleIncarnation` | 保留 | 包名保留 |
| `edge.ListenerConfig` | `edge.Config`（经 `node.Config.Edge` 启用） | |
| `edge.New`、`Bind`、`NewListener`、`NewSessionFactory` | internal（由 node 装配） | 签名含内部化类型 |
| `edge.NewEventHub()`、`NewEventHubWithRingCap(n)`、`SetSource`、`SetCaller` | `edge.NewEventHub(id rans.ID, ringCap int)`（0 取缺省）；Emit 经 `rans.ClientFrom(ctx)` 取调用面 | 删除两段式注入 |
| `edge.Authenticator`、`Claims`、`ClaimsFrom`、帧类型与外部码 | 保留 | |
| `codegen/ransgen` | `cmd/ransgen` | `go run github.com/cyinx/rans_golang/cmd/ransgen` |
| `testkit.*` | 保留，改为基于 `node` | |

**生成物面**（`go generate` 全量重生成并入库）：wrapper 实现 `rans.Entity`
（用户实体实现 `rans.Snapshotter` 时透传）；工厂表 `Kinds map[string]rans.Factory`
（工厂 `func(id rans.ID) rans.Entity`，直接填 `node.Config.Kinds`）；代理
`<Entity>Client{C rans.Client}`（`//rans:idempotent` 方法默认追加
`rans.Idempotent()`）；序列化器注册 `codec.Register`；`RansShapes` 在 init
自动 `codec.RegisterShapes`（不再经配置传入）；另生成 `rans_schema.json`
供 CI 兼容检查。

### ④ 行为变化与新增面摘要

行为变化 6 条（逐条测试锚定见 `CHANGELOG.md` v0.22.0 节）：①一切调用经
路由（本地专用入口删除，EX11）；②本地/远端错误同构 `*rans.Error`，Handle
错误投递边界折叠；③Error 帧结构化（线上）；④未知 kind 本地远端一致
`NotFound`+`UnknownKind`；⑤管理操作非本地 `NotFound`+`NotLocal` 不再静默
成功；⑥配置构造一次给全 + 非 Standalone 安全零值拒绝启动（fail-closed）。

新增面：`rans` 契约包全量（ID/Client/Error/…）、`node` 组装包、`codec`
公开编码面、`StrictSchema` 严格摘要门闸、`ransgen -check` 兼容检查、
导入图门禁。

## v0.21.0 专项升级要点（HR2 register-first 与实例级所有权，2026-10-09）

> v0.21.0 = HR2 批全部落地（H6 交接窗/排空 + H7 register-first/实例级
> 所有权 + H8 终局归一 + 键值 v2）。本节随发版**收口**（递进稿的「操作序
> 与门槛随发版说明补全」两项在本节补齐）。语义裁定正本：
> `docs/decisions/2026-10-09-hr2-register-first.md`、
> `docs/decisions/2026-10-09-hr2-unavailable-retry.md`。

### ① 断点性质（先读这个）

- **Redis 目录键值 v2 + 第四 Lua（luaRegister）= 同集群双端同版本断点**：
  主键值升为 `{"nodeId":N,"inc":K,"v":2}`（K 即激活纪元），不做 v1 兼容
  读取。v1 残留条目由 luaRegister 按固定语义处理（非兼容读取）：`nodeId`
  匹配 → 自指向收养覆写为 v2（升纪元）；不匹配 → 输，返回的 owner 只含
  节点号（inc=0）；其余残留由租约 TTL（≤5min）腾位。**升级窗内新旧二进制
  不可混跑**——建议操作序（二选一）：
  1. **全量停机再升**：停全部节点 → 全部替换为 v0.21.0 二进制 → 启动；
  2. **蓝绿切换**：新版本集群整组就绪（独立 Redis 键空间或清空残留）后
     切流，旧组整体退役。
  判活键 TTL 2min、租约 TTL ≤5min，切换完成后残留键自然排空；
- **线协议/握手/节点成员键零变化**（v0.21.0 面内 wire/contracts/transport
  无 diff；节点成员键值仅**增** `draining` 字段——`omitempty`，旧读端
  透明）；Go API 破坏全部是编译期的。

### ② 语义变化（对照 API 清单前先理解）

- **register-first 激活次序**：目录注册（原子取号）先于实体装载；仲裁输家
  **零用户代码**（不跑恢复/Activate/Deactivate，直接整箱转发赢家）；激活
  失败（恢复失败/Activate 失败）回绝码为 `StatusUnavailable`——本地错误链
  双哨兵 `ErrActivationFailed`+`ErrBusy`（`errors.Is` 两判成立），调用方可
  重试（Unavailable→无条件重试的分类面已随本版本落地，
  `docs/decisions/2026-10-09-hr2-unavailable-retry.md`）；
- **归属升为实例级** `directory.Owner{Node, Incarnation}`（替代
  `Location{NodeID}`）：注销/换锁/续租按 {node, inc} 同时匹配——同节点新
  实例不被旧实例的迟到操作误伤；续租按实例纪元跟踪，旧纪元实例的迟到续租
  不重置新实例租约（failed 集 → 本地自毁只伤旧实例）；
- **残留自愈**：目录里无实例的残留条目（进程重启孤儿/注销残留/v1 旧条目）
  由下次激活的**自指向收养（升纪元覆写）**+ 租约 TTL 清理——cleanup 不再
  做节点粒度兜底注销（owner 未捕获的终局零目录调用）；
- **陈旧清理异步化**：本地表 miss 不再等待目录注销完成（读循环零目录 IO）；
- **迁移**：换锁走实例级 CAS（期望归属 Owner，成功即升纪元）；目标端恢复
  即落库（收窄旧纪元写窗）；裸 inmem 目录亦可迁移——
  `ErrMigrationUnsupported` 配置形态消亡（自定义目录缺换锁能力面 = 迁移输，
  `ErrMigrationLost` 错误链留痕）；
- **「Activate/Deactivate 可安全按令牌读写」从前提条件变为现实验**
  （v0.20.0 ②中「6.1 把装载挪进 Activate 后自动获得该保证」的预期落地）：
  交接窗保证快照先于 Deactivate、终快照零丢失（EX9/EX10）——消费方可在
  Activate 装载状态、在 Deactivate/Handle 按 `rans.Incarnation(ctx)` 令牌
  条件写自有存储，优雅停机/钝化不再丢已确认写；
- **6.1 开工门槛达成**（ISSUES V3③）：6.1（MySQL checkpoint.Store、钝化、
  滚动 drain）的前置「HR1+HR2 全部落地」已满足——钝化面 =
  `Manager.Passivate(ctx,id)`（闲置回收 CollectPassivate 已接线）；排空 =
  `host.Shutdown` 内置新停序（手工装配方经 `remote.Drain` +
  `WithTeardownHead` 插入宿主组件收口）；checkpoint.Store 实现仍按下文
  v0.16.0 迁移指南的纪元围栏签名。

### ③ Go API 编译破坏清单（自 `git diff v0.20.0..HEAD` 实证导出）

**directory 面**（`directory`/`inmem`/`redis`/`contract`）：

| 变更 | 消费方动作 |
|---|---|
| `directory.Location` 类型删除 → `Owner{Node uint16; Incarnation uint64}` | 类型引用改名 + 字段 `NodeID`→`Node`、补纪元 |
| `Directory.TryRegister(ctx,id,Location)(bool,error)` 删除 → `Register(ctx,id,node uint16,leaseMs int64)(won bool, inc uint64, owner Owner, err error)` | 调用点改签名（输家分支经 owner 转发；leaseMs 传 0 即目录配置租约） |
| `Directory.UnregisterIfOwner(ctx,id,nodeID uint16)` → `(ctx,id,owner Owner)` | 传实例级 Owner（node+inc 同时匹配才删） |
| `Directory.Lookup` 返回 `(Owner, bool, error)` | 读侧解构改字段名 |
| `IncarnationSource` 接口与 `NextIncarnation` 删除 | 取号并入 Register——纪元从 Register 返回值取 |
| `LeaseDirectory.HeartbeatBatch(ctx,[]identity.UUID,owner uint16)` → `(ctx,[]Lease,owner uint16)`；新类型 `Lease{ID, Incarnation}` | 续租条目带纪元 |
| `TransferDirectory.TryTransferOwnership(ctx,id,expectedOwner uint16,newLoc Location)(bool,error)` → `(ctx,id,expected Owner,newNode uint16)(newIncarnation uint64, ok bool, err error)` | 换锁调用点改签名（期望归属=本实例 Owner） |
| `NodeRegistry.RegisterNode(ctx,node,addr string,kinds []string)` → `RegisterNode(ctx,node,NodeInfo{Addr, Kinds, Draining})` | 自定义 NodeRegistry 实现/测试桩改签名（v0.19.0 签名整体替换；节点键值增 draining 字段，旧读端透明） |
| 契约套件：`RunSuite`/`RunIncarnationSuite` 删除；`RunLeaseSuite` 未删——newDir 形参面改为返回 `LeaseSuiteDir`（LeaseDirectory+TransferDirectory） | 自定义目录实现改用 `RunRegisterSuite`/`RunLeaseSuite`（实例级语义）锁定；实现 `LeaseDirectory` 的同时需补 `TransferDirectory` 面方可过租约套件 |

**membership 面**：

| 变更 | 消费方动作 |
|---|---|
| `HeartbeatService.Track/Untrack` 增 `inc uint64` 形参 | 直调方补纪元 |
| `HeartbeatConfig.OnLost` → `func(ctx, id, inc uint64) error` | 回调内按 {id, inc} 匹配销毁（`mgr.DestroyCtxInstance`） |
| `NewHeartbeat`：Dir 须实现实例级续租面（`HeartbeatBatch([]Lease,…)`），缺失构造报错 | 自定义租约目录补面 |
| 续租接线退役：`mgr.SetOnActivated(hb.Track)` → `mgr.SetLeaseTracker(hb.Track, hb.Untrack)`（Track 前移到注册成功点） | 手工接线改形式（见下） |

**manager / routing / pawnite 面**：

| 变更 | 消费方动作 |
|---|---|
| 新增 `Manager.DestroyCtxInstance(ctx,id,inc)`、`Manager.SetLeaseTracker(track, untrack func(id, inc))` | 失租销毁/续租接线采用 |
| 哨兵删除：`manager.ErrLostArbitration`、`manager.ErrMigrationUnsupported` | 判 `*ArbitrationLostError`（输家同步窗）/ `ErrMigrationLost`（迁移输，链携带输因） |
| `ArbitrationLostError.Winner` 与 `SetOnArbitrationLost` 回调：`directory.Location` → `directory.Owner`（`WinnerNode()` 方法不变） | 回调签名与字段访问适配 |
| `routing.LocationCache.Get/PutPositive`：`directory.Location` → `directory.Owner` | 形参/返回适配 |
| `lifecycle.IdleCollector` 注入面替换：`Snapshot`/`Destroy` 两 func 字段删除 → `Passivate func(ctx, id) error`（钝化收敛为交接窗单调用） | 直接构造 `IdleCollector` 的消费方改注入面（规范构造点在 `manager/idle.go` 内部——经 `host` 装配零改动） |
| pawnite 新增（非破坏）：`NoteArbitrationLost(winner uint16)`、`TerminalReason` 六值枚举（TerminalRetired/TerminalMigrated 等，`SkipDirUnregister` 判据改经终局原因） | 无动作；强弃/迁移自定义终局观测可选用 |

### ④ 消费方自查清单

1. grep `TryRegister`/`NextIncarnation`/`IncarnationSource`/`Location`（directory
   语境）——直调面逐一改为 `Register`；取号面删除；
2. 自定义 `Directory` 实现：按新三方法面重写（Register 原子取号+自指向收养
   升纪元+实例级注销），跑 `directory/contract.RunRegisterSuite` 锁定；
3. 手工心跳接线（不经 `host`）：`mgr.SetLeaseTracker(hb.Track, hb.Untrack)`
   + `OnLost` 内 `mgr.DestroyCtxInstance(ctx, id, inc)`；
4. 判 `ErrLostArbitration`/`ErrMigrationUnsupported` 的分支改判新哨兵面；
5. **经 `host` 包装配的消费方编译面无感**（内部接线已随迁）——仍须读 ①
   确认升级操作序（键值 v2 断点与装配方式无关）。

## v0.20.0 专项升级要点（HR1 止血，2026-10-08）

> v0.20.0 = 2026-10-08 架构评估 HR1 批（ISSUES H1–H5，设计正本
> `docs/superpowers/specs/2026-10-08-runtime-hardening-design.md` §4）。
> **线协议与 Redis 键方案零变化，可滚动升级**；破坏性变更在 Go API 编译面。

### ① 心跳接线必须迁移（编译破坏，一次改完）

`membership.NewHeartbeatService(dir, self, interval, onFailure)` 整体替换
为 `membership.NewHeartbeat(cfg HeartbeatConfig) (*HeartbeatService, error)`。
消费方 `apps/agent-server/run.go:169` 现行 onFailure 里的**同步 `Destroy`
正是 ISSUES V3①（H1：同步 Destroy 冻结心跳环——续租与节点键停摆，批量
丢租时串行累加把节点拖出成员视图；该风险在现行部署上已存在）——迁移即
修复**，消费方不再需要（也不允许）在回调里自己做异步化：

```go
hb, err := membership.NewHeartbeat(membership.HeartbeatConfig{
    Dir:      dir,
    Self:     selfNode,
    Interval: interval,
    Fence:    mgr.Fence, // 失败集同步逐实体关准入（租约环上，毫秒级返回）
    OnLost: func(ctx context.Context, id identity.UUID) error {
        return mgr.DestroyCtx(ctx, id) // 有界异步销毁（worker 池 + 预算）
    },
})
```

- 有界并发（`MaxConcurrentDestroys` 缺省 8）、单次预算（`DestroyTimeout`
  缺省 30s）、同 id 在飞去重——全部内建，onFailure 时代的手工异步化/限流
  一并删除；
- `Fence` 必须接（`mgr.Fence`）：任何销毁动作之前先关准入，租约失守后
  新请求立即被 `StatusUnavailable`（"lease lost"）回绝、在途不受影响；
- `SelfFence` 本地租约自检（缺省 `max(LeaseTTL−2×Interval, Interval)`）：
  本节点续租连续不可确认超阈值即本地 Fence 全部实体止损并计数
  `rans.lease.self_fenced`——不配置即按缺省启用；
- 经 `host` 包装配的消费方**无感**（host 内部接线已随迁）。

### ② 有状态实体语义变化（wanju_agent_server）

- **Store 读失败 → 激活失败**（`manager.ErrRestoreFailed`，`errors.Is`
  可判，**可重试**）：不再以空状态上线、再由首轮 checkpoint 以更高纪元
  覆盖持久数据（评估 EX4：`"1000"` → `"0"`）。消费方对激活失败的既有
  重试路径即可覆盖，无需新代码；
- **Deactivate 只与成功的 Activate 配对**：激活失败实例、仲裁输家、
  Activate 成功但注册前被 Destroy 抢占的僵尸实例，都**不再调用用户
  Deactivate**。消费方当前 Activate 为空实现不受影响；6.1 把装载挪进
  Activate 后自动获得该保证——届时若 Activate 申请了资源，回收要么放
  Deactivate 前的快照面（激活失败路径会走到），要么接受失败路径无回收
  窗口；
- **工厂必须无副作用**：`EntityFactory` 在创建锁外执行、可能在激活竞争
  中被调用后整个丢弃——工厂里不得做资源申请或外部写；
- 次要：从 Store 恢复的实例首轮 checkpoint 不回写（刚读出的状态不会被
  原样写回）；激活取号失败从同步错误改为异步激活失败
  （`ErrActivationFailed`，可重试——与目录不可用同语义）。

### ③ 滚动升级与后续门槛

- **v0.20.0 可滚动升级**：线协议与 Redis 键方案零变化、无混跑断点、
  无需重跑 `go generate`（Go API 破坏是编译期的，逐节点替换二进制即可）；
- **HR2（v0.21.0）将是同集群同版本断点**（交接窗 + 键值格式 v2 +
  Lua 语义，见 hardening spec 兼容表）——届时无滚动窗口；
- 消费方 **6.1（MySQL checkpoint.Store、钝化、滚动 drain）的开工前置是
  HR1+HR2 全部落地**（ISSUES V3③；其中 checkpoint.Store 实现仍按下文
  v0.16.0 迁移指南的纪元围栏签名）。

## 一句话总结

**同集群双端必须同版本升级**（握手指纹与 Redis 键方案两处断点，v0.19.0 再
添节点成员键值格式一处），升完重跑 `go generate ./...`；LLM 类慢实体必须配
`WatchdogConfig.KindOverrides` 放宽激活超时，否则会被缺省看门狗强弃；强弃会
**取消在飞钩子的 ctx**（v0.15.0 起）——实体的 Deactivate/Activate 应消费
ctx 尽早返回。**v0.20.0 无新增混跑断点**（线协议/键零变化，可滚动升级），
但心跳构造 API 编译破坏、接线必须迁移（见 v0.20.0 专项①）；
**v0.21.0 恢复混跑断点**（目录键值 v2 + 第四 Lua——双端同版本、先停全部
再升，见 v0.21.0 专项①）。

## 新接入（首次使用 v0.21.0）

装配优先走 `host` 包（v0.17.0+）：`host.New(opts...)` 吸收 README QUICKSTART
手工装配序（目录/manager/心跳+租约自毁/位置缓存+变更轮询/kind 感知放置/地址
解析/启动停机次序），Redis 形态 `WithRedis` 全量接线，外部接入面经
`WithEdge`；手工装配序仍是其正本（README「最小装配清单（QUICKSTART）」，
arbitration 示例为可运行形态）。有状态实体必须配 `CheckpointStore`。

## 已接入方升级清单（旧 → v0.21.0）

1. `go.mod` 指向 v0.21.0（replace 保持本地路径的，指向新 tag）；
2. **重跑 `go generate ./...`** 重新入库 `rans_gen.go`（v0.8.0 握手指纹
   纳入零字段消息，生成物清单新增裸类型名条目——不重跑则与新版对端互拒
   握手；v0.19→v0.21 生成物面未变，此步可跳过，跨大版本升级仍建议执行；
   v0.21.0 生成器新增快照面透传——需要该面的生成形态实体重跑一次）；
3. 自查下表破坏性 API 引用并修正——**checkpoint.Store 实现方（含 JSONL/MySQL 计划）按 v0.16.0 新签名实现并做条件写**（迁移指南见下节）；
   **directory.NodeRegistry 实现方（含测试桩）按 v0.21.0 新签名实现**
   （`RegisterNode` 收 `NodeInfo{Addr,Kinds,Draining}` 三元组）；
4. **按 kind 配置看门狗**（v0.7.0 起 remote.Start 后看门狗缺省开跑不可
   关：激活/卸载 60s、执行 120s——LLM 类慢实体必须
   `WatchdogConfig.KindOverrides` 放宽 `ActivationTimeout`）；
5. Redis 侧：旧键（`rans:dir:*` 等）靠 TTL 自然消失，无需迁移；新旧实现
   互不可见，**同集群双端同版本**（滚动=停机窗口全量替换，无灰度——
   v0.19.0 节点成员键值格式与 v0.21.0 目录键值 v2 断点同此要求，见兼容性
   矩阵）。

## 破坏性 Go API 变更逐条

| 变更 | 版本 | 消费方动作 |
|---|---|---|
| `pawnite.DestroyingSince()` → `StateSince()` 改名 | v0.7.0 | 引用改名 |
| `remote.NewGateway/NewGatewayWithSweep` 去掉 self 首参 | v0.9.0 | 签名适配 |
| `tcp.Manager.SetOnLost` 移除（节点丢失单轨到 `transport.Handler.OnConnectionLost`） | v0.9.0 | 改接 Handler |
| `remote.Dispatcher.IsDraining` 移除 | v0.9.0 | 删引用（门闸由 Drain 管理） |
| `manager.Call` 主动取消不再报 `ErrTimeout`（透传 `context.Canceled` 包装；deadline 仍 `ErrTimeout`） | v0.9.0 | 取消分支的错误判别 |
| `remote.Start` 失败即终态（内部已 Stop，需重建实例重试） | v0.9.0 | 失败处理路径重建 |
| `directory/redis.Directory.NodeIDsByHeartbeat` 移除 → `NodeIDs`（`directory.NodeRegistry`） | v0.10.0 | 引用改名 |
| `NodeRegistry` 接口增 `NodeAddrs`（自定义实现方需补） | v0.11.0 | fake/实现补方法 |
| Redis Registry 键方案变更（总表 `rans:{reg}:nodes` + 判活键） | v0.12.0 | 无 API 动作；混跑断点 |
| 实体目录键族 `{dir}` hash tag | v0.13.0 | 无 API 动作；混跑断点 |
| `HeartbeatService.Run`/`ChangePoller.Run` 恰一次（重入立即返回） | v0.14.0 | 重启场景构造新实例 |
| **`checkpoint.Store.Put` 增 `incarnation uint64` 形参**；新哨兵 `checkpoint.ErrStaleIncarnation` | v0.16.0 | Store 实现改签名 + 条件写（见迁移指南） |
| **`directory.NodeRegistry.RegisterNode` 增 `kinds []string` 形参**；接口另增 `NodesByKind(ctx, kind)` | v0.19.0 | 实现方/测试桩改签名 + 补方法（传 nil kinds 即旧行为；本仓库 redis 已实现） |
| 节点成员键值格式：裸地址 → JSON `{addr,kinds}`（旧值为坏值，报错不跳过） | v0.19.0 | 无 API 动作；**混跑断点**——全集群同版本滚动（直读 `rans:{reg}:node:*` 原始值的脚本需按 JSON 解析，经接口消费零改动） |
| `host.Kill()` 含 edge 硬停（无 Bye 无 Drain，会话与连接同死） | v0.19.0 | 依赖 Kill 后 edge 面仍活的消费方改用 Shutdown |
| **`membership.NewHeartbeatService(...)` → `membership.NewHeartbeat(HeartbeatConfig)`**（同步 onFailure 语义删除；Fence/OnLost/MaxConcurrentDestroys/DestroyTimeout/SelfFence 见 v0.20.0 专项①） | v0.20.0 | 心跳接线迁移（编译破坏；host 装配方无感） |
| `manager.EntityFactory` 契约收紧：必须无副作用，可能在激活竞争中被调用后丢弃 | v0.20.0 | 工厂内不得申请资源/外部写 |
| **目录面实例化**：`Location` 删除→`Owner{Node,Incarnation}`；`TryRegister`→`Register`（取号原子域，返回 `(won,inc,owner,err)`）；`UnregisterIfOwner`/`Lookup`/`HeartbeatBatch`/`TryTransferOwnership` 改实例级签名；`IncarnationSource`/`NextIncarnation` 删除；契约套件 `RunSuite`/`RunIncarnationSuite` 删除 | v0.21.0 | 见 v0.21.0 专项③（自定义目录实现按新面重写并用 `RunRegisterSuite` 锁定） |
| **`membership` 实例化**：`Track/Untrack`/`OnLost` 增 `inc`；`NewHeartbeat` 要求 Dir 实现实例级续租面；接线 `SetOnActivated(hb.Track)` 退役→`SetLeaseTracker(hb.Track, hb.Untrack)` | v0.21.0 | 见 v0.21.0 专项③（手工接线改形式；host 装配方无感） |
| `manager`：新增 `DestroyCtxInstance`/`SetLeaseTracker`；哨兵 `ErrLostArbitration`/`ErrMigrationUnsupported` 删除；`ArbitrationLostError.Winner`/`SetOnArbitrationLost` 改 `directory.Owner` | v0.21.0 | 失租销毁改实例级面；哨兵判别改 `*ArbitrationLostError`/`ErrMigrationLost` |
| 目录键值 v2 + 第四 Lua：`{"nodeId":N,"inc":K,"v":2}`，无 v1 兼容读取 | v0.21.0 | 无 API 动作；**同集群同版本断点**（见 v0.21.0 专项①；操作序随发版说明） |

## 行为增强（非破坏，但要知道）

- **incarnation 纪元围栏（ISSUES X10，v0.16.0）**：manager 激活时从目录取
  单调纪元（redis=`rans:{dir}:inc` 共享 Hash，无 TTL），三个快照写点
  （checkpoint/钝化/迁移）全部携带——被强弃 incarnation 的晚落 Put 由
  Store 侧拒绝（`ErrStaleIncarnation`）；实体 Handle 与 Activate/Deactivate
  钩子的 ctx 都携带纪元（`rans.Incarnation(ctx)` 读取），宿主直写自有存储
  时以令牌做条件写。执行卡死强弃的 zombie 不启动 Deactivate；迁移卡死
  强弃后迟回 Won 的续体会启动 Deactivate 且收已取消 ctx（消费端三面合流
  select 依赖的落点）；双实例并发窗口上界=该 kind 生效停驻阈值
  （KindOverrides 放宽则放大）+ 宿主收尾宽限；
- **看门狗强弃取消面（ISSUES X9）**：强弃置终态的同时取消在飞
  Activate/Deactivate 钩子的 ctx（优雅路径永不取消；强弃时钩子在飞则
  Warn 留痕）。宿主约定：钩子应消费 ctx 尽早返回，不能快速返回的收尾
  分段放弃——否则强弃后新 incarnation 与 zombie 并发的窗口无界
  （v0.15.0）；
- 发送段受请求预算约束、reply/Forward 5s 预算（v0.9.0）；
- `SkipField` 深度 64 上限：>64 层嵌套的畸形帧现在 `ErrMalformed`（正常
  生成物远达不到）（v0.9.0）；
- wire 注册表硬错：未注册类型/未知名字返回 `wire.ErrUnregistered`（JSON
  兜底移除）、`RegisterValue` 冲突 panic——走生成物全量注册不受影响
  （v0.8.0）；
- 周期 checkpoint / 目录残骸清理随装配自动接线（v0.7.0）；
- 新增哨兵：`remote.ErrPendingCapacity`/`ErrDuplicateMessageID`、
  `tcp.ErrManagerClosed`（v0.9.0）；
- 新增 `StopAndWait(ctx)`（HeartbeatService/ChangePoller，M5）：停机确定性
  收口原语，宿主停序建议切换（v0.14.0）；
- Redis Cluster 支持：`redis.Options.Addrs` 传种子列表即 cluster/哨兵形态
  （`{reg}`/`{dir}` 双原子域已真集群实测；DB 须 0）（v0.11.0–v0.13.1）。

## v0.27.0 升级要点（断点：先停全部再升）

- **停机升级**：StatefulSet 缺省 RollingUpdate 会在启动代际检查处失败——改 `OnDelete` 或缩容到 0 再升；握手 v4→v5，混版本集群互拒。
- **Go API 迁移映射**（全部为编译期破坏，改完即完）：

| 旧 | 新 |
|---|---|
| `nc.Lease.Interval` / `nc.Lease.SelfFence` | `nc.Liveness.Interval` / `nc.Liveness.SelfFence`（`LeaseConfig` → `LivenessConfig`） |
| `nc.Outbox.Shards` | 删除（按目标分队列，无分片参数） |
| `rans.ReenterAfter(ctx, work, then)` 的 `then func(v, err)` | `then func(ctx, v, err)`（续体 ctx 可用于出站调用；其内 `rans.Defer` panic——终局已由运行时收场） |
| 指标名 `rans.lease.self_fenced` | `rans.membership.self_fenced` |

- **Redis 配置**：哨兵（`MasterName`）、ACL（`Username`）、TLS
  （`TLSConfig`）、单种子 Cluster（`Cluster: true`）现可配；ctx 预算
  恒约束 Redis IO（慢命令被调用方 deadline 截断——此前最坏十余秒）。
- **墓碑 TTL**（可选）：`TombstoneTTL > 0` 需纪元下限来源
  （`HighWaterReader` Store 或 `FencingFloor`——`node.New` 校验拒绝）；
  容量估算 ≈150 B × 曾激活实体数。
- **宿主围栏**：自持存储（如 MySQL）直写的围栏契约与正确 SQL 配方见
  README「宿主围栏契约」节——原升级通知中的 `ON DUPLICATE KEY UPDATE`
  配方不可用（不支持 WHERE，无法表达严格小于拒绝）。
- **互通前提**：与 rans_rust 的互通自 v0.20.0 后不成立（当前不互通）；
  Go 侧金向量基准见 `testdata/vectors/`。

## 兼容性矩阵

| 面 | 旧 ↔ 新 | 要求 |
|---|---|---|
| 握手 schema 指纹 | v0.7.x ↔ v0.8+ | **互拒**（双端同版本 + 重跑 go generate） |
| Redis 键方案 | <v0.10 ↔ v0.13+ | 互不可见（双端同版本；旧键靠 TTL 消失） |
| 节点成员键值格式 | <v0.19 ↔ v0.19+ | **互不可见**（旧值=坏值报错；全集群同版本滚动，判活键 TTL 2min 排空——v0.19.0） |
| 线协议 | v0.7–v0.21 | 零变更（MsgRetire 帧对旧端无行为变化；v0.20.0/v0.21.0 同样零变更） |
| 心跳 Go API | <v0.20 ↔ v0.20+ | **编译期破坏**（NewHeartbeat 接线迁移）；无运行时断点——可逐节点滚动升级（v0.20.0） |
| HR2 键值 v2 + 第四 Lua | v0.20 ↔ v0.21 | **同集群双端同版本断点**（无滚动窗口；操作序见 v0.21.0 专项①）——HR2 已全量落地（交接窗/排空/终局归一随 v0.21.0 交付） |

## checkpoint.Store 实现方迁移指南（v0.16.0）

旧签名 `Put(ctx, id, snapshot)` → 新签名 `Put(ctx, id, incarnation uint64,
snapshot)`，语义：**拒绝回退写入**（incarnation 小于该 id 已接受的最大值
→ 返回 `checkpoint.ErrStaleIncarnation`）；相等 = 同 incarnation 的
checkpoint 重试，允许覆盖；0 = 未围栏直通（目录无纪元源的裸形态，
rans 保持旧行为——正常装配走 redis/inmem 目录时不会出现 0）。

- **MySQL 形态（增量 5 计划）**：快照表加 `incarnation BIGINT UNSIGNED`
  列（per-id 高水位即快照行上的值），写入用条件写：`INSERT ... ON
  DUPLICATE KEY UPDATE snapshot=?, incarnation=? WHERE ...` 不直接支持
  条件的场合用 `UPDATE snapshots SET ... WHERE id=? AND incarnation <= ?`
  先行、影响行数 0 再 INSERT 回退（同 id 并发首写竞态由主键吸收）；
  拒绝路径返回包装了哨兵的错误。Delete 不回退高水位（删除不是重置）。
- **JSONL/文件形态（LocalStore 类）**：每 id 记录已见最大纪元（内存或
  元数据行），写入前比较；旧纪元写入丢弃并留痕（zombie 续体写旧文件
  的病根与 checkpoint 同治）。
- **宿主直写自有存储**（不经 checkpoint.Store 的业务写）：在 Handle/
  Activate/Deactivate 内 `inc, ok := rans.Incarnation(ctx)` 取令牌随写请求
  下发，存储侧同样条件写——迟回的 zombie Handle（慢 LLM 调用事后返回）
  携带旧纪元即被拒。

围栏取号成本：每次激活一次目录往返（节点代际启动取号一次全程携带；仲裁输家消耗
号段是正常现象（跳号无害，只求单调）。

## wanju_agent_server SSE→WS 迁移评估要点（v0.19.0，edge E3 交付面）

> 用途：导演 agent 服务将自建 HTTP/SSE 翻译层（`apps/agent-server` 的
> assembleCluster + 事件流面）迁移到 rans_golang edge 面（`host.WithEdge`）
> 的评估输入。设计正本 rans_golang
> `docs/superpowers/specs/2026-10-04-edge-hosting-design.md`；本节只对照
> 语义差异，迁移决策与排期归消费方。

### 事件环 / LastEventID 语义对照（消费方 spec §10.2 开放点）

消费方 spec §10.2 的开放点「事件源是宿主进程内存态，非宿主节点拿不到
事件流（SSE 限单入口）」由 edge E3 根治：实体事件经 EventHub 扇出为对会话
实体的 OneWay，**任意启用 edge 的节点接入都可收到事件**（rans_golang
`TestE3EAnyNodeAccessReceivesEvents` 判定锚定），不再要求客户端连上实体
宿主节点。语义对照：

| 面 | SSE 现状（消费方自建） | edge / EventHub（v0.19.0） |
|---|---|---|
| 事件游标 | HTTP `Last-Event-ID` 头 | `Subscribe` 帧 `LastEventID` 字段（0=新订阅不回放） |
| 事件环 | 消费方自建环（1024 先例） | EventHub 内建环（缺省 1024，`NewEventHubWithRingCap` 可调），满淘汰最旧 |
| 游标不可追 | 消费方自定义 | `SubscribeAckMsg.ReplayInvalid=true` 零补发——三形态：游标纪元不同（实体重激活后环从 0 起）/ 早于环基线（溢出淘汰）/ 超前前沿。客户端全量重建（无游标重订阅），与 SSE 现状边界一致 |
| 环持久化 | —— | **不持久化**（hub 是实体状态，重激活后环从 0 起）——消费方现状同为内存态，无回退；跨激活连续性是 rans 侧开放点（spec §14） |
| 多路订阅 | 每路一个 SSE 连接 | 单 WS 连接多路订阅，`Event` 帧带 `SourceKind/SourceInst` 区分来源 |
| 事件 ID | 消费方自定义序号 | `incarnation<<32|seq`（per 源实体单调；高 32 位翻页=实体重激活、环重置，客户端可检测） |

**客户端纪律（迁移时必须内建）**：重连游标 = 本客户端**已收到并记账的最大
EventID**，**禁用** `SubscribeAckMsg.CurrentID` 作下次游标（它是订阅时刻的
服务端前沿，Ack 后在途未达事件会被跳过而永久丢失）；帧序非契约——事件按
Source+EventID 聚合排序消费，不依赖帧到达序；订阅/退订走 Call 面占每会话
在飞信号量（缺省 64），事件到达不占。

### claims 同节点边界对授权形态的影响

若导演服务的授权形态依赖「每次调用携带用户身份做细粒授权」（如 workspace
级权限），注意 `edge.ClaimsFrom(ctx)` **仅同节点生效**：会话发起的调用在本
地路径 ctx 直达实体 Handle；跨节点路径 wire 帧不承载 ctx 值，**远端实体的
Handle ctx 永不携带声明**（这是缺省常态——接入节点与实体宿主不同节点时即
如此）。两条可靠形态：

1. **会话与实体同节点**：混部部署下也只能对「实体恰在接入节点」的调用读到
   声明，**放置不保证这一点**——除非业务设计把实体锚定在会话所在节点，
   否则不要把正确性押在此形态上；
2. **业务协议载荷自带凭据**（推荐）：声明作为消息字段随 wire 载荷过线，
   实体 Handle 自行校验——与节点拓扑无关，迁移后行为确定。

`AuthorizeTarget` 粗粒 kind 白名单不受此限（在边缘带 claims 执行、全局
有效）。claims 过线是 rans 侧开放点（spec §14），立项前跨节点细粒授权
以形态 2 为准。

### 其他迁移评估输入

- **装配面**：`assembleCluster` 的手工装配可切 `host.New` + `WithRedis` +
  `WithEdge`（QUICKSTART 装配序与停机序全部内置吸收）；SSE 翻译层整层
  退役，事件帧编解码复用 `wire`（客户端侧仍需薄 WS 层——SDK 不在
  rans_golang 仓库面内）；
- **部署约束**：`WithEdge` 改变握手指纹的 kind 维——**集群各节点必须一致
  启用**（混配互拒握手）；可用纯接入节点形态（`WithDeclaredKinds`）把接入
  面独立部署，业务放置不会落到它身上；
- **停机语义**：优雅停机 = Bye(draining) 尽力而为后裸断连；`Kill` = 无
  close 帧裸断连（客户端重连逻辑不应假设总能收到 Bye）；关闭码表冻结于
  rans_golang README「协议纪律」段；
- **已知粗糙点（如实告知）**：E3 运行时观察——一个死订阅者的投递重试最坏
  可占 5×2s 退避窗口并连坐同次 Emit 的其他订阅者（liveness 面，at-most-once
  无违），已登记 rans_golang spec §14 开放点；高订阅基数 kind 的迁移评估
  应计入该尾延迟。
