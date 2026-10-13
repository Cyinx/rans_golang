# rans_golang

Rans 虚拟 actor 运行时的 Go 移植，独立项目专人开发。语义唯一出处 `docs/porting/ARCHITECTURE.zh-CN.md`（移植规格）；Go 侧取舍记录在 `docs/decisions/`。

## Language

**Redis Registry（节点注册表）**（固定，2026-10-02 裁定）：
节点级成员信息的 Redis 承载面，接口 `directory.NodeRegistry`。键方案两层（均带 `{reg}` hash tag，同 slot）：**总表** `rans:{reg}:nodes`（Hash：field=节点号，**value=续命时间戳**（unix ms，写方时钟）——枚举面（HGETALL 一条命令）+ 判活初筛预留（为未来判活三件套：表过期初筛 + 探测 + 投票；当前时间戳仅记录，不参与判活））+ **判活键** `rans:{reg}:node:<节点号>`（String，SET PX——**存活的唯一权威**（服务端单时钟 TTL）+ 地址的唯一承载）。写入方 `internal/membership` 的 `Liveness`（节点存活信号；每周期无条件双写 + 启动即写；随 `node.Config.Lease.Interval` 周期驱动）；总表是超集，读面逐判活键探活收敛 + 死 field 惰性清理。
_Avoid_: 「redis 目录」（与实体目录撞名）

**Directory（实体目录）**（固定，2026-10-02 裁定；HR3 定形；2026-10-11 H13 裁定改写、随 v0.23.0 落地）：
身份→归属的承载，端口 `directory.Directory{Register, UnregisterIfOwner(Owner), Lookup}`。归属是**实例级** `directory.Owner{Node, Incarnation}`——注销/换锁按 {node, inc} 同时匹配。不承担节点存活语义——与 Redis Registry 分离。H13 改写：条目值升 `{node, nodeEpoch, inc}`，**有效性改由成员代际判定**（条目有效 ⟺ owner(node,epoch) 在本端成员视图存活）；取消逐实体续租（LeaseDirectory 语义删除）；死条目经 CAS 接管重占、优雅停机批量注销、崩溃残留无 TTL 躺置无害（详见 [[节点代际]]/[[接管]] 词条）。行文/注释/文档统一用「Redis Registry（节点注册表）」指节点注册表面，不用「redis 目录」。
_Avoid_: 把节点存活语义安在 Directory 头上；裸节点号归属（实例级归属缺纪元即旧形态）；逐实体续租语义（H13 后为反模式）

**目录键族 hash tag**（X8 选项 A 2026-10-02 落地；2026-10-11 H13 裁定改写、随 v0.23.0 落地）：
H13 终态：主键改**逐实体 hash tag**（键内含 `<kind>/<key>`，目录真分片）；class/node 索引与 `rans:{dir}:inc` 共享 Hash 删除（索引生产只写不读已实证；纪元改逐实体键值内自持，注册/收养/换锁在单键 Lua 内读-改-写原子）；心跳/租约类键随「取消逐实体续租」整体消亡。v0.23.0 落地前现状：主键/心跳/索引/纪元 Hash 集中 `{dir}` 单 slot + 四 Lua 多键原子。
_Avoid_: 新目录键沿用集中 `{dir}` tag（H13 后为反模式）

**Redis 部署形态**：
目标使用 Redis Cluster。registry（`{reg}`）与实体目录（`{dir}`）两个原子域均已 cluster 兼容；`redis.Options.Addrs` 传种子列表即 cluster/哨兵形态。cluster 兼容性已经**实测背书**：内网 3-master 测试 cluster（RANS_TEST_REDIS_CLUSTER_ADDRS 门禁）跑契约平移 + 真槽号断言 + SCAN 全分片遍历（ForEachMaster——通用 SCAN 在 cluster 下单节点失明，实测锤出后修复）。

**node（组装层）**（固定，2026-10-08 HR3 裁定 SD9；2026-10-12 v0.24.0 edge 退役改写）：
唯一组装根 `node.Node`：`node.New(Config)` 构造即完整（全部校验在构造期，运行中不可变——全仓无运行期 `Set*`），`Start/Shutdown/Close/Run` 生命周期三件套 + 便捷形态。原 `host`/`remote` 的组合部分、`manager` 的编排面全部并入/内部化。v0.24.0：edge 外部接入面（`edge` 包 + `internal/edgesrv` + `node.Config.Edge` 装配面）整体退役——外部客户端接入归消费方自建网关形态（wanju_agent_server gateway ingress），`EdgeAddr`、纯接入节点形态与 `edge_session` 系统 kind 随之消亡。
_Avoid_: `host`（旧组装包名，已删除）；`edge`/`edgesrv`/`Config.Edge`/`EdgeAddr`（v0.24.0 退役，历史语义见 edge hosting spec 与 CHANGELOG v0.24.0）；绕过 Config 直接构造内部组件

**Client（统一调用面）**（固定，2026-10-08 HR3 裁定 SD10）：
唯一调用入口 `rans.Client{Call, Tell}`——经 `Node.Client()` 或实体内 `rans.ClientFrom(ctx)` 获得；本地命中走本地快路径，与远端同一错误形态。单向发送动词是 **Tell**（SD4；`SendOneWay` 已删除，testkit 的 `Node` 包装面与统一调用面同词 Tell，不保留旧名）。生成代理 `<Entity>Client{C rans.Client}`。
_Avoid_: 本地专用调用入口（EX11 病根，已删除）；`Caller`（旧名）

**Code / Reason（统一错误模型）**（固定，2026-10-08 HR3 裁定）：
调用错误唯一形态 `*rans.Error{Code, Reason, Message, Node, AppCode, AppData}`——`Code` 是重试类别（线上数值同旧 `contracts.StatusCode`，附录 B），`Reason` 是具体原因（`Busy/Overloaded/NotLocal/UnknownKind/StaleLocation/...`）。本地与远端同构（EX12）；实体 Handle 错误在投递边界折叠为 `CodeError`+`Reason=Handler`，调用方不可 Unwrap 到实体原始错误。判错用 `rans.CodeOf/ReasonOf` 与 `errors.Is` 哨兵（`ErrTimeout/ErrUnavailable/ErrNotFound`）。
_Avoid_: `manager.ErrTimeout`/`*routing.TimeoutError` 等旧错误形态（已删除）；对调用返回值做 Unwrap 依赖

**Table（本地激活表）**（固定，2026-10-08 HR3 裁定）：
本地激活表面 `internal/activation.Table`（原 `manager.Manager` 核心：激活仲裁 register-first + 输家转发 + 销毁）。管理操作公开面是 `Node.Destroy(ctx, id)`/`Node.Migrate(ctx, id)`——只作用于本地实例，非本地 `NotFound`+`Reason=NotLocal`。
_Avoid_: `manager`（包已删除）；管理操作静默成功

**Liveness / LeaseKeeper**（固定，2026-10-09 HR3 票 17 落位；2026-10-11 H13 裁定改写、随 v0.23.0 落地）：
`internal/membership` 的节点存活与逐实体续租两半：`Liveness`（节点存活信号/注册/成员键/draining——Redis Registry 写入方）与 `LeaseKeeper`（逐实体续租/失租销毁/Track/Untrack）。原 `HeartbeatService`（名为心跳实为租约的复合体）拆分终形。H13 改写：LeaseKeeper 整体删除——所有权丢失改由接管方直发控制消息（旧 owner 节点对该 id 立即关准入）+ SelfFence 兜底；Liveness 保留并携带节点代际（启动时 `{reg}` 内全局计数器 HINCRBY 取单调值）。
_Avoid_: 「心跳服务」指续租面（心跳只是节点存活半边的机制名）；逐实体续租/失租销毁语义（H13 后为反模式）

**Factory / Snapshotter（实体契约）**（固定，2026-10-08 HR3 裁定）：
`rans.Factory func(id rans.ID) rans.Entity`——工厂拿到身份、必须无副作用（激活竞争中被调用后可能整个丢弃）；`rans.Snapshotter{Snapshot, Restore}`——迁移/钝化/checkpoint/终快照共用的快照面（替代 `manager.MigrationCarrier`）。实体契约 `rans.Entity{Activate, Handle, Deactivate}`。
_Avoid_: `func() Entity` 无参工厂（拿不到身份，已删除）；`MigrationCarrier{CaptureSnapshotBytes/RestoreFromSnapshotBytes}`（已删除）

**预算（Budget）**（2026-10-11 HR4 裁定，随 v0.23.0 落地）：
请求级 deadline 的过线形态——发送时刻剩余毫秒 `budget_ms`（相对值防时钟偏差）写入头部扩展块。本次预算 = min(调用方 ctx 剩余, `Timeout(d)` 选项, `MaxCallTimeout`)；无 deadline 无选项取 `DefaultCallTimeout`（30s，Max 缺省 10min）。每跳重推导：被调方出站调用取其自身 Handle ctx 剩余再走同一 min 规则。被调方 Handle ctx = 节点根 ctx 派生 + 收到时刻 + budget。
_Avoid_: 绝对时刻过线（时钟偏差）；网关 pending 的全局固定超时语义（随 H10 删除）

**传播键 / 仅本地键（Propagation key / Local-only key）**（2026-10-11 HR4 裁定 HD8 细化，随 v0.23.0 落地）：
请求级 ctx 值的两类登记键。传播键经 `rans.RegisterPropagation(key, encode, decode)` init 期全局登记、随调用过线（本地路径同语义重建 Handle ctx）；不登记的键 = 仅本地，永不序列化（宿主本地授权状态的承载形态）。护栏：单值 4 KiB、单请求合计 32 KiB，超限丢弃该键 + 计数。
_Avoid_: ctx 全透传（本地远端行为分叉，违反位置透明）；未登记键过线

**发件箱（Outbox）**（2026-10-11 HR4 裁定，随 v0.23.0 落地）：
节点级出站缓冲——单向发送入箱即返回，worker 按目标节点哈希分片保同目标 FIFO，负责路由与发送。满策略按生产者分野：显式 `Tell` 阻塞至 ctx 到期；事件扇出/错误回包/仲裁输家转发满即丢 + 计数（保 INV5：实体 loop 与读循环永不因下游慢而卡）。控制帧（Ping/Pong/Retire）走独立优先通道。
_Avoid_: 实体 loop 或读循环上的同步出站 IO（H11 要消除的形态）

**节点代际（Node epoch）**（2026-10-11 HR5 裁定，随 v0.23.0 落地）：
节点启动时 `{reg}` 内全局计数器 HINCRBY 取得的单调值，节点键值携带；目录条目记录 `{node, nodeEpoch, inc}`。**条目有效 ⟺ owner(node, epoch) 在本端成员视图存活**（调用方内存判定，Redis 单键 Lua 不跨 slot 查成员键）。同节点号重启 → epoch 变大 → 旧条目自然失效。
_Avoid_: 用 TTL/续租判条目有效性（H13 前旧形态）

**接管（Takeover）**（2026-10-11 HR5 裁定，随 v0.23.0 落地）：
死 owner 目录条目的重占——Register 携带 `expectedOwner` 做 CAS（逐实体键单 slot Lua 原子读-比-写），赢则 inc+1 覆写。接管方节点**直发控制消息**给旧 owner 节点令其对该 id 立即关准入；SelfFence 兜底误判（成员键过期 ⟹ 该节点 liveness 同败 ⟹ 自动关准入），inc 围栏保 INV3（旧纪元持久写被 Store 拒）。
_Avoid_: 无条件覆写条目（双实例无仲裁）

**重定向提示（Redirect hint）**（2026-10-11 HR5 裁定，随 v0.23.0 落地）：
非 owner 节点转发请求后，在回包/错误帧携带正确 owner，调用方立即改写本地位置缓存（不等正 TTL 30s 到期）——取代目录全量轮询（DirectoryWatcher 删除）的被动失效面；正 TTL 30s 作为提示丢失时的收敛上界。SPUBLISH 主动失效留扩展点不做。
_Avoid_: 全量轮询目录做缓存失效（BrowseAll 负载病根）

**取消（MsgCancel）**（2026-10-11 HR4 裁定，随 v0.23.0 落地）：
best-effort 的在途/在箱请求取消控制消息：调用方 ctx 因**取消**（非 deadline）结束时由本端网关发出，携带 MessageID、无回包。本地路径保留直接 cancel 穿透（重建 Handle ctx 时挂 cancel 面）。幂等、迟到无害（找不到目标即忽略）；链式经 Handle ctx 取消自然传播。调用方收场 `CodeCanceled`；在箱已标记未执行的丢弃计 `rans.call.canceled`（与 expired 分列）。
_Avoid_: 期待取消的确定性送达（best-effort，deadline 是兜底）

**延迟应答 / ReenterAfter（Deferred reply）**（2026-10-11 HR4 裁定 HD7 细化，随 v0.23.0 落地）：
两件套：`rans.Defer(ctx)` 标记本请求改延迟应答（拿 Responder，可单独使用——允许由后续消息的 Handle 回复；只 Defer 不 Reenter 的请求在 deadline 到期以 Timeout 收场）；`rans.ReenterAfter(ctx, work, then)` 让出实体 loop——work 在 loop 外执行（wctx 继承 deadline/trace/因果路径但**不追加自身**），then 作为续体任务回 loop 串行执行。看门狗只统计 loop 同步执行段；交接窗等待在途 work（预算内）超时取消 wctx。
_Avoid_: 隐式交错（Go 无续体调度点，《规格》§5.1 Interleaved 不移植）；实体内自建 goroutine+锁等外部 IO（ReenterAfter 要收编的形态）

**调用环（Cycle）**（2026-10-11 HR4 裁定，随 v0.23.0 落地）：
因果路径（根 trace + 实体身份 FNV-1a 64 哈希序列，走头部扩展块）在**准入时**含自身 → 立即以 `CodeError` + `Reason=Cycle` 回绝：消息未执行、不可重试（重试必然再成环）、文本带完整环。实体在 loop 上同步执行 Handle 期间发出的 Call 追加自身；Tell 不追加（非等待边）。深度上限 64，超限降级直通 + 告警。
_Avoid_: 等超时收场（EX2 病根）；单向发送计入等待边
