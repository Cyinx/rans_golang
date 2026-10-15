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
调用错误的约定形态 `*rans.Error{Code, Reason, Message, Node, AppCode, AppData}`——`Code` 是重试类别（线上数值同旧 `contracts.StatusCode`，附录 B），`Reason` 是具体原因（`Busy/Overloaded/NotLocal/UnknownKind/StaleLocation/...`）。本地与远端同构（EX12）；实体 Handle 错误在投递边界折叠为 `CodeError`+`Reason=Handler`，调用方不可 Unwrap 到实体原始错误。判错用 `rans.CodeOf/ReasonOf` 与 `errors.Is` 哨兵（`ErrTimeout/ErrUnavailable/ErrNotFound`）；超时类满足标准库 `errors.Is(err, context.DeadlineExceeded)`、取消类经包装链满足 `Is(err, context.Canceled)`（标准 Go 判错两半均成立）；重试环两次尝试之间与路由目录查询的 ctx 结束亦收场为 `*rans.Error`（超时→Timeout、取消→Canceled）。已知例外（如实收窄，勿做宽断言）：本地基础设施故障面（请求编码/应答解码）现以包装的普通 error 逃逸，`ErrSerialization`/`ErrDecode` 哨兵当前零生产者——「返回值必为 `*rans.Error`」不成立。
_Avoid_: `manager.ErrTimeout`/`*routing.TimeoutError` 等旧错误形态（已删除）；对调用返回值做 Unwrap 依赖；对错误形态做「必为 *rans.Error」的类型宽断言

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
节点级出站缓冲——单向发送入箱即返回，按目标分队列（每目标专属 worker，跨目标互不阻塞）保同目标 FIFO，负责路由与发送；死目标熔断（发送重试耗尽/端点判停开断，冷却探针/入站可达/复活通知闭合）。满策略按生产者分野：显式 `Tell` 阻塞至 ctx 到期；事件扇出/错误回包/仲裁输家转发/接管通知满即丢 + 计数（保 INV5：实体 loop 与读循环永不因下游慢而卡）；控制帧（接管通知/重定向提示）走预留容量。FIFO 承诺限于**同一连接内**的写出序——例外：连接收敛（发送侧缓解后窗口缩小、收端两读循环仍可能交错）、输家转发/重定向、迁移整箱转发、发送失败与故障。
_Avoid_: 实体 loop 或读循环上的同步出站 IO（H11 要消除的形态）；把重定向提示当优先通道帧（实走数据通道）

**成员视图（Local membership view）**（v0.27.0 起，票 01）：节点本地的存活节点全量快照（`NodeRegistry.Nodes` 周期刷新，缺省 1s、`Tuning.MembershipRefresh` 可调、下限 100ms）——路由有效性/放置候选/地址解析三消费面读快照（读放大收口：热路径不再逐跳/逐放置读注册表）。**视图年龄**上限 = 成员键 TTL（快照比判活键还老即不可信），超龄/未首刷/刷新失败后老化 → 查询退**保守存活**（返回存活、不降级实时读——发送失败+重试环兜底）。**即时失效**：连接丢失/端点判停/重定向提示（拓扑信号，非判死）触发合并窗（200ms）内的一次全量刷新；成员面换代/复活事件即时闭合死目标熔断。首查同步刷新（singleflight，1s 预算）——冷启动无放置偏斜窗口。
_Avoid_: 把接管闸门改读快照（双轨判定的分界——路由答错代价一次重试，接管答错代价双活）；给视图判活加实时读回退（{reg} 不可达时回退只会放大压力）

**节点代际（Node epoch）**（2026-10-11 HR5 裁定，随 v0.23.0 落地；v0.26.0 起续期比对）：
节点启动时 `{reg}` 内全局计数器 HINCRBY 取得的单调值，节点键值携带；目录条目记录 `{node, nodeEpoch, inc}`。**条目有效 ⟺ owner(node, epoch) 在本端成员视图存活**——判定双轨（v0.27.0 起）：路由热路径（正缓存命中）读本端 [[成员视图]] 快照（零注册表读）；接管闸门/迁移代际读/销毁清理保持实时注册表读（`NodeRegistry.NodeEpoch` 单键 GET——低频、保守）。读错误/视图不可信保守判活（瞬时故障不触发接管/缓存失效），键不在/已过期或代际不符判死。同节点号重启 → epoch 变大 → 旧条目自然失效。v0.27.0 起 epoch-0 条目形态消亡（Start 同步取号——见 [[成员视图]] 词条；遗留 epoch-0 条目经 CAS 接管自愈，分支 2 不再收养）。v0.26.0 起成员键走 `RenewNode` 比对续期（[[成员面一死永死]]）：存活期键消失 → 本代际作废重取号（新键写成功才发布——`next` 待发布防误判）；键被更大代际占据（NodeID 重号）→ 本进程让位（封表 + 自关）；取号回退（计数器随 Redis 丢数据回退）按已发布值+1 保护。
_Avoid_: 用 TTL/续租判条目有效性（H13 前旧形态）；把接管闸门改成读快照（快照答错=双活——双轨判定见决策记录）；同代际复活成员键（v0.26.0 前的缺陷形态——旧代际实例永驻双服务）

**接管（Takeover）**（2026-10-11 HR5 裁定，随 v0.23.0 落地；接管通知发送半边随 2026-10-13 修复批真实接线）：
死 owner 目录条目的重占——Register 携带 `expectedOwner` 做 CAS（逐实体键单 slot Lua 原子读-比-写），赢则 inc+1 覆写；接管闸门的判活（`directory.OwnerAlive`）对注册表读错误保守判活（瞬时故障不触发接管）。接管赢方**经发件箱以 `MsgOwnershipLost`（=16）控制帧直发**旧 owner 节点（控制面优先通道；best-effort——满箱即丢、不重试），旧 owner 收端按谓词围栏（`FenceWhere`，owner 全等才围）+ 实例身份守卫销毁。通知丢失的兜底按形态分野：旧 owner 成员键仍在 → 存活续期照常，实例滞留到下一次该实体的仲裁/陈旧信号（SelfFence 只兜底成员键写失败形态——键过期 ⟹ liveness 同败 ⟹ 自动关准入，对「通知丢失但成员键健在」不成立）；成员键同时消失 → 比对续期判 Absent → 换代围栏止损。inc 围栏保 INV3（旧纪元持久写被 Store 拒）。
_Avoid_: 无条件覆写条目（双实例无仲裁）；把接管通知当可靠投递（best-effort，SelfFence 是兜底不是冗余）

**重定向提示（Redirect hint）**（2026-10-11 HR5 裁定，随 v0.23.0 落地）：
非 owner 节点转发请求后，在回包/错误帧携带正确 owner，调用方立即改写本地位置缓存（不等正 TTL 30s 到期）——取代目录全量轮询（DirectoryWatcher 删除）的被动失效面；正 TTL 30s 作为提示丢失时的收敛上界。SPUBLISH 主动失效留扩展点不做。
_Avoid_: 全量轮询目录做缓存失效（BrowseAll 负载病根）

**取消（MsgCancel）**（2026-10-11 HR4 裁定，随 v0.23.0 落地）：
best-effort 的在途/在箱请求取消控制消息：调用方 ctx 因**取消**（非 deadline）结束时由本端网关发出，携带 MessageID、无回包——**经发件箱入请求同一目标队列**（排在请求之后不超车；目标熔断时随队列丢弃，请求必然也未送出）。重定向提示收端按 MessageID 改写本端网关在途条目的目标节点（from 守卫）——输家转发后取消与判停跟随赢家；输家对已标记取消的在箱投递不再转发（计 `rans.call.canceled`）。本地路径保留直接 cancel 穿透（重建 Handle ctx 时挂 cancel 面）。幂等、迟到无害（找不到目标即忽略）；链式经 Handle ctx 取消自然传播。调用方收场 `CodeCanceled`；在箱已标记未执行的丢弃计 `rans.call.canceled`（与 expired 分列）。
_Avoid_: 期待取消的确定性送达（best-effort，deadline 是兜底）

**延迟应答 / ReenterAfter（Deferred reply）**（2026-10-11 HR4 裁定 HD7 细化，随 v0.23.0 落地）：
两件套：`rans.Defer(ctx)` 标记本请求改延迟应答（拿 Responder，可单独使用——允许由后续消息的 Handle 回复；只 Defer 不 Reenter 的请求在 deadline 到期以 Timeout 收场）；`rans.ReenterAfter(ctx, work, then)` 让出实体 loop——work 在 loop 外执行（wctx 继承 deadline/trace/因果路径但**不追加自身**），then 作为续体任务回 loop 串行执行。看门狗只统计 loop 同步执行段；交接窗等待在途 work（预算内）超时取消 wctx。
_Avoid_: 隐式交错（Go 无续体调度点，《规格》§5.1 Interleaved 不移植）；实体内自建 goroutine+锁等外部 IO（ReenterAfter 要收编的形态）

**调用环（Cycle）**（2026-10-11 HR4 裁定，随 v0.23.0 落地）：
因果路径（根 trace + 实体身份 FNV-1a 64 哈希序列，走头部扩展块）在**准入时**含自身 → 立即以 `CodeError` + `Reason=Cycle` 回绝：消息未执行、不可重试（重试必然再成环）、文本带完整环。实体在 loop 上同步执行 Handle 期间发出的 Call 追加自身；Tell 不追加（非等待边）。深度上限 64，超限降级直通 + 告警。
_Avoid_: 等超时收场（EX2 病根）；单向发送计入等待边

**纪元下限（Incarnation floor）**（v0.26.0 新增）：
激活取号的下限语义——`Directory.Register` 的 `self.Incarnation` 入参作 floor（0=无），三个赢分支统一 `inc = max(现值, floor)+1`（CAS 比对与输家返回不受影响）。运行时下限 = max(Store 高水位（实体实现 Snapshotter 且 Store 实现 `HighWaterReader`）, `node.Config.FencingFloor` 宿主钩子)；任一读取出错激活失败（可重试）。目录键丢失后复活节点的取号仍高于持久层围栏水位——已确认写不静默丢失。跳号合法（普通注册/迁移在跳号后 +1 续接）。
_Avoid_: 把 floor 当 CAS 期望值（只抬高取号不参与裁决）；持久层水位与目录取号脱钩（R2 双活缺陷形态）

**协议代际（Protocol generation）**（v0.26.0 新增）：
握手协议版本区间（`wire.HandshakeVersionMin/Max`，当前 4/4）经成员键值（`pmin`/`pmax`）进入成员视图；`node.Start` 监听前读 `NodeRegistry.Nodes`，存在协议区间不相交的存活节点（含缺字段 0/0 的断点前版本节点）即拒绝启动（混版本集群零容忍）；读注册表失败 fail-open。握手本身仍是区间协商（双端区间相交即通），协议代际检查是启动面的集群拓扑防护。
_Avoid_: 依赖滚动升级窗口内的混版本互通（断点版本先停全部再升）

**成员面一死永死（Membership dies forever）**（v0.26.0 新增）：
成员键的生命周期不变量：键在本节点存活期间消失 ⟹ 本代际作废（围栏全部本地实例 + 重新取号，**不以同代际复活**——复活即旧代际实例双服务）；键被更大代际占据 ⟹ 本进程让位（停写 + 围栏全部 + 封表自关）。Liveness 每 tick `RenewNode` 比对续期实现该语义；「待发布代际」`next` 防「服务端写成功、客户端报错」误判 Superseded，也不得退回旧代际续期。
_Avoid_: 成员键无条件重写（v0.26.0 前形态——进程暂停 ≥ TTL 后同代际复活，双服务窗口敞开）
