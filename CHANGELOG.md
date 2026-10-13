# Changelog

对外发版记录（semver tag 对应）。

## v0.25.0（2026-10-13）

主题：**开源公开发布（首个 GitHub 公开 tag）**。对外 Go API、线协议、
行为零变更——本版全部为公开面治理。

### 公开面治理（零行为变更）

- 代码注释清除参考项目对照字样（Orleans/C#/.NET 等，13 文件 14 处，
  技术语义不变）
- Makefile gofmt 门禁只扫 git 跟踪文件：并行 worktree（`.worktrees/`）的
  未格式化 WIP 不再打挂本仓 `make test`
- 公开文档（CONTEXT.md/CHANGELOG.md/consumer-upgrade-notice.md）清除
  参考项目对照引用
- 内部过程文档（工单/规格/裁定记录）移出仓库，公开面仅保留
  README/CHANGELOG/CONTEXT.md/consumer-upgrade-notice.md
- README 面向开源重写：独立项目口吻、新增特性总览与安装节
- LICENSE：Apache License 2.0

## v0.24.0（2026-10-13）

主题：**edge 部件整体退役（破坏性变更，一次做透不留 Deprecated 中间态）+
2026-10-13 全仓审查修复批**（接管防线收口 / 交接窗与快照正确性 / 目录
语义对齐 / 装配校验收紧 / 调用面与生成器门卫）。消费方
wanju_agent_server 的外部客户端接入已迁移自建网关形态（gateway
ingress），其对 rans 的 edge 依赖已先行归零——上游删除零联动。edge 帧
（WS 外部线协议）从来不在 Go/Rust 两实现的线协议互通面（rans_rust 无对应
crate，已核实），退役不伤字节级互通。

### edge 部件退役（破坏性 Go API 变更）

- **删除 `edge` 包整体**：WS 监听器/会话机制（`internal/edgesrv` 一并删除）、
  外部帧编解码（Hello/Welcome/Call/OneWay/Subscribe/Unsubscribe/Ping/Pong/
  CallResult/Event/Bye 与外部码映射）、`Authenticator`/`Claims`、EventHub
  （订阅/事件环/扇出/LastEventID 回放）、`edge.Config`。
- **删除 node 组装面的 edge 接线**：`node.Config.Edge` 字段、纯接入节点形态
  （`Kinds` 现为无条件必填）、`Node.EdgeAddr()`、内置 `edge_session` 会话工厂
  与自动 `LocalOnlyKinds=[edge_session]` 接线、Start/Shutdown/Close 停机序中
  的 edge 段（外部连接的 Bye/宽限排空随之消失——网关形态由消费方自管）。
- **删除 internal/rpc 的 `LocalOnlyKinds` 机制**（remote.Config 字段 +
  dispatcher 远端激活守卫）：该守卫由 edge 宿主 spec §10 引入、唯一生产者
  是 edge 会话面，随宿主退役整体移除（含 `localonly_test.go`）。
- **删除 edge 仪表**：`rans.edge.sessions`、`rans.edge.preauth` 两 Gauge
  （`Node.Metrics()` 快照面其余 11 项不变）。
- **删除 `node.Config.DeclaredKinds`**：零读者的声明面字段，其唯一记载用途
  是纯接入节点记录集群全量 kinds——宿主形态消亡后一并删除。
- **连带清理**：`node.Config` 失去 Edge 字段族后装配校验收紧为 `Kinds`
  必填；testkit 的 HR4 事件扇出验收测试随 EventHub 退役删除（发件箱
  满即丢语义由 `internal/rpc` 单测继续覆盖）；`make fuzz` 收敛为两解码面
  （wire 帧 + 握手扩展对象）；`coder/websocket` 依赖随包删除移出 go.mod；
  kind 感知放置（KindScanner/KindPlacement）与非阻塞发送能力面
  （routeClient.TryTell）是已泛化的通用机制，保留。
- **依赖表同步**：《约定 spec》§3.2 依赖表删除 `edge` 行与
  `internal/edgesrv` 行（公开包收敛、rpc 行字面收口自然成立），根
  `depgraph_test.go` 门禁同 commit 同步（改表必同改测试）。
- **消费方迁移指引**：外部客户端接入不再经 rans——按消费方 gateway 形态
  自建 ingress（wanju_agent_server 已完成割接，见其 gateway-ingress 票
  08/09）；升级通知见 `docs/consumer-upgrade-notice.md` v0.24.0 节。
  决策正本：`docs/decisions/2026-10-12-edge-retirement.md`。

### 接管通知接线与误判接管防线收口

- **接管通知发送半边接线**（兑现 v0.23.0「接管赢方直发 `MsgOwnershipLost`」
  的已发版承诺——该承诺此前发送半边未装配，通知为生产不可达的死代码）：
  CAS 接管赢方现经节点发件箱以 `MsgOwnershipLost`（=16）控制帧直发原
  归属节点（帧类型属控制面集合，传输层入优先通道），旧 owner 收到后对
  归属全等的实例毫秒级关准入 + 有界销毁。best-effort：满箱即丢、关箱
  拒绝、不重试——通知丢失由 SelfFence 兜底（既定语义）。误判接管（节点
  健康、成员视图已判死）的旧 owner 不再永久滞留，双活窗口回到设计预期
  的毫秒级。钉死：testkit `TestFault_TakeoverForm1MemberKeyDeleted`
  （活体假死 → CAS 接管 → 旧 owner 经真实发送链收通知围栏销毁、窗口期
  无双服务、路由收敛新 owner）。
- **成员视图判活保守化**：`directory.OwnerAlive` 的注册表读错误（超时/
  连接失败等瞬时故障）由判死改为保守判活——不再因一次 Redis 读错误判
  活 owner 已死（否则接管 CAS 对原封未动的条目必然赢，实体被误迁、路由
  正缓存全量失效）；仅成员键确认不在/已过期才判死。真死场景由发送失败/
  重试环兜底。激活接管闸门与路由陈旧收口共用本判据，无第二份判定。
- **SelfFence 记账修复**：成员存活节拍的两种失败形态（成员键写失败、
  节点代际取号失败）统一按真实时刻推进自检时钟。此前取号失败以零时刻
  入账，记账双向失效——首败即产生垃圾基线（第二轮即全表关准入 + 逐实体
  止损销毁，节点从未对外可见就自毁全部本地实体），或失败不入账
  （SelfFence 永不触发、兜底失效）。止损语义不变：LocalIDs 全集关准入 +
  逐实体止损销毁、每失败幕恰一次、成功复位。
- **入站监听临时错误退避重试**：TCP accept 对临时类错误（EMFILE/ENFILE
  等资源耗尽形态）按毫秒级指数退避重试（1ms 起步封顶 20ms，成功接入即
  复位）并 Warn 留痕——此前任意 Accept 错误即静默终止监听循环，一次 fd
  耗尽后节点入站永久失聪且无告警、无恢复路径。监听器关闭静默终止、
  持久错误终止并留痕（语义不变）。

### 交接窗/快照/迁移正确性

- **交接窗静默判据补续体队列**：迁移/钝化/停机的终快照等待判据由
  「在途 work 为空」补齐为「在途 work 与续体队列双空」——work 完成与
  续体入队同临界区落地后，终快照不再抢先拍板把续体写永久蒸发（高频
  竞态：实测约四分之一轮次丢失）。「已确认 ⟹ 持久」不变量在交接窗
  闭合。周期 checkpoint 同族缺口同修：未达静默本轮 Skipped（零副作用、
  下轮重试），不再把未含续体写的快照连同覆盖该写的活动纪元一起入账
  （脏门控自此判净）。续体执行计入活动纪元（脏门控覆盖续体写）并登记
  在途执行槽（强弃解绑面统一）。
- **checkpoint 簿记实例身份守卫**：快照收尾/中止/等待/占门按 id 命中
  条目后补实例身份校验——慢 Store 写入收尾窗内实例被强弃并同 id 重激活
  时，旧 goroutine 不再把旧实例的活动纪元抬进新实例的 checkpoint 纪元
  （新 incarnation 的变更被系统性跳过 checkpoint 的病根）、不误放新实例
  在飞门（同实体 IO 永不重叠保持）。
- **随迁包携带源纪元 + 恢复高水位比较**：随迁包头扩展块追加字段号 5
  `src_incarnation`（varint，源端落库快照时的围栏纪元）——同 VerMax 内
  只追加字段号、旧端按线型跳过（缺位按无版本处理，恢复退回包优先历史
  行为）。恢复侧包命中时与 Store 的 per-id 高水位比较：包纪元低于高水位
  弃包落 Store 梯级——判输/预算耗尽迁移散落在曾收包节点的过期包，不再
  在接管恢复时以旧内容占据 Store 高水位、静默回滚已确认变更；同纪元按
  内容比对定胜负（成功迁移的包优先免 Store 读快路径保持）；包无版本/
  Store 无高水位能力/比对不可用保守不弃包。新增
  `checkpoint.HighWaterReader` 可选能力接口（`checkpoint.InMemory` 已
  实现；自定义 Store 可选补面）。
- **迁移换锁期望归属改用激活时捕获值**：换锁 CAS 的期望归属从「当下
  节点代际」改为实例在激活时刻捕获的归属三元（与目录条目现值同源）——
  启动窗口（节点可服务先于成员代际取号）内激活的 epoch-0 条目此后每次
  迁移不再恒败（此前严格三元比对 0≠N 恒 `ErrMigrationLost` 且静默）；
  表内无捕获值的极端交错按确定性拒绝判输（输因不被预算遮蔽）。

### 目录语义收口（行为变更，双实现对齐）

- **收养分支升写调用方纪元**：同节点自收养（含条目纪元为 0 的注入形态
  收养）落库从「保留条目现值纪元」改为升写调用方纪元——redis 实现的
  返回值与落库值从此一致（此前落库 0 而返回值非 0：按返回值注销恒
  false 成幽灵条目、对路由恒判活），与端口契约「赢时返回值是含新纪元
  的自归属、调用方必须采信」及 inmem 实现对齐。directory 契约套件新增
  三锚：epoch-0 条目被携带非零纪元的同节点收养（返回值=落库值=可按
  返回值注销）、零值字段漏填的注销反例、同节点换代 CAS 重占。
- **条件注销删除节点粒度通配**：`UnregisterIfOwner` 恢复与端口契约/
  inmem 一致的严格 `{node, epoch, inc}` 三元全等匹配——零值字段漏填的
  Owner 从「删光该节点全部条目」变为 no-op false（此前 redis Lua 的
  epoch/inc 通配模式是群删脚枪；全仓无该模式调用方）。自定义目录实现
  需对齐（契约套件已锁定）。

### 装配校验与配置收口

- **构造期校验收紧**（此前静默接受、运行期才爆发的形态现于 `node.New`
  构造期报错）：`Kinds` 拒绝 nil 工厂与不合法 kind 键（字符集同
  `rans.NewID` 规则）——此前 nil 工厂会让一帧远端消息 panic 于 TCP 读
  循环、进程崩溃；`Calls` 拒绝 `Max < Default`——此前 Default 分支完全
  绕过封顶（缺省预算现同受封顶）。
- **破坏性 Go API 变更**：`node.LeaseConfig` 删除
  `MaxConcurrentDestroys`/`DestroyTimeout` 两字段（止损销毁消费面已改
  实例级串行、单实体预算为内部固定值——两字段长期零消费、设置即静默
  no-op）；删除指标常量 `node.NameHeartbeatLost`（`rans.cluster.heartbeat_lost`
  承诺面从未注册、从未产出数据——幽灵指标）。
- **新增公共 API**：`rans.ValidKindName`——kind 字符集判据的正本出口
  （与 `rans.NewID` 共用），装配期校验复用同一判据；`node` 侧原同款
  本地复制已收口（`cmd/ransgen` 因零依赖设计保留本地复制、注释指回
  正本）。

### 调用面与生成器

- **本地 Tell 补 reenter 登记点**：本地单向投递与远端单向腿同构注入
  ReenterAfter 让出面——实体代码内 `ReenterAfter` 的可用性不再随消息
  到达路径分叉（此前本地 Tell 到达时在调用点 panic、被折叠为回绝且单向
  无回执即静默丢失，位置透明破口）。
- **ransgen 解析期门卫五项**（病态输入生成期报错而非产出坏码或延迟到
  运行期）：方法载荷参数名拒绝空白名（`_`）与 capitalize 归一碰撞
  （`foo`/`Foo` 共存，此前产出不可编译/重复声明的代码且零诊断）；
  `//rans:name` 拒绝空名（撞无值应答协议哨兵）与非法字符；多目录
  generate 先全量计划并查重跨包 wire 名唯一性（冲突报错带双方声明位置、
  失败零产出，不再延迟到运行期 init panic）；`-schema`/`-check` 的生成
  物排除名跟随 `-out` 旗标（不再硬编码）。合法输入生成物零变化。

### 跨节点 Tell→Call 保序修复（Call 请求帧入发件箱）

- 修复 v0.23.0 发件箱引入的跨路径次序回归：同一调用方先 Tell 后 Call 同一
  实体时，Call 请求帧此前的同步直发常超车仍在发件箱排队的 Tell
  （`examples/chat` 稳定输出空/部分历史）。Call 请求帧现与单向帧同箱同分片
  入队（`Outbox.PutCall`），同目标 FIFO 由分片单 worker 串行承载——
  「先 Tell 后 Call」的帧序即写出序，read-your-writes 语义恢复。
- 语义边界：同一调用方（同 goroutine）先 Tell 后 Call 同一目标 → 对端先见
  Tell；跨调用方不承诺次序；不同目标互不阻塞（分片并行保持）；故障/重路由
  场景为尽力而为（与 at-most-once 投递语义一致）。
- 预算与重试语义保持：Call 的登记/等待/超时/取消（MsgCancel）与直发形态
  同源（`Gateway.SendRequestQueued`，`SendRequest` 保留为直发组合体）；
  排队过预算的调用帧在发送段按自身 deadline 中止（确定未送出，不产生迟到
  执行）；发送重试耗尽经帧内 fail 回投，保持 Unavailable+SendFailed
  （确定未投递）形态——调用帧发送面不叠加端点判停，等待段不被
  FailPendingForNode 的 ConnectionLost（歧义类）覆盖，非幂等调用的无条件
  重试权不受损。
- 已知取舍：同分片异目标互相排队（哈希分片固有代价）现也适用于 Call 请求
  帧——Call 延迟与同分片队头耦合；性能与背压评估另行开票。

## v0.23.0（2026-10-12）

主题：**HR4+HR5 批（调用语义补齐 H10-H12 + 规模项 H13-H15）**——请求级
预算/传播键/尽力取消全链传播、出站发件箱去阻塞、Defer+ReenterAfter 让出
原语与准入判环、目录键值 v3（逐实体分片+成员代际+CAS 接管+续租拆除）、
重定向提示、放置正缓存与热路径分片。设计正本
`docs/superpowers/specs/2026-10-11-hr4-hr5-call-semantics-and-scale-design.md`
（磨尖裁定正本，细化《加固 spec》§7/§8）。**同集群双端同版本断点**
（目录键值 v3 + 新消息类型 + 头部因果字段退役），操作序与消费方影响见
`docs/consumer-upgrade-notice.md` v0.23.0 节。本节同时收编前一份
Unreleased（注释治理与文件拆分）内容。

内部重整（注释治理与文件拆分计划），**零 API / 线协议 / 行为变更；错误文本字符串
变更见下（非契约面）**：

- 错误文案前缀迁移：`rans/<pkg>:` → `rans: <操作>:`（仅错误文本字符串，非契约面——判错仍以
  `Code`/`Reason` 为准，`errors.Is` 哨兵不受影响）；日志消息保留 `rans/<pkg>:` 前缀用于排障定位。
- 12 个超 500 行生产文件按职责拆分（纯移动，不改行为）；placement 选点三份同形实现收敛为内核单点。
- godoc 契约化全量补全（契约信息保留，沿革叙事清零）；注释/体量门禁四项入 `make test`
  （仓库根 `commentgate_test.go`：ISSUES/日期字面 =0、单行 ≤100 显示列、超 500 行白名单、注释占比基线）。

### 调用预算合成（HR4 请求级预算·调用侧与本地腿，破坏性配置变更）

- `node.Config.Calls.RequestTimeout` 拆为 `Calls.Default`（缺省 30s，缺省常量
  `node.DefaultCallTimeout`）与 `Calls.Max`（缺省 10min，缺省常量
  `node.DefaultMaxCallTimeout`）——旧字段删除，零值构造期回落各自缺省。
- 新增 `rans.Timeout(d)` CallOption：本次预算 = min(调用方 ctx 剩余, `Timeout(d)`,
  `Calls.Max`)，无 deadline 无选项取 `Calls.Default`；`d ≤0` 视为未传，多个选项取最小。
- 网关 pending 从全局固定超时改为**逐请求预算**：deadline 在登记时按合成结果计算
  （250ms 周期扫描收割机制不变）；本地快路径 Handle ctx 的 deadline 按同一合成
  重导出（调用方取消穿透保持）。
- 网关超时错误文本 `timeout=` 改 `budget=`（非契约面，判错以 `Code`/`Reason` 为准）。

### 请求预算过线与出队过期（HR4 请求级预算·远端腿，线协议追加字段）

- 出站 Request/OneWay 帧在头部扩展块新增 `budget_ms`（字段号 1，varint，
  发送时刻剩余预算的相对毫秒值，防两端时钟偏差）；远端 Handle ctx 以
  「收到时刻 + budget_ms」派生 deadline，对端未带 `budget_ms` 时取本节点
  `Calls.Default`（防御缺省）——远端与本地腿的预算语义同构。
- 扩展块新增 `trace_id` 字段（字段号 2，string）：本版仅编解码通道（读写
  面与前向兼容跳过语义），无生产者。
- 新增 `rans.ReasonExpired`（值 17，只追加不复用）：出队执行前预算已耗尽
  的请求不执行，按 `CodeTimeout`+`ReasonExpired` 回绝（错误形态为约定
  spec §5.3 预留行）；本地与远端腿同检查点（共用投递执行入口）。
- 新增指标 `rans.call.expired`（Counter，`Node.Metrics()` 可见）：出队
  过期即弃累计。

### 因果路径 v2 与准入判环（HR4 H12，线协议追加字段 + 头部字段退役）

- **头部位置字段退役**：`HasCausalContext` 旗标位与头内嵌套因果对象
  （节点号序列）删除——位值只退役不复用，编码端不再置位、解码端不消费；
  因果路径改走扩展块新字段 `causal`（字段号 3，嵌套对象
  `{1=root string, 2=重复 varint 哈希}`），路径项为实体身份 `kind/key`
  的 FNV-1a 64 哈希（8B varint）。破坏性变更属 v0.23.0 同版本断点
  （同集群双端同版本，先停全部再升）。
- `trace_id`（字段号 2）自本版起有生产者：与 causal 的 root 同一生成源
  双载（本字段供通用观测面、causal.root 为环判定语义面）；仅 Call 携带，
  Tell 不写（Tell 是因果链边界——非等待边不追加、也不携带调用方路径）。
- 新增 `rans.ID.Hash()`：实体身份的 FNV-1a 64 哈希（线上环判定只携带
  哈希；环回绝文本展示哈希序列，消费方反查身份经本方法）。
- 准入判环：路径含目标实体自身哈希 → 消息未执行，立即以
  `CodeError`+`ReasonCycle` 回绝（本地与远端同点：pawnite 准入边界），
  不可重试（重试必然再成环，分类唯一表 Cycle → NoRetry），错误文本带
  完整环（哈希序列 + 目标自身收尾）。
- 深度上限 64：路径长度达上限后出站 Call 不再追加（降级直通，深链真环
  不判的接受性降级），计 `rans.call.causal_capped`（Counter，告警面）。

### Defer 延迟应答（HR4 H12 决策 16 前半，纯新增 API）

- 新增 `rans.Defer(ctx) *rans.Responder`：Handle 内调用，把本请求标记为
  延迟应答并取回应答器；Handle 随后必须返回 `(rans.Deferred, nil)` 哨兵
  ——投递边界据此不回包（本地与远端同一判读矩阵），等待 Responder 收场。
- `Responder.Reply(v, err)` 与 ctx 解耦，可存实体状态由后续任一消息的
  Handle 调用（聚合应答形态）；幂等（仅首次生效）；本请求 deadline 已过
  后调用丢弃且无效；err 非 nil 按 Handle 错误边界折叠后送达。
- 只 Defer 不回复的请求由调用方 deadline 到期以 Timeout 收场（收端不自建
  到期定时）；Handle 返回 Deferred 后请求不计入执行看门狗——延迟应答不算
  卡死。
- 误用形态：非请求 Handle ctx（含 Tell 单向投递）调用 Defer、同一请求上
  重复 Defer → panic 于调用点；返回 Deferred 却未 Defer、Defer 后返回业
  务值 → 投递边界回 `CodeError`+`ReasonHandler` 错误应答（哨兵值绝不当作
  成功回包）。

### ReenterAfter 让出重入（HR4 H12 决策 16 后半，纯新增 API）

- 新增 `rans.ReenterAfter(ctx, work, then)`（与 Defer 两件套，也可单独
  Tell 形态使用）：work 在实体 loop 外执行（wctx 继承请求 deadline/trace/
  因果路径但**不追加自身哈希**——让出期间 loop 空闲不构成等待边，work 调
  回本实体正常完成、调回仍在同步等待的上游立即判环）；then 作为续体任务
  回实体 loop 串行执行。
- 交接窗等待在途 work（预算内，超时取消 wctx；等待期 loop 只处理续体，
  新邮件按 H6 留箱）；看门狗强弃立即取消 wctx；执行看门狗只统计 loop
  同步执行段（续体重新计入）。
- MsgCancel 取消面扩展：work 在飞 → 取消 wctx；已 Defer 未回复 → 标记
  取消（后续 Reply 丢弃）。

### 传播键 RegisterPropagation（HR4 H10 决策 8，纯新增 API）

- 新增 `rans.RegisterPropagation(key, encode, decode)`：init 期全局登记
  传播键，值经编解码函数随调用过线（头部扩展块字段 4，重复
  `{1=name, 2=value}` 项）；不登记的键 = 仅本地（edge claims 语义保持）。
- 护栏：单值 4 KiB、单请求合计 32 KiB，超限丢弃该键 + 计数
  `rans.prop.value_dropped`/`rans.prop.total_dropped`（不使调用失败）。
- **本地投递改为重建 Handle ctx**：继承调用方 ctx 链 + 登记键
  `decode(encode(v))` 形态遮蔽（与远端解码注入同形态——INV6 位置透明）；
  调用方取消穿透保持。

### MsgCancel 尽力取消（HR4 H10 决策 9，新消息类型）

- 新增消息类型 `MsgCancel`（=15）：调用方 ctx 因**主动取消**（非预算到
  期）收场时异步发往目标节点，按（源节点, MessageID）定位：执行中 → 取消
  其 Handle ctx；在箱 → 标记出队即弃（计 `rans.call.canceled`）。幂等、
  迟到无害；链式经 Handle ctx 取消自然传播；本地路径保留直接取消穿透。

### 出站发件箱与传输去阻塞（HR4 H11）

- `transport.Conn.Send(ctx, hdr, body)` 尊重 ctx；新增 `TrySend`；控制帧
  （Ping/Pong/Retire）走独立优先通道（容量 64）不被
  数据背压饿死（writeLoop 控制帧优先预检）。**勘误（随 Unreleased 批次
  修正）**：本条初版声称「接管通知/重定向提示」同走优先通道，与实现
  不符——重定向提示实走数据通道（提示是可丢加速面，正 TTL 兜底，行为
  保持）；接管通知在 v0.23.0 时点发送半边尚未接线（发版承诺脱节），
  接线后走优先通道（见 Unreleased「接管通知发送半边接线」）。
- 节点发件箱（`node.Config.Outbox{Capacity 缺省 65536, Shards 缺省 16}`）：
  单向出站入箱即返，worker 按目标节点哈希分片保同目标 FIFO；满策略按
  生产者分野——显式 Tell 阻塞至 ctx 到期，事件扇出/错误回包/仲裁输家
  转发/正常应答帧满即丢 + 计数 `rans.outbox.dropped`（INV5：实体 loop
  与读循环零出站 IO）。指标：`rans.outbox.depth`/`dropped`/`send_failed`。
- `EventHub.Emit` 改走发件箱（路由梯子留发送腿、消亡的是满箱准入阻塞与
  网络等待段）；现 replyBudget 5s 同步重试链删除（异步预算归发件箱 30s）。

### 目录键值 v3 与成员代际（HR4 H13，同集群双端同版本断点）

- **实体主键改逐实体 hash tag** `rans:e:{<kind>/<key>}`（目录真分片；
  `rans.NewID` 禁身份段花括号）；条目值 v3
  `{"node":N,"epoch":E,"inc":K,"v":3}`；**主键无 TTL 永不清删**——注销
  写无主墓碑（保留 inc，纪元单调跨墓碑存续）；崩溃残留无害躺置（有效性
  由成员代际判定）。旧键族（`{dir}` 集中 tag/class·node 索引/inc 共享
  Hash/心跳键）全部废弃，**不做兼容读取**。
- `directory.Owner` 扩三元组 `{Node, Epoch, Incarnation}`；
  `Register(ctx, id, self, expected *Owner)` 四分支（无值/墓碑收养、同
  节点重注册——含 epoch=0 注入形态收养、CAS 接管、输返回现归属）；
  `Transfer` 并入合并端口（能力断言形态消亡）。三个原子**单键** Lua
  （任意 slot 合法，cluster 吞吐横切）；《规格》§11.1 同步改写。
- **续租机制整体拆除**（H13 决策 23）：LeaseKeeper/HeartbeatBatch/
  LeaseDirectory/class·node 索引/CleanupExpired·CleanupSweeper/
  `node.Config.Tuning.CleanupInterval`/DirectoryWatcher+BROWSE 全量轮询
  全部删除；`redis.Options.LeaseTTL` 删除（HeartbeatTTL 保留）。
  **所有权丢失发现**：死 owner 激活 CAS 接管（升纪元覆写，误判由 inc
  围栏 + SelfFence 兜底）+ 接管赢方直发 `MsgOwnershipLost`（=16，控制
  帧优先通道）令旧节点归属全等的实例毫秒级关准入+销毁。
- **SelfFence 迁 Liveness**：成员键写持续失败超阈值 → LocalIDs 全集关
  准入 + 止损销毁（`node.Config.Lease.SelfFence`，缺省
  max(MemberTTL−2×Interval, Interval)，<0 关闭）；失败幕恰一次。
- 优雅停机批量墓碑化：排空阶段②逐实体 Retire 的条件注销即墓碑（停机后
  本节点条目全 Lookup miss）。

### 重定向提示（HR4 H13 决策 24，新消息类型 + Error 帧字段）

- 新增消息类型 `MsgRedirectHint`（=17）：非 owner 节点仲裁落败转发请求
  的同时直发源端缓存加速帧（`{1:node, 2:epoch}`）——源端即时改写位置
  缓存（不等正 TTL 30s；提示丢失由 TTL 兜底）。**不裁决 pending**（请求
  本体已转发，正常应答从赢家到达）。
- Error 帧增字段 6 = owner 嵌套对象（`{1:node, 2:epoch}`，零值不上线；
  畸形嵌套不致错误帧失败）：终态 StaleLocation 错误携带正确归属。

### 放置正缓存与热路径分片（HR4 H14/H15）

- 放置正缓存：routeMiss 选点结果回填 5s 短 TTL 正条目
  （`LocationCache.PutPositiveTTL`）——只覆盖「选点→目录注册可见」的
  激活窗；负缓存 1s 防穿透语义不变。
- LocationCache 64 分片（饱和准入 O(分片容量)，EX7：10 万容量全表
  3.31ms → 亚毫秒）；容量语义分片均摊，`MaxEntries` 字段改
  `SetMaxEntries`。
- 网关 pending 表 256 分片 + 到期最小堆（扫描器 250ms 周期不变但只看
  各分片堆顶）+ 按节点二级索引（`FailPendingForNode` 只随该节点在途数
  线性）；容量拒绝保持全局精确（附录 A NET-8 不变）。

### 破坏性 Go API 变更汇总（本节为 v0.23.0 全量清单的索引）

- 配置：`Calls.RequestTimeout` → `Calls.Default`/`Calls.Max`；
  `redis.Options.LeaseTTL` 删除；`node.Config.Outbox`/`Tuning.CleanupInterval`
  增删；`LocationCache` 构造签名加容量形参。
- 目录面：`Owner` 扩 epoch；`Register`/`Transfer` 新签名（见上）；
  `LeaseDirectory`/`Lease`/`Browser`/`LookupByClass`/`LookupByNode`/
  `UnregisterNode`（旧签名）/`BrowseAll`/`CleanupExpired` 删除；
  `NodeRegistry` 增 `MemberTTL`/`UnregisterNode`（成员注销新语义）。
- membership：`LeaseKeeper` 删除；`LivenessConfig` 增 SelfFence 族字段。
- 语义变化：v2「租约过期 Lookup miss」收敛谓词改「成员视图淘汰 + 接管」；
  被窃取方的本地陈旧实例不再承诺限时自毁（止损归接管通知/闲置钝化/纪元
  围栏）。

## v0.22.0（2026-10-10）

主题：**HR3 批（2026-10-08 架构评估 S1 + 加固 H9）**——包结构按角色重整
（公开面收敛 + 实现细节全量 `internal/`）、统一调用面与统一错误模型、
握手 v3 线协议断点一次收口。设计正本
`docs/superpowers/specs/2026-10-08-structure-naming-conventions-design.md`
（下称《约定 spec》；逐符号映射表 §4.2 是消费方迁移的正本）与
`docs/superpowers/specs/2026-10-08-runtime-hardening-design.md` §6.1（H9）；
裁定正本 `docs/decisions/2026-10-09-hr3-package-structure.md`（SD1/SD3–SD10）、
`docs/decisions/2026-10-09-hr3-wire-v3-breakpoint.md`（HD6）；消费方割接
操作正本 `docs/consumer-upgrade-notice.md` v0.22.0 节。

### 部署断点（升级必读，不可静默丢失）

- **握手 v3 + 头部扩展块 + Error 帧结构化 = 同版本断点（滚动不兼容）**：
  握手体从定长尾结构（v2）升为版本区间协商 + 可扩展对象
  `[VerMin 2B][VerMax 2B][扩展对象][Nonce 16B][MAC]`——v2/v3 无兼容读取，
  升级窗内新旧二进制不可混跑（同集群双端同版本）。**建议操作序：先停全部
  再升**——全量停机 → 逐节点替换二进制 → 启动；操作细节见
  `docs/consumer-upgrade-notice.md` v0.22.0 节①。
- **此后线协议恢复演进能力**（HD6 承诺，HR3 起）：同一 VerMax 内向后兼容
  （新字段只追加字段号、旧端跳过）；破坏性变更必须提升 VerMin 并在
  CHANGELOG 标注「滚动不兼容」。SchemaDigest 从握手门禁降级为观测
  （失配只告警 + 计数，连接保持），kind 集不再参与握手——新增可选字段、
  新增 kind、单节点启用 edge 都不再互拒握手；CI 侧配套
  `ransgen -check` 基线兼容检查（`make schema-check`）承担发布前一致性。

### 破坏性 Go API 变更（升级必读，不可静默丢失）

**Go API 大规模破坏**——公开面从 22 个库包收敛为 9 个
（`rans`、`node`、`directory`(+redis/inmem/contract)、`checkpoint`、`edge`、
`codec`、`testkit`、`cmd/ransgen`），`identity`/`contracts`/`manager`/
`remote`/`host`/`membership`/`lifecycle`/`wire`/`transport`/`routing`/
`metrics`/`codegen` 等包**整包删除或内部化**。逐符号迁移正本 =
《约定 spec》**§4.2 映射表**（当前 → 目标全表，含 host 选项 →
`node.Config` 字段逐行对照），此处仅列主干，不在此重复全文：

| 变更（主干） | 消费方动作 |
|---|---|
| `identity.UUID/New(kind,inst)` → `rans.ID{Kind,Key}`/`rans.NewID(kind,key)`（`String()` 仍 `kind/key`，Redis 键无需迁移） | 引用改名 |
| `manager.Call`/`remote.Call`/`remote.SendOneWay` → `rans.Client.Call/Tell`（经 `Node.Client()` 或 `rans.ClientFrom(ctx)`） | **本地专用调用入口已删除**（EX11）——一切调用经路由；task poller 类终态投递必须改走 `Node.Client()` |
| `host.New(opts...)`/`remote.New(Config)`/`manager.New(...)` → `node.New(node.Config{...})` | 装配面统一；host 选项逐行映射见《约定 spec》§4.2「host 选项」表 |
| 错误面：`manager.ErrTimeout`/`*routing.TimeoutError`/`ErrNodeUnavailable`/`ErrSendFailed` 等 → `*rans.Error` + 哨兵（`errors.Is` 判 `rans.ErrTimeout`/`ErrUnavailable`/`ErrNotFound`，`CodeOf`/`ReasonOf` 取分类） | 判错点改统一模型；调用返回值不再可 Unwrap 到实体原始错误 |
| `manager.EntityFactory func() Entity` → `rans.Factory func(id rans.ID) rans.Entity`；`manager.MigrationCarrier` → `rans.Snapshotter` | 实体契约随 `rans` 包迁移 |
| `wire.RegisterValue/EncodeValue/DecodeValue` → `codec.Register/Encode/Decode`；`contracts.MessageType/StatusCode` → `internal/protocol`（不公开）/`rans.Code` | 手写序列化器改 import `codec` |
| `metrics.Snapshot/SetMeterProvider`、`ranslog.SetLogger` → `Node.Metrics()` + OTel 全局 provider + `rans.SetLogger` | 观测接入面改 node 快照/标准 OTel |
| 全部 21 个 `Set*`（manager/HeartbeatService/tcp/Dispatcher/Gateway/EventHub）删除 → `node.Config` 构造时一次给全 + `KindOptions` per-kind 收拢 | 构造期重排；测试注入走 `TestHooks`（仅 testkit 形态） |
| `MigrateAsync/OffloadIdleAsync` → `Node.Migrate(ctx, id)`（去 Async）；`Host.Kill` → testkit `Kill`；`Remote.Drain` → `Node.Shutdown` 内置 | 调用点改签名（管理操作只作用于本地实例：非本地 `NotFound`+`NotLocal`） |
| `codegen/ransgen` → `cmd/ransgen`；生成物 wrapper/代理随 `rans` 契约重生成 | **`go generate` 全量重生成并入库**（`//go:generate` 指令路径随改） |
| `edge.ListenerConfig` → `edge.Config`（经 `node.Config.Edge`）；`edge.NewEventHub()`+`SetSource`/`SetCaller` → `edge.NewEventHub(id, ringCap)`（出站经 `rans.ClientFrom(ctx)`） | edge 接入改 node 装配；EventHub 两段式注入删除 |

### 行为变化（消费方需知，约定 spec §1 逐条）

1. **删除本地专用调用入口**（EX11）：一切调用经路由（`Client.Call/Tell`），
   本地命中走本地快路径——workspace 归属他节点时自动路由投递，不再返回
   `busy/unavailable`。钉死：TestHR3_UnifiedClientRoutes。
2. **统一错误模型**（EX12）：本地与远端错误同构 `*rans.Error`；Handle 返回
   的错误在投递边界折叠（`CodeError`+`Reason=Handler`，AppError/AppDataError
   带 AppCode/AppData 过线），调用方不能 Unwrap 到实体的原始错误（不会把
   实体内部嵌套调用的运行时错误误判为本次调用状态）。钉死：
   TestHR3_UnifiedErrorModelLocalRemoteUniform / TestCallFlipToLocalHandlerErrorNoDuplicateExecution。
3. **Error 帧结构化**（线上，与 H9 同期）：Error 帧 body 由纯文本改为
   field-tag 对象 `{1: code, 2: reason, 3: message, 4: app_code, 5: app_data}`
   ——码与原因可过线、可 `errors.Is`。
4. **未知 kind 本地远端一致**：`NotFound` + `Reason=UnknownKind`（促重路由），
   不再回 DecodeError；异构 kind 节点可同集群共存（kind 维退出握手）。
   钉死：TestHR3_UnknownKindLocalRemoteUniform。
5. **管理操作显式本地语义**：`Node.Destroy`/`Node.Migrate` 作用于非本地
   实体返回 `NotFound`+`Reason=NotLocal`，不再静默成功或返回各异哨兵——
   消费方能分辨「已销毁」与「不在本节点」。钉死：
   TestNodeDestroy_NotLocal / TestNodeMigrate_NotLocal。
6. **配置构造时一次给全 + fail-closed**：全部 `Set*` 删除，`node.New`
   构造期全量校验；非单节点形态下安全配置为零值（未配 HMAC 且未显式
   `Insecure`）拒绝启动。钉死：TestNodeNew_RequiresHMACOrInsecure。

### 新增面（非破坏，按需采用）

- `rans` 契约包：`ID/NodeID/Entity/Factory/Snapshotter/Client/CallOption`
  （`Idempotent()`）、`Error/Code/Reason` + `NewError/CodeOf/ReasonOf` +
  哨兵族、`AppError/AppDataError` 业务错误接口、ctx 访问器
  `Self/Incarnation/ClientFrom`、`SetLogger`；
- `node` 组装包：`Config`（全字段构造期校验）、`Node`（`Start/Shutdown/
  Close/Run/Addr/Client/Destroy/Migrate/LocalIDs/EdgeAddr/Metrics`）、
  `LoadHMACKeyFromEnv/LoadHMACKeyFromFile`、`Placement`/`RandomPlacement`；
- `codec` 公开编码面：`Register/Encode/Decode/Serializer` +
  `RegisterShapes`/`SchemaDigest`（生成物 init 自动登记，随握手观测）；
- `SecurityConfig.StrictSchema`：SchemaDigest 严格门闸（默认观测模式）；
- `cmd/ransgen -check` + `rans_schema.json` 基线兼容检查（`make schema-check`
  门禁；基线 `cmd/ransgen/testdata/schema_baseline.json`）；
- 仓库根导入图门禁 `depgraph_test.go`（`TestImportGraph_AllowedEdges`，
  《约定 spec》§3.2 表逐条锁定）。

## v0.21.0（2026-10-09）

主题：**HR2 批（2026-10-08 架构评估 H6–H8）**——register-first 激活与实例级
所有权（H7）、交接窗与排空次序（H6）、pawnite 终局归一（H8）一次收口；随批
交付 testkit 故障注入与不变量检查器（spec §9）。设计正本
`docs/superpowers/specs/2026-10-08-runtime-hardening-design.md`
（§5.1–§5.3、§9），语义裁定正本
`docs/decisions/2026-10-09-hr2-register-first.md`、
`docs/decisions/2026-10-09-hr2-unavailable-retry.md`，逐项销账记录见
`docs/ISSUES.md` H6/H7/H8/X11/V3③ 行。
**同集群双端同版本断点**——Redis 目录键值 v2 + 第四 Lua（注册取号原子域），
升级窗内新旧二进制不可混跑；线协议/握手指纹/节点成员键格式零变化，Go API
破坏全部是编译期的。

### 部署断点（升级必读，不可静默丢失）

- **Redis 目录键值 v2 + 第四 Lua**：实体目录主键值从裸纪元升为
  `{"nodeId":N,"inc":K,"v":2}`（K 即激活纪元），不做 v1 兼容读取。v1 残留
  条目由第四 Lua（luaRegister）按固定语义处理（非兼容读取）：`nodeId` 匹配
  → 自指向收养覆写为 v2（升纪元）；不匹配 → 输，返回的 owner 只含节点号
  （inc=0）；其余残留由租约 TTL（≤5min）腾位。四个 Lua（注册取号/条件注销/
  迁移换锁/批量续租）全部 `{dir}` slot 单域多键操作；
- **建议操作序：「先停全部再升」**——全量停机 → 逐节点替换二进制 → 启动；
  或蓝绿切换（新版本集群整组就绪后切流）。判活键 TTL 2min、租约 TTL
  ≤5min，切换完成后残留键自然排空；升级操作细节见
  `docs/consumer-upgrade-notice.md` v0.21.0 节①。

### 破坏性 Go API 变更（升级必读，不可静默丢失）

**directory 面**（`directory`/`inmem`/`redis`/`contract`；能力拆分定形：
`Directory{Register, UnregisterIfOwner(Owner), Lookup}` +
`TransferDirectory{TryTransferOwnership(Owner…)}` + `LeaseDirectory{HeartbeatBatch([]Lease)}`）：

| 变更 | 消费方动作 |
|---|---|
| `directory.Location{NodeID}` 删除 → `directory.Owner{Node uint16; Incarnation uint64}` | 类型引用改名 + 字段 `NodeID`→`Node`、补纪元 |
| `Directory.TryRegister(ctx,id,Location)(bool,error)` 删除 → `Register(ctx,id,node uint16,leaseMs int64)(won bool, inc uint64, owner Owner, err error)`（原子取号+注册一域，取号面 `IncarnationSource`/`NextIncarnation` 同批删除） | 调用点改签名；纪元从 Register 返回值取（输家经 owner 转发；leaseMs 传 0 即目录配置租约） |
| `UnregisterIfOwner(ctx,id,nodeID uint16)` → `(ctx,id,owner Owner)`；`Lookup` 返回 `(Owner, bool, error)` | 实例级匹配（node+inc 同时命中才删） |
| `LeaseDirectory.HeartbeatBatch(ctx,[]UUID,owner)` → `(ctx,[]Lease,owner uint16)`；新类型 `directory.Lease{ID, Incarnation}` | 续租条目带纪元 |
| `TryTransferOwnership(ctx,id,expectedOwner uint16,newLoc Location)(bool,error)` → `(ctx,id,expected Owner,newNode uint16)(newInc uint64, ok bool, err error)` | 换锁期望归属=本实例 Owner |
| `directory.NodeRegistry.RegisterNode(ctx,node,addr,kinds)` → `RegisterNode(ctx,node,NodeInfo{Addr, Kinds, Draining})` | 自定义实现/测试桩改签名（三视图过滤语义见行为变化） |
| 契约套件：`RunSuite`/`RunIncarnationSuite` 删除；`RunLeaseSuite` 改签名（newDir 返回 `LeaseSuiteDir` = LeaseDirectory+TransferDirectory，非删除） | 自定义目录实现改用 `RunRegisterSuite`/`RunLeaseSuite`（实例级语义）锁定 |

**membership 面**：

| 变更 | 消费方动作 |
|---|---|
| `HeartbeatService.Track/Untrack` 增 `inc uint64` 形参 | 直调方补纪元 |
| `HeartbeatConfig.OnLost` → `func(ctx, id, inc uint64) error` | 回调内按 {id, inc} 匹配销毁（`mgr.DestroyCtxInstance`） |
| `NewHeartbeat`：Dir 须实现实例级续租面（`HeartbeatBatch([]Lease,…)`），缺失构造报错 | 自定义租约目录补面 |
| 续租接线退役：`mgr.SetOnActivated(hb.Track)` → `mgr.SetLeaseTracker(hb.Track, hb.Untrack)`（Track 前移到注册成功点） | 手工接线改形式（经 `host` 装配零改动） |

**manager / routing / pawnite / lifecycle 面**：

| 变更 | 消费方动作 |
|---|---|
| 哨兵删除：`manager.ErrLostArbitration`、`manager.ErrMigrationUnsupported` | 判 `*ArbitrationLostError`（输家同步窗）/ `ErrMigrationLost`（迁移输，链携带输因；裸 inmem 目录亦可迁移——缺换锁能力=迁移输） |
| `ArbitrationLostError.Winner` 与 `SetOnArbitrationLost` 回调：`directory.Location` → `directory.Owner`（`WinnerNode()` 方法不变） | 回调签名与字段访问适配 |
| `routing.LocationCache.Get/PutPositive`：`directory.Location` → `directory.Owner` | 形参/返回适配 |
| `lifecycle.IdleCollector` 注入面替换：`Snapshot`/`Destroy` 两 func 字段删除 → `Passivate func(ctx, id) error`（钝化收敛为交接窗单调用） | 直接构造 `IdleCollector` 的消费方改注入面（规范构造点在 `manager/idle.go` 内部——经 `host` 装配零改动） |

**随批新增（非破坏，按需采用）**：`Manager.DestroyCtxInstance/SetLeaseTracker/
Passivate/AwaitCheckpointSettled`、`pawnite.Handoff/HandoffKind/HandoffOutcome/
NoteArbitrationLost/TerminalReason`、哨兵 `manager.ErrMigrationNoStore`
（有状态实体未配置 Store 时迁移被拒）、`remote.Drain` 增 variadic 选项
`WithTeardownHead`（原单参调用面兼容）、`directory.NodeInfo`、testkit
`InvHistory`（CheckStrict/CheckCrashBound）`/FaultStore/WithLeaseFaults`。

### 行为变化（消费方需知）

- **register-first 激活次序**（H7，EX3）：目录注册（原子取号）先于实体
  装载；仲裁输家**零用户代码**——不跑恢复/Activate/Deactivate，直接整箱
  转发赢家（输家钩子拿到更大令牌毒化 fence 的病根消除）。钉死：
  TestRegisterFirst_LoserZeroUserHooksAndOwnerFenceIntact / TestArbitrationLostErrorCarriesWinner。
- **激活失败回绝 `StatusUnavailable` + 双哨兵**：恢复失败/Activate 失败的
  实例按 Unavailable 回绝调用方（本地错误链 `ErrActivationFailed`+`ErrBusy`
  双哨兵，`errors.Is` 两判成立），可重试。钉死：TestActivationFailure_RejectsWithUnavailableDualSentinel。
- **续租按实例纪元跟踪**（H7）：续租判据是 {owner, 纪元} 实例标记——旧纪元
  实例的迟到续租不重置新实例租约（failed 集 → 本地自毁只伤旧实例）；
  失租销毁 `OnLost` 按 {id, inc} 精确匹配。钉死：
  TestHeartbeatStaleInstanceLeaseFails / TestHeartbeatTrackUntrackInstanceEpoch
  / TestDestroyCtxInstance_EpochGuarded。
- **残留自愈 + 陈旧清理异步化**（H7）：目录残留条目（进程重启孤儿/注销残留/
  v1 旧条目）由下次激活的自指向收养（升纪元覆写）+ 租约 TTL 腾位；cleanup
  不再做节点粒度兜底注销——本地表 miss 不再等待目录注销完成（读循环零
  目录 IO）。钉死：TestEvictUnregisterDoesNotDeleteNewerRegistration。
- **换锁围栏时点前移**（H7）：迁移目标端「恢复即落库」，换锁窗以目标端
  首写持久化为界——旧纪元写窗最小化。钉死：
  TestMigrationFence_RestorePersistsImmediately
  / TestMigrationFence_WindowClosesAtTargetFirstPersist。
- **交接窗与终快照零丢失**（H6，EX9/EX10）：销毁/钝化/停机三路先开交接窗
  （在箱消息留存、准入照收）→ 终快照落库 → 再注销/终局——已确认写不再因
  优雅停机/钝化丢失，快照先于 Deactivate；终快照写失败预算内重试（3 次+
  退避），耗尽记 `rans.checkpoint.final_failed` 后照样终局。钉死：
  TestHandoffPassivateWindowZeroLoss / TestHandoffRetireFinalSnapshot
  / TestHandoffRetireFinalSnapshotBeatsStaleCheckpoint
  / TestHandoffPassivatePutFailureRollsBack。
- **优雅销毁期间同 id 再激活串行化到注销完成**：注销 IO 在飞时同 id
  GetOrActivate 命中垂死实例（消息留箱、终局按确定未执行回绝、重试落新
  激活）——新实例 Activate 不与本实例 Deactivate 重叠（有界：注销 5s
  预算 + 看门狗兜底；强弃路径的异步化语义不变）。
- **重试分类确定化**（H6，HD4）：`StatusUnavailable` 从模糊类改**无条件
  重试**——排空门闸/LocalOnly/激活失败的回绝对调用方确定未执行；实体
  Handle 错误在投递边界折叠（嵌套路由类错误不再诱发重复执行）。**延迟
  变化**：非幂等调用对排空门闸/LocalOnly 回绝经至多 3 次尝试+退避才见错
  （语义正确、耗时增加 ~60ms 量级）。钉死：
  TestCallUnavailableRetriedUnconditionally。
- **排空门闸按目标区分**（H6）：排空中本地表命中**放行**——尚未进 Retire
  交接窗的实体照常执行（非幂等调用成功的语义锚），已进交接窗实体留箱、
  注销后按确定未执行回绝并由调用方环内自动重路由他节点（中间 Unavailable
  不再可见）；本地 miss 回 Unavailable 促重路由（排空节点零新激活成结构
  性质）。钉死：TestDispatcherDrainGateAdmitsLocalWhileDraining
  / TestRemoteDrainGateRejectsNewRequests。
- **排空期迁移目标方向竞态窗（有界瞬态）**：他节点 rebalance 仍可能选中
  排空节点——迁移按输裁决回退、重试自愈，不丢数据；排空时长可能被拉长
  一个迁移窗，无正确性影响。
- **节点成员键增 `draining` 字段，旧读端透明**：`rans:{reg}:node:*` 值
  JSON 增 `draining`（`omitempty`——false 不写，旧格式自然解析）；直读
  原始值的脚本需容忍该字段，经 `NodeRegistry` 接口消费零改动。
  `NodeIDs`/`NodeAddrs`/`NodesByKind` 一致过滤 draining 节点（放置让位，
  语义=「可承接放置集」），`NodeAddr` 单点解析不过滤（在途应答照常回程）。

### 新增

- **`pawnite.Handoff(ctx, kind, target, core)`——交接窗原语**（迁移窗的
  泛化正本，H6）：三种 kind（`HandoffMigrate`/`HandoffPassivate`/
  `HandoffRetire`）表驱动组合规则（窗内 Migrate/Passivate 同步拒、Retire
  幂等跟随、任一窗内看门狗强弃 → 终局 `TerminalAbandonedInHandoff`）；
  core ctx = hookCtx 派生 + 调用方预算 ctx 的取消并集（X11 销账——强弃可
  取消卡死 core）；core panic 折输不折成功。钉死：
  TestHandoffCombinationMatrix / TestHandoffCorePanicLoses
  / TestAbandonCancelsStuckHandoffCore。
- **`Manager.Passivate(ctx, id)`**：钝化公共导出面（终快照落库 → 终局；
  失败回退 Active 下轮再试；无 Store 有状态实体拒绝）+ 闲置回收
  CollectPassivate 接线；`Manager.AwaitCheckpointSettled` 等待在飞快照
  结算（迁移 core 首步避让面）。钉死：TestIdleCollector_PassivatesCandidates
  / TestAwaitCheckpointSettled。
- **`remote.Drain(WithTeardownHead(fns...))` + 停序调整**：新停序 = 门闸+
  draining 标记（心跳继续）→ 并行 Retire → 出站 flush → 停心跳 → 节点
  注销 → 关传输；宿主自有组件（续租/轮询）收口经 teardown 头插到停序头
  ——停心跳先于节点注销，防在途续租把注销过的成员键写活（手工装配方
  迁移路径见升级通知 v0.21.0 节）。`membership.HeartbeatService.SetDraining`
  即时发布（kick——标记不等下一周期进入放置视图，续租路径零影响）。钉死：
  TestRemoteDrainTeardownHeadRunsBeforeTransportClose
  / TestShutdownRenewsLeaseAndNodeKeyDuringDrain
  / TestHeartbeatSetDrainingPublishesAndKeepsRenewing。
- **指标 `rans.checkpoint.final_failed`**：销毁/钝化终快照预算内重试耗尽、
  照样终局时的 Error 计数（丢失面收敛为「无快照可恢复」并告警留痕）。
  （`rans.lease.self_fenced`/`rans.activation.failed` 为 v0.20.0 已有。）
- **pawnite 终局归一**（H8）：write-once terminal + `disposeLate` 单点 +
  abandon 合并——5 条终局路径收口单点处置；`TerminalReason` 六值枚举
  （TerminalRetired/TerminalMigrated/TerminalArbitrationLost/TerminalAbandoned
  /TerminalAbandonedInHandoff/TerminalActivationFailed）+
  `NoteArbitrationLost(winner)` 输家导出面。钉死：
  TestSetTerminalLocked_WriteOnce / TestDisposeLate_Matrix
  / TestAbandonDuringLateDisposalSuppressesFinish。
- **testkit 故障注入与不变量检查器**（spec §9）：`FaultStore`（Put 失败/
  慢写注入）、`WithLeaseFaults` + `Cluster.DropLeases/HealLeases`（续租
  丢键的集群集成面）、`InvHistory.CheckStrict/CheckCrashBound`（崩溃一致
  性断言）+ 8 场景验收矩阵。钉死：TestInvHistoryCheckStrict
  / TestAcceptance_InvMatrixLeaseLoss / TestLeaseFaultDir / TestFaultStoreSlowPuts。
- **生成器快照面透传**：生成 wrapper 转发实体的
  `CaptureSnapshotBytes`/`RestoreFromSnapshotBytes`（生成形态实体可用
  checkpoint/迁移/钝化快照面）+ 快照面形状门禁。钉死：
  TestWrapperSnapshotCarrierFace / TestEntitySnapshotFaceShapeGate。

### 兼容性

| 变更面 | 旧→新 | 影响 |
|---|---|---|
| 线协议 / 握手指纹 | v0.20.x ↔ v0.21.0 | **零变更** |
| 节点成员键值 | v0.20.x ↔ v0.21.0 | 增字段 `draining`（omitempty，旧读端透明） |
| Redis 目录键值 | v0.20.x ↔ v0.21.0 | **v1→v2 断点**（无 v1 兼容读取）——同集群双端同版本，先停全部再升 |
| Go API | 见破坏性变更表 | 编译期破坏（无运行时断点；经 `host` 装配零改动） |

## v0.20.0（2026-10-08）

主题：**HR1 止血批（2026-10-08 架构评估 hardening §4 线，H1–H5）**——
心跳环冻结、激活空状态覆盖持久数据、收件箱预分配内存、激活串行化、
文档漂移五项确证风险一次收口。设计正本
`docs/superpowers/specs/2026-10-08-runtime-hardening-design.md`（§4.1–§4.5），
实施计划 `docs/superpowers/plans/2026-10-08-hr1-stop-the-bleeding.md`，
逐项销账记录见 `docs/ISSUES.md`「2026-10-08 架构评估新增」H1–H5 行。
**线协议与 Redis 键方案零变化——可滚动升级**（破坏性变更全部在 Go API
编译面，无混跑断点）。

### 破坏性 Go API 变更（升级必读，不可静默丢失）

- **`membership.NewHeartbeatService` 构造面整体替换为
  `membership.NewHeartbeat(cfg HeartbeatConfig) (*HeartbeatService, error)`**
  （返回类型仍是 `*HeartbeatService`，变的是构造面）：
  `HeartbeatConfig{Dir, Self, Interval, Fence, OnLost,
  MaxConcurrentDestroys, DestroyTimeout, SelfFence}`——`Fence func(id)`
  在租约环上同步逐实体调用于销毁之前（关准入，毫秒级返回）；
  `OnLost func(ctx, id) error` 在 `MaxConcurrentDestroys`（缺省 8）个
  worker 上带 `DestroyTimeout`（缺省 30s）预算异步执行；
  `SelfFence` 缺省 `max(LeaseTTL−2×Interval, Interval)`、<0 关闭。
  **旧 `onFailure func(ids []UUID)` 的同步回调语义删除**——同步 Destroy
  冻结心跳环正是 H1 病根（EX5：刷新周期 100ms，实测最大间隔 1.8s），
  消费方不得也不需在回调里自建异步化（迁移路径见
  `docs/consumer-upgrade-notice.md` v0.20.0 节）。host/testkit 内部接线
  已随迁（经 host 包装配的消费方无感）。
- **`manager.EntityFactory` 契约收紧**：工厂必须无副作用——激活取号 IO
  与工厂调用均移出全局创建锁（H4），工厂可能在激活竞争中被调用后整个
  丢弃（有副作用的工厂从此不再被锁保护，此前依赖锁串行化的写法会竞态）。

### 行为变化（消费方需知）

- **激活可能因 Store 读失败而失败**（H2）：`Store.Get` 的 IO 错误与快照
  解码错误都按激活失败收场（新哨兵 `manager.ErrRestoreFailed`，
  `errors.Is` 可判），调用方可重试——不再裸激活以空状态上线、再由首轮
  checkpoint 以更高纪元覆盖持久数据（EX4：`"1000"` → `"0"`）。钉死：
  TestRestore_IOErrorFailsActivationAndKeepsDurableState /
  TestRestore_DecodeErrorFailsActivation。
- **Deactivate 只与成功的 Activate 配对**（H2）：激活失败实例与仲裁输家
  不再调用用户 Deactivate（含 Activate 成功但注册前被 Destroy 抢占的
  僵尸实例）——用户 Activate 里的资源申请在失败路径上没有对应回收点，
  清理要么放 Deactivate 前的快照面、要么接受该窗口。钉死：
  TestDeactivate_PairedWithSuccessfulActivateOnly。
- **Store 恢复后首轮 checkpoint 不回写**（H2）：从 Store 恢复的实例首轮
  标记干净，刚读出的状态不会被原样回写一遍。钉死：
  TestCheckpoint_CleanAfterStoreRestore。
- **激活取号失败从同步错误改为异步激活失败**（H4）：目录纪元源
  `NextIncarnation` 失败不再同步阻断 pawnite 构造，已入箱消息经
  drainReject 回绝，本地调用方拿 `ErrActivationFailed` 后可重试
  （与目录不可用同语义）。

### 新增

- **`pawnite.Fence()` / `manager.Fence(id)`——租约失守关准入原语**：
  之后再投递一律 `StatusUnavailable`（"lease lost"）回绝，在途投递不受
  影响；幂等、非阻塞、只改内存状态。心跳失败集处理序：同步 Fence →
  有界异步销毁——任何销毁动作都晚于 Fence。
- **心跳双循环 + 有界异步销毁 + 本地租约自检**（H1）：`leaseLoop`（租约
  续期）与 `livenessLoop`（节点键刷新）分离互不阻塞；失败集先同步逐实体
  Fence 再入有界销毁队列（容量 1024，worker 池 + 在飞去重——同 id 在飞
  只保留一份，批量丢租不再串行累加冻结心跳环）；`SelfFence` 本地自检——
  本节点续租连续不可确认超阈值即 Fence 全部本地实体并留痕（节点键与全部
  续租停摆时先自杀止损，不等他节点视角）。钉死：
  TestHeartbeat_NodeKeyNotFrozenByBlockingDestroy /
  TestHeartbeat_BoundedConcurrentDestroysAndDedup /
  TestSelfFence_TriggersOnProlongedFailure。
- **`pawnite.SetIncarnation(inc uint64)`**：激活路径的取号落点（激活闭包
  首步取号后、实体 Activate 前调用）；首写 CAS 生效、后续静默忽略、
  0 = 未围栏哨兵。配套取号 IO 移出全局创建锁（H4）：K 个并发激活不再
  被串行成 K×RTT（EX6：20 并发 × 20ms 取号 404ms → <5ms）。钉死：
  TestGetOrActivate_IncarnationIOOutOfCreationLock。
- **本地单向投递补 fencing 纪元**（H4）：localOneWay 路径的 Handle ctx
  现在与远端投递同面携带 incarnation（此前本地路径漏挂，宿主条件写围栏
  在本地直达时失效）。钉死：TestLocalOneWay_HandleCtxCarriesIncarnation。
- **收件箱按需增长**（H3）：预分配 10,000 容量 chan 替换为切片 FIFO +
  信号量（按需增长、入箱前压缩已消费前缀、弹出即置 nil 槽、空闲回收大
  缓冲）——稳态内存 162.9 → 3.2 KiB/实体（EX1：1 万 edge 会话从约
  1.6 GiB 降到约 31 MiB 量级）。钉死：
  TestActivation_MemoryPerEntityWithinBudget。
- **指标**：`rans.lease.self_fenced`（本地自检触发轮数）、
  `rans.activation.failed{reason}`（激活失败计数，含 reason=restore /
  incarnation 等标签）。
- **文档漂移修正**（H5，七处）：README 周期 checkpoint「未实现」改为
  如实（v0.7.0 起随装配自动接线）、看门狗三形态口径统一（激活/卸载/
  执行）、销毁终快照按现状如实标注（终快照属 HR2 交接窗，现状不承诺）、
  X8「待裁」改为已裁定（v0.13.0 落地）、edge spec §2 的 Interleaved
  与环检测前提勘误；`remote/remote.go`、`lifecycle/drain.go`、
  `directory/redis/redis.go` 注释同步对齐实现。

### 兼容性

| 变更面 | 旧→新 | 影响 |
|---|---|---|
| 线协议 / 握手指纹 | v0.19.x ↔ v0.20.0 | **零变更**（可滚动升级、可混跑） |
| Redis 键方案 | v0.19.x ↔ v0.20.0 | **零变更** |
| Go API | `NewHeartbeatService` → `NewHeartbeat` | 编译期破坏（无运行时断点）；`EntityFactory` 契约收紧 |

## v0.19.0（2026-10-05）

主题：**edge 外部接入面 E3——订阅/事件/回放（EventHub）+ kind 感知放置 +
纯接入节点形态**；另收 E2 终审延后项收口批（闩世代复检/出站帧上限/Stop
硬停/仪表贯通）。设计正本
`docs/superpowers/specs/2026-10-04-edge-hosting-design.md`（§6/§7/§8/§10/§14
已随实现补落地注）。至此该 spec E1-E3 全部交付；消费方 SSE→WS 迁移评估见
`docs/consumer-upgrade-notice.md`。

### 破坏性变更（升级必读，不可静默丢失）

- **节点成员键值格式**：判活键 `rans:{reg}:node:<节点号>` 的值从裸地址
  字符串改为 JSON `{"addr":"...","kinds":[...]}`（kinds 恒为数组，nil
  规范化为 `[]`——纯心跳节点的合法形态）。旧值在新读面下是**坏值**（报错
  不 panic、不静默跳过）——**全集群同版本滚动升级**（先停旧再起新，无灰度
  窗口；判活键 TTL 2min，滚动完成后最迟 2min 自然排空；v0.8.0/v0.18.0
  断点同款要求）。经 `NodeRegistry`/`NodeAddrResolver` 接口消费的部署零
  改动（已核实全仓无绕过接口直读判活键的生产代码）。裁定正本
  `docs/decisions/2026-10-05-node-key-kinds.md`。
- **`directory.NodeRegistry` 接口扩展**：`RegisterNode(ctx, node, addr)` →
  `RegisterNode(ctx, node, addr string, kinds []string)`（传 nil 即旧行为，
  值里 kinds 为 `[]`）；另增 `NodesByKind(ctx, kind) ([]uint16, error)`。
  实现方/测试桩同步（本仓库唯一生产实现 redis 已实现；inmem 不实现
  NodeRegistry，无涉）。
- **`host.Kill()` 语义变化**：Kill = 整节点崩溃模拟，**含 edge 硬停**
  （`Listener.Stop`——关监听器 + 全部连接 CloseNow，无 Bye 无 Drain，会话与
  连接同死，客户端裸断连——真实崩溃本就如此）。此前 Kill 不触及 edge 面；
  优雅停机仍走 Shutdown（Drain+Bye，spec §8 注记）。

### 新增

- **EventHub（`edge` 包，spec §6）**：业务实体**可选嵌入**的订阅/事件内核
  ——`NewEventHub()`（环容量缺省 1024，`NewEventHubWithRingCap` 可调）+
  `SetSource`/`SetCaller` 两段式注入（激活后可得，形态见包注释）+ `Handle`
  委派（订阅族消息 SubscribeMsg/UnsubscribeMsg 在此终结并返回 Ack）+
  `Emit`/`EmitTo`（扇出全部订阅者/定向单订阅者——定向事件回放只给原受众）+
  `Subscribers()`（过滤器自筛）。hub 是实体状态**不持久化**（重连/重激活后
  重订阅，环从 0 起）。
- **订阅/回放语义（概要）**：`Subscribe`（可选 `LastEventID` 游标，0=新订阅
  不回放）→ 服务端**先**按序补发差量 `Event` 帧 → `CallResult` 帧承载
  `SubscribeAckMsg{BaseID, CurrentID, Replayed, ReplayInvalid}`；任一失效
  形态（游标纪元 ≠ 环纪元 / 游标早于环基线 / 游标超前前沿）→
  `ReplayInvalid=true` 零补发，客户端全量重建（SSE Last-Event-ID 同语义）。
  事件 ID = `incarnation<<32|seq`（纪元取自 Handle/Emit ctx 的
  `pawnite.Fencing`；ID 0 为「不回放」哨兵，高 32 位翻页=实体重激活环重置）；
  扇出 at-most-once（单订阅者 OneWay 失败记日志继续）。
- **会话侧订阅/事件帧翻译**：`SubscribeFrame/UnsubscribeFrame` 翻译为对目标
  实体的 `SubscribeMsg/UnsubscribeMsg` **普通 Call**（复用 per-target 链与
  CallResult 回包，零新增往返机制）；`EventMessage`（OneWay 从源实体扇出
  到会话）→ `Event` 帧写出（不占在飞信号量，写通道快路径）。
- **claims→ctx 注入（spec §7 L2 闭环）**：会话发起的一切出站调用（Call 与
  OneWay 同面）ctx 携带本会话声明，实体侧经 `edge.ClaimsFrom(ctx)` 读取。
  **仅同节点生效**——跨节点路径 wire 帧不承载 ctx 值，远端实体 Handle
  **永不携带声明**（跨节点细粒授权两形态：会话与实体同节点 / 业务协议载荷
  自带凭据）；`AuthorizeTarget` 粗粒门在边缘带 claims 执行、全局有效。
  claims 过线（wire Header 扩展）登记开放点（spec §14）。
- **kind 感知放置（spec §10）**：`membership.NewKindPlacement(scanner)`
  （实现 remote 侧鸭子接口 `KindPlacement.PickForKind`——放置按目标 kind
  过滤候选，纯接入节点不进业务候选；未实现该接口的 Placement 行为不变）；
  `remote.Config.LocalOnlyKinds`（入站 dispatcher 面守卫：kind 命中且本地表
  miss 的远端激活回绝 Unavailable）；host 缺省放置升级为 NewKindPlacement
  （redis 形态，消费方无感）。
- **纯接入节点形态**：`host.WithDeclaredKinds(names)` 声明集群全量业务 kind
  （握手指纹对齐）+ `WithKinds` 缺席 + `WithEdge` = 只承载 edge 会话、不承载
  业务实体的接入面（业务放置永不选中；`WithEdge` 自动
  `LocalOnlyKinds=[edge_session]`——会话激活永不放置远端）。
- **成员视图写入面**：`HeartbeatService.SetNodeKinds(kinds)`（Run 前注入，
  语义同 SetNodeAddr；不调用 = 纯心跳节点）。
- **E2 终审延后项收口（同批）**：`ensureSession` 一次性闩按实体 done 世代
  复检（会话实体被外部独立销毁后连接恢复服务，响应不再黑洞）；出站帧上限
  `ListenerConfig.MaxOutboundFrame`（缺省 4MiB，超限帧丢弃+Warn 不断连——
  与入站 FrameLimit 方向不对称是有意裁定）；`Listener.Stop`/`Edge.Stop` 硬停
  面（host.Kill 接线）；edge 仪表经 `ListenerConfig.Metrics` 注入
  （`host.WithMetrics` 贯通）。
- **E3 验收**（spec §12 E3 行）：`edge/e3e_test.go`
  `TestE3EAnyNodeAccessReceivesEvents`（任意节点接入都收到事件——跨节点
  事件缺失根治的判定测试）、`TestE3EReplayDeltaAcrossNodes` /
  `TestE3ERingOverflowFullRebuild`（重连跨节点差量回放/环溢出全量重建）、
  `TestE3EKillPartitionSuite`（kill/分区：事件 at-most-once、会话销毁收敛、
  游标跨激活 ReplayInvalid）；纯接入不被业务放置选中主锚
  `TestHostKindAwarePlacementPureAccessNeverPicked`（host，redis 门禁）。
  运行时观察两项（SendOneWay 零-deadline 重试环连坐面、出站本地回退会话
  僵尸）登记 spec §14 开放点。

## v0.18.0（2026-10-04）

主题：**edge 外部接入面（edge & hosting 线 E2）**——WebSocket 外部线协议 +
会话 pawnite（对称接缝）+ `host.WithEdge` 装配；另收 E1 终审延后的 host 补强
批。设计正本 `docs/superpowers/specs/2026-10-04-edge-hosting-design.md`
（§4 线协议/§5 会话/§7 鉴权/§8 生命周期/§9 攻击面；§5/§7 已随实现补修订注）。
内部节点间线协议与 Redis 键方案**零变更**。

### 部署约束（升级必读，不可静默丢失）

- **`WithEdge` 改变握手 schema 指纹**：启用后系统会话 kind `edge_session`
  并入 `remote.Config.Kinds` → kind 集合维指纹变化——**集群各节点必须一致
  启用 WithEdge**，混配节点（部分启用/部分不启用）互拒握手（v0.8.0 握手
  断点同款要求）。未配置 WithEdge 的节点指纹与行为零变化。
- 新增直接依赖 `github.com/coder/websocket` v1.8.15——仓库**首个网络协议
  第三方依赖**（选型论证与备选触发条件见
  `docs/decisions/2026-10-04-edge-websocket-lib.md`）。

### 新增

- **`edge` 包**：外部接入面三层。
  - **帧层**（`frame.go`）：11 个帧类型（数值即线上契约，`edge.FrameType`），
    编码复用 wire registry（field-tag 版本化 + 未知字段跳过）；错误信封
    `ErrFrame{Code, Message}` 外部面标准，业务错误码经信封无损过线；
    Subscribe/Unsubscribe/Event 类型号 E3 预留（收到回 40400 信封）。
    解码 fuzz `FuzzEdgeFrameDecode` 种子入库。
  - **会话 pawnite**（`session.go`）：每条外部连接一个会话实体
    （kind `edge_session`，inst = 会话 ID）。Go pawnite 每实体单 goroutine
    串行派发（序化事实，spec §5 修订注）——Handle 快路径只做「在飞信号量
    获取（满→Busy 40429，不排队）+ 按目标追加 FIFO 链 + 惰性起链 worker」：
    同目标帧序 = 追加序、慢目标只阻塞自身链、跨目标并行（spec §2 对称性
    修订的两个承诺等价达成）。在飞上限每会话口径缺省 64
    （`ListenerConfig.MaxInFlight` 注入）；出站响应经有界写通道 + done 守卫
    （会话销毁后迟到帧丢弃）。
  - **WS 监听器**（`listener.go`）：Upgrade 门口准入（会话总数上限缺省
    10000）→ pre-auth 门禁（仅 Hello，窗口 5s/帧限 1MiB）→ 鉴权
    （`Authenticator` 接口，实现留消费方）→ per-SessionKey 配额（缺省 64）
    → Welcome 直写 → **首帧惰性激活会话**（鉴权通过前不建 pawnite/目录
    条目——未认证洪水打不进 pawnite/目录面）→ ping/pong 判活（10s/30s）→
    `AuthorizeTarget` 粗粒白名单（`edge_session` 恒拒，不经回调）。断连
    销毁三重收敛（客户端关/判死/看门狗兜底），写通道满收敛双机制（abort
    带外关闭 + writer 持续消费丢弃）。Drain 序：停收 → 逐会话
    Bye(draining) → 宽限窗（缺省 2s）→ 会话 Destroy → writer 收队。
  - **`Edge` 组合体**（`edge.go`）：导出装配面（两段式构造解开 kinds 与
    remote 的环：`New → Kinds() → manager.New → remote.New → Bind → Start`）。
- **`host.WithEdge(cfg edge.ListenerConfig)`**：edge 全部配置唯一入口；
  `NodeID` 以 host 节点号覆盖；Start 次序 = 节点面之后起监听（对外可连时
  内部已就绪）；Shutdown 排空序最前插 edge 段。未配置时 host 行为零变化。
- **错误码映射表**（正本落 README「edge 外部接入面」节）：内部 StatusCode
  → 外部码——OK→0 / Error→50000 / NotFound→40404 / Serialization→40405 /
  Decode→40406 / Timeout→40408 / Unavailable→40409；edge 准入段
  40400（unsupported）/ 40403（forbidden）/ 40429（Busy）。
- **metrics**：`rans.edge.sessions`（已鉴权会话数）与 `rans.edge.preauth`
  （pre-auth 窗口连接数）Gauge，注册进 `metrics.Default()`。
- **Makefile `fuzz` 门禁扩为两条**：wire `FuzzFrameDecode` + edge
  `FuzzEdgeFrameDecode`（各 10s，同款种子入库纪律）。
- **E2 验收**（spec §12 E2 行，`edge/` 包内真 WS 客户端全链）：
  `TestE2ECrossNodeCallViaEdge`（redis 门禁——客户端接 node2 的 edge 面，
  Call 落 node1 实体，跨节点直达而非接入方重激活）、
  `TestE2EInFlightSemaphoreBusy` / `TestE2EInboxFloodAllAccounted`（信号量
  与收件箱洪水行为：每帧必在账）、`TestE2EMsgIDCorrelationOutOfOrder`
  （乱序回包按 msgID 关联）+ 连接序/pre-auth 门禁/错误信封映射契约测试
  （`edge` 包共 34 个测试 + 解码 fuzz）。
- **E2 边界声明**：Subscribe/事件回放/kind 感知放置属 E3；Claims→ctx 注入
  随 E3（本期 `Claims` 仅承载配额键 + opaque Data，翻译后 Call 的 ctx 尚
  未携带声明）；per-IP 连接配额延后（部署面 LB 职责，实现面收敛为
  per-SessionKey + 会话总数上限）。

### host 补强（E1 终审延后项，同批交付）

- `WithShutdownBudget(d)`：`Run` 的停机预算覆盖（缺省 15s）。
- poller→Invalidate 失效效力测试锚定（`TestHostPollerInvalidatesStaleRoute`
  ——「内置消灭 stale 路由坑」的宣称从锚定升级为证据）；inmem 形态
  `WithOnActivated`/`WithOnEvicted` 钩子直通（此前静默无效）；Shutdown
  幂等返回首闭包缓存错误 + hb/poller StopAndWait 错误 `errors.Join` 并入；
  `Start` 恰一次（二次调用报错，终态重建而非复用）。

## v0.17.0（2026-10-04）

主题：**host 组装层（edge & hosting 线 E1）**——C# `Rans.Hosting` 组装根的
Go 等价物（设计见 `docs/superpowers/specs/2026-10-04-edge-hosting-design.md`
§11）。`host.New(opts...)` 吸收 README QUICKSTART 手工装配序（目录/manager/
心跳+租约自毁/位置缓存+变更轮询/放置/地址解析/remote/启动停机次序），消除
消费方手抄与次序坑（`hb.SetNodeAddr` 先于 `hb.Run`、poller→Invalidate 漏接
即静默 stale 路由——全部内置消灭）。线协议、Redis 键方案与既有包公开 API
**零变更**（纯新增包 + 示例改造）。

### 新增

- **`host` 包**：RansHostBuilder 等价物。`New(opts...)` 装配
  （`WithNodeID`/`WithClusterName`/`WithKinds`/`WithSchemaShapes`/
  `WithHMAC`/`WithHMACFromEnv`/`WithNoHMAC`/`WithTLS`/`WithRedis`/
  `WithDirectory`/`WithCheckpointStore`/`WithHeartbeatInterval`/
  `WithPlacement`/`WithResolver`/`WithWatchdog`/`WithMetrics`/
  `WithOnActivated`/`WithOnEvicted` 等）；`Start/Shutdown/Kill/Run` 生命
  周期面（`Run` = Start → SIGTERM/SIGINT 等待 → 优雅停机；`Kill` 崩溃
  模拟，不排空不注销）。缺省 inmem 单节点形态；`WithRedis` 后 QUICKSTART
  ①~③ 全量接线（host 自建目录随 Shutdown 收口，放置/地址解析缺省内置）。
- **等价性锚定测试**：`host/redis_test.go` 的
  `TestHostEquivalenceWithManualQUICKSTART`——锚定同输入下两条装配路径
  （手工 QUICKSTART 与 host）的本地/跨节点 Call 往返与成员视图收敛行为
  一致。
- `examples/arbitration` 改 host 形态装配（QUICKSTART 手工清单在 README
  标注为「host 内部装配序正本」后保留）。

## v0.16.0（2026-10-03）

主题：**incarnation 纪元围栏（ISSUES X10，消费方核查单实锤立项）**——被
强弃 incarnation 的残留写入（rans checkpoint 与宿主直写存储同病）从
「靠取消面尽力收敛」升级为「Store 侧硬围栏拒绝」。含消费方核查批的
X9 形态锚定与新挂 X11（迁移 core 取消面缺口）。线协议零变更；Redis
新增一个目录族键（`rans:{dir}:inc`，无 TTL）。

### 破坏性 Go API 变更

- **`checkpoint.Store.Put` 签名**：`Put(ctx, id, snapshot)` →
  `Put(ctx, id, incarnation uint64, snapshot)`。实现方契约：维护 per-id
  已接受最大纪元，回退写入拒绝（新哨兵 `checkpoint.ErrStaleIncarnation`，
  errors.Is 可判）；相等=同 incarnation checkpoint 重试允许覆盖；0=未围栏
  直通（目录无纪元源的裸形态，保持旧行为）。MySQL 等实现建议 per-id 存
  最大纪元列并条件写（INSERT ... ON DUPLICATE KEY UPDATE ... WHERE 或先读
  后写带 WHERE incarnation < ?）。Delete 不撤销纪元高水位（删除不是重置）。

### 行为增强（非破坏）

- **纪元源**：新能力接口 `directory.IncarnationSource`（`NextIncarnation`），
  redis 目录（`rans:{dir}:inc` 共享 Hash HINCRBY，无 TTL——单调性跨条目
  死亡存续；单键命令 cluster 合法）与 inmem 目录均已实现；契约
  `contract.RunIncarnationSuite`（含 cluster 平移）。manager 激活时取号，
  失败按激活失败收场（目录不可用同语义，可重试）；仲裁输家消耗号段合法。
- **三个快照写点携带纪元**：周期 checkpoint / 闲置钝化 / 迁移 core 的
  CAS 前快照——旧 incarnation 的晚落 Put 被 Store 拒绝（证据测试
  TestCheckpointSweep_StalePutCannotOverwriteNewIncarnation：对旧签名
  跑出覆写实锤，v0.16.0 起终态为新快照且旧 Put 计一次 stale 拒绝）。
- **宿主 fencing ctx 面**：`pawnite.Options.Incarnation` +
  `pawnite.WithFencing/Fencing(ctx)`。Activate/Deactivate 钩子 ctx（经
  actCtx）与实体 Handle ctx（本地/远端投递 Deliver 时按**实际执行实例**
  包装）都携带本 incarnation 纪元——宿主直写自有存储时以令牌做条件写，
  zombie 迟回的 Handle 续体（慢 LLM 调用事后返回）写入可被拒。
- **装配期告警**：`SetCheckpointStore` 注入 Store 但目录无纪元源 → Warn
  （围栏静默失效可见）。

### X9 形态锚定（消费方核查批，行为无变化）

- 执行卡死强杀的 zombie **不启动** Deactivate（stopShutdown 让位先行，
  强于「晚到 Deactivate 收已取消 ctx」）：TestAbandonExecutionSkipsLate-
  Deactivate。
- 迁移卡死强弃后 core 迟回 Won 的续体**会**启动 Deactivate 且收**已取消**
  ctx（消费端 select 立即放行依赖的唯一真实落点）；同窗实证迁移 core
  ctx 不受强弃取消（新挂 ISSUES X11，低优）：TestAbandonedMigrationWon-
  LateDeactivateSeesCanceledCtx。
- 文档口径：双实例并发窗口上界=该 kind 生效停驻阈值（per-kind 覆盖放大，
  缺省 60s/120s/60s 仅是未覆盖形态）+ 宿主收尾宽限（规格 §14.2 注记更新）。

## v0.15.0（2026-10-03）

主题：**看门狗强弃取消面（ISSUES X9）**——zombie 生命周期钩子可取消，
「强弃后同 identity 双实例并发」窗口从无界收敛到宿主配合度界。
线协议与 Redis 键方案零变更；Go API 零变更（纯内部行为契约）。

### 行为契约（消费方需知）

- **强弃时取消在飞钩子 ctx**：三个看门狗强弃（卸载/激活/执行卡死）置
  终态的同时，取消在飞 Activate/Deactivate 钩子的 ctx（pawnite 自持
  hookCtx，自 Start ctx 派生：values 保留、parent 取消照旧传播）。动机：
  强弃后目录已注销、本地已逐出，新 incarnation 即刻可激活，而 zombie
  钩子仍在 loop goroutine 上运行——协作宿主消费 ctx 尽早返回；不消费
  ctx 的宿主行为不变（残留写入仍不设防，硬围栏见 ISSUES X10 挂账）。
- **优雅路径永不取消**：无强弃介入时钩子 ctx 全程未被取消（「销毁收尾
  不因调用方取消而中断」既有语义钉死：
  TestGracefulLifecycleNeverCancelsHookCtx）。
- **在飞留痕**：强弃时生命周期钩子仍在飞 → Warn 日志（`watchdog abandon
  with lifecycle hook in flight`，携带 uuid/hook）；执行卡死强杀（无钩子
  在飞）静默。
- **宿主约定**：Deactivate/Activate 应消费 ctx——不可快速返回的收尾工作
  分段放弃（消费方 wanju_agent_server 的 WorkspaceEntity.Deactivate 已
  立issue 跟进，见其 docs/issues/）。

### 修复/文档

- 修正「zombie 续体无害化」过度声明（pawnite/manager 注释 + 规格
  §14.2）：限定到 pawnite 内部状态；宿主侧效应明确「取消面收敛、硬围栏
  挂账」口径。
- ISSUES.md 立 X9（本批销账）/X10（checkpoint 版本围栏，挂账观察）。

## v0.14.0（2026-10-02）

主题：**M5/M6 裁定落地**——停机确定性收口原语 + 清理任务批量化。
线协议与 Redis 键方案零变更。

### 新增

- **`StopAndWait(ctx)`**（M5，`membership.HeartbeatService` 与
  `ChangePoller`）：Stop + ctx 预算内等待 Run goroutine 退出——返回 nil
  即在途最后一轮已结束，宿主可安全关闭目录等下游（消除「Stop 后在途
  心跳 IO 撞关闭连接」的悬空窗）；超时返回 ctx.Err；未启动立即 nil；
  幂等。**连带钉死 Run 恰一次语义**（重入 CAS 防御、done 通道只关一次，
  重启构造新实例）。testkit 停序（Cleanup/Kill/Shutdown）切换为
  StopAndWait（消费示范）。

### 修复/优化

- **`CleanupExpired` 页级 pipeline 批量化**（M6）：每 SSCAN 页（≤100
  成员）两批 pipeline（批量 Exists 探活 + 批量 SRem 摘除），往返从每成员
  2 次降到每页 2 次（万级残留从 ~2 万次往返的秒级降到几十次）；页内
  非原子语义与原逐成员一致（复活者下轮补扫）；cluster 形态 go-redis
  按 slot 自动分组分发。

### 文档

- 新增 `docs/consumer-upgrade-notice.md`：v0.7.0→v0.14.0 消费方升级
  通知合并稿（破坏性 API 逐条、升级清单、兼容性矩阵），待人工转达。

## v0.13.1（2026-10-02）

主题：**Redis Cluster 实测验证**——内网测试环境部署 3-master cluster，
契约套件平移全绿；实测锤出一个真缺陷并修复。

### 修复

- **SCAN 在 cluster 下单节点失明**（v0.11.0 引入 UniversalClient 面的
  缺陷）：SCAN 是节点本地命令，go-redis 通用 `cmdable` 实现把它路由到
  单个节点——真 cluster 实测 `BrowseAll` 返回 0 条（ChangePoller 变更
  轮询将全量失明）、`CleanupExpired` 同病。修复：`scanAllShards` 助手
  ——cluster 形态经 `ForEachMaster` 逐 master SCAN（并发回调加锁收集），
  单实例形态退化为原直接 SCAN（行为不变）。钉死：
  `TestClusterDirectoryE2E`（多实体散布分片 → BrowseAll 全量可见）。

### 新增（测试基建）

- cluster 实测套件（env 门禁 `RANS_TEST_REDIS_CLUSTER_ADDRS`，未设即
  skip，裸 `go test ./...` 仍全绿）：`TestClusterRealSlotUnity`（真
  CLUSTER KEYSLOT：{reg}/{dir} 域内同槽、两域分离）、契约三套件平移
  （基础/租约/registry——三 Lua 多键实跑、pipeline 跨 slot 分发）、
  `TestClusterDirectoryE2E`（散布注册 + BrowseAll 聚合 + Lua 2 换锁 +
  registry 全链路）。
- 至此 v0.11~v0.13 三批 cluster-ready 改动的兼容性论证全部由实测背书。

## v0.13.0（2026-10-02）

主题：**实体目录键族 `{dir}` hash tag**（ISSUES X8 选项 A 用户裁定）——
三 Lua 的多键操作在 Redis Cluster 下合法化，原子性无损。线协议与 Go API
零变更。至此 Redis 双原子域（`{reg}` registry / `{dir}` 目录）全部
cluster-ready。

### 变更

- 目录键族统一嵌 `{dir}`：主键 `rans:{dir}:dir:<kind>/<inst>`、心跳键
  `rans:{dir}:hb:...`、class·node 索引 `rans:{dir}:idx:...`——全部同
  slot。三 Lua（条件注销/迁移换锁/批量续租）的 KEYS 组合恢复 cluster
  合法；目录键量小操作轻，单 slot 不构成瓶颈，且不挤占业务键分布。
  BrowseAll/CleanupExpired 的 SCAN pattern 同步 tag 化。
- **修复**：总表非数值漂移 field 此前被跳过、未进惰性清理集（注释声明
  了清理但代码没做）——现与死 field 同集 HDEL（TestNodeIDsRedis 改造为
  总表漂移断言后锤出）。
- 兼容性：键名变更（`rans:dir:` → `rans:{dir}:dir:` 等），旧键靠租约
  TTL/索引 TTL 自然消失；新旧实现互不可见——**同集群双端同版本**。
  钉死：TestDirectoryClusterSlotUnity（三 Lua KEYS 全集的 tag 统一 +
  registry 域独立）。
- 遗留小项：真实 cluster 环境验证基建（docker redis-cluster 进测试
  门禁）——接 cluster 时补。

## v0.12.0（2026-10-02）

主题：**Redis Registry 键方案定型**（同日用户裁定）——总表 value 改续命
时间戳、地址单载判活键、`{reg}` hash tag 统一 slot。线协议与 Go API 零变更。

### 变更

- **总表 value = 续命时间戳**（unix ms，写方时钟）：判活
  初筛面预留——为未来判活三件套（表过期初筛 + 探测 + 投票）做数据铺垫；
  当前时间戳仅记录，判活权威仍是判活键的服务端 TTL（语义不变）。
- **地址单载于判活键**：总表不再冗余地址（枚举归总表、活+在哪归判活键，
  职责更清晰）；`NodeAddr`/`NodeAddrs` 读面不受影响（地址本就取自
  判活键）。
- **`{reg}` hash tag**：总表与全部节点键 `rans:{reg}:...` 共享同一 slot——
  未来 Lua 可原子地同时访问总表与节点键（三件套的探测宣告/票据写入面）。
  AGENTS.md 硬规则 5 同步收紧：新 registry 键必须沿用本 tag。
- 兼容性：键名变更（`rans:nodes` → `rans:{reg}:nodes` 等），旧键残留靠
  TTL 自然消失；新旧实现互不可见——**同集群双端同版本**（一贯要求）。
  钉死测试：TestRegistryClusterSlotUnity（tag 提取比对——内网 Redis 编译
  时禁用了 cluster 支持，CLUSTER KEYSLOT 不可用，改用 Redis 文档语义
  「同 tag 必同 slot」的本地等价断言）、TestRegistryRosterTimestampValue。

## v0.11.0（2026-10-02）

主题：**Redis Registry 总表（B3 裁定）+ 集群兼容第一步**。线协议与握手
指纹零变更。裁定：同日本会话用户裁定（SCAN 与 keyspace 的耦合是确定性
算法事实，需在万级实体预期下消除；未来必用 Redis Cluster）。

### 新增

- **总表 `rans:nodes`（Hash：field=节点号，value=地址）**：registry 读面
  从 SCAN 改为「HGETALL 枚举 + 逐判活键 GET 探活 + 死 field 惰性 HDEL」
  ——固定 2-3 次往返，与 keyspace 规模彻底解耦。动机（修正 v0.10.0 的
  不严谨表述）：SCAN 的返回数据量虽已 O(节点)，但服务端遍历成本仍是
  O(整个 keyspace)——实体键共享同一库，十万键时 NodeIDs ~1000 次往返
  （~200ms），百万键击穿 `RandomPlacement` 的 1s 查询超时 → 放置**静默
  退化**为全本地激活。
- **双写与撕裂语义**：`RegisterNode` pipeline 双写（判活键 SET PX +
  总表 HSET）；总表是超集，判活键是存活的唯一权威（服务端单时钟不变）
  ；读面探活收敛 + 惰性清理。`UnregisterNode` 双删（Drain 即时退视图
  语义保持）。
- **`NodeRegistry.NodeAddrs(ctx)`**：一次拿到「谁活 + 在哪」全量快照
  （接口扩展——自定义 NodeRegistry 实现方需补该方法；仓内仅 redis 实现
  与测试 fake）。
- **`redis.Options.Addrs`**：多地址种子列表（Redis Cluster/哨兵），底层
  换 `goredis.UniversalClient`；单地址行为与普通客户端逐字节一致
  （全量套件回归验证）。**registry 面单键命令族天然 cluster 兼容**。
- **AGENTS.md 术语节**：「Redis Registry（节点注册表）」固化为固定术语
  （总表+判活键的合称，接口 `directory.NodeRegistry`），与 Directory
  （实体目录）严格区分；新增硬规则：registry 键方案改动必须保持
  cluster 兼容约束（单键命令 + 容忍撕裂的 pipeline，禁多键 Lua/SCAN）。

### 已知边界（待裁）

- **X8**：实体目录面三 Lua 是多键操作，cluster 下 CROSSSLOT——接
  cluster 前必须裁定（选项与推荐见 ISSUES X8）。

## v0.10.0（2026-10-02）

主题：**节点级成员键（ISSUES X4/X5 裁定落地）**——成员面与逐实体租约
分离；地址发现内置于目录。线协议与握手
指纹**零变更**。裁定正本：`docs/decisions/2026-10-02-node-membership-key.md`。

### 破坏性变更（升级必读）

- **`directory/redis.Directory.NodeIDsByHeartbeat` 移除**：存活节点视图
  改由节点级成员键承载——新面 `NodeIDs`（`directory.NodeRegistry` 能力
  接口）。消费方把 `NodeIDsByHeartbeat` 引用改为 `NodeIDs` 即可。
- **混跑断点**：旧版本节点不写节点键 → 新版本的成员视图看不到它
  （放置不选它、地址解析失败）——**同集群双端同版本升级**（与 v0.8.0
  握手断点同要求；1.0 前无消费方上线，零迁移包袱）。

### 新增

- **节点级成员键 `rans:node:<nodeId>`**：键在=节点活、值=监听地址（一键
  双承载）。`directory.NodeRegistry` 能力接口（`RegisterNode`/`NodeAddr`/
  `NodeIDs`）+ redis 实现；契约由 `contract.RunNodeMembershipSuite` 锁定
  （键存在 ⟺ 节点可见、地址可读、TTL 续期、节点注销清键）。效果：空节点
  对放置可见（死锁解除）、冷启动窗口消除（启动即写）、存活视图扫描
  O(节点)（此前 O(全集群实体)）。
- **HeartbeatService 写成员键**：每周期无条件 `RegisterNode(self, addr)`
  （零实体照写、Run 启动即写一次）；地址经 `SetNodeAddr` 注入
  （`remote.Start` 后 `Addr()` 即得，注入后再 Run）。目录无 `NodeRegistry`
  能力（inmem）自动跳过该半边。
- **目录派生地址解析 `Directory.NodeAddrResolver()`**：返回
  `remote.Config.Resolve` 注入形态的函数（读成员键单键 GET）——装配不再
  需要 Start 后手工回填地址表（testkit redis 模式与 arbitration 示例已
  切换）。
- **README QUICKSTART**：「最小装配清单（从零到多进程可服务）」——
  完整接线骨架 + 钩子②（ChangePoller→Invalidate）漏接后果显式标红。

### 修复

- **X6**：终局 cleanup 的目录注销带 5s 超时 ctx（原 `Background` 无界——
  慢目录可冻结看门狗扫描环）；`fireOnEvicted` 钩子宿主责任（必须非阻塞）
  godoc 钉死。
- arbitration 演示 `preflight` 未带密码 env（内网带认证 Redis 直连模式
  下无法探活）。

### 行为注记

- 优雅停机（`Remote.Drain` 停序的目录注销）顺带 DEL 节点成员键——本节点
  即时退出成员视图，不等 TTL 过期；崩溃路径仍靠 TTL（~2min）自然淘汰。
- 逐实体心跳键语义不变（租约归属校验续期），仅不再兼职存活推导。
- X7：`lifecycle.RebalancerService` 宿主用法示范落在 arbitration 演示
  Phase C（`SetKindIdlePolicy` + `SetPlacement` + 单轮 `Scan` 迁移闲置
  实体跨节点）——预留面证明可用，仍不进 remote 自动装配。

## v0.9.0（2026-10-01）

主题：**双线架构审计批（1.0 前加固）**——「分函数 + 整体模块」两线独立
审计、逐项「先验证再判断」后落地的 10 项确证修复 + 死面清理 + 同构收敛。
线协议与握手指纹**零变更**（双端可跨版本混跑）；破坏性变更全部在 Go API
面，逐条如下。审计分诊与遗留疑问（X4-X7/T8/M5/M6）见 `docs/ISSUES.md`。

### 破坏性变更（升级必读）

- `remote.NewGateway` / `NewGatewayWithSweep` 签名去掉 `self` 首参
  （死字段，零读取）。
- `tcp.Manager.SetOnLost` 移除：节点丢失观测单轨化到
  `transport.Handler.OnConnectionLost`（第二钩子零生产消费方）。
- `remote.Dispatcher.IsDraining` 移除（零调用方；`SetDraining` 保留）。
- `manager.Call` 的**主动取消**不再伪装 `ErrTimeout`：deadline 仍返回
  `ErrTimeout`（既有契约不变），取消返回包装的 `context.Canceled`；同时
  已落地的应答优先于取消信号交付，实体回传的调用方 ctx 错误归一到调用侧
  语义。
- `tcp.NewManager(nil handler)` 装配期 panic（此前首条消息才爆）。

### 行为增强（非破坏）

- **钝化不再丢快照**：捕获槽与周期 checkpoint 撞车时让位一轮（此前两条
  快照路径双双落空、状态回退超出文档边界）；等待捕获受 ctx 预算。
- **PackageCache 容量上界**（默认 100k + 过期清扫 + FIFO）；
  **LocationCache order 簿记热 id churn 无界增长根修**（压缩按 id 去重）。
- **tcp.Manager Close 成为真终态**：关闭与在途拨号竞态收口（迟到登记的
  连接当场关闭）、Close 后重拨/重监听拒绝（`tcp.ErrManagerClosed`）。
- **握手体走页池**：每条连接不再白记一枚池 Put，`Gets==Puts` 漏还锚点
  恢复纯净（`TestManagerHandshakePoolAccounting` 钉死）。
- **Remote.Start 失败即终态**（内部 Stop 收口，不留 sweeper 泄漏）；
  **心跳挂 runCtx**（调用方取消 Start ctx 不再静默停摆 pong 踢除）。
- 网关拒绝哨兵：`remote.ErrPendingCapacity` / `ErrDuplicateMessageID`。
- **发送段受请求预算约束**：短超时配置下不再吃满 ~8s 重试链；
  `tcp.ErrManagerClosed` 短路重试。
- **reply/Forward 发送预算 5s**（`Dispatcher.replyBudget`）：失联对端
  不再队头阻塞连接读循环/实体串行 loop 数十秒。
- **SkipField 递归深度 ≤64**（`wire.MaxSkipFieldDepth`，与
  `MaxCausalDepth` 同族）：畸形深嵌套帧按 `ErrMalformed` 拒绝（正常生成
  物形状远达不到该深度）。

### 内部收敛（行为不变，测试钉死）

同构复合体去重（Destroy 双轨/drainForward 双壳/AbandonStuck×3/
lifecycle 四 Run 环/callOneWay 镜像/tcp 三处清表）、CheckpointSweep 锁
纪律统一、CheckpointSweeper.Scan 补子 ctx 上界、README 生产参数总表
与无 Store 迁移警示。

### 兼容性矩阵

| 变更面 | 旧→新 | 影响 |
|---|---|---|
| 线协议 / 握手指纹 | v0.8.x ↔ v0.9.0 | **零变更**（可混跑） |
| Go API | 上述破坏性条目 | 引用面自查（SetOnLost/IsDraining/NewGateway 签名） |
| manager.Call 错误面 | 取消形态 | `errors.Is(err, ErrTimeout)` 判超时不受影响；判「任何失败」的宽匹配需复核 |
| SkipField 深度 | >64 层畸形帧 | 此前可解、现在拒绝（合法形状不涉） |

## v0.8.0（2026-09-30）

主题：**关闭 schema 灰色通道 + 迁移窗口 flake 根修**。两项破坏性变更都在
握手兼容面上：**双端必须同版本升级**（旧↔新握手互拒），升级后须重跑
`go generate` 重新入库生成物。

### 破坏性变更（升级必读）

- **握手指纹纳入零字段消息**（ISSUES W1）：`RansShapes` 对零字段消息
  （无参方法派生请求、空结构体消息）也产出一条仅类型名的条目——增删/改名
  零字段消息从此翻指纹。旧版本不认识这些条目，**新旧版本互拒握手**；
  双端同升 v0.8.0 并重跑 `go generate ./...` 重新入库 `rans_gen.go`。
- **注册表收紧为硬错**（ISSUES X1）：`wire.EncodeValue` 遇未注册类型、
  `wire.DecodeValue` 遇未知名字不再静默落 JSON 兜底（旧形态解出无类型
  `map[string]any`，错位延迟到代理断言才暴露），改返回 `wire.ErrUnregistered`
  ；`wire.RegisterValue` 对同名异体/同类型双名注册 fail-fast panic（旧形态
  静默覆盖、后写赢）。依赖 JSON 兜底的代码必须在注册表显式注册类型；
  消费方生成物经 codegen 全量注册，正常不受影响。
- （v0.7.0 已记）`pawnite.DestroyingSince()` → `StateSince()` 改名。

### 新增

- 线协议控制面消息 `MsgRetire`（type=14）：连接退役通知。同节点连接收敛
  的败者在半关前发出；接收方据此（a）不把该连接的关闭误报为节点断连
  （不触发 `FailPendingForNode` 杀在途请求），（b）对本副本同样执行优雅
  退役——先排空冲刷已入队帧再收口（否则对端 FIN 早于本端排空完成时会
  静默丢弃已入队帧；-race 千轮级摇测实测定位并修复）。旧版本对端按
  unknown type 丢弃、行为不变，但建议双端同升。

### 修复

- **`TestAcceptance_MigrationWindowZeroLoss` 偶发失败根修**（ISSUES Flake
  观察哨销账）：两个独立根因——① tcp 双向首接触连接战（同节点顶替非确定
  性解冲突：坏序相互断连/已入队帧静默丢失），修复为确定性存活规则（低号
  节点发起的连接存活）+ 败者优雅退役（互斥封队→排空冲刷→半关）+ MsgRetire
  抑制帧 + 包装层 Unwrap 约定 + 网关首次重试免退避；② `registerSelf` 自指向
  残留分支的注销-重注册窗口可被源端 straggler 抢注（迁移线性化被静默撤销），
  修复为 `winner==self` 直接收养。裁定正本：
  `docs/decisions/2026-09-30-tcp-bidir-convergence.md`。
- codegen：`RansShapes` 零字段条目（上文 W1）；入库生成物与再生成漂移锁
  同步更新（examples/chat、testdata fixtures）。

### 兼容性矩阵

| 变更面 | 旧→新 | 影响 |
|---|---|---|
| 握手 schema 指纹 | v0.7.x ↔ v0.8.0 | **互拒**（双端同升） |
| 线协议 MsgRetire | 新→旧 | 旧端丢弃该帧、行为同旧（收敛Lost误报抑制仅同版本生效） |
| wire 注册表 API | 行为变更 | JSON 兜底移除、冲突 panic；签名不变 |
| pawnite API | v0.7.0 已破坏 | `StateSince()` 改名沿用 |

## v0.7.0（2026-09-30）

主题：粘性请求黑洞解除 + 纠正面默认部署。详见 tag 注记与
`docs/decisions/`（R1/X2/X3 批次 + 复审 Major-1）。

- `pawnite.DestroyingSince()` → `StateSince()`（统一状态进驻时钟）；
- 激活卡死第三形态 `AbandonStuckActivation`（粘性请求黑洞解除）；
- `remote.Start` 后看门狗缺省开跑不可关（激活/卸载 60s、执行 120s；慢实体
  按 kind 配 `WatchdogConfig.KindOverrides` 放宽）；
- 周期 checkpoint / 目录残骸清理随装配自动接线。
