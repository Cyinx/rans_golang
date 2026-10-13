# AGENTS.md — rans_golang 开发指南

> 先读上层公共指南 `../AGENTS.md`（工作区根，多仓库边界与通用工程纪律）。

## 仓库定位

Rans 虚拟 actor 运行时的 Go 实现，独立项目专人开发。语义唯一出处：
`docs/porting/ARCHITECTURE.zh-CN.md`（移植规格）；Go 侧取舍记录在 `docs/decisions/`。

## 内部过程文档（2026-10-13 开源剥离）

以下文档**已移出 git 跟踪、仅本地保留**（.gitignore 登记，换机器需手动
同步；不影响任何门禁测试——白名单与基线均内嵌在测试内）：
`.scratch/`（工单）、`docs/superpowers/`（spec/plan 归档）、
`docs/porting/`（移植规格）、`docs/decisions/`（裁定记录）、
`docs/ROADMAP.md`、`docs/agents/`、`docs/ISSUES.md`。
下文对这些路径的引用均指本地件。

## 路线图

**Phase 2-6 待补清单与自动推进运行协议：`docs/ROADMAP.md`（goal 模式交接正本，
接手前必读）**。每个 Phase 的实施 spec/工单落 `.scratch/<phase-slug>/`
（mattpocock 工单体系，见 `docs/agents/issue-tracker.md`）；历史计划归档在
`docs/superpowers/plans/`。

## 硬规则

1. 依赖只允许向下。公开面按角色分包（`rans` 契约 / `node` 组装 / `directory`、
   `checkpoint` 端口与适配器 / `codec` 编码 / `testkit`、
   `cmd/ransgen` 工具），实现细节全部在 `internal/`；模块内导入边由
   仓库根 `depgraph_test.go` 按《约定 spec》§3.2 表逐条锁定（可依赖清单
   之外的模块内导入一律失败，新包必须先登记再接线）。要点行：
   `rans` 只依赖两个零依赖叶包（`internal/ctxkey`、`internal/log`）；
   `internal/rpc` 可依赖表中其上全部内部包（`internal/testhooks` 除外）；
   `node` 是唯一组装根；examples 与 testkit 只经
   公开包使用运行时（testkit 另可经 `internal/testhooks`）；完整表以
   spec §3.2 与 `depgraph_test.go` 为准（两者由门禁测试咬合，改表必须
   同步改测试）。禁止业务概念进入本仓库（agent/wanju/MySQL 等实现归消费方）；
2. 对外 API 变更走 semver tag + changelog，破坏性变更提前通知消费方；
   线协议承诺（HR3 起）：同一握手 VerMax 内向后兼容（新字段只追加字段号、
   旧端跳过），破坏性变更必须提升 VerMin 并在 CHANGELOG 标注「滚动不兼容」；
   v0.22.0（握手 v3 + 头部扩展块 + Error 帧结构化）本身是同版本断点——
   同集群双端同版本，升级先停全部再升；schema 一致性默认为观测
   （`rans.handshake.schema_mismatch` 计数告警），`StrictSchema` 严格拒连
   由部署方按需开启；
3. 目录 Lua（v0.23.0 起三个原子单键 Lua：注册/收养/接管、条件注销墓碑、换锁）/激活仲裁/排空/迁移的行为变更必须过 directory/contract 与 testkit 故障套件；
4. 任何改动后跑 `make test`；实现工作在 git worktree 中进行（工作区根 §4.6）；
5. Redis Registry 的键方案改动必须保持 **Redis Cluster 兼容约束**：仅单键
   命令 + 容忍撕裂的双写 pipeline，不引入跨 slot 的多键 Lua / 跨键事务 /
   SCAN（同 slot 内的未来多键 Lua 是允许且预留的）；全部 registry 键必须
   沿用 `{reg}` hash tag（同 slot——未来 Lua 原子跨总表与节点键的前提）。
   实体目录主键为**逐实体 hash tag**（`rans:e:{<kind>/<key>}`——目录真分片，同实体单键操作同 slot；旧 `{dir}` 集中 tag 键族随 v0.23.0 断点消亡）；
6. 注释/体量门禁在仓库根 `commentgate_test.go`（`make test` 自动执行）：
   改注释、新增文件或写大段注释前先读《约定 spec》§6——ISSUES/日期字面
   =0、单行 ≤100 显示列、超 500 行白名单、注释占比基线只降不升。

## 术语

术语正本在仓库根 `CONTEXT.md`（Language 词条表，含裁定标注）；跨仓库
（rans_golang ↔ rans_rust ↔ 消费方）名词映射见工作区 `skills/terminology/`，
冲突时以其为仲裁。

---

## Agent skills

### Issue tracker

本地 markdown 模式：工单与 spec 落 `.scratch/<feature-slug>/`（本地件——
2026-10-13 开源剥离后不再入 git，跨机器延续靠本地同步）。见
`docs/agents/issue-tracker.md`（本地件）。

### Triage labels

默认五角色（needs-triage / needs-info / ready-for-agent / ready-for-human / wontfix），以工单文件内 `Status:` 行记录。见 `docs/agents/triage-labels.md`。

### Domain docs

单 context：`CONTEXT.md` 在仓库根（入 git）+ 决策记录在 `docs/decisions/`
（本地件，日期命名，现有惯例）。见 `docs/agents/domain.md`（本地件）。
