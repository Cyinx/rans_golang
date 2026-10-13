// Package redis 提供目录的 Redis 实现：三个原子单键 Lua（规格 §11.1）。
// 本文件定义键方案，是规格 KEYS 语义在 Go 侧的正本映射。
package redis

import (
	"fmt"

	"github.com/cyinx/rans_golang"
)

// 键方案（<p> = Options.KeyPrefix；键值 v3——H13 决策 19）：
//
//	实体主键  <p>rans:e:{<kind>/<inst>}    string；逐实体 hash tag——{}
//	                                      内是完整实体身份（经 rans.NewID
//	                                      禁 {/}）：目录真分片（多实体键散布
//	                                      多 slot，同实体单键操作同 slot）。
//	                                      值 v3 {"node":N,"epoch":E,"inc":K,
//	                                      "v":3}（同版本断点）；无 TTL、主键
//	                                      永不清删——注销写无主墓碑（inc 保留，
//	                                      单调跨墓碑存续），崩溃残留无害躺置
//	                                      （有效性由调用方按成员代际判定）
//	class 索引 <p>rans:{dir}:idx:class:<kind>  SET；成员=键成分 "<kind>/<inst>"
//	node 索引  <p>rans:{dir}:idx:node:<nodeID> SET；成员=键成分 "<kind>/<inst>"
//
// 索引是暂留面（票 17 随续租拆除一并退役）：注册/换锁/注销的主键变更在
// 单键 Lua 内原子完成后，Go 侧以单键命令（SADD/SREM/PEXPIRE pipeline）
// 容忍撕裂地维护索引——cluster 合法（跨 slot 的只有 pipeline 分发，非
// 原子域），撕裂残骸由 CleanupExpired 按值判活收敛。索引键集中 {dir}
// tag 是历史形态（多键 Lua 已不存在，tag 不再承担 cluster 合法性），仅
// 作为暂存键族的命名空间保留。
//
// 注册总表  <p>rans:{reg}:nodes          Hash；field=节点号，value=续命时间戳
// 判活键    <p>rans:{reg}:node:<nodeID>  string；SET PX=心跳 TTL（存活的
//
//	                                      唯一权威；值=JSON{addr,kinds,
//	                                      draining,epoch}）
//	代际计数器 <p>rans:{reg}:epoch         Hash；field 固定 "epoch"，value=
//	                                      单调节点代际（HINCRBY 取号，无 TTL）
//
// 键族裁定的决策出处：docs/decisions/2026-10-02-node-membership-key.md。
// 键值 v3 与逐实体 tag 的裁定出处：《规格》§11.1 与 HR4 票 15（H13 决策
// 19/21/23）；{dir} 集中 tag 的旧形态（四 Lua 多键原子）随本票消亡。
func (d *Directory) primaryKey(id rans.ID) string {
	return d.prefix + "rans:e:{" + id.String() + "}"
}

// primaryKeyByComponent 由索引成员（键成分 "<kind>/<inst>"）重建主键。
func (d *Directory) primaryKeyByComponent(component string) string {
	return d.prefix + "rans:e:{" + component + "}"
}

// registryTag 是 registry 键族的 hash tag：总表与全部节点键共享同一 slot
// （Redis Cluster 以 {} 内文本定槽）——未来 Lua 可原子地跨两键操作
// （判活三件套的宣告/票据面）。新 registry 键必须沿用本 tag（AGENTS.md
// 硬规则 5）。
const registryTag = "{reg}"

// directoryIndexTag 是暂留索引键族的 hash tag（见文件头键方案注记：索引
// 暂居 {dir} 单 slot，票 17 随索引退役一并消亡；实体主键已改逐实体 tag，
// 不在本族）。
const directoryIndexTag = "{dir}"

// nodeKey 是 node 的判活键（键在=节点活，值=JSON{addr,kinds}——存活的唯一
// 权威，地址与形态声明的唯一承载）。
func (d *Directory) nodeKey(node rans.NodeID) string {
	return fmt.Sprintf("%srans:%s:node:%d", d.prefix, registryTag, node)
}

// rosterKey 是 Redis Registry 的总表键（Hash：field=节点号，value=续命
// 时间戳——枚举面 + 三件套初筛预留；不存地址）。
func (d *Directory) rosterKey() string {
	return d.prefix + "rans:" + registryTag + ":nodes"
}

// epochCounterField 是代际计数器 Hash 的固定 field 名：{reg} 键族内只有
// 一个全局计数器（节点代际序列），单 field 承载。
const epochCounterField = "epoch"

// epochKey 是节点代际计数器键（{reg} 键族成员，与总表/判活键同 slot）：
// Hash 单 field，HINCRBY 取号。**无 TTL**——代际序列必须跨节点重启与优
// 雅停机存续（判活键死亡/注销后的再注册仍取得大于全部历史值的代际，
// 「同节点号重启 → epoch 严格递增 → 旧条目自然失效」的取号源）。
func (d *Directory) epochKey() string {
	return d.prefix + "rans:" + registryTag + ":epoch"
}
