package redis

import goredis "github.com/redis/go-redis/v9"

// 三个原子单键 Lua（规格 §11.1）。脚本正文逐行照抄规格代码块原文，包括
// 注释行；scripts_test.go 与规格正本副本逐字符比对锁定（防手抖改写）。
// 语义已由 directory/contract 契约套件锁定，改任何一个字符都必须是
// 有意识的规格决策并同步规格与测试常量。
//
// 单键纪律（Redis Cluster 兼容约束，AGENTS.md 硬规则 5）：每个脚本只操作
// 主键 `rans:e:{<kind>/<inst>}` 一个键——逐实体 hash tag 内读-改-写原子
// 完成，任意 slot 合法（目录真分片）。纪元在值内自持（读-改-写）。

// luaRegister 注册/收养/接管原子域（单键四分支）：
//
//	KEYS=[主键]
//	ARGV=[selfNode, selfEpoch, expNode(0=无期望), expEpoch, expInc]
//	返回 {1, 新纪元, 0, 0}=赢（分支 1/2/3，见下），{0, node, epoch, inc}=输
//	（分支 4：他实例持有且不可接管，返回现归属）。
//	分支 1：无值或墓碑（node=0）→ 纪元 = 墓碑残值+1（无值即 1）写自归属；
//	分支 2：现值归属即本实例（node+epoch 全等，同代际重注册）→ inc+1
//	升写调用方纪元——落库值恒等于返回值（端口契约：
//	赢时 current 是含新纪元的自归属，调用方必须采信）；
//	分支 3：expected 非空且与现值全等 → CAS 接管 inc+1 写自归属；
//	分支 4：否则输，条目原样，返回现归属（转发/提示用）。
//	ARGV[6] floor 是纪元下限（0=无）：三个赢分支统一 inc = max(ci,floor)+1；
//	CAS 比对与输家返回仍用 ci（floor 不影响裁决，只抬高赢侧取号）。
//	主键值随写 v3：{"node":N,"epoch":E,"inc":K,"v":3}（双端同版本）。
const luaRegister = `-- luaRegister（注册/收养/接管原子域，单键四分支）：
-- KEYS=[主键]  ARGV=[selfNode, selfEpoch, expNode(0=无期望), expEpoch, expInc, floor(纪元下限,0=无)]
local v = redis.call('GET', KEYS[1])
local cn, ce, ci = 0, 0, 0
if v then
  cn = tonumber(string.match(v, '"node":(%d+)')) or 0
  ce = tonumber(string.match(v, '"epoch":(%d+)')) or 0
  ci = tonumber(string.match(v, '"inc":(%d+)')) or 0
end
local base = ci
local fl = tonumber(ARGV[6]) or 0
if fl > base then base = fl end
local sn = tonumber(ARGV[1])
if cn ~= 0 and cn == sn and ce == tonumber(ARGV[2]) then
  local inc = base + 1
  redis.call('SET', KEYS[1], string.format('{"node":%d,"epoch":%d,"inc":%d,"v":3}', sn, tonumber(ARGV[2]), inc))
  return {1, inc, 0, 0}
end
local en = tonumber(ARGV[3])
if v and cn ~= 0 and en ~= 0 and cn == en and ce == tonumber(ARGV[4]) and ci == tonumber(ARGV[5]) then
  local inc = base + 1
  redis.call('SET', KEYS[1], string.format('{"node":%d,"epoch":%d,"inc":%d,"v":3}', sn, tonumber(ARGV[2]), inc))
  return {1, inc, 0, 0}
end
if not v or cn == 0 then
  local inc = base + 1
  redis.call('SET', KEYS[1], string.format('{"node":%d,"epoch":%d,"inc":%d,"v":3}', sn, tonumber(ARGV[2]), inc))
  return {1, inc, 0, 0}
end
return {0, cn, ce, ci}
`

// luaUnregister 条件注销为墓碑（单键）：
//
//	KEYS=[主键]  ARGV=[node, epoch, inc, ttlMS（墓碑 PX 毫秒，票 17；
//	0 = 永久——三元全等比对，实例级注销无任何通配形态，epoch/inc 缺省
//	的 Owner 不命中）]
//	返回 0=条目不存在或归属不符或已是墓碑（未动），1=已改写无主墓碑
//	（{"node":0,"epoch":0,"inc":K,"v":3}——inc 保留，纪元单调跨墓碑存
//	续；ttlMS>0 时墓碑带 PX 过期——开启方必须存在纪元下限来源，node.New
//	校验）。
const luaUnregister = `-- 条件注销为墓碑:三元全等匹配才改写
local v = redis.call('GET', KEYS[1])
if not v then return 0 end
local cn = tonumber(string.match(v, '"node":(%d+)')) or 0
if cn == 0 or cn ~= tonumber(ARGV[1]) then return 0 end
local ce = tonumber(string.match(v, '"epoch":(%d+)')) or 0
if ce ~= tonumber(ARGV[2]) then return 0 end
local ci = tonumber(string.match(v, '"inc":(%d+)')) or 0
if ci ~= tonumber(ARGV[3]) then return 0 end
local tomb = string.format('{"node":0,"epoch":0,"inc":%d,"v":3}', ci)
local px = tonumber(ARGV[4]) or 0
if px > 0 then
  redis.call('SET', KEYS[1], tomb, 'PX', px)
else
  redis.call('SET', KEYS[1], tomb)
end
return 1
`

// luaTransfer 换锁（实例级 CAS + 升纪元，单键）：
//
//	KEYS=[主键]
//	ARGV=[expNode, expEpoch, expInc, newNode, newEpoch]
//	返回 {0,0,0,0}=条目不存在，{-1, node, epoch, inc}=归属不符（含墓碑，
//	返回现归属），{1, 新纪元, 0, 0}=已换锁（写目标 node+epoch，inc=现值+1）。
const luaRenew = `-- luaRenew 比对续期（单键）：键缺席→{0}=Absent 不重建；键内 epoch>ARGV[3]→{-1}=Superseded 值不动；
-- 否则 SET 值 PX ttl→{1}=Renewed
local v = redis.call('GET', KEYS[1])
if not v then return {0} end
local ke = tonumber(string.match(v, '"epoch":(%d+)')) or 0
if ke > tonumber(ARGV[3]) then return {-1} end
redis.call('SET', KEYS[1], ARGV[1], 'PX', tonumber(ARGV[2]))
return {1}
`

// luaUnregisterNodeGuard 守卫注销（单键）：仅当键内代际等于入参才删
// （比较删除——迟到的旧代际停机不误删已让位者的键）。无条件档在 Go 侧
// 直接 DEL，不经本脚本。
const luaUnregisterNodeGuard = `-- luaUnregisterNode 守卫注销（单键）：键内 epoch==ARGV[1] 才删，缺席/不符不动
local v = redis.call('GET', KEYS[1])
if not v then return 0 end
local ke = tonumber(string.match(v, '"epoch":(%d+)')) or 0
if ke ~= tonumber(ARGV[1]) then return 0 end
redis.call('DEL', KEYS[1])
return 1
`

const luaTransfer = `-- luaTransfer（实例级 CAS + 升纪元，单键）：
-- KEYS=[主键]  ARGV=[expNode, expEpoch, expInc, newNode, newEpoch]
local v = redis.call('GET', KEYS[1])
if not v then return {0, 0, 0, 0} end
local cn = tonumber(string.match(v, '"node":(%d+)')) or 0
local ce = tonumber(string.match(v, '"epoch":(%d+)')) or 0
local ci = tonumber(string.match(v, '"inc":(%d+)')) or 0
if cn == 0 or cn ~= tonumber(ARGV[1]) or ce ~= tonumber(ARGV[2]) or ci ~= tonumber(ARGV[3]) then
  return {-1, cn, ce, ci}
end
local inc = ci + 1
redis.call('SET', KEYS[1], string.format('{"node":%d,"epoch":%d,"inc":%d,"v":3}', tonumber(ARGV[4]), tonumber(ARGV[5]), inc))
return {1, inc, 0, 0}
`

// ScriptRegister/ScriptUnregister/ScriptTransfer/ScriptUnregisterNodeGuard
// Lua 原文的只读导出面（票 20 金向量与消费方审计用；文本与脚本对象同源）。
func ScriptRegister() string            { return luaRegister }
func ScriptUnregister() string          { return luaUnregister }
func ScriptTransfer() string            { return luaTransfer }
func ScriptUnregisterNodeGuard() string { return luaUnregisterNodeGuard }

// 预编译脚本对象（EVALSHA 命中时免传源码；并发安全）。
var (
	scriptRegister   = goredis.NewScript(luaRegister)
	scriptUnregister = goredis.NewScript(luaUnregister)
	scriptTransfer   = goredis.NewScript(luaTransfer)
	scriptRenew      = goredis.NewScript(luaRenew)
	scriptUnregNode  = goredis.NewScript(luaUnregisterNodeGuard)
)
