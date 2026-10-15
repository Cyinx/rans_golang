package redis

import "testing"

// 本文件是「Lua 正文逐行照抄规格」的硬闸（规格 §11.1 移植要点）。
//
// 出处（provenance）：下方三个常量逐字节抄自
// docs/porting/ARCHITECTURE.zh-CN.md §11.1 的三个 ```lua 代码块原文
// （含注释行——注释也是规格文本的一部分；LF 行尾、无行尾空白）。
// 测试不读规格文件（规格可能移动/改版），以本文件常量为正本副本，
// 与 scripts.go 的实现常量逐字符比对，防止实现被手抖改写。
// 若规格语义需要变更，必须同时改这里与 scripts.go，并过契约套件。
const specLuaRegister = `-- luaRegister（注册/收养/接管原子域，单键四分支）：
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

const specLuaRenew = `-- luaRenew 比对续期（单键）：键缺席→{0}=Absent 不重建；键内 epoch>ARGV[3]→{-1}=Superseded 值不动；
-- 否则 SET 值 PX ttl→{1}=Renewed
local v = redis.call('GET', KEYS[1])
if not v then return {0} end
local ke = tonumber(string.match(v, '"epoch":(%d+)')) or 0
if ke > tonumber(ARGV[3]) then return {-1} end
redis.call('SET', KEYS[1], ARGV[1], 'PX', tonumber(ARGV[2]))
return {1}
`

const specLuaUnregisterNodeGuard = `-- luaUnregisterNode 守卫注销（单键）：键内 epoch==ARGV[1] 才删，缺席/不符不动
local v = redis.call('GET', KEYS[1])
if not v then return 0 end
local ke = tonumber(string.match(v, '"epoch":(%d+)')) or 0
if ke ~= tonumber(ARGV[1]) then return 0 end
redis.call('DEL', KEYS[1])
return 1
`

const specLuaUnregister = `-- 条件注销为墓碑:三元全等匹配才改写
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

const specLuaTransfer = `-- luaTransfer（实例级 CAS + 升纪元，单键）：
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

// TestLuaSourcesMatchSpec 断言实现常量与规格正本副本逐字符一致（三份全锁）。
func TestLuaSourcesMatchSpec(t *testing.T) {
	if luaRegister != specLuaRegister {
		t.Errorf("luaRegister 与规格 §11.1 文本不一致:\n got: %q\nwant: %q",
			luaRegister, specLuaRegister)
	}
	if luaUnregister != specLuaUnregister {
		t.Errorf("luaUnregister 与规格 §11.1 文本不一致:\n got: %q\nwant: %q",
			luaUnregister, specLuaUnregister)
	}
	if luaRenew != specLuaRenew {
		t.Errorf("luaRenew 与规格 §11.1 文本不一致:\n got: %q\nwant: %q",
			luaRenew, specLuaRenew)
	}
	if luaUnregisterNodeGuard != specLuaUnregisterNodeGuard {
		t.Errorf("luaUnregisterNodeGuard 与规格 §11.1 文本不一致:\n got: %q\nwant: %q",
			luaUnregisterNodeGuard, specLuaUnregisterNodeGuard)
	}
	if luaTransfer != specLuaTransfer {
		t.Errorf("luaTransfer 与规格 §11.1 文本不一致:\n got: %q\nwant: %q",
			luaTransfer, specLuaTransfer)
	}
}
