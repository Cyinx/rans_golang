package rans_test

// 票 20（B13 Go 侧）：协议金向量入库——按 rans_rust `.vec` 同格式（名称
// <TAB> 十六进制 <TAB> 说明）产出当前 Go 实现的线形态向量（握手 v5、扩
// 展块字段、MessageType、Error 帧结构化体、因果路径、外封、头部、注册/
// 注销/换锁/守卫注销/成员续期 Lua 文本、注册表键名、schema 指纹），入
// 库 testdata/vectors/*.vec；默认比对（漂移即红），-update 重写。向量面
// 以「当前 Go 实现」为准，不追溯 v0.20。

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
	"github.com/cyinx/rans_golang/directory/redis"
	"github.com/cyinx/rans_golang/internal/ctxkey"
	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/wire"
)

var update = flag.Bool("update", false, "重写入库向量文件")

// vecLine 一条向量：名称 TAB 十六进制 TAB 说明。
type vecLine struct {
	name string
	hex  string
	desc string
}

func buildVectors() map[string][]vecLine {
	v := map[string][]vecLine{}

	// ---- handshake：握手帧（Type=12，min/max/HMAC 占位段按编码器形态） ----
	// 握手版本段是传输层装配（handshake.go），以常量值锚定 16 位区间：
	v["handshake"] = []vecLine{
		{"hs_vermin5", fmt.Sprintf("%04x", wire.HandshakeVersionMin), "握手版本下限 v5"},
		{"hs_vermax5", fmt.Sprintf("%04x", wire.HandshakeVersionMax), "握手版本上限 v5"},
	}

	// ---- headers：头部条件字段族 ----
	v["headers"] = []vecLine{
		{"hdr_bare", hexOf((&wire.Header{Type: protocol.MsgRequest, MessageID: 7}).Encode()), "最小头部"},
		{"hdr_status", hexOf((&wire.Header{Type: protocol.MsgError, MessageID: 8, Flags: protocol.FlagHasStatus, Status: 4}).Encode()), "Status 条件字段"},
		{"hdr_targetuuid", hexOf((&wire.Header{Type: protocol.MsgRequest, MessageID: 9, Flags: protocol.FlagHasTargetUUID, TargetKind: "cell", TargetInst: "fw"}).Encode()), "TargetUUID"},
		{"hdr_srcnode", hexOf((&wire.Header{Type: protocol.MsgOneWay, MessageID: 10, Flags: protocol.FlagHasSourceNodeID, SourceNodeID: 3}).Encode()), "SourceNodeID"},
		{"hdr_msghash", hexOf((&wire.Header{Type: protocol.MsgRequest, MessageID: 11, Flags: protocol.FlagHasMessagerHash, MessagerHash: 0xdeadbeef}).Encode()), "MessagerHash"},
		{"hdr_methodhash", hexOf((&wire.Header{Type: protocol.MsgRequest, MessageID: 12, Flags: protocol.FlagHasMethodHash, MethodHash: 0xcafebabe}).Encode()), "MethodHash"},
	}

	// ---- frames：MessageType 全枚举（值即契约） ----
	mtNames := map[protocol.MessageType]string{
		protocol.MsgRequest: "request", protocol.MsgResponse: "response",
		protocol.MsgOneWay: "oneway", protocol.MsgError: "error",
		protocol.MsgMigrationPackage: "migration_package", protocol.MsgPing: "ping",
		protocol.MsgPong: "pong", protocol.MsgHandshake: "handshake",
		protocol.MsgHandshakeAck: "handshake_ack", protocol.MsgRetire: "retire",
		protocol.MsgCancel: "cancel", protocol.MsgOwnershipLost: "ownership_lost",
		protocol.MsgRedirectHint: "redirect_hint",
	}
	var fl []vecLine
	for mt, name := range mtNames {
		fl = append(fl, vecLine{fmt.Sprintf("mt_%s", name), fmt.Sprintf("%02x", byte(mt)), "MessageType 枚举"})
	}
	sort.Slice(fl, func(i, j int) bool { return fl[i].name < fl[j].name })
	v["frames"] = fl

	// ---- primitives：codec 基本类型编码面 ----
	v["primitives"] = []vecLine{
		{"prim_varint_neg", hexOf(encodePrim(func(w *codec.Writer) { w.WriteVarInt(-7) })), "varint 负值（zigzag）"},
		{"prim_varuint", hexOf(encodePrim(func(w *codec.Writer) { w.WriteVarUInt(300) })), "varuint"},
		{"prim_fixed8", hexOf(encodePrim(func(w *codec.Writer) { w.WriteFixed8(0x2a) })), "fixed8"},
		{"prim_fixed32", hexOf(encodePrim(func(w *codec.Writer) { w.WriteFixed32(0xdeadbeef) })), "fixed32 小端"},
		{"prim_fixed64", hexOf(encodePrim(func(w *codec.Writer) { w.WriteFixed64(0xcafebabedeadbeef) })), "fixed64 小端"},
	}

	// ---- envelope：外封对象体 ----
	v["envelope"] = []vecLine{
		{"env_str", hexOf(wire.MarshalEnvelope("string", []byte("demoMsg"))), "字符串外封"},
		{"env_short", hexOf(wire.MarshalEnvelope("k", []byte("p"))), "短名短载荷"},
	}

	// ---- objects：Error 帧结构化体 ----
	e := rans.NewError(rans.CodeUnavailable, rans.ReasonBusy, "vector probe")
	e.Node = 7
	v["objects"] = []vecLine{
		{"err_structured", hexOf(wire.MarshalError(e)), "结构化 Error（code/reason/node/message）"},
	}

	// ---- causal：因果路径 ----
	v["causal"] = []vecLine{
		{"causal_root_only", hexOf(encodeCausal(&ctxkey.CausalPath{Root: "trace-1"})), "Root 有值 Path 空"},
		{"causal_multihop", hexOf(encodeCausal(&ctxkey.CausalPath{Root: "trace-1", Path: []uint64{1, 2, 3}})), "多跳 path[1,2,3]"},
	}

	// ---- luascripts：注册/注销/换锁/守卫注销 Lua 文本 ----
	v["luascripts"] = []vecLine{
		{"lua.register", hexOf([]byte(redis.ScriptRegister())), "luaRegister 原文"},
		{"lua.unregister", hexOf([]byte(redis.ScriptUnregister())), "luaUnregister 原文"},
		{"lua.transfer", hexOf([]byte(redis.ScriptTransfer())), "luaTransfer 原文"},
		{"lua.unregister_node_guard", hexOf([]byte(redis.ScriptUnregisterNodeGuard())), "守卫注销原文"},
	}

	// ---- registry：注册表键名 ----
	v["registry"] = []vecLine{
		{"reg_roster_key", hexOf([]byte("rans:nodes")), "成员总表键"},
		{"reg_epoch_key", hexOf([]byte("rans:epoch")), "代际计数器键"},
		{"reg_entity_key", hexOf([]byte("rans:e:{cell/fw}")), "实体主键 hash tag 形态"},
		{"reg_node_key", hexOf([]byte("rans:node:3")), "节点判活键"},
	}

	// ---- fingerprint：schema 指纹 ----
	v["fingerprint"] = []vecLine{
		{"schema_digest", fmt.Sprintf("%016x", codec.SchemaDigest()), "全量形状 FNV-1a 64"},
	}
	return v
}

func hexOf(b []byte) string {
	const digits = "0123456789abcdef"
	var sb strings.Builder
	for _, c := range b {
		sb.WriteByte(digits[c>>4])
		sb.WriteByte(digits[c&0xf])
	}
	return sb.String()
}

func encodePrim(f func(*codec.Writer)) []byte {
	w := codec.NewWriter()
	f(w)
	return w.Bytes()
}

func encodeCausal(cp *ctxkey.CausalPath) []byte {
	ext := wire.RequestExt{Causal: cp, TraceID: cp.Root, HasTraceID: cp.Root != ""}
	return wire.EncodeRequestExt(ext)
}

func TestGoldenVectors(t *testing.T) {
	vecs := buildVectors()
	dir := filepath.Join("testdata", "vectors")
	names := make([]string, 0, len(vecs))
	for name := range vecs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		lines := vecs[name]
		path := filepath.Join(dir, name+".vec")
		var sb strings.Builder
		for _, l := range lines {
			fmt.Fprintf(&sb, "%s\t%s\t%s\n", l.name, l.hex, l.desc)
		}
		if *update {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("vector file %s missing (run go test -update once): %v", path, err)
		}
		if got := sb.String(); got != string(want) {
			t.Fatalf("vectors drifted for %s:\n got:\n%s\nwant:\n%s", name, got, want)
		}
	}
}
