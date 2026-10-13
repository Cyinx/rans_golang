// Package rans 是运行时契约层：身份（ID/NodeID）、实体契约（Entity/Factory/
// Snapshotter）与调用面、错误模型的公共类型。实现细节在 internal/。
package rans

import (
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"
)

// kindPattern：kind 是目录键分隔符成分，字符集受限（移植规格 §3.1）。
var kindPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,63}$`)

// ID 是实体身份：Kind（实体类型，持久化契约）+ Key（实例 key 规范串）。
// 值相等：结构体仅含 string，可直接 == 与作 map 键（移植规格 §3.2）。
type ID struct {
	Kind string
	Key  string
}

// NewID 构造并校验 ID：Kind 字符集受限（移植规格 §3.1），Key 非空；
// Kind 与 Key 各自禁 `{`/`}`——目录主键 `rans:e:{<kind>/<key>}` 的
// hash tag 承载完整实体身份（H13 键值 v3），花括号入身份会撕裂 tag 边界
// （Redis 以第一对 `{}` 定槽），故在身份构造点整体禁入。
func NewID(kind, key string) (ID, error) {
	if !kindPattern.MatchString(kind) {
		return ID{}, fmt.Errorf("rans: NewID: invalid kind %q", kind)
	}
	if key == "" {
		return ID{}, errors.New("rans: NewID: empty key")
	}
	if strings.ContainsAny(kind, "{}") {
		return ID{}, fmt.Errorf("rans: NewID: kind %q must not contain braces", kind)
	}
	if strings.ContainsAny(key, "{}") {
		return ID{}, fmt.Errorf("rans: NewID: key %q must not contain braces", key)
	}
	return ID{Kind: kind, Key: key}, nil
}

// String 返回目录/存储键成分 "kind/key"（fmt.Stringer）；格式是键方案契约，
// 与 Redis 目录键逐字节一致，不得更改。
func (id ID) String() string { return id.Kind + "/" + id.Key }

// IsZero 报告 ID 是否为零值。
func (id ID) IsZero() bool { return id == ID{} }

// Hash 返回实体身份的 FNV-1a 64 哈希（标准库 hash/fnv，输入为 String() 的
// 字节）：因果路径 v2 的环判定载体（H12 决策 14）——线上路径只携带哈希、
// 不携带 kind/key，环回绝文本展示哈希序列，反查身份归持有方经本方法。
func (id ID) Hash() uint64 {
	h := fnv.New64a()
	h.Write([]byte(id.String())) // fnv.Writer 的 Write 不产生错误（纯算术）
	return h.Sum64()
}

// NodeID 是集群节点号。0 是匿名源哨兵：表示无来源节点（本地产生或来源
// 未知的消息/错误）；有效节点号从 1 起。
type NodeID uint16
