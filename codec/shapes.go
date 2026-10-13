package codec

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
	"sync"
)

// 形状登记面：生成物（rans_gen.go）在 init 里把 RansShapes 登记进来；
// 摘要供握手观测与严格模式使用（H9）。登记是集合语义——重复声明不改变
// 摘要，声明顺序不改变摘要；跨包各自登记，摘要覆盖全量并集。

var (
	shapesMu sync.Mutex
	shapes   = map[string]struct{}{}
)

// RegisterShapes 登记本包生成物的消息形状（"TypeName:Field:Order:Type"，
// 生成器产出）。集合语义（去重、顺序无关、摘要覆盖并集）见文件头注释；
// 读数经 SchemaDigest。不阻塞；并发安全（互斥）；init 期装配调用。
func RegisterShapes(list []string) {
	shapesMu.Lock()
	for _, s := range list {
		shapes[s] = struct{}{}
	}
	shapesMu.Unlock()
}

// SchemaDigest 返回全量已登记形状的 FNV-1a 64 摘要（**去掉 kind 维**）：
// 形状集唯一决定摘要——集合相等（无论声明序）即摘要相等，增删任一形状
// 即变化。规范字节串为 [个数 uvarint]{[长度 uvarint][字节]}…（长度前缀
// 定界，元素内容任意都不产生拼接歧义）。不阻塞；并发安全；进程启动期
// 装配调用（Register/RegisterShapes 之后、节点 New 之前）。
func SchemaDigest() uint64 {
	shapesMu.Lock()
	uniq := make([]string, 0, len(shapes))
	for s := range shapes {
		uniq = append(uniq, s)
	}
	shapesMu.Unlock()
	sort.Strings(uniq)
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(uniq)))
	for _, s := range uniq {
		b = binary.AppendUvarint(b, uint64(len(s)))
		b = append(b, s...)
	}
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}
