// Package genfixture 生成实体冒烟 fixture（testkit 生成物×双节点套件的
// 实体面）：手写实体（kind=gen）+ ransgen 生成物（rans_gen.go 入库，
// //go:generate 再生成），形态复刻 examples/chat 的消费方集成惯例。
// 实体覆盖三类生成路径——请求-响应（[]string 字段：序列线型编解码的
// 真 wire 首跑）、OneWay、快照面（迁移随行状态）。仅供 testkit 冒烟套件
// 链接；wire 注册名（gen.*）不进发布 schema 基线（Makefile SCHEMA_DIRS
// 与 schema_test wireDirs 均不含本包——基线只辖消费方可见的线面），线面
// 漂移由 cmd/ransgen 的漂移锁钉住。
package genfixture

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/cyinx/rans_golang"
)

//go:generate go run github.com/cyinx/rans_golang/cmd/ransgen

// LogReply Append 的应答消息：[]string 字段按序列线型编码（WireSequence
// of String，规格 §7.3）。
type LogReply struct {
	Lines []string `rans:"order=1"`
}

// Notebook gen 实体：Append 追加并返回全量行，Mark 追加标记行。
// 用户实体不实现 rans.Entity——伴生 wrapper NotebookPawnite 由生成器
// 产出（生命周期委托/Handle 分发/快照透传）。
//
//rans:kind=gen
type Notebook struct {
	lines []string // 本地状态：无 order tag 不进消息面；快照面承载迁移
}

// 快照面编译锚：Snapshot/Restore 成对实现（生成器成对门卫同款约束），
// wrapper 据此透传 rans.Snapshotter——迁移/钝化/checkpoint 的能力发现面。
var _ rans.Snapshotter = (*Notebook)(nil)

// Append 请求-响应形态：追加 items 后返回全量行。请求与应答两侧的
// []string 都经生成序列化器在真 wire 上往返（冒烟套件的 WireSequence
// 载体）；返回克隆值——实体状态不随应答值外泄。
func (n *Notebook) Append(ctx context.Context, items []string) (LogReply, error) {
	n.lines = append(n.lines, items...)
	return LogReply{Lines: slices.Clone(n.lines)}, nil
}

// Mark OneWay 形态（无返回值）：标记行入箱即返回，无应答无回执；送达
// 以状态可见为准（冒烟套件经后续 Call 读回断言）。
func (n *Notebook) Mark(ctx context.Context, tag string) {
	n.lines = append(n.lines, "mark:"+tag)
}

// Snapshot 快照捕获（恒在实体 loop 串行静默点调用，无须加锁）：行清单
// 序列化为「uint32 行数 + 逐行 uint32 长度 + UTF-8 字节」。
func (n *Notebook) Snapshot() ([]byte, error) {
	buf := make([]byte, 4, 4+len(n.lines)*8)
	binary.BigEndian.PutUint32(buf, uint32(len(n.lines)))
	for _, line := range n.lines {
		row := make([]byte, 4, 4+len(line))
		binary.BigEndian.PutUint32(row, uint32(len(line)))
		row = append(row, line...)
		buf = append(buf, row...)
	}
	return buf, nil
}

// Restore 快照恢复（激活闭包语境）：形状不符（截断/长度越界/尾随字节）
// 必须报错——上层据此退化 Store 兜底，不得 panic 或静默截断。
func (n *Notebook) Restore(b []byte) error {
	if len(b) < 4 {
		return fmt.Errorf("genfixture: snapshot shape mismatch: %d bytes, want >= 4", len(b))
	}
	count := int(binary.BigEndian.Uint32(b))
	b = b[4:]
	// 每行至少占 4 字节头：上界预检拒绝畸形大计数（防越界预分配）。
	if count > len(b)/4 {
		return fmt.Errorf("genfixture: snapshot shape mismatch: %d lines cannot fit %d bytes", count, len(b))
	}
	lines := make([]string, 0, count)
	for i := 0; i < count; i++ {
		if len(b) < 4 {
			return fmt.Errorf("genfixture: snapshot truncated at line %d of %d", i, count)
		}
		size := int(binary.BigEndian.Uint32(b))
		b = b[4:]
		if size > len(b) {
			return fmt.Errorf("genfixture: snapshot line %d overruns payload: %d > %d", i, size, len(b))
		}
		lines = append(lines, string(b[:size]))
		b = b[size:]
	}
	if len(b) != 0 {
		return fmt.Errorf("genfixture: snapshot trailing bytes: %d", len(b))
	}
	n.lines = lines
	return nil
}
