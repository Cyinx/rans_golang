// schema 生成器（加固 spec §6.1 第 5 点）：rans_schema.json 产出
// 与 -check 兼容比对。清单 = 消息（wire 注册名）→ 字段 {id, name, wiretype}
// 全量；消息按 wire 名、字段按 id 升序 + 缩进稳定，逐字节可比对。-check
// 判据：同字段号改线型、同消息内字段号复用 → 失败（发布阻断，非零退出）；
// 删字段、删消息 → 告警（打印不阻断）；增字段、增消息走兼容通道静默通过。
// 基线 = 上一发布 tag 的清单，随发版更新（见 README「schema 兼容检查」节）。
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// ransSchema 是 rans_schema.json 的文件模型。
type ransSchema struct {
	Messages []schemaMessage `json:"messages"`
}

// schemaMessage 单消息：Name 为 wire 注册名（方法派生 <Kind>.<Method>、
// 显式 //rans:name 或类型名）。
type schemaMessage struct {
	Name   string        `json:"name"`
	Fields []schemaField `json:"fields"`
}

// schemaField 单字段：ID 即线序号（rans:"order=N"）。
type schemaField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	WireType string `json:"wiretype"`
}

// wireTypeNames fieldKind → 清单稳定字符串形（与 codec.Wire* 常量一一
// 对应；序列字段的线型在 field-tag 层即 WireSequence，元素线型随载荷内嵌）。
var wireTypeNames = map[fieldKind]string{
	kVarInt:  "varint",
	kFixed64: "fixed64",
	kString:  "string",
	kBytes:   "bytes",
	kMessage: "message",
	kSeq:     "sequence",
}

// buildSchema 对每个 dir 走完整生成管线（LoadPackage 排除生成物 + planGen
// 类型门卫），合并各包消息面为全量清单。skip 为生成物排除名，跟随 -out
// 旗标（排除的必须正是 generate 模式将要写出的那份文件——生成物回扫会让
// 派生请求的注册名落回类型名，清单失真），不得硬编码缺省名。wire 注册名
// 跨包必须唯一（清单按名索引，重名即歧义）；字段已由 planGen 门卫过支持
// 集，此处只做线型映射。
func buildSchema(dirs []string, skip string) (*ransSchema, error) {
	byName := map[string]string{} // wire 名 → 首见来源（Go 类型名，报重名用）
	var out []schemaMessage
	for _, dir := range dirs {
		pkg, err := LoadPackage(dir, skip)
		if err != nil {
			return nil, err
		}
		model, err := planGen(pkg)
		if err != nil {
			return nil, err
		}
		for _, m := range model.Messages {
			if prev, dup := byName[m.Name]; dup {
				return nil, fmt.Errorf("ransgen: duplicate wire message %q (types %s and %s across packages)",
					m.Name, prev, m.TypeName)
			}
			byName[m.Name] = m.TypeName
			sm := schemaMessage{Name: m.Name, Fields: make([]schemaField, 0, len(m.Fields))}
			for _, f := range m.Fields {
				wt, ok := wireTypeNames[f.Kind]
				if !ok {
					return nil, fmt.Errorf("ransgen: message %s: field %s: unclassified wire kind", m.Name, f.Name)
				}
				sm.Fields = append(sm.Fields, schemaField{ID: f.ID, Name: f.Name, WireType: wt})
			}
			sort.Slice(sm.Fields, func(i, j int) bool { return sm.Fields[i].ID < sm.Fields[j].ID })
			out = append(out, sm)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return &ransSchema{Messages: out}, nil
}

// renderSchemaJSON 渲染清单为稳定 JSON 字节（两空格缩进 + 尾换行）——
// 确定性排序下同一线面得到逐字节相同的文件。
func renderSchemaJSON(s *ransSchema) ([]byte, error) {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("ransgen: marshal schema: %w", err)
	}
	return append(b, '\n'), nil
}

// loadSchema 载入清单文件（基线或既有产出）。
func loadSchema(path string) (*ransSchema, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ransgen: read schema %s: %w", path, err)
	}
	var s ransSchema
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("ransgen: parse schema %s: %w", path, err)
	}
	return &s, nil
}

// checkResult 兼容比对结论：Failures 为发布阻断项（-check 非零退出），
// Warnings 为告警项（打印不阻断）。
type checkResult struct {
	Failures []string
	Warnings []string
}

// checkSchema 基线（上一发布 tag）对当前清单的兼容比对：同消息内字段号
// 换用新字段名 → 复用失败；同字段号同字段名改线型 → 线型失败；基线字段/
// 消息在当前缺席 → 告警。基线消息序即遍历序（清单按名升序产出），结论
// 顺序确定。
func checkSchema(base, cur *ransSchema) checkResult {
	curFields := map[string]map[int]schemaField{}
	for _, m := range cur.Messages {
		fs := make(map[int]schemaField, len(m.Fields))
		for _, f := range m.Fields {
			fs[f.ID] = f
		}
		curFields[m.Name] = fs
	}
	var res checkResult
	for _, m := range base.Messages {
		cfs, ok := curFields[m.Name]
		if !ok {
			res.Warnings = append(res.Warnings, fmt.Sprintf("消息 %q 已删除（基线有、当前无）", m.Name))
			continue
		}
		for _, f := range m.Fields {
			cf, ok := cfs[f.ID]
			if !ok {
				res.Warnings = append(res.Warnings, fmt.Sprintf("消息 %q 字段 %q（id=%d）已删除", m.Name, f.Name, f.ID))
				continue
			}
			if cf.Name != f.Name {
				res.Failures = append(res.Failures, fmt.Sprintf(
					"消息 %q 字段号 %d 复用：基线 %q → 当前 %q（同消息内字段号不得换用新字段）",
					m.Name, f.ID, f.Name, cf.Name))
				continue
			}
			if cf.WireType != f.WireType {
				res.Failures = append(res.Failures, fmt.Sprintf(
					"消息 %q 字段 %q（id=%d）线型变更：基线 %s → 当前 %s（同字段号不得改线型）",
					m.Name, f.Name, f.ID, f.WireType, cf.WireType))
			}
		}
	}
	return res
}

// runCheck 载入基线并对 dirs 的当前清单做兼容比对。skip 为生成物排除名
// （跟随 -out 旗标，见 buildSchema）。基础设施错误（清单不可建、基线不可
// 读/非法）走 error；兼容结论走 checkResult。
func runCheck(dirs []string, skip, baselinePath string) (checkResult, error) {
	cur, err := buildSchema(dirs, skip)
	if err != nil {
		return checkResult{}, err
	}
	base, err := loadSchema(baselinePath)
	if err != nil {
		return checkResult{}, err
	}
	return checkSchema(base, cur), nil
}

// runSchema 产出 dirs 合并清单到 outPath（skip 为生成物排除名，跟随 -out
// 旗标；内容一致不写，mtime 保持）。
func runSchema(dirs []string, skip, outPath string) error {
	s, err := buildSchema(dirs, skip)
	if err != nil {
		return err
	}
	content, err := renderSchemaJSON(s)
	if err != nil {
		return err
	}
	if old, err := os.ReadFile(outPath); err == nil && bytes.Equal(old, content) {
		return nil
	}
	return os.WriteFile(outPath, content, 0o644)
}
