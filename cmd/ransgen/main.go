// ransgen 是 Rans 代码生成器（规格 §6）：扫描目标包源码注解（//rans:kind /
// rans:"order=N" 字段 tag），按模板产出 rans_gen.go。
//
// 功能：序列化器生成——每条消息一个 codec.Register 注册 + 编码/解码助手，
// 字段类型门卫（S004/M405）；实体生成——kind→factory 的 Kinds 注册表 +
// <Entity>Pawnite wrapper，E316 双射门卫；调用生成——每方法请求类型
// （wire 路由名 <Kind>.<Method>）+ wrapper 类型 switch 分发 + <Entity>Client
// 客户端代理（C rans.Client，方法携 opts ...rans.CallOption），M402 返回
// 形态门卫；schema 清单面——-schema 产出 rans_schema.json（消息 → 字段
// {id, name, wiretype} 全量），-check 对发布基线做兼容检查。
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// dirList 可重复的 -dir 旗标：多目录合并产出/检查清单（各包消息面求并，
// wire 注册名跨包必须唯一）。
type dirList []string

// String 旗标值渲染（flag.Value 接口）：逗号连接。
func (d *dirList) String() string { return strings.Join(*d, ",") }

// Set 追加一次 -dir 取值（flag.Value 接口）。
func (d *dirList) Set(v string) error {
	*d = append(*d, v)
	return nil
}

func main() {
	var dirs dirList
	flag.Var(&dirs, "dir", "目标包目录（可重复，缺省当前目录；多目录合并产出/检查清单）")
	out := flag.String("out", "rans_gen.go", "生成物文件名（相对 -dir）")
	schemaOut := flag.String("schema", "", "产出 rans_schema.json 到指定路径（消息 → 字段 {id, name, wiretype} 全量清单）")
	check := flag.String("check", "", "对基线清单做兼容检查：同字段号改线型/字段号复用 → 失败（退出 1）；删字段/删消息 → 告警")
	flag.Parse()
	if len(dirs) == 0 {
		dirs = dirList{"."}
	}

	switch {
	case *check != "":
		res, err := runCheck(dirs, *check)
		if err != nil {
			fmt.Fprintln(os.Stderr, "ransgen:", err)
			os.Exit(1)
		}
		for _, w := range res.Warnings {
			fmt.Println("warning:", w)
		}
		for _, f := range res.Failures {
			fmt.Fprintln(os.Stderr, "ransgen:", f)
		}
		if len(res.Failures) > 0 {
			os.Exit(1)
		}
		fmt.Println("ransgen: schema check ok（线面对基线兼容）")
	case *schemaOut != "":
		if err := runSchema(dirs, *schemaOut); err != nil {
			fmt.Fprintln(os.Stderr, "ransgen:", err)
			os.Exit(1)
		}
	default:
		for _, dir := range dirs {
			if err := run(dir, *out); err != nil {
				fmt.Fprintln(os.Stderr, "ransgen:", err)
				os.Exit(1)
			}
		}
	}
}

// genModel 全文件生成模型：序列化器面 + 实体面 + 形状清单。
type genModel struct {
	Name     string
	UseMath  bool
	Messages []messageModel
	Entities []entityModel
	Shapes   []string // 消息形状清单（完整指纹形态）：字典序 "TypeName:Field:Order:Type"
}

// Imports 生成物导入行清单（空串条目 = 分组间空行；std 组 → 本仓组）。
// 产出行内已按路径升序，gofmt 组内排序不重排。本仓导入路径硬编码（生成器
// 与运行时同仓同版本）。生成物只 import 公开包（约定 spec §5.9：rans/
// codec）；codec 恒在：消息注册 init 之外，RansShapes 的登记 init
// （RegisterShapes）无条件生成。
func (m *genModel) Imports() []string {
	var std, repo []string
	if len(m.Entities) > 0 {
		std = append(std, `"context"`, `"fmt"`)
		repo = append(repo, `"github.com/cyinx/rans_golang"`)
	}
	if m.UseMath {
		std = append(std, `"math"`)
	}
	repo = append(repo, `"github.com/cyinx/rans_golang/codec"`)
	var out []string
	out = append(out, std...)
	if len(std) > 0 && len(repo) > 0 {
		out = append(out, "")
	}
	return append(out, repo...)
}

// planGen 全流程计划：normalize 归一 → 实体面（E316/生命周期/M402/请求
// 类型解析——用户手写同名请求被采纳并改挂 wire 路由名）→ 合并消息清单
// （注册名唯一性 + 按 Name 升序）→ 字段类型门卫（S004 复用）→ 合成模型。
func planGen(pkg *PackageDecl) (*genModel, error) {
	pkg.normalize()
	ents, msgs, err := planEntities(pkg)
	if err != nil {
		return nil, err
	}
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].Name < msgs[j].Name })
	if err := checkMessageList(msgs); err != nil {
		return nil, err
	}
	mms, useMath, err := planMessages(msgs)
	if err != nil {
		return nil, err
	}
	return &genModel{
		Name:     pkg.Name,
		UseMath:  useMath,
		Messages: mms,
		Entities: ents,
		Shapes:   planShapes(msgs),
	}, nil
}

// run 执行一次生成：解析 → 计划（类型门卫）→ 渲染 → 差异落盘。内容与现存
// 文件一致时不写（mtime 保持，generate 幂等的落盘半边）；门卫失败返回
// 错误（main 转非零退出）。
func run(dir, out string) error {
	pkg, err := LoadPackage(dir, out)
	if err != nil {
		return err
	}
	model, err := planGen(pkg)
	if err != nil {
		return err
	}
	content, err := Render(model, ransgenTemplate)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, out)
	if old, err := os.ReadFile(path); err == nil && bytes.Equal(old, content) {
		return nil
	}
	return os.WriteFile(path, content, 0o644)
}
