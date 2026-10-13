// method 生成器（规格 §6 的方法面发现）：从实体方法声明提取签名与位置，
// 并落地参数名门卫——载荷参数名直通派生请求的字段名，病态名（空白名、
// capitalize 归一碰撞）在解析期拦截，不产出坏码。
package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
)

// receiverTypeName 提取方法接收者的基类型名（值/指针接收者均可）。
func receiverTypeName(fd *ast.FuncDecl) (string, bool) {
	if len(fd.Recv.List) != 1 {
		return "", false
	}
	t := fd.Recv.List[0].Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}
	id, ok := t.(*ast.Ident)
	if !ok {
		return "", false // 泛型/复杂接收者：v1 不支持，静默跳过
	}
	return id.Name, true
}

// methodDecl 从方法声明提取签名（接收者不计入 Args）与 //rans:idempotent
// 旗注解（注解带值/重复即解析错误，位置为方法声明处）。参数名门卫只对
// RPC 面方法生效（请求字段只为其生成；生命周期/快照面/未导出方法的参数
// 名不进生成物，不受约束）。
func methodDecl(fset *token.FileSet, fd *ast.FuncDecl) (MethodDecl, error) {
	ann, err := parseAnnotations(fd.Doc)
	if err != nil {
		return MethodDecl{}, fmt.Errorf("%s: %w", fset.Position(fd.Pos()), err)
	}
	md := MethodDecl{
		Name:       fd.Name.Name,
		Args:       fieldDecls(fset, fd.Type.Params),
		Returns:    fieldDecls(fset, fd.Type.Results),
		Idempotent: ann.idempotent,
		Pos:        declPos(fset, fd.Pos()),
	}
	if rpcFaceMethod(md) {
		if err := checkParamNames(md.Name, md.Args); err != nil {
			return MethodDecl{}, fmt.Errorf("%s: %w", declPos(fset, fd.Pos()), err)
		}
	}
	return md, nil
}

// checkParamNames 方法参数名门卫（解析期）：载荷参数名直接成为派生请求
// 的字段名，两类病态名在生成期零诊断、坏码延迟到用户包编译才爆出，故在
// 解析层拦——与消息字段侧的 checkTaggedFieldNames 孪生（`_` 是 Go 未用
// 参数惯用法，字段侧早有同款先例、参数侧此前漏掉）：
//   - 空白名 `_`：产出字段访问 v._ 与字面量键 _ 均非合法源码。即使用户
//     手写请求类型采纳路径可覆写字段名，参数本身仍按笔误拦——`_` 表达的
//     「无绑定」意图在位置编码里不存在；
//   - capitalize 归一碰撞：foo/Foo 两个参数归一后同名，产出重复字段声明。
//
// 首参（ctx 位）不入载荷、名字被生成体丢弃，不受本门卫约束。
func checkParamNames(method string, args []FieldDecl) error {
	payload := args
	if len(payload) > 0 {
		payload = payload[1:]
	}
	for _, a := range payload {
		if a.Name == "_" {
			return fmt.Errorf("method %s: parameter _: blank parameter name not supported (v1): "+
				"the parameter name becomes a request field and a blank field cannot be referenced "+
				"by the generated code — name the parameter or drop it", method)
		}
	}
	names := requestFieldNames(payload)
	seen := make(map[string]string, len(names))
	for i, n := range names {
		label := paramLabel(i, payload[i])
		if prev, dup := seen[n]; dup {
			return fmt.Errorf("method %s: parameters %s and %s both derive request field %q — "+
				"rename one parameter", method, prev, label, n)
		}
		seen[n] = label
	}
	return nil
}

// requestFieldNames 载荷参数 → 请求字段名的唯一合成源（checkParamNames 与
// planRPCMethod 共用，两处规则不得漂移）：命名参数 capitalize 归一，未命名
// 参数按载荷位（1 起）合成 ArgN。
func requestFieldNames(args []FieldDecl) []string {
	names := make([]string, len(args))
	for i, a := range args {
		if a.Name == "" {
			names[i] = "Arg" + strconv.Itoa(i+1)
			continue
		}
		names[i] = capitalize(a.Name)
	}
	return names
}

// paramLabel 冲突错误里的参数指称：具名用原名，未命名用载荷位标注。
func paramLabel(i int, a FieldDecl) string {
	if a.Name == "" {
		return fmt.Sprintf("#%d (unnamed)", i+1)
	}
	return a.Name
}

// fieldDecls 字段列表 → FieldDecl（参数/返回值共用）：多名字段按名展开，
// 匿名字段以空名占位（位置语义由消费方解释）。
func fieldDecls(fset *token.FileSet, fl *ast.FieldList) []FieldDecl {
	if fl == nil {
		return nil
	}
	var out []FieldDecl
	for _, field := range fl.List {
		if len(field.Names) == 0 {
			out = append(out, FieldDecl{Name: "", Type: exprString(fset, field.Type)})
			continue
		}
		for _, name := range field.Names {
			out = append(out, FieldDecl{Name: name.Name, Type: exprString(fset, field.Type)})
		}
	}
	return out
}
