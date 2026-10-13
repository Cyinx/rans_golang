package main

import (
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// kindPattern 与 rans.ValidKindName 同规则本地复制（ransgen 只依赖标准库、
// 不 import 契约包——生成器可独立 go run）；正本 rans/id.go，两处必须
// 保持一致。
var kindPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,63}$`)

// namePattern wire 注册名（//rans:name 显式名）字符集：首字符字母/下划线，
// 其余字母数字点横线——点号支撑与派生名 <Kind>.<Method> 同构的命名空间
// 形态，横线对齐 kind 字符集；上限 128 段。空名不在此匹配（独立报错：
// 空 wire 名撞无值应答协议哨兵 codec.Encode(nil) 的 ("", nil, nil) 形态，
// 注册表同名消息会吞掉无值应答的解码，必须在门卫层拦）。
var namePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,127}$`)

// PackageDecl 是一次包扫描的完整发现结果（已排序，见 normalize）。
type PackageDecl struct {
	Name      string          // 包名
	Entities  []EntityDecl    // 按 KindName 升序
	Messages  []MessageDecl   // 按 Name 升序
	TypeNames map[string]bool // 包内全部声明类型名（请求类型发现规则用）
}

// EntityDecl 实体声明：//rans:kind=<name> 注解的 struct 及其方法集。
type EntityDecl struct {
	KindName string       // 注解的 kind 名（目录/身份键成分）
	TypeName string       // Go 类型名
	Methods  []MethodDecl // 按 Name 升序
}

// MethodDecl 方法签名（从 AST 提取）。Args/Returns 保持源码顺序——位置
// 语义，normalize 不重排。Idempotent 报告方法声明 doc 上的
// //rans:idempotent 旗注解（规格 §9.2：幂等标注的生成代理默认追加幂等
// 选项）。Pos 为声明处 file:line（派生请求的冲突定位用）。
type MethodDecl struct {
	Name       string
	Args       []FieldDecl
	Returns    []FieldDecl
	Idempotent bool
	Pos        string
}

// MessageDecl 消息声明：含 rans:"order=N" 字段 tag 的 struct。
//
// 方法派生的请求类型不自动入消息：只有带 order tag 字段的 struct 进入本
// 列表，//rans:name 仅提供显式命名。同一 struct 可既是实体又是消息（发现
// 层双登记；实体状态的序列化面不在此拦截）。
type MessageDecl struct {
	Name         string      // wire TypeName：//rans:name 显式命名，否则类型名
	NameExplicit bool        // Name 来自显式 //rans:name（采纳门卫依据）
	TypeName     string      // Go 类型名
	Pos          string      // 声明处 file:line（跨包 wire 名冲突定位用）
	Fields       []FieldDecl // 仅含 order tag 字段，按 Order 升序
}

// FieldDecl 字段/参数描述。Order 仅消息字段有意义（按 tag 值填充）。
type FieldDecl struct {
	Name  string
	Type  string
	Order int
}

// LoadPackage 解析 dir（跳过 _test.go 与 skip 指定的输出文件名）并发现
// 实体/方法/消息注解。门卫失败（空 kind、kind 字符集、非 struct 实体、
// 重复消息名、坏 order tag、重复 order、未知注解）返回带 file:line 的错误。
//
// skip（即 -out，默认 rans_gen.go）必须被排除：否则方法派生请求类型在
// 生成文件里二跑被重扫成消息（注册名落回类型名），破坏「派生请求不入
// 消息」契约与 generate 幂等。空串表示不排除任何文件。
func LoadPackage(dir, skip string) (*PackageDecl, error) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi fs.FileInfo) bool {
		if strings.HasSuffix(fi.Name(), "_test.go") {
			return false
		}
		return skip == "" || fi.Name() != skip
	}, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("ransgen: parse %s: %w", dir, err)
	}
	if len(pkgs) == 0 {
		return nil, fmt.Errorf("ransgen: %s: no non-test Go files found", dir)
	}
	if len(pkgs) > 1 {
		names := make([]string, 0, len(pkgs))
		for name := range pkgs {
			names = append(names, name)
		}
		sort.Strings(names)
		return nil, fmt.Errorf("ransgen: %s: multiple packages %v in one directory", dir, names)
	}

	var name string
	var pkg *ast.Package
	for name, pkg = range pkgs {
		break
	}

	files := make([]*ast.File, 0, len(pkg.Files))
	for _, f := range pkg.Files {
		files = append(files, f)
	}
	sort.Slice(files, func(i, j int) bool {
		return fset.Position(files[i].Pos()).Filename < fset.Position(files[j].Pos()).Filename
	})

	pd := &PackageDecl{Name: name, TypeNames: map[string]bool{}}
	// 第一遍：类型声明（实体 + 消息）。
	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}
			for _, spec := range gd.Specs {
				ts := spec.(*ast.TypeSpec)
				if err := collectTypeDecl(fset, pd, gd, ts); err != nil {
					return nil, err
				}
			}
		}
	}
	if err := checkMessageNames(pd); err != nil {
		return nil, err
	}
	// 第二遍：方法（实体类型已知后归属）。
	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Recv == nil {
				continue
			}
			recv, ok := receiverTypeName(fd)
			if !ok {
				continue
			}
			for i := range pd.Entities {
				if pd.Entities[i].TypeName == recv {
					md, err := methodDecl(fset, fd)
					if err != nil {
						return nil, err
					}
					pd.Entities[i].Methods = append(pd.Entities[i].Methods, md)
				}
			}
		}
	}
	pd.normalize()
	return pd, nil
}

// collectTypeDecl 处理单个 TypeSpec：实体注解校验 + 消息字段发现。
// 注解优先取 spec.Doc（分组声明形态），退回 GenDecl.Doc（单声明形态）。
func collectTypeDecl(fset *token.FileSet, pd *PackageDecl, gd *ast.GenDecl, ts *ast.TypeSpec) error {
	// 全类型名登记（不限 struct）：方法派生请求类型的发现规则按名匹配。
	pd.TypeNames[ts.Name.Name] = true

	doc := ts.Doc
	if doc == nil {
		doc = gd.Doc
	}
	ann, err := parseAnnotations(doc)
	if err != nil {
		return fmt.Errorf("%s: %w", fset.Position(ts.Pos()), err)
	}
	if ann.idempotent {
		return fmt.Errorf("%s: rans:idempotent is method-level (annotate the method declaration, not type %s)",
			fset.Position(ts.Pos()), ts.Name.Name)
	}

	st, isStruct := ts.Type.(*ast.StructType)
	// 泛型门卫（v1）：kind 注解类型与含 rans tag 字段的 struct（消息候选）
	// 拒绝类型参数——生成物以裸类型名引用 <T>（wrapper/助手/请求类型），
	// 无类型实参无法编译；生成面之外的泛型声明不受影响。
	if ts.TypeParams != nil {
		switch {
		case ann.hasKind:
			return fmt.Errorf("%s: entity %s: generics not supported (v1): type parameters on kind-annotated type",
				fset.Position(ts.Pos()), ts.Name.Name)
		case isStruct && structHasRansTag(st):
			return fmt.Errorf("%s: type %s: generics not supported (v1): type parameters on struct with rans-tagged field",
				fset.Position(ts.Pos()), ts.Name.Name)
		}
	}
	if ann.hasKind {
		if !isStruct {
			return fmt.Errorf("%s: entity annotation on non-struct type %s",
				fset.Position(ts.Pos()), ts.Name.Name)
		}
		if ann.kind == "" {
			return fmt.Errorf("%s: entity %s: empty kind name",
				fset.Position(ts.Pos()), ts.Name.Name)
		}
		if !kindPattern.MatchString(ann.kind) {
			return fmt.Errorf("%s: entity %s: invalid kind name %q (charset: ^[A-Za-z_][A-Za-z0-9_-]{0,63}$)",
				fset.Position(ts.Pos()), ts.Name.Name, ann.kind)
		}
		pd.Entities = append(pd.Entities, EntityDecl{KindName: ann.kind, TypeName: ts.Name.Name})
	}

	if !isStruct {
		// //rans:name 只对 struct 有意义；非 struct 上的该注解静默忽略
		//（发现层不为此设门卫）。
		return nil
	}
	// //rans:name 值门卫：空名与非法字符在此拦（struct 上一律校验——不合法
	// 的注解值不因「暂无 order tag 字段」而放过，补字段时同名即直通注册）。
	if ann.hasName {
		if err := checkWireName(ann.name); err != nil {
			return fmt.Errorf("%s: type %s: %w", declPos(fset, ts.Pos()), ts.Name.Name, err)
		}
	}
	msg, found, err := collectMessage(fset, ts, st)
	if err != nil {
		return err
	}
	if found {
		msg.Name = ts.Name.Name
		msg.Pos = declPos(fset, ts.Pos())
		if ann.hasName {
			msg.Name = ann.name
			msg.NameExplicit = true
		}
		pd.Messages = append(pd.Messages, msg)
	}
	return nil
}

// checkWireName 校验 //rans:name 注解值（空名/字符集违例分报；哨兵碰撞见 namePattern 注释）。
func checkWireName(name string) error {
	if name == "" {
		return fmt.Errorf("empty rans:name")
	}
	if !namePattern.MatchString(name) {
		return fmt.Errorf("invalid rans:name %q (charset: ^[A-Za-z_][A-Za-z0-9_.-]{0,127}$)", name)
	}
	return nil
}

// declPos 声明位置的稳定文本形 file:line（门卫与冲突错误定位用）。
func declPos(fset *token.FileSet, pos token.Pos) string {
	p := fset.Position(pos)
	return fmt.Sprintf("%s:%d", p.Filename, p.Line)
}

// collectMessage 提取 struct 的 order tag 字段。found=false 表示没有任何
// rans tag 字段（非消息）。任何 rans tag 字段的 tag 值必须形如 order=N。
func collectMessage(fset *token.FileSet, ts *ast.TypeSpec, st *ast.StructType) (MessageDecl, bool, error) {
	msg := MessageDecl{TypeName: ts.Name.Name}
	seen := map[int]string{}
	for _, field := range st.Fields.List {
		val, ok := ransTagValue(field)
		if !ok {
			continue // 无 rans 键：本地状态字段或其他用途 tag（如 json），跳过
		}
		if err := checkTaggedFieldNames(fset, ts.Name.Name, field); err != nil {
			return MessageDecl{}, false, fmt.Errorf("%s: %w", fset.Position(field.Pos()), err)
		}
		order, err := parseOrderTag(val)
		if err != nil {
			return MessageDecl{}, false, fmt.Errorf("%s: field %s: %w",
				fset.Position(field.Pos()), fieldName(field), err)
		}
		if prev, dup := seen[order]; dup {
			return MessageDecl{}, false, fmt.Errorf("%s: duplicate order %d on fields %s and %s",
				fset.Position(field.Pos()), order, prev, fieldName(field))
		}
		seen[order] = fieldName(field)
		for _, name := range field.Names {
			msg.Fields = append(msg.Fields, FieldDecl{
				Name:  name.Name,
				Type:  exprString(fset, field.Type),
				Order: order,
			})
		}
	}
	return msg, len(msg.Fields) > 0, nil
}

// ransTagValue 返回字段 rans tag 的值与是否存在该键；无 tag 或无 rans 键
// 时第二返回值为 false。
func ransTagValue(field *ast.Field) (string, bool) {
	if field.Tag == nil {
		return "", false
	}
	tagVal := strings.Trim(field.Tag.Value, "`")
	val, ok := reflect.StructTag(tagVal).Lookup("rans")
	return val, ok
}

// structHasRansTag 报告 struct 是否含 rans tag 字段（消息候选判定，
// 泛型门卫复用）。
func structHasRansTag(st *ast.StructType) bool {
	for _, field := range st.Fields.List {
		if _, ok := ransTagValue(field); ok {
			return true
		}
	}
	return false
}

// checkTaggedFieldNames 拒绝 rans tag 落在生成面无法正确表达的字段名形态：
// 匿名嵌入字段（tag 被解析且 order 被 seen 占位，但无 FieldDecl 产出——
// 消息静默缺字段）；多名字段（A, B string 共享一条 tag 只登记一次 order
// 却产出两个同 order 的 FieldDecl → 两份重复 case 臂，round-trip 错乱）；
// 空名 _（产出 FieldDecl{Name:"_"} → 生成 v._ 非法源码）。契约：一条
// rans tag 对应恰好一个具名字段。
func checkTaggedFieldNames(fset *token.FileSet, typeName string, field *ast.Field) error {
	if len(field.Names) == 0 {
		return fmt.Errorf("type %s: embedded field %s: rans tag not supported on embedded (unnamed) field (v1)",
			typeName, exprString(fset, field.Type))
	}
	if len(field.Names) > 1 {
		return fmt.Errorf("type %s: fields %s: rans tag not supported on multi-name field (v1): one name per tagged field",
			typeName, fieldName(field))
	}
	if field.Names[0].Name == "_" {
		return fmt.Errorf("type %s: field _: rans tag not supported on blank-named field (v1)", typeName)
	}
	return nil
}

// parseOrderTag 解析 rans tag 值，仅接受 order=<非负整数>。
func parseOrderTag(val string) (int, error) {
	rest, ok := strings.CutPrefix(val, "order=")
	if !ok {
		return 0, fmt.Errorf("malformed rans tag %q (want order=N)", val)
	}
	n, err := strconv.Atoi(rest)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("malformed rans tag %q (want order=N)", val)
	}
	return n, nil
}

// checkMessageNames 校验消息最终名（显式或类型名派生）包内唯一——覆盖
// 重复 //rans:name 与显式名撞类型名两种形态。
func checkMessageNames(pd *PackageDecl) error {
	return checkMessageList(pd.Messages)
}

// checkMessageList 校验消息清单注册名唯一（含用户消息与方法派生/改名请求
// 合并后的清单——派生 wire 名撞用户显式名在此拦截）。
func checkMessageList(msgs []MessageDecl) error {
	seen := map[string]string{}
	for _, m := range msgs {
		if prev, dup := seen[m.Name]; dup {
			return fmt.Errorf("ransgen: duplicate message name %q (types %s and %s)",
				m.Name, prev, m.TypeName)
		}
		seen[m.Name] = m.TypeName
	}
	return nil
}

// annotations 从 doc 注释提取 rans: 前缀注解：类型注解 //rans:key=value
// （kind/name）+ 方法旗注解 //rans:idempotent（不带值）。
type annotations struct {
	kind       string
	hasKind    bool
	name       string
	hasName    bool
	idempotent bool
}

func parseAnnotations(doc *ast.CommentGroup) (annotations, error) {
	var a annotations
	if doc == nil {
		return a, nil
	}
	for _, c := range doc.List {
		line := strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
		if !strings.HasPrefix(line, "rans:") {
			continue
		}
		key, val, _ := strings.Cut(strings.TrimPrefix(line, "rans:"), "=")
		switch key {
		case "kind":
			if a.hasKind {
				return a, fmt.Errorf("duplicate rans:kind annotation %q", val)
			}
			a.kind, a.hasKind = val, true
		case "name":
			if a.hasName {
				return a, fmt.Errorf("duplicate rans:name annotation %q", val)
			}
			a.name, a.hasName = val, true
		case "idempotent":
			if a.idempotent {
				return a, fmt.Errorf("duplicate rans:idempotent annotation")
			}
			if val != "" {
				return a, fmt.Errorf("malformed annotation rans:idempotent=%s (flag annotation takes no value)", val)
			}
			a.idempotent = true
		default:
			return a, fmt.Errorf("unknown annotation rans:%s=%s", key, val)
		}
	}
	return a, nil
}

func fieldName(field *ast.Field) string {
	var names []string
	for _, n := range field.Names {
		names = append(names, n.Name)
	}
	return strings.Join(names, ",")
}

// exprString 用 go/format 渲染类型表达式为源码串。
func exprString(fset *token.FileSet, e ast.Expr) string {
	var b strings.Builder
	if err := format.Node(&b, fset, e); err != nil {
		return "" // 已解析成功的表达式渲染不会失败；保底空串
	}
	return b.String()
}

// normalize 就地排序为规范序（确定性输出根基）：实体按 KindName、消息按
// Name、方法按 Name、消息字段按 Order 升序；方法参数/返回值保持源码顺序。
func (p *PackageDecl) normalize() {
	sort.Slice(p.Entities, func(i, j int) bool {
		return p.Entities[i].KindName < p.Entities[j].KindName
	})
	for i := range p.Entities {
		methods := p.Entities[i].Methods
		sort.Slice(methods, func(a, b int) bool { return methods[a].Name < methods[b].Name })
	}
	sort.Slice(p.Messages, func(i, j int) bool {
		return p.Messages[i].Name < p.Messages[j].Name
	})
	for i := range p.Messages {
		fields := p.Messages[i].Fields
		sort.SliceStable(fields, func(a, b int) bool { return fields[a].Order < fields[b].Order })
	}
}
