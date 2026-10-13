// entity 生成器（规格 §6）：E316 kind↔实体双射门卫 + wrapper
// （<Entity>Pawnite 实现 rans.Entity：生命周期按用户声明委托、Handle
// 类型 switch 分发）+ Kinds 注册表 + 每方法请求类型（派生或沿用用户手写，
// wire 路由名 <Kind>.<Method>）+ 客户端 Client（<Entity>Client，载荷工厂闭包
// 语义：每次调用构造新请求值）。M402 返回形态门卫与参数形态门卫在此落地。
package main

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// entityModel 实体面模板数据（planEntities 产物）。
type entityModel struct {
	Kind          string // 注册 kind 名
	TypeName      string // 用户实体 Go 类型名
	Pawnite       string // wrapper 类型名 <T>Pawnite
	HasActivate   bool   // 用户实体声明 Activate(context.Context) error
	HasDeactivate bool   // 用户实体声明 Deactivate(context.Context) error
	HasSnapshot   bool   // 用户实体实现快照能力面（Capture/Restore 成对，形态门卫保证）——wrapper 透传
	Methods       []rpcModel
}

// rpcModel 单方法面的模板数据：请求类型 + 分发体 + 代理函数（后两者为
// 计划期整段产出的源码串，缩进交给 gofmt 归一）。
type rpcModel struct {
	Name       string // 方法名
	ReqType    string // 请求类型名 <Entity><Method>Request
	WireName   string // wire 路由名 <Kind>.<Method>
	UserReq    bool   // 用户已手写同款请求类型（不再生成声明）
	Idempotent bool   // //rans:idempotent 注解：代理默认追加 rans.Idempotent()
	Fields     []requestField
	HasRet     bool   // (T, error) 形态
	RetType    string // T 的类型串
	OneWay     bool   // 无返回形态
	Dispatch   string // wrapper Handle 的 case 体
	Proxy      string // 代理函数整段
}

// requestField 请求载荷字段（位置编码：Order=参数序，从 1 起）。
type requestField struct {
	Name  string
	Type  string
	Order int
	Tag   string // 完整 rans tag 字面量（含反引号）
}

// lifecycleShape 生命周期方法要求形态：单参 context.Context、单返回 error。
func lifecycleShape(m MethodDecl) bool {
	return len(m.Args) == 1 && m.Args[0].Type == "context.Context" &&
		len(m.Returns) == 1 && m.Returns[0].Type == "error"
}

// rpcFaceMethod 判定方法是否入 Messager 面（规格 §6：面是显式接口，Go 等
// 价物 = 导出方法）：须导出且非 wrapper 自带的生命周期名（Activate/
// Deactivate 走委托、Handle 由生成分发提供）。未导出方法 = 实体内部辅助，
// 不产请求类型/注册/代理/分发 case。
func rpcFaceMethod(m MethodDecl) bool {
	switch m.Name {
	case "Activate", "Deactivate", "Handle":
		return false
	}
	r := []rune(m.Name)
	return len(r) > 0 && unicode.IsUpper(r[0])
}

// typeList 拼接类型串（门卫错误文本用）。
func typeList(fs []FieldDecl) string {
	ts := make([]string, len(fs))
	for i, f := range fs {
		ts[i] = f.Type
	}
	return strings.Join(ts, ", ")
}

// capitalize 首字符大写（派生请求字段名：参数名 → 导出风格字段名）。
func capitalize(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	r[0] = unicode.ToUpper(r[0])
	return string(r)
}

// orderTagLit 产 rans:"order=N" 字面量（含反引号，模板直插）。
func orderTagLit(n int) string {
	return "`" + `rans:"order=` + strconv.Itoa(n) + `"` + "`"
}

// planEntities E316 双射校验 + 生命周期/方法面门卫 + 请求类型解析，产出
// 实体面模板数据与合并后的消息清单（用户消息 + 派生/改名请求）。
// msgIn 为用户消息清单的副本基线（不就地改 pkg）。
func planEntities(pkg *PackageDecl) ([]entityModel, []MessageDecl, error) {
	byKind := map[string]string{}
	byType := map[string]string{}
	for _, e := range pkg.Entities {
		if prev, dup := byKind[e.KindName]; dup {
			return nil, nil, fmt.Errorf("ransgen: duplicate kind %q on entities %s and %s [E316]",
				e.KindName, prev, e.TypeName)
		}
		byKind[e.KindName] = e.TypeName
		if prev, dup := byType[e.TypeName]; dup {
			return nil, nil, fmt.Errorf("ransgen: entity type %s annotated with kinds %q and %q [E316]",
				e.TypeName, prev, e.KindName)
		}
		byType[e.TypeName] = e.KindName
	}

	msgs := append([]MessageDecl(nil), pkg.Messages...)

	// 可作返回类型的类型集（M405）：标量全集 + 已声明消息 + 全部派生请求
	// 类型名（它们同样经 RegisterValue 注册）。预扫描与实体遍历序无关。
	retTypes := make(map[string]bool, len(pkg.Messages))
	for _, m := range pkg.Messages {
		retTypes[m.TypeName] = true
	}
	// 派生请求类型名归属门卫：ReqType 须唯一归属一个（实体,方法）。跨对
	// 拼接碰撞（A.BCd 与 AB.Cd 同派 ABCdRequest）与双实体采纳同一用户类型
	// 都在此拦截——生成期报出类型名与两个来源。
	reqOwners := make(map[string]string)
	for _, e := range pkg.Entities {
		for _, m := range e.Methods {
			if !rpcFaceMethod(m) {
				continue
			}
			derived := e.TypeName + m.Name + "Request"
			retTypes[derived] = true
			if prev, dup := reqOwners[derived]; dup {
				return nil, nil, fmt.Errorf(
					"ransgen: derived request type %s collides across %s and %s.%s — rename one of the methods or entities",
					derived, prev, e.TypeName, m.Name)
			}
			reqOwners[derived] = e.TypeName + "." + m.Name
		}
	}

	ents := make([]entityModel, 0, len(pkg.Entities))
	for _, e := range pkg.Entities {
		em := entityModel{
			Kind:     e.KindName,
			TypeName: e.TypeName,
			Pawnite:  e.TypeName + "Pawnite",
		}
		hasCapture, hasRestore := false, false
		for _, m := range e.Methods {
			switch m.Name {
			case "Activate", "Deactivate":
				// 幂等注解门卫：生命周期方法走 wrapper 委托，无 RPC 面可标
				// 幂等——标注即笔误，生成期拦截（静默忽略会掩盖之）。
				if m.Idempotent {
					return nil, nil, fmt.Errorf(
						"ransgen: entity %s: rans:idempotent on lifecycle method %s (annotate RPC methods only)",
						e.TypeName, m.Name)
				}
				// 形态门卫：wrapper 的委托调用必须与用户签名对得上。
				if !lifecycleShape(m) {
					return nil, nil, fmt.Errorf(
						"ransgen: entity %s: lifecycle method %s must be func(context.Context) error, got (%s) (%s)",
						e.TypeName, m.Name, typeList(m.Args), typeList(m.Returns))
				}
				if m.Name == "Activate" {
					em.HasActivate = true
				} else {
					em.HasDeactivate = true
				}
				continue
			case "Snapshot", "Restore":
				// 快照能力面（rans.Snapshotter 透传）：
				// 不入 RPC 面（零参/[]byte 形态本就过不了 ctx 首参门卫），
				// 形态门卫保证生成体的透传调用与用户签名对得上。
				if m.Idempotent {
					return nil, nil, fmt.Errorf(
						"ransgen: entity %s: rans:idempotent on snapshot face method %s (annotate RPC methods only)",
						e.TypeName, m.Name)
				}
				if !snapshotFaceShape(m) {
					return nil, nil, fmt.Errorf(
						"ransgen: entity %s: snapshot face method %s has wrong shape (%s) (%s) — want Snapshot() ([]byte, error) and Restore([]byte) error",
						e.TypeName, m.Name, typeList(m.Args), typeList(m.Returns))
				}
				if m.Name == "Snapshot" {
					hasCapture = true
				} else {
					hasRestore = true
				}
				continue
			}
			if !rpcFaceMethod(m) {
				// 幂等注解门卫：未导出方法/Handle 不入 RPC 面，注解无处
				// 生效——同样生成期拦截。
				if m.Idempotent {
					return nil, nil, fmt.Errorf(
						"ransgen: entity %s: rans:idempotent on non-RPC method %s (annotate exported RPC methods only)",
						e.TypeName, m.Name)
				}
				// Handle（wrapper 自带分发）与未导出方法：不入 RPC 面。
				continue
			}

			rpc, m2, err := planRPCMethod(e, m, pkg, msgs, retTypes)
			if err != nil {
				return nil, nil, err
			}
			msgs = m2
			em.Methods = append(em.Methods, rpc)
		}
		// 成对门卫：rans.Snapshotter 两个方法须成对实现——只实现其一时
		// wrapper 无论生成与否都无法满足接口，静默忽略会掩盖意图（生成
		// 半边透传的死代码 / 用户以为有快照能力实际没有）。
		if hasCapture != hasRestore {
			return nil, nil, fmt.Errorf(
				"ransgen: entity %s: snapshot face must be implemented as a pair — Snapshot and Restore (found just one); implement both or neither",
				e.TypeName)
		}
		em.HasSnapshot = hasCapture
		ents = append(ents, em)
	}
	return ents, msgs, nil
}

// snapshotFaceShape 快照能力面的要求形态（rans.Snapshotter 契约）：
// Snapshot() ([]byte, error) 与 Restore([]byte)
// error。
func snapshotFaceShape(m MethodDecl) bool {
	switch m.Name {
	case "Snapshot":
		return len(m.Args) == 0 && len(m.Returns) == 2 &&
			m.Returns[0].Type == "[]byte" && m.Returns[1].Type == "error"
	case "Restore":
		return len(m.Args) == 1 && m.Args[0].Type == "[]byte" &&
			len(m.Returns) == 1 && m.Returns[0].Type == "error"
	}
	return false
}

// planRPCMethod 计划单方法面：参数/返回门卫（M402/M405、ctx 首参、S004
// 复用在消息规划层）→ 请求类型解析（派生或沿用用户手写）→ 分发体与代理
// 函数。retTypes 为可作返回类型的类型集（planEntities 预扫描）。返回更新
// 后的消息清单。
func planRPCMethod(e EntityDecl, m MethodDecl, pkg *PackageDecl, msgs []MessageDecl, retTypes map[string]bool) (rpcModel, []MessageDecl, error) {
	rpc := rpcModel{
		Name:       m.Name,
		ReqType:    e.TypeName + m.Name + "Request",
		WireName:   e.KindName + "." + m.Name,
		Idempotent: m.Idempotent,
	}

	// 参数形态门卫：首参必须是 ctx context.Context，其余为载荷参数。
	if len(m.Args) == 0 || m.Args[0].Type != "context.Context" {
		return rpc, msgs, fmt.Errorf(
			"ransgen: entity %s: method %s: first argument must be context.Context, got (%s)",
			e.TypeName, m.Name, typeList(m.Args))
	}

	// M402 返回形态门卫：支持 (T, error) / (error) / 无返回（OneWay）。
	switch {
	case len(m.Returns) == 0:
		rpc.OneWay = true
	case len(m.Returns) == 1 && m.Returns[0].Type == "error":
	case len(m.Returns) == 2 && m.Returns[1].Type == "error":
		rpc.HasRet, rpc.RetType = true, m.Returns[0].Type
	default:
		return rpc, msgs, fmt.Errorf(
			"ransgen: entity %s: method %s: unsupported return shape (%s) [M402] (supported: (T, error), (error), none)",
			e.TypeName, m.Name, typeList(m.Returns))
	}

	// M405 返回类型门卫：载荷须可编码（标量全集或已声明消息——均经
	// RegisterValue 注册，EncodeValue/DecodeValue 与代理断言对称）。指针、
	// 未声明 struct、接口、未注册切片会落 JSON 兜底或断言失配——生成期
	// 拦截。仅在包外运行时注册的类型生成期不可知，不在此放行。
	if _, scalar := leafKinds[rpc.RetType]; rpc.HasRet && !scalar && !retTypes[rpc.RetType] {
		return rpc, msgs, fmt.Errorf(
			"ransgen: entity %s: method %s: unsupported return type %q [M405] "+
				"(supported: scalars int, int64, uint64, string, bool, float64, []byte and declared message types; "+
				"declare a message type or use a supported scalar; types registered only at runtime "+
				"outside this package are not knowable at generation time)",
			e.TypeName, m.Name, rpc.RetType)
	}

	args := m.Args[1:]
	// 字段名合成唯一源 requestFieldNames（解析层参数名门卫同一函数，两处
	// 规则不得漂移）；参数名门卫已在解析层拦掉空白名与归一碰撞。
	names := requestFieldNames(args)
	fields := make([]requestField, len(args))
	for i, a := range args {
		fields[i] = requestField{
			Name:  names[i],
			Type:  a.Type,
			Order: i + 1,
			Tag:   orderTagLit(i + 1),
		}
	}

	// 请求类型发现规则：用户手写同款类型名 → 沿用（不生成声明），注册名
	// 改挂 wire 路由名，字段按 order 对位（用户字段名/类型必须匹配）。
	if pkg.TypeNames[rpc.ReqType] {
		idx := -1
		for j := range msgs {
			if msgs[j].TypeName == rpc.ReqType {
				idx = j
				break
			}
		}
		if idx < 0 {
			return rpc, msgs, fmt.Errorf(
				"ransgen: user request type %s has no rans:\"order=N\" tagged fields (cannot map method %s arguments)",
				rpc.ReqType, m.Name)
		}
		byOrder := map[int]FieldDecl{}
		for _, f := range msgs[idx].Fields {
			byOrder[f.Order] = f
		}
		for i, a := range args {
			f, ok := byOrder[i+1]
			if !ok {
				return rpc, msgs, fmt.Errorf(
					"ransgen: user request type %s: missing field with order=%d for argument %d of method %s",
					rpc.ReqType, i+1, i+1, m.Name)
			}
			if f.Type != a.Type {
				return rpc, msgs, fmt.Errorf(
					"ransgen: user request type %s: field %s type %s does not match argument %d type %s of method %s",
					rpc.ReqType, f.Name, f.Type, i+1, a.Type, m.Name)
			}
			fields[i].Name = f.Name
		}
		// 采纳门卫：用户类型带显式 //rans:name 时不得静默改挂 wire 路由
		// 名——显式名表达独立注册意图，无声覆盖会丢失之。去注解（纯手写
		// 请求形态）或改类型名（独立消息）二选一。
		if msgs[idx].NameExplicit {
			return rpc, msgs, fmt.Errorf(
				"ransgen: user request type %s carries explicit wire name %q; adopting it for %s.%s would silently re-register it under %q — remove the //rans:name annotation or rename the type",
				rpc.ReqType, msgs[idx].Name, e.TypeName, m.Name, rpc.WireName)
		}
		msgs[idx].Name = rpc.WireName
		rpc.UserReq = true
	} else {
		declFields := make([]FieldDecl, len(fields))
		for i, f := range fields {
			declFields[i] = FieldDecl{Name: f.Name, Type: f.Type, Order: f.Order}
		}
		// 派生请求的声明位置 = 方法声明处（跨包 wire 名冲突定位用）。
		msgs = append(msgs, MessageDecl{Name: rpc.WireName, TypeName: rpc.ReqType, Pos: m.Pos, Fields: declFields})
	}
	rpc.Fields = fields

	rpc.Dispatch = dispatchCode(e, m, rpc)
	rpc.Proxy = proxyCode(e, m, rpc)
	return rpc, msgs, nil
}

// dispatchCode 产 wrapper Handle 的 case 分发体（语句段，gofmt 归一缩进）。
func dispatchCode(e EntityDecl, m MethodDecl, rpc rpcModel) string {
	callArgs := make([]string, 0, len(rpc.Fields)+1)
	callArgs = append(callArgs, "ctx")
	for i := range rpc.Fields {
		callArgs = append(callArgs, "r."+rpc.Fields[i].Name)
	}
	call := fmt.Sprintf("w.E.%s(%s)", m.Name, strings.Join(callArgs, ", "))
	switch {
	case rpc.HasRet:
		return lines(
			"v, err := "+call,
			"if err != nil {",
			"return nil, err",
			"}",
			"return v, nil")
	case !rpc.OneWay: // (error) 形态
		return lines(
			"if err := "+call+"; err != nil {",
			"return nil, err",
			"}",
			"return nil, nil")
	default: // OneWay：分发后无应答
		return lines(
			call,
			"return nil, nil")
	}
}

// proxyCode 产客户端方法整段（含注释；请求-响应走 Client.Call + 应答类型
// 断言，OneWay 走 Client.Tell；载荷工厂闭包语义——每次调用构造新请求值）。
// 方法签名统一携带 opts ...rans.CallOption（统一调用面（spec §5.2）：<Entity>Client
// 的 C 字段是 rans.Client，经 node.Client() 或 rans.ClientFrom(ctx) 获得）。
// //rans:idempotent 注解方法（规格 §9.2）：代理内默认追加
// rans.Idempotent()——模糊投递类失败（超时/节点失联）由重试环自动重放，
// 重放安全性由标注者保证；调用方追加的其余选项原样透传。
// OneWay 路径不受注解影响（无应答语义，单向发送不产响应等待）。
func proxyCode(e EntityDecl, m MethodDecl, rpc rpcModel) string {
	params := []string{"ctx context.Context", "id rans.ID"}
	pairs := make([]string, 0, len(rpc.Fields))
	for i := range rpc.Fields {
		params = append(params, rpc.Fields[i].Name+" "+rpc.Fields[i].Type)
		pairs = append(pairs, rpc.Fields[i].Name+": "+rpc.Fields[i].Name)
	}
	params = append(params, "opts ...rans.CallOption")
	reqLit := fmt.Sprintf("%s{%s}", rpc.ReqType, strings.Join(pairs, ", "))
	recv := "func (p " + e.TypeName + "Client) " + m.Name + "(" + strings.Join(params, ", ") + ")"

	// 请求-响应的调用语句：注解方法默认追加 rans.Idempotent()，未注解只透
	// 传调用方选项。
	invoke := "resp, err := p.C.Call(ctx, id, " + reqLit + ", opts...)"
	if rpc.Idempotent {
		invoke = "resp, err := p.C.Call(ctx, id, " + reqLit + ", append(opts, rans.Idempotent())...)"
	}

	switch {
	case rpc.HasRet:
		return lines(
			"// "+m.Name+" 请求-响应调用（wire 路由名 "+rpc.WireName+
				idemNote(rpc)+"）。",
			recv+" ("+rpc.RetType+", error) {",
			"var zero "+rpc.RetType,
			invoke,
			"if err != nil {",
			"return zero, err",
			"}",
			"v, ok := resp.("+rpc.RetType+")",
			"if !ok {",
			"return zero, fmt.Errorf(\"rans: "+rpc.WireName+": unexpected response %T\", resp)",
			"}",
			"return v, nil",
			"}")
	case !rpc.OneWay: // (error) 形态
		return lines(
			"// "+m.Name+" 请求-响应调用（wire 路由名 "+rpc.WireName+
				idemNote(rpc)+"）。",
			recv+" error {",
			invoke,
			"if err != nil {",
			"return err",
			"}",
			"if resp != nil {",
			"return fmt.Errorf(\"rans: "+rpc.WireName+": unexpected response %T\", resp)",
			"}",
			"return nil",
			"}")
	default: // OneWay
		return lines(
			"// "+m.Name+" 单向发送（OneWay：入箱即返回，无应答无回执）。",
			recv+" error {",
			"return p.C.Tell(ctx, id, "+reqLit+", opts...)",
			"}")
	}
}

// idemNote 幂等注解方法的注释补记（未注解返回空串）。
func idemNote(rpc rpcModel) string {
	if !rpc.Idempotent {
		return ""
	}
	return "；//rans:idempotent——代理默认追加 rans.Idempotent()，模糊投递失败自动重放"
}
