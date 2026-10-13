// proxy_test 锁死生成物行为契约：Client 经内存 fake rans.Client
// 类型化往返、OneWay 走 Tell 分路、//rans:idempotent 默认追加选项、用户
// 手写请求类型共存、wrapper 分发未知请求防线、派生请求的 wire 注册名
// （TypeName 即方法路由）。
package entitydemo

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
)

// fakeClient 内存 rans.Client：记录末次请求与身份、区分 Call/Tell 计数与
// 末次调用选项集，返回预置应答。
type fakeClient struct {
	id     rans.ID
	req    any
	resp   any
	err    error
	called int
	sent   int
	opts   []rans.CallOption
}

func (f *fakeClient) Call(ctx context.Context, id rans.ID, req any, opts ...rans.CallOption) (any, error) {
	f.called++
	f.id, f.req, f.opts = id, req, opts
	return f.resp, f.err
}

func (f *fakeClient) Tell(ctx context.Context, id rans.ID, req any, opts ...rans.CallOption) error {
	f.sent++
	f.id, f.req, f.opts = id, req, opts
	return f.err
}

var (
	ctx = context.Background()
	cid = rans.ID{Kind: "counter", Key: "a"}
)

// TestProxyAddRoundTrip (T, error) 形态：proxy 构造请求 → Call → 应答类型
// 断言解码；请求载荷与身份被原样送达。
func TestProxyAddRoundTrip(t *testing.T) {
	fc := &fakeClient{resp: int64(7)}
	got, err := CounterClient{C: fc}.Add(ctx, cid, 7)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if got != 7 {
		t.Fatalf("got = %d, want 7", got)
	}
	if fc.called != 1 || fc.sent != 0 {
		t.Fatalf("Add 未走 Call 分路: called=%d sent=%d", fc.called, fc.sent)
	}
	if rans.IsIdempotent(fc.opts...) {
		t.Fatal("未注解方法不得默认携带幂等标注")
	}
	if fc.id != cid {
		t.Fatalf("身份未送达: %+v", fc.id)
	}
	if want := (CounterAddRequest{Delta: 7}); !reflect.DeepEqual(fc.req, want) {
		t.Fatalf("req = %+v, want %+v", fc.req, want)
	}
}

// TestProxyAddWrongResponseType 应答类型不符 → 明确报错（不静默零值）。
func TestProxyAddWrongResponseType(t *testing.T) {
	fc := &fakeClient{resp: "oops"}
	_, err := CounterClient{C: fc}.Add(ctx, cid, 1)
	if err == nil || !strings.Contains(err.Error(), "unexpected response") {
		t.Fatalf("err = %v, want 含 unexpected response", err)
	}
}

// TestProxyPutIdempotentAppendsOption //rans:idempotent 注解行为：Put 代理
// 默认追加 rans.Idempotent()（载荷与身份照常送达）；未注解方法（Reset）不
// 携带幂等标注——注解只影响标注方法自身。
func TestProxyPutIdempotentAppendsOption(t *testing.T) {
	fc := &fakeClient{resp: int64(9)}
	got, err := CounterClient{C: fc}.Put(ctx, cid, 9)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got != 9 {
		t.Fatalf("got = %d, want 9", got)
	}
	if fc.called != 1 || fc.sent != 0 {
		t.Fatalf("Put 未走 Call 分路: called=%d sent=%d", fc.called, fc.sent)
	}
	if !rans.IsIdempotent(fc.opts...) {
		t.Fatal("Put 未默认追加 rans.Idempotent() 选项")
	}
	if fc.id != cid {
		t.Fatalf("身份未送达: %+v", fc.id)
	}
	if want := (CounterPutRequest{To: 9}); !reflect.DeepEqual(fc.req, want) {
		t.Fatalf("req = %+v, want %+v", fc.req, want)
	}

	fc.resp, fc.called, fc.opts = nil, 0, nil // (error) 形态应答必须为 nil，否则代理按异常应答报错
	if err := (CounterClient{C: fc}).Reset(ctx, cid, 3); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if fc.called != 1 {
		t.Fatalf("未注解方法应走默认 Call: called=%d", fc.called)
	}
	if rans.IsIdempotent(fc.opts...) {
		t.Fatal("未注解方法不得默认携带幂等标注")
	}
}

// TestProxyResetUserRequestCoexists (error) 形态 + 用户手写请求类型共存：
// 代理按用户字段名构造（order 对位），多余字段保持零值。
func TestProxyResetUserRequestCoexists(t *testing.T) {
	fc := &fakeClient{}
	p := CounterClient{C: fc}
	if err := p.Reset(ctx, cid, 3); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if fc.called != 1 || fc.sent != 0 {
		t.Fatalf("Reset 未走 Call 分路: called=%d sent=%d", fc.called, fc.sent)
	}
	if want := (CounterResetRequest{Val: 3}); !reflect.DeepEqual(fc.req, want) {
		t.Fatalf("req = %+v, want %+v", fc.req, want)
	}
}

// TestProxyPingOneWay OneWay 形态：走 SendOneWay，绝不 Call。
func TestProxyPingOneWay(t *testing.T) {
	fc := &fakeClient{}
	p := CounterClient{C: fc}
	if err := p.Ping(ctx, cid); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if fc.sent != 1 || fc.called != 0 {
		t.Fatalf("Ping 未走 SendOneWay 分路: called=%d sent=%d", fc.called, fc.sent)
	}
	if want := (CounterPingRequest{}); !reflect.DeepEqual(fc.req, want) {
		t.Fatalf("req = %+v, want %+v", fc.req, want)
	}
}

// TestDispatchRoundTrip wrapper 分发：三形态请求类型 switch 到实体方法，
// 实体状态被真实推进。
func TestDispatchRoundTrip(t *testing.T) {
	c := &Counter{}
	w := &CounterPawnite{E: c}

	resp, err := w.Handle(ctx, CounterAddRequest{Delta: 5})
	if err != nil {
		t.Fatalf("Handle Add: %v", err)
	}
	if got, ok := resp.(int64); !ok || got != 5 {
		t.Fatalf("Add resp = %#v, want int64(5)", resp)
	}
	if c.n != 5 {
		t.Fatalf("实体状态未被推进: n=%d", c.n)
	}

	if _, err := w.Handle(ctx, CounterResetRequest{Val: 42, Who: "u"}); err != nil {
		t.Fatalf("Handle Reset: %v", err)
	}
	if c.n != 42 {
		t.Fatalf("Reset 后 n=%d, want 42", c.n)
	}

	if _, err := w.Handle(ctx, CounterPingRequest{}); err != nil {
		t.Fatalf("Handle Ping: %v", err)
	}
	if c.n != 0 {
		t.Fatalf("Ping 后 n=%d, want 0", c.n)
	}
}

// TestDispatchUnknownRequest 未知请求 → "rans: unknown request %T"。
func TestDispatchUnknownRequest(t *testing.T) {
	w := &CounterPawnite{E: &Counter{}}
	_, err := w.Handle(ctx, struct{}{})
	if err == nil || !strings.Contains(err.Error(), "rans: unknown request") {
		t.Fatalf("err = %v, want 含 rans: unknown request", err)
	}
}

// TestRequestWireRegistration 派生请求经序列化器注册（TypeName=<Kind>.<Method>，
// 方法路由），注册表命中非 JSON 兜底名。
func TestRequestWireRegistration(t *testing.T) {
	in := CounterAddRequest{Delta: -9}
	name, payload, err := codec.Encode(in)
	if err != nil {
		t.Fatalf("EncodeValue: %v", err)
	}
	if name != "counter.Add" {
		t.Fatalf("name = %q, want counter.Add", name)
	}
	got, err := codec.Decode(name, payload)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("round-trip 不一致: got %+v want %+v", got, in)
	}

	// 用户手写请求类型同样挂 wire 路由名（发现规则），多余字段一并上线。
	name2, payload2, err := codec.Encode(CounterResetRequest{Val: 4, Who: "w"})
	if err != nil {
		t.Fatalf("EncodeValue: %v", err)
	}
	if name2 != "counter.Reset" {
		t.Fatalf("name = %q, want counter.Reset", name2)
	}
	got2, err := codec.Decode(name2, payload2)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if !reflect.DeepEqual(got2, CounterResetRequest{Val: 4, Who: "w"}) {
		t.Fatalf("round-trip 不一致: got %+v", got2)
	}
}

// TestUnnamedParamRequestField 未命名参数 → ArgN 字段合成：wire 注册往返
// （TypeName=counter.Bump）+ wrapper 分发推进实体状态。
func TestUnnamedParamRequestField(t *testing.T) {
	in := CounterBumpRequest{Arg1: -5}
	name, payload, err := codec.Encode(in)
	if err != nil {
		t.Fatalf("EncodeValue: %v", err)
	}
	if name != "counter.Bump" {
		t.Fatalf("name = %q, want counter.Bump", name)
	}
	got, err := codec.Decode(name, payload)
	if err != nil {
		t.Fatalf("DecodeValue: %v", err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("round-trip 不一致: got %+v want %+v", got, in)
	}

	c := &Counter{}
	w := &CounterPawnite{E: c}
	if _, err := w.Handle(ctx, CounterBumpRequest{Arg1: 3}); err != nil {
		t.Fatalf("Handle Bump: %v", err)
	}
	if c.n != 1 {
		t.Fatalf("Bump 分发未推进实体状态: n=%d", c.n)
	}
}

// TestRansShapesContent 形状清单内容锁（完整指纹形态）：含方法派生
// 请求（counter.Add/Bump/Put）与用户手写请求（CounterResetRequest）；
// 零字段消息（CounterPingRequest）一条仅类型名条目——零字段消息的
// 增删/改名同样翻指纹。
func TestRansShapesContent(t *testing.T) {
	want := []string{
		"CounterAddRequest:Delta:1:int64", // 派生：参数名 delta → Delta
		"CounterBumpRequest:Arg1:1:int64", // 派生：未命名参数 → Arg1
		"CounterPingRequest",              // 零字段派生请求：仅类型名条目
		"CounterPutRequest:To:1:int64",    // 派生：//rans:idempotent 不影响形状
		"CounterResetRequest:Val:1:int64", // 用户手写请求
		"CounterResetRequest:Who:2:string",
	}
	if !reflect.DeepEqual(RansShapes, want) {
		t.Fatalf("RansShapes = %#v, want %#v", RansShapes, want)
	}
}
