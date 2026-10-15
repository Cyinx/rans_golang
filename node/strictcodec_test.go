package node

// 票 10 红测：opt-in 本地严格编解码（node.Config.Calls.StrictLocalCodec）——
// 开启后本地 Call/Tell 对请求与应答各做一次 codec 往返：未注册类型本地即
// 失败（与远端同构错误）、引用别名被切断；关闭时行为不变（引用直通）。

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
)

// strictAliasMap 已注册的可变类型：JSON 往返天然产生新副本（别名切断的
// 判据载体）。
var _ = func() bool {
	codec.Register("node.strictalias", map[string]int{}, codec.Serializer{
		Encode: func(v any) ([]byte, error) { return json.Marshal(v) },
		Decode: func(b []byte) (any, error) {
			m := map[string]int{}
			return m, json.Unmarshal(b, &m)
		},
	})
	return true
}()

// unregReq 刻意不注册的类型（未注册面判据）。
type unregReq struct{ N int }

// strictCodecEntity 探针实体：echo 形态回显请求并记执行。
type strictCodecEntity struct {
	m        map[string]int
	executed chan string
}

func (e *strictCodecEntity) Activate(context.Context) error   { return nil }
func (e *strictCodecEntity) Deactivate(context.Context) error { return nil }

func (e *strictCodecEntity) Handle(ctx context.Context, req any) (any, error) {
	switch v := req.(type) {
	case string:
		switch v {
		case "store":
			return e.m, nil
		case "get":
			return e.m, nil
		}
	case map[string]int:
		e.m = v
		return e.m, nil
	}
	select {
	case e.executed <- fmt.Sprintf("%T", req):
	default:
	}
	return req, nil
}

func strictCodecKinds() (map[string]rans.Factory, chan string) {
	executed := make(chan string, 8)
	return map[string]rans.Factory{
		"strictprobe": func(rans.ID) rans.Entity {
			return &strictCodecEntity{executed: executed}
		},
	}, executed
}

// TestNodeStrictLocalCodec_UnregisteredTypeFailsLocally 开启后本地调用传未
// 注册类型即失败（与远端同构的「rans: encode: request value」形态），实体
// 零执行；关闭时引用直通照常成功。
func TestNodeStrictLocalCodec_UnregisteredTypeFailsLocally(t *testing.T) {
	kinds, executed := strictCodecKinds()
	n := mustStartNode(t, Config{
		NodeID: 1, Standalone: true, Kinds: kinds,
		Calls: CallConfig{Default: 3 * time.Second, StrictLocalCodec: true},
	})
	defer shutdownNode(n)
	id := mustID(t, "strictprobe", "u1")
	if _, err := n.Client().Call(context.Background(), id, unregReq{N: 1}); err == nil ||
		!strings.Contains(err.Error(), "rans: encode: request value") {
		t.Fatalf("strict local call err = %v, want rans: encode: request value", err)
	}
	select {
	case s := <-executed:
		t.Fatalf("entity executed unregistered request (%s), want zero execution", s)
	default:
	}
	// 关闭（对照）：引用直通，本地成功（差异只在开启时出现——远端才会失败
	// 的差异面被本地复现的前提是开启）。
	kinds2, _ := strictCodecKinds()
	n2 := mustStartNode(t, Config{
		NodeID: 2, Standalone: true, Kinds: kinds2,
		Calls: CallConfig{Default: 3 * time.Second},
	})
	defer shutdownNode(n2)
	id2 := mustID(t, "strictprobe", "u2")
	if v, err := n2.Client().Call(context.Background(), id2, unregReq{N: 1}); err != nil {
		t.Fatalf("non-strict local call must pass by reference: %v", err)
	} else if _, ok := v.(unregReq); !ok {
		t.Fatalf("non-strict echo type = %T, want unregReq", v)
	}
}

// TestNodeStrictLocalCodec_NoAliasing 开启后调用方改写返回值不影响实体内部
// 状态（应答经 codec 往返，调用方拿到的是副本）；实体回存请求同理（请求
// 亦往返，实体持有的是调用方传入的副本）。
func TestNodeStrictLocalCodec_NoAliasing(t *testing.T) {
	kinds, _ := strictCodecKinds()
	n := mustStartNode(t, Config{
		NodeID: 1, Standalone: true, Kinds: kinds,
		Calls: CallConfig{Default: 3 * time.Second, StrictLocalCodec: true},
	})
	defer shutdownNode(n)
	id := mustID(t, "strictprobe", "a1")
	// 请求往返：实体回存的是副本——调用方随后改写本地 map 不影响实体。
	req := map[string]int{"a": 1}
	if v, err := n.Client().Call(context.Background(), id, req); err != nil {
		t.Fatalf("store call: %v", err)
	} else {
		reply := v.(map[string]int)
		reply["a"] = 99 // 改写应答副本
	}
	req["b"] = 2 // 改写请求副本（投递已完成，但仍验证实体侧无别名）
	v, err := n.Client().Call(context.Background(), id, "get")
	if err != nil {
		t.Fatalf("get call: %v", err)
	}
	got := v.(map[string]int)
	if got["a"] != 1 || len(got) != 1 {
		t.Fatalf("entity state = %v, want map[a:1] (aliasing must be cut off)", got)
	}
}
