package testkit

// HR4 验收落点（H12 决策 14/15，票 10）：因果路径 v2 与准入判环。
// EX2 原始形态（A→B→A 与自调用卡到超时）改写为 <10ms 以
// CodeError+Reason=Cycle 快失败（消息未执行、不可重试、文本带完整环）；
// Tell 是因果链边界（非等待边：不追加、不携带调用方路径——否则「A 调 B、
// B 对 A 所等的 C Tell、C 再调 A」会被误判成环）；深度上限 64 降级直通 +
// rans.call.causal_capped 告警计数。

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/codec"
)

// hopCmd 过线序列化（测试编排面）：两根 varint（Depth、UseTell 位）。
func init() {
	codec.Register("testkit.hopCmd", hopCmd{}, codec.Serializer{
		Encode: func(v any) ([]byte, error) {
			c := v.(hopCmd)
			w := codec.NewWriter()
			tell := int64(0)
			if c.UseTell {
				tell = 1
			}
			w.WriteVarInt(int64(c.Depth))
			w.WriteVarInt(tell)
			return w.Bytes(), nil
		},
		Decode: func(b []byte) (any, error) {
			r := codec.NewReader(b)
			depth, err := r.ReadVarInt()
			if err != nil {
				return nil, err
			}
			tell, err := r.ReadVarInt()
			if err != nil {
				return nil, err
			}
			return hopCmd{Depth: int(depth), UseTell: tell != 0}, nil
		},
	})
}

// hopOutcome 内层调用失败的观测：err 供 (Code, Reason) 与环文本断言，
// elapsed 是 <10ms 快失败的锚。
type hopOutcome struct {
	err     error
	elapsed time.Duration
}

// hopEntity 接力实体（kind "hopper"）：peer 为工厂按身份布线的固定对端
// （环拓扑互指、自环指自身、深链指后继）；Handle 依令向 peer 转发同型命令
// （Depth 递减，≤0 即终点）。Call 腿失败时把（错误, 耗时）非阻塞投给共享
// 观测通道（缓冲 + default 丢弃——测试只取首个失败，绝不阻塞实体 loop）；
// Tell 腿不观测失败（单向无回执，送达经终点计数断言）。
type hopEntity struct {
	peer rans.ID
	out  chan<- hopOutcome
	done chan<- struct{}
}

func (h *hopEntity) Activate(context.Context) error   { return nil }
func (h *hopEntity) Deactivate(context.Context) error { return nil }

// hopCmd 接力命令：Depth ≤0 为终点；UseTell 切换单向腿（Tell 不追加不
// 携带的边界观测）。
type hopCmd struct {
	Depth   int
	UseTell bool
}

func (h *hopEntity) Handle(ctx context.Context, req any) (any, error) {
	cmd, _ := req.(hopCmd)
	if cmd.Depth <= 0 {
		if h.done != nil {
			select {
			case h.done <- struct{}{}:
			default:
			}
		}
		return "done", nil
	}
	next := hopCmd{Depth: cmd.Depth - 1, UseTell: cmd.UseTell}
	if cmd.UseTell {
		return nil, rans.ClientFrom(ctx).Tell(ctx, h.peer, next)
	}
	start := time.Now()
	_, err := rans.ClientFrom(ctx).Call(ctx, h.peer, next)
	if err != nil {
		select {
		case h.out <- hopOutcome{err: err, elapsed: time.Since(start)}:
		default:
		}
	}
	return "done", err
}

// hopKinds 单 kind 工厂：peers 按 key 布线对端，out/done 为共享观测通道。
func hopKinds(peers map[string]rans.ID, out chan hopOutcome, done chan struct{}) map[string]rans.Factory {
	return map[string]rans.Factory{
		"hopper": func(id rans.ID) rans.Entity {
			return &hopEntity{peer: peers[id.Key], out: out, done: done}
		},
	}
}

// relayEntity 对端回执实体（kind "relay"）：非 "told" 请求按 tell 开关
// Call/Tell 对端；"told" 请求 = Tell 送达终点（told 计数 +1）。
type relayEntity struct {
	peer rans.ID
	tell bool
	told chan<- struct{}
}

func (e *relayEntity) Activate(context.Context) error   { return nil }
func (e *relayEntity) Deactivate(context.Context) error { return nil }

func (e *relayEntity) Handle(ctx context.Context, req any) (any, error) {
	if s, ok := req.(string); ok && s == "told" {
		select {
		case e.told <- struct{}{}:
		default:
		}
		return "ack", nil
	}
	if e.tell {
		return nil, rans.ClientFrom(ctx).Tell(ctx, e.peer, "told")
	}
	return rans.ClientFrom(ctx).Call(ctx, e.peer, "ping")
}

// TestHR4_EX2_CycleFastFail A→B→A（跨节点远端腿）与自调用（本地腿）都在
// 准入边界立即以 CodeError+Reason=Cycle 回绝：<10ms（旧形态卡到调用方
// 超时）、errors.Is/ReasonOf 可判、文本带完整环（含被环上的 A 自身哈希）。
// B 的内层调用错误直捕自 A 的准入回绝（外层经 Handle 错误折叠，Reason 归
// Handler——折叠面是既有契约，本票不断言外层 Reason）。
func TestHR4_EX2_CycleFastFail(t *testing.T) {
	out := make(chan hopOutcome, 8)
	done := make(chan struct{}, 8)
	a := mustID(t, "hopper", "a")
	b := mustID(t, "hopper", "b")
	a2 := mustID(t, "hopper", "a2")
	c := NewCluster(t, 2, hopKinds(map[string]rans.ID{
		"a": b, "b": a, "a2": a2, // a↔b 互指成环；a2 自指
		// warm 仅作连接预热的中性终点（Depth 0 命令即返回）。
	}, out, done))
	ctx := context.Background()

	// 双向连接预热 + B 预激活在节点 2：让 A→B 与 B→A 都是暖连接的远端腿，
	// 排除首拨号/握手成本混进 <10ms 锚（预热命令 Depth 0，不触发接力）。
	warm := mustID(t, "hopper", "warm")
	if _, err := c.Node(0).Call(ctx, warm, hopCmd{}); err != nil {
		t.Fatalf("warm node1: %v", err)
	}
	if _, err := c.Node(1).Call(ctx, warm, hopCmd{}); err != nil {
		t.Fatalf("warm node2→node1: %v", err)
	}
	if _, err := c.Node(1).Call(ctx, b, hopCmd{}); err != nil {
		t.Fatalf("pre-activate B on node 2: %v", err)
	}
	if _, err := c.Node(0).Call(ctx, b, hopCmd{}); err != nil {
		t.Fatalf("warm node1→node2: %v", err)
	}

	// 远端环：A（节点 1）→B（节点 2）→A。B 的内层 Call 在 A 的准入边界
	// 被 Cycle 回绝；外层调用同样以错误收场（不再卡到超时）。
	if _, err := c.Node(0).Call(ctx, a, hopCmd{Depth: 2}); err == nil {
		t.Fatal("A→B→A must fail, got nil error")
	}
	// 观测通道恰收两条：B 的内层直捕（准入回绝原样）+ A 的外层直捕（B 的
	// Handle 错误折叠形态——Handle 错误边界把 Reason 归 Handler，环文本
	// 随 Message 保留，既有折叠契约）。FIFO 定序：Cycle 先落。
	select {
	case o := <-out:
		if rans.CodeOf(o.err) != rans.CodeError || rans.ReasonOf(o.err) != rans.ReasonCycle {
			t.Fatalf("inner call error = %v, want CodeError+ReasonCycle", o.err)
		}
		if !errors.Is(o.err, &rans.Error{Code: rans.CodeError, Reason: rans.ReasonCycle}) {
			t.Fatal("errors.Is must match the Cycle form")
		}
		if !strings.Contains(o.err.Error(), hexHash(a)) {
			t.Fatalf("cycle text must carry the full ring (hash of A %s): %v", hexHash(a), o.err)
		}
		if o.elapsed > 10*time.Millisecond {
			t.Fatalf("cycle must fast-fail in <10ms, took %v", o.elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no inner outcome captured for A→B→A")
	}
	select {
	case o := <-out:
		if rans.ReasonOf(o.err) != rans.ReasonHandler || !strings.Contains(o.err.Error(), hexHash(a)) {
			t.Fatalf("outer fold = %v, want ReasonHandler with the ring text preserved", o.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no outer outcome captured for A→B→A")
	}

	// 本地自环：A 的 Handle 调 A——本地腿同一准入点、同一错误形态（恰一
	// 条观测：内层直捕 Cycle）。
	if _, err := c.Node(0).Call(ctx, a2, hopCmd{Depth: 1}); err == nil {
		t.Fatal("A→A self call must fail, got nil error")
	}
	select {
	case o := <-out:
		if rans.CodeOf(o.err) != rans.CodeError || rans.ReasonOf(o.err) != rans.ReasonCycle {
			t.Fatalf("self-call error = %v, want CodeError+ReasonCycle", o.err)
		}
		if o.elapsed > 10*time.Millisecond {
			t.Fatalf("self-cycle must fast-fail in <10ms, took %v", o.elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no inner outcome captured for A→A")
	}
}

// TestCausal_TellNotAwaitEdge Tell 非等待边不入链：等待链上的实体对链上
// 对端 Tell——不追加不携带调用方路径（否则对端哈希已在路径中，Tell 在其
// 准入被判环拒绝、调用方拿到 ErrBusy）、不判环、正常送达（"told" 终点
// 计数）；对自身 Tell 同理。
func TestCausal_TellNotAwaitEdge(t *testing.T) {
	told := make(chan struct{}, 4)
	ab := mustID(t, "relay", "a")
	bb := mustID(t, "relay", "b")
	cb := mustID(t, "relay", "c")
	c := NewCluster(t, 1, map[string]rans.Factory{
		"relay": func(id rans.ID) rans.Entity {
			var peer rans.ID
			tell := false
			switch id.Key {
			case "a":
				peer, tell = bb, true // A 的 Handle 对 B（等待链上的对端）Tell
			case "b":
				peer = ab // B 的 Handle 同步 Call A（构造等待边）
			case "c":
				peer, tell = cb, true // 自身 Tell
			}
			return &relayEntity{peer: peer, tell: tell, told: told}
		},
	})
	ctx := context.Background()

	// 等待链 + 链上 Tell：top→B→(Call)→A→(Tell)→B。B 忙于等 A 的应答时
	// "told" 在箱排队，B 的 Handle 返回后送达——全程零判环、零回绝。
	if _, err := c.Node(0).Call(ctx, bb, "go"); err != nil {
		t.Fatalf("await chain with ring Tell must complete, got %v", err)
	}
	select {
	case <-told:
	case <-time.After(5 * time.Second):
		t.Fatal("told message was not delivered (Tell must not be cycle-checked)")
	}

	// 自身 Tell：C 的 Handle 对 C Tell——不入链、正常送达。
	if _, err := c.Node(0).Call(ctx, cb, "go"); err != nil {
		t.Fatalf("self Tell must complete, got %v", err)
	}
	select {
	case <-told:
	case <-time.After(5 * time.Second):
		t.Fatal("self-told message was not delivered")
	}
}

// TestCausal_DepthCapPassthrough 深度上限 64：>64 跳的接力链降级直通
// （超限后不再追加，深链不误判成环）+ rans.call.causal_capped 告警计数。
func TestCausal_DepthCapPassthrough(t *testing.T) {
	const chainLen = 70 // > MaxCausalDepth(64)：h64 起的出站追加被跳过
	out := make(chan hopOutcome, 8)
	done := make(chan struct{}, 8)
	ids := make([]rans.ID, chainLen+1)
	for i := range ids {
		ids[i] = mustID(t, "hopper", "h"+strconv.Itoa(i))
	}
	peers := make(map[string]rans.ID, chainLen)
	for i := 0; i < chainLen; i++ {
		peers[ids[i].Key] = ids[i+1] // 先全量落 id 再布线（防零值对端）
	}
	c := NewCluster(t, 1, hopKinds(peers, out, done))
	ctx := context.Background()

	v, err := c.Node(0).Call(ctx, ids[0], hopCmd{Depth: chainLen})
	if err != nil {
		t.Fatalf("deep chain must pass through the depth cap, got %v", err)
	}
	if v != "done" {
		t.Fatalf("deep chain returned %v, want done", v)
	}
	select { // 全链无一次环误判
	case o := <-out:
		t.Fatalf("deep chain must not be rejected as cycle: %v", o.err)
	default:
	}
	pollNodeMetric(t, c, 0, "rans.call.causal_capped", 1, 2*time.Second)
}

// hexHash 测试侧的哈希展示形态（与环回绝文本同款 16 位 hex）。
func hexHash(id rans.ID) string { return fmt.Sprintf("%016x", id.Hash()) }
