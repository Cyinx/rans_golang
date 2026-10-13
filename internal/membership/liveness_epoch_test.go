package membership

// Liveness 节点代际（epoch）面测试（票 14）：启动取号一次、全程携带、
// NodeEpoch() 访问面读数一致。单调性跨重启的注册表侧语义由
// directory/contract 的代际套件锁定（redis 真 Redis + inmem 同型）；本文件
// 只锚存活信号组件自身的取号纪律。测试复用 liveness_test.go 的 fake 与
// 便捷构造（同包）。

import (
	"context"
	"testing"
	"time"
)

// TestLivenessEpochTakenOnceAndCarried 节点代际取号：启动首写前取号一次，
// 全程携带——多次注册携带同一代际且等于 NodeEpoch() 访问面读数；未启动
// （未取号）读 0；取号全程恰一次。
func TestLivenessEpochTakenOnceAndCarried(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := newFakeNodeRegistry(nil)
	svc := newLiveness(t, reg, 5, 20*time.Millisecond)
	if got := svc.NodeEpoch(); got != 0 {
		t.Fatalf("NodeEpoch before Start = %d, want 0 (not yet taken)", got)
	}
	svc.PublishSelf("10.0.0.5:1", nil)
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Close()

	// 启动首写已发生：携带的代际非零且等于访问面读数
	deadline := time.After(100 * time.Millisecond)
	var first uint64
	for {
		if calls := reg.registrations(); len(calls) >= 1 {
			first = calls[0].info.Epoch
			if first == 0 {
				t.Fatal("first registration must carry the taken epoch")
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("RegisterNode not called at Run start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if got := svc.NodeEpoch(); got != first {
		t.Fatalf("NodeEpoch() = %d, want the carried epoch %d", got, first)
	}

	// 多周期后：全部注册携带同一代际（全程携带，不二次取号）
	time.Sleep(120 * time.Millisecond)
	for i, c := range reg.registrations() {
		if c.info.Epoch != first {
			t.Fatalf("registration[%d] carries epoch %d, want the same %d across the whole lifetime", i, c.info.Epoch, first)
		}
	}
	if n := reg.nextEpochCount(); n != 1 {
		t.Fatalf("NextNodeEpoch called %d times, want exactly once per Liveness lifetime", n)
	}
}
