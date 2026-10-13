package activation

// 创建锁去 IO 契约测试：目录注册 IO（Register 原子域——register-
// first，取号并入注册）不得在全局创建锁 m.mu 的临界区内执行——注册在
// 各 pawnite 的激活闭包首步（run goroutine 上并行），K 个并发激活不再被
// 串行成 K×RTT；注册失败成为异步的激活失败（消息已入箱，经 drainReject
// 回绝，本地调用方拿到 ErrActivationFailed 哨兵后可重试——注册后失败属
// 执行前确定性失败，一律 Unavailable 回绝）。

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// slowRegDir 注册延迟的目录包装：Register 固定睡 delay——单次注册 RTT 的
// 受控模拟。取号并入 Register 原子域：包装层返回的纪元即注册结果的一部分。
type slowRegDir struct {
	directory.Directory // inmem 的全部目录能力（合并端口：注册/注销/查找）
	delay               time.Duration
}

func (s *slowRegDir) Register(ctx context.Context, id rans.ID, self directory.Owner, expected *directory.Owner) (bool, directory.Owner, error) {
	time.Sleep(s.delay)
	return s.Directory.Register(ctx, id, self, expected)
}

// failRegDir 注册门控失败的目录包装：Register 进入即上报并停在 gate 上，
// 放行后返回注入错误——把「投递先入箱、注册后失败」的异步窗钉成确定性
// 次序（规避准入被终局抢占的第三窗，与 registerfirst_test.go 的门控
// Store 同手法）。
type failRegDir struct {
	directory.Directory
	entered chan struct{}
	gate    chan struct{}
	err     error
}

func (f *failRegDir) Register(context.Context, rans.ID, directory.Owner, *directory.Owner) (bool, directory.Owner, error) {
	close(f.entered)
	<-f.gate
	return false, directory.Owner{}, f.err
}

// regBase 返回 inmem 目录基座（Directory 即合并端口，注册面
// 无须能力断言；包装层经嵌入透传）。
func regBase() directory.Directory {
	return inmem.New()
}

// TestGetOrActivate_RegisterIOOutOfCreationLock 20 个并发激活、单次注册
// RTT 20ms → 总耗时必须 < 3×RTT（注册已移出全局创建锁，在各自 pawnite
// goroutine 上并行；串行化即回归）。
func TestGetOrActivate_RegisterIOOutOfCreationLock(t *testing.T) {
	d := &slowRegDir{Directory: regBase(), delay: 20 * time.Millisecond}
	m := New(d, 1, map[string]rans.Factory{"k": func(_ rans.ID) rans.Entity { return nopTestEntity{} }})
	const K = 20
	ctx, cancel := ctxT(10 * time.Second)
	defer cancel()
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < K; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := m.GetOrActivate(ctx, mustTestID(t, "k", strconv.Itoa(i))); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if el := time.Since(start); el >= 60*time.Millisecond {
		t.Fatalf("激活被串行化：%v ≥ 3×RTT（注册仍在全局锁内？）", el)
	}
}

// TestGetOrActivate_RegisterFailureIsAsyncActivationFailure 注册失败不
// 再同步报错阻断 pawnite 构造：GetOrActivate 正常入表返回，消息入箱后被
// drainReject(Unavailable, ReasonActivationFailed) 回绝，本地调用方
// 拿到 ErrActivationFailed+rans.ErrUnavailable 双哨兵（可重试的执行前确定性
// 失败）。注册失败按激活失败计数（reason=register）：本测试未接线任何
// 状态面，计数增量只能来自注册失败路径本身。
func TestGetOrActivate_RegisterFailureIsAsyncActivationFailure(t *testing.T) {
	d := &failRegDir{
		Directory: regBase(),
		entered:   make(chan struct{}),
		gate:      make(chan struct{}),
		err:       errors.New("register: directory down"),
	}
	m := New(d, 1, map[string]rans.Factory{"k": func(_ rans.ID) rans.Entity { return nopTestEntity{} }})
	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()
	before := activationFailedCounter().Value()
	X := mustTestID(t, "k", "x")

	res := make(chan error, 1)
	go func() {
		_, err := m.InvokeLocal(ctx, X, "ping")
		res <- err
	}()
	<-d.entered // 激活停在注册上（实例 Activating、在途）
	waitFor(t, "delivery admitted to inbox", func() bool {
		p, ok := m.GetLocal(X)
		return ok && p.InboxLen() == 1
	})
	close(d.gate) // 放行失败：注册失败 → 在箱投递按 Unavailable 回绝

	var err error
	select {
	case err = <-res:
	case <-time.After(5 * time.Second):
		t.Fatal("Call did not settle after register failure")
	}
	if !errors.Is(err, ErrActivationFailed) {
		t.Fatalf("期望 ErrActivationFailed（异步窗），得到 %v", err)
	}
	if !errors.Is(err, rans.ErrUnavailable) {
		t.Fatalf("期望 rans.ErrUnavailable（注册失败=执行前确定性失败），得到 %v", err)
	}
	if got := activationFailedCounter().Value(); got != before+1 {
		t.Fatalf("rans.activation.failed 计数 %v，want %v（恰好一次）", got, before+1)
	}
	waitTerminalEviction(t, m, X)
}
