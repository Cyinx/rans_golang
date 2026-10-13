// genfixture_drift_lock 把 testkit/genfixture 钉在生成器当前行为上（与
// chat_drift_lock 同款机制）：只读跑一遍全生成管线（LoadPackage 排除
// rans_gen.go + planGen + Render），断言产物与已提交的
// testkit/genfixture/rans_gen.go 逐字节一致——生成器改动未重新入库即在
// go test 暴露。覆盖面锚点双断言：快照透传（迁移腿的随行状态载体）与
// 序列线型字段（[]string 的 WireSequence 编解码——真 wire 首跑路径）确实
// 进了生成面，防漂移锁空转。
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestGenFixtureDriftLock testkit/genfixture 再生成与入库生成物逐字节一致。
func TestGenFixtureDriftLock(t *testing.T) {
	fixtureDir := filepath.Join("..", "..", "testkit", "genfixture")
	pkg, err := LoadPackage(fixtureDir, "rans_gen.go")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	fresh := renderPlan(t, pkg)

	committed, err := os.ReadFile(filepath.Join(fixtureDir, "rans_gen.go"))
	if err != nil {
		t.Fatalf("读入库生成物: %v", err)
	}
	if !bytes.Equal(fresh, committed) {
		t.Fatalf("testkit/genfixture/rans_gen.go 与再生成不一致（生成器改动后需 go generate ./testkit/genfixture 重新入库）\n--- got ---\n%s\n--- want ---\n%s", fresh, committed)
	}

	// 覆盖面锚点：冒烟套件依赖的生成形态确实在产物里（防 drift 锁空转）。
	for _, want := range []string{
		"func (w *NotebookPawnite) Snapshot() ([]byte, error) {",
		"codec.WireSequence",
	} {
		if !bytes.Contains(fresh, []byte(want)) {
			t.Fatalf("生成物缺冒烟套件依赖的形态 %q:\n%s", want, fresh)
		}
	}
}
