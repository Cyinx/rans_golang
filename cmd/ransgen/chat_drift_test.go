// chat_drift_lock 把 examples/chat 钉在生成器当前行为上：
// 只读跑一遍全生成管线（LoadPackage 排除 rans_gen.go + planGen + Render），
// 断言产物与已提交的 examples/chat/rans_gen.go 逐字节一致——examples 与
// 生成器之间的漂移在 go test 即刻暴露，不再依赖手工 go generate 验收。
// 附带收益：chat 包的未导出消息类型（chatMsg）由此进生成覆盖面。
// 用只读管线而非 run()：漂移时不改写入库文件，测试保持无副作用。
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestChatDriftLock examples/chat 再生成与入库生成物逐字节一致。
func TestChatDriftLock(t *testing.T) {
	chatDir := filepath.Join("..", "..", "examples", "chat")
	pkg, err := LoadPackage(chatDir, "rans_gen.go")
	if err != nil {
		t.Fatalf("LoadPackage: %v", err)
	}
	fresh := renderPlan(t, pkg)

	committed, err := os.ReadFile(filepath.Join(chatDir, "rans_gen.go"))
	if err != nil {
		t.Fatalf("读入库生成物: %v", err)
	}
	if !bytes.Equal(fresh, committed) {
		t.Fatalf("examples/chat/rans_gen.go 与再生成不一致（生成器改动后需 go generate ./examples/chat 重新入库）\n--- got ---\n%s\n--- want ---\n%s", fresh, committed)
	}

	// 覆盖面锚点：chat 的未导出消息类型确实进了生成面（防 drift 锁空转）。
	if !bytes.Contains(fresh, []byte(`codec.Register("chatMsg"`)) {
		t.Fatalf("生成物缺未导出消息 chatMsg 的注册:\n%s", fresh)
	}
}
