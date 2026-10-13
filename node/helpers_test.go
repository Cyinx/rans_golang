package node

// 测试共用助手（装配/收口/身份）。

import (
	"context"
	"testing"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
)

// mustInjected 测试注入目录。
func mustInjected() directory.Directory { return inmem.New() }

// mustStartNode 构造并起跑（失败即 Fatal）。
func mustStartNode(t *testing.T, cfg Config) *Node {
	t.Helper()
	n, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return n
}

// shutdownNode 有界预算的优雅停机（对齐既有收口形态）。
func shutdownNode(n *Node) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = n.Shutdown(ctx)
}

// mustID 身份构造（失败即 Fatal）。
func mustID(t *testing.T, kind, key string) rans.ID {
	t.Helper()
	id, err := rans.NewID(kind, key)
	if err != nil {
		t.Fatalf("NewID: %v", err)
	}
	return id
}
