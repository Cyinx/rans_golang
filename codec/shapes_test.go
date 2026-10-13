package codec

import "testing"

// resetShapes 测试内清空全局形状登记（白盒：RegisterShapes 是 init 期全局
// 登记面，测试要观察各集合的摘要需可隔离）。
func resetShapes() {
	shapesMu.Lock()
	shapes = map[string]struct{}{}
	shapesMu.Unlock()
}

func TestSchemaDigestOrderInsensitive(t *testing.T) {
	defer resetShapes()
	resetShapes()
	RegisterShapes([]string{"b:2:1:int", "a:1:1:string"})
	d1 := SchemaDigest()
	resetShapes()
	RegisterShapes([]string{"a:1:1:string", "b:2:1:int"})
	d2 := SchemaDigest()
	if d1 != d2 {
		t.Fatalf("declaration order must not change digest: %x != %x", d1, d2)
	}
}

func TestSchemaDigestChangesWithShapes(t *testing.T) {
	defer resetShapes()
	resetShapes()
	RegisterShapes([]string{"a:1:1:string"})
	d1 := SchemaDigest()
	RegisterShapes([]string{"b:2:1:int"})
	d2 := SchemaDigest()
	if d1 == d2 {
		t.Fatal("adding a shape must change digest")
	}
	// 重复登记同形状：去重，摘要不变。
	RegisterShapes([]string{"a:1:1:string", "b:2:1:int"})
	if SchemaDigest() != d2 {
		t.Fatal("duplicate registration must be idempotent")
	}
}

func TestSchemaDigestEmptyStable(t *testing.T) {
	defer resetShapes()
	resetShapes()
	d1 := SchemaDigest()
	d2 := SchemaDigest()
	if d1 != d2 {
		t.Fatalf("empty-set digest must be stable: %x != %x", d1, d2)
	}
	RegisterShapes([]string{"a:1:1:string"})
	if SchemaDigest() == d1 {
		t.Fatal("empty set and singleton set must differ")
	}
}
