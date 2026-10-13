package rans_test

import (
	"testing"

	rans "github.com/cyinx/rans_golang"
)

// TestCodeValuesMatchWireSpec 钉死线上数值（移植规格附录 B）：数值是线协议
// 契约，锁死。
func TestCodeValuesMatchWireSpec(t *testing.T) {
	want := map[rans.Code]uint8{
		rans.CodeOK: 0, rans.CodeError: 1, rans.CodeNotFound: 2, rans.CodeTimeout: 3,
		rans.CodeUnavailable: 4, rans.CodeSerialization: 5, rans.CodeDecode: 6,
	}
	for sc, v := range want {
		if uint8(sc) != v {
			t.Errorf("%d != %d", sc, v)
		}
	}
}

// TestLocalOnlyCodeValues：只在本地产生、不过线的码值。
func TestLocalOnlyCodeValues(t *testing.T) {
	if rans.CodeCanceled != 64 || rans.CodeConnectionLost != 65 {
		t.Fatalf("local-only codes = %d/%d, want 64/65", rans.CodeCanceled, rans.CodeConnectionLost)
	}
}
