package tcp

import (
	"testing"
)

// TestConfigFailClosedHMAC 门闸真值表：仅「未配密钥（Secret nil）且未显式
// NoHMAC」触发 fail-closed；Secret 非 nil 时旗标不参与（MAC 恒启用）。
// 密钥装载助手测试在 node 包（node.LoadHMACKeyFromEnv/File 及其
// fail-closed 链）。
func TestConfigFailClosedHMAC(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want bool
	}{
		{"zero config: no secret, no opt-in", Config{}, true},
		{"explicit NoHMAC opt-in", Config{NoHMAC: true}, false},
		{"secret configured", Config{Secret: []byte("k")}, false},
		{"secret set, NoHMAC irrelevant", Config{Secret: []byte("k"), NoHMAC: true}, false},
		{"empty-but-non-nil secret counts as configured", Config{Secret: []byte{}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.failClosedHMAC(); got != tc.want {
				t.Fatalf("failClosedHMAC()=%v want %v", got, tc.want)
			}
		})
	}
}
