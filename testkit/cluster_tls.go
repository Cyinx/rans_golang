package testkit

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

// ---- 自含测试 PKI（WithTLS）----

// clusterCA 测试集群自签 CA（ECDSA P-256）：签发逐节点服务器证书。生成器
// 自含于本包（不跨包引用其他包的 _test 代码，testkit 保持自包含）。证书
// 全部测试内生成，不依赖外部文件，密钥材料不落盘。
type clusterCA struct {
	cert     *x509.Certificate
	key      *ecdsa.PrivateKey
	rootPool *x509.CertPool // 信任池（拨号侧 RootCAs 共享；构建后只读）
}

// newClusterCA 生成一把自签 CA（serial 须与该 CA 签发的节点证书序列号
// 相异——证书序列号唯一性）。
func newClusterCA(t *testing.T, serial int64) *clusterCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("testkit: generate ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: "testkit-cluster-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("testkit: create ca cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("testkit: parse ca cert: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &clusterCA{cert: cert, key: key, rootPool: pool}
}

// nodeTLSConfig 签发一张节点服务器证书并装配双角色 tls.Config：监听侧
// 持该证书（tls.NewListener 的服务器角色），拨号侧信任本 CA（RootCAs）并
// 以 127.0.0.1 为校验名（节点互联都在回环；证书 SAN 覆盖
// 127.0.0.1/::1/localhost）。
func (ca *clusterCA) nodeTLSConfig(t *testing.T, serial int64) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("testkit: generate node key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "testkit-node"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("testkit: create node cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("testkit: parse node cert: %v", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}},
		RootCAs:      ca.rootPool,
		ServerName:   "127.0.0.1",
		MinVersion:   tls.VersionTLS13,
	}
}
