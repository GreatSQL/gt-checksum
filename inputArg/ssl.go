package inputArg

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// setupSSLConfig 配置 TLS 并返回 DSN 中使用的 tls 参数值
// caFile: CA 证书 PEM 文件路径
// certFile: 客户端证书 PEM 文件路径
// keyFile: 客户端密钥 PEM 文件路径
// mode: SSL 模式 (DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY)
// tlsKey: 注册到 mysql 驱动的 TLS 配置名称
func setupSSLConfig(caFile, certFile, keyFile, mode, tlsKey string) (string, error) {
	mode = strings.ToUpper(strings.TrimSpace(mode))

	// SSL 模式白名单校验
	validModes := map[string]bool{
		"DISABLED": true, "PREFERRED": true, "REQUIRED": true,
		"VERIFY_CA": true, "VERIFY_IDENTITY": true,
	}
	if !validModes[mode] {
		return "", fmt.Errorf("invalid SSL mode: %q (must be DISABLED, PREFERRED, REQUIRED, VERIFY_CA, or VERIFY_IDENTITY)", mode)
	}

	// DISABLED 模式：禁用 SSL
	if mode == "DISABLED" {
		return "false", nil
	}

	// PREFERRED 模式：优先使用 SSL，无证书时使用 preferred
	if mode == "PREFERRED" && caFile == "" && certFile == "" && keyFile == "" {
		return "preferred", nil
	}

	// REQUIRED 模式：必须 SSL，但不验证证书
	if mode == "REQUIRED" && caFile == "" && certFile == "" && keyFile == "" {
		// InsecureSkipVerify=true 是 REQUIRED 模式的设计意图：只要求加密，不验证证书
		tlsCfg := &tls.Config{
			InsecureSkipVerify: true,
		}
		if err := mysql.RegisterTLSConfig(tlsKey, tlsCfg); err != nil {
			return "", fmt.Errorf("failed to register TLS config: %v", err)
		}
		return tlsKey, nil
	}

	// VERIFY_CA 或 VERIFY_IDENTITY 模式：需要加载证书
	if caFile == "" {
		return "", fmt.Errorf("SSL mode %s requires CA certificate file (sslCa)", mode)
	}

	// 验证客户端证书配对：只提供其中一个时报错
	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		return "", fmt.Errorf("both sslCert and sslKey must be provided together (got cert=%q, key=%q)", certFile, keyFile)
	}

	// 验证文件存在性
	if _, err := os.Stat(caFile); os.IsNotExist(err) {
		return "", fmt.Errorf("CA certificate file not found: %s", caFile)
	}

	// 加载 CA 证书
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return "", fmt.Errorf("failed to read CA certificate file %s: %v", caFile, err)
	}
	rootCertPool := x509.NewCertPool()
	if !rootCertPool.AppendCertsFromPEM(caCert) {
		return "", fmt.Errorf("failed to append CA certificate from %s", caFile)
	}

	// 构建 TLS 配置
	tlsCfg := &tls.Config{
		RootCAs: rootCertPool,
	}

	// VERIFY_CA 模式：仅验证证书链，不验证主机名
	// go-sql-driver/mysql 会自动设置 ServerName，导致 Go 默认行为同时验证证书链和主机名
	// 通过 InsecureSkipVerify=true + VerifyPeerCertificate 回调实现仅验证证书链
	if mode == "VERIFY_CA" {
		tlsCfg.InsecureSkipVerify = true
		tlsCfg.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			certs := make([]*x509.Certificate, len(rawCerts))
			for i, raw := range rawCerts {
				cert, err := x509.ParseCertificate(raw)
				if err != nil {
					return fmt.Errorf("failed to parse certificate: %v", err)
				}
				certs[i] = cert
			}
			// 验证证书链
			intermediates := x509.NewCertPool()
			for _, cert := range certs[1:] {
				intermediates.AddCert(cert)
			}
			opts := x509.VerifyOptions{
				Roots:         rootCertPool,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
			}
			_, err := certs[0].Verify(opts)
			return err
		}
	}

	// 如果提供了客户端证书，加载它
	if certFile != "" && keyFile != "" {
		if _, err := os.Stat(certFile); os.IsNotExist(err) {
			return "", fmt.Errorf("client certificate file not found: %s", certFile)
		}
		if _, err := os.Stat(keyFile); os.IsNotExist(err) {
			return "", fmt.Errorf("client key file not found: %s", keyFile)
		}
		clientCert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return "", fmt.Errorf("failed to load client certificate: %v", err)
		}
		tlsCfg.Certificates = []tls.Certificate{clientCert}
	}

	// VERIFY_IDENTITY 模式：验证证书链和服务端身份（主机名）
	// go-sql-driver/mysql 自动设置 ServerName，Go 默认 TLS 行为会同时验证证书链和主机名
	// 不需要额外配置

	// 注册 TLS 配置
	if err := mysql.RegisterTLSConfig(tlsKey, tlsCfg); err != nil {
		return "", fmt.Errorf("failed to register TLS config: %v", err)
	}

	return tlsKey, nil
}

// appendTLSToDSN 将 TLS 参数追加到 DSN 字符串中
// dsn: 原始 DSN 字符串
// tlsValue: TLS 参数值 (false, preferred, 或自定义配置名)
func appendTLSToDSN(dsn, tlsValue string) string {
	// 如果 tlsValue 为空或 false，不修改 DSN
	if tlsValue == "" || tlsValue == "false" {
		return dsn
	}

	// 检查查询参数中是否存在独立的 tls= 参数
	hasTLSParam := false
	if strings.Contains(dsn, "?") {
		queryPart := dsn[strings.Index(dsn, "?")+1:]
		for _, p := range strings.Split(queryPart, "&") {
			if strings.HasPrefix(p, "tls=") {
				hasTLSParam = true
				break
			}
		}
	}

	if hasTLSParam {
		// 替换现有的 tls 参数
		parts := strings.SplitN(dsn, "?", 2)
		base := parts[0]
		query := parts[1]
		// 重新构建查询字符串，替换 tls 参数
		params := strings.Split(query, "&")
		var newParams []string
		for _, param := range params {
			if !strings.HasPrefix(param, "tls=") {
				newParams = append(newParams, param)
			}
		}
		newParams = append(newParams, fmt.Sprintf("tls=%s", tlsValue))
		return fmt.Sprintf("%s?%s", base, strings.Join(newParams, "&"))
	}

	// 追加 tls 参数
	if strings.Contains(dsn, "?") {
		return fmt.Sprintf("%s&tls=%s", dsn, tlsValue)
	}
	return fmt.Sprintf("%s?tls=%s", dsn, tlsValue)
}
