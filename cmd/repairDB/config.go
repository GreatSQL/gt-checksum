package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// Global variables
var (
	config Config
)

// parseConfig parses the configuration file
func parseConfig(confFile string) error {
	content, err := os.ReadFile(confFile)
	if err != nil {
		return fmt.Errorf("Failed to read config file: %v", err)
	}

	logbinSet := false
	splitInsertOnDupKeySet := false
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, ";") || line == "" {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "dstDSN":
			config.DstDSN = value
		case "parallelThds":
			fmt.Sscanf(value, "%d", &config.ParallelThds)
		case "fixFileDir":
			config.FixFileDir = value
		case "logbin":
			logbinSet = true
			switch strings.ToUpper(value) {
			case "OFF":
				config.LogBin = false
			case "ON":
				config.LogBin = true
			default:
				return fmt.Errorf("invalid value for logbin: %q (must be ON or OFF)", value)
			}
		case "resultFile":
			config.ResultFile = value
		case "splitInsertOnDupKey":
			splitInsertOnDupKeySet = true
			switch strings.ToUpper(value) {
			case "OFF":
				config.SplitInsertOnDupKey = false
			case "ON":
				config.SplitInsertOnDupKey = true
			default:
				return fmt.Errorf("invalid value for splitInsertOnDupKey: %q (must be ON or OFF)", value)
			}
		case "resume":
			switch strings.ToUpper(strings.TrimSpace(value)) {
			case "OFF":
				config.Resume = "OFF"
			case "ON":
				config.Resume = "ON"
			case "ASK":
				config.Resume = "ASK"
			default:
				return fmt.Errorf("invalid value for resume: %q (must be OFF, ON, or ASK)", value)
			}
		case "dstSslCa":
			config.SslCa = value
		case "dstSslCert":
			config.SslCert = value
		case "dstSslKey":
			config.SslKey = value
		case "dstSslMode":
			config.SslMode = strings.ToUpper(strings.TrimSpace(value))
		}
	}

	if config.ParallelThds <= 0 {
		config.ParallelThds = 4
	}
	if config.FixFileDir == "" {
		config.FixFileDir = "./fixsql"
	}
	if !logbinSet {
		config.LogBin = true // default: keep sql_log_bin ON
	}
	if !splitInsertOnDupKeySet {
		config.SplitInsertOnDupKey = true
	}
	if config.Resume == "" {
		config.Resume = "OFF" // default: no resume
	}
	config.LogFile = "repairDB.log"

	if config.DstDSN == "" {
		return fmt.Errorf("Missing dstDSN parameter in config file")
	}

	return nil
}

// parseDSN extracts the raw DSN from the "mysql|..." prefixed format
func parseDSN(dsn string) string {
	parts := strings.Split(dsn, "|")
	if len(parts) != 2 {
		return dsn
	}
	return parts[1]
}

// setupSSLConfig configures TLS for repairDB and returns the tls parameter value for DSN
func setupSSLConfig(caFile, certFile, keyFile, mode string) (string, error) {
	mode = strings.ToUpper(strings.TrimSpace(mode))

	// SSL 模式白名单校验
	validModes := map[string]bool{
		"DISABLED": true, "PREFERRED": true, "REQUIRED": true,
		"VERIFY_CA": true, "VERIFY_IDENTITY": true,
	}
	if !validModes[mode] {
		return "", fmt.Errorf("invalid SSL mode: %q (must be DISABLED, PREFERRED, REQUIRED, VERIFY_CA, or VERIFY_IDENTITY)", mode)
	}

	// DISABLED mode
	if mode == "DISABLED" {
		return "false", nil
	}

	// PREFERRED mode without certificates
	if mode == "PREFERRED" && caFile == "" && certFile == "" && keyFile == "" {
		return "preferred", nil
	}

	// REQUIRED mode without certificates
	if mode == "REQUIRED" && caFile == "" && certFile == "" && keyFile == "" {
		// InsecureSkipVerify=true is the design intent for REQUIRED: encryption only, no cert verification
		tlsCfg := &tls.Config{
			InsecureSkipVerify: true,
		}
		if err := mysql.RegisterTLSConfig("repairDB-dst", tlsCfg); err != nil {
			return "", fmt.Errorf("failed to register TLS config: %v", err)
		}
		return "repairDB-dst", nil
	}

	// VERIFY_CA or VERIFY_IDENTITY mode
	if caFile == "" {
		return "", fmt.Errorf("SSL mode %s requires CA certificate file (dstSslCa)", mode)
	}

	// Validate client cert/key pairing
	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		return "", fmt.Errorf("both sslCert and sslKey must be provided together (got cert=%q, key=%q)", certFile, keyFile)
	}

	// Verify file existence
	if _, err := os.Stat(caFile); os.IsNotExist(err) {
		return "", fmt.Errorf("CA certificate file not found: %s", caFile)
	}

	// Load CA certificate
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return "", fmt.Errorf("failed to read CA certificate file %s: %v", caFile, err)
	}
	rootCertPool := x509.NewCertPool()
	if !rootCertPool.AppendCertsFromPEM(caCert) {
		return "", fmt.Errorf("failed to append CA certificate from %s", caFile)
	}

	// Build TLS config
	tlsCfg := &tls.Config{
		RootCAs: rootCertPool,
	}

	// VERIFY_CA mode: verify certificate chain only, skip hostname verification
	// go-sql-driver/mysql auto-sets ServerName from DSN, causing Go to verify hostname by default
	// Use InsecureSkipVerify=true + VerifyPeerCertificate callback for chain-only verification
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

	// Load client certificate if provided
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

	// VERIFY_IDENTITY mode: verify cert chain + hostname
	// go-sql-driver/mysql auto-sets ServerName, Go default TLS behavior handles both

	// Register TLS config
	if err := mysql.RegisterTLSConfig("repairDB-dst", tlsCfg); err != nil {
		return "", fmt.Errorf("failed to register TLS config: %v", err)
	}

	return "repairDB-dst", nil
}

// appendTLSToDSN appends TLS parameter to DSN string
func appendTLSToDSN(dsn, tlsValue string) string {
	if tlsValue == "" || tlsValue == "false" {
		return dsn
	}

	// Check if DSN already has an independent tls= parameter in query string
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
		parts := strings.SplitN(dsn, "?", 2)
		base := parts[0]
		query := parts[1]
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

	if strings.Contains(dsn, "?") {
		return fmt.Sprintf("%s&tls=%s", dsn, tlsValue)
	}
	return fmt.Sprintf("%s?tls=%s", dsn, tlsValue)
}
