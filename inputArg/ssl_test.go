package inputArg

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSetupSSLConfig_DisabledMode(t *testing.T) {
	result, err := setupSSLConfig("", "", "", "DISABLED", "test-disabled")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "false" {
		t.Errorf("expected 'false', got '%s'", result)
	}
}

func TestSetupSSLConfig_PreferredModeNoCerts(t *testing.T) {
	result, err := setupSSLConfig("", "", "", "PREFERRED", "test-preferred")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "preferred" {
		t.Errorf("expected 'preferred', got '%s'", result)
	}
}

func TestSetupSSLConfig_RequiredModeNoCerts(t *testing.T) {
	result, err := setupSSLConfig("", "", "", "REQUIRED", "test-required")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "test-required" {
		t.Errorf("expected 'test-required', got '%s'", result)
	}
}

func TestSetupSSLConfig_ModeCaseInsensitive(t *testing.T) {
	result, err := setupSSLConfig("", "", "", "disabled", "test-case")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "false" {
		t.Errorf("expected 'false', got '%s'", result)
	}
}

func TestSetupSSLConfig_VerifyCAMissingCAFile(t *testing.T) {
	_, err := setupSSLConfig("", "", "", "VERIFY_CA", "test-verify-ca")
	if err == nil {
		t.Fatal("expected error for missing CA file, got nil")
	}
}

func TestSetupSSLConfig_VerifyCANonexistentCAFile(t *testing.T) {
	_, err := setupSSLConfig("/nonexistent/ca.pem", "", "", "VERIFY_CA", "test-verify-ca")
	if err == nil {
		t.Fatal("expected error for nonexistent CA file, got nil")
	}
}

func TestSetupSSLConfig_VerifyCAWithInvalidCAFile(t *testing.T) {
	tmpDir := t.TempDir()
	caFile := filepath.Join(tmpDir, "invalid-ca.pem")

	// Write invalid certificate content
	if err := os.WriteFile(caFile, []byte("not a valid certificate"), 0644); err != nil {
		t.Fatalf("failed to write CA file: %v", err)
	}

	_, err := setupSSLConfig(caFile, "", "", "VERIFY_CA", "test-verify-ca-invalid")
	if err == nil {
		t.Fatal("expected error for invalid CA file, got nil")
	}
}

func TestSetupSSLConfig_VerifyIdentityMissingCAFile(t *testing.T) {
	_, err := setupSSLConfig("", "", "", "VERIFY_IDENTITY", "test-verify-identity")
	if err == nil {
		t.Fatal("expected error for missing CA file, got nil")
	}
}

func TestSetupSSLConfig_PreferreModeWithCAFile(t *testing.T) {
	tmpDir := t.TempDir()
	caFile := filepath.Join(tmpDir, "ca.pem")

	// Write invalid certificate - this should fail
	if err := os.WriteFile(caFile, []byte("not a valid certificate"), 0644); err != nil {
		t.Fatalf("failed to write CA file: %v", err)
	}

	// PREFERRED mode with CA file should try to load the certificate
	_, err := setupSSLConfig(caFile, "", "", "PREFERRED", "test-preferred-ca")
	if err == nil {
		t.Fatal("expected error for invalid CA file, got nil")
	}
}

func TestSetupSSLConfig_ClientCertFileNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	caFile := filepath.Join(tmpDir, "ca.pem")

	// Write invalid certificate content
	if err := os.WriteFile(caFile, []byte("not a valid certificate"), 0644); err != nil {
		t.Fatalf("failed to write CA file: %v", err)
	}

	_, err := setupSSLConfig(caFile, "/nonexistent/client-cert.pem", "/nonexistent/client-key.pem", "VERIFY_CA", "test-client-cert")
	if err == nil {
		t.Fatal("expected error for invalid CA file, got nil")
	}
}

func TestAppendTLSToDSN_EmptyTLSValue(t *testing.T) {
	dsn := "user:pass@tcp(127.0.0.1:3306)/db"
	result := appendTLSToDSN(dsn, "")
	if result != dsn {
		t.Errorf("expected '%s', got '%s'", dsn, result)
	}
}

func TestAppendTLSToDSN_FalseTLSValue(t *testing.T) {
	dsn := "user:pass@tcp(127.0.0.1:3306)/db"
	result := appendTLSToDSN(dsn, "false")
	if result != dsn {
		t.Errorf("expected '%s', got '%s'", dsn, result)
	}
}

func TestAppendTLSToDSN_NoQueryParams(t *testing.T) {
	dsn := "user:pass@tcp(127.0.0.1:3306)/db"
	result := appendTLSToDSN(dsn, "preferred")
	expected := "user:pass@tcp(127.0.0.1:3306)/db?tls=preferred"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestAppendTLSToDSN_WithQueryParams(t *testing.T) {
	dsn := "user:pass@tcp(127.0.0.1:3306)/db?charset=utf8mb4"
	result := appendTLSToDSN(dsn, "preferred")
	expected := "user:pass@tcp(127.0.0.1:3306)/db?charset=utf8mb4&tls=preferred"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestAppendTLSToDSN_ReplaceExistingTLS(t *testing.T) {
	dsn := "user:pass@tcp(127.0.0.1:3306)/db?charset=utf8mb4&tls=false"
	result := appendTLSToDSN(dsn, "preferred")
	expected := "user:pass@tcp(127.0.0.1:3306)/db?charset=utf8mb4&tls=preferred"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestAppendTLSToDSN_ReplaceExistingTLSWithOtherParams(t *testing.T) {
	dsn := "user:pass@tcp(127.0.0.1:3306)/db?tls=skip-verify&charset=utf8mb4&timeout=30s"
	result := appendTLSToDSN(dsn, "preferred")
	expected := "user:pass@tcp(127.0.0.1:3306)/db?charset=utf8mb4&timeout=30s&tls=preferred"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestAppendTLSToDSN_CustomConfigName(t *testing.T) {
	dsn := "user:pass@tcp(127.0.0.1:3306)/db"
	result := appendTLSToDSN(dsn, "gt-checksum-src")
	expected := "user:pass@tcp(127.0.0.1:3306)/db?tls=gt-checksum-src"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestAppendTLSToDSN_NoQuestionMark(t *testing.T) {
	dsn := "user:pass@tcp(127.0.0.1:3306)/db"
	result := appendTLSToDSN(dsn, "preferred")
	expected := "user:pass@tcp(127.0.0.1:3306)/db?tls=preferred"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}

func TestAppendTLSToDSN_WithQuestionMark(t *testing.T) {
	dsn := "user:pass@tcp(127.0.0.1:3306)/db?"
	result := appendTLSToDSN(dsn, "preferred")
	expected := "user:pass@tcp(127.0.0.1:3306)/db?&tls=preferred"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}
