package connstr

import (
	"errors"
	"strings"
	"testing"
)

func TestResolveOracleLegacyPassword(t *testing.T) {
	ciphertext := encryptedPassword(t, "secret-pass")
	jdbc := "scott/" + ciphertext + "@127.0.0.1:1521/orclpdb1?timezone=UTC"

	resolved, err := ResolveDSNPassword("oracle", jdbc, mustTestKey(t), true)
	if err != nil {
		t.Fatalf("ResolveDSNPassword() error = %v", err)
	}
	want := "scott/secret-pass@127.0.0.1:1521/orclpdb1?timezone=UTC"
	if resolved != want {
		t.Fatalf("resolved = %q, want %q", resolved, want)
	}
}

func TestResolveOracleLegacyEscapesSpecialPassword(t *testing.T) {
	ciphertext := encryptedPassword(t, "p/a ss")
	jdbc := "scott/" + ciphertext + "@127.0.0.1:1521/orclpdb1"

	resolved, err := ResolveDSNPassword("oracle", jdbc, mustTestKey(t), true)
	if err != nil {
		t.Fatalf("ResolveDSNPassword() error = %v", err)
	}
	if resolved != "scott/p%2Fa%20ss@127.0.0.1:1521/orclpdb1" {
		t.Fatalf("resolved = %q", resolved)
	}
}

func TestResolveOracleLegacyRejectsPlaintext(t *testing.T) {
	_, err := ResolveDSNPassword("oracle", "scott/tiger@127.0.0.1:1521/orclpdb1", mustTestKey(t), true)
	if !errors.Is(err, ErrPlaintextPassword) {
		t.Fatalf("ResolveDSNPassword() error = %v, want %v", err, ErrPlaintextPassword)
	}
}

func TestResolveOracleKeyValuePassword(t *testing.T) {
	ciphertext := encryptedPassword(t, `ti"ger`)
	jdbc := `connectString="127.0.0.1:1521/orclpdb1" user="scott" password="` + ciphertext + `" timezone="UTC"`

	resolved, err := ResolveDSNPassword("oracle", jdbc, mustTestKey(t), true)
	if err != nil {
		t.Fatalf("ResolveDSNPassword() error = %v", err)
	}
	if !strings.Contains(resolved, `password="ti\"ger"`) {
		t.Fatalf("resolved DSN did not contain escaped password: %q", resolved)
	}
	if strings.Contains(resolved, ciphertext) {
		t.Fatalf("resolved DSN still contains ciphertext: %q", resolved)
	}
}

func TestResolveOracleKeyValuePasswordUnquoted(t *testing.T) {
	ciphertext := encryptedPassword(t, "tiger")
	jdbc := `user=scott password=` + ciphertext + ` connectString="127.0.0.1:1521/orclpdb1"`

	resolved, err := ResolveDSNPassword("godror", jdbc, mustTestKey(t), true)
	if err != nil {
		t.Fatalf("ResolveDSNPassword() error = %v", err)
	}
	if !strings.Contains(resolved, `password="tiger"`) {
		t.Fatalf("resolved DSN did not quote password: %q", resolved)
	}
}

func TestResolveOracleKeyValuePasswordWithSpacesAroundEquals(t *testing.T) {
	ciphertext := encryptedPassword(t, "tiger")
	jdbc := `user = "scott" password = "` + ciphertext + `" connectString = "127.0.0.1:1521/orclpdb1"`

	resolved, err := ResolveDSNPassword("oracle", jdbc, mustTestKey(t), true)
	if err != nil {
		t.Fatalf("ResolveDSNPassword() error = %v", err)
	}
	if !strings.Contains(resolved, `password = "tiger"`) {
		t.Fatalf("resolved DSN did not preserve spaced key-value password: %q", resolved)
	}
}

func TestResolveOracleKeyValueRejectsMissingPassword(t *testing.T) {
	_, err := ResolveDSNPassword("oracle", `user="scott" connectString="127.0.0.1:1521/orclpdb1"`, mustTestKey(t), true)
	if !errors.Is(err, ErrMissingPassword) {
		t.Fatalf("ResolveDSNPassword() error = %v, want %v", err, ErrMissingPassword)
	}
}

func TestResolveOracleKeyValueRejectsInvalidFormat(t *testing.T) {
	_, err := ResolveDSNPassword("oracle", `user="scott" password="ENC[broken]`, mustTestKey(t), true)
	if err == nil {
		t.Fatalf("ResolveDSNPassword() error = nil, want invalid key-value error")
	}
}

func TestResolveOracleKeyValueAllowsPlaintextWhenOptional(t *testing.T) {
	jdbc := `user="scott" password="tiger" connectString="127.0.0.1:1521/orclpdb1"`
	resolved, err := ResolveDSNPassword("oracle", jdbc, mustTestKey(t), false)
	if err != nil {
		t.Fatalf("ResolveDSNPassword() error = %v", err)
	}
	if resolved != jdbc {
		t.Fatalf("resolved = %q, want %q", resolved, jdbc)
	}
}
