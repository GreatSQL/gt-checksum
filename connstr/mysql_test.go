package connstr

import (
	"errors"
	"strings"
	"testing"
)

func encryptedPassword(t *testing.T, password string) string {
	t.Helper()
	ciphertext, err := EncryptPassword(password, mustTestKey(t), "default")
	if err != nil {
		t.Fatalf("EncryptPassword() error = %v", err)
	}
	return ciphertext
}

func TestResolveMySQLPassword(t *testing.T) {
	ciphertext := encryptedPassword(t, "secret-pass")
	jdbc := "user:" + ciphertext + "@tcp(127.0.0.1:3306)/db?charset=utf8mb4"

	resolved, err := ResolveDSNPassword("mysql", jdbc, mustTestKey(t), true)
	if err != nil {
		t.Fatalf("ResolveDSNPassword() error = %v", err)
	}
	want := "user:secret-pass@tcp(127.0.0.1:3306)/db?charset=utf8mb4"
	if resolved != want {
		t.Fatalf("resolved = %q, want %q", resolved, want)
	}
}

func TestResolveMySQLPasswordWithAtInPassword(t *testing.T) {
	ciphertext := encryptedPassword(t, "p@ss")
	jdbc := "user:" + ciphertext + "@tcp(127.0.0.1:3306)/db"

	resolved, err := ResolveDSNPassword("", jdbc, mustTestKey(t), true)
	if err != nil {
		t.Fatalf("ResolveDSNPassword() error = %v", err)
	}
	if !strings.HasPrefix(resolved, "user:p@ss@tcp(") {
		t.Fatalf("resolved DSN did not preserve @ in password: %q", resolved)
	}
}

func TestResolveMySQLPasswordRejectsPlaintext(t *testing.T) {
	_, err := ResolveDSNPassword("mysql", "user:plain@tcp(127.0.0.1:3306)/db", mustTestKey(t), true)
	if !errors.Is(err, ErrPlaintextPassword) {
		t.Fatalf("ResolveDSNPassword() error = %v, want %v", err, ErrPlaintextPassword)
	}
}

func TestResolveMySQLPasswordAllowsPlaintextWhenOptional(t *testing.T) {
	jdbc := "user:plain@tcp(127.0.0.1:3306)/db"
	resolved, err := ResolveDSNPassword("mysql", jdbc, mustTestKey(t), false)
	if err != nil {
		t.Fatalf("ResolveDSNPassword() error = %v", err)
	}
	if resolved != jdbc {
		t.Fatalf("resolved = %q, want %q", resolved, jdbc)
	}
}

func TestResolveMySQLPasswordRejectsMissingPassword(t *testing.T) {
	tests := []string{
		"user@tcp(127.0.0.1:3306)/db",
		"user:@tcp(127.0.0.1:3306)/db",
		"tcp(127.0.0.1:3306)/db",
	}
	for _, jdbc := range tests {
		t.Run(jdbc, func(t *testing.T) {
			_, err := ResolveDSNPassword("mysql", jdbc, mustTestKey(t), true)
			if !errors.Is(err, ErrMissingPassword) {
				t.Fatalf("ResolveDSNPassword() error = %v, want %v", err, ErrMissingPassword)
			}
		})
	}
}

func TestResolveDSNPasswordRejectsUnsupportedDriver(t *testing.T) {
	_, err := ResolveDSNPassword("postgres", "user:pass@host/db", mustTestKey(t), true)
	if err == nil {
		t.Fatalf("ResolveDSNPassword() error = nil, want unsupported driver error")
	}
}
