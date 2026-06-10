package main

import (
	"bytes"
	"encoding/base64"
	"gt-checksum/connstr"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func cryptTestKey(t *testing.T) string {
	t.Helper()
	key := make([]byte, connstr.KeySize)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return base64.StdEncoding.EncodeToString(key)
}

func TestRunGenKey(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run([]string{"gen-key"}, &stdout, &stderr); err != nil {
		t.Fatalf("run(gen-key) error = %v, stderr=%s", err, stderr.String())
	}
	if _, err := connstr.ParseKey(strings.TrimSpace(stdout.String())); err != nil {
		t.Fatalf("generated key is not parseable: %v", err)
	}
}

func TestRunEncryptDecryptWithPassword(t *testing.T) {
	key := cryptTestKey(t)
	var encryptedOut, stderr bytes.Buffer
	if err := run([]string{"encrypt", "--key", key, "--password", "secret-pass"}, &encryptedOut, &stderr); err != nil {
		t.Fatalf("run(encrypt) error = %v, stderr=%s", err, stderr.String())
	}
	ciphertext := strings.TrimSpace(encryptedOut.String())
	if !connstr.IsEncryptedValue(ciphertext) {
		t.Fatalf("ciphertext %q is not ENC[...]", ciphertext)
	}

	var decryptedOut bytes.Buffer
	stderr.Reset()
	if err := run([]string{"decrypt", "--key", key, "--ciphertext", ciphertext}, &decryptedOut, &stderr); err != nil {
		t.Fatalf("run(decrypt) error = %v, stderr=%s", err, stderr.String())
	}
	if got := strings.TrimRight(decryptedOut.String(), "\r\n"); got != "secret-pass" {
		t.Fatalf("decrypted = %q, want secret-pass", got)
	}
}

func TestRunEncryptWithEnvKeyAndPasswordFile(t *testing.T) {
	key := cryptTestKey(t)
	t.Setenv(connstr.EnvKeyName, key)
	passwordFile := filepath.Join(t.TempDir(), "password.txt")
	if err := os.WriteFile(passwordFile, []byte("file-pass\n"), 0600); err != nil {
		t.Fatalf("write password file: %v", err)
	}

	var stdout, stderr bytes.Buffer
	if err := run([]string{"encrypt", "--password-file", passwordFile}, &stdout, &stderr); err != nil {
		t.Fatalf("run(encrypt --password-file) error = %v, stderr=%s", err, stderr.String())
	}
	plain, err := connstr.DecryptPassword(strings.TrimSpace(stdout.String()), mustParseCryptTestKey(t, key))
	if err != nil {
		t.Fatalf("DecryptPassword() error = %v", err)
	}
	if plain != "file-pass" {
		t.Fatalf("plain = %q, want file-pass", plain)
	}
}

func TestRunEncryptRejectsMissingKey(t *testing.T) {
	t.Setenv(connstr.EnvKeyName, "")
	var stdout, stderr bytes.Buffer
	err := run([]string{"encrypt", "--password", "secret-pass"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), connstr.ErrMissingKey.Error()) {
		t.Fatalf("run(encrypt) error = %v, want missing key", err)
	}
}

func TestRunEncryptRejectsPasswordAndFileTogether(t *testing.T) {
	key := cryptTestKey(t)
	passwordFile := filepath.Join(t.TempDir(), "password.txt")
	if err := os.WriteFile(passwordFile, []byte("file-pass"), 0600); err != nil {
		t.Fatalf("write password file: %v", err)
	}

	var stdout, stderr bytes.Buffer
	err := run([]string{"encrypt", "--key", key, "--password", "secret-pass", "--password-file", passwordFile}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "specify exactly one") {
		t.Fatalf("run(encrypt) error = %v, want one password source error", err)
	}
}

func TestRunRejectsKeyFileFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{"encrypt", "--key-file", "key.txt", "--password", "secret-pass"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("run(encrypt --key-file) error = %v, want undefined flag", err)
	}
}

func mustParseCryptTestKey(t *testing.T, key string) []byte {
	t.Helper()
	parsed, err := connstr.ParseKey(key)
	if err != nil {
		t.Fatalf("ParseKey() error = %v", err)
	}
	return parsed
}
