package connstr

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

func mustTestKey(t *testing.T) []byte {
	t.Helper()
	key, err := ParseKey(testKeyString())
	if err != nil {
		t.Fatalf("ParseKey() error = %v", err)
	}
	return key
}

func otherTestKey() []byte {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = byte(255 - i)
	}
	return key
}

func TestEncryptDecryptPassword(t *testing.T) {
	key := mustTestKey(t)
	ciphertext, err := EncryptPassword("secret-pass", key, "default")
	if err != nil {
		t.Fatalf("EncryptPassword() error = %v", err)
	}
	if !IsEncryptedValue(ciphertext) {
		t.Fatalf("ciphertext %q is not recognized as encrypted", ciphertext)
	}
	if strings.Contains(ciphertext, "secret-pass") {
		t.Fatalf("ciphertext leaked plaintext password: %s", ciphertext)
	}

	plain, err := DecryptPassword(ciphertext, key)
	if err != nil {
		t.Fatalf("DecryptPassword() error = %v", err)
	}
	if plain != "secret-pass" {
		t.Fatalf("plain = %q, want %q", plain, "secret-pass")
	}
}

func TestEncryptPasswordUsesRandomNonce(t *testing.T) {
	key := mustTestKey(t)
	first, err := EncryptPassword("same-password", key, "default")
	if err != nil {
		t.Fatalf("EncryptPassword(first) error = %v", err)
	}
	second, err := EncryptPassword("same-password", key, "default")
	if err != nil {
		t.Fatalf("EncryptPassword(second) error = %v", err)
	}
	if first == second {
		t.Fatalf("ciphertexts should differ for same plaintext")
	}
}

func TestDecryptPasswordRejectsWrongKey(t *testing.T) {
	ciphertext, err := EncryptPassword("secret-pass", mustTestKey(t), "default")
	if err != nil {
		t.Fatalf("EncryptPassword() error = %v", err)
	}
	_, err = DecryptPassword(ciphertext, otherTestKey())
	if !errors.Is(err, ErrDecryptFailed) {
		t.Fatalf("DecryptPassword() error = %v, want %v", err, ErrDecryptFailed)
	}
}

func TestDecryptPasswordRejectsTamperedCiphertext(t *testing.T) {
	ciphertext, err := EncryptPassword("secret-pass", mustTestKey(t), "default")
	if err != nil {
		t.Fatalf("EncryptPassword() error = %v", err)
	}
	tampered := strings.TrimSuffix(ciphertext, "]") + "x]"
	_, err = DecryptPassword(tampered, mustTestKey(t))
	if !errors.Is(err, ErrDecryptFailed) && !errors.Is(err, ErrInvalidCiphertext) {
		t.Fatalf("DecryptPassword() error = %v, want decrypt or ciphertext error", err)
	}
}

func TestDecryptPasswordRejectsInvalidFormat(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  error
	}{
		{name: "plain", value: "secret-pass", want: ErrInvalidCiphertext},
		{name: "missing field", value: "ENC[v1:aes256gcm:default]", want: ErrInvalidCiphertext},
		{name: "unsupported version", value: "ENC[v2:aes256gcm:default:aa:bb]", want: ErrUnsupportedCiphertext},
		{name: "unsupported alg", value: "ENC[v1:aes128gcm:default:aa:bb]", want: ErrUnsupportedCiphertext},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecryptPassword(tt.value, mustTestKey(t))
			if !errors.Is(err, tt.want) {
				t.Fatalf("DecryptPassword() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestEncryptPasswordRejectsInvalidInputs(t *testing.T) {
	_, err := EncryptPassword("secret-pass", []byte("short"), "default")
	if !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("EncryptPassword(short key) error = %v, want %v", err, ErrInvalidKey)
	}

	_, err = EncryptPassword("secret-pass", mustTestKey(t), "bad:kid")
	if !errors.Is(err, ErrInvalidCiphertext) {
		t.Fatalf("EncryptPassword(bad kid) error = %v, want %v", err, ErrInvalidCiphertext)
	}
}

func TestIsEncryptedValue(t *testing.T) {
	if !IsEncryptedValue(" ENC[v1:aes256gcm:default:aa:bb] ") {
		t.Fatalf("IsEncryptedValue() should trim surrounding spaces")
	}
	if IsEncryptedValue("secret-pass") {
		t.Fatalf("IsEncryptedValue() should reject plaintext")
	}
}

func TestDecryptPasswordRejectsInvalidKey(t *testing.T) {
	value := "ENC[v1:aes256gcm:default:aa:bb]"
	_, err := DecryptPassword(value, []byte("short"))
	if !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("DecryptPassword(short key) error = %v, want %v", err, ErrInvalidKey)
	}
}

func TestParseKeyAcceptsRawStdEncoding(t *testing.T) {
	keyBytes := make([]byte, KeySize)
	for i := range keyBytes {
		keyBytes[i] = byte(i + 1)
	}
	encoded := base64.RawStdEncoding.EncodeToString(keyBytes)
	key, err := ParseKey(encoded)
	if err != nil {
		t.Fatalf("ParseKey(raw std) error = %v", err)
	}
	if len(key) != KeySize {
		t.Fatalf("key length = %d, want %d", len(key), KeySize)
	}
}
