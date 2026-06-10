package connstr

import (
	"encoding/base64"
	"errors"
	"testing"
)

func testKeyString() string {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return base64.StdEncoding.EncodeToString(key)
}

func TestGenerateKey(t *testing.T) {
	encoded, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	key, err := ParseKey(encoded)
	if err != nil {
		t.Fatalf("ParseKey(GenerateKey()) error = %v", err)
	}
	if len(key) != KeySize {
		t.Fatalf("key length = %d, want %d", len(key), KeySize)
	}
}

func TestParseKey(t *testing.T) {
	key, err := ParseKey(testKeyString())
	if err != nil {
		t.Fatalf("ParseKey() error = %v", err)
	}
	if len(key) != KeySize {
		t.Fatalf("key length = %d, want %d", len(key), KeySize)
	}
}

func TestParseKeyRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want error
	}{
		{name: "empty", key: "", want: ErrMissingKey},
		{name: "not base64", key: "not-base64", want: ErrInvalidKey},
		{name: "wrong size", key: base64.StdEncoding.EncodeToString([]byte("short")), want: ErrInvalidKey},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseKey(tt.key)
			if !errors.Is(err, tt.want) {
				t.Fatalf("ParseKey() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestLoadKeyPrefersExplicitValue(t *testing.T) {
	t.Setenv(EnvKeyName, base64.StdEncoding.EncodeToString([]byte("short")))
	key, err := LoadKey(testKeyString())
	if err != nil {
		t.Fatalf("LoadKey() error = %v", err)
	}
	if len(key) != KeySize {
		t.Fatalf("key length = %d, want %d", len(key), KeySize)
	}
}

func TestLoadKeyFromEnv(t *testing.T) {
	t.Setenv(EnvKeyName, testKeyString())
	key, err := LoadKeyFromEnv()
	if err != nil {
		t.Fatalf("LoadKeyFromEnv() error = %v", err)
	}
	if len(key) != KeySize {
		t.Fatalf("key length = %d, want %d", len(key), KeySize)
	}
}

func TestLoadKeyFromEnvRequiresValue(t *testing.T) {
	t.Setenv(EnvKeyName, "")
	_, err := LoadKeyFromEnv()
	if !errors.Is(err, ErrMissingKey) {
		t.Fatalf("LoadKeyFromEnv() error = %v, want %v", err, ErrMissingKey)
	}
}
