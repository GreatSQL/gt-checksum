package connstr

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
)

const (
	EnvKeyName = "GT_CHECKSUM_DSN_KEY"
	KeySize    = 32
)

func GenerateKey() (string, error) {
	key := make([]byte, KeySize)
	if _, err := rand.Read(key); err != nil {
		return "", fmt.Errorf("generate dsn encryption key: %w", err)
	}
	return base64.StdEncoding.EncodeToString(key), nil
}

func ParseKey(encoded string) ([]byte, error) {
	trimmed := strings.TrimSpace(encoded)
	if trimmed == "" {
		return nil, ErrMissingKey
	}

	key, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		key, err = base64.RawStdEncoding.DecodeString(trimmed)
	}
	if err != nil || len(key) != KeySize {
		return nil, ErrInvalidKey
	}
	return key, nil
}

func LoadKey(keyValue string) ([]byte, error) {
	if strings.TrimSpace(keyValue) != "" {
		return ParseKey(keyValue)
	}
	return LoadKeyFromEnv()
}

func LoadKeyFromEnv() ([]byte, error) {
	return ParseKey(os.Getenv(EnvKeyName))
}
