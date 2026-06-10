package connstr

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
)

const (
	ciphertextPrefix = "ENC["
	ciphertextSuffix = "]"
	cipherVersion    = "v1"
	cipherAlgorithm  = "aes256gcm"
	defaultKeyID     = "default"
)

func IsEncryptedValue(value string) bool {
	trimmed := strings.TrimSpace(value)
	return strings.HasPrefix(trimmed, ciphertextPrefix) && strings.HasSuffix(trimmed, ciphertextSuffix)
}

func EncryptPassword(password string, key []byte, kid string) (string, error) {
	if len(key) != KeySize {
		return "", ErrInvalidKey
	}
	if strings.TrimSpace(kid) == "" {
		kid = defaultKeyID
	}
	if strings.ContainsAny(kid, ":]") {
		return "", fmt.Errorf("%w: invalid key id", ErrInvalidCiphertext)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create aes-gcm cipher: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}

	ciphertext := gcm.Seal(nil, nonce, []byte(password), nil)
	return fmt.Sprintf("ENC[%s:%s:%s:%s:%s]",
		cipherVersion,
		cipherAlgorithm,
		kid,
		base64.RawURLEncoding.EncodeToString(nonce),
		base64.RawURLEncoding.EncodeToString(ciphertext),
	), nil
}

func DecryptPassword(value string, key []byte) (string, error) {
	if len(key) != KeySize {
		return "", ErrInvalidKey
	}

	parts, err := parseCiphertext(value)
	if err != nil {
		return "", err
	}
	if parts.version != cipherVersion || parts.algorithm != cipherAlgorithm {
		return "", ErrUnsupportedCiphertext
	}

	nonce, err := base64.RawURLEncoding.DecodeString(parts.nonce)
	if err != nil {
		return "", ErrInvalidCiphertext
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(parts.ciphertext)
	if err != nil {
		return "", ErrInvalidCiphertext
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create aes-gcm cipher: %w", err)
	}
	if len(nonce) != gcm.NonceSize() || len(ciphertext) == 0 {
		return "", ErrInvalidCiphertext
	}

	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", ErrDecryptFailed
	}
	return string(plain), nil
}

type encryptedParts struct {
	version    string
	algorithm  string
	kid        string
	nonce      string
	ciphertext string
}

func parseCiphertext(value string) (encryptedParts, error) {
	trimmed := strings.TrimSpace(value)
	if !IsEncryptedValue(trimmed) {
		return encryptedParts{}, ErrInvalidCiphertext
	}

	body := strings.TrimSuffix(strings.TrimPrefix(trimmed, ciphertextPrefix), ciphertextSuffix)
	fields := strings.Split(body, ":")
	if len(fields) != 5 {
		return encryptedParts{}, ErrInvalidCiphertext
	}
	for _, field := range fields {
		if field == "" {
			return encryptedParts{}, ErrInvalidCiphertext
		}
	}
	return encryptedParts{
		version:    fields[0],
		algorithm:  fields[1],
		kid:        fields[2],
		nonce:      fields[3],
		ciphertext: fields[4],
	}, nil
}
