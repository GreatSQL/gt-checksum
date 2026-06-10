package connstr

import "errors"

var (
	ErrMissingKey            = errors.New("dsn encryption key is required")
	ErrInvalidKey            = errors.New("dsn encryption key must be base64 encoded 32 bytes")
	ErrMissingPassword       = errors.New("dsn password is required")
	ErrPlaintextPassword     = errors.New("dsn password must use ENC[...] ciphertext")
	ErrInvalidCiphertext     = errors.New("invalid ENC[...] ciphertext")
	ErrUnsupportedCiphertext = errors.New("unsupported ENC[...] ciphertext")
	ErrDecryptFailed         = errors.New("failed to decrypt dsn password")
)
