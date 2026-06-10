package connstr

import (
	"fmt"
	"strings"
)

func ResolveDSNPassword(driver, jdbc string, key []byte, requireEncrypted bool) (string, error) {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "", "mysql":
		return resolveMySQLPassword(jdbc, key, requireEncrypted)
	case "oracle", "godror":
		return resolveOraclePassword(jdbc, key, requireEncrypted)
	default:
		return "", fmt.Errorf("unsupported DSN driver %q", driver)
	}
}

func resolveMySQLPassword(jdbc string, key []byte, requireEncrypted bool) (string, error) {
	at := strings.LastIndex(jdbc, "@")
	if at <= 0 {
		return "", fmt.Errorf("invalid MySQL DSN: %w", ErrMissingPassword)
	}

	credentials := jdbc[:at]
	colon := strings.Index(credentials, ":")
	if colon < 0 || colon == len(credentials)-1 {
		return "", fmt.Errorf("invalid MySQL DSN: %w", ErrMissingPassword)
	}

	password := credentials[colon+1:]
	resolved, err := resolvePasswordValue(password, key, requireEncrypted)
	if err != nil {
		return "", fmt.Errorf("invalid MySQL DSN password: %w", err)
	}

	return jdbc[:colon+1] + resolved + jdbc[at:], nil
}

func resolvePasswordValue(password string, key []byte, requireEncrypted bool) (string, error) {
	if password == "" {
		return "", ErrMissingPassword
	}
	if IsEncryptedValue(password) {
		return DecryptPassword(password, key)
	}
	if requireEncrypted {
		return "", ErrPlaintextPassword
	}
	return password, nil
}
