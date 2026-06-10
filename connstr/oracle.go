package connstr

import (
	"fmt"
	"net/url"
	"strings"
	"unicode"
)

func resolveOraclePassword(jdbc string, key []byte, requireEncrypted bool) (string, error) {
	if IsOracleKeyValueDSN(jdbc) {
		return resolveOracleKeyValuePassword(jdbc, key, requireEncrypted)
	}
	return resolveOracleLegacyPassword(jdbc, key, requireEncrypted)
}

func IsOracleKeyValueDSN(jdbc string) bool {
	entries, err := parseOracleKeyValueEntries(jdbc)
	if err != nil || len(entries) == 0 {
		return false
	}
	for _, entry := range entries {
		switch strings.ToLower(entry.key) {
		case "user", "password", "connectstring":
			return true
		}
	}
	return false
}

func resolveOracleLegacyPassword(jdbc string, key []byte, requireEncrypted bool) (string, error) {
	at := strings.LastIndex(jdbc, "@")
	if at <= 0 || at == len(jdbc)-1 {
		return "", fmt.Errorf("invalid Oracle DSN: %w", ErrMissingPassword)
	}

	credentials := jdbc[:at]
	slash := strings.Index(credentials, "/")
	if slash < 0 || slash == len(credentials)-1 {
		return "", fmt.Errorf("invalid Oracle DSN credentials: %w", ErrMissingPassword)
	}

	password := credentials[slash+1:]
	resolved, err := resolvePasswordValue(password, key, requireEncrypted)
	if err != nil {
		return "", fmt.Errorf("invalid Oracle DSN password: %w", err)
	}
	if IsEncryptedValue(password) {
		resolved = url.PathEscape(resolved)
	}

	return jdbc[:slash+1] + resolved + jdbc[at:], nil
}

func resolveOracleKeyValuePassword(jdbc string, key []byte, requireEncrypted bool) (string, error) {
	entries, err := parseOracleKeyValueEntries(jdbc)
	if err != nil {
		return "", err
	}

	passwordIndex := -1
	for i := range entries {
		if strings.EqualFold(entries[i].key, "password") {
			passwordIndex = i
		}
	}
	if passwordIndex < 0 {
		return "", fmt.Errorf("invalid Oracle DSN key-value format: %w", ErrMissingPassword)
	}

	entry := entries[passwordIndex]
	resolved, err := resolvePasswordValue(entry.value, key, requireEncrypted)
	if err != nil {
		return "", fmt.Errorf("invalid Oracle DSN password: %w", err)
	}
	if !IsEncryptedValue(entry.value) {
		return jdbc, nil
	}

	escaped := escapeOracleKeyValue(resolved, entry.quote)
	if entry.quoted {
		return jdbc[:entry.valueStart] + escaped + jdbc[entry.valueEnd:], nil
	}
	return jdbc[:entry.valueStart] + `"` + escapeOracleKeyValue(resolved, '"') + `"` + jdbc[entry.valueEnd:], nil
}

type oracleKeyValueEntry struct {
	key        string
	value      string
	valueStart int
	valueEnd   int
	quoted     bool
	quote      byte
}

func parseOracleKeyValueEntries(jdbc string) ([]oracleKeyValueEntry, error) {
	entries := make([]oracleKeyValueEntry, 0)
	for i := 0; i < len(jdbc); {
		for i < len(jdbc) && unicode.IsSpace(rune(jdbc[i])) {
			i++
		}
		if i >= len(jdbc) {
			break
		}

		keyStart := i
		for i < len(jdbc) && jdbc[i] != '=' && !unicode.IsSpace(rune(jdbc[i])) {
			i++
		}
		key := strings.TrimSpace(jdbc[keyStart:i])
		if key == "" {
			return nil, fmt.Errorf("invalid Oracle DSN key-value format")
		}
		for i < len(jdbc) && unicode.IsSpace(rune(jdbc[i])) {
			i++
		}
		if i >= len(jdbc) || jdbc[i] != '=' {
			return nil, fmt.Errorf("invalid Oracle DSN key-value format")
		}
		i++
		for i < len(jdbc) && unicode.IsSpace(rune(jdbc[i])) {
			i++
		}
		if i >= len(jdbc) {
			return nil, fmt.Errorf("invalid Oracle DSN key-value format")
		}

		entry := oracleKeyValueEntry{key: key, valueStart: i, valueEnd: i}
		if jdbc[i] == '\'' || jdbc[i] == '"' {
			quote := jdbc[i]
			entry.quoted = true
			entry.quote = quote
			i++
			entry.valueStart = i
			var b strings.Builder
			for i < len(jdbc) {
				ch := jdbc[i]
				if ch == '\\' {
					if i+1 >= len(jdbc) {
						return nil, fmt.Errorf("invalid Oracle DSN key-value format")
					}
					b.WriteByte(jdbc[i+1])
					i += 2
					continue
				}
				if ch == quote {
					entry.valueEnd = i
					entry.value = b.String()
					i++
					entries = append(entries, entry)
					goto nextEntry
				}
				b.WriteByte(ch)
				i++
			}
			return nil, fmt.Errorf("invalid Oracle DSN key-value format")
		}

		entry.valueStart = i
		for i < len(jdbc) && !unicode.IsSpace(rune(jdbc[i])) {
			i++
		}
		entry.valueEnd = i
		entry.value = jdbc[entry.valueStart:entry.valueEnd]
		entries = append(entries, entry)

	nextEntry:
	}
	return entries, nil
}

func escapeOracleKeyValue(value string, quote byte) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	if quote == '\'' {
		return strings.ReplaceAll(escaped, `'`, `\'`)
	}
	return strings.ReplaceAll(escaped, `"`, `\"`)
}
