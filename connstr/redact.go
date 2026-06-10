package connstr

import "strings"

const redactedPassword = "******"

func RedactFullDSN(raw string) string {
	parts := strings.SplitN(raw, "|", 2)
	if len(parts) == 2 {
		driver := strings.TrimSpace(parts[0])
		return driver + "|" + RedactDSN(driver, strings.TrimSpace(parts[1]))
	}
	return RedactDSN("mysql", strings.TrimSpace(raw))
}

func RedactDSN(driver, jdbc string) string {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "", "mysql":
		return redactMySQLDSN(jdbc)
	case "oracle", "godror":
		return redactOracleDSN(jdbc)
	default:
		return redactPasswordAssignmentFallback(jdbc)
	}
}

func redactMySQLDSN(jdbc string) string {
	at := strings.LastIndex(jdbc, "@")
	if at <= 0 {
		return jdbc
	}
	credentials := jdbc[:at]
	colon := strings.Index(credentials, ":")
	if colon < 0 || colon == len(credentials)-1 {
		return jdbc
	}
	return jdbc[:colon+1] + redactedPassword + jdbc[at:]
}

func redactOracleDSN(jdbc string) string {
	if IsOracleKeyValueDSN(jdbc) {
		redacted, ok := redactOracleKeyValueDSN(jdbc)
		if ok {
			return redacted
		}
		return redactPasswordAssignmentFallback(jdbc)
	}
	redacted := redactOracleLegacyDSN(jdbc)
	if redacted != jdbc {
		return redacted
	}
	return redactPasswordAssignmentFallback(jdbc)
}

func redactOracleLegacyDSN(jdbc string) string {
	at := strings.LastIndex(jdbc, "@")
	if at <= 0 || at == len(jdbc)-1 {
		return jdbc
	}
	credentials := jdbc[:at]
	slash := strings.Index(credentials, "/")
	if slash < 0 || slash == len(credentials)-1 {
		return jdbc
	}
	return jdbc[:slash+1] + redactedPassword + jdbc[at:]
}

func redactOracleKeyValueDSN(jdbc string) (string, bool) {
	entries, err := parseOracleKeyValueEntries(jdbc)
	if err != nil {
		return "", false
	}

	passwordIndex := -1
	for i := range entries {
		if strings.EqualFold(entries[i].key, "password") {
			passwordIndex = i
		}
	}
	if passwordIndex < 0 {
		return jdbc, true
	}

	entry := entries[passwordIndex]
	if entry.quoted {
		return jdbc[:entry.valueStart] + redactedPassword + jdbc[entry.valueEnd:], true
	}
	return jdbc[:entry.valueStart] + `"` + redactedPassword + `"` + jdbc[entry.valueEnd:], true
}

func redactPasswordAssignmentFallback(jdbc string) string {
	start, end, quoted := findPasswordAssignmentValue(jdbc)
	if start < 0 {
		return jdbc
	}
	if quoted {
		return jdbc[:start] + redactedPassword + jdbc[end:]
	}
	return jdbc[:start] + `"` + redactedPassword + `"` + jdbc[end:]
}

func findPasswordAssignmentValue(jdbc string) (start int, end int, quoted bool) {
	lower := strings.ToLower(jdbc)
	for searchFrom := 0; searchFrom < len(lower); {
		idx := strings.Index(lower[searchFrom:], "password")
		if idx < 0 {
			return -1, -1, false
		}
		idx += searchFrom
		j := idx + len("password")
		for j < len(jdbc) && isASCIISpace(jdbc[j]) {
			j++
		}
		if j >= len(jdbc) || jdbc[j] != '=' {
			searchFrom = idx + len("password")
			continue
		}
		j++
		for j < len(jdbc) && isASCIISpace(jdbc[j]) {
			j++
		}
		if j >= len(jdbc) {
			return -1, -1, false
		}
		if jdbc[j] == '\'' || jdbc[j] == '"' {
			quote := jdbc[j]
			valueStart := j + 1
			k := valueStart
			for k < len(jdbc) {
				if jdbc[k] == '\\' {
					k += 2
					continue
				}
				if jdbc[k] == quote {
					return valueStart, k, true
				}
				k++
			}
			return valueStart, len(jdbc), true
		}

		valueStart := j
		for j < len(jdbc) && !isASCIISpace(jdbc[j]) {
			j++
		}
		return valueStart, j, false
	}
	return -1, -1, false
}

func isASCIISpace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	default:
		return false
	}
}
