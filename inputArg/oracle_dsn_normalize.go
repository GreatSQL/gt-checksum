package inputArg

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
)

// normalizeOracleJDBC converts legacy Oracle DSN:
// user/password@host:port/service?k=v
// into godror key-value format:
// user="..." password="..." connectString="..." k="v"
func normalizeOracleJDBC(raw string) (string, error) {
	jdbc := strings.TrimSpace(raw)
	if jdbc == "" {
		return "", fmt.Errorf("empty Oracle DSN")
	}

	lower := strings.ToLower(jdbc)
	if strings.Contains(lower, "connectstring=") && strings.Contains(lower, "user=") {
		return jdbc, nil
	}
	if strings.HasPrefix(lower, "oracle://") {
		return jdbc, nil
	}

	at := strings.LastIndex(jdbc, "@")
	if at <= 0 || at == len(jdbc)-1 {
		return "", fmt.Errorf("invalid Oracle DSN %q: expected format user/password@host:port/service_name[?k=v]", raw)
	}

	cred := jdbc[:at]
	connectAndQuery := jdbc[at+1:]

	slash := strings.Index(cred, "/")
	if slash <= 0 || slash == len(cred)-1 {
		return "", fmt.Errorf("invalid Oracle DSN credentials %q: expected user/password", cred)
	}

	userRaw := cred[:slash]
	passRaw := cred[slash+1:]
	user, err := url.PathUnescape(userRaw)
	if err != nil {
		return "", fmt.Errorf("invalid Oracle DSN username %q: %w", userRaw, err)
	}
	pass, err := url.PathUnescape(passRaw)
	if err != nil {
		return "", fmt.Errorf("invalid Oracle DSN password %q: %w", passRaw, err)
	}

	connectString := connectAndQuery
	queryPart := ""
	if qIdx := strings.Index(connectAndQuery, "?"); qIdx >= 0 {
		connectString = connectAndQuery[:qIdx]
		queryPart = connectAndQuery[qIdx+1:]
	}
	connectString = strings.TrimSpace(connectString)
	if err := validateOracleConnectString(connectString); err != nil {
		return "", err
	}

	params, err := parseOracleQueryParams(queryPart)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString(`user="`)
	b.WriteString(escapeOracleDSNValue(user))
	b.WriteString(`" password="`)
	b.WriteString(escapeOracleDSNValue(pass))
	b.WriteString(`" connectString="`)
	b.WriteString(escapeOracleDSNValue(connectString))
	b.WriteString(`"`)

	for _, key := range orderedOracleParamKeys(params) {
		b.WriteString(" ")
		b.WriteString(key)
		b.WriteString(`="`)
		b.WriteString(escapeOracleDSNValue(params[key]))
		b.WriteString(`"`)
	}

	return b.String(), nil
}

func validateOracleConnectString(connectString string) error {
	if connectString == "" {
		return fmt.Errorf("invalid Oracle DSN: empty connect string")
	}
	slash := strings.Index(connectString, "/")
	if slash <= 0 || slash == len(connectString)-1 {
		return fmt.Errorf("invalid Oracle connect string %q: expected host:port/service_name", connectString)
	}

	hostPort := connectString[:slash]
	service := connectString[slash+1:]
	if hostPort == "" || service == "" {
		return fmt.Errorf("invalid Oracle connect string %q: host, port and service_name are required", connectString)
	}
	if !strings.Contains(hostPort, ":") {
		return fmt.Errorf("invalid Oracle connect string %q: missing :port in host:port/service_name", connectString)
	}
	return nil
}

func parseOracleQueryParams(queryPart string) (map[string]string, error) {
	params := make(map[string]string)
	if strings.TrimSpace(queryPart) == "" {
		return params, nil
	}

	values, err := url.ParseQuery(queryPart)
	if err != nil {
		return nil, fmt.Errorf("invalid Oracle DSN query parameters %q: %w", queryPart, err)
	}
	for key, arr := range values {
		trimmedKey := strings.TrimSpace(key)
		if trimmedKey == "" {
			return nil, fmt.Errorf("invalid Oracle DSN query parameters %q: empty parameter name", queryPart)
		}
		value := ""
		if len(arr) > 0 {
			value = arr[len(arr)-1]
		}
		params[trimmedKey] = value
	}
	return params, nil
}

func orderedOracleParamKeys(params map[string]string) []string {
	if len(params) == 0 {
		return nil
	}

	keys := make([]string, 0, len(params))
	seen := make(map[string]struct{}, len(params))

	preferred := []string{"timezone", "noTimezoneCheck"}
	for _, p := range preferred {
		if _, ok := params[p]; ok {
			keys = append(keys, p)
			seen[p] = struct{}{}
		}
	}

	rest := make([]string, 0, len(params))
	for key := range params {
		if _, ok := seen[key]; ok {
			continue
		}
		rest = append(rest, key)
	}
	sort.Strings(rest)
	keys = append(keys, rest...)

	return keys
}

func escapeOracleDSNValue(v string) string {
	s := strings.ReplaceAll(v, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}
