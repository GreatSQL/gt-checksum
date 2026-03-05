package oracle

import (
	"fmt"
	"regexp"
	"strings"
)

var oracleSimpleIdentifierPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_$#]*$`)

func normalizeOracleIdentifier(name string) (string, bool) {
	trimmed := strings.TrimSpace(name)
	if len(trimmed) >= 2 && strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"") {
		unquoted := trimmed[1 : len(trimmed)-1]
		return strings.ReplaceAll(unquoted, "\"\"", "\""), true
	}
	return trimmed, false
}

func escapeOracleLiteral(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func shouldQuoteOracleIdentifier(name string) bool {
	normalized, explicitlyQuoted := normalizeOracleIdentifier(name)
	if normalized == "" {
		return false
	}
	if explicitlyQuoted {
		return true
	}
	return !oracleSimpleIdentifierPattern.MatchString(normalized)
}

func oracleIdentifier(name string) string {
	normalized, explicitlyQuoted := normalizeOracleIdentifier(name)
	if normalized == "" {
		return ""
	}
	if explicitlyQuoted || !oracleSimpleIdentifierPattern.MatchString(normalized) {
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(normalized, "\"", "\"\""))
	}
	return strings.ToUpper(normalized)
}

func oracleColumnIdentifier(name string) string {
	return oracleIdentifier(name)
}

func oracleQualifiedTable(schema, table string) string {
	return fmt.Sprintf("%s.%s", oracleIdentifier(schema), oracleIdentifier(table))
}

// Oracle data dictionary lookup:
// - Explicit case object names (quoted identifiers) use exact match.
// - Normal identifiers use case-insensitive comparison via UPPER.
func oracleMetadataMatchExpr(column, value string) string {
	normalized, explicitlyQuoted := normalizeOracleIdentifier(value)
	escaped := escapeOracleLiteral(normalized)
	if explicitlyQuoted || !oracleSimpleIdentifierPattern.MatchString(normalized) {
		return fmt.Sprintf("%s = '%s'", column, escaped)
	}
	return fmt.Sprintf("UPPER(%s) = UPPER('%s')", column, escaped)
}
