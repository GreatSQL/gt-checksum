package main

import (
	"fmt"
	"regexp"
	"strings"
)

var delimiterDirectivePattern = regexp.MustCompile(`(?i)^\s*DELIMITER\s+(.+?)\s*;?\s*$`)

var mysqlDateFormatLiteralForExecPattern = regexp.MustCompile(`(?i)DATE_FORMAT\(\s*'((?:\\'|[^'])*)'\s*,\s*'%Y-%m-%d %H:%i:%s'\s*\)`)
var mysqlDateTimePrefixForExecPattern = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(\.\d{1,6})?`)

func isBeginStatement(stmt string) bool {
	s := strings.ToUpper(strings.TrimSpace(stmt))
	return s == "BEGIN" || s == "START TRANSACTION"
}

func isCommitOrRollbackStatement(stmt string) bool {
	s := strings.ToUpper(strings.TrimSpace(stmt))
	return s == "COMMIT" || s == "ROLLBACK"
}

func isMySQLDashCommentStart(content string, idx int) bool {
	if idx+1 >= len(content) || content[idx] != '-' || content[idx+1] != '-' {
		return false
	}
	// MySQL treats "--" as a comment starter only when the second dash is
	// followed by at least one whitespace/control character.
	if idx+2 >= len(content) {
		return false
	}
	switch content[idx+2] {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	default:
		return false
	}
}

func trimLeadingSQLWhitespaceAndComments(content string) string {
	i := 0
	for i < len(content) {
		switch {
		case content[i] == ' ' || content[i] == '\t' || content[i] == '\n' || content[i] == '\r' || content[i] == '\f' || content[i] == '\v':
			i++
		case isMySQLDashCommentStart(content, i):
			i += 2
			for i < len(content) && content[i] != '\n' {
				i++
			}
		case content[i] == '#':
			i++
			for i < len(content) && content[i] != '\n' {
				i++
			}
		case i+1 < len(content) && content[i] == '/' && content[i+1] == '*':
			i += 2
			for i+1 < len(content) && !(content[i] == '*' && content[i+1] == '/') {
				i++
			}
			if i+1 < len(content) {
				i += 2
			}
		default:
			return content[i:]
		}
	}
	return ""
}

// parseDelimiterDirective parses lines like:
// DELIMITER $$
// DELIMITER $$;
func parseDelimiterDirective(line string) (string, bool) {
	matches := delimiterDirectivePattern.FindStringSubmatch(strings.TrimSpace(line))
	if len(matches) != 2 {
		return "", false
	}
	raw := strings.TrimSpace(matches[1])
	if raw == "" {
		return "", false
	}
	if raw == ";" {
		return ";", true
	}

	delimiter := raw
	// Compatibility: allow DELIMITER $$; style with trailing statement terminator.
	delimiter = strings.TrimSuffix(delimiter, ";")
	delimiter = strings.TrimSpace(delimiter)
	if delimiter == "" {
		return "", false
	}
	return delimiter, true
}

// extractStatementsByDelimiter extracts completed SQL statements from content
// with the provided delimiter, skipping delimiters inside literals/comments.
func extractStatementsByDelimiter(content, delimiter string) ([]string, string) {
	if delimiter == "" {
		delimiter = ";"
	}

	var statements []string
	start := 0
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false
	escaped := false

	for i := 0; i < len(content); {
		c := content[i]
		var next byte
		if i+1 < len(content) {
			next = content[i+1]
		}

		if inLineComment {
			if c == '\n' {
				inLineComment = false
			}
			i++
			continue
		}
		if inBlockComment {
			if c == '*' && next == '/' {
				inBlockComment = false
				i += 2
				continue
			}
			i++
			continue
		}

		if escaped {
			escaped = false
			i++
			continue
		}
		if inSingleQuote {
			if c == '\\' {
				escaped = true
				i++
				continue
			}
			if c == '\'' {
				inSingleQuote = false
			}
			i++
			continue
		}
		if inDoubleQuote {
			if c == '\\' {
				escaped = true
				i++
				continue
			}
			if c == '"' {
				inDoubleQuote = false
			}
			i++
			continue
		}
		if inBacktick {
			if c == '`' {
				inBacktick = false
			}
			i++
			continue
		}

		switch {
		case isMySQLDashCommentStart(content, i):
			inLineComment = true
			i += 2
			continue
		case c == '#':
			inLineComment = true
			i++
			continue
		case c == '/' && next == '*':
			inBlockComment = true
			i += 2
			continue
		case c == '\'':
			inSingleQuote = true
			i++
			continue
		case c == '"':
			inDoubleQuote = true
			i++
			continue
		case c == '`':
			inBacktick = true
			i++
			continue
		}

		if strings.HasPrefix(content[i:], delimiter) {
			stmt := strings.TrimSpace(content[start:i])
			if stmt != "" {
				statements = append(statements, stmt)
			}
			i += len(delimiter)
			// Compatibility: some generated files use "$$;" instead of "$$".
			if i < len(content) && content[i] == ';' {
				i++
			}
			start = i
			continue
		}

		i++
	}

	return statements, trimLeadingSQLWhitespaceAndComments(content[start:])
}

// splitSQLStatements splits SQL statements and supports MySQL DELIMITER directive.
func splitSQLStatements(content string) []string {
	var statements []string
	delimiter := ";"
	var currentStmt strings.Builder
	lines := strings.Split(content, "\n")

	for i, line := range lines {
		if newDelimiter, ok := parseDelimiterDirective(line); ok {
			ready, rest := extractStatementsByDelimiter(currentStmt.String(), delimiter)
			statements = append(statements, ready...)
			currentStmt.Reset()
			currentStmt.WriteString(rest)
			delimiter = newDelimiter
			continue
		}

		currentStmt.WriteString(line)
		if i < len(lines)-1 {
			currentStmt.WriteString("\n")
		}

		ready, rest := extractStatementsByDelimiter(currentStmt.String(), delimiter)
		if len(ready) > 0 {
			statements = append(statements, ready...)
			currentStmt.Reset()
			currentStmt.WriteString(rest)
		}
	}

	lastStmt := strings.TrimSpace(trimLeadingSQLWhitespaceAndComments(currentStmt.String()))
	if lastStmt != "" {
		statements = append(statements, lastStmt)
	}

	return statements
}

func normalizeMySQLDateTimeLiteralForExec(value string) string {
	s := strings.TrimSpace(value)
	if s == "" {
		return s
	}
	matches := mysqlDateTimePrefixForExecPattern.FindStringSubmatch(s)
	if len(matches) >= 3 {
		frac := ""
		if len(matches) >= 4 {
			frac = matches[3]
		}
		return matches[1] + " " + matches[2] + frac
	}
	if len(s) >= 19 && s[10] == 'T' {
		return s[:10] + " " + s[11:]
	}
	return s
}

func normalizeMySQLDateFormatLiteralInSQLForExec(sql string) string {
	if !strings.Contains(strings.ToUpper(sql), "DATE_FORMAT(") {
		return sql
	}
	return mysqlDateFormatLiteralForExecPattern.ReplaceAllStringFunc(sql, func(segment string) string {
		matches := mysqlDateFormatLiteralForExecPattern.FindStringSubmatch(segment)
		if len(matches) < 2 {
			return segment
		}
		raw := strings.ReplaceAll(matches[1], `\'`, `'`)
		normalized := normalizeMySQLDateTimeLiteralForExec(raw)
		escaped := strings.ReplaceAll(normalized, `'`, `\'`)
		return fmt.Sprintf("'%s'", escaped)
	})
}
