package main

import (
	"fmt"
	"regexp"
	"strings"
)

var delimiterDirectivePattern = regexp.MustCompile(`(?i)^\s*DELIMITER\s+(.+?)\s*;?\s*$`)

var mysqlDateFormatLiteralForExecPattern = regexp.MustCompile(`(?i)DATE_FORMAT\(\s*'((?:\\'|[^'])*)'\s*,\s*'%Y-%m-%d %H:%i:%s'\s*\)`)
var mysqlDateTimePrefixForExecPattern = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(\.\d{1,6})?`)

type sqlStatement struct {
	sql       string
	startLine int
	endLine   int
}

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
	trimmed, _ := trimLeadingSQLWhitespaceAndCommentsWithOffset(content)
	return trimmed
}

func trimLeadingSQLWhitespaceAndCommentsWithOffset(content string) (string, int) {
	i := 0
	for i < len(content) {
		switch {
		case isSQLWhitespace(content[i]):
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
			return content[i:], i
		}
	}
	return "", i
}

func isSQLWhitespace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	default:
		return false
	}
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

func extractStatementsByDelimiterWithLocation(content, delimiter string, baseLine int) ([]sqlStatement, string, int) {
	if delimiter == "" {
		delimiter = ";"
	}

	var statements []sqlStatement
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
			raw := content[start:i]
			stmt, stmtStart, stmtEnd := trimSQLStatementText(raw)
			if stmt != "" {
				absoluteStart := start + stmtStart
				absoluteEnd := start + stmtEnd
				statements = append(statements, sqlStatement{
					sql:       stmt,
					startLine: baseLine + strings.Count(content[:absoluteStart], "\n"),
					endLine:   baseLine + strings.Count(content[:absoluteEnd], "\n"),
				})
			}
			i += len(delimiter)
			if i < len(content) && content[i] == ';' {
				i++
			}
			start = i
			continue
		}

		i++
	}

	rest, restOffset := trimLeadingSQLWhitespaceAndCommentsWithOffset(content[start:])
	restStartLine := baseLine + strings.Count(content[:start+restOffset], "\n")
	return statements, rest, restStartLine
}

func trimSQLStatementText(raw string) (string, int, int) {
	start := 0
	for start < len(raw) && isSQLWhitespace(raw[start]) {
		start++
	}
	end := len(raw)
	for end > start && isSQLWhitespace(raw[end-1]) {
		end--
	}
	return raw[start:end], start, end
}

// splitSQLStatements splits SQL statements and supports MySQL DELIMITER directive.
func splitSQLStatements(content string) []string {
	withLocation := splitSQLStatementsWithLocation(content)
	statements := make([]string, 0, len(withLocation))
	for _, stmt := range withLocation {
		statements = append(statements, stmt.sql)
	}
	return statements
}

func splitSQLStatementsWithLocation(content string) []sqlStatement {
	var statements []sqlStatement
	delimiter := ";"
	var currentStmt strings.Builder
	currentStartLine := 1
	lines := strings.Split(content, "\n")

	// flushBlock 对当前累积的块整体调用一次解析。与「每追加一行就重扫整个 buffer」
	// 的旧实现相比，这里仅在 DELIMITER 指令切换处和文件末尾各调用一次，
	// 把单条跨多行语句的解析成本从 O(K²) 降为 O(K)（K 为该语句占用的行数）。
	// extractStatementsByDelimiterWithLocation 是关于 (content, delimiter, baseLine)
	// 的纯函数，对整块调用一次与对其递增前缀逐行调用产出的语句边界、rest 及行号完全一致。
	flushBlock := func() {
		ready, rest, restStartLine := extractStatementsByDelimiterWithLocation(currentStmt.String(), delimiter, currentStartLine)
		statements = append(statements, ready...)
		currentStmt.Reset()
		currentStmt.WriteString(rest)
		currentStartLine = restStartLine
	}

	for i, line := range lines {
		lineNo := i + 1
		if newDelimiter, ok := parseDelimiterDirective(line); ok {
			// 用旧 delimiter 处理已累积的块，再切换到新 delimiter。
			flushBlock()
			delimiter = newDelimiter
			continue
		}

		if currentStmt.Len() == 0 {
			currentStartLine = lineNo
		}
		currentStmt.WriteString(line)
		if i < len(lines)-1 {
			currentStmt.WriteString("\n")
		}
	}

	// 文件末尾：对最后一个块整体解析一次，取出所有以 delimiter 结尾的语句，
	// 剩余的 rest 交给下方逻辑作为「未被 delimiter 终结的尾部语句」处理。
	flushBlock()

	lastRaw := currentStmt.String()
	lastRest, restOffset := trimLeadingSQLWhitespaceAndCommentsWithOffset(lastRaw)
	lastStmt, stmtStart, stmtEnd := trimSQLStatementText(lastRest)
	if lastStmt != "" {
		absoluteStart := restOffset + stmtStart
		absoluteEnd := restOffset + stmtEnd
		statements = append(statements, sqlStatement{
			sql:       lastStmt,
			startLine: currentStartLine + strings.Count(lastRaw[:absoluteStart], "\n"),
			endLine:   currentStartLine + strings.Count(lastRaw[:absoluteEnd], "\n"),
		})
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
