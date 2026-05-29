package main

import (
	"fmt"
	"strings"
)

type splitInsertStatement struct {
	sql        string
	tupleIndex int
}

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	errText := err.Error()
	return strings.Contains(errText, "Error 1062") || strings.Contains(errText, "Duplicate entry")
}

func splitMultiValueInsert(stmt string) ([]splitInsertStatement, bool, error) {
	sql := strings.TrimSpace(trimLeadingSQLWhitespaceAndComments(stmt))
	if sql == "" || !hasInsertIntoPrefix(sql) {
		return nil, false, nil
	}

	sql = strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	valuesIndex := findTopLevelKeyword(sql, "VALUES")
	if valuesIndex < 0 {
		return nil, false, nil
	}

	prefix := strings.TrimSpace(sql[:valuesIndex+len("VALUES")])
	valuesPart := strings.TrimSpace(sql[valuesIndex+len("VALUES"):])
	tuples, suffix, err := splitValueTuples(valuesPart)
	if err != nil {
		return nil, false, err
	}
	if suffix != "" || len(tuples) <= 1 {
		return nil, false, nil
	}

	statements := make([]splitInsertStatement, 0, len(tuples))
	for i, tuple := range tuples {
		statements = append(statements, splitInsertStatement{
			sql:        prefix + " " + tuple + ";",
			tupleIndex: i + 1,
		})
	}
	return statements, true, nil
}

func hasInsertIntoPrefix(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(upper, "INSERT") {
		return false
	}
	fields := strings.Fields(upper)
	return len(fields) >= 2 && fields[0] == "INSERT" && fields[1] == "INTO"
}

func findTopLevelKeyword(sql, keyword string) int {
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false
	escaped := false
	parenDepth := 0

	for i := 0; i < len(sql); {
		c := sql[i]
		var next byte
		if i+1 < len(sql) {
			next = sql[i+1]
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
		case isMySQLDashCommentStart(sql, i):
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
		case c == '(':
			parenDepth++
			i++
			continue
		case c == ')':
			if parenDepth > 0 {
				parenDepth--
			}
			i++
			continue
		}

		if parenDepth == 0 && i+len(keyword) <= len(sql) && strings.EqualFold(sql[i:i+len(keyword)], keyword) && hasKeywordBoundary(sql, i, i+len(keyword)) {
			return i
		}
		i++
	}

	return -1
}

func hasKeywordBoundary(sql string, start, end int) bool {
	if start > 0 && isSQLIdentChar(sql[start-1]) {
		return false
	}
	if end < len(sql) && isSQLIdentChar(sql[end]) {
		return false
	}
	return true
}

func isSQLIdentChar(c byte) bool {
	return c == '_' || c == '$' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func splitValueTuples(valuesPart string) ([]string, string, error) {
	var tuples []string
	for i := 0; i < len(valuesPart); {
		i = skipSQLWhitespaceAndComments(valuesPart, i)
		if i >= len(valuesPart) {
			return tuples, "", nil
		}
		if valuesPart[i] != '(' {
			return tuples, strings.TrimSpace(valuesPart[i:]), nil
		}

		tupleStart := i
		parenDepth := 0
		inSingleQuote := false
		inDoubleQuote := false
		inBacktick := false
		inLineComment := false
		inBlockComment := false
		escaped := false
		closed := false

		for i < len(valuesPart) {
			c := valuesPart[i]
			var next byte
			if i+1 < len(valuesPart) {
				next = valuesPart[i+1]
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
			case isMySQLDashCommentStart(valuesPart, i):
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
			case c == '"':
				inDoubleQuote = true
			case c == '`':
				inBacktick = true
			case c == '(':
				parenDepth++
			case c == ')':
				parenDepth--
				if parenDepth < 0 {
					return nil, "", fmt.Errorf("unexpected closing parenthesis in INSERT values")
				}
				if parenDepth == 0 {
					i++
					tuples = append(tuples, strings.TrimSpace(valuesPart[tupleStart:i]))
					closed = true
				}
			}
			if closed {
				break
			}
			i++
		}

		if !closed {
			return nil, "", fmt.Errorf("unterminated INSERT values tuple")
		}

		i = skipSQLWhitespaceAndComments(valuesPart, i)
		if i >= len(valuesPart) {
			return tuples, "", nil
		}
		if valuesPart[i] != ',' {
			return tuples, strings.TrimSpace(valuesPart[i:]), nil
		}
		i++
	}
	return tuples, "", nil
}

func skipSQLWhitespaceAndComments(sql string, i int) int {
	for i < len(sql) {
		switch {
		case isSQLWhitespace(sql[i]):
			i++
		case isMySQLDashCommentStart(sql, i):
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case sql[i] == '#':
			i++
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*':
			i += 2
			for i+1 < len(sql) && !(sql[i] == '*' && sql[i+1] == '/') {
				i++
			}
			if i+1 < len(sql) {
				i += 2
			}
		default:
			return i
		}
	}
	return i
}
