package actions

import (
	"context"
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"strings"
	"time"
)

type repairSQLContextExecer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type datafixSplitInsertStatement struct {
	sql        string
	tupleIndex int
}

func execRepairStatementWithDupKeySplit(ctx context.Context, execer repairSQLContextExecer, stmt string, logThreadSeq int64, location string) error {
	if _, err := execer.ExecContext(ctx, stmt); err == nil {
		return nil
	} else if isDatafixDuplicateKeyError(err) {
		return execRepairSplitInsertOnDuplicateKey(ctx, execer, stmt, err, logThreadSeq, location)
	} else {
		return err
	}
}

func execRepairSplitInsertOnDuplicateKey(ctx context.Context, execer repairSQLContextExecer, stmt string, duplicateErr error, logThreadSeq int64, location string) error {
	splitStatements, ok, err := splitDatafixMultiValueInsert(stmt)
	if err != nil {
		logDatafixDupKeySplitWarn(fmt.Sprintf("(%d) [DUPKEY-SPLIT] %s duplicate key split skipped: parse error=%v originalError=%v", logThreadSeq, location, err, duplicateErr))
		return duplicateErr
	}
	if !ok {
		return duplicateErr
	}

	return execRepairSplitInsertStatements(ctx, execer, splitStatements, duplicateErr, logThreadSeq, location)
}

func execRepairSplitInsertStatements(ctx context.Context, execer repairSQLContextExecer, splitStatements []datafixSplitInsertStatement, duplicateErr error, logThreadSeq int64, location string) error {
	startTime := time.Now()
	var successRows int64
	var skippedDuplicates int64

	logDatafixDupKeySplitInfo(fmt.Sprintf("(%d) [DUPKEY-SPLIT] %s duplicate key detected, split multi-values INSERT into %d single INSERT statements: %v", logThreadSeq, location, len(splitStatements), duplicateErr))
	for _, splitStmt := range splitStatements {
		result, execErr := execer.ExecContext(ctx, splitStmt.sql)
		if execErr == nil {
			rows, rowsErr := result.RowsAffected()
			if rowsErr != nil {
				rows = 1
			}
			successRows += rows
			logDatafixDupKeySplitDebug(fmt.Sprintf("(%d) [DUPKEY-SPLIT] %s tuple #%d/%d executed successfully, rowsAffected=%d", logThreadSeq, location, splitStmt.tupleIndex, len(splitStatements), rows))
			continue
		}

		if isDatafixDuplicateKeyError(execErr) {
			skippedDuplicates++
			logDatafixDupKeySplitWarn(fmt.Sprintf("(%d) [DUPKEY-SPLIT] %s tuple #%d/%d skipped duplicate: sql=%s error=%v", logThreadSeq, location, splitStmt.tupleIndex, len(splitStatements), splitStmt.sql, execErr))
			continue
		}

		logDatafixDupKeySplitError(fmt.Sprintf("(%d) [DUPKEY-SPLIT] %s tuple #%d/%d failed with non-duplicate error: sql=%s error=%v", logThreadSeq, location, splitStmt.tupleIndex, len(splitStatements), splitStmt.sql, execErr))
		return fmt.Errorf("split INSERT tuple #%d/%d failed: %v", splitStmt.tupleIndex, len(splitStatements), execErr)
	}

	logDatafixDupKeySplitInfo(fmt.Sprintf("(%d) [DUPKEY-SPLIT] %s split retry completed, tupleTotal=%d successRows=%d skippedDuplicates=%d elapsed=%v", logThreadSeq, location, len(splitStatements), successRows, skippedDuplicates, time.Since(startTime)))
	return nil
}

func isDatafixDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	errText := err.Error()
	return strings.Contains(errText, "Error 1062") || strings.Contains(errText, "Duplicate entry")
}

func splitDatafixMultiValueInsert(stmt string) ([]datafixSplitInsertStatement, bool, error) {
	sql := strings.TrimSpace(trimLeadingDatafixSQLWhitespaceAndComments(stmt))
	if sql == "" || !hasDatafixInsertIntoPrefix(sql) {
		return nil, false, nil
	}

	sql = strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	valuesIndex := findDatafixTopLevelKeyword(sql, "VALUES")
	if valuesIndex < 0 {
		return nil, false, nil
	}

	prefix := strings.TrimSpace(sql[:valuesIndex+len("VALUES")])
	valuesPart := strings.TrimSpace(sql[valuesIndex+len("VALUES"):])
	tuples, suffix, err := splitDatafixValueTuples(valuesPart)
	if err != nil {
		return nil, false, err
	}
	if suffix != "" || len(tuples) <= 1 {
		return nil, false, nil
	}

	statements := make([]datafixSplitInsertStatement, 0, len(tuples))
	for i, tuple := range tuples {
		statements = append(statements, datafixSplitInsertStatement{
			sql:        prefix + " " + tuple + ";",
			tupleIndex: i + 1,
		})
	}
	return statements, true, nil
}

func hasDatafixInsertIntoPrefix(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(upper, "INSERT") {
		return false
	}
	fields := strings.Fields(upper)
	return len(fields) >= 2 && fields[0] == "INSERT" && fields[1] == "INTO"
}

func findDatafixTopLevelKeyword(sql, keyword string) int {
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
		case isDatafixMySQLDashCommentStart(sql, i):
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

		if parenDepth == 0 && i+len(keyword) <= len(sql) && strings.EqualFold(sql[i:i+len(keyword)], keyword) && hasDatafixKeywordBoundary(sql, i, i+len(keyword)) {
			return i
		}
		i++
	}

	return -1
}

func hasDatafixKeywordBoundary(sql string, start, end int) bool {
	if start > 0 && isDatafixSQLIdentChar(sql[start-1]) {
		return false
	}
	if end < len(sql) && isDatafixSQLIdentChar(sql[end]) {
		return false
	}
	return true
}

func isDatafixSQLIdentChar(c byte) bool {
	return c == '_' || c == '$' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func splitDatafixValueTuples(valuesPart string) ([]string, string, error) {
	var tuples []string
	for i := 0; i < len(valuesPart); {
		i = skipDatafixSQLWhitespaceAndComments(valuesPart, i)
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
			case isDatafixMySQLDashCommentStart(valuesPart, i):
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

		i = skipDatafixSQLWhitespaceAndComments(valuesPart, i)
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

func trimLeadingDatafixSQLWhitespaceAndComments(content string) string {
	i := skipDatafixSQLWhitespaceAndComments(content, 0)
	if i >= len(content) {
		return ""
	}
	return content[i:]
}

func skipDatafixSQLWhitespaceAndComments(sql string, i int) int {
	for i < len(sql) {
		switch {
		case isDatafixSQLWhitespace(sql[i]):
			i++
		case isDatafixMySQLDashCommentStart(sql, i):
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

func isDatafixSQLWhitespace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	default:
		return false
	}
}

func isDatafixMySQLDashCommentStart(content string, idx int) bool {
	if idx+1 >= len(content) || content[idx] != '-' || content[idx+1] != '-' {
		return false
	}
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

func logDatafixDupKeySplitInfo(msg string) {
	if global.Wlog != nil {
		global.Wlog.Info(msg)
	}
}

func logDatafixDupKeySplitDebug(msg string) {
	if global.Wlog != nil {
		global.Wlog.Debug(msg)
	}
}

func logDatafixDupKeySplitWarn(msg string) {
	if global.Wlog != nil {
		global.Wlog.Warn(msg)
	}
}

func logDatafixDupKeySplitError(msg string) {
	if global.Wlog != nil {
		global.Wlog.Error(msg)
	}
}
