package actions

import (
	"database/sql"
	"fmt"
	mysql "gt-checksum/MySQL"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
	"os"
	"regexp"
	"strings"
)

var mysqlTableAutoIncrementOptionPattern = regexp.MustCompile(`(?i)\)\s*ENGINE\s*=.*?\bAUTO_INCREMENT\s*=\s*([0-9]+)\b`)
var mysqlAlterTableStatementPattern = regexp.MustCompile("(?is)^\\s*ALTER\\s+TABLE\\s+((?:`[^`]+`\\.`[^`]+`)|(?:[^\\s]+))\\s+(.*?);?\\s*$")

func resolveMySQLTableAutoIncrementFixValue(sourceValue, destValue sql.NullInt64) (int64, bool) {
	if sourceValue.Valid == destValue.Valid {
		if !sourceValue.Valid {
			return 0, false
		}
		if sourceValue.Int64 == destValue.Int64 {
			return 0, false
		}
	}
	if sourceValue.Valid {
		return sourceValue.Int64, true
	}
	if destValue.Valid {
		return 0, true
	}
	return 0, false
}

func buildMySQLTableAutoIncrementAdvisory(destSchema, destTable string, sourceValue, destValue sql.NullInt64) (schemacompat.ConstraintRepairSuggestion, bool) {
	fixValue, needsFix := resolveMySQLTableAutoIncrementFixValue(sourceValue, destValue)
	if !needsFix {
		return schemacompat.ConstraintRepairSuggestion{}, false
	}

	suggestion := schemacompat.ConstraintRepairSuggestion{
		Kind:  "TABLE AUTO_INCREMENT",
		Level: schemacompat.ConstraintRepairLevelAdvisoryOnly,
		Reason: fmt.Sprintf(
			"table AUTO_INCREMENT next value differs between source and target (source=%v, target=%v); this drift does not change existing rows and should only be aligned if future inserts must continue from the source sequence",
			nullInt64ForLog(sourceValue),
			nullInt64ForLog(destValue),
		),
	}
	if sourceValue.Valid {
		suggestion.Statements = []string{
			fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT=%d;", destSchema, destTable, fixValue),
		}
	}
	return suggestion, true
}

func normalizeFixSQLForExec(stmt string) string {
	s := strings.TrimSpace(stmt)
	if s == "" {
		return ""
	}
	if strings.HasPrefix(s, "--") || strings.HasPrefix(s, "/*") {
		return ""
	}
	if strings.HasPrefix(strings.ToUpper(s), "DELIMITER ") {
		return ""
	}
	if strings.HasSuffix(s, "$$") {
		s = strings.TrimSpace(strings.TrimSuffix(s, "$$"))
	}
	return s
}

func buildConstraintAdvisoryLines(scope string, suggestions []schemacompat.ConstraintRepairSuggestion) []string {
	if len(suggestions) == 0 {
		return nil
	}

	lines := []string{
		fmt.Sprintf("-- gt-checksum advisory begin: %s", scope),
		"-- generated as manual review SQL only; review carefully before execution",
	}
	for _, suggestion := range suggestions {
		isManualReview := suggestion.Level == schemacompat.ConstraintRepairLevelManualReview
		lines = append(lines, fmt.Sprintf("-- level: %s", suggestion.Level))
		lines = append(lines, fmt.Sprintf("-- kind: %s", suggestion.Kind))
		if suggestion.ConstraintName != "" {
			lines = append(lines, fmt.Sprintf("-- constraint: %s", suggestion.ConstraintName))
		}
		if suggestion.Reason != "" {
			lines = append(lines, fmt.Sprintf("-- reason: %s", suggestion.Reason))
		}
		if len(suggestion.Statements) == 0 {
			lines = append(lines, "-- suggested SQL: none")
			continue
		}
		for _, stmt := range suggestion.Statements {
			stmt = strings.TrimSpace(stmt)
			if isManualReview {
				// manual-review 级别：SQL 语句必须以注释形式输出，避免用户误执行
				// 添加显著标记以便检索和人工审核
				lines = append(lines, "")
				lines = append(lines, "-- !!! MANUAL REVIEW REQUIRED: review carefully before execution !!!")
				lines = append(lines, fmt.Sprintf("-- %s", stmt))
				lines = append(lines, "-- !!! END manual-review block !!!")
				lines = append(lines, "")
			} else {
				// advisory-only 级别：SQL 语句以注释形式写出，仅供参考
				lines = append(lines, fmt.Sprintf("-- %s", stmt))
			}
		}
	}
	lines = append(lines, fmt.Sprintf("-- gt-checksum advisory end: %s", scope))
	return lines
}

func (stcls *schemaTable) writeAdvisoryFixSql(sqls []string, logThreadSeq int64) error {
	if len(sqls) == 0 {
		return nil
	}

	if !strings.EqualFold(stcls.datafix, "file") {
		global.Wlog.Warn(fmt.Sprintf("(%d) Constraint repair suggestions were generated but not executed. Use datafix=file to export advisory SQL.", logThreadSeq))
		return nil
	}

	objType := stcls.fixFileObjectType
	if objType == "" {
		objType = "table"
	}
	tableFileName := fmt.Sprintf("%s/%s.%s.%s.sql",
		stcls.datafixSql, objType,
		fixFileNameEncode(stcls.schema), fixFileNameEncode(stcls.table))
	file, err := os.OpenFile(tableFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open advisory fix file %s: %v", tableFileName, err)
	}
	defer file.Close()
	return mysql.WriteFixIfNeededFile(stcls.datafix, file, sqls, logThreadSeq, stcls.djdbc)
}

type alterTableMergeBucket struct {
	firstIndex int
	tableExpr  string
	clauses    []string
}

func parseAlterTableStatement(stmt string) (tableExpr string, clause string, ok bool) {
	matches := mysqlAlterTableStatementPattern.FindStringSubmatch(strings.TrimSpace(stmt))
	if len(matches) != 3 {
		return "", "", false
	}
	tableExpr = strings.TrimSpace(matches[1])
	clause = strings.TrimSpace(matches[2])
	if tableExpr == "" || clause == "" {
		return "", "", false
	}
	clause = strings.TrimSuffix(clause, ";")
	clause = strings.TrimSpace(clause)
	if clause == "" {
		return "", "", false
	}
	return tableExpr, clause, true
}

func alterTableMergeKey(tableExpr string) string {
	key := strings.ReplaceAll(strings.TrimSpace(tableExpr), "`", "")
	return strings.ToLower(key)
}

// mergeAlterTableStatements merges ALTER TABLE statements targeting the same table.
// It supports non-contiguous ALTER statements and keeps non-ALTER SQL ordering intact.
// Special handling: my_row_id VISIBLE/INVISIBLE operations are never merged with other operations.
func mergeAlterTableStatements(sqls []string, logThreadSeq int64) []string {
	if len(sqls) <= 1 {
		return sqls
	}

	// 调试：打印所有传入的 SQL 语句
	if global.Wlog != nil {
		for i, stmt := range sqls {
			global.Wlog.Debug(fmt.Sprintf("(%d) mergeAlterTableStatements input[%d]: %s", logThreadSeq, i, stmt))
		}
	}

	buckets := make(map[string]*alterTableMergeBucket)
	for idx, stmt := range sqls {
		// 检查是否是 my_row_id 的 VISIBLE/INVISIBLE 操作
		// 这些操作必须保持独立，不能与其他操作合并
		if isMyRowIDVisibilityStatement(stmt) {
			if global.Wlog != nil {
				global.Wlog.Debug(fmt.Sprintf("(%d) Skipping merge for my_row_id VISIBLE/INVISIBLE statement: %s", logThreadSeq, stmt))
			}
			continue
		}

		tableExpr, clause, ok := parseAlterTableStatement(stmt)
		if !ok {
			continue
		}
		key := alterTableMergeKey(tableExpr)
		b, exists := buckets[key]
		if !exists {
			b = &alterTableMergeBucket{
				firstIndex: idx,
				tableExpr:  tableExpr,
			}
			buckets[key] = b
		}
		b.clauses = append(b.clauses, clause)
	}

	if len(buckets) == 0 {
		return sqls
	}

	merged := make([]string, 0, len(sqls))
	for idx, stmt := range sqls {
		// my_row_id 的 VISIBLE/INVISIBLE 操作保持独立
		if isMyRowIDVisibilityStatement(stmt) {
			merged = append(merged, stmt)
			continue
		}

		tableExpr, _, ok := parseAlterTableStatement(stmt)
		if !ok {
			merged = append(merged, stmt)
			continue
		}
		key := alterTableMergeKey(tableExpr)
		b, exists := buckets[key]
		if !exists {
			merged = append(merged, stmt)
			continue
		}
		if idx != b.firstIndex {
			continue
		}
		combined := fmt.Sprintf("ALTER TABLE %s %s;", b.tableExpr, strings.Join(b.clauses, ", "))
		if len(b.clauses) > 1 {
			if global.Wlog != nil {
				global.Wlog.Debug(fmt.Sprintf("(%d) Merged %d ALTER TABLE statements for %s into one statement", logThreadSeq, len(b.clauses), b.tableExpr))
			}
		}
		merged = append(merged, combined)
	}
	return merged
}

// isMyRowIDVisibilityStatement 检查 SQL 语句是否是 my_row_id 的 VISIBLE/INVISIBLE 操作
func isMyRowIDVisibilityStatement(stmt string) bool {
	upperStmt := strings.ToUpper(strings.TrimSpace(stmt))
	// 检查是否包含 my_row_id 和 VISIBLE/INVISIBLE 关键字
	if !strings.Contains(upperStmt, "MY_ROW_ID") {
		return false
	}
	if !strings.Contains(upperStmt, "VISIBLE") && !strings.Contains(upperStmt, "INVISIBLE") {
		return false
	}
	// 检查是否是 ALTER TABLE ... MODIFY COLUMN 操作
	if !strings.Contains(upperStmt, "ALTER TABLE") || !strings.Contains(upperStmt, "MODIFY COLUMN") {
		return false
	}
	return true
}
