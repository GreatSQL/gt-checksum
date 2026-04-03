package mysql

import (
	"bufio"
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// 跟踪已经在添加列时设置了主键的列
var (
	AutoIncrementColumnsWithPrimaryKey map[string]bool
	// 跟踪目标端表是否存在主键，key格式：schema.table
	DestTableHasPrimaryKey map[string]bool
	// 缓存表的主键列信息，key格式：schema.table
	TablePrimaryKeyColumns map[string][]string
	// 跟踪每个数据库连接当前使用的数据库，key格式：connectionPointer|schema
	CurrentDatabaseCache map[string]string

	// 互斥锁保护缓存map的并发访问
	tablePrimaryKeyMutex sync.RWMutex
	databaseCacheMutex   sync.RWMutex
)

// mysqlQuoteIdent 对 MySQL 标识符加反引号，并对内部反引号做双写转义。
func mysqlQuoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// alterTablePrefixRe 匹配 "ALTER TABLE `schema`.`table` OPERATION[;]"
// 并将 OPERATION 部分捕获为 group 1。
// 标识符可以是反引号引用（含内部 “ 转义）或不含空格的裸名。
var alterTablePrefixRe = regexp.MustCompile(
	"(?i)^ALTER\\s+TABLE\\s+(?:`(?:[^`]|``)*`|\\S+)\\.(?:`(?:[^`]|``)*`|\\S+)\\s+(.+?)\\s*;?\\s*$")

type MysqlDataAbnormalFixStruct struct {
	Schema                  string
	Table                   string
	RowData                 string
	SourceDevice            string
	DestDevice              string
	Sqlwhere                string
	IndexColumnType         string
	ColData                 []map[string]string
	IndexType               string
	IndexColumn             []string
	DatafixType             string
	SourceSchema            string            // 添加源端schema字段
	CaseSensitiveObjectName string            // 是否区分对象名大小写
	IndexVisibilityMap      map[string]string // 索引可见性信息
	ForeignKeyDefinitions   map[string]string // 外键DDL定义信息
	DestFlavor              global.DatabaseFlavor // 目标端数据库类型，用于生成兼容目标端语法的 fix SQL
}

type foreignKeyColumn struct {
	ordinalPosition  int
	columnName       string
	referencedSchema string
	referencedTable  string
	referencedColumn string
}

func buildForeignKeyDDLForFix(fkName string, infoRows []foreignKeyColumn, sourceSchema string) (string, bool) {
	if len(infoRows) == 0 {
		return "", false
	}

	sort.Slice(infoRows, func(i, j int) bool {
		return infoRows[i].ordinalPosition < infoRows[j].ordinalPosition
	})

	referencedSchema := infoRows[0].referencedSchema
	if referencedSchema == "" {
		referencedSchema = sourceSchema
	}

	referencedTable := infoRows[0].referencedTable
	sourceColumns := make([]string, 0, len(infoRows))
	referencedColumns := make([]string, 0, len(infoRows))
	for _, item := range infoRows {
		if item.referencedTable == "" || item.referencedColumn == "" {
			return "", false
		}
		sourceColumns = append(sourceColumns, mysqlQuoteIdent(item.columnName))
		referencedColumns = append(referencedColumns, mysqlQuoteIdent(item.referencedColumn))
	}

	if referencedTable == "" {
		return "", false
	}

	return fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s)",
		mysqlQuoteIdent(fkName),
		strings.Join(sourceColumns, ","),
		mysqlQuoteIdent(referencedSchema),
		mysqlQuoteIdent(referencedTable),
		strings.Join(referencedColumns, ",")), true
}

/*
MySQL 生成insert修复语句
*/
// escapeSQLString 对SQL字符串进行转义，处理特殊字符
func escapeSQLString(str string) string {
	// 直接使用database/sql的Quote函数，确保正确转义
	// 或者使用更安全的转义方式
	var result strings.Builder
	for i := 0; i < len(str); i++ {
		c := str[i]
		switch c {
		case '\'':
			result.WriteString("\\'")
		case '\\':
			result.WriteString("\\\\")
		case '"':
			result.WriteString("\\\"")
		case '\000':
			result.WriteString("\\0")
		case '\n':
			result.WriteString("\\n")
		case '\r':
			result.WriteString("\\r")
		case '\b':
			result.WriteString("\\b")
		case '\t':
			result.WriteString("\\t")
		case '\x1a':
			result.WriteString("\\Z")
		default:
			result.WriteByte(c)
		}
	}
	return result.String()
}

var mysqlDateTimePrefixPattern = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(\.\d{1,6})?`)
var mysqlDateLiteralPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
var mysqlTimeLiteralPattern = regexp.MustCompile(`^\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?$`)
var mysqlDateTimeLiteralPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?$`)
var floatScalePattern = regexp.MustCompile(`(?i)^FLOAT\s*\(\s*\d+\s*,\s*(\d+)\s*\)`)
var integerLiteralPattern = regexp.MustCompile(`^[+-]?\d+$`)
var numericLiteralPattern = regexp.MustCompile(`^[+-]?\d+(?:\.\d+)?$`)
var mysqlKeywordFunctionPattern = regexp.MustCompile(`(?i)^(current_timestamp|current_date|current_time|localtime|localtimestamp)(?:\((\d*)\))?$`)
var mysqlKeywordFunctionInDefinitionPattern = regexp.MustCompile(`(?i)\b(current_timestamp|current_date|current_time|localtime|localtimestamp)(?:\((\d*)\))?`)
var inlinePrimaryKeyPattern = regexp.MustCompile(`(?i)\s+PRIMARY\s+KEY\b`)
var routineFixMetadataCommentPattern = regexp.MustCompile(`(?is)/\*GT_CHECKSUM_METADATA:.*?\*/`)

// normalizeMySQLDateTimeLiteral converts common Oracle/Golang datetime string forms
// (e.g. "2026-02-17 16:04:25 +0800 CST") to MySQL DATETIME/TIMESTAMP literal
// "YYYY-MM-DD HH:MM:SS[.ffffff]".
func normalizeMySQLDateTimeLiteral(value string) string {
	s := strings.TrimSpace(value)
	if s == "" {
		return s
	}
	matches := mysqlDateTimePrefixPattern.FindStringSubmatch(s)
	if len(matches) >= 3 {
		frac := ""
		if len(matches) >= 4 {
			frac = matches[3]
		}
		return matches[1] + " " + matches[2] + frac
	}
	// Fallback: replace ISO T separator if present.
	if len(s) >= 19 && s[10] == 'T' {
		return s[:10] + " " + s[11:]
	}
	return s
}

func stripDeprecatedZeroFillAttr(columnType string) string {
	if !strings.Contains(strings.ToUpper(columnType), "ZEROFILL") {
		return columnType
	}

	fields := strings.Fields(columnType)
	filtered := make([]string, 0, len(fields))
	hasUnsigned := false
	for _, field := range fields {
		switch {
		case strings.EqualFold(field, "ZEROFILL"):
			continue
		case strings.EqualFold(field, "UNSIGNED"):
			hasUnsigned = true
		}
		filtered = append(filtered, field)
	}
	if !hasUnsigned {
		filtered = append(filtered, "unsigned")
	}
	return strings.Join(filtered, " ")
}

func normalizeMySQLKeywordFunction(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	matches := mysqlKeywordFunctionPattern.FindStringSubmatch(trimmed)
	if len(matches) != 3 {
		return "", false
	}

	name := strings.ToUpper(matches[1])
	if matches[2] == "" {
		return name, true
	}
	return fmt.Sprintf("%s(%s)", name, matches[2]), true
}

func normalizeMySQLKeywordFunctionsInDefinition(definition string) string {
	return mysqlKeywordFunctionInDefinitionPattern.ReplaceAllStringFunc(definition, func(match string) string {
		if normalized, ok := normalizeMySQLKeywordFunction(match); ok {
			return normalized
		}
		return match
	})
}

func formatMySQLColumnDefault(defaultValue string, nullable bool) string {
	trimmed := strings.TrimSpace(defaultValue)
	switch {
	case strings.EqualFold(trimmed, "null") || strings.EqualFold(trimmed, "NULL"):
		if nullable {
			return "DEFAULT NULL"
		}
		return ""
	case trimmed == "":
		return ""
	default:
		if normalized, ok := normalizeMySQLKeywordFunction(trimmed); ok {
			return "DEFAULT " + normalized
		}
		literal, _ := schemacompat.UnwrapQuotedDefaultLiteral(trimmed)
		literal = normalizeMySQLDateTimeLiteral(strings.TrimSpace(literal))
		switch {
		case numericLiteralPattern.MatchString(literal):
			return "DEFAULT " + literal
		case mysqlDateLiteralPattern.MatchString(literal),
			mysqlTimeLiteralPattern.MatchString(literal),
			mysqlDateTimeLiteralPattern.MatchString(literal):
			return fmt.Sprintf("DEFAULT '%s'", escapeSQLString(literal))
		default:
			return fmt.Sprintf("DEFAULT '%s'", escapeSQLString(literal))
		}
	}
}

func isBinaryLikeColumnType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	return strings.HasPrefix(t, "BIT") ||
		strings.HasPrefix(t, "BINARY") ||
		strings.HasPrefix(t, "VARBINARY") ||
		strings.Contains(t, "BLOB")
}

func formatMySQLInsertLiteral(value, dataType string) string {
	if strings.EqualFold(value, "<entry>") {
		return "''"
	}
	if strings.EqualFold(value, "<nil>") {
		return "NULL"
	}
	if strings.EqualFold(dataType, "DATETIME") || strings.Contains(strings.ToUpper(dataType), "TIMESTAMP") {
		return fmt.Sprintf("'%s'", escapeSQLString(normalizeMySQLDateTimeLiteral(value)))
	}
	if isBinaryLikeColumnType(dataType) {
		return fmt.Sprintf("0x%X", []byte(value))
	}
	return fmt.Sprintf("'%s'", escapeSQLString(value))
}

func lookupColumnDataType(colData []map[string]string, columnName string) string {
	for _, col := range colData {
		if strings.EqualFold(col["columnName"], columnName) {
			return col["dataType"]
		}
	}
	return ""
}

func floatDeleteScaleByType(dataType string) (int, bool) {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	if !strings.HasPrefix(t, "FLOAT") {
		return 0, false
	}
	matches := floatScalePattern.FindStringSubmatch(t)
	if len(matches) == 2 {
		scale, err := strconv.Atoi(matches[1])
		if err == nil && scale >= 0 && scale <= 30 {
			return scale, true
		}
	}
	return 0, false
}

func isFloatDeleteType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	return strings.HasPrefix(t, "FLOAT") || strings.HasPrefix(t, "DOUBLE") || strings.HasPrefix(t, "REAL")
}

func isIntegerDeleteType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	return strings.HasPrefix(t, "TINYINT") ||
		strings.HasPrefix(t, "SMALLINT") ||
		strings.HasPrefix(t, "MEDIUMINT") ||
		strings.HasPrefix(t, "INT") ||
		strings.HasPrefix(t, "INTEGER") ||
		strings.HasPrefix(t, "BIGINT")
}

func buildIntegerDeletePredicate(columnName, value, dataType string) (string, bool) {
	if !isIntegerDeleteType(dataType) {
		return "", false
	}
	v := strings.TrimSpace(value)
	if !integerLiteralPattern.MatchString(v) {
		return "", false
	}
	return fmt.Sprintf("`%s` = %s", columnName, v), true
}

func buildFloatDeletePredicate(columnName, value, dataType string) (string, bool) {
	if !isFloatDeleteType(dataType) {
		return "", false
	}
	fv, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return "", false
	}
	floatLiteral := strconv.FormatFloat(fv, 'f', -1, 64)
	if scale, ok := floatDeleteScaleByType(dataType); ok {
		return fmt.Sprintf("ROUND(`%s`, %d) = ROUND(%s, %d)", columnName, scale, floatLiteral, scale), true
	}
	// Fallback for FLOAT without declared scale:
	// keep both sides as plain numeric comparison to avoid unnecessary CAST in fix SQL.
	return fmt.Sprintf("`%s` = %s", columnName, floatLiteral), true
}

func (my *MysqlDataAbnormalFixStruct) FixInsertSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	//查询该表的列名和列信息
	var (
		insertSql     string
		valuesNameSeq []string
		targetSchema  = my.Schema // 默认使用目标schema
		vlog          string
	)

	vlog = fmt.Sprintf("(%d) Generating INSERT repair statement for %s.%s (target: %s)", logThreadSeq, my.Schema, my.Table, targetSchema)
	global.Wlog.Debug(vlog)

	// 检查ColData是否为空，如果为空，尝试从数据库中查询表的列信息
	if len(my.ColData) == 0 {
		vlog = fmt.Sprintf("(%d) Warning: No column data available for table %s.%s, trying to query from database",
			logThreadSeq, targetSchema, my.Table)
		global.Wlog.Warn(vlog)

		// 从INFORMATION_SCHEMA.COLUMNS中查询表的列信息
		query := "SELECT COLUMN_NAME AS columnName, ORDINAL_POSITION AS columnSeq, COLUMN_TYPE AS dataType FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
		rows, err := db.Query(query, targetSchema, my.Table)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Error: Failed to query column information from database: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			// 如果查询失败，回退到使用临时列名
			rowParts := strings.Split(my.RowData, "/*go actions columnData*/")
			if len(rowParts) == 0 {
				return "", fmt.Errorf("no column data available and empty row data for table %s.%s (mapping: %s->%s)",
					targetSchema, my.Table, my.SourceSchema, my.Schema)
			}

			// 创建临时列数据结构
			tempColData := make([]map[string]string, len(rowParts))
			for i := range rowParts {
				tempColData[i] = map[string]string{
					"columnName": fmt.Sprintf("col_%d", i+1),
					"columnSeq":  strconv.Itoa(i + 1),
					"dataType":   "VARCHAR", // 默认类型
				}
			}
			my.ColData = tempColData
		} else {
			// 解析查询结果
			var columns []map[string]string
			for rows.Next() {
				var columnName, columnSeqStr, dataType string
				if err := rows.Scan(&columnName, &columnSeqStr, &dataType); err != nil {
					vlog = fmt.Sprintf("(%d) Error: Failed to scan column information: %v", logThreadSeq, err)
					global.Wlog.Error(vlog)
					continue
				}
				columns = append(columns, map[string]string{
					"columnName": columnName,
					"columnSeq":  columnSeqStr,
					"dataType":   dataType,
				})
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				vlog = fmt.Sprintf("(%d) Error: Failed to iterate column information: %v", logThreadSeq, rowsErr)
				global.Wlog.Warn(vlog)
			}
			_ = rows.Close()

			if len(columns) > 0 {
				my.ColData = columns
				vlog = fmt.Sprintf("(%d) Successfully queried column information from database for table %s.%s, found %d columns",
					logThreadSeq, targetSchema, my.Table, len(columns))
				global.Wlog.Debug(vlog)
			} else {
				vlog = fmt.Sprintf("(%d) Warning: No column information found in database for table %s.%s, using temporary column names",
					logThreadSeq, targetSchema, my.Table)
				global.Wlog.Warn(vlog)

				// 如果查询结果为空，回退到使用临时列名
				rowParts := strings.Split(my.RowData, "/*go actions columnData*/")
				if len(rowParts) == 0 {
					return "", fmt.Errorf("no column data available and empty row data for table %s.%s (mapping: %s->%s)",
						targetSchema, my.Table, my.SourceSchema, my.Schema)
				}

				// 创建临时列数据结构
				tempColData := make([]map[string]string, len(rowParts))
				for i := range rowParts {
					tempColData[i] = map[string]string{
						"columnName": fmt.Sprintf("col_%d", i+1),
						"columnSeq":  strconv.Itoa(i + 1),
						"dataType":   "VARCHAR", // 默认类型
					}
				}
				my.ColData = tempColData
			}
		}
	}

	//Handle timezone issues with MySQL datetime columns (e.g. 2021-01-23 10:16:29 +0800 CST)
	rowParts := strings.Split(my.RowData, "/*go actions columnData*/")
	for k, v := range rowParts {
		dataType := ""
		if k < len(my.ColData) {
			dataType = my.ColData[k]["dataType"]
		} else {
			vlog = fmt.Sprintf("(%d) Warning: Column index %d exceeds available column data for %s.%s",
				logThreadSeq, k, targetSchema, my.Table)
			global.Wlog.Warn(vlog)
		}
		tmpcolumnName := formatMySQLInsertLiteral(v, dataType)
		valuesNameSeq = append(valuesNameSeq, tmpcolumnName)
	}

	if len(valuesNameSeq) > 0 {
		queryColumn := strings.Join(valuesNameSeq, ",")

		// 从ColData中提取所有列名，包括不可见列
		columnNames := make([]string, 0, len(my.ColData))
		for _, col := range my.ColData {
			if colName, ok := col["columnName"]; ok && colName != "" {
				columnNames = append(columnNames, fmt.Sprintf("`%s`", colName))
			}
		}

		// 如果有列名信息，则生成包含列名的INSERT语句
		if len(columnNames) > 0 {
			insertSql = fmt.Sprintf("INSERT INTO `%s`.`%s`(%s) VALUES(%s);", targetSchema, my.Table, strings.Join(columnNames, ","), queryColumn)
		} else {
			// 如果没有列名信息，回退到原始的VALUES语法
			insertSql = fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES(%s);", targetSchema, my.Table, queryColumn)
		}
	}

	return insertSql, nil
}

// FixUpdateSqlExec generates UPDATE SET <compareColNames> WHERE <pkCols> for columns-mode two-sided rows.
// srcRowData is the source-side row data (values separated by "/*go actions columnData*/") in ColData column order.
// compareColNames are the non-PK source column names used to look up values in ColData.
// srcToDstCol maps source column names to destination column names for the SET clause; nil means names are identical.
// The WHERE clause is built from my.IndexColumn (PK/unique key columns).
func (my *MysqlDataAbnormalFixStruct) FixUpdateSqlExec(db *sql.DB, srcRowData string, compareColNames []string, srcToDstCol map[string]string, logThreadSeq int64) (string, error) {
	if len(compareColNames) == 0 || len(my.ColData) == 0 {
		return "", nil
	}

	// Build col name → position map from ColData.
	// Keys are lowercased so lookups are case-insensitive across DB metadata and user config.
	colPosMap := make(map[string]int, len(my.ColData))
	colTypeMap := make(map[string]string, len(my.ColData))
	for i, col := range my.ColData {
		if name, ok := col["columnName"]; ok {
			colPosMap[strings.ToLower(name)] = i
			colTypeMap[strings.ToLower(name)] = col["dataType"]
		}
	}

	rowParts := strings.Split(srcRowData, "/*go actions columnData*/")

	// Helper: get formatted literal for a named column (case-insensitive lookup)
	colLiteral := func(colName string) (string, bool) {
		key := strings.ToLower(colName)
		pos, ok := colPosMap[key]
		if !ok || pos >= len(rowParts) {
			return "", false
		}
		return formatMySQLInsertLiteral(rowParts[pos], colTypeMap[key]), true
	}

	// Build SET clause from compareColNames (non-PK columns to update).
	// Use srcToDstCol to translate source column names to destination column names for the SET key.
	// srcToDstCol keys are lowercased at construction time; look up with ToLower for case-insensitive mapping.
	//
	// Design note on error strategy: a missing SET column is treated as a degraded but recoverable
	// situation — the UPDATE is still emitted with the remaining columns, and a Warn is logged so the
	// operator can investigate. This is intentional: skipping one non-PK column causes a partial
	// update, which is visible and correctable on the next checksum cycle.
	// By contrast, a missing PK column (see WHERE clause below) is fatal: without a complete WHERE
	// predicate the UPDATE would match unintended rows, so we return an error immediately.
	var setClauses []string
	for _, srcColName := range compareColNames {
		lit, ok := colLiteral(srcColName)
		if !ok {
			global.Wlog.Warn(fmt.Sprintf("(%d) FixUpdateSqlExec: column %q not found in ColData for %s.%s; skipping SET clause",
				logThreadSeq, srcColName, my.Schema, my.Table))
			continue
		}
		dstColName := srcColName
		if srcToDstCol != nil {
			if mapped, exists := srcToDstCol[strings.ToLower(srcColName)]; exists {
				dstColName = mapped
			}
		}
		setClauses = append(setClauses, fmt.Sprintf("%s=%s", mysqlQuoteIdent(dstColName), lit))
	}
	if len(setClauses) == 0 {
		return "", nil
	}

	// Build WHERE clause from IndexColumn (PK columns).
	// colLiteral looks up values case-insensitively (ToLower), so the case of pkCol only
	// affects the quoted identifier emitted into the SQL string — not the value lookup.
	// MySQL/MariaDB evaluate quoted identifiers case-insensitively, so this is safe for
	// the current target set. If support for case-sensitive databases (e.g. PostgreSQL) is
	// added in the future, IndexColumn entries must be normalised to match the target's
	// actual column definition before being passed here.
	var whereClauses []string
	for _, pkCol := range my.IndexColumn {
		lit, ok := colLiteral(pkCol)
		if !ok {
			return "", fmt.Errorf("(%d) FixUpdateSqlExec: PK column %q not found in row data for %s.%s",
				logThreadSeq, pkCol, my.Schema, my.Table)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s=%s", mysqlQuoteIdent(pkCol), lit))
	}
	if len(whereClauses) == 0 {
		return "", fmt.Errorf("(%d) FixUpdateSqlExec: no PK columns (IndexColumn) configured for %s.%s", logThreadSeq, my.Schema, my.Table)
	}

	return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s;",
		mysqlQuoteIdent(my.Schema), mysqlQuoteIdent(my.Table),
		strings.Join(setClauses, ","),
		strings.Join(whereClauses, " AND ")), nil
}

/*
MySQL generate delete repair statement
*/
func (my *MysqlDataAbnormalFixStruct) FixDeleteSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	var (
		deleteSql, deleteSqlWhere string
		ad                        = make(map[string]int)
		acc                       = make(map[string]string) //判断特殊数据类型
		vlog                      string
	)
	var targetSchema = my.Schema // 默认使用目标schema

	// 检查表是否有主键，如果有，强制使用主键作为条件
	hasPrimaryKey := false
	primaryKeyColumns := []string{}
	tableKey := fmt.Sprintf("%s.%s", targetSchema, my.Table)

	// 先检查缓存（使用读锁）
	tablePrimaryKeyMutex.RLock()
	if columns, exists := TablePrimaryKeyColumns[tableKey]; exists {
		tablePrimaryKeyMutex.RUnlock()
		hasPrimaryKey = len(columns) > 0
		primaryKeyColumns = columns
	} else {
		tablePrimaryKeyMutex.RUnlock()
		// 查询表的主键信息
		query := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION"
		rows, err := db.Query(query, targetSchema, my.Table)
		if err == nil {
			for rows.Next() {
				var columnName string
				if err := rows.Scan(&columnName); err == nil {
					hasPrimaryKey = true
					primaryKeyColumns = append(primaryKeyColumns, columnName)
				}
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				vlog = fmt.Sprintf("(%d) Failed to iterate primary key rows for %s.%s: %v", logThreadSeq, targetSchema, my.Table, rowsErr)
				global.Wlog.Warn(vlog)
			}
			_ = rows.Close()
		}
		// 缓存结果（使用写锁）
		tablePrimaryKeyMutex.Lock()
		TablePrimaryKeyColumns[tableKey] = primaryKeyColumns
		tablePrimaryKeyMutex.Unlock()
	}

	// 如果表有主键，强制使用主键作为条件
	if hasPrimaryKey && len(primaryKeyColumns) > 0 {
		my.IndexType = "pri"
		my.IndexColumn = primaryKeyColumns
		vlog = fmt.Sprintf("(%d) Found primary key for table %s.%s: %v, forcing IndexType to 'pri'", logThreadSeq, targetSchema, my.Table, primaryKeyColumns)
		global.Wlog.Debug(vlog)
	} else {
		// 如果没有主键，检查是否有唯一键
		hasUniqueKey := false
		uniqueKeyColumns := []string{}

		// 查询表的唯一键信息
		uniqueQuery := "SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0 AND INDEX_NAME != 'PRIMARY' ORDER BY INDEX_NAME, SEQ_IN_INDEX"
		uniqueRows, uniqueErr := db.Query(uniqueQuery, targetSchema, my.Table)
		if uniqueErr == nil {
			// 使用map来按索引名称分组列
			uniqueIndices := make(map[string][]string)

			for uniqueRows.Next() {
				var indexName, columnName string
				if uniqueErr := uniqueRows.Scan(&indexName, &columnName); uniqueErr == nil {
					uniqueIndices[indexName] = append(uniqueIndices[indexName], columnName)
				}
			}
			if rowsErr := uniqueRows.Err(); rowsErr != nil {
				vlog = fmt.Sprintf("(%d) Failed to iterate unique index rows for %s.%s: %v", logThreadSeq, targetSchema, my.Table, rowsErr)
				global.Wlog.Warn(vlog)
			}

			// 如果有唯一键，使用第一个唯一键
			for indexName, columns := range uniqueIndices {
				hasUniqueKey = true
				uniqueKeyColumns = columns
				vlog = fmt.Sprintf("(%d) Found unique key '%s' for table %s.%s: %v, forcing IndexType to 'uni'", logThreadSeq, indexName, targetSchema, my.Table, uniqueKeyColumns)
				global.Wlog.Debug(vlog)
				break // 只使用第一个唯一键
			}
			_ = uniqueRows.Close()
		}

		// 如果表有唯一键，强制使用唯一键作为条件
		if hasUniqueKey && len(uniqueKeyColumns) > 0 {
			my.IndexType = "uni"
			my.IndexColumn = uniqueKeyColumns
		} else {
			// 如果既没有主键也没有唯一键，则设置为mul类型，并使用所有列作为条件
			my.IndexType = "mul"

			// 获取表的所有列名
			allColumnsQuery := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
			allColumnsRows, allColumnsErr := db.Query(allColumnsQuery, targetSchema, my.Table)
			if allColumnsErr == nil {
				allColumns := []string{}
				for allColumnsRows.Next() {
					var columnName string
					if err := allColumnsRows.Scan(&columnName); err == nil {
						allColumns = append(allColumns, columnName)
					}
				}
				if rowsErr := allColumnsRows.Err(); rowsErr != nil {
					vlog = fmt.Sprintf("(%d) Failed to iterate all columns for %s.%s: %v", logThreadSeq, targetSchema, my.Table, rowsErr)
					global.Wlog.Warn(vlog)
				}

				if len(allColumns) > 0 {
					my.IndexColumn = allColumns
					vlog = fmt.Sprintf("(%d) No primary or unique key found for table %s.%s, using all columns as conditions: %v", logThreadSeq, targetSchema, my.Table, allColumns)
					global.Wlog.Debug(vlog)
				}
				_ = allColumnsRows.Close()
			}
		}
	}

	// 确保ColData不为空
	if len(my.ColData) == 0 {
		vlog = fmt.Sprintf("(%d) Warning: No column data available for table %s.%s when generating DELETE statement",
			logThreadSeq, targetSchema, my.Table)
		global.Wlog.Warn(vlog)

		// 如果IndexColumn有值，尝试从中创建临时列数据
		if len(my.IndexColumn) > 0 {
			tempColData := make([]map[string]string, len(my.IndexColumn))
			for i, colName := range my.IndexColumn {
				tempColData[i] = map[string]string{
					"columnName": colName,
					"columnSeq":  strconv.Itoa(i + 1),
					"dataType":   "VARCHAR", // 默认类型
				}
			}
			my.ColData = tempColData
			vlog = fmt.Sprintf("(%d) Created temporary column structure from index columns for table %s.%s",
				logThreadSeq, targetSchema, my.Table)
			global.Wlog.Debug(vlog)
		} else if my.RowData != "" {
			// 从行数据中推断列数量
			rowParts := strings.Split(my.RowData, "/*go actions columnData*/")
			if len(rowParts) > 0 {
				tempColData := make([]map[string]string, len(rowParts))
				for i := range rowParts {
					tempColData[i] = map[string]string{
						"columnName": fmt.Sprintf("col_%d", i+1),
						"columnSeq":  strconv.Itoa(i + 1),
						"dataType":   "VARCHAR", // 默认类型
					}
				}
				my.ColData = tempColData
				vlog = fmt.Sprintf("(%d) Created temporary column structure with %d columns from row data for table %s.%s",
					logThreadSeq, len(my.ColData), targetSchema, my.Table)
				global.Wlog.Debug(vlog)
			}
		}

		// 如果仍然为空，返回错误
		if len(my.ColData) == 0 {
			return "", fmt.Errorf("no column data available for table %s.%s and cannot infer from available information",
				targetSchema, my.Table)
		}
	}

	colData := my.ColData
	for _, i := range colData {
		cls, _ := strconv.Atoi(fmt.Sprintf("%s", i["columnSeq"]))
		ad[i["columnName"]] = cls
		if strings.HasPrefix(i["dataType"], "double(") {
			acc["double"] = i["columnName"]
		}
	}
	vlog = fmt.Sprintf("(%d) Generating DELETE repair statement for %s.%s (target: %s)", logThreadSeq, my.Schema, my.Table, targetSchema)
	global.Wlog.Debug(vlog)

	if my.IndexType == "mul" {
		var FB, AS []string

		// 优先使用IndexColumn中的列（如果有的话）
		if len(my.IndexColumn) > 0 {
			FB = my.IndexColumn
			vlog = fmt.Sprintf("(%d) Using columns from IndexColumn for table %s.%s: %v", logThreadSeq, targetSchema, my.Table, FB)
			global.Wlog.Debug(vlog)
		} else {
			// 否则从colData中获取列名
			for _, i := range colData {
				if colName, ok := i["columnName"]; ok {
					FB = append(FB, colName)
				}
			}
			vlog = fmt.Sprintf("(%d) Using columns from colData for table %s.%s: %v", logThreadSeq, targetSchema, my.Table, FB)
			global.Wlog.Debug(vlog)
		}

		if len(FB) == 0 {
			// 确定正确的错误信息中应该使用的schema名称
			errorSchema := targetSchema
			if my.Schema != "" {
				// 如果是目标端操作，使用目标schema
				errorSchema = my.Schema
			}
			return "", fmt.Errorf("no valid columns found for table %s.%s (mapping: %s->%s)",
				errorSchema, my.Table, my.SourceSchema, my.Schema)
		}

		// 创建一个映射，将列名映射到列序号和值
		columnMap := make(map[string]string)
		// 不进行额外的字符串替换处理，直接分割原始行数据
		rowParts := strings.Split(my.RowData, "/*go actions columnData*/")

		// 首先尝试使用colData中的列序号信息来映射值
		for _, col := range colData {
			colName, ok1 := col["columnName"]
			colSeqStr, ok2 := col["columnSeq"]
			if !ok1 || !ok2 {
				continue
			}

			colSeq, err := strconv.Atoi(colSeqStr)
			if err != nil || colSeq <= 0 || colSeq > len(rowParts) {
				continue
			}

			// 列序号是1-based，但数组索引是0-based
			// 对于DATA列，直接使用原始值，不做任何处理
			columnMap[colName] = rowParts[colSeq-1]
		}

		// 如果没有足够的映射，尝试直接按顺序映射
		if len(columnMap) < len(FB) && len(rowParts) >= len(FB) {
			for i, colName := range FB {
				if _, exists := columnMap[colName]; !exists && i < len(rowParts) {
					// 直接使用原始值，不做任何处理
					columnMap[colName] = rowParts[i]
				}
			}
		}

		// 生成WHERE条件
		for _, colName := range FB {
			if value, ok := columnMap[colName]; ok {
				dataType := lookupColumnDataType(colData, colName)
				if value == "<nil>" {
					AS = append(AS, fmt.Sprintf("`%s` IS NULL", colName))
				} else if value == "<entry>" {
					AS = append(AS, fmt.Sprintf("`%s` = ''", colName))
				} else if predicate, ok := buildIntegerDeletePredicate(colName, value, dataType); ok {
					AS = append(AS, predicate)
				} else if predicate, ok := buildFloatDeletePredicate(colName, value, dataType); ok {
					AS = append(AS, predicate)
				} else if value == acc["double"] {
					AS = append(AS, fmt.Sprintf("CONCAT(`%s`,'') = '%s'", colName, value))
				} else {
					// 确保DELETE语句使用目标端的实际数据格式
					// 对于WHERE条件，使用目标端数据的原始格式，包括尾部空格
					// 这是因为我们需要精确匹配目标端的数据，删除正确的行
					// 生成WHERE条件时，使用目标端的原始数据格式，包括尾部空格
					AS = append(AS, fmt.Sprintf("`%s` = '%s'", colName, escapeSQLString(value)))
				}
			}
		}

		if len(AS) > 0 {
			deleteSqlWhere = strings.Join(AS, " AND ")
			vlog = fmt.Sprintf("(%d) Generated WHERE condition for table %s.%s: %s", logThreadSeq, targetSchema, my.Table, deleteSqlWhere)
			global.Wlog.Debug(vlog)
		} else {
			vlog = fmt.Sprintf("(%d) Failed to generate WHERE condition for table %s.%s: no valid column-value pairs", logThreadSeq, targetSchema, my.Table)
			global.Wlog.Warn(vlog)
		}
	}

	vlog = fmt.Sprintf("(%d) Generating DELETE repair statement using unique index for %s.%s", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Debug(vlog)

	if my.IndexType == "pri" || my.IndexType == "uni" {
		// 添加对空IndexColumn的检查
		if len(my.IndexColumn) == 0 {
			return "", fmt.Errorf("no index columns defined for table %s.%s", targetSchema, my.Table)
		}

		// 创建一个映射，将列名映射到列序号和值
		columnMap := make(map[string]string)
		rowParts := strings.Split(my.RowData, "/*go actions columnData*/")

		for i, col := range colData {
			colName, ok := col["columnName"]
			if !ok || i >= len(rowParts) {
				continue
			}
			columnMap[colName] = rowParts[i]
		}

		// 只使用索引列（主键或唯一键）作为WHERE条件
		var AS []string
		for _, colName := range my.IndexColumn {
			if value, ok := columnMap[colName]; ok {
				dataType := lookupColumnDataType(colData, colName)
				if value == "<nil>" {
					AS = append(AS, fmt.Sprintf("`%s` IS NULL", colName))
				} else if value == "<entry>" {
					AS = append(AS, fmt.Sprintf("`%s` = ''", colName))
				} else if predicate, ok := buildIntegerDeletePredicate(colName, value, dataType); ok {
					AS = append(AS, predicate)
				} else if predicate, ok := buildFloatDeletePredicate(colName, value, dataType); ok {
					AS = append(AS, predicate)
				} else if value == acc["double"] {
					AS = append(AS, fmt.Sprintf("CONCAT(`%s`,'') = '%s'", colName, value))
				} else {
					// 确保DELETE语句使用目标端的实际数据格式
					// 对于WHERE条件，使用目标端数据的原始格式，包括尾部空格
					// 这是因为我们需要精确匹配目标端的数据，删除正确的行
					// 生成WHERE条件时，使用目标端的原始数据格式，包括尾部空格
					AS = append(AS, fmt.Sprintf("`%s` = '%s'", colName, escapeSQLString(value)))
				}
			}
		}

		if len(AS) > 0 {
			deleteSqlWhere = strings.Join(AS, " AND ")
		}
	}
	if len(deleteSqlWhere) > 0 {
		// 生成数据库连接的唯一标识符
		dbPointer := fmt.Sprintf("%p", db)

		// 检查缓存，避免重复执行USE语句（使用读锁）
		databaseCacheMutex.RLock()
		currentDB, exists := CurrentDatabaseCache[dbPointer]
		databaseCacheMutex.RUnlock()

		if !exists || currentDB != targetSchema {
			// 确保目标数据库存在
			if _, err := db.Exec(fmt.Sprintf("USE `%s`", targetSchema)); err != nil {
				return "", fmt.Errorf("target database %s does not exist", targetSchema)
			}
			// 更新缓存（使用写锁）
			databaseCacheMutex.Lock()
			CurrentDatabaseCache[dbPointer] = targetSchema
			databaseCacheMutex.Unlock()
		}

		// 统计目标端中与当前条件匹配的记录数量，以确定合适的LIMIT值
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` WHERE %s", targetSchema, my.Table, deleteSqlWhere)
		var matchCount int
		if err := db.QueryRow(countQuery).Scan(&matchCount); err != nil {
			// 如果统计失败，默认使用LIMIT 1
			vlog = fmt.Sprintf("(%d) Failed to count matching records: %v, using LIMIT 1", logThreadSeq, err)
			global.Wlog.Warn(vlog)
			matchCount = 1
		}

		// 根据实际匹配数量设置LIMIT值
		limit := 1
		// 对于唯一索引中的NULL值，不应该删除所有匹配记录，只删除多余的部分
		// 在MySQL中，唯一索引允许多个NULL值，因为NULL不等于NULL
		// 但是对于没有唯一索引的表，即使有NULL值，也应该删除所有匹配的记录
		if matchCount > 1 && (my.IndexType != "pri" && my.IndexType != "uni" || !strings.Contains(deleteSqlWhere, "IS NULL")) {
			limit = matchCount
		}

		// 判断是否是主键、唯一键或隐藏主键 my_row_id
		isUniqueKey := my.IndexType == "pri" || my.IndexType == "uni" || (len(my.IndexColumn) == 1 && my.IndexColumn[0] == "my_row_id")

		// 修复：对于唯一索引中的 NULL 值，因为唯一索引允许多个 NULL 值，
		// 所以即使是唯一字段，在删除包含 NULL 的记录时也必须加上 LIMIT N 约束，
		// 以免错误地删除目标端所有该字段为 NULL 的其他记录
		if isUniqueKey && !strings.Contains(deleteSqlWhere, "IS NULL") {
			deleteSql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;", targetSchema, my.Table, deleteSqlWhere)
		} else {
			deleteSql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s LIMIT %d;", targetSchema, my.Table, deleteSqlWhere, limit)
		}
	} else {
		return "", fmt.Errorf("failed to generate DELETE statement for table %s.%s: no valid conditions", targetSchema, my.Table)
	}
	return deleteSql, nil
}

// 从外键DDL定义中提取引用表和字段信息
func extractForeignKeyInfo(ddlDefinition, fkName string) (string, string) {
	// 如果没有提供DDL定义，则返回空
	if ddlDefinition == "" {
		return "", ""
	}

	// 查找REFERENCES关键字
	lowerDDL := strings.ToLower(ddlDefinition)
	refIndex := strings.Index(lowerDDL, "references")
	if refIndex == -1 {
		return "", ""
	}

	// 提取REFERENCES之后的内容
	afterRef := strings.TrimSpace(ddlDefinition[refIndex+len("references"):])

	// 提取引用表名（可能包含schema前缀）
	var refTable, refColumn string
	parts := strings.Split(afterRef, "(")
	if len(parts) >= 2 {
		// 提取引用表名，去掉可能的反引号和schema前缀
		refTablePart := strings.TrimSpace(parts[0])
		refTablePart = strings.Trim(refTablePart, "`")

		// 处理包含schema的情况，如 `sbtest`.`tb_dept1`
		if strings.Contains(refTablePart, ".") {
			tableParts := strings.Split(refTablePart, ".")
			refTable = strings.Trim(tableParts[len(tableParts)-1], "`")
		} else {
			refTable = refTablePart
		}

		// 提取引用字段名
		fieldPart := strings.TrimSpace(parts[1])
		fieldEndIndex := strings.Index(fieldPart, ")")
		if fieldEndIndex != -1 {
			refColumn = strings.TrimSpace(fieldPart[:fieldEndIndex])
			refColumn = strings.Trim(refColumn, "`")
		}
	}

	return refTable, refColumn
}

// 从源端数据库获取表的外键定义信息
func (my *MysqlDataAbnormalFixStruct) LoadForeignKeyDefinitions(db *sql.DB, logThreadSeq int64) error {
	var vlog string

	// 初始化外键定义映射
	if my.ForeignKeyDefinitions == nil {
		my.ForeignKeyDefinitions = make(map[string]string)
	}

	// 使用源端schema进行查询
	sourceSchema := my.SourceSchema
	if sourceSchema == "" {
		sourceSchema = my.Schema
	}

	vlog = fmt.Sprintf("(%d) Loading foreign key definitions for table %s.%s from source schema %s",
		logThreadSeq, sourceSchema, my.Table, sourceSchema)
	global.Wlog.Debug(vlog)

	// Query the child-side KEY_COLUMN_USAGE rows directly so composite foreign
	// keys keep their ordinal position and do not cross-join referenced columns.
	query := `
		SELECT 
			kcu.CONSTRAINT_NAME,
			kcu.ORDINAL_POSITION,
			kcu.COLUMN_NAME AS SOURCE_COLUMN_NAME,
			kcu.REFERENCED_TABLE_SCHEMA,
			kcu.REFERENCED_TABLE_NAME,
			kcu.REFERENCED_COLUMN_NAME
		FROM 
			INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
		WHERE 
			kcu.TABLE_SCHEMA = ? 
			AND kcu.TABLE_NAME = ?
			AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY 
			kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
	`

	rows, err := db.Query(query, sourceSchema, my.Table)
	if err != nil {
		vlog = fmt.Sprintf("(%d) Error querying foreign key definitions: %v", logThreadSeq, err)
		global.Wlog.Warn(vlog)
		return err
	}

	fkInfoMap := make(map[string][]foreignKeyColumn)

	for rows.Next() {
		// 使用sql.NullString处理可能为NULL的值
		var constraintName, sourceColumn string
		var ordinalPosition int
		var referencedSchema, referencedTable, referencedColumn sql.NullString

		if err := rows.Scan(&constraintName, &ordinalPosition, &sourceColumn, &referencedSchema, &referencedTable, &referencedColumn); err != nil {
			vlog = fmt.Sprintf("(%d) Error scanning foreign key row: %v", logThreadSeq, err)
			global.Wlog.Warn(vlog)
			continue
		}

		// 将sql.NullString转换为普通string，NULL值转为空字符串
		referencedSchemaStr := ""
		referencedTableStr := ""
		referencedColumnStr := ""
		if referencedSchema.Valid {
			referencedSchemaStr = referencedSchema.String
		}
		if referencedTable.Valid {
			referencedTableStr = referencedTable.String
		}
		if referencedColumn.Valid {
			referencedColumnStr = referencedColumn.String
		}

		// 存储外键信息
		fkInfoMap[constraintName] = append(fkInfoMap[constraintName], foreignKeyColumn{
			ordinalPosition:  ordinalPosition,
			columnName:       sourceColumn,
			referencedSchema: referencedSchemaStr,
			referencedTable:  referencedTableStr,
			referencedColumn: referencedColumnStr,
		})
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		return rowsErr
	}
	_ = rows.Close()

	// 构建完整的外键DDL定义
	for fkName, infoRows := range fkInfoMap {
		fkDDL, ok := buildForeignKeyDDLForFix(fkName, infoRows, sourceSchema)
		if !ok {
			vlog = fmt.Sprintf("(%d) Invalid foreign key info for %s: missing referenced table or column",
				logThreadSeq, fkName)
			global.Wlog.Warn(vlog)
			continue
		}
		my.ForeignKeyDefinitions[fkName] = fkDDL
		vlog = fmt.Sprintf("(%d) Found foreign key: %s", logThreadSeq, fkDDL)
		global.Wlog.Debug(vlog)
	}

	return nil
}

func (my *MysqlDataAbnormalFixStruct) FixAlterIndexSqlExec(e, f []string, si map[string][]string, sourceDrive string, logThreadSeq int64) []string {
	var (
		sqlS         []string
		targetSchema = my.Schema // 使用目标schema（保持原始大小写）
		strsql       string
	)

	// 检查是否需要加载外键定义
	if my.ForeignKeyDefinitions == nil || len(my.ForeignKeyDefinitions) == 0 {
		// 输出警告日志，但继续执行，因为这里没有数据库连接
		vlog := fmt.Sprintf("(%d) Warning: Foreign key definitions not loaded for table %s.%s", logThreadSeq, my.Schema, my.Table)
		global.Wlog.Warn(vlog)
	}

	for _, v := range e {
		var c []string
		for _, vi := range si[v] {
			// 从vi字符串中提取原始列名（格式：columnName/*seq*/indexSeq/*type*/columnType）
			parts := strings.Split(vi, "/*seq*/")
			if len(parts) > 0 {
				// 保留原始列名的大小写
				c = append(c, strings.TrimSpace(parts[0]))
			}
		}

		// 检查是否是外键约束
		isForeignKey := false
		fkDDL := ""
		if my.ForeignKeyDefinitions != nil {
			if ddl, exists := my.ForeignKeyDefinitions[v]; exists {
				isForeignKey = true
				fkDDL = ddl
			}
		}

		// 构建SQL语句
		if isForeignKey {
			// 生成外键约束的SQL
			strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), fkDDL)
			vlog := fmt.Sprintf("(%d) Generating foreign key SQL: %s", logThreadSeq, strsql)
			global.Wlog.Debug(vlog)
		} else {
			// 生成普通索引的SQL
			// MariaDB 使用 IGNORED 关键字隐藏索引，MySQL 使用 INVISIBLE。
			indexHiddenKeyword := "INVISIBLE"
			if my.DestFlavor == global.DatabaseFlavorMariaDB {
				indexHiddenKeyword = "IGNORED"
			}
			var invisibleClause string
			if my.IndexVisibilityMap != nil {
				if visibility, exists := my.IndexVisibilityMap[v]; exists && (strings.EqualFold(visibility, "NO") || strings.EqualFold(visibility, "INVISIBLE") || strings.EqualFold(visibility, "IGNORED")) {
					invisibleClause = " " + indexHiddenKeyword
				}
			}
			switch my.IndexType {
			case "pri":
				strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD PRIMARY KEY(`%s`);", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), strings.Join(c, "`,`"))
			case "uni":
				strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD UNIQUE INDEX %s(`%s`)%s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), mysqlQuoteIdent(v), strings.Join(c, "`,`"), invisibleClause)
			case "mul":
				strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD INDEX %s(`%s`)%s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), mysqlQuoteIdent(v), strings.Join(c, "`,`"), invisibleClause)
			}
		}
		sqlS = append(sqlS, strsql)
	}

	for _, v := range f {
		// 检查是否是外键约束
		isForeignKey := false
		if my.ForeignKeyDefinitions != nil {
			_, isForeignKey = my.ForeignKeyDefinitions[v]
		}

		if isForeignKey {
			// 删除外键约束
			strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP FOREIGN KEY %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), mysqlQuoteIdent(v))
		} else {
			// 处理普通索引、唯一索引和主键索引
			switch my.IndexType {
			case "pri":
				strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP PRIMARY KEY;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table))
			case "uni":
				strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP INDEX %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), mysqlQuoteIdent(v))
			case "mul":
				strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP INDEX %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), mysqlQuoteIdent(v))
			}
		}
		sqlS = append(sqlS, strsql)
	}

	return sqlS
}

// 初始化全局变量
func init() {
	AutoIncrementColumnsWithPrimaryKey = make(map[string]bool)
	DestTableHasPrimaryKey = make(map[string]bool)
	TablePrimaryKeyColumns = make(map[string][]string)
	CurrentDatabaseCache = make(map[string]string)
}

// 检查目标表是否存在主键并更新DestTableHasPrimaryKey映射
func (my *MysqlDataAbnormalFixStruct) CheckDestTableHasPrimaryKey(db *sql.DB, logThreadSeq int64) bool {
	key := fmt.Sprintf("%s.%s", my.Schema, my.Table)

	// 如果已经检查过，直接返回结果（使用读锁）
	tablePrimaryKeyMutex.RLock()
	if hasPK, exists := DestTableHasPrimaryKey[key]; exists {
		tablePrimaryKeyMutex.RUnlock()
		return hasPK
	}
	tablePrimaryKeyMutex.RUnlock()

	// Cache the full primary key column list so later SQL consolidation can
	// safely decide whether DROP PRIMARY KEY becomes redundant.
	query := `
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
ORDER BY ORDINAL_POSITION
`
	rows, err := db.Query(query, my.Schema, my.Table)
	hasPK := false
	primaryKeyColumns := make([]string, 0)
	if err == nil {
		for rows.Next() {
			var columnName string
			if scanErr := rows.Scan(&columnName); scanErr == nil && strings.TrimSpace(columnName) != "" {
				hasPK = true
				primaryKeyColumns = append(primaryKeyColumns, columnName)
			}
		}
		_ = rows.Close()
	}

	// 更新映射（使用写锁）
	tablePrimaryKeyMutex.Lock()
	DestTableHasPrimaryKey[key] = hasPK
	TablePrimaryKeyColumns[key] = primaryKeyColumns
	tablePrimaryKeyMutex.Unlock()

	return hasPK
}

func (my *MysqlDataAbnormalFixStruct) FixAlterColumnSqlDispos(alterType string, columnDataType []string, columnSeq int, lastColumn, curryColumn string, logThreadSeq int64) string {
	var sqlS string
	if len(columnDataType) > 6 {
		directDefinition := strings.TrimSpace(columnDataType[6])
		if directDefinition != "" && !strings.EqualFold(directDefinition, "null") {
			// MODIFY existing primary-key columns should not inline PRIMARY KEY again.
			if strings.EqualFold(alterType, "modify") && shouldSkipInlinePrimaryKeyClause(my.Schema, my.Table, curryColumn) {
				directDefinition = normalizeInlinePrimaryKeyClause(directDefinition)
			}
			columnLocation := ""
			if columnSeq == 0 {
				columnLocation = "FIRST"
			} else if lastColumn != "alterNoAfter" {
				columnLocation = fmt.Sprintf("AFTER `%s`", lastColumn)
			}

			switch alterType {
			case "add":
				if columnLocation != "" {
					return fmt.Sprintf(" ADD COLUMN `%s` %s %s", curryColumn, directDefinition, columnLocation)
				}
				return fmt.Sprintf(" ADD COLUMN `%s` %s", curryColumn, directDefinition)
			case "modify":
				if columnLocation != "" {
					return fmt.Sprintf(" MODIFY COLUMN `%s` %s %s", curryColumn, directDefinition, columnLocation)
				}
				return fmt.Sprintf(" MODIFY COLUMN `%s` %s", curryColumn, directDefinition)
			case "change":
				parts := strings.Split(curryColumn, ":")
				if len(parts) == 2 {
					if columnLocation != "" {
						return fmt.Sprintf(" CHANGE COLUMN `%s` `%s` %s %s", parts[0], parts[1], directDefinition, columnLocation)
					}
					return fmt.Sprintf(" CHANGE COLUMN `%s` `%s` %s", parts[0], parts[1], directDefinition)
				}
				if columnLocation != "" {
					return fmt.Sprintf(" MODIFY COLUMN `%s` %s %s", curryColumn, directDefinition, columnLocation)
				}
				return fmt.Sprintf(" MODIFY COLUMN `%s` %s", curryColumn, directDefinition)
			}
		}
	}

	// 构建属性列表，只添加非空的值
	var attributes []string

	// 预处理数据类型，移除INVISIBLE关键字（如果存在）
	hasInvisible := false
	if strings.Contains(strings.ToUpper(columnDataType[0]), "INVISIBLE") {
		hasInvisible = true
		// 从数据类型中完全移除INVISIBLE关键字，使用大小写不敏感的替换
		columnDataType[0] = strings.ReplaceAll(columnDataType[0], "INVISIBLE", "")
		columnDataType[0] = strings.ReplaceAll(columnDataType[0], "invisible", "")
		// 去除多余的空格
		columnDataType[0] = strings.TrimSpace(columnDataType[0])
		// 处理可能的多个空格情况
		for strings.Contains(columnDataType[0], "  ") {
			columnDataType[0] = strings.ReplaceAll(columnDataType[0], "  ", " ")
		}
	}
	columnDataType[0] = stripDeprecatedZeroFillAttr(columnDataType[0])
	columnDataType[0] = schemacompat.StripMySQLMetadataOnlyExtraTokens(columnDataType[0])
	columnDataType[0] = normalizeMySQLKeywordFunctionsInDefinition(columnDataType[0])

	// 添加数据类型
	attributes = append(attributes, columnDataType[0])

	// 添加字符集
	if columnDataType[1] != "null" {
		attributes = append(attributes, fmt.Sprintf("CHARACTER SET %s", columnDataType[1]))
	}

	// 添加排序规则
	if columnDataType[2] != "null" {
		attributes = append(attributes, fmt.Sprintf("COLLATE %s", columnDataType[2]))
	}

	// 添加NOT NULL约束
	if strings.ToUpper(columnDataType[3]) == "NO" {
		attributes = append(attributes, "NOT NULL")
	}

	// Preserve SQL function defaults and NULL defaults without string quoting.
	if columnDataType[4] != "empty" {
		if defaultClause := formatMySQLColumnDefault(columnDataType[4], strings.ToUpper(columnDataType[3]) != "NO"); defaultClause != "" {
			attributes = append(attributes, defaultClause)
		}
	}

	// 添加COMMENT属性（用于struct模式下列注释修复）
	// 约定："null" 表示无值来源，不强制设置；空字符串会生成 COMMENT '' 以清空目标注释
	if len(columnDataType) > 5 {
		columnComment := columnDataType[5]
		if !strings.EqualFold(columnComment, "null") {
			attributes = append(attributes, fmt.Sprintf("COMMENT '%s'", escapeSQLString(columnComment)))
		}
	}

	// 初始化AutoIncrementColumnsWithPrimaryKey映射
	if AutoIncrementColumnsWithPrimaryKey == nil {
		AutoIncrementColumnsWithPrimaryKey = make(map[string]bool)
	}

	// 检查是否需要设置主键（对于自增列，无论是add还是modify操作）
	hasAutoIncrement := strings.Contains(strings.ToUpper(columnDataType[0]), "AUTO_INCREMENT")
	needInlinePrimaryKey := hasAutoIncrement
	if needInlinePrimaryKey && strings.EqualFold(alterType, "modify") && shouldSkipInlinePrimaryKeyClause(my.Schema, my.Table, curryColumn) {
		needInlinePrimaryKey = false
	}
	if needInlinePrimaryKey {
		// 对于自增列，需要设置为主键
		attributes = append(attributes, "PRIMARY KEY")
		// 标记该列已经设置了主键，避免在索引修复时重复设置
		key := fmt.Sprintf("%s.%s.%s", my.Schema, my.Table, curryColumn)
		AutoIncrementColumnsWithPrimaryKey[key] = true
	}

	// 添加INVISIBLE关键字（如果存在）
	if hasInvisible {
		attributes = append(attributes, "INVISIBLE")
	}

	// 添加列位置
	columnLocation := ""
	if columnSeq == 0 {
		columnLocation = "FIRST"
	} else if lastColumn != "alterNoAfter" {
		columnLocation = fmt.Sprintf("AFTER `%s`", lastColumn)
	}

	// 构建最终SQL
	switch alterType {
	case "add", "modify":
		// 检查是否需要设置主键
		hasPrimaryKeyAttr := false
		for _, attr := range attributes {
			if strings.ToUpper(attr) == "PRIMARY KEY" {
				hasPrimaryKeyAttr = true
				break
			}
		}

		// 只有当目标表存在主键且当前列不是已有单列主键时，才需要删除旧主键。
		// 对已有主键列做 MODIFY 时，保留原主键即可，避免对 generated invisible
		// primary key 这类场景生成多余的 DROP PRIMARY KEY。
		key := fmt.Sprintf("%s.%s", my.Schema, my.Table)
		needDropPrimaryKey := hasPrimaryKeyAttr && DestTableHasPrimaryKey[key]
		if needDropPrimaryKey && strings.EqualFold(alterType, "modify") {
			primaryKeyColumns := cachedPrimaryKeyColumns(my.Schema, my.Table)
			if len(primaryKeyColumns) == 1 && strings.EqualFold(primaryKeyColumns[0], curryColumn) {
				needDropPrimaryKey = false
			}
		}

		// 统一处理ADD和MODIFY操作，确保主键处理逻辑一致
		operation := "ADD COLUMN"
		if alterType == "modify" {
			operation = "MODIFY COLUMN"
		}

		if columnLocation != "" {
			if needDropPrimaryKey {
				// 先删除旧主键，再进行列操作
				sqlS = fmt.Sprintf(" DROP PRIMARY KEY, %s `%s` %s %s", operation, curryColumn, strings.Join(attributes, " "), columnLocation)
			} else {
				sqlS = fmt.Sprintf(" %s `%s` %s %s", operation, curryColumn, strings.Join(attributes, " "), columnLocation)
			}
		} else {
			if needDropPrimaryKey {
				sqlS = fmt.Sprintf(" DROP PRIMARY KEY, %s `%s` %s", operation, curryColumn, strings.Join(attributes, " "))
			} else {
				sqlS = fmt.Sprintf(" %s `%s` %s", operation, curryColumn, strings.Join(attributes, " "))
			}
		}
	case "drop":
		sqlS = fmt.Sprintf(" DROP COLUMN `%s`", curryColumn)
	case "change":
		// 对于CHANGE操作，需要原始列名和新列名
		// 假设curryColumn格式为"原始列名:新列名"
		parts := strings.Split(curryColumn, ":")
		if len(parts) == 2 {
			originalCol := parts[0]
			newCol := parts[1]
			if columnLocation != "" {
				sqlS = fmt.Sprintf(" CHANGE COLUMN `%s` `%s` %s %s", originalCol, newCol, strings.Join(attributes, " "), columnLocation)
			} else {
				sqlS = fmt.Sprintf(" CHANGE COLUMN `%s` `%s` %s", originalCol, newCol, strings.Join(attributes, " "))
			}
		} else {
			// 如果格式不正确，降级为MODIFY
			if columnLocation != "" {
				sqlS = fmt.Sprintf(" MODIFY COLUMN `%s` %s %s", curryColumn, strings.Join(attributes, " "), columnLocation)
			} else {
				sqlS = fmt.Sprintf(" MODIFY COLUMN `%s` %s", curryColumn, strings.Join(attributes, " "))
			}
		}
	}
	return sqlS
}
func (my *MysqlDataAbnormalFixStruct) FixAlterColumnSqlGenerate(modifyColumn []string, logThreadSeq int64) []string {
	var (
		alterSql     []string
		targetSchema = my.Schema // 使用目标schema（保持原始大小写）
	)

	if len(modifyColumn) > 0 {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), strings.Join(modifyColumn, ",")))
	}
	return alterSql
}

// FixAlterColumnAndIndexSqlGenerate 合并列修复和索引修复操作，生成单个ALTER TABLE语句
func (my *MysqlDataAbnormalFixStruct) FixAlterColumnAndIndexSqlGenerate(columnOperations, indexOperations []string, logThreadSeq int64) []string {
	var (
		alterSql     []string
		targetSchema = my.Schema // 使用目标schema（保持原始大小写）
	)

	// 初始化AutoIncrementColumnsWithPrimaryKey映射
	if AutoIncrementColumnsWithPrimaryKey == nil {
		AutoIncrementColumnsWithPrimaryKey = make(map[string]bool)
	}

	// Filter index operations before combining them into one ALTER TABLE.
	filteredIndexOperations := make([]string, 0)
	for _, op := range indexOperations {
		// 检查是否是添加主键的操作
		if strings.Contains(strings.ToUpper(op), "ADD PRIMARY KEY") {
			// 尝试提取列名
			// 格式可能是：ALTER TABLE `schema`.`table` ADD PRIMARY KEY(`column`);
			startIdx := strings.Index(strings.ToUpper(op), "ADD PRIMARY KEY(")
			if startIdx != -1 {
				startIdx += len("ADD PRIMARY KEY(")
				endIdx := strings.Index(op[startIdx:], ")")
				if endIdx != -1 {
					colName := strings.TrimSpace(op[startIdx : startIdx+endIdx])
					// 去除反引号
					colName = strings.Trim(colName, "`")
					key := fmt.Sprintf("%s.%s.%s", my.Schema, my.Table, colName)
					// 如果该列已经在添加时设置为主键，则跳过此索引操作
					if _, exists := AutoIncrementColumnsWithPrimaryKey[key]; !exists {
						filteredIndexOperations = append(filteredIndexOperations, op)
					}
				} else {
					// 无法解析列名，保留原始操作
					filteredIndexOperations = append(filteredIndexOperations, op)
				}
			} else {
				// 不是标准格式，保留原始操作
				filteredIndexOperations = append(filteredIndexOperations, op)
			}
		} else {
			// 不是添加主键的操作，保留
			filteredIndexOperations = append(filteredIndexOperations, op)
		}
	}
	filteredIndexOperations = my.filterRedundantDropPrimaryKeyOperations(columnOperations, filteredIndexOperations)

	// 合并所有操作
	var allOperations []string
	allOperations = append(allOperations, columnOperations...)
	allOperations = append(allOperations, filteredIndexOperations...)

	if len(allOperations) > 0 {
		// 提取操作内容（去除ALTER TABLE前缀和分号）
		var operationContents []string
		for _, op := range allOperations {
			content := normalizeAlterOperationContent(op)
			if content != "" {
				operationContents = append(operationContents, content)
			}
		}

		if len(operationContents) > 0 {
			// 生成单个ALTER TABLE语句，包含所有操作
			alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), strings.Join(operationContents, ", ")))

			// 添加调试日志
			vlog := fmt.Sprintf("(%d) Generated combined ALTER TABLE SQL for %s.%s: %d column operations, %d index operations",
				logThreadSeq, targetSchema, my.Table, len(columnOperations), len(indexOperations))
			global.Wlog.Debug(vlog)
		}
	}

	return alterSql
}

func normalizeAlterOperationContent(op string) string {
	op = strings.TrimSpace(op)
	if op == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToUpper(op), "ALTER TABLE") {
		// 用正则提取操作片段，支持含空格的反引号标识符（BUG-5 修复）
		m := alterTablePrefixRe.FindStringSubmatch(op)
		if m != nil {
			return strings.TrimSpace(m[1])
		}
	}
	return strings.TrimSpace(strings.TrimSuffix(op, ";"))
}

func extractDroppedColumnNameFromAlterClause(op string) string {
	clause := normalizeAlterOperationContent(op)
	if clause == "" || !strings.HasPrefix(strings.ToUpper(clause), "DROP COLUMN") {
		return ""
	}

	rest := strings.TrimSpace(clause[len("DROP COLUMN"):])
	if rest == "" {
		return ""
	}
	if strings.HasPrefix(rest, "`") {
		end := strings.Index(rest[1:], "`")
		if end == -1 {
			return ""
		}
		return rest[1 : end+1]
	}
	parts := strings.Fields(rest)
	if len(parts) == 0 {
		return ""
	}
	return strings.Trim(parts[0], "`")
}

func collectDroppedColumns(columnOperations []string) map[string]struct{} {
	droppedColumns := make(map[string]struct{})
	for _, op := range columnOperations {
		columnName := extractDroppedColumnNameFromAlterClause(op)
		if strings.TrimSpace(columnName) == "" {
			continue
		}
		droppedColumns[strings.ToUpper(columnName)] = struct{}{}
	}
	return droppedColumns
}

func cachedPrimaryKeyColumns(schema, table string) []string {
	tableKey := fmt.Sprintf("%s.%s", schema, table)
	tablePrimaryKeyMutex.RLock()
	defer tablePrimaryKeyMutex.RUnlock()
	if columns, exists := TablePrimaryKeyColumns[tableKey]; exists {
		copied := make([]string, len(columns))
		copy(copied, columns)
		return copied
	}
	return nil
}

func containsPrimaryKeyColumn(primaryKeyColumns []string, column string) bool {
	trimmedColumn := strings.TrimSpace(column)
	if trimmedColumn == "" {
		return false
	}
	for _, pkColumn := range primaryKeyColumns {
		if strings.EqualFold(strings.TrimSpace(pkColumn), trimmedColumn) {
			return true
		}
	}
	return false
}

func shouldSkipInlinePrimaryKeyClause(schema, table, column string) bool {
	primaryKeyColumns := cachedPrimaryKeyColumns(schema, table)
	if len(primaryKeyColumns) == 0 {
		return false
	}
	return containsPrimaryKeyColumn(primaryKeyColumns, column)
}

func normalizeInlinePrimaryKeyClause(definition string) string {
	cleaned := inlinePrimaryKeyPattern.ReplaceAllString(definition, "")
	normalized := strings.Join(strings.Fields(cleaned), " ")
	return strings.TrimSpace(normalized)
}

func (my *MysqlDataAbnormalFixStruct) filterRedundantDropPrimaryKeyOperations(columnOperations, indexOperations []string) []string {
	droppedColumns := collectDroppedColumns(columnOperations)
	if len(droppedColumns) == 0 {
		return indexOperations
	}

	primaryKeyColumns := cachedPrimaryKeyColumns(my.Schema, my.Table)
	if len(primaryKeyColumns) != 1 {
		return indexOperations
	}
	if _, exists := droppedColumns[strings.ToUpper(primaryKeyColumns[0])]; !exists {
		return indexOperations
	}

	filtered := make([]string, 0, len(indexOperations))
	for _, op := range indexOperations {
		clause := normalizeAlterOperationContent(op)
		if strings.EqualFold(strings.TrimSpace(clause), "DROP PRIMARY KEY") {
			continue
		}
		filtered = append(filtered, op)
	}
	return filtered
}

// FixAlterIndexSqlGenerate 合并索引操作，生成单个ALTER TABLE语句
func (my *MysqlDataAbnormalFixStruct) FixAlterIndexSqlGenerate(indexOperations []string, logThreadSeq int64) []string {
	var (
		alterSql     []string
		targetSchema = my.Schema // 使用目标schema（保持原始大小写）
	)

	if len(indexOperations) > 0 {
		// 提取操作内容（去除ALTER TABLE前缀和分号）
		var operationContents []string
		for _, op := range indexOperations {
			content := normalizeAlterOperationContent(op)
			if content != "" {
				operationContents = append(operationContents, content)
			}
		}

		if len(operationContents) > 0 {
			// 生成单个ALTER TABLE语句，包含所有索引操作
			alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), strings.Join(operationContents, ", ")))

			// 添加调试日志
			vlog := fmt.Sprintf("(%d) Generated combined ALTER TABLE SQL for %s.%s: %d index operations",
				logThreadSeq, targetSchema, my.Table, len(indexOperations))
			global.Wlog.Debug(vlog)
		}
	}

	return alterSql
}

// FixTableCharsetSqlGenerate 生成表级别字符集转换的SQL语句
func (my *MysqlDataAbnormalFixStruct) FixTableCharsetSqlGenerate(charset, collation string, logThreadSeq int64) []string {
	var (
		alterSql     []string
		targetSchema = my.Schema // 默认使用目标schema
	)

	// 防护空 charset：当 LEFT JOIN COLLATIONS 失败时 charset 可能为空，
	// 此时从 collation 名推断 charset，最终兜底为 utf8mb4。
	trimmedCharset := strings.TrimSpace(charset)
	if trimmedCharset == "" {
		trimmedCharset = schemacompat.InferCharsetFromCollation(collation)
		if trimmedCharset == "" {
			trimmedCharset = "utf8mb4"
		}
		if global.Wlog != nil {
			vlog := fmt.Sprintf("(%d) Table charset was empty, inferred as %s from collation %s for %s.%s",
				logThreadSeq, trimmedCharset, collation, targetSchema, my.Table)
			global.Wlog.Warn(vlog)
		}
	}

	// 生成表级别字符集转换的SQL语句
	if strings.TrimSpace(collation) == "" {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s CONVERT TO CHARACTER SET %s;",
			mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), trimmedCharset))
	} else {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s CONVERT TO CHARACTER SET %s COLLATE %s;",
			mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), trimmedCharset, collation))
	}

	// 添加日志，方便调试
	vlog := fmt.Sprintf("(%d) Generated table charset conversion SQL: %s", logThreadSeq, alterSql[0])
	if global.Wlog != nil {
		global.Wlog.Debug(vlog)
	}

	return alterSql
}

// FixTableAutoIncrementSqlGenerate generates table-level AUTO_INCREMENT fix SQL.
func (my *MysqlDataAbnormalFixStruct) FixTableAutoIncrementSqlGenerate(nextValue int64, logThreadSeq int64) []string {
	var (
		alterSql     []string
		targetSchema = my.Schema
	)

	alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s AUTO_INCREMENT=%d;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), nextValue))

	vlog := fmt.Sprintf("(%d) Generated table AUTO_INCREMENT SQL: %s", logThreadSeq, alterSql[0])
	if global.Wlog != nil {
		global.Wlog.Debug(vlog)
	}

	return alterSql
}


// 包级变量，用于存储已写入文件的SQL语句，实现跨函数调用的去重
var writtenSqlMap sync.Map

// 包级变量，用于跟踪是否有修复SQL被写入
var hasFixSqlWritten bool

// WriteFixIfNeededFile writes fix SQLs to an opened *os.File when datafix is "file"
// dstDSN 参数用于获取字符集设置
func WriteFixIfNeededFile(datafix string, sfile *os.File, sqls []string, logThreadSeq int64, dstDSN ...string) error {
	if !strings.EqualFold(datafix, "file") || sfile == nil || len(sqls) == 0 {
		return nil
	}

	// 过滤多余的ADD PRIMARY KEY语句
	filteredSqls := filterRedundantPrimaryKeyStatements(sqls)

	// 过滤重复的SQL语句
	var uniqueSqls []string
	for _, sql := range filteredSqls {
		// 去除首尾空白字符进行比较
		trimmedSql := strings.TrimSpace(sql)
		if trimmedSql == "" {
			continue
		}

		// DELIMITER 及 charset/collation 会话变量 SET 语句是每个文件必须独立包含的控制语句，跳过去重检查
		upperTrimmed := strings.ToUpper(trimmedSql)
		if strings.HasPrefix(upperTrimmed, "DELIMITER ") ||
			strings.HasPrefix(upperTrimmed, "SET CHARACTER_SET_CLIENT") ||
			strings.HasPrefix(upperTrimmed, "SET COLLATION_CONNECTION") ||
			strings.HasPrefix(upperTrimmed, "SET COLLATION_DATABASE") {
			uniqueSqls = append(uniqueSqls, sql)
			continue
		}

		// 使用sync.Map检查SQL是否已存在
		if _, loaded := writtenSqlMap.LoadOrStore(trimmedSql, true); !loaded {
			uniqueSqls = append(uniqueSqls, sql)
		}
	}

	w := bufio.NewWriter(sfile)

	// 检查文件是否为空，为空则添加必要的前置语句
	fileInfo, err := sfile.Stat()
	if err == nil && fileInfo.Size() == 0 {
		// 从dstDSN参数中获取charset值，如果没有提供则使用默认值utf8mb4
		charset := "utf8mb4"
		if len(dstDSN) > 0 && dstDSN[0] != "" {
			charset = global.ExtractCharsetFromDSN(dstDSN[0])
		}

		// 添加必要的前置语句
		preSqls := global.BuildMySQLSessionPreamble(charset)

		for _, preSql := range preSqls {
			if _, err := w.WriteString(preSql + "\n"); err != nil {
				return err
			}
		}

		vlog := fmt.Sprintf("(%d) Added necessary SET statements to fix SQL file", logThreadSeq)
		global.Wlog.Debug(vlog)
	}

	for _, s := range uniqueSqls {
		ss := strings.TrimSpace(s)
		if ss == "" {
			continue
		}
		if !isFixCommentLine(ss) && !strings.HasSuffix(ss, ";") {
			ss += ";"
		}
		if _, err := w.WriteString(ss + "\n"); err != nil {
			return err
		}
		// 设置标志，表示有修复SQL被写入
		hasFixSqlWritten = true
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return nil
}

func isFixCommentLine(stmt string) bool {
	s := strings.TrimSpace(stmt)
	return strings.HasPrefix(s, "--") || strings.HasPrefix(s, "/*")
}

// filterRedundantPrimaryKeyStatements 过滤多余的ADD PRIMARY KEY语句
// 当发现有ADD COLUMN语句已经设置了PRIMARY KEY时，移除后续的单独ADD PRIMARY KEY语句
func filterRedundantPrimaryKeyStatements(sqls []string) []string {
	// 存储表和列的映射关系，用于检测重复的主键定义
	// key: tableIdentifier (schema.table)
	// value: map of column names that are already set as primary keys
	primaryKeyTables := make(map[string]map[string]bool)
	// 存储需要保留的SQL语句
	var result []string

	// 第一遍扫描：识别并记录在ADD COLUMN语句中设置为PRIMARY KEY的列
	for _, sql := range sqls {
		sqlUpper := strings.ToUpper(sql)

		// 检查是否是ADD COLUMN语句且包含PRIMARY KEY
		if strings.Contains(sqlUpper, "ADD COLUMN") && strings.Contains(sqlUpper, "PRIMARY KEY") {
			// 提取表标识符 (schema.table)
			tableID := extractTableIdentifier(sql)
			if tableID == "" {
				continue
			}

			// 提取列名
			column := extractColumnNameFromAddColumn(sql)
			if column == "" {
				continue
			}

			// 初始化表的映射
			if _, exists := primaryKeyTables[tableID]; !exists {
				primaryKeyTables[tableID] = make(map[string]bool)
			}
			// 记录该列已经是主键
			primaryKeyTables[tableID][strings.ToUpper(column)] = true
		}
	}

	// 第二遍扫描：过滤多余的ADD PRIMARY KEY语句
	for _, sql := range sqls {
		sqlUpper := strings.ToUpper(sql)

		// 检查是否是单独的ADD PRIMARY KEY语句（不包含ADD COLUMN）
		if strings.Contains(sqlUpper, "ADD PRIMARY KEY") && !strings.Contains(sqlUpper, "ADD COLUMN") {
			// 提取表标识符
			tableID := extractTableIdentifier(sql)
			if tableID == "" {
				// 如果无法提取表信息，保留这条SQL
				result = append(result, sql)
				continue
			}

			// 提取列名
			column := extractColumnNameFromAddPrimaryKey(sql)
			if column == "" {
				// 如果无法提取列信息，保留这条SQL
				result = append(result, sql)
				continue
			}

			// 检查该列是否已经在ADD COLUMN语句中设置为主键
			if tableMap, exists := primaryKeyTables[tableID]; exists {
				if tableMap[strings.ToUpper(column)] {
					// 跳过这个多余的ADD PRIMARY KEY语句
					continue
				}
			}
		}

		// 保留这条SQL语句
		result = append(result, sql)
	}

	return result
}

// extractTableIdentifier 从SQL语句中提取表标识符 (schema.table)
func extractTableIdentifier(sql string) string {
	// 查找ALTER TABLE部分
	alterTablePos := strings.ToUpper(sql)
	startPos := strings.Index(alterTablePos, "ALTER TABLE")
	if startPos == -1 {
		return ""
	}

	// 跳过ALTER TABLE
	startPos += len("ALTER TABLE")
	rest := strings.TrimSpace(sql[startPos:])

	// 提取表标识符，考虑可能的反引号
	if strings.HasPrefix(rest, "`") {
		// 查找第一个反引号
		firstQuote := 0
		// 查找第一个结束反引号
		endQuote := strings.Index(rest[firstQuote+1:], "`")
		if endQuote == -1 {
			return ""
		}
		endQuote++ // 调整索引，因为我们从firstQuote+1开始查找

		// 检查是否有schema.table格式
		if endQuote+1 < len(rest) && rest[endQuote+1] == '.' {
			// 提取schema
			schema := rest[firstQuote+1 : endQuote]

			// 查找table的开始位置
			tableStart := endQuote + 2 // 跳过.和可能的空格
			if tableStart < len(rest) && rest[tableStart] == '`' {
				tableStart++ // 跳过开始反引号
				tableEnd := strings.Index(rest[tableStart:], "`")
				if tableEnd != -1 {
					table := rest[tableStart : tableStart+tableEnd]
					return fmt.Sprintf("%s.%s", schema, table)
				}
			}
		} else {
			// 只有表名没有schema
			table := rest[firstQuote+1 : endQuote]
			return table
		}
	}

	// 如果没有反引号，尝试查找空格分割的表名
	parts := strings.Fields(rest)
	if len(parts) > 0 {
		return parts[0]
	}

	return ""
}

// extractColumnNameFromAddColumn 从ADD COLUMN语句中提取列名
func extractColumnNameFromAddColumn(sql string) string {
	// 查找ADD COLUMN部分
	addColumnPos := strings.ToUpper(sql)
	startPos := strings.Index(addColumnPos, "ADD COLUMN")
	if startPos == -1 {
		return ""
	}

	// 跳过ADD COLUMN
	startPos += len("ADD COLUMN")
	rest := strings.TrimSpace(sql[startPos:])

	// 提取列名，考虑可能的反引号
	if strings.HasPrefix(rest, "`") {
		// 查找第一个反引号
		firstQuote := 0
		// 查找第一个结束反引号
		endQuote := strings.Index(rest[firstQuote+1:], "`")
		if endQuote != -1 {
			return rest[firstQuote+1 : firstQuote+1+endQuote]
		}
	}

	// 如果没有反引号，尝试查找空格分割的列名
	parts := strings.Fields(rest)
	if len(parts) > 0 {
		// 可能包含类型信息，提取第一个部分
		return parts[0]
	}

	return ""
}

// extractColumnNameFromAddPrimaryKey 从ADD PRIMARY KEY语句中提取列名
func extractColumnNameFromAddPrimaryKey(sql string) string {
	// 查找ADD PRIMARY KEY部分
	addPKPos := strings.ToUpper(sql)
	startPos := strings.Index(addPKPos, "ADD PRIMARY KEY(")
	if startPos == -1 {
		return ""
	}

	// 跳过ADD PRIMARY KEY(
	startPos += len("ADD PRIMARY KEY(")
	rest := sql[startPos:]

	// 查找结束括号
	endPos := strings.Index(rest, ")")
	if endPos == -1 {
		return ""
	}

	// 提取括号内的内容（列名）
	columnPart := strings.TrimSpace(rest[:endPos])

	// 去除可能的反引号
	return strings.Trim(columnPart, "`")
}

// writeFixSQLToFile appends SQL statements into the specified file
func writeFixSQLToFile(path string, sqls []string, logThreadSeq int64) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		vlog := fmt.Sprintf("(%d) Failed to open fix SQL file %s: %v", logThreadSeq, path, err)
		global.Wlog.Error(vlog)
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	wrote := 0
	for _, s := range sqls {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		// ensure semicolon termination
		if !strings.HasSuffix(s, ";") {
			s = s + ";"
		}
		if _, err := w.WriteString(s + "\n"); err != nil {
			vlog := fmt.Sprintf("(%d) Failed to write fix SQL to %s: %v", logThreadSeq, path, err)
			global.Wlog.Error(vlog)
			return err
		}
		wrote++
	}
	if err := w.Flush(); err != nil {
		vlog := fmt.Sprintf("(%d) Failed to flush fix SQL to %s: %v", logThreadSeq, path, err)
		global.Wlog.Error(vlog)
		return err
	}

	vlog := fmt.Sprintf("(%d) Appended %d fix SQL statements to %s", logThreadSeq, wrote, path)
	global.Wlog.Debug(vlog)
	return nil
}

// GenerateRoutineFixSQL builds DROP + CREATE statements for procedure/function
// routineType should be "PROCEDURE" or "FUNCTION"
func GenerateRoutineFixSQL(sourceSchema, destSchema, name, routineType, sourceDef string) []string {
	upperType := strings.ToUpper(strings.TrimSpace(routineType))
	// 处理源端定义中的schema名称替换
	processedDef := processRoutineSchemaNames(sourceDef, sourceSchema, destSchema)
	createStmt := extractRoutineCreateStatement(processedDef)
	if !routineCreateLooksExecutable(createStmt, upperType) {
		return []string{
			fmt.Sprintf("-- WARNING: unable to auto-generate executable CREATE %s for `%s`.`%s`", upperType, destSchema, name),
			fmt.Sprintf("-- Suggested manual check: SHOW CREATE %s `%s`.`%s`;", upperType, sourceSchema, name),
		}
	}

	drop := fmt.Sprintf("DROP %s IF EXISTS %s.%s;", upperType, mysqlQuoteIdent(destSchema), mysqlQuoteIdent(name))
	return []string{drop, createStmt}
}

// GenerateTriggerFixSQL builds DROP + CREATE statements for trigger
func GenerateTriggerFixSQL(sourceSchema, destSchema, name, sourceDef string) []string {
	drop := fmt.Sprintf("DROP TRIGGER IF EXISTS %s.%s;", mysqlQuoteIdent(destSchema), mysqlQuoteIdent(name))

	// 处理源端定义中的schema名称替换
	processedDef := processTriggerSchemaNames(sourceDef, sourceSchema, destSchema)

	return []string{drop, strings.TrimSpace(processedDef)}
}

// CheckAndCleanupEmptyFixFile removes empty per-object fix files and files that
// contain only session preamble / transaction wrappers but no actual repair SQL.
func CheckAndCleanupEmptyFixFile(fixFileDir string) error {
	// 检查目录是否存在
	if _, err := os.Stat(fixFileDir); err != nil {
		// 目录不存在，不需要处理
		return nil
	}

	// 遍历目录中的所有.sql文件
	files, err := os.ReadDir(fixFileDir)
	if err != nil {
		return fmt.Errorf("failed to read fix file directory: %v", err)
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".sql") {
			filePath := fmt.Sprintf("%s/%s", fixFileDir, file.Name())

			info, err := file.Info()
			if err != nil {
				return fmt.Errorf("failed to stat fix SQL file %s: %v", file.Name(), err)
			}

			// 快速路径：物理为空直接删除，无需读内容
			if info.Size() == 0 {
				if err := os.Remove(filePath); err != nil {
					return fmt.Errorf("failed to remove empty fix SQL file %s: %v", file.Name(), err)
				}
				continue
			}

				// 慢路径：流式扫描非空文件，仅在必要时继续向后读取。
				hasActualFixSql, err := fixSQLFileHasActualStatements(filePath)
				if err != nil {
					return fmt.Errorf("failed to scan fix SQL file %s: %v", file.Name(), err)
				}

			if !hasActualFixSql {
				// 文件只包含preamble和事务控制语句，删除它
				if err := os.Remove(filePath); err != nil {
					return fmt.Errorf("failed to remove empty fix SQL file %s: %v", file.Name(), err)
				}
			}
		}
	}

	return nil
}

func fixSQLFileHasActualStatements(filePath string) (bool, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 8*1024*1024)

	for scanner.Scan() {
		trimmedLine := strings.TrimSpace(scanner.Text())
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "SET ") || trimmedLine == "BEGIN;" || trimmedLine == "COMMIT;" {
			continue
		}
		return true, nil
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return false, nil
}

// processRoutineSchemaNames 处理存储过程和函数定义中的schema名称替换
func processRoutineSchemaNames(sourceDef, sourceSchema, destSchema string) string {
	// 替换过程/函数名中的schema名称
	processedDef := strings.ReplaceAll(sourceDef, fmt.Sprintf("`%s`", sourceSchema), fmt.Sprintf("`%s`", destSchema))

	// 如果替换后没有包含目标schema名，说明原始语句没有schema名，需要手动添加
	if !strings.Contains(processedDef, fmt.Sprintf("`%s`", destSchema)) {
		// 查找"PROCEDURE"或"FUNCTION"关键字的位置
		procIndex := strings.Index(strings.ToUpper(processedDef), "PROCEDURE")
		funcIndex := strings.Index(strings.ToUpper(processedDef), "FUNCTION")

		var keywordIndex int
		var keywordType string
		if procIndex != -1 && funcIndex != -1 {
			if procIndex < funcIndex {
				keywordIndex = procIndex
				keywordType = "PROCEDURE"
			} else {
				keywordIndex = funcIndex
				keywordType = "FUNCTION"
			}
		} else if procIndex != -1 {
			keywordIndex = procIndex
			keywordType = "PROCEDURE"
		} else if funcIndex != -1 {
			keywordIndex = funcIndex
			keywordType = "FUNCTION"
		}

		if keywordIndex != -1 {
			// 从关键字之后开始处理
			afterKeyword := processedDef[keywordIndex+len(keywordType):]

			// 跳过可能的DEFINER子句
			afterKeyword = strings.TrimSpace(afterKeyword)
			if strings.HasPrefix(strings.ToUpper(afterKeyword), "DEFINER=") {
				// 跳过DEFINER子句直到下一个关键字
				parenCount := 0
				for i, char := range afterKeyword {
					if char == '(' {
						parenCount++
					} else if char == ')' {
						parenCount--
						if parenCount == 0 {
							afterKeyword = afterKeyword[i+1:]
							break
						}
					}
				}
			}

			// 跳过IF NOT EXISTS
			afterKeyword = strings.TrimSpace(afterKeyword)
			if strings.HasPrefix(strings.ToUpper(afterKeyword), "IF NOT EXISTS") {
				afterKeyword = afterKeyword[len("IF NOT EXISTS"):]
			}

			// 查找真正的过程/函数名
			afterKeyword = strings.TrimSpace(afterKeyword)

			// 跳过可能的DEFINER部分后，找到真正的函数名
			// 策略：找到第一个反引号，然后检查后面是否包含(字符(参数开始)
			funcNameStart := strings.Index(afterKeyword, "`")
			if funcNameStart != -1 {
				// 找到下一个反引号
				funcNameEnd := strings.Index(afterKeyword[funcNameStart+1:], "`")
				if funcNameEnd != -1 {
					// 提取候选名称
					candidateName := afterKeyword[funcNameStart+1 : funcNameStart+funcNameEnd+1]

					// 检查这个名称后面是否跟着(
					restAfterName := afterKeyword[funcNameStart+funcNameEnd+2:]
					restAfterName = strings.TrimSpace(restAfterName)

					// 如果后面跟着(或者是第一个出现在FUNCTION/PROCEDURE后面的反引号名称，认为这是真正的函数名
					if strings.HasPrefix(restAfterName, "(") {
						routineName := candidateName
						restOfDef := afterKeyword[funcNameStart+funcNameEnd+2:]

						// 在过程/函数名前添加schema名，确保格式为`destSchema`.`routineName`
						newRoutineName := fmt.Sprintf("`%s`.`%s`", destSchema, routineName)
						beforeKeyword := processedDef[:keywordIndex]
						processedDef = beforeKeyword + keywordType + " " + newRoutineName + restOfDef
					} else {
						// 如果没有找到合适的函数名，尝试查找FUNCTION/PROCEDURE关键字后的函数名
						// 这是针对特殊情况的备用逻辑
						funcKeyword := keywordType

						// 从关键字后开始查找真正的函数名
						keywordPos := strings.ToUpper(processedDef)
						funcStartPos := strings.Index(keywordPos, funcKeyword)
						if funcStartPos != -1 {
							funcStartPos += len(funcKeyword)
							afterFunc := processedDef[funcStartPos:]
							afterFunc = strings.TrimSpace(afterFunc)

							// 跳过IF NOT EXISTS
							if strings.HasPrefix(strings.ToUpper(afterFunc), "IF NOT EXISTS") {
								afterFunc = afterFunc[len("IF NOT EXISTS"):]
								afterFunc = strings.TrimSpace(afterFunc)
							}

							// 再次查找反引号
							if strings.HasPrefix(afterFunc, "`") {
								endQuoteIndex := strings.Index(afterFunc[1:], "`")
								if endQuoteIndex != -1 {
									routineName := afterFunc[1 : endQuoteIndex+1]
									restOfDef := afterFunc[endQuoteIndex+2:]

									// 添加schema名
									newRoutineName := fmt.Sprintf("`%s`.`%s`", destSchema, routineName)
									beforeFunc := processedDef[:funcStartPos]
									processedDef = beforeFunc + " " + newRoutineName + restOfDef
								}
							}
						}
					}
				}
			}
		}
	}

	return processedDef
}

func extractRoutineCreateStatement(sourceDef string) string {
	normalized := strings.TrimSpace(strings.ReplaceAll(sourceDef, "\r", ""))
	if normalized == "" {
		return ""
	}

	lines := strings.Split(normalized, "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToUpper(trimmed), "DELIMITER ") {
			continue
		}
		filtered = append(filtered, line)
	}

	normalized = strings.TrimSpace(strings.Join(filtered, "\n"))
	normalized = strings.TrimSpace(routineFixMetadataCommentPattern.ReplaceAllString(normalized, ""))
	if normalized == "" {
		return ""
	}

	upper := strings.ToUpper(normalized)
	if idx := strings.Index(upper, "CREATE "); idx >= 0 {
		normalized = strings.TrimSpace(normalized[idx:])
	}

	for {
		switch {
		case strings.HasSuffix(normalized, "$$"):
			normalized = strings.TrimSpace(strings.TrimSuffix(normalized, "$$"))
		case strings.HasSuffix(normalized, "$"):
			normalized = strings.TrimSpace(strings.TrimSuffix(normalized, "$"))
		case strings.HasSuffix(normalized, ";"):
			normalized = strings.TrimSpace(strings.TrimSuffix(normalized, ";"))
		default:
			return normalized
		}
	}
}

func routineCreateLooksExecutable(createStmt, routineType string) bool {
	normalized := strings.TrimSpace(createStmt)
	if normalized == "" {
		return false
	}

	upper := strings.ToUpper(normalized)
	if !strings.Contains(upper, "CREATE ") || !strings.Contains(upper, routineType) {
		return false
	}

	typeIndex := strings.Index(upper, routineType)
	if typeIndex == -1 {
		return false
	}

	openParenOffset := strings.Index(normalized[typeIndex:], "(")
	if openParenOffset == -1 {
		return false
	}
	openParenIndex := typeIndex + openParenOffset
	depth := 0
	closingParenIndex := -1
	for i := openParenIndex; i < len(normalized); i++ {
		switch normalized[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				closingParenIndex = i
				break
			}
		}
	}
	if closingParenIndex == -1 {
		return false
	}

	rest := strings.TrimSpace(normalized[closingParenIndex+1:])
	return rest != ""
}

// processTriggerSchemaNames 处理触发器定义中的schema名称替换
func processTriggerSchemaNames(sourceDef, sourceSchema, destSchema string) string {
	// 首先尝试直接替换已有的schema名称
	processedDef := strings.ReplaceAll(sourceDef, fmt.Sprintf("`%s`.`", sourceSchema), fmt.Sprintf("`%s`.`", destSchema))

	// 检查是否需要添加schema前缀到触发器名
	if !strings.Contains(strings.ToUpper(processedDef), strings.ToUpper(fmt.Sprintf("TRIGGER `%s`.`", destSchema))) {
		// 查找TRIGGER关键字的位置（忽略大小写）
		triggerIndex := strings.Index(strings.ToUpper(processedDef), "TRIGGER")
		if triggerIndex != -1 {
			// 从TRIGGER之后开始查找触发器名
			searchStart := triggerIndex + len("TRIGGER")

			// 跳过可能的DEFINER部分
			remaining := processedDef[searchStart:]
			trimmed := strings.TrimSpace(remaining)

			// 如果包含DEFINER，找到其结束位置
			if strings.HasPrefix(strings.ToUpper(trimmed), "DEFINER=") {
				// 使用括号匹配来正确跳过整个DEFINER子句
				parenCount := 0
				for i, char := range trimmed {
					if char == '(' {
						parenCount++
					} else if char == ')' {
						parenCount--
						if parenCount == 0 {
							searchStart += i + 1
							break
						}
					}
				}
			}

			// 查找真正的触发器名
			remaining = processedDef[searchStart:]

			// 跳过可能的DEFINER部分后，找到真正的触发器名
			// 策略：找到第一个反引号，然后检查后面是否包含ON关键字
			triggerNameStart := strings.Index(remaining, "`")
			if triggerNameStart != -1 {
				// 找到下一个反引号
				triggerNameEnd := strings.Index(remaining[triggerNameStart+1:], "`")
				if triggerNameEnd != -1 {
					// 提取候选名称
					candidateName := remaining[triggerNameStart+1 : triggerNameStart+triggerNameEnd+1]

					// 检查这个名称后面是否包含ON关键字(触发器语法中ON表示表名开始)
					restAfterName := remaining[triggerNameStart+triggerNameEnd+2:]
					restAfterName = strings.TrimSpace(restAfterName)

					// 如果后面包含ON关键字，认为这是真正的触发器名
					if strings.Contains(strings.ToUpper(restAfterName), " ON ") {
						// 构建新的触发器名，添加schema前缀
						newTriggerName := fmt.Sprintf("`%s`.`%s`", destSchema, candidateName)

						// 替换原始触发器名
						before := processedDef[:searchStart+triggerNameStart]
						after := processedDef[searchStart+triggerNameStart+triggerNameEnd+2:]
						processedDef = before + newTriggerName + after
					}
				}
			} else {
				// 如果没有找到反引号包围的触发器名，尝试备用策略
				// 从CREATE TRIGGER关键字后开始查找
				keywordPos := strings.ToUpper(processedDef)
				triggerStartPos := strings.Index(keywordPos, "TRIGGER")
				if triggerStartPos != -1 {
					triggerStartPos += len("TRIGGER")
					afterTrigger := processedDef[triggerStartPos:]
					afterTrigger = strings.TrimSpace(afterTrigger)

					// 再次尝试查找反引号
					if strings.HasPrefix(afterTrigger, "`") {
						endQuoteIndex := strings.Index(afterTrigger[1:], "`")
						if endQuoteIndex != -1 {
							triggerName := afterTrigger[1 : endQuoteIndex+1]
							restOfDef := afterTrigger[endQuoteIndex+2:]

							// 添加schema名
							newTriggerName := fmt.Sprintf("`%s`.`%s`", destSchema, triggerName)
							beforeCreate := processedDef[:triggerStartPos]
							processedDef = beforeCreate + " " + newTriggerName + restOfDef
						}
					}
				}
			}
		}
	}

	// 确保表名也有正确的schema前缀
	processedDef = replaceTableSchemaInTrigger(processedDef, sourceSchema, destSchema)

	return processedDef
}

// replaceTableSchemaInTrigger 替换触发器ON子句中的表名schema
func replaceTableSchemaInTrigger(triggerDef, sourceSchema, destSchema string) string {
	onIndex := strings.Index(strings.ToUpper(triggerDef), " ON ")
	if onIndex != -1 {
		afterOn := triggerDef[onIndex+4:]
		forEachIndex := strings.Index(strings.ToUpper(afterOn), " FOR EACH ROW")
		if forEachIndex == -1 {
			return triggerDef
		}

		tableRef := strings.TrimSpace(afterOn[:forEachIndex])
		rest := afterOn[forEachIndex:]

		if tableRef == "" {
			return triggerDef
		}

		if !strings.Contains(tableRef, ".") {
			return triggerDef[:onIndex+4] + fmt.Sprintf("`%s`.%s", destSchema, tableRef) + rest
		}

		parts := strings.SplitN(tableRef, ".", 2)
		if len(parts) != 2 {
			return triggerDef
		}

		schemaPart := strings.Trim(parts[0], "` ")
		tablePart := parts[1]
		if strings.EqualFold(schemaPart, sourceSchema) {
			return triggerDef[:onIndex+4] + fmt.Sprintf("`%s`.%s", destSchema, tablePart) + rest
		}
	}

	return triggerDef
}
