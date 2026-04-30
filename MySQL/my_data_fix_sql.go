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
	// 缓存表是否有 NOT NULL 唯一索引，key格式：schema.table
	TableHasNotNullUniqueIndex map[string]bool
	// 缓存目标端是否启用 sql_generate_invisible_primary_key
	sqlGenerateInvisiblePKEnabled *bool

	// 互斥锁保护缓存map的并发访问
	tablePrimaryKeyMutex          sync.RWMutex
	databaseCacheMutex            sync.RWMutex
	tableNotNullUniqueIndexMutex  sync.RWMutex
	sqlGenerateInvisiblePKMutex   sync.RWMutex
)

// mysqlQuoteIdent 对 MySQL 标识符加反引号，并对内部反引号做双写转义。
// filterPKColumnsAgainstSource 保留能在源端列集合中找到的主键列，过滤掉
// 目标端特有（如 MySQL 8.0 自动生成的 my_row_id 隐藏主键）的列。
// 返回过滤后的主键列以及被丢弃的列。
func filterPKColumnsAgainstSource(pkColumns []string, sourceColData []map[string]string) (kept, dropped []string) {
	sourceColSet := make(map[string]struct{}, len(sourceColData))
	for _, col := range sourceColData {
		if name, ok := col["columnName"]; ok && name != "" {
			sourceColSet[strings.ToLower(name)] = struct{}{}
		}
	}
	for _, pk := range pkColumns {
		if _, ok := sourceColSet[strings.ToLower(pk)]; ok {
			kept = append(kept, pk)
		} else {
			dropped = append(dropped, pk)
		}
	}
	return kept, dropped
}

func mysqlQuoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// mysqlIndexColDDLExpr 从索引 token 中提取可直接用于 DDL 的表达式。
//
// 普通列 token（格式：colName/*seq*/N/*type*/T/*prefix*/P）：
//   - 返回 `colName` 或 `colName`(prefix)
//   - 旧格式 token（无 /*prefix*/ 段）向后兼容：prefix 视为 0
//
// 函数索引 token（格式：/*expr*/EXPRESSION/*seq*/N/*type*//*prefix*/0）：
//   - 返回带括号的表达式（例如 (abs(`price`))），可直接嵌入 ADD INDEX idx_name((EXPR))
//   - MySQL information_schema.EXPRESSION 不含外层括号，DDL 需手动补括号
func mysqlIndexColDDLExpr(token string) string {
	// 函数索引 token 以 /*expr*/ 开头
	if strings.HasPrefix(token, "/*expr*/") {
		rest := strings.TrimPrefix(token, "/*expr*/")
		var expr string
		if seqIdx := strings.Index(rest, "/*seq*/"); seqIdx >= 0 {
			expr = strings.TrimSpace(rest[:seqIdx])
		} else {
			expr = strings.TrimSpace(rest)
		}
		// MySQL 函数索引 DDL 必须用括号包裹表达式：ADD INDEX idx((expr))
		if !strings.HasPrefix(expr, "(") {
			expr = "(" + expr + ")"
		}
		return expr
	}

	colName := strings.TrimSpace(token)
	prefix := 0
	if seqParts := strings.Split(token, "/*seq*/"); len(seqParts) == 2 {
		colName = strings.TrimSpace(seqParts[0])
		if typeParts := strings.Split(seqParts[1], "/*type*/"); len(typeParts) == 2 {
			if prefixParts := strings.Split(typeParts[1], "/*prefix*/"); len(prefixParts) == 2 {
				if n, err := strconv.Atoi(strings.TrimSpace(prefixParts[1])); err == nil {
					prefix = n
				}
			}
		}
	}
	quoted := mysqlQuoteIdent(colName)
	if prefix > 0 {
		return fmt.Sprintf("%s(%d)", quoted, prefix)
	}
	return quoted
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
	if isBitColumnType(dataType) {
		// BIT 列经过 CAST(col AS UNSIGNED) 归一化后返回十进制字符串，
		// 直接作为整数字面量写入，MySQL 会按列位宽隐式转换，避免 ASCII 字节
		// 被 `0x%X` 编码成超长字节串导致 ERROR 1406 (22001) Data too long。
		v := strings.TrimSpace(value)
		if v == "" {
			return "0"
		}
		if _, err := strconv.ParseUint(v, 10, 64); err == nil {
			return v
		}
		return fmt.Sprintf("0x%X", []byte(value))
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
	// Fallback for bare FLOAT without declared scale:
	// CAST(... AS FLOAT) is only supported from MySQL 8.0.17+; avoid it so that
	// MySQL 5.7 / 8.0.0-8.0.16 / MariaDB 10.x can still execute the repair SQL.
	// Use ROUND with 7 significant digits (matches FLOAT single precision).
	t := strings.ToUpper(strings.TrimSpace(dataType))
	if strings.HasPrefix(t, "FLOAT") {
		return fmt.Sprintf("ROUND(`%s`, 7) = ROUND(%s, 7)", columnName, floatLiteral), true
	}
	return fmt.Sprintf("`%s` = %s", columnName, floatLiteral), true
}

// 初始化全局变量
func init() {
	AutoIncrementColumnsWithPrimaryKey = make(map[string]bool)
	DestTableHasPrimaryKey = make(map[string]bool)
	TablePrimaryKeyColumns = make(map[string][]string)
	CurrentDatabaseCache = make(map[string]string)
	TableHasNotNullUniqueIndex = make(map[string]bool)
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

// CheckTableHasNotNullUniqueIndex 检查表是否有 NOT NULL 唯一索引
// 返回 true 表示表有至少一个唯一索引，且该索引的所有列都是 NOT NULL
func CheckTableHasNotNullUniqueIndex(db *sql.DB, schema, table string, logThreadSeq int64) (bool, error) {
	key := fmt.Sprintf("%s.%s", schema, table)

	// 如果已经检查过，直接返回结果（使用读锁）
	tableNotNullUniqueIndexMutex.RLock()
	if hasIndex, exists := TableHasNotNullUniqueIndex[key]; exists {
		tableNotNullUniqueIndexMutex.RUnlock()
		return hasIndex, nil
	}
	tableNotNullUniqueIndexMutex.RUnlock()

	// 查询唯一索引及其列的 NULL 约束
	// 使用 GROUP BY 按索引名分组，检查是否所有列都是 NOT NULL
	query := `
		SELECT s.INDEX_NAME,
		       COUNT(*) as total_cols,
		       SUM(CASE WHEN c.IS_NULLABLE = 'NO' THEN 1 ELSE 0 END) as not_null_cols
		FROM INFORMATION_SCHEMA.STATISTICS s
		JOIN INFORMATION_SCHEMA.COLUMNS c
		  ON s.TABLE_SCHEMA = c.TABLE_SCHEMA
		  AND s.TABLE_NAME = c.TABLE_NAME
		  AND s.COLUMN_NAME = c.COLUMN_NAME
		WHERE s.TABLE_SCHEMA = ?
		  AND s.TABLE_NAME = ?
		  AND s.NON_UNIQUE = 0
		  AND s.INDEX_NAME != 'PRIMARY'
		GROUP BY s.INDEX_NAME
		HAVING total_cols = not_null_cols
		LIMIT 1
	`

	var indexName string
	var totalCols, notNullCols int
	err := db.QueryRow(query, schema, table).Scan(&indexName, &totalCols, &notNullCols)

	hasIndex := false
	if err == nil {
		// 找到至少一个唯一索引，且所有列都是 NOT NULL
		hasIndex = true
	} else if err != sql.ErrNoRows {
		// 查询出错（非"无结果"错误）
		vlog := fmt.Sprintf("(%d) Error checking NOT NULL unique index for %s.%s: %v", logThreadSeq, schema, table, err)
		global.Wlog.Error(vlog)
		return false, err
	}
	// sql.ErrNoRows 表示没有找到符合条件的唯一索引，hasIndex 保持为 false

	// 更新缓存（使用写锁）
	tableNotNullUniqueIndexMutex.Lock()
	TableHasNotNullUniqueIndex[key] = hasIndex
	tableNotNullUniqueIndexMutex.Unlock()

	return hasIndex, nil
}

// CheckSqlGenerateInvisiblePrimaryKey 检查目标端是否启用 sql_generate_invisible_primary_key
// 返回 true 表示目标端已启用该参数，会自动为无主键表生成 my_row_id 隐藏列
func CheckSqlGenerateInvisiblePrimaryKey(db *sql.DB, logThreadSeq int64) (bool, error) {
	// 如果已经检查过，直接返回结果（使用读锁）
	sqlGenerateInvisiblePKMutex.RLock()
	if sqlGenerateInvisiblePKEnabled != nil {
		enabled := *sqlGenerateInvisiblePKEnabled
		sqlGenerateInvisiblePKMutex.RUnlock()
		return enabled, nil
	}
	sqlGenerateInvisiblePKMutex.RUnlock()

	// 查询目标端的 sql_generate_invisible_primary_key 变量
	query := "SHOW VARIABLES LIKE 'sql_generate_invisible_primary_key'"
	var varName, varValue string
	err := db.QueryRow(query).Scan(&varName, &varValue)

	enabled := false
	if err == nil {
		// 变量存在，检查值是否为 1 或 ON
		varValue = strings.ToUpper(strings.TrimSpace(varValue))
		if varValue == "1" || varValue == "ON" {
			enabled = true
		}
	} else if err != sql.ErrNoRows {
		// 查询出错（非"无结果"错误）
		vlog := fmt.Sprintf("(%d) Error checking sql_generate_invisible_primary_key: %v", logThreadSeq, err)
		global.Wlog.Warn(vlog)
		// 查询失败时假定未启用，不返回错误
	}
	// sql.ErrNoRows 或变量值为 0/OFF 表示未启用，enabled 保持为 false

	// 更新缓存（使用写锁）
	sqlGenerateInvisiblePKMutex.Lock()
	sqlGenerateInvisiblePKEnabled = &enabled
	sqlGenerateInvisiblePKMutex.Unlock()

	return enabled, nil
}

// GenerateMyRowIDColumnDef 生成 my_row_id 列定义数组（符合列定义数组结构）
// 返回 6 元素数组：[数据类型, 字符集, 排序规则, NULL约束, 默认值, 列注释]
func GenerateMyRowIDColumnDef() []string {
	return []string{
		"bigint unsigned NOT NULL AUTO_INCREMENT /*!80023 INVISIBLE */", // [0] 数据类型
		"null",  // [1] 字符集
		"null",  // [2] 排序规则
		"NO",    // [3] NULL 约束（NOT NULL）
		"empty", // [4] 默认值
		"",      // [5] 列注释
	}
}

// ShouldAddMyRowID 综合判断是否需要为表添加 my_row_id 隐藏列
// 返回 true 表示需要添加，false 表示不需要添加
func ShouldAddMyRowID(db *sql.DB, schema, table, requirePK string, logThreadSeq int64) (bool, error) {
	// 1. 检查 requirePK 是否为 ON
	if strings.ToUpper(strings.TrimSpace(requirePK)) != "ON" {
		return false, nil
	}

	// 2. 检查目标端是否已启用 sql_generate_invisible_primary_key
	enabled, err := CheckSqlGenerateInvisiblePrimaryKey(db, logThreadSeq)
	if err != nil {
		// 查询失败时假定未启用，继续检查
		vlog := fmt.Sprintf("(%d) Failed to check sql_generate_invisible_primary_key, assuming disabled: %v", logThreadSeq, err)
		global.Wlog.Warn(vlog)
	}
	if enabled {
		// 目标端已启用自动生成隐藏主键，无需手动添加
		return false, nil
	}

	// 3. 检查表是否有主键
	// 使用 CheckDestTableHasPrimaryKey 的逻辑，但需要适配为独立函数调用
	key := fmt.Sprintf("%s.%s", schema, table)
	tablePrimaryKeyMutex.RLock()
	hasPK, exists := DestTableHasPrimaryKey[key]
	tablePrimaryKeyMutex.RUnlock()

	if !exists {
		// 缓存中没有，需要查询
		query := `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		          WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'`
		rows, err := db.Query(query, schema, table)
		if err != nil {
			vlog := fmt.Sprintf("(%d) Error checking primary key for %s.%s: %v", logThreadSeq, schema, table, err)
			global.Wlog.Error(vlog)
			return false, err
		}
		defer rows.Close()

		hasPK = false
		for rows.Next() {
			var colName string
			if err := rows.Scan(&colName); err != nil {
				continue
			}
			hasPK = true
			break
		}

		// 更新缓存
		tablePrimaryKeyMutex.Lock()
		DestTableHasPrimaryKey[key] = hasPK
		tablePrimaryKeyMutex.Unlock()
	}

	if hasPK {
		// 表有主键，无需添加 my_row_id
		return false, nil
	}

	// 4. 检查表是否有 NOT NULL 唯一索引
	hasNotNullUniqueIndex, err := CheckTableHasNotNullUniqueIndex(db, schema, table, logThreadSeq)
	if err != nil {
		return false, err
	}
	if hasNotNullUniqueIndex {
		// 表有 NOT NULL 唯一索引，无需添加 my_row_id
		return false, nil
	}

	// 5. 检查表是否已有 my_row_id 列
	// 查询 INFORMATION_SCHEMA.COLUMNS 检查是否存在 my_row_id 列
	query := `SELECT COLUMN_NAME, EXTRA FROM INFORMATION_SCHEMA.COLUMNS
	          WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'my_row_id'`
	var colName, extra string
	err = db.QueryRow(query, schema, table).Scan(&colName, &extra)
	if err == nil {
		// 表已有 my_row_id 列，无需添加
		return false, nil
	} else if err != sql.ErrNoRows {
		// 查询出错
		vlog := fmt.Sprintf("(%d) Error checking my_row_id column for %s.%s: %v", logThreadSeq, schema, table, err)
		global.Wlog.Error(vlog)
		return false, err
	}

	// 所有条件都满足，需要添加 my_row_id
	return true, nil
}

// IsValidMyRowIDColumn 检查目标端列是否为符合条件的 my_row_id 列
// 检查条件：
// 1. 列名必须是 my_row_id
// 2. 数据类型必须是 int 或 bigint（不限定是否 unsigned）
// 3. 不限定是否 AUTO_INCREMENT
// 4. 必须声明是 PRIMARY KEY
// 5. 必须声明是 INVISIBLE 属性
// 6. 必须在第一列或最后一列（不能在中间位置）
//
// 参数：
// - db: 数据库连接
// - schema: 数据库名
// - table: 表名
// - columnName: 列名（规范化后的列名，用于匹配）
// - columnSeq: 列在表中的位置（从 0 开始）
// - totalColumns: 表的总列数
// - requirePK: requirePK 参数值（ON|OFF）
// - logThreadSeq: 日志线程序号
//
// 返回：
// - bool: true 表示是符合条件的 my_row_id 列，false 表示不是
// - error: 查询错误
func IsValidMyRowIDColumn(db *sql.DB, schema, table, columnName string, columnSeq, totalColumns int, requirePK string, logThreadSeq int64) (bool, error) {
	// 1. 检查 requirePK 是否为 ON
	if strings.ToUpper(strings.TrimSpace(requirePK)) != "ON" {
		return false, nil
	}

	// 2. 检查列名是否为 my_row_id（不区分大小写）
	if strings.ToLower(strings.TrimSpace(columnName)) != "my_row_id" {
		return false, nil
	}

	// 3. 检查列位置是否在第一列或最后一列
	if columnSeq != 0 && columnSeq != totalColumns-1 {
		vlog := fmt.Sprintf("(%d) Column %s in %s.%s is at position %d (not first or last), not a valid my_row_id", logThreadSeq, columnName, schema, table, columnSeq)
		global.Wlog.Debug(vlog)
		return false, nil
	}

	// 4. 查询列的详细信息
	query := `
		SELECT c.DATA_TYPE, c.EXTRA, k.CONSTRAINT_NAME
		FROM INFORMATION_SCHEMA.COLUMNS c
		LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
		  ON c.TABLE_SCHEMA = k.TABLE_SCHEMA
		  AND c.TABLE_NAME = k.TABLE_NAME
		  AND c.COLUMN_NAME = k.COLUMN_NAME
		  AND k.CONSTRAINT_NAME = 'PRIMARY'
		WHERE c.TABLE_SCHEMA = ?
		  AND c.TABLE_NAME = ?
		  AND c.COLUMN_NAME = ?
	`

	var dataType, extra string
	var constraintName sql.NullString
	err := db.QueryRow(query, schema, table, columnName).Scan(&dataType, &extra, &constraintName)
	if err != nil {
		if err == sql.ErrNoRows {
			// 列不存在
			return false, nil
		}
		vlog := fmt.Sprintf("(%d) Error checking my_row_id column details for %s.%s.%s: %v", logThreadSeq, schema, table, columnName, err)
		global.Wlog.Error(vlog)
		return false, err
	}

	// 5. 检查数据类型是否为 int 或 bigint
	dataType = strings.ToLower(strings.TrimSpace(dataType))
	if dataType != "int" && dataType != "bigint" {
		vlog := fmt.Sprintf("(%d) Column %s in %s.%s has data type %s (not int or bigint), not a valid my_row_id", logThreadSeq, columnName, schema, table, dataType)
		global.Wlog.Debug(vlog)
		return false, nil
	}

	// 6. 检查是否为 PRIMARY KEY
	if !constraintName.Valid || constraintName.String != "PRIMARY" {
		vlog := fmt.Sprintf("(%d) Column %s in %s.%s is not a PRIMARY KEY, not a valid my_row_id", logThreadSeq, columnName, schema, table)
		global.Wlog.Debug(vlog)
		return false, nil
	}

	// 7. 检查是否为 INVISIBLE
	extra = strings.ToUpper(strings.TrimSpace(extra))
	if !strings.Contains(extra, "INVISIBLE") {
		vlog := fmt.Sprintf("(%d) Column %s in %s.%s is not INVISIBLE (EXTRA=%s), not a valid my_row_id", logThreadSeq, columnName, schema, table, extra)
		global.Wlog.Debug(vlog)
		return false, nil
	}

	// 所有条件都满足，是符合条件的 my_row_id 列
	vlog := fmt.Sprintf("(%d) Column %s in %s.%s is a valid my_row_id column (type=%s, PRIMARY KEY, INVISIBLE, position=%d/%d)", logThreadSeq, columnName, schema, table, dataType, columnSeq, totalColumns)
	global.Wlog.Info(vlog)
	return true, nil
}
