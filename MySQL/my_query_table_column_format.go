package mysql

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

func getDBScopeKey(db *sql.DB) string {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	if scopeKey, ok := dbScopeKeyGlobalCache[db]; ok {
		return scopeKey
	}
	dbScopeKeySeq++
	scopeKey := fmt.Sprintf("dbscope-%d", dbScopeKeySeq)
	dbScopeKeyGlobalCache[db] = scopeKey
	return scopeKey
}

func scopedTableCacheKey(db *sql.DB, schema, table, suffix string) string {
	scopeKey := getDBScopeKey(db)
	if suffix == "" {
		return fmt.Sprintf("%s.%s.%s", scopeKey, schema, table)
	}
	return fmt.Sprintf("%s.%s.%s.%s", scopeKey, schema, table, suffix)
}

func scopedColumnCacheKey(db *sql.DB, schema, table, column string) string {
	return fmt.Sprintf("%s.%s.%s.%s", getDBScopeKey(db), schema, table, column)
}

func containsString(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}
	return false
}

func isMySQLIntegerType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	base := t
	if idx := strings.Index(base, "("); idx >= 0 {
		base = strings.TrimSpace(base[:idx])
	}
	return base == "TINYINT" ||
		base == "SMALLINT" ||
		base == "MEDIUMINT" ||
		base == "INT" ||
		base == "INTEGER" ||
		base == "BIGINT"
}

func shouldNormalizeZeroFillInteger(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	return strings.Contains(t, "ZEROFILL") && isMySQLIntegerType(t)
}

func isMySQLTimeOnlyType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	return t == "TIME" || strings.HasPrefix(t, "TIME(")
}

func formatComparableColumnExpr(columnExpr, dataType string) string {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	formatted := columnExpr
	if t == "DATE" {
		formatted = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d 00:00:00')", formatted)
	}
	if isMySQLTimeOnlyType(t) {
		formatted = fmt.Sprintf("time_format(%s,'%%H:%%i:%%s')", formatted)
	}
	if strings.HasPrefix(t, "DATETIME") {
		formatted = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", formatted)
	}
	if strings.Contains(t, "TIMESTAMP") {
		formatted = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", formatted)
	}
	if strings.HasPrefix(t, "DOUBLE(") {
		dianAfter := strings.ReplaceAll(strings.Split(t, ",")[1], ")", "")
		bb, _ := strconv.Atoi(dianAfter)
		dianBefer := strings.Split(strings.Split(t, ",")[0], "(")[1]
		bbc, _ := strconv.Atoi(dianBefer)
		formatted = fmt.Sprintf("CAST(%s AS DECIMAL(%d,%d))", formatted, bbc, bb)
	}
	if shouldNormalizeZeroFillInteger(t) {
		// Ignore display-only leading zeros from ZEROFILL when comparing data.
		formatted = fmt.Sprintf("CAST(%s AS DECIMAL(65,0))", formatted)
	}
	// BIT(N) 在 Go MySQL 驱动中返回原始字节，直接作为分片键或对比值会产生不可打印
	// 字符（如 0x01），塞回 Oracle NUMBER 列谓词会触发 ORA-01722。用 CAST 归一化为
	// 无符号整数文本后，Oracle 端 TO_CHAR(NUMBER,'FM…') 的输出亦可一一匹配。
	if t == "BIT" || strings.HasPrefix(t, "BIT(") {
		formatted = fmt.Sprintf("CAST(%s AS UNSIGNED)", formatted)
	}
	return formatted
}

func isBitColumnType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	return t == "BIT" || strings.HasPrefix(t, "BIT(")
}

func (my *QueryTable) resolveColumnDataType(db *sql.DB, columnName string, logThreadSeq int64) string {
	if columnName == "" {
		return ""
	}
	for _, col := range my.TableColumn {
		if col["columnName"] == columnName {
			if dt := col["dataType"]; dt != "" {
				return dt
			}
		}
	}
	cacheKey := scopedColumnCacheKey(db, my.Schema, my.Table, columnName)
	cacheMutex.RLock()
	if dt, ok := columnDataTypeGlobalCache[cacheKey]; ok && dt != "" {
		cacheMutex.RUnlock()
		return dt
	}
	cacheMutex.RUnlock()
	query := fmt.Sprintf("SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s'",
		my.Schema, my.Table, columnName)
	var dt string
	if err := db.QueryRow(query).Scan(&dt); err != nil {
		return ""
	}
	if dt != "" {
		cacheMutex.Lock()
		columnDataTypeGlobalCache[cacheKey] = dt
		cacheMutex.Unlock()
	}
	return dt
}

func normalizeColumnLookupKey(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func buildColumnLookupByName(cols []map[string]string) map[string]map[string]string {
	byName := make(map[string]map[string]string, len(cols))
	for _, col := range cols {
		if name, ok := col["columnName"]; ok {
			byName[normalizeColumnLookupKey(name)] = col
		}
	}
	return byName
}

func orderColumnsForCompare(tableColumns []map[string]string, pkCols []string, compareCols []string) []map[string]string {
	if len(compareCols) == 0 {
		return tableColumns
	}

	colByName := buildColumnLookupByName(tableColumns)
	pkSet := make(map[string]bool, len(pkCols))
	for _, c := range pkCols {
		pkSet[normalizeColumnLookupKey(c)] = true
	}

	ordered := make([]map[string]string, 0, len(pkCols)+len(compareCols))
	for _, pkCol := range pkCols {
		if c, ok := colByName[normalizeColumnLookupKey(pkCol)]; ok {
			ordered = append(ordered, c)
		}
	}
	for _, cmpCol := range compareCols {
		key := normalizeColumnLookupKey(cmpCol)
		if pkSet[key] {
			continue
		}
		if c, ok := colByName[key]; ok {
			ordered = append(ordered, c)
		}
	}
	return ordered
}
