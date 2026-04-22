package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"regexp"
	"strconv"
	"strings"
)

func (my *QueryTable) checkColumnExists(db *sql.DB, columnName string, logThreadSeq int64) (bool, error) {
	var (
		Event = "Check_Column_Exists"
	)

	// Generate cache key in format: dbPtr.schema.table.column
	// 关键修复：包含db指针地址以区分源端和目标端
	// 当源端与目标端的schema.table相同时，防止缓存串用
	cacheKey := fmt.Sprintf("%p.%s.%s.%s", db, my.Schema, my.Table, columnName)

	// Check if result is already in global cache
	cacheMutex.RLock()
	if exists, ok := columnExistsGlobalCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		//vlog := fmt.Sprintf("(%d) [%s] Column %s existence check result from global cache: %v", logThreadSeq, Event, columnName, exists)
		//global.Wlog.Debug(vlog)
		return exists, nil
	}
	cacheMutex.RUnlock()

	tableColumns, err := my.getTableColumnSet(db, logThreadSeq)
	if err == nil {
		_, exists := tableColumns[strings.ToLower(columnName)]
		vlog := fmt.Sprintf("(%d) [%s] Column %s existence check result (table cache): %v", logThreadSeq, Event, columnName, exists)
		global.Wlog.Debug(vlog)
		cacheMutex.Lock()
		columnExistsGlobalCache[cacheKey] = exists
		cacheMutex.Unlock()
		return exists, nil
	}

	// 兜底：使用单列查询（仅在表级缓存加载失败时触发）
	count := 0
	strsql := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'", my.Schema, my.Table, columnName)
	vlog := fmt.Sprintf("(%d) [%s] Table cache unavailable, checking column %s via fallback SQL", logThreadSeq, Event, columnName)
	global.Wlog.Debug(vlog)

	// 直接使用db.QueryRow避免使用DBSQLforExec的重试机制，因为这个查询很简单
	err = db.QueryRow(strsql).Scan(&count)
	if err != nil {
		// 如果查询失败，使用DESCRIBE语句作为备选方案
		vlog = fmt.Sprintf("(%d) [%s] Failed to query information_schema, using DESCRIBE as fallback. Error: %s", logThreadSeq, Event, err)
		global.Wlog.Debug(vlog)

		// 使用DESCRIBE语句检查列是否存在
		describeSQL := fmt.Sprintf("DESCRIBE `%s`.`%s` %s", my.Schema, my.Table, columnName)
		rows, err := db.Query(describeSQL)
		if err != nil {
			vlog = fmt.Sprintf("(%d) [%s] DESCRIBE query failed, column %s likely does not exist. Error: %s", logThreadSeq, Event, columnName, err)
			global.Wlog.Debug(vlog)
			// Cache the result in global cache
			cacheMutex.Lock()
			columnExistsGlobalCache[cacheKey] = false
			cacheMutex.Unlock()
			return false, nil // 列不存在
		}
		defer rows.Close()
		exists := rows.Next()
		vlog = fmt.Sprintf("(%d) [%s] Column %s existence check result: %v", logThreadSeq, Event, columnName, exists)
		global.Wlog.Debug(vlog)
		// Cache the result in global cache
		cacheMutex.Lock()
		columnExistsGlobalCache[cacheKey] = exists
		cacheMutex.Unlock()
		return exists, nil
	}

	exists := count > 0
	vlog = fmt.Sprintf("(%d) [%s] Column %s existence check result: %v", logThreadSeq, Event, columnName, exists)
	global.Wlog.Debug(vlog)
	// Cache the result in global cache
	cacheMutex.Lock()
	columnExistsGlobalCache[cacheKey] = exists
	cacheMutex.Unlock()
	return exists, nil
}

func (my *QueryTable) getTableColumnSet(db *sql.DB, logThreadSeq int64) (map[string]struct{}, error) {
	scopeKey := getDBScopeKey(db)
	cacheKey := fmt.Sprintf("%s.%s.%s.columns", scopeKey, my.Schema, my.Table)
	cacheMutex.RLock()
	if cached, ok := tableColumnSetGlobalCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		return cached, nil
	}
	cacheMutex.RUnlock()

	query := fmt.Sprintf("SELECT COLUMN_NAME FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", my.Schema, my.Table)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colSet := make(map[string]struct{}, 16)
	for rows.Next() {
		var colName string
		if err = rows.Scan(&colName); err != nil {
			return nil, err
		}
		colSet[strings.ToLower(colName)] = struct{}{}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	cacheMutex.Lock()
	tableColumnSetGlobalCache[cacheKey] = colSet
	cacheMutex.Unlock()
	global.Wlog.Debug(fmt.Sprintf("(%d) [Check_Column_Exists] Loaded table column cache for %s.%s, columns=%d", logThreadSeq, my.Schema, my.Table, len(colSet)))
	return colSet, nil
}

func (my *QueryTable) getLeadingIndexName(db *sql.DB, columnName string) string {
	cacheKey := fmt.Sprintf("%p.%s.%s.%s", db, my.Schema, my.Table, columnName)
	cacheMutex.RLock()
	if indexName, ok := leadingIndexNameGlobalCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		return indexName
	}
	cacheMutex.RUnlock()

	query := fmt.Sprintf(
		"SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s' AND SEQ_IN_INDEX=1 ORDER BY (INDEX_NAME='PRIMARY') DESC, NON_UNIQUE ASC, INDEX_NAME ASC LIMIT 1",
		my.Schema,
		my.Table,
		columnName,
	)
	var indexName string
	if err := db.QueryRow(query).Scan(&indexName); err != nil {
		indexName = ""
	}

	cacheMutex.Lock()
	leadingIndexNameGlobalCache[cacheKey] = indexName
	cacheMutex.Unlock()
	return indexName
}

func (my *QueryTable) getTableRowsEstimate(db *sql.DB) uint64 {
	cacheKey := fmt.Sprintf("%p.%s.%s.tableRows", db, my.Schema, my.Table)
	cacheMutex.RLock()
	if cached, ok := columnDataTypeGlobalCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		if n, err := strconv.ParseUint(cached, 10, 64); err == nil {
			return n
		}
	} else {
		cacheMutex.RUnlock()
	}

	query := fmt.Sprintf("SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' LIMIT 1", my.Schema, my.Table)
	var tableRows sql.NullInt64
	if err := db.QueryRow(query).Scan(&tableRows); err != nil || !tableRows.Valid || tableRows.Int64 <= 0 {
		return 0
	}

	cacheMutex.Lock()
	columnDataTypeGlobalCache[cacheKey] = strconv.FormatInt(tableRows.Int64, 10)
	cacheMutex.Unlock()
	return uint64(tableRows.Int64)
}

func (my *QueryTable) getLeadingIndexCardinality(db *sql.DB, indexName string, columnName string) uint64 {
	if indexName == "" || columnName == "" {
		return 0
	}
	cacheKey := fmt.Sprintf("%p.%s.%s.%s.%s.cardinality", db, my.Schema, my.Table, indexName, columnName)
	cacheMutex.RLock()
	if cached, ok := columnDataTypeGlobalCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		if n, err := strconv.ParseUint(cached, 10, 64); err == nil {
			return n
		}
	} else {
		cacheMutex.RUnlock()
	}

	query := fmt.Sprintf(
		"SELECT CARDINALITY FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND INDEX_NAME='%s' AND COLUMN_NAME='%s' AND SEQ_IN_INDEX=1 LIMIT 1",
		my.Schema,
		my.Table,
		indexName,
		columnName,
	)
	var cardinality sql.NullInt64
	if err := db.QueryRow(query).Scan(&cardinality); err != nil || !cardinality.Valid || cardinality.Int64 <= 0 {
		return 0
	}

	cacheMutex.Lock()
	columnDataTypeGlobalCache[cacheKey] = strconv.FormatInt(cardinality.Int64, 10)
	cacheMutex.Unlock()
	return uint64(cardinality.Int64)
}

func shouldUseFastGroupMode(where string, tableRows uint64, cardinality uint64, hasLeadingIndex bool) bool {
	if where != "" || !hasLeadingIndex {
		return false
	}
	if tableRows == 0 || cardinality == 0 || tableRows < cardinality {
		return false
	}
	dupRatio := float64(tableRows) / float64(cardinality)
	return dupRatio <= fastGroupModeDupRatioThreshold
}

func extractWhereColumnsForGroupQuery(where string) []string {
	if strings.TrimSpace(where) == "" {
		return nil
	}
	// Match identifier on left side of common predicates.
	// Accept both `col` and bare col forms.
	re := regexp.MustCompile("(?i)(?:`([a-zA-Z0-9_]+)`|\\b([a-zA-Z_][a-zA-Z0-9_]*)\\b)\\s*(?:<=|>=|!=|=|<|>|LIKE\\b|IN\\b|BETWEEN\\b|IS\\b)")
	matches := re.FindAllStringSubmatch(where, -1)
	if len(matches) == 0 {
		return nil
	}
	keywords := map[string]struct{}{
		"AND": {}, "OR": {}, "NOT": {}, "NULL": {}, "TRUE": {}, "FALSE": {},
		"SELECT": {}, "FROM": {}, "WHERE": {}, "GROUP": {}, "ORDER": {}, "LIMIT": {},
	}
	seen := make(map[string]struct{}, len(matches))
	cols := make([]string, 0, len(matches))
	for _, m := range matches {
		col := strings.TrimSpace(m[1])
		if col == "" {
			col = strings.TrimSpace(m[2])
		}
		if col == "" {
			continue
		}
		up := strings.ToUpper(col)
		if _, bad := keywords[up]; bad {
			continue
		}
		if _, ok := seen[up]; ok {
			continue
		}
		seen[up] = struct{}{}
		cols = append(cols, col)
	}
	return cols
}

func (my *QueryTable) chooseGroupByForceIndex(db *sql.DB, groupColumn, where string) string {
	// No where: preserve historical behavior, prefer leading index of grouped column.
	if strings.TrimSpace(where) == "" {
		return my.getLeadingIndexName(db, groupColumn)
	}

	// Where exists: prefer index that matches filter columns to avoid full index scan.
	// Example: WHERE ps_partkey range + GROUP BY ps_suppkey should prefer PRIMARY(ps_partkey,...)
	for _, col := range extractWhereColumnsForGroupQuery(where) {
		idx := my.getLeadingIndexName(db, col)
		if idx != "" {
			return idx
		}
	}

	// Fall back to no FORCE INDEX and let optimizer decide.
	return ""
}

func (my *QueryTable) getMaxLeadingIndexCardinality(db *sql.DB) uint64 {
	cacheKey := fmt.Sprintf("%p.%s.%s.maxLeadingCardinality", db, my.Schema, my.Table)
	cacheMutex.RLock()
	if cached, ok := columnDataTypeGlobalCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		if n, err := strconv.ParseUint(cached, 10, 64); err == nil {
			return n
		}
	} else {
		cacheMutex.RUnlock()
	}

	query := fmt.Sprintf("SELECT MAX(CARDINALITY) FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND SEQ_IN_INDEX=1", my.Schema, my.Table)
	var card sql.NullInt64
	if err := db.QueryRow(query).Scan(&card); err != nil || !card.Valid || card.Int64 <= 0 {
		return 0
	}

	cacheMutex.Lock()
	columnDataTypeGlobalCache[cacheKey] = strconv.FormatInt(card.Int64, 10)
	cacheMutex.Unlock()
	return uint64(card.Int64)
}
