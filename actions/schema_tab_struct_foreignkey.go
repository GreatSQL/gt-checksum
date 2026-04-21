package actions

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"gt-checksum/global"
	"gt-checksum/schemacompat"
)

// queryMySQLForeignKeyIndexNames 返回 MySQL 表上所有外键对应 backing index 名称集合（大写）。
// MySQL 通常将 FK 约束名作为自动创建的 backing index 名，但以下场景会导致"FK 约束名 ≠ 索引名"：
//  1. 用户在创建 FK 前已显式创建可用索引，MySQL 复用该索引；
//  2. FK 创建语句显式使用 `FOREIGN KEY index_name (...)` 指定索引名；
//  3. 升级/迁移过程 FK 与索引被拆分并重命名。
//
// 为了精准识别 backing index，我们先查出每个 FK 的列顺序，再在 STATISTICS 中匹配
// "同表、同列、同 SEQ_IN_INDEX 前缀"的索引，避免把误把其他索引当 backing 剔除。
func queryMySQLForeignKeyIndexNames(db *sql.DB, schema, table string) (map[string]bool, error) {
	result := make(map[string]bool)

	// Step1: 按 FK 名 + POSITION 收集列顺序
	fkColRows, err := db.Query(`
		SELECT CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION`, schema, table)
	if err != nil {
		return result, err
	}
	fkColumns := make(map[string][]string)
	for fkColRows.Next() {
		var constraintName, columnName string
		var ordinalPos int
		if scanErr := fkColRows.Scan(&constraintName, &columnName, &ordinalPos); scanErr != nil {
			fkColRows.Close()
			global.Wlog.Warnf("queryMySQLForeignKeyIndexNames: scan FK column row failed for %s.%s: %v", schema, table, scanErr)
			return result, scanErr
		}
		fkColumns[strings.ToUpper(constraintName)] = append(fkColumns[strings.ToUpper(constraintName)], columnName)
	}
	fkColRows.Close()
	if err = fkColRows.Err(); err != nil {
		return result, err
	}

	if len(fkColumns) == 0 {
		return result, nil
	}

	// Step2: 查表上全部索引的列序列
	idxRows, err := db.Query(`
		SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX
		FROM INFORMATION_SCHEMA.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`, schema, table)
	if err != nil {
		// 查不到 STATISTICS 时回退为"FK 约束名即 backing index 名"——与旧版本一致
		global.Wlog.Warnf("queryMySQLForeignKeyIndexNames: STATISTICS lookup failed for %s.%s (%v); falling back to constraint-name heuristic", schema, table, err)
		for name := range fkColumns {
			result[name] = true
		}
		return result, nil
	}
	indexCols := make(map[string][]string)
	for idxRows.Next() {
		var indexName, columnName string
		var seqInIndex int
		if scanErr := idxRows.Scan(&indexName, &columnName, &seqInIndex); scanErr != nil {
			idxRows.Close()
			global.Wlog.Warnf("queryMySQLForeignKeyIndexNames: scan index row failed for %s.%s: %v", schema, table, scanErr)
			return result, scanErr
		}
		indexCols[indexName] = append(indexCols[indexName], columnName)
	}
	idxRows.Close()
	if err = idxRows.Err(); err != nil {
		return result, err
	}

	// Step3: 为每个 FK 匹配第一个列序列完全以 FK 列为前缀的索引
	//   - 若同名索引存在（默认情况），优先匹配同名；
	//   - 否则按列前缀匹配；
	//   - 最后兜底：FK 约束名也放入结果，避免遗漏。
	for fkName, cols := range fkColumns {
		matched := false
		if _, ok := indexCols[fkName]; ok {
			result[fkName] = true
			matched = true
		}
		for idxName, idxColList := range indexCols {
			if len(idxColList) < len(cols) {
				continue
			}
			isPrefix := true
			for i, c := range cols {
				if !strings.EqualFold(idxColList[i], c) {
					isPrefix = false
					break
				}
			}
			if isPrefix {
				result[strings.ToUpper(idxName)] = true
				matched = true
			}
		}
		if !matched {
			// 两种场景都没匹配到时保留 FK 约束名以兼容旧逻辑
			result[fkName] = true
		}
	}
	return result, nil
}

func queryMySQLCreateTableStatement(db *sql.DB, schema, table string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", escapeMySQLIdentifier(schema), escapeMySQLIdentifier(table))
	var (
		objectName string
		createStmt string
	)
	if err := db.QueryRow(query).Scan(&objectName, &createStmt); err != nil {
		return "", err
	}
	return createStmt, nil
}

func extractExplicitMySQLTableAutoIncrementValue(createStmt string) sql.NullInt64 {
	matches := mysqlTableAutoIncrementOptionPattern.FindStringSubmatch(createStmt)
	if len(matches) < 2 {
		return sql.NullInt64{}
	}
	n, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: n, Valid: true}
}

type mysqlUniqueIndexMetadata struct {
	Name      string
	Columns   []string
	HasPrefix bool
	IsPrimary bool
	IsUnique  bool
}

func loadMySQLUniqueIndexMetadata(db *sql.DB, schema, table string) ([]mysqlUniqueIndexMetadata, error) {
	rows, err := db.Query(`
SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY INDEX_NAME, SEQ_IN_INDEX
`, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type rowItem struct {
		name      string
		nonUnique int
		seq       int
		column    string
		subPart   sql.NullInt64
	}

	grouped := make(map[string][]rowItem)
	order := make([]string, 0)
	for rows.Next() {
		var item rowItem
		if err := rows.Scan(&item.name, &item.nonUnique, &item.seq, &item.column, &item.subPart); err != nil {
			return nil, err
		}
		if _, ok := grouped[item.name]; !ok {
			order = append(order, item.name)
		}
		grouped[item.name] = append(grouped[item.name], item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make([]mysqlUniqueIndexMetadata, 0)
	for _, name := range order {
		items := grouped[name]
		if len(items) == 0 {
			continue
		}
		if items[0].nonUnique != 0 && !strings.EqualFold(name, "PRIMARY") {
			continue
		}
		sort.Slice(items, func(i, j int) bool { return items[i].seq < items[j].seq })
		meta := mysqlUniqueIndexMetadata{
			Name:      name,
			IsPrimary: strings.EqualFold(name, "PRIMARY"),
			IsUnique:  true,
		}
		for _, item := range items {
			meta.Columns = append(meta.Columns, item.column)
			if item.subPart.Valid {
				meta.HasPrefix = true
			}
		}
		result = append(result, meta)
	}
	return result, nil
}

func foreignKeyMatchesStrictUniqueIndex(fk schemacompat.CanonicalConstraint, indexes []mysqlUniqueIndexMetadata) bool {
	if len(fk.ReferencedColumns) == 0 {
		return false
	}
	for _, idx := range indexes {
		if idx.HasPrefix {
			continue
		}
		if len(idx.Columns) != len(fk.ReferencedColumns) {
			continue
		}
		match := true
		for i := range idx.Columns {
			if !strings.EqualFold(idx.Columns[i], fk.ReferencedColumns[i]) {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func detectStrictForeignKeyIssues(db *sql.DB, fks []schemacompat.CanonicalConstraint) ([]schemacompat.CanonicalConstraint, error) {
	cache := make(map[string][]mysqlUniqueIndexMetadata)
	issues := make([]schemacompat.CanonicalConstraint, 0)

	for _, fk := range fks {
		if fk.ReferencedSchema == "" || fk.ReferencedTable == "" {
			continue
		}
		cacheKey := strings.ToLower(fmt.Sprintf("%s.%s", fk.ReferencedSchema, fk.ReferencedTable))
		indexes, ok := cache[cacheKey]
		if !ok {
			loaded, err := loadMySQLUniqueIndexMetadata(db, fk.ReferencedSchema, fk.ReferencedTable)
			if err != nil {
				return nil, err
			}
			indexes = loaded
			cache[cacheKey] = indexes
		}
		if !foreignKeyMatchesStrictUniqueIndex(fk, indexes) {
			issues = append(issues, fk)
		}
	}

	sort.Slice(issues, func(i, j int) bool {
		left := fmt.Sprintf("%s:%s.%s", issues[i].Name, issues[i].ReferencedSchema, issues[i].ReferencedTable)
		right := fmt.Sprintf("%s:%s.%s", issues[j].Name, issues[j].ReferencedSchema, issues[j].ReferencedTable)
		return left < right
	})
	return issues, nil
}

// preloadOracleForeignKeys runs a single all_constraints/all_cons_columns JOIN
// against Oracle to collect every FK definition across the given schemas. This
// replaces a per-table query loop (21 tables × ~3s on Oracle 11g = ~60s) with
// one batch query, and the result is served from memory in Foreign().
//
// The returned map is keyed by upper-cased schema/table/constraint, with values
// formatted identically to Oracle/or_scheme_table_column.go:Foreign so that
// canonicalization downstream sees an identical shape.
func preloadOracleForeignKeys(db *sql.DB, schemas []string, logThreadSeq int64) map[string]map[string]map[string]string {
	if db == nil || len(schemas) == 0 {
		return nil
	}
	uniq := make(map[string]struct{}, len(schemas))
	inList := make([]string, 0, len(schemas))
	for _, s := range schemas {
		up := strings.ToUpper(strings.TrimSpace(s))
		if up == "" {
			continue
		}
		if _, dup := uniq[up]; dup {
			continue
		}
		uniq[up] = struct{}{}
		inList = append(inList, "'"+strings.ReplaceAll(up, "'", "''")+"'")
	}
	if len(inList) == 0 {
		return nil
	}

	// Oracle 元数据视图的 OWNER 列默认按 unquoted identifier 存储为大写，
	// 上面已经用 strings.ToUpper 处理过 schema 名，这里直接等值匹配即可避免
	// UPPER(fk.OWNER) 包裹索引列导致 all_constraints.OWNER 上的索引失效。
	q := fmt.Sprintf(`SELECT fk.OWNER, fk.TABLE_NAME, fk.CONSTRAINT_NAME, fkcol.COLUMN_NAME, fkcol.POSITION, fk.R_OWNER, rk.TABLE_NAME AS REF_TABLE, rkcol.COLUMN_NAME AS REF_COLUMN, fk.DELETE_RULE FROM all_constraints fk JOIN all_cons_columns fkcol ON fk.OWNER=fkcol.OWNER AND fk.CONSTRAINT_NAME=fkcol.CONSTRAINT_NAME AND fk.TABLE_NAME=fkcol.TABLE_NAME JOIN all_constraints rk ON fk.R_OWNER=rk.OWNER AND fk.R_CONSTRAINT_NAME=rk.CONSTRAINT_NAME JOIN all_cons_columns rkcol ON rk.OWNER=rkcol.OWNER AND rk.CONSTRAINT_NAME=rkcol.CONSTRAINT_NAME AND rkcol.POSITION=fkcol.POSITION WHERE fk.CONSTRAINT_TYPE='R' AND fk.OWNER IN (%s) ORDER BY fk.OWNER, fk.TABLE_NAME, fk.CONSTRAINT_NAME, fkcol.POSITION`, strings.Join(inList, ","))

	rows, err := db.Query(q)
	if err != nil {
		global.Wlog.Warnf("(%d) [Q_Foreign_Batch] Oracle batch FK preload failed, fallback to per-table: %v", logThreadSeq, err)
		return nil
	}
	defer rows.Close()

	type fkKey struct{ schema, table, name string }
	type fkEntry struct {
		refOwner, refTable, deleteRule string
		fkCols, refCols                []string
	}
	entries := make(map[fkKey]*fkEntry)
	var order []fkKey

	for rows.Next() {
		var owner, table, name, col, refOwner, refTable, refCol, deleteRule sql.NullString
		var position sql.NullInt64
		if err := rows.Scan(&owner, &table, &name, &col, &position, &refOwner, &refTable, &refCol, &deleteRule); err != nil {
			global.Wlog.Warnf("(%d) [Q_Foreign_Batch] scan row failed: %v", logThreadSeq, err)
			return nil
		}
		k := fkKey{strings.ToUpper(owner.String), strings.ToUpper(table.String), strings.ToUpper(name.String)}
		e, ok := entries[k]
		if !ok {
			e = &fkEntry{
				refOwner:   strings.ToUpper(refOwner.String),
				refTable:   strings.ToUpper(refTable.String),
				deleteRule: strings.ToUpper(deleteRule.String),
			}
			entries[k] = e
			order = append(order, k)
		}
		e.fkCols = append(e.fkCols, strings.ToUpper(col.String))
		e.refCols = append(e.refCols, strings.ToUpper(refCol.String))
	}
	if err := rows.Err(); err != nil {
		global.Wlog.Warnf("(%d) [Q_Foreign_Batch] row iteration failed: %v", logThreadSeq, err)
		return nil
	}

	out := make(map[string]map[string]map[string]string)
	// Ensure every requested schema has an entry so downstream callers can
	// distinguish "preloaded but empty" from "not preloaded" via presence.
	for s := range uniq {
		out[s] = make(map[string]map[string]string)
	}
	for _, k := range order {
		e := entries[k]
		fkParts := make([]string, len(e.fkCols))
		for i, c := range e.fkCols {
			fkParts[i] = "!" + c + "!"
		}
		refParts := make([]string, len(e.refCols))
		for i, c := range e.refCols {
			refParts[i] = "!" + c + "!"
		}
		def := fmt.Sprintf("CONSTRAINT !%s! FOREIGN KEY (%s) REFERENCES !%s!.!%s! (%s)",
			k.name,
			strings.Join(fkParts, ", "),
			e.refOwner,
			e.refTable,
			strings.Join(refParts, ", "),
		)
		if e.deleteRule != "" && e.deleteRule != "NO ACTION" {
			def += " ON DELETE " + e.deleteRule
		}
		if _, ok := out[k.schema]; !ok {
			out[k.schema] = make(map[string]map[string]string)
		}
		if _, ok := out[k.schema][k.table]; !ok {
			out[k.schema][k.table] = make(map[string]string)
		}
		out[k.schema][k.table][k.name] = def
	}
	global.Wlog.Debug(fmt.Sprintf("(%d) [Q_Foreign_Batch] Oracle batch FK preload done: schemas=%d, fks=%d", logThreadSeq, len(uniq), len(order)))
	return out
}

// preloadOracleTableColumns batches per-table Q_table_columns lookups into one
// dba_tab_columns/dba_col_comments scan across the given schemas. Callers can
// then serve tableColumnName from memory instead of executing 21 separate
// per-table queries (~100ms each on Oracle 11g).
//
// The returned nested map uses upper-cased schema/table keys. Each inner slice
// matches the row shape produced by Oracle/or_scheme_table_column.go:
// TableColumnName so downstream code sees an identical input.
func preloadOracleTableColumns(db *sql.DB, schemas []string, logThreadSeq int64) map[string]map[string][]map[string]interface{} {
	if db == nil || len(schemas) == 0 {
		return nil
	}
	uniq := make(map[string]struct{}, len(schemas))
	inList := make([]string, 0, len(schemas))
	for _, s := range schemas {
		up := strings.ToUpper(strings.TrimSpace(s))
		if up == "" {
			continue
		}
		if _, dup := uniq[up]; dup {
			continue
		}
		uniq[up] = struct{}{}
		inList = append(inList, "'"+strings.ReplaceAll(up, "'", "''")+"'")
	}
	if len(inList) == 0 {
		return nil
	}

	// 使用与 Oracle/or_scheme_table_column.go:TableColumnName 完全一致的列裁剪
	// 逻辑，加上 OWNER 作为分组键；按 (OWNER,TABLE_NAME,COLUMN_ID) 排序确保
	// 与原单表查询相同的列顺序。
	q := fmt.Sprintf(`SELECT tc.OWNER AS "schemaName", tc.TABLE_NAME AS "tableName", tc.COLUMN_NAME AS "columnName", `+
		`DECODE(tc.DATA_TYPE, `+
		`'NUMBER', NVL2(DATA_PRECISION, 'NUMBER(' || tc.DATA_PRECISION || ',' || tc.DATA_SCALE || ')', 'NUMBER'), `+
		`'VARCHAR2', 'VARCHAR2(' || tc.DATA_LENGTH || ')', `+
		`'CHAR', 'CHAR(' || tc.DATA_LENGTH || ')', `+
		`'NCHAR', 'NCHAR(' || tc.CHAR_LENGTH || ')', `+
		`'NVARCHAR2', 'NVARCHAR2(' || tc.CHAR_LENGTH || ')', `+
		`'RAW', 'RAW(' || tc.DATA_LENGTH || ')', `+
		`'FLOAT', NVL2(tc.DATA_PRECISION, 'FLOAT(' || tc.DATA_PRECISION || ')', 'FLOAT'), `+
		`'TIMESTAMP', 'TIMESTAMP(' || NVL(tc.DATA_SCALE, 6) || ')', `+
		`tc.DATA_TYPE) AS "columnType", `+
		`tc.NULLABLE AS "isNull", `+
		`TO_NCHAR(cc.COMMENTS) AS "columnComment", `+
		`tc.DATA_DEFAULT AS "columnDefault" `+
		`FROM dba_tab_columns tc `+
		`JOIN dba_col_comments cc ON tc.OWNER=cc.OWNER AND tc.TABLE_NAME=cc.TABLE_NAME AND tc.COLUMN_NAME=cc.COLUMN_NAME `+
		`WHERE tc.OWNER IN (%s) `+
		`ORDER BY tc.OWNER, tc.TABLE_NAME, tc.COLUMN_ID`, strings.Join(inList, ","))

	rows, err := db.Query(q)
	if err != nil {
		global.Wlog.Warnf("(%d) [Q_table_columns_Batch] Oracle batch column preload failed, fallback to per-table: %v", logThreadSeq, err)
		return nil
	}
	defer rows.Close()

	out := make(map[string]map[string][]map[string]interface{})
	for s := range uniq {
		out[s] = make(map[string][]map[string]interface{})
	}
	var total int
	for rows.Next() {
		var schemaName, tableName, columnName, columnType, isNull, columnComment, columnDefault sql.NullString
		if err := rows.Scan(&schemaName, &tableName, &columnName, &columnType, &isNull, &columnComment, &columnDefault); err != nil {
			global.Wlog.Warnf("(%d) [Q_table_columns_Batch] scan row failed: %v", logThreadSeq, err)
			return nil
		}
		sch := strings.ToUpper(schemaName.String)
		tbl := strings.ToUpper(tableName.String)
		row := map[string]interface{}{
			"columnName":    columnName.String,
			"columnType":    columnType.String,
			"isNull":        isNull.String,
			"columnComment": columnComment.String,
			"columnDefault": columnDefault.String,
		}
		if _, ok := out[sch]; !ok {
			out[sch] = make(map[string][]map[string]interface{})
		}
		out[sch][tbl] = append(out[sch][tbl], row)
		total++
	}
	if err := rows.Err(); err != nil {
		global.Wlog.Warnf("(%d) [Q_table_columns_Batch] row iteration failed: %v", logThreadSeq, err)
		return nil
	}
	global.Wlog.Debug(fmt.Sprintf("(%d) [Q_table_columns_Batch] Oracle batch column preload done: schemas=%d, rows=%d", logThreadSeq, len(uniq), total))
	return out
}

// lookupOracleTableColumns returns the preloaded column rows for the given
// schema/table, or (nil, false) if the cache does not cover this schema.
// A schema entry with no table key means "preloaded but table empty"; in that
// case an empty slice + true is returned so callers skip the per-table query.
func lookupOracleTableColumns(cache map[string]map[string][]map[string]interface{}, schema, table string) ([]map[string]interface{}, bool) {
	if cache == nil {
		return nil, false
	}
	tabs, ok := cache[strings.ToUpper(schema)]
	if !ok {
		return nil, false
	}
	if rows, ok := tabs[strings.ToUpper(table)]; ok {
		return rows, true
	}
	return []map[string]interface{}{}, true
}

// preloadOracleIndexRows batches the ALL_TAB_COLS/ALL_IND_COLUMNS/ALL_INDEXES/
// ALL_CONSTRAINTS JOIN across multiple schemas into a single query. This
// replaces per-table execution (21 tables × ~1s on Oracle 11g = ~21s) with one
// schema-wide query served from memory.
//
// The returned map uses upper-cased schema/table keys. Each row matches the
// column shape produced by Oracle/or_query_table_data.go:QueryTableIndexColumnInfo
// (keys: columnName, columnType, columnKey, nonUnique, indexName, IndexSeq,
// columnSeq), so IndexDisposF sees an identical input.
func preloadOracleIndexRows(db *sql.DB, schemas []string, logThreadSeq int64) map[string]map[string][]map[string]interface{} {
	if db == nil || len(schemas) == 0 {
		return nil
	}
	uniq := make(map[string]struct{}, len(schemas))
	inList := make([]string, 0, len(schemas))
	for _, s := range schemas {
		up := strings.ToUpper(strings.TrimSpace(s))
		if up == "" {
			continue
		}
		if _, dup := uniq[up]; dup {
			continue
		}
		uniq[up] = struct{}{}
		inList = append(inList, "'"+strings.ReplaceAll(up, "'", "''")+"'")
	}
	if len(inList) == 0 {
		return nil
	}

	q := fmt.Sprintf(`SELECT c.OWNER AS "schemaName", c.TABLE_NAME AS "tableName", c.COLUMN_NAME AS "columnName", DECODE(c.DATA_TYPE, 'DATE', c.data_type, c.DATA_TYPE || '(' || c.data_LENGTH || ')') AS "columnType", DECODE(co.constraint_type, 'P', '1', '0') AS "columnKey", i.UNIQUENESS AS "nonUnique", ic.INDEX_NAME AS "indexName", ic.COLUMN_POSITION AS "IndexSeq", c.COLUMN_ID AS "columnSeq" FROM all_tab_cols c INNER JOIN all_ind_columns ic ON c.TABLE_NAME=ic.TABLE_NAME AND c.OWNER=ic.INDEX_OWNER AND c.COLUMN_NAME=ic.COLUMN_NAME INNER JOIN all_indexes i ON ic.INDEX_OWNER=i.OWNER AND ic.INDEX_NAME=i.INDEX_NAME AND ic.TABLE_NAME=i.TABLE_NAME LEFT JOIN all_constraints co ON co.owner=c.owner AND co.table_name=c.table_name AND co.index_name=i.index_name WHERE c.OWNER IN (%s) ORDER BY c.OWNER, c.TABLE_NAME, i.INDEX_NAME, ic.COLUMN_POSITION`, strings.Join(inList, ","))

	rows, err := db.Query(q)
	if err != nil {
		global.Wlog.Warnf("(%d) [Q_Index_Statistics_Batch] Oracle batch index preload failed, fallback to per-table: %v", logThreadSeq, err)
		return nil
	}
	defer rows.Close()

	out := make(map[string]map[string][]map[string]interface{})
	for s := range uniq {
		out[s] = make(map[string][]map[string]interface{})
	}
	for rows.Next() {
		var schemaName, tableName, columnName, columnType, columnKey, nonUnique, indexName sql.NullString
		var indexSeq, columnSeq sql.NullInt64
		if err := rows.Scan(&schemaName, &tableName, &columnName, &columnType, &columnKey, &nonUnique, &indexName, &indexSeq, &columnSeq); err != nil {
			global.Wlog.Warnf("(%d) [Q_Index_Statistics_Batch] scan row failed: %v", logThreadSeq, err)
			return nil
		}
		sch := strings.ToUpper(schemaName.String)
		tbl := strings.ToUpper(tableName.String)
		row := map[string]interface{}{
			"columnName": columnName.String,
			"columnType": columnType.String,
			"columnKey":  columnKey.String,
			"nonUnique":  nonUnique.String,
			"indexName":  indexName.String,
			"IndexSeq":   strconv.FormatInt(indexSeq.Int64, 10),
			"columnSeq":  strconv.FormatInt(columnSeq.Int64, 10),
		}
		if _, ok := out[sch]; !ok {
			out[sch] = make(map[string][]map[string]interface{})
		}
		out[sch][tbl] = append(out[sch][tbl], row)
	}
	if err := rows.Err(); err != nil {
		global.Wlog.Warnf("(%d) [Q_Index_Statistics_Batch] row iteration failed: %v", logThreadSeq, err)
		return nil
	}
	global.Wlog.Debug(fmt.Sprintf("(%d) [Q_Index_Statistics_Batch] Oracle batch index preload done: schemas=%d", logThreadSeq, len(uniq)))
	return out
}

// lookupOracleIndexRows returns the preloaded index rows for (schema, table).
// Returns (rows, true) when the schema was preloaded (even if the table has no
// indexes, in which case rows is an empty slice). (nil, false) means the
// caller must fall back to a live query.
func lookupOracleIndexRows(cache map[string]map[string][]map[string]interface{}, schema, table string) ([]map[string]interface{}, bool) {
	if cache == nil {
		return nil, false
	}
	tabs, ok := cache[strings.ToUpper(schema)]
	if !ok {
		return nil, false
	}
	if rows, ok := tabs[strings.ToUpper(table)]; ok {
		return rows, true
	}
	return []map[string]interface{}{}, true
}

// lookupForeignKeyCache returns the preloaded FK definitions for the given
// schema/table, or nil if not present. Keys are case-insensitive.
func lookupForeignKeyCache(cache map[string]map[string]map[string]string, schema, table string) (map[string]string, bool) {
	if cache == nil {
		return nil, false
	}
	tabs, ok := cache[strings.ToUpper(schema)]
	if !ok {
		return nil, false
	}
	fks, ok := tabs[strings.ToUpper(table)]
	if !ok {
		// Schema was preloaded but this table has no FKs — return empty map.
		return map[string]string{}, true
	}
	return fks, true
}
