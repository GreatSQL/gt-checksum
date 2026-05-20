package actions

import (
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
	"sort"
	"strings"
)

func mergeStructDiffState(current, incoming string) string {
	switch strings.TrimSpace(incoming) {
	case global.SkipDiffsYes, global.SkipDiffsDDLYes:
		return global.SkipDiffsYes
	case global.SkipDiffsWarnOnly:
		if strings.TrimSpace(current) == global.SkipDiffsYes || strings.TrimSpace(current) == global.SkipDiffsDDLYes {
			return global.SkipDiffsYes
		}
		return global.SkipDiffsWarnOnly
	case global.SkipDiffsCollationMapped:
		cur := strings.TrimSpace(current)
		if cur == global.SkipDiffsYes || cur == global.SkipDiffsDDLYes || cur == global.SkipDiffsWarnOnly {
			return cur
		}
		return global.SkipDiffsCollationMapped
	default:
		if strings.TrimSpace(current) == "" {
			return global.SkipDiffsNo
		}
		return current
	}
}

func shouldUseCaseSensitiveColumnMatching(sourceDrive, destDrive, caseSensitiveObjectName string, oracleToMySQLDataMode bool) bool {
	if oracleToMySQLDataMode {
		return false
	}
	// MySQL column identifiers are matched case-insensitively even when table
	// name handling remains case-sensitive on the host filesystem.
	if strings.EqualFold(sourceDrive, "mysql") && strings.EqualFold(destDrive, "mysql") {
		return false
	}
	return strings.EqualFold(caseSensitiveObjectName, "yes")
}

// tableExistenceCacheKey 构造 tableExistenceCache 的缓存键。
// 使用 db 指针地址区分源/目标连接，避免同 drive+schema 场景下表名混淆。
func tableExistenceCacheKey(db *sql.DB, drive, schema string) string {
	return fmt.Sprintf("%p|%s|%s", db, drive, strings.ToUpper(schema))
}

// preloadTableExistence 预加载 schemas 内所有 BASE TABLE 名称，写入 stcls.tableExistenceCache。
// Oracle 走 ALL_TABLES、MySQL 走 INFORMATION_SCHEMA.TABLES，均只发送一次 SQL。
func (stcls *schemaTable) preloadTableExistence(db *sql.DB, drive string, schemas []string) {
	if db == nil || len(schemas) == 0 {
		return
	}
	if stcls.tableExistenceCache == nil {
		stcls.tableExistenceCache = make(map[string]map[string]struct{})
	}
	upperSchemas := make([]string, 0, len(schemas))
	seen := make(map[string]struct{}, len(schemas))
	for _, s := range schemas {
		if s == "" {
			continue
		}
		k := strings.ToUpper(s)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		upperSchemas = append(upperSchemas, k)
		cacheKey := tableExistenceCacheKey(db, drive, k)
		if _, ok := stcls.tableExistenceCache[cacheKey]; !ok {
			stcls.tableExistenceCache[cacheKey] = make(map[string]struct{})
		}
	}
	if len(upperSchemas) == 0 {
		return
	}
	quoted := make([]string, 0, len(upperSchemas))
	for _, s := range upperSchemas {
		quoted = append(quoted, "'"+escapeSQLLiteral(s)+"'")
	}
	var query string
	if isOracleDrive(drive) {
		query = fmt.Sprintf("SELECT UPPER(owner) AS owner, UPPER(table_name) AS table_name FROM all_tables WHERE UPPER(owner) IN (%s)", strings.Join(quoted, ","))
	} else {
		query = fmt.Sprintf("SELECT UPPER(TABLE_SCHEMA) AS owner, UPPER(TABLE_NAME) AS table_name FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) IN (%s) AND TABLE_TYPE='BASE TABLE'", strings.Join(quoted, ","))
	}
	rows, err := db.Query(query)
	if err != nil {
		global.Wlog.Warn(fmt.Sprintf("preloadTableExistence failed for drive=%s: %v", drive, err))
		return
	}
	defer rows.Close()
	for rows.Next() {
		var owner, tableName string
		if err := rows.Scan(&owner, &tableName); err != nil {
			continue
		}
		cacheKey := tableExistenceCacheKey(db, drive, owner)
		if _, ok := stcls.tableExistenceCache[cacheKey]; !ok {
			stcls.tableExistenceCache[cacheKey] = make(map[string]struct{})
		}
		stcls.tableExistenceCache[cacheKey][strings.ToUpper(tableName)] = struct{}{}
	}
}

func cloneSQLStatements(sqls []string) []string {
	if len(sqls) == 0 {
		return nil
	}
	cloned := make([]string, len(sqls))
	copy(cloned, sqls)
	return cloned
}

func queryVersionInfoFromDB(db *sql.DB) (global.MySQLVersionInfo, error) {
	if db == nil {
		return global.MySQLVersionInfo{}, fmt.Errorf("db is nil")
	}

	var version string
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		return global.MySQLVersionInfo{}, err
	}
	return global.ParseMySQLVersion(version)
}

func (stcls *schemaTable) getDisplayTableName(schema, table string) string {
	// 检查是否存在映射关系
	if mappedSchema, exists := stcls.tableMappings[schema]; exists && mappedSchema != schema {
		// 存在映射关系，返回包含映射信息的名称
		return fmt.Sprintf("%s.%s:%s.%s", schema, table, mappedSchema, table)
	}

	// 不存在映射关系，返回普通名称
	return fmt.Sprintf("%s.%s", schema, table)
}

func (stcls *schemaTable) getSourceTableName(schema, table string) string {
	return fmt.Sprintf("%s.%s", schema, table)
}

func isOracleDriveName(drive string) bool {
	return strings.EqualFold(drive, "godror") || strings.EqualFold(drive, "oracle")
}

func splitSchemaTableCacheKey(key string) (string, string, bool) {
	parts := strings.SplitN(key, "/*schema&table*/", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func (stcls *schemaTable) sourceObjectNameEqual(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if isOracleDriveName(stcls.sourceDrive) {
		return strings.EqualFold(a, b)
	}
	if strings.EqualFold(stcls.caseSensitiveObjectName, "yes") {
		return a == b
	}
	return strings.EqualFold(a, b)
}

func (stcls *schemaTable) destObjectNameEqual(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if isOracleDriveName(stcls.destDrive) {
		return strings.EqualFold(a, b)
	}
	if strings.EqualFold(stcls.caseSensitiveObjectName, "yes") {
		return a == b
	}
	return strings.EqualFold(a, b)
}

func (stcls *schemaTable) findMappedSchema(sourceSchema string) (string, bool) {
	if mapped, ok := stcls.tableMappings[sourceSchema]; ok {
		return mapped, true
	}
	for src, dst := range stcls.tableMappings {
		if stcls.sourceObjectNameEqual(src, sourceSchema) {
			return dst, true
		}
	}
	return "", false
}

func (stcls *schemaTable) tableKeyInSet(tableSet map[string]int, schema, table string) bool {
	for key := range tableSet {
		parts := strings.SplitN(key, ".", 2)
		if len(parts) != 2 {
			continue
		}
		if stcls.sourceObjectNameEqual(parts[0], schema) && stcls.sourceObjectNameEqual(parts[1], table) {
			return true
		}
	}
	return false
}

func splitSourcePattern(pattern string) (string, string, bool) {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return "", "", false
	}
	if strings.Contains(pattern, ":") {
		pattern = strings.SplitN(pattern, ":", 2)[0]
	}
	parts := strings.SplitN(pattern, ".", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), true
}

func hasObjectWildcard(pattern string) bool {
	return strings.Contains(pattern, "*") || strings.Contains(pattern, "%")
}

func (stcls *schemaTable) isExplicitSourceTableSelection(schema, table string) bool {
	for _, pattern := range strings.Split(stcls.table, ",") {
		sourceSchema, sourceTable, ok := splitSourcePattern(pattern)
		if !ok || hasObjectWildcard(sourceSchema) || hasObjectWildcard(sourceTable) {
			continue
		}
		if stcls.sourceObjectNameEqual(sourceSchema, schema) && stcls.sourceObjectNameEqual(sourceTable, table) {
			return true
		}
	}
	return false
}

func (stcls *schemaTable) recordIgnoredMatchedTable(schema, table string) {
	qualifiedName := fmt.Sprintf("%s.%s", schema, table)
	for _, existing := range stcls.ignoredMatchedTables {
		parts := strings.SplitN(existing, ".", 2)
		if len(parts) != 2 {
			continue
		}
		if stcls.sourceObjectNameEqual(parts[0], schema) && stcls.sourceObjectNameEqual(parts[1], table) {
			return
		}
	}
	stcls.ignoredMatchedTables = append(stcls.ignoredMatchedTables, qualifiedName)
}

func (stcls *schemaTable) IgnoredMatchedTablesSummary() string {
	if len(stcls.ignoredMatchedTables) == 0 {
		return ""
	}
	summary := append([]string(nil), stcls.ignoredMatchedTables...)
	sort.Strings(summary)
	return strings.Join(summary, ", ")
}

func (stcls *schemaTable) shouldIgnoreMatchedTable(ignoreSchema map[string]int, schema, table string) bool {
	if !stcls.tableKeyInSet(ignoreSchema, schema, table) {
		return false
	}
	if stcls.isExplicitSourceTableSelection(schema, table) {
		if global.Wlog != nil {
			global.Wlog.Warn(fmt.Sprintf("Explicitly selected table %s.%s also matches ignoreTables; keeping it in the checklist", schema, table))
		}
		return false
	}
	stcls.recordIgnoredMatchedTable(schema, table)
	return true
}

func (stcls *schemaTable) getDestTableName(schema, table string) string {
	destSchema := schema
	if mappedSchema, exists := stcls.tableMappings[schema]; exists {
		destSchema = mappedSchema
	}
	return fmt.Sprintf("%s.%s", destSchema, table)
}

func escapeSQLLiteral(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func escapeMySQLIdentifier(value string) string {
	return strings.ReplaceAll(value, "`", "``")
}

func isOracleDrive(drive string) bool {
	return drive == "godror" || strings.EqualFold(drive, "oracle")
}

func (stcls *schemaTable) isMySQLToMySQL() bool {
	return strings.EqualFold(stcls.sourceDrive, "mysql") && strings.EqualFold(stcls.destDrive, "mysql")
}

func (stcls *schemaTable) isMariaDBToMySQL() bool {
	return stcls.sourceVersionInfo().Flavor == global.DatabaseFlavorMariaDB &&
		stcls.destVersionInfo().Flavor == global.DatabaseFlavorMySQL
}

func (stcls *schemaTable) isOracleToMySQL() bool {
	return isOracleDrive(stcls.sourceDrive) &&
		stcls.destVersionInfo().Flavor == global.DatabaseFlavorMySQL
}

func normalizeMetadataComment(v string) string {
	s := strings.TrimSpace(v)
	switch strings.ToLower(s) {
	case "", "<entry>", "<nil>", "null":
		return ""
	default:
		return s
	}
}

// applyDTypeMappingOverrides 根据 dTypeMapping 规则将 nullable/default 覆盖写入 repairAttrs
// repairAttrs 布局：[0]=type [1]=charset [2]=collation [3]=nullable [4]=default [5]=comment
// sourceType 为列的原始源端类型（在 repairPlan 改写 repairAttrs[0] 之前保存），用于规则匹配；
// 传空字符串时回退到 repairAttrs[0]（兼容旧调用路径）。
func applyDTypeMappingOverrides(repairAttrs []string, colName string, isOracleToMySQL, isMariaDBToMySQL bool, sourceType string, schema, table string) {
	if schemacompat.GlobalDTypeMappingRules == nil || len(repairAttrs) < 5 {
		return
	}
	var rules []schemacompat.TypeMappingRule
	if isOracleToMySQL {
		rules = schemacompat.GlobalDTypeMappingRules.DTypeMapping.OracleToMySQL
	} else if isMariaDBToMySQL {
		rules = schemacompat.GlobalDTypeMappingRules.DTypeMapping.MariaDBToMySQL
	} else {
		rules = schemacompat.GlobalDTypeMappingRules.DTypeMapping.MySQLUpgrade
	}
	if len(rules) == 0 {
		return
	}
	matchType := sourceType
	if strings.TrimSpace(matchType) == "" {
		matchType = repairAttrs[0]
	}
	sourceNullable := strings.ToUpper(strings.TrimSpace(repairAttrs[3])) != "NO"
	autoInc := strings.Contains(strings.ToLower(matchType), "auto_increment")
	ctx := schemacompat.BuildMappingContext(matchType, sourceNullable, colName, autoInc, schema, table)
	rule, _, matched := schemacompat.MatchUserRuleWithOverrides(rules, ctx)
	if !matched || rule == nil {
		return
	}
	// 覆盖 nullable
	if rule.Nullable != nil {
		if *rule.Nullable {
			repairAttrs[3] = "YES"
		} else {
			repairAttrs[3] = "NO"
		}
	}
	// 覆盖 default
	if rule.Default != nil {
		switch v := rule.Default.(type) {
		case string:
			repairAttrs[4] = v
		case bool:
			if v {
				repairAttrs[4] = "1"
			} else {
				repairAttrs[4] = "0"
			}
		case int64:
			repairAttrs[4] = fmt.Sprintf("%d", v)
		case float64:
			repairAttrs[4] = fmt.Sprintf("%g", v)
		}
	}
	// 覆盖 unsigned：将 UNSIGNED 属性写入 repairAttrs[0]（类型字符串）
	if rule.Unsigned != nil {
		typeUpper := strings.ToUpper(repairAttrs[0])
		if *rule.Unsigned {
			if !strings.Contains(typeUpper, "UNSIGNED") {
				repairAttrs[0] = repairAttrs[0] + " unsigned"
			}
		} else {
			if strings.Contains(typeUpper, "UNSIGNED") {
				repairAttrs[0] = strings.ReplaceAll(repairAttrs[0], " unsigned", "")
				repairAttrs[0] = strings.ReplaceAll(repairAttrs[0], " UNSIGNED", "")
			}
		}
	}
	// 覆盖 autoinc：将 AUTO_INCREMENT 属性写入 repairAttrs[0]（类型字符串）
	if rule.AutoInc != nil {
		typeUpper := strings.ToUpper(repairAttrs[0])
		if *rule.AutoInc {
			if !strings.Contains(typeUpper, "AUTO_INCREMENT") {
				repairAttrs[0] = repairAttrs[0] + " AUTO_INCREMENT"
			}
		} else {
			if strings.Contains(typeUpper, "AUTO_INCREMENT") {
				repairAttrs[0] = strings.ReplaceAll(repairAttrs[0], " AUTO_INCREMENT", "")
				repairAttrs[0] = strings.ReplaceAll(repairAttrs[0], "auto_increment", "")
			}
		}
	}
}

func listMariaDBSequenceNames(db *sql.DB, schema string) ([]string, error) {
	rows, err := db.Query(`
SELECT TABLE_NAME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'SEQUENCE'
ORDER BY TABLE_NAME
`, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sequenceNames := make([]string, 0)
	for rows.Next() {
		var sequenceName string
		if err := rows.Scan(&sequenceName); err != nil {
			return nil, err
		}
		sequenceNames = append(sequenceNames, sequenceName)
	}
	return sequenceNames, rows.Err()
}

func (stcls *schemaTable) emitMariaDBSequenceWarnings(checkTableList []string, logThreadSeq int64) {
	if stcls.sourceVersionInfo().Flavor != global.DatabaseFlavorMariaDB || stcls.destVersionInfo().Flavor != global.DatabaseFlavorMySQL {
		return
	}

	for _, sourceSchema := range collectSourceSchemasForStructCheck(checkTableList) {
		sequenceNames, err := listMariaDBSequenceNames(stcls.sourceDB, sourceSchema)
		if err != nil {
			global.Wlog.Warn(fmt.Sprintf("(%d) Failed to list MariaDB sequences for schema %s: %v", logThreadSeq, sourceSchema, err))
			continue
		}
		if len(sequenceNames) == 0 {
			continue
		}

		destSchema := stcls.mappedDestSchema(sourceSchema)
		suggestions := schemacompat.BuildMariaDBSequenceObjectSuggestions(sourceSchema, sequenceNames)
		for idx, sequenceName := range sequenceNames {
			stcls.appendPod(Pod{
				Schema:      sourceSchema,
				Table:       sequenceName,
				CheckObject: "Sequence",
				DIFFS:       global.SkipDiffsWarnOnly,
				Datafix:     stcls.datafix,
			})

			scope := fmt.Sprintf("%s.%s SEQUENCE", destSchema, sequenceName)
			advisoryLines := buildConstraintAdvisoryLines(scope, []schemacompat.ConstraintRepairSuggestion{suggestions[idx]})

			originalSchema, originalTable, originalDestTable := stcls.schema, stcls.table, stcls.destTable
			stcls.schema = destSchema
			stcls.table = sequenceName
			stcls.destTable = sequenceName
			if err := stcls.writeAdvisoryFixSql(advisoryLines, logThreadSeq); err != nil {
				global.Wlog.Error(fmt.Sprintf("(%d) Failed to write SEQUENCE advisory SQL for %s.%s: %v", logThreadSeq, sourceSchema, sequenceName, err))
			}
			stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
		}
	}
}

func nullInt64ForLog(v sql.NullInt64) interface{} {
	if !v.Valid {
		return nil
	}
	return v.Int64
}

func escapeMySQLCommentLiteral(v string) string {
	s := strings.ReplaceAll(v, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}

func extractMySQLObjectCommentFromCreate(createSQL string) string {
	matches := mysqlCreateObjectCommentPattern.FindStringSubmatch(createSQL)
	if len(matches) < 2 {
		return ""
	}
	return normalizeMetadataComment(strings.ReplaceAll(matches[1], `\'`, `'`))
}

func (stcls *schemaTable) mappedDestSchema(sourceSchema string) string {
	if mappedSchema, exists := stcls.tableMappings[sourceSchema]; exists && strings.TrimSpace(mappedSchema) != "" {
		return mappedSchema
	}
	return sourceSchema
}

func (stcls *schemaTable) tableExistsByDrive(db *sql.DB, drive, schema, table, objectKind string) (bool, error) {
	var (
		count int
		query string
	)

	// 仅 BASE TABLE 校验可走预加载缓存；VIEW 仍回源查询以保留原有语义。
	if stcls != nil && stcls.tableExistenceCache != nil {
		kind := strings.ToLower(strings.TrimSpace(objectKind))
		if kind == "" || kind == "table" {
			if tables, ok := stcls.tableExistenceCache[tableExistenceCacheKey(db, drive, schema)]; ok {
				_, exists := tables[strings.ToUpper(table)]
				return exists, nil
			}
		}
	}

	if isOracleDrive(drive) {
		query = fmt.Sprintf(
			"SELECT COUNT(1) FROM all_tables WHERE UPPER(owner)=UPPER('%s') AND UPPER(table_name)=UPPER('%s')",
			escapeSQLLiteral(schema),
			escapeSQLLiteral(table),
		)
	} else {
		tableTypeCond := " AND TABLE_TYPE='BASE TABLE'"
		if strings.ToLower(strings.TrimSpace(objectKind)) == "view" {
			tableTypeCond = " AND TABLE_TYPE='VIEW'"
		}
		if strings.ToLower(stcls.caseSensitiveObjectName) == "yes" {
			query = fmt.Sprintf(
				"SELECT COUNT(1) FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'%s",
				escapeSQLLiteral(schema),
				escapeSQLLiteral(table),
				tableTypeCond,
			)
		} else {
			query = fmt.Sprintf(
				"SELECT COUNT(1) FROM information_schema.TABLES WHERE LOWER(TABLE_SCHEMA)=LOWER('%s') AND LOWER(TABLE_NAME)=LOWER('%s')%s",
				escapeSQLLiteral(schema),
				escapeSQLLiteral(table),
				tableTypeCond,
			)
		}
	}

	if err := db.QueryRow(query).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}
