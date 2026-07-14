package actions

import (
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
)

// stripPartitionEngineAnnotations removes per-partition ENGINE=<engine> trailing
// annotations from SHOW CREATE TABLE output. These vary across MySQL/MariaDB
// versions or forks (e.g. "ENGINE = InnoDB" vs "ENGINE=INNODB") and do not
// affect partition structure semantics.
func stripPartitionEngineAnnotations(value string) string {
	// Match ENGINE= or ENGINE = followed by the engine name, possibly at end of
	// a partition definition line (before comma, closing paren, or end of string).
	// Use (?i) for case-insensitive matching of "ENGINE" and engine names.
	re := regexp.MustCompile(`(?i)\s*ENGINE\s*=\s*\w+\s*`)
	return re.ReplaceAllString(value, "")
}

func normalizePartitionCompareText(value string) string {
	normalized := strings.TrimSpace(value)
	normalized = strings.ReplaceAll(normalized, "`", "")
	normalized = strings.ReplaceAll(normalized, "!", "")
	normalized = strings.Join(strings.Fields(normalized), " ")
	normalized = partitionDelimiterSpacingPattern.ReplaceAllString(normalized, "$1")
	return strings.ToUpper(normalized)
}

func normalizePartitionFullDefinition(value string) string {
	normalized := strings.TrimSpace(value)
	for {
		matches := mysqlVersionedCommentWrapperPattern.FindStringSubmatch(normalized)
		if len(matches) != 2 {
			break
		}
		// SHOW CREATE TABLE may wrap the same partition clause in a versioned
		// comment on one side but not the other. The wrapper itself is metadata
		// noise and should not affect semantic comparison.
		normalized = strings.TrimSpace(matches[1])
	}
	// Strip per-partition ENGINE=<engine> trailing annotations that vary across
	// MySQL/MariaDB versions or forks (e.g. "ENGINE = InnoDB" vs "ENGINE=INNODB").
	// These are storage-engine metadata noise and do not affect partition semantics.
	normalized = stripPartitionEngineAnnotations(normalized)
	return normalizePartitionCompareText(normalized)
}

func parsePartitionMetadataEntries(partitions map[string]string, tableKey string) []partitionMetadata {
	entries := make([]partitionMetadata, 0)
	prefix := tableKey + "."
	for key, value := range partitions {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		matches := partitionMetadataPattern.FindStringSubmatch(value)
		if len(matches) != 7 {
			continue
		}
		ordinal, err := strconv.Atoi(strings.TrimSpace(matches[2]))
		if err != nil {
			continue
		}
		entries = append(entries, partitionMetadata{
			Name:        strings.TrimSpace(matches[1]),
			Ordinal:     ordinal,
			Method:      strings.TrimSpace(matches[3]),
			Expression:  strings.TrimSpace(matches[4]),
			Description: strings.TrimSpace(matches[5]),
			Rows:        strings.TrimSpace(matches[6]),
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Ordinal == entries[j].Ordinal {
			return entries[i].Name < entries[j].Name
		}
		return entries[i].Ordinal < entries[j].Ordinal
	})
	return entries
}

func partitionRowsReportedEmpty(meta partitionMetadata) bool {
	rows := strings.TrimSpace(meta.Rows)
	if rows == "" {
		return false
	}
	value, err := strconv.ParseFloat(rows, 64)
	if err != nil {
		return false
	}
	return value == 0
}

func buildPartitionValidationQuery(schemaName, tableName, partitionName string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` PARTITION (`%s`);", schemaName, tableName, partitionName)
}

func buildDropPartitionAdvisoryLines(schemaName, tableName string, partitions []partitionMetadata) []string {
	if len(partitions) == 0 {
		return nil
	}
	lines := []string{
		fmt.Sprintf("-- gt-checksum advisory begin: %s.%s partition repair", schemaName, tableName),
	}
	for _, partition := range partitions {
		lines = append(lines, "-- 请在确认该分区不存在任何数据后再执行此操作")
		lines = append(lines, fmt.Sprintf("-- 注意: TABLE_ROWS 是 InnoDB 估算值，可能不准确，请务必用 SELECT COUNT(*) 核实"))
		lines = append(lines, fmt.Sprintf("-- %s", buildPartitionValidationQuery(schemaName, tableName, partition.Name)))
		lines = append(lines, fmt.Sprintf("-- ALTER TABLE `%s`.`%s` DROP PARTITION `%s`;", schemaName, tableName, partition.Name))
	}
	lines = append(lines, fmt.Sprintf("-- gt-checksum advisory end: %s.%s partition repair", schemaName, tableName))
	return lines
}

// toDaysToReadableDate attempts to convert a TO_DAYS() integer value back to a
// human-readable date string. Returns empty string if the value is not a valid
// TO_DAYS result (valid range: 1-3652424, corresponding to year 0000-9999).
//
// MySQL TO_DAYS() reference points:
//   - TO_DAYS('0000-01-01') = 1
//   - TO_DAYS('2024-01-01') = 739251
//   - TO_DAYS('2024-02-01') = 739282
func toDaysToReadableDate(daysStr string) string {
	days, err := strconv.Atoi(strings.TrimSpace(daysStr))
	if err != nil || days < 1 || days > 3652424 {
		return ""
	}
	// Use a known reference point to calculate the date:
	// TO_DAYS('2024-01-01') = 739251
	refDays := 739251
	refDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	// Calculate offset from reference
	offset := days - refDays
	t := refDate.AddDate(0, 0, offset)
	return t.Format("2006-01-02")
}

func formatPartitionDescriptionForAdd(meta partitionMetadata) (string, bool) {
	description := strings.TrimSpace(meta.Description)
	if description == "" {
		return "", false
	}
	method := normalizePartitionCompareText(meta.Method)

	switch {
	case strings.HasPrefix(method, "RANGE"):
		if strings.EqualFold(description, "MAXVALUE") {
			if strings.Contains(method, "COLUMNS") {
				return "(MAXVALUE)", true
			}
			return "MAXVALUE", true
		}
		// For TO_DAYS partitions, try to convert integer boundary to readable date
		if strings.Contains(method, "TO_DAYS") {
			// Description might be a plain integer like "739282"
			if readableDate := toDaysToReadableDate(description); readableDate != "" {
				return fmt.Sprintf("(TO_DAYS('%s'))", readableDate), true
			}
			// Description might already be in TO_DAYS('YYYY-MM-DD') format
			// or might be wrapped in parentheses
		}
		if strings.HasPrefix(description, "(") && strings.HasSuffix(description, ")") {
			return description, true
		}
		return fmt.Sprintf("(%s)", description), true
	case strings.HasPrefix(method, "LIST"):
		if strings.HasPrefix(description, "(") && strings.HasSuffix(description, ")") {
			return description, true
		}
		return fmt.Sprintf("(%s)", description), true
	default:
		return "", false
	}
}

func buildAddPartitionClause(meta partitionMetadata) (string, bool) {
	formattedDescription, ok := formatPartitionDescriptionForAdd(meta)
	if !ok {
		return "", false
	}
	method := normalizePartitionCompareText(meta.Method)
	switch {
	case strings.HasPrefix(method, "RANGE"):
		return fmt.Sprintf("PARTITION `%s` VALUES LESS THAN %s", meta.Name, formattedDescription), true
	case strings.HasPrefix(method, "LIST"):
		return fmt.Sprintf("PARTITION `%s` VALUES IN %s", meta.Name, formattedDescription), true
	default:
		return "", false
	}
}

func buildAddPartitionSQL(schemaName, tableName string, partitions []partitionMetadata) []string {
	clauses := make([]string, 0, len(partitions))
	for _, partition := range partitions {
		clause, ok := buildAddPartitionClause(partition)
		if !ok {
			return nil
		}
		clauses = append(clauses, clause)
	}
	if len(clauses) == 0 {
		return nil
	}
	return []string{
		fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PARTITION (%s);", schemaName, tableName, strings.Join(clauses, ", ")),
	}
}

// partitionMethodCompatible checks whether source and dest partitions use the
// same partition method and expression. If these differ, automatic repair is
// not possible.
func partitionMethodCompatible(sourceEntries, destEntries []partitionMetadata) bool {
	if len(sourceEntries) == 0 || len(destEntries) == 0 {
		return false
	}
	// Compare method and expression from the first entry (they should be
	// identical across all entries for the same table).
	srcMethod := normalizePartitionCompareText(sourceEntries[0].Method)
	dstMethod := normalizePartitionCompareText(destEntries[0].Method)
	if srcMethod != dstMethod {
		return false
	}
	srcExpr := normalizePartitionCompareText(sourceEntries[0].Expression)
	dstExpr := normalizePartitionCompareText(destEntries[0].Expression)
	return srcExpr == dstExpr
}

// buildReorganizePartitionSQL generates a REORGANIZE PARTITION statement that
// splits a MAXVALUE partition into new partitions while preserving the MAXVALUE
// partition at the end.
func buildReorganizePartitionSQL(schemaName, tableName string, maxPart partitionMetadata, newParts []partitionMetadata) (string, error) {
	if len(newParts) == 0 {
		return "", fmt.Errorf("no new partitions specified for REORGANIZE")
	}
	// Sort new partitions by ordinal to maintain logical order
	sort.Slice(newParts, func(i, j int) bool {
		return newParts[i].Ordinal < newParts[j].Ordinal
	})
	clauses := make([]string, 0, len(newParts)+1)
	for _, part := range newParts {
		clause, ok := buildAddPartitionClause(part)
		if !ok {
			return "", fmt.Errorf("cannot format partition %s for REORGANIZE", part.Name)
		}
		clauses = append(clauses, clause)
	}
	// Add the MAXVALUE partition at the end
	maxClause, ok := buildAddPartitionClause(maxPart)
	if !ok {
		return "", fmt.Errorf("cannot format MAXVALUE partition %s for REORGANIZE", maxPart.Name)
	}
	clauses = append(clauses, maxClause)
	return fmt.Sprintf("ALTER TABLE `%s`.`%s` REORGANIZE PARTITION `%s` INTO (%s);",
		schemaName, tableName, maxPart.Name, strings.Join(clauses, ", ")), nil
}

func buildPartitionRepairSQLs(sourceSchema, sourceTable, destSchema, destTable string, sourcePartitions, destPartitions map[string]string) ([]string, []string, bool, string) {
	sourceTableKey := fmt.Sprintf("%s.%s", sourceSchema, sourceTable)
	destTableKey := fmt.Sprintf("%s.%s", destSchema, destTable)
	sourceEntries := parsePartitionMetadataEntries(sourcePartitions, sourceTableKey)
	destEntries := parsePartitionMetadataEntries(destPartitions, destTableKey)

	if len(sourceEntries) == 0 || len(destEntries) == 0 {
		return nil, nil, false, "partition metadata is incomplete"
	}

	// Check partition method compatibility (RANGE/LIST/HASH and expression must match)
	if !partitionMethodCompatible(sourceEntries, destEntries) {
		return nil, nil, false, "partition method or expression differs"
	}

	// Build name→metadata maps for set-based comparison
	sourceMap := make(map[string]partitionMetadata, len(sourceEntries))
	for _, entry := range sourceEntries {
		sourceMap[strings.ToUpper(entry.Name)] = entry
	}
	destMap := make(map[string]partitionMetadata, len(destEntries))
	for _, entry := range destEntries {
		destMap[strings.ToUpper(entry.Name)] = entry
	}

	// Identify partitions to drop (in dest but not in source) and add (in source but not in dest)
	var toDrop []partitionMetadata
	var toAdd []partitionMetadata

	for name, entry := range destMap {
		if _, exists := sourceMap[name]; !exists {
			toDrop = append(toDrop, entry)
		}
	}
	for name, entry := range sourceMap {
		if _, exists := destMap[name]; !exists {
			toAdd = append(toAdd, entry)
		}
	}

	// If no differences found, partitions are consistent
	if len(toDrop) == 0 && len(toAdd) == 0 {
		return nil, nil, true, "partition sets are identical after name-based comparison"
	}

	// Sort for deterministic output
	sort.Slice(toDrop, func(i, j int) bool { return toDrop[i].Ordinal < toDrop[j].Ordinal })
	sort.Slice(toAdd, func(i, j int) bool { return toAdd[i].Ordinal < toAdd[j].Ordinal })

	// Check for MAXVALUE partition in dest (need REORGANIZE for inserts before MAXVALUE)
	var maxPart *partitionMetadata
	for _, entry := range destEntries {
		if strings.EqualFold(strings.TrimSpace(entry.Description), "MAXVALUE") {
			maxPart = &entry
			break
		}
	}

	var execRepairSQLs []string
	var advisoryRepairSQLs []string

	// Handle partitions to drop (dest has, source doesn't)
	if len(toDrop) > 0 {
		allEmpty := true
		for _, partition := range toDrop {
			if !partitionRowsReportedEmpty(partition) {
				allEmpty = false
				break
			}
		}
		if allEmpty {
			// All extra partitions are empty - generate advisory DROP
			advisoryRepairSQLs = append(advisoryRepairSQLs, buildDropPartitionAdvisoryLines(destSchema, destTable, toDrop)...)
		} else {
			// Some partitions have data - warn but still provide advisory
			advisoryRepairSQLs = append(advisoryRepairSQLs,
				fmt.Sprintf("-- WARNING: Some extra partitions may contain data (TABLE_ROWS is an estimate for InnoDB). Please verify with SELECT COUNT(*) before dropping."))
			advisoryRepairSQLs = append(advisoryRepairSQLs, buildDropPartitionAdvisoryLines(destSchema, destTable, toDrop)...)
		}
	}

	// Handle partitions to add (source has, dest doesn't)
	if len(toAdd) > 0 {
		if maxPart != nil {
			// MAXVALUE partition exists - use REORGANIZE to insert before it
			reorganizeSQL, err := buildReorganizePartitionSQL(destSchema, destTable, *maxPart, toAdd)
			if err != nil {
				return nil, nil, false, fmt.Sprintf("cannot generate REORGANIZE PARTITION: %v", err)
			}
			execRepairSQLs = append(execRepairSQLs, reorganizeSQL)
		} else {
			// No MAXVALUE - use simple ADD PARTITION
			addSQL := buildAddPartitionSQL(destSchema, destTable, toAdd)
			if len(addSQL) == 0 {
				return nil, nil, false, "cannot generate ADD PARTITION SQL for missing partitions"
			}
			execRepairSQLs = append(execRepairSQLs, addSQL...)
		}
	}

	reason := fmt.Sprintf("detected %d partitions to drop and %d partitions to add", len(toDrop), len(toAdd))
	return execRepairSQLs, advisoryRepairSQLs, true, reason
}

func classifyPartitionRepairDiffState(execRepairSQLs, advisoryRepairSQLs []string, handled bool) string {
	if !handled {
		return global.SkipDiffsYes
	}
	if len(execRepairSQLs) == 0 && len(advisoryRepairSQLs) > 0 {
		return global.SkipDiffsWarnOnly
	}
	return global.SkipDiffsYes
}

func (stcls *schemaTable) loadTablePartitionExpressions(db *sql.DB, drive, schemaName, tableName, caseSensitiveObjectName string, logThreadSeq int64) []string {
	partitions, err := stcls.cachedPartitions(db, drive, schemaName, tableName, logThreadSeq)
	if err != nil {
		global.Wlog.Warn(fmt.Sprintf("(%d) Failed to load partition expressions for %s.%s: %v", logThreadSeq, schemaName, tableName, err))
		return nil
	}
	_ = caseSensitiveObjectName
	tableKey := fmt.Sprintf("%s.%s", schemaName, tableName)
	entries := parsePartitionMetadataEntries(partitions, tableKey)
	if len(entries) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	expressions := make([]string, 0, len(entries))
	for _, entry := range entries {
		expr := normalizePartitionCompareText(entry.Expression)
		if strings.TrimSpace(expr) == "" {
			continue
		}
		if _, exists := seen[expr]; exists {
			continue
		}
		seen[expr] = struct{}{}
		expressions = append(expressions, expr)
	}
	return expressions
}

func partitionExpressionsReferenceColumn(expressions []string, columnNames ...string) bool {
	if len(expressions) == 0 || len(columnNames) == 0 {
		return false
	}

	// Pre-compile one regexp per unique column name so the inner expression
	// loop never triggers repeated MustCompile calls.
	type columnPattern struct {
		pattern *regexp.Regexp
	}
	patterns := make([]columnPattern, 0, len(columnNames))
	for _, candidate := range columnNames {
		normalizedColumn := strings.ToUpper(strings.TrimSpace(strings.ReplaceAll(candidate, "`", "")))
		if normalizedColumn == "" {
			continue
		}
		patterns = append(patterns, columnPattern{
			pattern: regexp.MustCompile(fmt.Sprintf(partitionExpressionColumnPatternTemplate, regexp.QuoteMeta(normalizedColumn))),
		})
	}
	if len(patterns) == 0 {
		return false
	}

	for _, expression := range expressions {
		normalizedExpression := strings.ToUpper(strings.TrimSpace(strings.ReplaceAll(expression, "`", "")))
		for _, cp := range patterns {
			if cp.pattern.MatchString(normalizedExpression) {
				return true
			}
		}
	}
	return false
}

func shouldDeferPartitionKeyColumnRepair(expressions []string, decision schemacompat.CompatibilityDecision, columnNames ...string) bool {
	if !decision.IsMismatch() || decision.State == schemacompat.CompatibilityWarnOnly {
		return false
	}
	return partitionExpressionsReferenceColumn(expressions, columnNames...)
}

// preloadOraclePartitionedTables 批量拉取 schemas 下 partitioned='YES' 的表集合。
// 结果写入 partitionedTableCache 供 cachedPartitions 快速判定。
func (stcls *schemaTable) preloadOraclePartitionedTables(db *sql.DB, drive string, schemas []string) {
	if db == nil || !isOracleDrive(drive) || len(schemas) == 0 {
		return
	}
	if stcls.partitionedTableCache == nil {
		stcls.partitionedTableCache = make(map[string]map[string]struct{})
	}
	if stcls.partitionedTableCacheLoaded == nil {
		stcls.partitionedTableCacheLoaded = make(map[string]bool)
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
		cacheKey := drive + "|" + k
		if _, ok := stcls.partitionedTableCache[cacheKey]; !ok {
			stcls.partitionedTableCache[cacheKey] = make(map[string]struct{})
		}
		stcls.partitionedTableCacheLoaded[cacheKey] = true
	}
	if len(upperSchemas) == 0 {
		return
	}
	quoted := make([]string, 0, len(upperSchemas))
	for _, s := range upperSchemas {
		quoted = append(quoted, "'"+escapeSQLLiteral(s)+"'")
	}
	query := fmt.Sprintf("SELECT UPPER(owner) AS owner, UPPER(table_name) AS table_name FROM all_tables WHERE partitioned='YES' AND UPPER(owner) IN (%s)", strings.Join(quoted, ","))
	rows, err := db.Query(query)
	if err != nil {
		global.Wlog.Warn(fmt.Sprintf("preloadOraclePartitionedTables failed: %v", err))
		return
	}
	defer rows.Close()
	for rows.Next() {
		var owner, tableName string
		if err := rows.Scan(&owner, &tableName); err != nil {
			continue
		}
		cacheKey := drive + "|" + strings.ToUpper(owner)
		if _, ok := stcls.partitionedTableCache[cacheKey]; !ok {
			stcls.partitionedTableCache[cacheKey] = make(map[string]struct{})
		}
		stcls.partitionedTableCache[cacheKey][strings.ToUpper(tableName)] = struct{}{}
	}
}

// cachedPartitions 统一从缓存读取 Partitions()，未命中则回源并写回缓存。
// key 使用 db指针|drive|schema|table 组合，用 db 指针地址区分源/目的连接，
// 避免 MySQL→MySQL 同库同表名场景下缓存碰撞导致漏报分区差异。
func (stcls *schemaTable) cachedPartitions(db *sql.DB, drive, schema, table string, logThreadSeq int64) (map[string]string, error) {
	if stcls == nil || db == nil {
		tc := dbExec.TableColumnNameStruct{Schema: schema, Table: table, Drive: drive}
		return tc.Query().Partitions(db, logThreadSeq)
	}
	key := fmt.Sprintf("%p|%s|%s|%s", db, drive, schema, table)
	if stcls.partitionsCache != nil {
		if cached, ok := stcls.partitionsCache[key]; ok {
			return cached, nil
		}
	}
	// Oracle 场景：若批量预加载确认该表未分区，直接返回空 map，跳过两次 Oracle 往返。
	if isOracleDrive(drive) && stcls.partitionedTableCacheLoaded != nil {
		cacheKey := drive + "|" + strings.ToUpper(schema)
		if stcls.partitionedTableCacheLoaded[cacheKey] {
			partitionedSet := stcls.partitionedTableCache[cacheKey]
			if _, ok := partitionedSet[strings.ToUpper(table)]; !ok {
				empty := map[string]string{}
				if stcls.partitionsCache == nil {
					stcls.partitionsCache = make(map[string]map[string]string)
				}
				stcls.partitionsCache[key] = empty
				return empty, nil
			}
		}
	}
	tc := dbExec.TableColumnNameStruct{Schema: schema, Table: table, Drive: drive}
	parts, err := tc.Query().Partitions(db, logThreadSeq)
	if err != nil {
		return parts, err
	}
	if stcls.partitionsCache == nil {
		stcls.partitionsCache = make(map[string]map[string]string)
	}
	stcls.partitionsCache[key] = parts
	return parts, nil
}

// 校验分区
func (stcls *schemaTable) Partitions(dtabS []string, logThreadSeq, logThreadSeq2 int64, isCalledFromStruct ...bool) {
	var (
		vlog                             string
		err                              error
		c, d                             []string
		sourcePartitions, destPartitions map[string]string
		pods                             = Pod{
			Datafix:     "no",
			CheckObject: "partitions",
		}
	)

	// 如果是从 Struct 函数调用的，则将 CheckObject 设置为 "struct"
	if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
		pods.CheckObject = "struct"
	}
	vlog = fmt.Sprintf("(%d) Start init check source and target DB partition table. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		sourceSchema, sourceTable, destSchema, destTable := parseSourceAndDestTablePair(i, stcls.tableMappings)
		stcls.schema = sourceSchema
		stcls.table = sourceTable
		stcls.destTable = destTable

		// Oracle→MySQL: partition syntax differs drastically; only do existence comparison.
		// First check whether either side actually has partitions. If neither does, treat
		// as consistent (same as MySQL→MySQL behaviour) and skip without any advisory.
		if stcls.isOracleToMySQL() {
			srcParts, srcPartsErr := stcls.cachedPartitions(stcls.sourceDB, stcls.sourceDrive, sourceSchema, sourceTable, logThreadSeq2)
			dstParts, dstPartsErr := stcls.cachedPartitions(stcls.destDB, stcls.destDrive, destSchema, destTable, logThreadSeq2)

			if srcPartsErr != nil {
				global.Wlog.Warnf("(%d) Oracle→MySQL: failed to query source partitions for %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, srcPartsErr)
			}
			if dstPartsErr != nil {
				global.Wlog.Warnf("(%d) Oracle→MySQL: failed to query dest partitions for %s.%s: %v", logThreadSeq, destSchema, destTable, dstPartsErr)
			}

			sourceTableKey := fmt.Sprintf("%s.%s", sourceSchema, sourceTable)
			cleanTableKey := sourceTableKey
			if strings.Contains(sourceTableKey, ":") {
				cleanTableKey = strings.Split(sourceTableKey, ":")[0]
			}

			// If both sides have no partitions, treat as consistent — no advisory, no warn-only.
			if len(srcParts) == 0 && len(dstParts) == 0 && srcPartsErr == nil && dstPartsErr == nil {
				vlog = fmt.Sprintf("(%d) Oracle→MySQL table %s.%s: no partitions on either side, skipping partition check", logThreadSeq, sourceSchema, sourceTable)
				global.Wlog.Debug(vlog)
				if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
					if stcls.partitionDiffsMap == nil {
						stcls.partitionDiffsMap = make(map[string]bool)
					}
					stcls.partitionDiffsMap[cleanTableKey] = false
				}
				continue
			}

			// At least one side has partitions OR a partition query failed:
			// fall back to advisory warn-only and explicitly mark the failure
			// in the advisory note so users know the comparison is indicative
			// rather than authoritative.
			pods.Schema = sourceSchema
			pods.Table = sourceTable
			pods.DIFFS = global.SkipDiffsWarnOnly
			var advisoryNote string
			if srcPartsErr != nil || dstPartsErr != nil {
				advisoryNote = fmt.Sprintf("-- [Advisory] Oracle→MySQL partition query FAILED for table %s.%s (srcErr=%v, dstErr=%v); please verify partitions manually", sourceSchema, sourceTable, srcPartsErr, dstPartsErr)
			} else {
				advisoryNote = fmt.Sprintf("-- [Advisory] Oracle→MySQL partition comparison for table %s.%s is not supported in this version; please verify partitions manually", sourceSchema, sourceTable)
			}
			vlog = fmt.Sprintf("(%d) Skipping detailed partition comparison for Oracle→MySQL table %s.%s (advisory only)", logThreadSeq, sourceSchema, sourceTable)
			global.Wlog.Info(vlog)
			if stcls.datafix == "file" {
				if err = stcls.writeAdvisoryFixSql([]string{advisoryNote}, logThreadSeq); err != nil {
					global.Wlog.Errorf("(%d) Failed to write partition advisory for Oracle→MySQL table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, err)
				}
			} else {
				global.Wlog.Warnf("(%d) Oracle→MySQL table %s.%s partition advisory skipped (datafix=%s); please verify manually", logThreadSeq, sourceSchema, sourceTable, stcls.datafix)
			}
			if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
				if stcls.partitionDiffsMap == nil {
					stcls.partitionDiffsMap = make(map[string]bool)
				}
				if stcls.structWarnOnlyDiffsMap == nil {
					stcls.structWarnOnlyDiffsMap = make(map[string]bool)
				}
				stcls.partitionDiffsMap[cleanTableKey] = false
				stcls.structWarnOnlyDiffsMap[cleanTableKey] = true
			}
			if len(isCalledFromStruct) == 0 || !isCalledFromStruct[0] {
				stcls.appendPod(pods)
			}
			continue
		}

		vlog = fmt.Sprintf("(%d) Start processing srcDSN {%s} table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.sourceDrive, sourceSchema, sourceTable)
		global.Wlog.Debug(vlog)
		if sourcePartitions, err = stcls.cachedPartitions(stcls.sourceDB, stcls.sourceDrive, sourceSchema, sourceTable, logThreadSeq2); err != nil {
			global.Wlog.Errorf("(%d) Failed to get source partitions for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, err)
			return
		}

		vlog = fmt.Sprintf("(%d) srcDSN {%s} table %s.%s partitions count: %d", logThreadSeq, stcls.sourceDrive, sourceSchema, sourceTable, len(sourcePartitions))
		global.Wlog.Debug(vlog)

		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.destDrive, destSchema, destTable)
		global.Wlog.Debug(vlog)
		if destPartitions, err = stcls.cachedPartitions(stcls.destDB, stcls.destDrive, destSchema, destTable, logThreadSeq2); err != nil {
			global.Wlog.Errorf("(%d) Failed to get dest partitions for table %s.%s: %v", logThreadSeq, destSchema, destTable, err)
			return
		}
		vlog = fmt.Sprintf("(%d) Dest DB %s table %s.%s partitions count: %d", logThreadSeq, stcls.destDrive, destSchema, destTable, len(destPartitions))
		global.Wlog.Debug(vlog)

		pods.Schema = sourceSchema
		pods.Table = sourceTable
		if len(sourcePartitions) == 0 && len(destPartitions) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, sourceSchema, sourceTable)
			global.Wlog.Debug(vlog)
			continue
		}

		// Mapped-table verification needs source and target keys separately.
		sourceTableKey := fmt.Sprintf("%s.%s", sourceSchema, sourceTable)
		destTableKey := fmt.Sprintf("%s.%s", destSchema, destTable)

		// 1. 检查表级别的分区定义是否一致
		pods.DIFFS = "no"

		// 先比较完整的分区定义（包含分区类型、列和所有分区）
		sourceFullDef, sourceHasDef := sourcePartitions[sourceTableKey]
		destFullDef, destHasDef := destPartitions[destTableKey]

		// 记录具体的分区名称用于详细比较
		sourcePartitionNames := make([]string, 0)
		destPartitionNames := make([]string, 0)

		// 提取源端和目标端的分区名称
		for k := range sourcePartitions {
			if strings.HasPrefix(k, sourceTableKey+".") {
				// 提取分区名称部分 (schema.table.partition -> partition)
				parts := strings.Split(k, ".")
				if len(parts) == 3 {
					sourcePartitionNames = append(sourcePartitionNames, parts[2])
				}
			}
		}

		for k := range destPartitions {
			if strings.HasPrefix(k, destTableKey+".") {
				parts := strings.Split(k, ".")
				if len(parts) == 3 {
					destPartitionNames = append(destPartitionNames, parts[2])
				}
			}
		}

		vlog = fmt.Sprintf("(%d) Table %s.%s source partitions: %v, dest partitions: %v", logThreadSeq, sourceSchema, sourceTable, sourcePartitionNames, destPartitionNames)
		global.Wlog.Debug(vlog)

		sourceFullDefNormalized := normalizePartitionFullDefinition(sourceFullDef)
		destFullDefNormalized := normalizePartitionFullDefinition(destFullDef)

		// 直接比较完整的分区定义，但先做标识符和空白归一化，避免
		// `customer_id` 与 customer_id 这类纯文本噪音被误判成结构差异。
		if sourceFullDefNormalized != destFullDefNormalized {
			pods.DIFFS = "yes"
			vlog = fmt.Sprintf("(%d) Table %s.%s partition definitions mismatch", logThreadSeq, sourceSchema, sourceTable)
			global.Wlog.Warn(vlog)
			d = append(d, "Partition definitions mismatch")

			// Only handle low-risk tail partition drift automatically.
			execRepairSQLs, advisoryRepairSQLs, handled, reason := buildPartitionRepairSQLs(
				sourceSchema,
				sourceTable,
				destSchema,
				destTable,
				sourcePartitions,
				destPartitions,
			)
			if handled {
				pods.DIFFS = classifyPartitionRepairDiffState(execRepairSQLs, advisoryRepairSQLs, handled)
				if len(execRepairSQLs) > 0 {
					vlog = fmt.Sprintf("(%d) Generated executable partition repair SQLs for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, execRepairSQLs)
					global.Wlog.Warn(vlog)
					if err = stcls.writeFixSql(execRepairSQLs, logThreadSeq); err != nil {
						global.Wlog.Errorf("(%d) Failed to write executable partition repair SQLs for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, err)
						return
					}
				}
				if len(advisoryRepairSQLs) > 0 {
					vlog = fmt.Sprintf("(%d) Generated advisory partition repair SQLs for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, advisoryRepairSQLs)
					global.Wlog.Warn(vlog)
					if err = stcls.writeFixSql(advisoryRepairSQLs, logThreadSeq); err != nil {
						global.Wlog.Errorf("(%d) Failed to write advisory partition repair SQLs for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, err)
						return
					}
				}
				vlog = fmt.Sprintf("(%d) Partition mismatch for table %s.%s was classified as a supported repair shape: %s", logThreadSeq, sourceSchema, sourceTable, reason)
				global.Wlog.Warn(vlog)
			} else {
				// Fall back to a generic note when the partition mismatch cannot be repaired safely.
				cleanTable := sourceTable
				if strings.Contains(cleanTable, ":") {
					parts := strings.Split(cleanTable, ":")
					cleanTable = parts[0]
				}
				fixSQLHint := fmt.Sprintf("-- [Note] The partitions for table %s.%s is inconsistent, please check manually", sourceSchema, cleanTable)
				if err = stcls.writeFixSql([]string{fixSQLHint}, logThreadSeq); err != nil {
					global.Wlog.Errorf("(%d) Failed to write partition manual-check hint for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, err)
					return
				}
				vlog = fmt.Sprintf("(%d) Partition mismatch for table %s.%s remains manual-review only: %s", logThreadSeq, sourceSchema, sourceTable, reason)
				global.Wlog.Warn(vlog)
			}
		} else {
			// Partition definitions can differ textually across versions or SHOW CREATE
			// variants, so treat normalized-equal definitions as consistent.
			vlog = fmt.Sprintf("(%d) Table %s.%s partition definitions are consistent after normalization", logThreadSeq, sourceSchema, sourceTable)
			global.Wlog.Debug(vlog)
			c = append(c, "All partitions consistent")
			continue // 跳过后续的分区比较，因为定义已经完全一致
			// 这里不再单独比较每个分区，因为已经通过完整分区定义进行了比较
		}

		// 记录分区定义的比较结果
		if sourceHasDef && destHasDef {
			vlog = fmt.Sprintf("(%d) Table %s.%s full partition definitions compared: source='%s', dest='%s'", logThreadSeq, sourceSchema, sourceTable, sourceFullDef, destFullDef)
			global.Wlog.Debug(vlog)
		}

		vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s partitions. normal partitions: %v, abnormal partitions: %v", logThreadSeq, sourceSchema, sourceTable, c, d)
		global.Wlog.Debug(vlog)

		// 如果是从 Struct 函数调用的，则将结果存储在全局变量中
		if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
			// 使用完整的schema.table作为键

			// Keep partition diff state on the schemaTable instance so repeated
			// checks do not reuse package-level mutable state.
			if stcls.partitionDiffsMap == nil {
				stcls.partitionDiffsMap = make(map[string]bool)
			}
			if stcls.structWarnOnlyDiffsMap == nil {
				stcls.structWarnOnlyDiffsMap = make(map[string]bool)
			}
			if stcls.structCollationMappedMap == nil {
				stcls.structCollationMappedMap = make(map[string]bool)
			}

			// 确保使用干净的表名格式（不含映射后缀）
			cleanTableKey := sourceTableKey
			if strings.Contains(sourceTableKey, ":") {
				parts := strings.Split(sourceTableKey, ":")
				cleanTableKey = parts[0]
			}

			stcls.partitionDiffsMap[cleanTableKey] = pods.DIFFS == "yes"
			if pods.DIFFS == global.SkipDiffsWarnOnly {
				stcls.structWarnOnlyDiffsMap[cleanTableKey] = true
			}

			vlog = fmt.Sprintf("(%d) Storing partition check result for table %s (cleaned to %s): %v",
				logThreadSeq, sourceTableKey, cleanTableKey, stcls.partitionDiffsMap[cleanTableKey])
			global.Wlog.Debug(vlog)
		} else {
			// 不是从 Struct 函数调用时，添加到 measuredDataPods
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table partitions data. normal table count: [%d] abnormal table count: [%d]", logThreadSeq, len(c), len(d))
	global.Wlog.Info(vlog)
}
