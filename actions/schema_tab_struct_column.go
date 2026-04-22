package actions

import (
	"database/sql"
	"fmt"
	mysql "gt-checksum/MySQL"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
	"sort"
	"strings"
)

func (stcls *schemaTable) rememberColumnRepairOperations(tableKey string, sqls []string) {
	if stcls == nil || len(sqls) == 0 {
		return
	}
	if stcls.columnRepairMap == nil {
		stcls.columnRepairMap = make(map[string][]string)
	}
	stcls.columnRepairMap[tableKey] = cloneSQLStatements(sqls)
}

func (stcls *schemaTable) pendingColumnRepairOperations(tableKey string) []string {
	if stcls == nil || stcls.columnRepairMap == nil {
		return nil
	}
	return cloneSQLStatements(stcls.columnRepairMap[tableKey])
}

func (stcls *schemaTable) forgetColumnRepairOperations(tableKey string) {
	if stcls == nil || stcls.columnRepairMap == nil {
		return
	}
	delete(stcls.columnRepairMap, tableKey)
}

func hasAutoIncrementColumnAttribute(columnDefinition []string) bool {
	for _, attr := range columnDefinition {
		if strings.Contains(strings.ToUpper(attr), "AUTO_INCREMENT") {
			return true
		}
	}
	return false
}

func adjustDestColumnSeqAfterDrops(destColumnSeq map[string]int, dropped []string) {
	droppedPositions := make([]int, 0, len(dropped))
	for _, col := range dropped {
		droppedPositions = append(droppedPositions, destColumnSeq[col])
		delete(destColumnSeq, col)
	}
	sort.Ints(droppedPositions)
	for col, seq := range destColumnSeq {
		adj := 0
		for _, dp := range droppedPositions {
			if dp < seq {
				adj++
			}
		}
		destColumnSeq[col] -= adj
	}
}

func (stcls *schemaTable) tableColumnName(db *sql.DB, tc dbExec.TableColumnNameStruct, logThreadSeq, logThreadSeq2 int64) ([]map[string][]string, error) {
	var (
		col       []map[string][]string
		vlog      string
		CS        []string
		queryData []map[string]interface{}
		err       error
		Event     = "Q_table_columns"
		A         = make(map[string][]string)
		C         = func(c string) string {
			switch c {
			case "<nil>":
				return "null"
			case "<entry>":
				return "" // 返回空字符串而不是"empty"
			default:
				return c
			}
		}
	)
	// 若是 Oracle 源端且已批量预加载列元数据，则直接命中缓存，避免一次
	// dba_tab_columns 单表查询的网络往返 + 解析开销。
	if db == stcls.sourceDB && isOracleDrive(tc.Drive) && stcls.sourceOracleColumnsCache != nil {
		if cached, ok := lookupOracleTableColumns(stcls.sourceOracleColumnsCache, tc.Schema, tc.Table); ok {
			queryData = cached
		}
	}
	if queryData == nil {
		if queryData, err = tc.Query().TableColumnName(db, logThreadSeq2); err != nil {
			return col, err
		}
	}
	vlog = fmt.Sprintf("(%d) [%s] Starting column validation", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	for _, v := range queryData {
		if fmt.Sprintf("%v", v["columnName"]) != "" {
			// 获取extra属性，包含AUTO_INCREMENT和INVISIBLE等特殊属性
			extra := C(fmt.Sprintf("%v", v["extra"]))
			extra = schemacompat.StripMySQLMetadataOnlyExtraTokens(extra)
			// 将extra添加到列定义数组中，放在columnType之后，这样可以在生成SQL时包含特殊属性
			columnType := fmt.Sprintf("%v", v["columnType"])
			// 如果有extra属性，添加到columnType后面
			if extra != "null" && extra != "" {
				columnType = fmt.Sprintf("%s %s", columnType, extra)
			}
			A[fmt.Sprintf("%v", v["columnName"])] = []string{C(columnType), C(fmt.Sprintf("%v", v["charset"])), C(fmt.Sprintf("%v", v["collationName"])), C(fmt.Sprintf("%v", v["isNull"])), C(fmt.Sprintf("%v", v["columnDefault"])), C(fmt.Sprintf("%v", v["columnComment"]))}
			CS = append(CS, fmt.Sprintf("%v", v["columnName"]))
		}
	}
	for _, v := range CS {
		col = append(col, map[string][]string{v: A[v]})
	}
	vlog = fmt.Sprintf("(%d) [%s] Column validation completed", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	return col, nil
}

func (stcls *schemaTable) detectOracleToMySQLColumnHardMismatch(
	sourceColumnMap, destColumnMap map[string][]string,
	getSourceOriginalColumnName func(string) string,
	getDestOriginalColumnName func(string) string,
) (column string, reason string, found bool) {
	for colKey, sourceAttrs := range sourceColumnMap {
		destAttrs, ok := destColumnMap[colKey]
		if !ok {
			continue
		}
		sourceOriginal := getSourceOriginalColumnName(colKey)
		destOriginal := getDestOriginalColumnName(colKey)
		repairName := destOriginal
		if strings.TrimSpace(repairName) == "" {
			repairName = sourceOriginal
		}
		sourceCanonical := schemacompat.CanonicalizeOracleColumnForComparison(
			sourceOriginal, sourceAttrs, stcls.destVersionInfo(),
		)
		destCanonical := schemacompat.CanonicalizeColumnForComparison(
			destOriginal, destAttrs,
			stcls.destVersionInfo(), stcls.sourceVersionInfo(),
			"", stcls.checkRules.MariaDBJSONTargetType,
		)
		checks := []struct {
			kind     string
			decision schemacompat.CompatibilityDecision
		}{
			{"type", schemacompat.DecideOracleToMySQLTypeCompatibility(sourceCanonical, destCanonical)},
			{"charset", schemacompat.DecideOracleToMySQLCharsetCompatibility(sourceCanonical, destCanonical)},
			{"collation", schemacompat.DecideOracleToMySQLCollationCompatibility(sourceCanonical, destCanonical)},
		}
		for _, c := range checks {
			if !c.decision.IsMismatch() {
				continue
			}
			if c.decision.State == schemacompat.CompatibilityWarnOnly {
				continue
			}
			return repairName, fmt.Sprintf("column %s %s mismatch: source=%s, target=%s (%s)",
				repairName, c.kind, c.decision.Source, c.decision.Target, c.decision.Reason), true
		}
	}
	return "", "", false
}

func isIgnorableGeneratedInvisibleColumn(colName string, columnMap map[string][]string) bool {
	if !strings.EqualFold(strings.TrimSpace(colName), "my_row_id") {
		return false
	}
	columnDef, exists := columnMap[colName]
	if !exists {
		return false
	}
	for _, def := range columnDef {
		upperDef := strings.ToUpper(strings.TrimSpace(def))
		if strings.Contains(upperDef, "INVISIBLE") {
			return true
		}
	}
	return false
}

func filterIgnorableGeneratedInvisibleColumns(columns []string, columnMap map[string][]string) ([]string, []string) {
	kept := make([]string, 0, len(columns))
	ignored := make([]string, 0)
	for _, col := range columns {
		if isIgnorableGeneratedInvisibleColumn(col, columnMap) {
			ignored = append(ignored, col)
			continue
		}
		kept = append(kept, col)
	}
	return kept, ignored
}

func normalizeDataCheckColumnInfo(sourceCols, destCols []map[string]string) ([]map[string]string, []map[string]string, []string) {
	sourceKeys := make(map[string]struct{}, len(sourceCols))
	for _, col := range sourceCols {
		name := strings.TrimSpace(col["columnName"])
		if name == "" {
			name = strings.TrimSpace(col["COLUMN_NAME"])
		}
		if name == "" {
			continue
		}
		sourceKeys[strings.ToUpper(name)] = struct{}{}
	}

	filteredDest := make([]map[string]string, 0, len(destCols))
	stripped := make([]string, 0)
	for _, col := range destCols {
		name := strings.TrimSpace(col["columnName"])
		if name == "" {
			name = strings.TrimSpace(col["COLUMN_NAME"])
		}
		extra := strings.TrimSpace(col["extra"])
		if extra == "" {
			extra = strings.TrimSpace(col["EXTRA"])
		}
		if name != "" {
			if _, exists := sourceKeys[strings.ToUpper(name)]; !exists &&
				strings.EqualFold(name, "my_row_id") &&
				strings.Contains(strings.ToUpper(extra), "INVISIBLE") {
				stripped = append(stripped, name)
				continue
			}
		}
		filteredDest = append(filteredDest, col)
	}

	return sourceCols, filteredDest, stripped
}

func queryOraclePrimaryKeyColumns(db *sql.DB, schema, table string) ([]string, error) {
	// Use ALL_* views (not DBA_*) so that non-DBA application accounts with only
	// SELECT privileges on the target schema can still retrieve PK columns. Using
	// DBA_* requires SELECT_CATALOG_ROLE which production accounts rarely have,
	// and failing back to a PK-less CREATE TABLE breaks sql_require_primary_key=ON.
	query := `SELECT cc.column_name
FROM all_constraints c
JOIN all_cons_columns cc ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name
WHERE c.constraint_type = 'P' AND c.owner = :1 AND c.table_name = :2
ORDER BY cc.position`
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var col string
		if scanErr := rows.Scan(&col); scanErr != nil {
			return nil, scanErr
		}
		cols = append(cols, col)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

func (stcls *schemaTable) TableColumnNameCheck(checkTableList []string, logThreadSeq, logThreadSeq2 int64) ([]string, []string, error) {
	var (
		vlog                                 string
		newCheckTableList, abnormalTableList []string
		aa                                   = &CheckSumTypeStruct{}
		tableAbnormalBool                    = false
		event                                string
	)
	if stcls.structWarnOnlyDiffsMap == nil {
		stcls.structWarnOnlyDiffsMap = make(map[string]bool)
	}
	if stcls.structCollationMappedMap == nil {
		stcls.structCollationMappedMap = make(map[string]bool)
	}
	stcls.emitMariaDBSequenceWarnings(checkTableList, logThreadSeq)
	vlog = fmt.Sprintf("(%d) %s Validating structure differences between source and target", logThreadSeq, event)
	global.Wlog.Debug(vlog)

	// 批量预加载源/目标端 BASE TABLE 名单，tableExistsByDrive 将从缓存命中。
	if stcls.tableExistenceCache == nil {
		srcSchemas := make(map[string]struct{}, len(checkTableList))
		dstSchemas := make(map[string]struct{}, len(checkTableList))
		for _, item := range checkTableList {
			srcSchema, _, dstSchema, _ := parseSourceAndDestTablePair(item, stcls.tableMappings)
			if srcSchema != "" {
				srcSchemas[strings.ToUpper(srcSchema)] = struct{}{}
			}
			if dstSchema != "" {
				dstSchemas[strings.ToUpper(dstSchema)] = struct{}{}
			}
		}
		toSlice := func(m map[string]struct{}) []string {
			out := make([]string, 0, len(m))
			for k := range m {
				out = append(out, k)
			}
			return out
		}
		stcls.preloadTableExistence(stcls.sourceDB, stcls.sourceDrive, toSlice(srcSchemas))
		stcls.preloadTableExistence(stcls.destDB, stcls.destDrive, toSlice(dstSchemas))
		// Oracle 源端：批量识别已分区表，cachedPartitions 对非分区表可短路返回空结果。
		if isOracleDrive(stcls.sourceDrive) {
			stcls.preloadOraclePartitionedTables(stcls.sourceDB, stcls.sourceDrive, toSlice(srcSchemas))
		}
		if isOracleDrive(stcls.destDrive) {
			stcls.preloadOraclePartitionedTables(stcls.destDB, stcls.destDrive, toSlice(dstSchemas))
		}
	}

	// Oracle 源端一次性批量预加载列元数据，避免后续 21 次逐表 Q_table_columns。
	if isOracleDrive(stcls.sourceDrive) && stcls.sourceOracleColumnsCache == nil {
		schemasSet := make(map[string]struct{}, len(checkTableList))
		for _, item := range checkTableList {
			srcSchema, _, _, _ := parseSourceAndDestTablePair(item, stcls.tableMappings)
			if srcSchema != "" {
				schemasSet[strings.ToUpper(srcSchema)] = struct{}{}
			}
		}
		if len(schemasSet) > 0 {
			schemas := make([]string, 0, len(schemasSet))
			for s := range schemasSet {
				schemas = append(schemas, s)
			}
			stcls.sourceOracleColumnsCache = preloadOracleTableColumns(stcls.sourceDB, schemas, logThreadSeq2)
		}
	}
	for _, v := range checkTableList {
		// 处理可能存在的映射规则（格式：sourceSchema.sourceTable:destSchema.destTable）
		sourceTable := v
		destTable := v

		// 检查是否包含映射规则（是否包含":"字符）
		if strings.Contains(v, ":") {
			parts := strings.Split(v, ":")
			sourceTable = parts[0]
			destTable = parts[1]
		}

		// 从表列表中提取源端schema和表名
		sourceParts := strings.Split(sourceTable, ".")
		if len(sourceParts) < 2 {
			vlog = fmt.Sprintf("(%d) %s Invalid table format: %s, expected schema.table", logThreadSeq, event, sourceTable)
			global.Wlog.Error(vlog)
			continue
		}
		sourceSchema := sourceParts[0]
		sourceTableName := sourceParts[1]

		// 从表列表中提取目标端schema和表名
		destParts := strings.Split(destTable, ".")
		if len(destParts) < 2 {
			vlog = fmt.Sprintf("(%d) %s Invalid table format: %s, expected schema.table", logThreadSeq, event, destTable)
			global.Wlog.Error(vlog)
			continue
		}
		destSchema := destParts[0]
		destTableName := destParts[1]

		// 设置当前处理的表名
		stcls.schema = sourceSchema
		stcls.table = sourceTableName
		// 记录目标表名，用于后续操作
		stcls.destTable = destTableName

		// 如果没有明确的映射规则，则检查全局映射规则
		if sourceTable == destTable && sourceSchema == destSchema {
			if mappedSchema, exists := stcls.tableMappings[sourceSchema]; exists {
				destSchema = mappedSchema
			}
		}

		vlog = fmt.Sprintf("Table mapping options - source: %s, target: %s, mappings: %v", sourceSchema, destSchema, stcls.tableMappings)
		global.Wlog.Debug(vlog)
		mappedTableKey := fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)
		if sourceSchema != destSchema || sourceTableName != destTableName {
			mappedTableKey = fmt.Sprintf("%s.%s:%s.%s", sourceSchema, sourceTableName, destSchema, destTableName)
		}

		vlog = fmt.Sprintf("(%d %s Validating table structure %s.%s -> %s.%s", logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table)
		global.Wlog.Debug(vlog)

		// 检查源表和目标表是否存在（按驱动走不同元数据查询）
		sourceTableExists, err := stcls.tableExistsByDrive(stcls.sourceDB, stcls.sourceDrive, sourceSchema, sourceTableName, "table")
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Error checking source table existence %s.%s: %v", logThreadSeq, event, sourceSchema, sourceTableName, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}
		destTableExists, err := stcls.tableExistsByDrive(stcls.destDB, stcls.destDrive, destSchema, destTableName, "table")
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Error checking target table existence %s.%s: %v", logThreadSeq, event, destSchema, destTableName, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}

		oracleToMySQLDataMode := stcls.sourceDrive == "godror" && stcls.destDrive == "mysql" && stcls.checkRules.CheckObject != "struct"

		if !sourceTableExists || !destTableExists {
			if oracleToMySQLDataMode {
				diffReason := "table missing on one side"
				if !sourceTableExists && !destTableExists {
					diffReason = "table missing on both source and target"
				} else if !sourceTableExists {
					diffReason = "table missing on source"
				} else if !destTableExists {
					diffReason = "table missing on target"
				}
				pod := Pod{
					Schema:      sourceSchema,
					Table:       sourceTableName,
					CheckObject: "data",
					DIFFS:       "DDL-yes",
					Datafix:     stcls.datafix,
					Rows:        diffReason,
				}
				stcls.appendPod(pod)
				abnormalTableList = append(abnormalTableList, mappedTableKey)
				global.AddSkippedTableWithDiffs(sourceSchema, sourceTableName, "data", diffReason, global.SkipDiffsDDLYes)
				vlog = fmt.Sprintf("(%d) %s Skip data check for %s.%s due to DDL mismatch: %s", logThreadSeq, event, sourceSchema, sourceTableName, diffReason)
				global.Wlog.Warn(vlog)
				continue
			}

			if !sourceTableExists && !destTableExists {
				vlog = fmt.Sprintf("(%d) %s Source/target table both missing: %s.%s -> %s.%s", logThreadSeq, event, sourceSchema, sourceTableName, destSchema, destTableName)
				global.Wlog.Warn(vlog)
				pod := Pod{
					Schema:      sourceSchema,
					Table:       sourceTableName,
					CheckObject: stcls.checkRules.CheckObject,
					DIFFS:       "yes",
					Datafix:     stcls.datafix,
				}
				stcls.appendPod(pod)
				// Keep abnormalTableList entry so data-mode EvaluateDataCheckPreflight
				// correctly accounts this table as DDL-abnormal (SkipChecksum) rather
				// than triggering the fatal "No valid tables" branch.
				// Struct() uses a pod-snapshot guard to prevent a duplicate Pod entry.
				abnormalTableList = append(abnormalTableList, mappedTableKey)
				continue
			}

			if !sourceTableExists {
				vlog = fmt.Sprintf("(%d) %s Source table %s.%s does not exist", logThreadSeq, event, sourceSchema, sourceTableName)
				global.Wlog.Warn(vlog)
				global.AddSkippedTableWithDiffs(sourceSchema, sourceTableName, "data", "table does not exist", global.SkipDiffsDDLYes)
				pod := Pod{
					Schema:      sourceSchema,
					Table:       sourceTableName,
					CheckObject: stcls.checkRules.CheckObject,
					DIFFS:       "yes",
					Datafix:     stcls.datafix,
				}
				stcls.appendPod(pod)
				// Keep abnormalTableList entry for data-mode preflight accounting.
				// Struct() uses a pod-snapshot guard to prevent a duplicate Pod entry.
				abnormalTableList = append(abnormalTableList, mappedTableKey)
				continue
			}
		}

		// 处理特殊情况：源表存在但目标表不存在
		if sourceTableExists && !destTableExists {
			// 添加调试信息，确认schema映射处理
			vlog = fmt.Sprintf("(%d) %s Processing table creation with mapping - source: %s.%s -> dest: %s.%s", logThreadSeq, event, sourceSchema, sourceTableName, destSchema, destTableName)
			global.Wlog.Debug(vlog)

			// Oracle→MySQL: generate CREATE TABLE from Oracle metadata
			if stcls.isOracleToMySQL() {
				tc := dbExec.TableColumnNameStruct{Drive: stcls.sourceDrive, Schema: sourceSchema, Table: sourceTableName}
				oracleColumns, oraErr := tc.Query().TableColumnName(stcls.sourceDB, logThreadSeq)
				if oraErr != nil {
					vlog = fmt.Sprintf("(%d) %s Error querying Oracle columns for CREATE TABLE %s.%s: %v", logThreadSeq, event, sourceSchema, sourceTableName, oraErr)
					global.Wlog.Error(vlog)
					return nil, nil, oraErr
				}
				// Query Oracle primary key columns so the generated CREATE TABLE
				// includes a PRIMARY KEY clause; this is required when the target
				// MySQL has sql_require_primary_key=ON (default on MySQL 8.0+).
				oracleIndexData := make(map[string][]string)
				if pkCols, pkErr := queryOraclePrimaryKeyColumns(stcls.sourceDB, sourceSchema, sourceTableName); pkErr != nil {
					vlog = fmt.Sprintf("(%d) %s Warning: failed to query Oracle primary key for %s.%s: %v (proceeding without PK)", logThreadSeq, event, sourceSchema, sourceTableName, pkErr)
					global.Wlog.Warn(vlog)
				} else if len(pkCols) > 0 {
					oracleIndexData["PRIMARY"] = pkCols
				}
				createTableSql := schemacompat.GenerateOracleToMySQLCreateTableSQL(destSchema, destTableName, oracleColumns, oracleIndexData, stcls.destVersionInfo())
				if createTableSql != "" {
					vlog = fmt.Sprintf("(%d) %s Generated Oracle→MySQL CREATE TABLE for %s.%s", logThreadSeq, event, destSchema, destTableName)
					global.Wlog.Info(vlog)
					originalSchema, originalTable, originalDestTable := stcls.schema, stcls.table, stcls.destTable
					stcls.schema = destSchema
					stcls.table = destTableName
					stcls.destTable = destTableName
					if err = stcls.writeFixSql([]string{createTableSql}, logThreadSeq); err != nil {
						stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
						return nil, nil, err
					}
					stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
					stcls.appendPod(Pod{
						Schema:      sourceSchema,
						Table:       sourceTableName,
						CheckObject: stcls.checkRules.CheckObject,
						DIFFS:       "yes",
						Datafix:     stcls.datafix,
					})
					abnormalTableList = append(abnormalTableList, mappedTableKey)
				}
				continue
			}

			sourceMeta, sourceMetaErr := queryMySQLTableLevelMetadata(stcls.sourceDB, sourceSchema, sourceTableName)
			if sourceMetaErr != nil {
				vlog = fmt.Sprintf("(%d) %s Failed to query source table metadata for %s.%s before CREATE TABLE generation: %v", logThreadSeq, event, sourceSchema, sourceTableName, sourceMetaErr)
				global.Wlog.Warn(vlog)
			} else {
				jsonDowngradeColumns := schemacompat.DetectMariaDBJSONDowngradeColumns(
					sourceMeta.CreateTableSQL,
					stcls.sourceVersionInfo(),
					stcls.destVersionInfo(),
					stcls.checkRules.MariaDBJSONTargetType,
				)
				if len(jsonDowngradeColumns) > 0 {
					advisoryLines := buildConstraintAdvisoryLines(
						fmt.Sprintf("%s.%s MariaDB JSON downgrade", destSchema, destTableName),
						schemacompat.BuildMariaDBJSONDowngradeSuggestions(destSchema, destTableName, jsonDowngradeColumns, stcls.checkRules.MariaDBJSONTargetType),
					)
					originalSchema, originalTable, originalDestTable := stcls.schema, stcls.table, stcls.destTable
					stcls.schema = destSchema
					stcls.table = destTableName
					stcls.destTable = destTableName
					if err = stcls.writeAdvisoryFixSql(advisoryLines, logThreadSeq); err != nil {
						stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
						return nil, nil, err
					}
					stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
				}

				// MariaDB-only temporal and sequence constructs must stay on the
				// advisory path because there is no safe automatic MySQL rewrite.
				unsupportedFeatures := schemacompat.DetectMariaDBUnsupportedTableFeatures(sourceMeta.CreateTableSQL, stcls.sourceVersionInfo(), stcls.destVersionInfo())
				if len(unsupportedFeatures) > 0 {
					vlog = fmt.Sprintf("(%d) %s Skip automatic CREATE TABLE for %s.%s because unsupported MariaDB features were detected: %+v", logThreadSeq, event, sourceSchema, sourceTableName, unsupportedFeatures)
					global.Wlog.Warn(vlog)

					advisoryLines := buildConstraintAdvisoryLines(
						fmt.Sprintf("%s.%s MariaDB unsupported features", destSchema, destTableName),
						schemacompat.BuildMariaDBUnsupportedFeatureSuggestions(destSchema, destTableName, unsupportedFeatures),
					)
					originalSchema, originalTable, originalDestTable := stcls.schema, stcls.table, stcls.destTable
					stcls.schema = destSchema
					stcls.table = destTableName
					stcls.destTable = destTableName
					if err = stcls.writeAdvisoryFixSql(advisoryLines, logThreadSeq); err != nil {
						stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
						return nil, nil, err
					}
					stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable

					stcls.appendPod(Pod{
						Schema:      sourceSchema,
						Table:       sourceTableName,
						CheckObject: stcls.checkRules.CheckObject,
						DIFFS:       global.SkipDiffsWarnOnly,
						Datafix:     stcls.datafix,
					})
					tableKey := fmt.Sprintf("%s.%s", destSchema, destTableName)
					stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)
					stcls.structWarnOnlyDiffsMap[fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)] = true
					continue
				}
			}

			// 生成CREATE TABLE语句
			createTableSql, err := generateCreateTableSql(stcls.sourceDB, sourceSchema, destSchema, sourceTableName, destTableName, stcls.sourceVersionInfo(), stcls.destVersionInfo(), stcls.checkRules.MariaDBJSONTargetType, logThreadSeq)
			if err != nil {
				vlog = fmt.Sprintf("(%d) %s Error generating CREATE TABLE statement for %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, err)
				global.Wlog.Error(vlog)
				return nil, nil, err
			}

			// 验证生成的CREATE TABLE语句是否包含正确的schema名
			if !strings.Contains(createTableSql, fmt.Sprintf("`%s`", destSchema)) {
				vlog = fmt.Sprintf("(%d) %s Warning: Generated CREATE TABLE statement may be missing target schema '%s': %s", logThreadSeq, event, destSchema, createTableSql)
				global.Wlog.Warn(vlog)
			}

			vlog = fmt.Sprintf("(%d) %s Generated CREATE TABLE statement for %s.%s: %s", logThreadSeq, event, destSchema, destTableName, createTableSql)
			global.Wlog.Debug(vlog)

			// 应用修复SQL
			vlog = fmt.Sprintf("(%d) %s Applying CREATE TABLE statement to %s.%s", logThreadSeq, event, destSchema, destTableName)
			global.Wlog.Debug(vlog)
			originalSchema, originalTable, originalDestTable := stcls.schema, stcls.table, stcls.destTable
			stcls.schema = destSchema
			stcls.table = destTableName
			stcls.destTable = destTableName
			if err = stcls.writeFixSql([]string{createTableSql}, logThreadSeq); err != nil {
				stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
				vlog = fmt.Sprintf("(%d) %s Error applying CREATE TABLE statement: %v", logThreadSeq, event, err)
				global.Wlog.Error(vlog)
				return nil, nil, err
			}
			stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable

			// 创建表示差异的Pod记录
			pod := Pod{
				Schema:      destSchema,
				Table:       destTableName,
				CheckObject: stcls.checkRules.CheckObject,
				DIFFS:       "yes",
				Datafix:     stcls.datafix,
			}
			stcls.appendPod(pod)

			abnormalTableList = append(abnormalTableList, fmt.Sprintf("%s.%s", destSchema, destTableName))

			// 重要：将此表标记为已处理，以防止后续的索引比较逻辑生成额外的ALTER语句
			tableKey := fmt.Sprintf("%s.%s", destSchema, destTableName)
			stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)

			continue
		}

		// 处理特殊情况：源表不存在但目标表存在
		if !sourceTableExists && destTableExists {
			// 生成DROP TABLE语句
			dropTableSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", destSchema, destTableName)

			vlog = fmt.Sprintf("(%d) %s Generated DROP TABLE statement for %s.%s: %s", logThreadSeq, event, destSchema, destTableName, dropTableSql)
			global.Wlog.Debug(vlog)

			// 应用修复SQL
			vlog = fmt.Sprintf("(%d) %s Applying DROP TABLE statement to %s.%s", logThreadSeq, event, destSchema, destTableName)
			global.Wlog.Debug(vlog)
			if err = stcls.writeFixSql([]string{dropTableSql}, logThreadSeq); err != nil {
				return nil, nil, err
			}

			// 将表添加到异常列表中
			abnormalTableList = append(abnormalTableList, fmt.Sprintf("%s.%s", destSchema, destTableName))

			// 重要：将此表标记为已处理，以防止后续的索引比较逻辑生成额外的DROP语句
			// 使用局部变量来跟踪需要删除的表
			tableKey := fmt.Sprintf("%s.%s", destSchema, destTableName)
			stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)

			continue
		}

		// 如果源表和目标表都存在，则继续原有的比较逻辑
		var sColumn, dColumn []map[string][]string

		dbf := dbExec.DataAbnormalFixStruct{
			Schema:                  destSchema, // 使用目标端schema
			Table:                   destTableName,
			DestDevice:              stcls.destDrive,
			DatafixType:             stcls.datafix,
			SourceSchema:            sourceSchema, // 添加源端schema
			CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
			DestFlavor:              stcls.destVersionInfo().Flavor,
		}
		tc := dbExec.TableColumnNameStruct{Schema: sourceSchema, Table: sourceTableName, Drive: stcls.sourceDrive, CaseSensitiveObjectName: stcls.caseSensitiveObjectName}
		sColumn, err = stcls.tableColumnName(stcls.sourceDB, tc, logThreadSeq, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Failed to get metadata for source table %s.%s: %v", logThreadSeq, event, sourceSchema, stcls.table, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}
		vlog = fmt.Sprintf("(%d) %s Source table %s.%s has %d columns", logThreadSeq, event, sourceSchema, stcls.table, len(sColumn))
		global.Wlog.Debug(vlog)

		// 使用目标端schema
		tcDest := dbExec.TableColumnNameStruct{Schema: destSchema, Table: destTableName, Drive: stcls.destDrive, CaseSensitiveObjectName: stcls.caseSensitiveObjectName}
		dColumn, err = stcls.tableColumnName(stcls.destDB, tcDest, logThreadSeq, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Failed to get metadata for target table %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}
		vlog = fmt.Sprintf("(%d) %s Target table %s.%s has %d columns", logThreadSeq, event, destSchema, stcls.table, len(dColumn))
		global.Wlog.Debug(vlog)

		sourcePartitionExpressions := stcls.loadTablePartitionExpressions(stcls.sourceDB, stcls.sourceDrive, sourceSchema, sourceTableName, stcls.caseSensitiveObjectName, logThreadSeq2)
		destPartitionExpressions := stcls.loadTablePartitionExpressions(stcls.destDB, stcls.destDrive, destSchema, destTableName, stcls.caseSensitiveObjectName, logThreadSeq2)
		partitionExpressions := append([]string{}, sourcePartitionExpressions...)
		partitionExpressions = append(partitionExpressions, destPartitionExpressions...)

		alterSlice := []string{}
		var sourceColumnSlice, destColumnSlice []string
		var sourceColumnMap, destColumnMap = make(map[string][]string), make(map[string][]string)
		var sourceColumnSeq, destColumnSeq = make(map[string]int), make(map[string]int)
		droppedAutoIncrementColumn := false
		// Keep source and target casing separately so later repair SQL can still
		// use the real target identifier even when compare keys are normalized.
		var sourceOriginalColumnNameMap = make(map[string]string)
		var destOriginalColumnNameMap = make(map[string]string)
		columnNameCaseSensitive := shouldUseCaseSensitiveColumnMatching(
			stcls.sourceDrive,
			stcls.destDrive,
			stcls.caseSensitiveObjectName,
			oracleToMySQLDataMode,
		)

		for k1, v1 := range sColumn {
			v1k := ""
			for k, v22 := range v1 {
				sourceOriginalColumnNameMap[strings.ToUpper(k)] = k

				// 根据匹配模式决定是使用原始列名还是大写列名进行比较
				if columnNameCaseSensitive {
					// 严格区分大小写，使用原始列名
					v1k = k
				} else {
					// 不区分大小写，统一使用大写键进行内部比较
					v1k = strings.ToUpper(k)
				}

				sourceColumnMap[v1k] = v22
				sourceColumnSeq[v1k] = k1
			}
			sourceColumnSlice = append(sourceColumnSlice, v1k)
		}
		for k1, v1 := range dColumn {
			v1k := ""
			for k, v22 := range v1 {
				destOriginalColumnNameMap[strings.ToUpper(k)] = k

				// 根据匹配模式决定是使用原始列名还是大写列名进行比较
				if columnNameCaseSensitive {
					// 严格区分大小写，使用原始列名
					v1k = k
				} else {
					// 不区分大小写，统一使用大写键进行内部比较
					v1k = strings.ToUpper(k)
				}

				destColumnMap[v1k] = v22
				destColumnSeq[v1k] = k1
			}
			destColumnSlice = append(destColumnSlice, v1k)
		}

		// 确保在生成SQL时使用原始大小写的列名
		// 创建一个函数来获取正确大小写的列名
		getSourceOriginalColumnName := func(colName string) string {
			if columnNameCaseSensitive {
				return colName
			}
			upperColName := strings.ToUpper(colName)
			if originalName, exists := sourceOriginalColumnNameMap[upperColName]; exists {
				return originalName
			}
			return colName
		}
		getDestOriginalColumnName := func(colName string) string {
			if columnNameCaseSensitive {
				return colName
			}
			upperColName := strings.ToUpper(colName)
			if originalName, exists := destOriginalColumnNameMap[upperColName]; exists {
				return originalName
			}
			if originalName, exists := sourceOriginalColumnNameMap[upperColName]; exists {
				return originalName
			}
			return colName
		}
		getTargetPositionColumnName := func(colName string) string {
			return getDestOriginalColumnName(colName)
		}

		addColumn, delColumn := aa.Arrcmp(sourceColumnSlice, destColumnSlice)

		// 检查是否只是列名大小写不同的情况
		// 当大小写敏感时，需要特殊处理大小写不同但实际上是同一列的情况
		if columnNameCaseSensitive {
			// 创建临时映射，用于存储大小写不敏感的列名比较
			var lowerSourceMap = make(map[string]string)
			var lowerDestMap = make(map[string]string)

			// 存储小写列名到原始列名的映射
			for _, col := range sourceColumnSlice {
				lowerSourceMap[strings.ToLower(col)] = col
			}
			for _, col := range destColumnSlice {
				lowerDestMap[strings.ToLower(col)] = col
			}

			// 查找只是大小写不同的列
			var caseOnlyDiffColumns []struct {
				sourceCol string
				destCol   string
			}

			// 检查addColumn和delColumn中是否有大小写对应的列
			for _, addCol := range addColumn {
				lowerAddCol := strings.ToLower(addCol)
				if destCol, exists := lowerDestMap[lowerAddCol]; exists {
					// 找到一个只是大小写不同的列
					caseOnlyDiffColumns = append(caseOnlyDiffColumns, struct {
						sourceCol string
						destCol   string
					}{sourceCol: addCol, destCol: destCol})
				}
			}

			// 从addColumn和delColumn中移除这些大小写不同的列
			var newAddColumn []string
			var newDelColumn []string

			// 创建一个集合来快速查找大小写不同的列
			caseDiffDestCols := make(map[string]bool)
			for _, colPair := range caseOnlyDiffColumns {
				caseDiffDestCols[colPair.destCol] = true
			}

			// 过滤addColumn，移除大小写不同的列
			for _, addCol := range addColumn {
				isCaseDiff := false
				for _, colPair := range caseOnlyDiffColumns {
					if addCol == colPair.sourceCol {
						isCaseDiff = true
						break
					}
				}
				if !isCaseDiff {
					newAddColumn = append(newAddColumn, addCol)
				}
			}

			// 过滤delColumn，移除大小写不同的列
			for _, delCol := range delColumn {
				if !caseDiffDestCols[delCol] {
					newDelColumn = append(newDelColumn, delCol)
				}
			}

			// 更新addColumn和delColumn
			addColumn = newAddColumn
			delColumn = newDelColumn

			// 为大小写不同的列生成CHANGE操作，并从destColumnMap中移除目标列
			// 同时将源列添加到destColumnMap中，避免后续代码重复处理
			for _, colPair := range caseOnlyDiffColumns {
				// 获取源列的定义
				if sourceDef, exists := sourceColumnMap[colPair.sourceCol]; exists {
					// 查找列的位置信息
					var position int
					var lastColumn string
					for i, col := range sourceColumnSlice {
						if col == colPair.sourceCol {
							position = i
							if i > 0 {
								lastColumn = sourceColumnSlice[i-1]
							} else {
								lastColumn = "alterNoAfter"
							}
							break
						}
					}

					// Oracle→MySQL: 列名仅大小写不同时不视为真实差异。
					// Oracle 默认将列名存为大写，MySQL 列名大小写不敏感，
					// 不应生成 CHANGE COLUMN SQL，避免误报 inconsistent。
					// 只需将 destColumnMap key 从旧列名（MySQL 小写）重映射到
					// 源列名（Oracle 大写），同时保留原始 MySQL 列定义供后续类型比对。
					if stcls.isOracleToMySQL() {
						originalDestDef := destColumnMap[colPair.destCol]
						delete(destColumnMap, colPair.destCol)
						destColumnMap[colPair.sourceCol] = originalDestDef
						// Preserve the *target* column's ordinal position when
						// renaming the map key. Overwriting with the source
						// ordinal would hide real column-order mismatches that
						// should still be detected (e.g. MySQL DDL reordered a
						// column relative to the Oracle definition).
						if destOrd, ok := destColumnSeq[colPair.destCol]; ok {
							destColumnSeq[colPair.sourceCol] = destOrd
							delete(destColumnSeq, colPair.destCol)
						}
						vlog = fmt.Sprintf("(%d) %s Column %s only differs in case from %s (Oracle→MySQL: case difference skipped, not a real mismatch)", logThreadSeq, event, colPair.destCol, colPair.sourceCol)
						global.Wlog.Debug(vlog)
						_ = sourceDef
						continue
					}

					// 生成CHANGE操作的SQL
					// 使用格式"原始列名:新列名"
					changeColName := fmt.Sprintf("%s:%s", colPair.destCol, colPair.sourceCol)
					changeSql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("change", sourceDef, position, lastColumn, changeColName, logThreadSeq)
					alterSlice = append(alterSlice, changeSql)

					vlog = fmt.Sprintf("(%d) %s Column %s only differs in case from %s, using CHANGE instead of DROP+ADD", logThreadSeq, event, colPair.destCol, colPair.sourceCol)
					global.Wlog.Info(vlog)

					// 从destColumnMap中移除目标列（旧列名）
					delete(destColumnMap, colPair.destCol)
					// 将源列（新列名）添加到destColumnMap中，避免后续代码重复处理
					destColumnMap[colPair.sourceCol] = sourceDef
					// 更新列的顺序信息
					destColumnSeq[colPair.sourceCol] = sourceColumnSeq[colPair.sourceCol]
				}
			}
		}

		// columns 模式：在 data 预检中，把用户已明确映射的列对从 add/delColumn 中豁免，
		// 避免因列重命名映射被误判为 DDL mismatch 而跳过数据校验。
		// struct 检查仍保留完整差异（用户可能确实想看重命名差异）。
		// 精确匹配当前表对，避免多表批次中误伤无关表（与 table_query_concurrency.go:239 保持一致）。
		if stcls.columnPlan != nil && stcls.checkRules.CheckObject != "struct" &&
			(stcls.columnPlan.SourceSchema == "" ||
				(strings.EqualFold(stcls.columnPlan.SourceSchema, sourceSchema) &&
					strings.EqualFold(stcls.columnPlan.SourceTable, sourceTableName) &&
					strings.EqualFold(stcls.columnPlan.TargetSchema, destSchema) &&
					strings.EqualFold(stcls.columnPlan.TargetTable, destTableName))) {
			addRemoveSet := make(map[string]bool)
			delRemoveSet := make(map[string]bool)
			for _, pair := range stcls.columnPlan.Pairs {
				srcUpper := strings.ToUpper(pair.SourceColumn)
				dstUpper := strings.ToUpper(pair.TargetColumn)
				for _, ac := range addColumn {
					if strings.ToUpper(ac) == srcUpper {
						addRemoveSet[ac] = true
						break
					}
				}
				for _, dc := range delColumn {
					if strings.ToUpper(dc) == dstUpper {
						delRemoveSet[dc] = true
						break
					}
				}
			}
			if len(addRemoveSet) > 0 || len(delRemoveSet) > 0 {
				filtered := addColumn[:0]
				for _, c := range addColumn {
					if !addRemoveSet[c] {
						filtered = append(filtered, c)
					}
				}
				addColumn = filtered
				filtered = delColumn[:0]
				for _, c := range delColumn {
					if !delRemoveSet[c] {
						filtered = append(filtered, c)
					}
				}
				delColumn = filtered
			}
		}

		// 移除对data类型的特殊处理，只处理struct类型的检查对象
		if stcls.checkRules.CheckObject != "struct" {
			addColumn, ignoredSourceHiddenColumns := filterIgnorableGeneratedInvisibleColumns(addColumn, sourceColumnMap)
			delColumn, ignoredTargetHiddenColumns := filterIgnorableGeneratedInvisibleColumns(delColumn, destColumnMap)
			if len(ignoredSourceHiddenColumns) > 0 || len(ignoredTargetHiddenColumns) > 0 {
				vlog = fmt.Sprintf("(%d) %s Ignoring generated invisible column differences for data precheck %s.%s -> %s.%s - ignored source extras: %v, ignored target missing: %v",
					logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table, ignoredSourceHiddenColumns, ignoredTargetHiddenColumns)
				global.Wlog.Info(vlog)
			}
			if len(addColumn) == 0 && len(delColumn) == 0 {
				// Oracle→MySQL data 模式预检：列名全一致时仍需扫描列定义，
				// 捕获类型/字符集/排序规则的硬不兼容（如 FLOAT vs DECIMAL、
				// CHAR 长度差、DATETIME 精度差、BINARY 与 VARCHAR 互不兼容）。
				// 命中即视为 DDL 差异，标记 DDL-yes 并提示用户先做 struct 修复，
				// 避免 data 模式反复生成相同的修复 SQL。
				if stcls.isOracleToMySQL() {
					if col, reason, mismatch := stcls.detectOracleToMySQLColumnHardMismatch(
						sourceColumnMap, destColumnMap,
						getSourceOriginalColumnName, getDestOriginalColumnName,
					); mismatch {
						diffReason := fmt.Sprintf("DDL mismatch (Oracle→MySQL column %s): %s", col, reason)
						vlog = fmt.Sprintf("(%d) %s Oracle→MySQL data precheck detected column definition mismatch %s.%s -> %s.%s: %s",
							logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table, diffReason)
						global.Wlog.Warn(vlog)
						pod := Pod{
							Schema:      sourceSchema,
							Table:       stcls.table,
							CheckObject: "data",
							DIFFS:       "DDL-yes",
							Datafix:     stcls.datafix,
							Rows:        diffReason,
						}
						stcls.appendPod(pod)
						global.AddSkippedTableWithDiffs(sourceSchema, stcls.table, "data", diffReason, global.SkipDiffsDDLYes)
						abnormalTableList = append(abnormalTableList, mappedTableKey)
						continue
					}
				}
				// 使用目标端schema
				newCheckTableList = append(newCheckTableList, mappedTableKey)
			} else {
				// 检查是否包含INVISIBLE列的差异
				hasInvisibleColumns := false

				// 检查addColumn中是否有INVISIBLE列
				for _, col := range addColumn {
					if colDef, exists := sourceColumnMap[col]; exists && len(colDef) > 0 {
						for _, def := range colDef {
							if strings.Contains(strings.ToUpper(def), "INVISIBLE") || strings.Contains(strings.ToUpper(def), "/*80023 INVISIBLE */") {
								hasInvisibleColumns = true
								break
							}
						}
						if hasInvisibleColumns {
							break
						}
					}
				}

				// 使用正确的源和目标数据库名
				if hasInvisibleColumns {
					// 设置全局变量标记存在INVISIBLE列差异
					global.HasInvisibleColumnMismatch = true
					// 对于包含INVISIBLE列差异的情况，使用更明确的警告信息
					vlog = fmt.Sprintf("(%d) %s Structure mismatch with INVISIBLE columns %s.%s -> %s.%s - Extra: %v, Missing: %v. Data validation skipped.",
						logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table, addColumn, delColumn)
					global.Wlog.Warn(vlog)
					// 创建表结构检查记录，使用struct类型
					pod := Pod{
						Schema:      destSchema,
						Table:       stcls.table,
						CheckObject: "struct",
						DIFFS:       "DDL-yes",
						Datafix:     stcls.datafix,
					}
					stcls.appendPod(pod)
				} else {
					diffReason := fmt.Sprintf("DDL mismatch: Extra=%v, Missing=%v", addColumn, delColumn)
					vlog = fmt.Sprintf("(%d) %s Structure mismatch %s.%s -> %s.%s - Extra: %v, Missing: %v",
						logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table, addColumn, delColumn)
					global.Wlog.Warn(vlog)
					// 创建表结构检查记录，确保DDL不一致的表在报告中正确显示Diffs=yes
					pod := Pod{
						Schema:      sourceSchema,
						Table:       stcls.table,
						CheckObject: "data",
						DIFFS:       "DDL-yes",
						Datafix:     stcls.datafix,
						Rows:        diffReason,
					}
					stcls.appendPod(pod)
					global.AddSkippedTableWithDiffs(sourceSchema, stcls.table, "data", diffReason, global.SkipDiffsDDLYes)
				}
				abnormalTableList = append(abnormalTableList, mappedTableKey)
			}
			// 无论checkObject设置如何，都只生成struct类型的记录，避免重复
			continue
		}

		columnAdvisorySuggestions := make([]schemacompat.ConstraintRepairSuggestion, 0)
		columnCollationRepairCandidates := make([]columnCollationRepairCandidate, 0)
		columnRiskDifferent := false
		isOracleToMySQL := stcls.isOracleToMySQL()
		useCanonicalCompare := (strings.EqualFold(stcls.sourceDrive, "mysql") && strings.EqualFold(stcls.destDrive, "mysql")) || isOracleToMySQL
		sourceCreateSQL := ""
		destCreateSQL := ""
		sourceColumnDefinitions := make(map[string]string)
		destColumnDefinitions := make(map[string]string)
		if useCanonicalCompare && !isOracleToMySQL {
			if sourceCreateSQL, err = queryMySQLCreateTableStatement(stcls.sourceDB, sourceSchema, stcls.table); err != nil {
				vlog = fmt.Sprintf("(%d) %s Failed to query source SHOW CREATE TABLE for %s.%s: %v", logThreadSeq, event, sourceSchema, stcls.table, err)
				global.Wlog.Warn(vlog)
				sourceCreateSQL = ""
			} else {
				sourceColumnDefinitions = schemacompat.ExtractColumnDefinitionsFromCreateSQL(sourceCreateSQL)
			}
			if destCreateSQL, err = queryMySQLCreateTableStatement(stcls.destDB, destSchema, stcls.destTable); err != nil {
				vlog = fmt.Sprintf("(%d) %s Failed to query target SHOW CREATE TABLE for %s.%s: %v", logThreadSeq, event, destSchema, stcls.destTable, err)
				global.Wlog.Warn(vlog)
				destCreateSQL = ""
			} else {
				destColumnDefinitions = schemacompat.ExtractColumnDefinitionsFromCreateSQL(destCreateSQL)
			}
		}
		if isOracleToMySQL {
			// Oracle→MySQL: only query dest (MySQL) side for CREATE TABLE
			if destCreateSQL, err = queryMySQLCreateTableStatement(stcls.destDB, destSchema, stcls.destTable); err != nil {
				vlog = fmt.Sprintf("(%d) %s Failed to query target SHOW CREATE TABLE for %s.%s: %v", logThreadSeq, event, destSchema, stcls.destTable, err)
				global.Wlog.Warn(vlog)
				destCreateSQL = ""
			} else {
				destColumnDefinitions = schemacompat.ExtractColumnDefinitionsFromCreateSQL(destCreateSQL)
			}
		}

		vlog = fmt.Sprintf("(%d) %s Columns to remove from target %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, delColumn)
		global.Wlog.Debug(vlog)
		// 先删除缺失的
		if len(delColumn) > 0 {
			// 收集所有需要删除的列名
			var colsToDelete []string
			for _, v1 := range delColumn {
				if hasAutoIncrementColumnAttribute(destColumnMap[v1]) {
					droppedAutoIncrementColumn = true
				}
				// 使用原始大小写的列名生成SQL
				originalColName := getDestOriginalColumnName(v1)
				dropSql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("drop", destColumnMap[v1], 1, "", originalColName, logThreadSeq)
				alterSlice = append(alterSlice, dropSql)
				colsToDelete = append(colsToDelete, v1)
			}
			// 在循环外删除所有标记的列，并同步调整 destColumnSeq
			// 若不调整，被删列之后的列序号与源端对比时会产生偏移，
			// 导致仅有 collation 差异的列被误判为"序号不匹配"而生成重复 MODIFY。
			for _, col := range colsToDelete {
				delete(destColumnMap, col)
			}
			adjustDestColumnSeqAfterDrops(destColumnSeq, colsToDelete)
		}
		vlog = fmt.Sprintf("(%d) %s DROP SQL for %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, alterSlice)
		global.Wlog.Debug(vlog)
		for k1, v1 := range sourceColumnSlice {
			lastcolumn := ""
			var alterColumnData []string
			if k1 == 0 {
				lastcolumn = sourceColumnSlice[k1]
			} else {
				lastcolumn = sourceColumnSlice[k1-1]
			}
			// 始终使用src作为修复规则
			alterColumnData = sourceColumnMap[v1]
			if _, ok := destColumnMap[v1]; ok {
				// 直接使用strict模式，删除了永远不会执行的loose分支
				// 使用固定值：ScheckMod=strict
				// 严格比较列的所有属性
				tableAbnormalBool = false

				// 比较列类型
				sourceType := ""
				destType := ""
				if len(sourceColumnMap[v1]) > 0 {
					sourceType = sourceColumnMap[v1][0]
				}
				if len(destColumnMap[v1]) > 0 {
					destType = destColumnMap[v1][0]
				}

				sourceOriginalColName := getSourceOriginalColumnName(v1)
				destOriginalColName := getDestOriginalColumnName(v1)
				repairColumnName := destOriginalColName
				if strings.TrimSpace(repairColumnName) == "" {
					repairColumnName = sourceOriginalColName
				}
				var sourceCanonical schemacompat.CanonicalColumn
				var destCanonical schemacompat.CanonicalColumn
				if useCanonicalCompare {
					if isOracleToMySQL {
						sourceCanonical = schemacompat.CanonicalizeOracleColumnForComparison(
							sourceOriginalColName,
							sourceColumnMap[v1],
							stcls.destVersionInfo(),
						)
					} else {
						sourceCanonical = schemacompat.CanonicalizeColumnForComparison(
							sourceOriginalColName,
							sourceColumnMap[v1],
							stcls.sourceVersionInfo(),
							stcls.destVersionInfo(),
							sourceColumnDefinitions[sourceOriginalColName],
							stcls.checkRules.MariaDBJSONTargetType,
						)
					}
					destCanonical = schemacompat.CanonicalizeColumnForComparison(
						destOriginalColName,
						destColumnMap[v1],
						stcls.destVersionInfo(),
						stcls.sourceVersionInfo(),
						destColumnDefinitions[destOriginalColName],
						stcls.checkRules.MariaDBJSONTargetType,
					)
				}

				// 打印调试信息
				vlog = fmt.Sprintf("(%d) %s Column %s type comparison: source=%s, dest=%s", logThreadSeq, event, repairColumnName, sourceType, destType)
				global.Wlog.Debug(vlog)

				// 比较列类型
				if useCanonicalCompare {
					var decision schemacompat.CompatibilityDecision
					if isOracleToMySQL {
						decision = schemacompat.DecideOracleToMySQLTypeCompatibility(sourceCanonical, destCanonical)
					} else {
						decision = schemacompat.DecideColumnDefinitionCompatibility(sourceCanonical, destCanonical)
					}
					if decision.IsMismatch() {
						if shouldDeferPartitionKeyColumnRepair(partitionExpressions, decision, sourceOriginalColName, destOriginalColName) {
							vlog = fmt.Sprintf("(%d) %s Column %s definition mismatch requires manual review because it participates in the partition expression: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceType, destType, decision.Reason)
							global.Wlog.Warn(vlog)
							columnRiskDifferent = true
							columnAdvisorySuggestions = append(columnAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
								ConstraintName: repairColumnName,
								Kind:           "PARTITION KEY COLUMN",
								Level:          schemacompat.ConstraintRepairLevelAdvisoryOnly,
								Reason:         fmt.Sprintf("partition key column requires manual review: %s", decision.Reason),
							})
						} else if decision.State == schemacompat.CompatibilityWarnOnly {
							vlog = fmt.Sprintf("(%d) %s Column %s definition warning: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceType, destType, decision.Reason)
							global.Wlog.Warn(vlog)
							columnRiskDifferent = true
							columnAdvisorySuggestions = append(columnAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
								ConstraintName: repairColumnName,
								Kind:           "COLUMN ATTRIBUTE",
								Level:          schemacompat.ConstraintRepairLevelAdvisoryOnly,
								Reason:         decision.Reason,
							})
						} else {
							tableAbnormalBool = true
							vlog = fmt.Sprintf("(%d) %s Column %s definition mismatch: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceType, destType, decision.Reason)
							global.Wlog.Warn(vlog)
						}
					} else if decision.State == schemacompat.CompatibilityNormalizedEqual {
						vlog = fmt.Sprintf("(%d) %s Column %s definition normalized-equal: source=%s, dest=%s, reason=%s",
							logThreadSeq, event, repairColumnName, sourceType, destType, decision.Reason)
						global.Wlog.Debug(vlog)
					}
				} else if sourceType != destType {
					tableAbnormalBool = true
					vlog = fmt.Sprintf("(%d) %s Column %s type mismatch: source=%s, dest=%s", logThreadSeq, event, repairColumnName, sourceType, destType)
					global.Wlog.Warn(vlog)
				}

				// 比较字符集
				sourceCharset := ""
				destCharset := ""
				if len(sourceColumnMap[v1]) > 1 {
					sourceCharset = sourceColumnMap[v1][1]
				}
				if len(destColumnMap[v1]) > 1 {
					destCharset = destColumnMap[v1][1]
				}

				// 如果两者都不为空或null，则比较
				if (sourceCharset != "null" && sourceCharset != "") ||
					(destCharset != "null" && destCharset != "") {
					if useCanonicalCompare {
						var decision schemacompat.CompatibilityDecision
						if isOracleToMySQL {
							decision = schemacompat.DecideOracleToMySQLCharsetCompatibility(sourceCanonical, destCanonical)
						} else {
							decision = schemacompat.DecideColumnCharsetCompatibility(sourceCanonical, destCanonical)
						}
						if shouldDeferPartitionKeyColumnRepair(partitionExpressions, decision, sourceOriginalColName, destOriginalColName) {
							vlog = fmt.Sprintf("(%d) %s Column %s charset mismatch requires manual review because it participates in the partition expression: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCharset, destCharset, decision.Reason)
							global.Wlog.Warn(vlog)
							columnRiskDifferent = true
							columnAdvisorySuggestions = append(columnAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
								ConstraintName: repairColumnName,
								Kind:           "PARTITION KEY COLUMN",
								Level:          schemacompat.ConstraintRepairLevelAdvisoryOnly,
								Reason:         fmt.Sprintf("partition key column requires manual review: %s", decision.Reason),
							})
						} else if decision.IsMismatch() {
							tableAbnormalBool = true
							vlog = fmt.Sprintf("(%d) %s Column %s charset mismatch: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCharset, destCharset, decision.Reason)
							global.Wlog.Warn(vlog)
						} else if decision.State == schemacompat.CompatibilityNormalizedEqual {
							vlog = fmt.Sprintf("(%d) %s Column %s charset normalized-equal: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCharset, destCharset, decision.Reason)
							global.Wlog.Debug(vlog)
						}
					} else if sourceCharset != destCharset {
						tableAbnormalBool = true
						vlog = fmt.Sprintf("(%d) %s Column %s charset mismatch: source=%s, dest=%s",
							logThreadSeq, event, repairColumnName, sourceCharset, destCharset)
						global.Wlog.Warn(vlog)
					}
				}

				// 比较排序规则
				sourceCollation := ""
				destCollation := ""
				if len(sourceColumnMap[v1]) > 2 {
					sourceCollation = sourceColumnMap[v1][2]
				}
				if len(destColumnMap[v1]) > 2 {
					destCollation = destColumnMap[v1][2]
				}

				// 如果两者都不为空或null，则比较
				if (sourceCollation != "null" && sourceCollation != "") ||
					(destCollation != "null" && destCollation != "") {
					if useCanonicalCompare {
						var decision schemacompat.CompatibilityDecision
						if isOracleToMySQL {
							decision = schemacompat.DecideOracleToMySQLCollationCompatibility(sourceCanonical, destCanonical)
						} else {
							decision = schemacompat.DecideColumnCollationCompatibility(sourceCanonical, destCanonical)
						}
						// MariaDB→MySQL：非 MariaDB 特有的 collation 在 MySQL 中合法存在，视为真实差异
						if decision.State == schemacompat.CompatibilityWarnOnly && stcls.isMariaDBToMySQL() {
							if _, isMappable := schemacompat.MapMariaDBCollationToMySQL(sourceCollation); !isMappable {
								decision.State = schemacompat.CompatibilityUnsupported
								decision.Reason = fmt.Sprintf("cross-platform collation mismatch: source=%s is valid in MySQL but differs from target=%s",
									sourceCollation, destCollation)
							}
						}
						if shouldDeferPartitionKeyColumnRepair(partitionExpressions, decision, sourceOriginalColName, destOriginalColName) {
							vlog = fmt.Sprintf("(%d) %s Column %s collation mismatch requires manual review because it participates in the partition expression: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCollation, destCollation, decision.Reason)
							global.Wlog.Warn(vlog)
							columnRiskDifferent = true
							columnAdvisorySuggestions = append(columnAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
								ConstraintName: repairColumnName,
								Kind:           "PARTITION KEY COLUMN",
								Level:          schemacompat.ConstraintRepairLevelAdvisoryOnly,
								Reason:         fmt.Sprintf("partition key column requires manual review: %s", decision.Reason),
							})
						} else if decision.State == schemacompat.CompatibilityWarnOnly {
							vlog = fmt.Sprintf("(%d) %s Column %s collation warning: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCollation, destCollation, decision.Reason)
							global.Wlog.Warn(vlog)
							// 如果该列已因类型/定义差异被标记为 tableAbnormalBool=true，
							// 后续会生成包含正确 charset/collation 的 MODIFY，无需重复加入 collation repair candidates
							if tableAbnormalBool {
								vlog = fmt.Sprintf("(%d) %s Column %s collation drift skipped from repair candidates: already covered by definition mismatch repair",
									logThreadSeq, event, repairColumnName)
								global.Wlog.Debug(vlog)
							} else {
								columnCollationRepairCandidates = append(columnCollationRepairCandidates, columnCollationRepairCandidate{
									ColumnName:       repairColumnName,
									ColumnSeq:        k1,
									LastColumn:       getTargetPositionColumnName(lastcolumn),
									SourceAttrs:      append([]string(nil), alterColumnData...),
									SourceDefinition: sourceColumnDefinitions[sourceOriginalColName],
									SourceCharset:    sourceCharset,
									SourceCollation:  sourceCollation,
									DestCharset:      destCharset,
									DestCollation:    destCollation,
									Reason:           decision.Reason,
								})
							}
						} else if decision.IsMismatch() {
							tableAbnormalBool = true
							vlog = fmt.Sprintf("(%d) %s Column %s collation mismatch: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCollation, destCollation, decision.Reason)
							global.Wlog.Warn(vlog)
						} else if decision.State == schemacompat.CompatibilityNormalizedEqual {
							vlog = fmt.Sprintf("(%d) %s Column %s collation normalized-equal: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCollation, destCollation, decision.Reason)
							global.Wlog.Debug(vlog)
						}
					} else if sourceCollation != destCollation {
						tableAbnormalBool = true
						vlog = fmt.Sprintf("(%d) %s Column %s collation mismatch: source=%s, dest=%s",
							logThreadSeq, event, repairColumnName, sourceCollation, destCollation)
						global.Wlog.Warn(vlog)
					}
				}

				// 比较是否允许NULL
				sourceIsNull := ""
				destIsNull := ""
				if len(sourceColumnMap[v1]) > 3 {
					sourceIsNull = sourceColumnMap[v1][3]
				}
				if len(destColumnMap[v1]) > 3 {
					destIsNull = destColumnMap[v1][3]
				}

				nullMismatch := false
				if useCanonicalCompare {
					// Oracle 返回 Y/N，MySQL 返回 YES/NO；canonical 层已统一为 bool，直接比较
					nullMismatch = sourceCanonical.Nullable != destCanonical.Nullable
				} else {
					nullMismatch = sourceIsNull != destIsNull
				}
				if nullMismatch {
					tableAbnormalBool = true
					vlog = fmt.Sprintf("(%d) %s Column %s NULL constraint mismatch: source=%s, dest=%s",
						logThreadSeq, event, repairColumnName, sourceIsNull, destIsNull)
					global.Wlog.Warn(vlog)
				}

				// 比较默认值
				sourceDefault := ""
				destDefault := ""
				if len(sourceColumnMap[v1]) > 4 {
					sourceDefault = sourceColumnMap[v1][4]
				}
				if len(destColumnMap[v1]) > 4 {
					destDefault = destColumnMap[v1][4]
				}

				// 如果两者都不为null，则比较
				if sourceDefault != "null" && destDefault != "null" {
					if useCanonicalCompare {
						var decision schemacompat.CompatibilityDecision
						if stcls.isOracleToMySQL() {
							// Oracle→MySQL uses dedicated comparison so that
							// seq.NEXTVAL-cleared defaults become WarnOnly
							// instead of Unsupported.
							decision = schemacompat.DecideOracleToMySQLDefaultCompatibility(sourceCanonical, destCanonical)
						} else {
							decision = schemacompat.DecideColumnDefaultCompatibility(sourceCanonical, destCanonical)
						}
						if decision.IsMismatch() {
							tableAbnormalBool = true
							vlog = fmt.Sprintf("(%d) %s Column %s default value mismatch: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceDefault, destDefault, decision.Reason)
							global.Wlog.Warn(vlog)
						} else if decision.State == schemacompat.CompatibilityNormalizedEqual {
							vlog = fmt.Sprintf("(%d) %s Column %s default value normalized-equal: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceDefault, destDefault, decision.Reason)
							global.Wlog.Debug(vlog)
						}
					} else if sourceDefault != destDefault {
						tableAbnormalBool = true
						vlog = fmt.Sprintf("(%d) %s Column %s default value mismatch: source=%s, dest=%s",
							logThreadSeq, event, repairColumnName, sourceDefault, destDefault)
						global.Wlog.Warn(vlog)
					}
				}

				// 仅在 MySQL -> MySQL 场景比较列注释
				if stcls.isMySQLToMySQL() {
					sourceComment := ""
					destComment := ""
					if len(sourceColumnMap[v1]) > 5 {
						sourceComment = normalizeMetadataComment(sourceColumnMap[v1][5])
					}
					if len(destColumnMap[v1]) > 5 {
						destComment = normalizeMetadataComment(destColumnMap[v1][5])
					}
					if sourceComment != destComment {
						tableAbnormalBool = true
						vlog = fmt.Sprintf("(%d) %s Column %s comment mismatch: source=%q, dest=%q",
							logThreadSeq, event, repairColumnName, sourceComment, destComment)
						global.Wlog.Warn(vlog)
					}
				}

				// 比较列顺序
				// 注意：当添加一个自增列作为主键并使用FIRST关键字时，其他列的顺序自然会被调整
				// 因此需要检查是否有添加自增列的操作，如果有，跳过因为这个原因导致的列顺序不匹配
				hasAutoIncrementPrimaryKeyAdd := false
				for _, alterOp := range alterSlice {
					if strings.Contains(strings.ToUpper(alterOp), "ADD COLUMN") &&
						strings.Contains(strings.ToUpper(alterOp), "AUTO_INCREMENT") &&
						strings.Contains(strings.ToUpper(alterOp), "PRIMARY KEY") &&
						strings.Contains(strings.ToUpper(alterOp), "FIRST") {
						hasAutoIncrementPrimaryKeyAdd = true
						break
					}
				}

				if !hasAutoIncrementPrimaryKeyAdd && sourceColumnSeq[v1] != destColumnSeq[v1] {
					tableAbnormalBool = true
					vlog = fmt.Sprintf("(%d) %s Column %s sequence mismatch: source=%d, dest=%d",
						logThreadSeq, event, repairColumnName, sourceColumnSeq[v1], destColumnSeq[v1])
					global.Wlog.Warn(vlog)
				}
				if tableAbnormalBool {
					sourceOriginalColName := getSourceOriginalColumnName(v1)
					repairColumnName := getDestOriginalColumnName(v1)
					if strings.TrimSpace(repairColumnName) == "" {
						repairColumnName = sourceOriginalColName
					}
					originalLastColumn := getTargetPositionColumnName(lastcolumn)
					repairAttrs := append([]string(nil), alterColumnData...)
					if useCanonicalCompare {
						var repairPlan schemacompat.ColumnRepairPlan
						if isOracleToMySQL {
							repairPlan = schemacompat.BuildOracleToMySQLRepairPlan(
								sourceOriginalColName,
								repairAttrs,
								stcls.destVersionInfo(),
							)
						} else {
							repairPlan = schemacompat.BuildTargetColumnRepairPlan(
								sourceOriginalColName,
								repairAttrs,
								stcls.sourceVersionInfo(),
								stcls.destVersionInfo(),
								sourceColumnDefinitions[sourceOriginalColName],
								stcls.checkRules.MariaDBJSONTargetType,
							)
						}
						if len(repairAttrs) < 6 {
							for len(repairAttrs) < 6 {
								repairAttrs = append(repairAttrs, "null")
							}
						}
						if strings.TrimSpace(repairPlan.Type) != "" {
							repairAttrs[0] = repairPlan.Type
						}
						if strings.TrimSpace(repairPlan.Charset) != "" {
							repairAttrs[1] = repairPlan.Charset
						}
						if strings.TrimSpace(repairPlan.Collation) != "" {
							repairAttrs[2] = repairPlan.Collation
						}
						if repairPlan.UseDirectDefinition {
							if len(repairAttrs) < 7 {
								repairAttrs = append(repairAttrs, repairPlan.DirectDefinition)
							} else {
								repairAttrs[6] = repairPlan.DirectDefinition
							}
						}
					}
					// Oracle nullable 格式（N/Y）规范化为 MySQL 格式（NO/YES）
					// Oracle 返回 N 表示 NOT NULL，但 FixAlterColumnSqlDispos 只识别 "NO"
					if isOracleToMySQL && len(repairAttrs) > 3 {
						switch strings.ToUpper(strings.TrimSpace(repairAttrs[3])) {
						case "N":
							repairAttrs[3] = "NO"
						case "Y":
							repairAttrs[3] = "YES"
						}
					}
					// 检查目标表是否存在主键
					if mysqlDataFix, ok := dbf.DataAbnormalFix().(*mysql.MysqlDataAbnormalFixStruct); ok {
						mysqlDataFix.CheckDestTableHasPrimaryKey(stcls.destDB, logThreadSeq)
					}
					modifySql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("modify", repairAttrs, k1, originalLastColumn, repairColumnName, logThreadSeq)
					if suggestion, gated := stcls.buildColumnShrinkAdvisory(destSchema, stcls.destTable, repairColumnName, sourceCanonical, destCanonical, modifySql); gated {
						vlog = fmt.Sprintf("(%d) %s Column %s modify repair downgraded to advisory-only by shrink safety gate: %s", logThreadSeq, event, repairColumnName, suggestion.Reason)
						global.Wlog.Warn(vlog)
						columnRiskDifferent = true
						columnAdvisorySuggestions = append(columnAdvisorySuggestions, suggestion)
					} else {
						vlog = fmt.Sprintf("(%d) %s The column name of column %s of the source and target table %s.%s:[%s.%s] is the same, but the definition of the column is inconsistent, and a modify statement is generated, and the modification statement is {%v}", logThreadSeq, event, repairColumnName, stcls.schema, stcls.table, destSchema, stcls.table, modifySql)
						global.Wlog.Warn(vlog)
						alterSlice = append(alterSlice, modifySql)
					}
				}
				delete(destColumnMap, v1)
			} else {
				var position int
				// 使用固定值：ScheckOrder=yes，总是使用源列的实际位置
				position = k1
				// Use the source identifier for ADD COLUMN and the current target
				// identifier for positional clauses when available.
				originalColName := getSourceOriginalColumnName(v1)
				originalLastColumn := getTargetPositionColumnName(lastcolumn)
				repairAttrs := append([]string(nil), sourceColumnMap[v1]...)
				if useCanonicalCompare {
					var repairPlan schemacompat.ColumnRepairPlan
					if isOracleToMySQL {
						repairPlan = schemacompat.BuildOracleToMySQLRepairPlan(
							originalColName,
							repairAttrs,
							stcls.destVersionInfo(),
						)
					} else {
						repairPlan = schemacompat.BuildTargetColumnRepairPlan(
							originalColName,
							repairAttrs,
							stcls.sourceVersionInfo(),
							stcls.destVersionInfo(),
							sourceColumnDefinitions[originalColName],
							stcls.checkRules.MariaDBJSONTargetType,
						)
					}
					if len(repairAttrs) < 6 {
						for len(repairAttrs) < 6 {
							repairAttrs = append(repairAttrs, "null")
						}
					}
					if strings.TrimSpace(repairPlan.Type) != "" {
						repairAttrs[0] = repairPlan.Type
					}
					if strings.TrimSpace(repairPlan.Charset) != "" {
						repairAttrs[1] = repairPlan.Charset
					}
					if strings.TrimSpace(repairPlan.Collation) != "" {
						repairAttrs[2] = repairPlan.Collation
					}
					if repairPlan.UseDirectDefinition {
						if len(repairAttrs) < 7 {
							repairAttrs = append(repairAttrs, repairPlan.DirectDefinition)
						} else {
							repairAttrs[6] = repairPlan.DirectDefinition
						}
					}
				}
				// Oracle nullable 格式（N/Y）规范化为 MySQL 格式（NO/YES）
				if isOracleToMySQL && len(repairAttrs) > 3 {
					switch strings.ToUpper(strings.TrimSpace(repairAttrs[3])) {
					case "N":
						repairAttrs[3] = "NO"
					case "Y":
						repairAttrs[3] = "YES"
					}
				}
				// 检查目标表是否存在主键
				if mysqlDataFix, ok := dbf.DataAbnormalFix().(*mysql.MysqlDataAbnormalFixStruct); ok {
					mysqlDataFix.CheckDestTableHasPrimaryKey(stcls.destDB, logThreadSeq)
				}
				addSql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("add", repairAttrs, position, originalLastColumn, originalColName, logThreadSeq)
				vlog = fmt.Sprintf("(%d) %s Missing column %s in %s.%s - ADD: %v", logThreadSeq, event, originalColName, destSchema, stcls.table, addSql)
				global.Wlog.Warn(vlog)
				alterSlice = append(alterSlice, addSql)
				delete(destColumnMap, v1)
			}
		}

		fixer := dbf.DataAbnormalFix()

		// 先生成列级别的修复SQL
		sqlS := fixer.FixAlterColumnSqlGenerate(alterSlice, logThreadSeq)
		constraintAdvisorySQLs := make([]string, 0)
		tableAdvisorySuggestions := make([]schemacompat.ConstraintRepairSuggestion, 0)
		executableColumnCollationRepair := false
		columnCollationRepairHandled := len(columnCollationRepairCandidates) == 0

		tableCharsetDifferent := false
		tableCollationDifferent := false
		tableCommentDifferent := false
		tableAutoIncrementRiskDifferent := false
		tableRowFormatDifferent := false
		tableCollationRiskDifferent := false
		tableCollationMappedDifferent := false
		tableCheckRiskDifferent := false
		tableUnsupportedRiskDifferent := false

		if stcls.isMySQLToMySQL() {
			sourceMeta, errSourceMeta := queryMySQLTableLevelMetadata(stcls.sourceDB, sourceSchema, stcls.table)
			if errSourceMeta != nil {
				vlog = fmt.Sprintf("(%d) %s Failed to query source table metadata for %s.%s: %v", logThreadSeq, event, sourceSchema, stcls.table, errSourceMeta)
				global.Wlog.Error(vlog)
			} else {
				destMeta, errDestMeta := queryMySQLTableLevelMetadata(stcls.destDB, destSchema, stcls.destTable)
				if errDestMeta != nil {
					vlog = fmt.Sprintf("(%d) %s Failed to query target table metadata for %s.%s: %v", logThreadSeq, event, destSchema, stcls.destTable, errDestMeta)
					global.Wlog.Error(vlog)
				} else {
					sourceMeta.TableComment = normalizeMetadataComment(sourceMeta.TableComment)
					destMeta.TableComment = normalizeMetadataComment(destMeta.TableComment)

					unsupportedFeatures := schemacompat.DetectMariaDBUnsupportedTableFeatures(sourceMeta.CreateTableSQL, stcls.sourceVersionInfo(), stcls.destVersionInfo())
					if len(unsupportedFeatures) > 0 {
						tableUnsupportedRiskDifferent = true
						vlog = fmt.Sprintf("(%d) %s MariaDB unsupported features detected for %s.%s -> %s.%s: %+v",
							logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.destTable, unsupportedFeatures)
						global.Wlog.Warn(vlog)
						constraintAdvisorySQLs = append(
							constraintAdvisorySQLs,
							buildConstraintAdvisoryLines(
								fmt.Sprintf("%s.%s MariaDB unsupported features", destSchema, stcls.destTable),
								schemacompat.BuildMariaDBUnsupportedFeatureSuggestions(destSchema, stcls.destTable, unsupportedFeatures),
							)...,
						)
					}

					// MariaDB LEFT JOIN information_schema.COLLATIONS 可能返回空的 TableCharset，
					// 在比较前从 collation 名推断 charset，避免误判为 charset mismatch
					if strings.TrimSpace(sourceMeta.TableCharset) == "" && strings.TrimSpace(sourceMeta.TableCollation) != "" {
						inferred := schemacompat.InferCharsetFromCollation(sourceMeta.TableCollation)
						if inferred != "" {
							vlog = fmt.Sprintf("(%d) %s Source table charset was empty, inferred as %s from collation %s for %s.%s",
								logThreadSeq, event, inferred, sourceMeta.TableCollation, sourceSchema, stcls.table)
							global.Wlog.Warn(vlog)
							sourceMeta.TableCharset = inferred
						}
					}

					charsetDecision := schemacompat.DecideCharsetCompatibility(sourceMeta.TableCharset, destMeta.TableCharset)
					if charsetDecision.IsMismatch() {
						tableCharsetDifferent = true
						vlog = fmt.Sprintf("(%d) %s Table charset mismatch: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCharset, destMeta.TableCharset, charsetDecision.Reason)
						global.Wlog.Warn(vlog)
					} else if charsetDecision.State == schemacompat.CompatibilityNormalizedEqual {
						vlog = fmt.Sprintf("(%d) %s Table charset normalized-equal: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCharset, destMeta.TableCharset, charsetDecision.Reason)
						global.Wlog.Debug(vlog)
					}

					// 检查是否所有列级 collation 差异都属于已知的 MariaDB→MySQL 等价映射
					allColumnCollationMapped := len(columnCollationRepairCandidates) > 0 && len(alterSlice) == 0
					if allColumnCollationMapped {
						for _, c := range columnCollationRepairCandidates {
							mapped, ok := schemacompat.MapMariaDBCollationToMySQL(c.SourceCollation)
							if !ok || !strings.EqualFold(mapped, strings.TrimSpace(c.DestCollation)) {
								allColumnCollationMapped = false
								break
							}
						}
					}

					if allColumnCollationMapped {
						// 所有列级 collation 差异都是已知的跨平台等价映射，无需生成修复 SQL
						tableCollationMappedDifferent = true
						columnCollationRepairHandled = true
						vlog = fmt.Sprintf("(%d) %s All %d column collation differences are cross-platform mappings for %s.%s -> %s.%s, no fix SQL needed",
							logThreadSeq, event, len(columnCollationRepairCandidates), sourceSchema, stcls.table, destSchema, stcls.destTable)
						global.Wlog.Warn(vlog)
					} else if repairSQLs, ok := stcls.buildColumnCollationRepairSQL(fixer, sourceMeta, destMeta, sourceColumnDefinitions, columnCollationRepairCandidates, logThreadSeq); ok {
						executableColumnCollationRepair = true
						columnCollationRepairHandled = true
						vlog = fmt.Sprintf("(%d) %s Generated executable column collation repair SQL for %s.%s -> %s.%s: %v",
							logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.destTable, repairSQLs)
						global.Wlog.Warn(vlog)
						sqlS = append(sqlS, repairSQLs...)
					} else if len(columnCollationRepairCandidates) > 0 {
						columnRiskDifferent = true
						columnCollationRepairHandled = true
						columnAdvisorySuggestions = append(columnAdvisorySuggestions, buildColumnCollationAdvisorySuggestions(columnCollationRepairCandidates)...)
					}

					collationDecision := schemacompat.DecideCollationCompatibility(sourceMeta.TableCollation, destMeta.TableCollation)
					// MariaDB→MySQL 跨平台场景：非 MariaDB 特有的 collation（如 utf8mb4_general_ci）在 MySQL 中合法存在，
					// 排序行为不同于目标端，应视为真实差异而非默认 collation 漂移
					if collationDecision.State == schemacompat.CompatibilityWarnOnly && stcls.isMariaDBToMySQL() {
						if _, isMappable := schemacompat.MapMariaDBCollationToMySQL(sourceMeta.TableCollation); !isMappable {
							collationDecision.State = schemacompat.CompatibilityUnsupported
							collationDecision.Reason = fmt.Sprintf("cross-platform collation mismatch: source=%s is valid in MySQL but differs from target=%s",
								sourceMeta.TableCollation, destMeta.TableCollation)
							vlog = fmt.Sprintf("(%d) %s Reclassified table collation drift as hard mismatch for MariaDB→MySQL: source=%s, dest=%s",
								logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation)
							global.Wlog.Debug(vlog)
						}
					}
					if collationDecision.State == schemacompat.CompatibilityWarnOnly {
						// 检查是否为 MariaDB→MySQL 已知的 collation 等价映射（如 uca1400→0900）
						mappedCollation, isMappable := schemacompat.MapMariaDBCollationToMySQL(sourceMeta.TableCollation)
						if isMappable && strings.EqualFold(mappedCollation, strings.TrimSpace(destMeta.TableCollation)) {
							// 已知的跨平台 collation 等价映射，标记为 collation-mapped，不生成任何 fix SQL
							tableCollationMappedDifferent = true
							vlog = fmt.Sprintf("(%d) %s Table collation-mapped: source=%s maps to target=%s, no fix SQL needed",
								logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation)
							global.Wlog.Warn(vlog)
						} else if executableColumnCollationRepair || tableCharsetDifferent {
							// 可执行的列级 collation 修复 SQL 或表级 charset 差异修复已包含 CONVERT TO CHARACTER SET，
							// 跳过重复的表级 advisory 输出
							vlog = fmt.Sprintf("(%d) %s Table collation drift already covered by executable column collation repair: source=%s, dest=%s", logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation)
							global.Wlog.Debug(vlog)
						} else {
							tableCollationRiskDifferent = true
							vlog = fmt.Sprintf("(%d) %s Table collation warning: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation, collationDecision.Reason)
							global.Wlog.Warn(vlog)
							tableAdvisorySuggestions = append(tableAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
								Kind:   "TABLE COLLATION",
								Level:  schemacompat.ConstraintRepairLevelAdvisoryOnly,
								Reason: collationDecision.Reason,
								Statements: func() []string {
									advisoryCollation := sourceMeta.TableCollation
									if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(advisoryCollation); ok {
										advisoryCollation = mapped
									}
									return fixer.FixTableCharsetSqlGenerate(sourceMeta.TableCharset, advisoryCollation, logThreadSeq)
								}(),
							})
						}
					} else if collationDecision.IsMismatch() {
						tableCollationDifferent = true
						vlog = fmt.Sprintf("(%d) %s Table collation mismatch: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation, collationDecision.Reason)
						global.Wlog.Warn(vlog)
					} else if collationDecision.State == schemacompat.CompatibilityNormalizedEqual {
						vlog = fmt.Sprintf("(%d) %s Table collation normalized-equal: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation, collationDecision.Reason)
						global.Wlog.Debug(vlog)
					}

					if tableCharsetDifferent || tableCollationDifferent {
						repairCollation := sourceMeta.TableCollation
						if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(repairCollation); ok {
							repairCollation = mapped
						}
						sqlS = append(sqlS, fixer.FixTableCharsetSqlGenerate(sourceMeta.TableCharset, repairCollation, logThreadSeq)...)
					}

					rowFormatDecision := schemacompat.DecideTableRowFormatCompatibility(
						schemacompat.CanonicalizeMySQLTableOptions(sourceMeta.RowFormat, sourceMeta.CreateOptions, sourceMeta.TableComment),
						schemacompat.CanonicalizeMySQLTableOptions(destMeta.RowFormat, destMeta.CreateOptions, destMeta.TableComment),
					)
					if rowFormatDecision.IsMismatch() {
						tableRowFormatDifferent = true
						vlog = fmt.Sprintf("(%d) %s Table row format mismatch: source=%s, dest=%s, reason=%s",
							logThreadSeq, event, sourceMeta.RowFormat, destMeta.RowFormat, rowFormatDecision.Reason)
						global.Wlog.Warn(vlog)
					} else if rowFormatDecision.State == schemacompat.CompatibilityNormalizedEqual {
						vlog = fmt.Sprintf("(%d) %s Table row format normalized-equal: source=%s, dest=%s, reason=%s",
							logThreadSeq, event, sourceMeta.RowFormat, destMeta.RowFormat, rowFormatDecision.Reason)
						global.Wlog.Debug(vlog)
					}

					sourceCatalog := schemacompat.BuildSchemaFeatureCatalog(stcls.sourceVersionInfo())
					destCatalog := schemacompat.BuildSchemaFeatureCatalog(stcls.destVersionInfo())
					sourceChecks := schemacompat.ExtractCheckConstraintsFromCreateSQL(sourceMeta.CreateTableSQL)
					sourceChecks = schemacompat.FilterPortableCheckConstraints(sourceChecks, stcls.sourceVersionInfo(), stcls.destVersionInfo(), sourceColumnDefinitions)
					destChecks := schemacompat.ExtractCheckConstraintsFromCreateSQL(destMeta.CreateTableSQL)
					checkDecision := schemacompat.DecideCheckConstraintCompatibility(sourceChecks, destChecks, sourceCatalog, destCatalog)
					if checkDecision.IsMismatch() {
						tableCheckRiskDifferent = true
						vlog = fmt.Sprintf("(%d) %s Table CHECK constraint risk detected for %s.%s -> %s.%s: %s",
							logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.destTable, checkDecision.Reason)
						global.Wlog.Warn(vlog)
						checkSuggestions := schemacompat.BuildCheckConstraintRepairSuggestions(destSchema, stcls.destTable, sourceChecks, destChecks, checkDecision)
						constraintAdvisorySQLs = append(
							constraintAdvisorySQLs,
							buildConstraintAdvisoryLines(fmt.Sprintf("%s.%s CHECK constraints", destSchema, stcls.destTable), checkSuggestions)...,
						)
					}

					if advisorySuggestion, needsFix := buildMySQLTableAutoIncrementAdvisory(destSchema, stcls.destTable, sourceMeta.AutoIncrement, destMeta.AutoIncrement); needsFix && !droppedAutoIncrementColumn {
						tableAutoIncrementRiskDifferent = true
						vlog = fmt.Sprintf("(%d) %s Table AUTO_INCREMENT drift recorded as advisory-only: source=%v, dest=%v", logThreadSeq, event, nullInt64ForLog(sourceMeta.AutoIncrement), nullInt64ForLog(destMeta.AutoIncrement))
						global.Wlog.Warn(vlog)
						tableAdvisorySuggestions = append(tableAdvisorySuggestions, advisorySuggestion)
					} else if needsFix && droppedAutoIncrementColumn {
						vlog = fmt.Sprintf("(%d) %s Skip table AUTO_INCREMENT repair for %s.%s because the target auto-increment column is being dropped",
							logThreadSeq, event, destSchema, stcls.table)
						global.Wlog.Debug(vlog)
					}

					if sourceMeta.TableComment != destMeta.TableComment {
						tableCommentDifferent = true
						escapedComment := escapeMySQLCommentLiteral(sourceMeta.TableComment)
						tableCommentSql := fmt.Sprintf("ALTER TABLE `%s`.`%s` COMMENT = '%s';", destSchema, stcls.destTable, escapedComment)
						vlog = fmt.Sprintf("(%d) %s Table comment mismatch: source='%s', dest='%s', generating fix SQL", logThreadSeq, event, sourceMeta.TableComment, destMeta.TableComment)
						global.Wlog.Warn(vlog)
						sqlS = append(sqlS, tableCommentSql)
					}
				}
			}
		}

		if len(tableAdvisorySuggestions) > 0 {
			constraintAdvisorySQLs = append(
				constraintAdvisorySQLs,
				buildConstraintAdvisoryLines(fmt.Sprintf("%s.%s TABLE options", destSchema, stcls.destTable), tableAdvisorySuggestions)...,
			)
		}
		if !columnCollationRepairHandled && len(columnCollationRepairCandidates) > 0 {
			columnRiskDifferent = true
			columnAdvisorySuggestions = append(columnAdvisorySuggestions, buildColumnCollationAdvisorySuggestions(columnCollationRepairCandidates)...)
		}
		if len(columnAdvisorySuggestions) > 0 {
			constraintAdvisorySQLs = append(
				constraintAdvisorySQLs,
				buildConstraintAdvisoryLines(fmt.Sprintf("%s.%s COLUMN attributes", destSchema, stcls.destTable), columnAdvisorySuggestions)...,
			)
		}

		hasWarnOnlyTableLevelDiff := columnRiskDifferent || tableAutoIncrementRiskDifferent || tableCollationRiskDifferent || tableCheckRiskDifferent || tableUnsupportedRiskDifferent
		hasCollationMappedOnly := tableCollationMappedDifferent && !columnRiskDifferent && !tableAutoIncrementRiskDifferent && !tableCollationRiskDifferent && !tableCheckRiskDifferent && !tableUnsupportedRiskDifferent
		hasHardTableLevelDiff := tableCharsetDifferent || tableCollationDifferent || tableCommentDifferent || tableRowFormatDifferent
		if len(alterSlice) > 0 || hasHardTableLevelDiff || executableColumnCollationRepair {
			abnormalTableList = append(abnormalTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
		} else if hasWarnOnlyTableLevelDiff {
			stcls.structWarnOnlyDiffsMap[fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)] = true
			newCheckTableList = append(newCheckTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
		} else if hasCollationMappedOnly {
			stcls.structCollationMappedMap[fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)] = true
			newCheckTableList = append(newCheckTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
		} else {
			newCheckTableList = append(newCheckTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
		}

		vlog = fmt.Sprintf("(%d) %s Structure validation completed for %s.%s -> %s.%s", logThreadSeq, event, stcls.schema, stcls.table, destSchema, stcls.table)
		global.Wlog.Debug(vlog)

		// 如果sqlS不为空（表示没有应用过列级别修复），则应用它
		if len(sqlS) > 0 {
			tableKey := fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)
			stcls.rememberColumnRepairOperations(tableKey, sqlS)
			vlog = fmt.Sprintf("(%d) %s Deferred column/table repair statements for %s.%s until index reconciliation: %v", logThreadSeq, event, destSchema, stcls.table, sqlS)
			global.Wlog.Debug(vlog)
		}
		if len(constraintAdvisorySQLs) > 0 {
			vlog = fmt.Sprintf("(%d) %s Writing advisory-only constraint repair suggestions for %s.%s", logThreadSeq, event, destSchema, stcls.destTable)
			global.Wlog.Debug(vlog)
			if err = stcls.writeAdvisoryFixSql(constraintAdvisorySQLs, logThreadSeq); err != nil {
				return nil, nil, err
			}
		}
	}
	vlog = fmt.Sprintf("(%d) %s Table structure validation completed", logThreadSeq, event)
	global.Wlog.Info(vlog)

	return newCheckTableList, abnormalTableList, nil
}
