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

// resolveTableMapping 处理 P1：解析"schema.table[:schema.table]"格式，应用全局 schema 映射，
// 并把当前处理的表名回写到 stcls.schema/.table/.destTable。ok=false 表示输入格式非法，调用方应跳过。
func (stcls *schemaTable) resolveTableMapping(item string, logThreadSeq int64, event string) (sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey string, ok bool) {
	sourceTable := item
	destTable := item
	if strings.Contains(item, ":") {
		parts := strings.Split(item, ":")
		sourceTable = parts[0]
		destTable = parts[1]
	}

	sourceParts := strings.Split(sourceTable, ".")
	if len(sourceParts) < 2 {
		global.Wlog.Error(fmt.Sprintf("(%d) %s Invalid table format: %s, expected schema.table", logThreadSeq, event, sourceTable))
		return "", "", "", "", "", false
	}
	sourceSchema = sourceParts[0]
	sourceTableName = sourceParts[1]

	destParts := strings.Split(destTable, ".")
	if len(destParts) < 2 {
		global.Wlog.Error(fmt.Sprintf("(%d) %s Invalid table format: %s, expected schema.table", logThreadSeq, event, destTable))
		return "", "", "", "", "", false
	}
	destSchema = destParts[0]
	destTableName = destParts[1]

	stcls.schema = sourceSchema
	stcls.table = sourceTableName
	stcls.destTable = destTableName

	if sourceTable == destTable && sourceSchema == destSchema {
		if mappedSchema, exists := stcls.tableMappings[sourceSchema]; exists {
			destSchema = mappedSchema
		}
	}

	global.Wlog.Debug(fmt.Sprintf("Table mapping options - source: %s, target: %s, mappings: %v", sourceSchema, destSchema, stcls.tableMappings))
	mappedTableKey = fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)
	if sourceSchema != destSchema || sourceTableName != destTableName {
		mappedTableKey = fmt.Sprintf("%s.%s:%s.%s", sourceSchema, sourceTableName, destSchema, destTableName)
	}

	global.Wlog.Debug(fmt.Sprintf("(%d %s Validating table structure %s.%s -> %s.%s", logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table))
	return sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, true
}

// checkTableExistence 处理 P2：查询并校验源/目标表存在性。
// 当任一端缺失且当前分支应登记 Pod 并跳过该表时，skip=true；
// 否则 skip=false，调用方继续后续阶段（可能落入 P3 源存在但目标缺失的分支）。
func (stcls *schemaTable) checkTableExistence(
	sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, event string,
	logThreadSeq int64,
) (sourceTableExists, destTableExists, skip bool, err error) {
	sourceTableExists, err = stcls.tableExistsByDrive(stcls.sourceDB, stcls.sourceDrive, sourceSchema, sourceTableName, "table")
	if err != nil {
		vlog := fmt.Sprintf("(%d) %s Error checking source table existence %s.%s: %v", logThreadSeq, event, sourceSchema, sourceTableName, err)
		global.Wlog.Error(vlog)
		return false, false, false, err
	}
	destTableExists, err = stcls.tableExistsByDrive(stcls.destDB, stcls.destDrive, destSchema, destTableName, "table")
	if err != nil {
		vlog := fmt.Sprintf("(%d) %s Error checking target table existence %s.%s: %v", logThreadSeq, event, destSchema, destTableName, err)
		global.Wlog.Error(vlog)
		return false, false, false, err
	}

	if sourceTableExists && destTableExists {
		return true, true, false, nil
	}

	oracleToMySQLDataMode := stcls.sourceDrive == "godror" && stcls.destDrive == "mysql" && stcls.checkRules.CheckObject != "struct"

	if oracleToMySQLDataMode {
		diffReason := "table missing on one side"
		if !sourceTableExists && !destTableExists {
			diffReason = "table missing on both source and target"
		} else if !sourceTableExists {
			diffReason = "table missing on source"
		} else if !destTableExists {
			diffReason = "table missing on target"
		}
		stcls.appendPod(Pod{
			Schema:      sourceSchema,
			Table:       sourceTableName,
			CheckObject: "data",
			DIFFS:       "DDL-yes",
			Datafix:     stcls.datafix,
			Rows:        diffReason,
		})
		global.AddSkippedTableWithDiffs(sourceSchema, sourceTableName, "data", diffReason, global.SkipDiffsDDLYes)
		vlog := fmt.Sprintf("(%d) %s Skip data check for %s.%s due to DDL mismatch: %s", logThreadSeq, event, sourceSchema, sourceTableName, diffReason)
		global.Wlog.Warn(vlog)
		_ = mappedTableKey
		return sourceTableExists, destTableExists, true, nil
	}

	if !sourceTableExists && !destTableExists {
		vlog := fmt.Sprintf("(%d) %s Source/target table both missing: %s.%s -> %s.%s", logThreadSeq, event, sourceSchema, sourceTableName, destSchema, destTableName)
		global.Wlog.Warn(vlog)
		stcls.appendPod(Pod{
			Schema:      sourceSchema,
			Table:       sourceTableName,
			CheckObject: stcls.checkRules.CheckObject,
			DIFFS:       "yes",
			Datafix:     stcls.datafix,
		})
		return false, false, true, nil
	}

	if !sourceTableExists {
		vlog := fmt.Sprintf("(%d) %s Source table %s.%s does not exist", logThreadSeq, event, sourceSchema, sourceTableName)
		global.Wlog.Warn(vlog)
		global.AddSkippedTableWithDiffs(sourceSchema, sourceTableName, "data", "table does not exist", global.SkipDiffsDDLYes)
		stcls.appendPod(Pod{
			Schema:      sourceSchema,
			Table:       sourceTableName,
			CheckObject: stcls.checkRules.CheckObject,
			DIFFS:       "yes",
			Datafix:     stcls.datafix,
		})
		return false, true, true, nil
	}

	// sourceTableExists && !destTableExists — 由 P3 分支（handleTargetMissingTable）处理。
	return sourceTableExists, destTableExists, false, nil
}

// columnMetaState 汇集 P5 阶段产出的列元数据与大小写归并后的中间状态，
// 供 P6 / P7 在同一次表处理中消费。
type columnMetaState struct {
	sColumn, dColumn                   []map[string][]string
	dbf                                dbExec.DataAbnormalFixStruct
	sourceColumnMap, destColumnMap     map[string][]string
	sourceColumnSeq, destColumnSeq     map[string]int
	sourceColumnSlice, destColumnSlice []string
	sourceOriginalColumnNameMap        map[string]string
	destOriginalColumnNameMap          map[string]string
	columnNameCaseSensitive            bool
	getSourceOriginalColumnName        func(string) string
	getDestOriginalColumnName          func(string) string
	getTargetPositionColumnName        func(string) string
	addColumn, delColumn               []string
	alterSlice                         []string
	sourcePartitionExpressions         []string
	destPartitionExpressions           []string
	partitionExpressions               []string
}

// structModeState holds the mutable state accumulated during P7 (struct-mode column reconciliation).
// It is created by prepareStructModeState and threaded through the sub-phase methods.
type structModeState struct {
	columnAdvisorySuggestions       []schemacompat.ConstraintRepairSuggestion
	columnCollationRepairCandidates []columnCollationRepairCandidate
	columnRiskDifferent             bool
	isOracleToMySQL                 bool
	useCanonicalCompare             bool
	sourceColumnDefinitions         map[string]string
	destColumnDefinitions           map[string]string
	droppedAutoIncrementColumn      bool
	alterSlice                      []string
}

// charsetAdvisoryResult 承载 P7 8f 字符集/排序规则 advisory 阶段的输出结果。
type charsetAdvisoryResult struct {
	sqlS                            []string
	constraintAdvisorySQLs          []string
	columnAdvisorySuggestions       []schemacompat.ConstraintRepairSuggestion
	columnRiskDifferent             bool
	executableColumnCollationRepair bool
	tableCharsetDifferent           bool
	tableCollationDifferent         bool
	tableCommentDifferent           bool
	tableAutoIncrementRiskDifferent bool
	tableRowFormatDifferent         bool
	tableCollationRiskDifferent     bool
	tableCollationMappedDifferent   bool
	tableCheckRiskDifferent         bool
	tableUnsupportedRiskDifferent   bool
}

// loadAndNormalizeColumns 处理 P5：查询源/目标列元数据，按大小写敏感策略归并，
// 计算 add/delColumn 并为"仅大小写差异"的列对生成 CHANGE SQL（Oracle→MySQL 场景豁免）。
func (stcls *schemaTable) loadAndNormalizeColumns(
	sourceSchema, sourceTableName, destSchema, destTableName, event string,
	oracleToMySQLDataMode bool,
	aa *CheckSumTypeStruct,
	logThreadSeq, logThreadSeq2 int64,
) (*columnMetaState, error) {
	cm := &columnMetaState{
		sourceColumnMap:             make(map[string][]string),
		destColumnMap:               make(map[string][]string),
		sourceColumnSeq:             make(map[string]int),
		destColumnSeq:               make(map[string]int),
		sourceOriginalColumnNameMap: make(map[string]string),
		destOriginalColumnNameMap:   make(map[string]string),
		alterSlice:                  []string{},
	}

	cm.dbf = dbExec.DataAbnormalFixStruct{
		Schema:                  destSchema,
		Table:                   destTableName,
		DestDevice:              stcls.destDrive,
		DatafixType:             stcls.datafix,
		SourceSchema:            sourceSchema,
		CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
		DestFlavor:              stcls.destVersionInfo().Flavor,
	}

	tc := dbExec.TableColumnNameStruct{Schema: sourceSchema, Table: sourceTableName, Drive: stcls.sourceDrive, CaseSensitiveObjectName: stcls.caseSensitiveObjectName}
	sColumn, err := stcls.tableColumnName(stcls.sourceDB, tc, logThreadSeq, logThreadSeq2)
	if err != nil {
		global.Wlog.Error(fmt.Sprintf("(%d) %s Failed to get metadata for source table %s.%s: %v", logThreadSeq, event, sourceSchema, stcls.table, err))
		return nil, err
	}
	global.Wlog.Debug(fmt.Sprintf("(%d) %s Source table %s.%s has %d columns", logThreadSeq, event, sourceSchema, stcls.table, len(sColumn)))
	cm.sColumn = sColumn

	tcDest := dbExec.TableColumnNameStruct{Schema: destSchema, Table: destTableName, Drive: stcls.destDrive, CaseSensitiveObjectName: stcls.caseSensitiveObjectName}
	dColumn, err := stcls.tableColumnName(stcls.destDB, tcDest, logThreadSeq, logThreadSeq2)
	if err != nil {
		global.Wlog.Error(fmt.Sprintf("(%d) %s Failed to get metadata for target table %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, err))
		return nil, err
	}
	global.Wlog.Debug(fmt.Sprintf("(%d) %s Target table %s.%s has %d columns", logThreadSeq, event, destSchema, stcls.table, len(dColumn)))
	cm.dColumn = dColumn

	cm.sourcePartitionExpressions = stcls.loadTablePartitionExpressions(stcls.sourceDB, stcls.sourceDrive, sourceSchema, sourceTableName, stcls.caseSensitiveObjectName, logThreadSeq2)
	cm.destPartitionExpressions = stcls.loadTablePartitionExpressions(stcls.destDB, stcls.destDrive, destSchema, destTableName, stcls.caseSensitiveObjectName, logThreadSeq2)
	cm.partitionExpressions = append([]string{}, cm.sourcePartitionExpressions...)
	cm.partitionExpressions = append(cm.partitionExpressions, cm.destPartitionExpressions...)

	cm.columnNameCaseSensitive = shouldUseCaseSensitiveColumnMatching(
		stcls.sourceDrive,
		stcls.destDrive,
		stcls.caseSensitiveObjectName,
		oracleToMySQLDataMode,
	)

	stcls.buildColumnMaps(cm)
	stcls.installColumnNameAccessors(cm)

	cm.addColumn, cm.delColumn = aa.Arrcmp(cm.sourceColumnSlice, cm.destColumnSlice)

	if cm.columnNameCaseSensitive {
		stcls.mergeCaseOnlyColumnDiffs(cm, logThreadSeq, event)
	}

	return cm, nil
}

// buildColumnMaps 根据大小写敏感策略把原始 sColumn/dColumn 展开成便于比对的 map+slice。
func (stcls *schemaTable) buildColumnMaps(cm *columnMetaState) {
	for k1, v1 := range cm.sColumn {
		v1k := ""
		for k, v22 := range v1 {
			cm.sourceOriginalColumnNameMap[strings.ToUpper(k)] = k
			if cm.columnNameCaseSensitive {
				v1k = k
			} else {
				v1k = strings.ToUpper(k)
			}
			cm.sourceColumnMap[v1k] = v22
			cm.sourceColumnSeq[v1k] = k1
		}
		cm.sourceColumnSlice = append(cm.sourceColumnSlice, v1k)
	}
	for k1, v1 := range cm.dColumn {
		v1k := ""
		for k, v22 := range v1 {
			cm.destOriginalColumnNameMap[strings.ToUpper(k)] = k
			if cm.columnNameCaseSensitive {
				v1k = k
			} else {
				v1k = strings.ToUpper(k)
			}
			cm.destColumnMap[v1k] = v22
			cm.destColumnSeq[v1k] = k1
		}
		cm.destColumnSlice = append(cm.destColumnSlice, v1k)
	}
}

// installColumnNameAccessors 为 SQL 生成阶段安装"归一键 → 原始大小写列名"访问器。
func (stcls *schemaTable) installColumnNameAccessors(cm *columnMetaState) {
	sourceOriginalColumnNameMap := cm.sourceOriginalColumnNameMap
	destOriginalColumnNameMap := cm.destOriginalColumnNameMap
	caseSensitive := cm.columnNameCaseSensitive

	cm.getSourceOriginalColumnName = func(colName string) string {
		if caseSensitive {
			return colName
		}
		upperColName := strings.ToUpper(colName)
		if originalName, exists := sourceOriginalColumnNameMap[upperColName]; exists {
			return originalName
		}
		return colName
	}
	cm.getDestOriginalColumnName = func(colName string) string {
		if caseSensitive {
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
	cm.getTargetPositionColumnName = func(colName string) string {
		return cm.getDestOriginalColumnName(colName)
	}
}

// mergeCaseOnlyColumnDiffs 在大小写敏感匹配下识别"仅列名大小写不同"的列对，
// 把它们从 add/delColumn 中剔除；非 Oracle→MySQL 情形同时生成 CHANGE 修复 SQL。
func (stcls *schemaTable) mergeCaseOnlyColumnDiffs(cm *columnMetaState, logThreadSeq int64, event string) {
	lowerSourceMap := make(map[string]string)
	lowerDestMap := make(map[string]string)
	for _, col := range cm.sourceColumnSlice {
		lowerSourceMap[strings.ToLower(col)] = col
	}
	for _, col := range cm.destColumnSlice {
		lowerDestMap[strings.ToLower(col)] = col
	}

	var caseOnlyDiffColumns []struct {
		sourceCol string
		destCol   string
	}
	for _, addCol := range cm.addColumn {
		lowerAddCol := strings.ToLower(addCol)
		if destCol, exists := lowerDestMap[lowerAddCol]; exists {
			caseOnlyDiffColumns = append(caseOnlyDiffColumns, struct {
				sourceCol string
				destCol   string
			}{sourceCol: addCol, destCol: destCol})
		}
	}

	caseDiffDestCols := make(map[string]bool)
	for _, colPair := range caseOnlyDiffColumns {
		caseDiffDestCols[colPair.destCol] = true
	}

	var newAddColumn []string
	for _, addCol := range cm.addColumn {
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
	var newDelColumn []string
	for _, delCol := range cm.delColumn {
		if !caseDiffDestCols[delCol] {
			newDelColumn = append(newDelColumn, delCol)
		}
	}
	cm.addColumn = newAddColumn
	cm.delColumn = newDelColumn

	for _, colPair := range caseOnlyDiffColumns {
		sourceDef, exists := cm.sourceColumnMap[colPair.sourceCol]
		if !exists {
			continue
		}
		var position int
		var lastColumn string
		for i, col := range cm.sourceColumnSlice {
			if col == colPair.sourceCol {
				position = i
				if i > 0 {
					lastColumn = cm.sourceColumnSlice[i-1]
				} else {
					lastColumn = "alterNoAfter"
				}
				break
			}
		}

		// Oracle→MySQL: 列名仅大小写不同时不视为真实差异。
		if stcls.isOracleToMySQL() {
			originalDestDef := cm.destColumnMap[colPair.destCol]
			delete(cm.destColumnMap, colPair.destCol)
			cm.destColumnMap[colPair.sourceCol] = originalDestDef
			if destOrd, ok := cm.destColumnSeq[colPair.destCol]; ok {
				cm.destColumnSeq[colPair.sourceCol] = destOrd
				delete(cm.destColumnSeq, colPair.destCol)
			}
			global.Wlog.Debug(fmt.Sprintf("(%d) %s Column %s only differs in case from %s (Oracle→MySQL: case difference skipped, not a real mismatch)", logThreadSeq, event, colPair.destCol, colPair.sourceCol))
			_ = sourceDef
			continue
		}

		changeColName := fmt.Sprintf("%s:%s", colPair.destCol, colPair.sourceCol)
		changeSql := cm.dbf.DataAbnormalFix().FixAlterColumnSqlDispos("change", sourceDef, position, lastColumn, changeColName, logThreadSeq)
		cm.alterSlice = append(cm.alterSlice, changeSql)

		global.Wlog.Info(fmt.Sprintf("(%d) %s Column %s only differs in case from %s, using CHANGE instead of DROP+ADD", logThreadSeq, event, colPair.destCol, colPair.sourceCol))

		delete(cm.destColumnMap, colPair.destCol)
		cm.destColumnMap[colPair.sourceCol] = sourceDef
		cm.destColumnSeq[colPair.sourceCol] = cm.sourceColumnSeq[colPair.sourceCol]
	}
}

// evaluateNonStructColumnDiff 处理 P6：非-struct 模式（data / columns 预检）下的列差异分流。
// 在 columnPlan 豁免、隐藏列过滤、Oracle→MySQL 硬不兼容扫描、INVISIBLE 列差异识别之后，
// 把表对分类为"预检通过"或"DDL 异常"并登记 Pod / Skipped 状态。调用方始终 `continue` 到下一张表。
// 返回 newKey 非空则追加到 newCheckTableList；abnormalKey 非空则追加到 abnormalTableList。
func (stcls *schemaTable) evaluateNonStructColumnDiff(
	cm *columnMetaState,
	sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, event string,
	logThreadSeq int64,
) (newKey, abnormalKey string) {
	// 列豁免：精确匹配当前表对，避免多表批次中误伤无关表。
	if stcls.columnPlan != nil &&
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
			for _, ac := range cm.addColumn {
				if strings.ToUpper(ac) == srcUpper {
					addRemoveSet[ac] = true
					break
				}
			}
			for _, dc := range cm.delColumn {
				if strings.ToUpper(dc) == dstUpper {
					delRemoveSet[dc] = true
					break
				}
			}
		}
		if len(addRemoveSet) > 0 || len(delRemoveSet) > 0 {
			filtered := cm.addColumn[:0]
			for _, c := range cm.addColumn {
				if !addRemoveSet[c] {
					filtered = append(filtered, c)
				}
			}
			cm.addColumn = filtered
			filtered = cm.delColumn[:0]
			for _, c := range cm.delColumn {
				if !delRemoveSet[c] {
					filtered = append(filtered, c)
				}
			}
			cm.delColumn = filtered
		}
	}

	addColumn, ignoredSourceHiddenColumns := filterIgnorableGeneratedInvisibleColumns(cm.addColumn, cm.sourceColumnMap)
	delColumn, ignoredTargetHiddenColumns := filterIgnorableGeneratedInvisibleColumns(cm.delColumn, cm.destColumnMap)
	cm.addColumn = addColumn
	cm.delColumn = delColumn
	if len(ignoredSourceHiddenColumns) > 0 || len(ignoredTargetHiddenColumns) > 0 {
		global.Wlog.Info(fmt.Sprintf("(%d) %s Ignoring generated invisible column differences for data precheck %s.%s -> %s.%s - ignored source extras: %v, ignored target missing: %v",
			logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table, ignoredSourceHiddenColumns, ignoredTargetHiddenColumns))
	}

	if len(addColumn) == 0 && len(delColumn) == 0 {
		// Oracle→MySQL data 模式预检：列名全一致时仍需扫描列定义，
		// 捕获类型/字符集/排序规则的硬不兼容。
		if stcls.isOracleToMySQL() {
			if col, reason, mismatch := stcls.detectOracleToMySQLColumnHardMismatch(
				cm.sourceColumnMap, cm.destColumnMap,
				cm.getSourceOriginalColumnName, cm.getDestOriginalColumnName,
			); mismatch {
				diffReason := fmt.Sprintf("DDL mismatch (Oracle→MySQL column %s): %s", col, reason)
				global.Wlog.Warn(fmt.Sprintf("(%d) %s Oracle→MySQL data precheck detected column definition mismatch %s.%s -> %s.%s: %s",
					logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table, diffReason))
				stcls.appendPod(Pod{
					Schema:      sourceSchema,
					Table:       stcls.table,
					CheckObject: "data",
					DIFFS:       "DDL-yes",
					Datafix:     stcls.datafix,
					Rows:        diffReason,
				})
				global.AddSkippedTableWithDiffs(sourceSchema, stcls.table, "data", diffReason, global.SkipDiffsDDLYes)
				return "", mappedTableKey
			}
		}
		return mappedTableKey, ""
	}

	// 存在真实列差异：判断是否因 INVISIBLE 列导致。
	hasInvisibleColumns := false
	for _, col := range addColumn {
		if colDef, exists := cm.sourceColumnMap[col]; exists && len(colDef) > 0 {
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

	if hasInvisibleColumns {
		global.HasInvisibleColumnMismatch = true
		global.Wlog.Warn(fmt.Sprintf("(%d) %s Structure mismatch with INVISIBLE columns %s.%s -> %s.%s - Extra: %v, Missing: %v. Data validation skipped.",
			logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table, addColumn, delColumn))
		stcls.appendPod(Pod{
			Schema:      destSchema,
			Table:       stcls.table,
			CheckObject: "struct",
			DIFFS:       "DDL-yes",
			Datafix:     stcls.datafix,
		})
	} else {
		diffReason := fmt.Sprintf("DDL mismatch: Extra=%v, Missing=%v", addColumn, delColumn)
		global.Wlog.Warn(fmt.Sprintf("(%d) %s Structure mismatch %s.%s -> %s.%s - Extra: %v, Missing: %v",
			logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table, addColumn, delColumn))
		stcls.appendPod(Pod{
			Schema:      sourceSchema,
			Table:       stcls.table,
			CheckObject: "data",
			DIFFS:       "DDL-yes",
			Datafix:     stcls.datafix,
			Rows:        diffReason,
		})
		global.AddSkippedTableWithDiffs(sourceSchema, stcls.table, "data", diffReason, global.SkipDiffsDDLYes)
	}
	return "", mappedTableKey
}

// prepareStructCheck 处理 P0：初始化 stcls 状态映射、发送 MariaDB sequence 告警、
// 并按需预加载源/目标端 BASE TABLE 名单与 Oracle 分区/列元数据。
func (stcls *schemaTable) prepareStructCheck(checkTableList []string, logThreadSeq, logThreadSeq2 int64, event string) {
	if stcls.structWarnOnlyDiffsMap == nil {
		stcls.structWarnOnlyDiffsMap = make(map[string]bool)
	}
	if stcls.structCollationMappedMap == nil {
		stcls.structCollationMappedMap = make(map[string]bool)
	}
	stcls.emitMariaDBSequenceWarnings(checkTableList, logThreadSeq)
	global.Wlog.Debug(fmt.Sprintf("(%d) %s Validating structure differences between source and target", logThreadSeq, event))

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
}

// applyDestFixSQL 用 destSchema/destTableName 暂时切换 stcls.schema/.table/.destTable，
// 把指定的修复 SQL 写入 fixSQL 文件，退出前恢复原值。供 P3/P4/P5 等写建表/删表/改表阶段使用。
func (stcls *schemaTable) applyDestFixSQL(destSchema, destTableName string, sqls []string, logThreadSeq int64) error {
	originalSchema, originalTable, originalDestTable := stcls.schema, stcls.table, stcls.destTable
	stcls.schema = destSchema
	stcls.table = destTableName
	stcls.destTable = destTableName
	defer func() {
		stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
	}()
	return stcls.writeFixSql(sqls, logThreadSeq)
}

// applyDestAdvisorySQL 与 applyDestFixSQL 同理，但写到 advisory 通道。
func (stcls *schemaTable) applyDestAdvisorySQL(destSchema, destTableName string, lines []string, logThreadSeq int64) error {
	originalSchema, originalTable, originalDestTable := stcls.schema, stcls.table, stcls.destTable
	stcls.schema = destSchema
	stcls.table = destTableName
	stcls.destTable = destTableName
	defer func() {
		stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
	}()
	return stcls.writeAdvisoryFixSql(lines, logThreadSeq)
}

// handleTargetMissingTable 处理 P3：源表存在但目标表不存在。
// 返回值 abnormalKey 非空时调用方需追加到 abnormalTableList；无论如何调用方都要 `continue`。
func (stcls *schemaTable) handleTargetMissingTable(
	sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, event string,
	logThreadSeq int64,
) (abnormalKey string, err error) {
	global.Wlog.Debug(fmt.Sprintf("(%d) %s Processing table creation with mapping - source: %s.%s -> dest: %s.%s", logThreadSeq, event, sourceSchema, sourceTableName, destSchema, destTableName))

	// Oracle→MySQL: generate CREATE TABLE from Oracle metadata
	if stcls.isOracleToMySQL() {
		tc := dbExec.TableColumnNameStruct{Drive: stcls.sourceDrive, Schema: sourceSchema, Table: sourceTableName}
		oracleColumns, oraErr := tc.Query().TableColumnName(stcls.sourceDB, logThreadSeq)
		if oraErr != nil {
			global.Wlog.Error(fmt.Sprintf("(%d) %s Error querying Oracle columns for CREATE TABLE %s.%s: %v", logThreadSeq, event, sourceSchema, sourceTableName, oraErr))
			return "", oraErr
		}
		// Query Oracle primary key columns so the generated CREATE TABLE
		// includes a PRIMARY KEY clause; this is required when the target
		// MySQL has sql_require_primary_key=ON (default on MySQL 8.0+).
		oracleIndexData := make(map[string][]string)
		if pkCols, pkErr := queryOraclePrimaryKeyColumns(stcls.sourceDB, sourceSchema, sourceTableName); pkErr != nil {
			global.Wlog.Warn(fmt.Sprintf("(%d) %s Warning: failed to query Oracle primary key for %s.%s: %v (proceeding without PK)", logThreadSeq, event, sourceSchema, sourceTableName, pkErr))
		} else if len(pkCols) > 0 {
			oracleIndexData["PRIMARY"] = pkCols
		}
		createTableSql := schemacompat.GenerateOracleToMySQLCreateTableSQL(destSchema, destTableName, oracleColumns, oracleIndexData, stcls.destVersionInfo())
		if createTableSql == "" {
			return "", nil
		}
		global.Wlog.Info(fmt.Sprintf("(%d) %s Generated Oracle→MySQL CREATE TABLE for %s.%s", logThreadSeq, event, destSchema, destTableName))
		if err = stcls.applyDestFixSQL(destSchema, destTableName, []string{createTableSql}, logThreadSeq); err != nil {
			return "", err
		}
		stcls.appendPod(Pod{
			Schema:      sourceSchema,
			Table:       sourceTableName,
			CheckObject: stcls.checkRules.CheckObject,
			DIFFS:       "yes",
			Datafix:     stcls.datafix,
		})
		return mappedTableKey, nil
	}

	sourceMeta, sourceMetaErr := queryMySQLTableLevelMetadata(stcls.sourceDB, sourceSchema, sourceTableName)
	if sourceMetaErr != nil {
		global.Wlog.Warn(fmt.Sprintf("(%d) %s Failed to query source table metadata for %s.%s before CREATE TABLE generation: %v", logThreadSeq, event, sourceSchema, sourceTableName, sourceMetaErr))
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
			if err = stcls.applyDestAdvisorySQL(destSchema, destTableName, advisoryLines, logThreadSeq); err != nil {
				return "", err
			}
		}

		// MariaDB-only temporal and sequence constructs must stay on the
		// advisory path because there is no safe automatic MySQL rewrite.
		unsupportedFeatures := schemacompat.DetectMariaDBUnsupportedTableFeatures(sourceMeta.CreateTableSQL, stcls.sourceVersionInfo(), stcls.destVersionInfo())
		if len(unsupportedFeatures) > 0 {
			global.Wlog.Warn(fmt.Sprintf("(%d) %s Skip automatic CREATE TABLE for %s.%s because unsupported MariaDB features were detected: %+v", logThreadSeq, event, sourceSchema, sourceTableName, unsupportedFeatures))

			advisoryLines := buildConstraintAdvisoryLines(
				fmt.Sprintf("%s.%s MariaDB unsupported features", destSchema, destTableName),
				schemacompat.BuildMariaDBUnsupportedFeatureSuggestions(destSchema, destTableName, unsupportedFeatures),
			)
			if err = stcls.applyDestAdvisorySQL(destSchema, destTableName, advisoryLines, logThreadSeq); err != nil {
				return "", err
			}

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
			// 该分支原先 append "sourceSchema.sourceTableName" 风格键；保留原 mappedTableKey 结构不必追加
			// 因为原实现此处也不 append abnormalTableList（只 continue）。
			return "", nil
		}
	}

	createTableSql, err := generateCreateTableSql(stcls.sourceDB, sourceSchema, destSchema, sourceTableName, destTableName, stcls.sourceVersionInfo(), stcls.destVersionInfo(), stcls.checkRules.MariaDBJSONTargetType, logThreadSeq)
	if err != nil {
		global.Wlog.Error(fmt.Sprintf("(%d) %s Error generating CREATE TABLE statement for %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, err))
		return "", err
	}

	if !strings.Contains(createTableSql, fmt.Sprintf("`%s`", destSchema)) {
		global.Wlog.Warn(fmt.Sprintf("(%d) %s Warning: Generated CREATE TABLE statement may be missing target schema '%s': %s", logThreadSeq, event, destSchema, createTableSql))
	}

	global.Wlog.Debug(fmt.Sprintf("(%d) %s Generated CREATE TABLE statement for %s.%s: %s", logThreadSeq, event, destSchema, destTableName, createTableSql))
	global.Wlog.Debug(fmt.Sprintf("(%d) %s Applying CREATE TABLE statement to %s.%s", logThreadSeq, event, destSchema, destTableName))
	if err = stcls.applyDestFixSQL(destSchema, destTableName, []string{createTableSql}, logThreadSeq); err != nil {
		global.Wlog.Error(fmt.Sprintf("(%d) %s Error applying CREATE TABLE statement: %v", logThreadSeq, event, err))
		return "", err
	}

	stcls.appendPod(Pod{
		Schema:      destSchema,
		Table:       destTableName,
		CheckObject: stcls.checkRules.CheckObject,
		DIFFS:       "yes",
		Datafix:     stcls.datafix,
	})

	tableKey := fmt.Sprintf("%s.%s", destSchema, destTableName)
	stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)
	return tableKey, nil
}

// handleSourceMissingTable 处理 P4：源表不存在但目标表存在。
// 生成 DROP TABLE 修复 SQL，登记 skipIndexCheckTables，返回 abnormalTableList 应追加的键。
func (stcls *schemaTable) handleSourceMissingTable(destSchema, destTableName string, logThreadSeq int64, event string) (string, error) {
	dropTableSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", destSchema, destTableName)

	vlog := fmt.Sprintf("(%d) %s Generated DROP TABLE statement for %s.%s: %s", logThreadSeq, event, destSchema, destTableName, dropTableSql)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) %s Applying DROP TABLE statement to %s.%s", logThreadSeq, event, destSchema, destTableName)
	global.Wlog.Debug(vlog)
	if err := stcls.writeFixSql([]string{dropTableSql}, logThreadSeq); err != nil {
		return "", err
	}

	tableKey := fmt.Sprintf("%s.%s", destSchema, destTableName)
	stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)
	return tableKey, nil
}

func (stcls *schemaTable) TableColumnNameCheck(checkTableList []string, logThreadSeq, logThreadSeq2 int64) ([]string, []string, error) {
	var (
		vlog                                 string
		newCheckTableList, abnormalTableList []string
		aa                                   = &CheckSumTypeStruct{}
		event                                string
	)
	stcls.prepareStructCheck(checkTableList, logThreadSeq, logThreadSeq2, event)
	for _, v := range checkTableList {
		sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, ok := stcls.resolveTableMapping(v, logThreadSeq, event)
		if !ok {
			continue
		}

		sourceTableExists, destTableExists, skip, err := stcls.checkTableExistence(sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, event, logThreadSeq)
		if err != nil {
			return nil, nil, err
		}
		if skip {
			abnormalTableList = append(abnormalTableList, mappedTableKey)
			continue
		}

		oracleToMySQLDataMode := stcls.sourceDrive == "godror" && stcls.destDrive == "mysql" && stcls.checkRules.CheckObject != "struct"

		if sourceTableExists && !destTableExists {
			abnormalKey, err := stcls.handleTargetMissingTable(sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, event, logThreadSeq)
			if err != nil {
				return nil, nil, err
			}
			if abnormalKey != "" {
				abnormalTableList = append(abnormalTableList, abnormalKey)
			}
			continue
		}

		// 处理特殊情况：源表不存在但目标表存在
		if !sourceTableExists && destTableExists {
			abnormalKey, err := stcls.handleSourceMissingTable(destSchema, destTableName, logThreadSeq, event)
			if err != nil {
				return nil, nil, err
			}
			abnormalTableList = append(abnormalTableList, abnormalKey)
			continue
		}

		cm, err := stcls.loadAndNormalizeColumns(sourceSchema, sourceTableName, destSchema, destTableName, event, oracleToMySQLDataMode, aa, logThreadSeq, logThreadSeq2)
		if err != nil {
			return nil, nil, err
		}
		if stcls.checkRules.CheckObject != "struct" {
			newKey, abnormalKey := stcls.evaluateNonStructColumnDiff(cm, sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, event, logThreadSeq)
			if newKey != "" {
				newCheckTableList = append(newCheckTableList, newKey)
			}
			if abnormalKey != "" {
				abnormalTableList = append(abnormalTableList, abnormalKey)
			}
			continue
		}

		// 8a: struct 上下文准备（SHOW CREATE TABLE / definitions）
		sms := stcls.prepareStructModeState(sourceSchema, destSchema, cm.alterSlice, logThreadSeq, event)

		vlog = fmt.Sprintf("(%d) %s Columns to remove from target %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, cm.delColumn)
		global.Wlog.Debug(vlog)
		// 8b: 删除目标端多余列（AUTO_INCREMENT 守护）
		stcls.dropExcessColumns(sms, cm, logThreadSeq, event, destSchema)
		// 8c+8d: 列差异调和（新增列 + 列修改）
		stcls.reconcileColumnDiffs(sms, cm, sourceSchema, destSchema, logThreadSeq, event)

		// 8e: 生成列级别的修复SQL
		fixer := cm.dbf.DataAbnormalFix()
		sqlS := fixer.FixAlterColumnSqlGenerate(sms.alterSlice, logThreadSeq)

		// 8f: MySQL→MySQL 字符集/排序规则/表级属性 advisory 检查
		result := stcls.buildCharsetAdvisory(sms, cm, fixer, sourceSchema, destSchema, logThreadSeq, event)
		sqlS = append(sqlS, result.sqlS...)
		constraintAdvisorySQLs := result.constraintAdvisorySQLs
		columnRiskDifferent := result.columnRiskDifferent
		executableColumnCollationRepair := result.executableColumnCollationRepair
		tableCharsetDifferent := result.tableCharsetDifferent
		tableCollationDifferent := result.tableCollationDifferent
		tableCommentDifferent := result.tableCommentDifferent
		tableAutoIncrementRiskDifferent := result.tableAutoIncrementRiskDifferent
		tableRowFormatDifferent := result.tableRowFormatDifferent
		tableCollationRiskDifferent := result.tableCollationRiskDifferent
		tableCollationMappedDifferent := result.tableCollationMappedDifferent
		tableCheckRiskDifferent := result.tableCheckRiskDifferent
		tableUnsupportedRiskDifferent := result.tableUnsupportedRiskDifferent

		hasWarnOnlyTableLevelDiff := columnRiskDifferent || tableAutoIncrementRiskDifferent || tableCollationRiskDifferent || tableCheckRiskDifferent || tableUnsupportedRiskDifferent
		hasCollationMappedOnly := tableCollationMappedDifferent && !columnRiskDifferent && !tableAutoIncrementRiskDifferent && !tableCollationRiskDifferent && !tableCheckRiskDifferent && !tableUnsupportedRiskDifferent
		hasHardTableLevelDiff := tableCharsetDifferent || tableCollationDifferent || tableCommentDifferent || tableRowFormatDifferent
		if len(sms.alterSlice) > 0 || hasHardTableLevelDiff || executableColumnCollationRepair {
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

// prepareStructModeState 初始化 P7 struct 模式每张表的运行时状态（8a）：
// 计算 isOracleToMySQL / useCanonicalCompare 标志，并通过 SHOW CREATE TABLE
// 提取源/目标端列级定义字符串，供后续 canonical 比较使用。
func (stcls *schemaTable) prepareStructModeState(
	sourceSchema, destSchema string,
	initialAlterSlice []string,
	logThreadSeq int64, event string,
) *structModeState {
	isOraToMy := stcls.isOracleToMySQL()
	useCanon := (strings.EqualFold(stcls.sourceDrive, "mysql") && strings.EqualFold(stcls.destDrive, "mysql")) || isOraToMy

	sms := &structModeState{
		columnAdvisorySuggestions:       make([]schemacompat.ConstraintRepairSuggestion, 0),
		columnCollationRepairCandidates: make([]columnCollationRepairCandidate, 0),
		isOracleToMySQL:                 isOraToMy,
		useCanonicalCompare:             useCanon,
		sourceColumnDefinitions:         make(map[string]string),
		destColumnDefinitions:           make(map[string]string),
		alterSlice:                      append([]string(nil), initialAlterSlice...),
	}

	if useCanon && !isOraToMy {
		if srcSQL, err := queryMySQLCreateTableStatement(stcls.sourceDB, sourceSchema, stcls.table); err != nil {
			vlog := fmt.Sprintf("(%d) %s Failed to query source SHOW CREATE TABLE for %s.%s: %v", logThreadSeq, event, sourceSchema, stcls.table, err)
			global.Wlog.Warn(vlog)
		} else {
			sms.sourceColumnDefinitions = schemacompat.ExtractColumnDefinitionsFromCreateSQL(srcSQL)
		}
		if dstSQL, err := queryMySQLCreateTableStatement(stcls.destDB, destSchema, stcls.destTable); err != nil {
			vlog := fmt.Sprintf("(%d) %s Failed to query target SHOW CREATE TABLE for %s.%s: %v", logThreadSeq, event, destSchema, stcls.destTable, err)
			global.Wlog.Warn(vlog)
		} else {
			sms.destColumnDefinitions = schemacompat.ExtractColumnDefinitionsFromCreateSQL(dstSQL)
		}
	}
	if isOraToMy {
		if dstSQL, err := queryMySQLCreateTableStatement(stcls.destDB, destSchema, stcls.destTable); err != nil {
			vlog := fmt.Sprintf("(%d) %s Failed to query target SHOW CREATE TABLE for %s.%s: %v", logThreadSeq, event, destSchema, stcls.destTable, err)
			global.Wlog.Warn(vlog)
		} else {
			sms.destColumnDefinitions = schemacompat.ExtractColumnDefinitionsFromCreateSQL(dstSQL)
		}
	}
	return sms
}

// dropExcessColumns 处理 P7 8b：将目标端多余的列生成 DROP COLUMN SQL，
// 并更新 cm.destColumnMap / cm.destColumnSeq 使后续阶段序号对齐。
// 若被删除的列含 AUTO_INCREMENT，sms.droppedAutoIncrementColumn 将被置为 true。
func (stcls *schemaTable) dropExcessColumns(
	sms *structModeState, cm *columnMetaState,
	logThreadSeq int64, event, destSchema string,
) {
	vlog := fmt.Sprintf("(%d) %s Columns to remove from target %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, cm.delColumn)
	global.Wlog.Debug(vlog)
	if len(cm.delColumn) == 0 {
		return
	}
	var colsToDelete []string
	for _, v1 := range cm.delColumn {
		if hasAutoIncrementColumnAttribute(cm.destColumnMap[v1]) {
			sms.droppedAutoIncrementColumn = true
		}
		originalColName := cm.getDestOriginalColumnName(v1)
		dropSql := cm.dbf.DataAbnormalFix().FixAlterColumnSqlDispos("drop", cm.destColumnMap[v1], 1, "", originalColName, logThreadSeq)
		sms.alterSlice = append(sms.alterSlice, dropSql)
		colsToDelete = append(colsToDelete, v1)
	}
	for _, col := range colsToDelete {
		delete(cm.destColumnMap, col)
	}
	adjustDestColumnSeqAfterDrops(cm.destColumnSeq, colsToDelete)
	vlog = fmt.Sprintf("(%d) %s DROP SQL for %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, sms.alterSlice)
	global.Wlog.Debug(vlog)
}

// reconcileColumnDiffs 处理 P7 8c+8d：遍历源端列序，
// 对目标端已存在的列进行严格属性比较并生成 MODIFY SQL（8d），
// 对目标端缺失的列生成 ADD COLUMN SQL（8c）。
func (stcls *schemaTable) reconcileColumnDiffs(
	sms *structModeState, cm *columnMetaState,
	sourceSchema, destSchema string,
	logThreadSeq int64, event string,
) {
	var vlog string
	for k1, v1 := range cm.sourceColumnSlice {
			lastcolumn := ""
			var alterColumnData []string
			if k1 == 0 {
				lastcolumn = cm.sourceColumnSlice[k1]
			} else {
				lastcolumn = cm.sourceColumnSlice[k1-1]
			}
			// 始终使用src作为修复规则
			alterColumnData = cm.sourceColumnMap[v1]
			if _, ok := cm.destColumnMap[v1]; ok {
				// 直接使用strict模式，删除了永远不会执行的loose分支
				// 使用固定值：ScheckMod=strict
				// 严格比较列的所有属性
				tableAbnormalBool := false

				// 比较列类型
				sourceType := ""
				destType := ""
				if len(cm.sourceColumnMap[v1]) > 0 {
					sourceType = cm.sourceColumnMap[v1][0]
				}
				if len(cm.destColumnMap[v1]) > 0 {
					destType = cm.destColumnMap[v1][0]
				}

				sourceOriginalColName := cm.getSourceOriginalColumnName(v1)
				destOriginalColName := cm.getDestOriginalColumnName(v1)
				repairColumnName := destOriginalColName
				if strings.TrimSpace(repairColumnName) == "" {
					repairColumnName = sourceOriginalColName
				}
				var sourceCanonical schemacompat.CanonicalColumn
				var destCanonical schemacompat.CanonicalColumn
				if sms.useCanonicalCompare {
					if sms.isOracleToMySQL {
						sourceCanonical = schemacompat.CanonicalizeOracleColumnForComparison(
							sourceOriginalColName,
							cm.sourceColumnMap[v1],
							stcls.destVersionInfo(),
						)
					} else {
						sourceCanonical = schemacompat.CanonicalizeColumnForComparison(
							sourceOriginalColName,
							cm.sourceColumnMap[v1],
							stcls.sourceVersionInfo(),
							stcls.destVersionInfo(),
							sms.sourceColumnDefinitions[sourceOriginalColName],
							stcls.checkRules.MariaDBJSONTargetType,
						)
					}
					destCanonical = schemacompat.CanonicalizeColumnForComparison(
						destOriginalColName,
						cm.destColumnMap[v1],
						stcls.destVersionInfo(),
						stcls.sourceVersionInfo(),
						sms.destColumnDefinitions[destOriginalColName],
						stcls.checkRules.MariaDBJSONTargetType,
					)
				}

				// 打印调试信息
				vlog = fmt.Sprintf("(%d) %s Column %s type comparison: source=%s, dest=%s", logThreadSeq, event, repairColumnName, sourceType, destType)
				global.Wlog.Debug(vlog)

				// 比较列类型
				if sms.useCanonicalCompare {
					var decision schemacompat.CompatibilityDecision
					if sms.isOracleToMySQL {
						decision = schemacompat.DecideOracleToMySQLTypeCompatibility(sourceCanonical, destCanonical)
					} else {
						decision = schemacompat.DecideColumnDefinitionCompatibility(sourceCanonical, destCanonical)
					}
					if decision.IsMismatch() {
						if shouldDeferPartitionKeyColumnRepair(cm.partitionExpressions, decision, sourceOriginalColName, destOriginalColName) {
							vlog = fmt.Sprintf("(%d) %s Column %s definition mismatch requires manual review because it participates in the partition expression: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceType, destType, decision.Reason)
							global.Wlog.Warn(vlog)
							sms.columnRiskDifferent = true
							sms.columnAdvisorySuggestions = append(sms.columnAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
								ConstraintName: repairColumnName,
								Kind:           "PARTITION KEY COLUMN",
								Level:          schemacompat.ConstraintRepairLevelAdvisoryOnly,
								Reason:         fmt.Sprintf("partition key column requires manual review: %s", decision.Reason),
							})
						} else if decision.State == schemacompat.CompatibilityWarnOnly {
							vlog = fmt.Sprintf("(%d) %s Column %s definition warning: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceType, destType, decision.Reason)
							global.Wlog.Warn(vlog)
							sms.columnRiskDifferent = true
							sms.columnAdvisorySuggestions = append(sms.columnAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
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
				if len(cm.sourceColumnMap[v1]) > 1 {
					sourceCharset = cm.sourceColumnMap[v1][1]
				}
				if len(cm.destColumnMap[v1]) > 1 {
					destCharset = cm.destColumnMap[v1][1]
				}

				// 如果两者都不为空或null，则比较
				if (sourceCharset != "null" && sourceCharset != "") ||
					(destCharset != "null" && destCharset != "") {
					if sms.useCanonicalCompare {
						var decision schemacompat.CompatibilityDecision
						if sms.isOracleToMySQL {
							decision = schemacompat.DecideOracleToMySQLCharsetCompatibility(sourceCanonical, destCanonical)
						} else {
							decision = schemacompat.DecideColumnCharsetCompatibility(sourceCanonical, destCanonical)
						}
						if shouldDeferPartitionKeyColumnRepair(cm.partitionExpressions, decision, sourceOriginalColName, destOriginalColName) {
							vlog = fmt.Sprintf("(%d) %s Column %s charset mismatch requires manual review because it participates in the partition expression: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCharset, destCharset, decision.Reason)
							global.Wlog.Warn(vlog)
							sms.columnRiskDifferent = true
							sms.columnAdvisorySuggestions = append(sms.columnAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
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
				if len(cm.sourceColumnMap[v1]) > 2 {
					sourceCollation = cm.sourceColumnMap[v1][2]
				}
				if len(cm.destColumnMap[v1]) > 2 {
					destCollation = cm.destColumnMap[v1][2]
				}

				// 如果两者都不为空或null，则比较
				if (sourceCollation != "null" && sourceCollation != "") ||
					(destCollation != "null" && destCollation != "") {
					if sms.useCanonicalCompare {
						var decision schemacompat.CompatibilityDecision
						if sms.isOracleToMySQL {
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
						if shouldDeferPartitionKeyColumnRepair(cm.partitionExpressions, decision, sourceOriginalColName, destOriginalColName) {
							vlog = fmt.Sprintf("(%d) %s Column %s collation mismatch requires manual review because it participates in the partition expression: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCollation, destCollation, decision.Reason)
							global.Wlog.Warn(vlog)
							sms.columnRiskDifferent = true
							sms.columnAdvisorySuggestions = append(sms.columnAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
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
								sms.columnCollationRepairCandidates = append(sms.columnCollationRepairCandidates, columnCollationRepairCandidate{
									ColumnName:       repairColumnName,
									ColumnSeq:        k1,
									LastColumn:       cm.getTargetPositionColumnName(lastcolumn),
									SourceAttrs:      append([]string(nil), alterColumnData...),
									SourceDefinition: sms.sourceColumnDefinitions[sourceOriginalColName],
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
				if len(cm.sourceColumnMap[v1]) > 3 {
					sourceIsNull = cm.sourceColumnMap[v1][3]
				}
				if len(cm.destColumnMap[v1]) > 3 {
					destIsNull = cm.destColumnMap[v1][3]
				}

				nullMismatch := false
				if sms.useCanonicalCompare {
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
				if len(cm.sourceColumnMap[v1]) > 4 {
					sourceDefault = cm.sourceColumnMap[v1][4]
				}
				if len(cm.destColumnMap[v1]) > 4 {
					destDefault = cm.destColumnMap[v1][4]
				}

				// 如果两者都不为null，则比较
				if sourceDefault != "null" && destDefault != "null" {
					if sms.useCanonicalCompare {
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
					if len(cm.sourceColumnMap[v1]) > 5 {
						sourceComment = normalizeMetadataComment(cm.sourceColumnMap[v1][5])
					}
					if len(cm.destColumnMap[v1]) > 5 {
						destComment = normalizeMetadataComment(cm.destColumnMap[v1][5])
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
				for _, alterOp := range sms.alterSlice {
					if strings.Contains(strings.ToUpper(alterOp), "ADD COLUMN") &&
						strings.Contains(strings.ToUpper(alterOp), "AUTO_INCREMENT") &&
						strings.Contains(strings.ToUpper(alterOp), "PRIMARY KEY") &&
						strings.Contains(strings.ToUpper(alterOp), "FIRST") {
						hasAutoIncrementPrimaryKeyAdd = true
						break
					}
				}

				if !hasAutoIncrementPrimaryKeyAdd && cm.sourceColumnSeq[v1] != cm.destColumnSeq[v1] {
					tableAbnormalBool = true
					vlog = fmt.Sprintf("(%d) %s Column %s sequence mismatch: source=%d, dest=%d",
						logThreadSeq, event, repairColumnName, cm.sourceColumnSeq[v1], cm.destColumnSeq[v1])
					global.Wlog.Warn(vlog)
				}
				if tableAbnormalBool {
					sourceOriginalColName := cm.getSourceOriginalColumnName(v1)
					repairColumnName := cm.getDestOriginalColumnName(v1)
					if strings.TrimSpace(repairColumnName) == "" {
						repairColumnName = sourceOriginalColName
					}
					originalLastColumn := cm.getTargetPositionColumnName(lastcolumn)
					repairAttrs := append([]string(nil), alterColumnData...)
					if sms.useCanonicalCompare {
						var repairPlan schemacompat.ColumnRepairPlan
						if sms.isOracleToMySQL {
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
								sms.sourceColumnDefinitions[sourceOriginalColName],
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
					if sms.isOracleToMySQL && len(repairAttrs) > 3 {
						switch strings.ToUpper(strings.TrimSpace(repairAttrs[3])) {
						case "N":
							repairAttrs[3] = "NO"
						case "Y":
							repairAttrs[3] = "YES"
						}
					}
					// 检查目标表是否存在主键
					if mysqlDataFix, ok := cm.dbf.DataAbnormalFix().(*mysql.MysqlDataAbnormalFixStruct); ok {
						mysqlDataFix.CheckDestTableHasPrimaryKey(stcls.destDB, logThreadSeq)
					}
					modifySql := cm.dbf.DataAbnormalFix().FixAlterColumnSqlDispos("modify", repairAttrs, k1, originalLastColumn, repairColumnName, logThreadSeq)
					if suggestion, gated := stcls.buildColumnShrinkAdvisory(destSchema, stcls.destTable, repairColumnName, sourceCanonical, destCanonical, modifySql); gated {
						vlog = fmt.Sprintf("(%d) %s Column %s modify repair downgraded to advisory-only by shrink safety gate: %s", logThreadSeq, event, repairColumnName, suggestion.Reason)
						global.Wlog.Warn(vlog)
						sms.columnRiskDifferent = true
						sms.columnAdvisorySuggestions = append(sms.columnAdvisorySuggestions, suggestion)
					} else {
						vlog = fmt.Sprintf("(%d) %s The column name of column %s of the source and target table %s.%s:[%s.%s] is the same, but the definition of the column is inconsistent, and a modify statement is generated, and the modification statement is {%v}", logThreadSeq, event, repairColumnName, stcls.schema, stcls.table, destSchema, stcls.table, modifySql)
						global.Wlog.Warn(vlog)
						sms.alterSlice = append(sms.alterSlice, modifySql)
					}
				}
				delete(cm.destColumnMap, v1)
			} else {
				var position int
				// 使用固定值：ScheckOrder=yes，总是使用源列的实际位置
				position = k1
				// Use the source identifier for ADD COLUMN and the current target
				// identifier for positional clauses when available.
				originalColName := cm.getSourceOriginalColumnName(v1)
				originalLastColumn := cm.getTargetPositionColumnName(lastcolumn)
				repairAttrs := append([]string(nil), cm.sourceColumnMap[v1]...)
				if sms.useCanonicalCompare {
					var repairPlan schemacompat.ColumnRepairPlan
					if sms.isOracleToMySQL {
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
							sms.sourceColumnDefinitions[originalColName],
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
				if sms.isOracleToMySQL && len(repairAttrs) > 3 {
					switch strings.ToUpper(strings.TrimSpace(repairAttrs[3])) {
					case "N":
						repairAttrs[3] = "NO"
					case "Y":
						repairAttrs[3] = "YES"
					}
				}
				// 检查目标表是否存在主键
				if mysqlDataFix, ok := cm.dbf.DataAbnormalFix().(*mysql.MysqlDataAbnormalFixStruct); ok {
					mysqlDataFix.CheckDestTableHasPrimaryKey(stcls.destDB, logThreadSeq)
				}
				addSql := cm.dbf.DataAbnormalFix().FixAlterColumnSqlDispos("add", repairAttrs, position, originalLastColumn, originalColName, logThreadSeq)
				vlog = fmt.Sprintf("(%d) %s Missing column %s in %s.%s - ADD: %v", logThreadSeq, event, originalColName, destSchema, stcls.table, addSql)
				global.Wlog.Warn(vlog)
				sms.alterSlice = append(sms.alterSlice, addSql)
				delete(cm.destColumnMap, v1)
			}
		}
}

// buildCharsetAdvisory 处理 P7 8f：MySQL→MySQL 场景下的字符集/排序规则/表级属性 advisory 检查。
// 返回 charsetAdvisoryResult 包含生成的修复 SQL、advisory SQL 以及各类差异标志。
func (stcls *schemaTable) buildCharsetAdvisory(
	sms *structModeState, cm *columnMetaState, fixer dbExec.DataAbnormalFixInterface,
	sourceSchema, destSchema string,
	logThreadSeq int64, event string,
) *charsetAdvisoryResult {
	var vlog string
	result := &charsetAdvisoryResult{
		sqlS:                   make([]string, 0),
		constraintAdvisorySQLs: make([]string, 0),
	}

	if !stcls.isMySQLToMySQL() {
		return result
	}

	tableAdvisorySuggestions := make([]schemacompat.ConstraintRepairSuggestion, 0)
	columnCollationRepairHandled := len(sms.columnCollationRepairCandidates) == 0

	sourceMeta, errSourceMeta := queryMySQLTableLevelMetadata(stcls.sourceDB, sourceSchema, stcls.table)
	if errSourceMeta != nil {
		vlog = fmt.Sprintf("(%d) %s Failed to query source table metadata for %s.%s: %v", logThreadSeq, event, sourceSchema, stcls.table, errSourceMeta)
		global.Wlog.Error(vlog)
		return result
	}

	destMeta, errDestMeta := queryMySQLTableLevelMetadata(stcls.destDB, destSchema, stcls.destTable)
	if errDestMeta != nil {
		vlog = fmt.Sprintf("(%d) %s Failed to query target table metadata for %s.%s: %v", logThreadSeq, event, destSchema, stcls.destTable, errDestMeta)
		global.Wlog.Error(vlog)
		return result
	}

	sourceMeta.TableComment = normalizeMetadataComment(sourceMeta.TableComment)
	destMeta.TableComment = normalizeMetadataComment(destMeta.TableComment)

	// MariaDB 不支持特性检测
	unsupportedFeatures := schemacompat.DetectMariaDBUnsupportedTableFeatures(sourceMeta.CreateTableSQL, stcls.sourceVersionInfo(), stcls.destVersionInfo())
	if len(unsupportedFeatures) > 0 {
		result.tableUnsupportedRiskDifferent = true
		vlog = fmt.Sprintf("(%d) %s MariaDB unsupported features detected for %s.%s -> %s.%s: %+v",
			logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.destTable, unsupportedFeatures)
		global.Wlog.Warn(vlog)
		result.constraintAdvisorySQLs = append(
			result.constraintAdvisorySQLs,
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
		result.tableCharsetDifferent = true
		vlog = fmt.Sprintf("(%d) %s Table charset mismatch: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCharset, destMeta.TableCharset, charsetDecision.Reason)
		global.Wlog.Warn(vlog)
	} else if charsetDecision.State == schemacompat.CompatibilityNormalizedEqual {
		vlog = fmt.Sprintf("(%d) %s Table charset normalized-equal: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCharset, destMeta.TableCharset, charsetDecision.Reason)
		global.Wlog.Debug(vlog)
	}

	// 检查是否所有列级 collation 差异都属于已知的 MariaDB→MySQL 等价映射
	allColumnCollationMapped := len(sms.columnCollationRepairCandidates) > 0 && len(sms.alterSlice) == 0
	if allColumnCollationMapped {
		for _, c := range sms.columnCollationRepairCandidates {
			mapped, ok := schemacompat.MapMariaDBCollationToMySQL(c.SourceCollation)
			if !ok || !strings.EqualFold(mapped, strings.TrimSpace(c.DestCollation)) {
				allColumnCollationMapped = false
				break
			}
		}
	}

	if allColumnCollationMapped {
		// 所有列级 collation 差异都是已知的跨平台等价映射，无需生成修复 SQL
		result.tableCollationMappedDifferent = true
		columnCollationRepairHandled = true
		vlog = fmt.Sprintf("(%d) %s All %d column collation differences are cross-platform mappings for %s.%s -> %s.%s, no fix SQL needed",
			logThreadSeq, event, len(sms.columnCollationRepairCandidates), sourceSchema, stcls.table, destSchema, stcls.destTable)
		global.Wlog.Warn(vlog)
	} else if repairSQLs, ok := stcls.buildColumnCollationRepairSQL(fixer, sourceMeta, destMeta, sms.sourceColumnDefinitions, sms.columnCollationRepairCandidates, logThreadSeq); ok {
		result.executableColumnCollationRepair = true
		columnCollationRepairHandled = true
		vlog = fmt.Sprintf("(%d) %s Generated executable column collation repair SQL for %s.%s -> %s.%s: %v",
			logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.destTable, repairSQLs)
		global.Wlog.Warn(vlog)
		result.sqlS = append(result.sqlS, repairSQLs...)
	} else if len(sms.columnCollationRepairCandidates) > 0 {
		result.columnRiskDifferent = true
		columnCollationRepairHandled = true
		result.columnAdvisorySuggestions = append(result.columnAdvisorySuggestions, buildColumnCollationAdvisorySuggestions(sms.columnCollationRepairCandidates)...)
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
			result.tableCollationMappedDifferent = true
			vlog = fmt.Sprintf("(%d) %s Table collation-mapped: source=%s maps to target=%s, no fix SQL needed",
				logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation)
			global.Wlog.Warn(vlog)
		} else if result.executableColumnCollationRepair || result.tableCharsetDifferent {
			// 可执行的列级 collation 修复 SQL 或表级 charset 差异修复已包含 CONVERT TO CHARACTER SET，
			// 跳过重复的表级 advisory 输出
			vlog = fmt.Sprintf("(%d) %s Table collation drift already covered by executable column collation repair: source=%s, dest=%s", logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation)
			global.Wlog.Debug(vlog)
		} else {
			result.tableCollationRiskDifferent = true
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
		result.tableCollationDifferent = true
		vlog = fmt.Sprintf("(%d) %s Table collation mismatch: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation, collationDecision.Reason)
		global.Wlog.Warn(vlog)
	} else if collationDecision.State == schemacompat.CompatibilityNormalizedEqual {
		vlog = fmt.Sprintf("(%d) %s Table collation normalized-equal: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation, collationDecision.Reason)
		global.Wlog.Debug(vlog)
	}

	if result.tableCharsetDifferent || result.tableCollationDifferent {
		repairCollation := sourceMeta.TableCollation
		if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(repairCollation); ok {
			repairCollation = mapped
		}
		result.sqlS = append(result.sqlS, fixer.FixTableCharsetSqlGenerate(sourceMeta.TableCharset, repairCollation, logThreadSeq)...)
	}

	rowFormatDecision := schemacompat.DecideTableRowFormatCompatibility(
		schemacompat.CanonicalizeMySQLTableOptions(sourceMeta.RowFormat, sourceMeta.CreateOptions, sourceMeta.TableComment),
		schemacompat.CanonicalizeMySQLTableOptions(destMeta.RowFormat, destMeta.CreateOptions, destMeta.TableComment),
	)
	if rowFormatDecision.IsMismatch() {
		result.tableRowFormatDifferent = true
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
	sourceChecks = schemacompat.FilterPortableCheckConstraints(sourceChecks, stcls.sourceVersionInfo(), stcls.destVersionInfo(), sms.sourceColumnDefinitions)
	destChecks := schemacompat.ExtractCheckConstraintsFromCreateSQL(destMeta.CreateTableSQL)
	checkDecision := schemacompat.DecideCheckConstraintCompatibility(sourceChecks, destChecks, sourceCatalog, destCatalog)
	if checkDecision.IsMismatch() {
		result.tableCheckRiskDifferent = true
		vlog = fmt.Sprintf("(%d) %s Table CHECK constraint risk detected for %s.%s -> %s.%s: %s",
			logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.destTable, checkDecision.Reason)
		global.Wlog.Warn(vlog)
		checkSuggestions := schemacompat.BuildCheckConstraintRepairSuggestions(destSchema, stcls.destTable, sourceChecks, destChecks, checkDecision)
		result.constraintAdvisorySQLs = append(
			result.constraintAdvisorySQLs,
			buildConstraintAdvisoryLines(fmt.Sprintf("%s.%s CHECK constraints", destSchema, stcls.destTable), checkSuggestions)...,
		)
	}

	if advisorySuggestion, needsFix := buildMySQLTableAutoIncrementAdvisory(destSchema, stcls.destTable, sourceMeta.AutoIncrement, destMeta.AutoIncrement); needsFix && !sms.droppedAutoIncrementColumn {
		result.tableAutoIncrementRiskDifferent = true
		vlog = fmt.Sprintf("(%d) %s Table AUTO_INCREMENT drift recorded as advisory-only: source=%v, dest=%v", logThreadSeq, event, nullInt64ForLog(sourceMeta.AutoIncrement), nullInt64ForLog(destMeta.AutoIncrement))
		global.Wlog.Warn(vlog)
		tableAdvisorySuggestions = append(tableAdvisorySuggestions, advisorySuggestion)
	} else if needsFix && sms.droppedAutoIncrementColumn {
		vlog = fmt.Sprintf("(%d) %s Skip table AUTO_INCREMENT repair for %s.%s because the target auto-increment column is being dropped",
			logThreadSeq, event, destSchema, stcls.table)
		global.Wlog.Debug(vlog)
	}

	if sourceMeta.TableComment != destMeta.TableComment {
		result.tableCommentDifferent = true
		escapedComment := escapeMySQLCommentLiteral(sourceMeta.TableComment)
		tableCommentSql := fmt.Sprintf("ALTER TABLE `%s`.`%s` COMMENT = '%s';", destSchema, stcls.destTable, escapedComment)
		vlog = fmt.Sprintf("(%d) %s Table comment mismatch: source='%s', dest='%s', generating fix SQL", logThreadSeq, event, sourceMeta.TableComment, destMeta.TableComment)
		global.Wlog.Warn(vlog)
		result.sqlS = append(result.sqlS, tableCommentSql)
	}

	if len(tableAdvisorySuggestions) > 0 {
		result.constraintAdvisorySQLs = append(
			result.constraintAdvisorySQLs,
			buildConstraintAdvisoryLines(fmt.Sprintf("%s.%s TABLE options", destSchema, stcls.destTable), tableAdvisorySuggestions)...,
		)
	}
	if !columnCollationRepairHandled && len(sms.columnCollationRepairCandidates) > 0 {
		result.columnRiskDifferent = true
		result.columnAdvisorySuggestions = append(result.columnAdvisorySuggestions, buildColumnCollationAdvisorySuggestions(sms.columnCollationRepairCandidates)...)
	}

	return result
}