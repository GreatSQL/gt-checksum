package actions

import (
	"database/sql"
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	mysql "gt-checksum/MySQL"
	"gt-checksum/schemacompat"
	"strings"
)

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
			stcls.schema, stcls.table,
		)
		destCanonical := schemacompat.CanonicalizeColumnForComparison(
			destOriginal, destAttrs,
			stcls.destVersionInfo(), stcls.sourceVersionInfo(),
			"", stcls.checkRules.MariaDBJSONTargetType,
			stcls.schema, stcls.table,
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

	// 从分区表达式中提取分区列，用于分区表主键修复
	if len(cm.destPartitionExpressions) > 0 {
		partitionColumns := mysql.ExtractPartitionColumnsFromExpressions(cm.destPartitionExpressions)
		if len(partitionColumns) > 0 {
			// 将分区列信息直接设置到 DataAbnormalFixStruct 上
			cm.dbf.PartitionColumns = partitionColumns
		}
	}

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
