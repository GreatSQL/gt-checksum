package actions

import (
	"fmt"
	mysql "gt-checksum/MySQL"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
	"strings"
)

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
	hasMyRowIDOffset                bool // 标记目标端存在 my_row_id 导致的列顺序偏移
}

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

func (stcls *schemaTable) dropExcessColumns(
	sms *structModeState, cm *columnMetaState,
	logThreadSeq int64, event, destSchema string,
) {
	vlog := fmt.Sprintf("(%d) %s Columns to remove from target %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, cm.delColumn)
	global.Wlog.Debug(vlog)
	if len(cm.delColumn) == 0 {
		return
	}

	// 计算目标表的总列数（用于判断 my_row_id 列位置）
	totalColumns := len(cm.destColumnSlice)

	// 检查是否将要添加显式主键列（通过 ADD COLUMN ... PRIMARY KEY）
	hasExplicitPrimaryKeyAddition := false
	for _, alterSQL := range sms.alterSlice {
		upperSQL := strings.ToUpper(alterSQL)
		if strings.Contains(upperSQL, "ADD COLUMN") && strings.Contains(upperSQL, "PRIMARY KEY") {
			hasExplicitPrimaryKeyAddition = true
			break
		}
	}

	var colsToDelete []string
	for _, v1 := range cm.delColumn {
		originalColName := cm.getDestOriginalColumnName(v1)

		// 检查是否为符合条件的 my_row_id 列（仅在 MySQL→MySQL 场景下）
		if stcls.isMySQLToMySQL() {
			// 获取列在目标表中的位置
			columnSeq := cm.destColumnSeq[v1]

			// 调用 IsValidMyRowIDColumn 检查
			isValidMyRowID, err := mysql.IsValidMyRowIDColumn(
				stcls.destDB,
				destSchema,
				stcls.table,
				originalColName,
				columnSeq,
				totalColumns,
				stcls.checkRules.RequirePK,
				logThreadSeq,
			)
			if err != nil {
				vlog = fmt.Sprintf("(%d) %s Error checking if %s is valid my_row_id for %s.%s: %v", logThreadSeq, event, originalColName, destSchema, stcls.table, err)
				global.Wlog.Warn(vlog)
			} else if isValidMyRowID {
				// 如果将要添加显式主键，则不跳过删除 my_row_id，因为需要先删除隐式主键
				if hasExplicitPrimaryKeyAddition {
					vlog = fmt.Sprintf("(%d) %s Will DROP my_row_id column %s in %s.%s because explicit PRIMARY KEY will be added", logThreadSeq, event, originalColName, destSchema, stcls.table)
					global.Wlog.Info(vlog)
					// 继续执行删除操作，不跳过
				} else {
					// 是符合条件的 my_row_id 列，且不需要添加显式主键，跳过 DROP 操作
					vlog = fmt.Sprintf("(%d) %s Skipping DROP for valid my_row_id column %s in %s.%s (requirePK=ON)", logThreadSeq, event, originalColName, destSchema, stcls.table)
					global.Wlog.Info(vlog)

					// 注意：不要从 destColumnMap 中删除该列
					// 保留 my_row_id 在 destColumnMap 中，以便后续 reconcileColumnDiffs 能够检测到列顺序偏移
					// 并正确设置 hasMyRowIDOffset 标志，避免生成多余的列位置调整 SQL
					continue
				}
			}
		}

		if hasAutoIncrementColumnAttribute(cm.destColumnMap[v1]) {
			sms.droppedAutoIncrementColumn = true
		}
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

func (stcls *schemaTable) reconcileColumnDiffs(
	sms *structModeState, cm *columnMetaState,
	sourceSchema, destSchema string,
	logThreadSeq int64, event string,
) []string {
	var vlog string

	// 检查目标端是否存在 my_row_id 隐藏主键导致的列顺序偏移
	// 当 requirePK=ON 且目标端有 my_row_id 时，所有普通列的顺序会偏移 +1
	// 此时不应该生成 MODIFY COLUMN 来调整普通列位置，而是应该调整 my_row_id 位置
	hasMyRowIDOffset := false
	if stcls.isMySQLToMySQL() && strings.ToUpper(strings.TrimSpace(stcls.checkRules.RequirePK)) == "ON" {
		// 检查目标端是否存在 my_row_id 列
		// 根据 columnNameCaseSensitive 标志使用正确的列名格式
		myRowIDKey := "my_row_id"
		if !cm.columnNameCaseSensitive {
			myRowIDKey = strings.ToUpper(myRowIDKey)
		}
		if _, exists := cm.destColumnMap[myRowIDKey]; exists {
			// 检查源端是否不存在 my_row_id 列
			if _, sourceHasMyRowID := cm.sourceColumnMap[myRowIDKey]; !sourceHasMyRowID {
				// 目标端有 my_row_id 但源端没有，说明存在列顺序偏移
				hasMyRowIDOffset = true
				sms.hasMyRowIDOffset = true // 设置标志，传递给 generateMyRowIDRepositionSQL
				vlog = fmt.Sprintf("(%d) Detected my_row_id offset: source has no my_row_id, dest has my_row_id, will skip position-only MODIFY statements for regular columns", logThreadSeq)
				global.Wlog.Debug(vlog)
			}
		}
	}

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
					// 检查字段是否是分区字段且目标端不允许 NULL
					// 如果是分区字段且目标端不允许 NULL，则这是合理的约束，不应该生成修复 SQL
					isPartitionKeyColumn := partitionExpressionsReferenceColumn(cm.partitionExpressions, sourceOriginalColName, destOriginalColName)
					if isPartitionKeyColumn && destIsNull == "NO" {
						// 分区字段且目标端不允许 NULL，这是合理的约束
						vlog = fmt.Sprintf("(%d) %s Column %s NULL constraint mismatch is expected for partition key column: source=%s, dest=%s (no fix needed)",
							logThreadSeq, event, repairColumnName, sourceIsNull, destIsNull)
						global.Wlog.Info(vlog)
					} else {
						tableAbnormalBool = true
						vlog = fmt.Sprintf("(%d) %s Column %s NULL constraint mismatch: source=%s, dest=%s",
							logThreadSeq, event, repairColumnName, sourceIsNull, destIsNull)
						global.Wlog.Warn(vlog)
					}
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

				if !hasAutoIncrementPrimaryKeyAdd && !hasMyRowIDOffset && cm.sourceColumnSeq[v1] != cm.destColumnSeq[v1] {
					tableAbnormalBool = true
					vlog = fmt.Sprintf("(%d) %s Column %s sequence mismatch: source=%d, dest=%d",
						logThreadSeq, event, repairColumnName, cm.sourceColumnSeq[v1], cm.destColumnSeq[v1])
					global.Wlog.Warn(vlog)
				} else if hasMyRowIDOffset && cm.sourceColumnSeq[v1] != cm.destColumnSeq[v1] {
					// 如果是因为 my_row_id 导致的列顺序偏移，记录日志但不标记为异常
					vlog = fmt.Sprintf("(%d) %s Column %s sequence mismatch caused by my_row_id offset (source=%d, dest=%d), will be fixed by repositioning my_row_id",
						logThreadSeq, event, repairColumnName, cm.sourceColumnSeq[v1], cm.destColumnSeq[v1])
					global.Wlog.Debug(vlog)
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

	// 在处理完所有列差异后，检查是否需要添加 my_row_id
	if stcls.isMySQLToMySQL() {
		shouldAdd, err := mysql.ShouldAddMyRowID(stcls.destDB, destSchema, stcls.table, stcls.checkRules.RequirePK, logThreadSeq)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Error checking if should add my_row_id for %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, err)
			global.Wlog.Error(vlog)
		} else if shouldAdd {
			// 需要添加 my_row_id 列
			// 获取目标表的最后一列名
			lastColumnName := ""
			lastColumnSeq := len(cm.sourceColumnSlice) - 1
			if lastColumnSeq >= 0 && lastColumnSeq < len(cm.sourceColumnSlice) {
				lastColumnName = cm.sourceColumnSlice[lastColumnSeq]
			}

			// 生成 my_row_id 列定义数组
			myRowIDDef := mysql.GenerateMyRowIDColumnDef()

			// 调用 FixAlterColumnSqlDispos 生成 ALTER TABLE ADD COLUMN 语句
			addSql := cm.dbf.DataAbnormalFix().FixAlterColumnSqlDispos("add", myRowIDDef, lastColumnSeq, lastColumnName, "my_row_id", logThreadSeq)
			vlog = fmt.Sprintf("(%d) %s Adding my_row_id column to %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, addSql)
			global.Wlog.Info(vlog)
			sms.alterSlice = append(sms.alterSlice, addSql)

			// 如果表有分区，需要生成单独的 ADD PRIMARY KEY 语句
			if mysqlFixer, ok := cm.dbf.DataAbnormalFix().(*mysql.MysqlDataAbnormalFixStruct); ok {
				if len(mysqlFixer.PartitionColumns) > 0 {
					pkSql := mysqlFixer.GeneratePartitionTablePrimaryKeySql("my_row_id", logThreadSeq)
					if pkSql != "" {
						vlog = fmt.Sprintf("(%d) %s Adding partition table primary key for %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, pkSql)
						global.Wlog.Info(vlog)
						sms.alterSlice = append(sms.alterSlice, pkSql)
					}
				}
			}
		}
	}

	// 检查是否需要调整 my_row_id 隐式主键的位置
	// 当 requirePK=ON 且目标端存在 my_row_id 隐式主键时，如果需要调整其他列到 my_row_id 前面，
	// 需要生成两条独立的 ALTER TABLE 语句：1) 先设置 VISIBLE 2) 调整位置并设置回 INVISIBLE
	// 这两条语句不能与其他列修复操作合并，必须单独执行
	var myRowIDRepositionSQLs []string
	if stcls.isMySQLToMySQL() && strings.ToUpper(strings.TrimSpace(stcls.checkRules.RequirePK)) == "ON" {
		repositionSQLs, err := stcls.checkAndGenerateMyRowIDRepositionSQL(
			sms, cm, destSchema, logThreadSeq, event,
		)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Error checking my_row_id reposition for %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, err)
			global.Wlog.Error(vlog)
		} else if len(repositionSQLs) > 0 {
			myRowIDRepositionSQLs = repositionSQLs
			vlog = fmt.Sprintf("(%d) %s Generated %d independent my_row_id reposition SQL statements for %s.%s", logThreadSeq, event, len(repositionSQLs), destSchema, stcls.table)
			global.Wlog.Info(vlog)
		}
	}

	return myRowIDRepositionSQLs
}

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

type structRiskEvaluation struct {
	abnormalKey string
	newKey      string
	shouldWriteAdvisory bool
}

func (stcls *schemaTable) evaluateStructRiskAndWriteFixSQL(
	sms *structModeState,
	result *charsetAdvisoryResult,
	sourceSchema, sourceTableName, destSchema string,
	myRowIDRepositionSQLCount int,
	logThreadSeq int64, event string,
) structRiskEvaluation {
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

	eval := structRiskEvaluation{}
	if len(sms.alterSlice) > 0 || hasHardTableLevelDiff || executableColumnCollationRepair || myRowIDRepositionSQLCount > 0 {
		eval.abnormalKey = fmt.Sprintf("%s.%s", destSchema, stcls.table)
	} else if hasWarnOnlyTableLevelDiff {
		stcls.structWarnOnlyDiffsMap[fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)] = true
		eval.newKey = fmt.Sprintf("%s.%s", destSchema, stcls.table)
	} else if hasCollationMappedOnly {
		stcls.structCollationMappedMap[fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)] = true
		eval.newKey = fmt.Sprintf("%s.%s", destSchema, stcls.table)
	} else {
		eval.newKey = fmt.Sprintf("%s.%s", destSchema, stcls.table)
	}

	vlog := fmt.Sprintf("(%d) %s Structure validation completed for %s.%s -> %s.%s", logThreadSeq, event, stcls.schema, stcls.table, destSchema, stcls.table)
	global.Wlog.Debug(vlog)

	eval.shouldWriteAdvisory = len(result.constraintAdvisorySQLs) > 0
	return eval
}

func (stcls *schemaTable) finalizeStructPod(
	sqlS []string,
	constraintAdvisorySQLs []string,
	sourceSchema, sourceTableName, destSchema string,
	logThreadSeq int64, event string,
) error {
	var vlog string
	if len(sqlS) > 0 {
		tableKey := fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)
		stcls.rememberColumnRepairOperations(tableKey, sqlS)
		vlog = fmt.Sprintf("(%d) %s Deferred column/table repair statements for %s.%s until index reconciliation: %v", logThreadSeq, event, destSchema, stcls.table, sqlS)
		global.Wlog.Debug(vlog)
	}
	if len(constraintAdvisorySQLs) > 0 {
		vlog = fmt.Sprintf("(%d) %s Writing advisory-only constraint repair suggestions for %s.%s", logThreadSeq, event, destSchema, stcls.destTable)
		global.Wlog.Debug(vlog)
		if err := stcls.writeAdvisoryFixSql(constraintAdvisorySQLs, logThreadSeq); err != nil {
			return err
		}
	}
	return nil
}