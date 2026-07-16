package actions

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"strings"
)

func sendFixSQL(cc chanFixSQLItem, chunkSeq int64, sql string) {
	if sql != "" {
		cc <- fixSQLItem{ChunkSeq: chunkSeq, SQL: sql}
	}
}

/*
差异数据的二次校验，并生成修复语句
*/
func (sp *SchedulePlan) AbnormalDataDispos(diffQueryData chanDiffDataS, cc chanFixSQLItem, logThreadSeq int64) {
	var (
		vlog             string
		aa               = &CheckSumTypeStruct{}
		curry            = make(chanStruct, sp.concurrency)
		totalInsertCount int64 // 全局INSERT语句计数器
	)
	isUniqueIndex := strings.HasPrefix(sp.indexColumnType, "pri_") || strings.HasPrefix(sp.indexColumnType, "uni_")
	// For unique/primary indexed compare flow, chunk ranges are non-overlapping in practice.
	// Keep global PK dedupe only for non-unique flows to reduce large hash-map residency.
	useGlobalKeyDedupe := !isUniqueIndex

	// 在处理前清空所有全局去重映射，确保每次运行都有干净的状态
	deleteMutex.Lock()
	deletePrimaryKeys = make(map[uint64]struct{})
	deleteMutex.Unlock()

	insertMutex.Lock()
	insertedPrimaryKeys = make(map[uint64]struct{}) // 关键修复：清空INSERT主键跟踪映射
	insertMutex.Unlock()
	vlog = fmt.Sprintf("(%d) Processing differences and generating repair statements for %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
	logStageMemory("diff-compare-start", logThreadSeq, sp.schema, sp.table)

	for {
		select {
		case c, ok := <-diffQueryData:
			if !ok {
				if len(curry) == 0 {
					logStageMemory("diff-compare-end", logThreadSeq, sp.schema, sp.table)
					global.Wlog.Debugf("DEBUG_FINAL_COUNT_%d: Total INSERT statements generated for %s.%s: %d\n",
						logThreadSeq, sp.schema, sp.table, totalInsertCount)
					vlog = fmt.Sprintf("(%d) Completed difference processing and repair statements for %s.%s", logThreadSeq, sp.schema, sp.table)
					global.Wlog.Info(vlog)
					close(cc)
					// 关闭回滚 SQL channel，让 RollbackDispos goroutine 正常退出
					if sp.rollCC != nil {
						close(sp.rollCC)
						sp.rollCC = nil
					}
					return
				}
			} else {
				sdb := sp.sdbPool.Get(logThreadSeq)
				ddb := sp.ddbPool.Get(logThreadSeq)
				curry <- struct{}{}
				go func(c1 DifferencesDataStruct, sdb, ddb *sql.DB) {
					defer func() {
						cc <- fixSQLItem{ChunkSeq: c1.ChunkSeq, Done: true}
						<-curry
						sp.sdbPool.Put(sdb, logThreadSeq)
						sp.ddbPool.Put(ddb, logThreadSeq)
					}()
					// 使用映射后的源端和目标端schema和table
					sourceSchema := sp.sourceSchema
					destSchema := sp.destSchema
					table := sp.table

					// 获取列数据时使用原始schema.table组合
					colData := sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sourceSchema, table)]

					// 处理源端SQL条件，确保使用正确的源端数据范围
					var sourceSqlWhere string

					// 修复：使用分批查询逻辑，避免全表查询导致内存消耗过大
					// 基于现有的WHERE条件进行查询，这些条件已经由recursiveIndexColumn正确分片
					var destSqlWhere string // 在更外层声明变量
					// 使用原始的WHERE条件，这些条件已经按照chunkSize正确分片
					sourceSqlWhere = c1.SqlWhere["src"]
					destSqlWhere = c1.SqlWhere["dst"]

					// 确保使用正确的schema
					if strings.Contains(sourceSqlWhere, fmt.Sprintf("`%s`", destSchema)) {
						sourceSqlWhere = strings.Replace(sourceSqlWhere,
							fmt.Sprintf("`%s`", destSchema),
							fmt.Sprintf("`%s`", sourceSchema), -1)
					}
					if strings.Contains(sourceSqlWhere, fmt.Sprintf("%s.", destSchema)) {
						sourceSqlWhere = strings.Replace(sourceSqlWhere,
							fmt.Sprintf("%s.", destSchema),
							fmt.Sprintf("%s.", sourceSchema), -1)
					}

					// 处理目标端SQL条件，确保使用目标端schema
					if strings.Contains(destSqlWhere, fmt.Sprintf("`%s`", sourceSchema)) {
						destSqlWhere = strings.Replace(destSqlWhere,
							fmt.Sprintf("`%s`", sourceSchema),
							fmt.Sprintf("`%s`", destSchema), -1)
					}
					if strings.Contains(destSqlWhere, fmt.Sprintf("%s.", sourceSchema)) {
						destSqlWhere = strings.Replace(destSqlWhere,
							fmt.Sprintf("%s.", sourceSchema),
							fmt.Sprintf("%s.", destSchema), -1)
					}

					// 重要修复：添加去重逻辑，防止分片数据重复处理
					// 每个WHERE条件应该是独立的，不应该有重叠
					vlog = fmt.Sprintf("(%d) Using chunked query - Source: %s, Target: %s", logThreadSeq, sourceSqlWhere, destSqlWhere)
					global.Wlog.Debug(vlog)

					// Log for debugging
					vlog = fmt.Sprintf("(%d) AbnormalDataDispos - Source SQL condition: %s", logThreadSeq, sourceSqlWhere)
					global.Wlog.Debug(vlog)
					vlog = fmt.Sprintf("(%d) AbnormalDataDispos - Target SQL condition: %s", logThreadSeq, destSqlWhere)
					global.Wlog.Debug(vlog)

					// 源端查询使用sourceSchema和table
					var (
						stt, dtt string
						err      error
					)
					idxc := dbExec.IndexColumnStruct{
						Schema:                  sourceSchema,
						Table:                   table,
						TableColumn:             colData.SColumnInfo,
						Drivce:                  sp.sdrive,
						CaseSensitiveObjectName: sp.caseSensitiveObjectName,
						Sqlwhere:                sourceSqlWhere, // 使用处理后的源端SQL条件
						ColumnName:              sp.columnName,
						CompareColumns:          sp.columnPlanSourceCols,
					}
					stt, err = idxc.TableIndexColumn().GeneratingQueryCriteria(sdb, logThreadSeq)
					if err != nil {
						global.Wlog.Warn(fmt.Sprintf("(%d) failed to query source chunk by criteria for %s.%s, fallback to raw SQL query, err=%v", logThreadSeq, sourceSchema, table, err))
						fallbackSourceSQL := strings.TrimSpace(c1.SqlWhere["src"])
						if strings.HasPrefix(strings.ToUpper(fallbackSourceSQL), "SELECT") {
							sourceRows, fallbackErr := queryRowsDataBySQL(sdb, fallbackSourceSQL, sp.sdrive, logThreadSeq)
							if fallbackErr != nil {
								global.Wlog.Error(fmt.Sprintf("(%d) source fallback query failed for %s.%s, mark table as diff, err=%v", logThreadSeq, sourceSchema, table, fallbackErr))
								lock.Lock()
								if sp.pods != nil {
									sp.pods.DIFFS = "yes"
								}
								lock.Unlock()
								return
							}
							stt = strings.Join(sourceRows, "/*go actions rowData*/")
						} else {
							global.Wlog.Error(fmt.Sprintf("(%d) source fallback SQL is unavailable for %s.%s, mark table as diff", logThreadSeq, sourceSchema, table))
							lock.Lock()
							if sp.pods != nil {
								sp.pods.DIFFS = "yes"
							}
							lock.Unlock()
							return
						}
					}

					// 目标端查询使用destSchema和table
					destTable := sp.getDestTableName()
					idxcDest := dbExec.IndexColumnStruct{
						Schema:                  destSchema,
						Table:                   destTable,
						TableColumn:             colData.DColumnInfo,
						Drivce:                  sp.ddrive,
						CaseSensitiveObjectName: sp.caseSensitiveObjectName,
						Sqlwhere:                destSqlWhere, // 使用处理后的目标端SQL条件
						ColumnName:              sp.columnName,
						CompareColumns:          sp.columnPlanTargetCols,
					}
					dtt, err = idxcDest.TableIndexColumn().GeneratingQueryCriteria(ddb, logThreadSeq)
					if err != nil {
						global.Wlog.Warn(fmt.Sprintf("(%d) failed to query dest chunk by criteria for %s.%s, fallback to raw SQL query, err=%v", logThreadSeq, destSchema, destTable, err))
						fallbackDestSQL := strings.TrimSpace(c1.SqlWhere["dst"])
						if strings.HasPrefix(strings.ToUpper(fallbackDestSQL), "SELECT") {
							destRows, fallbackErr := queryRowsDataBySQL(ddb, fallbackDestSQL, sp.ddrive, logThreadSeq)
							if fallbackErr != nil {
								global.Wlog.Error(fmt.Sprintf("(%d) dest fallback query failed for %s.%s, mark table as diff, err=%v", logThreadSeq, destSchema, destTable, fallbackErr))
								lock.Lock()
								if sp.pods != nil {
									sp.pods.DIFFS = "yes"
								}
								lock.Unlock()
								return
							}
							dtt = strings.Join(destRows, "/*go actions rowData*/")
						} else {
							global.Wlog.Error(fmt.Sprintf("(%d) dest fallback SQL is unavailable for %s.%s, mark table as diff", logThreadSeq, destSchema, destTable))
							lock.Lock()
							if sp.pods != nil {
								sp.pods.DIFFS = "yes"
							}
							lock.Unlock()
							return
						}
					}

					if aa.CheckHash(stt) != aa.CheckHash(dtt) {
						vlog = fmt.Sprintf("(%d) Data checksum mismatch for %s.%s, need to find specific differences", logThreadSeq, c1.Schema, c1.Table)
						global.Wlog.Debug(vlog)
						waitForMemoryBudget(0.92)

						// 重要优化：精确比较数据，只找出真正需要修复的记录
						// 1. 将源端和目标端数据转换为切片
						sourceData := strings.Split(stt, "/*go actions rowData*/")
						destData := strings.Split(dtt, "/*go actions rowData*/")

						// 2. 使用优化的Arrcmp实现，只返回真正需要修复的记录
						// 先清理空记录，保留重复记录（不进行去重）
						cleanSourceData := make([]string, 0, len(sourceData))
						cleanDestData := make([]string, 0, len(destData))

						for _, data := range sourceData {
							// 只检查是否为空记录，不使用TrimSpace，保留原始数据中的空格
							if data != "" && data != "/*go actions rowData*/" {
								cleanSourceData = append(cleanSourceData, data)
							}
						}

						for _, data := range destData {
							// 只检查是否为空记录，不使用TrimSpace，保留原始数据中的空格
							if data != "" && data != "/*go actions rowData*/" {
								cleanDestData = append(cleanDestData, data)
							}
						}

						// 3. 记录去重前后的数据量
						vlog = fmt.Sprintf("(%d) Data deduplication - Source: %d->%d, Dest: %d->%d for %s.%s",
							logThreadSeq, len(sourceData), len(cleanSourceData), len(destData), len(cleanDestData), c1.Schema, c1.Table)
						global.Wlog.Debug(vlog)

						// 避免在大差异场景输出大文本日志，防止日志构造额外放大内存占用
						if len(cleanSourceData) > 0 {
							global.Wlog.Debugf("DEBUG_SOURCE_DATA_%d: sourceRecords=%d (sample suppressed)", logThreadSeq, len(cleanSourceData))
						}

						// 检查去重是否真的有效
						// 只有当源数据确实有内容时，才检查重复记录
						if len(sourceData) != len(cleanSourceData) {
							// 检查是否只有一个空字符串（源表为空的情况）
							if len(sourceData) == 1 && sourceData[0] == "" {
								// 源表为空，不是真正的重复记录
								global.Wlog.Debugf("(%d) Source data is empty, skipping duplicate check for %s.%s", logThreadSeq, c1.Schema, c1.Table)
							} else {
								duplicateCount := len(sourceData) - len(cleanSourceData)
								vlog = fmt.Sprintf("(%d) Found %d duplicate records in source data for %s.%s", logThreadSeq, duplicateCount, c1.Schema, c1.Table)
								global.Wlog.Warn(vlog)
							}
						}

						if len(destData) != len(cleanDestData) {
							// 检查是否只有一个空字符串（目标表为空的情况）
							if len(destData) == 1 && destData[0] == "" {
								// 目标表为空，不是真正的重复记录
								global.Wlog.Debugf("(%d) Destination table %s.%s is empty, skipping duplicate check", logThreadSeq, c1.Schema, c1.Table)

								// 每个表只输出一次目标表为空的提示
								tableKey := fmt.Sprintf("%s.%s", c1.Schema, c1.Table)
								emptyTableMutex.Lock()
								if !emptyTableWarned[tableKey] {
									// 输出目标表为空的提示
									vlog = fmt.Sprintf("(%d) Destination table %s.%s is empty, all source records will be added", logThreadSeq, c1.Schema, c1.Table)
									global.Wlog.Warn(vlog)
									// 标记该表已输出提示
									emptyTableWarned[tableKey] = true
								}
								emptyTableMutex.Unlock()
							} else {
								duplicateCount := len(destData) - len(cleanDestData)
								vlog = fmt.Sprintf("(%d) Found %d duplicate records in dest data for %s.%s", logThreadSeq, duplicateCount, c1.Schema, c1.Table)
								global.Wlog.Warn(vlog)
							}
						}

						// 4. 使用Arrcmp进行精确比较
						// columns 模式下，查询结果已按 PK+compareColumns 顺序裁剪，必须使用过滤后的列元信息，
						// 否则 buildFloatComparisonScales / buildTemporalCompareKinds 产生的位置索引与实际列位置不符。
						effectiveSrcCols := colData.SColumnInfo
						effectiveDstCols := colData.DColumnInfo
						if len(sp.columnPlanSourceCols) > 0 {
							effectiveSrcCols = columnsModeFilteredCols(colData.SColumnInfo, sp.columnPlanSourceCols, sp.columnName)
							effectiveDstCols = columnsModeFilteredCols(colData.DColumnInfo, sp.columnPlanTargetCols, sp.columnName)
						}

						// columns 模式下，PK 列不能参与归一化。
						// columnsModeExtractPKKey() 从 Arrcmp 返回的行（即归一化后的行）中提取 PK key；
						// 若 PK 列是 TIMESTAMP(6)/TIME(6)/FLOAT 等类型，归一化会截断精度，导致原本不同
						// 的两行映射到同一 PK key，进而错误地触发 two-sided UPDATE 或吞掉真实差异。
						// 修复方案：提前计算 PK 列在过滤后列列表中的位置，然后将这些位置从
						// float/temporal 归一化向量中屏蔽（float: -1，temporal: ""），确保 PK 值
						// 在 Arrcmp 前后保持原始精度，而仅对 compare 列做归一化。
						var earlyPKPositions []int
						if len(sp.columnPlanSourceCols) > 0 {
							earlyPKPositions, _ = columnsModeSplitPKAndCompare(effectiveSrcCols, sp.columnName)
						}

						// 归一化前保存目标端原始行快照：归一化仅用于比对，
						// DELETE WHERE 子句必须使用 MySQL 实际存储值，否则无法命中目标行。
						origCleanDestData := append([]string(nil), cleanDestData...)
						anyNormApplied := false

						floatCompareScales := buildFloatComparisonScales(effectiveSrcCols, effectiveDstCols)
						for _, pos := range earlyPKPositions {
							if pos < len(floatCompareScales) {
								floatCompareScales[pos] = -1 // 跳过 PK 列 float 归一化
							}
						}
						if len(floatCompareScales) > 0 {
							cleanSourceData = normalizeRowsForFloatComparison(cleanSourceData, floatCompareScales)
							cleanDestData = normalizeRowsForFloatComparison(cleanDestData, floatCompareScales)
							anyNormApplied = true
							global.Wlog.Debugf("(%d) Applied float normalization for %s.%s before Arrcmp", logThreadSeq, c1.Schema, c1.Table)
						}
						temporalCompareKinds := buildTemporalCompareKinds(effectiveSrcCols, effectiveDstCols)
						for _, pos := range earlyPKPositions {
							if pos < len(temporalCompareKinds) {
								temporalCompareKinds[pos] = "" // 跳过 PK 列时间归一化
							}
						}
						if len(temporalCompareKinds) > 0 {
							cleanSourceData = normalizeRowsForTemporalComparison(cleanSourceData, temporalCompareKinds)
							cleanDestData = normalizeRowsForTemporalComparison(cleanDestData, temporalCompareKinds)
							anyNormApplied = true
							global.Wlog.Debugf("(%d) Applied temporal normalization for %s.%s before Arrcmp", logThreadSeq, c1.Schema, c1.Table)
						}
						charTrimFlags := buildCharTrimFlags(effectiveSrcCols)
						if len(charTrimFlags) > 0 {
							cleanSourceData = normalizeRowsForCharComparison(cleanSourceData, charTrimFlags)
							global.Wlog.Debugf("(%d) Applied CHAR trailing-space normalization for %s.%s before Arrcmp", logThreadSeq, c1.Schema, c1.Table)
						}

						if len(cleanSourceData) == len(cleanDestData) &&
							hashRowsIgnoringOrder(cleanSourceData) == hashRowsIgnoringOrder(cleanDestData) {
							// Normalization made both row multisets equal, so this chunk requires no fix DML.
							global.Wlog.Debugf("(%d) Normalized row sets are equal for %s.%s, skip diff DML generation",
								logThreadSeq, c1.Schema, c1.Table)
							return
						}
						add, del := aa.Arrcmp(cleanSourceData, cleanDestData)
						if len(add) > 0 && len(del) > 0 && len(temporalCompareKinds) > 0 {
							var healed int
							add, del, healed = reconcileTemporalNullArtifacts(add, del, temporalCompareKinds, effectiveSrcCols, effectiveDstCols)
							if healed > 0 {
								global.Wlog.Warnf("(%d) Reconciled %d temporal null artifacts for %s.%s (INTERVAL/TIME scan compatibility)",
									logThreadSeq, healed, c1.Schema, c1.Table)
							}
						}
						// 将 del 中经归一化处理后的行替换回目标端原始存储值。
						// 归一化（float/temporal）使 Arrcmp 能正确识别语义等价行，但修复 SQL
						// 的 DELETE WHERE 条件必须使用 MySQL 实际存储值才能精确命中目标行。
						if anyNormApplied && len(del) > 0 {
							del = remapDelToOriginalDest(del, cleanDestData, origCleanDestData)
						}
						stt, dtt = "", ""

						// 5. 记录发现的差异数量
						vlog = fmt.Sprintf("CHUNK_AUDIT: source=%d dest=%d add=%d del=%d table=%s.%s where=%s",
							len(cleanSourceData), len(cleanDestData), len(add), len(del), c1.Schema, c1.Table, sourceSqlWhere)
						global.Wlog.Debug(vlog)

						// 添加调试信息：检查差异数量的合理性
						expectedAddCount := len(cleanSourceData) - len(cleanDestData)
						if len(cleanDestData) == 0 {
							global.Wlog.Debugf("DEBUG_DIFF_ANALYSIS_%d: Expected add count: %d (source=%d, dest=0), Actual add count: %d\n",
								logThreadSeq, len(cleanSourceData), len(cleanSourceData), len(add))
						} else {
							global.Wlog.Debugf("DEBUG_DIFF_ANALYSIS_%d: Expected add count: %d (source=%d, dest=%d), Actual add count: %d\n",
								logThreadSeq, expectedAddCount, len(cleanSourceData), len(cleanDestData), len(add))
						}

						if len(add) > expectedAddCount+10 {
							global.Wlog.Debugf("DEBUG_ADD_DATA_%d: addCount=%d expected=%d (sample suppressed)",
								logThreadSeq, len(add), expectedAddCount)
						}
						if len(cleanSourceData) == 1 && len(cleanDestData) == 1 && len(add) == 1 && len(del) == 1 {
							global.Wlog.Warnf("ROW_COMPARE_SAMPLE_%d table=%s.%s sourceRow=%q destRow=%q addRow=%q delRow=%q",
								logThreadSeq, c1.Schema, c1.Table, cleanSourceData[0], cleanDestData[0], add[0], del[0])
						}

						// 6. 比较记录数量差异的日志记录
						// Arrcmp已经完成了精确的集合差异计算，不再对add数组进行截断
						if len(del) == 1 && len(add) > 100 {
							vlog = fmt.Sprintf("(%d) Note: 1 record to delete and %d to add for %s.%s (this is expected for large data differences)", logThreadSeq, len(add), c1.Schema, c1.Table)
							global.Wlog.Debug(vlog)
						}
						if len(del) > 0 || len(add) > 0 {
							// 确保使用正确的源和目标schema
							sourceSchema := sp.sourceSchema
							destSchema := sp.destSchema
							if sourceSchema == "" {
								sourceSchema = c1.Schema
							}
							if destSchema == "" {
								destSchema = c1.Schema
							}

							// 添加对空IndexColumn的检查
							indexColumns := sp.columnName
							if len(indexColumns) == 0 {
								// 如果没有索引列，使用所有列作为条件
								indexColumns = make([]string, 0, len(colData.DColumnInfo))
								for _, colInfo := range colData.DColumnInfo {
									if colName, ok := colInfo["columnName"]; ok {
										indexColumns = append(indexColumns, colName)
									}
								}
							}

							// 处理源端和目标端SQL条件
							// 获取原始SQL条件
							originalSourceSqlWhere := c1.SqlWhere["src"]
							originalDestSqlWhere := c1.SqlWhere["dst"]

							// 处理源端SQL条件，确保使用源端schema
							sourceSqlWhere := originalSourceSqlWhere
							// 如果源端SQL条件中包含目标端schema，替换为源端schema
							if strings.Contains(sourceSqlWhere, fmt.Sprintf("`%s`", destSchema)) {
								sourceSqlWhere = strings.Replace(sourceSqlWhere,
									fmt.Sprintf("`%s`", destSchema),
									fmt.Sprintf("`%s`", sourceSchema), -1)
							}
							if strings.Contains(sourceSqlWhere, fmt.Sprintf("%s.", destSchema)) {
								sourceSqlWhere = strings.Replace(sourceSqlWhere,
									fmt.Sprintf("%s.", destSchema),
									fmt.Sprintf("%s.", sourceSchema), -1)
							}

							// 处理目标端SQL条件，确保使用目标端schema
							destSqlWhere := originalDestSqlWhere
							// 如果目标端SQL条件中包含源端schema，替换为目标端schema
							if strings.Contains(destSqlWhere, fmt.Sprintf("`%s`", sourceSchema)) {
								destSqlWhere = strings.Replace(destSqlWhere,
									fmt.Sprintf("`%s`", sourceSchema),
									fmt.Sprintf("`%s`", destSchema), -1)
							}
							if strings.Contains(destSqlWhere, fmt.Sprintf("%s.", sourceSchema)) {
								destSqlWhere = strings.Replace(destSqlWhere,
									fmt.Sprintf("%s.", sourceSchema),
									fmt.Sprintf("%s.", destSchema), -1)
							}

							// Log for debugging
							vlog = fmt.Sprintf("(%d) DataFixSql - Source SQL condition: %s", logThreadSeq, sourceSqlWhere)
							global.Wlog.Debug(vlog)
							vlog = fmt.Sprintf("(%d) DataFixSql - Target SQL condition: %s", logThreadSeq, destSqlWhere)
							global.Wlog.Debug(vlog)

							// 修复SQL生成时使用正确的schema映射
							dbf := dbExec.DataAbnormalFixStruct{
								Schema:                  destSchema,   // 目标schema
								SourceSchema:            sourceSchema, // 源端schema，用于处理数据库映射关系
								Table:                   destTable,    // 使用目标端表名
								ColData:                 colData.DColumnInfo,
								Sqlwhere:                destSqlWhere, // 使用处理后的目标端SQL条件
								DestDevice:              sp.ddrive,
								IndexColumn:             indexColumns,
								DatafixType:             sp.datafixType,
								CaseSensitiveObjectName: sp.caseSensitiveObjectName,
							}
							if strings.HasPrefix(c1.indexColumnType, "pri") {
								dbf.IndexType = "pri"
							} else if strings.HasPrefix(c1.indexColumnType, "uni") {
								dbf.IndexType = "uni"
							} else {
								dbf.IndexType = "mul"
							}

							// columns 模式：按行 key 状态路由（two-sided / source-only / target-only）
							if len(sp.columnPlanSourceCols) > 0 {
								// 构建过滤后列列表（PK ∪ compareColumns，保持原表列顺序）
								filteredSrcCols := columnsModeFilteredCols(colData.SColumnInfo, sp.columnPlanSourceCols, indexColumns)
								filteredDstCols := columnsModeFilteredCols(colData.DColumnInfo, sp.columnPlanTargetCols, indexColumns)

								// 找出 PK 列在过滤后列列表中的位置，并拆出 compare 列。
								// 这里必须大小写不敏感，否则会与 orderColumnsForCompare /
								// FixUpdateSqlExec 的归一化语义不一致，导致 two-sided 配对错误。
								pkPositions, compareColNames := columnsModeSplitPKAndCompare(filteredSrcCols, indexColumns)

								// 构建源→目标列名映射，用于生成 UPDATE SET 子句时使用目标端列名。
								// 键统一小写，与 FixUpdateSqlExec 中的查找逻辑保持大小写不敏感一致。
								var srcToDstCol map[string]string
								if !sp.columnPlanSimpleMode && len(sp.columnPlanSourceCols) > 0 {
									srcToDstCol = make(map[string]string, len(sp.columnPlanSourceCols))
									for i, src := range sp.columnPlanSourceCols {
										if i < len(sp.columnPlanTargetCols) {
											srcToDstCol[strings.ToLower(src)] = sp.columnPlanTargetCols[i]
										}
									}
								}

								// 按 PK key 索引 add / del 行
								addByPK := make(map[string]string, len(add))
								for _, row := range add {
									addByPK[columnsModeExtractPKKey(row, pkPositions)] = row
								}
								delByPK := make(map[string]string, len(del))
								for _, row := range del {
									delByPK[columnsModeExtractPKKey(row, pkPositions)] = row
								}

								// Two-sided 行：生成 UPDATE；source-only 行：仅通知
								var chunkSrcOnly, chunkDstOnly int
								for pkKey, srcRow := range addByPK {
									if _, exists := delByPK[pkKey]; exists {
										// two-sided：source 与 target 的 compare 列有差异 → UPDATE
										if sp.datafixType == "no" {
											// compare-only 模式：只记录差异，不生成修复 SQL（Oracle 等不支持 UPDATE 的目标端在此路径安全退出）
											lock.Lock()
											if sp.pods != nil {
												sp.pods.DIFFS = "yes"
											}
											lock.Unlock()
										} else {
											dbf.RowData = srcRow
											dbf.ColData = filteredSrcCols
											sqlstr, err := dbf.DataAbnormalFix().FixUpdateSqlExec(ddb, srcRow, compareColNames, srcToDstCol, logThreadSeq)
											if err != nil {
												sp.getErr(fmt.Sprintf("dest: checksum table %s.%s generate UPDATE sql error (columns-mode).", c1.Schema, c1.Table), err)
											} else if sqlstr != "" {
												sendFixSQL(cc, c1.ChunkSeq, sqlstr)
												// 为 UPDATE fix 生成回滚：DELETE 新行 + INSERT 旧行
												if sp.rollCC != nil {
													oldDstRow := delByPK[pkKey]
													if rbDel := rollbackRowToDelete(destSchema, destTable, srcRow, filteredSrcCols, sp.columnName); rbDel != "" {
														sendRollback(sp.rollCC, rbDel)
													}
													if rbIns := rollbackRowToInsert(destSchema, destTable, oldDstRow, filteredDstCols); rbIns != "" {
														sendRollback(sp.rollCC, rbIns)
													}
												}
											}
										}
									} else {
										// source-only：target 中不存在此 key，在 columns 模式下不生成 INSERT，仅计数
										lock.Lock()
										if sp.pods != nil {
											sp.pods.DIFFS = "yes"
										}
										lock.Unlock()
										chunkSrcOnly++
										// 计数，用于后续生成 advisory 提示文件
										if sp.sourceOnlyAdvisory != nil {
											sp.sourceOnlyAdvisory.mu.Lock()
											sp.sourceOnlyAdvisory.sourceOnlyCount++
											sp.sourceOnlyAdvisory.mu.Unlock()
										}
									}
								}

								// Target-only 行：由 extraRowsSyncToSource 控制
								for pkKey, dstRow := range delByPK {
									if _, exists := addByPK[pkKey]; !exists {
										if sp.extraRowsSyncToSource == "ON" {
											dbf.RowData = dstRow
											dbf.ColData = filteredDstCols
											sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
											if err != nil {
												sp.getErr(fmt.Sprintf("dest: checksum table %s.%s generate DELETE sql error (columns-mode target-only).", c1.Schema, c1.Table), err)
											} else if sqlstr != "" {
												sendFixSQL(cc, c1.ChunkSeq, sqlstr)
												// 为 columns-mode target-only DELETE 生成回滚 INSERT
												if sp.rollCC != nil {
													if rbSql := rollbackRowToInsert(destSchema, destTable, dstRow, filteredDstCols); rbSql != "" {
														sendRollback(sp.rollCC, rbSql)
													}
												}
											}
										} else {
											// 不生成 DELETE，但仍标记为差异，确保 Diffs=yes
											lock.Lock()
											if sp.pods != nil {
												sp.pods.DIFFS = "yes"
											}
											lock.Unlock()
											chunkDstOnly++
											// 计数，用于后续生成 advisory 提示文件
											if sp.sourceOnlyAdvisory != nil {
												sp.sourceOnlyAdvisory.mu.Lock()
												sp.sourceOnlyAdvisory.targetOnlyCount++
												sp.sourceOnlyAdvisory.mu.Unlock()
											}
										}
									}
								}
								// 每个 chunk 汇总一次，避免大差异时产生大量逐行日志
								if chunkSrcOnly > 0 || chunkDstOnly > 0 {
									global.Wlog.Warn(fmt.Sprintf("(%d) [columns-mode] %s.%s chunk diff: %d source-only row(s) skipped (no INSERT generated), %d target-only row(s) skipped (set extraRowsSyncToSource=ON to generate DELETE)",
										logThreadSeq, c1.Schema, c1.Table, chunkSrcOnly, chunkDstOnly))
								}
								return // columns 模式路由完成，跳过常规 INSERT/DELETE 生成
							}

							// 关键修复：确保DELETE语句一定在INSERT语句之前生成
							// 先处理所有DELETE语句
							if len(del) > 0 {
								vlog = fmt.Sprintf("(%d) Generating DELETE statements for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
								global.Wlog.Debugf("DEBUG_SQL_ORDER_%d: Processing %d DELETE statements first for %s.%s\n",
									logThreadSeq, len(del), c1.Schema, c1.Table)

								deleteSqlSize := sp.deleteSqlSize

								// 分组处理DELETE语句，每fixTrxNum条合并一次
								for batchStart := 0; batchStart < len(del); batchStart += sp.fixTrxNum {
									batchEnd := batchStart + sp.fixTrxNum
									if batchEnd > len(del) {
										batchEnd = len(del)
									}
									batchDel := del[batchStart:batchEnd]

									// 处理单字段主键和多字段联合主键的批量DELETE
									var primaryCols []string
									var isSinglePrimary bool
									var primaryCol string
									if len(dbf.IndexColumn) > 0 {
										primaryCols = dbf.IndexColumn // 获取所有主键列
										isSinglePrimary = len(primaryCols) == 1
										if isSinglePrimary {
											primaryCol = primaryCols[0] // 使用唯一的主键列
										}
									}

									// 对于MySQL，合并DELETE语句
									if sp.ddrive == "mysql" {
										// 只有当IndexType为pri或uni时，才使用主键合并逻辑
										if len(dbf.IndexColumn) > 0 && (dbf.IndexType == "pri" || dbf.IndexType == "uni") {

											// 收集所有DELETE语句的主键值，并进行去重
											var primaryValues []string
											processedPrimaryValues := make(map[string]struct{}) // 局部去重，避免同一批次内重复
											for _, i := range batchDel {
												dbf.RowData = i
												sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
												if err != nil {
													sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
													continue
												}

												// 为 DELETE fix 生成回滚 INSERT
												if sp.rollCC != nil {
													if rbSql := rollbackRowToInsert(destSchema, destTable, i, dbf.ColData); rbSql != "" {
														sendRollback(sp.rollCC, rbSql)
													}
												}

												// 提取WHERE条件中的值
												if strings.Contains(sqlstr, "WHERE") {
													wherePart := strings.Split(sqlstr, "WHERE")[1]
													wherePart = strings.TrimSpace(strings.TrimSuffix(wherePart, ";"))

													var primaryKey string
													var primaryValue string

													if isSinglePrimary {
														// 单字段主键：提取单个值
														key := fmt.Sprintf("`%s` = '", primaryCol)
														if strings.Contains(wherePart, key) {
															part := strings.Split(wherePart, key)[1]
															if strings.Contains(part, "'") {
																value := strings.Split(part, "'")[0]
																primaryValue = "'" + value + "'"
																primaryKey = fmt.Sprintf("%s.%s.%s:%s", c1.Schema, c1.Table, primaryCol, value)
															}
														}
													} else {
														// 多字段联合主键：提取所有主键值组合
														var valueList []string
														var keyList []string
														foundAllValues := true
														for _, col := range primaryCols {
															// 构建匹配模式：`col` = 'value'
															pattern := fmt.Sprintf("`%s` = '", col)
															index := strings.Index(wherePart, pattern)
															if index == -1 {
																foundAllValues = false
																break
															}
															// 提取值
															afterPattern := wherePart[index+len(pattern):]
															valueEnd := strings.Index(afterPattern, "'")
															if valueEnd == -1 {
																foundAllValues = false
																break
															}
															value := afterPattern[:valueEnd]
															valueList = append(valueList, "'"+value+"'")
															keyList = append(keyList, fmt.Sprintf("%s:%s", col, value))
															// 从剩余字符串中查找下一个主键条件
															wherePart = afterPattern[valueEnd+1:]
														}
														if foundAllValues {
															// 构建值组合字符串：('val1', 'val2', 'val3')
															primaryValue = "(" + strings.Join(valueList, ", ") + ")"
															// 构建唯一键：schema.table.col1:val1,col2:val2
															primaryKey = fmt.Sprintf("%s.%s.%s", c1.Schema, c1.Table, strings.Join(keyList, ","))
														}
													}

													// 检查该主键值是否已经处理过（全局去重）
													if primaryKey != "" {
														exists := hasDeleteKey(primaryKey, useGlobalKeyDedupe)

														// 同时检查局部去重，避免同一批次内重复
														_, localExists := processedPrimaryValues[primaryKey]

														// 关键修复：检查该主键是否已经被INSERT过
														inserted := hasInsertKey(primaryKey, useGlobalKeyDedupe)

														// 如果该主键已经被INSERT过，或者已经被DELETE过，或者在本批次内重复，则跳过
														if !exists && !localExists && !inserted {
															// 添加到全局去重map
															markDeleteKeyIfAbsent(primaryKey, useGlobalKeyDedupe)

															// 添加到局部去重map
															processedPrimaryValues[primaryKey] = struct{}{}

															// 添加到主键值列表
															primaryValues = append(primaryValues, primaryValue)
														}
													}
												}
											}

											// 如果成功提取了多个值，根据长度限制生成合并的DELETE语句
											if len(primaryValues) > 0 {
												// 生成基础SQL部分
												var baseSql string
												if isSinglePrimary {
													// 单字段主键：WHERE `col` IN (
													// 使用目标schema而非源schema
													baseSql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` IN (", sp.destSchema, destTable, primaryCol)
												} else {
													// 多字段联合主键：WHERE (`col1`, `col2`, `col3`) IN (
													colNames := make([]string, len(primaryCols))
													for i, col := range primaryCols {
														colNames[i] = fmt.Sprintf("`%s`", col)
													}
													// 使用目标schema和目标表名
													baseSql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE (%s) IN (", sp.destSchema, destTable, strings.Join(colNames, ", "))
												}
												baseSqlLen := len(baseSql)
												closeBracketLen := len(");")

												// 根据长度限制合并值
												var currentValues []string
												currentLength := baseSqlLen

												for i, value := range primaryValues {
													valueLen := len(value)
													separatorLen := 0
													if i > 0 {
														separatorLen = 2 // 逗号和空格的长度 ", "
													}

													// 检查添加当前值是否会超过长度限制
													if currentLength+separatorLen+valueLen+closeBracketLen > deleteSqlSize {
														// 如果当前已经有值，先生成并发送当前的合并SQL
														if len(currentValues) > 0 {
															mergedSql := fmt.Sprintf("%s%s);", baseSql, strings.Join(currentValues, ", "))
															sendFixSQL(cc, c1.ChunkSeq, mergedSql)
															// 重置当前值列表和长度
															currentValues = []string{value}
															currentLength = baseSqlLen + valueLen
														} else {
															// 如果单个值就超过限制，单独处理这条记录
															// 查找对应的原始记录并单独执行
															dbf.RowData = batchDel[i]
															sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
															if err != nil {
																sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
															}
															if sqlstr != "" {
																sendFixSQL(cc, c1.ChunkSeq, sqlstr)
															}
														}
													} else {
														// 添加当前值到合并列表
														currentValues = append(currentValues, value)
														currentLength += separatorLen + valueLen
													}
												}

												// 处理剩余的值
												if len(currentValues) > 0 {
													mergedSql := fmt.Sprintf("%s%s);", baseSql, strings.Join(currentValues, ", "))
													sendFixSQL(cc, c1.ChunkSeq, mergedSql)
												}
											} else {
												// 如果无法合并，回退到单独执行
												for _, i := range batchDel {
													dbf.RowData = i
													sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
													if err != nil {
														sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
														continue
													}

													// 为 DELETE fix 生成回滚 INSERT
													if sp.rollCC != nil {
														if rbSql := rollbackRowToInsert(destSchema, destTable, i, dbf.ColData); rbSql != "" {
															sendRollback(sp.rollCC, rbSql)
														}
													}

													// 提取WHERE条件中的主键值，用于去重
													var primaryKey string
													if strings.Contains(sqlstr, "WHERE") {
														wherePart := strings.Split(sqlstr, "WHERE")[1]
														wherePart = strings.TrimSpace(strings.TrimSuffix(wherePart, ";"))

														if isSinglePrimary {
															key := fmt.Sprintf("`%s` = '", primaryCol)
															if strings.Contains(wherePart, key) {
																part := strings.Split(wherePart, key)[1]
																if strings.Contains(part, "'") {
																	value := strings.Split(part, "'")[0]
																	primaryKey = fmt.Sprintf("%s.%s.%s:%s", c1.Schema, c1.Table, primaryCol, value)
																}
															}
														} else {
															// 多字段联合主键：提取所有主键值组合
															var keyList []string
															foundAllValues := true
															for _, col := range primaryCols {
																pattern := fmt.Sprintf("`%s` = '", col)
																index := strings.Index(wherePart, pattern)
																if index == -1 {
																	foundAllValues = false
																	break
																}
																afterPattern := wherePart[index+len(pattern):]
																valueEnd := strings.Index(afterPattern, "'")
																if valueEnd == -1 {
																	foundAllValues = false
																	break
																}
																value := afterPattern[:valueEnd]
																keyList = append(keyList, fmt.Sprintf("%s:%s", col, value))
																wherePart = afterPattern[valueEnd+1:]
															}
															if foundAllValues {
																primaryKey = fmt.Sprintf("%s.%s.%s", c1.Schema, c1.Table, strings.Join(keyList, ","))
															}
														}
													}

													// 检查该主键值是否已经处理过
													if primaryKey != "" {
														if markDeleteKeyIfAbsent(primaryKey, useGlobalKeyDedupe) {
															// 发送SQL语句
															if sqlstr != "" {
																sendFixSQL(cc, c1.ChunkSeq, sqlstr)
															}
														}
													} else {
														// 如果无法提取主键值，直接发送SQL语句
														if sqlstr != "" {
															sendFixSQL(cc, c1.ChunkSeq, sqlstr)
														}
													}
												}
											}
										} else {
											// 对于无主键或普通索引（mul），统计相同记录的数量，生成带正确LIMIT的DELETE语句
											rowCountMap := make(map[string]int)
											for _, i := range batchDel {
												rowCountMap[i]++
											}

											for rowData, count := range rowCountMap {
												dbf.RowData = rowData
												sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
												if err != nil {
													sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
													continue
												}

												// 为 mul 类型 DELETE 生成回滚 INSERT（每行一条）
												if sp.rollCC != nil {
													if rbSql := rollbackRowToInsert(destSchema, destTable, rowData, dbf.ColData); rbSql != "" {
														for j := 0; j < count; j++ {
															sendRollback(sp.rollCC, rbSql)
														}
													}
												}

												// 修改SQL语句，将LIMIT 1改为LIMIT count
												if strings.Contains(sqlstr, "LIMIT 1") {
													sqlstr = strings.Replace(sqlstr, "LIMIT 1", fmt.Sprintf("LIMIT %d", count), 1)
												}

												// 使用修改后的SQL作为去重键
												if markDeleteKeyIfAbsent(sqlstr, useGlobalKeyDedupe) {
													// 发送SQL语句
													if sqlstr != "" {
														sendFixSQL(cc, c1.ChunkSeq, sqlstr)
													}
												}
											}
										}
									} else {
										// 对于非MySQL数据库，暂时保持单独执行
										for _, i := range batchDel {
											dbf.RowData = i
											sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
											if err != nil {
												sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
												continue
											}

											// 为 DELETE fix 生成回滚 INSERT
											if sp.rollCC != nil {
												if rbSql := rollbackRowToInsert(destSchema, destTable, i, dbf.ColData); rbSql != "" {
													sendRollback(sp.rollCC, rbSql)
												}
											}

											// 提取WHERE条件中的主键值，用于去重
											var primaryKey string
											if strings.Contains(sqlstr, "WHERE") {
												wherePart := strings.Split(sqlstr, "WHERE")[1]
												wherePart = strings.TrimSpace(strings.TrimSuffix(wherePart, ";"))

												if isSinglePrimary {
													key := fmt.Sprintf("`%s` = '", primaryCol)
													if strings.Contains(wherePart, key) {
														part := strings.Split(wherePart, key)[1]
														if strings.Contains(part, "'") {
															value := strings.Split(part, "'")[0]
															primaryKey = fmt.Sprintf("%s.%s.%s:%s", c1.Schema, c1.Table, primaryCol, value)
														}
													}
												} else {
													// 多字段联合主键：提取所有主键值组合
													var keyList []string
													foundAllValues := true
													for _, col := range primaryCols {
														pattern := fmt.Sprintf("`%s` = '", col)
														index := strings.Index(wherePart, pattern)
														if index == -1 {
															foundAllValues = false
															break
														}
														afterPattern := wherePart[index+len(pattern):]
														valueEnd := strings.Index(afterPattern, "'")
														if valueEnd == -1 {
															foundAllValues = false
															break
														}
														value := afterPattern[:valueEnd]
														keyList = append(keyList, fmt.Sprintf("%s:%s", col, value))
														wherePart = afterPattern[valueEnd+1:]
													}
													if foundAllValues {
														primaryKey = fmt.Sprintf("%s.%s.%s", c1.Schema, c1.Table, strings.Join(keyList, ","))
													}
												}
											}

											// 检查该主键值是否已经处理过
											if primaryKey != "" {
												exists := hasDeleteKey(primaryKey, useGlobalKeyDedupe)

												// 关键修复：检查该主键是否已经被INSERT过
												inserted := hasInsertKey(primaryKey, useGlobalKeyDedupe)

												if !exists && !inserted {
													// 添加到全局去重map
													markDeleteKeyIfAbsent(primaryKey, useGlobalKeyDedupe)

													// 发送SQL语句
													if sqlstr != "" {
														sendFixSQL(cc, c1.ChunkSeq, sqlstr)
													}
												}
											} else {
												// 对于无法提取主键值的情况，使用完整SQL作为去重键
												if markDeleteKeyIfAbsent(sqlstr, useGlobalKeyDedupe) {
													// 发送SQL语句
													if sqlstr != "" {
														sendFixSQL(cc, c1.ChunkSeq, sqlstr)
													}
												}
											}
										}
									}
								}
								vlog = fmt.Sprintf("(%d) DELETE statements generated for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
							}
							// DELETE语句处理完成后，再处理INSERT语句
							if len(add) > 0 {
								vlog = fmt.Sprintf("(%d) Generating INSERT statements for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
								global.Wlog.Debugf("DEBUG_SQL_ORDER_%d: Processing %d INSERT statements after DELETE for %s.%s\n",
									logThreadSeq, len(add), c1.Schema, c1.Table)

								// 分组处理INSERT语句，每fixTrxNum条合并一次
								for batchStart := 0; batchStart < len(add); batchStart += sp.fixTrxNum {
									batchEnd := batchStart + sp.fixTrxNum
									if batchEnd > len(add) {
										batchEnd = len(add)
									}
									batchAdd := add[batchStart:batchEnd]

									// INSERT去重已由insertedPrimaryKeys机制保证，不再限制batchAdd大小

									// 生成单独的INSERT语句，避免多线程并发下的重复冲突
									global.Wlog.Debugf("DEBUG_INSERT_LOOP_%d: Starting INSERT generation for %d records in batch for %s.%s\n",
										logThreadSeq, len(batchAdd), c1.Schema, c1.Table)

									insertCount := 0
									duplicateCount := 0
									for batchIndex, i := range batchAdd {
										dbf.RowData = i
										sqlstr, err := dbf.DataAbnormalFix().FixInsertSqlExec(ddb, sp.ddrive, logThreadSeq)
										if err != nil {
											sp.getErr(fmt.Sprintf("dest: checksum table %s.%s generate INSERT sql error.", c1.Schema, c1.Table), err)
										} else if sqlstr != "" {
											// 关键修复：进行INSERT去重检查，防止跨chunk重复生成INSERT语句
											// 使用RowData（以/*go actions columnData*/分隔）提取主键值进行去重
											isDuplicate := false
											if len(dbf.IndexColumn) > 0 {
												rowParts := strings.Split(dbf.RowData, "/*go actions columnData*/")

												// 关键修复：构建列名到位置的映射，因为RowData的列顺序可能与IndexColumn不同
												// RowData的列顺序由SELECT语句决定（通常来自ColData/SColumnInfo），
												// 而不是主键列的顺序。直接用rowParts[idx]会取到错误的列值
												colNameToIdx := make(map[string]int)
												for ci, colInfo := range dbf.ColData {
													if name, ok := colInfo["columnName"]; ok {
														colNameToIdx[name] = ci
													}
												}

												if len(rowParts) >= len(dbf.ColData) {
													var keyList []string
													allFound := true
													for _, col := range dbf.IndexColumn {
														if colIdx, ok := colNameToIdx[col]; ok && colIdx < len(rowParts) {
															keyList = append(keyList, fmt.Sprintf("%s:%s", col, rowParts[colIdx]))
														} else {
															// 如果找不到列位置，跳过去重检查
															allFound = false
															break
														}
													}
													if allFound {
														// 关键修复：如果主键列中包含NULL值，跳过去重检查
														// 在MySQL中 NULL != NULL，UNIQUE KEY允许多个NULL值
														hasNullKey := false
														for _, kv := range keyList {
															kvParts := strings.SplitN(kv, ":", 2)
															if len(kvParts) == 2 {
																val := strings.TrimSpace(kvParts[1])
																if val == "" || val == dataDispos.ValueNullPlaceholder || strings.EqualFold(val, "NULL") {
																	hasNullKey = true
																	break
																}
															}
														}
														// NULL行不参与去重(MySQL中NULL!=NULL)，仅对非NULL行进行去重
														if !hasNullKey {
															primaryKey := fmt.Sprintf("%s.%s.%s", c1.Schema, c1.Table, strings.Join(keyList, ","))
															alreadyInserted := !markInsertKeyIfAbsent(primaryKey, useGlobalKeyDedupe)
															if alreadyInserted {
																isDuplicate = true
															}
														} // end if !hasNullKey
													}
												}
											}
											if isDuplicate {
												duplicateCount++
												continue
											}
											insertCount++
											// 记录生成的SQL语句
											vlog = fmt.Sprintf("(%d) Generated INSERT statement for %s.%s", logThreadSeq, c1.Schema, c1.Table)
											global.Wlog.Debug(vlog)

											// 如果是前几条记录，输出调试信息
											if insertCount <= 5 {
												sqlPreview := sqlstr
												if len(sqlstr) > 50 {
													sqlPreview = sqlstr[:50] + "..."
												}
												global.Wlog.Debugf("DEBUG_INSERT_DETAIL_%d: Batch[%d] - Insert count %d - SQL starts with: %s\n",
													logThreadSeq, batchIndex, insertCount, sqlPreview)
											}

											sendFixSQL(cc, c1.ChunkSeq, sqlstr)
											// 为 INSERT fix 生成回滚 DELETE
											if sp.rollCC != nil {
												if rbSql := rollbackInsertToDelete(sqlstr, destSchema, destTable, dbf.IndexColumn); rbSql != "" {
													sendRollback(sp.rollCC, rbSql)
												}
											}
											totalInsertCount++
										}
									}

									if duplicateCount > 0 {
										global.Wlog.Debugf("DEBUG_INSERT_LOOP_%d: Generated %d INSERT statements, skipped %d duplicates for batch with %d records in %s.%s (Total so far: %d)\n",
											logThreadSeq, insertCount, duplicateCount, len(batchAdd), c1.Schema, c1.Table, totalInsertCount)
									} else {
										global.Wlog.Debugf("DEBUG_INSERT_LOOP_%d: Generated %d INSERT statements for batch with %d records in %s.%s (Total so far: %d)\n",
											logThreadSeq, insertCount, len(batchAdd), c1.Schema, c1.Table, totalInsertCount)
									}
								}
								vlog = fmt.Sprintf("(%d) INSERT statements generated for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
							}
						}
					}
				}(c, sdb, ddb)
			}
		}
	}
}

// writeColumnsModeAdvisory 在 columns 模式下，当存在未自动生成修复 SQL 的差异行时，
// 写入一个纯注释的 advisory 文件，提示人工介入。涵盖两种情形：
//   - source-only：源端有目标端无，columns 模式不生成 INSERT（全列未知）
//   - target-only：目标端有源端无，extraRowsSyncToSource=OFF，不生成 DELETE
//
// 文件命名：<fixFileDir>/columns-advisory.<schema>.<table>.sql
// 文件内容全部为 SQL 注释（-- 开头），不含任何可执行语句，不会被误执行。

// columnsModeFilteredCols returns a column info slice for columns-mode SELECT queries,
// keeping only PK columns (pkCols) and user-selected compare columns (compareCols),
// in the original table column order from allCols.
// columnsModeFilteredCols builds a column-info slice for columns mode.
// Output order: PK columns first (in pkCols order), then compare columns in compareCols order.
// This mirrors the SELECT column order produced by GeneratingQuerySql so that row-string
// positions are aligned between source and target (Pairs semantics).

// columnsModeSplitPKAndCompare aligns PK detection with the rest of the columns-mode
// pipeline by treating column names case-insensitively (ToLower + TrimSpace, consistent
// with normalizeColumnLookupKey in my_query_table_data.go).
//
// compareColNames preserves the original case of col["columnName"] from filteredCols;
// callers (FixUpdateSqlExec, orderColumnsForCompare) perform their own ToLower lookup,
// so the original case is safe to retain here.

// pkKeyMissingMarker 是 columnsModeExtractPKKey 在 pos 越界时使用的哨兵值。
// \x01 (SOH) 不会出现在正常 MySQL 文本数据中，可与合法空字符串 PK 值严格区分。
const pkKeyMissingMarker = "\x01<MISSING>\x01"

// columnsModeExtractPKKey extracts a composite PK key string from a row data string.
// pkPositions are the 0-based column positions of PK columns within the filtered column list.
// Values are joined with a NUL byte to avoid collisions with normal data.
//
// Out-of-bounds positions are represented with pkKeyMissingMarker (not empty string) so
// a legitimately-empty-string PK value and a missing PK component produce different keys.
// A WARN log is emitted when out-of-bounds occurs, because it means the row data is corrupted.
