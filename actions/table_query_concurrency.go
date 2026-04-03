package actions

import (
	"fmt"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"math/rand"
	"os"
	"strings"
	"sync"
)

type SchedulePlan struct {
	chunkSize, mqQueueDepth int

	schema, table            string   //待校验库名、表名
	sourceSchema, destSchema string   //源端和目标端库名
	destTable                string   //目标端表名（表映射场景下可能与源端不同）
	columnName               []string //待校验表的列名，有可能是多个
	tmpTableDataFileDir      string   //临时表文件生成的相对路径
	tableIndexColumnMap      map[string][]string
	sdbPool, ddbPool         *global.Pool
	datafixType              string
	datafixSql               string
	sdrive, ddrive           string
	checkObject              string
	checkNoIndexTable        string //是否检查无索引表
	tableAllCol              map[string]global.TableAllColumnInfoS
	caseSensitiveObjectName  string //是否区分对象名大小写

	file                      *os.File
	TmpFileName               string
	fixTrxNum                 int
	fixTrxSize                int
	insertSqlSize             int
	deleteSqlSize             int
	chanrowCount, concurrency int //单次并发一次校验的行数
	TmpTablePath              string
	smtype                    string //是源端还是目标端
	indexColumnType           string
	pods                      *Pod
	tableMaxRows              uint64
	djdbc                     string
	tableMappings             map[string]string // 表映射关系
	bar                       *Bar              // 进度条
	forceFullTableCheck       bool              // 是否强制进行全表检查

	// columns 模式字段
	// columnPlan 不为 nil 时表示当前为部分列校验模式。
	// 由 inputArg.ConfigParameter.ColumnPlan 携带，类型用 interface{} 以避免与 actions 包的循环导入。
	// 实际类型为 *inputArg.TableColumnPlan；调用方通过 getColumnPlan() 转换访问。
	columnPlanSourceCols   []string // ColumnPlan.SourceColumns()，nil = 全列模式
	columnPlanTargetCols   []string // ColumnPlan.TargetColumns()，nil = 全列模式
	columnPlanSimpleMode   bool     // ColumnPlan.SimpleMode
	columnPlanSourceSchema string   // 列计划绑定的源端 schema（用于多表场景的精确匹配）
	columnPlanSourceTable  string   // 列计划绑定的源端表名
	columnPlanTargetSchema string   // 列计划绑定的目标端 schema
	columnPlanTargetTable  string   // 列计划绑定的目标端表名
	extraRowsSyncToSource  string   // ON | OFF，仅 columns 模式下有意义

	// sourceOnlyAdvisory 在 columns 模式下收集 source-only 行的 PK，用于生成 advisory 提示文件。
	// 非 columns 模式时为 nil。
	sourceOnlyAdvisory *columnsModeSourceOnlyAdvisory
}

// columnsModeSourceOnlyAdvisory 记录 columns 模式下差异行的统计信息，
// 用于在校验结束后生成人工介入建议文件。
type columnsModeSourceOnlyAdvisory struct {
	mu              sync.Mutex
	schema          string
	table           string
	destSchema      string
	destTable       string   // 目标端表名（表映射场景下可能与源端不同）
	indexCols       []string // 主键 / 唯一键列名
	sourceOnlyCount int      // 源端有目标端无的行数（未生成 INSERT）
	targetOnlyCount int      // 目标端有源端无的行数（extraRowsSyncToSource=OFF，未生成 DELETE）
}

// buildColumnsInfo returns a human-readable summary of the partial-columns plan for
// display in the Columns output column.  Returns "" when not in columns mode.
//
// SimpleMode  → "srcSchema.srcTable.col1,srcSchema.srcTable.col2,…"
// MappingMode → "srcSchema.srcTable.srcCol:dstSchema.dstTable.dstCol,…"
func (sp *SchedulePlan) buildColumnsInfo() string {
	if len(sp.columnPlanSourceCols) == 0 {
		return ""
	}
	srcSchema := sp.sourceSchema
	if srcSchema == "" {
		srcSchema = sp.schema
	}
	dstSchema := sp.destSchema
	if dstSchema == "" {
		dstSchema = srcSchema
	}
	dstTable := sp.destTable
	if dstTable == "" {
		dstTable = sp.table
	}
	if sp.columnPlanSimpleMode && dstTable == sp.table && dstSchema == srcSchema {
		parts := make([]string, len(sp.columnPlanSourceCols))
		for i, col := range sp.columnPlanSourceCols {
			parts[i] = fmt.Sprintf("%s.%s.%s", srcSchema, sp.table, col)
		}
		return strings.Join(parts, ",")
	}
	parts := make([]string, len(sp.columnPlanSourceCols))
	for i, srcCol := range sp.columnPlanSourceCols {
		dstCol := srcCol
		if i < len(sp.columnPlanTargetCols) {
			dstCol = sp.columnPlanTargetCols[i]
		}
		parts[i] = fmt.Sprintf("%s.%s.%s:%s.%s.%s", srcSchema, sp.table, srcCol, dstSchema, dstTable, dstCol)
	}
	return strings.Join(parts, ",")
}

// getDisplayTableName 返回表的显示名称，包含映射关系信息
// 如果存在映射关系，返回格式为 "sourceSchema.table:destSchema.table"
// 如果不存在映射关系，返回格式为 "schema.table"
func (sp *SchedulePlan) getDisplayTableName() string {
	// 检查是否存在映射关系
	if mappedSchema, exists := sp.tableMappings[sp.schema]; exists && mappedSchema != sp.schema {
		// 存在映射关系，返回包含映射信息的名称
		return fmt.Sprintf("%s.%s:%s.%s", sp.schema, sp.table, mappedSchema, sp.table)
	}

	// 不存在映射关系，返回普通名称
	return fmt.Sprintf("%s.%s", sp.schema, sp.table)
}

/*
差异数据信息结构体
*/
type DifferencesDataStruct struct {
	Schema string //存在差异数据的库
	Table  string //存在差异数据的表
	Spoint string //校验开始时的源端全局一致性点
	Dpoint string //校验开始时的目端全局一致性点
	//TableColumnInfo []map[string]string //该表的所有列信息，包括列类型
	TableColumnInfo global.TableAllColumnInfoS //该表的所有列信息，包括列类型
	SqlWhere        map[string]string          //差异数据查询的where 条件
	indexColumnType string                     //索引列类型
}

func preserveDDLResultPods(pods []Pod) []Pod {
	preserved := make([]Pod, 0, len(pods))
	for _, pod := range pods {
		if pod.DIFFS == "DDL-yes" {
			preserved = append(preserved, pod)
		}
	}
	return preserved
}

/*
查询索引列信息，并发执行调度生成
*/
func (sp *SchedulePlan) Schedulingtasks() {
	totalTables := len(sp.tableIndexColumnMap)
	var logThreadSeq int64 // outer-loop log identifier; inner branches may override
	for k, v := range sp.tableIndexColumnMap {
		//是否校验无索引表
		if sp.checkNoIndexTable == "no" && len(v) == 0 {
			continue
		}
		// 为每个表创建独立的SchedulePlan副本，避免并发冲突
		spCopy := *sp
		// 解析key中的源表和目标表信息
		// key格式: sourceSchema/*gtchecksumSchemaTable*/sourceTable/*indexColumnType*/indexType/*mapping*/destSchema/*mappingTable*/destTable
		vlog := fmt.Sprintf("Processing table key: %s", k)
		global.Wlog.Debug(vlog)

		var sourceSchema, sourceTable, destSchema, destTable, indexType string

		// 解析源表schema和表名
		if strings.Contains(k, "/*gtchecksumSchemaTable*/") {
			parts := strings.Split(k, "/*gtchecksumSchemaTable*/")
			if len(parts) >= 2 {
				sourceSchema = parts[0]
				remainingPart := parts[1]

				// 解析源表表名
				if strings.Contains(remainingPart, "/*indexColumnType*/") {
					tableParts := strings.Split(remainingPart, "/*indexColumnType*/")
					if len(tableParts) >= 2 {
						sourceTable = tableParts[0]
						remainingPart = tableParts[1]

						// 解析索引类型
						if strings.Contains(remainingPart, "/*mapping*/") {
							indexParts := strings.Split(remainingPart, "/*mapping*/")
							if len(indexParts) >= 2 {
								indexType = indexParts[0]
								remainingPart = indexParts[1]

								// 解析目标表schema和表名
								if strings.Contains(remainingPart, "/*mappingTable*/") {
									destParts := strings.Split(remainingPart, "/*mappingTable*/")
									if len(destParts) >= 2 {
										destSchema = destParts[0]
										destTable = destParts[1]
									}
								}
							}
						}
					}
				} else if strings.Contains(remainingPart, "/*mapping*/") {
					// 处理无索引表的情况
					tableParts := strings.Split(remainingPart, "/*mapping*/")
					if len(tableParts) >= 2 {
						sourceTable = tableParts[0]
						remainingPart = tableParts[1]

						// 解析目标表schema和表名
						if strings.Contains(remainingPart, "/*mappingTable*/") {
							destParts := strings.Split(remainingPart, "/*mappingTable*/")
							if len(destParts) >= 2 {
								destSchema = destParts[0]
								destTable = destParts[1]
							}
						}
					}
				}
			}
		}

		// 设置SchedulePlan副本的属性
		spCopy.schema = sourceSchema
		spCopy.table = sourceTable
		spCopy.sourceSchema = sourceSchema
		spCopy.destSchema = destSchema
		spCopy.indexColumnType = indexType
		if destTable == "" {
			destTable = sourceTable
		}
		spCopy.destTable = destTable

		// columns 模式精确表对匹配：列计划只对指定的源/目标表对生效，其余表回退全列模式，避免列计划错误套用到多表场景。
		if sp.columnPlanSourceTable != "" {
			if sourceSchema != sp.columnPlanSourceSchema || sourceTable != sp.columnPlanSourceTable ||
				destSchema != sp.columnPlanTargetSchema || destTable != sp.columnPlanTargetTable {
				spCopy.columnPlanSourceCols = nil
				spCopy.columnPlanTargetCols = nil
				global.Wlog.Debug(fmt.Sprintf("(%d) [columns-mode] table %s.%s→%s.%s does not match columns plan (%s.%s→%s.%s); switching to full-column mode",
					logThreadSeq, sourceSchema, sourceTable, destSchema, destTable,
					sp.columnPlanSourceSchema, sp.columnPlanSourceTable,
					sp.columnPlanTargetSchema, sp.columnPlanTargetTable))
			}
		}

		tmpFile, err := os.OpenFile(spCopy.TmpFileName, os.O_CREATE|os.O_RDWR, 0777)
		if err != nil {
			global.Wlog.Error(fmt.Sprintf("Failed to open temp file %s for table %s.%s: %v", spCopy.TmpFileName, spCopy.sourceSchema, spCopy.table, err))
			continue
		}
		spCopy.file = tmpFile

		vlog = fmt.Sprintf("Key parsed - Source: %s.%s, Target: %s.%s, Index: %s",
			sourceSchema, sourceTable, destSchema, destTable, indexType)
		global.Wlog.Debug(vlog)

		// DDL一致性检查：校验源端与目标端表结构是否一致
		sourceColKey := fmt.Sprintf("%s_gtchecksum_%s", sourceSchema, sourceTable)
		destColKey := fmt.Sprintf("%s_gtchecksum_%s", destSchema, destTable)
		if sourceColInfo, ok1 := sp.tableAllCol[sourceColKey]; ok1 {
			if destColInfo, ok2 := sp.tableAllCol[destColKey]; ok2 {
				srcCols := sourceColInfo.SColumnInfo
				dstCols := destColInfo.DColumnInfo

				if len(spCopy.columnPlanSourceCols) > 0 {
					// columns 模式：只对选中列做 DDL 严格检查；非选中列差异降级为警告。
					selectedSrc := filterColumnInfoByNames(srcCols, spCopy.columnPlanSourceCols)
					selectedDst := filterColumnInfoByNames(dstCols, spCopy.columnPlanTargetCols)

					// columns 存在性校验：指定列必须在两端表中实际存在，否则拒绝继续（避免静默缩小比较范围）。
					if missingSrc := findMissingColumnNames(spCopy.columnPlanSourceCols, srcCols); len(missingSrc) > 0 {
						errMsg := fmt.Sprintf("columns parameter specifies column(s) %v that do not exist in source table %s.%s",
							missingSrc, sourceSchema, sourceTable)
						fmt.Printf("\n[ERROR] %s\n", errMsg)
						global.Wlog.Error(fmt.Sprintf("(%d) [columns-mode] %s", logThreadSeq, errMsg))
						global.AddSkippedTableWithDiffs(sourceSchema, sourceTable, "data", "columns-missing-in-source: "+strings.Join(missingSrc, ","), global.SkipDiffsDDLYes)
						if totalTables == 1 {
							os.Exit(1)
						}
						continue
					}
					if missingDst := findMissingColumnNames(spCopy.columnPlanTargetCols, dstCols); len(missingDst) > 0 {
						errMsg := fmt.Sprintf("columns parameter specifies column(s) %v that do not exist in target table %s.%s",
							missingDst, destSchema, destTable)
						fmt.Printf("\n[ERROR] %s\n", errMsg)
						global.Wlog.Error(fmt.Sprintf("(%d) [columns-mode] %s", logThreadSeq, errMsg))
						global.AddSkippedTableWithDiffs(sourceSchema, sourceTable, "data", "columns-missing-in-target: "+strings.Join(missingDst, ","), global.SkipDiffsDDLYes)
						if totalTables == 1 {
							os.Exit(1)
						}
						continue
					}

					// 对选中列做类型兼容性检查（位置对齐，支持列名映射）
					if typeMismatch := checkColumnsPairTypeMismatch(selectedSrc, selectedDst, spCopy.columnPlanSourceCols, spCopy.columnPlanTargetCols); typeMismatch != "" {
						fmt.Printf("\n[ERROR] columns mode DDL type mismatch for %s.%s → %s.%s, cannot compare:\n%s\n",
							sourceSchema, sourceTable, destSchema, destTable, typeMismatch)
						global.Wlog.Error(fmt.Sprintf("columns mode DDL type mismatch for %s.%s→%s.%s: %s",
							sourceSchema, sourceTable, destSchema, destTable, typeMismatch))
						global.AddSkippedTableWithDiffs(sourceSchema, sourceTable, "data", "columns-DDL-mismatch: "+typeMismatch, global.SkipDiffsDDLYes)
						if totalTables == 1 {
							os.Exit(1)
						}
						continue
					}

					// 非选中列是否存在差异：运行全列检查，有差异时仅警告，不阻断
					if fullMismatch := checkDDLConsistency(srcCols, dstCols, sourceSchema, sourceTable, destSchema, destTable, sp.sdrive, sp.ddrive); fullMismatch != "" {
						vlog = fmt.Sprintf("(%d) [columns-mode] non-selected-columns-differ for %s.%s→%s.%s (compare not blocked): %s",
							logThreadSeq, sourceSchema, sourceTable, destSchema, destTable, fullMismatch)
						global.Wlog.Warn(vlog)
					}
				} else {
					// 全列模式：原有逻辑
					if mismatch := checkDDLConsistency(srcCols, dstCols, sourceSchema, sourceTable, destSchema, destTable, sp.sdrive, sp.ddrive); mismatch != "" {
						fmt.Printf("\n[WARNING] DDL mismatch detected for table %s.%s vs %s.%s, skipping checksum:\n%s\n",
							sourceSchema, sourceTable, destSchema, destTable, mismatch)
						global.Wlog.Error(fmt.Sprintf("DDL mismatch detected for table %s.%s vs %s.%s: %s",
							sourceSchema, sourceTable, destSchema, destTable, mismatch))
						global.AddSkippedTableWithDiffs(sourceSchema, sourceTable, "data", "DDL mismatch: "+mismatch, global.SkipDiffsDDLYes)
						if totalTables == 1 {
							fmt.Printf("\n[ERROR] Only one table to check and data checksum precheck found a DDL mismatch. Exiting.\n")
							fmt.Printf("Source: %s.%s, Target: %s.%s\n", sourceSchema, sourceTable, destSchema, destTable)
							fmt.Printf("Detail: %s\n", mismatch)
							fmt.Printf("Suggestion: run checkObject=struct or align the table DDL before retrying data checksum.\n")
							global.Wlog.Error(fmt.Sprintf("Only one table to check (%s.%s) and data checksum precheck found a DDL mismatch, exiting", sourceSchema, sourceTable))
							os.Exit(1)
						}
						continue
					}
				}
			}
		}

		if len(v) == 0 { //校验无索引表
			// columns 模式不支持无索引表：无法可靠定位行，禁止启用
			if len(spCopy.columnPlanSourceCols) > 0 {
				vlog = fmt.Sprintf("(%d) [columns-mode] table %s.%s has no primary/unique key; columns mode requires a reliable row identifier — skipping",
					logThreadSeq, sourceSchema, sourceTable)
				global.Wlog.Error(vlog)
				fmt.Printf("\n[ERROR] columns mode cannot be used with no-index table %s.%s; specify a table with a primary or unique key.\n",
					sourceSchema, sourceTable)
				global.AddSkippedTableWithDiffs(sourceSchema, sourceTable, "data", "columns-mode-no-index-table", global.SkipDiffsDDLYes)
				if totalTables == 1 {
					os.Exit(1)
				}
				continue
			}

			spCopy.chanrowCount = spCopy.chunkSize
			logThreadSeq := rand.Int63()

			// 开始新表的进度显示
			tableName := fmt.Sprintf("begin checksum no-index table %s.%s", spCopy.schema, spCopy.table)
			fmt.Printf("\n%s\n", tableName)

			// 为每个表创建独立的进度条，但不在初始化时设置总数
			// 总数将在SingleTableCheckProcessing中根据实际行数设置
			spCopy.bar = &Bar{}

			spCopy.SingleTableCheckProcessing(spCopy.chanrowCount, logThreadSeq)

			// 显示完成消息
			fmt.Printf("table %s.%s checksum completed\n", spCopy.schema, spCopy.table)
		} else { //校验有索引的表
			spCopy.chanrowCount = spCopy.chunkSize
			spCopy.columnName = v
			// 显示开始信息
			displayTableName := spCopy.getDisplayTableName()
			fmt.Printf("\nbegin checksum index table %s\n", displayTableName)

			// 为每个表创建独立的进度条，以100为总数显示百分比进度
			spCopy.bar = &Bar{}
			spCopy.bar.NewOption(0, 100, "task") // 以100为总数，显示百分比进度

			spCopy.doIndexDataCheck()

			// 显示完成消息
			fmt.Printf("table %s checksum completed\n", displayTableName)
		}
		if spCopy.file != nil {
			_ = spCopy.file.Close()
		}
		os.Remove(spCopy.TmpFileName)
	}

}

// NewTableProgress is deprecated and removed as each table now creates its own progress bar directly in Schedulingtasks

func CheckTableQuerySchedule(sdb, ddb *global.Pool, tableIndexColumnMap map[string][]string, tableAllCol map[string]global.TableAllColumnInfoS, m inputArg.ConfigParameter) *SchedulePlan {
	// 保留前置结构校验阶段已经生成的DDL差异结果，避免在进入数据校验调度时被清空
	measuredDataPods = preserveDDLResultPods(measuredDataPods)

	// 解析表映射关系
	tableMappings := make(map[string]string)

	// 处理表映射关系
	if m.SecondaryL.SchemaV.Tables != "" {
		for _, pattern := range strings.Split(m.SecondaryL.SchemaV.Tables, ",") {
			if strings.Contains(pattern, ":") {
				mapping := strings.SplitN(pattern, ":", 2)
				if len(mapping) == 2 {
					srcPattern := mapping[0]
					dstPattern := mapping[1]

					// 处理 db1.*:db2.* 格式
					if strings.HasSuffix(srcPattern, ".*") && strings.HasSuffix(dstPattern, ".*") {
						srcDB := strings.TrimSuffix(srcPattern, ".*")
						dstDB := strings.TrimSuffix(dstPattern, ".*")
						tableMappings[srcDB] = dstDB
					} else if strings.Contains(srcPattern, ".") && strings.Contains(dstPattern, ".") {
						// 处理 db1.t1:db2.t2 格式
						srcParts := strings.Split(srcPattern, ".")
						dstParts := strings.Split(dstPattern, ".")

						if len(srcParts) > 0 && len(dstParts) > 0 {
							srcDB := srcParts[0]
							dstDB := dstParts[0]
							tableMappings[srcDB] = dstDB
						}
					}
				}
			}
		}
	}

	sp := &SchedulePlan{
		concurrency:             m.SecondaryL.RulesV.ParallelThds,
		sdbPool:                 sdb,
		ddbPool:                 ddb,
		chunkSize:               m.SecondaryL.RulesV.ChanRowCount,
		tableIndexColumnMap:     tableIndexColumnMap,
		tableAllCol:             tableAllCol,
		datafixType:             m.SecondaryL.RepairV.Datafix,
		datafixSql:              m.SecondaryL.RepairV.FixFileDir,
		sdrive:                  m.SecondaryL.DsnsV.SrcDrive,
		ddrive:                  m.SecondaryL.DsnsV.DestDrive,
		mqQueueDepth:            m.SecondaryL.RulesV.QueueSize,
		checkNoIndexTable:       m.SecondaryL.SchemaV.CheckNoIndexTable,
		checkObject:             m.SecondaryL.RulesV.CheckObject,
		TmpFileName:             m.NoIndexTableTmpFile,
		caseSensitiveObjectName: m.SecondaryL.SchemaV.CaseSensitiveObjectName,
		fixTrxNum:               m.SecondaryL.RepairV.FixTrxNum,
		fixTrxSize:              m.SecondaryL.RepairV.FixTrxSize,
		insertSqlSize:           m.SecondaryL.RepairV.InsertSqlSize * 1024,
		deleteSqlSize:           m.SecondaryL.RepairV.DeleteSqlSize * 1024,
		djdbc:                   m.SecondaryL.DsnsV.DestJdbc,
		tableMappings:           tableMappings,
		extraRowsSyncToSource:   m.SecondaryL.RepairV.ExtraRowsSyncToSource,
	}

	// columns 模式：将列计划展开为源/目标两侧的列列表注入 SchedulePlan。
	if plan := m.ColumnPlan; plan != nil {
		sp.columnPlanSourceCols = plan.SourceColumns()
		sp.columnPlanTargetCols = plan.TargetColumns()
		sp.columnPlanSimpleMode = plan.SimpleMode
		sp.columnPlanSourceSchema = plan.SourceSchema
		sp.columnPlanSourceTable = plan.SourceTable
		sp.columnPlanTargetSchema = plan.TargetSchema
		sp.columnPlanTargetTable = plan.TargetTable
	}

	return sp
}

// checkDDLConsistency 检查源端与目标端表的DDL定义是否一致
// 返回空字符串表示一致，返回非空字符串为详细的不一致报告
func checkDDLConsistency(sourceColumns, destColumns []map[string]string, sourceSchema, sourceTable, destSchema, destTable, sourceDrive, destDrive string) string {
	type columnMeta struct {
		name     string
		dataType string
		extra    string
	}

	isOracleDrive := func(d string) bool {
		x := strings.ToLower(strings.TrimSpace(d))
		return x == "oracle" || x == "godror"
	}
	isMySQLDrive := func(d string) bool {
		x := strings.ToLower(strings.TrimSpace(d))
		return x == "mysql"
	}
	normalizeColumnKey := func(name string) string {
		key := strings.TrimSpace(name)
		// Keep the fast DDL precheck consistent with the struct compare path:
		// MySQL and MariaDB column identifiers are compared case-insensitively.
		if isMySQLDrive(sourceDrive) && isMySQLDrive(destDrive) {
			return strings.ToUpper(key)
		}
		// Oracle metadata returns unquoted identifiers in uppercase.
		// For Oracle<->MySQL comparison, compare column names case-insensitively
		// to avoid false DDL mismatch on same semantic column names.
		if (isOracleDrive(sourceDrive) && isMySQLDrive(destDrive)) || (isMySQLDrive(sourceDrive) && isOracleDrive(destDrive)) {
			return strings.ToUpper(key)
		}
		return key
	}
	isIgnorableHiddenColumn := func(col columnMeta) bool {
		// MySQL 8.0+ may synthesize an invisible generated primary key column named
		// my_row_id. Data checksum should not treat that metadata-only addition as a
		// hard DDL mismatch when the column stays hidden from normal row access.
		return strings.EqualFold(strings.TrimSpace(col.name), "my_row_id") &&
			strings.Contains(strings.ToUpper(strings.TrimSpace(col.extra)), "INVISIBLE")
	}

	// 构建源端列名集合
	sourceColMap := make(map[string]columnMeta)
	for _, col := range sourceColumns {
		name := ""
		if v, ok := col["columnName"]; ok {
			name = v
		} else if v, ok := col["COLUMN_NAME"]; ok {
			name = v
		}
		dataType := ""
		if v, ok := col["dataType"]; ok {
			dataType = v
		} else if v, ok := col["DATA_TYPE"]; ok {
			dataType = v
		}
		extra := ""
		if v, ok := col["extra"]; ok {
			extra = v
		} else if v, ok := col["EXTRA"]; ok {
			extra = v
		}
		if name != "" {
			key := normalizeColumnKey(name)
			sourceColMap[key] = columnMeta{name: name, dataType: dataType, extra: extra}
		}
	}

	// 构建目标端列名集合
	destColMap := make(map[string]columnMeta)
	for _, col := range destColumns {
		name := ""
		if v, ok := col["columnName"]; ok {
			name = v
		} else if v, ok := col["COLUMN_NAME"]; ok {
			name = v
		}
		dataType := ""
		if v, ok := col["dataType"]; ok {
			dataType = v
		} else if v, ok := col["DATA_TYPE"]; ok {
			dataType = v
		}
		extra := ""
		if v, ok := col["extra"]; ok {
			extra = v
		} else if v, ok := col["EXTRA"]; ok {
			extra = v
		}
		if name != "" {
			key := normalizeColumnKey(name)
			destColMap[key] = columnMeta{name: name, dataType: dataType, extra: extra}
		}
	}

	var mismatches []string
	hasExistenceMismatch := false

	// 检查源端有但目标端没有的列
	for key, srcCol := range sourceColMap {
		if isIgnorableHiddenColumn(srcCol) {
			continue
		}
		if _, exists := destColMap[key]; !exists {
			hasExistenceMismatch = true
			mismatches = append(mismatches, fmt.Sprintf("  Column '%s' (%s) exists in source %s.%s but NOT in target %s.%s",
				srcCol.name, srcCol.dataType, sourceSchema, sourceTable, destSchema, destTable))
		}
	}

	// 检查目标端有但源端没有的列
	for key, destCol := range destColMap {
		if isIgnorableHiddenColumn(destCol) {
			continue
		}
		if _, exists := sourceColMap[key]; !exists {
			hasExistenceMismatch = true
			mismatches = append(mismatches, fmt.Sprintf("  Column '%s' (%s) exists in target %s.%s but NOT in source %s.%s",
				destCol.name, destCol.dataType, destSchema, destTable, sourceSchema, sourceTable))
		}
	}

	// 检查列数量是否一致
	effectiveSourceCount := 0
	for _, col := range sourceColMap {
		if isIgnorableHiddenColumn(col) {
			continue
		}
		effectiveSourceCount++
	}
	effectiveDestCount := 0
	for _, col := range destColMap {
		if isIgnorableHiddenColumn(col) {
			continue
		}
		effectiveDestCount++
	}
	if effectiveSourceCount != effectiveDestCount && !hasExistenceMismatch {
		mismatches = append(mismatches, fmt.Sprintf("  Column count mismatch: source %s.%s has %d columns, target %s.%s has %d columns",
			sourceSchema, sourceTable, effectiveSourceCount, destSchema, destTable, effectiveDestCount))
	}

	if len(mismatches) > 0 {
		return strings.Join(mismatches, "\n")
	}
	return ""
}

// filterColumnInfoByNames returns a new slice containing only the entries from colInfo
// whose "columnName" key matches one of the names in the allowList (case-insensitive,
// to match MySQL/MariaDB column name semantics).
// The result is returned in the order defined by allowList.
func filterColumnInfoByNames(colInfo []map[string]string, allowList []string) []map[string]string {
	if len(allowList) == 0 {
		return colInfo
	}
	byName := make(map[string]map[string]string, len(colInfo))
	for _, col := range colInfo {
		if name, ok := col["columnName"]; ok {
			byName[strings.ToLower(name)] = col
		}
	}
	result := make([]map[string]string, 0, len(allowList))
	for _, name := range allowList {
		if col, found := byName[strings.ToLower(name)]; found {
			result = append(result, col)
		}
	}
	return result
}

// findMissingColumnNames returns the subset of requested column names that are not present
// in colInfo (case-insensitive match on "columnName" key, to match MySQL/MariaDB semantics).
// Returns nil when all are found.
func findMissingColumnNames(requested []string, colInfo []map[string]string) []string {
	byName := make(map[string]bool, len(colInfo))
	for _, col := range colInfo {
		if name, ok := col["columnName"]; ok {
			byName[strings.ToLower(name)] = true
		}
	}
	var missing []string
	for _, name := range requested {
		if !byName[strings.ToLower(name)] {
			missing = append(missing, name)
		}
	}
	return missing
}

// checkColumnsPairTypeMismatch verifies that each source→target column pair in the
// columns plan has compatible data types.  srcCols and dstCols are already filtered
// to the selected columns and are ordered the same as srcNames/dstNames.
// Returns a human-readable mismatch description, or "" when everything is compatible.
func checkColumnsPairTypeMismatch(srcCols, dstCols []map[string]string, srcNames, dstNames []string) string {
	if len(srcCols) != len(dstCols) {
		return fmt.Sprintf("selected column count mismatch: source has %d, target has %d", len(srcCols), len(dstCols))
	}
	var mismatches []string
	for i := range srcCols {
		srcType := srcCols[i]["dataType"]
		dstType := dstCols[i]["dataType"]
		// Normalise: strip length/precision qualifiers for a coarse category check.
		srcBase := strings.ToUpper(strings.Split(srcType, "(")[0])
		dstBase := strings.ToUpper(strings.Split(dstType, "(")[0])
		if srcBase != dstBase {
			srcName := ""
			if i < len(srcNames) {
				srcName = srcNames[i]
			}
			dstName := ""
			if i < len(dstNames) {
				dstName = dstNames[i]
			}
			mismatches = append(mismatches, fmt.Sprintf(
				"  column pair %s→%s: incompatible types %s vs %s",
				srcName, dstName, srcType, dstType,
			))
		}
	}
	if len(mismatches) > 0 {
		return strings.Join(mismatches, "\n")
	}
	return ""
}
