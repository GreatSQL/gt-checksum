package actions

import (
	"database/sql"
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	mysql "gt-checksum/MySQL"
	"gt-checksum/schemacompat"
	"regexp"
	"strings"
	"sync"
)

func (stcls *schemaTable) SchemaTableAllCol(tableList []string, logThreadSeq, logThreadSeq2 int64) map[string]global.TableAllColumnInfoS {
	var (
		vlog           string
		tableCol       = make(map[string]global.TableAllColumnInfoS)
		tableColMu     sync.Mutex
		interfToString = func(colData []map[string]interface{}) []map[string]string {
			kel := make([]map[string]string, 0)
			for i := range colData {
				ke := make(map[string]string)
				for ii, iv := range colData[i] {
					ke[ii] = fmt.Sprintf("%v", iv)
				}
				kel = append(kel, ke)
			}
			return kel
		}
	)

	vlog = fmt.Sprintf("(%d) Start to obtain the metadata information of the source-target verification table ...", logThreadSeq)
	global.Wlog.Info(vlog)

	workers := stcls.tableIndexMetaWorkerCount(len(tableList))
	type job struct {
		sourceSchema, tableName, destSchema, destTableName string
	}
	jobs := make(chan job, len(tableList))

	for _, i := range tableList {
		// 添加调试日志，查看当前处理的表项
		vlog = fmt.Sprintf("(%d) Processing table entry: %s", logThreadSeq, i)
		global.Wlog.Debug(vlog)

		var sourceSchema, tableName, destSchema, destTableName string

		// 检查是否包含映射关系（格式为 sourceSchema.sourceTable:destSchema.destTable）
		if strings.Contains(i, ":") {
			parts := strings.Split(i, ":")
			if len(parts) == 2 {
				sourceParts := strings.Split(parts[0], ".")
				destParts := strings.Split(parts[1], ".")

				if len(sourceParts) == 2 && len(destParts) == 2 {
					sourceSchema = sourceParts[0]
					tableName = sourceParts[1]
					destSchema = destParts[0]
					destTableName = destParts[1]

					vlog = fmt.Sprintf("(%d) Parsed mapping: sourceSchema=%s, tableName=%s, destSchema=%s, destTableName=%s", logThreadSeq, sourceSchema, tableName, destSchema, destTableName)
					global.Wlog.Debug(vlog)
				} else {
					vlog = fmt.Sprintf("(%d) Invalid table mapping format: %s", logThreadSeq, i)
					global.Wlog.Error(vlog)
					continue
				}
			} else {
				vlog = fmt.Sprintf("(%d) Invalid table mapping format: %s", logThreadSeq, i)
				global.Wlog.Error(vlog)
				continue
			}
		} else {
			// 传统格式：schema.table
			parts := strings.Split(i, ".")
			if len(parts) == 2 {
				sourceSchema = parts[0]
				tableName = parts[1]
				destTableName = tableName

				// 根据映射规则确定目标端schema
				destSchema = sourceSchema
				if mappedSchema, exists := stcls.tableMappings[sourceSchema]; exists {
					destSchema = mappedSchema
				}

				vlog = fmt.Sprintf("(%d) Traditional format: sourceSchema=%s, tableName=%s, destSchema=%s", logThreadSeq, sourceSchema, tableName, destSchema)
				global.Wlog.Debug(vlog)
			} else {
				vlog = fmt.Sprintf("(%d) Invalid table format: %s", logThreadSeq, i)
				global.Wlog.Error(vlog)
				continue
			}
		}

		jobs <- job{sourceSchema: sourceSchema, tableName: tableName, destSchema: destSchema, destTableName: destTableName}
	}
	close(jobs)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				tc := dbExec.TableColumnNameStruct{Schema: j.sourceSchema, Table: j.tableName, Drive: stcls.sourceDrive}
				a, err := tc.Query().TableAllColumn(stcls.sourceDB, logThreadSeq2)
				if err != nil {
					global.Wlog.Warn(fmt.Sprintf("(%d) Source TableAllColumn query failed for %s.%s: %v", logThreadSeq, j.sourceSchema, j.tableName, err))
					continue
				}
				tc.Schema = j.destSchema
				tc.Table = j.destTableName
				tc.Drive = stcls.destDrive
				b, err := tc.Query().TableAllColumn(stcls.destDB, logThreadSeq2)
				if err != nil {
					global.Wlog.Warn(fmt.Sprintf("(%d) Target TableAllColumn query failed for %s.%s: %v", logThreadSeq, j.destSchema, j.destTableName, err))
					continue
				}
				sourceColInfo := interfToString(a)
				destColInfo := interfToString(b)
				if strings.EqualFold(stcls.checkRules.CheckObject, "data") {
					var strippedGeneratedColumns []string
					sourceColInfo, destColInfo, strippedGeneratedColumns = normalizeDataCheckColumnInfo(sourceColInfo, destColInfo)
					if len(strippedGeneratedColumns) > 0 {
						global.Wlog.Info(fmt.Sprintf("(%d) Stripped generated invisible columns from data-check target metadata for %s.%s -> %s.%s: %v", logThreadSeq, j.sourceSchema, j.tableName, j.destSchema, j.destTableName, strippedGeneratedColumns))
					}
				}
				entry := global.TableAllColumnInfoS{
					SColumnInfo: sourceColInfo,
					DColumnInfo: destColInfo,
				}
				srcKey := fmt.Sprintf("%s_gtchecksum_%s", j.sourceSchema, j.tableName)
				dstKey := fmt.Sprintf("%s_gtchecksum_%s", j.destSchema, j.destTableName)
				tableColMu.Lock()
				tableCol[srcKey] = entry
				if dstKey != srcKey {
					tableCol[dstKey] = entry
				}
				tableColMu.Unlock()
			}
		}()
	}
	wg.Wait()

	vlog = fmt.Sprintf("(%d) The metadata information of the source target verification table has been obtained", logThreadSeq)
	global.Wlog.Info(vlog)
	return tableCol
}

func (stcls *schemaTable) Struct(dtabS []string, logThreadSeq, logThreadSeq2 int64) error {
	//校验列名
	var (
		vlog  string
		event string
		// 用于记录每个表的索引、分区和外键是否一致的映射
		tableStructDiffs = make(map[string]bool)
		// 用于跟踪已经添加过Pod记录的表，避免重复添加
		existingTableKeys = make(map[string]bool)
	)
	event = fmt.Sprintf("[check_table_columns]")
	stcls.structWarnOnlyDiffsMap = make(map[string]bool)
	stcls.structCollationMappedMap = make(map[string]bool)

	// Split dtabS into BASE TABLE entries and VIEW entries.
	// dtabS is reassigned here so all downstream code (Index/Partitions/Foreign)
	// automatically operates only on real tables.
	var viewEntries []string
	dtabS, viewEntries = splitTableViewEntries(dtabS, stcls.objectKinds, stcls.caseSensitiveObjectName)

	fmt.Println("gt-checksum: Checking table structure")
	vlog = fmt.Sprintf("(%d) %s checking table structure of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)
	// Snapshot measuredDataPods length before TableColumnNameCheck so we can
	// identify pods it appends directly (missing-table entries).  Those tables
	// must not receive a second Pod from the loop below.
	podCountBeforeCheck := len(measuredDataPods)
	normal, abnormal, err := stcls.TableColumnNameCheck(dtabS, logThreadSeq, logThreadSeq2)
	if err != nil {
		return err
	}
	vlog = fmt.Sprintf("(%d) %s Table structure and column checksum of srcDB and dstDB completed. The consistent result is {%s}(num [%d]), and the inconsistent result is {%s}(num [%d])", logThreadSeq, event, normal, len(normal), abnormal, len(abnormal))
	global.Wlog.Debug(vlog)
	// Pre-populate existingTableKeys from pods that TableColumnNameCheck already
	// appended (e.g. both-missing or source-missing tables).  This prevents the
	// append(normal, abnormal...) loop below from creating duplicate Pod entries
	// while still allowing abnormalTableList to carry its data-mode preflight count.
	// NOTE: This guard is effective only when stcls.aggregate == false (the current
	// production path for Struct()).  When aggregate is true, appendPod writes to
	// stcls.podsBuffer instead of measuredDataPods, so the slice delta is empty and
	// the guard has no effect.  That path does not currently call Struct(), so the
	// limitation is latent rather than actively triggered.
	for _, p := range measuredDataPods[podCountBeforeCheck:] {
		existingTableKeys[fmt.Sprintf("%s.%s", p.Schema, p.Table)] = true
	}

	// 初始化表结构差异映射
	for _, i := range dtabS {
		var sourceSchema, tableName string

		// 处理映射格式 schema.table:schema.table
		if strings.Contains(i, ":") {
			parts := strings.Split(i, ":")
			if len(parts) == 2 {
				sourceParts := strings.Split(parts[0], ".")
				if len(sourceParts) == 2 {
					sourceSchema = sourceParts[0]
					tableName = sourceParts[1]
				}
			}
		} else {
			// 处理普通格式 schema.table
			parts := strings.Split(i, ".")
			if len(parts) == 2 {
				sourceSchema = parts[0]
				tableName = parts[1]
			}
		}

		// 将表结构差异初始化为false（表示一致）
		tableKey := fmt.Sprintf("%s.%s", sourceSchema, tableName)
		tableStructDiffs[tableKey] = false

		// 如果表在abnormal列表中，则标记为不一致
		for _, abnormalTable := range abnormal {
			// 确保完全匹配表名，包括schema
			if abnormalTable == fmt.Sprintf("%s.%s", sourceSchema, tableName) {
				tableStructDiffs[tableKey] = true
				break
			}
		}
	}

	// 处理正常表和异常表，创建Pod实例
	for _, i := range append(normal, abnormal...) {
		aa := strings.Split(i, ".")
		destSchema := aa[0]
		tableName := aa[1]

		// 查找源端schema
		sourceSchema := destSchema
		for src, dst := range stcls.tableMappings {
			if dst == destSchema {
				sourceSchema = src
				break
			}
		}

		// 构建表的唯一键
		tableKey := fmt.Sprintf("%s.%s", sourceSchema, tableName)

		// 检查该表是否已在skipIndexCheckTables中（表示已被特殊处理过）
		isProcessed := false
		destTableKey := fmt.Sprintf("%s.%s", destSchema, tableName)
		for _, skipTable := range stcls.skipIndexCheckTables {
			if skipTable == destTableKey {
				isProcessed = true
				break
			}
		}

		// 如果表已经被处理过，或者已经添加过Pod记录，则跳过
		if !isProcessed && !existingTableKeys[tableKey] {
			// 为每个表创建新的Pod实例
			pods := Pod{
				Datafix:     stcls.datafix,
				CheckObject: "struct",
				Schema:      sourceSchema,
				Table:       tableName,
				DIFFS:       global.SkipDiffsNo,
			}

			// 如果表在abnormal列表中，则标记为不一致
			for _, abnormalTable := range abnormal {
				if abnormalTable == i {
					pods.DIFFS = global.SkipDiffsYes
					break
				}
			}
			if pods.DIFFS == global.SkipDiffsNo && stcls.structWarnOnlyDiffsMap[tableKey] {
				pods.DIFFS = global.SkipDiffsWarnOnly
			}
			if pods.DIFFS == global.SkipDiffsNo && stcls.structCollationMappedMap[tableKey] {
				pods.DIFFS = global.SkipDiffsCollationMapped
			}

			// 设置映射信息
			if sourceSchema != destSchema {
				// 记录映射关系到全局变量
				mappingRelation := fmt.Sprintf("%s.%s:%s.%s", sourceSchema, tableName, destSchema, tableName)
				exists := false
				for _, existingMapping := range TableMappingRelations {
					if existingMapping == mappingRelation {
						exists = true
						break
					}
				}
				if !exists {
					TableMappingRelations = append(TableMappingRelations, mappingRelation)
				}

				// 设置映射信息
				pods.MappingInfo = fmt.Sprintf("Schema: %s:%s", sourceSchema, destSchema)
			}

			measuredDataPods = append(measuredDataPods, pods)
			// 标记该表已添加Pod记录
			existingTableKeys[tableKey] = true
		}
	}

	// 创建一个自定义的结构体，用于在Index、Partitions和Foreign函数中捕获不一致的表
	type structDiffCollector struct {
		diffs map[string]bool
	}

	collector := &structDiffCollector{
		diffs: tableStructDiffs,
	}

	// 2. 执行索引校验 (原来的 Index 函数)
	fmt.Println("gt-checksum: Checking table indexes")
	vlog = fmt.Sprintf("(%d) %s checking table indexes of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)

	// 初始化索引差异映射
	stcls.indexDiffsMap = make(map[string]bool)

	// 调用Index函数进行索引校验
	fmt.Println("gt-checksum: Checking table indexes")
	vlog = fmt.Sprintf("(%d) %s checking table indexes of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)

	// 调用原始的Index函数
	if err := stcls.Index(dtabS, logThreadSeq, logThreadSeq2, true); err != nil {
		return err
	}

	// 使用indexDiffsMap更新collector.diffs
	for tableKey, hasDiff := range stcls.indexDiffsMap {
		if hasDiff {
			// 只更新存在于映射中的表
			if _, exists := collector.diffs[tableKey]; exists {
				collector.diffs[tableKey] = true
				vlog = fmt.Sprintf("(%d) Index check found differences for table %s",
					logThreadSeq, tableKey)
				global.Wlog.Debug(vlog)
			}
		}
	}

	// 3. 执行分区校验 (原来的 Partitions 函数)
	fmt.Println("gt-checksum: Checking table partitions")
	vlog = fmt.Sprintf("(%d) %s checking table partitions of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)

	// 3. 执行分区校验 (原来的 Partitions 函数)
	fmt.Println("gt-checksum: Checking table partitions")
	vlog = fmt.Sprintf("(%d) %s checking table partitions of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)

	// 初始化全局分区差异映射
	stcls.partitionDiffsMap = make(map[string]bool)
	vlog = fmt.Sprintf("(%d) %s Starting partitions check for %d tables, will query INFORMATION_SCHEMA.PARTITIONS for each table", logThreadSeq, event, len(dtabS))
	global.Wlog.Debug(vlog)

	// 调用Partitions函数进行分区检查，会查询INFORMATION_SCHEMA.PARTITIONS表
	stcls.Partitions(dtabS, logThreadSeq, logThreadSeq2, true)
	vlog = fmt.Sprintf("(%d) %s Completed partitions check, results: %v", logThreadSeq, event, stcls.partitionDiffsMap)
	global.Wlog.Debug(vlog)

	// 使用全局partitionDiffsMap更新collector.diffs
	vlog = fmt.Sprintf("(%d) Processing partition diffs map with %d entries: %v", logThreadSeq, len(stcls.partitionDiffsMap), stcls.partitionDiffsMap)
	global.Wlog.Debug(vlog)
	for tableKey, hasDiff := range stcls.partitionDiffsMap {
		vlog = fmt.Sprintf("(%d) Checking partition diff for table %s: %v", logThreadSeq, tableKey, hasDiff)
		global.Wlog.Debug(vlog)
		if hasDiff {
			// 尝试直接使用tableKey更新
			if _, exists := collector.diffs[tableKey]; exists {
				collector.diffs[tableKey] = true
				vlog = fmt.Sprintf("(%d) Partitions check found differences for table %s, updated diffs map",
					logThreadSeq, tableKey)
				global.Wlog.Debug(vlog)
			} else {
				// 如果直接匹配失败，尝试清理表名格式后匹配（移除可能的后缀）
				cleanTableKey := tableKey
				if strings.Contains(tableKey, ":") {
					parts := strings.Split(tableKey, ":")
					cleanTableKey = parts[0]
				}
				if _, exists := collector.diffs[cleanTableKey]; exists {
					collector.diffs[cleanTableKey] = true
					vlog = fmt.Sprintf("(%d) Partitions check found differences for table %s (cleaned to %s), updated diffs map",
						logThreadSeq, tableKey, cleanTableKey)
					global.Wlog.Debug(vlog)
				} else {
					vlog = fmt.Sprintf("(%d) Partitions diff found for table %s, but no matching entry in diffs map",
						logThreadSeq, tableKey)
					global.Wlog.Debug(vlog)
				}
			}
		}
	}

	// 4. 执行外键校验 (原来的 Foreign 函数)
	fmt.Println("gt-checksum: Checking table foreign keys")
	vlog = fmt.Sprintf("(%d) %s checking table foreign keys of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)

	// 初始化全局外键差异映射
	stcls.foreignKeyDiffsMap = make(map[string]bool)

	// 修改Foreign函数，使其能够存储检查结果
	stcls.Foreign(dtabS, logThreadSeq, logThreadSeq2, true)

	// 使用全局foreignKeyDiffsMap更新collector.diffs
	for tableKey, hasDiff := range stcls.foreignKeyDiffsMap {
		if hasDiff {
			// 只更新存在于映射中的表
			if _, exists := collector.diffs[tableKey]; exists {
				collector.diffs[tableKey] = true
				vlog = fmt.Sprintf("(%d) Foreign key check found differences for table %s",
					logThreadSeq, tableKey)
				global.Wlog.Debug(vlog)
			}
		}
	}

	// 添加调试日志，输出所有表的结构差异状态
	vlog = fmt.Sprintf("(%d) Table structure differences map: %v", logThreadSeq, collector.diffs)
	global.Wlog.Debug(vlog)

	// 更新struct记录的DIFFS状态
	for i, pod := range measuredDataPods {
		if pod.CheckObject == "struct" {
			// 使用完整的schema.table作为键
			tableKey := fmt.Sprintf("%s.%s", pod.Schema, pod.Table)

			// 检查这个特定的表是否在差异映射中
			isDifferent, exists := collector.diffs[tableKey]
			hasWarnOnly := stcls.structWarnOnlyDiffsMap[tableKey]
			hasCollationMapped := stcls.structCollationMappedMap[tableKey]

			vlog = fmt.Sprintf("(%d) Checking table %s.%s, current DIFFS=%s, in diff map: %v, exists: %v, warnOnly: %v, collationMapped: %v",
				logThreadSeq, pod.Schema, pod.Table, pod.DIFFS, isDifferent, exists, hasWarnOnly, hasCollationMapped)
			global.Wlog.Debug(vlog)

			// 先应用硬差异，再应用纯风险告警，最后应用 collation-mapped
			if exists && isDifferent {
				measuredDataPods[i].DIFFS = mergeStructDiffState(measuredDataPods[i].DIFFS, global.SkipDiffsYes)
				vlog = fmt.Sprintf("(%d) Table %s.%s has structure differences, setting DIFFS to yes",
					logThreadSeq, pod.Schema, pod.Table)
				global.Wlog.Debug(vlog)
			} else if hasWarnOnly {
				measuredDataPods[i].DIFFS = mergeStructDiffState(measuredDataPods[i].DIFFS, global.SkipDiffsWarnOnly)
				vlog = fmt.Sprintf("(%d) Table %s.%s only has warn-only structure risks, setting DIFFS to warn-only",
					logThreadSeq, pod.Schema, pod.Table)
				global.Wlog.Debug(vlog)
			} else if hasCollationMapped {
				measuredDataPods[i].DIFFS = mergeStructDiffState(measuredDataPods[i].DIFFS, global.SkipDiffsCollationMapped)
				vlog = fmt.Sprintf("(%d) Table %s.%s has cross-platform collation mapping only, setting DIFFS to collation-mapped",
					logThreadSeq, pod.Schema, pod.Table)
				global.Wlog.Debug(vlog)
			}
		}
	}

	fmt.Println("gt-checksum: Table structure verification completed")
	vlog = fmt.Sprintf("(%d) %s check source and target DB table struct complete", logThreadSeq, event)
	global.Wlog.Info(vlog)

	// 5. Process any VIEW entries that were split off at the top.
	if err := stcls.checkViewStruct(viewEntries, logThreadSeq, logThreadSeq2); err != nil {
		return err
	}

	return nil
}

func rewriteCreateTableTargetIdentifier(createTableStmt, destSchema, destTable string) string {
	matches := createTableTargetIdentifierPattern.FindStringSubmatch(createTableStmt)
	if len(matches) == 0 {
		return createTableStmt
	}
	return createTableTargetIdentifierPattern.ReplaceAllString(createTableStmt, fmt.Sprintf("${1}`%s`.`%s`", destSchema, destTable))
}

func generateCreateTableSql(sourceDB *sql.DB, sourceSchema string, destSchema string, tableName string, destTable string, sourceVersion, destVersion global.MySQLVersionInfo, mariaDBJSONTargetType string, logThreadSeq int64) (string, error) {
	var (
		vlog  string
		event = "generateCreateTableSql"
	)

	// 查询源表的完整DDL，包括AUTO_INCREMENT, TABLE_COLLATION, CREATE_OPTIONS, TABLE_COMMENT等属性
	showCreateTableQuery := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", sourceSchema, tableName)
	var tableName2, createTableStmt string
	err := sourceDB.QueryRow(showCreateTableQuery).Scan(&tableName2, &createTableStmt)
	if err != nil {
		vlog = fmt.Sprintf("(%d) %s Error getting CREATE TABLE statement for %s.%s: %v", logThreadSeq, event, sourceSchema, tableName, err)
		global.Wlog.Error(vlog)
		return "", err
	}

	// 添加IF NOT EXISTS前缀
	if !strings.Contains(strings.ToUpper(createTableStmt), "IF NOT EXISTS") {
		// 查找"CREATE TABLE"后的位置，并在其后添加"IF NOT EXISTS"
		createTableIndex := strings.Index(strings.ToUpper(createTableStmt), "CREATE TABLE")
		if createTableIndex != -1 {
			// 找到"CREATE TABLE"之后的位置
			afterCreateTable := createTableIndex + len("CREATE TABLE")
			// 在"CREATE TABLE"之后插入" IF NOT EXISTS"
			createTableStmt = createTableStmt[:afterCreateTable] + " IF NOT EXISTS" + createTableStmt[afterCreateTable:]
		}
	}

	createTableStmt = rewriteCreateTableTargetIdentifier(createTableStmt, destSchema, destTable)

	// 确保CREATE TABLE语句包含表级别的字符集和排序规则
	// 查询表的字符集和排序规则
	tableCharsetCollationQuery := fmt.Sprintf(`
		SELECT t.TABLE_COLLATION, c.CHARACTER_SET_NAME, t.AUTO_INCREMENT, t.CREATE_OPTIONS, t.TABLE_COMMENT
		FROM information_schema.TABLES t 
		JOIN information_schema.COLLATIONS c ON t.TABLE_COLLATION = c.COLLATION_NAME 
		WHERE t.TABLE_SCHEMA = '%s' AND t.TABLE_NAME = '%s'
	`, sourceSchema, tableName)

	var tableCollation, tableCharset string
	var autoIncrement sql.NullInt64
	var createOptions, tableComment string
	err = sourceDB.QueryRow(tableCharsetCollationQuery).Scan(&tableCollation, &tableCharset, &autoIncrement, &createOptions, &tableComment)
	if err != nil {
		vlog = fmt.Sprintf("(%d) %s Error getting table properties for %s.%s: %v", logThreadSeq, event, sourceSchema, tableName, err)
		global.Wlog.Error(vlog)
		// 即使获取表属性失败，我们仍然可以继续使用原始的CREATE TABLE语句
		return createTableStmt, nil
	}

	// 检查CREATE TABLE语句是否已经包含字符集和排序规则定义
	hasCharset := strings.Contains(strings.ToUpper(createTableStmt), "CHARACTER SET") || strings.Contains(strings.ToUpper(createTableStmt), "CHARSET")
	hasCollation := strings.Contains(strings.ToUpper(createTableStmt), "COLLATE")

	// 如果没有包含字符集和排序规则，添加它们
	if !hasCharset && !hasCollation && tableCharset != "" && tableCollation != "" {
		// 在语句末尾添加字符集和排序规则定义
		// 通常CREATE TABLE语句以ENGINE=xxx结尾，我们需要在这之后添加字符集和排序规则
		if strings.Contains(createTableStmt, "ENGINE=") {
			parts := strings.SplitN(createTableStmt, "ENGINE=", 2)
			if len(parts) == 2 {
				enginePart := parts[1]
				endIndex := strings.Index(enginePart, ";")
				if endIndex != -1 {
					// 在分号前添加字符集和排序规则定义
					createTableStmt = parts[0] + "ENGINE=" + enginePart[:endIndex] +
						fmt.Sprintf(" CHARACTER SET %s COLLATE %s", tableCharset, tableCollation) +
						enginePart[endIndex:]
				} else {
					// 如果没有分号，直接在末尾添加
					createTableStmt = createTableStmt +
						fmt.Sprintf(" CHARACTER SET %s COLLATE %s", tableCharset, tableCollation)
				}
			}
		} else {
			// 如果没有ENGINE=，直接在末尾添加（去掉最后的分号，然后再加上）
			if strings.HasSuffix(createTableStmt, ";") {
				createTableStmt = createTableStmt[:len(createTableStmt)-1] +
					fmt.Sprintf(" CHARACTER SET %s COLLATE %s;", tableCharset, tableCollation)
			} else {
				createTableStmt = createTableStmt +
					fmt.Sprintf(" CHARACTER SET %s COLLATE %s;", tableCharset, tableCollation)
			}
		}
	}

	// 确保AUTO_INCREMENT值被正确设置
	if autoIncrement.Valid && autoIncrement.Int64 > 0 {
		// 检查CREATE TABLE语句是否已经包含AUTO_INCREMENT定义
		hasAutoIncrement := strings.Contains(strings.ToUpper(createTableStmt), "AUTO_INCREMENT")

		if !hasAutoIncrement {
			// 在语句末尾添加AUTO_INCREMENT定义
			if strings.HasSuffix(createTableStmt, ";") {
				createTableStmt = createTableStmt[:len(createTableStmt)-1] +
					fmt.Sprintf(" AUTO_INCREMENT=%d;", autoIncrement.Int64)
			} else {
				createTableStmt = createTableStmt +
					fmt.Sprintf(" AUTO_INCREMENT=%d;", autoIncrement.Int64)
			}
		}
	}

	// 确保表注释被正确设置
	if tableComment != "" && !strings.Contains(strings.ToUpper(createTableStmt), "COMMENT") {
		// 在语句末尾添加表注释
		if strings.HasSuffix(createTableStmt, ";") {
			createTableStmt = createTableStmt[:len(createTableStmt)-1] +
				fmt.Sprintf(" COMMENT='%s';", strings.Replace(tableComment, "'", "\\'", -1))
		} else {
			createTableStmt = createTableStmt +
				fmt.Sprintf(" COMMENT='%s';", strings.Replace(tableComment, "'", "\\'", -1))
		}
	}

	vlog = fmt.Sprintf("(%d) %s Generated CREATE TABLE statement for %s.%s with charset %s and collation %s",
		logThreadSeq, event, destSchema, destTable, tableCharset, tableCollation)
	global.Wlog.Debug(vlog)

	// 确保SQL语句末尾有分号
	if !strings.HasSuffix(createTableStmt, ";") {
		createTableStmt = createTableStmt + ";"
	}

	rewriteNeeded := schemacompat.ShouldRewriteMariaDBCreateTable(createTableStmt, sourceVersion, destVersion)
	if rewriteNeeded {
		beforeRewrite := createTableStmt
		createTableStmt = schemacompat.ConvertMariaDBCreateTableToMySQL(createTableStmt, sourceVersion, destVersion, mariaDBJSONTargetType)
		global.Wlog.Debug(fmt.Sprintf("(%d) %s MariaDB CREATE TABLE rewrite applied for %s.%s: sourceFlavor=%s targetFlavor=%s changed=%t",
			logThreadSeq,
			event,
			destSchema,
			destTable,
			sourceVersion.FlavorName(),
			destVersion.FlavorName(),
			beforeRewrite != createTableStmt,
		))
		if !strings.HasSuffix(createTableStmt, ";") {
			createTableStmt = createTableStmt + ";"
		}
	}

	return createTableStmt, nil
}

// injectMyRowIDIntoCreateTable 在 CREATE TABLE 语句中注入 my_row_id 列定义和 PRIMARY KEY 约束
// 如果需要添加 my_row_id，则在最后一列后、PRIMARY KEY/UNIQUE KEY/KEY/ENGINE 之前插入列定义和主键约束
func injectMyRowIDIntoCreateTable(createTableStmt string, destDB *sql.DB, destSchema, destTable, requirePK string, logThreadSeq int64) (string, error) {
	// 导入 mysql 包的函数
	shouldAdd, err := mysql.ShouldAddMyRowID(destDB, destSchema, destTable, requirePK, logThreadSeq)
	if err != nil {
		vlog := fmt.Sprintf("(%d) Error checking if should add my_row_id for %s.%s: %v", logThreadSeq, destSchema, destTable, err)
		global.Wlog.Error(vlog)
		return createTableStmt, err
	}

	if !shouldAdd {
		// 不需要添加 my_row_id
		return createTableStmt, nil
	}

	// 使用正则表达式在最后一列后插入 my_row_id 定义和 PRIMARY KEY 约束
	// 匹配模式：最后一列定义后的位置（在 PRIMARY KEY/UNIQUE KEY/KEY/ENGINE 之前）
	// 注意：需要处理多种情况：
	// 1. ) ENGINE=...
	// 2. , PRIMARY KEY (...)
	// 3. , UNIQUE KEY ...
	// 4. , KEY ...
	pattern := regexp.MustCompile(`(,\s*` + "`" + `[^` + "`" + `]+` + "`" + `[^,)]+)\s*(\)\s*ENGINE|\)\s*$|,\s*PRIMARY\s+KEY|,\s*UNIQUE\s+KEY|,\s*KEY)`)

	// 检查是否匹配
	if !pattern.MatchString(createTableStmt) {
		// 如果没有匹配，尝试简单的模式：在最后的 ) ENGINE 之前插入
		simplePattern := regexp.MustCompile(`\)\s*(ENGINE|DEFAULT|AUTO_INCREMENT|COMMENT)`)
		if simplePattern.MatchString(createTableStmt) {
			replacement := ",\n  `my_row_id` bigint unsigned NOT NULL AUTO_INCREMENT /*!80023 INVISIBLE */,\n  PRIMARY KEY (`my_row_id`)\n) $1"
			createTableStmt = simplePattern.ReplaceAllString(createTableStmt, replacement)
		} else {
			// 如果仍然没有匹配，记录警告并返回原始语句
			vlog := fmt.Sprintf("(%d) Warning: Cannot inject my_row_id into CREATE TABLE for %s.%s: pattern not matched", logThreadSeq, destSchema, destTable)
			global.Wlog.Warn(vlog)
			return createTableStmt, nil
		}
	} else {
		// 正常匹配，插入 my_row_id 和 PRIMARY KEY
		replacement := "$1,\n  `my_row_id` bigint unsigned NOT NULL AUTO_INCREMENT /*!80023 INVISIBLE */,\n  PRIMARY KEY (`my_row_id`)$2"
		createTableStmt = pattern.ReplaceAllString(createTableStmt, replacement)
	}

	vlog := fmt.Sprintf("(%d) Injected my_row_id column and PRIMARY KEY into CREATE TABLE for %s.%s", logThreadSeq, destSchema, destTable)
	global.Wlog.Debug(vlog)

	return createTableStmt, nil
}
