package actions

import (
	"fmt"
	mysql "gt-checksum/MySQL"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func indexColumnsOnlyDifferInCase(sourceColumns, destColumns []string) bool {
	if len(sourceColumns) != len(destColumns) {
		return false
	}
	for i := range sourceColumns {
		if !strings.EqualFold(sourceColumns[i], destColumns[i]) {
			return false
		}
	}
	return true
}

func mergeIndexVisibilityHints(base map[string]string, hints map[string]string) map[string]string {
	if len(base) == 0 && len(hints) == 0 {
		return map[string]string{}
	}
	merged := make(map[string]string, len(base)+len(hints))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range hints {
		merged[k] = v
	}
	return merged
}

func isInvisibleLikeIndexVisibility(visibility string) bool {
	return strings.EqualFold(visibility, "NO") || strings.EqualFold(visibility, "INVISIBLE") || strings.EqualFold(visibility, "IGNORED")
}

func (stcls *schemaTable) tableIndexAlgorithm(indexType map[string][]string) (string, []string) {
	if len(indexType) > 0 {
		// 优先选择主键索引
		if len(indexType["pri_single"]) > 0 {
			return "pri_single", indexType["pri_single"]
		}
		if len(indexType["pri_multi"]) > 0 {
			return "pri_multi", indexType["pri_multi"]
		}
		if len(indexType["pri_multiseriate"]) > 0 {
			return "pri_multiseriate", indexType["pri_multiseriate"]
		}

		// 其次选择唯一索引
		if len(indexType["uni_single"]) > 0 {
			return "uni_single", indexType["uni_single"]
		}
		if len(indexType["uni_multi"]) > 0 {
			return "uni_multi", indexType["uni_multi"]
		}
		if len(indexType["uni_multiseriate"]) > 0 {
			return "uni_multiseriate", indexType["uni_multiseriate"]
		}

		// 最后选择普通索引
		if len(indexType["mul_single"]) > 0 {
			return "mul_single", indexType["mul_single"]
		}
		if len(indexType["mul_multi"]) > 0 {
			return "mul_multi", indexType["mul_multi"]
		}
		if len(indexType["mul_multiseriate"]) > 0 {
			return "mul_multiseriate", indexType["mul_multiseriate"]
		}
	}
	return "", []string{}
}

func (stcls *schemaTable) TableIndexColumn(dtabS []string, logThreadSeq, logThreadSeq2 int64) map[string][]string {
	var (
		vlog                string
		tableIndexColumnMap = make(map[string][]string)
	)
	vlog = fmt.Sprintf("(%d) Start to query the table index listing information and select the appropriate index ...", logThreadSeq)
	global.Wlog.Info(vlog)

	// 添加调试日志，查看传入的表列表和映射规则
	vlog = fmt.Sprintf("TableIndexColumn received dtabS: %v", dtabS)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("Current table mappings: %v", stcls.tableMappings)
	global.Wlog.Debug(vlog)

	workers := stcls.tableIndexMetaWorkerCount(len(dtabS))
	vlog = fmt.Sprintf("(%d) TableIndexColumn worker pool size: %d", logThreadSeq, workers)
	global.Wlog.Debug(vlog)

	// Oracle 源端：一次性批量拉取全部 schema 的索引元数据，避免对每张表各发一次
	// ALL_* 4-way JOIN（Oracle 11g 单次 ~1s，N 张表并行 4 worker 仍需 N/4 秒）。
	var sourceOracleIndexCache map[string]map[string][]map[string]interface{}
	if isOracleDrive(stcls.sourceDrive) {
		schemasSet := make(map[string]struct{}, len(dtabS))
		for _, entry := range dtabS {
			srcSchema, _, _, _, ok := parseSchemaTableMappingEntry(entry)
			if ok && srcSchema != "" {
				schemasSet[strings.ToUpper(srcSchema)] = struct{}{}
			}
		}
		schemas := make([]string, 0, len(schemasSet))
		for s := range schemasSet {
			schemas = append(schemas, s)
		}
		sourceOracleIndexCache = preloadOracleIndexRows(stcls.sourceDB, schemas, logThreadSeq2)
	}

	type tableIndexJob struct {
		rawEntry     string
		sourceSchema string
		sourceTable  string
		destSchema   string
		destTable    string
	}

	jobs := make(chan tableIndexJob, len(dtabS))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				startAt := time.Now()
				logMsg := fmt.Sprintf("Processing table entry: %s", job.rawEntry)
				global.Wlog.Debug(logMsg)

				logMsg = fmt.Sprintf("Parsed mapping: sourceSchema=%s, sourceTable=%s, destSchema=%s, destTable=%s",
					job.sourceSchema, job.sourceTable, job.destSchema, job.destTable)
				global.Wlog.Debug(logMsg)

				logMsg = fmt.Sprintf("(%d) Start querying source index metadata for table %s.%s (target mapping %s.%s)",
					logThreadSeq, job.sourceSchema, job.sourceTable, job.destSchema, job.destTable)
				global.Wlog.Debug(logMsg)

				idxc := dbExec.IndexColumnStruct{Schema: job.sourceSchema, Table: job.sourceTable, Drivce: stcls.sourceDrive, CaseSensitiveObjectName: stcls.caseSensitiveObjectName}
				var queryData []map[string]interface{}
				if cached, ok := lookupOracleIndexRows(sourceOracleIndexCache, job.sourceSchema, job.sourceTable); ok {
					queryData = cached
				} else {
					var err error
					queryData, err = idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
					if err != nil {
						logMsg = fmt.Sprintf("(%d) Error querying source table index for %s.%s: %v", logThreadSeq, job.sourceSchema, job.sourceTable, err)
						global.Wlog.Error(logMsg)
						continue
					}
				}

				tc := dbExec.TableColumnNameStruct{Schema: job.sourceSchema, Table: job.sourceTable, Drive: stcls.sourceDrive, Db: stcls.sourceDB}
				indexType := tc.Query().TableIndexChoice(queryData, logThreadSeq2)
				logMsg = fmt.Sprintf("(%d) Source table %s.%s index list information query completed. index list message is {%v}",
					logThreadSeq, job.sourceSchema, job.sourceTable, indexType)
				global.Wlog.Debug(logMsg)

				displayTableName := fmt.Sprintf("%s.%s:%s.%s", job.sourceSchema, job.sourceTable, job.destSchema, job.destTable)

				if len(indexType) == 0 {
					key := fmt.Sprintf("%s/*gtchecksumSchemaTable*/%s/*mapping*/%s/*mappingTable*/%s",
						job.sourceSchema, job.sourceTable, job.destSchema, job.destTable)
					mu.Lock()
					tableIndexColumnMap[key] = []string{}
					mu.Unlock()

					logMsg = fmt.Sprintf("(%d) The source table %s has no index.", logThreadSeq, displayTableName)
					global.Wlog.Warn(logMsg)
				} else {
					logMsg = fmt.Sprintf("(%d) Start to perform index selection on source table %s.%s according to the algorithm",
						logThreadSeq, job.sourceSchema, job.sourceTable)
					global.Wlog.Debug(logMsg)

					ab, aa := stcls.tableIndexAlgorithm(indexType)
					key := fmt.Sprintf("%s/*gtchecksumSchemaTable*/%s/*indexColumnType*/%s/*mapping*/%s/*mappingTable*/%s",
						job.sourceSchema, job.sourceTable, ab, job.destSchema, job.destTable)
					mu.Lock()
					tableIndexColumnMap[key] = aa
					mu.Unlock()

					logMsg = fmt.Sprintf("(%d) The index selection of source table %s is completed, and the selected index information is { keyName:%s keyColumn: %s}",
						logThreadSeq, displayTableName, ab, aa)
					global.Wlog.Debug(logMsg)
				}

				logMsg = fmt.Sprintf("(%d) Source index metadata phase completed for %s in %s", logThreadSeq, displayTableName, time.Since(startAt).Round(time.Millisecond))
				global.Wlog.Debug(logMsg)
			}
		}()
	}

	seen := make(map[string]struct{}, len(dtabS))
	for _, entry := range dtabS {
		sourceSchema, sourceTable, destSchema, destTable, ok := parseSchemaTableMappingEntry(entry)
		if !ok {
			vlog = fmt.Sprintf("Skip invalid table entry in TableIndexColumn: %s", entry)
			global.Wlog.Warn(vlog)
			continue
		}

		uniqueKey := fmt.Sprintf("%s.%s:%s.%s", sourceSchema, sourceTable, destSchema, destTable)
		if _, exists := seen[uniqueKey]; exists {
			continue
		}
		seen[uniqueKey] = struct{}{}

		jobs <- tableIndexJob{
			rawEntry:     entry,
			sourceSchema: sourceSchema,
			sourceTable:  sourceTable,
			destSchema:   destSchema,
			destTable:    destTable,
		}
	}
	close(jobs)
	wg.Wait()

	vlog = fmt.Sprintf("(%d) Table index listing information and appropriate index completion", logThreadSeq)
	global.Wlog.Info(vlog)
	return tableIndexColumnMap
}

func (stcls *schemaTable) tableIndexMetaWorkerCount(tableCount int) int {
	if tableCount <= 1 {
		return 1
	}

	workers := stcls.checkRules.ParallelThds
	if workers <= 0 {
		workers = 4
	}
	if workers < 2 {
		workers = 2
	}
	if workers > 8 {
		workers = 8
	}
	if workers > tableCount {
		workers = tableCount
	}

	return workers
}

func (stcls *schemaTable) Index(dtabS []string, logThreadSeq, logThreadSeq2 int64, isCalledFromStruct ...bool) error {
	var (
		vlog  string
		sqlS  []string
		aa    = &CheckSumTypeStruct{}
		event string
		// 辅助函数：提取列名和序号
		extractColumnInfo = func(columnStr string) (string, int) {
			// 从格式 "columnName/*seq*/1/*type*/columnType" 中提取信息
			parts := strings.Split(columnStr, "/*seq*/")
			// 保留原始列名大小写
			colName := strings.TrimSpace(parts[0])
			seqStr := strings.Split(parts[1], "/*type*/")[0]
			seq, _ := strconv.Atoi(seqStr)

			return colName, seq
		}

		// 辅助函数：按序号排序列并返回纯列名（仅用于大小写比较等不需要前缀的场景）
		sortColumns = func(columns []string) []string {
			type ColumnInfo struct {
				name string
				seq  int
			}
			var columnInfos []ColumnInfo

			// 提取列信息
			for _, col := range columns {
				name, seq := extractColumnInfo(col)
				columnInfos = append(columnInfos, ColumnInfo{name: name, seq: seq})
			}

			// 按序号排序
			sort.Slice(columnInfos, func(i, j int) bool {
				return columnInfos[i].seq < columnInfos[j].seq
			})

			// 返回排序后的纯列名
			var result []string
			for _, col := range columnInfos {
				result = append(result, fmt.Sprintf("%s", col.name))
			}
			return result
		}

		// 辅助函数：按序号排序列，返回可直接用于 DDL 的带引号列表达式（含前缀长度）。
		// token 格式：colName/*seq*/N/*type*/T/*prefix*/P
		// 函数索引 token 格式：/*expr*/EXPRESSION/*seq*/N/*type*//*prefix*/0
		// 旧格式（无 /*prefix*/）兼容处理：prefix 视为 0。
		quoteColumnWithPrefix = func(token string) string {
			// 函数索引 token 以 /*expr*/ 开头，返回带括号的表达式（MySQL DDL 要求）
			if strings.HasPrefix(token, "/*expr*/") {
				rest := strings.TrimPrefix(token, "/*expr*/")
				var expr string
				if seqIdx := strings.Index(rest, "/*seq*/"); seqIdx >= 0 {
					expr = strings.TrimSpace(rest[:seqIdx])
				} else {
					expr = strings.TrimSpace(rest)
				}
				// MySQL 函数索引 DDL 必须用括号包裹表达式：ADD INDEX idx((expr))
				if !strings.HasPrefix(expr, "(") {
					expr = "(" + expr + ")"
				}
				return expr
			}
			colName := strings.TrimSpace(token)
			prefix := 0
			if seqParts := strings.Split(token, "/*seq*/"); len(seqParts) == 2 {
				colName = strings.TrimSpace(seqParts[0])
				if typeParts := strings.Split(seqParts[1], "/*type*/"); len(typeParts) == 2 {
					if prefixParts := strings.Split(typeParts[1], "/*prefix*/"); len(prefixParts) == 2 {
						if n, err := strconv.Atoi(strings.TrimSpace(prefixParts[1])); err == nil {
							prefix = n
						}
					}
				}
			}
			quoted := fmt.Sprintf("`%s`", strings.ReplaceAll(colName, "`", "``"))
			if prefix > 0 {
				return fmt.Sprintf("%s(%d)", quoted, prefix)
			}
			return quoted
		}

		sortColumnsPreservingPrefix = func(columns []string) []string {
			type ColumnInfo struct {
				expr string
				seq  int
			}
			var columnInfos []ColumnInfo
			for _, col := range columns {
				_, seq := extractColumnInfo(col)
				columnInfos = append(columnInfos, ColumnInfo{expr: quoteColumnWithPrefix(col), seq: seq})
			}
			sort.Slice(columnInfos, func(i, j int) bool {
				return columnInfos[i].seq < columnInfos[j].seq
			})
			var result []string
			for _, col := range columnInfos {
				result = append(result, col.expr)
			}
			return result
		}

		constraintNameKey = func(name string) string {
			if stcls.caseSensitiveObjectName == "no" {
				return strings.ToUpper(name)
			}
			return name
		}

		indexGenerate = func(smu, dmu map[string][]string, a *CheckSumTypeStruct, indexType string, sourceVisibilityMap, destVisibilityMap map[string]string) []string {
			var cc, c, d []string

			// 根据映射规则确定目标端schema
			destSchema := stcls.schema
			if mappedSchema, exists := stcls.tableMappings[stcls.schema]; exists {
				destSchema = mappedSchema
			}

			dbf := dbExec.DataAbnormalFixStruct{
				Schema:                  destSchema, // 使用目标端schema
				Table:                   stcls.table,
				SourceDevice:            stcls.sourceDrive,
				DestDevice:              stcls.destDrive,
				IndexType:               indexType,
				DatafixType:             stcls.datafix,
				SourceSchema:            stcls.schema,                   // 添加源端schema
				CaseSensitiveObjectName: stcls.caseSensitiveObjectName,  // 传递是否区分对象名大小写
				IndexVisibilityMap:      sourceVisibilityMap,            // 传递索引可见性信息
				DestFlavor:              stcls.destVersionInfo().Flavor, // 用于生成兼容目标端语法的 fix SQL
			}

			sourceCanonicalIndexes := schemacompat.CanonicalizeMySQLIndexes(smu, sourceVisibilityMap)
			destCanonicalIndexes := schemacompat.CanonicalizeMySQLIndexes(dmu, destVisibilityMap)
			sourceCanonicalByName := make(map[string]schemacompat.CanonicalIndex)
			destCanonicalByName := make(map[string]schemacompat.CanonicalIndex)
			sourceCanonicalConstraints := make(map[string]schemacompat.CanonicalConstraint)
			destCanonicalConstraints := make(map[string]schemacompat.CanonicalConstraint)
			for _, idx := range sourceCanonicalIndexes {
				sourceCanonicalByName[constraintNameKey(idx.Name)] = idx
			}
			for _, idx := range destCanonicalIndexes {
				destCanonicalByName[constraintNameKey(idx.Name)] = idx
			}
			switch indexType {
			case "pri":
				for _, constraint := range schemacompat.CanonicalizePrimaryKeyConstraints(sourceCanonicalIndexes) {
					sourceCanonicalConstraints[constraintNameKey(constraint.Name)] = constraint
				}
				for _, constraint := range schemacompat.CanonicalizePrimaryKeyConstraints(destCanonicalIndexes) {
					destCanonicalConstraints[constraintNameKey(constraint.Name)] = constraint
				}
			case "uni":
				for _, constraint := range schemacompat.CanonicalizeUniqueConstraints(sourceCanonicalIndexes) {
					sourceCanonicalConstraints[constraintNameKey(constraint.Name)] = constraint
				}
				for _, constraint := range schemacompat.CanonicalizeUniqueConstraints(destCanonicalIndexes) {
					destCanonicalConstraints[constraintNameKey(constraint.Name)] = constraint
				}
			}

			// 首先比较索引名称
			for k := range smu {
				c = append(c, k)
			}
			for k := range dmu {
				d = append(d, k)
			}
			sort.Strings(c)
			sort.Strings(d)

			// 如果索引名称不同，生成修复SQL
			if a.CheckMd5(strings.Join(c, ",")) != a.CheckMd5(strings.Join(d, ",")) {
				e, f := a.Arrcmp(c, d)

				// 当 requirePK=ON 且索引类型为主键时，检查目标端主键是否为 my_row_id
				// 如果是，则从待删除索引列表中移除（允许目标端保留 my_row_id 主键）
				if indexType == "pri" && stcls.isMySQLToMySQL() && len(f) > 0 {
					if strings.ToUpper(strings.TrimSpace(stcls.checkRules.RequirePK)) == "ON" {
						// 检查目标端主键列是否为 my_row_id
						for _, idxName := range f {
							if cols, ok := dmu[idxName]; ok && len(cols) > 0 {
								// 提取第一个列名（主键可能是单列或多列，这里只检查第一列）
								firstCol := cols[0]
								// 从 token 中提取列名（格式：columnName/*seq*/N/*type*/T/*prefix*/P）
								colName := strings.TrimSpace(firstCol)
								if seqIdx := strings.Index(firstCol, "/*seq*/"); seqIdx >= 0 {
									colName = strings.TrimSpace(firstCol[:seqIdx])
								}

								// 如果主键列是 my_row_id，从待删除列表中移除
								if strings.ToLower(strings.TrimSpace(colName)) == "my_row_id" {
									vlog := fmt.Sprintf("(%d) %s Skipping DROP PRIMARY KEY for my_row_id in %s.%s (requirePK=ON)", logThreadSeq, event, destSchema, stcls.table)
									global.Wlog.Info(vlog)
									// 从 f 中移除该索引
									var newF []string
									for _, v := range f {
										if v != idxName {
											newF = append(newF, v)
										}
									}
									f = newF
									break
								}
							}
						}
					}
				}

				// 对于新增的索引，需要处理列顺序
				newIndexMap := make(map[string][]string)
				for _, idx := range e {
					if cols, ok := smu[idx]; ok {
						// 传入原始 token（含前缀信息），由 FixAlterIndexSqlExec 内部解析
						newIndexMap[idx] = cols
					}
				}
				// 获取数据修复实例
				fixInstance := dbf.DataAbnormalFix()

				// 对于MySQL数据库，尝试加载外键定义
				if stcls.sourceDrive == "mysql" {
					// 将接口转换为MySQL具体类型
					if mysqlFix, ok := fixInstance.(*mysql.MysqlDataAbnormalFixStruct); ok {
						// 使用源端数据库连接加载外键定义
						err := mysqlFix.LoadForeignKeyDefinitions(stcls.sourceDB, logThreadSeq)
						if err != nil {
							vlog := fmt.Sprintf("(%d) Failed to load foreign key definitions for table %s.%s: %v",
								logThreadSeq, stcls.schema, stcls.table, err)
							global.Wlog.Warn(vlog)
						} else {
							vlog := fmt.Sprintf("(%d) Successfully loaded %d foreign key definitions for table %s.%s",
								logThreadSeq, len(mysqlFix.ForeignKeyDefinitions), stcls.schema, stcls.table)
							global.Wlog.Debug(vlog)
						}
					}
				}

				// 执行索引修复SQL生成
				cc = append(cc, fixInstance.FixAlterIndexSqlExec(e, f, newIndexMap, stcls.sourceDrive, logThreadSeq)...)
			}
			// 无论索引名称集合是否一致，都要对两端均存在的同名索引比较具体内容
			// （名称集合不同时，同名但内容不同的索引会被上方分支跳过，需在此补充检查）
			for k, sColumns := range smu {
				if dColumns, exists := dmu[k]; exists {
					semanticMismatch := false
					canonicalKey := constraintNameKey(k)
					indexSemanticMatch := false
					constraintSemanticMatch := indexType == "mul"
					if sourceIdx, ok := sourceCanonicalByName[canonicalKey]; ok {
						if destIdx, ok := destCanonicalByName[canonicalKey]; ok {
							indexDecision := schemacompat.DecideIndexCompatibility(sourceIdx, destIdx)
							if indexDecision.IsMismatch() {
								semanticMismatch = true
								vlog = fmt.Sprintf("(%d) %s Index %s semantic mismatch: reason=%s", logThreadSeq, event, k, indexDecision.Reason)
								global.Wlog.Warn(vlog)
							} else {
								indexSemanticMatch = true
							}
						}
					}
					if indexType == "pri" || indexType == "uni" {
						if sourceConstraint, ok := sourceCanonicalConstraints[canonicalKey]; ok {
							if destConstraint, ok := destCanonicalConstraints[canonicalKey]; ok {
								constraintDecision := schemacompat.DecideKeyConstraintCompatibility(sourceConstraint, destConstraint)
								if constraintDecision.IsMismatch() {
									semanticMismatch = true
									vlog = fmt.Sprintf("(%d) %s %s constraint %s semantic mismatch: reason=%s", logThreadSeq, event, strings.ToUpper(indexType), k, constraintDecision.Reason)
									global.Wlog.Warn(vlog)
								} else {
									constraintSemanticMatch = true
								}
							}
						}
					}
					if indexSemanticMatch && constraintSemanticMatch && !semanticMismatch {
						continue
					}

					sSortedColumns := sortColumns(sColumns)
					dSortedColumns := sortColumns(dColumns)
					if !semanticMismatch && indexColumnsOnlyDifferInCase(sSortedColumns, dSortedColumns) {
						continue
					}

					// 比较同名索引的列及其顺序（包含序号信息的比较）
					if semanticMismatch || a.CheckMd5(strings.Join(sColumns, ",")) != a.CheckMd5(strings.Join(dColumns, ",")) {
						// 检查是否仅仅是列名大小写不同（当caseSensitiveObjectName=yes时）
						columnsOnlyCaseDifferent := false
						if stcls.caseSensitiveObjectName == "yes" && len(sColumns) == len(dColumns) {
							columnsOnlyCaseDifferent = true
							lowerSourceColumns := make(map[string]bool)
							for _, col := range sColumns {
								lowerSourceColumns[strings.ToLower(col)] = true
							}
							for _, col := range dColumns {
								if !lowerSourceColumns[strings.ToLower(col)] {
									columnsOnlyCaseDifferent = false
									break
								}
							}
						}

						// 如果只是列名大小写不同且是主键，跳过重建主键
						if columnsOnlyCaseDifferent && indexType == "pri" && !semanticMismatch {
							continue
						}

						// 1. 先生成删除旧索引的SQL
						// 根据映射规则确定目标端schema
						destSchema := stcls.schema
						if mappedSchema, exists := stcls.tableMappings[stcls.schema]; exists {
							destSchema = mappedSchema
						}

						// 2. 纯列名（用于自增主键检测等需要原始列名的场景）
						plainColumns := sSortedColumns

						// 检查是否是主键且该列是自增列
						isAutoIncrementPrimaryKey := false
						if indexType == "pri" && len(plainColumns) == 1 {
							// 构建键名：schema.table.column
							key := fmt.Sprintf("%s.%s.%s", destSchema, stcls.table, plainColumns[0])
							// 检查该列是否已经在添加列时设置了主键
							if mysql.AutoIncrementColumnsWithPrimaryKey != nil && mysql.AutoIncrementColumnsWithPrimaryKey[key] {
								isAutoIncrementPrimaryKey = true
								vlog = fmt.Sprintf("(%d) %s Column %s is already set as PRIMARY KEY in ALTER TABLE ADD COLUMN statement, skipping index repair",
									logThreadSeq, event, plainColumns[0])
								global.Wlog.Debug(vlog)
							}
						}

						// 3. 生成创建索引的SQL
						// 根据映射规则确定目标端schema
						destSchema = stcls.schema
						if mappedSchema, exists := stcls.tableMappings[stcls.schema]; exists {
							destSchema = mappedSchema
						}

						// 带引号且保留前缀长度的列 DDL 表达式，例如 `goods_name`(20)
						quotedColumns := sortColumnsPreservingPrefix(sColumns)

						// 获取索引可见性信息
						// MariaDB 使用 IGNORED 关键字，MySQL 使用 INVISIBLE。
						indexHiddenKeyword := "INVISIBLE"
						if stcls.destVersionInfo().Flavor == global.DatabaseFlavorMariaDB {
							indexHiddenKeyword = "IGNORED"
						}
						visibility := ""
						if (indexType == "mul" || indexType == "uni") && sourceVisibilityMap != nil {
							if vis, ok := sourceVisibilityMap[k]; ok && isInvisibleLikeIndexVisibility(vis) {
								visibility = " " + indexHiddenKeyword
							}
						}

						// 只有当不是自增列主键时才生成创建索引的SQL
						if !isAutoIncrementPrimaryKey {
							// 1. 先删除目标端已存在的同名索引（先删后建，避免重复索引报错）
							if indexType == "pri" {
								cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PRIMARY KEY;",
									destSchema, stcls.table))
							} else {
								cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP INDEX `%s`;",
									destSchema, stcls.table, k))
							}
							// 2. 再新建符合源端定义的索引
							if indexType == "pri" {
								cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PRIMARY KEY(%s);",
									destSchema, stcls.table, strings.Join(quotedColumns, ", ")))
							} else if indexType == "uni" {
								cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD UNIQUE INDEX `%s`(%s)%s;",
									destSchema, stcls.table, k, strings.Join(quotedColumns, ", "), visibility))
							} else {
								cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD INDEX `%s`(%s)%s;",
									destSchema, stcls.table, k, strings.Join(quotedColumns, ", "), visibility))
							}
						}
					}
				}
			}
			return cc
		}
	)

	fmt.Println("gt-checksum: Starting index checks")
	event = fmt.Sprintf("[%s]", "check_table_index")
	//校验索引
	vlog = fmt.Sprintf("(%d) %s start init check source and target DB index Column. to check it...", logThreadSeq, event)
	global.Wlog.Info(vlog)

	// Preload Oracle index metadata across all source schemas so the per-table
	// loop can serve QueryTableIndexColumnInfo from memory instead of firing
	// one ALL_* 4-way JOIN per table (21 tables × ~1s on 11g = ~21s).
	var sourceOracleIndexCache map[string]map[string][]map[string]interface{}
	if isOracleDrive(stcls.sourceDrive) {
		schemasSet := make(map[string]struct{}, len(dtabS))
		for _, i := range dtabS {
			srcSchema, _, _, _ := parseSourceAndDestTablePair(i, stcls.tableMappings)
			if srcSchema != "" {
				schemasSet[strings.ToUpper(srcSchema)] = struct{}{}
			}
		}
		schemas := make([]string, 0, len(schemasSet))
		for s := range schemasSet {
			schemas = append(schemas, s)
		}
		sourceOracleIndexCache = preloadOracleIndexRows(stcls.sourceDB, schemas, logThreadSeq2)
	}

	for _, i := range dtabS {
		sourceSchema, tableName, destSchema, destTable := parseSourceAndDestTablePair(i, stcls.tableMappings)
		// 在正确的作用域内声明索引相关变量
		var spri, suni, smul, dpri, duni, dmul map[string][]string
		var sourceIndexVisibilityMap, destIndexVisibilityMap map[string]string

		stcls.table = tableName
		stcls.schema = sourceSchema
		stcls.destTable = destTable

		// 检查表是否在skipIndexCheckTables列表中，如果是，则跳过
		tableKey := fmt.Sprintf("%s.%s", destSchema, destTable)
		isDropped := false
		for _, droppedTable := range stcls.skipIndexCheckTables {
			if strings.EqualFold(droppedTable, tableKey) {
				vlog = fmt.Sprintf("(%d) %s Skipping index check for table %s as it is marked for deletion", logThreadSeq, event, tableKey)
				global.Wlog.Info(vlog)
				isDropped = true
				break
			}
		}
		if isDropped {
			continue
		}

		idxc := dbExec.IndexColumnStruct{Schema: sourceSchema, Table: stcls.table, Drivce: stcls.sourceDrive, CaseSensitiveObjectName: stcls.caseSensitiveObjectName}
		vlog = fmt.Sprintf("(%d) %s Start processing srcDSN {%s} table %s.%s index column data. to dispos it...", logThreadSeq, event, stcls.sourceDrive, sourceSchema, stcls.table)
		global.Wlog.Debug(vlog)
		var squeryData []map[string]interface{}
		if cached, ok := lookupOracleIndexRows(sourceOracleIndexCache, sourceSchema, stcls.table); ok {
			squeryData = cached
		} else {
			var qErr error
			squeryData, qErr = idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
			if qErr != nil {
				vlog = fmt.Sprintf("(%d) %s Querying the index column data of srcDSN {%s} database table %s failed, and the error message is {%v}", logThreadSeq, event, stcls.sourceDrive, i, qErr)
				global.Wlog.Error(vlog)
				return qErr
			}
		}
		spri, suni, smul, sourceIndexVisibilityMap = idxc.TableIndexColumn().IndexDisposF(squeryData, logThreadSeq2)
		if sourceCreateSQL, err := queryMySQLCreateTableStatement(stcls.sourceDB, sourceSchema, stcls.table); err == nil {
			sourceIndexVisibilityMap = mergeIndexVisibilityHints(sourceIndexVisibilityMap, schemacompat.ExtractIndexVisibilityHintsFromCreateSQL(sourceCreateSQL))
		}
		vlog = fmt.Sprintf("(%d) %s The index column data of the source %s database table %s.%s is {primary:%v,unique key:%v,index key:%v}",
			logThreadSeq,
			event,
			stcls.sourceDrive,
			sourceSchema,
			stcls.table,
			spri,
			suni,
			smul)
		global.Wlog.Debug(vlog)

		idxc.Schema = destSchema
		idxc.Table = destTable
		idxc.Drivce = stcls.destDrive
		vlog = fmt.Sprintf("(%d) %s Start processing dstDSN {%s} table %s.%s index column data. to dispos it...", logThreadSeq, event, stcls.destDrive, destSchema, destTable)
		global.Wlog.Debug(vlog)
		dqueryData, err := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.destDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Querying the index column data of dstDSN {%s} database table %s failed, and the error message is {%v}", logThreadSeq, event, stcls.destDrive, i, err)
			global.Wlog.Error(vlog)
			return err
		}
		dpri, duni, dmul, destIndexVisibilityMap = idxc.TableIndexColumn().IndexDisposF(dqueryData, logThreadSeq2)
		if destCreateSQL, err := queryMySQLCreateTableStatement(stcls.destDB, destSchema, stcls.destTable); err == nil {
			destIndexVisibilityMap = mergeIndexVisibilityHints(destIndexVisibilityMap, schemacompat.ExtractIndexVisibilityHintsFromCreateSQL(destCreateSQL))
		}
		// Oracle→MySQL：Oracle FK 约束不会自动创建 backing index，但 MySQL 会。
		// 将 MySQL 目标端中属于 FK 约束的自动 backing index 从比对集合中排除，
		// 避免误生成 DROP INDEX 修复语句。
		if stcls.isOracleToMySQL() {
			if fkIndexNames, fkErr := queryMySQLForeignKeyIndexNames(stcls.destDB, destSchema, destTable); fkErr == nil {
				for idxName := range dmul {
					if fkIndexNames[strings.ToUpper(idxName)] {
						delete(dmul, idxName)
					}
				}
				for idxName := range duni {
					if fkIndexNames[strings.ToUpper(idxName)] {
						delete(duni, idxName)
					}
				}
			}
		}
		vlog = fmt.Sprintf("(%d) %s The index column data of the dest %s database table %s.%s is {primary:%v,unique key:%v,index key:%v}",
			logThreadSeq,
			event,
			stcls.destDrive,
			destSchema,
			destTable,
			dpri,
			duni,
			dmul)
		global.Wlog.Debug(vlog)

		var pods = Pod{
			Datafix:     stcls.datafix,
			CheckObject: "index",
			DIFFS:       "no",
			Schema:      stcls.schema,
			Table:       stcls.table,
		}

		// 如果是从 Struct 函数调用的，则将 CheckObject 设置为 "struct"
		if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
			pods.CheckObject = "struct"
		}
		//先比较主键索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the primary key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(spri, dpri, aa, "pri", sourceIndexVisibilityMap, destIndexVisibilityMap)...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the primary key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		//再比较唯一索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the unique key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(suni, duni, aa, "uni", sourceIndexVisibilityMap, destIndexVisibilityMap)...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the unique key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Info(vlog)
		//后比较普通索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the no-unique key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(smul, dmul, aa, "mul", sourceIndexVisibilityMap, destIndexVisibilityMap)...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the no-unique key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		// 应用并清空 sqlS
		columnRepairKey := fmt.Sprintf("%s.%s", stcls.schema, stcls.table)
		pendingColumnOperations := stcls.pendingColumnRepairOperations(columnRepairKey)
		if len(sqlS) > 0 {
			pods.DIFFS = "yes"

			// 检查是否有列修复操作需要合并
			if len(pendingColumnOperations) > 0 {
				// 创建DataAbnormalFixStruct用于合并操作
				destSchema := stcls.schema
				if mappedSchema, exists := stcls.tableMappings[stcls.schema]; exists {
					destSchema = mappedSchema
				}

				dbf := dbExec.DataAbnormalFixStruct{
					Schema:                  destSchema,
					Table:                   stcls.table,
					SourceDevice:            stcls.sourceDrive,
					DestDevice:              stcls.destDrive,
					DatafixType:             stcls.datafix,
					CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
					SourceSchema:            stcls.schema,
					DestFlavor:              stcls.destVersionInfo().Flavor,
				}

				// 合并列修复和索引修复操作
				combinedSql := dbf.DataAbnormalFix().FixAlterColumnAndIndexSqlGenerate(pendingColumnOperations, sqlS, logThreadSeq)

				// 使用合并后的SQL
				sqlS = combinedSql

				// Column repair operations have been merged into the final
				// ALTER TABLE statement and can now be discarded.
				stcls.forgetColumnRepairOperations(columnRepairKey)

				vlog = fmt.Sprintf("(%d) %s Merged column and index operations for table %s.%s",
					logThreadSeq, event, stcls.schema, stcls.table)
				global.Wlog.Debug(vlog)
			} else {
				// 只有索引操作，合并索引操作
				destSchema := stcls.schema
				if mappedSchema, exists := stcls.tableMappings[stcls.schema]; exists {
					destSchema = mappedSchema
				}

				dbf := dbExec.DataAbnormalFixStruct{
					Schema:                  destSchema,
					Table:                   stcls.table,
					SourceDevice:            stcls.sourceDrive,
					DestDevice:              stcls.destDrive,
					DatafixType:             stcls.datafix,
					SourceSchema:            stcls.schema,
					CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
					DestFlavor:              stcls.destVersionInfo().Flavor,
				}

				combinedSql := dbf.DataAbnormalFix().FixAlterIndexSqlGenerate(sqlS, logThreadSeq)
				sqlS = combinedSql
			}

			if err := stcls.writeFixSql(sqlS, logThreadSeq); err != nil {
				return err
			}
			sqlS = []string{} // 清空 sqlS 以便下一个表使用

			// 添加调试日志，记录索引不一致的表
			vlog = fmt.Sprintf("(%d) %s Table %s.%s has index differences, setting DIFFS to yes",
				logThreadSeq, event, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
		} else if len(pendingColumnOperations) > 0 {
			// Tables with column-level fixes but no index diffs still need their
			// deferred ALTER TABLE written once the index phase confirms no merge
			// is needed.
			if err := stcls.writeFixSql(pendingColumnOperations, logThreadSeq); err != nil {
				return err
			}
			stcls.forgetColumnRepairOperations(columnRepairKey)
			vlog = fmt.Sprintf("(%d) %s Flushed deferred column/table repair statements for table %s.%s",
				logThreadSeq, event, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
		}

		// 如果是从 Struct 函数调用的，则将结果存储在临时变量中，以便 Struct 函数可以使用
		if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
			// 使用完整的schema.table作为键
			tableKey := fmt.Sprintf("%s.%s", stcls.schema, stcls.table)

			// Keep index diff state on the schemaTable instance so concurrent
			// schemaTable values do not share mutable global maps.
			if stcls.indexDiffsMap == nil {
				stcls.indexDiffsMap = make(map[string]bool)
			}
			stcls.indexDiffsMap[tableKey] = pods.DIFFS == "yes"

			vlog = fmt.Sprintf("(%d) %s Storing index check result for table %s.%s: %v",
				logThreadSeq, event, stcls.schema, stcls.table, stcls.indexDiffsMap[tableKey])
			global.Wlog.Debug(vlog)
		} else {
			// 不是从 Struct 函数调用时，添加到 measuredDataPods
			measuredDataPods = append(measuredDataPods, pods)
		}
		vlog = fmt.Sprintf("(%d) %s The source target segment table %s.%s index column data verification is completed", logThreadSeq, event, stcls.schema, stcls.table)
		global.Wlog.Info(vlog)
	}
	if len(stcls.columnRepairMap) > 0 {
		// A final sweep prevents deferred column SQL from being dropped if a
		// table never reached the merge branch above.
		for tableKey, pendingSQL := range stcls.columnRepairMap {
			if len(pendingSQL) == 0 {
				continue
			}
			parts := strings.SplitN(tableKey, ".", 2)
			if len(parts) == 2 {
				stcls.schema = parts[0]
				stcls.table = parts[1]
				stcls.destTable = parts[1]
			}
			if err := stcls.writeFixSql(pendingSQL, logThreadSeq); err != nil {
				return err
			}
			vlog = fmt.Sprintf("(%d) %s Flushed remaining deferred repair statements for table %s",
				logThreadSeq, event, tableKey)
			global.Wlog.Debug(vlog)
		}
		stcls.columnRepairMap = make(map[string][]string)
	}
	fmt.Println("gt-checksum: Index verification completed")
	return nil
}
