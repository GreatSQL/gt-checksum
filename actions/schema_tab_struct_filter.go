package actions

import (
	"database/sql"
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"sort"
	"strings"
)

func collectSourceSchemasForStructCheck(checkTableList []string) []string {
	seen := make(map[string]struct{})
	schemas := make([]string, 0)
	for _, item := range checkTableList {
		sourceItem := item
		if strings.Contains(item, ":") {
			sourceItem = strings.SplitN(item, ":", 2)[0]
		}
		parts := strings.SplitN(sourceItem, ".", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			continue
		}
		schemaName := parts[0]
		if _, exists := seen[schemaName]; exists {
			continue
		}
		seen[schemaName] = struct{}{}
		schemas = append(schemas, schemaName)
	}
	sort.Strings(schemas)
	return schemas
}

func parseSourceAndDestTablePair(mapping string, schemaMappings map[string]string) (string, string, string, string) {
	sourceSchema := ""
	sourceTable := ""
	destSchema := ""
	destTable := ""

	if strings.Contains(mapping, ":") {
		parts := strings.SplitN(mapping, ":", 2)
		if len(parts) == 2 {
			sourceParts := strings.SplitN(parts[0], ".", 2)
			destParts := strings.SplitN(parts[1], ".", 2)
			if len(sourceParts) == 2 {
				sourceSchema = sourceParts[0]
				sourceTable = sourceParts[1]
			}
			if len(destParts) == 2 {
				destSchema = destParts[0]
				destTable = destParts[1]
			}
		}
	}

	if sourceSchema == "" || sourceTable == "" {
		parts := strings.SplitN(mapping, ".", 2)
		if len(parts) == 2 {
			sourceSchema = parts[0]
			sourceTable = parts[1]
		}
	}

	if destSchema == "" {
		if mappedSchema, ok := schemaMappings[sourceSchema]; ok && strings.TrimSpace(mappedSchema) != "" {
			destSchema = mappedSchema
		} else {
			destSchema = sourceSchema
		}
	}
	if destTable == "" {
		destTable = sourceTable
	}

	return sourceSchema, sourceTable, destSchema, destTable
}

func (stcls *schemaTable) FuzzyMatchingDispos(dbCheckNameList map[string]int, Ftable string, logThreadSeq int64) map[string]int {
	var (
		schema string
		vlog   string
	)
	b := make(map[string]int)
	f := make(map[string]int)
	sourceSchemas := extractSchemaNamesFromCacheKeys(dbCheckNameList)
	if strings.TrimSpace(Ftable) == "" || strings.EqualFold(strings.TrimSpace(Ftable), "nil") {
		return f
	}

	// 添加调试日志，显示当前的映射规则
	vlog = fmt.Sprintf("Current table mappings: %v", stcls.tableMappings)
	global.Wlog.Debug(vlog)

	//处理库的模糊查询
	for _, i := range strings.Split(Ftable, ",") {
		// 解析映射关系
		srcPattern := i
		dstPattern := ""
		hasMappingRule := false

		if strings.Contains(i, ":") {
			parts := strings.SplitN(i, ":", 2)
			if len(parts) == 2 {
				srcPattern = parts[0]
				dstPattern = parts[1]
				hasMappingRule = true
			}
		}

		vlog = fmt.Sprintf("Processing table pattern: source=%s, target=%s, mapped=%v", srcPattern, dstPattern, hasMappingRule)
		global.Wlog.Debug(vlog)

		if !strings.Contains(srcPattern, ".") {
			continue
		}

		schema = strings.ReplaceAll(srcPattern[:strings.Index(srcPattern, ".")], "%", "")

		// 处理通配符模式
		if schema == "*" { //处理*库
			for _, schemaName := range sourceSchemas {
				b[schemaName]++
				vlog = fmt.Sprintf("Added wildcard schema: %s", schemaName)
				global.Wlog.Debug(vlog)
			}
		} else if strings.HasPrefix(schema, "%") && strings.HasSuffix(schema, "%") { //处理%schema%
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for _, schemaName := range sourceSchemas {
				if strings.Contains(schemaName, tmpschema) {
					b[schemaName]++
					vlog = fmt.Sprintf("Added %%schema%% match: %s", schemaName)
					global.Wlog.Debug(vlog)
				}
			}
		} else if strings.HasPrefix(schema, "%") && !strings.HasSuffix(schema, "%") { //处理%schema
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for _, schemaName := range sourceSchemas {
				if strings.HasSuffix(schemaName, tmpschema) {
					b[schemaName]++
					vlog = fmt.Sprintf("Added %%schema match: %s", schemaName)
					global.Wlog.Debug(vlog)
				}
			}
		} else if !strings.HasPrefix(schema, "%") && strings.HasSuffix(schema, "%") { //处理schema%
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for _, schemaName := range sourceSchemas {
				if strings.HasPrefix(schemaName, tmpschema) {
					b[schemaName]++
					vlog = fmt.Sprintf("Added schema%% match: %s", schemaName)
					global.Wlog.Debug(vlog)
				}
			}
		} else { //处理schema
			// 检查是否在映射规则中存在（Oracle源端按不区分大小写匹配）
			if _, exists := stcls.findMappedSchema(schema); exists {
				added := false
				for _, schemaName := range sourceSchemas {
					if stcls.sourceObjectNameEqual(schemaName, schema) {
						b[schemaName]++
						added = true
						vlog = fmt.Sprintf("Added source schema from mapping: %s (pattern: %s)", schemaName, schema)
						global.Wlog.Debug(vlog)
					}
				}
				if !added {
					b[schema]++
					vlog = fmt.Sprintf("Added source schema from mapping fallback: %s", schema)
					global.Wlog.Debug(vlog)
				}
			} else if hasMappingRule {
				// 如果有明确的映射规则，尝试使用它
				dstSchema := ""
				if strings.Contains(dstPattern, ".") {
					dstSchema = dstPattern[:strings.Index(dstPattern, ".")]
				} else {
					dstSchema = dstPattern
				}

				// 检查源schema是否存在于数据库列表中（大小写兼容）
				for _, schemaName := range sourceSchemas {
					if stcls.sourceObjectNameEqual(schemaName, schema) {
						b[schemaName]++
						vlog = fmt.Sprintf("Added explicit mapping source schema: %s -> %s", schemaName, dstSchema)
						global.Wlog.Debug(vlog)
					}
				}
			} else {
				// 检查是否是目标端schema
				found := false
				for src, dst := range stcls.tableMappings {
					if stcls.destObjectNameEqual(dst, schema) {
						// 找到对应源端schema
						b[src]++
						found = true
						vlog = fmt.Sprintf("Added reverse mapping source schema: %s -> %s", src, dst)
						global.Wlog.Debug(vlog)
						break
					}
				}
				// 如果没有映射关系，则按常规处理
				if !found {
					// 检查schema是否存在于数据库列表中（大小写兼容）
					for _, schemaName := range sourceSchemas {
						if stcls.sourceObjectNameEqual(schemaName, schema) {
							b[schemaName]++
							vlog = fmt.Sprintf("Added direct schema (no mapping): %s", schemaName)
							global.Wlog.Debug(vlog)
						}
					}
				}
			}
		}
	}

	vlog = fmt.Sprintf("After schema processing, b map: %v", b)
	global.Wlog.Debug(vlog)

	//处理表的模糊查询
	for _, i := range strings.Split(Ftable, ",") {
		// 解析映射关系
		srcPattern := i
		dstPattern := ""
		hasMappingRule := false

		if strings.Contains(i, ":") {
			parts := strings.SplitN(i, ":", 2)
			if len(parts) == 2 {
				srcPattern = parts[0]
				dstPattern = parts[1]
				hasMappingRule = true
			}
		}

		vlog = fmt.Sprintf("Processing table pattern: src=%s, dst=%s, hasMapping=%v", srcPattern, dstPattern, hasMappingRule)
		global.Wlog.Debug(vlog)

		if !strings.Contains(srcPattern, ".") {
			continue
		}

		schema = strings.ReplaceAll(srcPattern[:strings.Index(srcPattern, ".")], "%", "")
		table := srcPattern[strings.Index(srcPattern, ".")+1:]

		vlog = fmt.Sprintf("Parsed schema=%s, table=%s", schema, table)
		global.Wlog.Debug(vlog)

		// 处理表名通配符
		for dbSchema, _ := range b {
			// 检查是否有映射关系
			mappedSchema := dbSchema
			if mapped, exists := stcls.findMappedSchema(dbSchema); exists {
				mappedSchema = mapped
				vlog = fmt.Sprintf("Found schema mapping: %s -> %s", dbSchema, mappedSchema)
				global.Wlog.Debug(vlog)
			}

			// 检查schema是否匹配
			if stcls.sourceObjectNameEqual(dbSchema, schema) || schema == "*" {
				// 构建表名查询
				for dbName, _ := range dbCheckNameList {
					dbSchemaName, dbTableName, ok := splitSchemaTableCacheKey(dbName)
					if !ok {
						continue
					}

					// 检查schema是否匹配
					if !stcls.sourceObjectNameEqual(dbSchemaName, dbSchema) {
						continue
					}

					// 处理表名通配符
					if table == "*" { // 处理schema.*
						f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
						vlog = fmt.Sprintf("Added table pattern: %s.%s", dbSchema, dbTableName)
						global.Wlog.Debug(vlog)
					} else if strings.HasPrefix(table, "%") && !strings.HasSuffix(table, "%") { // 处理schema.%table
						tmptable := strings.ReplaceAll(table, "%", "")
						if strings.HasSuffix(dbTableName, tmptable) {
							f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
							vlog = fmt.Sprintf("Added suffix pattern: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					} else if !strings.HasPrefix(table, "%") && strings.HasSuffix(table, "%") { // 处理schema.table%
						tmptable := strings.ReplaceAll(table, "%", "")
						if strings.HasPrefix(dbTableName, tmptable) {
							f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
							vlog = fmt.Sprintf("Added table%% match: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					} else if strings.HasPrefix(table, "%") && strings.HasSuffix(table, "%") { // 处理schema.%table%
						tmptable := strings.ReplaceAll(table, "%", "")
						if strings.Contains(dbTableName, tmptable) {
							f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
							vlog = fmt.Sprintf("Added %%table%% match: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					} else { // 处理schema.table
						if stcls.sourceObjectNameEqual(dbTableName, table) {
							f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
							vlog = fmt.Sprintf("Added exact table match: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					}
				}
			}
		}
	}

	vlog = fmt.Sprintf("Final result map: %v", f)
	global.Wlog.Debug(vlog)

	return f
}

func extractSchemaNamesFromCacheKeys(dbCheckNameList map[string]int) []string {
	schemaSet := make(map[string]struct{}, len(dbCheckNameList))
	for cacheKey := range dbCheckNameList {
		schemaName, _, ok := splitSchemaTableCacheKey(cacheKey)
		if !ok {
			// fallback for legacy key format
			schemaName = cacheKey
		}
		schemaSet[schemaName] = struct{}{}
	}
	result := make([]string, 0, len(schemaSet))
	for schemaName := range schemaSet {
		result = append(result, schemaName)
	}
	sort.Strings(result)
	return result
}

/*
处理需要校验的库表
将忽略的库表从校验列表中去除，如果校验列表为空则退出
*/
// 定义一个新的结构体来存储表映射信息
type TableMapping struct {
	SourceSchema string // 源端schema
	SourceTable  string // 源端表名
	DestSchema   string // 目标端schema
	DestTable    string // 目标端表名
}

var schemaTableFilterDatabaseNameList = func(tc dbExec.TableColumnNameStruct, db *sql.DB, logThreadSeq int64) (map[string]int, error) {
	return tc.Query().DatabaseNameList(db, logThreadSeq)
}

var schemaTableFilterObjectTypeMap = func(tc dbExec.TableColumnNameStruct, db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	return tc.Query().ObjectTypeMap(db, logThreadSeq)
}

// extractCandidateSchemas returns the distinct schema names present in the
// DatabaseNameList key set (format: "schema/*schema&table*/table").
// The result is used to constrain the ObjectTypeMap metadata query to only the
// schemas relevant for this run instead of performing a full-instance scan.
func extractCandidateSchemas(candidates map[string]int) []string {
	seen := make(map[string]struct{}, len(candidates))
	for key := range candidates {
		const sep = "/*schema&table*/"
		if idx := strings.Index(key, sep); idx > 0 {
			seen[key[:idx]] = struct{}{}
		}
	}
	schemas := make([]string, 0, len(seen))
	for s := range seen {
		schemas = append(schemas, s)
	}
	return schemas
}

func (stcls *schemaTable) SchemaTableFilter(logThreadSeq1, logThreadSeq2 int64) ([]string, error) {
	var (
		vlog            string
		f               []string
		dbCheckNameList map[string]int
		err             error
	)
	fmt.Println("gt-checksum: Starting table checks")
	vlog = fmt.Sprintf("(%d) Obtain schema.table info", logThreadSeq1)
	global.Wlog.Info(vlog)

	// 解析表映射规则
	stcls.parseTableMappings(stcls.table)

	// 添加调试日志，显示解析后的映射规则
	vlog = fmt.Sprintf("Table mappings after parsing: %v", stcls.tableMappings)
	global.Wlog.Debug(vlog)

	// 获取源数据库信息列表
	tc := dbExec.TableColumnNameStruct{
		Table:                   stcls.table,
		Drive:                   stcls.sourceDrive,
		Db:                      stcls.sourceDB,
		IgnoreTable:             stcls.ignoreTable,
		CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
	}
	vlog = fmt.Sprintf("(%d) Obtain source databases list", logThreadSeq1)
	global.Wlog.Debug(vlog)
	if dbCheckNameList, err = schemaTableFilterDatabaseNameList(tc, stcls.sourceDB, logThreadSeq2); err != nil {
		return f, err
	}

	// Populate the per-run object-type map (table vs. view).
	// A failed query is non-fatal: we log a warning and continue with an empty
	// map, which preserves the previous behaviour of treating every object as a
	// BASE TABLE.
	//
	// Pass the candidate schema set extracted from dbCheckNameList so that
	// the driver can restrict the INFORMATION_SCHEMA.TABLES scan to only the
	// schemas relevant to this run, avoiding a costly full-instance scan.
	tc.CandidateSchemas = extractCandidateSchemas(dbCheckNameList)
	if kinds, kindErr := schemaTableFilterObjectTypeMap(tc, stcls.sourceDB, logThreadSeq2); kindErr != nil {
		vlog = fmt.Sprintf("(%d) ObjectTypeMap query failed (non-fatal, treating all objects as BASE TABLE): %v", logThreadSeq1, kindErr)
		global.Wlog.Warn(vlog)
		stcls.objectKinds = make(map[string]string)
	} else {
		stcls.objectKinds = kinds
		vlog = fmt.Sprintf("(%d) ObjectTypeMap loaded: %d entries", logThreadSeq1, len(kinds))
		global.Wlog.Debug(vlog)
	}

	sampleLimit := 8
	if len(dbCheckNameList) <= sampleLimit {
		vlog = fmt.Sprintf("(%d) Source databases list(size=%d): %v", logThreadSeq1, len(dbCheckNameList), dbCheckNameList)
	} else {
		sample := make([]string, 0, sampleLimit)
		for k := range dbCheckNameList {
			sample = append(sample, k)
			if len(sample) >= sampleLimit {
				break
			}
		}
		sort.Strings(sample)
		vlog = fmt.Sprintf("(%d) Source databases list(size=%d, sample=%v)", logThreadSeq1, len(dbCheckNameList), sample)
	}
	global.Wlog.Debug(vlog)

	// 判断源库是否为空
	if len(dbCheckNameList) == 0 {
		vlog = fmt.Sprintf("(%d) Databases of srcDSN {%s} is empty, please check if the \"tables\" option is correct", logThreadSeq1, stcls.sourceDrive)
		global.Wlog.Error(vlog)
		return f, nil
	}

	// 处理映射关系中的目标库
	// 如果有映射关系，也需要获取目标库的信息
	destDbCheckNameList := make(map[string]int)

	// 检查是否有映射关系
	hasMapping := false
	for _, pattern := range strings.Split(stcls.table, ",") {
		if strings.Contains(pattern, ":") {
			hasMapping = true
			break
		}
	}

	// 如果有映射关系，获取目标库信息
	if hasMapping {
		vlog = fmt.Sprintf("(%d) Mapping relationship detected, obtaining destination databases list", logThreadSeq1)
		global.Wlog.Debug(vlog)

		tcDest := dbExec.TableColumnNameStruct{
			Table:                   stcls.table,
			Drive:                   stcls.destDrive,
			Db:                      stcls.destDB,
			IgnoreTable:             stcls.ignoreTable,
			CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
		}

		destDbList, err := schemaTableFilterDatabaseNameList(tcDest, stcls.destDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Error getting destination databases list: %v", logThreadSeq1, err)
			global.Wlog.Error(vlog)
		} else {
			destDbCheckNameList = destDbList
			if len(destDbCheckNameList) <= sampleLimit {
				vlog = fmt.Sprintf("(%d) Destination databases list(size=%d): %v", logThreadSeq1, len(destDbCheckNameList), destDbCheckNameList)
			} else {
				sample := make([]string, 0, sampleLimit)
				for k := range destDbCheckNameList {
					sample = append(sample, k)
					if len(sample) >= sampleLimit {
						break
					}
				}
				sort.Strings(sample)
				vlog = fmt.Sprintf("(%d) Destination databases list(size=%d, sample=%v)", logThreadSeq1, len(destDbCheckNameList), sample)
			}
			global.Wlog.Debug(vlog)
		}
	}

	// 创建表映射列表
	tableMappings := make([]TableMapping, 0)

	// 处理 db1.*:db2.* 格式的映射
	for _, pattern := range strings.Split(stcls.table, ",") {
		if strings.Contains(pattern, ":") {
			mapping := strings.SplitN(pattern, ":", 2)
			if len(mapping) == 2 {
				srcPattern := mapping[0]
				dstPattern := mapping[1]

				// 处理 db1.*:db2.* 格式
				if strings.HasSuffix(srcPattern, ".*") && strings.HasSuffix(dstPattern, ".*") {
					srcDB := strings.TrimSuffix(srcPattern, ".*")
					dstDB := strings.TrimSuffix(dstPattern, ".*")

					vlog = fmt.Sprintf("Processing wildcard mapping: %s.* -> %s.*", srcDB, dstDB)
					global.Wlog.Debug(vlog)

					// 获取源库中的所有表（Oracle源端按不区分大小写匹配schema）
					for dbName := range dbCheckNameList {
						dbSchemaName, tableName, ok := splitSchemaTableCacheKey(dbName)
						if !ok || !stcls.sourceObjectNameEqual(dbSchemaName, srcDB) {
							continue
						}

						// 创建表映射
						mapping := TableMapping{
							SourceSchema: dbSchemaName,
							SourceTable:  tableName,
							DestSchema:   dstDB,
							DestTable:    tableName,
						}
						tableMappings = append(tableMappings, mapping)

						vlog = fmt.Sprintf("Added mapping: %s.%s -> %s.%s", dbSchemaName, tableName, dstDB, tableName)
						global.Wlog.Debug(vlog)
					}

					// 检查目标库中是否有源库中不存在的表
					for dbName := range destDbCheckNameList {
						dbSchemaName, tableName, ok := splitSchemaTableCacheKey(dbName)
						if !ok || !stcls.destObjectNameEqual(dbSchemaName, dstDB) {
							continue
						}

						// 检查这个表是否已经在映射列表中
						found := false
						for _, m := range tableMappings {
							if stcls.destObjectNameEqual(m.DestSchema, dstDB) && m.DestTable == tableName {
								found = true
								break
							}
						}

						// 如果没有找到，添加新的映射
						if !found {
							mapping := TableMapping{
								SourceSchema: srcDB,
								SourceTable:  tableName,
								DestSchema:   dbSchemaName,
								DestTable:    tableName,
							}
							tableMappings = append(tableMappings, mapping)

							vlog = fmt.Sprintf("Added mapping from dest table: %s.%s -> %s.%s", srcDB, tableName, dbSchemaName, tableName)
							global.Wlog.Debug(vlog)
						}
					}
				} else if strings.Contains(srcPattern, ".") && strings.Contains(dstPattern, ".") {
					// 处理 db1.t1:db2.t2 格式
					srcParts := strings.Split(srcPattern, ".")
					dstParts := strings.Split(dstPattern, ".")

					if len(srcParts) == 2 && len(dstParts) == 2 {
						srcDB := srcParts[0]
						srcTable := srcParts[1]
						dstDB := dstParts[0]
						dstTable := dstParts[1]

						// 检查表名是否包含通配符
						if strings.Contains(srcTable, "%") || strings.Contains(dstTable, "%") {
							// 处理带通配符的表名映射
							for dbName := range dbCheckNameList {
								dbSchemaName, tableName, ok := splitSchemaTableCacheKey(dbName)
								if !ok || !stcls.sourceObjectNameEqual(dbSchemaName, srcDB) {
									continue
								}

								// 检查表名是否匹配源端通配符模式
								matchSrc := false
								if strings.HasPrefix(srcTable, "%") && strings.HasSuffix(srcTable, "%") {
									// 处理 %table% 模式
									tmpTable := strings.ReplaceAll(srcTable, "%", "")
									if strings.Contains(tableName, tmpTable) {
										matchSrc = true
									}
								} else if strings.HasPrefix(srcTable, "%") {
									// 处理 %table 模式
									tmpTable := strings.ReplaceAll(srcTable, "%", "")
									if strings.HasSuffix(tableName, tmpTable) {
										matchSrc = true
									}
								} else if strings.HasSuffix(srcTable, "%") {
									// 处理 table% 模式
									tmpTable := strings.ReplaceAll(srcTable, "%", "")
									if strings.HasPrefix(tableName, tmpTable) {
										matchSrc = true
									}
								}

								if matchSrc {
									// 生成目标端表名
									destTableName := tableName

									// 创建表映射
									mapping := TableMapping{
										SourceSchema: dbSchemaName,
										SourceTable:  tableName,
										DestSchema:   dstDB,
										DestTable:    destTableName,
									}
									tableMappings = append(tableMappings, mapping)

									vlog = fmt.Sprintf("Added wildcard mapping: %s.%s -> %s.%s", dbSchemaName, tableName, dstDB, destTableName)
									global.Wlog.Debug(vlog)
								}
							}
						} else {
							// 处理精确表名映射
							// 检查源端表是否存在
							srcKey := fmt.Sprintf("%s/*schema&table*/%s", srcDB, srcTable)
							if strings.EqualFold(stcls.caseSensitiveObjectName, "no") {
								srcKey = strings.ToLower(srcKey)
							}
							if _, srcExists := dbCheckNameList[srcKey]; !srcExists {
								vlog = fmt.Sprintf("Source table %s.%s does not exist in source DB, skipping mapping to %s.%s", srcDB, srcTable, dstDB, dstTable)
								global.Wlog.Warn(vlog)
								continue
							}
							// 创建表映射
							mapping := TableMapping{
								SourceSchema: srcDB,
								SourceTable:  srcTable,
								DestSchema:   dstDB,
								DestTable:    dstTable,
							}
							tableMappings = append(tableMappings, mapping)

							vlog = fmt.Sprintf("Added direct mapping: %s.%s -> %s.%s", srcDB, srcTable, dstDB, dstTable)
							global.Wlog.Debug(vlog)
						}
					}
				}
			}
		} else {
			// 处理非映射模式，如 db1.*
			if strings.HasSuffix(pattern, ".*") {
				srcDB := strings.TrimSuffix(pattern, ".*")

				// 处理忽略表
				ignoreSchema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.ignoreTable, logThreadSeq1)

				// 获取该库中的所有表
				for dbName := range dbCheckNameList {
					dbSchemaName, tableName, ok := splitSchemaTableCacheKey(dbName)
					if !ok || !stcls.sourceObjectNameEqual(dbSchemaName, srcDB) {
						continue
					}

					// ignoreTables should only remove wildcard-selected tables.
					if stcls.shouldIgnoreMatchedTable(ignoreSchema, dbSchemaName, tableName) {
						vlog = fmt.Sprintf("Ignoring table due to ignoreTables: %s.%s", dbSchemaName, tableName)
						global.Wlog.Debug(vlog)
						continue
					}

					// 创建表映射（源端和目标端相同）
					mapping := TableMapping{
						SourceSchema: dbSchemaName,
						SourceTable:  tableName,
						DestSchema:   dbSchemaName,
						DestTable:    tableName,
					}
					tableMappings = append(tableMappings, mapping)

					vlog = fmt.Sprintf("Added non-mapping entry: %s.%s", dbSchemaName, tableName)
					global.Wlog.Debug(vlog)
				}
			} else if strings.Contains(pattern, ".") {
				// 处理 db1.t1 格式
				parts := strings.Split(pattern, ".")
				if len(parts) == 2 {
					srcDB := parts[0]
					srcTable := parts[1]

					// 检查表名是否包含通配符
					if strings.Contains(srcTable, "%") {
						// 处理表名通配符
						for dbName := range dbCheckNameList {
							dbSchemaName, tableName, ok := splitSchemaTableCacheKey(dbName)
							if !ok || !stcls.sourceObjectNameEqual(dbSchemaName, srcDB) {
								continue
							}

							// 检查表名是否匹配通配符模式
							match := false
							if strings.HasPrefix(srcTable, "%") && strings.HasSuffix(srcTable, "%") {
								// 处理 %table% 模式
								tmpTable := strings.ReplaceAll(srcTable, "%", "")
								if strings.Contains(tableName, tmpTable) {
									match = true
								}
							} else if strings.HasPrefix(srcTable, "%") {
								// 处理 %table 模式
								tmpTable := strings.ReplaceAll(srcTable, "%", "")
								if strings.HasSuffix(tableName, tmpTable) {
									match = true
								}
							} else if strings.HasSuffix(srcTable, "%") {
								// 处理 table% 模式
								tmpTable := strings.ReplaceAll(srcTable, "%", "")
								if strings.HasPrefix(tableName, tmpTable) {
									match = true
								}
							}

							if match {
								// 处理忽略表
								ignoreSchema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.ignoreTable, logThreadSeq1)

								if stcls.shouldIgnoreMatchedTable(ignoreSchema, dbSchemaName, tableName) {
									vlog = fmt.Sprintf("Ignoring table due to ignoreTables: %s.%s", dbSchemaName, tableName)
									global.Wlog.Debug(vlog)
									continue
								}

								// 创建表映射（源端和目标端相同）
								mapping := TableMapping{
									SourceSchema: dbSchemaName,
									SourceTable:  tableName,
									DestSchema:   dbSchemaName,
									DestTable:    tableName,
								}
								tableMappings = append(tableMappings, mapping)

								vlog = fmt.Sprintf("Added wildcard matching entry: %s.%s", dbSchemaName, tableName)
								global.Wlog.Debug(vlog)
							}
						}
					} else {
						// 处理精确表名
						// 检查源端表是否存在
						srcKey := fmt.Sprintf("%s/*schema&table*/%s", srcDB, srcTable)
						if strings.EqualFold(stcls.caseSensitiveObjectName, "no") {
							srcKey = strings.ToLower(srcKey)
						}
						if _, srcExists := dbCheckNameList[srcKey]; !srcExists {
							vlog = fmt.Sprintf("Source table %s.%s does not exist in source DB, skipping", srcDB, srcTable)
							global.Wlog.Warn(vlog)
							continue
						}

						// 处理忽略表
						ignoreSchema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.ignoreTable, logThreadSeq1)

						if stcls.shouldIgnoreMatchedTable(ignoreSchema, srcDB, srcTable) {
							vlog = fmt.Sprintf("Ignoring table due to ignoreTables: %s.%s", srcDB, srcTable)
							global.Wlog.Debug(vlog)
							continue
						}

						// 创建表映射（源端和目标端相同）
						mapping := TableMapping{
							SourceSchema: srcDB,
							SourceTable:  srcTable,
							DestSchema:   srcDB,
							DestTable:    srcTable,
						}
						tableMappings = append(tableMappings, mapping)

						vlog = fmt.Sprintf("Added direct non-mapping entry: %s.%s", srcDB, srcTable)
						global.Wlog.Debug(vlog)
					}
				}
			}
		}
	}

	// 如果没有找到任何映射，尝试使用默认方式处理
	if len(tableMappings) == 0 {
		vlog = fmt.Sprintf("No mappings found, using default processing")
		global.Wlog.Debug(vlog)

		// 使用模糊匹配处理表名
		schema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.table, logThreadSeq1)

		// 处理忽略表
		ignoreSchema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.ignoreTable, logThreadSeq1)
		for k := range schema {
			parts := strings.SplitN(k, ".", 2)
			if len(parts) != 2 {
				continue
			}
			if stcls.shouldIgnoreMatchedTable(ignoreSchema, parts[0], parts[1]) {
				delete(schema, k)
			}
		}

		// 构建返回列表
		for k, _ := range schema {
			parts := strings.Split(k, ".")
			if len(parts) == 2 {
				schemaName := parts[0]
				tableName := parts[1]

				// 查找源端schema名
				sourceSchema := schemaName
				destSchema := schemaName

				// 检查是否存在映射关系
				if mappedSchema, exists := stcls.tableMappings[schemaName]; exists {
					destSchema = mappedSchema
				}

				// 创建表映射
				mapping := TableMapping{
					SourceSchema: sourceSchema,
					SourceTable:  tableName,
					DestSchema:   destSchema,
					DestTable:    tableName,
				}
				tableMappings = append(tableMappings, mapping)

				vlog = fmt.Sprintf("Added default mapping: %s.%s -> %s.%s", sourceSchema, tableName, destSchema, tableName)
				global.Wlog.Debug(vlog)
			}
		}
	}

	// 将表映射信息转换为字符串列表，格式为 "sourceSchema.sourceTable:destSchema.destTable"
	for _, mapping := range tableMappings {
		// 构建包含映射信息的表名
		mappedTableName := fmt.Sprintf("%s.%s:%s.%s", mapping.SourceSchema, mapping.SourceTable, mapping.DestSchema, mapping.DestTable)
		f = append(f, mappedTableName)

		// 如果源表和目标表不同，则添加到映射关系列表中
		if mapping.SourceSchema != mapping.DestSchema || mapping.SourceTable != mapping.DestTable {
			mappingRelation := fmt.Sprintf("%s.%s:%s.%s", mapping.SourceSchema, mapping.SourceTable, mapping.DestSchema, mapping.DestTable)
			// 检查是否已存在相同的映射关系
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
		}

		vlog = fmt.Sprintf("Final mapped table: %s", mappedTableName)
		global.Wlog.Debug(vlog)
	}

	// For data mode: remove VIEW objects from the check list.
	// Views do not store data independently; including them causes the checksum
	// to run against the view's underlying query, which can hang when the
	// DEFINER account no longer exists (issue #I899YZ).
	if strings.EqualFold(stcls.checkRules.CheckObject, "data") && len(stcls.objectKinds) > 0 {
		filtered := f[:0]
		skipped := 0
		for _, entry := range f {
			// entry format: "srcSchema.srcTable:dstSchema.dstTable"
			srcPart := entry
			if idx := strings.Index(entry, ":"); idx >= 0 {
				srcPart = entry[:idx]
			}
			parts := strings.SplitN(srcPart, ".", 2)
			if len(parts) == 2 {
				key := fmt.Sprintf("%s/*schema&table*/%s", parts[0], parts[1])
				if strings.EqualFold(strings.ToLower(stcls.caseSensitiveObjectName), "no") {
					key = strings.ToLower(key)
				}
				if stcls.objectKinds[key] == "VIEW" {
					vlog = fmt.Sprintf("(%d) Skipping VIEW in data mode: %s", logThreadSeq1, srcPart)
					global.Wlog.Info(vlog)
					skipped++
					continue
				}
			}
			filtered = append(filtered, entry)
		}
		if skipped > 0 {
			f = filtered
			vlog = fmt.Sprintf("(%d) data mode: skipped %d VIEW object(s), %d object(s) remain.", logThreadSeq1, skipped, len(f))
			global.Wlog.Info(vlog)
		}
	}

	vlog = fmt.Sprintf("(%d) Obtain schema.table %s success, num [%d].", logThreadSeq1, f, len(f))
	global.Wlog.Info(vlog)
	return f, nil
}

func parseSchemaTableMappingEntry(entry string) (sourceSchema, sourceTable, destSchema, destTable string, ok bool) {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return "", "", "", "", false
	}

	if strings.Contains(entry, ":") {
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			return "", "", "", "", false
		}

		sourceParts := strings.Split(parts[0], ".")
		destParts := strings.Split(parts[1], ".")
		if len(sourceParts) != 2 || len(destParts) != 2 {
			return "", "", "", "", false
		}

		if sourceParts[0] == "" || sourceParts[1] == "" || destParts[0] == "" || destParts[1] == "" {
			return "", "", "", "", false
		}

		return sourceParts[0], sourceParts[1], destParts[0], destParts[1], true
	}

	parts := strings.Split(entry, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", "", "", false
	}

	return parts[0], parts[1], parts[0], parts[1], true
}

func (stcls *schemaTable) parseTableMappings(Ftable string) {
	stcls.tableMappings = make(map[string]string)

	vlog := fmt.Sprintf("Parsing table mappings for pattern: %s", Ftable)
	global.Wlog.Debug(vlog)

	// 解析映射规则，如 db1.*:db2.*
	for _, pattern := range strings.Split(Ftable, ",") {
		vlog = fmt.Sprintf("Processing pattern: %s", pattern)
		global.Wlog.Debug(vlog)

		if strings.Contains(pattern, ":") {
			mapping := strings.SplitN(pattern, ":", 2)
			if len(mapping) == 2 {
				srcPattern := mapping[0]
				dstPattern := mapping[1]

				vlog = fmt.Sprintf("Found mapping: %s -> %s", srcPattern, dstPattern)
				global.Wlog.Debug(vlog)

				// 处理 db1.*:db2.* 格式
				if strings.HasSuffix(srcPattern, ".*") && strings.HasSuffix(dstPattern, ".*") {
					srcDB := strings.TrimSuffix(srcPattern, ".*")
					dstDB := strings.TrimSuffix(dstPattern, ".*")
					stcls.tableMappings[srcDB] = dstDB
					vlog = fmt.Sprintf("Mapped (.* format): %s -> %s", srcDB, dstDB)
					global.Wlog.Debug(vlog)
				} else if strings.HasSuffix(srcPattern, "*") && strings.HasSuffix(dstPattern, "*") {
					// 处理 db1*:db2* 格式 (针对用户输入的"db1.*:db2.*"但实际被解析为"db1*:db2*"的情况)
					srcDB := strings.TrimSuffix(srcPattern, "*")
					dstDB := strings.TrimSuffix(dstPattern, "*")
					stcls.tableMappings[srcDB] = dstDB
					vlog = fmt.Sprintf("Mapped (* format): %s -> %s", srcDB, dstDB)
					global.Wlog.Debug(vlog)
				} else {
					// 处理其他格式的映射，如 db1.t1:db2.t2
					srcParts := strings.Split(srcPattern, ".")
					dstParts := strings.Split(dstPattern, ".")

					if len(srcParts) > 0 && len(dstParts) > 0 {
						srcDB := srcParts[0]
						dstDB := dstParts[0]
						stcls.tableMappings[srcDB] = dstDB
						vlog = fmt.Sprintf("Mapped (direct format): %s -> %s", srcDB, dstDB)
						global.Wlog.Debug(vlog)
					}
				}
			}
		} else {
			// 处理非映射模式，如 db1.*
			if strings.HasSuffix(pattern, ".*") {
				srcDB := strings.TrimSuffix(pattern, ".*")
				stcls.tableMappings[srcDB] = srcDB // 没有映射时，源和目标相同
				vlog = fmt.Sprintf("Non-mapping pattern (.* format): %s", srcDB)
				global.Wlog.Debug(vlog)
			} else if strings.HasSuffix(pattern, "*") {
				srcDB := strings.TrimSuffix(pattern, "*")
				stcls.tableMappings[srcDB] = srcDB // 没有映射时，源和目标相同
				vlog = fmt.Sprintf("Non-mapping pattern (* format): %s", srcDB)
				global.Wlog.Debug(vlog)
			} else if strings.Contains(pattern, ".") {
				// 处理 db1.t1 格式
				srcParts := strings.Split(pattern, ".")
				if len(srcParts) > 0 {
					srcDB := srcParts[0]
					stcls.tableMappings[srcDB] = srcDB
					vlog = fmt.Sprintf("Non-mapping pattern (direct format): %s", srcDB)
					global.Wlog.Debug(vlog)
				}
			}
		}
	}

	vlog = fmt.Sprintf("Final table mappings: %v", stcls.tableMappings)
	global.Wlog.Debug(vlog)
}
