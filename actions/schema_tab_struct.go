package actions

import (
	"database/sql"
	"encoding/json"
	"fmt"
	mysql "gt-checksum/MySQL"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// 全局变量
var (
	// 用于存储表映射关系
	TableMappingRelations []string
	// 用于存储索引检查结果的全局变量
	indexDiffsMap map[string]bool
	// 用于存储分区检查结果的全局变量
	partitionDiffsMap map[string]bool
	// 用于存储外键检查结果的全局变量
	foreignKeyDiffsMap map[string]bool
)

// measuredDataPods 在 terminal_result_output.go 中已定义

type schemaTable struct {
	// 现有字段...
	aggregate bool // 是否启用缓冲聚合（最小入侵新增）
	// 统一缓冲，用 CheckObject 区分 proc/func（最小入侵新增）
	podsBuffer              []Pod
	schema                  string
	table                   string
	destTable               string // 目标表名，可能与源表名不同
	ignoreSchema            string
	ignoreTable             string
	sourceDrive             string
	destDrive               string
	sourceDB                *sql.DB
	destDB                  *sql.DB
	caseSensitiveObjectName string
	datafix                 string
	sfile                   *os.File
	djdbc                   string
	checkRules              inputArg.RulesS
	// 添加表映射规则
	tableMappings map[string]string
	// 需要跳过索引检查的表列表
	skipIndexCheckTables []string
	// 列修复操作映射表，用于合并列和索引操作
	columnRepairMap map[string][]string
}

// normalizeStoredProcBody 规范化存储过程体，以便更准确地比较
// 规范化处理包括：
// 1. 移除多余的空格和换行符
// 2. 将所有空白字符规范化为单个空格
// 3. 移除注释
// 4. 将所有关键字转换为大写（可选，取决于数据库的大小写敏感性）
// 5. 规范化算术表达式，移除不必要的空格
func normalizeStoredProcBody(body string) string {
	if body == "" {
		return ""
	}

	// 记录原始内容，用于调试
	originalBody := body

	// 保存GT_CHECKSUM_METADATA注释
	metadataRegex := regexp.MustCompile(`/\*GT_CHECKSUM_METADATA:(.*?)\*/`)
	// 暂时移除元数据注释，以便不影响其他处理
	body = metadataRegex.ReplaceAllString(body, "")

	// 移除注释
	// 这里简化处理，实际可能需要更复杂的正则表达式
	re := regexp.MustCompile(`--.*?\n|/\*[\s\S]*?\*/`)
	body = re.ReplaceAllString(body, " ")

	// 规范化空白字符
	re = regexp.MustCompile(`\s+`)
	body = re.ReplaceAllString(body, " ")

	// 移除开头和结尾的空格
	body = strings.TrimSpace(body)

	// 注意：不再规范化算术表达式，因为这会导致功能性差异被忽略
	// 例如，n1 + n2 和 n1 + n2*2 应该被视为不同的表达式

	// 如果规范化后的内容与原始内容有显著差异，记录日志
	if len(originalBody) > 0 && float64(len(body))/float64(len(originalBody)) < 0.5 {
		global.Wlog.Warn(fmt.Sprintf("Significant difference after normalization. Original length: %d, Normalized length: %d", len(originalBody), len(body)))
	}

	return body
}

// extractMetadataFromProcedure 从存储过程定义中提取元数据
func extractMetadataFromProcedure(procDef string) map[string]string {
	metadata := make(map[string]string)

	// 查找GT_CHECKSUM_METADATA注释
	metadataRegex := regexp.MustCompile(`/\*GT_CHECKSUM_METADATA:(.*?)\*/`)
	metadataMatches := metadataRegex.FindStringSubmatch(procDef)

	if len(metadataMatches) > 1 {
		// 解析JSON格式的元数据
		jsonStr := metadataMatches[1]
		var metadataMap map[string]interface{}

		// 尝试解析JSON
		err := json.Unmarshal([]byte(jsonStr), &metadataMap)
		if err == nil {
			// 将解析后的元数据添加到结果映射中
			for key, value := range metadataMap {
				metadata[strings.ToUpper(key)] = fmt.Sprintf("%v", value)
			}
		}
	}

	// 提取DEFINER信息
	definerRegex := regexp.MustCompile(`CREATE\s+DEFINER\s*=\s*['"]?([^'"]*)['"]?@['"]?([^'"]*)['"]?`)
	definerMatches := definerRegex.FindStringSubmatch(procDef)
	if len(definerMatches) > 2 {
		metadata["DEFINER"] = fmt.Sprintf("%s@%s", definerMatches[1], definerMatches[2])
	}

	// 提取SQL_MODE
	sqlModeRegex := regexp.MustCompile(`SQL\s+SECURITY\s+(\w+)`)
	sqlModeMatches := sqlModeRegex.FindStringSubmatch(procDef)
	if len(sqlModeMatches) > 1 {
		metadata["SQL_MODE"] = sqlModeMatches[1]
	}

	// 提取CHARACTER_SET_CLIENT
	charsetRegex := regexp.MustCompile(`CHARACTER_SET_CLIENT\s*=\s*(\w+)`)
	charsetMatches := charsetRegex.FindStringSubmatch(procDef)
	if len(charsetMatches) > 1 {
		metadata["CHARACTER_SET_CLIENT"] = charsetMatches[1]
	}

	// 提取COLLATION_CONNECTION
	collationRegex := regexp.MustCompile(`COLLATION_CONNECTION\s*=\s*(\w+)`)
	collationMatches := collationRegex.FindStringSubmatch(procDef)
	if len(collationMatches) > 1 {
		metadata["COLLATION_CONNECTION"] = collationMatches[1]
	}

	// 提取DATABASE_COLLATION
	dbCollationRegex := regexp.MustCompile(`DATABASE\s+COLLATION\s*=\s*(\w+)`)
	dbCollationMatches := dbCollationRegex.FindStringSubmatch(procDef)
	if len(dbCollationMatches) > 1 {
		metadata["DATABASE_COLLATION"] = dbCollationMatches[1]
	}

	return metadata
}

// getDisplayTableName 返回表的显示名称，包含映射关系信息
// 如果存在映射关系，返回格式为 "sourceSchema.table:destSchema.table"
// 如果不存在映射关系，返回格式为 "schema.table"
func (stcls *schemaTable) getDisplayTableName(schema, table string) string {
	// 检查是否存在映射关系
	if mappedSchema, exists := stcls.tableMappings[schema]; exists && mappedSchema != schema {
		// 存在映射关系，返回包含映射信息的名称
		return fmt.Sprintf("%s.%s:%s.%s", schema, table, mappedSchema, table)
	}

	// 不存在映射关系，返回普通名称
	return fmt.Sprintf("%s.%s", schema, table)
}

// getSourceTableName 返回源表的名称
func (stcls *schemaTable) getSourceTableName(schema, table string) string {
	return fmt.Sprintf("%s.%s", schema, table)
}

// getDestTableName 返回目标表的名称
func (stcls *schemaTable) getDestTableName(schema, table string) string {
	destSchema := schema
	if mappedSchema, exists := stcls.tableMappings[schema]; exists {
		destSchema = mappedSchema
	}
	return fmt.Sprintf("%s.%s", destSchema, table)
}

/*
查询待校验表的列名
*/
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
	if queryData, err = tc.Query().TableColumnName(db, logThreadSeq2); err != nil {
		return col, err
	}
	vlog = fmt.Sprintf("(%d) [%s] Starting column validation", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	for _, v := range queryData {
		if fmt.Sprintf("%v", v["columnName"]) != "" {
			// 获取extra属性，包含AUTO_INCREMENT和INVISIBLE等特殊属性
			extra := C(fmt.Sprintf("%v", v["extra"]))
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

/*
校验表的列名是否正确
*/
func (stcls *schemaTable) TableColumnNameCheck(checkTableList []string, logThreadSeq, logThreadSeq2 int64) ([]string, []string, error) {
	var (
		vlog                                 string
		newCheckTableList, abnormalTableList []string
		aa                                   = &CheckSumTypeStruct{}
		err                                  error
		tableAbnormalBool                    = false
		event                                string
	)
	vlog = fmt.Sprintf("(%d) %s Validating structure differences between source and target", logThreadSeq, event)
	global.Wlog.Debug(vlog)
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

		vlog = fmt.Sprintf("(%d %s Validating table structure %s.%s -> %s.%s", logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table)
		global.Wlog.Debug(vlog)

		// 检查源表是否存在
		sourceTableExists := true
		sourceTableQuery := fmt.Sprintf("SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", sourceSchema, sourceTableName)
		var exists int
		err = stcls.sourceDB.QueryRow(sourceTableQuery).Scan(&exists)
		if err == sql.ErrNoRows {
			sourceTableExists = false
			vlog = fmt.Sprintf("(%d) %s Source table %s.%s does not exist", logThreadSeq, event, sourceSchema, stcls.table)
			global.Wlog.Warn(vlog)
		} else if err != nil {
			vlog = fmt.Sprintf("(%d) %s Error checking source table existence %s.%s: %v", logThreadSeq, event, sourceSchema, stcls.table, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}

		// 检查目标表是否存在
		destTableExists := true
		destTableQuery := fmt.Sprintf("SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", destSchema, destTableName)
		err = stcls.destDB.QueryRow(destTableQuery).Scan(&exists)
		if err == sql.ErrNoRows {
			destTableExists = false
			vlog = fmt.Sprintf("(%d) %s Target table %s.%s does not exist", logThreadSeq, event, destSchema, stcls.table)
			global.Wlog.Warn(vlog)
		} else if err != nil {
			vlog = fmt.Sprintf("(%d) %s Error checking target table existence %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}

		// 处理特殊情况：源表存在但目标表不存在
		if sourceTableExists && !destTableExists {
			// 生成CREATE TABLE语句
			createTableSql, err := generateCreateTableSql(stcls.sourceDB, sourceSchema, destSchema, sourceTableName, logThreadSeq)
			if err != nil {
				vlog = fmt.Sprintf("(%d) %s Error generating CREATE TABLE statement for %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, err)
				global.Wlog.Error(vlog)
				return nil, nil, err
			}

			vlog = fmt.Sprintf("(%d) %s Generated CREATE TABLE statement for %s.%s: %s", logThreadSeq, event, destSchema, destTableName, createTableSql)
			global.Wlog.Debug(vlog)

			// 应用修复SQL
			vlog = fmt.Sprintf("(%d) %s Applying CREATE TABLE statement to %s.%s", logThreadSeq, event, destSchema, destTableName)
			global.Wlog.Debug(vlog)
			if err = mysql.WriteFixIfNeededFile(stcls.datafix, stcls.sfile, []string{createTableSql}, logThreadSeq); err != nil {
				return nil, nil, err
			}

			abnormalTableList = append(abnormalTableList, fmt.Sprintf("%s.%s", destSchema, destTableName))
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
			if err = mysql.WriteFixIfNeededFile(stcls.datafix, stcls.sfile, []string{dropTableSql}, logThreadSeq); err != nil {
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
			Table:                   stcls.table,
			DestDevice:              stcls.destDrive,
			DatafixType:             stcls.datafix,
			SourceSchema:            sourceSchema, // 添加源端schema
			CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
		}
		tc := dbExec.TableColumnNameStruct{Schema: sourceSchema, Table: stcls.table, Drive: stcls.sourceDrive}
		sColumn, err = stcls.tableColumnName(stcls.sourceDB, tc, logThreadSeq, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Failed to get metadata for source table %s.%s: %v", logThreadSeq, event, sourceSchema, stcls.table, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}
		vlog = fmt.Sprintf("(%d) %s Source table %s.%s has %d columns", logThreadSeq, event, sourceSchema, stcls.table, len(sColumn))
		global.Wlog.Debug(vlog)

		// 使用目标端schema
		tc.Schema = destSchema
		tc.Drive = stcls.destDrive
		dColumn, err = stcls.tableColumnName(stcls.destDB, tc, logThreadSeq, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Failed to get metadata for target table %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}
		vlog = fmt.Sprintf("(%d) %s Target table %s.%s has %d columns", logThreadSeq, event, destSchema, stcls.table, len(dColumn))
		global.Wlog.Debug(vlog)

		alterSlice := []string{}
		var sourceColumnSlice, destColumnSlice []string
		var sourceColumnMap, destColumnMap = make(map[string][]string), make(map[string][]string)
		var sourceColumnSeq, destColumnSeq = make(map[string]int), make(map[string]int)
		// 创建原始列名映射，用于保存原始大小写
		var originalColumnNameMap = make(map[string]string)

		for k1, v1 := range sColumn {
			v1k := ""
			for k, v22 := range v1 {
				// 保存原始列名
				originalColumnNameMap[strings.ToUpper(k)] = k

				// 根据caseSensitiveObjectName决定是使用原始列名还是大写列名进行比较
				if stcls.caseSensitiveObjectName == "yes" {
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
				// 保存原始列名
				originalColumnNameMap[strings.ToUpper(k)] = k

				// 根据caseSensitiveObjectName决定是使用原始列名还是大写列名进行比较
				if stcls.caseSensitiveObjectName == "yes" {
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
		getOriginalColumnName := func(colName string) string {
			// 根据caseSensitiveObjectName决定如何查找原始列名
			if stcls.caseSensitiveObjectName == "yes" {
				// 严格区分大小写时，colName已经是原始列名，直接返回
				return colName
			} else {
				// 不区分大小写时，使用大写列名作为键查找原始列名
				upperColName := strings.ToUpper(colName)
				if originalName, exists := originalColumnNameMap[upperColName]; exists {
					return originalName
				}
				return colName
			}
		}

		addColumn, delColumn := aa.Arrcmp(sourceColumnSlice, destColumnSlice)

		// 检查是否只是列名大小写不同的情况
		// 当caseSensitiveObjectName=yes时，我们需要特殊处理大小写不同但实际上是同一列的情况
		if stcls.caseSensitiveObjectName == "yes" {
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

		if stcls.checkRules.CheckObject == "data" {
			if len(addColumn) == 0 && len(delColumn) == 0 {
				// 使用目标端schema
				newCheckTableList = append(newCheckTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
			} else {
				// 使用正确的源和目标数据库名
				vlog = fmt.Sprintf("(%d) %s Structure mismatch %s.%s -> %s.%s - Extra: %v, Missing: %v", logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table, addColumn, delColumn)
				global.Wlog.Error(vlog)
				abnormalTableList = append(abnormalTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
			}
			// 当checkObject=data时，只进行数据校验，不进行表结构校验或生成修改表结构的SQL语句
			continue
		}

		vlog = fmt.Sprintf("(%d) %s Columns to remove from target %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, delColumn)
		global.Wlog.Debug(vlog)
		// 先删除缺失的
		if len(delColumn) > 0 {
			// 收集所有需要删除的列名
			var colsToDelete []string
			for _, v1 := range delColumn {
				// 使用原始大小写的列名生成SQL
				originalColName := getOriginalColumnName(v1)
				dropSql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("drop", destColumnMap[v1], 1, "", originalColName, logThreadSeq)
				alterSlice = append(alterSlice, dropSql)
				colsToDelete = append(colsToDelete, v1)
			}
			// 在循环外删除所有标记的列
			for _, col := range colsToDelete {
				delete(destColumnMap, col)
			}
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

				// 获取原始大小写的列名
				originalColName := getOriginalColumnName(v1)

				// 打印调试信息
				vlog = fmt.Sprintf("(%d) %s Column %s type comparison: source=%s, dest=%s", logThreadSeq, event, originalColName, sourceType, destType)
				global.Wlog.Debug(vlog)

				// 比较列类型
				if sourceType != destType {
					tableAbnormalBool = true
					vlog = fmt.Sprintf("(%d) %s Column %s type mismatch: source=%s, dest=%s", logThreadSeq, event, originalColName, sourceType, destType)
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
					if sourceCharset != destCharset {
						tableAbnormalBool = true
						vlog = fmt.Sprintf("(%d) %s Column %s charset mismatch: source=%s, dest=%s",
							logThreadSeq, event, originalColName, sourceCharset, destCharset)
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
					if sourceCollation != destCollation {
						tableAbnormalBool = true
						vlog = fmt.Sprintf("(%d) %s Column %s collation mismatch: source=%s, dest=%s",
							logThreadSeq, event, originalColName, sourceCollation, destCollation)
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

				if sourceIsNull != destIsNull {
					tableAbnormalBool = true
					vlog = fmt.Sprintf("(%d) %s Column %s NULL constraint mismatch: source=%s, dest=%s",
						logThreadSeq, event, originalColName, sourceIsNull, destIsNull)
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
					if sourceDefault != destDefault {
						tableAbnormalBool = true
						vlog = fmt.Sprintf("(%d) %s Column %s default value mismatch: source=%s, dest=%s",
							logThreadSeq, event, originalColName, sourceDefault, destDefault)
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
						logThreadSeq, event, originalColName, sourceColumnSeq[v1], destColumnSeq[v1])
					global.Wlog.Warn(vlog)
				}
				if tableAbnormalBool {
					// 使用原始大小写的列名生成SQL
					originalColName := getOriginalColumnName(v1)
					originalLastColumn := getOriginalColumnName(lastcolumn)
					modifySql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("modify", alterColumnData, k1, originalLastColumn, originalColName, logThreadSeq)
					vlog = fmt.Sprintf("(%d) %s The column name of column %s of the source and target table %s.%s:[%s.%s] is the same, but the definition of the column is inconsistent, and a modify statement is generated, and the modification statement is {%v}", logThreadSeq, originalColName, stcls.schema, stcls.table, destSchema, stcls.table, modifySql)
					global.Wlog.Warn(vlog)
					alterSlice = append(alterSlice, modifySql)
				}
				delete(destColumnMap, v1)
			} else {
				// 使用固定值：ScheckOrder=yes
				lastcolumn = lastcolumn
				var position int
				// 使用固定值：ScheckOrder=yes，总是使用源列的实际位置
				position = k1
				// 使用原始大小写的列名生成SQL
				originalColName := getOriginalColumnName(v1)
				originalLastColumn := getOriginalColumnName(lastcolumn)
				addSql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("add", sourceColumnMap[v1], position, originalLastColumn, originalColName, logThreadSeq)
				vlog = fmt.Sprintf("(%d) %s Missing column %s in %s.%s - ADD: %v", logThreadSeq, event, originalColName, destSchema, stcls.table, addSql)
				global.Wlog.Warn(vlog)
				alterSlice = append(alterSlice, addSql)
				delete(destColumnMap, v1)
			}
		}

		// 在TableColumnNameCheck函数中，在比较完列级别的属性后，添加表级别字符集和排序规则的比较
		// 在生成alterSlice后，添加以下代码

		// 检查表级别的字符集和排序规则
		tableCharsetCollationQuery := fmt.Sprintf(`
    SELECT t.TABLE_COLLATION, c.CHARACTER_SET_NAME 
    FROM information_schema.TABLES t 
    JOIN information_schema.COLLATIONS c ON t.TABLE_COLLATION = c.COLLATION_NAME 
    WHERE t.TABLE_SCHEMA = '%s' AND t.TABLE_NAME = '%s'
`, sourceSchema, stcls.table)

		var sourceTableCollation, sourceTableCharset string
		rows, err := stcls.sourceDB.Query(tableCharsetCollationQuery)
		if err == nil {
			defer rows.Close()
			if rows.Next() {
				err = rows.Scan(&sourceTableCollation, &sourceTableCharset)
				if err != nil {
					vlog = fmt.Sprintf("(%d) %s Failed to scan source table charset/collation: %v", logThreadSeq, event, err)
					global.Wlog.Error(vlog)
				}
			}
		}

		tableCharsetCollationQuery = fmt.Sprintf(`
    SELECT t.TABLE_COLLATION, c.CHARACTER_SET_NAME 
    FROM information_schema.TABLES t 
    JOIN information_schema.COLLATIONS c ON t.TABLE_COLLATION = c.COLLATION_NAME 
    WHERE t.TABLE_SCHEMA = '%s' AND t.TABLE_NAME = '%s'
`, destSchema, stcls.table)

		var destTableCollation, destTableCharset string
		rows, err = stcls.destDB.Query(tableCharsetCollationQuery)
		if err == nil {
			defer rows.Close()
			if rows.Next() {
				err = rows.Scan(&destTableCollation, &destTableCharset)
				if err != nil {
					vlog = fmt.Sprintf("(%d) %s Failed to scan dest table charset/collation: %v", logThreadSeq, event, err)
					global.Wlog.Error(vlog)
				}
			}
		}

		// 比较表级别的字符集和排序规则
		tableCharsetDifferent := false
		if sourceTableCharset != "" && destTableCharset != "" && sourceTableCharset != destTableCharset {
			tableCharsetDifferent = true
			vlog = fmt.Sprintf("(%d) %s Table charset mismatch: source=%s, dest=%s",
				logThreadSeq, event, sourceTableCharset, destTableCharset)
			global.Wlog.Warn(vlog)
		}

		tableCollationDifferent := false
		if sourceTableCollation != "" && destTableCollation != "" && sourceTableCollation != destTableCollation {
			tableCollationDifferent = true
			vlog = fmt.Sprintf("(%d) %s Table collation mismatch: source=%s, dest=%s",
				logThreadSeq, event, sourceTableCollation, destTableCollation)
			global.Wlog.Warn(vlog)
		}

		// 先生成列级别的修复SQL
		sqlS := dbf.DataAbnormalFix().FixAlterColumnSqlGenerate(alterSlice, logThreadSeq)

		// 如果表级别的字符集或排序规则不一致，生成修复SQL
		if tableCharsetDifferent || tableCollationDifferent {
			// 生成表级别字符集转换的SQL语句
			convertSqlS := dbf.DataAbnormalFix().FixTableCharsetSqlGenerate(sourceTableCharset, sourceTableCollation, logThreadSeq)

			// 无论datafix是什么值，都将表级别字符集转换的SQL语句添加到sqlS中
			sqlS = append(sqlS, convertSqlS...)
		}

		// 检查表注释是否一致并生成修复SQL
		var sourceTableComment, destTableComment string
		var errTableComment error

		// 根据数据库类型创建相应的查询实例
		if stcls.sourceDrive == "mysql" {
			mysqlQuery := &mysql.QueryTable{Schema: stcls.schema, Table: stcls.table}
			sourceTableComment, errTableComment = mysqlQuery.TableComment(stcls.sourceDB, logThreadSeq)
			if errTableComment == nil && sourceTableComment != "" {
				mysqlQuery.Schema = destSchema
				destTableComment, errTableComment = mysqlQuery.TableComment(stcls.destDB, logThreadSeq)
				if errTableComment == nil && sourceTableComment != destTableComment {
					// 生成修改表注释的SQL语句
					tableCommentSql := fmt.Sprintf("ALTER TABLE `%s`.`%s` COMMENT = '%s';", destSchema, stcls.table, strings.ReplaceAll(sourceTableComment, "'", "\\'"))
					vlog = fmt.Sprintf("(%d) %s Table comment mismatch: source='%s', dest='%s', generating fix SQL", logThreadSeq, event, sourceTableComment, destTableComment)
					global.Wlog.Warn(vlog)
					sqlS = append(sqlS, tableCommentSql)
				}
			}
		}

		if len(alterSlice) > 0 {
			abnormalTableList = append(abnormalTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
		} else {
			newCheckTableList = append(newCheckTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
		}

		vlog = fmt.Sprintf("(%d) %s Structure validation completed for %s.%s -> %s.%s", logThreadSeq, event, stcls.schema, stcls.table, destSchema, stcls.table)
		global.Wlog.Debug(vlog)

		// 如果sqlS不为空（表示没有应用过列级别修复），则应用它
		if len(sqlS) > 0 {
			vlog = fmt.Sprintf("(%d) %s Applying repair statements to %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, sqlS)
			global.Wlog.Debug(vlog)
			// 统一封装为按 datafix=file 追加写入
			if err = mysql.WriteFixIfNeededFile(stcls.datafix, stcls.sfile, sqlS, logThreadSeq); err != nil {
				return nil, nil, err
			}
			vlog = fmt.Sprintf("(%d) %s Repair statements applied to %s.%s", logThreadSeq, event, destSchema, stcls.table)
			global.Wlog.Debug(vlog)
		}
	}
	vlog = fmt.Sprintf("(%d) %s Table structure validation completed", logThreadSeq, event)
	global.Wlog.Info(vlog)

	return newCheckTableList, abnormalTableList, nil
}

/*
该函数用于获取MySQL的表的索引信息,判断表是否存在索引，加入存在，获取索引的类型，以主键索引、唯一索引、普通索引及无索引，主键索引或唯一索引以自增id为优先

	缺少索引列为空或null的处理
*/
func (stcls *schemaTable) tableIndexAlgorithm(indexType map[string][]string) (string, []string) {
	if len(indexType) > 0 {
		// 优先选择主键索引
		if len(indexType["pri_single"]) > 0 {
			return "pri_single", indexType["pri_single"]
		}
		if len(indexType["pri_multi"]) > 0 {
			return "pri_multi", indexType["pri_multi"]
		}

		// 其次选择唯一索引
		if len(indexType["uni_single"]) > 0 {
			return "uni_single", indexType["uni_single"]
		}
		if len(indexType["uni_multi"]) > 0 {
			return "uni_multi", indexType["uni_multi"]
		}

		// 最后选择普通索引
		if len(indexType["mul_single"]) > 0 {
			return "mul_single", indexType["mul_single"]
		}
		if len(indexType["mul_multi"]) > 0 {
			return "mul_multi", indexType["mul_multi"]
		}
	}
	return "", []string{}
}

// 处理模糊匹配，支持数据库映射规则
func (stcls *schemaTable) FuzzyMatchingDispos(dbCheckNameList map[string]int, Ftable string, logThreadSeq int64) map[string]int {
	var (
		schema string
		vlog   string
	)
	b := make(map[string]int)
	f := make(map[string]int)

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
			for k, _ := range dbCheckNameList {
				b[k]++
				vlog = fmt.Sprintf("Added wildcard schema: %s", k)
				global.Wlog.Debug(vlog)
			}
		} else if strings.HasPrefix(schema, "%") && strings.HasSuffix(schema, "%") { //处理%schema%
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for k, _ := range dbCheckNameList {
				if strings.Contains(k, tmpschema) {
					b[k]++
					vlog = fmt.Sprintf("Added %schema% match: %s", k)
					global.Wlog.Debug(vlog)
				}
			}
		} else if strings.HasPrefix(schema, "%") && !strings.HasSuffix(schema, "%") { //处理%schema
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for k, _ := range dbCheckNameList {
				if strings.HasSuffix(k, tmpschema) {
					b[k]++
					vlog = fmt.Sprintf("Added %schema match: %s", k)
					global.Wlog.Debug(vlog)
				}
			}
		} else if !strings.HasPrefix(schema, "%") && strings.HasSuffix(schema, "%") { //处理schema%
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for k, _ := range dbCheckNameList {
				if strings.HasPrefix(k, tmpschema) {
					b[k]++
					vlog = fmt.Sprintf("Added schema% match: %s", k)
					global.Wlog.Debug(vlog)
				}
			}
		} else { //处理schema
			// 检查是否在映射规则中存在
			if _, exists := stcls.tableMappings[schema]; exists {
				// schema是源端schema，直接添加
				b[schema]++
				vlog = fmt.Sprintf("Added source schema from mapping: %s", schema)
				global.Wlog.Debug(vlog)
			} else if hasMappingRule {
				// 如果有明确的映射规则，尝试使用它
				dstSchema := ""
				if strings.Contains(dstPattern, ".") {
					dstSchema = dstPattern[:strings.Index(dstPattern, ".")]
				} else {
					dstSchema = dstPattern
				}

				// 检查源schema是否存在于数据库列表中
				if _, exists := dbCheckNameList[schema]; exists {
					b[schema]++
					vlog = fmt.Sprintf("Added explicit mapping source schema: %s -> %s", schema, dstSchema)
					global.Wlog.Debug(vlog)
				}
			} else {
				// 检查是否是目标端schema
				found := false
				for src, dst := range stcls.tableMappings {
					if dst == schema {
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
					// 检查schema是否存在于数据库列表中
					if _, exists := dbCheckNameList[schema]; exists {
						b[schema]++
						vlog = fmt.Sprintf("Added direct schema (no mapping): %s", schema)
						global.Wlog.Debug(vlog)
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
			if mapped, exists := stcls.tableMappings[dbSchema]; exists {
				mappedSchema = mapped
				vlog = fmt.Sprintf("Found schema mapping: %s -> %s", dbSchema, mappedSchema)
				global.Wlog.Debug(vlog)
			}

			// 检查schema是否匹配
			if dbSchema == schema || schema == "*" {
				// 构建表名查询
				for dbName, _ := range dbCheckNameList {
					dbParts := strings.Split(dbName, "/*schema&table*/")
					if len(dbParts) < 2 {
						continue
					}

					dbSchemaName := dbParts[0]
					dbTableName := dbParts[1]

					// 检查schema是否匹配
					if dbSchemaName != dbSchema {
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
							vlog = fmt.Sprintf("Added table% match: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					} else if strings.HasPrefix(table, "%") && strings.HasSuffix(table, "%") { // 处理schema.%table%
						tmptable := strings.ReplaceAll(table, "%", "")
						if strings.Contains(dbTableName, tmptable) {
							f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
							vlog = fmt.Sprintf("Added %table% match: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					} else { // 处理schema.table
						if strings.EqualFold(dbTableName, table) {
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
	if dbCheckNameList, err = tc.Query().DatabaseNameList(stcls.sourceDB, logThreadSeq2); err != nil {
		return f, err
	}
	vlog = fmt.Sprintf("(%d) Source databases list: %v", logThreadSeq1, dbCheckNameList)
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

		destDbList, err := tcDest.Query().DatabaseNameList(stcls.destDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Error getting destination databases list: %v", logThreadSeq1, err)
			global.Wlog.Error(vlog)
		} else {
			destDbCheckNameList = destDbList
			vlog = fmt.Sprintf("(%d) Destination databases list: %v", logThreadSeq1, destDbCheckNameList)
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

					// 获取源库中的所有表
					for dbName, _ := range dbCheckNameList {
						if strings.HasPrefix(dbName, srcDB+"/*schema&table*/") {
							tableName := strings.TrimPrefix(dbName, srcDB+"/*schema&table*/")

							// 创建表映射
							mapping := TableMapping{
								SourceSchema: srcDB,
								SourceTable:  tableName,
								DestSchema:   dstDB,
								DestTable:    tableName,
							}
							tableMappings = append(tableMappings, mapping)

							vlog = fmt.Sprintf("Added mapping: %s.%s -> %s.%s", srcDB, tableName, dstDB, tableName)
							global.Wlog.Debug(vlog)
						}
					}

					// 检查目标库中是否有源库中不存在的表
					for dbName, _ := range destDbCheckNameList {
						if strings.HasPrefix(dbName, dstDB+"/*schema&table*/") {
							tableName := strings.TrimPrefix(dbName, dstDB+"/*schema&table*/")

							// 检查这个表是否已经在映射列表中
							found := false
							for _, m := range tableMappings {
								if m.DestSchema == dstDB && m.DestTable == tableName {
									found = true
									break
								}
							}

							// 如果没有找到，添加新的映射
							if !found {
								mapping := TableMapping{
									SourceSchema: srcDB,
									SourceTable:  tableName,
									DestSchema:   dstDB,
									DestTable:    tableName,
								}
								tableMappings = append(tableMappings, mapping)

								vlog = fmt.Sprintf("Added mapping from dest table: %s.%s -> %s.%s", srcDB, tableName, dstDB, tableName)
								global.Wlog.Debug(vlog)
							}
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
		} else {
			// 处理非映射模式，如 db1.*
			if strings.HasSuffix(pattern, ".*") {
				srcDB := strings.TrimSuffix(pattern, ".*")

				// 获取该库中的所有表
				for dbName, _ := range dbCheckNameList {
					if strings.HasPrefix(dbName, srcDB+"/*schema&table*/") {
						tableName := strings.TrimPrefix(dbName, srcDB+"/*schema&table*/")

						// 创建表映射（源端和目标端相同）
						mapping := TableMapping{
							SourceSchema: srcDB,
							SourceTable:  tableName,
							DestSchema:   srcDB,
							DestTable:    tableName,
						}
						tableMappings = append(tableMappings, mapping)

						vlog = fmt.Sprintf("Added non-mapping entry: %s.%s", srcDB, tableName)
						global.Wlog.Debug(vlog)
					}
				}
			} else if strings.Contains(pattern, ".") {
				// 处理 db1.t1 格式
				parts := strings.Split(pattern, ".")
				if len(parts) == 2 {
					srcDB := parts[0]
					srcTable := parts[1]

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

	// 如果没有找到任何映射，尝试使用默认方式处理
	if len(tableMappings) == 0 {
		vlog = fmt.Sprintf("No mappings found, using default processing")
		global.Wlog.Debug(vlog)

		// 使用模糊匹配处理表名
		schema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.table, logThreadSeq1)

		// 处理忽略表
		ignoreSchema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.ignoreTable, logThreadSeq1)
		for k, _ := range ignoreSchema {
			if _, ok := schema[k]; ok {
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

	vlog = fmt.Sprintf("(%d) Obtain schema.table %s success, num [%d].", logThreadSeq1, f, len(f))
	global.Wlog.Info(vlog)
	return f, nil
}

/*
库表的所有列信息
*/
func (stcls *schemaTable) SchemaTableAllCol(tableList []string, logThreadSeq, logThreadSeq2 int64) map[string]global.TableAllColumnInfoS {
	var (
		a, b           []map[string]interface{}
		err            error
		vlog           string
		tableCol       = make(map[string]global.TableAllColumnInfoS)
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
	for _, i := range tableList {
		// 添加调试日志，查看当前处理的表项
		vlog = fmt.Sprintf("(%d) Processing table entry: %s", logThreadSeq, i)
		global.Wlog.Debug(vlog)

		var sourceSchema, tableName, destSchema string

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

					vlog = fmt.Sprintf("(%d) Parsed mapping: sourceSchema=%s, tableName=%s, destSchema=%s", logThreadSeq, sourceSchema, tableName, destSchema)
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

		vlog = fmt.Sprintf("(%d) Start to query all column information of srcDSN {%s} table %s.%s", logThreadSeq, stcls.sourceDrive, sourceSchema, tableName)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: sourceSchema, Table: tableName, Drive: stcls.sourceDrive}
		a, err = tc.Query().TableAllColumn(stcls.sourceDB, logThreadSeq2)
		if err != nil {
			return nil
		}
		vlog = fmt.Sprintf("(%d) All column information query of srcDSN {%s} table %s.%s is completed", logThreadSeq, stcls.sourceDrive, sourceSchema, tableName)
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) Start to query all column information of dstDSN {%s} table %s.%s", logThreadSeq, stcls.destDrive, destSchema, tableName)
		global.Wlog.Debug(vlog)
		tc.Schema = destSchema
		tc.Drive = stcls.destDrive
		b, err = tc.Query().TableAllColumn(stcls.destDB, logThreadSeq2)
		if err != nil {
			return nil
		}
		vlog = fmt.Sprintf("(%d) All column information query of dstDSN {%s} table %s.%s is completed", logThreadSeq, stcls.destDrive, destSchema, tableName)
		global.Wlog.Debug(vlog)
		tableCol[fmt.Sprintf("%s_gtchecksum_%s", destSchema, tableName)] = global.TableAllColumnInfoS{
			SColumnInfo: interfToString(a),
			DColumnInfo: interfToString(b),
		}
		vlog = fmt.Sprintf("(%d) all column information query of source table %s.%s and target table %s.%s is completed. table column message is {source: %s, dest: %s}", logThreadSeq, sourceSchema, tableName, destSchema, tableName, interfToString(a), interfToString(b))
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) The metadata information of the source target verification table has been obtained", logThreadSeq)
	global.Wlog.Info(vlog)
	return tableCol
}

/*
获取校验表的索引列信息，包含是否有索引，列名，列序号
*/
func (stcls *schemaTable) TableIndexColumn(dtabS []string, logThreadSeq, logThreadSeq2 int64) map[string][]string {
	var (
		queryData           []map[string]interface{}
		err                 error
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

	for _, i := range dtabS {
		vlog = fmt.Sprintf("Processing table entry: %s", i)
		global.Wlog.Debug(vlog)

		// 解析表映射信息
		var sourceSchema, sourceTable, destSchema, destTable string

		// 检查是否包含映射关系（格式为 sourceSchema.sourceTable:destSchema.destTable）
		if strings.Contains(i, ":") {
			parts := strings.Split(i, ":")
			if len(parts) == 2 {
				sourceParts := strings.Split(parts[0], ".")
				destParts := strings.Split(parts[1], ".")

				if len(sourceParts) == 2 && len(destParts) == 2 {
					sourceSchema = sourceParts[0]
					sourceTable = sourceParts[1]
					destSchema = destParts[0]
					destTable = destParts[1]
				}
			}
		} else {
			// 没有映射关系，源端和目标端相同
			parts := strings.Split(i, ".")
			if len(parts) == 2 {
				sourceSchema = parts[0]
				sourceTable = parts[1]
				destSchema = sourceSchema
				destTable = sourceTable
			}
		}

		// 设置当前表名
		stcls.table = sourceTable

		vlog = fmt.Sprintf("Parsed mapping: sourceSchema=%s, sourceTable=%s, destSchema=%s, destTable=%s",
			sourceSchema, sourceTable, destSchema, destTable)
		global.Wlog.Debug(vlog)

		vlog = fmt.Sprintf("(%d) Start querying the index list information of source table %s.%s and target table %s.%s.",
			logThreadSeq, sourceSchema, sourceTable, destSchema, destTable)
		global.Wlog.Debug(vlog)

		// 查询源端索引信息
		idxc := dbExec.IndexColumnStruct{Schema: sourceSchema, Table: sourceTable, Drivce: stcls.sourceDrive}
		queryData, err = idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Error querying source table index: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			continue
		}
		tc := dbExec.TableColumnNameStruct{Schema: sourceSchema, Table: sourceTable, Drive: stcls.sourceDrive, Db: stcls.sourceDB}
		indexType := tc.Query().TableIndexChoice(queryData, logThreadSeq2)
		vlog = fmt.Sprintf("(%d) Source table %s.%s index list information query completed. index list message is {%v}",
			logThreadSeq, sourceSchema, sourceTable, indexType)
		global.Wlog.Debug(vlog)

		// 查询目标端索引信息
		idxcDest := dbExec.IndexColumnStruct{Schema: destSchema, Table: destTable, Drivce: stcls.destDrive}
		queryDataDest, err := idxcDest.TableIndexColumn().QueryTableIndexColumnInfo(stcls.destDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Error querying destination table index: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			continue
		}

		tcDest := dbExec.TableColumnNameStruct{Schema: destSchema, Table: destTable, Drive: stcls.destDrive, Db: stcls.destDB}
		indexTypeDest := tcDest.Query().TableIndexChoice(queryDataDest, logThreadSeq2)
		vlog = fmt.Sprintf("(%d) Target table %s.%s index list information query completed. index list message is {%v}",
			logThreadSeq, destSchema, destTable, indexTypeDest)
		global.Wlog.Debug(vlog)

		// 使用源端schema和表名作为key，因为后续处理中会根据源端表进行数据校验
		// 同时在key中保存目标端schema和表名，以便后续处理
		if len(indexType) == 0 { //针对于表没有索引的，进行处理
			key := fmt.Sprintf("%s/*gtchecksumSchemaTable*/%s/*mapping*/%s/*mappingTable*/%s",
				sourceSchema, sourceTable, destSchema, destTable)
			tableIndexColumnMap[key] = []string{}

			// 构建显示名称，包含映射关系
			displayTableName := fmt.Sprintf("%s.%s:%s.%s", sourceSchema, sourceTable, destSchema, destTable)

			vlog = fmt.Sprintf("(%d) The source table %s has no index.", logThreadSeq, displayTableName)
			global.Wlog.Warn(vlog)
		} else {
			vlog = fmt.Sprintf("(%d) Start to perform index selection on source table %s.%s according to the algorithm",
				logThreadSeq, sourceSchema, sourceTable)
			global.Wlog.Debug(vlog)
			ab, aa := stcls.tableIndexAlgorithm(indexType)
			key := fmt.Sprintf("%s/*gtchecksumSchemaTable*/%s/*indexColumnType*/%s/*mapping*/%s/*mappingTable*/%s",
				sourceSchema, sourceTable, ab, destSchema, destTable)
			tableIndexColumnMap[key] = aa

			// 构建显示名称，包含映射关系
			displayTableName := fmt.Sprintf("%s.%s:%s.%s", sourceSchema, sourceTable, destSchema, destTable)

			vlog = fmt.Sprintf("(%d) The index selection of source table %s is completed, and the selected index information is { keyName:%s keyColumn: %s}",
				logThreadSeq, displayTableName, ab, aa)
			global.Wlog.Debug(vlog)
		}
	}
	vlog = fmt.Sprintf("(%d) Table index listing information and appropriate index completion", logThreadSeq)
	global.Wlog.Info(vlog)
	return tableIndexColumnMap
}

// 解析表映射规则
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

/*
校验触发器
*/
func (stcls *schemaTable) Trigger(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog       string
		tmpM       = make(map[string]int)
		schemaMap  = make(map[string]int)
		triggerMap = make(map[string]string) // 存储具体的触发器名称
		c, d       []string
		pods       = Pod{
			Datafix:     stcls.datafix,
			CheckObject: "trigger",
		}
		sourceTrigger, destTrigger map[string]string
		err                        error
	)

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Trigger. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)

	// 从dtabS中提取schema信息和触发器名称
	for _, i := range dtabS {
		// 处理映射格式 schema.trigger:schema.trigger
		if strings.Contains(i, ":") {
			parts := strings.Split(i, ":")
			if len(parts) == 2 {
				sourceParts := strings.Split(parts[0], ".")
				if len(sourceParts) >= 1 {
					schema := sourceParts[0]

					// schema的名字要区分大小写
					if stcls.caseSensitiveObjectName == "yes" {
						// 当区分大小写时，保持原始大小写
					} else {
						// 当不区分大小写时，也保持原始大小写
					}
					schemaMap[schema] = 1

					// 如果指定了具体的触发器名称
					if len(sourceParts) >= 2 && sourceParts[1] != "*" {
						// 保持trigger名称的原始大小写
						triggerName := sourceParts[1]
						triggerMap[schema+"."+triggerName] = triggerName
					}
				}
			}
		} else {
			// 处理普通格式 schema.trigger 或 schema.*
			parts := strings.Split(i, ".")
			if len(parts) >= 1 {
				schema := parts[0]

				if stcls.caseSensitiveObjectName == "yes" {
					// 当区分大小写时，保持原始大小写
				} else {
					// 当不区分大小写时，也保持原始大小写
				}
				schemaMap[schema] = 1

				// 如果指定了具体的触发器名称
				if len(parts) >= 2 && parts[1] != "*" {
					triggerName := parts[1]
					triggerMap[schema+"."+triggerName] = triggerName
				}
			}
		}
	}

	// 添加调试日志，显示提取的schema和触发器信息
	vlog = fmt.Sprintf("(%d) Extracted schema map: %v, trigger map: %v", logThreadSeq, schemaMap, triggerMap)
	global.Wlog.Debug(vlog)

	// 如果schemaMap为空，但stcls.schema不为空，则使用stcls.schema
	if len(schemaMap) == 0 && stcls.schema != "" {
		schema := stcls.schema
		if stcls.caseSensitiveObjectName == "yes" {
			// 当区分大小写时，保持原始大小写
		} else {
			// 当不区分大小写时，也保持原始大小写
		}
		schemaMap[schema] = 1
		vlog = fmt.Sprintf("(%d) No schema found in dtabS, using default schema: %s", logThreadSeq, schema)
		global.Wlog.Debug(vlog)
	}
	//校验触发器
	for schema, _ := range schemaMap {
		pods.Schema = schema
		vlog = fmt.Sprintf("(%d) Start processing srcDSN {%s} databases %s Trigger. to dispos it...", logThreadSeq, stcls.sourceDrive, schema)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{
			Schema:                  schema,
			Drive:                   stcls.sourceDrive,
			CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
		}

		// 获取源数据库的触发器
		if sourceTrigger, err = tc.Query().Trigger(stcls.sourceDB, logThreadSeq2); err != nil {
			vlog = fmt.Sprintf("(%d) Error querying source triggers: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return
		}

		// 如果有指定具体的触发器，则过滤结果
		if len(triggerMap) > 0 {
			filteredSourceTrigger := make(map[string]string)
			for k, v := range sourceTrigger {
				// 提取触发器名称时需要更加小心
				parts := strings.Split(k, ".")
				var triggerName string
				if len(parts) > 1 {
					// 移除可能存在的引号
					triggerName = strings.ReplaceAll(parts[1], "\"", "")
				} else {
					// 如果没有点号，使用整个键
					triggerName = strings.ReplaceAll(k, "\"", "")
				}

				// 保持trigger名称的原始大小写，不做转换

				triggerKey := schema + "." + triggerName

				// 添加调试日志
				vlog = fmt.Sprintf("(%d) Checking trigger: %s, key: %s", logThreadSeq, k, triggerKey)
				global.Wlog.Debug(vlog)

				// 检查是否在过滤映射中
				if _, exists := triggerMap[triggerKey]; exists {
					filteredSourceTrigger[k] = v
					vlog = fmt.Sprintf("(%d) Keeping trigger: %s", logThreadSeq, k)
					global.Wlog.Debug(vlog)
				}
			}
			sourceTrigger = filteredSourceTrigger
		} else {
			// 如果triggerMap为空（表示使用通配符），则不进行过滤，保留所有触发器
			vlog = fmt.Sprintf("(%d) No specific triggers specified, keeping all %d source triggers", logThreadSeq, len(sourceTrigger))
			global.Wlog.Debug(vlog)

			// 当使用通配符时，将所有触发器名称添加到triggerMap中，以便后续比较
			for k, _ := range sourceTrigger {
				parts := strings.Split(k, ".")
				var triggerName string
				if len(parts) > 1 {
					triggerName = strings.ReplaceAll(parts[1], "\"", "")
				} else {
					triggerName = strings.ReplaceAll(k, "\"", "")
				}

				// 保持trigger名称的原始大小写，不做转换

				triggerKey := schema + "." + triggerName
				triggerMap[triggerKey] = triggerName
				vlog = fmt.Sprintf("(%d) Added trigger to map: %s", logThreadSeq, triggerKey)
				global.Wlog.Debug(vlog)
			}
		}

		vlog = fmt.Sprintf("(%d) srcDSN {%s} databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, schema, sourceTrigger)
		global.Wlog.Debug(vlog)

		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} databases %s Trigger data. to dispos it...", logThreadSeq, stcls.destDrive, schema)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive

		// 获取目标数据库的触发器
		if destTrigger, err = tc.Query().Trigger(stcls.destDB, logThreadSeq2); err != nil {
			vlog = fmt.Sprintf("(%d) Error querying destination triggers: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return
		}

		// 如果有指定具体的触发器，则过滤结果
		if len(triggerMap) > 0 {
			filteredDestTrigger := make(map[string]string)
			for k, v := range destTrigger {
				// 提取触发器名称时需要更加小心
				parts := strings.Split(k, ".")
				var triggerName string
				if len(parts) > 1 {
					// 移除可能存在的引号
					triggerName = strings.ReplaceAll(parts[1], "\"", "")
				} else {
					// 如果没有点号，使用整个键
					triggerName = strings.ReplaceAll(k, "\"", "")
				}

				// 保持trigger名称的原始大小写，不做转换

				triggerKey := schema + "." + triggerName

				// 添加调试日志
				vlog = fmt.Sprintf("(%d) Checking dest trigger: %s, key: %s", logThreadSeq, k, triggerKey)
				global.Wlog.Debug(vlog)

				// 检查是否在过滤映射中
				if _, exists := triggerMap[triggerKey]; exists {
					filteredDestTrigger[k] = v
					vlog = fmt.Sprintf("(%d) Keeping dest trigger: %s", logThreadSeq, k)
					global.Wlog.Debug(vlog)
				}
			}
			destTrigger = filteredDestTrigger
		} else {
			// 如果triggerMap为空（表示使用通配符），则不进行过滤，保留所有触发器
			vlog = fmt.Sprintf("(%d) No specific triggers specified, keeping all %d destination triggers", logThreadSeq, len(destTrigger))
			global.Wlog.Debug(vlog)

			// 当使用通配符时，将所有目标端触发器名称也添加到triggerMap中
			for k, _ := range destTrigger {
				parts := strings.Split(k, ".")
				var triggerName string
				if len(parts) > 1 {
					triggerName = strings.ReplaceAll(parts[1], "\"", "")
				} else {
					triggerName = strings.ReplaceAll(k, "\"", "")
				}

				// 保持trigger名称的原始大小写，不做转换

				triggerKey := schema + "." + triggerName
				triggerMap[triggerKey] = triggerName
				vlog = fmt.Sprintf("(%d) Added dest trigger to map: %s", logThreadSeq, triggerKey)
				global.Wlog.Debug(vlog)
			}
		}

		vlog = fmt.Sprintf("(%d) dstDSN {%s} databases %s message is {%s}", logThreadSeq, stcls.destDrive, schema, destTrigger)
		global.Wlog.Debug(vlog)

		if len(sourceTrigger) == 0 && len(destTrigger) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, schema)
			global.Wlog.Debug(vlog)
			continue
		}

		tmpM = make(map[string]int)
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Trigger. to dispos it...", logThreadSeq, schema)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceTrigger {
			tmpM[k]++
		}
		for k, _ := range destTrigger {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Trigger is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		for k, _ := range tmpM {
			pods.TriggerName = strings.ReplaceAll(strings.Split(k, ".")[1], "\"", "")
			if sourceTrigger[k] != destTrigger[k] {
				pods.DIFFS = "yes"
				d = append(d, k)

				// Generate and write fix SQL for TRIGGER mismatch using SHOW CREATE and DELIMITER
				trName := strings.ReplaceAll(strings.Split(k, ".")[1], "\"", "")
				trSourceDef := sourceTrigger[k]
				query := fmt.Sprintf("SHOW CREATE TRIGGER `%s`.`%s`", schema, trName)
				if rows, err := stcls.sourceDB.Query(query); err == nil {
					defer rows.Close()
					if cols, err := rows.Columns(); err == nil && rows.Next() {
						values := make([]sql.RawBytes, len(cols))
						args := make([]interface{}, len(cols))
						for i := range values {
							args[i] = &values[i]
						}
						if err := rows.Scan(args...); err == nil {
							for i, col := range cols {
								if strings.EqualFold(col, "SQL Original Statement") || strings.EqualFold(col, "SQL Statement") {
									if v := string(values[i]); len(strings.TrimSpace(v)) > 0 {
										trSourceDef = v
										break
									}
								}
							}
						}
					}
				}
				tsqls := mysql.GenerateTriggerFixSQL(schema, trName, trSourceDef)
				// wrap with DELIMITER to ensure body semicolons don't conflict
				var out []string
				out = append(out, "DELIMITER $$")
				for _, s := range tsqls {
					ts := strings.TrimSpace(s)
					if strings.HasPrefix(strings.ToUpper(ts), "DROP ") && !strings.HasSuffix(ts, ";") {
						ts = ts + ";"
					}
					out = append(out, ts+"\n$$")
				}
				out = append(out, "DELIMITER ;")
				_ = mysql.WriteFixIfNeededFile(stcls.datafix, stcls.sfile, out, logThreadSeq)
			} else {
				pods.DIFFS = "no"
				c = append(c, k)
			}
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Trigger. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			vlog = fmt.Sprintf("(%d) The source target segment databases %s Trigger data verification is completed", logThreadSeq, schema)
			global.Wlog.Debug(vlog)
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Trigger data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

/*
校验存储过程
*/
/*
最小入侵新增：统一附加与刷新方法
*/
func (stcls *schemaTable) setAggregate(on bool) {
	stcls.aggregate = on
}

func (stcls *schemaTable) appendPod(p Pod) {
	if stcls.aggregate {
		stcls.podsBuffer = append(stcls.podsBuffer, p)
	} else {
		measuredDataPods = append(measuredDataPods, p)
	}
}

func (stcls *schemaTable) flushPods() {
	if len(stcls.podsBuffer) > 0 {
		measuredDataPods = append(measuredDataPods, stcls.podsBuffer...)
		stcls.podsBuffer = nil
	}
}

/*
最小入侵新增：以返回值形式获取 Proc 结果
- 通过临时开启 aggregate 模式，复用现有 Proc 逻辑来采集 pods
- 调用结束后恢复原 aggregate 与 podsBuffer 状态
*/
func (stcls *schemaTable) ProcRet(dtabS []string, logThreadSeq, logThreadSeq2 int64) ([]Pod, error) {
	// 备份现场
	prevAggregate := stcls.aggregate
	prevBuffer := stcls.podsBuffer

	// 使用独立缓冲并开启聚合
	stcls.aggregate = true
	stcls.podsBuffer = nil

	// 复用原逻辑
	stcls.Proc(dtabS, logThreadSeq, logThreadSeq2)

	// 拷贝结果
	var res []Pod
	if len(stcls.podsBuffer) > 0 {
		res = make([]Pod, len(stcls.podsBuffer))
		copy(res, stcls.podsBuffer)
	}

	// 恢复现场
	stcls.podsBuffer = prevBuffer
	stcls.aggregate = prevAggregate

	return res, nil
}

/*
最小入侵新增：以返回值形式获取 Func 结果
- 通过临时开启 aggregate 模式，复用现有 Func 逻辑来采集 pods
- 调用结束后恢复原 aggregate 与 podsBuffer 状态
*/
func (stcls *schemaTable) FuncRet(dtabS []string, logThreadSeq, logThreadSeq2 int64) ([]Pod, error) {
	// 备份现场
	prevAggregate := stcls.aggregate
	prevBuffer := stcls.podsBuffer

	// 使用独立缓冲并开启聚合
	stcls.aggregate = true
	stcls.podsBuffer = nil

	// 复用原逻辑
	stcls.Func(dtabS, logThreadSeq, logThreadSeq2)

	// 拷贝结果
	var res []Pod
	if len(stcls.podsBuffer) > 0 {
		res = make([]Pod, len(stcls.podsBuffer))
		copy(res, stcls.podsBuffer)
	}

	// 恢复现场
	stcls.podsBuffer = prevBuffer
	stcls.aggregate = prevAggregate

	return res, nil
}

/*
最小入侵新增：统一入口，先后调用 Proc 与 Func，最后合并输出
- 结果追加通过 appendPod 实现，兼容外部是否启用 aggregate
*/
func (stcls *schemaTable) ProcAndFunc(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	procPods, _ := stcls.ProcRet(dtabS, logThreadSeq, logThreadSeq2)
	funcPods, _ := stcls.FuncRet(dtabS, logThreadSeq, logThreadSeq2)

	// 合并并输出
	for _, p := range procPods {
		stcls.appendPod(p)
	}
	for _, p := range funcPods {
		stcls.appendPod(p)
	}
}

/*
Routine: unified comparison for PROCEDURE and FUNCTION.
- routineType: "", "PROCEDURE", or "FUNCTION"
- Prefer tc.Query().Routine(); if it fails, fallback to old Proc/Func paths.
- Use appendPod to emit pods to buffer or measuredDataPods per aggregate flag.
*/
func showCreateRoutine(db *sql.DB, schema, name, routineType string) (string, error) {
	var query string
	if strings.EqualFold(routineType, "PROCEDURE") {
		query = fmt.Sprintf("SHOW CREATE PROCEDURE `%s`.`%s`", schema, name)
	} else {
		query = fmt.Sprintf("SHOW CREATE FUNCTION `%s`.`%s`", schema, name)
	}

	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if !rows.Next() {
		return "", fmt.Errorf("no SHOW CREATE result for %s.%s %s", schema, name, routineType)
	}

	// 使用 RawBytes 动态接收所有列
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	if err := rows.Scan(scanArgs...); err != nil {
		return "", err
	}

	// 找到正确的 Create 列名
	targetCol := ""
	if strings.EqualFold(routineType, "PROCEDURE") {
		targetCol = "Create Procedure"
	} else {
		targetCol = "Create Function"
	}

	var createSQL string
	for i, col := range cols {
		if strings.EqualFold(col, targetCol) {
			createSQL = string(values[i])
			break
		}
	}

	if strings.TrimSpace(createSQL) == "" {
		return "", fmt.Errorf("SHOW CREATE did not return %q column; got: %v", targetCol, cols)
	}
	return createSQL, nil
}

func (stcls *schemaTable) Routine(dtabS []string, logThreadSeq, logThreadSeq2 int64, routineType string) {
	// 合并 Proc/Func 主体逻辑，统一解析与比对，统一输出字段 ProcName
	// 解析 dtabS，构建 schemaMap 与过滤映射
	schemaMap := make(map[string]int)
	procMap := make(map[string]string)
	funcMap := make(map[string]string)

	if stcls.caseSensitiveObjectName == "no" {
		// 统一转小写的辅助闭包
		lower := func(s string) string { return strings.ToLower(s) }
		_ = lower
	}

	for _, i := range dtabS {
		if strings.Contains(i, ":") {
			parts := strings.Split(i, ":")
			if len(parts) == 2 {
				sourceParts := strings.Split(parts[0], ".")
				if len(sourceParts) >= 1 {
					schema := sourceParts[0]
					if stcls.caseSensitiveObjectName == "no" {
						schema = strings.ToLower(schema)
					}
					schemaMap[schema] = 1
					// 提取名称
					if len(sourceParts) >= 2 && sourceParts[1] != "*" {
						name := sourceParts[1]
						if stcls.caseSensitiveObjectName == "no" {
							name = strings.ToLower(name)
						}
						// 根据 routineType 放入对应过滤映射；为空则两者都放
						key := schema + "." + name
						if routineType == "" || strings.EqualFold(routineType, "PROCEDURE") {
							procMap[key] = name
						}
						if routineType == "" || strings.EqualFold(routineType, "FUNCTION") {
							funcMap[key] = name
						}
					}
				}
			}
		} else {
			parts := strings.Split(i, ".")
			if len(parts) >= 1 {
				schema := parts[0]
				if stcls.caseSensitiveObjectName == "no" {
					schema = strings.ToLower(schema)
				}
				schemaMap[schema] = 1
				if len(parts) >= 2 && parts[1] != "*" {
					name := parts[1]
					if stcls.caseSensitiveObjectName == "no" {
						name = strings.ToLower(name)
					}
					key := schema + "." + name
					if routineType == "" || strings.EqualFold(routineType, "PROCEDURE") {
						procMap[key] = name
					}
					if routineType == "" || strings.EqualFold(routineType, "FUNCTION") {
						funcMap[key] = name
					}
				}
			}
		}
	}

	// 如果 schemaMap 为空但有默认 schema，则使用默认
	if len(schemaMap) == 0 && stcls.schema != "" {
		schema := stcls.schema
		if stcls.caseSensitiveObjectName == "no" {
			schema = strings.ToLower(schema)
		}
		schemaMap[schema] = 1
	}

	// 统一遍历 schema，分别处理 PROCEDURE 与 FUNCTION（按 routineType 过滤）
	for schema := range schemaMap {
		// PROCEDURE 处理
		if routineType == "" || strings.EqualFold(routineType, "PROCEDURE") {
			var (
				sourceProc, destProc map[string]string
				err                  error
				tmpM                 = make(map[string]int)
				c, d                 []string
				vlog                 string
				pods                 = Pod{Datafix: stcls.datafix, CheckObject: "Procedure", Schema: schema}
			)

			tc := dbExec.TableColumnNameStruct{
				Schema:                  schema,
				Drive:                   stcls.sourceDrive,
				CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
			}
			if sourceProc, err = tc.Query().Proc(stcls.sourceDB, logThreadSeq2); err != nil {
				vlog = fmt.Sprintf("(%d) Error querying source procedures: %v", logThreadSeq, err)
				global.Wlog.Error(vlog)
				// 不中断其他 schema 或 object 的检查
			}
			tc.Drive = stcls.destDrive
			if destProc, err = tc.Query().Proc(stcls.destDB, logThreadSeq2); err != nil {
				vlog = fmt.Sprintf("(%d) Error querying destination procedures: %v", logThreadSeq, err)
				global.Wlog.Error(vlog)
			}

			// 过滤或通配填充 procMap
			if len(procMap) > 0 {
				filteredSource := make(map[string]string)
				for k, v := range sourceProc {
					if k == "DEFINER" {
						filteredSource[k] = v
						continue
					}
					name := k
					if stcls.caseSensitiveObjectName == "no" {
						name = strings.ToLower(name)
					}
					key := schema + "." + name
					if _, ok := procMap[key]; ok {
						filteredSource[k] = v
						if bodyKey := k + "_BODY"; true {
							if _, ok := sourceProc[bodyKey]; ok {
								filteredSource[bodyKey] = sourceProc[bodyKey]
							}
						}
					}
				}
				sourceProc = filteredSource

				filteredDest := make(map[string]string)
				for k, v := range destProc {
					if k == "DEFINER" {
						filteredDest[k] = v
						continue
					}
					name := k
					if stcls.caseSensitiveObjectName == "no" {
						name = strings.ToLower(name)
					}
					key := schema + "." + name
					if _, ok := procMap[key]; ok {
						filteredDest[k] = v
						if bodyKey := k + "_BODY"; true {
							if _, ok := destProc[bodyKey]; ok {
								filteredDest[bodyKey] = destProc[bodyKey]
							}
						}
					}
				}
				destProc = filteredDest
			} else {
				for k := range sourceProc {
					if k == "DEFINER" || strings.HasSuffix(k, "_BODY") {
						continue
					}
					name := k
					if stcls.caseSensitiveObjectName == "no" {
						name = strings.ToLower(name)
					}
					procMap[schema+"."+name] = name
				}
				for k := range destProc {
					if k == "DEFINER" || strings.HasSuffix(k, "_BODY") {
						continue
					}
					name := k
					if stcls.caseSensitiveObjectName == "no" {
						name = strings.ToLower(name)
					}
					procMap[schema+"."+name] = name
				}
			}

			// 并集与比对
			if len(sourceProc) > 0 || len(destProc) > 0 {
				tmpM = make(map[string]int)
				for k := range sourceProc {
					if k == "DEFINER" || strings.HasSuffix(k, "_BODY") {
						continue
					}
					tmpM[k]++
				}
				for k := range destProc {
					if k == "DEFINER" || strings.HasSuffix(k, "_BODY") {
						continue
					}
					tmpM[k]++
				}

				for k, v := range tmpM {
					if v == 2 {
						// 同时存在：比较过程体
						srcBody := strings.Join(strings.Fields(sourceProc[k+"_BODY"]), "")
						dstBody := strings.Join(strings.Fields(destProc[k+"_BODY"]), "")
						// 当 BODY 都为空时，回退到主定义字段进行比较，避免误判为一致
						if srcBody == "" && dstBody == "" {
							srcDef := strings.Join(strings.Fields(sourceProc[k]), "")
							dstDef := strings.Join(strings.Fields(destProc[k]), "")
							if srcDef == "" && dstDef == "" {
								// 两侧都缺失可比较内容，视为差异，避免漏报
								pods.ProcName = k
								pods.DIFFS = "yes"
								d = append(d, k)
							} else if srcDef != dstDef {
								pods.ProcName = k
								pods.DIFFS = "yes"
								d = append(d, k)
							} else {
								pods.ProcName = k
								pods.DIFFS = "no"
								c = append(c, k)
							}
						} else if srcBody != dstBody {
							pods.ProcName = k
							pods.DIFFS = "yes"
							d = append(d, k)
						} else {
							pods.ProcName = k
							pods.DIFFS = "no"
							c = append(c, k)
						}
					} else {
						// 仅一侧存在
						pods.ProcName = k
						pods.DIFFS = "yes"
						d = append(d, k)
					}
					stcls.appendPod(pods)
					// Generate and write fix SQL for PROCEDURE differences (use SHOW CREATE for full definition)
					if pods.DIFFS == "yes" && pods.CheckObject == "Procedure" {
						sourceDef, err := showCreateRoutine(stcls.sourceDB, schema, k, "PROCEDURE")
						if err != nil || len(strings.TrimSpace(sourceDef)) == 0 {
							// 回退：使用之前采集到的定义
							if def, ok := sourceProc[k]; ok {
								sourceDef = def
							}
						}
						sqls := mysql.GenerateRoutineFixSQL(schema, k, "PROCEDURE", sourceDef)
						// wrap with DELIMITER for routine definitions
						var out []string
						out = append(out, "DELIMITER $$")
						for _, s := range sqls {
							ts := strings.TrimSpace(s)
							// ensure DROP has trailing ';' before delimiter
							if strings.HasPrefix(strings.ToUpper(ts), "DROP ") && !strings.HasSuffix(ts, ";") {
								ts = ts + ";"
							}
							out = append(out, ts+"\n$$")
						}
						out = append(out, "DELIMITER ;")
						_ = mysql.WriteFixIfNeededFile(stcls.datafix, stcls.sfile, out, logThreadSeq)
					}
				}
			}
			// 汇总日志
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Stored Procedure. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
		}

		// FUNCTION 处理
		if routineType == "" || strings.EqualFold(routineType, "FUNCTION") {
			var (
				sourceFunc, destFunc map[string]string
				err                  error
				tmpM                 = make(map[string]int)
				c, d                 []string
				vlog                 string
				pods                 = Pod{Datafix: stcls.datafix, CheckObject: "Function", Schema: schema}
			)

			tc := dbExec.TableColumnNameStruct{
				Schema:                  schema,
				Drive:                   stcls.sourceDrive,
				CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
			}
			if sourceFunc, err = tc.Query().Func(stcls.sourceDB, logThreadSeq2); err != nil {
				vlog = fmt.Sprintf("(%d) Error querying source functions: %v", logThreadSeq, err)
				global.Wlog.Error(vlog)
			}
			tc.Drive = stcls.destDrive
			if destFunc, err = tc.Query().Func(stcls.destDB, logThreadSeq2); err != nil {
				vlog = fmt.Sprintf("(%d) Error querying destination functions: %v", logThreadSeq, err)
				global.Wlog.Error(vlog)
			}

			// 过滤或通配填充 funcMap
			if len(funcMap) > 0 {
				filteredSource := make(map[string]string)
				for k, v := range sourceFunc {
					name := k
					if stcls.caseSensitiveObjectName == "no" {
						name = strings.ToLower(name)
					}
					key := schema + "." + name
					if _, ok := funcMap[key]; ok {
						filteredSource[k] = v
					}
				}
				sourceFunc = filteredSource

				filteredDest := make(map[string]string)
				for k, v := range destFunc {
					name := k
					if stcls.caseSensitiveObjectName == "no" {
						name = strings.ToLower(name)
					}
					key := schema + "." + name
					if _, ok := funcMap[key]; ok {
						filteredDest[k] = v
					}
				}
				destFunc = filteredDest
			} else {
				for k := range sourceFunc {
					name := k
					if stcls.caseSensitiveObjectName == "no" {
						name = strings.ToLower(name)
					}
					funcMap[schema+"."+name] = name
				}
				for k := range destFunc {
					name := k
					if stcls.caseSensitiveObjectName == "no" {
						name = strings.ToLower(name)
					}
					funcMap[schema+"."+name] = name
				}
			}

			// 并集与比对
			if len(sourceFunc) > 0 || len(destFunc) > 0 {
				tmpM = make(map[string]int)
				for k := range sourceFunc {
					tmpM[k]++
				}
				for k := range destFunc {
					tmpM[k]++
				}
				for k, v := range tmpM {
					if v == 2 {
						sv := sourceFunc[k]
						dv := destFunc[k]
						if sv != dv {
							pods.ProcName = k
							pods.DIFFS = "yes"
							d = append(d, k)
						} else {
							pods.ProcName = k
							pods.DIFFS = "no"
							c = append(c, k)
						}
					} else {
						pods.ProcName = k
						pods.DIFFS = "yes"
						d = append(d, k)
					}
					stcls.appendPod(pods)
					// Generate and write fix SQL for FUNCTION differences (use SHOW CREATE for full definition)
					if pods.DIFFS == "yes" && pods.CheckObject == "Function" {
						funcSource, err := showCreateRoutine(stcls.sourceDB, schema, k, "FUNCTION")
						if err != nil || len(strings.TrimSpace(funcSource)) == 0 {
							// 回退：使用之前采集到的定义
							if def, ok := sourceFunc[k]; ok {
								funcSource = def
							}
						}
						funcSqls := mysql.GenerateRoutineFixSQL(schema, k, "FUNCTION", funcSource)
						// wrap with DELIMITER for routine definitions
						var fout []string
						fout = append(fout, "DELIMITER $$")
						for _, s := range funcSqls {
							ts := strings.TrimSpace(s)
							if strings.HasPrefix(strings.ToUpper(ts), "DROP ") && !strings.HasSuffix(ts, ";") {
								ts = ts + ";"
							}
							fout = append(fout, ts+"\n$$")
						}
						fout = append(fout, "DELIMITER ;")
						_ = mysql.WriteFixIfNeededFile(stcls.datafix, stcls.sfile, fout, logThreadSeq)
					}
				}
			}
			// 汇总日志
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Stored Function. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			stcls.flushPods()
		}
	}
}

/*
Wrapper to Routine for PROCEDURE
*/
func (stcls *schemaTable) Proc(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	stcls.Routine(dtabS, logThreadSeq, logThreadSeq2, "PROCEDURE")
	return
}

/*
校验函数
*/
/*
Wrapper to Routine for FUNCTION
*/
func (stcls *schemaTable) Func(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	stcls.Routine(dtabS, logThreadSeq, logThreadSeq2, "FUNCTION")
	return
}

func (stcls *schemaTable) Foreign(dtabS []string, logThreadSeq, logThreadSeq2 int64, isCalledFromStruct ...bool) {
	var (
		vlog                       string
		sourceForeign, destForeign map[string]string
		tmpM                       = make(map[string]int)
		err                        error
		pods                       = Pod{
			Datafix:     "no",
			CheckObject: "foreign",
		}
	)

	// 如果是从 Struct 函数调用的，则将 CheckObject 设置为 "struct"
	if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
		pods.CheckObject = "struct"
	}

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Foreign. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	//校验外键
	var c, d []string
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		vlog = fmt.Sprintf("(%d) Start processing srcDSN {%s} table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		pods.Schema = stcls.schema
		pods.Table = stcls.table
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		if sourceForeign, err = tc.Query().Foreign(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) srcDSN {%s} table %s.%s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table, sourceForeign)
		global.Wlog.Debug(vlog)

		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive
		if destForeign, err = tc.Query().Foreign(stcls.destDB, logThreadSeq2); err != nil {
			return
		}

		vlog = fmt.Sprintf("(%d) dstDSN {%s} table %s.%s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table, destForeign)
		global.Wlog.Debug(vlog)
		if len(sourceForeign) == 0 && len(destForeign) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
			continue
		}
		tmpM = make(map[string]int)
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target table %s.%s Foreign Name. to dispos it...", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceForeign {
			tmpM[k]++
		}
		for k, _ := range destForeign {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Foreign table is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		// 初始化为"no"，如果发现任何不一致，则设置为"yes"
		pods.DIFFS = "no"

		for k, _ := range tmpM {
			if sourceForeign[k] != destForeign[k] {
				pods.DIFFS = "yes" // 如果有任何不一致，设置为"yes"
				d = append(d, k)
			} else {
				c = append(c, k)
				// 不要在这里重置DIFFS
			}
		}
		vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s Foreign. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, stcls.schema, stcls.table, c, len(c), d, len(d))
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) The source target segment table %s.%s Foreign data verification is completed", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		// 如果是从 Struct 函数调用的，则将结果存储在全局变量中
		if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
			// 使用完整的schema.table作为键
			tableKey := fmt.Sprintf("%s.%s", pods.Schema, pods.Table)

			// 将结果存储在全局变量中，以便 Struct 函数可以使用
			if foreignKeyDiffsMap == nil {
				foreignKeyDiffsMap = make(map[string]bool)
			}
			foreignKeyDiffsMap[tableKey] = pods.DIFFS == "yes"

			vlog = fmt.Sprintf("(%d) Storing foreign key check result for table %s: %v",
				logThreadSeq, tableKey, foreignKeyDiffsMap[tableKey])
			global.Wlog.Debug(vlog)
		} else {
			// 不是从 Struct 函数调用时，添加到 measuredDataPods
			stcls.appendPod(pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Foreign data. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
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
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		vlog = fmt.Sprintf("(%d) Start processing srcDSN {%s} table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		if sourcePartitions, err = tc.Query().Partitions(stcls.sourceDB, logThreadSeq2); err != nil {
			global.Wlog.Error("(%d) Failed to get source partitions for table %s.%s: %v", logThreadSeq, stcls.schema, stcls.table, err)
			return
		}

		vlog = fmt.Sprintf("(%d) srcDSN {%s} table %s.%s partitions count: %d", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table, len(sourcePartitions))
		global.Wlog.Debug(vlog)

		tc.Drive = stcls.destDrive
		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		if destPartitions, err = tc.Query().Partitions(stcls.destDB, logThreadSeq2); err != nil {
			global.Wlog.Error("(%d) Failed to get dest partitions for table %s.%s: %v", logThreadSeq, stcls.schema, stcls.table, err)
			return
		}
		vlog = fmt.Sprintf("(%d) Dest DB %s table %s.%s partitions count: %d", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table, len(destPartitions))
		global.Wlog.Debug(vlog)

		pods.Schema = stcls.schema
		pods.Table = stcls.table
		if len(sourcePartitions) == 0 && len(destPartitions) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
			continue
		}

		// 获取表的完整分区定义键
		tableKey := fmt.Sprintf("%s.%s", stcls.schema, stcls.table)

		// 1. 检查表级别的分区定义是否一致
		pods.DIFFS = "no"

		// 先比较完整的分区定义（包含分区类型、列和所有分区）
		sourceFullDef, sourceHasDef := sourcePartitions[tableKey]
		destFullDef, destHasDef := destPartitions[tableKey]

		// 记录具体的分区名称用于详细比较
		sourcePartitionNames := make([]string, 0)
		destPartitionNames := make([]string, 0)

		// 提取源端和目标端的分区名称
		for k := range sourcePartitions {
			if strings.HasPrefix(k, tableKey+".") {
				// 提取分区名称部分 (schema.table.partition -> partition)
				parts := strings.Split(k, ".")
				if len(parts) == 3 {
					sourcePartitionNames = append(sourcePartitionNames, parts[2])
				}
			}
		}

		for k := range destPartitions {
			if strings.HasPrefix(k, tableKey+".") {
				parts := strings.Split(k, ".")
				if len(parts) == 3 {
					destPartitionNames = append(destPartitionNames, parts[2])
				}
			}
		}

		vlog = fmt.Sprintf("(%d) Table %s.%s source partitions: %v, dest partitions: %v", logThreadSeq, stcls.schema, stcls.table, sourcePartitionNames, destPartitionNames)
		global.Wlog.Debug(vlog)

		// 检查分区数量是否一致
		if len(sourcePartitionNames) != len(destPartitionNames) {
			pods.DIFFS = "yes"
			vlog = fmt.Sprintf("(%d) Table %s.%s partition count mismatch: source=%d, dest=%d", logThreadSeq, stcls.schema, stcls.table, len(sourcePartitionNames), len(destPartitionNames))
			global.Wlog.Warn(vlog)
			d = append(d, fmt.Sprintf("Partition count mismatch: source=%d, dest=%d", len(sourcePartitionNames), len(destPartitionNames)))

			// 生成修复SQL提示
			if sourceFullDef != "" {
				// 清理表名，移除可能存在的映射后缀
				cleanTable := stcls.table
				if strings.Contains(cleanTable, ":") {
					parts := strings.Split(cleanTable, ":")
					cleanTable = parts[0]
				}
				fixSQLHint := fmt.Sprintf("-- [Note] The partitions for table %s.%s is inconsistent, please fix it yourself", stcls.schema, cleanTable)
				// 将修复SQL写入文件
				if stcls.datafix == "file" && stcls.sfile != nil {
					mysql.WriteFixIfNeededFile("file", stcls.sfile, []string{fixSQLHint}, logThreadSeq)
				} else {
					fmt.Println(fixSQLHint)
				}
			}
		} else {
			// 检查每个分区是否存在且定义一致
			for _, partitionName := range sourcePartitionNames {
				partitionKey := fmt.Sprintf("%s.%s.%s", stcls.schema, stcls.table, partitionName)
				sourcePartDef := sourcePartitions[partitionKey]
				destPartDef, destHasPart := destPartitions[partitionKey]

				if !destHasPart {
					// 源端有但目标端没有的分区
					pods.DIFFS = "yes"
					vlog = fmt.Sprintf("(%d) Table %s.%s partition %s exists in source but not in destination", logThreadSeq, stcls.schema, stcls.table, partitionName)
					global.Wlog.Warn(vlog)
					d = append(d, fmt.Sprintf("Missing partition: %s", partitionName))

					// 生成修复SQL提示
					if sourceFullDef != "" {
						// 清理表名，移除可能存在的映射后缀
						cleanTable := stcls.table
						if strings.Contains(cleanTable, ":") {
							parts := strings.Split(cleanTable, ":")
							cleanTable = parts[0]
						}
						fixSQLHint := fmt.Sprintf("-- [Note] The partitions for table %s.%s is inconsistent, run the following SQL to fix please:\n-- ALTER TABLE %s.%s %s;",
							stcls.schema, cleanTable, stcls.schema, cleanTable, sourceFullDef)
						// 将修复SQL写入文件
						if stcls.datafix == "file" && stcls.sfile != nil {
							mysql.WriteFixIfNeededFile("file", stcls.sfile, []string{fixSQLHint}, logThreadSeq)
						} else {
							fmt.Println(fixSQLHint)
						}
					}
				} else if sourcePartDef != destPartDef {
					// 分区存在但定义不一致
					pods.DIFFS = "yes"
					vlog = fmt.Sprintf("(%d) Table %s.%s partition %s definition mismatch: source='%s', dest='%s'", logThreadSeq, stcls.schema, stcls.table, partitionName, sourcePartDef, destPartDef)
					global.Wlog.Warn(vlog)
					d = append(d, fmt.Sprintf("Partition %s definition mismatch", partitionName))

					// 生成修复SQL提示
					if sourceFullDef != "" {
						// 清理表名，移除可能存在的映射后缀
						cleanTable := stcls.table
						if strings.Contains(cleanTable, ":") {
							parts := strings.Split(cleanTable, ":")
							cleanTable = parts[0]
						}
						fixSQLHint := fmt.Sprintf("-- [Note] The partitions for table %s.%s is inconsistent, run the following SQL to fix please:\n-- ALTER TABLE %s.%s %s;",
							stcls.schema, cleanTable, stcls.schema, cleanTable, sourceFullDef)
						// 将修复SQL写入文件
						if stcls.datafix == "file" && stcls.sfile != nil {
							mysql.WriteFixIfNeededFile("file", stcls.sfile, []string{fixSQLHint}, logThreadSeq)
						} else {
							fmt.Println(fixSQLHint)
						}
					}
				} else {
					// 分区一致
					c = append(c, partitionName)
				}
			}

			// 检查目标端是否有额外的分区
			for _, partitionName := range destPartitionNames {
				partitionKey := fmt.Sprintf("%s.%s.%s", stcls.schema, stcls.table, partitionName)
				if _, exists := sourcePartitions[partitionKey]; !exists {
					// 目标端有但源端没有的分区
					pods.DIFFS = "yes"
					vlog = fmt.Sprintf("(%d) Table %s.%s partition %s exists in destination but not in source", logThreadSeq, stcls.schema, stcls.table, partitionName)
					global.Wlog.Warn(vlog)
					d = append(d, fmt.Sprintf("Extra partition in destination: %s", partitionName))

					// 生成修复SQL提示
					if sourceFullDef != "" {
						// 清理表名，移除可能存在的映射后缀
						cleanTable := stcls.table
						if strings.Contains(cleanTable, ":") {
							parts := strings.Split(cleanTable, ":")
							cleanTable = parts[0]
						}
						fixSQLHint := fmt.Sprintf("-- [Note] The partitions for table %s.%s is inconsistent, run the following SQL to fix please:\n-- ALTER TABLE %s.%s %s;",
							stcls.schema, cleanTable, stcls.schema, cleanTable, sourceFullDef)
						// 将修复SQL写入文件
						if stcls.datafix == "file" && stcls.sfile != nil {
							mysql.WriteFixIfNeededFile("file", stcls.sfile, []string{fixSQLHint}, logThreadSeq)
						} else {
							fmt.Println(fixSQLHint)
						}
					}
				}
			}
		}

		// 记录分区定义的比较结果
		if sourceHasDef && destHasDef {
			vlog = fmt.Sprintf("(%d) Table %s.%s full partition definitions compared: source='%s', dest='%s'", logThreadSeq, stcls.schema, stcls.table, sourceFullDef, destFullDef)
			global.Wlog.Debug(vlog)
		}

		vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s partitions. normal partitions: %v, abnormal partitions: %v", logThreadSeq, stcls.schema, stcls.table, c, d)
		global.Wlog.Debug(vlog)

		// 如果是从 Struct 函数调用的，则将结果存储在全局变量中
		if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
			// 使用完整的schema.table作为键

			// 将结果存储在全局变量中，以便 Struct 函数可以使用
			if partitionDiffsMap == nil {
				partitionDiffsMap = make(map[string]bool)
			}

			// 确保使用干净的表名格式（不含映射后缀）
			cleanTableKey := tableKey
			if strings.Contains(tableKey, ":") {
				parts := strings.Split(tableKey, ":")
				cleanTableKey = parts[0]
			}

			partitionDiffsMap[cleanTableKey] = pods.DIFFS == "yes"

			vlog = fmt.Sprintf("(%d) Storing partition check result for table %s (cleaned to %s): %v",
				logThreadSeq, tableKey, cleanTableKey, partitionDiffsMap[cleanTableKey])
			global.Wlog.Debug(vlog)
		} else {
			// 不是从 Struct 函数调用时，添加到 measuredDataPods
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table partitions data. normal table count: [%d] abnormal table count: [%d]", logThreadSeq, len(c), len(d))
	global.Wlog.Info(vlog)
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

		// 辅助函数：按序号排序列并返回纯列名
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

		indexGenerate = func(smu, dmu map[string][]string, a *CheckSumTypeStruct, indexType string, indexVisibilityMap map[string]string) []string {
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
				SourceSchema:            stcls.schema,                  // 添加源端schema
				CaseSensitiveObjectName: stcls.caseSensitiveObjectName, // 传递是否区分对象名大小写
				IndexVisibilityMap:      indexVisibilityMap,            // 传递索引可见性信息
			}

			// 首先比较索引名称
			for k := range smu {
				c = append(c, k)
			}
			for k := range dmu {
				d = append(d, k)
			}

			// 如果索引名称不同，生成修复SQL
			if a.CheckMd5(strings.Join(c, ",")) != a.CheckMd5(strings.Join(d, ",")) {
				e, f := a.Arrcmp(c, d)
				// 对于新增的索引，需要处理列顺序
				newIndexMap := make(map[string][]string)
				for _, idx := range e {
					if cols, ok := smu[idx]; ok {
						// 对列进行排序并去除序号信息
						newIndexMap[idx] = sortColumns(cols)
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
						cc = fixInstance.FixAlterIndexSqlExec(e, f, newIndexMap, stcls.sourceDrive, logThreadSeq)
			} else {
				// 即使索引名称相同，也要比较索引的具体内容
				for k, sColumns := range smu {
					if dColumns, exists := dmu[k]; exists {
						// 比较同名索引的列及其顺序
						// 如果设置了大小写不敏感，则在比较前将列名转换为大写
						sColumnsForCompare := sColumns
						dColumnsForCompare := dColumns

						// 当caseSensitiveObjectName=no时，需要特殊处理列名大小写
						if stcls.caseSensitiveObjectName == "no" {
							// 提取并转换源端列名
							sSortedColumns := sortColumns(sColumns)
							dSortedColumns := sortColumns(dColumns)

							// 如果列名只是大小写不同，则认为它们是相同的
							if len(sSortedColumns) == len(dSortedColumns) {
								allMatch := true
								for i := 0; i < len(sSortedColumns); i++ {
									if strings.ToUpper(sSortedColumns[i]) != strings.ToUpper(dSortedColumns[i]) {
										allMatch = false
										break
									}
								}
								if allMatch {
									// 列名只是大小写不同，跳过修改
									continue
								}
							}
						}

						// 比较同名索引的列及其顺序（包含序号信息的比较）
						if a.CheckMd5(strings.Join(sColumnsForCompare, ",")) != a.CheckMd5(strings.Join(dColumnsForCompare, ",")) {
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
							if columnsOnlyCaseDifferent && indexType == "pri" {
								continue
							}

							// 1. 先生成删除旧索引的SQL
							// 根据映射规则确定目标端schema
							destSchema := stcls.schema
							if mappedSchema, exists := stcls.tableMappings[stcls.schema]; exists {
								destSchema = mappedSchema
							}

							// 2. 获取排序后的纯列名
							sortedColumns := sortColumns(sColumns)

							// 检查是否是主键且该列是自增列
							isAutoIncrementPrimaryKey := false
							if indexType == "pri" && len(sortedColumns) == 1 {
								// 构建键名：schema.table.column
								key := fmt.Sprintf("%s.%s.%s", destSchema, stcls.table, sortedColumns[0])
								// 检查该列是否已经在添加列时设置了主键
								if mysql.AutoIncrementColumnsWithPrimaryKey != nil && mysql.AutoIncrementColumnsWithPrimaryKey[key] {
									isAutoIncrementPrimaryKey = true
									vlog = fmt.Sprintf("(%d) %s Column %s is already set as PRIMARY KEY in ALTER TABLE ADD COLUMN statement, skipping index repair",
										logThreadSeq, event, sortedColumns[0])
									global.Wlog.Debug(vlog)
								}
							}

							// 3. 生成创建索引的SQL
							// 根据映射规则确定目标端schema
							destSchema = stcls.schema
							if mappedSchema, exists := stcls.tableMappings[stcls.schema]; exists {
								destSchema = mappedSchema
							}

							// 为每个列名添加反引号，确保大小写敏感性
							quotedColumns := make([]string, len(sortedColumns))
							for i, col := range sortedColumns {
								quotedColumns[i] = fmt.Sprintf("`%s`", col)
							}

							// 获取索引可见性信息
							visibility := ""
							if indexType == "mul" && indexVisibilityMap != nil {
								if vis, ok := indexVisibilityMap[k]; ok && strings.ToUpper(vis) == "INVISIBLE" {
									visibility = " INVISIBLE"
								}
							}

							// 只有当不是自增列主键时才生成创建索引的SQL
							if !isAutoIncrementPrimaryKey {
								if indexType == "pri" {
									cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PRIMARY KEY(%s);",
										destSchema, stcls.table, strings.Join(quotedColumns, ", ")))
								} else if indexType == "uni" {
									cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD UNIQUE INDEX `%s`(%s);",
										destSchema, stcls.table, k, strings.Join(quotedColumns, ", ")))
								} else {
									cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD INDEX `%s`(%s)%s;",
										destSchema, stcls.table, k, strings.Join(quotedColumns, ", "), visibility))
								}
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
	for _, i := range dtabS {
		// 从表列表中提取源端schema和表名
		sourceSchema := ""
		tableName := ""
		// 在正确的作用域内声明索引相关变量
		var spri, suni, smul, dpri, duni, dmul map[string][]string
		var sourceIndexVisibilityMap map[string]string

		// 检查是否是映射格式 (db1.t1:db2.t1)
		if strings.Contains(i, ":") {
			// 处理映射格式
			parts := strings.Split(i, ":")
			if len(parts) == 2 {
				// 处理源端
				if strings.Contains(parts[0], ".") {
					sourceParts := strings.Split(parts[0], ".")
					if len(sourceParts) == 2 {
						sourceSchema = sourceParts[0]
						tableName = sourceParts[1]
					}
				}

				// 处理目标端，提取schema存入映射表
				if strings.Contains(parts[1], ".") {
					destParts := strings.Split(parts[1], ".")
					if len(destParts) >= 1 {
						stcls.tableMappings[sourceSchema] = destParts[0]
					}
				}
			}
		} else if strings.Contains(i, ".") {
			// 处理普通格式 (schema.table)
			parts := strings.Split(i, ".")
			if len(parts) == 2 {
				sourceSchema = parts[0]
				tableName = parts[1]
			}
		}

		stcls.table = tableName
		stcls.schema = sourceSchema // 设置stcls.schema为sourceSchema

		// 根据映射规则确定目标端schema
		destSchema := sourceSchema
		if mappedSchema, exists := stcls.tableMappings[sourceSchema]; exists {
			destSchema = mappedSchema
		}

		// 检查表是否在skipIndexCheckTables列表中，如果是，则跳过
		tableKey := fmt.Sprintf("%s.%s", destSchema, tableName)
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

		idxc := dbExec.IndexColumnStruct{Schema: sourceSchema, Table: stcls.table, Drivce: stcls.sourceDrive}
		vlog = fmt.Sprintf("(%d) %s Start processing srcDSN {%s} table %s.%s index column data. to dispos it...", logThreadSeq, event, stcls.sourceDrive, sourceSchema, stcls.table)
		global.Wlog.Debug(vlog)
		squeryData, err := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Querying the index column data of srcDSN {%s} database table %s failed, and the error message is {%v}", logThreadSeq, event, stcls.sourceDrive, i, err)
			global.Wlog.Error(vlog)
			return err
		}
		spri, suni, smul, sourceIndexVisibilityMap = idxc.TableIndexColumn().IndexDisposF(squeryData, logThreadSeq2)
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
		idxc.Drivce = stcls.destDrive
		vlog = fmt.Sprintf("(%d) %s Start processing dstDSN {%s} table %s.%s index column data. to dispos it...", logThreadSeq, event, stcls.destDrive, destSchema, stcls.table)
		global.Wlog.Debug(vlog)
		dqueryData, err := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.destDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Querying the index column data of dstDSN {%s} database table %s failed, and the error message is {%v}", logThreadSeq, event, stcls.destDrive, i, err)
			global.Wlog.Error(vlog)
			return err
		}
		dpri, duni, dmul, _ = idxc.TableIndexColumn().IndexDisposF(dqueryData, logThreadSeq2)
		vlog = fmt.Sprintf("(%d) %s The index column data of the dest %s database table %s.%s is {primary:%v,unique key:%v,index key:%v}",
			logThreadSeq,
			event,
			stcls.destDrive,
			destSchema,
			stcls.table,
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
		sqlS = append(sqlS, indexGenerate(spri, dpri, aa, "pri", sourceIndexVisibilityMap)...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the primary key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		//再比较唯一索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the unique key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(suni, duni, aa, "uni", sourceIndexVisibilityMap)...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the unique key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Info(vlog)
		//后比较普通索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the no-unique key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(smul, dmul, aa, "mul", sourceIndexVisibilityMap)...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the no-unique key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		// 应用并清空 sqlS
		if len(sqlS) > 0 {
			pods.DIFFS = "yes"

			// 检查是否有列修复操作需要合并
			tableKey := fmt.Sprintf("%s.%s", stcls.schema, stcls.table)
			if stcls.columnRepairMap != nil {
				if columnOperations, exists := stcls.columnRepairMap[tableKey]; exists && len(columnOperations) > 0 {
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
					}

					// 合并列修复和索引修复操作
					combinedSql := dbf.DataAbnormalFix().FixAlterColumnAndIndexSqlGenerate(columnOperations, sqlS, logThreadSeq)

					// 使用合并后的SQL
					sqlS = combinedSql

					// 从columnRepairMap中删除已处理的表
					delete(stcls.columnRepairMap, tableKey)

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
					}

					combinedSql := dbf.DataAbnormalFix().FixAlterIndexSqlGenerate(sqlS, logThreadSeq)
					sqlS = combinedSql
				}
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
				}

				combinedSql := dbf.DataAbnormalFix().FixAlterIndexSqlGenerate(sqlS, logThreadSeq)
				sqlS = combinedSql
			}

			err := mysql.WriteFixIfNeededFile(stcls.datafix, stcls.sfile, sqlS, logThreadSeq)
			if err != nil {
				return err
			}
			sqlS = []string{} // 清空 sqlS 以便下一个表使用

			// 添加调试日志，记录索引不一致的表
			vlog = fmt.Sprintf("(%d) %s Table %s.%s has index differences, setting DIFFS to yes",
				logThreadSeq, event, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
		}

		// 如果是从 Struct 函数调用的，则将结果存储在临时变量中，以便 Struct 函数可以使用
		if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
			// 使用完整的schema.table作为键
			tableKey := fmt.Sprintf("%s.%s", stcls.schema, stcls.table)

			// 将结果存储在全局变量中，以便 Struct 函数可以使用
			if indexDiffsMap == nil {
				indexDiffsMap = make(map[string]bool)
			}
			indexDiffsMap[tableKey] = pods.DIFFS == "yes"

			vlog = fmt.Sprintf("(%d) %s Storing index check result for table %s.%s: %v",
				logThreadSeq, event, stcls.schema, stcls.table, indexDiffsMap[tableKey])
			global.Wlog.Debug(vlog)
		} else {
			// 不是从 Struct 函数调用时，添加到 measuredDataPods
			measuredDataPods = append(measuredDataPods, pods)
		}
		vlog = fmt.Sprintf("(%d) %s The source target segment table %s.%s index column data verification is completed", logThreadSeq, event, stcls.schema, stcls.table)
		global.Wlog.Info(vlog)
	}
	fmt.Println("gt-checksum: Index verification completed")
	return nil
}

/*
校验表结构是否正确
当设置checkObject=struct时，同时执行表结构、索引、分区和外键的校验
*/
func (stcls *schemaTable) Struct(dtabS []string, logThreadSeq, logThreadSeq2 int64) error {
	//校验列名
	var (
		vlog  string
		event string
		// 用于记录每个表的索引、分区和外键是否一致的映射
		tableStructDiffs = make(map[string]bool)
	)
	event = fmt.Sprintf("[check_table_columns]")
	fmt.Println("gt-checksum: Checking table structure")
	vlog = fmt.Sprintf("(%d) %s checking table structure of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)
	normal, abnormal, err := stcls.TableColumnNameCheck(dtabS, logThreadSeq, logThreadSeq2)
	if err != nil {
		return err
	}
	vlog = fmt.Sprintf("(%d) %s Table structure and column checksum of srcDB and dstDB completed. The consistent result is {%s}(num [%d]), and the inconsistent result is {%s}(num [%d])", logThreadSeq, event, normal, len(normal), abnormal, len(abnormal))
	global.Wlog.Debug(vlog)

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

		// 为每个表创建新的Pod实例
		pods := Pod{
			Datafix:     stcls.datafix,
			CheckObject: "struct",
			Schema:      sourceSchema,
			Table:       tableName,
			DIFFS:       "no",
		}

		// 如果表在abnormal列表中，则标记为不一致
		for _, abnormalTable := range abnormal {
			if abnormalTable == i {
				pods.DIFFS = "yes"
				break
			}
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
	indexDiffsMap = make(map[string]bool)

	// 调用Index函数进行索引校验
	fmt.Println("gt-checksum: Checking table indexes")
	vlog = fmt.Sprintf("(%d) %s checking table indexes of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)

	// 调用原始的Index函数
	if err := stcls.Index(dtabS, logThreadSeq, logThreadSeq2, true); err != nil {
		return err
	}

	// 使用indexDiffsMap更新collector.diffs
	for tableKey, hasDiff := range indexDiffsMap {
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
	partitionDiffsMap = make(map[string]bool)
	vlog = fmt.Sprintf("(%d) %s Starting partitions check for %d tables, will query INFORMATION_SCHEMA.PARTITIONS for each table", logThreadSeq, event, len(dtabS))
	global.Wlog.Debug(vlog)

	// 调用Partitions函数进行分区检查，会查询INFORMATION_SCHEMA.PARTITIONS表
	stcls.Partitions(dtabS, logThreadSeq, logThreadSeq2, true)
	vlog = fmt.Sprintf("(%d) %s Completed partitions check, results: %v", logThreadSeq, event, partitionDiffsMap)
	global.Wlog.Debug(vlog)

	// 使用全局partitionDiffsMap更新collector.diffs
	vlog = fmt.Sprintf("(%d) Processing partition diffs map with %d entries: %v", logThreadSeq, len(partitionDiffsMap), partitionDiffsMap)
	global.Wlog.Debug(vlog)
	for tableKey, hasDiff := range partitionDiffsMap {
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
	foreignKeyDiffsMap = make(map[string]bool)

	// 修改Foreign函数，使其能够存储检查结果
	stcls.Foreign(dtabS, logThreadSeq, logThreadSeq2, true)

	// 使用全局foreignKeyDiffsMap更新collector.diffs
	for tableKey, hasDiff := range foreignKeyDiffsMap {
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

			vlog = fmt.Sprintf("(%d) Checking table %s.%s, current DIFFS=%s, in diff map: %v, exists: %v",
				logThreadSeq, pod.Schema, pod.Table, pod.DIFFS, isDifferent, exists)
			global.Wlog.Debug(vlog)

			// 只有当表存在于差异映射中且被标记为不一致时，才更新DIFFS状态
			if exists && isDifferent {
				measuredDataPods[i].DIFFS = "yes"
				vlog = fmt.Sprintf("(%d) Table %s.%s has structure differences, setting DIFFS to yes",
					logThreadSeq, pod.Schema, pod.Table)
				global.Wlog.Debug(vlog)
			}
		}
	}

	fmt.Println("gt-checksum: Table structure verification completed")
	vlog = fmt.Sprintf("(%d) %s check source and target DB table struct complete", logThreadSeq, event)
	global.Wlog.Info(vlog)
	return nil
}

/*
用于测试db链接串是否正确，是否可以连接
*/
func dbOpenTest(drive, jdbc string) *sql.DB {
	p := dbExec.DBexec()
	p.JDBC = jdbc
	p.DBDevice = drive
	db, err := p.OpenDB()
	if err != nil {
		fmt.Println("")
		os.Exit(1)
	}
	err1 := db.Ping()
	if err1 != nil {
		os.Exit(1)
	}
	return db
}

/*
库表的初始化
*/
func SchemaTableInit(m *inputArg.ConfigParameter) *schemaTable {
	sdb := dbOpenTest(m.SecondaryL.DsnsV.SrcDrive, m.SecondaryL.DsnsV.SrcJdbc)
	ddb := dbOpenTest(m.SecondaryL.DsnsV.DestDrive, m.SecondaryL.DsnsV.DestJdbc)

	// 初始化表映射关系
	tableMappings := make(map[string]string)

	// 解析tables参数中的映射关系
	tables := m.SecondaryL.SchemaV.Tables
	for _, tableItem := range strings.Split(tables, ",") {
		if strings.Contains(tableItem, ":") {
			parts := strings.Split(tableItem, ":")
			if len(parts) == 2 {
				// 处理db1.*:db2.*格式
				if strings.Contains(parts[0], ".*") && strings.Contains(parts[1], ".*") {
					sourceSchema := strings.TrimSuffix(parts[0], ".*")
					destSchema := strings.TrimSuffix(parts[1], ".*")
					tableMappings[sourceSchema] = destSchema
				} else {
					// 处理db1.table1:db2.table2格式
					sourceParts := strings.Split(parts[0], ".")
					destParts := strings.Split(parts[1], ".")
					if len(sourceParts) >= 1 && len(destParts) >= 1 {
						sourceSchema := sourceParts[0]
						destSchema := destParts[0]
						tableMappings[sourceSchema] = destSchema
					}
				}
			}
		}
	}

	// 初始化全局映射变量
	indexDiffsMap = make(map[string]bool)
	partitionDiffsMap = make(map[string]bool)
	foreignKeyDiffsMap = make(map[string]bool)

	// 添加调试日志
	vlog := fmt.Sprintf("Initialized table mappings: %v", tableMappings)
	global.Wlog.Debug(vlog)

	return &schemaTable{
		ignoreTable:             m.SecondaryL.SchemaV.IgnoreTables,
		table:                   m.SecondaryL.SchemaV.Tables,
		sourceDrive:             m.SecondaryL.DsnsV.SrcDrive,
		destDrive:               m.SecondaryL.DsnsV.DestDrive,
		sourceDB:                sdb,
		destDB:                  ddb,
		caseSensitiveObjectName: m.SecondaryL.SchemaV.CaseSensitiveObjectName,
		datafix:                 m.SecondaryL.RepairV.Datafix,
		sfile:                   m.SecondaryL.RepairV.FixFileFINE,
		djdbc:                   m.SecondaryL.DsnsV.DestJdbc,
		checkRules:              m.SecondaryL.RulesV,
		tableMappings:           tableMappings,
	}
}

/*
获取源数据库连接
*/
func (stcls *schemaTable) GetSourceDB() *sql.DB {
	return stcls.sourceDB
}

/*
获取目标数据库连接
*/
func (stcls *schemaTable) GetDestDB() *sql.DB {
	return stcls.destDB
}

// generateCreateTableSql 生成创建表的SQL语句，包括表级别的字符集和排序规则
func generateCreateTableSql(sourceDB *sql.DB, sourceSchema string, destSchema string, tableName string, logThreadSeq int64) (string, error) {
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

	// 替换schema名称
	createTableStmt = strings.Replace(createTableStmt, fmt.Sprintf("`%s`", sourceSchema), fmt.Sprintf("`%s`", destSchema), -1)

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
		logThreadSeq, event, destSchema, tableName, tableCharset, tableCollation)
	global.Wlog.Debug(vlog)

	// 确保SQL语句末尾有分号
	if !strings.HasSuffix(createTableStmt, ";") {
		createTableStmt = createTableStmt + ";"
	}

	return createTableStmt, nil
}
