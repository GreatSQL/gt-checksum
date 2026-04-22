package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"regexp"
	"strings"
)

// 缓存“首列可用索引名”，避免高频场景重复查询information_schema.statistics
var leadingIndexNameGlobalCache = make(map[string]string)

// 缓存表的列集合，避免逐列执行information_schema查询
var tableColumnSetGlobalCache = make(map[string]map[string]struct{})
var dbScopeKeyGlobalCache = make(map[*sql.DB]string)
var dbScopeKeySeq uint64
var statisticsIndexVisibilityCache = make(map[string]bool)
var statisticsExpressionCache = make(map[string]bool)

// 在首层分组场景下，使用 `GROUP BY col, count=1` 的轻量模式阈值。
// 当表行数/首列基数较低（重复度低）时，该模式通常明显快于 COUNT(*) 聚合。
const fastGroupModeDupRatioThreshold = 8.0

// buildMySQLIndexStatisticsQuery 构建索引统计查询语句。
// 以 STATISTICS 为驱动表，LEFT JOIN COLUMNS，确保函数索引（COLUMN_NAME=NULL）不被 INNER JOIN 丢弃。
// includeVisibility：是否查询 IS_VISIBLE 列（MySQL 8.0+）。
// includeExpression：是否查询 EXPRESSION 列（MySQL 8.0.13+ 函数索引）。

// supportsStatisticsExpression 探测当前 MySQL 版本是否在 INFORMATION_SCHEMA.STATISTICS 中提供
// EXPRESSION 列（MySQL 8.0.13+ 函数索引支持），结果按连接实例缓存。

/*
查询MySQL库下指定表的索引统计信息
*/

/*
根据MySQL库下指定表的索引信息，筛选主键索引、唯一索引、普通索引
*/

/*
查询表，输出索引列数据的字符串长度，判断是否有null或空
*/

/*
MySQL 查询有索引表的总行数
*/

/*
处理MySQL 5.7版本针对列数据类型为FLOAT类型时，select where column = 'float'查询不出数据问题
*/

// 检查指定的列是否存在于表中

/*
处理MySQL 5.7版本针对列数据类型为FLOAT类型时，select where column = 'float'查询不出数据问题
*/
func (my QueryTable) FloatTypeQueryDispos(db *sql.DB, where string, logThreadSeq int64) (string, error) {
	var whereExist string
	column, err := my.TableAllColumn(db, logThreadSeq)
	if err != nil {
		return "", err
	}
	var C = make(map[string]string)
	whereExist = where
	for _, i := range strings.Split(where, "and") {
		if strings.Contains(i, " = ") {
			C[strings.ToUpper(strings.TrimSpace(strings.Split(i, " = ")[0]))] = strings.TrimSpace(strings.Split(i, " = ")[1])
		}
	}
	for _, i := range column {
		if V, ok := C[strings.ToUpper(fmt.Sprintf("%v", i["columnName"]))]; ok {
			if strings.Contains(fmt.Sprintf("%v", i["dataType"]), "float") {
				D := strings.Split(fmt.Sprintf("%v", i["dataType"]), ",")
				Place := D[1][:strings.Index(D[1], ")")]
				whereExist = strings.ReplaceAll(where, fmt.Sprintf("%v = %v", i["columnName"], V), fmt.Sprintf("format(%v,%v) = format(%v,%v)", i["columnName"], Place, V, Place))
			}
		}
	}
	return fmt.Sprintf("WHERE %s ", whereExist), nil
}

/*
MySQL库下查询表的索引列数据，并进行去重排序
*/
func (my QueryTable) TmpTableColumnGroupDataDispos(db *sql.DB, where string, columnName string, logThreadSeq int64) (chan map[string]interface{}, error) {
	var (
		whereExist string
		Event      = "Q_Index_ColumnData"
		logMsg     string
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start to query the index column data of the following table %s.%s in the %s database and de-reorder the data.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)

	// 先检查表中是否存在该列
	columnExists, err := my.checkColumnExists(db, columnName, logThreadSeq)
	if err != nil {
		return nil, err
	}
	if !columnExists {
		logMsg = fmt.Sprintf("(%d) [%s] Column %s does not exist in table %s.%s, skipping query to avoid errors.", logThreadSeq, Event, columnName, my.Schema, my.Table)
		global.Wlog.Warn(logMsg)
		// 返回空的channel表示跳过该列的查询
		emptyChan := make(chan map[string]interface{})
		close(emptyChan)
		return emptyChan, nil
	}

	version, err := my.DatabaseVersion(db, logThreadSeq)
	if err != nil {
		return nil, err
	}
	whereExist = ""
	if where != "" {
		if strings.Contains(version, "5.7") {
			whereExist, err = my.FloatTypeQueryDispos(db, where, logThreadSeq)
			if err != nil {
				return nil, err
			}
		} else {
			whereExist = fmt.Sprintf("WHERE %s ", where)
		}
	}
	// 修复：必须添加ORDER BY，因为recursiveIndexColumn依赖有序数据来生成分片
	forceIndexClause := ""
	leadingIndexName := my.getLeadingIndexName(db, columnName)
	groupQueryForceIndexName := my.chooseGroupByForceIndex(db, columnName, where)
	if groupQueryForceIndexName != "" {
		logMsg = fmt.Sprintf("(%d) [%s] Force index chosen for grouped query on %s.%s(%s): %s",
			logThreadSeq, Event, my.Schema, my.Table, columnName, groupQueryForceIndexName)
		global.Wlog.Debug(logMsg)
	}
	if where != "" && groupQueryForceIndexName != "" && !strings.EqualFold(groupQueryForceIndexName, leadingIndexName) {
		logMsg = fmt.Sprintf("(%d) [%s] Use WHERE-driven force index (%s) instead of GROUP-column index (%s) for %s.%s",
			logThreadSeq, Event, groupQueryForceIndexName, leadingIndexName, my.Schema, my.Table)
		global.Wlog.Info(logMsg)
	}
	if where != "" && groupQueryForceIndexName == "" {
		logMsg = fmt.Sprintf("(%d) [%s] No suitable force index detected from WHERE columns for %s.%s(%s), fallback to optimizer",
			logThreadSeq, Event, my.Schema, my.Table, columnName)
		global.Wlog.Debug(logMsg)
	}
	if groupQueryForceIndexName != "" {
		forceIndexClause = fmt.Sprintf(" FORCE INDEX (`%s`)", groupQueryForceIndexName)
	}
	useFastGroupMode := false
	if shouldUseFastGroupMode(where, my.getTableRowsEstimate(db), my.getLeadingIndexCardinality(db, leadingIndexName, columnName), leadingIndexName != "") {
		useFastGroupMode = true
		logMsg = fmt.Sprintf("(%d) [%s] Fast group mode enabled for %s.%s column %s", logThreadSeq, Event, my.Schema, my.Table, columnName)
		global.Wlog.Info(logMsg)
	}

	// BIT 列原始字节作为分组键会产生不可打印字符，跨库比对时无法与 Oracle NUMBER
	// 的 TO_CHAR 结果对齐。对 BIT 列改用 CAST(col AS UNSIGNED) 做归一化，使分组键
	// 变成可比较的十进制字符串。
	selectExpr := fmt.Sprintf("`%s`", columnName)
	groupOrderExpr := fmt.Sprintf("`%s`", columnName)
	if isBitColumnType(my.resolveColumnDataType(db, columnName, logThreadSeq)) {
		selectExpr = fmt.Sprintf("CAST(`%s` AS UNSIGNED)", columnName)
		groupOrderExpr = selectExpr
	}

	accurateForceSQL := fmt.Sprintf("SELECT %s AS columnName, COUNT(1) AS count FROM `%s`.`%s`%s %s GROUP BY %s ORDER BY %s", selectExpr, my.Schema, my.Table, forceIndexClause, whereExist, groupOrderExpr, groupOrderExpr)
	accuratePlainSQL := fmt.Sprintf("SELECT %s AS columnName, COUNT(1) AS count FROM `%s`.`%s` %s GROUP BY %s ORDER BY %s", selectExpr, my.Schema, my.Table, whereExist, groupOrderExpr, groupOrderExpr)

	fastForceSQL := fmt.Sprintf("SELECT %s AS columnName, 1 AS count FROM `%s`.`%s`%s %s GROUP BY %s ORDER BY %s", selectExpr, my.Schema, my.Table, forceIndexClause, whereExist, groupOrderExpr, groupOrderExpr)
	fastPlainSQL := fmt.Sprintf("SELECT %s AS columnName, 1 AS count FROM `%s`.`%s` %s GROUP BY %s ORDER BY %s", selectExpr, my.Schema, my.Table, whereExist, groupOrderExpr, groupOrderExpr)

	primarySQL := accurateForceSQL
	secondarySQL := accuratePlainSQL
	finalFallbackSQL := ""
	if useFastGroupMode {
		primarySQL = fastForceSQL
		secondarySQL = fastPlainSQL
		finalFallbackSQL = accuratePlainSQL
	}
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(primarySQL); err != nil {
		// FORCE INDEX 可能在目标端不可用，失败后回退到普通分组查询
		if dispos.SqlRows, err = dispos.DBSQLforExec(secondarySQL); err != nil {
			// Fast mode 再次失败时，兜底回退到精确 COUNT(*) 分组，优先保证正确性。
			if finalFallbackSQL != "" {
				if dispos.SqlRows, err = dispos.DBSQLforExec(finalFallbackSQL); err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		}
	}
	C := dispos.DataChanDispos()
	logMsg = fmt.Sprintf("(%d) [%s] The index column data query of the following table %s.%s in the %s database is completed.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	return C, nil
}

/*
MySQL 查询表的统计信息中行数
*/
func (my *QueryTable) TableRows(db *sql.DB, logThreadSeq int64) (uint64, error) {
	var (
		Event  = "Q_I_S_tableRows"
		logMsg string
	)
	// 确保Schema不为空
	if my.Schema == "" {
		logMsg = fmt.Sprintf("(%d) [%s] Schema is empty for table %s, cannot get row count. Please specify a schema.", logThreadSeq, Event, my.Table)
		global.Wlog.Error(logMsg)
		return 0, fmt.Errorf("schema is empty for table %s", my.Table)
	}

	logMsg = fmt.Sprintf("(%d) [%s] Start querying row count for table %s.%s in the %s database", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)

	// Prefer INFORMATION_SCHEMA.TABLES row estimate and avoid heavy COUNT(*) full scans.
	if tableRows := my.getTableRowsEstimate(db); tableRows > 0 {
		logMsg = fmt.Sprintf("(%d) [%s] TABLE_ROWS estimate for %s.%s: %d", logThreadSeq, Event, my.Schema, my.Table, tableRows)
		global.Wlog.Debug(logMsg)
		return tableRows, nil
	}

	// Fallback to max leading index cardinality estimate.
	if cardRows := my.getMaxLeadingIndexCardinality(db); cardRows > 0 {
		logMsg = fmt.Sprintf("(%d) [%s] MAX(CARDINALITY) estimate for %s.%s: %d", logThreadSeq, Event, my.Schema, my.Table, cardRows)
		global.Wlog.Debug(logMsg)
		return cardRows, nil
	}

	logMsg = fmt.Sprintf("(%d) [%s] Row estimate unavailable for %s.%s, returning 0 without COUNT(*) fallback", logThreadSeq, Event, my.Schema, my.Table)
	global.Wlog.Warn(logMsg)
	return 0, nil
}

/*
处理无索引表查询select的order by列，防止原目标端查询的段不一致情况
*/
func (my *QueryTable) NoIndexOrderBySingerColumn(orderCol []map[string]string) []string {
	//处理order by column
	var selectC []string
	for _, v := range orderCol {
		selectC = append(selectC, v["columnName"])
		//if strings.HasPrefix(v["dataType"], "INT") {
		//	return v["columnName"]
		//}
		//if strings.HasPrefix(v["dataType"], "DATETIME") {
		//	return v["columnName"]
		//}
		//if strings.HasPrefix(v["dataType"], "TIMESTAMP") {
		//	return v["columnName"]
		//}
		//if strings.HasPrefix(v["dataType"], "CHAR") {
		//	return v["columnName"]
		//}
		//if strings.HasPrefix(v["dataType"], "VARCHAR") {
		//	return v["columnName"]
		//}
	}
	return selectC
}

/*
查询无索引表的数据（使用limit分页的方式），并排序
*/
func (my *QueryTable) NoIndexGeneratingQueryCriteria(db *sql.DB, beginSeq uint64, chanrowCount int, logThreadSeq int64) (string, error) {
	var (
		columnNameSeq []string
		Event         = "Q_table_Data"
		err           error
		query         string
		logMsg        string
	)

	// 如果没有列信息，使用"*"查询所有列
	if len(my.TableColumn) == 0 {
		query = fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT %d,%d", my.Schema, my.Table, beginSeq, chanrowCount)
		dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
		if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
			logMsg = fmt.Sprintf("(%d) [%s] Failed to execute query: %s, Error: %v", logThreadSeq, Event, query, err)
			global.Wlog.Error(logMsg)
			// 记录跳过的表信息到全局变量中
			global.AddSkippedTable(my.Schema, my.Table, "data", fmt.Sprintf("query failed: %v", err))
			return "", err
		}
		tableData, err := dispos.DataRowsDispos([]string{})
		if err != nil {
			return "", err
		}
		defer dispos.SqlRows.Close()
		return strings.Join(tableData, "/*go actions rowData*/"), nil
	}

	// 处理列名
	for _, i := range my.TableColumn {
		tmpcolumnName := fmt.Sprintf("`%s`", i["columnName"])
		tmpcolumnName = formatComparableColumnExpr(tmpcolumnName, i["dataType"])
		if shouldNormalizeZeroFillInteger(i["dataType"]) {
			global.Wlog.Info(fmt.Sprintf("(%d) [%s] Apply ZEROFILL normalization for %s.%s.%s",
				logThreadSeq, Event, my.Schema, my.Table, i["columnName"]))
		}
		columnNameSeq = append(columnNameSeq, tmpcolumnName)
	}

	// 确保至少有一个列名
	if len(columnNameSeq) == 0 {
		columnNameSeq = append(columnNameSeq, "*")
	}

	query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` LIMIT %d,%d", strings.Join(columnNameSeq, ","), my.Schema, my.Table, beginSeq, chanrowCount)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return "", err
	}
	tableData, err := dispos.DataRowsDispos([]string{})
	if err != nil {
		return "", err
	}
	defer dispos.SqlRows.Close()
	return strings.Join(tableData, "/*go actions rowData*/"), nil
}

/*
MySQL 通过where条件查询表的分段数据（查询数据生成带有gtchecksum标识的数据块）
*/
func (my QueryTable) GeneratingQueryCriteria(db *sql.DB, logThreadSeq int64) (string, error) {
	var (
		Event         = "Q_Table_Data"
		columnNameSeq []string
		err           error
		logMsg        string
	)
	//vlog = fmt.Sprintf("(%d) [%s] Start to query the segmented data of the following table %s.%s in the %s database through the where condition.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	//global.Wlog.Debug(vlog)

	// 检查WHERE子句中引用的列是否存在
	if my.Sqlwhere != "" {
		// 简单解析WHERE子句中的列名（支持更多格式）
		whereClause := my.Sqlwhere
		// 移除可能的前后空格
		whereClause = strings.TrimSpace(whereClause)

		// 提取可能的列名（改进的列名识别算法）
		// 1. 首先尝试提取带反引号的列名
		var columns []string

		// 定义SQL关键字列表，包含MySQL函数名
		sqlKeywords := []string{
			"select", "from", "where", "and", "or", "not", "is", "null",
			">=", "<=", "!=", "=", ">", "<", "like", "in", "between",
			"as", "group", "by", "order", "having", "limit", "offset",
			"join", "inner", "left", "right", "outer", "on", "using",
			"distinct", "all", "union", "intersect", "except", "exists",
			"true", "false", "case", "when", "then", "else", "end",
			// MySQL函数名
			"date_format", "cast", "convert", "concat", "substring", "length",
			"trim", "lower", "upper", "date", "time", "year", "month", "day",
			"hour", "minute", "second", "now", "current_date", "current_time", "current_timestamp",
			"if", "ifnull", "coalesce", "round", "floor", "ceil", "abs", "sum",
			"count", "avg", "max", "min", "stddev", "variance",
		}

		// 匹配带反引号的列名
		backtickRegex := regexp.MustCompile("`([^`]+)`")
		backtickMatches := backtickRegex.FindAllStringSubmatch(whereClause, -1)
		for _, match := range backtickMatches {
			if len(match) > 1 {
				columnName := match[1]
				// 处理可能的 schema.table 格式，只提取列名部分
				if dotIndex := strings.LastIndex(columnName, "."); dotIndex != -1 {
					columnName = columnName[dotIndex+1:]
				}
				// 过滤掉数据库名和表名
				if strings.ToLower(columnName) != strings.ToLower(my.Schema) &&
					strings.ToLower(columnName) != strings.ToLower(my.Table) &&
					!containsString(columns, columnName) {
					columns = append(columns, columnName)
				}
			}
		}

		// 匹配不带反引号的列名（通过操作符识别）
		// 注意：只从操作符左边提取列名，避免将值识别为列名
		// 定义操作符，注意顺序：长操作符在前，避免被短操作符错误分割
		operators := []string{">=", "<=", "!=", "=", ">", "<", " LIKE ", " IN ", " BETWEEN ", " NOT LIKE ", " NOT IN ", " IS NOT ", " IS "}

		// 将WHERE子句按AND和OR分割，处理每个条件
		conditions := strings.Split(whereClause, " AND ")
		for _, cond := range conditions {
			// 进一步按OR分割
			orConditions := strings.Split(cond, " OR ")
			for _, orCond := range orConditions {
				// 处理每个原子条件
				atomCond := strings.TrimSpace(orCond)
				if atomCond == "" {
					continue
				}

				// 标记是否找到操作符
				foundOperator := false

				// 遍历所有操作符，尝试匹配
				for _, op := range operators {
					if strings.Contains(atomCond, op) {
						// 找到操作符，只提取操作符左边的部分作为列名
						parts := strings.Split(atomCond, op)
						if len(parts) > 0 {
							// 处理左边部分，提取可能的列名
							leftPart := strings.TrimSpace(parts[0])
							// 移除可能的括号
							leftPart = strings.TrimPrefix(leftPart, "(")
							leftPart = strings.TrimSuffix(leftPart, ")")
							leftPart = strings.TrimSpace(leftPart)

							// 如果左边部分包含反引号，已经在前面处理过了，跳过
							if !strings.Contains(leftPart, "`") && leftPart != "" {
								// 检查是否已经添加过该列名
								if !containsString(columns, leftPart) {
									// 转换为小写用于比较
									wordLower := strings.ToLower(leftPart)

									// 1. 检查是否为SQL关键字
									isKeyword := false
									for _, keyword := range sqlKeywords {
										if wordLower == keyword {
											isKeyword = true
											break
										}
									}
									if isKeyword {
										continue
									}

									// 2. 检查是否为数据库名或表名
									if strings.ToLower(leftPart) == strings.ToLower(my.Schema) ||
										strings.ToLower(leftPart) == strings.ToLower(my.Table) {
										// 跳过数据库名和表名
										continue
									}

									// 额外检查：如果字符串与数据库名或表名完全匹配（不区分大小写），也跳过
									if strings.EqualFold(leftPart, my.Schema) || strings.EqualFold(leftPart, my.Table) {
										continue
									}

									// 3. 检查是否为纯数字值
									isNumeric := true
									for _, r := range leftPart {
										if !(r >= '0' && r <= '9') {
											isNumeric = false
											break
										}
									}
									if isNumeric {
										continue
									}

									// 符合条件，添加到列名列表
									columns = append(columns, leftPart)
								}
							}
						}
						foundOperator = true
						break
					}
				}

				// 如果没有找到操作符，可能是复杂条件，跳过列名检查
				if !foundOperator {
					continue
				}
			}
		}

		// 收集所有无效列
		hasInvalidColumn := false
		invalidColumns := make([]string, 0)

		// 检查每个列是否存在
		for _, column := range columns {
			if exists, err := my.checkColumnExists(db, column, logThreadSeq); err != nil {
				global.Wlog.Warn(fmt.Sprintf("(%d) [%s] Failed to check column %s existence: %v", logThreadSeq, Event, column, err))
			} else if !exists {
				hasInvalidColumn = true
				invalidColumns = append(invalidColumns, column)
			}
		}

		// 如果存在无效列，记录并返回错误
		if hasInvalidColumn {
			global.Wlog.Warn(fmt.Sprintf("(%d) [%s] Columns '%v' in WHERE clause do not exist in table %s.%s", logThreadSeq, Event, invalidColumns, my.Schema, my.Table))
			// 记录跳过的表信息到全局变量中
			global.AddSkippedTable(my.Schema, my.Table, "data", fmt.Sprintf("invalid columns: %v", invalidColumns))
			return "", fmt.Errorf("invalid columns in WHERE clause: %v", invalidColumns)
		}
	}

	// 获取表的所有列名
	if len(my.TableColumn) == 0 {
		cacheKey := scopedTableCacheKey(db, my.Schema, my.Table, "allColumns")

		// Check if result is already in global cache
		cacheMutex.RLock()
		if cachedColumns, ok := allColumnsGlobalCache[cacheKey]; ok {
			cacheMutex.RUnlock()
			// Use cached column names
			for _, columnName := range cachedColumns {
				// 直接使用列名，后续会添加格式化处理
				columnNameSeq = append(columnNameSeq, columnName)
			}
		} else {
			cacheMutex.RUnlock()
			// 从INFORMATION_SCHEMA.COLUMNS获取列信息
			strsql := fmt.Sprintf("SELECT COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY ORDINAL_POSITION", my.Schema, my.Table)
			dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
			if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
				logMsg = fmt.Sprintf("(%d) [%s] Failed to execute query: %s, Error: %v", logThreadSeq, Event, strsql, err)
				global.Wlog.Error(logMsg)
				// 记录跳过的表信息到全局变量中
				global.AddSkippedTable(my.Schema, my.Table, "data", fmt.Sprintf("query failed: %v", err))
				return "", err
			}
			// Cache the raw column names in global cache for future use
			var cachedColumns []string
			var tableColumnData []map[string]string
			for dispos.SqlRows.Next() {
				var columnName, dataType string
				if err := dispos.SqlRows.Scan(&columnName, &dataType); err != nil {
					global.Wlog.Error(fmt.Sprintf("(%d) [%s] Failed to scan column name: %v", logThreadSeq, Event, err))
					dispos.SqlRows.Close()
					return "", err
				}
				cachedColumns = append(cachedColumns, columnName)
				columnNameSeq = append(columnNameSeq, columnName)
				// 保存列名和数据类型信息
				tableColumnData = append(tableColumnData, map[string]string{
					"columnName": columnName,
					"dataType":   dataType,
				})
			}
			dispos.SqlRows.Close()

			// Save to global cache
			cacheMutex.Lock()
			allColumnsGlobalCache[cacheKey] = cachedColumns
			// 保存列数据类型信息到my.TableColumn，以便后续使用
			my.TableColumn = tableColumnData
			cacheMutex.Unlock()
		}
	} else {
		// 使用已有的列名
		for _, column := range my.TableColumn {
			columnNameSeq = append(columnNameSeq, column["columnName"])
		}
	}

	// columns 模式：按 CompareColumns 过滤并重排 TableColumn，保证 SELECT 列顺序与
	// columnsModeFilteredCols 所产生的 filteredSrcCols/filteredDstCols 顺序一致，
	// 避免在 FixUpdateSqlExec 中用 colPosMap 定位行数据时出现列值错位。
	if len(my.CompareColumns) > 0 && len(my.TableColumn) > 0 {
		my.TableColumn = orderColumnsForCompare(my.TableColumn, my.ColumnName, my.CompareColumns)
		columnNameSeq = make([]string, 0, len(my.TableColumn))
		for _, col := range my.TableColumn {
			columnNameSeq = append(columnNameSeq, col["columnName"])
		}
	}

	// 对列名应用格式化，特别是时间类型列
	formattedColumnSeq := make([]string, 0, len(columnNameSeq))
	for _, columnName := range columnNameSeq {
		var tmpcolumnName string
		tmpcolumnName = fmt.Sprintf("`%s`", columnName)

		// 定义缓存键，在所有条件分支中都可以使用
		cacheKey := scopedColumnCacheKey(db, my.Schema, my.Table, columnName)

		// 查找当前列的数据类型
		var dataType string
		columnDataTypeFound := false

		// 1. 首先尝试从my.TableColumn中获取数据类型
		for _, column := range my.TableColumn {
			if column["columnName"] == columnName {
				if dt, ok := column["dataType"]; ok && dt != "" {
					dataType = dt
					columnDataTypeFound = true
					break
				}
			}
		}

		// 2. 如果从my.TableColumn中没有找到数据类型，尝试从全局缓存获取
		if !columnDataTypeFound {
			cacheMutex.RLock()
			if dt, ok := columnDataTypeGlobalCache[cacheKey]; ok {
				dataType = dt
				columnDataTypeFound = true
			}
			cacheMutex.RUnlock()
		}

		// 3. 如果从全局缓存中也没有找到数据类型，查询INFORMATION_SCHEMA.COLUMNS获取
		if !columnDataTypeFound {
			strsql := fmt.Sprintf("SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s'", my.Schema, my.Table, columnName)
			dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
			if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err == nil {
				if dispos.SqlRows.Next() {
					dispos.SqlRows.Scan(&dataType)
					columnDataTypeFound = true
					// 将数据类型缓存到全局变量中，以便后续使用
					cacheMutex.Lock()
					columnDataTypeGlobalCache[cacheKey] = dataType
					cacheMutex.Unlock()
				}
				dispos.SqlRows.Close()
			}
		}

		// 应用与GeneratingQuerySql相同的格式化逻辑
		tmpcolumnName = formatComparableColumnExpr(tmpcolumnName, dataType)
		if shouldNormalizeZeroFillInteger(dataType) {
			global.Wlog.Info(fmt.Sprintf("(%d) [%s] Apply ZEROFILL normalization for %s.%s.%s",
				logThreadSeq, Event, my.Schema, my.Table, columnName))
		}
		formattedColumnSeq = append(formattedColumnSeq, tmpcolumnName)
	}
	columnNameSeq = formattedColumnSeq

	// 确保至少有一个列名
	if len(columnNameSeq) == 0 {
		columnNameSeq = append(columnNameSeq, "*")
	}

	// 构造完整的SELECT语句
	// 清理Sqlwhere，确保它只包含WHERE条件部分，不包含SELECT语句
	whereClause := my.Sqlwhere
	whereClause = strings.TrimSpace(whereClause)

	// 如果whereClause包含SELECT关键字，尝试提取真正的WHERE条件
	if strings.Contains(strings.ToLower(whereClause), "select") {
		// 寻找最后一个WHERE关键字的位置
		whereLower := strings.ToLower(whereClause)
		whereIndex := strings.LastIndex(whereLower, " where ")
		if whereIndex != -1 {
			// 提取WHERE后面的内容作为真正的条件
			whereClause = whereClause[whereIndex+7:] // +7 to skip " WHERE "
			whereClause = strings.TrimSpace(whereClause)
		}
	}

	// 确保WHERE子句不以WHERE开头（如果用户已经添加了WHERE关键字）
	whereLower := strings.ToLower(whereClause)
	if strings.HasPrefix(whereLower, "where ") {
		whereClause = whereClause[6:] // 移除WHERE前缀
		whereClause = strings.TrimSpace(whereClause)
	}

	strsql := fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s", strings.Join(columnNameSeq, ","), my.Schema, my.Table, whereClause)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return "", err
	}
	tableData, err := dispos.DataRowsDispos([]string{})
	if err != nil {
		return "", err
	}
	defer dispos.SqlRows.Close()
	logMsg = fmt.Sprintf("(%d) [%s] Complete the data in the following table %s.%s of the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	return strings.Join(tableData, "/*go actions rowData*/"), nil
}

// 辅助函数：检查字符串是否包含在切片中

// isBitColumnType 判断 MySQL 的列类型是否为 BIT 系列。

// resolveColumnDataType 按 TableColumn → 全局缓存 → INFORMATION_SCHEMA.COLUMNS
// 的顺序获取指定列的 COLUMN_TYPE；任何一级命中即返回。跨库场景下对于目标端 MySQL
// BIT 列是否要做 CAST 归一化需要它来判断。

/*
MySQL 生成查询数据的sql语句
*/
func (my *QueryTable) GeneratingQuerySql(db *sql.DB, logThreadSeq int64) (string, error) {
	var (
		columnNameSeq []string
		Event         = "E_Table_SQL"
		selectSql     string
		logMsg        string
	)
	//vlog = fmt.Sprintf("(%d) [%s] Start to generate the data query sql of table %s.%s in the %s database", logThreadSeq, Event, my.Schema, my.Table, DBType)
	//global.Wlog.Debug(vlog)

	// 如果TableColumn为空，从数据库查询获取列信息
	if len(my.TableColumn) == 0 {
		cacheKey := scopedTableCacheKey(db, my.Schema, my.Table, "tableColumn")

		// Check if complete table column information is already in global cache
		cacheMutex.RLock()
		if cachedTableColumn, ok := tableColumnGlobalCache[cacheKey]; ok {
			cacheMutex.RUnlock()
			//vlog = fmt.Sprintf("(%d) [%s] TableColumn information loaded from global cache for table %s.%s", logThreadSeq, Event, my.Schema, my.Table)
			//global.Wlog.Debug(vlog)
			// Use cached table column information
			my.TableColumn = cachedTableColumn
		} else {
			cacheMutex.RUnlock()
			logMsg = fmt.Sprintf("(%d) [%s] TableColumn is empty, querying column info from database for table %s.%s", logThreadSeq, Event, my.Schema, my.Table)
			global.Wlog.Debug(logMsg)

			// 查询表的所有列信息
			query := fmt.Sprintf("SELECT COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'", my.Schema, my.Table)
			rows, err := db.Query(query)
			if err != nil {
				logMsg = fmt.Sprintf("(%d) [%s] Failed to query column info for table %s.%s: %v", logThreadSeq, Event, my.Schema, my.Table, err)
				global.Wlog.Error(logMsg)
				return "", err
			}
			defer rows.Close()

			// 将查询结果填充到TableColumn
			var cachedTableColumn []map[string]string
			for rows.Next() {
				var columnName, dataType string
				if err := rows.Scan(&columnName, &dataType); err != nil {
					logMsg = fmt.Sprintf("(%d) [%s] Failed to scan column info for table %s.%s: %v", logThreadSeq, Event, my.Schema, my.Table, err)
					global.Wlog.Error(logMsg)
					return "", err
				}
				tableColumnEntry := map[string]string{
					"columnName": columnName,
					"dataType":   dataType,
				}
				cachedTableColumn = append(cachedTableColumn, tableColumnEntry)
				// Cache individual column mappings as well
				columnCacheKey := scopedColumnCacheKey(db, my.Schema, my.Table, columnName)
				cacheMutex.Lock()
				columnDataTypeGlobalCache[columnCacheKey] = dataType
				cacheMutex.Unlock()
			}

			// Save complete table column information to global cache
			cacheMutex.Lock()
			tableColumnGlobalCache[cacheKey] = cachedTableColumn
			cacheMutex.Unlock()

			// Assign to current instance
			my.TableColumn = cachedTableColumn
		}
	}

	// columns 模式：按 CompareColumns 过滤 TableColumn，同时保留 ColumnName（PK/索引列）以支持行级 key 匹配。
	// 列顺序：PK 列优先（按 ColumnName 顺序），其后按 CompareColumns 顺序追加非 PK 的比较列。
	// 源端与目标端均采用同样的排列策略，保证行字符串位置 i 语义一一对应，避免多列映射场景下的错位比较。
	// 空 CompareColumns 表示全列模式，不做过滤。
	if len(my.CompareColumns) > 0 {
		colByName := buildColumnLookupByName(my.TableColumn)
		pkSet := make(map[string]bool, len(my.ColumnName))
		for _, c := range my.ColumnName {
			pkSet[normalizeColumnLookupKey(c)] = true
		}
		ordered := orderColumnsForCompare(my.TableColumn, my.ColumnName, my.CompareColumns)
		for _, cmpCol := range my.CompareColumns {
			key := normalizeColumnLookupKey(cmpCol)
			if pkSet[key] {
				continue
			}
			if _, ok := colByName[key]; !ok {
				// 对 CompareColumns 中未找到的列记录警告（DDL 预检阶段应已拦截）
				global.Wlog.Warn(fmt.Sprintf("(%d) [%s] columns parameter specifies column %q but it was not found in table %s.%s; skipping",
					logThreadSeq, Event, cmpCol, my.Schema, my.Table))
			}
		}
		my.TableColumn = ordered
	}

	//处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
	for _, i := range my.TableColumn {
		tmpcolumnName := fmt.Sprintf("`%s`", i["columnName"])
		tmpcolumnName = formatComparableColumnExpr(tmpcolumnName, i["dataType"])
		if shouldNormalizeZeroFillInteger(i["dataType"]) {
			global.Wlog.Info(fmt.Sprintf("(%d) [%s] Apply ZEROFILL normalization for %s.%s.%s",
				logThreadSeq, Event, my.Schema, my.Table, i["columnName"]))
		}
		columnNameSeq = append(columnNameSeq, tmpcolumnName)
	}
	queryColumn := strings.Join(columnNameSeq, ",")
	version, err := my.DatabaseVersion(db, logThreadSeq)
	if strings.Contains(version, "5.7") {
		my.Sqlwhere, err = my.FloatTypeQueryDispos(db, my.Sqlwhere, logThreadSeq)
		if err != nil {
			return "", err
		}
	} else {
		if !strings.HasPrefix(strings.TrimSpace(my.Sqlwhere), "WHERE") {
			my.Sqlwhere = fmt.Sprintf(" WHERE %s ", my.Sqlwhere)
		}
	}
	// 确保WHERE条件中不包含schema名称
	cleanSqlWhere := strings.ReplaceAll(my.Sqlwhere, fmt.Sprintf("`%s`.`%s`", my.Schema, my.Table), fmt.Sprintf("`%s`", my.Table))
	cleanSqlWhere = strings.ReplaceAll(cleanSqlWhere, fmt.Sprintf("%s.%s", my.Schema, my.Table), fmt.Sprintf("%s", my.Table))

	selectSql = fmt.Sprintf("SELECT %s FROM `%s`.`%s` %s", queryColumn, my.Schema, my.Table, cleanSqlWhere)
	//vlog = fmt.Sprintf("(%d) [%s] Complete the data query sql of table %s.%s in the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	//global.Wlog.Debug(vlog)
	return selectSql, nil
}
