package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

/*
查询MySQL库下指定表的索引统计信息
*/
func (my *QueryTable) QueryTableIndexColumnInfo(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var (
		Event     = "Q_Index_Statistics"
		tableData []map[string]interface{}
		err       error
	)
	strsql = fmt.Sprintf("SELECT isc.COLUMN_NAME AS columnName, isc.COLUMN_TYPE AS columnType, isc.COLUMN_KEY AS columnKey,isc.EXTRA AS autoIncrement, iss.NON_UNIQUE AS nonUnique, iss.INDEX_NAME AS indexName, iss.SEQ_IN_INDEX AS IndexSeq, isc.ORDINAL_POSITION AS columnSeq, iss.IS_VISIBLE AS indexVisibility FROM INFORMATION_SCHEMA.COLUMNS isc INNER JOIN (SELECT NON_UNIQUE, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, IS_VISIBLE FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s') AS iss ON isc.COLUMN_NAME=iss.COLUMN_NAME WHERE isc.TABLE_SCHEMA='%s' AND isc.TABLE_NAME='%s';", my.Schema, my.Table, my.Schema, my.Table)
	vlog = fmt.Sprintf("(%d) [%s] Generate a sql statement to query the index statistics of table %s.%s under the %s database.sql messige is {%s}", logThreadSeq, Event, my.Schema, my.Table, DBType, strsql)
	global.Wlog.Debug(vlog)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	tableData, err = dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	vlog = fmt.Sprintf("(%d) [%s] The index statistics query of table %s.%s under the %s database is completed. index statistics is {%v}", logThreadSeq, Event, my.Schema, my.Table, DBType, tableData)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tableData, err
}

/*
根据MySQL库下指定表的索引信息，筛选主键索引、唯一索引、普通索引
*/
func (my *QueryTable) IndexDisposF(queryData []map[string]interface{}, logThreadSeq int64) (map[string][]string, map[string][]string, map[string][]string, map[string]string) {
	var (
		nultiseriateIndexColumnMap = make(map[string][]string)
		multiseriateIndexColumnMap = make(map[string][]string)
		priIndexColumnMap          = make(map[string][]string)
		// 添加一个新的map来存储索引的可见性信息
		indexVisibilityMap = make(map[string]string)
		indexName          string
		currIndexName      string
		Event              = "E_Index_Filter"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to filter the primary key index, unique index, and common index based on the index information of the specified table %s.%s under the %s library", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)

	// 用于临时存储每个索引的列顺序
	indexColumns := make(map[string]map[string]string)

	for _, v := range queryData {
		currIndexName = fmt.Sprintf("%s", v["indexName"])
		if my.CaseSensitiveObjectName == "no" {
			currIndexName = strings.ToUpper(fmt.Sprintf("%s", v["indexName"]))
		}

		columnName := fmt.Sprintf("%s", v["columnName"])
		indexSeq := fmt.Sprintf("%s", v["IndexSeq"])
		columnType := fmt.Sprintf("%s", v["columnType"])
		// 获取索引可见性信息
		indexVisibility := fmt.Sprintf("%s", v["indexVisibility"])

		// 初始化map
		if _, exists := indexColumns[currIndexName]; !exists {
			indexColumns[currIndexName] = make(map[string]string)
			// 存储索引可见性信息
			indexVisibilityMap[currIndexName] = indexVisibility
		}

		// 存储列的顺序信息
		indexColumns[currIndexName][indexSeq] = columnName + "/*seq*/" + indexSeq + "/*type*/" + columnType

		// 更新当前索引名
		if currIndexName != indexName {
			indexName = currIndexName
		}
	}

	// 按照索引序号排序并添加到最终的map中
	for idxName, columns := range indexColumns {
		// 获取所有序号并排序
		var seqNums []int
		for seq := range columns {
			seqNum, _ := strconv.Atoi(seq)
			seqNums = append(seqNums, seqNum)
		}
		sort.Ints(seqNums)

		// 按序号顺序添加列
		var orderedColumns []string
		for _, seq := range seqNums {
			seqStr := strconv.Itoa(seq)
			orderedColumns = append(orderedColumns, columns[seqStr])
		}

		// 根据索引类型添加到相应的map中
		if idxName == "PRIMARY" {
			priIndexColumnMap["pri"] = orderedColumns
		} else {
			// 检查第一个匹配的索引列来确定是否为唯一索引
			isUnique := false
			for _, v := range queryData {
				if fmt.Sprintf("%s", v["indexName"]) == idxName {
					isUnique = v["nonUnique"].(string) == "0"
					break
				}
			}

			if isUnique {
				nultiseriateIndexColumnMap[idxName] = orderedColumns
			} else {
				multiseriateIndexColumnMap[idxName] = orderedColumns
			}
		}
	}

	vlog = fmt.Sprintf("(%d) [%s] The index information screening of the specified table %s.%s under the %s library is completed", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)

	// 返回四个map：主键索引、唯一索引、普通索引和索引可见性信息
	return priIndexColumnMap, nultiseriateIndexColumnMap, multiseriateIndexColumnMap, indexVisibilityMap
}

/*
查询表，输出索引列数据的字符串长度，判断是否有null或空
*/
func (my *QueryTable) TmpTableIndexColumnSelectDispos(logThreadSeq int64) map[string]string {
	//根据索引列的多少，生成select 列条件，并生成列长度，为判断列是否为null或为空做判断
	var (
		columnSelect = make(map[string]string)
		columnName   = my.ColumnName
		Event        = "D_Index_Length"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the length of the query index column in table %s.%s in the specified %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	//根据索引列的多少，生成select 列条件，并生成列长度，为判断列是否为null或为空做判断
	if len(columnName) == 1 {
		columnSelect["selectColumnName"] = strings.Join(columnName, "")
		columnSelect["selectColumnLength"] = fmt.Sprintf("LENGTH(trim(%s)) AS %s_length", strings.Join(columnName, ""), strings.Join(columnName, ""))
		columnSelect["selectColumnLengthSlice"] = fmt.Sprintf("%s_length", strings.Join(columnName, ""))
		columnSelect["selectColumnNull"] = fmt.Sprintf("%s IS NULL ", strings.Join(columnName, ""))
		columnSelect["selectColumnEmpty"] = fmt.Sprintf("%s = '' ", strings.Join(columnName, ""))
	} else if len(columnName) > 1 {
		columnSelect["selectColumnName"] = strings.Join(columnName, "/*column*/")
		var aa, bb, cc, dd, ee []string
		for i := range columnName {
			aa = append(aa, fmt.Sprintf("LENGTH(trim(%s)) AS %s_length", columnName[i], columnName[i]))
			bb = append(bb, fmt.Sprintf("%s_length", columnName[i]))
			cc = append(cc, fmt.Sprintf("%s IS NULL ", columnName[i]))
			dd = append(dd, fmt.Sprintf("%s = '' ", columnName[i]))
			ee = append(ee, fmt.Sprintf("%s != '' ", columnName[i]))
		}
		columnSelect["selectColumnLength"] = strings.Join(aa, "/*column*/")
		columnSelect["selectColumnLengthSlice"] = strings.Join(bb, "/*column*/")
		columnSelect["selectColumnNull"] = strings.Join(cc, "/*column*/")
		columnSelect["selectColumnEmpty"] = strings.Join(dd, "/*column*/")
	}
	vlog = fmt.Sprintf("(%d) [%s] The length of the query index column of table %s.%s in the %s database is completed.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	return columnSelect
}

/*
MySQL 查询有索引表的总行数
*/
func (my *QueryTable) TmpTableIndexColumnRowsCount(db *sql.DB, logThreadSeq int64) (uint64, error) {
	var (
		tmpTableCount uint64
		Event         = "Q_Index_Table_Count"
		E             string
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the total number of rows in the following table %s.%s of the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("SELECT index_name AS INDEX_NAME, column_name AS columnName, cardinality as CARDINALITY FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND SEQ_IN_INDEX=1", my.Schema, my.Table)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return 0, err
	}
	if B, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{}); err != nil {
		return 0, err
	} else {
		if len(B) != 0 {
			var C []int
			for _, i := range B {
				d, _ := strconv.Atoi(fmt.Sprintf("%s", i["CARDINALITY"]))
				C = append(C, d)
			}
			sort.Ints(C)
			for _, i := range B {
				d, _ := strconv.Atoi(fmt.Sprintf("%s", i["CARDINALITY"]))
				if d == C[0] {
					E = fmt.Sprintf("%s", i["columnName"])
					break
				}
			}
		}
	}
	if E != "" {
		strsql = fmt.Sprintf("SELECT SUM(a.count) AS sum FROM (SELECT COUNT(1) AS count FROM `%s`.`%s` GROUP BY %s) a", my.Schema, my.Table, E)
	} else {
		strsql = fmt.Sprintf("SELECT COUNT(1) AS sum FROM `%s`.`%s`", my.Schema, my.Table)
	}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return 0, err
	}
	if tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{}); err != nil {
		return 0, err
	} else {
		for _, i := range tableData {
			d, _ := strconv.ParseUint(fmt.Sprintf("%s", i["sum"]), 10, 64)
			tmpTableCount += d
		}
	}
	vlog = fmt.Sprintf("(%d) [%s] The query of the total number of rows in the following table %s.%s of the %s database is completed.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tmpTableCount, nil
}

/*
处理MySQL 5.7版本针对列数据类型为FLOAT类型时，select where column = 'float'查询不出数据问题
*/

// 检查指定的列是否存在于表中
func (my *QueryTable) checkColumnExists(db *sql.DB, columnName string, logThreadSeq int64) (bool, error) {
	var (
		Event = "Check_Column_Exists"
		count int
	)

	// Generate cache key in format: schema.table.column
	cacheKey := fmt.Sprintf("%s.%s.%s", my.Schema, my.Table, columnName)

	// Check if result is already in global cache
	cacheMutex.RLock()
	if exists, ok := columnExistsGlobalCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		//vlog := fmt.Sprintf("(%d) [%s] Column %s existence check result from global cache: %v", logThreadSeq, Event, columnName, exists)
		//global.Wlog.Debug(vlog)
		return exists, nil
	}
	cacheMutex.RUnlock()

	// 直接使用一条SQL查询列是否存在，避免使用用户变量
	strsql := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'", my.Schema, my.Table, columnName)

	vlog := fmt.Sprintf("(%d) [%s] Checking if column %s exists in table %s.%s", logThreadSeq, Event, columnName, my.Schema, my.Table)
	global.Wlog.Debug(vlog)

	// 直接使用db.QueryRow避免使用DBSQLforExec的重试机制，因为这个查询很简单
	err := db.QueryRow(strsql).Scan(&count)
	if err != nil {
		// 如果查询失败，使用DESCRIBE语句作为备选方案
		vlog = fmt.Sprintf("(%d) [%s] Failed to query information_schema, using DESCRIBE as fallback. Error: %s", logThreadSeq, Event, err)
		global.Wlog.Debug(vlog)

		// 使用DESCRIBE语句检查列是否存在
		describeSQL := fmt.Sprintf("DESCRIBE `%s`.`%s` %s", my.Schema, my.Table, columnName)
		rows, err := db.Query(describeSQL)
		if err != nil {
			vlog = fmt.Sprintf("(%d) [%s] DESCRIBE query failed, column %s likely does not exist. Error: %s", logThreadSeq, Event, columnName, err)
			global.Wlog.Debug(vlog)
			// Cache the result in global cache
			cacheMutex.Lock()
			columnExistsGlobalCache[cacheKey] = false
			cacheMutex.Unlock()
			return false, nil // 列不存在
		}
		defer rows.Close()
		exists := rows.Next()
		vlog = fmt.Sprintf("(%d) [%s] Column %s existence check result: %v", logThreadSeq, Event, columnName, exists)
		global.Wlog.Debug(vlog)
		// Cache the result in global cache
		cacheMutex.Lock()
		columnExistsGlobalCache[cacheKey] = exists
		cacheMutex.Unlock()
		return exists, nil
	}

	exists := count > 0
	vlog = fmt.Sprintf("(%d) [%s] Column %s existence check result: %v", logThreadSeq, Event, columnName, exists)
	global.Wlog.Debug(vlog)
	// Cache the result in global cache
	cacheMutex.Lock()
	columnExistsGlobalCache[cacheKey] = exists
	cacheMutex.Unlock()
	return exists, nil
}

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
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the index column data of the following table %s.%s in the %s database and de-reorder the data.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)

	// 先检查表中是否存在该列
	columnExists, err := my.checkColumnExists(db, columnName, logThreadSeq)
	if err != nil {
		return nil, err
	}
	if !columnExists {
		vlog = fmt.Sprintf("(%d) [%s] Column %s does not exist in table %s.%s, skipping query to avoid errors.", logThreadSeq, Event, columnName, my.Schema, my.Table)
		global.Wlog.Warn(vlog)
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
	// 修复：对于复合主键，查询所有唯一值，而不是分组后的值
	// 这确保了所有可能的主键组合都被处理
	//strsql = fmt.Sprintf("SELECT DISTINCT %s AS columnName, COUNT(1) OVER (PARTITION BY %s) AS count FROM `%s`.`%s` %s ORDER BY %s", columnName, columnName, my.Schema, my.Table, whereExist, columnName)
	//上面的SQL效率太低，改成下面这样
	strsql = fmt.Sprintf("SELECT %s AS columnName, COUNT(1) AS count FROM `%s`.`%s` %s GROUP BY %s", columnName, my.Schema, my.Table, whereExist, columnName)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		// 如果窗口函数失败，回退到分组查询
		strsql = fmt.Sprintf("SELECT %s AS columnName, COUNT(1) AS count FROM `%s`.`%s` %s GROUP BY %s ORDER BY %s", columnName, my.Schema, my.Table, whereExist, columnName, columnName)
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			return nil, err
		}
	}
	C := dispos.DataChanDispos()
	vlog = fmt.Sprintf("(%d) [%s] The index column data query of the following table %s.%s in the %s database is completed.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	return C, nil
}

/*
MySQL 查询表的统计信息中行数
*/
func (my *QueryTable) TableRows(db *sql.DB, logThreadSeq int64) (uint64, error) {
	var (
		Event = "Q_I_S_tableRows"
	)
	// 确保Schema不为空
	if my.Schema == "" {
		vlog := fmt.Sprintf("(%d) [%s] Schema is empty for table %s, cannot get row count. Please specify a schema.", logThreadSeq, Event, my.Table)
		global.Wlog.Error(vlog)
		return 0, fmt.Errorf("schema is empty for table %s", my.Table)
	}

	vlog := fmt.Sprintf("(%d) [%s] Start querying the statistical information of table %s.%s in the %s database and get the number of rows in the table", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)

	// 首先尝试从INFORMATION_SCHEMA获取表统计信息
	strsql := fmt.Sprintf("SELECT TABLE_ROWS AS tableRows FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'", my.Schema, my.Table)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		vlog = fmt.Sprintf("(%d) [%s] Failed to get table statistics: %v, trying COUNT(*) instead", logThreadSeq, Event, err)
		global.Wlog.Warn(vlog)
		return 0, nil
	}

	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return 0, nil
	}
	defer dispos.SqlRows.Close()

	// 检查tableData是否为空，如果为空则使用COUNT(*)查询
	if len(tableData) == 0 {
		vlog = fmt.Sprintf("(%d) [%s] No table statistics found for table %s.%s in the %s database, trying COUNT(*)", logThreadSeq, Event, my.Schema, my.Table, DBType)
		global.Wlog.Warn(vlog)

		// 使用COUNT(*)查询获取行数
		strsql = fmt.Sprintf("SELECT COUNT(*) AS tableRows FROM `%s`.`%s`", my.Schema, my.Table)
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			vlog = fmt.Sprintf("(%d) [%s] Failed to get row count with COUNT(*): %v", logThreadSeq, Event, err)
			global.Wlog.Error(vlog)
			return 0, nil
		}

		tableData, err = dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err != nil {
			return 0, nil
		}

		if len(tableData) == 0 {
			vlog = fmt.Sprintf("(%d) [%s] No rows returned from COUNT(*) query for table %s.%s", logThreadSeq, Event, my.Schema, my.Table)
			global.Wlog.Warn(vlog)
			return 0, nil
		}
	}

	vlog = fmt.Sprintf("(%d) [%s] The number of rows in table %s.%s in the %s database has been obtained.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)

	return strconv.ParseUint(fmt.Sprintf("%s", tableData[0]["tableRows"]), 10, 64)
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
	)

	// 如果没有列信息，使用"*"查询所有列
	if len(my.TableColumn) == 0 {
		strsql := fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT %d,%d", my.Schema, my.Table, beginSeq, chanrowCount)
		dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			vlog = fmt.Sprintf("(%d) [%s] Failed to execute query: %s, Error: %v", logThreadSeq, Event, strsql, err)
			global.Wlog.Error(vlog)
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
		var tmpcolumnName string
		tmpcolumnName = fmt.Sprintf("`%s`", i["columnName"])
		if strings.ToUpper(i["dataType"]) == "DATETIME" {
			tmpcolumnName = fmt.Sprintf("DATE_FORMAT(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", tmpcolumnName)
		}
		if strings.Contains(strings.ToUpper(i["dataType"]), "TIMESTAMP") {
			tmpcolumnName = fmt.Sprintf("DATE_FORMAT(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", tmpcolumnName)
		}
		if strings.HasPrefix(strings.ToUpper(i["dataType"]), "DOUBLE(") {
			dianAfter := strings.ReplaceAll(strings.Split(i["dataType"], ",")[1], ")", "")
			bb, _ := strconv.Atoi(dianAfter)
			dianBefer := strings.Split(strings.Split(i["dataType"], ",")[0], "(")[1]
			bbc, _ := strconv.Atoi(dianBefer)
			tmpcolumnName = fmt.Sprintf("CAST(%s AS DECIMAL(%d,%d))", tmpcolumnName, bbc, bb)
		}
		columnNameSeq = append(columnNameSeq, tmpcolumnName)
	}

	// 确保至少有一个列名
	if len(columnNameSeq) == 0 {
		columnNameSeq = append(columnNameSeq, "*")
	}

	strsql = fmt.Sprintf("SELECT %s FROM `%s`.`%s` LIMIT %d,%d", strings.Join(columnNameSeq, ","), my.Schema, my.Table, beginSeq, chanrowCount)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
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
			vlog = fmt.Sprintf("(%d) [%s] Columns '%v' in WHERE clause do not exist in table %s.%s", logThreadSeq, Event, invalidColumns, my.Schema, my.Table)
			global.Wlog.Warn(vlog)
			// 记录跳过的表信息到全局变量中
			global.AddSkippedTable(my.Schema, my.Table, "data", fmt.Sprintf("invalid columns: %v", invalidColumns))
			return "", fmt.Errorf("invalid columns in WHERE clause: %v", invalidColumns)
		}
	}

	// 获取表的所有列名
	if len(my.TableColumn) == 0 {
		// Generate cache key in format: schema.table
		cacheKey := fmt.Sprintf("%s.%s", my.Schema, my.Table)

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
			strsql := fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY ORDINAL_POSITION", my.Schema, my.Table)
			dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
			if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
				vlog = fmt.Sprintf("(%d) [%s] Failed to execute query: %s, Error: %v", logThreadSeq, Event, strsql, err)
				global.Wlog.Error(vlog)
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

	// 对列名应用格式化，特别是时间类型列
	formattedColumnSeq := make([]string, 0, len(columnNameSeq))
	for _, columnName := range columnNameSeq {
		var tmpcolumnName string
		tmpcolumnName = fmt.Sprintf("`%s`", columnName)

		// 定义缓存键，在所有条件分支中都可以使用
		cacheKey := fmt.Sprintf("%s.%s.%s", my.Schema, my.Table, columnName)

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
			strsql := fmt.Sprintf("SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s'", my.Schema, my.Table, columnName)
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
		if strings.ToUpper(dataType) == "DATETIME" {
			tmpcolumnName = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", tmpcolumnName)
		}
		if strings.Contains(strings.ToUpper(dataType), "TIMESTAMP") {
			tmpcolumnName = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", tmpcolumnName)
		}
		if strings.HasPrefix(strings.ToUpper(dataType), "DOUBLE(") {
			dianAfter := strings.ReplaceAll(strings.Split(dataType, ",")[1], ")", "")
			bb, _ := strconv.Atoi(dianAfter)
			dianBefer := strings.Split(strings.Split(dataType, ",")[0], "(")[1]
			bbc, _ := strconv.Atoi(dianBefer)
			tmpcolumnName = fmt.Sprintf("CAST(%s AS DECIMAL(%d,%d))", tmpcolumnName, bbc, bb)
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
	vlog = fmt.Sprintf("(%d) [%s] Complete the data in the following table %s.%s of the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	return strings.Join(tableData, "/*go actions rowData*/"), nil
}

// 辅助函数：检查字符串是否包含在切片中
func containsString(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}
	return false
}

/*
MySQL 生成查询数据的sql语句
*/
func (my *QueryTable) GeneratingQuerySql(db *sql.DB, logThreadSeq int64) (string, error) {
	var (
		columnNameSeq []string
		Event         = "E_Table_SQL"
		selectSql     string
	)
	//vlog = fmt.Sprintf("(%d) [%s] Start to generate the data query sql of table %s.%s in the %s database", logThreadSeq, Event, my.Schema, my.Table, DBType)
	//global.Wlog.Debug(vlog)

	// 如果TableColumn为空，从数据库查询获取列信息
	if len(my.TableColumn) == 0 {
		// Generate cache key in format: schema.table
		cacheKey := fmt.Sprintf("%s.%s", my.Schema, my.Table)

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
			vlog = fmt.Sprintf("(%d) [%s] TableColumn is empty, querying column info from database for table %s.%s", logThreadSeq, Event, my.Schema, my.Table)
			global.Wlog.Debug(vlog)

			// 查询表的所有列信息
			query := fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'", my.Schema, my.Table)
			rows, err := db.Query(query)
			if err != nil {
				vlog = fmt.Sprintf("(%d) [%s] Failed to query column info for table %s.%s: %v", logThreadSeq, Event, my.Schema, my.Table, err)
				global.Wlog.Error(vlog)
				return "", err
			}
			defer rows.Close()

			// 将查询结果填充到TableColumn
			var cachedTableColumn []map[string]string
			for rows.Next() {
				var columnName, dataType string
				if err := rows.Scan(&columnName, &dataType); err != nil {
					vlog = fmt.Sprintf("(%d) [%s] Failed to scan column info for table %s.%s: %v", logThreadSeq, Event, my.Schema, my.Table, err)
					global.Wlog.Error(vlog)
					return "", err
				}
				tableColumnEntry := map[string]string{
					"columnName": columnName,
					"dataType":   dataType,
				}
				cachedTableColumn = append(cachedTableColumn, tableColumnEntry)
				// Cache individual column mappings as well
				columnCacheKey := fmt.Sprintf("%s.%s.%s", my.Schema, my.Table, columnName)
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

	//处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
	for _, i := range my.TableColumn {
		var tmpcolumnName string
		tmpcolumnName = fmt.Sprintf("`%s`", i["columnName"])
		if strings.ToUpper(i["dataType"]) == "DATETIME" {
			tmpcolumnName = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", tmpcolumnName)
		}
		if strings.Contains(strings.ToUpper(i["dataType"]), "TIMESTAMP") {
			tmpcolumnName = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", tmpcolumnName)
		}
		if strings.HasPrefix(strings.ToUpper(i["dataType"]), "DOUBLE(") {
			dianAfter := strings.ReplaceAll(strings.Split(i["dataType"], ",")[1], ")", "")
			bb, _ := strconv.Atoi(dianAfter)
			dianBefer := strings.Split(strings.Split(i["dataType"], ",")[0], "(")[1]
			bbc, _ := strconv.Atoi(dianBefer)
			tmpcolumnName = fmt.Sprintf("CAST(%s AS DECIMAL(%d,%d))", tmpcolumnName, bbc, bb)
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
