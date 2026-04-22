package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"sort"
	"strconv"
	"strings"
)

func buildMySQLIndexStatisticsQuery(schema, table string, includeVisibility, includeExpression bool) string {
	var visibilitySel, expressionSel string
	if includeVisibility {
		visibilitySel = "iss.IS_VISIBLE"
	} else {
		visibilitySel = "'VISIBLE'"
	}
	if includeExpression {
		expressionSel = ", iss.EXPRESSION AS expression"
	}
	return fmt.Sprintf(
		"SELECT iss.COLUMN_NAME AS columnName, isc.COLUMN_TYPE AS columnType, isc.COLUMN_KEY AS columnKey, isc.EXTRA AS autoIncrement, iss.NON_UNIQUE AS nonUnique, iss.INDEX_NAME AS indexName, iss.SEQ_IN_INDEX AS IndexSeq, isc.ORDINAL_POSITION AS columnSeq, %s AS indexVisibility, iss.SUB_PART AS subPart%s FROM INFORMATION_SCHEMA.STATISTICS iss LEFT JOIN INFORMATION_SCHEMA.COLUMNS isc ON isc.COLUMN_NAME = iss.COLUMN_NAME AND isc.TABLE_SCHEMA = iss.TABLE_SCHEMA AND isc.TABLE_NAME = iss.TABLE_NAME WHERE iss.TABLE_SCHEMA='%s' AND iss.TABLE_NAME='%s';",
		visibilitySel, expressionSel, schema, table,
	)
}

func supportsStatisticsExpression(db *sql.DB, logThreadSeq int64) (bool, error) {
	cacheKey := fmt.Sprintf("%s.statistics.expression", getDBScopeKey(db))

	cacheMutex.RLock()
	if supported, ok := statisticsExpressionCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		return supported, nil
	}
	cacheMutex.RUnlock()

	const query = "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='information_schema' AND TABLE_NAME='STATISTICS' AND COLUMN_NAME='EXPRESSION' LIMIT 1"

	var marker int
	err := db.QueryRow(query).Scan(&marker)
	switch err {
	case nil:
		cacheMutex.Lock()
		statisticsExpressionCache[cacheKey] = true
		cacheMutex.Unlock()
		return true, nil
	case sql.ErrNoRows:
		cacheMutex.Lock()
		statisticsExpressionCache[cacheKey] = false
		cacheMutex.Unlock()
		return false, nil
	default:
		logMsg := fmt.Sprintf("(%d) [Q_Index_Statistics] Failed to probe INFORMATION_SCHEMA.STATISTICS.EXPRESSION support: %v", logThreadSeq, err)
		global.Wlog.Error(logMsg)
		return false, err
	}
}

func supportsStatisticsIndexVisibility(db *sql.DB, logThreadSeq int64) (bool, error) {
	cacheKey := fmt.Sprintf("%s.statistics.is_visible", getDBScopeKey(db))

	cacheMutex.RLock()
	if supported, ok := statisticsIndexVisibilityCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		return supported, nil
	}
	cacheMutex.RUnlock()

	const query = "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='information_schema' AND TABLE_NAME='STATISTICS' AND COLUMN_NAME='IS_VISIBLE' LIMIT 1"

	var marker int
	err := db.QueryRow(query).Scan(&marker)
	switch err {
	case nil:
		cacheMutex.Lock()
		statisticsIndexVisibilityCache[cacheKey] = true
		cacheMutex.Unlock()
		return true, nil
	case sql.ErrNoRows:
		cacheMutex.Lock()
		statisticsIndexVisibilityCache[cacheKey] = false
		cacheMutex.Unlock()
		return false, nil
	default:
		logMsg := fmt.Sprintf("(%d) [Q_Index_Statistics] Failed to probe INFORMATION_SCHEMA.STATISTICS.IS_VISIBLE support: %v", logThreadSeq, err)
		global.Wlog.Error(logMsg)
		return false, err
	}
}

func (my *QueryTable) QueryTableIndexColumnInfo(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var (
		Event     = "Q_Index_Statistics"
		tableData []map[string]interface{}
		err       error
		query     string
		logMsg    string
	)
	includeVisibility, err := supportsStatisticsIndexVisibility(db, logThreadSeq)
	if err != nil {
		return nil, err
	}
	if !includeVisibility {
		logMsg = fmt.Sprintf("(%d) [%s] INFORMATION_SCHEMA.STATISTICS.IS_VISIBLE is not available on %s.%s; using compatibility query", logThreadSeq, Event, my.Schema, my.Table)
		global.Wlog.Debug(logMsg)
	}
	includeExpression, err := supportsStatisticsExpression(db, logThreadSeq)
	if err != nil {
		return nil, err
	}
	if includeExpression {
		logMsg = fmt.Sprintf("(%d) [%s] INFORMATION_SCHEMA.STATISTICS.EXPRESSION is available on %s.%s; functional index detection enabled", logThreadSeq, Event, my.Schema, my.Table)
		global.Wlog.Debug(logMsg)
	}
	query = buildMySQLIndexStatisticsQuery(my.Schema, my.Table, includeVisibility, includeExpression)
	logMsg = fmt.Sprintf("(%d) [%s] Generate a sql statement to query the index statistics of table %s.%s under the %s database.sql messige is {%s}", logThreadSeq, Event, my.Schema, my.Table, DBType, query)
	global.Wlog.Debug(logMsg)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db, Schema: my.Schema, Table: my.Table}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, err
	}
	tableData, err = dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	logMsg = fmt.Sprintf("(%d) [%s] The index statistics query of table %s.%s under the %s database is completed. index statistics is {%v}", logThreadSeq, Event, my.Schema, my.Table, DBType, tableData)
	global.Wlog.Debug(logMsg)
	defer dispos.SqlRows.Close()
	return tableData, err
}

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
		logMsg             string
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start to filter the primary key index, unique index, and common index based on the index information of the specified table %s.%s under the %s library", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)

	// 用于临时存储每个索引的列顺序
	indexColumns := make(map[string]map[string]string)

	for _, v := range queryData {
		currIndexName = fmt.Sprintf("%s", v["indexName"])
		if my.CaseSensitiveObjectName == "no" {
			currIndexName = strings.ToUpper(fmt.Sprintf("%s", v["indexName"]))
		}

		rawColumnName := v["columnName"]
		columnName := fmt.Sprintf("%s", rawColumnName)
		indexSeq := fmt.Sprintf("%s", v["IndexSeq"])
		columnType := fmt.Sprintf("%s", v["columnType"])
		// 获取索引可见性信息
		indexVisibility := fmt.Sprintf("%s", v["indexVisibility"])

		// 检测函数索引：COLUMN_NAME 为 NULL，EXPRESSION 含表达式
		funcExpr := ""
		if e, ok := v["expression"]; ok && e != nil {
			funcExpr = fmt.Sprintf("%s", e)
			// MySQL INFORMATION_SCHEMA.STATISTICS.EXPRESSION 列会对字符串字面值中的
			// 单引号做反斜杠转义（如 _utf8mb4\'$.tags\' → _utf8mb4'$.tags'）。
			// 生成 DDL 时必须还原为原始单引号，否则 ALTER TABLE 语法报错。
			funcExpr = strings.ReplaceAll(funcExpr, `\'`, `'`)
		}
		isFuncIndex := rawColumnName == nil || columnName == "<nil>"

		// 提取前缀索引长度（SUB_PART），NULL 表示全列索引，记为 0
		subPartVal := 0
		if sp, ok := v["subPart"]; ok && sp != nil {
			switch val := sp.(type) {
			case int64:
				subPartVal = int(val)
			case uint64:
				subPartVal = int(val)
			case int:
				subPartVal = val
			case []byte:
				if n, err := strconv.Atoi(string(val)); err == nil {
					subPartVal = n
				} else {
					logMsg = fmt.Sprintf("(%d) [%s] Failed to parse SUB_PART []byte value %q for %s.%s column %s: %v", logThreadSeq, Event, string(val), my.Schema, my.Table, columnName, err)
					global.Wlog.Debug(logMsg)
				}
			case string:
				if n, err := strconv.Atoi(val); err == nil {
					subPartVal = n
				} else {
					logMsg = fmt.Sprintf("(%d) [%s] Failed to parse SUB_PART string value %q for %s.%s column %s: %v", logThreadSeq, Event, val, my.Schema, my.Table, columnName, err)
					global.Wlog.Debug(logMsg)
				}
			}
		}
		subPartStr := strconv.Itoa(subPartVal)

		// 初始化map
		if _, exists := indexColumns[currIndexName]; !exists {
			indexColumns[currIndexName] = make(map[string]string)
			// 存储索引可见性信息
			indexVisibilityMap[currIndexName] = indexVisibility
		}

		// 存储列的顺序信息
		// 普通列格式：columnName/*seq*/N/*type*/columnType/*prefix*/P
		// 函数索引格式：/*expr*/EXPRESSION/*seq*/N/*type*//*prefix*/0
		var token string
		if isFuncIndex && funcExpr != "" {
			token = "/*expr*/" + funcExpr + "/*seq*/" + indexSeq + "/*type*//*prefix*/0"
			logMsg = fmt.Sprintf("(%d) [%s] Detected functional index %s on %s.%s, expr=%s", logThreadSeq, Event, currIndexName, my.Schema, my.Table, funcExpr)
			global.Wlog.Debug(logMsg)
		} else {
			token = columnName + "/*seq*/" + indexSeq + "/*type*/" + columnType + "/*prefix*/" + subPartStr
		}
		indexColumns[currIndexName][indexSeq] = token

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

	logMsg = fmt.Sprintf("(%d) [%s] The index information screening of the specified table %s.%s under the %s library is completed", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)

	// 返回四个map：主键索引、唯一索引、普通索引和索引可见性信息
	return priIndexColumnMap, nultiseriateIndexColumnMap, multiseriateIndexColumnMap, indexVisibilityMap
}

func (my *QueryTable) TmpTableIndexColumnSelectDispos(logThreadSeq int64) map[string]string {
	//根据索引列的多少，生成select 列条件，并生成列长度，为判断列是否为null或为空做判断
	var (
		columnSelect = make(map[string]string)
		columnName   = my.ColumnName
		Event        = "D_Index_Length"
		logMsg       string
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start to query the length of the query index column in table %s.%s in the specified %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
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
	logMsg = fmt.Sprintf("(%d) [%s] The length of the query index column of table %s.%s in the %s database is completed.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	return columnSelect
}

func (my *QueryTable) TmpTableIndexColumnRowsCount(db *sql.DB, logThreadSeq int64) (uint64, error) {
	var (
		tmpTableCount uint64
		Event         = "Q_Index_Table_Count"
		E             string
		err           error
		query         string
		logMsg        string
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start to query the total number of rows in the following table %s.%s of the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	query = fmt.Sprintf("SELECT index_name AS INDEX_NAME, column_name AS columnName, cardinality as CARDINALITY FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND SEQ_IN_INDEX=1", my.Schema, my.Table)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
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
		query = fmt.Sprintf("SELECT SUM(a.count) AS sum FROM (SELECT COUNT(1) AS count FROM `%s`.`%s` GROUP BY %s) a", my.Schema, my.Table, E)
	} else {
		query = fmt.Sprintf("SELECT COUNT(1) AS sum FROM `%s`.`%s`", my.Schema, my.Table)
	}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
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
	logMsg = fmt.Sprintf("(%d) [%s] The query of the total number of rows in the following table %s.%s of the %s database is completed.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	defer dispos.SqlRows.Close()
	return tmpTableCount, nil
}
