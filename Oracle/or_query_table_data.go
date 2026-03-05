package oracle

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"sort"
	"strconv"
	"strings"
)

/*
	查询Oracle库下指定表的索引统计信息
*/

func (or *QueryTable) QueryTableIndexColumnInfo(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var (
		Event     = "Q_Index_Statistics"
		tableData []map[string]interface{}
		err       error
	)
	strsql = fmt.Sprintf("SELECT c.COLUMN_NAME AS \"columnName\", DECODE(c.DATA_TYPE, 'DATE', c.data_type, c.DATA_TYPE || '(' || c.data_LENGTH || ')') AS \"columnType\", DECODE(co.constraint_type, 'P', '1', '0') AS \"columnKey\", i.UNIQUENESS AS \"nonUnique\", ic.INDEX_NAME AS \"indexName\", ic.COLUMN_POSITION AS \"IndexSeq\", c.COLUMN_ID AS \"columnSeq\" FROM all_tab_cols c INNER JOIN all_ind_columns ic ON c.TABLE_NAME=ic.TABLE_NAME AND c.OWNER=ic.INDEX_OWNER AND c.COLUMN_NAME=ic.COLUMN_NAME INNER JOIN all_indexes i ON ic.INDEX_OWNER=i.OWNER AND ic.INDEX_NAME=i.INDEX_NAME AND ic.TABLE_NAME=i.TABLE_NAME LEFT JOIN all_constraints co ON co.owner=c.owner AND co.table_name=c.table_name AND co.index_name=i.index_name WHERE %s AND %s ORDER BY I.INDEX_NAME, ic.COLUMN_POSITION", oracleMetadataMatchExpr("c.OWNER", or.Schema), oracleMetadataMatchExpr("c.TABLE_NAME", or.Table))
	vlog = fmt.Sprintf("(%d) [%s] Generate a sql statement to query the index statistics of table %s.%s under the %s database.sql messige is {%s}", logThreadSeq, Event, or.Schema, or.Table, DBType, strsql)
	global.Wlog.Debug(vlog)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	tableData, err = dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	vlog = fmt.Sprintf("(%d) [%s] The index statistics query of table %s.%s under the %s database is completed. index statistics is {%v}", logThreadSeq, Event, or.Schema, or.Table, DBType, tableData)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tableData, err
}

/*
根据Oracle库下指定表的索引信息，筛选主键索引、唯一索引、普通索引
*/
func (or *QueryTable) IndexDisposF(queryData []map[string]interface{}, logThreadSeq int64) (map[string][]string, map[string][]string, map[string][]string, map[string]string) {
	var (
		nultiseriateIndexColumnMap = make(map[string][]string)
		multiseriateIndexColumnMap = make(map[string][]string)
		priIndexColumnMap          = make(map[string][]string)
		indexName                  string
		currIndexName              string
		Event                      = "E_Index_Filter"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to filter the primary key index, unique index, and common index based on the index information of the specified table %s.%s under the %s library", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)

	// 用于临时存储每个索引的列顺序
	indexColumns := make(map[string]map[string]string)

	for _, v := range queryData {
		currIndexName = fmt.Sprintf("%s", v["indexName"])
		if or.CaseSensitiveObjectName == "no" {
			currIndexName = strings.ToUpper(fmt.Sprintf("%s", v["indexName"]))
		}

		columnName := fmt.Sprintf("%s", v["columnName"])
		indexSeq := fmt.Sprintf("%s", v["IndexSeq"])
		columnType := fmt.Sprintf("%s", v["columnType"])

		// 初始化map
		if _, exists := indexColumns[currIndexName]; !exists {
			indexColumns[currIndexName] = make(map[string]string)
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

	vlog = fmt.Sprintf("(%d) [%s] The index information screening of the specified table %s.%s under the %s library is completed", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)

	// Oracle暂不使用索引可见性信息，返回空map
	return priIndexColumnMap, nultiseriateIndexColumnMap, multiseriateIndexColumnMap, make(map[string]string)
}

/*
查询表，输出索引列数据的字符串长度，判断是否有null或空
*/
func (or *QueryTable) TmpTableIndexColumnSelectDispos(logThreadSeq int64) map[string]string {
	//根据索引列的多少，生成select 列条件，并生成列长度，为判断列是否为null或为空做判断
	var (
		columnSelect = make(map[string]string)
		columnName   = or.ColumnName
		Event        = "D_Index_Length"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the length of the query index column in table %s.%s in the specified %s database.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	//根据索引列的多少，生成select 列条件，并生成列长度，为判断列是否为null或为空做判断
	if len(columnName) == 1 {
		columnSelect["selectColumnName"] = strings.Join(columnName, "")
		columnSelect["selectColumnLength"] = fmt.Sprintf("LENGTH(trim(%s)) AS %s_length", strings.Join(columnName, ""), strings.Join(columnName, ""))
		columnSelect["selectColumnLengthSlice"] = fmt.Sprintf("%s_length", strings.Join(columnName, ""))
		columnSelect["selectColumnNull"] = fmt.Sprintf("%s is null ", strings.Join(columnName, ""))
		columnSelect["selectColumnEmpty"] = fmt.Sprintf("%s = '' ", strings.Join(columnName, ""))
		columnSelect["selectColumnData"] = fmt.Sprintf("%s != '' and %s is not null ", strings.Join(columnName, ""), strings.Join(columnName, ""))
	} else if len(columnName) > 1 {
		columnSelect["selectColumnName"] = strings.Join(columnName, "/*column*/")
		var aa, bb, cc, dd []string
		for i := range columnName {
			aa = append(aa, fmt.Sprintf("LENGTH(trim(%s)) AS %s_length", columnName[i], columnName[i]))
			bb = append(bb, fmt.Sprintf("%s_length", columnName[i]))
			cc = append(cc, fmt.Sprintf("%s is null ", columnName[i]))
			dd = append(dd, fmt.Sprintf("%s = '' ", columnName[i]))
		}
		columnSelect["selectColumnLength"] = strings.Join(aa, "/*column*/")
		columnSelect["selectColumnLengthSlice"] = strings.Join(bb, "/*column*/")
		columnSelect["selectColumnNull"] = strings.Join(cc, "/*column*/")
		columnSelect["selectColumnEmpty"] = strings.Join(dd, "/*column*/")
	}
	vlog = fmt.Sprintf("(%d) [%s] The length of the query index column of table %s.%s in the %s database is completed.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	return columnSelect
}

/*
Oracle 查询有索引表的总行数
*/
func (or *QueryTable) TmpTableIndexColumnRowsCount(db *sql.DB, logThreadSeq int64) (uint64, error) {
	var (
		tmpTableCount uint64
		Event         = "Q_Index_Table_Count"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the total number of rows in the following table %s.%s of the %s database.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("SELECT COUNT(1) AS \"sum\" FROM %s", oracleQualifiedTable(or.Schema, or.Table))
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
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
	vlog = fmt.Sprintf("(%d) [%s] The query of the total number of rows in the following table %s.%s of the %s database is completed.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tmpTableCount, nil
}

/*
Oracle库下查询表的索引列数据，并进行去重排序
*/
func (or *QueryTable) TmpTableColumnGroupDataDispos(db *sql.DB, where string, columnName string, logThreadSeq int64) (chan map[string]interface{}, error) {
	var (
		Event      = "Q_Index_ColumnData"
		whereExist string
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the index column data of the following table %s.%s in the %s database and de-reorder the data.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	whereExist = where
	if where != "" {
		whereExist = fmt.Sprintf("where %s ", where)
	}
	columnRef := oracleColumnIdentifier(columnName)
	strsql = fmt.Sprintf("SELECT %s AS \"columnName\", COUNT(1) AS \"count\" FROM %s %s GROUP BY %s ORDER BY %s", columnRef, oracleQualifiedTable(or.Schema, or.Table), whereExist, columnRef, columnRef)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	C := dispos.DataChanDispos()
	vlog = fmt.Sprintf("(%d) [%s] The index column data query of the following table %s.%s in the %s database is completed.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	return C, nil
}

/*
MySQL 查询表的统计信息中行数
*/
func (or *QueryTable) TableRows(db *sql.DB, logThreadSeq int64) (uint64, error) {
	var (
		tmpTableCount uint64
		Event         = "Q_Index_Table_Count"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the total number of rows in the following table %s.%s of the %s database.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("SELECT COUNT(1) AS \"sum\" FROM %s", oracleQualifiedTable(or.Schema, or.Table))
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
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
	vlog = fmt.Sprintf("(%d) [%s] The query of the total number of rows in the following table %s.%s of the %s database is completed.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tmpTableCount, nil
}

// 处理无索引表查询select的order by列，防止原目标端查询的段不一致情况
func (or *QueryTable) NoIndexOrderBySingerColumn(orderCol []map[string]string) []string {
	//处理order by column
	var selectC []string
	for _, v := range orderCol {
		selectC = append(selectC, v["columnName"])
		//if strings.HasPrefix(v["dataType"], "NUMBER") {
		//	return v["columnName"]
		//}
		//if strings.HasPrefix(v["dataType"], "DATE") {
		//	return v["columnName"]
		//}
		//if strings.HasPrefix(v["dataType"], "CHAR") {
		//	return v["columnName"]
		//}
		//if strings.HasPrefix(v["dataType"], "VARCHAR2") {
		//	return v["columnName"]
		//}
	}
	return selectC
}

func (or *QueryTable) NoIndexGeneratingQueryCriteria(db *sql.DB, beginSeq uint64, chanrowCount int, logThreadSeq int64) (string, error) {
	var (
		columnNameSeq []string
		Event         = "Q_table_Data"
	)
	// 统一Oracle可比较表达式，规避跨库日期/时间/间隔类型格式差异
	for _, i := range or.TableColumn {
		columnNameSeq = append(columnNameSeq, oracleComparableColumnExpr(i["columnName"], i["dataType"]))
	}
	queryColumn := strings.Join(columnNameSeq, ",")
	if queryColumn == "" {
		queryColumn = "*"
	}
	strsql = fmt.Sprintf("SELECT %s FROM ( SELECT A.*, ROWNUM RN FROM (SELECT * FROM %s) A WHERE ROWNUM <= %d) WHERE RN > %d", queryColumn, oracleQualifiedTable(or.Schema, or.Table), beginSeq+uint64(chanrowCount), beginSeq)
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
Oracle 通过where条件查询表的分段数据（查询数据生成带有gtchecksum标识的数据块）
*/
func (or *QueryTable) GeneratingQueryCriteria(db *sql.DB, logThreadSeq int64) (string, error) {
	var (
		Event = "Q_Table_Data"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the segmented data of the following table %s.%s in the %s database through the where condition.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(or.Sqlwhere); err != nil {
		return "", err
	}
	tableData, err := dispos.DataRowsDispos([]string{})
	if err != nil {
		return "", err
	}
	defer dispos.SqlRows.Close()
	vlog = fmt.Sprintf("(%d) [%s] Complete the data in the following table %s.%s of the %s database.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	return strings.Join(tableData, "/*go actions rowData*/"), nil
}

/*
Oracle 生成查询数据的sql语句
*/
func (or *QueryTable) GeneratingQuerySql(db *sql.DB, logThreadSeq int64) (string, error) {
	var (
		columnNameSeq []string
		Event         = "E_Table_SQL"
		selectSql     string
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to generate the data query sql of table %s.%s in the %s database", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	// 统一Oracle可比较表达式，规避跨库日期/时间/间隔类型格式差异
	for _, i := range or.TableColumn {
		columnNameSeq = append(columnNameSeq, oracleComparableColumnExpr(i["columnName"], i["dataType"]))
	}
	queryColumn := strings.Join(columnNameSeq, ",")
	if queryColumn == "" {
		queryColumn = "*"
	}
	selectSql = fmt.Sprintf("SELECT %s FROM %s WHERE %s", queryColumn, oracleQualifiedTable(or.Schema, or.Table), or.Sqlwhere)
	vlog = fmt.Sprintf("(%d) [%s] Complete the data query sql of table %s.%s in the %s database.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	return selectSql, nil
}

func oracleComparableColumnExpr(columnName, dataType string) string {
	columnExpr := oracleColumnIdentifier(columnName)
	t := strings.ToUpper(strings.TrimSpace(dataType))

	if t == "DATE" {
		return fmt.Sprintf("TO_CHAR(%s,'YYYY-MM-DD HH24:MI:SS')", columnExpr)
	}
	if strings.Contains(t, "TIMESTAMP") {
		return fmt.Sprintf("TO_CHAR(%s,'YYYY-MM-DD HH24:MI:SS')", columnExpr)
	}
	if strings.HasPrefix(t, "INTERVAL DAY") {
		// Normalize Oracle INTERVAL DAY TO SECOND to textual form first,
		// then parse to HH:MM:SS in Go comparison layer.
		// This avoids driver-dependent binary/nanosecond representations.
		return fmt.Sprintf("CASE WHEN %s IS NULL THEN NULL ELSE TRIM(TO_CHAR(%s)) END", columnExpr, columnExpr)
	}
	if strings.HasPrefix(t, "NUMBER(") {
		parts := strings.Split(t, ",")
		if len(parts) == 2 {
			dianAfter := strings.ReplaceAll(parts[1], ")", "")
			bb, bbErr := strconv.Atoi(dianAfter)
			dianBefore := strings.Split(parts[0], "(")
			if len(dianBefore) == 2 {
				bbc, bbcErr := strconv.Atoi(dianBefore[1])
				if bbErr == nil && bbcErr == nil {
					mu := "9"
					nu := "0"
					var tmpa, tmpb []string
					for ii := 0; ii < bb; ii++ {
						tmpa = append(tmpa, nu)
					}
					for ii := 1; ii < bbc-bb; ii++ {
						tmpb = append(tmpb, mu)
					}
					if bb == 0 {
						return fmt.Sprintf("TO_CHAR(%s,'FM%s0')", columnExpr, strings.Join(tmpb, ""))
					}
					return fmt.Sprintf("TO_CHAR(%s,'FM%s0.%s')", columnExpr, strings.Join(tmpb, ""), strings.Join(tmpa, ""))
				}
			}
		}
	}
	return columnExpr
}
