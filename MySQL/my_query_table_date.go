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

/*
	查询MySQL库下指定表的索引统计信息
*/
func (my *QueryTable) QueryTableIndexColumnInfo(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var (
		Event     = "Q_Index_Statistics"
		tableData []map[string]interface{}
		err       error
	)
	strsql = fmt.Sprintf("select isc.COLUMN_NAME as columnName,isc.COLUMN_TYPE as columnType,isc.COLUMN_KEY as columnKey,isc.EXTRA as autoIncrement,iss.NON_UNIQUE as nonUnique,iss.INDEX_NAME as indexName,iss.SEQ_IN_INDEX IndexSeq,isc.ORDINAL_POSITION columnSeq from information_schema.columns isc inner join (select NON_UNIQUE,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME from information_schema.STATISTICS where table_schema='%s' and table_name='%s') as iss on isc.column_name =iss.column_name where isc.table_schema='%s' and isc.table_name='%s';", my.Schema, my.Table, my.Schema, my.Table)
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
func (my *QueryTable) IndexDisposF(queryData []map[string]interface{}, logThreadSeq int64) (map[string][]string, map[string][]string, map[string][]string) {
	var (
		nultiseriateIndexColumnMap            = make(map[string][]string)
		multiseriateIndexColumnMap            = make(map[string][]string)
		priIndexColumnMap                     = make(map[string][]string)
		PriIndexCol, uniIndexCol, mulIndexCol []string
		indexName                             string
		currIndexName                         string
		Event                                 = "E_Index_Filter"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to filter the primary key index, unique index, and common index based on the index information of the specified table %s.%s under the %s library", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	for _, v := range queryData {
		currIndexName = fmt.Sprintf("%s", v["indexName"])
		if my.LowerCaseTableNames == "no" {
			currIndexName = strings.ToUpper(fmt.Sprintf("%s", v["indexName"]))
		}
		//判断唯一索引（包含主键索引和普通索引）
		if v["nonUnique"].(string) == "0" {
			if currIndexName == "PRIMARY" {
				if currIndexName != indexName {
					indexName = currIndexName
				}
				PriIndexCol = append(PriIndexCol, fmt.Sprintf("%s", v["columnName"]))
				priIndexColumnMap["pri"] = PriIndexCol
			} else {
				if currIndexName != indexName {
					indexName = currIndexName
					nultiseriateIndexColumnMap[indexName] = append(uniIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
				} else {
					nultiseriateIndexColumnMap[indexName] = append(nultiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
				}
			}
		}
		//处理普通索引
		if v["nonUnique"].(string) != "0" {
			if currIndexName != indexName {
				indexName = currIndexName
				multiseriateIndexColumnMap[indexName] = append(mulIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			} else {
				multiseriateIndexColumnMap[indexName] = append(multiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			}
		}
	}
	vlog = fmt.Sprintf("(%d) [%s] The index information screening of the specified table %s.%s under the %s library is completed", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	return priIndexColumnMap, nultiseriateIndexColumnMap, multiseriateIndexColumnMap
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
		columnSelect["selectColumnLength"] = fmt.Sprintf("LENGTH(trim(%s)) as %s_length", strings.Join(columnName, ""), strings.Join(columnName, ""))
		columnSelect["selectColumnLengthSlice"] = fmt.Sprintf("%s_length", strings.Join(columnName, ""))
		columnSelect["selectColumnNull"] = fmt.Sprintf("%s is null ", strings.Join(columnName, ""))
		columnSelect["selectColumnEmpty"] = fmt.Sprintf("%s = '' ", strings.Join(columnName, ""))
	} else if len(columnName) > 1 {
		columnSelect["selectColumnName"] = strings.Join(columnName, "/*column*/")
		var aa, bb, cc, dd, ee []string
		for i := range columnName {
			aa = append(aa, fmt.Sprintf("LENGTH(trim(%s)) as %s_length", columnName[i], columnName[i]))
			bb = append(bb, fmt.Sprintf("%s_length", columnName[i]))
			cc = append(cc, fmt.Sprintf("%s is null ", columnName[i]))
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
	strsql = fmt.Sprintf("select index_name AS INDEX_NAME,column_name AS columnName,cardinality as CARDINALITY from INFORMATION_SCHEMA.STATISTICS where TABLE_SCHEMA = '%s' and table_name = '%s' and SEQ_IN_INDEX=1", my.Schema, my.Table)
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
		strsql = fmt.Sprintf("select sum(a.count) as sum from (select count(1) as count from `%s`.`%s` group by %s) a", my.Schema, my.Table, E)
	} else {
		strsql = fmt.Sprintf("select count(1) as sum from `%s`.`%s`", my.Schema, my.Table)
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
func (my QueryTable) FloatTypeQueryDispos(db *sql.DB, where string, logThreadSeq int64) (string, error) {
	var whereExist string
	column, err := my.TableAllColumn(db, logThreadSeq)
	if err != nil {
		return "", err
	}
	var C = make(map[string]string)
	whereExist = fmt.Sprintf("where %v", where)
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
				whereExist = fmt.Sprintf("where %s ", strings.ReplaceAll(where, fmt.Sprintf("%v = %v", i["columnName"], V), fmt.Sprintf("format(%v,%v) = format(%v,%v)", i["columnName"], Place, V, Place)))
			}
		}
	}
	return whereExist, nil
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
	version, err := my.DatabaseVersion(db, logThreadSeq)
	if err != nil {
		return nil, err
	}
	whereExist = where
	if where != "" {
		whereExist = fmt.Sprintf("where %s ", where)
		if strings.Contains(version, "5.7") {
			whereExist, err = my.FloatTypeQueryDispos(db, where, logThreadSeq)
			if err != nil {
				return nil, err
			}
		}
	}
	strsql = fmt.Sprintf("select %s as columnName,count(1) as count from `%s`.`%s` %s group by %s", columnName, my.Schema, my.Table, whereExist, columnName)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
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
	vlog = fmt.Sprintf("(%d) [%s] Start querying the statistical information of table %s.%s in the %s database and get the number of rows in the table", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select TABLE_ROWS as tableRows from information_schema.tables where table_schema='%s' and table_name ='%s'", my.Schema, my.Table)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return 0, err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return 0, err
	}
	defer dispos.SqlRows.Close()
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
	for _, i := range my.TableColumn {
		var tmpcolumnName string
		tmpcolumnName = i["columnName"]
		if strings.ToUpper(i["dataType"]) == "DATETIME" {
			tmpcolumnName = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", i["columnName"])
		}
		if strings.Contains(strings.ToUpper(i["dataType"]), "TIMESTAMP") {
			tmpcolumnName = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", i["columnName"])
		}
		if strings.HasPrefix(strings.ToUpper(i["dataType"]), "DOUBLE(") {
			dianAfter := strings.ReplaceAll(strings.Split(i["dataType"], ",")[1], ")", "")
			bb, _ := strconv.Atoi(dianAfter)
			dianBefer := strings.Split(strings.Split(i["dataType"], ",")[0], "(")[1]
			bbc, _ := strconv.Atoi(dianBefer)
			tmpcolumnName = fmt.Sprintf("CAST(%s AS DECIMAL(%d,%d))", i["columnName"], bbc, bb)
		}
		columnNameSeq = append(columnNameSeq, tmpcolumnName)
	}
	strsql = fmt.Sprintf("select %s from `%s`.`%s` limit %d,%d", strings.Join(columnNameSeq, ","), my.Schema, my.Table, beginSeq, chanrowCount)
	//if orderByColumn != "" {
	//	strsql = fmt.Sprintf("select * from `%s`.`%s` order by %s limit %d,%d", my.Schema, my.Table, orderByColumn, beginSeq, chanrowCount)
	//}
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
	MySQL 通过where条件查询表的分段数据（查询数据生成带有greatdbCheck标识的数据块）
*/
func (my QueryTable) GeneratingQueryCriteria(db *sql.DB, logThreadSeq int64) (string, error) {
	var (
		Event = "Q_Table_Data"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the segmented data of the following table %s.%s in the %s database through the where condition.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(my.Sqlwhere); err != nil {
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

/*
	MySQL 生成查询数据的sql语句
*/
func (my *QueryTable) GeneratingQuerySql(db *sql.DB, logThreadSeq int64) (string, error) {
	var (
		columnNameSeq []string
		Event         = "E_Table_SQL"
		selectSql     string
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to generate the data query sql of table %s.%s in the %s database", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
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
		if !strings.HasPrefix(strings.TrimSpace(my.Sqlwhere), "where") {
			my.Sqlwhere = fmt.Sprintf(" where %s ", my.Sqlwhere)
		}
	}
	selectSql = fmt.Sprintf("select %s from `%s`.`%s` %s", queryColumn, my.Schema, my.Table, my.Sqlwhere)
	vlog = fmt.Sprintf("(%d) [%s] Complete the data query sql of table %s.%s in the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	return selectSql, nil
}
