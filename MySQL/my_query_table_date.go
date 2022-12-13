package mysql

import (
	"database/sql"
	"fmt"
	"greatdbCheck/global"
	"strconv"
	"strings"
)

type QueryTableDate struct {
	Schema           string
	Table            string
	TmpTableFileName string
	ColumnName       []string
	ChanrowCount     int
	TableColumn      []map[string]string
	//TableColumn global.TableAllColumnInfoS
	Sqlwhere                       string
	ColData                        []map[string]string
	SelectColumnString, LengthTrim string
	ColumnLengthAs                 []string
	BeginSeq                       string
	RowDataCh                      int64
	//ColData map[string]global.TableAllColumnInfoS
}

//查询MySQL 临时表
type IndexColumn struct {
	Schema           string
	Table            string
	TmpTableFileName string
	ColumnName       []string
	ChanrowCount     int
	TableColumn      []map[string]string
	Sqlwhere         string
	ColData          []map[string]string
}

/*
   查询表的索引信息， 并输出当前所有的索引信息列
*/
func (my QueryTableDate) QueryTableIndexColumnInfo(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	strsql := fmt.Sprintf("select isc.COLUMN_NAME as columnName,isc.COLUMN_TYPE as columnType,isc.COLUMN_KEY as columnKey,isc.EXTRA as autoIncrement,iss.NON_UNIQUE as nonUnique,iss.INDEX_NAME as indexName,iss.SEQ_IN_INDEX IndexSeq,isc.ORDINAL_POSITION columnSeq from information_schema.columns isc inner join (select NON_UNIQUE,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME from information_schema.STATISTICS where table_schema='%s' and table_name='%s') as iss on isc.column_name =iss.column_name where isc.table_schema='%s' and isc.table_name='%s';", my.Schema, my.Table, my.Schema, my.Table)
	slog := fmt.Sprintf("(%d) MySQL DB query table index column info exec sql is {%s}", logThreadSeq, strsql)
	global.Wlog.Info(slog)
	sqlRows, err := db.Query(strsql)
	if err != nil {
		elog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}", logThreadSeq, strsql, err)
		global.Wlog.Error(elog)
	}
	clog := fmt.Sprintf("(%d) start dispos oracle DB query table %s.%s index column data.", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Info(clog)
	tableData, err := rowDataDisposMap(sqlRows, "IndexColumn", logThreadSeq)
	zlog := fmt.Sprintf("(%d) Oracle db query table index column data completion.", logThreadSeq)
	global.Wlog.Info(zlog)
	
	return tableData, err
}
func (my QueryTableDate) IndexDisposF(queryData []map[string]interface{}, logThreadSeq int64) ([]string, map[string][]string, map[string][]string) {
	nultiseriateIndexColumnMap := make(map[string][]string)
	multiseriateIndexColumnMap := make(map[string][]string)
	var PriIndexCol, uniIndexCol, mulIndexCol []string
	var indexName string
	alog := fmt.Sprintf("(%d) Start to classify different index columns of MySQL DB table, to dispos it...", logThreadSeq)
	global.Wlog.Info(alog)
	for _, v := range queryData {
		var currIndexName = strings.ToUpper(v["indexName"].(string))
		//判断唯一索引（包含主键索引和普通索引）
		if v["nonUnique"].(string) == "0" {
			if currIndexName == "PRIMARY" {
				if currIndexName != indexName {
					indexName = currIndexName
				}
				PriIndexCol = append(PriIndexCol, fmt.Sprintf("%s", v["columnName"]))
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
	clog := fmt.Sprintf("(%d) Complete the classification of different index columns of MySQL DB table. primary key message is {%s} num [%d] unique key message is {%s} num [%d] nounique key message is {%s} num [%d]", logThreadSeq, PriIndexCol, len(PriIndexCol), nultiseriateIndexColumnMap, len(nultiseriateIndexColumnMap), multiseriateIndexColumnMap, len(multiseriateIndexColumnMap))
	global.Wlog.Info(clog)
	return PriIndexCol, nultiseriateIndexColumnMap, multiseriateIndexColumnMap
}

func (my *IndexColumn) QPrepareRow(db *sql.DB, sqlStr string) (*sql.Rows, error) {
	global.Wlog.Info("begin exec query sql \"", sqlStr, "\"")
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		global.Wlog.Error("select sql exec fail. sql: ", sqlStr, " Error info: ", err)
		return nil, err
	}
	global.Wlog.Info("sql exec successful. sql info: ", sqlStr)
	return sqlRows, nil
}

func (my *IndexColumn) QueryTableAllColumnSeq(db *sql.DB) ([]map[string]interface{}, error) {
	sqlStr := fmt.Sprintf("select COLUMN_NAME as columnName ,COLUMN_TYPE as dataType,ORDINAL_POSITION as columnSeq from information_schema.columns where table_schema= '%s' and table_name='%s' order by ORDINAL_POSITION;", my.Schema, my.Table)
	global.Wlog.Info("[check table index column data] exec mysql sql info: ", sqlStr)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		global.Wlog.Error("[check table index column data] exec mysql sql fail. sql info: ", sqlStr, "Error Info: ", err)
		return nil, err
	}
	tableData, err := rowDataDisposMap(sqlRows, "IndexColumn", 13)
	return tableData, err
}

/*
   查询表，生成select column信息并输出索引列数据的字符串长度，判断是否有null或空
*/
func (my *QueryTableDate) TmpTableIndexColumnDataLength(logThreadSeq int64) (string, []string, string) {
	//根据索引列的多少，生成select 列条件，并生成列长度，为判断列是否为null或为空做判断
	var (
		selectColumnString, lengthTrim string
		columnLengthAs                 []string
		columnName                     = my.ColumnName
	)
	alog := fmt.Sprintf("(%d) MySQL DB starts to handle index class length.", logThreadSeq)
	global.Wlog.Info(alog)
	//根据索引列的多少，生成select 列条件，并生成列长度，为判断列是否为null或为空做判断
	if len(columnName) == 1 {
		selectColumnString = strings.Join(columnName, "")
		lengthTrim = fmt.Sprintf("LENGTH(trim(%s)) as %s_length", strings.Join(my.ColumnName, ""), strings.Join(columnName, ""))
		columnLengthAs = append(columnLengthAs, fmt.Sprintf("%s_length", strings.Join(columnName, "")))
	} else if len(columnName) > 1 {
		selectColumnString = strings.Join(columnName, ",")
		var aa []string
		for i := range columnName {
			aa = append(aa, fmt.Sprintf("LENGTH(trim(%s)) as %s_length", columnName[i], columnName[i]))
			columnLengthAs = append(columnLengthAs, fmt.Sprintf("%s_length", columnName[i]))
		}
		lengthTrim = strings.Join(aa, ",")
	}
	return selectColumnString, columnLengthAs, lengthTrim
}

/*
   查询表，生成select column信息并输出索引列数据的字符串长度，判断是否有null或空
*/
func (my *QueryTableDate) TmpTableRowsCount(db *sql.DB, logThreadSeq int64) (int, error) {
	var (
		tmpTableCount int
	)
	alog := fmt.Sprintf("(%d) Start to query the total number of rows in MySQL DB current check table %s.%s ...", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Info(alog)
	sqlstr := fmt.Sprintf("select a.* from (select (@i:=@i+1) as i from `%s`.`%s`,(select @i:=0) i) a order by a.i desc limit 1", my.Schema, my.Table)
	db.QueryRow(sqlstr).Scan(&tmpTableCount)
	blog := fmt.Sprintf("(%d) The total number of rows in MySQL DB database table %s.%s is [%d].", logThreadSeq, my.Schema, my.Table, tmpTableCount)
	global.Wlog.Info(blog)
	return tmpTableCount, nil
}

/*
	按照分页查询表的索引列数据
*/
func (my *QueryTableDate) TmpTableIndexColumnDataDispos(db *sql.DB, logThreadSeq int64) ([]string, error) {
	var (
		strsql string
		err    error
	)
	alog := fmt.Sprintf("(%d) MySQL DB check table %s.%s start query processing index column data", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Info(alog)
	bens, _ := strconv.Atoi(strings.Split(my.BeginSeq, ",")[0])
	strsql = fmt.Sprintf("select %s,%s from `%s`.`%s` group by %s order by %s limit %d,%d;", my.SelectColumnString, my.LengthTrim, my.Schema, my.Table, my.SelectColumnString, my.SelectColumnString, bens, my.RowDataCh)
	blog := fmt.Sprintf("(%d) MySQL DB query table index column data info exec sql is {%s}", logThreadSeq, strsql)
	global.Wlog.Info(blog)

	rows, err := db.Query(strsql)
	if err != nil {
		clog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s},Error info is {%s}.", logThreadSeq, strsql, err)
		global.Wlog.Error(clog)
	}
	var tableRowData []string
	if rows == nil {
		return nil, nil
	}

	column, err1 := rows.Columns()
	if err1 != nil {
		clog := fmt.Sprintf("(%d) MySQL DB failed to get column information. sql message is {%s},Error info is {%s}.", logThreadSeq, strsql, err)
		global.Wlog.Error(clog)
	}
	valuePtrs := make([]interface{}, len(column))
	values := make([]interface{}, len(column))
	for rows.Next() {
		var tmpStringInputSlice []string
		for i := 0; i < len(column); i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range column {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		for _, aa1 := range my.ColumnLengthAs {
			//对null做处理
			var tmpadf interface{}
			if fmt.Sprintf("%v", entry[aa1]) == "<nil>" {
				tmpadf = "greatdbCheckNULL"
				entry[strings.ReplaceAll(aa1, "_length", "")] = tmpadf
			}
			//对空字符串做处理
			if fmt.Sprintf("%v", entry[aa1]) == "0" {
				tmpadf = "greatdbCheckEmtry"
				entry[strings.ReplaceAll(aa1, "_length", "")] = tmpadf
			}
		}
		for _, aa1 := range my.ColumnName {
			if len(aa1) > 0 {
				tmpStringInputSlice = append(tmpStringInputSlice, fmt.Sprintf("%v", entry[aa1]))
			}
		}
		tableRowData = append(tableRowData, strings.Join(tmpStringInputSlice, "/*,*/"))
	}
	rows.Close()
	zlog := fmt.Sprintf("(%d) MySQL DB check table %s.%s query index column data completed", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Info(zlog)
	return tableRowData, nil
}

//处理无索引表查询select的order by列，防止原目标端查询的段不一致情况
func (or *QueryTableDate) NoIndexOrderBySingerColumn(orderCol []map[string]string) string {
	//处理order by column
	for _, v := range orderCol {
		if strings.HasPrefix(v["dataType"], "INT") {
			return v["columnName"]
		}
		if strings.HasPrefix(v["dataType"], "DATETIME") {
			return v["columnName"]
		}
		if strings.HasPrefix(v["dataType"], "TIMESTAMP") {
			return v["columnName"]
		}
		if strings.HasPrefix(v["dataType"], "CHAR") {
			return v["columnName"]
		}
		if strings.HasPrefix(v["dataType"], "VARCHAR") {
			return v["columnName"]
		}
	}
	return ""
}

/*
	无索引下的处理
*/
func (my *QueryTableDate) NoIndexGeneratingQueryCriteria(db *sql.DB, beginSeq, chanrowCount int, orderByColumn string, logThreadSeq int64) (string, error) {
	var rowDataString []string
	sqlstr := fmt.Sprintf("select * from `%s`.`%s` limit %d,%d", my.Schema, my.Table, beginSeq, chanrowCount)
	if orderByColumn != "" {
		sqlstr = fmt.Sprintf("select * from `%s`.`%s` order by %s limit %d,%d", my.Schema, my.Table, orderByColumn, beginSeq, chanrowCount)
	}

	alog := fmt.Sprintf("(%d) MySQL DB query table data info exec sql is {%s}", logThreadSeq, sqlstr)
	global.Wlog.Info(alog)
	rows, err := db.Query(sqlstr)
	if err != nil {
		blog := fmt.Sprintf("(%d) exec MySQL DB sql fail. sql info is {%s} error info is {%s}.", logThreadSeq, sqlstr, err)
		global.Wlog.Error(blog)
	}
	columns, err := rows.Columns()
	if err != nil {
		blog := fmt.Sprintf("(%d) get table columns of MySQL DB sql fail. error info is {%s}.", logThreadSeq, err)
		global.Wlog.Error(blog)
		return "", err
	}
	valuePtrs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for rows.Next() {
		var tmpaaS []string
		for i := 0; i < len(columns); i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		for i := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			tmpaaS = append(tmpaaS, fmt.Sprintf("%v", v))
		}
		tmpaa := strings.Join(tmpaaS, "/*go actions columnData*/")
		rowDataString = append(rowDataString, tmpaa)
	}
	rows.Close()
	return strings.Join(rowDataString, "/*go actions rowData*/"), nil
}

func (my *QueryTableDate) performQueryConditions(db *sql.DB, sqlstr string, logThreadSeq int64) (string, error) {
	var rows *sql.Rows
	var rowDataString []string
	alog := fmt.Sprintf("(%d) MySQL DB query table chunk data info exec sql is {%s}", logThreadSeq, sqlstr)
	global.Wlog.Info(alog)

	rows, err := db.Query(sqlstr)
	if err != nil {
		blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlstr, err)
		global.Wlog.Error(blog)
	}
	clog := fmt.Sprintf("(%d) start dispos MySQL DB query table %s.%s chunk data info.", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Info(clog)
	if rows == nil {
		return "", nil
	}
	columns, err := rows.Columns()
	if err != nil {
		errInfo := fmt.Sprintf("(%d) MySQL DB Get the column fail. Error Info: ", logThreadSeq, err)
		global.Wlog.Error(errInfo)
		return "", err
	}
	valuePtrs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for rows.Next() {
		var tmpaaS []string
		for i := 0; i < len(columns); i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		for i := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			tmpaaS = append(tmpaaS, fmt.Sprintf("%v", v))
		}
		tmpaa := strings.Join(tmpaaS, "/*go actions columnData*/")
		rowDataString = append(rowDataString, tmpaa)
	}
	rows.Close()
	zlog := fmt.Sprintf("(%d) MySQL DB query table %s.%s metadata data completion.", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Info(zlog)
	return strings.Join(rowDataString, "/*go actions rowData*/"), nil
}

/*
   该函数用于需要查询源目表端数据库校验块数据，查询数据生成带有greatdbCheck标识的数据块
*/
func (my QueryTableDate) GeneratingQueryCriteria(db *sql.DB, logThreadSeq int64) (string, error) {
	rowData, err := my.performQueryConditions(db, my.Sqlwhere, logThreadSeq)
	if err != nil {
		return "", err
	}
	return rowData, nil
}

/*
   该函数用于需要查询源目表端数据库校验块数据，查询数据生成带有greatdbCheck标识的数据块
*/
func (my *QueryTableDate) GeneratingQuerySql(logThreadSeq int64) string {
	var columnNameSeq []string
	//处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
	alog := fmt.Sprintf("(%d) MySQL DB starts to process the checklist %s.%s select column", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Info(alog)
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
	queryColumn := strings.Join(columnNameSeq, ",")
	blog := fmt.Sprintf("(%d) MySQL DB checklist %s.%s select sql is {%s}", logThreadSeq, my.Schema, my.Table, fmt.Sprintf("select %s from `%s`.`%s` where %s", queryColumn, my.Schema, my.Table, my.Sqlwhere))
	global.Wlog.Info(blog)
	//fmt.Println(fmt.Sprintf("select %s from `%s`.`%s` where %s", queryColumn, my.Schema, my.Table, my.Sqlwhere))
	return fmt.Sprintf("select %s from `%s`.`%s` where %s", queryColumn, my.Schema, my.Table, my.Sqlwhere)
}
