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
	Sqlwhere string
	ColData  []map[string]string
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
func (my QueryTableDate) QueryTableIndexColumnInfo(db *sql.DB) ([]map[string]interface{}, error) {
	strsql := fmt.Sprintf("select isc.COLUMN_NAME as columnName,isc.COLUMN_TYPE as columnType,isc.COLUMN_KEY as columnKey,isc.EXTRA as autoIncrement,iss.NON_UNIQUE as nonUnique,iss.INDEX_NAME as indexName,iss.SEQ_IN_INDEX IndexSeq,isc.ORDINAL_POSITION columnSeq from information_schema.columns isc inner join (select NON_UNIQUE,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME from information_schema.STATISTICS where table_schema='%s' and table_name='%s') as iss on isc.column_name =iss.column_name where isc.table_schema='%s' and isc.table_name='%s';", my.Schema, my.Table, my.Schema, my.Table)
	global.Wlog.Info("[check table index] dbexec mysql sql info: ", strsql)
	sqlRows, err := db.Query(strsql)
	if err != nil {
		global.Wlog.Error("[check table index] exec mysql sql fail. sql info: ", strsql, "error info: ", err)
	}
	tableData, err := rowDataDisposMap(sqlRows, "IndexColumn")
	global.Wlog.Info("[check IndexColumn] table ", my.Schema, ".", my.Table, " index column info is ", tableData)
	return tableData, err

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
	tableData, err := rowDataDisposMap(sqlRows, "IndexColumn")
	return tableData, err
}

/*
   查询表，生成select column信息并输出索引列数据的字符串长度，判断是否有null或空
*/
func (my *QueryTableDate) TmpTableIndexColumnDataLength() (string, []string, string) {
	//根据索引列的多少，生成select 列条件，并生成列长度，为判断列是否为null或为空做判断
	var (
		selectColumnString, lengthTrim string
		columnLengthAs                 []string
		columnName                     = my.ColumnName
	)
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
func (my *QueryTableDate) TmpTableRowsCount(db *sql.DB) (int, error) {
	var (
		tmpTableCount int
	)
	sqlstr := fmt.Sprintf("select a.* from (select (@i:=@i+1) as i from `%s`.`%s`,(select @i:=0) i) a order by a.i desc limit 1;", my.Schema, my.Table)
	global.Wlog.Info("[check table index column data] exec mysql sql info: ", sqlstr)
	db.QueryRow(sqlstr).Scan(&tmpTableCount)
	return tmpTableCount, nil
}

/*
	按照分页查询表的索引列数据
*/
func (my *QueryTableDate) TmpTableIndexColumnDataDispos(db *sql.DB, threadId int, selectColumnString, lengthTrim string, columnLengthAs, columnName []string, beginSeq, rowDataCh int64) ([]string, error) {
	var (
		strsql string
		err    error
	)
	strsql = fmt.Sprintf("select %s,%s from `%s`.`%s` group by %s limit %d,%d;", selectColumnString, lengthTrim, my.Schema, my.Table, selectColumnString, beginSeq, rowDataCh)
	rows, err := db.Query(strsql)
	if err != nil {
		global.Wlog.Error("[check table index column data] (", threadId, ") exec mysql sql fail. sql info: ", strsql, "error info: ", err)
	}
	var tableRowData []string
	if rows == nil {
		return nil, nil
	}
	column, err1 := rows.Columns()
	if err1 != nil {
		global.Wlog.Error("[check table index column data] (", threadId, ") exec mysql sql fail. sql info: ", strsql, "error info: ", err1)
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
		for _, aa1 := range columnLengthAs {
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
		for _, aa1 := range columnName {
			if len(aa1) > 0 {
				tmpStringInputSlice = append(tmpStringInputSlice, fmt.Sprintf("%v", entry[aa1]))
			}
		}
		tableRowData = append(tableRowData, strings.Join(tmpStringInputSlice, "/*,*/"))
	}
	rows.Close()
	return tableRowData, nil
}

/*
	无索引下的处理
*/
func (my *QueryTableDate) NoIndexGeneratingQueryCriteria(db *sql.DB, beginSeq, chanrowCount int) (string, error) {
	var rowDataString []string
	sqlstr := fmt.Sprintf("select * from `%s`.`%s` limit %d,%d", my.Schema, my.Table, beginSeq, chanrowCount)
	rows, err := db.Query(sqlstr)
	if err != nil {
		global.Wlog.Error("exec mysql sql fail. sql info: ", sqlstr, "error info: ", err)
	}
	global.Wlog.Debug("GreatdbCheck exec sql: \"", sqlstr, "\" at the MySQL")
	columns, err := rows.Columns()
	if err != nil {
		global.Wlog.Error("GreatdbCheck exec sql fail. sql: ", sqlstr, "error info: ", err)
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

func (my *QueryTableDate) performQueryConditions(db *sql.DB, sqlstr string) (string, error) {
	var rows *sql.Rows
	var rowDataString []string
	rows, err := db.Query(sqlstr)
	if err != nil {
		global.Wlog.Error("exec mysql sql fail. sql info: ", sqlstr, "error info: ", err)
	}
	global.Wlog.Debug("GreatdbCheck exec sql: \"", sqlstr, "\" at the MySQL")
	if rows == nil {
		return "", nil
	}
	columns, err := rows.Columns()
	if err != nil {
		global.Wlog.Error("GreatdbCheck exec sql fail. sql: ", sqlstr, "error info: ", err)
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

/*
   该函数用于需要查询源目表端数据库校验块数据，查询数据生成带有greatdbCheck标识的数据块
*/
func (my QueryTableDate) GeneratingQueryCriteria(db *sql.DB) (string, error) {
	rowData, err := my.performQueryConditions(db, my.Sqlwhere)
	if err != nil {
		return "", err
	}
	return rowData, nil
}

/*
   该函数用于需要查询源目表端数据库校验块数据，查询数据生成带有greatdbCheck标识的数据块
*/
func (my *QueryTableDate) GeneratingQuerySql() string {
	var columnNameSeq []string
	//处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
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
	return fmt.Sprintf("select %s from `%s`.`%s` where %s", queryColumn, my.Schema, my.Table, my.Sqlwhere)
}
