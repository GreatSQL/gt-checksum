package oracle

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
	Sqlwhere         string
	ColData          []map[string]string
}

//查询oracle 临时表
type IndexColumn struct {
	Schema           string
	Table            string
	TmpTableFileName string
	ColumnName       []string
	ChanrowCount     int
	TableColumn      []map[string]string
	Sqlwhere         string
	ColData          []map[string]interface{}
}

func (or QueryTableDate) QueryTableIndexColumnInfo(db *sql.DB) ([]map[string]interface{}, error) {
	strsql := fmt.Sprintf("select c.COLUMN_NAME as \"columnName\",decode(c.DATA_TYPE,'DATE',c.data_type,c.DATA_TYPE||'('||c.data_LENGTH||')')  as \"columnType\",i.index_type as \"columnKey\",i.UNIQUENESS as \"nonUnique\" ,ic.INDEX_NAME as \"indexName\",ic.COLUMN_POSITION as \"IndexSeq\", c.COLUMN_ID as \"columnSeq\" from all_tab_cols c inner join all_ind_columns ic on c.TABLE_NAME=ic.TABLE_NAME and c.OWNER=ic.INDEX_OWNER and c.COLUMN_NAME=ic.COLUMN_NAME inner join  all_indexes i on ic.INDEX_OWNER=i.OWNER and ic.INDEX_NAME=i.INDEX_NAME and ic.TABLE_NAME=i.TABLE_NAME where c.OWNER = '%s' and c.TABLE_NAME = '%s' ORDER BY I.INDEX_NAME,ic.COLUMN_POSITION", strings.ToUpper(or.Schema), or.Table)
	global.Wlog.Info("[check table index] dbexec oracle sql info: ", strsql)
	sqlRows, err := db.Query(strsql)
	if err != nil {
		global.Wlog.Error("[check table index] exec oracle sql fail. sql info: ", strsql, "error info: ", err)
	}
	tableData, err := rowDataDisposMap(sqlRows, "IndexColumn")
	global.Wlog.Info("[check IndexColumn] table ", or.Schema, ".", or.Table, " index column info is ", tableData)
	return tableData, err
}
func (or *QueryTableDate) TmpTableIndexColumnDataLength() (string, []string, string) {
	//根据索引列的多少，生成select 列条件，并生成列长度，为判断列是否为null或为空做判断
	var (
		selectColumnString, lengthTrim string
		columnLengthAs                 []string
		columnName                     = or.ColumnName
	)
	//根据索引列的多少，生成select 列条件，并生成列长度，为判断列是否为null或为空做判断
	if len(columnName) == 1 {
		selectColumnString = strings.Join(columnName, "")
		lengthTrim = fmt.Sprintf("NVL(LENGTH(trim(%s)),0) as %s_LENGTH", strings.Join(or.ColumnName, ""), strings.Join(columnName, ""))
		columnLengthAs = append(columnLengthAs, fmt.Sprintf("%s_LENGTH", strings.Join(columnName, "")))
	} else if len(columnName) > 1 {
		selectColumnString = strings.Join(columnName, ",")
		var aa []string
		for i := range columnName {
			aa = append(aa, fmt.Sprintf("NVL(LENGTH(trim(%s)),0) as %s_LENGTH", columnName[i], columnName[i]))
			columnLengthAs = append(columnLengthAs, fmt.Sprintf("%s_LENGTH", columnName[i]))
		}
		lengthTrim = strings.Join(aa, ",")
	}
	return selectColumnString, columnLengthAs, lengthTrim
}
func (or *QueryTableDate) TmpTableRowsCount(db *sql.DB) (int, error) {
	var (
		tmpTableCount int
	)
	sqlstr := fmt.Sprintf("select count(*) from \"%s\".\"%s\"", strings.ToUpper(or.Schema), or.Table)
	global.Wlog.Info("[check table index column data] exec oracle sql info: ", sqlstr)
	db.QueryRow(sqlstr).Scan(&tmpTableCount)
	return tmpTableCount, nil
}
func (or *QueryTableDate) TmpTableIndexColumnDataDispos(db *sql.DB, threadId int, selectColumnString, lengthTrim string, columnLengthAs, columnName []string, beginSeq string, rowDataCh int64) ([]string, error) {
	var (
		strsql    string
		err       error
		rowDisops = func(rows *sql.Rows) []string {
			var tableRowData []string
			column, err1 := rows.Columns()
			if err1 != nil {
				global.Wlog.Error("[check table index column data] (", threadId, ") exec oracle sql fail. sql info: ", strsql, "error info: ", err1)
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
					if fmt.Sprintf("%v", entry[strings.ToUpper(aa1)]) == "0" {
						tmpadf = "greatdbCheckNULL"
						entry[strings.ReplaceAll(aa1, "_LENGTH", "")] = tmpadf
					}
				}
				for _, aa1 := range columnName {
					if len(aa1) > 0 {
						tmpStringInputSlice = append(tmpStringInputSlice, fmt.Sprintf("%v", entry[strings.ToUpper(aa1)]))
					}
				}
				tableRowData = append(tableRowData, strings.Join(tmpStringInputSlice, "/*,*/"))
			}
			return tableRowData
		}
	)
	bensql, _ := strconv.Atoi(strings.Split(beginSeq, ",")[0])
	//countsql, _ := strconv.Atoi(strings.Split(beginSeq, ",")[1])
	strsql = fmt.Sprintf("SELECT * FROM ( SELECT A.*, ROWNUM RN FROM (SELECT %s,%s FROM \"%s\".\"%s\" group by %s) A WHERE ROWNUM <= %d) WHERE RN > %d", selectColumnString, lengthTrim, strings.ToUpper(or.Schema), or.Table, selectColumnString, int64(bensql)+rowDataCh, bensql)
	rows, err := db.Query(strsql)
	if err != nil {
		global.Wlog.Error("[check table index column data] (", threadId, ") exec oracle sql fail. sql info: ", strsql, "error info: ", err)
	}
	if rows == nil {
		return nil, nil
	}
	tableRowData := rowDisops(rows)
	rows.Close()
	//if int64(bensql)+rowDataCh >= int64(countsql) {
	//	strsql = fmt.Sprintf("SELECT %s,%s FROM \"%s\".\"%s\" where code is null group by %s ", selectColumnString, lengthTrim, strings.ToUpper(or.Schema), or.Table, selectColumnString)
	//	rows, err = db.Query(strsql)
	//	if err != nil {
	//		global.Wlog.Error("[check table index column data] (", threadId, ") exec oracle sql fail. sql info: ", strsql, "error info: ", err)
	//	}
	//	if rows == nil {
	//		return nil, nil
	//	}
	//	tableRowData1 := rowDisops(rows)
	//	for _, i := range tableRowData1 {
	//		tableRowData = append(tableRowData, i)
	//	}
	//}

	return tableRowData, nil
}

//处理oracle的null值，由于oracle的null值不会存储在索引上，需要单独处理
func (or *QueryTableDate) TmpTableIndexColumnNullDispos(db *sql.DB, threadId int, selectColumnString, lengthTrim string, columnLengthAs, columnName []string, beginSeq, rowDataCh int64) ([]string, error) {
	var (
		strsql    string
		err       error
		rowDisops = func(rows *sql.Rows) []string {
			var tableRowData []string
			column, err1 := rows.Columns()
			if err1 != nil {
				global.Wlog.Error("[check table index column data] (", threadId, ") exec oracle sql fail. sql info: ", strsql, "error info: ", err1)
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
					if fmt.Sprintf("%v", entry[strings.ToUpper(aa1)]) == "0" {
						tmpadf = "greatdbCheckNULL"
						entry[strings.ReplaceAll(aa1, "_length", "")] = tmpadf
					}
				}
				for _, aa1 := range columnName {
					if len(aa1) > 0 {
						tmpStringInputSlice = append(tmpStringInputSlice, fmt.Sprintf("%v", entry[strings.ToUpper(aa1)]))
					}
				}
				tableRowData = append(tableRowData, strings.Join(tmpStringInputSlice, "/*,*/"))
			}
			return tableRowData
		}
	)
	strsql = fmt.Sprintf("SELECT %s,NVL(LENGTH(trim(%s)), 0) as CODE_LENGTH FROM \"%s\".\"%s\" where code is null group by %s ", selectColumnString, lengthTrim, strings.ToUpper(or.Schema), or.Table, selectColumnString)
	rows, err := db.Query(strsql)
	if err != nil {
		global.Wlog.Error("[check table index column data] (", threadId, ") exec oracle sql fail. sql info: ", strsql, "error info: ", err)
	}

	if rows == nil {
		return nil, nil
	}
	tableRowData := rowDisops(rows)
	return tableRowData, nil
}
func (or *QueryTableDate) QueryTableAllColumnSeq(db *sql.DB) ([]map[string]interface{}, error) {
	//sqlStr := fmt.Sprintf("select COLUMN_NAME as columnName ,COLUMN_TYPE as dataType,ORDINAL_POSITION as columnSeq from information_schema.columns where table_schema= '%s' and table_name='%s' order by ORDINAL_POSITION", or.Schema, or.Table)
	sqlStr := fmt.Sprintf("SELECT column_name as \"columnName\",data_type as \"dataType\" FROM all_tab_cols c where c.OWNER = '%s' and c.TABLE_NAME = '%s' order by column_id asc", or.Schema, or.Table)
	global.Wlog.Info("[check table index column data] exec oracle sql info: ", sqlStr)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		global.Wlog.Error("[check table index column data] exec oracle sql fail. sql info: ", sqlStr, "Error Info: ", err)
		return nil, err
	}
	tableData, err := rowDataDisposMap(sqlRows, "IndexColumn")
	return tableData, err
}
func (or *QueryTableDate) NoIndexGeneratingQueryCriteria(db *sql.DB, beginSeq, chanrowCount int) (string, error) {
	var rowDataString []string
	sqlstr := fmt.Sprintf("SELECT * FROM ( SELECT A.*, ROWNUM RN FROM (SELECT * FROM \"%s\".\"%s\") A WHERE ROWNUM <= %d) WHERE RN > %d", strings.ToUpper(or.Schema), or.Table, beginSeq+chanrowCount, beginSeq)
	rows, err := db.Query(sqlstr)
	if err != nil {
		global.Wlog.Error("exec oracle sql fail. sql info: ", sqlstr, "error info: ", err)
	}
	global.Wlog.Debug("GreatdbCheck exec sql: \"", sqlstr, "\" at the oracle")
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
func (or *QueryTableDate) performQueryConditions(db *sql.DB, sqlstr string) (string, error) {
	var rows *sql.Rows
	var rowDataString []string
	rows, err := db.Query(sqlstr)
	if err != nil {
		global.Wlog.Error("exec oracle sql fail. sql info: ", sqlstr, "error info: ", err)
	}
	global.Wlog.Debug("GreatdbCheck exec sql: \"", sqlstr, "\" at the oracle")
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

func (or *QueryTableDate) GeneratingQueryCriteria(db *sql.DB) (string, error) {
	rowData, err := or.performQueryConditions(db, or.Sqlwhere)
	if err != nil {
		return "", err
	}
	return rowData, nil
}

func (or *QueryTableDate) GeneratingQuerySql() string {
	var columnNameSeq []string

	//处理oracle查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
	for _, i := range or.TableColumn {
		mu := "9"
		nu := "0"
		var tmpcolumnName string
		tmpcolumnName = i["columnName"]
		if strings.ToUpper(i["dataType"]) == "DATE" {
			tmpcolumnName = fmt.Sprintf("to_char(%s,'YYYY-MM-DD HH24:MI:SS')", i["columnName"])
		}
		if strings.Contains(strings.ToUpper(i["dataType"]), "TIMESTAMP") {
			tmpcolumnName = fmt.Sprintf("to_char(%s,'YYYY-MM-DD HH24:MI:SS')", i["columnName"])
		}
		if strings.HasPrefix(strings.ToUpper(i["dataType"]), "NUMBER(") {
			dianAfter := strings.ReplaceAll(strings.Split(i["dataType"], ",")[1], ")", "")
			bb, _ := strconv.Atoi(dianAfter)
			dianBefer := strings.Split(strings.Split(i["dataType"], ",")[0], "(")[1]
			bbc, _ := strconv.Atoi(dianBefer)
			var tmpa, tmpb []string
			for ii := 0; ii < bb; ii++ {
				tmpa = append(tmpa, nu)
			}
			for ii := 1; ii < bbc-bb; ii++ {
				tmpb = append(tmpb, mu)
			}
			if bb == 0 {
				tmpcolumnName = fmt.Sprintf("to_char(%s,'FM%s0')", i["columnName"], strings.Join(tmpb, ""))
			} else {
				tmpcolumnName = fmt.Sprintf("to_char(%s,'FM%s0.%s')", i["columnName"], strings.Join(tmpb, ""), strings.Join(tmpa, ""))
			}
		}
		columnNameSeq = append(columnNameSeq, tmpcolumnName)
	}
	queryColumn := strings.Join(columnNameSeq, ",")
	//sqlstr := fmt.Sprintf("select %s from \"%s\".\"%s\" as of scn %s where %s", queryColumn, schema, table, oracleScn, sqlWhere)
	//fmt.Println(fmt.Sprintf("select %s from \"%s\".\"%s\" where %s", queryColumn, strings.ToUpper(or.Schema), or.Table, or.Sqlwhere))
	return fmt.Sprintf("select %s from \"%s\".\"%s\" where %s", queryColumn, strings.ToUpper(or.Schema), or.Table, or.Sqlwhere)
}
