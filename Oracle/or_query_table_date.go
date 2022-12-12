package oracle

import (
	"database/sql"
	"fmt"
	"greatdbCheck/global"
	"strconv"
	"strings"
)

type QueryTableDate struct {
	Schema                         string
	Table                          string
	TmpTableFileName               string
	ColumnName                     []string
	ChanrowCount                   int
	TableColumn                    []map[string]string
	Sqlwhere                       string
	ColData                        []map[string]string
	SelectColumnString, LengthTrim string
	ColumnLengthAs                 []string
	BeginSeq                       string
	RowDataCh                      int64
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

func (or QueryTableDate) QueryTableIndexColumnInfo(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	strsql := fmt.Sprintf("select c.COLUMN_NAME as \"columnName\",decode(c.DATA_TYPE,'DATE',c.data_type,c.DATA_TYPE || '(' || c.data_LENGTH || ')') as \"columnType\", decode(co.constraint_type, 'P','1','0') as \"columnKey\",i.UNIQUENESS as \"nonUnique\", ic.INDEX_NAME as \"indexName\", ic.COLUMN_POSITION as \"IndexSeq\", c.COLUMN_ID as \"columnSeq\" from all_tab_cols c inner join all_ind_columns ic on c.TABLE_NAME = ic.TABLE_NAME and c.OWNER = ic.INDEX_OWNER and c.COLUMN_NAME = ic.COLUMN_NAME inner join all_indexes i on ic.INDEX_OWNER = i.OWNER and ic.INDEX_NAME = i.INDEX_NAME and ic.TABLE_NAME = i.TABLE_NAME left join all_constraints co on co.owner = c.owner and co.table_name = c.table_name and co.index_name = i.index_name where c.OWNER = '%s' and c.TABLE_NAME = '%s' ORDER BY I.INDEX_NAME, ic.COLUMN_POSITION", strings.ToUpper(or.Schema), or.Table)
	slog := fmt.Sprintf("(%d) oracle DB query table index column info exec sql is {%s}", logThreadSeq, strsql)
	global.Wlog.Info(slog)
	sqlRows, err := db.Query(strsql)
	if err != nil {
		elog := fmt.Sprintf("(%d) oracle DB exec sql fail. sql message is {%s} Error info is {%s}", logThreadSeq, strsql, err)
		global.Wlog.Error(elog)
	}
	clog := fmt.Sprintf("(%d) start dispos oracle DB query table %s.%s index column data.", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Info(clog)
	tableData, err := rowDataDisposMap(sqlRows, "IndexColumn", logThreadSeq)
	zlog := fmt.Sprintf("(%d) Oracle db query table index column data completion.", logThreadSeq)
	global.Wlog.Info(zlog)
	return tableData, err
}
func (or QueryTableDate) IndexDisposF(queryData []map[string]interface{}, logThreadSeq int64) ([]string, map[string][]string, map[string][]string) {
	nultiseriateIndexColumnMap := make(map[string][]string)
	multiseriateIndexColumnMap := make(map[string][]string)
	var PriIndexCol, uniIndexCol, mulIndexCol []string
	var indexName string
	alog := fmt.Sprintf("(%d) Start to classify different index columns of Oracle db table, to dispos it...", logThreadSeq)
	global.Wlog.Info(alog)
	for _, v := range queryData {
		var currIndexName = strings.ToUpper(v["indexName"].(string))
		//判断唯一索引（包含主键索引和普通索引）
		if v["nonUnique"].(string) == "UNIQUE" {
			if v["columnKey"].(string) == "1" {
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
		if v["nonUnique"].(string) == "NONUNIQUE" {
			if currIndexName != indexName {
				indexName = currIndexName
				multiseriateIndexColumnMap[indexName] = append(mulIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			} else {
				multiseriateIndexColumnMap[indexName] = append(multiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			}
		}
	}
	clog := fmt.Sprintf("(%d) Complete the classification of different index columns of Oracle db table. primary key message is {%s} num [%d] unique key message is {%s} num [%d] nounique key message is {%s} num [%d]", logThreadSeq, PriIndexCol, len(PriIndexCol), nultiseriateIndexColumnMap, len(nultiseriateIndexColumnMap), multiseriateIndexColumnMap, len(multiseriateIndexColumnMap))
	global.Wlog.Info(clog)
	return PriIndexCol, nultiseriateIndexColumnMap, multiseriateIndexColumnMap
}

func (or *QueryTableDate) TmpTableIndexColumnDataLength(logThreadSeq int64) (string, []string, string) {
	//根据索引列的多少，生成select 列条件，并生成列长度，为判断列是否为null或为空做判断
	var (
		selectColumnString, lengthTrim string
		columnLengthAs                 []string
		columnName                     = or.ColumnName
	)
	alog := fmt.Sprintf("(%d) Oracle DB starts to handle index class length.", logThreadSeq)
	global.Wlog.Info(alog)
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
func (or *QueryTableDate) TmpTableRowsCount(db *sql.DB, logThreadSeq int64) (int, error) {
	var (
		tmpTableCount int
	)
	alog := fmt.Sprintf("(%d) Start to query the total number of rows in Oracle DB current check table %s.%s ...", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Info(alog)
	sqlstr := fmt.Sprintf("select count(*) from \"%s\".\"%s\"", strings.ToUpper(or.Schema), or.Table)
	db.QueryRow(sqlstr).Scan(&tmpTableCount)
	blog := fmt.Sprintf("(%d) The total number of rows in Oracle DB database table %s.%s is [%d].", logThreadSeq, or.Schema, or.Table, tmpTableCount)
	global.Wlog.Info(blog)
	return tmpTableCount, nil
}
func (or *QueryTableDate) TmpTableIndexColumnDataDispos(db *sql.DB, logThreadSeq int64) ([]string, error) {
	var (
		strsql    string
		err       error
		rowDisops = func(rows *sql.Rows, threadId int64) []string {
			var tableRowData []string
			column, err1 := rows.Columns()
			if err1 != nil {
				clog := fmt.Sprintf("(%d) Oracle DB failed to get column information. sql message is {%s},Error info is {%s}.", logThreadSeq, strsql, err)
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
				for _, aa1 := range or.ColumnLengthAs {
					//对null做处理
					var tmpadf interface{}
					if fmt.Sprintf("%v", entry[strings.ToUpper(aa1)]) == "0" {
						tmpadf = "greatdbCheckNULL"
						entry[strings.ReplaceAll(aa1, "_LENGTH", "")] = tmpadf
					}
				}
				for _, aa1 := range or.ColumnName {
					if len(aa1) > 0 {
						tmpStringInputSlice = append(tmpStringInputSlice, fmt.Sprintf("%v", entry[strings.ToUpper(aa1)]))
					}
				}
				tableRowData = append(tableRowData, strings.Join(tmpStringInputSlice, "/*,*/"))
			}
			return tableRowData
		}
	)
	alog := fmt.Sprintf("(%d) Oracle DB check table %s.%s start query processing index column data", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Info(alog)

	bensql, _ := strconv.Atoi(strings.Split(or.BeginSeq, ",")[0])
	//countsql, _ := strconv.Atoi(strings.Split(beginSeq, ",")[1])
	strsql = fmt.Sprintf("SELECT * FROM ( SELECT A.*, ROWNUM RN FROM (SELECT %s,%s FROM \"%s\".\"%s\" group by %s order by %s) A WHERE ROWNUM <= %d) WHERE RN > %d", or.SelectColumnString, or.LengthTrim, strings.ToUpper(or.Schema), or.Table, or.SelectColumnString, or.SelectColumnString, int64(bensql)+or.RowDataCh, bensql)
	blog := fmt.Sprintf("(%d) Oracle DB query table index column data info exec sql is {%s}", logThreadSeq, strsql)
	global.Wlog.Info(blog)

	rows, err := db.Query(strsql)
	if err != nil {
		clog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s},Error info is {%s}.", logThreadSeq, strsql, err)
		global.Wlog.Error(clog)
	}
	if rows == nil {
		return nil, nil
	}
	tableRowData := rowDisops(rows, logThreadSeq)

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
	zlog := fmt.Sprintf("(%d) Oracle DB check table %s.%s query index column data completed", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Info(zlog)
	return tableRowData, nil
}

//处理oracle的null值，由于oracle的null值不会存储在索引上，需要单独处理
//func (or *QueryTableDate) TmpTableIndexColumnNullDispos(db *sql.DB, threadId int, selectColumnString, lengthTrim string, columnLengthAs, columnName []string, beginSeq, rowDataCh int64) ([]string, error) {
//	var (
//		strsql    string
//		err       error
//		rowDisops = func(rows *sql.Rows) []string {
//			var tableRowData []string
//			column, err1 := rows.Columns()
//			if err1 != nil {
//				global.Wlog.Error("[check table index column data] (", threadId, ") exec oracle sql fail. sql info: ", strsql, "error info: ", err1)
//			}
//			valuePtrs := make([]interface{}, len(column))
//			values := make([]interface{}, len(column))
//			for rows.Next() {
//				var tmpStringInputSlice []string
//				for i := 0; i < len(column); i++ {
//					valuePtrs[i] = &values[i]
//				}
//				rows.Scan(valuePtrs...)
//				entry := make(map[string]interface{})
//				for i, col := range column {
//					var v interface{}
//					val := values[i]
//					b, ok := val.([]byte)
//					if ok {
//						v = string(b)
//					} else {
//						v = val
//					}
//					entry[col] = v
//				}
//				for _, aa1 := range columnLengthAs {
//					//对null做处理
//					var tmpadf interface{}
//					if fmt.Sprintf("%v", entry[strings.ToUpper(aa1)]) == "0" {
//						tmpadf = "greatdbCheckNULL"
//						entry[strings.ReplaceAll(aa1, "_length", "")] = tmpadf
//					}
//				}
//				for _, aa1 := range columnName {
//					if len(aa1) > 0 {
//						tmpStringInputSlice = append(tmpStringInputSlice, fmt.Sprintf("%v", entry[strings.ToUpper(aa1)]))
//					}
//				}
//				tableRowData = append(tableRowData, strings.Join(tmpStringInputSlice, "/*,*/"))
//			}
//			return tableRowData
//		}
//	)
//	strsql = fmt.Sprintf("SELECT %s,NVL(LENGTH(trim(%s)), 0) as CODE_LENGTH FROM \"%s\".\"%s\" where code is null group by %s ", selectColumnString, lengthTrim, strings.ToUpper(or.Schema), or.Table, selectColumnString)
//	slog := fmt.Sprintf("(%d) oracle DB query table metadata info exec sql is {%s}", or.ThreadId, strsql)
//	global.Wlog.Info(slog)
//	rows, err := db.Query(strsql)
//	if err != nil {
//		global.Wlog.Error("[check table index column data] (", threadId, ") exec oracle sql fail. sql info: ", strsql, "error info: ", err)
//	}
//
//	if rows == nil {
//		return nil, nil
//	}
//	tableRowData := rowDisops(rows)
//	return tableRowData, nil
//}

//func (or *QueryTableDate) QueryTableAllColumnSeq(db *sql.DB) ([]map[string]interface{}, error) {
//	//sqlStr := fmt.Sprintf("select COLUMN_NAME as columnName ,COLUMN_TYPE as dataType,ORDINAL_POSITION as columnSeq from information_schema.columns where table_schema= '%s' and table_name='%s' order by ORDINAL_POSITION", or.Schema, or.Table)
//	sqlStr := fmt.Sprintf("SELECT column_name as \"columnName\",data_type as \"dataType\" FROM all_tab_cols c where c.OWNER = '%s' and c.TABLE_NAME = '%s' order by column_id asc", or.Schema, or.Table)
//	slog := fmt.Sprintf("(%d) oracle DB query table metadata info exec sql is {%s}", or.ThreadId, sqlStr)
//	global.Wlog.Info(slog)
//
//	sqlRows, err := db.Query(sqlStr)
//	if err != nil {
//		global.Wlog.Error("[check table index column data] exec oracle sql fail. sql info: ", sqlStr, "Error Info: ", err)
//		return nil, err
//	}
//	tableData, err := rowDataDisposMap(sqlRows, "IndexColumn", 15)
//	return tableData, err
//}

//处理无索引表查询select的order by列，防止原目标端查询的段不一致情况
func (or *QueryTableDate) NoIndexOrderBySingerColumn(orderCol []map[string]string) string {
	//处理order by column
	for _, v := range orderCol {
		if strings.HasPrefix(v["dataType"], "NUMBER") {
			return v["columnName"]
		}
		if strings.HasPrefix(v["dataType"], "DATE") {
			return v["columnName"]
		}
		if strings.HasPrefix(v["dataType"], "CHAR") {
			return v["columnName"]
		}
		if strings.HasPrefix(v["dataType"], "VARCHAR2") {
			return v["columnName"]
		}
	}
	return ""
}

func (or *QueryTableDate) NoIndexGeneratingQueryCriteria(db *sql.DB, beginSeq, chanrowCount int, orderByColumn string, logThreadSeq int64) (string, error) {
	var rowDataString []string
	sqlstr := fmt.Sprintf("SELECT * FROM ( SELECT A.*, ROWNUM RN FROM (SELECT * FROM \"%s\".\"%s\") A WHERE ROWNUM <= %d) WHERE RN > %d", strings.ToUpper(or.Schema), or.Table, beginSeq+chanrowCount, beginSeq)
	if orderByColumn != "" {
		sqlstr = fmt.Sprintf("SELECT * FROM ( SELECT A.*, ROWNUM RN FROM (SELECT * FROM \"%s\".\"%s\" order by %s) A WHERE ROWNUM <= %d) WHERE RN > %d", strings.ToUpper(or.Schema), or.Table, orderByColumn, beginSeq+chanrowCount, beginSeq)
	}

	alog := fmt.Sprintf("(%d) Oracle DB query table data info exec sql is {%s}", logThreadSeq, sqlstr)
	global.Wlog.Info(alog)
	rows, err := db.Query(sqlstr)
	if err != nil {
		blog := fmt.Sprintf("(%d) exec Oracle DB sql fail. sql info is {%s} error info is {%s}.", logThreadSeq, sqlstr, err)
		global.Wlog.Error(blog)
		return "", err
	}

	columns, err := rows.Columns()
	if err != nil {
		blog := fmt.Sprintf("(%d) get table columns of Oracle DB sql fail. error info is {%s}.", logThreadSeq, err)
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
func (or *QueryTableDate) performQueryConditions(db *sql.DB, sqlstr string, logThreadSeq int64) (string, error) {
	var rows *sql.Rows
	var rowDataString []string
	alog := fmt.Sprintf("(%d) Oracle DB query table chunk data info exec sql is {%s}", logThreadSeq, sqlstr)
	global.Wlog.Info(alog)

	rows, err := db.Query(sqlstr)
	if err != nil {
		blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlstr, err)
		global.Wlog.Error(blog)
	}
	clog := fmt.Sprintf("(%d) start dispos Oracle DB query table %s.%s chunk data info.", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Info(clog)
	if rows == nil {
		return "", nil
	}
	columns, err := rows.Columns()
	if err != nil {
		errInfo := fmt.Sprintf("(%d) Oracle DB Get the column fail. Error Info: ", logThreadSeq, err)
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
	zlog := fmt.Sprintf("(%d) Oracle DB query table %s.%s metadata data completion.", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Info(zlog)
	return strings.Join(rowDataString, "/*go actions rowData*/"), nil
}

func (or *QueryTableDate) GeneratingQueryCriteria(db *sql.DB, logThreadSeq int64) (string, error) {
	rowData, err := or.performQueryConditions(db, or.Sqlwhere, logThreadSeq)
	if err != nil {
		return "", err
	}
	return rowData, nil
}

func (or *QueryTableDate) GeneratingQuerySql(logThreadSeq int64) string {
	var columnNameSeq []string
	//处理oracle查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
	alog := fmt.Sprintf("(%d) Oracle DB starts to process the checklist %s.%s select column", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Info(alog)
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
	blog := fmt.Sprintf("(%d) Oracle DB checklist %s.%s select sql is {%s}", logThreadSeq, or.Schema, or.Table, fmt.Sprintf("select %s from `%s`.`%s` where %s", queryColumn, or.Schema, or.Table, or.Sqlwhere))
	global.Wlog.Info(blog)
	return fmt.Sprintf("select %s from \"%s\".\"%s\" where %s", queryColumn, strings.ToUpper(or.Schema), or.Table, or.Sqlwhere)
}
