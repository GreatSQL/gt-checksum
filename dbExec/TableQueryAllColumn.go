package dbExec

//
//import (
//	"database/sql"
//	"fmt"
//	"gt-checksum/global"
//	"strings"
//)
//
//type TableQueryAllColumnStruct struct{}
//
//type TableQueryAllColumnInterface interface {
//	GeneratingQuerySql() string
//	GeneratingQueryCriteria(db *sql.DB) (string, error)
//}
//
//type MySQLAllColumnStruct struct {
//	schema      string
//	table       string
//	tableColumn []map[string]string
//	sqlwhere    string
//	colData     []map[string]interface{}
//}
//type OracleAllColumnStruct struct {
//	schema      string
//	table       string
//	tableColumn []map[string]string
//	sqlwhere    string
//	colData     []map[string]interface{}
//}
//
//func (my *MySQLAllColumnStruct) performQueryConditions(db *sql.DB, sqlstr string) (string, error) {
//	var rows *sql.Rows
//	var rowDataString []string
//	rows, err := db.Query(sqlstr)
//	if err != nil {
//		fmt.Println(err)
//	}
//	global.Wlog.Debug("GreatdbCheck exec sql: \"", sqlstr, "\" at the MySQL")
//	columns, err := rows.Columns()
//	if err != nil {
//		global.Wlog.Error("GreatdbCheck exec sql fail. sql: ", sqlstr, "error info: ", err)
//		return "", err
//	}
//	valuePtrs := make([]interface{}, len(columns))
//	values := make([]interface{}, len(columns))
//	for rows.Next() {
//		var tmpaaS []string
//		for i := 0; i < len(columns); i++ {
//			valuePtrs[i] = &values[i]
//		}
//		rows.Scan(valuePtrs...)
//		for i := range columns {
//			var v interface{}
//			val := values[i]
//			b, ok := val.([]byte)
//			if ok {
//				v = string(b)
//			} else {
//				v = val
//			}
//			tmpaaS = append(tmpaaS, fmt.Sprintf("%v", v))
//		}
//		tmpaa := strings.Join(tmpaaS, "/*go actions columnData*/")
//		rowDataString = append(rowDataString, tmpaa)
//	}
//	rows.Close()
//	return strings.Join(rowDataString, "/*go actions rowData*/"), nil
//}
//
///*
//   该函数用于需要查询源目表端数据库校验块数据，查询数据生成带有greatdbCheck标识的数据块
//*/
//func (my *MySQLAllColumnStruct) GeneratingQueryCriteria(db *sql.DB) (string, error) {
//	var columnNameSeq []string
//	//查询该表的列名和列信息
//	var sqlStr string
//	//处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
//	for i := range my.tableColumn {
//		var tmpcolumnName string
//		tmpcolumnName = my.tableColumn[i]["columnName"]
//		if strings.ToUpper(my.tableColumn[i]["dataType"]) == "DATETIME" {
//			tmpcolumnName = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", my.tableColumn[i]["columnName"])
//		}
//		if strings.Contains(strings.ToUpper(my.tableColumn[i]["dataType"]), "TIMESTAMP") {
//			tmpcolumnName = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", my.tableColumn[i]["columnName"])
//		}
//		columnNameSeq = append(columnNameSeq, tmpcolumnName)
//	}
//	queryColumn := strings.Join(columnNameSeq, ",")
//	sqlStr = fmt.Sprintf("select %s from `%s`.`%s` where %s", queryColumn, my.schema, my.table, my.sqlwhere)
//	fmt.Println(sqlStr)
//	//rowData, err := my.performQueryConditions(db, sqlStr)
//	//if err != nil {
//	//	return "", err
//	//}
//	rowData := ""
//	return rowData, nil
//}
//
///*
//   该函数用于需要查询源目表端数据库校验块数据，查询数据生成带有greatdbCheck标识的数据块
//*/
//func (my *MySQLAllColumnStruct) GeneratingQuerySql() (string) {
//	var columnNameSeq []string
//	//处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
//	for i := range my.tableColumn {
//		var tmpcolumnName string
//		tmpcolumnName = my.tableColumn[i]["columnName"]
//		if strings.ToUpper(my.tableColumn[i]["dataType"]) == "DATETIME" {
//			tmpcolumnName = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", my.tableColumn[i]["columnName"])
//		}
//		if strings.Contains(strings.ToUpper(my.tableColumn[i]["dataType"]), "TIMESTAMP") {
//			tmpcolumnName = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", my.tableColumn[i]["columnName"])
//		}
//		columnNameSeq = append(columnNameSeq, tmpcolumnName)
//	}
//	queryColumn := strings.Join(columnNameSeq, ",")
//	return fmt.Sprintf("select %s from `%s`.`%s` where %s", queryColumn, my.schema, my.table, my.sqlwhere)
//}
//
//func (or *OracleAllColumnStruct) GeneratingQueryCriteria(db *sql.DB) (string, error) {
//	return "", nil
//}
//
//
//func (or *OracleAllColumnStruct) GeneratingQuerySql() (string) {
//	return ""
//}
//
//func (tqacs TableQueryAllColumnStruct) IndexColumnExec(dname, tname string, tableColumn []map[string]string, sqlwhere string, dbDevice string) TableQueryAllColumnInterface {
//	var tqaci TableQueryAllColumnInterface
//	if dbDevice == "mysql" {
//		tqaci = &MySQLAllColumnStruct{
//			schema:      dname,
//			table:       tname,
//			tableColumn: tableColumn,
//			sqlwhere:    sqlwhere,
//		}
//	}
//	if dbDevice == "oracle" {
//		tqaci = &OracleAllColumnStruct{}
//	}
//	return tqaci
//}
//func Tqacs() *TableQueryAllColumnStruct {
//	return &TableQueryAllColumnStruct{}
//}
