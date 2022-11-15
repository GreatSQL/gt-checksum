package oracle

import (
	"database/sql"
	"fmt"
	"greatdbCheck/global"
	"strings"
)

type QueryTable struct {
	Schema string
	Table  string
	Db     *sql.DB
}

var rowDataDisposMap = func(sqlRows *sql.Rows, event string) ([]map[string]interface{}, error) {
	// 获取列名
	columns, err := sqlRows.Columns()
	if err != nil {
		errInfo := fmt.Sprintf("[check %s] exec mysql sql fail. Error Info: ", event, err)
		global.Wlog.Error(errInfo)
		return nil, err
	}
	// 定义一个切片，长度是字段的个数，切片里面的元素类型是sql.RawBytes
	//values := make([]sql.RawBytes,len(columns))
	//定义一个切片，元素类型是interface{}接口
	//scanArgs := make([]interface{},len(values))
	valuePtrs := make([]interface{}, len(columns))
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, len(columns))
	for sqlRows.Next() {
		for i := 0; i < len(columns); i++ {
			valuePtrs[i] = &values[i]
		}
		sqlRows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
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
		tableData = append(tableData, entry)
	}
	return tableData, nil
}

func (or *QueryTable) DatabaseNameList(ignschema string) []string {
	var sqlStr string
	var dbName []string
	excludeSchema := fmt.Sprintf("'SYS','OUTLN','SYSTEM','DBSNMP','APPQOSSYS','WMSYS','EXFSYS','CTXSYS','XDB','ORDDATA','ORDSYS','MDSYS','OLAPSYS','SYSMAN','FLOWS_FILES','APEX_030200','OWBSYS','SCOTT','HR','OE','SH','IX','PM','%s'", strings.ToUpper(ignschema))
	if or.Schema == "*" {
		sqlStr = fmt.Sprintf("select distinct OWNER as \"databaseName\" from all_tables where owner not in (%s)", excludeSchema)
	} else {
		or.Schema = strings.ReplaceAll(or.Schema, ",", "','")
		sqlStr = fmt.Sprintf("select distinct OWNER as \"databaseName\" from all_tables where owner in (%s) and owner not in (%s)", or.Schema, excludeSchema)
	}
	global.Wlog.Info("[check Schema] exec mysql sql info: ", sqlStr)
	rows, err := or.Db.Query(sqlStr)
	if err != nil {
		global.Wlog.Error("[check Schema] exec mysql sql fail. sql info: ", sqlStr, "error info: ", err)
	}
	tableData, err := rowDataDisposMap(rows, "Schema")
	if err == nil && len(tableData) > 0 {
		for i := range tableData {
			dbName = append(dbName, strings.ToUpper(fmt.Sprintf("%v", tableData[i]["databaseName"])))
		}
	}
	defer rows.Close()
	return dbName
	return []string{}
}

func (or *QueryTable) TableNameList(db *sql.DB) ([]map[string]interface{}, error) {
	var sqlStr string
	if or.Table == "*" {
		sqlStr = fmt.Sprintf("SELECT owner as \"databaseName\",table_name as \"tableName\" FROM DBA_TABLES WHERE OWNER='%s'", or.Schema)
	} else {
		sqlStr = fmt.Sprintf("SELECT owner as \"databaseName\",table_name as \"tableName\" FROM DBA_TABLES WHERE OWNER='%s' and table_name = '%s'", or.Schema, or.Table)
	}
	global.Wlog.Info("[check table] exec mysql sql info: ", sqlStr)
	rows, err1 := or.Db.Query(sqlStr)
	if err1 != nil {
		global.Wlog.Error("[check table] exec mysql sql fail. sql info: ", sqlStr, "error info: ", err1)
	}
	defer rows.Close()
	return rowDataDisposMap(rows, "Table")
}

func (or *QueryTable) TableColumnName(db *sql.DB) ([]map[string]interface{}, error) {
	strsql := fmt.Sprintf("select column_name as \"columnName\" from all_tab_columns where owner='%s' and table_name='%s' order by 'column_id'", strings.ToUpper(or.Schema), or.Table)
	global.Wlog.Info("[check table column name] dbexec mysql sql info: ", strsql)
	rows, err := db.Query(strsql)
	if err != nil {
		global.Wlog.Error("[check table column name] exec mysql sql fail. sql info: ", strsql, "error info: ", err)
	}
	tableData, err := rowDataDisposMap(rows, "Column")
	defer rows.Close()
	return tableData, err
}

/*
	获取校验表的列信息，包含列名，列序号，列类型
*/
func (or *QueryTable) TableAllColumn(db *sql.DB) ([]map[string]interface{}, error) {
	//sqlStr := fmt.Sprintf("select column_name as \"columnName\",data_type as \"dataType\",COLUMN_id as \"columnSeq\" from all_tab_columns where owner=\"%s\" and table_name=\"%s\" order by 'column_id'", strings.ToUpper(or.Schema), or.Table)
	sqlStr := fmt.Sprintf("SELECT column_name as \"columnName\",case when data_type='NUMBER' AND DATA_PRECISION is null THEN DATA_TYPE when data_type='NUMBER' AND DATA_PRECISION is not null then DATA_TYPE || '(' || DATA_PRECISION || ',' || NVL(DATA_SCALE,0) || ')' when data_type='VARCHAR2' THEN DATA_TYPE||'('||DATA_LENGTH||')' ELSE DATA_TYPE END AS \"dataType\",COLUMN_id as \"columnSeq\" FROM all_tab_columns WHERE owner='%s' and TABLE_NAME = '%s' order by 'column_id'", strings.ToUpper(or.Schema), or.Table)
	global.Wlog.Info("[check table index column data] exec mysql sql info: ", sqlStr)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		global.Wlog.Error("[check table index column data] exec mysql sql fail. sql info: ", sqlStr, "Error Info: ", err)
		return nil, err
	}
	defer sqlRows.Close()
	return rowDataDisposMap(sqlRows, "TableAllColumn")
}

func (or *QueryTable) Trigger(db *sql.DB) ([]map[string]interface{}, error) {
	return nil, nil
}
func (or *QueryTable) Proc(db *sql.DB) (map[string]string, error) {
	return nil, nil
}
func (or *QueryTable) Func(db *sql.DB) (map[string]string, error) {
	return nil, nil
}
func (or *QueryTable) Foreign(db *sql.DB) (map[string]string, error) {
	return nil, nil
}
func (or *QueryTable) Partitions(db *sql.DB) (map[string]string, error) {
	return nil, nil
}
func (or *QueryTable) Struct(db *sql.DB) (map[string]string, error) {
	return nil, nil
}
