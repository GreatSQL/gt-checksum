package mysql

import (
	"database/sql"
	"fmt"
	"greatdbCheck/global"
	"strconv"
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

/*
   获取MySQL的database的列表信息，排除'information_Schema','performance_Schema','sys','mysql'
*/
func (my *QueryTable) DatabaseNameList(ignSchema string) []string {
	var sqlStr string
	var dbName []string
	excludeSchema := fmt.Sprintf("'information_Schema','performance_Schema','sys','mysql','%s'", ignSchema)
	if my.Schema == "*" {
		sqlStr = fmt.Sprintf("select Schema_NAME as databaseName from information_Schema.Schemata where Schema_name not in (%s);", excludeSchema)
	} else {
		my.Schema = strings.ReplaceAll(my.Schema, ",", "','")
		sqlStr = fmt.Sprintf("select Schema_NAME as databaseName from information_Schema.Schemata where Schema_name  in ('%s') and Schema_name not in (%s);", my.Schema, excludeSchema)
	}
	global.Wlog.Info("[check Schema] exec mysql sql info: ", sqlStr)
	rows, err := my.Db.Query(sqlStr)
	if err != nil {
		global.Wlog.Error("[check Schema] exec mysql sql fail. sql info: ", sqlStr, "error info: ", err)
	}
	tableData, err := rowDataDisposMap(rows, "Schema")
	if err == nil && len(tableData) > 0 {
		for i := range tableData {
			dbName = append(dbName, fmt.Sprintf("%v", tableData[i]["databaseName"]))
		}
	}
	defer rows.Close()
	return dbName
}

func (my *QueryTable) TableNameList(db *sql.DB) ([]map[string]interface{}, error) {
	var sqlStr string
	if my.Table == "*" {
		sqlStr = fmt.Sprintf("select table_Schema as databaseName,table_name as tableName from information_Schema.tables where TABLE_Schema in ('%s');", my.Schema)
	} else {
		sqlStr = fmt.Sprintf("select table_Schema as databaseName,table_name as tableName from information_Schema.tables where TABLE_Schema in ('%s') and TABLE_NAME in ('%s');", my.Schema, my.Table)
	}
	global.Wlog.Info("[check table] exec mysql sql info: ", sqlStr)
	rows, err1 := my.Db.Query(sqlStr)
	if err1 != nil {
		global.Wlog.Error("[check table] exec mysql sql fail. sql info: ", sqlStr, "error info: ", err1)
	}
	defer rows.Close()
	return rowDataDisposMap(rows, "Table")
}

func (my *QueryTable) TableColumnName(db *sql.DB) ([]map[string]interface{}, error) {
	strsql := fmt.Sprintf("select COLUMN_NAME as columnName from information_Schema.columns where TABLE_Schema='%s' and TABLE_NAME='%s' order by ORDINAL_POSITION;", my.Schema, my.Table)
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
func (my *QueryTable) TableAllColumn(db *sql.DB) ([]map[string]interface{}, error) {
	sqlStr := fmt.Sprintf("select COLUMN_NAME as columnName ,COLUMN_TYPE as dataType,ORDINAL_POSITION as columnSeq from information_Schema.columns where table_Schema= '%s' and table_name='%s' order by ORDINAL_POSITION;", my.Schema, my.Table)
	global.Wlog.Info("[check table index column data] exec mysql sql info: ", sqlStr)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		global.Wlog.Error("[check table index column data] exec mysql sql fail. sql info: ", sqlStr, "Error Info: ", err)
		return nil, err
	}
	defer sqlRows.Close()
	return rowDataDisposMap(sqlRows, "TableAllColumn")
}

func (my *QueryTable) Trigger(db *sql.DB) ([]map[string]interface{}, error) {
	sqlStr := fmt.Sprintf("select DEFINER,TRIGGER_NAME,ACTION_TIMING,EVENT_MANIPULATION,EVENT_OBJECT_SCHEMA,EVENT_OBJECT_TABLE,ACTION_ORIENTATION,ACTION_STATEMENT from INFORMATION_SCHEMA.TRIGGERS where TRIGGER_SCHEMA in ('%s') and EVENT_OBJECT_TABLE in ('%s');", my.Schema, my.Table)
	global.Wlog.Info("[check Trigger] exec mysql sql info: ", sqlStr)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		global.Wlog.Error("[check Trigger] exec mysql sql fail. sql info: ", sqlStr, "Error Info: ", err)
		return nil, err
	}
	return rowDataDisposMap(sqlRows, "Trigger")
}

var procP = func(inout []map[string]interface{}, event string) map[string]string {
	var tmpa = make(map[string]string)
	for _, v := range inout {
		ORDINAL_POSITIO, err1 := strconv.Atoi(fmt.Sprintf("%s", v["ORDINAL_POSITION"]))
		if err1 != nil {
			fmt.Println(err1)
		}
		SPECIFIC_NAME := fmt.Sprintf("%s", v["SPECIFIC_NAME"])
		PARAMETER_MODE := fmt.Sprintf("%s", v["PARAMETER_MODE"])
		if event == "Func" {
			PARAMETER_MODE = ""
		}
		if _, ok := tmpa["SPECIFIC_NAME"]; !ok && ORDINAL_POSITIO == 1 {
			tmpa[SPECIFIC_NAME] = fmt.Sprintf("%s %s %s", PARAMETER_MODE, v["PARAMETER_NAME"], v["DTD_IDENTIFIER"])
		} else if _, ok = tmpa[SPECIFIC_NAME]; ok && ORDINAL_POSITIO > 1 {
			if strings.Split(fmt.Sprintf("%s", tmpa[SPECIFIC_NAME]), " ")[0] == PARAMETER_MODE {
				tmpa[SPECIFIC_NAME] = fmt.Sprintf("%s ,%s %s", tmpa[SPECIFIC_NAME], v["PARAMETER_NAME"], v["DTD_IDENTIFIER"])
			} else {
				tmpa[SPECIFIC_NAME] = fmt.Sprintf("%s %s ,%s %s", PARAMETER_MODE, tmpa[SPECIFIC_NAME], v["PARAMETER_NAME"], v["DTD_IDENTIFIER"])
			}
		}
	}
	return tmpa
}
var procR = func(createProc []map[string]interface{}, tmpa map[string]string, event string) map[string]string {
	var tmpb = make(map[string]string)
	for _, v := range createProc {
		ROUTINE_DEFINITION := fmt.Sprintf("%s", v["ROUTINE_DEFINITION"])
		ROUTINE_NAME := fmt.Sprintf("%s", v["ROUTINE_NAME"])
		tmpb["DEFINER"] = fmt.Sprintf("%s", v["DEFINER"])
		user := strings.Split(fmt.Sprintf("%s", v["DEFINER"]), "@")[0]
		host := strings.Split(fmt.Sprintf("%s", v["DEFINER"]), "@")[1]
		if event == "Proc" {
			tmpb[ROUTINE_NAME] = fmt.Sprintf("delimiter $\nCREATE DEFINER='%s'@'%s' PROCEDURE %s(%s) %s$ \ndelimiter ;", user, host, ROUTINE_NAME, tmpa[ROUTINE_NAME], strings.ReplaceAll(ROUTINE_DEFINITION, "\n", ""))
		}
		if event == "Func" {
			tmpb[ROUTINE_NAME] = fmt.Sprintf("delimiter $\nCREATE DEFINER='%s'@'%s' FUNCTION %s(%s) %s$ \ndelimiter ;", user, host, ROUTINE_NAME, tmpa[ROUTINE_NAME], strings.ReplaceAll(ROUTINE_DEFINITION, "\n", ""))
		}
	}
	return tmpb
}

func (my *QueryTable) Proc(db *sql.DB) (map[string]string, error) {
	sqlStr := fmt.Sprintf("select SPECIFIC_SCHEMA,SPECIFIC_NAME,ORDINAL_POSITION,PARAMETER_MODE,PARAMETER_NAME,DTD_IDENTIFIER from information_schema.PARAMETERS where SPECIFIC_SCHEMA in ('%s') and ROUTINE_TYPE='PROCEDURE' order by ORDINAL_POSITION;", my.Schema)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		global.Wlog.Error("[check Proc] exec mysql sql fail. sql info: ", sqlStr, "Error Info: ", err)
		return nil, err
	}
	inout, err := rowDataDisposMap(sqlRows, "Proc")
	if err != nil {
		fmt.Println(err)
	}
	sqlStr = fmt.Sprintf("select ROUTINE_SCHEMA,ROUTINE_NAME,ROUTINE_DEFINITION,DEFINER from information_schema.ROUTINES where routine_schema in ('%s') and ROUTINE_TYPE='PROCEDURE';", my.Schema)
	global.Wlog.Info("[check Proc] exec mysql sql info: ", sqlStr)
	sqlRows, err = db.Query(sqlStr)
	createProc, err := rowDataDisposMap(sqlRows, "Proc")
	if err != nil {
		fmt.Println(err)
	}
	return procR(createProc, procP(inout, "Proc"), "Proc"), nil
}

func (my *QueryTable) Func(db *sql.DB) (map[string]string, error) {
	var tmpb = make(map[string]string)
	sqlStr := fmt.Sprintf("select DEFINER,ROUTINE_NAME from information_schema.ROUTINES where routine_schema in ('%s') and ROUTINE_TYPE='FUNCTION';", my.Schema)
	global.Wlog.Info("[check Func] exec mysql sql info: ", sqlStr)
	sqlRows, err := db.Query(sqlStr)
	routineName, err := rowDataDisposMap(sqlRows, "Func")
	if err != nil {
		fmt.Println(err)
	}
	for _, v := range routineName {
		sqlStr = fmt.Sprintf("SHOW CREATE FUNCTION %s.%s;", my.Schema, v["ROUTINE_NAME"])
		global.Wlog.Info("[check Func] exec mysql sql info: ", sqlStr)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			return tmpb, err
		}
		createFunc, err1 := rowDataDisposMap(sqlRows, "Func")
		if err1 != nil {
			fmt.Println(err1)
		}
		for _, b := range createFunc {
			tmpb[fmt.Sprintf("%s", v["ROUTINE_NAME"])] = fmt.Sprintf("%s/*proc*/delimiter $\n%s$\ndelimiter ;\n", v["DEFINER"], b["Create Function"])
		}
	}
	return tmpb, nil
}
func (my *QueryTable) Foreign(db *sql.DB) (map[string]string, error) {
	var (
		routineNameM = make(map[string]int)
		tmpb         = make(map[string]string)
	)
	sqlStr := fmt.Sprintf("select CONSTRAINT_SCHEMA,TABLE_NAME from information_schema.referential_constraints where CONSTRAINT_SCHEMA in ('%s') and TABLE_NAME in ('%s');", my.Schema, my.Table)
	global.Wlog.Info("[check Func] exec mysql sql info: ", sqlStr)
	sqlRows, err := db.Query(sqlStr)
	foreignName, err := rowDataDisposMap(sqlRows, "Foreign")
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range foreignName {
		routineNameM[fmt.Sprintf("%s.%s", v["CONSTRAINT_SCHEMA"], v["TABLE_NAME"])]++
	}
	for k, _ := range routineNameM {
		sqlStr = fmt.Sprintf("SHOW CREATE TABLE %s;", k)
		global.Wlog.Info("[check Foreign] exec mysql sql info: ", sqlStr)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			tmpb[k] = ""
			return tmpb, err
		}
		createForeign, err1 := rowDataDisposMap(sqlRows, "Foreign")
		if err1 != nil {
			fmt.Println(err1)
		}
		for _, b := range createForeign {
			tmpb[k] = fmt.Sprintf("%s", b["Create Table"])
		}
	}
	return tmpb, nil
}
func (my *QueryTable) Partitions(db *sql.DB) (map[string]string, error) {
	var (
		routineNameM = make(map[string]int)
		tmpb         = make(map[string]string)
	)
	sqlStr := fmt.Sprintf("select TABLE_SCHEMA,TABLE_NAME from information_schema.partitions where table_schema in ('%s') and TABLE_NAME in ('%s') and PARTITION_NAME <> '';", my.Schema, my.Table)
	global.Wlog.Info("[check Partitions] exec mysql sql info: ", sqlStr)
	sqlRows, err := db.Query(sqlStr)
	partitionsName, err := rowDataDisposMap(sqlRows, "Partitions")
	if err != nil {
		fmt.Println(err)
	}
	for _, v := range partitionsName {
		routineNameM[fmt.Sprintf("%s.%s", v["TABLE_SCHEMA"], v["TABLE_NAME"])]++
	}

	for k, _ := range routineNameM {
		sqlStr = fmt.Sprintf("SHOW CREATE TABLE %s;", k)
		global.Wlog.Info("[check Foreign] exec mysql sql info: ", sqlStr)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			tmpb[k] = ""
			return tmpb, err
		}
		createPartitions, err1 := rowDataDisposMap(sqlRows, "Partitions")
		if err1 != nil {
			fmt.Println(err1)
		}
		for _, b := range createPartitions {
			tmpb[k] = fmt.Sprintf("%s", b["Create Table"])
		}
	}
	return tmpb, nil
}

func (my *QueryTable) Struct(db *sql.DB) (map[string]string, error) {
	return nil, nil
}
