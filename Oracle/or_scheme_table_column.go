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
		errInfo := fmt.Sprintf("[check %s] exec oracle sql fail. Error Info: ", event, err)
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

//处理唯一索引索引（包含主键索引）
func (or *QueryTable) keyChoiceDispos(IndexColumnMap map[string][]string, indexType string) map[string][]string {
	breakIndexColumnType := []string{"NUMBER", "INT", "CHAR", "VARCHAR2", "YEAR", "DATE", "TIME"}
	var a, c = make(map[string][]string), make(map[string][]int)
	var indexChoice = make(map[string][]string)
	// ----- 处理唯一索引列，根据选择规则选择一个单列索引，（选择次序：int<--char<--year<--date<-time<-其他）
	infoStr := fmt.Sprintf("Greatdbcheck Checks whether table %s.%s has a unique key index", or.Schema, or.Table)
	global.Wlog.Info(infoStr)
	var tmpSliceNum = 100
	var tmpSliceNumMap = make(map[string]int)
	//先找出唯一联合索引数量最少的
	var z string
	for k, i := range IndexColumnMap {
		if len(i) <= tmpSliceNum {
			if len(i) < tmpSliceNum {
				delete(tmpSliceNumMap, z)
			}
			tmpSliceNum = len(i)
			tmpSliceNumMap[k] = len(i)
			z = k
		}
	}
	//单列唯一索引处理，选择最短的且最合适的索引列（选择次序：int<--char<--year<--date<-time）
	var choseSeq int = 1000000
	for k, v := range tmpSliceNumMap {
		if v == 1 {
			d := strings.Split(strings.Join(IndexColumnMap[k], ""), " /*actions Column Type*/ ")
			indexColType := d[1]
			var e []string
			for kb, vb := range breakIndexColumnType {
				if strings.HasPrefix(strings.ToUpper(indexColType), vb) {
					if kb < choseSeq {
						indexChoice[fmt.Sprintf("%s_single", indexType)] = append(e, d[0])
					}
					choseSeq = kb
				}
			}
		}
		if v > 1 {
			var nultIndexColumnSlice, nultIndexColumnTypeSlice []string
			for _, vu := range IndexColumnMap[k] {
				e := strings.Split(vu, " /*actions Column Type*/ ")
				nultIndexColumnSlice = append(nultIndexColumnSlice, e[0])
				nultIndexColumnTypeSlice = append(nultIndexColumnTypeSlice, e[1])
			}
			tmpIntCount := strings.Count(strings.ToUpper(strings.Join(nultIndexColumnTypeSlice, ",")), "INT")
			tmpCharCount := strings.Count(strings.ToUpper(strings.Join(nultIndexColumnTypeSlice, ",")), "CHAR")
			//处理索引列数量相同的情况，计算每个索引列中包含的int和char数量
			c[k] = []int{tmpIntCount, tmpCharCount}
			a[k] = nultIndexColumnSlice
		}
	}

	var intCharMax int
	var indexChoisName string
	for k, v := range c {
		if v[0] > intCharMax {
			intCharMax = v[0]
			indexChoisName = k
		}
		if indexChoisName == "" && intCharMax == 0 && v[1] > 0 {
			intCharMax = v[0]
			indexChoisName = k
		}
		if v[0] == 0 && v[1] == 0 {
			indexChoisName = k
			break
		}
	}
	indexChoice[fmt.Sprintf("%s_multiseriate", indexType)] = a[indexChoisName]
	return indexChoice
}

func (or *QueryTable) TableIndexChoice(queryData []map[string]interface{}) map[string][]string {
	global.Wlog.Debug("actions init db Example.")
	var (
		indexChoice                           = make(map[string][]string)
		nultiseriateIndexColumnMap            = make(map[string][]string)
		multiseriateIndexColumnMap            = make(map[string][]string)
		PriIndexCol, uniIndexCol, mulIndexCol []string
		indexName                             string
	)
	//索引列处理，联合索引进行列合并
	//去除主键索引列、唯一索引列、普通索引列的所有列明
	for _, v := range queryData {
		if v["nonUnique"].(string) == "UNIQUE" {
			//处理主键索引
			if strings.HasPrefix(v["indexName"].(string), "SYS") {
				if v["indexName"].(string) != indexName {
					indexName = v["indexName"].(string)
				}
				PriIndexCol = append(PriIndexCol, fmt.Sprintf("%s", v["columnName"]))
			} else {
				//处理唯一索引
				if v["indexName"].(string) != indexName {
					indexName = v["indexName"].(string)
					nultiseriateIndexColumnMap[indexName] = append(uniIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
				} else {
					nultiseriateIndexColumnMap[indexName] = append(nultiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
				}
			}
		}
		//处理普通索引
		if v["nonUnique"].(string) == "NONUNIQUE" {
			if v["indexName"].(string) != indexName {
				indexName = v["indexName"].(string)
				multiseriateIndexColumnMap[indexName] = append(mulIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			} else {
				multiseriateIndexColumnMap[indexName] = append(multiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			}
		}
	}
	//处理主键索引列
	//判断是否存在主键索引,每个表的索引只有一个
	infoStr := fmt.Sprintf("Greatdbcheck Checks whether table %s.%s has a primary key index", or.Schema, or.Table)
	global.Wlog.Info(infoStr)
	if len(PriIndexCol) == 1 { //单列主键索引
		indexChoice["pri_single"] = PriIndexCol
	} else if len(PriIndexCol) > 1 { //联合主键索引
		indexChoice["pri_multiseriate"] = PriIndexCol
	}

	g := or.keyChoiceDispos(nultiseriateIndexColumnMap, "uni")
	for k, v := range g {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	f := or.keyChoiceDispos(multiseriateIndexColumnMap, "mui")
	for k, v := range f {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	return indexChoice
}

func (or *QueryTable) Trigger(db *sql.DB) (map[string]string, error) {
	var tmpb = make(map[string]string)
	sqlStr := fmt.Sprintf(" select TRIGGER_name as TRIGGER_NAME from all_triggers where owner = '%s' and TABLE_name = '%s'", or.Schema, or.Table)
	global.Wlog.Info("[check Trigger] exec oracle sql info: ", sqlStr)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		global.Wlog.Error("[check Trigger] exec oracle sql fail. sql info: ", sqlStr, "Error Info: ", err)
		return nil, err
	}
	triggerName, err := rowDataDisposMap(sqlRows, "Trigger")
	if err != nil {
		fmt.Println(err)
	}
	for _, v := range triggerName {
		sqlStr = fmt.Sprintf(" SELECT DBMS_METADATA.GET_DDL('TRIGGER','%s','%s') AS CREATE_TRIGGER FROM DUAL", v["TRIGGER_NAME"], or.Schema)
		global.Wlog.Info("[check Proc] exec oracle sql info: ", sqlStr)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			return tmpb, err
		}
		createFunc, err1 := rowDataDisposMap(sqlRows, "TRIGGER")
		if err1 != nil {
			fmt.Println(err1)
		}
		for _, b := range createFunc {
			tmpb[fmt.Sprintf("%s", v["TRIGGER_NAME"])] = strings.ReplaceAll(fmt.Sprintf("%s", b["CREATE_TRIGGER"]), "\n", "")
		}
	}
	return tmpb, nil
}
func (or *QueryTable) Proc(db *sql.DB) (map[string]string, error) {
	var tmpb = make(map[string]string)
	sqlStr := fmt.Sprintf(" select object_name as ROUTINE_NAME from all_procedures where object_type='PROCEDURE' and owner = '%s'", or.Schema)
	global.Wlog.Info("[check Proc] exec oracle sql info: ", sqlStr)
	sqlRows, err := db.Query(sqlStr)
	routineName, err := rowDataDisposMap(sqlRows, "Proc")
	if err != nil {
		fmt.Println(err)
	}
	for _, v := range routineName {
		sqlStr = fmt.Sprintf(" SELECT DBMS_METADATA.GET_DDL('PROCEDURE','%s','%s') AS CREATE_PROCEDURE FROM DUAL", v["ROUTINE_NAME"], or.Schema)
		global.Wlog.Info("[check Proc] exec oracle sql info: ", sqlStr)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			return tmpb, err
		}
		createFunc, err1 := rowDataDisposMap(sqlRows, "Proc")
		if err1 != nil {
			fmt.Println(err1)
		}
		for _, b := range createFunc {
			tmpb[fmt.Sprintf("%s", v["ROUTINE_NAME"])] = strings.ReplaceAll(fmt.Sprintf("%s", b["CREATE_PROCEDURE"]), "\n", "")
		}
	}
	return tmpb, nil
}
func (or *QueryTable) Func(db *sql.DB) (map[string]string, error) {
	var tmpb = make(map[string]string)
	sqlStr := fmt.Sprintf("select OBJECT_NAME as ROUTINE_NAME  from all_procedures where object_type='FUNCTION' and owner = '%s'", or.Schema)
	global.Wlog.Info("[check Func] exec oracle sql info: ", sqlStr)
	sqlRows, err := db.Query(sqlStr)
	routineName, err := rowDataDisposMap(sqlRows, "Func")
	if err != nil {
		fmt.Println(err)
	}
	for _, v := range routineName {
		sqlStr = fmt.Sprintf(" SELECT DBMS_METADATA.GET_DDL('FUNCTION','%s','%s') AS CREATE_FUNCTION FROM DUAL", v["ROUTINE_NAME"], or.Schema)
		global.Wlog.Info("[check Func] exec oracle sql info: ", sqlStr)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			return tmpb, err
		}
		createFunc, err1 := rowDataDisposMap(sqlRows, "Func")
		if err1 != nil {
			fmt.Println(err1)
		}
		for _, b := range createFunc {
			tmpb[fmt.Sprintf("%s", v["ROUTINE_NAME"])] = strings.ReplaceAll(fmt.Sprintf("%s", b["CREATE_FUNCTION"]), "\n", "")
		}
	}
	return tmpb, nil
}
func (or *QueryTable) Foreign(db *sql.DB) (map[string]string, error) {
	var (
		routineNameM = make(map[string]int)
		tmpb         = make(map[string]string)
	)
	sqlStr := fmt.Sprintf(" select c.OWNER as DATABASE,c.table_name as TABLENAME, c.r_constraint_name,c.delete_rule,cc.column_name,cc.position from user_constraints c join user_cons_columns cc on c.constraint_name=cc.constraint_name and c.table_name=cc.table_name  where c.constraint_type='R' and c.validated='VALIDATED' and c.OWNER = '%s' and c.table_name='%s'", strings.ToUpper(or.Schema), or.Table)
	global.Wlog.Info("[check Func] exec oracle sql info: ", sqlStr)
	sqlRows, err := db.Query(sqlStr)
	foreignName, err := rowDataDisposMap(sqlRows, "Foreign")
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range foreignName {
		routineNameM[fmt.Sprintf("%s.%s", v["DATABASE"], v["TABLENAME"])]++
	}
	for k, _ := range routineNameM {
		schema, table := strings.Split(k, ".")[0], strings.Split(k, ".")[1]
		sqlStr = fmt.Sprintf("SELECT DBMS_METADATA.GET_DDL('TABLE','%s','%s') as CREATE_Foreign FROM DUAL", table, schema)
		global.Wlog.Info("[check Foreign] exec oracle sql info: ", sqlStr)
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
			tmpb[k] = fmt.Sprintf("%s", b["CREATE_Foreign"])
		}
	}
	return tmpb, nil
}
func (or *QueryTable) Partitions(db *sql.DB) (map[string]string, error) {
	var (
		routineNameM = make(map[string]int)
		tmpb         = make(map[string]string)
	)
	sqlStr := fmt.Sprintf("select OWNER,TABLE_NAME from all_tables  where owner='%s' and TABLE_NAME='%s' and partitioned='YES'", or.Schema, or.Table)
	global.Wlog.Info("[check Partitions] exec mysql sql info: ", sqlStr)
	sqlRows, err := db.Query(sqlStr)
	partitionsName, err := rowDataDisposMap(sqlRows, "Partitions")
	if err != nil {
		fmt.Println(err)
	}
	for _, v := range partitionsName {
		routineNameM[fmt.Sprintf("%s.%s", v["OWNER"], v["TABLE_NAME"])]++
	}

	for k, _ := range routineNameM {
		schema, table := strings.Split(k, ".")[0], strings.Split(k, ".")[1]
		sqlStr = fmt.Sprintf("SELECT DBMS_METADATA.GET_DDL('TABLE','%s','%s') AS CREATE_PARTITIONS FROM DUAL", table, schema)
		global.Wlog.Info("[check Foreign] exec oracle sql info: ", sqlStr)
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
			tmpb[k] = fmt.Sprintf("%s", b["CREATE_PARTITIONS"])
		}
	}
	return tmpb, nil
}
func (or *QueryTable) Struct(db *sql.DB) (map[string]string, error) {
	return nil, nil
}
