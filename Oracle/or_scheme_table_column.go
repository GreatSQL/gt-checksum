package oracle

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"strings"
)

type QueryTable struct {
	Schema              string
	Table               string
	Db                  *sql.DB
	Datafix             string
	LowerCaseTableNames string
	TmpTableFileName    string
	ColumnName          []string
	ChanrowCount        int
	TableColumn         []map[string]string
	Sqlwhere            string
	ColData             []map[string]string
	BeginSeq            string
	RowDataCh           int64
	SelectColumn        map[string]string
}

var (
	DBType = "Oracle"
	vlog   string
	err    error
	strsql string
)

/*
   Oracle 获取对应的库表信息，排除'SYS','OUTLN','SYSTEM','DBSNMP','APPQOSSYS','WMSYS','EXFSYS','CTXSYS','XDB','ORDDATA','ORDSYS','MDSYS','OLAPSYS','SYSMAN','FLOWS_FILES','APEX_030200','OWBSYS','SCOTT','HR','OE','SH','IX','PM'
*/
func (or *QueryTable) DatabaseNameList(db *sql.DB, logThreadSeq int64) (map[string]int, error) {
	var (
		excludeSchema string
		A             = make(map[string]int)
		Event         = "Q_Schema_Table_List"
	)
	excludeSchema = fmt.Sprintf("'SYS','OUTLN','SYSTEM','DBSNMP','APPQOSSYS','WMSYS','EXFSYS','CTXSYS','XDB','ORDDATA','ORDSYS','MDSYS','OLAPSYS','SYSMAN','FLOWS_FILES','APEX_030200','OWBSYS','HR','OE','SH','IX','PM'")
	vlog = fmt.Sprintf("(%d) [%s] Start to query the metadata of the %s database and obtain library and table information.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("SELECT owner as \"databaseName\",table_name as \"tableName\" FROM DBA_TABLES WHERE OWNER not in (%s)", excludeSchema)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	rows, err := dispos.DBSQLforExec(strsql)
	if err != nil {
		return nil, err
	}
	dispos.SqlRows = rows
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for i := range tableData {
		var ga string
		gd, gt := fmt.Sprintf("%v", tableData[i]["databaseName"]), fmt.Sprintf("%v", tableData[i]["tableName"])
		if or.LowerCaseTableNames == "no" {
			gd = strings.ToUpper(gd)
			gt = strings.ToUpper(gt)
		}
		ga = fmt.Sprintf("%v/*schema&table*/%v", gd, gt)
		A[ga]++
	}
	vlog = fmt.Sprintf("(%d) [%s] Complete the library and table information query of the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	defer rows.Close()
	return A, nil
}

/*
	Oracle 通过查询表的元数据信息获取列名
*/
func (or *QueryTable) TableColumnName(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var (
		Event = "Q_table_columns"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start querying the metadata information of table %s.%s in the %s database and get all the column names", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select column_name as \"columnName\" from all_tab_columns where owner='%s' and table_name='%s' order by 'column_id'", strings.ToUpper(or.Schema), or.Table)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		if err != nil {
			return nil, err
		}
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	vlog = fmt.Sprintf("(%d) [%s] Complete the acquisition of all column names in the following table %s.%s of the %s database.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tableData, err
}

/*
	Oracle 查看当前用户是否有全局变量
*/
func (or *QueryTable) GlobalAccessPri(db *sql.DB, logThreadSeq int64) (bool, error) {
	var (
		globalPri     = make(map[string]int)
		Event         = "Q_Table_Global_Access_Pri"
		globalDynamic []map[string]interface{}
	)
	vlog = fmt.Sprintf("(%d) [%s] The permissions that the current %s DB needs to check is message {%v}, to check it...", logThreadSeq, Event, DBType, globalPri)
	global.Wlog.Debug(vlog)
	globalPri["SELECT ANY DICTIONARY"] = 0
	//vlog = fmt.Sprintf("(%d) The permissions that the current Oracle DB needs to check is message {%v}, to check it...", logThreadSeq, globalPri)
	//global.Wlog.Debug(vlog)

	var globalPriS []string
	for k, _ := range globalPri {
		globalPriS = append(globalPriS, k)
	}
	//查找全局权限 类似于grant all privileges on *.* 或 grant select on *.*
	vlog = fmt.Sprintf("(%d) [%s] Query the current %s DB global dynamic grants permission, to query it...", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select PRIVILEGE as \"privileges\" from user_sys_privs where PRIVILEGE IN ('%s')", strings.Join(globalPriS, "','"))
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return false, err
	}
	if globalDynamic, err = dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{}); err != nil {
		return false, err
	}
	//权限缺失列表
	if len(globalDynamic) == 0 {
		strsql = fmt.Sprintf("SELECT PRIVILEGE as \"privileges\" FROM ROLE_SYS_PRIVS WHERE PRIVILEGE IN ('%s') group by PRIVILEGE", strings.Join(globalPriS, "','"))
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			return false, err
		}
		if globalDynamic, err = dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{}); err != nil {
			return false, err
		}
	}
	for _, gd := range globalDynamic {
		if _, ok := globalPri[strings.ToUpper(fmt.Sprintf("%s", gd["privileges"]))]; ok {
			delete(globalPri, strings.ToUpper(fmt.Sprintf("%s", gd["privileges"])))
		}
	}
	if len(globalPri) == 0 {
		vlog = fmt.Sprintf("(%d) [%s] The current global access user with permission to connect to %s DB is normal and can be verified normally...", logThreadSeq, Event, DBType)
		global.Wlog.Debug(vlog)
		return true, nil
	}
	if _, ok := globalPri["SELECT ANY DICTIONARY"]; !ok {
		vlog = fmt.Sprintf("(%d) [%s] The current user connecting to %s DB lacks \"SELECT ANY DICTIONARY\" permission, Please authorize this permission...", logThreadSeq, Event, DBType)
		global.Wlog.Error(vlog)
		return false, nil
	}
	//if _, ok := globalPri["ALTER SYSTEM"]; ok {
	//	klog := fmt.Sprintf("(%d) The current user connecting to Oracle DB lacks \"ALTER SYSTEM\" permission, and the check table is empty", logThreadSeq)
	//	global.Wlog.Error(klog)
	//	return nil, nil
	//}
	return true, nil
}

/*
	Oracle 查询用户是否有表的查询权限
*/
func (or *QueryTable) TableAccessPriCheck(db *sql.DB, checkTableList []string, datefix string, logThreadSeq int64) (map[string]int, error) {
	var (
		globalPri, globalPriAllTab = make(map[string]int), make(map[string]int)
		newCheckTableList          = make(map[string]int)
		A                          = make(map[string]int)
		PT, abPT                   = make(map[string]int), make(map[string]int)
		Event                      = "Q_Table_Access_Pri"
		columnMerge                = func(AC []map[string]interface{}, F1 ...string) map[string][]string {
			var cc = make(map[string][]string)
			var N, E string
			var dd []string
			for _, C := range AC {
				if len(F1) == 1 {
					E = fmt.Sprintf("%s", F1)
				} else {
					E = fmt.Sprintf("%s", C[F1[0]])
				}
				if E != N {
					N = E
					dd = []string{}
					if len(F1) == 1 {
						dd = append(dd, strings.ToUpper(fmt.Sprintf("%s", C[F1[0]])))
					} else {
						dd = append(dd, strings.ToUpper(fmt.Sprintf("%s", C[F1[1]])))
					}
				} else {
					if len(F1) == 1 {
						dd = append(dd, strings.ToUpper(fmt.Sprintf("%s", C[F1[0]])))
					} else {
						dd = append(dd, strings.ToUpper(fmt.Sprintf("%s", C[F1[1]])))
					}
				}
				cc[N] = dd
			}
			return cc
		}
		priAllTableS []string
	)
	globalPri["SELECT"] = 0
	globalPriAllTab["SELECT ANY TABLE"] = 0
	if strings.ToUpper(datefix) == "TABLE" {
		globalPri["INSERT"] = 0
		globalPriAllTab["INSERT ANY TABLE"] = 0
		globalPri["DELETE"] = 0
		globalPriAllTab["DELETE ANY TABLE"] = 0
	}
	for k, _ := range globalPriAllTab {
		priAllTableS = append(priAllTableS, k)
	}
	vlog = fmt.Sprintf("(%d) [%s] The permissions that the current %s DB needs to check is message {%v},check table list is {%v}. to check it...", logThreadSeq, Event, DBType, globalPri, newCheckTableList)
	global.Wlog.Debug(vlog)

	//针对要校验的库做去重（库级别的）
	//校验库.表由切片改为map
	for _, AA := range checkTableList {
		newCheckTableList[strings.ToUpper(AA)]++
	}
	//vlog = fmt.Sprintf("(%d) The current Oracle DB needs to check the permission table is message {%v}, to check it...", logThreadSeq, newCheckTableList)
	//global.Wlog.Debug(vlog)
	//校验库做去重处理
	for _, aa := range checkTableList {
		A[strings.ToUpper(strings.Split(aa, ".")[0])]++
	}
	//vlog = fmt.Sprintf("(%d) The current Oracle DB needs to check the authority of the library is message {%v},to check it...", logThreadSeq, A)
	//global.Wlog.Debug(vlog)

	//处理oracle用户角色，该用户角色拥有insert、select 、delete any table的权限
	//查找全局权限 类似于grant all privileges on *.* 或 grant select on *.*
	vlog = fmt.Sprintf("(%d) [%s] Query the current %s DB global dynamic grants permission, to query it...", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("SELECT PRIVILEGE as \"privileges\" FROM ROLE_SYS_PRIVS WHERE PRIVILEGE IN ('%s') group by PRIVILEGE", strings.Join(priAllTableS, "','"))
	//globalDynamic, err := sqlQuery(logThreadSeq, strsql, "Global Dynamic Grants")
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	globalDynamic, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}

	//该用户有权限角色设置
	if len(globalDynamic) != 0 {
		A = globalPriAllTab
		CC := columnMerge(globalDynamic, "privileges")
		for _, v := range CC {
			var aaseq int
			for _, vi := range v {
				if _, ok := A[fmt.Sprintf("%s", vi)]; ok {
					aaseq++
				}
			}
			if len(A) == aaseq {
				vlog = fmt.Sprintf("(%d) [%s] The %s DB table information that meets the permissions and needs to be verified is {%v}...", logThreadSeq, Event, DBType, newCheckTableList)
				global.Wlog.Debug(vlog)
				return newCheckTableList, nil
			}
		}
		//权限缺失列表
		for _, v := range CC {
			for _, vi := range v {
				if _, ok := A[fmt.Sprintf("%s", vi)]; ok {
					delete(globalPriAllTab, vi)
				}
			}
		}
	}
	//查询当前表的权限
	//类似于grant all privileges on pcms.a 或 grant select on pcms.a
	vlog = fmt.Sprintf("(%d) %s Query the current %s DB global dynamic table permission, to query it...", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	//遍历没有schema pri权限的剩余库
	var DM = make(map[string]int)
	for _, D := range checkTableList {
		DM[strings.ToUpper(D)]++
	}
	strsql = fmt.Sprintf("select owner||'.'||table_name AS \"tablesName\",PRIVILEGE as \"privileges\" from user_tab_privs")
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	tablePri, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	DD := columnMerge(tablePri, "tablesName", "privileges")
	for K, V := range DD {
		var aaaseq int
		if _, ok := DM[strings.ToUpper(K)]; ok {
			for _, vi := range V {
				if _, ok1 := globalPriAllTab[fmt.Sprintf("%s ANY TABLE", strings.ToUpper(vi))]; ok1 {
					aaaseq++
				}
			}
		}
		if aaaseq == len(globalPriAllTab) {
			PT[K]++
		} else {
			abPT[K]++
		}
	}
	if len(PT) == 0 {
		vlog = fmt.Sprintf("(%d) [%s] The current table %s.%s in %s DB lacks some rights restrictions, please check the rights related to the table %s.%s {%v}", logThreadSeq, Event, or.Schema, or.Table, DBType, or.Schema, or.Table, globalPri)
		global.Wlog.Error(vlog)
		return nil, nil
	}
	vlog = fmt.Sprintf("(%d) [%s] The %s DB table information that needs to be verified to meet the permissions is {%v}, and the information that is not satisfied is {%v}...", logThreadSeq, Event, DBType, PT, abPT)
	global.Wlog.Debug(vlog)
	return PT, nil
}

/*
	Oracle 获取校验表的列信息，包含列名，列序号，列类型
*/
func (or *QueryTable) TableAllColumn(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var (
		Event = "Q_Table_Column_Metadata"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the metadata of all the columns of table %s.%s in the %s database", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("SELECT column_name as \"columnName\",case when data_type='NUMBER' AND DATA_PRECISION is null THEN DATA_TYPE when data_type='NUMBER' AND DATA_PRECISION is not null then DATA_TYPE || '(' || DATA_PRECISION || ',' || NVL(DATA_SCALE,0) || ')' when data_type='VARCHAR2' THEN DATA_TYPE||'('||DATA_LENGTH||')' ELSE DATA_TYPE END AS \"dataType\",COLUMN_id as \"columnSeq\",NULLABLE as \"isNull\" FROM all_tab_columns WHERE owner='%s' and TABLE_NAME = '%s' order by 'column_id'", strings.ToUpper(or.Schema), or.Table)

	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	vlog = fmt.Sprintf("(%d) [%s] Complete the metadata query of all columns in table %s.%s in the %s database.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tableData, err
}

/*
	Oracle 处理唯一索引索引（包含主键索引）
*/
func (or *QueryTable) keyChoiceDispos(IndexColumnMap map[string][]string, indexType string) map[string][]string {
	var (
		breakIndexColumnType = []string{"NUMBER", "INTEGER", "SMALLINT", "DECIMAL", "BINARY_FLOAT", "BINARY_DOUBLE", "FLOAT", "INT", "CHAR", "VARCHAR2", "YEAR", "DATE", "TIME"}
		a, c                 = make(map[string][]string), make(map[string][]int)
		indexChoice          = make(map[string][]string)
		tmpSliceNum          = 100
		tmpSliceNumMap       = make(map[string]int)
		z                    string
		choseSeq             = 1000000
		intCharMax           int
		indexChoisName       string
	)
	// ----- 处理唯一索引列，根据选择规则选择一个单列索引，（选择次序：int<--char<--year<--date<-time<-其他）
	//infoStr := fmt.Sprintf("Greatdbcheck Checks whether table %s.%s has a unique key index", or.Schema, or.Table)
	//global.Wlog.Debug(infoStr)
	//先找出唯一联合索引数量最少的
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

/*
	Oracle 表的索引选择
*/
func (or *QueryTable) TableIndexChoice(queryData []map[string]interface{}, logThreadSeq int64) map[string][]string {
	var (
		indexChoice                           = make(map[string][]string)
		nultiseriateIndexColumnMap            = make(map[string][]string)
		multiseriateIndexColumnMap            = make(map[string][]string)
		PriIndexCol, uniIndexCol, mulIndexCol []string
		indexName                             string
		Event                                 = "Q_Table_Index_Choice"
	)
	if len(queryData) == 0 {
		return nil
	}
	//索引列处理，联合索引进行列合并
	//去除主键索引列、唯一索引列、普通索引列的所有列明
	vlog = fmt.Sprintf("(%d) [%s] Start to select the appropriate index column in the following table %s.%s of the %s database.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
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
	//vlog = fmt.Sprintf("(%d) Oracle DB index merge processing complete. The index merged data is {primary key: %v,unique key: %v,nounique key: %v}", logThreadSeq, PriIndexCol, nultiseriateIndexColumnMap, multiseriateIndexColumnMap)
	//global.Wlog.Debug(vlog)
	//处理主键索引列
	//判断是否存在主键索引,每个表的索引只有一个
	//vlog = fmt.Sprintf("(%d) MySQL DB primary key index starts to choose the best.", logThreadSeq)
	//global.Wlog.Debug(vlog)
	if len(PriIndexCol) == 1 { //单列主键索引
		indexChoice["pri_single"] = PriIndexCol
	} else if len(PriIndexCol) > 1 { //联合主键索引
		indexChoice["pri_multiseriate"] = PriIndexCol
	}
	//vlog = fmt.Sprintf("(%d) MySQL DB unique key index starts to choose the best.", logThreadSeq)
	//global.Wlog.Debug(vlog)
	g := or.keyChoiceDispos(nultiseriateIndexColumnMap, "uni")
	for k, v := range g {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	//vlog = fmt.Sprintf("(%d) MySQL DB nounique key index starts to choose the best.", logThreadSeq)
	//global.Wlog.Debug(vlog)
	f := or.keyChoiceDispos(multiseriateIndexColumnMap, "mui")
	for k, v := range f {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	vlog = fmt.Sprintf("(%s) [%s] Complete the selection of the appropriate index column in the following table %s.%s of the %s database.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	return indexChoice
}

/*
	Oracle 查询触发器信息
*/
func (or *QueryTable) Trigger(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		tmpb = make(map[string]string)
		//sqlStr                string
		triggerOn, triggerTRX string
		Event                 = "Q_Table_Index_Choice"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the trigger information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select TRIGGER_name as triggerName,TABLE_NAME as tableName from all_triggers where owner = '%s'", or.Schema)
	//vlog = fmt.Sprintf("(%d) Oracle DB query table query Trigger info exec sql is {%s}", logThreadSeq, sqlStr)
	//global.Wlog.Debug(vlog)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	triggerName, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for _, v := range triggerName {
		strsql = fmt.Sprintf(" SELECT DBMS_METADATA.GET_DDL('TRIGGER','%s','%s') AS CREATE_TRIGGER FROM DUAL", v["TRIGGERNAME"], or.Schema)
		//vlog = fmt.Sprintf("(%d) Oracle DB query create Trigger databases %s info, exec sql is {%s}", logThreadSeq, or.Schema, sqlStr)
		//global.Wlog.Debug(vlog)
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			return nil, err
		}
		createTrigger, err1 := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err1 != nil {
			return nil, err
		}

		for _, b := range createTrigger {
			c := strings.TrimSpace(strings.ReplaceAll(fmt.Sprintf("%s", b["CREATE_TRIGGER"]), "\n", " "))
			d := strings.Join(strings.Fields(c), " ")
			//获取trigger Name
			onI := strings.Index(d, " ON ")
			triNbeg, triNend := strings.Index(d[:onI], "\""), strings.LastIndexAny(d[:onI], "\"")
			triggerN := d[triNbeg : triNend+1]
			//获取trigger action
			triggerAction := strings.TrimSpace(d[triNend+1 : onI])
			//获取trigger 作用的表
			if strings.Contains(d, "BEGIN") && strings.Contains(d, "END") {
				e := d[onI+1 : strings.Index(d, "BEGIN")]
				triActionTend := strings.LastIndexAny(e, "\"")
				triActionTbeg := strings.Index(e, "\"")
				triggerOn = strings.ReplaceAll(strings.ToUpper(e[triActionTbeg:triActionTend+1]), "\"", "")

				//触发器结构体  begin --> commit
				begst := strings.Index(d, "BEGIN")
				end := strings.Index(d, "END")
				triggerTRX = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(strings.ToUpper(d[begst:end]), "COMMIT", ""), ";", ""))
				triggerTRX = strings.ReplaceAll(triggerTRX, "\"", "")
			}
			tmpb[triggerN] = fmt.Sprintf("%s %s %s", triggerAction, triggerOn, triggerTRX)
		}
		//vlog = fmt.Sprintf("(%d) Oracle DB query databases %s Trigger data completion...", logThreadSeq, or.Schema)
		//global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%s) [%s] Complete the trigger information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tmpb, nil
}

/*
	存储过程校验
*/
func (or *QueryTable) Proc(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		tmpb  = make(map[string]string)
		Event = "Q_Proc"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the stored procedure information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf(" select object_name as ROUTINE_NAME from all_procedures where object_type='PROCEDURE' and owner = '%s'", or.Schema)
	//vlog = fmt.Sprintf("(%d) Oracle DB query table query Stored Procedure info exec sql is {%s}", logThreadSeq, sqlStr)
	//global.Wlog.Debug(vlog)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	routineName, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for _, v := range routineName {
		strsql = fmt.Sprintf(" SELECT DBMS_METADATA.GET_DDL('PROCEDURE','%s','%s') AS CREATE_PROCEDURE FROM DUAL", v["ROUTINE_NAME"], or.Schema)
		//vlog = fmt.Sprintf("(%d) Oracle DB query table create Stored Procedure info exec sql is {%s}", logThreadSeq, sqlStr)
		//global.Wlog.Debug(vlog)

		//vlog = fmt.Sprintf("(%d) Oracle DB query databases %s dispos Stored Procedure data info. to dispos it ...", logThreadSeq, or.Schema)
		//global.Wlog.Debug(vlog)
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			return nil, err
		}
		createFunc, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err != nil {
			return nil, err
		}
		for _, b := range createFunc {
			tmpb[strings.ToUpper(fmt.Sprintf("%s", v["ROUTINE_NAME"]))] = strings.ReplaceAll(fmt.Sprintf("%s", b["CREATE_PROCEDURE"]), "\n", "")
		}
		vlog = fmt.Sprintf("(%d) Oracle DB query databases %s Stored Procedure data completion...", logThreadSeq, or.Schema)
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) [%s] Complete the stored procedure information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tmpb, nil
}

/*
	Oracle 存储函数或自定义函数校验
*/
func (or *QueryTable) Func(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		//sqlStr string
		//vlog   string
		tmpb  = make(map[string]string)
		Event = "Q_Proc"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the stored Func information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select OBJECT_NAME as ROUTINE_NAME  from all_procedures where object_type='FUNCTION' and owner = '%s'", or.Schema)
	//vlog = fmt.Sprintf("(%d) Oracle DB query table query Stored Function info exec sql is {%s}", logThreadSeq, sqlStr)
	//global.Wlog.Debug(vlog)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	routineName, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for _, v := range routineName {
		strsql = fmt.Sprintf(" SELECT DBMS_METADATA.GET_DDL('FUNCTION','%s','%s') AS CREATE_FUNCTION FROM DUAL", v["ROUTINE_NAME"], or.Schema)
		//vlog = fmt.Sprintf("(%d) Oracle DB query create Stored Function databases %s info, exec sql is {%s}", logThreadSeq, or.Schema, sqlStr)
		//global.Wlog.Debug(vlog)
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			return nil, err
		}
		createFunc, err1 := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err1 != nil {
			return nil, err1
		}
		for _, b := range createFunc {
			d := strings.Join(strings.Fields(strings.ReplaceAll(fmt.Sprintf("%s", b["CREATE_FUNCTION"]), "\n", " ")), " ")
			if strings.Contains(strings.ToUpper(d), "BEGIN") && strings.Contains(strings.ToUpper(d), "END") {
				strings.Index(d, "BEGIN")
			}
			tmpb[strings.ToUpper(fmt.Sprintf("%s", v["ROUTINE_NAME"]))] = strings.ReplaceAll(fmt.Sprintf("%s", b["CREATE_FUNCTION"]), "\n", "")
		}
		//vlog = fmt.Sprintf("(%d) Oracle DB query databases %s Stored Function data completion...", logThreadSeq, or.Schema)
		//global.Wlog.Debug(vlog)
	}
	defer dispos.SqlRows.Close()
	vlog = fmt.Sprintf("(%d) [%s] Complete the stored Func information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	return tmpb, nil
}

/*
	Oracle 外键检查
*/
func (or *QueryTable) Foreign(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		routineNameM = make(map[string]int)
		tmpb         = make(map[string]string)
		Event        = "Q_Foreign"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the Foreign information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf(" select c.OWNER as DATABASE,c.table_name as TABLENAME, c.r_constraint_name,c.delete_rule,cc.column_name,cc.position from user_constraints c join user_cons_columns cc on c.constraint_name=cc.constraint_name and c.table_name=cc.table_name  where c.constraint_type='R' and c.validated='VALIDATED' and c.OWNER = '%s' and c.table_name='%s'", strings.ToUpper(or.Schema), or.Table)
	//vlog = fmt.Sprintf("(%d) Oracle DB query table query Foreign info exec sql is {%s}", logThreadSeq, sqlStr)
	//global.Wlog.Debug(vlog)
	//sqlRows, err := db.Query(sqlStr)
	//if err != nil {
	//	vlog = fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
	//	global.Wlog.Error(vlog)
	//	return nil, err
	//}
	//if sqlRows == nil {
	//	return nil, nil
	//}
	//foreignName, err := rowDataDisposMap(sqlRows, "Foreign", logThreadSeq)
	//if err != nil {
	//	vlog = fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
	//	global.Wlog.Error(vlog)
	//	return nil, err
	//}
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	foreignName, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for _, v := range foreignName {
		routineNameM[fmt.Sprintf("%s.%s", v["DATABASE"], v["TABLENAME"])]++
	}
	for k, _ := range routineNameM {
		schema, table := strings.Split(k, ".")[0], strings.Split(k, ".")[1]
		strsql = fmt.Sprintf("SELECT DBMS_METADATA.GET_DDL('TABLE','%s','%s') as CREATE_FOREIGN FROM DUAL", table, schema)
		//vlog = fmt.Sprintf("(%d) MySQL DB query create Foreign table %s.%s info, exec sql is {%s}", logThreadSeq, or.Schema, or.Table, sqlStr)
		//global.Wlog.Debug(vlog)
		//sqlRows, err = db.Query(sqlStr)
		//if err != nil {
		//	vlog = fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		//	global.Wlog.Error(vlog)
		//	tmpb[k] = ""
		//	return tmpb, err
		//}
		//if sqlRows == nil {
		//	return nil, nil
		//}
		//vlog = fmt.Sprintf("(%d) start dispos Oracle DB create table %s.%s create Foreign info.", logThreadSeq, or.Schema, or.Table)
		//global.Wlog.Debug(vlog)
		//createForeign, err1 := rowDataDisposMap(sqlRows, "Foreign", logThreadSeq)
		//if err1 != nil {
		//	return nil, err
		//}
		//vlog = fmt.Sprintf("(%d) Oracle DB query table %s.%s create Foreign completion.", logThreadSeq, or.Schema, or.Table)
		//global.Wlog.Debug(vlog)
		//vlog = fmt.Sprintf("(%d) Oracle DB query table %s.%s dispos Foreign data info. to dispos it ...", logThreadSeq, or.Schema, or.Table)
		//global.Wlog.Debug(vlog)
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			return nil, err
		}
		createForeign, err1 := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err1 != nil {
			return nil, err1
		}
		for _, b := range createForeign {
			var p, q, o string
			d := fmt.Sprintf("%s", b["CREATE_FOREIGN"])
			f := strings.Split(d, "\n")
			for _, g := range f {
				if strings.Contains(g, "CONSTRAINT") {
					p = strings.TrimSpace(g)
				}
				if strings.Contains(g, "REFERENCES") {
					q = strings.TrimSpace(g)
				}
			}
			if strings.Contains(p, "CONSTRAINT") && strings.Contains(p, "REFERENCES") {
				o = p
			}
			if strings.HasPrefix(q, "REFERENCES") {
				o = strings.ReplaceAll(fmt.Sprintf("%s %s", p, q), " ENABLE", "")
			}
			tmpb[k] = strings.ToUpper(strings.ReplaceAll(o, "\"", "!"))
		}
		//vlog = fmt.Sprintf("(%d) Oracle DB query table %s.%s Foreign data completion...", logThreadSeq, or.Schema, or.Table)
		//global.Wlog.Debug(vlog)
	}
	defer dispos.SqlRows.Close()
	vlog = fmt.Sprintf("(%d) [%s] Complete the Foreign information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	return tmpb, nil
}

/*
	分区表校验
*/
func (or *QueryTable) Partitions(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		routineNameM = make(map[string]int)
		tmpb         = make(map[string]string)
		Event        = "Q_Partitions"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the Partitions information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select OWNER,TABLE_NAME from all_tables  where owner='%s' and TABLE_NAME='%s' and partitioned='YES'", or.Schema, or.Table)
	//vlog = fmt.Sprintf("(%d) Oracle DB query table query partitions info exec sql is {%s}", logThreadSeq, sqlStr)
	//global.Wlog.Debug(vlog)
	//sqlRows, err := db.Query(sqlStr)
	//if err != nil {
	//	vlog = fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
	//	global.Wlog.Error(vlog)
	//	return nil, err
	//}
	//if sqlRows == nil {
	//	return nil, nil
	//}
	//vlog = fmt.Sprintf("(%d) start dispos Oracle DB query table %s.%s query Partitions info.", logThreadSeq, or.Schema, or.Table)
	//global.Wlog.Debug(vlog)
	//partitionsName, err := rowDataDisposMap(sqlRows, "Partitions", 10)
	//if err != nil {
	//	return nil, err
	//}
	//vlog = fmt.Sprintf("(%d) Oracle DB query table %s.%s query Partitions completion.", logThreadSeq, or.Schema, or.Table)
	//global.Wlog.Debug(vlog)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	partitionsName, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for _, v := range partitionsName {
		routineNameM[fmt.Sprintf("%s.%s", v["OWNER"], v["TABLE_NAME"])]++
	}
	for k, _ := range routineNameM {
		var zi string
		if strings.EqualFold(k, fmt.Sprintf("%s.%s", or.Schema, or.Table)) {
			zi = fmt.Sprintf("%s.%s", or.Schema, or.Table)
		} else {
			zi = k
		}
		schema, table := strings.Split(k, ".")[0], strings.Split(k, ".")[1]
		strsql = fmt.Sprintf("SELECT DBMS_METADATA.GET_DDL('TABLE','%s','%s') AS CREATE_PARTITIONS FROM DUAL", table, schema)
		//vlog = fmt.Sprintf("(%d) Oracle DB query create partitions table %s.%s info, exec sql is {%s}", logThreadSeq, or.Schema, or.Table, sqlStr)
		//global.Wlog.Debug(vlog)
		//sqlRows, err = db.Query(sqlStr)
		//if err != nil {
		//	vlog = fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		//	global.Wlog.Error(vlog)
		//	tmpb[k] = ""
		//	return tmpb, err
		//}
		//if sqlRows == nil {
		//	return nil, nil
		//}
		//vlog = fmt.Sprintf("(%d) start dispos Oracle DB create table %s.%s create Partitions info.", logThreadSeq, or.Schema, or.Table)
		//global.Wlog.Debug(vlog)
		//createPartitions, err1 := rowDataDisposMap(sqlRows, "Partitions", logThreadSeq)
		//if err1 != nil {
		//	return nil, err1
		//}
		//vlog = fmt.Sprintf("(%d) Oracle DB query table %s.%s create Partitions completion.", logThreadSeq, or.Schema, or.Table)
		//global.Wlog.Debug(vlog)
		//vlog = fmt.Sprintf("(%d) Oracle DB query table %s.%s dispos Partitions data info. to dispos it ...", logThreadSeq, or.Schema, or.Table)
		//global.Wlog.Debug(vlog)
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			return nil, err
		}
		createPartitions, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err != nil {
			return nil, err
		}
		for _, b := range createPartitions {
			z := strings.Split(fmt.Sprintf("%s", b["CREATE_PARTITIONS"]), "\n")
			var a, c []string
			var partitionMode, partitionColumn string
			for _, bi := range z {
				if strings.Contains(bi, " PARTITION BY ") {
					ii := strings.TrimSpace(strings.ReplaceAll(bi, " PARTITION BY ", ""))
					partitionMode, partitionColumn = strings.Split(ii, " ")[0], strings.ReplaceAll(strings.Split(ii, " ")[1], "\"", "")
					c = append(c, fmt.Sprintf(" PARTITION BY %s %s", partitionMode, strings.ToUpper(partitionColumn)))
				}
				if strings.Contains(bi, "SUBPARTITION BY ") || strings.Contains(bi, "SUBPARTITIONS ") {
					c = append(c, strings.ToUpper(strings.TrimSpace(bi)))
				}
				if strings.Contains(bi, "PARTITION ") && strings.Contains(bi, "VALUES ") {
					bi = strings.ReplaceAll(bi, "\"", "")
					c = append(c, fmt.Sprintf(" %s,", strings.TrimSpace(bi)))
				}
				if strings.Contains(bi, "PARTITION \"") {
					c = append(c, bi)
				}
			}
			x := fmt.Sprintf("%s %s);", strings.Join(a, ""), strings.Join(c, "")[:len(strings.Join(c, ""))-1])
			xs := strings.Join(strings.Fields(x), " ")

			if strings.ToUpper(partitionMode) == "HASH" {
				ad := strings.Split(strings.ReplaceAll(strings.Split(xs, ") ")[1], "PARTITION ", ""), " ")
				xs = fmt.Sprintf("PARTITION BY %s %s PARTITIONS %d);", partitionMode, partitionColumn, len(ad))
			}
			tmpb[zi] = strings.ReplaceAll(xs, "\"", "!")
		}
		//vlog = fmt.Sprintf("(%d) Oracle DB query table %s.%s partitions data completion...", logThreadSeq, or.Schema, or.Table)
		//global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) [%s] Complete the Partitions information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	return tmpb, nil
}
func (or *QueryTable) Struct(db *sql.DB) (map[string]string, error) {
	return nil, nil
}
