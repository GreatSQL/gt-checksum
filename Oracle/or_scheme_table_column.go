package oracle

import (
	"database/sql"
	"fmt"
	"greatdbCheck/global"
	"strings"
)

type QueryTable struct {
	Schema  string
	Table   string
	Db      *sql.DB
	Datafix string
}

var rowDataDisposMap = func(sqlRows *sql.Rows, event string, seq int64) ([]map[string]interface{}, error) {
	// 获取列名
	columns, err := sqlRows.Columns()
	if err != nil {
		errInfo := fmt.Sprintf("(", seq, ") Oracle DB Get the column fail. Error Info: ", event, err)
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

func (or *QueryTable) DatabaseNameList(ignschema string, logThreadSeq int64) []string {
	var sqlStr, excludeSchema string
	var dbName []string
	excludeSchema = fmt.Sprintf("'SYS','OUTLN','SYSTEM','DBSNMP','APPQOSSYS','WMSYS','EXFSYS','CTXSYS','XDB','ORDDATA','ORDSYS','MDSYS','OLAPSYS','SYSMAN','FLOWS_FILES','APEX_030200','OWBSYS','SCOTT','HR','OE','SH','IX','PM'")
	alog := fmt.Sprintf("(%d) Oracle DB ignore sys database message is {%s}", logThreadSeq, excludeSchema)
	global.Wlog.Info(alog)
	if ignschema != "" {
		excludeSchema = fmt.Sprintf("'SYS','OUTLN','SYSTEM','DBSNMP','APPQOSSYS','WMSYS','EXFSYS','CTXSYS','XDB','ORDDATA','ORDSYS','MDSYS','OLAPSYS','SYSMAN','FLOWS_FILES','APEX_030200','OWBSYS','SCOTT','HR','OE','SH','IX','PM','%s'", strings.ToUpper(ignschema))
	}
	blog := fmt.Sprintf("(%d) Oracle DB ignore database message is {%s}", logThreadSeq, excludeSchema)
	global.Wlog.Info(blog)
	if or.Schema == "*" {
		sqlStr = fmt.Sprintf("select distinct OWNER as \"databaseName\" from all_tables where owner not in (%s)", excludeSchema)
	} else {
		or.Schema = strings.ReplaceAll(or.Schema, ",", "','")
		sqlStr = fmt.Sprintf("select distinct OWNER as \"databaseName\" from all_tables where owner in ('%s') and owner not in (%s)", or.Schema, excludeSchema)
	}
	clog := fmt.Sprintf("(%d) Oracle DB query database exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(clog)
	clog = fmt.Sprintf("(%d) Oracle DB begin exec sql.", logThreadSeq)
	global.Wlog.Info(clog)
	rows, err := or.Db.Query(sqlStr)
	if err != nil {
		dlog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s},Error info is {%s}", logThreadSeq, sqlStr, err)
		global.Wlog.Error(dlog)
	}
	elog := fmt.Sprintf("(%d) Oracle DB complete exec sql.", logThreadSeq)
	global.Wlog.Info(elog)
	tableData, err := rowDataDisposMap(rows, "Schema", logThreadSeq)
	if err == nil && len(tableData) > 0 {
		for i := range tableData {
			dbName = append(dbName, strings.ToUpper(fmt.Sprintf("%v", tableData[i]["databaseName"])))
		}
	}
	defer rows.Close()
	return dbName
	return []string{}
}

func (or *QueryTable) TableNameList(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var sqlStr string
	if or.Table == "*" {
		sqlStr = fmt.Sprintf("SELECT owner as \"databaseName\",table_name as \"tableName\" FROM DBA_TABLES WHERE OWNER='%s'", or.Schema)
	} else {
		sqlStr = fmt.Sprintf("SELECT owner as \"databaseName\",table_name as \"tableName\" FROM DBA_TABLES WHERE OWNER='%s' and table_name = '%s'", or.Schema, or.Table)
	}
	alog := fmt.Sprintf("(%d) Oracle DB query table metadata info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)
	rows, err1 := or.Db.Query(sqlStr)
	if err1 != nil {
		blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s}, Error info is {%s}.", logThreadSeq, sqlStr, err1)
		global.Wlog.Error(blog)
	}
	defer rows.Close()
	return rowDataDisposMap(rows, "Table", logThreadSeq)
}

func (or *QueryTable) TableColumnName(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	strsql := fmt.Sprintf("select column_name as \"columnName\" from all_tab_columns where owner='%s' and table_name='%s' order by 'column_id'", strings.ToUpper(or.Schema), or.Table)
	alog := fmt.Sprintf("(%d) Oracle DB query table metadata info exec sql is {%s}", logThreadSeq, strsql)
	global.Wlog.Info(alog)
	rows, err := db.Query(strsql)
	if err != nil {
		blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s},Error info is {%s}", logThreadSeq, strsql, err)
		global.Wlog.Error(blog)
	}
	clog := fmt.Sprintf("(%d) start dispos Oracle DB query table %s.%s metadata info.", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Info(clog)
	tableData, err := rowDataDisposMap(rows, "Column", logThreadSeq)
	zlog := fmt.Sprintf("(%d) Oracle DB query table %s.%s metadata data completion.", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Info(zlog)
	defer rows.Close()
	return tableData, err
}

/*
	检查访问视图的权限是否存在
*/
func (or *QueryTable) GlobalAccessPri(db *sql.DB, logThreadSeq int64) bool {
	var (
		globalPri = make(map[string]int)
		sqlQuery  = func(logseq int64, sql, logKeyword string) []map[string]interface{} {
			alog := fmt.Sprintf("(%d) Oracle DB query %s info exec sql is {%s}", logseq, logKeyword, sql)
			global.Wlog.Info(alog)
			rows, err := db.Query(sql)
			if err != nil {
				blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logseq, sql, err)
				global.Wlog.Error(blog)
			}
			clog := fmt.Sprintf("(%d) start dispos Oracle DB query %s.", logseq, logKeyword)
			global.Wlog.Info(clog)
			a, _ := rowDataDisposMap(rows, "Privileges", logseq)
			dlog := fmt.Sprintf("(%d) Oracle DB query %s data completion.", logseq, logKeyword)
			global.Wlog.Info(dlog)
			return a
		}
	)
	globalPri["SELECT ANY DICTIONARY"] = 0
	elog := fmt.Sprintf("(%d) The permissions that the current Oracle DB needs to check is message {%v}, to check it...", logThreadSeq, globalPri)
	global.Wlog.Info(elog)

	var globalPriS []string
	for k, _ := range globalPri {
		globalPriS = append(globalPriS, k)
	}
	//查找全局权限 类似于grant all privileges on *.* 或 grant select on *.*
	ilog := fmt.Sprintf("(%d) Query the current Oracle DB global dynamic grants permission, to query it...", logThreadSeq)
	global.Wlog.Info(ilog)
	//strsql := fmt.Sprintf("SELECT GRANTEE as \"role\",PRIVILEGE as \"privileges\" FROM DBA_SYS_PRIVS b,(select GRANTED_ROLE as \"role\" from user_role_privs group by GRANTED_ROLE) a where a.\"role\" =b.GRANTEE AND b.privilege in ('SELECT ANY DICTIONARY')")
	strsql := fmt.Sprintf("select PRIVILEGE as \"privileges\" from user_sys_privs where PRIVILEGE IN ('%s')", strings.Join(globalPriS, "','"))
	globalDynamic := sqlQuery(logThreadSeq, strsql, "Global Dynamic Grants")
	//权限缺失列表
	if len(globalDynamic) == 0 {
		strsql = fmt.Sprintf("SELECT PRIVILEGE as \"privileges\" FROM ROLE_SYS_PRIVS WHERE PRIVILEGE IN ('%s') group by PRIVILEGE", strings.Join(globalPriS, "','"))
		globalDynamic = sqlQuery(logThreadSeq, strsql, "Global Dynamic Grants")
	}
	for _, gd := range globalDynamic {
		if _, ok := globalPri[strings.ToUpper(fmt.Sprintf("%s", gd["privileges"]))]; ok {
			delete(globalPri, strings.ToUpper(fmt.Sprintf("%s", gd["privileges"])))
		}
	}
	if len(globalPri) == 0 {
		jlog := fmt.Sprintf("(%d) The current global access user with permission to connect to Oracle DB is normal and can be verified normally...", logThreadSeq)
		global.Wlog.Info(jlog)
		return true
	}
	if _, ok := globalPri["SELECT ANY DICTIONARY"]; ok {
		klog := fmt.Sprintf("(%d) The current user connecting to Oracle DB lacks \"SELECT ANY DICTIONARY\" permission, Please authorize this permission...", logThreadSeq)
		global.Wlog.Error(klog)
		return false
	}
	//if _, ok := globalPri["ALTER SYSTEM"]; ok {
	//	klog := fmt.Sprintf("(%d) The current user connecting to Oracle DB lacks \"ALTER SYSTEM\" permission, and the check table is empty", logThreadSeq)
	//	global.Wlog.Error(klog)
	//	return nil, nil
	//}
	return true
}
func (or *QueryTable) TableAccessPriCheck(db *sql.DB, checkTableList []string, datefix string, logThreadSeq int64) (map[string]int, error) {
	var (
		globalPri, globalPriAllTab = make(map[string]int), make(map[string]int)
		newCheckTableList          = make(map[string]int)
		A                          = make(map[string]int)
		PT, abPT                   = make(map[string]int), make(map[string]int)
		sqlQuery                   = func(logseq int64, sql, logKeyword string) []map[string]interface{} {
			alog := fmt.Sprintf("(%d) Oracle DB query %s info exec sql is {%s}", logseq, logKeyword, sql)
			global.Wlog.Info(alog)
			rows, err := db.Query(sql)
			if err != nil {
				blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logseq, sql, err)
				global.Wlog.Error(blog)
			}
			clog := fmt.Sprintf("(%d) start dispos Oracle DB query %s.", logseq, logKeyword)
			global.Wlog.Info(clog)
			a, _ := rowDataDisposMap(rows, "Privileges", logseq)
			dlog := fmt.Sprintf("(%d) Oracle DB query %s data completion.", logseq, logKeyword)
			global.Wlog.Info(dlog)
			return a
		}
		columnMerge = func(AC []map[string]interface{}, F1 ...string) map[string][]string {
			var cc = make(map[string][]string)
			var N, E string
			var dd []string
			for _, C := range AC {
				if len(F1) == 1 {
					E = fmt.Sprintf("%s", F1)
				} else {
					E = fmt.Sprintf("%s", C[F1[0]])
				}
				//E = fmt.Sprintf("%s", F1)
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
	)

	globalPri["SELECT"] = 0
	globalPriAllTab["SELECT ANY TABLE"] = 0
	if strings.ToUpper(datefix) == "TABLE" {
		globalPri["INSERT"] = 0
		globalPriAllTab["INSERT ANY TABLE"] = 0
		globalPri["DELETE"] = 0
		globalPriAllTab["DELETE ANY TABLE"] = 0
	}
	var priAllTableS []string
	for k, _ := range globalPriAllTab {
		priAllTableS = append(priAllTableS, k)
	}
	elog := fmt.Sprintf("(%d) The permissions that the current Oracle DB needs to check is message {%v}, to check it...", logThreadSeq, globalPri)
	global.Wlog.Info(elog)

	//针对要校验的库做去重（库级别的）
	//校验库.表由切片改为map
	for _, AA := range checkTableList {
		newCheckTableList[strings.ToUpper(AA)]++
	}
	flog := fmt.Sprintf("(%d) The current Oracle DB needs to check the permission table is message {%v}, to check it...", logThreadSeq, newCheckTableList)
	global.Wlog.Info(flog)
	//校验库做去重处理
	for _, aa := range checkTableList {
		A[strings.ToUpper(strings.Split(aa, ".")[0])]++
	}
	glog := fmt.Sprintf("(%d) The current Oracle DB needs to check the authority of the library is message {%v},to check it...", logThreadSeq, A)
	global.Wlog.Info(glog)

	//处理oracle用户角色，该用户角色拥有insert、select 、delete any table的权限
	//查找全局权限 类似于grant all privileges on *.* 或 grant select on *.*
	ilog := fmt.Sprintf("(%d) Query the current Oracle DB global dynamic grants permission, to query it...", logThreadSeq)
	global.Wlog.Info(ilog)
	strsql := fmt.Sprintf("SELECT PRIVILEGE as \"privileges\" FROM ROLE_SYS_PRIVS WHERE PRIVILEGE IN ('%s') group by PRIVILEGE", strings.Join(priAllTableS, "','"))
	globalDynamic := sqlQuery(logThreadSeq, strsql, "Global Dynamic Grants")
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
				jlog := fmt.Sprintf("(%d) The Oracle DB table information that meets the permissions and needs to be verified is {%v}...", logThreadSeq, newCheckTableList)
				global.Wlog.Info(jlog)
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
	//查询当前库的权限
	//类似于grant all privileges on pcms.* 或 grant select on pcms.*

	//查询当前表的权限
	//类似于grant all privileges on pcms.a 或 grant select on pcms.a
	nlog := fmt.Sprintf("(%d) Query the current Oracle DB global dynamic table permission, to query it...", logThreadSeq)
	global.Wlog.Info(nlog)
	//遍历没有schema pri权限的剩余库
	var DM = make(map[string]int)
	for _, D := range checkTableList {
		DM[strings.ToUpper(D)]++
	}
	strsql = fmt.Sprintf("select owner||'.'||table_name AS \"tablesName\",PRIVILEGE as \"privileges\" from user_tab_privs")
	tablePri := sqlQuery(logThreadSeq, strsql, "TABLE PRIVILEGES")
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
	fmt.Println("111: ", or.Schema, or.Table)
	if len(PT) == 0 {
		olog := fmt.Sprintf("(%d) The current table %s.%s in Oracle DB lacks some rights restrictions, please check the rights related to the table %s.%s {%v}", logThreadSeq, or.Schema, or.Table, or.Schema, or.Table, globalPri)
		global.Wlog.Error(olog)
	}
	olog := fmt.Sprintf("(%d) The Oracle DB table information that needs to be verified to meet the permissions is {%v}, and the information that is not satisfied is {%v}...", logThreadSeq, PT, abPT)
	global.Wlog.Info(olog)
	return PT, nil
}

/*
	获取校验表的列信息，包含列名，列序号，列类型
*/
func (or *QueryTable) TableAllColumn(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	//sqlStr := fmt.Sprintf("select column_name as \"columnName\",data_type as \"dataType\",COLUMN_id as \"columnSeq\" from all_tab_columns where owner=\"%s\" and table_name=\"%s\" order by 'column_id'", strings.ToUpper(or.Schema), or.Table)
	sqlStr := fmt.Sprintf("SELECT column_name as \"columnName\",case when data_type='NUMBER' AND DATA_PRECISION is null THEN DATA_TYPE when data_type='NUMBER' AND DATA_PRECISION is not null then DATA_TYPE || '(' || DATA_PRECISION || ',' || NVL(DATA_SCALE,0) || ')' when data_type='VARCHAR2' THEN DATA_TYPE||'('||DATA_LENGTH||')' ELSE DATA_TYPE END AS \"dataType\",COLUMN_id as \"columnSeq\" FROM all_tab_columns WHERE owner='%s' and TABLE_NAME = '%s' order by 'column_id'", strings.ToUpper(or.Schema), or.Table)
	alog := fmt.Sprintf("(%d) Oracle DB query table query Table metadata info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		global.Wlog.Error(blog)
		return nil, err
	}
	defer sqlRows.Close()
	return rowDataDisposMap(sqlRows, "TableAllColumn", logThreadSeq)
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

func (or *QueryTable) TableIndexChoice(queryData []map[string]interface{}, logThreadSeq int64) map[string][]string {
	var (
		indexChoice                           = make(map[string][]string)
		nultiseriateIndexColumnMap            = make(map[string][]string)
		multiseriateIndexColumnMap            = make(map[string][]string)
		PriIndexCol, uniIndexCol, mulIndexCol []string
		indexName                             string
	)
	//索引列处理，联合索引进行列合并
	//去除主键索引列、唯一索引列、普通索引列的所有列明
	alog := fmt.Sprintf("(%d) Oracle DB starts to merge and process primary key index,unique index, and common index.", logThreadSeq)
	global.Wlog.Info(alog)
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
	blog := fmt.Sprintf("(%d) Oracle DB index merge processing complete. The index merged data is {primary key: %v,unique key: %v,nounique key: %v}", logThreadSeq, PriIndexCol, nultiseriateIndexColumnMap, multiseriateIndexColumnMap)
	global.Wlog.Info(blog)
	//处理主键索引列
	//判断是否存在主键索引,每个表的索引只有一个
	clog := fmt.Sprintf("(%d) MySQL DB primary key index starts to choose the best.", logThreadSeq)
	global.Wlog.Info(clog)
	if len(PriIndexCol) == 1 { //单列主键索引
		indexChoice["pri_single"] = PriIndexCol
	} else if len(PriIndexCol) > 1 { //联合主键索引
		indexChoice["pri_multiseriate"] = PriIndexCol
	}
	dlog := fmt.Sprintf("(%d) MySQL DB unique key index starts to choose the best.", logThreadSeq)
	global.Wlog.Info(dlog)
	g := or.keyChoiceDispos(nultiseriateIndexColumnMap, "uni")
	for k, v := range g {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	elog := fmt.Sprintf("(%d) MySQL DB nounique key index starts to choose the best.", logThreadSeq)
	global.Wlog.Info(elog)
	f := or.keyChoiceDispos(multiseriateIndexColumnMap, "mui")
	for k, v := range f {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	return indexChoice
}

func (or *QueryTable) Trigger(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var tmpb = make(map[string]string)
	sqlStr := fmt.Sprintf("select TRIGGER_name as triggerName,TABLE_NAME as tableName from all_triggers where owner = '%s'", or.Schema)
	alog := fmt.Sprintf("(%d) Oracle DB query table query Trigger info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)

	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		global.Wlog.Error(blog)
		return nil, err
	}
	triggerName, _ := rowDataDisposMap(sqlRows, "Trigger", logThreadSeq)
	for _, v := range triggerName {
		sqlStr = fmt.Sprintf(" SELECT DBMS_METADATA.GET_DDL('TRIGGER','%s','%s') AS CREATE_TRIGGER FROM DUAL", v["TRIGGERNAME"], or.Schema)
		elog := fmt.Sprintf("(%d) Oracle DB query create Trigger databases %s info, exec sql is {%s}", logThreadSeq, or.Schema, sqlStr)
		global.Wlog.Info(elog)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
			global.Wlog.Error(blog)
			return tmpb, err
		}
		flog := fmt.Sprintf("(%d) start dispos Oracle DB databases %s create Trigger info.", logThreadSeq, or.Schema)
		global.Wlog.Info(flog)
		createTrigger, _ := rowDataDisposMap(sqlRows, "TRIGGER", logThreadSeq)
		glog := fmt.Sprintf("(%d) Oracle DB query databases %s create Trigger completion.", logThreadSeq, or.Schema)
		global.Wlog.Info(glog)
		hlog := fmt.Sprintf("(%d) Oracle DB query databases %s dispos Trigger data info. to dispos it ...", logThreadSeq, or.Schema)
		global.Wlog.Info(hlog)

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
			var triggerOn, triggerTRX string
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
		zlog := fmt.Sprintf("(%d) Oracle DB query databases %s Trigger data completion...", logThreadSeq, or.Schema)
		global.Wlog.Info(zlog)
	}
	return tmpb, nil
}
func (or *QueryTable) Proc(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var tmpb = make(map[string]string)
	sqlStr := fmt.Sprintf(" select object_name as ROUTINE_NAME from all_procedures where object_type='PROCEDURE' and owner = '%s'", or.Schema)
	alog := fmt.Sprintf("(%d) Oracle DB query table query Stored Procedure info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		global.Wlog.Error(blog)
	}
	flog := fmt.Sprintf("(%d) start dispos Oracle DB databases %s create Stored Procedure info.", logThreadSeq, or.Schema)
	global.Wlog.Info(flog)
	routineName, _ := rowDataDisposMap(sqlRows, "Proc", logThreadSeq)
	hlog := fmt.Sprintf("(%d) Oracle DB query databases %s dispos Stored Procedure data info. to dispos it ...", logThreadSeq, or.Schema)
	global.Wlog.Info(hlog)
	for _, v := range routineName {
		sqlStr = fmt.Sprintf(" SELECT DBMS_METADATA.GET_DDL('PROCEDURE','%s','%s') AS CREATE_PROCEDURE FROM DUAL", v["ROUTINE_NAME"], or.Schema)
		ilog := fmt.Sprintf("(%d) Oracle DB query table create Stored Procedure info exec sql is {%s}", logThreadSeq, sqlStr)
		global.Wlog.Info(ilog)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
			global.Wlog.Error(blog)
			return tmpb, err
		}
		jlog := fmt.Sprintf("(%d) start dispos Oracle DB databases %s create Stored Procedure info.", logThreadSeq, or.Schema)
		global.Wlog.Info(jlog)
		createFunc, _ := rowDataDisposMap(sqlRows, "Proc", logThreadSeq)
		klog := fmt.Sprintf("(%d) Oracle DB query databases %s dispos Stored Procedure data info. to dispos it ...", logThreadSeq, or.Schema)
		global.Wlog.Info(klog)
		for _, b := range createFunc {
			tmpb[strings.ToUpper(fmt.Sprintf("%s", v["ROUTINE_NAME"]))] = strings.ReplaceAll(fmt.Sprintf("%s", b["CREATE_PROCEDURE"]), "\n", "")
		}
		zlog := fmt.Sprintf("(%d) Oracle DB query databases %s Stored Procedure data completion...", logThreadSeq, or.Schema)
		global.Wlog.Info(zlog)
	}
	return tmpb, nil
}
func (or *QueryTable) Func(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var tmpb = make(map[string]string)
	sqlStr := fmt.Sprintf("select OBJECT_NAME as ROUTINE_NAME  from all_procedures where object_type='FUNCTION' and owner = '%s'", or.Schema)
	alog := fmt.Sprintf("(%d) Oracle DB query table query Stored Function info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)
	sqlRows, err := db.Query(sqlStr)
	routineName, err := rowDataDisposMap(sqlRows, "Func", logThreadSeq)
	if err != nil {
		blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		global.Wlog.Error(blog)
	}
	for _, v := range routineName {
		sqlStr = fmt.Sprintf(" SELECT DBMS_METADATA.GET_DDL('FUNCTION','%s','%s') AS CREATE_FUNCTION FROM DUAL", v["ROUTINE_NAME"], or.Schema)
		elog := fmt.Sprintf("(%d) Oracle DB query create Stored Function databases %s info, exec sql is {%s}", logThreadSeq, or.Schema, sqlStr)
		global.Wlog.Info(elog)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
			global.Wlog.Error(blog)
		}
		flog := fmt.Sprintf("(%d) start dispos Oracle DB databases %s create Stored Function info.", logThreadSeq, or.Schema)
		global.Wlog.Info(flog)
		createFunc, _ := rowDataDisposMap(sqlRows, "Func", logThreadSeq)
		glog := fmt.Sprintf("(%d) Oracle DB query databases %s create Stored Function completion.", logThreadSeq, or.Schema)
		global.Wlog.Info(glog)
		hlog := fmt.Sprintf("(%d) Oracle DB query databases %s dispos Stored Function data info. to dispos it ...", logThreadSeq, or.Schema)
		global.Wlog.Info(hlog)

		for _, b := range createFunc {
			d := strings.Join(strings.Fields(strings.ReplaceAll(fmt.Sprintf("%s", b["CREATE_FUNCTION"]), "\n", " ")), " ")
			if strings.Contains(strings.ToUpper(d), "BEGIN") && strings.Contains(strings.ToUpper(d), "END") {
				strings.Index(d, "BEGIN")
			}
			tmpb[strings.ToUpper(fmt.Sprintf("%s", v["ROUTINE_NAME"]))] = strings.ReplaceAll(fmt.Sprintf("%s", b["CREATE_FUNCTION"]), "\n", "")
		}
		zlog := fmt.Sprintf("(%d) Oracle DB query databases %s Stored Function data completion...", logThreadSeq, or.Schema)
		global.Wlog.Info(zlog)
	}
	return tmpb, nil
}
func (or *QueryTable) Foreign(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		routineNameM = make(map[string]int)
		tmpb         = make(map[string]string)
	)
	sqlStr := fmt.Sprintf(" select c.OWNER as DATABASE,c.table_name as TABLENAME, c.r_constraint_name,c.delete_rule,cc.column_name,cc.position from user_constraints c join user_cons_columns cc on c.constraint_name=cc.constraint_name and c.table_name=cc.table_name  where c.constraint_type='R' and c.validated='VALIDATED' and c.OWNER = '%s' and c.table_name='%s'", strings.ToUpper(or.Schema), or.Table)
	alog := fmt.Sprintf("(%d) Oracle DB query table query Foreign info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)
	sqlRows, err := db.Query(sqlStr)
	foreignName, err := rowDataDisposMap(sqlRows, "Foreign", logThreadSeq)
	if err != nil {
		blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		global.Wlog.Error(blog)
	}

	for _, v := range foreignName {
		routineNameM[fmt.Sprintf("%s.%s", v["DATABASE"], v["TABLENAME"])]++
	}
	for k, _ := range routineNameM {
		schema, table := strings.Split(k, ".")[0], strings.Split(k, ".")[1]
		sqlStr = fmt.Sprintf("SELECT DBMS_METADATA.GET_DDL('TABLE','%s','%s') as CREATE_FOREIGN FROM DUAL", table, schema)
		elog := fmt.Sprintf("(%d) MySQL DB query create Foreign table %s.%s info, exec sql is {%s}", logThreadSeq, or.Schema, or.Table, sqlStr)
		global.Wlog.Info(elog)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
			global.Wlog.Error(blog)
			tmpb[k] = ""
			return tmpb, err
		}
		flog := fmt.Sprintf("(%d) start dispos Oracle DB create table %s.%s create Foreign info.", logThreadSeq, or.Schema, or.Table)
		global.Wlog.Info(flog)
		createForeign, _ := rowDataDisposMap(sqlRows, "Foreign", logThreadSeq)
		glog := fmt.Sprintf("(%d) Oracle DB query table %s.%s create Foreign completion.", logThreadSeq, or.Schema, or.Table)
		global.Wlog.Info(glog)
		hlog := fmt.Sprintf("(%d) Oracle DB query table %s.%s dispos Foreign data info. to dispos it ...", logThreadSeq, or.Schema, or.Table)
		global.Wlog.Info(hlog)

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
		zlog := fmt.Sprintf("(%d) Oracle DB query table %s.%s Foreign data completion...", logThreadSeq, or.Schema, or.Table)
		global.Wlog.Info(zlog)
	}
	return tmpb, nil
}
func (or *QueryTable) Partitions(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		routineNameM = make(map[string]int)
		tmpb         = make(map[string]string)
	)
	sqlStr := fmt.Sprintf("select OWNER,TABLE_NAME from all_tables  where owner='%s' and TABLE_NAME='%s' and partitioned='YES'", or.Schema, or.Table)
	alog := fmt.Sprintf("(%d) Oracle DB query table query partitions info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		global.Wlog.Error(blog)
	}
	clog := fmt.Sprintf("(%d) start dispos Oracle DB query table %s.%s query Partitions info.", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Info(clog)
	partitionsName, _ := rowDataDisposMap(sqlRows, "Partitions", 10)
	dlog := fmt.Sprintf("(%d) Oracle DB query table %s.%s query Partitions completion.", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Info(dlog)
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
		sqlStr = fmt.Sprintf("SELECT DBMS_METADATA.GET_DDL('TABLE','%s','%s') AS CREATE_PARTITIONS FROM DUAL", table, schema)
		elog := fmt.Sprintf("(%d) Oracle DB query create partitions table %s.%s info, exec sql is {%s}", logThreadSeq, or.Schema, or.Table, sqlStr)
		global.Wlog.Info(elog)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			blog := fmt.Sprintf("(%d) Oracle DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
			global.Wlog.Error(blog)
			tmpb[k] = ""
			return tmpb, err
		}
		flog := fmt.Sprintf("(%d) start dispos Oracle DB create table %s.%s create Partitions info.", logThreadSeq, or.Schema, or.Table)
		global.Wlog.Info(flog)
		createPartitions, _ := rowDataDisposMap(sqlRows, "Partitions", logThreadSeq)
		glog := fmt.Sprintf("(%d) Oracle DB query table %s.%s create Partitions completion.", logThreadSeq, or.Schema, or.Table)
		global.Wlog.Info(glog)
		hlog := fmt.Sprintf("(%d) Oracle DB query table %s.%s dispos Partitions data info. to dispos it ...", logThreadSeq, or.Schema, or.Table)
		global.Wlog.Info(hlog)
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
		zlog := fmt.Sprintf("(%d) Oracle DB query table %s.%s partitions data completion...", logThreadSeq, or.Schema, or.Table)
		global.Wlog.Info(zlog)
	}
	return tmpb, nil
}
func (or *QueryTable) Struct(db *sql.DB) (map[string]string, error) {
	return nil, nil
}
