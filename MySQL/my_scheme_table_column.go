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

var rowDataDisposMap = func(sqlRows *sql.Rows, event string, seq int64) ([]map[string]interface{}, error) {
	// 获取列名
	columns, err := sqlRows.Columns()
	if err != nil {
		errInfo := fmt.Sprintf("(%d) MySQL DB Get the column fail. Error Info: ", seq, event, err)
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
func (my *QueryTable) DatabaseNameList(ignSchema string, logThreadSeq int64) []string {
	var sqlStr string
	var dbName []string
	excludeSchema := fmt.Sprintf("'information_Schema','performance_Schema','sys','mysql'")
	alog := fmt.Sprintf("(%d) MySQL DB ignore sys database message is {%s}", logThreadSeq, excludeSchema)
	global.Wlog.Info(alog)
	excludeSchema = fmt.Sprintf("%s,'%s'", excludeSchema, ignSchema)
	blog := fmt.Sprintf("(%d) MySQL DB ignore database message is {%s}", logThreadSeq, excludeSchema)
	global.Wlog.Info(blog)
	if my.Schema == "*" {
		sqlStr = fmt.Sprintf("select Schema_NAME as databaseName from information_Schema.Schemata where Schema_name not in (%s);", excludeSchema)
	} else {
		my.Schema = strings.ReplaceAll(my.Schema, ",", "','")
		sqlStr = fmt.Sprintf("select Schema_NAME as databaseName from information_Schema.Schemata where Schema_name  in ('%s') and Schema_name not in (%s);", my.Schema, excludeSchema)
	}
	clog := fmt.Sprintf("(%d) MySQL DB query database exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(clog)

	dlog := fmt.Sprintf("(%d) MySQL DB begin exec sql.", logThreadSeq)
	global.Wlog.Info(dlog)
	rows, err := my.Db.Query(sqlStr)
	if err != nil {
		elog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s},Error info is {%s}", logThreadSeq, sqlStr, err)
		global.Wlog.Error(elog)
	}
	flog := fmt.Sprintf("(%d) MySQL DB complete exec sql.")
	global.Wlog.Info(flog)
	tableData, err := rowDataDisposMap(rows, "Schema", logThreadSeq)
	if err == nil && len(tableData) > 0 {
		for i := range tableData {
			dbName = append(dbName, fmt.Sprintf("%v", tableData[i]["databaseName"]))
		}
	}
	defer rows.Close()
	return dbName
}

func (my *QueryTable) TableNameList(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var sqlStr string
	if my.Table == "*" {
		sqlStr = fmt.Sprintf("select table_Schema as databaseName,table_name as tableName from information_Schema.tables where TABLE_Schema in ('%s');", my.Schema)
	} else {
		sqlStr = fmt.Sprintf("select table_Schema as databaseName,table_name as tableName from information_Schema.tables where TABLE_Schema in ('%s') and TABLE_NAME in ('%s');", my.Schema, my.Table)
	}
	alog := fmt.Sprintf("(%d) MySQL DB query table metadata info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)
	rows, err1 := my.Db.Query(sqlStr)
	if err1 != nil {
		blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s},Error info is {%s}.", logThreadSeq, sqlStr, err1)
		global.Wlog.Error(blog)
	}
	defer rows.Close()
	return rowDataDisposMap(rows, "Table", logThreadSeq)
}

func (my *QueryTable) TableColumnName(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	strsql := fmt.Sprintf("select COLUMN_NAME as columnName from information_Schema.columns where TABLE_Schema='%s' and TABLE_NAME='%s' order by ORDINAL_POSITION;", my.Schema, my.Table)
	alog := fmt.Sprintf("(%d) MySQL DB query table metadata info exec sql is {%s}", logThreadSeq, strsql)
	global.Wlog.Info(alog)
	rows, err := db.Query(strsql)
	if err != nil {
		blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, strsql, err)
		global.Wlog.Error(blog)
	}
	clog := fmt.Sprintf("(%d) start dispos MySQL DB query table %s.%s metadata info.", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Info(clog)
	tableData, err := rowDataDisposMap(rows, "Column", logThreadSeq)
	zlog := fmt.Sprintf("(%d) MySQL DB query table %s.%s metadata data completion.", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Info(zlog)
	defer rows.Close()
	return tableData, err
}
func (my *QueryTable) GlobalAccessPri(db *sql.DB, logThreadSeq int64) bool {
	var (
		//logThreadSeq int = 20
		globalPri   = make(map[string]int)
		currentUser string
		sqlQuery    = func(logseq int64, sql, logKeyword string) []map[string]interface{} {
			alog := fmt.Sprintf("(%d) MySQL DB query %s info exec sql is {%s}", logseq, logKeyword, sql)
			global.Wlog.Info(alog)
			rows, err := db.Query(sql)
			if err != nil {
				blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logseq, sql, err)
				global.Wlog.Error(blog)
			}
			clog := fmt.Sprintf("(%d) start dispos MySQL DB query %s.", logseq, logKeyword)
			global.Wlog.Info(clog)
			a, _ := rowDataDisposMap(rows, "Privileges", logseq)
			dlog := fmt.Sprintf("(%d) MySQL DB query %s data completion.", logseq, logKeyword)
			global.Wlog.Info(dlog)
			return a
		}
	)

	//针对要校验的库做去重（库级别的）
	globalPri["SESSION_VARIABLES_ADMIN"] = 0
	//globalPri["FLUSH_TABLES"] = 0
	globalPri["REPLICATION CLIENT"] = 0

	elog := fmt.Sprintf("(%d) The permissions that the current MySQL DB needs to check is message {%v}, to check it...", logThreadSeq, globalPri)
	global.Wlog.Info(elog)

	var globalPriS []string
	for k, _ := range globalPri {
		globalPriS = append(globalPriS, k)
	}
	//获取当前匹配的用户
	strsql := fmt.Sprintf("select current_user() as user;")
	currentUser = fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%s", sqlQuery(logThreadSeq, strsql, "current user")[0]["user"]), "@", "'@'"))
	hlog := fmt.Sprintf("(%d) The user account corresponding to the currently connected MySQL DB user is message {%s}", logThreadSeq, currentUser)
	global.Wlog.Info(hlog)

	//查找全局权限 类似于grant all privileges on *.* 或 grant select on *.*
	ilog := fmt.Sprintf("(%d) Query the current MySQL DB global dynamic grants permission, to query it...", logThreadSeq)
	global.Wlog.Info(ilog)
	strsql = fmt.Sprintf("select PRIVILEGE_TYPE as privileges from information_schema.USER_PRIVILEGES where PRIVILEGE_TYPE in('%s') and grantee = \"%s\";", strings.Join(globalPriS, "','"), currentUser)
	globalDynamic := sqlQuery(logThreadSeq, strsql, "Global Dynamic Grants")
	//权限缺失列表
	for _, gd := range globalDynamic {
		if _, ok := globalPri[strings.ToUpper(fmt.Sprintf("%s", gd["privileges"]))]; ok {
			delete(globalPri, strings.ToUpper(fmt.Sprintf("%s", gd["privileges"])))
		}
	}
	if len(globalPri) == 0 {
		jlog := fmt.Sprintf("(%d) The current global access user with permission to connect to MySQL DB is normal and can be verified normally...", logThreadSeq)
		global.Wlog.Info(jlog)
		return true
	}
	if _, ok := globalPri["SESSION_VARIABLES_ADMIN"]; ok {
		klog := fmt.Sprintf("(%d) The current user connecting to MySQL DB lacks \"session_variables_admin\" permission, and the check table is empty", logThreadSeq)
		global.Wlog.Error(klog)
		return false
	}
	if _, ok := globalPri["REPLICATION CLIENT"]; ok {
		klog := fmt.Sprintf("(%d) The current user connecting to MySQL DB lacks \"REPLICATION CLIENT\" permission, and the check table is empty", logThreadSeq)
		global.Wlog.Error(klog)
		return false
	}
	//if _, ok := globalPri["FLUSH_TABLES"]; ok {
	//	klog := fmt.Sprintf("(%d) The current user connecting to MySQL DB lacks \"FLUSH_TABLES\" permission, and the check table is empty", logThreadSeq)
	//	global.Wlog.Error(klog)
	//	return false
	//}

	return true
}

//连接用户访问表的权限检查及全局权限检查
func (my *QueryTable) TableAccessPriCheck(db *sql.DB, checkTableList []string, datefix string, logThreadSeq int64) (map[string]int, error) {
	var (
		globalPri         = make(map[string]int)
		newCheckTableList = make(map[string]int)
		currentUser       string
		A                 = make(map[string]int)
		PT, abPT          = make(map[string]int), make(map[string]int)
		sqlQuery          = func(logseq int64, sql, logKeyword string) []map[string]interface{} {
			alog := fmt.Sprintf("(%d) MySQL DB query %s info exec sql is {%s}", logseq, logKeyword, sql)
			global.Wlog.Info(alog)
			rows, err := db.Query(sql)
			if err != nil {
				blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logseq, sql, err)
				global.Wlog.Error(blog)
			}
			clog := fmt.Sprintf("(%d) start dispos MySQL DB query %s.", logseq, logKeyword)
			global.Wlog.Info(clog)
			a, _ := rowDataDisposMap(rows, "Privileges", logseq)
			dlog := fmt.Sprintf("(%d) MySQL DB query %s data completion.", logseq, logKeyword)
			global.Wlog.Info(dlog)
			return a
		}
	)

	//针对要校验的库做去重（库级别的）
	globalPri["SELECT"] = 0
	if strings.ToUpper(datefix) == "TABLE" {
		globalPri["INSERT"] = 0
		globalPri["DELETE"] = 0
	}
	elog := fmt.Sprintf("(%d) The permissions that the current MySQL DB needs to check is message {%v}, to check it...", logThreadSeq, globalPri)
	global.Wlog.Info(elog)

	//校验库.表由切片改为map
	for _, AA := range checkTableList {
		newCheckTableList[strings.ToUpper(AA)]++
	}
	flog := fmt.Sprintf("(%d) The current MySQL DB needs to check the permission table is message {%v}, to check it...", logThreadSeq, newCheckTableList)
	global.Wlog.Info(flog)
	//校验库做去重处理
	for _, aa := range checkTableList {
		A[strings.ToUpper(strings.Split(aa, ".")[0])]++
	}
	glog := fmt.Sprintf("(%d) The current MySQL DB needs to check the authority of the library is message {%v},to check it...", logThreadSeq, A)
	global.Wlog.Info(glog)

	//获取当前匹配的用户
	strsql := fmt.Sprintf("select current_user() as user;")
	currentUser = fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%s", sqlQuery(logThreadSeq, strsql, "current user")[0]["user"]), "@", "'@'"))
	hlog := fmt.Sprintf("(%d) The user account corresponding to the currently connected MySQL DB user is message {%s}", logThreadSeq, currentUser)
	global.Wlog.Info(hlog)

	//查找全局权限 类似于grant all privileges on *.* 或 grant select on *.*
	ilog := fmt.Sprintf("(%d) Query the current MySQL DB global dynamic grants permission, to query it...", logThreadSeq)
	global.Wlog.Info(ilog)
	strsql = fmt.Sprintf("select PRIVILEGE_TYPE as privileges from information_schema.USER_PRIVILEGES where PRIVILEGE_TYPE in('SELECT','DELETE','INSERT') and grantee = \"%s\";", currentUser)
	globalDynamic := sqlQuery(logThreadSeq, strsql, "Global Dynamic Grants")
	//权限缺失列表
	for _, gd := range globalDynamic {
		if _, ok := globalPri[strings.ToUpper(fmt.Sprintf("%s", gd["privileges"]))]; ok {
			delete(globalPri, strings.ToUpper(fmt.Sprintf("%s", gd["privileges"])))
		}
	}
	if len(globalPri) == 0 {
		jlog := fmt.Sprintf("(%d) The MySQL DB table information that meets the permissions and needs to be verified is {%v}...", logThreadSeq, newCheckTableList)
		global.Wlog.Info(jlog)
		return newCheckTableList, nil
	}

	//查询当前库的权限
	//类似于grant all privileges on pcms.* 或 grant select on pcms.*
	llog := fmt.Sprintf("(%d) Query the current MySQL DB global dynamic schema permission, to query it...", logThreadSeq)
	global.Wlog.Info(llog)
	for AC, _ := range A {
		var cc []string
		var intseq int
		strsql = fmt.Sprintf("select TABLE_SCHEMA as databaseName,PRIVILEGE_TYPE as privileges from information_schema.schema_PRIVILEGES where PRIVILEGE_TYPE in ('SELECT','DELETE','INSERT') and TABLE_SCHEMA = '%s' and grantee = \"%s\";", AC, currentUser)
		schemaPri := sqlQuery(logThreadSeq, strsql, "SCHEMA PRIVILEGES")
		if len(schemaPri) == 0 {
			continue
		}
		for _, ab := range schemaPri {
			cc = append(cc, fmt.Sprintf("%s", ab["privileges"]))
		}
		for _, ci := range cc {
			if _, ok := globalPri[ci]; ok {
				intseq++
			}
		}
		if intseq == len(globalPri) {
			delete(A, AC)
		}
	}
	if len(A) == 0 {
		jlog := fmt.Sprintf("(%d) The MySQL DB table information that meets the permissions and needs to be verified is {%v}...", logThreadSeq, newCheckTableList)
		global.Wlog.Info(jlog)
		return newCheckTableList, nil
	}
	mlog := fmt.Sprintf("(%d) MySQL DB library level permissions are not satisfied with {%v}", logThreadSeq, A)
	global.Wlog.Warn(mlog)
	//查询当前表的权限
	//类似于grant all privileges on pcms.a 或 grant select on pcms.a
	nlog := fmt.Sprintf("(%d) Query the current MySQL DB global dynamic table permission, to query it...", logThreadSeq)
	global.Wlog.Info(nlog)
	//遍历没有schema pri权限的剩余库
	var DM = make(map[string]int)
	for _, D := range checkTableList {
		DM[strings.ToUpper(D)]++
	}

	for B, _ := range A {
		//按照每个库，查询table pri权限
		strsql = fmt.Sprintf("select table_name as tableName,PRIVILEGE_TYPE as privileges from information_schema.table_PRIVILEGES where PRIVILEGE_TYPE in('SELECT','DELETE','INSERT') and TABLE_SCHEMA = '%s' and grantee = \"%s\";", B, currentUser)
		tablePri := sqlQuery(logThreadSeq, strsql, "TABLE PRIVILEGES")
		if len(tablePri) == 0 {
			continue
		}
		//合并当前表的权限
		var cc = make(map[string][]string)
		var N string
		var dd []string
		for _, C := range tablePri {
			E := strings.ToUpper(fmt.Sprintf("%s.%s", B, C["tableName"]))
			if E != N {
				N = E
				dd = []string{}
				dd = append(dd, strings.ToUpper(fmt.Sprintf("%s", C["privileges"])))
			} else {
				dd = append(dd, strings.ToUpper(fmt.Sprintf("%s", C["privileges"])))
			}
			cc[N] = dd
		}
		//判断权限表
		//判断当前表的所有权限是否包全部包含（指定权限）
		for k, v := range cc {
			if _, ok := DM[k]; ok {
				for D, _ := range globalPri {
					if strings.Index(strings.Join(v, ","), D) == -1 {
						abPT[k]++
					} else {
						PT[k]++
					}
				}
			}
		}
	}
	olog := fmt.Sprintf("(%d) The MySQL DB table information that needs to be verified to meet the permissions is {%v}, and the information that is not satisfied is {%v}...", logThreadSeq, PT, abPT)
	global.Wlog.Info(olog)
	return PT, nil
}

/*
	获取校验表的列信息，包含列名，列序号，列类型
*/
func (my *QueryTable) TableAllColumn(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	sqlStr := fmt.Sprintf("select COLUMN_NAME as columnName ,COLUMN_TYPE as dataType,ORDINAL_POSITION as columnSeq from information_Schema.columns where table_Schema= '%s' and table_name='%s' order by ORDINAL_POSITION;", my.Schema, my.Table)
	alog := fmt.Sprintf("(%d) MySQL DB query table query Table metadata info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		global.Wlog.Error(blog)
		return nil, err
	}
	defer sqlRows.Close()
	return rowDataDisposMap(sqlRows, "TableAllColumn", logThreadSeq)
}

//处理唯一索引索引（包含主键索引）
func (my *QueryTable) keyChoiceDispos(IndexColumnMap map[string][]string, indexType string) map[string][]string {
	breakIndexColumnType := []string{"INT", "CHAR", "VARCHAR", "YEAR", "DATE", "TIME"}
	var a, c = make(map[string][]string), make(map[string][]int)
	var indexChoice = make(map[string][]string)
	// ----- 处理唯一索引列，根据选择规则选择一个单列索引，（选择次序：int<--char<--year<--date<-time<-其他）
	infoStr := fmt.Sprintf("Greatdbcheck Checks whether table %s.%s has a unique key index", my.Schema, my.Table)
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
				if strings.Contains(strings.ToUpper(indexColType), vb) {
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

func (my *QueryTable) TableIndexChoice(queryData []map[string]interface{}, logThreadSeq int64) map[string][]string {
	var indexChoice = make(map[string][]string)
	nultiseriateIndexColumnMap := make(map[string][]string)
	multiseriateIndexColumnMap := make(map[string][]string)
	var PriIndexCol, uniIndexCol, mulIndexCol []string
	var indexName string
	if len(queryData) == 0 {
		return nil
	}
	//索引列处理，联合索引进行列合并
	//去除主键索引列、唯一索引列、普通索引列的所有列明
	alog := fmt.Sprintf("(%d) MySQL DB starts to merge and process primary key index,unique index, and common index.", logThreadSeq)
	global.Wlog.Info(alog)
	for _, v := range queryData {
		if v["nonUnique"].(string) == "0" {
			//处理主键索引
			if strings.Contains(v["indexName"].(string), "PRIMARY") {
				if v["indexName"].(string) != indexName {
					indexName = v["indexName"].(string)
				}
				PriIndexCol = append(PriIndexCol, fmt.Sprintf("%s", v["columnName"]))
			}
			//处理唯一索引
			if v["indexName"].(string) != indexName {
				indexName = v["indexName"].(string)
				nultiseriateIndexColumnMap[indexName] = append(uniIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			} else {
				nultiseriateIndexColumnMap[indexName] = append(nultiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			}
		}
		//处理普通索引
		if v["nonUnique"].(string) == "1" {
			if v["indexName"].(string) != indexName {
				indexName = v["indexName"].(string)
				multiseriateIndexColumnMap[indexName] = append(mulIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			} else {
				multiseriateIndexColumnMap[indexName] = append(multiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			}
		}
	}
	blog := fmt.Sprintf("(%d) MySQL DB index merge processing complete. The index merged data is {primary key: %v,unique key: %v,nounique key: %v}", logThreadSeq, PriIndexCol, nultiseriateIndexColumnMap, multiseriateIndexColumnMap)
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
	g := my.keyChoiceDispos(nultiseriateIndexColumnMap, "uni")
	for k, v := range g {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	elog := fmt.Sprintf("(%d) MySQL DB nounique key index starts to choose the best.", logThreadSeq)
	global.Wlog.Info(elog)
	f := my.keyChoiceDispos(multiseriateIndexColumnMap, "mui")
	for k, v := range f {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	return indexChoice
}
func (my *QueryTable) Trigger(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var tmpb = make(map[string]string)
	sqlStr := fmt.Sprintf("select TRIGGER_NAME as triggerName,EVENT_OBJECT_TABLE as tableName from INFORMATION_SCHEMA.TRIGGERS where TRIGGER_SCHEMA in ('%s');", my.Schema)
	alog := fmt.Sprintf("(%d) MySQL DB query table query Trigger info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)

	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		global.Wlog.Error(blog)
		return nil, err
	}

	triggerName, err := rowDataDisposMap(sqlRows, "Trigger", logThreadSeq)
	for _, v := range triggerName {
		sqlStr = fmt.Sprintf("show create trigger %s.%s", my.Schema, v["triggerName"])
		elog := fmt.Sprintf("(%d) MySQL DB query create Trigger databases %s info, exec sql is {%s}", logThreadSeq, my.Schema, sqlStr)
		global.Wlog.Info(elog)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
			global.Wlog.Error(blog)
			return tmpb, err
		}
		flog := fmt.Sprintf("(%d) start dispos MySQL DB databases %s create Trigger info.", logThreadSeq, my.Schema)
		global.Wlog.Info(flog)
		createTrigger, _ := rowDataDisposMap(sqlRows, "TRIGGER", logThreadSeq)
		glog := fmt.Sprintf("(%d) MySQL db query databases %s create Trigger completion.", logThreadSeq, my.Schema)
		global.Wlog.Info(glog)
		hlog := fmt.Sprintf("(%d) MySQL db query databases %s dispos Trigger data info. to dispos it ...", logThreadSeq, my.Schema)
		global.Wlog.Info(hlog)

		for _, b := range createTrigger {
			//获取trigger name
			triggerNa := strings.ToUpper(fmt.Sprintf("\"%s\".\"%s\"", my.Schema, b["Trigger"]))
			d := strings.Join(strings.Fields(strings.ReplaceAll(fmt.Sprintf("%s", b["SQL Original Statement"]), "\n", "")), " ")

			//获取trigger action
			f := strings.Index(d, "TRIGGER")
			g := strings.Index(d, "ON")
			triggerAction := strings.TrimSpace(d[f:g][strings.LastIndexAny(d[f:g], "`")+1:])
			var triggerOn, triggerTRX string
			if strings.Contains(d, "BEGIN") && strings.Contains(d, "END") {
				// 获取trigger table
				i := strings.Index(d, "BEGIN")
				triggerTab := d[g:i][strings.Index(d[g:i], "`")+1 : strings.LastIndexAny(d[g:i], "`")]
				triggerOn = strings.ToUpper(triggerTab)
				//获取trigger struct
				j := strings.Index(d, "END")
				triggerTRX = strings.ToUpper(d[i:j])
			}
			tmpb[triggerNa] = fmt.Sprintf("%s %s %s", triggerAction, triggerOn, triggerTRX)
			//tmpb[fmt.Sprintf("%s", b["Trigger"])] = strings.ReplaceAll(fmt.Sprintf("%s", b["SQL Original Statement"]), "\n", "")
		}
		zlog := fmt.Sprintf("(%d) MySQL db query databases %s Trigger data completion...", logThreadSeq, my.Schema)
		global.Wlog.Info(zlog)
	}
	return tmpb, nil
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
		ROUTINE_NAME := strings.ToUpper(fmt.Sprintf("%s", v["ROUTINE_NAME"]))
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

func (my *QueryTable) Proc(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	sqlStr := fmt.Sprintf("select SPECIFIC_SCHEMA,SPECIFIC_NAME,ORDINAL_POSITION,PARAMETER_MODE,PARAMETER_NAME,DTD_IDENTIFIER from information_schema.PARAMETERS where SPECIFIC_SCHEMA in ('%s') and ROUTINE_TYPE='PROCEDURE' order by ORDINAL_POSITION;", my.Schema)
	alog := fmt.Sprintf("(%d) MySQL DB query table query Stored Procedure info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		global.Wlog.Error(blog)
		return nil, err
	}
	inout, _ := rowDataDisposMap(sqlRows, "Proc", logThreadSeq)

	sqlStr = fmt.Sprintf("select ROUTINE_SCHEMA,ROUTINE_NAME,ROUTINE_DEFINITION,DEFINER from information_schema.ROUTINES where routine_schema in ('%s') and ROUTINE_TYPE='PROCEDURE';", my.Schema)
	clog := fmt.Sprintf("(%d) MySQL DB query table query Stored Procedure info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(clog)
	sqlRows, err = db.Query(sqlStr)
	if err != nil {
		blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		global.Wlog.Error(blog)
		return nil, err
	}
	flog := fmt.Sprintf("(%d) start dispos MySQL DB databases %s create Stored Procedure info.", logThreadSeq, my.Schema)
	global.Wlog.Info(flog)
	createProc, _ := rowDataDisposMap(sqlRows, "Proc", logThreadSeq)
	hlog := fmt.Sprintf("(%d) MySQL db query databases %s dispos Stored Procedure data info. to dispos it ...", logThreadSeq, my.Schema)
	global.Wlog.Info(hlog)
	return procR(createProc, procP(inout, "Proc"), "Proc"), nil
}

func (my *QueryTable) Func(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var tmpb = make(map[string]string)
	sqlStr := fmt.Sprintf("select DEFINER,ROUTINE_NAME from information_schema.ROUTINES where routine_schema in ('%s') and ROUTINE_TYPE='FUNCTION';", my.Schema)
	alog := fmt.Sprintf("(%d) MySQL DB query table query Stored Function info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		global.Wlog.Error(blog)
	}
	routineName, _ := rowDataDisposMap(sqlRows, "Func", logThreadSeq)
	for _, v := range routineName {
		sqlStr = fmt.Sprintf("SHOW CREATE FUNCTION %s.%s;", my.Schema, v["ROUTINE_NAME"])
		elog := fmt.Sprintf("(%d) MySQL DB query create Stored Function databases %s info, exec sql is {%s}", logThreadSeq, my.Schema, sqlStr)
		global.Wlog.Info(elog)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
			global.Wlog.Error(blog)
			return tmpb, err
		}
		flog := fmt.Sprintf("(%d) start dispos MySQL DB databases %s create Stored Function info.", logThreadSeq, my.Schema)
		global.Wlog.Info(flog)
		createFunc, _ := rowDataDisposMap(sqlRows, "Func", logThreadSeq)
		glog := fmt.Sprintf("(%d) MySQL db query databases %s create Stored Function completion.", logThreadSeq, my.Schema)
		global.Wlog.Info(glog)
		hlog := fmt.Sprintf("(%d) MySQL db query databases %s dispos Stored Function data info. to dispos it ...", logThreadSeq, my.Schema)
		global.Wlog.Info(hlog)

		for _, b := range createFunc {
			d := strings.Join(strings.Fields(strings.ReplaceAll(fmt.Sprintf("%s", b["CREATE_FUNCTION"]), "\n", " ")), " ")
			if strings.Contains(strings.ToUpper(d), "BEGIN") && strings.Contains(strings.ToUpper(d), "END") {
				strings.Index(d, "BEGIN")
			}
			//tmpb[fmt.Sprintf("%s", v["ROUTINE_NAME"])] = ""
			tmpb[strings.ToUpper(fmt.Sprintf("%s", v["ROUTINE_NAME"]))] = fmt.Sprintf("%s/*proc*/delimiter $\n%s$\ndelimiter ;\n", v["DEFINER"], b["Create Function"])
		}
		zlog := fmt.Sprintf("(%d) MySQL db query databases %s Stored Function data completion...", logThreadSeq, my.Schema)
		global.Wlog.Info(zlog)
	}
	return tmpb, nil
}
func (my *QueryTable) Foreign(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		routineNameM = make(map[string]int)
		tmpb         = make(map[string]string)
	)
	sqlStr := fmt.Sprintf("select CONSTRAINT_SCHEMA,TABLE_NAME from information_schema.referential_constraints where CONSTRAINT_SCHEMA in ('%s') and TABLE_NAME in ('%s');", my.Schema, my.Table)
	alog := fmt.Sprintf("(%d) MySQL DB query table query Foreign info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)

	sqlRows, err := db.Query(sqlStr)
	foreignName, err := rowDataDisposMap(sqlRows, "Foreign", logThreadSeq)
	if err != nil {
		blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		global.Wlog.Error(blog)
	}

	for _, v := range foreignName {
		routineNameM[fmt.Sprintf("%s.%s", v["CONSTRAINT_SCHEMA"], v["TABLE_NAME"])]++
	}
	for k, _ := range routineNameM {
		var z string
		if strings.EqualFold(k, fmt.Sprintf("%s.%s", my.Schema, my.Table)) {
			z = fmt.Sprintf("%s.%s", my.Schema, my.Table)
		} else {
			z = k
		}
		sqlStr = fmt.Sprintf("SHOW CREATE TABLE %s;", k)
		elog := fmt.Sprintf("(%d) MySQL DB query create Foreign table %s.%s info, exec sql is {%s}", logThreadSeq, my.Schema, my.Table, sqlStr)
		global.Wlog.Info(elog)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
			global.Wlog.Error(blog)
			tmpb[k] = ""
			return tmpb, err
		}
		flog := fmt.Sprintf("(%d) start dispos MySQL DB create table %s.%s create Foreign info.", logThreadSeq, my.Schema, my.Table)
		global.Wlog.Info(flog)
		createForeign, _ := rowDataDisposMap(sqlRows, "Foreign", logThreadSeq)
		glog := fmt.Sprintf("(%d) MySQL db query table %s.%s create Foreign completion.", logThreadSeq, my.Schema, my.Table)
		global.Wlog.Info(glog)
		hlog := fmt.Sprintf("(%d) MySQL db query table %s.%s dispos Foreign data info. to dispos it ...", logThreadSeq, my.Schema, my.Table)
		global.Wlog.Info(hlog)

		for _, b := range createForeign {
			var p, q, o string
			d := fmt.Sprintf("%s", b["Create Table"])
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
				l := strings.Split(strings.TrimSpace(strings.Split(p, "REFERENCES")[1]), " ")[0]
				o = strings.ReplaceAll(p, l, fmt.Sprintf("`%s`.%s", strings.Split(k, ".")[0], l))
			}
			if strings.HasPrefix(q, "REFERENCES") {
				o = fmt.Sprintf("%s %s", p, q)
			}
			tmpb[z] = strings.ToUpper(strings.ReplaceAll(o, "`", "!"))
		}
		zlog := fmt.Sprintf("(%d) MySQL db query table %s.%s Foreign data completion...", logThreadSeq, my.Schema, my.Table)
		global.Wlog.Info(zlog)
	}
	return tmpb, nil
}
func (my *QueryTable) Partitions(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		routineNameM = make(map[string]int)
		tmpb         = make(map[string]string)
	)
	sqlStr := fmt.Sprintf("select TABLE_SCHEMA,TABLE_NAME from information_schema.partitions where table_schema in ('%s') and TABLE_NAME in ('%s') and PARTITION_NAME <> '';", my.Schema, my.Table)
	alog := fmt.Sprintf("(%d) MySQL DB query table query partitions info exec sql is {%s}", logThreadSeq, sqlStr)
	global.Wlog.Info(alog)
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		global.Wlog.Error(blog)
	}
	clog := fmt.Sprintf("(%d) start dispos MySQL DB query table %s.%s query Partitions info.", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Info(clog)
	partitionsName, err := rowDataDisposMap(sqlRows, "Partitions", logThreadSeq)
	dlog := fmt.Sprintf("(%d) MySQL DB query table %s.%s query Partitions completion.", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Info(dlog)

	for _, v := range partitionsName {
		routineNameM[fmt.Sprintf("%s.%s", v["TABLE_SCHEMA"], v["TABLE_NAME"])]++
	}

	for k, _ := range routineNameM {
		sqlStr = fmt.Sprintf("SHOW CREATE TABLE %s;", k)
		elog := fmt.Sprintf("(%d) MySQL DB query create partitions table %s.%s info, exec sql is {%s}", logThreadSeq, my.Schema, my.Table, sqlStr)
		global.Wlog.Info(elog)
		sqlRows, err = db.Query(sqlStr)
		if err != nil {
			blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
			global.Wlog.Error(blog)
			tmpb[k] = ""
			return tmpb, err
		}
		flog := fmt.Sprintf("(%d) start dispos MySQL DB create table %s.%s create Partitions info.", logThreadSeq, my.Schema, my.Table)
		global.Wlog.Info(flog)
		createPartitions, _ := rowDataDisposMap(sqlRows, "Partitions", logThreadSeq)
		glog := fmt.Sprintf("(%d) MySQL db query table %s.%s create Partitions completion.", logThreadSeq, my.Schema, my.Table)
		global.Wlog.Info(glog)
		hlog := fmt.Sprintf("(%d) MySQL db query table %s.%s dispos Partitions data info. to dispos it ...", logThreadSeq, my.Schema, my.Table)
		global.Wlog.Info(hlog)
		for _, b := range createPartitions {
			var partitionMode, partitionColumn string
			var zi string
			if strings.EqualFold(k, fmt.Sprintf("%s.%s", my.Schema, my.Table)) {
				zi = fmt.Sprintf("%s.%s", my.Schema, my.Table)
			} else {
				zi = k
			}
			z := strings.Split(fmt.Sprintf("%s", b["Create Table"]), "\n")
			var a, c []string
			for _, bi := range z {
				if strings.Contains(bi, " PARTITION BY ") {
					il := strings.Index(bi, "PARTITION")
					ii := strings.Join(strings.Fields(strings.TrimSpace(strings.ReplaceAll(bi[il:], "PARTITION BY ", ""))), " ")
					partitionMode, partitionColumn = strings.Split(ii, " ")[0], strings.ReplaceAll(strings.ReplaceAll(strings.Split(ii, " ")[1], "COLUMNS(", "("), "`", "")
					c = append(c, fmt.Sprintf(" PARTITION BY %s %s", partitionMode, strings.ToUpper(partitionColumn)))
				}
				if strings.Contains(bi, "SUBPARTITION BY ") || strings.Contains(bi, "SUBPARTITIONS ") {
					c = append(c, strings.ToUpper(bi))
				}
				if strings.Contains(bi, "PARTITION ") && strings.Contains(bi, "VALUES ") {
					ii := strings.Index(bi, "ENGINE")
					il := strings.ReplaceAll(strings.TrimSpace(bi[:ii]), "IN", "")
					c = append(c, fmt.Sprintf(" %s,", il))
				}
				//处理hash分区
				if strings.Contains(bi, "PARTITIONS ") {
					var ll string
					ll = bi
					if strings.Contains(bi, "*/") {
						ll = bi[:strings.Index(bi, "*/")]
					}
					c = append(c, fmt.Sprintf(" %s", ll))
				}
			}
			x := fmt.Sprintf("%s %s);", strings.Join(a, ""), strings.Join(c, "")[:len(strings.Join(c, ""))-1])
			xs := strings.Join(strings.Fields(x), " ")
			tmpb[zi] = strings.ReplaceAll(xs, "`", "!")
		}
		zlog := fmt.Sprintf("(%d) MySQL db query table %s.%s partitions data completion...", logThreadSeq, my.Schema, my.Table)
		global.Wlog.Info(zlog)
	}
	return tmpb, nil
}

func (my *QueryTable) Struct(db *sql.DB) (map[string]string, error) {
	return nil, nil
}
