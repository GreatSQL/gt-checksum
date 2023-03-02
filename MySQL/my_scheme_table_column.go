package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"strconv"
	"strings"
)

type QueryTable struct {
	Schema              string
	Table               string
	IgnoreTable         string
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
	DBType = "MySQL"
	vlog   string
	err    error
	strsql string
	procP  = func(inout []map[string]interface{}, event string) map[string]string {
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
	procR = func(createProc []map[string]interface{}, tmpa map[string]string, event string) map[string]string {
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
)

/*
	行数据处理
*/

/*
   MySQL 获取对应的库表信息，排除'information_Schema','performance_Schema','sys','mysql'
*/
func (my *QueryTable) DatabaseNameList(db *sql.DB, logThreadSeq int64) (map[string]int, error) {
	var (
		A     = make(map[string]int)
		Event = "Q_Schema_Table_List"
	)
	excludeSchema := fmt.Sprintf("'information_Schema','performance_Schema','sys','mysql'")
	vlog = fmt.Sprintf("(%d) [%s] Start to query the metadata of the %s database and obtain library and table information.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select TABLE_SCHEMA as databaseName,TABLE_NAME as tableName from information_Schema.TABLES where TABLE_SCHEMA not in (%s);", excludeSchema)

	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for i := range tableData {
		var ga string
		gd, gt := fmt.Sprintf("%v", tableData[i]["databaseName"]), fmt.Sprintf("%v", tableData[i]["tableName"])
		if my.LowerCaseTableNames == "no" {
			gd = strings.ToUpper(gd)
			gt = strings.ToUpper(gt)
		}
		ga = fmt.Sprintf("%v/*schema&table*/%v", gd, gt)
		A[ga]++
	}
	vlog = fmt.Sprintf("(%d) [%s] Complete the library and table information query of the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return A, nil
}

/*
	MySQL 通过查询表的元数据信息获取列名
*/
func (my *QueryTable) TableColumnName(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var (
		Event = "Q_table_columns"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start querying the metadata information of table %s.%s in the %s database and get all the column names", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select COLUMN_NAME as columnName from information_Schema.columns where TABLE_Schema='%s' and TABLE_NAME='%s' order by ORDINAL_POSITION;", my.Schema, my.Table)
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
	vlog = fmt.Sprintf("(%d) [%s] Complete the acquisition of all column names in the following table %s.%s of the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tableData, err
}

/*
	MySQL 查询数据库版本信息
*/
func (my *QueryTable) DatabaseVersion(db *sql.DB, logThreadSeq int64) (string, error) {
	var (
		version string
		rows    *sql.Rows
		Event   = "Q_M_Versions"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start querying the version information of the %s database", logThreadSeq, Event, DBType)
	strsql = fmt.Sprintf("select version()")
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if rows, err = dispos.DBSQLforExec(strsql); err != nil {
		if err != nil {
			return "", err
		}
	}
	dispos.SqlRows = rows
	a, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return "", err
	}
	if len(a) == 0 {
		return "", nil
	}
	for _, i := range a {
		if cc, ok := i["version()"]; ok {
			version = fmt.Sprintf("%v", cc)
			break
		}
	}
	vlog = fmt.Sprintf("(%d) [%s] Complete the version information query of the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	defer rows.Close()
	return version, nil
}

/*
	MySQL 查看当前用户是否有全局变量
*/
func (my *QueryTable) GlobalAccessPri(db *sql.DB, logThreadSeq int64) (bool, error) {
	var (
		globalPri   = make(map[string]int)
		version     string
		currentUser string
		rows        *sql.Rows
		Event       = "Q_Table_Global_Access_Pri"

		//sqlQuery = func(logseq int64, sql, logKeyword string) ([]map[string]interface{}, error) {

		//vlog = fmt.Sprintf("(%d) MySQL DB query %s info exec sql is {%s}", logseq, logKeyword, sql)
		//global.Wlog.Debug(vlog)
		//rows, err := db.Query(sql)
		//if err != nil {
		//	vlog = fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logseq, sql, err)
		//	global.Wlog.Error(vlog)
		//	return nil, err
		//}
		//if rows == nil {
		//	return nil, nil
		//}
		//vlog = fmt.Sprintf("(%d) start dispos MySQL DB query %s.", logseq, logKeyword)
		//global.Wlog.Debug(vlog)
		//a, err := rowDataDisposMap(rows, "Privileges", logseq)
		//dispos := dataDispos.DBdataDispos{DBtype: "MySQL", Logseq: logseq, SqlRows: rows, Event: "Privileges"}

		//vlog = fmt.Sprintf("(%d) MySQL DB query %s data completion.", logseq, logKeyword)
		//global.Wlog.Debug(vlog)
		//	return a, nil
		//}
	)
	//要确定MySQL的版本，5.7和8.0
	if version, err = my.DatabaseVersion(db, logThreadSeq); err != nil {
		return false, err
	}
	if version == "" {
		return false, nil
	}
	if strings.HasPrefix(version, "8.") {
		globalPri["SESSION_VARIABLES_ADMIN"] = 0
	}
	//globalPri["FLUSH_TABLES"] = 0
	globalPri["REPLICATION CLIENT"] = 0

	vlog = fmt.Sprintf("(%d) [%s] The permissions that the current %s DB needs to check is message {%v}, to check it...", logThreadSeq, Event, DBType, globalPri)
	global.Wlog.Debug(vlog)
	var globalPriS []string
	for k, _ := range globalPri {
		globalPriS = append(globalPriS, k)
	}
	//获取当前匹配的用户
	strsql = fmt.Sprintf("select current_user() as user;")
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if rows, err = dispos.DBSQLforExec(strsql); err != nil {
		return false, err
	}
	dispos.SqlRows = rows
	CC, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return false, err
	}

	currentUser = fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%s", CC[0]["user"]), "@", "'@'"))
	vlog = fmt.Sprintf("(%d) [%s] The user account corresponding to the currently connected %s DB user is message {%s}", logThreadSeq, Event, DBType, currentUser)
	global.Wlog.Debug(vlog)

	//查找全局权限 类似于grant all privileges on *.* 或 grant select on *.*
	vlog = fmt.Sprintf("(%d) [%s] Query the current %s DB global dynamic grants permission, to query it...", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select PRIVILEGE_TYPE as privileges from information_schema.USER_PRIVILEGES where PRIVILEGE_TYPE in('%s') and grantee = \"%s\";", strings.Join(globalPriS, "','"), currentUser)
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return false, err
	}
	globalDynamic, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return false, err
	}
	//权限缺失列表
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
	if _, ok := globalPri["SESSION_VARIABLES_ADMIN"]; ok && strings.HasPrefix(version, "8.") {
		vlog = fmt.Sprintf("(%d) [%s] The current user connecting to %s DB lacks \"session_variables_admin\" permission, and the check table is empty", logThreadSeq, Event, DBType)
		global.Wlog.Error(vlog)
		return false, nil
	}
	if _, ok := globalPri["REPLICATION CLIENT"]; ok {
		vlog = fmt.Sprintf("(%d) [%s] The current user connecting to %s DB lacks \"REPLICATION CLIENT\" permission, and the check table is empty", logThreadSeq, Event, DBType)
		global.Wlog.Error(vlog)
		return false, nil
	}
	//if _, ok := globalPri["FLUSH_TABLES"]; ok {
	//	vlog = fmt.Sprintf("(%d) The current user connecting to MySQL DB lacks \"FLUSH_TABLES\" permission, and the check table is empty", logThreadSeq)
	//	global.Wlog.Error(vlog)
	//	return false
	//}
	return true, nil
}

/*
	MySQL 查询用户是否有表的读写权限
*/
func (my *QueryTable) TableAccessPriCheck(db *sql.DB, checkTableList []string, datefix string, logThreadSeq int64) (map[string]int, error) {
	var (
		globalPri         = make(map[string]int)
		newCheckTableList = make(map[string]int)
		currentUser       string
		A                 = make(map[string]int)
		PT, abPT          = make(map[string]int), make(map[string]int)
		Event             = "Q_Table_Access_Pri"
		//sqlQuery          = func(logseq int64, sql, logKeyword string) ([]map[string]interface{}, error) {
		//	vlog = fmt.Sprintf("(%d) MySQL DB query %s info exec sql is {%s}", logseq, logKeyword, sql)
		//	global.Wlog.Debug(vlog)
		//	rows, err := db.Query(sql)
		//	if err != nil {
		//		vlog = fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logseq, sql, err)
		//		global.Wlog.Error(vlog)
		//		return nil, err
		//	}
		//	if rows == nil {
		//		return nil, nil
		//	}
		//	vlog = fmt.Sprintf("(%d) start dispos MySQL DB query %s.", logseq, logKeyword)
		//	global.Wlog.Debug(vlog)
		//	//a, err := rowDataDisposMap(rows, "Privileges", logseq)
		//	dispos := dataDispos.DBdataDispos{DBtype: "MySQL", Logseq: logseq, SqlRows: rows, Event: "Privileges"}
		//	a, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		//	rows.Close()
		//	if err != nil {
		//		return nil, err
		//	}
		//	vlog = fmt.Sprintf("(%d) MySQL DB query %s data completion.", logseq, logKeyword)
		//	global.Wlog.Debug(vlog)
		//	return a, nil
		//}
		globalPriS []string
	)

	//针对要校验的库做去重（库级别的）
	globalPri["SELECT"] = 0
	if strings.ToUpper(datefix) == "TABLE" {
		globalPri["INSERT"] = 0
		globalPri["DELETE"] = 0
	}
	for k, _ := range globalPri {
		globalPriS = append(globalPriS, k)
	}
	vlog = fmt.Sprintf("(%d) [%s] The permissions that the current %s DB needs to check is message {%v},check table list is {%v}. to check it...", logThreadSeq, Event, DBType, globalPri, newCheckTableList)
	global.Wlog.Debug(vlog)

	//校验库.表由切片改为map
	for _, AA := range checkTableList {
		newCheckTableList[strings.ToUpper(AA)]++
	}
	//校验库做去重处理
	for _, aa := range checkTableList {
		A[strings.ToUpper(strings.Split(aa, ".")[0])]++
	}

	//获取当前匹配的用户
	strsql = fmt.Sprintf("select current_user() as user;")
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	CC, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	//CC, err := sqlQuery(logThreadSeq, strsql, "current user")
	//if err != nil {
	//	return nil, err
	//}
	//if len(CC) == 0 {
	//	return nil, nil
	//}
	currentUser = fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%s", CC[0]["user"]), "@", "'@'"))
	//vlog = fmt.Sprintf("(%d) The user account corresponding to the currently connected MySQL DB user is message {%s}", logThreadSeq, currentUser)
	//global.Wlog.Debug(vlog)
	//查找全局权限 类似于grant all privileges on *.* 或 grant select on *.*
	vlog = fmt.Sprintf("(%d) [%s] Query the current %s DB global dynamic grants permission, to query it...", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select PRIVILEGE_TYPE as privileges from information_schema.USER_PRIVILEGES where PRIVILEGE_TYPE in('%s') and grantee = \"%s\";", strings.Join(globalPriS, "','"), currentUser)
	//globalDynamic, err := sqlQuery(logThreadSeq, strsql, "Global Dynamic Grants")
	//if err != nil {
	//	return nil, err
	//}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	globalDynamic, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	//权限缺失列表
	for _, gd := range globalDynamic {
		if _, ok := globalPri[strings.ToUpper(fmt.Sprintf("%s", gd["privileges"]))]; ok {
			delete(globalPri, strings.ToUpper(fmt.Sprintf("%s", gd["privileges"])))
		}
	}
	if len(globalPri) == 0 {
		vlog = fmt.Sprintf("(%d) [%s] The %s DB table information that meets the permissions and needs to be verified is {%v}...", logThreadSeq, Event, DBType, newCheckTableList)
		global.Wlog.Debug(vlog)
		return newCheckTableList, nil
	}

	//查询当前库的权限
	//类似于grant all privileges on pcms.* 或 grant select on pcms.*
	vlog = fmt.Sprintf("(%d) [%s] Query the current %s DB global dynamic schema permission, to query it...", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	for AC, _ := range A {
		var cc []string
		var intseq int
		strsql = fmt.Sprintf("select TABLE_SCHEMA as databaseName,PRIVILEGE_TYPE as privileges from information_schema.schema_PRIVILEGES where PRIVILEGE_TYPE in ('%s') and TABLE_SCHEMA = '%s' and grantee = \"%s\";", strings.Join(globalPriS, "','"), AC, currentUser)
		//schemaPri, err1 := sqlQuery(logThreadSeq, strsql, "SCHEMA PRIVILEGES")
		//if err1 != nil {
		//	return nil, err
		//}
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			return nil, err
		}
		schemaPri, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err != nil {
			return nil, err
		}
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
		vlog = fmt.Sprintf("(%d) [%s] The %s DB table information that meets the permissions and needs to be verified is {%v}...", logThreadSeq, Event, DBType, newCheckTableList)
		global.Wlog.Debug(vlog)
		return newCheckTableList, nil
	}
	//vlog = fmt.Sprintf("(%d) MySQL DB library level permissions are not satisfied with {%v}", logThreadSeq, A)
	//global.Wlog.Debug(vlog)
	//查询当前表的权限
	//类似于grant all privileges on pcms.a 或 grant select on pcms.a
	vlog = fmt.Sprintf("(%d) [%s] Query the current %s DB global dynamic table permission, to query it...", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	//遍历没有schema pri权限的剩余库
	var DM = make(map[string]int)
	for _, D := range checkTableList {
		DM[strings.ToUpper(D)]++
	}

	for B, _ := range A {
		//按照每个库，查询table pri权限
		strsql = fmt.Sprintf("select table_name as tableName,PRIVILEGE_TYPE as privileges from information_schema.table_PRIVILEGES where PRIVILEGE_TYPE in('SELECT','DELETE','INSERT') and TABLE_SCHEMA = '%s' and grantee = \"%s\";", B, currentUser)
		//tablePri, err1 := sqlQuery(logThreadSeq, strsql, "TABLE PRIVILEGES")
		//if err1 != nil {
		//	return nil, err
		//}
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			return nil, err
		}
		tablePri, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err != nil {
			return nil, err
		}
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
	vlog = fmt.Sprintf("(%d) [%s] The %s DB table information that needs to be verified to meet the permissions is {%v}, and the information that is not satisfied is {%v}...", logThreadSeq, Event, DBType, PT, abPT)
	global.Wlog.Debug(vlog)
	return PT, nil
}

/*
	MySQL 获取校验表的列信息，包含列名，列序号，列类型
*/
func (my *QueryTable) TableAllColumn(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var (
		//sqlStr string
		//rows   *sql.Rows
		Event = "Q_Table_Column_Metadata"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the metadata of all the columns of table %s.%s in the %s database", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select COLUMN_NAME as columnName ,COLUMN_TYPE as dataType,ORDINAL_POSITION as columnSeq,IS_NULLABLE as isNull from information_Schema.columns where table_Schema= '%s' and table_name='%s' order by ORDINAL_POSITION;", my.Schema, my.Table)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}

	//for i := 1; i < 4; i++ {
	//	rows, err = db.Query(strsql)
	//	if err != nil {
	//		blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, strsql, err)
	//		global.Wlog.Error(blog)
	//		vlog = fmt.Sprintf("(%d) Failed to query the table column source table [%v.%v] for the %v time.", logThreadSeq, my.Schema, my.Table, i)
	//		global.Wlog.Error(vlog)
	//		if i == 3 {
	//			return nil, err
	//		}
	//		time.Sleep(5 * time.Second)
	//	} else {
	//		break
	//	}
	//}
	//if err != nil {
	//	blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, strsql, err)
	//	global.Wlog.Error(blog)
	//	return nil, err
	//}
	//if rows == nil {
	//	return nil, nil
	//}

	//dispos := dataDispos.DBdataDispos{DBtype: "MySQL", Logseq: logThreadSeq, SqlRows: rows, Event: "tableAllColumn"}
	//tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	vlog = fmt.Sprintf("(%d) [%s] Complete the metadata query of all columns in table %s.%s in the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tableData, err
}

/*
	MySQL 处理唯一索引索引（包含主键索引）
*/
func (my *QueryTable) keyChoiceDispos(IndexColumnMap map[string][]string, indexType string) map[string][]string {
	var (
		a, c                 = make(map[string][]string), make(map[string][]int)
		indexChoice          = make(map[string][]string)
		breakIndexColumnType = []string{"INT", "FLOAT", "DOUBLE", "DECIMAL", "CHAR", "VARCHAR", "YEAR", "DATE", "TIME"}
		tmpSliceNum          = 100
		tmpSliceNumMap       = make(map[string]int)
		z                    string
		choseSeq             = 1000000
		intCharMax           int
		indexChoisName       string
	)
	// ----- 处理唯一索引列，根据选择规则选择一个单列索引，（选择次序：int<--char<--year<--date<-time<-其他）
	//infoStr := fmt.Sprintf("Greatdbcheck Checks whether table %s.%s has a unique key index", my.Schema, my.Table)
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
	MySQL 表的索引选择
*/
func (my *QueryTable) TableIndexChoice(queryData []map[string]interface{}, logThreadSeq int64) map[string][]string {
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
	vlog = fmt.Sprintf("(%d) [%s] Start to select the appropriate index column in the following table %s.%s of the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
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
	//vlog = fmt.Sprintf("(%d) MySQL DB index merge processing complete. The index merged data is {primary key: %v,unique key: %v,nounique key: %v}", logThreadSeq, PriIndexCol, nultiseriateIndexColumnMap, multiseriateIndexColumnMap)
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
	g := my.keyChoiceDispos(nultiseriateIndexColumnMap, "uni")
	for k, v := range g {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	//vlog = fmt.Sprintf("(%d) MySQL DB nounique key index starts to choose the best.", logThreadSeq)
	//global.Wlog.Debug(vlog)
	f := my.keyChoiceDispos(multiseriateIndexColumnMap, "mui")
	for k, v := range f {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	vlog = fmt.Sprintf("(%s) [%s] Complete the selection of the appropriate index column in the following table %s.%s of the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(vlog)
	return indexChoice
}

/*
	MySQL 查询触发器信息
*/
func (my *QueryTable) Trigger(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		tmpb  = make(map[string]string)
		Event = "Q_Trigger"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the trigger information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select TRIGGER_NAME as triggerName,EVENT_OBJECT_TABLE as tableName from INFORMATION_SCHEMA.TRIGGERS where TRIGGER_SCHEMA in ('%s');", my.Schema)
	//sqlRows, err := db.Query(strsql)
	//if err != nil {
	//	vlog = fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, strsql, err)
	//	global.Wlog.Error(vlog)
	//	return nil, err
	//}
	//if sqlRows == nil {
	//	return nil, nil
	//}
	//triggerName, err := rowDataDisposMap(sqlRows, "Trigger", logThreadSeq)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	//dispos := dataDispos.DBdataDispos{DBtype: "MySQL", Logseq: logThreadSeq, SqlRows: sqlRows, Event: "Trigger"}
	triggerName, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for _, v := range triggerName {
		strsql = fmt.Sprintf("show create trigger %s.%s", my.Schema, v["triggerName"])
		//vlog = fmt.Sprintf("(%d) MySQL DB query create Trigger databases %s info, exec sql is {%s}", logThreadSeq, my.Schema, sqlStr)
		//global.Wlog.Debug(vlog)
		//sqlRows, err = db.Query(strsql)
		//if err != nil {
		//	vlog = fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		//	global.Wlog.Error(vlog)
		//	return tmpb, err
		//}
		//if sqlRows == nil {
		//	return nil, nil
		//}
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			return nil, err
		}
		//vlog = fmt.Sprintf("(%d) start dispos MySQL DB databases %s create Trigger info.", logThreadSeq, my.Schema)
		//global.Wlog.Debug(vlog)
		//createTrigger, err1 := rowDataDisposMap(sqlRows, "TRIGGER", logThreadSeq)
		//dispos = dataDispos.DBdataDispos{DBtype: "MySQL", Logseq: logThreadSeq, SqlRows: sqlRows, Event: "Trigger"}
		createTrigger, err1 := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err1 != nil {
			return nil, err
		}
		//vlog = fmt.Sprintf("(%d) MySQL db query databases %s create Trigger completion.", logThreadSeq, my.Schema)
		//global.Wlog.Debug(vlog)
		//vlog = fmt.Sprintf("(%d) MySQL db query databases %s dispos Trigger data info. to dispos it ...", logThreadSeq, my.Schema)
		//global.Wlog.Debug(vlog)
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
		}
		//vlog = fmt.Sprintf("(%d) MySQL db query databases %s Trigger data completion...", logThreadSeq, my.Schema)
		//global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%s) [%s] Complete the trigger information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tmpb, nil
}

/*
	MySQL 存储过程校验
*/
func (my *QueryTable) Proc(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		//vlog   string
		//sqlStr string
		Event = "Q_Proc"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the stored procedure information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select SPECIFIC_SCHEMA,SPECIFIC_NAME,ORDINAL_POSITION,PARAMETER_MODE,PARAMETER_NAME,DTD_IDENTIFIER from information_schema.PARAMETERS where SPECIFIC_SCHEMA in ('%s') and ROUTINE_TYPE='PROCEDURE' order by ORDINAL_POSITION;", my.Schema)
	//vlog = fmt.Sprintf("(%d) MySQL DB query table query Stored Procedure info exec sql is {%s}", logThreadSeq, sqlStr)
	//global.Wlog.Debug(vlog)
	//sqlRows, err := db.Query(sqlStr)
	//if err != nil {
	//	vlog = fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
	//	global.Wlog.Error(vlog)
	//	return nil, err
	//}
	//if sqlRows == nil {
	//	return nil, nil
	//}
	//inout, err := rowDataDisposMap(sqlRows, "Proc", logThreadSeq)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	//dispos := dataDispos.DBdataDispos{DBtype: "MySQL", Logseq: logThreadSeq, SqlRows: sqlRows, Event: "Proc"}
	inout, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	strsql = fmt.Sprintf("select ROUTINE_SCHEMA,ROUTINE_NAME,ROUTINE_DEFINITION,DEFINER from information_schema.ROUTINES where routine_schema in ('%s') and ROUTINE_TYPE='PROCEDURE';", my.Schema)
	//vlog = fmt.Sprintf("(%d) MySQL DB query table query Stored Procedure info exec sql is {%s}", logThreadSeq, sqlStr)
	//global.Wlog.Debug(vlog)
	//sqlRows, err = db.Query(sqlStr)
	//if err != nil {
	//	blog := fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
	//	global.Wlog.Error(blog)
	//	return nil, err
	//}
	//if sqlRows == nil {
	//	return nil, nil
	//}
	//vlog = fmt.Sprintf("(%d) start dispos MySQL DB databases %s create Stored Procedure info.", logThreadSeq, my.Schema)
	//global.Wlog.Debug(vlog)
	//createProc, err := rowDataDisposMap(sqlRows, "Proc", logThreadSeq)
	//dispos := dataDispos.DBdataDispos{DBtype: "MySQL", Logseq: logThreadSeq, SqlRows: sqlRows, Event: "Proc"}
	//dispos = dataDispos.DBdataDispos{DBtype: "MySQL", Logseq: logThreadSeq, SqlRows: sqlRows, Event: "Proc"}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	createProc, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	vlog = fmt.Sprintf("(%d) [%s] Complete the stored procedure information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return procR(createProc, procP(inout, "Proc"), "Proc"), nil
}

/*
	MySQL 存储函数或自定义函数校验
*/
func (my *QueryTable) Func(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		//sqlStr string
		tmpb  = make(map[string]string)
		Event = "Q_Proc"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the stored Func information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select DEFINER,ROUTINE_NAME from information_schema.ROUTINES where routine_schema in ('%s') and ROUTINE_TYPE='FUNCTION';", my.Schema)
	//vlog = fmt.Sprintf("(%d) MySQL DB query table query Stored Function info exec sql is {%s}", logThreadSeq, sqlStr)
	//global.Wlog.Debug(vlog)
	//sqlRows, err := db.Query(sqlStr)
	//if err != nil {
	//	vlog = fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
	//	global.Wlog.Error(vlog)
	//	return nil, err
	//}
	//if sqlRows == nil {
	//	return nil, nil
	//}
	//routineName, err := rowDataDisposMap(sqlRows, "Func", logThreadSeq)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	//dispos := dataDispos.DBdataDispos{DBtype: "MySQL", Logseq: logThreadSeq, SqlRows: sqlRows, Event: "Func"}
	routineName, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for _, v := range routineName {
		strsql = fmt.Sprintf("SHOW CREATE FUNCTION %s.%s;", my.Schema, v["ROUTINE_NAME"])
		//vlog = fmt.Sprintf("(%d) MySQL DB query create Stored Function databases %s info, exec sql is {%s}", logThreadSeq, my.Schema, sqlStr)
		//global.Wlog.Debug(vlog)
		//sqlRows, err = db.Query(sqlStr)
		//if err != nil {
		//	vlog = fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		//	global.Wlog.Error(vlog)
		//	return tmpb, err
		//}
		//if sqlRows == nil {
		//	return nil, nil
		//}
		//vlog = fmt.Sprintf("(%d) start dispos MySQL DB databases %s create Stored Function info.", logThreadSeq, my.Schema)
		//global.Wlog.Debug(vlog)
		//createFunc, err1 := rowDataDisposMap(sqlRows, "Func", logThreadSeq)
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			return nil, err
		}
		createFunc, err1 := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err1 != nil {
			return nil, err1
		}
		//vlog = fmt.Sprintf("(%d) MySQL db query databases %s create Stored Function completion.", logThreadSeq, my.Schema)
		//global.Wlog.Debug(vlog)
		//vlog = fmt.Sprintf("(%d) MySQL db query databases %s dispos Stored Function data info. to dispos it ...", logThreadSeq, my.Schema)
		//global.Wlog.Debug(vlog)

		for _, b := range createFunc {
			d := strings.Join(strings.Fields(strings.ReplaceAll(fmt.Sprintf("%s", b["CREATE_FUNCTION"]), "\n", " ")), " ")
			if strings.Contains(strings.ToUpper(d), "BEGIN") && strings.Contains(strings.ToUpper(d), "END") {
				strings.Index(d, "BEGIN")
			}
			tmpb[strings.ToUpper(fmt.Sprintf("%s", v["ROUTINE_NAME"]))] = fmt.Sprintf("%s/*proc*/delimiter $\n%s$\ndelimiter ;\n", v["DEFINER"], b["Create Function"])
		}
		//vlog = fmt.Sprintf("(%d) MySQL db query databases %s Stored Function data completion...", logThreadSeq, my.Schema)
		//global.Wlog.Debug(vlog)
	}
	defer dispos.SqlRows.Close()
	vlog = fmt.Sprintf("(%d) [%s] Complete the stored Func information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	return tmpb, nil
}

/*
	MySQL 外键校验
*/
func (my *QueryTable) Foreign(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		//sqlStr       string
		//vlog         string
		routineNameM = make(map[string]int)
		tmpb         = make(map[string]string)
		Event        = "Q_Foreign"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the Foreign information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select CONSTRAINT_SCHEMA,TABLE_NAME from information_schema.referential_constraints where CONSTRAINT_SCHEMA in ('%s') and TABLE_NAME in ('%s');", my.Schema, my.Table)
	//vlog = fmt.Sprintf("(%d) MySQL DB query table query Foreign info exec sql is {%s}", logThreadSeq, sqlStr)
	//global.Wlog.Debug(vlog)

	//sqlRows, err := db.Query(sqlStr)
	//if err != nil {
	//	vlog = fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
	//	global.Wlog.Error(vlog)
	//	return nil, err
	//}
	//if sqlRows == nil {
	//	return nil, nil
	//}
	//foreignName, err := rowDataDisposMap(sqlRows, "Foreign", logThreadSeq)
	//dispos := dataDispos.DBdataDispos{DBtype: "MySQL", Logseq: logThreadSeq, SqlRows: sqlRows, Event: "Foreign"}
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	foreignName, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
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
		strsql = fmt.Sprintf("SHOW CREATE TABLE %s;", k)
		//vlog = fmt.Sprintf("(%d) MySQL DB query create Foreign table %s.%s info, exec sql is {%s}", logThreadSeq, my.Schema, my.Table, sqlStr)
		//global.Wlog.Debug(vlog)
		//sqlRows, err = db.Query(sqlStr)
		//if err != nil {
		//	vlog = fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		//	global.Wlog.Error(vlog)
		//	tmpb[k] = ""
		//	return tmpb, err
		//}
		//vlog = fmt.Sprintf("(%d) start dispos MySQL DB create table %s.%s create Foreign info.", logThreadSeq, my.Schema, my.Table)
		//global.Wlog.Debug(vlog)
		//createForeign, err1 := rowDataDisposMap(sqlRows, "Foreign", logThreadSeq)
		//dispos = dataDispos.DBdataDispos{DBtype: "MySQL", Logseq: logThreadSeq, SqlRows: sqlRows, Event: "Foreign"}
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			return nil, err
		}
		createForeign, err1 := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err1 != nil {
			return nil, err1
		}
		//vlog = fmt.Sprintf("(%d) MySQL db query table %s.%s create Foreign completion.", logThreadSeq, my.Schema, my.Table)
		//global.Wlog.Debug(vlog)
		//vlog = fmt.Sprintf("(%d) MySQL db query table %s.%s dispos Foreign data info. to dispos it ...", logThreadSeq, my.Schema, my.Table)
		//global.Wlog.Debug(vlog)

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
		//vlog = fmt.Sprintf("(%d) MySQL db query table %s.%s Foreign data completion...", logThreadSeq, my.Schema, my.Table)
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
func (my *QueryTable) Partitions(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		routineNameM = make(map[string]int)
		tmpb         = make(map[string]string)
		Event        = "Q_Partitions"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the Partitions information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select TABLE_SCHEMA,TABLE_NAME from information_schema.partitions where table_schema in ('%s') and TABLE_NAME in ('%s') and PARTITION_NAME <> '';", my.Schema, my.Table)
	//vlog = fmt.Sprintf("(%d) MySQL DB query table query partitions info exec sql is {%s}", logThreadSeq, sqlStr)
	//global.Wlog.Debug(vlog)
	//sqlRows, err := db.Query(sqlStr)
	//if err != nil {
	//	vlog = fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
	//	global.Wlog.Error(vlog)
	//	return nil, err
	//}
	//if sqlRows == nil {
	//	return nil, nil
	//}
	//vlog = fmt.Sprintf("(%d) start dispos MySQL DB query table %s.%s query Partitions info.", logThreadSeq, my.Schema, my.Table)
	//global.Wlog.Debug(vlog)
	//partitionsName, err := rowDataDisposMap(sqlRows, "Partitions", logThreadSeq)
	//dispos := dataDispos.DBdataDispos{DBtype: "MySQL", Logseq: logThreadSeq, SqlRows: sqlRows, Event: "Partitions"}
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	partitionsName, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	//vlog = fmt.Sprintf("(%d) MySQL DB query table %s.%s query Partitions completion.", logThreadSeq, my.Schema, my.Table)
	//global.Wlog.Debug(vlog)

	for _, v := range partitionsName {
		routineNameM[fmt.Sprintf("%s.%s", v["TABLE_SCHEMA"], v["TABLE_NAME"])]++
	}

	for k, _ := range routineNameM {
		strsql = fmt.Sprintf("SHOW CREATE TABLE %s;", k)
		//vlog = fmt.Sprintf("(%d) MySQL DB query create partitions table %s.%s info, exec sql is {%s}", logThreadSeq, my.Schema, my.Table, sqlStr)
		//global.Wlog.Debug(vlog)
		//sqlRows, err = db.Query(sqlStr)
		//if err != nil {
		//	vlog = fmt.Sprintf("(%d) MySQL DB exec sql fail. sql message is {%s} Error info is {%s}.", logThreadSeq, sqlStr, err)
		//	global.Wlog.Error(vlog)
		//	tmpb[k] = ""
		//	return tmpb, err
		//}
		//if sqlRows == nil {
		//	return nil, nil
		//}
		//vlog = fmt.Sprintf("(%d) start dispos MySQL DB create table %s.%s create Partitions info.", logThreadSeq, my.Schema, my.Table)
		//global.Wlog.Debug(vlog)
		////createPartitions, err1 := rowDataDisposMap(sqlRows, "Partitions", logThreadSeq)
		//dispos = dataDispos.DBdataDispos{DBtype: "MySQL", Logseq: logThreadSeq, SqlRows: sqlRows, Event: "Partitions"}
		if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
			return nil, err
		}
		createPartitions, err1 := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err1 != nil {
			return nil, err1
		}
		//vlog = fmt.Sprintf("(%d) MySQL db query table %s.%s create Partitions completion.", logThreadSeq, my.Schema, my.Table)
		//global.Wlog.Debug(vlog)
		//vlog = fmt.Sprintf("(%d) MySQL db query table %s.%s dispos Partitions data info. to dispos it ...", logThreadSeq, my.Schema, my.Table)
		//global.Wlog.Debug(vlog)
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
		//vlog = fmt.Sprintf("(%d) MySQL db query table %s.%s partitions data completion...", logThreadSeq, my.Schema, my.Table)
		//global.Wlog.Debug(vlog)
	}
	defer dispos.SqlRows.Close()
	vlog = fmt.Sprintf("(%d) [%s] Complete the Partitions information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(vlog)
	return tmpb, nil
}

func (my *QueryTable) Struct(db *sql.DB) (map[string]string, error) {
	return nil, nil
}
