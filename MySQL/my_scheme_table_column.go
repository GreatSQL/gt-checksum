package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"strconv"
	"strings"
	"sync"
)

type QueryTable struct {
	Schema                  string
	Table                   string
	IgnoreTable             string
	Db                      *sql.DB
	Datafix                 string
	CaseSensitiveObjectName string
	TmpTableFileName        string
	ColumnName              []string
	ChanrowCount            int
	TableColumn             []map[string]string
	Sqlwhere                string
	ColData                 []map[string]string
	BeginSeq                string
	RowDataCh               int64
	SelectColumn            map[string]string
	// Caching fields to optimize repeated INFORMATION_SCHEMA queries
	columnExistsCache   map[string]bool   // Cache for column existence checks
	allColumnsCache     []string          // Cache for all column names ordered by ORDINAL_POSITION
	columnDataTypeCache map[string]string // Cache for column name to data type mapping
}

var (
	DBType = "MySQL"

	// Global caching for expensive INFORMATION_SCHEMA queries
	// These caches are shared across all QueryTable instances
	// cache key format: schema.table.column for column existence
	// cache key format: schema.table for column lists and data types
	columnExistsGlobalCache   = make(map[string]bool)
	allColumnsGlobalCache     = make(map[string][]string)
	columnDataTypeGlobalCache = make(map[string]string)
	tableColumnGlobalCache    = make(map[string][]map[string]string)      // Cache for complete table column information (fills TableColumn field)
	tableAllColumnGlobalCache = make(map[string][]map[string]interface{}) // Cache for TableAllColumn results
	// Cache for database version information (fills SELECT VERSION() requests)
	// Cache key format: connection identifier
	databaseVersionCache = make(map[string]string)
	// Mutex to protect global caches
	cacheMutex sync.RWMutex

	procP = func(inout []map[string]interface{}, event string) map[string]string {
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

		// 获取环境属性
		var sqlMode, charsetClient, collationConn, dbCollation, definer string
		if len(createProc) > 0 {
			sqlMode = fmt.Sprintf("%s", createProc[0]["SQL_MODE"])
			charsetClient = fmt.Sprintf("%s", createProc[0]["CHARACTER_SET_CLIENT"])
			collationConn = fmt.Sprintf("%s", createProc[0]["COLLATION_CONNECTION"])
			dbCollation = fmt.Sprintf("%s", createProc[0]["DATABASE_COLLATION"])
			definer = fmt.Sprintf("%s", createProc[0]["DEFINER"])
		}

		for _, v := range createProc {
			ROUTINE_DEFINITION := fmt.Sprintf("%s", v["ROUTINE_DEFINITION"])
			ROUTINE_NAME := strings.ToUpper(fmt.Sprintf("%s", v["ROUTINE_NAME"]))
			user := strings.Split(fmt.Sprintf("%s", v["DEFINER"]), "@")[0]
			host := strings.Split(fmt.Sprintf("%s", v["DEFINER"]), "@")[1]

			// 将存储过程的完整定义和属性存储在一个JSON格式的字符串中
			if event == "Proc" {
				// 创建一个包含所有属性的JSON格式字符串，并将其嵌入到存储过程定义中
				// 使用特殊注释格式 /*GT_CHECKSUM_METADATA:...*/，这样不会影响存储过程的执行
				metadataComment := fmt.Sprintf(`/*GT_CHECKSUM_METADATA:{"sql_mode":"%s","character_set_client":"%s","collation_connection":"%s","database_collation":"%s","definer":"%s"}*/`,
					sqlMode, charsetClient, collationConn, dbCollation, definer)

				// 存储完整的存储过程定义，包括环境属性作为注释
				tmpb[ROUTINE_NAME] = fmt.Sprintf("DELIMITER $\n%s\nCREATE DEFINER='%s'@'%s' PROCEDURE %s(%s) %s$ \nDELIMITER ;",
					metadataComment, user, host, ROUTINE_NAME, tmpa[ROUTINE_NAME], ROUTINE_DEFINITION)
			}

			if event == "Func" {
				// 创建一个包含所有属性的JSON格式字符串，并将其嵌入到函数定义中
				metadataComment := fmt.Sprintf(`/*GT_CHECKSUM_METADATA:{"sql_mode":"%s","character_set_client":"%s","collation_connection":"%s","database_collation":"%s","definer":"%s"}*/`,
					sqlMode, charsetClient, collationConn, dbCollation, definer)

				// 存储完整的函数定义，包括环境属性作为注释
				tmpb[ROUTINE_NAME] = fmt.Sprintf("DELIMITER $\n%s\nCREATE DEFINER='%s'@'%s' FUNCTION %s(%s) %s$ \nDELIMITER ;",
					metadataComment, user, host, ROUTINE_NAME, tmpa[ROUTINE_NAME], strings.ReplaceAll(ROUTINE_DEFINITION, "\n", ""))
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
		A      = make(map[string]int)
		Event  = "Q_Schema_Table_List"
		query  string
		logMsg string
		err    error
	)
	excludeSchema := fmt.Sprintf("'information_Schema','performance_Schema','sys','mysql'")
	query = fmt.Sprintf("SELECT TABLE_SCHEMA AS databaseName, TABLE_NAME AS tableName FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN (%s);", excludeSchema)
	logMsg = fmt.Sprintf("(%d) [%s] Start to query the metadata of the %s database and obtain library and table information. SQL: {%s}", logThreadSeq, Event, DBType, query)
	global.Wlog.Debug(logMsg)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for i := range tableData {
		var ga string
		gd, gt := fmt.Sprintf("%v", tableData[i]["databaseName"]), fmt.Sprintf("%v", tableData[i]["tableName"])
		if strings.ToLower(my.CaseSensitiveObjectName) == "no" {
			gd = strings.ToLower(gd)
			gt = strings.ToLower(gt)
		}
		ga = fmt.Sprintf("%v/*schema&table*/%v", gd, gt)
		A[ga]++
	}
	logMsg = fmt.Sprintf("(%d) [%s] Complete the library and table information query of the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	defer dispos.SqlRows.Close()
	return A, nil
}

/*
MySQL 通过查询表的元数据信息获取列名
*/
func (my *QueryTable) TableColumnName(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var (
		Event  = "Q_table_columns"
		query  string
		logMsg string
		err    error
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start querying the metadata information of table %s.%s in the %s database and get all the column names", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	query = fmt.Sprintf("SELECT COLUMN_NAME AS columnName, COLUMN_TYPE AS columnType, IS_NULLABLE AS isNull, CHARACTER_SET_NAME AS charset, COLLATION_NAME AS collationName, COLUMN_COMMENT AS columnComment, COLUMN_DEFAULT AS columnDefault, EXTRA AS extra FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY ORDINAL_POSITION", my.Schema, my.Table)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	logMsg = fmt.Sprintf("(%d) [%s] Complete the acquisition of all column names in the following table %s.%s of the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	defer dispos.SqlRows.Close()
	return tableData, err
}

/*
MySQL 获取表的注释信息
*/
func (my *QueryTable) TableComment(db *sql.DB, logThreadSeq int64) (string, error) {
	var (
		Event  = "Q_Table_Comment"
		query  string
		logMsg string
		err    error
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start to query the comment of table %s.%s in the %s database", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	query = fmt.Sprintf("SELECT TABLE_COMMENT AS tableComment FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s';", my.Schema, my.Table)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return "", err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return "", err
	}

	comment := ""
	if len(tableData) > 0 {
		comment = fmt.Sprintf("%s", tableData[0]["tableComment"])
	}

	logMsg = fmt.Sprintf("(%d) [%s] Complete the comment query of table %s.%s in the %s database: %s", logThreadSeq, Event, my.Schema, my.Table, DBType, comment)
	global.Wlog.Debug(logMsg)
	defer dispos.SqlRows.Close()
	return comment, nil
}

/*
MySQL 查询数据库版本信息
*/
func (my *QueryTable) DatabaseVersion(db *sql.DB, logThreadSeq int64) (string, error) {
	var (
		version string
		rows    *sql.Rows
		Event   = "Q_M_Versions"
		query   string
		logMsg  string
		err     error
	)

	cacheKey := getDBScopeKey(db)

	// Try to get cached version first
	cacheMutex.RLock()
	if cachedVersion, ok := databaseVersionCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		//kvlog := fmt.Sprintf("(%d) [%s] Using cached version information for database connection %p: %s", logThreadSeq, Event, db, cachedVersion)
		//kglobal.Wlog.Debug(vlog)
		return cachedVersion, nil
	}
	cacheMutex.RUnlock()

	// Cache miss, execute the query
	logMsg = fmt.Sprintf("(%d) [%s] Start querying the version information of the %s database", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	query = fmt.Sprintf("SELECT VERSION() AS VERSION")
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if rows, err = dispos.DBSQLforExec(query); err != nil {
		return "", err
	}
	defer rows.Close()

	dispos.SqlRows = rows
	a, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return "", err
	}
	if len(a) == 0 {
		return "", nil
	}
	for _, i := range a {
		if cc, ok := i["VERSION"]; ok {
			version = fmt.Sprintf("%v", cc)
			break
		}
	}

	// Cache the version information for future use
	cacheMutex.Lock()
	databaseVersionCache[cacheKey] = version
	cacheMutex.Unlock()

	//vlog = fmt.Sprintf("(%d) [%s] Complete the version information query of the %s database and cached version: %s", logThreadSeq, Event, DBType, version)
	//global.Wlog.Debug(vlog)

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
		query       string
		logMsg      string
		err         error
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

	logMsg = fmt.Sprintf("(%d) [%s] The permissions that the current %s DB needs to check is message {%v}, to check it...", logThreadSeq, Event, DBType, globalPri)
	global.Wlog.Debug(logMsg)
	var globalPriS []string
	for k, _ := range globalPri {
		globalPriS = append(globalPriS, k)
	}
	//获取当前匹配的用户
	query = fmt.Sprintf("SELECT CURRENT_USER() AS user;")
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if rows, err = dispos.DBSQLforExec(query); err != nil {
		return false, err
	}
	dispos.SqlRows = rows
	CC, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return false, err
	}

	currentUser = fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%s", CC[0]["user"]), "@", "'@'"))
	logMsg = fmt.Sprintf("(%d) [%s] The user account corresponding to the currently connected %s DB user is message {%s}", logThreadSeq, Event, DBType, currentUser)
	global.Wlog.Debug(logMsg)

	//查找全局权限 类似于grant all privileges on *.* 或 grant select on *.*
	logMsg = fmt.Sprintf("(%d) [%s] Query the current %s DB global dynamic grants permission, to query it...", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	query = fmt.Sprintf("SELECT PRIVILEGE_TYPE AS privileges FROM INFORMATION_SCHEMA.USER_PRIVILEGES WHERE PRIVILEGE_TYPE IN('%s') AND GRANTEE=\"%s\";", strings.Join(globalPriS, "','"), currentUser)
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
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
		logMsg = fmt.Sprintf("(%d) [%s] The current global access user with permission to connect to %s DB is normal and can be verified normally...", logThreadSeq, Event, DBType)
		global.Wlog.Debug(logMsg)
		return true, nil
	}
	if _, ok := globalPri["SESSION_VARIABLES_ADMIN"]; ok && strings.HasPrefix(version, "8.") {
		logMsg = fmt.Sprintf("(%d) [%s] The current user connecting to %s DB lacks \"session_variables_admin\" permission, and the check table is empty", logThreadSeq, Event, DBType)
		global.Wlog.Error(logMsg)
		return false, nil
	}
	if _, ok := globalPri["REPLICATION CLIENT"]; ok {
		logMsg = fmt.Sprintf("(%d) [%s] The current user connecting to %s DB lacks \"REPLICATION CLIENT\" permission, and the check table is empty", logThreadSeq, Event, DBType)
		global.Wlog.Error(logMsg)
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
func (my *QueryTable) TableAccessPriCheck(db *sql.DB, checkTableList []string, datafix string, logThreadSeq int64) (map[string]int, error) {
	var (
		globalPri         = make(map[string]int)
		newCheckTableList = make(map[string]int)
		currentUser       string
		A                 = make(map[string]int)
		PT, abPT          = make(map[string]int), make(map[string]int)
		Event             = "Q_Table_Access_Pri"
		globalPriS        []string
		query             string
		logMsg            string
		err               error
	)

	//针对要校验的库做去重（库级别的）
	globalPri["SELECT"] = 0
	if strings.ToUpper(datafix) == "TABLE" {
		globalPri["INSERT"] = 0
		globalPri["DELETE"] = 0
		globalPri["ALTER"] = 0
	}
	for k, _ := range globalPri {
		globalPriS = append(globalPriS, k)
	}
	logMsg = fmt.Sprintf("(%d) [%s] The permissions that the current %s DB needs to check is message {%v},check table list is {%v}. to check it...", logThreadSeq, Event, DBType, globalPri, newCheckTableList)
	global.Wlog.Debug(logMsg)

	//校验库.表由切片改为map
	for _, AA := range checkTableList {
		newCheckTableList[AA]++
		if my.CaseSensitiveObjectName == "no" {
			newCheckTableList[strings.ToUpper(AA)]++
		}
	}
	//校验库做去重处理
	for _, aa := range checkTableList {
		if strings.Contains(aa, ".") {
			A[strings.Split(aa, ".")[0]]++
			if my.CaseSensitiveObjectName == "no" {
				A[strings.ToUpper(strings.Split(aa, ".")[0])]++
			}
		}
	}
	//获取当前匹配的用户
	query = fmt.Sprintf("SELECT CURRENT_USER() AS user;")
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, err
	}
	CC, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	currentUser = fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%s", CC[0]["user"]), "@", "'@'"))
	//查找全局权限 类似于grant all privileges on *.* 或 grant select on *.*
	logMsg = fmt.Sprintf("(%d) [%s] Query the current %s DB global dynamic grants permission, to query it...", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	query = fmt.Sprintf("SELECT PRIVILEGE_TYPE AS privileges FROM INFORMATION_SCHEMA.USER_PRIVILEGES WHERE PRIVILEGE_TYPE IN('%s') AND GRANTEE=\"%s\";", strings.Join(globalPriS, "','"), currentUser)
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
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
		logMsg = fmt.Sprintf("(%d) [%s] The %s DB table information that meets the permissions and needs to be verified is {%v}...", logThreadSeq, Event, DBType, newCheckTableList)
		global.Wlog.Debug(logMsg)
		return newCheckTableList, nil
	}

	//查询当前库的权限
	//类似于grant all privileges on pcms.* 或 grant select on pcms.*
	logMsg = fmt.Sprintf("(%d) [%s] Query the current %s DB global dynamic schema permission, to query it...", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	for AC, _ := range A {
		var cc []string
		var intseq int
		query = fmt.Sprintf("SELECT TABLE_SCHEMA AS databaseName, PRIVILEGE_TYPE AS privileges FROM INFORMATION_SCHEMA.SCHEMA_PRIVILEGES WHERE PRIVILEGE_TYPE IN('%s') AND TABLE_SCHEMA='%s' AND GRANTEE=\"%s\";", strings.Join(globalPriS, "','"), AC, currentUser)
		if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
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
		logMsg = fmt.Sprintf("(%d) [%s] The %s DB table information that meets the permissions and needs to be verified is {%v}...", logThreadSeq, Event, DBType, newCheckTableList)
		global.Wlog.Debug(logMsg)
		return newCheckTableList, nil
	}
	//vlog = fmt.Sprintf("(%d) MySQL DB library level permissions are not satisfied with {%v}", logThreadSeq, A)
	//global.Wlog.Debug(vlog)
	//查询当前表的权限
	//类似于grant all privileges on pcms.a 或 grant select on pcms.a
	logMsg = fmt.Sprintf("(%d) [%s] Query the current %s DB global dynamic table permission, to query it...", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	//遍历没有schema pri权限的剩余库
	var DM = make(map[string]int)
	for _, D := range checkTableList {
		DM[D]++
		if my.CaseSensitiveObjectName == "no" {
			DM[strings.ToUpper(D)]++
		}
	}
	for B, _ := range A {
		//按照每个库，查询table pri权限
		query = fmt.Sprintf("SELECT TABLE_NAME AS tableName, PRIVILEGE_TYPE AS privileges FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES WHERE PRIVILEGE_TYPE IN('%s') AND TABLE_SCHEMA='%s' AND GRANTEE=\"%s\";", strings.Join(globalPriS, "','"), B, currentUser)
		if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
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
			var E string
			// 无论CaseSensitiveObjectName设置如何，都保持原始大小写
			E = fmt.Sprintf("%s.%s", B, C["tableName"])
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
	logMsg = fmt.Sprintf("(%d) [%s] The %s DB table information that needs to be verified to meet the permissions is {%v}, and the information that is not satisfied is {%v}...", logThreadSeq, Event, DBType, PT, abPT)
	global.Wlog.Debug(logMsg)
	return PT, nil
}

/*
MySQL 获取校验表的列信息，包含列名，列序号，列类型
*/
func (my *QueryTable) TableAllColumn(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var (
		Event    = "Q_Table_Column_Metadata"
		err      error
		query    string
		logMsg   string
		cacheKey string
	)

	cacheKey = scopedTableCacheKey(db, my.Schema, my.Table, "tableAllColumn")

	// Check if result is already in global cache
	cacheMutex.RLock()
	if cachedTableAllColumn, ok := tableAllColumnGlobalCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		logMsg = fmt.Sprintf("(%d) [%s] Using cached TableAllColumn information for table %s.%s", logThreadSeq, Event, my.Schema, my.Table)
		global.Wlog.Debug(logMsg)
		return cachedTableAllColumn, nil
	}
	cacheMutex.RUnlock()

	logMsg = fmt.Sprintf("(%d) [%s] Start to query the metadata of all the columns of table %s.%s in the %s database", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	query = fmt.Sprintf("SELECT COLUMN_NAME AS columnName, COLUMN_TYPE AS dataType, ORDINAL_POSITION AS columnSeq, IS_NULLABLE AS isNull, COLUMN_COMMENT AS columnComment FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY ORDINAL_POSITION;", my.Schema, my.Table)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}

	// Cache the result in global cache for future use
	cacheMutex.Lock()
	tableAllColumnGlobalCache[cacheKey] = tableData
	cacheMutex.Unlock()

	logMsg = fmt.Sprintf("(%d) [%s] Complete the metadata query of all columns in table %s.%s in the %s database. Cached results for future use.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
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
		logMsg                                string
	)
	if len(queryData) == 0 {
		return nil
	}
	//索引列处理，联合索引进行列合并
	//去除主键索引列、唯一索引列、普通索引列的所有列明
	logMsg = fmt.Sprintf("(%d) [%s] Start to select the appropriate index column in the following table %s.%s of the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
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
	logMsg = fmt.Sprintf("(%d) MySQL DB primary key index starts to choose the best.", logThreadSeq)
	global.Wlog.Debug(logMsg)
	if len(PriIndexCol) == 1 { //单列主键索引
		indexChoice["pri_single"] = PriIndexCol
	} else if len(PriIndexCol) > 1 { //联合主键索引
		indexChoice["pri_multiseriate"] = PriIndexCol
	}
	logMsg = fmt.Sprintf("(%d) MySQL DB unique key index starts to choose the best.", logThreadSeq)
	global.Wlog.Debug(logMsg)
	g := my.keyChoiceDispos(nultiseriateIndexColumnMap, "uni")
	for k, v := range g {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	f := my.keyChoiceDispos(multiseriateIndexColumnMap, "mul")
	for k, v := range f {
		if len(v) > 0 {
			indexChoice[k] = v
		}
	}
	logMsg = fmt.Sprintf("(%d) [%s] Complete the selection of the appropriate index column in the following table %s.%s of the %s database.", logThreadSeq, Event, my.Schema, my.Table, DBType)
	global.Wlog.Debug(logMsg)
	return indexChoice
}

/*
MySQL 查询触发器信息
*/
func (my *QueryTable) Trigger(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		tmpb   = make(map[string]string)
		Event  = "Q_Trigger"
		query  string
		logMsg string
		err    error
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start to query the trigger information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	query = fmt.Sprintf("SELECT TRIGGER_NAME AS triggerName, EVENT_OBJECT_TABLE AS tableName FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA IN('%s');", my.Schema)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, err
	}
	triggerName, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for _, v := range triggerName {
		query = fmt.Sprintf("SHOW CREATE TRIGGER %s.%s", my.Schema, v["triggerName"])
		if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
			return nil, err
		}
		createTrigger, err1 := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err1 != nil {
			return nil, err1
		}
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
		logMsg = fmt.Sprintf("(%d) MySQL db query databases %s Trigger data completion...", logThreadSeq, my.Schema)
		global.Wlog.Debug(logMsg)
	}
	logMsg = fmt.Sprintf("(%d) [%s] Complete the trigger information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	defer dispos.SqlRows.Close()
	return tmpb, nil
}

/*
MySQL 存储过程和函数统一校验（新增）
- 一次性从 INFORMATION_SCHEMA.PARAMETERS 与 INFORMATION_SCHEMA.ROUTINES 查询
- 按 ROUTINE_TYPE 将结果分别组装为 PROCEDURE / FUNCTION 的定义文本
- 返回 routines 与 types 两张表，供上层或兼容包装使用
*/
func (my *QueryTable) Routine(db *sql.DB, logThreadSeq int64) (map[string]string, map[string]string, error) {
	var (
		routines = make(map[string]string) // name -> body
		types    = make(map[string]string) // name -> "PROCEDURE"/"FUNCTION"
		Event    = "Q_Routine"
		query    string
		logMsg   string
		err      error
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start to query PROCEDURE and FUNCTION information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)

	// 1) 查询参数：同时取 PROCEDURE 与 FUNCTION
	query = fmt.Sprintf("SELECT SPECIFIC_SCHEMA, SPECIFIC_NAME, ROUTINE_TYPE, ORDINAL_POSITION, PARAMETER_MODE, PARAMETER_NAME, DTD_IDENTIFIER FROM INFORMATION_SCHEMA.PARAMETERS WHERE SPECIFIC_SCHEMA IN('%s') AND ROUTINE_TYPE IN('PROCEDURE','FUNCTION') ORDER BY SPECIFIC_NAME, ORDINAL_POSITION;", my.Schema)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, nil, err
	}
	inoutAll, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, nil, err
	}

	// 拆分参数到 Proc/Func 两组
	var inoutProc, inoutFunc []map[string]interface{}
	for _, r := range inoutAll {
		if strings.EqualFold(fmt.Sprintf("%s", r["ROUTINE_TYPE"]), "PROCEDURE") {
			inoutProc = append(inoutProc, r)
		} else if strings.EqualFold(fmt.Sprintf("%s", r["ROUTINE_TYPE"]), "FUNCTION") {
			inoutFunc = append(inoutFunc, r)
		}
	}
	tmpaProc := procP(inoutProc, "Proc")
	tmpaFunc := procP(inoutFunc, "Func")

	// 2) 从 ROUTINES 取定义与属性，并带出 ROUTINE_TYPE
	query = fmt.Sprintf("SELECT ROUTINE_NAME, ROUTINE_DEFINITION, DEFINER, SQL_MODE, CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION, ROUTINE_TYPE FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA='%s' AND ROUTINE_TYPE IN('PROCEDURE','FUNCTION');", my.Schema)
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, nil, err
	}
	createAll, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, nil, err
	}
	defer dispos.SqlRows.Close()

	// 拆分 ROUTINES 到 Proc/Func 两组，并用现有 procR 生成定义
	var createProc, createFunc []map[string]interface{}
	for _, r := range createAll {
		if strings.EqualFold(fmt.Sprintf("%s", r["ROUTINE_TYPE"]), "PROCEDURE") {
			createProc = append(createProc, r)
		} else if strings.EqualFold(fmt.Sprintf("%s", r["ROUTINE_TYPE"]), "FUNCTION") {
			createFunc = append(createFunc, r)
		}
	}

	procMap := procR(createProc, tmpaProc, "Proc")
	funcMap := procR(createFunc, tmpaFunc, "Func")

	// 合并并记录类型
	for k, v := range procMap {
		routines[k] = v
		types[k] = "PROCEDURE"
	}
	for k, v := range funcMap {
		routines[k] = v
		types[k] = "FUNCTION"
	}

	logMsg = fmt.Sprintf("(%d) [%s] Complete the PROCEDURE and FUNCTION information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	return routines, types, nil
}

/*
MySQL 存储过程校验
*/
/*
Deprecated: use Routine() instead.
兼容包装：复用 Routine()，仅返回 PROCEDURE。
*/
func (my *QueryTable) Proc(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	routines, types, err := my.Routine(db, logThreadSeq)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string)
	for name, body := range routines {
		if strings.EqualFold(types[name], "PROCEDURE") {
			out[name] = body
		}
	}
	return out, nil
}

/*
MySQL 存储函数或自定义函数校验
*/
/*
Deprecated: use Routine() instead.
兼容包装：复用 Routine()，仅返回 FUNCTION。
*/
func (my *QueryTable) Func(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	routines, types, err := my.Routine(db, logThreadSeq)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string)
	for name, body := range routines {
		if strings.EqualFold(types[name], "FUNCTION") {
			out[name] = body
		}
	}
	return out, nil
}

/*
MySQL 外键校验
*/
func (my *QueryTable) Foreign(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		tmpb   = make(map[string]string)
		Event  = "Q_Foreign"
		query  string
		logMsg string
		err    error
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start to query the Foreign information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)

	// 使用INFORMATION_SCHEMA获取完整的外键约束信息
	// 这个查询会获取外键名称、列名、引用的表和列信息
	query = fmt.Sprintf(`
			SELECT 
				rc.CONSTRAINT_NAME,
			kcu.COLUMN_NAME,
			rc.CONSTRAINT_SCHEMA AS REFERENCED_TABLE_SCHEMA,
			rc.REFERENCED_TABLE_NAME,
			rcu.COLUMN_NAME AS REFERENCED_COLUMN_NAME,
			rc.DELETE_RULE,
			rc.UPDATE_RULE
		FROM 
			INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
		JOIN 
			INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
				ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
				AND rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA 
				AND rc.TABLE_NAME = kcu.TABLE_NAME
		JOIN 
			INFORMATION_SCHEMA.KEY_COLUMN_USAGE rcu 
				ON rc.UNIQUE_CONSTRAINT_NAME = rcu.CONSTRAINT_NAME 
				AND rc.CONSTRAINT_SCHEMA = rcu.TABLE_SCHEMA 
				AND rc.REFERENCED_TABLE_NAME = rcu.TABLE_NAME
		WHERE 
			rc.CONSTRAINT_SCHEMA = '%s' 
			AND rc.TABLE_NAME = '%s'
		ORDER BY 
			rc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
		`, my.Schema, my.Table)

	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		logMsg = fmt.Sprintf("(%d) [%s] Error executing foreign key query: %v", logThreadSeq, Event, err)
		global.Wlog.Error(logMsg)
		return nil, err
	}

	foreignKeys, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		logMsg = fmt.Sprintf("(%d) [%s] Error processing foreign key results: %v", logThreadSeq, Event, err)
		global.Wlog.Error(logMsg)
		return nil, err
	}
	defer dispos.SqlRows.Close()

	// 按约束名称分组外键信息
	fkMap := make(map[string][]map[string]interface{})
	for _, fk := range foreignKeys {
		constraintName := fmt.Sprintf("%s", fk["CONSTRAINT_NAME"])
		if _, exists := fkMap[constraintName]; !exists {
			fkMap[constraintName] = []map[string]interface{}{}
		}
		fkMap[constraintName] = append(fkMap[constraintName], fk)
	}

	// 构建完整的外键DDL定义
	for constraintName, fkInfos := range fkMap {
		if len(fkInfos) == 0 {
			continue
		}

		// 获取第一个外键信息作为基础
		firstFk := fkInfos[0]
		referencedSchema := fmt.Sprintf("%s", firstFk["REFERENCED_TABLE_SCHEMA"])
		referencedTable := fmt.Sprintf("%s", firstFk["REFERENCED_TABLE_NAME"])
		deleteRule := fmt.Sprintf("%s", firstFk["DELETE_RULE"])
		updateRule := fmt.Sprintf("%s", firstFk["UPDATE_RULE"])

		// 收集列信息
		var sourceColumns []string
		var referencedColumns []string
		for _, fkInfo := range fkInfos {
			sourceColumns = append(sourceColumns, fmt.Sprintf("!%s!", fkInfo["COLUMN_NAME"]))
			referencedColumns = append(referencedColumns, fmt.Sprintf("!%s!", fkInfo["REFERENCED_COLUMN_NAME"]))
		}

		// 构建外键DDL
		sourceColumnsStr := strings.Join(sourceColumns, ", ")
		referencedColumnsStr := strings.Join(referencedColumns, ", ")
		ddl := fmt.Sprintf("CONSTRAINT !%s! FOREIGN KEY (!%s!) REFERENCES !%s!.!%s! (!%s!)",
			constraintName, sourceColumnsStr, referencedSchema, referencedTable, referencedColumnsStr)

		// 添加删除和更新规则
		if deleteRule != "NO ACTION" && deleteRule != "RESTRICT" {
			ddl += " ON DELETE " + deleteRule
		}
		if updateRule != "NO ACTION" && updateRule != "RESTRICT" {
			ddl += " ON UPDATE " + updateRule
		}

		// 存储到结果map中，使用大写并将反引号替换为感叹号
		tableKey := fmt.Sprintf("%s.%s", my.Schema, my.Table)
		tmpb[tableKey] = strings.ToUpper(ddl)

		logMsg = fmt.Sprintf("(%d) [%s] Found foreign key: %s", logThreadSeq, Event, ddl)
		global.Wlog.Debug(logMsg)
	}

	logMsg = fmt.Sprintf("(%d) [%s] Complete the Foreign information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	return tmpb, nil
}

/*
分区表校验
*/
func (my *QueryTable) Partitions(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		tmpb   = make(map[string]string)
		Event  = "Q_Partitions"
		err    error
		logMsg string
		query  string
	)

	// 正确提取表名，避免表名中包含schema信息
	actualTableName := my.Table
	if strings.Contains(actualTableName, ":") {
		parts := strings.Split(actualTableName, ":")
		if len(parts) > 0 {
			actualTableName = parts[0]
		}
	}

	logMsg = fmt.Sprintf("(%d) [%s] Start to query the Partitions information for table %s.%s under the %s database.", logThreadSeq, Event, my.Schema, actualTableName, DBType)
	global.Wlog.Debug(logMsg)

	// 直接查询表的分区信息，包括分区名称和详细定义
	query = fmt.Sprintf("SELECT PARTITION_NAME, PARTITION_ORDINAL_POSITION, PARTITION_METHOD, PARTITION_EXPRESSION, PARTITION_DESCRIPTION, TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND PARTITION_NAME<>'' ORDER BY PARTITION_ORDINAL_POSITION;", my.Schema, actualTableName)
	logMsg = fmt.Sprintf("(%d) [%s] Executing query on INFORMATION_SCHEMA.PARTITIONS: %s", logThreadSeq, Event, query)
	global.Wlog.Debug(logMsg)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, err
	}
	partitionsInfo, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}

	// 如果有分区，获取表的创建语句以提取完整的分区定义
	if len(partitionsInfo) > 0 {
		query = fmt.Sprintf("SHOW CREATE TABLE %s.%s;", my.Schema, actualTableName)
		if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
			return nil, err
		}
		createTableInfo, err1 := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err1 != nil {
			return nil, err1
		}

		if len(createTableInfo) > 0 {
			createTableSQL := fmt.Sprintf("%s", createTableInfo[0]["Create Table"])
			z := strings.Split(createTableSQL, "\n")

			// 提取分区定义信息 - 改进版本，确保捕获完整的分区定义
			var partitionDefs []string
			inPartitionSection := false

			for _, bi := range z {
				trimmedLine := strings.TrimSpace(bi)
				upperLine := strings.ToUpper(trimmedLine)

				// 检测分区定义开始 - 支持PARTITION BY和SUBPARTITION BY
				if strings.Contains(upperLine, "PARTITION BY") || strings.Contains(upperLine, "SUBPARTITION BY") {
					inPartitionSection = true
					partitionDefs = append(partitionDefs, upperLine)
				} else if inPartitionSection {
					// 收集分区定义部分的所有行，直到遇到结束括号或引擎定义
					if trimmedLine != "" && !strings.HasPrefix(upperLine, "ENGINE=") && !strings.HasPrefix(upperLine, "DEFAULT CHARSET") {
						partitionDefs = append(partitionDefs, upperLine)
					}
					// 分区定义结束 - 确保我们不会提前退出
					if strings.Contains(upperLine, ");") {
						inPartitionSection = false
						break
					}
				}
			}

			// 将所有分区定义合并为一个字符串作为表的分区定义
			// 移除所有空格，使比较更加严格和准确
			fullPartitionDef := strings.Join(partitionDefs, " ")
			fullPartitionDef = strings.Join(strings.Fields(fullPartitionDef), " ")
			fullPartitionDef = strings.ReplaceAll(fullPartitionDef, "`", "!")

			// 增加日志，记录完整的分区定义用于调试
			logMsg = fmt.Sprintf("(%d) [%s] Extracted full partition definition for %s.%s: %s", logThreadSeq, Event, my.Schema, actualTableName, fullPartitionDef)
			global.Wlog.Debug(logMsg)

			// 使用表名作为键，存储完整的分区定义
			tableKey := fmt.Sprintf("%s.%s", my.Schema, my.Table)
			tmpb[tableKey] = fullPartitionDef

			// 同时为每个分区单独创建条目，便于比较
			for _, p := range partitionsInfo {
				partitionName := fmt.Sprintf("%s", p["PARTITION_NAME"])
				partitionKey := fmt.Sprintf("%s.%s.%s", my.Schema, my.Table, partitionName)
				// 存储分区的详细信息，包括所有分区属性
				partitionDetails := fmt.Sprintf("NAME=%s,ORDINAL=%s,METHOD=%s,EXPRESSION=%s,DESCRIPTION=%s,ROWS=%s",
					partitionName,
					p["PARTITION_ORDINAL_POSITION"],
					p["PARTITION_METHOD"],
					p["PARTITION_EXPRESSION"],
					p["PARTITION_DESCRIPTION"],
					p["TABLE_ROWS"])
				tmpb[partitionKey] = partitionDetails
				logMsg = fmt.Sprintf("(%d) [%s] Stored partition %s details: %s", logThreadSeq, Event, partitionKey, partitionDetails)
				global.Wlog.Debug(logMsg)
			}
		}
	}

	defer dispos.SqlRows.Close()
	logMsg = fmt.Sprintf("(%d) [%s] Complete the Partitions information query for table %s.%s under the %s database. Found %d partitions.", logThreadSeq, Event, my.Schema, actualTableName, DBType, len(partitionsInfo))
	global.Wlog.Debug(logMsg)
	return tmpb, nil
}

func (my *QueryTable) Struct(db *sql.DB) (map[string]string, error) {
	return nil, nil
}
