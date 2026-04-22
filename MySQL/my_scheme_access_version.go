package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"strings"
)

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
	if global.DetectDatabaseFlavor(version) == global.DatabaseFlavorMariaDB {
		logMsg = fmt.Sprintf("(%d) [%s] Skip global privilege precheck for %s DB version %s; current MariaDB source path does not require MySQL-specific global privilege names", logThreadSeq, Event, DBType, version)
		global.Wlog.Info(logMsg)
		return true, nil
	}
	normalizedCheckObject := strings.ToLower(strings.TrimSpace(global.CurrentCheckObject))
	requireSessionVariablesAdmin := strings.HasPrefix(version, "8.") && (normalizedCheckObject == "" || normalizedCheckObject == "data")
	if requireSessionVariablesAdmin {
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
	if _, ok := globalPri["SESSION_VARIABLES_ADMIN"]; ok && requireSessionVariablesAdmin {
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
