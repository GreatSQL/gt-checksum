package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"sort"
	"strings"
)

func normalizePrivilegeAccessRole(accessRole string) string {
	normalizedAccessRole := strings.ToLower(strings.TrimSpace(accessRole))
	if normalizedAccessRole == "" {
		return "unknown"
	}
	return normalizedAccessRole
}

func formatPrivilegeAccessRoleForLog(accessRole string) string {
	return fmt.Sprintf("[%s]", normalizePrivilegeAccessRole(accessRole))
}

func normalizePrivilegeCheckObject(checkObject string) string {
	normalizedCheckObject := strings.ToLower(strings.TrimSpace(checkObject))
	switch normalizedCheckObject {
	case "":
		return "data"
	case "proc", "procedure", "func", "function":
		return "routine"
	default:
		return normalizedCheckObject
	}
}

func sortedPrivilegeKeys(privileges map[string]int) []string {
	privilegeKeys := make([]string, 0, len(privileges))
	for privilege := range privileges {
		privilegeKeys = append(privilegeKeys, privilege)
	}
	sort.Strings(privilegeKeys)
	return privilegeKeys
}

func mysqlGlobalGrantSQL(privileges []string, currentUser string) string {
	if len(privileges) == 0 || currentUser == "" {
		return ""
	}
	return fmt.Sprintf("GRANT %s ON *.* TO %s;", strings.Join(privileges, ", "), currentUser)
}

func mysqlQuoteIdentifier(identifier string) string {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return "``"
	}
	if identifier == "*" {
		return identifier
	}
	return fmt.Sprintf("`%s`", strings.ReplaceAll(identifier, "`", "``"))
}

func mysqlTableGrantSQL(privileges []string, tableName, currentUser string) string {
	if len(privileges) == 0 || tableName == "" || currentUser == "" {
		return ""
	}
	objectName := mysqlQuoteIdentifier(tableName)
	if strings.Contains(tableName, ".") {
		parts := strings.SplitN(tableName, ".", 2)
		objectName = fmt.Sprintf("%s.%s", mysqlQuoteIdentifier(parts[0]), mysqlQuoteIdentifier(parts[1]))
	}
	return fmt.Sprintf("GRANT %s ON %s TO %s;", strings.Join(privileges, ", "), objectName, currentUser)
}

func mysqlVersionSupportsShowRoutine(version string) bool {
	versionInfo, err := global.ParseMySQLVersion(version)
	if err != nil || versionInfo.Flavor != global.DatabaseFlavorMySQL {
		return false
	}
	if versionInfo.Major > 8 {
		return true
	}
	if versionInfo.Major != 8 {
		return false
	}
	return versionInfo.Minor > 0 || versionInfo.Patch >= 20
}

func mysqlVersionRequiresGlobalSelectForRoutine(version string) bool {
	versionInfo, err := global.ParseMySQLVersion(version)
	if err != nil || versionInfo.Flavor != global.DatabaseFlavorMySQL {
		return false
	}
	return versionInfo.Major == 8 && versionInfo.Minor == 0 && versionInfo.Patch < 20
}

func mysqlRequiredTablePrivileges(checkObject, datafix, accessRole string) map[string]int {
	requiredPrivileges := make(map[string]int)
	normalizedCheckObject := normalizePrivilegeCheckObject(checkObject)
	normalizedAccessRole := normalizePrivilegeAccessRole(accessRole)
	isDestTableFix := strings.ToUpper(strings.TrimSpace(datafix)) == "TABLE" && normalizedAccessRole != "source" && normalizedAccessRole != "src"

	switch normalizedCheckObject {
	case "data":
		requiredPrivileges["SELECT"] = 0
		if isDestTableFix {
			requiredPrivileges["INSERT"] = 0
			requiredPrivileges["DELETE"] = 0
		}
	case "struct":
		requiredPrivileges["SELECT"] = 0
		if isDestTableFix {
			requiredPrivileges["ALTER"] = 0
		}
	case "trigger":
		requiredPrivileges["TRIGGER"] = 0
	case "routine":
		// Routine definition visibility is version-specific and may require a
		// global dynamic privilege (SHOW_ROUTINE) or SELECT on mysql.proc.
		// It is handled by mysqlRoutineDefinitionAccessPriCheck instead of the
		// generic schema/table privilege path.
	default:
		requiredPrivileges["SELECT"] = 0
	}
	return requiredPrivileges
}

func mysqlAddObjectPrivilege(privileges map[string]map[string]int, objectName, privilege string) {
	objectName = strings.TrimSpace(objectName)
	privilege = strings.ToUpper(strings.TrimSpace(privilege))
	if objectName == "" || privilege == "" {
		return
	}
	if _, ok := privileges[objectName]; !ok {
		privileges[objectName] = make(map[string]int)
	}
	privileges[objectName][privilege]++
}

func mysqlHasObjectPrivilege(privileges map[string]map[string]int, objectName, privilege string) bool {
	privilege = strings.ToUpper(strings.TrimSpace(privilege))
	for _, key := range []string{objectName, strings.ToUpper(objectName)} {
		if grantedPrivileges, ok := privileges[key]; ok {
			if _, granted := grantedPrivileges[privilege]; granted {
				return true
			}
		}
	}
	return false
}

func mysqlMissingTablePrivileges(checkTableList, requiredPrivileges []string, globalGranted map[string]int, schemaGranted, tableGranted map[string]map[string]int) map[string][]string {
	missingByTable := make(map[string][]string)
	for _, tableName := range checkTableList {
		tableName = strings.TrimSpace(tableName)
		if tableName == "" {
			continue
		}
		schemaName := ""
		if dotIndex := strings.Index(tableName, "."); dotIndex >= 0 {
			schemaName = tableName[:dotIndex]
		}
		for _, privilege := range requiredPrivileges {
			privilege = strings.ToUpper(strings.TrimSpace(privilege))
			if _, granted := globalGranted[privilege]; granted {
				continue
			}
			if schemaName != "" && mysqlHasObjectPrivilege(schemaGranted, schemaName, privilege) {
				continue
			}
			if mysqlHasObjectPrivilege(tableGranted, tableName, privilege) {
				continue
			}
			missingByTable[tableName] = append(missingByTable[tableName], privilege)
		}
		if len(missingByTable[tableName]) > 0 {
			sort.Strings(missingByTable[tableName])
		}
	}
	return missingByTable
}

func mysqlFormatMissingTablePrivilegeDetails(missingByTable map[string][]string, currentUser string) []string {
	if len(missingByTable) == 0 {
		return nil
	}
	tableNames := make([]string, 0, len(missingByTable))
	for tableName := range missingByTable {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)
	details := make([]string, 0, len(tableNames))
	for _, tableName := range tableNames {
		missingPrivileges := missingByTable[tableName]
		details = append(details, fmt.Sprintf("%s missing %v; grant: %s", tableName, missingPrivileges, mysqlTableGrantSQL(missingPrivileges, tableName, currentUser)))
	}
	return details
}

func mysqlRequiresReplicationClientPrivilege(checkObject string) bool {
	switch strings.ToLower(strings.TrimSpace(checkObject)) {
	case "binlog", "inc", "increment", "incremental":
		return true
	default:
		return false
	}
}

func mysqlRowsContainPrivilege(rows []map[string]interface{}, privilege string) bool {
	privilege = strings.ToUpper(strings.TrimSpace(privilege))
	for _, row := range rows {
		if strings.ToUpper(fmt.Sprintf("%s", row["privileges"])) == privilege {
			return true
		}
	}
	return false
}

func (my *QueryTable) routineDefinitionAccessPriCheck(db *sql.DB, checkTableList []string, newCheckTableList map[string]int, accessRole string, logThreadSeq int64) (map[string]int, error) {
	const Event = "Q_Table_Access_Pri"
	queryRows := func(query string) ([]map[string]interface{}, error) {
		dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
		var err error
		if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
			return nil, err
		}
		if dispos.SqlRows != nil {
			defer dispos.SqlRows.Close()
		}
		return dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	}

	currentUserRows, err := queryRows("SELECT CURRENT_USER() AS user;")
	if err != nil {
		return nil, err
	}
	if len(currentUserRows) == 0 {
		return nil, fmt.Errorf("current MySQL user is empty")
	}
	currentUser := fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%s", currentUserRows[0]["user"]), "@", "'@'"))

	version, err := my.DatabaseVersion(db, logThreadSeq)
	if err != nil {
		return nil, err
	}
	roleForLog := formatPrivilegeAccessRoleForLog(accessRole)
	global.Wlog.Debug(fmt.Sprintf("(%d) [%s] Checking routine definition privileges for %s role on %s version %s, check table list is {%v}", logThreadSeq, Event, roleForLog, DBType, version, checkTableList))

	queryGlobalPrivileges := func(privileges []string) ([]map[string]interface{}, error) {
		query := fmt.Sprintf("SELECT PRIVILEGE_TYPE AS privileges FROM INFORMATION_SCHEMA.USER_PRIVILEGES WHERE PRIVILEGE_TYPE IN('%s') AND GRANTEE=\"%s\";", strings.Join(privileges, "','"), currentUser)
		return queryRows(query)
	}

	if mysqlVersionSupportsShowRoutine(version) {
		globalPrivilegeRows, err := queryGlobalPrivileges([]string{"SHOW_ROUTINE", "SELECT"})
		if err != nil {
			return nil, err
		}
		if mysqlRowsContainPrivilege(globalPrivilegeRows, "SHOW_ROUTINE") || mysqlRowsContainPrivilege(globalPrivilegeRows, "SELECT") {
			global.Wlog.Debug(fmt.Sprintf("(%d) [%s] Routine definition privilege precheck passed for %s role with SHOW_ROUTINE or global SELECT", logThreadSeq, Event, roleForLog))
			return newCheckTableList, nil
		}
		grantSQL := mysqlGlobalGrantSQL([]string{"SHOW_ROUTINE"}, currentUser)
		global.Wlog.Error(fmt.Sprintf("(%d) [%s] The current user connecting to %s DB for %s role lacks routine definition privilege on MySQL %s. MySQL 8.0.20+ requires SHOW_ROUTINE or global SELECT to read routine definitions. Suggested GRANT statement: %s", logThreadSeq, Event, DBType, roleForLog, version, grantSQL))
		return map[string]int{}, nil
	}

	globalSelectRows, err := queryGlobalPrivileges([]string{"SELECT"})
	if err != nil {
		return nil, err
	}
	if mysqlRowsContainPrivilege(globalSelectRows, "SELECT") {
		global.Wlog.Debug(fmt.Sprintf("(%d) [%s] Routine definition privilege precheck passed for %s role with global SELECT", logThreadSeq, Event, roleForLog))
		return newCheckTableList, nil
	}

	if mysqlVersionRequiresGlobalSelectForRoutine(version) {
		grantSQL := mysqlGlobalGrantSQL([]string{"SELECT"}, currentUser)
		global.Wlog.Error(fmt.Sprintf("(%d) [%s] The current user connecting to %s DB for %s role lacks routine definition privilege on MySQL %s. MySQL 8.0.0-8.0.19 does not support SHOW_ROUTINE and requires global SELECT for routines not defined by the current user. Suggested GRANT statement: %s", logThreadSeq, Event, DBType, roleForLog, version, grantSQL))
		return map[string]int{}, nil
	}

	schemaPrivilegeQuery := fmt.Sprintf("SELECT TABLE_SCHEMA AS databaseName, PRIVILEGE_TYPE AS privileges FROM INFORMATION_SCHEMA.SCHEMA_PRIVILEGES WHERE PRIVILEGE_TYPE='SELECT' AND TABLE_SCHEMA='mysql' AND GRANTEE=\"%s\";", currentUser)
	schemaPrivilegeRows, err := queryRows(schemaPrivilegeQuery)
	if err != nil {
		return nil, err
	}
	if mysqlRowsContainPrivilege(schemaPrivilegeRows, "SELECT") {
		global.Wlog.Debug(fmt.Sprintf("(%d) [%s] Routine definition privilege precheck passed for %s role with SELECT on mysql.*", logThreadSeq, Event, roleForLog))
		return newCheckTableList, nil
	}

	tablePrivilegeQuery := fmt.Sprintf("SELECT TABLE_NAME AS tableName, PRIVILEGE_TYPE AS privileges FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES WHERE PRIVILEGE_TYPE='SELECT' AND TABLE_SCHEMA='mysql' AND TABLE_NAME='proc' AND GRANTEE=\"%s\";", currentUser)
	tablePrivilegeRows, err := queryRows(tablePrivilegeQuery)
	if err != nil {
		return nil, err
	}
	if mysqlRowsContainPrivilege(tablePrivilegeRows, "SELECT") {
		global.Wlog.Debug(fmt.Sprintf("(%d) [%s] Routine definition privilege precheck passed for %s role with SELECT on mysql.proc", logThreadSeq, Event, roleForLog))
		return newCheckTableList, nil
	}

	grantSQL := mysqlTableGrantSQL([]string{"SELECT"}, "mysql.proc", currentUser)
	global.Wlog.Error(fmt.Sprintf("(%d) [%s] The current user connecting to %s DB for %s role lacks routine definition privilege on MySQL %s. MySQL 5.6/5.7 and MariaDB require being the routine definer or having SELECT on mysql.proc for complete routine definitions. Suggested GRANT statement: %s", logThreadSeq, Event, DBType, roleForLog, version, grantSQL))
	return map[string]int{}, nil
}

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
func (my *QueryTable) GlobalAccessPri(db *sql.DB, accessRole string, logThreadSeq int64) (bool, error) {
	var (
		globalPri            = make(map[string]int)
		version              string
		currentUser          string
		rows                 *sql.Rows
		Event                = "Q_Table_Global_Access_Pri"
		query                string
		logMsg               string
		err                  error
		normalizedAccessRole = normalizePrivilegeAccessRole(accessRole)
	)
	roleForLog := formatPrivilegeAccessRoleForLog(normalizedAccessRole)
	//要确定MySQL的版本，5.7和8.0
	if version, err = my.DatabaseVersion(db, logThreadSeq); err != nil {
		return false, err
	}
	if version == "" {
		return false, nil
	}
	if global.DetectDatabaseFlavor(version) == global.DatabaseFlavorMariaDB {
		logMsg = fmt.Sprintf("(%d) [%s] Skip global privilege precheck for %s DB %s role version %s; current MariaDB path does not require MySQL-specific global privilege names", logThreadSeq, Event, DBType, roleForLog, version)
		global.Wlog.Info(logMsg)
		return true, nil
	}
	normalizedCheckObject := strings.ToLower(strings.TrimSpace(global.CurrentCheckObject))
	requireSessionVariablesAdmin := strings.HasPrefix(version, "8.") && (normalizedCheckObject == "" || normalizedCheckObject == "data")
	if requireSessionVariablesAdmin {
		globalPri["SESSION_VARIABLES_ADMIN"] = 0
	}
	//globalPri["FLUSH_TABLES"] = 0
	if mysqlRequiresReplicationClientPrivilege(normalizedCheckObject) {
		globalPri["REPLICATION CLIENT"] = 0
	} else {
		logMsg = fmt.Sprintf("(%d) [%s] Skip REPLICATION CLIENT precheck for %s DB %s role checkObject=%s; current path does not read MySQL replication/binlog status", logThreadSeq, Event, DBType, roleForLog, normalizedCheckObject)
		global.Wlog.Debug(logMsg)
	}

	logMsg = fmt.Sprintf("(%d) [%s] The permissions that the current %s DB needs to check for %s role is message {%v}, to check it...", logThreadSeq, Event, DBType, roleForLog, globalPri)
	global.Wlog.Debug(logMsg)
	if len(globalPri) == 0 {
		logMsg = fmt.Sprintf("(%d) [%s] No required global privileges for the current %s DB %s role checkObject=%s; skip global privilege precheck", logThreadSeq, Event, DBType, roleForLog, normalizedCheckObject)
		global.Wlog.Debug(logMsg)
		return true, nil
	}
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
		logMsg = fmt.Sprintf("(%d) [%s] The current global access user with permission to connect to %s DB for %s role is normal and can be verified normally...", logThreadSeq, Event, DBType, roleForLog)
		global.Wlog.Debug(logMsg)
		return true, nil
	}
	missingPrivileges := sortedPrivilegeKeys(globalPri)
	grantSQL := mysqlGlobalGrantSQL(missingPrivileges, currentUser)
	logMsg = fmt.Sprintf("(%d) [%s] The current user connecting to %s DB for %s role lacks required global privileges %v. Suggested GRANT statement: %s", logThreadSeq, Event, DBType, roleForLog, missingPrivileges, grantSQL)
	global.Wlog.Error(logMsg)
	return false, nil
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
func (my *QueryTable) TableAccessPriCheck(db *sql.DB, checkTableList []string, checkObject, datafix, accessRole string, logThreadSeq int64) (map[string]int, error) {
	var (
		globalPri          = make(map[string]int)
		newCheckTableList  = make(map[string]int)
		currentUser        string
		A                  = make(map[string]int)
		PT, abPT           = make(map[string]int), make(map[string]int)
		Event              = "Q_Table_Access_Pri"
		globalPriS         []string
		requiredPrivileges []string
		globalGranted      = make(map[string]int)
		schemaGranted      = make(map[string]map[string]int)
		tableGranted       = make(map[string]map[string]int)
		query              string
		logMsg             string
		err                error
	)

	// 源端只需要读取权限；目标端按 checkObject/datafix 组合检查实际修复路径需要的权限。
	globalPri = mysqlRequiredTablePrivileges(checkObject, datafix, accessRole)
	normalizedAccessRole := normalizePrivilegeAccessRole(accessRole)
	roleForLog := formatPrivilegeAccessRoleForLog(normalizedAccessRole)
	normalizedCheckObject := normalizePrivilegeCheckObject(checkObject)
	requiredPrivileges = sortedPrivilegeKeys(globalPri)
	globalPriS = append(globalPriS, requiredPrivileges...)

	//校验库.表由切片改为map
	for _, AA := range checkTableList {
		newCheckTableList[AA]++
		if my.CaseSensitiveObjectName == "no" {
			newCheckTableList[strings.ToUpper(AA)]++
		}
	}
	logMsg = fmt.Sprintf("(%d) [%s] The permissions that the current %s DB needs to check for %s role and checkObject=%s is message {%v}, check table list is {%v}. to check it...", logThreadSeq, Event, DBType, roleForLog, normalizedCheckObject, globalPri, checkTableList)
	global.Wlog.Debug(logMsg)
	if normalizedCheckObject == "routine" {
		return my.routineDefinitionAccessPriCheck(db, checkTableList, newCheckTableList, normalizedAccessRole, logThreadSeq)
	}
	if len(requiredPrivileges) == 0 {
		logMsg = fmt.Sprintf("(%d) [%s] No table-level privileges need to be checked for %s role and checkObject=%s.", logThreadSeq, Event, roleForLog, normalizedCheckObject)
		global.Wlog.Debug(logMsg)
		return newCheckTableList, nil
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
		privilege := strings.ToUpper(fmt.Sprintf("%s", gd["privileges"]))
		if _, ok := globalPri[privilege]; ok {
			globalGranted[privilege]++
			delete(globalPri, privilege)
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
			privilege := strings.ToUpper(fmt.Sprintf("%s", ab["privileges"]))
			cc = append(cc, privilege)
			mysqlAddObjectPrivilege(schemaGranted, AC, privilege)
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
			privilege := strings.ToUpper(fmt.Sprintf("%s", C["privileges"]))
			mysqlAddObjectPrivilege(tableGranted, E, privilege)
			if E != N {
				N = E
				dd = []string{}
				dd = append(dd, privilege)
			} else {
				dd = append(dd, privilege)
			}
			cc[N] = dd
		}
		//判断权限表
		//判断当前表的所有权限是否包全部包含（指定权限）
		for k, v := range cc {
			if _, ok := DM[k]; ok {
				matchedPrivilegeCount := 0
				for D := range globalPri {
					if strings.Index(strings.Join(v, ","), D) == -1 {
						abPT[k]++
					} else {
						matchedPrivilegeCount++
					}
				}
				if matchedPrivilegeCount == len(globalPri) {
					PT[k]++
				}
			}
		}
	}
	missingByTable := mysqlMissingTablePrivileges(checkTableList, requiredPrivileges, globalGranted, schemaGranted, tableGranted)
	combinedPT := make(map[string]int)
	combinedAbPT := make(map[string]int)
	for _, tableName := range checkTableList {
		tableName = strings.TrimSpace(tableName)
		if tableName == "" {
			continue
		}
		if missingPrivileges, missing := missingByTable[tableName]; missing && len(missingPrivileges) > 0 {
			combinedAbPT[tableName]++
			if my.CaseSensitiveObjectName == "no" {
				combinedAbPT[strings.ToUpper(tableName)]++
			}
			continue
		}
		combinedPT[tableName]++
		if my.CaseSensitiveObjectName == "no" {
			combinedPT[strings.ToUpper(tableName)]++
		}
	}
	if len(missingByTable) > 0 {
		missingDetails := mysqlFormatMissingTablePrivilegeDetails(missingByTable, currentUser)
		logMsg = fmt.Sprintf("(%d) [%s] The current user connecting to %s DB for %s role requires table privileges %v. Missing table privilege details: %s", logThreadSeq, Event, DBType, roleForLog, requiredPrivileges, strings.Join(missingDetails, " | "))
		global.Wlog.Error(logMsg)
	}
	logMsg = fmt.Sprintf("(%d) [%s] The %s DB table information that needs to be verified to meet the permissions is {%v}, and the information that is not satisfied is {%v}...", logThreadSeq, Event, DBType, combinedPT, combinedAbPT)
	global.Wlog.Debug(logMsg)
	return combinedPT, nil
}

/*
MySQL 获取校验表的列信息，包含列名，列序号，列类型
*/
