package databaseType

//
//import (
//	"database/sql"
//	"fmt"
//	"greatdbCheck/dbExec"
//	"greatdbCheck/global"
//	"os"
//	"strconv"
//	"strings"
//)
//
//type MysqlExecStruct struct{}
//
///*
//   获取MySQL的database的列表信息，排除'information_schema','performance_schema','sys','mysql'
//*/
//func (my MysqlExecStruct) DatabaseNameList(JdbcUrl, dbnameParamter, ignschema string) []string {
//	var sqlStr string
//	var dbName []string
//	dbExample := dbExec.GetDBexec(JdbcUrl, "mysql")
//	db, _ := dbExample.OpenDB()
//	excludeSchema := fmt.Sprintf("'information_schema','performance_schema','sys','mysql','%s'", ignschema)
//	if dbnameParamter == "*" {
//		sqlStr = fmt.Sprintf("select SCHEMA_NAME as databaseName from information_schema.schemata where schema_name not in (%s);", excludeSchema)
//	} else {
//		dbnameParamter = strings.ReplaceAll(dbnameParamter, ",", "','")
//		sqlStr = fmt.Sprintf("select SCHEMA_NAME as databaseName from information_schema.schemata where schema_name  in ('%s');", dbnameParamter)
//	}
//	aa, err := dbExample.QMapData(db, sqlStr)
//	global.Wlog.Info("sql query data info: ", aa)
//	if err == nil && len(aa) > 0 {
//		for i := range aa {
//			global.Wlog.Debug("database ", aa[i]["databaseName"], "be going to checksum.")
//			dbName = append(dbName, fmt.Sprintf("%v", aa[i]["databaseName"]))
//		}
//	}
//	defer db.Close()
//	return dbName
//}
//
///*
//   检查单库下的表的列表信息
//*/
//func (my MysqlExecStruct) TableNameList(JdbcUrl, dbnameParamter, tableParamter string) []map[string]interface{} {
//	var sqlStr string
//	dbExample := dbExec.GetDBexec(JdbcUrl)
//	global.Wlog.Debug("actions get database connection info.")
//	db, _ := dbExample.OpenDB()
//	if tableParamter == "*" {
//		sqlStr = fmt.Sprintf("select table_schema as databaseName,table_name as tableName from information_schema.tables where TABLE_SCHEMA in ('%s');", dbnameParamter)
//	} else {
//		sqlStr = fmt.Sprintf("select table_schema as databaseName,table_name as tableName from information_schema.tables where TABLE_SCHEMA in ('%s') and TABLE_NAME in ('%s');", dbnameParamter, tableParamter)
//	}
//	queryData, _ := dbExample.QMapData(db, sqlStr)
//	defer db.Close()
//	return queryData
//}
//
///*
//   检查表列信息
//*/
//func (my MysqlExecStruct) TableColumnList(JdbcUrl, dbnameParamter, tableParamter string) []map[string]interface{} {
//	global.Wlog.Debug("actions init db Example.")
//	dbExample := dbExec.GetDBexec(JdbcUrl)
//	db, _ := dbExample.OpenDB()
//	global.Wlog.Info("actions begin query columns info")
//	strsql := fmt.Sprintf("select COLUMN_NAME as columnName from information_schema.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' order by ORDINAL_POSITION;", dbnameParamter, tableParamter)
//	queryData, _ := dbExample.QMapData(db, strsql)
//	defer db.Close()
//	return queryData
//}
//
///*
//   检查表索引列信息
//*/
//func (my MysqlExecStruct) IndexColumnList(JdbcUrl, dbnameParamter, tableParamter string) map[string][]string {
//	global.Wlog.Debug("actions init db Example.")
//	dbExample := dbExec.GetDBexec(JdbcUrl)
//	db, _ := dbExample.OpenDB()
//	strsql := fmt.Sprintf("select isc.COLUMN_NAME as columnName,isc.COLUMN_TYPE as columnType,isc.COLUMN_KEY as columnKey,isc.EXTRA as autoIncrement,iss.NON_UNIQUE as nonUnique,iss.INDEX_NAME as indexName,iss.SEQ_IN_INDEX IndexSeq,isc.ORDINAL_POSITION columnSeq from information_schema.columns isc inner join (select NON_UNIQUE,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME from information_schema.STATISTICS where table_schema='%s' and table_name='%s') as iss on isc.column_name =iss.column_name where isc.table_schema='%s' and isc.table_name='%s';", dbnameParamter, tableParamter, dbnameParamter, tableParamter)
//	queryData, _ := dbExample.QMapData(db, strsql)
//	var indexType = make(map[string][]string)
//	nultiseriateIndexColumnMap := make(map[string][]string)
//	multiseriateIndexColumnMap := make(map[string][]string)
//	breakIndexColumnType := []string{"INT", "CHAR", "YEAR", "DATE", "TIME"}
//	if len(queryData) > 0 {
//		var PriIndexCol, uniIndexCol, mulIndexCol []string
//		var indexName string
//		//去除主键索引列、唯一索引列、普通索引列的所有列明
//		for v := range queryData {
//			if queryData[v]["nonUnique"].(int64) == 0 {
//				if queryData[v]["indexName"] == "PRIMARY" {
//					if queryData[v]["indexName"].(string) != indexName {
//						indexName = queryData[v]["indexName"].(string)
//					}
//					PriIndexCol = append(PriIndexCol, fmt.Sprintf("%s", queryData[v]["columnName"]))
//				} else {
//					if queryData[v]["indexName"].(string) != indexName {
//						indexName = queryData[v]["indexName"].(string)
//						nultiseriateIndexColumnMap[indexName] = append(uniIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", queryData[v]["columnName"], queryData[v]["columnType"]))
//					} else {
//						nultiseriateIndexColumnMap[indexName] = append(nultiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", queryData[v]["columnName"], queryData[v]["columnType"]))
//					}
//				}
//			} else {
//				if queryData[v]["indexName"].(string) != indexName {
//					indexName = queryData[v]["indexName"].(string)
//					multiseriateIndexColumnMap[indexName] = append(mulIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", queryData[v]["columnName"], queryData[v]["columnType"]))
//				} else {
//					multiseriateIndexColumnMap[indexName] = append(multiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", queryData[v]["columnName"], queryData[v]["columnType"]))
//				}
//				//mulIndexCol = append(mulIndexCol, fmt.Sprintf("%s", queryData[v]["columnName"]))
//			}
//		}
//		//判断是否存在主键索引,每个表的索引只有一个
//		infoStr := fmt.Sprintf("Greatdbcheck Checks whether table %s.%s has a primary key index", dbnameParamter, tableParamter)
//		global.Wlog.Info(infoStr)
//		if len(PriIndexCol) == 1 { //单列主键索引
//			indexType["pri_single"] = PriIndexCol
//		} else if len(PriIndexCol) > 1 { //联合主键索引
//			indexType["pri_multiseriate"] = PriIndexCol
//		}
//
//		//唯一索引判断选择
//		infoStr = fmt.Sprintf("Greatdbcheck Checks whether table %s.%s has a unique key index", dbnameParamter, tableParamter)
//		global.Wlog.Info(infoStr)
//		indexName = ""
//		//单列唯一索引
//		if len(nultiseriateIndexColumnMap) > 0 {
//			//处理单列索引，找出合适的索引列（选择次序：int<--char<--year<--date<-time）
//			for _, i := range nultiseriateIndexColumnMap {
//				if len(i) == 1 { //单列索引
//					tmpa := strings.Split(strings.Join(i, ""), " /*actions Column Type*/ ")
//					indexColType := tmpa[1]
//					var tmpaa []string
//					for v := range breakIndexColumnType {
//						if strings.Contains(strings.ToUpper(indexColType), breakIndexColumnType[v]) {
//							indexType["uni_single"] = append(tmpaa, tmpa[0])
//							break
//						}
//					}
//				}
//			}
//		}
//
//		//如何是多列索引，择选找出当给列最多的
//		if len(nultiseriateIndexColumnMap) > 1 {
//			//1)先找出联合索引数量最多的
//			tmpSliceNum := 1
//			for i := range nultiseriateIndexColumnMap {
//				if len(nultiseriateIndexColumnMap[i]) > tmpSliceNum {
//					tmpSliceNum = len(nultiseriateIndexColumnMap[i])
//				}
//			}
//			for _, i := range nultiseriateIndexColumnMap {
//				var nultIndexColumnSlice, nultIndexColumnTypeSlice []string
//				for v := range i {
//					tmpiv := strings.ReplaceAll(i[v], " /*actions Column Type*/ ", ",")
//					tmpaa := strings.Split(tmpiv, ",")
//					nultIndexColumnSlice = append(nultIndexColumnSlice, tmpaa[0])
//					nultIndexColumnTypeSlice = append(nultIndexColumnTypeSlice, tmpaa[1])
//				}
//				if len(i) == tmpSliceNum { //加入最多的有多个
//					tmpIntCount := strings.Count(strings.ToUpper(strings.Join(nultIndexColumnTypeSlice, ",")), "INT")
//					tmpCharCount := strings.Count(strings.ToUpper(strings.Join(nultIndexColumnTypeSlice, ",")), "CHAR")
//					if tmpIntCount >= tmpCharCount {
//						indexType["nui_multiseriate"] = nultIndexColumnSlice
//						break
//					} else if tmpCharCount >= tmpIntCount {
//						indexType["nui_multiseriate"] = nultIndexColumnSlice
//						break
//					} else {
//						indexType["nui_multiseriate"] = nultIndexColumnSlice
//						break
//					}
//				}
//			}
//		}
//
//		//判断是否存在普通索引,选出索引列，优先选择多列索引，如果没有，则按优先级选择。
//		infoStr = fmt.Sprintf("Greatdbcheck Checks whether table %s.%s has a key index", dbnameParamter, tableParamter)
//		global.Wlog.Info(infoStr)
//
//		if len(multiseriateIndexColumnMap) > 0 {
//			//处理单列索引，找出合适的索引列（选择次序：int<--char<--year<--date<-time）
//			for i := range multiseriateIndexColumnMap {
//				if len(multiseriateIndexColumnMap[i]) == 1 { //单列索引
//					tmpa := strings.Split(strings.Join(multiseriateIndexColumnMap[i], ""), " /*actions Column Type*/ ")
//					indexColType := tmpa[1]
//					var tmpaa []string
//					for v := range breakIndexColumnType {
//						if strings.Contains(strings.ToUpper(indexColType), breakIndexColumnType[v]) {
//							indexType["mui_single"] = append(tmpaa, tmpa[0])
//							break
//						}
//					}
//				}
//			}
//			//多列索引选择
//			if len(multiseriateIndexColumnMap) > 1 {
//				//1)先找出联合索引数量最多的
//				tmpSliceNum := 1
//				for i := range multiseriateIndexColumnMap {
//					if len(multiseriateIndexColumnMap[i]) > tmpSliceNum {
//						tmpSliceNum = len(multiseriateIndexColumnMap[i])
//					}
//				}
//				for _, i := range multiseriateIndexColumnMap {
//					var multIndexColumnSlice, multIndexColumnTypeSlice []string
//					for v := range i {
//						tmpiv := strings.ReplaceAll(i[v], " /*actions Column Type*/ ", ",")
//						tmpaa := strings.Split(tmpiv, ",")
//						multIndexColumnSlice = append(multIndexColumnSlice, tmpaa[0])
//						multIndexColumnTypeSlice = append(multIndexColumnTypeSlice, tmpaa[1])
//					}
//					if len(i) == tmpSliceNum { //加入最多的有多个
//						tmpIntCount := strings.Count(strings.ToUpper(strings.Join(multIndexColumnTypeSlice, ",")), "INT")
//						tmpCharCount := strings.Count(strings.ToUpper(strings.Join(multIndexColumnTypeSlice, ",")), "CHAR")
//						if tmpIntCount >= tmpCharCount {
//							indexType["mui_multiseriate"] = multIndexColumnSlice
//							break
//						} else if tmpCharCount >= tmpIntCount {
//							indexType["mui_multiseriate"] = multIndexColumnSlice
//							break
//						} else {
//							indexType["mui_multiseriate"] = multIndexColumnSlice
//							break
//						}
//					}
//				}
//			}
//		}
//	} else {
//		infoStr := fmt.Sprintf("Greatdbcheck detects that the current table %s.%s is not using an index for MySQL", dbnameParamter, tableParamter)
//		global.Wlog.Warn(infoStr)
//	}
//	defer db.Close()
//	return indexType
//}
//
///*
//   刷新表，将内存中已经修改的表而未来的及刷脏的数据进行刷脏
//*/
//func (my *MysqlExecStruct) FlushTable(db *sql.DB) {
//	sqlstr := fmt.Sprintf("FLUSH /*!40101 LOCAL */ TABLES")
//	global.Wlog.Debug("GreatdbCheck executes SQL \"FLUSH /*!40101 LOCAL */ TABLES\" at the MySQL")
//	if _, err := db.Exec(sqlstr); err != nil {
//		global.Wlog.Error("exec sql fail. sql: ", sqlstr, "error info: ", err)
//		os.Exit(1)
//	}
//}
//
///*
//   添加全局一致性读锁，防止数据写入
//*/
//func (my *MysqlExecStruct) FushTableReadLock(db *sql.DB) {
//	sqlstr := fmt.Sprintf("FLUSH TABLES WITH READ LOCK")
//	global.Wlog.Debug("GreatdbCheck executes SQL \"FLUSH TABLES WITH READ LOCK\" at the MySQL")
//	if _, err := db.Exec(sqlstr); err != nil {
//		global.Wlog.Error("exec sql fail. sql: ", sqlstr, "error info: ", err)
//		os.Exit(1)
//	}
//}
//
///*
//   创建源、目并发查询数据时需要的 快照会话，防止数据修改查询数据不对
//*/
//func (my *MysqlExecStruct) SessionRR(jdbcurl string, concurrency int) chan *sql.DB {
//
//	var cisoRRsessionChan chan *sql.DB //设置有全局一致性事务的事务快照的db连接id管道
//	cisoRRsessionChan = make(chan *sql.DB, concurrency)
//	for i := 1; i <= concurrency; i++ {
//		sddb, _ := dbExec.GetDBexec(jdbcurl, "mysql").OpenDB()
//		sqlstr := "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
//		global.Wlog.Debug("GreatdbCheck executes ", "\"", sqlstr, "\"", " at the MySQL")
//		if _, err := sddb.Exec(sqlstr); err != nil {
//			global.Wlog.Error("exec sql fail. sql: ", sqlstr, "error info: ", err)
//		} else {
//			cisoRRsessionChan <- sddb
//		}
//	}
//	return cisoRRsessionChan
//}
//
///*
//  获取全局一致性位点
//*/
//func (my *MysqlExecStruct) GlobalConsistencyPoint(db *sql.DB) map[string]string {
//	var file, position, binlog_Do_DB, binlog_Ignore_DB, executed_Gtid_Set string
//	var globalPoint = make(map[string]string)
//	sqlstr := fmt.Sprintf("SHOW MASTER STATUS")
//	global.Wlog.Debug("GreatdbCheck executes \"", sqlstr, "\" at the MySQL")
//	if rows, err := db.Query(sqlstr); err == nil {
//		for rows.Next() {
//			rows.Scan(&file, &position, &binlog_Do_DB, &binlog_Ignore_DB, &executed_Gtid_Set)
//		}
//		infostr := fmt.Sprintf("The current master status of mysql is binlogFile: %s, binlogPos: %s, binlog_do_db: %s, binlog_ignore_db: %s, executed_gtid_set: %s", file, position, binlog_Do_DB, binlog_Ignore_DB, executed_Gtid_Set)
//		global.Wlog.Info(infostr)
//	} else {
//		global.Wlog.Error("exec sql fail. sql: ", sqlstr, "error info: ", err)
//	}
//	globalPoint["file"] = file
//	globalPoint["position"] = position
//	globalPoint["Point"] = executed_Gtid_Set
//	return globalPoint
//}
//
///*
//   解锁
//*/
//func (my *MysqlExecStruct) Unlock(db *sql.DB) error {
//	var err error = nil
//	sqlstr := fmt.Sprintf("UNLOCK TABLES")
//	global.Wlog.Debug("GreatdbCheck executes \"", sqlstr, "\" at the MySQL")
//	if _, err = db.Exec(sqlstr); err != nil {
//		global.Wlog.Error("exec sql fail. sql: ", sqlstr, "error info: ", err)
//		return err
//	}
//	return err
//}
//
///*
//   加全局一致性锁，获取一致性位点，并创建多连接一致性快照
//*/
//func (my *MysqlExecStruct) GlobalConsistencySnapshot(jdbc string, RowConcurrency int) (chan *sql.DB, map[string]string) {
//	sourdb, _ := dbExec.GetDBexec(jdbc).OpenDB()
//	fmt.Println("Task 1: GreatdbCheck Starts to execute Flush Table on the source end.")
//	global.Wlog.Info("Task 1: GreatdbCheck Starts to execute Flush Table on the source end.")
//	my.FlushTable(sourdb)
//	fmt.Println("Task 2: GreatdbCheck Starts to execute Flush Table read lock on the source end.")
//	global.Wlog.Info("Task 2: GreatdbCheck Starts to execute Flush Table read lock on the source end.")
//	my.FushTableReadLock(sourdb)
//	//获取全局一致性位点
//	fmt.Println("Task 3: GreatdbCheck Starts to obtain the current global consistency point.")
//	global.Wlog.Info("Task 3: GreatdbCheck Starts to obtain the current global consistency point.")
//	globalPoint := my.GlobalConsistencyPoint(sourdb)
//
//	//创建并发连接数，并设置一致性快照
//	fmt.Println("Task 4: GreatdbCheck starts to create a consistency snapshot on the source end.")
//	global.Wlog.Info("Task 4: GreatdbCheck starts to create a consistency snapshot on the source end.")
//
//	cisoRRsessionChan := my.SessionRR(jdbc, RowConcurrency)
//
//	fmt.Println("Task 5: GreatdbCheck Starts to create a consistency snapshot on the target end.")
//	global.Wlog.Info("Task 5: GreatdbCheck Starts to create a consistency snapshot on the target end.")
//	fmt.Println("Task 6: GreatdbCheck Starts to execute unlock on the source end")
//	global.Wlog.Info("Task 6: GreatdbCheck Starts to execute unlock on the source end")
//	//解全局锁
//	my.Unlock(sourdb)
//	return cisoRRsessionChan, globalPoint
//}
//
///*
//   在当前目录下创建一个目录，二级目录为数据库名，在二级目录下创建临时文件，文件名称为原目标端的表，向表中插入索引列数据，并返回总行数
//*/
//func (my *MysqlExecStruct) TmpTableDataFileInput(db *sql.DB, schema, tmpTableFileName, table string, columnName []string, chanrowCount int) (int, error) {
//	var strSql, selectColumnString, lengthTrim string
//	var columnLengthAs []string
//	dbExample := dbExec.DBConnStruct{}
//	//根据主键列，生成序列号并写入临时表中
//	global.Wlog.Info("GreatdbCheck begin Write to temporary table based on pseudo ordinal generated by index column")
//	//aa := "SET session sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));"
//
//	if len(columnName) == 1 {
//		selectColumnString = strings.Join(columnName, "")
//		lengthTrim = fmt.Sprintf("LENGTH(trim(%s)) as %s_length", strings.Join(columnName, ""), strings.Join(columnName, ""))
//		columnLengthAs = append(columnLengthAs, fmt.Sprintf("%s_length", strings.Join(columnName, "")))
//	} else {
//		selectColumnString = strings.Join(columnName, ",")
//		var aa []string
//		for i := range columnName {
//			aa = append(aa, fmt.Sprintf("LENGTH(trim(%s)) as %s_length", columnName[i], columnName[i]))
//			columnLengthAs = append(columnLengthAs, fmt.Sprintf("%s_length"), columnName[i])
//		}
//		lengthTrim = strings.Join(aa, ",")
//	}
//	strSql = fmt.Sprintf("select %s,%s from %s.%s group by %s;", selectColumnString, lengthTrim, schema, table, selectColumnString)
//	var tmpTableCount int
//	var tableDataFil = &global.TableDateFileStruct{
//		FileName: tmpTableFileName,
//	}
//
//	if rows, err := dbExample.QPrepareRow(db, strSql); err == nil {
//		var tmpStringInputMapSlice = make(map[int][]string)
//		columns, _ := rows.Columns()
//		valuePtrs := make([]interface{}, len(columns))
//		values := make([]interface{}, len(columns))
//		for rows.Next() {
//			var tmpStringInputSlice []string
//			for i := 0; i < len(columns); i++ {
//				valuePtrs[i] = &values[i]
//			}
//			rows.Scan(valuePtrs...)
//			entry := make(map[string]interface{})
//			for i, col := range columns {
//				var v interface{}
//				val := values[i]
//				b, ok := val.([]byte)
//				if ok {
//					v = string(b)
//				} else {
//					v = val
//				}
//				entry[col] = v
//			}
//
//			for _, aa1 := range columnLengthAs {
//				//对null做处理
//				var tmpadf interface{}
//				if fmt.Sprintf("%v", entry[aa1]) == "<nil>" {
//					tmpadf = "greatdbCheckNULL"
//					entry[strings.ReplaceAll(aa1, "_length", "")] = tmpadf
//				}
//				//对空字符串做处理
//				if fmt.Sprintf("%v", entry[aa1]) == "0" {
//					tmpadf = "greatdbCheckEmtry"
//					entry[strings.ReplaceAll(aa1, "_length", "")] = tmpadf
//				}
//			}
//			for _, aa1 := range columnName {
//				if len(aa1) > 0 {
//					tmpStringInputSlice = append(tmpStringInputSlice, fmt.Sprintf("%v", entry[aa1]))
//				}
//			}
//			tmpStringInputMapSlice[tmpTableCount] = tmpStringInputSlice
//			tmpTableCount++
//			//分段写入，切片中数据大于单次并发查询的行数时，进行
//			if tmpTableCount%chanrowCount == 0 {
//				if err = tableDataFil.WriteFile(tmpStringInputMapSlice); err != nil {
//					global.Wlog.Error(fmt.Sprintf("actions Write file fail. err info：", err))
//					return 0, err
//				}
//				tmpStringInputMapSlice = make(map[int][]string)
//			}
//		}
//		rows.Close()
//		if err = tableDataFil.WriteFile(tmpStringInputMapSlice); err != nil {
//			global.Wlog.Error(fmt.Sprintf("actions Write file fail. err info：", err))
//			return 0, err
//		}
//		//tmpStringInputSlice = []string{}
//	}
//	return tmpTableCount, nil
//}
//
///*
//   该函数用于获取待校验表的列信息
//*/
//func (my *MysqlExecStruct) DbQueryTableColumnInfo(jdbc, schema, table string) ([]map[string]interface{}, error) {
//	sourdb, _ := dbExec.GetDBexec(jdbc).OpenDB()
//	dbExample := dbExec.DBConnStruct{}
//	sqlStr := fmt.Sprintf("select COLUMN_NAME as columnName ,COLUMN_TYPE as dataType,ORDINAL_POSITION as columnSeq from information_schema.columns where table_schema= '%s' and table_name='%s' order by ORDINAL_POSITION;", schema, table)
//	return dbExample.QMapData(sourdb, sqlStr)
//}
//
///*
//   该函数用于需要查询源目表端数据库校验块数据，查询数据生成带有greatdbCheck标识的数据块
//*/
//func (my *MysqlExecStruct) DbQueryDataString(db *sql.DB, schema, table string, tableColumn []map[string]string, sqlWhere string) (string, error) {
//	defer func() {
//		if err := recover(); err != nil {
//
//		}
//	}()
//	dbExample := dbExec.DBConnStruct{}
//	var columnNameSeq []string
//	//查询该表的列名和列信息
//	var sqlStr string
//	//处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
//	for i := range tableColumn {
//		var tmpcolumnName string
//		tmpcolumnName = tableColumn[i]["columnName"]
//		if strings.ToUpper(tableColumn[i]["dataType"]) == "DATETIME" {
//			tmpcolumnName = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", tableColumn[i]["columnName"])
//		}
//		if strings.Contains(strings.ToUpper(tableColumn[i]["dataType"]), "TIMESTAMP") {
//			tmpcolumnName = fmt.Sprintf("date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", tableColumn[i]["columnName"])
//		}
//		columnNameSeq = append(columnNameSeq, tmpcolumnName)
//	}
//	queryColumn := strings.Join(columnNameSeq, ",")
//	sqlStr = fmt.Sprintf("select %s from `%s`.`%s` where %s", queryColumn, schema, table, sqlWhere)
//	return dbExample.DbSqlExecString(db, sqlStr)
//}
//
///*
//  MySQL 生成insert修复语句
//*/
//func (my *MysqlExecStruct) FixInsertSqlExec(db *sql.DB, schema, table, rowData string) string {
//	dbExample := dbExec.DBConnStruct{}
//	//查询该表的列名和列信息
//	var insertSql string
//	var valuesNameSeq []string
//	insertSql = fmt.Sprintf("select COLUMN_NAME as columnName,COLUMN_TYPE as dataType,ORDINAL_POSITION as columnSeq from information_schema.columns where table_schema='%s' and table_name='%s' order by ORDINAL_POSITION;", schema, table)
//	tableColumn, err := dbExample.QMapData(db, insertSql)
//	if err != nil {
//		os.Exit(1)
//	}
//
//	//处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
//	for i := range tableColumn {
//		var tmpcolumnName string
//		tmprowSlic := strings.Split(rowData, "/*go actions columnData*/")
//		tmpcolumnName = fmt.Sprintf("'%s'", tmprowSlic[i])
//		if strings.ToUpper(tableColumn[i]["dataType"].(string)) == "DATETIME" {
//			tmpColumnSeq, _ := strconv.Atoi(fmt.Sprintf("%v", tableColumn[i]["columnSeq"]))
//			tmprowSLIC := strings.ReplaceAll(tmprowSlic[tmpColumnSeq-1], "'", "")
//			tmpcolumnName = fmt.Sprintf("date_format('%s','%%Y-%%m-%%d %%H:%%i:%%s')", tmprowSLIC)
//		}
//		if strings.Contains(strings.ToUpper(tableColumn[i]["dataType"].(string)), "TIMESTAMP") {
//			tmpColumnSeq, _ := strconv.Atoi(fmt.Sprintf("%v", tableColumn[i]["columnSeq"]))
//			tmprowSLIC := strings.ReplaceAll(tmprowSlic[tmpColumnSeq-1], "'", "")
//			tmpcolumnName = fmt.Sprintf("date_format('%s','%%Y-%%m-%%d %%H:%%i:%%s')", tmprowSLIC)
//		}
//		valuesNameSeq = append(valuesNameSeq, tmpcolumnName)
//	}
//
//	queryColumn := strings.Join(valuesNameSeq, ",")
//	if strings.Contains(queryColumn, "'<nil>'") {
//		insertSql = fmt.Sprintf("insert into `%s`.`%s` values(%s);", schema, table, strings.ReplaceAll(queryColumn, "'<nil>'", "NULL"))
//	} else {
//		insertSql = fmt.Sprintf("insert into `%s`.`%s` values(%s);", schema, table, queryColumn)
//	}
//	return insertSql
//}
//
///*
//  mysql 生成delete 修复语句
//*/
//func (my *MysqlExecStruct) FixDeleteSqlExec(rowData, schema, table string, tableColInfo []map[string]string, delSqlWhere string) string {
//	var deleteSql, deleteSqlWhere string
//	var indexColName string
//	var indexColSeq string
//	if strings.Contains(delSqlWhere, " in (") {
//		aa := strings.ReplaceAll(delSqlWhere, "/* actions */ ", "")
//		indexColName = strings.Split(aa, " in (")[0]
//		for i := range tableColInfo {
//			if tableColInfo[i]["columnName"] == strings.TrimSpace(indexColName) {
//				indexColSeq = tableColInfo[i]["columnSeq"]
//			}
//		}
//		for k, v := range strings.Split(rowData, "/*go actions columnData*/") {
//			if indexColSeq == strconv.Itoa(k+1) {
//				deleteSqlWhere = fmt.Sprintf(" %s = %s ;", indexColName, v)
//			}
//		}
//	} else {
//		deleteSqlWhere = delSqlWhere
//	}
//	deleteSql = fmt.Sprintf("delete from `%s`.`%s` where %s", schema, table, deleteSqlWhere)
//	fmt.Println(deleteSql)
//	return deleteSql
//}
//
///*
//   执行修复语句
//*/
//func (my *MysqlExecStruct) execRapirSql(db *sql.DB, sqlstr string) {
//	//执行sql语句不记录binlog
//	stmat, err := db.Prepare("set sql_log_bin=0")
//	if _, err = stmat.Exec(); err != nil {
//		global.Wlog.Error("actions Exec dataFix SQL fail. sql is:", "\"set sql_log_bin=0\"", " error msg: ", err)
//	}
//	stmat, err = db.Prepare(sqlstr)
//	global.Wlog.Debug("GreatdbCheck prepare sql: \"", sqlstr, "\" at the MySQL")
//	if err != nil {
//		global.Wlog.Error("GreatdbCheck parpare sql fail. sql: ", sqlstr, "error info: ", err)
//	}
//	_, err = stmat.Exec()
//	if err != nil {
//		global.Wlog.Error("GreatdbCheck exec sql fail. sql: ", sqlstr, "error info: ", err)
//	}
//}
