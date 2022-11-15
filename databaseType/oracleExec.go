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
//type OracleExecStruct struct{}
//
////获取MySQL的database的列表信息，排除'information_schema','performance_schema','sys','mysql'
//func (or *OracleExecStruct) DatabaseNameList(JdbcUrl, dbnameParamter, ignschema string) []string {
//	var sqlStr string
//	var dbName []string
//	dbExample := dbExec.GetDBexec(JdbcUrl, "oracle")
//	db, _ := dbExample.OpenDB()
//	excludeSchema := fmt.Sprintf("'SYS','OUTLN','SYSTEM','DBSNMP','APPQOSSYS','WMSYS','EXFSYS','CTXSYS','XDB','ORDDATA','ORDSYS','MDSYS','OLAPSYS','SYSMAN','FLOWS_FILES','APEX_030200','OWBSYS','SCOTT','HR','OE','SH','IX','PM','%s'", strings.ToUpper(ignschema))
//	if dbnameParamter == "*" {
//		sqlStr = fmt.Sprintf("select distinct OWNER as \"databaseName\" from all_tables where owner not in (%s)", excludeSchema)
//	} else {
//		dbnameParamter = strings.ReplaceAll(dbnameParamter, ",", "','")
//		sqlStr = fmt.Sprintf("select SCHEMA_NAme from information_schema.schemata where schema_name  in ('%s')", dbnameParamter)
//	}
//	aa, err := dbExample.QMapData(db, sqlStr)
//	global.Wlog.Info("sql query data info: ", aa)
//	if err == nil {
//		for i := range aa {
//			global.Wlog.Debug("database ", aa[i]["databaseName"], "be going to checksum.")
//			dbName = append(dbName, fmt.Sprintf("%s", aa[i]["databaseName"]))
//		}
//	}
//	defer db.Close()
//	return dbName
//}
//
//func (or *OracleExecStruct) TableNameList(JdbcUrl, dbnameParamter, tableParamter string) []map[string]interface{} {
//	dbnameParamter = strings.ToUpper(dbnameParamter)
//	tableParamter = strings.ToUpper(tableParamter)
//	var sqlStr string
//	dbExample := dbExec.GetDBexec(JdbcUrl)
//	global.Wlog.Debug("actions get database connection info.")
//	db, _ := dbExample.OpenDB()
//	if tableParamter == "*" {
//		sqlStr = fmt.Sprintf("SELECT owner as \"databaseName\",table_name as \"tableName\" FROM DBA_TABLES WHERE OWNER='%s'", dbnameParamter)
//	} else {
//		sqlStr = fmt.Sprintf("SELECT owner as \"databaseName\",table_name as \"tableName\" FROM DBA_TABLES WHERE OWNER='%s' and table_name = '%s'", dbnameParamter, tableParamter)
//	}
//	queryData, _ := dbExample.QMapData(db, sqlStr)
//	defer db.Close()
//	return queryData
//}
//func (or *OracleExecStruct) TableColumnList(JdbcUrl, dbnameParamter, tableParamter string) []map[string]interface{} {
//	dbnameParamter = strings.ToUpper(dbnameParamter)
//	tableParamter = strings.ToUpper(tableParamter)
//	global.Wlog.Debug("actions init db Example.")
//	dbExample := dbExec.GetDBexec(JdbcUrl)
//	db, _ := dbExample.OpenDB()
//	global.Wlog.Info("actions begin query columns info")
//	strsql := fmt.Sprintf("select column_name as \"columnName\" from all_tab_columns where owner='%s' and table_name='%s' order by 'column_id'", dbnameParamter, tableParamter)
//	queryData, _ := dbExample.QMapData(db, strsql)
//	defer db.Close()
//	return queryData
//}
//func (or *OracleExecStruct) IndexColumnList(JdbcUrl, dbnameParamter, tableParamter string) map[string]string {
//	global.Wlog.Debug("actions init db Example.")
//	dbExample := dbExec.GetDBexec(JdbcUrl)
//	db, _ := dbExample.OpenDB()
//	strsql := fmt.Sprintf("select c.COLUMN_NAME as \"columnName\",decode(c.DATA_TYPE,'DATE',c.data_type,c.DATA_TYPE||'('||c.data_length||')')  as \"columnType\",i.index_type as \"columnKey\",i.UNIQUENESS as \"nonUnique\" ,ic.INDEX_NAME as \"indexName\",ic.COLUMN_POSITION as \"IndexSeq\", c.COLUMN_ID as \"columnSeq\" from all_tab_cols c inner join all_ind_columns ic on c.TABLE_NAME=ic.TABLE_NAME and c.OWNER=ic.INDEX_OWNER and c.COLUMN_NAME=ic.COLUMN_NAME inner join  all_indexes i on ic.INDEX_OWNER=i.OWNER and ic.INDEX_NAME=i.INDEX_NAME and ic.TABLE_NAME=i.TABLE_NAME where c.OWNER = '%s' and c.TABLE_NAME = '%s' ORDER BY I.INDEX_NAME,ic.COLUMN_POSITION", dbnameParamter, tableParamter)
//	queryData, _ := dbExample.QMapData(db, strsql)
//	var indexType = make(map[string]string)
//	if len(queryData) > 0 {
//		var columnIndexSeq int
//		for v := range queryData {
//			if tmpint, err := strconv.Atoi(fmt.Sprintf("%s", queryData[v]["IndexSeq"])); err == nil {
//				columnIndexSeq = tmpint
//			} else {
//				fmt.Println(err)
//				os.Exit(1)
//			}
//			//判断是否存在主键索引
//			infoStr := fmt.Sprintf("Greatdbcheck Checks whether table %s.%s has a primary key index", dbnameParamter, tableParamter)
//			global.Wlog.Info(infoStr)
//			numericTypes := "NUMBER,INTEGER,BINARY_FLOAT,BINARY_DOUBLE,FLOAT,INT,ROWID"
//			if queryData[v]["nonUnique"] == "UNIQUE" && columnIndexSeq == 1 {
//				//判断是否存在int主键索引
//				if strings.Contains(numericTypes, queryData[v]["columnType"].(string)) {
//					indexType["uni_int"] = fmt.Sprintf("%sgreatdbcheck@%sgreatdbcheck*%sgreatdbcheck^%v", "1", queryData[v]["columnName"], queryData[v]["columnType"], queryData[v]["columnSeq"])
//					infoStr = fmt.Sprintf("Greatdbcheck The primary key index of table %s.%s is int.", dbnameParamter, tableParamter)
//					global.Wlog.Info(infoStr)
//				} else {
//					//判断是否存在非int主键索引
//					indexType["uni_var"] = fmt.Sprintf("%sgreatdbcheck@%sgreatdbcheck*%sgreatdbcheck^%v", "1", queryData[v]["columnName"], queryData[v]["columnType"], queryData[v]["columnSeq"])
//					infoStr = fmt.Sprintf("Greatdbcheck The primary key index of non-int type exists in table %s.%s.", dbnameParamter, tableParamter)
//					global.Wlog.Info(infoStr)
//				}
//			}
//			//判断是否存在普通索引,选出索引列，优先选择多列索引，如果没有，则按优先级选择。
//			infoStr = fmt.Sprintf("Greatdbcheck Checks whether table %s.%s has a key index", dbnameParamter, tableParamter)
//			global.Wlog.Info(infoStr)
//			if queryData[v]["nonUnique"] == "NONUNIQUE" && columnIndexSeq == 1 {
//				//判断是否存在普通的int唯一索引
//				if strings.Contains(numericTypes, queryData[v]["columnType"].(string)) {
//					indexType["mul_int"] = fmt.Sprintf("%sgreatdbcheck@%sgreatdbcheck*%sgreatdbcheck^%v", "1", queryData[v]["columnName"], queryData[v]["columnType"], queryData[v]["columnSeq"])
//					infoStr = fmt.Sprintf("Greatdbcheck The key index of table %s.%s is int.", dbnameParamter, tableParamter)
//					global.Wlog.Info(infoStr)
//				} else {
//					//判断是否存在普通的非int唯一索引
//					indexType["mul_var"] = fmt.Sprintf("%sgreatdbcheck@%sgreatdbcheck*%sgreatdbcheck^%v", "1", queryData[v]["columnName"], queryData[v]["columnType"], queryData[v]["columnSeq"])
//					infoStr = fmt.Sprintf("Greatdbcheck The key index of non-int type exists in table %s.%s.", dbnameParamter, tableParamter)
//					global.Wlog.Info(infoStr)
//				}
//			}
//		}
//	} else {
//		infoStr := fmt.Sprintf("Greatdbcheck detects that the current table %s.%s is not using an index for Oracle", dbnameParamter, tableParamter)
//		global.Wlog.Warn(infoStr)
//	}
//	defer db.Close()
//	return indexType
//}
//
///*
//   刷新表，将内存中已经修改的表而未来的及刷脏的数据进行刷脏
//*/
//func (or *OracleExecStruct) FlushTable(db *sql.DB) {
//	sqlstr := fmt.Sprintf("alter system checkpoint")
//	global.Wlog.Debug("GreatdbCheck executes SQL \"alter system checkpoint\" at the Oracle")
//	if _, err := db.Exec(sqlstr); err != nil {
//		global.Wlog.Error("exec sql fail. sql: ", sqlstr, "error info: ", err)
//		os.Exit(1)
//	}
//}
//
///*
//   创建源、目并发查询数据时需要的 快照会话，防止数据修改查询数据不对
//*/
//func (or *OracleExecStruct) SessionRR(jdbcurl string, concurrency int) chan *sql.DB {
//	var cisoRRsessionChan chan *sql.DB //设置有全局一致性事务的事务快照的db连接id管道
//	cisoRRsessionChan = make(chan *sql.DB, concurrency)
//	for i := 1; i <= concurrency; i++ {
//		sddb, _ := dbExec.GetDBexec(jdbcurl).OpenDB()
//		cisoRRsessionChan <- sddb
//	}
//	return cisoRRsessionChan
//}
//
///*
//创建一致性快照，获取全局一致性位点
//*/
//func (or *OracleExecStruct) GlobalConsistencyScn(db *sql.DB) string {
//	var sqlstr, globalScnSeq string
//	//查看当前gtid是否开启
//	sqlstr = fmt.Sprintf("select current_scn as \"globalScn\" from v$database")
//	global.Wlog.Debug("GreatdbCheck executes \"", sqlstr, "\" at the oracle")
//	if rows, err := db.Query(sqlstr); err == nil {
//		for rows.Next() {
//			rows.Scan(&globalScnSeq)
//			infostr := fmt.Sprintf("The current global scn of oracle is SCN: %s", globalScnSeq)
//			global.Wlog.Info(infostr)
//		}
//	} else {
//		global.Wlog.Error("exec sql fail. sql: ", sqlstr, "error info: ", err)
//	}
//	return globalScnSeq
//}
//
///*
//   加全局一致性锁，获取一致性位点，并创建多连接一致性快照
//*/
//func (or *OracleExecStruct) GlobalConsistencySnapshot(jdbc string, RowConcurrency int) (chan *sql.DB, map[string]string) {
//	var globalScnMap = make(map[string]string)
//	sourdb, _ := dbExec.GetDBexec(jdbc).OpenDB()
//	fmt.Println("Task 1: GreatdbCheck Starts to execute Flush Table on the source end.")
//	global.Wlog.Info("Task 1: GreatdbCheck Starts to execute Flush Table on the source end.")
//	or.FlushTable(sourdb)
//	//获取全局一致性位点
//	fmt.Println("Task 2: GreatdbCheck Starts to obtain the current global consistency point.")
//	global.Wlog.Info("Task 2: GreatdbCheck Starts to obtain the current global consistency point.")
//	globalScn := or.GlobalConsistencyScn(sourdb)
//	globalScnMap["Point"] = globalScn
//	//创建并发连接数，并设置一致性快照
//	fmt.Println("Task 3: GreatdbCheck starts to create a consistency snapshot on the source end.")
//	global.Wlog.Info("Task 3: GreatdbCheck starts to create a consistency snapshot on the source end.")
//	infostr := fmt.Sprintf("The current scn status of oracle is %s", globalScnMap)
//	fmt.Println(infostr)
//	cisoRRsessionChan := or.SessionRR(jdbc, RowConcurrency)
//	return cisoRRsessionChan, globalScnMap
//}
//
//func (or *OracleExecStruct) CreateCheckTmpSchema(db *sql.DB, schema string) error {
//	//创建临时表
//	global.Wlog.Info("GreatdbCheck begin create tmp ", schema)
//	dbExample := dbExec.DBConnStruct{}
//	strSql := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", schema)
//	err := dbExample.LongSessionExec(db, strSql)
//	if err != nil {
//		return err
//	}
//	strSql = fmt.Sprintf("CREATE DATABASE `%S`;", schema)
//	err = dbExample.LongSessionExec(db, strSql)
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
///*
//  创建临时表，并插入数据
//*/
//func (or OracleExecStruct) CheckTableRowCount(db *sql.DB, schema, table, tmpTableName, columnName, columnType string) (int, string) {
//	//创建临时表
//	global.Wlog.Info("GreatdbCheck begin create ", schema, ".", tmpTableName, " temporary table for oracle")
//	dbExample := dbExec.DBConnStruct{}
//	strSql := fmt.Sprintf("select decode(c.DATA_TYPE,'DATE',c.data_type,c.DATA_TYPE||'('||c.data_length||')')  as \"columnType\" from all_tab_cols C where owner='%s' and table_name='%s' and column_name='%s'", schema, table, columnName)
//	columnType, _ = dbExample.DbSqlExecString(db, strSql)
//	strSql = fmt.Sprintf("create table \"%s\".\"%s\" (rownumId number primary key,%s %s)", schema, tmpTableName, columnName, columnType)
//	dbExample.LongSessionExec(db, strSql)
//	//根据主键列，生成序列号并写入临时表中
//	global.Wlog.Info("GreatdbCheck begin Write to temporary table based on pseudo ordinal generated by index column")
//	strSql = fmt.Sprintf("insert into \"%s\".\"%s\"(rownumId,%s) select rownum,%s from (select distinct %s from \"%s\".\"%s\")", schema, tmpTableName, columnName, columnName, columnName, schema, table)
//	dbExample.LongSessionExec(db, strSql)
//	//获取临时表的索引列数据
//	strSql = fmt.Sprintf("select %s from \"%s\".\"%s\"", columnName, schema, tmpTableName)
//	indexColumnData, _ := dbExample.DbSqlExecString(db, strSql)
//	//获取该表总行数
//	global.Wlog.Info("GreatdbCheck Obtains the total number of rows in the table")
//	strSql = fmt.Sprintf("select max(rownumId) from \"%s\".\"%s\"", schema, tmpTableName)
//	tmpTableCount, _ := dbExample.LSQInt(db, strSql)
//	return tmpTableCount, indexColumnData
//}
//
//func (or OracleExecStruct) QueryCheckTableRowCount(db *sql.DB, schema, table, tmpTableName, columnName string, chanrowCount, i int, columnSeq string) map[string]string {
//	var chanData = make(map[string]string)
//	//创建临时表
//	global.Wlog.Info("GreatdbCheck begin query ", schema, ".", tmpTableName, " temporary table")
//	dbExample := dbExec.DBConnStruct{}
//	strSql := fmt.Sprintf("select %s from (select %s from \"%s\".\"%s\" order by rownumId asc) where rownum<=%d", columnName, columnName, schema, tmpTableName, chanrowCount)
//	rowDataSelic, _ := dbExample.LSQSEInt(db, strSql)
//	//生成where 条件
//	chankey := fmt.Sprintf("task%d:%s@%s", i, schema, table)
//	chanData[chankey] = fmt.Sprintf("/* %s */ %s  in ('%s')", columnSeq, columnName, strings.Join(rowDataSelic, "','"))
//	//在临时表中删除已查询的数据
//	//读取完成后进行逻辑删除，原因是防止异常中断导致无法继续
//	sqlstr := fmt.Sprintf("delete from \"%s\".\"%s\" where %s", schema, tmpTableName, chanData[chankey])
//	dbExample.LongSessionExec(db, sqlstr)
//	return chanData
//}
//func (or *OracleExecStruct) DropCheckTmpTable(sdb, ddb *sql.DB, schema, table, tmpTableName, droptableAction string) {
//	strSql := fmt.Sprintf("select rownumId from \"%s\".\"%s\"", schema, tmpTableName)
//	dbExample := dbExec.DBConnStruct{}
//	if tmpTableCount, err := dbExample.LSQInt(sdb, strSql); tmpTableCount == 0 && err == nil || droptableAction == "drop" {
//		global.Wlog.Info("GreatdbCheck The current table data query plan is complete. ", schema, ".", table)
//		//删除表
//		strSql = fmt.Sprintf("drop table \"%s\".\"%s\"", schema, tmpTableName)
//		global.Wlog.Info("GreatdbCheck begin drop A temporary table. table info is: ", schema, ".", tmpTableName)
//		dbExample.LongSessionExec(sdb, strSql)
//		dbExample.LongSessionExec(ddb, strSql)
//	}
//}
//
//func (or *OracleExecStruct) DbQueryDataString(db *sql.DB, schema, table, oracleScn, sqlWhere string) string {
//	dbExample := dbExec.DBConnStruct{}
//	//查询该表的列名和列信息
//	var sqlStr string
//	var columnNameSeq []string
//	sqlStr = fmt.Sprintf("SELECT column_name as \"columnName\",data_type as \"dataType\" FROM all_tab_cols c where c.OWNER = '%s' and c.TABLE_NAME = '%s' order by column_id asc", schema, table)
//	tableColumn, err := dbExample.QMapData(db, sqlStr)
//	if err != nil {
//		os.Exit(1)
//	}
//	//处理oracle查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
//	for i := range tableColumn {
//		var tmpcolumnName string
//		tmpcolumnName = tableColumn[i]["columnName"].(string)
//		if strings.ToUpper(tableColumn[i]["dataType"].(string)) == "DATE" {
//			tmpcolumnName = fmt.Sprintf("to_char(%s,'YYYY-MM-DD HH24:MI:SS')", tableColumn[i]["columnName"])
//		}
//		if strings.Contains(strings.ToUpper(tableColumn[i]["dataType"].(string)), "TIMESTAMP") {
//			//tmp_dataTypeSeq := strings.Split(strings.ReplaceAll(tableColumn[i]["dataType"].(string),")",""),"(")[1]
//			//tmpcolumnName = fmt.Sprintf("to_char(%s,'YYYY-MM-DD HH24:MI:SS.FF%s')", tableColumn[i]["columnName"],tmp_dataTypeSeq)
//			tmpcolumnName = fmt.Sprintf("to_char(%s,'YYYY-MM-DD HH24:MI:SS')", tableColumn[i]["columnName"])
//		}
//		columnNameSeq = append(columnNameSeq, tmpcolumnName)
//	}
//	queryColumn := strings.Join(columnNameSeq, ",")
//	//执行查询数据库语句
//	sqlstr := fmt.Sprintf("select %s from \"%s\".\"%s\" as of scn %s where %s", queryColumn, schema, table, oracleScn, sqlWhere)
//	rowData, _ := dbExample.DbSqlExecString(db, sqlstr)
//	return rowData
//}
//
///*
//  oracle 生成insert 修复语句
//*/
//func (or *OracleExecStruct) FixInsertSqlExec(db *sql.DB, schema, table, rowData string) string {
//	dbExample := dbExec.DBConnStruct{}
//	//查询该表的列名和列信息
//	var sqlStr string
//	var valuesNameSeq []string
//	sqlStr = fmt.Sprintf("SELECT column_name as \"columnName\",data_type as \"dataType\",column_id as \"columnSeq\" FROM all_tab_cols c where c.OWNER = '%s' and c.TABLE_NAME = '%s' order by column_id asc", schema, table)
//	tableColumn, err := dbExample.QMapData(db, sqlStr)
//	if err != nil {
//		os.Exit(1)
//	}
//	//处理oracle查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
//	for i := range tableColumn {
//		var tmpcolumnName string
//		tmprowSlic := strings.Split(rowData, "/*go actions columnData*/")
//		tmpcolumnName = fmt.Sprintf("'%s'", tmprowSlic[i])
//		if strings.ToUpper(tableColumn[i]["dataType"].(string)) == "DATE" {
//			tmpColumnSeq, _ := strconv.Atoi(fmt.Sprintf("%v", tableColumn[i]["columnSeq"]))
//			tmprowSLIC := strings.ReplaceAll(tmprowSlic[tmpColumnSeq-1], "'", "")
//			tmpcolumnName = fmt.Sprintf("TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')", tmprowSLIC)
//		}
//		if strings.Contains(strings.ToUpper(tableColumn[i]["dataType"].(string)), "TIMESTAMP") {
//			tmpColumnSeq, _ := strconv.Atoi(fmt.Sprintf("%v", tableColumn[i]["columnSeq"]))
//			tmprowSLIC := strings.ReplaceAll(tmprowSlic[tmpColumnSeq-1], "'", "")
//			tmpcolumnName = fmt.Sprintf("TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS')", tmprowSLIC)
//		}
//		valuesNameSeq = append(valuesNameSeq, tmpcolumnName)
//	}
//	queryColumn := strings.Join(valuesNameSeq, ",")
//	sqlstr := fmt.Sprintf("insert into \"%s\".\"%s\" values(%s)", schema, table, queryColumn)
//	return sqlstr
//}
//
///*
//  oracle 生成delete 修复语句
//*/
//func (or *OracleExecStruct) FixDeleteSqlExec(rowData, schema, table, indexCol string, indexColSeq int) string {
//	tmprow := strings.Split(rowData, "/*go actions columnData*/")
//	return fmt.Sprintf("delete from \"%s\".\"%s\" where %s = '%v'", schema, table, indexCol, tmprow[indexColSeq-1])
//}
//func (or *OracleExecStruct) execRapirSql(db *sql.DB, sqlstr string) {
//	//执行sql语句不记录binlog
//	stmat, err := db.Prepare(sqlstr)
//	global.Wlog.Debug("GreatdbCheck prepare sql: \"", sqlstr, "\" at the MySQL")
//	if err != nil {
//		global.Wlog.Error("GreatdbCheck parpare sql fail. sql: ", sqlstr, "error info: ", err)
//	}
//	_, err = stmat.Exec()
//	if err != nil {
//		global.Wlog.Error("GreatdbCheck exec sql fail. sql: ", sqlstr, "error info: ", err)
//	}
//}
