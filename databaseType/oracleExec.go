package databaseType

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
