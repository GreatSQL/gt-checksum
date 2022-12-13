package actions

import (
	"database/sql"
	"errors"
	"fmt"
	"greatdbCheck/dbExec"
	"greatdbCheck/global"
	"greatdbCheck/inputArg"
	"os"
	"strings"
)

type schemaTable struct {
	schema              string
	table               string
	ignoreSchema        string
	ignoreTable         string
	sourceDrive         string
	destDrive           string
	sourceDB            *sql.DB
	destDB              *sql.DB
	lowerCaseTableNames string
	datefix             string
}

/*
 该函数用于根据库名来获取MySQL的表数据信息
 如果要检测单库下所有表，则需要输入库名.* ，如果要检测某一个表，则需要输入库名.表名
*/
func (stcls *schemaTable) tableList(dbNameList []string, logThreadSeq, logThreadSeq2 int64) []string {
	var tnS []string
	var tmpIgnoreMap = make(map[string]int)
	//处理排除的表
	alog := fmt.Sprintf("(%d) start init ignore table.", logThreadSeq)
	global.Wlog.Info(alog)
	if stcls.ignoreTable != "" {
		if strings.Contains(stcls.ignoreTable, ",") { //多个忽略表
			tmpIgT := strings.Split(stcls.ignoreTable, ",")
			for i := range tmpIgT {
				if strings.Contains(tmpIgT[i], ".") {
					dbname := strings.Split(tmpIgT[i], ".")[0]
					tablename := strings.Split(tmpIgT[i], ".")[1]
					key := fmt.Sprintf("%s.%s", dbname, tablename)
					tmpIgnoreMap[key] = 0
				}
			}
		} else { //单个忽略表
			if strings.Contains(stcls.ignoreTable, ".") {
				dbname := strings.Split(stcls.ignoreTable, ".")[0]
				tablename := strings.Split(stcls.ignoreTable, ".")[1]
				key := fmt.Sprintf("%s.%s", dbname, tablename)
				tmpIgnoreMap[key] = 0
			}
		}
	}
	blog := fmt.Sprintf("(%d) ignore table message is {%v} num [%d].", logThreadSeq, tmpIgnoreMap, len(tmpIgnoreMap))
	global.Wlog.Info(blog)

	//处理库列表，生成tableName 例如pcms.*
	var opt []string
	if len(dbNameList) > 0 {
		for i := range dbNameList {
			tmpa := fmt.Sprintf("%s.*", dbNameList[i])
			opt = append(opt, tmpa)
		}
	} else {
		opt = strings.Split(stcls.table, ",")
	}
	clog := fmt.Sprintf("(%d) start init check table info.", logThreadSeq)
	global.Wlog.Info(clog)
	if len(opt) > 0 {
		var tmpM = make(map[string]int)
		for _, op := range opt {
			if strings.Contains(op, ".") {
				dbName := strings.Split(op, ".")[0]
				tbName := strings.Split(op, ".")[1]
				tc := dbExec.TableColumnNameStruct{Schema: dbName, Table: tbName, Drive: stcls.sourceDrive, Db: stcls.sourceDB}
				squeryData, _ := tc.Query().TableNameList(stcls.sourceDB, logThreadSeq2)
				tc.Drive = stcls.destDrive
				tc.Db = stcls.destDB
				dqueryData, _ := tc.Query().TableNameList(stcls.destDB, logThreadSeq2)
				for _, i := range squeryData {
					var a string
					a = fmt.Sprintf("%s.%s", i["databaseName"].(string), i["tableName"].(string))
					if stcls.lowerCaseTableNames == "no" {
						a = strings.ToUpper(fmt.Sprintf("%s.%s", i["databaseName"].(string), i["tableName"].(string)))
					}
					if _, ok := tmpIgnoreMap[a]; ok {
						continue
					} else {
						tmpM[a]++
					}
				}
				for _, i := range dqueryData {
					var a string
					a = fmt.Sprintf("%s.%s", i["databaseName"].(string), i["tableName"].(string))
					if stcls.lowerCaseTableNames == "no" {
						a = strings.ToUpper(fmt.Sprintf("%s.%s", i["databaseName"].(string), i["tableName"].(string)))
					}
					if _, ok := tmpIgnoreMap[a]; ok {
						continue
					} else {
						tmpM[a]++
					}
				}
			}
		}
		for k, _ := range tmpM {
			tnS = append(tnS, k)
		}
	}
	dlog := fmt.Sprintf("(%d) chck table message is {%s} num [%d]", logThreadSeq, tnS, len(tnS))
	global.Wlog.Info(dlog)
	return tnS
}

/*
 该函数用于获取MySQL的数据库信息,返回库名列表，排除'information_schema','performance_schema','sys','mysql'
*/
func (stcls *schemaTable) schemaList(dbname string, ignoreSchema string, logThreadSeq1, logThreadSeq2 int64) []string {
	var ignschema string
	var dbCheckNameList []string
	if ignoreSchema != "" {
		tmpa := strings.Split(ignoreSchema, ",")
		ignschema = strings.Join(tmpa, "','")
	}
	alog := fmt.Sprintf("(%d) ignore Schema is {%s}", logThreadSeq1, ignschema)
	global.Wlog.Info(alog)
	//获取当前数据库信息列表
	tc := dbExec.TableColumnNameStruct{Schema: dbname, Table: stcls.table, Drive: stcls.sourceDrive, Db: stcls.sourceDB}
	blog := fmt.Sprintf("(%d) query check database list info.", logThreadSeq1)
	global.Wlog.Info(blog)
	dbCheckNameList = tc.Query().DatabaseNameList(ignschema, logThreadSeq2)
	clog := fmt.Sprintf("(%d) checksum database list message is {%s}", logThreadSeq1, dbCheckNameList)
	global.Wlog.Info(clog)
	return dbCheckNameList
}

/*
   查询待校验表的列名
*/
func (stcls *schemaTable) tableColumnName(db *sql.DB, drive string, logThreadSeq, logThreadSeq2 int64) string {
	var col string
	//var logThreadSeq int = 5
	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: drive}
	queryData, _ := tc.Query().TableColumnName(db, logThreadSeq2)
	alog := fmt.Sprintf("(%d) start dispos DB query columns data. to dispos it...", logThreadSeq)
	global.Wlog.Info(alog)
	for _, v := range queryData {
		if v["columnName"].(string) != "" {
			col += fmt.Sprintf("%s,", v["columnName"].(string))
		}
	}
	blog := fmt.Sprintf("(%d) complete dispos DB query columns data.", logThreadSeq)
	global.Wlog.Info(blog)
	return col
}

/*
	针对表的列名进行校验
*/
func (stcls *schemaTable) TableColumnNameCheck(checkTableList []string, logThreadSeq, logThreadSeq2 int64) ([]string, []string) {
	var newCheckTableList, abnormalTableList []string
	//var logThreadSeq int = 5
	elog := fmt.Sprintf("(%d) Start to get the source and target table structure and column information and check whether they are consistent", logThreadSeq)
	global.Wlog.Info(elog)
	global.Wlog.Info()
	for _, v := range checkTableList {
		var sColumn, dColumn string
		stcls.schema = strings.Split(v, ".")[0]
		stcls.table = strings.Split(v, ".")[1]
		if stcls.lowerCaseTableNames == "yes" {
			sColumn = stcls.tableColumnName(stcls.sourceDB, stcls.sourceDrive, logThreadSeq, logThreadSeq2)
			dColumn = stcls.tableColumnName(stcls.destDB, stcls.destDrive, logThreadSeq, logThreadSeq2)
		} else {
			sColumn = strings.ToUpper(stcls.tableColumnName(stcls.sourceDB, stcls.sourceDrive, logThreadSeq, logThreadSeq2))
			dColumn = strings.ToUpper(stcls.tableColumnName(stcls.destDB, stcls.destDrive, logThreadSeq, logThreadSeq2))
		}
		alog := fmt.Sprintf("(%d) source DB table name [%s.%s] column name message is {%s} num [%d]", logThreadSeq, stcls.schema, stcls.table, sColumn, len(strings.Split(sColumn, ","))-1)
		global.Wlog.Info(alog)
		blog := fmt.Sprintf("(%d) dest DB table name [%s.%s] column name message is {%s} num [%d]", logThreadSeq, stcls.schema, stcls.table, dColumn, len(strings.Split(dColumn, ","))-1)
		global.Wlog.Info(blog)

		//防止异构数据库中列明大小不一致
		clog := fmt.Sprintf("(%d) start diff source dest db table columns name, to check it...", logThreadSeq)
		global.Wlog.Info(clog)
		if CheckSum().CheckSha1(sColumn) == CheckSum().CheckSha1(dColumn) {
			newCheckTableList = append(newCheckTableList, v)
		} else {
			abnormalTableList = append(abnormalTableList, v)
		}
		dlog := fmt.Sprintf("(%d) complete checksum source dest db table columns name.", logThreadSeq)
		global.Wlog.Info(dlog)
	}
	return newCheckTableList, abnormalTableList
}

/*
	检查当前用户对该库表是否有响应的权限（权限包括：查询权限，flush_tables,session_variables_admin）
*/
func (stcls *schemaTable) GlobalAccessPriCheck(logThreadSeq, logThreadSeq2 int64) bool {
	//var logThreadSeq int = 19
	elog := fmt.Sprintf("(%d) Start to get the source and target Global Access Permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Info(elog)
	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive, Datafix: stcls.datefix}
	flog := fmt.Sprintf("(%d) Start to get the source Global Access Permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Info(flog)
	StableList := tc.Query().GlobalAccessPri(stcls.sourceDB, logThreadSeq2)
	glog := fmt.Sprintf("(%d) The Global Access Permission verification of the source DB is completed, and the status of the global access permission is {%v}.", logThreadSeq, StableList)
	global.Wlog.Info(glog)
	tc.Drive = stcls.destDrive
	hlog := fmt.Sprintf("(%d) Start to get the dest Global Access Permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Info(hlog)
	DtableList := tc.Query().GlobalAccessPri(stcls.destDB, logThreadSeq2)
	ilog := fmt.Sprintf("(%d) The Global Access Permission verification of the dest DB is completed, and the status of the global access permission is {%v}.", logThreadSeq, DtableList)
	global.Wlog.Info(ilog)
	if StableList && DtableList {
		jlog := fmt.Sprintf("(%d) The verification of the global access permission of the source and destination is completed", logThreadSeq)
		global.Wlog.Info(jlog)
		return true
	}
	klog := fmt.Sprintf("(%d) Some global access permissions are missing at the source and destination, and verification cannot continue.", logThreadSeq)
	global.Wlog.Error(klog)
	return false
}
func (stcls *schemaTable) TableAccessPriCheck(checkTableList []string, logThreadSeq, logThreadSeq2 int64) ([]string, []string) {
	var newCheckTableList, abnormalTableList []string
	elog := fmt.Sprintf("(%d) Start to get the source and target table access permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Info(elog)
	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
	flog := fmt.Sprintf("(%d) Start to get the source table access permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Info(flog)
	StableList, _ := tc.Query().TableAccessPriCheck(stcls.sourceDB, checkTableList, stcls.datefix, logThreadSeq2)
	glog := fmt.Sprintf("(%d) Complete the verification table permission verification of the source DB, the current verification table with permission is {%v}.", logThreadSeq, StableList)
	global.Wlog.Info(glog)
	tc.Drive = stcls.destDrive
	hlog := fmt.Sprintf("(%d) Start to get the dest table access permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Info(hlog)
	DtableList, _ := tc.Query().TableAccessPriCheck(stcls.destDB, checkTableList, stcls.datefix, logThreadSeq2)
	if len(DtableList) == 0 {
		ilog := fmt.Sprintf("(%d) Complete the verification table permission verification of the source DB, the current verification table with permission is {%v}.", logThreadSeq, StableList)
		global.Wlog.Error(ilog)
	} else {
		ilog := fmt.Sprintf("(%d) Complete the verification table permission verification of the source DB, the current verification table with permission is {%v}.", logThreadSeq, StableList)
		global.Wlog.Info(ilog)
	}
	jlog := fmt.Sprintf("(%d) Start processing the difference of the table to be checked at the source and target.", logThreadSeq)
	global.Wlog.Info(jlog)
	for k, _ := range StableList {
		if _, ok := DtableList[k]; ok {
			newCheckTableList = append(newCheckTableList, k)
		} else {
			abnormalTableList = append(abnormalTableList, k)
		}
	}
	klog := fmt.Sprintf("(%d) The difference processing of the table to be checked at the source and target ends is completed. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, newCheckTableList, len(newCheckTableList), abnormalTableList, len(abnormalTableList))
	global.Wlog.Info(klog)
	return newCheckTableList, abnormalTableList
}

/*
 该函数用于获取MySQL的表的索引信息,判断表是否存在索引，加入存在，获取索引的类型，以主键索引、唯一索引、普通索引及无索引，主键索引或唯一索引以自增id为优先
  缺少索引列为空或null的处理
*/
func (stcls *schemaTable) tableIndexAlgorithm(indexType map[string][]string) (string, []string) {
	if len(indexType) > 0 {
		//假如有单列主键索引，则选择单列主键索引
		if len(indexType["pri_single"]) > 0 {
			return "pri_single", indexType["pri_single"]
		}
		//假如没有单列主键索引，有多列主键索引，且有单列唯一索引，则选择单列唯一索引
		if len(indexType["uni_single"]) > 0 {
			return "uni_single", indexType["uni_single"]
		}

		//假如没有单列主键索引，有多列主键索引，没有单列唯一索引，则选择多列主键索引
		if len(indexType["pri_multiseriate"]) > 0 {
			return "pri_multiseriate", indexType["pri_multiseriate"]
		}

		//假如没有单列主键索引，有多列主键索引，没有单列唯一索引，有多列唯一索引， 则选择多列主键索引
		if len(indexType["uni_multiseriate"]) > 0 {
			return "uni_multiseriate", indexType["uni_multiseriate"]
		}

		//有单列索引存在
		if len(indexType["mui_single"]) >= 1 {
			return "mui_single", indexType["mui_single"]
		}

		//有无单列普通索引，和多列普通索引，选择多列普通索引
		if len(indexType["mui_multiseriate"]) > 1 {
			return "mui_multiseriate", indexType["mui_multiseriate"]
		}
	} else {
		var err = errors.New("Missing indexes")
		global.Wlog.Error("[check table index choose]GreatdbCheck Check table ", stcls.schema, ".", stcls.table, ", no indexed columns, checksum terminated", err)
		//return "noIndex", []string{""}
	}
	return "", nil
}

/*
	处理需要校验的库表
	将忽略的库表从校验列表中去除，如果校验列表为空则退出
*/
func (stcls *schemaTable) SchemaTableFilter(logThreadSeq1, logThreadSeq2 int64) []string {
	//根据配置文件中的过滤条件筛选需要校验的表
	var dbNameList []string
	alog := fmt.Sprintf("(%d) Start to init schema info.", logThreadSeq1)
	global.Wlog.Info(alog)
	//处理待校验的库数量
	if stcls.schema != "" {
		dbNameList = stcls.schemaList(stcls.schema, stcls.ignoreSchema, logThreadSeq1, logThreadSeq2)
		//判断校验的库是否为空，为空则退出
		if len(dbNameList) == 0 {
			blog := fmt.Sprintf("(%d) check Schema is emty, will exit!", logThreadSeq1)
			global.Wlog.Error(blog)
			os.Exit(1)
		}
	}
	clog := fmt.Sprintf("(%d) schema {%s} init sccessfully, num [%d].", logThreadSeq1, dbNameList, len(dbNameList))
	global.Wlog.Info(clog)

	dlog := fmt.Sprintf("(%d) Start to init table info.", logThreadSeq1)
	global.Wlog.Info(dlog)
	//处理表校验
	tableList := stcls.tableList(dbNameList, logThreadSeq1, logThreadSeq2)
	if len(tableList) == 0 {
		elog := fmt.Sprintf("(%d) check table is emty, will exit!", logThreadSeq1)
		global.Wlog.Error(elog)
		os.Exit(1)
	}
	flog := fmt.Sprintf("(%d) table {%s} init sccessfully, num [%d].", logThreadSeq1, tableList, len(tableList))
	global.Wlog.Info(flog)
	return tableList
}

/*
	库表的所有列信息
*/
func (stcls *schemaTable) SchemaTableAllCol(tableList []string, logThreadSeq, logThreadSeq2 int64) map[string]global.TableAllColumnInfoS {
	var tableCol = make(map[string]global.TableAllColumnInfoS)
	var interfToString = func(colData []map[string]interface{}) []map[string]string {
		kel := make([]map[string]string, 0)
		for i := range colData {
			ke := make(map[string]string)
			for ii, iv := range colData[i] {
				ke[ii] = fmt.Sprintf("%v", iv)
			}
			kel = append(kel, ke)
		}
		return kel
	}
	alog := fmt.Sprintf("(%d) Start to obtain the metadata information of the source-target verification table ...", logThreadSeq)
	global.Wlog.Info(alog)
	for _, i := range tableList {
		if strings.Contains(i, ".") {
			schema := strings.Split(i, ".")[0]
			table := strings.Split(i, ".")[1]
			blog := fmt.Sprintf("(%d) Start to query all column information of source DB %s table %s.%s", logThreadSeq, stcls.sourceDrive, schema, table)
			global.Wlog.Info(blog)
			tc := dbExec.TableColumnNameStruct{Schema: schema, Table: table, Drive: stcls.sourceDrive}
			a, _ := tc.Query().TableAllColumn(stcls.sourceDB, logThreadSeq2)
			clog := fmt.Sprintf("(%d) All column information query of source DB %s table %s.%s is completed", logThreadSeq, stcls.sourceDrive, schema, table)
			global.Wlog.Info(clog)

			dlog := fmt.Sprintf("(%d) Start to query all column information of dest DB %s table %s.%s", logThreadSeq, stcls.destDrive, schema, table)
			global.Wlog.Info(dlog)
			tc.Drive = stcls.destDrive
			b, _ := tc.Query().TableAllColumn(stcls.destDB, logThreadSeq2)
			elog := fmt.Sprintf("(%d) All column information query of dest DB %s table %s.%s is completed", logThreadSeq, stcls.destDrive, schema, table)
			global.Wlog.Info(elog)
			tableCol[fmt.Sprintf("%s_greatdbCheck_%s", schema, table)] = global.TableAllColumnInfoS{
				SColumnInfo: interfToString(a),
				DColumnInfo: interfToString(b),
			}
			flog := fmt.Sprintf("(%d) all column information query of table %s.%s is completed. table column message is {source: %s, dest: %s}", logThreadSeq, schema, table, interfToString(a), interfToString(b))
			global.Wlog.Info(flog)
		}
	}
	glog := fmt.Sprintf("(%d) The metadata information of the source target verification table has been obtained !!!", logThreadSeq)
	global.Wlog.Info(glog)
	return tableCol
}

/*
	获取校验表的索引列信息，包含是否有索引，列名，列序号
*/
func (stcls *schemaTable) TableIndexColumn(dtabS []string, logThreadSeq, logThreadSeq2 int64) map[string][]string {
	var tableIndexColumnMap = make(map[string][]string)
	alog := fmt.Sprintf("(%d) Start to query the table index listing information and select the appropriate index ...", logThreadSeq)
	global.Wlog.Info(alog)
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		blog := fmt.Sprintf("(%d) Start querying the index list information of table %s.%s.", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Info(blog)
		idxc := dbExec.IndexColumnStruct{Schema: stcls.schema, Table: stcls.table, Drivce: stcls.sourceDrive}
		queryData, _ := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive, Db: stcls.sourceDB}
		indexType := tc.Query().TableIndexChoice(queryData, logThreadSeq2)
		clog := fmt.Sprintf("(%d) Table %s.%s index list information query completed. index list message is {%v}", logThreadSeq, stcls.schema, stcls.table, indexType)
		global.Wlog.Info(clog)

		if len(indexType) == 0 { //针对于表没有索引的，进行处理
			key := fmt.Sprintf("%s/*greatdbSchemaTable*/%s", stcls.schema, stcls.table)
			tableIndexColumnMap[key] = []string{}
			dlog := fmt.Sprintf("(%d) The current table %s.%s has no index.", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Warn(dlog)
		} else {
			flog := fmt.Sprintf("(%d) Start to perform index selection on table %s.%s according to the algorithm", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Info(flog)
			ab, aa := stcls.tableIndexAlgorithm(indexType)
			key := fmt.Sprintf("%s/*greatdbSchemaTable*/%s/*indexColumnType*/%s", stcls.schema, stcls.table, ab)
			tableIndexColumnMap[key] = aa
			glog := fmt.Sprintf("(%d) The index selection of table %s.%s is completed, and the selected index information is { keyName:%s keyColumn: %s}", logThreadSeq, stcls.schema, stcls.table, ab, aa)
			global.Wlog.Info(glog)
		}
	}
	zlog := fmt.Sprintf("(%d) Table index listing information and appropriate index completion !!!", logThreadSeq)
	global.Wlog.Info(zlog)
	return tableIndexColumnMap
}

/*
	校验触发器
*/
func (stcls *schemaTable) Trigger(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var pods = Pod{
		Datafix:     "no",
		CheckObject: "trigger",
	}
	var z = make(map[string]int)
	var c, d []string
	alog := fmt.Sprintf("(%d) Start init check source and target DB Trigger. to check it...", logThreadSeq)
	global.Wlog.Info(alog)
	for _, i := range dtabS {
		i = strings.Split(i, ".")[0]
		if stcls.lowerCaseTableNames == "yes" {
			i = strings.ToUpper(i)
			z[i]++
		}
		if stcls.lowerCaseTableNames == "no" {
			z[i]++
		}
	}
	//校验触发器
	for i, _ := range z {
		pods.Schema = stcls.schema
		blog := fmt.Sprintf("(%d) Start processing source DB %s data databases %s Trigger. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema)
		global.Wlog.Info(blog)
		tc := dbExec.TableColumnNameStruct{Schema: i, Drive: stcls.sourceDrive}
		sourceTrigger, _ := tc.Query().Trigger(stcls.sourceDB, logThreadSeq2)
		clog := fmt.Sprintf("(%d) Source DB %s data databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, sourceTrigger)
		global.Wlog.Info(clog)

		dlog := fmt.Sprintf("(%d) Start processing dest DB %s data databases %s Trigger data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema)
		global.Wlog.Info(dlog)
		tc.Drive = stcls.destDrive
		destTrigger, _ := tc.Query().Trigger(stcls.destDB, logThreadSeq2)
		elog := fmt.Sprintf("(%d) Dest DB %s data databases %s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, destTrigger)
		global.Wlog.Info(elog)

		if len(sourceTrigger) == 0 && len(destTrigger) == 0 {
			continue
			flog := fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, stcls.schema)
			global.Wlog.Info(flog)
		}
		var tmpM = make(map[string]int)
		glog := fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Trigger. to dispos it...", logThreadSeq, stcls.schema)
		global.Wlog.Info(glog)
		for k, _ := range sourceTrigger {
			tmpM[k]++
		}
		for k, _ := range destTrigger {
			tmpM[k]++
		}
		hlog := fmt.Sprintf("(%d) Start to compare whether the Trigger is consistent.", logThreadSeq)
		global.Wlog.Info(hlog)
		for k, _ := range tmpM {
			pods.TriggerName = strings.ReplaceAll(strings.Split(k, ".")[1], "\"", "")
			if sourceTrigger[k] != destTrigger[k] {
				pods.Differences = "yes"
				d = append(d, k)
			} else {
				pods.Differences = "no"
				c = append(c, k)
			}
			zlog := fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Trigger. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, stcls.schema, c, len(c), d, len(d))
			global.Wlog.Info(zlog)
			llog := fmt.Sprintf("(%d) The source target segment databases %s Trigger data verification is completed.!!!", logThreadSeq, stcls.schema)
			global.Wlog.Info(llog)
			measuredDataPods = append(measuredDataPods, pods)
		}
		zlog := fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Trigger data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
		global.Wlog.Info(zlog)
	}
}

/*
	校验存储过程
*/
func (stcls *schemaTable) Proc(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var schemaMap = make(map[string]int)
	var pods = Pod{
		Datafix:     "no",
		CheckObject: "proc",
	}
	alog := fmt.Sprintf("(%d) Start init check source and target DB Stored Procedure. to check it...", logThreadSeq)
	global.Wlog.Info(alog)
	for _, i := range dtabS {
		schemaMap[strings.Split(i, ".")[0]] = +schemaMap[strings.Split(i, ".")[0]]
	}
	var c, d []string
	for schema, _ := range schemaMap {
		blog := fmt.Sprintf("(%d) Start processing source DB %s data databases %s Stored Procedure. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema)
		global.Wlog.Info(blog)
		tc := dbExec.TableColumnNameStruct{Schema: schema, Drive: stcls.sourceDrive}
		sourceProc, _ := tc.Query().Proc(stcls.sourceDB, logThreadSeq2)
		clog := fmt.Sprintf("(%d) Source DB %s data databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, sourceProc)
		global.Wlog.Info(clog)

		tc.Drive = stcls.destDrive
		dlog := fmt.Sprintf("(%d) Start processing dest DB %s data table %s Stored Procedure data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Info(dlog)
		destProc, _ := tc.Query().Proc(stcls.destDB, logThreadSeq2)
		elog := fmt.Sprintf("(%d) Dest DB %s data databases %s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, destProc)
		global.Wlog.Info(elog)

		if len(sourceProc) == 0 && len(destProc) == 0 {
			continue
			flog := fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, stcls.schema)
			global.Wlog.Info(flog)
		}
		var tmpM = make(map[string]int)
		glog := fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Stored Procedure. to dispos it...", logThreadSeq, stcls.schema)
		global.Wlog.Info(glog)
		for k, _ := range sourceProc {
			if k == "DEFINER" {
				continue
			}
			tmpM[k]++
		}
		for k, _ := range destProc {
			if k == "DEFINER" {
				continue
			}
			tmpM[k]++
		}
		flog := fmt.Sprintf("(%d) Start to compare whether the Stored Procedure is consistent.", logThreadSeq)
		global.Wlog.Info(flog)
		pods.Schema = schema
		for k, v := range tmpM {
			if stcls.sourceDrive != stcls.destDrive {
				if v == 2 {
					pods.ProcName = k
					pods.Differences = "no"
					c = append(c, k)
				} else {
					pods.ProcName = k
					pods.Differences = "yes"
					d = append(d, k)
				}
			} else {
				if sourceProc[k] != destProc[k] {
					pods.ProcName = k
					pods.Differences = "yes"
					d = append(d, k)
				} else {
					pods.ProcName = k
					pods.Differences = "no"
					c = append(c, k)
				}
			}
			zlog := fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Stored Procedure. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, stcls.schema, c, len(c), d, len(d))
			global.Wlog.Info(zlog)
			llog := fmt.Sprintf("(%d) The source target segment databases %s Stored Procedure data verification is completed.!!!", logThreadSeq, stcls.schema)
			global.Wlog.Info(llog)
			measuredDataPods = append(measuredDataPods, pods)
		}
		zlog := fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Stored Procedure data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
		global.Wlog.Info(zlog)
	}
}

/*
	校验函数
*/
func (stcls *schemaTable) Func(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var schemaMap = make(map[string]int)
	var pods = Pod{
		Datafix:     "no",
		CheckObject: "func",
	}
	alog := fmt.Sprintf("(%d) Start init check source and target DB Stored Function. to check it...", logThreadSeq)
	global.Wlog.Info(alog)
	for _, i := range dtabS {
		schemaMap[strings.Split(i, ".")[0]] = +schemaMap[strings.Split(i, ".")[0]]
	}
	var c, d []string
	for schema, _ := range schemaMap {
		blog := fmt.Sprintf("(%d) Start processing source DB %s data databases %s Stored Function. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema)
		global.Wlog.Info(blog)
		tc := dbExec.TableColumnNameStruct{Schema: schema, Drive: stcls.sourceDrive}
		sourceFunc, _ := tc.Query().Func(stcls.sourceDB, logThreadSeq2)
		clog := fmt.Sprintf("(%d) Source DB %s data databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, sourceFunc)
		global.Wlog.Info(clog)

		tc.Drive = stcls.destDrive
		dlog := fmt.Sprintf("(%d) Start processing dest DB %s data table %s Stored Function data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Info(dlog)
		destFunc, _ := tc.Query().Func(stcls.destDB, logThreadSeq2)
		elog := fmt.Sprintf("(%d) Dest DB %s data databases %s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, destFunc)
		global.Wlog.Info(elog)

		if len(sourceFunc) == 0 && len(destFunc) == 0 {
			continue
			flog := fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, stcls.schema)
			global.Wlog.Info(flog)
		}
		var tmpM = make(map[string]int)
		glog := fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Stored Function. to dispos it...", logThreadSeq, stcls.schema)
		global.Wlog.Info(glog)
		for k, _ := range sourceFunc {
			tmpM[k]++
		}
		for k, _ := range destFunc {
			tmpM[k]++
		}
		hlog := fmt.Sprintf("(%d) Start to compare whether the Stored Function is consistent.", logThreadSeq)
		global.Wlog.Info(hlog)
		pods.Schema = schema
		for k, v := range tmpM {
			var sv, dv string
			if stcls.sourceDrive != stcls.destDrive { //异构,只校验函数名
				if v == 2 {
					pods.FuncName = k
					pods.Differences = "no"
					c = append(c, k)
				} else {
					pods.FuncName = k
					pods.Differences = "yes"
					d = append(d, k)
				}
			} else { //相同架构，校验函数结构体
				sv, dv = sourceFunc[k], destFunc[k]
				if sv != dv {
					pods.FuncName = k
					pods.Differences = "yes"
					d = append(d, k)
				} else {
					pods.FuncName = k
					pods.Differences = "no"
					c = append(c, k)
				}
			}
			zlog := fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Stored Function. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, stcls.schema, c, len(c), d, len(d))
			global.Wlog.Info(zlog)
			llog := fmt.Sprintf("(%d) The source target segment databases %s Stored Function data verification is completed.!!!", logThreadSeq, stcls.schema)
			global.Wlog.Info(llog)
			measuredDataPods = append(measuredDataPods, pods)
		}
		zlog := fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Stored Function data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
		global.Wlog.Info(zlog)
	}
}

/*
	校验函数
*/

func (stcls *schemaTable) IndexDisposF(queryData []map[string]interface{}) ([]string, map[string][]string, map[string][]string) {
	nultiseriateIndexColumnMap := make(map[string][]string)
	multiseriateIndexColumnMap := make(map[string][]string)
	var PriIndexCol, uniIndexCol, mulIndexCol []string
	var indexName string
	for _, v := range queryData {
		var currIndexName = strings.ToUpper(v["indexName"].(string))
		//判断唯一索引（包含主键索引和普通索引）
		if stcls.sourceDrive == "mysql" {

		}
		if v["nonUnique"].(string) == "0" || v["nonUnique"].(string) == "UNIQUE" {
			if currIndexName == "PRIMARY" || v["columnKey"].(string) == "1" {
				if currIndexName != indexName {
					indexName = currIndexName
				}
				PriIndexCol = append(PriIndexCol, fmt.Sprintf("%s", v["columnName"]))
			} else {
				if currIndexName != indexName {
					indexName = currIndexName
					nultiseriateIndexColumnMap[indexName] = append(uniIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
				} else {
					nultiseriateIndexColumnMap[indexName] = append(nultiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
				}
			}
		}
		//处理普通索引
		if v["nonUnique"].(string) == "1" || (v["nonUnique"].(string) == "NONUNIQUE" && v["columnKey"].(string) == "0") {
			if currIndexName != indexName {
				indexName = currIndexName
				multiseriateIndexColumnMap[indexName] = append(mulIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			} else {
				multiseriateIndexColumnMap[indexName] = append(multiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			}
		}
	}
	return PriIndexCol, nultiseriateIndexColumnMap, multiseriateIndexColumnMap
}

func (stcls *schemaTable) Foreign(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var pods = Pod{
		Datafix:     "no",
		CheckObject: "Foreign",
	}
	alog := fmt.Sprintf("(%d) Start init check source and target DB Foreign. to check it...", logThreadSeq)
	global.Wlog.Info(alog)
	//校验外键
	var c, d []string
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		blog := fmt.Sprintf("(%d) Start processing source DB %s data table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table)
		global.Wlog.Info(blog)
		pods.Schema = stcls.schema
		pods.Table = stcls.table
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		sourceForeign, _ := tc.Query().Foreign(stcls.sourceDB, logThreadSeq2)
		clog := fmt.Sprintf("(%d) Source DB %s data table %s.%s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table, sourceForeign)
		global.Wlog.Info(clog)

		dlog := fmt.Sprintf("(%d) Start processing dest DB %s data table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Info(dlog)
		tc.Drive = stcls.destDrive
		destForeign, _ := tc.Query().Foreign(stcls.destDB, logThreadSeq2)
		elog := fmt.Sprintf("(%d) Dest DB %s data table %s.%s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table, destForeign)
		global.Wlog.Info(elog)

		if len(sourceForeign) == 0 && len(destForeign) == 0 {
			continue
			flog := fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Info(flog)
		}
		var tmpM = make(map[string]int)
		glog := fmt.Sprintf("(%d) Start seeking the union of the source and target table %s.%s Foreign Name. to dispos it...", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Info(glog)
		for k, _ := range sourceForeign {
			tmpM[k]++
		}
		for k, _ := range destForeign {
			tmpM[k]++
		}
		hlog := fmt.Sprintf("(%d) Start to compare whether the Foreign table is consistent.", logThreadSeq)
		global.Wlog.Info(hlog)
		for k, _ := range tmpM {
			if sourceForeign[k] != destForeign[k] {
				pods.Differences = "yes"
				d = append(d, k)
			} else {
				pods.Differences = "no"
				c = append(c, k)
			}
		}
		zlog := fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s Foreign. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, stcls.schema, stcls.table, c, len(c), d, len(d))
		global.Wlog.Info(zlog)
		llog := fmt.Sprintf("(%d) The source target segment table %s.%s Foreign data verification is completed.!!!", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Info(llog)
		measuredDataPods = append(measuredDataPods, pods)
	}
	zlog := fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Foreign data. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(zlog)
}

//校验分区
func (stcls *schemaTable) Partitions(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var pods = Pod{
		Datafix:     "no",
		CheckObject: "Partitions",
	}
	alog := fmt.Sprintf("(%d) Start init check source and target DB partition table. to check it...", logThreadSeq)
	global.Wlog.Info(alog)
	var c, d []string
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		blog := fmt.Sprintf("(%d) Start processing source DB %s data table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table)
		global.Wlog.Info(blog)
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		sourcePartitions, _ := tc.Query().Partitions(stcls.sourceDB, logThreadSeq2)
		clog := fmt.Sprintf("(%d) Source DB %s data table %s.%s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table, sourcePartitions)
		global.Wlog.Info(clog)

		tc.Drive = stcls.destDrive
		dlog := fmt.Sprintf("(%d) Start processing dest DB %s data table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Info(dlog)
		destPartitions, _ := tc.Query().Partitions(stcls.destDB, logThreadSeq2)
		elog := fmt.Sprintf("(%d) Dest DB %s data table %s.%s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table, destPartitions)
		global.Wlog.Info(elog)

		pods.Schema = stcls.schema
		pods.Table = stcls.table
		if len(sourcePartitions) == 0 && len(destPartitions) == 0 {
			continue
			flog := fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Info(flog)
		}
		var tmpM = make(map[string]int)
		glog := fmt.Sprintf("(%d) Start seeking the union of the source and target table %s.%s Partitions Column. to dispos it...", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Info(glog)
		for k, _ := range sourcePartitions {
			tmpM[k]++
		}
		for k, _ := range destPartitions {
			tmpM[k]++
		}
		hlog := fmt.Sprintf("(%d) Start to compare whether the partitions table is consistent.", logThreadSeq)
		global.Wlog.Info(hlog)
		for k, _ := range tmpM {
			if strings.Join(strings.Fields(sourcePartitions[k]), "") != strings.Join(strings.Fields(destPartitions[k]), "") {
				pods.Differences = "yes"
				d = append(d, k)
			} else {
				c = append(c, k)
				pods.Differences = "no"
			}
		}
		zlog := fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s partitions. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, stcls.schema, stcls.table, c, len(c), d, len(d))
		global.Wlog.Info(zlog)
		llog := fmt.Sprintf("(%d) The source target segment table %s.%s partitions data verification is completed.!!!", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Info(llog)
		measuredDataPods = append(measuredDataPods, pods)
	}
	zlog := fmt.Sprintf("(%d) Complete the consistency check of the source target segment table partitions data. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(zlog)
}

func (stcls *schemaTable) Index(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		abNormalTableM, normalTableM = make(map[string][]string), make(map[string][]string)
		indexCompare                 = func(smul, dmul map[string][]string) ([]string, []string) {
			var abNormalTableS, normalTableS []string
			var tmpa = make(map[string]int)
			for k, _ := range smul {
				tmpa[k]++
			}
			for k, _ := range dmul {
				tmpa[k]++
			}
			for k, _ := range tmpa {
				var sv, dv []string
				if _, ok := smul[k]; ok {
					sv = smul[k]
				}
				if _, ok := dmul[k]; ok {
					dv = dmul[k]
				}
				if strings.Join(sv, ",") != strings.Join(dv, ",") {
					abNormalTableS = append(abNormalTableS, k)
				} else {
					normalTableS = append(normalTableS, k)
				}
			}
			return normalTableS, abNormalTableS
		}
	)
	//校验索引
	alog := fmt.Sprintf("(%d) start init check source and target DB index Column. to check it...", logThreadSeq)
	global.Wlog.Info(alog)
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		idxc := dbExec.IndexColumnStruct{Schema: stcls.schema, Table: stcls.table, Drivce: stcls.sourceDrive}
		slog := fmt.Sprintf("(%d) Start processing source DB %s data table %s.%s index column data. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table)
		global.Wlog.Info(slog)
		squeryData, _ := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
		spri, suni, smul := idxc.TableIndexColumn().IndexDisposF(squeryData, logThreadSeq2)
		idxc.Drivce = stcls.destDrive
		dlog := fmt.Sprintf("(%d) Start processing dest DB %s data table %s.%s index column data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Info(dlog)
		dqueryData, _ := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.destDB, logThreadSeq2)
		dpri, duni, dmul := idxc.TableIndexColumn().IndexDisposF(dqueryData, logThreadSeq2)
		st := fmt.Sprintf("%s.%s", stcls.schema, stcls.table)
		//先比较主键索引
		plog := fmt.Sprintf("(%d) Start to compare whether the primary key index is consistent.", logThreadSeq)
		global.Wlog.Info(plog)
		if strings.Join(spri, ",") != strings.Join(dpri, ",") {
			abNormalTableM[st] = spri
		} else {
			normalTableM[st] = spri
		}
		plog = fmt.Sprintf("(%d) Compare whether the primary key index is consistent and verified. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, normalTableM, len(normalTableM), abNormalTableM, len(abNormalTableM))
		global.Wlog.Info(plog)

		ulog := fmt.Sprintf("(%d) Start to compare whether the unique key index is consistent.", logThreadSeq)
		global.Wlog.Info(ulog)
		//在比较唯一索引
		var c, d []string
		if len(duni) > 0 || len(suni) > 0 {
			c, d = indexCompare(suni, duni)
			normalTableM[st] = append(normalTableM[st], c...)
			abNormalTableM[st] = append(abNormalTableM[st], d...)
		}
		ulog = fmt.Sprintf("(%d) Compare whether the unique key index is consistent and verified. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
		global.Wlog.Info(ulog)

		nlog := fmt.Sprintf("(%d) Start to compare whether the no-unique key index is consistent.", logThreadSeq)
		global.Wlog.Info(nlog)
		//比较普通索引
		if len(smul) > 0 || len(dmul) > 0 {
			c, d = indexCompare(smul, dmul)
			normalTableM[st] = append(normalTableM[st], c...)
			abNormalTableM[st] = append(abNormalTableM[st], d...)
		}
		nlog = fmt.Sprintf("(%d) Compare whether the no-unique key index is consistent and verified. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
		global.Wlog.Info(nlog)

		dlog = fmt.Sprintf("(%d) The source target segment table %s.%s index column data verification is completed.!!!", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Info(dlog)
	}
	zlog := fmt.Sprintf("(%d) Complete the consistency check of the source target segment table index column. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, normalTableM, len(normalTableM), abNormalTableM, len(abNormalTableM))
	global.Wlog.Info(zlog)
	var pods = Pod{
		Datafix:     "no",
		CheckObject: "Index",
	}
	for k, v := range normalTableM {
		aa := strings.Split(k, ".")
		pods.Schema = aa[0]
		pods.Table = aa[1]
		pods.IndexCol = fmt.Sprintf("%s", strings.Join(v, ","))
		if _, ok := abNormalTableM[k]; !ok {
			pods.Differences = "no"
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	for k, v := range abNormalTableM {
		aa := strings.Split(k, ".")
		pods.Schema = aa[0]
		pods.Table = aa[1]
		pods.IndexCol = fmt.Sprintf("%s", strings.Join(v, ","))
		pods.Differences = "yes"
		measuredDataPods = append(measuredDataPods, pods)
	}
}
func (stcls *schemaTable) Struct(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	//校验列名
	alog := fmt.Sprintf("(%d) begin check source and target struct. check object is {%s} num[%d]", logThreadSeq, dtabS, len(dtabS))
	global.Wlog.Info(alog)
	normal, abnormal := stcls.TableColumnNameCheck(dtabS, logThreadSeq, logThreadSeq2)
	blog := fmt.Sprintf("(%d) Complete the data consistency check of the source target segment table structure column. normal table message is {%s} num [%d], abnormal table message is {%s} num [%d].", logThreadSeq, normal, len(normal), abnormal, len(abnormal))
	global.Wlog.Info(blog)
	//输出校验结果信息
	var pods = Pod{
		IndexCol:    "no",
		CheckMod:    "columnName",
		Datafix:     "no",
		CheckObject: "Struct",
	}
	for _, i := range normal {
		aa := strings.Split(i, ".")
		pods.Schema = aa[0]
		pods.Table = aa[1]
		pods.Differences = "no"
		measuredDataPods = append(measuredDataPods, pods)
	}
	for _, i := range abnormal {
		aa := strings.Split(i, ".")
		pods.Schema = aa[0]
		pods.Table = aa[1]
		pods.Differences = "yes"
		measuredDataPods = append(measuredDataPods, pods)
	}
	clog := fmt.Sprintf("(%d) check source and target DB table struct complete!!!", logThreadSeq)
	global.Wlog.Info(clog)
	//校验列类型
}

/*
	用于测试db链接串是否正确，是否可以连接
*/
func dbOpenTest(drive, jdbc string) *sql.DB {
	p := dbExec.DBexec()
	p.JDBC = jdbc
	p.DBDevice = drive
	db, err := p.OpenDB()
	if err != nil {
		fmt.Println("")
		os.Exit(1)
	}
	err1 := db.Ping()
	if err1 != nil {
		os.Exit(1)
	}
	return db
}

/*
	库表的初始化
*/
func SchemaTableInit(m *inputArg.ConfigParameter) *schemaTable {
	sdb := dbOpenTest(m.SourceDrive, m.SourceJdbc)
	ddb := dbOpenTest(m.DestDrive, m.DestJdbc)
	return &schemaTable{
		ignoreSchema:        m.Igschema,
		ignoreTable:         m.Igtable,
		schema:              m.Schema,
		table:               m.Table,
		sourceDrive:         m.SourceDrive,
		destDrive:           m.DestDrive,
		sourceDB:            sdb,
		destDB:              ddb,
		lowerCaseTableNames: m.LowerCaseTableNames,
		datefix:             m.Datafix,
	}
}
