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
}

/*
 该函数用于根据库名来获取MySQL的表数据信息
 如果要检测单库下所有表，则需要输入库名.* ，如果要检测某一个表，则需要输入库名.表名
*/
func (stcls *schemaTable) tableList(dbNameList []string) []string {
	var tnS []string
	var tmpIgnoreMap = make(map[string]int)
	//处理排除的表
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
	global.Wlog.Info("[check Table] ignore table is ", tmpIgnoreMap)
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
	if len(opt) > 0 {
		var tmpM = make(map[string]int)
		for _, op := range opt {
			if strings.Contains(op, ".") {
				dbName := strings.Split(op, ".")[0]
				tbName := strings.Split(op, ".")[1]
				tc := dbExec.TableColumnNameStruct{Schema: dbName, Table: tbName, Drive: stcls.sourceDrive, Db: stcls.sourceDB}
				squeryData, _ := tc.Query().TableNameList(stcls.sourceDB)
				tc.Drive = stcls.destDrive
				tc.Db = stcls.destDB
				dqueryData, _ := tc.Query().TableNameList(stcls.destDB)
				for _, i := range squeryData {
					a := fmt.Sprintf("%s.%s", i["databaseName"].(string), i["tableName"].(string))
					if _, ok := tmpIgnoreMap[a]; ok {
						continue
					} else {
						tmpM[a]++
					}
				}
				for _, i := range dqueryData {
					a := fmt.Sprintf("%s.%s", i["databaseName"].(string), i["tableName"].(string))
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
	global.Wlog.Info("[check table] chck table is ", tnS)
	return tnS
}

/*
 该函数用于获取MySQL的数据库信息,返回库名列表，排除'information_schema','performance_schema','sys','mysql'
*/
func (stcls *schemaTable) schemaList(dbname string, ignoreSchema string) []string {
	var ignschema string
	var dbCheckNameList []string
	if ignoreSchema != "" {
		tmpa := strings.Split(ignoreSchema, ",")
		ignschema = strings.Join(tmpa, "','")
	}
	global.Wlog.Info("[ignoreSchema] check ignore Schema is ", ignschema)
	//获取当前数据库信息列表

	tc := dbExec.TableColumnNameStruct{Schema: dbname, Table: stcls.table, Drive: stcls.sourceDrive, Db: stcls.sourceDB}
	dbCheckNameList = tc.Query().DatabaseNameList(ignschema)
	global.Wlog.Info("[checkSum Schema] checksum database list info: ", dbCheckNameList)
	return dbCheckNameList
}

/*
   查询待校验表的列名
*/
func (stcls *schemaTable) tableColumnName(db *sql.DB, drive string) string {
	var col string
	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: drive}
	queryData, _ := tc.Query().TableColumnName(db)
	for _, v := range queryData {
		col += fmt.Sprintf("%s,", v["columnName"].(string))
	}
	global.Wlog.Info("[check table column name] checksum table Column info is ", stcls.schema, ".", stcls.table, ":", queryData)
	return col
}

/*
	针对表的列名进行校验
*/
func (stcls *schemaTable) TableColumnNameCheck(checkTableList []string) ([]string, []string) {
	global.Wlog.Info("[check table Column Name] begin check source and dest table column name differences.")
	var newCheckTableList, abnormalTableList []string
	for _, v := range checkTableList {
		var sColumn, dColumn string
		stcls.schema = strings.Split(v, ".")[0]
		stcls.table = strings.Split(v, ".")[1]
		if stcls.lowerCaseTableNames == "yes" {
			sColumn = stcls.tableColumnName(stcls.sourceDB, stcls.sourceDrive)
			dColumn = stcls.tableColumnName(stcls.destDB, stcls.destDrive)
		} else {
			sColumn = strings.ToUpper(stcls.tableColumnName(stcls.sourceDB, stcls.sourceDrive))
			dColumn = strings.ToUpper(stcls.tableColumnName(stcls.destDB, stcls.destDrive))
		}
		//防止异构数据库中列明大小不一致
		if CheckSum().CheckSha1(sColumn) == CheckSum().CheckSha1(dColumn) {
			newCheckTableList = append(newCheckTableList, v)
		} else {
			abnormalTableList = append(abnormalTableList, v)
		}
	}
	global.Wlog.Info("[check table Column Name] check table info is ", newCheckTableList)
	return newCheckTableList, abnormalTableList
}

/*
   检查表索引列信息
*/
func (stcls *schemaTable) indexColumnList(queryData []map[string]interface{}) map[string][]string {
	global.Wlog.Debug("actions init db Example.")
	var indexType = make(map[string][]string)
	nultiseriateIndexColumnMap := make(map[string][]string)
	multiseriateIndexColumnMap := make(map[string][]string)
	breakIndexColumnType := []string{"INT", "CHAR", "YEAR", "DATE", "TIME"}
	var PriIndexCol, uniIndexCol, mulIndexCol []string
	var indexName string

	if len(queryData) == 0 {
		return indexType
	}
	//索引列处理，联合索引进行列合并
	//去除主键索引列、唯一索引列、普通索引列的所有列明
	for v := range queryData {
		if queryData[v]["nonUnique"].(string) == "0" {
			if queryData[v]["indexName"] == "PRIMARY" {
				if queryData[v]["indexName"].(string) != indexName {
					indexName = queryData[v]["indexName"].(string)
				}
				PriIndexCol = append(PriIndexCol, fmt.Sprintf("%s", queryData[v]["columnName"]))
			} else {
				if queryData[v]["indexName"].(string) != indexName {
					indexName = queryData[v]["indexName"].(string)
					nultiseriateIndexColumnMap[indexName] = append(uniIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", queryData[v]["columnName"], queryData[v]["columnType"]))
				} else {
					nultiseriateIndexColumnMap[indexName] = append(nultiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", queryData[v]["columnName"], queryData[v]["columnType"]))
				}
			}
		} else {
			if queryData[v]["indexName"].(string) != indexName {
				indexName = queryData[v]["indexName"].(string)
				multiseriateIndexColumnMap[indexName] = append(mulIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", queryData[v]["columnName"], queryData[v]["columnType"]))
			} else {
				multiseriateIndexColumnMap[indexName] = append(multiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", queryData[v]["columnName"], queryData[v]["columnType"]))
			}
		}
	}

	//处理主键索引列
	//判断是否存在主键索引,每个表的索引只有一个
	infoStr := fmt.Sprintf("Greatdbcheck Checks whether table %s.%s has a primary key index", stcls.schema, stcls.table)
	global.Wlog.Info(infoStr)
	if len(PriIndexCol) == 1 { //单列主键索引
		indexType["pri_single"] = PriIndexCol
	} else if len(PriIndexCol) > 1 { //联合主键索引
		indexType["pri_multiseriate"] = PriIndexCol
	}

	// ----- 处理唯一索引列，根据选择规则选择一个单列索引，（选择次序：int<--char<--year<--date<-time<-其他）
	infoStr = fmt.Sprintf("Greatdbcheck Checks whether table %s.%s has a unique key index", stcls.schema, stcls.table)
	global.Wlog.Info(infoStr)
	var tmpSliceNum = 1
	var tmpSliceNumMap = make(map[string]int)
	//先找出联合索引数量最多的
	for _, i := range nultiseriateIndexColumnMap {
		if len(i) > tmpSliceNum {
			tmpSliceNum = len(i)
		}
	}
	//针对多个最长的联合索引列进行map匹配
	for k, i := range nultiseriateIndexColumnMap {
		if len(i) == tmpSliceNum {
			tmpSliceNumMap[k] = tmpSliceNum
		}
	}

	//唯一索引判断选择
	indexName = ""
	//单列唯一索引
	if len(nultiseriateIndexColumnMap) > 0 {
		//处理单列索引，找出合适的索引列（选择次序：int<--char<--year<--date<-time）
		for i := range nultiseriateIndexColumnMap {
			if len(nultiseriateIndexColumnMap[i]) > 1 {
				continue
			}
			tmpa := strings.Split(strings.Join(nultiseriateIndexColumnMap[i], ""), " /*actions Column Type*/ ")
			indexColType := tmpa[1]
			var tmpaa []string
			breakStatus := false
			for v := range breakIndexColumnType {
				if strings.Contains(strings.ToUpper(indexColType), breakIndexColumnType[v]) {
					indexType["uni_single"] = append(tmpaa, tmpa[0])
					breakStatus = true
					break
				}
			}
			if breakStatus {
				break
			}
			indexType["uni_single"] = append(tmpaa, tmpa[0])
		}
		//处理多列索引
		var tmpa, tmpc = make(map[string][]string), make(map[string]int)
		for k, i := range nultiseriateIndexColumnMap {
			if len(i) <= 1 {
				continue
			}
			//如果是多列索引，择选找出当给列最多的
			var nultIndexColumnSlice, nultIndexColumnTypeSlice []string
			for v := range i {
				tmpiv := strings.ReplaceAll(i[v], " /*actions Column Type*/ ", ",")
				tmpaa := strings.Split(tmpiv, ",")
				nultIndexColumnSlice = append(nultIndexColumnSlice, tmpaa[0])
				nultIndexColumnTypeSlice = append(nultIndexColumnTypeSlice, tmpaa[1])
			}
			tmpIntCount := strings.Count(strings.ToUpper(strings.Join(nultIndexColumnTypeSlice, ",")), "INT")
			tmpCharCount := strings.Count(strings.ToUpper(strings.Join(nultIndexColumnTypeSlice, ",")), "CHAR")
			if tmpIntCount >= tmpCharCount && tmpIntCount != 0 {
				tmpc[k] = tmpIntCount
			} else if tmpIntCount < tmpCharCount && tmpCharCount != 0 {
				tmpc[k] = tmpCharCount
			} else {
				tmpc[k] = 0
			}
			tmpa[k] = nultIndexColumnSlice
		}
		var intCharMax int
		for k, v := range tmpc {
			if v > intCharMax {
				intCharMax = v
				indexType["nui_multiseriate"] = tmpa[k]
			} else {
				if v == 0 {
					indexType["nui_multiseriate"] = tmpa[k]
				}
			}
		}
	}

	// ----- 判断是否存在普通索引,选出索引列，优先选择多列索引，如果没有，则按优先级选择。
	infoStr = fmt.Sprintf("Greatdbcheck Checks whether table %s.%s has a key index", stcls.schema, stcls.table)
	global.Wlog.Info(infoStr)
	//先找出联合索引数量最多的
	tmpSliceNum = 1
	for i := range multiseriateIndexColumnMap {
		if len(multiseriateIndexColumnMap[i]) > tmpSliceNum {
			tmpSliceNum = len(multiseriateIndexColumnMap[i])
		}
	}
	if len(multiseriateIndexColumnMap) > 0 {
		//处理单列索引，找出合适的索引列（选择次序：int<--char<--year<--date<-time）
		for i := range multiseriateIndexColumnMap {
			if len(multiseriateIndexColumnMap[i]) > 1 { //单列索引
				continue
			}
			tmpa := strings.Split(strings.Join(multiseriateIndexColumnMap[i], ""), " /*actions Column Type*/ ")
			indexColType := tmpa[1]
			var tmpaa []string
			breakStatus := false
			for v := range breakIndexColumnType {
				if strings.Contains(strings.ToUpper(indexColType), breakIndexColumnType[v]) {
					indexType["mui_single"] = append(tmpaa, tmpa[0])
					breakStatus = true
					break
				}
			}
			if breakStatus {
				break
			}
			indexType["mui_single"] = append(tmpaa, tmpa[0])
		}
		//处理多列索引
		var tmpa, tmpc = make(map[string][]string), make(map[string]int)
		for k, i := range multiseriateIndexColumnMap {
			if len(i) <= 1 {
				continue
			}
			//多列索引选择
			var multIndexColumnSlice, multIndexColumnTypeSlice []string
			for v := range i {
				tmpiv := strings.ReplaceAll(i[v], " /*actions Column Type*/ ", ",")
				tmpaa := strings.Split(tmpiv, ",")
				multIndexColumnSlice = append(multIndexColumnSlice, tmpaa[0])
				multIndexColumnTypeSlice = append(multIndexColumnTypeSlice, tmpaa[1])
			}
			tmpIntCount := strings.Count(strings.ToUpper(strings.Join(multIndexColumnTypeSlice, ",")), "INT")
			tmpCharCount := strings.Count(strings.ToUpper(strings.Join(multIndexColumnTypeSlice, ",")), "CHAR")
			if tmpIntCount >= tmpCharCount && tmpIntCount != 0 {
				tmpc[k] = tmpIntCount
			} else if tmpIntCount < tmpCharCount && tmpCharCount != 0 {
				tmpc[k] = tmpCharCount
			} else {
				tmpc[k] = 0
			}
			tmpa[k] = multIndexColumnSlice
		}
		var intCharMax int
		for k, v := range tmpc {
			if v > intCharMax {
				intCharMax = v
				indexType["mui_multiseriate"] = tmpa[k]
			} else {
				if v == 0 {
					indexType["mui_multiseriate"] = tmpa[k]
				}
			}
		}
		//if len(i) == tmpSliceNum { //加入最多的有多个
		//	tmpIntCount := strings.Count(strings.ToUpper(strings.Join(multIndexColumnTypeSlice, ",")), "INT")
		//	tmpCharCount := strings.Count(strings.ToUpper(strings.Join(multIndexColumnTypeSlice, ",")), "CHAR")
		//	if tmpIntCount >= tmpCharCount {
		//		indexType["mui_multiseriate"] = multIndexColumnSlice
		//		break
		//	} else if tmpCharCount >= tmpIntCount {
		//		indexType["mui_multiseriate"] = multIndexColumnSlice
		//		break
		//	} else {
		//		indexType["mui_multiseriate"] = multIndexColumnSlice
		//		break
		//	}
		//}
		//}
	}
	//}
	return indexType
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
		if len(indexType["pri_multiseriate"]) > 1 && len(indexType["uni_single"]) == 1 {
			return "uni_single", indexType["uni_single"]
		}

		//假如没有单列主键索引，有多列主键索引，没有单列唯一索引，则选择多列主键索引
		if len(indexType["pri_multiseriate"]) > 1 && len(indexType["uni_single"]) < 1 {
			return "pri_multiseriate", indexType["pri_multiseriate"]
		}

		//假如没有单列主键索引，有多列主键索引，没有单列唯一索引，有多列唯一索引， 则选择多列主键索引
		if len(indexType["pri_multiseriate"]) > 1 && len(indexType["nui_multiseriate"]) > 1 {
			return "pri_multiseriate", indexType["pri_multiseriate"]
		}

		//假如没有主键索引，有多列唯一索引和单列唯一索引，则选择单列唯一索引
		if len(indexType["nui_multiseriate"]) > 1 && len(indexType["uni_single"]) > 0 {
			return "uni_single", indexType["uni_single"]
		}

		//假如没有主键索引，有多列唯一索引和普通索引，则选择多列唯一索引
		if len(indexType["nui_multiseriate"]) > 1 && len(indexType["uni_single"]) < 1 {
			return "nui_multiseriate", indexType["nui_multiseriate"]
		}

		//只有单列普通索引
		if len(indexType["mui_single"]) == 1 && len(indexType["mui_multiseriate"]) < 1 {
			return "mui_single", indexType["mui_single"]
		}

		//有无单列普通索引，和多列普通索引，选择多列普通索引
		if len(indexType["mui_single"]) <= 1 && len(indexType["mui_multiseriate"]) > 1 {
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
func (stcls *schemaTable) SchemaTableFilter() []string {
	//根据配置文件中的过滤条件筛选需要校验的表
	var dbNameList []string
	//处理待校验的库数量
	if stcls.schema != "" {
		dbNameList = stcls.schemaList(stcls.schema, stcls.ignoreSchema)
		//判断校验的库是否为空，为空则退出
		if len(dbNameList) == 0 {
			global.Wlog.Error("[check Schema] check Schema is emty, will exit!")
			os.Exit(1)
		}
	}
	//处理表校验
	tableList := stcls.tableList(dbNameList)
	//newTableList, _ := stcls.TableColumnNameCheck(tableList)
	if len(tableList) == 0 {
		global.Wlog.Error("[check Table] check table is emty, will exit!")
		os.Exit(1)
	}
	return tableList
}

/*
	库表的所有列信息
*/

func (stcls *schemaTable) SchemaTableAllCol(tableList []string) map[string]global.TableAllColumnInfoS {
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
	for _, i := range tableList {
		if strings.Contains(i, ".") {
			schema := strings.Split(i, ".")[0]
			table := strings.Split(i, ".")[1]
			tc := dbExec.TableColumnNameStruct{Schema: schema, Table: table, Drive: stcls.sourceDrive}
			a, err := tc.Query().TableAllColumn(stcls.sourceDB)
			if err != nil {
				fmt.Println(err)
			}
			tc.Drive = stcls.destDrive
			b, err := tc.Query().TableAllColumn(stcls.destDB)
			if err != nil {
				fmt.Println(err)
			}
			tableCol[fmt.Sprintf("%s_greatdbCheck_%s", schema, table)] = global.TableAllColumnInfoS{
				SColumnInfo: interfToString(a),
				DColumnInfo: interfToString(b),
			}
		}
	}
	return tableCol
}

/*
	获取校验表的索引列信息，包含是否有索引，列名，列序号
*/
func (stcls *schemaTable) TableIndexColumn(dtabS []string) map[string][]string {
	var tableIndexColumnMap = make(map[string][]string)
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		idxc := dbExec.IndexColumnStruct{Schema: stcls.schema, Table: stcls.table, Drivce: stcls.sourceDrive}
		queryData, _ := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB)
		indexType := stcls.indexColumnList(queryData)
		if len(indexType) == 0 { //针对于表没有索引的，进行处理
			key := fmt.Sprintf("%s/*greatdbSchemaTable*/%s", stcls.schema, stcls.table)
			tableIndexColumnMap[key] = []string{}
			global.Wlog.Warn("[check table index] table ", " not index to choose")
		} else {
			ab, aa := stcls.tableIndexAlgorithm(indexType)
			key := fmt.Sprintf("%s/*greatdbSchemaTable*/%s/*indexColumnType*/%s", stcls.schema, stcls.table, ab)
			global.Wlog.Info("[check table index] table ", key, " index info: ", indexType)
			tableIndexColumnMap[key] = aa
			global.Wlog.Info("[check table index] table ", key, " index choose info: ", aa)
		}
	}
	global.Wlog.Info("[check table index] check table index info ", tableIndexColumnMap)
	return tableIndexColumnMap
}

/*
	校验触发器
*/
func (stcls *schemaTable) Trigger(dtabS []string) {
	var createTrigger = func(data []map[string]interface{}) map[string]string {
		var tmpMap, createTriggerSql = make(map[string]string), make(map[string]string)
		for _, v := range data {
			if _, ok := v["TRIGGER_NAME"]; ok {
				tmpMap["TRIGGER_NAME"] = fmt.Sprintf("%s", v["TRIGGER_NAME"])
			}
			if _, ok := v["ACTION_TIMING"]; ok {
				tmpMap["ACTION_TIMING"] = fmt.Sprintf("%s", v["ACTION_TIMING"])
			}
			if _, ok := v["EVENT_MANIPULATION"]; ok {
				tmpMap["EVENT_MANIPULATION"] = fmt.Sprintf("%s", v["EVENT_MANIPULATION"])
			}
			if _, ok := v["EVENT_OBJECT_SCHEMA"]; ok {
				tmpMap["EVENT_OBJECT_SCHEMA"] = fmt.Sprintf("%s", v["EVENT_OBJECT_SCHEMA"])
			}
			if _, ok := v["EVENT_OBJECT_TABLE"]; ok {
				tmpMap["EVENT_OBJECT_TABLE"] = fmt.Sprintf("%s", v["EVENT_OBJECT_TABLE"])
			}
			if _, ok := v["ACTION_ORIENTATION"]; ok {
				tmpMap["ACTION_ORIENTATION"] = fmt.Sprintf("%s", v["ACTION_ORIENTATION"])
			}
			if _, ok := v["ACTION_STATEMENT"]; ok {
				tmpMap["ACTION_STATEMENT"] = fmt.Sprintf("%s", v["ACTION_STATEMENT"])
			}
			if _, ok := v["DEFINER"]; ok {
				tmpMap["DEFINER"] = fmt.Sprintf("%s", v["DEFINER"])
			}
		}
		createTriggerSql["triggerSql"] = fmt.Sprintf("DELIMITER $ \nCREATE TRIGGER %s.%s %s %s ON %s.%s FOR EACH %s %s $\nDELIMITER ;", tmpMap["EVENT_OBJECT_SCHEMA"], tmpMap["TRIGGER_NAME"], tmpMap["ACTION_TIMING"], tmpMap["EVENT_MANIPULATION"], tmpMap["EVENT_OBJECT_SCHEMA"], tmpMap["EVENT_OBJECT_TABLE"], tmpMap["ACTION_ORIENTATION"], strings.ReplaceAll(tmpMap["ACTION_STATEMENT"], "\n", ""))
		createTriggerSql["triggerName"] = tmpMap["TRIGGER_NAME"]
		createTriggerSql["definer"] = tmpMap["DEFINER"]
		return createTriggerSql
	}
	//var schemaMap = make(map[string]int)
	var pods = Pod{
		Datafix:     "no",
		CheckObject: "trigger",
	}
	for _, sa := range dtabS {
		schema := strings.Split(sa, ".")[0]
		table := strings.Split(sa, ".")[1]
		pods.Schema = schema
		pods.Table = table
		tc := dbExec.TableColumnNameStruct{Schema: schema, Table: table, Drive: stcls.sourceDrive}
		sourceTrigger, err := tc.Query().Trigger(stcls.sourceDB)
		if err != nil {
			fmt.Println(err)
		}
		tc.Drive = stcls.destDrive
		destTrigger, err := tc.Query().Trigger(stcls.destDB)
		if err != nil {
			fmt.Println(err)
		}
		sct := createTrigger(sourceTrigger)
		dct := createTrigger(destTrigger)
		if sct["definer"] == "" && dct["definer"] == "" {
			continue
		}
		if sct["triggerSql"] != dct["triggerSql"] {
			if sct["triggerName"] == "" {
				pods.Definer = dct["definer"]
				pods.TriggerName = dct["triggerName"]
			} else {
				pods.Definer = sct["definer"]
				pods.TriggerName = sct["triggerName"]
			}
			pods.Differences = "yes"
		} else {
			pods.Definer = sct["definer"]
			pods.TriggerName = sct["triggerName"]
			pods.Differences = "no"
		}
		measuredDataPods = append(measuredDataPods, pods)
	}
}

/*
	校验存储过程
*/
func (stcls *schemaTable) Proc(dtabS []string) {
	var schemaMap = make(map[string]int)
	var pods = Pod{
		Datafix:     "no",
		CheckObject: "proc",
	}
	for _, i := range dtabS {
		schemaMap[strings.Split(i, ".")[0]] = +schemaMap[strings.Split(i, ".")[0]]
	}
	for schema, _ := range schemaMap {
		tc := dbExec.TableColumnNameStruct{Schema: schema, Drive: stcls.sourceDrive}
		sourceProc, err := tc.Query().Proc(stcls.sourceDB)
		if err != nil {
			fmt.Println(err)
		}
		tc.Drive = stcls.destDrive
		destProc, err := tc.Query().Proc(stcls.destDB)
		if err != nil {
			fmt.Println(err)
		}
		if len(sourceProc) == 0 && len(destProc) == 0 {
			continue
		}
		var tmpM = make(map[string]string)
		for k, _ := range sourceProc {
			if k == "DEFINER" {
				continue
			}
			tmpM[k] = sourceProc["DEFINER"]
		}
		for k, _ := range destProc {
			if k == "DEFINER" {
				continue
			}
			tmpM[k] = sourceProc["DEFINER"]
		}
		pods.Schema = schema
		for k, v := range tmpM {
			if sourceProc[k] != destProc[k] {
				pods.Definer = v
				pods.ProcName = k
				pods.Differences = "yes"
			} else {
				pods.ProcName = k
				pods.Definer = v
				pods.Differences = "no"
			}
		}
		measuredDataPods = append(measuredDataPods, pods)
	}
}

/*
	校验函数
*/
func (stcls *schemaTable) Func(dtabS []string) {
	var schemaMap = make(map[string]int)
	var pods = Pod{
		Datafix:     "no",
		CheckObject: "func",
	}

	for _, i := range dtabS {
		schemaMap[strings.Split(i, ".")[0]] = +schemaMap[strings.Split(i, ".")[0]]
	}
	for schema, _ := range schemaMap {
		tc := dbExec.TableColumnNameStruct{Schema: schema, Drive: stcls.sourceDrive}
		sourceFunc, err := tc.Query().Func(stcls.sourceDB)
		if err != nil {
			fmt.Println(err)
		}
		tc.Drive = stcls.destDrive
		destFunc, err := tc.Query().Func(stcls.destDB)
		if err != nil {
			fmt.Println(err)
		}
		if len(sourceFunc) == 0 && len(destFunc) == 0 {
			continue
		}
		var tmpM = make(map[string]int)
		for k, _ := range sourceFunc {
			tmpM[k]++
		}
		for k, _ := range destFunc {
			tmpM[k]++
		}
		pods.Schema = schema
		for k, _ := range tmpM {
			var sv, dv, sd, dd string
			if sourceFunc[k] != "" {
				sd = strings.Split(sourceFunc[k], "/*proc*/")[0]
				sv = strings.Split(sourceFunc[k], "/*proc*/")[1]
			}
			if destFunc[k] != "" {
				dd = strings.Split(destFunc[k], "/*proc*/")[0]
				dv = strings.Split(destFunc[k], "/*proc*/")[1]
			}

			if sv != dv {
				pods.Definer = sd
				if sv == "" {
					pods.Definer = dd
				}
				pods.FuncName = k
				pods.Differences = "yes"
			} else {
				pods.Definer = sd
				if sv == "" {
					pods.Definer = dd
				}
				pods.FuncName = k
				pods.Differences = "no"
			}
		}
		measuredDataPods = append(measuredDataPods, pods)
	}
}

/*
	校验函数
*/

var indexDisposF = func(queryData []map[string]interface{}) ([]string, map[string][]string, map[string][]string) {
	nultiseriateIndexColumnMap := make(map[string][]string)
	multiseriateIndexColumnMap := make(map[string][]string)
	var PriIndexCol, uniIndexCol, mulIndexCol []string
	var indexName string
	for v := range queryData {
		if queryData[v]["nonUnique"].(string) == "0" {
			if queryData[v]["indexName"] == "PRIMARY" {
				if queryData[v]["indexName"].(string) != indexName {
					indexName = queryData[v]["indexName"].(string)
				}
				PriIndexCol = append(PriIndexCol, fmt.Sprintf("%s", queryData[v]["columnName"]))
			} else {
				if queryData[v]["indexName"].(string) != indexName {
					indexName = queryData[v]["indexName"].(string)
					nultiseriateIndexColumnMap[indexName] = append(uniIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", queryData[v]["columnName"], queryData[v]["columnType"]))
				} else {
					nultiseriateIndexColumnMap[indexName] = append(nultiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", queryData[v]["columnName"], queryData[v]["columnType"]))
				}
			}
		} else {
			if queryData[v]["indexName"].(string) != indexName {
				indexName = queryData[v]["indexName"].(string)
				multiseriateIndexColumnMap[indexName] = append(mulIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", queryData[v]["columnName"], queryData[v]["columnType"]))
			} else {
				multiseriateIndexColumnMap[indexName] = append(multiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", queryData[v]["columnName"], queryData[v]["columnType"]))
			}
		}
	}
	return PriIndexCol, nultiseriateIndexColumnMap, multiseriateIndexColumnMap
}

func (stcls *schemaTable) Foreign(dtabS []string) {
	var pods = Pod{
		Datafix:     "no",
		CheckObject: "Foreign",
	}
	//校验外键
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		pods.Schema = stcls.schema
		pods.Table = stcls.table
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		sourceForeign, err := tc.Query().Foreign(stcls.sourceDB)
		tc.Drive = stcls.destDrive
		destForeign, err := tc.Query().Func(stcls.destDB)
		if err != nil {
			fmt.Println(err)
		}
		if len(sourceForeign) == 0 && len(destForeign) == 0 {
			continue
		}
		var tmpM = make(map[string]int)
		for k, _ := range sourceForeign {
			tmpM[k]++
		}
		for k, _ := range destForeign {
			tmpM[k]++
		}
		for k, _ := range tmpM {
			if sourceForeign[k] != destForeign[k] {
				pods.Differences = "yes"
			} else {
				pods.Differences = "no"
			}
		}
		measuredDataPods = append(measuredDataPods, pods)
	}
}

//校验分区
func (stcls *schemaTable) Partitions(dtabS []string) {
	var pods = Pod{
		Datafix:     "no",
		CheckObject: "Partitions",
	}

	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		sourcePartitions, err := tc.Query().Partitions(stcls.sourceDB)
		tc.Drive = stcls.destDrive
		destPartitions, err := tc.Query().Partitions(stcls.destDB)
		if err != nil {
			fmt.Println(err)
		}
		pods.Schema = stcls.schema
		pods.Table = stcls.table
		if len(sourcePartitions) == 0 && len(destPartitions) == 0 {
			continue
		}
		var tmpM = make(map[string]int)
		for k, _ := range sourcePartitions {
			tmpM[k]++
		}
		for k, _ := range destPartitions {
			tmpM[k]++
		}
		for k, _ := range tmpM {
			if sourcePartitions[k] != destPartitions[k] {
				pods.Differences = "yes"
			} else {
				pods.Differences = "no"
			}
		}
		measuredDataPods = append(measuredDataPods, pods)
	}
}
func (stcls *schemaTable) Index(dtabS []string) {
	var (
		abNormalTableM, normalTableM = make(map[string][]string), make(map[string][]string)
		indexCompare                 = func(smul, dmul map[string][]string, st string, abNormalTableM, normalTableM map[string][]string) {
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
				} else {
					sv = []string{}
				}
				if _, ok := dmul[k]; ok {
					dv = dmul[k]
				} else {
					dv = []string{}
				}
				if strings.Join(sv, ",") != strings.Join(dv, ",") {
					abNormalTableS = append(abNormalTableS, k)
				} else {
					normalTableS = append(normalTableS, k)
				}
			}
			if len(abNormalTableS) > 0 {
				abNormalTableM[st] = abNormalTableS
			}
			if len(normalTableS) > 0 {
				normalTableM[st] = normalTableS
			}
		}
	)
	//校验索引
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		idxc := dbExec.IndexColumnStruct{Schema: stcls.schema, Table: stcls.table, Drivce: stcls.sourceDrive}
		squeryData, _ := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB)
		dqueryData, _ := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.destDB)
		spri, suni, smul := indexDisposF(squeryData)
		dpri, duni, dmul := indexDisposF(dqueryData)
		st := fmt.Sprintf("%s.%s", stcls.schema, stcls.table)
		//先比较主键索引
		if strings.Join(spri, ",") != strings.Join(dpri, ",") {
			abNormalTableM[st] = []string{"primary"}
		} else {
			normalTableM[st] = spri
		}
		//在比较唯一索引
		if len(duni) > 0 || len(suni) > 0 {
			indexCompare(suni, duni, st, abNormalTableM, normalTableM)
		}
		//比较普通索引
		if len(smul) > 0 || len(dmul) > 0 {
			indexCompare(smul, dmul, st, abNormalTableM, normalTableM)
		}
	}
	var pods = Pod{
		Datafix:     "no",
		CheckObject: "Index",
	}
	for k, v := range normalTableM {
		aa := strings.Split(k, ".")
		pods.Schema = aa[0]
		pods.Table = aa[1]
		pods.IndexCol = strings.Join(v, ",")
		if _, ok := abNormalTableM[k]; !ok {
			pods.Differences = "no"
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	for k, v := range abNormalTableM {
		aa := strings.Split(k, ".")
		pods.Schema = aa[0]
		pods.Table = aa[1]
		pods.IndexCol = strings.Join(v, ",")
		pods.Differences = "yes"
		measuredDataPods = append(measuredDataPods, pods)
	}
}
func (stcls *schemaTable) Struct(dtabS []string) {
	//校验列名
	normal, abnormal := stcls.TableColumnNameCheck(dtabS)
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
	}
}
