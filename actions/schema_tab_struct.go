package actions

import (
	"database/sql"
	"errors"
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/inputArg"
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
	sfile               *os.File
	djdbc               string
}

/*
   查询待校验表的列名
*/
func (stcls *schemaTable) tableColumnName(db *sql.DB, tc dbExec.TableColumnNameStruct, logThreadSeq, logThreadSeq2 int64) (string, error) {
	var (
		col       string
		vlog      string
		queryData []map[string]interface{}
		err       error
		Event     = "Q_table_columns"
	)
	if queryData, err = tc.Query().TableColumnName(db, logThreadSeq2); err != nil {
		return "", err
	}
	vlog = fmt.Sprintf("(%d) [%s] start dispos DB query columns data. to dispos it...", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	for _, v := range queryData {
		if v["columnName"].(string) != "" {
			col += fmt.Sprintf("%s,", v["columnName"].(string))
		}
	}
	vlog = fmt.Sprintf("(%d) [%s] complete dispos DB query columns data.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	return col, nil
}

/*
	针对表的列名进行校验
*/
func (stcls *schemaTable) TableColumnNameCheck(checkTableList []string, logThreadSeq, logThreadSeq2 int64) ([]string, []string) {
	var (
		vlog                                 string
		newCheckTableList, abnormalTableList []string
		err                                  error
	)
	vlog = fmt.Sprintf("(%d) Start to check the consistency information of source and target table structure and column information ...", logThreadSeq)
	global.Wlog.Info(vlog)

	for _, v := range checkTableList {
		var sColumn, dColumn string
		stcls.schema = strings.Split(v, ".")[0]
		stcls.table = strings.Split(v, ".")[1]
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		sColumn, err = stcls.tableColumnName(stcls.sourceDB, tc, logThreadSeq, logThreadSeq2)
		if err != nil {
			return nil, nil
		}
		tc.Drive = stcls.destDrive
		dColumn, err = stcls.tableColumnName(stcls.destDB, tc, logThreadSeq, logThreadSeq2)
		if err != nil {
			return nil, nil
		}
		if stcls.lowerCaseTableNames == "no" {
			sColumn = strings.ToUpper(sColumn)
			dColumn = strings.ToUpper(dColumn)
		}
		vlog = fmt.Sprintf("(%d) source DB table name [%s.%s] column name message is {%s} num [%d]", logThreadSeq, stcls.schema, stcls.table, sColumn, len(strings.Split(sColumn, ","))-1)
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) dest DB table name [%s.%s] column name message is {%s} num [%d]", logThreadSeq, stcls.schema, stcls.table, dColumn, len(strings.Split(dColumn, ","))-1)
		global.Wlog.Debug(vlog)

		//防止异构数据库中列明大小不一致
		vlog = fmt.Sprintf("(%d) start diff source dest db table columns name, to check it...", logThreadSeq)
		global.Wlog.Debug(vlog)
		if CheckSum().CheckSha1(sColumn) == CheckSum().CheckSha1(dColumn) {
			newCheckTableList = append(newCheckTableList, v)
		} else {
			abnormalTableList = append(abnormalTableList, v)
		}
		vlog = fmt.Sprintf("(%d) complete checksum source dest db table columns name.", logThreadSeq)
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) The consistency information check of the source and target table structure and column information is completed", logThreadSeq)
	global.Wlog.Info(vlog)
	return newCheckTableList, abnormalTableList
}

/*
	检查当前用户对该库表是否有响应的权限（权限包括：查询权限，flush_tables,session_variables_admin）
*/
func (stcls *schemaTable) GlobalAccessPriCheck(logThreadSeq, logThreadSeq2 int64) bool {
	var (
		vlog                   string
		err                    error
		StableList, DtableList bool
	)
	vlog = fmt.Sprintf("(%d) Start to get the source and target Global Access Permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Info(vlog)
	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive, Datafix: stcls.datefix}
	vlog = fmt.Sprintf("(%d) Start to get the source Global Access Permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Debug(vlog)
	if StableList, err = tc.Query().GlobalAccessPri(stcls.sourceDB, logThreadSeq2); err != nil {
		return false
	}
	vlog = fmt.Sprintf("(%d) The Global Access Permission verification of the source DB is completed, and the status of the global access permission is {%v}.", logThreadSeq, StableList)
	global.Wlog.Debug(vlog)
	tc.Drive = stcls.destDrive
	vlog = fmt.Sprintf("(%d) Start to get the dest Global Access Permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Debug(vlog)

	if DtableList, err = tc.Query().GlobalAccessPri(stcls.destDB, logThreadSeq2); err != nil {
		return false
	}

	vlog = fmt.Sprintf("(%d) The Global Access Permission verification of the dest DB is completed, and the status of the global access permission is {%v}.", logThreadSeq, DtableList)
	global.Wlog.Debug(vlog)
	if StableList && DtableList {
		vlog = fmt.Sprintf("(%d) The verification of the global access permission of the source and destination is completed", logThreadSeq)
		global.Wlog.Info(vlog)
		return true
	}
	vlog = fmt.Sprintf("(%d) Some global access permissions are missing at the source and destination, and verification cannot continue.", logThreadSeq)
	global.Wlog.Error(vlog)
	return false
}
func (stcls *schemaTable) TableAccessPriCheck(checkTableList []string, logThreadSeq, logThreadSeq2 int64) ([]string, []string) {
	var (
		vlog                                 string
		err                                  error
		StableList, DtableList               map[string]int
		newCheckTableList, abnormalTableList []string
	)
	vlog = fmt.Sprintf("(%d) Start to get the source and target table access permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Info(vlog)
	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
	vlog = fmt.Sprintf("(%d) Start to get the source table access permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Debug(vlog)

	if StableList, err = tc.Query().TableAccessPriCheck(stcls.sourceDB, checkTableList, stcls.datefix, logThreadSeq2); err != nil {
		return nil, nil
	}
	if len(StableList) == 0 {
		vlog = fmt.Sprintf("(%d) Complete the verification table permission verification of the source DB, the current verification table with permission is {%v}.", logThreadSeq, StableList)
		global.Wlog.Error(vlog)
	} else {
		vlog = fmt.Sprintf("(%d) Complete the verification table permission verification of the source DB, the current verification table with permission is {%v}.", logThreadSeq, StableList)
		global.Wlog.Debug(vlog)
	}

	tc.Drive = stcls.destDrive
	vlog = fmt.Sprintf("(%d) Start to get the dest table access permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Debug(vlog)
	if DtableList, err = tc.Query().TableAccessPriCheck(stcls.destDB, checkTableList, stcls.datefix, logThreadSeq2); err != nil {
		return nil, nil
	}
	if len(DtableList) == 0 {
		vlog = fmt.Sprintf("(%d) Complete the verification table permission verification of the source DB, the current verification table with permission is {%v}.", logThreadSeq, DtableList)
		global.Wlog.Error(vlog)
	} else {
		vlog = fmt.Sprintf("(%d) Complete the verification table permission verification of the source DB, the current verification table with permission is {%v}.", logThreadSeq, DtableList)
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) Start processing the difference of the table to be checked at the source and target.", logThreadSeq)
	global.Wlog.Debug(vlog)
	for k, _ := range StableList {
		if _, ok := DtableList[k]; ok {
			newCheckTableList = append(newCheckTableList, k)
		} else {
			abnormalTableList = append(abnormalTableList, k)
		}
	}
	vlog = fmt.Sprintf("(%d) The difference processing of the table to be checked at the source and target ends is completed. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, newCheckTableList, len(newCheckTableList), abnormalTableList, len(abnormalTableList))
	global.Wlog.Info(vlog)
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

func (stcls *schemaTable) FuzzyMatchingDispos(dbCheckNameList map[string]int, Ftable string, logThreadSeq1 int64) map[string]int {
	var a, b, f = make(map[string]int), make(map[string]int), make(map[string]int)
	for k, _ := range dbCheckNameList {
		a[strings.Split(k, "/*schema&table*/")[0]]++
	}
	//处理*.*
	if Ftable == "*.*" {
		for k, _ := range dbCheckNameList {
			d := strings.Split(k, "/*schema&table*/")
			f[fmt.Sprintf("%s.%s", d[0], d[1])]++
		}
		return f
	}
	//处理库的模糊查询
	for _, i := range strings.Split(Ftable, ",") {
		if !strings.Contains(i, ".") {
			continue
		}
		schema := i[:strings.Index(i, ".")]
		if schema == "*" { //处理*.table
			b = dbCheckNameList
		} else if strings.HasPrefix(schema, "%") && !strings.HasSuffix(schema, "%") { //处理%schema.xxx
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for k, _ := range a {
				//获取该库对应下的表信息，以切片的方式
				if strings.HasSuffix(k, tmpschema) {
					for ki, _ := range dbCheckNameList {
						d := strings.Split(ki, "/*schema&table*/")
						if strings.EqualFold(d[0], k) {
							b[fmt.Sprintf("%s/*schema&table*/%s", k, d[1])]++
						}
					}
				}
			}
		} else if strings.HasSuffix(schema, "%") && !strings.HasPrefix(schema, "%") { //处理schema%.xxx
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for k, _ := range a {
				if strings.HasPrefix(k, tmpschema) {
					for ki, _ := range dbCheckNameList {
						d := strings.Split(ki, "/*schema&table*/")
						if strings.EqualFold(d[0], k) {
							b[fmt.Sprintf("%s/*schema&table*/%s", k, d[1])]++
						}
					}
				}
			}
		} else if strings.HasPrefix(schema, "%") && strings.HasSuffix(schema, "%") { //处理%schema%.xxx
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for k, _ := range a {
				if strings.Contains(k, tmpschema) {
					for ki, _ := range dbCheckNameList {
						d := strings.Split(ki, "/*schema&table*/")
						if strings.EqualFold(d[0], k) {
							b[fmt.Sprintf("%s/*schema&table*/%s", k, d[1])]++
						}
					}
				}
			}
		} else { //处理schema.xxx
			if _, ok := a[schema]; ok {
				for ki, _ := range dbCheckNameList {
					d := strings.Split(ki, "/*schema&table*/")
					if strings.EqualFold(d[0], schema) {
						b[fmt.Sprintf("%s/*schema&table*/%s", schema, d[1])]++
					}
				}
			}
		}
	}
	//处理表的模糊查询
	for _, i := range strings.Split(Ftable, ",") {
		if !strings.Contains(i, ".") {
			continue
		}
		schema := strings.ReplaceAll(i[:strings.Index(i, ".")], "%", "")
		table := i[strings.Index(i, ".")+1:]
		for k, _ := range b {
			g := strings.Split(k, "/*schema&table*/")
			if strings.Contains(g[0], schema) || schema == "*" {
				if table == "*" { //处理schema.*
					f[fmt.Sprintf("%s.%s", g[0], g[1])]++
				} else if strings.HasPrefix(table, "%") && !strings.HasSuffix(table, "%") { //处理schema.%table
					tmptable := strings.ReplaceAll(table, "%", "")
					if strings.HasSuffix(g[1], tmptable) {
						f[fmt.Sprintf("%s.%s", g[0], g[1])]++
					}
				} else if strings.HasSuffix(table, "%") && !strings.HasPrefix(table, "%") { //处理schema.table%
					tmptable := strings.ReplaceAll(table, "%", "")
					if strings.HasPrefix(g[1], tmptable) {
						f[fmt.Sprintf("%s.%s", g[0], g[1])]++
					}
				} else if strings.HasPrefix(table, "%") && strings.HasSuffix(table, "%") { //处理schema.%table%
					tmptable := strings.ReplaceAll(table, "%", "")
					if strings.Contains(g[1], tmptable) {
						f[fmt.Sprintf("%s.%s", g[0], g[1])]++
					}
				} else { //处理schema.table
					if strings.EqualFold(g[1], table) {
						f[fmt.Sprintf("%s.%s", g[0], g[1])]++
					}
				}
			}
		}
	}
	return f
}

/*
	处理需要校验的库表
	将忽略的库表从校验列表中去除，如果校验列表为空则退出
*/
func (stcls *schemaTable) SchemaTableFilter(logThreadSeq1, logThreadSeq2 int64) []string {
	var (
		vlog            string
		f               []string
		dbCheckNameList map[string]int
		err             error
	)
	vlog = fmt.Sprintf("(%d) Start to init schema.table info.", logThreadSeq1)
	global.Wlog.Info(vlog)
	//处理待校验的库数量
	if stcls.table == "" {
		return f
	}
	//获取当前数据库信息列表
	tc := dbExec.TableColumnNameStruct{Table: stcls.table, Drive: stcls.sourceDrive, Db: stcls.sourceDB, IgnoreTable: stcls.ignoreTable, LowerCaseTableNames: stcls.lowerCaseTableNames}
	vlog = fmt.Sprintf("(%d) query check database list info.", logThreadSeq1)
	global.Wlog.Debug(vlog)
	dbCheckNameList, err = tc.Query().DatabaseNameList(stcls.sourceDB, logThreadSeq2)
	if err != nil {
		return nil
	}
	vlog = fmt.Sprintf("(%d) checksum database list message is {%s}", logThreadSeq1, dbCheckNameList)
	global.Wlog.Debug(vlog)
	//判断校验的库是否为空，为空则退出
	if len(dbCheckNameList) == 0 {
		fmt.Println("gt-checksum report: check Schema.table is empty, please check the log for details!")
		vlog = fmt.Sprintf("(%d) check Schema.table is empty, exit", logThreadSeq1)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	schema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.table, logThreadSeq1)
	ignoreSchema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.ignoreTable, logThreadSeq1)
	for k, _ := range ignoreSchema {
		if _, ok := schema[k]; ok {
			delete(schema, k)
		}
	}
	for k, _ := range schema {
		f = append(f, k)
	}
	if len(f) == 0 {
		fmt.Println("gt-checksum report: check table is empty,please check the log for details!")
		vlog = fmt.Sprintf("(%d) check table is empty, exit", logThreadSeq1)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	vlog = fmt.Sprintf("(%d) schema.table {%s} init sccessfully, num [%d].", logThreadSeq1, f, len(f))
	global.Wlog.Info(vlog)
	return f
}

/*
	库表的所有列信息
*/
func (stcls *schemaTable) SchemaTableAllCol(tableList []string, logThreadSeq, logThreadSeq2 int64) map[string]global.TableAllColumnInfoS {
	var (
		a, b           []map[string]interface{}
		err            error
		vlog           string
		tableCol       = make(map[string]global.TableAllColumnInfoS)
		interfToString = func(colData []map[string]interface{}) []map[string]string {
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
	)
	vlog = fmt.Sprintf("(%d) Start to obtain the metadata information of the source-target verification table ...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range tableList {
		if strings.Contains(i, ".") {
			schema := strings.Split(i, ".")[0]
			table := strings.Split(i, ".")[1]
			vlog = fmt.Sprintf("(%d) Start to query all column information of source DB %s table %s.%s", logThreadSeq, stcls.sourceDrive, schema, table)
			global.Wlog.Debug(vlog)
			tc := dbExec.TableColumnNameStruct{Schema: schema, Table: table, Drive: stcls.sourceDrive}
			a, err = tc.Query().TableAllColumn(stcls.sourceDB, logThreadSeq2)
			if err != nil {
				return nil
			}
			vlog = fmt.Sprintf("(%d) All column information query of source DB %s table %s.%s is completed", logThreadSeq, stcls.sourceDrive, schema, table)
			global.Wlog.Debug(vlog)

			vlog = fmt.Sprintf("(%d) Start to query all column information of dest DB %s table %s.%s", logThreadSeq, stcls.destDrive, schema, table)
			global.Wlog.Debug(vlog)
			tc.Drive = stcls.destDrive
			b, err = tc.Query().TableAllColumn(stcls.destDB, logThreadSeq2)
			if err != nil {
				return nil
			}
			vlog = fmt.Sprintf("(%d) All column information query of dest DB %s table %s.%s is completed", logThreadSeq, stcls.destDrive, schema, table)
			global.Wlog.Debug(vlog)
			tableCol[fmt.Sprintf("%s_greatdbCheck_%s", schema, table)] = global.TableAllColumnInfoS{
				SColumnInfo: interfToString(a),
				DColumnInfo: interfToString(b),
			}
			vlog = fmt.Sprintf("(%d) all column information query of table %s.%s is completed. table column message is {source: %s, dest: %s}", logThreadSeq, schema, table, interfToString(a), interfToString(b))
			global.Wlog.Debug(vlog)
		}
	}
	vlog = fmt.Sprintf("(%d) The metadata information of the source target verification table has been obtained", logThreadSeq)
	global.Wlog.Info(vlog)
	return tableCol
}

/*
	获取校验表的索引列信息，包含是否有索引，列名，列序号
*/
func (stcls *schemaTable) TableIndexColumn(dtabS []string, logThreadSeq, logThreadSeq2 int64) map[string][]string {
	var (
		queryData           []map[string]interface{}
		err                 error
		vlog                string
		tableIndexColumnMap = make(map[string][]string)
	)
	vlog = fmt.Sprintf("(%d) Start to query the table index listing information and select the appropriate index ...", logThreadSeq)
	global.Wlog.Info(vlog)

	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		vlog = fmt.Sprintf("(%d) Start querying the index list information of table %s.%s.", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		idxc := dbExec.IndexColumnStruct{Schema: stcls.schema, Table: stcls.table, Drivce: stcls.sourceDrive}
		queryData, err = idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
		if err != nil {
			return nil
		}
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive, Db: stcls.sourceDB}
		indexType := tc.Query().TableIndexChoice(queryData, logThreadSeq2)
		vlog = fmt.Sprintf("(%d) Table %s.%s index list information query completed. index list message is {%v}", logThreadSeq, stcls.schema, stcls.table, indexType)
		global.Wlog.Debug(vlog)
		if len(indexType) == 0 { //针对于表没有索引的，进行处理
			key := fmt.Sprintf("%s/*greatdbSchemaTable*/%s", stcls.schema, stcls.table)
			tableIndexColumnMap[key] = []string{}
			vlog = fmt.Sprintf("(%d) The current table %s.%s has no index.", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Warn(vlog)
		} else {
			vlog = fmt.Sprintf("(%d) Start to perform index selection on table %s.%s according to the algorithm", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
			ab, aa := stcls.tableIndexAlgorithm(indexType)
			key := fmt.Sprintf("%s/*greatdbSchemaTable*/%s/*indexColumnType*/%s", stcls.schema, stcls.table, ab)
			tableIndexColumnMap[key] = aa
			vlog = fmt.Sprintf("(%d) The index selection of table %s.%s is completed, and the selected index information is { keyName:%s keyColumn: %s}", logThreadSeq, stcls.schema, stcls.table, ab, aa)
			global.Wlog.Debug(vlog)
		}
	}
	zlog := fmt.Sprintf("(%d) Table index listing information and appropriate index completion", logThreadSeq)
	global.Wlog.Info(zlog)
	return tableIndexColumnMap
}

/*
	校验触发器
*/
func (stcls *schemaTable) Trigger(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog string
		tmpM = make(map[string]int)
		z    = make(map[string]int)
		c, d []string
		pods = Pod{
			Datafix:     "no",
			CheckObject: "trigger",
		}
		sourceTrigger, destTrigger map[string]string
		err                        error
	)

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Trigger. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
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
		vlog = fmt.Sprintf("(%d) Start processing source DB %s data databases %s Trigger. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: i, Drive: stcls.sourceDrive}
		if sourceTrigger, err = tc.Query().Trigger(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Source DB %s data databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, sourceTrigger)
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) Start processing dest DB %s data databases %s Trigger data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive
		if destTrigger, err = tc.Query().Trigger(stcls.destDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Dest DB %s data databases %s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, destTrigger)
		global.Wlog.Debug(vlog)
		if len(sourceTrigger) == 0 && len(destTrigger) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			continue
		}
		tmpM = nil
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Trigger. to dispos it...", logThreadSeq, stcls.schema)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceTrigger {
			tmpM[k]++
		}
		for k, _ := range destTrigger {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Trigger is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		for k, _ := range tmpM {
			pods.TriggerName = strings.ReplaceAll(strings.Split(k, ".")[1], "\"", "")
			if sourceTrigger[k] != destTrigger[k] {
				pods.Differences = "yes"
				d = append(d, k)
			} else {
				pods.Differences = "no"
				c = append(c, k)
			}
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Trigger. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, stcls.schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			vlog = fmt.Sprintf("(%d) The source target segment databases %s Trigger data verification is completed", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Trigger data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

/*
	校验存储过程
*/
func (stcls *schemaTable) Proc(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog      string
		c, d      []string
		schemaMap = make(map[string]int)
		pods      = Pod{
			Datafix:     "no",
			CheckObject: "proc",
		}
		sourceProc, destProc map[string]string
		err                  error
		tmpM                 = make(map[string]int)
	)
	vlog = fmt.Sprintf("(%d) Start init check source and target DB Stored Procedure. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		schemaMap[strings.Split(i, ".")[0]] = +schemaMap[strings.Split(i, ".")[0]]
	}

	for schema, _ := range schemaMap {
		vlog = fmt.Sprintf("(%d) Start processing source DB %s data databases %s Stored Procedure. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: schema, Drive: stcls.sourceDrive}
		if sourceProc, err = tc.Query().Proc(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Source DB %s data databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, sourceProc)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive
		vlog = fmt.Sprintf("(%d) Start processing dest DB %s data table %s Stored Procedure data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		if destProc, err = tc.Query().Proc(stcls.destDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Dest DB %s data databases %s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, destProc)
		global.Wlog.Debug(vlog)
		if len(sourceProc) == 0 && len(destProc) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, stcls.schema)
			global.Wlog.Warn(vlog)
			continue
		}

		tmpM = nil
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Stored Procedure. to dispos it...", logThreadSeq, stcls.schema)
		global.Wlog.Debug(vlog)
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
		vlog = fmt.Sprintf("(%d) Start to compare whether the Stored Procedure is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
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
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Stored Procedure. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, stcls.schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			vlog = fmt.Sprintf("(%d) The source target segment databases %s Stored Procedure data verification is completed", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Stored Procedure data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

/*
	校验函数
*/
func (stcls *schemaTable) Func(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog                 string
		sourceFunc, destFunc map[string]string
		tmpM                 = make(map[string]int)
		schemaMap            = make(map[string]int)
		pods                 = Pod{
			Datafix:     "no",
			CheckObject: "func",
		}
		err  error
		c, d []string
	)

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Stored Function. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		schemaMap[strings.Split(i, ".")[0]] = +schemaMap[strings.Split(i, ".")[0]]
	}

	for schema, _ := range schemaMap {
		vlog = fmt.Sprintf("(%d) Start processing source DB %s data databases %s Stored Function. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: schema, Drive: stcls.sourceDrive}
		if sourceFunc, err = tc.Query().Func(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Source DB %s data databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, sourceFunc)
		global.Wlog.Debug(vlog)

		tc.Drive = stcls.destDrive
		vlog = fmt.Sprintf("(%d) Start processing dest DB %s data table %s Stored Function data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		if destFunc, err = tc.Query().Func(stcls.destDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Dest DB %s data databases %s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, destFunc)
		global.Wlog.Debug(vlog)

		if len(sourceFunc) == 0 && len(destFunc) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			continue
		}

		tmpM = nil
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Stored Function. to dispos it...", logThreadSeq, stcls.schema)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceFunc {
			tmpM[k]++
		}
		for k, _ := range destFunc {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Stored Function is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
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
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Stored Function. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, stcls.schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			vlog = fmt.Sprintf("(%d) The source target segment databases %s Stored Function data verification is completed", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Stored Function data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
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
	var (
		vlog                       string
		sourceForeign, destForeign map[string]string
		tmpM                       = make(map[string]int)
		err                        error
		pods                       = Pod{
			Datafix:     "no",
			CheckObject: "Foreign",
		}
	)

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Foreign. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	//校验外键
	var c, d []string
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		vlog = fmt.Sprintf("(%d) Start processing source DB %s data table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		pods.Schema = stcls.schema
		pods.Table = stcls.table
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		if sourceForeign, err = tc.Query().Foreign(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Source DB %s data table %s.%s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table, sourceForeign)
		global.Wlog.Debug(vlog)

		vlog = fmt.Sprintf("(%d) Start processing dest DB %s data table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive
		if destForeign, err = tc.Query().Foreign(stcls.destDB, logThreadSeq2); err != nil {
			return
		}

		vlog = fmt.Sprintf("(%d) Dest DB %s data table %s.%s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table, destForeign)
		global.Wlog.Debug(vlog)
		if len(sourceForeign) == 0 && len(destForeign) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
			continue
		}
		tmpM = nil
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target table %s.%s Foreign Name. to dispos it...", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceForeign {
			tmpM[k]++
		}
		for k, _ := range destForeign {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Foreign table is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		for k, _ := range tmpM {
			if sourceForeign[k] != destForeign[k] {
				pods.Differences = "yes"
				d = append(d, k)
			} else {
				pods.Differences = "no"
				c = append(c, k)
			}
		}
		vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s Foreign. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, stcls.schema, stcls.table, c, len(c), d, len(d))
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) The source target segment table %s.%s Foreign data verification is completed", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		measuredDataPods = append(measuredDataPods, pods)
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Foreign data. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

//校验分区
func (stcls *schemaTable) Partitions(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog                             string
		c, d                             []string
		sourcePartitions, destPartitions map[string]string
		pods                             = Pod{
			Datafix:     "no",
			CheckObject: "Partitions",
		}
		tmpM = make(map[string]int)
	)
	vlog = fmt.Sprintf("(%d) Start init check source and target DB partition table. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		vlog = fmt.Sprintf("(%d) Start processing source DB %s data table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		if sourcePartitions, err = tc.Query().Partitions(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}

		vlog = fmt.Sprintf("(%d) Source DB %s data table %s.%s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table, sourcePartitions)
		global.Wlog.Debug(vlog)

		tc.Drive = stcls.destDrive
		vlog = fmt.Sprintf("(%d) Start processing dest DB %s data table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		if destPartitions, err = tc.Query().Partitions(stcls.destDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Dest DB %s data table %s.%s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table, destPartitions)
		global.Wlog.Debug(vlog)

		pods.Schema = stcls.schema
		pods.Table = stcls.table
		if len(sourcePartitions) == 0 && len(destPartitions) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
			continue
		}

		tmpM = nil
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target table %s.%s Partitions Column. to dispos it...", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		for k, _ := range sourcePartitions {
			tmpM[k]++
		}
		for k, _ := range destPartitions {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the partitions table is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		for k, _ := range tmpM {
			if strings.Join(strings.Fields(sourcePartitions[k]), "") != strings.Join(strings.Fields(destPartitions[k]), "") {
				pods.Differences = "yes"
				d = append(d, k)
			} else {
				c = append(c, k)
				pods.Differences = "no"
			}
		}
		vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s partitions. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, stcls.schema, stcls.table, c, len(c), d, len(d))
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) The source target segment table %s.%s partitions data verification is completed", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		measuredDataPods = append(measuredDataPods, pods)
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table partitions data. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

func (stcls *schemaTable) Index(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog string
		sqlS []string
		aa   = &CheckSumTypeStruct{}
		//sqlM          = make(map[int]string)
		indexGenerate = func(smu, dmu map[string][]string, a *CheckSumTypeStruct, indexType string) []string {
			var cc, c, d []string
			for k, _ := range smu {
				c = append(c, k)
			}
			for k, _ := range dmu {
				d = append(d, k)
			}
			if a.CheckMd5(strings.Join(c, ",")) != a.CheckMd5(strings.Join(d, ",")) {
				e, f := a.Arrcmp(c, d)
				dbf := dbExec.DataAbnormalFixStruct{Schema: stcls.schema, Table: stcls.table, SourceDevice: stcls.destDrive, IndexType: indexType, DatafixType: stcls.datefix}
				cc, _ = dbf.DataAbnormalFix().FixAlterSqlExec(e, f, smu, "MySQL", logThreadSeq)
			}
			return cc
		}
	)
	//校验索引
	vlog = fmt.Sprintf("(%d) start init check source and target DB index Column. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		idxc := dbExec.IndexColumnStruct{Schema: stcls.schema, Table: stcls.table, Drivce: stcls.sourceDrive}
		vlog = fmt.Sprintf("(%d) Start processing source DB %s data table %s.%s index column data. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		squeryData, _ := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
		spri, suni, smul := idxc.TableIndexColumn().IndexDisposF(squeryData, logThreadSeq2)

		idxc.Drivce = stcls.destDrive
		vlog = fmt.Sprintf("(%d) Start processing dest DB %s data table %s.%s index column data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		dqueryData, _ := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.destDB, logThreadSeq2)
		dpri, duni, dmul := idxc.TableIndexColumn().IndexDisposF(dqueryData, logThreadSeq2)

		var pods = Pod{
			Datafix:     stcls.datefix,
			CheckObject: "Index",
			Differences: "no",
			Schema:      stcls.schema,
			Table:       stcls.table,
		}
		//先比较主键索引
		vlog = fmt.Sprintf("(%d) Start to compare whether the primary key index is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(spri, dpri, aa, "pri")...)
		vlog = fmt.Sprintf("(%d) Compare whether the primary key index is consistent and verified.", logThreadSeq)
		global.Wlog.Debug(vlog)
		//再比较唯一索引
		vlog = fmt.Sprintf("(%d) Start to compare whether the unique key index is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(suni, duni, aa, "uni")...)
		vlog = fmt.Sprintf("(%d) Compare whether the unique key index is consistent and verified.", logThreadSeq)
		global.Wlog.Info(vlog)
		//后比较普通索引
		vlog = fmt.Sprintf("(%d) Start to compare whether the no-unique key index is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(smul, dmul, aa, "mul")...)
		vlog = fmt.Sprintf("(%d) Compare whether the no-unique key index is consistent and verified.", logThreadSeq)
		global.Wlog.Debug(vlog)
		if len(sqlS) > 0 {
			pods.Differences = "yes"
		}
		//for k, v := range sqlS {
		//	sqlM[k] = v
		//}
		ApplyDataFix(sqlS, stcls.datefix, stcls.sfile, stcls.destDrive, stcls.djdbc, logThreadSeq)
		measuredDataPods = append(measuredDataPods, pods)
		vlog = fmt.Sprintf("(%d) The source target segment table %s.%s index column data verification is completed", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Info(vlog)
	}
}

/*
	校验表结构是否正确
*/
func (stcls *schemaTable) Struct(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	//校验列名
	var vlog string
	vlog = fmt.Sprintf("(%d) begin check source and target struct. check object is {%s} num[%d]", logThreadSeq, dtabS, len(dtabS))
	global.Wlog.Info(vlog)
	normal, abnormal := stcls.TableColumnNameCheck(dtabS, logThreadSeq, logThreadSeq2)
	vlog = fmt.Sprintf("(%d) Complete the data consistency check of the source target segment table structure column. normal table message is {%s} num [%d], abnormal table message is {%s} num [%d].", logThreadSeq, normal, len(normal), abnormal, len(abnormal))
	global.Wlog.Debug(vlog)
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
	clog := fmt.Sprintf("(%d) check source and target DB table struct complete", logThreadSeq)
	global.Wlog.Info(clog)
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
		ignoreTable:         m.Igtable,
		table:               m.Table,
		sourceDrive:         m.SourceDrive,
		destDrive:           m.DestDrive,
		sourceDB:            sdb,
		destDB:              ddb,
		lowerCaseTableNames: m.LowerCaseTableNames,
		datefix:             m.Datafix,
		sfile:               m.Sfile,
		djdbc:               m.DestJdbc,
	}
}
