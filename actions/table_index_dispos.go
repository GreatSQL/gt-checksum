package actions

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	chanString      chan string
	chanMap         chan map[string]string
	chanBool        chan bool
	chanDiffDataS   chan DifferencesDataStruct
	chanSliceString chan []string
	chanStruct      chan struct{}
)

var (
	lock sync.Mutex
)

/*
初始化差异数据信息结构体
*/
func InitDifferencesDataStruct() DifferencesDataStruct {
	return DifferencesDataStruct{}
}

/*
递归查询索引列数据，并按照单次校验块的大小来切割索引列数据，生成查询的where条件
*/
func (sp *SchedulePlan) recursiveIndexColumn(sqlWhere chanString, sdb, ddb *sql.DB, level, queryNum int, where string, selectColumn map[string]map[string]string, logThreadSeq int64) {
	var (
		sqlwhere       string //查询sql的where条件
		d, c           int    //索引列每一行group重复值的累加值，临时变量
		e, g           string //定义每个chunk的初始值和结尾值,e为起始值，g为数据查询的动态指针值
		vlog           string //日志输出变量
		autoIncSeq     uint64
		partFirstValue = true
		curryCount     int64
	)
	//获取索引列的数据类型
	a := sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.schema, sp.table)].SColumnInfo
	//查询源目标端索引列数据
	idxc := dbExec.IndexColumnStruct{Schema: sp.sourceSchema, Table: sp.table, ColumnName: sp.columnName,
		ChanrowCount: sp.chanrowCount, Drivce: sp.sdrive, SelectColumn: selectColumn[sp.sdrive], ColData: a}
	vlog = fmt.Sprintf("(%d) Querying source table %s.%s index column %s", logThreadSeq, sp.sourceSchema, sp.table, sp.columnName[level])
	global.Wlog.Debug(vlog)
	SdataChan1, err := idxc.TableIndexColumn().TmpTableColumnGroupDataDispos(sdb, where, sp.columnName[level], logThreadSeq)
	if err != nil {
		return
	}
	idxcDest := dbExec.IndexColumnStruct{Schema: sp.destSchema, Table: sp.table, ColumnName: sp.columnName,
		ChanrowCount: sp.chanrowCount, Drivce: sp.ddrive, SelectColumn: selectColumn[sp.ddrive], ColData: a}
	vlog = fmt.Sprintf("(%d) Querying target table %s.%s index column %s", logThreadSeq, sp.destSchema, sp.table, sp.columnName[level])
	global.Wlog.Debug(vlog)
	DdataChan1, err := idxcDest.TableIndexColumn().TmpTableColumnGroupDataDispos(ddb, where, sp.columnName[level], logThreadSeq)
	if err != nil {
		return
	}
	cMerge := dataDispos.DataInfo{ChanQueueDepth: sp.mqQueueDepth}
	ascUniqSDDataChan := cMerge.ChangeMerge(SdataChan1, DdataChan1)
	vlog = fmt.Sprintf("(%d) Processing WHERE conditions for index column %s in %s.%s", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	global.Wlog.Debug(vlog)
	//处理原目标端索引列数据的集合，并按照单次校验数据块大小来进行数据截取，如果是多列索引，则需要递归查询截取
	for {
		select {
		case cc, ok := <-ascUniqSDDataChan:
			autoIncSeq++
			var key, value string
			for k, v := range cc {
				key = k
				value = fmt.Sprintf("%v", v)
			}
			if !ok {
				if level == 0 {
					close(sqlWhere)
				}
				return
			}
			vlog = fmt.Sprintf("(%d) Index column %s level %d - WHERE: %s, value: %s, count: %v", logThreadSeq, sp.columnName[level], level, where, key, value)
			global.Wlog.Debug(vlog)
			if key == "<nil>" || key == "<entry>" {
				vlog = fmt.Sprintf("(%d) Processing NULL values for index column %s level %d", logThreadSeq, sp.columnName[level], level)
				global.Wlog.Debug(vlog)
				if e != "" { //假如null或者entry不是首行，则先处理原有数据条件
					if key != "END" {
						g = key
					}
					if e == g {
						sqlwhere = fmt.Sprintf(" %v >= '%v' and %v <= '%v' ", sp.columnName[level], e, sp.columnName[level], g)
					} else {
						sqlwhere = fmt.Sprintf(" %v > '%v' and %v <= '%v' ", sp.columnName[level], e, sp.columnName[level], g)
					}
					if where != "" {
						sqlwhere = fmt.Sprintf("%s %s", where, sqlwhere)
					}
					sqlWhere <- sqlwhere
					sqlwhere, e, g = "", "", ""
				}
				var whereExist string
				if where != "" {
					whereExist = fmt.Sprintf("%s and ", where)
				}
				if key == "<entry>" {
					sqlwhere = fmt.Sprintf("%s %s = '' ", whereExist, sp.columnName[level])
				}
				if key == "<nil>" {
					sqlwhere = fmt.Sprintf("%s %s is null ", whereExist, sp.columnName[level])
				}
				partFirstValue = true
				vlog = fmt.Sprintf("(%d) NULL values processed for index column %s level %d - WHERE: %s", logThreadSeq, sp.columnName[level], level, sqlwhere)
				global.Wlog.Debug(vlog)
				sqlWhere <- sqlwhere
				sqlwhere = ""
			} else {
				//获取联合索引或单列索引的首值
				if key != "END" && e == "" {
					e = key
				}
				vlog = fmt.Sprintf("(%d) Index column %s level %d starting value: %s", logThreadSeq, sp.columnName[level], level, e)
				global.Wlog.Debug(vlog)
				//获取每行的count值,并将count值记录及每次动态的值
				if key != "END" {
					c, _ = strconv.Atoi(value)
					g = key
					if level == 0 {
						curryCount = curryCount + int64(c)
					}
					// group count(*)的值进行累加
					d = d + c
				}
				//判断行数累加值是否小于要校验的值，并且是最后一条索引列数据
				if d < queryNum && d > 0 && key == "END" {
					vlog = fmt.Sprintf("(%d) Processing end of index column %s level %d", logThreadSeq, sp.columnName[level], level)
					global.Wlog.Debug(vlog)
					var whereExist string
					if where != "" {
						whereExist = fmt.Sprintf("%v and ", where)
					}
					if partFirstValue {
						sqlwhere = fmt.Sprintf("%v %v >= '%v' and %v <= '%v' ", whereExist, sp.columnName[level], e, sp.columnName[level], g)
						partFirstValue = false
					} else {
						sqlwhere = fmt.Sprintf("%v %v > '%v' and %v <= '%v' ", whereExist, sp.columnName[level], e, sp.columnName[level], g)
					}

					sqlWhere <- sqlwhere
					sqlwhere = ""
					vlog = fmt.Sprintf("(%d) Completed processing end of index column %s level %d - WHERE: %s", logThreadSeq, sp.columnName[level], level, sqlwhere)
					global.Wlog.Debug(vlog)
				}
			}
			//判断行数累加值是否>=要校验的值
			if d >= queryNum {
				//判断联合索引列深度
				//判断当前索引列的重复值是否是校验数据块大小的两倍
				if (d/queryNum < 2 && level < len(sp.columnName)-1) || level == len(sp.columnName)-1 { //小于校验块的两倍，则直接输出当前索引列深度的条件
					var whereExist string
					if where != "" { //非第一层索引列数据
						whereExist = fmt.Sprintf("%s and ", where)
					}
					if d == c && c >= queryNum { //单行索引列数据的group值大于并发数
						sqlwhere = fmt.Sprintf("%s %v = '%v' ", whereExist, sp.columnName[level], g)
					} else {
						if partFirstValue { //每段的首行数据
							sqlwhere = fmt.Sprintf("%s %v >= '%v' and %v <= '%v' ", whereExist, sp.columnName[level], e, sp.columnName[level], g)
							partFirstValue = false
						} else {
							sqlwhere = fmt.Sprintf("%s %v > '%v' and %v <= '%v' ", whereExist, sp.columnName[level], e, sp.columnName[level], g)
						}
					}
					sqlWhere <- sqlwhere
					if key != "END" {
						e = key
					}
					sqlwhere = ""
				} else {
					if where != "" {
						where = fmt.Sprintf(" %v and %v = '%v' ", where, sp.columnName[level], g)
					} else {
						where = fmt.Sprintf(" %v = '%v' ", sp.columnName[level], g)
					}
					level++ //索引列层数递增
					//进入下一层的索引计算
					sp.recursiveIndexColumn(sqlWhere, sdb, ddb, level, queryNum, where, selectColumn, logThreadSeq)

					level-- //回到上一层
					//递归处理结束后，处理where条件，将下一层的索引列条件去掉
					if strings.Contains(strings.TrimSpace(where), sp.columnName[level]) {
						where = strings.TrimSpace(where[:strings.Index(where, sp.columnName[level])])
						if strings.HasSuffix(where, "and") {
							where = strings.TrimSpace(where[:strings.LastIndex(where, "and")])
						}
					}
					if key != "END" {
						e = key
					}
				}
				d = 0 //累加值清0
			}
		}
	}
	vlog = fmt.Sprintf("(%d) Completed WHERE condition processing for index column %s in %s.%s", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	global.Wlog.Debug(vlog)
}

func (sp *SchedulePlan) indexColumnDispos(sqlWhere chanString, selectColumn map[string]map[string]string) {
	var (
		vlog         string
		logThreadSeq int64
	)
	time.Sleep(time.Nanosecond * 2)
	rand.Seed(time.Now().UnixNano())
	logThreadSeq = rand.Int63()
	vlog = fmt.Sprintf("(%d) Generating query sequence for table %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)

	//查询表索引列数据并且生成查询的where条件
	sdb := sp.sdbPool.Get(logThreadSeq)
	ddb := sp.ddbPool.Get(logThreadSeq)
	sp.recursiveIndexColumn(sqlWhere, sdb, ddb, 0, sp.chanrowCount, "", selectColumn, logThreadSeq)
	sp.sdbPool.Put(sdb, logThreadSeq)
	sp.ddbPool.Put(ddb, logThreadSeq)
	vlog = fmt.Sprintf("(%d) Query sequence generated for table %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
}

/*
针对表的所有列的数据类型进行处理，将列类型转换成字符串，例如时间类型
*/
// Deprecated: 请使用queryTableSqlSeparate函数替代
func (sp *SchedulePlan) queryTableSql(sqlWhere chanString, selectSql chanMap, cc1 global.TableAllColumnInfoS, sc chan int64, logThreadSeq int64) {
	// 保持向后兼容
	sp.queryTableSqlSeparate(sqlWhere, make(chanMap), make(chanMap), cc1, sc, logThreadSeq)
	var (
		vlog    string
		curry   = make(chanStruct, sp.concurrency)
		autoSeq int64
		err     error
	)
	vlog = fmt.Sprintf("(%d) Processing block data checksum queries", logThreadSeq)
	global.Wlog.Debug(vlog)
	for {
		select {
		case c, ok := <-sqlWhere:
			if !ok {
				if len(curry) == 0 {
					sc <- autoSeq
					close(sc)
					close(selectSql)
					return
				}
			} else {
				autoSeq++
				curry <- struct{}{}
				sdb := sp.sdbPool.Get(logThreadSeq)
				ddb := sp.ddbPool.Get(logThreadSeq)
				//查询该表的列名和列信息
				go func(c1 string, sd, dd *sql.DB, sdbPool, ddbPool *global.Pool) {
					var selectSqlMap = make(map[string]string)
					defer func() {
						sdbPool.Put(sd, logThreadSeq)
						ddbPool.Put(dd, logThreadSeq)
						<-curry
					}()
					// 为源端生成WHERE条件
					sourceWhere := strings.Replace(c1, fmt.Sprintf("%s.%s", sp.destSchema, sp.table), fmt.Sprintf("%s.%s", sp.sourceSchema, sp.table), -1)
					sourceWhere = strings.Replace(sourceWhere, fmt.Sprintf("`%s`.`%s`", sp.destSchema, sp.table), fmt.Sprintf("`%s`.`%s`", sp.sourceSchema, sp.table), -1)

					// 源端使用sourceSchema和sourceTable
					idxc := dbExec.IndexColumnStruct{
						Schema:      sp.sourceSchema,
						Table:       sp.table,
						TableColumn: cc1.SColumnInfo,
						Sqlwhere:    sourceWhere,
						Drivce:      sp.sdrive,
						ColData:     cc1.SColumnInfo,
					}
					lock.Lock()
					selectSqlMap[sp.sdrive], err = idxc.TableIndexColumn().GeneratingQuerySql(sd, logThreadSeq)
					if err != nil {
						vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Failed to generate source query SQL for %s.%s: %v", logThreadSeq, sp.sourceSchema, sp.table, err)
						global.Wlog.Error(vlog)
						lock.Unlock()
						return
					}
					lock.Unlock()

					// 确保目标数据库存在
					ddb := sp.ddbPool.Get(logThreadSeq)
					_, err = ddb.Exec(fmt.Sprintf("USE `%s`", sp.destSchema))
					if err != nil {
						vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Target database %s does not exist", logThreadSeq, sp.destSchema)
						global.Wlog.Error(vlog)
						sp.ddbPool.Put(ddb, logThreadSeq)
						return
					}
					sp.ddbPool.Put(ddb, logThreadSeq)

					// 为目标端生成WHERE条件
					destWhere := strings.Replace(c1, fmt.Sprintf("%s.%s", sp.sourceSchema, sp.table), fmt.Sprintf("%s.%s", sp.destSchema, sp.table), -1)
					destWhere = strings.Replace(destWhere, fmt.Sprintf("`%s`.`%s`", sp.sourceSchema, sp.table), fmt.Sprintf("`%s`.`%s`", sp.destSchema, sp.table), -1)

					// 目标端使用destSchema和destTable
					idxcDest := dbExec.IndexColumnStruct{
						Schema:      sp.destSchema,
						Table:       sp.table,
						TableColumn: cc1.DColumnInfo,
						Sqlwhere:    destWhere,
						Drivce:      sp.ddrive,
						ColData:     cc1.DColumnInfo,
					}
					// 添加对目标表存在的检查
					ddb = sp.ddbPool.Get(logThreadSeq)
					_, err = ddb.Exec(fmt.Sprintf("SELECT 1 FROM `%s`.`%s` LIMIT 1", sp.destSchema, sp.table))
					if err != nil {
						vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Target table %s.%s does not exist", logThreadSeq, sp.destSchema, sp.table)
						global.Wlog.Error(vlog)
						sp.ddbPool.Put(ddb, logThreadSeq)
						return
					}
					sp.ddbPool.Put(ddb, logThreadSeq)
					lock.Lock()
					selectSqlMap[sp.ddrive], err = idxcDest.TableIndexColumn().GeneratingQuerySql(dd, logThreadSeq)
					if err != nil {
						vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Failed to generate destination query SQL for %s.%s: %v", logThreadSeq, sp.destSchema, sp.table, err)
						global.Wlog.Error(vlog)
						return
					}
					lock.Unlock()
					vlog = fmt.Sprintf("(%d) Block data checksum queries completed", logThreadSeq)
					global.Wlog.Debug(vlog)
					selectSql <- selectSqlMap
				}(c, sdb, ddb, sp.sdbPool, sp.ddbPool)
			}
		}
	}
}

/*
针对表的所有列的数据类型进行处理，将列类型转换成字符串，例如时间类型，并执行sql语句
*/
// Deprecated: 请使用queryTableDataSeparate函数替代
func (sp *SchedulePlan) queryTableData(selectSql chanMap, diffQueryData chanDiffDataS, cc1 global.TableAllColumnInfoS, sc chan int64, logThreadSeq int64) {
	// 保持向后兼容
	sp.queryTableDataSeparate(selectSql, make(chanMap), diffQueryData, cc1, sc, logThreadSeq)
	var (
		vlog               string
		aa                 = &CheckSumTypeStruct{}
		differencesData    = InitDifferencesDataStruct()
		curry              = make(chanStruct, sp.concurrency)
		autoSeq1, autoSeq2 int64
	)
	sp.bar = &Bar{}
	// 始终使用rows模式
	if sp.tableMaxRows > 0 {
		barTotal := int64(sp.tableMaxRows / uint64(sp.chanrowCount))
		if sp.tableMaxRows%uint64(sp.chanrowCount) > 0 {
			barTotal += 1
		}
		sp.bar.NewOption(0, barTotal, "Processing")
	}
	for {
		select {
		case d, ok := <-sc:
			if ok {
				sp.bar.NewOption(0, d, "Processing")
			}
		case c, ok := <-selectSql:
			if !ok {
				if len(curry) == 0 {
					close(diffQueryData)
					return
				}
			} else {
				autoSeq1++
				// 源端检查使用sourceSchema
				idxc := dbExec.IndexColumnStruct{
					Schema:      sp.sourceSchema,
					Table:       sp.table,
					TableColumn: cc1.SColumnInfo,
					Sqlwhere:    c[sp.sdrive],
					Drivce:      sp.sdrive,
					ColData:     cc1.SColumnInfo,
				}
				curry <- struct{}{}
				go func(c1 map[string]string, cc1 global.TableAllColumnInfoS) {
					defer func() {
						<-curry
					}()
					//查询源端表数据
					vlog = fmt.Sprintf("(%d) Querying source table %s.%s block data", logThreadSeq, sp.sourceSchema, sp.table)
					global.Wlog.Debug(vlog)
					sdb := sp.sdbPool.Get(logThreadSeq)
					stt, err := idxc.TableIndexColumn().GeneratingQueryCriteria(sdb, logThreadSeq)
					vlog = fmt.Sprintf("(%d) Source table %s.%s query result", logThreadSeq, sp.sourceSchema, sp.table)
					global.Wlog.Debug(vlog)
					sp.sdbPool.Put(sdb, logThreadSeq)
					if err != nil {
						vlog = fmt.Sprintf("(%d) Failed to query source table %s.%s: %v", logThreadSeq, sp.sourceSchema, sp.table, err)
						global.Wlog.Error(vlog)
						return
					}

					// 目标端检查使用destSchema
					idxcDest := dbExec.IndexColumnStruct{
						Schema:      sp.destSchema,
						Table:       sp.table,
						Sqlwhere:    c1[sp.ddrive],
						TableColumn: cc1.DColumnInfo,
						Drivce:      sp.ddrive,
						ColData:     cc1.DColumnInfo,
					}
					ddb := sp.ddbPool.Get(logThreadSeq)
					dtt, err := idxcDest.TableIndexColumn().GeneratingQueryCriteria(ddb, logThreadSeq)
					vlog = fmt.Sprintf("(%d) Target table %s.%s query result", logThreadSeq, sp.destSchema, sp.table)
					global.Wlog.Debug(vlog)
					sp.ddbPool.Put(ddb, logThreadSeq)
					if err != nil {
						vlog = fmt.Sprintf("(%d) Failed to query target table %s.%s: %v", logThreadSeq, sp.destSchema, sp.table, err)
						global.Wlog.Error(vlog)
						return
					}
					vlog = fmt.Sprintf("(%d) Checking block data consistency for %s.%s", logThreadSeq, sp.sourceSchema, sp.table)
					global.Wlog.Debug(vlog)
					if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
						vlog = fmt.Sprintf("(%d) Data inconsistency found in %s.%s - Query: %s", logThreadSeq, sp.schema, sp.table, c1)
						global.Wlog.Debug(vlog)
						differencesData.Table = sp.table
						differencesData.Schema = sp.schema
						differencesData.SqlWhere = c1
						differencesData.TableColumnInfo = cc1
						differencesData.indexColumnType = sp.indexColumnType
						if differencesData.Schema != "" && differencesData.Table != "" {
							diffQueryData <- differencesData
						}
					} else {
						vlog = fmt.Sprintf("(%d) Data consistent in %s.%s - Query: %s", logThreadSeq, sp.schema, sp.table, c1)
						global.Wlog.Debug(vlog)
					}
					stt, dtt = "", ""
					vlog = fmt.Sprintf("(%d) Block data checksum completed for %s.%s", logThreadSeq, sp.schema, sp.table)
					global.Wlog.Debug(vlog)
				}(c, cc1)
			}
		}
		if autoSeq1 > autoSeq2 {
			sp.bar.Play(autoSeq1)
			autoSeq2 = autoSeq1
		}
	}
	sp.bar.Finish()
	time.Sleep(time.Millisecond)
}

/*
差异数据的二次校验，并生成修复语句
*/
func (sp *SchedulePlan) AbnormalDataDispos(diffQueryData chanDiffDataS, cc chanString, logThreadSeq int64) {
	var (
		vlog string
		aa   = &CheckSumTypeStruct{}
		//strsqlSliect []string
		curry = make(chanStruct, sp.concurrency)
	)
	vlog = fmt.Sprintf("(%d) Processing differences and generating repair statements for %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)

	for {
		select {
		case c, ok := <-diffQueryData:
			if !ok {
				if len(curry) == 0 {
					close(cc)
					return
				}
			} else {
				sdb := sp.sdbPool.Get(logThreadSeq)
				ddb := sp.ddbPool.Get(logThreadSeq)
				curry <- struct{}{}
				go func(c1 DifferencesDataStruct, sdb, ddb *sql.DB) {
					defer func() {
						<-curry
						sp.sdbPool.Put(sdb, logThreadSeq)
						sp.ddbPool.Put(ddb, logThreadSeq)
					}()
					// 使用映射后的源端和目标端schema和table
					sourceSchema := sp.sourceSchema
					destSchema := sp.destSchema
					table := sp.table

					// 获取列数据时使用原始schema.table组合
					colData := sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sourceSchema, table)]

					// 处理源端SQL条件，确保使用源端schema
					sourceSqlWhere := c1.SqlWhere[sp.sdrive]
					// 如果源端SQL条件中包含目标端schema，替换为源端schema
					if strings.Contains(sourceSqlWhere, fmt.Sprintf("`%s`", destSchema)) {
						sourceSqlWhere = strings.Replace(sourceSqlWhere,
							fmt.Sprintf("`%s`", destSchema),
							fmt.Sprintf("`%s`", sourceSchema), -1)
					}
					if strings.Contains(sourceSqlWhere, fmt.Sprintf("%s.", destSchema)) {
						sourceSqlWhere = strings.Replace(sourceSqlWhere,
							fmt.Sprintf("%s.", destSchema),
							fmt.Sprintf("%s.", sourceSchema), -1)
					}

					// 处理目标端SQL条件，确保使用目标端schema
					destSqlWhere := c1.SqlWhere[sp.ddrive]
					// 如果目标端SQL条件中包含源端schema，替换为目标端schema
					if strings.Contains(destSqlWhere, fmt.Sprintf("`%s`", sourceSchema)) {
						destSqlWhere = strings.Replace(destSqlWhere,
							fmt.Sprintf("`%s`", sourceSchema),
							fmt.Sprintf("`%s`", destSchema), -1)
					}
					if strings.Contains(destSqlWhere, fmt.Sprintf("%s.", sourceSchema)) {
						destSqlWhere = strings.Replace(destSqlWhere,
							fmt.Sprintf("%s.", sourceSchema),
							fmt.Sprintf("%s.", destSchema), -1)
					}

					// Log for debugging
					vlog = fmt.Sprintf("(%d) AbnormalDataDispos - Source SQL condition: %s", logThreadSeq, sourceSqlWhere)
					global.Wlog.Debug(vlog)
					vlog = fmt.Sprintf("(%d) AbnormalDataDispos - Target SQL condition: %s", logThreadSeq, destSqlWhere)
					global.Wlog.Debug(vlog)

					// 源端查询使用sourceSchema和table
					idxc := dbExec.IndexColumnStruct{
						Schema:      sourceSchema,
						Table:       table,
						TableColumn: colData.SColumnInfo,
						Drivce:      sp.sdrive,
						Sqlwhere:    sourceSqlWhere, // 使用处理后的源端SQL条件
					}
					stt, _ := idxc.TableIndexColumn().GeneratingQueryCriteria(sdb, logThreadSeq)

					// 目标端查询使用destSchema和table
					idxcDest := dbExec.IndexColumnStruct{
						Schema:      destSchema,
						Table:       table,
						TableColumn: colData.DColumnInfo,
						Drivce:      sp.ddrive,
						Sqlwhere:    destSqlWhere, // 使用处理后的目标端SQL条件
					}
					dtt, _ := idxcDest.TableIndexColumn().GeneratingQueryCriteria(ddb, logThreadSeq)

					if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
						add, del := aa.Arrcmp(strings.Split(stt, "/*go actions rowData*/"), strings.Split(dtt, "/*go actions rowData*/"))
						stt, dtt = "", ""
						vlog = fmt.Sprintf("(%d) Generating repair statements for %s.%s differences", logThreadSeq, c1.Schema, c1.Table)
						global.Wlog.Debug(vlog)
						if len(del) > 0 || len(add) > 0 {
							// 确保使用正确的源和目标schema
							sourceSchema := sp.sourceSchema
							destSchema := sp.destSchema
							if sourceSchema == "" {
								sourceSchema = c1.Schema
							}
							if destSchema == "" {
								destSchema = c1.Schema
							}

							// 添加对空IndexColumn的检查
							indexColumns := sp.columnName
							if len(indexColumns) == 0 {
								// 如果没有索引列，使用所有列作为条件
								indexColumns = make([]string, 0, len(colData.DColumnInfo))
								for _, colInfo := range colData.DColumnInfo {
									if colName, ok := colInfo["columnName"]; ok {
										indexColumns = append(indexColumns, colName)
									}
								}
							}

							// 处理源端和目标端SQL条件
							// 获取原始SQL条件
							originalSourceSqlWhere := c1.SqlWhere[sp.sdrive]
							originalDestSqlWhere := c1.SqlWhere[sp.ddrive]

							// 处理源端SQL条件，确保使用源端schema
							sourceSqlWhere := originalSourceSqlWhere
							// 如果源端SQL条件中包含目标端schema，替换为源端schema
							if strings.Contains(sourceSqlWhere, fmt.Sprintf("`%s`", destSchema)) {
								sourceSqlWhere = strings.Replace(sourceSqlWhere,
									fmt.Sprintf("`%s`", destSchema),
									fmt.Sprintf("`%s`", sourceSchema), -1)
							}
							if strings.Contains(sourceSqlWhere, fmt.Sprintf("%s.", destSchema)) {
								sourceSqlWhere = strings.Replace(sourceSqlWhere,
									fmt.Sprintf("%s.", destSchema),
									fmt.Sprintf("%s.", sourceSchema), -1)
							}

							// 处理目标端SQL条件，确保使用目标端schema
							destSqlWhere := originalDestSqlWhere
							// 如果目标端SQL条件中包含源端schema，替换为目标端schema
							if strings.Contains(destSqlWhere, fmt.Sprintf("`%s`", sourceSchema)) {
								destSqlWhere = strings.Replace(destSqlWhere,
									fmt.Sprintf("`%s`", sourceSchema),
									fmt.Sprintf("`%s`", destSchema), -1)
							}
							if strings.Contains(destSqlWhere, fmt.Sprintf("%s.", sourceSchema)) {
								destSqlWhere = strings.Replace(destSqlWhere,
									fmt.Sprintf("%s.", sourceSchema),
									fmt.Sprintf("%s.", destSchema), -1)
							}

							// Log for debugging
							vlog = fmt.Sprintf("(%d) DataFixSql - Source SQL condition: %s", logThreadSeq, sourceSqlWhere)
							global.Wlog.Debug(vlog)
							vlog = fmt.Sprintf("(%d) DataFixSql - Target SQL condition: %s", logThreadSeq, destSqlWhere)
							global.Wlog.Debug(vlog)

							// 修复SQL生成时使用正确的schema映射
							dbf := dbExec.DataAbnormalFixStruct{
								Schema:       destSchema,   // 目标schema
								SourceSchema: sourceSchema, // 源端schema，用于处理数据库映射关系
								Table:        table,        // 使用映射后的表名
								ColData:      colData.DColumnInfo,
								Sqlwhere:     destSqlWhere, // 使用处理后的目标端SQL条件
								DestDevice:   sp.ddrive,
								IndexColumn:  indexColumns,
								DatafixType:  sp.datafixType,
							}
							if strings.HasPrefix(c1.indexColumnType, "pri") {
								dbf.IndexType = "pri"
							} else if strings.HasPrefix(c1.indexColumnType, "uni") {
								dbf.IndexType = "uni"
							} else {
								dbf.IndexType = "mul"
							}
							if len(del) > 0 {
								vlog = fmt.Sprintf("(%d) Generating DELETE statements for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
								for _, i := range del {
									dbf.RowData = i
									sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
									if err != nil {
										sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
									}
									if sqlstr != "" {
										cc <- sqlstr
									}
								}
								vlog = fmt.Sprintf("(%d) DELETE statements generated for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
							}
							if len(add) > 0 {
								vlog = fmt.Sprintf("(%d) Generating INSERT statements for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
								for _, i := range add {
									dbf.RowData = i
									sqlstr, err := dbf.DataAbnormalFix().FixInsertSqlExec(ddb, sp.ddrive, logThreadSeq)
									if err != nil {
										sp.getErr(fmt.Sprintf("dest: checksum table %s.%s generate INSERT sql error.", c1.Schema, c1.Table), err)
									}
									if sqlstr != "" {
										cc <- sqlstr
									}
								}
								vlog = fmt.Sprintf("(%d) INSERT statements generated for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
							}
						}
					}
				}(c, sdb, ddb)
			}
		}
	}
	vlog = fmt.Sprintf("(%d) Completed difference processing and repair statements for %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
}

func (sp SchedulePlan) DataFixDispos(fixSQL chanString, logThreadSeq int64) {
	var (
		vlog     string
		noIndexD = make(chan struct{}, sp.concurrency)
		increSeq int
		sqlSlice []string
	)
	vlog = fmt.Sprintf("(%d) Applying repair statements to target table %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
	for {
		select {
		case v, ok := <-fixSQL:
			if !ok {
				if len(noIndexD) == 0 {
					if len(sqlSlice) > 0 {
						ApplyDataFix(sqlSlice, sp.datafixType, sp.sfile, sp.ddrive, sp.djdbc, logThreadSeq)
						vlog = fmt.Sprintf("(%d) DELETE repair statements generated for %s.%s", logThreadSeq, sp.schema, sp.table)
						global.Wlog.Debug(vlog)
						sqlSlice = []string{}
					} else {
						measuredDataPods = append(measuredDataPods, *sp.pods)
						return
					}
				}
			} else {
				increSeq++
				sp.pods.DIFFS = "yes"
				sqlSlice = append(sqlSlice, v)
				if increSeq == sp.fixTrxNum {
					var sqlSlice1 []string
					for _, i := range sqlSlice {
						sqlSlice1 = append(sqlSlice1, i)
					}
					sqlSlice = []string{}
					//noIndexD <- struct{}{}
					increSeq = 0
					//go func(a []string) {
					//	defer func() {
					//		<-noIndexD
					//	}()
					ApplyDataFix(sqlSlice1, sp.datafixType, sp.sfile, sp.ddrive, sp.djdbc, logThreadSeq)
					vlog = fmt.Sprintf("(%d) The delete repair sql statements of table %s.%s are generated.", logThreadSeq, sp.schema, sp.table)
					global.Wlog.Debug(vlog)
					//}(sqlSlice1)
				}
			}
		}
	}

}

/*
处理有索引表的数据校验
*/
func (sp SchedulePlan) doIndexDataCheck() {
	var (
		queueDepth          = sp.mqQueueDepth
		sqlWhere            = make(chanString, queueDepth)
		diffQueryData       = make(chanDiffDataS, queueDepth)
		fixSQL              = make(chanString, queueDepth)
		tableColumn         = sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.schema, sp.table)]
		selectColumnStringM = make(map[string]map[string]string)
	)
	var idxc, idxcDest dbExec.IndexColumnStruct
	rand.Seed(time.Now().UnixNano())
	logThreadSeq := rand.Int63()
	idxc = dbExec.IndexColumnStruct{Schema: sp.sourceSchema, Table: sp.table, ColumnName: sp.columnName,
		ChanrowCount: sp.chanrowCount, Drivce: sp.sdrive,
		ColData: sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.sourceSchema, sp.table)].SColumnInfo}
	selectColumnStringM[sp.sdrive] = idxc.TableIndexColumn().TmpTableIndexColumnSelectDispos(logThreadSeq)
	idxcDest = dbExec.IndexColumnStruct{Schema: sp.destSchema, Table: sp.table, ColumnName: sp.columnName,
		ChanrowCount: sp.chanrowCount, Drivce: sp.ddrive,
		ColData: sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.destSchema, sp.table)].DColumnInfo}
	selectColumnStringM[sp.ddrive] = idxcDest.TableIndexColumn().TmpTableIndexColumnSelectDispos(logThreadSeq)

	// 设置Pod结构体，包括映射关系信息
	mappingInfo := ""
	if sp.sourceSchema != sp.destSchema {
		mappingInfo = fmt.Sprintf("Schema: %s:%s", sp.sourceSchema, sp.destSchema)
		if sp.table != sp.table { // 如果表名也不同，添加表名映射信息
			mappingInfo += fmt.Sprintf(", Table: %s:%s", sp.table, sp.table)
		}
	} else if sp.table != sp.table { // 只有表名不同
		mappingInfo = fmt.Sprintf("Table: %s:%s", sp.table, sp.table)
	}

	sp.pods = &Pod{
		Schema:      sp.schema,
		Table:       sp.table,
		IndexColumn: strings.TrimLeft(strings.Join(sp.columnName, ","), ","),
		CheckObject: sp.checkObject, // 添加CheckObject字段
		DIFFS:       "no",
		Datafix:     sp.datafixType,
		MappingInfo: mappingInfo,
	}
	// 确保使用正确的源表和目标表的Schema
	idxc = dbExec.IndexColumnStruct{Schema: sp.sourceSchema, Table: sp.table, Drivce: sp.sdrive}
	sdb := sp.sdbPool.Get(logThreadSeq)
	var vlog string
	vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Querying source table rows for %s.%s", logThreadSeq, sp.sourceSchema, sp.table)
	global.Wlog.Debug(vlog)
	A, err := idxc.TableIndexColumn().TableRows(sdb, int64(logThreadSeq))
	sp.sdbPool.Put(sdb, logThreadSeq)
	if err != nil {
		vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Failed to get source table rows for %s.%s: %v", logThreadSeq, sp.sourceSchema, sp.table, err)
		global.Wlog.Error(vlog)
		return
	}

	idxcDest = dbExec.IndexColumnStruct{Schema: sp.destSchema, Table: sp.table, Drivce: sp.ddrive}
	ddb := sp.ddbPool.Get(logThreadSeq)
	vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Querying destination table rows for %s.%s", logThreadSeq, sp.destSchema, sp.table)
	global.Wlog.Debug(vlog)
	B, err := idxcDest.TableIndexColumn().TableRows(ddb, int64(logThreadSeq))
	if err != nil {
		vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Failed to get destination table rows for %s.%s: %v", logThreadSeq, sp.destSchema, sp.table, err)
		global.Wlog.Error(vlog)
		return
	}
	sp.ddbPool.Put(ddb, logThreadSeq)
	if A >= B {
		sp.tableMaxRows = A
	} else {
		sp.tableMaxRows = B
	}
	// 重新查询精确行数
	sourceExactCount := sp.getExactRowCount(sp.sdbPool, sp.sourceSchema, sp.table, logThreadSeq)
	targetExactCount := sp.getExactRowCount(sp.ddbPool, sp.destSchema, sp.table, logThreadSeq)
	sp.pods.Rows = fmt.Sprintf("%d,%d", sourceExactCount, targetExactCount)

	// 创建独立的channel用于源端和目标端查询SQL
	sourceSelectSql := make(chanMap, sp.mqQueueDepth)
	destSelectSql := make(chanMap, sp.mqQueueDepth)

	var scheduleCount = make(chan int64, 1)
	go sp.indexColumnDispos(sqlWhere, selectColumnStringM)

	// 调用分离的查询函数
	go sp.queryTableSqlSeparate(sqlWhere, sourceSelectSql, destSelectSql, tableColumn, scheduleCount, logThreadSeq)
	go sp.queryTableDataSeparate(sourceSelectSql, destSelectSql, diffQueryData, tableColumn, scheduleCount, logThreadSeq)

	go sp.AbnormalDataDispos(diffQueryData, fixSQL, logThreadSeq)
	sp.DataFixDispos(fixSQL, logThreadSeq)
}

// 新的函数处理分离的源端和目标端查询
func (sp *SchedulePlan) queryTableSqlSeparate(sqlWhere chanString, sourceSelectSql chanMap, destSelectSql chanMap, cc1 global.TableAllColumnInfoS, sc chan int64, logThreadSeq int64) {
	for c := range sqlWhere {
		// 源端查询SQL
		sourceWhere := strings.Replace(c, fmt.Sprintf("%s.%s", sp.destSchema, sp.table), fmt.Sprintf("%s.%s", sp.sourceSchema, sp.table), -1)
		sourceWhere = strings.Replace(sourceWhere, fmt.Sprintf("`%s`.`%s`", sp.destSchema, sp.table), fmt.Sprintf("`%s`.`%s`", sp.sourceSchema, sp.table), -1)

		idxc := dbExec.IndexColumnStruct{
			Schema:   sp.sourceSchema,
			Table:    sp.table,
			Drivce:   sp.sdrive,
			Sqlwhere: sourceWhere,
			ColData:  cc1.SColumnInfo,
		}
		sdb := sp.sdbPool.Get(logThreadSeq)
		sql, err := idxc.TableIndexColumn().GeneratingQuerySql(sdb, logThreadSeq)
		sp.sdbPool.Put(sdb, logThreadSeq)
		if err != nil {
			continue
		}
		sourceSelectSql <- map[string]string{sp.sdrive: sql}

		// 目标端查询SQL
		destWhere := strings.Replace(c, fmt.Sprintf("%s.%s", sp.sourceSchema, sp.table), fmt.Sprintf("%s.%s", sp.destSchema, sp.table), -1)
		destWhere = strings.Replace(destWhere, fmt.Sprintf("`%s`.`%s`", sp.sourceSchema, sp.table), fmt.Sprintf("`%s`.`%s`", sp.destSchema, sp.table), -1)

		idxcDest := dbExec.IndexColumnStruct{
			Schema:   sp.destSchema,
			Table:    sp.table,
			Drivce:   sp.ddrive,
			Sqlwhere: destWhere,
			ColData:  cc1.DColumnInfo,
		}
		ddb := sp.ddbPool.Get(logThreadSeq)
		sql, err = idxcDest.TableIndexColumn().GeneratingQuerySql(ddb, logThreadSeq)
		sp.ddbPool.Put(ddb, logThreadSeq)
		if err != nil {
			continue
		}
		destSelectSql <- map[string]string{sp.ddrive: sql}
	}
	close(sourceSelectSql)
	close(destSelectSql)
}

func (sp *SchedulePlan) queryTableDataSeparate(sourceSelectSql chanMap, destSelectSql chanMap, diffQueryData chanDiffDataS, cc1 global.TableAllColumnInfoS, sc chan int64, logThreadSeq int64) {
	var (
		curry = make(chanStruct, sp.concurrency)
	)

	for {
		select {
		case d, ok := <-sc:
			if ok {
				sp.bar.NewOption(0, d, "Processing")
			}
		case sourceSql, ok := <-sourceSelectSql:
			if !ok {
				if len(curry) == 0 {
					close(diffQueryData)
					return
				}
			} else {
				destSql := <-destSelectSql
				autoSeq := int64(0)
				autoSeq++
				curry <- struct{}{}
				go func(sourceSql, destSql map[string]string) {
					defer func() {
						<-curry
					}()

					// 源端查询
					sdb := sp.sdbPool.Get(logThreadSeq)
					stt, err := (&dbExec.IndexColumnStruct{
						Schema:   sp.sourceSchema,
						Table:    sp.table,
						Drivce:   sp.sdrive,
						Sqlwhere: sourceSql[sp.sdrive],
						ColData:  cc1.SColumnInfo,
					}).TableIndexColumn().GeneratingQueryCriteria(sdb, logThreadSeq)
					sp.sdbPool.Put(sdb, logThreadSeq)
					if err != nil {
						return
					}

					// 目标端查询
					ddb := sp.ddbPool.Get(logThreadSeq)
					dtt, err := (&dbExec.IndexColumnStruct{
						Schema:   sp.destSchema,
						Table:    sp.table,
						Drivce:   sp.ddrive,
						Sqlwhere: destSql[sp.ddrive],
						ColData:  cc1.DColumnInfo,
					}).TableIndexColumn().GeneratingQueryCriteria(ddb, logThreadSeq)
					sp.ddbPool.Put(ddb, logThreadSeq)
					if err != nil {
						return
					}

					// 比较结果
					aa := &CheckSumTypeStruct{}
					if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
						differencesData := DifferencesDataStruct{
							Schema:          sp.schema,
							Table:           sp.table,
							SqlWhere:        map[string]string{sp.sdrive: sourceSql[sp.sdrive], sp.ddrive: destSql[sp.ddrive]},
							TableColumnInfo: cc1,
						}
						diffQueryData <- differencesData
					}
				}(sourceSql, destSql)
			}
		}
	}
}
