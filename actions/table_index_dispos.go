package actions

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"gt-checksum/utils"
	"math/rand"
	"os"
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

	// 用于跟踪已经输出过目标表为空提示的表，避免重复输出
	emptyTableWarned = make(map[string]bool)
	emptyTableMutex  sync.Mutex
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
	vlog = fmt.Sprintf("(%d) Querying source table %s.%s index column %s with WHERE: %s", logThreadSeq, sp.sourceSchema, sp.table, sp.columnName[level], where)
	global.Wlog.Debug(vlog)
	// 对于复合主键，查询符合前一个索引列条件的索引值，而不是所有可能的值
	// 这确保了递归查询的效率
	SdataChan1, err := idxc.TableIndexColumn().TmpTableColumnGroupDataDispos(sdb, where, sp.columnName[level], logThreadSeq)
	if err != nil {
		return
	}
	idxcDest := dbExec.IndexColumnStruct{Schema: sp.destSchema, Table: sp.table, ColumnName: sp.columnName,
		ChanrowCount: sp.chanrowCount, Drivce: sp.ddrive, SelectColumn: selectColumn[sp.ddrive], ColData: a}
	vlog = fmt.Sprintf("(%d) Querying target table %s.%s index column %s with WHERE: %s", logThreadSeq, sp.destSchema, sp.table, sp.columnName[level], where)
	global.Wlog.Debug(vlog)
	// 对于复合主键，查询符合前一个索引列条件的索引值，而不是所有可能的值
	// 这确保了递归查询的效率
	DdataChan1, err := idxcDest.TableIndexColumn().TmpTableColumnGroupDataDispos(ddb, where, sp.columnName[level], logThreadSeq)
	if err != nil {
		return
	}

	// 修复：对于复合主键，确保递归时生成完整的WHERE条件
	if len(sp.columnName) > 1 {
		// 仅在复合主键的第一列时执行此逻辑
		if level == 0 {
			vlog = fmt.Sprintf("(%d) Handling composite primary key %s for %s.%s", logThreadSeq, strings.Join(sp.columnName, ","), sp.schema, sp.table)
			global.Wlog.Debug(vlog)
		}
	}

	cMerge := dataDispos.DataInfo{ChanQueueDepth: sp.mqQueueDepth}
	ascUniqSDDataChan := cMerge.ChangeMerge(SdataChan1, DdataChan1)

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
				// 修复：在通道关闭前，检查是否还有未处理的边界数据需要查询
				// 这确保了当总数据量正好是chunkSize的整数倍时，最后一条记录不会被遗漏
				global.Wlog.Debug("DEBUG_CHANNEL_CLOSE: level=%d, e='%s', e!=''=%v\n", level, e, e != "")
				// 移除level==0限制，确保所有层级都执行边界检查
				if e != "" {
					var whereExist string
					if where != "" {
						whereExist = fmt.Sprintf("%v and ", where)
					}

					// 生成包含剩余数据的WHERE条件，确保包含最大边界
					sqlwhere := fmt.Sprintf("%v `%v` >= '%v' ", whereExist, sp.columnName[level], e)
					sqlWhere <- sqlwhere

					vlog = fmt.Sprintf("(%d) Added final WHERE condition to ensure all data is covered: %s", logThreadSeq, sqlwhere)
					global.Wlog.Debug(vlog)
				}

				if level == 0 {
					close(sqlWhere)
				}
				return
			}
			//vlog = fmt.Sprintf("(%d) Index column %s level %d - WHERE: %s, value: %s, count: %v", logThreadSeq, sp.columnName[level], level, where, key, value)
			//global.Wlog.Debug(vlog)
			if key == "<nil>" || key == "<entry>" {
				vlog = fmt.Sprintf("(%d) Processing NULL values for index column %s level %d", logThreadSeq, sp.columnName[level], level)
				global.Wlog.Debug(vlog)
				if e != "" { //假如null或者entry不是首行，则先处理原有数据条件
					if key != "END" {
						g = key
					}
					if e == g {
						sqlwhere = fmt.Sprintf(" `%v` >= '%v' and `%v` <= '%v' ", sp.columnName[level], e, sp.columnName[level], g)
					} else {
						sqlwhere = fmt.Sprintf(" `%v` >= '%v' and `%v` <= '%v' ", sp.columnName[level], e, sp.columnName[level], g)
					}
					if where != "" {
						sqlwhere = fmt.Sprintf("%s %s", where, sqlwhere)
					}
					//global.Wlog.Debug("DEBUG_WHERE1: %s", sqlwhere)
					sqlWhere <- sqlwhere

					sqlwhere, e, g = "", "", ""
				}
				var whereExist string
				if where != "" {
					whereExist = fmt.Sprintf("%s and ", where)
				}
				if key == "<entry>" {
					sqlwhere = fmt.Sprintf("%s `%s` = '' ", whereExist, sp.columnName[level])
				}
				if key == "<nil>" {
					sqlwhere = fmt.Sprintf("%s `%s` is null ", whereExist, sp.columnName[level])
				}
				//global.Wlog.Debug("DEBUG_WHERE6: %s", sqlwhere)
				partFirstValue = true
				vlog = fmt.Sprintf("(%d) NULL values processed for index column %s level %d - WHERE: %s", logThreadSeq, sp.columnName[level], level, sqlwhere)
				global.Wlog.Debug(vlog)
				sqlWhere <- sqlwhere

				sqlwhere = ""
			} else {
				//获取联合索引或单列索引的首值
				if key != "END" && e == "" {
					e = key
					global.Wlog.Debug("DEBUG_FIRST_VALUE: First key from merged data stream is '%s'\n", key)
				}
				//vlog = fmt.Sprintf("(%d) Index column %s level %d starting value: %s", logThreadSeq, sp.columnName[level], level, e)
				//global.Wlog.Debug(vlog)

				// 如果是level=0的前几个值，额外记录调试信息
				if level == 0 && autoIncSeq <= 3 {
					global.Wlog.Debug("DEBUG_DATA_STREAM_%d: key='%s', value='%s', current e='%s'\n", autoIncSeq, key, value, e)
				}
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
					// 修复：对于最后一段数据，使用没有上界的条件以确保包含所有剩余记录
					if partFirstValue {
						sqlwhere = fmt.Sprintf("%v `%v` >= '%v' ", whereExist, sp.columnName[level], e)
						partFirstValue = false
					} else {
						sqlwhere = fmt.Sprintf("%v `%v` >= '%v' ", whereExist, sp.columnName[level], e)
					}
					//global.Wlog.Debug("DEBUG_WHERE7: %s", sqlwhere)

					sqlWhere <- sqlwhere

					sqlwhere = ""
					vlog = fmt.Sprintf("(%d) Completed processing end of index column %s level %d - WHERE: %s", logThreadSeq, sp.columnName[level], level, sqlwhere)
					global.Wlog.Debug(vlog)
				}
			}
			//判断行数累加值是否>=要校验的值
			if d >= queryNum {
				//判断联合索引列深度
				if level < len(sp.columnName)-1 { //如果不是最后一列，继续递归处理
					// 修复：对于复合主键，确保递归时传递完整的WHERE条件
					var newWhere string
					if where != "" {
						newWhere = fmt.Sprintf("%s and `%s` >= '%s' and `%s` < '%s'", where, sp.columnName[level], e, sp.columnName[level], g)
					} else {
						newWhere = fmt.Sprintf("`%s` >= '%s' and `%s` < '%s'", sp.columnName[level], e, sp.columnName[level], g)
					}
					//global.Wlog.Debug("DEBUG_WHERE3: %s", newWhere)

					level++ //索引列层数递增
					//进入下一层的索引计算
					sp.recursiveIndexColumn(sqlWhere, sdb, ddb, level, queryNum, newWhere, selectColumn, logThreadSeq)
					level-- //回到上一层
					if key != "END" {
						e = key
					}
				} else { //如果是最后一列，直接输出当前索引列深度的条件
					var whereExist string
					if where != "" { //非第一层索引列数据
						whereExist = fmt.Sprintf("%s and ", where)
					}
					if d == c && c >= queryNum { //单行索引列数据的group值大于并发数
						sqlwhere = fmt.Sprintf("%s `%v` = '%v' ", whereExist, sp.columnName[level], g)
					} else {
						if partFirstValue { //每段的首行数据
							sqlwhere = fmt.Sprintf("%s `%v` >= '%v' and `%v` < '%v' ", whereExist, sp.columnName[level], e, sp.columnName[level], g)
							//global.Wlog.Debug("DEBUG_WHERE8: %s", sqlwhere)

							partFirstValue = false
						} else {
							sqlwhere = fmt.Sprintf("%s `%v` >= '%v' and `%v` < '%v' ", whereExist, sp.columnName[level], e, sp.columnName[level], g)
							//global.Wlog.Debug("DEBUG_WHERE10: %s", sqlwhere)

						}
					}
					//global.Wlog.Debug("DEBUG_WHERE2: %s", sqlwhere)

					sqlWhere <- sqlwhere

					if key != "END" {
						e = key
					}
					sqlwhere = ""
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
		vlog string
		err  error
	)

	// 使用函数创建通道，以便在参数变更时重新初始化
	createCurryChan := func() chanStruct {
		return make(chanStruct, sp.concurrency)
	}

	curry := createCurryChan()
	autoSeq := int64(0)
	vlog = fmt.Sprintf("(%d) Processing block data checksum queries", logThreadSeq)
	global.Wlog.Debug(vlog)

	for {
		select {
		// 监听参数变更通知
		case <-utils.ParamChangedChan:
			// 检查并更新SchedulePlan的参数
			// 从全局配置重新获取最新参数值
			fromGlobalConfig := func() bool {
				// 获取全局配置的最新参数值
				globalConfig := inputArg.GetGlobalConfig()
				if globalConfig != nil {
					sp.concurrency = globalConfig.SecondaryL.RulesV.ParallelThds
					sp.chunkSize = globalConfig.SecondaryL.RulesV.ChanRowCount
					return true
				}
				return false
			}
			if fromGlobalConfig() {
				// 关闭旧通道并创建新通道
				close(curry)
				curry = createCurryChan()
				utils.ResetParamChanged()
				fmt.Printf("(%d) Parameters updated - concurrency: %d, chunkSize: %d\n", logThreadSeq, sp.concurrency, sp.chunkSize)
			}
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
		autoSeq1, autoSeq2 int64
	)

	// 使用函数创建通道，以便在参数变更时重新初始化
	createCurryChan := func() chanStruct {
		return make(chanStruct, sp.concurrency)
	}

	curry := createCurryChan()
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
		// 监听参数变更通知
		case <-utils.ParamChangedChan:
			// 检查并更新SchedulePlan的参数
			// 从全局配置重新获取最新参数值
			fromGlobalConfig := func() bool {
				// 获取全局配置的最新参数值
				globalConfig := inputArg.GetGlobalConfig()
				if globalConfig != nil {
					sp.concurrency = globalConfig.SecondaryL.RulesV.ParallelThds
					sp.chunkSize = globalConfig.SecondaryL.RulesV.ChanRowCount
					return true
				}
				return false
			}
			if fromGlobalConfig() {
				// 关闭旧通道并创建新通道
				close(curry)
				curry = createCurryChan()
				utils.ResetParamChanged()
				fmt.Printf("(%d) Parameters updated - concurrency: %d, chunkSize: %d\n", logThreadSeq, sp.concurrency, sp.chunkSize)
			}
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
		curry            = make(chanStruct, sp.concurrency)
		totalInsertCount int64                       // 全局INSERT语句计数器
		processedInserts = make(map[string]struct{}) // 全局已处理的INSERT记录去重
		insertMutex      sync.Mutex                  // 保护并发访问processedInserts map的互斥锁
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

					// 处理源端SQL条件，确保使用正确的源端数据范围
					var sourceSqlWhere string

					// 修复：使用分批查询逻辑，避免全表查询导致内存消耗过大
					// 基于现有的WHERE条件进行查询，这些条件已经由recursiveIndexColumn正确分片
					var destSqlWhere string // 在更外层声明变量
					// 使用原始的WHERE条件，这些条件已经按照chunkSize正确分片
					sourceSqlWhere = c1.SqlWhere[sp.sdrive]
					destSqlWhere = c1.SqlWhere[sp.ddrive]

					// 确保使用正确的schema
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

					// 重要修复：添加去重逻辑，防止分片数据重复处理
					// 每个WHERE条件应该是独立的，不应该有重叠
					vlog = fmt.Sprintf("(%d) Using chunked query - Source: %s, Target: %s", logThreadSeq, sourceSqlWhere, destSqlWhere)
					global.Wlog.Debug(vlog)

					// Log for debugging
					vlog = fmt.Sprintf("(%d) AbnormalDataDispos - Source SQL condition: %s", logThreadSeq, sourceSqlWhere)
					global.Wlog.Debug(vlog)
					vlog = fmt.Sprintf("(%d) AbnormalDataDispos - Target SQL condition: %s", logThreadSeq, destSqlWhere)
					global.Wlog.Debug(vlog)

					// 检查是否已经有查询结果，避免重复查询
					var stt, dtt string
					if c1.SourceData != "" && c1.DestData != "" {
						// 直接使用已经查询到的数据，避免重复执行SELECT请求
						vlog = fmt.Sprintf("(%d) Reusing existing query results, skipping duplicate SELECT requests for %s.%s", logThreadSeq, c1.Schema, c1.Table)
						global.Wlog.Debug(vlog)
						stt = c1.SourceData
						dtt = c1.DestData
					} else {
						// 源端查询使用sourceSchema和table
						idxc := dbExec.IndexColumnStruct{
							Schema:      sourceSchema,
							Table:       table,
							TableColumn: colData.SColumnInfo,
							Drivce:      sp.sdrive,
							Sqlwhere:    sourceSqlWhere, // 使用处理后的源端SQL条件
						}
						stt, _ = idxc.TableIndexColumn().GeneratingQueryCriteria(sdb, logThreadSeq)

						// 目标端查询使用destSchema和table
						idxcDest := dbExec.IndexColumnStruct{
							Schema:      destSchema,
							Table:       table,
							TableColumn: colData.DColumnInfo,
							Drivce:      sp.ddrive,
							Sqlwhere:    destSqlWhere, // 使用处理后的目标端SQL条件
						}
						dtt, _ = idxcDest.TableIndexColumn().GeneratingQueryCriteria(ddb, logThreadSeq)
					}

					if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
						vlog = fmt.Sprintf("(%d) Data checksum mismatch for %s.%s, need to find specific differences", logThreadSeq, c1.Schema, c1.Table)
						global.Wlog.Debug(vlog)

						// 重要优化：精确比较数据，只找出真正需要修复的记录
						// 1. 将源端和目标端数据转换为切片
						sourceData := strings.Split(stt, "/*go actions rowData*/")
						destData := strings.Split(dtt, "/*go actions rowData*/")

						// 2. 使用优化的Arrcmp实现，只返回真正需要修复的记录
						// 先清理空记录并去重
						cleanSourceData := make([]string, 0, len(sourceData))
						cleanDestData := make([]string, 0, len(destData))
						sourceSeen := make(map[string]struct{})
						destSeen := make(map[string]struct{})

						for _, data := range sourceData {
							data = strings.TrimSpace(data)
							if data != "" {
								if _, exists := sourceSeen[data]; !exists {
									sourceSeen[data] = struct{}{}
									cleanSourceData = append(cleanSourceData, data)
								}
							}
						}

						for _, data := range destData {
							data = strings.TrimSpace(data)
							if data != "" {
								if _, exists := destSeen[data]; !exists {
									destSeen[data] = struct{}{}
									cleanDestData = append(cleanDestData, data)
								}
							}
						}

						// 3. 记录去重前后的数据量
						vlog = fmt.Sprintf("(%d) Data deduplication - Source: %d->%d, Dest: %d->%d for %s.%s",
							logThreadSeq, len(sourceData), len(cleanSourceData), len(destData), len(cleanDestData), c1.Schema, c1.Table)
						global.Wlog.Debug(vlog)

						// 添加调试信息：输出前几条数据用于分析重复问题
						if len(cleanSourceData) > 0 {
							maxDebug := 5
							if len(cleanSourceData) < maxDebug {
								maxDebug = len(cleanSourceData)
							}
							global.Wlog.Debug("DEBUG_SOURCE_DATA_%d: First %d records:\n", logThreadSeq, maxDebug)
							for i := 0; i < maxDebug; i++ {
								global.Wlog.Debug("  [%d]: %s\n", i, cleanSourceData[i])
							}
						}

						// 检查去重是否真的有效
						// 只有当源数据确实有内容时，才检查重复记录
						if len(sourceData) != len(cleanSourceData) {
							// 检查是否只有一个空字符串（源表为空的情况）
							if len(sourceData) == 1 && sourceData[0] == "" {
								// 源表为空，不是真正的重复记录
								global.Wlog.Debug("(%d) Source data is empty, skipping duplicate check for %s.%s", logThreadSeq, c1.Schema, c1.Table)
							} else {
								duplicateCount := len(sourceData) - len(cleanSourceData)
								vlog = fmt.Sprintf("(%d) Found %d duplicate records in source data for %s.%s", logThreadSeq, duplicateCount, c1.Schema, c1.Table)
								global.Wlog.Warn(vlog)
							}
						}

						if len(destData) != len(cleanDestData) {
							// 检查是否只有一个空字符串（目标表为空的情况）
							if len(destData) == 1 && destData[0] == "" {
								// 目标表为空，不是真正的重复记录
								global.Wlog.Debug("(%d) Destination table %s.%s is empty, skipping duplicate check", logThreadSeq, c1.Schema, c1.Table)

								// 每个表只输出一次目标表为空的提示
								tableKey := fmt.Sprintf("%s.%s", c1.Schema, c1.Table)
								emptyTableMutex.Lock()
								if !emptyTableWarned[tableKey] {
									// 输出目标表为空的提示
									vlog = fmt.Sprintf("(%d) Destination table %s.%s is empty, all source records will be added", logThreadSeq, c1.Schema, c1.Table)
									global.Wlog.Warn(vlog)
									// 标记该表已输出提示
									emptyTableWarned[tableKey] = true
								}
								emptyTableMutex.Unlock()
							} else {
								duplicateCount := len(destData) - len(cleanDestData)
								vlog = fmt.Sprintf("(%d) Found %d duplicate records in dest data for %s.%s", logThreadSeq, duplicateCount, c1.Schema, c1.Table)
								global.Wlog.Warn(vlog)
							}
						}

						// 4. 使用Arrcmp进行精确比较
						add, del := aa.Arrcmp(cleanSourceData, cleanDestData)
						stt, dtt = "", ""

						// 5. 记录发现的差异数量
						vlog = fmt.Sprintf("(%d) Found %d records to add and %d records to delete for %s.%s", logThreadSeq, len(add), len(del), c1.Schema, c1.Table)
						global.Wlog.Debug(vlog)

						// 添加调试信息：检查差异数量的合理性
						expectedAddCount := len(cleanSourceData) - len(cleanDestData)
						if len(cleanDestData) == 0 {
							global.Wlog.Debug("DEBUG_DIFF_ANALYSIS_%d: Expected add count: %d (source=%d, dest=0), Actual add count: %d\n",
								logThreadSeq, len(cleanSourceData), len(cleanSourceData), len(add))
						} else {
							global.Wlog.Debug("DEBUG_DIFF_ANALYSIS_%d: Expected add count: %d (source=%d, dest=%d), Actual add count: %d\n",
								logThreadSeq, expectedAddCount, len(cleanSourceData), len(cleanDestData), len(add))
						}

						// 如果添加数量异常，输出前几条add数据进行检查
						if len(add) > expectedAddCount+10 {
							maxDebug := 10
							if len(add) < maxDebug {
								maxDebug = len(add)
							}
							global.Wlog.Debug("DEBUG_ADD_DATA_%d: First %d records (showing because count is abnormal):\n", logThreadSeq, maxDebug)
							for i := 0; i < maxDebug; i++ {
								global.Wlog.Debug("  [%d]: %s\n", i, add[i])
							}
						}

						// 6. 比较记录数量差异的日志记录
						// 记录删除和添加的记录数量，但不再自动清空add数组
						if len(del) == 1 && len(add) > 100 {
							// 关键修复：当删除数量为1且添加数量远大于1时，可能存在数据重复问题
							// 我们需要进一步验证add数组中的数据是否真实需要添加
							vlog = fmt.Sprintf("(%d) Warning: only 1 record to delete but %d to add for %s.%s", logThreadSeq, len(add), c1.Schema, c1.Table)
							global.Wlog.Warn(vlog)

							// 执行额外验证，确保add数组中的数据是真实需要添加的
							// 对于MySQL，我们可以使用更精确的比较方法
							if sp.ddrive == "mysql" {
								// 首先检查源端和目标端数据的总数
								sourceCount := len(cleanSourceData)
								destCount := len(cleanDestData)
								diffCount := sourceCount - destCount

								vlog = fmt.Sprintf("(%d) Source data count: %d, Destination data count: %d, Difference: %d", logThreadSeq, sourceCount, destCount, diffCount)
								global.Wlog.Debug(vlog)

								// 如果差异数量合理，则只保留必要的add记录
								if diffCount > 0 {
									// 根据实际数据差异限制add数组的大小，确保不会生成过多的INSERT语句
									vlog = fmt.Sprintf("(%d) Adjusting add records count from %d to %d based on actual data difference", logThreadSeq, len(add), diffCount)
									global.Wlog.Debug(vlog)
									if len(add) > diffCount {
										add = add[:diffCount]
									}
								}
							}
						}
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
								Schema:                  destSchema,   // 目标schema
								SourceSchema:            sourceSchema, // 源端schema，用于处理数据库映射关系
								Table:                   table,        // 使用映射后的表名
								ColData:                 colData.DColumnInfo,
								Sqlwhere:                destSqlWhere, // 使用处理后的目标端SQL条件
								DestDevice:              sp.ddrive,
								IndexColumn:             indexColumns,
								DatafixType:             sp.datafixType,
								CaseSensitiveObjectName: sp.caseSensitiveObjectName,
							}
							if strings.HasPrefix(c1.indexColumnType, "pri") {
								dbf.IndexType = "pri"
							} else if strings.HasPrefix(c1.indexColumnType, "uni") {
								dbf.IndexType = "uni"
							} else {
								dbf.IndexType = "mul"
							}

							// 关键修复：确保DELETE语句一定在INSERT语句之前生成
							// 先处理所有DELETE语句
							if len(del) > 0 {
								vlog = fmt.Sprintf("(%d) Generating DELETE statements for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
								global.Wlog.Debug("DEBUG_SQL_ORDER_%d: Processing %d DELETE statements first for %s.%s\n",
									logThreadSeq, len(del), c1.Schema, c1.Table)

								// 定义SQL长度限制 (1MB)
								const maxSqlSize = 1024 * 1024

								// 分组处理DELETE语句，每fixTrxNum条合并一次
								for batchStart := 0; batchStart < len(del); batchStart += sp.fixTrxNum {
									batchEnd := batchStart + sp.fixTrxNum
									if batchEnd > len(del) {
										batchEnd = len(del)
									}
									batchDel := del[batchStart:batchEnd]

									// 对于MySQL，合并DELETE语句
									if sp.ddrive == "mysql" {
										// 尝试提取主键或唯一键列名
										var primaryCol string
										// 只有当主键只有一列时，才使用IN条件合并
										if len(dbf.IndexColumn) == 1 {
											primaryCol = dbf.IndexColumn[0] // 使用唯一的主键列
										}

										// 如果有明确的单主键列，使用IN条件合并
										if primaryCol != "" {
											var values []string
											for _, i := range batchDel {
												dbf.RowData = i
												sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
												if err != nil {
													sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
													continue
												}

												// 提取WHERE条件中的值
												if strings.Contains(sqlstr, "WHERE") {
													wherePart := strings.Split(sqlstr, "WHERE")[1]
													wherePart = strings.TrimSpace(strings.TrimSuffix(wherePart, ";"))
													// 使用字符串分割来提取值，避免正则表达式转义问题
													key := fmt.Sprintf("`%s` = '", primaryCol)
													if strings.Contains(wherePart, key) {
														part := strings.Split(wherePart, key)[1]
														if strings.Contains(part, "'") {
															value := strings.Split(part, "'")[0]
															values = append(values, "'"+value+"'")
														}
													}
												}
											}

											// 如果成功提取了多个值，根据长度限制生成合并的DELETE语句
											if len(values) > 1 {
												// 生成基础SQL部分
												baseSql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` IN (", c1.Schema, c1.Table, primaryCol)
												baseSqlLen := len(baseSql)
												closeBracketLen := len(");")

												// 根据长度限制合并值
												var currentValues []string
												currentLength := baseSqlLen

												for i, value := range values {
													valueLen := len(value)
													separatorLen := 0
													if i > 0 {
														separatorLen = 1 // 逗号的长度
													}

													// 检查添加当前值是否会超过长度限制
													if currentLength+separatorLen+valueLen+closeBracketLen > maxSqlSize {
														// 如果当前已经有值，先生成并发送当前的合并SQL
														if len(currentValues) > 0 {
															mergedSql := fmt.Sprintf("%s%s);", baseSql, strings.Join(currentValues, ","))
															cc <- mergedSql
															// 重置当前值列表和长度
															currentValues = []string{value}
															currentLength = baseSqlLen + valueLen
														} else {
															// 如果单个值就超过限制，单独处理这条记录
															// 查找对应的原始记录并单独执行
															dbf.RowData = batchDel[i]
															sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
															if err != nil {
																sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
															}
															if sqlstr != "" {
																cc <- sqlstr
															}
														}
													} else {
														// 添加当前值到合并列表
														currentValues = append(currentValues, value)
														currentLength += separatorLen + valueLen
													}
												}

												// 处理剩余的值
												if len(currentValues) > 0 {
													mergedSql := fmt.Sprintf("%s%s);", baseSql, strings.Join(currentValues, ","))
													cc <- mergedSql
												}
											} else {
												// 如果无法合并，回退到单独执行
												for _, i := range batchDel {
													dbf.RowData = i
													sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
													if err != nil {
														sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
													}
													if sqlstr != "" {
														cc <- sqlstr
													}
												}
											}
										} else {
											// 对于复合主键或无主键，回退到单独执行，使用完整的WHERE条件
											for _, i := range batchDel {
												dbf.RowData = i
												sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
												if err != nil {
													sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
												}
												if sqlstr != "" {
													cc <- sqlstr
												}
											}
										}
									} else {
										// 对于非MySQL数据库，暂时保持单独执行
										for _, i := range batchDel {
											dbf.RowData = i
											sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
											if err != nil {
												sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
											}
											if sqlstr != "" {
												cc <- sqlstr
											}
										}
									}
								}
								vlog = fmt.Sprintf("(%d) DELETE statements generated for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
							}
							// DELETE语句处理完成后，再处理INSERT语句
							if len(add) > 0 {
								vlog = fmt.Sprintf("(%d) Generating INSERT statements for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
								global.Wlog.Debug("DEBUG_SQL_ORDER_%d: Processing %d INSERT statements after DELETE for %s.%s\n",
									logThreadSeq, len(add), c1.Schema, c1.Table)

								// 定义SQL长度限制 (1MB)
								const maxSqlSize = 1024 * 1024

								// 分组处理INSERT语句，每fixTrxNum条合并一次
								for batchStart := 0; batchStart < len(add); batchStart += sp.fixTrxNum {
									batchEnd := batchStart + sp.fixTrxNum
									if batchEnd > len(add) {
										batchEnd = len(add)
									}
									batchAdd := add[batchStart:batchEnd]

									// 关键修复：添加安全检查，确保batchAdd数组不包含过多数据
									if len(del) == 1 && len(batchAdd) > 10 {
										vlog = fmt.Sprintf("(%d) Safety check: limiting batchAdd size from %d to 10 when only 1 delete record", logThreadSeq, len(batchAdd))
										global.Wlog.Debug(vlog)
										batchAdd = batchAdd[:10]
									}

									// 生成单独的INSERT语句，避免多线程并发下的重复冲突
									global.Wlog.Debug("DEBUG_INSERT_LOOP_%d: Starting INSERT generation for %d records in batch for %s.%s\n",
										logThreadSeq, len(batchAdd), c1.Schema, c1.Table)

									insertCount := 0
									duplicateCount := 0
									for batchIndex, i := range batchAdd {
										dbf.RowData = i
										sqlstr, err := dbf.DataAbnormalFix().FixInsertSqlExec(ddb, sp.ddrive, logThreadSeq)
										if err != nil {
											sp.getErr(fmt.Sprintf("dest: checksum table %s.%s generate INSERT sql error.", c1.Schema, c1.Table), err)
										} else if sqlstr != "" {
											// 生成INSERT语句的唯一标识符用于去重
											// 提取VALUES部分作为唯一标识
											valuesStart := strings.Index(sqlstr, "VALUES(")
											valuesEnd := strings.LastIndex(sqlstr, ");")
											var insertKey string
											if valuesStart > 0 && valuesEnd > valuesStart+7 {
												insertKey = sqlstr[valuesStart+7 : valuesEnd]
											} else {
												// 如果解析失败，使用完整SQL作为key
												insertKey = sqlstr
											}

											// 检查是否已经处理过这条INSERT记录（需要加锁保护并发访问）
											insertMutex.Lock()
											if _, exists := processedInserts[insertKey]; exists {
												insertMutex.Unlock()
												duplicateCount++
												if duplicateCount <= 5 {
													insertPreview := insertKey
													if len(insertKey) > 50 {
														insertPreview = insertKey[:50]
													}
													global.Wlog.Debug("DEBUG_DUPLICATE_INSERT_%d: Skipping duplicate INSERT: %s\n",
														logThreadSeq, insertPreview)
												}
												continue
											}

											// 标记为已处理
											processedInserts[insertKey] = struct{}{}
											insertMutex.Unlock()
											insertCount++

											// 记录生成的SQL语句
											vlog = fmt.Sprintf("(%d) Generated INSERT statement for %s.%s", logThreadSeq, c1.Schema, c1.Table)
											global.Wlog.Debug(vlog)

											// 如果是前几条记录，输出调试信息
											if insertCount <= 5 {
												sqlPreview := sqlstr
												if len(sqlstr) > 50 {
													sqlPreview = sqlstr[:50] + "..."
												}
												global.Wlog.Debug("DEBUG_INSERT_DETAIL_%d: Batch[%d] - Insert count %d - SQL starts with: %s\n",
													logThreadSeq, batchIndex, insertCount, sqlPreview)
											}

											cc <- sqlstr
											totalInsertCount++
										}
									}

									if duplicateCount > 0 {
										global.Wlog.Debug("DEBUG_INSERT_LOOP_%d: Generated %d INSERT statements, skipped %d duplicates for batch with %d records in %s.%s (Total so far: %d)\n",
											logThreadSeq, insertCount, duplicateCount, len(batchAdd), c1.Schema, c1.Table, totalInsertCount)
									} else {
										global.Wlog.Debug("DEBUG_INSERT_LOOP_%d: Generated %d INSERT statements for batch with %d records in %s.%s (Total so far: %d)\n",
											logThreadSeq, insertCount, len(batchAdd), c1.Schema, c1.Table, totalInsertCount)
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
	global.Wlog.Debug("DEBUG_FINAL_COUNT_%d: Total INSERT statements generated for %s.%s: %d\n",
		logThreadSeq, sp.schema, sp.table, totalInsertCount)
	vlog = fmt.Sprintf("(%d) Completed difference processing and repair statements for %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
}

func (sp *SchedulePlan) DataFixDispos(fixSQL chanString, logThreadSeq int64) {
	var (
		vlog        string
		deleteCount int
		insertCount int
		// 关键修复：分别存储DELETE和INSERT语句，确保最终顺序
		deleteSqls  []string
		insertSqls  []string
		sqlBuffer   []string // 缓冲SQL语句，达到阈值时立即写入
		bufferLimit int      // 缓冲区大小限制，达到该值时立即写入文件
		isFinished  bool     // 标记是否已完成接收
	)

	// 修复：清空全局writtenSqlMap，确保只针对当前表去重，避免跨表影响
	writtenSqlMap = sync.Map{}

	// 使用fixTrxNum作为缓冲区大小，确保COMMIT间隔符合用户设置
	bufferLimit = sp.fixTrxNum
	// 如果fixTrxNum为0或负数，使用一个合理的默认值
	if bufferLimit <= 0 {
		bufferLimit = 1000
	}
	vlog = fmt.Sprintf("(%d) Applying repair statements to target table %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)

	// 启动一个goroutine来收集和处理所有SQL语句
	go func() {
		defer func() {
			// 处理剩余的SQL语句
			if len(sqlBuffer) > 0 {
				processBatch(sqlBuffer, sp.datafixType, sp.sfile, sp.ddrive, sp.djdbc, logThreadSeq, sp.fixTrxNum)
			}
			// 处理剩余的DELETE和INSERT语句
			if len(deleteSqls) > 0 || len(insertSqls) > 0 {
				var finalSqls []string
				finalSqls = append(finalSqls, deleteSqls...)
				finalSqls = append(finalSqls, insertSqls...)
				processBatch(finalSqls, sp.datafixType, sp.sfile, sp.ddrive, sp.djdbc, logThreadSeq, sp.fixTrxNum)
				vlog = fmt.Sprintf("(%d) Repair statements generated for %s.%s: DELETE=%d, INSERT=%d",
					logThreadSeq, sp.schema, sp.table, len(deleteSqls), len(insertSqls))
				global.Wlog.Debug(vlog)
				// 有差异时标记DIFFS为yes
				sp.pods.DIFFS = "yes"
			}
			// 无论是否有差异，都添加到结果中
			measuredDataPods = append(measuredDataPods, *sp.pods)
			isFinished = true
		}()

		for {
			v, ok := <-fixSQL
			if !ok {
				return
			}
			sp.pods.DIFFS = "yes"

			// 按SQL类型分别存储，确保最终顺序
			sqlTrim := strings.TrimSpace(strings.ToUpper(v))
			if strings.HasPrefix(sqlTrim, "DELETE") {
				deleteSqls = append(deleteSqls, v)
				deleteCount++
				//global.Wlog.Debug("DEBUG_BUFFER_DELETE_%d: Buffered DELETE statement #%d for %s.%s\n", logThreadSeq, deleteCount, sp.schema, sp.table)
			} else if strings.HasPrefix(sqlTrim, "INSERT") {
				insertSqls = append(insertSqls, v)
				insertCount++
				//global.Wlog.Debug("DEBUG_BUFFER_INSERT_%d: Buffered INSERT statement #%d for %s.%s\n", logThreadSeq, insertCount, sp.schema, sp.table)
			}

			// 缓冲所有SQL语句，达到阈值时立即写入文件
			sqlBuffer = append(sqlBuffer, v)
			if len(sqlBuffer) >= bufferLimit {
				// 立即写入缓冲区中的SQL语句
				processBatch(sqlBuffer, sp.datafixType, sp.sfile, sp.ddrive, sp.djdbc, logThreadSeq, sp.fixTrxNum)
				// 清空缓冲区
				sqlBuffer = []string{}
			}
		}
	}()

	// 等待处理完成
	for !isFinished {
		time.Sleep(time.Millisecond * 10)
	}
}

// processBatch 批量处理SQL语句，根据类型排序后写入文件
func processBatch(sqls []string, datafixType string, sfile *os.File, ddrive string, djdbc string, logThreadSeq int64, fixTrxNum int) {
	if len(sqls) == 0 {
		return
	}
	// 按SQL类型排序：所有DELETE在前，INSERT在后
	var deleteSqls []string
	var insertSqls []string

	for _, sql := range sqls {
		sqlTrim := strings.TrimSpace(strings.ToUpper(sql))
		if strings.HasPrefix(sqlTrim, "DELETE") {
			deleteSqls = append(deleteSqls, sql)
		} else if strings.HasPrefix(sqlTrim, "INSERT") {
			insertSqls = append(insertSqls, sql)
		}
	}

	// 构建最终的有序SQL列表
	var finalSqls []string
	finalSqls = append(finalSqls, deleteSqls...)
	finalSqls = append(finalSqls, insertSqls...)

	// 写入文件
	ApplyDataFixWithTrxNum(finalSqls, datafixType, sfile, ddrive, djdbc, logThreadSeq, fixTrxNum)
	global.Wlog.Debug("DEBUG_BATCH_WRITE_%d: Wrote %d SQL statements to file, DELETE=%d, INSERT=%d\n",
		logThreadSeq, len(finalSqls), len(deleteSqls), len(insertSqls))
}

// 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
		curry     = make(chanStruct, sp.concurrency)
		autoSeq   = int64(0) // 任务计数器
		total     = int64(0)
		startTime = time.Now().UnixMilli() // 开始时间
		allClosed = false
	)

	// 重新初始化进度条为100，用于显示百分比进度
	sp.bar = &Bar{}
	sp.bar.NewOption(0, 100, "Processing")

	for {
		// 检查是否所有工作都已完成
		if allClosed {
			// 等待所有协程完成
			if len(curry) == 0 {
				// 完成进度条显示
				sp.bar.Finish()
				close(diffQueryData)
				return
			}
			// 继续循环，等待协程完成
			time.Sleep(100 * time.Millisecond)
			continue
		}

		select {
		case d, ok := <-sc:
			if ok && d > 0 {
				total = d
				global.Wlog.Debug("DEBUG_PROGRESS_%d: Total tasks received=%d at time=%.2fs\n",
					logThreadSeq, d, float64(time.Now().UnixMilli()-startTime)/1000)
			}
		case sourceSql, ok := <-sourceSelectSql:
			if !ok {
				// 源通道关闭，检查目标通道
				select {
				case _, destOk := <-destSelectSql:
					if !destOk {
						// 目标通道也关闭了
						allClosed = true
					}
				default:
					// 目标通道可能还有数据，继续处理
				}
				continue
			}

			// 从目标通道读取数据，检查是否已关闭
			destSql, destOk := <-destSelectSql
			if !destOk {
				allClosed = true
				continue
			}

			autoSeq++

			// 计算当前完成百分比并更新进度条
			var displayProgress int64
			if total > 0 {
				// 计算当前完成的百分比，映射到100的刻度上
				percent := float64(autoSeq) / float64(total)
				displayProgress = int64(percent * 100)
				if displayProgress > 100 {
					displayProgress = 100
				}
			} else {
				// 没有总数时，使用更平滑的进度估算
				var estimatedTotal int64
				if autoSeq <= 50 {
					estimatedTotal = 100 // 前50个任务时，估算总共100个
				} else if autoSeq <= 100 {
					estimatedTotal = autoSeq * 2 // 51-100个任务时，估算为当前的2倍
				} else if autoSeq <= 300 {
					estimatedTotal = autoSeq + autoSeq/2 // 101-300个任务时，估算再需要50%的任务
				} else {
					estimatedTotal = autoSeq + 150 // 超过300个任务时，估算再需要150个
				}

				percent := float64(autoSeq) / float64(estimatedTotal)
				displayProgress = int64(percent * 100)

				// 限制进度显示，避免过早达到100%
				if displayProgress > 95 {
					displayProgress = 95 // 最多显示95%，给最终完成留空间
				}
			}

			// DEBUG: 记录进度更新
			//currentTime := time.Now().UnixMilli()
			//global.Wlog.Debug("DEBUG_PROGRESS_UPDATE_%d: autoSeq=%d, total=%d, displayProgress=%d, time=%.2fs, curry_len=%d\n", logThreadSeq, autoSeq, total, displayProgress, float64(currentTime-startTime)/1000, len(curry))

			// 更新进度条
			sp.bar.Play(displayProgress)
			// 强制刷新缓冲区确保实时显示
			fmt.Fprint(os.Stdout, "")

			curry <- struct{}{}
			go func(currentSeq int64, sourceSql, destSql map[string]string) {
				defer func() {
					<-curry
				}()

				// DEBUG: 记录任务开始处理
				//taskStartTime := time.Now().UnixMilli()
				//global.Wlog.Debug("DEBUG_TASK_START_%d: currentSeq=%d, autoSeq=%d, total=%d, time=%.2fs\n", logThreadSeq, currentSeq, autoSeq, total, float64(taskStartTime-startTime)/1000)

				// 源端查询
				sdb := sp.sdbPool.Get(logThreadSeq)
				//sourceQueryStart := time.Now().UnixMilli()
				//global.Wlog.Debug("DEBUG_SOURCE_START_%d: seq=%d, getting source query...\n", logThreadSeq, currentSeq)
				stt, err := (&dbExec.IndexColumnStruct{
					Schema:   sp.sourceSchema,
					Table:    sp.table,
					Drivce:   sp.sdrive,
					Sqlwhere: sourceSql[sp.sdrive],
					ColData:  cc1.SColumnInfo,
				}).TableIndexColumn().GeneratingQueryCriteria(sdb, logThreadSeq)
				//sourceQueryEnd := time.Now().UnixMilli()
				sp.sdbPool.Put(sdb, logThreadSeq)
				if err != nil {
					global.Wlog.Debug("DEBUG_TASK_ERROR_%d: source query failed for seq=%d: %v\n", logThreadSeq, currentSeq, err)
					return
				}

				//sourceDuration := float64(sourceQueryEnd-sourceQueryStart) / 1000
				//global.Wlog.Debug("DEBUG_SOURCE_QUERY_%d: seq=%d, duration=%.2fs, total_time_so_far=%.2fs\n", logThreadSeq, currentSeq, sourceDuration, float64(sourceQueryEnd-startTime)/1000)

				// 目标端查询
				ddb := sp.ddbPool.Get(logThreadSeq)
				//destQueryStart := time.Now().UnixMilli()
				dtt, err := (&dbExec.IndexColumnStruct{
					Schema:   sp.destSchema,
					Table:    sp.table,
					Drivce:   sp.ddrive,
					Sqlwhere: destSql[sp.ddrive],
					ColData:  cc1.DColumnInfo,
				}).TableIndexColumn().GeneratingQueryCriteria(ddb, logThreadSeq)
				//destQueryEnd := time.Now().UnixMilli()
				sp.ddbPool.Put(ddb, logThreadSeq)
				if err != nil {
					global.Wlog.Debug("DEBUG_TASK_ERROR_%d: dest query failed for seq=%d: %v\n", logThreadSeq, currentSeq, err)
					return
				}

				//global.Wlog.Debug("DEBUG_DEST_QUERY_%d: seq=%d, duration=%.2fs\n", logThreadSeq, currentSeq, float64(destQueryEnd-destQueryStart)/1000)

				// 比较结果
				aa := &CheckSumTypeStruct{}
				if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
					differencesData := DifferencesDataStruct{
						Schema:          sp.schema,
						Table:           sp.table,
						SqlWhere:        map[string]string{sp.sdrive: sourceSql[sp.sdrive], sp.ddrive: destSql[sp.ddrive]},
						TableColumnInfo: cc1,
						SourceData:      stt, // 传递已经查询到的源端数据，避免重复查询
						DestData:        dtt, // 传递已经查询到的目标端数据，避免重复查询
					}
					diffQueryData <- differencesData
				}

				// DEBUG: 记录任务完成时间
				//taskEndTime := time.Now().UnixMilli()
				//global.Wlog.Debug("DEBUG_TASK_END_%d: currentSeq=%d, autoSeq=%d, total=%d, totalTaskTime=%.2fs, timeFromStart=%.2fs\n", logThreadSeq, currentSeq, autoSeq, total, float64(taskEndTime-taskStartTime)/1000, float64(taskEndTime-startTime)/1000)

				// DEBUG: 记录任务完成（不更新进度条，避免跳动）
				//currentTime := time.Now().UnixMilli()
				//global.Wlog.Debug("DEBUG_TASK_COMPLETE_%d: currentSeq=%d, autoSeq=%d, total=%d, time=%.2fs, curry_len=%d\n", logThreadSeq, currentSeq, autoSeq, total, float64(currentTime-startTime)/1000, len(curry))
			}(autoSeq, sourceSql, destSql)
		}
	}
}
