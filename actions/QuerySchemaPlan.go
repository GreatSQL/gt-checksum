package actions

import (
	"fmt"
	"greatdbCheck/dbExec"
	"greatdbCheck/global"
	"greatdbCheck/inputArg"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	wg                 sync.WaitGroup
	lock               sync.Mutex
	breakIndexColumnMq = false
)

/*
   差异数据信息结构体
*/
type DifferencesDataStruct struct {
	Schema string //存在差异数据的库
	Table  string //存在差异数据的表
	Spoint string //校验开始时的源端全局一致性点
	Dpoint string //校验开始时的目端全局一致性点
	//TableColumnInfo []map[string]string //该表的所有列信息，包括列类型
	TableColumnInfo map[string]global.TableAllColumnInfoS //该表的所有列信息，包括列类型
	SqlWhere        map[string]string                     //差异数据查询的where 条件
	indexColumnType string                                //索引列类型
}

/*
   初始化差异数据信息结构体
*/
func InitDifferencesDataStruct() DifferencesDataStruct {
	return DifferencesDataStruct{}
}

var querySqlWhereSliceMap = make([]map[string]string, 0)

type SchedulePlan struct {
	Concurrency, singleIndexChanRowCount, jointIndexChanRowCount, mqQueueDepth int
	schema, table                                                              string   //待校验库名、表名
	columnName                                                                 []string //待校验表的列名，有可能是多个
	tmpTableDataFileDir                                                        string   //临时表文件生成的相对路径
	tableIndexColumnMap                                                        map[string][]string
	sdbConnPool, ddbConnPool                                                   *global.Pool
	datafixType                                                                string
	datafixSql                                                                 string
	sdrive, ddrive                                                             string
	checkMod                                                                   string
	checkNoIndexTable                                                          string //是否检查无索引表
	tableAllCol                                                                map[string]global.TableAllColumnInfoS
	ratio                                                                      int
}

type writeTmpTableDataStruct struct {
	columnName                                             []string //待校验表的列名，有可能是多个
	chanrowCount, concurrency, queueDepth                  int      //单次并发一次校验的行数
	TmpTablePath                                           string
	sdbPool                                                *global.Pool
	ddbPool                                                *global.Pool
	smtype                                                 string //是源端还是目标端
	sdrive, ddrive, datafixType, datafixSql, table, schema string
	indexColumnType                                        string
	checkMod                                               string
	tableIndexColumnMap                                    map[string][]string
	tableAllCol                                            map[string]global.TableAllColumnInfoS
	ratio                                                  int
}

func (wttds *writeTmpTableDataStruct) getErr(msg string, err error) {
	if err != nil {
		fmt.Println(err, ":", msg)
		os.Exit(1)
	}
}

// DoBatch 开启指定协程数批量执行
func (wttds *writeTmpTableDataStruct) DoBatch(ma chan string, out1 []map[string]string, goroutineNum int, selectColumnString, lengthTrim string, columnLengthAs []string, execFun func(out []map[string]string, threadId int, selectColumnString, lengthTrim, limit string, columnLengthAs []string)) {
	var (
		workLimiter = make(chan struct{}, goroutineNum)
	)
	var aa int = 0
	for i := range ma {
		global.Wlog.Info("[<- limitChan] select limitChan data is ", i)

		select {
		case workLimiter <- struct{}{}:
			wg.Add(1)
			aa++
			global.Wlog.Info("[<- limitChan] (", aa, ")  select limitChan data is ", i)
			go func(out []map[string]string, cc int, selectColumnString, lengthTrim, limit string, columnLengthAs []string) {
				defer func(bb int) {
					<-workLimiter
					defer wg.Done()
					global.Wlog.Info("[<- limitChan] (", bb, ") thread id exec complete.")
					return
				}(cc)
				execFun(out, aa, selectColumnString, lengthTrim, limit, columnLengthAs)
			}(out1, aa, selectColumnString, lengthTrim, i, columnLengthAs)
		}
	}
	wg.Wait()
}

/*
	计算源目标段表的最大行数
*/
func (wttds writeTmpTableDataStruct) LimiterSeq(limitPag chan int64, limitPagDone chan bool) { //定义变量
	var (
		schema                           = wttds.schema
		table                            = wttds.table
		columnName                       = wttds.columnName
		chanrowCount                     = wttds.chanrowCount
		maxTableCount, schedulePlanCount int
	)
	sdb := wttds.sdbPool.Get()
	ddb := wttds.ddbPool.Get()
	//查询原目标端的表总行数，并生成调度计划
	idxc := dbExec.IndexColumnStruct{Schema: wttds.schema, Table: wttds.table, ColumnName: wttds.columnName, ChanrowCount: wttds.chanrowCount, Drivce: wttds.sdrive}
	stmpTableCount, err := idxc.TableIndexColumn().TmpTableRowsCount(sdb)
	if err != nil {
		wttds.getErr(fmt.Sprintf("source: query table %s.%s rows total error.", schema, table), err)
	}
	idxc.Drivce = wttds.ddrive
	dtmpTableCount, err := idxc.TableIndexColumn().TmpTableRowsCount(ddb)
	if err != nil {
		wttds.getErr(fmt.Sprintf("dest: query table %s.%s rows total error.", schema, table), err)
	}
	wttds.sdbPool.Put(sdb)
	wttds.ddbPool.Put(ddb)
	if stmpTableCount > dtmpTableCount || stmpTableCount == dtmpTableCount {
		maxTableCount = stmpTableCount
	} else {
		maxTableCount = dtmpTableCount
	}
	//输出校验结果信息
	pods := Pod{
		Schema:      schema,
		Table:       table,
		IndexCol:    strings.TrimLeft(strings.Join(columnName, ","), ","),
		CheckMod:    wttds.checkMod,
		Differences: "no",
		Datafix:     wttds.datafixType,
	}
	if wttds.checkMod == "sample" {
		if stmpTableCount != dtmpTableCount {
			pods.Rows = fmt.Sprintf("%d|%d", stmpTableCount, dtmpTableCount)
			measuredDataPods = append(measuredDataPods, pods)
		} else {
			var newMaxTableCount int //抽样比例后的总数值
			newMaxTableCount = maxTableCount
			if maxTableCount > chanrowCount {
				newMaxTableCount = maxTableCount * wttds.ratio / 100
				chanrowCount = chanrowCount / wttds.concurrency
			}
			if newMaxTableCount%chanrowCount != 0 {
				schedulePlanCount = newMaxTableCount/chanrowCount + 1
			} else {
				schedulePlanCount = newMaxTableCount / chanrowCount
			}
			var beginSeq int64
			nanotime := int64(time.Now().Nanosecond())
			rand.Seed(nanotime)
			for i := 0; i < schedulePlanCount; i++ {
				if newMaxTableCount > chanrowCount {
					beginSeq = rand.Int63n(int64(maxTableCount))
				}
				limitPag <- beginSeq
				beginSeq = beginSeq + int64(chanrowCount)
			}
			pods.Rows = fmt.Sprintf("%d,%d", maxTableCount, newMaxTableCount)
			measuredDataPods = append(measuredDataPods, pods)
		}
	} else {
		if maxTableCount%chanrowCount != 0 {
			schedulePlanCount = maxTableCount/chanrowCount + 1
		} else {
			schedulePlanCount = maxTableCount / chanrowCount
		}
		var beginSeq int64 = 0
		for i := 0; i < schedulePlanCount; i++ {
			limitPag <- beginSeq
			beginSeq = beginSeq + int64(chanrowCount)
		}
		pods.Rows = fmt.Sprintf("%d,%d", maxTableCount, maxTableCount)
		measuredDataPods = append(measuredDataPods, pods)
	}
	limitPagDone <- true
	close(limitPag)
}

func (wttds writeTmpTableDataStruct) indexColUniq(il, pag int64, selectColumnString string, lengthTrim string, columnLengthAs []string) []string {
	threadId := 1
	sdb := wttds.sdbPool.Get()
	ddb := wttds.ddbPool.Get()
	tmpmap := make(map[string]string)
	tmpsl := []string{}
	global.Wlog.Info("[check table ", wttds.schema, ".", wttds.table, " index column] index column select where limit seq: ", fmt.Sprintf("%d,%d", il, pag))
	global.Wlog.Info("[check table ", wttds.schema, ".", wttds.table, " index column] source DB index column query limit seq [", fmt.Sprintf("%d,%d", il, pag), "] index data")
	idxc := dbExec.IndexColumnStruct{Schema: wttds.schema, Table: wttds.table, ColumnName: wttds.columnName, ChanrowCount: wttds.chanrowCount, Drivce: wttds.sdrive}
	sourceTmpTableData, err := idxc.TableIndexColumn().TmpTableIndexColumnDataDispos(sdb, threadId, selectColumnString, lengthTrim, columnLengthAs, wttds.columnName, il, pag)
	if err != nil {
		wttds.getErr(fmt.Sprintf("source: query table %s.%s index column data error.", wttds.schema, wttds.table), err)
	}
	global.Wlog.Info("[check table ", wttds.schema, ".", wttds.table, " index column] dest DB index column query limit seq [", fmt.Sprintf("%d,%d", il, pag), "] index data")
	idxc.Drivce = wttds.ddrive
	destTmpTableData, err := idxc.TableIndexColumn().TmpTableIndexColumnDataDispos(ddb, threadId, selectColumnString, lengthTrim, columnLengthAs, wttds.columnName, il, pag)
	if err != nil {
		wttds.getErr(fmt.Sprintf("dest: query table %s.%s index column data error.", wttds.schema, wttds.table), err)
	}
	wttds.sdbPool.Put(sdb)
	wttds.ddbPool.Put(ddb)
	for _, ii := range sourceTmpTableData {
		lock.Lock()
		tmpmap[ii] = ""
		lock.Unlock()
	}
	for _, ii := range destTmpTableData {
		lock.Lock()
		if _, ok := tmpmap[ii]; !ok {
			tmpmap[ii] = ""
		}
		lock.Unlock()
	}
	for k, _ := range tmpmap {
		tmpsl = append(tmpsl, k)
	}
	return tmpsl
}

/*
   该函数用于分批分段读取
   并发读取原、目标端 单表的索引列数据，根据表的数量按照单次校验的数据块行数进行切割，
   并发写入管道中，同时会有监听管道进行读取数据
*/
func (wttds writeTmpTableDataStruct) indexColUniqProduct(ma <-chan int64, indexColData chan<- []string, done <-chan bool, differDone chan<- bool, selectColumnString string, lengthTrim string, columnLengthAs []string, goroutineNum int) { //定义变量
	var (
		workLimiter = make(chan struct{}, goroutineNum)
		breakDone   = make(chan bool, 1)
		execStatus  = make(chan bool, 1)
		breakStatus = false
	)
	t := time.NewTicker(time.Millisecond)
	for time := range t.C {
		select {
		case d := <-done:
			if d {
				breakStatus = true
			}
		case <-breakDone:
			global.Wlog.Info("[check table ", wttds.schema, ".", wttds.table, " index column]", "table Index Column Data done!")
			fmt.Println("table Index Column Data done!", time.Format("2006-01-02 15:04:11"))
			close(execStatus)
			close(indexColData)
			differDone <- true
			return
		case status := <-execStatus:
			var aa = false
			if status {
				for i := 0; i < 3; i++ {
					if breakStatus && len(ma) == 0 && len(workLimiter) == 0 {
						aa = true
					} else {
						aa = false
					}
				}
				if aa {
					breakDone <- true
					close(breakDone)
				}
			}
			if len(ma) > 0 {
				for i := range ma {
					select {
					case workLimiter <- struct{}{}:
						//wg.Add(1)
						go func(i, g int64) {
							defer func() {
								<-workLimiter
								//defer wg.Done()
							}()
							indexColData <- wttds.indexColUniq(i, g, selectColumnString, lengthTrim, columnLengthAs)
						}(i, int64(wttds.chanrowCount))
					}
				}
				//wg.Wait()
			}
		default:
			if breakStatus {
				execStatus <- true
			} else {
				execStatus <- false
			}
		}
	}

	//threadId := 1
	//for i := range limitPag {
	//	select {
	//	case workLimiter <- struct{}{}:
	//		wg.Add(1)
	//		go func(il, pag int) {
	//			defer func() {
	//				<-workLimiter
	//				wg.Done()
	//			}()
	//			sdb = wttds.sdbPool.Get()
	//			ddb = wttds.ddbPool.Get()
	//			tmpmap := make(map[string]string)
	//			tmpsl := []string{}
	//			global.Wlog.Info("[check table ", wttds.schema, ".", wttds.table, " index column] index column select where limit seq: ", fmt.Sprintf("%d,%d", il, pag))
	//			global.Wlog.Info("[check table ", wttds.schema, ".", wttds.table, " index column] source DB index column query limit seq [", fmt.Sprintf("%d,%d", il, pag), "] index data")
	//			var sourceTmpTableData, destTmpTableData []string
	//			idxc.Drivce = wttds.sdrive
	//			sourceTmpTableData, err = idxc.TableIndexColumn().TmpTableIndexColumnDataDispos(sdb, threadId, selectColumnString, lengthTrim, columnLengthAs, wttds.columnName, il, pag)
	//			if err != nil {
	//				wttds.getErr(fmt.Sprintf("source: query table %s.%s index column data error.", schema, table), err)
	//			}
	//			global.Wlog.Info("[check table ", wttds.schema, ".", wttds.table, " index column] dest DB index column query limit seq [", fmt.Sprintf("%d,%d", il, pag), "] index data")
	//			idxc.Drivce = wttds.ddrive
	//			destTmpTableData, err = idxc.TableIndexColumn().TmpTableIndexColumnDataDispos(ddb, threadId, selectColumnString, lengthTrim, columnLengthAs, wttds.columnName, il, pag)
	//			if err != nil {
	//				wttds.getErr(fmt.Sprintf("dest: query table %s.%s index column data error.", schema, table), err)
	//			}
	//			wttds.sdbPool.Put(sdb)
	//			wttds.ddbPool.Put(ddb)
	//			for _, ii := range sourceTmpTableData {
	//				lock.Lock()
	//				tmpmap[ii] = ""
	//				lock.Unlock()
	//			}
	//			for _, ii := range destTmpTableData {
	//				lock.Lock()
	//				if _, ok := tmpmap[ii]; !ok {
	//					tmpmap[ii] = ""
	//				}
	//				lock.Unlock()
	//			}
	//			for k, _ := range tmpmap {
	//				tmpsl = append(tmpsl, k)
	//			}
	//			if len(tmpsl) > 0 {
	//				indexColData <- tmpsl
	//			}
	//		}(i, chanrowCount)
	//	}
	//}
	//IndexColumnDone <- true
	//global.Wlog.Info("[check table ", wttds.schema, ".", wttds.table, " index column]", "table Index Column Data done!")
	//fmt.Println("table Index Column Data done!", time.Now().Format("2006-01-02 15:04:11"))
	//close(indexColData)

}

/*
	根据表的索引列数据生成select where 条件
*/
func (wttds writeTmpTableDataStruct) TableSelectWhere(columnName, indexColumnData []string) map[string]string {
	var (
		chankey, nullString string
		chanData            = make(map[string]string)
		schema              = wttds.schema
		table               = wttds.table
	)
	//索引列为单列
	if len(columnName) == 1 {
		ro := rand.New(rand.NewSource(time.Now().UnixNano()))
		roo := ro.Int63n(1024000)
		chankey = fmt.Sprintf("task%d/*actionSchema*/%s/*actionTable*/%s", roo, schema, table)
		tmpDelString := strings.Join(indexColumnData, "','")
		tmpColumnname := strings.Join(columnName, "")
		if strings.Contains(tmpDelString, "greatdbCheckNULL") && strings.Contains(tmpDelString, "greatdbCheckEmtry") {
			nullString = fmt.Sprintf("/* %s */ %s  in ('%s') or %s is NULL or %s = ''", "actions", tmpColumnname, tmpDelString, tmpColumnname, tmpColumnname)
		} else if strings.Contains(tmpDelString, "greatdbCheckNULL") {
			nullString = fmt.Sprintf("/* %s */ %s  in ('%s') or %s is NULL", "actions", tmpColumnname, tmpDelString, tmpColumnname)
		} else if strings.Contains(tmpDelString, "greatdbCheckEmtry") {
			nullString = fmt.Sprintf("/* %s */ %s  in ('%s') or %s =''", "actions", tmpColumnname, tmpDelString, tmpColumnname)
		} else {
			nullString = fmt.Sprintf("/* %s */ %s  in ('%s')", "actions", tmpColumnname, tmpDelString)
		}
		chanData[chankey] = nullString
		global.Wlog.Debug("[query table ", wttds.schema, ".", wttds.table, " sql where] ", "Single index sql info: ", nullString)
	}
	//多列生成where条件，做单行校验
	if len(columnName) > 1 {
		var duoColumnSlisp []string
		for ri, r := range indexColumnData {
			ro := rand.New(rand.NewSource(time.Now().UnixNano()))
			roo := ro.Int63n(10240000)
			chankey = fmt.Sprintf("task%d%d/*actionSchema*/%s/*actionTable*/%s", ri, roo, schema, table)
			var tmpColumnWhereSlice []string
			var columnWhere string
			for c := range columnName {
				if len(r) == 0 || !strings.Contains(r, "/*,*/") || len(strings.Split(r, "/*,*/")) <= c || len(columnName) <= c {
					continue
				}
				tmpColumnVal := strings.Split(r, "/*,*/")[c]
				tmpColumnname := columnName[c]
				if tmpColumnVal == "greatdbCheckNULL" {
					columnWhere = fmt.Sprintf(" %s is NULL ", tmpColumnname)
				} else if tmpColumnVal == "greatdbCheckEmtry" {
					columnWhere = fmt.Sprintf(" %s = '' ", tmpColumnname)
				} else {
					columnWhere = fmt.Sprintf(" %s = '%s' ", tmpColumnname, tmpColumnVal)
				}
				tmpColumnWhereSlice = append(tmpColumnWhereSlice, columnWhere)
			}
			nullString = fmt.Sprintf(" %s", strings.Join(tmpColumnWhereSlice, "and"))
			duoColumnSlisp = append(duoColumnSlisp, nullString)
		}
		nullSt := fmt.Sprintf("( %s );", strings.Join(duoColumnSlisp, ") or ("))
		chanData[chankey] = nullSt
		global.Wlog.Debug("[query table ", wttds.schema, ".", wttds.table, " sql where] ", "Conform to the index sql info: ", nullSt)
		duoColumnSlisp = []string{}
	}
	return chanData
}

/*
	差异数据的二次校验，并生成修复语句
*/
func (wttds writeTmpTableDataStruct) AbnormalDataDispos(schema, table string, sqlwhere map[string]string, tableColInfo map[string]global.TableAllColumnInfoS, indexColumnType string) []string {
	var (
		aa           = &CheckSumTypeStruct{}
		sqlstr       string
		strsqlSliect []string
	)
	sdb := wttds.sdbPool.Get()
	ddb := wttds.ddbPool.Get()
	colData := wttds.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", wttds.schema, wttds.table)]
	idxc := dbExec.IndexColumnStruct{
		Schema:      wttds.schema,
		Table:       wttds.table,
		TableColumn: colData.SColumnInfo,
		Sqlwhere:    sqlwhere[wttds.sdrive],
		Drivce:      wttds.sdrive,
	}
	stt, err := idxc.TableIndexColumn().GeneratingQueryCriteria(sdb)
	if err != nil {
		wttds.getErr(fmt.Sprintf("source: query table %s.%s row data chan error.", wttds.schema, wttds.table), err)
	}
	idxc.Drivce = wttds.ddrive
	idxc.Sqlwhere = sqlwhere[wttds.ddrive]
	idxc.TableColumn = colData.DColumnInfo
	dtt, err := idxc.TableIndexColumn().GeneratingQueryCriteria(ddb)
	if err != nil {
		wttds.getErr(fmt.Sprintf("dest: query table %s.%s row data chan error.", wttds.schema, wttds.table), err)
	}
	if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
		add, del := aa.Arrcmp(strings.Split(stt, "/*go actions rowData*/"), strings.Split(dtt, "/*go actions rowData*/"))
		if len(del) > 0 || len(add) > 0 {
			lock.Lock()
			differencesSchemaTable[fmt.Sprintf("%sgreatdbCheck_greatdbCheck%s", schema, table)] = wttds.datafixType
			lock.Unlock()
			if len(del) > 0 {
				for _, i := range del {
					sqlstr, err = dbExec.DataFix().DataAbnormalFix(schema, table, i, colData.DColumnInfo, sqlwhere[wttds.ddrive], wttds.ddrive, indexColumnType).FixDeleteSqlExec(ddb)
					if err != nil {
						wttds.getErr(fmt.Sprintf("dest: checkSum table %s.%s generate delete sql error.", schema, table), err)
					}
					strsqlSliect = append(strsqlSliect, sqlstr)
				}
				if len(add) > 0 {
					for _, i := range add {
						sqlstr, err = dbExec.DataFix().DataAbnormalFix(schema, table, i, colData.DColumnInfo, sqlwhere[wttds.ddrive], wttds.ddrive, indexColumnType).FixInsertSqlExec(ddb)
						if err != nil {
							wttds.getErr(fmt.Sprintf("dest: checkSum table %s.%s generate insert sql error.", schema, table), err)
						}
						strsqlSliect = append(strsqlSliect, sqlstr)
					}
				}
			}
		}
		wttds.sdbPool.Put(sdb)
		wttds.ddbPool.Put(ddb)
	}
	return strsqlSliect
}

func (wttds writeTmpTableDataStruct) DataFixDispos(sqlstr []string) {
	sdb := wttds.sdbPool.Get()
	ddb := wttds.ddbPool.Get()
	for _, i := range sqlstr {
		err := ApplyDataFix(i, ddb, wttds.datafixType, wttds.datafixSql, wttds.ddrive)
		if err != nil {
			wttds.getErr(fmt.Sprintf("dest: checkSum table %s.%s exec delete sql error.", wttds.schema, wttds.table), err)
		}
	}
	wttds.sdbPool.Put(sdb)
	wttds.ddbPool.Put(ddb)
}

/*
	针对表的所有列的数据类型进行处理，将列类型转换成字符串，例如时间类型
*/
func (wttds writeTmpTableDataStruct) queryTableSql(ma map[string]string, cc1 map[string]global.TableAllColumnInfoS) map[string]string {
	var sqlwhereMap = make(map[string]string)
	for k, sqlwhere := range ma {
		var schema, table string
		//var tableColInfo []map[string]string
		if !strings.Contains(k, "/*actionSchema*/") || !strings.Contains(k, "/*actionTable*/") {
			continue
		}
		schema = strings.Split(strings.Split(k, "/*actionSchema*/")[1], "/*actionTable*/")[0]
		table = strings.Split(strings.Split(k, "/*actionSchema*/")[1], "/*actionTable*/")[1]
		tableColInfo := cc1[fmt.Sprintf("%s_greatdbCheck_%s", schema, table)]
		//查询该表的列名和列信息
		idxc := dbExec.IndexColumnStruct{Schema: schema, Table: table, TableColumn: tableColInfo.SColumnInfo, Sqlwhere: sqlwhere, Drivce: wttds.sdrive}
		lock.Lock()
		sqlwhereMap[wttds.sdrive] = idxc.TableIndexColumn().GeneratingQuerySql()
		lock.Unlock()
		idxc.Drivce = wttds.ddrive
		idxc.TableColumn = tableColInfo.DColumnInfo
		lock.Lock()
		sqlwhereMap[wttds.ddrive] = idxc.TableIndexColumn().GeneratingQuerySql()
		lock.Unlock()
	}
	return sqlwhereMap
}

/*
	针对表的所有列的数据类型进行处理，将列类型转换成字符串，例如时间类型，并执行sql语句
*/
func (wttds writeTmpTableDataStruct) queryTableData(sqlwhere map[string]string, cc1 map[string]global.TableAllColumnInfoS) DifferencesDataStruct {
	var (
		aa              = &CheckSumTypeStruct{}
		sdbPool         = wttds.sdbPool
		ddbPool         = wttds.ddbPool
		differencesData = InitDifferencesDataStruct()
	)
	tableColInfo := cc1[fmt.Sprintf("%s_greatdbCheck_%s", wttds.schema, wttds.table)]
	//查询该表的列名和列信息
	sdb := sdbPool.Get()
	ddb := ddbPool.Get()
	idxc := dbExec.IndexColumnStruct{
		Schema:      wttds.schema,
		Table:       wttds.table,
		TableColumn: tableColInfo.SColumnInfo,
		Sqlwhere:    sqlwhere[wttds.sdrive],
		Drivce:      wttds.sdrive,
	}
	stt, err := idxc.TableIndexColumn().GeneratingQueryCriteria(sdb)
	global.Wlog.Debug("")
	if err != nil {
		wttds.getErr(fmt.Sprintf("source: query table %s.%s data chan error.", wttds.schema, wttds.table), err)
	}
	idxc.Drivce = wttds.ddrive
	idxc.Sqlwhere = sqlwhere[wttds.ddrive]
	idxc.TableColumn = tableColInfo.DColumnInfo
	dtt, err := idxc.TableIndexColumn().GeneratingQueryCriteria(ddb)
	if err != nil {
		wttds.getErr(fmt.Sprintf("dest: query table %s.%s data chan error.", wttds.schema, wttds.table), err)
	}
	sdbPool.Put(sdb)
	ddbPool.Put(ddb)
	if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
		differencesData.Table = wttds.table
		differencesData.Schema = wttds.schema
		differencesData.SqlWhere = sqlwhere
		differencesData.TableColumnInfo = cc1
		differencesData.indexColumnType = wttds.indexColumnType
	} else {
		differencesData = InitDifferencesDataStruct()
	}
	return differencesData
}

func (wttds *writeTmpTableDataStruct) IndexColumnProduct(ma <-chan []string, out1 chan<- map[string]string, done <-chan bool, sqlwhereDone chan<- bool, goroutineNum int) {
	var (
		workLimiter = make(chan struct{}, goroutineNum)
		breakDone   = make(chan bool, 1)
		execStatus  = make(chan bool, 1)
		breakStatus = false
	)
	t := time.NewTicker(time.Millisecond)
	for time := range t.C {
		select {
		case d := <-done:
			if d {
				breakStatus = true
			}
		case <-breakDone:
			global.Wlog.Info("table QuerySql Where Data Generate done!")
			fmt.Println("table QuerySql Where Data Generate done!", time.Format("2006-01-02 15:04:11"))
			close(execStatus)
			sqlwhereDone <- true
			close(out1)
			return
		case status := <-execStatus:
			var aa = false
			if status {
				for i := 0; i < 3; i++ {
					if breakStatus && len(ma) == 0 && len(workLimiter) == 0 {
						aa = true
					} else {
						aa = false
					}
				}
				if aa {
					breakDone <- true
					close(breakDone)
				}
			}
			if len(ma) > 0 {
				for i := range ma {
					select {
					case workLimiter <- struct{}{}:
						//wg.Add(1)
						go func(i []string) {
							defer func() {
								<-workLimiter
								//wg.Done()
							}()
							//wttds.TableSelectWhere(wttds.columnName, i)
							//sqlwhere := wttds.TableSelectWhere(wttds.columnName, i)
							out1 <- wttds.TableSelectWhere(wttds.columnName, i)
							//out1 <- sqlwhere
							//select {
							//case out1 <- sqlwhere:
							//}
						}(i)
					}
				}
				//wg.Wait()
			}
		default:
			if breakStatus {
				execStatus <- true
			} else {
				execStatus <- false
			}
		}
	}
}
func (wttds *writeTmpTableDataStruct) SqlwhereProduct(ma chan map[string]string, out2 chan map[string]string, done <-chan bool, differDone chan<- bool, cc1 map[string]global.TableAllColumnInfoS, goroutineNum int) {
	var (
		workLimiter = make(chan struct{}, goroutineNum)
		breakDone   = make(chan bool, 1)
		execStatus  = make(chan bool, 1)
		breakStatus = false
	)
	t := time.NewTicker(time.Millisecond)
	for time := range t.C {
		select {
		case d := <-done:
			if d {
				breakStatus = true
			}
		case <-breakDone:
			fmt.Println("table query sql Product done!", time.Format("2006-01-02 15:04:11"))
			close(execStatus)
			close(out2)
			differDone <- true
			return
		case status := <-execStatus:
			if status {
				var aa = false
				for i := 0; i < 3; i++ {
					if breakStatus && len(ma) == 0 && len(workLimiter) == 0 {
						aa = true
					} else {
						aa = false
					}
				}
				if aa {
					breakDone <- true
					close(breakDone)
				}

			}
			if len(ma) > 0 {
				for i := range ma {
					select {
					case workLimiter <- struct{}{}:
						//wg.Add(1)
						go func(i map[string]string, cc1 map[string]global.TableAllColumnInfoS) {
							defer func() {
								<-workLimiter
								//wg.Done()
							}()
							out2 <- wttds.queryTableSql(i, cc1)
						}(i, cc1)
					}
				}
				//wg.Wait()
			}
		default:
			if breakStatus {
				execStatus <- true
			} else {
				execStatus <- false
			}
		}
	}
}

func (wttds *writeTmpTableDataStruct) QueryTableDataProduct(ma chan map[string]string, out2 chan DifferencesDataStruct, done <-chan bool, differDone chan<- bool, cc1 map[string]global.TableAllColumnInfoS, goroutineNum int) {
	var (
		workLimiter = make(chan struct{}, goroutineNum)
		breakDone   = make(chan bool, 1)
		execStatus  = make(chan bool, 1)
		breakStatus = false
	)
	t := time.NewTicker(time.Millisecond)
	for time := range t.C {
		select {
		case d := <-done:
			if d {
				breakStatus = true
			}
		case <-breakDone:
			fmt.Println("table All Measured Data CheckSum done!", time.Format("2006-01-02 15:04:11"))
			close(execStatus)
			close(out2)
			differDone <- true
			return
		case status := <-execStatus:
			var aa = false
			if status {
				for i := 0; i < 3; i++ {
					if breakStatus && len(ma) == 0 && len(workLimiter) == 0 {
						aa = true
					} else {
						aa = false
					}
				}
				if aa {
					breakDone <- true
					close(breakDone)
				}
			}
			if len(ma) > 0 {
				for i := range ma {
					select {
					case workLimiter <- struct{}{}:
						//wg.Add(1)
						go func(i map[string]string, cc1 map[string]global.TableAllColumnInfoS) {
							defer func() {
								<-workLimiter
								//wg.Done()
							}()
							difference := wttds.queryTableData(i, cc1)
							if difference.Schema != "" && difference.Table != "" {
								out2 <- difference
							}
						}(i, cc1)
					}
				}
				//wg.Wait()

			}
		default:
			if breakStatus {
				execStatus <- true
			} else {
				execStatus <- false
			}
		}
	}
}

/*
	处理差异数据
*/
func (wttds *writeTmpTableDataStruct) AbnormalDataProduct(ma chan DifferencesDataStruct, out1 chan []string, done <-chan bool, diffdone chan<- bool, goroutineNum int) {
	var (
		workLimiter = make(chan struct{}, goroutineNum)
		breakDone   = make(chan bool, 1)
		execStatus  = make(chan bool, 1)
		breakStatus = false
	)
	t := time.NewTicker(time.Millisecond)
	for time := range t.C {
		select {
		case d := <-done:
			if d {
				breakStatus = true
			}
		case <-breakDone:
			fmt.Println("table Differences in Data CheckSum done!", time.Format("2006-01-02 15:04:11"))
			close(execStatus)
			close(out1)
			diffdone <- true
			return
		case status := <-execStatus:
			if status {
				var aa = false
				for i := 0; i < 3; i++ {
					if breakStatus && len(ma) == 0 && len(workLimiter) == 0 {
						aa = true
					} else {
						aa = false
					}
				}
				if aa {
					breakDone <- true
					close(breakDone)
				}
			}
			if len(ma) > 0 {
				for i := range ma {
					select {
					case workLimiter <- struct{}{}:
						go func(i DifferencesDataStruct) {
							//wg.Add(1)
							defer func() {
								<-workLimiter
								//wg.Done()
							}()
							out1 <- wttds.AbnormalDataDispos(i.Schema, i.Table, i.SqlWhere, i.TableColumnInfo, i.indexColumnType)
						}(i)
					}
				}
				//wg.Wait()
			}
		default:
			if breakStatus {
				execStatus <- true
			} else {
				execStatus <- false
			}
		}
	}
}

/*
	处理差异数据
*/
func (wttds *writeTmpTableDataStruct) DataFixDataProduct(ma chan []string, diffdone <-chan bool, done chan<- bool, goroutineNum int) {
	var (
		workLimiter = make(chan struct{}, goroutineNum)
		breakDone   = make(chan bool, 1)
		execStatus  = make(chan bool, 1)
		breakStatus = false
	)
	t := time.NewTicker(time.Millisecond)
	for time := range t.C {
		select {
		case d := <-diffdone:
			if d {
				breakStatus = true
			}
		case <-breakDone:
			fmt.Println("table Differences in Data fix done!!", time.Format("2006-01-02 15:04:11"))
			close(execStatus)
			done <- true
			return
		case status := <-execStatus:
			if status {
				var aa = false
				for i := 0; i < 3; i++ {
					if breakStatus && len(ma) == 0 && len(workLimiter) == 0 {
						aa = true
					} else {
						aa = false
					}
				}
				if aa {
					breakDone <- true
					close(breakDone)
				}

			}
			if len(ma) > 0 {
				for i := range ma {
					select {
					case workLimiter <- struct{}{}:
						//wg.Add(1)
						go func(i []string) {
							defer func() {
								<-workLimiter
								//defer wg.Done()
							}()
							wttds.DataFixDispos(i)
						}(i)
					}
				}
				//wg.Wait()
			}
		default:
			if breakStatus {
				execStatus <- true
			} else {
				execStatus <- false
			}
		}
	}
}

func (wttds writeTmpTableDataStruct) doIndexDataCheck() {
	var (
		queueDepth                                                     = wttds.queueDepth
		selectWhere                                                    = make(chan map[string]string, queueDepth)
		sqlWhere                                                       = make(chan map[string]string, queueDepth)
		differencesData                                                = make(chan DifferencesDataStruct, queueDepth)
		indexColData                                                   = make(chan []string, queueDepth)
		dataFix                                                        = make(chan []string, queueDepth)
		differdone, done, IndexColumnDone, sqlwhereDone, queryDataDone = make(chan bool, 1), make(chan bool, 1), make(chan bool, 1), make(chan bool, 1), make(chan bool, 1)
		dataFixdone, limitPagDone                                      = make(chan bool, 1), make(chan bool, 1)
		limitPag                                                       = make(chan int64, queueDepth)
	)
	/*
		获取校验表的索引列数据、索引列长度,校验表的所有列信息
	*/
	//获取索引列数据长度，处理索引列数据中有null或空字符串的问题
	idxc := dbExec.IndexColumnStruct{Schema: wttds.schema, Table: wttds.table, ColumnName: wttds.columnName, ChanrowCount: wttds.chanrowCount, Drivce: wttds.sdrive}
	selectColumnString, columnLengthAs, lengthTrim := idxc.TableIndexColumn().TmpTableIndexColumnDataLength()
	//查询表的所有列及列的序号，为生成修复语句使用（生成delete语句）
	go wttds.LimiterSeq(limitPag, limitPagDone)
	go wttds.indexColUniqProduct(limitPag, indexColData, limitPagDone, IndexColumnDone, selectColumnString, lengthTrim, columnLengthAs, wttds.concurrency)
	go wttds.IndexColumnProduct(indexColData, selectWhere, IndexColumnDone, sqlwhereDone, wttds.concurrency)
	go wttds.SqlwhereProduct(selectWhere, sqlWhere, sqlwhereDone, queryDataDone, wttds.tableAllCol, wttds.concurrency)
	go wttds.QueryTableDataProduct(sqlWhere, differencesData, queryDataDone, differdone, wttds.tableAllCol, wttds.concurrency)
	go wttds.AbnormalDataProduct(differencesData, dataFix, differdone, dataFixdone, wttds.concurrency)
	go wttds.DataFixDataProduct(dataFix, dataFixdone, done, wttds.concurrency)

	for {
		select {
		case _, ok := <-done:
			if ok {
				fmt.Println(fmt.Sprintf("%s.%s 校验完成", wttds.schema, wttds.table))
				return
			}
		}
	}
}

/*
	无索引表的处理方式
*/
func (wttds writeTmpTableDataStruct) doNoIndexDataCheck(noIndexC chan struct{}) {
	var (
		aa     = &CheckSumTypeStruct{}
		sqlstr string
	)
	noIndexC <- struct{}{}
	//1、获取当前源目标端表的总行数
	sdb := wttds.sdbPool.Get()
	ddb := wttds.ddbPool.Get()
	idxc := dbExec.IndexColumnStruct{
		Drivce:       wttds.sdrive,
		Schema:       wttds.schema,
		Table:        wttds.table,
		ColumnName:   wttds.columnName,
		ChanrowCount: wttds.chanrowCount,
	}
	stmpTableCount, err := idxc.TableIndexColumn().TmpTableRowsCount(sdb)
	if err != nil {
		wttds.getErr(fmt.Sprintf("source: query table %s.%s rows total error.", wttds.schema, wttds.table), err)
	}
	idxc.Drivce = wttds.ddrive
	dtmpTableCount, err := idxc.TableIndexColumn().TmpTableRowsCount(ddb)
	if err != nil {
		wttds.getErr(fmt.Sprintf("dest: query table %s.%s rows total error.", wttds.schema, wttds.table), err)
	}
	var maxTableCount, schedulePlanCount int
	if stmpTableCount > dtmpTableCount || stmpTableCount == dtmpTableCount {
		maxTableCount = stmpTableCount
	} else {
		maxTableCount = dtmpTableCount
	}
	if maxTableCount%wttds.chanrowCount != 0 {
		schedulePlanCount = maxTableCount/wttds.chanrowCount + 1
	} else {
		schedulePlanCount = maxTableCount / wttds.chanrowCount
	}

	//输出校验结果信息
	measuredDataPods = append(measuredDataPods, Pod{
		Schema:      wttds.schema,
		Table:       wttds.table,
		IndexCol:    "noIndex",
		CheckMod:    wttds.checkMod,
		Rows:        strconv.Itoa(maxTableCount),
		Differences: "no",
		Datafix:     wttds.datafixType,
	})

	//2、生成查询计划
	var beginSeq int
	var tmpAnDateMap = make(map[string]string)
	colData := wttds.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", wttds.schema, wttds.table)]
	for i := 0; i < schedulePlanCount; i++ {
		idxc.Drivce = wttds.sdrive
		stt, _ := idxc.TableIndexColumn().NoIndexGeneratingQueryCriteria(sdb, beginSeq, wttds.chanrowCount)
		idxc.Drivce = wttds.ddrive
		dtt, _ := idxc.TableIndexColumn().NoIndexGeneratingQueryCriteria(ddb, beginSeq, wttds.chanrowCount)
		if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
			add, del := aa.Arrcmp(strings.Split(stt, "/*go actions rowData*/"), strings.Split(dtt, "/*go actions rowData*/"))
			differencesSchemaTable[fmt.Sprintf("%sgreatdbCheck_greatdbCheck%s", wttds.schema, wttds.table)] = wttds.datafixType
			if len(del) > 0 {
				for _, deli := range del {
					if a, ok := tmpAnDateMap[deli]; ok && a == "insert" {
						delete(tmpAnDateMap, deli)
					} else {
						tmpAnDateMap[deli] = "delete"
					}
				}
			}
			if len(add) > 0 {
				for _, addi := range add {
					if a, ok := tmpAnDateMap[addi]; ok && a == "delete" {
						delete(tmpAnDateMap, addi)
					} else {
						tmpAnDateMap[addi] = "insert"
					}
				}
			}
		}
		beginSeq = beginSeq + wttds.chanrowCount
	}
	for k, v := range tmpAnDateMap {
		indexColumnType := "mui"
		if v == "delete" {
			sqlstr, err = dbExec.DataFix().DataAbnormalFix(wttds.schema, wttds.table, k, colData.DColumnInfo, "", wttds.ddrive, indexColumnType).FixDeleteSqlExec(ddb)
			if err != nil {
				wttds.getErr(fmt.Sprintf("dest: checkSum table %s.%s generate delete sql error.", wttds.schema, wttds.table), err)
			}
			err = ApplyDataFix(sqlstr, ddb, wttds.datafixType, wttds.datafixSql, wttds.ddrive)
			if err != nil {
				wttds.getErr(fmt.Sprintf("dest: checkSum table %s.%s exec delete sql error.", wttds.schema, wttds.table), err)
			}
		}
		if v == "insert" {
			sqlstr, err = dbExec.DataFix().DataAbnormalFix(wttds.schema, wttds.table, k, colData.DColumnInfo, "", wttds.ddrive, indexColumnType).FixInsertSqlExec(ddb)
			if err != nil {
				wttds.getErr(fmt.Sprintf("dest: checkSum table %s.%s generate insert sql error.", wttds.schema, wttds.table), err)
			}
			err = ApplyDataFix(sqlstr, ddb, wttds.datafixType, wttds.datafixSql, wttds.ddrive)
			if err != nil {
				wttds.getErr(fmt.Sprintf("dest: checkSum table %s.%s exec insert sql error.", wttds.schema, wttds.table), err)
			}
		}
	}
	wttds.sdbPool.Put(sdb)
	wttds.ddbPool.Put(ddb)
	<-noIndexC
	fmt.Println(fmt.Sprintf("%s.%s 校验完成", wttds.schema, wttds.table))
}

/*
	使用count(1)的方式进行校验
*/
func (sp *SchedulePlan) DoCountDataCheck() {
	var schema, table string
	for k, v := range sp.tableIndexColumnMap {
		if strings.Contains(k, "/*indexColumnType*/") {
			ki := strings.Split(k, "/*indexColumnType*/")[0]
			//sp.indexColumnType = strings.Split(k, "/*indexColumnType*/")[1]
			if strings.Contains(ki, "/*greatdbSchemaTable*/") {
				schema = strings.Split(ki, "/*greatdbSchemaTable*/")[0]
				table = strings.Split(ki, "/*greatdbSchemaTable*/")[1]
			}
		} else {
			if strings.Contains(k, "/*greatdbSchemaTable*/") {
				schema = strings.Split(k, "/*greatdbSchemaTable*/")[0]
				table = strings.Split(k, "/*greatdbSchemaTable*/")[1]
			}
		}
		fmt.Println(fmt.Sprintf("begin count index table %s.%s", schema, table))
		global.Wlog.Info(fmt.Sprintf("begin count index table %s.%s", schema, table))
		var chanrowCount int
		switch len(v) {
		case 0:
			chanrowCount = 0
		case 1:
			chanrowCount = sp.singleIndexChanRowCount
		default:
			chanrowCount = sp.jointIndexChanRowCount
		}
		global.Wlog.Info(fmt.Sprintf("checkSum table %s.%s use chanorwCount Value is %s", schema, table, chanrowCount))
		sdb := sp.sdbConnPool.Get()
		ddb := sp.ddbConnPool.Get()
		//查询原目标端的表总行数，并生成调度计划
		idxc := dbExec.IndexColumnStruct{Schema: schema, Table: table, ColumnName: sp.columnName, ChanrowCount: chanrowCount, Drivce: sp.sdrive}
		stmpTableCount, err := idxc.TableIndexColumn().TmpTableRowsCount(sdb)
		if err != nil {
			//sp.getErr(fmt.Sprintf("source: query table %s.%s rows total error.", schema, table), err)
			fmt.Println(err)
			os.Exit(1)
		}
		idxc.Drivce = sp.ddrive
		dtmpTableCount, err := idxc.TableIndexColumn().TmpTableRowsCount(ddb)
		if err != nil {
			//wttds.getErr(fmt.Sprintf("dest: query table %s.%s rows total error.", schema, table), err)
			fmt.Println(err)
			os.Exit(1)
		}
		sp.sdbConnPool.Put(sdb)
		sp.ddbConnPool.Put(ddb)

		//输出校验结果信息
		var pods = Pod{
			Schema:   schema,
			Table:    table,
			IndexCol: strings.Join(v, ","),
			CheckMod: sp.checkMod,
			Datafix:  sp.datafixType,
		}
		if len(v) == 0 {
			pods.IndexCol = "no"
		}
		if stmpTableCount == dtmpTableCount {
			pods.Differences = "no"
			pods.Rows = strconv.Itoa(stmpTableCount)
		} else {
			pods.Differences = "yes"
			pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)
		}
		measuredDataPods = append(measuredDataPods, pods)
		fmt.Println(fmt.Sprintf("count index table %s.%s done !", schema, table))
		global.Wlog.Info(fmt.Sprintf("count index table %s.%s done !", schema, table))
	}
}

/*
	做数据的抽样检查，先使用count*，针对count*数量一致的表在进行部分数据的抽样检查
*/
//func (sp *SchedulePlan) DoSampleDataCheck() {
//	var schema, table string
//	for k, v := range sp.tableIndexColumnMap {
//		if strings.Contains(k, "/*indexColumnType*/") {
//			ki := strings.Split(k, "/*indexColumnType*/")[0]
//			//sp.indexColumnType = strings.Split(k, "/*indexColumnType*/")[1]
//			if strings.Contains(ki, "/*greatdbSchemaTable*/") {
//				schema = strings.Split(ki, "/*greatdbSchemaTable*/")[0]
//				table = strings.Split(ki, "/*greatdbSchemaTable*/")[1]
//			}
//		} else {
//			if strings.Contains(k, "/*greatdbSchemaTable*/") {
//				schema = strings.Split(k, "/*greatdbSchemaTable*/")[0]
//				table = strings.Split(k, "/*greatdbSchemaTable*/")[1]
//			}
//		}
//		fmt.Println(fmt.Sprintf("begin count index table %s.%s", schema, table))
//		global.Wlog.Info(fmt.Sprintf("begin count index table %s.%s", schema, table))
//		var chanrowCount int
//		switch len(v) {
//		case 0:
//			chanrowCount = 0
//		case 1:
//			chanrowCount = sp.singleIndexChanRowCount
//		default:
//			chanrowCount = sp.jointIndexChanRowCount
//		}
//		global.Wlog.Info(fmt.Sprintf("checkSum table %s.%s use chanorwCount Value is %s", schema, table, chanrowCount))
//		sdb := sp.sdbConnPool.Get()
//		ddb := sp.ddbConnPool.Get()
//		//查询原目标端的表总行数，并生成调度计划
//		idxc := dbExec.IndexColumnStruct{Schema: schema, Table: table, ColumnName: sp.columnName, ChanrowCount: chanrowCount, Drivce: sp.sdrive}
//		stmpTableCount, err := idxc.TableIndexColumn().TmpTableRowsCount(sdb)
//		if err != nil {
//			//sp.getErr(fmt.Sprintf("source: query table %s.%s rows total error.", schema, table), err)
//			fmt.Println(err)
//			os.Exit(1)
//		}
//		idxc.Drivce = sp.ddrive
//		dtmpTableCount, err := idxc.TableIndexColumn().TmpTableRowsCount(ddb)
//		if err != nil {
//			//wttds.getErr(fmt.Sprintf("dest: query table %s.%s rows total error.", schema, table), err)
//			fmt.Println(err)
//			os.Exit(1)
//		}
//		sp.sdbConnPool.Put(sdb)
//		sp.ddbConnPool.Put(ddb)
//		//输出校验结果信息
//		if stmpTableCount == dtmpTableCount {
//
//		}
//
//		var pods = Pod{
//			Schema:   schema,
//			Table:    table,
//			IndexCol: strings.Join(v, ","),
//			CheckMod: sp.checkMod,
//			Datafix:  sp.datafixType,
//		}
//		if len(v) == 0 {
//			pods.IndexCol = "no"
//		}
//		if stmpTableCount == dtmpTableCount {
//			pods.Differences = "no"
//			pods.Rows = strconv.Itoa(stmpTableCount)
//		} else {
//			pods.Differences = "yes"
//			pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)
//		}
//		measuredDataPods = append(measuredDataPods, pods)
//	}
//}

/*
   查询索引列信息，并发执行调度生成
*/
func (sp *SchedulePlan) Schedulingtasks() {
	if _, err := os.Stat(sp.datafixSql); err == nil {
		os.Remove(sp.datafixSql)
	}
	var chanrowCount int
	wd := &writeTmpTableDataStruct{
		sdbPool:             sp.sdbConnPool,
		sdrive:              sp.sdrive,
		ddrive:              sp.ddrive,
		ddbPool:             sp.ddbConnPool,
		tableIndexColumnMap: sp.tableIndexColumnMap,
		datafixType:         sp.datafixType,
		datafixSql:          sp.datafixSql,
		queueDepth:          sp.mqQueueDepth,
		concurrency:         sp.Concurrency,
		chanrowCount:        sp.jointIndexChanRowCount,
		tableAllCol:         sp.tableAllCol,
		checkMod:            sp.checkMod,
		ratio:               sp.ratio,
	}
	var noIndexStatusC = make(chan struct{}, len(sp.tableIndexColumnMap))
	for k, v := range sp.tableIndexColumnMap {
		var schema, table string
		if strings.Contains(k, "/*indexColumnType*/") {
			ki := strings.Split(k, "/*indexColumnType*/")[0]
			wd.indexColumnType = strings.Split(k, "/*indexColumnType*/")[1]
			if strings.Contains(ki, "/*greatdbSchemaTable*/") {
				schema = strings.Split(ki, "/*greatdbSchemaTable*/")[0]
				table = strings.Split(ki, "/*greatdbSchemaTable*/")[1]
			}
		} else {
			if strings.Contains(k, "/*greatdbSchemaTable*/") {
				schema = strings.Split(k, "/*greatdbSchemaTable*/")[0]
				table = strings.Split(k, "/*greatdbSchemaTable*/")[1]
			}
		}

		wd.schema = schema
		wd.table = table
		if sp.checkNoIndexTable == "yes" && len(v) == 0 {
			fmt.Println(fmt.Sprintf("begin checkSum no index table %s.%s", schema, table))
			global.Wlog.Info(fmt.Sprintf("begin checkSum no index %s.%s", schema, table))
			global.Wlog.Info(fmt.Sprintf("checkSum table %s.%s use chanorwCount Value is %d", schema, table, chanrowCount))
			go wd.doNoIndexDataCheck(noIndexStatusC)
		} else {
			//根据索引列数量觉得chanrowCount数
			if len(v) > 1 {
				chanrowCount = sp.jointIndexChanRowCount
			} else {
				chanrowCount = sp.singleIndexChanRowCount
			}
			wd.chanrowCount = chanrowCount
			wd.columnName = v
			fmt.Println(fmt.Sprintf("begin checkSum index table %s.%s", schema, table))
			global.Wlog.Info(fmt.Sprintf("begin checkSum %s.%s", schema, table))
			global.Wlog.Info(fmt.Sprintf("checkSum table %s.%s use chanorwCount Value is %d", schema, table, chanrowCount))
			wd.doIndexDataCheck()
		}
	}
	for {
		time.Sleep(time.Second)
		if len(noIndexStatusC) == 0 {
			break
		}
	}
}

func CheckTableQuerySchedule(sdb, ddb *global.Pool, tableIndexColumnMap map[string][]string, tableAllCol map[string]global.TableAllColumnInfoS, m inputArg.ConfigParameter) *SchedulePlan {
	return &SchedulePlan{
		Concurrency:             m.Concurrency,
		sdbConnPool:             sdb,
		ddbConnPool:             ddb,
		singleIndexChanRowCount: m.SingleIndexChanRowCount,
		jointIndexChanRowCount:  m.JointIndexChanRowCount,
		tableIndexColumnMap:     tableIndexColumnMap,
		tableAllCol:             tableAllCol,
		datafixType:             m.Datafix,
		datafixSql:              fmt.Sprintf("%s/%s", m.FixPath, m.FixFileName),
		sdrive:                  m.SourceDrive,
		ddrive:                  m.DestDrive,
		mqQueueDepth:            m.QueueDepth,
		checkNoIndexTable:       m.CheckNoIndexTable,
		checkMod:                m.CheckMode,
		ratio:                   m.Ratio,
	}
}
