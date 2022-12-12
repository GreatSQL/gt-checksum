package actions

import (
	"fmt"
	"greatdbCheck/dbExec"
	"greatdbCheck/global"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	wg                    sync.WaitGroup
	lock                  sync.Mutex
	breakIndexColumnMq    = false
	querySqlWhereSliceMap = make([]map[string]string, 0)
)

/*
   初始化差异数据信息结构体
*/
func InitDifferencesDataStruct() DifferencesDataStruct {
	return DifferencesDataStruct{}
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
func (wttds writeTmpTableDataStruct) SampLimiterSeq(limitPag chan string, limitPagDone chan bool) { //定义变量
	var (
		schema                           = wttds.schema
		table                            = wttds.table
		columnName                       = wttds.columnName
		chanrowCount                     = wttds.chanrowCount
		maxTableCount, schedulePlanCount int
	)
	time.Sleep(time.Nanosecond * 2)
	rand.Seed(time.Now().UnixNano())
	logThreadSeq := rand.Int63()

	alog := fmt.Sprintf("(%d) Check table %s.%s and start generating query sequence.", logThreadSeq, schema, table)
	global.Wlog.Info(alog)

	clog := fmt.Sprintf("(%d) The current verification table %s.%s single verification row number is [%d]", logThreadSeq, schema, table, chanrowCount)
	global.Wlog.Info(clog)
	sdb := wttds.sdbPool.Get(logThreadSeq)
	//查询原目标端的表总行数，并生成调度计划
	idxc := dbExec.IndexColumnStruct{Schema: wttds.schema, Table: wttds.table, ColumnName: wttds.columnName, ChanrowCount: wttds.chanrowCount, Drivce: wttds.sdrive}
	stmpTableCount, _ := idxc.TableIndexColumn().TmpTableRowsCount(sdb, logThreadSeq)
	wttds.sdbPool.Put(sdb, logThreadSeq)

	idxc.Drivce = wttds.ddrive
	ddb := wttds.ddbPool.Get(logThreadSeq)
	dtmpTableCount, _ := idxc.TableIndexColumn().TmpTableRowsCount(ddb, logThreadSeq)
	wttds.ddbPool.Put(ddb, logThreadSeq)

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
	if stmpTableCount != dtmpTableCount {
		pods.Rows = fmt.Sprintf("%d|%d", stmpTableCount, dtmpTableCount)
		measuredDataPods = append(measuredDataPods, pods)
	} else {
		var newMaxTableCount int //抽样比例后的总数值
		newMaxTableCount = maxTableCount
		if maxTableCount > chanrowCount {
			newMaxTableCount = maxTableCount * wttds.ratio / 100
			if chanrowCount > wttds.concurrency {
				chanrowCount = chanrowCount / wttds.concurrency
			}
		}
		if newMaxTableCount%chanrowCount != 0 {
			schedulePlanCount = newMaxTableCount/chanrowCount + 1
		} else {
			schedulePlanCount = newMaxTableCount / chanrowCount
		}
		tlog := fmt.Sprintf("(%d) There is currently index table %s.%s, the number of rows to be verified at a time is %d, and the number of rows to be verified is %d times", logThreadSeq, schema, table, chanrowCount, schedulePlanCount)
		global.Wlog.Info(tlog)
		var beginSeq int64
		nanotime := int64(time.Now().Nanosecond())
		rand.Seed(nanotime)
		for i := 0; i < schedulePlanCount; i++ {
			if newMaxTableCount > chanrowCount {
				beginSeq = rand.Int63n(int64(maxTableCount))
			}
			xlog := fmt.Sprintf("(%d) Verify table %s.%s The query sequence is written to the mq queue for the %d time, and the written information is {%s}", logThreadSeq, schema, table, i, fmt.Sprintf("%d,%d", beginSeq, maxTableCount))
			global.Wlog.Info(xlog)
			limitPag <- fmt.Sprintf("%d,%d", beginSeq, newMaxTableCount)
			beginSeq = beginSeq + int64(chanrowCount)
		}
		pods.Rows = fmt.Sprintf("%d,%d", maxTableCount, newMaxTableCount)
		measuredDataPods = append(measuredDataPods, pods)
	}
	limitPagDone <- true
	ylog := fmt.Sprintf("(%d) Verify table %s.%s Close the mq queue that stores the query sequence.", logThreadSeq, schema, table)
	global.Wlog.Info(ylog)
	close(limitPag)
	zlog := fmt.Sprintf("(%d) Verify that table %s.%s query sequence is generated. !!!", logThreadSeq, schema, table)
	global.Wlog.Info(zlog)
}

/*
	计算源目标段表的最大行数
*/
func (wttds writeTmpTableDataStruct) LimiterSeq(limitPag chan string, limitPagDone chan bool) { //定义变量
	var (
		schema                           = wttds.schema
		table                            = wttds.table
		columnName                       = wttds.columnName
		chanrowCount                     = wttds.chanrowCount
		maxTableCount, schedulePlanCount int
	)
	time.Sleep(time.Nanosecond * 2)
	rand.Seed(time.Now().UnixNano())
	logThreadSeq := rand.Int63()
	alog := fmt.Sprintf("(%d) Check table %s.%s and start generating query sequence.", logThreadSeq, schema, table)
	global.Wlog.Info(alog)
	//查询原目标端的表总行数，并生成调度计划
	clog := fmt.Sprintf("(%d) The current verification table %s.%s single verification row number is [%d]", logThreadSeq, schema, table, chanrowCount)
	global.Wlog.Info(clog)
	sdb := wttds.sdbPool.Get(logThreadSeq)
	idxc := dbExec.IndexColumnStruct{Schema: wttds.schema, Table: wttds.table, ColumnName: wttds.columnName, ChanrowCount: wttds.chanrowCount, Drivce: wttds.sdrive}
	stmpTableCount, _ := idxc.TableIndexColumn().TmpTableRowsCount(sdb, logThreadSeq)
	wttds.sdbPool.Put(sdb, logThreadSeq)

	idxc.Drivce = wttds.ddrive
	ddb := wttds.ddbPool.Get(logThreadSeq)
	dtmpTableCount, _ := idxc.TableIndexColumn().TmpTableRowsCount(ddb, logThreadSeq)
	wttds.ddbPool.Put(ddb, logThreadSeq)

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
	//if wttds.checkMod == "sample" {
	//	if stmpTableCount != dtmpTableCount {
	//		pods.Rows = fmt.Sprintf("%d|%d", stmpTableCount, dtmpTableCount)
	//		measuredDataPods = append(measuredDataPods, pods)
	//	} else {
	//		var newMaxTableCount int //抽样比例后的总数值
	//		newMaxTableCount = maxTableCount
	//		if maxTableCount > chanrowCount {
	//			newMaxTableCount = maxTableCount * wttds.ratio / 100
	//			if chanrowCount > wttds.concurrency {
	//				chanrowCount = chanrowCount / wttds.concurrency
	//			}
	//		}
	//		if newMaxTableCount%chanrowCount != 0 {
	//			schedulePlanCount = newMaxTableCount/chanrowCount + 1
	//		} else {
	//			schedulePlanCount = newMaxTableCount / chanrowCount
	//		}
	//		var beginSeq int64
	//		nanotime := int64(time.Now().Nanosecond())
	//		rand.Seed(nanotime)
	//		for i := 0; i < schedulePlanCount; i++ {
	//			if newMaxTableCount > chanrowCount {
	//				beginSeq = rand.Int63n(int64(maxTableCount))
	//			}
	//			limitPag <- fmt.Sprintf("%d,%d", beginSeq, newMaxTableCount)
	//			beginSeq = beginSeq + int64(chanrowCount)
	//		}
	//		pods.Rows = fmt.Sprintf("%d,%d", maxTableCount, newMaxTableCount)
	//		measuredDataPods = append(measuredDataPods, pods)
	//	}
	//} else {
	//	if maxTableCount%chanrowCount != 0 {
	//		schedulePlanCount = maxTableCount/chanrowCount + 1
	//	} else {
	//		schedulePlanCount = maxTableCount / chanrowCount
	//	}
	//	var beginSeq int64 = 0
	//	for i := 0; i < schedulePlanCount; i++ {
	//		limitPag <- fmt.Sprintf("%d,%d", beginSeq, maxTableCount)
	//		beginSeq = beginSeq + int64(chanrowCount)
	//	}
	//	pods.Rows = fmt.Sprintf("%d,%d", maxTableCount, maxTableCount)
	//	measuredDataPods = append(measuredDataPods, pods)
	//}
	if maxTableCount%chanrowCount != 0 {
		schedulePlanCount = maxTableCount/chanrowCount + 1
	} else {
		schedulePlanCount = maxTableCount / chanrowCount
	}
	tlog := fmt.Sprintf("(%d) There is currently index table %s.%s, the number of rows to be verified at a time is %d, and the number of rows to be verified is %d times", logThreadSeq, schema, table, chanrowCount, schedulePlanCount)
	global.Wlog.Info(tlog)
	var beginSeq int64 = 0
	for i := 0; i < schedulePlanCount; i++ {
		xlog := fmt.Sprintf("(%d) Verify table %s.%s The query sequence is written to the mq queue for the %d time, and the written information is {%s}", logThreadSeq, schema, table, i, fmt.Sprintf("%d,%d", beginSeq, maxTableCount))
		global.Wlog.Info(xlog)
		limitPag <- fmt.Sprintf("%d,%d", beginSeq, maxTableCount)
		beginSeq = beginSeq + int64(chanrowCount)
	}
	pods.Rows = fmt.Sprintf("%d,%d", maxTableCount, maxTableCount)
	measuredDataPods = append(measuredDataPods, pods)
	limitPagDone <- true
	ylog := fmt.Sprintf("(%d) Verify table %s.%s Close the mq queue that stores the query sequence.", logThreadSeq, schema, table)
	global.Wlog.Info(ylog)
	close(limitPag)
	zlog := fmt.Sprintf("(%d) Verify that table %s.%s query sequence is generated. !!!", logThreadSeq, schema, table)
	global.Wlog.Info(zlog)
}

func (wttds writeTmpTableDataStruct) indexColUniq(il string, pag int64, selectColumnString map[string]string, lengthTrim map[string]string, columnLengthAs map[string][]string, logThreadSeq int64) []string {
	tmpmap := make(map[string]int)
	tmpsl := []string{}
	alog := fmt.Sprintf("(%d) Check table %s.%s and start querying the source and target index column data . chunk data seq is {%d ~ %d}...", logThreadSeq, wttds.schema, wttds.table, il, pag)
	global.Wlog.Info(alog)

	idxc := dbExec.IndexColumnStruct{Schema: wttds.schema, Table: wttds.table, ColumnName: wttds.columnName,
		ChanrowCount: wttds.chanrowCount, Drivce: wttds.sdrive, SelectColumnString: selectColumnString[wttds.sdrive],
		LengthTrim: lengthTrim[wttds.sdrive], ColumnLengthAs: columnLengthAs[wttds.sdrive], BeginSeq: il, RowDataCh: pag}
	sdb := wttds.sdbPool.Get(logThreadSeq)
	sourceTmpTableData, _ := idxc.TableIndexColumn().TmpTableIndexColumnDataDispos(sdb, logThreadSeq)
	wttds.sdbPool.Put(sdb, logThreadSeq)

	idxc.Drivce = wttds.ddrive
	idxc.SelectColumnString = selectColumnString[wttds.ddrive]
	idxc.LengthTrim = lengthTrim[wttds.ddrive]
	idxc.ColumnLengthAs = columnLengthAs[wttds.ddrive]
	idxc.ColumnName = wttds.columnName
	idxc.BeginSeq = il
	idxc.RowDataCh = pag
	ddb := wttds.ddbPool.Get(logThreadSeq)
	destTmpTableData, _ := idxc.TableIndexColumn().TmpTableIndexColumnDataDispos(ddb, logThreadSeq)
	wttds.ddbPool.Put(ddb, logThreadSeq)

	blog := fmt.Sprintf("(%d) Check table %s.%s and start deduplication and union of index column data ...", logThreadSeq, wttds.schema, wttds.table)
	global.Wlog.Info(blog)
	if len(sourceTmpTableData) != 0 && len(destTmpTableData) != 0 {
		for _, ii := range sourceTmpTableData {
			lock.Lock()
			tmpmap[ii]++
			lock.Unlock()
		}
		for _, ii := range destTmpTableData {
			lock.Lock()
			if _, ok := tmpmap[ii]; !ok {
				tmpmap[ii]++
			}
			lock.Unlock()
		}
		for k, _ := range tmpmap {
			tmpsl = append(tmpsl, k)
		}

	}
	clog := fmt.Sprintf("(%d) Check table %s.%s The original target index column data processing is completed. !!!", logThreadSeq, wttds.schema, wttds.table)
	global.Wlog.Info(clog)
	return tmpsl
}

/*
	根据表的索引列数据生成select where 条件
*/
func (wttds writeTmpTableDataStruct) TableSelectWhere(columnName, indexColumnData []string, logThreadSeq int64) map[string]string {
	var (
		chankey, nullString string
		chanData            = make(map[string]string)
		schema              = wttds.schema
		table               = wttds.table
	)
	alog := fmt.Sprintf("(%d) Check table %s.%s to start generating the where condition of the query sql statement...", logThreadSeq, schema, table)
	global.Wlog.Info(alog)
	//索引列为单列
	if len(columnName) == 1 {
		blog := fmt.Sprintf("(%d) When the index column of the verification table %s.%s is a single-column index, start to generate the sql where condition", logThreadSeq, schema, table)
		global.Wlog.Info(blog)
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
		clog := fmt.Sprintf("(%d) When the index column of the verification table %s.%s is a single-column index, the sql where condition is generated", logThreadSeq, schema, table)
		global.Wlog.Info(clog)
		chanData[chankey] = nullString
		dlog := fmt.Sprintf("(%d) When the query condition of the index column of the verification table %s.%s is {%s}", logThreadSeq, schema, table, nullString)
		global.Wlog.Info(dlog)
	}

	//多列生成where条件，做单行校验
	if len(columnName) > 1 {
		blog := fmt.Sprintf("(%d) When the index column of the verification table %s.%s is a multi-column index, start to generate the sql where condition", logThreadSeq, schema, table)
		global.Wlog.Info(blog)
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
		clog := fmt.Sprintf("(%d) When the index column of the verification table %s.%s is a multi-column index, the sql where condition is generated", logThreadSeq, schema, table)
		global.Wlog.Info(clog)
		nullSt := fmt.Sprintf("( %s )", strings.Join(duoColumnSlisp, ") or ("))
		chanData[chankey] = nullSt
		//global.Wlog.Debug("[query table ", wttds.schema, ".", wttds.table, " sql where] ", "Conform to the index sql info: ", nullSt)
		dlog := fmt.Sprintf("(%d) When the query condition of the index column of the verification table %s.%s is {%s}", logThreadSeq, schema, table, nullSt)
		global.Wlog.Info(dlog)
		duoColumnSlisp = []string{}
	}
	return chanData
}

/*
	针对表的所有列的数据类型进行处理，将列类型转换成字符串，例如时间类型
*/
func (wttds writeTmpTableDataStruct) queryTableSql(ma map[string]string, cc1 map[string]global.TableAllColumnInfoS, logThreadSeq int64) map[string]string {
	var sqlwhereMap = make(map[string]string)
	alog := fmt.Sprintf("(%d) Start processing the block data verification query sql of the verification table ...", logThreadSeq)
	global.Wlog.Info(alog)
	for k, sqlwhere := range ma {
		var schema, table string
		if !strings.Contains(k, "/*actionSchema*/") || !strings.Contains(k, "/*actionTable*/") {
			continue
		}
		schema = strings.Split(strings.Split(k, "/*actionSchema*/")[1], "/*actionTable*/")[0]
		table = strings.Split(strings.Split(k, "/*actionSchema*/")[1], "/*actionTable*/")[1]
		tableColInfo := cc1[fmt.Sprintf("%s_greatdbCheck_%s", schema, table)]
		//查询该表的列名和列信息
		idxc := dbExec.IndexColumnStruct{Schema: schema, Table: table, TableColumn: tableColInfo.SColumnInfo, Sqlwhere: sqlwhere, Drivce: wttds.sdrive}
		lock.Lock()
		sqlwhereMap[wttds.sdrive] = idxc.TableIndexColumn().GeneratingQuerySql(logThreadSeq)
		lock.Unlock()
		idxc.Drivce = wttds.ddrive
		idxc.TableColumn = tableColInfo.DColumnInfo
		lock.Lock()
		sqlwhereMap[wttds.ddrive] = idxc.TableIndexColumn().GeneratingQuerySql(logThreadSeq)
		lock.Unlock()
	}
	blog := fmt.Sprintf("(%d) The block data verification query sql processing of the verification table is completed. !!!", logThreadSeq)
	global.Wlog.Info(blog)
	return sqlwhereMap
}

/*
	针对表的所有列的数据类型进行处理，将列类型转换成字符串，例如时间类型，并执行sql语句
*/
func (wttds writeTmpTableDataStruct) queryTableData(sqlwhere map[string]string, cc1 map[string]global.TableAllColumnInfoS, logThreadSeq int64) DifferencesDataStruct {
	var (
		aa              = &CheckSumTypeStruct{}
		sdbPool         = wttds.sdbPool
		ddbPool         = wttds.ddbPool
		differencesData = InitDifferencesDataStruct()
	)
	tableColInfo := cc1[fmt.Sprintf("%s_greatdbCheck_%s", wttds.schema, wttds.table)]
	//查询该表的列名和列信息
	idxc := dbExec.IndexColumnStruct{
		Schema:      wttds.schema,
		Table:       wttds.table,
		TableColumn: tableColInfo.SColumnInfo,
		Sqlwhere:    sqlwhere[wttds.sdrive],
		Drivce:      wttds.sdrive,
	}
	alog := fmt.Sprintf("(%d) Start to query the block data of check table %s.%s ...", logThreadSeq, wttds.schema, wttds.table)
	global.Wlog.Info(alog)

	sdb := sdbPool.Get(logThreadSeq)
	stt, _ := idxc.TableIndexColumn().GeneratingQueryCriteria(sdb, logThreadSeq)
	sdbPool.Put(sdb, logThreadSeq)

	idxc.Drivce = wttds.ddrive
	idxc.Sqlwhere = sqlwhere[wttds.ddrive]
	idxc.TableColumn = tableColInfo.DColumnInfo
	ddb := ddbPool.Get(logThreadSeq)
	dtt, _ := idxc.TableIndexColumn().GeneratingQueryCriteria(ddb, logThreadSeq)
	ddbPool.Put(ddb, logThreadSeq)

	clog := fmt.Sprintf("(%d) Check table %s.%s to start checking the consistency of block data ...", logThreadSeq, wttds.schema, wttds.table)
	global.Wlog.Info(clog)
	if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
		dlog := fmt.Sprintf("(%d) Verification table %s.%s The block data verified by the original target end is inconsistent. query sql is {%s}.", logThreadSeq, wttds.schema, wttds.table, sqlwhere)
		global.Wlog.Warn(dlog)
		differencesData.Table = wttds.table
		differencesData.Schema = wttds.schema
		differencesData.SqlWhere = sqlwhere
		differencesData.TableColumnInfo = cc1
		differencesData.indexColumnType = wttds.indexColumnType
	} else {
		elog := fmt.Sprintf("(%d) Verification table %s.%s The block data verified by the original target end is consistent. query sql is {%s}.", logThreadSeq, wttds.schema, wttds.table, sqlwhere)
		global.Wlog.Info(elog)
		differencesData = InitDifferencesDataStruct()
	}
	zlog := fmt.Sprintf("(%d) The block data verification of check table %s.%s is completed !!!", logThreadSeq, wttds.schema, wttds.table)
	global.Wlog.Info(zlog)
	return differencesData
}

/*
	差异数据的二次校验，并生成修复语句
*/
func (wttds writeTmpTableDataStruct) AbnormalDataDispos(schema, table string, sqlwhere map[string]string, tableColInfo map[string]global.TableAllColumnInfoS, indexColumnType string, logThreadSeq int64) []string {
	var (
		aa           = &CheckSumTypeStruct{}
		strsqlSliect []string
	)

	colData := wttds.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", wttds.schema, wttds.table)]
	idxc := dbExec.IndexColumnStruct{
		Schema:      wttds.schema,
		Table:       wttds.table,
		TableColumn: colData.SColumnInfo,
		Sqlwhere:    sqlwhere[wttds.sdrive],
		Drivce:      wttds.sdrive,
	}
	alog := fmt.Sprintf("(%d) Check table %s.%s to start differential data processing and generate repair statements ...", logThreadSeq, wttds.schema, wttds.table)
	global.Wlog.Info(alog)

	sdb := wttds.sdbPool.Get(logThreadSeq)
	stt, _ := idxc.TableIndexColumn().GeneratingQueryCriteria(sdb, logThreadSeq)
	wttds.sdbPool.Put(sdb, logThreadSeq)

	idxc.Drivce = wttds.ddrive
	idxc.Sqlwhere = sqlwhere[wttds.ddrive]
	idxc.TableColumn = colData.DColumnInfo
	ddb := wttds.ddbPool.Get(logThreadSeq)
	dtt, _ := idxc.TableIndexColumn().GeneratingQueryCriteria(ddb, logThreadSeq)
	wttds.ddbPool.Put(ddb, logThreadSeq)

	//对不同数据库的的null处理
	if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
		if strings.Contains(stt, "/*go actions columnData*//*") {
			stt = strings.ReplaceAll(stt, "/*go actions columnData*//*", "/*go actions columnData*/<nil>/*")
		}
		if strings.Contains(dtt, "/*go actions columnData*//*") {
			dtt = strings.ReplaceAll(dtt, "/*go actions columnData*//*", "/*go actions columnData*/<nil>/*")
		}
	}
	if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
		add, del := aa.Arrcmp(strings.Split(stt, "/*go actions rowData*/"), strings.Split(dtt, "/*go actions rowData*/"))
		clog := fmt.Sprintf("(%d) There is difference data in check table %d.%d, start to generate repair statement.", logThreadSeq, schema, table)
		global.Wlog.Info(clog)
		if len(del) > 0 || len(add) > 0 {
			lock.Lock()
			differencesSchemaTable[fmt.Sprintf("%sgreatdbCheck_greatdbCheck%s", schema, table)] = wttds.datafixType
			lock.Unlock()
			dbf := dbExec.DataAbnormalFixStruct{Schema: schema, Table: table, ColData: colData.DColumnInfo, Sqlwhere: sqlwhere[wttds.ddrive], SourceDevice: wttds.ddrive, IndexColumnType: indexColumnType}
			if len(del) > 0 {
				dlog := fmt.Sprintf("(%d) Start to generate the delete statement of check table %s.%s.", logThreadSeq, schema, table)
				global.Wlog.Info(dlog)
				for _, i := range del {
					dbf.RowData = i
					sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, wttds.sdrive, logThreadSeq)
					//sqlstr, err := dbExec.DataFix().DataAbnormalFix(schema, table, i, colData.DColumnInfo, sqlwhere[wttds.ddrive], wttds.ddrive, indexColumnType).FixDeleteSqlExec(ddb, wttds.sdrive)
					if err != nil {
						wttds.getErr(fmt.Sprintf("dest: checkSum table %s.%s generate delete sql error.", schema, table), err)
					}
					strsqlSliect = append(strsqlSliect, sqlstr)
				}
				elog := fmt.Sprintf("(%d) The delete repair statement verifying table %s.%s is complete. The delete statement is {%s}", logThreadSeq, schema, table, strsqlSliect)
				global.Wlog.Info(elog)
			}
			if len(add) > 0 {
				dlog := fmt.Sprintf("(%d) Start to generate the insert statement of check table %s.%s.", logThreadSeq, schema, table)
				global.Wlog.Info(dlog)
				for _, i := range add {
					dbf.RowData = i
					sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, wttds.sdrive, logThreadSeq)
					//sqlstr, err := dbExec.DataFix().DataAbnormalFix(schema, table, i, colData.DColumnInfo, sqlwhere[wttds.ddrive], wttds.ddrive, indexColumnType).FixInsertSqlExec(ddb, wttds.sdrive)
					if err != nil {
						wttds.getErr(fmt.Sprintf("dest: checkSum table %s.%s generate insert sql error.", schema, table), err)
					}
					strsqlSliect = append(strsqlSliect, sqlstr)
				}
				elog := fmt.Sprintf("(%d) The insert repair statement verifying table %s.%s is complete. The delete statement is {%s}", logThreadSeq, schema, table, strsqlSliect)
				global.Wlog.Info(elog)
			}
		}
	}
	zlog := fmt.Sprintf("(%d) Check table %s.%s to complete differential data processing and generate repair statements. !!!", logThreadSeq, wttds.schema, wttds.table)
	global.Wlog.Info(zlog)
	return strsqlSliect
}

func (wttds writeTmpTableDataStruct) DataFixDispos(sqlstr []string, logThreadSeq int64) {
	alog := fmt.Sprintf("(%d) The current table %s.%s processes the repair statement on the target side.", logThreadSeq, wttds.schema, wttds.table)
	global.Wlog.Info(alog)
	ddb := wttds.ddbPool.Get(logThreadSeq)
	for _, i := range sqlstr {
		ApplyDataFix(i, ddb, wttds.datafixType, wttds.sfile, wttds.ddrive, logThreadSeq)
	}
	wttds.ddbPool.Put(ddb, logThreadSeq)
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
		limitPag                                                       = make(chan string, queueDepth)
	)
	/*
		获取校验表的索引列数据、索引列长度,校验表的所有列信息
	*/
	//获取索引列数据长度，处理索引列数据中有null或空字符串的问题
	var selectColumnStringM = make(map[string]string)
	var lengthTrimM = make(map[string]string)
	var columnLengthAsM = make(map[string][]string)
	rand.Seed(time.Now().UnixNano())
	logThreadSeq := rand.Int63()
	idxc := dbExec.IndexColumnStruct{Schema: wttds.schema, Table: wttds.table, ColumnName: wttds.columnName, ChanrowCount: wttds.chanrowCount, Drivce: wttds.sdrive}
	selectColumnStringM[wttds.sdrive], columnLengthAsM[wttds.sdrive], lengthTrimM[wttds.sdrive] = idxc.TableIndexColumn().TmpTableIndexColumnDataLength(logThreadSeq)
	idxc.Drivce = wttds.ddrive
	selectColumnStringM[wttds.ddrive], columnLengthAsM[wttds.ddrive], lengthTrimM[wttds.ddrive] = idxc.TableIndexColumn().TmpTableIndexColumnDataLength(logThreadSeq)

	//查询表的所有列及列的序号，为生成修复语句使用（生成delete语句）
	go wttds.LimiterSeq(limitPag, limitPagDone)
	go wttds.indexColUniqProduct(limitPag, indexColData, limitPagDone, IndexColumnDone, selectColumnStringM, lengthTrimM, columnLengthAsM, wttds.concurrency)
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
