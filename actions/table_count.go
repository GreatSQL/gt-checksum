package actions

import (
	"fmt"
	"greatdbCheck/dbExec"
	"greatdbCheck/global"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func (sp *SchedulePlan) getErr(msg string, err error) {
	if err != nil {
		fmt.Println(err, ":", msg)
		os.Exit(1)
	}
}

/*
	使用count(1)的方式进行校验
*/
func (sp *SchedulePlan) DoCountDataCheck() {
	var schema, table string
	rand.Seed(time.Now().UnixNano())
	logThreadSeq := rand.Int63()
	alog := fmt.Sprintf("(%d) Start the table validation for the total number of rows ...", logThreadSeq)
	global.Wlog.Info(alog)
	for k, v := range sp.tableIndexColumnMap {
		if strings.Contains(k, "/*indexColumnType*/") {
			ki := strings.Split(k, "/*indexColumnType*/")[0]
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
		blog := fmt.Sprintf("(%d) Check table %s.%s initialization single check row number.", logThreadSeq, schema, table)
		global.Wlog.Info(blog)
		var chanrowCount int
		switch len(v) {
		case 0:
			chanrowCount = 0
		case 1:
			chanrowCount = sp.singleIndexChanRowCount
		default:
			chanrowCount = sp.jointIndexChanRowCount
		}
		clog := fmt.Sprintf("(%d) The current verification table %s.%s single verification row number is [%d]", logThreadSeq, schema, table, chanrowCount)
		global.Wlog.Info(clog)
		sdb := sp.sdbConnPool.Get(logThreadSeq)
		//查询原目标端的表总行数，并生成调度计划
		idxc := dbExec.IndexColumnStruct{Schema: schema, Table: table, ColumnName: sp.columnName, Drivce: sp.sdrive}
		stmpTableCount, _ := idxc.TableIndexColumn().TmpTableRowsCount(sdb, logThreadSeq)

		sp.sdbConnPool.Put(sdb, logThreadSeq)
		ddb := sp.ddbConnPool.Get(logThreadSeq)
		idxc.Drivce = sp.ddrive
		dtmpTableCount, _ := idxc.TableIndexColumn().TmpTableRowsCount(ddb, logThreadSeq)
		sp.ddbConnPool.Put(ddb, logThreadSeq)

		//输出校验结果信息
		var pods = Pod{
			Schema:   schema,
			Table:    table,
			IndexCol: strings.Join(v, ","),
			CheckMod: sp.checkMod,
			Datafix:  sp.datafixType,
		}
		if chanrowCount == 0 {
			pods.IndexCol = "no"
		}
		dlog := fmt.Sprintf("(%d) Start to verify the total number of rows of table %s.%s source and target ...", logThreadSeq, schema, table)
		global.Wlog.Info(dlog)
		if stmpTableCount == dtmpTableCount {
			elog := fmt.Sprintf("(%d) Verify that the total number of rows at the source and destination of table %s.%s is consistent", logThreadSeq, schema, table)
			global.Wlog.Info(elog)
			pods.Differences = "no"
			pods.Rows = strconv.Itoa(stmpTableCount)
		} else {
			flog := fmt.Sprintf("(%d) Verify that the total number of rows at the source and destination of table %s.%s is inconsistent.", logThreadSeq, schema, table)
			global.Wlog.Warn(flog)
			pods.Differences = "yes"
			pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)
		}
		measuredDataPods = append(measuredDataPods, pods)
		glog := fmt.Sprintf("(%d) Check table %s.%s The total number of rows at the source and target end has been checked.", logThreadSeq, schema, table)
		global.Wlog.Info(glog)
	}
	zlog := fmt.Sprintf("(%d) The total number of rows in the check table has been checked !!!", logThreadSeq)
	global.Wlog.Info(zlog)
}

func (sp *SchedulePlan) dataDisposCheck(schema, table string, chanrowCount int, noIndexC, noIndexD chan struct{}, logThreadSeq int64) {
	var (
		aa = &CheckSumTypeStruct{}
	)

	fmt.Println(fmt.Sprintf("begin checkSum no index table %s.%s", schema, table))
	//alog := fmt.Sprintf("(%d) Start to verify the data of the original target end of the non-indexed table %s.%s ...", logThreadSeq, schema,table)
	//global.Wlog.Info(alog)
	global.Wlog.Info(fmt.Sprintf("begin checkSum no index %s.%s", schema, table))
	//global.Wlog.Info(fmt.Sprintf("checkSum table %s.%s use chanorwCount Value is %d", schema, table, chanrowCount))
	//
	//1、获取当前源目标端表的总行数
	sdb := sp.sdbConnPool.Get(logThreadSeq)
	idxc := dbExec.IndexColumnStruct{Drivce: sp.sdrive, Schema: schema, Table: table, ColumnName: sp.columnName, ChanrowCount: chanrowCount}
	stmpTableCount, _ := idxc.TableIndexColumn().TmpTableRowsCount(sdb, logThreadSeq)
	sp.sdbConnPool.Put(sdb, logThreadSeq)

	idxc.Drivce = sp.ddrive
	ddb := sp.ddbConnPool.Get(logThreadSeq)
	dtmpTableCount, _ := idxc.TableIndexColumn().TmpTableRowsCount(ddb, logThreadSeq)
	sp.ddbConnPool.Put(ddb, logThreadSeq)

	var maxTableCount, schedulePlanCount int
	if stmpTableCount > dtmpTableCount || stmpTableCount == dtmpTableCount {
		maxTableCount = stmpTableCount
	} else {
		maxTableCount = dtmpTableCount
	}
	if maxTableCount%chanrowCount != 0 {
		schedulePlanCount = maxTableCount/chanrowCount + 1
	} else {
		schedulePlanCount = maxTableCount / chanrowCount
	}
	blog := fmt.Sprintf("(%d) There is currently no index table %s.%s, the number of rows to be verified at a time is %d, and the number of rows to be verified is %d times", logThreadSeq, schema, table, chanrowCount, schedulePlanCount)
	global.Wlog.Info(blog)
	//输出校验结果信息
	measuredDataPods = append(measuredDataPods, Pod{
		Schema:      schema,
		Table:       table,
		IndexCol:    "noIndex",
		CheckMod:    sp.checkMod,
		Rows:        strconv.Itoa(maxTableCount),
		Differences: "no",
		Datafix:     sp.datafixType,
	})

	//2、生成查询计划
	var beginSeq int
	var tmpAnDateMap = make(map[string]string)

	clog := fmt.Sprintf("(%d) There is currently no index table %s.%s, and the cycle check data is started.", logThreadSeq, schema, table)
	global.Wlog.Info(clog)
	noIndexOrderCol := sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", schema, table)]
	idxc.Drivce = sp.sdrive
	orderBySignerColumns := idxc.TableIndexColumn().NoIndexOrderBySingerColumn(noIndexOrderCol.SColumnInfo)

	for i := 1; i <= schedulePlanCount; i++ {
		noIndexD <- struct{}{}
		dlog := fmt.Sprintf("(%d) There is currently no index table %s.%s, and the %d data cycle check is started", logThreadSeq, schema, table, i)
		global.Wlog.Info(dlog)
		idxc.Drivce = sp.sdrive
		sdb = sp.sdbConnPool.Get(logThreadSeq)
		stt, _ := idxc.TableIndexColumn().NoIndexGeneratingQueryCriteria(sdb, beginSeq, chanrowCount, orderBySignerColumns, logThreadSeq)
		sp.sdbConnPool.Put(sdb, logThreadSeq)

		idxc.Drivce = sp.ddrive
		ddb = sp.ddbConnPool.Get(logThreadSeq)
		dtt, _ := idxc.TableIndexColumn().NoIndexGeneratingQueryCriteria(ddb, beginSeq, chanrowCount, orderBySignerColumns, logThreadSeq)
		sp.ddbConnPool.Put(ddb, logThreadSeq)

		elog := fmt.Sprintf("(%d) There is currently no index table %s.%s, and the %d md5 check of the data consistency of the original target is started.", logThreadSeq, schema, table, i)
		global.Wlog.Info(elog)
		if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
			flog := fmt.Sprintf("(%d) There is currently no index table %s.%s. The %d md5 check of the data consistency of the original target is abnormal.", logThreadSeq, schema, table, i)
			global.Wlog.Warn(flog)
			add, del := aa.Arrcmp(strings.Split(stt, "/*go actions rowData*/"), strings.Split(dtt, "/*go actions rowData*/"))
			differencesSchemaTable[fmt.Sprintf("%sgreatdbCheck_greatdbCheck%s", schema, table)] = sp.datafixType
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
		glog := fmt.Sprintf("(%d) There is currently no index table %s.%s The %d round of data cycle verification is complete.", logThreadSeq, schema, table, i)
		global.Wlog.Info(glog)
		beginSeq = beginSeq + chanrowCount
		<-noIndexD
	}
	hlog := fmt.Sprintf("(%d) There is currently no index table %s.%s cycle check data completed.", logThreadSeq, schema, table)
	global.Wlog.Info(hlog)

	colData := sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", schema, table)]
	dbf := dbExec.DataAbnormalFixStruct{Schema: schema, Table: table, ColData: colData.DColumnInfo, SourceDevice: sp.ddrive}
	global.Wlog.Info(dbf)
	for ki, vi := range tmpAnDateMap {
		noIndexD <- struct{}{}
		indexColumnType := "mui"
		go func() {
			defer func() {
				<-noIndexD
			}()
			if vi == "delete" {
				dlog := fmt.Sprintf("(%d) Start to generate the delete statement of check table %s.%s.", logThreadSeq, schema, table)
				global.Wlog.Info(dlog)
				dbf.RowData = ki
				dbf.IndexColumnType = indexColumnType
				sqlstr, _ := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.sdrive, logThreadSeq)
				ApplyDataFix(sqlstr, ddb, sp.datafixType, sp.sfile, sp.ddrive, logThreadSeq)
			}
			if vi == "insert" {
				dlog := fmt.Sprintf("(%d) Start to generate the insert statement of check table %s.%s.", logThreadSeq, schema, table)
				global.Wlog.Info(dlog)
				dbf.RowData = ki
				dbf.IndexColumnType = indexColumnType
				sqlstr, _ := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.sdrive, logThreadSeq)
				ApplyDataFix(sqlstr, ddb, sp.datafixType, sp.sfile, sp.ddrive, logThreadSeq)
			}
		}()

	}
	<-noIndexC
	xlog := fmt.Sprintf("(%d) No index table %s.%s The data consistency check of the original target end is completed", logThreadSeq, schema, table)
	global.Wlog.Info(xlog)
	fmt.Println(fmt.Sprintf("%s.%s 校验完成", schema, table))
}

/*
	无索引表的处理方式
*/
func (sp *SchedulePlan) DoNoIndexDataCheck(noIndexC chan struct{}) {
	var noIndexD = make(chan struct{}, sp.Concurrency)
	rand.Seed(time.Now().UnixNano())
	for k, v := range sp.tableIndexColumnMap {
		time.Sleep(time.Nanosecond * 2)
		logThreadSeq := rand.Int63()
		var schema, table string
		var chanrowCount int
		if strings.Contains(k, "/*indexColumnType*/") {
			ki := strings.Split(k, "/*indexColumnType*/")[0]
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
		switch len(v) {
		case 0:
			chanrowCount = 1000
		case 1:
			chanrowCount = sp.singleIndexChanRowCount
		default:
			chanrowCount = sp.jointIndexChanRowCount
		}
		if len(v) != 0 {
			continue
		}
		noIndexC <- struct{}{}
		go sp.dataDisposCheck(schema, table, chanrowCount, noIndexC, noIndexD, logThreadSeq)
	}
	ylog := fmt.Sprintf("The data consistency check of the non-indexed table is completed.!!!")
	global.Wlog.Info(ylog)
	for {
		time.Sleep(time.Second)
		if len(noIndexD) == 0 {
			close(noIndexD)
			break
		}
	}
}

/*
	做数据的抽样检查，先使用count*，针对count*数量一致的表在进行部分数据的抽样检查
*/
func (sp *SchedulePlan) DoSampleDataCheck() {
	//if _, err := os.Stat(sp.datafixSql); err == nil {
	//	os.Remove(sp.datafixSql)
	//}
	var chanrowCount int
	wd := &writeTmpTableDataStruct{
		sdbPool:             sp.sdbConnPool,
		sdrive:              sp.sdrive,
		ddrive:              sp.ddrive,
		ddbPool:             sp.ddbConnPool,
		tableIndexColumnMap: sp.tableIndexColumnMap,
		datafixType:         sp.datafixType,
		sfile:               sp.sfile,
		queueDepth:          sp.mqQueueDepth,
		concurrency:         sp.Concurrency,
		chanrowCount:        sp.jointIndexChanRowCount,
		tableAllCol:         sp.tableAllCol,
		checkMod:            sp.checkMod,
		ratio:               sp.ratio,
	}
	rand.Seed(time.Now().UnixNano())
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

		//wd.schema = schema
		//wd.table = table
		if len(v) == 0 {
			continue
		}
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
		//wd.doIndexDataCheck()
		var (
			queueDepth                                                     = sp.mqQueueDepth
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
		time.Sleep(time.Nanosecond * 2)
		logThreadSeq := rand.Int63()
		idxc := dbExec.IndexColumnStruct{Schema: schema, Table: table, ColumnName: sp.columnName, ChanrowCount: chanrowCount, Drivce: sp.sdrive}
		selectColumnStringM[sp.sdrive], columnLengthAsM[sp.sdrive], lengthTrimM[sp.sdrive] = idxc.TableIndexColumn().TmpTableIndexColumnDataLength(logThreadSeq)
		idxc.Drivce = sp.ddrive
		selectColumnStringM[sp.ddrive], columnLengthAsM[sp.ddrive], lengthTrimM[sp.ddrive] = idxc.TableIndexColumn().TmpTableIndexColumnDataLength(logThreadSeq)

		//查询表的所有列及列的序号，为生成修复语句使用（生成delete语句）
		go wd.SampLimiterSeq(limitPag, limitPagDone)
		go wd.indexColUniqProduct(limitPag, indexColData, limitPagDone, IndexColumnDone, selectColumnStringM, lengthTrimM, columnLengthAsM, wd.concurrency)
		go wd.IndexColumnProduct(indexColData, selectWhere, IndexColumnDone, sqlwhereDone, wd.concurrency)
		go wd.SqlwhereProduct(selectWhere, sqlWhere, sqlwhereDone, queryDataDone, wd.tableAllCol, wd.concurrency)
		go wd.QueryTableDataProduct(sqlWhere, differencesData, queryDataDone, differdone, wd.tableAllCol, wd.concurrency)
		go wd.AbnormalDataProduct(differencesData, dataFix, differdone, dataFixdone, wd.concurrency)
		go wd.DataFixDataProduct(dataFix, dataFixdone, done, wd.concurrency)

		for {
			select {
			case _, ok := <-done:
				if ok {
					fmt.Println(fmt.Sprintf("%s.%s 校验完成", schema, table))
					return
				}
			}
		}
	}
}
