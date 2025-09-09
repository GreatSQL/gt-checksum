package actions

import (
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"math/rand"
	"strings"
	"time"
)

/*
	单表的数据循环校验
*/
func (sp *SchedulePlan) sampSingleTableCheckProcessing(chanrowCount int, sampDataGroupNumber uint64, logThreadSeq int64) {
	var (
		vlog          string
		beginSeq      uint64
		stt, dtt      string
		err           error
		Cycles        uint64 //循环次数
		maxTableCount uint64
		md5Chan       = make(chan map[string]string, sp.mqQueueDepth)
		dataFixC      = make(chan map[string]string, sp.mqQueueDepth)
		noIndexC      = make(chan struct{}, sp.concurrency)
		tableRow      = make(chan int, sp.mqQueueDepth)
		//uniqMD5C      = make(chan map[string]string, 1)
		rowEnd bool
		//sqlStrExec    = make(chan string, sp.mqQueueDepth)
	)
	fmt.Println(fmt.Sprintf("begin checkSum no index table %s.%s", sp.schema, sp.table))
	vlog = fmt.Sprintf("(%d) Start to verify the data of the original target end of the non-indexed table %s.%s ...", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
	var A, B uint64
	idxc := dbExec.IndexColumnStruct{Drivce: sp.sdrive, Schema: sp.schema, Table: sp.table, ColumnName: sp.columnName, ChanrowCount: chanrowCount}
	sdb := sp.sdbPool.Get(int64(logThreadSeq))
	A, err = idxc.TableIndexColumn().TableRows(sdb, int64(logThreadSeq))
	sp.sdbPool.Put(sdb, int64(logThreadSeq))

	ddb := sp.ddbPool.Get(int64(logThreadSeq))
	B, err = idxc.TableIndexColumn().TableRows(ddb, int64(logThreadSeq))
	sp.ddbPool.Put(ddb, int64(logThreadSeq))
	//var barTableRow int64
	//if A >= B {
	//	barTableRow = int64(A)
	//} else {
	//	barTableRow = int64(B)
	//}
	//sp.bar.NewOption(0, barTableRow)
	fmt.Println(A, B)
	pods := Pod{Schema: sp.schema, Table: sp.table,
		IndexColumn:    "noIndex",
		CheckMode:    sp.checkMod,
		DIFFS: "no",
		Datafix:     sp.datafixType,
	}

	uniqMD5C := sp.AbDataMd5Unique(md5Chan, int64(logThreadSeq))
	sqlStrExec := sp.DataFixSql(dataFixC, &pods, int64(logThreadSeq))
	//检测临时文件，并按照一定条件读取
	go func() {
		for {
			select {
			case ic := <-uniqMD5C:
				FileOperate{File: sp.file, BufSize: 1024, fileName: sp.TmpFileName}.ConcurrencyReadFile(ic, dataFixC)
				dataFixC <- map[string]string{"END": "end"}
				close(dataFixC)
				return
			}
		}
	}()

	//统计表的总行数
	go func() {
		var cc int
		for {
			if rowEnd && len(noIndexC) == 0 {
				return
			}
			select {
			case tr := <-tableRow:
				cc++
				maxTableCount += uint64(tr)
			}
		}
	}()
	FileOper := FileOperate{File: sp.file, BufSize: 1024 * 4 * 1024, fileName: sp.TmpFileName}
	for {
		if rowEnd && len(noIndexC) == 0 {
			md5Chan <- map[string]string{"END": "end"}
			break
		}
		if rowEnd {
			continue
		}
		if beginSeq%sampDataGroupNumber != 0 {
			continue
		}
		Cycles++
		noIndexC <- struct{}{}
		go func(a, beginSeq uint64) {
			defer func() {
				<-noIndexC
			}()
			vlog = fmt.Sprintf("(%d) There is currently no index table %s.%s, and the %d md5 check of the data consistency of the original target is started.", logThreadSeq, sp.schema, sp.table, Cycles)
			global.Wlog.Debug(vlog)
			stt, dtt, err = sp.QueryTableData(beginSeq, Cycles, chanrowCount, int64(logThreadSeq))
			if err != nil {
				fmt.Println(err)
				return
			}
			slength := len(strings.Split(stt, "/*go actions rowData*/"))
			dlength := len(strings.Split(dtt, "/*go actions rowData*/"))
			if stt == dtt && stt == "" {
				rowEnd = true
				return
			}
			if slength >= dlength {
				tableRow <- slength
			} else {
				tableRow <- dlength
			}
			sp.QueryDataCheckSum(stt, dtt, md5Chan, FileOper, Cycles, logThreadSeq)
			vlog = fmt.Sprintf("(%d) There is currently no index table %s.%s The %d round of data cycle verification is complete.", logThreadSeq, sp.schema, sp.table, Cycles)
			global.Wlog.Debug(vlog)
		}(Cycles, beginSeq)
		beginSeq = beginSeq + uint64(chanrowCount)
		time.Sleep(500 * time.Millisecond)
		//if beginSeq < uint64(barTableRow) {
		//	sp.bar.Play(int64(beginSeq))
		//} else {
		//	sp.bar.Play(barTableRow)
		//}
	}
	//sp.bar.Finish()
	sp.FixSqlExec(sqlStrExec, int64(logThreadSeq))
	//输出校验结果信息
	pods.Rows = fmt.Sprintf("%v", maxTableCount)
	measuredDataPods = append(measuredDataPods, pods)
	vlog = fmt.Sprintf("(%d) No index table %s.%s The data consistency check of the original target end is completed", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
	fmt.Println(fmt.Sprintf("%s.%s 校验完成", sp.schema, sp.table))
}

/*
	做数据的抽样检查，先使用count*，针对count*数量一致的表在进行部分数据的抽样检查
*/
func (sp *SchedulePlan) DoSampleDataCheck() {
	var (
		stmpTableCount, dtmpTableCount uint64
		chanrowCount                   int
		err                            error
		vlog                           string
		queueDepth                     = sp.mqQueueDepth
		sqlWhere                       = make(chanString, queueDepth)
		selectSql                      = make(chanMap, queueDepth)
		diffQueryData                  = make(chanDiffDataS, queueDepth)
		fixSQL                         = make(chanString, queueDepth)
	)
	rand.Seed(time.Now().UnixNano())
	logThreadSeq := rand.Int63()
	vlog = fmt.Sprintf("(%d) Start the sampling data verification of the original target...", logThreadSeq)
	global.Wlog.Info(vlog)
	for k, v := range sp.tableIndexColumnMap {
		if sp.checkNoIndexTable == "no" && len(v) == 0 {
			continue
		}
		//输出校验结果信息
		sp.pods = &Pod{
			CheckObject: sp.checkObject,
			CheckMode:    sp.checkMod,
			DIFFS: "no",
		}
		if strings.Contains(k, "/*indexColumnType*/") {
			ki := strings.Split(k, "/*indexColumnType*/")[0]
			sp.pods.IndexColumn = strings.TrimLeft(strings.Join(v, ","), ",")
			sp.indexColumnType = strings.Split(k, "/*indexColumnType*/")[1]
			if strings.Contains(ki, "/*greatdbSchemaTable*/") {
				sp.schema = strings.Split(ki, "/*greatdbSchemaTable*/")[0]
				sp.table = strings.Split(ki, "/*greatdbSchemaTable*/")[1]
			}
		} else {
			sp.pods.IndexColumn = "no"
			if strings.Contains(k, "/*greatdbSchemaTable*/") {
				sp.schema = strings.Split(k, "/*greatdbSchemaTable*/")[0]
				sp.table = strings.Split(k, "/*greatdbSchemaTable*/")[1]
			}
		}
		sp.pods.Schema = sp.schema
		sp.pods.Table = sp.table
		fmt.Println(fmt.Sprintf("begin checkSum table %s.%s", sp.schema, sp.table))
		tableColumn := sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", sp.schema, sp.table)]
		//根据索引列数量决定chanrowCount数
		sp.chanrowCount = sp.chunkSize
		sp.columnName = v
		//统计表的总行数
		sdb := sp.sdbPool.Get(logThreadSeq)
		//查询原目标端的表总行数，并生成调度计划
		idxc := dbExec.IndexColumnStruct{Schema: sp.schema, Table: sp.table, ColumnName: sp.columnName, Drivce: sp.sdrive}
		stmpTableCount, err = idxc.TableIndexColumn().TmpTableIndexColumnRowsCount(sdb, logThreadSeq)
		sp.sdbPool.Put(sdb, logThreadSeq)
		if err != nil {
			return
		}
		ddb := sp.ddbPool.Get(logThreadSeq)
		idxc.Drivce = sp.ddrive
		dtmpTableCount, err = idxc.TableIndexColumn().TmpTableIndexColumnRowsCount(ddb, logThreadSeq)
		if err != nil {
			return
		}
		sp.ddbPool.Put(ddb, logThreadSeq)

		vlog = fmt.Sprintf("(%d) Start to verify the total number of rows of table %s.%s source and target ...", logThreadSeq, sp.schema, sp.table)
		global.Wlog.Debug(vlog)
		if stmpTableCount != dtmpTableCount {
			vlog = fmt.Sprintf("(%d) Verify that the total number of rows at the source and destination of table %s.%s is inconsistent.", logThreadSeq, sp.schema, sp.table)
			global.Wlog.Debug(vlog)
			sp.pods.DIFFS = "yes"
			sp.pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)
			measuredDataPods = append(measuredDataPods, *sp.pods)
			vlog = fmt.Sprintf("(%d) Check table %s.%s The total number of rows at the source and target end has been checked.", logThreadSeq, sp.schema, sp.table)
			global.Wlog.Debug(vlog)
			fmt.Println()
			fmt.Println(fmt.Sprintf("table %s.%s checksum complete", sp.schema, sp.table))
			continue
		}
		vlog = fmt.Sprintf("(%d) Verify that the total number of rows at the source and destination of table %s.%s is consistent", logThreadSeq, sp.schema, sp.table)
		global.Wlog.Debug(vlog)
		var sampDataGroupNumber, dataGroupNumber uint64
		var selectColumnStringM = make(map[string]map[string]string)
		dataGroupNumber = stmpTableCount / uint64(sp.chanrowCount)
		if (stmpTableCount/uint64(sp.chanrowCount))%uint64(sp.chanrowCount) > 0 {
			dataGroupNumber = dataGroupNumber + 1
		}
		sp.sampDataGroupNumber = int64(stmpTableCount / 100 * uint64(sp.ratio) / uint64(sp.chanrowCount))
		if (stmpTableCount/100*uint64(sp.ratio))%uint64(sp.chanrowCount) > 0 {
			sp.sampDataGroupNumber = sp.sampDataGroupNumber + 1
		}
		sp.tableMaxRows = stmpTableCount
		sp.pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)
		sp.pods.Sample = fmt.Sprintf("%d,%d", stmpTableCount, stmpTableCount/100*uint64(sp.ratio))

		if len(v) == 0 {
			sp.pods.IndexColumn = "noIndex"
			sp.sampSingleTableCheckProcessing(sp.chanrowCount, sampDataGroupNumber, logThreadSeq)
			//measuredDataPods = append(measuredDataPods, pods)
			fmt.Println()
			fmt.Println(fmt.Sprintf("table %s.%s checksum complete", sp.schema, sp.table))
			vlog = fmt.Sprintf("(%d) Check table %s.%s The total number of rows at the source and target end has been checked.", logThreadSeq, sp.schema, sp.table)
			global.Wlog.Debug(vlog)
			continue
		}
		//开始校验有索引表
		sp.pods.IndexColumn = strings.TrimLeft(strings.Join(sp.columnName, ","), ",")
		//获取索引列数据长度，处理索引列数据中有null或空字符串的问题
		idxc = dbExec.IndexColumnStruct{Schema: sp.schema, Table: sp.table, ColumnName: sp.columnName,
			ChanrowCount: chanrowCount, Drivce: sp.sdrive,
			ColData: sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", sp.schema, sp.table)].SColumnInfo}
		selectColumnStringM[sp.sdrive] = idxc.TableIndexColumn().TmpTableIndexColumnSelectDispos(logThreadSeq)
		idxc.Drivce = sp.ddrive
		selectColumnStringM[sp.ddrive] = idxc.TableIndexColumn().TmpTableIndexColumnSelectDispos(logThreadSeq)

		var scheduleCount = make(chan int64, 1)
		go sp.recursiveIndexColumn(sqlWhere, sdb, ddb, 0, sp.chanrowCount, "", selectColumnStringM, logThreadSeq)
		
		go sp.queryTableData(selectSql, diffQueryData, tableColumn, scheduleCount, logThreadSeq)
		go sp.AbnormalDataDispos(diffQueryData, fixSQL, logThreadSeq)
		sp.DataFixDispos(fixSQL, logThreadSeq)
		fmt.Println()
		fmt.Println(fmt.Sprintf("table %s.%s checksum complete", sp.schema, sp.table))
		vlog = fmt.Sprintf("(%d) Check table %s.%s The total number of rows at the source and target end has been checked.", logThreadSeq, sp.schema, sp.table)
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) The sampling data verification of the original target is completed !!!", logThreadSeq)
	global.Wlog.Info(vlog)
}
