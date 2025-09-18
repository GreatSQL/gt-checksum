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
		IndexColumn: "noIndex",
		CheckMode:   sp.checkMod,
		DIFFS:       "no",
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

	// 添加调试日志，显示表索引映射
	vlog = fmt.Sprintf("DoSampleDataCheck tableIndexColumnMap keys: %v", sp.tableIndexColumnMap)
	global.Wlog.Debug(vlog)

	for k, v := range sp.tableIndexColumnMap {
		if sp.checkNoIndexTable == "no" && len(v) == 0 {
			continue
		}

		// 解析键值，提取源表和目标表信息
		var sourceSchema, sourceTable, destSchema, destTable string
		var indexColumnType string

		// 检查是否包含映射信息
		if strings.Contains(k, "/*mapping*/") {
			// 新格式: "sourceSchema/*gtchecksumSchemaTable*/sourceTable/*mapping*/destSchema/*mappingTable*/destTable"
			// 或者: "sourceSchema/*gtchecksumSchemaTable*/sourceTable/*indexColumnType*/indexType/*mapping*/destSchema/*mappingTable*/destTable"

			if strings.Contains(k, "/*indexColumnType*/") {
				parts := strings.Split(k, "/*indexColumnType*/")
				ki := parts[0]
				indexColumnType = strings.Split(parts[1], "/*mapping*/")[0]

				if strings.Contains(ki, "/*gtchecksumSchemaTable*/") {
					sourceSchema = strings.Split(ki, "/*gtchecksumSchemaTable*/")[0]
					sourceTable = strings.Split(ki, "/*gtchecksumSchemaTable*/")[1]

					mappingPart := strings.Split(parts[1], "/*mapping*/")[1]
					if strings.Contains(mappingPart, "/*mappingTable*/") {
						destSchema = strings.Split(mappingPart, "/*mappingTable*/")[0]
						destTable = strings.Split(mappingPart, "/*mappingTable*/")[1]
					}
				}
			} else {
				ki := k
				if strings.Contains(ki, "/*gtchecksumSchemaTable*/") {
					sourceSchema = strings.Split(ki, "/*gtchecksumSchemaTable*/")[0]
					remainingPart := strings.Split(ki, "/*gtchecksumSchemaTable*/")[1]

					if strings.Contains(remainingPart, "/*mapping*/") {
						sourceTable = strings.Split(remainingPart, "/*mapping*/")[0]
						mappingPart := strings.Split(remainingPart, "/*mapping*/")[1]

						if strings.Contains(mappingPart, "/*mappingTable*/") {
							destSchema = strings.Split(mappingPart, "/*mappingTable*/")[0]
							destTable = strings.Split(mappingPart, "/*mappingTable*/")[1]
						}
					}
				}
			}
		} else {
			// 旧格式处理
			if strings.Contains(k, "/*indexColumnType*/") {
				parts := strings.Split(k, "/*indexColumnType*/")
				ki := parts[0]
				indexColumnType = parts[1]

				if strings.Contains(ki, "/*gtchecksumSchemaTable*/") {
					sourceSchema = strings.Split(ki, "/*gtchecksumSchemaTable*/")[0]
					sourceTable = strings.Split(ki, "/*gtchecksumSchemaTable*/")[1]
					destSchema = sourceSchema
					destTable = sourceTable
				}
			} else {
				if strings.Contains(k, "/*gtchecksumSchemaTable*/") {
					sourceSchema = strings.Split(k, "/*gtchecksumSchemaTable*/")[0]
					sourceTable = strings.Split(k, "/*gtchecksumSchemaTable*/")[1]
					destSchema = sourceSchema
					destTable = sourceTable
				}
			}
		}

		// 如果解析失败，跳过此项
		if sourceSchema == "" || sourceTable == "" {
			vlog = fmt.Sprintf("(%d) Failed to parse table information from key: %s", logThreadSeq, k)
			global.Wlog.Warn(vlog)
			continue
		}

		// 如果目标表信息为空，使用源表信息
		if destSchema == "" || destTable == "" {
			destSchema = sourceSchema
			destTable = sourceTable
		}

		// 构建显示名称，包含映射关系
		displayTableName := sourceSchema + "." + sourceTable
		if sourceSchema != destSchema || sourceTable != destTable {
			displayTableName = fmt.Sprintf("%s.%s:%s.%s", sourceSchema, sourceTable, destSchema, destTable)
		}

		//输出校验结果信息
		sp.pods = &Pod{
			CheckObject: sp.checkObject,
			CheckMode:   sp.checkMod,
			DIFFS:       "no",
			Schema:      sourceSchema,
			Table:       sourceTable,
		}

		// 如果存在映射关系，将映射信息添加到表名中
		if sourceSchema != destSchema || sourceTable != destTable {
			sp.pods.Table = fmt.Sprintf("%s:%s.%s", sp.pods.Table, destSchema, destTable)
		}

		if len(v) > 0 {
			sp.pods.IndexColumn = strings.TrimLeft(strings.Join(v, ","), ",")
			sp.indexColumnType = indexColumnType
		} else {
			sp.pods.IndexColumn = "no"
		}

		// 设置当前处理的表信息
		sp.schema = sourceSchema
		sp.table = sourceTable
		sp.sourceSchema = sourceSchema
		sp.destSchema = destSchema

		fmt.Println(fmt.Sprintf("begin checkSum table %s", displayTableName))

		// 使用正确的键名查找表列信息
		tableColumnKey := fmt.Sprintf("%s_gtchecksum_%s", destSchema, destTable)
		tableColumn, exists := sp.tableAllCol[tableColumnKey]
		if !exists {
			vlog = fmt.Sprintf("(%d) Table column information not found for %s", logThreadSeq, tableColumnKey)
			global.Wlog.Warn(vlog)
			continue
		}

		//根据索引列数量决定chanrowCount数
		sp.chanrowCount = sp.chunkSize
		sp.columnName = v

		//统计表的总行数
		sdb := sp.sdbPool.Get(logThreadSeq)
		//查询源端的表总行数
		idxc := dbExec.IndexColumnStruct{Schema: sourceSchema, Table: sourceTable, ColumnName: sp.columnName, Drivce: sp.sdrive}
		stmpTableCount, err = idxc.TableIndexColumn().TmpTableIndexColumnRowsCount(sdb, logThreadSeq)
		sp.sdbPool.Put(sdb, logThreadSeq)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Error getting source table row count: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return
		}

		ddb := sp.ddbPool.Get(logThreadSeq)
		//查询目标端的表总行数
		idxcDest := dbExec.IndexColumnStruct{Schema: destSchema, Table: destTable, ColumnName: sp.columnName, Drivce: sp.ddrive}
		dtmpTableCount, err = idxcDest.TableIndexColumn().TmpTableIndexColumnRowsCount(ddb, logThreadSeq)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Error getting destination table row count: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return
		}
		sp.ddbPool.Put(ddb, logThreadSeq)

		vlog = fmt.Sprintf("(%d) Start to verify the total number of rows of table %s source and target ...", logThreadSeq, displayTableName)
		global.Wlog.Debug(vlog)

		if stmpTableCount != dtmpTableCount {
			vlog = fmt.Sprintf("(%d) Verify that the total number of rows at the source and destination of table %s is inconsistent.", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
			sp.pods.DIFFS = "yes"
			sp.pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)
			measuredDataPods = append(measuredDataPods, *sp.pods)
			vlog = fmt.Sprintf("(%d) Check table %s The total number of rows at the source and target end has been checked.", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
			fmt.Println()
			fmt.Println(fmt.Sprintf("table %s checksum complete", displayTableName))
			continue
		}

		vlog = fmt.Sprintf("(%d) Verify that the total number of rows at the source and destination of table %s is consistent", logThreadSeq, displayTableName)
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
			fmt.Println()
			fmt.Println(fmt.Sprintf("table %s checksum complete", displayTableName))
			vlog = fmt.Sprintf("(%d) Check table %s The total number of rows at the source and target end has been checked.", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
			continue
		}

		//开始校验有索引表
		sp.pods.IndexColumn = strings.TrimLeft(strings.Join(sp.columnName, ","), ",")
		//获取索引列数据长度，处理索引列数据中有null或空字符串的问题
		idxc = dbExec.IndexColumnStruct{
			Schema:       sourceSchema,
			Table:        sourceTable,
			ColumnName:   sp.columnName,
			ChanrowCount: chanrowCount,
			Drivce:       sp.sdrive,
			ColData:      tableColumn.SColumnInfo,
		}
		selectColumnStringM[sp.sdrive] = idxc.TableIndexColumn().TmpTableIndexColumnSelectDispos(logThreadSeq)

		idxcDest = dbExec.IndexColumnStruct{
			Schema:       destSchema,
			Table:        destTable,
			ColumnName:   sp.columnName,
			ChanrowCount: chanrowCount,
			Drivce:       sp.ddrive,
			ColData:      tableColumn.DColumnInfo,
		}
		selectColumnStringM[sp.ddrive] = idxcDest.TableIndexColumn().TmpTableIndexColumnSelectDispos(logThreadSeq)

		var scheduleCount = make(chan int64, 1)
		go sp.recursiveIndexColumn(sqlWhere, sdb, ddb, 0, sp.chanrowCount, "", selectColumnStringM, logThreadSeq)

		go sp.queryTableDataSeparate(selectSql, make(chanMap), diffQueryData, tableColumn, scheduleCount, logThreadSeq)
		go sp.AbnormalDataDispos(diffQueryData, fixSQL, logThreadSeq)
		sp.DataFixDispos(fixSQL, logThreadSeq)
		fmt.Println()
		fmt.Println(fmt.Sprintf("table %s checksum complete", displayTableName))
		vlog = fmt.Sprintf("(%d) Check table %s The total number of rows at the source and target end has been checked.", logThreadSeq, displayTableName)
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) The sampling data verification of the original target is completed !!!", logThreadSeq)
	global.Wlog.Info(vlog)
}
