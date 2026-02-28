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
func (sp *SchedulePlan) sampSingleTableCheckProcessing(chanrowCount int, logThreadSeq int64) {
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
		rowEnd        bool
	)
	fmt.Printf("Starting checksum for table without index %s.%s\n", sp.schema, sp.table)
	vlog = fmt.Sprintf("(%d) Verifying data for table without index %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
	idxc := dbExec.IndexColumnStruct{Drivce: sp.sdrive, Schema: sp.schema, Table: sp.table, ColumnName: sp.columnName, ChanrowCount: chanrowCount}
	sdb := sp.sdbPool.Get(int64(logThreadSeq))
	_, err = idxc.TableIndexColumn().TableRows(sdb, int64(logThreadSeq))
	sp.sdbPool.Put(sdb, int64(logThreadSeq))

	ddb := sp.ddbPool.Get(int64(logThreadSeq))
	_, err = idxc.TableIndexColumn().TableRows(ddb, int64(logThreadSeq))
	sp.ddbPool.Put(ddb, int64(logThreadSeq))

	pods := Pod{Schema: sp.schema, Table: sp.table,
		IndexColumn: "NULL",
		CheckObject: "struct", // 设置CheckObject字段为"struct"
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
		// 不再使用采样逻辑，始终校验所有数据
		Cycles++
		noIndexC <- struct{}{}
		go func(a, beginSeq uint64) {
			defer func() {
				<-noIndexC
			}()
			vlog = fmt.Sprintf("(%d) Starting MD5 checksum round %d for table without index %s.%s", logThreadSeq, Cycles, sp.schema, sp.table)
			global.Wlog.Debug(vlog)
			stt, dtt, err = sp.QueryTableData(beginSeq, Cycles, chanrowCount, int64(logThreadSeq))
			if err != nil {
				global.Wlog.Error(fmt.Sprintf("(%d) Error occurred: %v", logThreadSeq, err))
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
			vlog = fmt.Sprintf("(%d) Completed MD5 checksum round %d for table without index %s.%s", logThreadSeq, Cycles, sp.schema, sp.table)
			global.Wlog.Debug(vlog)
		}(Cycles, beginSeq)
		beginSeq = beginSeq + uint64(chanrowCount)
		time.Sleep(500 * time.Millisecond)
		sp.FixSqlExec(sqlStrExec, int64(logThreadSeq))
		//输出校验结果信息
		pods.Rows = fmt.Sprintf("%v", maxTableCount)
		measuredDataPods = append(measuredDataPods, pods)
		vlog = fmt.Sprintf("(%d) Completed data checksum for table without index %s.%s", logThreadSeq, sp.schema, sp.table)
		global.Wlog.Info(vlog)
		global.Wlog.Info(fmt.Sprintf("(%d) Checksum completed for %s.%s", logThreadSeq, sp.schema, sp.table))
	}
}

/*
做数据的全量校验，先使用count*，针对count*数量一致的表再进行全量数据校验
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
	logThreadSeq := rand.Int63()
	vlog = fmt.Sprintf("(%d) Starting sampling data checksum", logThreadSeq)
	global.Wlog.Info(vlog)

	// 添加调试日志，显示表索引映射
	vlog = fmt.Sprintf("Table index column mapping options: %v", sp.tableIndexColumnMap)
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
			vlog = fmt.Sprintf("(%d) Unable to parse table information from key: %s", logThreadSeq, k)
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

		global.Wlog.Info(fmt.Sprintf("(%d) Starting checksum for table %s", logThreadSeq, displayTableName))

		// 使用正确的键名查找表列信息
		tableColumnKey := fmt.Sprintf("%s_gtchecksum_%s", destSchema, destTable)
		tableColumn, exists := sp.tableAllCol[tableColumnKey]
		if !exists {
			vlog = fmt.Sprintf("(%d) Column information not available for %s", logThreadSeq, tableColumnKey)
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
			vlog = fmt.Sprintf("(%d) Failed to retrieve source table row count: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return
		}

		ddb := sp.ddbPool.Get(logThreadSeq)
		//查询目标端的表总行数
		idxcDest := dbExec.IndexColumnStruct{Schema: destSchema, Table: destTable, ColumnName: sp.columnName, Drivce: sp.ddrive}
		dtmpTableCount, err = idxcDest.TableIndexColumn().TmpTableIndexColumnRowsCount(ddb, logThreadSeq)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Failed to retrieve target table row count: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return
		}
		sp.ddbPool.Put(ddb, logThreadSeq)

		vlog = fmt.Sprintf("(%d) Verifying row counts for table %s", logThreadSeq, displayTableName)
		global.Wlog.Debug(vlog)

		if stmpTableCount != dtmpTableCount {
			vlog = fmt.Sprintf("(%d) Row counts differ for table %s", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
			sp.pods.DIFFS = "yes"
			sp.pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)
			measuredDataPods = append(measuredDataPods, *sp.pods)
			vlog = fmt.Sprintf("(%d) Row count checksum completed for table %s", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
			fmt.Println()
			global.Wlog.Info(fmt.Sprintf("(%d) Checksum completed for table %s", logThreadSeq, displayTableName))
			continue
		}

		vlog = fmt.Sprintf("(%d) Row counts match for table %s", logThreadSeq, displayTableName)
		global.Wlog.Debug(vlog)

		var dataGroupNumber uint64
		var selectColumnStringM = make(map[string]map[string]string)
		dataGroupNumber = stmpTableCount / uint64(sp.chanrowCount)
		if (stmpTableCount/uint64(sp.chanrowCount))%uint64(sp.chanrowCount) > 0 {
			dataGroupNumber = dataGroupNumber + 1
		}
		// 始终使用全量校验
		sp.tableMaxRows = stmpTableCount
		sp.pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)

		if len(v) == 0 {
			sp.pods.IndexColumn = "NULL"
			sp.sampSingleTableCheckProcessing(sp.chanrowCount, logThreadSeq) // 移除sampDataGroupNumber参数
			fmt.Println()
			global.Wlog.Info(fmt.Sprintf("Table %s checksum completed", displayTableName))
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
		global.Wlog.Info(fmt.Sprintf("Table %s checksum completed", displayTableName))
		vlog = fmt.Sprintf("(%d) Check table %s The total number of rows at the source and target end has been checked.", logThreadSeq, displayTableName)
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) Sampling data checksum completed", logThreadSeq)
	global.Wlog.Info(vlog)
}
