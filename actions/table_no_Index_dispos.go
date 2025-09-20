package actions

import (
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"strings"
	"time"
)

/*
针对差异数据行做md5校验，并去除重复值
*/
func (sp *SchedulePlan) AbDataMd5Unique(md5Chan <-chan map[string]string, logThreadSeq int64) chan map[string]string {
	var A = make(chan map[string]string, 1)
	var tmpAnDateMap = make(map[string]string)
	go func() {
		for {
			select {
			case i, ok1 := <-md5Chan:
				if !ok1 {
					A <- tmpAnDateMap
					close(A)
					return
				} else {
					for k, v := range i {
						if l, ok := tmpAnDateMap[k]; ok && v != l {
							delete(tmpAnDateMap, k)
						} else {
							tmpAnDateMap[k] = v
						}
					}
				}
			}
		}
	}()
	return A
}

/*
无索引表的表统计信息行数
*/
func (sp *SchedulePlan) NoIndexTableCount(logThreadSeq int64) int64 {
	var (
		A, B uint64
		err  error
	)
	// 获取有效的列名
	var columnNames []string
	if len(sp.columnName) > 0 {
		columnNames = sp.columnName
	} else if cols, ok := sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.schema, sp.table)]; ok {
		// 从表结构信息中获取所有列名
		for _, colInfo := range cols.SColumnInfo {
			if name, ok := colInfo["COLUMN_NAME"]; ok {
				columnNames = append(columnNames, name)
			}
		}
	}

	// 如果没有列名，使用"0"作为默认值
	if len(columnNames) == 0 {
		columnNames = []string{"0"}
	}

	idxc := dbExec.IndexColumnStruct{Drivce: sp.sdrive, Schema: sp.sourceSchema, Table: sp.table, ColumnName: columnNames, ChanrowCount: sp.chanrowCount}

	sdb := sp.sdbPool.Get(int64(logThreadSeq))
	A, err = idxc.TableIndexColumn().TableRows(sdb, int64(logThreadSeq))
	if err != nil {
		return 0
	}
	sp.sdbPool.Put(sdb, int64(logThreadSeq))

	ddb := sp.ddbPool.Get(int64(logThreadSeq))
	idxcDest := dbExec.IndexColumnStruct{Drivce: sp.ddrive, Schema: sp.destSchema, Table: sp.table, ColumnName: columnNames, ChanrowCount: sp.chanrowCount}

	B, err = idxcDest.TableIndexColumn().TableRows(ddb, int64(logThreadSeq))
	if err != nil {
		return 0
	}
	sp.ddbPool.Put(ddb, int64(logThreadSeq))
	var barTableRow int64
	if A >= B {
		barTableRow = int64(A)
	} else {
		barTableRow = int64(B)
	}
	return barTableRow
}

/*
针对差异数据行做md5校验，并去除重复值
*/
func (sp *SchedulePlan) DataFixSql(tmpAnDateMap <-chan map[string]string, pods *Pod, logThreadSeq int64) chan string {
	var sqlStrExec = make(chan string, sp.mqQueueDepth)
	go func() {
		var (
			vlog     string
			noIndexD = make(chan struct{}, sp.concurrency)
		)
		displayTableName := sp.getDisplayTableName()
		vlog = fmt.Sprintf("(%d) Generating DELETE/INSERT statements for table %s", logThreadSeq, displayTableName)
		global.Wlog.Debug(vlog)
		colData := sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.schema, sp.table)]
		dbf := dbExec.DataAbnormalFixStruct{Schema: sp.schema, Table: sp.table, ColData: colData.DColumnInfo, DestDevice: sp.ddrive, DatafixType: sp.datafixType}
		for {
			select {
			case v, ok := <-tmpAnDateMap:
				if !ok {
					if len(noIndexD) == 0 {
						close(sqlStrExec)
						return
					}
				} else {
					var rowData, sqlType string
					for ki, vi := range v {
						rowData = ki
						sqlType = vi
						//noIndexD <- struct{}{}
						pods.DIFFS = "yes"
						dbf.IndexType = "mul"
						//go func() {
						//	defer func() {
						//		<-noIndexD
						//	}()
						ddb := sp.ddbPool.Get(logThreadSeq)
						if sqlType == "delete" {
							displayTableName := sp.getDisplayTableName()
							vlog = fmt.Sprintf("(%d) Generating DELETE repair statements for table %s", logThreadSeq, displayTableName)
							global.Wlog.Debug(vlog)
							dbf.RowData = rowData
							sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
							if err != nil {
								return
							}
							if sqlstr != "" {
								sqlStrExec <- sqlstr
							}
							displayTableName = sp.getDisplayTableName()
							vlog = fmt.Sprintf("(%d) DELETE repair statements generated for table %s", logThreadSeq, displayTableName)
							global.Wlog.Debug(vlog)
						}
						if sqlType == "insert" {
							displayTableName := sp.getDisplayTableName()
							vlog = fmt.Sprintf("(%d) Generating INSERT repair statements for table %s", logThreadSeq, displayTableName)
							global.Wlog.Debug(vlog)
							dbf.RowData = rowData
							sqlstr, err := dbf.DataAbnormalFix().FixInsertSqlExec(ddb, sp.ddrive, logThreadSeq)
							if err != nil {
								return
							}
							if sqlstr != "" {
								sqlStrExec <- sqlstr
							}
							displayTableName = sp.getDisplayTableName()
							vlog = fmt.Sprintf("(%d) INSERT repair statements generated for table %s", logThreadSeq, displayTableName)
							global.Wlog.Debug(vlog)
						}
						sp.ddbPool.Put(ddb, logThreadSeq)
						//}()
					}
				}
			}
		}
	}()
	return sqlStrExec
}

/*
针对差异数据行做md5校验，并去除重复值
*/
func (sp *SchedulePlan) FixSqlExec(sqlStrExec <-chan string, logThreadSeq int64) {
	var (
		vlog     string
		sqlSlice []string
		noIndexD = make(chan struct{}, sp.concurrency)
		increSeq int
	)
	displayTableName := sp.getDisplayTableName()
	vlog = fmt.Sprintf("(%d) Start to generate delete and insert sql statements for table %s.", logThreadSeq, displayTableName)
	global.Wlog.Debug(vlog)
	colData := sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.schema, sp.table)]
	dbf := dbExec.DataAbnormalFixStruct{Schema: sp.schema, Table: sp.table, ColData: colData.DColumnInfo, SourceDevice: sp.ddrive}
	dbf.IndexColumnType = "mul"
	for {
		select {
		case v, ok := <-sqlStrExec:
			if !ok {
				if len(noIndexD) == 0 {
					if len(sqlSlice) > 0 {
						ApplyDataFix(sqlSlice, sp.datafixType, sp.sfile, sp.ddrive, sp.djdbc, logThreadSeq)
						displayTableName := sp.getDisplayTableName()
						vlog = fmt.Sprintf("(%d) The delete repair sql statements of table %s are generated.", logThreadSeq, displayTableName)
						global.Wlog.Debug(vlog)
						sqlSlice = []string{}
					} else {
						return
					}
				}
			} else {
				increSeq++
				sqlSlice = append(sqlSlice, v)
				if increSeq == sp.fixTrxNum {
					var sqlSlice1 []string
					for _, i := range sqlSlice {
						sqlSlice1 = append(sqlSlice1, i)
					}
					sqlSlice = []string{}
					noIndexD <- struct{}{}
					increSeq = 0
					go func(a []string) {
						defer func() {
							<-noIndexD
						}()
						ApplyDataFix(a, sp.datafixType, sp.sfile, sp.ddrive, sp.djdbc, logThreadSeq)
						displayTableName := sp.getDisplayTableName()
						vlog = fmt.Sprintf("(%d) The delete repair sql statements of table %s are generated.", logThreadSeq, displayTableName)
						global.Wlog.Debug(vlog)
					}(sqlSlice1)
				}
			}
		}
	}
}

/*
查询无索引表数据
*/
func (sp *SchedulePlan) QueryTableData(beginSeq uint64, chunkSeq uint64, chanrowCount int, logThreadSeq int64) (string, string, error) {
	var (
		vlog     string
		err      error
		stt, dtt string
	)
	displayTableName := sp.getDisplayTableName()
	vlog = fmt.Sprintf("(%d) Starting data checksum for table without index %s", logThreadSeq, displayTableName)
	global.Wlog.Debug(vlog)
	noIndexOrderCol := sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.schema, sp.table)]
	idxc := dbExec.IndexColumnStruct{Drivce: sp.sdrive, Schema: sp.sourceSchema, Table: sp.table, TableColumn: noIndexOrderCol.SColumnInfo, ChanrowCount: chanrowCount}
	//allColumns := idxc.TableIndexColumn().NoIndexOrderBySingerColumn(noIndexOrderCol.SColumnInfo)
	sdb := sp.sdbPool.Get(logThreadSeq)
	stt, err = idxc.TableIndexColumn().NoIndexGeneratingQueryCriteria(sdb, beginSeq, chanrowCount, logThreadSeq)
	sp.sdbPool.Put(sdb, logThreadSeq)
	if err != nil {
		return "", "", err
	}
	idxcDest := dbExec.IndexColumnStruct{Drivce: sp.ddrive, Schema: sp.destSchema, Table: sp.table, TableColumn: noIndexOrderCol.DColumnInfo, ChanrowCount: chanrowCount}
	ddb := sp.ddbPool.Get(logThreadSeq)
	dtt, err = idxcDest.TableIndexColumn().NoIndexGeneratingQueryCriteria(ddb, beginSeq, chanrowCount, logThreadSeq)
	if err != nil {
		return "", "", err
	}

	sp.ddbPool.Put(ddb, logThreadSeq)
	return stt, dtt, nil
}

/*
针对查询的字符串进行md5校验，字符串不一致则进行差异处理
*/
func (sp *SchedulePlan) QueryDataCheckSum(stt, dtt string, md5chan chan<- map[string]string, FileOpen FileOperate, chunkSeq uint64, logThreadSeq int64) {
	var (
		vlog         string
		aa           = &CheckSumTypeStruct{}
		tmpAnDateMap = make(map[string]string)
	)
	displayTableName := sp.getDisplayTableName()
	vlog = fmt.Sprintf("(%d) Verifying data blocks for table without index %s", logThreadSeq, displayTableName)
	global.Wlog.Debug(vlog)
	if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
		displayTableName := sp.getDisplayTableName()
		vlog = fmt.Sprintf("(%d) MD5 checksum mismatch in round %d for table without index %s", logThreadSeq, chunkSeq, displayTableName)
		global.Wlog.Debug(vlog)
		add, del := aa.Arrcmp(strings.Split(stt, "/*go actions rowData*/"), strings.Split(dtt, "/*go actions rowData*/"))
		if len(del) > 0 {
			tmpAnDateMap = make(map[string]string)
			displayTableName := sp.getDisplayTableName()
			vlog = fmt.Sprintf("(%d) Processing redundant data for table %s", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
			FileOpen.SqlType = "delete"
			md5Slice, err := FileOpen.ConcurrencyWriteFile(del)
			if err != nil {
				return
			}
			//md5Slice := FileOperate{File: sp.file, BufSize: 1024 * 4 * 1024, SqlType: "delete"}.ConcurrencyWriteFile(del)
			for _, deli := range md5Slice {
				tmpAnDateMap[deli] = "delete"
			}
			md5chan <- tmpAnDateMap
			displayTableName = sp.getDisplayTableName()
			vlog = fmt.Sprintf("(%d) Redundant data processed for table %s", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
		}
		if len(add) > 0 {
			tmpAnDateMap = make(map[string]string)
			displayTableName := sp.getDisplayTableName()
			vlog = fmt.Sprintf("(%d) Processing missing data for table %s", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
			//md5Slice := FileOperate{File: sp.file, BufSize: 1024 * 4 * 1024, SqlType: "insert", fileName: sp.TmpFileName}.ConcurrencyWriteFile(add)
			FileOpen.SqlType = "insert"
			md5Slice, err := FileOpen.ConcurrencyWriteFile(add)
			if err != nil {
				return
			}
			for _, addi := range md5Slice {
				tmpAnDateMap[addi] = "insert"
			}
			md5chan <- tmpAnDateMap
			displayTableName = sp.getDisplayTableName()
			vlog = fmt.Sprintf("(%d) Missing data processed for table %s", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
		}
	}
	displayTableName = sp.getDisplayTableName()
	vlog = fmt.Sprintf("(%d) Data block checksum completed for table without index %s", logThreadSeq, displayTableName)
	global.Wlog.Debug(vlog)
}

// 无索引表读取临时文件，并返回差异的数据
func (sp *SchedulePlan) noIndexTableAbdataRead(uniqMD5C chan map[string]string, logThreadSeq int64) chan map[string]string {
	var dataFixC = make(chan map[string]string, sp.mqQueueDepth)
	//检测临时文件，并按照一定条件读取
	go func() {
		for {
			select {
			case ic, ok := <-uniqMD5C:
				if !ok {
					close(dataFixC)
					return
				} else {
					FileOperate{BufSize: 1024, fileName: sp.TmpFileName}.ConcurrencyReadFile(ic, dataFixC)
				}
			}
		}
	}()
	return dataFixC
}

/*
单表的数据循环校验
*/
func (sp *SchedulePlan) SingleTableCheckProcessing(chanrowCount int, logThreadSeq int64) {
	var (
		vlog          string
		beginSeq      uint64
		stt, dtt      string
		err           error
		Cycles        uint64 //循环次数
		maxTableCount uint64
		md5Chan       = make(chan map[string]string, sp.mqQueueDepth)
		noIndexC      = make(chan struct{}, sp.concurrency)
		tableRow      = make(chan int, sp.mqQueueDepth)
		rowEnd        bool
	)

	displayTableName := sp.getDisplayTableName()

	fmt.Printf("Starting checksum for table without index %s\n", displayTableName)
	vlog = fmt.Sprintf("(%d) Verifying data for table without index %s", logThreadSeq, displayTableName)
	global.Wlog.Info(vlog)
	barTableRow := sp.NoIndexTableCount(logThreadSeq)
	pods := Pod{Schema: sp.schema, Table: sp.table,
		IndexColumn: "noIndex",
		CheckMode:   sp.checkMod,
		DIFFS:       "no",
		Datafix:     sp.datafixType,
	}
	sp.bar.NewOption(0, barTableRow, "rows")
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

	//去重
	uniqMD5C := sp.AbDataMd5Unique(md5Chan, logThreadSeq)
	//根据去重后的数据读取文件，找出差异数据
	dataFixC := sp.noIndexTableAbdataRead(uniqMD5C, logThreadSeq)
	sqlStrExec := sp.DataFixSql(dataFixC, &pods, logThreadSeq)

	FileOper := FileOperate{File: sp.file, BufSize: 1024 * 4 * 1024, fileName: sp.TmpFileName}
	//循环读取表行数，并进行数据校验
	for {
		if rowEnd && len(noIndexC) == 0 {
			close(md5Chan)
			break
		}
		if rowEnd {
			continue
		}
		Cycles++
		noIndexC <- struct{}{}
		go func(a, beginSeq uint64) {
			defer func() {
				<-noIndexC
			}()
			displayTableName := sp.getDisplayTableName()
			vlog = fmt.Sprintf("(%d) Starting MD5 checksum round %d for table without index %s", logThreadSeq, Cycles, displayTableName)
			global.Wlog.Debug(vlog)
			stt, dtt, err = sp.QueryTableData(beginSeq, Cycles, chanrowCount, int64(logThreadSeq))
			if err != nil {
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
			displayTableName = sp.getDisplayTableName()
			vlog = fmt.Sprintf("(%d) Completed MD5 checksum round %d for table without index %s", logThreadSeq, Cycles, displayTableName)
			global.Wlog.Debug(vlog)
		}(Cycles, beginSeq)
		beginSeq = beginSeq + uint64(chanrowCount)
		time.Sleep(500 * time.Millisecond)
		if beginSeq < uint64(barTableRow) {
			sp.bar.Play(int64(beginSeq))
		} else {
			sp.bar.Play(barTableRow)
		}
	}
	sp.bar.Finish()
	sp.FixSqlExec(sqlStrExec, int64(logThreadSeq))
	//输出校验结果信息
	// 重新查询精确行数
	sourceExactCount := sp.getExactRowCount(sp.sdbPool, sp.schema, sp.table, logThreadSeq)
	targetExactCount := sp.getExactRowCount(sp.ddbPool, sp.schema, sp.table, logThreadSeq)
	pods.Rows = fmt.Sprintf("%d,%d", sourceExactCount, targetExactCount)
	measuredDataPods = append(measuredDataPods, pods)
	displayTableName = sp.getDisplayTableName()
	vlog = fmt.Sprintf("(%d) Data checksum completed for table without index %s", logThreadSeq, displayTableName)
	global.Wlog.Info(vlog)
	fmt.Printf("%s checksum completed\n", displayTableName)
}

// getExactRowCount 查询表的精确行数
func (sp *SchedulePlan) getExactRowCount(dbPool *global.Pool, schema, table string, logThreadSeq int64) int64 {
	db := dbPool.Get(logThreadSeq)
	defer dbPool.Put(db, logThreadSeq)

	// 处理库名映射
	var targetSchema string
	if dbPool == sp.sdbPool {
		// 源端使用原始schema
		targetSchema = schema
	} else if dbPool == sp.ddbPool {
		// 目标端检查是否有映射关系
		if mappedSchema, exists := sp.tableMappings[schema]; exists {
			targetSchema = mappedSchema
		} else {
			targetSchema = schema
		}
	}

	// 确保schema不为空
	if targetSchema == "" {
		vlog := fmt.Sprintf("(%d) Using default schema for table %s", logThreadSeq, table)
		global.Wlog.Warn(vlog)

		if dbPool == sp.sdbPool {
			targetSchema = sp.sourceSchema
		} else if dbPool == sp.ddbPool {
			targetSchema = sp.destSchema
		}

		if targetSchema == "" {
			vlog = fmt.Sprintf("(%d) Cannot determine schema for table %s", logThreadSeq, table)
			global.Wlog.Error(vlog)
			return 0
		}
	}

	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", targetSchema, table)
	vlog := fmt.Sprintf("(%d) Executing row count query: %s", logThreadSeq, query)
	global.Wlog.Debug(vlog)

	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		vlog = fmt.Sprintf("(%d) Failed to get row count for %s.%s: %v", logThreadSeq, targetSchema, table, err)
		global.Wlog.Error(vlog)
		return 0
	}

	vlog = fmt.Sprintf("(%d) Row count for %s.%s: %d", logThreadSeq, targetSchema, table, count)
	global.Wlog.Debug(vlog)

	return count
}
