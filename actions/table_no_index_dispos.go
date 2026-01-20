package actions

import (
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"strings"
	"sync"
	"time"
)

// 包级变量，用于存储已处理的SQL语句，实现跨函数调用的去重
var globalSqlMap sync.Map

/*
Perform MD5 verification on different data rows and remove duplicate values
*/
func (sp *SchedulePlan) AbDataMd5Unique(md5Chan <-chan map[string]string, logThreadSeq int64) chan map[string]string {
	var A = make(chan map[string]string, 1)
	var tmpAnDateMap = make(map[string]string)
	var md5Mutex sync.Mutex // 添加互斥锁保护并发访问
	go func() {
		for {
			select {
			case i, ok1 := <-md5Chan:
				if !ok1 {
					A <- tmpAnDateMap
					close(A)
					return
				} else {
					md5Mutex.Lock() // 加锁保护map操作
					for k, v := range i {
						if l, ok := tmpAnDateMap[k]; ok && v != l {
							delete(tmpAnDateMap, k)
						} else {
							tmpAnDateMap[k] = v
						}
					}
					md5Mutex.Unlock() // 解锁
				}
			}
		}
	}()
	return A
}

/*
Row count statistics for tables without indexes
*/
func (sp *SchedulePlan) NoIndexTableCount(logThreadSeq int64) int64 {
	var (
		A, B uint64
		err  error
	)
	// Get valid column names
	var columnNames []string
	if len(sp.columnName) > 0 {
		columnNames = sp.columnName
	} else if cols, ok := sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.schema, sp.table)]; ok {
		// Get all column names from table structure information
		for _, colInfo := range cols.SColumnInfo {
			if name, ok := colInfo["COLUMN_NAME"]; ok {
				columnNames = append(columnNames, name)
			} else if name, ok := colInfo["columnName"]; ok {
				// Try using lowercase key name
				columnNames = append(columnNames, name)
			}
		}
	}

	// If no column names, use "0" as default value
	if len(columnNames) == 0 {
		vlog := fmt.Sprintf("(%d) Warning: Table %s has no valid column names, using default column", logThreadSeq, sp.table)
		global.Wlog.Warn(vlog)
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
Perform MD5 verification on different data rows and remove duplicate values
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

		// Get valid column names for index columns
		var indexColumns []string
		if len(sp.columnName) > 0 {
			// If column names already exist, use them directly
			indexColumns = sp.columnName
		} else {
			// Otherwise get all column names from table structure as conditions
			for _, colInfo := range colData.DColumnInfo {
				if colName, ok := colInfo["columnName"]; ok {
					indexColumns = append(indexColumns, colName)
				} else if colName, ok := colInfo["COLUMN_NAME"]; ok {
					// Try using uppercase key name
					indexColumns = append(indexColumns, colName)
				}
			}
			// Log information
			vlog = fmt.Sprintf("(%d) Table %s has no index, using all columns as conditions, found %d columns", logThreadSeq, displayTableName, len(indexColumns))
			global.Wlog.Debug(vlog)

			// If still no column names, log warning and use default value
			if len(indexColumns) == 0 {
				vlog = fmt.Sprintf("(%d) Warning: Unable to get column names from table structure, will use default column", logThreadSeq, displayTableName)
				global.Wlog.Warn(vlog)
				// Add a default column to avoid subsequent processing failure
				indexColumns = []string{"id"}
			}
		}

		dbf := dbExec.DataAbnormalFixStruct{
			Schema:                  sp.destSchema,   // 使用目标schema而不是原始schema
			SourceSchema:            sp.sourceSchema, // 添加源schema用于处理映射关系
			Table:                   sp.table,
			ColData:                 colData.DColumnInfo,
			DestDevice:              sp.ddrive,
			CaseSensitiveObjectName: sp.caseSensitiveObjectName,
			DatafixType:             sp.datafixType,
			IndexColumn:             indexColumns, // 添加索引列信息
		}
		for {
			select {
			case v, ok := <-tmpAnDateMap:
				if !ok {
					if len(noIndexD) == 0 {
						close(sqlStrExec)
						return
					}
				} else {
					// 不要在循环外声明rowData和sqlType变量，避免变量覆盖
					for ki, vi := range v {
						// 在循环内声明变量，避免变量覆盖问题
						rowData := ki
						sqlType := vi
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

							// Ensure IndexColumn is not empty
							if len(dbf.IndexColumn) == 0 {
								vlog = fmt.Sprintf("(%d) Warning: Table %s has no index columns, trying to use all columns as conditions", logThreadSeq, displayTableName)
								global.Wlog.Warn(vlog)

								// Get all column names from table structure
								for _, colInfo := range colData.DColumnInfo {
									if colName, ok := colInfo["columnName"]; ok {
										dbf.IndexColumn = append(dbf.IndexColumn, colName)
									} else if colName, ok := colInfo["COLUMN_NAME"]; ok {
										// Try using uppercase key name
										dbf.IndexColumn = append(dbf.IndexColumn, colName)
									}
								}

								// If still no column names, log error and use default column
								if len(dbf.IndexColumn) == 0 {
									vlog = fmt.Sprintf("(%d) Error: Unable to get column names from table structure, will use default column", logThreadSeq, displayTableName)
									global.Wlog.Error(vlog)
									// Add a default column to avoid subsequent processing failure
									dbf.IndexColumn = []string{"id"}
									// Ensure RowData is not empty
									if dbf.RowData == "" {
										dbf.RowData = "id=0"
									}
								}
							}

							sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
							if err != nil {
								vlog = fmt.Sprintf("(%d) Failed to generate DELETE statement: %v", logThreadSeq, err)
								global.Wlog.Error(vlog)
								return
							}
							if sqlstr != "" {
								// 使用sqlstr作为键进行去重，确保同一SQL语句只被处理一次
								if _, loaded := globalSqlMap.LoadOrStore(sqlstr, true); !loaded {
									sqlStrExec <- sqlstr
								}
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

							// 确保IndexColumn不为空
							if len(dbf.IndexColumn) == 0 {
								vlog = fmt.Sprintf("(%d) Warn：table %s has no index column, try to using all columns", logThreadSeq, displayTableName)
								global.Wlog.Warn(vlog)

								// 从表结构中获取所有列名
								for _, colInfo := range colData.DColumnInfo {
									if colName, ok := colInfo["columnName"]; ok {
										dbf.IndexColumn = append(dbf.IndexColumn, colName)
									} else if colName, ok := colInfo["COLUMN_NAME"]; ok {
										// 尝试使用大写键名
										dbf.IndexColumn = append(dbf.IndexColumn, colName)
									}
								}

								// 如果仍然没有列名，记录错误并使用默认列
								if len(dbf.IndexColumn) == 0 {
									vlog = fmt.Sprintf("(%d) Error：can not obtain columns from table structure, will use default column", logThreadSeq, displayTableName)
									global.Wlog.Error(vlog)
									// 添加一个默认列，避免后续处理失败
									dbf.IndexColumn = []string{"id"}
									// 确保RowData不为空
									if dbf.RowData == "" {
										dbf.RowData = "id=0"
									}
								}
							}

							sqlstr, err := dbf.DataAbnormalFix().FixInsertSqlExec(ddb, sp.ddrive, logThreadSeq)
							if err != nil {
								vlog = fmt.Sprintf("(%d) Failed to generate INSERT statement: %v", logThreadSeq, err)
								global.Wlog.Error(vlog)
								return
							}
							if sqlstr != "" {
								// 使用sqlstr作为键进行去重，确保同一SQL语句只被处理一次
								if _, loaded := globalSqlMap.LoadOrStore(sqlstr, true); !loaded {
									sqlStrExec <- sqlstr
								}
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
Perform MD5 verification on different data rows and remove duplicate values
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
	dbf := dbExec.DataAbnormalFixStruct{Schema: sp.schema, Table: sp.table, ColData: colData.DColumnInfo, SourceDevice: sp.ddrive, CaseSensitiveObjectName: sp.caseSensitiveObjectName}
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
Query data from tables without indexes
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
Perform MD5 verification on query strings, process differences if strings don't match
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
		// 注意：Arrcmp函数的第一个参数是源端数据，第二个参数是目标端数据
		// added是源端有但目标端没有的数据，需要插入到目标端
		// deleted是目标端有但源端没有的数据，需要从目标端删除
		// 注意：Arrcmp函数的第一个参数是源端数据，第二个参数是目标端数据
		// added是源端有但目标端没有的数据，需要插入到目标端
		// deleted是目标端有但源端没有的数据，需要从目标端删除
		// Arrcmp函数的第一个参数是源端数据，第二个参数是目标端数据
		// 返回值：
		// - added: 源端有但目标端没有的数据，需要插入到目标端
		// - deleted: 目标端有但源端没有的数据，需要从目标端删除
		added, deleted := aa.Arrcmp(strings.Split(stt, "/*go actions rowData*/"), strings.Split(dtt, "/*go actions rowData*/"))
		if len(deleted) > 0 {
			tmpAnDateMap = make(map[string]string)
			displayTableName := sp.getDisplayTableName()
			vlog = fmt.Sprintf("(%d) Processing redundant data for table %s", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
			FileOpen.SqlType = "delete"
			md5Slice, err := FileOpen.ConcurrencyWriteFile(deleted)
			if err != nil {
				return
			}
			for _, deli := range md5Slice {
				tmpAnDateMap[deli] = "delete"
			}
			md5chan <- tmpAnDateMap
			displayTableName = sp.getDisplayTableName()
			vlog = fmt.Sprintf("(%d) Redundant data processed for table %s", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
		}
		if len(added) > 0 {
			tmpAnDateMap = make(map[string]string)
			displayTableName := sp.getDisplayTableName()
			vlog = fmt.Sprintf("(%d) Processing missing data for table %s", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
			FileOpen.SqlType = "insert"
			md5Slice, err := FileOpen.ConcurrencyWriteFile(added)
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

// Read temporary files for tables without indexes and return difference data
func (sp *SchedulePlan) noIndexTableAbdataRead(uniqMD5C chan map[string]string, logThreadSeq int64) chan map[string]string {
	var dataFixC = make(chan map[string]string, sp.mqQueueDepth)
	//Detect temporary files and read according to certain conditions
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
Single table data cycle verification
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
		IndexColumn: "NULL",
		CheckObject: sp.checkObject,
		DIFFS:       "no",
		Datafix:     sp.datafixType,
	}
	sp.bar.NewOption(0, barTableRow, "rows")
	//Count the total number of rows in the table
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

	//Deduplicate
	uniqMD5C := sp.AbDataMd5Unique(md5Chan, logThreadSeq)
	//Read files based on deduplicated data to find differences
	dataFixC := sp.noIndexTableAbdataRead(uniqMD5C, logThreadSeq)
	sqlStrExec := sp.DataFixSql(dataFixC, &pods, logThreadSeq)

	FileOper := FileOperate{File: sp.file, BufSize: 1024 * 4 * 1024, fileName: sp.TmpFileName}
	//Loop through table rows and perform data verification
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
	//Output verification result information
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

// getExactRowCount Query the exact number of rows in a table
func (sp *SchedulePlan) getExactRowCount(dbPool *global.Pool, schema, table string, logThreadSeq int64) int64 {
	db := dbPool.Get(logThreadSeq)
	defer dbPool.Put(db, logThreadSeq)

	// Handle schema name mapping
	var targetSchema string
	if dbPool == sp.sdbPool {
		// Source uses original schema
		targetSchema = schema
	} else if dbPool == sp.ddbPool {
		// Target checks if there is a mapping relationship
		if mappedSchema, exists := sp.tableMappings[schema]; exists {
			targetSchema = mappedSchema
		} else {
			targetSchema = schema
		}
	}

	// Ensure schema is not empty
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
