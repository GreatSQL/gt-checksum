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
	var A, B uint64
	idxc := dbExec.IndexColumnStruct{Drivce: sp.sdrive, Schema: sp.schema, Table: sp.table, ColumnName: sp.columnName, ChanrowCount: sp.chanrowCount}
	sdb := sp.sdbPool.Get(int64(logThreadSeq))
	A, err = idxc.TableIndexColumn().TableRows(sdb, int64(logThreadSeq))
	sp.sdbPool.Put(sdb, int64(logThreadSeq))

	ddb := sp.ddbPool.Get(int64(logThreadSeq))
	idxc.Drivce = sp.ddrive
	B, err = idxc.TableIndexColumn().TableRows(ddb, int64(logThreadSeq))
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
		vlog = fmt.Sprintf("(%d) Start to generate delete and insert sql statements for table %s.%s.", logThreadSeq, sp.schema, sp.table)
		global.Wlog.Debug(vlog)
		colData := sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", sp.schema, sp.table)]
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
						pods.Differences = "yes"
						dbf.IndexType = "mui"
						//go func() {
						//	defer func() {
						//		<-noIndexD
						//	}()
						ddb := sp.ddbPool.Get(logThreadSeq)
						if sqlType == "delete" {
							vlog = fmt.Sprintf("(%d) Start to generate the delete of table %s.%s to repair the sql statement.", logThreadSeq, sp.schema, sp.table)
							global.Wlog.Debug(vlog)
							dbf.RowData = rowData
							sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
							if err != nil {
								return
							}
							if sqlstr != "" {
								sqlStrExec <- sqlstr
							}
							vlog = fmt.Sprintf("(%d) The delete repair sql statements of table %s.%s are generated.", logThreadSeq, sp.schema, sp.table)
							global.Wlog.Debug(vlog)
						}
						if sqlType == "insert" {
							vlog = fmt.Sprintf("(%d) Start to generate the insert of table %s.%s to repair the sql statement.", logThreadSeq, sp.schema, sp.table)
							global.Wlog.Debug(vlog)
							dbf.RowData = rowData
							sqlstr, err := dbf.DataAbnormalFix().FixInsertSqlExec(ddb, sp.ddrive, logThreadSeq)
							if err != nil {
								return
							}
							if sqlstr != "" {
								sqlStrExec <- sqlstr
							}
							vlog = fmt.Sprintf("(%d) The insert repair sql statements of table %s.%s are generated.", logThreadSeq, sp.schema, sp.table)
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
	vlog = fmt.Sprintf("(%d) Start to generate delete and insert sql statements for table %s.%s.", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Debug(vlog)
	colData := sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", sp.schema, sp.table)]
	dbf := dbExec.DataAbnormalFixStruct{Schema: sp.schema, Table: sp.table, ColData: colData.DColumnInfo, SourceDevice: sp.ddrive}
	dbf.IndexColumnType = "mui"
	for {
		select {
		case v, ok := <-sqlStrExec:
			if !ok {
				if len(noIndexD) == 0 {
					if len(sqlSlice) > 0 {
						ApplyDataFix(sqlSlice, sp.datafixType, sp.sfile, sp.ddrive, sp.djdbc, logThreadSeq)
						vlog = fmt.Sprintf("(%d) The delete repair sql statements of table %s.%s are generated.", logThreadSeq, sp.schema, sp.table)
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
						vlog = fmt.Sprintf("(%d) The delete repair sql statements of table %s.%s are generated.", logThreadSeq, sp.schema, sp.table)
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
	vlog = fmt.Sprintf("(%d) There is currently no index table %s.%s, and the cycle check data is started.", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Debug(vlog)
	noIndexOrderCol := sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", sp.schema, sp.table)]
	idxc := dbExec.IndexColumnStruct{Drivce: sp.sdrive, Schema: sp.schema, Table: sp.table, TableColumn: noIndexOrderCol.SColumnInfo, ChanrowCount: chanrowCount}
	idxc.Drivce = sp.sdrive
	//allColumns := idxc.TableIndexColumn().NoIndexOrderBySingerColumn(noIndexOrderCol.SColumnInfo)
	sdb := sp.sdbPool.Get(logThreadSeq)
	stt, err = idxc.TableIndexColumn().NoIndexGeneratingQueryCriteria(sdb, beginSeq, chanrowCount, logThreadSeq)
	sp.sdbPool.Put(sdb, logThreadSeq)
	if err != nil {
		return "", "", err
	}
	idxc.Drivce = sp.ddrive
	ddb := sp.ddbPool.Get(logThreadSeq)
	dtt, err = idxc.TableIndexColumn().NoIndexGeneratingQueryCriteria(ddb, beginSeq, chanrowCount, logThreadSeq)
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
	vlog = fmt.Sprintf("(%d) Start to check whether the row data blocks of the original target-side non-indexed table %s.%s are consistent...", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Debug(vlog)
	if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
		vlog = fmt.Sprintf("(%d) There is currently no index table %s.%s. The %d md5 check of the data consistency of the original target is abnormal.", logThreadSeq, sp.schema, sp.table, chunkSeq)
		global.Wlog.Debug(vlog)
		add, del := aa.Arrcmp(strings.Split(stt, "/*go actions rowData*/"), strings.Split(dtt, "/*go actions rowData*/"))
		if len(del) > 0 {
			tmpAnDateMap = make(map[string]string)
			vlog = fmt.Sprintf("(%d) Start generating the redundant data in the difference data for table %s.%s.", logThreadSeq, sp.schema, sp.table)
			global.Wlog.Debug(vlog)
			FileOpen.SqlType = "delete"
			md5Slice := FileOpen.ConcurrencyWriteFile(del)
			//md5Slice := FileOperate{File: sp.file, BufSize: 1024 * 4 * 1024, SqlType: "delete"}.ConcurrencyWriteFile(del)
			for _, deli := range md5Slice {
				tmpAnDateMap[deli] = "delete"
			}
			md5chan <- tmpAnDateMap
			vlog = fmt.Sprintf("(%d) The redundant data in the difference data of table %s.%s is generated.", logThreadSeq, sp.schema, sp.table)
			global.Wlog.Debug(vlog)
		}
		if len(add) > 0 {
			tmpAnDateMap = make(map[string]string)
			vlog = fmt.Sprintf("(%d) The missing data in the difference data that starts generating table %s.%s.", logThreadSeq, sp.schema, sp.table)
			global.Wlog.Debug(vlog)
			//md5Slice := FileOperate{File: sp.file, BufSize: 1024 * 4 * 1024, SqlType: "insert", fileName: sp.TmpFileName}.ConcurrencyWriteFile(add)
			FileOpen.SqlType = "insert"
			md5Slice := FileOpen.ConcurrencyWriteFile(add)
			for _, addi := range md5Slice {
				tmpAnDateMap[addi] = "insert"
			}
			md5chan <- tmpAnDateMap
			vlog = fmt.Sprintf("(%d) The missing data in the difference data of table %s.%s is generated.", logThreadSeq, sp.schema, sp.table)
			global.Wlog.Debug(vlog)
		}
	}
	vlog = fmt.Sprintf("(%d) The consistency check of the row data block of table %s.%s without index on the original target is completed!!!", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Debug(vlog)
}

//无索引表读取临时文件，并返回差异的数据
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
	fmt.Println(fmt.Sprintf("begin checkSum no index table %s.%s", sp.schema, sp.table))
	vlog = fmt.Sprintf("(%d) Start to verify the data of the original target end of the non-indexed table %s.%s ...", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
	barTableRow := sp.NoIndexTableCount(logThreadSeq)
	pods := Pod{Schema: sp.schema, Table: sp.table,
		IndexCol:    "noIndex",
		CheckMod:    sp.checkMod,
		Differences: "no",
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
			vlog = fmt.Sprintf("(%d) There is currently no index table %s.%s, and the %d md5 check of the data consistency of the original target is started.", logThreadSeq, sp.schema, sp.table, Cycles)
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
			vlog = fmt.Sprintf("(%d) There is currently no index table %s.%s The %d round of data cycle verification is complete.", logThreadSeq, sp.schema, sp.table, Cycles)
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
	pods.Rows = fmt.Sprintf("%v", maxTableCount)
	measuredDataPods = append(measuredDataPods, pods)
	vlog = fmt.Sprintf("(%d) No index table %s.%s The data consistency check of the original target end is completed", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
	fmt.Println(fmt.Sprintf("%s.%s 校验完成", sp.schema, sp.table))
}
