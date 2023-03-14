package actions

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"math/rand"
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
	a := sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", sp.schema, sp.table)].SColumnInfo
	//查询源目标端索引列数据
	idxc := dbExec.IndexColumnStruct{Schema: sp.schema, Table: sp.table, ColumnName: sp.columnName,
		ChanrowCount: sp.chanrowCount, Drivce: sp.sdrive, SelectColumn: selectColumn[sp.sdrive], ColData: a}
	vlog = fmt.Sprintf("(%d) Start to query the index column data of index column [%v] of source table [%v.%v]...", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	global.Wlog.Debug(vlog)
	SdataChan1, err := idxc.TableIndexColumn().TmpTableColumnGroupDataDispos(sdb, where, sp.columnName[level], logThreadSeq)
	if err != nil {
		return
	}
	idxc.Drivce = sp.ddrive
	idxc.SelectColumn = selectColumn[sp.ddrive]
	vlog = fmt.Sprintf("(%d) Start to query the index column data of index column [%v] of dest table [%v.%v]...", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	global.Wlog.Debug(vlog)
	DdataChan1, err := idxc.TableIndexColumn().TmpTableColumnGroupDataDispos(ddb, where, sp.columnName[level], logThreadSeq)
	if err != nil {
		return
	}
	cMerge := dataDispos.DataInfo{ChanQueueDepth: sp.mqQueueDepth}
	ascUniqSDDataChan := cMerge.ChangeMerge(SdataChan1, DdataChan1)
	vlog = fmt.Sprintf("(%d) Start to recursively process the where condition of index column [%v] of table [%v.%v] according to the size of a single check block...", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	global.Wlog.Debug(vlog)
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
				if level == 0 {
					close(sqlWhere)
				}
				return
			}
			vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], the column value is [%v], and the number of repeated data in the column is [%v]", logThreadSeq, level, where, sp.columnName[level], autoIncSeq, key, value)
			global.Wlog.Debug(vlog)
			if key == "<nil>" || key == "<entry>" {
				vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], Start processing null value data...", logThreadSeq, level, where, sp.columnName[level], autoIncSeq)
				global.Wlog.Debug(vlog)
				if e != "" { //假如null或者entry不是首行，则先处理原有数据条件
					if key != "END" {
						g = key
					}
					if e == g {
						sqlwhere = fmt.Sprintf(" %v >= '%v' and %v <= '%v' ", sp.columnName[level], e, sp.columnName[level], g)
					} else {
						sqlwhere = fmt.Sprintf(" %v > '%v' and %v <= '%v' ", sp.columnName[level], e, sp.columnName[level], g)
					}
					if where != "" {
						sqlwhere = fmt.Sprintf("%s %s", where, sqlwhere)
					}
					sqlWhere <- sqlwhere
					sqlwhere, e, g = "", "", ""
				}
				var whereExist string
				if where != "" {
					whereExist = fmt.Sprintf("%s and ", where)
				}
				if key == "<entry>" {
					sqlwhere = fmt.Sprintf("%s %s = '' ", whereExist, sp.columnName[level])
				}
				if key == "<nil>" {
					sqlwhere = fmt.Sprintf("%s %s is null ", whereExist, sp.columnName[level])
				}
				partFirstValue = true
				vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], the query sql-where is [%v], Null value data processing is complete!!!", logThreadSeq, level, where, sp.columnName[level], autoIncSeq, sqlwhere)
				global.Wlog.Debug(vlog)
				sqlWhere <- sqlwhere
				sqlwhere = ""
			} else {
				//获取联合索引或单列索引的首值
				if key != "END" && e == "" {
					e = key
				}
				vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], The starting value of the current index column is [%v].", logThreadSeq, level, where, sp.columnName[level], autoIncSeq, e)
				global.Wlog.Debug(vlog)
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
					vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v],{end index column} {end row data} start dispos...", logThreadSeq, level, where, sp.columnName[level], autoIncSeq)
					global.Wlog.Debug(vlog)
					var whereExist string
					if where != "" {
						whereExist = fmt.Sprintf("%v and ", where)
					}
					if partFirstValue {
						sqlwhere = fmt.Sprintf("%v %v >= '%v' and %v <= '%v' ", whereExist, sp.columnName[level], e, sp.columnName[level], g)
						partFirstValue = false
					} else {
						sqlwhere = fmt.Sprintf("%v %v > '%v' and %v <= '%v' ", whereExist, sp.columnName[level], e, sp.columnName[level], g)
					}

					sqlWhere <- sqlwhere
					sqlwhere = ""
					vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v],the query sql where is [%v],{end index column} {end row data} dispos Finish!!!", logThreadSeq, level, where, sp.columnName[level], autoIncSeq, sqlwhere)
					global.Wlog.Debug(vlog)
				}
			}
			//判断行数累加值是否>=要校验的值
			if d >= queryNum {
				//判断联合索引列深度
				//判断当前索引列的重复值是否是校验数据块大小的两倍
				if (d/queryNum < 2 && level < len(sp.columnName)-1) || level == len(sp.columnName)-1 { //小于校验块的两倍，则直接输出当前索引列深度的条件
					var whereExist string
					if where != "" { //非第一层索引列数据
						whereExist = fmt.Sprintf("%s and ", where)
					}
					if d == c && c >= queryNum { //单行索引列数据的group值大于并发数
						sqlwhere = fmt.Sprintf("%s %v = '%v' ", whereExist, sp.columnName[level], g)
					} else {
						if partFirstValue { //每段的首行数据
							sqlwhere = fmt.Sprintf("%s %v >= '%v' and %v <= '%v' ", whereExist, sp.columnName[level], e, sp.columnName[level], g)
							partFirstValue = false
						} else {
							sqlwhere = fmt.Sprintf("%s %v > '%v' and %v <= '%v' ", whereExist, sp.columnName[level], e, sp.columnName[level], g)
						}
					}
					sqlWhere <- sqlwhere
					if key != "END" {
						e = key
					}
					sqlwhere = ""
				} else {
					if where != "" {
						where = fmt.Sprintf(" %v and %v = '%v' ", where, sp.columnName[level], g)
					} else {
						where = fmt.Sprintf(" %v = '%v' ", sp.columnName[level], g)
					}
					level++ //索引列层数递增
					//进入下一层的索引计算
					sp.recursiveIndexColumn(sqlWhere, sdb, ddb, level, queryNum, where, selectColumn, logThreadSeq)

					level-- //回到上一层
					//递归处理结束后，处理where条件，将下一层的索引列条件去掉
					if strings.Contains(strings.TrimSpace(where), sp.columnName[level]) {
						where = strings.TrimSpace(where[:strings.Index(where, sp.columnName[level])])
						if strings.HasSuffix(where, "and") {
							where = strings.TrimSpace(where[:strings.LastIndex(where, "and")])
						}
					}
					if key != "END" {
						e = key
					}
				}
				d = 0 //累加值清0
			}
		}
	}
	vlog = fmt.Sprintf("(%d) Recursively process the where condition of the index column [%v] of table [%v.%v] according to the size of the word check block!!!", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
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
	vlog = fmt.Sprintf("(%d) Check table %s.%s and start generating query sequence.", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)

	//查询表索引列数据并且生成查询的where条件
	sdb := sp.sdbPool.Get(logThreadSeq)
	ddb := sp.ddbPool.Get(logThreadSeq)
	sp.recursiveIndexColumn(sqlWhere, sdb, ddb, 0, sp.chanrowCount, "", selectColumn, logThreadSeq)
	sp.sdbPool.Put(sdb, logThreadSeq)
	sp.ddbPool.Put(ddb, logThreadSeq)
	vlog = fmt.Sprintf("(%d) Verify that table %s.%s query sequence is generated. !!!", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
}

/*
针对表的所有列的数据类型进行处理，将列类型转换成字符串，例如时间类型
*/
func (sp *SchedulePlan) queryTableSql(sqlWhere chanString, selectSql chanMap, cc1 global.TableAllColumnInfoS, sc chan int64, logThreadSeq int64) {
	var (
		vlog    string
		curry   = make(chanStruct, sp.concurrency)
		autoSeq int64
		err     error
	)
	vlog = fmt.Sprintf("(%d) Start processing the block data verification query sql of the verification table ...", logThreadSeq)
	global.Wlog.Debug(vlog)
	for {
		select {
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
					//查询该表的列名和列信息
					idxc := dbExec.IndexColumnStruct{Schema: sp.schema, Table: sp.table, TableColumn: cc1.SColumnInfo, Sqlwhere: c1, Drivce: sp.sdrive}
					lock.Lock()
					selectSqlMap[sp.sdrive], err = idxc.TableIndexColumn().GeneratingQuerySql(sd, logThreadSeq)
					if err != nil {
						return
					}
					lock.Unlock()
					idxc.Drivce = sp.ddrive
					idxc.TableColumn = cc1.DColumnInfo
					lock.Lock()
					selectSqlMap[sp.ddrive], err = idxc.TableIndexColumn().GeneratingQuerySql(dd, logThreadSeq)
					if err != nil {
						return
					}
					lock.Unlock()
					vlog = fmt.Sprintf("(%d) The block data verification query sql processing of the verification table is completed. !!!", logThreadSeq)
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
func (sp *SchedulePlan) queryTableData(selectSql chanMap, diffQueryData chanDiffDataS, cc1 global.TableAllColumnInfoS, sc chan int64, logThreadSeq int64) {
	var (
		vlog               string
		aa                 = &CheckSumTypeStruct{}
		differencesData    = InitDifferencesDataStruct()
		curry              = make(chanStruct, sp.concurrency)
		autoSeq1, autoSeq2 int64
	)
	sp.bar = &Bar{}
	if sp.checkMod == "rows" {
		if sp.tableMaxRows > 0 {
			barTotal := int64(sp.tableMaxRows / uint64(sp.chanrowCount))
			if sp.tableMaxRows%uint64(sp.chanrowCount) > 0 {
				barTotal += 1
			}
			sp.bar.NewOption(0, barTotal, "task")
		}
	}
	if sp.checkMod == "sample" {
		sp.bar.NewOption(0, sp.sampDataGroupNumber, "task")
	}
	for {
		select {
		case d, ok := <-sc:
			if ok {
				sp.bar.NewOption(0, d, "task")
			}
		case c, ok := <-selectSql:
			if !ok {
				if len(curry) == 0 {
					close(diffQueryData)
					return
				}
			} else {
				autoSeq1++
				idxc := dbExec.IndexColumnStruct{
					Schema:      sp.schema,
					Table:       sp.table,
					TableColumn: cc1.SColumnInfo,
					Sqlwhere:    c[sp.sdrive],
					Drivce:      sp.sdrive,
				}
				curry <- struct{}{}
				go func(c1 map[string]string, cc1 global.TableAllColumnInfoS) {
					defer func() {
						<-curry
					}()
					//查询该表的列名和列信息
					vlog = fmt.Sprintf("(%d) Start to query the block data of check table %s.%s ...", logThreadSeq, sp.schema, sp.table)
					global.Wlog.Debug(vlog)
					sdb := sp.sdbPool.Get(logThreadSeq)
					vlog = fmt.Sprintf("%v", c1)
					global.Wlog.Debug(vlog)
					stt, err := idxc.TableIndexColumn().GeneratingQueryCriteria(sdb, logThreadSeq)
					vlog = fmt.Sprintf("(%d) check source %s table %s.%s query data is {%v}", logThreadSeq, sp.sdrive, sp.schema, sp.table, stt)
					global.Wlog.Debug(vlog)
					sp.sdbPool.Put(sdb, logThreadSeq)
					if err != nil {
						return
					}
					idxc.Drivce = sp.ddrive
					idxc.Sqlwhere = c1[sp.ddrive]
					idxc.TableColumn = cc1.DColumnInfo
					ddb := sp.ddbPool.Get(logThreadSeq)
					dtt, err := idxc.TableIndexColumn().GeneratingQueryCriteria(ddb, logThreadSeq)
					vlog = fmt.Sprintf("(%d) check dest %s table %s.%s query data is {%v}", logThreadSeq, sp.ddrive, sp.schema, sp.table, dtt)
					global.Wlog.Debug(vlog)
					sp.ddbPool.Put(ddb, logThreadSeq)
					if err != nil {
						return
					}
					vlog = fmt.Sprintf("(%d) Check table %s.%s to start checking the consistency of block data ...", logThreadSeq, sp.schema, sp.table)
					global.Wlog.Debug(vlog)
					if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
						vlog = fmt.Sprintf("(%d) Verification table %s.%s The block data verified by the original target end is inconsistent. query sql is {%s}.", logThreadSeq, sp.schema, sp.table, c1)
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
						vlog = fmt.Sprintf("(%d) Verification table %s.%s The block data verified by the original target end is consistent. query sql is {%s}.", logThreadSeq, sp.schema, sp.table, c1)
						global.Wlog.Debug(vlog)
					}
					stt, dtt = "", ""
					vlog = fmt.Sprintf("(%d) The block data verification of check table %s.%s is completed !!!", logThreadSeq, sp.schema, sp.table)
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
		curry = make(chanStruct, sp.concurrency)
	)
	vlog = fmt.Sprintf("(%d) Check table %s.%s to start differential data processing and generate repair statements ...", logThreadSeq, sp.schema, sp.table)
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
					colData := sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", c1.Schema, c1.Table)]
					idxc := dbExec.IndexColumnStruct{
						Schema:      sp.schema,
						Table:       sp.table,
						TableColumn: colData.SColumnInfo,
						Drivce:      sp.sdrive,
					}
					idxc.Sqlwhere = c1.SqlWhere[sp.sdrive]
					stt, _ := idxc.TableIndexColumn().GeneratingQueryCriteria(sdb, logThreadSeq)
					idxc.Drivce = sp.ddrive
					idxc.Sqlwhere = c1.SqlWhere[sp.ddrive]
					idxc.TableColumn = colData.DColumnInfo
					dtt, _ := idxc.TableIndexColumn().GeneratingQueryCriteria(ddb, logThreadSeq)
					//对不同数据库的的null处理
					//if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
					//	if strings.Contains(stt, "/*go actions columnData*//*") {
					//		stt = strings.ReplaceAll(stt, "/*go actions columnData*//*", "/*go actions columnData*/<nil>/*")
					//	}
					//	if strings.Contains(dtt, "/*go actions columnData*//*") {
					//		dtt = strings.ReplaceAll(dtt, "/*go actions columnData*//*", "/*go actions columnData*/<nil>/*")
					//	}
					//}
					if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
						add, del := aa.Arrcmp(strings.Split(stt, "/*go actions rowData*/"), strings.Split(dtt, "/*go actions rowData*/"))
						stt, dtt = "", ""
						vlog = fmt.Sprintf("(%d) There is difference data in check table %d.%d, start to generate repair statement.", logThreadSeq, c1.Schema, c1.Table)
						global.Wlog.Debug(vlog)
						if len(del) > 0 || len(add) > 0 {
							dbf := dbExec.DataAbnormalFixStruct{Schema: c1.Schema, Table: c1.Table, ColData: colData.DColumnInfo, Sqlwhere: c1.SqlWhere[sp.ddrive], DestDevice: sp.ddrive, IndexColumn: sp.columnName, DatafixType: sp.datafixType}
							if strings.HasPrefix(c1.indexColumnType, "pri") {
								dbf.IndexType = "pri"
							} else if strings.HasPrefix(c1.indexColumnType, "uni") {
								dbf.IndexType = "uni"
							} else {
								dbf.IndexType = "mui"
							}
							if len(del) > 0 {
								vlog = fmt.Sprintf("(%d) Start to generate the delete statement of check table %s.%s.", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
								for _, i := range del {
									dbf.RowData = i
									sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
									if err != nil {
										sp.getErr(fmt.Sprintf("dest: checkSum table %s.%s generate delete sql error.", c1.Schema, c1.Table), err)
									}
									if sqlstr != "" {
										cc <- sqlstr
									}
								}
								vlog = fmt.Sprintf("(%d) The delete repair statement verifying table %s.%s is complete.", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
							}
							if len(add) > 0 {
								vlog = fmt.Sprintf("(%d) Start to generate the insert statement of check table %s.%s.", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
								for _, i := range add {
									dbf.RowData = i
									sqlstr, err := dbf.DataAbnormalFix().FixInsertSqlExec(ddb, sp.ddrive, logThreadSeq)
									if err != nil {
										sp.getErr(fmt.Sprintf("dest: checkSum table %s.%s generate insert sql error.", c1.Schema, c1.Table), err)
									}
									if sqlstr != "" {
										cc <- sqlstr
									}
								}
								vlog = fmt.Sprintf("(%d) The insert repair statement verifying table %s.%s is complete. ", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
							}
						}
					}
				}(c, sdb, ddb)
			}
		}
	}
	vlog = fmt.Sprintf("(%d) Check table %s.%s to complete differential data processing and generate repair statements. !!!", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
}

func (sp SchedulePlan) DataFixDispos(fixSQL chanString, logThreadSeq int64) {
	var (
		vlog     string
		noIndexD = make(chan struct{}, sp.concurrency)
		increSeq int
		sqlSlice []string
	)
	vlog = fmt.Sprintf("(%d) The current table %s.%s processes the repair statement on the target side.", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
	for {
		select {
		case v, ok := <-fixSQL:
			if !ok {
				if len(noIndexD) == 0 {
					if len(sqlSlice) > 0 {
						ApplyDataFix(sqlSlice, sp.datafixType, sp.sfile, sp.ddrive, sp.djdbc, logThreadSeq)
						vlog = fmt.Sprintf("(%d) The delete repair sql statements of table %s.%s are generated.", logThreadSeq, sp.schema, sp.table)
						global.Wlog.Debug(vlog)
						sqlSlice = []string{}
					} else {
						measuredDataPods = append(measuredDataPods, *sp.pods)
						return
					}
				}
			} else {
				increSeq++
				sp.pods.Differences = "yes"
				sqlSlice = append(sqlSlice, v)
				if increSeq == sp.fixTrxNum {
					var sqlSlice1 []string
					for _, i := range sqlSlice {
						sqlSlice1 = append(sqlSlice1, i)
					}
					sqlSlice = []string{}
					//noIndexD <- struct{}{}
					increSeq = 0
					//go func(a []string) {
					//	defer func() {
					//		<-noIndexD
					//	}()
					ApplyDataFix(sqlSlice1, sp.datafixType, sp.sfile, sp.ddrive, sp.djdbc, logThreadSeq)
					vlog = fmt.Sprintf("(%d) The delete repair sql statements of table %s.%s are generated.", logThreadSeq, sp.schema, sp.table)
					global.Wlog.Debug(vlog)
					//}(sqlSlice1)
				}
			}
		}
	}

}

/*
处理有索引表的数据校验
*/
func (sp SchedulePlan) doIndexDataCheck() {
	var (
		queueDepth          = sp.mqQueueDepth
		sqlWhere            = make(chanString, queueDepth)
		selectSql           = make(chanMap, queueDepth)
		diffQueryData       = make(chanDiffDataS, queueDepth)
		fixSQL              = make(chanString, queueDepth)
		tableColumn         = sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", sp.schema, sp.table)]
		selectColumnStringM = make(map[string]map[string]string)
	)
	rand.Seed(time.Now().UnixNano())
	logThreadSeq := rand.Int63()
	idxc := dbExec.IndexColumnStruct{Schema: sp.schema, Table: sp.table, ColumnName: sp.columnName,
		ChanrowCount: sp.chanrowCount, Drivce: sp.sdrive,
		ColData: sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", sp.schema, sp.table)].SColumnInfo}
	selectColumnStringM[sp.sdrive] = idxc.TableIndexColumn().TmpTableIndexColumnSelectDispos(logThreadSeq)
	idxc.Drivce = sp.ddrive
	selectColumnStringM[sp.ddrive] = idxc.TableIndexColumn().TmpTableIndexColumnSelectDispos(logThreadSeq)

	sp.pods = &Pod{
		Schema:      sp.schema,
		Table:       sp.table,
		IndexCol:    strings.TrimLeft(strings.Join(sp.columnName, ","), ","),
		CheckMod:    sp.checkMod,
		Differences: "no",
		Datafix:     sp.datafixType,
	}
	idxc.Drivce = sp.sdrive
	sdb := sp.sdbPool.Get(logThreadSeq)
	A, err := idxc.TableIndexColumn().TableRows(sdb, int64(logThreadSeq))
	sp.sdbPool.Put(sdb, logThreadSeq)
	if err != nil {
		return
	}
	idxc.Drivce = sp.ddrive
	ddb := sp.ddbPool.Get(logThreadSeq)
	B, err := idxc.TableIndexColumn().TableRows(ddb, int64(logThreadSeq))
	if err != nil {
		return
	}
	sp.ddbPool.Put(ddb, logThreadSeq)
	if A >= B {
		sp.tableMaxRows = A
	} else {
		sp.tableMaxRows = B
	}
	sp.pods.Rows = fmt.Sprintf("%d,%d", A, B)
	var scheduleCount = make(chan int64, 1)
	go sp.indexColumnDispos(sqlWhere, selectColumnStringM)
	go sp.queryTableSql(sqlWhere, selectSql, tableColumn, scheduleCount, logThreadSeq)
	go sp.queryTableData(selectSql, diffQueryData, tableColumn, scheduleCount, logThreadSeq)
	go sp.AbnormalDataDispos(diffQueryData, fixSQL, logThreadSeq)
	sp.DataFixDispos(fixSQL, logThreadSeq)
}
