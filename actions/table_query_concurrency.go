package actions

import (
	"fmt"
	"greatdbCheck/global"
	"greatdbCheck/inputArg"
	"math/rand"
	"os"
	"strings"
	"time"
)

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
	sfile                                                                      *os.File
	checkMod                                                                   string
	checkNoIndexTable                                                          string //是否检查无索引表
	tableAllCol                                                                map[string]global.TableAllColumnInfoS
	ratio                                                                      int
}

type writeTmpTableDataStruct struct {
	columnName                                 []string //待校验表的列名，有可能是多个
	chanrowCount, concurrency, queueDepth      int      //单次并发一次校验的行数
	TmpTablePath                               string
	sdbPool                                    *global.Pool
	ddbPool                                    *global.Pool
	smtype                                     string //是源端还是目标端
	sdrive, ddrive, datafixType, table, schema string
	sfile                                      *os.File
	indexColumnType                            string
	checkMod                                   string
	tableIndexColumnMap                        map[string][]string
	tableAllCol                                map[string]global.TableAllColumnInfoS
	ratio                                      int
}

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
   该函数用于分批分段读取
   并发读取原、目标端 单表的索引列数据，根据表的数量按照单次校验的数据块行数进行切割，
   并发写入管道中，同时会有监听管道进行读取数据
*/
func (wttds writeTmpTableDataStruct) indexColUniqProduct(ma <-chan string, indexColData chan<- []string, done <-chan bool, differDone chan<- bool, selectColumnString, lengthTrim map[string]string, columnLengthAs map[string][]string, goroutineNum int) { //定义变量
	var (
		workLimiter = make(chan struct{}, goroutineNum)
		breakDone   = make(chan bool, 1)
		execStatus  = make(chan bool, 1)
		breakStatus = false
	)
	t := time.NewTicker(time.Millisecond)
	for time1 := range t.C {
		select {
		case d := <-done:
			if d {
				breakStatus = true
			}
		case <-breakDone:
			global.Wlog.Info("[check table ", wttds.schema, ".", wttds.table, " index column]", "table Index Column Data done!")
			fmt.Println("table Index Column Data done!", time1.Format("2006-01-02 15:04:11"))
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
				alog := fmt.Sprintf("(%d) It is detected that there is data generated in mq.")
				global.Wlog.Info(alog)
				rand.Seed(time.Now().UnixNano())
				for i := range ma {
					select {
					case workLimiter <- struct{}{}:
						time.Sleep(time.Nanosecond * 2)
						go func(i string, g int64) {
							logThreadSeq := rand.Int63()
							defer func() {
								<-workLimiter
							}()
							z := wttds.indexColUniq(i, g, selectColumnString, lengthTrim, columnLengthAs, logThreadSeq)
							if len(z) > wttds.chanrowCount {
								indexColData <- z[:len(z)/2]
								indexColData <- z[len(z)/2:]
							} else {
								indexColData <- z
							}
						}(i, int64(wttds.chanrowCount))
					}
				}
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

func (wttds *writeTmpTableDataStruct) IndexColumnProduct(ma <-chan []string, out1 chan<- map[string]string, done <-chan bool, sqlwhereDone chan<- bool, goroutineNum int) {
	var (
		workLimiter = make(chan struct{}, goroutineNum)
		breakDone   = make(chan bool, 1)
		execStatus  = make(chan bool, 1)
		breakStatus = false
	)
	t := time.NewTicker(time.Millisecond)
	for time1 := range t.C {
		select {
		case d := <-done:
			if d {
				breakStatus = true
			}
		case <-breakDone:
			global.Wlog.Info("table QuerySql Where Data Generate done!")
			fmt.Println("table QuerySql Where Data Generate done!", time1.Format("2006-01-02 15:04:11"))
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
				rand.Seed(time.Now().UnixNano())
				for i := range ma {
					time.Sleep(time.Nanosecond * 2)
					select {
					case workLimiter <- struct{}{}:
						go func(i []string) {
							logThreadSeq := rand.Int63()
							defer func() {
								<-workLimiter
							}()
							out1 <- wttds.TableSelectWhere(wttds.columnName, i, logThreadSeq)
						}(i)
					}
				}
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
	for time1 := range t.C {
		select {
		case d := <-done:
			if d {
				breakStatus = true
			}
		case <-breakDone:
			fmt.Println("table query sql Product done!", time1.Format("2006-01-02 15:04:11"))
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
				rand.Seed(time.Now().UnixNano())
				for i := range ma {
					time.Sleep(time.Nanosecond * 2)
					select {
					case workLimiter <- struct{}{}:
						go func(i map[string]string, cc1 map[string]global.TableAllColumnInfoS) {
							logThreadSeq := rand.Int63()
							defer func() {
								<-workLimiter
							}()
							out2 <- wttds.queryTableSql(i, cc1, logThreadSeq)
						}(i, cc1)
					}
				}
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
	for time1 := range t.C {
		select {
		case d := <-done:
			if d {
				breakStatus = true
			}
		case <-breakDone:
			fmt.Println("table All Measured Data CheckSum done!", time1.Format("2006-01-02 15:04:11"))
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
				rand.Seed(time.Now().UnixNano())
				for i := range ma {
					time.Sleep(time.Nanosecond * 2)
					select {
					case workLimiter <- struct{}{}:
						go func(i map[string]string, cc1 map[string]global.TableAllColumnInfoS) {
							logThreadSeq := rand.Int63()
							defer func() {
								<-workLimiter
							}()
							difference := wttds.queryTableData(i, cc1, logThreadSeq)
							if difference.Schema != "" && difference.Table != "" {
								out2 <- difference
							}
						}(i, cc1)
					}
				}
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
	for time1 := range t.C {
		select {
		case d := <-done:
			if d {
				breakStatus = true
			}
		case <-breakDone:
			fmt.Println("table Differences in Data CheckSum done!", time1.Format("2006-01-02 15:04:11"))
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
				rand.Seed(time.Now().UnixNano())
				for i := range ma {
					time.Sleep(time.Nanosecond * 2)
					select {
					case workLimiter <- struct{}{}:
						go func(i DifferencesDataStruct) {
							logThreadSeq := rand.Int63()
							defer func() {
								<-workLimiter
							}()
							out1 <- wttds.AbnormalDataDispos(i.Schema, i.Table, i.SqlWhere, i.TableColumnInfo, i.indexColumnType, logThreadSeq)
						}(i)
					}
				}
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
	for time1 := range t.C {
		select {
		case d := <-diffdone:
			if d {
				breakStatus = true
			}
		case <-breakDone:
			fmt.Println("table Differences in Data fix done!!", time1.Format("2006-01-02 15:04:11"))
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
				rand.Seed(time.Now().UnixNano())
				for i := range ma {
					time.Sleep(time.Nanosecond * 2)
					select {
					case workLimiter <- struct{}{}:
						//wg.Add(1)
						go func(i []string) {
							logThreadSeq := rand.Int63()
							defer func() {
								<-workLimiter
								//defer wg.Done()
							}()
							wttds.DataFixDispos(i, logThreadSeq)
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
   查询索引列信息，并发执行调度生成
*/
func (sp *SchedulePlan) Schedulingtasks() {
	var chanrowCount int
	wd := &writeTmpTableDataStruct{
		sdbPool:             sp.sdbConnPool,
		sdrive:              sp.sdrive,
		ddrive:              sp.ddrive,
		ddbPool:             sp.ddbConnPool,
		tableIndexColumnMap: sp.tableIndexColumnMap,
		datafixType:         sp.datafixType,
		sfile:               sp.sfile, //修复文件的文件句柄
		queueDepth:          sp.mqQueueDepth,
		concurrency:         sp.Concurrency,
		chanrowCount:        sp.jointIndexChanRowCount,
		tableAllCol:         sp.tableAllCol,
		checkMod:            sp.checkMod,
		ratio:               sp.ratio,
	}
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
		wd.doIndexDataCheck()
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
		sfile:                   m.Sfile,
	}
}
