package actions

import (
	"fmt"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"math/rand"
	"os"
	"strings"
	"time"
)

type SchedulePlan struct {
	singleIndexChanRowCount, jointIndexChanRowCount, mqQueueDepth int
	schema, table                                                 string   //待校验库名、表名
	columnName                                                    []string //待校验表的列名，有可能是多个
	tmpTableDataFileDir                                           string   //临时表文件生成的相对路径
	tableIndexColumnMap                                           map[string][]string
	sdbPool, ddbPool                                              *global.Pool
	datafixType                                                   string
	datafixSql                                                    string
	sdrive, ddrive                                                string
	sfile                                                         *os.File
	checkMod, checkObject                                         string
	checkNoIndexTable                                             string //是否检查无索引表
	tableAllCol                                                   map[string]global.TableAllColumnInfoS
	ratio                                                         int
	file                                                          *os.File
	TmpFileName                                                   string
	bar                                                           *Bar
	fixTrxNum                                                     int
	chanrowCount, concurrency                                     int //单次并发一次校验的行数
	TmpTablePath                                                  string
	smtype                                                        string //是源端还是目标端
	indexColumnType                                               string
	pods                                                          *Pod
	tableMaxRows                                                  uint64
	sampDataGroupNumber                                           int64
	djdbc                                                         string
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
	TableColumnInfo global.TableAllColumnInfoS //该表的所有列信息，包括列类型
	SqlWhere        map[string]string          //差异数据查询的where 条件
	indexColumnType string                     //索引列类型
}

/*
查询索引列信息，并发执行调度生成
*/
func (sp *SchedulePlan) Schedulingtasks() {
	sp.bar = &Bar{}
	rand.Seed(time.Now().UnixNano())
	for k, v := range sp.tableIndexColumnMap {
		//是否校验无索引表
		if sp.checkNoIndexTable == "no" && len(v) == 0 {
			continue
		}
		sp.file, _ = os.OpenFile(sp.TmpFileName, os.O_CREATE|os.O_RDWR, 0777)
		if strings.Contains(k, "/*indexColumnType*/") {
			ki := strings.Split(k, "/*indexColumnType*/")[0]
			sp.indexColumnType = strings.Split(k, "/*indexColumnType*/")[1]
			if strings.Contains(ki, "/*greatdbSchemaTable*/") {
				sp.schema = strings.Split(ki, "/*greatdbSchemaTable*/")[0]
				sp.table = strings.Split(ki, "/*greatdbSchemaTable*/")[1]
			}
		} else {
			if strings.Contains(k, "/*greatdbSchemaTable*/") {
				sp.schema = strings.Split(k, "/*greatdbSchemaTable*/")[0]
				sp.table = strings.Split(k, "/*greatdbSchemaTable*/")[1]
			}
		}
		if len(v) == 0 { //校验无索引表
			if sp.singleIndexChanRowCount <= sp.jointIndexChanRowCount {
				sp.chanrowCount = sp.singleIndexChanRowCount
			} else {
				sp.chanrowCount = sp.jointIndexChanRowCount
			}
			logThreadSeq := rand.Int63()
			sp.SingleTableCheckProcessing(sp.chanrowCount, logThreadSeq)
		} else { //校验有索引的表
			if len(v) > 1 { //根据索引列数量觉得chanrowCount数
				sp.chanrowCount = sp.jointIndexChanRowCount
			} else {
				sp.chanrowCount = sp.singleIndexChanRowCount
			}
			sp.columnName = v
			fmt.Println(fmt.Sprintf("begin checkSum index table %s.%s", sp.schema, sp.table))
			sp.doIndexDataCheck()
			fmt.Println()
			fmt.Println(fmt.Sprintf("table %s.%s checksum complete", sp.schema, sp.table))
		}
		sp.file.Close()
		os.Remove(sp.TmpFileName)
	}
}

func CheckTableQuerySchedule(sdb, ddb *global.Pool, tableIndexColumnMap map[string][]string, tableAllCol map[string]global.TableAllColumnInfoS, m inputArg.ConfigParameter) *SchedulePlan {
	return &SchedulePlan{
		concurrency:             m.Concurrency,
		sdbPool:                 sdb,
		ddbPool:                 ddb,
		singleIndexChanRowCount: m.SingleIndexChanRowCount,
		jointIndexChanRowCount:  m.JointIndexChanRowCount,
		tableIndexColumnMap:     tableIndexColumnMap,
		tableAllCol:             tableAllCol,
		datafixType:             m.Datafix,
		datafixSql:              m.FixFileName,
		sdrive:                  m.SourceDrive,
		ddrive:                  m.DestDrive,
		mqQueueDepth:            m.QueueDepth,
		checkNoIndexTable:       m.CheckNoIndexTable,
		checkMod:                m.CheckMode,
		ratio:                   m.Ratio,
		sfile:                   m.Sfile,
		checkObject:             m.CheckObject,
		TmpFileName:             "tmp_file",
		fixTrxNum:               m.FixTrxNum,
		djdbc:                   m.DestJdbc,
	}
}
