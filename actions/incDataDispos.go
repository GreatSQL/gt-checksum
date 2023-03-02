package actions

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	dbExec "gt-checksum/MySQL"
	"strings"
)

//增量数据校验结构
type IncDataDisposStruct struct {
	//mytype     string         //mysql体系的类型 是MySQL或miriadb
	//host       string         //数据库连接地址
	//user       string         //数据库user
	//password   string         //数据库password
	//port       uint16         //数据库的端口
	//serverId   uint32         //伪装slave的server id
	//binlogFile string         //读取的增量binlog file文件
	//pos        uint32         //读取的增量binlog pos点

	sdrive    string //源驱动类型
	ddrive    string //目驱动类型
	sJdbcUrl  string
	dJdbcUrl  string            //
	sgs       map[string]string //源端一致性点
	dgs       map[string]string //目标端一致性点
	tableList map[string]int    //校验的表列表
	//previousGtide string         //当前的gtid集合
}

//存放异常数据的结构定义

type SourceItemAbnormalDataStruct struct {
	sourceTrxType   string //源端事务类型
	sourceSqlType   string //源端sql类型
	sourceSqlGather []string
	destTrxType     string   //目标端事务类型
	destSqlType     string   //目标端sql类型
	destSqlGather   []string //目标端sql集合
}

/*
	根据源目标端的一致性点，读取源目标端的Event，解析sql语句，进行binlog event比对，出现差异则进行处理
*/
func (idds IncDataDisposStruct) Aa(fullDataCompletionStatus chan struct{}, cqMq chan SourceItemAbnormalDataStruct) {
	var (
		sblockDone, dblockDone        = make(chan struct{}, 1), make(chan struct{}, 1)
		s, d                          = make(chan map[string][]string, 1), make(chan map[string][]string, 1)
		strxEvent, dtrxEvent          = make(map[string][]string, 1), make(map[string][]string, 1)
		trxCount               uint64 = 1
		str, dtr               []byte
		err                    error
		z                      = func(s chan map[string][]string, st *map[string][]string) {
			for {
				select {
				case *st = <-s:
				}
			}
		}
		//检测全量数据返回子线程退出信号
		x = func(f chan struct{}, d chan struct{}) {
			for {
				select {
				case _, ok := <-f:
					if ok {
						d <- struct{}{}
						f <- struct{}{}
					}
				}
			}
		}
		e        = dbExec.IncDataBinlog().IncBinlogPrepareInit(idds.sdrive, idds.sJdbcUrl, idds.sgs, idds.tableList)
		f        = dbExec.IncDataBinlog().IncBinlogPrepareInit(idds.ddrive, idds.dJdbcUrl, idds.dgs, idds.tableList)
		szQ, dzQ = make(chan struct{}, 1), make(chan struct{}, 1)
	)

	//监测源目端binlog event的变化，以事务为单位
	go e.OneEventSql(sblockDone, s, szQ)
	go f.OneEventSql(dblockDone, d, dzQ)

	//读取源目端binlog event的事务
	go z(s, &strxEvent)
	go z(d, &dtrxEvent)

	//监测全量数据是否处理完成
	go x(fullDataCompletionStatus, szQ)
	go x(fullDataCompletionStatus, dzQ)

	for {
		_, ok := strxEvent["quit"]
		_, ok1 := dtrxEvent["quit"]
		if len(strxEvent) == 1 && len(dtrxEvent) == 1 && trxCount == 1 && !ok && !ok1 { //判断两端库的起点是否相同
			str, err = json.Marshal(strxEvent)
			if err != nil {
				fmt.Println(err)
			}
			dtr, err = json.Marshal(dtrxEvent)
			if err != nil {
				fmt.Println(err)
			}
			if md5.Sum(str) == md5.Sum(dtr) {
				strxEvent = make(map[string][]string, 1)
				dtrxEvent = make(map[string][]string, 1)
				str, dtr = []byte{}, []byte{}
				<-sblockDone
				<-dblockDone
				trxCount++
			} else {
				for k, v := range dtrxEvent {
					for _, i := range v {
						var aa = SourceItemAbnormalDataStruct{
							destTrxType:   k,
							destSqlGather: []string{i},
						}
						if strings.HasPrefix(i, "delete") {
							aa.destSqlType = "delete"
						}
						if strings.HasPrefix(i, "update") {
							aa.destSqlType = "update"
						}
						if strings.HasPrefix(i, "insert into") {
							aa.destSqlType = "insert"
						}
						aa.destSqlGather = []string{i}
						cqMq <- aa
					}
				}
				dtrxEvent = make(map[string][]string, 1)
				dtr = []byte{}
				<-dblockDone
			}
		}
		if len(strxEvent) == 1 && len(dtrxEvent) == 1 && trxCount > 1 && !ok1 && !ok {
			str, err = json.Marshal(strxEvent)
			if err != nil {
				fmt.Println(err)
			}
			dtr, err = json.Marshal(dtrxEvent)
			if err != nil {
				fmt.Println(err)
			}
			if md5.Sum(str) == md5.Sum(dtr) {
				strxEvent = make(map[string][]string, 1)
				dtrxEvent = make(map[string][]string, 1)
				str, dtr = []byte{}, []byte{}
				<-sblockDone
				<-dblockDone
				trxCount++
			} else {
				var sk, dk string
				var sv, dv []string
				for k, v := range dtrxEvent {
					dk = k
					dv = v
				}
				for k, v := range strxEvent {
					sk = k
					sv = v
				}
				aa := SourceItemAbnormalDataStruct{
					sourceTrxType:   sk,
					sourceSqlGather: sv,
					destTrxType:     dk,
					destSqlGather:   dv,
				}

				cqMq <- aa
				dtrxEvent, strxEvent = make(map[string][]string, 1), make(map[string][]string, 1)
				str, dtr = []byte{}, []byte{}
				aa = SourceItemAbnormalDataStruct{}
				<-sblockDone
				<-dblockDone
				trxCount++
			}
		}

		//读取源目端binlog的线程停止
		if ok && ok1 {
			fmt.Println("---退出__-")
			break
		}
	}
}
func IncDataDisops(sdbdrive, ddbdrive, sjdbcUrl, djdbcurl string, sgs, dgs map[string]string, tableList []string) *IncDataDisposStruct {
	checkTableMap := make(map[string]int, len(tableList))
	for i := range tableList {
		checkTableMap[tableList[i]] = 0
	}
	return &IncDataDisposStruct{
		sdrive:    sdbdrive,
		ddrive:    ddbdrive,
		sJdbcUrl:  sjdbcUrl,
		dJdbcUrl:  djdbcurl,
		sgs:       sgs,
		dgs:       dgs,
		tableList: checkTableMap,
	}
}
