package mysql

import (
	"strconv"
	"strings"
	"time"
)

type IncDataBinlogPrepareStruct struct{}

type IncDataBinlogPrepareInterface interface {
	OneEventSql(block chan struct{}, trxEvent chan map[string][]string, dQ chan struct{})
	BinlogStreamer() interface{}
	BinlogStreamerClose(cfg interface{})
}

type OracleIncDataBinlogPrepareStruct struct {
}

//func (my *MySQLIncDataBinlogPrepareStruct) aa() {
//	ev := my.getEvent()
//}

//func (or *OracleIncDataBinlogPrepareStruct) aa() *replication.BinlogStreamer{}

func (idbps IncDataBinlogPrepareStruct) IncBinlogPrepareInit(dbDrive, jdbcUrl string, gs map[string]string, checkTableMap map[string]int) IncDataBinlogPrepareInterface {
	var incDbps IncDataBinlogPrepareInterface
	if dbDrive == "mysql" {
		userPassword := strings.Split(strings.Split(jdbcUrl, "@")[0], ":")
		tmpa := strings.Split(jdbcUrl, "@")[1]
		rightKindex := strings.Index(tmpa, ")")
		leftKindex := strings.Index(tmpa, "(") + 1
		hostPort := strings.Split(tmpa[leftKindex:rightKindex], ":")
		port, _ := strconv.Atoi(hostPort[1])
		binlogFile := gs["file"]
		pos, _ := strconv.Atoi(gs["position"])
		server_id := uint32(time.Now().Unix())
		incDbps = MySQLIncDataBinlogPrepareStruct{
			Mytype:     dbDrive,
			User:       userPassword[0],
			Password:   userPassword[1],
			Host:       hostPort[0],
			Port:       uint16(port),
			BinlogFile: binlogFile,
			Pos:        uint32(pos),
			ServerId:   server_id,
			TableList:  checkTableMap,
		}
	}
	//if dbDrive == "oracle" {
	//	incDbps = &OracleIncDataBinlogPrepareStruct{}
	//}
	return incDbps
}
func IncDataBinlog() IncDataBinlogPrepareStruct {
	return IncDataBinlogPrepareStruct{}
}
