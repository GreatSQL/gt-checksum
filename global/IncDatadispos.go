package global

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type IncDataDisposInterface interface {
	readIncData() //读取增量数据
}
type MySQLIncDataDisposStruct struct{}
type OraceIncDataDisposStruct struct{}
type IncDataDisposFunStruct struct{}

func (my MySQLIncDataDisposStruct) binlogGtidDispos(pos *bytes.Buffer) {
	tmpa := fmt.Sprintf("%v", pos)
	_ = strings.Split(tmpa, "\n") // Split result not used
}
func (my MySQLIncDataDisposStruct) readIncData() {
	cfg := replication.BinlogSyncerConfig{
		ServerID: 1613306,
		Flavor:   "mysql",
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "gtchecksum",
		Password: "gtchecksum",
	}
	syncer := replication.NewBinlogSyncer(cfg)
	streamer, _ := syncer.StartSync(mysql.Position{Name: "mysql-bin.000007", Pos: 653178})
	for {
		ev, _ := streamer.GetEvent(context.Background())
		var a string
		buf := bytes.NewBufferString(a)
		ev.Dump(buf)
		my.binlogGtidDispos(buf)
	}

}
func (my OraceIncDataDisposStruct) readIncData() {

}
func (idd IncDataDisposFunStruct) IncDataDispos(iddi IncDataDisposInterface) {
	iddi.readIncData()
}
