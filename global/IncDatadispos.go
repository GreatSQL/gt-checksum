package global

import (
	"bytes"
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"strings"
)

type IncDataDisposInterface interface {
	readIncData()    //读取增量数据
}
type MySQLIncDataDisposStruct struct {}
type OraceIncDataDisposStruct struct {}
type IncDataDisposFunStruct struct {}


//var binlogInfo = make(map[string]string)
func (my MySQLIncDataDisposStruct) binlogGtidDispos(pos *bytes.Buffer){
	tmpa := fmt.Sprintf("%v",pos)
	//fmt.Println(tmpa)
	tmpb := strings.Split(tmpa,"\n")
	fmt.Println(tmpb)
	//var tmpc []string
	//for i := range tmpb{
	//	fmt.Println(tmpb[i])
	//}

}
func (my MySQLIncDataDisposStruct) readIncData(){
	cfg := replication.BinlogSyncerConfig{
		ServerID: 1613306,
		Flavor: "mysql",
		Host:"172.16.50.162",
		Port: 3306,
		User: "pcms",
		Password: "pcms@123",
	}
	syncer := replication.NewBinlogSyncer(cfg)
	streamer, _ := syncer.StartSync(mysql.Position{ "mysql-bin.000007", 653178})
	for {
	//	fmt.Println(streamer.DumpEvents())
		ev, _ := streamer.GetEvent(context.Background())
		var a string
		buf := bytes.NewBufferString(a)
		ev.Dump(buf)
		my.binlogGtidDispos(buf)
		fmt.Println("------------------")
	}


}
func (my OraceIncDataDisposStruct) readIncData(){

}
func (idd IncDataDisposFunStruct) IncDataDispos(iddi IncDataDisposInterface){
	iddi.readIncData()
}
