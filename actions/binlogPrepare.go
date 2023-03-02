package actions

import (
	"bytes"
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func PareBinlog() {
	cfg := replication.BinlogSyncerConfig{
		ServerID: 1613306,
		Flavor:   "mysql",
		Host:     "172.16.50.162",
		Port:     3306,
		User:     "pcms",
		Password: "pcms@123",
	}
	syncer := replication.NewBinlogSyncer(cfg)
	// Start sync with specified binlog file and position
	// or you can start a gtid replication like

	//streamer, _ := syncer.StartSyncGTID()
	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	// the mariadb GTID set likes this "0-1-100"
	streamer, _ := syncer.StartSync(mysql.Position{"mysql-bin.000007", 651861})
	for {
		ev, _ := streamer.GetEvent(context.Background())
		var a string
		buf := bytes.NewBufferString(a)
		ev.Dump(buf)
		fmt.Println("--------")
		fmt.Println(buf)
		fmt.Println("--------")

	}

}
