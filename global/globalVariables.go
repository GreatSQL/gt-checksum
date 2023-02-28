package global

import (
	"gt-checksum/go-log/log"
)

/*
   初始化日志文件
*/
var Wlog *log.Logger

type TableAllColumnInfoS struct {
	SColumnInfo, DColumnInfo []map[string]string //表的所有列信息
}
