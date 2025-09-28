package global

import (
	"gt-checksum/go-log/log"
)

/*
初始化日志文件
*/
var Wlog *log.Logger

// DroppedTables 存储已经被标记为需要删除的表的列表
// 格式为 "schema.table"
var DroppedTables []string

type TableAllColumnInfoS struct {
	SColumnInfo, DColumnInfo []map[string]string //表的所有列信息
}
