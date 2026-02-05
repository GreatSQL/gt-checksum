package global

import (
	"fmt"
	"strings"
	"sync"

	"gt-checksum/go-log/log"
)

// ExtractCharsetFromDSN 从MySQL DSN连接字符串中提取charset参数值
// 如果未指定charset，则默认返回utf8mb4
func ExtractCharsetFromDSN(dsn string) string {
	// 解析DSN格式：mysql|user:password@tcp(host:port)/dbname?charset=utf8mb4
	// 或者标准DSN格式：user:password@tcp(host:port)/dbname?charset=utf8mb4
	if strings.Contains(dsn, "?charset=") {
		// 提取?charset=后面的部分
		charsetPart := dsn[strings.Index(dsn, "?charset=")+9:]
		// 检查是否有其他参数
		if strings.Contains(charsetPart, "&") {
			charsetPart = charsetPart[:strings.Index(charsetPart, "&")]
		}
		return strings.TrimSpace(charsetPart)
	}
	// 默认返回utf8mb4
	return "utf8mb4"
}

/*
初始化日志文件
*/
var Wlog *log.Logger

// DroppedTables 存储已经被标记为需要删除的表的列表
// 格式为 "schema.table"
var DroppedTables []string

// HasInvisibleColumnMismatch 标记是否存在INVISIBLE列差异导致的表结构不匹配
var HasInvisibleColumnMismatch bool

// SkippedTable 存储跳过的表信息
type SkippedTable struct {
	Schema      string
	Table       string
	CheckObject string
	Reason      string
}

// SkippedTables 存储所有跳过的表信息
var SkippedTables []SkippedTable
var skippedTablesMutex sync.Mutex

// AddSkippedTable 添加跳过的表信息到全局变量，避免重复添加
func AddSkippedTable(schema, table, checkObject, reason string) {
	skippedTablesMutex.Lock()
	defer skippedTablesMutex.Unlock()

	// 检查是否已经存在相同的表记录
	for _, existing := range SkippedTables {
		if existing.Schema == schema && existing.Table == table && existing.CheckObject == checkObject {
			// 已经存在相同的记录，不重复添加
			return
		}
	}

	// 添加新的跳过记录
	SkippedTables = append(SkippedTables, SkippedTable{
		Schema:      schema,
		Table:       table,
		CheckObject: checkObject,
		Reason:      reason,
	})
	Wlog.Warn(fmt.Sprintf("Skipping table %s.%s for %s check: %s", schema, table, checkObject, reason))
}

// GetSkippedTables 获取所有跳过的表信息
func GetSkippedTables() []SkippedTable {
	skippedTablesMutex.Lock()
	defer skippedTablesMutex.Unlock()
	// 返回副本，避免并发问题
	result := make([]SkippedTable, len(SkippedTables))
	copy(result, SkippedTables)
	return result
}

type TableAllColumnInfoS struct {
	SColumnInfo, DColumnInfo []map[string]string //表的所有列信息
}
