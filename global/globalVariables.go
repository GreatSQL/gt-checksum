package global

import (
	"fmt"
	"net/url"
	"strings"
	"sync"

	"gt-checksum/go-log/log"
)

// ExtractCharsetFromDSN 从MySQL DSN连接字符串中提取charset参数值
// 如果未指定charset，则默认返回utf8mb4
func ExtractCharsetFromDSN(dsn string) string {
	const defaultCharset = "utf8mb4"

	queryIndex := strings.Index(dsn, "?")
	if queryIndex == -1 || queryIndex == len(dsn)-1 {
		return defaultCharset
	}

	query := dsn[queryIndex+1:]
	if values, err := url.ParseQuery(query); err == nil {
		if charset := strings.TrimSpace(values.Get("charset")); charset != "" {
			return charset
		}
		for key, value := range values {
			if strings.EqualFold(strings.TrimSpace(key), "charset") && len(value) > 0 {
				charset := strings.TrimSpace(value[0])
				if charset != "" {
					return charset
				}
			}
		}
	}

	for _, pair := range strings.FieldsFunc(query, func(r rune) bool {
		return r == '&' || r == ';'
	}) {
		key, value, found := strings.Cut(pair, "=")
		if !found {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(key), "charset") {
			charset := strings.TrimSpace(value)
			if charset != "" {
				return charset
			}
		}
	}

	return defaultCharset
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

const (
	SkipDiffsYes    = "yes"
	SkipDiffsDDLYes = "DDL-yes"
)

// SkippedTable 存储跳过的表信息
type SkippedTable struct {
	Schema      string
	Table       string
	CheckObject string
	Reason      string
	Diffs       string
}

// SkippedTables 存储所有跳过的表信息
var SkippedTables []SkippedTable
var skippedTablesMutex sync.Mutex

// AddSkippedTable 添加跳过的表信息到全局变量，避免重复添加
func AddSkippedTable(schema, table, checkObject, reason string) {
	AddSkippedTableWithDiffs(schema, table, checkObject, reason, SkipDiffsYes)
}

func AddSkippedTableWithDiffs(schema, table, checkObject, reason, diffs string) {
	skippedTablesMutex.Lock()
	defer skippedTablesMutex.Unlock()

	normalizedDiffs := strings.TrimSpace(diffs)
	if normalizedDiffs == "" {
		normalizedDiffs = SkipDiffsYes
	}

	// 检查是否已经存在相同的表记录
	for i, existing := range SkippedTables {
		if existing.Schema == schema && existing.Table == table && existing.CheckObject == checkObject {
			if existing.Diffs != SkipDiffsDDLYes && normalizedDiffs == SkipDiffsDDLYes {
				SkippedTables[i].Diffs = normalizedDiffs
				if strings.TrimSpace(reason) != "" {
					SkippedTables[i].Reason = reason
				}
			}
			if strings.TrimSpace(SkippedTables[i].Reason) == "" && strings.TrimSpace(reason) != "" {
				SkippedTables[i].Reason = reason
			}
			return
		}
	}

	// 添加新的跳过记录
	SkippedTables = append(SkippedTables, SkippedTable{
		Schema:      schema,
		Table:       table,
		CheckObject: checkObject,
		Reason:      reason,
		Diffs:       normalizedDiffs,
	})
	if Wlog != nil {
		Wlog.Warn(fmt.Sprintf("Skipping table %s.%s for %s check: %s", schema, table, checkObject, reason))
	}
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

func ResetRuntimeState() {
	skippedTablesMutex.Lock()
	SkippedTables = nil
	skippedTablesMutex.Unlock()

	DroppedTables = nil
	HasInvisibleColumnMismatch = false
	SourceMySQLVersion = MySQLVersionInfo{}
	DestMySQLVersion = MySQLVersionInfo{}
}

type TableAllColumnInfoS struct {
	SColumnInfo, DColumnInfo []map[string]string //表的所有列信息
}
