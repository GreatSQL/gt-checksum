package actions

import (
	"regexp"
)

// extractCollationFromDDL 从 CREATE TABLE DDL 中提取字符集和排序规则信息
// 返回值：(collation, charset)
func extractCollationFromDDL(ddl string) (string, string) {
	var collation, charset string

	// 提取表级别的 COLLATE 子句
	// 匹配模式：COLLATE[=\s]+collation_name（支持 COLLATE=xxx 和 COLLATE xxx 两种格式）
	collatePattern := regexp.MustCompile(`(?i)COLLATE\s*=?\s*([a-zA-Z0-9_]+)`)
	if matches := collatePattern.FindStringSubmatch(ddl); len(matches) > 1 {
		collation = matches[1]
	}

	// 提取表级别的 CHARACTER SET 或 CHARSET 子句
	charsetPattern := regexp.MustCompile(`(?i)(?:CHARACTER\s+SET|CHARSET)\s*=?\s*([a-zA-Z0-9_]+)`)
	if matches := charsetPattern.FindStringSubmatch(ddl); len(matches) > 1 {
		charset = matches[1]
	}

	return collation, charset
}
