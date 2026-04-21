package mysql

import (
	"fmt"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
	"strings"
)

// FixTableCharsetSqlGenerate 生成表级别字符集转换的SQL语句
func (my *MysqlDataAbnormalFixStruct) FixTableCharsetSqlGenerate(charset, collation string, logThreadSeq int64) []string {
	var (
		alterSql     []string
		targetSchema = my.Schema // 默认使用目标schema
	)

	// 防护空 charset：当 LEFT JOIN COLLATIONS 失败时 charset 可能为空，
	// 此时从 collation 名推断 charset，最终兜底为 utf8mb4。
	trimmedCharset := strings.TrimSpace(charset)
	if trimmedCharset == "" {
		trimmedCharset = schemacompat.InferCharsetFromCollation(collation)
		if trimmedCharset == "" {
			trimmedCharset = "utf8mb4"
		}
		if global.Wlog != nil {
			vlog := fmt.Sprintf("(%d) Table charset was empty, inferred as %s from collation %s for %s.%s",
				logThreadSeq, trimmedCharset, collation, targetSchema, my.Table)
			global.Wlog.Warn(vlog)
		}
	}

	// 生成表级别字符集转换的SQL语句
	if strings.TrimSpace(collation) == "" {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s CONVERT TO CHARACTER SET %s;",
			mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), trimmedCharset))
	} else {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s CONVERT TO CHARACTER SET %s COLLATE %s;",
			mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), trimmedCharset, collation))
	}

	// 添加日志，方便调试
	vlog := fmt.Sprintf("(%d) Generated table charset conversion SQL: %s", logThreadSeq, alterSql[0])
	if global.Wlog != nil {
		global.Wlog.Debug(vlog)
	}

	return alterSql
}

// FixTableAutoIncrementSqlGenerate generates table-level AUTO_INCREMENT fix SQL.
func (my *MysqlDataAbnormalFixStruct) FixTableAutoIncrementSqlGenerate(nextValue int64, logThreadSeq int64) []string {
	var (
		alterSql     []string
		targetSchema = my.Schema
	)

	alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s AUTO_INCREMENT=%d;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), nextValue))

	vlog := fmt.Sprintf("(%d) Generated table AUTO_INCREMENT SQL: %s", logThreadSeq, alterSql[0])
	if global.Wlog != nil {
		global.Wlog.Debug(vlog)
	}

	return alterSql
}
