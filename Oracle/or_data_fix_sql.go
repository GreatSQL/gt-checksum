package oracle

import (
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"strconv"
	"strings"
)

type OracleDataAbnormalFixStruct struct {
	Schema          string
	Table           string
	RowData         string
	SourceDevice    string
	DestDevice      string
	Sqlwhere        string
	IndexColumnType string
	ColData         []map[string]string
	IndexType       string
	IndexColumn     []string
	DatafixType     string
	SourceSchema    string // 源端schema，用于处理数据库映射关系
}

/*
Oracle 生成insert修复语句
*/
func (or *OracleDataAbnormalFixStruct) FixInsertSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	//查询该表的列名和列信息
	var (
		insertSql     string
		valuesNameSeq []string
		targetSchema  = or.Schema // 默认使用目标schema
	)

	vlog := fmt.Sprintf("(%d) Generating INSERT repair statement for %s.%s (target: %s)", logThreadSeq, or.Schema, or.Table, targetSchema)
	global.Wlog.Debug(vlog)

	// 检查ColData是否为空，如果为空，尝试从行数据中推断列信息
	if len(or.ColData) == 0 {
		vlog = fmt.Sprintf("(%d) Warning: No column data available for table %s.%s, attempting to infer from row data",
			logThreadSeq, targetSchema, or.Table)
		global.Wlog.Warn(vlog)

		// 从行数据中推断列数量
		rowParts := strings.Split(or.RowData, "/*go actions columnData*/")
		if len(rowParts) == 0 {
			return "", fmt.Errorf("no column data available and empty row data for table %s.%s (mapping: %s->%s)",
				targetSchema, or.Table, or.SourceSchema, or.Schema)
		}

		// 创建临时列数据结构
		tempColData := make([]map[string]string, len(rowParts))
		for i := range rowParts {
			tempColData[i] = map[string]string{
				"columnName": fmt.Sprintf("col_%d", i+1),
				"columnSeq":  strconv.Itoa(i + 1),
				"dataType":   "VARCHAR2", // Oracle默认类型
			}
		}
		or.ColData = tempColData

		vlog = fmt.Sprintf("(%d) Created temporary column structure with %d columns for table %s.%s",
			logThreadSeq, len(or.ColData), targetSchema, or.Table)
		global.Wlog.Debug(vlog)
	}

	// Oracle处理时间戳和日期格式
	rowParts := strings.Split(or.RowData, "/*go actions columnData*/")
	for k, v := range rowParts {
		var tmpcolumnName string
		if strings.EqualFold(v, "<entry>") {
			tmpcolumnName = fmt.Sprintf("''")
		} else if strings.EqualFold(v, "<nil>") {
			tmpcolumnName = fmt.Sprintf("NULL")
		} else {
			// 检查索引是否越界
			if k < len(or.ColData) {
				if dataType, ok := or.ColData[k]["dataType"]; ok {
					if strings.ToUpper(dataType) == "DATE" {
						tmpcolumnName = fmt.Sprintf("TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')", v)
					} else if strings.Contains(strings.ToUpper(dataType), "TIMESTAMP") {
						tmpcolumnName = fmt.Sprintf("TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS.FF')", v)
					} else {
						tmpcolumnName = fmt.Sprintf("'%v'", v)
					}
				} else {
					// 如果没有dataType字段，使用默认格式
					tmpcolumnName = fmt.Sprintf("'%v'", v)
				}
			} else {
				// 如果索引越界，使用默认格式
				tmpcolumnName = fmt.Sprintf("'%v'", v)
				vlog = fmt.Sprintf("(%d) Warning: Column index %d exceeds available column data for %s.%s",
					logThreadSeq, k, targetSchema, or.Table)
				global.Wlog.Warn(vlog)
			}
		}
		valuesNameSeq = append(valuesNameSeq, tmpcolumnName)
	}

	// 构建INSERT语句
	insertSql = fmt.Sprintf("INSERT INTO %s.%s VALUES(%s)", targetSchema, or.Table, strings.Join(valuesNameSeq, ","))

	vlog = fmt.Sprintf("(%d) Generated INSERT SQL for %s.%s: %s", logThreadSeq, targetSchema, or.Table, insertSql)
	global.Wlog.Debug(vlog)

	return insertSql, nil
}

/*
Oracle 生成delete修复语句
*/
func (or *OracleDataAbnormalFixStruct) FixDeleteSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	var (
		deleteSql    string
		targetSchema = or.Schema // 默认使用目标schema
	)

	vlog := fmt.Sprintf("(%d) Generating DELETE repair statement for %s.%s (target: %s)", logThreadSeq, or.Schema, or.Table, targetSchema)
	global.Wlog.Debug(vlog)

	deleteSql = fmt.Sprintf("DELETE FROM %s.%s WHERE %s", targetSchema, or.Table, or.Sqlwhere)

	vlog = fmt.Sprintf("(%d) Generated DELETE SQL for %s.%s: %s", logThreadSeq, targetSchema, or.Table, deleteSql)
	global.Wlog.Debug(vlog)

	return deleteSql, nil
}

/*
Oracle 生成索引修复语句
*/
func (or *OracleDataAbnormalFixStruct) FixAlterIndexSqlExec(e, f []string, si map[string][]string, sourceDrive string, logThreadSeq int64) []string {
	var sqlS []string
	targetSchema := oracleIdentifier(or.Schema)
	targetTable := oracleIdentifier(or.Table)

	// 处理需要删除的索引（Oracle 语法）
	for _, v := range e {
		var strsql string
		switch or.IndexType {
		case "pri":
			// Oracle 支持 ALTER TABLE ... DROP PRIMARY KEY
			strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP PRIMARY KEY;", targetSchema, targetTable)
		case "uni", "mul":
			// Oracle DROP INDEX 是独立语句，非 ALTER TABLE ... DROP INDEX
			strsql = fmt.Sprintf("DROP INDEX %s.%s;", targetSchema, oracleIdentifier(v))
		}
		if strsql != "" {
			sqlS = append(sqlS, strsql)
		}
	}

	// 处理需要添加的索引（Oracle 语法）
	for _, v := range f {
		// 从 si map 提取该索引的列名（与 MySQL 路径一致）
		var cols []string
		for _, vi := range si[v] {
			parts := strings.Split(vi, "/*seq*/")
			if len(parts) > 0 {
				cols = append(cols, oracleIdentifier(strings.TrimSpace(parts[0])))
			}
		}
		// si 无数据时回退到 or.IndexColumn
		if len(cols) == 0 {
			for _, col := range or.IndexColumn {
				cols = append(cols, oracleIdentifier(col))
			}
		}
		colStr := strings.Join(cols, ", ")

		var strsql string
		switch or.IndexType {
		case "pri":
			strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD PRIMARY KEY (%s);", targetSchema, targetTable, colStr)
		case "uni":
			// Oracle 创建唯一索引：CREATE UNIQUE INDEX schema.name ON schema.table (cols)
			strsql = fmt.Sprintf("CREATE UNIQUE INDEX %s.%s ON %s.%s (%s);",
				targetSchema, oracleIdentifier(v), targetSchema, targetTable, colStr)
		case "mul":
			// Oracle 创建普通索引：CREATE INDEX schema.name ON schema.table (cols)
			strsql = fmt.Sprintf("CREATE INDEX %s.%s ON %s.%s (%s);",
				targetSchema, oracleIdentifier(v), targetSchema, targetTable, colStr)
		}
		if strsql != "" {
			sqlS = append(sqlS, strsql)
		}
	}

	return sqlS
}

func (or *OracleDataAbnormalFixStruct) FixAlterColumnSqlDispos(alterType string, columnDataType []string, columnSeq int, lastColumn, curryColumn string, logThreadSeq int64) string {
	return ""
}

func (or *OracleDataAbnormalFixStruct) FixAlterColumnSqlGenerate(modifyColumn []string, logThreadSeq int64) []string {
	var alterSql []string
	if len(modifyColumn) > 0 {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s",
			oracleIdentifier(or.Schema), oracleIdentifier(or.Table), strings.Join(modifyColumn, ",")))
	}
	return alterSql
}

// FixAlterColumnAndIndexSqlGenerate 合并列修复和索引修复操作
// 注意：Oracle不支持在单个ALTER TABLE语句中合并列和索引操作
func (or *OracleDataAbnormalFixStruct) FixAlterColumnAndIndexSqlGenerate(columnOperations, indexOperations []string, logThreadSeq int64) []string {
	var alterSql []string

	// 生成列修复SQL（仍使用 ALTER TABLE，Oracle 列操作支持）
	if len(columnOperations) > 0 {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s",
			oracleIdentifier(or.Schema), oracleIdentifier(or.Table), strings.Join(columnOperations, ",")))
	}

	// 索引修复SQL（CREATE INDEX / DROP INDEX）是独立语句，直接追加
	alterSql = append(alterSql, indexOperations...)

	return alterSql
}

// FixAlterIndexSqlGenerate 返回索引操作语句集合
// Oracle 的索引 DDL（CREATE INDEX / DROP INDEX）是独立语句，无需合并进 ALTER TABLE
func (or *OracleDataAbnormalFixStruct) FixAlterIndexSqlGenerate(indexOperations []string, logThreadSeq int64) []string {
	return indexOperations
}

// FixTableCharsetSqlGenerate 生成表级别字符集转换的SQL语句
// 注意：Oracle不支持MySQL的CONVERT TO CHARACTER SET语法，这里只是为了满足接口要求
func (or *OracleDataAbnormalFixStruct) FixTableCharsetSqlGenerate(charset, collation string, logThreadSeq int64) []string {
	// Oracle不支持MySQL的CONVERT TO CHARACTER SET语法，返回空数组
	vlog := fmt.Sprintf("(%d) Oracle does not support CONVERT TO CHARACTER SET syntax, skipping charset conversion for %s.%s",
		logThreadSeq, or.Schema, or.Table)
	global.Wlog.Warn(vlog)
	return []string{}
}

// FixTableAutoIncrementSqlGenerate exists only to satisfy the shared repair interface.
// Oracle does not support MySQL table-level AUTO_INCREMENT metadata repair.
func (or *OracleDataAbnormalFixStruct) FixTableAutoIncrementSqlGenerate(nextValue int64, logThreadSeq int64) []string {
	vlog := fmt.Sprintf("(%d) Oracle does not support AUTO_INCREMENT repair syntax, skipping for %s.%s",
		logThreadSeq, or.Schema, or.Table)
	global.Wlog.Warn(vlog)
	return []string{}
}
