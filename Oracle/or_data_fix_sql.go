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
	var targetSchema = or.Schema // 默认使用目标schema

	// 处理需要删除的索引
	for _, v := range e {
		var strsql string
		switch or.IndexType {
		case "pri":
			strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP PRIMARY KEY;", targetSchema, or.Table)
		case "uni":
			strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP INDEX %s;", targetSchema, or.Table, v)
		case "mul":
			strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP INDEX %s;", targetSchema, or.Table, v)
		}
		sqlS = append(sqlS, strsql)
	}

	// 处理需要添加的索引
	for _, v := range f {
		var strsql string
		switch or.IndexType {
		case "pri":
			strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD PRIMARY KEY(%s);", targetSchema, or.Table, strings.Join(or.IndexColumn, ","))
		case "uni":
			strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD UNIQUE INDEX %s(%s);", targetSchema, or.Table, v, strings.Join(or.IndexColumn, ","))
		case "mul":
			strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD INDEX %s(%s);", targetSchema, or.Table, v, strings.Join(or.IndexColumn, ","))
		}
		sqlS = append(sqlS, strsql)
	}

	return sqlS
}

func (or *OracleDataAbnormalFixStruct) FixAlterColumnSqlDispos(alterType string, columnDataType []string, columnSeq int, lastColumn, curryColumn string, logThreadSeq int64) string {
	return ""
}

func (or *OracleDataAbnormalFixStruct) FixAlterColumnSqlGenerate(modifyColumn []string, logThreadSeq int64) []string {
	var alterSql []string
	if len(modifyColumn) > 0 {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s", or.Schema, or.Table, strings.Join(modifyColumn, ",")))
	}
	return alterSql
}

// FixAlterColumnAndIndexSqlGenerate 合并列修复和索引修复操作
// 注意：Oracle不支持在单个ALTER TABLE语句中合并列和索引操作
func (or *OracleDataAbnormalFixStruct) FixAlterColumnAndIndexSqlGenerate(columnOperations, indexOperations []string, logThreadSeq int64) []string {
	// Oracle不支持在单个ALTER TABLE语句中合并列和索引操作
	// 分别生成列修复和索引修复SQL
	var alterSql []string
	
	// 生成列修复SQL
	if len(columnOperations) > 0 {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s", or.Schema, or.Table, strings.Join(columnOperations, ",")))
	}
	
	// 生成索引修复SQL
	if len(indexOperations) > 0 {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s", or.Schema, or.Table, strings.Join(indexOperations, ",")))
	}
	
	return alterSql
}

// FixAlterIndexSqlGenerate 合并索引操作
func (or *OracleDataAbnormalFixStruct) FixAlterIndexSqlGenerate(indexOperations []string, logThreadSeq int64) []string {
	var alterSql []string
	if len(indexOperations) > 0 {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s", or.Schema, or.Table, strings.Join(indexOperations, ",")))
	}
	return alterSql
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