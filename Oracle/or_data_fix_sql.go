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
}

func (or *OracleDataAbnormalFixStruct) FixInsertSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	//查询该表的列名和列信息
	var (
		insertSql     string
		valuesNameSeq []string
	)

	//colData := or.ColData
	vlog = fmt.Sprintf("(%d) Oracle DB check table %s.%s starts to generate insert repair statement.", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Debug(vlog)

	for k, v := range strings.Split(or.RowData, "/*go actions columnData*/") {
		var tmpcolumnName string
		if strings.EqualFold(v, "<entry>") {
			tmpcolumnName = fmt.Sprintf("''")
		} else if strings.EqualFold(v, "<nil>") {
			tmpcolumnName = fmt.Sprintf("NULL")
		} else {
			if strings.ToUpper(or.ColData[k]["dataType"]) == "DATETIME" {
				tmpcolumnName = fmt.Sprintf("TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')", v)
			} else if strings.Contains(strings.ToUpper(or.ColData[k]["dataType"]), "TIMESTAMP") {
				tmpcolumnName = fmt.Sprintf("TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')", v)
			} else {
				tmpcolumnName = fmt.Sprintf("'%v'", v)
			}
		}
		valuesNameSeq = append(valuesNameSeq, tmpcolumnName)
	}
	if len(valuesNameSeq) > 0 {
		queryColumn := strings.Join(valuesNameSeq, ",")
		if or.DatafixType == "file" {
			insertSql = fmt.Sprintf("INSERT INTO \"%s\".\"%s\" VALUES(%s);", or.Schema, or.Table, queryColumn)
		}
		if or.DatafixType == "table" {
			insertSql = fmt.Sprintf("INSERT INTO \"%s\".\"%s\" VALUES(%s)", or.Schema, or.Table, queryColumn)
		}
	}
	return insertSql, nil
}

func (or *OracleDataAbnormalFixStruct) FixDeleteSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	var (
		deleteSql, deleteSqlWhere string
		ad                        = make(map[string]int)
		acc                       = make(map[string]string) //判断特殊数据类型
	)

	colData := or.ColData
	for _, i := range colData {
		cls, _ := strconv.Atoi(fmt.Sprintf("%s", i["columnSeq"]))
		ad[i["columnName"]] = cls
		if strings.HasPrefix(i["dataType"], "double(") {
			acc["double"] = i["columnName"]
		}
	}
	vlog = fmt.Sprintf("(%d)  MySQL DB check table %s.%s starts to generate delete repair statement.", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Debug(vlog)
	vlog = fmt.Sprintf("(%d) MySQL DB check table %s.%s Generate delete repair statement based on unique index.", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Debug(vlog)
	if or.IndexType == "mul" {
		var FB, AS []string
		for _, i := range colData {
			FB = append(FB, i["columnName"])
		}
		rowData := strings.ReplaceAll(or.RowData, "/*go actions columnData*/<nil>/*go actions columnData*/", "/*go actions columnData*/greatdbNull/*go actions columnData*/")
		for k, v := range strings.Split(rowData, "/*go actions columnData*/") {
			if v == "<nil>" {
				AS = append(AS, fmt.Sprintf(" %s IS NULL ", FB[k]))
			} else if v == "<entry>" {
				AS = append(AS, fmt.Sprintf(" %s = ''", FB[k]))
			} else if v == acc["double"] {
				AS = append(AS, fmt.Sprintf("  CONCAT(%s,'') = '%s'", FB[k], v))
			} else {
				AS = append(AS, fmt.Sprintf(" %s = '%s' ", FB[k], v))
			}
		}
		deleteSqlWhere = strings.Join(AS, " AND ")
	}
	if or.IndexType == "pri" || or.IndexType == "uni" {
		var FB []string
		for _, i := range colData {
			for _, v := range or.IndexColumn {
				if strings.EqualFold(v, i["columnName"]) {
					FB = append(FB, i["columnSeq"])
				}
			}
		}
		var AS []string
		for k, v := range strings.Split(or.RowData, "/*go actions columnData*/") {
			for l, I := range FB {
				if I == strconv.Itoa(k+1) {
					if v == "<nil>" {
						AS = append(AS, fmt.Sprintf(" %s IS NULL ", or.IndexColumn[l]))
					} else if v == "<entry>" {
						AS = append(AS, fmt.Sprintf(" %s = ''", FB[k]))
					} else if v == acc["double"] {
						AS = append(AS, fmt.Sprintf("  CONCAT(%s,'') = '%s'", or.IndexColumn[l], v))
					} else {
						AS = append(AS, fmt.Sprintf(" %s = '%s' ", or.IndexColumn[l], v))
					}
				}
				deleteSqlWhere = strings.Join(AS, " AND ")
			}
		}
	}
	if len(deleteSqlWhere) > 0 {
		if or.DatafixType == "file" {
			deleteSql = fmt.Sprintf("DELETE FROM \"%s\".\"%s\" WHERE %s;", or.Schema, or.Table, deleteSqlWhere)
		}
		if or.DatafixType == "table" {
			deleteSql = fmt.Sprintf("DELETE FROM \"%s\".\"%s\" WHERE %s", or.Schema, or.Table, deleteSqlWhere)
		}
	}
	return deleteSql, nil
}

func (or *OracleDataAbnormalFixStruct) FixAlterIndexSqlExec(e, f []string, si map[string][]string, sourceDrive string, logThreadSeq int64) []string {
	var sqlS []string
	for _, v := range e {
		var c []string
		for _, vi := range si[v] {
			if len(strings.Split(vi, "/*actions Column Type*/")) > 0 {
				c = append(c, strings.TrimSpace(strings.Split(vi, "/*actions Column Type*/")[0]))
			}
		}
		switch or.IndexType {
		case "pri":
			strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD PRIMARY KEY(`%s`);", or.Schema, or.Table, strings.Join(c, "`,`"))
		case "uni":
			strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD UNIQUE INDEX %s(`%s`);", or.Schema, or.Table, v, strings.Join(c, "`,`"))
		case "mul":
			strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD INDEX %s(`%s`);", or.Schema, or.Table, v, strings.Join(c, "`,`"))
		}
		sqlS = append(sqlS, strsql)
	}
	for _, v := range f {
		switch or.IndexType {
		case "pri":
			strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP PRIMARY KEY;", or.Schema, or.Table)
		case "uni":
			strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP INDEX %s;", or.Schema, or.Table, v)
		case "mul":
			strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP INDEX %s;", or.Schema, or.Table, v)
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
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE `%s`.`%s` %s", or.Schema, or.Table, strings.Join(modifyColumn, ",")))
	}
	return alterSql
}

// FixTableCharsetSqlGenerate 生成表级别字符集转换的SQL语句
// 注意：Oracle不支持MySQL的CONVERT TO CHARACTER SET语法，这里只是为了满足接口要求
func (or *OracleDataAbnormalFixStruct) FixTableCharsetSqlGenerate(charset, collation string, logThreadSeq int64) []string {
	// Oracle不支持MySQL的CONVERT TO CHARACTER SET语法，返回空数组
	vlog = fmt.Sprintf("(%d) Oracle does not support CONVERT TO CHARACTER SET syntax, skipping charset conversion for %s.%s",
		logThreadSeq, or.Schema, or.Table)
	global.Wlog.Warn(vlog)
	return []string{}
}
