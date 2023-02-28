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
	vlog = fmt.Sprintf("(%d)  Oracle DB check table %s.%s starts to generate insert repair statement.", logThreadSeq, or.Schema, or.Table)
	global.Wlog.Debug(vlog)
	////处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
	//for i := range or.ColData {
	//	var tmpcolumnName string
	//	if !strings.Contains(or.RowData, "/*go actions columnData*/") {
	//		insertSql = fmt.Sprintf("insert into \"%s\".\"%s\" values(%s) ", strings.ToUpper(or.Schema), or.Table, or.RowData)
	//	}
	//	tmprowSlic := strings.Split(or.RowData, "/*go actions columnData*/")
	//	tmpcolumnName = fmt.Sprintf("'%s'", tmprowSlic[i])
	//	if strings.ToUpper(colData[i]["dataType"]) == "DATE" {
	//		tmpColumnSeq, _ := strconv.Atoi(fmt.Sprintf("%v", colData[i]["columnSeq"]))
	//		tmprowSLIC := strings.ReplaceAll(tmprowSlic[tmpColumnSeq-1], "'", "")
	//		tmpcolumnName = fmt.Sprintf("TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')", tmprowSLIC)
	//	}
	//	if strings.Contains(strings.ToUpper(colData[i]["dataType"]), "TIMESTAMP") {
	//		tmpColumnSeq, _ := strconv.Atoi(fmt.Sprintf("%v", colData[i]["columnSeq"]))
	//		tmprowSLIC := strings.ReplaceAll(tmprowSlic[tmpColumnSeq-1], "'", "")
	//		tmpcolumnName = fmt.Sprintf("TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS')", tmprowSLIC)
	//	}
	//	valuesNameSeq = append(valuesNameSeq, tmpcolumnName)
	//}
	//queryColumn := strings.Join(valuesNameSeq, ",")
	//if strings.Contains(queryColumn, "'<nil>'") {
	//	insertSql = fmt.Sprintf("insert into \"%s\".\"%s\" values(%s);", strings.ToUpper(or.Schema), or.Table, strings.ReplaceAll(queryColumn, "'<nil>'", "NULL"))
	//} else {
	//	insertSql = fmt.Sprintf("insert into \"%s\".\"%s\" values(%s);", strings.ToUpper(or.Schema), or.Table, queryColumn)
	//}
	//if sourceDrive == "mysql" && strings.Contains(insertSql,"'',")
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
			insertSql = fmt.Sprintf("insert into \"%s\".\"%s\" values(%s);", strings.ToUpper(or.Schema), or.Table, queryColumn)
		}
		if or.DatafixType == "table" {
			insertSql = fmt.Sprintf("insert into \"%s\".\"%s\" values(%s)", strings.ToUpper(or.Schema), or.Table, queryColumn)
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
	if or.IndexType == "mui" {
		var FB, AS []string
		for _, i := range colData {
			FB = append(FB, i["columnName"])
		}
		rowData := strings.ReplaceAll(or.RowData, "/*go actions columnData*/<nil>/*go actions columnData*/", "/*go actions columnData*/greatdbNull/*go actions columnData*/")
		for k, v := range strings.Split(rowData, "/*go actions columnData*/") {
			if v == "<nil>" {
				AS = append(AS, fmt.Sprintf(" %s is null ", FB[k]))
			} else if v == "<entry>" {
				AS = append(AS, fmt.Sprintf(" %s = ''", FB[k]))
			} else if v == acc["double"] {
				AS = append(AS, fmt.Sprintf("  concat(%s,'') = '%s'", FB[k], v))
			} else {
				AS = append(AS, fmt.Sprintf(" %s = '%s' ", FB[k], v))
			}
		}
		deleteSqlWhere = strings.Join(AS, " and ")
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
						AS = append(AS, fmt.Sprintf(" %s is null ", or.IndexColumn[l]))
					} else if v == "<entry>" {
						AS = append(AS, fmt.Sprintf(" %s = ''", FB[k]))
					} else if v == acc["double"] {
						AS = append(AS, fmt.Sprintf("  concat(%s,'') = '%s'", or.IndexColumn[l], v))
					} else {
						AS = append(AS, fmt.Sprintf(" %s = '%s' ", or.IndexColumn[l], v))
					}
				}
				deleteSqlWhere = strings.Join(AS, " and ")
			}
		}
	}
	if len(deleteSqlWhere) > 0 {
		if or.DatafixType == "file" {
			deleteSql = fmt.Sprintf("delete from \"%s\".\"%s\" where %s;", or.Schema, or.Table, deleteSqlWhere)
		}
		if or.DatafixType == "table" {
			deleteSql = fmt.Sprintf("delete from \"%s\".\"%s\" where %s", or.Schema, or.Table, deleteSqlWhere)
		}
	}
	return deleteSql, nil
}

func (or *OracleDataAbnormalFixStruct) FixAlterSqlExec(e, f []string, si map[string][]string, sourceDrive string, logThreadSeq int64) ([]string, error) {
	var sqlS []string
	for _, v := range e {
		var c []string
		for _, vi := range si[v] {
			c = append(c, strings.TrimSpace(strings.Split(vi, "/*actions Column Type*/")[0]))
		}
		switch or.IndexType {
		case "pri":
			strsql = fmt.Sprintf("alter table %s.%s add primary key(`%s`);", or.Schema, or.Table, strings.Join(c, "`,`"))
		case "uni":
			strsql = fmt.Sprintf("alter table %s.%s add unique index %s(`%s`);", or.Schema, or.Table, v, strings.Join(c, "`,`"))
		case "mul":
			strsql = fmt.Sprintf("alter table %s.%s add index %s(`%s`);", or.Schema, or.Table, v, strings.Join(c, "`,`"))
		}
		sqlS = append(sqlS, strsql)
	}
	for _, v := range f {
		switch or.IndexType {
		case "pri":
			strsql = fmt.Sprintf("alter table %s.%s drop primary key;", or.Schema, or.Table)
		case "uni":
			strsql = fmt.Sprintf("alter table %s.%s drop index %s;", or.Schema, or.Table, v)
		case "mul":
			strsql = fmt.Sprintf("alter table %s.%s drop index %s;", or.Schema, or.Table, v)
		}
		sqlS = append(sqlS, strsql)
	}
	return sqlS, nil
}
