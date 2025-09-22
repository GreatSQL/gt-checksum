package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"strconv"
	"strings"
)

type MysqlDataAbnormalFixStruct struct {
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

/*
  MySQL 生成insert修复语句
*/
func (my *MysqlDataAbnormalFixStruct) FixInsertSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	//查询该表的列名和列信息
	var (
		insertSql     string
		valuesNameSeq []string
	)
	vlog = fmt.Sprintf("(%d)  MySQL DB check table %s.%s starts to generate insert repair statement.", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Debug(vlog)
	//tmprowSlic := strings.Split(strings.TrimSpace(my.RowData), "/*go actions columnData*/")
	//处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
	for k, v := range strings.Split(my.RowData, "/*go actions columnData*/") {
		var tmpcolumnName string
		if strings.EqualFold(v, "<entry>") {
			tmpcolumnName = fmt.Sprintf("''")
		} else if strings.EqualFold(v, "<nil>") {
			tmpcolumnName = fmt.Sprintf("NULL")
		} else {
			if strings.ToUpper(my.ColData[k]["dataType"]) == "DATETIME" {
				tmpcolumnName = fmt.Sprintf("date_format('%s','%%Y-%%m-%%d %%H:%%i:%%s')", v)
			} else if strings.Contains(strings.ToUpper(my.ColData[k]["dataType"]), "TIMESTAMP") {
				tmpcolumnName = fmt.Sprintf("date_format('%s','%%Y-%%m-%%d %%H:%%i:%%s')", v)
			} else {
				tmpcolumnName = fmt.Sprintf("'%v'", v)
			}
		}
		valuesNameSeq = append(valuesNameSeq, tmpcolumnName)
	}
	//for i := range my.ColData {
	//	var tmpcolumnName string
	//	if !strings.Contains(my.RowData, "/*go actions columnData*/") {
	//		insertSql = fmt.Sprintf("insert into `%s`.`%s` values(%s) ", my.Schema, my.Table, my.RowData)
	//	}
	//	tmpcolumnName = fmt.Sprintf("'%s'", tmprowSlic[i])
	//	if strings.ToUpper(my.ColData[i]["dataType"]) == "DATETIME" {
	//		tmpColumnSeq, _ := strconv.Atoi(fmt.Sprintf("%v", my.ColData[i]["columnSeq"]))
	//		tmprowSLIC := strings.ReplaceAll(tmprowSlic[tmpColumnSeq-1], "'", "")
	//		tmpcolumnName = fmt.Sprintf("date_format('%s','%%Y-%%m-%%d %%H:%%i:%%s')", tmprowSLIC)
	//	}
	//	if strings.Contains(strings.ToUpper(my.ColData[i]["dataType"]), "TIMESTAMP") {
	//		tmpColumnSeq, _ := strconv.Atoi(fmt.Sprintf("%v", my.ColData[i]["columnSeq"]))
	//		tmprowSLIC := strings.ReplaceAll(tmprowSlic[tmpColumnSeq-1], "'", "")
	//		tmpcolumnName = fmt.Sprintf("date_format('%s','%%Y-%%m-%%d %%H:%%i:%%s')", tmprowSLIC)
	//	}
	//	valuesNameSeq = append(valuesNameSeq, tmpcolumnName)
	//}
	if len(valuesNameSeq) > 0 {
		queryColumn := strings.Join(valuesNameSeq, ",")
		insertSql = fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES(%s) ;", my.Schema, my.Table, queryColumn)
	}
	//if strings.Contains(queryColumn, "'<nil>'") {
	//	insertSql = fmt.Sprintf("insert into `%s`.`%s` values(%s) ;", my.Schema, my.Table, strings.ReplaceAll(queryColumn, "'<nil>'", "NULL"))
	//} else {
	//	insertSql = fmt.Sprintf("insert into `%s`.`%s` values(%s) ;", my.Schema, my.Table, queryColumn)
	//}
	//if sourceDrive == "godror" && strings.Contains(insertSql, ",'',") {
	//	insertSql = strings.ReplaceAll(insertSql, ",'',", ",null,")
	//}
	//fmt.Println(insertSql)
	return insertSql, nil
}

/*
  mysql 生成delete 修复语句
*/
func (my *MysqlDataAbnormalFixStruct) FixDeleteSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	var (
		deleteSql, deleteSqlWhere string
		ad                        = make(map[string]int)
		acc                       = make(map[string]string) //判断特殊数据类型
	)
	colData := my.ColData
	for _, i := range colData {
		cls, _ := strconv.Atoi(fmt.Sprintf("%s", i["columnSeq"]))
		ad[i["columnName"]] = cls
		if strings.HasPrefix(i["dataType"], "double(") {
			acc["double"] = i["columnName"]
		}
	}
	vlog = fmt.Sprintf("(%d)  MySQL DB check table %s.%s starts to generate delete repair statement.", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Debug(vlog)
	vlog = fmt.Sprintf("(%d) MySQL DB check table %s.%s Generate delete repair statement based on unique index.", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Debug(vlog)
	if my.IndexType == "mul" {
		var FB, AS []string
		for _, i := range colData {
			FB = append(FB, i["columnName"])
		}
		rowData := strings.ReplaceAll(my.RowData, "/*go actions columnData*/<nil>/*go actions columnData*/", "/*go actions columnData*/greatdbNull/*go actions columnData*/")
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
		deleteSqlWhere = strings.Join(AS, " and ")
	}
	if my.IndexType == "pri" || my.IndexType == "uni" {
		var FB []string
		for _, i := range colData {
			for _, v := range my.IndexColumn {
				if strings.EqualFold(v, i["columnName"]) {
					FB = append(FB, i["columnSeq"])
				}
			}
		}
		var AS []string
		for k, v := range strings.Split(my.RowData, "/*go actions columnData*/") {
			for l, I := range FB {
				if I == strconv.Itoa(k+1) {
					if v == "<nil>" {
						AS = append(AS, fmt.Sprintf(" %s IS NULL ", my.IndexColumn[l]))
					} else if v == "<entry>" {
						AS = append(AS, fmt.Sprintf(" %s = '' ", my.IndexColumn[l]))
					} else if v == acc["double"] {
						AS = append(AS, fmt.Sprintf("  CONCAT(%s,'') = '%s'", my.IndexColumn[l], v))
					} else {
						AS = append(AS, fmt.Sprintf(" %s = '%s' ", my.IndexColumn[l], v))
					}
				}
				deleteSqlWhere = strings.Join(AS, " AND ")
			}
		}
	}
	if len(deleteSqlWhere) > 0 {
		deleteSql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;", my.Schema, my.Table, deleteSqlWhere)
	}
	return deleteSql, nil
}
func (my *MysqlDataAbnormalFixStruct) FixAlterIndexSqlExec(e, f []string, si map[string][]string, sourceDrive string, logThreadSeq int64) []string {
	var sqlS []string
	for _, v := range e {
		var c []string
		for _, vi := range si[v] {
			if len(strings.Split(vi, "/*actions Column Type*/")) > 0 {
				c = append(c, strings.TrimSpace(strings.Split(vi, "/*actions Column Type*/")[0]))
			}
		}
		switch my.IndexType {
		case "pri":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PRIMARY KEY(`%s`);", my.Schema, my.Table, strings.Join(c, "`,`"))
		case "uni":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD UNIQUE INDEX %s(`%s`);", my.Schema, my.Table, v, strings.Join(c, "`,`"))
		case "mul":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD INDEX %s(`%s`);", my.Schema, my.Table, v, strings.Join(c, "`,`"))
		}
		sqlS = append(sqlS, strsql)
	}
	for _, v := range f {
		switch my.IndexType {
		case "pri":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PRIMARY KEY;", my.Schema, my.Table)
		case "uni":
			strsql = fmt.Sprintf("ALTER TABLE `%s.`%s DROP INDEX %s;", my.Schema, my.Table, v)
		case "mul":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP INDEX %s;", my.Schema, my.Table, v)
		}
		sqlS = append(sqlS, strsql)
	}
	return sqlS
}

func (my *MysqlDataAbnormalFixStruct) FixAlterColumnSqlDispos(alterType string, columnDataType []string, columnSeq int, lastColumn, curryColumn string, logThreadSeq int64) string {
	var sqlS string
	charsetN := ""
	if columnDataType[1] != "null" {
		charsetN = fmt.Sprintf("CHARACTER SET %s", columnDataType[1])
	}
	collationN := ""
	if columnDataType[2] != "null" {
		collationN = fmt.Sprintf("COLLATE %s", columnDataType[2])
	}
	nullS := ""
	if strings.ToUpper(columnDataType[3]) == "NO" {
		nullS = "NOT NULL"
	}
	collumnDefaultN := ""
	if columnDataType[4] == "empty" {
		collumnDefaultN = fmt.Sprintf("DEFAULT ''")
	} else if columnDataType[4] == "NULL" {
		collumnDefaultN = ""
	} else {
		collumnDefaultN = fmt.Sprintf("DEFAULT '%s'", columnDataType[4])
	}
	commantS := ""
	if columnDataType[5] != "empty" {
		commantS = fmt.Sprintf("COMMENT '%s'", columnDataType[5])
	}
	columnLocation := ""
	if columnSeq == 0 {
		columnLocation = "FIRST"
	} else {
		if lastColumn != "alterNoAfter" {
			columnLocation = fmt.Sprintf("AFTER `%s`", lastColumn)
		}

	}
	switch alterType {
	case "add":
		sqlS = fmt.Sprintf(" ADD COLUMN `%s` %s %s %s %s %s %s %s", curryColumn, columnDataType[0], charsetN, collationN, nullS, collumnDefaultN, commantS, columnLocation)
	case "modify":
		sqlS = fmt.Sprintf(" MODIFY COLUMN `%s` %s %s %s %s %s %s %s", curryColumn, columnDataType[0], charsetN, collationN, nullS, collumnDefaultN, commantS, columnLocation)
	case "drop":
		sqlS = fmt.Sprintf(" DROP COLUMN `%s` ", curryColumn)
	}
	return sqlS
}
func (my *MysqlDataAbnormalFixStruct) FixAlterColumnSqlGenerate(modifyColumn []string, logThreadSeq int64) []string {
	var alterSql []string
	if len(modifyColumn) > 0 {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE `%s`.`%s` %s;", my.Schema, my.Table, strings.Join(modifyColumn, ",")))
	}
	return alterSql
}
