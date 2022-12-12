package mysql

import (
	"database/sql"
	"fmt"
	"greatdbCheck/global"
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
}

/*
  MySQL 生成insert修复语句
*/
func (my *MysqlDataAbnormalFixStruct) FixInsertSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	//查询该表的列名和列信息
	var insertSql string
	var valuesNameSeq []string
	colData := my.ColData
	//处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
	for i := range my.ColData {
		var tmpcolumnName string
		if !strings.Contains(my.RowData, "/*go actions columnData*/") {
			insertSql = fmt.Sprintf("insert into `%s`.`%s` values(%s) ", my.Schema, my.Table, my.RowData)
		}
		tmprowSlic := strings.Split(my.RowData, "/*go actions columnData*/")
		tmpcolumnName = fmt.Sprintf("'%s'", tmprowSlic[i])
		if strings.ToUpper(colData[i]["dataType"]) == "DATETIME" {
			tmpColumnSeq, _ := strconv.Atoi(fmt.Sprintf("%v", colData[i]["columnSeq"]))
			tmprowSLIC := strings.ReplaceAll(tmprowSlic[tmpColumnSeq-1], "'", "")
			tmpcolumnName = fmt.Sprintf("date_format('%s','%%Y-%%m-%%d %%H:%%i:%%s')", tmprowSLIC)
		}
		if strings.Contains(strings.ToUpper(colData[i]["dataType"]), "TIMESTAMP") {
			tmpColumnSeq, _ := strconv.Atoi(fmt.Sprintf("%v", colData[i]["columnSeq"]))
			tmprowSLIC := strings.ReplaceAll(tmprowSlic[tmpColumnSeq-1], "'", "")
			tmpcolumnName = fmt.Sprintf("date_format('%s','%%Y-%%m-%%d %%H:%%i:%%s')", tmprowSLIC)
		}
		valuesNameSeq = append(valuesNameSeq, tmpcolumnName)
	}
	queryColumn := strings.Join(valuesNameSeq, ",")
	if strings.Contains(queryColumn, "'<nil>'") {
		insertSql = fmt.Sprintf("insert into `%s`.`%s` values(%s) ", my.Schema, my.Table, strings.ReplaceAll(queryColumn, "'<nil>'", "NULL"))
	} else {
		insertSql = fmt.Sprintf("insert into `%s`.`%s` values(%s) ", my.Schema, my.Table, queryColumn)
	}
	if sourceDrive == "godror" && strings.Contains(insertSql, ",'',") {
		insertSql = strings.ReplaceAll(insertSql, ",'',", ",null,")
	}
	return insertSql, nil
}

/*
  mysql 生成delete 修复语句
*/
func (my *MysqlDataAbnormalFixStruct) FixDeleteSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	var deleteSql, deleteSqlWhere string
	var indexColName string
	var indexColSeq string
	colData := my.ColData
	var ad = make(map[string]int)
	var acc = make(map[string]string) //判断特殊数据类型
	for _, i := range colData {
		cls, _ := strconv.Atoi(fmt.Sprintf("%s", i["columnSeq"]))
		ad[i["columnName"]] = cls
		if strings.HasPrefix(i["dataType"], "double(") {
			acc["double"] = i["columnName"]
		}
	}
	alog := fmt.Sprintf("(%d)  MySQL DB check table %s.%s starts to generate delete repair statement.", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Info(alog)
	//判断索引列是否是有唯一性
	if strings.Contains(my.IndexColumnType, "mui") { //判断索引列没有唯一性
		blog := fmt.Sprintf("(%d) MySQL DB check table %s.%s Generate delete repair statement based on common index.", logThreadSeq, my.Schema, my.Table)
		global.Wlog.Info(blog)
		var sqlwhereSlice []string
		if strings.Contains(my.RowData, "/*go actions columnData*/") { //多行数据
			for k, v := range strings.Split(my.RowData, "/*go actions columnData*/") {
				for ki, vi := range ad {
					if vi == k+1 {
						if v == "<nil>" {
							sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf(" %s is NULL", ki))
						} else if ki == acc["double"] {
							sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf("  concat(%s,'') = '%s'", ki, v))
						} else {
							sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf("  %s = '%s'", ki, v))
						}
					}
				}
			}
		} else { //单行数据
			for ki, _ := range ad {
				if ki == "<nil>" {
					sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf(" %s is NULL", ki))
				} else if ki == acc["double"] {
					sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf("  concat(%s,'') = '%s'", ki, my.RowData))
				} else {
					sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf("  %s = '%s'", ki, my.RowData))
				}
			}
		}
		deleteSqlWhere = fmt.Sprintf(" %s ", strings.Join(sqlwhereSlice, " and "))
	}
	if !strings.Contains(my.IndexColumnType, "mui") { //索引列具有唯一性
		clog := fmt.Sprintf("(%d) MySQL DB check table %s.%s Generate delete repair statement based on unique index.", logThreadSeq, my.Schema, my.Table)
		global.Wlog.Info(clog)
		if strings.Contains(my.Sqlwhere, " in (") {
			aa := strings.ReplaceAll(my.Sqlwhere, "/* actions */ ", "")
			indexColName = strings.Split(strings.Split(aa, " in (")[0], "where ")[1]
			for i := range colData {
				if strings.ToUpper(strings.TrimSpace(colData[i]["columnName"])) == strings.ToUpper(strings.TrimSpace(indexColName)) {
					indexColSeq = colData[i]["columnSeq"]
				}
			}
			if strings.Contains(my.RowData, "/*go actions columnData*/") {
				for k, v := range strings.Split(my.RowData, "/*go actions columnData*/") {
					if indexColSeq == strconv.Itoa(k+1) {
						if v == "<nil>" {
							deleteSqlWhere = fmt.Sprintf(" %s is NULL ", indexColName)
						} else if indexColName == acc["double"] {
							deleteSqlWhere = fmt.Sprintf("  concat(%s,'') = '%s'", indexColName, v)
						} else {
							deleteSqlWhere = fmt.Sprintf(" %s = '%s' ", indexColName, v)
						}
					}
				}
			}
			//单列数据
			if !strings.Contains(my.RowData, "/*go actions columnData*/") {
				for ki, _ := range ad {
					if my.RowData == "<nil>" {
						deleteSqlWhere = fmt.Sprintf(" %s is NULL ", my.RowData)
					} else if indexColName == acc["double"] {
						deleteSqlWhere = fmt.Sprintf("  concat(%s,'') = '%s'", ki, my.RowData)
					} else {
						deleteSqlWhere = fmt.Sprintf(" %s = '%s' ", ki, my.RowData)
					}
				}
			}
		} else if strings.Contains(my.Sqlwhere, " or (") {
			aa := strings.ReplaceAll(my.Sqlwhere, "/* actions */ ", "")
			indexColName = strings.TrimSpace(strings.ReplaceAll(strings.Split(aa, " or (")[1], ")", ""))
			var ac []string
			var add = make(map[string]int)
			if strings.Contains(indexColName, "and") {
				ab := strings.Split(indexColName, "and")
				for i := range ab {
					if !strings.Contains(ab[i], "=") {
						continue
					}
					ac = append(ac, strings.TrimSpace(strings.Split(ab[i], "=")[0]))
				}
			}
			for i := range colData {
				for v := range ac {
					if strings.ToUpper(strings.TrimSpace(colData[i]["columnName"])) == strings.ToUpper(strings.TrimSpace(ac[v])) {
						indexColSeq = colData[i]["columnSeq"]
						add[ac[v]], _ = strconv.Atoi(indexColSeq)
					}
				}
			}
			var sqlwhereSlice []string
			if strings.Contains(my.RowData, "/*go actions columnData*/") {
				for k, v := range strings.Split(my.RowData, "/*go actions columnData*/") {
					for ki, vi := range add {
						if vi == k+1 {
							if v == "<nil>" {
								sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf(" %s is NULL", ki))
							} else if indexColName == acc["double"] {
								sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf("  concat(%s,'') = '%s'", ki, v))
							} else {
								sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf("  %s = '%s'", ki, v))
							}
						}
					}
				}
				deleteSqlWhere = fmt.Sprintf(" %s ", strings.Join(sqlwhereSlice, " and "))
			} else {
				for ki, _ := range ad {
					if my.RowData == "<nil>" {
						deleteSqlWhere = fmt.Sprintf(" %s is NULL ", my.RowData)
					} else if indexColName == acc["double"] {
						deleteSqlWhere = fmt.Sprintf("  concat(%s,'') = '%s'", ki, my.RowData)
					} else {
						deleteSqlWhere = fmt.Sprintf(" %s = '%s' ", ki, my.RowData)
					}
				}
			}
		} else {
			deleteSqlWhere = my.Sqlwhere
		}
	}
	deleteSql = fmt.Sprintf("delete from `%s`.`%s` where %s;", my.Schema, my.Table, deleteSqlWhere)
	return deleteSql, nil
}
