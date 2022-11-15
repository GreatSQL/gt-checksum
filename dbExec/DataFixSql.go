package dbExec

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

type DataAbnormalFixStruct struct {
}
type DataAbnormalFixInterface interface {
	FixInsertSqlExec(db *sql.DB) (string, error)
	FixDeleteSqlExec(db *sql.DB) (string, error)
}

type MysqlDataAbnormalFixStruct struct {
	schema          string
	table           string
	rowData         string
	sourceDevice    string
	destDevice      string
	sqlwhere        string
	indexColumnType string
	colData         []map[string]string
}

type OracleDataAbnormalFixStruct struct {
	schema          string
	table           string
	rowData         string
	sourceDevice    string
	destDevice      string
	sqlwhere        string
	indexColumnType string
	colData         []map[string]string
}

/*
  MySQL 生成insert修复语句
*/
func (my *MysqlDataAbnormalFixStruct) FixInsertSqlExec(db *sql.DB) (string, error) {
	//查询该表的列名和列信息
	var insertSql string
	var valuesNameSeq []string
	colData := my.colData
	//处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
	for i := range my.colData {
		var tmpcolumnName string
		if !strings.Contains(my.rowData, "/*go actions columnData*/") {
			insertSql = fmt.Sprintf("insert into `%s`.`%s` values(%s);", my.schema, my.table, my.rowData)
		}
		tmprowSlic := strings.Split(my.rowData, "/*go actions columnData*/")
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
		insertSql = fmt.Sprintf("insert into `%s`.`%s` values(%s);", my.schema, my.table, strings.ReplaceAll(queryColumn, "'<nil>'", "NULL"))
	} else {
		insertSql = fmt.Sprintf("insert into `%s`.`%s` values(%s);", my.schema, my.table, queryColumn)
	}
	return insertSql, nil
}

func (or *OracleDataAbnormalFixStruct) FixInsertSqlExec(db *sql.DB) (string, error) {
	//查询该表的列名和列信息
	var insertSql string
	var valuesNameSeq []string
	colData := or.colData
	//处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
	for i := range or.colData {
		var tmpcolumnName string
		if !strings.Contains(or.rowData, "/*go actions columnData*/") {
			insertSql = fmt.Sprintf("insert into \"%s\".\"%s\" values(%s);", strings.ToUpper(or.schema), or.table, or.rowData)
		}
		tmprowSlic := strings.Split(or.rowData, "/*go actions columnData*/")
		tmpcolumnName = fmt.Sprintf("'%s'", tmprowSlic[i])
		if strings.ToUpper(colData[i]["dataType"]) == "DATE" {
			tmpColumnSeq, _ := strconv.Atoi(fmt.Sprintf("%v", colData[i]["columnSeq"]))
			tmprowSLIC := strings.ReplaceAll(tmprowSlic[tmpColumnSeq-1], "'", "")
			tmpcolumnName = fmt.Sprintf("TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')", tmprowSLIC)
		}
		if strings.Contains(strings.ToUpper(colData[i]["dataType"]), "TIMESTAMP") {
			tmpColumnSeq, _ := strconv.Atoi(fmt.Sprintf("%v", colData[i]["columnSeq"]))
			tmprowSLIC := strings.ReplaceAll(tmprowSlic[tmpColumnSeq-1], "'", "")
			tmpcolumnName = fmt.Sprintf("TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS')", tmprowSLIC)
		}
		valuesNameSeq = append(valuesNameSeq, tmpcolumnName)
	}
	queryColumn := strings.Join(valuesNameSeq, ",")
	if strings.Contains(queryColumn, "'<nil>'") {
		insertSql = fmt.Sprintf("insert into \"%s\".\"%s\" values(%s);", strings.ToUpper(or.schema), or.table, strings.ReplaceAll(queryColumn, "'<nil>'", "NULL"))
	} else {
		insertSql = fmt.Sprintf("insert into \"%s\".\"%s\" values(%s);", strings.ToUpper(or.schema), or.table, queryColumn)
	}
	return insertSql, nil
}

/*
  mysql 生成delete 修复语句
*/
func (my *MysqlDataAbnormalFixStruct) FixDeleteSqlExec(db *sql.DB) (string, error) {
	var deleteSql, deleteSqlWhere string
	var indexColName string
	var indexColSeq string
	colData := my.colData
	var ad = make(map[string]int)
	var acc = make(map[string]string) //判断特殊数据类型
	for _, i := range colData {
		cls, _ := strconv.Atoi(fmt.Sprintf("%s", i["columnSeq"]))
		ad[i["columnName"]] = cls
		if strings.HasPrefix(i["dataType"], "double(") {
			acc["double"] = i["columnName"]
		}
	}
	//判断索引列是否是有唯一性
	if strings.Contains(my.indexColumnType, "mui") { //判断索引列没有唯一性
		var sqlwhereSlice []string
		if strings.Contains(my.rowData, "/*go actions columnData*/") { //多行数据
			for k, v := range strings.Split(my.rowData, "/*go actions columnData*/") {
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
					sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf("  concat(%s,'') = '%s'", ki, my.rowData))
				} else {
					sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf("  %s = '%s'", ki, my.rowData))
				}
			}
		}
		deleteSqlWhere = fmt.Sprintf(" %s ;", strings.Join(sqlwhereSlice, " and "))

	} else { //索引列具有唯一性
		if strings.Contains(my.sqlwhere, " in (") {
			aa := strings.ReplaceAll(my.sqlwhere, "/* actions */ ", "")
			indexColName = strings.Split(strings.Split(aa, " in (")[0], "where ")[1]
			for i := range colData {
				if colData[i]["columnName"] == strings.TrimSpace(indexColName) {
					indexColSeq = colData[i]["columnSeq"]
				}
			}
			if strings.Contains(my.rowData, "/*go actions columnData*/") {
				for k, v := range strings.Split(my.rowData, "/*go actions columnData*/") {
					if indexColSeq == strconv.Itoa(k+1) {
						if v == "<nil>" {
							deleteSqlWhere = fmt.Sprintf(" %s is NULL ;", indexColName)
						} else if indexColName == acc["double"] {
							deleteSqlWhere = fmt.Sprintf("  concat(%s,'') = '%s'", indexColName, v)
						} else {
							deleteSqlWhere = fmt.Sprintf(" %s = '%s' ;", indexColName, v)
						}
					}
				}
			} else {
				for ki, _ := range ad {
					if my.rowData == "<nil>" {
						deleteSqlWhere = fmt.Sprintf(" %s is NULL ;", my.rowData)
					} else if indexColName == acc["double"] {
						deleteSqlWhere = fmt.Sprintf("  concat(%s,'') = '%s'", ki, my.rowData)
					} else {
						deleteSqlWhere = fmt.Sprintf(" %s = '%s' ;", ki, my.rowData)
					}
				}
			}
		} else if strings.Contains(my.sqlwhere, " or (") {
			aa := strings.ReplaceAll(my.sqlwhere, "/* actions */ ", "")
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
					if colData[i]["columnName"] == strings.TrimSpace(ac[v]) {
						indexColSeq = colData[i]["columnSeq"]
						add[ac[v]], _ = strconv.Atoi(indexColSeq)
					}
				}
			}
			var sqlwhereSlice []string
			if strings.Contains(my.rowData, "/*go actions columnData*/") {
				for k, v := range strings.Split(my.rowData, "/*go actions columnData*/") {
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
				deleteSqlWhere = fmt.Sprintf(" %s ;", strings.Join(sqlwhereSlice, " and "))
			} else {
				for ki, _ := range ad {
					if my.rowData == "<nil>" {
						deleteSqlWhere = fmt.Sprintf(" %s is NULL ;", my.rowData)
					} else if indexColName == acc["double"] {
						deleteSqlWhere = fmt.Sprintf("  concat(%s,'') = '%s'", ki, my.rowData)
					} else {
						deleteSqlWhere = fmt.Sprintf(" %s = '%s' ;", ki, my.rowData)
					}
				}
			}
		} else {
			deleteSqlWhere = my.sqlwhere
		}
	}
	deleteSql = fmt.Sprintf("delete from `%s`.`%s` where %s", my.schema, my.table, deleteSqlWhere)
	return deleteSql, nil
}

func (or *OracleDataAbnormalFixStruct) FixDeleteSqlExec(db *sql.DB) (string, error) {
	var deleteSql, deleteSqlWhere string
	var indexColName string
	var indexColSeq string
	colData := or.colData
	var ad = make(map[string]int)
	for i := range colData {
		cls, _ := strconv.Atoi(fmt.Sprintf("%s", colData[i]["columnSeq"]))
		ad[fmt.Sprintf("%s", colData[i]["columnName"])] = cls
	}
	//判断索引列是否是有唯一性
	if strings.Contains(or.indexColumnType, "mui") { //判断索引列没有唯一性
		var sqlwhereSlice []string
		if strings.Contains(or.rowData, "/*go actions columnData*/") {
			for k, v := range strings.Split(or.rowData, "/*go actions columnData*/") {
				for ki, vi := range ad {
					if vi == k+1 {
						if v == "<nil>" {
							sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf(" %s is NULL", ki))
						} else {
							sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf("  %s = '%s'", ki, v))
						}
					}
				}
			}
		} else {
			for ki, _ := range ad {
				if ki == "<nil>" {
					sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf(" %s is NULL", ki))
				} else {
					sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf("  %s = '%s'", ki, or.rowData))
				}
			}
		}
		deleteSqlWhere = fmt.Sprintf(" %s ;", strings.Join(sqlwhereSlice, " and "))
	} else { //索引列具有唯一性
		if strings.Contains(or.sqlwhere, " in (") {
			aa := strings.ReplaceAll(or.sqlwhere, "/* actions */ ", "")
			indexColName = strings.Split(strings.Split(aa, " in (")[0], "where ")[1]
			for i := range colData {
				if colData[i]["columnName"] == strings.TrimSpace(indexColName) {
					indexColSeq = colData[i]["columnSeq"]
				}
			}
			if strings.Contains(or.rowData, "/*go actions columnData*/") {
				for k, v := range strings.Split(or.rowData, "/*go actions columnData*/") {
					if indexColSeq == strconv.Itoa(k+1) {
						if v == "<nil>" {
							deleteSqlWhere = fmt.Sprintf(" %s is NULL ;", indexColName)
						} else {
							deleteSqlWhere = fmt.Sprintf(" %s = '%s' ;", indexColName, v)
						}
					}
				}
			} else {
				for ki, _ := range ad {
					if or.rowData == "<nil>" {
						deleteSqlWhere = fmt.Sprintf(" %s is NULL ;", or.rowData)
					} else {
						deleteSqlWhere = fmt.Sprintf(" %s = '%s' ;", ki, or.rowData)
					}
				}
			}
		} else if strings.Contains(or.sqlwhere, " or (") {
			aa := strings.ReplaceAll(or.sqlwhere, "/* actions */ ", "")
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
					if colData[i]["columnName"] == strings.TrimSpace(ac[v]) {
						indexColSeq = colData[i]["columnSeq"]
						add[ac[v]], _ = strconv.Atoi(indexColSeq)
					}
				}
			}
			var sqlwhereSlice []string
			if strings.Contains(or.rowData, "/*go actions columnData*/") {
				for k, v := range strings.Split(or.rowData, "/*go actions columnData*/") {
					for ki, vi := range add {
						if vi == k+1 {
							if v == "<nil>" {
								sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf(" %s is NULL", ki))
							} else {
								sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf("  %s = '%s'", ki, v))
							}
						}
					}
				}
				deleteSqlWhere = fmt.Sprintf(" %s ;", strings.Join(sqlwhereSlice, " and "))
			} else {
				for ki, _ := range ad {
					if or.rowData == "<nil>" {
						deleteSqlWhere = fmt.Sprintf(" %s is NULL ;", or.rowData)
					} else {
						deleteSqlWhere = fmt.Sprintf(" %s = '%s' ;", ki, or.rowData)
					}
				}
			}
		} else {
			deleteSqlWhere = or.sqlwhere
		}
	}
	deleteSql = fmt.Sprintf("delete from \"%s\".\"%s\" where %s", strings.ToUpper(or.schema), or.table, deleteSqlWhere)
	return deleteSql, nil
}

func (dafs DataAbnormalFixStruct) DataAbnormalFix(dname, tname, rowdata string, coldata []map[string]string, sqlwhere, dbDevice, indexCt string) DataAbnormalFixInterface {
	var tqaci DataAbnormalFixInterface
	if dbDevice == "mysql" {
		tqaci = &MysqlDataAbnormalFixStruct{
			schema:          dname,
			table:           tname,
			sqlwhere:        sqlwhere,
			rowData:         rowdata,
			sourceDevice:    dbDevice,
			indexColumnType: indexCt,
			colData:         coldata,
		}
	}
	if dbDevice == "godror" {
		tqaci = &OracleDataAbnormalFixStruct{
			schema:          dname,
			table:           tname,
			sqlwhere:        sqlwhere,
			rowData:         rowdata,
			sourceDevice:    dbDevice,
			indexColumnType: indexCt,
			colData:         coldata,
		}
	}
	return tqaci
}
func DataFix() *DataAbnormalFixStruct {
	return &DataAbnormalFixStruct{}
}
