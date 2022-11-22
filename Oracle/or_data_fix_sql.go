package oracle

import (
	"database/sql"
	"fmt"
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
}

func (or *OracleDataAbnormalFixStruct) FixInsertSqlExec(db *sql.DB, sourceDrive string) (string, error) {
	//查询该表的列名和列信息
	var insertSql string
	var valuesNameSeq []string
	colData := or.ColData
	//处理mysql查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
	for i := range or.ColData {
		var tmpcolumnName string
		if !strings.Contains(or.RowData, "/*go actions columnData*/") {
			insertSql = fmt.Sprintf("insert into \"%s\".\"%s\" values(%s) ", strings.ToUpper(or.Schema), or.Table, or.RowData)
		}
		tmprowSlic := strings.Split(or.RowData, "/*go actions columnData*/")
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
		insertSql = fmt.Sprintf("insert into \"%s\".\"%s\" values(%s)", strings.ToUpper(or.Schema), or.Table, strings.ReplaceAll(queryColumn, "'<nil>'", "NULL"))
	} else {
		insertSql = fmt.Sprintf("insert into \"%s\".\"%s\" values(%s)", strings.ToUpper(or.Schema), or.Table, queryColumn)
	}
	//if sourceDrive == "mysql" && strings.Contains(insertSql,"'',")
	return insertSql, nil
}

func (or *OracleDataAbnormalFixStruct) FixDeleteSqlExec(db *sql.DB, sourceDrive string) (string, error) {
	var deleteSql, deleteSqlWhere string
	var indexColName string
	var indexColSeq string
	colData := or.ColData
	var ad = make(map[string]int)
	for i := range colData {
		cls, _ := strconv.Atoi(fmt.Sprintf("%s", colData[i]["columnSeq"]))
		ad[fmt.Sprintf("%s", colData[i]["columnName"])] = cls
	}

	//判断索引列没有唯一性
	if strings.Contains(or.IndexColumnType, "mui") {
		var sqlwhereSlice []string
		if strings.Contains(or.RowData, "/*go actions columnData*/") {
			for k, v := range strings.Split(or.RowData, "/*go actions columnData*/") {
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
					sqlwhereSlice = append(sqlwhereSlice, fmt.Sprintf("  %s = '%s'", ki, or.RowData))
				}
			}
		}
		deleteSqlWhere = fmt.Sprintf(" %s ", strings.Join(sqlwhereSlice, " and "))
	}

	//索引列具有唯一性
	if !strings.Contains(or.IndexColumnType, "mui") {
		//单列索引处理方式
		if strings.Contains(or.Sqlwhere, " in (") {
			//获取索引列及列的序号
			aa := strings.ReplaceAll(or.Sqlwhere, "/* actions */ ", "")
			indexColName = strings.Split(strings.Split(aa, "in (")[0], "where ")[1]
			for _, i := range colData {
				if strings.ToUpper(i["columnName"]) == strings.ToUpper(strings.TrimSpace(indexColName)) {
					indexColSeq = i["columnSeq"]
				}
			}
			//处理单行有多列数据
			if strings.Contains(or.RowData, "/*go actions columnData*/") {
				for k, v := range strings.Split(or.RowData, "/*go actions columnData*/") {
					if indexColSeq == strconv.Itoa(k+1) {
						if v == "<nil>" {
							deleteSqlWhere = fmt.Sprintf(" %s is NULL ", indexColName)
						} else {
							deleteSqlWhere = fmt.Sprintf(" %s = '%s' ", indexColName, v)
						}
					}
				}
			}
			//处理单行有单列列数据
			if !strings.Contains(or.RowData, "/*go actions columnData*/") {
				for ki, _ := range ad {
					if or.RowData == "<nil>" {
						deleteSqlWhere = fmt.Sprintf(" %s is NULL ", or.RowData)
					} else {
						deleteSqlWhere = fmt.Sprintf(" %s = '%s' ", ki, or.RowData)
					}
				}
			}
		}
		//多列索引处理方式
		if strings.Contains(or.Sqlwhere, " or (") {
			aa := strings.ReplaceAll(or.Sqlwhere, "/* actions */ ", "")
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
			if strings.Contains(or.RowData, "/*go actions columnData*/") {
				for k, v := range strings.Split(or.RowData, "/*go actions columnData*/") {
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
				deleteSqlWhere = fmt.Sprintf(" %s ", strings.Join(sqlwhereSlice, " and "))
			}
			if !strings.Contains(or.RowData, "/*go actions columnData*/") {
				for ki, _ := range ad {
					if or.RowData == "<nil>" {
						deleteSqlWhere = fmt.Sprintf(" %s is NULL ", or.RowData)
					} else {
						deleteSqlWhere = fmt.Sprintf(" %s = '%s' ", ki, or.RowData)
					}
				}
			}
		}
		//只有一行数据
		if !strings.Contains(or.Sqlwhere, " or (") && !strings.Contains(or.Sqlwhere, " in (") {
			deleteSqlWhere = or.Sqlwhere
		}
	}
	deleteSql = fmt.Sprintf("delete from \"%s\".\"%s\" where %s ", strings.ToUpper(or.Schema), or.Table, deleteSqlWhere)
	return deleteSql, nil
}
