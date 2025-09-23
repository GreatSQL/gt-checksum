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
	SourceSchema    string // 添加源端schema字段
}

/*
MySQL 生成insert修复语句
*/
func (my *MysqlDataAbnormalFixStruct) FixInsertSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	//查询该表的列名和列信息
	var (
		insertSql     string
		valuesNameSeq []string
		targetSchema  = my.Schema // 默认使用目标schema
	)

	vlog = fmt.Sprintf("(%d) Generating INSERT repair statement for %s.%s (target: %s)", logThreadSeq, my.Schema, my.Table, targetSchema)
	global.Wlog.Debug(vlog)

	// 检查ColData是否为空，如果为空，尝试从行数据中推断列信息
	if len(my.ColData) == 0 {
		vlog = fmt.Sprintf("(%d) Warning: No column data available for table %s.%s, attempting to infer from row data",
			logThreadSeq, targetSchema, my.Table)
		global.Wlog.Warn(vlog)

		// 从行数据中推断列数量
		rowParts := strings.Split(my.RowData, "/*go actions columnData*/")
		if len(rowParts) == 0 {
			return "", fmt.Errorf("no column data available and empty row data for table %s.%s (mapping: %s->%s)",
				targetSchema, my.Table, my.SourceSchema, my.Schema)
		}

		// 创建临时列数据结构
		tempColData := make([]map[string]string, len(rowParts))
		for i := range rowParts {
			tempColData[i] = map[string]string{
				"columnName": fmt.Sprintf("col_%d", i+1),
				"columnSeq":  strconv.Itoa(i + 1),
				"dataType":   "VARCHAR", // 默认类型
			}
		}
		my.ColData = tempColData

		vlog = fmt.Sprintf("(%d) Created temporary column structure with %d columns for table %s.%s",
			logThreadSeq, len(my.ColData), targetSchema, my.Table)
		global.Wlog.Debug(vlog)
	}

	//Handle timezone issues with MySQL datetime columns (e.g. 2021-01-23 10:16:29 +0800 CST)
	rowParts := strings.Split(my.RowData, "/*go actions columnData*/")
	for k, v := range rowParts {
		var tmpcolumnName string
		if strings.EqualFold(v, "<entry>") {
			tmpcolumnName = fmt.Sprintf("''")
		} else if strings.EqualFold(v, "<nil>") {
			tmpcolumnName = fmt.Sprintf("NULL")
		} else {
			// 检查索引是否越界
			if k < len(my.ColData) {
				if dataType, ok := my.ColData[k]["dataType"]; ok {
					if strings.ToUpper(dataType) == "DATETIME" {
						tmpcolumnName = fmt.Sprintf("date_format('%s','%%Y-%%m-%%d %%H:%%i:%%s')", v)
					} else if strings.Contains(strings.ToUpper(dataType), "TIMESTAMP") {
						tmpcolumnName = fmt.Sprintf("date_format('%s','%%Y-%%m-%%d %%H:%%i:%%s')", v)
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
					logThreadSeq, k, targetSchema, my.Table)
				global.Wlog.Warn(vlog)
			}
		}
		valuesNameSeq = append(valuesNameSeq, tmpcolumnName)
	}

	if len(valuesNameSeq) > 0 {
		queryColumn := strings.Join(valuesNameSeq, ",")
		insertSql = fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES(%s) ;", targetSchema, my.Table, queryColumn)
	}

	return insertSql, nil
}

/*
MySQL generate delete repair statement
*/
func (my *MysqlDataAbnormalFixStruct) FixDeleteSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	var (
		deleteSql, deleteSqlWhere string
		ad                        = make(map[string]int)
		acc                       = make(map[string]string) //判断特殊数据类型
	)
	var targetSchema = my.Schema // 默认使用目标schema

	// 确保ColData不为空
	if len(my.ColData) == 0 {
		vlog = fmt.Sprintf("(%d) Warning: No column data available for table %s.%s when generating DELETE statement",
			logThreadSeq, targetSchema, my.Table)
		global.Wlog.Warn(vlog)

		// 如果IndexColumn有值，尝试从中创建临时列数据
		if len(my.IndexColumn) > 0 {
			tempColData := make([]map[string]string, len(my.IndexColumn))
			for i, colName := range my.IndexColumn {
				tempColData[i] = map[string]string{
					"columnName": colName,
					"columnSeq":  strconv.Itoa(i + 1),
					"dataType":   "VARCHAR", // 默认类型
				}
			}
			my.ColData = tempColData
			vlog = fmt.Sprintf("(%d) Created temporary column structure from index columns for table %s.%s",
				logThreadSeq, targetSchema, my.Table)
			global.Wlog.Debug(vlog)
		} else if my.RowData != "" {
			// 从行数据中推断列数量
			rowParts := strings.Split(my.RowData, "/*go actions columnData*/")
			if len(rowParts) > 0 {
				tempColData := make([]map[string]string, len(rowParts))
				for i := range rowParts {
					tempColData[i] = map[string]string{
						"columnName": fmt.Sprintf("col_%d", i+1),
						"columnSeq":  strconv.Itoa(i + 1),
						"dataType":   "VARCHAR", // 默认类型
					}
				}
				my.ColData = tempColData
				vlog = fmt.Sprintf("(%d) Created temporary column structure with %d columns from row data for table %s.%s",
					logThreadSeq, len(my.ColData), targetSchema, my.Table)
				global.Wlog.Debug(vlog)
			}
		}

		// 如果仍然为空，返回错误
		if len(my.ColData) == 0 {
			return "", fmt.Errorf("no column data available for table %s.%s and cannot infer from available information",
				targetSchema, my.Table)
		}
	}

	colData := my.ColData
	for _, i := range colData {
		cls, _ := strconv.Atoi(fmt.Sprintf("%s", i["columnSeq"]))
		ad[i["columnName"]] = cls
		if strings.HasPrefix(i["dataType"], "double(") {
			acc["double"] = i["columnName"]
		}
	}
	vlog = fmt.Sprintf("(%d) Generating DELETE repair statement for %s.%s (target: %s)", logThreadSeq, my.Schema, my.Table, targetSchema)
	global.Wlog.Debug(vlog)

	if my.IndexType == "mul" {
		var FB, AS []string
		for _, i := range colData {
			if colName, ok := i["columnName"]; ok {
				FB = append(FB, colName)
			}
		}
		if len(FB) == 0 {
			// 确定正确的错误信息中应该使用的schema名称
			errorSchema := targetSchema
			if my.Schema != "" {
				// 如果是目标端操作，使用目标schema
				errorSchema = my.Schema
			}
			return "", fmt.Errorf("no valid columns found for table %s.%s (mapping: %s->%s)",
				errorSchema, my.Table, my.SourceSchema, my.Schema)
		}

		rowData := strings.ReplaceAll(my.RowData, "/*go actions columnData*/<nil>/*go actions columnData*/", "/*go actions columnData*/greatdbNull/*go actions columnData*/")
		rowParts := strings.Split(rowData, "/*go actions columnData*/")
		for k, v := range rowParts {
			if k >= len(FB) {
				continue
			}
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
		if len(AS) > 0 {
			deleteSqlWhere = strings.Join(AS, " AND ")
		}
	}

	vlog = fmt.Sprintf("(%d) Generating DELETE repair statement using unique index for %s.%s", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Debug(vlog)

	if my.IndexType == "pri" || my.IndexType == "uni" {
		// 添加对空IndexColumn的检查
		if len(my.IndexColumn) == 0 {
			return "", fmt.Errorf("no index columns defined for table %s.%s", targetSchema, my.Table)
		}

		var FB []string
		for _, i := range colData {
			colName, ok := i["columnName"]
			if !ok {
				continue
			}
			for _, v := range my.IndexColumn {
				if strings.EqualFold(v, colName) {
					if seq, ok := i["columnSeq"]; ok {
						FB = append(FB, seq)
					}
				}
			}
		}

		var AS []string
		rowParts := strings.Split(my.RowData, "/*go actions columnData*/")
		for k, v := range rowParts {
			if k >= len(FB) {
				continue
			}
			for l, I := range FB {
				if I == strconv.Itoa(k+1) && l < len(my.IndexColumn) {
					colName := my.IndexColumn[l]
					if v == "<nil>" {
						AS = append(AS, fmt.Sprintf(" %s IS NULL ", colName))
					} else if v == "<entry>" {
						AS = append(AS, fmt.Sprintf(" %s = '' ", colName))
					} else if v == acc["double"] {
						AS = append(AS, fmt.Sprintf("  CONCAT(%s,'') = '%s'", colName, v))
					} else {
						AS = append(AS, fmt.Sprintf(" %s = '%s' ", colName, v))
					}
				}
			}
			deleteSqlWhere = strings.Join(AS, " AND ")
		}
	}
	if len(deleteSqlWhere) > 0 {
		// 确保目标数据库存在
		if _, err := db.Exec(fmt.Sprintf("USE `%s`", targetSchema)); err != nil {
			return "", fmt.Errorf("target database %s does not exist", targetSchema)
		}
		deleteSql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;", targetSchema, my.Table, deleteSqlWhere)
	} else {
		return "", fmt.Errorf("failed to generate DELETE statement for table %s.%s: no valid conditions", targetSchema, my.Table)
	}
	return deleteSql, nil
}
func (my *MysqlDataAbnormalFixStruct) FixAlterIndexSqlExec(e, f []string, si map[string][]string, sourceDrive string, logThreadSeq int64) []string {
	var (
		sqlS         []string
		targetSchema = my.Schema // 默认使用目标schema
	)

	for _, v := range e {
		var c []string
		for _, vi := range si[v] {
			if len(strings.Split(vi, "/*actions Column Type*/")) > 0 {
				c = append(c, strings.TrimSpace(strings.Split(vi, "/*actions Column Type*/")[0]))
			}
		}
		switch my.IndexType {
		case "pri":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PRIMARY KEY(`%s`);", targetSchema, my.Table, strings.Join(c, "`,`"))
		case "uni":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD UNIQUE INDEX %s(`%s`);", targetSchema, my.Table, v, strings.Join(c, "`,`"))
		case "mul":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD INDEX %s(`%s`);", targetSchema, my.Table, v, strings.Join(c, "`,`"))
		}
		sqlS = append(sqlS, strsql)
	}
	for _, v := range f {
		switch my.IndexType {
		case "pri":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PRIMARY KEY;", targetSchema, my.Table)
		case "uni":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP INDEX %s;", targetSchema, my.Table, v)
		case "mul":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP INDEX %s;", targetSchema, my.Table, v)
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
	var (
		alterSql     []string
		targetSchema = my.Schema // 默认使用目标schema
	)

	if len(modifyColumn) > 0 {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE `%s`.`%s` %s;", targetSchema, my.Table, strings.Join(modifyColumn, ",")))
	}
	return alterSql
}
