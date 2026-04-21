package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"strconv"
	"strings"
)

func (my *MysqlDataAbnormalFixStruct) FixInsertSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	//查询该表的列名和列信息
	var (
		insertSql     string
		valuesNameSeq []string
		targetSchema  = my.Schema // 默认使用目标schema
		vlog          string
	)

	vlog = fmt.Sprintf("(%d) Generating INSERT repair statement for %s.%s (target: %s)", logThreadSeq, my.Schema, my.Table, targetSchema)
	global.Wlog.Debug(vlog)

	// 检查ColData是否为空，如果为空，尝试从数据库中查询表的列信息
	if len(my.ColData) == 0 {
		vlog = fmt.Sprintf("(%d) Warning: No column data available for table %s.%s, trying to query from database",
			logThreadSeq, targetSchema, my.Table)
		global.Wlog.Warn(vlog)

		// 从INFORMATION_SCHEMA.COLUMNS中查询表的列信息
		query := "SELECT COLUMN_NAME AS columnName, ORDINAL_POSITION AS columnSeq, COLUMN_TYPE AS dataType FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
		rows, err := db.Query(query, targetSchema, my.Table)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Error: Failed to query column information from database: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			// 如果查询失败，回退到使用临时列名
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
		} else {
			// 解析查询结果
			var columns []map[string]string
			for rows.Next() {
				var columnName, columnSeqStr, dataType string
				if err := rows.Scan(&columnName, &columnSeqStr, &dataType); err != nil {
					vlog = fmt.Sprintf("(%d) Error: Failed to scan column information: %v", logThreadSeq, err)
					global.Wlog.Error(vlog)
					continue
				}
				columns = append(columns, map[string]string{
					"columnName": columnName,
					"columnSeq":  columnSeqStr,
					"dataType":   dataType,
				})
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				vlog = fmt.Sprintf("(%d) Error: Failed to iterate column information: %v", logThreadSeq, rowsErr)
				global.Wlog.Warn(vlog)
			}
			_ = rows.Close()

			if len(columns) > 0 {
				my.ColData = columns
				vlog = fmt.Sprintf("(%d) Successfully queried column information from database for table %s.%s, found %d columns",
					logThreadSeq, targetSchema, my.Table, len(columns))
				global.Wlog.Debug(vlog)
			} else {
				vlog = fmt.Sprintf("(%d) Warning: No column information found in database for table %s.%s, using temporary column names",
					logThreadSeq, targetSchema, my.Table)
				global.Wlog.Warn(vlog)

				// 如果查询结果为空，回退到使用临时列名
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
			}
		}
	}

	//Handle timezone issues with MySQL datetime columns (e.g. 2021-01-23 10:16:29 +0800 CST)
	rowParts := strings.Split(my.RowData, "/*go actions columnData*/")
	for k, v := range rowParts {
		dataType := ""
		if k < len(my.ColData) {
			dataType = my.ColData[k]["dataType"]
		} else {
			vlog = fmt.Sprintf("(%d) Warning: Column index %d exceeds available column data for %s.%s",
				logThreadSeq, k, targetSchema, my.Table)
			global.Wlog.Warn(vlog)
		}
		tmpcolumnName := formatMySQLInsertLiteral(v, dataType)
		valuesNameSeq = append(valuesNameSeq, tmpcolumnName)
	}

	if len(valuesNameSeq) > 0 {
		queryColumn := strings.Join(valuesNameSeq, ",")

		// 从ColData中提取所有列名，包括不可见列
		columnNames := make([]string, 0, len(my.ColData))
		for _, col := range my.ColData {
			if colName, ok := col["columnName"]; ok && colName != "" {
				columnNames = append(columnNames, fmt.Sprintf("`%s`", colName))
			}
		}

		// 如果有列名信息，则生成包含列名的INSERT语句
		if len(columnNames) > 0 {
			insertSql = fmt.Sprintf("INSERT INTO `%s`.`%s`(%s) VALUES(%s);", targetSchema, my.Table, strings.Join(columnNames, ","), queryColumn)
		} else {
			// 如果没有列名信息，回退到原始的VALUES语法
			insertSql = fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES(%s);", targetSchema, my.Table, queryColumn)
		}
	}

	return insertSql, nil
}
