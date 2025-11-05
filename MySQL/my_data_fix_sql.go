package mysql

import (
	"bufio"
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"os"
	"strconv"
	"strings"
)

// 跟踪已经在添加列时设置了主键的列
var (
	AutoIncrementColumnsWithPrimaryKey map[string]bool
)

type MysqlDataAbnormalFixStruct struct {
	Schema                  string
	Table                   string
	RowData                 string
	SourceDevice            string
	DestDevice              string
	Sqlwhere                string
	IndexColumnType         string
	ColData                 []map[string]string
	IndexType               string
	IndexColumn             []string
	DatafixType             string
	SourceSchema            string            // 添加源端schema字段
	CaseSensitiveObjectName string            // 是否区分对象名大小写
	IndexVisibilityMap      map[string]string // 索引可见性信息
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
		vlog          string
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
						tmpcolumnName = fmt.Sprintf("DATE_FORMAT('%s','%%Y-%%m-%%d %%H:%%i:%%s')", v)
					} else if strings.Contains(strings.ToUpper(dataType), "TIMESTAMP") {
						tmpcolumnName = fmt.Sprintf("DATE_FORMAT('%s','%%Y-%%m-%%d %%H:%%i:%%s')", v)
					} else {
						tmpcolumnName = fmt.Sprintf("'%v'", strings.TrimSpace(v))
					}
				} else {
					// 如果没有dataType字段，使用默认格式
					tmpcolumnName = fmt.Sprintf("'%v'", strings.TrimSpace(v))
				}
			} else {
				// 如果索引越界，使用默认格式
				tmpcolumnName = fmt.Sprintf("'%v'", strings.TrimSpace(v))
				vlog = fmt.Sprintf("(%d) Warning: Column index %d exceeds available column data for %s.%s",
					logThreadSeq, k, targetSchema, my.Table)
				global.Wlog.Warn(vlog)
			}
		}
		valuesNameSeq = append(valuesNameSeq, tmpcolumnName)
	}

	if len(valuesNameSeq) > 0 {
		queryColumn := strings.Join(valuesNameSeq, ",")
		insertSql = fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES(%s);", targetSchema, my.Table, queryColumn)
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
		vlog                      string
	)
	var targetSchema = my.Schema // 默认使用目标schema

	// 检查表是否有主键，如果有，强制使用主键作为条件
	hasPrimaryKey := false
	primaryKeyColumns := []string{}

	// 查询表的主键信息
	query := fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION", targetSchema, my.Table)
	rows, err := db.Query(query)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var columnName string
			if err := rows.Scan(&columnName); err == nil {
				hasPrimaryKey = true
				primaryKeyColumns = append(primaryKeyColumns, columnName)
			}
		}
	}

	// 如果表有主键，强制使用主键作为条件
	if hasPrimaryKey && len(primaryKeyColumns) > 0 {
		my.IndexType = "pri"
		my.IndexColumn = primaryKeyColumns
		vlog = fmt.Sprintf("(%d) Found primary key for table %s.%s: %v, forcing IndexType to 'pri'", logThreadSeq, targetSchema, my.Table, primaryKeyColumns)
		global.Wlog.Debug(vlog)
	} else {
		// 如果没有主键，检查是否有唯一键
		hasUniqueKey := false
		uniqueKeyColumns := []string{}

		// 查询表的唯一键信息
		uniqueQuery := fmt.Sprintf("SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND NON_UNIQUE = 0 AND INDEX_NAME != 'PRIMARY' ORDER BY INDEX_NAME, SEQ_IN_INDEX", targetSchema, my.Table)
		uniqueRows, uniqueErr := db.Query(uniqueQuery)
		if uniqueErr == nil {
			defer uniqueRows.Close()

			// 使用map来按索引名称分组列
			uniqueIndices := make(map[string][]string)

			for uniqueRows.Next() {
				var indexName, columnName string
				if uniqueErr := uniqueRows.Scan(&indexName, &columnName); uniqueErr == nil {
					uniqueIndices[indexName] = append(uniqueIndices[indexName], columnName)
				}
			}

			// 如果有唯一键，使用第一个唯一键
			for indexName, columns := range uniqueIndices {
				hasUniqueKey = true
				uniqueKeyColumns = columns
				vlog = fmt.Sprintf("(%d) Found unique key '%s' for table %s.%s: %v, forcing IndexType to 'uni'", logThreadSeq, indexName, targetSchema, my.Table, uniqueKeyColumns)
				global.Wlog.Debug(vlog)
				break // 只使用第一个唯一键
			}
		}

		// 如果表有唯一键，强制使用唯一键作为条件
		if hasUniqueKey && len(uniqueKeyColumns) > 0 {
			my.IndexType = "uni"
			my.IndexColumn = uniqueKeyColumns
		} else {
			// 如果既没有主键也没有唯一键，则设置为mul类型，并使用所有列作为条件
			my.IndexType = "mul"

			// 获取表的所有列名
			allColumnsQuery := fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION", targetSchema, my.Table)
			allColumnsRows, allColumnsErr := db.Query(allColumnsQuery)
			if allColumnsErr == nil {
				defer allColumnsRows.Close()

				allColumns := []string{}
				for allColumnsRows.Next() {
					var columnName string
					if err := allColumnsRows.Scan(&columnName); err == nil {
						allColumns = append(allColumns, columnName)
					}
				}

				if len(allColumns) > 0 {
					my.IndexColumn = allColumns
					vlog = fmt.Sprintf("(%d) No primary or unique key found for table %s.%s, using all columns as conditions: %v", logThreadSeq, targetSchema, my.Table, allColumns)
					global.Wlog.Debug(vlog)
				}
			}
		}
	}

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

		// 优先使用IndexColumn中的列（如果有的话）
		if len(my.IndexColumn) > 0 {
			FB = my.IndexColumn
			vlog = fmt.Sprintf("(%d) Using columns from IndexColumn for table %s.%s: %v", logThreadSeq, targetSchema, my.Table, FB)
			global.Wlog.Debug(vlog)
		} else {
			// 否则从colData中获取列名
			for _, i := range colData {
				if colName, ok := i["columnName"]; ok {
					FB = append(FB, colName)
				}
			}
			vlog = fmt.Sprintf("(%d) Using columns from colData for table %s.%s: %v", logThreadSeq, targetSchema, my.Table, FB)
			global.Wlog.Debug(vlog)
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

		// 创建一个映射，将列名映射到列序号和值
		columnMap := make(map[string]string)
		rowData := strings.ReplaceAll(my.RowData, "/*go actions columnData*/<nil>/*go actions columnData*/", "/*go actions columnData*/greatdbNull/*go actions columnData*/")
		rowParts := strings.Split(rowData, "/*go actions columnData*/")

		// 首先尝试使用colData中的列序号信息来映射值
		for _, col := range colData {
			colName, ok1 := col["columnName"]
			colSeqStr, ok2 := col["columnSeq"]
			if !ok1 || !ok2 {
				continue
			}

			colSeq, err := strconv.Atoi(colSeqStr)
			if err != nil || colSeq <= 0 || colSeq > len(rowParts) {
				continue
			}

			// 列序号是1-based，但数组索引是0-based
			columnMap[colName] = rowParts[colSeq-1]
		}

		// 如果没有足够的映射，尝试直接按顺序映射
		if len(columnMap) < len(FB) && len(rowParts) >= len(FB) {
			for i, colName := range FB {
				if _, exists := columnMap[colName]; !exists && i < len(rowParts) {
					columnMap[colName] = rowParts[i]
				}
			}
		}

		// 生成WHERE条件
		for _, colName := range FB {
			if value, ok := columnMap[colName]; ok {
				if value == "<nil>" {
					AS = append(AS, fmt.Sprintf("`%s` IS NULL", colName))
				} else if value == "<entry>" {
					AS = append(AS, fmt.Sprintf("`%s` = ''", colName))
				} else if value == acc["double"] {
					AS = append(AS, fmt.Sprintf("CONCAT(`%s`,'') = '%s'", colName, value))
				} else {
					AS = append(AS, fmt.Sprintf("`%s` = '%s'", colName, strings.TrimSpace(value)))
				}
			}
		}

		if len(AS) > 0 {
			deleteSqlWhere = strings.Join(AS, " AND ")
			vlog = fmt.Sprintf("(%d) Generated WHERE condition for table %s.%s: %s", logThreadSeq, targetSchema, my.Table, deleteSqlWhere)
			global.Wlog.Debug(vlog)
		} else {
			vlog = fmt.Sprintf("(%d) Failed to generate WHERE condition for table %s.%s: no valid column-value pairs", logThreadSeq, targetSchema, my.Table)
			global.Wlog.Warn(vlog)
		}
	}

	vlog = fmt.Sprintf("(%d) Generating DELETE repair statement using unique index for %s.%s", logThreadSeq, my.Schema, my.Table)
	global.Wlog.Debug(vlog)

	if my.IndexType == "pri" || my.IndexType == "uni" {
		// 添加对空IndexColumn的检查
		if len(my.IndexColumn) == 0 {
			return "", fmt.Errorf("no index columns defined for table %s.%s", targetSchema, my.Table)
		}

		// 创建一个映射，将列名映射到列序号和值
		columnMap := make(map[string]string)
		rowParts := strings.Split(my.RowData, "/*go actions columnData*/")

		for i, col := range colData {
			colName, ok := col["columnName"]
			if !ok || i >= len(rowParts) {
				continue
			}
			columnMap[colName] = rowParts[i]
		}

		// 只使用索引列（主键或唯一键）作为WHERE条件
		var AS []string
		for _, colName := range my.IndexColumn {
			if value, ok := columnMap[colName]; ok {
				if value == "<nil>" {
					AS = append(AS, fmt.Sprintf("`%s` IS NULL", colName))
				} else if value == "<entry>" {
					AS = append(AS, fmt.Sprintf("`%s` = ''", colName))
				} else if value == acc["double"] {
					AS = append(AS, fmt.Sprintf("CONCAT(`%s`,'') = '%s'", colName, value))
				} else {
					AS = append(AS, fmt.Sprintf("`%s` = '%s'", colName, strings.TrimSpace(value)))
				}
			}
		}

		if len(AS) > 0 {
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
		targetSchema = my.Schema // 使用目标schema（保持原始大小写）
	)

	for _, v := range e {
		var c []string
		for _, vi := range si[v] {
			// 从vi字符串中提取原始列名（格式：columnName/*seq*/indexSeq/*type*/columnType）
			parts := strings.Split(vi, "/*seq*/")
			if len(parts) > 0 {
				// 保留原始列名的大小写
				c = append(c, strings.TrimSpace(parts[0]))
			}
		}
		// 构建SQL语句，保持数据库名、表名和字段名的原始大小写
		var invisibleClause string
		if my.IndexVisibilityMap != nil {
			if visibility, exists := my.IndexVisibilityMap[v]; exists && visibility == "NO" {
				invisibleClause = " INVISIBLE"
			}
		}
		switch my.IndexType {
		case "pri":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PRIMARY KEY(`%s`);", targetSchema, my.Table, strings.Join(c, "`,`"))
		case "uni":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD UNIQUE INDEX %s(`%s`)%s;", targetSchema, my.Table, v, strings.Join(c, "`,`"), invisibleClause)
		case "mul":
			strsql = fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD INDEX %s(`%s`)%s;", targetSchema, my.Table, v, strings.Join(c, "`,`"), invisibleClause)
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
		collumnDefaultN = ""
	} else if columnDataType[4] == "NULL" {
		collumnDefaultN = ""
	} else if columnDataType[4] == "null" {
		// 如果列不允许为NULL（IS_NULLABLE=NO），则不应该设置DEFAULT NULL
		if strings.ToUpper(columnDataType[3]) != "NO" {
			collumnDefaultN = fmt.Sprintf("DEFAULT NULL")
		}
	} else {
		collumnDefaultN = fmt.Sprintf("DEFAULT '%s'", columnDataType[4])
	}
	commentS := ""
	if columnDataType[5] != "empty" {
		commentS = fmt.Sprintf("COMMENT '%s'", columnDataType[5])
	}
	columnLocation := ""
	if columnSeq == 0 {
		columnLocation = "FIRST"
	} else {
		if lastColumn != "alterNoAfter" {
			columnLocation = fmt.Sprintf("AFTER `%s`", lastColumn)
		}

	}

	// 初始化AutoIncrementColumnsWithPrimaryKey映射
	if AutoIncrementColumnsWithPrimaryKey == nil {
		AutoIncrementColumnsWithPrimaryKey = make(map[string]bool)
	}

	// 检查是否需要在添加列时同时设置为主键（对于自增列）
	addPrimaryKey := ""
	if alterType == "add" && strings.Contains(strings.ToLower(columnDataType[0]), "auto_increment") {
		// 对于自增列，需要同时设置为主键
		addPrimaryKey = " PRIMARY KEY"
		// 标记该列已经设置了主键，避免在索引修复时重复设置
		key := fmt.Sprintf("%s.%s.%s", my.Schema, my.Table, curryColumn)
		AutoIncrementColumnsWithPrimaryKey[key] = true
	}

	// 处理INVISIBLE属性（如果数据类型中包含）
	invisibleClause := ""
	if strings.Contains(strings.ToUpper(columnDataType[0]), "INVISIBLE") {
		// 从数据类型中提取INVISIBLE关键字
		invisibleClause = " INVISIBLE"
		// 从数据类型中移除INVISIBLE关键字，避免重复
		columnDataType[0] = strings.ReplaceAll(strings.ToUpper(columnDataType[0]), " INVISIBLE", "")
	}

	switch alterType {
	case "add":
		// 调整关键字顺序：INVISIBLE应在PRIMARY KEY之前，FIRST应在最后
		sqlS = fmt.Sprintf(" ADD COLUMN `%s` %s %s %s %s %s %s%s%s %s",
			curryColumn, columnDataType[0], charsetN, collationN, nullS, collumnDefaultN,
			commentS, invisibleClause, addPrimaryKey, columnLocation)
	case "modify":
		sqlS = fmt.Sprintf(" MODIFY COLUMN `%s` %s %s %s %s %s %s %s%s",
			curryColumn, columnDataType[0], charsetN, collationN, nullS, collumnDefaultN,
			commentS, columnLocation, invisibleClause)
	case "drop":
		sqlS = fmt.Sprintf(" DROP COLUMN `%s` ", curryColumn)
	case "change":
		// 对于CHANGE操作，需要原始列名和新列名
		// 假设curryColumn格式为"原始列名:新列名"
		parts := strings.Split(curryColumn, ":")
		if len(parts) == 2 {
			originalCol := parts[0]
			newCol := parts[1]
			sqlS = fmt.Sprintf(" CHANGE COLUMN `%s` `%s` %s %s %s %s %s %s %s%s",
				originalCol, newCol, columnDataType[0], charsetN, collationN, nullS,
				collumnDefaultN, commentS, columnLocation, invisibleClause)
		} else {
			// 如果格式不正确，降级为MODIFY
			sqlS = fmt.Sprintf(" MODIFY COLUMN `%s` %s %s %s %s %s %s %s%s",
				curryColumn, columnDataType[0], charsetN, collationN, nullS, collumnDefaultN,
				commentS, columnLocation, invisibleClause)
		}
	}
	return sqlS
}
func (my *MysqlDataAbnormalFixStruct) FixAlterColumnSqlGenerate(modifyColumn []string, logThreadSeq int64) []string {
	var (
		alterSql     []string
		targetSchema = my.Schema // 使用目标schema（保持原始大小写）
	)

	if len(modifyColumn) > 0 {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE `%s`.`%s` %s;", targetSchema, my.Table, strings.Join(modifyColumn, ",")))
	}
	return alterSql
}

// FixAlterColumnAndIndexSqlGenerate 合并列修复和索引修复操作，生成单个ALTER TABLE语句
func (my *MysqlDataAbnormalFixStruct) FixAlterColumnAndIndexSqlGenerate(columnOperations, indexOperations []string, logThreadSeq int64) []string {
	var (
		alterSql     []string
		targetSchema = my.Schema // 使用目标schema（保持原始大小写）
	)

	// 初始化AutoIncrementColumnsWithPrimaryKey映射
	if AutoIncrementColumnsWithPrimaryKey == nil {
		AutoIncrementColumnsWithPrimaryKey = make(map[string]bool)
	}

	// 过滤掉已经在列操作中设置为主键的列的索引操作
	filteredIndexOperations := make([]string, 0)
	for _, op := range indexOperations {
		// 检查是否是添加主键的操作
		if strings.Contains(strings.ToUpper(op), "ADD PRIMARY KEY") {
			// 尝试提取列名
			// 格式可能是：ALTER TABLE `schema`.`table` ADD PRIMARY KEY(`column`);
			startIdx := strings.Index(strings.ToUpper(op), "ADD PRIMARY KEY(")
			if startIdx != -1 {
				startIdx += len("ADD PRIMARY KEY(")
				endIdx := strings.Index(op[startIdx:], ")")
				if endIdx != -1 {
					colName := strings.TrimSpace(op[startIdx : startIdx+endIdx])
					// 去除反引号
					colName = strings.Trim(colName, "`")
					key := fmt.Sprintf("%s.%s.%s", my.Schema, my.Table, colName)
					// 如果该列已经在添加时设置为主键，则跳过此索引操作
					if _, exists := AutoIncrementColumnsWithPrimaryKey[key]; !exists {
						filteredIndexOperations = append(filteredIndexOperations, op)
					}
				} else {
					// 无法解析列名，保留原始操作
					filteredIndexOperations = append(filteredIndexOperations, op)
				}
			} else {
				// 不是标准格式，保留原始操作
				filteredIndexOperations = append(filteredIndexOperations, op)
			}
		} else {
			// 不是添加主键的操作，保留
			filteredIndexOperations = append(filteredIndexOperations, op)
		}
	}

	// 合并所有操作
	var allOperations []string
	allOperations = append(allOperations, columnOperations...)
	allOperations = append(allOperations, filteredIndexOperations...)

	if len(allOperations) > 0 {
		// 提取操作内容（去除ALTER TABLE前缀和分号）
		var operationContents []string
		for _, op := range allOperations {
			// 去除ALTER TABLE前缀
			op = strings.TrimSpace(op)
			if strings.HasPrefix(strings.ToUpper(op), "ALTER TABLE") {
				// 找到第一个空格后的内容
				parts := strings.SplitN(op, " ", 4)
				if len(parts) >= 4 {
					// 获取操作内容部分
					operationContent := strings.TrimSpace(parts[3])
					// 去除末尾的分号
					operationContent = strings.TrimSuffix(operationContent, ";")
					operationContents = append(operationContents, operationContent)
				}
			} else {
				// 如果不是ALTER TABLE语句，直接使用并去除分号
				op = strings.TrimSuffix(op, ";")
				operationContents = append(operationContents, op)
			}
		}

		if len(operationContents) > 0 {
			// 生成单个ALTER TABLE语句，包含所有操作
			alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE `%s`.`%s` %s;", targetSchema, my.Table, strings.Join(operationContents, ", ")))

			// 添加调试日志
			vlog := fmt.Sprintf("(%d) Generated combined ALTER TABLE SQL for %s.%s: %d column operations, %d index operations",
				logThreadSeq, targetSchema, my.Table, len(columnOperations), len(indexOperations))
			global.Wlog.Debug(vlog)
		}
	}

	return alterSql
}

// FixAlterIndexSqlGenerate 合并索引操作，生成单个ALTER TABLE语句
func (my *MysqlDataAbnormalFixStruct) FixAlterIndexSqlGenerate(indexOperations []string, logThreadSeq int64) []string {
	var (
		alterSql     []string
		targetSchema = my.Schema // 使用目标schema（保持原始大小写）
	)

	if len(indexOperations) > 0 {
		// 提取操作内容（去除ALTER TABLE前缀和分号）
		var operationContents []string
		for _, op := range indexOperations {
			// 去除ALTER TABLE前缀
			op = strings.TrimSpace(op)
			if strings.HasPrefix(strings.ToUpper(op), "ALTER TABLE") {
				// 找到第一个空格后的内容
				parts := strings.SplitN(op, " ", 4)
				if len(parts) >= 4 {
					// 获取操作内容部分
					operationContent := strings.TrimSpace(parts[3])
					// 去除末尾的分号
					operationContent = strings.TrimSuffix(operationContent, ";")
					operationContents = append(operationContents, operationContent)
				}
			} else {
				// 如果不是ALTER TABLE语句，直接使用并去除分号
				op = strings.TrimSuffix(op, ";")
				operationContents = append(operationContents, op)
			}
		}

		if len(operationContents) > 0 {
			// 生成单个ALTER TABLE语句，包含所有索引操作
			alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE `%s`.`%s` %s;", targetSchema, my.Table, strings.Join(operationContents, ", ")))

			// 添加调试日志
			vlog := fmt.Sprintf("(%d) Generated combined ALTER TABLE SQL for %s.%s: %d index operations",
				logThreadSeq, targetSchema, my.Table, len(indexOperations))
			global.Wlog.Debug(vlog)
		}
	}

	return alterSql
}

// FixTableCharsetSqlGenerate 生成表级别字符集转换的SQL语句
func (my *MysqlDataAbnormalFixStruct) FixTableCharsetSqlGenerate(charset, collation string, logThreadSeq int64) []string {
	var (
		alterSql     []string
		targetSchema = my.Schema // 默认使用目标schema
	)

	// 生成表级别字符集转换的SQL语句
	alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE `%s`.`%s` CONVERT TO CHARACTER SET %s COLLATE %s;",
		targetSchema, my.Table, charset, collation))

	// 添加日志，方便调试
	vlog := fmt.Sprintf("(%d) Generated table charset conversion SQL: %s", logThreadSeq, alterSql[0])
	global.Wlog.Debug(vlog)

	return alterSql
}

// WriteFixIfNeeded writes fix SQLs to file when datafix is "file"
func WriteFixIfNeeded(datafix, fixFileName string, sqls []string, logThreadSeq int64) error {
	if strings.EqualFold(datafix, "file") && len(sqls) > 0 && strings.TrimSpace(fixFileName) != "" {
		return writeFixSQLToFile(fixFileName, sqls, logThreadSeq)
	}
	return nil
}

// WriteFixIfNeededFile writes fix SQLs to an opened *os.File when datafix is "file"
func WriteFixIfNeededFile(datafix string, sfile *os.File, sqls []string, logThreadSeq int64) error {
	if !strings.EqualFold(datafix, "file") || sfile == nil || len(sqls) == 0 {
		return nil
	}

	// 过滤多余的ADD PRIMARY KEY语句
	filteredSqls := filterRedundantPrimaryKeyStatements(sqls)

	w := bufio.NewWriter(sfile)
	for _, s := range filteredSqls {
		ss := strings.TrimSpace(s)
		if ss == "" {
			continue
		}
		if !strings.HasSuffix(ss, ";") {
			ss += ";"
		}
		if _, err := w.WriteString(ss + "\n"); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return nil
}

// filterRedundantPrimaryKeyStatements 过滤多余的ADD PRIMARY KEY语句
// 当发现有ADD COLUMN语句已经设置了PRIMARY KEY时，移除后续的单独ADD PRIMARY KEY语句
func filterRedundantPrimaryKeyStatements(sqls []string) []string {
	// 存储表和列的映射关系，用于检测重复的主键定义
	// key: tableIdentifier (schema.table)
	// value: map of column names that are already set as primary keys
	primaryKeyTables := make(map[string]map[string]bool)
	// 存储需要保留的SQL语句
	var result []string

	// 第一遍扫描：识别并记录在ADD COLUMN语句中设置为PRIMARY KEY的列
	for _, sql := range sqls {
		sqlUpper := strings.ToUpper(sql)

		// 检查是否是ADD COLUMN语句且包含PRIMARY KEY
		if strings.Contains(sqlUpper, "ADD COLUMN") && strings.Contains(sqlUpper, "PRIMARY KEY") {
			// 提取表标识符 (schema.table)
			tableID := extractTableIdentifier(sql)
			if tableID == "" {
				continue
			}

			// 提取列名
			column := extractColumnNameFromAddColumn(sql)
			if column == "" {
				continue
			}

			// 初始化表的映射
			if _, exists := primaryKeyTables[tableID]; !exists {
				primaryKeyTables[tableID] = make(map[string]bool)
			}
			// 记录该列已经是主键
			primaryKeyTables[tableID][strings.ToUpper(column)] = true
		}
	}

	// 第二遍扫描：过滤多余的ADD PRIMARY KEY语句
	for _, sql := range sqls {
		sqlUpper := strings.ToUpper(sql)

		// 检查是否是单独的ADD PRIMARY KEY语句（不包含ADD COLUMN）
		if strings.Contains(sqlUpper, "ADD PRIMARY KEY") && !strings.Contains(sqlUpper, "ADD COLUMN") {
			// 提取表标识符
			tableID := extractTableIdentifier(sql)
			if tableID == "" {
				// 如果无法提取表信息，保留这条SQL
				result = append(result, sql)
				continue
			}

			// 提取列名
			column := extractColumnNameFromAddPrimaryKey(sql)
			if column == "" {
				// 如果无法提取列信息，保留这条SQL
				result = append(result, sql)
				continue
			}

			// 检查该列是否已经在ADD COLUMN语句中设置为主键
			if tableMap, exists := primaryKeyTables[tableID]; exists {
				if tableMap[strings.ToUpper(column)] {
					// 跳过这个多余的ADD PRIMARY KEY语句
					continue
				}
			}
		}

		// 保留这条SQL语句
		result = append(result, sql)
	}

	return result
}

// extractTableIdentifier 从SQL语句中提取表标识符 (schema.table)
func extractTableIdentifier(sql string) string {
	// 查找ALTER TABLE部分
	alterTablePos := strings.ToUpper(sql)
	startPos := strings.Index(alterTablePos, "ALTER TABLE")
	if startPos == -1 {
		return ""
	}

	// 跳过ALTER TABLE
	startPos += len("ALTER TABLE")
	rest := strings.TrimSpace(sql[startPos:])

	// 提取表标识符，考虑可能的反引号
	if strings.HasPrefix(rest, "`") {
		// 查找第一个反引号
		firstQuote := 0
		// 查找第一个结束反引号
		endQuote := strings.Index(rest[firstQuote+1:], "`")
		if endQuote == -1 {
			return ""
		}
		endQuote++ // 调整索引，因为我们从firstQuote+1开始查找

		// 检查是否有schema.table格式
		if endQuote+1 < len(rest) && rest[endQuote+1] == '.' {
			// 提取schema
			schema := rest[firstQuote+1 : endQuote]

			// 查找table的开始位置
			tableStart := endQuote + 2 // 跳过.和可能的空格
			if tableStart < len(rest) && rest[tableStart] == '`' {
				tableStart++ // 跳过开始反引号
				tableEnd := strings.Index(rest[tableStart:], "`")
				if tableEnd != -1 {
					table := rest[tableStart : tableStart+tableEnd]
					return fmt.Sprintf("%s.%s", schema, table)
				}
			}
		} else {
			// 只有表名没有schema
			table := rest[firstQuote+1 : endQuote]
			return table
		}
	}

	// 如果没有反引号，尝试查找空格分割的表名
	parts := strings.Fields(rest)
	if len(parts) > 0 {
		return parts[0]
	}

	return ""
}

// extractColumnNameFromAddColumn 从ADD COLUMN语句中提取列名
func extractColumnNameFromAddColumn(sql string) string {
	// 查找ADD COLUMN部分
	addColumnPos := strings.ToUpper(sql)
	startPos := strings.Index(addColumnPos, "ADD COLUMN")
	if startPos == -1 {
		return ""
	}

	// 跳过ADD COLUMN
	startPos += len("ADD COLUMN")
	rest := strings.TrimSpace(sql[startPos:])

	// 提取列名，考虑可能的反引号
	if strings.HasPrefix(rest, "`") {
		// 查找第一个反引号
		firstQuote := 0
		// 查找第一个结束反引号
		endQuote := strings.Index(rest[firstQuote+1:], "`")
		if endQuote != -1 {
			return rest[firstQuote+1 : firstQuote+1+endQuote]
		}
	}

	// 如果没有反引号，尝试查找空格分割的列名
	parts := strings.Fields(rest)
	if len(parts) > 0 {
		// 可能包含类型信息，提取第一个部分
		return parts[0]
	}

	return ""
}

// extractColumnNameFromAddPrimaryKey 从ADD PRIMARY KEY语句中提取列名
func extractColumnNameFromAddPrimaryKey(sql string) string {
	// 查找ADD PRIMARY KEY部分
	addPKPos := strings.ToUpper(sql)
	startPos := strings.Index(addPKPos, "ADD PRIMARY KEY(")
	if startPos == -1 {
		return ""
	}

	// 跳过ADD PRIMARY KEY(
	startPos += len("ADD PRIMARY KEY(")
	rest := sql[startPos:]

	// 查找结束括号
	endPos := strings.Index(rest, ")")
	if endPos == -1 {
		return ""
	}

	// 提取括号内的内容（列名）
	columnPart := strings.TrimSpace(rest[:endPos])

	// 去除可能的反引号
	return strings.Trim(columnPart, "`")
}

// writeFixSQLToFile appends SQL statements into the specified file
func writeFixSQLToFile(path string, sqls []string, logThreadSeq int64) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		vlog := fmt.Sprintf("(%d) Failed to open fix SQL file %s: %v", logThreadSeq, path, err)
		global.Wlog.Error(vlog)
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	wrote := 0
	for _, s := range sqls {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		// ensure semicolon termination
		if !strings.HasSuffix(s, ";") {
			s = s + ";"
		}
		if _, err := w.WriteString(s + "\n"); err != nil {
			vlog := fmt.Sprintf("(%d) Failed to write fix SQL to %s: %v", logThreadSeq, path, err)
			global.Wlog.Error(vlog)
			return err
		}
		wrote++
	}
	if err := w.Flush(); err != nil {
		vlog := fmt.Sprintf("(%d) Failed to flush fix SQL to %s: %v", logThreadSeq, path, err)
		global.Wlog.Error(vlog)
		return err
	}

	vlog := fmt.Sprintf("(%d) Appended %d fix SQL statements to %s", logThreadSeq, wrote, path)
	global.Wlog.Debug(vlog)
	return nil
}

// GenerateRoutineFixSQL builds DROP + CREATE statements for procedure/function
// routineType should be "PROCEDURE" or "FUNCTION"
func GenerateRoutineFixSQL(schema, name, routineType, sourceDef string) []string {
	drop := fmt.Sprintf("DROP %s IF EXISTS `%s`.`%s`;", strings.ToUpper(routineType), schema, name)
	// sourceDef is expected to be a full CREATE PROCEDURE/FUNCTION definition from source
	return []string{drop, strings.TrimSpace(sourceDef)}
}

// GenerateTriggerFixSQL builds DROP + CREATE statements for trigger
func GenerateTriggerFixSQL(schema, name, sourceDef string) []string {
	drop := fmt.Sprintf("DROP TRIGGER IF EXISTS `%s`.`%s`;", schema, name)
	// sourceDef is expected to be a full CREATE TRIGGER definition from source
	return []string{drop, strings.TrimSpace(sourceDef)}
}
