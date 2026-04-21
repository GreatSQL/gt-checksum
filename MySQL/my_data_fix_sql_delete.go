package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"strconv"
	"strings"
)

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
	tableKey := fmt.Sprintf("%s.%s", targetSchema, my.Table)

	// 先检查缓存（使用读锁）
	tablePrimaryKeyMutex.RLock()
	if columns, exists := TablePrimaryKeyColumns[tableKey]; exists {
		tablePrimaryKeyMutex.RUnlock()
		hasPrimaryKey = len(columns) > 0
		primaryKeyColumns = columns
	} else {
		tablePrimaryKeyMutex.RUnlock()
		// 查询表的主键信息
		query := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION"
		rows, err := db.Query(query, targetSchema, my.Table)
		if err == nil {
			for rows.Next() {
				var columnName string
				if err := rows.Scan(&columnName); err == nil {
					hasPrimaryKey = true
					primaryKeyColumns = append(primaryKeyColumns, columnName)
				}
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				vlog = fmt.Sprintf("(%d) Failed to iterate primary key rows for %s.%s: %v", logThreadSeq, targetSchema, my.Table, rowsErr)
				global.Wlog.Warn(vlog)
			}
			_ = rows.Close()
		}
		// 缓存结果（使用写锁）
		tablePrimaryKeyMutex.Lock()
		TablePrimaryKeyColumns[tableKey] = primaryKeyColumns
		tablePrimaryKeyMutex.Unlock()
	}

	// 过滤掉源端数据中不存在的主键列，避免 MySQL 8.0 自动生成的隐藏主键
	// my_row_id（或其它目标端独有列）导致 WHERE 条件无法构造。
	// 当过滤后 PK 为空时，回退到 uni/mul 分支以便用其它列或全部列构造条件。
	if hasPrimaryKey && len(primaryKeyColumns) > 0 && len(my.ColData) > 0 {
		filtered, dropped := filterPKColumnsAgainstSource(primaryKeyColumns, my.ColData)
		if len(dropped) > 0 {
			vlog = fmt.Sprintf("(%d) Dropping primary key columns %v absent from source row data for %s.%s (likely MySQL-generated invisible PK), falling back to other conditions",
				logThreadSeq, dropped, targetSchema, my.Table)
			global.Wlog.Debug(vlog)
		}
		primaryKeyColumns = filtered
		hasPrimaryKey = len(filtered) > 0
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
		uniqueQuery := "SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0 AND INDEX_NAME != 'PRIMARY' ORDER BY INDEX_NAME, SEQ_IN_INDEX"
		uniqueRows, uniqueErr := db.Query(uniqueQuery, targetSchema, my.Table)
		if uniqueErr == nil {
			// 使用map来按索引名称分组列
			uniqueIndices := make(map[string][]string)

			for uniqueRows.Next() {
				var indexName, columnName string
				if uniqueErr := uniqueRows.Scan(&indexName, &columnName); uniqueErr == nil {
					uniqueIndices[indexName] = append(uniqueIndices[indexName], columnName)
				}
			}
			if rowsErr := uniqueRows.Err(); rowsErr != nil {
				vlog = fmt.Sprintf("(%d) Failed to iterate unique index rows for %s.%s: %v", logThreadSeq, targetSchema, my.Table, rowsErr)
				global.Wlog.Warn(vlog)
			}

			// 如果有唯一键，使用第一个唯一键
			for indexName, columns := range uniqueIndices {
				hasUniqueKey = true
				uniqueKeyColumns = columns
				vlog = fmt.Sprintf("(%d) Found unique key '%s' for table %s.%s: %v, forcing IndexType to 'uni'", logThreadSeq, indexName, targetSchema, my.Table, uniqueKeyColumns)
				global.Wlog.Debug(vlog)
				break // 只使用第一个唯一键
			}
			_ = uniqueRows.Close()
		}

		// 如果表有唯一键，强制使用唯一键作为条件
		if hasUniqueKey && len(uniqueKeyColumns) > 0 {
			my.IndexType = "uni"
			my.IndexColumn = uniqueKeyColumns
		} else {
			// 如果既没有主键也没有唯一键，则设置为mul类型，并使用所有列作为条件
			my.IndexType = "mul"

			// 获取表的所有列名
			allColumnsQuery := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
			allColumnsRows, allColumnsErr := db.Query(allColumnsQuery, targetSchema, my.Table)
			if allColumnsErr == nil {
				allColumns := []string{}
				for allColumnsRows.Next() {
					var columnName string
					if err := allColumnsRows.Scan(&columnName); err == nil {
						allColumns = append(allColumns, columnName)
					}
				}
				if rowsErr := allColumnsRows.Err(); rowsErr != nil {
					vlog = fmt.Sprintf("(%d) Failed to iterate all columns for %s.%s: %v", logThreadSeq, targetSchema, my.Table, rowsErr)
					global.Wlog.Warn(vlog)
				}

				if len(allColumns) > 0 {
					my.IndexColumn = allColumns
					vlog = fmt.Sprintf("(%d) No primary or unique key found for table %s.%s, using all columns as conditions: %v", logThreadSeq, targetSchema, my.Table, allColumns)
					global.Wlog.Debug(vlog)
				}
				_ = allColumnsRows.Close()
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
		// 不进行额外的字符串替换处理，直接分割原始行数据
		rowParts := strings.Split(my.RowData, "/*go actions columnData*/")

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
			// 对于DATA列，直接使用原始值，不做任何处理
			columnMap[colName] = rowParts[colSeq-1]
		}

		// 如果没有足够的映射，尝试直接按顺序映射
		if len(columnMap) < len(FB) && len(rowParts) >= len(FB) {
			for i, colName := range FB {
				if _, exists := columnMap[colName]; !exists && i < len(rowParts) {
					// 直接使用原始值，不做任何处理
					columnMap[colName] = rowParts[i]
				}
			}
		}

		// 生成WHERE条件
		for _, colName := range FB {
			if value, ok := columnMap[colName]; ok {
				dataType := lookupColumnDataType(colData, colName)
				if value == "<nil>" {
					AS = append(AS, fmt.Sprintf("`%s` IS NULL", colName))
				} else if value == "<entry>" {
					AS = append(AS, fmt.Sprintf("`%s` = ''", colName))
				} else if predicate, ok := buildIntegerDeletePredicate(colName, value, dataType); ok {
					AS = append(AS, predicate)
				} else if predicate, ok := buildFloatDeletePredicate(colName, value, dataType); ok {
					AS = append(AS, predicate)
				} else if value == acc["double"] {
					AS = append(AS, fmt.Sprintf("CONCAT(`%s`,'') = '%s'", colName, value))
				} else {
					// 确保DELETE语句使用目标端的实际数据格式
					// 对于WHERE条件，使用目标端数据的原始格式，包括尾部空格
					// 这是因为我们需要精确匹配目标端的数据，删除正确的行
					// 生成WHERE条件时，使用目标端的原始数据格式，包括尾部空格
					AS = append(AS, fmt.Sprintf("`%s` = '%s'", colName, escapeSQLString(value)))
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
				dataType := lookupColumnDataType(colData, colName)
				if value == "<nil>" {
					AS = append(AS, fmt.Sprintf("`%s` IS NULL", colName))
				} else if value == "<entry>" {
					AS = append(AS, fmt.Sprintf("`%s` = ''", colName))
				} else if predicate, ok := buildIntegerDeletePredicate(colName, value, dataType); ok {
					AS = append(AS, predicate)
				} else if predicate, ok := buildFloatDeletePredicate(colName, value, dataType); ok {
					AS = append(AS, predicate)
				} else if value == acc["double"] {
					AS = append(AS, fmt.Sprintf("CONCAT(`%s`,'') = '%s'", colName, value))
				} else {
					// 确保DELETE语句使用目标端的实际数据格式
					// 对于WHERE条件，使用目标端数据的原始格式，包括尾部空格
					// 这是因为我们需要精确匹配目标端的数据，删除正确的行
					// 生成WHERE条件时，使用目标端的原始数据格式，包括尾部空格
					AS = append(AS, fmt.Sprintf("`%s` = '%s'", colName, escapeSQLString(value)))
				}
			}
		}

		if len(AS) > 0 {
			deleteSqlWhere = strings.Join(AS, " AND ")
		}
	}
	if len(deleteSqlWhere) > 0 {
		// 生成数据库连接的唯一标识符
		dbPointer := fmt.Sprintf("%p", db)

		// 检查缓存，避免重复执行USE语句（使用读锁）
		databaseCacheMutex.RLock()
		currentDB, exists := CurrentDatabaseCache[dbPointer]
		databaseCacheMutex.RUnlock()

		if !exists || currentDB != targetSchema {
			// 确保目标数据库存在
			if _, err := db.Exec(fmt.Sprintf("USE `%s`", targetSchema)); err != nil {
				return "", fmt.Errorf("target database %s does not exist", targetSchema)
			}
			// 更新缓存（使用写锁）
			databaseCacheMutex.Lock()
			CurrentDatabaseCache[dbPointer] = targetSchema
			databaseCacheMutex.Unlock()
		}

		// 统计目标端中与当前条件匹配的记录数量，以确定合适的LIMIT值
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` WHERE %s", targetSchema, my.Table, deleteSqlWhere)
		var matchCount int
		if err := db.QueryRow(countQuery).Scan(&matchCount); err != nil {
			// 如果统计失败，默认使用LIMIT 1
			vlog = fmt.Sprintf("(%d) Failed to count matching records: %v, using LIMIT 1", logThreadSeq, err)
			global.Wlog.Warn(vlog)
			matchCount = 1
		}

		// 根据实际匹配数量设置LIMIT值
		limit := 1
		// 对于唯一索引中的NULL值，不应该删除所有匹配记录，只删除多余的部分
		// 在MySQL中，唯一索引允许多个NULL值，因为NULL不等于NULL
		// 但是对于没有唯一索引的表，即使有NULL值，也应该删除所有匹配的记录
		if matchCount > 1 && (my.IndexType != "pri" && my.IndexType != "uni" || !strings.Contains(deleteSqlWhere, "IS NULL")) {
			limit = matchCount
		}

		// 判断是否是主键、唯一键或隐藏主键 my_row_id
		isUniqueKey := my.IndexType == "pri" || my.IndexType == "uni" || (len(my.IndexColumn) == 1 && my.IndexColumn[0] == "my_row_id")

		// 修复：对于唯一索引中的 NULL 值，因为唯一索引允许多个 NULL 值，
		// 所以即使是唯一字段，在删除包含 NULL 的记录时也必须加上 LIMIT N 约束，
		// 以免错误地删除目标端所有该字段为 NULL 的其他记录
		if isUniqueKey && !strings.Contains(deleteSqlWhere, "IS NULL") {
			deleteSql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;", targetSchema, my.Table, deleteSqlWhere)
		} else {
			deleteSql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s LIMIT %d;", targetSchema, my.Table, deleteSqlWhere, limit)
		}
	} else {
		return "", fmt.Errorf("failed to generate DELETE statement for table %s.%s: no valid conditions", targetSchema, my.Table)
	}
	return deleteSql, nil
}

// 从外键DDL定义中提取引用表和字段信息
func extractForeignKeyInfo(ddlDefinition, fkName string) (string, string) {
	// 如果没有提供DDL定义，则返回空
	if ddlDefinition == "" {
		return "", ""
	}

	// 查找REFERENCES关键字
	lowerDDL := strings.ToLower(ddlDefinition)
	refIndex := strings.Index(lowerDDL, "references")
	if refIndex == -1 {
		return "", ""
	}

	// 提取REFERENCES之后的内容
	afterRef := strings.TrimSpace(ddlDefinition[refIndex+len("references"):])

	// 提取引用表名（可能包含schema前缀）
	var refTable, refColumn string
	parts := strings.Split(afterRef, "(")
	if len(parts) >= 2 {
		// 提取引用表名，去掉可能的反引号和schema前缀
		refTablePart := strings.TrimSpace(parts[0])
		refTablePart = strings.Trim(refTablePart, "`")

		// 处理包含schema的情况，如 `sbtest`.`tb_dept1`
		if strings.Contains(refTablePart, ".") {
			tableParts := strings.Split(refTablePart, ".")
			refTable = strings.Trim(tableParts[len(tableParts)-1], "`")
		} else {
			refTable = refTablePart
		}

		// 提取引用字段名
		fieldPart := strings.TrimSpace(parts[1])
		fieldEndIndex := strings.Index(fieldPart, ")")
		if fieldEndIndex != -1 {
			refColumn = strings.TrimSpace(fieldPart[:fieldEndIndex])
			refColumn = strings.Trim(refColumn, "`")
		}
	}

	return refTable, refColumn
}

// 从源端数据库获取表的外键定义信息
func (my *MysqlDataAbnormalFixStruct) LoadForeignKeyDefinitions(db *sql.DB, logThreadSeq int64) error {
	var vlog string

	// 初始化外键定义映射
	if my.ForeignKeyDefinitions == nil {
		my.ForeignKeyDefinitions = make(map[string]string)
	}

	// 使用源端schema进行查询
	sourceSchema := my.SourceSchema
	if sourceSchema == "" {
		sourceSchema = my.Schema
	}

	vlog = fmt.Sprintf("(%d) Loading foreign key definitions for table %s.%s from source schema %s",
		logThreadSeq, sourceSchema, my.Table, sourceSchema)
	global.Wlog.Debug(vlog)

	// Query the child-side KEY_COLUMN_USAGE rows directly so composite foreign
	// keys keep their ordinal position and do not cross-join referenced columns.
	query := `
		SELECT 
			kcu.CONSTRAINT_NAME,
			kcu.ORDINAL_POSITION,
			kcu.COLUMN_NAME AS SOURCE_COLUMN_NAME,
			kcu.REFERENCED_TABLE_SCHEMA,
			kcu.REFERENCED_TABLE_NAME,
			kcu.REFERENCED_COLUMN_NAME
		FROM 
			INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
		WHERE 
			kcu.TABLE_SCHEMA = ? 
			AND kcu.TABLE_NAME = ?
			AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY 
			kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
	`

	rows, err := db.Query(query, sourceSchema, my.Table)
	if err != nil {
		vlog = fmt.Sprintf("(%d) Error querying foreign key definitions: %v", logThreadSeq, err)
		global.Wlog.Warn(vlog)
		return err
	}

	fkInfoMap := make(map[string][]foreignKeyColumn)

	for rows.Next() {
		// 使用sql.NullString处理可能为NULL的值
		var constraintName, sourceColumn string
		var ordinalPosition int
		var referencedSchema, referencedTable, referencedColumn sql.NullString

		if err := rows.Scan(&constraintName, &ordinalPosition, &sourceColumn, &referencedSchema, &referencedTable, &referencedColumn); err != nil {
			vlog = fmt.Sprintf("(%d) Error scanning foreign key row: %v", logThreadSeq, err)
			global.Wlog.Warn(vlog)
			continue
		}

		// 将sql.NullString转换为普通string，NULL值转为空字符串
		referencedSchemaStr := ""
		referencedTableStr := ""
		referencedColumnStr := ""
		if referencedSchema.Valid {
			referencedSchemaStr = referencedSchema.String
		}
		if referencedTable.Valid {
			referencedTableStr = referencedTable.String
		}
		if referencedColumn.Valid {
			referencedColumnStr = referencedColumn.String
		}

		// 存储外键信息
		fkInfoMap[constraintName] = append(fkInfoMap[constraintName], foreignKeyColumn{
			ordinalPosition:  ordinalPosition,
			columnName:       sourceColumn,
			referencedSchema: referencedSchemaStr,
			referencedTable:  referencedTableStr,
			referencedColumn: referencedColumnStr,
		})
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		return rowsErr
	}
	_ = rows.Close()

	// 构建完整的外键DDL定义
	for fkName, infoRows := range fkInfoMap {
		fkDDL, ok := buildForeignKeyDDLForFix(fkName, infoRows, sourceSchema)
		if !ok {
			vlog = fmt.Sprintf("(%d) Invalid foreign key info for %s: missing referenced table or column",
				logThreadSeq, fkName)
			global.Wlog.Warn(vlog)
			continue
		}
		my.ForeignKeyDefinitions[fkName] = fkDDL
		vlog = fmt.Sprintf("(%d) Found foreign key: %s", logThreadSeq, fkDDL)
		global.Wlog.Debug(vlog)
	}

	return nil
}
