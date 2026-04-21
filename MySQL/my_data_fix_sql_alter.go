package mysql

import (
	"fmt"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
	"strings"
)

func (my *MysqlDataAbnormalFixStruct) FixAlterIndexSqlExec(e, f []string, si map[string][]string, sourceDrive string, logThreadSeq int64) []string {
	var (
		sqlS         []string
		targetSchema = my.Schema // 使用目标schema（保持原始大小写）
		strsql       string
	)

	// 检查是否需要加载外键定义
	if my.ForeignKeyDefinitions == nil || len(my.ForeignKeyDefinitions) == 0 {
		// 输出警告日志，但继续执行，因为这里没有数据库连接
		vlog := fmt.Sprintf("(%d) Warning: Foreign key definitions not loaded for table %s.%s", logThreadSeq, my.Schema, my.Table)
		global.Wlog.Warn(vlog)
	}

	// 先生成 DROP 操作（f），再生成 ADD 操作（e），确保合并后的 ALTER TABLE 中 DROP 在 ADD 之前
	for _, v := range f {
		// 检查是否是外键约束
		isForeignKey := false
		if my.ForeignKeyDefinitions != nil {
			_, isForeignKey = my.ForeignKeyDefinitions[v]
		}

		if isForeignKey {
			// 删除外键约束
			strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP FOREIGN KEY %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), mysqlQuoteIdent(v))
		} else {
			// 处理普通索引、唯一索引和主键索引
			switch my.IndexType {
			case "pri":
				strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP PRIMARY KEY;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table))
			case "uni":
				strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP INDEX %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), mysqlQuoteIdent(v))
			case "mul":
				strsql = fmt.Sprintf("ALTER TABLE %s.%s DROP INDEX %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), mysqlQuoteIdent(v))
			}
		}
		sqlS = append(sqlS, strsql)
	}

	for _, v := range e {
		var c []string
		for _, vi := range si[v] {
			// 从vi字符串中提取列名及前缀长度
			// 格式：columnName/*seq*/indexSeq/*type*/columnType/*prefix*/P
			c = append(c, mysqlIndexColDDLExpr(vi))
		}

		// 检查是否是外键约束
		isForeignKey := false
		fkDDL := ""
		if my.ForeignKeyDefinitions != nil {
			if ddl, exists := my.ForeignKeyDefinitions[v]; exists {
				isForeignKey = true
				fkDDL = ddl
			}
		}

		// 构建SQL语句
		if isForeignKey {
			// 生成外键约束的SQL
			strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), fkDDL)
			vlog := fmt.Sprintf("(%d) Generating foreign key SQL: %s", logThreadSeq, strsql)
			global.Wlog.Debug(vlog)
		} else {
			// 生成普通索引的SQL
			// MariaDB 使用 IGNORED 关键字隐藏索引，MySQL 使用 INVISIBLE。
			indexHiddenKeyword := "INVISIBLE"
			if my.DestFlavor == global.DatabaseFlavorMariaDB {
				indexHiddenKeyword = "IGNORED"
			}
			var invisibleClause string
			if my.IndexVisibilityMap != nil {
				if visibility, exists := my.IndexVisibilityMap[v]; exists && (strings.EqualFold(visibility, "NO") || strings.EqualFold(visibility, "INVISIBLE") || strings.EqualFold(visibility, "IGNORED")) {
					invisibleClause = " " + indexHiddenKeyword
				}
			}
			switch my.IndexType {
			case "pri":
				strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD PRIMARY KEY(%s);", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), strings.Join(c, ", "))
			case "uni":
				strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD UNIQUE INDEX %s(%s)%s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), mysqlQuoteIdent(v), strings.Join(c, ", "), invisibleClause)
			case "mul":
				strsql = fmt.Sprintf("ALTER TABLE %s.%s ADD INDEX %s(%s)%s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), mysqlQuoteIdent(v), strings.Join(c, ", "), invisibleClause)
			}
		}
		sqlS = append(sqlS, strsql)
	}

	return sqlS
}

func (my *MysqlDataAbnormalFixStruct) FixAlterColumnSqlDispos(alterType string, columnDataType []string, columnSeq int, lastColumn, curryColumn string, logThreadSeq int64) string {
	var sqlS string
	if len(columnDataType) > 6 {
		directDefinition := strings.TrimSpace(columnDataType[6])
		if directDefinition != "" && !strings.EqualFold(directDefinition, "null") {
			// MODIFY existing primary-key columns should not inline PRIMARY KEY again.
			if strings.EqualFold(alterType, "modify") && shouldSkipInlinePrimaryKeyClause(my.Schema, my.Table, curryColumn) {
				directDefinition = normalizeInlinePrimaryKeyClause(directDefinition)
			}
			columnLocation := ""
			if columnSeq == 0 {
				columnLocation = "FIRST"
			} else if lastColumn != "alterNoAfter" {
				columnLocation = fmt.Sprintf("AFTER `%s`", lastColumn)
			}

			switch alterType {
			case "add":
				if columnLocation != "" {
					return fmt.Sprintf(" ADD COLUMN `%s` %s %s", curryColumn, directDefinition, columnLocation)
				}
				return fmt.Sprintf(" ADD COLUMN `%s` %s", curryColumn, directDefinition)
			case "modify":
				if columnLocation != "" {
					return fmt.Sprintf(" MODIFY COLUMN `%s` %s %s", curryColumn, directDefinition, columnLocation)
				}
				return fmt.Sprintf(" MODIFY COLUMN `%s` %s", curryColumn, directDefinition)
			case "change":
				parts := strings.Split(curryColumn, ":")
				if len(parts) == 2 {
					if columnLocation != "" {
						return fmt.Sprintf(" CHANGE COLUMN `%s` `%s` %s %s", parts[0], parts[1], directDefinition, columnLocation)
					}
					return fmt.Sprintf(" CHANGE COLUMN `%s` `%s` %s", parts[0], parts[1], directDefinition)
				}
				if columnLocation != "" {
					return fmt.Sprintf(" MODIFY COLUMN `%s` %s %s", curryColumn, directDefinition, columnLocation)
				}
				return fmt.Sprintf(" MODIFY COLUMN `%s` %s", curryColumn, directDefinition)
			}
		}
	}

	// 构建属性列表，只添加非空的值
	var attributes []string

	// 预处理数据类型，移除INVISIBLE关键字（如果存在）
	hasInvisible := false
	if strings.Contains(strings.ToUpper(columnDataType[0]), "INVISIBLE") {
		hasInvisible = true
		// 从数据类型中完全移除INVISIBLE关键字，使用大小写不敏感的替换
		columnDataType[0] = strings.ReplaceAll(columnDataType[0], "INVISIBLE", "")
		columnDataType[0] = strings.ReplaceAll(columnDataType[0], "invisible", "")
		// 去除多余的空格
		columnDataType[0] = strings.TrimSpace(columnDataType[0])
		// 处理可能的多个空格情况
		for strings.Contains(columnDataType[0], "  ") {
			columnDataType[0] = strings.ReplaceAll(columnDataType[0], "  ", " ")
		}
	}
	columnDataType[0] = stripDeprecatedZeroFillAttr(columnDataType[0])
	columnDataType[0] = schemacompat.StripMySQLMetadataOnlyExtraTokens(columnDataType[0])
	columnDataType[0] = normalizeMySQLKeywordFunctionsInDefinition(columnDataType[0])

	// 添加数据类型
	attributes = append(attributes, columnDataType[0])

	// 添加字符集
	if columnDataType[1] != "null" {
		attributes = append(attributes, fmt.Sprintf("CHARACTER SET %s", columnDataType[1]))
	}

	// 添加排序规则
	if columnDataType[2] != "null" {
		attributes = append(attributes, fmt.Sprintf("COLLATE %s", columnDataType[2]))
	}

	// 添加NOT NULL约束
	if strings.ToUpper(columnDataType[3]) == "NO" {
		attributes = append(attributes, "NOT NULL")
	}

	// Preserve SQL function defaults and NULL defaults without string quoting.
	if columnDataType[4] != "empty" {
		if defaultClause := formatMySQLColumnDefault(columnDataType[4], strings.ToUpper(columnDataType[3]) != "NO"); defaultClause != "" {
			attributes = append(attributes, defaultClause)
		}
	}

	// 添加COMMENT属性（用于struct模式下列注释修复）
	// 约定："null" 表示无值来源，不强制设置；空字符串会生成 COMMENT '' 以清空目标注释
	if len(columnDataType) > 5 {
		columnComment := columnDataType[5]
		if !strings.EqualFold(columnComment, "null") {
			attributes = append(attributes, fmt.Sprintf("COMMENT '%s'", escapeSQLString(columnComment)))
		}
	}

	// 初始化AutoIncrementColumnsWithPrimaryKey映射
	if AutoIncrementColumnsWithPrimaryKey == nil {
		AutoIncrementColumnsWithPrimaryKey = make(map[string]bool)
	}

	// 检查是否需要设置主键（对于自增列，无论是add还是modify操作）
	hasAutoIncrement := strings.Contains(strings.ToUpper(columnDataType[0]), "AUTO_INCREMENT")
	needInlinePrimaryKey := hasAutoIncrement
	if needInlinePrimaryKey && strings.EqualFold(alterType, "modify") && shouldSkipInlinePrimaryKeyClause(my.Schema, my.Table, curryColumn) {
		needInlinePrimaryKey = false
	}
	if needInlinePrimaryKey {
		// 对于自增列，需要设置为主键
		attributes = append(attributes, "PRIMARY KEY")
		// 标记该列已经设置了主键，避免在索引修复时重复设置
		key := fmt.Sprintf("%s.%s.%s", my.Schema, my.Table, curryColumn)
		AutoIncrementColumnsWithPrimaryKey[key] = true
	}

	// 添加INVISIBLE关键字（如果存在）
	if hasInvisible {
		attributes = append(attributes, "INVISIBLE")
	}

	// 添加列位置
	columnLocation := ""
	if columnSeq == 0 {
		columnLocation = "FIRST"
	} else if lastColumn != "alterNoAfter" {
		columnLocation = fmt.Sprintf("AFTER `%s`", lastColumn)
	}

	// 构建最终SQL
	switch alterType {
	case "add", "modify":
		// 检查是否需要设置主键
		hasPrimaryKeyAttr := false
		for _, attr := range attributes {
			if strings.ToUpper(attr) == "PRIMARY KEY" {
				hasPrimaryKeyAttr = true
				break
			}
		}

		// 只有当目标表存在主键且当前列不是已有单列主键时，才需要删除旧主键。
		// 对已有主键列做 MODIFY 时，保留原主键即可，避免对 generated invisible
		// primary key 这类场景生成多余的 DROP PRIMARY KEY。
		key := fmt.Sprintf("%s.%s", my.Schema, my.Table)
		needDropPrimaryKey := hasPrimaryKeyAttr && DestTableHasPrimaryKey[key]
		if needDropPrimaryKey && strings.EqualFold(alterType, "modify") {
			primaryKeyColumns := cachedPrimaryKeyColumns(my.Schema, my.Table)
			if len(primaryKeyColumns) == 1 && strings.EqualFold(primaryKeyColumns[0], curryColumn) {
				needDropPrimaryKey = false
			}
		}

		// 统一处理ADD和MODIFY操作，确保主键处理逻辑一致
		operation := "ADD COLUMN"
		if alterType == "modify" {
			operation = "MODIFY COLUMN"
		}

		if columnLocation != "" {
			if needDropPrimaryKey {
				// 先删除旧主键，再进行列操作
				sqlS = fmt.Sprintf(" DROP PRIMARY KEY, %s `%s` %s %s", operation, curryColumn, strings.Join(attributes, " "), columnLocation)
			} else {
				sqlS = fmt.Sprintf(" %s `%s` %s %s", operation, curryColumn, strings.Join(attributes, " "), columnLocation)
			}
		} else {
			if needDropPrimaryKey {
				sqlS = fmt.Sprintf(" DROP PRIMARY KEY, %s `%s` %s", operation, curryColumn, strings.Join(attributes, " "))
			} else {
				sqlS = fmt.Sprintf(" %s `%s` %s", operation, curryColumn, strings.Join(attributes, " "))
			}
		}
	case "drop":
		sqlS = fmt.Sprintf(" DROP COLUMN `%s`", curryColumn)
	case "change":
		// 对于CHANGE操作，需要原始列名和新列名
		// 假设curryColumn格式为"原始列名:新列名"
		parts := strings.Split(curryColumn, ":")
		if len(parts) == 2 {
			originalCol := parts[0]
			newCol := parts[1]
			if columnLocation != "" {
				sqlS = fmt.Sprintf(" CHANGE COLUMN `%s` `%s` %s %s", originalCol, newCol, strings.Join(attributes, " "), columnLocation)
			} else {
				sqlS = fmt.Sprintf(" CHANGE COLUMN `%s` `%s` %s", originalCol, newCol, strings.Join(attributes, " "))
			}
		} else {
			// 如果格式不正确，降级为MODIFY
			if columnLocation != "" {
				sqlS = fmt.Sprintf(" MODIFY COLUMN `%s` %s %s", curryColumn, strings.Join(attributes, " "), columnLocation)
			} else {
				sqlS = fmt.Sprintf(" MODIFY COLUMN `%s` %s", curryColumn, strings.Join(attributes, " "))
			}
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
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), strings.Join(modifyColumn, ",")))
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

	// Filter index operations before combining them into one ALTER TABLE.
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
	filteredIndexOperations = my.filterRedundantDropPrimaryKeyOperations(columnOperations, filteredIndexOperations)

	// 合并所有操作
	var allOperations []string
	allOperations = append(allOperations, columnOperations...)
	allOperations = append(allOperations, filteredIndexOperations...)

	if len(allOperations) > 0 {
		// 提取操作内容（去除ALTER TABLE前缀和分号）
		var operationContents []string
		for _, op := range allOperations {
			content := normalizeAlterOperationContent(op)
			if content != "" {
				operationContents = append(operationContents, content)
			}
		}

		if len(operationContents) > 0 {
			// 生成单个ALTER TABLE语句，包含所有操作
			alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), strings.Join(operationContents, ", ")))

			// 添加调试日志
			vlog := fmt.Sprintf("(%d) Generated combined ALTER TABLE SQL for %s.%s: %d column operations, %d index operations",
				logThreadSeq, targetSchema, my.Table, len(columnOperations), len(indexOperations))
			global.Wlog.Debug(vlog)
		}
	}

	return alterSql
}

func normalizeAlterOperationContent(op string) string {
	op = strings.TrimSpace(op)
	if op == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToUpper(op), "ALTER TABLE") {
		// 用正则提取操作片段，支持含空格的反引号标识符（BUG-5 修复）
		m := alterTablePrefixRe.FindStringSubmatch(op)
		if m != nil {
			return strings.TrimSpace(m[1])
		}
	}
	return strings.TrimSpace(strings.TrimSuffix(op, ";"))
}

func extractDroppedColumnNameFromAlterClause(op string) string {
	clause := normalizeAlterOperationContent(op)
	if clause == "" || !strings.HasPrefix(strings.ToUpper(clause), "DROP COLUMN") {
		return ""
	}

	rest := strings.TrimSpace(clause[len("DROP COLUMN"):])
	if rest == "" {
		return ""
	}
	if strings.HasPrefix(rest, "`") {
		end := strings.Index(rest[1:], "`")
		if end == -1 {
			return ""
		}
		return rest[1 : end+1]
	}
	parts := strings.Fields(rest)
	if len(parts) == 0 {
		return ""
	}
	return strings.Trim(parts[0], "`")
}

func collectDroppedColumns(columnOperations []string) map[string]struct{} {
	droppedColumns := make(map[string]struct{})
	for _, op := range columnOperations {
		columnName := extractDroppedColumnNameFromAlterClause(op)
		if strings.TrimSpace(columnName) == "" {
			continue
		}
		droppedColumns[strings.ToUpper(columnName)] = struct{}{}
	}
	return droppedColumns
}

func cachedPrimaryKeyColumns(schema, table string) []string {
	tableKey := fmt.Sprintf("%s.%s", schema, table)
	tablePrimaryKeyMutex.RLock()
	defer tablePrimaryKeyMutex.RUnlock()
	if columns, exists := TablePrimaryKeyColumns[tableKey]; exists {
		copied := make([]string, len(columns))
		copy(copied, columns)
		return copied
	}
	return nil
}

func containsPrimaryKeyColumn(primaryKeyColumns []string, column string) bool {
	trimmedColumn := strings.TrimSpace(column)
	if trimmedColumn == "" {
		return false
	}
	for _, pkColumn := range primaryKeyColumns {
		if strings.EqualFold(strings.TrimSpace(pkColumn), trimmedColumn) {
			return true
		}
	}
	return false
}

func shouldSkipInlinePrimaryKeyClause(schema, table, column string) bool {
	primaryKeyColumns := cachedPrimaryKeyColumns(schema, table)
	if len(primaryKeyColumns) == 0 {
		return false
	}
	return containsPrimaryKeyColumn(primaryKeyColumns, column)
}

func normalizeInlinePrimaryKeyClause(definition string) string {
	cleaned := inlinePrimaryKeyPattern.ReplaceAllString(definition, "")
	normalized := strings.Join(strings.Fields(cleaned), " ")
	return strings.TrimSpace(normalized)
}

func (my *MysqlDataAbnormalFixStruct) filterRedundantDropPrimaryKeyOperations(columnOperations, indexOperations []string) []string {
	droppedColumns := collectDroppedColumns(columnOperations)
	if len(droppedColumns) == 0 {
		return indexOperations
	}

	primaryKeyColumns := cachedPrimaryKeyColumns(my.Schema, my.Table)
	if len(primaryKeyColumns) != 1 {
		return indexOperations
	}
	if _, exists := droppedColumns[strings.ToUpper(primaryKeyColumns[0])]; !exists {
		return indexOperations
	}

	filtered := make([]string, 0, len(indexOperations))
	for _, op := range indexOperations {
		clause := normalizeAlterOperationContent(op)
		if strings.EqualFold(strings.TrimSpace(clause), "DROP PRIMARY KEY") {
			continue
		}
		filtered = append(filtered, op)
	}
	return filtered
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
			content := normalizeAlterOperationContent(op)
			if content != "" {
				operationContents = append(operationContents, content)
			}
		}

		if len(operationContents) > 0 {
			// 生成单个ALTER TABLE语句，包含所有索引操作
			alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), strings.Join(operationContents, ", ")))

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

	// 防护空 charset：当 LEFT JOIN COLLATIONS 失败时 charset 可能为空，
	// 此时从 collation 名推断 charset，最终兜底为 utf8mb4。
	trimmedCharset := strings.TrimSpace(charset)
	if trimmedCharset == "" {
		trimmedCharset = schemacompat.InferCharsetFromCollation(collation)
		if trimmedCharset == "" {
			trimmedCharset = "utf8mb4"
		}
		if global.Wlog != nil {
			vlog := fmt.Sprintf("(%d) Table charset was empty, inferred as %s from collation %s for %s.%s",
				logThreadSeq, trimmedCharset, collation, targetSchema, my.Table)
			global.Wlog.Warn(vlog)
		}
	}

	// 生成表级别字符集转换的SQL语句
	if strings.TrimSpace(collation) == "" {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s CONVERT TO CHARACTER SET %s;",
			mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), trimmedCharset))
	} else {
		alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s CONVERT TO CHARACTER SET %s COLLATE %s;",
			mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), trimmedCharset, collation))
	}

	// 添加日志，方便调试
	vlog := fmt.Sprintf("(%d) Generated table charset conversion SQL: %s", logThreadSeq, alterSql[0])
	if global.Wlog != nil {
		global.Wlog.Debug(vlog)
	}

	return alterSql
}

// FixTableAutoIncrementSqlGenerate generates table-level AUTO_INCREMENT fix SQL.
func (my *MysqlDataAbnormalFixStruct) FixTableAutoIncrementSqlGenerate(nextValue int64, logThreadSeq int64) []string {
	var (
		alterSql     []string
		targetSchema = my.Schema
	)

	alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s AUTO_INCREMENT=%d;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), nextValue))

	vlog := fmt.Sprintf("(%d) Generated table AUTO_INCREMENT SQL: %s", logThreadSeq, alterSql[0])
	if global.Wlog != nil {
		global.Wlog.Debug(vlog)
	}

	return alterSql
}
