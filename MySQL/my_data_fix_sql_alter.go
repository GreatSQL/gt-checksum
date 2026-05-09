package mysql

import (
	"fmt"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
	"regexp"
	"strings"
)

// ExtractPartitionColumnsFromExpressions 从分区表达式中提取分区列名
// 支持 RANGE、LIST、HASH 等分区方式
// 例如：`name` -> [name], `id`, `name` -> [id, name]
func ExtractPartitionColumnsFromExpressions(expressions []string) []string {
	if len(expressions) == 0 {
		return nil
	}

	columnSet := make(map[string]struct{})
	// 匹配反引号或不含空格的列名
	columnPattern := regexp.MustCompile("`([^`]+)`|\\b([a-zA-Z_][a-zA-Z0-9_]*)\\b")

	for _, expr := range expressions {
		if strings.TrimSpace(expr) == "" {
			continue
		}
		matches := columnPattern.FindAllStringSubmatch(expr, -1)
		for _, match := range matches {
			var colName string
			if match[1] != "" {
				colName = match[1]
			} else if match[2] != "" {
				colName = match[2]
			}
			if colName != "" && !isReservedKeyword(colName) {
				columnSet[strings.ToLower(colName)] = struct{}{}
			}
		}
	}

	if len(columnSet) == 0 {
		return nil
	}

	columns := make([]string, 0, len(columnSet))
	for col := range columnSet {
		columns = append(columns, col)
	}
	return columns
}

// isReservedKeyword 检查是否是 MySQL 保留关键字
func isReservedKeyword(word string) bool {
	reserved := map[string]bool{
		"year":      true,
		"month":     true,
		"day":       true,
		"to_days":   true,
		"dayofweek": true,
		"weekday":   true,
		"dayofyear": true,
		"quarter":   true,
		"week":      true,
		"hour":      true,
		"minute":    true,
		"second":    true,
		"abs":       true,
		"mod":       true,
		"and":       true,
		"or":        true,
		"not":       true,
		"between":   true,
		"in":        true,
		"is":        true,
		"null":      true,
		"true":      true,
		"false":     true,
	}
	return reserved[strings.ToLower(word)]
}

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
	// 注释掉 stripDeprecatedZeroFillAttr 调用，保留 zerofill 属性以生成正确的修复 SQL
	// columnDataType[0] = stripDeprecatedZeroFillAttr(columnDataType[0])
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

	// 检查表是否有分区，如果有分区且需要添加主键，则不能内联 PRIMARY KEY
	hasPartition := len(my.PartitionColumns) > 0
	if needInlinePrimaryKey && hasPartition && strings.EqualFold(alterType, "add") {
		// 对于分区表，不能在 ADD COLUMN 时内联 PRIMARY KEY
		// 而是需要生成单独的 ADD PRIMARY KEY 语句
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

// FixAlterColumnAndIndexSqlGenerate 合并列修复和索引修复操作，生成ALTER TABLE语句
// 特殊处理：my_row_id 的 VISIBLE/INVISIBLE 操作必须作为独立的 ALTER TABLE 语句输出，不能与其他操作合并
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
	columnOperations, filteredIndexOperations = my.filterRedundantDropPrimaryKeyOperations(columnOperations, filteredIndexOperations)

	// 分离 my_row_id 的 VISIBLE/INVISIBLE 操作、AUTO_INCREMENT 操作和其他操作
	var myRowIDOperations []string
	var autoIncrementOperations []string
	var regularOperations []string

	for _, op := range columnOperations {
		if isMyRowIDVisibilityOperation(op) {
			myRowIDOperations = append(myRowIDOperations, op)
		} else if isAutoIncrementOnlyOperation(op) {
			autoIncrementOperations = append(autoIncrementOperations, op)
		} else {
			regularOperations = append(regularOperations, op)
		}
	}

	// 合并常规操作（不包括 my_row_id 的 VISIBLE/INVISIBLE 操作）
	var allRegularOperations []string
	allRegularOperations = append(allRegularOperations, regularOperations...)
	allRegularOperations = append(allRegularOperations, filteredIndexOperations...)

	if len(allRegularOperations) > 0 {
		// 提取操作内容（去除ALTER TABLE前缀和分号）
		var operationContents []string
		for _, op := range allRegularOperations {
			content := normalizeAlterOperationContent(op)
			if content != "" {
				operationContents = append(operationContents, content)
			}
		}

		if len(operationContents) > 0 {
			// 生成单个ALTER TABLE语句，包含所有常规操作
			alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), strings.Join(operationContents, ", ")))

			// 添加调试日志
			vlog := fmt.Sprintf("(%d) Generated combined ALTER TABLE SQL for %s.%s: %d regular column operations, %d index operations",
				logThreadSeq, targetSchema, my.Table, len(regularOperations), len(indexOperations))
			global.Wlog.Debug(vlog)
		}
	}

	// 将 my_row_id 的 VISIBLE/INVISIBLE 操作作为独立的 ALTER TABLE 语句追加
	// 这些操作必须按顺序执行，不能与其他操作合并
	if len(myRowIDOperations) > 0 {
		for _, op := range myRowIDOperations {
			// 如果操作已经是完整的 ALTER TABLE 语句，直接使用
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(op)), "ALTER TABLE") {
				alterSql = append(alterSql, op)
			} else {
				// 否则，构造完整的 ALTER TABLE 语句
				alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), strings.TrimSpace(op)))
			}
		}
		vlog := fmt.Sprintf("(%d) Appended %d independent my_row_id VISIBLE/INVISIBLE operations for %s.%s",
			logThreadSeq, len(myRowIDOperations), targetSchema, my.Table)
		global.Wlog.Debug(vlog)
	}

	// 将 AUTO_INCREMENT 操作作为独立的 ALTER TABLE 语句追加
	// 这些操作必须独立执行，不能与其他操作合并（MySQL 限制）
	if len(autoIncrementOperations) > 0 {
		for _, op := range autoIncrementOperations {
			// 如果操作已经是完整的 ALTER TABLE 语句，直接使用
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(op)), "ALTER TABLE") {
				alterSql = append(alterSql, op)
			} else {
				// 否则，构造完整的 ALTER TABLE 语句
				alterSql = append(alterSql, fmt.Sprintf("ALTER TABLE %s.%s %s;", mysqlQuoteIdent(targetSchema), mysqlQuoteIdent(my.Table), strings.TrimSpace(op)))
			}
		}
		vlog := fmt.Sprintf("(%d) Appended %d independent AUTO_INCREMENT operations for %s.%s",
			logThreadSeq, len(autoIncrementOperations), targetSchema, my.Table)
		global.Wlog.Debug(vlog)
	}

	return alterSql
}

// isMyRowIDVisibilityOperation 检查操作是否是 my_row_id 的 VISIBLE/INVISIBLE 操作
func isMyRowIDVisibilityOperation(op string) bool {
	upperOp := strings.ToUpper(strings.TrimSpace(op))
	// 检查是否包含 my_row_id 和 VISIBLE/INVISIBLE 关键字
	if !strings.Contains(upperOp, "MY_ROW_ID") {
		return false
	}
	if !strings.Contains(upperOp, "VISIBLE") && !strings.Contains(upperOp, "INVISIBLE") {
		return false
	}
	// 检查是否是 MODIFY COLUMN 操作
	if !strings.Contains(upperOp, "MODIFY COLUMN") {
		return false
	}
	return true
}

// isAutoIncrementOnlyOperation 检查操作是否是纯 AUTO_INCREMENT 操作
func isAutoIncrementOnlyOperation(op string) bool {
	upperOp := strings.ToUpper(strings.TrimSpace(op))
	// 检查是否是 ALTER TABLE ... AUTO_INCREMENT=N 格式
	if !strings.Contains(upperOp, "ALTER TABLE") {
		return false
	}
	if !strings.Contains(upperOp, "AUTO_INCREMENT=") {
		return false
	}
	// 检查是否只包含 AUTO_INCREMENT 操作（没有其他操作如 ADD/DROP/MODIFY COLUMN 等）
	// 通过检查是否包含逗号来判断是否有多个操作
	if strings.Contains(upperOp, ",") {
		return false
	}
	// 检查是否包含其他 ALTER TABLE 操作关键字
	otherOperations := []string{
		"ADD COLUMN", "DROP COLUMN", "MODIFY COLUMN", "CHANGE COLUMN",
		"ADD INDEX", "DROP INDEX", "ADD KEY", "DROP KEY",
		"ADD PRIMARY KEY", "DROP PRIMARY KEY",
		"ADD CONSTRAINT", "DROP CONSTRAINT",
		"CONVERT TO CHARACTER SET", "COLLATE",
	}
	for _, opKeyword := range otherOperations {
		if strings.Contains(upperOp, opKeyword) {
			return false
		}
	}
	return true
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
		// 规范化操作内容，提取 ALTER TABLE 后的操作部分
		clause := normalizeAlterOperationContent(op)
		if clause == "" {
			continue
		}

		// 按逗号分割多个操作（如 "DROP COLUMN f9, DROP COLUMN my_row_id, DROP PRIMARY KEY, ..."）
		operations := strings.Split(clause, ",")
		for _, singleOp := range operations {
			singleOp = strings.TrimSpace(singleOp)
			if !strings.HasPrefix(strings.ToUpper(singleOp), "DROP COLUMN") {
				continue
			}

			// 提取列名
			rest := strings.TrimSpace(singleOp[len("DROP COLUMN"):])
			if rest == "" {
				continue
			}

			var columnName string
			if strings.HasPrefix(rest, "`") {
				end := strings.Index(rest[1:], "`")
				if end == -1 {
					continue
				}
				columnName = rest[1 : end+1]
			} else {
				parts := strings.Fields(rest)
				if len(parts) == 0 {
					continue
				}
				columnName = strings.Trim(parts[0], "`")
			}

			if strings.TrimSpace(columnName) != "" {
				droppedColumns[strings.ToUpper(columnName)] = struct{}{}
			}
		}
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

func (my *MysqlDataAbnormalFixStruct) filterRedundantDropPrimaryKeyOperations(columnOperations, indexOperations []string) ([]string, []string) {
	droppedColumns := collectDroppedColumns(columnOperations)
	primaryKeyColumns := cachedPrimaryKeyColumns(my.Schema, my.Table)

	// 检查是否将要添加显式主键列（通过 ADD COLUMN ... PRIMARY KEY）
	hasExplicitPrimaryKeyAddition := false
	for _, op := range columnOperations {
		upperOp := strings.ToUpper(op)
		if strings.Contains(upperOp, "ADD COLUMN") && strings.Contains(upperOp, "PRIMARY KEY") {
			hasExplicitPrimaryKeyAddition = true
			break
		}
	}

	// 如果将要添加显式主键，且当前主键是 my_row_id，则需要先删除 my_row_id 列
	modifiedColumnOperations := columnOperations
	if hasExplicitPrimaryKeyAddition && len(primaryKeyColumns) == 1 {
		pkCol := strings.ToLower(strings.TrimSpace(primaryKeyColumns[0]))
		if pkCol == "my_row_id" {
			// 检查是否已经有删除 my_row_id 的操作
			if _, exists := droppedColumns[strings.ToUpper(primaryKeyColumns[0])]; !exists {
				// 在 columnOperations 开头插入 DROP COLUMN my_row_id 操作
				dropMyRowIDOp := fmt.Sprintf(" DROP COLUMN `%s`", primaryKeyColumns[0])
				modifiedColumnOperations = make([]string, 0, len(columnOperations)+1)
				modifiedColumnOperations = append(modifiedColumnOperations, dropMyRowIDOp)
				modifiedColumnOperations = append(modifiedColumnOperations, columnOperations...)

				vlog := fmt.Sprintf("Inserted DROP COLUMN my_row_id before explicit PRIMARY KEY addition in %s.%s",
					my.Schema, my.Table)
				global.Wlog.Info(vlog)
			}
		}
	}

	// 如果已经删除了主键列，则过滤掉 DROP PRIMARY KEY 操作
	if len(droppedColumns) > 0 && len(primaryKeyColumns) == 1 {
		if _, exists := droppedColumns[strings.ToUpper(primaryKeyColumns[0])]; exists {
			filtered := make([]string, 0, len(indexOperations))
			for _, op := range indexOperations {
				clause := normalizeAlterOperationContent(op)
				if strings.EqualFold(strings.TrimSpace(clause), "DROP PRIMARY KEY") {
					continue
				}
				filtered = append(filtered, op)
			}
			return modifiedColumnOperations, filtered
		}
	}

	// 如果将要添加显式主键且当前主键是 my_row_id，也需要过滤掉 DROP PRIMARY KEY
	if hasExplicitPrimaryKeyAddition && len(primaryKeyColumns) == 1 {
		pkCol := strings.ToLower(strings.TrimSpace(primaryKeyColumns[0]))
		if pkCol == "my_row_id" {
			filtered := make([]string, 0, len(indexOperations))
			for _, op := range indexOperations {
				clause := normalizeAlterOperationContent(op)
				if strings.EqualFold(strings.TrimSpace(clause), "DROP PRIMARY KEY") {
					vlog := fmt.Sprintf("Filtered DROP PRIMARY KEY for my_row_id in %s.%s (explicit PK will be added)",
						my.Schema, my.Table)
					global.Wlog.Debug(vlog)
					continue
				}
				filtered = append(filtered, op)
			}
			return modifiedColumnOperations, filtered
		}
	}

	return modifiedColumnOperations, indexOperations
}

// GeneratePartitionTablePrimaryKeySql 为分区表生成主键修复 SQL
// 当表有分区且需要添加主键时，主键必须包含分区列
func (my *MysqlDataAbnormalFixStruct) GeneratePartitionTablePrimaryKeySql(myRowIDColumn string, logThreadSeq int64) string {
	if len(my.PartitionColumns) == 0 {
		return ""
	}

	// 构建主键列列表：my_row_id 在第一个位置，然后是分区列
	pkColumns := make([]string, 0, len(my.PartitionColumns)+1)
	pkColumns = append(pkColumns, mysqlQuoteIdent(myRowIDColumn))

	// 添加分区列
	for _, col := range my.PartitionColumns {
		pkColumns = append(pkColumns, mysqlQuoteIdent(col))
	}

	targetSchema := my.Schema
	sql := fmt.Sprintf(" ADD PRIMARY KEY(%s)", strings.Join(pkColumns, ", "))
	if global.Wlog != nil {
		vlog := fmt.Sprintf("(%d) Generated partition table primary key SQL for %s.%s: %s", logThreadSeq, targetSchema, my.Table, sql)
		global.Wlog.Debug(vlog)
	}
	return sql
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
