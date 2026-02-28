package actions

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	deleteStatementPattern       = regexp.MustCompile(`(?i)^DELETE\s+FROM\s+\x60([^\x60]+)\x60\.\x60([^\x60]+)\x60\s+WHERE\s+(.*?);$`)
	insertStatementPattern       = regexp.MustCompile(`(?i)^INSERT\s+INTO\s+\x60([^\x60]+)\x60\.\x60([^\x60]+)\x60\s*\(([^)]+)\)\s+VALUES\s*\((.+)\);$`)
	insertStatementNoColsPattern = regexp.MustCompile(`(?i)^INSERT\s+INTO\s+\x60([^\x60]+)\x60\.\x60([^\x60]+)\x60\s+VALUES\s*\((.+)\);$`)
)

// OptimizeDeleteSqls 优化 DELETE 语句，将相同表、相同列（单列或多列）的单条等值 DELETE 语句合并为 IN (...) 语句
func OptimizeDeleteSqls(sqls []string, maxSqlSize int, fixTrxNum int) []string {
	if len(sqls) <= 1 {
		return sqls
	}

	var optimizedSqls []string

	// 按 schema.table.column 聚合值
	type deleteGroup struct {
		schema string
		table  string
		column string
		values []string
	}

	// 顺序保留
	var groupKeys []string
	groups := make(map[string]*deleteGroup)

	for _, sql := range sqls {
		sqlTrim := strings.TrimSpace(sql)
		matches := deleteStatementPattern.FindStringSubmatch(sqlTrim)

		if len(matches) != 4 {
			optimizedSqls = append(optimizedSqls, sql)
			continue
		}

		schema := matches[1]
		table := matches[2]
		whereClause := strings.TrimSpace(matches[3])

		// 如果条件中包含 LIMIT 或者是模糊查询，放弃优化（虽然调用方已经过滤但增加安全保证）
		if strings.Contains(strings.ToUpper(whereClause), " LIMIT ") || strings.Contains(strings.ToUpper(whereClause), " IS NULL") {
			optimizedSqls = append(optimizedSqls, sql)
			continue
		}

		// 安全解析 whereClause，提取所有等值条件列和值
		// gt-checksum 输出的等值条件格式固定为 `col`='val' 或是 `col`=123，并用 and 连接
		cols, vals, ok := parseWhereConditions(whereClause)
		if !ok || len(cols) == 0 {
			optimizedSqls = append(optimizedSqls, sql)
			continue
		}

		// 将列组合为键，例如 `col1`, `col2` 或单一列 `col1`
		columnKey := strings.Join(cols, ", ")
		// 将值组合为元组，例如 ('val1', 'val2') 或单值 'val1'
		var valueTuple string
		if len(vals) > 1 {
			valueTuple = fmt.Sprintf("(%s)", strings.Join(vals, ", "))
			columnKey = fmt.Sprintf("(%s)", columnKey) // 复合主键用括号包裹
		} else {
			valueTuple = vals[0] // 单例主键直接使用值
		}

		key := fmt.Sprintf("%s.%s|%s", schema, table, columnKey)
		if _, exists := groups[key]; !exists {
			groups[key] = &deleteGroup{
				schema: schema,
				table:  table,
				column: columnKey,
				values: []string{},
			}
			groupKeys = append(groupKeys, key)
		}
		groups[key].values = append(groups[key].values, valueTuple)
	}

	// 生成合并后的 SQL
	for _, key := range groupKeys {
		group := groups[key]
		baseSql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s IN (", group.schema, group.table, group.column)

		var currentBatchValues []string
		currentLength := len(baseSql) + 2 // +2 for ");"

		for _, val := range group.values {
			valLen := len(val)
			if len(currentBatchValues) > 0 {
				valLen += 1 // for comma
			}

			// 如果超出数量限制或长度限制，生成一条 SQL 并开始新的批次
			// 但是只有1个元素时，就算超长也要生成啊（虽然不可能出现一行超长的情况，但是逻辑上）
			if len(currentBatchValues) >= fixTrxNum || (currentLength+valLen) > maxSqlSize {
				if len(currentBatchValues) > 0 {
					if len(currentBatchValues) == 1 {
						optimizedSqls = append(optimizedSqls, fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s = %s;", group.schema, group.table, group.column, currentBatchValues[0]))
					} else {
						optimizedSqls = append(optimizedSqls, fmt.Sprintf("%s%s);", baseSql, strings.Join(currentBatchValues, ",")))
					}
				}
				currentBatchValues = []string{val}
				currentLength = len(baseSql) + len(val) + 2
			} else {
				currentBatchValues = append(currentBatchValues, val)
				currentLength += valLen
			}
		}

		// 处理最后剩余的一批
		if len(currentBatchValues) > 0 {
			if len(currentBatchValues) == 1 {
				// 只有1个值时，降级为 = 等值比较
				optimizedSqls = append(optimizedSqls, fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s = %s;", group.schema, group.table, group.column, currentBatchValues[0]))
			} else {
				optimizedSqls = append(optimizedSqls, fmt.Sprintf("%s%s);", baseSql, strings.Join(currentBatchValues, ",")))
			}
		}
	}

	return optimizedSqls
}

// parseWhereConditions 安全解析 WHERE 语句的等值条件，支持忽略字符串值中的 AND 等关键字
func parseWhereConditions(where string) ([]string, []string, bool) {
	var conditions []string
	inQuote := false
	var last int
	for i := 0; i < len(where); i++ {
		// 跳过 MySQL 中的转义字符，比如 \', \", \\
		if where[i] == '\\' && i+1 < len(where) {
			i++
			continue
		}
		if where[i] == '\'' {
			inQuote = !inQuote
		}
		// 在非字符串内，寻找 ' and ' (大小写不敏感)
		if !inQuote && i+5 <= len(where) && strings.ToLower(where[i:i+5]) == " and " {
			conditions = append(conditions, strings.TrimSpace(where[last:i]))
			last = i + 5
			i += 4 // 跳过 " and" 的其余部分
		}
	}
	conditions = append(conditions, strings.TrimSpace(where[last:]))

	var cols []string
	var vals []string

	for _, cond := range conditions {
		if cond == "" {
			continue
		}
		// 寻找等号
		eqIdx := strings.Index(cond, "=")
		if eqIdx == -1 {
			// 如果有没有等号的复杂条件（如 IN, IS NULL, <, >），放弃合并优化
			return nil, nil, false
		}

		col := strings.TrimSpace(cond[:eqIdx])
		val := strings.TrimSpace(cond[eqIdx+1:])

		cols = append(cols, col)
		vals = append(vals, val)
	}

	return cols, vals, true
}

// OptimizeInsertSqls 优化 INSERT 语句，将相同表的多条 INSERT 语句合并为单条 VALUES 多组数据的格式
// 注意：根据MySQL唯一索引特性，NULL值记录不应被去重，因为NULL != NULL
func OptimizeInsertSqls(sqls []string, maxSqlSize int, fixTrxNum int) []string {
	if len(sqls) <= 1 {
		return sqls
	}

	var optimizedSqls []string

	type insertGroup struct {
		schema  string
		table   string
		columns string
		values  []string
	}

	var groupKeys []string
	groups := make(map[string]*insertGroup)

	for _, sql := range sqls {
		sqlTrim := strings.TrimSpace(sql)

		schema, table, columns, values, ok := parseInsertStatement(sqlTrim)
		if !ok {
			optimizedSqls = append(optimizedSqls, sql)
			continue
		}

		key := fmt.Sprintf("%s.%s|%s", schema, table, columns)
		if _, exists := groups[key]; !exists {
			groups[key] = &insertGroup{
				schema:  schema,
				table:   table,
				columns: columns,
				values:  []string{},
			}
			groupKeys = append(groupKeys, key)
		}

		// 不进行去重处理，保留所有原始记录
		// 根据MySQL唯一索引特性，NULL值记录不应被去重，因为NULL != NULL
		groups[key].values = append(groups[key].values, values)
	}

	for _, key := range groupKeys {
		group := groups[key]
		if len(group.values) == 0 {
			continue
		}

		baseSql := fmt.Sprintf("INSERT INTO `%s`.`%s`(%s) VALUES ", group.schema, group.table, group.columns)
		baseLength := len(baseSql)

		var currentBatchValues []string
		currentLength := baseLength

		for _, val := range group.values {
			valLen := len(val)
			if len(currentBatchValues) > 0 {
				valLen += 2
			}

			if len(currentBatchValues) >= fixTrxNum || (currentLength+valLen+2) > maxSqlSize {
				if len(currentBatchValues) > 0 {
					optimizedSqls = append(optimizedSqls, fmt.Sprintf("%s%s;", baseSql, strings.Join(currentBatchValues, ", ")))
				}
				currentBatchValues = []string{fmt.Sprintf("(%s)", val)}
				currentLength = baseLength + len(val) + 2
			} else {
				currentBatchValues = append(currentBatchValues, fmt.Sprintf("(%s)", val))
				currentLength += valLen
			}
		}

		if len(currentBatchValues) > 0 {
			optimizedSqls = append(optimizedSqls, fmt.Sprintf("%s%s;", baseSql, strings.Join(currentBatchValues, ", ")))
		}
	}

	return optimizedSqls
}

// parseInsertStatement 解析 INSERT 语句，提取 schema、table、columns 和 values
func parseInsertStatement(sql string) (schema, table, columns, values string, ok bool) {
	sql = strings.TrimSpace(sql)
	if !strings.HasPrefix(strings.ToUpper(sql), "INSERT INTO") {
		return "", "", "", "", false
	}

	matches := insertStatementPattern.FindStringSubmatch(sql)
	if len(matches) == 5 {
		return matches[1], matches[2], matches[3], matches[4], true
	}

	matchesNoCols := insertStatementNoColsPattern.FindStringSubmatch(sql)
	if len(matchesNoCols) == 4 {
		return matchesNoCols[1], matchesNoCols[2], "", matchesNoCols[3], true
	}

	return "", "", "", "", false
}
