package actions

import (
	"fmt"
	mysql "gt-checksum/MySQL"
	"path"
	"strings"
)

// matchRollSQLTarget reports whether rollback SQL should be generated for the given schema.table.
//
// genRollSQL values:
//   - "OFF" (default): never generate
//   - "ON": always generate
//   - custom: comma-separated list of "schema.table" patterns; "%" is treated as a wildcard ("*")
func matchRollSQLTarget(genRollSQL, schema, table string) bool {
	upper := strings.ToUpper(strings.TrimSpace(genRollSQL))
	if upper == "OFF" || upper == "" {
		return false
	}
	if upper == "ON" {
		return true
	}
	target := schema + "." + table
	for _, item := range strings.Split(genRollSQL, ",") {
		pattern := strings.TrimSpace(item)
		if pattern == "" {
			continue
		}
		// replace % with * for path.Match glob syntax
		pattern = strings.ReplaceAll(pattern, "%", "*")
		matched, err := path.Match(pattern, target)
		if err == nil && matched {
			return true
		}
	}
	return false
}

func (sp *SchedulePlan) shouldGenerateRollbackSQL() bool {
	if sp == nil {
		return false
	}
	if strings.ToLower(strings.TrimSpace(sp.checkObject)) != "data" {
		return false
	}
	datafixType := strings.ToLower(strings.TrimSpace(sp.datafixType))
	if datafixType != "file" && datafixType != "table" {
		return false
	}
	if strings.TrimSpace(sp.rollSqlDir) == "" {
		return false
	}
	return matchRollSQLTarget(sp.genRollSQL, sp.schema, sp.table)
}

func (sp *SchedulePlan) startRollbackDispos(queueDepth int, logThreadSeq int64) chan struct{} {
	if !sp.shouldGenerateRollbackSQL() {
		sp.rollCC = nil
		return nil
	}
	rollCC := make(chanString, queueDepth)
	sp.rollCC = rollCC
	rollDone := make(chan struct{})
	go func() {
		sp.RollbackDispos(rollCC, logThreadSeq)
		close(rollDone)
	}()
	return rollDone
}

// rollbackDeleteToInsert converts a DELETE fix SQL into a rollback INSERT statement.
// It parses the WHERE clause of the DELETE to extract column-value pairs and reconstructs
// an INSERT INTO statement. This handles pri/uni (no LIMIT) and mul (with LIMIT) formats.
func rollbackDeleteToInsert(deleteSQL, schema, table string) string {
	upper := strings.ToUpper(deleteSQL)
	whereIdx := strings.Index(upper, "WHERE")
	if whereIdx < 0 {
		return ""
	}
	whereClause := strings.TrimSpace(deleteSQL[whereIdx+5:])
	// Strip trailing LIMIT clause if present
	if limitIdx := strings.Index(strings.ToUpper(whereClause), " LIMIT "); limitIdx >= 0 {
		whereClause = strings.TrimSpace(whereClause[:limitIdx])
	}
	whereClause = strings.TrimRight(whereClause, "; ")

	if whereClause == "" {
		return ""
	}

	parts := splitWhereConditions(whereClause)
	if len(parts) == 0 {
		return ""
	}

	var colNames []string
	var values []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.ToUpper(part) == "AND" {
			continue
		}

		if strings.Contains(part, " IS NULL") {
			col := strings.TrimSpace(strings.Split(part, " IS")[0])
			col = strings.Trim(col, "`")
			colNames = append(colNames, fmt.Sprintf("`%s`", col))
			values = append(values, "NULL")
		} else if idx := strings.Index(part, " = "); idx > 0 {
			col := strings.TrimSpace(part[:idx])
			col = strings.Trim(col, "`")
			val := strings.TrimSpace(part[idx+3:])
			colNames = append(colNames, fmt.Sprintf("`%s`", col))
			values = append(values, val)
		}
	}

	if len(colNames) == 0 {
		return ""
	}
	return fmt.Sprintf("INSERT INTO `%s`.`%s`(%s) VALUES(%s);", schema, table,
		strings.Join(colNames, ","), strings.Join(values, ","))
}

// splitWhereConditions splits a WHERE clause by AND, respecting parentheses and quoted strings.
func splitWhereConditions(where string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false
	escaped := false

	for i := 0; i < len(where); i++ {
		c := where[i]
		if escaped {
			current.WriteByte(c)
			escaped = false
			continue
		}
		if c == '\\' {
			escaped = true
			current.WriteByte(c)
			continue
		}
		if c == '\'' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
			current.WriteByte(c)
			continue
		}
		if c == '"' && !inSingleQuote {
			inDoubleQuote = !inDoubleQuote
			current.WriteByte(c)
			continue
		}
		if inSingleQuote || inDoubleQuote {
			current.WriteByte(c)
			continue
		}
		if c == '(' {
			depth++
			current.WriteByte(c)
			continue
		}
		if c == ')' {
			depth--
			current.WriteByte(c)
			continue
		}

		if depth == 0 && c == ' ' && i+3 <= len(where) {
			upper := strings.ToUpper(where[i:])
			if strings.HasPrefix(upper, " AND ") {
				trimmed := strings.TrimSpace(current.String())
				if trimmed != "" {
					parts = append(parts, trimmed)
				}
				current.Reset()
				i += 4 // skip " AND"
				continue
			}
		}
		current.WriteByte(c)
	}
	trimmed := strings.TrimSpace(current.String())
	if trimmed != "" {
		parts = append(parts, trimmed)
	}
	return parts
}

// rollbackInsertToDelete converts an INSERT fix SQL into a rollback DELETE statement.
// It parses the INSERT INTO schema.table(col1,col2,...) VALUES(val1,val2,...) format
// and generates a DELETE using only the PK columns (from indexColumns) in the WHERE clause.
func rollbackInsertToDelete(insertSQL, schema, table string, indexColumns []string) string {
	colStart := strings.Index(insertSQL, "(")
	valIdx := strings.Index(strings.ToUpper(insertSQL), "VALUES")
	if colStart < 0 || valIdx < 0 {
		return ""
	}
	colPart := insertSQL[colStart+1 : strings.Index(insertSQL, ")")]
	valPart := insertSQL[valIdx+6:]
	valStart := strings.Index(valPart, "(")
	valEnd := strings.LastIndex(valPart, ")")
	if valStart < 0 || valEnd < 0 {
		return ""
	}
	valPart = valPart[valStart+1 : valEnd]

	colNames := splitRespectingQuotes(colPart, ',')
	values := splitRespectingQuotes(valPart, ',')
	if len(colNames) != len(values) {
		return ""
	}

	colValMap := make(map[string]string, len(colNames))
	for i, col := range colNames {
		col = strings.TrimSpace(strings.Trim(col, "`"))
		colValMap[col] = strings.TrimSpace(values[i])
	}

	var conditions []string
	for _, pkCol := range indexColumns {
		val, ok := colValMap[pkCol]
		if !ok {
			continue
		}
		if val == "NULL" {
			conditions = append(conditions, fmt.Sprintf("`%s` IS NULL", pkCol))
		} else {
			conditions = append(conditions, fmt.Sprintf("`%s` = %s", pkCol, val))
		}
	}

	if len(conditions) == 0 {
		return ""
	}
	return fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;", schema, table, strings.Join(conditions, " AND "))
}

// splitRespectingQuotes splits a string by sep, respecting single-quoted strings.
func splitRespectingQuotes(s string, sep rune) []string {
	var parts []string
	var current strings.Builder
	inQuote := false
	escaped := false

	for _, c := range s {
		if escaped {
			current.WriteRune(c)
			escaped = false
			continue
		}
		if c == '\\' {
			escaped = true
			current.WriteRune(c)
			continue
		}
		if c == '\'' {
			inQuote = !inQuote
			current.WriteRune(c)
			continue
		}
		if c == sep && !inQuote {
			parts = append(parts, current.String())
			current.Reset()
			continue
		}
		current.WriteRune(c)
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

// rollbackRowToInsert generates a rollback INSERT statement from raw row data.
// rowData is delimited by "/*go actions columnData*/".
// colData provides column metadata (columnName, dataType) for proper value formatting.
func rollbackRowToInsert(schema, table, rowData string, colData []map[string]string) string {
	if rowData == "" || len(colData) == 0 {
		return ""
	}
	rowParts := strings.Split(rowData, "/*go actions columnData*/")

	var colNames []string
	var values []string
	for i, col := range colData {
		colName, ok := col["columnName"]
		if !ok || colName == "" {
			continue
		}
		var rawVal string
		if i < len(rowParts) {
			rawVal = rowParts[i]
		}
		colNames = append(colNames, fmt.Sprintf("`%s`", colName))
		values = append(values, mysql.FormatMySQLInsertLiteral(rawVal, col["dataType"]))
	}

	if len(colNames) == 0 {
		return ""
	}
	return fmt.Sprintf("INSERT INTO `%s`.`%s`(%s) VALUES(%s);", schema, table,
		strings.Join(colNames, ","), strings.Join(values, ","))
}

// rollbackRowToDelete generates a rollback DELETE statement from raw row data using only PK columns.
// indexColumns specifies which columns to use in the WHERE clause.
func rollbackRowToDelete(schema, table, rowData string, colData []map[string]string, indexColumns []string) string {
	if rowData == "" || len(colData) == 0 || len(indexColumns) == 0 {
		return ""
	}
	rowParts := strings.Split(rowData, "/*go actions columnData*/")

	colValMap := make(map[string]string, len(colData))
	for i, col := range colData {
		colName, ok := col["columnName"]
		if !ok || colName == "" {
			continue
		}
		var rawVal string
		if i < len(rowParts) {
			rawVal = rowParts[i]
		}
		colValMap[colName] = rawVal
	}

	var conditions []string
	for _, pkCol := range indexColumns {
		rawVal, ok := colValMap[pkCol]
		if !ok {
			continue
		}
		if rawVal == "<nil>" {
			conditions = append(conditions, fmt.Sprintf("`%s` IS NULL", pkCol))
		} else if rawVal == "<entry>" {
			conditions = append(conditions, fmt.Sprintf("`%s` = ''", pkCol))
		} else {
			conditions = append(conditions, fmt.Sprintf("`%s` = '%s'", pkCol, mysql.EscapeSQLString(rawVal)))
		}
	}

	if len(conditions) == 0 {
		return ""
	}
	return fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;", schema, table, strings.Join(conditions, " AND "))
}

// sendRollback safely sends a rollback SQL to the rollCC channel if it is non-nil.
func sendRollback(rollCC chanString, sql string) {
	if rollCC != nil && sql != "" {
		rollCC <- sql
	}
}
