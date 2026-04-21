package actions

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	mysql "gt-checksum/MySQL"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
)

var routineMetadataCommentPattern = regexp.MustCompile(`/\*GT_CHECKSUM_METADATA:(.*?)\*/`)
var routineInlineCommentPattern = regexp.MustCompile(`--.*?\n|/\*[\s\S]*?\*/`)
var routineWhitespacePattern = regexp.MustCompile(`\s+`)
var routineCharsetCollationClausePattern = regexp.MustCompile(`(?i)CHARSET\s+([a-zA-Z0-9_]+)(?:\s+COLLATE\s+([a-zA-Z0-9_]+))?`)
var standaloneCollatePattern = regexp.MustCompile(`(?i)\bCOLLATE\s+([a-zA-Z0-9_]+)`)
var intDisplayWidthPattern = regexp.MustCompile(`(?i)\b((?:tiny|small|medium|big)?int)\(\d+\)`)

var routineHeaderIdentifierPattern = regexp.MustCompile("(?i)(CREATE\\s+(?:DEFINER\\s*=\\s*[^\\s]+\\s+)?(?:PROCEDURE|FUNCTION)\\s+)(`[^`]*`)")
var routineDefinerPattern = regexp.MustCompile(`CREATE\s+DEFINER\s*=\s*['"]?([^'"]*)['"]?@['"]?([^'"]*)['"]?`)
var routineSecurityPattern = regexp.MustCompile(`SQL\s+SECURITY\s+(\w+)`)
var routineCharsetPattern = regexp.MustCompile(`CHARACTER_SET_CLIENT\s*=\s*(\w+)`)
var routineCollationPattern = regexp.MustCompile(`COLLATION_CONNECTION\s*=\s*(\w+)`)
var routineDatabaseCollationPattern = regexp.MustCompile(`DATABASE\s+COLLATION\s*=\s*(\w+)`)

func normalizeStoredProcBody(body string) string {
	if body == "" {
		return ""
	}

	// 记录原始内容，用于调试
	originalBody := body

	// 保存GT_CHECKSUM_METADATA注释
	// 暂时移除元数据注释，以便不影响其他处理
	body = routineMetadataCommentPattern.ReplaceAllString(body, "")

	// 移除注释
	// 这里简化处理，实际可能需要更复杂的正则表达式
	body = routineInlineCommentPattern.ReplaceAllString(body, " ")

	// 规范化空白字符
	body = routineWhitespacePattern.ReplaceAllString(body, " ")

	// 移除开头和结尾的空格
	body = strings.TrimSpace(body)

	// 注意：不再规范化算术表达式，因为这会导致功能性差异被忽略
	// 例如，n1 + n2 和 n1 + n2*2 应该被视为不同的表达式

	// 如果规范化后的内容与原始内容有显著差异，记录日志
	if len(originalBody) > 0 && float64(len(body))/float64(len(originalBody)) < 0.5 {
		global.Wlog.Warn(fmt.Sprintf("Significant difference after normalization. Original length: %d, Normalized length: %d", len(originalBody), len(body)))
	}

	return body
}

// extractMetadataFromProcedure 从存储过程定义中提取元数据
func extractMetadataFromProcedure(procDef string) map[string]string {
	metadata := make(map[string]string)

	// 查找GT_CHECKSUM_METADATA注释
	metadataMatches := routineMetadataCommentPattern.FindStringSubmatch(procDef)

	if len(metadataMatches) > 1 {
		// 解析JSON格式的元数据
		jsonStr := metadataMatches[1]
		var metadataMap map[string]interface{}

		// 尝试解析JSON
		err := json.Unmarshal([]byte(jsonStr), &metadataMap)
		if err == nil {
			// 将解析后的元数据添加到结果映射中
			for key, value := range metadataMap {
				metadata[strings.ToUpper(key)] = fmt.Sprintf("%v", value)
			}
		}
	}

	// 提取DEFINER信息
	definerMatches := routineDefinerPattern.FindStringSubmatch(procDef)
	if len(definerMatches) > 2 {
		metadata["DEFINER"] = fmt.Sprintf("%s@%s", definerMatches[1], definerMatches[2])
	}

	// 提取SQL_MODE
	sqlModeMatches := routineSecurityPattern.FindStringSubmatch(procDef)
	if len(sqlModeMatches) > 1 {
		metadata["SQL_MODE"] = sqlModeMatches[1]
	}

	// 提取CHARACTER_SET_CLIENT
	charsetMatches := routineCharsetPattern.FindStringSubmatch(procDef)
	if len(charsetMatches) > 1 {
		metadata["CHARACTER_SET_CLIENT"] = charsetMatches[1]
	}

	// 提取COLLATION_CONNECTION
	collationMatches := routineCollationPattern.FindStringSubmatch(procDef)
	if len(collationMatches) > 1 {
		metadata["COLLATION_CONNECTION"] = collationMatches[1]
	}

	// 提取DATABASE_COLLATION
	dbCollationMatches := routineDatabaseCollationPattern.FindStringSubmatch(procDef)
	if len(dbCollationMatches) > 1 {
		metadata["DATABASE_COLLATION"] = dbCollationMatches[1]
	}

	return metadata
}

func normalizeRoutineDefinitionForCompare(definition string) string {
	normalized := strings.TrimSpace(definition)
	if normalized == "" {
		return ""
	}

	// Routine definitions collected from INFORMATION_SCHEMA may embed
	// environment metadata comments that differ between MariaDB and MySQL
	// while the executable body stays the same. Those metadata blobs should
	// not participate in semantic comparison.
	for {
		idx := strings.Index(normalized, "/*GT_CHECKSUM_METADATA:")
		if idx == -1 {
			break
		}
		endIdx := strings.Index(normalized[idx:], "*/")
		if endIdx == -1 {
			break
		}
		normalized = normalized[:idx] + normalized[idx+endIdx+2:]
	}

	// MySQL 8.0.17+ drops integer display widths from INFORMATION_SCHEMA
	// (e.g. int(11) → int, bigint(20) → bigint). Strip them so cross-version
	// comparisons don't produce false positives.
	normalized = intDisplayWidthPattern.ReplaceAllString(normalized, "$1")

	// 仅对 routine 标识符（如函数/过程名）做大小写归一，不对整个 definition 做 ToLower。
	// 这样可以保留函数体中字符串字面量的原始大小写（如 'Children' vs 'children'），
	// 避免吞掉真实的业务逻辑差异。
	normalized = routineHeaderIdentifierPattern.ReplaceAllStringFunc(normalized, func(m string) string {
		return strings.ToLower(m)
	})

	return strings.Join(strings.Fields(normalized), "")
}

func normalizeRoutineCreateSQLForCompare(createSQL string) string {
	normalized := strings.TrimSpace(createSQL)
	if normalized == "" {
		return ""
	}
	normalized = routineWhitespacePattern.ReplaceAllString(normalized, " ")
	return normalized
}

// mapMariaDBCollationInRoutineSQL 将 routine/trigger 定义中的 MariaDB 特有 collation
// 替换为 MySQL 等价物。处理两种形式：
//   - CHARSET utf8mb4 COLLATE utf8mb4_uca1400_ai_ci → CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci
//   - 独立的 COLLATE utf8mb4_uca1400_ai_ci → COLLATE utf8mb4_0900_ai_ci
func mapMariaDBCollationInRoutineSQL(createSQL string) string {
	if createSQL == "" {
		return createSQL
	}
	// 先处理 CHARSET ... COLLATE ... 组合形式
	result := routineCharsetCollationClausePattern.ReplaceAllStringFunc(createSQL, func(match string) string {
		parts := routineCharsetCollationClausePattern.FindStringSubmatch(match)
		if len(parts) < 3 || strings.TrimSpace(parts[2]) == "" {
			return match
		}
		collation := strings.TrimSpace(parts[2])
		if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(collation); ok {
			charset := strings.TrimSpace(parts[1])
			return fmt.Sprintf("CHARSET %s COLLATE %s", charset, mapped)
		}
		return match
	})
	// 再处理独立的 COLLATE 子句（不带 CHARSET 前缀的情况，如 DECLARE 变量声明中）
	result = standaloneCollatePattern.ReplaceAllStringFunc(result, func(match string) string {
		parts := standaloneCollatePattern.FindStringSubmatch(match)
		if len(parts) < 2 {
			return match
		}
		collation := strings.TrimSpace(parts[1])
		if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(collation); ok {
			return "COLLATE " + mapped
		}
		return match
	})
	return result
}

func normalizeRoutineCreateSQLForCompareWithCatalog(createSQL string, infos ...global.MySQLVersionInfo) string {
	normalized := normalizeRoutineCreateSQLForCompare(createSQL)
	if normalized == "" {
		return ""
	}

	// 收集所有平台的默认 collation 映射。在跨平台对比（如 MariaDB→MySQL）中，
	// 传入双方的版本信息可同时 strip 两端的平台默认 collation，避免修复后
	// 目标端显式带上源端默认 collation 而源端隐式省略导致的不可收敛假差异。
	mergedDefaults := make(map[string]map[string]bool) // charset → set of default collations
	for _, info := range infos {
		catalog := schemacompat.BuildSchemaFeatureCatalog(info)
		for charset, defCol := range catalog.DefaultCollationByCharset {
			lc := strings.ToLower(charset)
			if mergedDefaults[lc] == nil {
				mergedDefaults[lc] = make(map[string]bool)
			}
			mergedDefaults[lc][strings.ToLower(strings.TrimSpace(defCol))] = true
		}
	}

	return routineCharsetCollationClausePattern.ReplaceAllStringFunc(normalized, func(match string) string {
		parts := routineCharsetCollationClausePattern.FindStringSubmatch(match)
		if len(parts) < 2 {
			return match
		}

		charset := strings.ToLower(strings.TrimSpace(parts[1]))
		collation := ""
		if len(parts) > 2 {
			collation = strings.ToLower(strings.TrimSpace(parts[2]))
		}
		// 当 collation 等于任一平台该 charset 的默认值时，统一移除 COLLATE 子句。
		// 这样两端各自的平台默认 collation（如 MariaDB 的 utf8mb4_general_ci
		// 和 MySQL 8.0 的 utf8mb4_0900_ai_ci）会被归一化为同一形式，
		// 避免因平台默认值不同导致不可修复的假差异。
		if collation != "" {
			if defaults, ok := mergedDefaults[charset]; ok {
				if defaults[collation] {
					collation = ""
				}
			}
		}
		if collation == "" {
			return fmt.Sprintf("CHARSET %s", charset)
		}
		return fmt.Sprintf("CHARSET %s COLLATE %s", charset, collation)
	})
}

// getDisplayTableName 返回表的显示名称，包含映射关系信息
// 如果存在映射关系，返回格式为 "sourceSchema.table:destSchema.table"

func (stcls *schemaTable) shouldCompareRoutineMetadata() bool {
	src := stcls.sourceVersionInfo()
	dst := stcls.destVersionInfo()

	if strings.TrimSpace(src.Raw) == "" || strings.TrimSpace(dst.Raw) == "" {
		return stcls.isMySQLToMySQL()
	}

	if dst.Flavor == global.DatabaseFlavorMariaDB {
		return src.Flavor == global.DatabaseFlavorMariaDB
	}

	if dst.Flavor != global.DatabaseFlavorMySQL {
		return false
	}

	switch src.Flavor {
	case global.DatabaseFlavorMySQL:
		return dst.Flavor == global.DatabaseFlavorMySQL
	case global.DatabaseFlavorMariaDB:
		return dst.Series == "8.0" || dst.Series == "8.4"
	default:
		return false
	}
}

func loadMySQLRoutineComments(db *sql.DB, schema, routineType string, logThreadSeq int64) map[string]string {
	result := make(map[string]string)
	rows, err := db.Query(
		`SELECT ROUTINE_NAME, ROUTINE_COMMENT FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = ?`,
		schema, strings.ToUpper(strings.TrimSpace(routineType)),
	)
	if err != nil {
		global.Wlog.Warn(fmt.Sprintf("(%d) [loadMySQLRoutineComments] failed to query %s.%s comments: %v", logThreadSeq, schema, routineType, err))
		return result
	}
	defer rows.Close()

	for rows.Next() {
		var routineName string
		var routineComment sql.NullString
		if err := rows.Scan(&routineName, &routineComment); err != nil {
			global.Wlog.Warn(fmt.Sprintf("(%d) [loadMySQLRoutineComments] scan failed for %s.%s: %v", logThreadSeq, schema, routineType, err))
			continue
		}
		comment := ""
		if routineComment.Valid {
			comment = normalizeMetadataComment(routineComment.String)
		}
		result[strings.ToUpper(routineName)] = comment
	}
	return result
}

func buildMySQLRoutineCommentFixSQL(destSchema, name, routineType, comment string) string {
	escapedComment := escapeMySQLCommentLiteral(normalizeMetadataComment(comment))
	if strings.EqualFold(routineType, "PROCEDURE") {
		return fmt.Sprintf("ALTER PROCEDURE `%s`.`%s` COMMENT '%s';", destSchema, name, escapedComment)
	}
	return fmt.Sprintf("ALTER FUNCTION `%s`.`%s` COMMENT '%s';", destSchema, name, escapedComment)
}

func shouldRecreateRoutineForCommentDiff(sourceComment string) bool {
	return normalizeMetadataComment(sourceComment) == ""
}

func (stcls *schemaTable) ProcRet(dtabS []string, logThreadSeq, logThreadSeq2 int64) ([]Pod, error) {
	// 备份现场
	prevAggregate := stcls.aggregate
	prevBuffer := stcls.podsBuffer

	// 使用独立缓冲并开启聚合
	stcls.aggregate = true
	stcls.podsBuffer = nil

	// 复用原逻辑
	stcls.Proc(dtabS, logThreadSeq, logThreadSeq2)

	// 拷贝结果
	var res []Pod
	if len(stcls.podsBuffer) > 0 {
		res = make([]Pod, len(stcls.podsBuffer))
		copy(res, stcls.podsBuffer)
	}

	// 恢复现场
	stcls.podsBuffer = prevBuffer
	stcls.aggregate = prevAggregate

	return res, nil
}

/*
最小入侵新增：以返回值形式获取 Func 结果
- 通过临时开启 aggregate 模式，复用现有 Func 逻辑来采集 pods
- 调用结束后恢复原 aggregate 与 podsBuffer 状态
*/
func (stcls *schemaTable) FuncRet(dtabS []string, logThreadSeq, logThreadSeq2 int64) ([]Pod, error) {
	// 备份现场
	prevAggregate := stcls.aggregate
	prevBuffer := stcls.podsBuffer

	// 使用独立缓冲并开启聚合
	stcls.aggregate = true
	stcls.podsBuffer = nil

	// 复用原逻辑
	stcls.Func(dtabS, logThreadSeq, logThreadSeq2)

	// 拷贝结果
	var res []Pod
	if len(stcls.podsBuffer) > 0 {
		res = make([]Pod, len(stcls.podsBuffer))
		copy(res, stcls.podsBuffer)
	}

	// 恢复现场
	stcls.podsBuffer = prevBuffer
	stcls.aggregate = prevAggregate

	return res, nil
}

/*
最小入侵新增：统一入口，先后调用 Proc 与 Func，最后合并输出
- 结果追加通过 appendPod 实现，兼容外部是否启用 aggregate
*/
func (stcls *schemaTable) ProcAndFunc(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	procPods, _ := stcls.ProcRet(dtabS, logThreadSeq, logThreadSeq2)
	funcPods, _ := stcls.FuncRet(dtabS, logThreadSeq, logThreadSeq2)

	// 合并并输出
	for _, p := range procPods {
		stcls.appendPod(p)
	}
	for _, p := range funcPods {
		stcls.appendPod(p)
	}
}

/*
Routine: unified comparison for PROCEDURE and FUNCTION.
- routineType: "", "PROCEDURE", or "FUNCTION"
- Prefer tc.Query().Routine(); if it fails, fallback to old Proc/Func paths.
- Use appendPod to emit pods to buffer or measuredDataPods per aggregate flag.
*/
func showCreateRoutineOnce(db *sql.DB, schema, name, routineType string) (string, error) {
	var query string
	if strings.EqualFold(routineType, "PROCEDURE") {
		query = fmt.Sprintf("SHOW CREATE PROCEDURE `%s`.`%s`", schema, name)
	} else {
		query = fmt.Sprintf("SHOW CREATE FUNCTION `%s`.`%s`", schema, name)
	}

	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if !rows.Next() {
		return "", fmt.Errorf("no SHOW CREATE result for %s.%s %s", schema, name, routineType)
	}

	// 使用 RawBytes 动态接收所有列
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	if err := rows.Scan(scanArgs...); err != nil {
		return "", err
	}

	// 找到正确的 Create 列名
	targetCol := ""
	if strings.EqualFold(routineType, "PROCEDURE") {
		targetCol = "Create Procedure"
	} else {
		targetCol = "Create Function"
	}

	var createSQL string
	for i, col := range cols {
		if strings.EqualFold(col, targetCol) {
			createSQL = string(values[i])
			break
		}
	}

	if strings.TrimSpace(createSQL) == "" {
		return "", fmt.Errorf("SHOW CREATE did not return %q column; got: %v", targetCol, cols)
	}
	return createSQL, nil
}

func showCreateRoutine(db *sql.DB, schema, name, routineType string) (string, error) {
	candidates := []string{name}
	lowerName := strings.ToLower(name)
	upperName := strings.ToUpper(name)
	if lowerName != name {
		candidates = append(candidates, lowerName)
	}
	if upperName != name && upperName != lowerName {
		candidates = append(candidates, upperName)
	}

	var lastErr error
	for _, candidate := range candidates {
		createSQL, err := showCreateRoutineOnce(db, schema, candidate, routineType)
		if err == nil && strings.TrimSpace(createSQL) != "" {
			return createSQL, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("empty SHOW CREATE result for %s.%s %s", schema, candidate, routineType)
		}
	}
	return "", fmt.Errorf("SHOW CREATE failed for %s.%s %s, candidates=%v, lastErr=%v", schema, name, routineType, candidates, lastErr)
}

// queryRoutineCharsetMetadata 从 INFORMATION_SCHEMA.ROUTINES 查询 routine 的 charset session 元数据
func queryRoutineCharsetMetadata(db *sql.DB, schema, name, routineType string) (charsetClient, collationConn, dbCollation string) {
	row := db.QueryRow(
		`SELECT CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION
		   FROM INFORMATION_SCHEMA.ROUTINES
		  WHERE ROUTINE_SCHEMA = ? AND ROUTINE_NAME = ? AND ROUTINE_TYPE = ?`,
		schema, name, strings.ToUpper(routineType),
	)
	var cs, col, dbCol sql.NullString
	if err := row.Scan(&cs, &col, &dbCol); err != nil {
		global.Wlog.Warn(fmt.Sprintf("queryRoutineCharsetMetadata failed for %s.%s %s: %v", schema, name, routineType, err))
		return "", "", ""
	}
	return strings.TrimSpace(cs.String), strings.TrimSpace(col.String), strings.TrimSpace(dbCol.String)
}

// buildRoutineCharsetSetStatements 生成 routine fix SQL 需要的 charset session 变量 SET 语句
func buildRoutineCharsetSetStatements(csClient, colConn, dbCollation string, isMariaDBToMySQL bool) []string {
	if isMariaDBToMySQL {
		if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(colConn); ok {
			colConn = mapped
		}
		if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(dbCollation); ok {
			dbCollation = mapped
		}
	}

	stmts := make([]string, 0, 3)
	if csClient != "" {
		stmts = append(stmts, fmt.Sprintf("SET character_set_client = %s;", csClient))
	}
	if colConn != "" {
		stmts = append(stmts, fmt.Sprintf("SET collation_connection = %s;", colConn))
	}
	if dbCollation != "" {
		stmts = append(stmts, fmt.Sprintf("SET collation_database = %s;", dbCollation))
	}
	return stmts
}

// isCharsetMetadataCollationMapped 检查源端和目标端的 charset 会话元数据是否仅存在
// uca1400→0900 可映射的 collation 差异（MariaDB 11.5+ 默认 collation）。
// utf8mb4_general_ci 在 MySQL 8.0 中是完全支持的 collation，不属于映射范畴，
// 其与 utf8mb4_0900_ai_ci 的差异应视为真实差异并生成 fix SQL。
//
// 返回 true 当且仅当 CHARACTER_SET_CLIENT 一致、至少有一个 COLLATION 字段不同
// 且所有差异都可通过 MapMariaDBCollationToMySQL 映射。
func isCharsetMetadataCollationMapped(srcCSClient, srcColConn, srcDBCollation, dstCSClient, dstColConn, dstDBCollation string) bool {
	// CHARACTER_SET_CLIENT 不同则不是纯 collation 映射
	if !strings.EqualFold(strings.TrimSpace(srcCSClient), strings.TrimSpace(dstCSClient)) {
		return false
	}
	// 比较 COLLATION_CONNECTION —— DATABASE_COLLATION 是数据库级属性，
	// 在 MySQL 8.0 中无法按对象粒度修复，因此不纳入映射判断。
	src := strings.TrimSpace(srcColConn)
	dst := strings.TrimSpace(dstColConn)
	if strings.EqualFold(src, dst) {
		return false
	}
	if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(src); ok && strings.EqualFold(mapped, dst) {
		return true
	}
	return false
}

// hasCharsetMetadataCollationDiff 检查源端和目标端的 charset 会话元数据是否存在
// CHARACTER_SET_CLIENT 或 COLLATION_CONNECTION 差异。
// DATABASE_COLLATION 是数据库级属性，无法按对象粒度修复，不纳入判断。
func hasCharsetMetadataCollationDiff(srcCSClient, srcColConn, srcDBCollation, dstCSClient, dstColConn, dstDBCollation string) bool {
	if !strings.EqualFold(strings.TrimSpace(srcCSClient), strings.TrimSpace(dstCSClient)) {
		return true
	}
	return !strings.EqualFold(strings.TrimSpace(srcColConn), strings.TrimSpace(dstColConn))
}

func (stcls *schemaTable) normalizeRoutineObjectName(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}
	// Routine names are compared in a case-insensitive way to avoid
	// MariaDB/MySQL display-case drift (e.g. myAdd vs MYADD) causing
	// duplicated pseudo-diffs.
	return strings.ToUpper(trimmed)
}

func (stcls *schemaTable) normalizeRoutineObjectMap(items map[string]string) map[string]string {
	normalized := make(map[string]string, len(items))
	for key, value := range items {
		if key == "DEFINER" {
			if old, exists := normalized[key]; !exists || strings.TrimSpace(old) == "" {
				normalized[key] = value
			}
			continue
		}

		bodySuffix := ""
		baseKey := key
		if strings.HasSuffix(baseKey, "_BODY") {
			baseKey = strings.TrimSuffix(baseKey, "_BODY")
			bodySuffix = "_BODY"
		}

		normalizedName := stcls.normalizeRoutineObjectName(baseKey)
		if normalizedName == "" {
			continue
		}
		normalizedKey := normalizedName + bodySuffix
		if old, exists := normalized[normalizedKey]; exists {
			if strings.TrimSpace(old) == "" && strings.TrimSpace(value) != "" {
				normalized[normalizedKey] = value
			}
			continue
		}
		normalized[normalizedKey] = value
	}
	return normalized
}

func (stcls *schemaTable) Routine(dtabS []string, logThreadSeq, logThreadSeq2 int64, routineType string) {
	// 合并 Proc/Func 主体逻辑，统一解析与比对，统一输出字段 ProcName
	// 解析 dtabS，构建 schemaMap 与过滤映射
	schemaMap := make(map[string]int)
	procMap := make(map[string]string)
	funcMap := make(map[string]string)
	if stcls.caseSensitiveObjectName == "no" {
		// 统一转小写的辅助闭包
		lower := func(s string) string { return strings.ToLower(s) }
		_ = lower
	}

	for _, i := range dtabS {
		if strings.Contains(i, ":") {
			parts := strings.Split(i, ":")
			if len(parts) == 2 {
				sourceParts := strings.Split(parts[0], ".")
				if len(sourceParts) >= 1 {
					schema := sourceParts[0]
					if stcls.caseSensitiveObjectName == "no" {
						schema = strings.ToLower(schema)
					}
					schemaMap[schema] = 1
					// 提取名称
					if len(sourceParts) >= 2 && sourceParts[1] != "*" {
						name := stcls.normalizeRoutineObjectName(sourceParts[1])
						// 根据 routineType 放入对应过滤映射；为空则两者都放
						key := schema + "." + name
						if routineType == "" || strings.EqualFold(routineType, "PROCEDURE") {
							procMap[key] = name
						}
						if routineType == "" || strings.EqualFold(routineType, "FUNCTION") {
							funcMap[key] = name
						}
					}
				}
			}
		} else {
			parts := strings.Split(i, ".")
			if len(parts) >= 1 {
				schema := parts[0]
				if stcls.caseSensitiveObjectName == "no" {
					schema = strings.ToLower(schema)
				}
				schemaMap[schema] = 1
				if len(parts) >= 2 && parts[1] != "*" {
					name := stcls.normalizeRoutineObjectName(parts[1])
					key := schema + "." + name
					if routineType == "" || strings.EqualFold(routineType, "PROCEDURE") {
						procMap[key] = name
					}
					if routineType == "" || strings.EqualFold(routineType, "FUNCTION") {
						funcMap[key] = name
					}
				}
			}
		}
	}

	// 如果 schemaMap 为空但有默认 schema，则使用默认
	if len(schemaMap) == 0 && stcls.schema != "" {
		schema := stcls.schema
		if stcls.caseSensitiveObjectName == "no" {
			schema = strings.ToLower(schema)
		}
		schemaMap[schema] = 1
	}

	// 统一遍历 schema，分别处理 PROCEDURE 与 FUNCTION（按 routineType 过滤）
	for schema := range schemaMap {
		// PROCEDURE 处理
		if routineType == "" || strings.EqualFold(routineType, "PROCEDURE") {
			var (
				sourceProc, destProc map[string]string
				err                  error
				tmpM                 = make(map[string]int)
				c, d                 []string
				vlog                 string
				pods                 = Pod{Datafix: stcls.datafix, CheckObject: "Procedure", Schema: schema}
			)

			tc := dbExec.TableColumnNameStruct{
				Schema:                  schema,
				Drive:                   stcls.sourceDrive,
				CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
			}
			if sourceProc, err = tc.Query().Proc(stcls.sourceDB, logThreadSeq2); err != nil {
				vlog = fmt.Sprintf("(%d) Error querying source procedures: %v", logThreadSeq, err)
				global.Wlog.Error(vlog)
				// 不中断其他 schema 或 object 的检查
			}
			tc.Drive = stcls.destDrive
			if destProc, err = tc.Query().Proc(stcls.destDB, logThreadSeq2); err != nil {
				vlog = fmt.Sprintf("(%d) Error querying destination procedures: %v", logThreadSeq, err)
				global.Wlog.Error(vlog)
			}

			sourceProcComments := make(map[string]string)
			destProcComments := make(map[string]string)
			if stcls.shouldCompareRoutineMetadata() {
				sourceProcComments = loadMySQLRoutineComments(stcls.sourceDB, schema, "PROCEDURE", logThreadSeq)
				destProcComments = loadMySQLRoutineComments(stcls.destDB, schema, "PROCEDURE", logThreadSeq)
			}

			// 过滤或通配填充 procMap
			if len(procMap) > 0 {
				filteredSource := make(map[string]string)
				for k, v := range sourceProc {
					if k == "DEFINER" {
						filteredSource[k] = v
						continue
					}
					name := stcls.normalizeRoutineObjectName(k)
					key := schema + "." + name
					if _, ok := procMap[key]; ok {
						filteredSource[k] = v
						if bodyKey := k + "_BODY"; true {
							if _, ok := sourceProc[bodyKey]; ok {
								filteredSource[bodyKey] = sourceProc[bodyKey]
							}
						}
					}
				}
				sourceProc = filteredSource

				filteredDest := make(map[string]string)
				for k, v := range destProc {
					if k == "DEFINER" {
						filteredDest[k] = v
						continue
					}
					name := stcls.normalizeRoutineObjectName(k)
					key := schema + "." + name
					if _, ok := procMap[key]; ok {
						filteredDest[k] = v
						if bodyKey := k + "_BODY"; true {
							if _, ok := destProc[bodyKey]; ok {
								filteredDest[bodyKey] = destProc[bodyKey]
							}
						}
					}
				}
				destProc = filteredDest
			} else {
				for k := range sourceProc {
					if k == "DEFINER" || strings.HasSuffix(k, "_BODY") {
						continue
					}
					name := stcls.normalizeRoutineObjectName(k)
					procMap[schema+"."+name] = name
				}
				for k := range destProc {
					if k == "DEFINER" || strings.HasSuffix(k, "_BODY") {
						continue
					}
					name := stcls.normalizeRoutineObjectName(k)
					procMap[schema+"."+name] = name
				}
			}

			sourceProc = stcls.normalizeRoutineObjectMap(sourceProc)
			destProc = stcls.normalizeRoutineObjectMap(destProc)

			// 并集与比对
			if len(sourceProc) > 0 || len(destProc) > 0 {
				tmpM = make(map[string]int)
				for k := range sourceProc {
					if k == "DEFINER" || strings.HasSuffix(k, "_BODY") {
						continue
					}
					tmpM[k]++
				}
				for k := range destProc {
					if k == "DEFINER" || strings.HasSuffix(k, "_BODY") {
						continue
					}
					tmpM[k]++
				}

				for k, v := range tmpM {
					definitionDiff := false
					collationMappedOnly := false
					commentDiff := false
					definerDiff := false
					sourceComment := ""

					if v == 2 {
						// 优先比较显式过程体；如果当前采集路径没有单独的 BODY
						// 字段，则回退到归一化后的完整定义比较，并忽略环境元数据噪音。
						srcBody := normalizeStoredProcBody(sourceProc[k+"_BODY"])
						dstBody := normalizeStoredProcBody(destProc[k+"_BODY"])
						if srcBody == "" && dstBody == "" {
							srcDef := normalizeRoutineDefinitionForCompare(sourceProc[k])
							dstDef := normalizeRoutineDefinitionForCompare(destProc[k])
							if srcDef == "" && dstDef == "" {
								definitionDiff = true
							} else if srcDef != dstDef {
								definitionDiff = true
							}
						} else if srcBody != dstBody {
							definitionDiff = true
						}

						if definitionDiff && stcls.sourceVersionInfo().Flavor == global.DatabaseFlavorMariaDB && stcls.destVersionInfo().Flavor == global.DatabaseFlavorMySQL {
							sourceCreate, srcErr := showCreateRoutine(stcls.sourceDB, schema, k, "PROCEDURE")
							destCreate, dstErr := showCreateRoutine(stcls.destDB, schema, k, "PROCEDURE")
							if srcErr == nil && dstErr == nil {
								normalizedSourceCreate := normalizeRoutineCreateSQLForCompareWithCatalog(sourceCreate, stcls.sourceVersionInfo(), stcls.destVersionInfo())
								normalizedDestCreate := normalizeRoutineCreateSQLForCompareWithCatalog(destCreate, stcls.sourceVersionInfo(), stcls.destVersionInfo())
								// MariaDB→MySQL：将源端归一化后的 uca1400 collation 映射为 MySQL 等价物再比较
								normalizedSourceBeforeMapping := normalizedSourceCreate
								normalizedSourceCreate = mapMariaDBCollationInRoutineSQL(normalizedSourceCreate)
								if normalizedSourceCreate == normalizedDestCreate {
									global.Wlog.Debug(fmt.Sprintf("(%d) Procedure SHOW CREATE fallback matched %s.%s after normalization (collation-mapped)", logThreadSeq, schema, k))
									definitionDiff = false
									if normalizedSourceBeforeMapping != normalizedSourceCreate {
										collationMappedOnly = true
									}
								} else {
									global.Wlog.Debug(fmt.Sprintf("(%d) Procedure SHOW CREATE fallback still differs %s.%s: source=%q dest=%q", logThreadSeq, schema, k, normalizedSourceCreate, normalizedDestCreate))
								}
							} else {
								global.Wlog.Debug(fmt.Sprintf("(%d) Procedure SHOW CREATE fallback unavailable %s.%s: sourceErr=%v destErr=%v", logThreadSeq, schema, k, srcErr, dstErr))
							}
						}
					} else {
						// 仅一侧存在
						definitionDiff = true
					}

					if stcls.shouldCompareRoutineMetadata() {
						sourceComment = normalizeMetadataComment(sourceProcComments[strings.ToUpper(k)])
						destComment := normalizeMetadataComment(destProcComments[strings.ToUpper(k)])
						if sourceComment != destComment {
							commentDiff = true
							vlog = fmt.Sprintf("(%d) Procedure comment mismatch %s.%s: source=%q, dest=%q", logThreadSeq, schema, k, sourceComment, destComment)
							global.Wlog.Warn(vlog)
						}

						sourceDefiner := strings.TrimSpace(extractMetadataFromProcedure(sourceProc[k])["DEFINER"])
						destDefiner := strings.TrimSpace(extractMetadataFromProcedure(destProc[k])["DEFINER"])
						if sourceDefiner != destDefiner {
							definerDiff = true
							vlog = fmt.Sprintf("(%d) Procedure definer mismatch %s.%s: source=%q, dest=%q", logThreadSeq, schema, k, sourceDefiner, destDefiner)
							global.Wlog.Warn(vlog)
						}
					}

					// MariaDB→MySQL：当定义和其他属性均一致时，检查 charset 会话元数据的 collation 差异
					metadataCollationDiff := false
					if !definitionDiff && !commentDiff && !definerDiff && !collationMappedOnly && stcls.isMariaDBToMySQL() {
						srcCSClient, srcColConn, srcDBCollation := queryRoutineCharsetMetadata(stcls.sourceDB, schema, k, "PROCEDURE")
						dstCSClient, dstColConn, dstDBCollation := queryRoutineCharsetMetadata(stcls.destDB, schema, k, "PROCEDURE")
						if isCharsetMetadataCollationMapped(srcCSClient, srcColConn, srcDBCollation, dstCSClient, dstColConn, dstDBCollation) {
							collationMappedOnly = true
							global.Wlog.Debug(fmt.Sprintf("(%d) Procedure %s.%s charset metadata collation-mapped: uca1400→0900 drift (src=%s/%s dst=%s/%s)", logThreadSeq, schema, k, srcColConn, srcDBCollation, dstColConn, dstDBCollation))
						} else if hasCharsetMetadataCollationDiff(srcCSClient, srcColConn, srcDBCollation, dstCSClient, dstColConn, dstDBCollation) {
							metadataCollationDiff = true
							global.Wlog.Warn(fmt.Sprintf("(%d) Procedure %s.%s charset metadata collation mismatch requiring fix SQL (src=%s/%s dst=%s/%s)", logThreadSeq, schema, k, srcColConn, srcDBCollation, dstColConn, dstDBCollation))
						}
					}

					pods.ProcName = k
					if definitionDiff || commentDiff || definerDiff || metadataCollationDiff {
						pods.DIFFS = "yes"
						d = append(d, k)
					} else if collationMappedOnly {
						pods.DIFFS = global.SkipDiffsCollationMapped
						c = append(c, k)
						global.Wlog.Debug(fmt.Sprintf("(%d) Procedure %s.%s collation-mapped: only uca1400→0900 collation difference, no fix SQL generated", logThreadSeq, schema, k))
					} else {
						pods.DIFFS = "no"
						c = append(c, k)
					}
					stcls.appendPod(pods)

					// Generate and write fix SQL for PROCEDURE differences
					if pods.DIFFS == "yes" && pods.CheckObject == "Procedure" {
						// 确定目标schema
						destSchema := schema
						if mappedSchema, exists := stcls.tableMappings[schema]; exists {
							destSchema = mappedSchema
						}

						// When source comment is empty, ALTER ... COMMENT '' does not reliably
						// clear routine comments in MySQL. Recreate the routine instead.
						if commentDiff && !definitionDiff && !definerDiff && stcls.isMySQLToMySQL() {
							if !shouldRecreateRoutineForCommentDiff(sourceComment) {
								commentSQL := buildMySQLRoutineCommentFixSQL(destSchema, k, "PROCEDURE", sourceComment)
								global.Wlog.Warn(fmt.Sprintf("(%d) Generating PROCEDURE comment fix SQL: %s", logThreadSeq, commentSQL))
								origSchema, origTable, origObjType := stcls.schema, stcls.table, stcls.fixFileObjectType
								stcls.schema = schema
								stcls.table = k
								stcls.fixFileObjectType = "routine"
								if werr := stcls.writeFixSql([]string{commentSQL}, logThreadSeq); werr != nil {
									global.Wlog.Error(fmt.Sprintf("(%d) failed to write routine comment fix SQL for %s.%s: %v", logThreadSeq, schema, k, werr))
								}
								stcls.schema, stcls.table, stcls.fixFileObjectType = origSchema, origTable, origObjType
								continue
							}
							global.Wlog.Warn(fmt.Sprintf("(%d) PROCEDURE %s.%s source comment is empty, recreating routine instead of ALTER COMMENT", logThreadSeq, schema, k))
						}

						sourceDef, err := showCreateRoutine(stcls.sourceDB, schema, k, "PROCEDURE")
						if err != nil || len(strings.TrimSpace(sourceDef)) == 0 {
							global.Wlog.Warn(fmt.Sprintf("(%d) SHOW CREATE PROCEDURE unavailable for %s.%s: %v; fallback to INFORMATION_SCHEMA definition", logThreadSeq, schema, k, err))
							// 回退：使用之前采集到的定义
							if def, ok := sourceProc[k]; ok {
								sourceDef = def
							}
						}
						// MariaDB→MySQL：映射源端定义中的 MariaDB 特有 collation
						if stcls.isMariaDBToMySQL() {
							sourceDef = mapMariaDBCollationInRoutineSQL(sourceDef)
						}
						sqls := mysql.GenerateRoutineFixSQL(schema, destSchema, k, "PROCEDURE", sourceDef)
						// 查询 charset session 元数据并插入 SET 语句
						csClient, colConn, dbCollation := queryRoutineCharsetMetadata(stcls.sourceDB, schema, k, "PROCEDURE")
						if csClient != "" {
							charsetStmts := buildRoutineCharsetSetStatements(csClient, colConn, dbCollation, stcls.isMariaDBToMySQL())
							if len(charsetStmts) > 0 {
								enriched := make([]string, 0, len(charsetStmts)+len(sqls))
								enriched = append(enriched, charsetStmts...)
								enriched = append(enriched, sqls...)
								sqls = enriched
							}
						}
						normalizedSqls := make([]string, 0, len(sqls))
						for _, s := range sqls {
							ts := strings.TrimSpace(s)
							if ts == "" {
								continue
							}
							if strings.HasPrefix(strings.ToUpper(ts), "DROP ") && !strings.HasSuffix(ts, ";") {
								ts += ";"
							}
							normalizedSqls = append(normalizedSqls, ts)
						}
						out := make([]string, 0, len(normalizedSqls)+2)
						out = append(out, "DELIMITER $$")
						for _, stmt := range normalizedSqls {
							out = append(out, stmt+"\n$$")
						}
						out = append(out, "DELIMITER ;")
						origSchema, origTable, origObjType := stcls.schema, stcls.table, stcls.fixFileObjectType
						stcls.schema = schema
						stcls.table = k
						stcls.fixFileObjectType = "routine"
						if werr := stcls.writeFixSql(out, logThreadSeq); werr != nil {
							global.Wlog.Error(fmt.Sprintf("(%d) failed to write procedure fix SQL for %s.%s: %v", logThreadSeq, schema, k, werr))
						}
						stcls.schema, stcls.table, stcls.fixFileObjectType = origSchema, origTable, origObjType
					}
				}
			}
			// 汇总日志
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Stored Procedure. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
		}

		// FUNCTION 处理
		if routineType == "" || strings.EqualFold(routineType, "FUNCTION") {
			var (
				sourceFunc, destFunc map[string]string
				err                  error
				tmpM                 = make(map[string]int)
				c, d                 []string
				vlog                 string
				pods                 = Pod{Datafix: stcls.datafix, CheckObject: "Function", Schema: schema}
			)

			tc := dbExec.TableColumnNameStruct{
				Schema:                  schema,
				Drive:                   stcls.sourceDrive,
				CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
			}
			if sourceFunc, err = tc.Query().Func(stcls.sourceDB, logThreadSeq2); err != nil {
				vlog = fmt.Sprintf("(%d) Error querying source functions: %v", logThreadSeq, err)
				global.Wlog.Error(vlog)
			}
			tc.Drive = stcls.destDrive
			if destFunc, err = tc.Query().Func(stcls.destDB, logThreadSeq2); err != nil {
				vlog = fmt.Sprintf("(%d) Error querying destination functions: %v", logThreadSeq, err)
				global.Wlog.Error(vlog)
			}

			sourceFuncComments := make(map[string]string)
			destFuncComments := make(map[string]string)
			if stcls.shouldCompareRoutineMetadata() {
				sourceFuncComments = loadMySQLRoutineComments(stcls.sourceDB, schema, "FUNCTION", logThreadSeq)
				destFuncComments = loadMySQLRoutineComments(stcls.destDB, schema, "FUNCTION", logThreadSeq)
			}

			// 过滤或通配填充 funcMap
			if len(funcMap) > 0 {
				filteredSource := make(map[string]string)
				for k, v := range sourceFunc {
					name := stcls.normalizeRoutineObjectName(k)
					key := schema + "." + name
					if _, ok := funcMap[key]; ok {
						filteredSource[k] = v
					}
				}
				sourceFunc = filteredSource

				filteredDest := make(map[string]string)
				for k, v := range destFunc {
					name := stcls.normalizeRoutineObjectName(k)
					key := schema + "." + name
					if _, ok := funcMap[key]; ok {
						filteredDest[k] = v
					}
				}
				destFunc = filteredDest
			} else {
				for k := range sourceFunc {
					name := stcls.normalizeRoutineObjectName(k)
					funcMap[schema+"."+name] = name
				}
				for k := range destFunc {
					name := stcls.normalizeRoutineObjectName(k)
					funcMap[schema+"."+name] = name
				}
			}

			sourceFunc = stcls.normalizeRoutineObjectMap(sourceFunc)
			destFunc = stcls.normalizeRoutineObjectMap(destFunc)

			// 并集与比对
			if len(sourceFunc) > 0 || len(destFunc) > 0 {
				tmpM = make(map[string]int)
				for k := range sourceFunc {
					tmpM[k]++
				}
				for k := range destFunc {
					tmpM[k]++
				}
				for k, v := range tmpM {
					definitionDiff := false
					collationMappedOnly := false
					commentDiff := false
					definerDiff := false
					sourceComment := ""

					if v == 2 {
						cleanSourceFunc := normalizeRoutineDefinitionForCompare(sourceFunc[k])
						cleanDestFunc := normalizeRoutineDefinitionForCompare(destFunc[k])
						if cleanSourceFunc != cleanDestFunc {
							definitionDiff = true
							global.Wlog.Debug(fmt.Sprintf("(%d) Function definition diff %s.%s:\n  source=%q\n  dest  =%q", logThreadSeq, schema, k, cleanSourceFunc, cleanDestFunc))
						}

						if definitionDiff && stcls.sourceVersionInfo().Flavor == global.DatabaseFlavorMariaDB && stcls.destVersionInfo().Flavor == global.DatabaseFlavorMySQL {
							sourceCreate, srcErr := showCreateRoutine(stcls.sourceDB, schema, k, "FUNCTION")
							destCreate, dstErr := showCreateRoutine(stcls.destDB, schema, k, "FUNCTION")
							if srcErr == nil && dstErr == nil {
								normalizedSourceCreate := normalizeRoutineCreateSQLForCompareWithCatalog(sourceCreate, stcls.sourceVersionInfo(), stcls.destVersionInfo())
								normalizedDestCreate := normalizeRoutineCreateSQLForCompareWithCatalog(destCreate, stcls.sourceVersionInfo(), stcls.destVersionInfo())
								// MariaDB→MySQL：将源端归一化后的 uca1400 collation 映射为 MySQL 等价物再比较
								normalizedSourceBeforeMapping := normalizedSourceCreate
								normalizedSourceCreate = mapMariaDBCollationInRoutineSQL(normalizedSourceCreate)
								if normalizedSourceCreate == normalizedDestCreate {
									global.Wlog.Debug(fmt.Sprintf("(%d) Function SHOW CREATE fallback matched %s.%s after normalization (collation-mapped)", logThreadSeq, schema, k))
									definitionDiff = false
									if normalizedSourceBeforeMapping != normalizedSourceCreate {
										collationMappedOnly = true
									}
								} else {
									global.Wlog.Debug(fmt.Sprintf("(%d) Function SHOW CREATE fallback still differs %s.%s: source=%q dest=%q", logThreadSeq, schema, k, normalizedSourceCreate, normalizedDestCreate))
								}
							} else {
								global.Wlog.Debug(fmt.Sprintf("(%d) Function SHOW CREATE fallback unavailable %s.%s: sourceErr=%v destErr=%v", logThreadSeq, schema, k, srcErr, dstErr))
							}
						}
					} else {
						definitionDiff = true
					}

					if stcls.shouldCompareRoutineMetadata() {
						sourceComment = normalizeMetadataComment(sourceFuncComments[strings.ToUpper(k)])
						destComment := normalizeMetadataComment(destFuncComments[strings.ToUpper(k)])
						if sourceComment != destComment {
							commentDiff = true
							vlog = fmt.Sprintf("(%d) Function comment mismatch %s.%s: source=%q, dest=%q", logThreadSeq, schema, k, sourceComment, destComment)
							global.Wlog.Warn(vlog)
						}

						sourceDefiner := strings.TrimSpace(extractMetadataFromProcedure(sourceFunc[k])["DEFINER"])
						destDefiner := strings.TrimSpace(extractMetadataFromProcedure(destFunc[k])["DEFINER"])
						if sourceDefiner != destDefiner {
							definerDiff = true
							vlog = fmt.Sprintf("(%d) Function definer mismatch %s.%s: source=%q, dest=%q", logThreadSeq, schema, k, sourceDefiner, destDefiner)
							global.Wlog.Warn(vlog)
						}
					}

					// MariaDB→MySQL：当定义和其他属性均一致时，检查 charset 会话元数据的 collation 差异
					metadataCollationDiff := false
					if !definitionDiff && !commentDiff && !definerDiff && !collationMappedOnly && stcls.isMariaDBToMySQL() {
						srcCSClient, srcColConn, srcDBCollation := queryRoutineCharsetMetadata(stcls.sourceDB, schema, k, "FUNCTION")
						dstCSClient, dstColConn, dstDBCollation := queryRoutineCharsetMetadata(stcls.destDB, schema, k, "FUNCTION")
						if isCharsetMetadataCollationMapped(srcCSClient, srcColConn, srcDBCollation, dstCSClient, dstColConn, dstDBCollation) {
							collationMappedOnly = true
							global.Wlog.Debug(fmt.Sprintf("(%d) Function %s.%s charset metadata collation-mapped: uca1400→0900 drift (src=%s/%s dst=%s/%s)", logThreadSeq, schema, k, srcColConn, srcDBCollation, dstColConn, dstDBCollation))
						} else if hasCharsetMetadataCollationDiff(srcCSClient, srcColConn, srcDBCollation, dstCSClient, dstColConn, dstDBCollation) {
							metadataCollationDiff = true
							global.Wlog.Warn(fmt.Sprintf("(%d) Function %s.%s charset metadata collation mismatch requiring fix SQL (src=%s/%s dst=%s/%s)", logThreadSeq, schema, k, srcColConn, srcDBCollation, dstColConn, dstDBCollation))
						}
					}

					pods.ProcName = k
					if definitionDiff || commentDiff || definerDiff || metadataCollationDiff {
						pods.DIFFS = "yes"
						d = append(d, k)
					} else if collationMappedOnly {
						pods.DIFFS = global.SkipDiffsCollationMapped
						c = append(c, k)
						global.Wlog.Debug(fmt.Sprintf("(%d) Function %s.%s collation-mapped: only uca1400→0900 collation difference, no fix SQL generated", logThreadSeq, schema, k))
					} else {
						pods.DIFFS = "no"
						c = append(c, k)
					}
					stcls.appendPod(pods)

					// Generate and write fix SQL for FUNCTION differences
					if pods.DIFFS == "yes" && pods.CheckObject == "Function" {
						// 确定目标schema
						destSchema := schema
						if mappedSchema, exists := stcls.tableMappings[schema]; exists {
							destSchema = mappedSchema
						}

						// When source comment is empty, ALTER ... COMMENT '' does not reliably
						// clear routine comments in MySQL. Recreate the routine instead.
						if commentDiff && !definitionDiff && !definerDiff && stcls.isMySQLToMySQL() {
							if !shouldRecreateRoutineForCommentDiff(sourceComment) {
								commentSQL := buildMySQLRoutineCommentFixSQL(destSchema, k, "FUNCTION", sourceComment)
								global.Wlog.Warn(fmt.Sprintf("(%d) Generating FUNCTION comment fix SQL: %s", logThreadSeq, commentSQL))
								origSchema, origTable, origObjType := stcls.schema, stcls.table, stcls.fixFileObjectType
								stcls.schema = schema
								stcls.table = k
								stcls.fixFileObjectType = "routine"
								if werr := stcls.writeFixSql([]string{commentSQL}, logThreadSeq); werr != nil {
									global.Wlog.Error(fmt.Sprintf("(%d) failed to write routine comment fix SQL for %s.%s: %v", logThreadSeq, schema, k, werr))
								}
								stcls.schema, stcls.table, stcls.fixFileObjectType = origSchema, origTable, origObjType
								continue
							}
							global.Wlog.Warn(fmt.Sprintf("(%d) FUNCTION %s.%s source comment is empty, recreating routine instead of ALTER COMMENT", logThreadSeq, schema, k))
						}

						funcSource, err := showCreateRoutine(stcls.sourceDB, schema, k, "FUNCTION")
						if err != nil || len(strings.TrimSpace(funcSource)) == 0 {
							global.Wlog.Warn(fmt.Sprintf("(%d) SHOW CREATE FUNCTION unavailable for %s.%s: %v; fallback to INFORMATION_SCHEMA definition", logThreadSeq, schema, k, err))
							// 回退：使用之前采集到的定义
							if def, ok := sourceFunc[k]; ok {
								funcSource = def
							}
						}
						// MariaDB→MySQL：映射源端定义中的 MariaDB 特有 collation
						if stcls.isMariaDBToMySQL() {
							funcSource = mapMariaDBCollationInRoutineSQL(funcSource)
						}
						funcSqls := mysql.GenerateRoutineFixSQL(schema, destSchema, k, "FUNCTION", funcSource)
						// 查询 charset session 元数据并插入 SET 语句
						csClient, colConn, dbCollation := queryRoutineCharsetMetadata(stcls.sourceDB, schema, k, "FUNCTION")
						if csClient != "" {
							charsetStmts := buildRoutineCharsetSetStatements(csClient, colConn, dbCollation, stcls.isMariaDBToMySQL())
							if len(charsetStmts) > 0 {
								enriched := make([]string, 0, len(charsetStmts)+len(funcSqls))
								enriched = append(enriched, charsetStmts...)
								enriched = append(enriched, funcSqls...)
								funcSqls = enriched
							}
						}
						normalizedFuncSqls := make([]string, 0, len(funcSqls))
						for _, s := range funcSqls {
							ts := strings.TrimSpace(s)
							if ts == "" {
								continue
							}
							if strings.HasPrefix(strings.ToUpper(ts), "DROP ") && !strings.HasSuffix(ts, ";") {
								ts += ";"
							}
							normalizedFuncSqls = append(normalizedFuncSqls, ts)
						}
						out := make([]string, 0, len(normalizedFuncSqls)+2)
						out = append(out, "DELIMITER $$")
						for _, stmt := range normalizedFuncSqls {
							out = append(out, stmt+"\n$$")
						}
						out = append(out, "DELIMITER ;")
						origSchema, origTable, origObjType := stcls.schema, stcls.table, stcls.fixFileObjectType
						stcls.schema = schema
						stcls.table = k
						stcls.fixFileObjectType = "routine"
						if werr := stcls.writeFixSql(out, logThreadSeq); werr != nil {
							global.Wlog.Error(fmt.Sprintf("(%d) failed to write function fix SQL for %s.%s: %v", logThreadSeq, schema, k, werr))
						}
						stcls.schema, stcls.table, stcls.fixFileObjectType = origSchema, origTable, origObjType
					}
				}
			}
			// 汇总日志
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Stored Function. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			stcls.flushPods()
		}
	}

}

/*
Wrapper to Routine for PROCEDURE
*/
func (stcls *schemaTable) Proc(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	stcls.Routine(dtabS, logThreadSeq, logThreadSeq2, "PROCEDURE")
	return
}

/*
校验函数
*/
/*
Wrapper to Routine for FUNCTION
*/
func (stcls *schemaTable) Func(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	stcls.Routine(dtabS, logThreadSeq, logThreadSeq2, "FUNCTION")
	return
}
