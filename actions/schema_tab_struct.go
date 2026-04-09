package actions

import (
	"database/sql"
	"encoding/json"
	"fmt"
	mysql "gt-checksum/MySQL"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"gt-checksum/schemacompat"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 全局变量
var (
	// 用于存储表映射关系
	TableMappingRelations []string
)

var partitionMetadataPattern = regexp.MustCompile(`^NAME=(.*?),ORDINAL=(.*?),METHOD=(.*?),EXPRESSION=(.*?),DESCRIPTION=(.*),ROWS=(.*)$`)
var partitionDelimiterSpacingPattern = regexp.MustCompile(`\s*([(),])\s*`)
var mysqlVersionedCommentWrapperPattern = regexp.MustCompile(`(?is)^/\*!\d+\s*(.*?)\s*\*/$`)
var partitionExpressionColumnPatternTemplate = `(^|[^A-Z0-9_])%s([^A-Z0-9_]|$)`
var mysqlColumnCharsetOrCollationClausePattern = regexp.MustCompile(`(?i)\bCHARACTER\s+SET\b|\bCOLLATE\b`)
var mysqlCharacterColumnDefinitionPattern = regexp.MustCompile(`(?i)^(?:varchar|char|tinytext|text|mediumtext|longtext|enum|set)\b`)
var routineMetadataCommentPattern = regexp.MustCompile(`/\*GT_CHECKSUM_METADATA:(.*?)\*/`)
var routineInlineCommentPattern = regexp.MustCompile(`--.*?\n|/\*[\s\S]*?\*/`)
var routineWhitespacePattern = regexp.MustCompile(`\s+`)
var routineCharsetCollationClausePattern = regexp.MustCompile(`(?i)CHARSET\s+([a-zA-Z0-9_]+)(?:\s+COLLATE\s+([a-zA-Z0-9_]+))?`)
var standaloneCollatePattern = regexp.MustCompile(`(?i)\bCOLLATE\s+([a-zA-Z0-9_]+)`)
var intDisplayWidthPattern = regexp.MustCompile(`(?i)\b((?:tiny|small|medium|big)?int)\(\d+\)`)

// routineHeaderIdentifierPattern 匹配 routine 定义头部的标识符（DEFINER、routine 名称），
// 用于仅对标识符做大小写归一，而不影响函数体中的字符串字面量。
var routineHeaderIdentifierPattern = regexp.MustCompile("(?i)(CREATE\\s+(?:DEFINER\\s*=\\s*[^\\s]+\\s+)?(?:PROCEDURE|FUNCTION)\\s+)(`[^`]*`)")
var routineDefinerPattern = regexp.MustCompile(`CREATE\s+DEFINER\s*=\s*['"]?([^'"]*)['"]?@['"]?([^'"]*)['"]?`)
var routineSecurityPattern = regexp.MustCompile(`SQL\s+SECURITY\s+(\w+)`)
var routineCharsetPattern = regexp.MustCompile(`CHARACTER_SET_CLIENT\s*=\s*(\w+)`)
var routineCollationPattern = regexp.MustCompile(`COLLATION_CONNECTION\s*=\s*(\w+)`)
var routineDatabaseCollationPattern = regexp.MustCompile(`DATABASE\s+COLLATION\s*=\s*(\w+)`)
var createTableTargetIdentifierPattern = regexp.MustCompile(`(?i)(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)` + `(?:(` + "`[^`]+`" + `)\.)?(` + "`[^`]+`" + `)`)

// viewDefinerPattern matches the DEFINER clause in SHOW CREATE VIEW output, including
// both backtick-quoted and plain identifiers, so it can be stripped for comparison.
var viewDefinerPattern = regexp.MustCompile(`(?i)DEFINER\s*=\s*` + "`[^`]*`" + `@` + "`[^`]*`" + `\s*`)

// viewAlgorithmUndefinedPattern matches the ALGORITHM=UNDEFINED clause.
// ALGORITHM=UNDEFINED is the MySQL default and is semantically identical to omitting
// the ALGORITHM clause entirely; some MySQL versions/configurations include it in
// SHOW CREATE VIEW output while others omit it, which would otherwise cause false
// positives on otherwise-identical VIEW definitions.
var viewAlgorithmUndefinedPattern = regexp.MustCompile(`(?i)\bALGORITHM\s*=\s*UNDEFINED\s*`)
var viewExtractAlgorithmPattern = regexp.MustCompile(`(?i)\bALGORITHM\s*=\s*(UNDEFINED|MERGE|TEMPTABLE)\b`)

// viewSQLSecurityPattern matches the SQL SECURITY clause (DEFINER or INVOKER).
// In MySQL→MySQL migration scenarios, SQL SECURITY often legitimately changes
// (e.g. DEFINER on source, INVOKER on target after account restructuring).
// Per the cc design document §四, SQL SECURITY differences are downgraded to
// warn-log only in the first version and must not trigger Diffs=yes on their own.
var viewSQLSecurityPattern = regexp.MustCompile(`(?i)\bSQL\s+SECURITY\s+(?:DEFINER|INVOKER)\s*`)

// viewExtractSQLSecurityPattern captures the SQL SECURITY value so it can be
// logged when source and destination differ (warn-only, never Diffs=yes).
var viewExtractSQLSecurityPattern = regexp.MustCompile(`(?i)\bSQL\s+SECURITY\s+(DEFINER|INVOKER)\b`)

var viewWhitespaceNormPattern = regexp.MustCompile(`\s+`)

// viewHeaderBodyPattern splits a SHOW CREATE VIEW DDL into two capture groups:
//
//	(1) the CREATE … VIEW `name` header (keywords + identifiers only — safe to lowercase)
//	(2) the AS <select-body> tail (may contain string literals — must NOT be lowercased)
//
// The header stops at the last backtick-quoted identifier before "AS"; the body begins at "AS ".
var viewHeaderBodyPattern = regexp.MustCompile(`(?is)^(create\s+.*?view\s+(?:` + "`[^`]+`" + `\.)?` + "`[^`]+`" + `\s+)(as\s+.*)$`)

// viewSchemaInHeaderPattern matches a schema-qualified VIEW identifier in the
// normalised (lowercased) header, e.g. `db1`.`v1` .  It captures only the view
// part so the schema prefix can be stripped — preventing false Diffs=yes when
// source and destination use different schema names (cross-schema mapping).
var viewSchemaInHeaderPattern = regexp.MustCompile("`[^`]+`" + `\.(` + "`[^`]+`" + `\s*)$`)

// viewWhereOuterParensRe detects a WHERE clause whose condition starts with '('.
// MySQL 8.0 unconditionally wraps the entire WHERE condition in parentheses when
// storing the view definition (e.g. "where (`f1` > '3')"), while MariaDB and
// MySQL 5.7 omit the outer parens ("where `f1` > '3'").  Both forms are
// semantically identical and are normalised to the unparenthesized form.
var viewWhereOuterParensRe = regexp.MustCompile(`(?i)\bwhere\s+\(`)

// normalizeViewWhereOuterParens strips a single layer of redundant outer parentheses
// from the WHERE clause body when they wrap the entire condition up to the next
// top-level SQL clause or end of string.
//
// Safe cases (stripped):
//
//	"where (f1 > '3')"                    → "where f1 > '3'"
//	"where (f1 > '3') group by f1"        → "where f1 > '3' group by f1"
//	"where (a IN (1,2,3))"                → "where a IN (1,2,3)"
//
// NOT stripped (returned unchanged):
//
//	"where (a > 1) and (b < 2)"           ← outer paren does not span entire condition
//	"where a > 1"                         ← no outer paren present
func normalizeViewWhereOuterParens(body string) string {
	loc := viewWhereOuterParensRe.FindStringIndex(body)
	if loc == nil {
		return body
	}
	openPos := loc[1] - 1 // position of the '(' immediately after WHERE
	// Walk forward to find the balanced closing ')'.
	depth := 0
	closePos := -1
	for i := openPos; i < len(body); i++ {
		switch body[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				closePos = i
			}
		}
		if closePos >= 0 {
			break
		}
	}
	if closePos < 0 {
		return body // unbalanced parens — do not modify
	}
	// Only strip when nothing after the closing paren except whitespace or a
	// top-level SQL clause.  This avoids incorrectly stripping cases like
	// "where (a > 1) and b < 2" where the outer paren is NOT redundant.
	after := strings.TrimSpace(body[closePos+1:])
	afterUp := strings.ToUpper(after)
	topLevel := after == "" || after == ";" ||
		strings.HasPrefix(afterUp, "GROUP BY") ||
		strings.HasPrefix(afterUp, "HAVING") ||
		strings.HasPrefix(afterUp, "ORDER BY") ||
		strings.HasPrefix(afterUp, "LIMIT") ||
		strings.HasPrefix(afterUp, "UNION") ||
		strings.HasPrefix(afterUp, "EXCEPT") ||
		strings.HasPrefix(afterUp, "INTERSECT")
	if !topLevel {
		return body
	}
	return body[:openPos] + body[openPos+1:closePos] + body[closePos+1:]
}

type partitionMetadata struct {
	Name        string
	Ordinal     int
	Method      string
	Expression  string
	Description string
	Rows        string
}

func normalizePartitionCompareText(value string) string {
	normalized := strings.TrimSpace(value)
	normalized = strings.ReplaceAll(normalized, "`", "")
	normalized = strings.ReplaceAll(normalized, "!", "")
	normalized = strings.Join(strings.Fields(normalized), " ")
	normalized = partitionDelimiterSpacingPattern.ReplaceAllString(normalized, "$1")
	return strings.ToUpper(normalized)
}

func normalizePartitionFullDefinition(value string) string {
	normalized := strings.TrimSpace(value)
	for {
		matches := mysqlVersionedCommentWrapperPattern.FindStringSubmatch(normalized)
		if len(matches) != 2 {
			break
		}
		// SHOW CREATE TABLE may wrap the same partition clause in a versioned
		// comment on one side but not the other. The wrapper itself is metadata
		// noise and should not affect semantic comparison.
		normalized = strings.TrimSpace(matches[1])
	}
	return normalizePartitionCompareText(normalized)
}

func parsePartitionMetadataEntries(partitions map[string]string, tableKey string) []partitionMetadata {
	entries := make([]partitionMetadata, 0)
	prefix := tableKey + "."
	for key, value := range partitions {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		matches := partitionMetadataPattern.FindStringSubmatch(value)
		if len(matches) != 7 {
			continue
		}
		ordinal, err := strconv.Atoi(strings.TrimSpace(matches[2]))
		if err != nil {
			continue
		}
		entries = append(entries, partitionMetadata{
			Name:        strings.TrimSpace(matches[1]),
			Ordinal:     ordinal,
			Method:      strings.TrimSpace(matches[3]),
			Expression:  strings.TrimSpace(matches[4]),
			Description: strings.TrimSpace(matches[5]),
			Rows:        strings.TrimSpace(matches[6]),
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Ordinal == entries[j].Ordinal {
			return entries[i].Name < entries[j].Name
		}
		return entries[i].Ordinal < entries[j].Ordinal
	})
	return entries
}

func partitionRowsReportedEmpty(meta partitionMetadata) bool {
	rows := strings.TrimSpace(meta.Rows)
	if rows == "" {
		return false
	}
	value, err := strconv.ParseFloat(rows, 64)
	if err != nil {
		return false
	}
	return value == 0
}

func partitionsShareLeadingLayout(sourceParts, destParts []partitionMetadata) bool {
	sharedCount := len(sourceParts)
	if len(destParts) < sharedCount {
		sharedCount = len(destParts)
	}
	for idx := 0; idx < sharedCount; idx++ {
		sourceMeta := sourceParts[idx]
		destMeta := destParts[idx]
		if !strings.EqualFold(sourceMeta.Name, destMeta.Name) {
			return false
		}
		if normalizePartitionCompareText(sourceMeta.Method) != normalizePartitionCompareText(destMeta.Method) {
			return false
		}
		if normalizePartitionCompareText(sourceMeta.Expression) != normalizePartitionCompareText(destMeta.Expression) {
			return false
		}
		if normalizePartitionCompareText(sourceMeta.Description) != normalizePartitionCompareText(destMeta.Description) {
			return false
		}
	}
	return true
}

func buildPartitionValidationQuery(schemaName, tableName, partitionName string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` PARTITION (`%s`);", schemaName, tableName, partitionName)
}

func buildDropPartitionAdvisoryLines(schemaName, tableName string, partitions []partitionMetadata) []string {
	if len(partitions) == 0 {
		return nil
	}
	lines := []string{
		fmt.Sprintf("-- gt-checksum advisory begin: %s.%s partition repair", schemaName, tableName),
	}
	for _, partition := range partitions {
		lines = append(lines, "-- 请在确认该分区不存在任何数据后再执行此操作")
		lines = append(lines, fmt.Sprintf("-- %s", buildPartitionValidationQuery(schemaName, tableName, partition.Name)))
		lines = append(lines, fmt.Sprintf("-- ALTER TABLE `%s`.`%s` DROP PARTITION `%s`;", schemaName, tableName, partition.Name))
	}
	lines = append(lines, fmt.Sprintf("-- gt-checksum advisory end: %s.%s partition repair", schemaName, tableName))
	return lines
}

func formatPartitionDescriptionForAdd(meta partitionMetadata) (string, bool) {
	description := strings.TrimSpace(meta.Description)
	if description == "" {
		return "", false
	}
	method := normalizePartitionCompareText(meta.Method)

	switch {
	case strings.HasPrefix(method, "RANGE"):
		if strings.EqualFold(description, "MAXVALUE") {
			if strings.Contains(method, "COLUMNS") {
				return "(MAXVALUE)", true
			}
			return "MAXVALUE", true
		}
		if strings.HasPrefix(description, "(") && strings.HasSuffix(description, ")") {
			return description, true
		}
		return fmt.Sprintf("(%s)", description), true
	case strings.HasPrefix(method, "LIST"):
		if strings.HasPrefix(description, "(") && strings.HasSuffix(description, ")") {
			return description, true
		}
		return fmt.Sprintf("(%s)", description), true
	default:
		return "", false
	}
}

func buildAddPartitionClause(meta partitionMetadata) (string, bool) {
	formattedDescription, ok := formatPartitionDescriptionForAdd(meta)
	if !ok {
		return "", false
	}
	method := normalizePartitionCompareText(meta.Method)
	switch {
	case strings.HasPrefix(method, "RANGE"):
		return fmt.Sprintf("PARTITION `%s` VALUES LESS THAN %s", meta.Name, formattedDescription), true
	case strings.HasPrefix(method, "LIST"):
		return fmt.Sprintf("PARTITION `%s` VALUES IN %s", meta.Name, formattedDescription), true
	default:
		return "", false
	}
}

func buildAddPartitionSQL(schemaName, tableName string, partitions []partitionMetadata) []string {
	clauses := make([]string, 0, len(partitions))
	for _, partition := range partitions {
		clause, ok := buildAddPartitionClause(partition)
		if !ok {
			return nil
		}
		clauses = append(clauses, clause)
	}
	if len(clauses) == 0 {
		return nil
	}
	return []string{
		fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PARTITION (%s);", schemaName, tableName, strings.Join(clauses, ", ")),
	}
}

func buildPartitionRepairSQLs(sourceSchema, sourceTable, destSchema, destTable string, sourcePartitions, destPartitions map[string]string) ([]string, []string, bool, string) {
	sourceTableKey := fmt.Sprintf("%s.%s", sourceSchema, sourceTable)
	destTableKey := fmt.Sprintf("%s.%s", destSchema, destTable)
	sourceEntries := parsePartitionMetadataEntries(sourcePartitions, sourceTableKey)
	destEntries := parsePartitionMetadataEntries(destPartitions, destTableKey)

	if len(sourceEntries) == 0 || len(destEntries) == 0 {
		return nil, nil, false, "partition metadata is incomplete"
	}
	if !partitionsShareLeadingLayout(sourceEntries, destEntries) {
		return nil, nil, false, "shared partition prefix is not semantically identical"
	}

	switch {
	case len(sourceEntries) < len(destEntries):
		extraDestPartitions := destEntries[len(sourceEntries):]
		for _, partition := range extraDestPartitions {
			if !partitionRowsReportedEmpty(partition) {
				return nil, nil, false, fmt.Sprintf("extra target partition %s is not reported empty", partition.Name)
			}
		}
		return nil, buildDropPartitionAdvisoryLines(destSchema, destTable, extraDestPartitions), true, "extra empty tail partitions detected on target"
	case len(sourceEntries) > len(destEntries):
		missingDestPartitions := sourceEntries[len(destEntries):]
		addSQL := buildAddPartitionSQL(destSchema, destTable, missingDestPartitions)
		if len(addSQL) == 0 {
			return nil, nil, false, "tail partitions require an unsupported ADD PARTITION shape"
		}
		return addSQL, nil, true, "missing tail partitions detected on target"
	default:
		return nil, nil, false, "partition counts are identical but definitions still differ"
	}
}

func classifyPartitionRepairDiffState(execRepairSQLs, advisoryRepairSQLs []string, handled bool) string {
	if !handled {
		return global.SkipDiffsYes
	}
	if len(execRepairSQLs) == 0 && len(advisoryRepairSQLs) > 0 {
		return global.SkipDiffsWarnOnly
	}
	return global.SkipDiffsYes
}

func loadTablePartitionExpressions(db *sql.DB, drive, schemaName, tableName, caseSensitiveObjectName string, logThreadSeq int64) []string {
	tc := dbExec.TableColumnNameStruct{
		Drive:                   drive,
		Schema:                  schemaName,
		Table:                   tableName,
		CaseSensitiveObjectName: caseSensitiveObjectName,
	}
	partitions, err := tc.Query().Partitions(db, logThreadSeq)
	if err != nil {
		global.Wlog.Warn(fmt.Sprintf("(%d) Failed to load partition expressions for %s.%s: %v", logThreadSeq, schemaName, tableName, err))
		return nil
	}
	tableKey := fmt.Sprintf("%s.%s", schemaName, tableName)
	entries := parsePartitionMetadataEntries(partitions, tableKey)
	if len(entries) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	expressions := make([]string, 0, len(entries))
	for _, entry := range entries {
		expr := normalizePartitionCompareText(entry.Expression)
		if strings.TrimSpace(expr) == "" {
			continue
		}
		if _, exists := seen[expr]; exists {
			continue
		}
		seen[expr] = struct{}{}
		expressions = append(expressions, expr)
	}
	return expressions
}

func partitionExpressionsReferenceColumn(expressions []string, columnNames ...string) bool {
	if len(expressions) == 0 || len(columnNames) == 0 {
		return false
	}

	// Pre-compile one regexp per unique column name so the inner expression
	// loop never triggers repeated MustCompile calls.
	type columnPattern struct {
		pattern *regexp.Regexp
	}
	patterns := make([]columnPattern, 0, len(columnNames))
	for _, candidate := range columnNames {
		normalizedColumn := strings.ToUpper(strings.TrimSpace(strings.ReplaceAll(candidate, "`", "")))
		if normalizedColumn == "" {
			continue
		}
		patterns = append(patterns, columnPattern{
			pattern: regexp.MustCompile(fmt.Sprintf(partitionExpressionColumnPatternTemplate, regexp.QuoteMeta(normalizedColumn))),
		})
	}
	if len(patterns) == 0 {
		return false
	}

	for _, expression := range expressions {
		normalizedExpression := strings.ToUpper(strings.TrimSpace(strings.ReplaceAll(expression, "`", "")))
		for _, cp := range patterns {
			if cp.pattern.MatchString(normalizedExpression) {
				return true
			}
		}
	}
	return false
}

func shouldDeferPartitionKeyColumnRepair(expressions []string, decision schemacompat.CompatibilityDecision, columnNames ...string) bool {
	if !decision.IsMismatch() || decision.State == schemacompat.CompatibilityWarnOnly {
		return false
	}
	return partitionExpressionsReferenceColumn(expressions, columnNames...)
}

func mergeStructDiffState(current, incoming string) string {
	switch strings.TrimSpace(incoming) {
	case global.SkipDiffsYes, global.SkipDiffsDDLYes:
		return global.SkipDiffsYes
	case global.SkipDiffsWarnOnly:
		if strings.TrimSpace(current) == global.SkipDiffsYes || strings.TrimSpace(current) == global.SkipDiffsDDLYes {
			return global.SkipDiffsYes
		}
		return global.SkipDiffsWarnOnly
	case global.SkipDiffsCollationMapped:
		cur := strings.TrimSpace(current)
		if cur == global.SkipDiffsYes || cur == global.SkipDiffsDDLYes || cur == global.SkipDiffsWarnOnly {
			return cur
		}
		return global.SkipDiffsCollationMapped
	default:
		if strings.TrimSpace(current) == "" {
			return global.SkipDiffsNo
		}
		return current
	}
}

func shouldUseCaseSensitiveColumnMatching(sourceDrive, destDrive, caseSensitiveObjectName string, oracleToMySQLDataMode bool) bool {
	if oracleToMySQLDataMode {
		return false
	}
	// MySQL column identifiers are matched case-insensitively even when table
	// name handling remains case-sensitive on the host filesystem.
	if strings.EqualFold(sourceDrive, "mysql") && strings.EqualFold(destDrive, "mysql") {
		return false
	}
	return strings.EqualFold(caseSensitiveObjectName, "yes")
}

func indexColumnsOnlyDifferInCase(sourceColumns, destColumns []string) bool {
	if len(sourceColumns) != len(destColumns) {
		return false
	}
	for i := range sourceColumns {
		if !strings.EqualFold(sourceColumns[i], destColumns[i]) {
			return false
		}
	}
	return true
}

func mergeIndexVisibilityHints(base map[string]string, hints map[string]string) map[string]string {
	if len(base) == 0 && len(hints) == 0 {
		return map[string]string{}
	}
	merged := make(map[string]string, len(base)+len(hints))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range hints {
		merged[k] = v
	}
	return merged
}

func isInvisibleLikeIndexVisibility(visibility string) bool {
	return strings.EqualFold(visibility, "NO") || strings.EqualFold(visibility, "INVISIBLE") || strings.EqualFold(visibility, "IGNORED")
}

// measuredDataPods 在 terminal_result_output.go 中已定义

type schemaTable struct {
	// 现有字段...
	aggregate bool // 是否启用缓冲聚合（最小入侵新增）
	// 统一缓冲，用 CheckObject 区分 proc/func（最小入侵新增）
	podsBuffer              []Pod
	schema                  string
	table                   string
	destTable               string // 目标表名，可能与源表名不同
	ignoreSchema            string
	ignoreTable             string
	sourceDrive             string
	destDrive               string
	sourceVersion           global.MySQLVersionInfo
	destVersion             global.MySQLVersionInfo
	sourceDB                *sql.DB
	destDB                  *sql.DB
	caseSensitiveObjectName string
	datafix           string
	datafixSql        string
	fixFileObjectType string // 文件名中的对象类型前缀，如 "table"/"view"/"trigger"/"routine"
	djdbc                   string
	checkRules              inputArg.RulesS
	// 添加表映射规则
	tableMappings map[string]string
	// 需要跳过索引检查的表列表
	skipIndexCheckTables []string
	// 列修复操作映射表，用于合并列和索引操作
	columnRepairMap map[string][]string
	// Captures tables removed by ignoreTables for better diagnostics.
	ignoredMatchedTables []string
	// Keep per-run struct diff state on the schemaTable instance so repeated or
	// concurrent checks do not share mutable package-level maps.
	indexDiffsMap            map[string]bool
	partitionDiffsMap        map[string]bool
	foreignKeyDiffsMap       map[string]bool
	structWarnOnlyDiffsMap   map[string]bool
	structCollationMappedMap map[string]bool
	// objectKinds maps "schema/*schema&table*/table" (same key format as
	// DatabaseNameList) to the TABLE_TYPE value ("BASE TABLE" or "VIEW").
	// Populated once in SchemaTableFilter; absent key means "BASE TABLE".
	objectKinds map[string]string

	// columnPlan 非 nil 时表示当前运行处于部分列校验模式（columns 参数已配置）。
	// 在 TableColumnNameCheck 中用于豁免已明确映射的列对，避免误报 DDL mismatch。
	columnPlan *inputArg.TableColumnPlan
}

func cloneSQLStatements(sqls []string) []string {
	if len(sqls) == 0 {
		return nil
	}
	cloned := make([]string, len(sqls))
	copy(cloned, sqls)
	return cloned
}

// rememberColumnRepairOperations defers column-level fix SQL until index repairs
// are known, so both kinds of changes can be emitted as one ALTER TABLE.
func (stcls *schemaTable) rememberColumnRepairOperations(tableKey string, sqls []string) {
	if stcls == nil || len(sqls) == 0 {
		return
	}
	if stcls.columnRepairMap == nil {
		stcls.columnRepairMap = make(map[string][]string)
	}
	stcls.columnRepairMap[tableKey] = cloneSQLStatements(sqls)
}

func (stcls *schemaTable) pendingColumnRepairOperations(tableKey string) []string {
	if stcls == nil || stcls.columnRepairMap == nil {
		return nil
	}
	return cloneSQLStatements(stcls.columnRepairMap[tableKey])
}

func (stcls *schemaTable) forgetColumnRepairOperations(tableKey string) {
	if stcls == nil || stcls.columnRepairMap == nil {
		return
	}
	delete(stcls.columnRepairMap, tableKey)
}

func hasAutoIncrementColumnAttribute(columnDefinition []string) bool {
	for _, attr := range columnDefinition {
		if strings.Contains(strings.ToUpper(attr), "AUTO_INCREMENT") {
			return true
		}
	}
	return false
}


func (stcls *schemaTable) sourceVersionInfo() global.MySQLVersionInfo {
	if stcls != nil && strings.TrimSpace(stcls.sourceVersion.Raw) != "" {
		return stcls.sourceVersion
	}
	return global.SourceMySQLVersion
}

func (stcls *schemaTable) destVersionInfo() global.MySQLVersionInfo {
	if stcls != nil && strings.TrimSpace(stcls.destVersion.Raw) != "" {
		return stcls.destVersion
	}
	return global.DestMySQLVersion
}

func queryVersionInfoFromDB(db *sql.DB) (global.MySQLVersionInfo, error) {
	if db == nil {
		return global.MySQLVersionInfo{}, fmt.Errorf("db is nil")
	}

	var version string
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		return global.MySQLVersionInfo{}, err
	}
	return global.ParseMySQLVersion(version)
}

// normalizeStoredProcBody 规范化存储过程体，以便更准确地比较
// 规范化处理包括：
// 1. 移除多余的空格和换行符
// 2. 将所有空白字符规范化为单个空格
// 3. 移除注释
// 4. 将所有关键字转换为大写（可选，取决于数据库的大小写敏感性）
// 5. 规范化算术表达式，移除不必要的空格
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

// buildTriggerCharsetSetStatements 生成 trigger fix SQL 需要的 charset session 变量 SET 语句
func buildTriggerCharsetSetStatements(result triggerCreateResult, isMariaDBToMySQL bool) []string {
	return buildRoutineCharsetSetStatements(result.CharacterSetClient, result.CollationConnection, result.DatabaseCollation, isMariaDBToMySQL)
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
// 如果不存在映射关系，返回格式为 "schema.table"
func (stcls *schemaTable) getDisplayTableName(schema, table string) string {
	// 检查是否存在映射关系
	if mappedSchema, exists := stcls.tableMappings[schema]; exists && mappedSchema != schema {
		// 存在映射关系，返回包含映射信息的名称
		return fmt.Sprintf("%s.%s:%s.%s", schema, table, mappedSchema, table)
	}

	// 不存在映射关系，返回普通名称
	return fmt.Sprintf("%s.%s", schema, table)
}

// getSourceTableName 返回源表的名称
func (stcls *schemaTable) getSourceTableName(schema, table string) string {
	return fmt.Sprintf("%s.%s", schema, table)
}

func isOracleDriveName(drive string) bool {
	return strings.EqualFold(drive, "godror") || strings.EqualFold(drive, "oracle")
}

func splitSchemaTableCacheKey(key string) (string, string, bool) {
	parts := strings.SplitN(key, "/*schema&table*/", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func (stcls *schemaTable) sourceObjectNameEqual(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if isOracleDriveName(stcls.sourceDrive) {
		return strings.EqualFold(a, b)
	}
	if strings.EqualFold(stcls.caseSensitiveObjectName, "yes") {
		return a == b
	}
	return strings.EqualFold(a, b)
}

func (stcls *schemaTable) destObjectNameEqual(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if isOracleDriveName(stcls.destDrive) {
		return strings.EqualFold(a, b)
	}
	if strings.EqualFold(stcls.caseSensitiveObjectName, "yes") {
		return a == b
	}
	return strings.EqualFold(a, b)
}

func (stcls *schemaTable) findMappedSchema(sourceSchema string) (string, bool) {
	if mapped, ok := stcls.tableMappings[sourceSchema]; ok {
		return mapped, true
	}
	for src, dst := range stcls.tableMappings {
		if stcls.sourceObjectNameEqual(src, sourceSchema) {
			return dst, true
		}
	}
	return "", false
}

func (stcls *schemaTable) tableKeyInSet(tableSet map[string]int, schema, table string) bool {
	for key := range tableSet {
		parts := strings.SplitN(key, ".", 2)
		if len(parts) != 2 {
			continue
		}
		if stcls.sourceObjectNameEqual(parts[0], schema) && stcls.sourceObjectNameEqual(parts[1], table) {
			return true
		}
	}
	return false
}

func splitSourcePattern(pattern string) (string, string, bool) {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return "", "", false
	}
	if strings.Contains(pattern, ":") {
		pattern = strings.SplitN(pattern, ":", 2)[0]
	}
	parts := strings.SplitN(pattern, ".", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), true
}

func hasObjectWildcard(pattern string) bool {
	return strings.Contains(pattern, "*") || strings.Contains(pattern, "%")
}

// Explicit schema.table selections should win over ignoreTables to avoid
// silently dropping the only requested table from the checklist.
func (stcls *schemaTable) isExplicitSourceTableSelection(schema, table string) bool {
	for _, pattern := range strings.Split(stcls.table, ",") {
		sourceSchema, sourceTable, ok := splitSourcePattern(pattern)
		if !ok || hasObjectWildcard(sourceSchema) || hasObjectWildcard(sourceTable) {
			continue
		}
		if stcls.sourceObjectNameEqual(sourceSchema, schema) && stcls.sourceObjectNameEqual(sourceTable, table) {
			return true
		}
	}
	return false
}

func (stcls *schemaTable) recordIgnoredMatchedTable(schema, table string) {
	qualifiedName := fmt.Sprintf("%s.%s", schema, table)
	for _, existing := range stcls.ignoredMatchedTables {
		parts := strings.SplitN(existing, ".", 2)
		if len(parts) != 2 {
			continue
		}
		if stcls.sourceObjectNameEqual(parts[0], schema) && stcls.sourceObjectNameEqual(parts[1], table) {
			return
		}
	}
	stcls.ignoredMatchedTables = append(stcls.ignoredMatchedTables, qualifiedName)
}

func (stcls *schemaTable) IgnoredMatchedTablesSummary() string {
	if len(stcls.ignoredMatchedTables) == 0 {
		return ""
	}
	summary := append([]string(nil), stcls.ignoredMatchedTables...)
	sort.Strings(summary)
	return strings.Join(summary, ", ")
}

func (stcls *schemaTable) shouldIgnoreMatchedTable(ignoreSchema map[string]int, schema, table string) bool {
	if !stcls.tableKeyInSet(ignoreSchema, schema, table) {
		return false
	}
	if stcls.isExplicitSourceTableSelection(schema, table) {
		if global.Wlog != nil {
			global.Wlog.Warn(fmt.Sprintf("Explicitly selected table %s.%s also matches ignoreTables; keeping it in the checklist", schema, table))
		}
		return false
	}
	stcls.recordIgnoredMatchedTable(schema, table)
	return true
}

// getDestTableName 返回目标表的名称
func (stcls *schemaTable) getDestTableName(schema, table string) string {
	destSchema := schema
	if mappedSchema, exists := stcls.tableMappings[schema]; exists {
		destSchema = mappedSchema
	}
	return fmt.Sprintf("%s.%s", destSchema, table)
}

/*
查询待校验表的列名
*/
func (stcls *schemaTable) tableColumnName(db *sql.DB, tc dbExec.TableColumnNameStruct, logThreadSeq, logThreadSeq2 int64) ([]map[string][]string, error) {
	var (
		col       []map[string][]string
		vlog      string
		CS        []string
		queryData []map[string]interface{}
		err       error
		Event     = "Q_table_columns"
		A         = make(map[string][]string)
		C         = func(c string) string {
			switch c {
			case "<nil>":
				return "null"
			case "<entry>":
				return "" // 返回空字符串而不是"empty"
			default:
				return c
			}
		}
	)
	if queryData, err = tc.Query().TableColumnName(db, logThreadSeq2); err != nil {
		return col, err
	}
	vlog = fmt.Sprintf("(%d) [%s] Starting column validation", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	for _, v := range queryData {
		if fmt.Sprintf("%v", v["columnName"]) != "" {
			// 获取extra属性，包含AUTO_INCREMENT和INVISIBLE等特殊属性
			extra := C(fmt.Sprintf("%v", v["extra"]))
			extra = schemacompat.StripMySQLMetadataOnlyExtraTokens(extra)
			// 将extra添加到列定义数组中，放在columnType之后，这样可以在生成SQL时包含特殊属性
			columnType := fmt.Sprintf("%v", v["columnType"])
			// 如果有extra属性，添加到columnType后面
			if extra != "null" && extra != "" {
				columnType = fmt.Sprintf("%s %s", columnType, extra)
			}
			A[fmt.Sprintf("%v", v["columnName"])] = []string{C(columnType), C(fmt.Sprintf("%v", v["charset"])), C(fmt.Sprintf("%v", v["collationName"])), C(fmt.Sprintf("%v", v["isNull"])), C(fmt.Sprintf("%v", v["columnDefault"])), C(fmt.Sprintf("%v", v["columnComment"]))}
			CS = append(CS, fmt.Sprintf("%v", v["columnName"]))
		}
	}
	for _, v := range CS {
		col = append(col, map[string][]string{v: A[v]})
	}
	vlog = fmt.Sprintf("(%d) [%s] Column validation completed", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	return col, nil
}

func escapeSQLLiteral(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func escapeMySQLIdentifier(value string) string {
	return strings.ReplaceAll(value, "`", "``")
}

func isOracleDrive(drive string) bool {
	return drive == "godror" || strings.EqualFold(drive, "oracle")
}

func (stcls *schemaTable) isMySQLToMySQL() bool {
	return strings.EqualFold(stcls.sourceDrive, "mysql") && strings.EqualFold(stcls.destDrive, "mysql")
}

func (stcls *schemaTable) isMariaDBToMySQL() bool {
	return stcls.sourceVersionInfo().Flavor == global.DatabaseFlavorMariaDB &&
		stcls.destVersionInfo().Flavor == global.DatabaseFlavorMySQL
}

func isIgnorableGeneratedInvisibleColumn(colName string, columnMap map[string][]string) bool {
	if !strings.EqualFold(strings.TrimSpace(colName), "my_row_id") {
		return false
	}
	columnDef, exists := columnMap[colName]
	if !exists {
		return false
	}
	for _, def := range columnDef {
		upperDef := strings.ToUpper(strings.TrimSpace(def))
		if strings.Contains(upperDef, "INVISIBLE") {
			return true
		}
	}
	return false
}

func filterIgnorableGeneratedInvisibleColumns(columns []string, columnMap map[string][]string) ([]string, []string) {
	kept := make([]string, 0, len(columns))
	ignored := make([]string, 0)
	for _, col := range columns {
		if isIgnorableGeneratedInvisibleColumn(col, columnMap) {
			ignored = append(ignored, col)
			continue
		}
		kept = append(kept, col)
	}
	return kept, ignored
}

// Trigger metadata compare currently relies on INFORMATION_SCHEMA fields that
// are stable for MySQL-family sources and MySQL targets in the first-stage
// support matrix. When version info is unavailable, fall back to the driver
// pair so the existing MySQL -> MySQL behavior does not regress.
func (stcls *schemaTable) shouldCompareTriggerMetadata() bool {
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

// Routine metadata compare follows the same first-stage support matrix as
// trigger metadata: keep MySQL -> MySQL behavior unchanged, and explicitly
// enable MariaDB -> MySQL 8.0/8.4 so COMMENT and DEFINER drift are no longer
// silently skipped on the primary implementation path.
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

func normalizeMetadataComment(v string) string {
	s := strings.TrimSpace(v)
	switch strings.ToLower(s) {
	case "", "<entry>", "<nil>", "null":
		return ""
	default:
		return s
	}
}

func normalizeDataCheckColumnInfo(sourceCols, destCols []map[string]string) ([]map[string]string, []map[string]string, []string) {
	sourceKeys := make(map[string]struct{}, len(sourceCols))
	for _, col := range sourceCols {
		name := strings.TrimSpace(col["columnName"])
		if name == "" {
			name = strings.TrimSpace(col["COLUMN_NAME"])
		}
		if name == "" {
			continue
		}
		sourceKeys[strings.ToUpper(name)] = struct{}{}
	}

	filteredDest := make([]map[string]string, 0, len(destCols))
	stripped := make([]string, 0)
	for _, col := range destCols {
		name := strings.TrimSpace(col["columnName"])
		if name == "" {
			name = strings.TrimSpace(col["COLUMN_NAME"])
		}
		extra := strings.TrimSpace(col["extra"])
		if extra == "" {
			extra = strings.TrimSpace(col["EXTRA"])
		}
		if name != "" {
			if _, exists := sourceKeys[strings.ToUpper(name)]; !exists &&
				strings.EqualFold(name, "my_row_id") &&
				strings.Contains(strings.ToUpper(extra), "INVISIBLE") {
				stripped = append(stripped, name)
				continue
			}
		}
		filteredDest = append(filteredDest, col)
	}

	return sourceCols, filteredDest, stripped
}

type mysqlTableLevelMetadata struct {
	TableCollation string
	TableCharset   string
	AutoIncrement  sql.NullInt64
	RowFormat      string
	CreateOptions  string
	TableComment   string
	CreateTableSQL string
}

type columnCollationRepairCandidate struct {
	ColumnName       string
	ColumnSeq        int
	LastColumn       string
	SourceAttrs      []string
	SourceDefinition string
	SourceCharset    string
	SourceCollation  string
	DestCharset      string
	DestCollation    string
	Reason           string
}

func hasExplicitColumnCharsetOrCollation(definition string) bool {
	return mysqlColumnCharsetOrCollationClausePattern.MatchString(strings.TrimSpace(definition))
}

func isCharacterColumnDefinition(definition string) bool {
	return mysqlCharacterColumnDefinitionPattern.MatchString(strings.TrimSpace(definition))
}

func canUseTableCharsetConvertForColumnCollationDrift(sourceMeta, destMeta mysqlTableLevelMetadata, sourceColumnDefinitions map[string]string, candidates []columnCollationRepairCandidate) bool {
	if len(candidates) == 0 {
		return false
	}
	// 当 LEFT JOIN COLLATIONS 失败导致 charset 为空时，从 collation 名推断
	sourceCharset := strings.TrimSpace(sourceMeta.TableCharset)
	if sourceCharset == "" {
		sourceCharset = schemacompat.InferCharsetFromCollation(sourceMeta.TableCollation)
	}
	if sourceCharset == "" {
		return false
	}
	if !strings.EqualFold(sourceCharset, strings.TrimSpace(destMeta.TableCharset)) {
		return false
	}

	for _, definition := range sourceColumnDefinitions {
		if !isCharacterColumnDefinition(definition) {
			continue
		}
		if hasExplicitColumnCharsetOrCollation(definition) {
			return false
		}
	}

	for _, candidate := range candidates {
		if !strings.EqualFold(strings.TrimSpace(candidate.SourceCharset), strings.TrimSpace(sourceMeta.TableCharset)) {
			return false
		}
		if !strings.EqualFold(strings.TrimSpace(candidate.SourceCollation), strings.TrimSpace(sourceMeta.TableCollation)) {
			return false
		}
	}

	return true
}

func buildColumnCollationAdvisorySuggestions(candidates []columnCollationRepairCandidate) []schemacompat.ConstraintRepairSuggestion {
	suggestions := make([]schemacompat.ConstraintRepairSuggestion, 0, len(candidates))
	for _, candidate := range candidates {
		suggestions = append(suggestions, schemacompat.ConstraintRepairSuggestion{
			ConstraintName: candidate.ColumnName,
			Kind:           "COLUMN COLLATION",
			Level:          schemacompat.ConstraintRepairLevelAdvisoryOnly,
			Reason:         candidate.Reason,
		})
	}
	return suggestions
}

func (stcls *schemaTable) buildColumnCollationRepairSQL(
	fixer dbExec.DataAbnormalFixInterface,
	sourceMeta, destMeta mysqlTableLevelMetadata,
	sourceColumnDefinitions map[string]string,
	candidates []columnCollationRepairCandidate,
	logThreadSeq int64,
) ([]string, bool) {
	if len(candidates) == 0 {
		return nil, false
	}

	if canUseTableCharsetConvertForColumnCollationDrift(sourceMeta, destMeta, sourceColumnDefinitions, candidates) {
		// 使用 CONVERT TO CHARACTER SET 修复列级 collation 漂移时，必须始终显式指定
		// source collation，否则在跨版本场景（如 MySQL 5.6 → 8.0）下，目标端会使用
		// 其自身默认 collation（utf8mb4_0900_ai_ci），而非源端期望的 collation。
		collation := strings.TrimSpace(sourceMeta.TableCollation)
		// MariaDB UCA 14.0.0 collation 在 MySQL 上不存在，映射为 UCA 9.0.0 等价物
		if collation != "" {
			if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(collation); ok {
				collation = mapped
			}
		}
		sqls := fixer.FixTableCharsetSqlGenerate(sourceMeta.TableCharset, collation, logThreadSeq)
		return sqls, len(sqls) > 0
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].ColumnSeq < candidates[j].ColumnSeq
	})

	alterOps := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		repairAttrs := append([]string(nil), candidate.SourceAttrs...)
		if len(repairAttrs) < 6 {
			for len(repairAttrs) < 6 {
				repairAttrs = append(repairAttrs, "null")
			}
		}
		repairPlan := schemacompat.BuildTargetColumnRepairPlan(
			candidate.ColumnName,
			repairAttrs,
			stcls.sourceVersionInfo(),
			stcls.destVersionInfo(),
			candidate.SourceDefinition,
			stcls.checkRules.MariaDBJSONTargetType,
		)
		if strings.TrimSpace(repairPlan.Type) != "" {
			repairAttrs[0] = repairPlan.Type
		}
		if strings.TrimSpace(repairPlan.Charset) != "" {
			repairAttrs[1] = repairPlan.Charset
		}
		if strings.TrimSpace(repairPlan.Collation) != "" {
			repairAttrs[2] = repairPlan.Collation
		}
		if repairPlan.UseDirectDefinition {
			if len(repairAttrs) < 7 {
				repairAttrs = append(repairAttrs, repairPlan.DirectDefinition)
			} else {
				repairAttrs[6] = repairPlan.DirectDefinition
			}
		}
		alterOps = append(alterOps, fixer.FixAlterColumnSqlDispos("modify", repairAttrs, candidate.ColumnSeq, candidate.LastColumn, candidate.ColumnName, logThreadSeq))
	}

	if len(alterOps) == 0 {
		return nil, false
	}
	return fixer.FixAlterColumnSqlGenerate(alterOps, logThreadSeq), true
}

func listMariaDBSequenceNames(db *sql.DB, schema string) ([]string, error) {
	rows, err := db.Query(`
SELECT TABLE_NAME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'SEQUENCE'
ORDER BY TABLE_NAME
`, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sequenceNames := make([]string, 0)
	for rows.Next() {
		var sequenceName string
		if err := rows.Scan(&sequenceName); err != nil {
			return nil, err
		}
		sequenceNames = append(sequenceNames, sequenceName)
	}
	return sequenceNames, rows.Err()
}

func collectSourceSchemasForStructCheck(checkTableList []string) []string {
	seen := make(map[string]struct{})
	schemas := make([]string, 0)
	for _, item := range checkTableList {
		sourceItem := item
		if strings.Contains(item, ":") {
			sourceItem = strings.SplitN(item, ":", 2)[0]
		}
		parts := strings.SplitN(sourceItem, ".", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			continue
		}
		schemaName := parts[0]
		if _, exists := seen[schemaName]; exists {
			continue
		}
		seen[schemaName] = struct{}{}
		schemas = append(schemas, schemaName)
	}
	sort.Strings(schemas)
	return schemas
}

// Sequence objects are outside the table repair scope, so the best-effort
// behavior is to emit explicit warn-only rows and advisory notes up front.
func (stcls *schemaTable) emitMariaDBSequenceWarnings(checkTableList []string, logThreadSeq int64) {
	if stcls.sourceVersionInfo().Flavor != global.DatabaseFlavorMariaDB || stcls.destVersionInfo().Flavor != global.DatabaseFlavorMySQL {
		return
	}

	for _, sourceSchema := range collectSourceSchemasForStructCheck(checkTableList) {
		sequenceNames, err := listMariaDBSequenceNames(stcls.sourceDB, sourceSchema)
		if err != nil {
			global.Wlog.Warn(fmt.Sprintf("(%d) Failed to list MariaDB sequences for schema %s: %v", logThreadSeq, sourceSchema, err))
			continue
		}
		if len(sequenceNames) == 0 {
			continue
		}

		destSchema := stcls.mappedDestSchema(sourceSchema)
		suggestions := schemacompat.BuildMariaDBSequenceObjectSuggestions(sourceSchema, sequenceNames)
		for idx, sequenceName := range sequenceNames {
			stcls.appendPod(Pod{
				Schema:      sourceSchema,
				Table:       sequenceName,
				CheckObject: "Sequence",
				DIFFS:       global.SkipDiffsWarnOnly,
				Datafix:     stcls.datafix,
			})

			scope := fmt.Sprintf("%s.%s SEQUENCE", destSchema, sequenceName)
			advisoryLines := buildConstraintAdvisoryLines(scope, []schemacompat.ConstraintRepairSuggestion{suggestions[idx]})

			originalSchema, originalTable, originalDestTable := stcls.schema, stcls.table, stcls.destTable
			stcls.schema = destSchema
			stcls.table = sequenceName
			stcls.destTable = sequenceName
			if err := stcls.writeAdvisoryFixSql(advisoryLines, logThreadSeq); err != nil {
				global.Wlog.Error(fmt.Sprintf("(%d) Failed to write SEQUENCE advisory SQL for %s.%s: %v", logThreadSeq, sourceSchema, sequenceName, err))
			}
			stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
		}
	}
}

func queryMySQLTableLevelMetadata(db *sql.DB, schema, table string) (mysqlTableLevelMetadata, error) {
	var (
		collation sql.NullString
		charset   sql.NullString
		comment   sql.NullString
		rowFormat sql.NullString
		createOpt sql.NullString
	)

	query := `
SELECT t.TABLE_COLLATION, c.CHARACTER_SET_NAME, t.AUTO_INCREMENT, t.ROW_FORMAT, t.CREATE_OPTIONS, t.TABLE_COMMENT
FROM information_schema.TABLES t
LEFT JOIN information_schema.COLLATIONS c ON t.TABLE_COLLATION = c.COLLATION_NAME
WHERE t.TABLE_SCHEMA = ? AND t.TABLE_NAME = ?
`

	var runtimeNextAutoInc sql.NullInt64
	if err := db.QueryRow(query, schema, table).Scan(&collation, &charset, &runtimeNextAutoInc, &rowFormat, &createOpt, &comment); err != nil {
		return mysqlTableLevelMetadata{}, err
	}

	createStmt, err := queryMySQLCreateTableStatement(db, schema, table)
	if err != nil {
		return mysqlTableLevelMetadata{}, err
	}

	return mysqlTableLevelMetadata{
		TableCollation: collation.String,
		TableCharset:   charset.String,
		AutoIncrement:  extractExplicitMySQLTableAutoIncrementValue(createStmt),
		RowFormat:      rowFormat.String,
		CreateOptions:  createOpt.String,
		TableComment:   comment.String,
		CreateTableSQL: createStmt,
	}, nil
}

func nullInt64ForLog(v sql.NullInt64) interface{} {
	if !v.Valid {
		return nil
	}
	return v.Int64
}

func escapeMySQLCommentLiteral(v string) string {
	s := strings.ReplaceAll(v, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}

var mysqlCreateObjectCommentPattern = regexp.MustCompile(`(?is)\bCOMMENT\s+'((?:\\'|[^'])*)'`)
var mysqlTableAutoIncrementOptionPattern = regexp.MustCompile(`(?i)\)\s*ENGINE\s*=.*?\bAUTO_INCREMENT\s*=\s*([0-9]+)\b`)
var mysqlAlterTableStatementPattern = regexp.MustCompile("(?is)^\\s*ALTER\\s+TABLE\\s+((?:`[^`]+`\\.`[^`]+`)|(?:[^\\s]+))\\s+(.*?);?\\s*$")

func queryMySQLCreateTableStatement(db *sql.DB, schema, table string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", escapeMySQLIdentifier(schema), escapeMySQLIdentifier(table))
	var (
		objectName string
		createStmt string
	)
	if err := db.QueryRow(query).Scan(&objectName, &createStmt); err != nil {
		return "", err
	}
	return createStmt, nil
}

func extractExplicitMySQLTableAutoIncrementValue(createStmt string) sql.NullInt64 {
	matches := mysqlTableAutoIncrementOptionPattern.FindStringSubmatch(createStmt)
	if len(matches) < 2 {
		return sql.NullInt64{}
	}
	n, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: n, Valid: true}
}

type mysqlUniqueIndexMetadata struct {
	Name      string
	Columns   []string
	HasPrefix bool
	IsPrimary bool
	IsUnique  bool
}

func loadMySQLUniqueIndexMetadata(db *sql.DB, schema, table string) ([]mysqlUniqueIndexMetadata, error) {
	rows, err := db.Query(`
SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY INDEX_NAME, SEQ_IN_INDEX
`, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type rowItem struct {
		name      string
		nonUnique int
		seq       int
		column    string
		subPart   sql.NullInt64
	}

	grouped := make(map[string][]rowItem)
	order := make([]string, 0)
	for rows.Next() {
		var item rowItem
		if err := rows.Scan(&item.name, &item.nonUnique, &item.seq, &item.column, &item.subPart); err != nil {
			return nil, err
		}
		if _, ok := grouped[item.name]; !ok {
			order = append(order, item.name)
		}
		grouped[item.name] = append(grouped[item.name], item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make([]mysqlUniqueIndexMetadata, 0)
	for _, name := range order {
		items := grouped[name]
		if len(items) == 0 {
			continue
		}
		if items[0].nonUnique != 0 && !strings.EqualFold(name, "PRIMARY") {
			continue
		}
		sort.Slice(items, func(i, j int) bool { return items[i].seq < items[j].seq })
		meta := mysqlUniqueIndexMetadata{
			Name:      name,
			IsPrimary: strings.EqualFold(name, "PRIMARY"),
			IsUnique:  true,
		}
		for _, item := range items {
			meta.Columns = append(meta.Columns, item.column)
			if item.subPart.Valid {
				meta.HasPrefix = true
			}
		}
		result = append(result, meta)
	}
	return result, nil
}

func foreignKeyMatchesStrictUniqueIndex(fk schemacompat.CanonicalConstraint, indexes []mysqlUniqueIndexMetadata) bool {
	if len(fk.ReferencedColumns) == 0 {
		return false
	}
	for _, idx := range indexes {
		if idx.HasPrefix {
			continue
		}
		if len(idx.Columns) != len(fk.ReferencedColumns) {
			continue
		}
		match := true
		for i := range idx.Columns {
			if !strings.EqualFold(idx.Columns[i], fk.ReferencedColumns[i]) {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func detectStrictForeignKeyIssues(db *sql.DB, fks []schemacompat.CanonicalConstraint) ([]schemacompat.CanonicalConstraint, error) {
	cache := make(map[string][]mysqlUniqueIndexMetadata)
	issues := make([]schemacompat.CanonicalConstraint, 0)

	for _, fk := range fks {
		if fk.ReferencedSchema == "" || fk.ReferencedTable == "" {
			continue
		}
		cacheKey := strings.ToLower(fmt.Sprintf("%s.%s", fk.ReferencedSchema, fk.ReferencedTable))
		indexes, ok := cache[cacheKey]
		if !ok {
			loaded, err := loadMySQLUniqueIndexMetadata(db, fk.ReferencedSchema, fk.ReferencedTable)
			if err != nil {
				return nil, err
			}
			indexes = loaded
			cache[cacheKey] = indexes
		}
		if !foreignKeyMatchesStrictUniqueIndex(fk, indexes) {
			issues = append(issues, fk)
		}
	}

	sort.Slice(issues, func(i, j int) bool {
		left := fmt.Sprintf("%s:%s.%s", issues[i].Name, issues[i].ReferencedSchema, issues[i].ReferencedTable)
		right := fmt.Sprintf("%s:%s.%s", issues[j].Name, issues[j].ReferencedSchema, issues[j].ReferencedTable)
		return left < right
	})
	return issues, nil
}

func resolveMySQLTableAutoIncrementFixValue(sourceValue, destValue sql.NullInt64) (int64, bool) {
	if sourceValue.Valid == destValue.Valid {
		if !sourceValue.Valid {
			return 0, false
		}
		if sourceValue.Int64 == destValue.Int64 {
			return 0, false
		}
	}
	if sourceValue.Valid {
		return sourceValue.Int64, true
	}
	if destValue.Valid {
		return 0, true
	}
	return 0, false
}

func buildMySQLTableAutoIncrementAdvisory(destSchema, destTable string, sourceValue, destValue sql.NullInt64) (schemacompat.ConstraintRepairSuggestion, bool) {
	fixValue, needsFix := resolveMySQLTableAutoIncrementFixValue(sourceValue, destValue)
	if !needsFix {
		return schemacompat.ConstraintRepairSuggestion{}, false
	}

	suggestion := schemacompat.ConstraintRepairSuggestion{
		Kind:  "TABLE AUTO_INCREMENT",
		Level: schemacompat.ConstraintRepairLevelAdvisoryOnly,
		Reason: fmt.Sprintf(
			"table AUTO_INCREMENT next value differs between source and target (source=%v, target=%v); this drift does not change existing rows and should only be aligned if future inserts must continue from the source sequence",
			nullInt64ForLog(sourceValue),
			nullInt64ForLog(destValue),
		),
	}
	if sourceValue.Valid {
		suggestion.Statements = []string{
			fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT=%d;", destSchema, destTable, fixValue),
		}
	}
	return suggestion, true
}

func extractMySQLObjectCommentFromCreate(createSQL string) string {
	matches := mysqlCreateObjectCommentPattern.FindStringSubmatch(createSQL)
	if len(matches) < 2 {
		return ""
	}
	return normalizeMetadataComment(strings.ReplaceAll(matches[1], `\'`, `'`))
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

type triggerCreateResult struct {
	CreateSQL           string
	CharacterSetClient  string
	CollationConnection string
	DatabaseCollation   string
}

func showCreateTriggerSQL(db *sql.DB, schema, triggerName string) (string, error) {
	result, err := showCreateTriggerSQLWithCharset(db, schema, triggerName)
	if err != nil {
		return "", err
	}
	return result.CreateSQL, nil
}

func showCreateTriggerSQLWithCharset(db *sql.DB, schema, triggerName string) (triggerCreateResult, error) {
	row := db.QueryRow(
		`SELECT DEFINER, ACTION_TIMING, EVENT_MANIPULATION, EVENT_OBJECT_TABLE, ACTION_STATEMENT,
		        CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION
		   FROM INFORMATION_SCHEMA.TRIGGERS
		  WHERE TRIGGER_SCHEMA = ? AND TRIGGER_NAME = ?`,
		schema,
		triggerName,
	)

	var definer, actionTiming, eventManipulation, eventObjectTable, actionStatement string
	var csClient, colConnection, dbCollation sql.NullString
	if err := row.Scan(&definer, &actionTiming, &eventManipulation, &eventObjectTable, &actionStatement,
		&csClient, &colConnection, &dbCollation); err != nil {
		if err == sql.ErrNoRows {
			return triggerCreateResult{}, fmt.Errorf("no trigger metadata found for %s.%s", schema, triggerName)
		}
		return triggerCreateResult{}, err
	}

	createSQL := mysql.BuildTriggerCreateSQL(schema, triggerName, definer, actionTiming, eventManipulation, eventObjectTable, actionStatement)
	return triggerCreateResult{
		CreateSQL:           createSQL,
		CharacterSetClient:  strings.TrimSpace(csClient.String),
		CollationConnection: strings.TrimSpace(colConnection.String),
		DatabaseCollation:   strings.TrimSpace(dbCollation.String),
	}, nil
}

func loadMySQLTriggerMetadata(db *sql.DB, schema string, logThreadSeq int64) (map[string]string, map[string]string) {
	comments := make(map[string]string)
	definers := make(map[string]string)

	rows, err := db.Query(`
SELECT TRIGGER_NAME, DEFINER, ACTION_TIMING, EVENT_MANIPULATION, EVENT_OBJECT_TABLE, ACTION_STATEMENT
FROM INFORMATION_SCHEMA.TRIGGERS
WHERE TRIGGER_SCHEMA = ?
`, schema)
	if err != nil {
		global.Wlog.Warn(fmt.Sprintf("(%d) [loadMySQLTriggerMetadata] failed to query trigger metadata for %s: %v", logThreadSeq, schema, err))
		return comments, definers
	}
	defer rows.Close()

	for rows.Next() {
		var triggerName string
		var definer sql.NullString
		var actionTiming string
		var eventManipulation string
		var eventObjectTable string
		var actionStatement string

		if err := rows.Scan(&triggerName, &definer, &actionTiming, &eventManipulation, &eventObjectTable, &actionStatement); err != nil {
			global.Wlog.Warn(fmt.Sprintf("(%d) [loadMySQLTriggerMetadata] failed to scan trigger metadata for %s: %v", logThreadSeq, schema, err))
			continue
		}

		key := strings.ToUpper(fmt.Sprintf("\"%s\".\"%s\"", schema, triggerName))
		definers[key] = strings.TrimSpace(definer.String)

		// Build the trigger definition from INFORMATION_SCHEMA once so comment
		// extraction does not trigger an extra SHOW CREATE round-trip per row.
		createSQL := mysql.BuildTriggerCreateSQL(
			schema,
			triggerName,
			definer.String,
			actionTiming,
			eventManipulation,
			eventObjectTable,
			actionStatement,
		)
		comments[key] = extractMySQLObjectCommentFromCreate(createSQL)
	}

	if err := rows.Err(); err != nil {
		global.Wlog.Warn(fmt.Sprintf("(%d) [loadMySQLTriggerMetadata] row iteration failed for %s: %v", logThreadSeq, schema, err))
	}

	return comments, definers
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

func normalizeFixSQLForExec(stmt string) string {
	s := strings.TrimSpace(stmt)
	if s == "" {
		return ""
	}
	if strings.HasPrefix(s, "--") || strings.HasPrefix(s, "/*") {
		return ""
	}
	if strings.HasPrefix(strings.ToUpper(s), "DELIMITER ") {
		return ""
	}
	if strings.HasSuffix(s, "$$") {
		s = strings.TrimSpace(strings.TrimSuffix(s, "$$"))
	}
	return s
}

func (stcls *schemaTable) mappedDestSchema(sourceSchema string) string {
	if mappedSchema, exists := stcls.tableMappings[sourceSchema]; exists && strings.TrimSpace(mappedSchema) != "" {
		return mappedSchema
	}
	return sourceSchema
}

func parseSourceAndDestTablePair(mapping string, schemaMappings map[string]string) (string, string, string, string) {
	sourceSchema := ""
	sourceTable := ""
	destSchema := ""
	destTable := ""

	if strings.Contains(mapping, ":") {
		parts := strings.SplitN(mapping, ":", 2)
		if len(parts) == 2 {
			sourceParts := strings.SplitN(parts[0], ".", 2)
			destParts := strings.SplitN(parts[1], ".", 2)
			if len(sourceParts) == 2 {
				sourceSchema = sourceParts[0]
				sourceTable = sourceParts[1]
			}
			if len(destParts) == 2 {
				destSchema = destParts[0]
				destTable = destParts[1]
			}
		}
	}

	if sourceSchema == "" || sourceTable == "" {
		parts := strings.SplitN(mapping, ".", 2)
		if len(parts) == 2 {
			sourceSchema = parts[0]
			sourceTable = parts[1]
		}
	}

	if destSchema == "" {
		if mappedSchema, ok := schemaMappings[sourceSchema]; ok && strings.TrimSpace(mappedSchema) != "" {
			destSchema = mappedSchema
		} else {
			destSchema = sourceSchema
		}
	}
	if destTable == "" {
		destTable = sourceTable
	}

	return sourceSchema, sourceTable, destSchema, destTable
}

func buildConstraintAdvisoryLines(scope string, suggestions []schemacompat.ConstraintRepairSuggestion) []string {
	if len(suggestions) == 0 {
		return nil
	}

	lines := []string{
		fmt.Sprintf("-- gt-checksum advisory begin: %s", scope),
		"-- generated as manual review SQL only; these statements are not auto-executed by gt-checksum",
	}
	for _, suggestion := range suggestions {
		lines = append(lines, fmt.Sprintf("-- level: %s", suggestion.Level))
		lines = append(lines, fmt.Sprintf("-- kind: %s", suggestion.Kind))
		if suggestion.ConstraintName != "" {
			lines = append(lines, fmt.Sprintf("-- constraint: %s", suggestion.ConstraintName))
		}
		if suggestion.Reason != "" {
			lines = append(lines, fmt.Sprintf("-- reason: %s", suggestion.Reason))
		}
		if len(suggestion.Statements) == 0 {
			lines = append(lines, "-- suggested SQL: none")
			continue
		}
		for _, stmt := range suggestion.Statements {
			lines = append(lines, fmt.Sprintf("-- %s", strings.TrimSpace(stmt)))
		}
	}
	lines = append(lines, fmt.Sprintf("-- gt-checksum advisory end: %s", scope))
	return lines
}

func (stcls *schemaTable) writeAdvisoryFixSql(sqls []string, logThreadSeq int64) error {
	if len(sqls) == 0 {
		return nil
	}

	if !strings.EqualFold(stcls.datafix, "file") {
		global.Wlog.Warn(fmt.Sprintf("(%d) Constraint repair suggestions were generated but not executed. Use datafix=file to export advisory SQL.", logThreadSeq))
		return nil
	}

	objType := stcls.fixFileObjectType
	if objType == "" {
		objType = "table"
	}
	tableFileName := fmt.Sprintf("%s/%s.%s.%s.sql",
		stcls.datafixSql, objType,
		fixFileNameEncode(stcls.schema), fixFileNameEncode(stcls.table))
	file, err := os.OpenFile(tableFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open advisory fix file %s: %v", tableFileName, err)
	}
	defer file.Close()
	return mysql.WriteFixIfNeededFile(stcls.datafix, file, sqls, logThreadSeq, stcls.djdbc)
}

type alterTableMergeBucket struct {
	firstIndex int
	tableExpr  string
	clauses    []string
}

func parseAlterTableStatement(stmt string) (tableExpr string, clause string, ok bool) {
	matches := mysqlAlterTableStatementPattern.FindStringSubmatch(strings.TrimSpace(stmt))
	if len(matches) != 3 {
		return "", "", false
	}
	tableExpr = strings.TrimSpace(matches[1])
	clause = strings.TrimSpace(matches[2])
	if tableExpr == "" || clause == "" {
		return "", "", false
	}
	clause = strings.TrimSuffix(clause, ";")
	clause = strings.TrimSpace(clause)
	if clause == "" {
		return "", "", false
	}
	return tableExpr, clause, true
}

func alterTableMergeKey(tableExpr string) string {
	key := strings.ReplaceAll(strings.TrimSpace(tableExpr), "`", "")
	return strings.ToLower(key)
}

// mergeAlterTableStatements merges ALTER TABLE statements targeting the same table.
// It supports non-contiguous ALTER statements and keeps non-ALTER SQL ordering intact.
func mergeAlterTableStatements(sqls []string, logThreadSeq int64) []string {
	if len(sqls) <= 1 {
		return sqls
	}

	buckets := make(map[string]*alterTableMergeBucket)
	for idx, stmt := range sqls {
		tableExpr, clause, ok := parseAlterTableStatement(stmt)
		if !ok {
			continue
		}
		key := alterTableMergeKey(tableExpr)
		b, exists := buckets[key]
		if !exists {
			b = &alterTableMergeBucket{
				firstIndex: idx,
				tableExpr:  tableExpr,
			}
			buckets[key] = b
		}
		b.clauses = append(b.clauses, clause)
	}

	if len(buckets) == 0 {
		return sqls
	}

	merged := make([]string, 0, len(sqls))
	for idx, stmt := range sqls {
		tableExpr, _, ok := parseAlterTableStatement(stmt)
		if !ok {
			merged = append(merged, stmt)
			continue
		}
		key := alterTableMergeKey(tableExpr)
		b, exists := buckets[key]
		if !exists {
			merged = append(merged, stmt)
			continue
		}
		if idx != b.firstIndex {
			continue
		}
		combined := fmt.Sprintf("ALTER TABLE %s %s;", b.tableExpr, strings.Join(b.clauses, ", "))
		if len(b.clauses) > 1 {
			if global.Wlog != nil {
				global.Wlog.Debug(fmt.Sprintf("(%d) Merged %d ALTER TABLE statements for %s into one statement", logThreadSeq, len(b.clauses), b.tableExpr))
			}
		}
		merged = append(merged, combined)
	}
	return merged
}

// tableExistsByDrive reports whether an object exists in the given database.
// objectKind: "" or "table" → require TABLE_TYPE='BASE TABLE';
//
//	"view"  → require TABLE_TYPE='VIEW'.
//
// Oracle only queries all_tables (views are ignored, objectKind has no effect).
func (stcls *schemaTable) tableExistsByDrive(db *sql.DB, drive, schema, table, objectKind string) (bool, error) {
	var (
		count int
		query string
	)

	if isOracleDrive(drive) {
		query = fmt.Sprintf(
			"SELECT COUNT(1) FROM all_tables WHERE UPPER(owner)=UPPER('%s') AND UPPER(table_name)=UPPER('%s')",
			escapeSQLLiteral(schema),
			escapeSQLLiteral(table),
		)
	} else {
		tableTypeCond := " AND TABLE_TYPE='BASE TABLE'"
		if strings.ToLower(strings.TrimSpace(objectKind)) == "view" {
			tableTypeCond = " AND TABLE_TYPE='VIEW'"
		}
		if strings.ToLower(stcls.caseSensitiveObjectName) == "yes" {
			query = fmt.Sprintf(
				"SELECT COUNT(1) FROM information_schema.TABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'%s",
				escapeSQLLiteral(schema),
				escapeSQLLiteral(table),
				tableTypeCond,
			)
		} else {
			query = fmt.Sprintf(
				"SELECT COUNT(1) FROM information_schema.TABLES WHERE LOWER(TABLE_SCHEMA)=LOWER('%s') AND LOWER(TABLE_NAME)=LOWER('%s')%s",
				escapeSQLLiteral(schema),
				escapeSQLLiteral(table),
				tableTypeCond,
			)
		}
	}

	if err := db.QueryRow(query).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// splitTableViewEntries partitions a dtabS slice into BASE-TABLE entries and VIEW entries.
// Entries whose source part maps to "VIEW" in objectKinds are placed in viewEntries;
// everything else (or when objectKinds is empty) goes to tableEntries.
func splitTableViewEntries(dtabS []string, objectKinds map[string]string, caseSensitive string) (tableEntries, viewEntries []string) {
	for _, entry := range dtabS {
		srcPart := entry
		if idx := strings.Index(entry, ":"); idx >= 0 {
			srcPart = entry[:idx]
		}
		parts := strings.SplitN(srcPart, ".", 2)
		if len(parts) == 2 {
			key := fmt.Sprintf("%s/*schema&table*/%s", parts[0], parts[1])
			if strings.EqualFold(caseSensitive, "no") {
				key = strings.ToLower(key)
			}
			if objectKinds[key] == "VIEW" {
				viewEntries = append(viewEntries, entry)
				continue
			}
		}
		tableEntries = append(tableEntries, entry)
	}
	return
}

// queryMySQLCreateViewStatement runs SHOW CREATE VIEW and returns the raw DDL string.
func queryMySQLCreateViewStatement(db *sql.DB, schema, view string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE VIEW `%s`.`%s`", escapeMySQLIdentifier(schema), escapeMySQLIdentifier(view))
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if len(cols) < 2 {
		return "", fmt.Errorf("SHOW CREATE VIEW %s.%s: unexpected column count %d", schema, view, len(cols))
	}
	if !rows.Next() {
		return "", fmt.Errorf("SHOW CREATE VIEW %s.%s: no rows returned", schema, view)
	}
	dest := make([]interface{}, len(cols))
	raw := make([]sql.RawBytes, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return "", err
	}
	// Column index 1 is always "Create View"
	return string(raw[1]), rows.Err()
}

// queryMySQLViewCharsetMetadata queries INFORMATION_SCHEMA.VIEWS for the
// CHARACTER_SET_CLIENT and COLLATION_CONNECTION recorded when the view was created.
// These session-level values control the collation of view columns on recreation,
// so they must be re-applied when rebuilding the view on a target with different
// server defaults (e.g. MySQL 5.7 utf8mb4_general_ci → MySQL 8.0 utf8mb4_0900_ai_ci).
// Returns empty strings on error; callers treat empty strings as "unknown" and skip injection.
func queryMySQLViewCharsetMetadata(db *sql.DB, schema, view string) (csClient, colConn string) {
	row := db.QueryRow(
		`SELECT COALESCE(CHARACTER_SET_CLIENT,''), COALESCE(COLLATION_CONNECTION,'')
		   FROM INFORMATION_SCHEMA.VIEWS
		  WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
		schema, view,
	)
	var cs, col sql.NullString
	if err := row.Scan(&cs, &col); err != nil {
		return "", ""
	}
	return strings.TrimSpace(cs.String), strings.TrimSpace(col.String)
}

// normalizeViewCreateSQLForCompare normalises a SHOW CREATE VIEW DDL for comparison.
//
// Strategy:
//   - Strip the DEFINER clause entirely (account differences must not trigger Diffs=yes).
//   - Strip ALGORITHM=UNDEFINED (the default value; some MySQL versions include it in
//     SHOW CREATE VIEW output, others omit it — both are semantically identical).
//   - Strip SQL SECURITY clause (DEFINER/INVOKER); in migration scenarios this often
//     legitimately changes, so it must not trigger Diffs=yes on its own (cc §四).
//   - Collapse whitespace throughout.
//   - Lowercase only the header (CREATE … VIEW `name`), which contains only keywords and
//     backtick-quoted identifiers; the SELECT body is left in its original case to avoid
//     false-negatives or false-positives caused by string literals or column aliases.
//
// The header/body split is performed by viewHeaderBodyPattern which captures everything up
// to and including the last backtick-quoted VIEW identifier as group 1, and "AS <body>"
// as group 2.  If the pattern does not match (unexpected DDL format) the entire string is
// lowercased as a safe fallback.
func normalizeViewCreateSQLForCompare(createSQL string) string {
	// Step 1: strip DEFINER
	s := viewDefinerPattern.ReplaceAllString(createSQL, "")
	// Step 1b: strip ALGORITHM=UNDEFINED (default; equivalent to omitting ALGORITHM)
	s = viewAlgorithmUndefinedPattern.ReplaceAllString(s, "")
	// Step 1c: strip SQL SECURITY clause (migration-safe change; not a structural diff)
	s = viewSQLSecurityPattern.ReplaceAllString(s, "")
	// Step 2: collapse whitespace
	s = viewWhitespaceNormPattern.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)
	// Step 3: lowercase only the header, preserve SELECT body case
	if m := viewHeaderBodyPattern.FindStringSubmatch(s); len(m) == 3 {
		header := strings.ToLower(m[1])
		// Step 3b: strip optional schema prefix from the view identifier in the
		// header (e.g. `db1`.`v1` → `v1`), so that cross-schema mappings do not
		// produce false Diffs=yes when the two sides use different schema names.
		header = viewSchemaInHeaderPattern.ReplaceAllString(header, "$1")
		// Step 3c: strip redundant outer parentheses from the WHERE clause body.
		// MySQL 8.0 wraps the entire WHERE condition in parens ("where (expr)");
		// MariaDB / MySQL 5.7 do not.  Both are semantically identical.
		body := normalizeViewWhereOuterParens(m[2])
		return header + body
	}
	// Fallback: the DDL did not match expected format; lowercase everything.
	return strings.ToLower(s)
}

func effectiveViewSQLSecurity(createSQL string) string {
	if secMatch := viewExtractSQLSecurityPattern.FindStringSubmatch(createSQL); len(secMatch) == 2 {
		return strings.ToUpper(strings.TrimSpace(secMatch[1]))
	}
	// MySQL defaults VIEW SQL SECURITY to DEFINER when the clause is omitted.
	return "DEFINER"
}

func warnViewSQLSecurityDifference(logThreadSeq int64, sourceSchema, sourceViewName, srcCreateSQL, dstCreateSQL string) bool {
	srcSec := effectiveViewSQLSecurity(srcCreateSQL)
	dstSec := effectiveViewSQLSecurity(dstCreateSQL)
	if srcSec == dstSec {
		return false
	}
	if global.Wlog != nil {
		global.Wlog.Warn(fmt.Sprintf("(%d) [check_view_struct] VIEW %s.%s SQL SECURITY differs: src=%s dst=%s (not counted as Diffs=yes)",
			logThreadSeq, sourceSchema, sourceViewName, srcSec, dstSec))
	}
	return true
}

// queryMySQLViewColumnSignature queries INFORMATION_SCHEMA.COLUMNS for a view and returns
// a slice of canonical column descriptors ordered by ORDINAL_POSITION.  Each descriptor
// has the form "name|column_type|is_nullable|charset|collation".
//
// charset and collation are normalised to empty string when NULL (non-character columns).
// This lets the caller detect column-level metadata drift (type, nullability, charset,
// collation) independently of the CREATE VIEW DDL comparison.
func queryMySQLViewColumnSignature(db *sql.DB, schema, view string, caseSensitive string) ([]string, error) {
	var query string
	if strings.ToLower(strings.TrimSpace(caseSensitive)) == "yes" {
		query = fmt.Sprintf(
			"SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE,"+
				" COALESCE(CHARACTER_SET_NAME,''), COALESCE(COLLATION_NAME,'')"+
				" FROM INFORMATION_SCHEMA.COLUMNS"+
				" WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'"+
				" ORDER BY ORDINAL_POSITION",
			escapeSQLLiteral(schema), escapeSQLLiteral(view),
		)
	} else {
		query = fmt.Sprintf(
			"SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE,"+
				" COALESCE(CHARACTER_SET_NAME,''), COALESCE(COLLATION_NAME,'')"+
				" FROM INFORMATION_SCHEMA.COLUMNS"+
				" WHERE LOWER(TABLE_SCHEMA)=LOWER('%s') AND LOWER(TABLE_NAME)=LOWER('%s')"+
				" ORDER BY ORDINAL_POSITION",
			escapeSQLLiteral(schema), escapeSQLLiteral(view),
		)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sigs []string
	for rows.Next() {
		var colName, colType, isNullable, charset, collation string
		if err := rows.Scan(&colName, &colType, &isNullable, &charset, &collation); err != nil {
			return nil, err
		}
		// Normalise: lowercase column type and charset/collation names for comparison;
		// leave column name in original case (case-sensitive schemas may differ).
		if strings.EqualFold(caseSensitive, "no") {
			colName = strings.ToLower(colName)
		}
		sigs = append(sigs, fmt.Sprintf("%s|%s|%s|%s|%s",
			colName,
			strings.ToLower(colType),
			strings.ToUpper(isNullable), // YES / NO — normalise to upper
			strings.ToLower(charset),
			strings.ToLower(collation),
		))
	}
	return sigs, rows.Err()
}

// viewColumnSignaturesEqual returns true when src and dst have identical column signatures.
// It also returns a human-readable reason string when they differ (empty when equal).
func viewColumnSignaturesEqual(src, dst []string) (bool, string) {
	if len(src) != len(dst) {
		return false, fmt.Sprintf("column count differs (src=%d dst=%d)", len(src), len(dst))
	}
	for i := range src {
		if src[i] != dst[i] {
			return false, fmt.Sprintf("column[%d] differs: src=%q dst=%q", i, src[i], dst[i])
		}
	}
	return true, ""
}

// viewColumnSignaturesCollationOnly returns true when src and dst differ only in
// collation — not in column count, name, type, nullability, or charset.
//
// This pattern occurs in MySQL 5.7→8.0 migrations where the server-default utf8mb4
// collation changed from utf8mb4_general_ci to utf8mb4_0900_ai_ci.  VIEW columns
// reflect the underlying BASE TABLE column's collation at runtime (IS.COLUMNS is
// dynamic; SHOW CREATE VIEW's collation_connection is static metadata written at
// creation time and does not affect IS.COLUMNS).  Recreating the view with a
// different collation_connection does NOT change IS.COLUMNS for simple column
// references — only ALTERing the base table column fixes it.
//
// Callers should downgrade the severity from Diffs=yes to warn-only for this case,
// consistent with how table-struct collation drift is treated.
func viewColumnSignaturesCollationOnly(src, dst []string) bool {
	if len(src) != len(dst) {
		return false
	}
	anyDrift := false
	for i := range src {
		if src[i] == dst[i] {
			continue
		}
		// Signature format: colName|colType|isNullable|charset|collation
		srcP := strings.SplitN(src[i], "|", 5)
		dstP := strings.SplitN(dst[i], "|", 5)
		if len(srcP) != 5 || len(dstP) != 5 {
			return false // unparseable — treat as hard diff
		}
		// name, type, nullability, charset must all match
		if srcP[0] != dstP[0] || srcP[1] != dstP[1] || srcP[2] != dstP[2] || srcP[3] != dstP[3] {
			return false
		}
		// collation differs — this is allowed for collation-only drift
		anyDrift = true
	}
	return anyDrift
}

// buildViewColumnCollationDriftAdvisoryLines constructs an advisory block for the case
// where VIEW column signatures differ ONLY in collation (same type/nullability/charset).
//
// Root cause: IS.COLUMNS.COLLATION_NAME for VIEW columns is derived at runtime from the
// underlying BASE TABLE column's collation.  SHOW CREATE VIEW's collation_connection is
// static metadata written at creation time and has no effect on IS.COLUMNS for simple
// column references.  Recreating the view does NOT fix the IS.COLUMNS difference; only
// ALTERing the underlying table column collation does.
//
// Severity is downgraded to warn-only (not Diffs=yes) because this drift is a known
// MySQL 5.7→8.0 default-collation change (utf8mb4_general_ci→utf8mb4_0900_ai_ci) and
// is structurally equivalent to how table-level collation drift is treated.
func buildViewColumnCollationDriftAdvisoryLines(destSchema, viewName, diffReason string) []string {
	scope := fmt.Sprintf("%s.%s VIEW definition", destSchema, viewName)
	return []string{
		fmt.Sprintf("-- gt-checksum advisory begin: %s", scope),
		"-- generated as manual review note; no executable SQL is available",
		"-- level: warn-only",
		"-- kind: VIEW COLUMN COLLATION DRIFT",
		fmt.Sprintf("-- reason: %s", diffReason),
		"-- root-cause: VIEW column COLLATION_NAME in IS.COLUMNS reflects the underlying base-table",
		"--   column collation at runtime, not the VIEW's stored collation_connection metadata.",
		"--   On MySQL 5.7→8.0 migrations the default utf8mb4 collation changed from",
		"--   utf8mb4_general_ci to utf8mb4_0900_ai_ci, which propagates to all views over it.",
		"--   SHOW CREATE VIEW may show identical collation_connection on both sides but",
		"--   IS.COLUMNS still differs because the base-table columns have different collations.",
		"-- suggested fix: ALTER the underlying base-table column(s) to explicitly specify",
		"--   the target collation, then re-run checkObject=struct on the base table(s).",
		"--   Example: ALTER TABLE `<base_table>` MODIFY COLUMN `<col>` <type>",
		"--             CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;",
		fmt.Sprintf("-- gt-checksum advisory end: %s", scope),
	}
}

// buildViewColumnMetadataAdvisoryLines constructs an advisory block for the case where
// the normalised CREATE VIEW DDL is identical but column-level metadata (type, nullability,
// charset, collation) differs between source and destination.
//
// When srcCreateSQL and colConn are provided, the block contains executable SQL that sets
// the session collation to match the source before recreating the view.  This is the
// primary fix path for the MySQL 5.7→8.0 utf8mb4_general_ci→utf8mb4_0900_ai_ci drift.
// When csClient/colConn are empty the block falls back to "suggested SQL: none".
func buildViewColumnMetadataAdvisoryLines(destSchema, viewName, diffReason, srcCreateSQL, csClient, colConn string) []string {
	scope := fmt.Sprintf("%s.%s VIEW definition", destSchema, viewName)
	lines := []string{
		fmt.Sprintf("-- gt-checksum advisory begin: %s", scope),
		"-- generated as executable SQL; review before applying in the target session",
		"-- level: advisory-only",
		"-- kind: VIEW COLUMN METADATA",
		fmt.Sprintf("-- reason: column metadata drift — %s", diffReason),
	}
	createOrReplace, ok := buildCreateOrReplaceViewSQL(srcCreateSQL, destSchema, viewName)
	if ok && colConn != "" {
		if csClient != "" {
			lines = append(lines, fmt.Sprintf("SET character_set_client = %s;", csClient))
		}
		lines = append(lines,
			fmt.Sprintf("SET collation_connection = %s;", colConn),
			createOrReplace,
			"SET collation_connection = DEFAULT;",
		)
		if csClient != "" {
			lines = append(lines, "SET character_set_client = DEFAULT;")
		}
	} else {
		lines = append(lines, "-- suggested SQL: none")
	}
	lines = append(lines, fmt.Sprintf("-- gt-checksum advisory end: %s", scope))
	return lines
}

// buildCreateOrReplaceViewSQL transforms a SHOW CREATE VIEW DDL into a
// CREATE OR REPLACE VIEW statement targeting destSchema.destView.
// It strips the DEFINER clause, preserves explicit SQL SECURITY and any
// non-default ALGORITHM, collapses whitespace, and rewrites the header so the
// DBA can apply it directly to the destination database. The boolean result is
// true only when the rewrite is known-safe; otherwise callers must treat the
// suggestion as unavailable and avoid emitting executable-looking SQL.
func buildCreateOrReplaceViewSQL(srcCreateSQL, destSchema, destView string) (string, bool) {
	s := viewDefinerPattern.ReplaceAllString(srcCreateSQL, "")
	s = viewWhitespaceNormPattern.ReplaceAllString(strings.TrimSpace(s), " ")
	// Use the header/body split regex to separate "CREATE … VIEW `name`" from "AS …".
	if m := viewHeaderBodyPattern.FindStringSubmatch(s); len(m) == 3 {
		header := strings.TrimSpace(m[1])
		body := strings.TrimSpace(m[2]) // preserve SELECT body case and trailing CHECK OPTION

		algorithmClause := ""
		if algMatch := viewExtractAlgorithmPattern.FindStringSubmatch(header); len(algMatch) == 2 {
			alg := strings.ToUpper(strings.TrimSpace(algMatch[1]))
			if alg != "" && alg != "UNDEFINED" {
				algorithmClause = fmt.Sprintf(" ALGORITHM=%s", alg)
			}
		}

		securityClause := ""
		if secMatch := viewExtractSQLSecurityPattern.FindStringSubmatch(header); len(secMatch) == 2 {
			securityClause = fmt.Sprintf(" SQL SECURITY %s", strings.ToUpper(strings.TrimSpace(secMatch[1])))
		}

		return fmt.Sprintf("CREATE OR REPLACE%s%s VIEW `%s`.`%s` %s;",
			algorithmClause, securityClause, escapeMySQLIdentifier(destSchema), escapeMySQLIdentifier(destView), body), true
	}
	return "", false
}

// buildViewAdvisoryLines constructs the advisory SQL block for a VIEW difference
// (DDL mismatch or VIEW missing on destination).
//
// When csClient/colConn are non-empty (from INFORMATION_SCHEMA.VIEWS on the source),
// SET character_set_client / SET collation_connection statements are injected before
// the CREATE OR REPLACE VIEW so that the recreated view inherits the correct column
// collation metadata even when the target server has a different default collation
// (e.g. MySQL 5.7 utf8mb4_general_ci → MySQL 8.0 utf8mb4_0900_ai_ci).
//
// All SQL statements in the block are executable; only the surrounding metadata lines
// are comments.  The block is written to the advisory fix file for DBA review and
// sequential execution in a single session.
func buildViewAdvisoryLines(destSchema, viewName, srcCreateSQL, reason, csClient, colConn string) []string {
	scope := fmt.Sprintf("%s.%s VIEW definition", destSchema, viewName)
	createOrReplace, ok := buildCreateOrReplaceViewSQL(srcCreateSQL, destSchema, viewName)
	lines := []string{
		fmt.Sprintf("-- gt-checksum advisory begin: %s", scope),
		"-- generated as executable SQL; review before applying in the target session",
		"-- level: advisory-only",
		"-- kind: VIEW DEFINITION",
	}
	if ok {
		lines = append(lines, fmt.Sprintf("-- reason: %s", reason))
		if csClient != "" {
			lines = append(lines, fmt.Sprintf("SET character_set_client = %s;", csClient))
		}
		if colConn != "" {
			lines = append(lines, fmt.Sprintf("SET collation_connection = %s;", colConn))
		}
		lines = append(lines, createOrReplace)
		if colConn != "" {
			lines = append(lines, "SET collation_connection = DEFAULT;")
		}
		if csClient != "" {
			lines = append(lines, "SET character_set_client = DEFAULT;")
		}
	} else {
		lines = append(lines,
			fmt.Sprintf("-- reason: %s; unable to rewrite VIEW DDL safely", reason),
			"-- suggested SQL: none",
		)
	}
	lines = append(lines, fmt.Sprintf("-- gt-checksum advisory end: %s", scope))
	return lines
}

// buildViewDropAdvisoryLines constructs the advisory SQL block for the case where
// the VIEW exists on the destination but not on the source ("extra on target").
// The only safe suggestion is a DROP — no CREATE can be inferred.
func buildViewDropAdvisoryLines(destSchema, viewName string) []string {
	scope := fmt.Sprintf("%s.%s VIEW definition", destSchema, viewName)
	return []string{
		fmt.Sprintf("-- gt-checksum advisory begin: %s", scope),
		"-- generated as manual review SQL only; these statements are not auto-executed by gt-checksum",
		"-- level: advisory-only",
		"-- kind: VIEW DEFINITION",
		"-- reason: VIEW exists on target but not on source",
		fmt.Sprintf("-- DROP VIEW IF EXISTS `%s`.`%s`;", escapeMySQLIdentifier(destSchema), escapeMySQLIdentifier(viewName)),
		fmt.Sprintf("-- gt-checksum advisory end: %s", scope),
	}
}

// writeViewAdvisoryForDest temporarily sets stcls.table to destViewName, writes the
// advisory fix SQL, then restores stcls.table — regardless of whether
// writeAdvisoryFixSql succeeds or panics.  Using defer guarantees the restore
// even in exceptional code paths.
func (stcls *schemaTable) writeViewAdvisoryForDest(destViewName string, lines []string, logThreadSeq int64) error {
	orig := stcls.table
	origType := stcls.fixFileObjectType
	stcls.table = destViewName
	stcls.fixFileObjectType = "view"
	defer func() {
		stcls.table = orig
		stcls.fixFileObjectType = origType
	}()
	return stcls.writeAdvisoryFixSql(lines, logThreadSeq)
}

// checkViewStruct compares VIEW definitions between source and destination and appends
// Pod entries to measuredDataPods.  Advisory fix SQL is written when datafix=file.
// VIEW struct check is only performed for MySQL→MySQL; other drive combinations are skipped.
func (stcls *schemaTable) checkViewStruct(viewEntries []string, logThreadSeq, logThreadSeq2 int64) error {
	if len(viewEntries) == 0 {
		return nil
	}
	if stcls.sourceDrive != "mysql" || stcls.destDrive != "mysql" {
		global.Wlog.Warn(fmt.Sprintf("(%d) VIEW struct check skipped: only MySQL→MySQL is supported (src=%s, dst=%s)",
			logThreadSeq, stcls.sourceDrive, stcls.destDrive))
		return nil
	}

	fmt.Println("gt-checksum: Checking view definitions")
	global.Wlog.Info(fmt.Sprintf("(%d) [check_view_struct] checking view definitions of %v (num[%d])",
		logThreadSeq, viewEntries, len(viewEntries)))

	for _, entry := range viewEntries {
		sourceTable := entry
		destTable := entry
		if strings.Contains(entry, ":") {
			parts := strings.SplitN(entry, ":", 2)
			sourceTable = parts[0]
			destTable = parts[1]
		}
		srcParts := strings.SplitN(sourceTable, ".", 2)
		dstParts := strings.SplitN(destTable, ".", 2)
		if len(srcParts) < 2 || len(dstParts) < 2 {
			global.Wlog.Warn(fmt.Sprintf("(%d) [check_view_struct] skipping malformed entry: %s", logThreadSeq, entry))
			continue
		}
		sourceSchema, sourceViewName := srcParts[0], srcParts[1]
		destSchema, destViewName := dstParts[0], dstParts[1]

		pod := Pod{
			Datafix:     stcls.datafix,
			CheckObject: "struct",
			ObjectKind:  "view",
			Schema:      sourceSchema,
			Table:       sourceViewName,
		}
		if sourceSchema != destSchema {
			pod.MappingInfo = fmt.Sprintf("Schema: %s:%s", sourceSchema, destSchema)
		}

		srcExists, err := stcls.tableExistsByDrive(stcls.sourceDB, stcls.sourceDrive, sourceSchema, sourceViewName, "view")
		if err != nil {
			global.Wlog.Error(fmt.Sprintf("(%d) [check_view_struct] error checking source view %s.%s: %v",
				logThreadSeq, sourceSchema, sourceViewName, err))
			return err
		}
		dstExists, err := stcls.tableExistsByDrive(stcls.destDB, stcls.destDrive, destSchema, destViewName, "view")
		if err != nil {
			global.Wlog.Error(fmt.Sprintf("(%d) [check_view_struct] error checking dest view %s.%s: %v",
				logThreadSeq, destSchema, destViewName, err))
			return err
		}

		switch {
		case !srcExists && !dstExists:
			pod.DIFFS = global.SkipDiffsYes
			global.Wlog.Warn(fmt.Sprintf("(%d) [check_view_struct] VIEW %s.%s missing on both sides",
				logThreadSeq, sourceSchema, sourceViewName))
		case !srcExists:
			pod.DIFFS = global.SkipDiffsYes
			global.Wlog.Warn(fmt.Sprintf("(%d) [check_view_struct] VIEW %s.%s missing on source, advisory DROP generated",
				logThreadSeq, sourceSchema, sourceViewName))
			advisoryLines := buildViewDropAdvisoryLines(destSchema, destViewName)
			if wErr := stcls.writeViewAdvisoryForDest(destViewName, advisoryLines, logThreadSeq); wErr != nil {
				global.Wlog.Error(fmt.Sprintf("(%d) [check_view_struct] failed to write DROP advisory for %s.%s: %v",
					logThreadSeq, destSchema, destViewName, wErr))
			}
		case !dstExists:
			pod.DIFFS = global.SkipDiffsYes
			global.Wlog.Warn(fmt.Sprintf("(%d) [check_view_struct] VIEW %s.%s missing on destination",
				logThreadSeq, destSchema, destViewName))
			srcCreateSQL, qErr := queryMySQLCreateViewStatement(stcls.sourceDB, sourceSchema, sourceViewName)
			if qErr != nil {
				global.Wlog.Error(fmt.Sprintf("(%d) [check_view_struct] SHOW CREATE VIEW %s.%s failed: %v",
					logThreadSeq, sourceSchema, sourceViewName, qErr))
			} else {
				srcCSClient, srcColConn := queryMySQLViewCharsetMetadata(stcls.sourceDB, sourceSchema, sourceViewName)
				if stcls.isMariaDBToMySQL() {
					if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(srcColConn); ok {
						srcColConn = mapped
					}
				}
				advisoryLines := buildViewAdvisoryLines(destSchema, destViewName, srcCreateSQL, "VIEW missing on target", srcCSClient, srcColConn)
				if wErr := stcls.writeViewAdvisoryForDest(destViewName, advisoryLines, logThreadSeq); wErr != nil {
					global.Wlog.Error(fmt.Sprintf("(%d) [check_view_struct] failed to write advisory SQL for %s.%s: %v",
						logThreadSeq, destSchema, destViewName, wErr))
				}
			}
		default:
			// Both exist: compare normalised DDL, then column signatures.
			srcCreateSQL, qErr := queryMySQLCreateViewStatement(stcls.sourceDB, sourceSchema, sourceViewName)
			if qErr != nil {
				global.Wlog.Error(fmt.Sprintf("(%d) [check_view_struct] SHOW CREATE VIEW source %s.%s failed: %v",
					logThreadSeq, sourceSchema, sourceViewName, qErr))
				return qErr
			}
			dstCreateSQL, qErr := queryMySQLCreateViewStatement(stcls.destDB, destSchema, destViewName)
			if qErr != nil {
				global.Wlog.Error(fmt.Sprintf("(%d) [check_view_struct] SHOW CREATE VIEW dest %s.%s failed: %v",
					logThreadSeq, destSchema, destViewName, qErr))
				return qErr
			}
			srcNorm := normalizeViewCreateSQLForCompare(srcCreateSQL)
			dstNorm := normalizeViewCreateSQLForCompare(dstCreateSQL)
			ddlDiffers := srcNorm != dstNorm

			// SQL SECURITY warn log: emit a Warn so it is visible in logs even though
			// it does NOT count as Diffs=yes (e.g. DEFINER→INVOKER during migration).
			// Omitted clause is treated as the MySQL default "DEFINER", matching the
			// normalization logic used for DDL comparison.
			warnViewSQLSecurityDifference(logThreadSeq, sourceSchema, sourceViewName, srcCreateSQL, dstCreateSQL)

			// Fetch source charset/collation metadata once; reused by both advisory builders.
			// Errors are non-fatal: empty strings cause the SET injection to be skipped.
			srcCSClient, srcColConn := queryMySQLViewCharsetMetadata(stcls.sourceDB, sourceSchema, sourceViewName)
			if stcls.isMariaDBToMySQL() {
				if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(srcColConn); ok {
					srcColConn = mapped
				}
			}

			// Column-signature comparison (covers nullable/charset/collation drift that
			// SHOW CREATE VIEW may not surface, e.g. v_teststring-style cases).
			srcCols, colErr := queryMySQLViewColumnSignature(stcls.sourceDB, sourceSchema, sourceViewName, stcls.caseSensitiveObjectName)
			if colErr != nil {
				global.Wlog.Warn(fmt.Sprintf("(%d) [check_view_struct] column signature query failed for source %s.%s: %v (skipping column check)",
					logThreadSeq, sourceSchema, sourceViewName, colErr))
				srcCols = nil
			}
			dstCols, colErr := queryMySQLViewColumnSignature(stcls.destDB, destSchema, destViewName, stcls.caseSensitiveObjectName)
			if colErr != nil {
				global.Wlog.Warn(fmt.Sprintf("(%d) [check_view_struct] column signature query failed for dest %s.%s: %v (skipping column check)",
					logThreadSeq, destSchema, destViewName, colErr))
				dstCols = nil
			}
			colsEqual, colDiffReason := viewColumnSignaturesEqual(srcCols, dstCols)
			// colsEqual is vacuously true when either query failed (both nil slices have length 0).
			// Guard: treat nil result as "unknown" and do not trigger Diffs=yes on col side alone.
			colsDiffer := !colsEqual && srcCols != nil && dstCols != nil

			switch {
			case ddlDiffers:
				pod.DIFFS = global.SkipDiffsYes
				global.Wlog.Debug(fmt.Sprintf("(%d) [check_view_struct] VIEW %s.%s DDL differs\n  src: %s\n  dst: %s",
					logThreadSeq, sourceSchema, sourceViewName, srcNorm, dstNorm))
				advisoryLines := buildViewAdvisoryLines(destSchema, destViewName, srcCreateSQL, "VIEW definition differs", srcCSClient, srcColConn)
				if wErr := stcls.writeViewAdvisoryForDest(destViewName, advisoryLines, logThreadSeq); wErr != nil {
					global.Wlog.Error(fmt.Sprintf("(%d) [check_view_struct] failed to write advisory SQL for %s.%s: %v",
						logThreadSeq, destSchema, destViewName, wErr))
				}
			case colsDiffer:
				// DDL is identical but column-level metadata drifted.
				// Distinguish collation-only drift (warn-only) from hard differences
				// (type/nullability/charset changed → Diffs=yes).
				if viewColumnSignaturesCollationOnly(srcCols, dstCols) {
					// Collation-only drift: IS.COLUMNS reflects the BASE TABLE column's
					// collation at runtime.  Recreating the view does NOT fix this;
					// the underlying table column must be ALTERed.  Downgrade to warn-only.
					pod.DIFFS = global.SkipDiffsWarnOnly
					global.Wlog.Warn(fmt.Sprintf("(%d) [check_view_struct] VIEW %s.%s column collation drift (warn-only): %s",
						logThreadSeq, sourceSchema, sourceViewName, colDiffReason))
					advisoryLines := buildViewColumnCollationDriftAdvisoryLines(destSchema, destViewName, colDiffReason)
					if wErr := stcls.writeViewAdvisoryForDest(destViewName, advisoryLines, logThreadSeq); wErr != nil {
						global.Wlog.Error(fmt.Sprintf("(%d) [check_view_struct] failed to write collation drift advisory for %s.%s: %v",
							logThreadSeq, destSchema, destViewName, wErr))
					}
				} else {
					// Hard difference (type/nullability/charset changed): Diffs=yes.
					pod.DIFFS = global.SkipDiffsYes
					global.Wlog.Debug(fmt.Sprintf("(%d) [check_view_struct] VIEW %s.%s column metadata hard-differs: %s",
						logThreadSeq, sourceSchema, sourceViewName, colDiffReason))
					// IS.COLUMNS for VIEW columns reflects the underlying BASE TABLE column
					// definitions at runtime; rebuilding the VIEW cannot fix base-table
					// schema drift.  Fall back to advisory-only with "suggested SQL: none".
					advisoryLines := buildViewColumnMetadataAdvisoryLines(destSchema, destViewName, colDiffReason, "", "", "")
					if wErr := stcls.writeViewAdvisoryForDest(destViewName, advisoryLines, logThreadSeq); wErr != nil {
						global.Wlog.Error(fmt.Sprintf("(%d) [check_view_struct] failed to write column advisory for %s.%s: %v",
							logThreadSeq, destSchema, destViewName, wErr))
					}
				}
			default:
				pod.DIFFS = global.SkipDiffsNo
			}
		}

		measuredDataPods = append(measuredDataPods, pod)
	}
	return nil
}

/*
校验表的列名是否正确
*/
func (stcls *schemaTable) TableColumnNameCheck(checkTableList []string, logThreadSeq, logThreadSeq2 int64) ([]string, []string, error) {
	var (
		vlog                                 string
		newCheckTableList, abnormalTableList []string
		aa                                   = &CheckSumTypeStruct{}
		tableAbnormalBool                    = false
		event                                string
	)
	if stcls.structWarnOnlyDiffsMap == nil {
		stcls.structWarnOnlyDiffsMap = make(map[string]bool)
	}
	if stcls.structCollationMappedMap == nil {
		stcls.structCollationMappedMap = make(map[string]bool)
	}
	stcls.emitMariaDBSequenceWarnings(checkTableList, logThreadSeq)
	vlog = fmt.Sprintf("(%d) %s Validating structure differences between source and target", logThreadSeq, event)
	global.Wlog.Debug(vlog)
	for _, v := range checkTableList {
		// 处理可能存在的映射规则（格式：sourceSchema.sourceTable:destSchema.destTable）
		sourceTable := v
		destTable := v

		// 检查是否包含映射规则（是否包含":"字符）
		if strings.Contains(v, ":") {
			parts := strings.Split(v, ":")
			sourceTable = parts[0]
			destTable = parts[1]
		}

		// 从表列表中提取源端schema和表名
		sourceParts := strings.Split(sourceTable, ".")
		if len(sourceParts) < 2 {
			vlog = fmt.Sprintf("(%d) %s Invalid table format: %s, expected schema.table", logThreadSeq, event, sourceTable)
			global.Wlog.Error(vlog)
			continue
		}
		sourceSchema := sourceParts[0]
		sourceTableName := sourceParts[1]

		// 从表列表中提取目标端schema和表名
		destParts := strings.Split(destTable, ".")
		if len(destParts) < 2 {
			vlog = fmt.Sprintf("(%d) %s Invalid table format: %s, expected schema.table", logThreadSeq, event, destTable)
			global.Wlog.Error(vlog)
			continue
		}
		destSchema := destParts[0]
		destTableName := destParts[1]

		// 设置当前处理的表名
		stcls.schema = sourceSchema
		stcls.table = sourceTableName
		// 记录目标表名，用于后续操作
		stcls.destTable = destTableName

		// 如果没有明确的映射规则，则检查全局映射规则
		if sourceTable == destTable && sourceSchema == destSchema {
			if mappedSchema, exists := stcls.tableMappings[sourceSchema]; exists {
				destSchema = mappedSchema
			}
		}

		vlog = fmt.Sprintf("Table mapping options - source: %s, target: %s, mappings: %v", sourceSchema, destSchema, stcls.tableMappings)
		global.Wlog.Debug(vlog)
		mappedTableKey := fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)
		if sourceSchema != destSchema || sourceTableName != destTableName {
			mappedTableKey = fmt.Sprintf("%s.%s:%s.%s", sourceSchema, sourceTableName, destSchema, destTableName)
		}

		vlog = fmt.Sprintf("(%d %s Validating table structure %s.%s -> %s.%s", logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table)
		global.Wlog.Debug(vlog)

		// 检查源表和目标表是否存在（按驱动走不同元数据查询）
		sourceTableExists, err := stcls.tableExistsByDrive(stcls.sourceDB, stcls.sourceDrive, sourceSchema, sourceTableName, "table")
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Error checking source table existence %s.%s: %v", logThreadSeq, event, sourceSchema, sourceTableName, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}
		destTableExists, err := stcls.tableExistsByDrive(stcls.destDB, stcls.destDrive, destSchema, destTableName, "table")
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Error checking target table existence %s.%s: %v", logThreadSeq, event, destSchema, destTableName, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}

		oracleToMySQLDataMode := stcls.sourceDrive == "godror" && stcls.destDrive == "mysql" && stcls.checkRules.CheckObject != "struct"

		if !sourceTableExists || !destTableExists {
			if oracleToMySQLDataMode {
				diffReason := "table missing on one side"
				if !sourceTableExists && !destTableExists {
					diffReason = "table missing on both source and target"
				} else if !sourceTableExists {
					diffReason = "table missing on source"
				} else if !destTableExists {
					diffReason = "table missing on target"
				}
				pod := Pod{
					Schema:      sourceSchema,
					Table:       sourceTableName,
					CheckObject: "data",
					DIFFS:       "DDL-yes",
					Datafix:     stcls.datafix,
					Rows:        diffReason,
				}
				stcls.appendPod(pod)
				abnormalTableList = append(abnormalTableList, mappedTableKey)
				global.AddSkippedTableWithDiffs(sourceSchema, sourceTableName, "data", diffReason, global.SkipDiffsDDLYes)
				vlog = fmt.Sprintf("(%d) %s Skip data check for %s.%s due to DDL mismatch: %s", logThreadSeq, event, sourceSchema, sourceTableName, diffReason)
				global.Wlog.Warn(vlog)
				continue
			}

			if !sourceTableExists && !destTableExists {
				vlog = fmt.Sprintf("(%d) %s Source/target table both missing: %s.%s -> %s.%s", logThreadSeq, event, sourceSchema, sourceTableName, destSchema, destTableName)
				global.Wlog.Warn(vlog)
				pod := Pod{
					Schema:      sourceSchema,
					Table:       sourceTableName,
					CheckObject: stcls.checkRules.CheckObject,
					DIFFS:       "yes",
					Datafix:     stcls.datafix,
				}
				stcls.appendPod(pod)
				// Keep abnormalTableList entry so data-mode EvaluateDataCheckPreflight
				// correctly accounts this table as DDL-abnormal (SkipChecksum) rather
				// than triggering the fatal "No valid tables" branch.
				// Struct() uses a pod-snapshot guard to prevent a duplicate Pod entry.
				abnormalTableList = append(abnormalTableList, mappedTableKey)
				continue
			}

			if !sourceTableExists {
				vlog = fmt.Sprintf("(%d) %s Source table %s.%s does not exist", logThreadSeq, event, sourceSchema, sourceTableName)
				global.Wlog.Warn(vlog)
				global.AddSkippedTableWithDiffs(sourceSchema, sourceTableName, "data", "table does not exist", global.SkipDiffsDDLYes)
				pod := Pod{
					Schema:      sourceSchema,
					Table:       sourceTableName,
					CheckObject: stcls.checkRules.CheckObject,
					DIFFS:       "yes",
					Datafix:     stcls.datafix,
				}
				stcls.appendPod(pod)
				// Keep abnormalTableList entry for data-mode preflight accounting.
				// Struct() uses a pod-snapshot guard to prevent a duplicate Pod entry.
				abnormalTableList = append(abnormalTableList, mappedTableKey)
				continue
			}
		}

		// 处理特殊情况：源表存在但目标表不存在
		if sourceTableExists && !destTableExists {
			// 添加调试信息，确认schema映射处理
			vlog = fmt.Sprintf("(%d) %s Processing table creation with mapping - source: %s.%s -> dest: %s.%s", logThreadSeq, event, sourceSchema, sourceTableName, destSchema, destTableName)
			global.Wlog.Debug(vlog)

			sourceMeta, sourceMetaErr := queryMySQLTableLevelMetadata(stcls.sourceDB, sourceSchema, sourceTableName)
			if sourceMetaErr != nil {
				vlog = fmt.Sprintf("(%d) %s Failed to query source table metadata for %s.%s before CREATE TABLE generation: %v", logThreadSeq, event, sourceSchema, sourceTableName, sourceMetaErr)
				global.Wlog.Warn(vlog)
			} else {
				jsonDowngradeColumns := schemacompat.DetectMariaDBJSONDowngradeColumns(
					sourceMeta.CreateTableSQL,
					stcls.sourceVersionInfo(),
					stcls.destVersionInfo(),
					stcls.checkRules.MariaDBJSONTargetType,
				)
				if len(jsonDowngradeColumns) > 0 {
					advisoryLines := buildConstraintAdvisoryLines(
						fmt.Sprintf("%s.%s MariaDB JSON downgrade", destSchema, destTableName),
						schemacompat.BuildMariaDBJSONDowngradeSuggestions(destSchema, destTableName, jsonDowngradeColumns, stcls.checkRules.MariaDBJSONTargetType),
					)
					originalSchema, originalTable, originalDestTable := stcls.schema, stcls.table, stcls.destTable
					stcls.schema = destSchema
					stcls.table = destTableName
					stcls.destTable = destTableName
					if err = stcls.writeAdvisoryFixSql(advisoryLines, logThreadSeq); err != nil {
						stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
						return nil, nil, err
					}
					stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
				}

				// MariaDB-only temporal and sequence constructs must stay on the
				// advisory path because there is no safe automatic MySQL rewrite.
				unsupportedFeatures := schemacompat.DetectMariaDBUnsupportedTableFeatures(sourceMeta.CreateTableSQL, stcls.sourceVersionInfo(), stcls.destVersionInfo())
				if len(unsupportedFeatures) > 0 {
					vlog = fmt.Sprintf("(%d) %s Skip automatic CREATE TABLE for %s.%s because unsupported MariaDB features were detected: %+v", logThreadSeq, event, sourceSchema, sourceTableName, unsupportedFeatures)
					global.Wlog.Warn(vlog)

					advisoryLines := buildConstraintAdvisoryLines(
						fmt.Sprintf("%s.%s MariaDB unsupported features", destSchema, destTableName),
						schemacompat.BuildMariaDBUnsupportedFeatureSuggestions(destSchema, destTableName, unsupportedFeatures),
					)
					originalSchema, originalTable, originalDestTable := stcls.schema, stcls.table, stcls.destTable
					stcls.schema = destSchema
					stcls.table = destTableName
					stcls.destTable = destTableName
					if err = stcls.writeAdvisoryFixSql(advisoryLines, logThreadSeq); err != nil {
						stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
						return nil, nil, err
					}
					stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable

					stcls.appendPod(Pod{
						Schema:      sourceSchema,
						Table:       sourceTableName,
						CheckObject: stcls.checkRules.CheckObject,
						DIFFS:       global.SkipDiffsWarnOnly,
						Datafix:     stcls.datafix,
					})
					tableKey := fmt.Sprintf("%s.%s", destSchema, destTableName)
					stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)
					stcls.structWarnOnlyDiffsMap[fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)] = true
					continue
				}
			}

			// 生成CREATE TABLE语句
			createTableSql, err := generateCreateTableSql(stcls.sourceDB, sourceSchema, destSchema, sourceTableName, destTableName, stcls.sourceVersionInfo(), stcls.destVersionInfo(), stcls.checkRules.MariaDBJSONTargetType, logThreadSeq)
			if err != nil {
				vlog = fmt.Sprintf("(%d) %s Error generating CREATE TABLE statement for %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, err)
				global.Wlog.Error(vlog)
				return nil, nil, err
			}

			// 验证生成的CREATE TABLE语句是否包含正确的schema名
			if !strings.Contains(createTableSql, fmt.Sprintf("`%s`", destSchema)) {
				vlog = fmt.Sprintf("(%d) %s Warning: Generated CREATE TABLE statement may be missing target schema '%s': %s", logThreadSeq, event, destSchema, createTableSql)
				global.Wlog.Warn(vlog)
			}

			vlog = fmt.Sprintf("(%d) %s Generated CREATE TABLE statement for %s.%s: %s", logThreadSeq, event, destSchema, destTableName, createTableSql)
			global.Wlog.Debug(vlog)

			// 应用修复SQL
			vlog = fmt.Sprintf("(%d) %s Applying CREATE TABLE statement to %s.%s", logThreadSeq, event, destSchema, destTableName)
			global.Wlog.Debug(vlog)
			originalSchema, originalTable, originalDestTable := stcls.schema, stcls.table, stcls.destTable
			stcls.schema = destSchema
			stcls.table = destTableName
			stcls.destTable = destTableName
			if err = stcls.writeFixSql([]string{createTableSql}, logThreadSeq); err != nil {
				stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
				vlog = fmt.Sprintf("(%d) %s Error applying CREATE TABLE statement: %v", logThreadSeq, event, err)
				global.Wlog.Error(vlog)
				return nil, nil, err
			}
			stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable

			// 创建表示差异的Pod记录
			pod := Pod{
				Schema:      destSchema,
				Table:       destTableName,
				CheckObject: stcls.checkRules.CheckObject,
				DIFFS:       "yes",
				Datafix:     stcls.datafix,
			}
			stcls.appendPod(pod)

			abnormalTableList = append(abnormalTableList, fmt.Sprintf("%s.%s", destSchema, destTableName))

			// 重要：将此表标记为已处理，以防止后续的索引比较逻辑生成额外的ALTER语句
			tableKey := fmt.Sprintf("%s.%s", destSchema, destTableName)
			stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)

			continue
		}

		// 处理特殊情况：源表不存在但目标表存在
		if !sourceTableExists && destTableExists {
			// 生成DROP TABLE语句
			dropTableSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", destSchema, destTableName)

			vlog = fmt.Sprintf("(%d) %s Generated DROP TABLE statement for %s.%s: %s", logThreadSeq, event, destSchema, destTableName, dropTableSql)
			global.Wlog.Debug(vlog)

			// 应用修复SQL
			vlog = fmt.Sprintf("(%d) %s Applying DROP TABLE statement to %s.%s", logThreadSeq, event, destSchema, destTableName)
			global.Wlog.Debug(vlog)
			if err = stcls.writeFixSql([]string{dropTableSql}, logThreadSeq); err != nil {
				return nil, nil, err
			}

			// 将表添加到异常列表中
			abnormalTableList = append(abnormalTableList, fmt.Sprintf("%s.%s", destSchema, destTableName))

			// 重要：将此表标记为已处理，以防止后续的索引比较逻辑生成额外的DROP语句
			// 使用局部变量来跟踪需要删除的表
			tableKey := fmt.Sprintf("%s.%s", destSchema, destTableName)
			stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)

			continue
		}

		// 如果源表和目标表都存在，则继续原有的比较逻辑
		var sColumn, dColumn []map[string][]string

		dbf := dbExec.DataAbnormalFixStruct{
			Schema:                  destSchema, // 使用目标端schema
			Table:                   destTableName,
			DestDevice:              stcls.destDrive,
			DatafixType:             stcls.datafix,
			SourceSchema:            sourceSchema, // 添加源端schema
			CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
			DestFlavor:              stcls.destVersionInfo().Flavor,
		}
		tc := dbExec.TableColumnNameStruct{Schema: sourceSchema, Table: sourceTableName, Drive: stcls.sourceDrive, CaseSensitiveObjectName: stcls.caseSensitiveObjectName}
		sColumn, err = stcls.tableColumnName(stcls.sourceDB, tc, logThreadSeq, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Failed to get metadata for source table %s.%s: %v", logThreadSeq, event, sourceSchema, stcls.table, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}
		vlog = fmt.Sprintf("(%d) %s Source table %s.%s has %d columns", logThreadSeq, event, sourceSchema, stcls.table, len(sColumn))
		global.Wlog.Debug(vlog)

		// 使用目标端schema
		tcDest := dbExec.TableColumnNameStruct{Schema: destSchema, Table: destTableName, Drive: stcls.destDrive, CaseSensitiveObjectName: stcls.caseSensitiveObjectName}
		dColumn, err = stcls.tableColumnName(stcls.destDB, tcDest, logThreadSeq, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Failed to get metadata for target table %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}
		vlog = fmt.Sprintf("(%d) %s Target table %s.%s has %d columns", logThreadSeq, event, destSchema, stcls.table, len(dColumn))
		global.Wlog.Debug(vlog)

		sourcePartitionExpressions := loadTablePartitionExpressions(stcls.sourceDB, stcls.sourceDrive, sourceSchema, sourceTableName, stcls.caseSensitiveObjectName, logThreadSeq2)
		destPartitionExpressions := loadTablePartitionExpressions(stcls.destDB, stcls.destDrive, destSchema, destTableName, stcls.caseSensitiveObjectName, logThreadSeq2)
		partitionExpressions := append([]string{}, sourcePartitionExpressions...)
		partitionExpressions = append(partitionExpressions, destPartitionExpressions...)

		alterSlice := []string{}
		var sourceColumnSlice, destColumnSlice []string
		var sourceColumnMap, destColumnMap = make(map[string][]string), make(map[string][]string)
		var sourceColumnSeq, destColumnSeq = make(map[string]int), make(map[string]int)
		droppedAutoIncrementColumn := false
		// Keep source and target casing separately so later repair SQL can still
		// use the real target identifier even when compare keys are normalized.
		var sourceOriginalColumnNameMap = make(map[string]string)
		var destOriginalColumnNameMap = make(map[string]string)
		columnNameCaseSensitive := shouldUseCaseSensitiveColumnMatching(
			stcls.sourceDrive,
			stcls.destDrive,
			stcls.caseSensitiveObjectName,
			oracleToMySQLDataMode,
		)

		for k1, v1 := range sColumn {
			v1k := ""
			for k, v22 := range v1 {
				sourceOriginalColumnNameMap[strings.ToUpper(k)] = k

				// 根据匹配模式决定是使用原始列名还是大写列名进行比较
				if columnNameCaseSensitive {
					// 严格区分大小写，使用原始列名
					v1k = k
				} else {
					// 不区分大小写，统一使用大写键进行内部比较
					v1k = strings.ToUpper(k)
				}

				sourceColumnMap[v1k] = v22
				sourceColumnSeq[v1k] = k1
			}
			sourceColumnSlice = append(sourceColumnSlice, v1k)
		}
		for k1, v1 := range dColumn {
			v1k := ""
			for k, v22 := range v1 {
				destOriginalColumnNameMap[strings.ToUpper(k)] = k

				// 根据匹配模式决定是使用原始列名还是大写列名进行比较
				if columnNameCaseSensitive {
					// 严格区分大小写，使用原始列名
					v1k = k
				} else {
					// 不区分大小写，统一使用大写键进行内部比较
					v1k = strings.ToUpper(k)
				}

				destColumnMap[v1k] = v22
				destColumnSeq[v1k] = k1
			}
			destColumnSlice = append(destColumnSlice, v1k)
		}

		// 确保在生成SQL时使用原始大小写的列名
		// 创建一个函数来获取正确大小写的列名
		getSourceOriginalColumnName := func(colName string) string {
			if columnNameCaseSensitive {
				return colName
			}
			upperColName := strings.ToUpper(colName)
			if originalName, exists := sourceOriginalColumnNameMap[upperColName]; exists {
				return originalName
			}
			return colName
		}
		getDestOriginalColumnName := func(colName string) string {
			if columnNameCaseSensitive {
				return colName
			}
			upperColName := strings.ToUpper(colName)
			if originalName, exists := destOriginalColumnNameMap[upperColName]; exists {
				return originalName
			}
			if originalName, exists := sourceOriginalColumnNameMap[upperColName]; exists {
				return originalName
			}
			return colName
		}
		getTargetPositionColumnName := func(colName string) string {
			return getDestOriginalColumnName(colName)
		}

		addColumn, delColumn := aa.Arrcmp(sourceColumnSlice, destColumnSlice)

		// 检查是否只是列名大小写不同的情况
		// 当大小写敏感时，需要特殊处理大小写不同但实际上是同一列的情况
		if columnNameCaseSensitive {
			// 创建临时映射，用于存储大小写不敏感的列名比较
			var lowerSourceMap = make(map[string]string)
			var lowerDestMap = make(map[string]string)

			// 存储小写列名到原始列名的映射
			for _, col := range sourceColumnSlice {
				lowerSourceMap[strings.ToLower(col)] = col
			}
			for _, col := range destColumnSlice {
				lowerDestMap[strings.ToLower(col)] = col
			}

			// 查找只是大小写不同的列
			var caseOnlyDiffColumns []struct {
				sourceCol string
				destCol   string
			}

			// 检查addColumn和delColumn中是否有大小写对应的列
			for _, addCol := range addColumn {
				lowerAddCol := strings.ToLower(addCol)
				if destCol, exists := lowerDestMap[lowerAddCol]; exists {
					// 找到一个只是大小写不同的列
					caseOnlyDiffColumns = append(caseOnlyDiffColumns, struct {
						sourceCol string
						destCol   string
					}{sourceCol: addCol, destCol: destCol})
				}
			}

			// 从addColumn和delColumn中移除这些大小写不同的列
			var newAddColumn []string
			var newDelColumn []string

			// 创建一个集合来快速查找大小写不同的列
			caseDiffDestCols := make(map[string]bool)
			for _, colPair := range caseOnlyDiffColumns {
				caseDiffDestCols[colPair.destCol] = true
			}

			// 过滤addColumn，移除大小写不同的列
			for _, addCol := range addColumn {
				isCaseDiff := false
				for _, colPair := range caseOnlyDiffColumns {
					if addCol == colPair.sourceCol {
						isCaseDiff = true
						break
					}
				}
				if !isCaseDiff {
					newAddColumn = append(newAddColumn, addCol)
				}
			}

			// 过滤delColumn，移除大小写不同的列
			for _, delCol := range delColumn {
				if !caseDiffDestCols[delCol] {
					newDelColumn = append(newDelColumn, delCol)
				}
			}

			// 更新addColumn和delColumn
			addColumn = newAddColumn
			delColumn = newDelColumn

			// 为大小写不同的列生成CHANGE操作，并从destColumnMap中移除目标列
			// 同时将源列添加到destColumnMap中，避免后续代码重复处理
			for _, colPair := range caseOnlyDiffColumns {
				// 获取源列的定义
				if sourceDef, exists := sourceColumnMap[colPair.sourceCol]; exists {
					// 查找列的位置信息
					var position int
					var lastColumn string
					for i, col := range sourceColumnSlice {
						if col == colPair.sourceCol {
							position = i
							if i > 0 {
								lastColumn = sourceColumnSlice[i-1]
							} else {
								lastColumn = "alterNoAfter"
							}
							break
						}
					}

					// 生成CHANGE操作的SQL
					// 使用格式"原始列名:新列名"
					changeColName := fmt.Sprintf("%s:%s", colPair.destCol, colPair.sourceCol)
					changeSql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("change", sourceDef, position, lastColumn, changeColName, logThreadSeq)
					alterSlice = append(alterSlice, changeSql)

					vlog = fmt.Sprintf("(%d) %s Column %s only differs in case from %s, using CHANGE instead of DROP+ADD", logThreadSeq, event, colPair.destCol, colPair.sourceCol)
					global.Wlog.Info(vlog)

					// 从destColumnMap中移除目标列（旧列名）
					delete(destColumnMap, colPair.destCol)
					// 将源列（新列名）添加到destColumnMap中，避免后续代码重复处理
					destColumnMap[colPair.sourceCol] = sourceDef
					// 更新列的顺序信息
					destColumnSeq[colPair.sourceCol] = sourceColumnSeq[colPair.sourceCol]
				}
			}
		}

		// columns 模式：在 data 预检中，把用户已明确映射的列对从 add/delColumn 中豁免，
		// 避免因列重命名映射被误判为 DDL mismatch 而跳过数据校验。
		// struct 检查仍保留完整差异（用户可能确实想看重命名差异）。
		// 精确匹配当前表对，避免多表批次中误伤无关表（与 table_query_concurrency.go:239 保持一致）。
		if stcls.columnPlan != nil && stcls.checkRules.CheckObject != "struct" &&
			(stcls.columnPlan.SourceSchema == "" ||
				(strings.EqualFold(stcls.columnPlan.SourceSchema, sourceSchema) &&
					strings.EqualFold(stcls.columnPlan.SourceTable, sourceTableName) &&
					strings.EqualFold(stcls.columnPlan.TargetSchema, destSchema) &&
					strings.EqualFold(stcls.columnPlan.TargetTable, destTableName))) {
			addRemoveSet := make(map[string]bool)
			delRemoveSet := make(map[string]bool)
			for _, pair := range stcls.columnPlan.Pairs {
				srcUpper := strings.ToUpper(pair.SourceColumn)
				dstUpper := strings.ToUpper(pair.TargetColumn)
				for _, ac := range addColumn {
					if strings.ToUpper(ac) == srcUpper {
						addRemoveSet[ac] = true
						break
					}
				}
				for _, dc := range delColumn {
					if strings.ToUpper(dc) == dstUpper {
						delRemoveSet[dc] = true
						break
					}
				}
			}
			if len(addRemoveSet) > 0 || len(delRemoveSet) > 0 {
				filtered := addColumn[:0]
				for _, c := range addColumn {
					if !addRemoveSet[c] {
						filtered = append(filtered, c)
					}
				}
				addColumn = filtered
				filtered = delColumn[:0]
				for _, c := range delColumn {
					if !delRemoveSet[c] {
						filtered = append(filtered, c)
					}
				}
				delColumn = filtered
			}
		}

		// 移除对data类型的特殊处理，只处理struct类型的检查对象
		if stcls.checkRules.CheckObject != "struct" {
			addColumn, ignoredSourceHiddenColumns := filterIgnorableGeneratedInvisibleColumns(addColumn, sourceColumnMap)
			delColumn, ignoredTargetHiddenColumns := filterIgnorableGeneratedInvisibleColumns(delColumn, destColumnMap)
			if len(ignoredSourceHiddenColumns) > 0 || len(ignoredTargetHiddenColumns) > 0 {
				vlog = fmt.Sprintf("(%d) %s Ignoring generated invisible column differences for data precheck %s.%s -> %s.%s - ignored source extras: %v, ignored target missing: %v",
					logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table, ignoredSourceHiddenColumns, ignoredTargetHiddenColumns)
				global.Wlog.Info(vlog)
			}
			if len(addColumn) == 0 && len(delColumn) == 0 {
				// 使用目标端schema
				newCheckTableList = append(newCheckTableList, mappedTableKey)
			} else {
				// 检查是否包含INVISIBLE列的差异
				hasInvisibleColumns := false

				// 检查addColumn中是否有INVISIBLE列
				for _, col := range addColumn {
					if colDef, exists := sourceColumnMap[col]; exists && len(colDef) > 0 {
						for _, def := range colDef {
							if strings.Contains(strings.ToUpper(def), "INVISIBLE") || strings.Contains(strings.ToUpper(def), "/*80023 INVISIBLE */") {
								hasInvisibleColumns = true
								break
							}
						}
						if hasInvisibleColumns {
							break
						}
					}
				}

				// 使用正确的源和目标数据库名
				if hasInvisibleColumns {
					// 设置全局变量标记存在INVISIBLE列差异
					global.HasInvisibleColumnMismatch = true
					// 对于包含INVISIBLE列差异的情况，使用更明确的警告信息
					vlog = fmt.Sprintf("(%d) %s Structure mismatch with INVISIBLE columns %s.%s -> %s.%s - Extra: %v, Missing: %v. Data validation skipped.",
						logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table, addColumn, delColumn)
					global.Wlog.Warn(vlog)
					// 创建表结构检查记录，使用struct类型
					pod := Pod{
						Schema:      destSchema,
						Table:       stcls.table,
						CheckObject: "struct",
						DIFFS:       "DDL-yes",
						Datafix:     stcls.datafix,
					}
					stcls.appendPod(pod)
				} else {
					diffReason := fmt.Sprintf("DDL mismatch: Extra=%v, Missing=%v", addColumn, delColumn)
					vlog = fmt.Sprintf("(%d) %s Structure mismatch %s.%s -> %s.%s - Extra: %v, Missing: %v",
						logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table, addColumn, delColumn)
					global.Wlog.Warn(vlog)
					// 创建表结构检查记录，确保DDL不一致的表在报告中正确显示Diffs=yes
					pod := Pod{
						Schema:      sourceSchema,
						Table:       stcls.table,
						CheckObject: "data",
						DIFFS:       "DDL-yes",
						Datafix:     stcls.datafix,
						Rows:        diffReason,
					}
					stcls.appendPod(pod)
					global.AddSkippedTableWithDiffs(sourceSchema, stcls.table, "data", diffReason, global.SkipDiffsDDLYes)
				}
				abnormalTableList = append(abnormalTableList, mappedTableKey)
			}
			// 无论checkObject设置如何，都只生成struct类型的记录，避免重复
			continue
		}

		columnAdvisorySuggestions := make([]schemacompat.ConstraintRepairSuggestion, 0)
		columnCollationRepairCandidates := make([]columnCollationRepairCandidate, 0)
		columnRiskDifferent := false
		useCanonicalCompare := strings.EqualFold(stcls.sourceDrive, "mysql") && strings.EqualFold(stcls.destDrive, "mysql")
		sourceCreateSQL := ""
		destCreateSQL := ""
		sourceColumnDefinitions := make(map[string]string)
		destColumnDefinitions := make(map[string]string)
		if useCanonicalCompare {
			if sourceCreateSQL, err = queryMySQLCreateTableStatement(stcls.sourceDB, sourceSchema, stcls.table); err != nil {
				vlog = fmt.Sprintf("(%d) %s Failed to query source SHOW CREATE TABLE for %s.%s: %v", logThreadSeq, event, sourceSchema, stcls.table, err)
				global.Wlog.Warn(vlog)
				sourceCreateSQL = ""
			} else {
				sourceColumnDefinitions = schemacompat.ExtractColumnDefinitionsFromCreateSQL(sourceCreateSQL)
			}
			if destCreateSQL, err = queryMySQLCreateTableStatement(stcls.destDB, destSchema, stcls.destTable); err != nil {
				vlog = fmt.Sprintf("(%d) %s Failed to query target SHOW CREATE TABLE for %s.%s: %v", logThreadSeq, event, destSchema, stcls.destTable, err)
				global.Wlog.Warn(vlog)
				destCreateSQL = ""
			} else {
				destColumnDefinitions = schemacompat.ExtractColumnDefinitionsFromCreateSQL(destCreateSQL)
			}
		}

		vlog = fmt.Sprintf("(%d) %s Columns to remove from target %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, delColumn)
		global.Wlog.Debug(vlog)
		// 先删除缺失的
		if len(delColumn) > 0 {
			// 收集所有需要删除的列名
			var colsToDelete []string
			for _, v1 := range delColumn {
				if hasAutoIncrementColumnAttribute(destColumnMap[v1]) {
					droppedAutoIncrementColumn = true
				}
				// 使用原始大小写的列名生成SQL
				originalColName := getDestOriginalColumnName(v1)
				dropSql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("drop", destColumnMap[v1], 1, "", originalColName, logThreadSeq)
				alterSlice = append(alterSlice, dropSql)
				colsToDelete = append(colsToDelete, v1)
			}
			// 在循环外删除所有标记的列
			for _, col := range colsToDelete {
				delete(destColumnMap, col)
			}
		}
		vlog = fmt.Sprintf("(%d) %s DROP SQL for %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, alterSlice)
		global.Wlog.Debug(vlog)
		for k1, v1 := range sourceColumnSlice {
			lastcolumn := ""
			var alterColumnData []string
			if k1 == 0 {
				lastcolumn = sourceColumnSlice[k1]
			} else {
				lastcolumn = sourceColumnSlice[k1-1]
			}
			// 始终使用src作为修复规则
			alterColumnData = sourceColumnMap[v1]
			if _, ok := destColumnMap[v1]; ok {
				// 直接使用strict模式，删除了永远不会执行的loose分支
				// 使用固定值：ScheckMod=strict
				// 严格比较列的所有属性
				tableAbnormalBool = false

				// 比较列类型
				sourceType := ""
				destType := ""
				if len(sourceColumnMap[v1]) > 0 {
					sourceType = sourceColumnMap[v1][0]
				}
				if len(destColumnMap[v1]) > 0 {
					destType = destColumnMap[v1][0]
				}

				sourceOriginalColName := getSourceOriginalColumnName(v1)
				destOriginalColName := getDestOriginalColumnName(v1)
				repairColumnName := destOriginalColName
				if strings.TrimSpace(repairColumnName) == "" {
					repairColumnName = sourceOriginalColName
				}
				var sourceCanonical schemacompat.CanonicalColumn
				var destCanonical schemacompat.CanonicalColumn
				if useCanonicalCompare {
					sourceCanonical = schemacompat.CanonicalizeColumnForComparison(
						sourceOriginalColName,
						sourceColumnMap[v1],
						stcls.sourceVersionInfo(),
						stcls.destVersionInfo(),
						sourceColumnDefinitions[sourceOriginalColName],
						stcls.checkRules.MariaDBJSONTargetType,
					)
					destCanonical = schemacompat.CanonicalizeColumnForComparison(
						destOriginalColName,
						destColumnMap[v1],
						stcls.destVersionInfo(),
						stcls.sourceVersionInfo(),
						destColumnDefinitions[destOriginalColName],
						stcls.checkRules.MariaDBJSONTargetType,
					)
				}

				// 打印调试信息
				vlog = fmt.Sprintf("(%d) %s Column %s type comparison: source=%s, dest=%s", logThreadSeq, event, repairColumnName, sourceType, destType)
				global.Wlog.Debug(vlog)

				// 比较列类型
				if useCanonicalCompare {
					decision := schemacompat.DecideColumnDefinitionCompatibility(sourceCanonical, destCanonical)
					if decision.IsMismatch() {
						if shouldDeferPartitionKeyColumnRepair(partitionExpressions, decision, sourceOriginalColName, destOriginalColName) {
							vlog = fmt.Sprintf("(%d) %s Column %s definition mismatch requires manual review because it participates in the partition expression: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceType, destType, decision.Reason)
							global.Wlog.Warn(vlog)
							columnRiskDifferent = true
							columnAdvisorySuggestions = append(columnAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
								ConstraintName: repairColumnName,
								Kind:           "PARTITION KEY COLUMN",
								Level:          schemacompat.ConstraintRepairLevelAdvisoryOnly,
								Reason:         fmt.Sprintf("partition key column requires manual review: %s", decision.Reason),
							})
						} else if decision.State == schemacompat.CompatibilityWarnOnly {
							vlog = fmt.Sprintf("(%d) %s Column %s definition warning: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceType, destType, decision.Reason)
							global.Wlog.Warn(vlog)
							columnRiskDifferent = true
							columnAdvisorySuggestions = append(columnAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
								ConstraintName: repairColumnName,
								Kind:           "COLUMN ATTRIBUTE",
								Level:          schemacompat.ConstraintRepairLevelAdvisoryOnly,
								Reason:         decision.Reason,
							})
						} else {
							tableAbnormalBool = true
							vlog = fmt.Sprintf("(%d) %s Column %s definition mismatch: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceType, destType, decision.Reason)
							global.Wlog.Warn(vlog)
						}
					} else if decision.State == schemacompat.CompatibilityNormalizedEqual {
						vlog = fmt.Sprintf("(%d) %s Column %s definition normalized-equal: source=%s, dest=%s, reason=%s",
							logThreadSeq, event, repairColumnName, sourceType, destType, decision.Reason)
						global.Wlog.Debug(vlog)
					}
				} else if sourceType != destType {
					tableAbnormalBool = true
					vlog = fmt.Sprintf("(%d) %s Column %s type mismatch: source=%s, dest=%s", logThreadSeq, event, repairColumnName, sourceType, destType)
					global.Wlog.Warn(vlog)
				}

				// 比较字符集
				sourceCharset := ""
				destCharset := ""
				if len(sourceColumnMap[v1]) > 1 {
					sourceCharset = sourceColumnMap[v1][1]
				}
				if len(destColumnMap[v1]) > 1 {
					destCharset = destColumnMap[v1][1]
				}

				// 如果两者都不为空或null，则比较
				if (sourceCharset != "null" && sourceCharset != "") ||
					(destCharset != "null" && destCharset != "") {
					if useCanonicalCompare {
						decision := schemacompat.DecideColumnCharsetCompatibility(sourceCanonical, destCanonical)
						if shouldDeferPartitionKeyColumnRepair(partitionExpressions, decision, sourceOriginalColName, destOriginalColName) {
							vlog = fmt.Sprintf("(%d) %s Column %s charset mismatch requires manual review because it participates in the partition expression: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCharset, destCharset, decision.Reason)
							global.Wlog.Warn(vlog)
							columnRiskDifferent = true
							columnAdvisorySuggestions = append(columnAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
								ConstraintName: repairColumnName,
								Kind:           "PARTITION KEY COLUMN",
								Level:          schemacompat.ConstraintRepairLevelAdvisoryOnly,
								Reason:         fmt.Sprintf("partition key column requires manual review: %s", decision.Reason),
							})
						} else if decision.IsMismatch() {
							tableAbnormalBool = true
							vlog = fmt.Sprintf("(%d) %s Column %s charset mismatch: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCharset, destCharset, decision.Reason)
							global.Wlog.Warn(vlog)
						} else if decision.State == schemacompat.CompatibilityNormalizedEqual {
							vlog = fmt.Sprintf("(%d) %s Column %s charset normalized-equal: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCharset, destCharset, decision.Reason)
							global.Wlog.Debug(vlog)
						}
					} else if sourceCharset != destCharset {
						tableAbnormalBool = true
						vlog = fmt.Sprintf("(%d) %s Column %s charset mismatch: source=%s, dest=%s",
							logThreadSeq, event, repairColumnName, sourceCharset, destCharset)
						global.Wlog.Warn(vlog)
					}
				}

				// 比较排序规则
				sourceCollation := ""
				destCollation := ""
				if len(sourceColumnMap[v1]) > 2 {
					sourceCollation = sourceColumnMap[v1][2]
				}
				if len(destColumnMap[v1]) > 2 {
					destCollation = destColumnMap[v1][2]
				}

				// 如果两者都不为空或null，则比较
				if (sourceCollation != "null" && sourceCollation != "") ||
					(destCollation != "null" && destCollation != "") {
					if useCanonicalCompare {
						decision := schemacompat.DecideColumnCollationCompatibility(sourceCanonical, destCanonical)
						// MariaDB→MySQL：非 MariaDB 特有的 collation 在 MySQL 中合法存在，视为真实差异
						if decision.State == schemacompat.CompatibilityWarnOnly && stcls.isMariaDBToMySQL() {
							if _, isMappable := schemacompat.MapMariaDBCollationToMySQL(sourceCollation); !isMappable {
								decision.State = schemacompat.CompatibilityUnsupported
								decision.Reason = fmt.Sprintf("cross-platform collation mismatch: source=%s is valid in MySQL but differs from target=%s",
									sourceCollation, destCollation)
							}
						}
						if shouldDeferPartitionKeyColumnRepair(partitionExpressions, decision, sourceOriginalColName, destOriginalColName) {
							vlog = fmt.Sprintf("(%d) %s Column %s collation mismatch requires manual review because it participates in the partition expression: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCollation, destCollation, decision.Reason)
							global.Wlog.Warn(vlog)
							columnRiskDifferent = true
							columnAdvisorySuggestions = append(columnAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
								ConstraintName: repairColumnName,
								Kind:           "PARTITION KEY COLUMN",
								Level:          schemacompat.ConstraintRepairLevelAdvisoryOnly,
								Reason:         fmt.Sprintf("partition key column requires manual review: %s", decision.Reason),
							})
						} else if decision.State == schemacompat.CompatibilityWarnOnly {
							vlog = fmt.Sprintf("(%d) %s Column %s collation warning: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCollation, destCollation, decision.Reason)
							global.Wlog.Warn(vlog)
							// 如果该列已因类型/定义差异被标记为 tableAbnormalBool=true，
							// 后续会生成包含正确 charset/collation 的 MODIFY，无需重复加入 collation repair candidates
							if tableAbnormalBool {
								vlog = fmt.Sprintf("(%d) %s Column %s collation drift skipped from repair candidates: already covered by definition mismatch repair",
									logThreadSeq, event, repairColumnName)
								global.Wlog.Debug(vlog)
							} else {
								columnCollationRepairCandidates = append(columnCollationRepairCandidates, columnCollationRepairCandidate{
									ColumnName:       repairColumnName,
									ColumnSeq:        k1,
									LastColumn:       getTargetPositionColumnName(lastcolumn),
									SourceAttrs:      append([]string(nil), alterColumnData...),
									SourceDefinition: sourceColumnDefinitions[sourceOriginalColName],
									SourceCharset:    sourceCharset,
									SourceCollation:  sourceCollation,
									DestCharset:      destCharset,
									DestCollation:    destCollation,
									Reason:           decision.Reason,
								})
							}
						} else if decision.IsMismatch() {
							tableAbnormalBool = true
							vlog = fmt.Sprintf("(%d) %s Column %s collation mismatch: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCollation, destCollation, decision.Reason)
							global.Wlog.Warn(vlog)
						} else if decision.State == schemacompat.CompatibilityNormalizedEqual {
							vlog = fmt.Sprintf("(%d) %s Column %s collation normalized-equal: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceCollation, destCollation, decision.Reason)
							global.Wlog.Debug(vlog)
						}
					} else if sourceCollation != destCollation {
						tableAbnormalBool = true
						vlog = fmt.Sprintf("(%d) %s Column %s collation mismatch: source=%s, dest=%s",
							logThreadSeq, event, repairColumnName, sourceCollation, destCollation)
						global.Wlog.Warn(vlog)
					}
				}

				// 比较是否允许NULL
				sourceIsNull := ""
				destIsNull := ""
				if len(sourceColumnMap[v1]) > 3 {
					sourceIsNull = sourceColumnMap[v1][3]
				}
				if len(destColumnMap[v1]) > 3 {
					destIsNull = destColumnMap[v1][3]
				}

				if sourceIsNull != destIsNull {
					tableAbnormalBool = true
					vlog = fmt.Sprintf("(%d) %s Column %s NULL constraint mismatch: source=%s, dest=%s",
						logThreadSeq, event, repairColumnName, sourceIsNull, destIsNull)
					global.Wlog.Warn(vlog)
				}

				// 比较默认值
				sourceDefault := ""
				destDefault := ""
				if len(sourceColumnMap[v1]) > 4 {
					sourceDefault = sourceColumnMap[v1][4]
				}
				if len(destColumnMap[v1]) > 4 {
					destDefault = destColumnMap[v1][4]
				}

				// 如果两者都不为null，则比较
				if sourceDefault != "null" && destDefault != "null" {
					if useCanonicalCompare {
						decision := schemacompat.DecideColumnDefaultCompatibility(sourceCanonical, destCanonical)
						if decision.IsMismatch() {
							tableAbnormalBool = true
							vlog = fmt.Sprintf("(%d) %s Column %s default value mismatch: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceDefault, destDefault, decision.Reason)
							global.Wlog.Warn(vlog)
						} else if decision.State == schemacompat.CompatibilityNormalizedEqual {
							vlog = fmt.Sprintf("(%d) %s Column %s default value normalized-equal: source=%s, dest=%s, reason=%s",
								logThreadSeq, event, repairColumnName, sourceDefault, destDefault, decision.Reason)
							global.Wlog.Debug(vlog)
						}
					} else if sourceDefault != destDefault {
						tableAbnormalBool = true
						vlog = fmt.Sprintf("(%d) %s Column %s default value mismatch: source=%s, dest=%s",
							logThreadSeq, event, repairColumnName, sourceDefault, destDefault)
						global.Wlog.Warn(vlog)
					}
				}

				// 仅在 MySQL -> MySQL 场景比较列注释
				if stcls.isMySQLToMySQL() {
					sourceComment := ""
					destComment := ""
					if len(sourceColumnMap[v1]) > 5 {
						sourceComment = normalizeMetadataComment(sourceColumnMap[v1][5])
					}
					if len(destColumnMap[v1]) > 5 {
						destComment = normalizeMetadataComment(destColumnMap[v1][5])
					}
					if sourceComment != destComment {
						tableAbnormalBool = true
						vlog = fmt.Sprintf("(%d) %s Column %s comment mismatch: source=%q, dest=%q",
							logThreadSeq, event, repairColumnName, sourceComment, destComment)
						global.Wlog.Warn(vlog)
					}
				}

				// 比较列顺序
				// 注意：当添加一个自增列作为主键并使用FIRST关键字时，其他列的顺序自然会被调整
				// 因此需要检查是否有添加自增列的操作，如果有，跳过因为这个原因导致的列顺序不匹配
				hasAutoIncrementPrimaryKeyAdd := false
				for _, alterOp := range alterSlice {
					if strings.Contains(strings.ToUpper(alterOp), "ADD COLUMN") &&
						strings.Contains(strings.ToUpper(alterOp), "AUTO_INCREMENT") &&
						strings.Contains(strings.ToUpper(alterOp), "PRIMARY KEY") &&
						strings.Contains(strings.ToUpper(alterOp), "FIRST") {
						hasAutoIncrementPrimaryKeyAdd = true
						break
					}
				}

				if !hasAutoIncrementPrimaryKeyAdd && sourceColumnSeq[v1] != destColumnSeq[v1] {
					tableAbnormalBool = true
					vlog = fmt.Sprintf("(%d) %s Column %s sequence mismatch: source=%d, dest=%d",
						logThreadSeq, event, repairColumnName, sourceColumnSeq[v1], destColumnSeq[v1])
					global.Wlog.Warn(vlog)
				}
				if tableAbnormalBool {
					sourceOriginalColName := getSourceOriginalColumnName(v1)
					repairColumnName := getDestOriginalColumnName(v1)
					if strings.TrimSpace(repairColumnName) == "" {
						repairColumnName = sourceOriginalColName
					}
					originalLastColumn := getTargetPositionColumnName(lastcolumn)
					repairAttrs := append([]string(nil), alterColumnData...)
					if useCanonicalCompare {
						repairPlan := schemacompat.BuildTargetColumnRepairPlan(
							sourceOriginalColName,
							repairAttrs,
							stcls.sourceVersionInfo(),
							stcls.destVersionInfo(),
							sourceColumnDefinitions[sourceOriginalColName],
							stcls.checkRules.MariaDBJSONTargetType,
						)
						if len(repairAttrs) < 6 {
							for len(repairAttrs) < 6 {
								repairAttrs = append(repairAttrs, "null")
							}
						}
						if strings.TrimSpace(repairPlan.Type) != "" {
							repairAttrs[0] = repairPlan.Type
						}
						if strings.TrimSpace(repairPlan.Charset) != "" {
							repairAttrs[1] = repairPlan.Charset
						}
						if strings.TrimSpace(repairPlan.Collation) != "" {
							repairAttrs[2] = repairPlan.Collation
						}
						if repairPlan.UseDirectDefinition {
							if len(repairAttrs) < 7 {
								repairAttrs = append(repairAttrs, repairPlan.DirectDefinition)
							} else {
								repairAttrs[6] = repairPlan.DirectDefinition
							}
						}
					}
					// 检查目标表是否存在主键
					if mysqlDataFix, ok := dbf.DataAbnormalFix().(*mysql.MysqlDataAbnormalFixStruct); ok {
						mysqlDataFix.CheckDestTableHasPrimaryKey(stcls.destDB, logThreadSeq)
					}
					modifySql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("modify", repairAttrs, k1, originalLastColumn, repairColumnName, logThreadSeq)
					if suggestion, gated := stcls.buildColumnShrinkAdvisory(destSchema, stcls.destTable, repairColumnName, sourceCanonical, destCanonical, modifySql); gated {
						vlog = fmt.Sprintf("(%d) %s Column %s modify repair downgraded to advisory-only by shrink safety gate: %s", logThreadSeq, event, repairColumnName, suggestion.Reason)
						global.Wlog.Warn(vlog)
						columnRiskDifferent = true
						columnAdvisorySuggestions = append(columnAdvisorySuggestions, suggestion)
					} else {
						vlog = fmt.Sprintf("(%d) %s The column name of column %s of the source and target table %s.%s:[%s.%s] is the same, but the definition of the column is inconsistent, and a modify statement is generated, and the modification statement is {%v}", logThreadSeq, event, repairColumnName, stcls.schema, stcls.table, destSchema, stcls.table, modifySql)
						global.Wlog.Warn(vlog)
						alterSlice = append(alterSlice, modifySql)
					}
				}
				delete(destColumnMap, v1)
			} else {
				var position int
				// 使用固定值：ScheckOrder=yes，总是使用源列的实际位置
				position = k1
				// Use the source identifier for ADD COLUMN and the current target
				// identifier for positional clauses when available.
				originalColName := getSourceOriginalColumnName(v1)
				originalLastColumn := getTargetPositionColumnName(lastcolumn)
				repairAttrs := append([]string(nil), sourceColumnMap[v1]...)
				if useCanonicalCompare {
					repairPlan := schemacompat.BuildTargetColumnRepairPlan(
						originalColName,
						repairAttrs,
						stcls.sourceVersionInfo(),
						stcls.destVersionInfo(),
						sourceColumnDefinitions[originalColName],
						stcls.checkRules.MariaDBJSONTargetType,
					)
					if len(repairAttrs) < 6 {
						for len(repairAttrs) < 6 {
							repairAttrs = append(repairAttrs, "null")
						}
					}
					if strings.TrimSpace(repairPlan.Type) != "" {
						repairAttrs[0] = repairPlan.Type
					}
					if strings.TrimSpace(repairPlan.Charset) != "" {
						repairAttrs[1] = repairPlan.Charset
					}
					if strings.TrimSpace(repairPlan.Collation) != "" {
						repairAttrs[2] = repairPlan.Collation
					}
					if repairPlan.UseDirectDefinition {
						if len(repairAttrs) < 7 {
							repairAttrs = append(repairAttrs, repairPlan.DirectDefinition)
						} else {
							repairAttrs[6] = repairPlan.DirectDefinition
						}
					}
				}
				// 检查目标表是否存在主键
				if mysqlDataFix, ok := dbf.DataAbnormalFix().(*mysql.MysqlDataAbnormalFixStruct); ok {
					mysqlDataFix.CheckDestTableHasPrimaryKey(stcls.destDB, logThreadSeq)
				}
				addSql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("add", repairAttrs, position, originalLastColumn, originalColName, logThreadSeq)
				vlog = fmt.Sprintf("(%d) %s Missing column %s in %s.%s - ADD: %v", logThreadSeq, event, originalColName, destSchema, stcls.table, addSql)
				global.Wlog.Warn(vlog)
				alterSlice = append(alterSlice, addSql)
				delete(destColumnMap, v1)
			}
		}

		fixer := dbf.DataAbnormalFix()

		// 先生成列级别的修复SQL
		sqlS := fixer.FixAlterColumnSqlGenerate(alterSlice, logThreadSeq)
		constraintAdvisorySQLs := make([]string, 0)
		tableAdvisorySuggestions := make([]schemacompat.ConstraintRepairSuggestion, 0)
		executableColumnCollationRepair := false
		columnCollationRepairHandled := len(columnCollationRepairCandidates) == 0

		tableCharsetDifferent := false
		tableCollationDifferent := false
		tableCommentDifferent := false
		tableAutoIncrementRiskDifferent := false
		tableRowFormatDifferent := false
		tableCollationRiskDifferent := false
		tableCollationMappedDifferent := false
		tableCheckRiskDifferent := false
		tableUnsupportedRiskDifferent := false

		if stcls.isMySQLToMySQL() {
			sourceMeta, errSourceMeta := queryMySQLTableLevelMetadata(stcls.sourceDB, sourceSchema, stcls.table)
			if errSourceMeta != nil {
				vlog = fmt.Sprintf("(%d) %s Failed to query source table metadata for %s.%s: %v", logThreadSeq, event, sourceSchema, stcls.table, errSourceMeta)
				global.Wlog.Error(vlog)
			} else {
				destMeta, errDestMeta := queryMySQLTableLevelMetadata(stcls.destDB, destSchema, stcls.destTable)
				if errDestMeta != nil {
					vlog = fmt.Sprintf("(%d) %s Failed to query target table metadata for %s.%s: %v", logThreadSeq, event, destSchema, stcls.destTable, errDestMeta)
					global.Wlog.Error(vlog)
				} else {
					sourceMeta.TableComment = normalizeMetadataComment(sourceMeta.TableComment)
					destMeta.TableComment = normalizeMetadataComment(destMeta.TableComment)

					unsupportedFeatures := schemacompat.DetectMariaDBUnsupportedTableFeatures(sourceMeta.CreateTableSQL, stcls.sourceVersionInfo(), stcls.destVersionInfo())
					if len(unsupportedFeatures) > 0 {
						tableUnsupportedRiskDifferent = true
						vlog = fmt.Sprintf("(%d) %s MariaDB unsupported features detected for %s.%s -> %s.%s: %+v",
							logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.destTable, unsupportedFeatures)
						global.Wlog.Warn(vlog)
						constraintAdvisorySQLs = append(
							constraintAdvisorySQLs,
							buildConstraintAdvisoryLines(
								fmt.Sprintf("%s.%s MariaDB unsupported features", destSchema, stcls.destTable),
								schemacompat.BuildMariaDBUnsupportedFeatureSuggestions(destSchema, stcls.destTable, unsupportedFeatures),
							)...,
						)
					}

					// MariaDB LEFT JOIN information_schema.COLLATIONS 可能返回空的 TableCharset，
					// 在比较前从 collation 名推断 charset，避免误判为 charset mismatch
					if strings.TrimSpace(sourceMeta.TableCharset) == "" && strings.TrimSpace(sourceMeta.TableCollation) != "" {
						inferred := schemacompat.InferCharsetFromCollation(sourceMeta.TableCollation)
						if inferred != "" {
							vlog = fmt.Sprintf("(%d) %s Source table charset was empty, inferred as %s from collation %s for %s.%s",
								logThreadSeq, event, inferred, sourceMeta.TableCollation, sourceSchema, stcls.table)
							global.Wlog.Warn(vlog)
							sourceMeta.TableCharset = inferred
						}
					}

					charsetDecision := schemacompat.DecideCharsetCompatibility(sourceMeta.TableCharset, destMeta.TableCharset)
					if charsetDecision.IsMismatch() {
						tableCharsetDifferent = true
						vlog = fmt.Sprintf("(%d) %s Table charset mismatch: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCharset, destMeta.TableCharset, charsetDecision.Reason)
						global.Wlog.Warn(vlog)
					} else if charsetDecision.State == schemacompat.CompatibilityNormalizedEqual {
						vlog = fmt.Sprintf("(%d) %s Table charset normalized-equal: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCharset, destMeta.TableCharset, charsetDecision.Reason)
						global.Wlog.Debug(vlog)
					}

					// 检查是否所有列级 collation 差异都属于已知的 MariaDB→MySQL 等价映射
					allColumnCollationMapped := len(columnCollationRepairCandidates) > 0 && len(alterSlice) == 0
					if allColumnCollationMapped {
						for _, c := range columnCollationRepairCandidates {
							mapped, ok := schemacompat.MapMariaDBCollationToMySQL(c.SourceCollation)
							if !ok || !strings.EqualFold(mapped, strings.TrimSpace(c.DestCollation)) {
								allColumnCollationMapped = false
								break
							}
						}
					}

					if allColumnCollationMapped {
						// 所有列级 collation 差异都是已知的跨平台等价映射，无需生成修复 SQL
						tableCollationMappedDifferent = true
						columnCollationRepairHandled = true
						vlog = fmt.Sprintf("(%d) %s All %d column collation differences are cross-platform mappings for %s.%s -> %s.%s, no fix SQL needed",
							logThreadSeq, event, len(columnCollationRepairCandidates), sourceSchema, stcls.table, destSchema, stcls.destTable)
						global.Wlog.Warn(vlog)
					} else if repairSQLs, ok := stcls.buildColumnCollationRepairSQL(fixer, sourceMeta, destMeta, sourceColumnDefinitions, columnCollationRepairCandidates, logThreadSeq); ok {
						executableColumnCollationRepair = true
						columnCollationRepairHandled = true
						vlog = fmt.Sprintf("(%d) %s Generated executable column collation repair SQL for %s.%s -> %s.%s: %v",
							logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.destTable, repairSQLs)
						global.Wlog.Warn(vlog)
						sqlS = append(sqlS, repairSQLs...)
					} else if len(columnCollationRepairCandidates) > 0 {
						columnRiskDifferent = true
						columnCollationRepairHandled = true
						columnAdvisorySuggestions = append(columnAdvisorySuggestions, buildColumnCollationAdvisorySuggestions(columnCollationRepairCandidates)...)
					}

					collationDecision := schemacompat.DecideCollationCompatibility(sourceMeta.TableCollation, destMeta.TableCollation)
					// MariaDB→MySQL 跨平台场景：非 MariaDB 特有的 collation（如 utf8mb4_general_ci）在 MySQL 中合法存在，
					// 排序行为不同于目标端，应视为真实差异而非默认 collation 漂移
					if collationDecision.State == schemacompat.CompatibilityWarnOnly && stcls.isMariaDBToMySQL() {
						if _, isMappable := schemacompat.MapMariaDBCollationToMySQL(sourceMeta.TableCollation); !isMappable {
							collationDecision.State = schemacompat.CompatibilityUnsupported
							collationDecision.Reason = fmt.Sprintf("cross-platform collation mismatch: source=%s is valid in MySQL but differs from target=%s",
								sourceMeta.TableCollation, destMeta.TableCollation)
							vlog = fmt.Sprintf("(%d) %s Reclassified table collation drift as hard mismatch for MariaDB→MySQL: source=%s, dest=%s",
								logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation)
							global.Wlog.Debug(vlog)
						}
					}
					if collationDecision.State == schemacompat.CompatibilityWarnOnly {
						// 检查是否为 MariaDB→MySQL 已知的 collation 等价映射（如 uca1400→0900）
						mappedCollation, isMappable := schemacompat.MapMariaDBCollationToMySQL(sourceMeta.TableCollation)
						if isMappable && strings.EqualFold(mappedCollation, strings.TrimSpace(destMeta.TableCollation)) {
							// 已知的跨平台 collation 等价映射，标记为 collation-mapped，不生成任何 fix SQL
							tableCollationMappedDifferent = true
							vlog = fmt.Sprintf("(%d) %s Table collation-mapped: source=%s maps to target=%s, no fix SQL needed",
								logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation)
							global.Wlog.Warn(vlog)
						} else if executableColumnCollationRepair || tableCharsetDifferent {
							// 可执行的列级 collation 修复 SQL 或表级 charset 差异修复已包含 CONVERT TO CHARACTER SET，
							// 跳过重复的表级 advisory 输出
							vlog = fmt.Sprintf("(%d) %s Table collation drift already covered by executable column collation repair: source=%s, dest=%s", logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation)
							global.Wlog.Debug(vlog)
						} else {
							tableCollationRiskDifferent = true
							vlog = fmt.Sprintf("(%d) %s Table collation warning: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation, collationDecision.Reason)
							global.Wlog.Warn(vlog)
							tableAdvisorySuggestions = append(tableAdvisorySuggestions, schemacompat.ConstraintRepairSuggestion{
								Kind:   "TABLE COLLATION",
								Level:  schemacompat.ConstraintRepairLevelAdvisoryOnly,
								Reason: collationDecision.Reason,
								Statements: func() []string {
									advisoryCollation := sourceMeta.TableCollation
									if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(advisoryCollation); ok {
										advisoryCollation = mapped
									}
									return fixer.FixTableCharsetSqlGenerate(sourceMeta.TableCharset, advisoryCollation, logThreadSeq)
								}(),
							})
						}
					} else if collationDecision.IsMismatch() {
						tableCollationDifferent = true
						vlog = fmt.Sprintf("(%d) %s Table collation mismatch: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation, collationDecision.Reason)
						global.Wlog.Warn(vlog)
					} else if collationDecision.State == schemacompat.CompatibilityNormalizedEqual {
						vlog = fmt.Sprintf("(%d) %s Table collation normalized-equal: source=%s, dest=%s, reason=%s", logThreadSeq, event, sourceMeta.TableCollation, destMeta.TableCollation, collationDecision.Reason)
						global.Wlog.Debug(vlog)
					}

					if tableCharsetDifferent || tableCollationDifferent {
						repairCollation := sourceMeta.TableCollation
						if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(repairCollation); ok {
							repairCollation = mapped
						}
						sqlS = append(sqlS, fixer.FixTableCharsetSqlGenerate(sourceMeta.TableCharset, repairCollation, logThreadSeq)...)
					}

					rowFormatDecision := schemacompat.DecideTableRowFormatCompatibility(
						schemacompat.CanonicalizeMySQLTableOptions(sourceMeta.RowFormat, sourceMeta.CreateOptions, sourceMeta.TableComment),
						schemacompat.CanonicalizeMySQLTableOptions(destMeta.RowFormat, destMeta.CreateOptions, destMeta.TableComment),
					)
					if rowFormatDecision.IsMismatch() {
						tableRowFormatDifferent = true
						vlog = fmt.Sprintf("(%d) %s Table row format mismatch: source=%s, dest=%s, reason=%s",
							logThreadSeq, event, sourceMeta.RowFormat, destMeta.RowFormat, rowFormatDecision.Reason)
						global.Wlog.Warn(vlog)
					} else if rowFormatDecision.State == schemacompat.CompatibilityNormalizedEqual {
						vlog = fmt.Sprintf("(%d) %s Table row format normalized-equal: source=%s, dest=%s, reason=%s",
							logThreadSeq, event, sourceMeta.RowFormat, destMeta.RowFormat, rowFormatDecision.Reason)
						global.Wlog.Debug(vlog)
					}

					sourceCatalog := schemacompat.BuildSchemaFeatureCatalog(stcls.sourceVersionInfo())
					destCatalog := schemacompat.BuildSchemaFeatureCatalog(stcls.destVersionInfo())
					sourceChecks := schemacompat.ExtractCheckConstraintsFromCreateSQL(sourceMeta.CreateTableSQL)
					sourceChecks = schemacompat.FilterPortableCheckConstraints(sourceChecks, stcls.sourceVersionInfo(), stcls.destVersionInfo(), sourceColumnDefinitions)
					destChecks := schemacompat.ExtractCheckConstraintsFromCreateSQL(destMeta.CreateTableSQL)
					checkDecision := schemacompat.DecideCheckConstraintCompatibility(sourceChecks, destChecks, sourceCatalog, destCatalog)
					if checkDecision.IsMismatch() {
						tableCheckRiskDifferent = true
						vlog = fmt.Sprintf("(%d) %s Table CHECK constraint risk detected for %s.%s -> %s.%s: %s",
							logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.destTable, checkDecision.Reason)
						global.Wlog.Warn(vlog)
						checkSuggestions := schemacompat.BuildCheckConstraintRepairSuggestions(destSchema, stcls.destTable, sourceChecks, destChecks, checkDecision)
						constraintAdvisorySQLs = append(
							constraintAdvisorySQLs,
							buildConstraintAdvisoryLines(fmt.Sprintf("%s.%s CHECK constraints", destSchema, stcls.destTable), checkSuggestions)...,
						)
					}

					if advisorySuggestion, needsFix := buildMySQLTableAutoIncrementAdvisory(destSchema, stcls.destTable, sourceMeta.AutoIncrement, destMeta.AutoIncrement); needsFix && !droppedAutoIncrementColumn {
						tableAutoIncrementRiskDifferent = true
						vlog = fmt.Sprintf("(%d) %s Table AUTO_INCREMENT drift recorded as advisory-only: source=%v, dest=%v", logThreadSeq, event, nullInt64ForLog(sourceMeta.AutoIncrement), nullInt64ForLog(destMeta.AutoIncrement))
						global.Wlog.Warn(vlog)
						tableAdvisorySuggestions = append(tableAdvisorySuggestions, advisorySuggestion)
					} else if needsFix && droppedAutoIncrementColumn {
						vlog = fmt.Sprintf("(%d) %s Skip table AUTO_INCREMENT repair for %s.%s because the target auto-increment column is being dropped",
							logThreadSeq, event, destSchema, stcls.table)
						global.Wlog.Debug(vlog)
					}

					if sourceMeta.TableComment != destMeta.TableComment {
						tableCommentDifferent = true
						escapedComment := escapeMySQLCommentLiteral(sourceMeta.TableComment)
						tableCommentSql := fmt.Sprintf("ALTER TABLE `%s`.`%s` COMMENT = '%s';", destSchema, stcls.destTable, escapedComment)
						vlog = fmt.Sprintf("(%d) %s Table comment mismatch: source='%s', dest='%s', generating fix SQL", logThreadSeq, event, sourceMeta.TableComment, destMeta.TableComment)
						global.Wlog.Warn(vlog)
						sqlS = append(sqlS, tableCommentSql)
					}
				}
			}
		}

		if len(tableAdvisorySuggestions) > 0 {
			constraintAdvisorySQLs = append(
				constraintAdvisorySQLs,
				buildConstraintAdvisoryLines(fmt.Sprintf("%s.%s TABLE options", destSchema, stcls.destTable), tableAdvisorySuggestions)...,
			)
		}
		if !columnCollationRepairHandled && len(columnCollationRepairCandidates) > 0 {
			columnRiskDifferent = true
			columnAdvisorySuggestions = append(columnAdvisorySuggestions, buildColumnCollationAdvisorySuggestions(columnCollationRepairCandidates)...)
		}
		if len(columnAdvisorySuggestions) > 0 {
			constraintAdvisorySQLs = append(
				constraintAdvisorySQLs,
				buildConstraintAdvisoryLines(fmt.Sprintf("%s.%s COLUMN attributes", destSchema, stcls.destTable), columnAdvisorySuggestions)...,
			)
		}

		hasWarnOnlyTableLevelDiff := columnRiskDifferent || tableAutoIncrementRiskDifferent || tableCollationRiskDifferent || tableCheckRiskDifferent || tableUnsupportedRiskDifferent
		hasCollationMappedOnly := tableCollationMappedDifferent && !columnRiskDifferent && !tableAutoIncrementRiskDifferent && !tableCollationRiskDifferent && !tableCheckRiskDifferent && !tableUnsupportedRiskDifferent
		hasHardTableLevelDiff := tableCharsetDifferent || tableCollationDifferent || tableCommentDifferent || tableRowFormatDifferent
		if len(alterSlice) > 0 || hasHardTableLevelDiff || executableColumnCollationRepair {
			abnormalTableList = append(abnormalTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
		} else if hasWarnOnlyTableLevelDiff {
			stcls.structWarnOnlyDiffsMap[fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)] = true
			newCheckTableList = append(newCheckTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
		} else if hasCollationMappedOnly {
			stcls.structCollationMappedMap[fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)] = true
			newCheckTableList = append(newCheckTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
		} else {
			newCheckTableList = append(newCheckTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
		}

		vlog = fmt.Sprintf("(%d) %s Structure validation completed for %s.%s -> %s.%s", logThreadSeq, event, stcls.schema, stcls.table, destSchema, stcls.table)
		global.Wlog.Debug(vlog)

		// 如果sqlS不为空（表示没有应用过列级别修复），则应用它
		if len(sqlS) > 0 {
			tableKey := fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)
			stcls.rememberColumnRepairOperations(tableKey, sqlS)
			vlog = fmt.Sprintf("(%d) %s Deferred column/table repair statements for %s.%s until index reconciliation: %v", logThreadSeq, event, destSchema, stcls.table, sqlS)
			global.Wlog.Debug(vlog)
		}
		if len(constraintAdvisorySQLs) > 0 {
			vlog = fmt.Sprintf("(%d) %s Writing advisory-only constraint repair suggestions for %s.%s", logThreadSeq, event, destSchema, stcls.destTable)
			global.Wlog.Debug(vlog)
			if err = stcls.writeAdvisoryFixSql(constraintAdvisorySQLs, logThreadSeq); err != nil {
				return nil, nil, err
			}
		}
	}
	vlog = fmt.Sprintf("(%d) %s Table structure validation completed", logThreadSeq, event)
	global.Wlog.Info(vlog)

	return newCheckTableList, abnormalTableList, nil
}

/*
该函数用于获取MySQL的表的索引信息,判断表是否存在索引，加入存在，获取索引的类型，以主键索引、唯一索引、普通索引及无索引，主键索引或唯一索引以自增id为优先

	缺少索引列为空或null的处理
*/
func (stcls *schemaTable) tableIndexAlgorithm(indexType map[string][]string) (string, []string) {
	if len(indexType) > 0 {
		// 优先选择主键索引
		if len(indexType["pri_single"]) > 0 {
			return "pri_single", indexType["pri_single"]
		}
		if len(indexType["pri_multi"]) > 0 {
			return "pri_multi", indexType["pri_multi"]
		}
		if len(indexType["pri_multiseriate"]) > 0 {
			return "pri_multiseriate", indexType["pri_multiseriate"]
		}

		// 其次选择唯一索引
		if len(indexType["uni_single"]) > 0 {
			return "uni_single", indexType["uni_single"]
		}
		if len(indexType["uni_multi"]) > 0 {
			return "uni_multi", indexType["uni_multi"]
		}
		if len(indexType["uni_multiseriate"]) > 0 {
			return "uni_multiseriate", indexType["uni_multiseriate"]
		}

		// 最后选择普通索引
		if len(indexType["mul_single"]) > 0 {
			return "mul_single", indexType["mul_single"]
		}
		if len(indexType["mul_multi"]) > 0 {
			return "mul_multi", indexType["mul_multi"]
		}
		if len(indexType["mul_multiseriate"]) > 0 {
			return "mul_multiseriate", indexType["mul_multiseriate"]
		}
	}
	return "", []string{}
}

// 处理模糊匹配，支持数据库映射规则
func (stcls *schemaTable) FuzzyMatchingDispos(dbCheckNameList map[string]int, Ftable string, logThreadSeq int64) map[string]int {
	var (
		schema string
		vlog   string
	)
	b := make(map[string]int)
	f := make(map[string]int)
	sourceSchemas := extractSchemaNamesFromCacheKeys(dbCheckNameList)
	if strings.TrimSpace(Ftable) == "" || strings.EqualFold(strings.TrimSpace(Ftable), "nil") {
		return f
	}

	// 添加调试日志，显示当前的映射规则
	vlog = fmt.Sprintf("Current table mappings: %v", stcls.tableMappings)
	global.Wlog.Debug(vlog)

	//处理库的模糊查询
	for _, i := range strings.Split(Ftable, ",") {
		// 解析映射关系
		srcPattern := i
		dstPattern := ""
		hasMappingRule := false

		if strings.Contains(i, ":") {
			parts := strings.SplitN(i, ":", 2)
			if len(parts) == 2 {
				srcPattern = parts[0]
				dstPattern = parts[1]
				hasMappingRule = true
			}
		}

		vlog = fmt.Sprintf("Processing table pattern: source=%s, target=%s, mapped=%v", srcPattern, dstPattern, hasMappingRule)
		global.Wlog.Debug(vlog)

		if !strings.Contains(srcPattern, ".") {
			continue
		}

		schema = strings.ReplaceAll(srcPattern[:strings.Index(srcPattern, ".")], "%", "")

		// 处理通配符模式
		if schema == "*" { //处理*库
			for _, schemaName := range sourceSchemas {
				b[schemaName]++
				vlog = fmt.Sprintf("Added wildcard schema: %s", schemaName)
				global.Wlog.Debug(vlog)
			}
		} else if strings.HasPrefix(schema, "%") && strings.HasSuffix(schema, "%") { //处理%schema%
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for _, schemaName := range sourceSchemas {
				if strings.Contains(schemaName, tmpschema) {
					b[schemaName]++
					vlog = fmt.Sprintf("Added %%schema%% match: %s", schemaName)
					global.Wlog.Debug(vlog)
				}
			}
		} else if strings.HasPrefix(schema, "%") && !strings.HasSuffix(schema, "%") { //处理%schema
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for _, schemaName := range sourceSchemas {
				if strings.HasSuffix(schemaName, tmpschema) {
					b[schemaName]++
					vlog = fmt.Sprintf("Added %%schema match: %s", schemaName)
					global.Wlog.Debug(vlog)
				}
			}
		} else if !strings.HasPrefix(schema, "%") && strings.HasSuffix(schema, "%") { //处理schema%
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for _, schemaName := range sourceSchemas {
				if strings.HasPrefix(schemaName, tmpschema) {
					b[schemaName]++
					vlog = fmt.Sprintf("Added schema%% match: %s", schemaName)
					global.Wlog.Debug(vlog)
				}
			}
		} else { //处理schema
			// 检查是否在映射规则中存在（Oracle源端按不区分大小写匹配）
			if _, exists := stcls.findMappedSchema(schema); exists {
				added := false
				for _, schemaName := range sourceSchemas {
					if stcls.sourceObjectNameEqual(schemaName, schema) {
						b[schemaName]++
						added = true
						vlog = fmt.Sprintf("Added source schema from mapping: %s (pattern: %s)", schemaName, schema)
						global.Wlog.Debug(vlog)
					}
				}
				if !added {
					b[schema]++
					vlog = fmt.Sprintf("Added source schema from mapping fallback: %s", schema)
					global.Wlog.Debug(vlog)
				}
			} else if hasMappingRule {
				// 如果有明确的映射规则，尝试使用它
				dstSchema := ""
				if strings.Contains(dstPattern, ".") {
					dstSchema = dstPattern[:strings.Index(dstPattern, ".")]
				} else {
					dstSchema = dstPattern
				}

				// 检查源schema是否存在于数据库列表中（大小写兼容）
				for _, schemaName := range sourceSchemas {
					if stcls.sourceObjectNameEqual(schemaName, schema) {
						b[schemaName]++
						vlog = fmt.Sprintf("Added explicit mapping source schema: %s -> %s", schemaName, dstSchema)
						global.Wlog.Debug(vlog)
					}
				}
			} else {
				// 检查是否是目标端schema
				found := false
				for src, dst := range stcls.tableMappings {
					if stcls.destObjectNameEqual(dst, schema) {
						// 找到对应源端schema
						b[src]++
						found = true
						vlog = fmt.Sprintf("Added reverse mapping source schema: %s -> %s", src, dst)
						global.Wlog.Debug(vlog)
						break
					}
				}
				// 如果没有映射关系，则按常规处理
				if !found {
					// 检查schema是否存在于数据库列表中（大小写兼容）
					for _, schemaName := range sourceSchemas {
						if stcls.sourceObjectNameEqual(schemaName, schema) {
							b[schemaName]++
							vlog = fmt.Sprintf("Added direct schema (no mapping): %s", schemaName)
							global.Wlog.Debug(vlog)
						}
					}
				}
			}
		}
	}

	vlog = fmt.Sprintf("After schema processing, b map: %v", b)
	global.Wlog.Debug(vlog)

	//处理表的模糊查询
	for _, i := range strings.Split(Ftable, ",") {
		// 解析映射关系
		srcPattern := i
		dstPattern := ""
		hasMappingRule := false

		if strings.Contains(i, ":") {
			parts := strings.SplitN(i, ":", 2)
			if len(parts) == 2 {
				srcPattern = parts[0]
				dstPattern = parts[1]
				hasMappingRule = true
			}
		}

		vlog = fmt.Sprintf("Processing table pattern: src=%s, dst=%s, hasMapping=%v", srcPattern, dstPattern, hasMappingRule)
		global.Wlog.Debug(vlog)

		if !strings.Contains(srcPattern, ".") {
			continue
		}

		schema = strings.ReplaceAll(srcPattern[:strings.Index(srcPattern, ".")], "%", "")
		table := srcPattern[strings.Index(srcPattern, ".")+1:]

		vlog = fmt.Sprintf("Parsed schema=%s, table=%s", schema, table)
		global.Wlog.Debug(vlog)

		// 处理表名通配符
		for dbSchema, _ := range b {
			// 检查是否有映射关系
			mappedSchema := dbSchema
			if mapped, exists := stcls.findMappedSchema(dbSchema); exists {
				mappedSchema = mapped
				vlog = fmt.Sprintf("Found schema mapping: %s -> %s", dbSchema, mappedSchema)
				global.Wlog.Debug(vlog)
			}

			// 检查schema是否匹配
			if stcls.sourceObjectNameEqual(dbSchema, schema) || schema == "*" {
				// 构建表名查询
				for dbName, _ := range dbCheckNameList {
					dbSchemaName, dbTableName, ok := splitSchemaTableCacheKey(dbName)
					if !ok {
						continue
					}

					// 检查schema是否匹配
					if !stcls.sourceObjectNameEqual(dbSchemaName, dbSchema) {
						continue
					}

					// 处理表名通配符
					if table == "*" { // 处理schema.*
						f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
						vlog = fmt.Sprintf("Added table pattern: %s.%s", dbSchema, dbTableName)
						global.Wlog.Debug(vlog)
					} else if strings.HasPrefix(table, "%") && !strings.HasSuffix(table, "%") { // 处理schema.%table
						tmptable := strings.ReplaceAll(table, "%", "")
						if strings.HasSuffix(dbTableName, tmptable) {
							f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
							vlog = fmt.Sprintf("Added suffix pattern: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					} else if !strings.HasPrefix(table, "%") && strings.HasSuffix(table, "%") { // 处理schema.table%
						tmptable := strings.ReplaceAll(table, "%", "")
						if strings.HasPrefix(dbTableName, tmptable) {
							f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
							vlog = fmt.Sprintf("Added table%% match: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					} else if strings.HasPrefix(table, "%") && strings.HasSuffix(table, "%") { // 处理schema.%table%
						tmptable := strings.ReplaceAll(table, "%", "")
						if strings.Contains(dbTableName, tmptable) {
							f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
							vlog = fmt.Sprintf("Added %%table%% match: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					} else { // 处理schema.table
						if stcls.sourceObjectNameEqual(dbTableName, table) {
							f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
							vlog = fmt.Sprintf("Added exact table match: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					}
				}
			}
		}
	}

	vlog = fmt.Sprintf("Final result map: %v", f)
	global.Wlog.Debug(vlog)

	return f
}

func extractSchemaNamesFromCacheKeys(dbCheckNameList map[string]int) []string {
	schemaSet := make(map[string]struct{}, len(dbCheckNameList))
	for cacheKey := range dbCheckNameList {
		schemaName, _, ok := splitSchemaTableCacheKey(cacheKey)
		if !ok {
			// fallback for legacy key format
			schemaName = cacheKey
		}
		schemaSet[schemaName] = struct{}{}
	}
	result := make([]string, 0, len(schemaSet))
	for schemaName := range schemaSet {
		result = append(result, schemaName)
	}
	sort.Strings(result)
	return result
}

/*
处理需要校验的库表
将忽略的库表从校验列表中去除，如果校验列表为空则退出
*/
// 定义一个新的结构体来存储表映射信息
type TableMapping struct {
	SourceSchema string // 源端schema
	SourceTable  string // 源端表名
	DestSchema   string // 目标端schema
	DestTable    string // 目标端表名
}

var schemaTableFilterDatabaseNameList = func(tc dbExec.TableColumnNameStruct, db *sql.DB, logThreadSeq int64) (map[string]int, error) {
	return tc.Query().DatabaseNameList(db, logThreadSeq)
}

var schemaTableFilterObjectTypeMap = func(tc dbExec.TableColumnNameStruct, db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	return tc.Query().ObjectTypeMap(db, logThreadSeq)
}

// extractCandidateSchemas returns the distinct schema names present in the
// DatabaseNameList key set (format: "schema/*schema&table*/table").
// The result is used to constrain the ObjectTypeMap metadata query to only the
// schemas relevant for this run instead of performing a full-instance scan.
func extractCandidateSchemas(candidates map[string]int) []string {
	seen := make(map[string]struct{}, len(candidates))
	for key := range candidates {
		const sep = "/*schema&table*/"
		if idx := strings.Index(key, sep); idx > 0 {
			seen[key[:idx]] = struct{}{}
		}
	}
	schemas := make([]string, 0, len(seen))
	for s := range seen {
		schemas = append(schemas, s)
	}
	return schemas
}

func (stcls *schemaTable) SchemaTableFilter(logThreadSeq1, logThreadSeq2 int64) ([]string, error) {
	var (
		vlog            string
		f               []string
		dbCheckNameList map[string]int
		err             error
	)
	fmt.Println("gt-checksum: Starting table checks")
	vlog = fmt.Sprintf("(%d) Obtain schema.table info", logThreadSeq1)
	global.Wlog.Info(vlog)

	// 解析表映射规则
	stcls.parseTableMappings(stcls.table)

	// 添加调试日志，显示解析后的映射规则
	vlog = fmt.Sprintf("Table mappings after parsing: %v", stcls.tableMappings)
	global.Wlog.Debug(vlog)

	// 获取源数据库信息列表
	tc := dbExec.TableColumnNameStruct{
		Table:                   stcls.table,
		Drive:                   stcls.sourceDrive,
		Db:                      stcls.sourceDB,
		IgnoreTable:             stcls.ignoreTable,
		CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
	}
	vlog = fmt.Sprintf("(%d) Obtain source databases list", logThreadSeq1)
	global.Wlog.Debug(vlog)
	if dbCheckNameList, err = schemaTableFilterDatabaseNameList(tc, stcls.sourceDB, logThreadSeq2); err != nil {
		return f, err
	}

	// Populate the per-run object-type map (table vs. view).
	// A failed query is non-fatal: we log a warning and continue with an empty
	// map, which preserves the previous behaviour of treating every object as a
	// BASE TABLE.
	//
	// Pass the candidate schema set extracted from dbCheckNameList so that
	// the driver can restrict the INFORMATION_SCHEMA.TABLES scan to only the
	// schemas relevant to this run, avoiding a costly full-instance scan.
	tc.CandidateSchemas = extractCandidateSchemas(dbCheckNameList)
	if kinds, kindErr := schemaTableFilterObjectTypeMap(tc, stcls.sourceDB, logThreadSeq2); kindErr != nil {
		vlog = fmt.Sprintf("(%d) ObjectTypeMap query failed (non-fatal, treating all objects as BASE TABLE): %v", logThreadSeq1, kindErr)
		global.Wlog.Warn(vlog)
		stcls.objectKinds = make(map[string]string)
	} else {
		stcls.objectKinds = kinds
		vlog = fmt.Sprintf("(%d) ObjectTypeMap loaded: %d entries", logThreadSeq1, len(kinds))
		global.Wlog.Debug(vlog)
	}

	sampleLimit := 8
	if len(dbCheckNameList) <= sampleLimit {
		vlog = fmt.Sprintf("(%d) Source databases list(size=%d): %v", logThreadSeq1, len(dbCheckNameList), dbCheckNameList)
	} else {
		sample := make([]string, 0, sampleLimit)
		for k := range dbCheckNameList {
			sample = append(sample, k)
			if len(sample) >= sampleLimit {
				break
			}
		}
		sort.Strings(sample)
		vlog = fmt.Sprintf("(%d) Source databases list(size=%d, sample=%v)", logThreadSeq1, len(dbCheckNameList), sample)
	}
	global.Wlog.Debug(vlog)

	// 判断源库是否为空
	if len(dbCheckNameList) == 0 {
		vlog = fmt.Sprintf("(%d) Databases of srcDSN {%s} is empty, please check if the \"tables\" option is correct", logThreadSeq1, stcls.sourceDrive)
		global.Wlog.Error(vlog)
		return f, nil
	}

	// 处理映射关系中的目标库
	// 如果有映射关系，也需要获取目标库的信息
	destDbCheckNameList := make(map[string]int)

	// 检查是否有映射关系
	hasMapping := false
	for _, pattern := range strings.Split(stcls.table, ",") {
		if strings.Contains(pattern, ":") {
			hasMapping = true
			break
		}
	}

	// 如果有映射关系，获取目标库信息
	if hasMapping {
		vlog = fmt.Sprintf("(%d) Mapping relationship detected, obtaining destination databases list", logThreadSeq1)
		global.Wlog.Debug(vlog)

		tcDest := dbExec.TableColumnNameStruct{
			Table:                   stcls.table,
			Drive:                   stcls.destDrive,
			Db:                      stcls.destDB,
			IgnoreTable:             stcls.ignoreTable,
			CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
		}

		destDbList, err := schemaTableFilterDatabaseNameList(tcDest, stcls.destDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Error getting destination databases list: %v", logThreadSeq1, err)
			global.Wlog.Error(vlog)
		} else {
			destDbCheckNameList = destDbList
			if len(destDbCheckNameList) <= sampleLimit {
				vlog = fmt.Sprintf("(%d) Destination databases list(size=%d): %v", logThreadSeq1, len(destDbCheckNameList), destDbCheckNameList)
			} else {
				sample := make([]string, 0, sampleLimit)
				for k := range destDbCheckNameList {
					sample = append(sample, k)
					if len(sample) >= sampleLimit {
						break
					}
				}
				sort.Strings(sample)
				vlog = fmt.Sprintf("(%d) Destination databases list(size=%d, sample=%v)", logThreadSeq1, len(destDbCheckNameList), sample)
			}
			global.Wlog.Debug(vlog)
		}
	}

	// 创建表映射列表
	tableMappings := make([]TableMapping, 0)

	// 处理 db1.*:db2.* 格式的映射
	for _, pattern := range strings.Split(stcls.table, ",") {
		if strings.Contains(pattern, ":") {
			mapping := strings.SplitN(pattern, ":", 2)
			if len(mapping) == 2 {
				srcPattern := mapping[0]
				dstPattern := mapping[1]

				// 处理 db1.*:db2.* 格式
				if strings.HasSuffix(srcPattern, ".*") && strings.HasSuffix(dstPattern, ".*") {
					srcDB := strings.TrimSuffix(srcPattern, ".*")
					dstDB := strings.TrimSuffix(dstPattern, ".*")

					vlog = fmt.Sprintf("Processing wildcard mapping: %s.* -> %s.*", srcDB, dstDB)
					global.Wlog.Debug(vlog)

					// 获取源库中的所有表（Oracle源端按不区分大小写匹配schema）
					for dbName := range dbCheckNameList {
						dbSchemaName, tableName, ok := splitSchemaTableCacheKey(dbName)
						if !ok || !stcls.sourceObjectNameEqual(dbSchemaName, srcDB) {
							continue
						}

						// 创建表映射
						mapping := TableMapping{
							SourceSchema: dbSchemaName,
							SourceTable:  tableName,
							DestSchema:   dstDB,
							DestTable:    tableName,
						}
						tableMappings = append(tableMappings, mapping)

						vlog = fmt.Sprintf("Added mapping: %s.%s -> %s.%s", dbSchemaName, tableName, dstDB, tableName)
						global.Wlog.Debug(vlog)
					}

					// 检查目标库中是否有源库中不存在的表
					for dbName := range destDbCheckNameList {
						dbSchemaName, tableName, ok := splitSchemaTableCacheKey(dbName)
						if !ok || !stcls.destObjectNameEqual(dbSchemaName, dstDB) {
							continue
						}

						// 检查这个表是否已经在映射列表中
						found := false
						for _, m := range tableMappings {
							if stcls.destObjectNameEqual(m.DestSchema, dstDB) && m.DestTable == tableName {
								found = true
								break
							}
						}

						// 如果没有找到，添加新的映射
						if !found {
							mapping := TableMapping{
								SourceSchema: srcDB,
								SourceTable:  tableName,
								DestSchema:   dbSchemaName,
								DestTable:    tableName,
							}
							tableMappings = append(tableMappings, mapping)

							vlog = fmt.Sprintf("Added mapping from dest table: %s.%s -> %s.%s", srcDB, tableName, dbSchemaName, tableName)
							global.Wlog.Debug(vlog)
						}
					}
				} else if strings.Contains(srcPattern, ".") && strings.Contains(dstPattern, ".") {
					// 处理 db1.t1:db2.t2 格式
					srcParts := strings.Split(srcPattern, ".")
					dstParts := strings.Split(dstPattern, ".")

					if len(srcParts) == 2 && len(dstParts) == 2 {
						srcDB := srcParts[0]
						srcTable := srcParts[1]
						dstDB := dstParts[0]
						dstTable := dstParts[1]

						// 检查表名是否包含通配符
						if strings.Contains(srcTable, "%") || strings.Contains(dstTable, "%") {
							// 处理带通配符的表名映射
							for dbName := range dbCheckNameList {
								dbSchemaName, tableName, ok := splitSchemaTableCacheKey(dbName)
								if !ok || !stcls.sourceObjectNameEqual(dbSchemaName, srcDB) {
									continue
								}

								// 检查表名是否匹配源端通配符模式
								matchSrc := false
								if strings.HasPrefix(srcTable, "%") && strings.HasSuffix(srcTable, "%") {
									// 处理 %table% 模式
									tmpTable := strings.ReplaceAll(srcTable, "%", "")
									if strings.Contains(tableName, tmpTable) {
										matchSrc = true
									}
								} else if strings.HasPrefix(srcTable, "%") {
									// 处理 %table 模式
									tmpTable := strings.ReplaceAll(srcTable, "%", "")
									if strings.HasSuffix(tableName, tmpTable) {
										matchSrc = true
									}
								} else if strings.HasSuffix(srcTable, "%") {
									// 处理 table% 模式
									tmpTable := strings.ReplaceAll(srcTable, "%", "")
									if strings.HasPrefix(tableName, tmpTable) {
										matchSrc = true
									}
								}

								if matchSrc {
									// 生成目标端表名
									destTableName := tableName

									// 创建表映射
									mapping := TableMapping{
										SourceSchema: dbSchemaName,
										SourceTable:  tableName,
										DestSchema:   dstDB,
										DestTable:    destTableName,
									}
									tableMappings = append(tableMappings, mapping)

									vlog = fmt.Sprintf("Added wildcard mapping: %s.%s -> %s.%s", dbSchemaName, tableName, dstDB, destTableName)
									global.Wlog.Debug(vlog)
								}
							}
						} else {
							// 处理精确表名映射
							// 创建表映射
							mapping := TableMapping{
								SourceSchema: srcDB,
								SourceTable:  srcTable,
								DestSchema:   dstDB,
								DestTable:    dstTable,
							}
							tableMappings = append(tableMappings, mapping)

							vlog = fmt.Sprintf("Added direct mapping: %s.%s -> %s.%s", srcDB, srcTable, dstDB, dstTable)
							global.Wlog.Debug(vlog)
						}
					}
				}
			}
		} else {
			// 处理非映射模式，如 db1.*
			if strings.HasSuffix(pattern, ".*") {
				srcDB := strings.TrimSuffix(pattern, ".*")

				// 处理忽略表
				ignoreSchema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.ignoreTable, logThreadSeq1)

				// 获取该库中的所有表
				for dbName := range dbCheckNameList {
					dbSchemaName, tableName, ok := splitSchemaTableCacheKey(dbName)
					if !ok || !stcls.sourceObjectNameEqual(dbSchemaName, srcDB) {
						continue
					}

					// ignoreTables should only remove wildcard-selected tables.
					if stcls.shouldIgnoreMatchedTable(ignoreSchema, dbSchemaName, tableName) {
						vlog = fmt.Sprintf("Ignoring table due to ignoreTables: %s.%s", dbSchemaName, tableName)
						global.Wlog.Debug(vlog)
						continue
					}

					// 创建表映射（源端和目标端相同）
					mapping := TableMapping{
						SourceSchema: dbSchemaName,
						SourceTable:  tableName,
						DestSchema:   dbSchemaName,
						DestTable:    tableName,
					}
					tableMappings = append(tableMappings, mapping)

					vlog = fmt.Sprintf("Added non-mapping entry: %s.%s", dbSchemaName, tableName)
					global.Wlog.Debug(vlog)
				}
			} else if strings.Contains(pattern, ".") {
				// 处理 db1.t1 格式
				parts := strings.Split(pattern, ".")
				if len(parts) == 2 {
					srcDB := parts[0]
					srcTable := parts[1]

					// 检查表名是否包含通配符
					if strings.Contains(srcTable, "%") {
						// 处理表名通配符
						for dbName := range dbCheckNameList {
							dbSchemaName, tableName, ok := splitSchemaTableCacheKey(dbName)
							if !ok || !stcls.sourceObjectNameEqual(dbSchemaName, srcDB) {
								continue
							}

							// 检查表名是否匹配通配符模式
							match := false
							if strings.HasPrefix(srcTable, "%") && strings.HasSuffix(srcTable, "%") {
								// 处理 %table% 模式
								tmpTable := strings.ReplaceAll(srcTable, "%", "")
								if strings.Contains(tableName, tmpTable) {
									match = true
								}
							} else if strings.HasPrefix(srcTable, "%") {
								// 处理 %table 模式
								tmpTable := strings.ReplaceAll(srcTable, "%", "")
								if strings.HasSuffix(tableName, tmpTable) {
									match = true
								}
							} else if strings.HasSuffix(srcTable, "%") {
								// 处理 table% 模式
								tmpTable := strings.ReplaceAll(srcTable, "%", "")
								if strings.HasPrefix(tableName, tmpTable) {
									match = true
								}
							}

							if match {
								// 处理忽略表
								ignoreSchema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.ignoreTable, logThreadSeq1)

								if stcls.shouldIgnoreMatchedTable(ignoreSchema, dbSchemaName, tableName) {
									vlog = fmt.Sprintf("Ignoring table due to ignoreTables: %s.%s", dbSchemaName, tableName)
									global.Wlog.Debug(vlog)
									continue
								}

								// 创建表映射（源端和目标端相同）
								mapping := TableMapping{
									SourceSchema: dbSchemaName,
									SourceTable:  tableName,
									DestSchema:   dbSchemaName,
									DestTable:    tableName,
								}
								tableMappings = append(tableMappings, mapping)

								vlog = fmt.Sprintf("Added wildcard matching entry: %s.%s", dbSchemaName, tableName)
								global.Wlog.Debug(vlog)
							}
						}
					} else {
						// 处理精确表名
						// 处理忽略表
						ignoreSchema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.ignoreTable, logThreadSeq1)

						if stcls.shouldIgnoreMatchedTable(ignoreSchema, srcDB, srcTable) {
							vlog = fmt.Sprintf("Ignoring table due to ignoreTables: %s.%s", srcDB, srcTable)
							global.Wlog.Debug(vlog)
							continue
						}

						// 创建表映射（源端和目标端相同）
						mapping := TableMapping{
							SourceSchema: srcDB,
							SourceTable:  srcTable,
							DestSchema:   srcDB,
							DestTable:    srcTable,
						}
						tableMappings = append(tableMappings, mapping)

						vlog = fmt.Sprintf("Added direct non-mapping entry: %s.%s", srcDB, srcTable)
						global.Wlog.Debug(vlog)
					}
				}
			}
		}
	}

	// 如果没有找到任何映射，尝试使用默认方式处理
	if len(tableMappings) == 0 {
		vlog = fmt.Sprintf("No mappings found, using default processing")
		global.Wlog.Debug(vlog)

		// 使用模糊匹配处理表名
		schema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.table, logThreadSeq1)

		// 处理忽略表
		ignoreSchema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.ignoreTable, logThreadSeq1)
		for k := range schema {
			parts := strings.SplitN(k, ".", 2)
			if len(parts) != 2 {
				continue
			}
			if stcls.shouldIgnoreMatchedTable(ignoreSchema, parts[0], parts[1]) {
				delete(schema, k)
			}
		}

		// 构建返回列表
		for k, _ := range schema {
			parts := strings.Split(k, ".")
			if len(parts) == 2 {
				schemaName := parts[0]
				tableName := parts[1]

				// 查找源端schema名
				sourceSchema := schemaName
				destSchema := schemaName

				// 检查是否存在映射关系
				if mappedSchema, exists := stcls.tableMappings[schemaName]; exists {
					destSchema = mappedSchema
				}

				// 创建表映射
				mapping := TableMapping{
					SourceSchema: sourceSchema,
					SourceTable:  tableName,
					DestSchema:   destSchema,
					DestTable:    tableName,
				}
				tableMappings = append(tableMappings, mapping)

				vlog = fmt.Sprintf("Added default mapping: %s.%s -> %s.%s", sourceSchema, tableName, destSchema, tableName)
				global.Wlog.Debug(vlog)
			}
		}
	}

	// 将表映射信息转换为字符串列表，格式为 "sourceSchema.sourceTable:destSchema.destTable"
	for _, mapping := range tableMappings {
		// 构建包含映射信息的表名
		mappedTableName := fmt.Sprintf("%s.%s:%s.%s", mapping.SourceSchema, mapping.SourceTable, mapping.DestSchema, mapping.DestTable)
		f = append(f, mappedTableName)

		// 如果源表和目标表不同，则添加到映射关系列表中
		if mapping.SourceSchema != mapping.DestSchema || mapping.SourceTable != mapping.DestTable {
			mappingRelation := fmt.Sprintf("%s.%s:%s.%s", mapping.SourceSchema, mapping.SourceTable, mapping.DestSchema, mapping.DestTable)
			// 检查是否已存在相同的映射关系
			exists := false
			for _, existingMapping := range TableMappingRelations {
				if existingMapping == mappingRelation {
					exists = true
					break
				}
			}
			if !exists {
				TableMappingRelations = append(TableMappingRelations, mappingRelation)
			}
		}

		vlog = fmt.Sprintf("Final mapped table: %s", mappedTableName)
		global.Wlog.Debug(vlog)
	}

	// For data mode: remove VIEW objects from the check list.
	// Views do not store data independently; including them causes the checksum
	// to run against the view's underlying query, which can hang when the
	// DEFINER account no longer exists (issue #I899YZ).
	if strings.EqualFold(stcls.checkRules.CheckObject, "data") && len(stcls.objectKinds) > 0 {
		filtered := f[:0]
		skipped := 0
		for _, entry := range f {
			// entry format: "srcSchema.srcTable:dstSchema.dstTable"
			srcPart := entry
			if idx := strings.Index(entry, ":"); idx >= 0 {
				srcPart = entry[:idx]
			}
			parts := strings.SplitN(srcPart, ".", 2)
			if len(parts) == 2 {
				key := fmt.Sprintf("%s/*schema&table*/%s", parts[0], parts[1])
				if strings.EqualFold(strings.ToLower(stcls.caseSensitiveObjectName), "no") {
					key = strings.ToLower(key)
				}
				if stcls.objectKinds[key] == "VIEW" {
					vlog = fmt.Sprintf("(%d) Skipping VIEW in data mode: %s", logThreadSeq1, srcPart)
					global.Wlog.Info(vlog)
					skipped++
					continue
				}
			}
			filtered = append(filtered, entry)
		}
		if skipped > 0 {
			f = filtered
			vlog = fmt.Sprintf("(%d) data mode: skipped %d VIEW object(s), %d object(s) remain.", logThreadSeq1, skipped, len(f))
			global.Wlog.Info(vlog)
		}
	}

	vlog = fmt.Sprintf("(%d) Obtain schema.table %s success, num [%d].", logThreadSeq1, f, len(f))
	global.Wlog.Info(vlog)
	return f, nil
}

/*
库表的所有列信息
*/
func (stcls *schemaTable) SchemaTableAllCol(tableList []string, logThreadSeq, logThreadSeq2 int64) map[string]global.TableAllColumnInfoS {
	var (
		a, b           []map[string]interface{}
		err            error
		vlog           string
		tableCol       = make(map[string]global.TableAllColumnInfoS)
		interfToString = func(colData []map[string]interface{}) []map[string]string {
			kel := make([]map[string]string, 0)
			for i := range colData {
				ke := make(map[string]string)
				for ii, iv := range colData[i] {
					ke[ii] = fmt.Sprintf("%v", iv)
				}
				kel = append(kel, ke)
			}
			return kel
		}
	)

	vlog = fmt.Sprintf("(%d) Start to obtain the metadata information of the source-target verification table ...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range tableList {
		// 添加调试日志，查看当前处理的表项
		vlog = fmt.Sprintf("(%d) Processing table entry: %s", logThreadSeq, i)
		global.Wlog.Debug(vlog)

		var sourceSchema, tableName, destSchema, destTableName string

		// 检查是否包含映射关系（格式为 sourceSchema.sourceTable:destSchema.destTable）
		if strings.Contains(i, ":") {
			parts := strings.Split(i, ":")
			if len(parts) == 2 {
				sourceParts := strings.Split(parts[0], ".")
				destParts := strings.Split(parts[1], ".")

				if len(sourceParts) == 2 && len(destParts) == 2 {
					sourceSchema = sourceParts[0]
					tableName = sourceParts[1]
					destSchema = destParts[0]
					destTableName = destParts[1]

					vlog = fmt.Sprintf("(%d) Parsed mapping: sourceSchema=%s, tableName=%s, destSchema=%s, destTableName=%s", logThreadSeq, sourceSchema, tableName, destSchema, destTableName)
					global.Wlog.Debug(vlog)
				} else {
					vlog = fmt.Sprintf("(%d) Invalid table mapping format: %s", logThreadSeq, i)
					global.Wlog.Error(vlog)
					continue
				}
			} else {
				vlog = fmt.Sprintf("(%d) Invalid table mapping format: %s", logThreadSeq, i)
				global.Wlog.Error(vlog)
				continue
			}
		} else {
			// 传统格式：schema.table
			parts := strings.Split(i, ".")
			if len(parts) == 2 {
				sourceSchema = parts[0]
				tableName = parts[1]
				destTableName = tableName

				// 根据映射规则确定目标端schema
				destSchema = sourceSchema
				if mappedSchema, exists := stcls.tableMappings[sourceSchema]; exists {
					destSchema = mappedSchema
				}

				vlog = fmt.Sprintf("(%d) Traditional format: sourceSchema=%s, tableName=%s, destSchema=%s", logThreadSeq, sourceSchema, tableName, destSchema)
				global.Wlog.Debug(vlog)
			} else {
				vlog = fmt.Sprintf("(%d) Invalid table format: %s", logThreadSeq, i)
				global.Wlog.Error(vlog)
				continue
			}
		}

		vlog = fmt.Sprintf("(%d) Start to query all column information of srcDSN {%s} table %s.%s", logThreadSeq, stcls.sourceDrive, sourceSchema, tableName)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: sourceSchema, Table: tableName, Drive: stcls.sourceDrive}
		a, err = tc.Query().TableAllColumn(stcls.sourceDB, logThreadSeq2)
		if err != nil {
			return nil
		}
		vlog = fmt.Sprintf("(%d) All column information query of srcDSN {%s} table %s.%s is completed", logThreadSeq, stcls.sourceDrive, sourceSchema, tableName)
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) Start to query all column information of dstDSN {%s} table %s.%s", logThreadSeq, stcls.destDrive, destSchema, destTableName)
		global.Wlog.Debug(vlog)
		tc.Schema = destSchema
		tc.Table = destTableName
		tc.Drive = stcls.destDrive
		b, err = tc.Query().TableAllColumn(stcls.destDB, logThreadSeq2)
		if err != nil {
			return nil
		}
		vlog = fmt.Sprintf("(%d) All column information query of dstDSN {%s} table %s.%s is completed", logThreadSeq, stcls.destDrive, destSchema, destTableName)
		global.Wlog.Debug(vlog)
		sourceColInfo := interfToString(a)
		destColInfo := interfToString(b)
		if strings.EqualFold(stcls.checkRules.CheckObject, "data") {
			var strippedGeneratedColumns []string
			sourceColInfo, destColInfo, strippedGeneratedColumns = normalizeDataCheckColumnInfo(sourceColInfo, destColInfo)
			if len(strippedGeneratedColumns) > 0 {
				vlog = fmt.Sprintf("(%d) Stripped generated invisible columns from data-check target metadata for %s.%s -> %s.%s: %v", logThreadSeq, sourceSchema, tableName, destSchema, destTableName, strippedGeneratedColumns)
				global.Wlog.Info(vlog)
			}
		}
		entry := global.TableAllColumnInfoS{
			SColumnInfo: sourceColInfo,
			DColumnInfo: destColInfo,
		}
		srcKey := fmt.Sprintf("%s_gtchecksum_%s", sourceSchema, tableName)
		dstKey := fmt.Sprintf("%s_gtchecksum_%s", destSchema, destTableName)
		tableCol[srcKey] = entry
		if dstKey != srcKey {
			tableCol[dstKey] = entry
		}
		vlog = fmt.Sprintf("(%d) all column information query of source table %s.%s and target table %s.%s is completed. table column message is {source: %s, dest: %s}", logThreadSeq, sourceSchema, tableName, destSchema, destTableName, sourceColInfo, destColInfo)
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) The metadata information of the source target verification table has been obtained", logThreadSeq)
	global.Wlog.Info(vlog)
	return tableCol
}

/*
获取校验表的索引列信息，包含是否有索引，列名，列序号
*/
func (stcls *schemaTable) TableIndexColumn(dtabS []string, logThreadSeq, logThreadSeq2 int64) map[string][]string {
	var (
		vlog                string
		tableIndexColumnMap = make(map[string][]string)
	)
	vlog = fmt.Sprintf("(%d) Start to query the table index listing information and select the appropriate index ...", logThreadSeq)
	global.Wlog.Info(vlog)

	// 添加调试日志，查看传入的表列表和映射规则
	vlog = fmt.Sprintf("TableIndexColumn received dtabS: %v", dtabS)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("Current table mappings: %v", stcls.tableMappings)
	global.Wlog.Debug(vlog)

	workers := stcls.tableIndexMetaWorkerCount(len(dtabS))
	vlog = fmt.Sprintf("(%d) TableIndexColumn worker pool size: %d", logThreadSeq, workers)
	global.Wlog.Debug(vlog)

	type tableIndexJob struct {
		rawEntry     string
		sourceSchema string
		sourceTable  string
		destSchema   string
		destTable    string
	}

	jobs := make(chan tableIndexJob, len(dtabS))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				startAt := time.Now()
				logMsg := fmt.Sprintf("Processing table entry: %s", job.rawEntry)
				global.Wlog.Debug(logMsg)

				logMsg = fmt.Sprintf("Parsed mapping: sourceSchema=%s, sourceTable=%s, destSchema=%s, destTable=%s",
					job.sourceSchema, job.sourceTable, job.destSchema, job.destTable)
				global.Wlog.Debug(logMsg)

				logMsg = fmt.Sprintf("(%d) Start querying source index metadata for table %s.%s (target mapping %s.%s)",
					logThreadSeq, job.sourceSchema, job.sourceTable, job.destSchema, job.destTable)
				global.Wlog.Debug(logMsg)

				idxc := dbExec.IndexColumnStruct{Schema: job.sourceSchema, Table: job.sourceTable, Drivce: stcls.sourceDrive, CaseSensitiveObjectName: stcls.caseSensitiveObjectName}
				queryData, err := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
				if err != nil {
					logMsg = fmt.Sprintf("(%d) Error querying source table index for %s.%s: %v", logThreadSeq, job.sourceSchema, job.sourceTable, err)
					global.Wlog.Error(logMsg)
					continue
				}

				tc := dbExec.TableColumnNameStruct{Schema: job.sourceSchema, Table: job.sourceTable, Drive: stcls.sourceDrive, Db: stcls.sourceDB}
				indexType := tc.Query().TableIndexChoice(queryData, logThreadSeq2)
				logMsg = fmt.Sprintf("(%d) Source table %s.%s index list information query completed. index list message is {%v}",
					logThreadSeq, job.sourceSchema, job.sourceTable, indexType)
				global.Wlog.Debug(logMsg)

				displayTableName := fmt.Sprintf("%s.%s:%s.%s", job.sourceSchema, job.sourceTable, job.destSchema, job.destTable)

				if len(indexType) == 0 {
					key := fmt.Sprintf("%s/*gtchecksumSchemaTable*/%s/*mapping*/%s/*mappingTable*/%s",
						job.sourceSchema, job.sourceTable, job.destSchema, job.destTable)
					mu.Lock()
					tableIndexColumnMap[key] = []string{}
					mu.Unlock()

					logMsg = fmt.Sprintf("(%d) The source table %s has no index.", logThreadSeq, displayTableName)
					global.Wlog.Warn(logMsg)
				} else {
					logMsg = fmt.Sprintf("(%d) Start to perform index selection on source table %s.%s according to the algorithm",
						logThreadSeq, job.sourceSchema, job.sourceTable)
					global.Wlog.Debug(logMsg)

					ab, aa := stcls.tableIndexAlgorithm(indexType)
					key := fmt.Sprintf("%s/*gtchecksumSchemaTable*/%s/*indexColumnType*/%s/*mapping*/%s/*mappingTable*/%s",
						job.sourceSchema, job.sourceTable, ab, job.destSchema, job.destTable)
					mu.Lock()
					tableIndexColumnMap[key] = aa
					mu.Unlock()

					logMsg = fmt.Sprintf("(%d) The index selection of source table %s is completed, and the selected index information is { keyName:%s keyColumn: %s}",
						logThreadSeq, displayTableName, ab, aa)
					global.Wlog.Debug(logMsg)
				}

				logMsg = fmt.Sprintf("(%d) Source index metadata phase completed for %s in %s", logThreadSeq, displayTableName, time.Since(startAt).Round(time.Millisecond))
				global.Wlog.Debug(logMsg)
			}
		}()
	}

	seen := make(map[string]struct{}, len(dtabS))
	for _, entry := range dtabS {
		sourceSchema, sourceTable, destSchema, destTable, ok := parseSchemaTableMappingEntry(entry)
		if !ok {
			vlog = fmt.Sprintf("Skip invalid table entry in TableIndexColumn: %s", entry)
			global.Wlog.Warn(vlog)
			continue
		}

		uniqueKey := fmt.Sprintf("%s.%s:%s.%s", sourceSchema, sourceTable, destSchema, destTable)
		if _, exists := seen[uniqueKey]; exists {
			continue
		}
		seen[uniqueKey] = struct{}{}

		jobs <- tableIndexJob{
			rawEntry:     entry,
			sourceSchema: sourceSchema,
			sourceTable:  sourceTable,
			destSchema:   destSchema,
			destTable:    destTable,
		}
	}
	close(jobs)
	wg.Wait()

	vlog = fmt.Sprintf("(%d) Table index listing information and appropriate index completion", logThreadSeq)
	global.Wlog.Info(vlog)
	return tableIndexColumnMap
}

func (stcls *schemaTable) tableIndexMetaWorkerCount(tableCount int) int {
	if tableCount <= 1 {
		return 1
	}

	workers := stcls.checkRules.ParallelThds
	if workers <= 0 {
		workers = 4
	}
	if workers < 2 {
		workers = 2
	}
	if workers > 8 {
		workers = 8
	}
	if workers > tableCount {
		workers = tableCount
	}

	return workers
}

func parseSchemaTableMappingEntry(entry string) (sourceSchema, sourceTable, destSchema, destTable string, ok bool) {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return "", "", "", "", false
	}

	if strings.Contains(entry, ":") {
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			return "", "", "", "", false
		}

		sourceParts := strings.Split(parts[0], ".")
		destParts := strings.Split(parts[1], ".")
		if len(sourceParts) != 2 || len(destParts) != 2 {
			return "", "", "", "", false
		}

		if sourceParts[0] == "" || sourceParts[1] == "" || destParts[0] == "" || destParts[1] == "" {
			return "", "", "", "", false
		}

		return sourceParts[0], sourceParts[1], destParts[0], destParts[1], true
	}

	parts := strings.Split(entry, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", "", "", false
	}

	return parts[0], parts[1], parts[0], parts[1], true
}

// 解析表映射规则
func (stcls *schemaTable) parseTableMappings(Ftable string) {
	stcls.tableMappings = make(map[string]string)

	vlog := fmt.Sprintf("Parsing table mappings for pattern: %s", Ftable)
	global.Wlog.Debug(vlog)

	// 解析映射规则，如 db1.*:db2.*
	for _, pattern := range strings.Split(Ftable, ",") {
		vlog = fmt.Sprintf("Processing pattern: %s", pattern)
		global.Wlog.Debug(vlog)

		if strings.Contains(pattern, ":") {
			mapping := strings.SplitN(pattern, ":", 2)
			if len(mapping) == 2 {
				srcPattern := mapping[0]
				dstPattern := mapping[1]

				vlog = fmt.Sprintf("Found mapping: %s -> %s", srcPattern, dstPattern)
				global.Wlog.Debug(vlog)

				// 处理 db1.*:db2.* 格式
				if strings.HasSuffix(srcPattern, ".*") && strings.HasSuffix(dstPattern, ".*") {
					srcDB := strings.TrimSuffix(srcPattern, ".*")
					dstDB := strings.TrimSuffix(dstPattern, ".*")
					stcls.tableMappings[srcDB] = dstDB
					vlog = fmt.Sprintf("Mapped (.* format): %s -> %s", srcDB, dstDB)
					global.Wlog.Debug(vlog)
				} else if strings.HasSuffix(srcPattern, "*") && strings.HasSuffix(dstPattern, "*") {
					// 处理 db1*:db2* 格式 (针对用户输入的"db1.*:db2.*"但实际被解析为"db1*:db2*"的情况)
					srcDB := strings.TrimSuffix(srcPattern, "*")
					dstDB := strings.TrimSuffix(dstPattern, "*")
					stcls.tableMappings[srcDB] = dstDB
					vlog = fmt.Sprintf("Mapped (* format): %s -> %s", srcDB, dstDB)
					global.Wlog.Debug(vlog)
				} else {
					// 处理其他格式的映射，如 db1.t1:db2.t2
					srcParts := strings.Split(srcPattern, ".")
					dstParts := strings.Split(dstPattern, ".")

					if len(srcParts) > 0 && len(dstParts) > 0 {
						srcDB := srcParts[0]
						dstDB := dstParts[0]
						stcls.tableMappings[srcDB] = dstDB
						vlog = fmt.Sprintf("Mapped (direct format): %s -> %s", srcDB, dstDB)
						global.Wlog.Debug(vlog)
					}
				}
			}
		} else {
			// 处理非映射模式，如 db1.*
			if strings.HasSuffix(pattern, ".*") {
				srcDB := strings.TrimSuffix(pattern, ".*")
				stcls.tableMappings[srcDB] = srcDB // 没有映射时，源和目标相同
				vlog = fmt.Sprintf("Non-mapping pattern (.* format): %s", srcDB)
				global.Wlog.Debug(vlog)
			} else if strings.HasSuffix(pattern, "*") {
				srcDB := strings.TrimSuffix(pattern, "*")
				stcls.tableMappings[srcDB] = srcDB // 没有映射时，源和目标相同
				vlog = fmt.Sprintf("Non-mapping pattern (* format): %s", srcDB)
				global.Wlog.Debug(vlog)
			} else if strings.Contains(pattern, ".") {
				// 处理 db1.t1 格式
				srcParts := strings.Split(pattern, ".")
				if len(srcParts) > 0 {
					srcDB := srcParts[0]
					stcls.tableMappings[srcDB] = srcDB
					vlog = fmt.Sprintf("Non-mapping pattern (direct format): %s", srcDB)
					global.Wlog.Debug(vlog)
				}
			}
		}
	}

	vlog = fmt.Sprintf("Final table mappings: %v", stcls.tableMappings)
	global.Wlog.Debug(vlog)
}

/*
校验触发器
*/
func (stcls *schemaTable) Trigger(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog       string
		tmpM       = make(map[string]int)
		schemaMap  = make(map[string]int)
		triggerMap = make(map[string]string) // 存储具体的触发器名称
		c, d       []string
		pods       = Pod{
			Datafix:     stcls.datafix,
			CheckObject: "trigger",
		}
		sourceTrigger, destTrigger map[string]string
		err                        error
	)

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Trigger. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)

	// 从dtabS中提取schema信息和触发器名称
	for _, i := range dtabS {
		// 处理映射格式 schema.trigger:schema.trigger
		if strings.Contains(i, ":") {
			parts := strings.Split(i, ":")
			if len(parts) == 2 {
				sourceParts := strings.Split(parts[0], ".")
				if len(sourceParts) >= 1 {
					schema := sourceParts[0]

					// schema的名字要区分大小写
					if stcls.caseSensitiveObjectName == "yes" {
						// 当区分大小写时，保持原始大小写
					} else {
						// 当不区分大小写时，也保持原始大小写
					}
					schemaMap[schema] = 1

					// 如果指定了具体的触发器名称
					if len(sourceParts) >= 2 && sourceParts[1] != "*" {
						// 保持trigger名称的原始大小写
						triggerName := sourceParts[1]
						triggerMap[schema+"."+triggerName] = triggerName
					}
				}
			}
		} else {
			// 处理普通格式 schema.trigger 或 schema.*
			parts := strings.Split(i, ".")
			if len(parts) >= 1 {
				schema := parts[0]

				if stcls.caseSensitiveObjectName == "yes" {
					// 当区分大小写时，保持原始大小写
				} else {
					// 当不区分大小写时，也保持原始大小写
				}
				schemaMap[schema] = 1

				// 如果指定了具体的触发器名称
				if len(parts) >= 2 && parts[1] != "*" {
					triggerName := parts[1]
					triggerMap[schema+"."+triggerName] = triggerName
				}
			}
		}
	}

	// 添加调试日志，显示提取的schema和触发器信息
	vlog = fmt.Sprintf("(%d) Extracted schema map: %v, trigger map: %v", logThreadSeq, schemaMap, triggerMap)
	global.Wlog.Debug(vlog)

	// 如果schemaMap为空，但stcls.schema不为空，则使用stcls.schema
	if len(schemaMap) == 0 && stcls.schema != "" {
		schema := stcls.schema
		if stcls.caseSensitiveObjectName == "yes" {
			// 当区分大小写时，保持原始大小写
		} else {
			// 当不区分大小写时，也保持原始大小写
		}
		schemaMap[schema] = 1
		vlog = fmt.Sprintf("(%d) No schema found in dtabS, using default schema: %s", logThreadSeq, schema)
		global.Wlog.Debug(vlog)
	}
	//校验触发器
	for schema, _ := range schemaMap {
		pods.Schema = schema
		vlog = fmt.Sprintf("(%d) Start processing srcDSN {%s} databases %s Trigger. to dispos it...", logThreadSeq, stcls.sourceDrive, schema)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{
			Schema:                  schema,
			Drive:                   stcls.sourceDrive,
			CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
		}

		// 获取源数据库的触发器
		if sourceTrigger, err = tc.Query().Trigger(stcls.sourceDB, logThreadSeq2); err != nil {
			vlog = fmt.Sprintf("(%d) Error querying source triggers: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return
		}

		// 如果有指定具体的触发器，则过滤结果
		if len(triggerMap) > 0 {
			filteredSourceTrigger := make(map[string]string)
			for k, v := range sourceTrigger {
				// 提取触发器名称时需要更加小心
				parts := strings.Split(k, ".")
				var triggerName string
				if len(parts) > 1 {
					// 移除可能存在的引号
					triggerName = strings.ReplaceAll(parts[1], "\"", "")
				} else {
					// 如果没有点号，使用整个键
					triggerName = strings.ReplaceAll(k, "\"", "")
				}

				// 保持trigger名称的原始大小写，不做转换

				triggerKey := schema + "." + triggerName

				// 添加调试日志
				vlog = fmt.Sprintf("(%d) Checking trigger: %s, key: %s", logThreadSeq, k, triggerKey)
				global.Wlog.Debug(vlog)

				// 检查是否在过滤映射中
				if _, exists := triggerMap[triggerKey]; exists {
					filteredSourceTrigger[k] = v
					vlog = fmt.Sprintf("(%d) Keeping trigger: %s", logThreadSeq, k)
					global.Wlog.Debug(vlog)
				}
			}
			sourceTrigger = filteredSourceTrigger
		} else {
			// 如果triggerMap为空（表示使用通配符），则不进行过滤，保留所有触发器
			vlog = fmt.Sprintf("(%d) No specific triggers specified, keeping all %d source triggers", logThreadSeq, len(sourceTrigger))
			global.Wlog.Debug(vlog)

			// 当使用通配符时，将所有触发器名称添加到triggerMap中，以便后续比较
			for k, _ := range sourceTrigger {
				parts := strings.Split(k, ".")
				var triggerName string
				if len(parts) > 1 {
					triggerName = strings.ReplaceAll(parts[1], "\"", "")
				} else {
					triggerName = strings.ReplaceAll(k, "\"", "")
				}

				// 保持trigger名称的原始大小写，不做转换

				triggerKey := schema + "." + triggerName
				triggerMap[triggerKey] = triggerName
				vlog = fmt.Sprintf("(%d) Added trigger to map: %s", logThreadSeq, triggerKey)
				global.Wlog.Debug(vlog)
			}
		}

		vlog = fmt.Sprintf("(%d) srcDSN {%s} databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, schema, sourceTrigger)
		global.Wlog.Debug(vlog)

		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} databases %s Trigger data. to dispos it...", logThreadSeq, stcls.destDrive, schema)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive

		// 获取目标数据库的触发器
		if destTrigger, err = tc.Query().Trigger(stcls.destDB, logThreadSeq2); err != nil {
			vlog = fmt.Sprintf("(%d) Error querying destination triggers: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return
		}

		// 如果有指定具体的触发器，则过滤结果
		if len(triggerMap) > 0 {
			filteredDestTrigger := make(map[string]string)
			for k, v := range destTrigger {
				// 提取触发器名称时需要更加小心
				parts := strings.Split(k, ".")
				var triggerName string
				if len(parts) > 1 {
					// 移除可能存在的引号
					triggerName = strings.ReplaceAll(parts[1], "\"", "")
				} else {
					// 如果没有点号，使用整个键
					triggerName = strings.ReplaceAll(k, "\"", "")
				}

				// 保持trigger名称的原始大小写，不做转换

				triggerKey := schema + "." + triggerName

				// 添加调试日志
				vlog = fmt.Sprintf("(%d) Checking dest trigger: %s, key: %s", logThreadSeq, k, triggerKey)
				global.Wlog.Debug(vlog)

				// 检查是否在过滤映射中
				if _, exists := triggerMap[triggerKey]; exists {
					filteredDestTrigger[k] = v
					vlog = fmt.Sprintf("(%d) Keeping dest trigger: %s", logThreadSeq, k)
					global.Wlog.Debug(vlog)
				}
			}
			destTrigger = filteredDestTrigger
		} else {
			// 如果triggerMap为空（表示使用通配符），则不进行过滤，保留所有触发器
			vlog = fmt.Sprintf("(%d) No specific triggers specified, keeping all %d destination triggers", logThreadSeq, len(destTrigger))
			global.Wlog.Debug(vlog)

			// 当使用通配符时，将所有目标端触发器名称也添加到triggerMap中
			for k, _ := range destTrigger {
				parts := strings.Split(k, ".")
				var triggerName string
				if len(parts) > 1 {
					triggerName = strings.ReplaceAll(parts[1], "\"", "")
				} else {
					triggerName = strings.ReplaceAll(k, "\"", "")
				}

				// 保持trigger名称的原始大小写，不做转换

				triggerKey := schema + "." + triggerName
				triggerMap[triggerKey] = triggerName
				vlog = fmt.Sprintf("(%d) Added dest trigger to map: %s", logThreadSeq, triggerKey)
				global.Wlog.Debug(vlog)
			}
		}

		vlog = fmt.Sprintf("(%d) dstDSN {%s} databases %s message is {%s}", logThreadSeq, stcls.destDrive, schema, destTrigger)
		global.Wlog.Debug(vlog)

		sourceTriggerComments := make(map[string]string)
		destTriggerComments := make(map[string]string)
		sourceTriggerDefiners := make(map[string]string)
		destTriggerDefiners := make(map[string]string)
		if stcls.shouldCompareTriggerMetadata() {
			sourceTriggerComments, sourceTriggerDefiners = loadMySQLTriggerMetadata(stcls.sourceDB, schema, logThreadSeq)
			destTriggerComments, destTriggerDefiners = loadMySQLTriggerMetadata(stcls.destDB, schema, logThreadSeq)
		}

		if len(sourceTrigger) == 0 && len(destTrigger) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, schema)
			global.Wlog.Debug(vlog)
			continue
		}

		tmpM = make(map[string]int)
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Trigger. to dispos it...", logThreadSeq, schema)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceTrigger {
			tmpM[k]++
		}
		for k, _ := range destTrigger {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Trigger is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		for k, _ := range tmpM {
			pods.TriggerName = strings.ReplaceAll(strings.Split(k, ".")[1], "\"", "")
			definitionDiff := sourceTrigger[k] != destTrigger[k]
			collationMappedOnly := false
			if definitionDiff && stcls.isMariaDBToMySQL() {
				mappedSource := mapMariaDBCollationInRoutineSQL(sourceTrigger[k])
				if mappedSource == destTrigger[k] {
					definitionDiff = false
					collationMappedOnly = true
					global.Wlog.Debug(fmt.Sprintf("(%d) Trigger %s definition matches after MariaDB collation mapping", logThreadSeq, k))
				}
			}
			commentDiff := false
			definerDiff := false
			if stcls.shouldCompareTriggerMetadata() {
				sourceComment := normalizeMetadataComment(sourceTriggerComments[k])
				destComment := normalizeMetadataComment(destTriggerComments[k])
				if sourceComment != destComment {
					commentDiff = true
					vlog = fmt.Sprintf("(%d) Trigger comment mismatch %s: source=%q, dest=%q", logThreadSeq, k, sourceComment, destComment)
					global.Wlog.Warn(vlog)
				}

				sourceDefiner := strings.TrimSpace(sourceTriggerDefiners[k])
				destDefiner := strings.TrimSpace(destTriggerDefiners[k])
				if sourceDefiner != destDefiner {
					definerDiff = true
					vlog = fmt.Sprintf("(%d) Trigger definer mismatch %s: source=%q, dest=%q", logThreadSeq, k, sourceDefiner, destDefiner)
					global.Wlog.Warn(vlog)
				}
			}

			// MariaDB→MySQL：当 body 和其他属性均一致时，检查 charset 会话元数据的 collation 差异
			metadataCollationDiff := false
			if !definitionDiff && !commentDiff && !definerDiff && !collationMappedOnly && stcls.isMariaDBToMySQL() {
				trName := strings.ReplaceAll(strings.Split(k, ".")[1], "\"", "")
				srcResult, srcErr := showCreateTriggerSQLWithCharset(stcls.sourceDB, schema, trName)
				dstResult, dstErr := showCreateTriggerSQLWithCharset(stcls.destDB, schema, trName)
				if srcErr == nil && dstErr == nil {
					if isCharsetMetadataCollationMapped(srcResult.CharacterSetClient, srcResult.CollationConnection, srcResult.DatabaseCollation,
						dstResult.CharacterSetClient, dstResult.CollationConnection, dstResult.DatabaseCollation) {
						// uca1400→0900 映射（仅 MariaDB 11.5+ 触发）
						collationMappedOnly = true
						global.Wlog.Debug(fmt.Sprintf("(%d) Trigger %s charset metadata collation-mapped: uca1400→0900 drift (src=%s/%s dst=%s/%s)", logThreadSeq, k, srcResult.CollationConnection, srcResult.DatabaseCollation, dstResult.CollationConnection, dstResult.DatabaseCollation))
					} else if hasCharsetMetadataCollationDiff(srcResult.CharacterSetClient, srcResult.CollationConnection, srcResult.DatabaseCollation,
						dstResult.CharacterSetClient, dstResult.CollationConnection, dstResult.DatabaseCollation) {
						// 非可映射的 collation 差异（如 general_ci ↔ 0900_ai_ci），需生成 fix SQL
						metadataCollationDiff = true
						global.Wlog.Warn(fmt.Sprintf("(%d) Trigger %s charset metadata collation mismatch requiring fix SQL (src=%s/%s dst=%s/%s)", logThreadSeq, k, srcResult.CollationConnection, srcResult.DatabaseCollation, dstResult.CollationConnection, dstResult.DatabaseCollation))
					}
				}
			}

			if definitionDiff || commentDiff || definerDiff || metadataCollationDiff {
				pods.DIFFS = "yes"
				d = append(d, k)

				// Rebuild full trigger DDL from INFORMATION_SCHEMA instead of relying
				// on the body-only statement column returned by SHOW CREATE TRIGGER.
				trName := strings.ReplaceAll(strings.Split(k, ".")[1], "\"", "")
				trResult, showCreateErr := showCreateTriggerSQLWithCharset(stcls.sourceDB, schema, trName)
				trSourceDef := trResult.CreateSQL
				if showCreateErr != nil {
					global.Wlog.Warn(fmt.Sprintf("(%d) Failed to rebuild source trigger DDL for %s.%s: %v", logThreadSeq, schema, trName, showCreateErr))
					trSourceDef = sourceTrigger[k]
				}
				// 确定目标schema
				destSchema := schema
				if mappedSchema, exists := stcls.tableMappings[schema]; exists {
					destSchema = mappedSchema
				}
				// MariaDB→MySQL：映射源端定义中的 MariaDB 特有 collation
				if stcls.isMariaDBToMySQL() {
					trSourceDef = mapMariaDBCollationInRoutineSQL(trSourceDef)
				}
				tsqls := mysql.GenerateTriggerFixSQL(schema, destSchema, trName, trSourceDef)
				// 在 DROP/CREATE 语句前插入 charset session 变量设置
				if showCreateErr == nil && trResult.CharacterSetClient != "" {
					charsetSetStmts := buildTriggerCharsetSetStatements(trResult, stcls.isMariaDBToMySQL())
					if len(charsetSetStmts) > 0 {
						enriched := make([]string, 0, len(charsetSetStmts)+len(tsqls))
						enriched = append(enriched, charsetSetStmts...)
						enriched = append(enriched, tsqls...)
						tsqls = enriched
					}
				}
				// 每个 trigger 写入独立文件（trigger.schema.triggername.sql）
				out := make([]string, 0, len(tsqls)+2)
				out = append(out, "DELIMITER $$")
				for _, stmt := range tsqls {
					out = append(out, stmt+"\n$$")
				}
				out = append(out, "DELIMITER ;")
				origSchema, origTable, origObjType := stcls.schema, stcls.table, stcls.fixFileObjectType
				stcls.schema = schema
				stcls.table = trName
				stcls.fixFileObjectType = "trigger"
				if werr := stcls.writeFixSql(out, logThreadSeq); werr != nil {
					global.Wlog.Error(fmt.Sprintf("(%d) failed to write trigger fix SQL for %s.%s: %v", logThreadSeq, schema, trName, werr))
				}
				stcls.schema, stcls.table, stcls.fixFileObjectType = origSchema, origTable, origObjType
			} else if collationMappedOnly {
				pods.DIFFS = global.SkipDiffsCollationMapped
				c = append(c, k)
				global.Wlog.Debug(fmt.Sprintf("(%d) Trigger %s collation-mapped: only uca1400→0900 collation difference, no fix SQL generated", logThreadSeq, k))
			} else {
				pods.DIFFS = "no"
				c = append(c, k)
			}
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Trigger. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			vlog = fmt.Sprintf("(%d) The source target segment databases %s Trigger data verification is completed", logThreadSeq, schema)
			global.Wlog.Debug(vlog)
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Trigger data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

/*
校验存储过程
*/
/*
最小入侵新增：统一附加与刷新方法
*/
func (stcls *schemaTable) setAggregate(on bool) {
	stcls.aggregate = on
}

func (stcls *schemaTable) appendPod(p Pod) {
	if stcls.aggregate {
		stcls.podsBuffer = append(stcls.podsBuffer, p)
	} else {
		measuredDataPods = append(measuredDataPods, p)
	}
}

func (stcls *schemaTable) flushPods() {
	if len(stcls.podsBuffer) > 0 {
		measuredDataPods = append(measuredDataPods, stcls.podsBuffer...)
		stcls.podsBuffer = nil
	}
}

/*
最小入侵新增：以返回值形式获取 Proc 结果
- 通过临时开启 aggregate 模式，复用现有 Proc 逻辑来采集 pods
- 调用结束后恢复原 aggregate 与 podsBuffer 状态
*/
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

func (stcls *schemaTable) Foreign(dtabS []string, logThreadSeq, logThreadSeq2 int64, isCalledFromStruct ...bool) {
	var (
		vlog                       string
		sourceForeign, destForeign map[string]string
		err                        error
		pods                       = Pod{
			Datafix:     "no",
			CheckObject: "foreign",
		}
	)

	// 如果是从 Struct 函数调用的，则将 CheckObject 设置为 "struct"
	if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
		pods.CheckObject = "struct"
	}

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Foreign. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	//校验外键
	var c, d []string
	for _, i := range dtabS {
		sourceSchema, sourceTable, destSchema, destTable := parseSourceAndDestTablePair(i, stcls.tableMappings)
		stcls.schema = sourceSchema
		stcls.table = sourceTable
		stcls.destTable = destTable
		vlog = fmt.Sprintf("(%d) Start processing srcDSN {%s} table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.sourceDrive, sourceSchema, sourceTable)
		global.Wlog.Debug(vlog)
		pods.Schema = sourceSchema
		pods.Table = sourceTable
		tc := dbExec.TableColumnNameStruct{Schema: sourceSchema, Table: sourceTable, Drive: stcls.sourceDrive}
		if sourceForeign, err = tc.Query().Foreign(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) srcDSN {%s} table %s.%s message is {%s}", logThreadSeq, stcls.sourceDrive, sourceSchema, sourceTable, sourceForeign)
		global.Wlog.Debug(vlog)

		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.destDrive, destSchema, destTable)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive
		tc.Schema = destSchema
		tc.Table = destTable
		if destForeign, err = tc.Query().Foreign(stcls.destDB, logThreadSeq2); err != nil {
			return
		}

		vlog = fmt.Sprintf("(%d) dstDSN {%s} table %s.%s message is {%s}", logThreadSeq, stcls.destDrive, destSchema, destTable, destForeign)
		global.Wlog.Debug(vlog)
		if len(sourceForeign) == 0 && len(destForeign) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, sourceSchema, sourceTable)
			global.Wlog.Debug(vlog)
			continue
		}

		sourceCanonicalFKs := schemacompat.CanonicalizeForeignKeyDefinitions(sourceForeign)
		destCanonicalFKs := schemacompat.CanonicalizeForeignKeyDefinitions(destForeign)
		sourceByName := make(map[string]schemacompat.CanonicalConstraint)
		destByName := make(map[string]schemacompat.CanonicalConstraint)
		unionNames := make(map[string]struct{})
		for _, fk := range sourceCanonicalFKs {
			sourceByName[fk.Name] = fk
			unionNames[fk.Name] = struct{}{}
		}
		for _, fk := range destCanonicalFKs {
			destByName[fk.Name] = fk
			unionNames[fk.Name] = struct{}{}
		}

		vlog = fmt.Sprintf("(%d) Start to compare whether the Foreign table is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		// 初始化为"no"，如果发现任何不一致，则设置为"yes"
		pods.DIFFS = "no"
		advisorySuggestions := make([]schemacompat.ConstraintRepairSuggestion, 0)

		for fkName := range unionNames {
			sourceFK, sourceExists := sourceByName[fkName]
			destFK, destExists := destByName[fkName]
			if !sourceExists || !destExists {
				pods.DIFFS = "yes"
				d = append(d, fkName)
				vlog = fmt.Sprintf("(%d) Foreign key %s existence mismatch on table %s.%s", logThreadSeq, fkName, sourceSchema, sourceTable)
				global.Wlog.Warn(vlog)
				continue
			}

			decision := schemacompat.DecideForeignKeyCompatibility(sourceFK, destFK)
			if decision.IsMismatch() {
				pods.DIFFS = "yes"
				d = append(d, fkName)
				vlog = fmt.Sprintf("(%d) Foreign key %s definition mismatch on table %s.%s: %s", logThreadSeq, fkName, sourceSchema, sourceTable, decision.Reason)
				global.Wlog.Warn(vlog)
			} else {
				c = append(c, fkName)
			}
		}
		advisorySuggestions = append(advisorySuggestions, schemacompat.BuildForeignKeyRepairSuggestions(destSchema, destTable, sourceCanonicalFKs, destCanonicalFKs, stcls.tableMappings)...)

		if strings.EqualFold(stcls.destDrive, "mysql") && stcls.destVersionInfo().Series == "8.4" {
			strictIssues, strictErr := detectStrictForeignKeyIssues(stcls.sourceDB, sourceCanonicalFKs)
			if strictErr != nil {
				vlog = fmt.Sprintf("(%d) Failed to validate strict foreign key requirements for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, strictErr)
				global.Wlog.Warn(vlog)
			} else if len(strictIssues) > 0 {
				pods.DIFFS = "yes"
				for _, issue := range strictIssues {
					d = append(d, issue.Name)
					vlog = fmt.Sprintf(
						"(%d) MySQL 8.4 strict foreign key precheck warning for table %s.%s: foreign key %s references %s.%s(%s) without an exact UNIQUE/PRIMARY KEY match",
						logThreadSeq,
						sourceSchema,
						sourceTable,
						issue.Name,
						issue.ReferencedSchema,
						issue.ReferencedTable,
						strings.Join(issue.ReferencedColumns, ", "),
					)
					global.Wlog.Warn(vlog)
				}
				advisorySuggestions = append(advisorySuggestions, schemacompat.BuildStrictForeignKeyRepairSuggestions(strictIssues, stcls.tableMappings)...)
			}
		}

		if len(advisorySuggestions) > 0 {
			advisoryLines := buildConstraintAdvisoryLines(fmt.Sprintf("%s.%s FOREIGN KEY constraints", destSchema, destTable), advisorySuggestions)
			if err := stcls.writeAdvisoryFixSql(advisoryLines, logThreadSeq); err != nil {
				global.Wlog.Error(fmt.Sprintf("(%d) Failed to write foreign key advisory SQL for %s.%s: %v", logThreadSeq, destSchema, destTable, err))
				return
			}
		}

		vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s Foreign. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, sourceSchema, sourceTable, c, len(c), d, len(d))
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) The source target segment table %s.%s Foreign data verification is completed", logThreadSeq, sourceSchema, sourceTable)
		global.Wlog.Debug(vlog)
		// 如果是从 Struct 函数调用的，则将结果存储在全局变量中
		if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
			// 使用完整的schema.table作为键
			tableKey := fmt.Sprintf("%s.%s", pods.Schema, pods.Table)

			// Keep foreign-key diff state on the schemaTable instance so each run
			// owns its own lifecycle.
			if stcls.foreignKeyDiffsMap == nil {
				stcls.foreignKeyDiffsMap = make(map[string]bool)
			}
			stcls.foreignKeyDiffsMap[tableKey] = pods.DIFFS == "yes"

			vlog = fmt.Sprintf("(%d) Storing foreign key check result for table %s: %v",
				logThreadSeq, tableKey, stcls.foreignKeyDiffsMap[tableKey])
			global.Wlog.Debug(vlog)
		} else {
			// 不是从 Struct 函数调用时，添加到 measuredDataPods
			stcls.appendPod(pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Foreign data. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

// 校验分区
func (stcls *schemaTable) Partitions(dtabS []string, logThreadSeq, logThreadSeq2 int64, isCalledFromStruct ...bool) {
	var (
		vlog                             string
		err                              error
		c, d                             []string
		sourcePartitions, destPartitions map[string]string
		pods                             = Pod{
			Datafix:     "no",
			CheckObject: "partitions",
		}
	)

	// 如果是从 Struct 函数调用的，则将 CheckObject 设置为 "struct"
	if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
		pods.CheckObject = "struct"
	}
	vlog = fmt.Sprintf("(%d) Start init check source and target DB partition table. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		sourceSchema, sourceTable, destSchema, destTable := parseSourceAndDestTablePair(i, stcls.tableMappings)
		stcls.schema = sourceSchema
		stcls.table = sourceTable
		stcls.destTable = destTable
		vlog = fmt.Sprintf("(%d) Start processing srcDSN {%s} table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.sourceDrive, sourceSchema, sourceTable)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: sourceSchema, Table: sourceTable, Drive: stcls.sourceDrive}
		if sourcePartitions, err = tc.Query().Partitions(stcls.sourceDB, logThreadSeq2); err != nil {
			global.Wlog.Errorf("(%d) Failed to get source partitions for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, err)
			return
		}

		vlog = fmt.Sprintf("(%d) srcDSN {%s} table %s.%s partitions count: %d", logThreadSeq, stcls.sourceDrive, sourceSchema, sourceTable, len(sourcePartitions))
		global.Wlog.Debug(vlog)

		tc.Drive = stcls.destDrive
		tc.Schema = destSchema
		tc.Table = destTable
		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.destDrive, destSchema, destTable)
		global.Wlog.Debug(vlog)
		if destPartitions, err = tc.Query().Partitions(stcls.destDB, logThreadSeq2); err != nil {
			global.Wlog.Errorf("(%d) Failed to get dest partitions for table %s.%s: %v", logThreadSeq, destSchema, destTable, err)
			return
		}
		vlog = fmt.Sprintf("(%d) Dest DB %s table %s.%s partitions count: %d", logThreadSeq, stcls.destDrive, destSchema, destTable, len(destPartitions))
		global.Wlog.Debug(vlog)

		pods.Schema = sourceSchema
		pods.Table = sourceTable
		if len(sourcePartitions) == 0 && len(destPartitions) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, sourceSchema, sourceTable)
			global.Wlog.Debug(vlog)
			continue
		}

		// Mapped-table verification needs source and target keys separately.
		sourceTableKey := fmt.Sprintf("%s.%s", sourceSchema, sourceTable)
		destTableKey := fmt.Sprintf("%s.%s", destSchema, destTable)

		// 1. 检查表级别的分区定义是否一致
		pods.DIFFS = "no"

		// 先比较完整的分区定义（包含分区类型、列和所有分区）
		sourceFullDef, sourceHasDef := sourcePartitions[sourceTableKey]
		destFullDef, destHasDef := destPartitions[destTableKey]

		// 记录具体的分区名称用于详细比较
		sourcePartitionNames := make([]string, 0)
		destPartitionNames := make([]string, 0)

		// 提取源端和目标端的分区名称
		for k := range sourcePartitions {
			if strings.HasPrefix(k, sourceTableKey+".") {
				// 提取分区名称部分 (schema.table.partition -> partition)
				parts := strings.Split(k, ".")
				if len(parts) == 3 {
					sourcePartitionNames = append(sourcePartitionNames, parts[2])
				}
			}
		}

		for k := range destPartitions {
			if strings.HasPrefix(k, destTableKey+".") {
				parts := strings.Split(k, ".")
				if len(parts) == 3 {
					destPartitionNames = append(destPartitionNames, parts[2])
				}
			}
		}

		vlog = fmt.Sprintf("(%d) Table %s.%s source partitions: %v, dest partitions: %v", logThreadSeq, sourceSchema, sourceTable, sourcePartitionNames, destPartitionNames)
		global.Wlog.Debug(vlog)

		sourceFullDefNormalized := normalizePartitionFullDefinition(sourceFullDef)
		destFullDefNormalized := normalizePartitionFullDefinition(destFullDef)

		// 直接比较完整的分区定义，但先做标识符和空白归一化，避免
		// `customer_id` 与 customer_id 这类纯文本噪音被误判成结构差异。
		if sourceFullDefNormalized != destFullDefNormalized {
			pods.DIFFS = "yes"
			vlog = fmt.Sprintf("(%d) Table %s.%s partition definitions mismatch", logThreadSeq, sourceSchema, sourceTable)
			global.Wlog.Warn(vlog)
			d = append(d, "Partition definitions mismatch")

			// Only handle low-risk tail partition drift automatically.
			execRepairSQLs, advisoryRepairSQLs, handled, reason := buildPartitionRepairSQLs(
				sourceSchema,
				sourceTable,
				destSchema,
				destTable,
				sourcePartitions,
				destPartitions,
			)
			if handled {
				pods.DIFFS = classifyPartitionRepairDiffState(execRepairSQLs, advisoryRepairSQLs, handled)
				if len(execRepairSQLs) > 0 {
					vlog = fmt.Sprintf("(%d) Generated executable partition repair SQLs for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, execRepairSQLs)
					global.Wlog.Warn(vlog)
					if err = stcls.writeFixSql(execRepairSQLs, logThreadSeq); err != nil {
						global.Wlog.Errorf("(%d) Failed to write executable partition repair SQLs for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, err)
						return
					}
				}
				if len(advisoryRepairSQLs) > 0 {
					vlog = fmt.Sprintf("(%d) Generated advisory partition repair SQLs for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, advisoryRepairSQLs)
					global.Wlog.Warn(vlog)
					if err = stcls.writeFixSql(advisoryRepairSQLs, logThreadSeq); err != nil {
						global.Wlog.Errorf("(%d) Failed to write advisory partition repair SQLs for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, err)
						return
					}
				}
				vlog = fmt.Sprintf("(%d) Partition mismatch for table %s.%s was classified as a supported repair shape: %s", logThreadSeq, sourceSchema, sourceTable, reason)
				global.Wlog.Warn(vlog)
			} else {
				// Fall back to a generic note when the partition mismatch cannot be repaired safely.
				cleanTable := sourceTable
				if strings.Contains(cleanTable, ":") {
					parts := strings.Split(cleanTable, ":")
					cleanTable = parts[0]
				}
				fixSQLHint := fmt.Sprintf("-- [Note] The partitions for table %s.%s is inconsistent, please check manually", sourceSchema, cleanTable)
				if err = stcls.writeFixSql([]string{fixSQLHint}, logThreadSeq); err != nil {
					global.Wlog.Errorf("(%d) Failed to write partition manual-check hint for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, err)
					return
				}
				vlog = fmt.Sprintf("(%d) Partition mismatch for table %s.%s remains manual-review only: %s", logThreadSeq, sourceSchema, sourceTable, reason)
				global.Wlog.Warn(vlog)
			}
		} else {
			// Partition definitions can differ textually across versions or SHOW CREATE
			// variants, so treat normalized-equal definitions as consistent.
			vlog = fmt.Sprintf("(%d) Table %s.%s partition definitions are consistent after normalization", logThreadSeq, sourceSchema, sourceTable)
			global.Wlog.Debug(vlog)
			c = append(c, "All partitions consistent")
			continue // 跳过后续的分区比较，因为定义已经完全一致
			// 这里不再单独比较每个分区，因为已经通过完整分区定义进行了比较
		}

		// 记录分区定义的比较结果
		if sourceHasDef && destHasDef {
			vlog = fmt.Sprintf("(%d) Table %s.%s full partition definitions compared: source='%s', dest='%s'", logThreadSeq, sourceSchema, sourceTable, sourceFullDef, destFullDef)
			global.Wlog.Debug(vlog)
		}

		vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s partitions. normal partitions: %v, abnormal partitions: %v", logThreadSeq, sourceSchema, sourceTable, c, d)
		global.Wlog.Debug(vlog)

		// 如果是从 Struct 函数调用的，则将结果存储在全局变量中
		if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
			// 使用完整的schema.table作为键

			// Keep partition diff state on the schemaTable instance so repeated
			// checks do not reuse package-level mutable state.
			if stcls.partitionDiffsMap == nil {
				stcls.partitionDiffsMap = make(map[string]bool)
			}
			if stcls.structWarnOnlyDiffsMap == nil {
				stcls.structWarnOnlyDiffsMap = make(map[string]bool)
			}
			if stcls.structCollationMappedMap == nil {
				stcls.structCollationMappedMap = make(map[string]bool)
			}

			// 确保使用干净的表名格式（不含映射后缀）
			cleanTableKey := sourceTableKey
			if strings.Contains(sourceTableKey, ":") {
				parts := strings.Split(sourceTableKey, ":")
				cleanTableKey = parts[0]
			}

			stcls.partitionDiffsMap[cleanTableKey] = pods.DIFFS == "yes"
			if pods.DIFFS == global.SkipDiffsWarnOnly {
				stcls.structWarnOnlyDiffsMap[cleanTableKey] = true
			}

			vlog = fmt.Sprintf("(%d) Storing partition check result for table %s (cleaned to %s): %v",
				logThreadSeq, sourceTableKey, cleanTableKey, stcls.partitionDiffsMap[cleanTableKey])
			global.Wlog.Debug(vlog)
		} else {
			// 不是从 Struct 函数调用时，添加到 measuredDataPods
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table partitions data. normal table count: [%d] abnormal table count: [%d]", logThreadSeq, len(c), len(d))
	global.Wlog.Info(vlog)
}

func (stcls *schemaTable) Index(dtabS []string, logThreadSeq, logThreadSeq2 int64, isCalledFromStruct ...bool) error {
	var (
		vlog  string
		sqlS  []string
		aa    = &CheckSumTypeStruct{}
		event string
		// 辅助函数：提取列名和序号
		extractColumnInfo = func(columnStr string) (string, int) {
			// 从格式 "columnName/*seq*/1/*type*/columnType" 中提取信息
			parts := strings.Split(columnStr, "/*seq*/")
			// 保留原始列名大小写
			colName := strings.TrimSpace(parts[0])
			seqStr := strings.Split(parts[1], "/*type*/")[0]
			seq, _ := strconv.Atoi(seqStr)

			return colName, seq
		}

		// 辅助函数：按序号排序列并返回纯列名（仅用于大小写比较等不需要前缀的场景）
		sortColumns = func(columns []string) []string {
			type ColumnInfo struct {
				name string
				seq  int
			}
			var columnInfos []ColumnInfo

			// 提取列信息
			for _, col := range columns {
				name, seq := extractColumnInfo(col)
				columnInfos = append(columnInfos, ColumnInfo{name: name, seq: seq})
			}

			// 按序号排序
			sort.Slice(columnInfos, func(i, j int) bool {
				return columnInfos[i].seq < columnInfos[j].seq
			})

			// 返回排序后的纯列名
			var result []string
			for _, col := range columnInfos {
				result = append(result, fmt.Sprintf("%s", col.name))
			}
			return result
		}

		// 辅助函数：按序号排序列，返回可直接用于 DDL 的带引号列表达式（含前缀长度）。
		// token 格式：colName/*seq*/N/*type*/T/*prefix*/P
		// 函数索引 token 格式：/*expr*/EXPRESSION/*seq*/N/*type*//*prefix*/0
		// 旧格式（无 /*prefix*/）兼容处理：prefix 视为 0。
		quoteColumnWithPrefix = func(token string) string {
			// 函数索引 token 以 /*expr*/ 开头，返回带括号的表达式（MySQL DDL 要求）
			if strings.HasPrefix(token, "/*expr*/") {
				rest := strings.TrimPrefix(token, "/*expr*/")
				var expr string
				if seqIdx := strings.Index(rest, "/*seq*/"); seqIdx >= 0 {
					expr = strings.TrimSpace(rest[:seqIdx])
				} else {
					expr = strings.TrimSpace(rest)
				}
				// MySQL 函数索引 DDL 必须用括号包裹表达式：ADD INDEX idx((expr))
				if !strings.HasPrefix(expr, "(") {
					expr = "(" + expr + ")"
				}
				return expr
			}
			colName := strings.TrimSpace(token)
			prefix := 0
			if seqParts := strings.Split(token, "/*seq*/"); len(seqParts) == 2 {
				colName = strings.TrimSpace(seqParts[0])
				if typeParts := strings.Split(seqParts[1], "/*type*/"); len(typeParts) == 2 {
					if prefixParts := strings.Split(typeParts[1], "/*prefix*/"); len(prefixParts) == 2 {
						if n, err := strconv.Atoi(strings.TrimSpace(prefixParts[1])); err == nil {
							prefix = n
						}
					}
				}
			}
			quoted := fmt.Sprintf("`%s`", strings.ReplaceAll(colName, "`", "``"))
			if prefix > 0 {
				return fmt.Sprintf("%s(%d)", quoted, prefix)
			}
			return quoted
		}

		sortColumnsPreservingPrefix = func(columns []string) []string {
			type ColumnInfo struct {
				expr string
				seq  int
			}
			var columnInfos []ColumnInfo
			for _, col := range columns {
				_, seq := extractColumnInfo(col)
				columnInfos = append(columnInfos, ColumnInfo{expr: quoteColumnWithPrefix(col), seq: seq})
			}
			sort.Slice(columnInfos, func(i, j int) bool {
				return columnInfos[i].seq < columnInfos[j].seq
			})
			var result []string
			for _, col := range columnInfos {
				result = append(result, col.expr)
			}
			return result
		}

		constraintNameKey = func(name string) string {
			if stcls.caseSensitiveObjectName == "no" {
				return strings.ToUpper(name)
			}
			return name
		}

		indexGenerate = func(smu, dmu map[string][]string, a *CheckSumTypeStruct, indexType string, sourceVisibilityMap, destVisibilityMap map[string]string) []string {
			var cc, c, d []string

			// 根据映射规则确定目标端schema
			destSchema := stcls.schema
			if mappedSchema, exists := stcls.tableMappings[stcls.schema]; exists {
				destSchema = mappedSchema
			}

			dbf := dbExec.DataAbnormalFixStruct{
				Schema:                  destSchema, // 使用目标端schema
				Table:                   stcls.table,
				SourceDevice:            stcls.sourceDrive,
				DestDevice:              stcls.destDrive,
				IndexType:               indexType,
				DatafixType:             stcls.datafix,
				SourceSchema:            stcls.schema,                  // 添加源端schema
				CaseSensitiveObjectName: stcls.caseSensitiveObjectName, // 传递是否区分对象名大小写
				IndexVisibilityMap:      sourceVisibilityMap,           // 传递索引可见性信息
				DestFlavor:              stcls.destVersionInfo().Flavor, // 用于生成兼容目标端语法的 fix SQL
			}

			sourceCanonicalIndexes := schemacompat.CanonicalizeMySQLIndexes(smu, sourceVisibilityMap)
			destCanonicalIndexes := schemacompat.CanonicalizeMySQLIndexes(dmu, destVisibilityMap)
			sourceCanonicalByName := make(map[string]schemacompat.CanonicalIndex)
			destCanonicalByName := make(map[string]schemacompat.CanonicalIndex)
			sourceCanonicalConstraints := make(map[string]schemacompat.CanonicalConstraint)
			destCanonicalConstraints := make(map[string]schemacompat.CanonicalConstraint)
			for _, idx := range sourceCanonicalIndexes {
				sourceCanonicalByName[constraintNameKey(idx.Name)] = idx
			}
			for _, idx := range destCanonicalIndexes {
				destCanonicalByName[constraintNameKey(idx.Name)] = idx
			}
			switch indexType {
			case "pri":
				for _, constraint := range schemacompat.CanonicalizePrimaryKeyConstraints(sourceCanonicalIndexes) {
					sourceCanonicalConstraints[constraintNameKey(constraint.Name)] = constraint
				}
				for _, constraint := range schemacompat.CanonicalizePrimaryKeyConstraints(destCanonicalIndexes) {
					destCanonicalConstraints[constraintNameKey(constraint.Name)] = constraint
				}
			case "uni":
				for _, constraint := range schemacompat.CanonicalizeUniqueConstraints(sourceCanonicalIndexes) {
					sourceCanonicalConstraints[constraintNameKey(constraint.Name)] = constraint
				}
				for _, constraint := range schemacompat.CanonicalizeUniqueConstraints(destCanonicalIndexes) {
					destCanonicalConstraints[constraintNameKey(constraint.Name)] = constraint
				}
			}

			// 首先比较索引名称
			for k := range smu {
				c = append(c, k)
			}
			for k := range dmu {
				d = append(d, k)
			}
			sort.Strings(c)
			sort.Strings(d)

			// 如果索引名称不同，生成修复SQL
			if a.CheckMd5(strings.Join(c, ",")) != a.CheckMd5(strings.Join(d, ",")) {
				e, f := a.Arrcmp(c, d)
				// 对于新增的索引，需要处理列顺序
				newIndexMap := make(map[string][]string)
				for _, idx := range e {
					if cols, ok := smu[idx]; ok {
						// 传入原始 token（含前缀信息），由 FixAlterIndexSqlExec 内部解析
						newIndexMap[idx] = cols
					}
				}
				// 获取数据修复实例
				fixInstance := dbf.DataAbnormalFix()

				// 对于MySQL数据库，尝试加载外键定义
				if stcls.sourceDrive == "mysql" {
					// 将接口转换为MySQL具体类型
					if mysqlFix, ok := fixInstance.(*mysql.MysqlDataAbnormalFixStruct); ok {
						// 使用源端数据库连接加载外键定义
						err := mysqlFix.LoadForeignKeyDefinitions(stcls.sourceDB, logThreadSeq)
						if err != nil {
							vlog := fmt.Sprintf("(%d) Failed to load foreign key definitions for table %s.%s: %v",
								logThreadSeq, stcls.schema, stcls.table, err)
							global.Wlog.Warn(vlog)
						} else {
							vlog := fmt.Sprintf("(%d) Successfully loaded %d foreign key definitions for table %s.%s",
								logThreadSeq, len(mysqlFix.ForeignKeyDefinitions), stcls.schema, stcls.table)
							global.Wlog.Debug(vlog)
						}
					}
				}

				// 执行索引修复SQL生成
				cc = append(cc, fixInstance.FixAlterIndexSqlExec(e, f, newIndexMap, stcls.sourceDrive, logThreadSeq)...)
			}
			// 无论索引名称集合是否一致，都要对两端均存在的同名索引比较具体内容
			// （名称集合不同时，同名但内容不同的索引会被上方分支跳过，需在此补充检查）
			for k, sColumns := range smu {
					if dColumns, exists := dmu[k]; exists {
						semanticMismatch := false
						canonicalKey := constraintNameKey(k)
						indexSemanticMatch := false
						constraintSemanticMatch := indexType == "mul"
						if sourceIdx, ok := sourceCanonicalByName[canonicalKey]; ok {
							if destIdx, ok := destCanonicalByName[canonicalKey]; ok {
								indexDecision := schemacompat.DecideIndexCompatibility(sourceIdx, destIdx)
								if indexDecision.IsMismatch() {
									semanticMismatch = true
									vlog = fmt.Sprintf("(%d) %s Index %s semantic mismatch: reason=%s", logThreadSeq, event, k, indexDecision.Reason)
									global.Wlog.Warn(vlog)
								} else {
									indexSemanticMatch = true
								}
							}
						}
						if indexType == "pri" || indexType == "uni" {
							if sourceConstraint, ok := sourceCanonicalConstraints[canonicalKey]; ok {
								if destConstraint, ok := destCanonicalConstraints[canonicalKey]; ok {
									constraintDecision := schemacompat.DecideKeyConstraintCompatibility(sourceConstraint, destConstraint)
									if constraintDecision.IsMismatch() {
										semanticMismatch = true
										vlog = fmt.Sprintf("(%d) %s %s constraint %s semantic mismatch: reason=%s", logThreadSeq, event, strings.ToUpper(indexType), k, constraintDecision.Reason)
										global.Wlog.Warn(vlog)
									} else {
										constraintSemanticMatch = true
									}
								}
							}
						}
						if indexSemanticMatch && constraintSemanticMatch && !semanticMismatch {
							continue
						}

						sSortedColumns := sortColumns(sColumns)
						dSortedColumns := sortColumns(dColumns)
						if !semanticMismatch && indexColumnsOnlyDifferInCase(sSortedColumns, dSortedColumns) {
							continue
						}

						// 比较同名索引的列及其顺序（包含序号信息的比较）
						if semanticMismatch || a.CheckMd5(strings.Join(sColumns, ",")) != a.CheckMd5(strings.Join(dColumns, ",")) {
							// 检查是否仅仅是列名大小写不同（当caseSensitiveObjectName=yes时）
							columnsOnlyCaseDifferent := false
							if stcls.caseSensitiveObjectName == "yes" && len(sColumns) == len(dColumns) {
								columnsOnlyCaseDifferent = true
								lowerSourceColumns := make(map[string]bool)
								for _, col := range sColumns {
									lowerSourceColumns[strings.ToLower(col)] = true
								}
								for _, col := range dColumns {
									if !lowerSourceColumns[strings.ToLower(col)] {
										columnsOnlyCaseDifferent = false
										break
									}
								}
							}

							// 如果只是列名大小写不同且是主键，跳过重建主键
							if columnsOnlyCaseDifferent && indexType == "pri" && !semanticMismatch {
								continue
							}

							// 1. 先生成删除旧索引的SQL
							// 根据映射规则确定目标端schema
							destSchema := stcls.schema
							if mappedSchema, exists := stcls.tableMappings[stcls.schema]; exists {
								destSchema = mappedSchema
							}

							// 2. 纯列名（用于自增主键检测等需要原始列名的场景）
							plainColumns := sSortedColumns

							// 检查是否是主键且该列是自增列
							isAutoIncrementPrimaryKey := false
							if indexType == "pri" && len(plainColumns) == 1 {
								// 构建键名：schema.table.column
								key := fmt.Sprintf("%s.%s.%s", destSchema, stcls.table, plainColumns[0])
								// 检查该列是否已经在添加列时设置了主键
								if mysql.AutoIncrementColumnsWithPrimaryKey != nil && mysql.AutoIncrementColumnsWithPrimaryKey[key] {
									isAutoIncrementPrimaryKey = true
									vlog = fmt.Sprintf("(%d) %s Column %s is already set as PRIMARY KEY in ALTER TABLE ADD COLUMN statement, skipping index repair",
										logThreadSeq, event, plainColumns[0])
									global.Wlog.Debug(vlog)
								}
							}

							// 3. 生成创建索引的SQL
							// 根据映射规则确定目标端schema
							destSchema = stcls.schema
							if mappedSchema, exists := stcls.tableMappings[stcls.schema]; exists {
								destSchema = mappedSchema
							}

							// 带引号且保留前缀长度的列 DDL 表达式，例如 `goods_name`(20)
							quotedColumns := sortColumnsPreservingPrefix(sColumns)

							// 获取索引可见性信息
							// MariaDB 使用 IGNORED 关键字，MySQL 使用 INVISIBLE。
							indexHiddenKeyword := "INVISIBLE"
							if stcls.destVersionInfo().Flavor == global.DatabaseFlavorMariaDB {
								indexHiddenKeyword = "IGNORED"
							}
							visibility := ""
							if (indexType == "mul" || indexType == "uni") && sourceVisibilityMap != nil {
								if vis, ok := sourceVisibilityMap[k]; ok && isInvisibleLikeIndexVisibility(vis) {
									visibility = " " + indexHiddenKeyword
								}
							}

							// 只有当不是自增列主键时才生成创建索引的SQL
							if !isAutoIncrementPrimaryKey {
								// 1. 先删除目标端已存在的同名索引（先删后建，避免重复索引报错）
								if indexType == "pri" {
									cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PRIMARY KEY;",
										destSchema, stcls.table))
								} else {
									cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP INDEX `%s`;",
										destSchema, stcls.table, k))
								}
								// 2. 再新建符合源端定义的索引
								if indexType == "pri" {
									cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PRIMARY KEY(%s);",
										destSchema, stcls.table, strings.Join(quotedColumns, ", ")))
								} else if indexType == "uni" {
									cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD UNIQUE INDEX `%s`(%s)%s;",
										destSchema, stcls.table, k, strings.Join(quotedColumns, ", "), visibility))
								} else {
									cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD INDEX `%s`(%s)%s;",
										destSchema, stcls.table, k, strings.Join(quotedColumns, ", "), visibility))
								}
							}
						}
					}
				}
			return cc
		}
	)

	fmt.Println("gt-checksum: Starting index checks")
	event = fmt.Sprintf("[%s]", "check_table_index")
	//校验索引
	vlog = fmt.Sprintf("(%d) %s start init check source and target DB index Column. to check it...", logThreadSeq, event)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		sourceSchema, tableName, destSchema, destTable := parseSourceAndDestTablePair(i, stcls.tableMappings)
		// 在正确的作用域内声明索引相关变量
		var spri, suni, smul, dpri, duni, dmul map[string][]string
		var sourceIndexVisibilityMap, destIndexVisibilityMap map[string]string

		stcls.table = tableName
		stcls.schema = sourceSchema
		stcls.destTable = destTable

		// 检查表是否在skipIndexCheckTables列表中，如果是，则跳过
		tableKey := fmt.Sprintf("%s.%s", destSchema, destTable)
		isDropped := false
		for _, droppedTable := range stcls.skipIndexCheckTables {
			if strings.EqualFold(droppedTable, tableKey) {
				vlog = fmt.Sprintf("(%d) %s Skipping index check for table %s as it is marked for deletion", logThreadSeq, event, tableKey)
				global.Wlog.Info(vlog)
				isDropped = true
				break
			}
		}
		if isDropped {
			continue
		}

		idxc := dbExec.IndexColumnStruct{Schema: sourceSchema, Table: stcls.table, Drivce: stcls.sourceDrive, CaseSensitiveObjectName: stcls.caseSensitiveObjectName}
		vlog = fmt.Sprintf("(%d) %s Start processing srcDSN {%s} table %s.%s index column data. to dispos it...", logThreadSeq, event, stcls.sourceDrive, sourceSchema, stcls.table)
		global.Wlog.Debug(vlog)
		squeryData, err := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Querying the index column data of srcDSN {%s} database table %s failed, and the error message is {%v}", logThreadSeq, event, stcls.sourceDrive, i, err)
			global.Wlog.Error(vlog)
			return err
		}
		spri, suni, smul, sourceIndexVisibilityMap = idxc.TableIndexColumn().IndexDisposF(squeryData, logThreadSeq2)
		if sourceCreateSQL, err := queryMySQLCreateTableStatement(stcls.sourceDB, sourceSchema, stcls.table); err == nil {
			sourceIndexVisibilityMap = mergeIndexVisibilityHints(sourceIndexVisibilityMap, schemacompat.ExtractIndexVisibilityHintsFromCreateSQL(sourceCreateSQL))
		}
		vlog = fmt.Sprintf("(%d) %s The index column data of the source %s database table %s.%s is {primary:%v,unique key:%v,index key:%v}",
			logThreadSeq,
			event,
			stcls.sourceDrive,
			sourceSchema,
			stcls.table,
			spri,
			suni,
			smul)
		global.Wlog.Debug(vlog)

		idxc.Schema = destSchema
		idxc.Table = destTable
		idxc.Drivce = stcls.destDrive
		vlog = fmt.Sprintf("(%d) %s Start processing dstDSN {%s} table %s.%s index column data. to dispos it...", logThreadSeq, event, stcls.destDrive, destSchema, destTable)
		global.Wlog.Debug(vlog)
		dqueryData, err := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.destDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Querying the index column data of dstDSN {%s} database table %s failed, and the error message is {%v}", logThreadSeq, event, stcls.destDrive, i, err)
			global.Wlog.Error(vlog)
			return err
		}
		dpri, duni, dmul, destIndexVisibilityMap = idxc.TableIndexColumn().IndexDisposF(dqueryData, logThreadSeq2)
		if destCreateSQL, err := queryMySQLCreateTableStatement(stcls.destDB, destSchema, stcls.destTable); err == nil {
			destIndexVisibilityMap = mergeIndexVisibilityHints(destIndexVisibilityMap, schemacompat.ExtractIndexVisibilityHintsFromCreateSQL(destCreateSQL))
		}
		vlog = fmt.Sprintf("(%d) %s The index column data of the dest %s database table %s.%s is {primary:%v,unique key:%v,index key:%v}",
			logThreadSeq,
			event,
			stcls.destDrive,
			destSchema,
			destTable,
			dpri,
			duni,
			dmul)
		global.Wlog.Debug(vlog)

		var pods = Pod{
			Datafix:     stcls.datafix,
			CheckObject: "index",
			DIFFS:       "no",
			Schema:      stcls.schema,
			Table:       stcls.table,
		}

		// 如果是从 Struct 函数调用的，则将 CheckObject 设置为 "struct"
		if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
			pods.CheckObject = "struct"
		}
		//先比较主键索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the primary key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(spri, dpri, aa, "pri", sourceIndexVisibilityMap, destIndexVisibilityMap)...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the primary key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		//再比较唯一索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the unique key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(suni, duni, aa, "uni", sourceIndexVisibilityMap, destIndexVisibilityMap)...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the unique key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Info(vlog)
		//后比较普通索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the no-unique key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(smul, dmul, aa, "mul", sourceIndexVisibilityMap, destIndexVisibilityMap)...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the no-unique key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		// 应用并清空 sqlS
		columnRepairKey := fmt.Sprintf("%s.%s", stcls.schema, stcls.table)
		pendingColumnOperations := stcls.pendingColumnRepairOperations(columnRepairKey)
		if len(sqlS) > 0 {
			pods.DIFFS = "yes"

			// 检查是否有列修复操作需要合并
			if len(pendingColumnOperations) > 0 {
				// 创建DataAbnormalFixStruct用于合并操作
				destSchema := stcls.schema
				if mappedSchema, exists := stcls.tableMappings[stcls.schema]; exists {
					destSchema = mappedSchema
				}

				dbf := dbExec.DataAbnormalFixStruct{
					Schema:                  destSchema,
					Table:                   stcls.table,
					SourceDevice:            stcls.sourceDrive,
					DestDevice:              stcls.destDrive,
					DatafixType:             stcls.datafix,
					CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
					SourceSchema:            stcls.schema,
					DestFlavor:              stcls.destVersionInfo().Flavor,
				}

				// 合并列修复和索引修复操作
				combinedSql := dbf.DataAbnormalFix().FixAlterColumnAndIndexSqlGenerate(pendingColumnOperations, sqlS, logThreadSeq)

				// 使用合并后的SQL
				sqlS = combinedSql

				// Column repair operations have been merged into the final
				// ALTER TABLE statement and can now be discarded.
				stcls.forgetColumnRepairOperations(columnRepairKey)

				vlog = fmt.Sprintf("(%d) %s Merged column and index operations for table %s.%s",
					logThreadSeq, event, stcls.schema, stcls.table)
				global.Wlog.Debug(vlog)
			} else {
				// 只有索引操作，合并索引操作
				destSchema := stcls.schema
				if mappedSchema, exists := stcls.tableMappings[stcls.schema]; exists {
					destSchema = mappedSchema
				}

				dbf := dbExec.DataAbnormalFixStruct{
					Schema:                  destSchema,
					Table:                   stcls.table,
					SourceDevice:            stcls.sourceDrive,
					DestDevice:              stcls.destDrive,
					DatafixType:             stcls.datafix,
					SourceSchema:            stcls.schema,
					CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
					DestFlavor:              stcls.destVersionInfo().Flavor,
				}

				combinedSql := dbf.DataAbnormalFix().FixAlterIndexSqlGenerate(sqlS, logThreadSeq)
				sqlS = combinedSql
			}

			if err := stcls.writeFixSql(sqlS, logThreadSeq); err != nil {
				return err
			}
			sqlS = []string{} // 清空 sqlS 以便下一个表使用

			// 添加调试日志，记录索引不一致的表
			vlog = fmt.Sprintf("(%d) %s Table %s.%s has index differences, setting DIFFS to yes",
				logThreadSeq, event, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
		} else if len(pendingColumnOperations) > 0 {
			// Tables with column-level fixes but no index diffs still need their
			// deferred ALTER TABLE written once the index phase confirms no merge
			// is needed.
			if err := stcls.writeFixSql(pendingColumnOperations, logThreadSeq); err != nil {
				return err
			}
			stcls.forgetColumnRepairOperations(columnRepairKey)
			vlog = fmt.Sprintf("(%d) %s Flushed deferred column/table repair statements for table %s.%s",
				logThreadSeq, event, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
		}

		// 如果是从 Struct 函数调用的，则将结果存储在临时变量中，以便 Struct 函数可以使用
		if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
			// 使用完整的schema.table作为键
			tableKey := fmt.Sprintf("%s.%s", stcls.schema, stcls.table)

			// Keep index diff state on the schemaTable instance so concurrent
			// schemaTable values do not share mutable global maps.
			if stcls.indexDiffsMap == nil {
				stcls.indexDiffsMap = make(map[string]bool)
			}
			stcls.indexDiffsMap[tableKey] = pods.DIFFS == "yes"

			vlog = fmt.Sprintf("(%d) %s Storing index check result for table %s.%s: %v",
				logThreadSeq, event, stcls.schema, stcls.table, stcls.indexDiffsMap[tableKey])
			global.Wlog.Debug(vlog)
		} else {
			// 不是从 Struct 函数调用时，添加到 measuredDataPods
			measuredDataPods = append(measuredDataPods, pods)
		}
		vlog = fmt.Sprintf("(%d) %s The source target segment table %s.%s index column data verification is completed", logThreadSeq, event, stcls.schema, stcls.table)
		global.Wlog.Info(vlog)
	}
	if len(stcls.columnRepairMap) > 0 {
		// A final sweep prevents deferred column SQL from being dropped if a
		// table never reached the merge branch above.
		for tableKey, pendingSQL := range stcls.columnRepairMap {
			if len(pendingSQL) == 0 {
				continue
			}
			parts := strings.SplitN(tableKey, ".", 2)
			if len(parts) == 2 {
				stcls.schema = parts[0]
				stcls.table = parts[1]
				stcls.destTable = parts[1]
			}
			if err := stcls.writeFixSql(pendingSQL, logThreadSeq); err != nil {
				return err
			}
			vlog = fmt.Sprintf("(%d) %s Flushed remaining deferred repair statements for table %s",
				logThreadSeq, event, tableKey)
			global.Wlog.Debug(vlog)
		}
		stcls.columnRepairMap = make(map[string][]string)
	}
	fmt.Println("gt-checksum: Index verification completed")
	return nil
}

/*
校验表结构是否正确
当设置checkObject=struct时，同时执行表结构、索引、分区和外键的校验
*/
func (stcls *schemaTable) Struct(dtabS []string, logThreadSeq, logThreadSeq2 int64) error {
	//校验列名
	var (
		vlog  string
		event string
		// 用于记录每个表的索引、分区和外键是否一致的映射
		tableStructDiffs = make(map[string]bool)
		// 用于跟踪已经添加过Pod记录的表，避免重复添加
		existingTableKeys = make(map[string]bool)
	)
	event = fmt.Sprintf("[check_table_columns]")
	stcls.structWarnOnlyDiffsMap = make(map[string]bool)
	stcls.structCollationMappedMap = make(map[string]bool)

	// Split dtabS into BASE TABLE entries and VIEW entries.
	// dtabS is reassigned here so all downstream code (Index/Partitions/Foreign)
	// automatically operates only on real tables.
	var viewEntries []string
	dtabS, viewEntries = splitTableViewEntries(dtabS, stcls.objectKinds, stcls.caseSensitiveObjectName)

	fmt.Println("gt-checksum: Checking table structure")
	vlog = fmt.Sprintf("(%d) %s checking table structure of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)
	// Snapshot measuredDataPods length before TableColumnNameCheck so we can
	// identify pods it appends directly (missing-table entries).  Those tables
	// must not receive a second Pod from the loop below.
	podCountBeforeCheck := len(measuredDataPods)
	normal, abnormal, err := stcls.TableColumnNameCheck(dtabS, logThreadSeq, logThreadSeq2)
	if err != nil {
		return err
	}
	vlog = fmt.Sprintf("(%d) %s Table structure and column checksum of srcDB and dstDB completed. The consistent result is {%s}(num [%d]), and the inconsistent result is {%s}(num [%d])", logThreadSeq, event, normal, len(normal), abnormal, len(abnormal))
	global.Wlog.Debug(vlog)
	// Pre-populate existingTableKeys from pods that TableColumnNameCheck already
	// appended (e.g. both-missing or source-missing tables).  This prevents the
	// append(normal, abnormal...) loop below from creating duplicate Pod entries
	// while still allowing abnormalTableList to carry its data-mode preflight count.
	// NOTE: This guard is effective only when stcls.aggregate == false (the current
	// production path for Struct()).  When aggregate is true, appendPod writes to
	// stcls.podsBuffer instead of measuredDataPods, so the slice delta is empty and
	// the guard has no effect.  That path does not currently call Struct(), so the
	// limitation is latent rather than actively triggered.
	for _, p := range measuredDataPods[podCountBeforeCheck:] {
		existingTableKeys[fmt.Sprintf("%s.%s", p.Schema, p.Table)] = true
	}

	// 初始化表结构差异映射
	for _, i := range dtabS {
		var sourceSchema, tableName string

		// 处理映射格式 schema.table:schema.table
		if strings.Contains(i, ":") {
			parts := strings.Split(i, ":")
			if len(parts) == 2 {
				sourceParts := strings.Split(parts[0], ".")
				if len(sourceParts) == 2 {
					sourceSchema = sourceParts[0]
					tableName = sourceParts[1]
				}
			}
		} else {
			// 处理普通格式 schema.table
			parts := strings.Split(i, ".")
			if len(parts) == 2 {
				sourceSchema = parts[0]
				tableName = parts[1]
			}
		}

		// 将表结构差异初始化为false（表示一致）
		tableKey := fmt.Sprintf("%s.%s", sourceSchema, tableName)
		tableStructDiffs[tableKey] = false

		// 如果表在abnormal列表中，则标记为不一致
		for _, abnormalTable := range abnormal {
			// 确保完全匹配表名，包括schema
			if abnormalTable == fmt.Sprintf("%s.%s", sourceSchema, tableName) {
				tableStructDiffs[tableKey] = true
				break
			}
		}
	}

	// 处理正常表和异常表，创建Pod实例
	for _, i := range append(normal, abnormal...) {
		aa := strings.Split(i, ".")
		destSchema := aa[0]
		tableName := aa[1]

		// 查找源端schema
		sourceSchema := destSchema
		for src, dst := range stcls.tableMappings {
			if dst == destSchema {
				sourceSchema = src
				break
			}
		}

		// 构建表的唯一键
		tableKey := fmt.Sprintf("%s.%s", sourceSchema, tableName)

		// 检查该表是否已在skipIndexCheckTables中（表示已被特殊处理过）
		isProcessed := false
		destTableKey := fmt.Sprintf("%s.%s", destSchema, tableName)
		for _, skipTable := range stcls.skipIndexCheckTables {
			if skipTable == destTableKey {
				isProcessed = true
				break
			}
		}

		// 如果表已经被处理过，或者已经添加过Pod记录，则跳过
		if !isProcessed && !existingTableKeys[tableKey] {
			// 为每个表创建新的Pod实例
			pods := Pod{
				Datafix:     stcls.datafix,
				CheckObject: "struct",
				Schema:      sourceSchema,
				Table:       tableName,
				DIFFS:       global.SkipDiffsNo,
			}

			// 如果表在abnormal列表中，则标记为不一致
			for _, abnormalTable := range abnormal {
				if abnormalTable == i {
					pods.DIFFS = global.SkipDiffsYes
					break
				}
			}
			if pods.DIFFS == global.SkipDiffsNo && stcls.structWarnOnlyDiffsMap[tableKey] {
				pods.DIFFS = global.SkipDiffsWarnOnly
			}
			if pods.DIFFS == global.SkipDiffsNo && stcls.structCollationMappedMap[tableKey] {
				pods.DIFFS = global.SkipDiffsCollationMapped
			}

			// 设置映射信息
			if sourceSchema != destSchema {
				// 记录映射关系到全局变量
				mappingRelation := fmt.Sprintf("%s.%s:%s.%s", sourceSchema, tableName, destSchema, tableName)
				exists := false
				for _, existingMapping := range TableMappingRelations {
					if existingMapping == mappingRelation {
						exists = true
						break
					}
				}
				if !exists {
					TableMappingRelations = append(TableMappingRelations, mappingRelation)
				}

				// 设置映射信息
				pods.MappingInfo = fmt.Sprintf("Schema: %s:%s", sourceSchema, destSchema)
			}

			measuredDataPods = append(measuredDataPods, pods)
			// 标记该表已添加Pod记录
			existingTableKeys[tableKey] = true
		}
	}

	// 创建一个自定义的结构体，用于在Index、Partitions和Foreign函数中捕获不一致的表
	type structDiffCollector struct {
		diffs map[string]bool
	}

	collector := &structDiffCollector{
		diffs: tableStructDiffs,
	}

	// 2. 执行索引校验 (原来的 Index 函数)
	fmt.Println("gt-checksum: Checking table indexes")
	vlog = fmt.Sprintf("(%d) %s checking table indexes of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)

	// 初始化索引差异映射
	stcls.indexDiffsMap = make(map[string]bool)

	// 调用Index函数进行索引校验
	fmt.Println("gt-checksum: Checking table indexes")
	vlog = fmt.Sprintf("(%d) %s checking table indexes of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)

	// 调用原始的Index函数
	if err := stcls.Index(dtabS, logThreadSeq, logThreadSeq2, true); err != nil {
		return err
	}

	// 使用indexDiffsMap更新collector.diffs
	for tableKey, hasDiff := range stcls.indexDiffsMap {
		if hasDiff {
			// 只更新存在于映射中的表
			if _, exists := collector.diffs[tableKey]; exists {
				collector.diffs[tableKey] = true
				vlog = fmt.Sprintf("(%d) Index check found differences for table %s",
					logThreadSeq, tableKey)
				global.Wlog.Debug(vlog)
			}
		}
	}

	// 3. 执行分区校验 (原来的 Partitions 函数)
	fmt.Println("gt-checksum: Checking table partitions")
	vlog = fmt.Sprintf("(%d) %s checking table partitions of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)

	// 3. 执行分区校验 (原来的 Partitions 函数)
	fmt.Println("gt-checksum: Checking table partitions")
	vlog = fmt.Sprintf("(%d) %s checking table partitions of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)

	// 初始化全局分区差异映射
	stcls.partitionDiffsMap = make(map[string]bool)
	vlog = fmt.Sprintf("(%d) %s Starting partitions check for %d tables, will query INFORMATION_SCHEMA.PARTITIONS for each table", logThreadSeq, event, len(dtabS))
	global.Wlog.Debug(vlog)

	// 调用Partitions函数进行分区检查，会查询INFORMATION_SCHEMA.PARTITIONS表
	stcls.Partitions(dtabS, logThreadSeq, logThreadSeq2, true)
	vlog = fmt.Sprintf("(%d) %s Completed partitions check, results: %v", logThreadSeq, event, stcls.partitionDiffsMap)
	global.Wlog.Debug(vlog)

	// 使用全局partitionDiffsMap更新collector.diffs
	vlog = fmt.Sprintf("(%d) Processing partition diffs map with %d entries: %v", logThreadSeq, len(stcls.partitionDiffsMap), stcls.partitionDiffsMap)
	global.Wlog.Debug(vlog)
	for tableKey, hasDiff := range stcls.partitionDiffsMap {
		vlog = fmt.Sprintf("(%d) Checking partition diff for table %s: %v", logThreadSeq, tableKey, hasDiff)
		global.Wlog.Debug(vlog)
		if hasDiff {
			// 尝试直接使用tableKey更新
			if _, exists := collector.diffs[tableKey]; exists {
				collector.diffs[tableKey] = true
				vlog = fmt.Sprintf("(%d) Partitions check found differences for table %s, updated diffs map",
					logThreadSeq, tableKey)
				global.Wlog.Debug(vlog)
			} else {
				// 如果直接匹配失败，尝试清理表名格式后匹配（移除可能的后缀）
				cleanTableKey := tableKey
				if strings.Contains(tableKey, ":") {
					parts := strings.Split(tableKey, ":")
					cleanTableKey = parts[0]
				}
				if _, exists := collector.diffs[cleanTableKey]; exists {
					collector.diffs[cleanTableKey] = true
					vlog = fmt.Sprintf("(%d) Partitions check found differences for table %s (cleaned to %s), updated diffs map",
						logThreadSeq, tableKey, cleanTableKey)
					global.Wlog.Debug(vlog)
				} else {
					vlog = fmt.Sprintf("(%d) Partitions diff found for table %s, but no matching entry in diffs map",
						logThreadSeq, tableKey)
					global.Wlog.Debug(vlog)
				}
			}
		}
	}

	// 4. 执行外键校验 (原来的 Foreign 函数)
	fmt.Println("gt-checksum: Checking table foreign keys")
	vlog = fmt.Sprintf("(%d) %s checking table foreign keys of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)

	// 初始化全局外键差异映射
	stcls.foreignKeyDiffsMap = make(map[string]bool)

	// 修改Foreign函数，使其能够存储检查结果
	stcls.Foreign(dtabS, logThreadSeq, logThreadSeq2, true)

	// 使用全局foreignKeyDiffsMap更新collector.diffs
	for tableKey, hasDiff := range stcls.foreignKeyDiffsMap {
		if hasDiff {
			// 只更新存在于映射中的表
			if _, exists := collector.diffs[tableKey]; exists {
				collector.diffs[tableKey] = true
				vlog = fmt.Sprintf("(%d) Foreign key check found differences for table %s",
					logThreadSeq, tableKey)
				global.Wlog.Debug(vlog)
			}
		}
	}

	// 添加调试日志，输出所有表的结构差异状态
	vlog = fmt.Sprintf("(%d) Table structure differences map: %v", logThreadSeq, collector.diffs)
	global.Wlog.Debug(vlog)

	// 更新struct记录的DIFFS状态
	for i, pod := range measuredDataPods {
		if pod.CheckObject == "struct" {
			// 使用完整的schema.table作为键
			tableKey := fmt.Sprintf("%s.%s", pod.Schema, pod.Table)

			// 检查这个特定的表是否在差异映射中
			isDifferent, exists := collector.diffs[tableKey]
			hasWarnOnly := stcls.structWarnOnlyDiffsMap[tableKey]
			hasCollationMapped := stcls.structCollationMappedMap[tableKey]

			vlog = fmt.Sprintf("(%d) Checking table %s.%s, current DIFFS=%s, in diff map: %v, exists: %v, warnOnly: %v, collationMapped: %v",
				logThreadSeq, pod.Schema, pod.Table, pod.DIFFS, isDifferent, exists, hasWarnOnly, hasCollationMapped)
			global.Wlog.Debug(vlog)

			// 先应用硬差异，再应用纯风险告警，最后应用 collation-mapped
			if exists && isDifferent {
				measuredDataPods[i].DIFFS = mergeStructDiffState(measuredDataPods[i].DIFFS, global.SkipDiffsYes)
				vlog = fmt.Sprintf("(%d) Table %s.%s has structure differences, setting DIFFS to yes",
					logThreadSeq, pod.Schema, pod.Table)
				global.Wlog.Debug(vlog)
			} else if hasWarnOnly {
				measuredDataPods[i].DIFFS = mergeStructDiffState(measuredDataPods[i].DIFFS, global.SkipDiffsWarnOnly)
				vlog = fmt.Sprintf("(%d) Table %s.%s only has warn-only structure risks, setting DIFFS to warn-only",
					logThreadSeq, pod.Schema, pod.Table)
				global.Wlog.Debug(vlog)
			} else if hasCollationMapped {
				measuredDataPods[i].DIFFS = mergeStructDiffState(measuredDataPods[i].DIFFS, global.SkipDiffsCollationMapped)
				vlog = fmt.Sprintf("(%d) Table %s.%s has cross-platform collation mapping only, setting DIFFS to collation-mapped",
					logThreadSeq, pod.Schema, pod.Table)
				global.Wlog.Debug(vlog)
			}
		}
	}

	fmt.Println("gt-checksum: Table structure verification completed")
	vlog = fmt.Sprintf("(%d) %s check source and target DB table struct complete", logThreadSeq, event)
	global.Wlog.Info(vlog)

	// 5. Process any VIEW entries that were split off at the top.
	if err := stcls.checkViewStruct(viewEntries, logThreadSeq, logThreadSeq2); err != nil {
		return err
	}

	return nil
}

/*
用于测试db链接串是否正确，是否可以连接
*/
func dbOpenTest(drive, jdbc string) *sql.DB {
	p := dbExec.DBexec()
	p.JDBC = jdbc
	p.DBDevice = drive
	db, err := p.OpenDB()
	if err != nil {
		fmt.Println("")
		os.Exit(1)
	}
	err1 := db.Ping()
	if err1 != nil {
		os.Exit(1)
	}
	return db
}

/*
库表的初始化
*/
func SchemaTableInit(m *inputArg.ConfigParameter) *schemaTable {
	sdb := dbOpenTest(m.SecondaryL.DsnsV.SrcDrive, m.SecondaryL.DsnsV.SrcJdbc)
	ddb := dbOpenTest(m.SecondaryL.DsnsV.DestDrive, m.SecondaryL.DsnsV.DestJdbc)

	// 初始化表映射关系
	tableMappings := make(map[string]string)

	// 解析tables参数中的映射关系
	tables := m.SecondaryL.SchemaV.Tables
	for _, tableItem := range strings.Split(tables, ",") {
		if strings.Contains(tableItem, ":") {
			parts := strings.Split(tableItem, ":")
			if len(parts) == 2 {
				// 处理db1.*:db2.*格式
				if strings.Contains(parts[0], ".*") && strings.Contains(parts[1], ".*") {
					sourceSchema := strings.TrimSuffix(parts[0], ".*")
					destSchema := strings.TrimSuffix(parts[1], ".*")
					tableMappings[sourceSchema] = destSchema
				} else {
					// 处理db1.table1:db2.table2格式
					sourceParts := strings.Split(parts[0], ".")
					destParts := strings.Split(parts[1], ".")
					if len(sourceParts) >= 1 && len(destParts) >= 1 {
						sourceSchema := sourceParts[0]
						destSchema := destParts[0]
						tableMappings[sourceSchema] = destSchema
					}
				}
			}
		}
	}

	// 添加调试日志
	vlog := fmt.Sprintf("Initialized table mappings: %v", tableMappings)
	global.Wlog.Debug(vlog)

	sourceVersion := global.SourceMySQLVersion
	if detectedVersion, err := queryVersionInfoFromDB(sdb); err == nil {
		sourceVersion = detectedVersion
	} else if strings.TrimSpace(sourceVersion.Raw) == "" {
		global.Wlog.Warn(fmt.Sprintf("SchemaTableInit failed to detect source database version from live connection: %v", err))
	}

	destVersion := global.DestMySQLVersion
	if detectedVersion, err := queryVersionInfoFromDB(ddb); err == nil {
		destVersion = detectedVersion
	} else if strings.TrimSpace(destVersion.Raw) == "" {
		global.Wlog.Warn(fmt.Sprintf("SchemaTableInit failed to detect target database version from live connection: %v", err))
	}

	return &schemaTable{
		ignoreTable:              m.SecondaryL.SchemaV.IgnoreTables,
		table:                    m.SecondaryL.SchemaV.Tables,
		sourceDrive:              m.SecondaryL.DsnsV.SrcDrive,
		destDrive:                m.SecondaryL.DsnsV.DestDrive,
		sourceVersion:            sourceVersion,
		destVersion:              destVersion,
		sourceDB:                 sdb,
		destDB:                   ddb,
		caseSensitiveObjectName:  m.SecondaryL.SchemaV.CaseSensitiveObjectName,
		datafix:                  m.SecondaryL.RepairV.Datafix,
		datafixSql:               m.SecondaryL.RepairV.FixFileDir,
		djdbc:                    m.SecondaryL.DsnsV.DestJdbc,
		checkRules:               m.SecondaryL.RulesV,
		tableMappings:            tableMappings,
		columnRepairMap:          make(map[string][]string),
		indexDiffsMap:            make(map[string]bool),
		partitionDiffsMap:        make(map[string]bool),
		foreignKeyDiffsMap:       make(map[string]bool),
		structWarnOnlyDiffsMap:   make(map[string]bool),
		structCollationMappedMap: make(map[string]bool),
		columnPlan:               m.ColumnPlan,
	}
}

/*
writeFixSql 处理修复SQL文件写入逻辑，每对象写入独立文件（type.schema.object.sql）
*/
func (stcls *schemaTable) writeFixSql(sqls []string, logThreadSeq int64) error {
	if len(sqls) == 0 {
		return nil
	}

	// Merge ALTER TABLE statements for the same table (including non-contiguous ones)
	// to reduce metadata lock overhead and shorten DDL execution time.
	sqls = mergeAlterTableStatements(sqls, logThreadSeq)

	// 执行模式：直接在目标库执行（用于 comment 修复等场景）
	if strings.EqualFold(stcls.datafix, "table") {
		if stcls.destDB == nil {
			return fmt.Errorf("destination DB is nil in datafix=table mode")
		}
		for _, raw := range sqls {
			stmt := normalizeFixSQLForExec(raw)
			if stmt == "" {
				continue
			}
			if _, err := stcls.destDB.Exec(stmt); err != nil {
				return fmt.Errorf("failed to execute fix SQL in table mode: %v, sql: %s", err, stmt)
			}
			global.Wlog.Debug(fmt.Sprintf("(%d) Executed fix SQL in table mode: %s", logThreadSeq, stmt))
		}
		return nil
	}

	// 预览模式：仅写入修复文件
	if !strings.EqualFold(stcls.datafix, "file") {
		return nil
	}

	objType := stcls.fixFileObjectType
	if objType == "" {
		objType = "table"
	}
	tableFileName := fmt.Sprintf("%s/%s.%s.%s.sql",
		stcls.datafixSql, objType,
		fixFileNameEncode(stcls.schema), fixFileNameEncode(stcls.table))
	file, err := os.OpenFile(tableFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open fix file %s: %v", tableFileName, err)
	}
	defer file.Close()

	vlog := fmt.Sprintf("(%d) Opened object-specific fix file %s", logThreadSeq, tableFileName)
	global.Wlog.Debug(vlog)

	return mysql.WriteFixIfNeededFile(stcls.datafix, file, sqls, logThreadSeq, stcls.djdbc)
}

/*
获取源数据库连接
*/
func (stcls *schemaTable) GetSourceDB() *sql.DB {
	return stcls.sourceDB
}

/*
获取目标数据库连接
*/
func (stcls *schemaTable) GetDestDB() *sql.DB {
	return stcls.destDB
}

// generateCreateTableSql 生成创建表的SQL语句，包括表级别的字符集和排序规则
// rewriteCreateTableTargetIdentifier rewrites the leading CREATE TABLE target
// only, so mapped-table repairs do not accidentally keep the source table name.
func rewriteCreateTableTargetIdentifier(createTableStmt, destSchema, destTable string) string {
	matches := createTableTargetIdentifierPattern.FindStringSubmatch(createTableStmt)
	if len(matches) == 0 {
		return createTableStmt
	}
	return createTableTargetIdentifierPattern.ReplaceAllString(createTableStmt, fmt.Sprintf("${1}`%s`.`%s`", destSchema, destTable))
}

func generateCreateTableSql(sourceDB *sql.DB, sourceSchema string, destSchema string, tableName string, destTable string, sourceVersion, destVersion global.MySQLVersionInfo, mariaDBJSONTargetType string, logThreadSeq int64) (string, error) {
	var (
		vlog  string
		event = "generateCreateTableSql"
	)

	// 查询源表的完整DDL，包括AUTO_INCREMENT, TABLE_COLLATION, CREATE_OPTIONS, TABLE_COMMENT等属性
	showCreateTableQuery := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", sourceSchema, tableName)
	var tableName2, createTableStmt string
	err := sourceDB.QueryRow(showCreateTableQuery).Scan(&tableName2, &createTableStmt)
	if err != nil {
		vlog = fmt.Sprintf("(%d) %s Error getting CREATE TABLE statement for %s.%s: %v", logThreadSeq, event, sourceSchema, tableName, err)
		global.Wlog.Error(vlog)
		return "", err
	}

	// 添加IF NOT EXISTS前缀
	if !strings.Contains(strings.ToUpper(createTableStmt), "IF NOT EXISTS") {
		// 查找"CREATE TABLE"后的位置，并在其后添加"IF NOT EXISTS"
		createTableIndex := strings.Index(strings.ToUpper(createTableStmt), "CREATE TABLE")
		if createTableIndex != -1 {
			// 找到"CREATE TABLE"之后的位置
			afterCreateTable := createTableIndex + len("CREATE TABLE")
			// 在"CREATE TABLE"之后插入" IF NOT EXISTS"
			createTableStmt = createTableStmt[:afterCreateTable] + " IF NOT EXISTS" + createTableStmt[afterCreateTable:]
		}
	}

	createTableStmt = rewriteCreateTableTargetIdentifier(createTableStmt, destSchema, destTable)

	// 确保CREATE TABLE语句包含表级别的字符集和排序规则
	// 查询表的字符集和排序规则
	tableCharsetCollationQuery := fmt.Sprintf(`
		SELECT t.TABLE_COLLATION, c.CHARACTER_SET_NAME, t.AUTO_INCREMENT, t.CREATE_OPTIONS, t.TABLE_COMMENT
		FROM information_schema.TABLES t 
		JOIN information_schema.COLLATIONS c ON t.TABLE_COLLATION = c.COLLATION_NAME 
		WHERE t.TABLE_SCHEMA = '%s' AND t.TABLE_NAME = '%s'
	`, sourceSchema, tableName)

	var tableCollation, tableCharset string
	var autoIncrement sql.NullInt64
	var createOptions, tableComment string
	err = sourceDB.QueryRow(tableCharsetCollationQuery).Scan(&tableCollation, &tableCharset, &autoIncrement, &createOptions, &tableComment)
	if err != nil {
		vlog = fmt.Sprintf("(%d) %s Error getting table properties for %s.%s: %v", logThreadSeq, event, sourceSchema, tableName, err)
		global.Wlog.Error(vlog)
		// 即使获取表属性失败，我们仍然可以继续使用原始的CREATE TABLE语句
		return createTableStmt, nil
	}

	// 检查CREATE TABLE语句是否已经包含字符集和排序规则定义
	hasCharset := strings.Contains(strings.ToUpper(createTableStmt), "CHARACTER SET") || strings.Contains(strings.ToUpper(createTableStmt), "CHARSET")
	hasCollation := strings.Contains(strings.ToUpper(createTableStmt), "COLLATE")

	// 如果没有包含字符集和排序规则，添加它们
	if !hasCharset && !hasCollation && tableCharset != "" && tableCollation != "" {
		// 在语句末尾添加字符集和排序规则定义
		// 通常CREATE TABLE语句以ENGINE=xxx结尾，我们需要在这之后添加字符集和排序规则
		if strings.Contains(createTableStmt, "ENGINE=") {
			parts := strings.SplitN(createTableStmt, "ENGINE=", 2)
			if len(parts) == 2 {
				enginePart := parts[1]
				endIndex := strings.Index(enginePart, ";")
				if endIndex != -1 {
					// 在分号前添加字符集和排序规则定义
					createTableStmt = parts[0] + "ENGINE=" + enginePart[:endIndex] +
						fmt.Sprintf(" CHARACTER SET %s COLLATE %s", tableCharset, tableCollation) +
						enginePart[endIndex:]
				} else {
					// 如果没有分号，直接在末尾添加
					createTableStmt = createTableStmt +
						fmt.Sprintf(" CHARACTER SET %s COLLATE %s", tableCharset, tableCollation)
				}
			}
		} else {
			// 如果没有ENGINE=，直接在末尾添加（去掉最后的分号，然后再加上）
			if strings.HasSuffix(createTableStmt, ";") {
				createTableStmt = createTableStmt[:len(createTableStmt)-1] +
					fmt.Sprintf(" CHARACTER SET %s COLLATE %s;", tableCharset, tableCollation)
			} else {
				createTableStmt = createTableStmt +
					fmt.Sprintf(" CHARACTER SET %s COLLATE %s;", tableCharset, tableCollation)
			}
		}
	}

	// 确保AUTO_INCREMENT值被正确设置
	if autoIncrement.Valid && autoIncrement.Int64 > 0 {
		// 检查CREATE TABLE语句是否已经包含AUTO_INCREMENT定义
		hasAutoIncrement := strings.Contains(strings.ToUpper(createTableStmt), "AUTO_INCREMENT")

		if !hasAutoIncrement {
			// 在语句末尾添加AUTO_INCREMENT定义
			if strings.HasSuffix(createTableStmt, ";") {
				createTableStmt = createTableStmt[:len(createTableStmt)-1] +
					fmt.Sprintf(" AUTO_INCREMENT=%d;", autoIncrement.Int64)
			} else {
				createTableStmt = createTableStmt +
					fmt.Sprintf(" AUTO_INCREMENT=%d;", autoIncrement.Int64)
			}
		}
	}

	// 确保表注释被正确设置
	if tableComment != "" && !strings.Contains(strings.ToUpper(createTableStmt), "COMMENT") {
		// 在语句末尾添加表注释
		if strings.HasSuffix(createTableStmt, ";") {
			createTableStmt = createTableStmt[:len(createTableStmt)-1] +
				fmt.Sprintf(" COMMENT='%s';", strings.Replace(tableComment, "'", "\\'", -1))
		} else {
			createTableStmt = createTableStmt +
				fmt.Sprintf(" COMMENT='%s';", strings.Replace(tableComment, "'", "\\'", -1))
		}
	}

	vlog = fmt.Sprintf("(%d) %s Generated CREATE TABLE statement for %s.%s with charset %s and collation %s",
		logThreadSeq, event, destSchema, destTable, tableCharset, tableCollation)
	global.Wlog.Debug(vlog)

	// 确保SQL语句末尾有分号
	if !strings.HasSuffix(createTableStmt, ";") {
		createTableStmt = createTableStmt + ";"
	}

	rewriteNeeded := schemacompat.ShouldRewriteMariaDBCreateTable(createTableStmt, sourceVersion, destVersion)
	if rewriteNeeded {
		beforeRewrite := createTableStmt
		createTableStmt = schemacompat.ConvertMariaDBCreateTableToMySQL(createTableStmt, sourceVersion, destVersion, mariaDBJSONTargetType)
		global.Wlog.Debug(fmt.Sprintf("(%d) %s MariaDB CREATE TABLE rewrite applied for %s.%s: sourceFlavor=%s targetFlavor=%s changed=%t",
			logThreadSeq,
			event,
			destSchema,
			destTable,
			sourceVersion.FlavorName(),
			destVersion.FlavorName(),
			beforeRewrite != createTableStmt,
		))
		if !strings.HasSuffix(createTableStmt, ";") {
			createTableStmt = createTableStmt + ";"
		}
	}

	return createTableStmt, nil
}
