package schemacompat

import (
	"fmt"
	"gt-checksum/global"
	"regexp"
	"sort"
	"strings"
)

type ColumnRepairPlan struct {
	Type                string
	Charset             string
	Collation           string
	UseDirectDefinition bool
	DirectDefinition    string
}

type UnsupportedDDLFeature struct {
	Kind       string
	ObjectName string
	Reason     string
}

func normalizeMariaDBJSONTargetType(targetType string) string {
	switch strings.ToUpper(strings.TrimSpace(targetType)) {
	case "LONGTEXT":
		return "longtext"
	case "TEXT":
		return "text"
	default:
		return "json"
	}
}

var (
	createColumnLineRegexp        = regexp.MustCompile("^\\s*`([^`]+)`\\s+(.*)$")
	createIndexVisibilityRegexp   = regexp.MustCompile("(?i)^\\s*(?:unique\\s+)?key\\s+`([^`]+)`.*\\b(ignored|invisible)\\b")
	jsonValidConstraintRegexp     = regexp.MustCompile("(?i)json_valid\\s*\\(\\s*`?([^`)\\s]+)`?\\s*\\)")
	inlineJSONValidCheckRegexp    = regexp.MustCompile("(?i)\\s+check\\s*\\(\\s*json_valid\\s*\\(\\s*`?[^`)\\s]+`?\\s*\\)\\s*\\)")
	columnCharsetClauseRegexp     = regexp.MustCompile("(?i)\\s+character\\s+set\\s+[a-z0-9_]+")
	columnCollationClauseRegexp   = regexp.MustCompile("(?i)\\s+collate\\s+[a-z0-9_]+")
	mariadbCompressedRegexp       = regexp.MustCompile("(?i)/\\*m!\\d+\\s+compressed\\s*\\*/|\\bcompressed\\b")
	mariadbPersistentRegexp       = regexp.MustCompile("(?i)\\bpersistent\\b")
	generatedAlwaysPrefixRegexp   = regexp.MustCompile("(?i)\\bgenerated\\s+always\\s+")
	mariadbSystemVersioningRegexp = regexp.MustCompile("(?i)\\bwith\\s+system\\s+versioning\\b|\\bperiod\\s+for\\s+system_time\\b|\\bas\\s+row\\s+start\\b|\\bas\\s+row\\s+end\\b")
	mariadbWithoutOverlapsRegexp  = regexp.MustCompile("(?i)\\bwithout\\s+overlaps\\b")
	mariadbNextValueForRegexp     = regexp.MustCompile("(?i)\\bnext\\s+value\\s+for\\s+((`[^`]+`\\.)?`[^`]+`|[a-z0-9_$.]+)")
	doubleSpaceRegexp             = regexp.MustCompile(`\s{2,}`)
)

func ExtractColumnDefinitionsFromCreateSQL(createSQL string) map[string]string {
	result := make(map[string]string)
	for _, rawLine := range strings.Split(createSQL, "\n") {
		line := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(rawLine), ","))
		if !strings.HasPrefix(line, "`") {
			continue
		}
		matches := createColumnLineRegexp.FindStringSubmatch(line)
		if len(matches) != 3 {
			continue
		}
		result[matches[1]] = strings.TrimSpace(matches[2])
	}
	return result
}

func ExtractIndexVisibilityHintsFromCreateSQL(createSQL string) map[string]string {
	result := make(map[string]string)
	for _, rawLine := range strings.Split(createSQL, "\n") {
		line := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(rawLine), ","))
		matches := createIndexVisibilityRegexp.FindStringSubmatch(line)
		if len(matches) != 3 {
			continue
		}
		result[matches[1]] = strings.ToUpper(matches[2])
	}
	return result
}

// DetectMariaDBUnsupportedTableFeatures identifies MariaDB-only constructs that
// must stay on the advisory path when the target is MySQL.
func DetectMariaDBUnsupportedTableFeatures(createSQL string, sourceInfo, targetInfo global.MySQLVersionInfo) []UnsupportedDDLFeature {
	if sourceInfo.Flavor != global.DatabaseFlavorMariaDB || targetInfo.Flavor != global.DatabaseFlavorMySQL {
		return nil
	}

	features := make([]UnsupportedDDLFeature, 0)
	seen := make(map[string]struct{})
	appendFeature := func(kind, objectName, reason string) {
		key := strings.ToUpper(strings.TrimSpace(kind) + "|" + strings.TrimSpace(objectName))
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		features = append(features, UnsupportedDDLFeature{
			Kind:       kind,
			ObjectName: objectName,
			Reason:     reason,
		})
	}

	if mariadbSystemVersioningRegexp.MatchString(createSQL) {
		appendFeature(
			"SYSTEM VERSIONING",
			"",
			"MariaDB system-versioned table metadata has no direct MySQL equivalent; automatic repair is disabled",
		)
	}
	if mariadbWithoutOverlapsRegexp.MatchString(createSQL) {
		appendFeature(
			"WITHOUT OVERLAPS",
			"",
			"MariaDB application-time period constraints have no direct MySQL equivalent; automatic repair is disabled",
		)
	}
	for _, matches := range mariadbNextValueForRegexp.FindAllStringSubmatch(createSQL, -1) {
		if len(matches) < 2 {
			continue
		}
		sequenceName := normalizeSequenceReferenceName(matches[1])
		if sequenceName == "" {
			continue
		}
		appendFeature(
			"SEQUENCE",
			sequenceName,
			fmt.Sprintf("column defaults reference MariaDB sequence %s via NEXT VALUE FOR; automatic rewrite is disabled", sequenceName),
		)
	}

	return features
}

func CanonicalizeColumnForComparison(name string, attrs []string, sourceInfo, targetInfo global.MySQLVersionInfo, createDefinition, jsonTargetType string) CanonicalColumn {
	column := CanonicalizeMySQLColumn(name, attrs, sourceInfo)
	column.GeneratedExpression, column.GeneratedKind = extractGeneratedColumnMetadata(column.RawType, createDefinition, sourceInfo)

	if sourceInfo.Flavor == global.DatabaseFlavorMariaDB && targetInfo.Flavor == global.DatabaseFlavorMySQL {
		if column.CompressionAttr == "" && isMariaDBCompressedDefinition(createDefinition) {
			column.CompressionAttr = "COMPRESSED"
		}
		jsonTarget := normalizeMariaDBJSONTargetType(jsonTargetType)
		switch {
		case isMariaDBJSONAliasDefinition(name, createDefinition):
			if jsonTarget == "json" {
				column.NormalizedType = "json"
				column.Charset = ""
				column.Collation = ""
				column.NormalizedCharset = ""
				column.NormalizedCollation = ""
			} else {
				column.NormalizedType = jsonTarget
				column.SemanticWarning = fmt.Sprintf("MariaDB JSON alias is configured to downgrade to %s on MySQL target", strings.ToUpper(jsonTarget))
			}
		case isMariaDBINET6Definition(name, createDefinition) || strings.EqualFold(column.NormalizedType, "inet6"):
			column.NormalizedType = "varchar(39)"
			column.NormalizedCharset = normalizeCharset(targetDefaultCharset(sourceInfo))
			if column.NormalizedCharset == "" {
				column.NormalizedCharset = normalizeCharset(targetDefaultCharset(targetInfo))
			}
			column.NormalizedCollation = normalizeCollation(targetDefaultCollation(sourceInfo, column.NormalizedCharset))
			if column.NormalizedCollation == "" {
				column.NormalizedCollation = normalizeCollation(targetDefaultCollation(targetInfo, column.NormalizedCharset))
			}
		case isMariaDBUUIDDefinition(name, createDefinition) || strings.EqualFold(column.NormalizedType, "uuid"):
			column.NormalizedType = "char(36)"
			column.NormalizedCharset = normalizeCharset(targetDefaultCharset(sourceInfo))
			if column.NormalizedCharset == "" {
				column.NormalizedCharset = normalizeCharset(targetDefaultCharset(targetInfo))
			}
			column.NormalizedCollation = normalizeCollation(targetDefaultCollation(sourceInfo, column.NormalizedCharset))
			if column.NormalizedCollation == "" {
				column.NormalizedCollation = normalizeCollation(targetDefaultCollation(targetInfo, column.NormalizedCharset))
			}
		}
	}

	return column
}

func BuildTargetColumnRepairPlan(name string, attrs []string, sourceInfo, targetInfo global.MySQLVersionInfo, createDefinition, jsonTargetType string) ColumnRepairPlan {
	plan := ColumnRepairPlan{
		Type:      normalizeNullish(getColumnAttr(attrs, 0)),
		Charset:   normalizeNullish(getColumnAttr(attrs, 1)),
		Collation: normalizeNullish(getColumnAttr(attrs, 2)),
	}

	// GENERATED COLUMN: 所有 flavor 组合统一走 DirectDefinition，保留表达式和正确语法。
	// 必须在 MariaDB 早返回之前检查，否则 MySQL→MySQL 场景会跳过此处理。
	if hasGeneratedColumnDefinition(createDefinition) {
		plan.UseDirectDefinition = true
		if sourceInfo.Flavor == global.DatabaseFlavorMariaDB && targetInfo.Flavor == global.DatabaseFlavorMySQL {
			plan.DirectDefinition = convertMariaDBColumnDefinitionForMySQL(name, createDefinition)
		} else {
			// MySQL→MySQL 及其他组合：createDefinition 来自 SHOW CREATE TABLE，已是合法 DDL，直接使用。
			def := normalizeWhitespace(strings.TrimSpace(stripLeadingColumnIdentifier(name, createDefinition)))
			plan.DirectDefinition = def
		}
		return plan
	}

	if sourceInfo.Flavor != global.DatabaseFlavorMariaDB || targetInfo.Flavor != global.DatabaseFlavorMySQL {
		// MariaDB→MariaDB: COMPRESSED, PERSISTENT and similar attributes are
		// natively supported on the target side — do not strip them.
		if targetInfo.Flavor != global.DatabaseFlavorMariaDB {
			plan.Type = stripMariaDBOnlyColumnAttributes(plan.Type)
		}
		return plan
	}

	switch {
	case isMariaDBJSONAliasDefinition(name, createDefinition):
		switch normalizeMariaDBJSONTargetType(jsonTargetType) {
		case "longtext":
			plan.Type = "longtext"
		case "text":
			plan.Type = "text"
		default:
			plan.Type = "json"
			plan.Charset = "null"
			plan.Collation = "null"
		}
	case isMariaDBINET6Definition(name, createDefinition) || strings.EqualFold(normalizeWhitespace(plan.Type), "inet6"):
		plan.Type = "varchar(39)"
		plan.Charset = "null"
		plan.Collation = "null"
	case isMariaDBUUIDDefinition(name, createDefinition) || strings.EqualFold(normalizeWhitespace(plan.Type), "uuid"):
		plan.Type = "char(36)"
		plan.Charset = "null"
		plan.Collation = "null"
	default:
		plan.Type = stripMariaDBOnlyColumnAttributes(plan.Type)
	}
	if !plan.UseDirectDefinition {
		plan.Type = normalizeMySQLRepairColumnType(plan.Type)
	}

	// MariaDB UCA 14.0.0 collation 在 MySQL 上不存在，映射为 UCA 9.0.0 等价物
	if plan.Collation != "" && plan.Collation != "null" {
		if mapped, ok := MapMariaDBCollationToMySQL(plan.Collation); ok {
			plan.Collation = mapped
		}
	}

	return plan
}

func normalizeMySQLRepairColumnType(columnType string) string {
	normalized := normalizeWhitespace(strings.TrimSpace(columnType))
	if normalized == "" {
		return normalized
	}

	normalized = normalizeMySQLKeywordFunctionsForType(normalized)
	if !strings.Contains(strings.ToUpper(normalized), "ZEROFILL") {
		normalized = integerDisplayWidthRegex.ReplaceAllString(normalized, "${1}")
	}
	normalized = yearDisplayWidthRegex.ReplaceAllString(normalized, "year")
	return normalizeWhitespace(normalized)
}

func ConvertMariaDBCreateTableToMySQL(createSQL string, sourceInfo, targetInfo global.MySQLVersionInfo, jsonTargetType string) string {
	if !ShouldRewriteMariaDBCreateTable(createSQL, sourceInfo, targetInfo) {
		return createSQL
	}

	lines := strings.Split(createSQL, "\n")
	for i, rawLine := range lines {
		trimmed := strings.TrimSpace(rawLine)
		hasTrailingComma := strings.HasSuffix(trimmed, ",")

		if strings.HasPrefix(trimmed, "`") {
			matches := createColumnLineRegexp.FindStringSubmatch(strings.TrimSuffix(trimmed, ","))
			if len(matches) == 3 {
				columnName := matches[1]
				definition := matches[2]
				rewritten := rewriteMariaDBCreateColumnDefinitionForMySQL(columnName, definition, jsonTargetType)
				lines[i] = preserveLeadingWhitespace(rawLine) + fmt.Sprintf("`%s` %s", columnName, rewritten)
				if hasTrailingComma {
					lines[i] += ","
				}
				continue
			}
		}

		if matches := createIndexVisibilityRegexp.FindStringSubmatch(trimmed); len(matches) == 3 {
			lines[i] = strings.Replace(rawLine, matches[2], "INVISIBLE", 1)
		}
	}

	return strings.Join(lines, "\n")
}

func ShouldRewriteMariaDBCreateTable(createSQL string, sourceInfo, targetInfo global.MySQLVersionInfo) bool {
	if targetInfo.Flavor != "" && targetInfo.Flavor != global.DatabaseFlavorMySQL {
		return false
	}
	if sourceInfo.Flavor == global.DatabaseFlavorMariaDB {
		return true
	}

	rawLowerSQL := strings.ToLower(createSQL)
	normalizedSQL := " " + strings.ToLower(normalizeWhitespace(createSQL)) + " "
	return strings.Contains(rawLowerSQL, "json_valid(") ||
		strings.Contains(normalizedSQL, " json_valid (") ||
		strings.Contains(normalizedSQL, " inet6 ") ||
		strings.Contains(normalizedSQL, " uuid ") ||
		strings.Contains(rawLowerSQL, "compressed") ||
		strings.Contains(normalizedSQL, " ignored ")
}

func FilterPortableCheckConstraints(sourceChecks []CanonicalConstraint, sourceInfo, targetInfo global.MySQLVersionInfo, sourceColumnDefinitions map[string]string) []CanonicalConstraint {
	if sourceInfo.Flavor != global.DatabaseFlavorMariaDB || targetInfo.Flavor != global.DatabaseFlavorMySQL {
		return sourceChecks
	}

	filtered := make([]CanonicalConstraint, 0, len(sourceChecks))
	for _, item := range sourceChecks {
		if isMariaDBJSONAliasCheckConstraint(item, sourceColumnDefinitions) {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func DetectMariaDBJSONDowngradeColumns(createSQL string, sourceInfo, targetInfo global.MySQLVersionInfo, jsonTargetType string) []string {
	if sourceInfo.Flavor != global.DatabaseFlavorMariaDB || targetInfo.Flavor != global.DatabaseFlavorMySQL {
		return nil
	}
	jsonTarget := normalizeMariaDBJSONTargetType(jsonTargetType)
	if jsonTarget == "json" {
		return nil
	}

	columnDefinitions := ExtractColumnDefinitionsFromCreateSQL(createSQL)
	columnNames := make([]string, 0)
	for columnName, definition := range columnDefinitions {
		if isMariaDBJSONAliasDefinition(columnName, definition) {
			columnNames = append(columnNames, columnName)
		}
	}
	sort.Strings(columnNames)
	return columnNames
}

func getColumnAttr(attrs []string, idx int) string {
	if idx < 0 || idx >= len(attrs) {
		return ""
	}
	return attrs[idx]
}

func targetDefaultCharset(info global.MySQLVersionInfo) string {
	catalog := BuildSchemaFeatureCatalog(info)
	if strings.TrimSpace(catalog.DefaultCharset) == "" {
		return "utf8mb4"
	}
	return catalog.DefaultCharset
}

func targetDefaultCollation(info global.MySQLVersionInfo, charset string) string {
	catalog := BuildSchemaFeatureCatalog(info)
	normalizedCharset := normalizeCharset(charset)
	if collation, ok := catalog.DefaultCollationByCharset[normalizedCharset]; ok {
		return collation
	}
	return ""
}

func normalizeSequenceReferenceName(name string) string {
	normalized := strings.TrimSpace(strings.ReplaceAll(name, "`", ""))
	return normalizeWhitespace(normalized)
}

func stripMariaDBOnlyColumnAttributes(columnType string) string {
	normalized := strings.TrimSpace(columnType)
	if normalized == "" {
		return normalized
	}
	normalized = mariadbPersistentRegexp.ReplaceAllString(normalized, "STORED")
	normalized = strings.ReplaceAll(normalized, "PERSISTENT GENERATED", "STORED GENERATED")
	normalized = strings.ReplaceAll(normalized, "persistent generated", "stored generated")
	normalized = mariadbCompressedRegexp.ReplaceAllString(normalized, "")
	return normalizeWhitespace(normalized)
}

func isMariaDBJSONAliasDefinition(columnName, definition string) bool {
	def := strings.ToLower(normalizeWhitespace(definition))
	if def == "" {
		return false
	}
	if strings.Contains(def, " json ") || strings.HasPrefix(def, "json ") || def == "json" {
		return true
	}
	if !strings.Contains(def, "longtext") || !strings.Contains(def, "json_valid") {
		return false
	}
	matches := jsonValidConstraintRegexp.FindStringSubmatch(definition)
	if len(matches) != 2 {
		return false
	}
	return strings.EqualFold(matches[1], columnName)
}

func isMariaDBINET6Definition(columnName, definition string) bool {
	def := strings.ToLower(normalizeWhitespace(stripLeadingColumnIdentifier(columnName, definition)))
	return strings.HasPrefix(def, "inet6 ") || def == "inet6"
}

func isMariaDBUUIDDefinition(columnName, definition string) bool {
	def := strings.ToLower(normalizeWhitespace(stripLeadingColumnIdentifier(columnName, definition)))
	return strings.HasPrefix(def, "uuid ") || def == "uuid"
}

func isMariaDBCompressedDefinition(definition string) bool {
	return mariadbCompressedRegexp.MatchString(definition)
}

func isMariaDBJSONAliasCheckConstraint(item CanonicalConstraint, columnDefinitions map[string]string) bool {
	matches := jsonValidConstraintRegexp.FindStringSubmatch(item.NormalizedDefinition)
	if len(matches) != 2 {
		return false
	}
	columnName := matches[1]
	definition, ok := columnDefinitions[columnName]
	if !ok {
		return false
	}
	return isMariaDBJSONAliasDefinition(columnName, definition)
}

func hasGeneratedColumnDefinition(definition string) bool {
	_, _, ok := extractGeneratedColumnParts(definition)
	return ok
}

func extractGeneratedColumnMetadata(rawType, createDefinition string, sourceInfo global.MySQLVersionInfo) (string, string) {
	expr, kind, ok := extractGeneratedColumnParts(createDefinition)
	if ok {
		return normalizeWhitespace(expr), normalizeGeneratedKind(kind, sourceInfo.Flavor)
	}

	raw := strings.ToUpper(normalizeWhitespace(rawType))
	switch {
	case strings.Contains(raw, "STORED GENERATED"):
		return "", "STORED"
	case strings.Contains(raw, "VIRTUAL GENERATED"):
		return "", "VIRTUAL"
	case strings.Contains(raw, "PERSISTENT GENERATED"):
		return "", normalizeGeneratedKind("PERSISTENT", sourceInfo.Flavor)
	default:
		return "", ""
	}
}

func normalizeGeneratedKind(kind string, flavor global.DatabaseFlavor) string {
	switch strings.ToUpper(strings.TrimSpace(kind)) {
	case "PERSISTENT":
		if flavor == global.DatabaseFlavorMariaDB {
			return "STORED"
		}
		return "PERSISTENT"
	case "STORED":
		return "STORED"
	case "VIRTUAL":
		return "VIRTUAL"
	default:
		return ""
	}
}

func extractGeneratedColumnParts(definition string) (string, string, bool) {
	def := strings.TrimSpace(definition)
	if def == "" {
		return "", "", false
	}

	lower := strings.ToLower(def)
	asIndex := strings.Index(lower, " as (")
	if asIndex == -1 {
		asIndex = strings.Index(lower, " generated always as (")
	}
	if asIndex == -1 {
		return "", "", false
	}

	openIndex := strings.Index(def[asIndex:], "(")
	if openIndex == -1 {
		return "", "", false
	}
	openIndex += asIndex

	depth := 0
	closeIndex := -1
	for i := openIndex; i < len(def); i++ {
		switch def[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				closeIndex = i
				i = len(def)
			}
		}
	}
	if closeIndex == -1 {
		return "", "", false
	}

	expr := strings.TrimSpace(def[openIndex+1 : closeIndex])
	suffix := strings.ToUpper(strings.TrimSpace(def[closeIndex+1:]))
	switch {
	case strings.Contains(suffix, "PERSISTENT"):
		return expr, "PERSISTENT", true
	case strings.Contains(suffix, "STORED"):
		return expr, "STORED", true
	case strings.Contains(suffix, "VIRTUAL"):
		return expr, "VIRTUAL", true
	default:
		return expr, "", true
	}
}

func convertMariaDBColumnDefinitionForMySQL(columnName, definition string) string {
	def := strings.TrimSpace(definition)
	def = stripLeadingColumnIdentifier(columnName, def)
	def = stripMariaDBOnlyColumnAttributes(def)
	def = generatedAlwaysPrefixRegexp.ReplaceAllString(def, "")
	if expr, kind, ok := extractGeneratedColumnParts(definition); ok {
		baseType := strings.TrimSpace(def[:strings.Index(strings.ToLower(def), " as (")])
		if baseType == "" {
			baseType = strings.TrimSpace(stripLeadingColumnIdentifier(columnName, definition))
		}
		baseType = strings.TrimSpace(strings.TrimSuffix(baseType, ","))
		baseType = stripMariaDBOnlyColumnAttributes(baseType)
		if kind == "" {
			kind = "STORED"
		}
		if strings.EqualFold(kind, "PERSISTENT") {
			kind = "STORED"
		}
		def = fmt.Sprintf("%s GENERATED ALWAYS AS (%s) %s", baseType, normalizeWhitespace(expr), strings.ToUpper(kind))
	}
	return normalizeWhitespace(def)
}

func stripLeadingColumnIdentifier(columnName, definition string) string {
	trimmed := strings.TrimSpace(definition)
	prefix := fmt.Sprintf("`%s`", columnName)
	if strings.HasPrefix(trimmed, prefix) {
		return strings.TrimSpace(strings.TrimPrefix(trimmed, prefix))
	}
	return trimmed
}

func rewriteMariaDBCreateColumnDefinitionForMySQL(columnName, definition, jsonTargetType string) string {
	normalizedDefinition := strings.TrimSpace(definition)
	switch {
	case isMariaDBJSONAliasDefinition(columnName, normalizedDefinition):
		rewritten := stripMariaDBOnlyColumnAttributes(normalizedDefinition)
		rewritten = inlineJSONValidCheckRegexp.ReplaceAllString(rewritten, "")
		switch normalizeMariaDBJSONTargetType(jsonTargetType) {
		case "json":
			rewritten = columnCharsetClauseRegexp.ReplaceAllString(rewritten, "")
			rewritten = columnCollationClauseRegexp.ReplaceAllString(rewritten, "")
			rewritten = replaceLeadingColumnType(rewritten, "json")
		case "text":
			rewritten = replaceLeadingColumnType(rewritten, "text")
		default:
			rewritten = replaceLeadingColumnType(rewritten, "longtext")
		}
		return normalizeWhitespace(rewritten)
	case hasGeneratedColumnDefinition(normalizedDefinition):
		return convertMariaDBColumnDefinitionForMySQL(columnName, normalizedDefinition)
	}

	rewritten := stripMariaDBOnlyColumnAttributes(normalizedDefinition)
	typeHead := extractColumnTypeHead(rewritten)
	switch strings.ToLower(typeHead) {
	case "inet6":
		rewritten = replaceLeadingColumnType(rewritten, "varchar(39)")
	case "uuid":
		rewritten = replaceLeadingColumnType(rewritten, "char(36)")
	}
	return normalizeWhitespace(rewritten)
}

func extractColumnTypeHead(definition string) string {
	trimmed := strings.TrimSpace(definition)
	for i, r := range trimmed {
		if r == ' ' || r == '\t' {
			return trimmed[:i]
		}
	}
	return trimmed
}

func replaceLeadingColumnType(definition, newType string) string {
	oldType := extractColumnTypeHead(definition)
	if oldType == "" {
		return newType
	}
	return strings.TrimSpace(newType + definition[len(oldType):])
}

func extractColumnCommentClause(definition string) string {
	upper := strings.ToUpper(definition)
	idx := strings.Index(upper, " COMMENT ")
	if idx == -1 {
		return ""
	}
	return strings.TrimSpace(definition[idx+1:])
}

func preserveLeadingWhitespace(line string) string {
	for i, r := range line {
		if r != ' ' && r != '\t' {
			return line[:i]
		}
	}
	return ""
}
