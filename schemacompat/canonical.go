package schemacompat

import (
	"fmt"
	"gt-checksum/global"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type ColumnVisibility string

const (
	ColumnVisibilityVisible   ColumnVisibility = "VISIBLE"
	ColumnVisibilityInvisible ColumnVisibility = "INVISIBLE"
)

type IndexVisibility string

const (
	IndexVisibilityVisible   IndexVisibility = "VISIBLE"
	IndexVisibilityInvisible IndexVisibility = "INVISIBLE"
)

type CompatibilityState string

const (
	CompatibilityEqual           CompatibilityState = "equal"
	CompatibilityNormalizedEqual CompatibilityState = "normalized-equal"
	CompatibilityConvertible     CompatibilityState = "convertible"
	CompatibilityWarnOnly        CompatibilityState = "warn-only"
	CompatibilityUnsupported     CompatibilityState = "unsupported"
)

type CompatibilityDecision struct {
	State  CompatibilityState
	Reason string
	Source string
	Target string
}

func (d CompatibilityDecision) IsMismatch() bool {
	switch d.State {
	case CompatibilityEqual, CompatibilityNormalizedEqual:
		return false
	default:
		return true
	}
}

type CanonicalColumn struct {
	Name                string
	RawType             string
	NormalizedType      string
	SemanticWarning     string
	CompressionAttr     string
	GeneratedKind       string
	GeneratedExpression string
	Charset             string
	NormalizedCharset   string
	Collation           string
	NormalizedCollation string
	Nullable            bool
	DefaultValue        string
	Comment             string
	Visibility          ColumnVisibility
	AutoIncrement       bool
}

type CanonicalIndex struct {
	Name                  string
	Type                  string
	Columns               []string
	PrefixLength          []int
	Visibility            IndexVisibility
	NormalizedExpressions []string
}

type CanonicalConstraint struct {
	Name                 string
	Kind                 string
	Definition           string
	NormalizedDefinition string
	Columns              []string
	ReferencedSchema     string
	ReferencedTable      string
	ReferencedColumns    []string
	DeleteRule           string
	UpdateRule           string
}

type CanonicalTableOptions struct {
	Engine              string
	RowFormat           string
	NormalizedRowFormat string
	CreateOptions       string
	ExplicitRowFormat   bool
	Comment             string
}

var (
	whitespaceRegexp           = regexp.MustCompile(`\s+`)
	integerDisplayWidthRegex   = regexp.MustCompile(`\b(tinyint|smallint|mediumint|int|integer|bigint)\s*\(\s*\d+\s*\)`)
	yearDisplayWidthRegex      = regexp.MustCompile(`\byear\s*\(\s*4\s*\)`)
	mysqlKeywordDefaultRegex   = regexp.MustCompile(`(?i)^(current_timestamp|current_date|current_time|localtime|localtimestamp)(?:\((\d*)\))?$`)
	mysqlKeywordInTypeRegexp   = regexp.MustCompile(`(?i)\b(current_timestamp|current_date|current_time|localtime|localtimestamp)(?:\((\d*)\))?`)
	mysqlDateTimeDefaultRegexp = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(\.\d{1,6})?$`)
	rowFormatOptionRegexp      = regexp.MustCompile(`(?i)\brow_format\s*=\s*([a-z_]+)`)
	checkWithNameRegexp        = regexp.MustCompile("(?i)constraint\\s+`?([^`\\s]+)`?\\s+check\\s*(\\(.*\\))")
	checkNoNameRegexp          = regexp.MustCompile(`(?i)\bcheck\s*(\(.+\))`)
	fkNameRegexp               = regexp.MustCompile("(?i)constraint\\s+!([^!]+)!\\s+foreign\\s+key")
	fkDefinitionRegexp         = regexp.MustCompile(`(?i)foreign\s+key\s*\((.*?)\)\s*references\s*!([^!]+)!\s*\.\s*!([^!]+)!\s*\((.*?)\)`)
	fkDeleteRuleRegexp         = regexp.MustCompile(`(?i)\bon\s+delete\s+(cascade|restrict|set null|no action|set default)`)
	fkUpdateRuleRegexp         = regexp.MustCompile(`(?i)\bon\s+update\s+(cascade|restrict|set null|no action|set default)`)
	mariadbCompressedComment   = regexp.MustCompile(`(?i)/\*m!\d+\s+compressed\s*\*/`)
)

func normalizeWhitespace(v string) string {
	return strings.TrimSpace(whitespaceRegexp.ReplaceAllString(v, " "))
}

func normalizeNullish(v string) string {
	s := strings.TrimSpace(v)
	switch strings.ToLower(s) {
	case "", "null", "<nil>", "<entry>":
		return ""
	default:
		return s
	}
}

func normalizeCharset(v string) string {
	s := strings.ToLower(normalizeNullish(v))
	switch s {
	case "utf8":
		return "utf8mb3"
	default:
		return s
	}
}

func normalizeCollation(v string) string {
	s := strings.ToLower(normalizeNullish(v))
	if strings.HasPrefix(s, "utf8_") {
		return "utf8mb3_" + strings.TrimPrefix(s, "utf8_")
	}
	return s
}

func normalizeColumnDefaultValue(v string) string {
	raw := strings.TrimSpace(v)
	switch strings.ToLower(raw) {
	case "", "null", "<nil>", "<entry>":
		return ""
	}

	matches := mysqlKeywordDefaultRegex.FindStringSubmatch(raw)
	if len(matches) != 3 {
		unquoted, _ := UnwrapQuotedDefaultLiteral(raw)
		return normalizeDefaultLiteral(unquoted)
	}

	name := strings.ToUpper(matches[1])
	if matches[2] == "" {
		return name
	}
	return fmt.Sprintf("%s(%s)", name, matches[2])
}

// UnwrapQuotedDefaultLiteral removes up to 4 layers of escaped/nested quotes
// from MySQL INFORMATION_SCHEMA DEFAULT values. Exported so that the fixsql
// generator in the mysql package can reuse the same logic without duplication.
func UnwrapQuotedDefaultLiteral(v string) (string, bool) {
	s := strings.TrimSpace(v)
	changed := false
	// MySQL INFORMATION_SCHEMA DEFAULT values may be wrapped in up to 4
	// layers of escaped quotes (e.g. \'...\' → '...' → inner), depending
	// on the MySQL version and how the value was originally declared. Four
	// iterations covers all observed nesting depths across 5.6 → 8.4.
	for i := 0; i < 4; i++ {
		if strings.HasPrefix(s, "\\'") && strings.HasSuffix(s, "\\'") && len(s) >= 4 {
			s = "'" + s[2:len(s)-2] + "'"
		}
		if strings.HasPrefix(s, "\\\"") && strings.HasSuffix(s, "\\\"") && len(s) >= 4 {
			s = `"` + s[2:len(s)-2] + `"`
		}
		if len(s) < 2 {
			break
		}
		quote := s[0]
		if (quote != '\'' && quote != '"') || s[len(s)-1] != quote {
			break
		}

		inner := strings.TrimSpace(s[1 : len(s)-1])
		if quote == '\'' {
			inner = strings.ReplaceAll(inner, "\\'", "'")
			inner = strings.ReplaceAll(inner, "''", "'")
		} else {
			inner = strings.ReplaceAll(inner, "\\\"", `"`)
			inner = strings.ReplaceAll(inner, `""`, `"`)
		}
		s = inner
		changed = true
	}
	return s, changed
}

func normalizeDefaultLiteral(v string) string {
	normalized := normalizeWhitespace(strings.TrimSpace(v))
	if normalized == "" {
		return ""
	}

	if matches := mysqlDateTimeDefaultRegexp.FindStringSubmatch(normalized); len(matches) == 4 {
		return matches[1] + " " + matches[2] + matches[3]
	}
	if len(normalized) >= 19 && normalized[10] == 'T' {
		return normalized[:10] + " " + normalized[11:]
	}
	return normalized
}

func normalizeMySQLKeywordFunctionsForType(v string) string {
	return mysqlKeywordInTypeRegexp.ReplaceAllStringFunc(v, func(match string) string {
		matches := mysqlKeywordDefaultRegex.FindStringSubmatch(strings.TrimSpace(match))
		if len(matches) != 3 {
			return match
		}
		name := strings.ToLower(matches[1])
		if matches[2] == "" {
			return name
		}
		return fmt.Sprintf("%s(%s)", name, matches[2])
	})
}

// StripMySQLMetadataOnlyExtraTokens removes INFORMATION_SCHEMA.EXTRA markers
// that describe metadata state but are not valid standalone DDL fragments.
func StripMySQLMetadataOnlyExtraTokens(value string) string {
	s := normalizeWhitespace(strings.TrimSpace(value))
	if s == "" {
		return s
	}
	if strings.Contains(strings.ToLower(s), "default_generated") {
		s = strings.ReplaceAll(strings.ReplaceAll(s, "DEFAULT_GENERATED", " "), "default_generated", " ")
		s = normalizeWhitespace(s)
	}
	return s
}

func normalizeMySQLColumnType(raw string) (string, string, ColumnVisibility, bool, string) {
	s := strings.ToLower(StripMySQLMetadataOnlyExtraTokens(raw))
	visibility := ColumnVisibilityVisible
	autoIncrement := false
	compressionAttr := ""
	hasZeroFill := false
	generatedKind := ""

	if strings.Contains(s, "/*80023 invisible */") {
		visibility = ColumnVisibilityInvisible
		s = strings.ReplaceAll(s, "/*80023 invisible */", " ")
	}
	if strings.Contains(s, " invisible") || strings.HasSuffix(s, "invisible") {
		visibility = ColumnVisibilityInvisible
		s = strings.ReplaceAll(s, " invisible", " ")
	}
	if strings.Contains(s, " visible") || strings.HasSuffix(s, "visible") {
		s = strings.ReplaceAll(s, " visible", " ")
	}
	if strings.Contains(s, "auto_increment") {
		autoIncrement = true
		s = strings.ReplaceAll(s, "auto_increment", " ")
	}
	if mariadbCompressedComment.MatchString(s) {
		compressionAttr = "COMPRESSED"
		s = mariadbCompressedComment.ReplaceAllString(s, " ")
	}
	if strings.Contains(s, " compressed") || strings.HasSuffix(s, "compressed") {
		compressionAttr = "COMPRESSED"
		s = strings.ReplaceAll(s, " compressed", " ")
	}
	if strings.Contains(s, " persistent generated") || strings.HasSuffix(s, "persistent generated") {
		generatedKind = "STORED"
		s = strings.ReplaceAll(s, " persistent generated", " ")
	}
	if strings.Contains(s, " stored generated") || strings.HasSuffix(s, "stored generated") {
		generatedKind = "STORED"
		s = strings.ReplaceAll(s, " stored generated", " ")
	}
	if strings.Contains(s, " virtual generated") || strings.HasSuffix(s, "virtual generated") {
		generatedKind = "VIRTUAL"
		s = strings.ReplaceAll(s, " virtual generated", " ")
	}
	if strings.Contains(s, " generated always") {
		s = strings.ReplaceAll(s, " generated always", " ")
	}
	if strings.Contains(s, " generated") || strings.HasSuffix(s, "generated") {
		s = strings.ReplaceAll(s, " generated", " ")
	}
	if strings.Contains(s, " zerofill") || strings.HasSuffix(s, "zerofill") {
		hasZeroFill = true
		s = strings.ReplaceAll(s, " zerofill", " ")
	}

	s = normalizeWhitespace(s)
	s = strings.ReplaceAll(s, "integer", "int")
	s = integerDisplayWidthRegex.ReplaceAllString(s, "${1}")
	s = normalizeMySQLKeywordFunctionsForType(s)
	// YEAR(4) in MySQL 5.6/5.7 is rendered as YEAR in MySQL 8.0+, so treat
	// the display width drift as a textual difference only.
	s = yearDisplayWidthRegex.ReplaceAllString(s, "year")
	if hasZeroFill && !strings.Contains(s, " unsigned") && !strings.HasSuffix(s, "unsigned") {
		s = normalizeWhitespace(s + " unsigned")
	}
	s = normalizeWhitespace(s)

	return s, compressionAttr, visibility, autoIncrement, generatedKind
}

func CanonicalizeMySQLColumn(name string, attrs []string, _ global.MySQLVersionInfo) CanonicalColumn {
	getAttr := func(idx int) string {
		if idx < 0 || idx >= len(attrs) {
			return ""
		}
		return attrs[idx]
	}

	rawType := normalizeNullish(getAttr(0))
	normalizedType, compressionAttr, visibility, autoIncrement, generatedKind := normalizeMySQLColumnType(rawType)

	return CanonicalColumn{
		Name:                name,
		RawType:             rawType,
		NormalizedType:      normalizedType,
		CompressionAttr:     compressionAttr,
		GeneratedKind:       generatedKind,
		Charset:             normalizeNullish(getAttr(1)),
		NormalizedCharset:   normalizeCharset(getAttr(1)),
		Collation:           normalizeNullish(getAttr(2)),
		NormalizedCollation: normalizeCollation(getAttr(2)),
		Nullable:            strings.EqualFold(normalizeNullish(getAttr(3)), "YES"),
		DefaultValue:        normalizeColumnDefaultValue(getAttr(4)),
		Comment:             normalizeNullish(getAttr(5)),
		Visibility:          visibility,
		AutoIncrement:       autoIncrement,
	}
}

func DecideColumnDefinitionCompatibility(source, target CanonicalColumn) CompatibilityDecision {
	if source.NormalizedType != target.NormalizedType {
		return CompatibilityDecision{
			State:  CompatibilityUnsupported,
			Reason: fmt.Sprintf("column type differs after normalization: source=%s target=%s", source.NormalizedType, target.NormalizedType),
			Source: source.RawType,
			Target: target.RawType,
		}
	}
	if source.AutoIncrement != target.AutoIncrement {
		return CompatibilityDecision{
			State:  CompatibilityUnsupported,
			Reason: fmt.Sprintf("auto_increment attribute differs: source=%t target=%t", source.AutoIncrement, target.AutoIncrement),
			Source: source.RawType,
			Target: target.RawType,
		}
	}
	if source.Visibility != target.Visibility {
		return CompatibilityDecision{
			State:  CompatibilityUnsupported,
			Reason: fmt.Sprintf("column visibility differs: source=%s target=%s", source.Visibility, target.Visibility),
			Source: source.RawType,
			Target: target.RawType,
		}
	}
	if source.GeneratedKind != target.GeneratedKind {
		return CompatibilityDecision{
			State:  CompatibilityUnsupported,
			Reason: fmt.Sprintf("generated column kind differs: source=%s target=%s", source.GeneratedKind, target.GeneratedKind),
			Source: source.RawType,
			Target: target.RawType,
		}
	}
	if normalizeGeneratedExpression(source.GeneratedExpression) != normalizeGeneratedExpression(target.GeneratedExpression) {
		return CompatibilityDecision{
			State:  CompatibilityUnsupported,
			Reason: fmt.Sprintf("generated column expression differs: source=%s target=%s", source.GeneratedExpression, target.GeneratedExpression),
			Source: source.GeneratedExpression,
			Target: target.GeneratedExpression,
		}
	}
	if source.CompressionAttr != target.CompressionAttr {
		return CompatibilityDecision{
			State:  CompatibilityWarnOnly,
			Reason: fmt.Sprintf("column compression attribute differs: source=%s target=%s", source.CompressionAttr, target.CompressionAttr),
			Source: source.RawType,
			Target: target.RawType,
		}
	}
	if strings.TrimSpace(source.SemanticWarning) != "" || strings.TrimSpace(target.SemanticWarning) != "" {
		reason := strings.TrimSpace(source.SemanticWarning)
		if reason == "" {
			reason = strings.TrimSpace(target.SemanticWarning)
		}
		return CompatibilityDecision{
			State:  CompatibilityWarnOnly,
			Reason: reason,
			Source: source.RawType,
			Target: target.RawType,
		}
	}

	if strings.EqualFold(normalizeWhitespace(source.RawType), normalizeWhitespace(target.RawType)) {
		return CompatibilityDecision{
			State:  CompatibilityEqual,
			Reason: "column definition matches exactly",
			Source: source.RawType,
			Target: target.RawType,
		}
	}

	return CompatibilityDecision{
		State:  CompatibilityNormalizedEqual,
		Reason: fmt.Sprintf("column definition differs textually but matches after normalization: source=%s target=%s", source.NormalizedType, target.NormalizedType),
		Source: source.RawType,
		Target: target.RawType,
	}
}

func normalizeGeneratedExpression(expr string) string {
	normalized := normalizeWhitespace(expr)
	for hasBalancedOuterParentheses(normalized) {
		normalized = normalizeWhitespace(normalized[1 : len(normalized)-1])
	}
	return normalized
}

func hasBalancedOuterParentheses(expr string) bool {
	if len(expr) < 2 || expr[0] != '(' || expr[len(expr)-1] != ')' {
		return false
	}
	depth := 0
	for i, r := range expr {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(expr)-1 {
				return false
			}
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0
}

func DecideColumnCharsetCompatibility(source, target CanonicalColumn) CompatibilityDecision {
	return decideNormalizedStringCompatibility("charset", source.Charset, target.Charset, source.NormalizedCharset, target.NormalizedCharset)
}

func DecideColumnCollationCompatibility(source, target CanonicalColumn) CompatibilityDecision {
	return decideCollationCompatibility(source.Collation, target.Collation, source.NormalizedCollation, target.NormalizedCollation)
}

func DecideColumnDefaultCompatibility(source, target CanonicalColumn) CompatibilityDecision {
	if source.DefaultValue == target.DefaultValue {
		return CompatibilityDecision{
			State:  CompatibilityEqual,
			Reason: "column default matches after normalization",
			Source: source.DefaultValue,
			Target: target.DefaultValue,
		}
	}

	return CompatibilityDecision{
		State:  CompatibilityUnsupported,
		Reason: fmt.Sprintf("column default differs after normalization: source=%s target=%s", source.DefaultValue, target.DefaultValue),
		Source: source.DefaultValue,
		Target: target.DefaultValue,
	}
}

func NormalizeTableRowFormat(v string) string {
	return strings.ToUpper(normalizeNullish(v))
}

func DecideCharsetCompatibility(sourceRaw, targetRaw string) CompatibilityDecision {
	return decideNormalizedStringCompatibility("charset", sourceRaw, targetRaw, normalizeCharset(sourceRaw), normalizeCharset(targetRaw))
}

func DecideCollationCompatibility(sourceRaw, targetRaw string) CompatibilityDecision {
	return decideCollationCompatibility(sourceRaw, targetRaw, normalizeCollation(sourceRaw), normalizeCollation(targetRaw))
}

func CanonicalizeMySQLTableOptions(rowFormat, createOptions, comment string) CanonicalTableOptions {
	createOptions = normalizeWhitespace(createOptions)
	explicit := rowFormatOptionRegexp.MatchString(createOptions)
	return CanonicalTableOptions{
		RowFormat:           normalizeNullish(rowFormat),
		NormalizedRowFormat: NormalizeTableRowFormat(rowFormat),
		CreateOptions:       createOptions,
		ExplicitRowFormat:   explicit,
		Comment:             normalizeNullish(comment),
	}
}

func DecideTableRowFormatCompatibility(source, target CanonicalTableOptions) CompatibilityDecision {
	if source.NormalizedRowFormat == target.NormalizedRowFormat {
		if strings.EqualFold(source.RowFormat, target.RowFormat) {
			return CompatibilityDecision{
				State:  CompatibilityEqual,
				Reason: "row format matches exactly",
				Source: source.RowFormat,
				Target: target.RowFormat,
			}
		}
		return CompatibilityDecision{
			State:  CompatibilityNormalizedEqual,
			Reason: fmt.Sprintf("row format differs textually but matches after normalization: source=%s target=%s", source.NormalizedRowFormat, target.NormalizedRowFormat),
			Source: source.RowFormat,
			Target: target.RowFormat,
		}
	}

	if !source.ExplicitRowFormat && !target.ExplicitRowFormat {
		pair := map[string]map[string]struct{}{
			"COMPACT": {"DYNAMIC": {}},
			"DYNAMIC": {"COMPACT": {}},
		}
		if _, ok := pair[source.NormalizedRowFormat][target.NormalizedRowFormat]; ok {
			return CompatibilityDecision{
				State:  CompatibilityNormalizedEqual,
				Reason: fmt.Sprintf("row format differs (%s vs %s) but both sides use implicit defaults", source.NormalizedRowFormat, target.NormalizedRowFormat),
				Source: source.RowFormat,
				Target: target.RowFormat,
			}
		}
	}

	return CompatibilityDecision{
		State:  CompatibilityUnsupported,
		Reason: fmt.Sprintf("row format differs after normalization: source=%s target=%s", source.NormalizedRowFormat, target.NormalizedRowFormat),
		Source: source.RowFormat,
		Target: target.RowFormat,
	}
}

func normalizeConstraintDefinition(v string) string {
	s := normalizeWhitespace(v)
	s = strings.TrimSuffix(s, ",")
	return strings.TrimSpace(s)
}

func normalizeCheckConstraintDefinition(v string) string {
	definition := normalizeConstraintDefinition(v)
	if definition == "" {
		return definition
	}

	expr := definition
	for hasBalancedOuterParentheses(expr) {
		expr = normalizeWhitespace(expr[1 : len(expr)-1])
	}
	if expr == "" {
		return definition
	}
	return "(" + expr + ")"
}

func ExtractCheckConstraintsFromCreateSQL(createSQL string) []CanonicalConstraint {
	lines := strings.Split(createSQL, "\n")
	checks := make([]CanonicalConstraint, 0)
	anonymousSeq := 1

	for _, line := range lines {
		trimmed := normalizeConstraintDefinition(line)
		upper := strings.ToUpper(trimmed)
		if !strings.Contains(upper, "CHECK") {
			continue
		}

		name := ""
		definition := ""
		if matches := checkWithNameRegexp.FindStringSubmatch(trimmed); len(matches) == 3 {
			name = matches[1]
			definition = matches[2]
		} else if matches := checkNoNameRegexp.FindStringSubmatch(trimmed); len(matches) == 2 {
			name = fmt.Sprintf("CHECK_%d", anonymousSeq)
			definition = matches[1]
			anonymousSeq++
		} else {
			continue
		}

		checks = append(checks, CanonicalConstraint{
			Name:                 name,
			Kind:                 "CHECK",
			Definition:           definition,
			NormalizedDefinition: normalizeCheckConstraintDefinition(definition),
		})
	}

	sort.Slice(checks, func(i, j int) bool {
		if checks[i].Name == checks[j].Name {
			return checks[i].NormalizedDefinition < checks[j].NormalizedDefinition
		}
		return checks[i].Name < checks[j].Name
	})
	return checks
}

func DecideCheckConstraintCompatibility(source, target []CanonicalConstraint, sourceCatalog, targetCatalog SchemaFeatureCatalog) CompatibilityDecision {
	if len(source) == 0 && len(target) == 0 {
		return CompatibilityDecision{
			State:  CompatibilityEqual,
			Reason: "no check constraints on either side",
		}
	}

	sourceDefs := make([]string, 0, len(source))
	targetDefs := make([]string, 0, len(target))
	for _, item := range source {
		sourceDefs = append(sourceDefs, item.NormalizedDefinition)
	}
	for _, item := range target {
		targetDefs = append(targetDefs, item.NormalizedDefinition)
	}

	if strings.Join(sourceDefs, "|") != strings.Join(targetDefs, "|") {
		return CompatibilityDecision{
			State:  CompatibilityWarnOnly,
			Reason: fmt.Sprintf("check constraint definitions differ: source=%v target=%v", sourceDefs, targetDefs),
			Source: strings.Join(sourceDefs, "|"),
			Target: strings.Join(targetDefs, "|"),
		}
	}

	if len(source) > 0 && !sourceCatalog.EnforcesCheckConstraints && targetCatalog.EnforcesCheckConstraints {
		return CompatibilityDecision{
			State:  CompatibilityWarnOnly,
			Reason: "check constraints match textually but target enforces them while source does not",
			Source: strings.Join(sourceDefs, "|"),
			Target: strings.Join(targetDefs, "|"),
		}
	}

	return CompatibilityDecision{
		State:  CompatibilityEqual,
		Reason: "check constraints match exactly",
		Source: strings.Join(sourceDefs, "|"),
		Target: strings.Join(targetDefs, "|"),
	}
}

func CanonicalizeMySQLIndexes(indexMap map[string][]string, visibilityMap map[string]string) []CanonicalIndex {
	indexes := make([]CanonicalIndex, 0, len(indexMap))
	names := make([]string, 0, len(indexMap))
	for name := range indexMap {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		type indexColumn struct {
			name   string
			expr   string // 非空时为函数索引表达式（已规范化为小写）
			seq    int
			prefix int
		}

		parsedCols := make([]indexColumn, 0, len(indexMap[name]))
		for _, token := range indexMap[name] {
			colName := token
			expr := ""
			seq := 0
			prefix := 0
			// 检测函数索引 token（格式：/*expr*/EXPRESSION/*seq*/N/*type*//*prefix*/0）
			if strings.HasPrefix(token, "/*expr*/") {
				rest := strings.TrimPrefix(token, "/*expr*/")
				if seqIdx := strings.Index(rest, "/*seq*/"); seqIdx >= 0 {
					expr = strings.ToLower(strings.TrimSpace(rest[:seqIdx]))
					seqRest := rest[seqIdx+len("/*seq*/"):]
					if typeIdx := strings.Index(seqRest, "/*type*/"); typeIdx >= 0 {
						if parsed, err := strconv.Atoi(strings.TrimSpace(seqRest[:typeIdx])); err == nil {
							seq = parsed
						}
					} else {
						if parsed, err := strconv.Atoi(strings.TrimSpace(seqRest)); err == nil {
							seq = parsed
						}
					}
				} else {
					expr = strings.ToLower(strings.TrimSpace(rest))
				}
				colName = "" // 函数索引无对应列名
			} else if parts := strings.Split(token, "/*seq*/"); len(parts) == 2 {
				colName = strings.TrimSpace(parts[0])
				rest := parts[1]
				typeParts := strings.Split(rest, "/*type*/")
				seqPart := typeParts[0]
				if parsed, err := strconv.Atoi(seqPart); err == nil {
					seq = parsed
				}
				// 解析前缀长度；旧 token 无 /*prefix*/ 时向后兼容回退为 0
				if len(typeParts) == 2 {
					prefixParts := strings.Split(typeParts[1], "/*prefix*/")
					if len(prefixParts) == 2 {
						if parsed, err := strconv.Atoi(strings.TrimSpace(prefixParts[1])); err == nil {
							prefix = parsed
						}
					}
				}
			}
			parsedCols = append(parsedCols, indexColumn{name: colName, expr: expr, seq: seq, prefix: prefix})
		}

		sort.Slice(parsedCols, func(i, j int) bool {
			if parsedCols[i].seq == parsedCols[j].seq {
				return parsedCols[i].name < parsedCols[j].name
			}
			return parsedCols[i].seq < parsedCols[j].seq
		})

		cols := make([]string, 0, len(parsedCols))
		prefixes := make([]int, 0, len(parsedCols))
		exprs := make([]string, 0, len(parsedCols))
		for _, col := range parsedCols {
			// MySQL column identifiers are not case-sensitive, so keep index
			// semantics stable when metadata formatting differs only by case.
			cols = append(cols, strings.ToLower(col.name))
			prefixes = append(prefixes, col.prefix)
			exprs = append(exprs, col.expr)
		}

		visibility := IndexVisibilityVisible
		if strings.EqualFold(visibilityMap[name], "INVISIBLE") || strings.EqualFold(visibilityMap[name], "IGNORED") || strings.EqualFold(visibilityMap[name], "NO") {
			visibility = IndexVisibilityInvisible
		}

		indexes = append(indexes, CanonicalIndex{
			Name:                  name,
			Columns:               cols,
			PrefixLength:          prefixes,
			NormalizedExpressions: exprs,
			Visibility:            visibility,
		})
	}

	return indexes
}

func CanonicalizePrimaryKeyConstraints(indexes []CanonicalIndex) []CanonicalConstraint {
	constraints := make([]CanonicalConstraint, 0, len(indexes))
	for _, idx := range indexes {
		name := strings.TrimSpace(idx.Name)
		if name == "" {
			name = "PRIMARY"
		}
		definition := buildKeyConstraintDefinition("PRIMARY KEY", "PRIMARY", idx.Columns)
		constraints = append(constraints, CanonicalConstraint{
			Name:                 name,
			Kind:                 "PRIMARY KEY",
			Definition:           definition,
			NormalizedDefinition: definition,
			Columns:              append([]string(nil), idx.Columns...),
		})
	}
	return constraints
}

func CanonicalizeUniqueConstraints(indexes []CanonicalIndex) []CanonicalConstraint {
	constraints := make([]CanonicalConstraint, 0, len(indexes))
	for _, idx := range indexes {
		definition := buildKeyConstraintDefinition("UNIQUE", idx.Name, idx.Columns)
		constraints = append(constraints, CanonicalConstraint{
			Name:                 idx.Name,
			Kind:                 "UNIQUE",
			Definition:           definition,
			NormalizedDefinition: definition,
			Columns:              append([]string(nil), idx.Columns...),
		})
	}
	sort.Slice(constraints, func(i, j int) bool {
		return strings.ToUpper(constraints[i].Name) < strings.ToUpper(constraints[j].Name)
	})
	return constraints
}

func CanonicalizeForeignKeyDefinitions(defs map[string]string) []CanonicalConstraint {
	items := make([]CanonicalConstraint, 0, len(defs))
	names := make([]string, 0, len(defs))
	for name := range defs {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, key := range names {
		definition := normalizeConstraintDefinition(defs[key])
		name := key
		columns := []string{}
		referencedSchema := ""
		referencedTable := ""
		referencedColumns := []string{}
		deleteRule := ""
		updateRule := ""
		if matches := fkNameRegexp.FindStringSubmatch(definition); len(matches) == 2 {
			name = matches[1]
		}
		if matches := fkDefinitionRegexp.FindStringSubmatch(definition); len(matches) == 5 {
			columns = parseWrappedIdentifierList(matches[1])
			referencedSchema = strings.TrimSpace(matches[2])
			referencedTable = strings.TrimSpace(matches[3])
			referencedColumns = parseWrappedIdentifierList(matches[4])
		}
		if matches := fkDeleteRuleRegexp.FindStringSubmatch(definition); len(matches) == 2 {
			deleteRule = strings.ToUpper(normalizeWhitespace(matches[1]))
		}
		if matches := fkUpdateRuleRegexp.FindStringSubmatch(definition); len(matches) == 2 {
			updateRule = strings.ToUpper(normalizeWhitespace(matches[1]))
		}
		normalizedColumns := normalizeIdentifierList(columns)
		normalizedReferencedColumns := normalizeIdentifierList(referencedColumns)
		normalizedDefinition := buildForeignKeyConstraintDefinition(
			strings.ToUpper(name),
			referencedSchema,
			referencedTable,
			normalizedColumns,
			normalizedReferencedColumns,
			deleteRule,
			updateRule,
		)
		items = append(items, CanonicalConstraint{
			Name:                 strings.ToUpper(name),
			Kind:                 "FOREIGN KEY",
			Definition:           defs[key],
			NormalizedDefinition: normalizedDefinition,
			Columns:              normalizedColumns,
			ReferencedSchema:     referencedSchema,
			ReferencedTable:      referencedTable,
			ReferencedColumns:    normalizedReferencedColumns,
			DeleteRule:           deleteRule,
			UpdateRule:           updateRule,
		})
	}
	return items
}

// indexHasExpr 判断 CanonicalIndex 中是否包含函数索引列（NormalizedExpressions 非全空）。
func indexHasExpr(idx CanonicalIndex) bool {
	for _, e := range idx.NormalizedExpressions {
		if e != "" {
			return true
		}
	}
	return false
}

// indexElemKey 返回第 i 个位置的比较键：函数列返回规范化表达式，普通列返回列名。
func indexElemKey(idx CanonicalIndex, i int) string {
	if i < len(idx.NormalizedExpressions) && idx.NormalizedExpressions[i] != "" {
		return idx.NormalizedExpressions[i]
	}
	if i < len(idx.Columns) {
		return idx.Columns[i]
	}
	return ""
}

func DecideIndexCompatibility(source, target CanonicalIndex) CompatibilityDecision {
	// 若任一侧含函数索引列，则按位置逐元素比较（表达式 vs 表达式，或列名 vs 列名）
	if indexHasExpr(source) || indexHasExpr(target) {
		if len(source.Columns) != len(target.Columns) {
			return CompatibilityDecision{
				State:  CompatibilityUnsupported,
				Reason: fmt.Sprintf("index column count differs: source=%d target=%d", len(source.Columns), len(target.Columns)),
				Source: strings.Join(source.Columns, ","),
				Target: strings.Join(target.Columns, ","),
			}
		}
		for i := range source.Columns {
			srcKey := indexElemKey(source, i)
			dstKey := indexElemKey(target, i)
			if srcKey != dstKey {
				return CompatibilityDecision{
					State:  CompatibilityUnsupported,
					Reason: fmt.Sprintf("index element[%d] differs: source=%q target=%q", i, srcKey, dstKey),
					Source: srcKey,
					Target: dstKey,
				}
			}
		}
		// 列匹配后继续检查可见性
		if source.Visibility != target.Visibility {
			return CompatibilityDecision{
				State:  CompatibilityUnsupported,
				Reason: fmt.Sprintf("index visibility differs: source=%s target=%s", source.Visibility, target.Visibility),
				Source: string(source.Visibility),
				Target: string(target.Visibility),
			}
		}
		return CompatibilityDecision{
			State:  CompatibilityEqual,
			Reason: "index definition matches exactly",
			Source: strings.Join(source.Columns, ","),
			Target: strings.Join(target.Columns, ","),
		}
	}

	if strings.Join(source.Columns, ",") != strings.Join(target.Columns, ",") {
		return CompatibilityDecision{
			State:  CompatibilityUnsupported,
			Reason: fmt.Sprintf("index columns differ: source=%v target=%v", source.Columns, target.Columns),
			Source: strings.Join(source.Columns, ","),
			Target: strings.Join(target.Columns, ","),
		}
	}
	if len(source.PrefixLength) != len(target.PrefixLength) {
		return CompatibilityDecision{
			State:  CompatibilityUnsupported,
			Reason: fmt.Sprintf("index prefix length count differs: source=%v target=%v", source.PrefixLength, target.PrefixLength),
			Source: fmt.Sprintf("%v", source.PrefixLength),
			Target: fmt.Sprintf("%v", target.PrefixLength),
		}
	}
	for i := range source.PrefixLength {
		if source.PrefixLength[i] != target.PrefixLength[i] {
			return CompatibilityDecision{
				State:  CompatibilityUnsupported,
				Reason: fmt.Sprintf("index prefix length differs: source=%v target=%v", source.PrefixLength, target.PrefixLength),
				Source: fmt.Sprintf("%v", source.PrefixLength),
				Target: fmt.Sprintf("%v", target.PrefixLength),
			}
		}
	}
	if source.Visibility != target.Visibility {
		return CompatibilityDecision{
			State:  CompatibilityUnsupported,
			Reason: fmt.Sprintf("index visibility differs: source=%s target=%s", source.Visibility, target.Visibility),
			Source: string(source.Visibility),
			Target: string(target.Visibility),
		}
	}
	return CompatibilityDecision{
		State:  CompatibilityEqual,
		Reason: "index definition matches exactly",
		Source: strings.Join(source.Columns, ","),
		Target: strings.Join(target.Columns, ","),
	}
}

func DecideKeyConstraintCompatibility(source, target CanonicalConstraint) CompatibilityDecision {
	if source.Kind != target.Kind {
		return CompatibilityDecision{
			State:  CompatibilityUnsupported,
			Reason: fmt.Sprintf("constraint kind differs: source=%s target=%s", source.Kind, target.Kind),
			Source: source.Kind,
			Target: target.Kind,
		}
	}
	if strings.Join(source.Columns, ",") != strings.Join(target.Columns, ",") {
		return CompatibilityDecision{
			State:  CompatibilityUnsupported,
			Reason: fmt.Sprintf("%s columns differ: source=%v target=%v", source.Kind, source.Columns, target.Columns),
			Source: strings.Join(source.Columns, ","),
			Target: strings.Join(target.Columns, ","),
		}
	}
	if source.Kind == "UNIQUE" && !strings.EqualFold(source.Name, target.Name) {
		return CompatibilityDecision{
			State:  CompatibilityUnsupported,
			Reason: fmt.Sprintf("unique constraint name differs: source=%s target=%s", source.Name, target.Name),
			Source: source.Name,
			Target: target.Name,
		}
	}
	if source.NormalizedDefinition == target.NormalizedDefinition {
		return CompatibilityDecision{
			State:  CompatibilityEqual,
			Reason: fmt.Sprintf("%s definition matches exactly", source.Kind),
			Source: source.NormalizedDefinition,
			Target: target.NormalizedDefinition,
		}
	}
	return CompatibilityDecision{
		State:  CompatibilityNormalizedEqual,
		Reason: fmt.Sprintf("%s differs textually but matches on canonical key semantics", source.Kind),
		Source: source.NormalizedDefinition,
		Target: target.NormalizedDefinition,
	}
}

func DecideForeignKeyCompatibility(source, target CanonicalConstraint) CompatibilityDecision {
	if source.NormalizedDefinition != target.NormalizedDefinition {
		return CompatibilityDecision{
			State:  CompatibilityUnsupported,
			Reason: fmt.Sprintf("foreign key definition differs: source=%s target=%s", source.NormalizedDefinition, target.NormalizedDefinition),
			Source: source.NormalizedDefinition,
			Target: target.NormalizedDefinition,
		}
	}
	return CompatibilityDecision{
		State:  CompatibilityEqual,
		Reason: "foreign key definition matches exactly",
		Source: source.NormalizedDefinition,
		Target: target.NormalizedDefinition,
	}
}

func buildKeyConstraintDefinition(kind, name string, columns []string) string {
	switch kind {
	case "PRIMARY KEY":
		return fmt.Sprintf("PRIMARY KEY(%s)", strings.Join(columns, ","))
	case "UNIQUE":
		return fmt.Sprintf("UNIQUE %s(%s)", strings.ToUpper(strings.TrimSpace(name)), strings.Join(columns, ","))
	default:
		return fmt.Sprintf("%s(%s)", strings.TrimSpace(kind), strings.Join(columns, ","))
	}
}

func parseWrappedIdentifierList(segment string) []string {
	parts := strings.Split(segment, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		item = strings.Trim(item, "!")
		item = strings.Trim(item, "`")
		item = strings.TrimSpace(item)
		if item != "" {
			items = append(items, item)
		}
	}
	return items
}

func normalizeIdentifierList(items []string) []string {
	normalized := make([]string, 0, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		normalized = append(normalized, strings.ToLower(trimmed))
	}
	return normalized
}

func buildForeignKeyConstraintDefinition(name, referencedSchema, referencedTable string, columns, referencedColumns []string, deleteRule, updateRule string) string {
	definition := fmt.Sprintf(
		"FOREIGN KEY %s(%s) REFERENCES %s.%s(%s)",
		strings.ToUpper(strings.TrimSpace(name)),
		strings.Join(columns, ","),
		strings.TrimSpace(referencedSchema),
		strings.TrimSpace(referencedTable),
		strings.Join(referencedColumns, ","),
	)
	if deleteRule != "" {
		definition += " ON DELETE " + strings.ToUpper(strings.TrimSpace(deleteRule))
	}
	if updateRule != "" {
		definition += " ON UPDATE " + strings.ToUpper(strings.TrimSpace(updateRule))
	}
	return definition
}

func decideCollationCompatibility(sourceRaw, targetRaw, sourceNormalized, targetNormalized string) CompatibilityDecision {
	if sourceNormalized == targetNormalized {
		return decideNormalizedStringCompatibility("collation", sourceRaw, targetRaw, sourceNormalized, targetNormalized)
	}
	if IsUTF8MB4DefaultCollationDrift(sourceNormalized, targetNormalized) {
		return CompatibilityDecision{
			State:  CompatibilityWarnOnly,
			Reason: fmt.Sprintf("utf8mb4 default collation drift detected between legacy and MySQL 8.x defaults: source=%s target=%s", sourceNormalized, targetNormalized),
			Source: sourceRaw,
			Target: targetRaw,
		}
	}
	return CompatibilityDecision{
		State:  CompatibilityUnsupported,
		Reason: fmt.Sprintf("collation differs after normalization: source=%s target=%s", sourceNormalized, targetNormalized),
		Source: sourceRaw,
		Target: targetRaw,
	}
}

func IsUTF8MB4DefaultCollationDrift(sourceNormalized, targetNormalized string) bool {
	// Static pairs: known default collation upgrades between MySQL/MariaDB versions.
	pairs := map[string]map[string]struct{}{
		"utf8mb4_general_ci": {"utf8mb4_0900_ai_ci": {}},
		"utf8mb4_0900_ai_ci": {"utf8mb4_general_ci": {}},
	}
	if _, ok := pairs[sourceNormalized][targetNormalized]; ok {
		return true
	}

	// Dynamic check: MariaDB UCA 14.0.0 ↔ MySQL UCA 9.0.0 with matching sensitivity.
	// Covers MariaDB 11.2+/12.x default utf8mb4_uca1400_ai_ci and all sensitivity variants.
	srcMapped, srcIsUCA1400 := MapMariaDBCollationToMySQL(sourceNormalized)
	if srcIsUCA1400 && srcMapped == targetNormalized {
		return true
	}
	dstMapped, dstIsUCA1400 := MapMariaDBCollationToMySQL(targetNormalized)
	if dstIsUCA1400 && dstMapped == sourceNormalized {
		return true
	}
	return false
}

// MapMariaDBCollationToMySQL maps a MariaDB UCA 14.0.0 collation to the closest
// MySQL UCA 9.0.0 equivalent by preserving the charset prefix and sensitivity
// suffix (ai_ci, as_ci, as_cs, ai_cs). Locale variants are folded into the base
// MySQL collation. Returns ("", false) if the input is not a UCA 14.0.0 collation.
func MapMariaDBCollationToMySQL(collation string) (string, bool) {
	lower := strings.ToLower(strings.TrimSpace(collation))
	if !strings.Contains(lower, "_uca1400_") {
		return "", false
	}

	parts := strings.SplitN(lower, "_uca1400_", 2)
	if len(parts) != 2 {
		return "", false
	}
	charset := parts[0]
	suffix := parts[1]

	sensitivities := []string{"ai_ci", "as_ci", "as_cs", "ai_cs"}

	// Base collation (no locale): e.g. utf8mb4_uca1400_ai_ci → utf8mb4_0900_ai_ci
	for _, sens := range sensitivities {
		if suffix == sens {
			return charset + "_0900_" + sens, true
		}
	}
	// Locale variant: e.g. utf8mb4_uca1400_swedish_ai_ci → utf8mb4_0900_ai_ci
	for _, sens := range sensitivities {
		if strings.HasSuffix(suffix, "_"+sens) {
			return charset + "_0900_" + sens, true
		}
	}

	return "", false
}

// InferCharsetFromCollation extracts the character set name from a collation
// name by matching known charset prefixes. Returns empty string on failure.
func InferCharsetFromCollation(collation string) string {
	lower := strings.ToLower(strings.TrimSpace(collation))
	if lower == "" || lower == "null" {
		return ""
	}
	for _, prefix := range []string{
		"utf8mb4_", "utf8mb3_", "utf8_", "latin1_", "ascii_",
		"utf16_", "utf32_", "ucs2_", "gbk_", "gb2312_", "big5_",
	} {
		if strings.HasPrefix(lower, prefix) {
			return strings.TrimSuffix(prefix, "_")
		}
	}
	if idx := strings.Index(lower, "_"); idx > 0 {
		return lower[:idx]
	}
	return ""
}

func decideNormalizedStringCompatibility(label, sourceRaw, targetRaw, sourceNormalized, targetNormalized string) CompatibilityDecision {
	if sourceNormalized != targetNormalized {
		return CompatibilityDecision{
			State:  CompatibilityUnsupported,
			Reason: fmt.Sprintf("%s differs after normalization: source=%s target=%s", label, sourceNormalized, targetNormalized),
			Source: sourceRaw,
			Target: targetRaw,
		}
	}

	if strings.EqualFold(sourceRaw, targetRaw) {
		return CompatibilityDecision{
			State:  CompatibilityEqual,
			Reason: fmt.Sprintf("%s matches exactly", label),
			Source: sourceRaw,
			Target: targetRaw,
		}
	}

	return CompatibilityDecision{
		State:  CompatibilityNormalizedEqual,
		Reason: fmt.Sprintf("%s differs textually but matches after normalization: source=%s target=%s", label, sourceNormalized, targetNormalized),
		Source: sourceRaw,
		Target: targetRaw,
	}
}
