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
	whitespaceRegexp         = regexp.MustCompile(`\s+`)
	integerDisplayWidthRegex = regexp.MustCompile(`\b(tinyint|smallint|mediumint|int|integer|bigint)\s*\(\s*\d+\s*\)`)
	yearDisplayWidthRegex    = regexp.MustCompile(`\byear\s*\(\s*4\s*\)`)
	rowFormatOptionRegexp    = regexp.MustCompile(`(?i)\brow_format\s*=\s*([a-z_]+)`)
	checkWithNameRegexp      = regexp.MustCompile("(?i)constraint\\s+`?([^`\\s]+)`?\\s+check\\s*(\\(.*\\))")
	checkNoNameRegexp        = regexp.MustCompile(`(?i)\bcheck\s*(\(.+\))`)
	fkNameRegexp             = regexp.MustCompile("(?i)constraint\\s+!([^!]+)!\\s+foreign\\s+key")
	fkDefinitionRegexp       = regexp.MustCompile(`(?i)foreign\s+key\s*\((.*?)\)\s*references\s*!([^!]+)!\s*\.\s*!([^!]+)!\s*\((.*?)\)`)
	fkDeleteRuleRegexp       = regexp.MustCompile(`(?i)\bon\s+delete\s+(cascade|restrict|set null|no action|set default)`)
	fkUpdateRuleRegexp       = regexp.MustCompile(`(?i)\bon\s+update\s+(cascade|restrict|set null|no action|set default)`)
	mariadbCompressedComment = regexp.MustCompile(`(?i)/\*m!\d+\s+compressed\s*\*/`)
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

func normalizeMySQLColumnType(raw string) (string, string, ColumnVisibility, bool, string) {
	s := strings.ToLower(normalizeWhitespace(raw))
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
	// INFORMATION_SCHEMA.EXTRA in MySQL 8.0+ may add DEFAULT_GENERATED for
	// TIMESTAMP/DATETIME defaults. This is metadata noise, not a type change.
	if strings.Contains(s, " default_generated") || strings.HasSuffix(s, "default_generated") {
		s = strings.ReplaceAll(s, " default_generated", " ")
		s = strings.TrimSuffix(s, "default_generated")
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
		DefaultValue:        normalizeNullish(getAttr(4)),
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
			seq    int
			prefix int
		}

		parsedCols := make([]indexColumn, 0, len(indexMap[name]))
		for _, token := range indexMap[name] {
			colName := token
			seq := 0
			if parts := strings.Split(token, "/*seq*/"); len(parts) == 2 {
				colName = strings.TrimSpace(parts[0])
				seqPart := strings.Split(parts[1], "/*type*/")[0]
				if parsed, err := strconv.Atoi(seqPart); err == nil {
					seq = parsed
				}
			}
			parsedCols = append(parsedCols, indexColumn{name: colName, seq: seq})
		}

		sort.Slice(parsedCols, func(i, j int) bool {
			if parsedCols[i].seq == parsedCols[j].seq {
				return parsedCols[i].name < parsedCols[j].name
			}
			return parsedCols[i].seq < parsedCols[j].seq
		})

		cols := make([]string, 0, len(parsedCols))
		prefixes := make([]int, 0, len(parsedCols))
		for _, col := range parsedCols {
			// MySQL column identifiers are not case-sensitive, so keep index
			// semantics stable when metadata formatting differs only by case.
			cols = append(cols, strings.ToLower(col.name))
			prefixes = append(prefixes, col.prefix)
		}

		visibility := IndexVisibilityVisible
		if strings.EqualFold(visibilityMap[name], "INVISIBLE") || strings.EqualFold(visibilityMap[name], "IGNORED") || strings.EqualFold(visibilityMap[name], "NO") {
			visibility = IndexVisibilityInvisible
		}

		indexes = append(indexes, CanonicalIndex{
			Name:         name,
			Columns:      cols,
			PrefixLength: prefixes,
			Visibility:   visibility,
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

func DecideIndexCompatibility(source, target CanonicalIndex) CompatibilityDecision {
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
	if isUTF8MB4DefaultCollationDrift(sourceNormalized, targetNormalized) {
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

func isUTF8MB4DefaultCollationDrift(sourceNormalized, targetNormalized string) bool {
	pairs := map[string]map[string]struct{}{
		"utf8mb4_general_ci": {"utf8mb4_0900_ai_ci": {}},
		"utf8mb4_0900_ai_ci": {"utf8mb4_general_ci": {}},
	}
	_, ok := pairs[sourceNormalized][targetNormalized]
	return ok
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
