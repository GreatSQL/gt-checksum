package schemacompat

import (
	"fmt"
	"gt-checksum/global"
	"regexp"
	"strconv"
	"strings"
)

// CrossDBRoutineCompareStrategy defines the comparison strategy for
// cross-database routine/trigger objects (reserved for future use).
type CrossDBRoutineCompareStrategy string

const (
	RoutineCompareExistence CrossDBRoutineCompareStrategy = "existence"
	RoutineCompareSemantic  CrossDBRoutineCompareStrategy = "semantic"
)

var (
	oracleNumberPrecisionRegexp    = regexp.MustCompile(`(?i)^NUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$`)
	oracleNumberNoPrecisionRegexp  = regexp.MustCompile(`(?i)^NUMBER\s*$`)
	oracleVarchar2Regexp           = regexp.MustCompile(`(?i)^VARCHAR2\s*\(\s*(\d+)\s*\)$`)
	oracleCharRegexp               = regexp.MustCompile(`(?i)^CHAR\s*\(\s*(\d+)\s*\)$`)
	oracleNCharRegexp              = regexp.MustCompile(`(?i)^NCHAR\s*\(\s*(\d+)\s*\)$`)
	oracleNVarchar2Regexp          = regexp.MustCompile(`(?i)^NVARCHAR2\s*\(\s*(\d+)\s*\)$`)
	oracleRawRegexp                = regexp.MustCompile(`(?i)^RAW\s*\(\s*(\d+)\s*\)$`)
	oracleFloatRegexp              = regexp.MustCompile(`(?i)^FLOAT\s*(?:\(\s*(\d+)\s*\))?$`)
	oracleTimestampRegexp          = regexp.MustCompile(`(?i)^TIMESTAMP\s*\(\s*(\d+)\s*\)$`)
	oracleTimestampTZRegexp        = regexp.MustCompile(`(?i)^TIMESTAMP\s*\(\s*(\d+)\s*\)\s+WITH\s+(?:LOCAL\s+)?TIME\s+ZONE$`)
	oracleTimestampPlainRegexp     = regexp.MustCompile(`(?i)^TIMESTAMP$`)
	oracleTimestampPlainTZRegexp   = regexp.MustCompile(`(?i)^TIMESTAMP\s+WITH\s+(?:LOCAL\s+)?TIME\s+ZONE$`)
	oracleIntervalYMRegexp         = regexp.MustCompile(`(?i)^INTERVAL\s+YEAR.*TO\s+MONTH$`)
	oracleIntervalDSRegexp         = regexp.MustCompile(`(?i)^INTERVAL\s+DAY.*TO\s+SECOND.*$`)
	oracleNumberIntRegexp          = regexp.MustCompile(`(?i)^NUMBER\s*\(\s*(\d+)\s*\)$`)
	oracleDefaultSysdateRegexp     = regexp.MustCompile(`(?i)^\s*SYSDATE\s*$`)
	oracleDefaultSystimestampRegex = regexp.MustCompile(`(?i)^\s*SYSTIMESTAMP\s*$`)
	oracleDefaultSeqNextvalRegexp  = regexp.MustCompile(`(?i)\.\s*NEXTVAL\s*$`)
)

// NormalizeOracleColumnType maps an Oracle column type string to its MySQL equivalent.
func NormalizeOracleColumnType(oracleType string) string {
	trimmed := strings.TrimSpace(oracleType)
	upper := strings.ToUpper(trimmed)

	// VARCHAR2(n) → varchar(n)
	if m := oracleVarchar2Regexp.FindStringSubmatch(trimmed); len(m) == 2 {
		return fmt.Sprintf("varchar(%s)", m[1])
	}

	// NVARCHAR2(n) → varchar(n) (character length, already in chars from CHAR_LENGTH)
	if m := oracleNVarchar2Regexp.FindStringSubmatch(trimmed); len(m) == 2 {
		return fmt.Sprintf("varchar(%s)", m[1])
	}

	// CHAR(n) → char(n)
	if m := oracleCharRegexp.FindStringSubmatch(trimmed); len(m) == 2 {
		return fmt.Sprintf("char(%s)", m[1])
	}

	// NCHAR(n) → char(n) (character length)
	if m := oracleNCharRegexp.FindStringSubmatch(trimmed); len(m) == 2 {
		return fmt.Sprintf("char(%s)", m[1])
	}

	// NUMBER(p,s) with s > 0 → decimal(p,s)
	if m := oracleNumberPrecisionRegexp.FindStringSubmatch(trimmed); len(m) == 3 {
		p, _ := strconv.Atoi(m[1])
		s, _ := strconv.Atoi(m[2])
		if s > 0 {
			return fmt.Sprintf("decimal(%d,%d)", p, s)
		}
		// NUMBER(p,0) → integer type based on precision
		return mapOracleIntegerPrecision(p)
	}

	// NUMBER(p) → integer type (same as NUMBER(p,0))
	if m := oracleNumberIntRegexp.FindStringSubmatch(trimmed); len(m) == 2 {
		p, _ := strconv.Atoi(m[1])
		return mapOracleIntegerPrecision(p)
	}

	// NUMBER (no precision) → decimal(38,0)
	if oracleNumberNoPrecisionRegexp.MatchString(trimmed) {
		return "decimal(38,0)"
	}

	// RAW(n) → varbinary(n)
	if m := oracleRawRegexp.FindStringSubmatch(trimmed); len(m) == 2 {
		return fmt.Sprintf("varbinary(%s)", m[1])
	}

	// FLOAT / FLOAT(p) → double
	if oracleFloatRegexp.MatchString(trimmed) {
		return "double"
	}

	// TIMESTAMP(n) → datetime(n)
	if m := oracleTimestampRegexp.FindStringSubmatch(trimmed); len(m) == 2 {
		frac := m[1]
		if frac == "0" {
			return "datetime"
		}
		return fmt.Sprintf("datetime(%s)", frac)
	}

	// TIMESTAMP (plain, no precision) → datetime(6) (Oracle default is 6)
	if oracleTimestampPlainRegexp.MatchString(trimmed) {
		return "datetime(6)"
	}

	// TIMESTAMP WITH [LOCAL] TIME ZONE(n) → datetime(n) (timezone info lost)
	if m := oracleTimestampTZRegexp.FindStringSubmatch(trimmed); len(m) == 2 {
		frac := m[1]
		if frac == "0" {
			return "datetime"
		}
		return fmt.Sprintf("datetime(%s)", frac)
	}

	// TIMESTAMP WITH [LOCAL] TIME ZONE (plain) → datetime(6)
	if oracleTimestampPlainTZRegexp.MatchString(trimmed) {
		return "datetime(6)"
	}

	// INTERVAL YEAR TO MONTH → varchar(30)
	if oracleIntervalYMRegexp.MatchString(trimmed) {
		return "varchar(30)"
	}

	// INTERVAL DAY TO SECOND → varchar(30)
	if oracleIntervalDSRegexp.MatchString(trimmed) {
		return "varchar(30)"
	}

	// Simple keyword-based mappings
	switch upper {
	case "DATE":
		return "datetime"
	case "CLOB", "NCLOB", "LONG":
		return "longtext"
	case "BLOB":
		return "longblob"
	case "LONG RAW":
		return "longblob"
	case "BINARY_FLOAT":
		// Oracle BINARY_FLOAT is a 32-bit IEEE754 single-precision float.
		// Map to MySQL `double` (not `float`) to avoid the same single-precision
		// comparison/aggregation accuracy issues already addressed elsewhere for
		// bare FLOAT columns. Users who need strict 32-bit semantics can still
		// explicitly define FLOAT(24) in MySQL.
		return "double"
	case "BINARY_DOUBLE":
		return "double"
	case "XMLTYPE":
		return "longtext"
	case "ROWID":
		return "varchar(18)"
	case "DECIMAL":
		return "decimal"
	case "BOOLEAN":
		return "tinyint(1)"
	}

	// UROWID(n) → varchar(n)
	if strings.HasPrefix(upper, "UROWID") {
		if n := extractParenNumber(trimmed); n != "" {
			return fmt.Sprintf("varchar(%s)", n)
		}
		return "varchar(4000)"
	}

	// Fallback: lowercase the type as-is
	return strings.ToLower(trimmed)
}

// mapOracleIntegerPrecision maps NUMBER(p,0) to the appropriate MySQL integer type.
func mapOracleIntegerPrecision(p int) string {
	switch {
	case p <= 2:
		if p == 1 {
			return "tinyint(1)"
		}
		return "tinyint"
	case p <= 4:
		return "smallint"
	case p <= 6:
		return "mediumint"
	case p <= 9:
		return "int"
	case p <= 18:
		return "bigint"
	default:
		return fmt.Sprintf("decimal(%d,0)", p)
	}
}

// extractParenNumber extracts the numeric value from "TYPE(N)" patterns.
func extractParenNumber(s string) string {
	start := strings.Index(s, "(")
	end := strings.Index(s, ")")
	if start < 0 || end < 0 || end <= start+1 {
		return ""
	}
	return strings.TrimSpace(s[start+1 : end])
}

// normalizeOracleNullable maps Oracle "Y"/"N" nullable to boolean.
func normalizeOracleNullable(v string) bool {
	s := strings.TrimSpace(strings.ToUpper(v))
	return s == "Y" || s == "YES"
}

// normalizeOracleDefault normalizes an Oracle column default value for comparison
// against a MySQL default value.
func normalizeOracleDefault(v string) string {
	raw := strings.TrimSpace(v)
	switch strings.ToLower(raw) {
	case "", "null", "<nil>", "<entry>":
		return ""
	}

	// SYSDATE → CURRENT_TIMESTAMP
	if oracleDefaultSysdateRegexp.MatchString(raw) {
		return "CURRENT_TIMESTAMP"
	}

	// SYSTIMESTAMP → CURRENT_TIMESTAMP
	if oracleDefaultSystimestampRegex.MatchString(raw) {
		return "CURRENT_TIMESTAMP"
	}

	// seq.NEXTVAL → empty (no MySQL equivalent)
	if oracleDefaultSeqNextvalRegexp.MatchString(raw) {
		return ""
	}

	// Strip trailing spaces Oracle may pad
	raw = strings.TrimRight(raw, " ")

	// Unwrap single-quoted string literals: 'abc' → abc
	if len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'' {
		inner := raw[1 : len(raw)-1]
		inner = strings.ReplaceAll(inner, "''", "'")
		return inner
	}

	return raw
}

// CanonicalizeOracleColumnForComparison builds a CanonicalColumn from Oracle
// column metadata for comparison against a MySQL target column.
func CanonicalizeOracleColumnForComparison(name string, attrs []string, destVersion global.MySQLVersionInfo) CanonicalColumn {
	getAttr := func(idx int) string {
		if idx < 0 || idx >= len(attrs) {
			return ""
		}
		return attrs[idx]
	}

	rawType := normalizeNullish(getAttr(0))
	normalizedType := NormalizeOracleColumnType(rawType)

	return CanonicalColumn{
		Name:                name,
		RawType:             rawType,
		NormalizedType:      normalizedType,
		Charset:             "",
		NormalizedCharset:   "",
		Collation:           "",
		NormalizedCollation: "",
		Nullable:            normalizeOracleNullable(getAttr(3)),
		DefaultValue:        normalizeOracleDefault(getAttr(4)),
		Comment:             normalizeNullish(getAttr(5)),
		Visibility:          ColumnVisibilityVisible,
		AutoIncrement:       false,
	}
}

// DecideOracleToMySQLTypeCompatibility compares an Oracle-normalized source type
// against a MySQL target type and returns a compatibility decision.
func DecideOracleToMySQLTypeCompatibility(source, target CanonicalColumn) CompatibilityDecision {
	srcType := strings.ToLower(strings.TrimSpace(source.NormalizedType))
	dstType := strings.ToLower(strings.TrimSpace(target.NormalizedType))
	srcRawUpper := strings.ToUpper(source.RawType)

	// Check warn-only cases first (before exact-match), because normalized
	// types may match while the mapping still loses information.

	// TIMESTAMP WITH TIME ZONE → datetime — warn about timezone info loss
	if strings.Contains(srcRawUpper, "TIME ZONE") && strings.HasPrefix(dstType, "datetime") {
		return CompatibilityDecision{
			State:  CompatibilityWarnOnly,
			Reason: fmt.Sprintf("Oracle %s maps to MySQL %s but timezone information will be lost", source.RawType, target.RawType),
			Source: source.RawType,
			Target: target.RawType,
		}
	}

	// INTERVAL types → varchar — warn about semantic loss
	if strings.Contains(srcRawUpper, "INTERVAL") && strings.HasPrefix(dstType, "varchar") {
		return CompatibilityDecision{
			State:  CompatibilityWarnOnly,
			Reason: fmt.Sprintf("Oracle %s has no MySQL equivalent; mapped to %s for storage only", source.RawType, target.RawType),
			Source: source.RawType,
			Target: target.RawType,
		}
	}

	// XMLTYPE → longtext — warn about XML validation loss
	if srcRawUpper == "XMLTYPE" && dstType == "longtext" {
		return CompatibilityDecision{
			State:  CompatibilityWarnOnly,
			Reason: "Oracle XMLTYPE maps to MySQL longtext but XML validation capability is lost",
			Source: source.RawType,
			Target: target.RawType,
		}
	}

	if srcType == dstType {
		return CompatibilityDecision{
			State:  CompatibilityNormalizedEqual,
			Reason: "Oracle type maps to identical MySQL type",
			Source: source.RawType,
			Target: target.RawType,
		}
	}

	// Check semantic equivalences (normalized forms may differ textually)
	if areOracleToMySQLTypesEquivalent(srcType, dstType) {
		return CompatibilityDecision{
			State:  CompatibilityNormalizedEqual,
			Reason: "Oracle type is semantically equivalent to MySQL type",
			Source: source.RawType,
			Target: target.RawType,
		}
	}

	// Oracle NUMBER(p,0) normalizes to a specific MySQL integer type based on
	// precision, but users often choose any MySQL integer type that fits their
	// actual data range. Treat any Oracle integer → MySQL integer mapping as
	// equivalent (e.g. NUMBER(10)→bigint source vs INT target).
	if isMySQLIntegerType(srcType) && isMySQLIntegerType(dstType) {
		return CompatibilityDecision{
			State:  CompatibilityNormalizedEqual,
			Reason: fmt.Sprintf("Oracle integer type %s is compatible with MySQL integer type %s", source.RawType, target.RawType),
			Source: source.RawType,
			Target: target.RawType,
		}
	}

	// Oracle NUMBER without explicit precision can actually store arbitrary
	// decimals (precision up to 38, scale up to 127). Silently treating it as
	// equivalent to a MySQL integer type would truncate fractional data without
	// any warning. Downgrade to WarnOnly so operators can verify the data shape
	// before accepting the mapping.
	if srcRawUpper == "NUMBER" && isMySQLIntegerType(dstType) {
		return CompatibilityDecision{
			State:  CompatibilityWarnOnly,
			Reason: fmt.Sprintf("Oracle NUMBER (no precision) may hold fractional values; MySQL integer type %s would truncate — verify sample data before treating as compatible", target.RawType),
			Source: source.RawType,
			Target: target.RawType,
		}
	}

	return CompatibilityDecision{
		State:  CompatibilityUnsupported,
		Reason: fmt.Sprintf("Oracle type %s (normalized: %s) does not match MySQL type %s", source.RawType, srcType, dstType),
		Source: source.RawType,
		Target: target.RawType,
	}
}

// isMySQLIntegerType reports whether t (already lowercased and display-width-stripped)
// is one of the MySQL integer types.
func isMySQLIntegerType(t string) bool {
	switch stripMySQLIntDisplayWidth(strings.ToLower(strings.TrimSpace(t))) {
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		return true
	}
	return false
}

// areOracleToMySQLTypesEquivalent checks for semantically equivalent type pairs
// that may have different textual representations.
func areOracleToMySQLTypesEquivalent(srcNormalized, dstNormalized string) bool {
	equivalences := [][2]string{
		// int display width variations
		{"tinyint(1)", "tinyint"},
		{"tinyint", "tinyint(4)"},
		{"smallint", "smallint(6)"},
		{"mediumint", "mediumint(9)"},
		{"int", "int(11)"},
		{"bigint", "bigint(20)"},
		// decimal variations
		{"decimal", "decimal(10,0)"},
		// datetime precision
		{"datetime", "datetime(0)"},
	}

	for _, eq := range equivalences {
		if (srcNormalized == eq[0] && dstNormalized == eq[1]) ||
			(srcNormalized == eq[1] && dstNormalized == eq[0]) {
			return true
		}
	}

	// Strip MySQL integer display width for comparison
	srcStripped := stripMySQLIntDisplayWidth(srcNormalized)
	dstStripped := stripMySQLIntDisplayWidth(dstNormalized)
	if srcStripped == dstStripped && srcStripped != srcNormalized {
		return true
	}

	return false
}

// stripMySQLIntDisplayWidth removes display width from MySQL integer types for comparison.
func stripMySQLIntDisplayWidth(t string) string {
	intTypes := []string{"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}
	lower := strings.ToLower(t)
	for _, it := range intTypes {
		if strings.HasPrefix(lower, it+"(") {
			paren := strings.Index(lower, "(")
			end := strings.Index(lower, ")")
			if paren >= 0 && end > paren {
				return lower[:paren] + lower[end+1:]
			}
		}
	}
	return lower
}

// DecideOracleToMySQLCharsetCompatibility always returns NormalizedEqual
// because Oracle does not have column-level charset metadata.
func DecideOracleToMySQLCharsetCompatibility(source, target CanonicalColumn) CompatibilityDecision {
	return CompatibilityDecision{
		State:  CompatibilityNormalizedEqual,
		Reason: "Oracle has no column-level charset; comparison skipped",
		Source: source.Charset,
		Target: target.Charset,
	}
}

// DecideOracleToMySQLCollationCompatibility always returns NormalizedEqual
// because Oracle does not have column-level collation metadata.
func DecideOracleToMySQLCollationCompatibility(source, target CanonicalColumn) CompatibilityDecision {
	return CompatibilityDecision{
		State:  CompatibilityNormalizedEqual,
		Reason: "Oracle has no column-level collation; comparison skipped",
		Source: source.Collation,
		Target: target.Collation,
	}
}

// DecideOracleToMySQLDefaultCompatibility compares Oracle and MySQL default values.
func DecideOracleToMySQLDefaultCompatibility(source, target CanonicalColumn) CompatibilityDecision {
	srcDefault := source.DefaultValue
	dstDefault := target.DefaultValue

	if srcDefault == dstDefault {
		return CompatibilityDecision{
			State:  CompatibilityNormalizedEqual,
			Reason: "default values match after normalization",
		}
	}

	// Both empty
	if srcDefault == "" && dstDefault == "" {
		return CompatibilityDecision{
			State:  CompatibilityNormalizedEqual,
			Reason: "both defaults are empty/NULL",
		}
	}

	// seq.NEXTVAL was cleared → treat as no-default match
	if srcDefault == "" && dstDefault != "" {
		return CompatibilityDecision{
			State:  CompatibilityWarnOnly,
			Reason: fmt.Sprintf("Oracle default cleared (sequence or no equivalent); MySQL has default=%s", dstDefault),
		}
	}

	return CompatibilityDecision{
		State:  CompatibilityUnsupported,
		Reason: fmt.Sprintf("default value mismatch: Oracle=%q MySQL=%q", srcDefault, dstDefault),
	}
}

// BuildOracleToMySQLRepairPlan generates a ColumnRepairPlan that converts
// Oracle column attributes to MySQL-compatible repair attributes.
func BuildOracleToMySQLRepairPlan(name string, attrs []string, destVersion global.MySQLVersionInfo) ColumnRepairPlan {
	getAttr := func(idx int) string {
		if idx < 0 || idx >= len(attrs) {
			return ""
		}
		return attrs[idx]
	}

	rawType := normalizeNullish(getAttr(0))
	mysqlType := NormalizeOracleColumnType(rawType)

	return ColumnRepairPlan{
		Type:      mysqlType,
		Charset:   "",
		Collation: "",
	}
}

// GenerateOracleToMySQLCreateTableSQL generates a MySQL CREATE TABLE statement
// from Oracle column metadata and index data. The destination MySQL version is
// used to pick a default collation consistent with the server default:
// utf8mb4_0900_ai_ci for MySQL 8.0+, utf8mb4_general_ci for older releases.
func GenerateOracleToMySQLCreateTableSQL(destSchema, destTable string, oracleColumns []map[string]interface{}, oracleIndexData map[string][]string, destVersion global.MySQLVersionInfo) string {
	if len(oracleColumns) == 0 {
		return ""
	}

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (\n",
		escapeMySQLIdent(destSchema), escapeMySQLIdent(destTable)))

	for i, col := range oracleColumns {
		colName := fmt.Sprintf("%v", col["columnName"])
		if colName == "" || colName == "<nil>" {
			continue
		}
		rawType := fmt.Sprintf("%v", col["columnType"])
		mysqlType := NormalizeOracleColumnType(rawType)

		nullable := strings.TrimSpace(fmt.Sprintf("%v", col["isNull"]))
		nullClause := ""
		if strings.EqualFold(nullable, "N") || strings.EqualFold(nullable, "NO") {
			nullClause = " NOT NULL"
		}

		defaultClause := ""
		rawDefault := strings.TrimSpace(fmt.Sprintf("%v", col["columnDefault"]))
		if rawDefault != "" && rawDefault != "<nil>" && !strings.EqualFold(rawDefault, "null") {
			normalized := normalizeOracleDefault(rawDefault)
			if normalized != "" {
				if strings.EqualFold(normalized, "CURRENT_TIMESTAMP") {
					defaultClause = fmt.Sprintf(" DEFAULT %s", normalized)
				} else {
					defaultClause = fmt.Sprintf(" DEFAULT '%s'", strings.ReplaceAll(normalized, "'", "''"))
				}
			}
		}

		comment := ""
		rawComment := normalizeNullish(fmt.Sprintf("%v", col["columnComment"]))
		if rawComment != "" {
			comment = fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(rawComment, "'", "''"))
		}

		comma := ","
		if i == len(oracleColumns)-1 && len(oracleIndexData) == 0 {
			comma = ""
		}

		buf.WriteString(fmt.Sprintf("  `%s` %s%s%s%s%s\n",
			escapeMySQLIdent(colName), mysqlType, nullClause, defaultClause, comment, comma))
	}

	// Add primary key if available from index data
	if pk, exists := oracleIndexData["PRIMARY"]; exists && len(pk) > 0 {
		pkCols := make([]string, len(pk))
		for i, c := range pk {
			pkCols[i] = fmt.Sprintf("`%s`", escapeMySQLIdent(c))
		}
		buf.WriteString(fmt.Sprintf("  PRIMARY KEY (%s)\n", strings.Join(pkCols, ",")))
	}

	collation := "utf8mb4_general_ci"
	if destVersion.Major >= 8 {
		collation = "utf8mb4_0900_ai_ci"
	}
	buf.WriteString(fmt.Sprintf(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=%s;\n", collation))
	return buf.String()
}

// escapeMySQLIdent escapes backtick characters in MySQL identifiers.
func escapeMySQLIdent(v string) string {
	return strings.ReplaceAll(v, "`", "``")
}
