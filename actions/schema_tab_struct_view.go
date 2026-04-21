package actions

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"gt-checksum/global"
	"gt-checksum/schemacompat"
)

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

// viewIntegerDisplayWidthRegex matches MySQL/MariaDB integer display widths such as
// "int(10)" or "bigint(20)".  MySQL 8.0.17+ removed display widths from
// INFORMATION_SCHEMA.COLUMNS for integer types (except those declared with ZEROFILL),
// so a MariaDB 10.x source reporting "int(10) unsigned" will textually differ from a
// MySQL 8.0 destination reporting "int unsigned" even though the columns are
// semantically identical.  This regex lets us strip the display width before
// comparing view column signatures.
var viewIntegerDisplayWidthRegex = regexp.MustCompile(`\b(tinyint|smallint|mediumint|int|integer|bigint)\s*\(\s*\d+\s*\)`)

// viewYearDisplayWidthRegex normalises the legacy "year(4)" form (MySQL 5.6/5.7)
// to plain "year" (MySQL 8.0+ rendering).
var viewYearDisplayWidthRegex = regexp.MustCompile(`\byear\s*\(\s*\d+\s*\)`)

// normalizeViewColumnTypeForCompare lowercases the column type and erases
// display-width drift that is purely a rendering difference between MariaDB/MySQL
// versions.  It must be applied to BOTH source and destination signatures so that
// callers can compare apples-to-apples.
func normalizeViewColumnTypeForCompare(colType string) string {
	s := strings.ToLower(strings.TrimSpace(colType))
	s = viewIntegerDisplayWidthRegex.ReplaceAllString(s, "${1}")
	s = viewYearDisplayWidthRegex.ReplaceAllString(s, "year")
	// "integer" and "int" are aliases — collapse to the shorter form.
	s = regexp.MustCompile(`\binteger\b`).ReplaceAllString(s, "int")
	return strings.Join(strings.Fields(s), " ")
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
			normalizeViewColumnTypeForCompare(colType),
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
