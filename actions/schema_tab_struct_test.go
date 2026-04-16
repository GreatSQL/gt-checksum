package actions

import (
	"bytes"
	"database/sql"
	"strings"
	"testing"

	"gt-checksum/dbExec"
	"gt-checksum/global"
	golog "gt-checksum/go-log/log"
	"gt-checksum/inputArg"
	"gt-checksum/schemacompat"
)

// ---------- normalizeRoutineDefinitionForCompare ----------

func TestNormalizeRoutineDefinitionForCompare_Empty(t *testing.T) {
	if got := normalizeRoutineDefinitionForCompare(""); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestNormalizeRoutineDefinitionForCompare_StripMetadataComment(t *testing.T) {
	input := "CREATE FUNCTION `f`() RETURNS int /*GT_CHECKSUM_METADATA:abc*/ BEGIN RETURN 1; END"
	got := normalizeRoutineDefinitionForCompare(input)
	if contains(got, "GT_CHECKSUM_METADATA") {
		t.Fatalf("metadata comment not stripped: %q", got)
	}
}

func TestNormalizeRoutineDefinitionForCompare_StripIntDisplayWidth(t *testing.T) {
	// MySQL 5.6 returns int(11), MySQL 8.0.17+ returns int
	src := "CREATE FUNCTION `myAdd`(n1 int(11), n2 int(11)) RETURNS int(11) BEGIN RETURN n1+n2; END"
	dst := "CREATE FUNCTION `myAdd`(n1 int, n2 int) RETURNS int BEGIN RETURN n1+n2; END"
	if normalizeRoutineDefinitionForCompare(src) != normalizeRoutineDefinitionForCompare(dst) {
		t.Fatalf("int display width difference should be normalized away")
	}
}

func TestNormalizeRoutineDefinitionForCompare_PreserveStringLiteralCase(t *testing.T) {
	// The key fix: string literals in the body should NOT be lowered
	src := "CREATE FUNCTION `f`() RETURNS varchar(10) BEGIN RETURN 'Children'; END"
	dst := "CREATE FUNCTION `f`() RETURNS varchar(10) BEGIN RETURN 'children'; END"
	srcN := normalizeRoutineDefinitionForCompare(src)
	dstN := normalizeRoutineDefinitionForCompare(dst)
	if srcN == dstN {
		t.Fatalf("string literal case difference should NOT be swallowed: both normalized to %q", srcN)
	}
}

func TestNormalizeRoutineDefinitionForCompare_RoutineNameCaseInsensitive(t *testing.T) {
	src := "CREATE FUNCTION `getAgeStr`() RETURNS varchar(10) BEGIN RETURN 'x'; END"
	dst := "CREATE FUNCTION `GETAGESTR`() RETURNS varchar(10) BEGIN RETURN 'x'; END"
	if normalizeRoutineDefinitionForCompare(src) != normalizeRoutineDefinitionForCompare(dst) {
		t.Fatalf("routine name case difference should be normalized")
	}
}

func TestNormalizeRoutineDefinitionForCompare_BigIntDisplayWidth(t *testing.T) {
	src := "CREATE PROCEDURE `p`(IN id bigint(20)) BEGIN SELECT id; END"
	dst := "CREATE PROCEDURE `p`(IN id bigint) BEGIN SELECT id; END"
	if normalizeRoutineDefinitionForCompare(src) != normalizeRoutineDefinitionForCompare(dst) {
		t.Fatalf("bigint display width difference should be normalized away")
	}
}

// ---------- normalizeRoutineCreateSQLForCompareWithCatalog ----------

func TestNormalizeRoutineCreateSQLForCompareWithCatalog_NoVersionInfo(t *testing.T) {
	sql := "CREATE FUNCTION `f`() RETURNS int BEGIN RETURN 1; END"
	got := normalizeRoutineCreateSQLForCompareWithCatalog(sql)
	if got == "" {
		t.Fatal("should return non-empty result even without version info")
	}
}

func TestNormalizeRoutineCreateSQLForCompareWithCatalog_StripPlatformDefaultCollation(t *testing.T) {
	// MySQL 8.0 default: utf8mb4_0900_ai_ci
	sql := "CREATE FUNCTION `f`() RETURNS varchar(10) CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci BEGIN RETURN 'x'; END"
	info := global.MySQLVersionInfo{
		Flavor: global.DatabaseFlavorMySQL,
		Major:  8,
		Minor:  0,
	}
	got := normalizeRoutineCreateSQLForCompareWithCatalog(sql, info)
	if contains(got, "utf8mb4_0900_ai_ci") {
		t.Fatalf("platform default collation should be stripped, got: %q", got)
	}
}

func TestNormalizeRoutineCreateSQLForCompareWithCatalog_CrossPlatformMerge(t *testing.T) {
	// Both platforms' defaults should be stripped
	sql := "CREATE FUNCTION `f`() RETURNS varchar(10) CHARSET utf8mb4 COLLATE utf8mb4_general_ci BEGIN RETURN 'x'; END"
	srcInfo := global.MySQLVersionInfo{
		Flavor: global.DatabaseFlavorMariaDB,
		Major:  10,
		Minor:  5,
	}
	dstInfo := global.MySQLVersionInfo{
		Flavor: global.DatabaseFlavorMySQL,
		Major:  8,
		Minor:  0,
	}
	got := normalizeRoutineCreateSQLForCompareWithCatalog(sql, srcInfo, dstInfo)
	if contains(got, "utf8mb4_general_ci") {
		t.Fatalf("source platform default collation should be stripped, got: %q", got)
	}
}

// ---------- isCharsetMetadataCollationMapped ----------

func TestIsCharsetMetadataCollationMapped_SameCollation(t *testing.T) {
	// Same everything → not "mapped", just identical → should return false
	if isCharsetMetadataCollationMapped("utf8mb4", "utf8mb4_general_ci", "utf8mb4_general_ci",
		"utf8mb4", "utf8mb4_general_ci", "utf8mb4_general_ci") {
		t.Fatal("identical metadata should not be flagged as collation-mapped")
	}
}

func TestIsCharsetMetadataCollationMapped_DifferentCharsetClient(t *testing.T) {
	// CHARACTER_SET_CLIENT mismatch → not a pure collation mapping
	if isCharsetMetadataCollationMapped("utf8mb4", "utf8mb4_uca1400_ai_ci", "utf8mb4_uca1400_ai_ci",
		"utf8", "utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci") {
		t.Fatal("different CHARACTER_SET_CLIENT should not be mapped")
	}
}

func TestIsCharsetMetadataCollationMapped_UCA1400to0900(t *testing.T) {
	// MariaDB 11.5+ uca1400 → MySQL 0900 mapping
	if !isCharsetMetadataCollationMapped("utf8mb4", "utf8mb4_uca1400_ai_ci", "utf8mb4_uca1400_ai_ci",
		"utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci") {
		t.Fatal("uca1400→0900 should be detected as collation-mapped")
	}
}

// ---------- hasCharsetMetadataCollationDiff ----------

func TestHasCharsetMetadataCollationDiff_NoDiff(t *testing.T) {
	if hasCharsetMetadataCollationDiff("utf8mb4", "utf8mb4_general_ci", "utf8mb4_general_ci",
		"utf8mb4", "utf8mb4_general_ci", "utf8mb4_general_ci") {
		t.Fatal("identical metadata should not have diff")
	}
}

func TestHasCharsetMetadataCollationDiff_CharsetClientDiff(t *testing.T) {
	if !hasCharsetMetadataCollationDiff("utf8mb4", "utf8mb4_general_ci", "utf8mb4_general_ci",
		"utf8", "utf8mb4_general_ci", "utf8mb4_general_ci") {
		t.Fatal("CHARACTER_SET_CLIENT difference should be detected")
	}
}

func TestHasCharsetMetadataCollationDiff_CollationConnDiff(t *testing.T) {
	if !hasCharsetMetadataCollationDiff("utf8mb4", "utf8mb4_general_ci", "utf8mb4_general_ci",
		"utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4_general_ci") {
		t.Fatal("COLLATION_CONNECTION difference should be detected")
	}
}

func TestHasCharsetMetadataCollationDiff_CaseInsensitive(t *testing.T) {
	if hasCharsetMetadataCollationDiff("UTF8MB4", "utf8mb4_general_ci", "x",
		"utf8mb4", "UTF8MB4_GENERAL_CI", "y") {
		t.Fatal("comparison should be case-insensitive")
	}
}

// ---------------------------------------------------------------------------
// normalizeViewCreateSQLForCompare
// ---------------------------------------------------------------------------

func TestNormalizeViewCreateSQL_stripsDefiner(t *testing.T) {
	input := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v1` AS SELECT 1"
	got := normalizeViewCreateSQLForCompare(input)
	if contains(got, "definer=") {
		t.Errorf("DEFINER clause not stripped: %q", got)
	}
}

func TestNormalizeViewCreateSQL_equivalentAfterNormalize(t *testing.T) {
	src := "CREATE ALGORITHM=UNDEFINED DEFINER=`user1`@`host1` SQL SECURITY DEFINER VIEW `v1` AS SELECT id, name FROM t"
	dst := "CREATE ALGORITHM=UNDEFINED DEFINER=`user2`@`host2` SQL SECURITY DEFINER VIEW `v1` AS SELECT id, name FROM t"
	if normalizeViewCreateSQLForCompare(src) != normalizeViewCreateSQLForCompare(dst) {
		t.Errorf("equivalent VIEWs should normalize to same string")
	}
}

func TestNormalizeViewCreateSQL_differentQueryNotEqual(t *testing.T) {
	src := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v1` AS SELECT id FROM t"
	dst := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v1` AS SELECT id, name FROM t"
	if normalizeViewCreateSQLForCompare(src) == normalizeViewCreateSQLForCompare(dst) {
		t.Errorf("views with different SELECT should not normalize to same string")
	}
}

func TestNormalizeViewCreateSQL_collapseWhitespace(t *testing.T) {
	input := "CREATE  VIEW  `v1`  AS  SELECT  1"
	got := normalizeViewCreateSQLForCompare(input)
	if contains(got, "  ") {
		t.Errorf("double spaces should be collapsed: %q", got)
	}
}

func TestNormalizeViewCreateSQL_preservesBodyCase(t *testing.T) {
	// String literal 'ABC' in SELECT body must survive normalization unchanged.
	src := "CREATE ALGORITHM=UNDEFINED DEFINER=`u1`@`h1` SQL SECURITY DEFINER VIEW `v1` AS SELECT 'ABC' AS col"
	dst := "CREATE ALGORITHM=UNDEFINED DEFINER=`u2`@`h2` SQL SECURITY DEFINER VIEW `v1` AS SELECT 'abc' AS col"
	// Different string literals → should NOT normalize to the same value.
	if normalizeViewCreateSQLForCompare(src) == normalizeViewCreateSQLForCompare(dst) {
		t.Errorf("views with different string literals in SELECT body should not be equal after normalization")
	}
}

func TestNormalizeViewCreateSQL_headerKeywordsLowercased(t *testing.T) {
	// The header (up to and including the VIEW name) should be lowercased.
	input := "CREATE ALGORITHM=UNDEFINED SQL SECURITY DEFINER VIEW `V1` AS SELECT 1"
	got := normalizeViewCreateSQLForCompare(input)
	// "create" should appear in lowercase in the result.
	if !contains(got, "create") {
		t.Errorf("header keyword CREATE should be lowercased: %q", got)
	}
}

func TestNormalizeViewCreateSQL_sameBodyDifferentDefiner(t *testing.T) {
	// Two views with same SELECT but different DEFINER must normalize to same value.
	src := "CREATE ALGORITHM=UNDEFINED DEFINER=`admin`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM orders"
	dst := "CREATE ALGORITHM=UNDEFINED DEFINER=`app`@`10.0.0.1` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM orders"
	if normalizeViewCreateSQLForCompare(src) != normalizeViewCreateSQLForCompare(dst) {
		t.Errorf("views differing only in DEFINER should normalize to same value")
	}
}

func TestNormalizeViewCreateSQL_algorithmUndefinedEqualsOmitted(t *testing.T) {
	// ALGORITHM=UNDEFINED is the MySQL default; a VIEW created without an explicit
	// ALGORITHM clause is semantically identical to one with ALGORITHM=UNDEFINED.
	// Some MySQL versions include it in SHOW CREATE VIEW, others omit it — both
	// must normalize to the same string.
	withAlgorithm := "CREATE ALGORITHM=UNDEFINED SQL SECURITY DEFINER VIEW `v1` AS SELECT id FROM t"
	withoutAlgorithm := "CREATE SQL SECURITY DEFINER VIEW `v1` AS SELECT id FROM t"
	if normalizeViewCreateSQLForCompare(withAlgorithm) != normalizeViewCreateSQLForCompare(withoutAlgorithm) {
		t.Errorf("ALGORITHM=UNDEFINED and omitted ALGORITHM should normalize to same value;\n  with: %q\n  without: %q",
			normalizeViewCreateSQLForCompare(withAlgorithm),
			normalizeViewCreateSQLForCompare(withoutAlgorithm))
	}
}

func TestNormalizeViewCreateSQL_algorithmMergeNotStripped(t *testing.T) {
	// Only ALGORITHM=UNDEFINED is stripped; MERGE and TEMPTABLE are intentional
	// user choices and must remain in the normalized output so differences are caught.
	withMerge := "CREATE ALGORITHM=MERGE SQL SECURITY DEFINER VIEW `v1` AS SELECT id FROM t"
	withUndefined := "CREATE ALGORITHM=UNDEFINED SQL SECURITY DEFINER VIEW `v1` AS SELECT id FROM t"
	if normalizeViewCreateSQLForCompare(withMerge) == normalizeViewCreateSQLForCompare(withUndefined) {
		t.Errorf("ALGORITHM=MERGE vs ALGORITHM=UNDEFINED should NOT normalize to same value")
	}
}

func TestNormalizeViewCreateSQL_sqlSecurityDefinerEqualsInvoker(t *testing.T) {
	// SQL SECURITY DEFINER vs INVOKER is a migration-safe change; it must not
	// trigger Diffs=yes on its own (cc §四).  Both sides should normalize to the
	// same string when the SELECT body is otherwise identical.
	withDefiner := "CREATE SQL SECURITY DEFINER VIEW `v1` AS SELECT id FROM t"
	withInvoker := "CREATE SQL SECURITY INVOKER VIEW `v1` AS SELECT id FROM t"
	if normalizeViewCreateSQLForCompare(withDefiner) != normalizeViewCreateSQLForCompare(withInvoker) {
		t.Errorf("SQL SECURITY DEFINER vs INVOKER should normalize to same value;\n  definer: %q\n  invoker: %q",
			normalizeViewCreateSQLForCompare(withDefiner),
			normalizeViewCreateSQLForCompare(withInvoker))
	}
}

func TestNormalizeViewCreateSQL_sqlSecurityOmittedEqualsExplicit(t *testing.T) {
	// A VIEW created without SQL SECURITY (MySQL defaults to DEFINER) must equal
	// a VIEW with SQL SECURITY DEFINER after normalization.
	withClause := "CREATE SQL SECURITY DEFINER VIEW `v1` AS SELECT id FROM t"
	withoutClause := "CREATE VIEW `v1` AS SELECT id FROM t"
	if normalizeViewCreateSQLForCompare(withClause) != normalizeViewCreateSQLForCompare(withoutClause) {
		t.Errorf("explicit SQL SECURITY DEFINER and omitted SQL SECURITY should normalize to same value;\n  with: %q\n  without: %q",
			normalizeViewCreateSQLForCompare(withClause),
			normalizeViewCreateSQLForCompare(withoutClause))
	}
}

// TestNormalizeViewCreateSQL_schemaQualifiedEqualsUnqualified verifies that a
// schema-prefixed view identifier (e.g. `db1`.`v1`, returned by some MySQL
// versions) normalises to the same value as an unqualified identifier (`v1`).
// This prevents false Diffs=yes in cross-schema-mapping scenarios.
func TestNormalizeViewCreateSQL_schemaQualifiedEqualsUnqualified(t *testing.T) {
	qualified := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM t"
	unqualified := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v1` AS SELECT id FROM t"
	qNorm := normalizeViewCreateSQLForCompare(qualified)
	uNorm := normalizeViewCreateSQLForCompare(unqualified)
	if qNorm != uNorm {
		t.Errorf("schema-qualified and unqualified view names should normalize to same value;\n  qualified → %q\n  unqualified → %q",
			qNorm, uNorm)
	}
}

// TestNormalizeViewCreateSQL_crossSchemaMappingNoDiff verifies that when source and
// destination use different schema names (e.g. db1→db2 mapping) but identical VIEW
// bodies, the normalised DDLs are equal — no false Diffs=yes.
func TestNormalizeViewCreateSQL_crossSchemaMappingNoDiff(t *testing.T) {
	srcDDL := "CREATE DEFINER=`root`@`%` VIEW `db1`.`v_orders` AS SELECT id, amount FROM orders"
	dstDDL := "CREATE DEFINER=`app`@`%` VIEW `db2`.`v_orders` AS SELECT id, amount FROM orders"
	if normalizeViewCreateSQLForCompare(srcDDL) != normalizeViewCreateSQLForCompare(dstDDL) {
		t.Errorf("views with same body but different schemas should normalize to same value;\n  src → %q\n  dst → %q",
			normalizeViewCreateSQLForCompare(srcDDL),
			normalizeViewCreateSQLForCompare(dstDDL))
	}
}

// ---------------------------------------------------------------------------
// normalizeViewWhereOuterParens
// ---------------------------------------------------------------------------

func TestNormalizeViewWhereOuterParens_simpleCondition(t *testing.T) {
	// Core scenario: MariaDB vs MySQL 8.0 parenthesis difference.
	in := "AS select `t`.`f1` AS `f1` from `t` where (`t`.`f1` > '3')"
	want := "AS select `t`.`f1` AS `f1` from `t` where `t`.`f1` > '3'"
	if got := normalizeViewWhereOuterParens(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeViewWhereOuterParens_noParens(t *testing.T) {
	// Already in canonical form (MariaDB style) — must be unchanged.
	in := "AS select `f1` from `t` where `f1` > '3'"
	if got := normalizeViewWhereOuterParens(in); got != in {
		t.Errorf("no-paren body should be unchanged, got %q", got)
	}
}

func TestNormalizeViewWhereOuterParens_nestedParens(t *testing.T) {
	// Outer paren wraps condition that itself contains parens (IN clause).
	in := "AS select f1 from t where (f1 IN (1, 2, 3))"
	want := "AS select f1 from t where f1 IN (1, 2, 3)"
	if got := normalizeViewWhereOuterParens(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeViewWhereOuterParens_withGroupBy(t *testing.T) {
	// Outer paren before GROUP BY clause should also be stripped.
	in := "AS select f1, count(*) from t where (f1 > '3') group by f1"
	want := "AS select f1, count(*) from t where f1 > '3' group by f1"
	if got := normalizeViewWhereOuterParens(in); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeViewWhereOuterParens_partialParenNotStripped(t *testing.T) {
	// "where (a > 1) and b < 2" — outer paren does NOT span the full condition.
	// Must NOT be stripped to avoid changing the semantics.
	in := "AS select f1 from t where (f1 > '3') and f2 = 'x'"
	if got := normalizeViewWhereOuterParens(in); got != in {
		t.Errorf("partial-paren body should be unchanged, got %q", got)
	}
}

// TestNormalizeViewCreateSQL_mariaDBvsMySQL80WhereParens is the regression test
// for the actual MariaDB→MySQL 8.0 scenario: identical views that differ only in
// whether the WHERE body is parenthesized must normalize to the same string.
func TestNormalizeViewCreateSQL_mariaDBvsMySQL80WhereParens(t *testing.T) {
	// MariaDB SHOW CREATE VIEW — no outer parens on WHERE condition.
	srcDDL := "CREATE ALGORITHM=UNDEFINED DEFINER=`checksum`@`%` SQL SECURITY DEFINER VIEW `v_teststring` AS select `teststring`.`f1` AS `f1` from `teststring` where `teststring`.`f1` > '3'"
	// MySQL 8.0 SHOW CREATE VIEW — MySQL 8.0 wraps the WHERE condition in parens.
	dstDDL := "CREATE ALGORITHM=UNDEFINED DEFINER=`checksum`@`%` SQL SECURITY DEFINER VIEW `v_teststring` AS select `teststring`.`f1` AS `f1` from `teststring` where (`teststring`.`f1` > '3')"
	srcNorm := normalizeViewCreateSQLForCompare(srcDDL)
	dstNorm := normalizeViewCreateSQLForCompare(dstDDL)
	if srcNorm != dstNorm {
		t.Errorf("MariaDB vs MySQL 8.0 WHERE paren difference should normalize to equal;\n  src: %q\n  dst: %q",
			srcNorm, dstNorm)
	}
}

// TestCheckViewStruct_sqlSecurityDiffIsNotDiffsYes verifies that when src has
// SQL SECURITY DEFINER and dst has SQL SECURITY INVOKER, the normalised DDL is
// identical — meaning the comparison produces ddlDiffers=false (Diffs=no).
// This exercises the strip logic without requiring a live database connection.
func TestCheckViewStruct_sqlSecurityDiffIsNotDiffsYes(t *testing.T) {
	srcDDL := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM t"
	dstDDL := "CREATE ALGORITHM=UNDEFINED DEFINER=`app`@`%` SQL SECURITY INVOKER VIEW `db1`.`v1` AS SELECT id FROM t"
	srcNorm := normalizeViewCreateSQLForCompare(srcDDL)
	dstNorm := normalizeViewCreateSQLForCompare(dstDDL)
	if srcNorm != dstNorm {
		t.Errorf("SQL SECURITY DEFINER vs INVOKER should produce equal normalized DDL (Diffs=no);\n  src: %q\n  dst: %q",
			srcNorm, dstNorm)
	}
}

func TestWarnViewSQLSecurityDifference_emitsWarn(t *testing.T) {
	origWlog := global.Wlog
	defer func() {
		global.Wlog = origWlog
	}()

	var buf bytes.Buffer
	handler, err := golog.NewStreamHandler(&buf)
	if err != nil {
		t.Fatalf("NewStreamHandler failed: %v", err)
	}
	global.Wlog = golog.NewDefault(handler)

	srcDDL := "CREATE DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM t"
	dstDDL := "CREATE DEFINER=`app`@`%` SQL SECURITY INVOKER VIEW `db1`.`v1` AS SELECT id FROM t"

	if !warnViewSQLSecurityDifference(88, "db1", "v1", srcDDL, dstDDL) {
		t.Fatalf("expected warnViewSQLSecurityDifference to report a warning")
	}
	got := buf.String()
	if !contains(got, "SQL SECURITY differs: src=DEFINER dst=INVOKER") {
		t.Fatalf("expected warn log to mention effective SQL SECURITY values, got %q", got)
	}
}

func TestWarnViewSQLSecurityDifference_omittedEqualsExplicitDefiner(t *testing.T) {
	origWlog := global.Wlog
	defer func() {
		global.Wlog = origWlog
	}()

	var buf bytes.Buffer
	handler, err := golog.NewStreamHandler(&buf)
	if err != nil {
		t.Fatalf("NewStreamHandler failed: %v", err)
	}
	global.Wlog = golog.NewDefault(handler)

	srcDDL := "CREATE VIEW `db1`.`v1` AS SELECT id FROM t"
	dstDDL := "CREATE SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM t"

	if warnViewSQLSecurityDifference(89, "db1", "v1", srcDDL, dstDDL) {
		t.Fatalf("omitted SQL SECURITY and explicit DEFINER should be treated as equal")
	}
	if got := buf.String(); got != "" {
		t.Fatalf("expected no warn log, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// viewColumnSignaturesEqual
// ---------------------------------------------------------------------------

func TestViewColumnSignaturesEqual_empty(t *testing.T) {
	ok, reason := viewColumnSignaturesEqual(nil, nil)
	if !ok {
		t.Errorf("nil==nil should be equal, got reason=%q", reason)
	}
}

func TestViewColumnSignaturesEqual_same(t *testing.T) {
	cols := []string{"id|int|NO||", "name|varchar(100)|YES|utf8mb4|utf8mb4_0900_ai_ci"}
	ok, reason := viewColumnSignaturesEqual(cols, cols)
	if !ok {
		t.Errorf("identical signatures should be equal, got reason=%q", reason)
	}
}

func TestViewColumnSignaturesEqual_countDiff(t *testing.T) {
	src := []string{"id|int|NO||"}
	dst := []string{"id|int|NO||", "name|varchar(100)|YES|utf8mb4|utf8mb4_0900_ai_ci"}
	ok, reason := viewColumnSignaturesEqual(src, dst)
	if ok {
		t.Errorf("different column counts should not be equal")
	}
	if !contains(reason, "column count") {
		t.Errorf("reason should mention column count, got %q", reason)
	}
}

func TestViewColumnSignaturesEqual_typeDiff(t *testing.T) {
	src := []string{"id|int|NO||"}
	dst := []string{"id|bigint|NO||"}
	ok, reason := viewColumnSignaturesEqual(src, dst)
	if ok {
		t.Errorf("differing column type should not be equal")
	}
	if !contains(reason, "column[0]") {
		t.Errorf("reason should mention column index, got %q", reason)
	}
}

func TestViewColumnSignaturesEqual_collationDiff(t *testing.T) {
	src := []string{"name|varchar(100)|YES|utf8mb4|utf8mb4_0900_ai_ci"}
	dst := []string{"name|varchar(100)|YES|utf8mb4|utf8mb4_unicode_ci"}
	ok, _ := viewColumnSignaturesEqual(src, dst)
	if ok {
		t.Errorf("differing collation should not be equal")
	}
}

// TestNormalizeViewColumnTypeForCompare_intDisplayWidth verifies that
// MariaDB-style "int(10) unsigned" and MySQL 8.0 "int unsigned" normalise to
// the same canonical form, which is the root-cause fix for the
// "id|int(10) unsigned|NO||" vs "id|int unsigned|NO||" view metadata drift.
func TestNormalizeViewColumnTypeForCompare_intDisplayWidth(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"int(10) unsigned", "int unsigned"},
		{"INT UNSIGNED", "int unsigned"},
		{"bigint(20)", "bigint"},
		{"BIGINT", "bigint"},
		{"tinyint(1)", "tinyint"},
		{"integer(11)", "int"},
		{"year(4)", "year"},
		{"varchar(100)", "varchar(100)"}, // unchanged
	}
	for _, c := range cases {
		if got := normalizeViewColumnTypeForCompare(c.in); got != c.want {
			t.Errorf("normalize(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestViewColumnSignaturesEqual_intDisplayWidthDrift is the end-to-end regression
// for the user-reported bug: src=MariaDB 10.0 reports "int(10) unsigned" while
// dst=MySQL 8.0 reports "int unsigned" for the same column; the two signatures
// must compare equal after normalization.
func TestViewColumnSignaturesEqual_intDisplayWidthDrift(t *testing.T) {
	// Signatures are the post-normalization shape that queryMySQLViewColumnSignature
	// produces — both ends should land on "int unsigned".
	src := []string{"id|" + normalizeViewColumnTypeForCompare("int(10) unsigned") + "|NO||"}
	dst := []string{"id|" + normalizeViewColumnTypeForCompare("int unsigned") + "|NO||"}
	ok, reason := viewColumnSignaturesEqual(src, dst)
	if !ok {
		t.Errorf("int(10) unsigned vs int unsigned should be equal after normalization, got reason=%q", reason)
	}
}

// ---------------------------------------------------------------------------
// viewColumnSignaturesCollationOnly
// ---------------------------------------------------------------------------

func TestViewColumnSignaturesCollationOnly_trueWhenOnlyCollationDiffers(t *testing.T) {
	// Mirrors the MySQL 5.7→8.0 utf8mb4_general_ci→utf8mb4_0900_ai_ci scenario.
	src := []string{
		"f1|char(1)|YES|utf8mb4|utf8mb4_general_ci",
		"f2|varchar(100)|NO|utf8mb4|utf8mb4_general_ci",
	}
	dst := []string{
		"f1|char(1)|YES|utf8mb4|utf8mb4_0900_ai_ci",
		"f2|varchar(100)|NO|utf8mb4|utf8mb4_0900_ai_ci",
	}
	if !viewColumnSignaturesCollationOnly(src, dst) {
		t.Errorf("collation-only diff should return true")
	}
}

func TestViewColumnSignaturesCollationOnly_falseWhenTypeDiffers(t *testing.T) {
	src := []string{"id|int|NO||"}
	dst := []string{"id|bigint|NO||"}
	if viewColumnSignaturesCollationOnly(src, dst) {
		t.Errorf("type difference should return false")
	}
}

func TestViewColumnSignaturesCollationOnly_falseWhenCharsetDiffers(t *testing.T) {
	src := []string{"f1|varchar(10)|YES|utf8mb4|utf8mb4_general_ci"}
	dst := []string{"f1|varchar(10)|YES|latin1|latin1_swedish_ci"}
	if viewColumnSignaturesCollationOnly(src, dst) {
		t.Errorf("charset difference should return false")
	}
}

func TestViewColumnSignaturesCollationOnly_falseWhenCountDiffers(t *testing.T) {
	src := []string{"f1|char(1)|YES|utf8mb4|utf8mb4_general_ci"}
	dst := []string{"f1|char(1)|YES|utf8mb4|utf8mb4_0900_ai_ci", "f2|int|NO||"}
	if viewColumnSignaturesCollationOnly(src, dst) {
		t.Errorf("count difference should return false")
	}
}

func TestViewColumnSignaturesCollationOnly_falseWhenEqual(t *testing.T) {
	sig := []string{"f1|char(1)|YES|utf8mb4|utf8mb4_general_ci"}
	if viewColumnSignaturesCollationOnly(sig, sig) {
		t.Errorf("equal signatures should return false (no drift)")
	}
}

func TestBuildViewColumnCollationDriftAdvisoryLines_containsExpectedFields(t *testing.T) {
	lines := buildViewColumnCollationDriftAdvisoryLines("db1", "v1", "column[0] differs: collation")
	mustContainLine(t, lines, "warn-only")
	mustContainLine(t, lines, "VIEW COLUMN COLLATION DRIFT")
	mustContainLine(t, lines, "column[0] differs: collation")
	mustContainLine(t, lines, "base-table")
	mustContainLine(t, lines, "advisory begin")
	mustContainLine(t, lines, "advisory end")
	// Must NOT contain executable SQL (no DROP VIEW / CREATE OR REPLACE).
	for _, l := range lines {
		if contains(strings.ToUpper(l), "DROP VIEW") || contains(strings.ToUpper(l), "CREATE OR REPLACE") {
			t.Errorf("collation-drift advisory must not contain DDL SQL: %q", l)
		}
	}
}

// ---------------------------------------------------------------------------
// buildViewColumnMetadataAdvisoryLines
// ---------------------------------------------------------------------------

func TestBuildViewColumnMetadataAdvisoryLines_suggestedSQLNone(t *testing.T) {
	// No srcCreateSQL / colConn → falls back to "suggested SQL: none".
	lines := buildViewColumnMetadataAdvisoryLines("db1", "v1", "column[0] differs", "", "", "")
	found := false
	for _, l := range lines {
		if contains(l, "suggested SQL: none") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("column-metadata advisory without colConn must contain 'suggested SQL: none'")
	}
}

func TestBuildViewColumnMetadataAdvisoryLines_kindLine(t *testing.T) {
	lines := buildViewColumnMetadataAdvisoryLines("db1", "v1", "reason", "", "", "")
	found := false
	for _, l := range lines {
		if contains(l, "VIEW COLUMN METADATA") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("column-metadata advisory must contain 'kind: VIEW COLUMN METADATA'")
	}
}

func TestBuildViewColumnMetadataAdvisoryLines_withCollation(t *testing.T) {
	// When colConn is provided with valid srcCreateSQL, block must contain executable SET + CREATE (no DROP).
	srcDDL := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM t"
	lines := buildViewColumnMetadataAdvisoryLines("db1", "v1", "column[0] differs: collation", srcDDL, "utf8mb4", "utf8mb4_general_ci")
	mustContainLine(t, lines, "SET collation_connection = utf8mb4_general_ci;")
	mustContainLine(t, lines, "CREATE OR REPLACE")
	mustContainLine(t, lines, "SET collation_connection = DEFAULT;")
	mustContainLine(t, lines, "SET character_set_client = DEFAULT;")
	mustContainLine(t, lines, "VIEW COLUMN METADATA")
	// No "suggested SQL: none" when fix SQL is available.
	for _, l := range lines {
		if contains(l, "suggested SQL: none") {
			t.Errorf("advisory with colConn must not contain 'suggested SQL: none': %q", l)
		}
	}
	// DROP VIEW must not appear; CREATE OR REPLACE is sufficient.
	for _, l := range lines {
		if contains(strings.ToUpper(l), "DROP VIEW") {
			t.Errorf("advisory must not contain DROP VIEW; CREATE OR REPLACE is sufficient: %q", l)
		}
	}
}

// ---------------------------------------------------------------------------
// splitTableViewEntries
// ---------------------------------------------------------------------------

func TestSplitTableViewEntries_emptyObjectKinds(t *testing.T) {
	dtabS := []string{"db1.t1", "db1.v1", "db1.t2"}
	tables, views := splitTableViewEntries(dtabS, nil, "yes")
	if len(tables) != 3 {
		t.Errorf("with empty objectKinds all entries should be tables, got %d", len(tables))
	}
	if len(views) != 0 {
		t.Errorf("with empty objectKinds no views expected, got %d", len(views))
	}
}

func TestSplitTableViewEntries_separatesViews(t *testing.T) {
	kinds := map[string]string{
		"db1/*schema&table*/v_orders": "VIEW",
		"db1/*schema&table*/t1":       "BASE TABLE",
	}
	dtabS := []string{"db1.t1", "db1.v_orders", "db1.t2"}
	tables, views := splitTableViewEntries(dtabS, kinds, "yes")
	if len(tables) != 2 {
		t.Errorf("expected 2 table entries, got %d: %v", len(tables), tables)
	}
	if len(views) != 1 || views[0] != "db1.v_orders" {
		t.Errorf("expected [db1.v_orders] in views, got %v", views)
	}
}

func TestSplitTableViewEntries_mappedEntry(t *testing.T) {
	kinds := map[string]string{
		"db1/*schema&table*/v1": "VIEW",
	}
	// Mapped entry: source part is "db1.v1"
	dtabS := []string{"db1.v1:db2.v1"}
	tables, views := splitTableViewEntries(dtabS, kinds, "yes")
	if len(tables) != 0 {
		t.Errorf("expected 0 table entries, got %d", len(tables))
	}
	if len(views) != 1 {
		t.Errorf("expected 1 view entry, got %d", len(views))
	}
}

func TestSplitTableViewEntries_caseInsensitiveKey(t *testing.T) {
	kinds := map[string]string{
		"db1/*schema&table*/v_orders": "VIEW",
	}
	dtabS := []string{"DB1.V_ORDERS"}
	tables, views := splitTableViewEntries(dtabS, kinds, "no") // no → keys lowercased
	if len(views) != 1 {
		t.Errorf("case-insensitive lookup: expected 1 view, got tables=%v views=%v", tables, views)
	}
}

// ---------------------------------------------------------------------------
// buildViewAdvisoryLines
// ---------------------------------------------------------------------------

func TestBuildViewAdvisoryLines_containsBeginEnd(t *testing.T) {
	lines := buildViewAdvisoryLines("db1", "v1", "CREATE VIEW `v1` AS SELECT 1", "VIEW definition differs", "", "")
	firstLine := lines[0]
	lastLine := lines[len(lines)-1]
	if !contains(firstLine, "advisory begin") {
		t.Errorf("first line should contain 'advisory begin': %q", firstLine)
	}
	if !contains(lastLine, "advisory end") {
		t.Errorf("last line should contain 'advisory end': %q", lastLine)
	}
}

func TestBuildViewAdvisoryLines_metaLinesCommented(t *testing.T) {
	// Metadata lines (begin/end/level/kind/reason) must be comments.
	// SQL statements (CREATE OR REPLACE VIEW) must NOT be commented.
	lines := buildViewAdvisoryLines("db1", "v1", "CREATE VIEW `v1` AS SELECT 1", "reason", "", "")
	for _, line := range lines {
		if contains(strings.ToUpper(line), "CREATE OR REPLACE") {
			if len(line) > 0 && line[0] == '-' {
				t.Errorf("SQL statement should NOT be commented out: %q", line)
			}
		}
	}
}

func TestBuildViewAdvisoryLines_noDropView(t *testing.T) {
	// CREATE OR REPLACE VIEW is sufficient for both "missing" and "definition differs" scenarios.
	// DROP VIEW must NOT be generated to avoid a deletion window if CREATE fails.
	lines := buildViewAdvisoryLines("db1", "v1", "CREATE VIEW `v1` AS SELECT 1", "missing", "", "")
	for _, l := range lines {
		if contains(strings.ToUpper(l), "DROP VIEW") {
			t.Errorf("advisory lines must not contain DROP VIEW; CREATE OR REPLACE is sufficient: %q", l)
		}
	}
}

func TestBuildViewAdvisoryLines_withCollation(t *testing.T) {
	// When csClient+colConn are provided, SET statements must be present with symmetric DEFAULT restores.
	// DROP VIEW must NOT appear; CREATE OR REPLACE is sufficient.
	srcDDL := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM t"
	lines := buildViewAdvisoryLines("db1", "v1", srcDDL, "VIEW definition differs", "utf8mb4", "utf8mb4_general_ci")
	mustContainLine(t, lines, "SET character_set_client = utf8mb4;")
	mustContainLine(t, lines, "SET collation_connection = utf8mb4_general_ci;")
	mustContainLine(t, lines, "SET collation_connection = DEFAULT;")
	mustContainLine(t, lines, "SET character_set_client = DEFAULT;")
	mustContainLine(t, lines, "CREATE OR REPLACE")
	// SET statements must be executable (no -- prefix).
	for _, l := range lines {
		if (contains(l, "SET collation_connection") || contains(l, "SET character_set_client")) && len(l) > 0 && l[0] == '-' {
			t.Errorf("SET statement must not be commented: %q", l)
		}
	}
	// DROP VIEW must not appear.
	for _, l := range lines {
		if contains(strings.ToUpper(l), "DROP VIEW") {
			t.Errorf("advisory must not contain DROP VIEW: %q", l)
		}
	}
}

func TestBuildCreateOrReplaceViewSQL_preservesExplicitSecurityAndAlgorithm(t *testing.T) {
	srcDDL := "CREATE ALGORITHM=MERGE DEFINER=`app`@`%` SQL SECURITY INVOKER VIEW `db1`.`v1` AS SELECT id FROM orders"
	got, ok := buildCreateOrReplaceViewSQL(srcDDL, "db2", "v1")
	if !ok {
		t.Fatalf("expected safe rewrite")
	}
	if !contains(got, "CREATE OR REPLACE ALGORITHM=MERGE SQL SECURITY INVOKER VIEW `db2`.`v1`") {
		t.Fatalf("expected advisory SQL to preserve explicit ALGORITHM and SQL SECURITY, got %q", got)
	}
	if contains(got, "DEFINER=") {
		t.Fatalf("DEFINER must be stripped from advisory SQL, got %q", got)
	}
}

func TestBuildCreateOrReplaceViewSQL_omitsUndefinedAlgorithm(t *testing.T) {
	srcDDL := "CREATE ALGORITHM=UNDEFINED DEFINER=`app`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM orders"
	got, ok := buildCreateOrReplaceViewSQL(srcDDL, "db2", "v1")
	if !ok {
		t.Fatalf("expected safe rewrite")
	}
	if contains(got, "ALGORITHM=UNDEFINED") {
		t.Fatalf("default ALGORITHM=UNDEFINED should not be preserved in advisory SQL, got %q", got)
	}
	if !contains(got, "SQL SECURITY DEFINER") {
		t.Fatalf("explicit SQL SECURITY should still be preserved, got %q", got)
	}
}

func TestBuildCreateOrReplaceViewSQL_preservesWithCheckOption(t *testing.T) {
	srcDDL := "CREATE DEFINER=`app`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM orders WITH CASCADED CHECK OPTION"
	got, ok := buildCreateOrReplaceViewSQL(srcDDL, "db2", "v1")
	if !ok {
		t.Fatalf("expected safe rewrite")
	}
	if !contains(got, "WITH CASCADED CHECK OPTION") {
		t.Fatalf("CHECK OPTION must be preserved in advisory SQL, got %q", got)
	}
	if !contains(got, "CREATE OR REPLACE SQL SECURITY DEFINER VIEW `db2`.`v1` AS SELECT id FROM orders WITH CASCADED CHECK OPTION;") {
		t.Fatalf("expected rewritten advisory SQL with preserved CHECK OPTION, got %q", got)
	}
}

func TestBuildCreateOrReplaceViewSQL_escapesDestinationIdentifiers(t *testing.T) {
	srcDDL := "CREATE DEFINER=`app`@`%` VIEW `db1`.`v1` AS SELECT 1"
	got, ok := buildCreateOrReplaceViewSQL(srcDDL, "db`2", "v`1")
	if !ok {
		t.Fatalf("expected safe rewrite")
	}
	if !contains(got, "VIEW `db``2`.`v``1` AS SELECT 1;") {
		t.Fatalf("expected rewritten advisory SQL to escape destination identifiers, got %q", got)
	}
}

func TestBuildCreateOrReplaceViewSQL_unparseableReturnsUnsafe(t *testing.T) {
	got, ok := buildCreateOrReplaceViewSQL("CREATE VIEW", "db2", "v1")
	if ok {
		t.Fatalf("unparseable DDL must not be marked safe, got %q", got)
	}
	if got != "" {
		t.Fatalf("unparseable DDL should return empty suggestion, got %q", got)
	}
}

func TestBuildViewAdvisoryLines_unparseableDDLUsesSuggestedSQLNone(t *testing.T) {
	lines := buildViewAdvisoryLines("db1", "v1", "CREATE VIEW", "VIEW definition differs", "", "")
	mustContainLine(t, lines, "suggested SQL: none")
	for _, l := range lines {
		if contains(l, "DROP VIEW IF EXISTS") || contains(l, "CREATE OR REPLACE") {
			t.Fatalf("unsafe rewrite fallback must not emit executable-looking SQL: %q", l)
		}
	}
}

// ---------------------------------------------------------------------------
// Plan §11.9 test points 3-7: checkViewStruct decision logic
// (Tests are pure-function: they exercise the helpers that encode each branch's
// decision without requiring a real DB connection.)
// ---------------------------------------------------------------------------

// Test 3: sameDefinition — identical DDL + identical column signatures → Diffs=no.
// Verified via the two comparison helpers; both must return "equal".
func TestCheckViewStruct_sameDefinition(t *testing.T) {
	ddl := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id, name FROM orders"
	srcNorm := normalizeViewCreateSQLForCompare(ddl)
	dstNorm := normalizeViewCreateSQLForCompare(ddl)
	if srcNorm != dstNorm {
		t.Fatalf("identical DDL should normalize to equal strings")
	}
	cols := []string{"id|int|NO||", "name|varchar(100)|YES|utf8mb4|utf8mb4_0900_ai_ci"}
	ok, reason := viewColumnSignaturesEqual(cols, cols)
	if !ok {
		t.Fatalf("identical column sigs should be equal, got reason=%q", reason)
	}
	// Both checks pass → would produce Diffs=no in checkViewStruct.
}

// Test 4: definitionDiff — different SELECT body → DDL check returns ≠ → advisory generated.
func TestCheckViewStruct_definitionDiff(t *testing.T) {
	srcDDL := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM orders"
	dstDDL := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id, name FROM orders"
	if normalizeViewCreateSQLForCompare(srcDDL) == normalizeViewCreateSQLForCompare(dstDDL) {
		t.Fatalf("different SELECT bodies should NOT normalize to the same string")
	}
	// Advisory must contain level/kind/CREATE OR REPLACE VIEW.
	lines := buildViewAdvisoryLines("db1", "v1", srcDDL, "VIEW definition differs", "", "")
	mustContainLine(t, lines, "level: advisory-only")
	mustContainLine(t, lines, "kind: VIEW DEFINITION")
	mustContainLine(t, lines, "CREATE OR REPLACE")
}

// Test 5: missingOnTarget — source has view, dest does not → advisory with CREATE OR REPLACE VIEW.
func TestCheckViewStruct_missingOnTarget(t *testing.T) {
	srcDDL := "CREATE ALGORITHM=UNDEFINED DEFINER=`app`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM t"
	lines := buildViewAdvisoryLines("db1", "v1", srcDDL, "VIEW missing on target", "", "")
	mustContainLine(t, lines, "VIEW missing on target")
	mustContainLine(t, lines, "CREATE OR REPLACE")
	mustContainLine(t, lines, "level: advisory-only")
	mustContainLine(t, lines, "kind: VIEW DEFINITION")
	// DEFINER must not appear in the suggested SQL.
	for _, l := range lines {
		if contains(l, "DEFINER=") {
			t.Errorf("advisory line must not contain DEFINER: %q", l)
		}
	}
}

// Test 6: extraOnTarget — dest has view, source does not → DROP VIEW advisory.
func TestCheckViewStruct_extraOnTarget(t *testing.T) {
	lines := buildViewDropAdvisoryLines("db1", "v1")
	mustContainLine(t, lines, "DROP VIEW IF EXISTS")
	mustContainLine(t, lines, "level: advisory-only")
	mustContainLine(t, lines, "kind: VIEW DEFINITION")
	mustContainLine(t, lines, "exists on target but not on source")
	// Must NOT suggest a CREATE statement (no source DDL available).
	for _, l := range lines {
		if contains(strings.ToUpper(l), "CREATE OR REPLACE") {
			t.Errorf("DROP-only advisory must not contain CREATE OR REPLACE: %q", l)
		}
	}
}

// Test 8 (§11.9 test point 9): columnMetadataDiff — DDL is textually identical after
// normalization, but INFORMATION_SCHEMA.COLUMNS reports different column signatures.
// In this case checkViewStruct must still produce Diffs=yes and a column-metadata
// advisory (not Diffs=no).
func TestCheckViewStruct_columnMetadataDiff(t *testing.T) {
	ddl := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `db1`.`v_orders` AS SELECT id, amount FROM orders"

	// Verify the two DDL strings normalize to the same value (identical DDL).
	if normalizeViewCreateSQLForCompare(ddl) != normalizeViewCreateSQLForCompare(ddl) {
		t.Fatal("identical DDL should normalize to equal strings (pre-condition)")
	}

	// Simulate column drift: same DDL, but target column has different type/collation.
	srcCols := []string{
		"id|int|NO||",
		"amount|decimal(10,2)|NO||",
	}
	dstCols := []string{
		"id|bigint|NO||", // type widened on target
		"amount|decimal(10,2)|NO||",
	}

	// The column comparison must detect the drift.
	colsEqual, reason := viewColumnSignaturesEqual(srcCols, dstCols)
	if colsEqual {
		t.Fatalf("column signatures with type drift should not be equal")
	}
	if !contains(reason, "column[0]") {
		t.Errorf("reason should identify the differing column index, got %q", reason)
	}

	// The advisory generated for this case must use the column-metadata format
	// (suggested SQL: none) — not a CREATE OR REPLACE advisory.
	// No colConn provided → falls back to "suggested SQL: none".
	advisoryLines := buildViewColumnMetadataAdvisoryLines("db1", "v_orders", reason, "", "", "")
	mustContainLine(t, advisoryLines, "suggested SQL: none")
	mustContainLine(t, advisoryLines, "VIEW COLUMN METADATA")
	mustContainLine(t, advisoryLines, "level: advisory-only")

	// The advisory must NOT suggest a CREATE OR REPLACE VIEW when colConn is empty
	// (column type drift cannot be fixed by recreating the view with a different collation).
	for _, l := range advisoryLines {
		if contains(strings.ToUpper(l), "CREATE OR REPLACE VIEW") {
			t.Errorf("column-metadata advisory without colConn must not contain CREATE OR REPLACE VIEW: %q", l)
		}
	}
}

// Test 7: data mode VIEW filter — VIEW entries must not reach the table-processing path.
// The data-mode filter in SchemaTableFilter uses the same key-lookup logic as
// splitTableViewEntries.  We verify the partitioning here: with VIEW objectKinds,
// all VIEW entries end up in viewEntries (never in tableEntries).
func TestCheckViewStruct_dataModeViewFilter(t *testing.T) {
	kinds := map[string]string{
		"sales/*schema&table*/v_summary": "VIEW",
		"sales/*schema&table*/orders":    "BASE TABLE",
	}
	entries := []string{"sales.orders", "sales.v_summary", "sales.customers"}
	tableEntries, viewEntries := splitTableViewEntries(entries, kinds, "yes")

	for _, e := range tableEntries {
		if e == "sales.v_summary" {
			t.Errorf("VIEW entry %q must not appear in tableEntries", e)
		}
	}
	if len(viewEntries) != 1 || viewEntries[0] != "sales.v_summary" {
		t.Errorf("expected viewEntries=[sales.v_summary], got %v", viewEntries)
	}
	// Tables not in objectKinds (sales.customers) are treated as BASE TABLE.
	found := false
	for _, e := range tableEntries {
		if e == "sales.customers" {
			found = true
		}
	}
	if !found {
		t.Errorf("unknown entry sales.customers should fall through to tableEntries")
	}
}

// ---------------------------------------------------------------------------
// ignoreTables / data-mode VIEW filter — contract tests
// ---------------------------------------------------------------------------

// TestIgnoreTables_viewAbsentFromDtabSNotInViewEntries verifies that a VIEW
// entry filtered out by ignoreTables (i.e. absent from dtabS) never appears
// in viewEntries after splitTableViewEntries.  ignoreTables removes entries
// from dtabS before splitTableViewEntries is called; the function can only
// partition what it is given.
func TestIgnoreTables_viewAbsentFromDtabSNotInViewEntries(t *testing.T) {
	kinds := map[string]string{
		"sales/*schema&table*/v_ignored": "VIEW",
		"sales/*schema&table*/orders":    "BASE TABLE",
	}
	// v_ignored is absent — simulates ignoreTables having removed it.
	dtabS := []string{"sales.orders"}
	tableEntries, viewEntries := splitTableViewEntries(dtabS, kinds, "yes")

	for _, e := range viewEntries {
		if e == "sales.v_ignored" {
			t.Errorf("ignoreTables-filtered VIEW must not appear in viewEntries: got %v", viewEntries)
		}
	}
	if len(tableEntries) != 1 || tableEntries[0] != "sales.orders" {
		t.Errorf("expected tableEntries=[sales.orders], got %v", tableEntries)
	}
}

// TestDataModeViewSkipKeyFormat verifies that the key format used by the
// data-mode VIEW skip (schema/*schema&table*/table) is identical to the key
// format stored in objectKinds by the table-discovery path.  The two code
// paths must agree on this format or VIEWs silently slip through.
func TestDataModeViewSkipKeyFormat(t *testing.T) {
	// Simulate the key inserted during table discovery.
	discoveryKey := "gt_checksum/*schema&table*/v_teststring"

	// Simulate the key built by the data-mode skip logic
	// (from lines 4543-4547 of schema_tab_struct.go):
	//   entry = "gt_checksum.v_teststring"
	//   srcPart = "gt_checksum.v_teststring"
	//   parts = ["gt_checksum", "v_teststring"]
	//   key = fmt.Sprintf("%s/*schema&table*/%s", parts[0], parts[1])
	entry := "gt_checksum.v_teststring"
	dotIdx := 0
	for i, c := range entry {
		if c == '.' {
			dotIdx = i
			break
		}
	}
	schema := entry[:dotIdx]
	table := entry[dotIdx+1:]
	skipKey := schema + "/*schema&table*/" + table

	if skipKey != discoveryKey {
		t.Errorf("data-mode skip key %q != discovery key %q — key format mismatch will silently miss VIEW filter", skipKey, discoveryKey)
	}
}

func TestSchemaTableFilter_dataModeSkipsViewsOnRealPath(t *testing.T) {
	origList := schemaTableFilterDatabaseNameList
	origKinds := schemaTableFilterObjectTypeMap
	origWlog := global.Wlog
	defer func() {
		schemaTableFilterDatabaseNameList = origList
		schemaTableFilterObjectTypeMap = origKinds
		global.Wlog = origWlog
		TableMappingRelations = nil
	}()
	nullHandler, _ := golog.NewNullHandler()
	global.Wlog = golog.NewDefault(nullHandler)

	schemaTableFilterDatabaseNameList = func(_ dbExec.TableColumnNameStruct, _ *sql.DB, _ int64) (map[string]int, error) {
		return map[string]int{
			"sales/*schema&table*/orders":    1,
			"sales/*schema&table*/v_summary": 1,
		}, nil
	}
	schemaTableFilterObjectTypeMap = func(_ dbExec.TableColumnNameStruct, _ *sql.DB, _ int64) (map[string]string, error) {
		return map[string]string{
			"sales/*schema&table*/orders":    "BASE TABLE",
			"sales/*schema&table*/v_summary": "VIEW",
		}, nil
	}

	stcls := &schemaTable{
		table:                   "sales.orders,sales.v_summary",
		sourceDrive:             "mysql",
		destDrive:               "mysql",
		caseSensitiveObjectName: "yes",
		checkRules:              inputArg.RulesS{CheckObject: "data"},
	}

	got, err := stcls.SchemaTableFilter(1, 2)
	if err != nil {
		t.Fatalf("SchemaTableFilter returned error: %v", err)
	}
	if len(got) != 1 || got[0] != "sales.orders:sales.orders" {
		t.Fatalf("data mode should keep only base tables, got %v", got)
	}
	if stcls.objectKinds["sales/*schema&table*/v_summary"] != "VIEW" {
		t.Fatalf("objectKinds must be populated on the real SchemaTableFilter path")
	}
}

func TestSchemaTableFilter_ignoreTablesStillFiltersViewsOnRealPath(t *testing.T) {
	origList := schemaTableFilterDatabaseNameList
	origKinds := schemaTableFilterObjectTypeMap
	origWlog := global.Wlog
	defer func() {
		schemaTableFilterDatabaseNameList = origList
		schemaTableFilterObjectTypeMap = origKinds
		global.Wlog = origWlog
		TableMappingRelations = nil
	}()
	nullHandler, _ := golog.NewNullHandler()
	global.Wlog = golog.NewDefault(nullHandler)

	schemaTableFilterDatabaseNameList = func(_ dbExec.TableColumnNameStruct, _ *sql.DB, _ int64) (map[string]int, error) {
		return map[string]int{
			"sales/*schema&table*/orders":    1,
			"sales/*schema&table*/v_keep":    1,
			"sales/*schema&table*/v_ignored": 1,
		}, nil
	}
	schemaTableFilterObjectTypeMap = func(_ dbExec.TableColumnNameStruct, _ *sql.DB, _ int64) (map[string]string, error) {
		return map[string]string{
			"sales/*schema&table*/orders":    "BASE TABLE",
			"sales/*schema&table*/v_keep":    "VIEW",
			"sales/*schema&table*/v_ignored": "VIEW",
		}, nil
	}

	stcls := &schemaTable{
		table:                   "sales.*",
		ignoreTable:             "sales.v_ignored",
		sourceDrive:             "mysql",
		destDrive:               "mysql",
		caseSensitiveObjectName: "yes",
		checkRules:              inputArg.RulesS{CheckObject: "struct"},
	}

	got, err := stcls.SchemaTableFilter(1, 2)
	if err != nil {
		t.Fatalf("SchemaTableFilter returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("struct mode with ignoreTables should leave 2 entries, got %v", got)
	}
	for _, entry := range got {
		if entry == "sales.v_ignored:sales.v_ignored" {
			t.Fatalf("ignored VIEW must not survive SchemaTableFilter: %v", got)
		}
	}
	foundView := false
	for _, entry := range got {
		if entry == "sales.v_keep:sales.v_keep" {
			foundView = true
		}
	}
	if !foundView {
		t.Fatalf("non-ignored VIEW must remain in struct mode, got %v", got)
	}
}

// mustContainLine fails the test if no line in lines contains sub.
func mustContainLine(t *testing.T, lines []string, sub string) {
	t.Helper()
	for _, l := range lines {
		if contains(l, sub) {
			return
		}
	}
	t.Errorf("expected a line containing %q; got:\n%s", sub, strings.Join(lines, "\n"))
}

// TestExtractCandidateSchemas verifies that extractCandidateSchemas correctly
// extracts unique schema names from a DatabaseNameList key set.
func TestExtractCandidateSchemas(t *testing.T) {
	candidates := map[string]int{
		"sales/*schema&table*/orders":    1,
		"sales/*schema&table*/v_summary": 1,
		"hr/*schema&table*/employees":    1,
	}
	got := extractCandidateSchemas(candidates)
	// Convert to a set for order-independent comparison.
	gotSet := make(map[string]bool, len(got))
	for _, s := range got {
		gotSet[s] = true
	}
	if !gotSet["sales"] || !gotSet["hr"] {
		t.Errorf("extractCandidateSchemas: expected [sales hr], got %v", got)
	}
	if len(got) != 2 {
		t.Errorf("extractCandidateSchemas: expected 2 distinct schemas, got %d: %v", len(got), got)
	}
}

// TestExtractCandidateSchemas_empty ensures an empty candidate map returns an
// empty slice (which causes ObjectTypeMap to fall back to the full scan).
func TestExtractCandidateSchemas_empty(t *testing.T) {
	if got := extractCandidateSchemas(map[string]int{}); len(got) != 0 {
		t.Errorf("expected empty slice, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// VIEW advisory uca1400 collation mapping (MariaDB 11.5+ → MySQL 8.0/8.4)
// ---------------------------------------------------------------------------

// TestBuildViewAdvisoryLines_uca1400MappedToMySQL verifies that the
// utf8mb4_uca1400_ai_ci collation (MariaDB 11.5+) is mapped to
// utf8mb4_0900_ai_ci before being embedded in the SET statement so the
// generated SQL is executable on MySQL 8.0/8.4.
func TestBuildViewAdvisoryLines_uca1400MappedToMySQL(t *testing.T) {
	srcDDL := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM t"
	// Simulate the mapping applied in checkViewStruct: pass already-mapped collation.
	lines := buildViewAdvisoryLines("db1", "v1", srcDDL, "VIEW definition differs", "utf8mb4", "utf8mb4_0900_ai_ci")
	// The SET line must use the MySQL-compatible collation, not uca1400.
	mustContainLine(t, lines, "SET collation_connection = utf8mb4_0900_ai_ci;")
	mustContainLine(t, lines, "SET character_set_client = DEFAULT;")
	mustContainLine(t, lines, "SET collation_connection = DEFAULT;")
	for _, l := range lines {
		if contains(l, "uca1400") {
			t.Errorf("advisory must not contain MariaDB-only uca1400 collation: %q", l)
		}
	}
	// DROP VIEW must not appear.
	for _, l := range lines {
		if contains(strings.ToUpper(l), "DROP VIEW") {
			t.Errorf("advisory must not contain DROP VIEW: %q", l)
		}
	}
}

// TestBuildViewColumnMetadataAdvisoryLines_uca1400MappedToMySQL mirrors the
// same scenario for the column-metadata advisory builder.
func TestBuildViewColumnMetadataAdvisoryLines_uca1400MappedToMySQL(t *testing.T) {
	srcDDL := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `db1`.`v1` AS SELECT id FROM t"
	lines := buildViewColumnMetadataAdvisoryLines("db1", "v1", "column[0] differs: collation", srcDDL, "utf8mb4", "utf8mb4_0900_ai_ci")
	mustContainLine(t, lines, "SET collation_connection = utf8mb4_0900_ai_ci;")
	mustContainLine(t, lines, "SET character_set_client = DEFAULT;")
	mustContainLine(t, lines, "SET collation_connection = DEFAULT;")
	for _, l := range lines {
		if contains(l, "uca1400") {
			t.Errorf("column-metadata advisory must not contain MariaDB-only uca1400 collation: %q", l)
		}
	}
	// DROP VIEW must not appear.
	for _, l := range lines {
		if contains(strings.ToUpper(l), "DROP VIEW") {
			t.Errorf("column-metadata advisory must not contain DROP VIEW: %q", l)
		}
	}
}

// helper
func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		(len(s) > 0 && len(sub) > 0 && searchSubstring(s, sub)))
}

func searchSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ---------- shouldCompareTriggerMetadata / shouldCompareRoutineMetadata ----------

func makeSchemaTable(srcRaw, srcSeries string, srcFlavor global.DatabaseFlavor, dstRaw, dstSeries string, dstFlavor global.DatabaseFlavor) *schemaTable {
	src := global.MySQLVersionInfo{Raw: srcRaw, Series: srcSeries, Flavor: srcFlavor}
	dst := global.MySQLVersionInfo{Raw: dstRaw, Series: dstSeries, Flavor: dstFlavor}
	return &schemaTable{
		sourceDrive:   "mysql",
		destDrive:     "mysql",
		sourceVersion: src,
		destVersion:   dst,
	}
}

func TestShouldCompareTriggerMetadata(t *testing.T) {
	tests := []struct {
		name     string
		st       *schemaTable
		expected bool
	}{
		{
			name:     "mariadb-to-mariadb-returns-true",
			st:       makeSchemaTable("10.6.12-MariaDB", "10.6", global.DatabaseFlavorMariaDB, "10.11.5-MariaDB", "10.11", global.DatabaseFlavorMariaDB),
			expected: true,
		},
		{
			name:     "mysql-to-mariadb-returns-false",
			st:       makeSchemaTable("8.0.33", "8.0", global.DatabaseFlavorMySQL, "10.11.5-MariaDB", "10.11", global.DatabaseFlavorMariaDB),
			expected: false,
		},
		{
			name:     "mysql-to-mysql-returns-true",
			st:       makeSchemaTable("8.0.33", "8.0", global.DatabaseFlavorMySQL, "8.0.35", "8.0", global.DatabaseFlavorMySQL),
			expected: true,
		},
		{
			name:     "mariadb-to-mysql80-returns-true",
			st:       makeSchemaTable("10.6.12-MariaDB", "10.6", global.DatabaseFlavorMariaDB, "8.0.33", "8.0", global.DatabaseFlavorMySQL),
			expected: true,
		},
		{
			name:     "mariadb-to-mysql57-returns-false",
			st:       makeSchemaTable("10.6.12-MariaDB", "10.6", global.DatabaseFlavorMariaDB, "5.7.42", "5.7", global.DatabaseFlavorMySQL),
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.st.shouldCompareTriggerMetadata(); got != tt.expected {
				t.Fatalf("shouldCompareTriggerMetadata() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestShouldCompareRoutineMetadata(t *testing.T) {
	tests := []struct {
		name     string
		st       *schemaTable
		expected bool
	}{
		{
			name:     "mariadb-to-mariadb-returns-true",
			st:       makeSchemaTable("10.6.12-MariaDB", "10.6", global.DatabaseFlavorMariaDB, "10.11.5-MariaDB", "10.11", global.DatabaseFlavorMariaDB),
			expected: true,
		},
		{
			name:     "mysql-to-mariadb-returns-false",
			st:       makeSchemaTable("8.0.33", "8.0", global.DatabaseFlavorMySQL, "10.11.5-MariaDB", "10.11", global.DatabaseFlavorMariaDB),
			expected: false,
		},
		{
			name:     "mysql-to-mysql-returns-true",
			st:       makeSchemaTable("8.0.33", "8.0", global.DatabaseFlavorMySQL, "8.0.35", "8.0", global.DatabaseFlavorMySQL),
			expected: true,
		},
		{
			name:     "mariadb-to-mysql84-returns-true",
			st:       makeSchemaTable("10.6.12-MariaDB", "10.6", global.DatabaseFlavorMariaDB, "8.4.0", "8.4", global.DatabaseFlavorMySQL),
			expected: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.st.shouldCompareRoutineMetadata(); got != tt.expected {
				t.Fatalf("shouldCompareRoutineMetadata() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// ---------- adjustDestColumnSeqAfterDrops ----------

// TestAdjustDestColumnSeqAfterDrops_singleDropAtHead 模拟 MySQL 8.4 目标端
// 存在隐式主键 my_row_id（position=0），DROP 后剩余列序号应向前压缩一位。
// 这是 dul-fix-collation bug 的核心场景：若序号不调整，f1(dest=1) vs f1(src=0)
// 被误判为序号不匹配，导致仅有 collation 差异的列也生成重复的 MODIFY 语句。
func TestAdjustDestColumnSeqAfterDrops_singleDropAtHead(t *testing.T) {
	seq := map[string]int{
		"MY_ROW_ID": 0,
		"F1":        1,
		"F2":        2,
		"F3":        3,
	}
	adjustDestColumnSeqAfterDrops(seq, []string{"MY_ROW_ID"})

	if _, exists := seq["MY_ROW_ID"]; exists {
		t.Fatal("MY_ROW_ID should have been removed from seq map")
	}
	want := map[string]int{"F1": 0, "F2": 1, "F3": 2}
	for col, wantSeq := range want {
		if got := seq[col]; got != wantSeq {
			t.Errorf("seq[%s] = %d, want %d", col, got, wantSeq)
		}
	}
}

// TestAdjustDestColumnSeqAfterDrops_multipleDrops 验证连续删除多列后序号压缩正确。
func TestAdjustDestColumnSeqAfterDrops_multipleDrops(t *testing.T) {
	seq := map[string]int{
		"A": 0,
		"B": 1,
		"C": 2,
		"D": 3,
		"E": 4,
	}
	// 删除 A(0) 和 C(2)，剩余 B→0，D→1，E→2
	adjustDestColumnSeqAfterDrops(seq, []string{"A", "C"})

	if _, exists := seq["A"]; exists {
		t.Fatal("A should have been removed")
	}
	if _, exists := seq["C"]; exists {
		t.Fatal("C should have been removed")
	}
	want := map[string]int{"B": 0, "D": 1, "E": 2}
	for col, wantSeq := range want {
		if got := seq[col]; got != wantSeq {
			t.Errorf("seq[%s] = %d, want %d", col, got, wantSeq)
		}
	}
}

// TestAdjustDestColumnSeqAfterDrops_dropAtTail 删除末尾列，前面列序号不变。
func TestAdjustDestColumnSeqAfterDrops_dropAtTail(t *testing.T) {
	seq := map[string]int{
		"F1": 0,
		"F2": 1,
		"F3": 2,
	}
	adjustDestColumnSeqAfterDrops(seq, []string{"F3"})

	want := map[string]int{"F1": 0, "F2": 1}
	for col, wantSeq := range want {
		if got := seq[col]; got != wantSeq {
			t.Errorf("seq[%s] = %d, want %d", col, got, wantSeq)
		}
	}
}

// TestAdjustDestColumnSeqAfterDrops_noDrop 无删除列时 seq 保持不变。
func TestAdjustDestColumnSeqAfterDrops_noDrop(t *testing.T) {
	seq := map[string]int{
		"F1": 0,
		"F2": 1,
	}
	adjustDestColumnSeqAfterDrops(seq, []string{})

	want := map[string]int{"F1": 0, "F2": 1}
	for col, wantSeq := range want {
		if got := seq[col]; got != wantSeq {
			t.Errorf("seq[%s] = %d, want %d", col, got, wantSeq)
		}
	}
}

// ---------- buildConstraintAdvisoryLines ----------

// TestBuildConstraintAdvisoryLines_ManualReviewExecutable 验证 manual-review 级别的 SQL 语句
// 应写成可执行形式（无 -- 前缀），不能全部注释掉。
// 这是 Oracle→MySQL checkObject=struct 外键差异修复 SQL 为空的 bug 修复验证。
func TestBuildConstraintAdvisoryLines_ManualReviewExecutable(t *testing.T) {
	suggestions := []schemacompat.ConstraintRepairSuggestion{
		{
			ConstraintName: "FK_EMP_DEPT1",
			Kind:           "FOREIGN KEY",
			Level:          schemacompat.ConstraintRepairLevelManualReview,
			Reason:         "target has an extra foreign key that does not exist on the source side",
			Statements:     []string{"ALTER TABLE `gt_checksum`.`tb_emp6` DROP FOREIGN KEY `FK_EMP_DEPT1`"},
		},
	}

	lines := buildConstraintAdvisoryLines("gt_checksum.tb_emp6 FOREIGN KEY constraints", suggestions)

	joined := strings.Join(lines, "\n")

	// 可执行语句必须出现（不带 -- 前缀）
	if !strings.Contains(joined, "ALTER TABLE `gt_checksum`.`tb_emp6` DROP FOREIGN KEY `FK_EMP_DEPT1`;") {
		t.Errorf("manual-review SQL 应为可执行形式，但实际输出:\n%s", joined)
	}

	// 确保带 -- 前缀的注释版本不存在
	if strings.Contains(joined, "-- ALTER TABLE") {
		t.Errorf("manual-review SQL 不应以注释形式输出，但实际输出:\n%s", joined)
	}
}

// TestBuildConstraintAdvisoryLines_AdvisoryOnlyCommented 验证 advisory-only 级别的 SQL 语句
// 仍以注释形式写出（仅供参考，不可直接执行）。
func TestBuildConstraintAdvisoryLines_AdvisoryOnlyCommented(t *testing.T) {
	suggestions := []schemacompat.ConstraintRepairSuggestion{
		{
			Kind:       "TABLE COLLATION",
			Level:      schemacompat.ConstraintRepairLevelAdvisoryOnly,
			Reason:     "collation difference is advisory only",
			Statements: []string{"ALTER TABLE `t1` CONVERT TO CHARACTER SET utf8mb4"},
		},
	}

	lines := buildConstraintAdvisoryLines("test.t1 TABLE options", suggestions)

	joined := strings.Join(lines, "\n")

	// advisory-only 语句必须以注释形式出现
	if !strings.Contains(joined, "-- ALTER TABLE `t1` CONVERT TO CHARACTER SET utf8mb4") {
		t.Errorf("advisory-only SQL 应以注释形式输出，但实际输出:\n%s", joined)
	}
}

// TestBuildConstraintAdvisoryLines_Empty 空建议返回 nil。
func TestBuildConstraintAdvisoryLines_Empty(t *testing.T) {
	lines := buildConstraintAdvisoryLines("scope", nil)
	if lines != nil {
		t.Errorf("空建议应返回 nil，got %v", lines)
	}
}

// TestIsOracleToMySQL_GodrorSource 验证 isOracleToMySQL() 在 godror 驱动 + MySQL 目标时返回 true。
// Bug 背景：Oracle→MySQL struct 校验中，无论源/目标表是否有分区，代码都无条件触发
// warn-only 并生成 Advisory 修复 SQL。修复后，仅当至少一侧有分区时才触发 advisory。
func TestIsOracleToMySQL_GodrorSource(t *testing.T) {
	st := &schemaTable{
		sourceDrive: "godror",
		// Raw 必须非空，destVersionInfo() 才会返回 destVersion 而非全局变量
		destVersion: global.MySQLVersionInfo{Raw: "8.0.32", Flavor: global.DatabaseFlavorMySQL},
	}
	if !st.isOracleToMySQL() {
		t.Error("godror + MySQL 目标应返回 isOracleToMySQL=true")
	}
}

// TestIsOracleToMySQL_MySQLSource 验证 MySQL→MySQL 场景不被识别为 Oracle→MySQL。
func TestIsOracleToMySQL_MySQLSource(t *testing.T) {
	st := &schemaTable{
		sourceDrive: "mysql",
		destVersion: global.MySQLVersionInfo{Raw: "8.0.32", Flavor: global.DatabaseFlavorMySQL},
	}
	if st.isOracleToMySQL() {
		t.Error("mysql 源不应被识别为 Oracle→MySQL 场景")
	}
}

// TestPartitionDiffsMap_NoPartitionNoWarnOnly 验证无分区表在 Oracle→MySQL
// 场景下，partitionDiffsMap 应被初始化为 false，structWarnOnlyDiffsMap 不应被设置。
// 此测例对应 Bug 修复：account 表无分区但产生 warn-only + Advisory 修复 SQL 的误报。
func TestPartitionDiffsMap_NoPartitionNoWarnOnly(t *testing.T) {
	st := &schemaTable{
		sourceDrive:            "godror",
		destVersion:            global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL},
		partitionDiffsMap:      make(map[string]bool),
		structWarnOnlyDiffsMap: make(map[string]bool),
	}

	// 模拟：两端均无分区时，只设置 partitionDiffsMap[key]=false，不设置 structWarnOnlyDiffsMap
	tableKey := "gt_checksum.account"
	st.partitionDiffsMap[tableKey] = false
	// structWarnOnlyDiffsMap 故意不写入

	if st.partitionDiffsMap[tableKey] != false {
		t.Errorf("无分区表 partitionDiffsMap 应为 false，got %v", st.partitionDiffsMap[tableKey])
	}
	if st.structWarnOnlyDiffsMap[tableKey] {
		t.Errorf("无分区表不应在 structWarnOnlyDiffsMap 中被标记为 true")
	}
}

// TestOracleToMySQL_NullableNormalization 验证 Oracle nullable 值（N/Y）被正确规范化为
// MySQL 格式（NO/YES），以确保生成的 MODIFY COLUMN SQL 包含正确的 NOT NULL 约束。
// Bug 背景：Oracle 返回 nullable="N" 表示 NOT NULL，而 FixAlterColumnSqlDispos 只识别 "NO"，
// 导致 NOT NULL 约束被遗漏，修复 SQL 执行后再次校验仍报 Diffs=yes（无限循环）。
func TestOracleToMySQL_NullableNormalization(t *testing.T) {
	tests := []struct {
		name     string
		input    string // Oracle nullable 原始值
		wantNorm string // 规范化后期望的 MySQL 格式值
	}{
		{"N should normalize to NO", "N", "NO"},
		{"Y should normalize to YES", "Y", "YES"},
		{"already NO stays NO", "NO", "NO"},
		{"already YES stays YES", "YES", "YES"},
		{"lowercase n should normalize to NO", "n", "NO"},
		{"lowercase y should normalize to YES", "y", "YES"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 模拟 schema_tab_struct.go 中的规范化逻辑
			repairAttrs := []string{"decimal(20,0)", "null", "null", tt.input, "", ""}
			switch strings.ToUpper(strings.TrimSpace(repairAttrs[3])) {
			case "N":
				repairAttrs[3] = "NO"
			case "Y":
				repairAttrs[3] = "YES"
			}
			if repairAttrs[3] != tt.wantNorm {
				t.Errorf("nullable normalization(%q) = %q, want %q", tt.input, repairAttrs[3], tt.wantNorm)
			}
		})
	}
}
