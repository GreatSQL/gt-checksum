package actions

import (
	"testing"

	"gt-checksum/global"
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
