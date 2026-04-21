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


// ---------- shouldCompareRoutineMetadata ----------

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
