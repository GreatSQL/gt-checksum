package actions

import (
	"testing"
)

// ---------- isCharsetMetadataCollationMapped / hasCharsetMetadataCollationDiff ----------

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
