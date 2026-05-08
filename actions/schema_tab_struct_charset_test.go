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

// TestCanUseTableCharsetConvertForColumnCollationDrift_CrossVersion 测试跨版本场景下的表级 COLLATE 修复优化
// 场景：MySQL 5.6 → MySQL 8.0，表级 COLLATE 不一致，但字段无显式定义
func TestCanUseTableCharsetConvertForColumnCollationDrift_CrossVersion(t *testing.T) {
	// 模拟 MySQL 5.6 → 8.0 场景
	// 源端：CHARSET=utf8mb4（无显式 COLLATE，隐式使用 utf8mb4_general_ci）
	// 目标端：CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
	sourceMeta := mysqlTableLevelMetadata{
		TableCharset:   "utf8mb4",
		TableCollation: "utf8mb4_general_ci", // MySQL 5.6 默认值
	}
	destMeta := mysqlTableLevelMetadata{
		TableCharset:   "utf8mb4",
		TableCollation: "utf8mb4_0900_ai_ci", // MySQL 8.0 默认值
	}

	// 字段定义中无显式 CHARSET/COLLATE
	sourceColumnDefinitions := map[string]string{
		"doc_code":     "char(20) NOT NULL DEFAULT '' COMMENT '文档固定编号'",
		"title":        "varchar(150) NOT NULL DEFAULT '' COMMENT '文章标题'",
		"author_desc":  "varchar(255) DEFAULT NULL COMMENT '作者简介'",
		"full_content": "text COMMENT '文章正文'",
	}

	// 所有字段的 COLLATE 差异都是由表级 COLLATE 差异引起的
	candidates := []columnCollationRepairCandidate{
		{
			ColumnName:       "doc_code",
			SourceCharset:    "utf8mb4",
			SourceCollation:  "utf8mb4_general_ci",
			DestCharset:      "utf8mb4",
			DestCollation:    "utf8mb4_0900_ai_ci",
			SourceDefinition: "char(20) NOT NULL DEFAULT '' COMMENT '文档固定编号'",
		},
		{
			ColumnName:       "title",
			SourceCharset:    "utf8mb4",
			SourceCollation:  "utf8mb4_general_ci",
			DestCharset:      "utf8mb4",
			DestCollation:    "utf8mb4_0900_ai_ci",
			SourceDefinition: "varchar(150) NOT NULL DEFAULT '' COMMENT '文章标题'",
		},
	}

	result := canUseTableCharsetConvertForColumnCollationDrift(sourceMeta, destMeta, sourceColumnDefinitions, candidates)
	if !result {
		t.Errorf("Expected canUseTableCharsetConvertForColumnCollationDrift to return true for cross-version scenario, got false")
	}
}

// TestCanUseTableCharsetConvertForColumnCollationDrift_ExplicitColumnCollation 测试字段显式定义 COLLATE 的场景
// 场景：字段显式定义了 COLLATE，不应使用表级 CONVERT TO
func TestCanUseTableCharsetConvertForColumnCollationDrift_ExplicitColumnCollation(t *testing.T) {
	sourceMeta := mysqlTableLevelMetadata{
		TableCharset:   "utf8mb4",
		TableCollation: "utf8mb4_general_ci",
	}
	destMeta := mysqlTableLevelMetadata{
		TableCharset:   "utf8mb4",
		TableCollation: "utf8mb4_0900_ai_ci",
	}

	// 字段定义中显式指定了 COLLATE
	sourceColumnDefinitions := map[string]string{
		"doc_code": "char(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL",
	}

	candidates := []columnCollationRepairCandidate{
		{
			ColumnName:       "doc_code",
			SourceCharset:    "utf8mb4",
			SourceCollation:  "utf8mb4_bin",
			DestCharset:      "utf8mb4",
			DestCollation:    "utf8mb4_0900_ai_ci",
			SourceDefinition: "char(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL",
		},
	}

	result := canUseTableCharsetConvertForColumnCollationDrift(sourceMeta, destMeta, sourceColumnDefinitions, candidates)
	if result {
		t.Errorf("Expected canUseTableCharsetConvertForColumnCollationDrift to return false when column has explicit COLLATE, got true")
	}
}
