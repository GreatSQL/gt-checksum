package mysql

import (
	"strings"
	"testing"
)

// TestFixAlterColumnAndIndexSqlGenerate_AutoIncrementSeparation 测试 AUTO_INCREMENT 操作能够独立生成
// 这是针对 bug fix-auto-id 的回归测试
func TestFixAlterColumnAndIndexSqlGenerate_AutoIncrementSeparation(t *testing.T) {
	my := &MysqlDataAbnormalFixStruct{
		Schema: "gt_checksum",
		Table:  "teststring",
	}

	// 模拟场景：列操作包含 DROP COLUMN, ADD COLUMN, MODIFY COLUMN 和 AUTO_INCREMENT
	columnOperations := []string{
		"ALTER TABLE `gt_checksum`.`teststring` DROP COLUMN `my_row_id`;",
		"ALTER TABLE `gt_checksum`.`teststring` DROP COLUMN `f9`;",
		"ALTER TABLE `gt_checksum`.`teststring` ADD COLUMN `id` int(10) unsigned auto_increment NOT NULL PRIMARY KEY FIRST;",
		"ALTER TABLE `gt_checksum`.`teststring` MODIFY COLUMN `f1` char(1) DEFAULT NULL;",
		"ALTER TABLE `gt_checksum`.`teststring` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;",
		"ALTER TABLE `gt_checksum`.`teststring` AUTO_INCREMENT=4;",
	}

	// 模拟场景：索引操作包含 DROP INDEX 和 ADD INDEX
	indexOperations := []string{
		"ALTER TABLE `gt_checksum`.`teststring` DROP INDEX `idx_teststring_1`;",
		"ALTER TABLE `gt_checksum`.`teststring` ADD INDEX `idx_teststring_f2`(`f2`);",
	}

	result := my.FixAlterColumnAndIndexSqlGenerate(columnOperations, indexOperations, 1)

	// 验证结果
	if len(result) < 2 {
		t.Fatalf("Expected at least 2 SQL statements, got %d", len(result))
	}

	// 验证第一条 SQL：应该包含所有常规操作（列、索引、字符集转换），但不包含 AUTO_INCREMENT
	firstSQL := result[0]
	if !strings.Contains(firstSQL, "DROP COLUMN") {
		t.Errorf("First SQL should contain DROP COLUMN operation")
	}
	if !strings.Contains(firstSQL, "ADD COLUMN") {
		t.Errorf("First SQL should contain ADD COLUMN operation")
	}
	if !strings.Contains(firstSQL, "DROP INDEX") {
		t.Errorf("First SQL should contain DROP INDEX operation")
	}
	if !strings.Contains(firstSQL, "ADD INDEX") {
		t.Errorf("First SQL should contain ADD INDEX operation")
	}
	if !strings.Contains(firstSQL, "CONVERT TO CHARACTER SET") {
		t.Errorf("First SQL should contain CONVERT TO CHARACTER SET operation")
	}

	// 关键验证：第一条 SQL 不应该包含 AUTO_INCREMENT
	if strings.Contains(firstSQL, "AUTO_INCREMENT=") {
		t.Errorf("First SQL should NOT contain AUTO_INCREMENT operation, but got: %s", firstSQL)
	}

	// 验证最后一条 SQL：应该是独立的 AUTO_INCREMENT 操作
	lastSQL := result[len(result)-1]
	if !strings.Contains(lastSQL, "AUTO_INCREMENT=4") {
		t.Errorf("Last SQL should contain AUTO_INCREMENT=4, got: %s", lastSQL)
	}

	// 验证最后一条 SQL 只包含 AUTO_INCREMENT 操作，不包含其他操作
	if strings.Contains(lastSQL, "DROP COLUMN") || strings.Contains(lastSQL, "ADD COLUMN") ||
		strings.Contains(lastSQL, "MODIFY COLUMN") || strings.Contains(lastSQL, "DROP INDEX") ||
		strings.Contains(lastSQL, "ADD INDEX") || strings.Contains(lastSQL, "CONVERT TO") {
		t.Errorf("Last SQL should only contain AUTO_INCREMENT operation, but got: %s", lastSQL)
	}

	t.Logf("Generated SQL statements:")
	for i, sql := range result {
		t.Logf("  [%d] %s", i+1, sql)
	}
}

// TestIsAutoIncrementOnlyOperation 测试 isAutoIncrementOnlyOperation 函数
func TestIsAutoIncrementOnlyOperation(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "Pure AUTO_INCREMENT operation",
			input:    "ALTER TABLE `gt_checksum`.`teststring` AUTO_INCREMENT=4;",
			expected: true,
		},
		{
			name:     "AUTO_INCREMENT with other operations",
			input:    "ALTER TABLE `gt_checksum`.`teststring` DROP COLUMN `f1`, AUTO_INCREMENT=4;",
			expected: false,
		},
		{
			name:     "AUTO_INCREMENT with CONVERT TO",
			input:    "ALTER TABLE `gt_checksum`.`teststring` CONVERT TO CHARACTER SET utf8mb4, AUTO_INCREMENT=4;",
			expected: false,
		},
		{
			name:     "Not an ALTER TABLE statement",
			input:    "CREATE TABLE test (id INT AUTO_INCREMENT);",
			expected: false,
		},
		{
			name:     "No AUTO_INCREMENT",
			input:    "ALTER TABLE `gt_checksum`.`teststring` DROP COLUMN `f1`;",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isAutoIncrementOnlyOperation(tt.input)
			if result != tt.expected {
				t.Errorf("isAutoIncrementOnlyOperation(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestIsSpatialColumnType 验证空间类型识别函数覆盖所有 MySQL 空间类型。
// 回归测试：fix-sp-idx — SPATIAL KEY 修复 SQL 不应携带前缀长度。
func TestIsSpatialColumnType(t *testing.T) {
	spatialTypes := []string{
		"point", "Point", "POINT",
		"geometry", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection", "geomcollection",
	}
	for _, ct := range spatialTypes {
		if !isSpatialColumnType(ct) {
			t.Errorf("isSpatialColumnType(%q) = false, want true", ct)
		}
	}

	nonSpatialTypes := []string{"varchar(100)", "int", "text", "blob", "json", "char(10)"}
	for _, ct := range nonSpatialTypes {
		if isSpatialColumnType(ct) {
			t.Errorf("isSpatialColumnType(%q) = true, want false", ct)
		}
	}
}
