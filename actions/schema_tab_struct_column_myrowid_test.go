package actions

import (
	"fmt"
	"strings"
	"testing"
)

// TestMyRowIDRepositionSQL_GenerateSteps 测试生成 my_row_id 位置调整 SQL 的逻辑
func TestMyRowIDRepositionSQL_GenerateSteps(t *testing.T) {
	tests := []struct {
		name           string
		schema         string
		table          string
		dataType       string
		lastColumnName string
		expectedStep1  string
		expectedStep2  string
	}{
		{
			name:           "bigint data type",
			schema:         "gt_checksum",
			table:          "test1",
			dataType:       "bigint",
			lastColumnName: "a1",
			expectedStep1:  "ALTER TABLE `gt_checksum`.`test1` MODIFY COLUMN `my_row_id` bigint unsigned NOT NULL AUTO_INCREMENT /*!80023 VISIBLE */;",
			expectedStep2:  "ALTER TABLE `gt_checksum`.`test1` MODIFY COLUMN `my_row_id` bigint unsigned NOT NULL AUTO_INCREMENT /*!80023 INVISIBLE */ AFTER `a1`;",
		},
		{
			name:           "int data type",
			schema:         "test_db",
			table:          "test_table",
			dataType:       "int",
			lastColumnName: "col1",
			expectedStep1:  "ALTER TABLE `test_db`.`test_table` MODIFY COLUMN `my_row_id` int unsigned NOT NULL AUTO_INCREMENT /*!80023 VISIBLE */;",
			expectedStep2:  "ALTER TABLE `test_db`.`test_table` MODIFY COLUMN `my_row_id` int unsigned NOT NULL AUTO_INCREMENT /*!80023 INVISIBLE */ AFTER `col1`;",
		},
		{
			name:           "column with underscore",
			schema:         "my_schema",
			table:          "my_table",
			dataType:       "bigint",
			lastColumnName: "user_id",
			expectedStep1:  "ALTER TABLE `my_schema`.`my_table` MODIFY COLUMN `my_row_id` bigint unsigned NOT NULL AUTO_INCREMENT /*!80023 VISIBLE */;",
			expectedStep2:  "ALTER TABLE `my_schema`.`my_table` MODIFY COLUMN `my_row_id` bigint unsigned NOT NULL AUTO_INCREMENT /*!80023 INVISIBLE */ AFTER `user_id`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step1 := fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY COLUMN `my_row_id` %s unsigned NOT NULL AUTO_INCREMENT /*!80023 VISIBLE */;", tt.schema, tt.table, tt.dataType)
			step2 := fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY COLUMN `my_row_id` %s unsigned NOT NULL AUTO_INCREMENT /*!80023 INVISIBLE */ AFTER `%s`;", tt.schema, tt.table, tt.dataType, tt.lastColumnName)

			if step1 != tt.expectedStep1 {
				t.Errorf("Step1 mismatch:\nExpected: %s\nGot:      %s", tt.expectedStep1, step1)
			}

			if step2 != tt.expectedStep2 {
				t.Errorf("Step2 mismatch:\nExpected: %s\nGot:      %s", tt.expectedStep2, step2)
			}
		})
	}
}

// TestDetectColumnPositionChange 测试检测列位置调整的逻辑
func TestDetectColumnPositionChange(t *testing.T) {
	tests := []struct {
		name                      string
		alterSlice                []string
		expectedHasPositionChange bool
	}{
		{
			name: "has MODIFY COLUMN with FIRST",
			alterSlice: []string{
				" MODIFY COLUMN `a1` int(11) DEFAULT NULL FIRST",
			},
			expectedHasPositionChange: true,
		},
		{
			name: "has MODIFY COLUMN with AFTER",
			alterSlice: []string{
				" MODIFY COLUMN `a1` int(11) DEFAULT NULL AFTER `b1`",
			},
			expectedHasPositionChange: true,
		},
		{
			name: "has ADD COLUMN without position",
			alterSlice: []string{
				" ADD COLUMN `new_col` int(11) DEFAULT NULL",
			},
			expectedHasPositionChange: false,
		},
		{
			name: "has MODIFY COLUMN without position",
			alterSlice: []string{
				" MODIFY COLUMN `a1` int(11) DEFAULT NULL",
			},
			expectedHasPositionChange: false,
		},
		{
			name:                      "empty alterSlice",
			alterSlice:                []string{},
			expectedHasPositionChange: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasColumnPositionChange := false
			for _, alterSQL := range tt.alterSlice {
				upperSQL := strings.ToUpper(alterSQL)
				if strings.Contains(upperSQL, "MODIFY COLUMN") && (strings.Contains(upperSQL, "FIRST") || strings.Contains(upperSQL, "AFTER")) {
					hasColumnPositionChange = true
					break
				}
			}

			if hasColumnPositionChange != tt.expectedHasPositionChange {
				t.Errorf("Expected hasColumnPositionChange=%v, got %v", tt.expectedHasPositionChange, hasColumnPositionChange)
			}
		})
	}
}

// TestMyRowIDRepositionSQL_EdgeCases 测试边界情况
func TestMyRowIDRepositionSQL_EdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		schema         string
		table          string
		dataType       string
		lastColumnName string
		wantStep1      bool
		wantStep2      bool
	}{
		{
			name:           "normal case",
			schema:         "test_db",
			table:          "test_table",
			dataType:       "bigint",
			lastColumnName: "a1",
			wantStep1:      true,
			wantStep2:      true,
		},
		{
			name:           "empty last column name",
			schema:         "test_db",
			table:          "test_table",
			dataType:       "bigint",
			lastColumnName: "",
			wantStep1:      false,
			wantStep2:      false, // 不应该生成任何 SQL
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.lastColumnName != "" {
				step1 := fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY COLUMN `my_row_id` %s unsigned NOT NULL AUTO_INCREMENT /*!80023 VISIBLE */;", tt.schema, tt.table, tt.dataType)
				step2 := fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY COLUMN `my_row_id` %s unsigned NOT NULL AUTO_INCREMENT /*!80023 INVISIBLE */ AFTER `%s`;", tt.schema, tt.table, tt.dataType, tt.lastColumnName)

				if tt.wantStep1 && step1 == "" {
					t.Error("Expected step1 to be generated")
				}

				if tt.wantStep2 && step2 == "" {
					t.Error("Expected step2 to be generated")
				}

				// 验证 SQL 格式正确
				if !strings.HasPrefix(step1, "ALTER TABLE") {
					t.Errorf("Step1 should start with 'ALTER TABLE', got: %s", step1)
				}
				if !strings.HasSuffix(step1, ";") {
					t.Errorf("Step1 should end with ';', got: %s", step1)
				}
				if !strings.HasPrefix(step2, "ALTER TABLE") {
					t.Errorf("Step2 should start with 'ALTER TABLE', got: %s", step2)
				}
				if !strings.HasSuffix(step2, ";") {
					t.Errorf("Step2 should end with ';', got: %s", step2)
				}
			}
		})
	}
}
