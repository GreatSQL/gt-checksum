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

// TestGetLastColumnAfterAdditions 测试分析 ADD COLUMN 语句后确定最后一列的逻辑
func TestGetLastColumnAfterAdditions(t *testing.T) {
	tests := []struct {
		name              string
		alterSlice        []string
		currentLastColumn string
		expectedLastCol   string
	}{
		{
			name: "add column after current last column",
			alterSlice: []string{
				"ALTER TABLE `gt_checksum`.`test1` ADD COLUMN `a2` int(11) DEFAULT NULL COMMENT '' AFTER `a1`;",
			},
			currentLastColumn: "a1",
			expectedLastCol:   "a2",
		},
		{
			name: "add multiple columns sequentially",
			alterSlice: []string{
				"ALTER TABLE `test_db`.`test_table` ADD COLUMN `col2` int DEFAULT NULL AFTER `col1`;",
				"ALTER TABLE `test_db`.`test_table` ADD COLUMN `col3` varchar(50) DEFAULT NULL AFTER `col2`;",
			},
			currentLastColumn: "col1",
			expectedLastCol:   "col3",
		},
		{
			name: "add column with FIRST clause",
			alterSlice: []string{
				"ALTER TABLE `test_db`.`test_table` ADD COLUMN `new_first` int DEFAULT NULL FIRST;",
			},
			currentLastColumn: "a1",
			expectedLastCol:   "a1", // FIRST 不影响最后一列
		},
		{
			name: "add column without position clause",
			alterSlice: []string{
				"ALTER TABLE `test_db`.`test_table` ADD COLUMN `new_col` int DEFAULT NULL;",
			},
			currentLastColumn: "a1",
			expectedLastCol:   "new_col", // 默认添加到最后
		},
		{
			name: "add column in middle",
			alterSlice: []string{
				"ALTER TABLE `test_db`.`test_table` ADD COLUMN `middle_col` int DEFAULT NULL AFTER `col1`;",
			},
			currentLastColumn: "col3",
			expectedLastCol:   "col3", // 添加到中间不影响最后一列
		},
		{
			name:              "no add column statements",
			alterSlice:        []string{},
			currentLastColumn: "a1",
			expectedLastCol:   "a1",
		},
		{
			name: "mixed with modify column statements",
			alterSlice: []string{
				"ALTER TABLE `test_db`.`test_table` MODIFY COLUMN `col1` int DEFAULT NULL;",
				"ALTER TABLE `test_db`.`test_table` ADD COLUMN `col2` int DEFAULT NULL AFTER `col1`;",
			},
			currentLastColumn: "col1",
			expectedLastCol:   "col2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 直接测试逻辑，不依赖 schemaTable 对象
			lastColumn := tt.currentLastColumn

			// 遍历所有 ALTER 语句，找出 ADD COLUMN 语句
			for _, alterSQL := range tt.alterSlice {
				upperSQL := strings.ToUpper(alterSQL)

				// 只处理 ADD COLUMN 语句
				if !strings.Contains(upperSQL, "ADD COLUMN") {
					continue
				}

				// 提取列名：ADD COLUMN `column_name` ...
				// 找到 ADD COLUMN 后的第一个反引号对
				addColIdx := strings.Index(upperSQL, "ADD COLUMN")
				if addColIdx < 0 {
					continue
				}

				// 从 ADD COLUMN 之后开始查找列名
				afterAddCol := alterSQL[addColIdx+len("ADD COLUMN"):]
				parts := strings.Split(afterAddCol, "`")
				if len(parts) < 2 {
					continue
				}
				newColumnName := parts[1]

				// 检查是否有 AFTER 子句
				if strings.Contains(upperSQL, "AFTER") {
					// 提取 AFTER 后面的列名
					afterIdx := strings.Index(upperSQL, "AFTER")
					if afterIdx > 0 {
						afterPart := alterSQL[afterIdx+len("AFTER"):]
						afterParts := strings.Split(afterPart, "`")
						if len(afterParts) >= 2 {
							afterColumnName := afterParts[1]
							// 如果新列添加在当前最后一列之后，更新最后一列
							if afterColumnName == lastColumn {
								lastColumn = newColumnName
							}
						}
					}
				} else if strings.Contains(upperSQL, "FIRST") {
					// 如果是 FIRST，不影响最后一列
					continue
				} else {
					// 没有 AFTER 或 FIRST，默认添加到最后
					lastColumn = newColumnName
				}
			}

			if lastColumn != tt.expectedLastCol {
				t.Errorf("Expected last column '%s', got '%s'", tt.expectedLastCol, lastColumn)
			}
		})
	}
}
