package mysql

import (
	"testing"
)

// TestCollectDroppedColumns_PhantomMyRowID 测试 collectDroppedColumns 函数
// 能够正确解析包含多个 DROP COLUMN 操作的 ALTER TABLE 语句
// 这是针对 phantom-myrowid bug 的专项测例
func TestCollectDroppedColumns_PhantomMyRowID(t *testing.T) {
	tests := []struct {
		name              string
		columnOperations  []string
		expectedDropped   []string
		unexpectedDropped []string
	}{
		{
			name: "单个 DROP COLUMN 操作",
			columnOperations: []string{
				"ALTER TABLE `test`.`table1` DROP COLUMN `f9`;",
			},
			expectedDropped:   []string{"F9"},
			unexpectedDropped: []string{"MY_ROW_ID"},
		},
		{
			name: "多个 DROP COLUMN 操作（逗号分隔）",
			columnOperations: []string{
				"ALTER TABLE `test`.`table1` DROP COLUMN `f9`, DROP COLUMN `my_row_id`;",
			},
			expectedDropped:   []string{"F9", "MY_ROW_ID"},
			unexpectedDropped: []string{},
		},
		{
			name: "包含多个操作的完整 ALTER TABLE 语句（phantom-myrowid 场景）",
			columnOperations: []string{
				"ALTER TABLE `gt_checksum`.`teststring` DROP COLUMN `f9`, DROP COLUMN `my_row_id`, DROP PRIMARY KEY, ADD COLUMN `id` int(10) unsigned auto_increment NOT NULL COMMENT '' PRIMARY KEY FIRST;",
			},
			expectedDropped:   []string{"F9", "MY_ROW_ID"},
			unexpectedDropped: []string{"ID"},
		},
		{
			name: "多个独立的 DROP COLUMN 操作",
			columnOperations: []string{
				"DROP COLUMN `f9`",
				"DROP COLUMN `my_row_id`",
			},
			expectedDropped:   []string{"F9", "MY_ROW_ID"},
			unexpectedDropped: []string{},
		},
		{
			name: "混合场景：包含 DROP COLUMN 和其他操作",
			columnOperations: []string{
				"ALTER TABLE `test`.`table1` DROP COLUMN `col1`, MODIFY COLUMN `col2` int, DROP COLUMN `col3`;",
			},
			expectedDropped:   []string{"COL1", "COL3"},
			unexpectedDropped: []string{"COL2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			droppedColumns := collectDroppedColumns(tt.columnOperations)

			// 检查期望被识别的列
			for _, col := range tt.expectedDropped {
				if _, exists := droppedColumns[col]; !exists {
					t.Errorf("Expected column %q to be in droppedColumns, but it was not found", col)
				}
			}

			// 检查不应该被识别的列
			for _, col := range tt.unexpectedDropped {
				if _, exists := droppedColumns[col]; exists {
					t.Errorf("Column %q should not be in droppedColumns, but it was found", col)
				}
			}
		})
	}
}
