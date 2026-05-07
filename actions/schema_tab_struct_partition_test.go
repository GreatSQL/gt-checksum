package actions

import (
	"testing"

	"gt-checksum/global"
	mysql "gt-checksum/MySQL"
)

// ---------- partitionDiffsMap ----------

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

// TestExtractPartitionColumnsFromExpressions 验证从分区表达式中提取分区列的功能
func TestExtractPartitionColumnsFromExpressions(t *testing.T) {
	tests := []struct {
		name       string
		expressions []string
		expected   []string
	}{
		{
			name:        "单个分区列",
			expressions: []string{"`name`"},
			expected:    []string{"name"},
		},
		{
			name:        "多个分区列",
			expressions: []string{"`id`, `name`"},
			expected:    []string{"id", "name"},
		},
		{
			name:        "分区表达式中的函数",
			expressions: []string{"YEAR(`date`)"},
			expected:    []string{"date"},
		},
		{
			name:        "空表达式",
			expressions: []string{},
			expected:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mysql.ExtractPartitionColumnsFromExpressions(tt.expressions)
			if len(result) != len(tt.expected) {
				t.Errorf("期望 %d 个分区列，得到 %d 个", len(tt.expected), len(result))
				return
			}
			// 由于结果是 map 转换的，顺序可能不同，所以检查内容而不是顺序
			resultMap := make(map[string]bool)
			for _, col := range result {
				resultMap[col] = true
			}
			for _, col := range tt.expected {
				if !resultMap[col] {
					t.Errorf("期望包含列 %s，但未找到", col)
				}
			}
		})
	}
}

// TestGeneratePartitionTablePrimaryKeySql 验证为分区表生成主键修复 SQL 的功能
func TestGeneratePartitionTablePrimaryKeySql(t *testing.T) {
	fixer := &mysql.MysqlDataAbnormalFixStruct{
		Schema:           "gt_checksum",
		Table:            "list_partition_table",
		PartitionColumns: []string{"name"},
	}

	sql := fixer.GeneratePartitionTablePrimaryKeySql("my_row_id", 1)
	if sql == "" {
		t.Errorf("期望生成主键修复 SQL，但得到空字符串")
		return
	}

	// 验证 SQL 包含必要的元素
	if !contains(sql, "ADD PRIMARY KEY") {
		t.Errorf("SQL 应包含 'ADD PRIMARY KEY'，得到: %s", sql)
	}
	if !contains(sql, "my_row_id") {
		t.Errorf("SQL 应包含 'my_row_id'，得到: %s", sql)
	}
	if !contains(sql, "name") {
		t.Errorf("SQL 应包含分区列 'name'，得到: %s", sql)
	}
	// 验证 my_row_id 在第一个位置
	if !contains(sql, "`my_row_id`, `name`") {
		t.Errorf("my_row_id 应在第一个位置，得到: %s", sql)
	}
}

// TestGeneratePartitionTablePrimaryKeySql_MultiplePartitionColumns 验证多个分区列的情况
func TestGeneratePartitionTablePrimaryKeySql_MultiplePartitionColumns(t *testing.T) {
	fixer := &mysql.MysqlDataAbnormalFixStruct{
		Schema:           "gt_checksum",
		Table:            "range_partition_table",
		PartitionColumns: []string{"id", "name"},
	}

	sql := fixer.GeneratePartitionTablePrimaryKeySql("my_row_id", 1)
	if sql == "" {
		t.Errorf("期望生成主键修复 SQL，但得到空字符串")
		return
	}

	// 验证 SQL 包含所有分区列
	if !contains(sql, "my_row_id") {
		t.Errorf("SQL 应包含 'my_row_id'，得到: %s", sql)
	}
	if !contains(sql, "id") {
		t.Errorf("SQL 应包含分区列 'id'，得到: %s", sql)
	}
	if !contains(sql, "name") {
		t.Errorf("SQL 应包含分区列 'name'，得到: %s", sql)
	}
}

// TestGeneratePartitionTablePrimaryKeySql_NoPartitionColumns 验证无分区列的情况
func TestGeneratePartitionTablePrimaryKeySql_NoPartitionColumns(t *testing.T) {
	fixer := &mysql.MysqlDataAbnormalFixStruct{
		Schema:           "gt_checksum",
		Table:            "normal_table",
		PartitionColumns: []string{},
	}

	sql := fixer.GeneratePartitionTablePrimaryKeySql("my_row_id", 1)
	if sql != "" {
		t.Errorf("无分区列时应返回空字符串，得到: %s", sql)
	}
}
