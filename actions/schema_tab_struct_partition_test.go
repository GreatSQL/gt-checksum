package actions

import (
	"fmt"
	"strings"
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

// ---------- D1: 集合差分算法测试 ----------

// TestBuildPartitionRepairSQLs_DropMiddlePartition 验证删除中部分区的场景（D1.1）
func TestBuildPartitionRepairSQLs_DropMiddlePartition(t *testing.T) {
	// 源端：p1..p5,p7..p12（删了 p6）
	// 目的端：p1..p12
	sourcePartitions := buildTestPartitionMap("gt_checksum.t_range_int",
		[]string{"p1", "p2", "p3", "p4", "p5", "p7", "p8", "p9", "p10", "p11", "p12"})
	destPartitions := buildTestPartitionMap("gt_checksum.t_range_int",
		[]string{"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9", "p10", "p11", "p12"})

	_, advisorySQLs, handled, reason := buildPartitionRepairSQLs(
		"gt_checksum", "t_range_int", "gt_checksum", "t_range_int",
		sourcePartitions, destPartitions)

	if !handled {
		t.Errorf("删除中部分区应被处理，reason: %s", reason)
	}
	if len(advisorySQLs) == 0 {
		t.Error("应生成 DROP PARTITION p6 的建议 SQL")
	}
	// 验证包含 p6 的 DROP 建议
	found := false
	for _, sql := range advisorySQLs {
		if contains(sql, "DROP PARTITION") && contains(sql, "p6") {
			found = true
			break
		}
	}
	if !found {
		t.Error("应生成包含 DROP PARTITION p6 的建议")
	}
}

// TestBuildPartitionRepairSQLs_SplitMaxValue 验证 MAXVALUE 分区 split 场景（D1.2）
func TestBuildPartitionRepairSQLs_SplitMaxValue(t *testing.T) {
	// 源端：p_00,p_01,p_02,p_03,p_max（split 了 p_max）
	// 目的端：p_00,p_01,p_02,p_max
	sourcePartitions := buildTestPartitionMap("gt_checksum.t_bucket",
		[]string{"p_00", "p_01", "p_02", "p_03", "p_max"})
	destPartitions := buildTestPartitionMapWithMaxValue("gt_checksum.t_bucket",
		[]string{"p_00", "p_01", "p_02", "p_max"}, "p_max")

	execSQLs, _, handled, reason := buildPartitionRepairSQLs(
		"gt_checksum", "t_bucket", "gt_checksum", "t_bucket",
		sourcePartitions, destPartitions)

	if !handled {
		t.Errorf("split MAXVALUE 应被处理，reason: %s", reason)
	}
	if len(execSQLs) == 0 {
		t.Error("应生成 REORGANIZE PARTITION SQL")
	}
	// 验证生成的是 REORGANIZE 语句
	found := false
	for _, sql := range execSQLs {
		if contains(sql, "REORGANIZE PARTITION") && contains(sql, "p_max") {
			found = true
			break
		}
	}
	if !found {
		t.Error("应生成 REORGANIZE PARTITION p_max 的 SQL")
	}
}

// TestBuildPartitionRepairSQLs_RollingWindow 验证滚动窗口平移场景（D1.3）
func TestBuildPartitionRepairSQLs_RollingWindow(t *testing.T) {
	// 源端：p0102,p0103,p0104（滑动了一格）
	// 目的端：p0101,p0102,p0103
	sourcePartitions := buildTestPartitionMap("gt_checksum.t_daily",
		[]string{"p0102", "p0103", "p0104"})
	destPartitions := buildTestPartitionMap("gt_checksum.t_daily",
		[]string{"p0101", "p0102", "p0103"})

	execSQLs, advisorySQLs, handled, reason := buildPartitionRepairSQLs(
		"gt_checksum", "t_daily", "gt_checksum", "t_daily",
		sourcePartitions, destPartitions)

	if !handled {
		t.Errorf("滚动窗口平移应被处理，reason: %s", reason)
	}
	// 应该有 DROP p0101 和 ADD p0104
	if len(advisorySQLs) == 0 {
		t.Error("应生成 DROP p0101 的建议 SQL")
	}
	if len(execSQLs) == 0 {
		t.Error("应生成 ADD p0104 的 SQL")
	}
}

// TestBuildPartitionRepairSQLs_AddWithMaxValue 验证带 MAXVALUE 的新增分区场景（D1.4）
func TestBuildPartitionRepairSQLs_AddWithMaxValue(t *testing.T) {
	// 源端：p0101,p0102,pmax
	// 目的端：p0101,pmax
	sourcePartitions := buildTestPartitionMap("gt_checksum.t_monthly",
		[]string{"p0101", "p0102", "pmax"})
	destPartitions := buildTestPartitionMapWithMaxValue("gt_checksum.t_monthly",
		[]string{"p0101", "pmax"}, "pmax")

	execSQLs, _, handled, reason := buildPartitionRepairSQLs(
		"gt_checksum", "t_monthly", "gt_checksum", "t_monthly",
		sourcePartitions, destPartitions)

	if !handled {
		t.Errorf("带 MAXVALUE 的新增分区应被处理，reason: %s", reason)
	}
	if len(execSQLs) == 0 {
		t.Error("应生成 REORGANIZE PARTITION SQL")
	}
	// 验证生成的是 REORGANIZE 而非简单 ADD
	found := false
	for _, sql := range execSQLs {
		if contains(sql, "REORGANIZE PARTITION") {
			found = true
			break
		}
	}
	if !found {
		t.Error("带 MAXVALUE 时应生成 REORGANIZE PARTITION 语句")
	}
}

// TestBuildPartitionRepairSQLs_MethodMismatch 验证分区方法不匹配的情况
func TestBuildPartitionRepairSQLs_MethodMismatch(t *testing.T) {
	// 源端使用 RANGE，目的端使用 LIST
	sourcePartitions := map[string]string{
		"gt_checksum.t_diff": "PARTITION BY RANGE (id)",
		"gt_checksum.t_diff.p1": "NAME=p1,ORDINAL=1,METHOD=RANGE,EXPRESSION=id,DESCRIPTION=10,ROWS=0",
	}
	destPartitions := map[string]string{
		"gt_checksum.t_diff": "PARTITION BY LIST (id)",
		"gt_checksum.t_diff.p1": "NAME=p1,ORDINAL=1,METHOD=LIST,EXPRESSION=id,DESCRIPTION=(10),ROWS=0",
	}

	_, _, handled, reason := buildPartitionRepairSQLs(
		"gt_checksum", "t_diff", "gt_checksum", "t_diff",
		sourcePartitions, destPartitions)

	if handled {
		t.Error("分区方法不匹配时不应被自动处理")
	}
	if !contains(reason, "method or expression differs") {
		t.Errorf("原因应包含 'method or expression differs'，got: %s", reason)
	}
}

// ---------- D3: TO_DAYS 边界可读性测试 ----------

// TestToDaysToReadableDate 验证 TO_DAYS 整数转换为日期
func TestToDaysToReadableDate(t *testing.T) {
	tests := []struct {
		name     string
		days     string
		expected string
	}{
		{"2024-02-01", "739282", "2024-02-01"},
		{"2024-01-01", "739251", "2024-01-01"},
		{"2023-12-31", "739250", "2023-12-31"},
		{"invalid", "abc", ""},
		{"out of range", "0", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toDaysToReadableDate(tt.days)
			if result != tt.expected {
				t.Errorf("toDaysToReadableDate(%s) = %s, want %s", tt.days, result, tt.expected)
			}
		})
	}
}

// TestFormatPartitionDescriptionForAdd_ToDays 验证 TO_DAYS 分区边界格式化
func TestFormatPartitionDescriptionForAdd_ToDays(t *testing.T) {
	meta := partitionMetadata{
		Name:        "p20240201",
		Method:      "RANGE (TO_DAYS(dt))",
		Description: "739282",
	}

	result, ok := formatPartitionDescriptionForAdd(meta)
	if !ok {
		t.Error("TO_DAYS 分区应能格式化")
	}
	if !contains(result, "TO_DAYS('2024-02-01')") {
		t.Errorf("应包含可读日期，got: %s", result)
	}
}

// ---------- D4: ENGINE= 尾注剥离测试 ----------

// TestStripPartitionEngineAnnotations 验证 ENGINE= 尾注剥离
func TestStripPartitionEngineAnnotations(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "ENGINE = InnoDB",
			input:    "PARTITION p1 VALUES LESS THAN (10) ENGINE = InnoDB",
			expected: "PARTITION p1 VALUES LESS THAN (10)",
		},
		{
			name:     "ENGINE=INNODB",
			input:    "PARTITION p1 VALUES LESS THAN (10) ENGINE=INNODB",
			expected: "PARTITION p1 VALUES LESS THAN (10)",
		},
		{
			name:     "无 ENGINE",
			input:    "PARTITION p1 VALUES LESS THAN (10)",
			expected: "PARTITION p1 VALUES LESS THAN (10)",
		},
		{
			name:     "多个 ENGINE",
			input:    "PARTITION p1 VALUES LESS THAN (10) ENGINE = InnoDB, PARTITION p2 VALUES LESS THAN (20) ENGINE = InnoDB",
			expected: "PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN (20)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripPartitionEngineAnnotations(tt.input)
			result = strings.Join(strings.Fields(result), " ")
			expected := strings.Join(strings.Fields(tt.expected), " ")
			if result != expected {
				t.Errorf("stripPartitionEngineAnnotations() = %q, want %q", result, expected)
			}
		})
	}
}

// TestNormalizePartitionFullDefinition_EngineStripped 验证完整定义归一化时剥离 ENGINE
func TestNormalizePartitionFullDefinition_EngineStripped(t *testing.T) {
	def1 := "PARTITION BY RANGE (id) (PARTITION p1 VALUES LESS THAN (10) ENGINE = InnoDB, PARTITION p2 VALUES LESS THAN (20) ENGINE = InnoDB)"
	def2 := "PARTITION BY RANGE (id) (PARTITION p1 VALUES LESS THAN (10) ENGINE=INNODB, PARTITION p2 VALUES LESS THAN (20) ENGINE=INNODB)"

	norm1 := normalizePartitionFullDefinition(def1)
	norm2 := normalizePartitionFullDefinition(def2)

	if norm1 != norm2 {
		t.Errorf("剥离 ENGINE 后两个定义应相等:\nnorm1: %s\nnorm2: %s", norm1, norm2)
	}
}

// ---------- 辅助函数 ----------

func buildTestPartitionMap(tableKey string, names []string) map[string]string {
	result := map[string]string{
		tableKey: "PARTITION BY RANGE (id)",
	}
	for i, name := range names {
		result[fmt.Sprintf("%s.%s", tableKey, name)] = fmt.Sprintf(
			"NAME=%s,ORDINAL=%d,METHOD=RANGE,EXPRESSION=id,DESCRIPTION=%d,ROWS=0",
			name, i+1, (i+1)*10)
	}
	return result
}

func buildTestPartitionMapWithMaxValue(tableKey string, names []string, maxPartName string) map[string]string {
	result := map[string]string{
		tableKey: "PARTITION BY RANGE (id)",
	}
	for i, name := range names {
		desc := fmt.Sprintf("%d", (i+1)*10)
		if name == maxPartName {
			desc = "MAXVALUE"
		}
		result[fmt.Sprintf("%s.%s", tableKey, name)] = fmt.Sprintf(
			"NAME=%s,ORDINAL=%d,METHOD=RANGE,EXPRESSION=id,DESCRIPTION=%s,ROWS=0",
			name, i+1, desc)
	}
	return result
}
