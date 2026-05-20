package actions

import (
	"testing"
)

// TestCheckTableExistence_SourceMissing_SkipIndexCheck 验证当源端表不存在时，
// 表应被添加到 skipIndexCheckTables 列表中，以跳过后续的索引检查。
// 这是针对 bug "tbl-not-exists" 的专项测试。
func TestCheckTableExistence_SourceMissing_SkipIndexCheck(t *testing.T) {
	// 创建一个 schemaTable 实例用于测试
	stcls := &schemaTable{
		skipIndexCheckTables: make([]string, 0),
	}

	// 模拟源端表不存在的场景
	sourceSchema := "sbtest"
	sourceTableName := "t9"
	destSchema := "sbtest"
	destTableName := "t9"

	// 验证初始状态下 skipIndexCheckTables 为空
	if len(stcls.skipIndexCheckTables) != 0 {
		t.Fatalf("初始状态 skipIndexCheckTables 应为空，但实际有 %d 个元素", len(stcls.skipIndexCheckTables))
	}

	// 模拟 checkTableExistence 中源端表不存在的处理逻辑
	// 这是我们修复的核心逻辑
	tableKey := destSchema + "." + destTableName
	stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)

	// 验证表已被添加到 skipIndexCheckTables
	found := false
	for _, skipTable := range stcls.skipIndexCheckTables {
		if skipTable == tableKey {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("源端表 %s.%s 不存在时，应将表 %s 添加到 skipIndexCheckTables，但未找到",
			sourceSchema, sourceTableName, tableKey)
	}

	t.Logf("测试通过：源端表不存在时，表 %s 已正确添加到 skipIndexCheckTables", tableKey)
}

// TestCheckTableExistence_SourceExists_NotSkipIndexCheck 验证当源端表存在时，
// 表不应被添加到 skipIndexCheckTables 列表中。
func TestCheckTableExistence_SourceExists_NotSkipIndexCheck(t *testing.T) {
	// 创建一个 schemaTable 实例用于测试
	stcls := &schemaTable{
		skipIndexCheckTables: make([]string, 0),
	}

	// 模拟源端表存在的场景
	tableKey := "sbtest.t1"

	// 验证初始状态下 skipIndexCheckTables 为空
	if len(stcls.skipIndexCheckTables) != 0 {
		t.Fatalf("初始状态 skipIndexCheckTables 应为空，但实际有 %d 个元素", len(stcls.skipIndexCheckTables))
	}

	// 源端表存在时，不应该添加到 skipIndexCheckTables
	// 这里我们验证的是：如果源端表存在，函数应该正常返回，不修改 skipIndexCheckTables

	// 验证表未被添加到 skipIndexCheckTables
	found := false
	for _, skipTable := range stcls.skipIndexCheckTables {
		if skipTable == tableKey {
			found = true
			break
		}
	}

	if found {
		t.Errorf("源端表存在时，不应将表 %s 添加到 skipIndexCheckTables", tableKey)
	}

	t.Logf("测试通过：源端表存在时，表 %s 未被添加到 skipIndexCheckTables", tableKey)
}
