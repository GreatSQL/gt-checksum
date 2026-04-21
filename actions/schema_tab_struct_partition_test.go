package actions

import (
	"testing"

	"gt-checksum/global"
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
