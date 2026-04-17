package mysql

import (
	"reflect"
	"testing"
)

// TestFilterPKColumnsAgainstSource_DropMyRowId 验证当目标端 MySQL 8.0
// 生成的隐藏主键 my_row_id 不存在于源端列数据时会被剔除，避免在
// FixDeleteSqlExec 中走 pri 分支却无法从 RowData 取到对应值，进而导致
// `failed to generate DELETE statement ... no valid conditions` 错误。
// 回归测例：regression-oracle-20260417-084030 中 TESTBIN / TESTSTRING。
func TestFilterPKColumnsAgainstSource_DropMyRowId(t *testing.T) {
	sourceCols := []map[string]string{
		{"columnName": "F1", "columnSeq": "1"},
		{"columnName": "F2", "columnSeq": "2"},
	}
	kept, dropped := filterPKColumnsAgainstSource([]string{"my_row_id"}, sourceCols)
	if len(kept) != 0 {
		t.Fatalf("expected my_row_id to be dropped, kept=%v", kept)
	}
	if !reflect.DeepEqual(dropped, []string{"my_row_id"}) {
		t.Fatalf("dropped=%v, want [my_row_id]", dropped)
	}
}

// TestFilterPKColumnsAgainstSource_KeepRealPK 源端含有主键列时保留。
func TestFilterPKColumnsAgainstSource_KeepRealPK(t *testing.T) {
	sourceCols := []map[string]string{
		{"columnName": "ID", "columnSeq": "1"},
		{"columnName": "NAME", "columnSeq": "2"},
	}
	kept, dropped := filterPKColumnsAgainstSource([]string{"id"}, sourceCols)
	if !reflect.DeepEqual(kept, []string{"id"}) {
		t.Fatalf("kept=%v, want [id]", kept)
	}
	if len(dropped) != 0 {
		t.Fatalf("expected nothing dropped, dropped=%v", dropped)
	}
}

// TestFilterPKColumnsAgainstSource_CompositePartial 复合主键中仅部分列能在源端
// 找到时，仅保留可匹配列，其余丢弃（若全部丢弃则调用方应回退到 mul 分支）。
func TestFilterPKColumnsAgainstSource_CompositePartial(t *testing.T) {
	sourceCols := []map[string]string{
		{"columnName": "ORDER_ID", "columnSeq": "1"},
	}
	kept, dropped := filterPKColumnsAgainstSource([]string{"order_id", "my_row_id"}, sourceCols)
	if !reflect.DeepEqual(kept, []string{"order_id"}) {
		t.Fatalf("kept=%v, want [order_id]", kept)
	}
	if !reflect.DeepEqual(dropped, []string{"my_row_id"}) {
		t.Fatalf("dropped=%v, want [my_row_id]", dropped)
	}
}
