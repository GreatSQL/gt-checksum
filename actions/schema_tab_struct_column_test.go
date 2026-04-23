package actions

import (
	"reflect"
	"sort"
	"testing"
)

// ---------- hasAutoIncrementColumnAttribute ----------

func TestHasAutoIncrementColumnAttribute_Present(t *testing.T) {
	def := []string{"int", "utf8mb4", "utf8mb4_general_ci", "NO", "0", "id auto_increment"}
	if !hasAutoIncrementColumnAttribute(def) {
		t.Fatal("should detect AUTO_INCREMENT regardless of case")
	}
}

func TestHasAutoIncrementColumnAttribute_Absent(t *testing.T) {
	def := []string{"int", "utf8mb4", "utf8mb4_general_ci", "NO", "0", "primary key"}
	if hasAutoIncrementColumnAttribute(def) {
		t.Fatal("should not report AUTO_INCREMENT when missing")
	}
}

func TestHasAutoIncrementColumnAttribute_EmptySlice(t *testing.T) {
	if hasAutoIncrementColumnAttribute(nil) {
		t.Fatal("nil slice must return false")
	}
	if hasAutoIncrementColumnAttribute([]string{}) {
		t.Fatal("empty slice must return false")
	}
}

func TestHasAutoIncrementColumnAttribute_PartialMatch(t *testing.T) {
	// Should match when AUTO_INCREMENT is embedded
	def := []string{"bigint unsigned NOT NULL AUTO_INCREMENT"}
	if !hasAutoIncrementColumnAttribute(def) {
		t.Fatal("should detect embedded AUTO_INCREMENT token")
	}
}

// 说明：adjustDestColumnSeqAfterDrops 的详尽用例位于
// schema_tab_struct_test.go（singleDropAtHead/multipleDrops/dropAtTail/noDrop）。
// 本文件补充一个 1-based 偏移的中段删除场景，避免回归时遗漏非 0 起点的用法。

func TestAdjustDestColumnSeqAfterDrops_OneBasedMiddle(t *testing.T) {
	seq := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	adjustDestColumnSeqAfterDrops(seq, []string{"b"})
	want := map[string]int{"a": 1, "c": 2, "d": 3}
	if !reflect.DeepEqual(seq, want) {
		t.Fatalf("after dropping b: got %v, want %v", seq, want)
	}
}

// ---------- isIgnorableGeneratedInvisibleColumn ----------

func TestIsIgnorableGeneratedInvisibleColumn_MyRowIdInvisible(t *testing.T) {
	m := map[string][]string{
		"my_row_id": {"bigint", "", "", "NO", "", "INVISIBLE"},
	}
	if !isIgnorableGeneratedInvisibleColumn("my_row_id", m) {
		t.Fatal("my_row_id with INVISIBLE token should be ignorable")
	}
}

// 文档化实际行为：列名判断虽走 EqualFold，但 columnMap 查找仍需精确匹配
// （map 键区分大小写）。混用大小写时会返回 false——这是当前实现的约束，
// 后续重构必须保持一致，直到有独立需求修正。
func TestIsIgnorableGeneratedInvisibleColumn_MapLookupCaseSensitive(t *testing.T) {
	m := map[string][]string{
		"my_row_id": {"bigint INVISIBLE"},
	}
	if isIgnorableGeneratedInvisibleColumn("MY_ROW_ID", m) {
		t.Fatal("columnMap 使用精确键查找，大小写不匹配应返回 false（当前行为）")
	}
}

func TestIsIgnorableGeneratedInvisibleColumn_NotMyRowId(t *testing.T) {
	m := map[string][]string{
		"other_col": {"bigint INVISIBLE"},
	}
	if isIgnorableGeneratedInvisibleColumn("other_col", m) {
		t.Fatal("non-my_row_id columns must not be ignorable")
	}
}

func TestIsIgnorableGeneratedInvisibleColumn_MyRowIdVisible(t *testing.T) {
	m := map[string][]string{
		"my_row_id": {"bigint", "", "", "NO", "", ""},
	}
	if isIgnorableGeneratedInvisibleColumn("my_row_id", m) {
		t.Fatal("my_row_id without INVISIBLE must not be ignorable")
	}
}

func TestIsIgnorableGeneratedInvisibleColumn_MissingFromMap(t *testing.T) {
	m := map[string][]string{}
	if isIgnorableGeneratedInvisibleColumn("my_row_id", m) {
		t.Fatal("missing column entry must not be ignorable")
	}
}

// ---------- filterIgnorableGeneratedInvisibleColumns ----------

func TestFilterIgnorableGeneratedInvisibleColumns_Mixed(t *testing.T) {
	cols := []string{"id", "my_row_id", "name"}
	m := map[string][]string{
		"id":        {"int"},
		"my_row_id": {"bigint INVISIBLE"},
		"name":      {"varchar(10)"},
	}
	kept, ignored := filterIgnorableGeneratedInvisibleColumns(cols, m)
	wantKept := []string{"id", "name"}
	wantIgnored := []string{"my_row_id"}
	if !reflect.DeepEqual(kept, wantKept) {
		t.Fatalf("kept: got %v, want %v", kept, wantKept)
	}
	if !reflect.DeepEqual(ignored, wantIgnored) {
		t.Fatalf("ignored: got %v, want %v", ignored, wantIgnored)
	}
}

func TestFilterIgnorableGeneratedInvisibleColumns_NoIgnores(t *testing.T) {
	cols := []string{"id", "name"}
	m := map[string][]string{"id": {"int"}, "name": {"varchar(10)"}}
	kept, ignored := filterIgnorableGeneratedInvisibleColumns(cols, m)
	if !reflect.DeepEqual(kept, cols) {
		t.Fatalf("kept should equal input when no ignores: got %v", kept)
	}
	if len(ignored) != 0 {
		t.Fatalf("ignored should be empty: got %v", ignored)
	}
}

func TestFilterIgnorableGeneratedInvisibleColumns_EmptyInput(t *testing.T) {
	kept, ignored := filterIgnorableGeneratedInvisibleColumns(nil, nil)
	if len(kept) != 0 || len(ignored) != 0 {
		t.Fatalf("empty input must yield empty slices: kept=%v ignored=%v", kept, ignored)
	}
}

// ---------- normalizeDataCheckColumnInfo ----------

func TestNormalizeDataCheckColumnInfo_StripMyRowIdInvisibleOnTarget(t *testing.T) {
	// Source has no my_row_id; target does with INVISIBLE → strip from target.
	source := []map[string]string{
		{"columnName": "id"},
		{"columnName": "name"},
	}
	dest := []map[string]string{
		{"columnName": "id"},
		{"columnName": "my_row_id", "extra": "INVISIBLE"},
		{"columnName": "name"},
	}
	_, filteredDest, stripped := normalizeDataCheckColumnInfo(source, dest)
	if len(filteredDest) != 2 {
		t.Fatalf("target my_row_id should be filtered out, got %d cols: %v", len(filteredDest), filteredDest)
	}
	if !reflect.DeepEqual(stripped, []string{"my_row_id"}) {
		t.Fatalf("stripped list mismatch: got %v", stripped)
	}
}

func TestNormalizeDataCheckColumnInfo_KeepMyRowIdWhenOnBothSides(t *testing.T) {
	// If source also has my_row_id, keep on target.
	source := []map[string]string{
		{"columnName": "my_row_id"},
	}
	dest := []map[string]string{
		{"columnName": "my_row_id", "extra": "INVISIBLE"},
	}
	_, filteredDest, stripped := normalizeDataCheckColumnInfo(source, dest)
	if len(filteredDest) != 1 {
		t.Fatalf("my_row_id present on both sides must be kept: got %v", filteredDest)
	}
	if len(stripped) != 0 {
		t.Fatalf("stripped should be empty, got %v", stripped)
	}
}

func TestNormalizeDataCheckColumnInfo_KeepMyRowIdWhenVisible(t *testing.T) {
	// Target my_row_id without INVISIBLE should be kept even if source lacks it.
	source := []map[string]string{{"columnName": "id"}}
	dest := []map[string]string{
		{"columnName": "id"},
		{"columnName": "my_row_id", "extra": ""},
	}
	_, filteredDest, stripped := normalizeDataCheckColumnInfo(source, dest)
	if len(filteredDest) != 2 {
		t.Fatalf("visible my_row_id must be kept: got %v", filteredDest)
	}
	if len(stripped) != 0 {
		t.Fatalf("stripped should be empty: got %v", stripped)
	}
}

func TestNormalizeDataCheckColumnInfo_UppercaseKeyFallback(t *testing.T) {
	// Ensure fallback to COLUMN_NAME / EXTRA keys works.
	source := []map[string]string{{"COLUMN_NAME": "id"}}
	dest := []map[string]string{
		{"COLUMN_NAME": "id"},
		{"COLUMN_NAME": "my_row_id", "EXTRA": "invisible"},
	}
	_, filteredDest, stripped := normalizeDataCheckColumnInfo(source, dest)
	if len(filteredDest) != 1 || len(stripped) != 1 {
		t.Fatalf("uppercase-key fallback failed: dest=%v stripped=%v", filteredDest, stripped)
	}
}

func TestNormalizeDataCheckColumnInfo_SourceUntouched(t *testing.T) {
	// Source slice is returned as-is; verify reference integrity.
	source := []map[string]string{{"columnName": "id"}, {"columnName": "x"}}
	dest := []map[string]string{{"columnName": "id"}}
	got, _, _ := normalizeDataCheckColumnInfo(source, dest)
	if len(got) != len(source) {
		t.Fatalf("source length altered: got %d, want %d", len(got), len(source))
	}
	// Stable order check.
	names := []string{got[0]["columnName"], got[1]["columnName"]}
	sort.Strings(names)
	if !reflect.DeepEqual(names, []string{"id", "x"}) {
		t.Fatalf("source content altered: %v", names)
	}
}
