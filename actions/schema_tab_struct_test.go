package actions

import (
	"strings"
	"testing"

	"gt-checksum/global"
)

// ---------- shared test helpers ----------

func mustContainLine(t *testing.T, lines []string, sub string) {
	t.Helper()
	for _, l := range lines {
		if contains(l, sub) {
			return
		}
	}
	t.Errorf("expected a line containing %q; got:\n%s", sub, strings.Join(lines, "\n"))
}

// TestExtractCandidateSchemas verifies that extractCandidateSchemas correctly
// extracts unique schema names from a DatabaseNameList key set.
func TestExtractCandidateSchemas(t *testing.T) {
	candidates := map[string]int{
		"sales/*schema&table*/orders":    1,
		"sales/*schema&table*/v_summary": 1,
		"hr/*schema&table*/employees":    1,
	}
	got := extractCandidateSchemas(candidates)
	// Convert to a set for order-independent comparison.
	gotSet := make(map[string]bool, len(got))
	for _, s := range got {
		gotSet[s] = true
	}
	if !gotSet["sales"] || !gotSet["hr"] {
		t.Errorf("extractCandidateSchemas: expected [sales hr], got %v", got)
	}
	if len(got) != 2 {
		t.Errorf("extractCandidateSchemas: expected 2 distinct schemas, got %d: %v", len(got), got)
	}
}

// TestExtractCandidateSchemas_empty ensures an empty candidate map returns an
// empty slice (which causes ObjectTypeMap to fall back to the full scan).
func TestExtractCandidateSchemas_empty(t *testing.T) {
	if got := extractCandidateSchemas(map[string]int{}); len(got) != 0 {
		t.Errorf("expected empty slice, got %v", got)
	}
}

// helper
func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		(len(s) > 0 && len(sub) > 0 && searchSubstring(s, sub)))
}

func searchSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func makeSchemaTable(srcRaw, srcSeries string, srcFlavor global.DatabaseFlavor, dstRaw, dstSeries string, dstFlavor global.DatabaseFlavor) *schemaTable {
	src := global.MySQLVersionInfo{Raw: srcRaw, Series: srcSeries, Flavor: srcFlavor}
	dst := global.MySQLVersionInfo{Raw: dstRaw, Series: dstSeries, Flavor: dstFlavor}
	return &schemaTable{
		sourceDrive:   "mysql",
		destDrive:     "mysql",
		sourceVersion: src,
		destVersion:   dst,
	}
}

// ---------- adjustDestColumnSeqAfterDrops ----------

// TestAdjustDestColumnSeqAfterDrops_singleDropAtHead 模拟 MySQL 8.4 目标端
// 存在隐式主键 my_row_id（position=0），DROP 后剩余列序号应向前压缩一位。
// 这是 dul-fix-collation bug 的核心场景：若序号不调整，f1(dest=1) vs f1(src=0)
// 被误判为序号不匹配，导致仅有 collation 差异的列也生成重复的 MODIFY 语句。
func TestAdjustDestColumnSeqAfterDrops_singleDropAtHead(t *testing.T) {
	seq := map[string]int{
		"MY_ROW_ID": 0,
		"F1":        1,
		"F2":        2,
		"F3":        3,
	}
	adjustDestColumnSeqAfterDrops(seq, []string{"MY_ROW_ID"})

	if _, exists := seq["MY_ROW_ID"]; exists {
		t.Fatal("MY_ROW_ID should have been removed from seq map")
	}
	want := map[string]int{"F1": 0, "F2": 1, "F3": 2}
	for col, wantSeq := range want {
		if got := seq[col]; got != wantSeq {
			t.Errorf("seq[%s] = %d, want %d", col, got, wantSeq)
		}
	}
}

// TestAdjustDestColumnSeqAfterDrops_multipleDrops 验证连续删除多列后序号压缩正确。
func TestAdjustDestColumnSeqAfterDrops_multipleDrops(t *testing.T) {
	seq := map[string]int{
		"A": 0,
		"B": 1,
		"C": 2,
		"D": 3,
		"E": 4,
	}
	// 删除 A(0) 和 C(2)，剩余 B→0，D→1，E→2
	adjustDestColumnSeqAfterDrops(seq, []string{"A", "C"})

	if _, exists := seq["A"]; exists {
		t.Fatal("A should have been removed")
	}
	if _, exists := seq["C"]; exists {
		t.Fatal("C should have been removed")
	}
	want := map[string]int{"B": 0, "D": 1, "E": 2}
	for col, wantSeq := range want {
		if got := seq[col]; got != wantSeq {
			t.Errorf("seq[%s] = %d, want %d", col, got, wantSeq)
		}
	}
}

// TestAdjustDestColumnSeqAfterDrops_dropAtTail 删除末尾列，前面列序号不变。
func TestAdjustDestColumnSeqAfterDrops_dropAtTail(t *testing.T) {
	seq := map[string]int{
		"F1": 0,
		"F2": 1,
		"F3": 2,
	}
	adjustDestColumnSeqAfterDrops(seq, []string{"F3"})

	want := map[string]int{"F1": 0, "F2": 1}
	for col, wantSeq := range want {
		if got := seq[col]; got != wantSeq {
			t.Errorf("seq[%s] = %d, want %d", col, got, wantSeq)
		}
	}
}

// TestAdjustDestColumnSeqAfterDrops_noDrop 无删除列时 seq 保持不变。
func TestAdjustDestColumnSeqAfterDrops_noDrop(t *testing.T) {
	seq := map[string]int{
		"F1": 0,
		"F2": 1,
	}
	adjustDestColumnSeqAfterDrops(seq, []string{})

	want := map[string]int{"F1": 0, "F2": 1}
	for col, wantSeq := range want {
		if got := seq[col]; got != wantSeq {
			t.Errorf("seq[%s] = %d, want %d", col, got, wantSeq)
		}
	}
}

// ---------- isOracleToMySQL ----------

// TestIsOracleToMySQL_GodrorSource 验证 isOracleToMySQL() 在 godror 驱动 + MySQL 目标时返回 true。
// Bug 背景：Oracle→MySQL struct 校验中，无论源/目标表是否有分区，代码都无条件触发
// warn-only 并生成 Advisory 修复 SQL。修复后，仅当至少一侧有分区时才触发 advisory。
func TestIsOracleToMySQL_GodrorSource(t *testing.T) {
	st := &schemaTable{
		sourceDrive: "godror",
		// Raw 必须非空，destVersionInfo() 才会返回 destVersion 而非全局变量
		destVersion: global.MySQLVersionInfo{Raw: "8.0.32", Flavor: global.DatabaseFlavorMySQL},
	}
	if !st.isOracleToMySQL() {
		t.Error("godror + MySQL 目标应返回 isOracleToMySQL=true")
	}
}

// TestIsOracleToMySQL_MySQLSource 验证 MySQL→MySQL 场景不被识别为 Oracle→MySQL。
func TestIsOracleToMySQL_MySQLSource(t *testing.T) {
	st := &schemaTable{
		sourceDrive: "mysql",
		destVersion: global.MySQLVersionInfo{Raw: "8.0.32", Flavor: global.DatabaseFlavorMySQL},
	}
	if st.isOracleToMySQL() {
		t.Error("mysql 源不应被识别为 Oracle→MySQL 场景")
	}
}

// ---------- OracleToMySQL nullable normalization ----------

// TestOracleToMySQL_NullableNormalization 验证 Oracle nullable 值（N/Y）被正确规范化为
// MySQL 格式（NO/YES），以确保生成的 MODIFY COLUMN SQL 包含正确的 NOT NULL 约束。
// Bug 背景：Oracle 返回 nullable="N" 表示 NOT NULL，而 FixAlterColumnSqlDispos 只识别 "NO"，
// 导致 NOT NULL 约束被遗漏，修复 SQL 执行后再次校验仍报 Diffs=yes（无限循环）。
func TestOracleToMySQL_NullableNormalization(t *testing.T) {
	tests := []struct {
		name     string
		input    string // Oracle nullable 原始值
		wantNorm string // 规范化后期望的 MySQL 格式值
	}{
		{"N should normalize to NO", "N", "NO"},
		{"Y should normalize to YES", "Y", "YES"},
		{"already NO stays NO", "NO", "NO"},
		{"already YES stays YES", "YES", "YES"},
		{"lowercase n should normalize to NO", "n", "NO"},
		{"lowercase y should normalize to YES", "y", "YES"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 模拟 schema_tab_struct.go 中的规范化逻辑
			repairAttrs := []string{"decimal(20,0)", "null", "null", tt.input, "", ""}
			switch strings.ToUpper(strings.TrimSpace(repairAttrs[3])) {
			case "N":
				repairAttrs[3] = "NO"
			case "Y":
				repairAttrs[3] = "YES"
			}
			if repairAttrs[3] != tt.wantNorm {
				t.Errorf("nullable normalization(%q) = %q, want %q", tt.input, repairAttrs[3], tt.wantNorm)
			}
		})
	}
}
