package mysql

import (
	"strings"
	"testing"
)

func TestOrderColumnsForCompare_CaseInsensitive(t *testing.T) {
	allCols := []map[string]string{
		{"columnName": "id", "dataType": "bigint"},
		{"columnName": "amount", "dataType": "decimal(10,2)"},
		{"columnName": "note", "dataType": "varchar(32)"},
	}

	got := orderColumnsForCompare(allCols, []string{"ID"}, []string{"ID", "AMOUNT"})
	if len(got) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(got))
	}
	if got[0]["columnName"] != "id" {
		t.Fatalf("first column = %q, want id", got[0]["columnName"])
	}
	if got[1]["columnName"] != "amount" {
		t.Fatalf("second column = %q, want amount", got[1]["columnName"])
	}
}

// TestFormatComparableColumnExpr_DatetimeWithPrecision 验证 DATETIME(n) 类型能正确被 date_format 格式化。
// Bug：原代码 t == "DATETIME" 无法匹配 DATETIME(6) / DATETIME(3)，导致 Oracle→MySQL 校验中
// c_timestamp 列不被格式化，与 Oracle TO_CHAR 截断到秒的结果永久不匹配（修复 SQL 无法收敛）。
func TestFormatComparableColumnExpr_DatetimeWithPrecision(t *testing.T) {
	cases := []struct {
		dataType string
		wantFmt  bool // 期望包含 date_format
	}{
		{"DATETIME", true},
		{"DATETIME(6)", true},
		{"DATETIME(3)", true},
		{"datetime(6)", true},
		{"TIMESTAMP", true},
		{"TIMESTAMP(6)", true},
		{"DATE", true}, // DATE 走 date_format 00:00:00 逻辑，也含 date_format
		{"VARCHAR(32)", false},
	}
	for _, c := range cases {
		got := formatComparableColumnExpr("`col`", c.dataType)
		hasFmt := strings.Contains(got, "date_format") || strings.Contains(got, "time_format")
		if hasFmt != c.wantFmt {
			t.Errorf("formatComparableColumnExpr(`col`, %q) = %q; wantFmt=%v got=%v",
				c.dataType, got, c.wantFmt, hasFmt)
		}
		// 对 DATETIME(n) 必须确认是 date_format 而非其他格式函数
		if c.dataType == "DATETIME(6)" || c.dataType == "DATETIME(3)" {
			if !strings.Contains(got, "date_format") {
				t.Errorf("DATETIME(%s) should use date_format, got: %s", c.dataType, got)
			}
		}
	}
}
