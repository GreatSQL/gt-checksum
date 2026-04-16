package actions

import (
	"strings"
	"testing"
)

// TestRemapDelToOriginalDest_FloatPrecision 验证 Oracle FLOAT→MySQL double 精度差异场景：
// Oracle 返回 "123.449997"，MySQL 实际存储 "123.44999694824219"。
// 浮点归一化后两端相同（用于比对），但 del 必须还原为 MySQL 原始值（用于 DELETE WHERE）。
func TestRemapDelToOriginalDest_FloatPrecision(t *testing.T) {
	const sep = "/*go actions columnData*/"

	// MySQL 实际存储行（原始）
	origRow := strings.Join([]string{"123.44999694824219", "123.44999694824219", "123.45", "12.456"}, sep)
	// 归一化后（float round-6 后与 Oracle 一致）
	normalizedRow := strings.Join([]string{"123.449997", "123.449997", "123.45", "12.456"}, sep)

	origCleanDestData := []string{origRow}
	normalizedDest := []string{normalizedRow}
	// Arrcmp 返回的 del 包含归一化后的行
	normalizedDel := []string{normalizedRow}

	result := remapDelToOriginalDest(normalizedDel, normalizedDest, origCleanDestData)

	if len(result) != 1 {
		t.Fatalf("expected 1 result row, got %d", len(result))
	}
	if result[0] != origRow {
		t.Errorf("expected original row\n  %q\ngot\n  %q", origRow, result[0])
	}
}

// TestRemapDelToOriginalDest_NoNorm 验证无归一化时行为：原始行与归一化行相同，映射后结果一致。
func TestRemapDelToOriginalDest_NoNorm(t *testing.T) {
	const sep = "/*go actions columnData*/"
	row := strings.Join([]string{"100", "200", "text"}, sep)

	result := remapDelToOriginalDest([]string{row}, []string{row}, []string{row})
	if len(result) != 1 || result[0] != row {
		t.Errorf("expected %q, got %v", row, result)
	}
}

// TestRemapDelToOriginalDest_MultiRow 验证多行场景：两个归一化相同的 del 行分别还原到不同原始行。
func TestRemapDelToOriginalDest_MultiRow(t *testing.T) {
	const sep = "/*go actions columnData*/"

	orig1 := strings.Join([]string{"123.44999694824219", "1"}, sep)
	orig2 := strings.Join([]string{"123.44999694824219", "2"}, sep)
	norm1 := strings.Join([]string{"123.449997", "1"}, sep)
	norm2 := strings.Join([]string{"123.449997", "2"}, sep)

	origCleanDestData := []string{orig1, orig2}
	normalizedDest := []string{norm1, norm2}
	normalizedDel := []string{norm1, norm2}

	result := remapDelToOriginalDest(normalizedDel, normalizedDest, origCleanDestData)

	if len(result) != 2 {
		t.Fatalf("expected 2 result rows, got %d", len(result))
	}
	// 两行都应还原为原始值
	if result[0] != orig1 || result[1] != orig2 {
		t.Errorf("result mismatch: got %v, want [%q %q]", result, orig1, orig2)
	}
}

// TestRemapDelToOriginalDest_LengthMismatch 验证长度不一致时保守返回原 del。
func TestRemapDelToOriginalDest_LengthMismatch(t *testing.T) {
	const sep = "/*go actions columnData*/"
	norm := strings.Join([]string{"1", "2"}, sep)
	orig := strings.Join([]string{"1", "2"}, sep)

	// normalizedDest 和 origDest 长度不同 → 应原样返回
	result := remapDelToOriginalDest([]string{norm}, []string{norm, norm}, []string{orig})
	if len(result) != 1 || result[0] != norm {
		t.Errorf("expected fallback to %q, got %v", norm, result)
	}
}

// TestNormalizeFloatComparisonValue_OracleFloat 验证浮点归一化函数对 Oracle FLOAT 值的处理。
func TestNormalizeFloatComparisonValue_OracleFloat(t *testing.T) {
	// 归一化到 6 位小数
	got := normalizeFloatComparisonValue("123.44999694824219", 6)
	want := "123.449997"
	if got != want {
		t.Errorf("normalizeFloatComparisonValue(\"123.44999694824219\", 6) = %q, want %q", got, want)
	}

	got2 := normalizeFloatComparisonValue("123.449997", 6)
	if got2 != want {
		t.Errorf("normalizeFloatComparisonValue(\"123.449997\", 6) = %q, want %q", got2, want)
	}
}
